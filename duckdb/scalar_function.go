package duckdb

import (
	"fmt"
	"math"
	"runtime"
	"slices"
	"sync/atomic"

	"github.com/bububa/duckdbspike/internal/abi"
)

// ScalarFunctionBindArg describes one argument expression inspected during a
// ScalarFunction's Bind callback. When IsFoldable is true, Value holds the
// constant value folded by DuckDB.
type ScalarFunctionBindArg struct {
	Type       LogicalType
	IsFoldable bool
	Value      Value
}

// ScalarFunctionBindInput supplies the bound argument expressions passed to a
// ScalarFunction's optional Bind callback.
type ScalarFunctionBindInput struct {
	Args []ScalarFunctionBindArg
}

// ScalarFunctionChunk wraps one DuckDB input DataChunk and output Vector during
// a single ExecuteChunk call. It is valid only for the duration of that call
// and must not be retained after ExecuteChunk returns.
type ScalarFunctionChunk struct {
	api      *abi.API
	rows     int
	argVecs  []uintptr
	argPlans []columnPlan
	output   uintptr
	retPlan  *columnPlan
	limits   DecodeLimits
}

// Len returns the number of rows in the current batch.
func (c *ScalarFunctionChunk) Len() int {
	if c == nil {
		return 0
	}
	return c.rows
}

// ArgCount returns the number of input argument columns in the current batch.
func (c *ScalarFunctionChunk) ArgCount() int {
	if c == nil {
		return 0
	}
	return len(c.argVecs)
}

// Arg decodes the value at input column col and row index row.
func (c *ScalarFunctionChunk) Arg(col, row int) (Value, error) {
	if c == nil || c.output == 0 {
		return Value{}, fmt.Errorf("%w: invalid scalar function chunk", ErrInvalidInput)
	}
	if col < 0 || col >= len(c.argVecs) {
		return Value{}, fmt.Errorf("%w: argument column %d out of range [0,%d)", ErrOutOfRange, col, len(c.argVecs))
	}
	if row < 0 || row >= c.rows {
		return Value{}, fmt.Errorf("%w: row %d out of range [0,%d)", ErrOutOfRange, row, c.rows)
	}
	return decodeVectorValue(c.api, &c.argPlans[col], c.argVecs[col], uint64(row), c.limits)
}

// Row decodes all input argument values for row into dst (reusing dst if it has
// sufficient capacity).
func (c *ScalarFunctionChunk) Row(row int, dst []Value) ([]Value, error) {
	if c == nil || c.output == 0 {
		return nil, fmt.Errorf("%w: invalid scalar function chunk", ErrInvalidInput)
	}
	if row < 0 || row >= c.rows {
		return nil, fmt.Errorf("%w: row %d out of range [0,%d)", ErrOutOfRange, row, c.rows)
	}
	n := len(c.argVecs)
	if cap(dst) < n {
		dst = make([]Value, n)
	} else {
		dst = dst[:n]
	}
	for col := range n {
		v, err := decodeVectorValue(c.api, &c.argPlans[col], c.argVecs[col], uint64(row), c.limits)
		if err != nil {
			return nil, fmt.Errorf("arg[%d]: %w", col, err)
		}
		dst[col] = v
	}
	return dst, nil
}

// SetOutput writes val into the output vector at row, rolling back any partial
// list/map vector mutations on error.
func (c *ScalarFunctionChunk) SetOutput(row int, val Value) error {
	if c == nil || c.output == 0 {
		return fmt.Errorf("%w: invalid scalar function chunk", ErrInvalidInput)
	}
	if row < 0 || row >= c.rows {
		return fmt.Errorf("%w: row %d out of range [0,%d)", ErrOutOfRange, row, c.rows)
	}
	var undo []listSizeUndo
	if err := writeVectorValue(c.api, c.retPlan, c.output, uint64(row), val, 0, c.limits, &undo); err != nil {
		for _, u := range slices.Backward(undo) {
			_ = c.api.ListVectorSetSize(u.vec, u.prevSize)
		}
		return err
	}
	return nil
}

// ScalarFunctionSpec defines a pure-Go DuckDB scalar function registered via
// Conn.RegisterScalarFunction or Conn.RegisterScalarFunctionSet.
//
// Exactly one of Execute or ExecuteChunk must be provided. When Execute is used
// and SpecialNulls is false (the default), rows with any NULL input argument
// automatically produce SQL NULL without invoking Execute.
type ScalarFunctionSpec struct {
	Name         string
	Parameters   []LogicalType
	Varargs      *LogicalType
	ReturnType   LogicalType
	Volatile     bool
	SpecialNulls bool
	Bind         func(input ScalarFunctionBindInput) (any, error)
	Init         func(bindData any) (any, error)
	Execute      func(bindData, localState any, args []Value) (Value, error)
	ExecuteChunk func(bindData, localState any, chunk *ScalarFunctionChunk) error
}

type scalarFuncDef struct {
	api            *abi.API
	limits         DecodeLimits
	spec           ScalarFunctionSpec
	fixedParamPlan []*columnPlan // nil slot means dynamic (TypeAny)
	hasVarargs     bool
	returnPlan     columnPlan
}

type scalarFuncBindState struct {
	userData any
}

type scalarFuncLocalState struct {
	userData any
}

var (
	scalarFuncDispatchAPI atomic.Pointer[abi.API]
	globalSFDispatcher    = scalarFuncDispatcher{}
)

type scalarFuncDispatcher struct{}

func setScalarBindErr(api *abi.API, info uintptr, msg string) {
	cmsg := cErrorString(msg)
	api.ScalarFunctionBindSetError(info, &cmsg[0])
	runtime.KeepAlive(cmsg)
}

func setScalarInitErr(api *abi.API, info uintptr, msg string) {
	cmsg := cErrorString(msg)
	api.ScalarFunctionInitSetError(info, &cmsg[0])
	runtime.KeepAlive(cmsg)
}

func setScalarExecErr(api *abi.API, info uintptr, msg string) {
	cmsg := cErrorString(msg)
	api.ScalarFunctionSetError(info, &cmsg[0])
	runtime.KeepAlive(cmsg)
}

// resolveChunkInputPlans builds or reuses column plans for all columns of an
// input DataChunk. Concrete fixed parameters reuse precomputed plans; TypeAny
// and Varargs columns inspect VectorGetColumnType once per chunk.
func resolveChunkInputPlans(api *abi.API, input uintptr, fixedPlans []*columnPlan, hasVarargs bool, limits DecodeLimits) ([]uintptr, []columnPlan, error) {
	colCount := api.DataChunkGetColumnCount(input)
	if colCount > limits.MaxCollectionLen || colCount > math.MaxInt {
		return nil, nil, fmt.Errorf("%w: input column count %d exceeds limit", ErrLimitExceeded, colCount)
	}
	n := int(colCount)
	vecs := make([]uintptr, n)
	plans := make([]columnPlan, n)
	for i := range n {
		vec := api.DataChunkGetVector(input, uint64(i))
		vecs[i] = vec
		if !hasVarargs && i < len(fixedPlans) && fixedPlans[i] != nil {
			plans[i] = *fixedPlans[i]
			continue
		}
		if i < len(fixedPlans) && fixedPlans[i] != nil {
			plans[i] = *fixedPlans[i]
			continue
		}
		lt := api.VectorGetColumnType(vec)
		plan, err := buildColumnPlan(api, lt, 0, limits)
		if lt != 0 {
			api.DestroyLogicalType(&lt)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("input column %d: %w", i, err)
		}
		plans[i] = plan
	}
	return vecs, plans, nil
}

func (scalarFuncDispatcher) Bind(info uintptr) {
	bootAPI := scalarFuncDispatchAPI.Load()
	if bootAPI == nil || bootAPI.ScalarFunctionBindGetExtraInfo == nil {
		return
	}
	extraID := bootAPI.ScalarFunctionBindGetExtraInfo(info)
	raw, ok := abi.LookupHookHandle(extraID)
	if !ok {
		if bootAPI.ScalarFunctionBindSetError != nil {
			setScalarBindErr(bootAPI, info, "scalar function definition handle not found")
		}
		return
	}
	def, ok := raw.(*scalarFuncDef)
	if !ok || def == nil {
		if bootAPI.ScalarFunctionBindSetError != nil {
			setScalarBindErr(bootAPI, info, "invalid scalar function definition handle")
		}
		return
	}
	api := def.api

	defer func() {
		if r := recover(); r != nil {
			setScalarBindErr(api, info, fmt.Sprintf("scalar function %q bind panic: %v", def.spec.Name, r))
		}
	}()

	argCount := api.ScalarFunctionBindGetArgumentCount(info)
	if argCount > def.limits.MaxCollectionLen || argCount > math.MaxInt {
		setScalarBindErr(api, info, fmt.Sprintf("scalar function %q argument count %d exceeds limit", def.spec.Name, argCount))
		return
	}

	var clientCtx uintptr
	defer func() {
		if clientCtx != 0 {
			api.DestroyClientContext(&clientCtx)
		}
	}()

	bindArgs := make([]ScalarFunctionBindArg, int(argCount))
	for i := range argCount {
		expr := api.ScalarFunctionBindGetArgument(info, i)
		if expr == 0 {
			continue
		}
		ltHandle := api.ExpressionReturnType(expr)
		var argLT LogicalType
		if ltHandle != 0 {
			tid := api.GetTypeID(ltHandle)
			switch tid {
			case abi.TypeAny:
				argLT = AnyType()
			case abi.TypeInvalid:
				argLT = LogicalType{Kind: TypeInvalid}
			default:
				plan, err := buildColumnPlan(api, ltHandle, 0, def.limits)
				if err != nil {
					api.DestroyLogicalType(&ltHandle)
					api.DestroyExpression(&expr)
					setScalarBindErr(api, info, fmt.Sprintf("scalar function %q bind arg %d type: %v", def.spec.Name, i, err))
					return
				}
				argLT = plan.lt
			}
			api.DestroyLogicalType(&ltHandle)
		}

		foldable := api.ExpressionIsFoldable(expr)
		var foldedVal Value
		if foldable {
			if clientCtx == 0 {
				api.ScalarFunctionGetClientContext(info, &clientCtx)
			}
			if clientCtx != 0 {
				var valHandle uintptr
				errData := api.ExpressionFold(clientCtx, expr, &valHandle)
				if errData != 0 {
					hasErr := api.ErrorDataHasError(errData)
					var errMsg string
					if hasErr {
						errMsg = abi.GoString(api.ErrorDataMessage(errData))
					}
					api.DestroyErrorData(&errData)
					if hasErr {
						if valHandle != 0 {
							api.DestroyValue(&valHandle)
						}
						api.DestroyExpression(&expr)
						setScalarBindErr(api, info, fmt.Sprintf("scalar function %q fold arg %d: %s", def.spec.Name, i, errMsg))
						return
					}
				}
				if valHandle != 0 {
					v, err := decodeNativeValue(api, valHandle, 0, def.limits)
					api.DestroyValue(&valHandle)
					if err != nil {
						api.DestroyExpression(&expr)
						setScalarBindErr(api, info, fmt.Sprintf("scalar function %q decode folded arg %d: %v", def.spec.Name, i, err))
						return
					}
					foldedVal = v
				}
			}
		}
		api.DestroyExpression(&expr)

		bindArgs[i] = ScalarFunctionBindArg{
			Type:       argLT,
			IsFoldable: foldable,
			Value:      foldedVal,
		}
	}

	userData, err := def.spec.Bind(ScalarFunctionBindInput{Args: bindArgs})
	if err != nil {
		setScalarBindErr(api, info, err.Error())
		return
	}

	bindID := abi.RegisterHookHandle(&scalarFuncBindState{userData: userData})
	api.ScalarFunctionSetBindData(info, bindID, abi.HookDeleteCallback())
	api.ScalarFunctionSetBindDataCopy(info, abi.HookCopyCallback())
}

func (scalarFuncDispatcher) Init(info uintptr) {
	bootAPI := scalarFuncDispatchAPI.Load()
	if bootAPI == nil || bootAPI.ScalarFunctionInitGetExtraInfo == nil {
		return
	}
	extraID := bootAPI.ScalarFunctionInitGetExtraInfo(info)
	raw, ok := abi.LookupHookHandle(extraID)
	if !ok {
		if bootAPI.ScalarFunctionInitSetError != nil {
			setScalarInitErr(bootAPI, info, "scalar function definition handle not found")
		}
		return
	}
	def, ok := raw.(*scalarFuncDef)
	if !ok || def == nil {
		if bootAPI.ScalarFunctionInitSetError != nil {
			setScalarInitErr(bootAPI, info, "invalid scalar function definition handle")
		}
		return
	}
	api := def.api

	defer func() {
		if r := recover(); r != nil {
			setScalarInitErr(api, info, fmt.Sprintf("scalar function %q init panic: %v", def.spec.Name, r))
		}
	}()

	var bindData any
	if def.spec.Bind != nil {
		bindID := api.ScalarFunctionInitGetBindData(info)
		if rawBind, ok := abi.LookupHookHandle(bindID); ok {
			if bs, ok := rawBind.(*scalarFuncBindState); ok && bs != nil {
				bindData = bs.userData
			}
		}
	}

	localState, err := def.spec.Init(bindData)
	if err != nil {
		setScalarInitErr(api, info, err.Error())
		return
	}

	stateID := abi.RegisterHookHandle(&scalarFuncLocalState{userData: localState})
	api.ScalarFunctionInitSetState(info, stateID, abi.HookDeleteCallback())
}

func (scalarFuncDispatcher) Execute(info, input, output uintptr) {
	bootAPI := scalarFuncDispatchAPI.Load()
	if bootAPI == nil || bootAPI.ScalarFunctionGetExtraInfo == nil {
		return
	}
	extraID := bootAPI.ScalarFunctionGetExtraInfo(info)
	raw, ok := abi.LookupHookHandle(extraID)
	if !ok {
		if bootAPI.ScalarFunctionSetError != nil {
			setScalarExecErr(bootAPI, info, "scalar function definition handle not found")
		}
		return
	}
	def, ok := raw.(*scalarFuncDef)
	if !ok || def == nil {
		if bootAPI.ScalarFunctionSetError != nil {
			setScalarExecErr(bootAPI, info, "invalid scalar function definition handle")
		}
		return
	}
	api := def.api

	defer func() {
		if r := recover(); r != nil {
			setScalarExecErr(api, info, fmt.Sprintf("scalar function %q execute panic: %v", def.spec.Name, r))
		}
	}()

	var bindData any
	if def.spec.Bind != nil {
		bindID := api.ScalarFunctionGetBindData(info)
		if rawBind, ok := abi.LookupHookHandle(bindID); ok {
			if bs, ok := rawBind.(*scalarFuncBindState); ok && bs != nil {
				bindData = bs.userData
			}
		}
	}

	var localState any
	if def.spec.Init != nil {
		stateID := api.ScalarFunctionGetState(info)
		if rawState, ok := abi.LookupHookHandle(stateID); ok {
			if ls, ok := rawState.(*scalarFuncLocalState); ok && ls != nil {
				localState = ls.userData
			}
		}
	}

	rowCount := api.DataChunkGetSize(input)
	if rowCount == 0 {
		return
	}
	if rowCount > math.MaxInt {
		setScalarExecErr(api, info, fmt.Sprintf("scalar function %q chunk size %d overflows int", def.spec.Name, rowCount))
		return
	}

	argVecs, argPlans, err := resolveChunkInputPlans(api, input, def.fixedParamPlan, def.hasVarargs, def.limits)
	if err != nil {
		setScalarExecErr(api, info, fmt.Sprintf("scalar function %q: %v", def.spec.Name, err))
		return
	}

	if def.spec.ExecuteChunk != nil {
		chunk := ScalarFunctionChunk{
			api:      api,
			rows:     int(rowCount),
			argVecs:  argVecs,
			argPlans: argPlans,
			output:   output,
			retPlan:  &def.returnPlan,
			limits:   def.limits,
		}
		if err := def.spec.ExecuteChunk(bindData, localState, &chunk); err != nil {
			setScalarExecErr(api, info, err.Error())
		}
		return
	}

	colCount := len(argVecs)
	argValidities := make([]uintptr, colCount)
	if !def.spec.SpecialNulls {
		for col := range colCount {
			argValidities[col] = api.VectorGetValidity(argVecs[col])
		}
	}
	rowArgs := make([]Value, colCount)
	var undo []listSizeUndo

	for row := range rowCount {
		if !def.spec.SpecialNulls {
			hasNull := false
			for col := range colCount {
				if argPlans[col].lt.Kind == TypeSQLNull || !rowValid(argValidities[col], row) {
					hasNull = true
					break
				}
			}
			if hasNull {
				undo = undo[:0]
				if err := writeVectorValue(api, &def.returnPlan, output, row, NullValue(), 0, def.limits, &undo); err != nil {
					setScalarExecErr(api, info, fmt.Sprintf("scalar function %q write NULL row %d: %v", def.spec.Name, row, err))
					return
				}
				continue
			}
		}

		for col := range colCount {
			v, err := decodeVectorValue(api, &argPlans[col], argVecs[col], row, def.limits)
			if err != nil {
				setScalarExecErr(api, info, fmt.Sprintf("scalar function %q decode arg[%d] row %d: %v", def.spec.Name, col, row, err))
				return
			}
			rowArgs[col] = v
		}

		outVal, err := def.spec.Execute(bindData, localState, rowArgs)
		if err != nil {
			setScalarExecErr(api, info, err.Error())
			return
		}

		undo = undo[:0]
		if err := writeVectorValue(api, &def.returnPlan, output, row, outVal, 0, def.limits, &undo); err != nil {
			for _, u := range slices.Backward(undo) {
				_ = api.ListVectorSetSize(u.vec, u.prevSize)
			}
			setScalarExecErr(api, info, fmt.Sprintf("scalar function %q write output row %d: %v", def.spec.Name, row, err))
			return
		}
	}
}

func validateScalarFunctionSpec(spec ScalarFunctionSpec, requireName bool) error {
	if requireName && spec.Name == "" {
		return fmt.Errorf("%w: scalar function name must not be empty", ErrInvalidInput)
	}
	if spec.ReturnType.Kind == TypeInvalid || spec.ReturnType.Kind == TypeAny {
		return fmt.Errorf("%w: scalar function %q requires a concrete return type", ErrInvalidInput, spec.Name)
	}
	if (spec.Execute == nil) == (spec.ExecuteChunk == nil) {
		return fmt.Errorf("%w: scalar function %q must set exactly one of Execute or ExecuteChunk", ErrInvalidInput, spec.Name)
	}
	for i, pt := range spec.Parameters {
		if pt.Kind == TypeInvalid {
			return fmt.Errorf("%w: scalar function %q parameter %d has INVALID type", ErrInvalidInput, spec.Name, i)
		}
	}
	if spec.Varargs != nil && spec.Varargs.Kind == TypeInvalid {
		return fmt.Errorf("%w: scalar function %q varargs has INVALID type", ErrInvalidInput, spec.Name)
	}
	return nil
}

func buildNativeScalarFunctionLocked(api *abi.API, limits DecodeLimits, spec ScalarFunctionSpec) (uintptr, uintptr, error) {
	var cname []byte
	var err error
	if spec.Name != "" {
		cname, err = abi.CString(spec.Name)
		if err != nil {
			return 0, 0, fmt.Errorf("%w: scalar function name %q: %v", ErrInvalidInput, spec.Name, err)
		}
	}

	retLT, err := createNativeLogicalType(api, spec.ReturnType, 0, limits)
	if err != nil {
		return 0, 0, fmt.Errorf("scalar function %q return type: %w", spec.Name, err)
	}
	defer api.DestroyLogicalType(&retLT)

	retPlan, err := buildColumnPlan(api, retLT, 0, limits)
	if err != nil {
		return 0, 0, fmt.Errorf("scalar function %q return type: %w", spec.Name, err)
	}

	paramLTs := make([]uintptr, len(spec.Parameters))
	fixedPlans := make([]*columnPlan, len(spec.Parameters))
	defer func() {
		for i := range paramLTs {
			if paramLTs[i] != 0 {
				api.DestroyLogicalType(&paramLTs[i])
			}
		}
	}()
	for i, pt := range spec.Parameters {
		h, err := createNativeLogicalType(api, pt, 0, limits)
		if err != nil {
			return 0, 0, fmt.Errorf("scalar function %q parameter %d: %w", spec.Name, i, err)
		}
		paramLTs[i] = h
		if pt.Kind != TypeAny {
			p, err := buildColumnPlan(api, h, 0, limits)
			if err != nil {
				return 0, 0, fmt.Errorf("scalar function %q parameter %d: %w", spec.Name, i, err)
			}
			fixedPlans[i] = &p
		}
	}

	var varargsLT uintptr
	if spec.Varargs != nil {
		varargsLT, err = createNativeLogicalType(api, *spec.Varargs, 0, limits)
		if err != nil {
			return 0, 0, fmt.Errorf("scalar function %q varargs: %w", spec.Name, err)
		}
		defer api.DestroyLogicalType(&varargsLT)
	}

	sf := api.CreateScalarFunction()
	if sf == 0 {
		return 0, 0, nativeError("create_scalar_function", abi.ErrorOutOfMemory, "")
	}

	if len(cname) > 0 {
		api.ScalarFunctionSetName(sf, &cname[0])
		runtime.KeepAlive(cname)
	}
	for _, h := range paramLTs {
		api.ScalarFunctionAddParameter(sf, h)
	}
	if varargsLT != 0 {
		api.ScalarFunctionSetVarargs(sf, varargsLT)
	}
	api.ScalarFunctionSetReturnType(sf, retLT)
	if spec.Volatile {
		api.ScalarFunctionSetVolatile(sf)
	}
	if spec.SpecialNulls {
		api.ScalarFunctionSetSpecialHandling(sf)
	}

	def := &scalarFuncDef{
		api:            api,
		limits:         limits,
		spec:           spec,
		fixedParamPlan: fixedPlans,
		hasVarargs:     spec.Varargs != nil,
		returnPlan:     retPlan,
	}

	scalarFuncDispatchAPI.Store(api)
	abi.SetScalarFunctionDispatcher(globalSFDispatcher)

	extraID := abi.RegisterHookHandle(def)
	api.ScalarFunctionSetExtraInfo(sf, extraID, abi.HookDeleteCallback())
	if spec.Bind != nil {
		api.ScalarFunctionSetBind(sf, abi.ScalarFunctionBindCallback())
	}
	if spec.Init != nil {
		api.ScalarFunctionSetInit(sf, abi.ScalarFunctionInitCallback())
	}
	api.ScalarFunctionSetFunction(sf, abi.ScalarFunctionExecCallback())
	return sf, extraID, nil
}

// RegisterScalarFunction registers a Go-backed SQL scalar function on c.
func (c *Conn) RegisterScalarFunction(spec ScalarFunctionSpec) error {
	return c.registerScalarFunctionWithTx(nil, spec)
}

// RegisterScalarFunction registers a Go-backed SQL scalar function within tx.
func (tx *Tx) RegisterScalarFunction(spec ScalarFunctionSpec) error {
	if tx == nil || tx.conn == nil {
		return ErrTxDone
	}
	return tx.conn.registerScalarFunctionWithTx(tx, spec)
}

func (c *Conn) registerScalarFunctionWithTx(tx *Tx, spec ScalarFunctionSpec) error {
	if c == nil {
		return ErrClosed
	}
	if err := validateScalarFunctionSpec(spec, true); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkTxLocked(tx); err != nil {
		return err
	}
	if err := c.usableLocked(); err != nil {
		return err
	}

	api := c.api
	sf, extraID, err := buildNativeScalarFunctionLocked(api, c.limits, spec)
	if err != nil {
		return err
	}
	state := api.RegisterScalarFunction(c.handle, sf)
	api.DestroyScalarFunction(&sf)
	if state != abi.StateSuccess {
		abi.ReleaseHookHandle(extraID)
		return nativeError("register_scalar_function", abi.ErrorInvalid, fmt.Sprintf("failed to register scalar function %q", spec.Name))
	}
	return nil
}

// RegisterScalarFunctionSet registers an overloaded set of Go-backed SQL scalar
// functions under name on c. Each overload may omit Name (inheriting name) or
// specify Name == name.
func (c *Conn) RegisterScalarFunctionSet(name string, specs ...ScalarFunctionSpec) error {
	return c.registerScalarFunctionSetWithTx(nil, name, specs)
}

// RegisterScalarFunctionSet registers an overloaded set of Go-backed SQL scalar
// functions under name within tx.
func (tx *Tx) RegisterScalarFunctionSet(name string, specs ...ScalarFunctionSpec) error {
	if tx == nil || tx.conn == nil {
		return ErrTxDone
	}
	return tx.conn.registerScalarFunctionSetWithTx(tx, name, specs)
}

func (c *Conn) registerScalarFunctionSetWithTx(tx *Tx, name string, specs []ScalarFunctionSpec) error {
	if c == nil {
		return ErrClosed
	}
	if name == "" {
		return fmt.Errorf("%w: scalar function set name must not be empty", ErrInvalidInput)
	}
	if len(specs) == 0 {
		return fmt.Errorf("%w: scalar function set %q requires at least one overload", ErrInvalidInput, name)
	}
	cname, err := abi.CString(name)
	if err != nil {
		return fmt.Errorf("%w: scalar function set name %q: %v", ErrInvalidInput, name, err)
	}
	normalized := make([]ScalarFunctionSpec, len(specs))
	for i, s := range specs {
		if s.Name == "" {
			s.Name = name
		} else if s.Name != name {
			return fmt.Errorf("%w: scalar function overload %d name %q does not match set name %q", ErrInvalidInput, i, s.Name, name)
		}
		if err := validateScalarFunctionSpec(s, true); err != nil {
			return err
		}
		normalized[i] = s
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkTxLocked(tx); err != nil {
		return err
	}
	if err := c.usableLocked(); err != nil {
		return err
	}

	api := c.api
	set := api.CreateScalarFunctionSet(&cname[0])
	runtime.KeepAlive(cname)
	if set == 0 {
		return nativeError("create_scalar_function_set", abi.ErrorOutOfMemory, "")
	}

	extraIDs := make([]uintptr, 0, len(normalized))
	releaseAll := func() {
		for _, id := range extraIDs {
			abi.ReleaseHookHandle(id)
		}
	}

	for i, spec := range normalized {
		sf, extraID, err := buildNativeScalarFunctionLocked(api, c.limits, spec)
		if err != nil {
			api.DestroyScalarFunctionSet(&set)
			releaseAll()
			return err
		}
		extraIDs = append(extraIDs, extraID)
		addState := api.AddScalarFunctionToSet(set, sf)
		api.DestroyScalarFunction(&sf)
		if addState != abi.StateSuccess {
			api.DestroyScalarFunctionSet(&set)
			releaseAll()
			return nativeError("add_scalar_function_to_set", abi.ErrorInvalid, fmt.Sprintf("failed to add overload %d to scalar function set %q", i, name))
		}
	}

	state := api.RegisterScalarFunctionSet(c.handle, set)
	api.DestroyScalarFunctionSet(&set)
	if state != abi.StateSuccess {
		releaseAll()
		return nativeError("register_scalar_function_set", abi.ErrorInvalid, fmt.Sprintf("failed to register scalar function set %q", name))
	}
	return nil
}
