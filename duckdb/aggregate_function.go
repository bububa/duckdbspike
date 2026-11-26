package duckdb

import (
	"fmt"
	"runtime"
	"slices"
	"sync/atomic"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

// AggregateFunctionSpec defines a pure-Go DuckDB aggregate function registered
// via Conn.RegisterAggregateFunction or Conn.RegisterAggregateFunctionSet.
//
// Each group's Go state returned by Init is referenced by an 8-byte integer
// handle ID inside DuckDB's aggregate hash table (never a raw Go pointer), so
// it is safe across C++ memory moves, Go GC, and multi-threaded hash
// aggregation, and is released deterministically by DuckDB's state destructor.
type AggregateFunctionSpec struct {
	Name         string
	Parameters   []LogicalType
	ReturnType   LogicalType
	SpecialNulls bool
	Init         func() any
	Update       func(state any, args []Value) error
	Combine      func(source, target any) error
	Finalize     func(state any) (Value, error)
}

type aggFuncDef struct {
	api            *abi.API
	limits         DecodeLimits
	spec           AggregateFunctionSpec
	fixedParamPlan []*columnPlan // nil slot means dynamic (TypeAny)
	returnPlan     columnPlan
}

type aggStateBox struct {
	userData any
}

var (
	aggFuncDispatchAPI atomic.Pointer[abi.API]
	globalAFDispatcher = aggFuncDispatcher{}
)

const aggStateSlotSize = uint64(unsafe.Sizeof(uintptr(0)))

type aggFuncDispatcher struct{}

func setAggErr(api *abi.API, info uintptr, msg string) {
	cmsg := cErrorString(msg)
	api.AggregateFunctionSetError(info, &cmsg[0])
	runtime.KeepAlive(cmsg)
}

func aggStatePtrAt(statesArray uintptr, index uint64) uintptr {
	return *(*uintptr)(abi.Pointer(statesArray + uintptr(index)*unsafe.Sizeof(uintptr(0))))
}

func readAggStateID(statePtr uintptr) uintptr {
	if statePtr == 0 {
		return 0
	}
	return *(*uintptr)(abi.Pointer(statePtr))
}

func writeAggStateID(statePtr uintptr, id uintptr) {
	if statePtr == 0 {
		return
	}
	*(*uintptr)(abi.Pointer(statePtr)) = id
}

func lookupAggFuncDef(info uintptr) (*aggFuncDef, *abi.API) {
	bootAPI := aggFuncDispatchAPI.Load()
	if bootAPI == nil || bootAPI.AggregateFunctionGetExtraInfo == nil {
		return nil, nil
	}
	extraID := bootAPI.AggregateFunctionGetExtraInfo(info)
	raw, ok := abi.LookupHookHandle(extraID)
	if !ok {
		if bootAPI.AggregateFunctionSetError != nil {
			setAggErr(bootAPI, info, "aggregate function definition handle not found")
		}
		return nil, nil
	}
	def, ok := raw.(*aggFuncDef)
	if !ok || def == nil {
		if bootAPI.AggregateFunctionSetError != nil {
			setAggErr(bootAPI, info, "invalid aggregate function definition handle")
		}
		return nil, nil
	}
	return def, def.api
}

func (aggFuncDispatcher) StateSize(_ uintptr) uint64 {
	return aggStateSlotSize
}

func (aggFuncDispatcher) Init(info, state uintptr) {
	writeAggStateID(state, 0)
	def, api := lookupAggFuncDef(info)
	if def == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			setAggErr(api, info, fmt.Sprintf("aggregate function %q init panic: %v", def.spec.Name, r))
		}
	}()

	userState := def.spec.Init()
	id := abi.RegisterHookHandle(&aggStateBox{userData: userState})
	writeAggStateID(state, id)
}

func (aggFuncDispatcher) Update(info, input, states uintptr) {
	def, api := lookupAggFuncDef(info)
	if def == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			setAggErr(api, info, fmt.Sprintf("aggregate function %q update panic: %v", def.spec.Name, r))
		}
	}()

	rowCount := api.DataChunkGetSize(input)
	if rowCount == 0 {
		return
	}
	argVecs, argPlans, err := resolveChunkInputPlans(api, input, def.fixedParamPlan, false, def.limits)
	if err != nil {
		setAggErr(api, info, fmt.Sprintf("aggregate function %q: %v", def.spec.Name, err))
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
				continue
			}
		}

		sp := aggStatePtrAt(states, row)
		id := readAggStateID(sp)
		rawState, ok := abi.LookupHookHandle(id)
		if !ok {
			setAggErr(api, info, fmt.Sprintf("aggregate function %q update missing state handle at row %d", def.spec.Name, row))
			return
		}
		box := rawState.(*aggStateBox)

		for col := range colCount {
			v, err := decodeVectorValue(api, &argPlans[col], argVecs[col], row, def.limits)
			if err != nil {
				setAggErr(api, info, fmt.Sprintf("aggregate function %q decode arg[%d] row %d: %v", def.spec.Name, col, row, err))
				return
			}
			rowArgs[col] = v
		}

		if err := def.spec.Update(box.userData, rowArgs); err != nil {
			setAggErr(api, info, err.Error())
			return
		}
	}
}

func (aggFuncDispatcher) Combine(info, source, target uintptr, count uint64) {
	def, api := lookupAggFuncDef(info)
	if def == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			setAggErr(api, info, fmt.Sprintf("aggregate function %q combine panic: %v", def.spec.Name, r))
		}
	}()

	for i := range count {
		srcID := readAggStateID(aggStatePtrAt(source, i))
		dstID := readAggStateID(aggStatePtrAt(target, i))
		rawSrc, okSrc := abi.LookupHookHandle(srcID)
		rawDst, okDst := abi.LookupHookHandle(dstID)
		if !okSrc || !okDst {
			setAggErr(api, info, fmt.Sprintf("aggregate function %q combine missing state handle at index %d", def.spec.Name, i))
			return
		}
		if err := def.spec.Combine(rawSrc.(*aggStateBox).userData, rawDst.(*aggStateBox).userData); err != nil {
			setAggErr(api, info, err.Error())
			return
		}
	}
}

func (aggFuncDispatcher) Finalize(info, source, result uintptr, count, offset uint64) {
	def, api := lookupAggFuncDef(info)
	if def == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			setAggErr(api, info, fmt.Sprintf("aggregate function %q finalize panic: %v", def.spec.Name, r))
		}
	}()

	var undo []listSizeUndo
	for i := range count {
		srcID := readAggStateID(aggStatePtrAt(source, i))
		rawSrc, ok := abi.LookupHookHandle(srcID)
		if !ok {
			setAggErr(api, info, fmt.Sprintf("aggregate function %q finalize missing state handle at index %d", def.spec.Name, i))
			return
		}
		val, err := def.spec.Finalize(rawSrc.(*aggStateBox).userData)
		if err != nil {
			setAggErr(api, info, err.Error())
			return
		}
		undo = undo[:0]
		if err := writeVectorValue(api, &def.returnPlan, result, offset+i, val, 0, def.limits, &undo); err != nil {
			for _, u := range slices.Backward(undo) {
				_ = api.ListVectorSetSize(u.vec, u.prevSize)
			}
			setAggErr(api, info, fmt.Sprintf("aggregate function %q write result row %d: %v", def.spec.Name, offset+i, err))
			return
		}
	}
}

func (aggFuncDispatcher) Destroy(states uintptr, count uint64) {
	for i := range count {
		sp := aggStatePtrAt(states, i)
		if sp == 0 {
			continue
		}
		id := readAggStateID(sp)
		writeAggStateID(sp, 0)
		abi.ReleaseHookHandle(id)
	}
}

func validateAggregateFunctionSpec(spec AggregateFunctionSpec, requireName bool) error {
	if requireName && spec.Name == "" {
		return fmt.Errorf("%w: aggregate function name must not be empty", ErrInvalidInput)
	}
	if spec.ReturnType.Kind == TypeInvalid || spec.ReturnType.Kind == TypeAny {
		return fmt.Errorf("%w: aggregate function %q requires a concrete return type", ErrInvalidInput, spec.Name)
	}
	if spec.Init == nil || spec.Update == nil || spec.Combine == nil || spec.Finalize == nil {
		return fmt.Errorf("%w: aggregate function %q requires Init, Update, Combine, and Finalize callbacks", ErrInvalidInput, spec.Name)
	}
	for i, pt := range spec.Parameters {
		if pt.Kind == TypeInvalid {
			return fmt.Errorf("%w: aggregate function %q parameter %d has INVALID type", ErrInvalidInput, spec.Name, i)
		}
	}
	return nil
}

func buildNativeAggregateFunctionLocked(api *abi.API, limits DecodeLimits, spec AggregateFunctionSpec) (uintptr, uintptr, error) {
	var cname []byte
	var err error
	if spec.Name != "" {
		cname, err = abi.CString(spec.Name)
		if err != nil {
			return 0, 0, fmt.Errorf("%w: aggregate function name %q: %v", ErrInvalidInput, spec.Name, err)
		}
	}

	retLT, err := createNativeLogicalType(api, spec.ReturnType, 0, limits)
	if err != nil {
		return 0, 0, fmt.Errorf("aggregate function %q return type: %w", spec.Name, err)
	}
	defer api.DestroyLogicalType(&retLT)

	retPlan, err := buildColumnPlan(api, retLT, 0, limits)
	if err != nil {
		return 0, 0, fmt.Errorf("aggregate function %q return type: %w", spec.Name, err)
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
			return 0, 0, fmt.Errorf("aggregate function %q parameter %d: %w", spec.Name, i, err)
		}
		paramLTs[i] = h
		if pt.Kind != TypeAny {
			p, err := buildColumnPlan(api, h, 0, limits)
			if err != nil {
				return 0, 0, fmt.Errorf("aggregate function %q parameter %d: %w", spec.Name, i, err)
			}
			fixedPlans[i] = &p
		}
	}

	af := api.CreateAggregateFunction()
	if af == 0 {
		return 0, 0, nativeError("create_aggregate_function", abi.ErrorOutOfMemory, "")
	}

	if len(cname) > 0 {
		api.AggregateFunctionSetName(af, &cname[0])
		runtime.KeepAlive(cname)
	}
	for _, h := range paramLTs {
		api.AggregateFunctionAddParameter(af, h)
	}
	api.AggregateFunctionSetReturnType(af, retLT)
	if spec.SpecialNulls {
		api.AggregateFunctionSetSpecialHandling(af)
	}

	def := &aggFuncDef{
		api:            api,
		limits:         limits,
		spec:           spec,
		fixedParamPlan: fixedPlans,
		returnPlan:     retPlan,
	}

	aggFuncDispatchAPI.Store(api)
	abi.SetAggregateFunctionDispatcher(globalAFDispatcher)

	extraID := abi.RegisterHookHandle(def)
	api.AggregateFunctionSetExtraInfo(af, extraID, abi.HookDeleteCallback())
	api.AggregateFunctionSetFunctions(
		af,
		abi.AggregateStateSizeCallback(),
		abi.AggregateInitCallback(),
		abi.AggregateUpdateCallback(),
		abi.AggregateCombineCallback(),
		abi.AggregateFinalizeCallback(),
	)
	api.AggregateFunctionSetDestructor(af, abi.AggregateDestroyCallback())
	return af, extraID, nil
}

// RegisterAggregateFunction registers a Go-backed SQL aggregate function on c.
func (c *Conn) RegisterAggregateFunction(spec AggregateFunctionSpec) error {
	return c.registerAggregateFunctionWithTx(nil, spec)
}

// RegisterAggregateFunction registers a Go-backed SQL aggregate function within tx.
func (tx *Tx) RegisterAggregateFunction(spec AggregateFunctionSpec) error {
	if tx == nil || tx.conn == nil {
		return ErrTxDone
	}
	return tx.conn.registerAggregateFunctionWithTx(tx, spec)
}

func (c *Conn) registerAggregateFunctionWithTx(tx *Tx, spec AggregateFunctionSpec) error {
	if c == nil {
		return ErrClosed
	}
	if err := validateAggregateFunctionSpec(spec, true); err != nil {
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
	af, extraID, err := buildNativeAggregateFunctionLocked(api, c.limits, spec)
	if err != nil {
		return err
	}
	state := api.RegisterAggregateFunction(c.handle, af)
	api.DestroyAggregateFunction(&af)
	if state != abi.StateSuccess {
		abi.ReleaseHookHandle(extraID)
		return nativeError("register_aggregate_function", abi.ErrorInvalid, fmt.Sprintf("failed to register aggregate function %q", spec.Name))
	}
	return nil
}

// RegisterAggregateFunctionSet registers an overloaded set of Go-backed SQL
// aggregate functions under name on c. Each overload may omit Name (inheriting
// name) or specify Name == name.
func (c *Conn) RegisterAggregateFunctionSet(name string, specs ...AggregateFunctionSpec) error {
	return c.registerAggregateFunctionSetWithTx(nil, name, specs)
}

// RegisterAggregateFunctionSet registers an overloaded set of Go-backed SQL
// aggregate functions under name within tx.
func (tx *Tx) RegisterAggregateFunctionSet(name string, specs ...AggregateFunctionSpec) error {
	if tx == nil || tx.conn == nil {
		return ErrTxDone
	}
	return tx.conn.registerAggregateFunctionSetWithTx(tx, name, specs)
}

func (c *Conn) registerAggregateFunctionSetWithTx(tx *Tx, name string, specs []AggregateFunctionSpec) error {
	if c == nil {
		return ErrClosed
	}
	if name == "" {
		return fmt.Errorf("%w: aggregate function set name must not be empty", ErrInvalidInput)
	}
	if len(specs) == 0 {
		return fmt.Errorf("%w: aggregate function set %q requires at least one overload", ErrInvalidInput, name)
	}
	cname, err := abi.CString(name)
	if err != nil {
		return fmt.Errorf("%w: aggregate function set name %q: %v", ErrInvalidInput, name, err)
	}
	normalized := make([]AggregateFunctionSpec, len(specs))
	for i, s := range specs {
		if s.Name == "" {
			s.Name = name
		} else if s.Name != name {
			return fmt.Errorf("%w: aggregate function overload %d name %q does not match set name %q", ErrInvalidInput, i, s.Name, name)
		}
		if err := validateAggregateFunctionSpec(s, true); err != nil {
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
	set := api.CreateAggregateFunctionSet(&cname[0])
	runtime.KeepAlive(cname)
	if set == 0 {
		return nativeError("create_aggregate_function_set", abi.ErrorOutOfMemory, "")
	}

	extraIDs := make([]uintptr, 0, len(normalized))
	releaseAll := func() {
		for _, id := range extraIDs {
			abi.ReleaseHookHandle(id)
		}
	}

	for i, spec := range normalized {
		af, extraID, err := buildNativeAggregateFunctionLocked(api, c.limits, spec)
		if err != nil {
			api.DestroyAggregateFunctionSet(&set)
			releaseAll()
			return err
		}
		extraIDs = append(extraIDs, extraID)
		addState := api.AddAggregateFunctionToSet(set, af)
		api.DestroyAggregateFunction(&af)
		if addState != abi.StateSuccess {
			api.DestroyAggregateFunctionSet(&set)
			releaseAll()
			return nativeError("add_aggregate_function_to_set", abi.ErrorInvalid, fmt.Sprintf("failed to add overload %d to aggregate function set %q", i, name))
		}
	}

	state := api.RegisterAggregateFunctionSet(c.handle, set)
	api.DestroyAggregateFunctionSet(&set)
	if state != abi.StateSuccess {
		releaseAll()
		return nativeError("register_aggregate_function_set", abi.ErrorInvalid, fmt.Sprintf("failed to register aggregate function set %q", name))
	}
	return nil
}
