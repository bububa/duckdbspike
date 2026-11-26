package duckdb

import (
	"fmt"
	"math"
	"runtime"
	"sync/atomic"

	"github.com/bububa/duckdbspike/internal/abi"
)

// CopyFunctionBindInput supplies the source query column types and COPY options
// during a COPY ... TO Bind callback. Option keys are uppercased by DuckDB.
type CopyFunctionBindInput struct {
	ColumnTypes []LogicalType
	Options     map[string]Value
}

// CopyFunctionGlobalInitInput supplies BindData and the target FilePath during
// a COPY ... TO GlobalInit callback.
type CopyFunctionGlobalInitInput struct {
	BindData any
	FilePath string
}

// CopyFunctionChunk provides read-only access to one input DataChunk during a
// COPY ... TO Sink callback. It is valid only for the duration of that Sink
// call and must not be retained after Sink returns.
type CopyFunctionChunk struct {
	api    *abi.API
	rows   int
	vecs   []uintptr
	plans  []columnPlan
	limits DecodeLimits
}

// Len returns the number of rows in this input chunk.
func (c *CopyFunctionChunk) Len() int {
	if c == nil {
		return 0
	}
	return c.rows
}

// ColumnCount returns the number of columns in this input chunk.
func (c *CopyFunctionChunk) ColumnCount() int {
	if c == nil {
		return 0
	}
	return len(c.vecs)
}

// Value decodes the value at column col and row index row.
func (c *CopyFunctionChunk) Value(col, row int) (Value, error) {
	if c == nil || len(c.vecs) == 0 {
		return Value{}, fmt.Errorf("%w: invalid copy function chunk", ErrInvalidInput)
	}
	if col < 0 || col >= len(c.vecs) {
		return Value{}, fmt.Errorf("%w: column %d out of range [0,%d)", ErrOutOfRange, col, len(c.vecs))
	}
	if row < 0 || row >= c.rows {
		return Value{}, fmt.Errorf("%w: row %d out of range [0,%d)", ErrOutOfRange, row, c.rows)
	}
	return decodeVectorValue(c.api, &c.plans[col], c.vecs[col], uint64(row), c.limits)
}

// Row decodes all columns for row into dst (reusing dst if it has sufficient
// capacity).
func (c *CopyFunctionChunk) Row(row int, dst []Value) ([]Value, error) {
	if c == nil {
		return nil, fmt.Errorf("%w: invalid copy function chunk", ErrInvalidInput)
	}
	if row < 0 || row >= c.rows {
		return nil, fmt.Errorf("%w: row %d out of range [0,%d)", ErrOutOfRange, row, c.rows)
	}
	n := len(c.vecs)
	if cap(dst) < n {
		dst = make([]Value, n)
	} else {
		dst = dst[:n]
	}
	for col := range n {
		v, err := decodeVectorValue(c.api, &c.plans[col], c.vecs[col], uint64(row), c.limits)
		if err != nil {
			return nil, fmt.Errorf("col[%d]: %w", col, err)
		}
		dst[col] = v
	}
	return dst, nil
}

// CopyFunctionSpec defines a pure-Go custom DuckDB COPY format registered via
// Conn.RegisterCopyFunction or Tx.RegisterCopyFunction.
//
// At least one of Sink (for COPY ... TO) or CopyFrom (for COPY ... FROM) must
// be provided. When CopyFrom is set, its Parameters default to a single VARCHAR
// file-path parameter if omitted, and its Bind callback receives the target
// table schema in TableFunctionBindInput.TargetColumns.
type CopyFunctionSpec struct {
	Name string

	Bind       func(input CopyFunctionBindInput) (bindData any, err error)
	GlobalInit func(input CopyFunctionGlobalInitInput) (globalState any, err error)
	Sink       func(bindData, globalState any, chunk *CopyFunctionChunk) error
	Finalize   func(bindData, globalState any) error

	CopyFrom *TableFunctionSpec
}

type copyFuncDef struct {
	api    *abi.API
	limits DecodeLimits
	spec   CopyFunctionSpec
}

type copyFuncBindState struct {
	plans    []columnPlan
	userData any
}

type copyFuncGlobalState struct {
	userData any
}

var (
	copyFuncDispatchAPI atomic.Pointer[abi.API]
	globalCopyDispatch  = copyFuncDispatcher{}
)

type copyFuncDispatcher struct{}

func setCopyBindErr(api *abi.API, info uintptr, msg string) {
	cmsg := cErrorString(msg)
	api.CopyFunctionBindSetError(info, &cmsg[0])
	runtime.KeepAlive(cmsg)
}

func setCopyInitErr(api *abi.API, info uintptr, msg string) {
	cmsg := cErrorString(msg)
	api.CopyFunctionGlobalInitSetError(info, &cmsg[0])
	runtime.KeepAlive(cmsg)
}

func setCopySinkErr(api *abi.API, info uintptr, msg string) {
	cmsg := cErrorString(msg)
	api.CopyFunctionSinkSetError(info, &cmsg[0])
	runtime.KeepAlive(cmsg)
}

func setCopyFinalizeErr(api *abi.API, info uintptr, msg string) {
	cmsg := cErrorString(msg)
	api.CopyFunctionFinalizeSetError(info, &cmsg[0])
	runtime.KeepAlive(cmsg)
}

func (copyFuncDispatcher) Bind(info uintptr) {
	bootAPI := copyFuncDispatchAPI.Load()
	if bootAPI == nil || bootAPI.CopyFunctionBindGetExtraInfo == nil {
		return
	}
	extraID := bootAPI.CopyFunctionBindGetExtraInfo(info)
	raw, ok := abi.LookupHookHandle(extraID)
	if !ok {
		if bootAPI.CopyFunctionBindSetError != nil {
			setCopyBindErr(bootAPI, info, "copy function definition handle not found")
		}
		return
	}
	def, ok := raw.(*copyFuncDef)
	if !ok || def == nil {
		if bootAPI.CopyFunctionBindSetError != nil {
			setCopyBindErr(bootAPI, info, "invalid copy function definition handle")
		}
		return
	}
	api := def.api

	defer func() {
		if r := recover(); r != nil {
			setCopyBindErr(api, info, fmt.Sprintf("copy function %q bind panic: %v", def.spec.Name, r))
		}
	}()

	colCount := api.CopyFunctionBindGetColumnCount(info)
	if colCount > def.limits.MaxCollectionLen || colCount > math.MaxInt {
		setCopyBindErr(api, info, fmt.Sprintf("copy function %q column count %d exceeds limit", def.spec.Name, colCount))
		return
	}
	plans := make([]columnPlan, int(colCount))
	colTypes := make([]LogicalType, int(colCount))
	for i := range colCount {
		ltHandle := api.CopyFunctionBindGetColumnType(info, i)
		plan, err := buildColumnPlan(api, ltHandle, 0, def.limits)
		if ltHandle != 0 {
			api.DestroyLogicalType(&ltHandle)
		}
		if err != nil {
			setCopyBindErr(api, info, fmt.Sprintf("copy function %q column %d: %v", def.spec.Name, i, err))
			return
		}
		plans[i] = plan
		colTypes[i] = *cloneLogicalType(plan.lt)
	}

	options := make(map[string]Value)
	if optVal := api.CopyFunctionBindGetOptions(info); optVal != 0 {
		v, err := decodeNativeValue(api, optVal, 0, def.limits)
		api.DestroyValue(&optVal)
		if err != nil {
			setCopyBindErr(api, info, fmt.Sprintf("copy function %q options: %v", def.spec.Name, err))
			return
		}
		if !v.IsNull() && v.Kind() == TypeStruct {
			st, err := v.Struct()
			if err != nil {
				setCopyBindErr(api, info, fmt.Sprintf("copy function %q options struct: %v", def.spec.Name, err))
				return
			}
			for _, f := range st.Fields {
				options[f.Name] = f.Value
			}
		}
	}

	var userData any
	if def.spec.Bind != nil {
		var err error
		userData, err = def.spec.Bind(CopyFunctionBindInput{
			ColumnTypes: colTypes,
			Options:     options,
		})
		if err != nil {
			setCopyBindErr(api, info, err.Error())
			return
		}
	}

	bindID := abi.RegisterHookHandle(&copyFuncBindState{
		plans:    plans,
		userData: userData,
	})
	api.CopyFunctionBindSetBindData(info, bindID, abi.HookDeleteCallback())
}

func (copyFuncDispatcher) GlobalInit(info uintptr) {
	bootAPI := copyFuncDispatchAPI.Load()
	if bootAPI == nil || bootAPI.CopyFunctionGlobalInitGetExtraInfo == nil {
		return
	}
	extraID := bootAPI.CopyFunctionGlobalInitGetExtraInfo(info)
	raw, ok := abi.LookupHookHandle(extraID)
	if !ok {
		if bootAPI.CopyFunctionGlobalInitSetError != nil {
			setCopyInitErr(bootAPI, info, "copy function definition handle not found")
		}
		return
	}
	def, ok := raw.(*copyFuncDef)
	if !ok || def == nil {
		if bootAPI.CopyFunctionGlobalInitSetError != nil {
			setCopyInitErr(bootAPI, info, "invalid copy function definition handle")
		}
		return
	}
	api := def.api

	defer func() {
		if r := recover(); r != nil {
			setCopyInitErr(api, info, fmt.Sprintf("copy function %q global init panic: %v", def.spec.Name, r))
		}
	}()

	var bindData any
	if bindID := api.CopyFunctionGlobalInitGetBindData(info); bindID != 0 {
		if rawBind, ok := abi.LookupHookHandle(bindID); ok {
			if bs, ok := rawBind.(*copyFuncBindState); ok && bs != nil {
				bindData = bs.userData
			}
		}
	}
	filePath := abi.GoString(api.CopyFunctionGlobalInitGetFilePath(info))

	var userState any
	if def.spec.GlobalInit != nil {
		var err error
		userState, err = def.spec.GlobalInit(CopyFunctionGlobalInitInput{
			BindData: bindData,
			FilePath: filePath,
		})
		if err != nil {
			setCopyInitErr(api, info, err.Error())
			return
		}
	}

	stateID := abi.RegisterHookHandle(&copyFuncGlobalState{userData: userState})
	api.CopyFunctionGlobalInitSetGlobalState(info, stateID, abi.HookDeleteCallback())
}

func (copyFuncDispatcher) Sink(info, input uintptr) {
	bootAPI := copyFuncDispatchAPI.Load()
	if bootAPI == nil || bootAPI.CopyFunctionSinkGetExtraInfo == nil {
		return
	}
	extraID := bootAPI.CopyFunctionSinkGetExtraInfo(info)
	raw, ok := abi.LookupHookHandle(extraID)
	if !ok {
		if bootAPI.CopyFunctionSinkSetError != nil {
			setCopySinkErr(bootAPI, info, "copy function definition handle not found")
		}
		return
	}
	def, ok := raw.(*copyFuncDef)
	if !ok || def == nil {
		if bootAPI.CopyFunctionSinkSetError != nil {
			setCopySinkErr(bootAPI, info, "invalid copy function definition handle")
		}
		return
	}
	api := def.api

	defer func() {
		if r := recover(); r != nil {
			setCopySinkErr(api, info, fmt.Sprintf("copy function %q sink panic: %v", def.spec.Name, r))
		}
	}()

	bindID := api.CopyFunctionSinkGetBindData(info)
	rawBind, ok := abi.LookupHookHandle(bindID)
	if !ok {
		setCopySinkErr(api, info, fmt.Sprintf("copy function %q bind state handle not found", def.spec.Name))
		return
	}
	bindState, ok := rawBind.(*copyFuncBindState)
	if !ok || bindState == nil {
		setCopySinkErr(api, info, fmt.Sprintf("copy function %q invalid bind state handle", def.spec.Name))
		return
	}

	var globalState any
	if stateID := api.CopyFunctionSinkGetGlobalState(info); stateID != 0 {
		if rawState, ok := abi.LookupHookHandle(stateID); ok {
			if gs, ok := rawState.(*copyFuncGlobalState); ok && gs != nil {
				globalState = gs.userData
			}
		}
	}

	rowCount := api.DataChunkGetSize(input)
	if rowCount == 0 {
		return
	}
	if rowCount > math.MaxInt {
		setCopySinkErr(api, info, fmt.Sprintf("copy function %q chunk size %d overflows int", def.spec.Name, rowCount))
		return
	}

	colCount := api.DataChunkGetColumnCount(input)
	if int(colCount) != len(bindState.plans) {
		setCopySinkErr(api, info, fmt.Sprintf("copy function %q chunk column count %d != bound %d", def.spec.Name, colCount, len(bindState.plans)))
		return
	}
	vecs := make([]uintptr, int(colCount))
	for i := range colCount {
		vecs[i] = api.DataChunkGetVector(input, i)
	}

	chunk := CopyFunctionChunk{
		api:    api,
		rows:   int(rowCount),
		vecs:   vecs,
		plans:  bindState.plans,
		limits: def.limits,
	}
	if err := def.spec.Sink(bindState.userData, globalState, &chunk); err != nil {
		setCopySinkErr(api, info, err.Error())
	}
}

func (copyFuncDispatcher) Finalize(info uintptr) {
	bootAPI := copyFuncDispatchAPI.Load()
	if bootAPI == nil || bootAPI.CopyFunctionFinalizeGetExtraInfo == nil {
		return
	}
	extraID := bootAPI.CopyFunctionFinalizeGetExtraInfo(info)
	raw, ok := abi.LookupHookHandle(extraID)
	if !ok {
		if bootAPI.CopyFunctionFinalizeSetError != nil {
			setCopyFinalizeErr(bootAPI, info, "copy function definition handle not found")
		}
		return
	}
	def, ok := raw.(*copyFuncDef)
	if !ok || def == nil {
		if bootAPI.CopyFunctionFinalizeSetError != nil {
			setCopyFinalizeErr(bootAPI, info, "invalid copy function definition handle")
		}
		return
	}
	api := def.api

	defer func() {
		if r := recover(); r != nil {
			setCopyFinalizeErr(api, info, fmt.Sprintf("copy function %q finalize panic: %v", def.spec.Name, r))
		}
	}()

	if def.spec.Finalize == nil {
		return
	}

	var bindData any
	if bindID := api.CopyFunctionFinalizeGetBindData(info); bindID != 0 {
		if rawBind, ok := abi.LookupHookHandle(bindID); ok {
			if bs, ok := rawBind.(*copyFuncBindState); ok && bs != nil {
				bindData = bs.userData
			}
		}
	}
	var globalState any
	if stateID := api.CopyFunctionFinalizeGetGlobalState(info); stateID != 0 {
		if rawState, ok := abi.LookupHookHandle(stateID); ok {
			if gs, ok := rawState.(*copyFuncGlobalState); ok && gs != nil {
				globalState = gs.userData
			}
		}
	}

	if err := def.spec.Finalize(bindData, globalState); err != nil {
		setCopyFinalizeErr(api, info, err.Error())
	}
}

// RegisterCopyFunction registers a custom COPY TO / COPY FROM format on c.
func (c *Conn) RegisterCopyFunction(spec CopyFunctionSpec) error {
	return c.registerCopyFunctionWithTx(nil, spec)
}

// RegisterCopyFunction registers a custom COPY TO / COPY FROM format within tx.
func (tx *Tx) RegisterCopyFunction(spec CopyFunctionSpec) error {
	if tx == nil || tx.conn == nil {
		return ErrTxDone
	}
	return tx.conn.registerCopyFunctionWithTx(tx, spec)
}

func (c *Conn) registerCopyFunctionWithTx(tx *Tx, spec CopyFunctionSpec) error {
	if c == nil {
		return ErrClosed
	}
	if spec.Name == "" {
		return fmt.Errorf("%w: copy function name must not be empty", ErrInvalidInput)
	}
	if spec.Sink == nil && spec.CopyFrom == nil {
		return fmt.Errorf("%w: copy function %q requires Sink (COPY TO) and/or CopyFrom (COPY FROM)", ErrInvalidInput, spec.Name)
	}
	cname, err := abi.CString(spec.Name)
	if err != nil {
		return fmt.Errorf("%w: copy function name %q: %v", ErrInvalidInput, spec.Name, err)
	}

	var copyFromSpec TableFunctionSpec
	hasCopyFrom := spec.CopyFrom != nil
	if hasCopyFrom {
		copyFromSpec = *spec.CopyFrom
		if copyFromSpec.Name == "" {
			copyFromSpec.Name = spec.Name
		}
		if len(copyFromSpec.Parameters) == 0 {
			copyFromSpec.Parameters = []LogicalType{ScalarType(TypeVarchar)}
		}
		if len(copyFromSpec.Parameters) != 1 || copyFromSpec.Parameters[0].Kind != TypeVarchar {
			return fmt.Errorf("%w: copy function %q CopyFrom must take a single VARCHAR file-path parameter", ErrInvalidInput, spec.Name)
		}
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
	cf := api.CreateCopyFunction()
	if cf == 0 {
		return nativeError("create_copy_function", abi.ErrorOutOfMemory, "")
	}

	api.CopyFunctionSetName(cf, &cname[0])
	runtime.KeepAlive(cname)

	var extraID uintptr
	var copyFromExtraID uintptr

	if spec.Sink != nil {
		def := &copyFuncDef{
			api:    api,
			limits: c.limits,
			spec:   spec,
		}
		copyFuncDispatchAPI.Store(api)
		abi.SetCopyFunctionDispatcher(globalCopyDispatch)

		extraID = abi.RegisterHookHandle(def)
		api.CopyFunctionSetExtraInfo(cf, extraID, abi.HookDeleteCallback())
		api.CopyFunctionSetBind(cf, abi.CopyFunctionBindCallback())
		api.CopyFunctionSetGlobalInit(cf, abi.CopyFunctionGlobalInitCallback())
		api.CopyFunctionSetSink(cf, abi.CopyFunctionSinkCallback())
		api.CopyFunctionSetFinalize(cf, abi.CopyFunctionFinalizeCallback())
	}

	if hasCopyFrom {
		tf, tfExtraID, err := buildNativeTableFunctionLocked(api, c.limits, copyFromSpec)
		if err != nil {
			api.DestroyCopyFunction(&cf)
			abi.ReleaseHookHandle(extraID)
			return err
		}
		copyFromExtraID = tfExtraID
		api.CopyFunctionSetCopyFromFunction(cf, tf)
		api.DestroyTableFunction(&tf)
	}

	state := api.RegisterCopyFunction(c.handle, cf)
	api.DestroyCopyFunction(&cf)
	if state != abi.StateSuccess {
		abi.ReleaseHookHandle(extraID)
		abi.ReleaseHookHandle(copyFromExtraID)
		return nativeError("register_copy_function", abi.ErrorInvalid, fmt.Sprintf("failed to register copy function %q", spec.Name))
	}
	return nil
}
