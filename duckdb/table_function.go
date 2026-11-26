package duckdb

import (
	"fmt"
	"math"
	"runtime"
	"slices"
	"sync/atomic"

	"github.com/bububa/duckdbspike/internal/abi"
)

// TableFunctionBindInput supplies the decoded positional and named arguments
// passed to a TableFunction's Bind callback. When the table function is used as
// a COPY ... FROM handler, TargetColumns contains the destination table schema
// pre-populated by DuckDB (and Bind should not return new Columns).
type TableFunctionBindInput struct {
	Args          []Value
	NamedArgs     map[string]Value
	TargetColumns []Column
}

// TableFunctionBindResult declares the output schema, optional cardinality
// hint, and arbitrary Go BindData returned from a TableFunction's Bind callback.
type TableFunctionBindResult struct {
	Columns     []Column
	Cardinality uint64
	ExactCard   bool
	BindData    any
}

// TableFunctionInitInput supplies BindData and (when ProjectionPushdown is
// enabled) the projected column indices requested by DuckDB. Virtual columns
// such as DuckDB's ROW_ID for COUNT(*) appear as math.MaxUint64.
type TableFunctionInitInput struct {
	BindData         any
	ProjectedColumns []uint64
}

// TableFunctionInitResult returns global scan state and an optional MaxThreads
// parallelism hint (values <= 0 leave DuckDB's default single-threaded setting).
type TableFunctionInitResult struct {
	InitData   any
	MaxThreads uint64
}

// TableFunctionLocalInitResult returns per-worker local scan state for parallel
// table function execution.
type TableFunctionLocalInitResult struct {
	LocalState any
}

// TableFunctionSpec defines a pure-Go DuckDB table-valued function registered
// via Conn.RegisterTableFunction or Tx.RegisterTableFunction.
type TableFunctionSpec struct {
	Name               string
	Parameters         []LogicalType
	NamedParameters    map[string]LogicalType
	ProjectionPushdown bool
	Bind               func(input TableFunctionBindInput) (TableFunctionBindResult, error)
	Init               func(input TableFunctionInitInput) (TableFunctionInitResult, error)
	LocalInit          func(input TableFunctionInitInput) (TableFunctionLocalInitResult, error)
	Execute            func(bindData, initData, localData any, chunk *TableFunctionChunk) error
}

type projectedSlot struct {
	plan    *columnPlan
	virtual bool
}

// TableFunctionChunk wraps one DuckDB output DataChunk during a single Execute
// call. It is valid only for the duration of that Execute call and must not be
// retained after Execute returns.
type TableFunctionChunk struct {
	api             *abi.API
	handle          uintptr
	cap             int
	rows            int
	cols            []Column
	slots           []projectedSlot
	hasPhysicalCols bool
	limits          DecodeLimits
}

// Cap returns the maximum number of rows this output chunk can hold (DuckDB's
// native vector size, typically 2048).
func (c *TableFunctionChunk) Cap() int {
	if c == nil {
		return 0
	}
	return c.cap
}

// Len returns the number of rows currently populated in this output chunk.
func (c *TableFunctionChunk) Len() int {
	if c == nil {
		return 0
	}
	return c.rows
}

// Columns returns a copy of the active output columns in chunk vector order.
// Virtual columns projected by DuckDB (such as ROW_ID for COUNT(*)) have
// Kind == TypeInvalid.
func (c *TableFunctionChunk) Columns() []Column {
	if c == nil || len(c.cols) == 0 {
		return nil
	}
	out := make([]Column, len(c.cols))
	for i, col := range c.cols {
		out[i] = Column{Name: col.Name, Type: *cloneLogicalType(col.Type)}
	}
	return out
}

// AppendRow writes one row of values into the output chunk in projected column
// order, rolling back any partial list/map vector mutations on error.
func (c *TableFunctionChunk) AppendRow(vals ...Value) error {
	if c == nil || c.handle == 0 {
		return fmt.Errorf("%w: invalid table function chunk", ErrInvalidInput)
	}
	if c.rows >= c.cap {
		return fmt.Errorf("%w: table function chunk capacity %d reached", ErrOutOfRange, c.cap)
	}
	if len(vals) != len(c.slots) {
		return fmt.Errorf("%w: table function row has %d values, expected %d", ErrInvalidInput, len(vals), len(c.slots))
	}

	row := uint64(c.rows)
	var undo []listSizeUndo
	rollback := func() {
		for _, u := range slices.Backward(undo) {
			_ = c.api.ListVectorSetSize(u.vec, u.prevSize)
		}
	}

	for colIdx, slot := range c.slots {
		if slot.virtual {
			continue
		}
		vec := c.api.DataChunkGetVector(c.handle, uint64(colIdx))
		if err := writeVectorValue(c.api, slot.plan, vec, row, vals[colIdx], 0, c.limits, &undo); err != nil {
			rollback()
			return fmt.Errorf("table function col[%d]: %w", colIdx, err)
		}
	}
	c.rows++
	return nil
}

// SetSize directly sets the output chunk's row count. When the query projects
// only virtual columns (such as `SELECT count(*) FROM my_tf()` with
// ProjectionPushdown enabled), SetSize allows emitting n rows without writing
// physical vectors. When physical columns are projected, SetSize can only
// shrink the row count (n <= Len()).
func (c *TableFunctionChunk) SetSize(n int) error {
	if c == nil || c.handle == 0 {
		return fmt.Errorf("%w: invalid table function chunk", ErrInvalidInput)
	}
	if n < 0 || n > c.cap {
		return fmt.Errorf("%w: table function chunk size %d out of range [0, %d]", ErrOutOfRange, n, c.cap)
	}
	if c.hasPhysicalCols && n > c.rows {
		return fmt.Errorf("%w: SetSize(%d) cannot grow chunk with physical columns beyond appended rows (%d)", ErrInvalidInput, n, c.rows)
	}
	c.rows = n
	return nil
}

type tableFuncDef struct {
	api             *abi.API
	limits          DecodeLimits
	vectorSize      uint64
	spec            TableFunctionSpec
	namedParamNames []string
	namedParamCStrs [][]byte
}

type tableFuncBindState struct {
	columns  []Column
	plans    []columnPlan
	userData any
}

type tableFuncInitState struct {
	projectedCols   []Column
	projectedSlots  []projectedSlot
	hasPhysicalCols bool
	userData        any
}

type tableFuncLocalState struct {
	userData any
}

var (
	tableFuncDispatchAPI atomic.Pointer[abi.API]
	globalTFDispatcher   = tableFuncDispatcher{}
)

type tableFuncDispatcher struct{}

func setBindErr(api *abi.API, info uintptr, msg string) {
	cmsg := cErrorString(msg)
	api.BindSetError(info, &cmsg[0])
	runtime.KeepAlive(cmsg)
}

func setInitErr(api *abi.API, info uintptr, msg string) {
	cmsg := cErrorString(msg)
	api.InitSetError(info, &cmsg[0])
	runtime.KeepAlive(cmsg)
}

func setFuncErr(api *abi.API, info uintptr, msg string) {
	cmsg := cErrorString(msg)
	api.FunctionSetError(info, &cmsg[0])
	runtime.KeepAlive(cmsg)
}

func (tableFuncDispatcher) Bind(info uintptr) {
	bootAPI := tableFuncDispatchAPI.Load()
	if bootAPI == nil || bootAPI.BindGetExtraInfo == nil {
		return
	}
	extraID := bootAPI.BindGetExtraInfo(info)
	raw, ok := abi.LookupHookHandle(extraID)
	if !ok {
		if bootAPI.BindSetError != nil {
			setBindErr(bootAPI, info, "table function definition handle not found")
		}
		return
	}
	def, ok := raw.(*tableFuncDef)
	if !ok || def == nil {
		if bootAPI.BindSetError != nil {
			setBindErr(bootAPI, info, "invalid table function definition handle")
		}
		return
	}
	api := def.api

	defer func() {
		if r := recover(); r != nil {
			setBindErr(api, info, fmt.Sprintf("table function %q bind panic: %v", def.spec.Name, r))
		}
	}()

	paramCount := api.BindGetParameterCount(info)
	if paramCount > def.limits.MaxCollectionLen || paramCount > math.MaxInt {
		setBindErr(api, info, fmt.Sprintf("table function %q parameter count %d exceeds limit", def.spec.Name, paramCount))
		return
	}
	args := make([]Value, int(paramCount))
	for i := range paramCount {
		vh := api.BindGetParameter(info, i)
		v, err := decodeNativeValue(api, vh, 0, def.limits)
		if vh != 0 {
			api.DestroyValue(&vh)
		}
		if err != nil {
			setBindErr(api, info, fmt.Sprintf("table function %q parameter %d: %v", def.spec.Name, i, err))
			return
		}
		args[i] = v
	}

	var namedArgs map[string]Value
	if len(def.namedParamNames) > 0 {
		namedArgs = make(map[string]Value, len(def.namedParamNames))
		for i, name := range def.namedParamNames {
			vh := api.BindGetNamedParameter(info, &def.namedParamCStrs[i][0])
			if vh == 0 {
				continue
			}
			v, err := decodeNativeValue(api, vh, 0, def.limits)
			api.DestroyValue(&vh)
			if err != nil {
				setBindErr(api, info, fmt.Sprintf("table function %q named parameter %q: %v", def.spec.Name, name, err))
				return
			}
			namedArgs[name] = v
		}
	}

	// When invoked as a COPY ... FROM handler, DuckDB pre-populates the target
	// table's columns on bind_info and forbids calling BindAddResultColumn.
	var targetCols []Column
	var targetPlans []columnPlan
	if api.TableFunctionBindGetResultColumnCount != nil {
		targetCount := api.TableFunctionBindGetResultColumnCount(info)
		if targetCount > def.limits.MaxCollectionLen || targetCount > math.MaxInt {
			setBindErr(api, info, fmt.Sprintf("table function %q target column count %d exceeds limit", def.spec.Name, targetCount))
			return
		}
		if targetCount > 0 {
			targetCols = make([]Column, int(targetCount))
			targetPlans = make([]columnPlan, int(targetCount))
			for i := range targetCount {
				colName := abi.GoString(api.TableFunctionBindGetResultColumnName(info, i))
				ltHandle := api.TableFunctionBindGetResultColumnType(info, i)
				plan, err := buildColumnPlan(api, ltHandle, 0, def.limits)
				if ltHandle != 0 {
					api.DestroyLogicalType(&ltHandle)
				}
				if err != nil {
					setBindErr(api, info, fmt.Sprintf("table function %q target column %d (%q): %v", def.spec.Name, i, colName, err))
					return
				}
				targetCols[i] = Column{Name: colName, Type: plan.lt}
				targetPlans[i] = plan
			}
		}
	}

	var targetColsInput []Column
	if len(targetCols) > 0 {
		targetColsInput = make([]Column, len(targetCols))
		for i, col := range targetCols {
			targetColsInput[i] = Column{Name: col.Name, Type: *cloneLogicalType(col.Type)}
		}
	}

	res, err := def.spec.Bind(TableFunctionBindInput{
		Args:          args,
		NamedArgs:     namedArgs,
		TargetColumns: targetColsInput,
	})
	if err != nil {
		setBindErr(api, info, err.Error())
		return
	}

	var cols []Column
	var plans []columnPlan
	if len(targetCols) > 0 {
		// COPY ... FROM mode: use target table columns without calling BindAddResultColumn.
		cols = targetCols
		plans = targetPlans
	} else {
		if len(res.Columns) == 0 {
			setBindErr(api, info, fmt.Sprintf("table function %q bind returned no result columns", def.spec.Name))
			return
		}

		ltHandles := make([]uintptr, len(res.Columns))
		defer func() {
			for i := range ltHandles {
				if ltHandles[i] != 0 {
					api.DestroyLogicalType(&ltHandles[i])
				}
			}
		}()
		cnames := make([][]byte, len(res.Columns))
		cols = make([]Column, len(res.Columns))
		plans = make([]columnPlan, len(res.Columns))

		for i, col := range res.Columns {
			if col.Name == "" {
				setBindErr(api, info, fmt.Sprintf("table function %q result column %d has empty name", def.spec.Name, i))
				return
			}
			if col.Type.Kind == TypeAny {
				setBindErr(api, info, fmt.Sprintf("table function %q result column %q cannot have ANY type", def.spec.Name, col.Name))
				return
			}
			cname, err := abi.CString(col.Name)
			if err != nil {
				setBindErr(api, info, fmt.Sprintf("table function %q result column %q: %v", def.spec.Name, col.Name, err))
				return
			}
			ltHandle, err := createNativeLogicalType(api, col.Type, 0, def.limits)
			if err != nil {
				setBindErr(api, info, fmt.Sprintf("table function %q result column %q: %v", def.spec.Name, col.Name, err))
				return
			}
			ltHandles[i] = ltHandle
			plan, err := buildColumnPlan(api, ltHandle, 0, def.limits)
			if err != nil {
				setBindErr(api, info, fmt.Sprintf("table function %q result column %q: %v", def.spec.Name, col.Name, err))
				return
			}
			cnames[i] = cname
			plans[i] = plan
			cols[i] = Column{Name: col.Name, Type: plan.lt}
		}

		for i := range res.Columns {
			api.BindAddResultColumn(info, &cnames[i][0], ltHandles[i])
			runtime.KeepAlive(cnames[i])
		}
	}

	if res.Cardinality > 0 || res.ExactCard {
		api.BindSetCardinality(info, res.Cardinality, res.ExactCard)
	}

	bindState := &tableFuncBindState{
		columns:  cols,
		plans:    plans,
		userData: res.BindData,
	}
	bindID := abi.RegisterHookHandle(bindState)
	api.BindSetBindData(info, bindID, abi.HookDeleteCallback())
}

func readProjectedColumnIndices(api *abi.API, info uintptr) []uint64 {
	count := api.InitGetColumnCount(info)
	if count == 0 {
		return nil
	}
	out := make([]uint64, int(count))
	for i := range count {
		out[i] = api.InitGetColumnIndex(info, i)
	}
	return out
}

func (tableFuncDispatcher) Init(info uintptr) {
	bootAPI := tableFuncDispatchAPI.Load()
	if bootAPI == nil || bootAPI.InitGetExtraInfo == nil {
		return
	}
	extraID := bootAPI.InitGetExtraInfo(info)
	rawDef, ok := abi.LookupHookHandle(extraID)
	if !ok {
		if bootAPI.InitSetError != nil {
			setInitErr(bootAPI, info, "table function definition handle not found")
		}
		return
	}
	def, ok := rawDef.(*tableFuncDef)
	if !ok || def == nil {
		if bootAPI.InitSetError != nil {
			setInitErr(bootAPI, info, "invalid table function definition handle")
		}
		return
	}
	api := def.api

	defer func() {
		if r := recover(); r != nil {
			setInitErr(api, info, fmt.Sprintf("table function %q init panic: %v", def.spec.Name, r))
		}
	}()

	bindID := api.InitGetBindData(info)
	rawBind, ok := abi.LookupHookHandle(bindID)
	if !ok {
		setInitErr(api, info, fmt.Sprintf("table function %q bind state handle not found", def.spec.Name))
		return
	}
	bindState, ok := rawBind.(*tableFuncBindState)
	if !ok || bindState == nil {
		setInitErr(api, info, fmt.Sprintf("table function %q invalid bind state handle", def.spec.Name))
		return
	}

	var (
		projectedIndices []uint64
		projectedCols    []Column
		projectedSlots   []projectedSlot
		hasPhysical      bool
	)
	if def.spec.ProjectionPushdown {
		projectedIndices = readProjectedColumnIndices(api, info)
		projectedCols = make([]Column, len(projectedIndices))
		projectedSlots = make([]projectedSlot, len(projectedIndices))
		for i, colIdx := range projectedIndices {
			if colIdx < uint64(len(bindState.columns)) {
				projectedCols[i] = bindState.columns[colIdx]
				projectedSlots[i] = projectedSlot{plan: &bindState.plans[colIdx]}
				hasPhysical = true
			} else {
				projectedCols[i] = Column{Type: LogicalType{Kind: TypeInvalid}}
				projectedSlots[i] = projectedSlot{virtual: true}
			}
		}
	} else {
		projectedCols = bindState.columns
		projectedSlots = make([]projectedSlot, len(bindState.plans))
		for i := range bindState.plans {
			projectedSlots[i] = projectedSlot{plan: &bindState.plans[i]}
		}
		hasPhysical = len(bindState.plans) > 0
	}

	initState := &tableFuncInitState{
		projectedCols:   projectedCols,
		projectedSlots:  projectedSlots,
		hasPhysicalCols: hasPhysical,
	}

	if def.spec.Init != nil {
		var projCopy []uint64
		if len(projectedIndices) > 0 {
			projCopy = append([]uint64(nil), projectedIndices...)
		}
		res, err := def.spec.Init(TableFunctionInitInput{
			BindData:         bindState.userData,
			ProjectedColumns: projCopy,
		})
		if err != nil {
			setInitErr(api, info, err.Error())
			return
		}
		initState.userData = res.InitData
		if res.MaxThreads > 0 {
			api.InitSetMaxThreads(info, res.MaxThreads)
		}
	}

	initID := abi.RegisterHookHandle(initState)
	api.InitSetInitData(info, initID, abi.HookDeleteCallback())
}

func (tableFuncDispatcher) LocalInit(info uintptr) {
	bootAPI := tableFuncDispatchAPI.Load()
	if bootAPI == nil || bootAPI.InitGetExtraInfo == nil {
		return
	}
	extraID := bootAPI.InitGetExtraInfo(info)
	rawDef, ok := abi.LookupHookHandle(extraID)
	if !ok {
		if bootAPI.InitSetError != nil {
			setInitErr(bootAPI, info, "table function definition handle not found")
		}
		return
	}
	def, ok := rawDef.(*tableFuncDef)
	if !ok || def == nil {
		if bootAPI.InitSetError != nil {
			setInitErr(bootAPI, info, "invalid table function definition handle")
		}
		return
	}
	api := def.api

	defer func() {
		if r := recover(); r != nil {
			setInitErr(api, info, fmt.Sprintf("table function %q local init panic: %v", def.spec.Name, r))
		}
	}()

	bindID := api.InitGetBindData(info)
	rawBind, ok := abi.LookupHookHandle(bindID)
	if !ok {
		setInitErr(api, info, fmt.Sprintf("table function %q bind state handle not found", def.spec.Name))
		return
	}
	bindState, ok := rawBind.(*tableFuncBindState)
	if !ok || bindState == nil {
		setInitErr(api, info, fmt.Sprintf("table function %q invalid bind state handle", def.spec.Name))
		return
	}

	var projectedIndices []uint64
	if def.spec.ProjectionPushdown {
		projectedIndices = readProjectedColumnIndices(api, info)
	}

	res, err := def.spec.LocalInit(TableFunctionInitInput{
		BindData:         bindState.userData,
		ProjectedColumns: projectedIndices,
	})
	if err != nil {
		setInitErr(api, info, err.Error())
		return
	}
	localID := abi.RegisterHookHandle(&tableFuncLocalState{userData: res.LocalState})
	api.InitSetInitData(info, localID, abi.HookDeleteCallback())
}

func (tableFuncDispatcher) Execute(info, output uintptr) {
	bootAPI := tableFuncDispatchAPI.Load()
	if bootAPI == nil || bootAPI.FunctionGetExtraInfo == nil {
		return
	}
	extraID := bootAPI.FunctionGetExtraInfo(info)
	rawDef, ok := abi.LookupHookHandle(extraID)
	if !ok {
		if bootAPI.DataChunkSetSize != nil {
			bootAPI.DataChunkSetSize(output, 0)
		}
		if bootAPI.FunctionSetError != nil {
			setFuncErr(bootAPI, info, "table function definition handle not found")
		}
		return
	}
	def, ok := rawDef.(*tableFuncDef)
	if !ok || def == nil {
		if bootAPI.DataChunkSetSize != nil {
			bootAPI.DataChunkSetSize(output, 0)
		}
		if bootAPI.FunctionSetError != nil {
			setFuncErr(bootAPI, info, "invalid table function definition handle")
		}
		return
	}
	api := def.api

	defer func() {
		if r := recover(); r != nil {
			api.DataChunkSetSize(output, 0)
			setFuncErr(api, info, fmt.Sprintf("table function %q execute panic: %v", def.spec.Name, r))
		}
	}()

	bindID := api.FunctionGetBindData(info)
	rawBind, ok := abi.LookupHookHandle(bindID)
	if !ok {
		api.DataChunkSetSize(output, 0)
		setFuncErr(api, info, fmt.Sprintf("table function %q bind state handle not found", def.spec.Name))
		return
	}
	bindState, ok := rawBind.(*tableFuncBindState)
	if !ok || bindState == nil {
		api.DataChunkSetSize(output, 0)
		setFuncErr(api, info, fmt.Sprintf("table function %q invalid bind state handle", def.spec.Name))
		return
	}

	initID := api.FunctionGetInitData(info)
	rawInit, ok := abi.LookupHookHandle(initID)
	if !ok {
		api.DataChunkSetSize(output, 0)
		setFuncErr(api, info, fmt.Sprintf("table function %q init state handle not found", def.spec.Name))
		return
	}
	initState, ok := rawInit.(*tableFuncInitState)
	if !ok || initState == nil {
		api.DataChunkSetSize(output, 0)
		setFuncErr(api, info, fmt.Sprintf("table function %q invalid init state handle", def.spec.Name))
		return
	}

	var localData any
	if def.spec.LocalInit != nil {
		localID := api.FunctionGetLocalInitData(info)
		if rawLocal, ok := abi.LookupHookHandle(localID); ok {
			if ls, ok := rawLocal.(*tableFuncLocalState); ok && ls != nil {
				localData = ls.userData
			}
		}
	}

	chunk := TableFunctionChunk{
		api:             api,
		handle:          output,
		cap:             int(def.vectorSize),
		cols:            initState.projectedCols,
		slots:           initState.projectedSlots,
		hasPhysicalCols: initState.hasPhysicalCols,
		limits:          def.limits,
	}
	if err := def.spec.Execute(bindState.userData, initState.userData, localData, &chunk); err != nil {
		api.DataChunkSetSize(output, 0)
		setFuncErr(api, info, err.Error())
		return
	}
	api.DataChunkSetSize(output, uint64(chunk.rows))
}

// RegisterTableFunction registers a Go-backed SQL table-valued function on c.
func (c *Conn) RegisterTableFunction(spec TableFunctionSpec) error {
	return c.registerTableFunctionWithTx(nil, spec)
}

// RegisterTableFunction registers a Go-backed SQL table-valued function within tx.
func (tx *Tx) RegisterTableFunction(spec TableFunctionSpec) error {
	if tx == nil || tx.conn == nil {
		return ErrTxDone
	}
	return tx.conn.registerTableFunctionWithTx(tx, spec)
}

func buildNativeTableFunctionLocked(api *abi.API, limits DecodeLimits, spec TableFunctionSpec) (uintptr, uintptr, error) {
	if spec.Name == "" {
		return 0, 0, fmt.Errorf("%w: table function name must not be empty", ErrInvalidInput)
	}
	if spec.Bind == nil {
		return 0, 0, fmt.Errorf("%w: table function %q requires Bind callback", ErrInvalidInput, spec.Name)
	}
	if spec.Execute == nil {
		return 0, 0, fmt.Errorf("%w: table function %q requires Execute callback", ErrInvalidInput, spec.Name)
	}

	cname, err := abi.CString(spec.Name)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: table function name %q: %v", ErrInvalidInput, spec.Name, err)
	}

	namedNames := make([]string, 0, len(spec.NamedParameters))
	for k := range spec.NamedParameters {
		if k == "" {
			return 0, 0, fmt.Errorf("%w: table function %q has empty named parameter name", ErrInvalidInput, spec.Name)
		}
		namedNames = append(namedNames, k)
	}
	slices.Sort(namedNames)
	namedCStrs := make([][]byte, len(namedNames))
	for i, name := range namedNames {
		cb, err := abi.CString(name)
		if err != nil {
			return 0, 0, fmt.Errorf("%w: table function %q named parameter %q: %v", ErrInvalidInput, spec.Name, name, err)
		}
		namedCStrs[i] = cb
	}

	tf := api.CreateTableFunction()
	if tf == 0 {
		return 0, 0, nativeError("create_table_function", abi.ErrorOutOfMemory, "")
	}

	posTypes := make([]uintptr, len(spec.Parameters))
	namedTypes := make([]uintptr, len(namedNames))
	defer func() {
		for i := range posTypes {
			if posTypes[i] != 0 {
				api.DestroyLogicalType(&posTypes[i])
			}
		}
		for i := range namedTypes {
			if namedTypes[i] != 0 {
				api.DestroyLogicalType(&namedTypes[i])
			}
		}
	}()

	for i, pt := range spec.Parameters {
		lt, err := createNativeLogicalType(api, pt, 0, limits)
		if err != nil {
			api.DestroyTableFunction(&tf)
			return 0, 0, fmt.Errorf("table function %q parameter %d: %w", spec.Name, i, err)
		}
		posTypes[i] = lt
	}
	for i, name := range namedNames {
		lt, err := createNativeLogicalType(api, spec.NamedParameters[name], 0, limits)
		if err != nil {
			api.DestroyTableFunction(&tf)
			return 0, 0, fmt.Errorf("table function %q named parameter %q: %w", spec.Name, name, err)
		}
		namedTypes[i] = lt
	}

	var vecSize uint64 = 2048
	if api.VectorSize != nil {
		if sz := api.VectorSize(); sz > 0 && sz <= math.MaxInt {
			vecSize = sz
		}
	}

	def := &tableFuncDef{
		api:             api,
		limits:          limits,
		vectorSize:      vecSize,
		spec:            spec,
		namedParamNames: namedNames,
		namedParamCStrs: namedCStrs,
	}

	// Wire the process-wide singleton table-function dispatcher before
	// registering with DuckDB.
	tableFuncDispatchAPI.Store(api)
	abi.SetTableFunctionDispatcher(globalTFDispatcher)

	api.TableFunctionSetName(tf, &cname[0])
	runtime.KeepAlive(cname)

	for _, lt := range posTypes {
		api.TableFunctionAddParameter(tf, lt)
	}
	for i, lt := range namedTypes {
		api.TableFunctionAddNamedParameter(tf, &namedCStrs[i][0], lt)
		runtime.KeepAlive(namedCStrs[i])
	}

	extraID := abi.RegisterHookHandle(def)
	api.TableFunctionSetExtraInfo(tf, extraID, abi.HookDeleteCallback())
	api.TableFunctionSetBind(tf, abi.TableFunctionBindCallback())
	api.TableFunctionSetInit(tf, abi.TableFunctionInitCallback())
	if spec.LocalInit != nil {
		api.TableFunctionSetLocalInit(tf, abi.TableFunctionLocalInitCallback())
	}
	api.TableFunctionSetFunction(tf, abi.TableFunctionExecCallback())
	if spec.ProjectionPushdown {
		api.TableFunctionSupportsProjectionPushdown(tf, true)
	}
	return tf, extraID, nil
}

func (c *Conn) registerTableFunctionWithTx(tx *Tx, spec TableFunctionSpec) error {
	if c == nil {
		return ErrClosed
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
	tf, extraID, err := buildNativeTableFunctionLocked(api, c.limits, spec)
	if err != nil {
		return err
	}

	state := api.RegisterTableFunction(c.handle, tf)
	api.DestroyTableFunction(&tf)
	if state != abi.StateSuccess {
		// ReleaseHookHandle is idempotent: on real DuckDB DestroyTableFunction
		// already invoked HookDeleteCallback(extraID); on minimal mocks this
		// guarantees zero leaked handles.
		abi.ReleaseHookHandle(extraID)
		return nativeError("register_table_function", abi.ErrorInvalid, fmt.Sprintf("failed to register table function %q", spec.Name))
	}
	return nil
}
