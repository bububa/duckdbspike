package abi

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/ebitengine/purego"
)

// ReplacementScanHandler implements the Go side of a DuckDB replacement scan
// callback registered via Database.AddReplacementScan.
type ReplacementScanHandler interface {
	Scan(info uintptr, tableName string)
}

// TableFunctionDispatcher routes DuckDB's 4 table-function C callbacks
// (Bind, Init, LocalInit, Execute) from the process-wide singleton purego
// trampolines into package duckdb.
type TableFunctionDispatcher interface {
	Bind(info uintptr)
	Init(info uintptr)
	LocalInit(info uintptr)
	Execute(info, output uintptr)
}

// ScalarFunctionDispatcher routes DuckDB's 3 scalar-function C callbacks
// (Bind, Init, Execute) from process-wide singleton purego trampolines into
// package duckdb.
type ScalarFunctionDispatcher interface {
	Bind(info uintptr)
	Init(info uintptr)
	Execute(info, input, output uintptr)
}

// AggregateFunctionDispatcher routes DuckDB's 6 aggregate-function C callbacks
// (StateSize, Init, Update, Combine, Finalize, Destroy) from process-wide
// singleton purego trampolines into package duckdb.
type AggregateFunctionDispatcher interface {
	StateSize(info uintptr) uint64
	Init(info, state uintptr)
	Update(info, input, states uintptr)
	Combine(info, source, target uintptr, count uint64)
	Finalize(info, source, result uintptr, count, offset uint64)
	Destroy(states uintptr, count uint64)
}

// CastFunctionDispatcher routes DuckDB's cast-function C callback from a
// process-wide singleton purego trampoline into package duckdb.
type CastFunctionDispatcher interface {
	Execute(info uintptr, count uint64, input, output uintptr) bool
}

// CopyFunctionDispatcher routes DuckDB's 4 COPY ... TO C callbacks
// (Bind, GlobalInit, Sink, Finalize) from process-wide singleton purego
// trampolines into package duckdb.
type CopyFunctionDispatcher interface {
	Bind(info uintptr)
	GlobalInit(info uintptr)
	Sink(info, input uintptr)
	Finalize(info uintptr)
}

// LogStorageHandler implements the Go side of a DuckDB custom log storage
// registered via Database.RegisterLogStorage.
type LogStorageHandler interface {
	Write(timestampMicros int64, level, logType, message string)
}

type tableFunctionDispatcherBox struct {
	d TableFunctionDispatcher
}

type scalarFunctionDispatcherBox struct {
	d ScalarFunctionDispatcher
}

type aggregateFunctionDispatcherBox struct {
	d AggregateFunctionDispatcher
}

type castFunctionDispatcherBox struct {
	d CastFunctionDispatcher
}

type copyFunctionDispatcherBox struct {
	d CopyFunctionDispatcher
}

var (
	hookSeq     atomic.Uintptr
	hookCount   atomic.Int64
	hookHandles sync.Map // map[uintptr]any

	hookDeleteOnce sync.Once
	hookDeleteCB   uintptr

	hookCopyOnce sync.Once
	hookCopyCB   uintptr

	replacementScanOnce sync.Once
	replacementScanCB   uintptr

	tableFuncOnce     sync.Once
	tableFuncBindCB   uintptr
	tableFuncInitCB   uintptr
	tableFuncLocalCB  uintptr
	tableFuncExecCB   uintptr
	tableFuncDispatch atomic.Pointer[tableFunctionDispatcherBox]

	scalarFuncOnce     sync.Once
	scalarFuncBindCB   uintptr
	scalarFuncInitCB   uintptr
	scalarFuncExecCB   uintptr
	scalarFuncDispatch atomic.Pointer[scalarFunctionDispatcherBox]

	aggFuncOnce       sync.Once
	aggFuncSizeCB     uintptr
	aggFuncInitCB     uintptr
	aggFuncUpdateCB   uintptr
	aggFuncCombineCB  uintptr
	aggFuncFinalizeCB uintptr
	aggFuncDestroyCB  uintptr
	aggFuncDispatch   atomic.Pointer[aggregateFunctionDispatcherBox]

	castFuncOnce     sync.Once
	castFuncExecCB   uintptr
	castFuncDispatch atomic.Pointer[castFunctionDispatcherBox]

	copyFuncOnce       sync.Once
	copyFuncBindCB     uintptr
	copyFuncInitCB     uintptr
	copyFuncSinkCB     uintptr
	copyFuncFinalizeCB uintptr
	copyFuncDispatch   atomic.Pointer[copyFunctionDispatcherBox]

	logStorageOnce    sync.Once
	logStorageWriteCB uintptr
)

// RegisterHookHandle stores v in the process-wide hook handle table and returns
// a non-zero integer token safe to pass across C void* extra_info / bind_data /
// init_data / aggregate state boundaries without violating Go pointer rules.
func RegisterHookHandle(v any) uintptr {
	for {
		if id := hookSeq.Add(1); id != 0 {
			hookHandles.Store(id, v)
			hookCount.Add(1)
			return id
		}
	}
}

// LookupHookHandle looks up a live hook handle previously registered with
// RegisterHookHandle.
func LookupHookHandle(id uintptr) (any, bool) {
	if id == 0 {
		return nil, false
	}
	return hookHandles.Load(id)
}

// ReleaseHookHandle removes id from the hook handle table. Release is
// idempotent and safe to call concurrently.
func ReleaseHookHandle(id uintptr) {
	if id == 0 {
		return
	}
	if _, ok := hookHandles.LoadAndDelete(id); ok {
		hookCount.Add(-1)
	}
}

// CloneHookHandle allocates a new handle ID referencing the same stored value
// as id, returning 0 if id is not present. Used by HookCopyCallback so DuckDB
// can safely copy and independently destroy read-only bind data.
func CloneHookHandle(id uintptr) uintptr {
	v, ok := LookupHookHandle(id)
	if !ok {
		return 0
	}
	return RegisterHookHandle(v)
}

// HookHandleCount returns the number of currently live Go hook handles awaiting
// DuckDB delete callbacks. Intended for leak detection in tests.
func HookHandleCount() int64 {
	return hookCount.Load()
}

// HookDeleteCallback returns the process-wide singleton C delete callback
// (`void (*)(void *data)`) that releases a handle registered via
// RegisterHookHandle. Consumes 1 permanent purego callback slot.
func HookDeleteCallback() uintptr {
	hookDeleteOnce.Do(func() {
		hookDeleteCB = purego.NewCallback(func(_ purego.CDecl, data uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			ReleaseHookHandle(data)
			return 0
		})
	})
	return hookDeleteCB
}

// HookCopyCallback returns the process-wide singleton C copy callback
// (`void *(*)(void *data)`) that clones a handle ID via CloneHookHandle so
// DuckDB can copy and independently destroy read-only bind data. Consumes 1
// permanent purego callback slot.
func HookCopyCallback() uintptr {
	hookCopyOnce.Do(func() {
		hookCopyCB = purego.NewCallback(func(_ purego.CDecl, data uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			return CloneHookHandle(data)
		})
	})
	return hookCopyCB
}

// ReplacementScanCallback returns the process-wide singleton C callback
// (`void (*)(duckdb_replacement_scan_info, const char *, void *)`) that
// dispatches to a ReplacementScanHandler stored via RegisterHookHandle.
// Consumes 1 permanent purego callback slot.
func ReplacementScanCallback() uintptr {
	replacementScanOnce.Do(func() {
		replacementScanCB = purego.NewCallback(func(_ purego.CDecl, info, tableNamePtr, data uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if info == 0 || data == 0 {
				return 0
			}
			v, ok := LookupHookHandle(data)
			if !ok {
				return 0
			}
			handler, ok := v.(ReplacementScanHandler)
			if !ok || handler == nil {
				return 0
			}
			handler.Scan(info, GoString(tableNamePtr))
			return 0
		})
	})
	return replacementScanCB
}

// SetTableFunctionDispatcher installs the active dispatcher invoked by the 4
// singleton TableFunction C callbacks.
func SetTableFunctionDispatcher(d TableFunctionDispatcher) {
	if d == nil {
		tableFuncDispatch.Store(nil)
		return
	}
	tableFuncDispatch.Store(&tableFunctionDispatcherBox{d: d})
}

func ensureTableFunctionCallbacks() {
	tableFuncOnce.Do(func() {
		tableFuncBindCB = purego.NewCallback(func(_ purego.CDecl, info uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if box := tableFuncDispatch.Load(); box != nil && box.d != nil && info != 0 {
				box.d.Bind(info)
			}
			return 0
		})
		tableFuncInitCB = purego.NewCallback(func(_ purego.CDecl, info uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if box := tableFuncDispatch.Load(); box != nil && box.d != nil && info != 0 {
				box.d.Init(info)
			}
			return 0
		})
		tableFuncLocalCB = purego.NewCallback(func(_ purego.CDecl, info uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if box := tableFuncDispatch.Load(); box != nil && box.d != nil && info != 0 {
				box.d.LocalInit(info)
			}
			return 0
		})
		tableFuncExecCB = purego.NewCallback(func(_ purego.CDecl, info, output uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if box := tableFuncDispatch.Load(); box != nil && box.d != nil && info != 0 && output != 0 {
				box.d.Execute(info, output)
			}
			return 0
		})
	})
}

// TableFunctionBindCallback returns the process-wide singleton C bind callback.
func TableFunctionBindCallback() uintptr {
	ensureTableFunctionCallbacks()
	return tableFuncBindCB
}

// TableFunctionInitCallback returns the process-wide singleton C global-init callback.
func TableFunctionInitCallback() uintptr {
	ensureTableFunctionCallbacks()
	return tableFuncInitCB
}

// TableFunctionLocalInitCallback returns the process-wide singleton C local-init callback.
func TableFunctionLocalInitCallback() uintptr {
	ensureTableFunctionCallbacks()
	return tableFuncLocalCB
}

// TableFunctionExecCallback returns the process-wide singleton C execution callback.
func TableFunctionExecCallback() uintptr {
	ensureTableFunctionCallbacks()
	return tableFuncExecCB
}

// SetScalarFunctionDispatcher installs the active dispatcher invoked by the 3
// singleton ScalarFunction C callbacks.
func SetScalarFunctionDispatcher(d ScalarFunctionDispatcher) {
	if d == nil {
		scalarFuncDispatch.Store(nil)
		return
	}
	scalarFuncDispatch.Store(&scalarFunctionDispatcherBox{d: d})
}

func ensureScalarFunctionCallbacks() {
	scalarFuncOnce.Do(func() {
		scalarFuncBindCB = purego.NewCallback(func(_ purego.CDecl, info uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if box := scalarFuncDispatch.Load(); box != nil && box.d != nil && info != 0 {
				box.d.Bind(info)
			}
			return 0
		})
		scalarFuncInitCB = purego.NewCallback(func(_ purego.CDecl, info uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if box := scalarFuncDispatch.Load(); box != nil && box.d != nil && info != 0 {
				box.d.Init(info)
			}
			return 0
		})
		scalarFuncExecCB = purego.NewCallback(func(_ purego.CDecl, info, input, output uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if box := scalarFuncDispatch.Load(); box != nil && box.d != nil && info != 0 && input != 0 && output != 0 {
				box.d.Execute(info, input, output)
			}
			return 0
		})
	})
}

// ScalarFunctionBindCallback returns the process-wide singleton C scalar bind callback.
func ScalarFunctionBindCallback() uintptr {
	ensureScalarFunctionCallbacks()
	return scalarFuncBindCB
}

// ScalarFunctionInitCallback returns the process-wide singleton C scalar init callback.
func ScalarFunctionInitCallback() uintptr {
	ensureScalarFunctionCallbacks()
	return scalarFuncInitCB
}

// ScalarFunctionExecCallback returns the process-wide singleton C scalar execution callback.
func ScalarFunctionExecCallback() uintptr {
	ensureScalarFunctionCallbacks()
	return scalarFuncExecCB
}

// SetAggregateFunctionDispatcher installs the active dispatcher invoked by the
// 6 singleton AggregateFunction C callbacks.
func SetAggregateFunctionDispatcher(d AggregateFunctionDispatcher) {
	if d == nil {
		aggFuncDispatch.Store(nil)
		return
	}
	aggFuncDispatch.Store(&aggregateFunctionDispatcherBox{d: d})
}

func ensureAggregateFunctionCallbacks() {
	aggFuncOnce.Do(func() {
		aggFuncSizeCB = purego.NewCallback(func(_ purego.CDecl, info uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if box := aggFuncDispatch.Load(); box != nil && box.d != nil {
				return uintptr(box.d.StateSize(info))
			}
			return unsafe.Sizeof(uintptr(0))
		})
		aggFuncInitCB = purego.NewCallback(func(_ purego.CDecl, info, state uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if box := aggFuncDispatch.Load(); box != nil && box.d != nil && state != 0 {
				box.d.Init(info, state)
			}
			return 0
		})
		aggFuncUpdateCB = purego.NewCallback(func(_ purego.CDecl, info, input, states uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if box := aggFuncDispatch.Load(); box != nil && box.d != nil && info != 0 && input != 0 && states != 0 {
				box.d.Update(info, input, states)
			}
			return 0
		})
		aggFuncCombineCB = purego.NewCallback(func(_ purego.CDecl, info, source, target, count uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if box := aggFuncDispatch.Load(); box != nil && box.d != nil && info != 0 && source != 0 && target != 0 && count != 0 {
				box.d.Combine(info, source, target, uint64(count))
			}
			return 0
		})
		aggFuncFinalizeCB = purego.NewCallback(func(_ purego.CDecl, info, source, result, count, offset uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if box := aggFuncDispatch.Load(); box != nil && box.d != nil && info != 0 && source != 0 && result != 0 && count != 0 {
				box.d.Finalize(info, source, result, uint64(count), uint64(offset))
			}
			return 0
		})
		aggFuncDestroyCB = purego.NewCallback(func(_ purego.CDecl, states, count uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if box := aggFuncDispatch.Load(); box != nil && box.d != nil && states != 0 && count != 0 {
				box.d.Destroy(states, uint64(count))
			}
			return 0
		})
	})
}

// AggregateStateSizeCallback returns the process-wide singleton C aggregate state-size callback.
func AggregateStateSizeCallback() uintptr {
	ensureAggregateFunctionCallbacks()
	return aggFuncSizeCB
}

// AggregateInitCallback returns the process-wide singleton C aggregate state-init callback.
func AggregateInitCallback() uintptr {
	ensureAggregateFunctionCallbacks()
	return aggFuncInitCB
}

// AggregateUpdateCallback returns the process-wide singleton C aggregate update callback.
func AggregateUpdateCallback() uintptr {
	ensureAggregateFunctionCallbacks()
	return aggFuncUpdateCB
}

// AggregateCombineCallback returns the process-wide singleton C aggregate combine callback.
func AggregateCombineCallback() uintptr {
	ensureAggregateFunctionCallbacks()
	return aggFuncCombineCB
}

// AggregateFinalizeCallback returns the process-wide singleton C aggregate finalize callback.
func AggregateFinalizeCallback() uintptr {
	ensureAggregateFunctionCallbacks()
	return aggFuncFinalizeCB
}

// AggregateDestroyCallback returns the process-wide singleton C aggregate state destructor callback.
func AggregateDestroyCallback() uintptr {
	ensureAggregateFunctionCallbacks()
	return aggFuncDestroyCB
}

// SetCastFunctionDispatcher installs the active dispatcher invoked by the
// singleton CastFunction C callback.
func SetCastFunctionDispatcher(d CastFunctionDispatcher) {
	if d == nil {
		castFuncDispatch.Store(nil)
		return
	}
	castFuncDispatch.Store(&castFunctionDispatcherBox{d: d})
}

// CastFunctionExecCallback returns the process-wide singleton C cast callback
// (`bool (*)(duckdb_function_info, idx_t, duckdb_vector, duckdb_vector)`).
func CastFunctionExecCallback() uintptr {
	castFuncOnce.Do(func() {
		castFuncExecCB = purego.NewCallback(func(_ purego.CDecl, info, count, input, output uintptr) uintptr {
			ok := false
			defer func() {
				_ = recover()
			}()
			if box := castFuncDispatch.Load(); box != nil && box.d != nil && info != 0 && input != 0 && output != 0 {
				ok = box.d.Execute(info, uint64(count), input, output)
			}
			if ok {
				return 1
			}
			return 0
		})
	})
	return castFuncExecCB
}

// SetCopyFunctionDispatcher installs the active dispatcher invoked by the 4
// singleton CopyFunction C callbacks.
func SetCopyFunctionDispatcher(d CopyFunctionDispatcher) {
	if d == nil {
		copyFuncDispatch.Store(nil)
		return
	}
	copyFuncDispatch.Store(&copyFunctionDispatcherBox{d: d})
}

func ensureCopyFunctionCallbacks() {
	copyFuncOnce.Do(func() {
		copyFuncBindCB = purego.NewCallback(func(_ purego.CDecl, info uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if box := copyFuncDispatch.Load(); box != nil && box.d != nil && info != 0 {
				box.d.Bind(info)
			}
			return 0
		})
		copyFuncInitCB = purego.NewCallback(func(_ purego.CDecl, info uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if box := copyFuncDispatch.Load(); box != nil && box.d != nil && info != 0 {
				box.d.GlobalInit(info)
			}
			return 0
		})
		copyFuncSinkCB = purego.NewCallback(func(_ purego.CDecl, info, input uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if box := copyFuncDispatch.Load(); box != nil && box.d != nil && info != 0 && input != 0 {
				box.d.Sink(info, input)
			}
			return 0
		})
		copyFuncFinalizeCB = purego.NewCallback(func(_ purego.CDecl, info uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if box := copyFuncDispatch.Load(); box != nil && box.d != nil && info != 0 {
				box.d.Finalize(info)
			}
			return 0
		})
	})
}

// CopyFunctionBindCallback returns the process-wide singleton C COPY TO bind callback.
func CopyFunctionBindCallback() uintptr {
	ensureCopyFunctionCallbacks()
	return copyFuncBindCB
}

// CopyFunctionGlobalInitCallback returns the process-wide singleton C COPY TO global-init callback.
func CopyFunctionGlobalInitCallback() uintptr {
	ensureCopyFunctionCallbacks()
	return copyFuncInitCB
}

// CopyFunctionSinkCallback returns the process-wide singleton C COPY TO sink callback.
func CopyFunctionSinkCallback() uintptr {
	ensureCopyFunctionCallbacks()
	return copyFuncSinkCB
}

// CopyFunctionFinalizeCallback returns the process-wide singleton C COPY TO finalize callback.
func CopyFunctionFinalizeCallback() uintptr {
	ensureCopyFunctionCallbacks()
	return copyFuncFinalizeCB
}

// LogStorageWriteCallback returns the process-wide singleton C log entry write
// callback (`void (*)(void *extra_data, duckdb_timestamp *timestamp, const char *level, const char *log_type, const char *log_message)`).
func LogStorageWriteCallback() uintptr {
	logStorageOnce.Do(func() {
		logStorageWriteCB = purego.NewCallback(func(_ purego.CDecl, extraData, tsPtr, levelPtr, logTypePtr, msgPtr uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if extraData == 0 {
				return 0
			}
			v, ok := LookupHookHandle(extraData)
			if !ok {
				return 0
			}
			handler, ok := v.(LogStorageHandler)
			if !ok || handler == nil {
				return 0
			}
			var micros int64
			if tsPtr != 0 {
				micros = (*Timestamp)(Pointer(tsPtr)).Micros
			}
			handler.Write(micros, GoString(levelPtr), GoString(logTypePtr), GoString(msgPtr))
			return 0
		})
	})
	return logStorageWriteCB
}
