package duckdb

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

func TestCopyFunctionMockCopyToCopyFromErrorsAndZeroLeaks(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	api := lib.api
	baseHooks := abi.HookHandleCount()

	var (
		mu          sync.Mutex
		nextHandle  uintptr = 7000
		liveTypes           = make(map[uintptr]int32)
		liveCFs             = make(map[uintptr]bool)
		liveTFs             = make(map[uintptr]bool)
		liveVals            = make(map[uintptr]bool)
		extraID     uintptr
		tfExtraID   uintptr
		bindID      uintptr
		globalID    uintptr
		tfBindID    uintptr
		copyFromSet bool
		failReg     bool
		bindErr     string
		initErr     string
		sinkErr     string
		finErr      string
		inVals      = []int64{7, 8}
		inValidity  = []uint64{0b11}
	)

	alloc := func() uintptr {
		mu.Lock()
		defer mu.Unlock()
		h := nextHandle
		nextHandle++
		return h
	}

	api.VectorSize = func() uint64 { return 16 }
	api.CreateLogicalType = func(tid int32) uintptr {
		h := alloc()
		mu.Lock()
		liveTypes[h] = tid
		mu.Unlock()
		return h
	}
	api.GetTypeID = func(h uintptr) int32 {
		mu.Lock()
		defer mu.Unlock()
		return liveTypes[h]
	}
	api.LogicalTypeGetAlias = func(h uintptr) uintptr { return 0 }
	api.DestroyLogicalType = func(h *uintptr) {
		if h == nil || *h == 0 {
			return
		}
		mu.Lock()
		delete(liveTypes, *h)
		mu.Unlock()
		*h = 0
	}
	api.DestroyValue = func(v *uintptr) {
		if v == nil || *v == 0 {
			return
		}
		mu.Lock()
		delete(liveVals, *v)
		mu.Unlock()
		*v = 0
	}

	// CopyFunction mocks.
	api.CreateCopyFunction = func() uintptr {
		h := alloc()
		mu.Lock()
		liveCFs[h] = true
		mu.Unlock()
		return h
	}
	api.DestroyCopyFunction = func(cf *uintptr) {
		if cf == nil || *cf == 0 {
			return
		}
		mu.Lock()
		delete(liveCFs, *cf)
		mu.Unlock()
		*cf = 0
	}
	api.CopyFunctionSetName = func(cf uintptr, name *byte) {}
	api.CopyFunctionSetExtraInfo = func(cf uintptr, extra, del uintptr) { extraID = extra }
	api.CopyFunctionSetBind = func(cf, cb uintptr) {}
	api.CopyFunctionSetGlobalInit = func(cf, cb uintptr) {}
	api.CopyFunctionSetSink = func(cf, cb uintptr) {}
	api.CopyFunctionSetFinalize = func(cf, cb uintptr) {}
	api.CopyFunctionSetCopyFromFunction = func(cf, tf uintptr) { copyFromSet = true }
	api.RegisterCopyFunction = func(conn, cf uintptr) int32 {
		if failReg {
			return abi.StateError
		}
		return abi.StateSuccess
	}

	api.CopyFunctionBindGetExtraInfo = func(info uintptr) uintptr { return extraID }
	api.CopyFunctionBindGetColumnCount = func(info uintptr) uint64 { return 1 }
	api.CopyFunctionBindGetColumnType = func(info uintptr, idx uint64) uintptr { return api.CreateLogicalType(abi.TypeBigint) }
	api.CopyFunctionBindGetOptions = func(info uintptr) uintptr { return 0 }
	api.CopyFunctionBindSetBindData = func(info, id, del uintptr) { bindID = id }
	api.CopyFunctionBindSetError = func(info uintptr, msg *byte) { bindErr = abi.GoString(uintptr(unsafe.Pointer(msg))) }

	cpath, _ := abi.CString("/tmp/mock.tsv")
	api.CopyFunctionGlobalInitGetExtraInfo = func(info uintptr) uintptr { return extraID }
	api.CopyFunctionGlobalInitGetBindData = func(info uintptr) uintptr { return bindID }
	api.CopyFunctionGlobalInitGetFilePath = func(info uintptr) uintptr { return uintptr(unsafe.Pointer(&cpath[0])) }
	api.CopyFunctionGlobalInitSetGlobalState = func(info, id, del uintptr) { globalID = id }
	api.CopyFunctionGlobalInitSetError = func(info uintptr, msg *byte) { initErr = abi.GoString(uintptr(unsafe.Pointer(msg))) }

	api.CopyFunctionSinkGetExtraInfo = func(info uintptr) uintptr { return extraID }
	api.CopyFunctionSinkGetBindData = func(info uintptr) uintptr { return bindID }
	api.CopyFunctionSinkGetGlobalState = func(info uintptr) uintptr { return globalID }
	api.CopyFunctionSinkSetError = func(info uintptr, msg *byte) { sinkErr = abi.GoString(uintptr(unsafe.Pointer(msg))) }

	api.CopyFunctionFinalizeGetExtraInfo = func(info uintptr) uintptr { return extraID }
	api.CopyFunctionFinalizeGetBindData = func(info uintptr) uintptr { return bindID }
	api.CopyFunctionFinalizeGetGlobalState = func(info uintptr) uintptr { return globalID }
	api.CopyFunctionFinalizeSetError = func(info uintptr, msg *byte) { finErr = abi.GoString(uintptr(unsafe.Pointer(msg))) }

	api.DataChunkGetSize = func(chunk uintptr) uint64 { return 2 }
	api.DataChunkGetColumnCount = func(chunk uintptr) uint64 { return 1 }
	api.DataChunkGetVector = func(chunk uintptr, col uint64) uintptr { return 950 }
	api.VectorGetData = func(vec uintptr) uintptr { return uintptr(unsafe.Pointer(&inVals[0])) }
	api.VectorGetValidity = func(vec uintptr) uintptr { return uintptr(unsafe.Pointer(&inValidity[0])) }

	// TableFunction mocks for COPY ... FROM.
	api.CreateTableFunction = func() uintptr {
		h := alloc()
		mu.Lock()
		liveTFs[h] = true
		mu.Unlock()
		return h
	}
	api.DestroyTableFunction = func(tf *uintptr) {
		if tf == nil || *tf == 0 {
			return
		}
		mu.Lock()
		delete(liveTFs, *tf)
		mu.Unlock()
		*tf = 0
	}
	api.TableFunctionSetName = func(tf uintptr, name *byte) {}
	api.TableFunctionAddParameter = func(tf, lt uintptr) {}
	api.TableFunctionAddNamedParameter = func(tf uintptr, name *byte, lt uintptr) {}
	api.TableFunctionSetExtraInfo = func(tf, extra, del uintptr) { tfExtraID = extra }
	api.TableFunctionSetBind = func(tf, cb uintptr) {}
	api.TableFunctionSetInit = func(tf, cb uintptr) {}
	api.TableFunctionSetFunction = func(tf, cb uintptr) {}

	targetColName, _ := abi.CString("id")
	api.BindGetExtraInfo = func(info uintptr) uintptr { return tfExtraID }
	api.BindGetParameterCount = func(info uintptr) uint64 { return 0 }
	api.TableFunctionBindGetResultColumnCount = func(info uintptr) uint64 { return 1 }
	api.TableFunctionBindGetResultColumnName = func(info uintptr, idx uint64) uintptr { return uintptr(unsafe.Pointer(&targetColName[0])) }
	api.TableFunctionBindGetResultColumnType = func(info uintptr, idx uint64) uintptr { return api.CreateLogicalType(abi.TypeBigint) }
	addColCalled := false
	api.BindAddResultColumn = func(info uintptr, name *byte, lt uintptr) { addColCalled = true }
	api.BindSetBindData = func(info, id, del uintptr) { tfBindID = id }
	api.BindSetError = func(info uintptr, msg *byte) { bindErr = abi.GoString(uintptr(unsafe.Pointer(msg))) }

	db, err := lib.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Validation guards.
	if err := conn.RegisterCopyFunction(CopyFunctionSpec{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty copy spec err = %v", err)
	}
	if err := conn.RegisterCopyFunction(CopyFunctionSpec{Name: "x"}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("missing Sink and CopyFrom err = %v", err)
	}
	if err := conn.RegisterCopyFunction(CopyFunctionSpec{
		Name: "bad_from",
		CopyFrom: &TableFunctionSpec{
			Parameters: []LogicalType{ScalarType(TypeBigint)},
			Bind:       func(TableFunctionBindInput) (TableFunctionBindResult, error) { return TableFunctionBindResult{}, nil },
			Execute:    func(any, any, any, *TableFunctionChunk) error { return nil },
		},
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("non-VARCHAR CopyFrom param err = %v", err)
	}

	// Registration failure must clean up both CopyTo and CopyFrom handles.
	failReg = true
	if err := conn.RegisterCopyFunction(CopyFunctionSpec{
		Name: "fail_fmt",
		Sink: func(any, any, *CopyFunctionChunk) error { return nil },
		CopyFrom: &TableFunctionSpec{
			Bind:    func(TableFunctionBindInput) (TableFunctionBindResult, error) { return TableFunctionBindResult{}, nil },
			Execute: func(any, any, any, *TableFunctionChunk) error { return nil },
		},
	}); err == nil {
		t.Fatal("expected RegisterCopyFunction error")
	}
	if got := abi.HookHandleCount(); got != baseHooks {
		t.Fatalf("HookHandleCount after failed register = %d, want %d", got, baseHooks)
	}
	failReg = false

	// Successful registration of both COPY TO and COPY FROM.
	var (
		gotPath       string
		sunk          []int64
		finalized     bool
		gotTargetCols []Column
	)
	err = conn.RegisterCopyFunction(CopyFunctionSpec{
		Name: "mock_tsv",
		Bind: func(in CopyFunctionBindInput) (any, error) {
			if len(in.ColumnTypes) != 1 || in.ColumnTypes[0].Kind != TypeBigint {
				return nil, errors.New("unexpected bind column types")
			}
			return "bind_ok", nil
		},
		GlobalInit: func(in CopyFunctionGlobalInitInput) (any, error) {
			gotPath = in.FilePath
			return "global_ok", nil
		},
		Sink: func(bd, gs any, chunk *CopyFunctionChunk) error {
			if bd != "bind_ok" || gs != "global_ok" {
				return errors.New("bad state")
			}
			for r := range chunk.Len() {
				v, err := chunk.Value(0, r)
				if err != nil {
					return err
				}
				n, _ := v.Int64()
				sunk = append(sunk, n)
			}
			return nil
		},
		Finalize: func(bd, gs any) error {
			finalized = true
			return nil
		},
		CopyFrom: &TableFunctionSpec{
			Bind: func(in TableFunctionBindInput) (TableFunctionBindResult, error) {
				gotTargetCols = in.TargetColumns
				return TableFunctionBindResult{BindData: "from_ok"}, nil
			},
			Execute: func(any, any, any, *TableFunctionChunk) error { return nil },
		},
	})
	if err != nil {
		t.Fatalf("RegisterCopyFunction: %v", err)
	}
	if !copyFromSet {
		t.Fatal("expected CopyFunctionSetCopyFromFunction to be called")
	}

	// Drive COPY TO lifecycle.
	globalCopyDispatch.Bind(1)
	if bindErr != "" || bindID == 0 {
		t.Fatalf("Copy Bind mismatch: err=%q bindID=%d", bindErr, bindID)
	}
	globalCopyDispatch.GlobalInit(1)
	if initErr != "" || globalID == 0 || gotPath != "/tmp/mock.tsv" {
		t.Fatalf("Copy GlobalInit mismatch: err=%q globalID=%d path=%q", initErr, globalID, gotPath)
	}
	globalCopyDispatch.Sink(1, 555)
	if sinkErr != "" || len(sunk) != 2 || sunk[0] != 7 || sunk[1] != 8 {
		t.Fatalf("Copy Sink mismatch: err=%q sunk=%v", sinkErr, sunk)
	}
	globalCopyDispatch.Finalize(1)
	if finErr != "" || !finalized {
		t.Fatalf("Copy Finalize mismatch: err=%q finalized=%v", finErr, finalized)
	}

	// Drive COPY FROM Bind and verify TargetColumns populated & BindAddResultColumn NOT called.
	globalTFDispatcher.Bind(1)
	if bindErr != "" || tfBindID == 0 || addColCalled {
		t.Fatalf("CopyFrom Bind mismatch: err=%q tfBindID=%d addColCalled=%v", bindErr, tfBindID, addColCalled)
	}
	if len(gotTargetCols) != 1 || gotTargetCols[0].Name != "id" || gotTargetCols[0].Type.Kind != TypeBigint {
		t.Fatalf("gotTargetCols = %+v, want [id BIGINT]", gotTargetCols)
	}

	// Simulate delete callbacks.
	abi.ReleaseHookHandle(tfBindID)
	abi.ReleaseHookHandle(globalID)
	abi.ReleaseHookHandle(bindID)
	abi.ReleaseHookHandle(extraID)
	abi.ReleaseHookHandle(tfExtraID)

	if got := abi.HookHandleCount(); got != baseHooks {
		t.Fatalf("HookHandleCount = %d, want %d", got, baseHooks)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if len(liveCFs) != 0 || len(liveTFs) != 0 || len(liveTypes) != 0 || len(liveVals) != 0 {
		t.Fatalf("leaked native handles: cf=%d tf=%d types=%d vals=%d", len(liveCFs), len(liveTFs), len(liveTypes), len(liveVals))
	}
	f.assertZeroLeaks(t)
}

func TestLogStorageMockWritePanicDoubleDeleteAndZeroLeaks(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	api := lib.api
	baseHooks := abi.HookHandleCount()

	var (
		mu         sync.Mutex
		nextHandle uintptr = 8000
		liveLS             = make(map[uintptr]bool)
		extraID    uintptr
		deleteCB   uintptr
		writeCB    uintptr
		failReg    bool
	)

	api.CreateLogStorage = func() uintptr {
		mu.Lock()
		defer mu.Unlock()
		h := nextHandle
		nextHandle++
		liveLS[h] = true
		return h
	}
	api.DestroyLogStorage = func(ls *uintptr) {
		if ls == nil || *ls == 0 {
			return
		}
		mu.Lock()
		delete(liveLS, *ls)
		mu.Unlock()
		*ls = 0
		if failReg {
			// Mirror DuckDB v1.5.6 logging-c.cpp double-delete on failure.
			abi.ReleaseHookHandle(extraID)
		}
	}
	api.LogStorageSetName = func(ls uintptr, name *byte) {}
	api.LogStorageSetExtraData = func(ls, extra, del uintptr) {
		extraID = extra
		deleteCB = del
	}
	api.LogStorageSetWriteLogEntry = func(ls, cb uintptr) { writeCB = cb }
	api.RegisterLogStorage = func(db, ls uintptr) int32 {
		if failReg {
			// Simulate internal shared_ptr destructor invoking delete callback first.
			abi.ReleaseHookHandle(extraID)
			return abi.StateError
		}
		return abi.StateSuccess
	}

	db, err := lib.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	// Validation guards.
	if err := db.RegisterLogStorage("", func(LogEntry) {}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty name err = %v", err)
	}
	if err := db.RegisterLogStorage("mem", nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("nil fn err = %v", err)
	}

	// Registration failure with double-delete must not underflow HookHandleCount.
	failReg = true
	if err := db.RegisterLogStorage("dup", func(LogEntry) {}); err == nil {
		t.Fatal("expected RegisterLogStorage failure")
	}
	if got := abi.HookHandleCount(); got != baseHooks {
		t.Fatalf("HookHandleCount after failed double-delete = %d, want %d", got, baseHooks)
	}
	failReg = false

	// Register live log storage and verify dispatch + panic recovery.
	var got []LogEntry
	if err := db.RegisterLogStorage("mem_log", func(e LogEntry) {
		if e.Message == "panic" {
			panic("logger boom")
		}
		got = append(got, e)
	}); err != nil {
		t.Fatalf("RegisterLogStorage: %v", err)
	}
	if deleteCB != abi.HookDeleteCallback() || writeCB != abi.LogStorageWriteCallback() {
		t.Fatal("unexpected callback pointers")
	}

	raw, ok := abi.LookupHookHandle(extraID)
	if !ok {
		t.Fatal("missing log storage handle")
	}
	h := raw.(abi.LogStorageHandler)
	h.Write(123456, "INFO", "QueryLog", "SELECT 1")
	h.Write(123457, "ERROR", "QueryLog", "panic") // must not crash
	if len(got) != 1 || got[0].Timestamp.Value != 123456 || got[0].Level != "INFO" || got[0].Type != "QueryLog" || !strings.Contains(got[0].Message, "SELECT 1") {
		t.Fatalf("captured entries = %+v", got)
	}

	abi.ReleaseHookHandle(extraID)
	if got := abi.HookHandleCount(); got != baseHooks {
		t.Fatalf("HookHandleCount = %d, want %d", got, baseHooks)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.RegisterLogStorage("closed", func(LogEntry) {}); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close RegisterLogStorage = %v, want ErrClosed", err)
	}
	if len(liveLS) != 0 {
		t.Fatalf("leaked %d log storage handles", len(liveLS))
	}
	f.assertZeroLeaks(t)
}
