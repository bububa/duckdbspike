package duckdb

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

func TestCustomConfigOptionAndLogicalTypeMockLifecycleAndZeroLeaks(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	api := lib.api

	var (
		mu           sync.Mutex
		nextHandle   uintptr = 1000
		liveOpts             = make(map[uintptr]bool)
		liveTypes            = make(map[uintptr]int32)
		typeAliases          = make(map[uintptr]string)
		liveVals             = make(map[uintptr]bool)
		lastOptName  string
		lastOptDesc  string
		lastOptScope int32
		lastRegType  uintptr
		lastRegAlias string
		failRegOpt   bool
		failRegType  bool
	)

	allocHandle := func() uintptr {
		mu.Lock()
		defer mu.Unlock()
		h := nextHandle
		nextHandle++
		return h
	}

	api.CreateConfigOption = func() uintptr {
		h := allocHandle()
		mu.Lock()
		liveOpts[h] = true
		mu.Unlock()
		return h
	}
	api.DestroyConfigOption = func(opt *uintptr) {
		if opt == nil || *opt == 0 {
			return
		}
		mu.Lock()
		delete(liveOpts, *opt)
		mu.Unlock()
		*opt = 0
	}
	api.ConfigOptionSetName = func(opt uintptr, name *byte) {
		lastOptName = abi.GoString(uintptr(unsafe.Pointer(name)))
	}
	api.ConfigOptionSetType = func(opt uintptr, lt uintptr) {}
	api.ConfigOptionSetDefaultValue = func(opt uintptr, dv uintptr) {}
	api.ConfigOptionSetDefaultScope = func(opt uintptr, scope int32) {
		lastOptScope = scope
	}
	api.ConfigOptionSetDescription = func(opt uintptr, desc *byte) {
		lastOptDesc = abi.GoString(uintptr(unsafe.Pointer(desc)))
	}
	api.RegisterConfigOption = func(conn uintptr, opt uintptr) int32 {
		if failRegOpt {
			return abi.StateError
		}
		return abi.StateSuccess
	}

	api.CreateLogicalType = func(typeID int32) uintptr {
		h := allocHandle()
		mu.Lock()
		liveTypes[h] = typeID
		mu.Unlock()
		return h
	}
	api.CreateStructType = func(memberTypes *uintptr, memberNames *uintptr, count uint64) uintptr {
		h := allocHandle()
		mu.Lock()
		liveTypes[h] = abi.TypeStruct
		mu.Unlock()
		return h
	}
	api.LogicalTypeSetAlias = func(h uintptr, alias *byte) {
		mu.Lock()
		typeAliases[h] = abi.GoString(uintptr(unsafe.Pointer(alias)))
		mu.Unlock()
	}
	api.DestroyLogicalType = func(lt *uintptr) {
		if lt == nil || *lt == 0 {
			return
		}
		mu.Lock()
		delete(liveTypes, *lt)
		delete(typeAliases, *lt)
		mu.Unlock()
		*lt = 0
	}
	api.CreateVarcharLength = func(text *byte, length uint64) uintptr {
		h := allocHandle()
		mu.Lock()
		liveVals[h] = true
		mu.Unlock()
		return h
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
	api.RegisterLogicalType = func(conn uintptr, lt uintptr, info uintptr) int32 {
		if failRegType {
			return abi.StateError
		}
		mu.Lock()
		lastRegType = lt
		lastRegAlias = typeAliases[lt]
		mu.Unlock()
		return abi.StateSuccess
	}

	db, err := lib.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Config option validation guards.
	if err := conn.RegisterConfigOption(ConfigOptionSpec{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty name err = %v, want ErrInvalidInput", err)
	}
	if err := conn.RegisterConfigOption(ConfigOptionSpec{Name: "x"}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("missing type & default err = %v, want ErrInvalidInput", err)
	}
	if err := conn.RegisterConfigOption(ConfigOptionSpec{Name: "x", Type: AnyType()}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("ANY type err = %v, want ErrInvalidInput", err)
	}
	defSent := DefaultValue()
	if err := conn.RegisterConfigOption(ConfigOptionSpec{Name: "x", DefaultValue: &defSent}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("DefaultValue sentinel err = %v, want ErrInvalidInput", err)
	}
	if err := conn.RegisterConfigOption(ConfigOptionSpec{Name: "x", Type: ScalarType(TypeVarchar), DefaultScope: ConfigOptionScope(99)}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid scope err = %v, want ErrInvalidInput", err)
	}

	// Successful config option registration.
	defVal := VarcharValue("prod")
	if err := conn.RegisterConfigOption(ConfigOptionSpec{
		Name:         "app_env",
		Type:         ScalarType(TypeVarchar),
		DefaultValue: &defVal,
		DefaultScope: ConfigOptionScopeSession,
		Description:  "application environment",
	}); err != nil {
		t.Fatalf("RegisterConfigOption: %v", err)
	}
	if lastOptName != "app_env" || lastOptDesc != "application environment" || lastOptScope != abi.ConfigOptionScopeSession {
		t.Fatalf("captured config option = (%q, %q, %d)", lastOptName, lastOptDesc, lastOptScope)
	}

	// Registration error path must still destroy handles.
	failRegOpt = true
	if err := conn.RegisterConfigOption(ConfigOptionSpec{Name: "bad_opt", Type: ScalarType(TypeVarchar)}); err == nil {
		t.Fatal("expected error when RegisterConfigOption returns StateError")
	}
	failRegOpt = false

	// Custom logical type validation guards.
	if err := conn.RegisterLogicalType("", ScalarType(TypeBigint)); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty logical type name err = %v, want ErrInvalidInput", err)
	}
	if err := conn.RegisterLogicalType("bad_any", AnyType()); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("ANY logical type err = %v, want ErrInvalidInput", err)
	}
	conflictLT := ScalarType(TypeBigint)
	conflictLT.Alias = "other_name"
	if err := conn.RegisterLogicalType("my_id", conflictLT); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("conflicting alias err = %v, want ErrInvalidInput", err)
	}

	// Aliased composite STRUCT logical type must propagate alias to native handle.
	geoStruct := StructType(
		StructFieldType{Name: "lat", Type: ScalarType(TypeDouble)},
		StructFieldType{Name: "lng", Type: ScalarType(TypeDouble)},
	)
	if err := conn.RegisterLogicalType("geo_pair", geoStruct); err != nil {
		t.Fatalf("RegisterLogicalType(geo_pair): %v", err)
	}
	if lastRegType == 0 || lastRegAlias != "geo_pair" {
		t.Fatalf("registered type=(%d, %q), want non-zero handle with alias geo_pair", lastRegType, lastRegAlias)
	}

	failRegType = true
	if err := conn.RegisterLogicalType("dup_type", ScalarType(TypeBigint)); err == nil {
		t.Fatal("expected error when RegisterLogicalType returns StateError")
	}
	failRegType = false

	// Nil Tx guards.
	var nilTx *Tx
	if err := nilTx.RegisterConfigOption(ConfigOptionSpec{Name: "a", Type: ScalarType(TypeBigint)}); !errors.Is(err, ErrTxDone) {
		t.Fatalf("nil Tx.RegisterConfigOption = %v, want ErrTxDone", err)
	}
	if err := nilTx.RegisterLogicalType("a", ScalarType(TypeBigint)); !errors.Is(err, ErrTxDone) {
		t.Fatalf("nil Tx.RegisterLogicalType = %v, want ErrTxDone", err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := conn.RegisterConfigOption(ConfigOptionSpec{Name: "closed", Type: ScalarType(TypeBigint)}); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close RegisterConfigOption = %v, want ErrClosed", err)
	}
	if err := conn.RegisterLogicalType("closed", ScalarType(TypeBigint)); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close RegisterLogicalType = %v, want ErrClosed", err)
	}

	if len(liveOpts) != 0 || len(liveTypes) != 0 || len(liveVals) != 0 {
		t.Fatalf("leaked native handles: opts=%d types=%d vals=%d", len(liveOpts), len(liveTypes), len(liveVals))
	}
	f.assertZeroLeaks(t)
}

func TestReplacementScanMockDispatchErrorPanicAndCleanup(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	api := lib.api
	baseHooks := abi.HookHandleCount()

	var (
		mu          sync.Mutex
		nextVal     uintptr = 2000
		liveVals            = make(map[uintptr]string)
		extraID     uintptr
		deleteCB    uintptr
		fnName      string
		addedParams []string
		lastErr     string
	)

	api.AddReplacementScan = func(db uintptr, replacement uintptr, extra uintptr, del uintptr) {
		if replacement != abi.ReplacementScanCallback() {
			t.Errorf("unexpected replacement callback pointer %#x", replacement)
		}
		if del != abi.HookDeleteCallback() {
			t.Errorf("unexpected delete callback pointer %#x", del)
		}
		extraID = extra
		deleteCB = del
	}
	api.ReplacementScanSetFunctionName = func(info uintptr, name *byte) {
		fnName = abi.GoString(uintptr(unsafe.Pointer(name)))
	}
	api.ReplacementScanAddParameter = func(info uintptr, param uintptr) {
		mu.Lock()
		addedParams = append(addedParams, liveVals[param])
		mu.Unlock()
	}
	api.ReplacementScanSetError = func(info uintptr, errMsg *byte) {
		lastErr = abi.GoString(uintptr(unsafe.Pointer(errMsg)))
	}
	api.CreateVarcharLength = func(text *byte, length uint64) uintptr {
		mu.Lock()
		defer mu.Unlock()
		h := nextVal
		nextVal++
		if length == 0 {
			liveVals[h] = ""
		} else {
			liveVals[h] = string(unsafe.Slice(text, int(length)))
		}
		return h
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

	db, err := lib.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	if err := db.AddReplacementScan(nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("nil fn err = %v, want ErrInvalidInput", err)
	}

	if err := db.AddReplacementScan(func(tableName string) (*ReplacementScanResult, error) {
		switch tableName {
		case "mem://orders":
			return &ReplacementScanResult{
				FunctionName: "read_mem",
				Parameters:   []Value{VarcharValue("orders")},
			}, nil
		case "err://boom":
			return nil, errors.New("custom scan error")
		case "panic://boom":
			panic("scan boom")
		case "empty://fn":
			return &ReplacementScanResult{}, nil
		default:
			return nil, nil
		}
	}); err != nil {
		t.Fatalf("AddReplacementScan: %v", err)
	}

	if got := abi.HookHandleCount(); got != baseHooks+1 {
		t.Fatalf("HookHandleCount = %d, want %d", got, baseHooks+1)
	}
	if deleteCB == 0 || extraID == 0 {
		t.Fatalf("AddReplacementScan did not register extraID/deleteCB")
	}

	raw, ok := abi.LookupHookHandle(extraID)
	if !ok {
		t.Fatal("replacement scan handle missing")
	}
	handler := raw.(abi.ReplacementScanHandler)

	// 1. Decline unknown table name.
	handler.Scan(1, "normal_table")
	if fnName != "" || lastErr != "" || len(addedParams) != 0 {
		t.Fatalf("decline mutated state: fn=%q err=%q params=%v", fnName, lastErr, addedParams)
	}

	// 2. Successful replacement with parameter.
	handler.Scan(1, "mem://orders")
	if fnName != "read_mem" || len(addedParams) != 1 || addedParams[0] != "orders" || lastErr != "" {
		t.Fatalf("match got fn=%q params=%v err=%q", fnName, addedParams, lastErr)
	}
	if len(liveVals) != 0 {
		t.Fatalf("leaked %d parameter values after replacement scan", len(liveVals))
	}

	// 3. Error propagation.
	lastErr = ""
	handler.Scan(1, "err://boom")
	if !strings.Contains(lastErr, "custom scan error") {
		t.Fatalf("error got %q, want custom scan error", lastErr)
	}

	// 4. Panic recovery.
	lastErr = ""
	handler.Scan(1, "panic://boom")
	if !strings.Contains(lastErr, "scan boom") {
		t.Fatalf("panic got %q, want scan boom", lastErr)
	}

	// 5. Empty function name rejected.
	lastErr = ""
	handler.Scan(1, "empty://fn")
	if !strings.Contains(lastErr, "empty function name") {
		t.Fatalf("empty fn err got %q", lastErr)
	}

	// Simulate DuckDB invoking delete callback on database close.
	abi.ReleaseHookHandle(extraID)
	if got := abi.HookHandleCount(); got != baseHooks {
		t.Fatalf("HookHandleCount after release = %d, want %d", got, baseHooks)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.AddReplacementScan(func(string) (*ReplacementScanResult, error) { return nil, nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close AddReplacementScan = %v, want ErrClosed", err)
	}
	f.assertZeroLeaks(t)
}

func TestTableFunctionMockLifecycleProjectionPushdownAndZeroLeaks(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	api := lib.api
	baseHooks := abi.HookHandleCount()

	var (
		mu           sync.Mutex
		nextHandle   uintptr = 3000
		liveTFs              = make(map[uintptr]bool)
		liveTypes            = make(map[uintptr]int32)
		extraID      uintptr
		bindID       uintptr
		initID       uintptr
		localID      uintptr
		bindErr      string
		initErr      string
		funcErr      string
		maxThreads   uint64
		cardinality  uint64
		exactCard    bool
		addedCols    []string
		chunkSize    uint64
		projIndices  = []uint64{1} // simulate projecting only column 1
		failRegister bool
		vecBuf       = make([]int64, 16)
		validityBuf  = make([]uint64, 1)
		localInitSet atomic.Bool
	)
	validityBuf[0] = ^uint64(0)

	alloc := func() uintptr {
		mu.Lock()
		defer mu.Unlock()
		h := nextHandle
		nextHandle++
		return h
	}

	api.VectorSize = func() uint64 { return 16 }
	api.CreateLogicalType = func(typeID int32) uintptr {
		h := alloc()
		mu.Lock()
		liveTypes[h] = typeID
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
	api.TableFunctionAddParameter = func(tf uintptr, lt uintptr) {}
	api.TableFunctionAddNamedParameter = func(tf uintptr, name *byte, lt uintptr) {}
	api.TableFunctionSetExtraInfo = func(tf uintptr, extra uintptr, del uintptr) {
		extraID = extra
	}
	api.TableFunctionSetBind = func(tf uintptr, cb uintptr) {}
	api.TableFunctionSetInit = func(tf uintptr, cb uintptr) {}
	api.TableFunctionSetLocalInit = func(tf uintptr, cb uintptr) {
		localInitSet.Store(true)
	}
	api.TableFunctionSetFunction = func(tf uintptr, cb uintptr) {}
	api.TableFunctionSupportsProjectionPushdown = func(tf uintptr, pushdown bool) {}
	api.RegisterTableFunction = func(conn uintptr, tf uintptr) int32 {
		if failRegister {
			return abi.StateError
		}
		return abi.StateSuccess
	}

	// Bind / Init / Execute mocks
	api.BindGetExtraInfo = func(info uintptr) uintptr { return extraID }
	api.BindGetParameterCount = func(info uintptr) uint64 { return 1 }
	api.BindGetParameter = func(info uintptr, idx uint64) uintptr { return 9001 }
	api.BindGetNamedParameter = func(info uintptr, name *byte) uintptr {
		if abi.GoString(uintptr(unsafe.Pointer(name))) == "step" {
			return 9002
		}
		return 0
	}
	api.GetValueType = func(val uintptr) uintptr {
		// Return a persistent BIGINT logical type handle for mock decoding.
		return api.CreateLogicalType(abi.TypeBigint)
	}
	api.IsNullValue = func(val uintptr) bool { return false }
	api.GetInt64 = func(val uintptr) int64 {
		if val == 9001 {
			return 3
		}
		return 10
	}
	api.DestroyValue = func(v *uintptr) {
		if v != nil {
			*v = 0
		}
	}
	api.BindAddResultColumn = func(info uintptr, name *byte, lt uintptr) {
		addedCols = append(addedCols, abi.GoString(uintptr(unsafe.Pointer(name))))
	}
	api.BindSetCardinality = func(info uintptr, card uint64, exact bool) {
		cardinality = card
		exactCard = exact
	}
	api.BindSetBindData = func(info uintptr, id uintptr, del uintptr) { bindID = id }
	api.BindSetError = func(info uintptr, msg *byte) {
		bindErr = abi.GoString(uintptr(unsafe.Pointer(msg)))
	}

	api.InitGetExtraInfo = func(info uintptr) uintptr { return extraID }
	api.InitGetBindData = func(info uintptr) uintptr { return bindID }
	api.InitGetColumnCount = func(info uintptr) uint64 { return uint64(len(projIndices)) }
	api.InitGetColumnIndex = func(info uintptr, idx uint64) uint64 { return projIndices[idx] }
	api.InitSetMaxThreads = func(info uintptr, mt uint64) { maxThreads = mt }
	api.InitSetInitData = func(info uintptr, id uintptr, del uintptr) {
		if info == 2 {
			localID = id
		} else {
			initID = id
		}
	}
	api.InitSetError = func(info uintptr, msg *byte) {
		initErr = abi.GoString(uintptr(unsafe.Pointer(msg)))
	}

	api.FunctionGetExtraInfo = func(info uintptr) uintptr { return extraID }
	api.FunctionGetBindData = func(info uintptr) uintptr { return bindID }
	api.FunctionGetInitData = func(info uintptr) uintptr { return initID }
	api.FunctionGetLocalInitData = func(info uintptr) uintptr { return localID }
	api.FunctionSetError = func(info uintptr, msg *byte) {
		funcErr = abi.GoString(uintptr(unsafe.Pointer(msg)))
	}
	api.DataChunkGetVector = func(chunk uintptr, col uint64) uintptr { return 500 + uintptr(col) }
	api.DataChunkSetSize = func(chunk uintptr, sz uint64) { chunkSize = sz }
	api.VectorGetData = func(vec uintptr) uintptr { return uintptr(unsafe.Pointer(&vecBuf[0])) }
	api.VectorGetValidity = func(vec uintptr) uintptr { return uintptr(unsafe.Pointer(&validityBuf[0])) }
	api.VectorEnsureValidityWritable = func(vec uintptr) {}

	db, err := lib.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Validation guards.
	if err := conn.RegisterTableFunction(TableFunctionSpec{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty name err = %v", err)
	}
	if err := conn.RegisterTableFunction(TableFunctionSpec{Name: "t"}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("missing Bind err = %v", err)
	}
	if err := conn.RegisterTableFunction(TableFunctionSpec{
		Name: "t",
		Bind: func(TableFunctionBindInput) (TableFunctionBindResult, error) { return TableFunctionBindResult{}, nil },
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("missing Execute err = %v", err)
	}

	// Failed registration must not leak extra_info handle.
	failRegister = true
	if err := conn.RegisterTableFunction(TableFunctionSpec{
		Name:    "fail_tf",
		Bind:    func(TableFunctionBindInput) (TableFunctionBindResult, error) { return TableFunctionBindResult{}, nil },
		Execute: func(any, any, any, *TableFunctionChunk) error { return nil },
	}); err == nil {
		t.Fatal("expected registration error")
	}
	if got := abi.HookHandleCount(); got != baseHooks {
		t.Fatalf("HookHandleCount after failed register = %d, want %d", got, baseHooks)
	}
	failRegister = false

	// Register a full TableFunction with projection pushdown + LocalInit.
	var gotLocal any
	var sawProjected []uint64
	err = conn.RegisterTableFunction(TableFunctionSpec{
		Name:               "mock_gen",
		Parameters:         []LogicalType{AnyType()},
		NamedParameters:    map[string]LogicalType{"step": ScalarType(TypeBigint)},
		ProjectionPushdown: true,
		Bind: func(in TableFunctionBindInput) (TableFunctionBindResult, error) {
			n, _ := in.Args[0].Int64()
			step, _ := in.NamedArgs["step"].Int64()
			return TableFunctionBindResult{
				Columns: []Column{
					{Name: "a", Type: ScalarType(TypeBigint)},
					{Name: "b", Type: ScalarType(TypeBigint)},
				},
				Cardinality: uint64(n),
				ExactCard:   true,
				BindData:    [2]int64{n, step},
			}, nil
		},
		Init: func(in TableFunctionInitInput) (TableFunctionInitResult, error) {
			sawProjected = in.ProjectedColumns
			return TableFunctionInitResult{InitData: "global_state", MaxThreads: 4}, nil
		},
		LocalInit: func(in TableFunctionInitInput) (TableFunctionLocalInitResult, error) {
			return TableFunctionLocalInitResult{LocalState: "worker_local"}, nil
		},
		Execute: func(bindData, initData, localData any, chunk *TableFunctionChunk) error {
			gotLocal = localData
			params := bindData.([2]int64)
			if len(chunk.Columns()) == 1 && chunk.Columns()[0].Type.Kind == TypeInvalid {
				// Virtual COUNT(*) projection path.
				return chunk.SetSize(int(params[0]))
			}
			// Physical projection path: SetSize growing beyond Len() must fail.
			if err := chunk.SetSize(5); !errors.Is(err, ErrInvalidInput) {
				return errors.New("expected SetSize to reject growing physical chunk")
			}
			for i := int64(0); i < params[0]; i++ {
				if err := chunk.AppendRow(Int64Value(i * params[1])); err != nil {
					return err
				}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("RegisterTableFunction: %v", err)
	}
	if !localInitSet.Load() {
		t.Fatal("expected LocalInit callback to be registered")
	}

	// Drive Bind -> Init -> LocalInit -> Execute via globalTFDispatcher.
	globalTFDispatcher.Bind(1)
	if bindErr != "" {
		t.Fatalf("Bind error: %s", bindErr)
	}
	if len(addedCols) != 2 || addedCols[0] != "a" || addedCols[1] != "b" || cardinality != 3 || !exactCard {
		t.Fatalf("Bind output mismatch: cols=%v card=%d exact=%v", addedCols, cardinality, exactCard)
	}

	globalTFDispatcher.Init(1)
	if initErr != "" || maxThreads != 4 || len(sawProjected) != 1 || sawProjected[0] != 1 {
		t.Fatalf("Init mismatch: err=%q maxThreads=%d proj=%v", initErr, maxThreads, sawProjected)
	}
	globalTFDispatcher.LocalInit(2)
	if initErr != "" || localID == 0 {
		t.Fatalf("LocalInit mismatch: err=%q localID=%d", initErr, localID)
	}

	globalTFDispatcher.Execute(1, 99)
	if funcErr != "" || chunkSize != 3 || gotLocal != "worker_local" {
		t.Fatalf("Execute mismatch: err=%q chunkSize=%d local=%v", funcErr, chunkSize, gotLocal)
	}
	if vecBuf[0] != 0 || vecBuf[1] != 10 || vecBuf[2] != 20 {
		t.Fatalf("vecBuf = %v, want [0 10 20 ...]", vecBuf[:3])
	}

	// Test virtual COUNT(*) projection (math.MaxUint64).
	abi.ReleaseHookHandle(initID)
	projIndices = []uint64{math.MaxUint64}
	globalTFDispatcher.Init(1)
	if initErr != "" {
		t.Fatalf("virtual Init error: %s", initErr)
	}
	chunkSize = 0
	globalTFDispatcher.Execute(1, 99)
	if funcErr != "" || chunkSize != 3 {
		t.Fatalf("virtual Execute mismatch: err=%q chunkSize=%d", funcErr, chunkSize)
	}

	// Simulate DuckDB delete callbacks releasing local, init, bind, and def handles.
	abi.ReleaseHookHandle(localID)
	abi.ReleaseHookHandle(initID)
	abi.ReleaseHookHandle(bindID)
	abi.ReleaseHookHandle(extraID)

	if got := abi.HookHandleCount(); got != baseHooks {
		t.Fatalf("HookHandleCount after teardown = %d, want %d", got, baseHooks)
	}
	if len(liveTFs) != 0 {
		t.Fatalf("leaked %d table function definitions", len(liveTFs))
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	f.assertZeroLeaks(t)
}
