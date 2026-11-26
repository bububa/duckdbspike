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

func TestScalarFunctionAndSetMockLifecycleBindInitExecuteAndZeroLeaks(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	api := lib.api
	baseHooks := abi.HookHandleCount()

	var (
		mu          sync.Mutex
		nextHandle  uintptr = 4000
		liveTypes           = make(map[uintptr]int32)
		liveSFs             = make(map[uintptr]bool)
		liveSets            = make(map[uintptr]bool)
		liveExprs           = make(map[uintptr]bool)
		liveCtxs            = make(map[uintptr]bool)
		liveVals            = make(map[uintptr]bool)
		extraID     uintptr
		bindID      uintptr
		initID      uintptr
		copyCB      uintptr
		bindErr     string
		initErr     string
		execErr     string
		failReg     bool
		inVals      = []int64{10, 20, 30}
		inValidity  = []uint64{0b101} // row 1 is NULL
		outVals     = make([]int64, 3)
		outValidity = []uint64{0b111}
	)

	alloc := func() uintptr {
		mu.Lock()
		defer mu.Unlock()
		h := nextHandle
		nextHandle++
		return h
	}

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

	api.CreateScalarFunction = func() uintptr {
		h := alloc()
		mu.Lock()
		liveSFs[h] = true
		mu.Unlock()
		return h
	}
	api.DestroyScalarFunction = func(sf *uintptr) {
		if sf == nil || *sf == 0 {
			return
		}
		mu.Lock()
		delete(liveSFs, *sf)
		mu.Unlock()
		*sf = 0
	}
	api.ScalarFunctionSetName = func(sf uintptr, name *byte) {}
	api.ScalarFunctionAddParameter = func(sf uintptr, lt uintptr) {}
	api.ScalarFunctionSetVarargs = func(sf uintptr, lt uintptr) {}
	api.ScalarFunctionSetReturnType = func(sf uintptr, lt uintptr) {}
	api.ScalarFunctionSetVolatile = func(sf uintptr) {}
	api.ScalarFunctionSetSpecialHandling = func(sf uintptr) {}
	api.ScalarFunctionSetExtraInfo = func(sf uintptr, extra uintptr, del uintptr) { extraID = extra }
	api.ScalarFunctionSetBind = func(sf uintptr, cb uintptr) {}
	api.ScalarFunctionSetInit = func(sf uintptr, cb uintptr) {}
	api.ScalarFunctionSetFunction = func(sf uintptr, cb uintptr) {}
	api.RegisterScalarFunction = func(conn uintptr, sf uintptr) int32 {
		if failReg {
			return abi.StateError
		}
		return abi.StateSuccess
	}
	api.CreateScalarFunctionSet = func(name *byte) uintptr {
		h := alloc()
		mu.Lock()
		liveSets[h] = true
		mu.Unlock()
		return h
	}
	api.DestroyScalarFunctionSet = func(set *uintptr) {
		if set == nil || *set == 0 {
			return
		}
		mu.Lock()
		delete(liveSets, *set)
		mu.Unlock()
		*set = 0
	}
	api.AddScalarFunctionToSet = func(set uintptr, sf uintptr) int32 { return abi.StateSuccess }
	api.RegisterScalarFunctionSet = func(conn uintptr, set uintptr) int32 {
		if failReg {
			return abi.StateError
		}
		return abi.StateSuccess
	}

	// Bind/Expression mocks
	api.ScalarFunctionBindGetExtraInfo = func(info uintptr) uintptr { return extraID }
	api.ScalarFunctionBindGetArgumentCount = func(info uintptr) uint64 { return 1 }
	api.ScalarFunctionBindGetArgument = func(info uintptr, idx uint64) uintptr {
		h := alloc()
		mu.Lock()
		liveExprs[h] = true
		mu.Unlock()
		return h
	}
	api.DestroyExpression = func(expr *uintptr) {
		if expr == nil || *expr == 0 {
			return
		}
		mu.Lock()
		delete(liveExprs, *expr)
		mu.Unlock()
		*expr = 0
	}
	api.ExpressionReturnType = func(expr uintptr) uintptr { return api.CreateLogicalType(abi.TypeBigint) }
	api.ExpressionIsFoldable = func(expr uintptr) bool { return true }
	api.ScalarFunctionGetClientContext = func(info uintptr, out *uintptr) {
		h := alloc()
		mu.Lock()
		liveCtxs[h] = true
		mu.Unlock()
		*out = h
	}
	api.DestroyClientContext = func(ctx *uintptr) {
		if ctx == nil || *ctx == 0 {
			return
		}
		mu.Lock()
		delete(liveCtxs, *ctx)
		mu.Unlock()
		*ctx = 0
	}
	api.ExpressionFold = func(clientCtx uintptr, expr uintptr, outVal *uintptr) uintptr {
		h := alloc()
		mu.Lock()
		liveVals[h] = true
		mu.Unlock()
		*outVal = h
		return 0
	}
	borrowedBigint := api.CreateLogicalType(abi.TypeBigint)
	defer api.DestroyLogicalType(&borrowedBigint)
	api.GetValueType = func(val uintptr) uintptr { return borrowedBigint }
	api.IsNullValue = func(val uintptr) bool { return false }
	api.GetInt64 = func(val uintptr) int64 { return 5 }
	api.DestroyValue = func(v *uintptr) {
		if v == nil || *v == 0 {
			return
		}
		mu.Lock()
		delete(liveVals, *v)
		mu.Unlock()
		*v = 0
	}
	api.ScalarFunctionSetBindData = func(info uintptr, id uintptr, del uintptr) { bindID = id }
	api.ScalarFunctionSetBindDataCopy = func(info uintptr, cb uintptr) { copyCB = cb }
	api.ScalarFunctionBindSetError = func(info uintptr, msg *byte) { bindErr = abi.GoString(uintptr(unsafe.Pointer(msg))) }

	// Init / Exec mocks
	api.ScalarFunctionInitGetExtraInfo = func(info uintptr) uintptr { return extraID }
	api.ScalarFunctionInitGetBindData = func(info uintptr) uintptr { return bindID }
	api.ScalarFunctionInitSetState = func(info uintptr, id uintptr, del uintptr) { initID = id }
	api.ScalarFunctionInitSetError = func(info uintptr, msg *byte) { initErr = abi.GoString(uintptr(unsafe.Pointer(msg))) }
	api.ScalarFunctionGetExtraInfo = func(info uintptr) uintptr { return extraID }
	api.ScalarFunctionGetBindData = func(info uintptr) uintptr { return bindID }
	api.ScalarFunctionGetState = func(info uintptr) uintptr { return initID }
	api.ScalarFunctionSetError = func(info uintptr, msg *byte) { execErr = abi.GoString(uintptr(unsafe.Pointer(msg))) }

	api.DataChunkGetSize = func(chunk uintptr) uint64 { return 3 }
	api.DataChunkGetColumnCount = func(chunk uintptr) uint64 { return 1 }
	api.DataChunkGetVector = func(chunk uintptr, col uint64) uintptr { return 701 }
	api.VectorGetData = func(vec uintptr) uintptr {
		if vec == 701 {
			return uintptr(unsafe.Pointer(&inVals[0]))
		}
		return uintptr(unsafe.Pointer(&outVals[0]))
	}
	api.VectorGetValidity = func(vec uintptr) uintptr {
		if vec == 701 {
			return uintptr(unsafe.Pointer(&inValidity[0]))
		}
		return uintptr(unsafe.Pointer(&outValidity[0]))
	}
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
	if err := conn.RegisterScalarFunction(ScalarFunctionSpec{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty spec err = %v", err)
	}
	if err := conn.RegisterScalarFunction(ScalarFunctionSpec{
		Name:       "bad_ret",
		ReturnType: AnyType(),
		Execute:    func(any, any, []Value) (Value, error) { return NullValue(), nil },
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("ANY return err = %v", err)
	}
	if err := conn.RegisterScalarFunction(ScalarFunctionSpec{
		Name:         "both_exec",
		ReturnType:   ScalarType(TypeBigint),
		Execute:      func(any, any, []Value) (Value, error) { return NullValue(), nil },
		ExecuteChunk: func(any, any, *ScalarFunctionChunk) error { return nil },
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("both Execute and ExecuteChunk err = %v", err)
	}
	if err := conn.RegisterScalarFunctionSet("empty_set"); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty set err = %v", err)
	}

	// Failed registration must not leak handles.
	failReg = true
	if err := conn.RegisterScalarFunction(ScalarFunctionSpec{
		Name:       "fail_sf",
		Parameters: []LogicalType{ScalarType(TypeBigint)},
		ReturnType: ScalarType(TypeBigint),
		Execute:    func(any, any, []Value) (Value, error) { return Int64Value(1), nil },
	}); err == nil {
		t.Fatal("expected RegisterScalarFunction failure")
	}
	if err := conn.RegisterScalarFunctionSet("fail_set", ScalarFunctionSpec{
		Parameters: []LogicalType{ScalarType(TypeBigint)},
		ReturnType: ScalarType(TypeBigint),
		Execute:    func(any, any, []Value) (Value, error) { return Int64Value(1), nil },
	}); err == nil {
		t.Fatal("expected RegisterScalarFunctionSet failure")
	}
	if got := abi.HookHandleCount(); got != baseHooks {
		t.Fatalf("HookHandleCount after failed register = %d, want %d", got, baseHooks)
	}
	failReg = false

	// Register a full scalar function with Bind + Init + Execute.
	var invokedRows int
	err = conn.RegisterScalarFunction(ScalarFunctionSpec{
		Name:       "mock_add_folded",
		Parameters: []LogicalType{ScalarType(TypeBigint)},
		ReturnType: ScalarType(TypeBigint),
		Bind: func(in ScalarFunctionBindInput) (any, error) {
			if len(in.Args) != 1 || !in.Args[0].IsFoldable {
				return nil, errors.New("expected foldable arg")
			}
			v, _ := in.Args[0].Value.Int64()
			return v, nil
		},
		Init: func(bindData any) (any, error) {
			return bindData.(int64) * 2, nil // 10
		},
		Execute: func(bindData, localState any, args []Value) (Value, error) {
			invokedRows++
			x, _ := args[0].Int64()
			return Int64Value(x + localState.(int64)), nil
		},
	})
	if err != nil {
		t.Fatalf("RegisterScalarFunction: %v", err)
	}

	globalSFDispatcher.Bind(1)
	if bindErr != "" || bindID == 0 || copyCB != abi.HookCopyCallback() {
		t.Fatalf("Bind mismatch: err=%q bindID=%d copyCB=%#x", bindErr, bindID, copyCB)
	}
	// Verify CloneHookHandle (used by HookCopyCallback) works and balances cleanly.
	clonedBindID := abi.CloneHookHandle(bindID)
	if clonedBindID == 0 {
		t.Fatal("CloneHookHandle failed")
	}
	abi.ReleaseHookHandle(clonedBindID)

	globalSFDispatcher.Init(1)
	if initErr != "" || initID == 0 {
		t.Fatalf("Init mismatch: err=%q initID=%d", initErr, initID)
	}

	globalSFDispatcher.Execute(1, 500, 702)
	if execErr != "" {
		t.Fatalf("Execute error: %s", execErr)
	}
	// Row 1 was NULL so Execute must only run for rows 0 and 2 (SpecialNulls == false).
	if invokedRows != 2 {
		t.Fatalf("invokedRows = %d, want 2 (NULL row skipped)", invokedRows)
	}
	if outVals[0] != 20 || outVals[2] != 40 {
		t.Fatalf("outVals = %v, want [20, ?, 40]", outVals)
	}
	if outValidity[0]&(1<<1) != 0 {
		t.Fatalf("expected row 1 output validity bit to be cleared (NULL), got %#b", outValidity[0])
	}

	// Simulate delete callbacks releasing init, bind, and extra handles.
	abi.ReleaseHookHandle(initID)
	abi.ReleaseHookHandle(bindID)
	abi.ReleaseHookHandle(extraID)

	// Verify ExecuteChunk path + panic recovery.
	err = conn.RegisterScalarFunction(ScalarFunctionSpec{
		Name:       "mock_chunk_panic",
		Parameters: []LogicalType{ScalarType(TypeBigint)},
		ReturnType: ScalarType(TypeBigint),
		ExecuteChunk: func(_, _ any, chunk *ScalarFunctionChunk) error {
			if chunk.Len() != 3 || chunk.ArgCount() != 1 {
				return errors.New("bad chunk dims")
			}
			v0, err := chunk.Arg(0, 0)
			if err != nil {
				return err
			}
			if n, _ := v0.Int64(); n != 10 {
				return errors.New("bad arg(0,0)")
			}
			panic("chunk boom")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	execErr = ""
	globalSFDispatcher.Execute(1, 500, 702)
	if !strings.Contains(execErr, "chunk boom") {
		t.Fatalf("expected panic error, got %q", execErr)
	}
	abi.ReleaseHookHandle(extraID)

	if got := abi.HookHandleCount(); got != baseHooks {
		t.Fatalf("HookHandleCount = %d, want %d", got, baseHooks)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if len(liveSFs) != 0 || len(liveSets) != 0 || len(liveExprs) != 0 || len(liveCtxs) != 0 || len(liveVals) != 0 {
		t.Fatalf("leaked native handles: sf=%d set=%d expr=%d ctx=%d val=%d",
			len(liveSFs), len(liveSets), len(liveExprs), len(liveCtxs), len(liveVals))
	}
	f.assertZeroLeaks(t)
}

func TestAggregateFunctionMockStateOffsetCombineDestroyAndZeroLeaks(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	api := lib.api
	baseHooks := abi.HookHandleCount()

	var (
		mu          sync.Mutex
		nextHandle  uintptr = 5000
		liveTypes           = make(map[uintptr]int32)
		liveAFs             = make(map[uintptr]bool)
		extraID     uintptr
		aggErr      string
		inVals      = []int64{4, 99, 6}
		inValidity  = []uint64{0b101} // row 1 NULL
		outVals     = make([]int64, 5)
		outValidity = []uint64{0b11111}
	)

	alloc := func() uintptr {
		mu.Lock()
		defer mu.Unlock()
		h := nextHandle
		nextHandle++
		return h
	}

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
	api.CreateAggregateFunction = func() uintptr {
		h := alloc()
		mu.Lock()
		liveAFs[h] = true
		mu.Unlock()
		return h
	}
	api.DestroyAggregateFunction = func(af *uintptr) {
		if af == nil || *af == 0 {
			return
		}
		mu.Lock()
		delete(liveAFs, *af)
		mu.Unlock()
		*af = 0
	}
	api.AggregateFunctionSetName = func(af uintptr, name *byte) {}
	api.AggregateFunctionAddParameter = func(af uintptr, lt uintptr) {}
	api.AggregateFunctionSetReturnType = func(af uintptr, lt uintptr) {}
	api.AggregateFunctionSetSpecialHandling = func(af uintptr) {}
	api.AggregateFunctionSetExtraInfo = func(af uintptr, extra uintptr, del uintptr) { extraID = extra }
	api.AggregateFunctionSetFunctions = func(af uintptr, sz, initCB, upd, comb, fin uintptr) {}
	api.AggregateFunctionSetDestructor = func(af uintptr, del uintptr) {}
	api.RegisterAggregateFunction = func(conn uintptr, af uintptr) int32 { return abi.StateSuccess }
	api.AggregateFunctionGetExtraInfo = func(info uintptr) uintptr { return extraID }
	api.AggregateFunctionSetError = func(info uintptr, msg *byte) {
		aggErr = abi.GoString(uintptr(unsafe.Pointer(msg)))
	}
	api.DataChunkGetSize = func(chunk uintptr) uint64 { return 3 }
	api.DataChunkGetColumnCount = func(chunk uintptr) uint64 { return 1 }
	api.DataChunkGetVector = func(chunk uintptr, col uint64) uintptr { return 801 }
	api.VectorGetData = func(vec uintptr) uintptr {
		if vec == 801 {
			return uintptr(unsafe.Pointer(&inVals[0]))
		}
		return uintptr(unsafe.Pointer(&outVals[0]))
	}
	api.VectorGetValidity = func(vec uintptr) uintptr {
		if vec == 801 {
			return uintptr(unsafe.Pointer(&inValidity[0]))
		}
		return uintptr(unsafe.Pointer(&outValidity[0]))
	}
	api.VectorEnsureValidityWritable = func(vec uintptr) {}

	db, err := lib.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if err := conn.RegisterAggregateFunction(AggregateFunctionSpec{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty agg spec err = %v", err)
	}
	if err := conn.RegisterAggregateFunctionSet("empty_agg"); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty agg set err = %v", err)
	}

	type sumState struct{ total int64 }
	err = conn.RegisterAggregateFunction(AggregateFunctionSpec{
		Name:       "mock_sum",
		Parameters: []LogicalType{ScalarType(TypeBigint)},
		ReturnType: ScalarType(TypeBigint),
		Init:       func() any { return &sumState{} },
		Update: func(st any, args []Value) error {
			v, _ := args[0].Int64()
			st.(*sumState).total += v
			return nil
		},
		Combine: func(src, dst any) error {
			dst.(*sumState).total += src.(*sumState).total
			return nil
		},
		Finalize: func(st any) (Value, error) {
			return Int64Value(st.(*sumState).total), nil
		},
	})
	if err != nil {
		t.Fatalf("RegisterAggregateFunction: %v", err)
	}

	if sz := globalAFDispatcher.StateSize(1); sz != uint64(unsafe.Sizeof(uintptr(0))) {
		t.Fatalf("StateSize = %d, want %d", sz, unsafe.Sizeof(uintptr(0)))
	}

	// Simulate two 8-byte C++ hash-table state slots.
	var slotA, slotB uintptr
	globalAFDispatcher.Init(1, uintptr(unsafe.Pointer(&slotA)))
	globalAFDispatcher.Init(1, uintptr(unsafe.Pointer(&slotB)))
	if aggErr != "" || slotA == 0 || slotB == 0 {
		t.Fatalf("Init mismatch: err=%q slotA=%d slotB=%d", aggErr, slotA, slotB)
	}

	// Update rows 0,1,2 targeting slotA, slotA, slotB (row 1 is NULL -> skipped).
	updStates := []uintptr{
		uintptr(unsafe.Pointer(&slotA)),
		uintptr(unsafe.Pointer(&slotA)),
		uintptr(unsafe.Pointer(&slotB)),
	}
	globalAFDispatcher.Update(1, 600, uintptr(unsafe.Pointer(&updStates[0])))
	if aggErr != "" {
		t.Fatalf("Update error: %s", aggErr)
	}

	// Combine slotB (6) into slotA (4) => slotA = 10.
	srcArr := []uintptr{uintptr(unsafe.Pointer(&slotB))}
	dstArr := []uintptr{uintptr(unsafe.Pointer(&slotA))}
	globalAFDispatcher.Combine(1, uintptr(unsafe.Pointer(&srcArr[0])), uintptr(unsafe.Pointer(&dstArr[0])), 1)
	if aggErr != "" {
		t.Fatalf("Combine error: %s", aggErr)
	}

	// Finalize slotA at non-zero offset = 2!
	finArr := []uintptr{uintptr(unsafe.Pointer(&slotA))}
	globalAFDispatcher.Finalize(1, uintptr(unsafe.Pointer(&finArr[0])), 802, 1, 2)
	if aggErr != "" {
		t.Fatalf("Finalize error: %s", aggErr)
	}
	if outVals[2] != 10 {
		t.Fatalf("outVals[2] = %d, want 10 (respecting offset=2)", outVals[2])
	}

	// Destroy both state slots via AggregateDestroyCallback dispatcher.
	destroyArr := []uintptr{
		uintptr(unsafe.Pointer(&slotA)),
		uintptr(unsafe.Pointer(&slotB)),
	}
	globalAFDispatcher.Destroy(uintptr(unsafe.Pointer(&destroyArr[0])), 2)
	if slotA != 0 || slotB != 0 {
		t.Fatalf("Destroy did not zero slots: slotA=%d slotB=%d", slotA, slotB)
	}

	abi.ReleaseHookHandle(extraID)
	if got := abi.HookHandleCount(); got != baseHooks {
		t.Fatalf("HookHandleCount = %d, want %d", got, baseHooks)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if len(liveAFs) != 0 || len(liveTypes) != 0 {
		t.Fatalf("leaked native handles: af=%d types=%d", len(liveAFs), len(liveTypes))
	}
	f.assertZeroLeaks(t)
}

func TestCastFunctionMockNormalTryAndZeroLeaks(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	api := lib.api
	baseHooks := abi.HookHandleCount()

	var (
		mu           sync.Mutex
		nextHandle   uintptr = 6000
		liveTypes            = make(map[uintptr]int32)
		liveCFs              = make(map[uintptr]bool)
		extraID      uintptr
		castMode     = abi.CastModeNormal
		lastErr      string
		rowErrRow    = ^uint64(0)
		rowErrMsg    string
		implicitCost int64 = -999
		inVals             = []int64{10, -5, 20}
		inValidity         = []uint64{0b111}
		outVals            = make([]int64, 3)
		outValidity        = []uint64{0b111}
	)

	alloc := func() uintptr {
		mu.Lock()
		defer mu.Unlock()
		h := nextHandle
		nextHandle++
		return h
	}

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
	api.CreateCastFunction = func() uintptr {
		h := alloc()
		mu.Lock()
		liveCFs[h] = true
		mu.Unlock()
		return h
	}
	api.DestroyCastFunction = func(cf *uintptr) {
		if cf == nil || *cf == 0 {
			return
		}
		mu.Lock()
		delete(liveCFs, *cf)
		mu.Unlock()
		*cf = 0
	}
	api.CastFunctionSetSourceType = func(cf uintptr, lt uintptr) {}
	api.CastFunctionSetTargetType = func(cf uintptr, lt uintptr) {}
	api.CastFunctionSetImplicitCastCost = func(cf uintptr, cost int64) { implicitCost = cost }
	api.CastFunctionSetExtraInfo = func(cf uintptr, extra uintptr, del uintptr) { extraID = extra }
	api.CastFunctionSetFunction = func(cf uintptr, cb uintptr) {}
	api.RegisterCastFunction = func(conn uintptr, cf uintptr) int32 { return abi.StateSuccess }
	api.CastFunctionGetExtraInfo = func(info uintptr) uintptr { return extraID }
	api.CastFunctionGetCastMode = func(info uintptr) int32 { return castMode }
	api.CastFunctionSetError = func(info uintptr, msg *byte) {
		lastErr = abi.GoString(uintptr(unsafe.Pointer(msg)))
	}
	api.CastFunctionSetRowError = func(info uintptr, msg *byte, row uint64, output uintptr) {
		rowErrRow = row
		rowErrMsg = abi.GoString(uintptr(unsafe.Pointer(msg)))
		outValidity[0] &^= uint64(1) << row
	}
	api.VectorGetData = func(vec uintptr) uintptr {
		if vec == 901 {
			return uintptr(unsafe.Pointer(&inVals[0]))
		}
		return uintptr(unsafe.Pointer(&outVals[0]))
	}
	api.VectorGetValidity = func(vec uintptr) uintptr {
		if vec == 901 {
			return uintptr(unsafe.Pointer(&inValidity[0]))
		}
		return uintptr(unsafe.Pointer(&outValidity[0]))
	}
	api.VectorEnsureValidityWritable = func(vec uintptr) {}

	db, err := lib.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if CastModeNormal.String() != "NORMAL" || CastModeTry.String() != "TRY" {
		t.Fatalf("unexpected CastMode strings: %q, %q", CastModeNormal, CastModeTry)
	}
	if err := conn.RegisterCastFunction(CastFunctionSpec{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty cast spec err = %v", err)
	}

	cost := int64(10)
	err = conn.RegisterCastFunction(CastFunctionSpec{
		SourceType:       ScalarType(TypeBigint),
		TargetType:       ScalarType(TypeBigint),
		ImplicitCastCost: &cost,
		Cast: func(mode CastMode, val Value) (Value, error) {
			n, _ := val.Int64()
			if n < 0 {
				return Value{}, errors.New("negative not allowed")
			}
			return Int64Value(n * 2), nil
		},
	})
	if err != nil {
		t.Fatalf("RegisterCastFunction: %v", err)
	}
	if implicitCost != 10 {
		t.Fatalf("implicitCost = %d, want 10", implicitCost)
	}

	// Normal mode aborts on row 1 (-5).
	castMode = abi.CastModeNormal
	ok := globalCFDispatcher.Execute(1, 3, 901, 902)
	if ok || !strings.Contains(lastErr, "negative not allowed") {
		t.Fatalf("normal cast = (%v, %q), want (false, negative not allowed)", ok, lastErr)
	}

	// TRY_CAST sets row error on row 1 and still casts row 2 (20 -> 40).
	castMode = abi.CastModeTry
	outVals = []int64{0, 0, 0}
	outValidity[0] = 0b111
	ok = globalCFDispatcher.Execute(1, 3, 901, 902)
	if ok || rowErrRow != 1 || !strings.Contains(rowErrMsg, "negative not allowed") {
		t.Fatalf("try cast = (%v, row=%d, msg=%q)", ok, rowErrRow, rowErrMsg)
	}
	if outVals[0] != 20 || outVals[2] != 40 || (outValidity[0]&(1<<1)) != 0 {
		t.Fatalf("try cast outputs = %v validity=%#b", outVals, outValidity[0])
	}

	abi.ReleaseHookHandle(extraID)
	if got := abi.HookHandleCount(); got != baseHooks {
		t.Fatalf("HookHandleCount = %d, want %d", got, baseHooks)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if len(liveCFs) != 0 || len(liveTypes) != 0 {
		t.Fatalf("leaked native handles: cf=%d types=%d", len(liveCFs), len(liveTypes))
	}
	f.assertZeroLeaks(t)
}
