package duckdb

import (
	"context"
	"errors"
	"testing"

	"github.com/bububa/duckdbspike/internal/abi"
)

func TestInstanceCacheMockLifecycleAndZeroLeaks(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	api := lib.api

	caches := map[uintptr]struct{}{}
	var failCreate bool
	var failGetMsg string

	api.CreateInstanceCache = func() uintptr {
		f.mu.Lock()
		defer f.mu.Unlock()
		if failCreate {
			return 0
		}
		id := f.allocID()
		caches[id] = struct{}{}
		return id
	}
	api.GetOrCreateFromCache = func(cache uintptr, path *byte, out *uintptr, config uintptr, outErr *uintptr) int32 {
		f.mu.Lock()
		defer f.mu.Unlock()
		if _, ok := caches[cache]; !ok {
			return abi.StateError
		}
		if failGetMsg != "" {
			if outErr != nil {
				*outErr = f.ownedCstr(failGetMsg)
			}
			id := f.allocID()
			f.dbs[id] = struct{}{}
			*out = id
			return abi.StateError
		}
		id := f.allocID()
		f.dbs[id] = struct{}{}
		*out = id
		return abi.StateSuccess
	}
	api.DestroyInstanceCache = func(cache *uintptr) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if cache == nil || *cache == 0 {
			return
		}
		if _, ok := caches[*cache]; !ok {
			f.doubleFree++
		}
		delete(caches, *cache)
		*cache = 0
	}

	failCreate = true
	if _, err := lib.NewInstanceCache(); err == nil {
		t.Fatal("expected NewInstanceCache error")
	}
	failCreate = false

	ic, err := lib.NewInstanceCache()
	if err != nil {
		t.Fatal(err)
	}

	// Open failure frees outError and allocated DB handle.
	failGetMsg = "cache open failed"
	if _, err := ic.Open(ctx, ":memory:"); err == nil {
		t.Fatal("expected cache Open error")
	}
	failGetMsg = ""

	// Open through BudgetController.OpenCached and direct ic.Open.
	bc, err := NewBudgetController(GlobalBudget{
		MaxConcurrent:    2,
		TotalMemoryBytes: 256 << 20,
		TotalThreads:     4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()

	db1, err := bc.OpenCached(ctx, ic, "/tmp/shared.duckdb", InstanceConfig{
		NativeMemoryBytes: 64 << 20,
		Threads:           1,
	})
	if err != nil {
		t.Fatal(err)
	}
	db2, err := ic.Open(ctx, "/tmp/shared.duckdb")
	if err != nil {
		t.Fatal(err)
	}
	if bc.Snapshot().ActiveInstances != 1 {
		t.Fatalf("expected 1 budgeted instance, got %+v", bc.Snapshot())
	}

	// Closing InstanceCache cascades to db1 & db2, releasing budget reservation too.
	if err := ic.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ic.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := ic.Open(ctx, ":memory:"); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close Open = %v, want ErrClosed", err)
	}
	if bc.Snapshot().ActiveInstances != 0 {
		t.Fatalf("budget reservation leaked: %+v", bc.Snapshot())
	}
	if len(caches) != 0 {
		t.Fatalf("instance cache leaked: %d", len(caches))
	}
	_ = db1.Close()
	_ = db2.Close()
	f.assertZeroLeaks(t)
}

func TestTxMockStateMachineAndBusyGuards(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	db, _ := lib.Open(ctx, ":memory:")
	conn, _ := db.Connect(ctx)

	tx, err := conn.BeginTx(ctx, TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := conn.BeginTx(ctx, TxOptions{}); !errors.Is(err, ErrTxActive) {
		t.Fatalf("double BeginTx = %v, want ErrTxActive", err)
	}

	// Open Rows blocks Commit/Rollback with ErrBusy.
	f.resultColumns = []string{"x"}
	rows, err := tx.Query(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); !errors.Is(err, ErrBusy) {
		t.Fatalf("Commit while Rows open = %v, want ErrBusy", err)
	}
	_ = rows.Close()

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// Post-commit calls return ErrTxDone.
	if err := tx.Commit(ctx); !errors.Is(err, ErrTxDone) {
		t.Fatalf("second Commit = %v, want ErrTxDone", err)
	}
	if err := tx.Rollback(ctx); !errors.Is(err, ErrTxDone) {
		t.Fatalf("Rollback after Commit = %v, want ErrTxDone", err)
	}
	if _, err := tx.Exec(ctx, "SELECT 1"); !errors.Is(err, ErrTxDone) {
		t.Fatalf("Exec after Commit = %v, want ErrTxDone", err)
	}

	// New Tx invalidated cleanly on Conn.Close.
	tx2, err := conn.BeginTx(ctx, TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if err := tx2.Rollback(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("Rollback after Conn.Close = %v, want ErrClosed", err)
	}
	_ = db.Close()
	f.assertZeroLeaks(t)
}

func TestScriptMockFailuresAndZeroLeaks(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	api := lib.api

	extracted := map[uintptr]string{}
	var extractCount uint64
	var extractErr string
	var failPrepAt int = -1

	api.ExtractStatements = func(conn uintptr, q *byte, out *uintptr) uint64 {
		f.mu.Lock()
		defer f.mu.Unlock()
		id := f.allocID()
		extracted[id] = extractErr
		*out = id
		return extractCount
	}
	api.ExtractStatementsError = func(ex uintptr) uintptr {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.cstr(extracted[ex])
	}
	api.DestroyExtracted = func(ex *uintptr) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if ex == nil || *ex == 0 {
			return
		}
		if _, ok := extracted[*ex]; !ok {
			f.doubleFree++
		}
		delete(extracted, *ex)
		*ex = 0
	}
	api.PrepareExtractedStatement = func(conn, ex uintptr, idx uint64, out *uintptr) int32 {
		f.mu.Lock()
		defer f.mu.Unlock()
		id := f.allocID()
		if failPrepAt >= 0 && int(idx) == failPrepAt {
			f.stmts[id] = &fakeStmt{errMsg: "bad extracted stmt"}
			*out = id
			return abi.StateError
		}
		f.stmts[id] = &fakeStmt{}
		*out = id
		return abi.StateSuccess
	}

	db, _ := lib.Open(ctx, ":memory:")
	conn, _ := db.Connect(ctx)

	// Empty script: ExecScript -> (0, nil), QueryScript -> ErrInvalidInput.
	extractCount = 0
	extractErr = ""
	if res, err := conn.ExecScript(ctx, "-- comment only"); err != nil || res.RowsAffected != 0 {
		t.Fatalf("empty ExecScript = (%+v, %v)", res, err)
	}
	if _, err := conn.QueryScript(ctx, ""); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty QueryScript = %v, want ErrInvalidInput", err)
	}

	// Syntax error during extract destroys extracted handle.
	extractErr = "parser error"
	if _, err := conn.ExecScript(ctx, "SELECT ???"); err == nil {
		t.Fatal("expected extract error")
	}
	extractErr = ""

	// Mid-script prepare failure destroys failed stmt and extracted handle.
	extractCount = 3
	failPrepAt = 1
	if _, err := conn.ExecScript(ctx, "S0; S1; S2"); err == nil {
		t.Fatal("expected prepare_extracted error")
	}
	failPrepAt = -1

	// Successful 3-statement ExecScript sums RowsAffected.
	f.resultRowsAffected = 2
	res, err := conn.ExecScript(ctx, "S0; S1; S2")
	if err != nil || res.RowsAffected != 6 {
		t.Fatalf("ExecScript = (%+v, %v), want RowsAffected=6", res, err)
	}

	// Successful QueryScript streams last statement.
	f.resultColumns = []string{"out"}
	rows, err := conn.QueryScript(ctx, "S0; S1; SELECT 1")
	if err != nil {
		t.Fatalf("QueryScript: %v", err)
	}
	_ = rows.Close()

	if len(extracted) != 0 {
		t.Fatalf("extracted handle leaked: %d", len(extracted))
	}
	_ = db.Close()
	f.assertZeroLeaks(t)
}

func TestAppenderMockLeaseErrorsAndZeroLeaks(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	api := lib.api

	appenders := map[uintptr]string{}
	errDatas := map[uintptr]string{}
	var failCreate bool
	var failAppendVal bool
	var failClose bool

	api.ErrorDataHasError = func(ed uintptr) bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return errDatas[ed] != ""
	}
	api.ErrorDataErrorType = func(ed uintptr) int32 { return abi.ErrorInvalid }
	api.ErrorDataMessage = func(ed uintptr) uintptr {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.cstr(errDatas[ed])
	}
	api.DestroyErrorData = func(ed *uintptr) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if ed == nil || *ed == 0 {
			return
		}
		if _, ok := errDatas[*ed]; !ok {
			f.doubleFree++
		}
		delete(errDatas, *ed)
		*ed = 0
	}
	api.AppenderCreateExt = func(conn uintptr, cat, schema, table *byte, out *uintptr) int32 {
		f.mu.Lock()
		defer f.mu.Unlock()
		id := f.allocID()
		*out = id
		if failCreate {
			appenders[id] = "table does not exist"
			return abi.StateError
		}
		appenders[id] = ""
		return abi.StateSuccess
	}
	api.AppenderErrorData = func(app uintptr) uintptr {
		f.mu.Lock()
		defer f.mu.Unlock()
		id := f.allocID()
		errDatas[id] = appenders[app]
		return id
	}
	api.AppenderColumnCount = func(app uintptr) uint64 { return 2 }
	api.AppenderColumnType = func(app uintptr, col uint64) uintptr {
		f.mu.Lock()
		defer f.mu.Unlock()
		id := f.allocID()
		if col == 0 {
			f.ltypes[id] = abi.TypeBigint
		} else {
			f.ltypes[id] = abi.TypeUUID
		}
		return id
	}
	api.AppendInt64 = func(app uintptr, v int64) int32 { return abi.StateSuccess }
	api.AppendDefault = func(app uintptr) int32 { return abi.StateSuccess }
	api.AppendNull = func(app uintptr) int32 { return abi.StateSuccess }
	api.CreateUUID = func(u abi.Uhugeint) uintptr {
		f.mu.Lock()
		defer f.mu.Unlock()
		id := f.allocID()
		f.values[id] = struct{}{}
		return id
	}
	api.DestroyValue = func(val *uintptr) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if val == nil || *val == 0 {
			return
		}
		if _, ok := f.values[*val]; !ok {
			f.doubleFree++
		}
		delete(f.values, *val)
		*val = 0
	}
	api.AppendValue = func(app uintptr, val uintptr) int32 {
		f.mu.Lock()
		defer f.mu.Unlock()
		if failAppendVal {
			appenders[app] = "constraint violation"
			return abi.StateError
		}
		return abi.StateSuccess
	}
	api.AppenderEndRow = func(app uintptr) int32 { return abi.StateSuccess }
	api.AppenderFlush = func(app uintptr) int32 { return abi.StateSuccess }
	api.AppenderClear = func(app uintptr) int32 { return abi.StateSuccess }
	api.AppenderClose = func(app uintptr) int32 {
		f.mu.Lock()
		defer f.mu.Unlock()
		if failClose {
			appenders[app] = "deferred flush constraint error"
			return abi.StateError
		}
		return abi.StateSuccess
	}
	api.AppenderDestroy = func(app *uintptr) int32 {
		f.mu.Lock()
		defer f.mu.Unlock()
		if app == nil || *app == 0 {
			return abi.StateSuccess
		}
		if _, ok := appenders[*app]; !ok {
			f.doubleFree++
		}
		delete(appenders, *app)
		*app = 0
		return abi.StateSuccess
	}

	db, _ := lib.Open(ctx, ":memory:")
	conn, _ := db.Connect(ctx)

	// DefaultValue rejected in regular SQL parameter binding.
	f.nParams = 1
	if _, err := conn.Query(ctx, "SELECT ?", DefaultValue()); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("DefaultValue in Query = %v, want ErrInvalidInput", err)
	}
	f.nParams = 0

	// Appender create failure surfaces ErrorData message and destroys appender.
	failCreate = true
	_, err := conn.NewAppender(ctx, "", "", "missing_tbl")
	var derr *Error
	if !errors.As(err, &derr) || derr.Message != "table does not exist" {
		t.Fatalf("create err = %v", err)
	}
	failCreate = false

	app, err := conn.NewAppender(ctx, "", "", "items")
	if err != nil {
		t.Fatal(err)
	}
	// Exclusive Conn lease blocks concurrent Query/Exec/BeginTx.
	if _, err := conn.Exec(ctx, "SELECT 1"); !errors.Is(err, ErrBusy) {
		t.Fatalf("Exec while Appender open = %v, want ErrBusy", err)
	}
	if _, err := conn.BeginTx(ctx, TxOptions{}); !errors.Is(err, ErrBusy) {
		t.Fatalf("BeginTx while Appender open = %v, want ErrBusy", err)
	}
	// Wrong arity rejected.
	if err := app.AppendRow(Int64Value(1)); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("wrong arity = %v, want ErrInvalidInput", err)
	}
	// AppendValue failure destroys temporary duckdb_value and ErrorData.
	failAppendVal = true
	if err := app.AppendRow(Int64Value(1), UUIDValue(UUID{})); err == nil {
		t.Fatal("expected AppendValue failure")
	}
	failAppendVal = false

	// Close surfaces deferred AppenderClose error, destroys handle, and releases Conn lease.
	failClose = true
	if err := app.Close(); !errors.As(err, &derr) || derr.Message != "deferred flush constraint error" {
		t.Fatalf("Close err = %v", err)
	}
	if err := app.Close(); err != nil {
		t.Fatalf("idempotent second Close = %v", err)
	}
	// Conn usable again after Appender.Close.
	if _, err := conn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatalf("Exec after Appender.Close: %v", err)
	}

	if len(appenders) != 0 || len(errDatas) != 0 {
		t.Fatalf("leaked appenders=%d errDatas=%d", len(appenders), len(errDatas))
	}
	_ = db.Close()
	f.assertZeroLeaks(t)
}
