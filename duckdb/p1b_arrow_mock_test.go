package duckdb

import (
	"context"
	"errors"
	"testing"

	"github.com/bububa/duckdbspike/internal/abi"
)

func TestArrowRowsMockLifecycleReleaseDetachAndZeroLeaks(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	api := lib.api

	arrowOpts := map[uintptr]struct{}{}
	errDatas := map[uintptr]string{}
	var chunkToArrowErr string

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
	api.ConnectionGetArrowOptions = func(conn uintptr, out *uintptr) {
		f.mu.Lock()
		defer f.mu.Unlock()
		id := f.allocID()
		arrowOpts[id] = struct{}{}
		*out = id
	}
	api.DestroyArrowOptions = func(opts *uintptr) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if opts == nil || *opts == 0 {
			return
		}
		if _, ok := arrowOpts[*opts]; !ok {
			f.doubleFree++
		}
		delete(arrowOpts, *opts)
		*opts = 0
	}
	api.ToArrowSchema = func(opts uintptr, types, names *uintptr, count uint64, out *abi.ArrowSchema) uintptr {
		f.mu.Lock()
		defer f.mu.Unlock()
		out.Format = f.cstr("+s")
		out.NChildren = int64(count)
		return 0
	}
	api.DataChunkToArrow = func(opts uintptr, chunk uintptr, out *abi.ArrowArray) uintptr {
		f.mu.Lock()
		defer f.mu.Unlock()
		if chunkToArrowErr != "" {
			id := f.allocID()
			errDatas[id] = chunkToArrowErr
			return id
		}
		out.Length = int64(f.chunks[chunk])
		return 0
	}

	db, _ := lib.Open(ctx, ":memory:")
	conn, _ := db.Connect(ctx)

	// Non-streaming result rejected cleanly.
	f.nonStreamingResult = true
	if _, err := conn.QueryArrow(ctx, "SELECT 1"); !errors.Is(err, ErrNotStreaming) {
		t.Fatalf("non-streaming QueryArrow = %v, want ErrNotStreaming", err)
	}
	f.nonStreamingResult = false

	// Streaming ArrowRows with Stmt lease, Schema/Array release & detach, MaxRows.
	f.resultColumns = []string{"id"}
	f.chunkSizes = []uint64{0, 3, 4}
	_ = conn.SetResultLimits(ResultLimits{MaxRows: 5})

	stmt, err := conn.Prepare(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	ar, err := stmt.QueryArrow(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Busy checks while ArrowRows is open.
	if err := stmt.Close(); !errors.Is(err, ErrBusy) {
		t.Fatalf("Stmt.Close while ArrowRows open = %v, want ErrBusy", err)
	}
	if _, err := conn.Exec(ctx, "SELECT 1"); !errors.Is(err, ErrBusy) {
		t.Fatalf("Conn.Exec while ArrowRows open = %v, want ErrBusy", err)
	}

	var releaseCount int
	schema, err := ar.Schema()
	if err != nil {
		t.Fatal(err)
	}
	schema.release = func(*abi.ArrowSchema) { releaseCount++ }
	if schema.View().Format() != "+s" || schema.View().ChildCount() != 1 {
		t.Fatalf("unexpected schema view: format=%q children=%d", schema.View().Format(), schema.View().ChildCount())
	}
	schema.Release()
	schema.Release() // idempotent
	if releaseCount != 1 || !schema.Released() {
		t.Fatalf("schema releaseCount=%d released=%v", releaseCount, schema.Released())
	}

	// First non-empty chunk (3 rows) succeeds; Detach prevents Release callback.
	arr1, err := ar.Next()
	if err != nil || arr1 == nil || arr1.View().Length() != 3 {
		t.Fatalf("arr1 = (%+v, %v)", arr1, err)
	}
	var arrReleaseCount int
	arr1.release = func(*abi.ArrowArray) { arrReleaseCount++ }
	if ptr := arr1.Detach(); ptr == nil {
		t.Fatal("Detach returned nil")
	}
	arr1.Release()
	if arrReleaseCount != 0 || !arr1.Released() {
		t.Fatalf("Detach failed to prevent release: count=%d", arrReleaseCount)
	}

	// Second chunk (4 rows) exceeds MaxRows=5 -> ErrBudgetExceeded.
	if _, err := ar.Next(); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("second chunk = %v, want ErrBudgetExceeded", err)
	}
	_ = ar.Close()
	_ = stmt.Close()

	// DataChunkToArrow error frees ErrorData and destroys chunk.
	_ = conn.SetResultLimits(ResultLimits{})
	f.chunkSizes = []uint64{2}
	chunkToArrowErr = "arrow conversion failed"
	ar2, err := conn.QueryArrow(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	var derr *Error
	if _, err := ar2.Next(); !errors.As(err, &derr) || derr.Message != "arrow conversion failed" {
		t.Fatalf("DataChunkToArrow err = %v", err)
	}
	_ = ar2.Close()

	if len(arrowOpts) != 0 || len(errDatas) != 0 {
		t.Fatalf("leaked arrowOpts=%d errDatas=%d", len(arrowOpts), len(errDatas))
	}
	_ = db.Close()
	f.assertZeroLeaks(t)
}
