package duckdb

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bububa/duckdbspike/internal/abi"
)

func TestCancelReassertSurvivesNativeInterruptReset(t *testing.T) {
	f, lib := newFakeNative(t)
	api := lib.api
	db, _ := lib.Open(context.Background(), ":memory:")
	conn, _ := db.Connect(context.Background())
	conn.interruptInterval = 200 * time.Microsecond

	ctx, cancel := context.WithCancel(context.Background())
	var interrupted atomic.Bool
	var resets atomic.Int32
	enterTask := make(chan struct{}, 1)
	unblockPrepare := make(chan struct{})

	f.onInterrupt = func(uintptr) {
		interrupted.Store(true)
	}

	origPrepare := api.Prepare
	api.Prepare = func(c uintptr, q *byte, out *uintptr) int32 {
		// Simulate cancel arriving during Prepare and DuckDB's InitialCleanup
		// clearing the interrupted flag before PendingExecuteTask runs.
		cancel()
		<-unblockPrepare
		interrupted.Store(false)
		resets.Add(1)
		return origPrepare(c, q, out)
	}
	go func() {
		// Wait until at least 1 interrupt fired, then let Prepare finish with reset.
		for !interrupted.Load() {
			time.Sleep(100 * time.Microsecond)
		}
		close(unblockPrepare)
	}()

	_, err := conn.Query(ctx, "SELECT 1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got err=%v, want context.Canceled", err)
	}
	if resets.Load() != 1 {
		t.Fatalf("expected simulated reset")
	}
	_ = enterTask
	// Now test reset happening INSIDE PendingExecuteTask: first interrupt cleared,
	// second re-asserted interrupt breaks the task loop.
	api.Prepare = origPrepare
	ctx2, cancel2 := context.WithCancel(context.Background())
	interrupted.Store(false)
	var seenReset atomic.Bool
	api.PendingExecuteTask = func(p uintptr) int32 {
		if !seenReset.Swap(true) {
			cancel2()
			// Wait for first interrupt, then clear it as DuckDB init can do.
			for !interrupted.Load() {
				time.Sleep(100 * time.Microsecond)
			}
			interrupted.Store(false)
			// Block until re-asserted interrupt arrives.
			for !interrupted.Load() {
				time.Sleep(100 * time.Microsecond)
			}
			return abi.PendingError
		}
		return abi.PendingResultReady
	}
	_, err = conn.Query(ctx2, "SELECT 2")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("reset in task: got err=%v, want context.Canceled", err)
	}
	_ = db.Close()
	f.assertZeroLeaks(t)
}

func TestCancelDuringFetchAndConcurrentCloseNoDeadlock(t *testing.T) {
	t.Run("rows_close_interrupts_blocked_fetch", func(t *testing.T) {
		f, lib := newFakeNative(t)
		api := lib.api
		db, _ := lib.Open(context.Background(), ":memory:")
		conn, _ := db.Connect(context.Background())
		conn.interruptInterval = 200 * time.Microsecond

		inFetch := make(chan struct{})
		interrupted := make(chan struct{})
		var once sync.Once
		f.onInterrupt = func(uintptr) {
			once.Do(func() { close(interrupted) })
		}
		api.FetchChunk = func(r abi.Result) uintptr {
			close(inFetch)
			<-interrupted
			return 0
		}

		rows, err := conn.Query(context.Background(), "SELECT 1")
		if err != nil {
			t.Fatal(err)
		}
		nextDone := make(chan bool, 1)
		go func() {
			nextDone <- rows.Next()
		}()
		<-inFetch
		if err := rows.Close(); err != nil {
			t.Fatalf("Rows.Close: %v", err)
		}
		if <-nextDone {
			t.Fatal("Next must return false after concurrent Close")
		}
		if !errors.Is(rows.Err(), ErrClosed) {
			t.Fatalf("rows.Err() = %v, want ErrClosed", rows.Err())
		}
		// Connection must be immediately reusable after Rows.Close joined.
		api.FetchChunk = func(r abi.Result) uintptr { return 0 }
		rows2, err := conn.Query(context.Background(), "SELECT 2")
		if err != nil {
			t.Fatalf("reuse after close: %v", err)
		}
		_ = rows2.Close()
		_ = db.Close()
		f.assertZeroLeaks(t)
	})

	t.Run("database_close_interrupts_multiple_blocked_conns_and_joins", func(t *testing.T) {
		f, lib := newFakeNative(t)
		api := lib.api
		db, _ := lib.Open(context.Background(), ":memory:")
		c1, _ := db.Connect(context.Background())
		c2, _ := db.Connect(context.Background())
		c1.interruptInterval = 200 * time.Microsecond
		c2.interruptInterval = 200 * time.Microsecond

		var started atomic.Int32
		allStarted := make(chan struct{})
		unblock := make(map[uintptr]chan struct{})
		unblock[c1.handle] = make(chan struct{})
		unblock[c2.handle] = make(chan struct{})
		var mu sync.Mutex

		f.onInterrupt = func(ch uintptr) {
			mu.Lock()
			defer mu.Unlock()
			if ch != 0 {
				if done, ok := unblock[ch]; ok {
					select {
					case <-done:
					default:
						close(done)
					}
				}
			}
		}

		// Map pending handle to its owning conn handle so both block independently.
		pendingConn := make(map[uintptr]uintptr)
		origPrepare := api.Prepare
		stmtConn := make(map[uintptr]uintptr)
		api.Prepare = func(c uintptr, q *byte, out *uintptr) int32 {
			st := origPrepare(c, q, out)
			if st == abi.StateSuccess {
				mu.Lock()
				stmtConn[*out] = c
				mu.Unlock()
			}
			return st
		}
		origPending := api.PendingPreparedStreaming
		api.PendingPreparedStreaming = func(s uintptr, out *uintptr) int32 {
			st := origPending(s, out)
			if st == abi.StateSuccess {
				mu.Lock()
				pendingConn[*out] = stmtConn[s]
				mu.Unlock()
			}
			return st
		}
		api.PendingExecuteTask = func(p uintptr) int32 {
			mu.Lock()
			ch := pendingConn[p]
			done := unblock[ch]
			mu.Unlock()
			if started.Add(1) == 2 {
				close(allStarted)
			}
			<-done
			return abi.PendingError
		}

		errs := make(chan error, 2)
		go func() { _, err := c1.Query(context.Background(), "SELECT 1"); errs <- err }()
		go func() { _, err := c2.Query(context.Background(), "SELECT 2"); errs <- err }()
		<-allStarted

		if err := db.Close(); err != nil {
			t.Fatalf("db.Close: %v", err)
		}
		for i := range 2 {
			if err := <-errs; !errors.Is(err, ErrClosed) {
				t.Fatalf("conn[%d] err = %v, want ErrClosed", i, err)
			}
		}
		f.assertZeroLeaks(t)
	})
}

func TestOldCancelerNeverPoisonsNextQueryAndLinearization(t *testing.T) {
	f, lib := newFakeNative(t)
	api := lib.api
	db, _ := lib.Open(context.Background(), ":memory:")
	conn, _ := db.Connect(context.Background())
	conn.interruptInterval = 100 * time.Microsecond

	// Track whether any interrupt fires while a non-canceled query is running.
	var runningClean atomic.Bool
	var poisoned atomic.Int64
	f.onInterrupt = func(uintptr) {
		if runningClean.Load() {
			poisoned.Add(1)
		}
	}

	origTask := api.PendingExecuteTask
	for i := range 30 {
		// 1) Canceled query that spins until interrupted.
		ctx, cancel := context.WithCancel(context.Background())
		var gotInt atomic.Bool
		prevOnInt := f.onInterrupt
		f.onInterrupt = func(ch uintptr) {
			gotInt.Store(true)
			prevOnInt(ch)
		}
		api.PendingExecuteTask = func(p uintptr) int32 {
			cancel()
			for !gotInt.Load() {
				time.Sleep(50 * time.Microsecond)
			}
			return abi.PendingError
		}
		_, err := conn.Query(ctx, "SELECT 1")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("iter %d canceled query err = %v", i, err)
		}
		f.onInterrupt = prevOnInt

		// 2) Immediately run a clean query; stale canceler must already be joined.
		api.PendingExecuteTask = func(p uintptr) int32 {
			time.Sleep(300 * time.Microsecond)
			return origTask(p)
		}
		runningClean.Store(true)
		rows, err := conn.Query(context.Background(), "SELECT 2")
		if err != nil {
			t.Fatalf("iter %d clean query failed: %v", i, err)
		}
		for rows.Next() {
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iter %d clean rows.Err(): %v", i, err)
		}
		_ = rows.Close()
		runningClean.Store(false)
	}
	if poisoned.Load() != 0 {
		t.Fatalf("stale canceler fired %d interrupts during subsequent clean queries", poisoned.Load())
	}

	// Linearization: cancel winning before EOF prevents clean EOF even when
	// FetchChunk returns 0 with no native error.
	f.chunkSizes = []uint64{1}
	ctx, cancel := context.WithCancel(context.Background())
	rows, err := conn.Query(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("expected first row")
	}
	cancel()
	// Wait for AfterFunc to latch cancellation.
	for rows.scope.err() == nil {
		time.Sleep(50 * time.Microsecond)
	}
	if _, err := rows.DecodeRow(nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("DecodeRow after cancel = %v, want context.Canceled", err)
	}
	if rows.Next() {
		t.Fatal("Next after cancel must be false")
	}
	if !errors.Is(rows.Err(), context.Canceled) {
		t.Fatalf("Rows.Err after cancel = %v, want context.Canceled", rows.Err())
	}
	_ = rows.Close()
	_ = db.Close()
	f.assertZeroLeaks(t)
}
