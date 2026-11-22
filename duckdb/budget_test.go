package duckdb

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBudgetControllerGlobalLimitsQueueAndRelease(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	bc, err := NewBudgetController(GlobalBudget{
		MaxConcurrent:    2,
		MaxWaitQueue:     1,
		TotalMemoryBytes: 200 << 20,
		TotalThreads:     4,
		TotalSpillBytes:  1024 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()

	// Impossible request exceeding global memory fails immediately with ErrBudgetExceeded.
	if _, err := bc.Open(ctx, lib, ":memory:", InstanceConfig{
		NativeMemoryBytes: 300 << 20,
		Threads:           1,
	}); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("exceeds global mem = %v, want ErrBudgetExceeded", err)
	}

	cfg := InstanceConfig{
		NativeMemoryBytes: 100 << 20,
		Threads:           2,
		SpillBytes:        512 << 20,
	}
	db1, err := bc.Open(ctx, lib, ":memory:", cfg)
	if err != nil {
		t.Fatalf("db1: %v", err)
	}
	db2, err := bc.Open(ctx, lib, ":memory:", cfg)
	if err != nil {
		t.Fatalf("db2: %v", err)
	}
	snap := bc.Snapshot()
	if snap.ActiveInstances != 2 || snap.UsedMemoryBytes != 200<<20 || snap.UsedThreads != 4 || snap.UsedSpillBytes != 1024<<20 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}

	// Third caller enters wait queue (MaxWaitQueue = 1).
	db3Ch := make(chan *Database, 1)
	err3Ch := make(chan error, 1)
	go func() {
		d, e := bc.Open(ctx, lib, ":memory:", cfg)
		db3Ch <- d
		err3Ch <- e
	}()
	// Wait until waiter is queued.
	for bc.Snapshot().Waiting != 1 {
		time.Sleep(100 * time.Microsecond)
	}

	// Fourth caller hits full queue -> ErrQueueFull immediately.
	if _, err := bc.Open(ctx, lib, ":memory:", cfg); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full queue = %v, want ErrQueueFull", err)
	}

	// Closing db1 releases its reservation and wakes db3.
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-err3Ch; err != nil {
		t.Fatalf("db3 open failed: %v", err)
	}
	db3 := <-db3Ch

	// Waiter canceling via context frees its queue slot.
	cancelCtx, cancel := context.WithCancel(ctx)
	cancelErrCh := make(chan error, 1)
	go func() {
		_, e := bc.Open(cancelCtx, lib, ":memory:", cfg)
		cancelErrCh <- e
	}()
	for bc.Snapshot().Waiting != 1 {
		time.Sleep(100 * time.Microsecond)
	}
	cancel()
	if err := <-cancelErrCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter err = %v, want context.Canceled", err)
	}
	if bc.Snapshot().Waiting != 0 {
		t.Fatalf("waiter not removed on cancel: %+v", bc.Snapshot())
	}

	// Open failure releases global reservation immediately.
	_ = db2.Close()
	f.failOpenExt = "boom"
	if _, err := bc.Open(ctx, lib, ":memory:", cfg); err == nil {
		t.Fatal("expected open error")
	}
	f.failOpenExt = ""
	if snap := bc.Snapshot(); snap.ActiveInstances != 1 || snap.UsedMemoryBytes != 100<<20 {
		t.Fatalf("reservation leaked after failed Open: %+v", snap)
	}

	_ = db3.Close()
	if snap := bc.Snapshot(); snap.ActiveInstances != 0 || snap.UsedMemoryBytes != 0 || snap.UsedThreads != 0 || snap.UsedSpillBytes != 0 {
		t.Fatalf("reservation leaked after all Close: %+v", snap)
	}
	f.assertZeroLeaks(t)
}

func TestPerQueryRowAndByteBudgetsNeverSilentlyTruncate(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	db, _ := lib.Open(ctx, ":memory:")
	conn, _ := db.Connect(ctx)
	if err := conn.SetResultLimits(ResultLimits{MaxRows: 5}); err != nil {
		t.Fatal(err)
	}
	f.chunkSizes = []uint64{4, 4}
	rows, err := conn.Query(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	var count int
	for rows.Next() {
		count++
	}
	if count != 5 {
		t.Fatalf("read %d rows before limit, want 5", count)
	}
	if !errors.Is(rows.Err(), ErrBudgetExceeded) {
		t.Fatalf("rows.Err() = %v, want ErrBudgetExceeded (must not look like EOF)", rows.Err())
	}
	_ = rows.Close()
	_ = db.Close()
	f.assertZeroLeaks(t)
}
