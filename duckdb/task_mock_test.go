package duckdb

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTaskStateAndExecuteTasksMockLifecycleAndJoinSafety(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	api := lib.api

	var (
		mu             sync.Mutex
		nextHandle     uintptr = 100
		states                 = make(map[uintptr]chan struct{})
		activeWorkers  atomic.Int32
		destroyedEarly atomic.Bool
		execTasksCalls atomic.Int32
		execNCalls     atomic.Int32
	)

	api.ExecuteTasks = func(db uintptr, maxTasks uint64) {
		execTasksCalls.Add(1)
	}
	api.CreateTaskState = func(db uintptr) uintptr {
		mu.Lock()
		defer mu.Unlock()
		h := nextHandle
		nextHandle++
		states[h] = make(chan struct{})
		return h
	}
	api.ExecuteTasksState = func(state uintptr) {
		mu.Lock()
		ch := states[state]
		mu.Unlock()
		activeWorkers.Add(1)
		defer activeWorkers.Add(-1)
		<-ch
		time.Sleep(5 * time.Millisecond)
	}
	api.ExecuteNTasksState = func(state uintptr, maxTasks uint64) uint64 {
		execNCalls.Add(1)
		if maxTasks > 2 {
			return 2
		}
		return maxTasks
	}
	api.FinishExecution = func(state uintptr) {
		mu.Lock()
		ch := states[state]
		mu.Unlock()
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
	api.TaskStateIsFinished = func(state uintptr) bool {
		mu.Lock()
		ch := states[state]
		mu.Unlock()
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}
	api.DestroyTaskState = func(state uintptr) {
		if activeWorkers.Load() != 0 {
			destroyedEarly.Store(true)
		}
		mu.Lock()
		delete(states, state)
		mu.Unlock()
	}

	db, err := lib.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	// ExecuteTasks(0) is a clean no-op; positive maxTasks invokes native API.
	if err := db.ExecuteTasks(0); err != nil {
		t.Fatalf("ExecuteTasks(0): %v", err)
	}
	if execTasksCalls.Load() != 0 {
		t.Fatalf("ExecuteTasks(0) unexpectedly called native API")
	}
	if err := db.ExecuteTasks(4); err != nil {
		t.Fatalf("ExecuteTasks(4): %v", err)
	}
	if execTasksCalls.Load() != 1 {
		t.Fatalf("execTasksCalls = %d, want 1", execTasksCalls.Load())
	}

	// TaskState with two concurrent blocked Execute() workers joined by Close().
	ts, err := db.NewTaskState()
	if err != nil {
		t.Fatal(err)
	}
	if ts.IsFinished() {
		t.Fatal("fresh TaskState reported finished")
	}
	if n, err := ts.ExecuteN(0); err != nil || n != 0 {
		t.Fatalf("ExecuteN(0) = (%d, %v), want (0, nil)", n, err)
	}
	if n, err := ts.ExecuteN(10); err != nil || n != 2 {
		t.Fatalf("ExecuteN(10) = (%d, %v), want (2, nil)", n, err)
	}

	var wg sync.WaitGroup
	started := make(chan struct{}, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started <- struct{}{}
			if err := ts.Execute(); err != nil {
				t.Errorf("ts.Execute() err = %v", err)
			}
		}()
	}
	<-started
	<-started
	time.Sleep(5 * time.Millisecond)

	// Close must wake workers, wait for both to exit, and only then destroy.
	if err := ts.Close(); err != nil {
		t.Fatalf("ts.Close(): %v", err)
	}
	wg.Wait()
	if destroyedEarly.Load() {
		t.Fatal("DestroyTaskState was called while ExecuteTasksState worker was still active")
	}
	if !ts.IsFinished() {
		t.Fatal("closed TaskState should report IsFinished == true")
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("second ts.Close(): %v", err)
	}
	if err := ts.Execute(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Execute after Close = %v, want ErrClosed", err)
	}
	if _, err := ts.ExecuteN(1); !errors.Is(err, ErrClosed) {
		t.Fatalf("ExecuteN after Close = %v, want ErrClosed", err)
	}

	// TaskState left open is woken and joined automatically by Database.Close().
	ts2, err := db.NewTaskState()
	if err != nil {
		t.Fatal(err)
	}
	done2 := make(chan error, 1)
	go func() {
		done2 <- ts2.Execute()
	}()
	time.Sleep(5 * time.Millisecond)

	if err := db.Close(); err != nil {
		t.Fatalf("db.Close(): %v", err)
	}
	if err := <-done2; err != nil {
		t.Fatalf("ts2.Execute after db.Close = %v", err)
	}
	if destroyedEarly.Load() {
		t.Fatal("DestroyTaskState ran early during db.Close")
	}
	mu.Lock()
	leaked := len(states)
	mu.Unlock()
	if leaked != 0 {
		t.Fatalf("leaked %d native task states", leaked)
	}

	// Post-close Database calls return ErrClosed.
	if err := db.ExecuteTasks(1); !errors.Is(err, ErrClosed) {
		t.Fatalf("ExecuteTasks after db.Close = %v, want ErrClosed", err)
	}
	if _, err := db.NewTaskState(); !errors.Is(err, ErrClosed) {
		t.Fatalf("NewTaskState after db.Close = %v, want ErrClosed", err)
	}

	// Nil receiver guards.
	var nilDB *Database
	var nilTS *TaskState
	if err := nilDB.ExecuteTasks(1); !errors.Is(err, ErrClosed) {
		t.Fatalf("nil db ExecuteTasks = %v", err)
	}
	if _, err := nilDB.NewTaskState(); !errors.Is(err, ErrClosed) {
		t.Fatalf("nil db NewTaskState = %v", err)
	}
	if !nilTS.IsFinished() || nilTS.Close() != nil {
		t.Fatal("nil TaskState guards failed")
	}
	nilTS.Finish()

	f.assertZeroLeaks(t)
}
