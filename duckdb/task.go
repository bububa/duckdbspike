package duckdb

import (
	"sync"
	"time"

	"github.com/bububa/duckdbspike/internal/abi"
)

// ExecuteTasks executes up to maxTasks DuckDB tasks on the current OS thread
// (for databases configured with external_threads). Returns early if no tasks
// are queued. Safe to call concurrently; Database.Close waits for in-flight
// ExecuteTasks calls to return before destroying the native database.
func (d *Database) ExecuteTasks(maxTasks uint64) error {
	if d == nil {
		return ErrClosed
	}
	if maxTasks == 0 {
		return nil
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return ErrClosed
	}
	api := d.lib.api
	handle := d.handle
	d.execTasksWG.Add(1)
	d.mu.Unlock()
	defer d.execTasksWG.Done()

	api.ExecuteTasks(handle, maxTasks)
	return nil
}

// TaskState coordinates external worker goroutines executing DuckDB background
// tasks via duckdb_execute_tasks_state / duckdb_execute_n_tasks_state. Multiple
// goroutines may share one TaskState concurrently. Call Finish to wake blocked
// Execute workers and Close to wait for all active workers and release the
// native task state. Any TaskStates left open are finished and joined
// automatically during Database.Close.
type TaskState struct {
	_        noCopy
	db       *Database
	api      *abi.API
	mu       sync.Mutex
	activeWG sync.WaitGroup
	handle   uintptr
	finished bool
	closed   bool
	once     sync.Once
}

// NewTaskState creates an owned DuckDB task state bound to d for external
// task execution (e.g., when d was opened with external_threads > 0).
func (d *Database) NewTaskState() (*TaskState, error) {
	if d == nil {
		return nil, ErrClosed
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, ErrClosed
	}
	api := d.lib.api
	handle := api.CreateTaskState(d.handle)
	if handle == 0 {
		return nil, nativeError("create_task_state", abi.ErrorOutOfMemory, "")
	}
	ts := &TaskState{
		db:     d,
		api:    api,
		handle: handle,
	}
	if d.taskStates == nil {
		d.taskStates = make(map[*TaskState]struct{})
	}
	d.taskStates[ts] = struct{}{}
	return ts, nil
}

// Execute runs DuckDB tasks on the calling goroutine until Finish or Close is
// called on ts (or on the owning Database). Multiple goroutines may call
// Execute concurrently on the same TaskState.
func (ts *TaskState) Execute() error {
	if ts == nil {
		return ErrClosed
	}
	ts.mu.Lock()
	if ts.closed {
		ts.mu.Unlock()
		return ErrClosed
	}
	if ts.finished {
		ts.mu.Unlock()
		return nil
	}
	handle := ts.handle
	ts.activeWG.Add(1)
	ts.mu.Unlock()
	defer ts.activeWG.Done()

	ts.api.ExecuteTasksState(handle)
	return nil
}

// ExecuteN executes up to maxTasks queued DuckDB tasks on the calling goroutine
// and returns the number of tasks actually executed. Returns (0, nil) when
// maxTasks == 0 or Finish has already been called.
func (ts *TaskState) ExecuteN(maxTasks uint64) (uint64, error) {
	if ts == nil {
		return 0, ErrClosed
	}
	if maxTasks == 0 {
		return 0, nil
	}
	ts.mu.Lock()
	if ts.closed {
		ts.mu.Unlock()
		return 0, ErrClosed
	}
	if ts.finished {
		ts.mu.Unlock()
		return 0, nil
	}
	handle := ts.handle
	ts.activeWG.Add(1)
	ts.mu.Unlock()
	defer ts.activeWG.Done()

	return ts.api.ExecuteNTasksState(handle, maxTasks), nil
}

// Finish signals all workers running Execute / ExecuteN on ts to stop and
// return. Safe to call multiple times or concurrently with Execute.
func (ts *TaskState) Finish() {
	if ts == nil {
		return
	}
	ts.mu.Lock()
	if ts.closed || ts.handle == 0 {
		ts.mu.Unlock()
		return
	}
	ts.finished = true
	handle := ts.handle
	ts.activeWG.Add(1)
	ts.mu.Unlock()
	defer ts.activeWG.Done()

	ts.api.FinishExecution(handle)
}

// IsFinished reports whether execution on ts has been finished or closed.
func (ts *TaskState) IsFinished() bool {
	if ts == nil {
		return true
	}
	ts.mu.Lock()
	if ts.closed || ts.handle == 0 {
		ts.mu.Unlock()
		return true
	}
	if ts.finished {
		ts.mu.Unlock()
		return true
	}
	handle := ts.handle
	ts.activeWG.Add(1)
	ts.mu.Unlock()
	defer ts.activeWG.Done()

	return ts.api.TaskStateIsFinished(handle)
}

// Close finishes execution, waits for every concurrent Execute / ExecuteN /
// Finish / IsFinished call on ts to return, destroys the native task state,
// and deregisters ts from its Database. Idempotent.
func (ts *TaskState) Close() error {
	if ts == nil {
		return nil
	}
	ts.once.Do(func() {
		ts.mu.Lock()
		handle := ts.handle
		ts.finished = true
		ts.closed = true
		ts.mu.Unlock()

		if handle != 0 {
			ts.api.FinishExecution(handle)
			// DuckDB requires no active duckdb_execute_tasks_state calls during
			// duckdb_destroy_task_state. Periodically re-signal while workers
			// unwind in case DuckDB's shared scheduler semaphore delivered the
			// initial wakeup to an internal thread or before a worker parked.
			done := make(chan struct{})
			go func() {
				ts.activeWG.Wait()
				close(done)
			}()
			ticker := time.NewTicker(time.Millisecond)
		waitLoop:
			for {
				select {
				case <-done:
					break waitLoop
				case <-ticker.C:
					ts.api.FinishExecution(handle)
				}
			}
			ticker.Stop()
		} else {
			ts.activeWG.Wait()
		}

		ts.mu.Lock()
		ts.handle = 0
		ts.mu.Unlock()

		if handle != 0 {
			ts.api.DestroyTaskState(handle)
		}
		ts.db.forgetTaskState(ts)
	})
	return nil
}

func (d *Database) forgetTaskState(ts *TaskState) {
	d.mu.Lock()
	delete(d.taskStates, ts)
	d.mu.Unlock()
}
