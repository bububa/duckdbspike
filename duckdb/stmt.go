package duckdb

import (
	"context"
	"fmt"
	"math"
	"runtime"

	"github.com/bububa/duckdbspike/internal/abi"
)

// Stmt owns one native prepared statement. A Stmt stays alive while a Rows
// started from it is open, because the streaming result borrows it.
type Stmt struct {
	_      noCopy
	conn   *Conn
	handle uintptr
	closed bool
	rows   *Rows
}

// ParamCount returns the number of parameters declared by the statement.
func (s *Stmt) ParamCount() (int, error) {
	if s == nil {
		return 0, ErrClosed
	}
	c := s.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if s.closed || c.closed {
		return 0, ErrClosed
	}
	n := c.api.NParams(s.handle)
	if n > math.MaxInt {
		return 0, fmt.Errorf("%w: parameter count %d overflows int", ErrInvalidInput, n)
	}
	return int(n), nil
}

// Columns inspects the prepared statement's output schema before execution,
// enabling callers and query templates to assert exact column names and types
// (for example rejecting expressions that widen DECIMAL to DOUBLE). Any
// unsupported logical type fails immediately.
func (s *Stmt) Columns() ([]Column, error) {
	if s == nil {
		return nil, ErrClosed
	}
	c := s.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if s.closed || c.closed {
		return nil, ErrClosed
	}
	api := c.api
	n := api.PreparedStatementColumnCount(s.handle)
	if n > math.MaxInt {
		return nil, fmt.Errorf("%w: column count %d overflows int", ErrInvalidInput, n)
	}
	cols := make([]Column, int(n))
	for i := uint64(0); i < n; i++ {
		namePtr := api.PreparedStatementColumnName(s.handle, i)
		name := abi.GoString(namePtr)
		if namePtr != 0 {
			api.Free(namePtr)
		}
		lt := api.PreparedStatementColumnLogicalType(s.handle, i)
		plan, err := buildColumnPlan(api, lt, 0, c.limits)
		api.DestroyLogicalType(&lt)
		if err != nil {
			return nil, fmt.Errorf("column %d (%q): %w", i, name, err)
		}
		cols[i] = Column{Name: name, Type: plan.lt}
	}
	return cols, nil
}

// Exec binds args, runs the statement to completion, copies RowsAffected, and
// destroys the native result before returning.
func (s *Stmt) Exec(ctx context.Context, args ...Value) (ExecResult, error) {
	if s == nil {
		return ExecResult{}, ErrClosed
	}
	if ctx == nil {
		return ExecResult{}, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return ExecResult{}, err
	}
	c := s.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if s.closed {
		return ExecResult{}, ErrClosed
	}
	if err := c.usableLocked(); err != nil {
		return ExecResult{}, err
	}
	return s.execLocked(args)
}

// Query binds args and starts a streaming result. The Rows borrows the Stmt.
func (s *Stmt) Query(ctx context.Context, args ...Value) (*Rows, error) {
	if s == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c := s.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	if err := c.usableLocked(); err != nil {
		return nil, err
	}
	return s.startLocked(args)
}

// runPendingLocked binds args and runs pending(streaming) → tasks → execute.
// On failure, both pending and result are destroyed before returning.
func (s *Stmt) runPendingLocked(args []Value) (abi.Result, error) {
	if err := s.bindLocked(args); err != nil {
		return abi.Result{}, err
	}
	api := s.conn.api
	var pending uintptr
	if api.PendingPreparedStreaming(s.handle, &pending) != abi.StateSuccess {
		message := abi.GoString(api.PendingError(pending))
		if pending != 0 {
			api.DestroyPending(&pending)
		}
		return abi.Result{}, nativeError("pending", abi.ErrorInvalid, message)
	}
tasks:
	for {
		switch api.PendingExecuteTask(pending) {
		case abi.PendingResultReady:
			break tasks
		case abi.PendingResultNotReady:
		case abi.PendingNoTasks:
			runtime.Gosched()
		default:
			message := abi.GoString(api.PendingError(pending))
			api.DestroyPending(&pending)
			return abi.Result{}, nativeError("execute_task", abi.ErrorInvalid, message)
		}
	}
	var result abi.Result
	state := api.ExecutePending(pending, &result)
	api.DestroyPending(&pending)
	if state != abi.StateSuccess {
		err := nativeError("execute", api.ResultErrorType(&result), abi.GoString(api.ResultError(&result)))
		api.DestroyResult(&result)
		return abi.Result{}, err
	}
	return result, nil
}

func (s *Stmt) execLocked(args []Value) (ExecResult, error) {
	result, err := s.runPendingLocked(args)
	if err != nil {
		return ExecResult{}, err
	}
	api := s.conn.api
	affected := api.RowsChanged(&result)
	api.DestroyResult(&result)
	return ExecResult{RowsAffected: affected}, nil
}

// startLocked executes the statement, validates every column's logical type
// tree (even if the result has 0 rows or all NULLs), and publishes a Rows only
// after the native result is verified to be streaming.
func (s *Stmt) startLocked(args []Value) (*Rows, error) {
	result, err := s.runPendingLocked(args)
	if err != nil {
		return nil, err
	}
	api := s.conn.api
	if !api.ResultIsStreaming(result) {
		api.DestroyResult(&result)
		return nil, ErrNotStreaming
	}
	n := api.ColumnCount(&result)
	if n > math.MaxInt {
		api.DestroyResult(&result)
		return nil, fmt.Errorf("%w: column count %d overflows int", ErrInvalidInput, n)
	}
	limits := s.conn.limits
	columns := make([]Column, int(n))
	plans := make([]columnPlan, int(n))
	for i := uint64(0); i < n; i++ {
		name := abi.GoString(api.ColumnName(&result, i))
		lt := api.ColumnLogicalType(&result, i)
		plan, planErr := buildColumnPlan(api, lt, 0, limits)
		api.DestroyLogicalType(&lt)
		if planErr != nil {
			api.DestroyResult(&result)
			return nil, fmt.Errorf("column %d (%q): %w", i, name, planErr)
		}
		columns[i] = Column{Name: name, Type: plan.lt}
		plans[i] = plan
	}
	rows := &Rows{
		conn:         s.conn,
		stmt:         s,
		result:       result,
		columns:      columns,
		plans:        plans,
		limits:       limits,
		rowsAffected: api.RowsChanged(&result),
	}
	s.rows = rows
	s.conn.active = rows
	return rows, nil
}

// Close releases the statement. It returns ErrBusy while a Rows started from
// it is open. Close is idempotent.
func (s *Stmt) Close() error {
	if s == nil {
		return nil
	}
	c := s.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if s.closed {
		return nil
	}
	if s.rows != nil {
		return ErrBusy
	}
	s.releaseLocked()
	return nil
}

func (s *Stmt) releaseLocked() {
	if s.closed {
		return
	}
	if s.rows != nil {
		s.rows.releaseLocked()
	}
	s.closed = true
	s.conn.api.DestroyPrepare(&s.handle)
	delete(s.conn.stmts, s)
}
