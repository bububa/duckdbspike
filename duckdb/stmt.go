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

// Exec runs the statement to completion, copies RowsAffected, and destroys the
// native result before returning.
func (s *Stmt) Exec(ctx context.Context) (ExecResult, error) {
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
	return s.execLocked()
}

// Query starts a streaming result. The Rows borrows the Stmt.
func (s *Stmt) Query(ctx context.Context) (*Rows, error) {
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
	return s.startLocked()
}

// runPendingLocked runs pending(streaming) → tasks → execute. On failure, both
// pending and result are destroyed before returning.
func (s *Stmt) runPendingLocked() (abi.Result, error) {
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

func (s *Stmt) execLocked() (ExecResult, error) {
	result, err := s.runPendingLocked()
	if err != nil {
		return ExecResult{}, err
	}
	api := s.conn.api
	affected := api.RowsChanged(&result)
	api.DestroyResult(&result)
	return ExecResult{RowsAffected: affected}, nil
}

// startLocked executes the statement and publishes a Rows only after the
// native result is verified to be streaming.
func (s *Stmt) startLocked() (*Rows, error) {
	result, err := s.runPendingLocked()
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
	columns := make([]Column, n)
	for i := range columns {
		columns[i].Name = abi.GoString(api.ColumnName(&result, uint64(i)))
	}
	rows := &Rows{
		conn:         s.conn,
		stmt:         s,
		result:       result,
		columns:      columns,
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
