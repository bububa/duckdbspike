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
	_         noCopy
	conn      *Conn
	handle    uintptr
	closed    bool
	rows      *Rows
	arrowRows *ArrowRows
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

// ParamNames returns the parameter identifiers declared by the statement in
// 1-based ordinal order (for example, "1", "2" for positional parameters or
// "tenant", "cutoff" for named $tenant / $cutoff parameters).
func (s *Stmt) ParamNames() ([]string, error) {
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
	n := api.NParams(s.handle)
	if n > math.MaxInt {
		return nil, fmt.Errorf("%w: parameter count %d overflows int", ErrInvalidInput, n)
	}
	names := make([]string, int(n))
	for i := range n {
		ptr := api.ParameterName(s.handle, i+1)
		names[i] = abi.GoString(ptr)
		if ptr != 0 {
			api.Free(ptr)
		}
	}
	return names, nil
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
	for i := range n {
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

// Exec binds args, runs the statement to completion under ctx cancellation,
// copies RowsAffected, and destroys the native result before returning.
func (s *Stmt) Exec(ctx context.Context, args ...Value) (ExecResult, error) {
	return s.execWithBinder(ctx, func() error { return s.bindLocked(args) })
}

// ExecNamed binds named parameters ($name), runs the statement to completion
// under ctx cancellation, copies RowsAffected, and destroys the native result.
func (s *Stmt) ExecNamed(ctx context.Context, args ...NamedArg) (ExecResult, error) {
	return s.execWithBinder(ctx, func() error { return s.bindNamedLocked(args) })
}

func (s *Stmt) execWithBinder(ctx context.Context, bind func() error) (ExecResult, error) {
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
	scope, err := c.beginScopeLocked(ctx)
	if err != nil {
		return ExecResult{}, err
	}
	defer c.endScopeLocked(scope)
	return s.execLocked(bind, scope)
}

// Query binds args and starts a streaming result governed by ctx cancellation
// through final fetch and decode. The Rows borrows the Stmt.
func (s *Stmt) Query(ctx context.Context, args ...Value) (*Rows, error) {
	return s.queryWithBinder(ctx, func() error { return s.bindLocked(args) })
}

// QueryNamed binds named parameters ($name) and starts a streaming result
// governed by ctx cancellation through final fetch and decode.
func (s *Stmt) QueryNamed(ctx context.Context, args ...NamedArg) (*Rows, error) {
	return s.queryWithBinder(ctx, func() error { return s.bindNamedLocked(args) })
}

func (s *Stmt) queryWithBinder(ctx context.Context, bind func() error) (*Rows, error) {
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
	scope, err := c.beginScopeLocked(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := s.startLocked(bind, scope)
	if err != nil {
		c.endScopeLocked(scope)
		return nil, err
	}
	return rows, nil
}

// runPendingLocked binds parameters and runs pending(streaming) → tasks → execute
// under scope. On failure or cancellation, pending and result are destroyed
// before returning.
func (s *Stmt) runPendingLocked(bind func() error, scope *cancelScope) (abi.Result, error) {
	if cerr := scope.err(); cerr != nil {
		return abi.Result{}, cerr
	}
	if err := bind(); err != nil {
		return abi.Result{}, err
	}
	if cerr := scope.err(); cerr != nil {
		return abi.Result{}, cerr
	}
	api := s.conn.api
	var pending uintptr
	if api.PendingPreparedStreaming(s.handle, &pending) != abi.StateSuccess {
		message := abi.GoString(api.PendingError(pending))
		if pending != 0 {
			api.DestroyPending(&pending)
		}
		if cerr := scope.err(); cerr != nil {
			return abi.Result{}, cerr
		}
		return abi.Result{}, nativeError("pending", abi.ErrorInvalid, message)
	}
tasks:
	for {
		if cerr := scope.err(); cerr != nil {
			api.DestroyPending(&pending)
			return abi.Result{}, cerr
		}
		switch api.PendingExecuteTask(pending) {
		case abi.PendingResultReady:
			break tasks
		case abi.PendingResultNotReady:
		case abi.PendingNoTasks:
			runtime.Gosched()
		default:
			message := abi.GoString(api.PendingError(pending))
			api.DestroyPending(&pending)
			if cerr := scope.err(); cerr != nil {
				return abi.Result{}, cerr
			}
			return abi.Result{}, nativeError("execute_task", abi.ErrorInvalid, message)
		}
	}
	if cerr := scope.err(); cerr != nil {
		api.DestroyPending(&pending)
		return abi.Result{}, cerr
	}
	var result abi.Result
	state := api.ExecutePending(pending, &result)
	api.DestroyPending(&pending)
	if state != abi.StateSuccess {
		err := nativeError("execute", api.ResultErrorType(&result), abi.GoString(api.ResultError(&result)))
		api.DestroyResult(&result)
		if cerr := scope.err(); cerr != nil {
			return abi.Result{}, cerr
		}
		return abi.Result{}, err
	}
	return result, nil
}

func (s *Stmt) execNoFinishLocked(bind func() error, scope *cancelScope) (ExecResult, error) {
	result, err := s.runPendingLocked(bind, scope)
	if err != nil {
		return ExecResult{}, err
	}
	api := s.conn.api
	affected := api.RowsChanged(&result)
	var stmtType StatementType
	if api.ResultStatementType != nil {
		stmtType = StatementType(api.ResultStatementType(result))
	} else if api.PreparedStatementType != nil {
		stmtType = StatementType(api.PreparedStatementType(s.handle))
	}
	var returnType ResultType
	if api.ResultReturnType != nil {
		returnType = ResultType(api.ResultReturnType(result))
	}
	api.DestroyResult(&result)
	if cerr := scope.err(); cerr != nil {
		return ExecResult{}, cerr
	}
	return ExecResult{
		RowsAffected:  affected,
		StatementType: stmtType,
		ReturnType:    returnType,
	}, nil
}

func (s *Stmt) execLocked(bind func() error, scope *cancelScope) (ExecResult, error) {
	res, err := s.execNoFinishLocked(bind, scope)
	if err != nil {
		return ExecResult{}, err
	}
	if !scope.tryFinish() {
		return ExecResult{}, scope.err()
	}
	return res, nil
}

// startLocked executes the statement, validates every column's logical type
// tree (even if the result has 0 rows or all NULLs), and publishes a Rows only
// after the native result is verified to be streaming and not canceled.
func (s *Stmt) startLocked(bind func() error, scope *cancelScope) (*Rows, error) {
	result, err := s.runPendingLocked(bind, scope)
	if err != nil {
		return nil, err
	}
	api := s.conn.api
	if cerr := scope.err(); cerr != nil {
		api.DestroyResult(&result)
		return nil, cerr
	}
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
	for i := range n {
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
	if cerr := scope.err(); cerr != nil {
		api.DestroyResult(&result)
		return nil, cerr
	}
	var stmtType StatementType
	if api.ResultStatementType != nil {
		stmtType = StatementType(api.ResultStatementType(result))
	} else if api.PreparedStatementType != nil {
		stmtType = StatementType(api.PreparedStatementType(s.handle))
	}
	var returnType ResultType
	if api.ResultReturnType != nil {
		returnType = ResultType(api.ResultReturnType(result))
	}
	rows := &Rows{
		conn:         s.conn,
		stmt:         s,
		scope:        scope,
		result:       result,
		columns:      columns,
		plans:        plans,
		limits:       limits,
		resultLimits: s.conn.resultLimits,
		stmtType:     stmtType,
		returnType:   returnType,
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
	if s.rows != nil || s.arrowRows != nil {
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
	if s.arrowRows != nil {
		s.arrowRows.releaseLocked()
	}
	s.closed = true
	s.conn.api.DestroyPrepare(&s.handle)
	delete(s.conn.stmts, s)
}
