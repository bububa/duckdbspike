package duckdb

import (
	"context"
	"fmt"
	"runtime"

	"github.com/bububa/duckdbspike/internal/abi"
)

// ExecScript extracts and executes all statements in sql sequentially under ctx
// cancellation. Statements 0..N-2 must have no parameters; args are bound to
// the final statement N-1. RowsAffected reports the total rows changed across
// statements in the script. Empty or comment-only scripts return zero
// ExecResult and nil (unless args are supplied, which returns ErrInvalidInput).
func (c *Conn) ExecScript(ctx context.Context, sql string, args ...Value) (ExecResult, error) {
	return c.execScriptWithBinder(ctx, nil, sql, len(args) > 0, func(s *Stmt) error { return s.bindLocked(args) })
}

// ExecScriptNamed extracts and executes all statements in sql sequentially
// under ctx cancellation, binding named parameters ($name) to the final
// statement N-1.
func (c *Conn) ExecScriptNamed(ctx context.Context, sql string, args ...NamedArg) (ExecResult, error) {
	return c.execScriptWithBinder(ctx, nil, sql, len(args) > 0, func(s *Stmt) error { return s.bindNamedLocked(args) })
}

func (c *Conn) execScriptWithBinder(ctx context.Context, tx *Tx, sql string, hasArgs bool, bindLast func(*Stmt) error) (ExecResult, error) {
	if c == nil {
		return ExecResult{}, ErrClosed
	}
	if ctx == nil {
		return ExecResult{}, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return ExecResult{}, err
	}
	csql, err := abi.CString(sql)
	if err != nil {
		return ExecResult{}, fmt.Errorf("%w: sql: %v", ErrInvalidInput, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkTxLocked(tx); err != nil {
		return ExecResult{}, err
	}
	if err := c.usableLocked(); err != nil {
		return ExecResult{}, err
	}
	scope, err := c.beginScopeLocked(ctx)
	if err != nil {
		return ExecResult{}, err
	}
	defer c.endScopeLocked(scope)

	api := c.api
	var extracted uintptr
	count := api.ExtractStatements(c.handle, &csql[0], &extracted)
	runtime.KeepAlive(csql)
	defer func() {
		if extracted != 0 {
			api.DestroyExtracted(&extracted)
		}
	}()
	if count == 0 {
		msg := abi.GoString(api.ExtractStatementsError(extracted))
		if cerr := scope.err(); cerr != nil {
			return ExecResult{}, cerr
		}
		if msg != "" {
			return ExecResult{}, nativeError("extract_statements", abi.ErrorInvalid, msg)
		}
		if hasArgs {
			return ExecResult{}, fmt.Errorf("%w: script contains no statements to bind parameters to", ErrInvalidInput)
		}
		if !scope.tryFinish() {
			return ExecResult{}, scope.err()
		}
		return ExecResult{}, nil
	}

	var totalAffected uint64
	for i := uint64(0); i < count; i++ {
		if cerr := scope.err(); cerr != nil {
			return ExecResult{}, cerr
		}
		s, err := c.prepareExtractedLocked(extracted, i, scope)
		if err != nil {
			return ExecResult{}, err
		}
		binder := func() error { return s.bindLocked(nil) }
		if i == count-1 && bindLast != nil {
			binder = func() error { return bindLast(s) }
		}
		res, execErr := s.execNoFinishLocked(binder, scope)
		s.releaseLocked()
		if execErr != nil {
			return ExecResult{}, execErr
		}
		totalAffected += res.RowsAffected
	}
	if !scope.tryFinish() {
		return ExecResult{}, scope.err()
	}
	return ExecResult{RowsAffected: totalAffected}, nil
}

// QueryScript extracts all statements in sql, executes statements 0..N-2
// sequentially (which must have no parameters), and binds args to stream the
// final statement N-1. The returned Rows owns the final Stmt and cancelScope.
func (c *Conn) QueryScript(ctx context.Context, sql string, args ...Value) (*Rows, error) {
	return c.queryScriptWithBinder(ctx, nil, sql, func(s *Stmt) error { return s.bindLocked(args) })
}

// QueryScriptNamed extracts all statements in sql, executes statements 0..N-2
// sequentially (which must have no parameters), and binds named parameters
// ($name) to stream the final statement N-1.
func (c *Conn) QueryScriptNamed(ctx context.Context, sql string, args ...NamedArg) (*Rows, error) {
	return c.queryScriptWithBinder(ctx, nil, sql, func(s *Stmt) error { return s.bindNamedLocked(args) })
}

func (c *Conn) queryScriptWithBinder(ctx context.Context, tx *Tx, sql string, bindLast func(*Stmt) error) (*Rows, error) {
	if c == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	csql, err := abi.CString(sql)
	if err != nil {
		return nil, fmt.Errorf("%w: sql: %v", ErrInvalidInput, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkTxLocked(tx); err != nil {
		return nil, err
	}
	if err := c.usableLocked(); err != nil {
		return nil, err
	}
	scope, err := c.beginScopeLocked(ctx)
	if err != nil {
		return nil, err
	}

	api := c.api
	var extracted uintptr
	count := api.ExtractStatements(c.handle, &csql[0], &extracted)
	runtime.KeepAlive(csql)
	if count == 0 {
		msg := abi.GoString(api.ExtractStatementsError(extracted))
		if extracted != 0 {
			api.DestroyExtracted(&extracted)
		}
		cerr := scope.err()
		c.endScopeLocked(scope)
		if cerr != nil {
			return nil, cerr
		}
		if msg != "" {
			return nil, nativeError("extract_statements", abi.ErrorInvalid, msg)
		}
		return nil, fmt.Errorf("%w: script contains no statements", ErrInvalidInput)
	}

	for i := uint64(0); i < count-1; i++ {
		if cerr := scope.err(); cerr != nil {
			api.DestroyExtracted(&extracted)
			c.endScopeLocked(scope)
			return nil, cerr
		}
		s, err := c.prepareExtractedLocked(extracted, i, scope)
		if err != nil {
			api.DestroyExtracted(&extracted)
			c.endScopeLocked(scope)
			return nil, err
		}
		_, execErr := s.execNoFinishLocked(func() error { return s.bindLocked(nil) }, scope)
		s.releaseLocked()
		if execErr != nil {
			api.DestroyExtracted(&extracted)
			c.endScopeLocked(scope)
			return nil, execErr
		}
	}

	lastStmt, err := c.prepareExtractedLocked(extracted, count-1, scope)
	api.DestroyExtracted(&extracted)
	if err != nil {
		c.endScopeLocked(scope)
		return nil, err
	}
	rows, err := lastStmt.startLocked(func() error { return bindLast(lastStmt) }, scope)
	if err != nil {
		lastStmt.releaseLocked()
		c.endScopeLocked(scope)
		return nil, err
	}
	rows.ownsStmt = true
	return rows, nil
}

func (c *Conn) prepareExtractedLocked(extracted uintptr, index uint64, scope *cancelScope) (*Stmt, error) {
	if cerr := scope.err(); cerr != nil {
		return nil, cerr
	}
	var handle uintptr
	state := c.api.PrepareExtractedStatement(c.handle, extracted, index, &handle)
	if state != abi.StateSuccess {
		message := abi.GoString(c.api.PrepareError(handle))
		if handle != 0 {
			c.api.DestroyPrepare(&handle)
		}
		if cerr := scope.err(); cerr != nil {
			return nil, cerr
		}
		return nil, nativeError(fmt.Sprintf("prepare_extracted[%d]", index), abi.ErrorInvalid, message)
	}
	if cerr := scope.err(); cerr != nil {
		c.api.DestroyPrepare(&handle)
		return nil, cerr
	}
	return &Stmt{conn: c, handle: handle}, nil
}
