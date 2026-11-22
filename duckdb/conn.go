package duckdb

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/bububa/duckdbspike/internal/abi"
)

// ExecResult holds copied metadata from a non-row statement (DDL, SET,
// CHECKPOINT, DML). All native resources are released before Exec returns.
type ExecResult struct {
	RowsAffected uint64
}

// Conn owns one native connection. All native calls on a Conn and on its Stmts
// and Rows are serialized by mu. Cancellation signals use cancelMu, which is
// never held across a native call, so duckdb_interrupt never blocks on mu.
type Conn struct {
	_      noCopy
	db     *Database
	api    *abi.API
	mu     sync.Mutex
	handle uintptr
	closed bool
	limits DecodeLimits
	stmts  map[*Stmt]struct{}
	active *Rows

	cancelMu          sync.Mutex
	closing           bool
	activeScope       *cancelScope
	interruptInterval time.Duration
}

// SetDecodeLimits configures per-value decoding limits for subsequent queries
// on this connection. Zero fields select DefaultDecodeLimits.
func (c *Conn) SetDecodeLimits(limits DecodeLimits) error {
	if c == nil {
		return ErrClosed
	}
	norm, err := limits.normalized()
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	c.limits = norm
	return nil
}

func (c *Conn) beginScopeLocked(ctx context.Context) (*cancelScope, error) {
	c.cancelMu.Lock()
	if c.closing {
		c.cancelMu.Unlock()
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		c.cancelMu.Unlock()
		return nil, err
	}
	scope := newCancelScope(ctx, c.api, c.handle, c.interruptInterval)
	c.activeScope = scope
	c.cancelMu.Unlock()
	if err := ctx.Err(); err != nil {
		scope.cancel(err)
		c.endScopeLocked(scope)
		return nil, err
	}
	return scope, nil
}

func (c *Conn) endScopeLocked(scope *cancelScope) {
	if scope == nil {
		return
	}
	c.cancelMu.Lock()
	if c.activeScope == scope {
		c.activeScope = nil
	}
	c.cancelMu.Unlock()
	scope.join()
}

// beginClose stops admitting new scopes and interrupts any in-flight native
// call on this connection without acquiring c.mu.
func (c *Conn) beginClose() {
	c.cancelMu.Lock()
	c.closing = true
	scope := c.activeScope
	c.cancelMu.Unlock()
	if scope != nil {
		scope.cancel(ErrClosed)
	}
}

// Prepare compiles a single trusted SQL statement. SQL containing NUL is
// rejected before reaching native code.
func (c *Conn) Prepare(ctx context.Context, sql string) (*Stmt, error) {
	if c == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return nil, err
	}
	scope, err := c.beginScopeLocked(ctx)
	if err != nil {
		return nil, err
	}
	defer c.endScopeLocked(scope)
	s, err := c.prepareLocked(sql, scope)
	if err != nil {
		return nil, err
	}
	if !scope.tryFinish() {
		s.releaseLocked()
		return nil, scope.err()
	}
	c.stmts[s] = struct{}{}
	return s, nil
}

// Exec prepares, binds args, and runs a statement that does not stream rows
// (such as SET, CREATE VIEW, or CHECKPOINT). RowsAffected is copied before the
// native result and statement are destroyed.
func (c *Conn) Exec(ctx context.Context, sql string, args ...Value) (ExecResult, error) {
	if c == nil {
		return ExecResult{}, ErrClosed
	}
	if ctx == nil {
		return ExecResult{}, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return ExecResult{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return ExecResult{}, err
	}
	scope, err := c.beginScopeLocked(ctx)
	if err != nil {
		return ExecResult{}, err
	}
	defer c.endScopeLocked(scope)
	s, err := c.prepareLocked(sql, scope)
	if err != nil {
		return ExecResult{}, err
	}
	defer s.releaseLocked()
	return s.execLocked(args, scope)
}

// Query prepares sql, binds args, and starts a streaming result. The returned
// Rows owns both the statement and the cancellation scope, joining and
// releasing them on Close.
func (c *Conn) Query(ctx context.Context, sql string, args ...Value) (*Rows, error) {
	if c == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return nil, err
	}
	scope, err := c.beginScopeLocked(ctx)
	if err != nil {
		return nil, err
	}
	s, err := c.prepareLocked(sql, scope)
	if err != nil {
		c.endScopeLocked(scope)
		return nil, err
	}
	rows, err := s.startLocked(args, scope)
	if err != nil {
		s.releaseLocked()
		c.endScopeLocked(scope)
		return nil, err
	}
	rows.ownsStmt = true
	return rows, nil
}

func (c *Conn) usableLocked() error {
	if c.closed {
		return ErrClosed
	}
	if c.active != nil {
		return ErrBusy
	}
	return nil
}

func (c *Conn) prepareLocked(sql string, scope *cancelScope) (*Stmt, error) {
	csql, err := abi.CString(sql)
	if err != nil {
		return nil, fmt.Errorf("%w: sql: %v", ErrInvalidInput, err)
	}
	if err := scope.err(); err != nil {
		return nil, err
	}
	var handle uintptr
	state := c.api.Prepare(c.handle, &csql[0], &handle)
	runtime.KeepAlive(csql)
	if state != abi.StateSuccess {
		// duckdb_prepare allocates a statement even on failure.
		message := abi.GoString(c.api.PrepareError(handle))
		if handle != 0 {
			c.api.DestroyPrepare(&handle)
		}
		if cerr := scope.err(); cerr != nil {
			return nil, cerr
		}
		return nil, nativeError("prepare", abi.ErrorInvalid, message)
	}
	if cerr := scope.err(); cerr != nil {
		c.api.DestroyPrepare(&handle)
		return nil, cerr
	}
	return &Stmt{conn: c, handle: handle}, nil
}

// Close interrupts any active native work on this connection, joins the native
// call and its cancellation executor without a deadline, closes the open Rows
// and every Stmt, then disconnects. Close is idempotent.
func (c *Conn) Close() error {
	if c == nil {
		return nil
	}
	c.beginClose()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	if c.active != nil {
		c.active.releaseLocked()
	}
	for s := range c.stmts {
		s.releaseLocked()
	}
	c.api.Disconnect(&c.handle)
	c.mu.Unlock()
	c.db.forget(c)
	return nil
}
