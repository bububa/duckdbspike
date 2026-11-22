package duckdb

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/bububa/duckdbspike/internal/abi"
)

// ExecResult holds copied metadata from a non-row statement (DDL, SET,
// CHECKPOINT, DML). All native resources are released before Exec returns.
type ExecResult struct {
	RowsAffected uint64
}

// Conn owns one native connection. All native calls on a Conn and on its Stmts
// and Rows are serialized by mu.
type Conn struct {
	_      noCopy
	db     *Database
	api    *abi.API
	mu     sync.Mutex
	handle uintptr
	closed bool
	stmts  map[*Stmt]struct{}
	active *Rows
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
	s, err := c.prepareLocked(sql)
	if err != nil {
		return nil, err
	}
	c.stmts[s] = struct{}{}
	return s, nil
}

// Exec prepares and runs a statement that does not stream rows (such as SET,
// CREATE VIEW, or CHECKPOINT). RowsAffected is copied before the native result
// and statement are destroyed.
func (c *Conn) Exec(ctx context.Context, sql string) (ExecResult, error) {
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
	s, err := c.prepareLocked(sql)
	if err != nil {
		return ExecResult{}, err
	}
	defer s.releaseLocked()
	return s.execLocked()
}

// Query prepares sql and starts a streaming result. The returned Rows owns
// the statement and releases it on Close.
func (c *Conn) Query(ctx context.Context, sql string) (*Rows, error) {
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
	s, err := c.prepareLocked(sql)
	if err != nil {
		return nil, err
	}
	rows, err := s.startLocked()
	if err != nil {
		s.releaseLocked()
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

func (c *Conn) prepareLocked(sql string) (*Stmt, error) {
	csql, err := abi.CString(sql)
	if err != nil {
		return nil, fmt.Errorf("%w: sql: %v", ErrInvalidInput, err)
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
		return nil, nativeError("prepare", abi.ErrorInvalid, message)
	}
	return &Stmt{conn: c, handle: handle}, nil
}

// Close closes the open Rows and every Stmt of this connection, then
// disconnects. Close is idempotent; concurrent callers wait for completion.
func (c *Conn) Close() error {
	if c == nil {
		return nil
	}
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
