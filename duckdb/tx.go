package duckdb

import (
	"context"
	"fmt"
)

// TxOptions configures a DuckDB transaction started by Conn.BeginTx.
type TxOptions struct {
	// ReadOnly begins a read-only transaction (BEGIN TRANSACTION READ ONLY).
	ReadOnly bool
}

// Tx represents an explicit SQL transaction on a Conn. A Conn may have at most
// one active Tx at a time. Call Commit or Rollback to finish the transaction;
// operations after completion return ErrTxDone.
type Tx struct {
	_        noCopy
	conn     *Conn
	readOnly bool
	done     bool
}

// BeginTx starts an explicit transaction on c. Returns ErrTxActive if c already
// has an open transaction, or ErrBusy if a Rows or Appender is open.
func (c *Conn) BeginTx(ctx context.Context, opts TxOptions) (*Tx, error) {
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
	if c.tx != nil {
		return nil, ErrTxActive
	}
	sql := "BEGIN TRANSACTION"
	if opts.ReadOnly {
		sql = "BEGIN TRANSACTION READ ONLY"
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
	defer s.releaseLocked()
	if _, err := s.execLocked(func() error { return s.bindLocked(nil) }, scope); err != nil {
		return nil, err
	}
	tx := &Tx{conn: c, readOnly: opts.ReadOnly}
	c.tx = tx
	return tx, nil
}

// Commit commits the transaction. Returns ErrTxDone if the transaction already
// finished, or ErrBusy if a Rows or Appender is still open on the connection.
func (tx *Tx) Commit(ctx context.Context) error {
	return tx.finish(ctx, "COMMIT")
}

// Rollback aborts the transaction. Returns ErrTxDone if the transaction already
// finished, or ErrBusy if a Rows or Appender is still open on the connection.
func (tx *Tx) Rollback(ctx context.Context) error {
	return tx.finish(ctx, "ROLLBACK")
}

func (tx *Tx) finish(ctx context.Context, sql string) error {
	if tx == nil || tx.conn == nil {
		return ErrTxDone
	}
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c := tx.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		tx.done = true
		return ErrClosed
	}
	if tx.done || c.tx != tx {
		return ErrTxDone
	}
	if err := c.usableLocked(); err != nil {
		return err
	}
	scope, err := c.beginScopeLocked(ctx)
	if err != nil {
		return err
	}
	defer c.endScopeLocked(scope)
	s, err := c.prepareLocked(sql, scope)
	if err != nil {
		tx.done = true
		c.tx = nil
		return err
	}
	defer s.releaseLocked()
	_, execErr := s.execLocked(func() error { return s.bindLocked(nil) }, scope)
	tx.done = true
	c.tx = nil
	return execErr
}

// Prepare prepares sql within tx.
func (tx *Tx) Prepare(ctx context.Context, sql string) (*Stmt, error) {
	if tx == nil || tx.conn == nil {
		return nil, ErrTxDone
	}
	return tx.conn.prepareWithTx(ctx, tx, sql)
}

// Exec executes a non-row statement within tx.
func (tx *Tx) Exec(ctx context.Context, sql string, args ...Value) (ExecResult, error) {
	if tx == nil || tx.conn == nil {
		return ExecResult{}, ErrTxDone
	}
	return tx.conn.execWithBinder(ctx, tx, sql, func(s *Stmt) error { return s.bindLocked(args) })
}

// ExecNamed executes a non-row statement with named parameters within tx.
func (tx *Tx) ExecNamed(ctx context.Context, sql string, args ...NamedArg) (ExecResult, error) {
	if tx == nil || tx.conn == nil {
		return ExecResult{}, ErrTxDone
	}
	return tx.conn.execWithBinder(ctx, tx, sql, func(s *Stmt) error { return s.bindNamedLocked(args) })
}

// Query starts a streaming query within tx.
func (tx *Tx) Query(ctx context.Context, sql string, args ...Value) (*Rows, error) {
	if tx == nil || tx.conn == nil {
		return nil, ErrTxDone
	}
	return tx.conn.queryWithBinder(ctx, tx, sql, func(s *Stmt) error { return s.bindLocked(args) })
}

// QueryNamed starts a streaming query with named parameters within tx.
func (tx *Tx) QueryNamed(ctx context.Context, sql string, args ...NamedArg) (*Rows, error) {
	if tx == nil || tx.conn == nil {
		return nil, ErrTxDone
	}
	return tx.conn.queryWithBinder(ctx, tx, sql, func(s *Stmt) error { return s.bindNamedLocked(args) })
}

// ExecScript executes a multi-statement SQL script within tx, distributing
// positional args across statements in declaration order.
func (tx *Tx) ExecScript(ctx context.Context, sql string, args ...Value) (ExecResult, error) {
	if tx == nil || tx.conn == nil {
		return ExecResult{}, ErrTxDone
	}
	return tx.conn.execScriptWithArgs(ctx, tx, sql, newScriptArgsPos(args))
}

// ExecScriptNamed executes a multi-statement SQL script within tx, routing
// named parameters ($name) to the statements that reference them.
func (tx *Tx) ExecScriptNamed(ctx context.Context, sql string, args ...NamedArg) (ExecResult, error) {
	if tx == nil || tx.conn == nil {
		return ExecResult{}, ErrTxDone
	}
	sa, err := newScriptArgsNamed(args)
	if err != nil {
		return ExecResult{}, err
	}
	return tx.conn.execScriptWithArgs(ctx, tx, sql, sa)
}

// QueryScript executes leading non-row statements and streams row-returning
// statements within tx (additional result sets via Rows.NextResultSet).
func (tx *Tx) QueryScript(ctx context.Context, sql string, args ...Value) (*Rows, error) {
	if tx == nil || tx.conn == nil {
		return nil, ErrTxDone
	}
	return tx.conn.queryScriptWithArgs(ctx, tx, sql, newScriptArgsPos(args))
}

// QueryScriptNamed executes leading non-row statements and streams
// row-returning statements with named parameters ($name) within tx.
func (tx *Tx) QueryScriptNamed(ctx context.Context, sql string, args ...NamedArg) (*Rows, error) {
	if tx == nil || tx.conn == nil {
		return nil, ErrTxDone
	}
	sa, err := newScriptArgsNamed(args)
	if err != nil {
		return nil, err
	}
	return tx.conn.queryScriptWithArgs(ctx, tx, sql, sa)
}

// NewAppender opens an exclusive bulk Appender within tx.
func (tx *Tx) NewAppender(ctx context.Context, catalog, schema, table string) (*Appender, error) {
	if tx == nil || tx.conn == nil {
		return nil, ErrTxDone
	}
	return tx.conn.newAppenderWithTx(ctx, tx, catalog, schema, table)
}

// NewQueryAppender opens an exclusive query-backed bulk Appender within tx.
func (tx *Tx) NewQueryAppender(ctx context.Context, query string, cols []Column, tableName string) (*Appender, error) {
	if tx == nil || tx.conn == nil {
		return nil, ErrTxDone
	}
	return tx.conn.newQueryAppenderWithTx(ctx, tx, query, cols, tableName)
}

// QueryArrow starts a streaming Arrow C Data Interface query within tx.
func (tx *Tx) QueryArrow(ctx context.Context, sql string, args ...Value) (*ArrowRows, error) {
	if tx == nil || tx.conn == nil {
		return nil, ErrTxDone
	}
	return tx.conn.queryArrowWithBinder(ctx, tx, sql, func(s *Stmt) error { return s.bindLocked(args) })
}

// QueryArrowNamed starts a streaming Arrow C Data Interface query with named
// parameters within tx.
func (tx *Tx) QueryArrowNamed(ctx context.Context, sql string, args ...NamedArg) (*ArrowRows, error) {
	if tx == nil || tx.conn == nil {
		return nil, ErrTxDone
	}
	return tx.conn.queryArrowWithBinder(ctx, tx, sql, func(s *Stmt) error { return s.bindNamedLocked(args) })
}

// QueryScriptArrow executes leading non-row statements in sql sequentially and
// streams row-returning statements as an Arrow cursor within tx (additional
// result sets via ArrowRows.NextResultSet).
func (tx *Tx) QueryScriptArrow(ctx context.Context, sql string, args ...Value) (*ArrowRows, error) {
	if tx == nil || tx.conn == nil {
		return nil, ErrTxDone
	}
	return tx.conn.queryScriptArrowWithArgs(ctx, tx, sql, newScriptArgsPos(args))
}

// QueryScriptArrowNamed executes leading non-row statements in sql sequentially
// and streams row-returning statements as an Arrow cursor with named parameters
// within tx (additional result sets via ArrowRows.NextResultSet).
func (tx *Tx) QueryScriptArrowNamed(ctx context.Context, sql string, args ...NamedArg) (*ArrowRows, error) {
	if tx == nil || tx.conn == nil {
		return nil, ErrTxDone
	}
	sa, err := newScriptArgsNamed(args)
	if err != nil {
		return nil, err
	}
	return tx.conn.queryScriptArrowWithArgs(ctx, tx, sql, sa)
}
