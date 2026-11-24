package duckdbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/bububa/duckdbspike/duckdb"
)

type sqlConn struct {
	conn           *duckdb.Conn
	ownedConnector *Connector
	scanTime       bool
	closed         atomic.Bool
}

var (
	_ driver.Conn               = (*sqlConn)(nil)
	_ driver.ConnPrepareContext = (*sqlConn)(nil)
	_ driver.ExecerContext      = (*sqlConn)(nil)
	_ driver.QueryerContext     = (*sqlConn)(nil)
	_ driver.ConnBeginTx        = (*sqlConn)(nil)
	_ driver.NamedValueChecker  = (*sqlConn)(nil)
	_ driver.Pinger             = (*sqlConn)(nil)
	_ driver.Validator          = (*sqlConn)(nil)
	_ driver.SessionResetter    = (*sqlConn)(nil)
)

func (c *sqlConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *sqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.closed.Load() {
		return nil, driver.ErrBadConn
	}
	stmt, err := c.conn.Prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	return &sqlStmt{stmt: stmt, scanTime: c.scanTime}, nil
}

func (c *sqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.closed.Load() {
		return nil, driver.ErrBadConn
	}
	pos, named, isNamed, err := convertArgs(args)
	if err != nil {
		return nil, err
	}
	var res duckdb.ExecResult
	switch {
	case isNamed:
		res, err = c.conn.ExecNamed(ctx, query, named...)
	case len(pos) == 0 && strings.ContainsRune(query, ';'):
		res, err = c.conn.ExecScript(ctx, query)
	default:
		res, err = c.conn.Exec(ctx, query, pos...)
	}
	if err != nil {
		return nil, err
	}
	return sqlResult{rowsAffected: res.RowsAffected}, nil
}

func (c *sqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.closed.Load() {
		return nil, driver.ErrBadConn
	}
	pos, named, isNamed, err := convertArgs(args)
	if err != nil {
		return nil, err
	}
	var rows *duckdb.Rows
	if isNamed {
		rows, err = c.conn.QueryNamed(ctx, query, named...)
	} else {
		rows, err = c.conn.Query(ctx, query, pos...)
	}
	if err != nil {
		return nil, err
	}
	return newSQLRows(rows, c.scanTime), nil
}

func (c *sqlConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *sqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.closed.Load() {
		return nil, driver.ErrBadConn
	}
	if sql.IsolationLevel(opts.Isolation) != sql.LevelDefault {
		return nil, fmt.Errorf("%w: unsupported transaction isolation level %v", duckdb.ErrInvalidInput, sql.IsolationLevel(opts.Isolation))
	}
	tx, err := c.conn.BeginTx(ctx, duckdb.TxOptions{ReadOnly: opts.ReadOnly})
	if err != nil {
		return nil, err
	}
	return &sqlTx{tx: tx}, nil
}

func (c *sqlConn) CheckNamedValue(nv *driver.NamedValue) error {
	_, err := toDuckDBValue(nv.Value)
	return err
}

func (c *sqlConn) Ping(ctx context.Context) error {
	if c.closed.Load() {
		return driver.ErrBadConn
	}
	rows, err := c.conn.Query(ctx, "SELECT 1")
	if err != nil {
		if errors.Is(err, duckdb.ErrClosed) {
			return driver.ErrBadConn
		}
		return err
	}
	return rows.Close()
}

func (c *sqlConn) IsValid() bool {
	return !c.closed.Load()
}

func (c *sqlConn) ResetSession(ctx context.Context) error {
	if c.closed.Load() {
		return driver.ErrBadConn
	}
	return ctx.Err()
}

func (c *sqlConn) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	err := c.conn.Close()
	if c.ownedConnector != nil {
		err = errors.Join(err, c.ownedConnector.Close())
	}
	return err
}

type sqlTx struct {
	tx *duckdb.Tx
}

func (t *sqlTx) Commit() error {
	return t.tx.Commit(context.Background())
}

func (t *sqlTx) Rollback() error {
	return t.tx.Rollback(context.Background())
}

type sqlResult struct {
	rowsAffected uint64
}

func (r sqlResult) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("%w: LastInsertId is not supported by DuckDB", duckdb.ErrUnsupportedType)
}

func (r sqlResult) RowsAffected() (int64, error) {
	return int64(r.rowsAffected), nil
}
