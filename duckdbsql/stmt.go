package duckdbsql

import (
	"context"
	"database/sql/driver"

	"github.com/bububa/duckdbspike/duckdb"
)

type sqlStmt struct {
	stmt     *duckdb.Stmt
	scanTime bool
}

var (
	_ driver.Stmt              = (*sqlStmt)(nil)
	_ driver.StmtExecContext   = (*sqlStmt)(nil)
	_ driver.StmtQueryContext  = (*sqlStmt)(nil)
	_ driver.NamedValueChecker = (*sqlStmt)(nil)
)

func (s *sqlStmt) Close() error {
	return s.stmt.Close()
}

func (s *sqlStmt) NumInput() int {
	n, err := s.stmt.ParamCount()
	if err != nil {
		return -1
	}
	return n
}

func (s *sqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	nv := make([]driver.NamedValue, len(args))
	for i, a := range args {
		nv[i] = driver.NamedValue{Ordinal: i + 1, Value: a}
	}
	return s.ExecContext(context.Background(), nv)
}

func (s *sqlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	pos, named, isNamed, err := convertArgs(args)
	if err != nil {
		return nil, err
	}
	var res duckdb.ExecResult
	if isNamed {
		res, err = s.stmt.ExecNamed(ctx, named...)
	} else {
		res, err = s.stmt.Exec(ctx, pos...)
	}
	if err != nil {
		return nil, err
	}
	return sqlResult{rowsAffected: res.RowsAffected}, nil
}

func (s *sqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	nv := make([]driver.NamedValue, len(args))
	for i, a := range args {
		nv[i] = driver.NamedValue{Ordinal: i + 1, Value: a}
	}
	return s.QueryContext(context.Background(), nv)
}

func (s *sqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	pos, named, isNamed, err := convertArgs(args)
	if err != nil {
		return nil, err
	}
	var rows *duckdb.Rows
	if isNamed {
		rows, err = s.stmt.QueryNamed(ctx, named...)
	} else {
		rows, err = s.stmt.Query(ctx, pos...)
	}
	if err != nil {
		return nil, err
	}
	return newSQLRows(rows, s.scanTime), nil
}

func (s *sqlStmt) CheckNamedValue(nv *driver.NamedValue) error {
	_, err := toDuckDBValue(nv.Value)
	return err
}
