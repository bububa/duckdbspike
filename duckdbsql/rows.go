package duckdbsql

import (
	"database/sql/driver"
	"io"
	"math"
	"reflect"

	"github.com/bububa/duckdbspike/duckdb"
)

type sqlRows struct {
	rows     *duckdb.Rows
	columns  []duckdb.Column
	buf      []duckdb.Value
	scanTime bool
}

var (
	_ driver.Rows                           = (*sqlRows)(nil)
	_ driver.RowsNextResultSet              = (*sqlRows)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*sqlRows)(nil)
	_ driver.RowsColumnTypeScanType         = (*sqlRows)(nil)
	_ driver.RowsColumnTypeLength           = (*sqlRows)(nil)
	_ driver.RowsColumnTypePrecisionScale   = (*sqlRows)(nil)
	_ driver.RowsColumnTypeNullable         = (*sqlRows)(nil)
)

func newSQLRows(rows *duckdb.Rows, scanTime bool) *sqlRows {
	return &sqlRows{
		rows:     rows,
		columns:  rows.Columns(),
		scanTime: scanTime,
	}
}

func (r *sqlRows) Columns() []string {
	names := make([]string, len(r.columns))
	for i, c := range r.columns {
		names[i] = c.Name
	}
	return names
}

func (r *sqlRows) Close() error {
	return r.rows.Close()
}

func (r *sqlRows) Next(dest []driver.Value) error {
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	var err error
	r.buf, err = r.rows.DecodeRow(r.buf)
	if err != nil {
		return err
	}
	for i := range dest {
		dv, err := fromDuckDBValue(r.buf[i], r.scanTime)
		if err != nil {
			return err
		}
		dest[i] = dv
	}
	return nil
}

func (r *sqlRows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.columns) {
		return ""
	}
	return r.columns[index].Type.String()
}

func (r *sqlRows) ColumnTypeScanType(index int) reflect.Type {
	if index < 0 || index >= len(r.columns) {
		return reflect.TypeFor[any]()
	}
	return scanTypeForLogicalType(r.columns[index].Type, r.scanTime)
}

func (r *sqlRows) ColumnTypeLength(index int) (length int64, ok bool) {
	if index < 0 || index >= len(r.columns) {
		return 0, false
	}
	switch r.columns[index].Type.Kind {
	case duckdb.TypeVarchar, duckdb.TypeBlob, duckdb.TypeBit, duckdb.TypeBignum, duckdb.TypeGeometry:
		return math.MaxInt64, true
	default:
		return 0, false
	}
}

func (r *sqlRows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if index < 0 || index >= len(r.columns) {
		return 0, 0, false
	}
	lt := r.columns[index].Type
	if lt.Kind == duckdb.TypeDecimal {
		return int64(lt.DecimalWidth), int64(lt.DecimalScale), true
	}
	return 0, 0, false
}

func (r *sqlRows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return true, true
}

func (r *sqlRows) HasNextResultSet() bool {
	return r.rows.HasNextResultSet()
}

func (r *sqlRows) NextResultSet() error {
	if !r.rows.NextResultSet() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	r.columns = r.rows.Columns()
	r.buf = r.buf[:0]
	return nil
}
