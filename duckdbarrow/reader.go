package duckdbarrow

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/bububa/duckdbspike/duckdb"
)

type recordReader struct {
	refCount atomic.Int64
	rows     *duckdb.ArrowRows
	alloc    memory.Allocator
	schema   *arrow.Schema
	cur      arrow.RecordBatch
	err      error
}

// ScriptRecordReader extends apache/arrow-go's array.RecordReader with
// multi-result-set navigation for multi-statement SQL scripts.
type ScriptRecordReader interface {
	array.RecordReader
	HasNextResultSet() bool
	NextResultSet() bool
}

var (
	_ array.RecordReader = (*recordReader)(nil)
	_ ScriptRecordReader = (*recordReader)(nil)
)

// NewRecordReader wraps a streaming *duckdb.ArrowRows in an apache/arrow-go
// array.RecordReader. Ownership of rows transfers to the returned RecordReader,
// which closes rows when its reference count drops to zero (or immediately if
// initialization fails).
func NewRecordReader(rows *duckdb.ArrowRows, alloc memory.Allocator) (array.RecordReader, error) {
	return NewScriptRecordReader(rows, alloc)
}

// NewScriptRecordReader wraps a streaming *duckdb.ArrowRows in a
// ScriptRecordReader supporting both array.RecordReader iteration and
// multi-result-set navigation (HasNextResultSet / NextResultSet). Ownership of
// rows transfers to the returned reader.
func NewScriptRecordReader(rows *duckdb.ArrowRows, alloc memory.Allocator) (ScriptRecordReader, error) {
	if rows == nil {
		return nil, fmt.Errorf("%w: nil ArrowRows", duckdb.ErrInvalidInput)
	}
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	rawSchema, err := rows.Schema()
	if err != nil {
		_ = rows.Close()
		return nil, err
	}
	schema, err := ImportSchema(rawSchema)
	rawSchema.Release()
	if err != nil {
		_ = rows.Close()
		return nil, err
	}
	r := &recordReader{
		rows:   rows,
		alloc:  alloc,
		schema: schema,
	}
	r.refCount.Store(1)
	return r, nil
}

// ArrowQuerier is satisfied by both *duckdb.Conn and *duckdb.Tx.
type ArrowQuerier interface {
	QueryArrow(ctx context.Context, sql string, args ...duckdb.Value) (*duckdb.ArrowRows, error)
	QueryArrowNamed(ctx context.Context, sql string, args ...duckdb.NamedArg) (*duckdb.ArrowRows, error)
}

// ArrowScriptQuerier is satisfied by both *duckdb.Conn and *duckdb.Tx.
type ArrowScriptQuerier interface {
	QueryScriptArrow(ctx context.Context, sql string, args ...duckdb.Value) (*duckdb.ArrowRows, error)
	QueryScriptArrowNamed(ctx context.Context, sql string, args ...duckdb.NamedArg) (*duckdb.ArrowRows, error)
}

// ArrowRegistrar is satisfied by both *duckdb.Conn and *duckdb.Tx.
type ArrowRegistrar interface {
	RegisterArrowView(ctx context.Context, name string, schema *duckdb.ArrowSchema, array *duckdb.ArrowArray) (*duckdb.ArrowView, error)
	RegisterArrowStream(ctx context.Context, name string, provider duckdb.ArrowStreamProvider) (*duckdb.ArrowView, error)
}

// Query executes a streaming DuckDB query on conn or tx and returns an
// apache/arrow-go array.RecordReader over the resulting record batches.
func Query(ctx context.Context, q ArrowQuerier, alloc memory.Allocator, sql string, args ...duckdb.Value) (array.RecordReader, error) {
	if q == nil {
		return nil, duckdb.ErrClosed
	}
	rows, err := q.QueryArrow(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return NewRecordReader(rows, alloc)
}

// QueryNamed executes a streaming DuckDB query with named parameters ($name)
// on conn or tx and returns an apache/arrow-go array.RecordReader.
func QueryNamed(ctx context.Context, q ArrowQuerier, alloc memory.Allocator, sql string, args ...duckdb.NamedArg) (array.RecordReader, error) {
	if q == nil {
		return nil, duckdb.ErrClosed
	}
	rows, err := q.QueryArrowNamed(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return NewRecordReader(rows, alloc)
}

// QueryScript executes leading non-row statements in sql and returns a
// ScriptRecordReader (which embeds array.RecordReader) streaming row-returning
// statements on conn or tx. Additional row-returning statements in sql can be
// consumed via HasNextResultSet and NextResultSet.
func QueryScript(ctx context.Context, q ArrowScriptQuerier, alloc memory.Allocator, sql string, args ...duckdb.Value) (ScriptRecordReader, error) {
	if q == nil {
		return nil, duckdb.ErrClosed
	}
	rows, err := q.QueryScriptArrow(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return NewScriptRecordReader(rows, alloc)
}

// QueryScriptNamed executes leading non-row statements in sql and returns a
// ScriptRecordReader (which embeds array.RecordReader) streaming row-returning
// statements with named parameters ($name) on conn or tx. Additional result
// sets can be consumed via HasNextResultSet and NextResultSet.
func QueryScriptNamed(ctx context.Context, q ArrowScriptQuerier, alloc memory.Allocator, sql string, args ...duckdb.NamedArg) (ScriptRecordReader, error) {
	if q == nil {
		return nil, duckdb.ErrClosed
	}
	rows, err := q.QueryScriptArrowNamed(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return NewScriptRecordReader(rows, alloc)
}

// QueryStmt executes a prepared *duckdb.Stmt and returns an apache/arrow-go
// array.RecordReader borrowing stmt until the reader is released.
func QueryStmt(ctx context.Context, stmt *duckdb.Stmt, alloc memory.Allocator, args ...duckdb.Value) (array.RecordReader, error) {
	if stmt == nil {
		return nil, duckdb.ErrClosed
	}
	rows, err := stmt.QueryArrow(ctx, args...)
	if err != nil {
		return nil, err
	}
	return NewRecordReader(rows, alloc)
}

// QueryStmtNamed executes a prepared *duckdb.Stmt with named parameters ($name)
// and returns an apache/arrow-go array.RecordReader borrowing stmt.
func QueryStmtNamed(ctx context.Context, stmt *duckdb.Stmt, alloc memory.Allocator, args ...duckdb.NamedArg) (array.RecordReader, error) {
	if stmt == nil {
		return nil, duckdb.ErrClosed
	}
	rows, err := stmt.QueryArrowNamed(ctx, args...)
	if err != nil {
		return nil, err
	}
	return NewRecordReader(rows, alloc)
}

// StmtSchema inspects the Arrow *arrow.Schema of a prepared *duckdb.Stmt
// without executing it.
func StmtSchema(stmt *duckdb.Stmt) (*arrow.Schema, error) {
	if stmt == nil {
		return nil, duckdb.ErrClosed
	}
	raw, err := stmt.ArrowSchema()
	if err != nil {
		return nil, err
	}
	defer raw.Release()
	return ImportSchema(raw)
}

func (r *recordReader) Retain() {
	r.refCount.Add(1)
}

func (r *recordReader) Release() {
	if r.refCount.Add(-1) == 0 {
		if r.cur != nil {
			r.cur.Release()
			r.cur = nil
		}
		if r.rows != nil {
			_ = r.rows.Close()
			r.rows = nil
		}
	}
}

func (r *recordReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *recordReader) Next() bool {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if r.err != nil || r.rows == nil {
		return false
	}
	arr, err := r.rows.Next()
	if err != nil {
		r.err = err
		return false
	}
	if arr == nil {
		return false
	}
	rec, err := ImportRecordBatch(r.schema, arr, r.alloc)
	arr.Release()
	if err != nil {
		r.err = err
		return false
	}
	r.cur = rec
	return true
}

func (r *recordReader) RecordBatch() arrow.RecordBatch {
	return r.cur
}

func (r *recordReader) Record() arrow.RecordBatch {
	return r.cur
}

func (r *recordReader) Err() error {
	return r.err
}

func (r *recordReader) HasNextResultSet() bool {
	if r.rows == nil || r.err != nil {
		return false
	}
	return r.rows.HasNextResultSet()
}

func (r *recordReader) NextResultSet() bool {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if r.rows == nil || r.err != nil {
		return false
	}
	if !r.rows.NextResultSet() {
		if err := r.rows.Err(); err != nil {
			r.err = err
		}
		return false
	}
	rawSchema, err := r.rows.Schema()
	if err != nil {
		r.err = err
		return false
	}
	schema, err := ImportSchema(rawSchema)
	rawSchema.Release()
	if err != nil {
		r.err = err
		return false
	}
	r.schema = schema
	return true
}
