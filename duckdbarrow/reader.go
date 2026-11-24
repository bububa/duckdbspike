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

var _ array.RecordReader = (*recordReader)(nil)

// NewRecordReader wraps a streaming *duckdb.ArrowRows in an apache/arrow-go
// array.RecordReader. Ownership of rows transfers to the returned RecordReader,
// which closes rows when its reference count drops to zero (or immediately if
// initialization fails).
func NewRecordReader(rows *duckdb.ArrowRows, alloc memory.Allocator) (array.RecordReader, error) {
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

// Query executes a streaming DuckDB query and returns an apache/arrow-go
// array.RecordReader over the resulting record batches.
func Query(ctx context.Context, conn *duckdb.Conn, alloc memory.Allocator, sql string, args ...duckdb.Value) (array.RecordReader, error) {
	if conn == nil {
		return nil, duckdb.ErrClosed
	}
	rows, err := conn.QueryArrow(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return NewRecordReader(rows, alloc)
}

// QueryNamed executes a streaming DuckDB query with named parameters ($name)
// and returns an apache/arrow-go array.RecordReader.
func QueryNamed(ctx context.Context, conn *duckdb.Conn, alloc memory.Allocator, sql string, args ...duckdb.NamedArg) (array.RecordReader, error) {
	if conn == nil {
		return nil, duckdb.ErrClosed
	}
	rows, err := conn.QueryArrowNamed(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return NewRecordReader(rows, alloc)
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
