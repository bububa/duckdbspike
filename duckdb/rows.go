package duckdb

import (
	"fmt"
	"math"

	"github.com/bububa/duckdbspike/internal/abi"
)

// Column is copied result metadata. Columns are addressed by ordinal;
// duplicate names are preserved.
type Column struct {
	Name string
}

// Rows is an exclusive streaming cursor over one native result. It holds the
// connection lease until Close. Chunk memory is borrowed from the current
// native chunk and is never exposed beyond the next call to Next or Close.
type Rows struct {
	_            noCopy
	conn         *Conn
	stmt         *Stmt
	ownsStmt     bool
	result       abi.Result
	columns      []Column
	rowsAffected uint64
	chunk        uintptr
	size         uint64
	err          error
	done         bool
	closed       bool
}

// Columns returns a copy of the result columns. Metadata is copied when the
// result opens and remains readable after Close.
func (r *Rows) Columns() []Column {
	if r == nil {
		return nil
	}
	return append([]Column(nil), r.columns...)
}

// RowsAffected returns the row change count copied before the result closes.
func (r *Rows) RowsAffected() uint64 {
	if r == nil {
		return 0
	}
	return r.rowsAffected
}

// ChunkRowCount returns the number of rows in the current chunk, or 0 when no
// chunk is loaded or after Close.
func (r *Rows) ChunkRowCount() int {
	if r == nil {
		return 0
	}
	c := r.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.closed || r.chunk == 0 {
		return 0
	}
	return int(r.size)
}

// Next advances to the next non-empty native chunk. It returns false at end of
// stream, after an error (see Err), or after Close. A NULL native chunk is end
// of stream only when the result carries no error.
func (r *Rows) Next() bool {
	if r == nil {
		return false
	}
	c := r.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.closed {
		if r.err == nil && !r.done {
			r.err = ErrClosed
		}
		return false
	}
	if r.done || r.err != nil {
		return false
	}
	api := c.api
	for {
		if r.chunk != 0 {
			api.DestroyDataChunk(&r.chunk)
			r.size = 0
		}
		chunk := api.FetchChunk(r.result)
		if chunk == 0 {
			message := abi.GoString(api.ResultError(&r.result))
			errType := api.ResultErrorType(&r.result)
			if message != "" || errType != abi.ErrorInvalid {
				r.err = nativeError("fetch", errType, message)
			} else {
				r.done = true
			}
			return false
		}
		size := api.DataChunkGetSize(chunk)
		if size > math.MaxInt {
			api.DestroyDataChunk(&chunk)
			r.err = fmt.Errorf("%w: chunk size %d overflows int", ErrInvalidInput, size)
			return false
		}
		r.chunk = chunk
		r.size = size
		if r.size > 0 {
			return true
		}
	}
}

// Err returns the error that ended iteration, if any. Calling Next after Close
// records ErrClosed unless iteration had already ended.
func (r *Rows) Err() error {
	if r == nil {
		return ErrClosed
	}
	c := r.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	return r.err
}

// Close releases the chunk, the result, and an owned statement, then returns
// the connection lease. Close is idempotent.
func (r *Rows) Close() error {
	if r == nil {
		return nil
	}
	c := r.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	r.releaseLocked()
	return nil
}

func (r *Rows) releaseLocked() {
	if r.closed {
		return
	}
	r.closed = true
	api := r.conn.api
	if r.chunk != 0 {
		api.DestroyDataChunk(&r.chunk)
		r.size = 0
	}
	api.DestroyResult(&r.result)
	if r.conn.active == r {
		r.conn.active = nil
	}
	s := r.stmt
	s.rows = nil
	if r.ownsStmt {
		s.releaseLocked()
	}
}
