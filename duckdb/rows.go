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
	Type LogicalType
}

// Rows is an exclusive streaming cursor over one native result. It holds the
// connection lease and the query's cancellation scope until Close. At most one
// native chunk is alive at a time; chunk memory is never exposed to callers,
// and every decoded Value owns its Go data and survives subsequent Next and
// Close calls.
type Rows struct {
	_            noCopy
	conn         *Conn
	stmt         *Stmt
	scope        *cancelScope
	ownsStmt     bool
	result       abi.Result
	columns      []Column
	plans        []columnPlan
	limits       DecodeLimits
	resultLimits ResultLimits
	rowsAffected uint64
	rowsRead     uint64
	bytesRead    uint64
	chunk        uintptr
	chunkSize    uint64
	rowIdx       uint64
	hasRow       bool
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

// Next advances the cursor by one row, streaming one native chunk at a time.
// It returns false at end of stream, on cancellation or error (see Err), or
// after Close. A NULL native chunk is end of stream only when the result
// carries no error AND clean completion wins the linearization point against
// cancellation.
func (r *Rows) Next() bool {
	if r == nil {
		return false
	}
	c := r.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.closed {
		r.hasRow = false
		if r.err == nil && !r.done {
			r.err = ErrClosed
		}
		return false
	}
	if r.done || r.err != nil {
		r.hasRow = false
		return false
	}
	if cerr := r.scope.err(); cerr != nil {
		r.hasRow = false
		r.err = cerr
		return false
	}
	if r.hasRow && r.rowIdx+1 < r.chunkSize {
		if r.resultLimits.MaxRows > 0 && r.rowsRead >= r.resultLimits.MaxRows {
			r.hasRow = false
			r.err = fmt.Errorf("%w: result row count exceeds limit %d", ErrBudgetExceeded, r.resultLimits.MaxRows)
			return false
		}
		r.rowIdx++
		r.rowsRead++
		return true
	}
	api := c.api
	for {
		if r.chunk != 0 {
			api.DestroyDataChunk(&r.chunk)
			r.chunkSize = 0
			r.rowIdx = 0
			r.hasRow = false
		}
		if cerr := r.scope.err(); cerr != nil {
			r.err = cerr
			return false
		}
		chunk := api.FetchChunk(r.result)
		if chunk == 0 {
			message := abi.GoString(api.ResultError(&r.result))
			errType := api.ResultErrorType(&r.result)
			if cerr := r.scope.err(); cerr != nil {
				r.err = cerr
				return false
			}
			if message != "" || errType != abi.ErrorInvalid {
				r.err = nativeError("fetch", errType, message)
				return false
			}
			if !r.scope.tryFinish() {
				r.err = r.scope.err()
				return false
			}
			r.done = true
			return false
		}
		size := api.DataChunkGetSize(chunk)
		if size > math.MaxInt {
			api.DestroyDataChunk(&chunk)
			r.err = fmt.Errorf("%w: chunk size %d overflows int", ErrLimitExceeded, size)
			return false
		}
		if cerr := r.scope.err(); cerr != nil {
			api.DestroyDataChunk(&chunk)
			r.err = cerr
			return false
		}
		r.chunk = chunk
		r.chunkSize = size
		if size > 0 {
			if r.resultLimits.MaxRows > 0 && r.rowsRead >= r.resultLimits.MaxRows {
				r.err = fmt.Errorf("%w: result row count exceeds limit %d", ErrBudgetExceeded, r.resultLimits.MaxRows)
				return false
			}
			r.rowIdx = 0
			r.hasRow = true
			r.rowsRead++
			return true
		}
	}
}

// DecodeRow decodes every column of the current row into dst (reusing dst's
// backing array when capacity permits). Returned Values own their data and
// remain valid after Next and Close.
func (r *Rows) DecodeRow(dst []Value) ([]Value, error) {
	if r == nil {
		return nil, ErrClosed
	}
	c := r.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.closed {
		return nil, ErrClosed
	}
	if r.err != nil {
		return nil, r.err
	}
	if cerr := r.scope.err(); cerr != nil {
		r.err = cerr
		return nil, cerr
	}
	if !r.hasRow || r.chunk == 0 {
		return nil, fmt.Errorf("%w: DecodeRow called without an active row", ErrInvalidInput)
	}
	ncol := len(r.plans)
	if cap(dst) < ncol {
		dst = make([]Value, ncol)
	} else {
		dst = dst[:ncol]
	}
	api := c.api
	var rowBytes uint64
	for i := 0; i < ncol; i++ {
		vec := api.DataChunkGetVector(r.chunk, uint64(i))
		v, err := decodeVectorValue(api, &r.plans[i], vec, r.rowIdx, r.limits)
		if err != nil {
			r.err = fmt.Errorf("column %d (%q): %w", i, r.columns[i].Name, err)
			return nil, r.err
		}
		if r.resultLimits.MaxBytes > 0 {
			vb := valueBytes(v)
			if r.bytesRead+rowBytes+vb < r.bytesRead || r.bytesRead+rowBytes+vb > r.resultLimits.MaxBytes {
				r.err = fmt.Errorf("%w: decoded result bytes exceed limit %d", ErrBudgetExceeded, r.resultLimits.MaxBytes)
				return nil, r.err
			}
			rowBytes += vb
		}
		dst[i] = v
	}
	r.bytesRead += rowBytes
	if cerr := r.scope.err(); cerr != nil {
		r.err = cerr
		return nil, cerr
	}
	return dst, nil
}

// ReadBatch decodes up to maxRows rows into caller-owned slices without
// prefetching the entire result. Returning (batch, nil) with len(batch) == 0
// signals clean end of stream.
func (r *Rows) ReadBatch(maxRows int, dst [][]Value) ([][]Value, error) {
	if maxRows <= 0 {
		return nil, fmt.Errorf("%w: maxRows must be positive", ErrInvalidInput)
	}
	batch := dst[:0]
	for len(batch) < maxRows {
		if !r.Next() {
			if err := r.Err(); err != nil {
				return nil, err
			}
			return batch, nil
		}
		var rowBuf []Value
		if len(batch) < cap(batch) {
			rowBuf = batch[:len(batch)+1][len(batch)]
		}
		row, err := r.DecodeRow(rowBuf)
		if err != nil {
			return nil, err
		}
		batch = append(batch, row)
	}
	return batch, nil
}

// ColumnValue decodes a single column of the current row by zero-based ordinal.
func (r *Rows) ColumnValue(col int) (Value, error) {
	if r == nil {
		return Value{}, ErrClosed
	}
	c := r.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.closed {
		return Value{}, ErrClosed
	}
	if r.err != nil {
		return Value{}, r.err
	}
	if cerr := r.scope.err(); cerr != nil {
		r.err = cerr
		return Value{}, cerr
	}
	if !r.hasRow || r.chunk == 0 {
		return Value{}, fmt.Errorf("%w: ColumnValue called without an active row", ErrInvalidInput)
	}
	if col < 0 || col >= len(r.plans) {
		return Value{}, fmt.Errorf("%w: column ordinal %d out of range [0,%d)", ErrOutOfRange, col, len(r.plans))
	}
	vec := c.api.DataChunkGetVector(r.chunk, uint64(col))
	v, err := decodeVectorValue(c.api, &r.plans[col], vec, r.rowIdx, r.limits)
	if err != nil {
		return Value{}, fmt.Errorf("column %d (%q): %w", col, r.columns[col].Name, err)
	}
	if cerr := r.scope.err(); cerr != nil {
		r.err = cerr
		return Value{}, cerr
	}
	return v, nil
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

// Close interrupts any in-flight FetchChunk on this Rows, joins the native
// call and its cancellation executor without a deadline, releases the chunk,
// result, and owned statement, and returns the connection lease. Close is
// idempotent.
func (r *Rows) Close() error {
	if r == nil {
		return nil
	}
	c := r.conn
	if !c.mu.TryLock() {
		r.scope.cancel(ErrClosed)
		c.mu.Lock()
	}
	defer c.mu.Unlock()
	r.releaseLocked()
	return nil
}

func (r *Rows) releaseLocked() {
	if r.closed {
		return
	}
	r.closed = true
	r.hasRow = false
	r.conn.endScopeLocked(r.scope)
	api := r.conn.api
	if r.chunk != 0 {
		api.DestroyDataChunk(&r.chunk)
		r.chunkSize = 0
		r.rowIdx = 0
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
