package duckdb

import (
	"context"
	"fmt"
	"math"
	"runtime"

	"github.com/bububa/duckdbspike/internal/abi"
)

// Appender streams bulk rows directly into a DuckDB table without SQL parsing
// overhead. An open Appender holds an exclusive lease on its Conn (returning
// ErrBusy to concurrent operations) until Close. Callers must call Close to
// flush remaining buffered rows and release native resources.
type Appender struct {
	_       noCopy
	conn    *Conn
	handle  uintptr
	columns []Column
	closed  bool
}

// NewAppender opens an exclusive bulk Appender targeting catalog.schema.table.
// Empty catalog or schema selects DuckDB's default catalog/schema.
func (c *Conn) NewAppender(ctx context.Context, catalog, schema, table string) (*Appender, error) {
	return c.newAppenderWithTx(ctx, nil, catalog, schema, table)
}

func (c *Conn) newAppenderWithTx(ctx context.Context, tx *Tx, catalog, schema, table string) (*Appender, error) {
	if c == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if table == "" {
		return nil, fmt.Errorf("%w: empty table name", ErrInvalidInput)
	}
	var ccat, cschema, ctable []byte
	var err error
	var catPtr, schemaPtr *byte
	if catalog != "" {
		ccat, err = abi.CString(catalog)
		if err != nil {
			return nil, fmt.Errorf("%w: catalog: %v", ErrInvalidInput, err)
		}
		catPtr = &ccat[0]
	}
	if schema != "" {
		cschema, err = abi.CString(schema)
		if err != nil {
			return nil, fmt.Errorf("%w: schema: %v", ErrInvalidInput, err)
		}
		schemaPtr = &cschema[0]
	}
	ctable, err = abi.CString(table)
	if err != nil {
		return nil, fmt.Errorf("%w: table: %v", ErrInvalidInput, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkTxLocked(tx); err != nil {
		return nil, err
	}
	if err := c.usableLocked(); err != nil {
		return nil, err
	}

	api := c.api
	var handle uintptr
	state := api.AppenderCreateExt(c.handle, catPtr, schemaPtr, &ctable[0], &handle)
	runtime.KeepAlive(ccat)
	runtime.KeepAlive(cschema)
	runtime.KeepAlive(ctable)
	if state != abi.StateSuccess {
		var appErr *Error
		if handle != 0 {
			appErr = consumeErrorData(api, "appender_create", api.AppenderErrorData(handle))
			_ = api.AppenderDestroy(&handle)
		}
		if appErr == nil {
			appErr = nativeError("appender_create", abi.ErrorInvalid, "")
		}
		return nil, appErr
	}

	app := &Appender{conn: c, handle: handle}
	if err := app.refreshColumnsLocked(); err != nil {
		_ = api.AppenderDestroy(&app.handle)
		return nil, err
	}
	c.activeAppender = app
	return app, nil
}

func (a *Appender) refreshColumnsLocked() error {
	api := a.conn.api
	n := api.AppenderColumnCount(a.handle)
	if n > math.MaxInt {
		return fmt.Errorf("%w: appender column count %d overflows int", ErrInvalidInput, n)
	}
	cols := make([]Column, int(n))
	for i := uint64(0); i < n; i++ {
		lt := api.AppenderColumnType(a.handle, i)
		plan, err := buildColumnPlan(api, lt, 0, a.conn.limits)
		api.DestroyLogicalType(&lt)
		if err != nil {
			return fmt.Errorf("appender column %d: %w", i, err)
		}
		cols[i] = Column{Type: plan.lt}
	}
	a.columns = cols
	return nil
}

func (a *Appender) errorLocked(op string) error {
	if err := consumeErrorData(a.conn.api, op, a.conn.api.AppenderErrorData(a.handle)); err != nil {
		return err
	}
	return nativeError(op, abi.ErrorInvalid, "")
}

// Columns returns a copy of the active column types expected by AppendRow.
func (a *Appender) Columns() []Column {
	if a == nil {
		return nil
	}
	a.conn.mu.Lock()
	defer a.conn.mu.Unlock()
	return append([]Column(nil), a.columns...)
}

// AddColumn restricts subsequent AppendRow calls to the explicitly added column
// list (in the order added). Unspecified columns receive their SQL DEFAULT.
func (a *Appender) AddColumn(name string) error {
	if a == nil {
		return ErrClosed
	}
	if name == "" {
		return fmt.Errorf("%w: empty column name", ErrInvalidInput)
	}
	cname, err := abi.CString(name)
	if err != nil {
		return fmt.Errorf("%w: column name: %v", ErrInvalidInput, err)
	}
	c := a.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if a.closed || c.closed {
		return ErrClosed
	}
	state := c.api.AppenderAddColumn(a.handle, &cname[0])
	runtime.KeepAlive(cname)
	if state != abi.StateSuccess {
		return a.errorLocked("appender_add_column")
	}
	return a.refreshColumnsLocked()
}

// ClearColumns resets the active column list back to all table columns.
func (a *Appender) ClearColumns() error {
	if a == nil {
		return ErrClosed
	}
	c := a.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if a.closed || c.closed {
		return ErrClosed
	}
	if c.api.AppenderClearColumns(a.handle) != abi.StateSuccess {
		return a.errorLocked("appender_clear_columns")
	}
	return a.refreshColumnsLocked()
}

// AppendRow appends one complete row to the appender buffer. Use NullValue()
// for SQL NULL and DefaultValue() to evaluate the column's SQL DEFAULT.
func (a *Appender) AppendRow(vals ...Value) error {
	if a == nil {
		return ErrClosed
	}
	c := a.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if a.closed || c.closed {
		return ErrClosed
	}
	if len(vals) != len(a.columns) {
		return fmt.Errorf("%w: appender expects %d values per row, got %d", ErrInvalidInput, len(a.columns), len(vals))
	}
	for i, v := range vals {
		if err := a.appendOneLocked(i, v); err != nil {
			return err
		}
	}
	if c.api.AppenderEndRow(a.handle) != abi.StateSuccess {
		return a.errorLocked("appender_end_row")
	}
	return nil
}

func (a *Appender) appendOneLocked(colIdx int, v Value) error {
	api := a.conn.api
	var state int32
	switch {
	case v.IsDefault():
		state = api.AppendDefault(a.handle)
	case v.IsNull():
		state = api.AppendNull(a.handle)
	default:
		switch v.Kind() {
		case TypeBoolean:
			state = api.AppendBool(a.handle, v.lo != 0)
		case TypeTinyint:
			state = api.AppendInt8(a.handle, int8(int64(v.lo)))
		case TypeSmallint:
			state = api.AppendInt16(a.handle, int16(int64(v.lo)))
		case TypeInteger:
			state = api.AppendInt32(a.handle, int32(int64(v.lo)))
		case TypeBigint:
			state = api.AppendInt64(a.handle, int64(v.lo))
		case TypeUTinyint:
			state = api.AppendUint8(a.handle, uint8(v.lo))
		case TypeUSmallint:
			state = api.AppendUint16(a.handle, uint16(v.lo))
		case TypeUInteger:
			state = api.AppendUint32(a.handle, uint32(v.lo))
		case TypeUBigint:
			state = api.AppendUint64(a.handle, v.lo)
		case TypeHugeint:
			state = api.AppendHugeint(a.handle, abi.Hugeint{Lower: v.lo, Upper: int64(v.hi)})
		case TypeUhugeint:
			state = api.AppendUhugeint(a.handle, abi.Uhugeint{Lower: v.lo, Upper: v.hi})
		case TypeFloat:
			state = api.AppendFloat(a.handle, math.Float32frombits(uint32(v.lo)))
		case TypeDouble:
			state = api.AppendDouble(a.handle, math.Float64frombits(v.lo))
		case TypeDate:
			state = api.AppendDate(a.handle, abi.Date{Days: int32(int64(v.lo))})
		case TypeTime:
			state = api.AppendTime(a.handle, abi.Time{Micros: int64(v.lo)})
		case TypeTimestamp:
			state = api.AppendTimestamp(a.handle, abi.Timestamp{Micros: int64(v.lo)})
		case TypeInterval:
			state = api.AppendInterval(a.handle, abi.Interval{
				Months: int32(uint32(v.hi >> 32)),
				Days:   int32(uint32(v.hi)),
				Micros: int64(v.lo),
			})
		case TypeVarchar:
			str, _ := v.ref.(string)
			if len(str) == 0 {
				state = api.AppendVarcharLength(a.handle, &emptyBindByte, 0)
			} else {
				b := []byte(str)
				state = api.AppendVarcharLength(a.handle, &b[0], uint64(len(b)))
				runtime.KeepAlive(b)
			}
		case TypeBlob:
			blob, _ := v.ref.([]byte)
			if len(blob) == 0 {
				state = api.AppendBlob(a.handle, &emptyBindByte, 0)
			} else {
				state = api.AppendBlob(a.handle, &blob[0], uint64(len(blob)))
				runtime.KeepAlive(blob)
			}
		case TypeDecimal, TypeTimeNS, TypeTimeTZ,
			TypeTimestampS, TypeTimestampMS, TypeTimestampNS, TypeTimestampTZ,
			TypeUUID, TypeEnum, TypeList, TypeArray, TypeStruct, TypeMap:
			nv, err := createNativeValue(api, v, 0, a.conn.limits)
			if err != nil {
				return fmt.Errorf("appender col[%d]: %w", colIdx, err)
			}
			state = api.AppendValue(a.handle, nv)
			api.DestroyValue(&nv)
		default:
			return fmt.Errorf("%w: appender col[%d] unsupported type %s", ErrUnsupportedType, colIdx, v.Kind())
		}
	}
	if state != abi.StateSuccess {
		return a.errorLocked(fmt.Sprintf("append[%d]", colIdx))
	}
	return nil
}

// AppendArrowChunk ingests an Arrow C Data Interface chunk directly into the
// appender via duckdb_schema_from_arrow, duckdb_data_chunk_from_arrow, and
// duckdb_append_data_chunk. On success, ownership of array's native buffers
// transfers to DuckDB and array is marked released; schema remains owned by
// the caller to Release.
func (a *Appender) AppendArrowChunk(schema *ArrowSchema, array *ArrowArray) error {
	if a == nil {
		return ErrClosed
	}
	if schema == nil || schema.Released() || array == nil || array.Released() {
		return fmt.Errorf("%w: nil or already-released Arrow schema/array", ErrInvalidInput)
	}
	c := a.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if a.closed || c.closed {
		return ErrClosed
	}
	api := c.api
	var converted uintptr
	if err := consumeErrorData(api, "schema_from_arrow", api.SchemaFromArrow(c.handle, schema.raw, &converted)); err != nil {
		if converted != 0 {
			api.DestroyArrowConvertedSchema(&converted)
		}
		return err
	}
	defer api.DestroyArrowConvertedSchema(&converted)

	var chunk uintptr
	if err := consumeErrorData(api, "data_chunk_from_arrow", api.DataChunkFromArrow(c.handle, array.raw, converted, &chunk)); err != nil {
		if chunk != 0 {
			api.DestroyDataChunk(&chunk)
		}
		return err
	}
	// Ownership of array's native data has transferred to DuckDB's DataChunk.
	array.markTransferred()
	defer api.DestroyDataChunk(&chunk)

	if api.AppendDataChunk(a.handle, chunk) != abi.StateSuccess {
		return a.errorLocked("append_data_chunk")
	}
	return nil
}

// Flush flushes buffered rows to the target table.
func (a *Appender) Flush() error {
	if a == nil {
		return ErrClosed
	}
	c := a.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if a.closed || c.closed {
		return ErrClosed
	}
	if c.api.AppenderFlush(a.handle) != abi.StateSuccess {
		return a.errorLocked("appender_flush")
	}
	return nil
}

// Clear discards all unflushed buffered rows without closing the appender.
func (a *Appender) Clear() error {
	if a == nil {
		return ErrClosed
	}
	c := a.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if a.closed || c.closed {
		return ErrClosed
	}
	if c.api.AppenderClear(a.handle) != abi.StateSuccess {
		return a.errorLocked("appender_clear")
	}
	return nil
}

// Close flushes remaining rows, captures any deferred close error, destroys
// the native appender handle, and releases the connection lease. Idempotent.
func (a *Appender) Close() error {
	if a == nil {
		return nil
	}
	c := a.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	return a.releaseLocked()
}

func (a *Appender) releaseLocked() error {
	if a.closed {
		return nil
	}
	a.closed = true
	api := a.conn.api
	var closeErr error
	if a.handle != 0 {
		if api.AppenderClose(a.handle) != abi.StateSuccess {
			closeErr = a.errorLocked("appender_close")
		}
		if api.AppenderDestroy(&a.handle) != abi.StateSuccess && closeErr == nil {
			closeErr = nativeError("appender_destroy", abi.ErrorInvalid, "")
		}
	}
	if a.conn.activeAppender == a {
		a.conn.activeAppender = nil
	}
	return closeErr
}
