package duckdb

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

// ArrowSchemaView is a read-only borrowed view over a C Data Interface
// ArrowSchema node. Valid only while its owning ArrowSchema has not been
// released or detached.
type ArrowSchemaView struct {
	raw *abi.ArrowSchema
}

// Valid reports whether v points to a non-null ArrowSchema node.
func (v ArrowSchemaView) Valid() bool { return v.raw != nil }

// Format returns the Arrow C Data Interface format string (for example "l",
// "u", "d:18,4", "+s", "+l").
func (v ArrowSchemaView) Format() string {
	if v.raw == nil {
		return ""
	}
	return abi.GoString(v.raw.Format)
}

// Name returns the Arrow field name.
func (v ArrowSchemaView) Name() string {
	if v.raw == nil {
		return ""
	}
	return abi.GoString(v.raw.Name)
}

// Flags returns the ArrowSchema flags bitmask.
func (v ArrowSchemaView) Flags() int64 {
	if v.raw == nil {
		return 0
	}
	return v.raw.Flags
}

// MetadataBytes returns a Go-owned copy of the raw Arrow C Data Interface
// metadata buffer (length-prefixed int32 key/value pairs), or nil if no
// metadata is attached.
func (v ArrowSchemaView) MetadataBytes() []byte {
	if v.raw == nil || v.raw.Metadata == 0 {
		return nil
	}
	const maxMetadataBytes = 16 << 20
	base := abi.Pointer(v.raw.Metadata)
	count := *(*int32)(base)
	if count <= 0 || count > 1<<20 {
		return nil
	}
	off := uintptr(4)
	for i := int32(0); i < count; i++ {
		if off+4 > maxMetadataBytes {
			return nil
		}
		klen := *(*int32)(unsafe.Add(base, off))
		if klen < 0 || uintptr(klen) > maxMetadataBytes-off-4 {
			return nil
		}
		off += 4 + uintptr(klen)
		if off+4 > maxMetadataBytes {
			return nil
		}
		vlen := *(*int32)(unsafe.Add(base, off))
		if vlen < 0 || uintptr(vlen) > maxMetadataBytes-off-4 {
			return nil
		}
		off += 4 + uintptr(vlen)
	}
	out := make([]byte, int(off))
	copy(out, unsafe.Slice((*byte)(base), int(off)))
	return out
}

// ChildCount returns the number of child schema nodes.
func (v ArrowSchemaView) ChildCount() int {
	if v.raw == nil || v.raw.NChildren <= 0 {
		return 0
	}
	return int(v.raw.NChildren)
}

// Child returns a borrowed view of the i-th child schema node.
func (v ArrowSchemaView) Child(i int) ArrowSchemaView {
	if v.raw == nil || i < 0 || int64(i) >= v.raw.NChildren || v.raw.Children == 0 {
		return ArrowSchemaView{}
	}
	childPtr := *(*uintptr)(unsafe.Add(abi.Pointer(v.raw.Children), uintptr(i)*unsafe.Sizeof(uintptr(0))))
	if childPtr == 0 {
		return ArrowSchemaView{}
	}
	return ArrowSchemaView{raw: (*abi.ArrowSchema)(abi.Pointer(childPtr))}
}

// Dictionary returns a borrowed view of the dictionary schema, if present.
func (v ArrowSchemaView) Dictionary() ArrowSchemaView {
	if v.raw == nil || v.raw.Dictionary == 0 {
		return ArrowSchemaView{}
	}
	return ArrowSchemaView{raw: (*abi.ArrowSchema)(abi.Pointer(v.raw.Dictionary))}
}

// ArrowSchema owns a native Apache Arrow C Data Interface struct ArrowSchema.
// Call Release when finished, or Detach to transfer ownership to an external
// C Data Interface consumer.
type ArrowSchema struct {
	_        noCopy
	mu       sync.Mutex
	raw      *abi.ArrowSchema
	released bool
	release  func(*abi.ArrowSchema)
}

// View returns a borrowed read-only view of the root schema node.
func (s *ArrowSchema) View() ArrowSchemaView {
	if s == nil {
		return ArrowSchemaView{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released || s.raw == nil {
		return ArrowSchemaView{}
	}
	return ArrowSchemaView{raw: s.raw}
}

// Ptr returns the raw *ArrowSchema pointer for C Data Interface interop while
// retaining ownership in s. Returns nil if s has been released or detached.
func (s *ArrowSchema) Ptr() unsafe.Pointer {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released || s.raw == nil {
		return nil
	}
	return unsafe.Pointer(s.raw)
}

// Detach transfers ownership of the underlying C ArrowSchema to the caller and
// marks s released so subsequent Release calls become no-ops.
func (s *ArrowSchema) Detach() unsafe.Pointer {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released || s.raw == nil {
		return nil
	}
	ptr := unsafe.Pointer(s.raw)
	s.raw = nil
	s.released = true
	return ptr
}

// Released reports whether s has been released or detached.
func (s *ArrowSchema) Released() bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.released
}

func (s *ArrowSchema) takeOwnership() (*abi.ArrowSchema, func(*abi.ArrowSchema)) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released || s.raw == nil {
		return nil, nil
	}
	raw, rel := s.raw, s.release
	s.raw = nil
	s.release = nil
	s.released = true
	return raw, rel
}

// Release invokes the native ArrowSchema release callback. Idempotent.
func (s *ArrowSchema) Release() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.released {
		s.mu.Unlock()
		return
	}
	raw := s.raw
	rel := s.release
	s.raw = nil
	s.released = true
	s.mu.Unlock()
	if raw != nil {
		if rel != nil {
			rel(raw)
		} else {
			abi.ReleaseArrowSchema(raw)
		}
	}
}

// ArrowArrayView is a read-only borrowed view over a C Data Interface
// ArrowArray node. Valid only while its owning ArrowArray has not been
// released or detached.
type ArrowArrayView struct {
	raw *abi.ArrowArray
}

// Valid reports whether v points to a non-null ArrowArray node.
func (v ArrowArrayView) Valid() bool { return v.raw != nil }

// Length returns the logical element/row count of the array.
func (v ArrowArrayView) Length() int64 {
	if v.raw == nil {
		return 0
	}
	return v.raw.Length
}

// NullCount returns the number of null entries (-1 if unknown).
func (v ArrowArrayView) NullCount() int64 {
	if v.raw == nil {
		return 0
	}
	return v.raw.NullCount
}

// Offset returns the logical start offset into the array buffers.
func (v ArrowArrayView) Offset() int64 {
	if v.raw == nil {
		return 0
	}
	return v.raw.Offset
}

// BufferCount returns the number of native buffers attached to the array.
func (v ArrowArrayView) BufferCount() int {
	if v.raw == nil || v.raw.NBuffers <= 0 {
		return 0
	}
	return int(v.raw.NBuffers)
}

// Buffer returns the raw pointer to the i-th native buffer (may be nil for
// all-valid validity bitmaps or zero-length data buffers).
func (v ArrowArrayView) Buffer(i int) unsafe.Pointer {
	if v.raw == nil || i < 0 || int64(i) >= v.raw.NBuffers || v.raw.Buffers == 0 {
		return nil
	}
	p := *(*uintptr)(unsafe.Add(abi.Pointer(v.raw.Buffers), uintptr(i)*unsafe.Sizeof(uintptr(0))))
	if p == 0 {
		return nil
	}
	return abi.Pointer(p)
}

// ChildCount returns the number of child array nodes.
func (v ArrowArrayView) ChildCount() int {
	if v.raw == nil || v.raw.NChildren <= 0 {
		return 0
	}
	return int(v.raw.NChildren)
}

// Child returns a borrowed view of the i-th child array node.
func (v ArrowArrayView) Child(i int) ArrowArrayView {
	if v.raw == nil || i < 0 || int64(i) >= v.raw.NChildren || v.raw.Children == 0 {
		return ArrowArrayView{}
	}
	childPtr := *(*uintptr)(unsafe.Add(abi.Pointer(v.raw.Children), uintptr(i)*unsafe.Sizeof(uintptr(0))))
	if childPtr == 0 {
		return ArrowArrayView{}
	}
	return ArrowArrayView{raw: (*abi.ArrowArray)(abi.Pointer(childPtr))}
}

// Dictionary returns a borrowed view of the dictionary array, if present.
func (v ArrowArrayView) Dictionary() ArrowArrayView {
	if v.raw == nil || v.raw.Dictionary == 0 {
		return ArrowArrayView{}
	}
	return ArrowArrayView{raw: (*abi.ArrowArray)(abi.Pointer(v.raw.Dictionary))}
}

// ArrowArray owns a native Apache Arrow C Data Interface struct ArrowArray.
// Call Release when finished, or Detach to transfer ownership to an external
// C Data Interface consumer.
type ArrowArray struct {
	_        noCopy
	mu       sync.Mutex
	raw      *abi.ArrowArray
	released bool
	release  func(*abi.ArrowArray)
}

// View returns a borrowed read-only view of the root array node.
func (a *ArrowArray) View() ArrowArrayView {
	if a == nil {
		return ArrowArrayView{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.released || a.raw == nil {
		return ArrowArrayView{}
	}
	return ArrowArrayView{raw: a.raw}
}

// Ptr returns the raw *ArrowArray pointer while retaining ownership in a.
func (a *ArrowArray) Ptr() unsafe.Pointer {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.released || a.raw == nil {
		return nil
	}
	return unsafe.Pointer(a.raw)
}

// Detach transfers ownership of the underlying C ArrowArray to the caller and
// marks a released so subsequent Release calls become no-ops.
func (a *ArrowArray) Detach() unsafe.Pointer {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.released || a.raw == nil {
		return nil
	}
	ptr := unsafe.Pointer(a.raw)
	a.raw = nil
	a.released = true
	return ptr
}

func (a *ArrowArray) markTransferred() {
	if a == nil {
		return
	}
	a.mu.Lock()
	if a.raw != nil {
		a.raw.Release = 0
	}
	a.raw = nil
	a.released = true
	a.mu.Unlock()
}

// Released reports whether a has been released, detached, or transferred.
func (a *ArrowArray) Released() bool {
	if a == nil {
		return true
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.released
}

func (a *ArrowArray) takeOwnership() (*abi.ArrowArray, func(*abi.ArrowArray)) {
	if a == nil {
		return nil, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.released || a.raw == nil {
		return nil, nil
	}
	raw, rel := a.raw, a.release
	a.raw = nil
	a.release = nil
	a.released = true
	return raw, rel
}

// Release invokes the native ArrowArray release callback. Idempotent.
func (a *ArrowArray) Release() {
	if a == nil {
		return
	}
	a.mu.Lock()
	if a.released {
		a.mu.Unlock()
		return
	}
	raw := a.raw
	rel := a.release
	a.raw = nil
	a.released = true
	a.mu.Unlock()
	if raw != nil {
		if rel != nil {
			rel(raw)
		} else {
			abi.ReleaseArrowArray(raw)
		}
	}
}

// ArrowRows is an exclusive streaming cursor that exports each fetched native
// DuckDB data chunk as an Apache Arrow C Data Interface ArrowArray (struct
// array whose children are the result columns). It preserves the streaming-only
// execution contract, cancellation scope, and ResultLimits.MaxRows cap.
type ArrowRows struct {
	_            noCopy
	conn         *Conn
	stmt         *Stmt
	scope        *cancelScope
	ownsStmt     bool
	result       abi.Result
	arrowOpts    uintptr
	columns      []Column
	resultLimits ResultLimits
	rowsRead     uint64
	err          error
	done         bool
	closed       bool
}

// QueryArrow prepares sql, binds args, and starts a streaming Arrow cursor.
func (c *Conn) QueryArrow(ctx context.Context, sql string, args ...Value) (*ArrowRows, error) {
	return c.queryArrowWithBinder(ctx, nil, sql, func(s *Stmt) error { return s.bindLocked(args) })
}

// QueryArrowNamed prepares sql, binds named parameters ($name), and starts a
// streaming Arrow cursor.
func (c *Conn) QueryArrowNamed(ctx context.Context, sql string, args ...NamedArg) (*ArrowRows, error) {
	return c.queryArrowWithBinder(ctx, nil, sql, func(s *Stmt) error { return s.bindNamedLocked(args) })
}

func (c *Conn) queryArrowWithBinder(ctx context.Context, tx *Tx, sql string, bind func(*Stmt) error) (*ArrowRows, error) {
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
	if err := c.checkTxLocked(tx); err != nil {
		return nil, err
	}
	if err := c.usableLocked(); err != nil {
		return nil, err
	}
	scope, err := c.beginScopeLocked(ctx)
	if err != nil {
		return nil, err
	}
	s, err := c.prepareLocked(sql, scope)
	if err != nil {
		c.endScopeLocked(scope)
		return nil, err
	}
	ar, err := s.startArrowLocked(func() error { return bind(s) }, scope)
	if err != nil {
		s.releaseLocked()
		c.endScopeLocked(scope)
		return nil, err
	}
	ar.ownsStmt = true
	return ar, nil
}

// QueryArrow binds args and starts a streaming Arrow cursor borrowing s.
func (s *Stmt) QueryArrow(ctx context.Context, args ...Value) (*ArrowRows, error) {
	return s.queryArrowWithBinder(ctx, func() error { return s.bindLocked(args) })
}

// QueryArrowNamed binds named parameters ($name) and starts a streaming Arrow
// cursor borrowing s.
func (s *Stmt) QueryArrowNamed(ctx context.Context, args ...NamedArg) (*ArrowRows, error) {
	return s.queryArrowWithBinder(ctx, func() error { return s.bindNamedLocked(args) })
}

// ArrowSchema exports an owned Apache Arrow C Data Interface ArrowSchema
// describing the prepared statement's output columns without executing the
// statement (using DuckDB's non-deprecated duckdb_to_arrow_schema API).
// Callers must call Release (or Detach) on the returned schema.
func (s *Stmt) ArrowSchema() (*ArrowSchema, error) {
	if s == nil {
		return nil, ErrClosed
	}
	c := s.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if s.closed || c.closed {
		return nil, ErrClosed
	}
	api := c.api
	n := api.PreparedStatementColumnCount(s.handle)
	if n > math.MaxInt {
		return nil, fmt.Errorf("%w: column count %d overflows int", ErrInvalidInput, n)
	}
	types := make([]uintptr, int(n))
	defer func() {
		for i := range types {
			if types[i] != 0 {
				api.DestroyLogicalType(&types[i])
			}
		}
	}()
	var pinner runtime.Pinner
	defer pinner.Unpin()
	cnames := make([][]byte, int(n))
	namePtrs := make([]uintptr, int(n))
	for i := uint64(0); i < n; i++ {
		namePtr := api.PreparedStatementColumnName(s.handle, i)
		name := abi.GoString(namePtr)
		if namePtr != 0 {
			api.Free(namePtr)
		}
		lt := api.PreparedStatementColumnLogicalType(s.handle, i)
		types[i] = lt
		if _, err := buildColumnPlan(api, lt, 0, c.limits); err != nil {
			return nil, fmt.Errorf("column %d (%q): %w", i, name, err)
		}
		cb, err := abi.CString(name)
		if err != nil {
			return nil, fmt.Errorf("%w: column %d name: %v", ErrInvalidInput, i, err)
		}
		cnames[int(i)] = cb
		pinner.Pin(&cb[0])
		namePtrs[int(i)] = uintptr(unsafe.Pointer(&cb[0]))
	}
	var arrowOpts uintptr
	api.ConnectionGetArrowOptions(c.handle, &arrowOpts)
	if arrowOpts == 0 {
		return nil, nativeError("connection_get_arrow_options", abi.ErrorInvalid, "")
	}
	defer api.DestroyArrowOptions(&arrowOpts)

	var typesPtr, namesPtr *uintptr
	if n > 0 {
		typesPtr = &types[0]
		namesPtr = &namePtrs[0]
	} else {
		typesPtr = &emptyBindHandle
		namesPtr = &emptyBindHandle
	}
	out := &abi.ArrowSchema{}
	errData := api.ToArrowSchema(arrowOpts, typesPtr, namesPtr, n, out)
	runtime.KeepAlive(types)
	runtime.KeepAlive(cnames)
	runtime.KeepAlive(namePtrs)
	if err := consumeErrorData(api, "to_arrow_schema", errData); err != nil {
		abi.ReleaseArrowSchema(out)
		return nil, err
	}
	return &ArrowSchema{raw: out}, nil
}

// QueryScriptArrow executes leading statements in sql sequentially and streams
// the final statement as an Apache Arrow C Data Interface cursor. Positional
// args are distributed across statements in declaration order.
func (c *Conn) QueryScriptArrow(ctx context.Context, sql string, args ...Value) (*ArrowRows, error) {
	return c.queryScriptArrowWithArgs(ctx, nil, sql, newScriptArgsPos(args))
}

// QueryScriptArrowNamed executes leading statements in sql sequentially and
// streams the final statement as an Apache Arrow C Data Interface cursor,
// routing named parameters ($name) across statements.
func (c *Conn) QueryScriptArrowNamed(ctx context.Context, sql string, args ...NamedArg) (*ArrowRows, error) {
	sa, err := newScriptArgsNamed(args)
	if err != nil {
		return nil, err
	}
	return c.queryScriptArrowWithArgs(ctx, nil, sql, sa)
}

func (c *Conn) queryScriptArrowWithArgs(ctx context.Context, tx *Tx, sql string, args *scriptArgs) (*ArrowRows, error) {
	if c == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	csql, err := abi.CString(sql)
	if err != nil {
		return nil, fmt.Errorf("%w: sql: %v", ErrInvalidInput, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkTxLocked(tx); err != nil {
		return nil, err
	}
	if err := c.usableLocked(); err != nil {
		return nil, err
	}
	scope, err := c.beginScopeLocked(ctx)
	if err != nil {
		return nil, err
	}

	api := c.api
	var extracted uintptr
	count := api.ExtractStatements(c.handle, &csql[0], &extracted)
	runtime.KeepAlive(csql)
	if count == 0 {
		msg := abi.GoString(api.ExtractStatementsError(extracted))
		if extracted != 0 {
			api.DestroyExtracted(&extracted)
		}
		cerr := scope.err()
		c.endScopeLocked(scope)
		if cerr != nil {
			return nil, cerr
		}
		if msg != "" {
			return nil, nativeError("extract_statements", abi.ErrorInvalid, msg)
		}
		return nil, fmt.Errorf("%w: script contains no statements", ErrInvalidInput)
	}

	for i := uint64(0); i < count-1; i++ {
		if cerr := scope.err(); cerr != nil {
			api.DestroyExtracted(&extracted)
			c.endScopeLocked(scope)
			return nil, cerr
		}
		s, err := c.prepareExtractedLocked(extracted, i, scope)
		if err != nil {
			api.DestroyExtracted(&extracted)
			c.endScopeLocked(scope)
			return nil, err
		}
		_, execErr := s.execNoFinishLocked(func() error { return args.bindStmtLocked(s, false) }, scope)
		s.releaseLocked()
		if execErr != nil {
			api.DestroyExtracted(&extracted)
			c.endScopeLocked(scope)
			return nil, execErr
		}
	}

	lastStmt, err := c.prepareExtractedLocked(extracted, count-1, scope)
	api.DestroyExtracted(&extracted)
	if err != nil {
		c.endScopeLocked(scope)
		return nil, err
	}
	ar, err := lastStmt.startArrowLocked(func() error { return args.bindStmtLocked(lastStmt, true) }, scope)
	if err != nil {
		lastStmt.releaseLocked()
		c.endScopeLocked(scope)
		return nil, err
	}
	ar.ownsStmt = true
	return ar, nil
}

func (s *Stmt) queryArrowWithBinder(ctx context.Context, bind func() error) (*ArrowRows, error) {
	if s == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c := s.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	if err := c.usableLocked(); err != nil {
		return nil, err
	}
	scope, err := c.beginScopeLocked(ctx)
	if err != nil {
		return nil, err
	}
	ar, err := s.startArrowLocked(bind, scope)
	if err != nil {
		c.endScopeLocked(scope)
		return nil, err
	}
	return ar, nil
}

func (s *Stmt) startArrowLocked(bind func() error, scope *cancelScope) (*ArrowRows, error) {
	result, err := s.runPendingLocked(bind, scope)
	if err != nil {
		return nil, err
	}
	api := s.conn.api
	if cerr := scope.err(); cerr != nil {
		api.DestroyResult(&result)
		return nil, cerr
	}
	if !api.ResultIsStreaming(result) {
		api.DestroyResult(&result)
		return nil, ErrNotStreaming
	}
	n := api.ColumnCount(&result)
	if n > math.MaxInt {
		api.DestroyResult(&result)
		return nil, fmt.Errorf("%w: column count %d overflows int", ErrInvalidInput, n)
	}
	limits := s.conn.limits
	columns := make([]Column, int(n))
	for i := uint64(0); i < n; i++ {
		name := abi.GoString(api.ColumnName(&result, i))
		lt := api.ColumnLogicalType(&result, i)
		plan, planErr := buildColumnPlan(api, lt, 0, limits)
		api.DestroyLogicalType(&lt)
		if planErr != nil {
			api.DestroyResult(&result)
			return nil, fmt.Errorf("column %d (%q): %w", i, name, planErr)
		}
		columns[i] = Column{Name: name, Type: plan.lt}
	}
	var arrowOpts uintptr
	if api.ResultGetArrowOptions != nil {
		arrowOpts = api.ResultGetArrowOptions(&result)
	}
	if arrowOpts == 0 {
		api.ConnectionGetArrowOptions(s.conn.handle, &arrowOpts)
	}
	if arrowOpts == 0 {
		api.DestroyResult(&result)
		return nil, nativeError("result_get_arrow_options", abi.ErrorInvalid, "")
	}
	if cerr := scope.err(); cerr != nil {
		api.DestroyArrowOptions(&arrowOpts)
		api.DestroyResult(&result)
		return nil, cerr
	}
	ar := &ArrowRows{
		conn:         s.conn,
		stmt:         s,
		scope:        scope,
		result:       result,
		arrowOpts:    arrowOpts,
		columns:      columns,
		resultLimits: s.conn.resultLimits,
	}
	s.arrowRows = ar
	s.conn.activeArrow = ar
	return ar, nil
}

// Columns returns a copy of the audited DuckDB column metadata.
func (r *ArrowRows) Columns() []Column {
	if r == nil {
		return nil
	}
	return append([]Column(nil), r.columns...)
}

// Schema exports an owned Apache Arrow C Data Interface ArrowSchema describing
// the result columns. Callers must call Release (or Detach) on the returned
// schema.
func (r *ArrowRows) Schema() (*ArrowSchema, error) {
	if r == nil {
		return nil, ErrClosed
	}
	c := r.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.closed {
		return nil, ErrClosed
	}
	if cerr := r.scope.err(); cerr != nil {
		r.err = cerr
		return nil, cerr
	}
	api := c.api
	n := len(r.columns)
	types := make([]uintptr, n)
	defer func() {
		for i := range types {
			if types[i] != 0 {
				api.DestroyLogicalType(&types[i])
			}
		}
	}()
	var pinner runtime.Pinner
	defer pinner.Unpin()
	cnames := make([][]byte, n)
	namePtrs := make([]uintptr, n)
	for i := 0; i < n; i++ {
		types[i] = api.ColumnLogicalType(&r.result, uint64(i))
		cb, err := abi.CString(r.columns[i].Name)
		if err != nil {
			return nil, fmt.Errorf("%w: column %d name: %v", ErrInvalidInput, i, err)
		}
		cnames[i] = cb
		pinner.Pin(&cb[0])
		namePtrs[i] = uintptr(unsafe.Pointer(&cb[0]))
	}
	var typesPtr, namesPtr *uintptr
	if n > 0 {
		typesPtr = &types[0]
		namesPtr = &namePtrs[0]
	} else {
		typesPtr = &emptyBindHandle
		namesPtr = &emptyBindHandle
	}
	out := &abi.ArrowSchema{}
	errData := api.ToArrowSchema(r.arrowOpts, typesPtr, namesPtr, uint64(n), out)
	runtime.KeepAlive(types)
	runtime.KeepAlive(cnames)
	runtime.KeepAlive(namePtrs)
	if err := consumeErrorData(api, "to_arrow_schema", errData); err != nil {
		abi.ReleaseArrowSchema(out)
		return nil, err
	}
	return &ArrowSchema{raw: out}, nil
}

// Next fetches the next non-empty streaming data chunk and exports it as an
// owned ArrowArray (whose children correspond to the result columns).
// Returning (nil, nil) signals clean end of stream. Callers must call Release
// (or Detach) on each returned ArrowArray.
func (r *ArrowRows) Next() (*ArrowArray, error) {
	if r == nil {
		return nil, ErrClosed
	}
	c := r.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.closed {
		if r.err == nil && !r.done {
			r.err = ErrClosed
		}
		return nil, r.err
	}
	if r.done {
		return nil, nil
	}
	if r.err != nil {
		return nil, r.err
	}
	api := c.api
	for {
		if cerr := r.scope.err(); cerr != nil {
			r.err = cerr
			return nil, cerr
		}
		chunk := api.FetchChunk(r.result)
		if chunk == 0 {
			message := abi.GoString(api.ResultError(&r.result))
			errType := api.ResultErrorType(&r.result)
			if cerr := r.scope.err(); cerr != nil {
				r.err = cerr
				return nil, cerr
			}
			if message != "" || errType != abi.ErrorInvalid {
				r.err = nativeError("fetch", errType, message)
				return nil, r.err
			}
			if !r.scope.tryFinish() {
				r.err = r.scope.err()
				return nil, r.err
			}
			r.done = true
			return nil, nil
		}
		size := api.DataChunkGetSize(chunk)
		if cerr := r.scope.err(); cerr != nil {
			api.DestroyDataChunk(&chunk)
			r.err = cerr
			return nil, cerr
		}
		if size == 0 {
			api.DestroyDataChunk(&chunk)
			continue
		}
		if r.resultLimits.MaxRows > 0 && (r.rowsRead+size < r.rowsRead || r.rowsRead+size > r.resultLimits.MaxRows) {
			api.DestroyDataChunk(&chunk)
			r.err = fmt.Errorf("%w: result row count exceeds limit %d", ErrBudgetExceeded, r.resultLimits.MaxRows)
			return nil, r.err
		}
		out := &abi.ArrowArray{}
		errData := api.DataChunkToArrow(r.arrowOpts, chunk, out)
		api.DestroyDataChunk(&chunk)
		if err := consumeErrorData(api, "data_chunk_to_arrow", errData); err != nil {
			abi.ReleaseArrowArray(out)
			r.err = err
			return nil, err
		}
		r.rowsRead += size
		return &ArrowArray{raw: out}, nil
	}
}

// Err returns the error that ended iteration, if any.
func (r *ArrowRows) Err() error {
	if r == nil {
		return ErrClosed
	}
	c := r.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	return r.err
}

// Close interrupts any in-flight fetch, joins the cancellation scope without a
// deadline, destroys native options/result/owned statement, and releases the
// connection lease. Close is idempotent.
func (r *ArrowRows) Close() error {
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

func (r *ArrowRows) releaseLocked() {
	if r.closed {
		return
	}
	r.closed = true
	r.conn.endScopeLocked(r.scope)
	api := r.conn.api
	if r.arrowOpts != 0 {
		api.DestroyArrowOptions(&r.arrowOpts)
	}
	api.DestroyResult(&r.result)
	if r.conn.activeArrow == r {
		r.conn.activeArrow = nil
	}
	s := r.stmt
	s.arrowRows = nil
	if r.ownsStmt {
		s.releaseLocked()
	}
}
