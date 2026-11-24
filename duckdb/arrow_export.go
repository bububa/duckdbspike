package duckdb

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

const maxArrowExportDepth = 64

// ArrowSchemaSpec describes a Go-constructed Arrow C Data Interface schema
// node for export via NewArrowSchema.
type ArrowSchemaSpec struct {
	Format     string
	Name       string
	Metadata   []byte // Arrow C Data Interface binary metadata format, optional
	Flags      int64
	Children   []ArrowSchemaSpec
	Dictionary *ArrowSchemaSpec
}

// ArrowArraySpec describes a Go-constructed Arrow C Data Interface array node
// for export via NewArrowArray. Nil entries in Buffers export as NULL pointers
// (e.g. an all-valid validity bitmap at index 0). Non-nil slices are pinned in
// Go memory until the ArrowArray is released by Go or C++.
type ArrowArraySpec struct {
	Length     int64
	NullCount  int64
	Offset     int64
	Buffers    [][]byte
	Children   []ArrowArraySpec
	Dictionary *ArrowArraySpec
}

type arrowPinArena struct {
	pinner       *runtime.Pinner
	keepAlive    []any
	emptyBuf     []byte
	emptyBufAddr uintptr
}

func newArrowPinArena() *arrowPinArena {
	a := &arrowPinArena{
		pinner:   new(runtime.Pinner),
		emptyBuf: make([]byte, 1),
	}
	a.pinner.Pin(&a.emptyBuf[0])
	a.emptyBufAddr = uintptr(unsafe.Pointer(&a.emptyBuf[0]))
	return a
}

func (a *arrowPinArena) pinCString(s string) (uintptr, error) {
	cb, err := abi.CString(s)
	if err != nil {
		return 0, err
	}
	a.pinner.Pin(&cb[0])
	a.keepAlive = append(a.keepAlive, cb)
	return uintptr(unsafe.Pointer(&cb[0])), nil
}

func (a *arrowPinArena) pinBytesCopy(src []byte) uintptr {
	if len(src) == 0 {
		return 0
	}
	cp := make([]byte, len(src))
	copy(cp, src)
	a.pinner.Pin(&cp[0])
	a.keepAlive = append(a.keepAlive, cp)
	return uintptr(unsafe.Pointer(&cp[0]))
}

func (a *arrowPinArena) pinBuffer(buf []byte) uintptr {
	if buf == nil {
		return 0
	}
	if len(buf) == 0 {
		return a.emptyBufAddr
	}
	a.pinner.Pin(&buf[0])
	a.keepAlive = append(a.keepAlive, buf)
	return uintptr(unsafe.Pointer(&buf[0]))
}

func (a *arrowPinArena) unpin() {
	a.pinner.Unpin()
	a.keepAlive = nil
	a.emptyBuf = nil
	a.emptyBufAddr = 0
}

// NewArrowSchema builds a Go-owned, pinned Arrow C Data Interface ArrowSchema
// backed by a process-wide singleton release callback. The returned schema is
// safe to pass to DuckDB (AppendArrowChunk, RegisterArrowView) or external C
// consumers; call Release when finished.
func NewArrowSchema(spec ArrowSchemaSpec) (*ArrowSchema, error) {
	arena := newArrowPinArena()
	raw, err := buildExportedSchema(arena, spec, 0)
	if err != nil {
		arena.unpin()
		return nil, err
	}
	abi.RegisterExportedArrowSchema(raw, arena.unpin)
	return &ArrowSchema{raw: raw}, nil
}

func buildExportedSchema(arena *arrowPinArena, spec ArrowSchemaSpec, depth int) (*abi.ArrowSchema, error) {
	if depth > maxArrowExportDepth {
		return nil, fmt.Errorf("%w: Arrow schema nesting depth exceeds %d", ErrLimitExceeded, maxArrowExportDepth)
	}
	if spec.Format == "" {
		return nil, fmt.Errorf("%w: ArrowSchema format must not be empty", ErrInvalidInput)
	}
	fmtPtr, err := arena.pinCString(spec.Format)
	if err != nil {
		return nil, fmt.Errorf("%w: ArrowSchema format: %v", ErrInvalidInput, err)
	}
	namePtr, err := arena.pinCString(spec.Name)
	if err != nil {
		return nil, fmt.Errorf("%w: ArrowSchema name: %v", ErrInvalidInput, err)
	}
	raw := &abi.ArrowSchema{
		Format:   fmtPtr,
		Name:     namePtr,
		Metadata: arena.pinBytesCopy(spec.Metadata),
		Flags:    spec.Flags,
	}
	arena.pinner.Pin(raw)
	arena.keepAlive = append(arena.keepAlive, raw)

	if len(spec.Children) > 0 {
		childPtrs := make([]uintptr, len(spec.Children))
		for i, cs := range spec.Children {
			child, err := buildExportedSchema(arena, cs, depth+1)
			if err != nil {
				return nil, fmt.Errorf("child %d: %w", i, err)
			}
			abi.MarkArrowSchemaChildRelease(child)
			childPtrs[i] = uintptr(unsafe.Pointer(child))
		}
		arena.pinner.Pin(&childPtrs[0])
		arena.keepAlive = append(arena.keepAlive, childPtrs)
		raw.NChildren = int64(len(spec.Children))
		raw.Children = uintptr(unsafe.Pointer(&childPtrs[0]))
	}
	if spec.Dictionary != nil {
		dict, err := buildExportedSchema(arena, *spec.Dictionary, depth+1)
		if err != nil {
			return nil, fmt.Errorf("dictionary: %w", err)
		}
		abi.MarkArrowSchemaChildRelease(dict)
		raw.Dictionary = uintptr(unsafe.Pointer(dict))
	}
	return raw, nil
}

// NewArrowArray builds a Go-owned, pinned Arrow C Data Interface ArrowArray
// backed by a process-wide singleton release callback. If onRelease is non-nil,
// it is invoked exactly once when Go or C++ releases the array (after all
// pinned buffers are unpinned), allowing callers such as duckdbarrow to release
// an arrow.RecordBatch.
func NewArrowArray(spec ArrowArraySpec, onRelease func()) (*ArrowArray, error) {
	arena := newArrowPinArena()
	raw, err := buildExportedArray(arena, spec, 0)
	if err != nil {
		arena.unpin()
		return nil, err
	}
	abi.RegisterExportedArrowArray(raw, func() {
		arena.unpin()
		if onRelease != nil {
			onRelease()
		}
	})
	return &ArrowArray{raw: raw}, nil
}

func buildExportedArray(arena *arrowPinArena, spec ArrowArraySpec, depth int) (*abi.ArrowArray, error) {
	if depth > maxArrowExportDepth {
		return nil, fmt.Errorf("%w: Arrow array nesting depth exceeds %d", ErrLimitExceeded, maxArrowExportDepth)
	}
	if spec.Length < 0 || spec.Offset < 0 {
		return nil, fmt.Errorf("%w: negative ArrowArray length (%d) or offset (%d)", ErrInvalidInput, spec.Length, spec.Offset)
	}
	raw := &abi.ArrowArray{
		Length:    spec.Length,
		NullCount: spec.NullCount,
		Offset:    spec.Offset,
	}
	arena.pinner.Pin(raw)
	arena.keepAlive = append(arena.keepAlive, raw)

	if len(spec.Buffers) > 0 {
		bufPtrs := make([]uintptr, len(spec.Buffers))
		for i, b := range spec.Buffers {
			bufPtrs[i] = arena.pinBuffer(b)
		}
		arena.pinner.Pin(&bufPtrs[0])
		arena.keepAlive = append(arena.keepAlive, bufPtrs)
		raw.NBuffers = int64(len(spec.Buffers))
		raw.Buffers = uintptr(unsafe.Pointer(&bufPtrs[0]))
	}
	if len(spec.Children) > 0 {
		childPtrs := make([]uintptr, len(spec.Children))
		for i, cs := range spec.Children {
			child, err := buildExportedArray(arena, cs, depth+1)
			if err != nil {
				return nil, fmt.Errorf("child %d: %w", i, err)
			}
			abi.MarkArrowArrayChildRelease(child)
			childPtrs[i] = uintptr(unsafe.Pointer(child))
		}
		arena.pinner.Pin(&childPtrs[0])
		arena.keepAlive = append(arena.keepAlive, childPtrs)
		raw.NChildren = int64(len(spec.Children))
		raw.Children = uintptr(unsafe.Pointer(&childPtrs[0]))
	}
	if spec.Dictionary != nil {
		dict, err := buildExportedArray(arena, *spec.Dictionary, depth+1)
		if err != nil {
			return nil, fmt.Errorf("dictionary: %w", err)
		}
		abi.MarkArrowArrayChildRelease(dict)
		raw.Dictionary = uintptr(unsafe.Pointer(dict))
	}
	return raw, nil
}

// ArrowStreamProvider supplies a schema and a sequence of ArrowArrays to
// Conn.RegisterArrowStream. Next must return (nil, nil) on clean end-of-stream.
// Close is called once when the registered ArrowView is closed or if
// registration fails after taking ownership of provider.
type ArrowStreamProvider interface {
	Schema() (*ArrowSchema, error)
	Next() (*ArrowArray, error)
	Close() error
}

// ArrowView owns a temporary DuckDB view registered from an Arrow C Data
// Interface schema/array (duckdb_arrow_array_scan) or streaming provider
// (duckdb_arrow_scan). Call Close when done to drop the view, destroy/release
// the native stream, and release all backing Arrow resources.
type ArrowView struct {
	_            noCopy
	conn         *Conn
	name         string
	stream       uintptr               // from duckdb_arrow_array_scan
	streamExport *abi.ArrowArrayStream // from duckdb_arrow_scan
	schemaRaw    *abi.ArrowSchema
	schemaRel    func(*abi.ArrowSchema)
	arrayRaw     *abi.ArrowArray
	arrayRel     func(*abi.ArrowArray)
	closed       bool
}

type arrowStreamAdapter struct {
	mu         sync.Mutex
	provider   ArrowStreamProvider
	schemaRaw  *abi.ArrowSchema
	schemaRel  func(*abi.ArrowSchema)
	rawStream  *abi.ArrowArrayStream
	pinner     *runtime.Pinner
	lastErrBuf []byte
	lastErrPtr uintptr
	errPinners []*runtime.Pinner
	closed     bool
}

func (a *arrowStreamAdapter) setLastErrorLocked(msg string) {
	if msg == "" {
		msg = "arrow stream error"
	}
	cb, err := abi.CString(msg)
	if err != nil {
		cb = append([]byte(strings.ReplaceAll(msg, "\x00", "?")), 0)
	}
	p := new(runtime.Pinner)
	p.Pin(&cb[0])
	a.lastErrBuf = cb
	a.lastErrPtr = uintptr(unsafe.Pointer(&cb[0]))
	a.errPinners = append(a.errPinners, p)
}

func (a *arrowStreamAdapter) GetSchema(out *abi.ArrowSchema) int32 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.schemaRaw == nil {
		a.setLastErrorLocked("arrow stream closed")
		return 22 // EINVAL
	}
	*out = *a.schemaRaw
	// DuckDB's FactoryGetSchema clears out.release = NULL and never invokes it;
	// schemaRaw remains owned by arrowStreamAdapter until Release.
	out.Release = 0
	return 0
}

func (a *arrowStreamAdapter) GetNext(out *abi.ArrowArray) int32 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		*out = abi.ArrowArray{}
		return 0
	}
	arr, err := a.provider.Next()
	if err != nil {
		*out = abi.ArrowArray{}
		a.setLastErrorLocked(err.Error())
		return 5 // EIO
	}
	if arr == nil {
		*out = abi.ArrowArray{}
		return 0
	}
	raw, _ := arr.takeOwnership()
	if raw == nil {
		*out = abi.ArrowArray{}
		a.setLastErrorLocked("arrow stream provider returned released ArrowArray")
		return 22 // EINVAL
	}
	// Transfer ownership of this batch's ArrowArray to DuckDB, which will
	// invoke out.Release (our singleton ArrowArrayReleaseCallback) when done.
	*out = *raw
	return 0
}

func (a *arrowStreamAdapter) GetLastError() uintptr {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastErrPtr
}

func (a *arrowStreamAdapter) Release() {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	a.closed = true
	provider := a.provider
	sRaw, sRel := a.schemaRaw, a.schemaRel
	a.schemaRaw, a.schemaRel = nil, nil
	pinner := a.pinner
	errPinners := a.errPinners
	a.errPinners = nil
	a.lastErrBuf = nil
	a.lastErrPtr = 0
	a.mu.Unlock()

	if provider != nil {
		_ = provider.Close()
	}
	if sRaw != nil {
		if sRel != nil {
			sRel(sRaw)
		} else {
			abi.ReleaseArrowSchema(sRaw)
		}
	}
	for _, ep := range errPinners {
		ep.Unpin()
	}
	if pinner != nil {
		pinner.Unpin()
	}
}

// RegisterArrowStream registers a multi-batch ArrowStreamProvider as a
// temporary SQL view named name on c via duckdb_arrow_scan. On success,
// ownership of provider transfers to the returned ArrowView, which closes
// provider and releases the schema when ArrowView.Close (or Conn.Close) runs.
// On error, provider is NOT closed unless Schema() succeeded before an internal
// error; callers should inspect err and close provider on error if needed.
//
// Note: DuckDB v1.5.6's deprecated duckdb_arrow_scan C API exhibits an upstream
// projection-pushdown bug if a query skips leading columns (non-prefix column
// projection). Queries against the view should select all columns (or a
// contiguous leading prefix, e.g. CREATE TEMP TABLE t AS SELECT * FROM view);
// for arbitrary SQL projections, prefer Appender.AppendArrowChunk /
// duckdbarrow.AppendRecordReader.
func (c *Conn) RegisterArrowStream(ctx context.Context, name string, provider ArrowStreamProvider) (*ArrowView, error) {
	return c.registerArrowStreamWithTx(ctx, nil, name, provider)
}

// RegisterArrowStream registers a multi-batch ArrowStreamProvider as a
// temporary SQL view within this transaction's connection.
func (t *Tx) RegisterArrowStream(ctx context.Context, name string, provider ArrowStreamProvider) (*ArrowView, error) {
	if t == nil {
		return nil, ErrTxDone
	}
	return t.conn.registerArrowStreamWithTx(ctx, t, name, provider)
}

func (c *Conn) registerArrowStreamWithTx(ctx context.Context, tx *Tx, name string, provider ArrowStreamProvider) (*ArrowView, error) {
	if c == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if name == "" {
		return nil, fmt.Errorf("%w: arrow view name must not be empty", ErrInvalidInput)
	}
	cname, err := abi.CString(name)
	if err != nil {
		return nil, fmt.Errorf("%w: arrow view name: %v", ErrInvalidInput, err)
	}
	if provider == nil {
		return nil, fmt.Errorf("%w: nil ArrowStreamProvider", ErrInvalidInput)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkTxLocked(tx); err != nil {
		return nil, err
	}
	if err := c.usableLocked(); err != nil {
		return nil, err
	}

	schema, err := provider.Schema()
	if err != nil {
		return nil, err
	}
	if schema == nil || schema.Released() {
		return nil, fmt.Errorf("%w: ArrowStreamProvider returned nil or released schema", ErrInvalidInput)
	}
	sRaw, sRel := schema.takeOwnership()

	rawStream := &abi.ArrowArrayStream{}
	pinner := new(runtime.Pinner)
	pinner.Pin(rawStream)
	adapter := &arrowStreamAdapter{
		provider:  provider,
		schemaRaw: sRaw,
		schemaRel: sRel,
		rawStream: rawStream,
		pinner:    pinner,
	}
	abi.RegisterExportedArrowStream(rawStream, adapter)

	state := c.api.ArrowScan(c.handle, &cname[0], rawStream)
	runtime.KeepAlive(cname)
	if state != abi.StateSuccess {
		// On registration failure, do not close caller's provider; only release
		// our stream handle, pinned stream struct, and exported schema.
		adapter.mu.Lock()
		adapter.provider = nil
		adapter.mu.Unlock()
		abi.ReleaseArrowStream(rawStream)
		return nil, nativeError("arrow_scan", abi.ErrorInvalid, fmt.Sprintf("register arrow stream view %q failed", name))
	}

	v := &ArrowView{
		conn:         c,
		name:         name,
		streamExport: rawStream,
	}
	if c.arrowViews == nil {
		c.arrowViews = make(map[*ArrowView]struct{})
	}
	c.arrowViews[v] = struct{}{}
	return v, nil
}

// Name returns the SQL view name registered on the connection.
func (v *ArrowView) Name() string {
	if v == nil {
		return ""
	}
	return v.name
}

// RegisterArrowView registers schema and array as a temporary SQL view named
// name on c via duckdb_arrow_array_scan. On success, ownership of schema and
// array transfers to the returned ArrowView (marking schema and array released)
// and they are released when ArrowView.Close or Conn.Close runs. On error,
// schema and array remain owned by the caller.
func (c *Conn) RegisterArrowView(ctx context.Context, name string, schema *ArrowSchema, array *ArrowArray) (*ArrowView, error) {
	return c.registerArrowViewWithTx(ctx, nil, name, schema, array)
}

// RegisterArrowView registers schema and array as a temporary SQL view within
// this transaction's connection.
func (t *Tx) RegisterArrowView(ctx context.Context, name string, schema *ArrowSchema, array *ArrowArray) (*ArrowView, error) {
	if t == nil {
		return nil, ErrTxDone
	}
	return t.conn.registerArrowViewWithTx(ctx, t, name, schema, array)
}

func (c *Conn) registerArrowViewWithTx(ctx context.Context, tx *Tx, name string, schema *ArrowSchema, array *ArrowArray) (*ArrowView, error) {
	if c == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if name == "" {
		return nil, fmt.Errorf("%w: arrow view name must not be empty", ErrInvalidInput)
	}
	cname, err := abi.CString(name)
	if err != nil {
		return nil, fmt.Errorf("%w: arrow view name: %v", ErrInvalidInput, err)
	}
	if schema == nil || schema.Released() || array == nil || array.Released() {
		return nil, fmt.Errorf("%w: nil or already-released Arrow schema/array", ErrInvalidInput)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkTxLocked(tx); err != nil {
		return nil, err
	}
	if err := c.usableLocked(); err != nil {
		return nil, err
	}
	if schema.Released() || array.Released() {
		return nil, fmt.Errorf("%w: nil or already-released Arrow schema/array", ErrInvalidInput)
	}
	var stream uintptr
	state := c.api.ArrowArrayScan(c.handle, &cname[0], schema.raw, array.raw, &stream)
	runtime.KeepAlive(cname)
	if state != abi.StateSuccess {
		if stream != 0 {
			c.api.DestroyArrowStream(&stream)
		}
		return nil, nativeError("arrow_array_scan", abi.ErrorInvalid, fmt.Sprintf("register arrow view %q failed", name))
	}
	sRaw, sRel := schema.takeOwnership()
	aRaw, aRel := array.takeOwnership()
	v := &ArrowView{
		conn:      c,
		name:      name,
		stream:    stream,
		schemaRaw: sRaw,
		schemaRel: sRel,
		arrayRaw:  aRaw,
		arrayRel:  aRel,
	}
	if c.arrowViews == nil {
		c.arrowViews = make(map[*ArrowView]struct{})
	}
	c.arrowViews[v] = struct{}{}
	return v, nil
}

// Close drops the temporary view (when the connection is idle and open),
// destroys the native Arrow stream, and releases the owned ArrowArray and
// ArrowSchema. Idempotent.
func (v *ArrowView) Close() error {
	if v == nil {
		return nil
	}
	c := v.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if v.closed {
		return nil
	}
	if !c.closed && c.handle != 0 && c.usableLocked() == nil && c.api.Prepare != nil {
		dropSQL := fmt.Sprintf(`DROP VIEW IF EXISTS "%s"`, strings.ReplaceAll(v.name, `"`, `""`))
		if scope, err := c.beginScopeLocked(context.Background()); err == nil {
			if s, err := c.prepareLocked(dropSQL, scope); err == nil {
				_, _ = s.execLocked(func() error { return nil }, scope)
				s.releaseLocked()
			}
			c.endScopeLocked(scope)
		}
	}
	v.releaseLocked()
	return nil
}

func (v *ArrowView) releaseLocked() {
	if v.closed {
		return
	}
	v.closed = true
	if v.conn != nil && v.conn.arrowViews != nil {
		delete(v.conn.arrowViews, v)
	}
	if v.stream != 0 && v.conn != nil && v.conn.api != nil && v.conn.api.DestroyArrowStream != nil {
		v.conn.api.DestroyArrowStream(&v.stream)
	}
	if v.streamExport != nil {
		exp := v.streamExport
		v.streamExport = nil
		abi.ReleaseArrowStream(exp)
	}
	if v.arrayRaw != nil {
		raw, rel := v.arrayRaw, v.arrayRel
		v.arrayRaw, v.arrayRel = nil, nil
		if rel != nil {
			rel(raw)
		} else {
			abi.ReleaseArrowArray(raw)
		}
	}
	if v.schemaRaw != nil {
		raw, rel := v.schemaRaw, v.schemaRel
		v.schemaRaw, v.schemaRel = nil, nil
		if rel != nil {
			rel(raw)
		} else {
			abi.ReleaseArrowSchema(raw)
		}
	}
}
