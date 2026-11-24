// Package duckdbarrow integrates duckdb's purego Arrow C Data Interface with
// github.com/apache/arrow-go/v18 without cgo (CGO_ENABLED=0).
//
// Each streamed native ArrowArray chunk is copied into Go-owned Arrow memory
// buffers and released immediately, so returned arrow.RecordBatch values own
// their memory and remain safe after the RecordReader or Conn closes.
package duckdbarrow

import (
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/bububa/duckdbspike/duckdb"
)

const (
	arrowFlagDictOrdered = 1 << 0
	arrowFlagNullable    = 1 << 1
	arrowFlagMapKeysSort = 1 << 2
)

// ImportSchema converts a duckdb.ArrowSchema (C Data Interface struct schema)
// into a Go *arrow.Schema without cgo.
func ImportSchema(s *duckdb.ArrowSchema) (*arrow.Schema, error) {
	if s == nil || s.Released() {
		return nil, fmt.Errorf("%w: nil or released ArrowSchema", duckdb.ErrInvalidInput)
	}
	root := s.View()
	if !root.Valid() {
		return nil, fmt.Errorf("%w: invalid ArrowSchema", duckdb.ErrInvalidInput)
	}
	if root.Format() != "+s" {
		return nil, fmt.Errorf("%w: expected root struct schema \"+s\", got %q", duckdb.ErrUnsupportedType, root.Format())
	}
	n := root.ChildCount()
	fields := make([]arrow.Field, n)
	for i := 0; i < n; i++ {
		f, err := importField(root.Child(i), 0)
		if err != nil {
			return nil, fmt.Errorf("field %d: %w", i, err)
		}
		fields[i] = f
	}
	return arrow.NewSchema(fields, nil), nil
}

func importField(v duckdb.ArrowSchemaView, depth int) (arrow.Field, error) {
	if !v.Valid() {
		return arrow.Field{}, fmt.Errorf("%w: null ArrowSchema child", duckdb.ErrInvalidInput)
	}
	if depth > 64 {
		return arrow.Field{}, fmt.Errorf("%w: Arrow schema nesting depth exceeds limit", duckdb.ErrLimitExceeded)
	}
	dt, err := importDataType(v, depth)
	if err != nil {
		return arrow.Field{}, err
	}
	return arrow.Field{
		Name:     v.Name(),
		Type:     dt,
		Nullable: v.Flags()&arrowFlagNullable != 0,
	}, nil
}

func importDataType(v duckdb.ArrowSchemaView, depth int) (arrow.DataType, error) {
	baseDT, err := parseArrowFormat(v, depth)
	if err != nil {
		return nil, err
	}
	if dict := v.Dictionary(); dict.Valid() {
		valDT, err := importDataType(dict, depth+1)
		if err != nil {
			return nil, err
		}
		return &arrow.DictionaryType{
			IndexType: baseDT,
			ValueType: valDT,
			Ordered:   v.Flags()&arrowFlagDictOrdered != 0,
		}, nil
	}
	return baseDT, nil
}

func parseArrowFormat(v duckdb.ArrowSchemaView, depth int) (arrow.DataType, error) {
	format := v.Format()
	switch format {
	case "n":
		return arrow.Null, nil
	case "b":
		return arrow.FixedWidthTypes.Boolean, nil
	case "c":
		return arrow.PrimitiveTypes.Int8, nil
	case "s":
		return arrow.PrimitiveTypes.Int16, nil
	case "i":
		return arrow.PrimitiveTypes.Int32, nil
	case "l":
		return arrow.PrimitiveTypes.Int64, nil
	case "C":
		return arrow.PrimitiveTypes.Uint8, nil
	case "S":
		return arrow.PrimitiveTypes.Uint16, nil
	case "I":
		return arrow.PrimitiveTypes.Uint32, nil
	case "L":
		return arrow.PrimitiveTypes.Uint64, nil
	case "f":
		return arrow.PrimitiveTypes.Float32, nil
	case "g":
		return arrow.PrimitiveTypes.Float64, nil
	case "z":
		return arrow.BinaryTypes.Binary, nil
	case "Z":
		return arrow.BinaryTypes.LargeBinary, nil
	case "vz":
		return arrow.BinaryTypes.BinaryView, nil
	case "u":
		return arrow.BinaryTypes.String, nil
	case "U":
		return arrow.BinaryTypes.LargeString, nil
	case "vu":
		return arrow.BinaryTypes.StringView, nil
	case "tdD":
		return arrow.FixedWidthTypes.Date32, nil
	case "tdm":
		return arrow.FixedWidthTypes.Date64, nil
	case "tts":
		return arrow.FixedWidthTypes.Time32s, nil
	case "ttm":
		return arrow.FixedWidthTypes.Time32ms, nil
	case "ttu":
		return arrow.FixedWidthTypes.Time64us, nil
	case "ttn":
		return arrow.FixedWidthTypes.Time64ns, nil
	case "tiM":
		return arrow.FixedWidthTypes.MonthInterval, nil
	case "tiD":
		return arrow.FixedWidthTypes.DayTimeInterval, nil
	case "tin":
		return arrow.FixedWidthTypes.MonthDayNanoInterval, nil
	case "+l":
		if v.ChildCount() != 1 {
			return nil, fmt.Errorf("%w: list schema expects 1 child, got %d", duckdb.ErrInvalidInput, v.ChildCount())
		}
		elem, err := importField(v.Child(0), depth+1)
		if err != nil {
			return nil, err
		}
		return arrow.ListOfField(elem), nil
	case "+L":
		if v.ChildCount() != 1 {
			return nil, fmt.Errorf("%w: large list schema expects 1 child, got %d", duckdb.ErrInvalidInput, v.ChildCount())
		}
		elem, err := importField(v.Child(0), depth+1)
		if err != nil {
			return nil, err
		}
		return arrow.LargeListOfField(elem), nil
	case "+s":
		n := v.ChildCount()
		fields := make([]arrow.Field, n)
		for i := 0; i < n; i++ {
			f, err := importField(v.Child(i), depth+1)
			if err != nil {
				return nil, err
			}
			fields[i] = f
		}
		return arrow.StructOf(fields...), nil
	case "+m":
		if v.ChildCount() != 1 {
			return nil, fmt.Errorf("%w: map schema expects 1 child struct, got %d", duckdb.ErrInvalidInput, v.ChildCount())
		}
		entriesView := v.Child(0)
		if entriesView.ChildCount() != 2 {
			return nil, fmt.Errorf("%w: map entries struct expects 2 children, got %d", duckdb.ErrInvalidInput, entriesView.ChildCount())
		}
		keyField, err := importField(entriesView.Child(0), depth+1)
		if err != nil {
			return nil, err
		}
		valField, err := importField(entriesView.Child(1), depth+1)
		if err != nil {
			return nil, err
		}
		mt := arrow.MapOf(keyField.Type, valField.Type)
		mt.KeysSorted = v.Flags()&arrowFlagMapKeysSort != 0
		return mt, nil
	}

	// Parameterized format strings: d:P,S[,B], w:N, +w:N, ts[smun]:TZ.
	switch {
	case strings.HasPrefix(format, "d:"):
		parts := strings.Split(format[2:], ",")
		if len(parts) < 2 || len(parts) > 3 {
			return nil, fmt.Errorf("%w: invalid decimal format %q", duckdb.ErrUnsupportedType, format)
		}
		prec, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("%w: invalid decimal precision in %q", duckdb.ErrUnsupportedType, format)
		}
		scale, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("%w: invalid decimal scale in %q", duckdb.ErrUnsupportedType, format)
		}
		bits := 128
		if len(parts) == 3 {
			bits, err = strconv.Atoi(parts[2])
			if err != nil {
				return nil, fmt.Errorf("%w: invalid decimal bitwidth in %q", duckdb.ErrUnsupportedType, format)
			}
		}
		switch bits {
		case 128:
			return &arrow.Decimal128Type{Precision: int32(prec), Scale: int32(scale)}, nil
		case 256:
			return &arrow.Decimal256Type{Precision: int32(prec), Scale: int32(scale)}, nil
		default:
			return nil, fmt.Errorf("%w: unsupported decimal bitwidth %d", duckdb.ErrUnsupportedType, bits)
		}
	case strings.HasPrefix(format, "w:"):
		w, err := strconv.Atoi(format[2:])
		if err != nil || w < 0 {
			return nil, fmt.Errorf("%w: invalid fixed-size binary format %q", duckdb.ErrUnsupportedType, format)
		}
		return &arrow.FixedSizeBinaryType{ByteWidth: w}, nil
	case strings.HasPrefix(format, "+w:"):
		sz, err := strconv.Atoi(format[3:])
		if err != nil || sz < 0 || v.ChildCount() != 1 {
			return nil, fmt.Errorf("%w: invalid fixed-size list format %q", duckdb.ErrUnsupportedType, format)
		}
		elem, err := importField(v.Child(0), depth+1)
		if err != nil {
			return nil, err
		}
		return arrow.FixedSizeListOfField(int32(sz), elem), nil
	case strings.HasPrefix(format, "ts") && len(format) >= 4 && format[3] == ':':
		tz := format[4:]
		var unit arrow.TimeUnit
		switch format[2] {
		case 's':
			unit = arrow.Second
		case 'm':
			unit = arrow.Millisecond
		case 'u':
			unit = arrow.Microsecond
		case 'n':
			unit = arrow.Nanosecond
		default:
			return nil, fmt.Errorf("%w: unsupported timestamp format %q", duckdb.ErrUnsupportedType, format)
		}
		return &arrow.TimestampType{Unit: unit, TimeZone: tz}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported Arrow format %q", duckdb.ErrUnsupportedType, format)
	}
}

// ImportRecordBatch copies a duckdb.ArrowArray (root struct array matching
// schema) into a Go-owned arrow.RecordBatch allocated from alloc. The caller
// remains responsible for calling arr.Release() (which may happen immediately
// after ImportRecordBatch returns).
func ImportRecordBatch(schema *arrow.Schema, arr *duckdb.ArrowArray, alloc memory.Allocator) (arrow.RecordBatch, error) {
	if schema == nil || arr == nil || arr.Released() {
		return nil, fmt.Errorf("%w: nil schema or released ArrowArray", duckdb.ErrInvalidInput)
	}
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	root := arr.View()
	if !root.Valid() {
		return nil, fmt.Errorf("%w: invalid ArrowArray", duckdb.ErrInvalidInput)
	}
	if root.ChildCount() != schema.NumFields() {
		return nil, fmt.Errorf("%w: ArrowArray child count %d != schema fields %d", duckdb.ErrInvalidInput, root.ChildCount(), schema.NumFields())
	}
	cols := make([]arrow.Array, schema.NumFields())
	releaseCols := func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}
	for i := 0; i < schema.NumFields(); i++ {
		data, err := importArrayData(schema.Field(i).Type, root.Child(i), alloc, 0)
		if err != nil {
			releaseCols()
			return nil, fmt.Errorf("column %d (%q): %w", i, schema.Field(i).Name, err)
		}
		cols[i] = array.MakeFromData(data)
		data.Release()
	}
	rec := array.NewRecordBatch(schema, cols, root.Length())
	releaseCols()
	return rec, nil
}

func copyBuffer(alloc memory.Allocator, ptr unsafe.Pointer, size int) *memory.Buffer {
	if ptr == nil {
		return nil
	}
	buf := memory.NewResizableBuffer(alloc)
	buf.Resize(size)
	if size > 0 {
		copy(buf.Bytes(), unsafe.Slice((*byte)(ptr), size))
	}
	return buf
}

func releaseBuffers(bufs []*memory.Buffer) {
	for _, b := range bufs {
		if b != nil {
			b.Release()
		}
	}
}

func releaseChildren(children []arrow.ArrayData) {
	for _, c := range children {
		if c != nil {
			c.Release()
		}
	}
}

func importArrayData(dt arrow.DataType, v duckdb.ArrowArrayView, alloc memory.Allocator, depth int) (*array.Data, error) {
	if !v.Valid() {
		return nil, fmt.Errorf("%w: null ArrowArray child", duckdb.ErrInvalidInput)
	}
	if depth > 64 {
		return nil, fmt.Errorf("%w: Arrow array nesting depth exceeds limit", duckdb.ErrLimitExceeded)
	}
	length := int(v.Length())
	offset := int(v.Offset())
	nulls := int(v.NullCount())
	total := offset + length

	// Dictionary-encoded arrays (e.g. DuckDB ENUM).
	if dictType, ok := dt.(*arrow.DictionaryType); ok {
		idxData, err := importArrayData(dictType.IndexType, v, alloc, depth+1)
		if err != nil {
			return nil, err
		}
		dictData, err := importArrayData(dictType.ValueType, v.Dictionary(), alloc, depth+1)
		if err != nil {
			idxData.Release()
			return nil, err
		}
		data := array.NewDataWithDictionary(dictType, idxData.Len(), idxData.Buffers(), idxData.NullN(), idxData.Offset(), dictData)
		idxData.Release()
		dictData.Release()
		return data, nil
	}

	switch t := dt.(type) {
	case *arrow.NullType:
		return array.NewData(dt, length, nil, nil, nulls, offset), nil

	case *arrow.BooleanType:
		if v.BufferCount() < 2 {
			return nil, fmt.Errorf("%w: boolean array expects >=2 buffers, got %d", duckdb.ErrInvalidInput, v.BufferCount())
		}
		nbytes := int(bitutil.BytesForBits(int64(total)))
		bufs := []*memory.Buffer{
			copyBuffer(alloc, v.Buffer(0), nbytes),
			copyBuffer(alloc, v.Buffer(1), nbytes),
		}
		data := array.NewData(dt, length, bufs, nil, nulls, offset)
		releaseBuffers(bufs)
		return data, nil

	case arrow.FixedWidthDataType:
		if v.BufferCount() < 2 {
			return nil, fmt.Errorf("%w: fixed-width array %s expects >=2 buffers, got %d", duckdb.ErrInvalidInput, dt, v.BufferCount())
		}
		valBytes := total * t.Bytes()
		bufs := []*memory.Buffer{
			copyBuffer(alloc, v.Buffer(0), int(bitutil.BytesForBits(int64(total)))),
			copyBuffer(alloc, v.Buffer(1), valBytes),
		}
		data := array.NewData(dt, length, bufs, nil, nulls, offset)
		releaseBuffers(bufs)
		return data, nil

	case *arrow.BinaryType, *arrow.StringType:
		if v.BufferCount() < 3 {
			return nil, fmt.Errorf("%w: binary/string array expects >=3 buffers, got %d", duckdb.ErrInvalidInput, v.BufferCount())
		}
		offCount := total + 1
		offPtr := v.Buffer(1)
		var dataLen int
		if offPtr != nil && offCount > 0 {
			offs := unsafe.Slice((*int32)(offPtr), offCount)
			dataLen = int(offs[total])
		}
		bufs := []*memory.Buffer{
			copyBuffer(alloc, v.Buffer(0), int(bitutil.BytesForBits(int64(total)))),
			copyBuffer(alloc, offPtr, offCount*4),
			copyBuffer(alloc, v.Buffer(2), dataLen),
		}
		data := array.NewData(dt, length, bufs, nil, nulls, offset)
		releaseBuffers(bufs)
		return data, nil

	case *arrow.LargeBinaryType, *arrow.LargeStringType:
		if v.BufferCount() < 3 {
			return nil, fmt.Errorf("%w: large binary/string array expects >=3 buffers, got %d", duckdb.ErrInvalidInput, v.BufferCount())
		}
		offCount := total + 1
		offPtr := v.Buffer(1)
		var dataLen int
		if offPtr != nil && offCount > 0 {
			offs := unsafe.Slice((*int64)(offPtr), offCount)
			dataLen = int(offs[total])
		}
		bufs := []*memory.Buffer{
			copyBuffer(alloc, v.Buffer(0), int(bitutil.BytesForBits(int64(total)))),
			copyBuffer(alloc, offPtr, offCount*8),
			copyBuffer(alloc, v.Buffer(2), dataLen),
		}
		data := array.NewData(dt, length, bufs, nil, nulls, offset)
		releaseBuffers(bufs)
		return data, nil

	case *arrow.BinaryViewType, *arrow.StringViewType:
		// Arrow C Data Interface for BinaryView/StringView:
		// buffer[0] = validity, buffer[1] = views (16B * total),
		// buffer[2..n-2] = variadic data buffers,
		// buffer[n-1] = int64[n-3] sizes of variadic data buffers.
		nb := v.BufferCount()
		if nb < 3 {
			return nil, fmt.Errorf("%w: binary/string view array expects >=3 buffers, got %d", duckdb.ErrInvalidInput, nb)
		}
		nDataBufs := nb - 3
		bufs := make([]*memory.Buffer, 2+nDataBufs)
		bufs[0] = copyBuffer(alloc, v.Buffer(0), int(bitutil.BytesForBits(int64(total))))
		bufs[1] = copyBuffer(alloc, v.Buffer(1), total*arrow.ViewHeaderSizeBytes)
		if nDataBufs > 0 {
			sizesPtr := v.Buffer(nb - 1)
			if sizesPtr == nil {
				releaseBuffers(bufs)
				return nil, fmt.Errorf("%w: missing variadic buffer sizes for view array", duckdb.ErrInvalidInput)
			}
			sizes := unsafe.Slice((*int64)(sizesPtr), nDataBufs)
			for i := 0; i < nDataBufs; i++ {
				bufs[2+i] = copyBuffer(alloc, v.Buffer(2+i), int(sizes[i]))
			}
		}
		data := array.NewData(dt, length, bufs, nil, nulls, offset)
		releaseBuffers(bufs)
		return data, nil

	case *arrow.ListType:
		if v.BufferCount() < 2 || v.ChildCount() != 1 {
			return nil, fmt.Errorf("%w: list array invalid buffers/children", duckdb.ErrInvalidInput)
		}
		childData, err := importArrayData(t.Elem(), v.Child(0), alloc, depth+1)
		if err != nil {
			return nil, err
		}
		bufs := []*memory.Buffer{
			copyBuffer(alloc, v.Buffer(0), int(bitutil.BytesForBits(int64(total)))),
			copyBuffer(alloc, v.Buffer(1), (total+1)*4),
		}
		children := []arrow.ArrayData{childData}
		data := array.NewData(dt, length, bufs, children, nulls, offset)
		releaseBuffers(bufs)
		releaseChildren(children)
		return data, nil

	case *arrow.LargeListType:
		if v.BufferCount() < 2 || v.ChildCount() != 1 {
			return nil, fmt.Errorf("%w: large list array invalid buffers/children", duckdb.ErrInvalidInput)
		}
		childData, err := importArrayData(t.Elem(), v.Child(0), alloc, depth+1)
		if err != nil {
			return nil, err
		}
		bufs := []*memory.Buffer{
			copyBuffer(alloc, v.Buffer(0), int(bitutil.BytesForBits(int64(total)))),
			copyBuffer(alloc, v.Buffer(1), (total+1)*8),
		}
		children := []arrow.ArrayData{childData}
		data := array.NewData(dt, length, bufs, children, nulls, offset)
		releaseBuffers(bufs)
		releaseChildren(children)
		return data, nil

	case *arrow.FixedSizeListType:
		if v.BufferCount() < 1 || v.ChildCount() != 1 {
			return nil, fmt.Errorf("%w: fixed-size list array invalid buffers/children", duckdb.ErrInvalidInput)
		}
		childData, err := importArrayData(t.Elem(), v.Child(0), alloc, depth+1)
		if err != nil {
			return nil, err
		}
		bufs := []*memory.Buffer{
			copyBuffer(alloc, v.Buffer(0), int(bitutil.BytesForBits(int64(total)))),
		}
		children := []arrow.ArrayData{childData}
		data := array.NewData(dt, length, bufs, children, nulls, offset)
		releaseBuffers(bufs)
		releaseChildren(children)
		return data, nil

	case *arrow.StructType:
		if v.BufferCount() < 1 || v.ChildCount() != t.NumFields() {
			return nil, fmt.Errorf("%w: struct array child count %d != %d", duckdb.ErrInvalidInput, v.ChildCount(), t.NumFields())
		}
		children := make([]arrow.ArrayData, t.NumFields())
		for i := 0; i < t.NumFields(); i++ {
			cd, err := importArrayData(t.Field(i).Type, v.Child(i), alloc, depth+1)
			if err != nil {
				releaseChildren(children)
				return nil, err
			}
			children[i] = cd
		}
		bufs := []*memory.Buffer{
			copyBuffer(alloc, v.Buffer(0), int(bitutil.BytesForBits(int64(total)))),
		}
		data := array.NewData(dt, length, bufs, children, nulls, offset)
		releaseBuffers(bufs)
		releaseChildren(children)
		return data, nil

	case *arrow.MapType:
		if v.BufferCount() < 2 || v.ChildCount() != 1 {
			return nil, fmt.Errorf("%w: map array invalid buffers/children", duckdb.ErrInvalidInput)
		}
		childData, err := importArrayData(t.Elem(), v.Child(0), alloc, depth+1)
		if err != nil {
			return nil, err
		}
		bufs := []*memory.Buffer{
			copyBuffer(alloc, v.Buffer(0), int(bitutil.BytesForBits(int64(total)))),
			copyBuffer(alloc, v.Buffer(1), (total+1)*4),
		}
		children := []arrow.ArrayData{childData}
		data := array.NewData(dt, length, bufs, children, nulls, offset)
		releaseBuffers(bufs)
		releaseChildren(children)
		return data, nil

	default:
		return nil, fmt.Errorf("%w: unsupported Arrow data type %s", duckdb.ErrUnsupportedType, dt)
	}
}
