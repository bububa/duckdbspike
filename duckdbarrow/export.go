package duckdbarrow

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/bububa/duckdbspike/duckdb"
)

const (
	maxExportDepth            = 64
	arrowExtensionNameKey     = "ARROW:extension:name"
	arrowExtensionMetadataKey = "ARROW:extension:metadata"
)

// ExportSchema converts a Go *arrow.Schema into a pinned, C Data Interface
// *duckdb.ArrowSchema without cgo. Callers must call Release on the returned
// schema unless ownership is transferred (for example to Conn.RegisterArrowView).
func ExportSchema(schema *arrow.Schema) (*duckdb.ArrowSchema, error) {
	if schema == nil {
		return nil, fmt.Errorf("%w: nil Arrow schema", duckdb.ErrInvalidInput)
	}
	children := make([]duckdb.ArrowSchemaSpec, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		cs, err := exportFieldSpec(schema.Field(i), 0)
		if err != nil {
			return nil, fmt.Errorf("field %d (%q): %w", i, schema.Field(i).Name, err)
		}
		children[i] = cs
	}
	root := duckdb.ArrowSchemaSpec{
		Format:   "+s",
		Metadata: encodeArrowMetadata(schema.Metadata()),
		Children: children,
	}
	return duckdb.NewArrowSchema(root)
}

func encodeArrowMetadata(md arrow.Metadata) []byte {
	n := md.Len()
	if n == 0 {
		return nil
	}
	size := 4
	for i := range n {
		size += 4 + len(md.Keys()[i]) + 4 + len(md.Values()[i])
	}
	out := make([]byte, 0, size)
	out = binary.NativeEndian.AppendUint32(out, uint32(int32(n)))
	for i := range n {
		k := md.Keys()[i]
		v := md.Values()[i]
		out = binary.NativeEndian.AppendUint32(out, uint32(int32(len(k))))
		out = append(out, k...)
		out = binary.NativeEndian.AppendUint32(out, uint32(int32(len(v))))
		out = append(out, v...)
	}
	return out
}

func mergeExtensionMetadata(base arrow.Metadata, ext arrow.ExtensionType) arrow.Metadata {
	keys := make([]string, 0, base.Len()+2)
	vals := make([]string, 0, base.Len()+2)
	hasName := false
	hasMeta := false
	for i := 0; i < base.Len(); i++ {
		k := base.Keys()[i]
		switch k {
		case arrowExtensionNameKey:
			hasName = true
		case arrowExtensionMetadataKey:
			hasMeta = true
		}
		keys = append(keys, k)
		vals = append(vals, base.Values()[i])
	}
	if !hasName {
		keys = append(keys, arrowExtensionNameKey)
		vals = append(vals, ext.ExtensionName())
	}
	if !hasMeta {
		if ser := ext.Serialize(); ser != "" {
			keys = append(keys, arrowExtensionMetadataKey)
			vals = append(vals, ser)
		}
	}
	return arrow.NewMetadata(keys, vals)
}

func exportFieldSpec(f arrow.Field, depth int) (duckdb.ArrowSchemaSpec, error) {
	if depth > maxExportDepth {
		return duckdb.ArrowSchemaSpec{}, fmt.Errorf("%w: Arrow schema nesting depth exceeds %d", duckdb.ErrLimitExceeded, maxExportDepth)
	}
	md := f.Metadata
	dt := f.Type
	if ext, ok := dt.(arrow.ExtensionType); ok {
		md = mergeExtensionMetadata(md, ext)
		dt = ext.StorageType()
	}
	spec, err := exportDataTypeSpec(dt, depth)
	if err != nil {
		return duckdb.ArrowSchemaSpec{}, err
	}
	spec.Name = f.Name
	spec.Metadata = encodeArrowMetadata(md)
	if f.Nullable {
		spec.Flags |= arrowFlagNullable
	}
	return spec, nil
}

func exportDataTypeSpec(dt arrow.DataType, depth int) (duckdb.ArrowSchemaSpec, error) {
	if dt == nil {
		return duckdb.ArrowSchemaSpec{}, fmt.Errorf("%w: nil Arrow data type", duckdb.ErrInvalidInput)
	}
	if depth > maxExportDepth {
		return duckdb.ArrowSchemaSpec{}, fmt.Errorf("%w: Arrow schema nesting depth exceeds %d", duckdb.ErrLimitExceeded, maxExportDepth)
	}
	if ext, ok := dt.(arrow.ExtensionType); ok {
		return exportDataTypeSpec(ext.StorageType(), depth)
	}
	switch t := dt.(type) {
	case *arrow.NullType:
		return duckdb.ArrowSchemaSpec{Format: "n"}, nil
	case *arrow.BooleanType:
		return duckdb.ArrowSchemaSpec{Format: "b"}, nil
	case *arrow.Int8Type:
		return duckdb.ArrowSchemaSpec{Format: "c"}, nil
	case *arrow.Int16Type:
		return duckdb.ArrowSchemaSpec{Format: "s"}, nil
	case *arrow.Int32Type:
		return duckdb.ArrowSchemaSpec{Format: "i"}, nil
	case *arrow.Int64Type:
		return duckdb.ArrowSchemaSpec{Format: "l"}, nil
	case *arrow.Uint8Type:
		return duckdb.ArrowSchemaSpec{Format: "C"}, nil
	case *arrow.Uint16Type:
		return duckdb.ArrowSchemaSpec{Format: "S"}, nil
	case *arrow.Uint32Type:
		return duckdb.ArrowSchemaSpec{Format: "I"}, nil
	case *arrow.Uint64Type:
		return duckdb.ArrowSchemaSpec{Format: "L"}, nil
	case *arrow.Float16Type:
		return duckdb.ArrowSchemaSpec{Format: "e"}, nil
	case *arrow.Float32Type:
		return duckdb.ArrowSchemaSpec{Format: "f"}, nil
	case *arrow.Float64Type:
		return duckdb.ArrowSchemaSpec{Format: "g"}, nil
	case *arrow.BinaryType:
		return duckdb.ArrowSchemaSpec{Format: "z"}, nil
	case *arrow.LargeBinaryType:
		return duckdb.ArrowSchemaSpec{Format: "Z"}, nil
	case *arrow.BinaryViewType:
		return duckdb.ArrowSchemaSpec{Format: "vz"}, nil
	case *arrow.StringType:
		return duckdb.ArrowSchemaSpec{Format: "u"}, nil
	case *arrow.LargeStringType:
		return duckdb.ArrowSchemaSpec{Format: "U"}, nil
	case *arrow.StringViewType:
		return duckdb.ArrowSchemaSpec{Format: "vu"}, nil
	case *arrow.FixedSizeBinaryType:
		return duckdb.ArrowSchemaSpec{Format: fmt.Sprintf("w:%d", t.ByteWidth)}, nil
	case *arrow.Decimal32Type:
		return duckdb.ArrowSchemaSpec{Format: fmt.Sprintf("d:%d,%d,32", t.Precision, t.Scale)}, nil
	case *arrow.Decimal64Type:
		return duckdb.ArrowSchemaSpec{Format: fmt.Sprintf("d:%d,%d,64", t.Precision, t.Scale)}, nil
	case *arrow.Decimal128Type:
		return duckdb.ArrowSchemaSpec{Format: fmt.Sprintf("d:%d,%d", t.Precision, t.Scale)}, nil
	case *arrow.Decimal256Type:
		return duckdb.ArrowSchemaSpec{Format: fmt.Sprintf("d:%d,%d,256", t.Precision, t.Scale)}, nil
	case *arrow.Date32Type:
		return duckdb.ArrowSchemaSpec{Format: "tdD"}, nil
	case *arrow.Date64Type:
		return duckdb.ArrowSchemaSpec{Format: "tdm"}, nil
	case *arrow.Time32Type:
		switch t.Unit {
		case arrow.Second:
			return duckdb.ArrowSchemaSpec{Format: "tts"}, nil
		case arrow.Millisecond:
			return duckdb.ArrowSchemaSpec{Format: "ttm"}, nil
		default:
			return duckdb.ArrowSchemaSpec{}, fmt.Errorf("%w: unsupported time32 unit %v", duckdb.ErrUnsupportedType, t.Unit)
		}
	case *arrow.Time64Type:
		switch t.Unit {
		case arrow.Microsecond:
			return duckdb.ArrowSchemaSpec{Format: "ttu"}, nil
		case arrow.Nanosecond:
			return duckdb.ArrowSchemaSpec{Format: "ttn"}, nil
		default:
			return duckdb.ArrowSchemaSpec{}, fmt.Errorf("%w: unsupported time64 unit %v", duckdb.ErrUnsupportedType, t.Unit)
		}
	case *arrow.TimestampType:
		var u byte
		switch t.Unit {
		case arrow.Second:
			u = 's'
		case arrow.Millisecond:
			u = 'm'
		case arrow.Microsecond:
			u = 'u'
		case arrow.Nanosecond:
			u = 'n'
		default:
			return duckdb.ArrowSchemaSpec{}, fmt.Errorf("%w: unsupported timestamp unit %v", duckdb.ErrUnsupportedType, t.Unit)
		}
		return duckdb.ArrowSchemaSpec{Format: fmt.Sprintf("ts%c:%s", u, t.TimeZone)}, nil
	case *arrow.MonthIntervalType:
		return duckdb.ArrowSchemaSpec{Format: "tiM"}, nil
	case *arrow.DayTimeIntervalType:
		return duckdb.ArrowSchemaSpec{Format: "tiD"}, nil
	case *arrow.MonthDayNanoIntervalType:
		return duckdb.ArrowSchemaSpec{Format: "tin"}, nil
	case *arrow.DurationType:
		switch t.Unit {
		case arrow.Second:
			return duckdb.ArrowSchemaSpec{Format: "tDs"}, nil
		case arrow.Millisecond:
			return duckdb.ArrowSchemaSpec{Format: "tDm"}, nil
		case arrow.Microsecond:
			return duckdb.ArrowSchemaSpec{Format: "tDu"}, nil
		case arrow.Nanosecond:
			return duckdb.ArrowSchemaSpec{Format: "tDn"}, nil
		default:
			return duckdb.ArrowSchemaSpec{}, fmt.Errorf("%w: unsupported duration unit %v", duckdb.ErrUnsupportedType, t.Unit)
		}
	case *arrow.DenseUnionType:
		return exportUnionSchemaSpec("+ud:", t.Fields(), t.TypeCodes(), depth)
	case *arrow.SparseUnionType:
		return exportUnionSchemaSpec("+us:", t.Fields(), t.TypeCodes(), depth)
	case *arrow.DictionaryType:
		idxSpec, err := exportDataTypeSpec(t.IndexType, depth+1)
		if err != nil {
			return duckdb.ArrowSchemaSpec{}, err
		}
		valSpec, err := exportDataTypeSpec(t.ValueType, depth+1)
		if err != nil {
			return duckdb.ArrowSchemaSpec{}, err
		}
		idxSpec.Dictionary = &valSpec
		if t.Ordered {
			idxSpec.Flags |= arrowFlagDictOrdered
		}
		return idxSpec, nil
	case *arrow.ListType:
		elem, err := exportFieldSpec(t.ElemField(), depth+1)
		if err != nil {
			return duckdb.ArrowSchemaSpec{}, err
		}
		return duckdb.ArrowSchemaSpec{
			Format:   "+l",
			Children: []duckdb.ArrowSchemaSpec{elem},
		}, nil
	case *arrow.LargeListType:
		elem, err := exportFieldSpec(t.ElemField(), depth+1)
		if err != nil {
			return duckdb.ArrowSchemaSpec{}, err
		}
		return duckdb.ArrowSchemaSpec{
			Format:   "+L",
			Children: []duckdb.ArrowSchemaSpec{elem},
		}, nil
	case *arrow.ListViewType:
		elem, err := exportFieldSpec(t.ElemField(), depth+1)
		if err != nil {
			return duckdb.ArrowSchemaSpec{}, err
		}
		return duckdb.ArrowSchemaSpec{
			Format:   "+vl",
			Children: []duckdb.ArrowSchemaSpec{elem},
		}, nil
	case *arrow.LargeListViewType:
		elem, err := exportFieldSpec(t.ElemField(), depth+1)
		if err != nil {
			return duckdb.ArrowSchemaSpec{}, err
		}
		return duckdb.ArrowSchemaSpec{
			Format:   "+vL",
			Children: []duckdb.ArrowSchemaSpec{elem},
		}, nil
	case *arrow.RunEndEncodedType:
		fields := t.Fields()
		children := make([]duckdb.ArrowSchemaSpec, len(fields))
		for i, sf := range fields {
			cs, err := exportFieldSpec(sf, depth+1)
			if err != nil {
				return duckdb.ArrowSchemaSpec{}, fmt.Errorf("run-end encoded field %d (%q): %w", i, sf.Name, err)
			}
			children[i] = cs
		}
		return duckdb.ArrowSchemaSpec{
			Format:   "+r",
			Children: children,
		}, nil
	case *arrow.FixedSizeListType:
		elem, err := exportFieldSpec(t.ElemField(), depth+1)
		if err != nil {
			return duckdb.ArrowSchemaSpec{}, err
		}
		return duckdb.ArrowSchemaSpec{
			Format:   fmt.Sprintf("+w:%d", t.Len()),
			Children: []duckdb.ArrowSchemaSpec{elem},
		}, nil
	case *arrow.StructType:
		fields := t.Fields()
		children := make([]duckdb.ArrowSchemaSpec, len(fields))
		for i, sf := range fields {
			cs, err := exportFieldSpec(sf, depth+1)
			if err != nil {
				return duckdb.ArrowSchemaSpec{}, fmt.Errorf("struct field %d (%q): %w", i, sf.Name, err)
			}
			children[i] = cs
		}
		return duckdb.ArrowSchemaSpec{
			Format:   "+s",
			Children: children,
		}, nil
	case *arrow.MapType:
		elem, err := exportFieldSpec(t.ElemField(), depth+1)
		if err != nil {
			return duckdb.ArrowSchemaSpec{}, err
		}
		var flags int64
		if t.KeysSorted {
			flags |= arrowFlagMapKeysSort
		}
		return duckdb.ArrowSchemaSpec{
			Format:   "+m",
			Flags:    flags,
			Children: []duckdb.ArrowSchemaSpec{elem},
		}, nil
	default:
		return duckdb.ArrowSchemaSpec{}, fmt.Errorf("%w: unsupported Arrow data type %s", duckdb.ErrUnsupportedType, dt)
	}
}

// ExportRecordBatch exports rec as a pinned C Data Interface *duckdb.ArrowArray
// without cgo. ExportRecordBatch retains rec and releases it automatically when
// Go or DuckDB invokes the ArrowArray release callback. Callers must call
// Release on the returned *duckdb.ArrowArray unless ownership is transferred to
// Appender.AppendArrowChunk or Conn.RegisterArrowView.
func ExportRecordBatch(rec arrow.RecordBatch) (*duckdb.ArrowArray, error) {
	if rec == nil {
		return nil, fmt.Errorf("%w: nil Arrow RecordBatch", duckdb.ErrInvalidInput)
	}
	numCols := int(rec.NumCols())
	children := make([]duckdb.ArrowArraySpec, numCols)
	for i := range numCols {
		col := rec.Column(i)
		if col == nil {
			return nil, fmt.Errorf("%w: column %d is nil", duckdb.ErrInvalidInput, i)
		}
		cs, err := exportArrayDataSpec(col.Data(), 0)
		if err != nil {
			return nil, fmt.Errorf("column %d: %w", i, err)
		}
		children[i] = cs
	}
	root := duckdb.ArrowArraySpec{
		Length:    rec.NumRows(),
		NullCount: 0,
		Offset:    0,
		Buffers:   [][]byte{nil}, // root struct array has 1 buffer (NULL validity = all valid)
		Children:  children,
	}
	rec.Retain()
	arr, err := duckdb.NewArrowArray(root, rec.Release)
	if err != nil {
		rec.Release()
		return nil, err
	}
	return arr, nil
}

func bufferBytes(buf *memory.Buffer, nonNilEmpty bool) []byte {
	if buf == nil {
		if nonNilEmpty {
			return []byte{}
		}
		return nil
	}
	b := buf.Bytes()
	if b == nil && nonNilEmpty {
		return []byte{}
	}
	return b
}

func exportArrayDataSpec(data arrow.ArrayData, depth int) (duckdb.ArrowArraySpec, error) {
	if data == nil {
		return duckdb.ArrowArraySpec{}, fmt.Errorf("%w: nil Arrow ArrayData", duckdb.ErrInvalidInput)
	}
	if depth > maxExportDepth {
		return duckdb.ArrowArraySpec{}, fmt.Errorf("%w: Arrow array nesting depth exceeds %d", duckdb.ErrLimitExceeded, maxExportDepth)
	}
	dt := data.DataType()
	if ext, ok := dt.(arrow.ExtensionType); ok {
		dt = ext.StorageType()
	}
	spec := duckdb.ArrowArraySpec{
		Length:    int64(data.Len()),
		NullCount: int64(data.NullN()),
		Offset:    int64(data.Offset()),
	}
	srcBufs := data.Buffers()
	getBuf := func(i int, nonNilEmpty bool) []byte {
		if i >= len(srcBufs) {
			if nonNilEmpty {
				return []byte{}
			}
			return nil
		}
		return bufferBytes(srcBufs[i], nonNilEmpty)
	}

	if _, isDict := dt.(*arrow.DictionaryType); isDict {
		spec.Buffers = [][]byte{
			getBuf(0, false),
			getBuf(1, true),
		}
		dictSpec, err := exportArrayDataSpec(data.Dictionary(), depth+1)
		if err != nil {
			return duckdb.ArrowArraySpec{}, fmt.Errorf("dictionary: %w", err)
		}
		spec.Dictionary = &dictSpec
		return spec, nil
	}

	switch dt.(type) {
	case *arrow.NullType:
		// 0 buffers.
		return spec, nil

	case *arrow.BooleanType, arrow.FixedWidthDataType:
		spec.Buffers = [][]byte{
			getBuf(0, false),
			getBuf(1, true),
		}
		return spec, nil

	case *arrow.BinaryType, *arrow.StringType, *arrow.LargeBinaryType, *arrow.LargeStringType:
		spec.Buffers = [][]byte{
			getBuf(0, false),
			getBuf(1, true),
			getBuf(2, true),
		}
		return spec, nil

	case *arrow.BinaryViewType, *arrow.StringViewType:
		nDataBufs := 0
		if len(srcBufs) > 2 {
			nDataBufs = len(srcBufs) - 2
		}
		bufs := make([][]byte, 3+nDataBufs)
		bufs[0] = getBuf(0, false)
		bufs[1] = getBuf(1, true)
		sizes := make([]byte, 0, nDataBufs*8)
		for i := 0; i < nDataBufs; i++ {
			db := getBuf(2+i, true)
			bufs[2+i] = db
			sizes = binary.NativeEndian.AppendUint64(sizes, uint64(int64(len(db))))
		}
		bufs[2+nDataBufs] = sizes
		spec.Buffers = bufs
		return spec, nil

	case *arrow.ListType, *arrow.LargeListType, *arrow.MapType:
		if len(data.Children()) != 1 {
			return duckdb.ArrowArraySpec{}, fmt.Errorf("%w: %s expects 1 child, got %d", duckdb.ErrInvalidInput, dt, len(data.Children()))
		}
		child, err := exportArrayDataSpec(data.Children()[0], depth+1)
		if err != nil {
			return duckdb.ArrowArraySpec{}, err
		}
		spec.Buffers = [][]byte{
			getBuf(0, false),
			getBuf(1, true),
		}
		spec.Children = []duckdb.ArrowArraySpec{child}
		return spec, nil

	case *arrow.ListViewType, *arrow.LargeListViewType:
		if len(data.Children()) != 1 {
			return duckdb.ArrowArraySpec{}, fmt.Errorf("%w: %s expects 1 child, got %d", duckdb.ErrInvalidInput, dt, len(data.Children()))
		}
		child, err := exportArrayDataSpec(data.Children()[0], depth+1)
		if err != nil {
			return duckdb.ArrowArraySpec{}, err
		}
		spec.Buffers = [][]byte{
			getBuf(0, false),
			getBuf(1, true),
			getBuf(2, true),
		}
		spec.Children = []duckdb.ArrowArraySpec{child}
		return spec, nil

	case *arrow.RunEndEncodedType:
		kids := data.Children()
		if len(kids) != 2 {
			return duckdb.ArrowArraySpec{}, fmt.Errorf("%w: run-end encoded expects 2 children, got %d", duckdb.ErrInvalidInput, len(kids))
		}
		runEnds, err := exportArrayDataSpec(kids[0], depth+1)
		if err != nil {
			return duckdb.ArrowArraySpec{}, fmt.Errorf("run_ends: %w", err)
		}
		vals, err := exportArrayDataSpec(kids[1], depth+1)
		if err != nil {
			return duckdb.ArrowArraySpec{}, fmt.Errorf("values: %w", err)
		}
		spec.NullCount = 0
		spec.Buffers = nil
		spec.Children = []duckdb.ArrowArraySpec{runEnds, vals}
		return spec, nil

	case *arrow.FixedSizeListType:
		if len(data.Children()) != 1 {
			return duckdb.ArrowArraySpec{}, fmt.Errorf("%w: fixed-size list expects 1 child, got %d", duckdb.ErrInvalidInput, len(data.Children()))
		}
		child, err := exportArrayDataSpec(data.Children()[0], depth+1)
		if err != nil {
			return duckdb.ArrowArraySpec{}, err
		}
		spec.Buffers = [][]byte{getBuf(0, false)}
		spec.Children = []duckdb.ArrowArraySpec{child}
		return spec, nil

	case *arrow.StructType:
		kids := data.Children()
		children := make([]duckdb.ArrowArraySpec, len(kids))
		for i, k := range kids {
			cs, err := exportArrayDataSpec(k, depth+1)
			if err != nil {
				return duckdb.ArrowArraySpec{}, fmt.Errorf("struct child %d: %w", i, err)
			}
			children[i] = cs
		}
		spec.Buffers = [][]byte{getBuf(0, false)}
		spec.Children = children
		return spec, nil

	case *arrow.DenseUnionType:
		kids := data.Children()
		children := make([]duckdb.ArrowArraySpec, len(kids))
		for i, k := range kids {
			cs, err := exportArrayDataSpec(k, depth+1)
			if err != nil {
				return duckdb.ArrowArraySpec{}, fmt.Errorf("dense union child %d: %w", i, err)
			}
			children[i] = cs
		}
		// arrow-go stores [nil, typeIDs, offsets]; C Data Interface expects [typeIDs, offsets].
		spec.NullCount = 0
		spec.Buffers = [][]byte{
			getBuf(1, true),
			getBuf(2, true),
		}
		spec.Children = children
		return spec, nil

	case *arrow.SparseUnionType:
		kids := data.Children()
		children := make([]duckdb.ArrowArraySpec, len(kids))
		for i, k := range kids {
			cs, err := exportArrayDataSpec(k, depth+1)
			if err != nil {
				return duckdb.ArrowArraySpec{}, fmt.Errorf("sparse union child %d: %w", i, err)
			}
			children[i] = cs
		}
		// arrow-go stores [nil, typeIDs]; C Data Interface expects [typeIDs].
		spec.NullCount = 0
		spec.Buffers = [][]byte{
			getBuf(1, true),
		}
		spec.Children = children
		return spec, nil

	default:
		return duckdb.ArrowArraySpec{}, fmt.Errorf("%w: unsupported Arrow data type %s", duckdb.ErrUnsupportedType, dt)
	}
}

// AppendRecordBatch exports rec via the Arrow C Data Interface and ingests it
// directly into app using Appender.AppendArrowChunk.
func AppendRecordBatch(app *duckdb.Appender, rec arrow.RecordBatch) error {
	if app == nil {
		return duckdb.ErrClosed
	}
	if rec == nil {
		return fmt.Errorf("%w: nil Arrow RecordBatch", duckdb.ErrInvalidInput)
	}
	if rec.NumRows() == 0 {
		return nil
	}
	schema, err := ExportSchema(rec.Schema())
	if err != nil {
		return err
	}
	defer schema.Release()
	arr, err := ExportRecordBatch(rec)
	if err != nil {
		return err
	}
	defer arr.Release()
	return app.AppendArrowChunk(schema, arr)
}

// AppendRecordReader streams every non-empty RecordBatch from reader into app
// via AppendRecordBatch, reusing one exported ArrowSchema across all batches.
func AppendRecordReader(app *duckdb.Appender, reader array.RecordReader) error {
	if app == nil {
		return duckdb.ErrClosed
	}
	if reader == nil {
		return fmt.Errorf("%w: nil Arrow RecordReader", duckdb.ErrInvalidInput)
	}
	schema, err := ExportSchema(reader.Schema())
	if err != nil {
		return err
	}
	defer schema.Release()
	for reader.Next() {
		rec := reader.RecordBatch()
		if rec == nil || rec.NumRows() == 0 {
			continue
		}
		arr, err := ExportRecordBatch(rec)
		if err != nil {
			return err
		}
		if err := app.AppendArrowChunk(schema, arr); err != nil {
			arr.Release()
			return err
		}
	}
	return reader.Err()
}

// RegisterRecordBatch exports rec and registers it on conn or tx as a temporary
// DuckDB view named viewName via RegisterArrowView (duckdb_arrow_array_scan).
// Because DuckDB's underlying array-scan stream drains on first scan, the view
// is single-use; call Close on the returned *duckdb.ArrowView when done.
func RegisterRecordBatch(ctx context.Context, reg ArrowRegistrar, viewName string, rec arrow.RecordBatch) (*duckdb.ArrowView, error) {
	if reg == nil {
		return nil, duckdb.ErrClosed
	}
	if rec == nil {
		return nil, fmt.Errorf("%w: nil Arrow RecordBatch", duckdb.ErrInvalidInput)
	}
	schema, err := ExportSchema(rec.Schema())
	if err != nil {
		return nil, err
	}
	arr, err := ExportRecordBatch(rec)
	if err != nil {
		schema.Release()
		return nil, err
	}
	view, err := reg.RegisterArrowView(ctx, viewName, schema, arr)
	if err != nil {
		arr.Release()
		schema.Release()
		return nil, err
	}
	return view, nil
}

func exportUnionSchemaSpec(prefix string, fields []arrow.Field, codes []arrow.UnionTypeCode, depth int) (duckdb.ArrowSchemaSpec, error) {
	if len(fields) != len(codes) {
		return duckdb.ArrowSchemaSpec{}, fmt.Errorf("%w: union fields (%d) != type codes (%d)", duckdb.ErrInvalidInput, len(fields), len(codes))
	}
	codeStrs := make([]string, len(codes))
	children := make([]duckdb.ArrowSchemaSpec, len(fields))
	for i, f := range fields {
		codeStrs[i] = strconv.Itoa(int(codes[i]))
		cs, err := exportFieldSpec(f, depth+1)
		if err != nil {
			return duckdb.ArrowSchemaSpec{}, fmt.Errorf("union member %d (%q): %w", i, f.Name, err)
		}
		children[i] = cs
	}
	return duckdb.ArrowSchemaSpec{
		Format:   prefix + strings.Join(codeStrs, ","),
		Children: children,
	}, nil
}

type recordReaderStreamProvider struct {
	reader array.RecordReader
	closed bool
}

func (p *recordReaderStreamProvider) Schema() (*duckdb.ArrowSchema, error) {
	return ExportSchema(p.reader.Schema())
}

func (p *recordReaderStreamProvider) Next() (*duckdb.ArrowArray, error) {
	for p.reader.Next() {
		rec := p.reader.RecordBatch()
		if rec == nil || rec.NumRows() == 0 {
			continue
		}
		return ExportRecordBatch(rec)
	}
	if err := p.reader.Err(); err != nil {
		return nil, err
	}
	return nil, nil
}

func (p *recordReaderStreamProvider) Close() error {
	if !p.closed {
		p.closed = true
		p.reader.Release()
	}
	return nil
}

// RegisterRecordReader registers a multi-batch array.RecordReader as a
// streaming temporary DuckDB SQL view named viewName on conn or tx via
// RegisterArrowStream (duckdb_arrow_scan). Batches are pulled lazily as DuckDB
// scans the view without materializing the stream into a single batch.
// RegisterRecordReader retains reader and releases it when ArrowView.Close
// (or Conn.Close) runs. See Conn.RegisterArrowStream for DuckDB v1.5.6
// projection-pushdown caveats; for arbitrary column projections, prefer
// AppendRecordReader into a temporary table.
func RegisterRecordReader(ctx context.Context, reg ArrowRegistrar, viewName string, reader array.RecordReader) (*duckdb.ArrowView, error) {
	if reg == nil {
		return nil, duckdb.ErrClosed
	}
	if reader == nil {
		return nil, fmt.Errorf("%w: nil Arrow RecordReader", duckdb.ErrInvalidInput)
	}
	reader.Retain()
	view, err := reg.RegisterArrowStream(ctx, viewName, &recordReaderStreamProvider{reader: reader})
	if err != nil {
		reader.Release()
		return nil, err
	}
	return view, nil
}
