package duckdb

import (
	"fmt"
	"math"
	"math/big"
	"runtime"
	"unicode/utf8"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

func writeAt[T any](base uintptr, index uint64, val T) {
	var zero T
	stride := uintptr(unsafe.Sizeof(zero))
	*(*T)(abi.Pointer(base + uintptr(index)*stride)) = val
}

func setVectorNull(api *abi.API, vec uintptr, row uint64) {
	api.VectorEnsureValidityWritable(vec)
	vptr := api.VectorGetValidity(vec)
	if vptr == 0 {
		return
	}
	wordIdx := row / 64
	bit := uint64(1) << (row % 64)
	ptr := (*uint64)(abi.Pointer(vptr + uintptr(wordIdx)*8))
	*ptr &^= bit
}

func setVectorValid(api *abi.API, vec uintptr, row uint64) {
	vptr := api.VectorGetValidity(vec)
	if vptr == 0 {
		return
	}
	wordIdx := row / 64
	bit := uint64(1) << (row % 64)
	ptr := (*uint64)(abi.Pointer(vptr + uintptr(wordIdx)*8))
	*ptr |= bit
}

type listSizeUndo struct {
	vec      uintptr
	prevSize uint64
}

func (a *Appender) supportsChunkBufferingLocked() bool {
	api := a.conn.api
	return api.VectorSize != nil &&
		api.CreateDataChunk != nil &&
		api.DataChunkReset != nil &&
		api.DataChunkSetSize != nil &&
		api.DataChunkGetVector != nil &&
		api.DestroyDataChunk != nil &&
		api.VectorGetData != nil &&
		api.VectorEnsureValidityWritable != nil &&
		api.UnsafeVectorAssignStringElementLen != nil &&
		api.AppendDataChunk != nil
}

func (a *Appender) destroyChunkLocked() {
	if a.chunk != 0 {
		a.conn.api.DestroyDataChunk(&a.chunk)
		a.chunkRow = 0
		a.chunkCap = 0
	}
}

func (a *Appender) initChunkLocked(nativeTypes []uintptr) error {
	a.destroyChunkLocked()
	if !a.supportsChunkBufferingLocked() || len(nativeTypes) == 0 {
		return nil
	}
	api := a.conn.api
	vsz := api.VectorSize()
	if vsz == 0 {
		return nil
	}
	chunk := api.CreateDataChunk(&nativeTypes[0], uint64(len(nativeTypes)))
	runtime.KeepAlive(nativeTypes)
	if chunk == 0 {
		return nativeError("create_data_chunk", abi.ErrorOutOfMemory, "")
	}
	a.chunk = chunk
	a.chunkCap = vsz
	a.chunkRow = 0
	return nil
}

func (a *Appender) flushChunkLocked() error {
	if a.chunk == 0 || a.chunkRow == 0 {
		return nil
	}
	api := a.conn.api
	api.DataChunkSetSize(a.chunk, a.chunkRow)
	state := api.AppendDataChunk(a.handle, a.chunk)
	api.DataChunkReset(a.chunk)
	a.chunkRow = 0
	if state != abi.StateSuccess {
		return a.errorLocked("append_data_chunk")
	}
	return nil
}

func (a *Appender) appendRowChunkLocked(vals []Value) error {
	api := a.conn.api
	row := a.chunkRow
	var undo []listSizeUndo

	rollback := func() {
		for i := len(undo) - 1; i >= 0; i-- {
			_ = api.ListVectorSetSize(undo[i].vec, undo[i].prevSize)
		}
	}

	for colIdx, v := range vals {
		if v.IsDefault() {
			if api.AppendDefaultToChunk == nil {
				rollback()
				return fmt.Errorf("%w: appender col[%d] default-to-chunk unavailable", ErrUnsupportedType, colIdx)
			}
			if api.AppendDefaultToChunk(a.handle, a.chunk, uint64(colIdx), row) != abi.StateSuccess {
				rollback()
				return a.errorLocked(fmt.Sprintf("append_default_to_chunk[%d]", colIdx))
			}
			continue
		}
		vec := api.DataChunkGetVector(a.chunk, uint64(colIdx))
		if err := writeVectorValue(api, &a.plans[colIdx], vec, row, v, 0, a.conn.limits, &undo); err != nil {
			rollback()
			return fmt.Errorf("appender col[%d]: %w", colIdx, err)
		}
	}

	a.chunkRow++
	if a.chunkRow >= a.chunkCap {
		return a.flushChunkLocked()
	}
	return nil
}

func writeVectorValue(api *abi.API, plan *columnPlan, vec uintptr, row uint64, v Value, depth int, limits DecodeLimits, undo *[]listSizeUndo) error {
	if depth > limits.MaxNestingDepth {
		return fmt.Errorf("%w: appender nesting depth %d exceeds %d", ErrLimitExceeded, depth, limits.MaxNestingDepth)
	}
	if v.IsDefault() {
		return fmt.Errorf("%w: DefaultValue is only valid at top-level Appender columns", ErrInvalidInput)
	}
	if vec == 0 {
		return fmt.Errorf("%w: null native vector", ErrInvalidInput)
	}
	if v.IsNull() {
		setVectorNull(api, vec, row)
		// For STRUCT / UNION vectors, also mark child vectors NULL at row to
		// mirror DuckDB's FlatVector::SetNull recursive convention.
		switch plan.lt.Kind {
		case TypeStruct:
			for i := range plan.fields {
				childVec := api.StructVectorGetChild(vec, uint64(i))
				if childVec != 0 {
					setVectorNull(api, childVec, row)
				}
			}
		case TypeUnion:
			tagVec := api.StructVectorGetChild(vec, 0)
			if tagVec != 0 {
				setVectorNull(api, tagVec, row)
			}
			for i := range plan.fields {
				memberVec := api.StructVectorGetChild(vec, uint64(i+1))
				if memberVec != 0 {
					setVectorNull(api, memberVec, row)
				}
			}
		}
		return nil
	}

	setVectorValid(api, vec, row)

	switch plan.lt.Kind {
	case TypeSQLNull:
		setVectorNull(api, vec, row)
		return nil

	case TypeStruct:
		st, err := v.Struct()
		if err != nil {
			return err
		}
		if len(st.Fields) != len(plan.fields) {
			return fmt.Errorf("%w: struct field count %d != column field count %d", ErrTypeMismatch, len(st.Fields), len(plan.fields))
		}
		byName := make(map[string]Value, len(st.Fields))
		allMatchOrder := true
		for i, f := range st.Fields {
			if f.Name != plan.fields[i].name {
				allMatchOrder = false
			}
			byName[f.Name] = f.Value
		}
		for i := range plan.fields {
			var fv Value
			if allMatchOrder {
				fv = st.Fields[i].Value
			} else {
				var ok bool
				fv, ok = byName[plan.fields[i].name]
				if !ok {
					return fmt.Errorf("%w: struct missing field %q", ErrTypeMismatch, plan.fields[i].name)
				}
			}
			childVec := api.StructVectorGetChild(vec, uint64(i))
			if err := writeVectorValue(api, &plan.fields[i].plan, childVec, row, fv, depth+1, limits, undo); err != nil {
				return fmt.Errorf("field %q: %w", plan.fields[i].name, err)
			}
		}
		return nil

	case TypeUnion:
		u, err := v.Union()
		if err != nil {
			return err
		}
		if int(u.Tag) >= len(plan.fields) {
			return fmt.Errorf("%w: union tag %d out of range [0,%d)", ErrOutOfRange, u.Tag, len(plan.fields))
		}
		tagVec := api.StructVectorGetChild(vec, 0)
		if tagVec == 0 {
			return fmt.Errorf("%w: union vector missing tag child", ErrInvalidInput)
		}
		setVectorValid(api, tagVec, row)
		tagData := api.VectorGetData(tagVec)
		if tagData == 0 {
			return fmt.Errorf("%w: union tag vector has null data pointer", ErrInvalidInput)
		}
		writeAt[uint8](tagData, row, u.Tag)
		for i := range plan.fields {
			memberVec := api.StructVectorGetChild(vec, uint64(i+1))
			if uint8(i) == u.Tag {
				if err := writeVectorValue(api, &plan.fields[i].plan, memberVec, row, u.Value, depth+1, limits, undo); err != nil {
					return fmt.Errorf("union member %d (%q): %w", i, plan.fields[i].name, err)
				}
			} else if memberVec != 0 {
				setVectorNull(api, memberVec, row)
			}
		}
		return nil

	case TypeArray:
		av, err := v.Array()
		if err != nil {
			return err
		}
		size := plan.lt.ArraySize
		if uint64(len(av.Elements)) != size {
			return fmt.Errorf("%w: array length %d != column ARRAY[%d]", ErrTypeMismatch, len(av.Elements), size)
		}
		if size > 0 && row > math.MaxUint64/size {
			return fmt.Errorf("%w: array row offset overflow", ErrLimitExceeded)
		}
		base := row * size
		childVec := api.ArrayVectorGetChild(vec)
		for j := uint64(0); j < size; j++ {
			if err := writeVectorValue(api, plan.elem, childVec, base+j, av.Elements[j], depth+1, limits, undo); err != nil {
				return fmt.Errorf("[%d]: %w", j, err)
			}
		}
		return nil

	case TypeList:
		lv, err := v.List()
		if err != nil {
			return err
		}
		n := uint64(len(lv.Elements))
		curSize := api.ListVectorGetSize(vec)
		newSize := curSize + n
		if newSize < curSize {
			return fmt.Errorf("%w: list child size overflow", ErrLimitExceeded)
		}
		*undo = append(*undo, listSizeUndo{vec: vec, prevSize: curSize})
		if n > 0 {
			if api.ListVectorReserve(vec, newSize) != abi.StateSuccess {
				return nativeError("list_vector_reserve", abi.ErrorOutOfMemory, "")
			}
			if api.ListVectorSetSize(vec, newSize) != abi.StateSuccess {
				return nativeError("list_vector_set_size", abi.ErrorInvalid, "")
			}
		}
		data := api.VectorGetData(vec)
		if data == 0 {
			return fmt.Errorf("%w: list vector has null data pointer", ErrInvalidInput)
		}
		writeAt[abi.ListEntry](data, row, abi.ListEntry{Offset: curSize, Length: n})
		if n > 0 {
			childVec := api.ListVectorGetChild(vec)
			for j := uint64(0); j < n; j++ {
				if err := writeVectorValue(api, plan.elem, childVec, curSize+j, lv.Elements[j], depth+1, limits, undo); err != nil {
					return fmt.Errorf("[%d]: %w", j, err)
				}
			}
		}
		return nil

	case TypeMap:
		mv, err := v.Map()
		if err != nil {
			return err
		}
		n := uint64(len(mv.Entries))
		curSize := api.ListVectorGetSize(vec)
		newSize := curSize + n
		if newSize < curSize {
			return fmt.Errorf("%w: map child size overflow", ErrLimitExceeded)
		}
		*undo = append(*undo, listSizeUndo{vec: vec, prevSize: curSize})
		if n > 0 {
			if api.ListVectorReserve(vec, newSize) != abi.StateSuccess {
				return nativeError("list_vector_reserve", abi.ErrorOutOfMemory, "")
			}
			if api.ListVectorSetSize(vec, newSize) != abi.StateSuccess {
				return nativeError("list_vector_set_size", abi.ErrorInvalid, "")
			}
		}
		data := api.VectorGetData(vec)
		if data == 0 {
			return fmt.Errorf("%w: map vector has null data pointer", ErrInvalidInput)
		}
		writeAt[abi.ListEntry](data, row, abi.ListEntry{Offset: curSize, Length: n})
		if n > 0 {
			structVec := api.ListVectorGetChild(vec)
			keyVec := api.StructVectorGetChild(structVec, 0)
			valVec := api.StructVectorGetChild(structVec, 1)
			for j := uint64(0); j < n; j++ {
				if mv.Entries[j].Key.IsNull() {
					return fmt.Errorf("%w: map key[%d] must not be NULL", ErrInvalidInput, j)
				}
				setVectorValid(api, structVec, curSize+j)
				if err := writeVectorValue(api, plan.key, keyVec, curSize+j, mv.Entries[j].Key, depth+1, limits, undo); err != nil {
					return fmt.Errorf("map key[%d]: %w", j, err)
				}
				if err := writeVectorValue(api, plan.val, valVec, curSize+j, mv.Entries[j].Value, depth+1, limits, undo); err != nil {
					return fmt.Errorf("map val[%d]: %w", j, err)
				}
			}
		}
		return nil

	case TypeVarchar:
		s, err := v.Varchar()
		if err != nil {
			return err
		}
		if !utf8.ValidString(s) {
			return fmt.Errorf("%w: VARCHAR value is not valid UTF-8", ErrInvalidInput)
		}
		assignVectorBytes(api, vec, row, []byte(s))
		return nil

	case TypeBlob:
		b, err := v.Blob()
		if err != nil {
			return err
		}
		assignVectorBytes(api, vec, row, b)
		return nil

	case TypeBit:
		b, err := v.Bit()
		if err != nil {
			return err
		}
		if err := b.Validate(); err != nil {
			return err
		}
		assignVectorBytes(api, vec, row, b.Data)
		return nil

	case TypeBignum:
		bn, err := v.Bignum()
		if err != nil {
			return err
		}
		wire := encodeBignumWire(bn)
		assignVectorBytes(api, vec, row, wire)
		return nil
	}

	data := api.VectorGetData(vec)
	if data == 0 {
		return fmt.Errorf("%w: null vector data pointer for %s", ErrInvalidInput, plan.lt.Kind)
	}

	switch plan.lt.Kind {
	case TypeBoolean:
		b, err := v.Bool()
		if err != nil {
			return err
		}
		var raw byte
		if b {
			raw = 1
		}
		writeAt[byte](data, row, raw)
		return nil

	case TypeTinyint:
		n, err := v.AsInt64()
		if err != nil {
			return err
		}
		if n < math.MinInt8 || n > math.MaxInt8 {
			return fmt.Errorf("%w: %d out of TINYINT range", ErrOutOfRange, n)
		}
		writeAt[int8](data, row, int8(n))
		return nil

	case TypeSmallint:
		n, err := v.AsInt64()
		if err != nil {
			return err
		}
		if n < math.MinInt16 || n > math.MaxInt16 {
			return fmt.Errorf("%w: %d out of SMALLINT range", ErrOutOfRange, n)
		}
		writeAt[int16](data, row, int16(n))
		return nil

	case TypeInteger:
		n, err := v.AsInt64()
		if err != nil {
			return err
		}
		if n < math.MinInt32 || n > math.MaxInt32 {
			return fmt.Errorf("%w: %d out of INTEGER range", ErrOutOfRange, n)
		}
		writeAt[int32](data, row, int32(n))
		return nil

	case TypeBigint:
		n, err := v.AsInt64()
		if err != nil {
			return err
		}
		writeAt[int64](data, row, n)
		return nil

	case TypeUTinyint:
		u, err := v.AsUint64()
		if err != nil {
			return err
		}
		if u > math.MaxUint8 {
			return fmt.Errorf("%w: %d out of UTINYINT range", ErrOutOfRange, u)
		}
		writeAt[uint8](data, row, uint8(u))
		return nil

	case TypeUSmallint:
		u, err := v.AsUint64()
		if err != nil {
			return err
		}
		if u > math.MaxUint16 {
			return fmt.Errorf("%w: %d out of USMALLINT range", ErrOutOfRange, u)
		}
		writeAt[uint16](data, row, uint16(u))
		return nil

	case TypeUInteger:
		u, err := v.AsUint64()
		if err != nil {
			return err
		}
		if u > math.MaxUint32 {
			return fmt.Errorf("%w: %d out of UINTEGER range", ErrOutOfRange, u)
		}
		writeAt[uint32](data, row, uint32(u))
		return nil

	case TypeUBigint:
		u, err := v.AsUint64()
		if err != nil {
			return err
		}
		writeAt[uint64](data, row, u)
		return nil

	case TypeHugeint:
		var h Hugeint
		if v.Kind() == TypeHugeint {
			var err error
			h, err = v.Hugeint()
			if err != nil {
				return err
			}
		} else {
			n, err := v.AsInt64()
			if err != nil {
				return err
			}
			h = HugeintFromInt64(n)
		}
		writeAt[abi.Hugeint](data, row, abi.Hugeint{Lower: h.Lower, Upper: h.Upper})
		return nil

	case TypeUhugeint:
		var u Uhugeint
		if v.Kind() == TypeUhugeint {
			var err error
			u, err = v.Uhugeint()
			if err != nil {
				return err
			}
		} else {
			n, err := v.AsUint64()
			if err != nil {
				return err
			}
			u = UhugeintFromUint64(n)
		}
		writeAt[abi.Uhugeint](data, row, abi.Uhugeint{Lower: u.Lower, Upper: u.Upper})
		return nil

	case TypeFloat:
		f, err := v.Float32()
		if err != nil {
			return err
		}
		writeAt[float32](data, row, f)
		return nil

	case TypeDouble:
		var d float64
		if v.Kind() == TypeFloat {
			f, err := v.Float32()
			if err != nil {
				return err
			}
			d = float64(f)
		} else {
			var err error
			d, err = v.Float64()
			if err != nil {
				return err
			}
		}
		writeAt[float64](data, row, d)
		return nil

	case TypeDecimal:
		coef, err := coerceDecimalForColumn(v, plan.lt.DecimalWidth, plan.lt.DecimalScale)
		if err != nil {
			return err
		}
		switch plan.decimalInternal {
		case abi.TypeSmallint:
			n, ok := coef.Int64()
			if !ok || n < math.MinInt16 || n > math.MaxInt16 {
				return fmt.Errorf("%w: decimal coefficient overflows SMALLINT storage", ErrOutOfRange)
			}
			writeAt[int16](data, row, int16(n))
		case abi.TypeInteger:
			n, ok := coef.Int64()
			if !ok || n < math.MinInt32 || n > math.MaxInt32 {
				return fmt.Errorf("%w: decimal coefficient overflows INTEGER storage", ErrOutOfRange)
			}
			writeAt[int32](data, row, int32(n))
		case abi.TypeBigint:
			n, ok := coef.Int64()
			if !ok {
				return fmt.Errorf("%w: decimal coefficient overflows BIGINT storage", ErrOutOfRange)
			}
			writeAt[int64](data, row, n)
		case abi.TypeHugeint:
			writeAt[abi.Hugeint](data, row, abi.Hugeint{Lower: coef.Lower, Upper: coef.Upper})
		default:
			return fmt.Errorf("%w: unsupported DECIMAL physical type %d", ErrUnsupportedType, plan.decimalInternal)
		}
		return nil

	case TypeDate:
		d, err := v.Date()
		if err != nil {
			return err
		}
		writeAt[abi.Date](data, row, abi.Date{Days: d.Days})
		return nil

	case TypeTime:
		t, err := v.Time()
		if err != nil {
			return err
		}
		writeAt[abi.Time](data, row, abi.Time{Micros: t.Micros})
		return nil

	case TypeTimeNS:
		t, err := v.TimeNS()
		if err != nil {
			return err
		}
		writeAt[abi.TimeNS](data, row, abi.TimeNS{Nanos: t.Nanos})
		return nil

	case TypeTimeTZ:
		tz, err := v.TimeTZ()
		if err != nil {
			return err
		}
		if err := tz.Validate(); err != nil {
			return err
		}
		bits := encodeTimeTZBits(tz.Micros, tz.OffsetSeconds)
		writeAt[abi.TimeTZ](data, row, abi.TimeTZ{Bits: bits})
		return nil

	case TypeTimestamp:
		ts, err := v.Timestamp()
		if err != nil {
			return err
		}
		if ts.Unit != TimestampUnitMicros {
			return fmt.Errorf("%w: expected TIMESTAMP (micros), got %s", ErrTypeMismatch, v.Kind())
		}
		writeAt[int64](data, row, ts.Value)
		return nil

	case TypeTimestampS:
		ts, err := v.Timestamp()
		if err != nil {
			return err
		}
		if ts.Unit != TimestampUnitSeconds {
			return fmt.Errorf("%w: expected TIMESTAMP_S, got %s", ErrTypeMismatch, v.Kind())
		}
		writeAt[int64](data, row, ts.Value)
		return nil

	case TypeTimestampMS:
		ts, err := v.Timestamp()
		if err != nil {
			return err
		}
		if ts.Unit != TimestampUnitMillis {
			return fmt.Errorf("%w: expected TIMESTAMP_MS, got %s", ErrTypeMismatch, v.Kind())
		}
		writeAt[int64](data, row, ts.Value)
		return nil

	case TypeTimestampNS:
		ts, err := v.Timestamp()
		if err != nil {
			return err
		}
		if ts.Unit != TimestampUnitNanos {
			return fmt.Errorf("%w: expected TIMESTAMP_NS, got %s", ErrTypeMismatch, v.Kind())
		}
		writeAt[int64](data, row, ts.Value)
		return nil

	case TypeTimestampTZ:
		ts, err := v.Timestamp()
		if err != nil {
			return err
		}
		if ts.Unit != TimestampUnitTZ {
			return fmt.Errorf("%w: expected TIMESTAMPTZ, got %s", ErrTypeMismatch, v.Kind())
		}
		writeAt[int64](data, row, ts.Value)
		return nil

	case TypeInterval:
		iv, err := v.Interval()
		if err != nil {
			return err
		}
		writeAt[abi.Interval](data, row, abi.Interval{Months: iv.Months, Days: iv.Days, Micros: iv.Micros})
		return nil

	case TypeUUID:
		if v.Kind() != TypeUUID {
			return fmt.Errorf("%w: expected UUID, got %s", ErrTypeMismatch, v.Kind())
		}
		// Mirror decode.go: DuckDB stores UUID vectors as hugeint with MSB flipped.
		writeAt[abi.Hugeint](data, row, abi.Hugeint{
			Lower: v.hi,
			Upper: int64(v.lo ^ (uint64(1) << 63)),
		})
		return nil

	case TypeEnum:
		var label string
		switch v.Kind() {
		case TypeEnum:
			e, err := v.Enum()
			if err != nil {
				return err
			}
			label = e.Label
		case TypeVarchar:
			var err error
			label, err = v.Varchar()
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: expected ENUM or VARCHAR label, got %s", ErrTypeMismatch, v.Kind())
		}
		idx, ok := plan.enumLookup[label]
		if !ok {
			return fmt.Errorf("%w: enum label %q not found in column dictionary", ErrInvalidInput, label)
		}
		switch plan.enumInternal {
		case abi.TypeUTinyint:
			writeAt[uint8](data, row, uint8(idx))
		case abi.TypeUSmallint:
			writeAt[uint16](data, row, uint16(idx))
		case abi.TypeUInteger:
			writeAt[uint32](data, row, idx)
		default:
			return fmt.Errorf("%w: unsupported ENUM physical type %d", ErrUnsupportedType, plan.enumInternal)
		}
		return nil

	default:
		return fmt.Errorf("%w: chunk appender unsupported column type %s", ErrUnsupportedType, plan.lt.Kind)
	}
}

func assignVectorBytes(api *abi.API, vec uintptr, row uint64, b []byte) {
	if len(b) == 0 {
		api.UnsafeVectorAssignStringElementLen(vec, row, &emptyBindByte, 0)
		return
	}
	api.UnsafeVectorAssignStringElementLen(vec, row, &b[0], uint64(len(b)))
	runtime.KeepAlive(b)
}

func encodeTimeTZBits(micros int64, offsetSeconds int32) uint64 {
	const timeTZOffsetBits = 24
	encodedOffset := uint64(uint32(timeTZMaxOffset - offsetSeconds))
	return (uint64(micros) << timeTZOffsetBits) | encodedOffset
}

// encodeBignumWire produces DuckDB's 3-byte sortable header + big-endian
// magnitude wire format (inverse of decodeBignumWire in decode.go).
func encodeBignumWire(bn Bignum) []byte {
	mag := bn.mag
	if len(mag) == 0 {
		// DuckDB encodes 0 as positive with a single 0x00 magnitude byte.
		return []byte{0x80, 0x00, 0x01, 0x00}
	}
	n := len(mag)
	out := make([]byte, 3+n)
	h0 := byte(0x80 | ((n >> 16) & 0x7F))
	h1 := byte((n >> 8) & 0xFF)
	h2 := byte(n & 0xFF)
	if bn.neg {
		out[0] = ^h0
		out[1] = ^h1
		out[2] = ^h2
		for i, b := range mag {
			out[3+i] = ^b
		}
	} else {
		out[0] = h0
		out[1] = h1
		out[2] = h2
		copy(out[3:], mag)
	}
	return out
}

func coerceDecimalForColumn(v Value, targetWidth, targetScale uint8) (Hugeint, error) {
	var (
		srcBig   *big.Int
		srcScale uint8
	)
	switch v.Kind() {
	case TypeDecimal:
		d, err := v.Decimal()
		if err != nil {
			return Hugeint{}, err
		}
		if err := d.Validate(); err != nil {
			return Hugeint{}, err
		}
		if d.Width == targetWidth && d.Scale == targetScale {
			return d.Value, nil
		}
		srcBig = d.Value.BigInt()
		srcScale = d.Scale
	default:
		n, err := v.AsInt64()
		if err != nil {
			return Hugeint{}, fmt.Errorf("%w: expected DECIMAL or integer, got %s", ErrTypeMismatch, v.Kind())
		}
		srcBig = big.NewInt(n)
		srcScale = 0
	}

	if targetScale >= srcScale {
		diff := int(targetScale - srcScale)
		if diff > 0 {
			mul := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(diff)), nil)
			srcBig.Mul(srcBig, mul)
		}
	} else {
		diff := int(srcScale - targetScale)
		div := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(diff)), nil)
		q, rem := new(big.Int).QuoRem(srcBig, div, new(big.Int))
		if rem.Sign() != 0 {
			return Hugeint{}, fmt.Errorf("%w: rescaling DECIMAL scale %d -> %d would lose precision", ErrOutOfRange, srcScale, targetScale)
		}
		srcBig = q
	}

	d, err := NewDecimal(targetWidth, targetScale, srcBig)
	if err != nil {
		return Hugeint{}, err
	}
	return d.Value, nil
}
