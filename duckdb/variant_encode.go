package duckdb

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"unicode/utf8"

	"github.com/bububa/duckdbspike/internal/abi"
)

type variantChildEntry struct {
	hasKey bool
	keyIdx uint32
	valIdx uint32
}

type variantValueEntry struct {
	typeID     uint8
	byteOffset uint32
}

type variantRowEncoder struct {
	limits   DecodeLimits
	maxNodes uint64

	keys     []string
	keyIndex map[string]uint32
	children []variantChildEntry
	values   []variantValueEntry
	data     []byte
}

func newVariantRowEncoder(limits DecodeLimits) *variantRowEncoder {
	maxNodes := limits.MaxCollectionLen
	if maxNodes < math.MaxUint64 {
		maxNodes++
	}
	return &variantRowEncoder{
		limits:   limits,
		maxNodes: maxNodes,
	}
}

func appendUvarint32(dst []byte, v uint32) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

func (e *variantRowEncoder) internKey(k string) (uint32, error) {
	if !utf8.ValidString(k) {
		return 0, fmt.Errorf("%w: VARIANT object key is not valid UTF-8", ErrInvalidInput)
	}
	if uint64(len(k)) > e.limits.MaxValueBytes {
		return 0, fmt.Errorf("%w: VARIANT object key length %d exceeds %d", ErrLimitExceeded, len(k), e.limits.MaxValueBytes)
	}
	if e.keyIndex == nil {
		e.keyIndex = make(map[string]uint32)
	}
	if idx, ok := e.keyIndex[k]; ok {
		return idx, nil
	}
	if uint64(len(e.keys)) >= e.limits.MaxCollectionLen || len(e.keys) >= math.MaxUint32 {
		return 0, fmt.Errorf("%w: VARIANT keys dictionary exceeds limit", ErrLimitExceeded)
	}
	idx := uint32(len(e.keys))
	e.keys = append(e.keys, k)
	e.keyIndex[k] = idx
	return idx, nil
}

func (e *variantRowEncoder) checkDataCapacity(extra int) error {
	next := uint64(len(e.data)) + uint64(extra)
	if next < uint64(len(e.data)) || next > e.limits.MaxValueBytes || next > math.MaxUint32 {
		return fmt.Errorf("%w: VARIANT encoded data size %d exceeds limit", ErrLimitExceeded, next)
	}
	return nil
}

func (e *variantRowEncoder) encodeNode(v Value, depth int) (uint32, error) {
	if depth > e.limits.MaxNestingDepth {
		return 0, fmt.Errorf("%w: VARIANT nesting depth %d exceeds %d", ErrLimitExceeded, depth, e.limits.MaxNestingDepth)
	}
	if v.IsDefault() {
		return 0, fmt.Errorf("%w: DefaultValue cannot be encoded inside VARIANT", ErrInvalidInput)
	}

	// Unwrap nested Variant / Union wrappers before reserving a node slot.
	for !v.IsNull() {
		switch v.Kind() {
		case TypeVariant:
			vr, err := v.Variant()
			if err != nil {
				return 0, err
			}
			v = vr.Value
			continue
		case TypeUnion:
			u, err := v.Union()
			if err != nil {
				return 0, err
			}
			v = u.Value
			continue
		}
		break
	}

	if uint64(len(e.values)) >= e.maxNodes || len(e.values) >= math.MaxUint32 {
		return 0, fmt.Errorf("%w: VARIANT node count exceeds limit %d", ErrLimitExceeded, e.maxNodes)
	}
	myIdx := uint32(len(e.values))
	e.values = append(e.values, variantValueEntry{})

	if v.IsNull() {
		e.values[myIdx] = variantValueEntry{typeID: variantTypeNull, byteOffset: uint32(len(e.data))}
		return myIdx, nil
	}

	byteOff := uint32(len(e.data))

	appendVarBytes := func(tag uint8, raw []byte) (uint32, error) {
		if uint64(len(raw)) > e.limits.MaxValueBytes || len(raw) > math.MaxUint32 {
			return 0, fmt.Errorf("%w: VARIANT payload length %d exceeds limit", ErrLimitExceeded, len(raw))
		}
		if err := e.checkDataCapacity(5 + len(raw)); err != nil {
			return 0, err
		}
		e.data = appendUvarint32(e.data, uint32(len(raw)))
		e.data = append(e.data, raw...)
		e.values[myIdx] = variantValueEntry{typeID: tag, byteOffset: byteOff}
		return myIdx, nil
	}

	switch v.Kind() {
	case TypeBoolean:
		b, err := v.Bool()
		if err != nil {
			return 0, err
		}
		tag := variantTypeBoolFalse
		if b {
			tag = variantTypeBoolTrue
		}
		e.values[myIdx] = variantValueEntry{typeID: tag, byteOffset: byteOff}
		return myIdx, nil

	case TypeTinyint:
		n, err := v.Int8()
		if err != nil {
			return 0, err
		}
		if err := e.checkDataCapacity(1); err != nil {
			return 0, err
		}
		e.data = append(e.data, byte(n))
		e.values[myIdx] = variantValueEntry{typeID: variantTypeInt8, byteOffset: byteOff}
		return myIdx, nil

	case TypeSmallint:
		n, err := v.Int16()
		if err != nil {
			return 0, err
		}
		if err := e.checkDataCapacity(2); err != nil {
			return 0, err
		}
		e.data = binary.LittleEndian.AppendUint16(e.data, uint16(n))
		e.values[myIdx] = variantValueEntry{typeID: variantTypeInt16, byteOffset: byteOff}
		return myIdx, nil

	case TypeInteger:
		n, err := v.Int32()
		if err != nil {
			return 0, err
		}
		if err := e.checkDataCapacity(4); err != nil {
			return 0, err
		}
		e.data = binary.LittleEndian.AppendUint32(e.data, uint32(n))
		e.values[myIdx] = variantValueEntry{typeID: variantTypeInt32, byteOffset: byteOff}
		return myIdx, nil

	case TypeBigint:
		n, err := v.Int64()
		if err != nil {
			return 0, err
		}
		if err := e.checkDataCapacity(8); err != nil {
			return 0, err
		}
		e.data = binary.LittleEndian.AppendUint64(e.data, uint64(n))
		e.values[myIdx] = variantValueEntry{typeID: variantTypeInt64, byteOffset: byteOff}
		return myIdx, nil

	case TypeHugeint:
		h, err := v.Hugeint()
		if err != nil {
			return 0, err
		}
		if err := e.checkDataCapacity(16); err != nil {
			return 0, err
		}
		e.data = binary.LittleEndian.AppendUint64(e.data, h.Lower)
		e.data = binary.LittleEndian.AppendUint64(e.data, uint64(h.Upper))
		e.values[myIdx] = variantValueEntry{typeID: variantTypeInt128, byteOffset: byteOff}
		return myIdx, nil

	case TypeUTinyint:
		u, err := v.Uint8()
		if err != nil {
			return 0, err
		}
		if err := e.checkDataCapacity(1); err != nil {
			return 0, err
		}
		e.data = append(e.data, u)
		e.values[myIdx] = variantValueEntry{typeID: variantTypeUint8, byteOffset: byteOff}
		return myIdx, nil

	case TypeUSmallint:
		u, err := v.Uint16()
		if err != nil {
			return 0, err
		}
		if err := e.checkDataCapacity(2); err != nil {
			return 0, err
		}
		e.data = binary.LittleEndian.AppendUint16(e.data, u)
		e.values[myIdx] = variantValueEntry{typeID: variantTypeUint16, byteOffset: byteOff}
		return myIdx, nil

	case TypeUInteger:
		u, err := v.Uint32()
		if err != nil {
			return 0, err
		}
		if err := e.checkDataCapacity(4); err != nil {
			return 0, err
		}
		e.data = binary.LittleEndian.AppendUint32(e.data, u)
		e.values[myIdx] = variantValueEntry{typeID: variantTypeUint32, byteOffset: byteOff}
		return myIdx, nil

	case TypeUBigint:
		u, err := v.Uint64()
		if err != nil {
			return 0, err
		}
		if err := e.checkDataCapacity(8); err != nil {
			return 0, err
		}
		e.data = binary.LittleEndian.AppendUint64(e.data, u)
		e.values[myIdx] = variantValueEntry{typeID: variantTypeUint64, byteOffset: byteOff}
		return myIdx, nil

	case TypeUhugeint:
		u, err := v.Uhugeint()
		if err != nil {
			return 0, err
		}
		if err := e.checkDataCapacity(16); err != nil {
			return 0, err
		}
		e.data = binary.LittleEndian.AppendUint64(e.data, u.Lower)
		e.data = binary.LittleEndian.AppendUint64(e.data, u.Upper)
		e.values[myIdx] = variantValueEntry{typeID: variantTypeUint128, byteOffset: byteOff}
		return myIdx, nil

	case TypeFloat:
		f, err := v.Float32()
		if err != nil {
			return 0, err
		}
		if err := e.checkDataCapacity(4); err != nil {
			return 0, err
		}
		e.data = binary.LittleEndian.AppendUint32(e.data, math.Float32bits(f))
		e.values[myIdx] = variantValueEntry{typeID: variantTypeFloat, byteOffset: byteOff}
		return myIdx, nil

	case TypeDouble:
		d, err := v.Float64()
		if err != nil {
			return 0, err
		}
		if err := e.checkDataCapacity(8); err != nil {
			return 0, err
		}
		e.data = binary.LittleEndian.AppendUint64(e.data, math.Float64bits(d))
		e.values[myIdx] = variantValueEntry{typeID: variantTypeDouble, byteOffset: byteOff}
		return myIdx, nil

	case TypeDecimal:
		d, err := v.Decimal()
		if err != nil {
			return 0, err
		}
		if err := d.Validate(); err != nil {
			return 0, err
		}
		if err := e.checkDataCapacity(26); err != nil {
			return 0, err
		}
		e.data = appendUvarint32(e.data, uint32(d.Width))
		e.data = appendUvarint32(e.data, uint32(d.Scale))
		switch {
		case d.Width <= 4:
			n, ok := d.Value.Int64()
			if !ok || n < math.MinInt16 || n > math.MaxInt16 {
				return 0, fmt.Errorf("%w: VARIANT DECIMAL(%d,%d) overflows int16", ErrOutOfRange, d.Width, d.Scale)
			}
			e.data = binary.LittleEndian.AppendUint16(e.data, uint16(int16(n)))
		case d.Width <= 9:
			n, ok := d.Value.Int64()
			if !ok || n < math.MinInt32 || n > math.MaxInt32 {
				return 0, fmt.Errorf("%w: VARIANT DECIMAL(%d,%d) overflows int32", ErrOutOfRange, d.Width, d.Scale)
			}
			e.data = binary.LittleEndian.AppendUint32(e.data, uint32(int32(n)))
		case d.Width <= 18:
			n, ok := d.Value.Int64()
			if !ok {
				return 0, fmt.Errorf("%w: VARIANT DECIMAL(%d,%d) overflows int64", ErrOutOfRange, d.Width, d.Scale)
			}
			e.data = binary.LittleEndian.AppendUint64(e.data, uint64(n))
		default:
			e.data = binary.LittleEndian.AppendUint64(e.data, d.Value.Lower)
			e.data = binary.LittleEndian.AppendUint64(e.data, uint64(d.Value.Upper))
		}
		e.values[myIdx] = variantValueEntry{typeID: variantTypeDecimal, byteOffset: byteOff}
		return myIdx, nil

	case TypeVarchar:
		s, err := v.Varchar()
		if err != nil {
			return 0, err
		}
		if !utf8.ValidString(s) {
			return 0, fmt.Errorf("%w: VARIANT VARCHAR is not valid UTF-8", ErrInvalidInput)
		}
		return appendVarBytes(variantTypeVarchar, []byte(s))

	case TypeEnum:
		en, err := v.Enum()
		if err != nil {
			return 0, err
		}
		if !utf8.ValidString(en.Label) {
			return 0, fmt.Errorf("%w: VARIANT ENUM label is not valid UTF-8", ErrInvalidInput)
		}
		return appendVarBytes(variantTypeVarchar, []byte(en.Label))

	case TypeBlob:
		b, err := v.Blob()
		if err != nil {
			return 0, err
		}
		return appendVarBytes(variantTypeBlob, b)

	case TypeUUID:
		if err := e.checkDataCapacity(16); err != nil {
			return 0, err
		}
		// Inverse of decodeNode(variantTypeUUID): lower=v.hi, upper=v.lo^(1<<63).
		e.data = binary.LittleEndian.AppendUint64(e.data, v.hi)
		e.data = binary.LittleEndian.AppendUint64(e.data, v.lo^(uint64(1)<<63))
		e.values[myIdx] = variantValueEntry{typeID: variantTypeUUID, byteOffset: byteOff}
		return myIdx, nil

	case TypeDate:
		d, err := v.Date()
		if err != nil {
			return 0, err
		}
		if err := e.checkDataCapacity(4); err != nil {
			return 0, err
		}
		e.data = binary.LittleEndian.AppendUint32(e.data, uint32(d.Days))
		e.values[myIdx] = variantValueEntry{typeID: variantTypeDate, byteOffset: byteOff}
		return myIdx, nil

	case TypeTime:
		t, err := v.Time()
		if err != nil {
			return 0, err
		}
		if err := e.checkDataCapacity(8); err != nil {
			return 0, err
		}
		e.data = binary.LittleEndian.AppendUint64(e.data, uint64(t.Micros))
		e.values[myIdx] = variantValueEntry{typeID: variantTypeTimeMicros, byteOffset: byteOff}
		return myIdx, nil

	case TypeTimeNS:
		t, err := v.TimeNS()
		if err != nil {
			return 0, err
		}
		if err := e.checkDataCapacity(8); err != nil {
			return 0, err
		}
		e.data = binary.LittleEndian.AppendUint64(e.data, uint64(t.Nanos))
		e.values[myIdx] = variantValueEntry{typeID: variantTypeTimeNanos, byteOffset: byteOff}
		return myIdx, nil

	case TypeTimeTZ:
		tz, err := v.TimeTZ()
		if err != nil {
			return 0, err
		}
		if err := tz.Validate(); err != nil {
			return 0, err
		}
		if err := e.checkDataCapacity(8); err != nil {
			return 0, err
		}
		e.data = binary.LittleEndian.AppendUint64(e.data, encodeTimeTZBits(tz.Micros, tz.OffsetSeconds))
		e.values[myIdx] = variantValueEntry{typeID: variantTypeTimeMicrosTZ, byteOffset: byteOff}
		return myIdx, nil

	case TypeTimestamp, TypeTimestampS, TypeTimestampMS, TypeTimestampNS, TypeTimestampTZ:
		ts, err := v.Timestamp()
		if err != nil {
			return 0, err
		}
		var tag uint8
		switch ts.Unit {
		case TimestampUnitSeconds:
			tag = variantTypeTimestampSec
		case TimestampUnitMillis:
			tag = variantTypeTimestampMillis
		case TimestampUnitMicros:
			tag = variantTypeTimestampMicros
		case TimestampUnitNanos:
			tag = variantTypeTimestampNanos
		case TimestampUnitTZ:
			tag = variantTypeTimestampTZ
		default:
			return 0, fmt.Errorf("%w: invalid timestamp unit %d", ErrInvalidInput, ts.Unit)
		}
		if err := e.checkDataCapacity(8); err != nil {
			return 0, err
		}
		e.data = binary.LittleEndian.AppendUint64(e.data, uint64(ts.Value))
		e.values[myIdx] = variantValueEntry{typeID: tag, byteOffset: byteOff}
		return myIdx, nil

	case TypeInterval:
		iv, err := v.Interval()
		if err != nil {
			return 0, err
		}
		if err := e.checkDataCapacity(16); err != nil {
			return 0, err
		}
		e.data = binary.LittleEndian.AppendUint32(e.data, uint32(iv.Months))
		e.data = binary.LittleEndian.AppendUint32(e.data, uint32(iv.Days))
		e.data = binary.LittleEndian.AppendUint64(e.data, uint64(iv.Micros))
		e.values[myIdx] = variantValueEntry{typeID: variantTypeInterval, byteOffset: byteOff}
		return myIdx, nil

	case TypeBignum:
		bn, err := v.Bignum()
		if err != nil {
			return 0, err
		}
		return appendVarBytes(variantTypeBignum, encodeBignumWire(bn))

	case TypeBit:
		b, err := v.Bit()
		if err != nil {
			return 0, err
		}
		if err := b.Validate(); err != nil {
			return 0, err
		}
		return appendVarBytes(variantTypeBitstring, b.Data)

	case TypeGeometry:
		g, err := v.Geometry()
		if err != nil {
			return 0, err
		}
		if len(g.WKB) == 0 {
			return 0, fmt.Errorf("%w: VARIANT GEOMETRY WKB payload must not be empty", ErrInvalidInput)
		}
		return appendVarBytes(variantTypeGeometry, g.WKB)

	case TypeList:
		lv, err := v.List()
		if err != nil {
			return 0, err
		}
		return e.encodeArrayNode(myIdx, byteOff, lv.Elements, depth)

	case TypeArray:
		av, err := v.Array()
		if err != nil {
			return 0, err
		}
		return e.encodeArrayNode(myIdx, byteOff, av.Elements, depth)

	case TypeStruct:
		st, err := v.Struct()
		if err != nil {
			return 0, err
		}
		return e.encodeObjectNode(myIdx, byteOff, st.Fields, depth)

	case TypeMap:
		mv, err := v.Map()
		if err != nil {
			return 0, err
		}
		fields := make([]StructFieldValue, len(mv.Entries))
		for i, entry := range mv.Entries {
			if entry.Key.IsNull() || entry.Key.Kind() != TypeVarchar {
				return 0, fmt.Errorf("%w: VARIANT MAP key[%d] must be non-NULL VARCHAR, got %s", ErrTypeMismatch, i, entry.Key.Kind())
			}
			k, err := entry.Key.Varchar()
			if err != nil {
				return 0, err
			}
			fields[i] = StructFieldValue{Name: k, Value: entry.Value}
		}
		return e.encodeObjectNode(myIdx, byteOff, fields, depth)

	default:
		return 0, fmt.Errorf("%w: unsupported value type %s inside VARIANT", ErrUnsupportedType, v.Kind())
	}
}

func (e *variantRowEncoder) encodeArrayNode(myIdx uint32, byteOff uint32, elems []Value, depth int) (uint32, error) {
	n := uint64(len(elems))
	if n > e.limits.MaxCollectionLen || n > math.MaxUint32 {
		return 0, fmt.Errorf("%w: VARIANT array length %d exceeds limit", ErrLimitExceeded, n)
	}
	if n == 0 {
		if err := e.checkDataCapacity(1); err != nil {
			return 0, err
		}
		e.data = append(e.data, 0)
		e.values[myIdx] = variantValueEntry{typeID: variantTypeArray, byteOffset: byteOff}
		return myIdx, nil
	}
	if uint64(len(e.children))+n > e.limits.MaxCollectionLen || uint64(len(e.children))+n > math.MaxUint32 {
		return 0, fmt.Errorf("%w: VARIANT children count exceeds limit", ErrLimitExceeded)
	}
	childrenStart := uint32(len(e.children))
	e.children = append(e.children, make([]variantChildEntry, int(n))...)

	if err := e.checkDataCapacity(10); err != nil {
		return 0, err
	}
	e.data = appendUvarint32(e.data, uint32(n))
	e.data = appendUvarint32(e.data, childrenStart)
	e.values[myIdx] = variantValueEntry{typeID: variantTypeArray, byteOffset: byteOff}

	for i, elem := range elems {
		childValIdx, err := e.encodeNode(elem, depth+1)
		if err != nil {
			return 0, fmt.Errorf("VARIANT array[%d]: %w", i, err)
		}
		e.children[int(childrenStart)+i] = variantChildEntry{
			hasKey: false,
			valIdx: childValIdx,
		}
	}
	return myIdx, nil
}

func (e *variantRowEncoder) encodeObjectNode(myIdx uint32, byteOff uint32, fields []StructFieldValue, depth int) (uint32, error) {
	n := uint64(len(fields))
	if n > e.limits.MaxCollectionLen || n > math.MaxUint32 {
		return 0, fmt.Errorf("%w: VARIANT object field count %d exceeds limit", ErrLimitExceeded, n)
	}
	if n == 0 {
		if err := e.checkDataCapacity(1); err != nil {
			return 0, err
		}
		e.data = append(e.data, 0)
		e.values[myIdx] = variantValueEntry{typeID: variantTypeObject, byteOffset: byteOff}
		return myIdx, nil
	}

	// DuckDB VARIANT objects store children sorted lexicographically by key name
	// so engine binary-search lookups (variant_extract / v.key) succeed.
	sorted := make([]StructFieldValue, len(fields))
	copy(sorted, fields)
	slices.SortFunc(sorted, func(a, b StructFieldValue) int {
		return cmp.Compare(a.Name, b.Name)
	})
	for i := 1; i < len(sorted); i++ {
		if sorted[i].Name == sorted[i-1].Name {
			return 0, fmt.Errorf("%w: duplicate VARIANT object key %q", ErrInvalidInput, sorted[i].Name)
		}
	}

	if uint64(len(e.children))+n > e.limits.MaxCollectionLen || uint64(len(e.children))+n > math.MaxUint32 {
		return 0, fmt.Errorf("%w: VARIANT children count exceeds limit", ErrLimitExceeded)
	}
	childrenStart := uint32(len(e.children))
	e.children = append(e.children, make([]variantChildEntry, int(n))...)

	if err := e.checkDataCapacity(10); err != nil {
		return 0, err
	}
	e.data = appendUvarint32(e.data, uint32(n))
	e.data = appendUvarint32(e.data, childrenStart)
	e.values[myIdx] = variantValueEntry{typeID: variantTypeObject, byteOffset: byteOff}

	for i, f := range sorted {
		keyIdx, err := e.internKey(f.Name)
		if err != nil {
			return 0, fmt.Errorf("VARIANT object key %q: %w", f.Name, err)
		}
		childValIdx, err := e.encodeNode(f.Value, depth+1)
		if err != nil {
			return 0, fmt.Errorf("VARIANT object field %q: %w", f.Name, err)
		}
		e.children[int(childrenStart)+i] = variantChildEntry{
			hasKey: true,
			keyIdx: keyIdx,
			valIdx: childValIdx,
		}
	}
	return myIdx, nil
}

func writeVariantVectorValue(api *abi.API, vec uintptr, row uint64, v Value, depth int, limits DecodeLimits, undo *[]listSizeUndo) error {
	keysVec := api.StructVectorGetChild(vec, 0)
	childrenVec := api.StructVectorGetChild(vec, 1)
	valuesVec := api.StructVectorGetChild(vec, 2)
	dataVec := api.StructVectorGetChild(vec, 3)
	if keysVec == 0 || childrenVec == 0 || valuesVec == 0 || dataVec == 0 {
		return fmt.Errorf("%w: VARIANT vector missing physical child vector", ErrInvalidInput)
	}

	enc := newVariantRowEncoder(limits)
	if _, err := enc.encodeNode(v, depth); err != nil {
		return err
	}

	keysListData := api.VectorGetData(keysVec)
	childrenListData := api.VectorGetData(childrenVec)
	valuesListData := api.VectorGetData(valuesVec)
	if keysListData == 0 || childrenListData == 0 || valuesListData == 0 {
		return fmt.Errorf("%w: VARIANT child list vector has null data pointer", ErrInvalidInput)
	}

	// 1. keys :: LIST<VARCHAR>
	setVectorValid(api, keysVec, row)
	curKeys := api.ListVectorGetSize(keysVec)
	nKeys := uint64(len(enc.keys))
	newKeys := curKeys + nKeys
	if newKeys < curKeys {
		return fmt.Errorf("%w: VARIANT keys list size overflow", ErrLimitExceeded)
	}
	*undo = append(*undo, listSizeUndo{vec: keysVec, prevSize: curKeys})
	if nKeys > 0 {
		if api.ListVectorReserve(keysVec, newKeys) != abi.StateSuccess {
			return nativeError("list_vector_reserve", abi.ErrorOutOfMemory, "")
		}
		if api.ListVectorSetSize(keysVec, newKeys) != abi.StateSuccess {
			return nativeError("list_vector_set_size", abi.ErrorInvalid, "")
		}
	}
	writeAt[abi.ListEntry](keysListData, row, abi.ListEntry{Offset: curKeys, Length: nKeys})
	if nKeys > 0 {
		keysChildVec := api.ListVectorGetChild(keysVec)
		for i, k := range enc.keys {
			idx := curKeys + uint64(i)
			setVectorValid(api, keysChildVec, idx)
			assignVectorBytes(api, keysChildVec, idx, []byte(k))
		}
	}

	// 2. children :: LIST<STRUCT(keys_index UINTEGER, values_index UINTEGER)>
	setVectorValid(api, childrenVec, row)
	curChildren := api.ListVectorGetSize(childrenVec)
	nChildren := uint64(len(enc.children))
	newChildren := curChildren + nChildren
	if newChildren < curChildren {
		return fmt.Errorf("%w: VARIANT children list size overflow", ErrLimitExceeded)
	}
	*undo = append(*undo, listSizeUndo{vec: childrenVec, prevSize: curChildren})
	if nChildren > 0 {
		if api.ListVectorReserve(childrenVec, newChildren) != abi.StateSuccess {
			return nativeError("list_vector_reserve", abi.ErrorOutOfMemory, "")
		}
		if api.ListVectorSetSize(childrenVec, newChildren) != abi.StateSuccess {
			return nativeError("list_vector_set_size", abi.ErrorInvalid, "")
		}
	}
	writeAt[abi.ListEntry](childrenListData, row, abi.ListEntry{Offset: curChildren, Length: nChildren})
	if nChildren > 0 {
		childrenStructVec := api.ListVectorGetChild(childrenVec)
		childKeysVec := api.StructVectorGetChild(childrenStructVec, 0)
		childValsVec := api.StructVectorGetChild(childrenStructVec, 1)
		childKeysData := api.VectorGetData(childKeysVec)
		childValsData := api.VectorGetData(childValsVec)
		if childKeysData == 0 || childValsData == 0 {
			return fmt.Errorf("%w: VARIANT children struct vector has null data pointer", ErrInvalidInput)
		}
		for i, ch := range enc.children {
			idx := curChildren + uint64(i)
			setVectorValid(api, childrenStructVec, idx)
			if ch.hasKey {
				setVectorValid(api, childKeysVec, idx)
				writeAt[uint32](childKeysData, idx, ch.keyIdx)
			} else {
				setVectorNull(api, childKeysVec, idx)
			}
			setVectorValid(api, childValsVec, idx)
			writeAt[uint32](childValsData, idx, ch.valIdx)
		}
	}

	// 3. values :: LIST<STRUCT(type_id UTINYINT, byte_offset UINTEGER)>
	setVectorValid(api, valuesVec, row)
	curValues := api.ListVectorGetSize(valuesVec)
	nValues := uint64(len(enc.values))
	newValues := curValues + nValues
	if newValues < curValues {
		return fmt.Errorf("%w: VARIANT values list size overflow", ErrLimitExceeded)
	}
	*undo = append(*undo, listSizeUndo{vec: valuesVec, prevSize: curValues})
	if nValues > 0 {
		if api.ListVectorReserve(valuesVec, newValues) != abi.StateSuccess {
			return nativeError("list_vector_reserve", abi.ErrorOutOfMemory, "")
		}
		if api.ListVectorSetSize(valuesVec, newValues) != abi.StateSuccess {
			return nativeError("list_vector_set_size", abi.ErrorInvalid, "")
		}
	}
	writeAt[abi.ListEntry](valuesListData, row, abi.ListEntry{Offset: curValues, Length: nValues})
	if nValues > 0 {
		valuesStructVec := api.ListVectorGetChild(valuesVec)
		valTypeVec := api.StructVectorGetChild(valuesStructVec, 0)
		valOffsetVec := api.StructVectorGetChild(valuesStructVec, 1)
		valTypeData := api.VectorGetData(valTypeVec)
		valOffsetData := api.VectorGetData(valOffsetVec)
		if valTypeData == 0 || valOffsetData == 0 {
			return fmt.Errorf("%w: VARIANT values struct vector has null data pointer", ErrInvalidInput)
		}
		for i, ve := range enc.values {
			idx := curValues + uint64(i)
			setVectorValid(api, valuesStructVec, idx)
			setVectorValid(api, valTypeVec, idx)
			writeAt[uint8](valTypeData, idx, ve.typeID)
			setVectorValid(api, valOffsetVec, idx)
			writeAt[uint32](valOffsetData, idx, ve.byteOffset)
		}
	}

	// 4. data :: BLOB
	setVectorValid(api, dataVec, row)
	assignVectorBytes(api, dataVec, row, enc.data)
	return nil
}
