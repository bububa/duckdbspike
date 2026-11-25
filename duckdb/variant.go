package duckdb

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

// DuckDB v1.5.6 VariantLogicalType tags (from duckdb.hpp).
const (
	variantTypeNull            uint8 = 0
	variantTypeBoolTrue        uint8 = 1
	variantTypeBoolFalse       uint8 = 2
	variantTypeInt8            uint8 = 3
	variantTypeInt16           uint8 = 4
	variantTypeInt32           uint8 = 5
	variantTypeInt64           uint8 = 6
	variantTypeInt128          uint8 = 7
	variantTypeUint8           uint8 = 8
	variantTypeUint16          uint8 = 9
	variantTypeUint32          uint8 = 10
	variantTypeUint64          uint8 = 11
	variantTypeUint128         uint8 = 12
	variantTypeFloat           uint8 = 13
	variantTypeDouble          uint8 = 14
	variantTypeDecimal         uint8 = 15
	variantTypeVarchar         uint8 = 16
	variantTypeBlob            uint8 = 17
	variantTypeUUID            uint8 = 18
	variantTypeDate            uint8 = 19
	variantTypeTimeMicros      uint8 = 20
	variantTypeTimeNanos       uint8 = 21
	variantTypeTimestampSec    uint8 = 22
	variantTypeTimestampMillis uint8 = 23
	variantTypeTimestampMicros uint8 = 24
	variantTypeTimestampNanos  uint8 = 25
	variantTypeTimeMicrosTZ    uint8 = 26
	variantTypeTimestampTZ     uint8 = 27
	variantTypeInterval        uint8 = 28
	variantTypeObject          uint8 = 29
	variantTypeArray           uint8 = 30
	variantTypeBignum          uint8 = 31
	variantTypeBitstring       uint8 = 32
	variantTypeGeometry        uint8 = 33
)

// validateVariantLogicalType verifies that a DuckDB VARIANT logical type has
// the pinned v1.5.6 4-child physical layout:
//
//	STRUCT(
//	  keys     VARCHAR[],
//	  children STRUCT(keys_index UINTEGER, values_index UINTEGER)[],
//	  values   STRUCT(type_id UTINYINT, byte_offset UINTEGER)[],
//	  data     BLOB
//	)
//
// Any layout drift is rejected cleanly with ErrUnsupportedType.
func validateVariantLogicalType(api *abi.API, lt uintptr) error {
	if api.StructTypeChildCount == nil || api.StructTypeChildName == nil || api.StructTypeChildType == nil {
		return fmt.Errorf("%w: VARIANT structural inspection symbols unavailable", ErrUnsupportedType)
	}
	if got := api.StructTypeChildCount(lt); got != 4 {
		return fmt.Errorf("%w: VARIANT expects 4 physical children (keys, children, values, data), got %d", ErrUnsupportedType, got)
	}
	wantNames := [4]string{"keys", "children", "values", "data"}
	children := make([]uintptr, 4)
	defer func() {
		for i := range children {
			if children[i] != 0 {
				api.DestroyLogicalType(&children[i])
			}
		}
	}()
	for i := range uint64(4) {
		namePtr := api.StructTypeChildName(lt, i)
		name := abi.GoString(namePtr)
		if namePtr != 0 {
			api.Free(namePtr)
		}
		children[i] = api.StructTypeChildType(lt, i)
		if name != wantNames[i] {
			return fmt.Errorf("%w: VARIANT child %d named %q, want %q", ErrUnsupportedType, i, name, wantNames[i])
		}
		if children[i] == 0 {
			return fmt.Errorf("%w: VARIANT child %d (%q) has null logical type", ErrUnsupportedType, i, name)
		}
	}

	// Child 0: keys :: LIST<VARCHAR>
	if err := checkVariantListElem(api, children[0], "keys", abi.TypeVarchar); err != nil {
		return err
	}
	// Child 1: children :: LIST<STRUCT(keys_index UINTEGER, values_index UINTEGER)>
	if err := checkVariantListStruct(api, children[1], "children", [2]string{"keys_index", "values_index"}, [2]int32{abi.TypeUInteger, abi.TypeUInteger}); err != nil {
		return err
	}
	// Child 2: values :: LIST<STRUCT(type_id UTINYINT, byte_offset UINTEGER)>
	if err := checkVariantListStruct(api, children[2], "values", [2]string{"type_id", "byte_offset"}, [2]int32{abi.TypeUTinyint, abi.TypeUInteger}); err != nil {
		return err
	}
	// Child 3: data :: BLOB
	if got := api.GetTypeID(children[3]); got != abi.TypeBlob {
		return fmt.Errorf("%w: VARIANT child \"data\" has type %d, want BLOB (%d)", ErrUnsupportedType, got, abi.TypeBlob)
	}
	return nil
}

func checkVariantListElem(api *abi.API, lt uintptr, name string, wantElemID int32) error {
	if got := api.GetTypeID(lt); got != abi.TypeList {
		return fmt.Errorf("%w: VARIANT child %q has type %d, want LIST (%d)", ErrUnsupportedType, name, got, abi.TypeList)
	}
	elem := api.ListTypeChildType(lt)
	if elem == 0 {
		return fmt.Errorf("%w: VARIANT child %q has null element type", ErrUnsupportedType, name)
	}
	defer api.DestroyLogicalType(&elem)
	if got := api.GetTypeID(elem); got != wantElemID {
		return fmt.Errorf("%w: VARIANT child %q element has type %d, want %d", ErrUnsupportedType, name, got, wantElemID)
	}
	return nil
}

func checkVariantListStruct(api *abi.API, lt uintptr, name string, wantFields [2]string, wantTypes [2]int32) error {
	if got := api.GetTypeID(lt); got != abi.TypeList {
		return fmt.Errorf("%w: VARIANT child %q has type %d, want LIST (%d)", ErrUnsupportedType, name, got, abi.TypeList)
	}
	elem := api.ListTypeChildType(lt)
	if elem == 0 {
		return fmt.Errorf("%w: VARIANT child %q has null struct element", ErrUnsupportedType, name)
	}
	defer api.DestroyLogicalType(&elem)
	if got := api.GetTypeID(elem); got != abi.TypeStruct {
		return fmt.Errorf("%w: VARIANT child %q element has type %d, want STRUCT (%d)", ErrUnsupportedType, name, got, abi.TypeStruct)
	}
	if got := api.StructTypeChildCount(elem); got != 2 {
		return fmt.Errorf("%w: VARIANT child %q element struct has %d fields, want 2", ErrUnsupportedType, name, got)
	}
	for i := range uint64(2) {
		nPtr := api.StructTypeChildName(elem, i)
		fname := abi.GoString(nPtr)
		if nPtr != 0 {
			api.Free(nPtr)
		}
		ft := api.StructTypeChildType(elem, i)
		var ftypeID int32
		if ft != 0 {
			ftypeID = api.GetTypeID(ft)
			api.DestroyLogicalType(&ft)
		}
		if fname != wantFields[i] || ftypeID != wantTypes[i] {
			return fmt.Errorf("%w: VARIANT child %q.%d is (%q, type %d), want (%q, type %d)", ErrUnsupportedType, name, i, fname, ftypeID, wantFields[i], wantTypes[i])
		}
	}
	return nil
}

type variantRowDecoder struct {
	api    *abi.API
	limits DecodeLimits

	keysEntry       abi.ListEntry
	keysChildData   uintptr
	keysChildValid  uintptr
	keysChildSize   uint64
	childrenEntry   abi.ListEntry
	childKeysData   uintptr
	childKeysValid  uintptr
	childValsData   uintptr
	childValsValid  uintptr
	valuesEntry     abi.ListEntry
	valTypeData     uintptr
	valTypeValid    uintptr
	valOffsetData   uintptr
	valOffsetValid  uintptr
	data            []byte
	visiting        map[uint32]struct{}
	decodedNodes    uint64
	maxDecodedNodes uint64
}

func decodeVariantValue(api *abi.API, vec uintptr, row uint64, limits DecodeLimits) (Value, error) {
	keysVec := api.StructVectorGetChild(vec, 0)
	childrenVec := api.StructVectorGetChild(vec, 1)
	valuesVec := api.StructVectorGetChild(vec, 2)
	dataVec := api.StructVectorGetChild(vec, 3)
	if keysVec == 0 || childrenVec == 0 || valuesVec == 0 || dataVec == 0 {
		return Value{}, fmt.Errorf("%w: VARIANT vector missing physical child vector", ErrInvalidInput)
	}
	if !rowValid(api.VectorGetValidity(keysVec), row) ||
		!rowValid(api.VectorGetValidity(childrenVec), row) ||
		!rowValid(api.VectorGetValidity(valuesVec), row) ||
		!rowValid(api.VectorGetValidity(dataVec), row) {
		return Value{}, fmt.Errorf("%w: non-NULL VARIANT row has NULL physical child entry", ErrInvalidInput)
	}

	keysListData := api.VectorGetData(keysVec)
	childrenListData := api.VectorGetData(childrenVec)
	valuesListData := api.VectorGetData(valuesVec)
	dataBlobData := api.VectorGetData(dataVec)
	if keysListData == 0 || childrenListData == 0 || valuesListData == 0 || dataBlobData == 0 {
		return Value{}, fmt.Errorf("%w: VARIANT child vector has null data pointer", ErrInvalidInput)
	}

	keysEntry := readAt[abi.ListEntry](keysListData, row)
	childrenEntry := readAt[abi.ListEntry](childrenListData, row)
	valuesEntry := readAt[abi.ListEntry](valuesListData, row)

	keysChildVec := api.ListVectorGetChild(keysVec)
	keysChildSize := api.ListVectorGetSize(keysVec)
	if keysEntry.Offset+keysEntry.Length < keysEntry.Offset || keysEntry.Offset+keysEntry.Length > keysChildSize {
		return Value{}, fmt.Errorf("%w: VARIANT keys list range [%d,+%d) exceeds child size %d", ErrInvalidInput, keysEntry.Offset, keysEntry.Length, keysChildSize)
	}

	childrenStructVec := api.ListVectorGetChild(childrenVec)
	childrenChildSize := api.ListVectorGetSize(childrenVec)
	if childrenEntry.Offset+childrenEntry.Length < childrenEntry.Offset || childrenEntry.Offset+childrenEntry.Length > childrenChildSize {
		return Value{}, fmt.Errorf("%w: VARIANT children list range [%d,+%d) exceeds child size %d", ErrInvalidInput, childrenEntry.Offset, childrenEntry.Length, childrenChildSize)
	}

	valuesStructVec := api.ListVectorGetChild(valuesVec)
	valuesChildSize := api.ListVectorGetSize(valuesVec)
	if valuesEntry.Offset+valuesEntry.Length < valuesEntry.Offset || valuesEntry.Offset+valuesEntry.Length > valuesChildSize {
		return Value{}, fmt.Errorf("%w: VARIANT values list range [%d,+%d) exceeds child size %d", ErrInvalidInput, valuesEntry.Offset, valuesEntry.Length, valuesChildSize)
	}
	if valuesEntry.Length == 0 {
		return Value{}, fmt.Errorf("%w: non-NULL VARIANT row has empty values list", ErrInvalidInput)
	}

	rawData, err := readNativeStringBytes(dataBlobData, row, limits.MaxValueBytes)
	if err != nil {
		return Value{}, fmt.Errorf("VARIANT data: %w", err)
	}

	childKeysVec := api.StructVectorGetChild(childrenStructVec, 0)
	childValsVec := api.StructVectorGetChild(childrenStructVec, 1)
	valTypeVec := api.StructVectorGetChild(valuesStructVec, 0)
	valOffsetVec := api.StructVectorGetChild(valuesStructVec, 1)

	maxNodes := limits.MaxCollectionLen
	if maxNodes < math.MaxUint64 {
		maxNodes++ // allow root + MaxCollectionLen nested values
	}
	if valuesEntry.Length < maxNodes {
		maxNodes = valuesEntry.Length
	}

	dec := &variantRowDecoder{
		api:             api,
		limits:          limits,
		keysEntry:       keysEntry,
		keysChildData:   api.VectorGetData(keysChildVec),
		keysChildValid:  api.VectorGetValidity(keysChildVec),
		keysChildSize:   keysChildSize,
		childrenEntry:   childrenEntry,
		childKeysData:   api.VectorGetData(childKeysVec),
		childKeysValid:  api.VectorGetValidity(childKeysVec),
		childValsData:   api.VectorGetData(childValsVec),
		childValsValid:  api.VectorGetValidity(childValsVec),
		valuesEntry:     valuesEntry,
		valTypeData:     api.VectorGetData(valTypeVec),
		valTypeValid:    api.VectorGetValidity(valTypeVec),
		valOffsetData:   api.VectorGetData(valOffsetVec),
		valOffsetValid:  api.VectorGetValidity(valOffsetVec),
		data:            rawData,
		visiting:        make(map[uint32]struct{}),
		maxDecodedNodes: maxNodes,
	}

	inner, err := dec.decodeNode(0, 0)
	if err != nil {
		return Value{}, err
	}
	return VariantValue(inner), nil
}

func readNativeStringBytes(data uintptr, row uint64, maxBytes uint64) ([]byte, error) {
	const stride = uint64(unsafe.Sizeof(abi.StringT{}))
	if row > math.MaxUint64/stride {
		return nil, fmt.Errorf("%w: string row offset overflow", ErrLimitExceeded)
	}
	addr := data + uintptr(row*stride)
	s := *(*abi.StringT)(abi.Pointer(addr))
	length := uint64(s.Length)
	if length > maxBytes {
		return nil, fmt.Errorf("%w: value length %d exceeds %d", ErrLimitExceeded, length, maxBytes)
	}
	if s.Length <= abi.InlineStringLength {
		return copyNativeBytes(addr+unsafe.Offsetof(abi.StringT{}.Prefix), length)
	}
	return copyNativeBytes(s.Ptr, length)
}

func decodeUvarint32(buf []byte) (uint32, int, error) {
	if len(buf) == 0 {
		return 0, 0, fmt.Errorf("%w: truncated VARIANT varint", ErrInvalidInput)
	}
	var val uint32
	for i := range 5 {
		if i >= len(buf) {
			return 0, 0, fmt.Errorf("%w: truncated VARIANT varint", ErrInvalidInput)
		}
		b := buf[i]
		if i == 4 && b >= 0x10 {
			return 0, 0, fmt.Errorf("%w: VARIANT varint overflows uint32", ErrInvalidInput)
		}
		val |= uint32(b&0x7f) << (7 * i)
		if b&0x80 == 0 {
			return val, i + 1, nil
		}
	}
	return 0, 0, fmt.Errorf("%w: VARIANT varint exceeds 5 bytes", ErrInvalidInput)
}

func (d *variantRowDecoder) decodeNode(valIdx uint32, depth int) (Value, error) {
	if depth > d.limits.MaxNestingDepth {
		return Value{}, fmt.Errorf("%w: VARIANT nesting depth %d exceeds %d", ErrLimitExceeded, depth, d.limits.MaxNestingDepth)
	}
	if uint64(valIdx) >= d.valuesEntry.Length {
		return Value{}, fmt.Errorf("%w: VARIANT values_index %d out of range [0,%d)", ErrInvalidInput, valIdx, d.valuesEntry.Length)
	}
	if _, cycle := d.visiting[valIdx]; cycle {
		return Value{}, fmt.Errorf("%w: cycle detected in VARIANT at values_index %d", ErrInvalidInput, valIdx)
	}
	if d.decodedNodes >= d.maxDecodedNodes {
		return Value{}, fmt.Errorf("%w: VARIANT decoded node count exceeds limit %d", ErrLimitExceeded, d.maxDecodedNodes)
	}
	d.decodedNodes++

	absValRow := d.valuesEntry.Offset + uint64(valIdx)
	if !rowValid(d.valTypeValid, absValRow) || !rowValid(d.valOffsetValid, absValRow) {
		return Value{}, fmt.Errorf("%w: VARIANT values entry %d has NULL metadata", ErrInvalidInput, valIdx)
	}
	typeID := readAt[uint8](d.valTypeData, absValRow)
	byteOff := readAt[uint32](d.valOffsetData, absValRow)
	if uint64(byteOff) > uint64(len(d.data)) {
		return Value{}, fmt.Errorf("%w: VARIANT byte_offset %d exceeds data length %d", ErrInvalidInput, byteOff, len(d.data))
	}
	payload := d.data[byteOff:]

	readBytes := func(n int) ([]byte, error) {
		if len(payload) < n {
			return nil, fmt.Errorf("%w: VARIANT type %d needs %d bytes, only %d available", ErrInvalidInput, typeID, n, len(payload))
		}
		return payload[:n], nil
	}
	readVarBytes := func() ([]byte, error) {
		n32, hdrLen, err := decodeUvarint32(payload)
		if err != nil {
			return nil, err
		}
		if uint64(n32) > d.limits.MaxValueBytes {
			return nil, fmt.Errorf("%w: VARIANT payload length %d exceeds %d", ErrLimitExceeded, n32, d.limits.MaxValueBytes)
		}
		rest := payload[hdrLen:]
		if uint64(len(rest)) < uint64(n32) {
			return nil, fmt.Errorf("%w: VARIANT payload truncated: need %d bytes, have %d", ErrInvalidInput, n32, len(rest))
		}
		return rest[:n32], nil
	}

	switch typeID {
	case variantTypeNull:
		return TypedNullValue(ScalarType(TypeSQLNull)), nil
	case variantTypeBoolTrue:
		return BoolValue(true), nil
	case variantTypeBoolFalse:
		return BoolValue(false), nil
	case variantTypeInt8:
		b, err := readBytes(1)
		if err != nil {
			return Value{}, err
		}
		return Int8Value(int8(b[0])), nil
	case variantTypeInt16:
		b, err := readBytes(2)
		if err != nil {
			return Value{}, err
		}
		return Int16Value(int16(binary.LittleEndian.Uint16(b))), nil
	case variantTypeInt32:
		b, err := readBytes(4)
		if err != nil {
			return Value{}, err
		}
		return Int32Value(int32(binary.LittleEndian.Uint32(b))), nil
	case variantTypeInt64:
		b, err := readBytes(8)
		if err != nil {
			return Value{}, err
		}
		return Int64Value(int64(binary.LittleEndian.Uint64(b))), nil
	case variantTypeInt128:
		b, err := readBytes(16)
		if err != nil {
			return Value{}, err
		}
		return HugeintValue(Hugeint{
			Lower: binary.LittleEndian.Uint64(b[0:8]),
			Upper: int64(binary.LittleEndian.Uint64(b[8:16])),
		}), nil
	case variantTypeUint8:
		b, err := readBytes(1)
		if err != nil {
			return Value{}, err
		}
		return Uint8Value(b[0]), nil
	case variantTypeUint16:
		b, err := readBytes(2)
		if err != nil {
			return Value{}, err
		}
		return Uint16Value(binary.LittleEndian.Uint16(b)), nil
	case variantTypeUint32:
		b, err := readBytes(4)
		if err != nil {
			return Value{}, err
		}
		return Uint32Value(binary.LittleEndian.Uint32(b)), nil
	case variantTypeUint64:
		b, err := readBytes(8)
		if err != nil {
			return Value{}, err
		}
		return Uint64Value(binary.LittleEndian.Uint64(b)), nil
	case variantTypeUint128:
		b, err := readBytes(16)
		if err != nil {
			return Value{}, err
		}
		return UhugeintValue(Uhugeint{
			Lower: binary.LittleEndian.Uint64(b[0:8]),
			Upper: binary.LittleEndian.Uint64(b[8:16]),
		}), nil
	case variantTypeFloat:
		b, err := readBytes(4)
		if err != nil {
			return Value{}, err
		}
		return Float32Value(math.Float32frombits(binary.LittleEndian.Uint32(b))), nil
	case variantTypeDouble:
		b, err := readBytes(8)
		if err != nil {
			return Value{}, err
		}
		return Float64Value(math.Float64frombits(binary.LittleEndian.Uint64(b))), nil
	case variantTypeDecimal:
		width32, n1, err := decodeUvarint32(payload)
		if err != nil {
			return Value{}, err
		}
		scale32, n2, err := decodeUvarint32(payload[n1:])
		if err != nil {
			return Value{}, err
		}
		if width32 < 1 || width32 > 38 || scale32 > width32 {
			return Value{}, fmt.Errorf("%w: invalid VARIANT DECIMAL(%d,%d)", ErrInvalidInput, width32, scale32)
		}
		rest := payload[n1+n2:]
		var coef Hugeint
		switch {
		case width32 <= 4:
			if len(rest) < 2 {
				return Value{}, fmt.Errorf("%w: truncated VARIANT DECIMAL16", ErrInvalidInput)
			}
			coef = HugeintFromInt64(int64(int16(binary.LittleEndian.Uint16(rest[:2]))))
		case width32 <= 9:
			if len(rest) < 4 {
				return Value{}, fmt.Errorf("%w: truncated VARIANT DECIMAL32", ErrInvalidInput)
			}
			coef = HugeintFromInt64(int64(int32(binary.LittleEndian.Uint32(rest[:4]))))
		case width32 <= 18:
			if len(rest) < 8 {
				return Value{}, fmt.Errorf("%w: truncated VARIANT DECIMAL64", ErrInvalidInput)
			}
			coef = HugeintFromInt64(int64(binary.LittleEndian.Uint64(rest[:8])))
		default:
			if len(rest) < 16 {
				return Value{}, fmt.Errorf("%w: truncated VARIANT DECIMAL128", ErrInvalidInput)
			}
			coef = Hugeint{
				Lower: binary.LittleEndian.Uint64(rest[0:8]),
				Upper: int64(binary.LittleEndian.Uint64(rest[8:16])),
			}
		}
		return DecimalValue(Decimal{Width: uint8(width32), Scale: uint8(scale32), Value: coef})
	case variantTypeVarchar:
		b, err := readVarBytes()
		if err != nil {
			return Value{}, err
		}
		return VarcharValue(string(b)), nil
	case variantTypeBlob:
		b, err := readVarBytes()
		if err != nil {
			return Value{}, err
		}
		return BlobValue(b), nil
	case variantTypeUUID:
		b, err := readBytes(16)
		if err != nil {
			return Value{}, err
		}
		lower := binary.LittleEndian.Uint64(b[0:8])
		upper := binary.LittleEndian.Uint64(b[8:16])
		return Value{
			kind: uint8(TypeUUID),
			lo:   upper ^ (uint64(1) << 63),
			hi:   lower,
		}, nil
	case variantTypeDate:
		b, err := readBytes(4)
		if err != nil {
			return Value{}, err
		}
		return DateValue(Date{Days: int32(binary.LittleEndian.Uint32(b))}), nil
	case variantTypeTimeMicros:
		b, err := readBytes(8)
		if err != nil {
			return Value{}, err
		}
		return TimeValue(Time{Micros: int64(binary.LittleEndian.Uint64(b))}), nil
	case variantTypeTimeNanos:
		b, err := readBytes(8)
		if err != nil {
			return Value{}, err
		}
		return TimeNSValue(TimeNS{Nanos: int64(binary.LittleEndian.Uint64(b))}), nil
	case variantTypeTimestampSec:
		b, err := readBytes(8)
		if err != nil {
			return Value{}, err
		}
		return Value{kind: uint8(TypeTimestampS), lo: binary.LittleEndian.Uint64(b), tsU: uint8(TimestampUnitSeconds)}, nil
	case variantTypeTimestampMillis:
		b, err := readBytes(8)
		if err != nil {
			return Value{}, err
		}
		return Value{kind: uint8(TypeTimestampMS), lo: binary.LittleEndian.Uint64(b), tsU: uint8(TimestampUnitMillis)}, nil
	case variantTypeTimestampMicros:
		b, err := readBytes(8)
		if err != nil {
			return Value{}, err
		}
		return Value{kind: uint8(TypeTimestamp), lo: binary.LittleEndian.Uint64(b), tsU: uint8(TimestampUnitMicros)}, nil
	case variantTypeTimestampNanos:
		b, err := readBytes(8)
		if err != nil {
			return Value{}, err
		}
		return Value{kind: uint8(TypeTimestampNS), lo: binary.LittleEndian.Uint64(b), tsU: uint8(TimestampUnitNanos)}, nil
	case variantTypeTimeMicrosTZ:
		b, err := readBytes(8)
		if err != nil {
			return Value{}, err
		}
		const (
			timeTZOffsetBits = 24
			timeTZOffsetMask = (uint64(1) << timeTZOffsetBits) - 1
		)
		bits := binary.LittleEndian.Uint64(b)
		micros := int64(bits >> timeTZOffsetBits)
		offset := timeTZMaxOffset - int32(bits&timeTZOffsetMask)
		return Value{kind: uint8(TypeTimeTZ), lo: uint64(micros), hi: uint64(int64(offset))}, nil
	case variantTypeTimestampTZ:
		b, err := readBytes(8)
		if err != nil {
			return Value{}, err
		}
		return Value{kind: uint8(TypeTimestampTZ), lo: binary.LittleEndian.Uint64(b), tsU: uint8(TimestampUnitTZ)}, nil
	case variantTypeInterval:
		b, err := readBytes(16)
		if err != nil {
			return Value{}, err
		}
		return IntervalValue(Interval{
			Months: int32(binary.LittleEndian.Uint32(b[0:4])),
			Days:   int32(binary.LittleEndian.Uint32(b[4:8])),
			Micros: int64(binary.LittleEndian.Uint64(b[8:16])),
		}), nil
	case variantTypeBignum:
		b, err := readVarBytes()
		if err != nil {
			return Value{}, err
		}
		bn, err := decodeBignumWire(b)
		if err != nil {
			return Value{}, err
		}
		return BignumValue(bn), nil
	case variantTypeBitstring:
		b, err := readVarBytes()
		if err != nil {
			return Value{}, err
		}
		return BitValue(Bit{Data: b})
	case variantTypeGeometry:
		b, err := readVarBytes()
		if err != nil {
			return Value{}, err
		}
		return GeometryValue(Geometry{WKB: b}), nil
	case variantTypeObject:
		return d.decodeObject(valIdx, payload, depth)
	case variantTypeArray:
		return d.decodeArray(valIdx, payload, depth)
	default:
		return Value{}, fmt.Errorf("%w: unknown VARIANT logical type tag %d", ErrUnsupportedType, typeID)
	}
}

func (d *variantRowDecoder) decodeNestedHeader(payload []byte) (count uint32, childrenIdx uint32, err error) {
	c, n1, err := decodeUvarint32(payload)
	if err != nil {
		return 0, 0, err
	}
	if uint64(c) > d.limits.MaxCollectionLen {
		return 0, 0, fmt.Errorf("%w: VARIANT nested length %d exceeds %d", ErrLimitExceeded, c, d.limits.MaxCollectionLen)
	}
	if c == 0 {
		return 0, 0, nil
	}
	idx, _, err := decodeUvarint32(payload[n1:])
	if err != nil {
		return 0, 0, err
	}
	if uint64(idx)+uint64(c) > d.childrenEntry.Length {
		return 0, 0, fmt.Errorf("%w: VARIANT children slice [%d,+%d) exceeds length %d", ErrInvalidInput, idx, c, d.childrenEntry.Length)
	}
	return c, idx, nil
}

func (d *variantRowDecoder) decodeObject(valIdx uint32, payload []byte, depth int) (Value, error) {
	count, childrenIdx, err := d.decodeNestedHeader(payload)
	if err != nil {
		return Value{}, err
	}
	d.visiting[valIdx] = struct{}{}
	defer delete(d.visiting, valIdx)

	fields := make([]StructFieldValue, int(count))
	fieldTypes := make([]StructFieldType, int(count))
	for i := range count {
		absChildRow := d.childrenEntry.Offset + uint64(childrenIdx) + uint64(i)
		if !rowValid(d.childKeysValid, absChildRow) || !rowValid(d.childValsValid, absChildRow) {
			return Value{}, fmt.Errorf("%w: VARIANT OBJECT child %d has NULL key/value index", ErrInvalidInput, i)
		}
		keyIdx := readAt[uint32](d.childKeysData, absChildRow)
		childValIdx := readAt[uint32](d.childValsData, absChildRow)
		if uint64(keyIdx) >= d.keysEntry.Length {
			return Value{}, fmt.Errorf("%w: VARIANT keys_index %d out of range [0,%d)", ErrInvalidInput, keyIdx, d.keysEntry.Length)
		}
		absKeyRow := d.keysEntry.Offset + uint64(keyIdx)
		if !rowValid(d.keysChildValid, absKeyRow) {
			return Value{}, fmt.Errorf("%w: VARIANT key %d is NULL", ErrInvalidInput, keyIdx)
		}
		keyBytes, err := readNativeStringBytes(d.keysChildData, absKeyRow, d.limits.MaxValueBytes)
		if err != nil {
			return Value{}, fmt.Errorf("VARIANT object key[%d]: %w", i, err)
		}
		keyName := string(keyBytes)
		fv, err := d.decodeNode(childValIdx, depth+1)
		if err != nil {
			return Value{}, fmt.Errorf("VARIANT object field %q: %w", keyName, err)
		}
		ft := fv.Type()
		fields[i] = StructFieldValue{Name: keyName, Type: ft, Value: fv}
		fieldTypes[i] = StructFieldType{Name: keyName, Type: ft}
	}
	lt := StructType(fieldTypes...)
	return Value{kind: uint8(TypeStruct), ref: fields, lt: &lt}, nil
}

func (d *variantRowDecoder) decodeArray(valIdx uint32, payload []byte, depth int) (Value, error) {
	count, childrenIdx, err := d.decodeNestedHeader(payload)
	if err != nil {
		return Value{}, err
	}
	d.visiting[valIdx] = struct{}{}
	defer delete(d.visiting, valIdx)

	elems := make([]Value, int(count))
	for i := range count {
		absChildRow := d.childrenEntry.Offset + uint64(childrenIdx) + uint64(i)
		if !rowValid(d.childValsValid, absChildRow) {
			return Value{}, fmt.Errorf("%w: VARIANT ARRAY element %d has NULL values_index", ErrInvalidInput, i)
		}
		childValIdx := readAt[uint32](d.childValsData, absChildRow)
		ev, err := d.decodeNode(childValIdx, depth+1)
		if err != nil {
			return Value{}, fmt.Errorf("VARIANT array[%d]: %w", i, err)
		}
		elems[i] = ev
	}
	lt := ListType(ScalarType(TypeVariant))
	return Value{kind: uint8(TypeList), ref: elems, lt: &lt}, nil
}
