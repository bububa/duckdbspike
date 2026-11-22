package duckdb

import (
	"fmt"
	"math"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

// DecodeLimits bounds Go-side memory and recursion while decoding native
// chunks. Zero fields use DefaultDecodeLimits.
type DecodeLimits struct {
	MaxNestingDepth  int
	MaxValueBytes    uint64
	MaxCollectionLen uint64
}

// DefaultDecodeLimits returns conservative per-value decoding bounds.
func DefaultDecodeLimits() DecodeLimits {
	return DecodeLimits{
		MaxNestingDepth:  16,
		MaxValueBytes:    64 << 20, // 64 MiB
		MaxCollectionLen: 1_000_000,
	}
}

func (l DecodeLimits) normalized() (DecodeLimits, error) {
	def := DefaultDecodeLimits()
	if l.MaxNestingDepth < 0 {
		return DecodeLimits{}, fmt.Errorf("%w: negative MaxNestingDepth", ErrInvalidInput)
	}
	if l.MaxNestingDepth == 0 {
		l.MaxNestingDepth = def.MaxNestingDepth
	}
	if l.MaxValueBytes == 0 {
		l.MaxValueBytes = def.MaxValueBytes
	}
	if l.MaxCollectionLen == 0 {
		l.MaxCollectionLen = def.MaxCollectionLen
	}
	return l, nil
}

// columnPlan is an immutable decoded type tree built once at statement/result
// inspection so no native duckdb_logical_type handle stays open during fetch.
type columnPlan struct {
	lt              LogicalType
	decimalInternal int32
	elem            *columnPlan
	key             *columnPlan
	val             *columnPlan
	fields          []columnFieldPlan
}

type columnFieldPlan struct {
	name string
	plan columnPlan
}

func buildColumnPlan(api *abi.API, lt uintptr, depth int, limits DecodeLimits) (columnPlan, error) {
	if lt == 0 {
		return columnPlan{}, fmt.Errorf("%w: null logical type", ErrUnsupportedType)
	}
	if depth > limits.MaxNestingDepth {
		return columnPlan{}, fmt.Errorf("%w: nesting depth %d exceeds %d", ErrLimitExceeded, depth, limits.MaxNestingDepth)
	}
	id := api.GetTypeID(lt)
	switch id {
	case abi.TypeBoolean:
		return columnPlan{lt: ScalarType(TypeBoolean)}, nil
	case abi.TypeTinyint:
		return columnPlan{lt: ScalarType(TypeTinyint)}, nil
	case abi.TypeSmallint:
		return columnPlan{lt: ScalarType(TypeSmallint)}, nil
	case abi.TypeInteger:
		return columnPlan{lt: ScalarType(TypeInteger)}, nil
	case abi.TypeBigint:
		return columnPlan{lt: ScalarType(TypeBigint)}, nil
	case abi.TypeUTinyint:
		return columnPlan{lt: ScalarType(TypeUTinyint)}, nil
	case abi.TypeUSmallint:
		return columnPlan{lt: ScalarType(TypeUSmallint)}, nil
	case abi.TypeUInteger:
		return columnPlan{lt: ScalarType(TypeUInteger)}, nil
	case abi.TypeUBigint:
		return columnPlan{lt: ScalarType(TypeUBigint)}, nil
	case abi.TypeHugeint:
		return columnPlan{lt: ScalarType(TypeHugeint)}, nil
	case abi.TypeUHugeint:
		return columnPlan{lt: ScalarType(TypeUhugeint)}, nil
	case abi.TypeFloat:
		return columnPlan{lt: ScalarType(TypeFloat)}, nil
	case abi.TypeDouble:
		return columnPlan{lt: ScalarType(TypeDouble)}, nil
	case abi.TypeVarchar:
		return columnPlan{lt: ScalarType(TypeVarchar)}, nil
	case abi.TypeBlob:
		return columnPlan{lt: ScalarType(TypeBlob)}, nil
	case abi.TypeDate:
		return columnPlan{lt: ScalarType(TypeDate)}, nil
	case abi.TypeTime:
		return columnPlan{lt: ScalarType(TypeTime)}, nil
	case abi.TypeTimeNS:
		return columnPlan{lt: ScalarType(TypeTimeNS)}, nil
	case abi.TypeTimestamp:
		return columnPlan{lt: ScalarType(TypeTimestamp)}, nil
	case abi.TypeTimestampS:
		return columnPlan{lt: ScalarType(TypeTimestampS)}, nil
	case abi.TypeTimestampMS:
		return columnPlan{lt: ScalarType(TypeTimestampMS)}, nil
	case abi.TypeTimestampNS:
		return columnPlan{lt: ScalarType(TypeTimestampNS)}, nil
	case abi.TypeTimestampTZ:
		return columnPlan{lt: ScalarType(TypeTimestampTZ)}, nil
	case abi.TypeInterval:
		return columnPlan{lt: ScalarType(TypeInterval)}, nil
	case abi.TypeDecimal:
		width := api.DecimalWidth(lt)
		scale := api.DecimalScale(lt)
		if width < 1 || width > 38 || scale > width {
			return columnPlan{}, fmt.Errorf("%w: invalid DECIMAL(%d,%d)", ErrUnsupportedType, width, scale)
		}
		internal := api.DecimalInternalType(lt)
		switch internal {
		case abi.TypeSmallint, abi.TypeInteger, abi.TypeBigint, abi.TypeHugeint:
		default:
			return columnPlan{}, fmt.Errorf("%w: unsupported DECIMAL physical type %d", ErrUnsupportedType, internal)
		}
		return columnPlan{
			lt:              DecimalType(width, scale),
			decimalInternal: internal,
		}, nil
	case abi.TypeList:
		childLT := api.ListTypeChildType(lt)
		defer api.DestroyLogicalType(&childLT)
		childPlan, err := buildColumnPlan(api, childLT, depth+1, limits)
		if err != nil {
			return columnPlan{}, err
		}
		elemLT := childPlan.lt
		return columnPlan{
			lt:   LogicalType{Kind: TypeList, Elem: &elemLT},
			elem: &childPlan,
		}, nil
	case abi.TypeArray:
		size := api.ArrayTypeArraySize(lt)
		if size > limits.MaxCollectionLen {
			return columnPlan{}, fmt.Errorf("%w: array size %d exceeds %d", ErrLimitExceeded, size, limits.MaxCollectionLen)
		}
		childLT := api.ArrayTypeChildType(lt)
		defer api.DestroyLogicalType(&childLT)
		childPlan, err := buildColumnPlan(api, childLT, depth+1, limits)
		if err != nil {
			return columnPlan{}, err
		}
		elemLT := childPlan.lt
		return columnPlan{
			lt:   LogicalType{Kind: TypeArray, ArraySize: size, Elem: &elemLT},
			elem: &childPlan,
		}, nil
	case abi.TypeMap:
		keyLT := api.MapTypeKeyType(lt)
		valLT := api.MapTypeValueType(lt)
		defer api.DestroyLogicalType(&keyLT)
		defer api.DestroyLogicalType(&valLT)
		keyPlan, err := buildColumnPlan(api, keyLT, depth+1, limits)
		if err != nil {
			return columnPlan{}, err
		}
		valPlan, err := buildColumnPlan(api, valLT, depth+1, limits)
		if err != nil {
			return columnPlan{}, err
		}
		kLT, vLT := keyPlan.lt, valPlan.lt
		return columnPlan{
			lt:  LogicalType{Kind: TypeMap, Key: &kLT, Value: &vLT},
			key: &keyPlan,
			val: &valPlan,
		}, nil
	case abi.TypeStruct:
		count := api.StructTypeChildCount(lt)
		if count > limits.MaxCollectionLen || count > math.MaxInt {
			return columnPlan{}, fmt.Errorf("%w: struct field count %d exceeds %d", ErrLimitExceeded, count, limits.MaxCollectionLen)
		}
		fields := make([]StructFieldType, 0, count)
		fieldPlans := make([]columnFieldPlan, 0, count)
		for i := uint64(0); i < count; i++ {
			namePtr := api.StructTypeChildName(lt, i)
			name := abi.GoString(namePtr)
			if namePtr != 0 {
				api.Free(namePtr)
			}
			childLT := api.StructTypeChildType(lt, i)
			childPlan, err := buildColumnPlan(api, childLT, depth+1, limits)
			api.DestroyLogicalType(&childLT)
			if err != nil {
				return columnPlan{}, err
			}
			fields = append(fields, StructFieldType{Name: name, Type: childPlan.lt})
			fieldPlans = append(fieldPlans, columnFieldPlan{name: name, plan: childPlan})
		}
		return columnPlan{
			lt:     LogicalType{Kind: TypeStruct, Fields: fields},
			fields: fieldPlans,
		}, nil
	default:
		return columnPlan{}, fmt.Errorf("%w: native type id %d", ErrUnsupportedType, id)
	}
}

func rowValid(validity uintptr, row uint64) bool {
	if validity == 0 {
		return true
	}
	word := readAt[uint64](validity, row/64)
	return word&(uint64(1)<<(row%64)) != 0
}

func readAt[T any](base uintptr, index uint64) T {
	var zero T
	off := uintptr(index) * unsafe.Sizeof(zero)
	return *(*T)(unsafe.Add(abi.Pointer(base), off))
}

func copyNativeBytes(base uintptr, length uint64) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}
	if base == 0 {
		return nil, fmt.Errorf("%w: null native buffer for %d bytes", ErrInvalidInput, length)
	}
	if length > math.MaxInt {
		return nil, fmt.Errorf("%w: native length %d overflows int", ErrLimitExceeded, length)
	}
	n := int(length)
	out := make([]byte, n)
	copy(out, unsafe.Slice((*byte)(abi.Pointer(base)), n))
	return out, nil
}

func decodeVectorValue(api *abi.API, plan *columnPlan, vec uintptr, row uint64, limits DecodeLimits) (Value, error) {
	if vec == 0 {
		return Value{}, fmt.Errorf("%w: null native vector", ErrInvalidInput)
	}
	if !rowValid(api.VectorGetValidity(vec), row) {
		return TypedNullValue(plan.lt), nil
	}

	switch plan.lt.Kind {
	case TypeStruct:
		fields := make([]StructFieldValue, len(plan.fields))
		for i := range plan.fields {
			childVec := api.StructVectorGetChild(vec, uint64(i))
			v, err := decodeVectorValue(api, &plan.fields[i].plan, childVec, row, limits)
			if err != nil {
				return Value{}, err
			}
			fields[i] = StructFieldValue{
				Name:  plan.fields[i].name,
				Type:  plan.fields[i].plan.lt,
				Value: v,
			}
		}
		return Value{
			typ: plan.lt,
			st:  StructValue{Fields: fields},
		}, nil

	case TypeArray:
		size := plan.lt.ArraySize
		if size > limits.MaxCollectionLen || size > math.MaxInt {
			return Value{}, fmt.Errorf("%w: array size %d exceeds %d", ErrLimitExceeded, size, limits.MaxCollectionLen)
		}
		if size > 0 && row > math.MaxUint64/size {
			return Value{}, fmt.Errorf("%w: array row offset overflow (%d * %d)", ErrLimitExceeded, row, size)
		}
		base := row * size
		if base+size < base {
			return Value{}, fmt.Errorf("%w: array range overflow", ErrLimitExceeded)
		}
		childVec := api.ArrayVectorGetChild(vec)
		elems := make([]Value, int(size))
		for j := uint64(0); j < size; j++ {
			v, err := decodeVectorValue(api, plan.elem, childVec, base+j, limits)
			if err != nil {
				return Value{}, err
			}
			elems[j] = v
		}
		return Value{
			typ: plan.lt,
			arr: ArrayValue{ElemType: plan.elem.lt, Size: size, Elements: elems},
		}, nil
	}

	data := api.VectorGetData(vec)
	if data == 0 {
		return Value{}, fmt.Errorf("%w: null vector data pointer for %s", ErrInvalidInput, plan.lt.Kind)
	}

	switch plan.lt.Kind {
	case TypeBoolean:
		return BoolValue(readAt[byte](data, row) != 0), nil
	case TypeTinyint:
		return Int8Value(readAt[int8](data, row)), nil
	case TypeSmallint:
		return Int16Value(readAt[int16](data, row)), nil
	case TypeInteger:
		return Int32Value(readAt[int32](data, row)), nil
	case TypeBigint:
		return Int64Value(readAt[int64](data, row)), nil
	case TypeUTinyint:
		return Uint8Value(readAt[uint8](data, row)), nil
	case TypeUSmallint:
		return Uint16Value(readAt[uint16](data, row)), nil
	case TypeUInteger:
		return Uint32Value(readAt[uint32](data, row)), nil
	case TypeUBigint:
		return Uint64Value(readAt[uint64](data, row)), nil
	case TypeHugeint:
		h := readAt[abi.Hugeint](data, row)
		return HugeintValue(Hugeint{Lower: h.Lower, Upper: h.Upper}), nil
	case TypeUhugeint:
		u := readAt[abi.Uhugeint](data, row)
		return UhugeintValue(Uhugeint{Lower: u.Lower, Upper: u.Upper}), nil
	case TypeFloat:
		return Float32Value(readAt[float32](data, row)), nil
	case TypeDouble:
		return Float64Value(readAt[float64](data, row)), nil
	case TypeDecimal:
		var coef Hugeint
		switch plan.decimalInternal {
		case abi.TypeSmallint:
			coef = HugeintFromInt64(int64(readAt[int16](data, row)))
		case abi.TypeInteger:
			coef = HugeintFromInt64(int64(readAt[int32](data, row)))
		case abi.TypeBigint:
			coef = HugeintFromInt64(readAt[int64](data, row))
		case abi.TypeHugeint:
			h := readAt[abi.Hugeint](data, row)
			coef = Hugeint{Lower: h.Lower, Upper: h.Upper}
		default:
			return Value{}, fmt.Errorf("%w: unsupported DECIMAL physical type %d", ErrUnsupportedType, plan.decimalInternal)
		}
		return DecimalValue(Decimal{
			Width: plan.lt.DecimalWidth,
			Scale: plan.lt.DecimalScale,
			Value: coef,
		})
	case TypeVarchar, TypeBlob:
		const stride = uint64(unsafe.Sizeof(abi.StringT{}))
		if row > math.MaxUint64/stride {
			return Value{}, fmt.Errorf("%w: string row offset overflow", ErrLimitExceeded)
		}
		addr := data + uintptr(row*stride)
		s := *(*abi.StringT)(abi.Pointer(addr))
		length := uint64(s.Length)
		if length > limits.MaxValueBytes {
			return Value{}, fmt.Errorf("%w: value length %d exceeds %d", ErrLimitExceeded, length, limits.MaxValueBytes)
		}
		var raw []byte
		var err error
		if s.Length <= abi.InlineStringLength {
			raw, err = copyNativeBytes(addr+unsafe.Offsetof(abi.StringT{}.Prefix), length)
		} else {
			raw, err = copyNativeBytes(s.Ptr, length)
		}
		if err != nil {
			return Value{}, err
		}
		if plan.lt.Kind == TypeVarchar {
			return VarcharValue(string(raw)), nil
		}
		return Value{typ: ScalarType(TypeBlob), blob: raw}, nil
	case TypeDate:
		return DateValue(Date{Days: readAt[int32](data, row)}), nil
	case TypeTime:
		return TimeValue(Time{Micros: readAt[int64](data, row)}), nil
	case TypeTimeNS:
		return TimeNSValue(TimeNS{Nanos: readAt[int64](data, row)}), nil
	case TypeTimestamp:
		return Value{typ: ScalarType(TypeTimestamp), ts: Timestamp{Value: readAt[int64](data, row), Unit: TimestampUnitMicros}}, nil
	case TypeTimestampS:
		return Value{typ: ScalarType(TypeTimestampS), ts: Timestamp{Value: readAt[int64](data, row), Unit: TimestampUnitSeconds}}, nil
	case TypeTimestampMS:
		return Value{typ: ScalarType(TypeTimestampMS), ts: Timestamp{Value: readAt[int64](data, row), Unit: TimestampUnitMillis}}, nil
	case TypeTimestampNS:
		return Value{typ: ScalarType(TypeTimestampNS), ts: Timestamp{Value: readAt[int64](data, row), Unit: TimestampUnitNanos}}, nil
	case TypeTimestampTZ:
		return Value{typ: ScalarType(TypeTimestampTZ), ts: Timestamp{Value: readAt[int64](data, row), Unit: TimestampUnitTZ}}, nil
	case TypeInterval:
		iv := readAt[abi.Interval](data, row)
		return IntervalValue(Interval{Months: iv.Months, Days: iv.Days, Micros: iv.Micros}), nil
	case TypeList:
		entry := readAt[abi.ListEntry](data, row)
		if entry.Length > limits.MaxCollectionLen || entry.Length > math.MaxInt {
			return Value{}, fmt.Errorf("%w: list length %d exceeds %d", ErrLimitExceeded, entry.Length, limits.MaxCollectionLen)
		}
		end := entry.Offset + entry.Length
		if end < entry.Offset {
			return Value{}, fmt.Errorf("%w: list entry offset overflow", ErrLimitExceeded)
		}
		if end > api.ListVectorGetSize(vec) {
			return Value{}, fmt.Errorf("%w: list entry [%d:%d) exceeds child size %d", ErrInvalidInput, entry.Offset, end, api.ListVectorGetSize(vec))
		}
		childVec := api.ListVectorGetChild(vec)
		elems := make([]Value, int(entry.Length))
		for j := uint64(0); j < entry.Length; j++ {
			v, err := decodeVectorValue(api, plan.elem, childVec, entry.Offset+j, limits)
			if err != nil {
				return Value{}, err
			}
			elems[j] = v
		}
		return Value{
			typ:  plan.lt,
			list: ListValue{ElemType: plan.elem.lt, Elements: elems},
		}, nil
	case TypeMap:
		entry := readAt[abi.ListEntry](data, row)
		if entry.Length > limits.MaxCollectionLen || entry.Length > math.MaxInt {
			return Value{}, fmt.Errorf("%w: map length %d exceeds %d", ErrLimitExceeded, entry.Length, limits.MaxCollectionLen)
		}
		end := entry.Offset + entry.Length
		if end < entry.Offset {
			return Value{}, fmt.Errorf("%w: map entry offset overflow", ErrLimitExceeded)
		}
		if end > api.ListVectorGetSize(vec) {
			return Value{}, fmt.Errorf("%w: map entry [%d:%d) exceeds child size %d", ErrInvalidInput, entry.Offset, end, api.ListVectorGetSize(vec))
		}
		structVec := api.ListVectorGetChild(vec)
		keyVec := api.StructVectorGetChild(structVec, 0)
		valVec := api.StructVectorGetChild(structVec, 1)
		entries := make([]MapEntry, int(entry.Length))
		for j := uint64(0); j < entry.Length; j++ {
			k, err := decodeVectorValue(api, plan.key, keyVec, entry.Offset+j, limits)
			if err != nil {
				return Value{}, err
			}
			v, err := decodeVectorValue(api, plan.val, valVec, entry.Offset+j, limits)
			if err != nil {
				return Value{}, err
			}
			entries[j] = MapEntry{Key: k, Value: v}
		}
		return Value{
			typ: plan.lt,
			mp:  MapValue{KeyType: plan.key.lt, ValueType: plan.val.lt, Entries: entries},
		}, nil
	default:
		return Value{}, fmt.Errorf("%w: %s", ErrUnsupportedType, plan.lt.Kind)
	}
}
