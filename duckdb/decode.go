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
	enumInternal    int32
	enumLookup      map[string]uint32
	elem            *columnPlan
	key             *columnPlan
	val             *columnPlan
	fields          []columnFieldPlan
}

type columnFieldPlan struct {
	name string
	plan columnPlan
}

func readLogicalTypeAlias(api *abi.API, lt uintptr) string {
	if api.LogicalTypeGetAlias == nil {
		return ""
	}
	ptr := api.LogicalTypeGetAlias(lt)
	if ptr == 0 {
		return ""
	}
	alias := abi.GoString(ptr)
	api.Free(ptr)
	return alias
}

func buildColumnPlan(api *abi.API, lt uintptr, depth int, limits DecodeLimits) (columnPlan, error) {
	if lt == 0 {
		return columnPlan{}, fmt.Errorf("%w: null logical type", ErrUnsupportedType)
	}
	if depth > limits.MaxNestingDepth {
		return columnPlan{}, fmt.Errorf("%w: nesting depth %d exceeds %d", ErrLimitExceeded, depth, limits.MaxNestingDepth)
	}
	id := api.GetTypeID(lt)
	alias := readLogicalTypeAlias(api, lt)

	var plan columnPlan
	switch id {
	case abi.TypeBoolean:
		plan = columnPlan{lt: ScalarType(TypeBoolean)}
	case abi.TypeTinyint:
		plan = columnPlan{lt: ScalarType(TypeTinyint)}
	case abi.TypeSmallint:
		plan = columnPlan{lt: ScalarType(TypeSmallint)}
	case abi.TypeInteger:
		plan = columnPlan{lt: ScalarType(TypeInteger)}
	case abi.TypeBigint:
		plan = columnPlan{lt: ScalarType(TypeBigint)}
	case abi.TypeUTinyint:
		plan = columnPlan{lt: ScalarType(TypeUTinyint)}
	case abi.TypeUSmallint:
		plan = columnPlan{lt: ScalarType(TypeUSmallint)}
	case abi.TypeUInteger:
		plan = columnPlan{lt: ScalarType(TypeUInteger)}
	case abi.TypeUBigint:
		plan = columnPlan{lt: ScalarType(TypeUBigint)}
	case abi.TypeHugeint:
		plan = columnPlan{lt: ScalarType(TypeHugeint)}
	case abi.TypeUHugeint:
		plan = columnPlan{lt: ScalarType(TypeUhugeint)}
	case abi.TypeFloat:
		plan = columnPlan{lt: ScalarType(TypeFloat)}
	case abi.TypeDouble:
		plan = columnPlan{lt: ScalarType(TypeDouble)}
	case abi.TypeVarchar:
		plan = columnPlan{lt: ScalarType(TypeVarchar)}
	case abi.TypeBlob:
		plan = columnPlan{lt: ScalarType(TypeBlob)}
	case abi.TypeDate:
		plan = columnPlan{lt: ScalarType(TypeDate)}
	case abi.TypeTime:
		plan = columnPlan{lt: ScalarType(TypeTime)}
	case abi.TypeTimeNS:
		plan = columnPlan{lt: ScalarType(TypeTimeNS)}
	case abi.TypeTimeTZ:
		plan = columnPlan{lt: ScalarType(TypeTimeTZ)}
	case abi.TypeTimestamp:
		plan = columnPlan{lt: ScalarType(TypeTimestamp)}
	case abi.TypeTimestampS:
		plan = columnPlan{lt: ScalarType(TypeTimestampS)}
	case abi.TypeTimestampMS:
		plan = columnPlan{lt: ScalarType(TypeTimestampMS)}
	case abi.TypeTimestampNS:
		plan = columnPlan{lt: ScalarType(TypeTimestampNS)}
	case abi.TypeTimestampTZ:
		plan = columnPlan{lt: ScalarType(TypeTimestampTZ)}
	case abi.TypeInterval:
		plan = columnPlan{lt: ScalarType(TypeInterval)}
	case abi.TypeUUID:
		plan = columnPlan{lt: ScalarType(TypeUUID)}
	case abi.TypeSQLNull:
		plan = columnPlan{lt: ScalarType(TypeSQLNull)}
	case abi.TypeBit:
		plan = columnPlan{lt: ScalarType(TypeBit)}
	case abi.TypeBignum:
		plan = columnPlan{lt: ScalarType(TypeBignum)}
	case abi.TypeGeometry:
		var crs string
		if api.GeometryTypeGetCRS != nil {
			if ptr := api.GeometryTypeGetCRS(lt); ptr != 0 {
				crs = abi.GoString(ptr)
				api.Free(ptr)
			}
		}
		plan = columnPlan{lt: GeometryType(crs)}
	case abi.TypeVariant:
		if err := validateVariantLogicalType(api, lt); err != nil {
			return columnPlan{}, err
		}
		plan = columnPlan{lt: ScalarType(TypeVariant)}
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
		plan = columnPlan{
			lt:              DecimalType(width, scale),
			decimalInternal: internal,
		}
	case abi.TypeEnum:
		internal := api.EnumInternalType(lt)
		switch internal {
		case abi.TypeUTinyint, abi.TypeUSmallint, abi.TypeUInteger:
		default:
			return columnPlan{}, fmt.Errorf("%w: unsupported ENUM physical type %d", ErrUnsupportedType, internal)
		}
		dictSize := uint64(api.EnumDictionarySize(lt))
		if dictSize > limits.MaxCollectionLen || dictSize > math.MaxInt {
			return columnPlan{}, fmt.Errorf("%w: enum dictionary size %d exceeds %d", ErrLimitExceeded, dictSize, limits.MaxCollectionLen)
		}
		values := make([]string, int(dictSize))
		lookup := make(map[string]uint32, int(dictSize))
		for i := range dictSize {
			ptr := api.EnumDictionaryValue(lt, i)
			label := abi.GoString(ptr)
			if ptr != 0 {
				api.Free(ptr)
			}
			if uint64(len(label)) > limits.MaxValueBytes {
				return columnPlan{}, fmt.Errorf("%w: enum label length %d exceeds %d", ErrLimitExceeded, len(label), limits.MaxValueBytes)
			}
			values[i] = label
			lookup[label] = uint32(i)
		}
		plan = columnPlan{
			lt:           LogicalType{Kind: TypeEnum, EnumValues: values},
			enumInternal: internal,
			enumLookup:   lookup,
		}
	case abi.TypeList:
		childLT := api.ListTypeChildType(lt)
		defer api.DestroyLogicalType(&childLT)
		childPlan, err := buildColumnPlan(api, childLT, depth+1, limits)
		if err != nil {
			return columnPlan{}, err
		}
		elemLT := childPlan.lt
		plan = columnPlan{
			lt:   LogicalType{Kind: TypeList, Elem: &elemLT},
			elem: &childPlan,
		}
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
		plan = columnPlan{
			lt:   LogicalType{Kind: TypeArray, ArraySize: size, Elem: &elemLT},
			elem: &childPlan,
		}
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
		plan = columnPlan{
			lt:  LogicalType{Kind: TypeMap, Key: &kLT, Value: &vLT},
			key: &keyPlan,
			val: &valPlan,
		}
	case abi.TypeStruct:
		count := api.StructTypeChildCount(lt)
		if count > limits.MaxCollectionLen || count > math.MaxInt {
			return columnPlan{}, fmt.Errorf("%w: struct field count %d exceeds %d", ErrLimitExceeded, count, limits.MaxCollectionLen)
		}
		fields := make([]StructFieldType, 0, count)
		fieldPlans := make([]columnFieldPlan, 0, count)
		for i := range count {
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
		plan = columnPlan{
			lt:     LogicalType{Kind: TypeStruct, Fields: fields},
			fields: fieldPlans,
		}
	case abi.TypeUnion:
		count := api.UnionTypeMemberCount(lt)
		if count == 0 || count > 256 || count > limits.MaxCollectionLen {
			return columnPlan{}, fmt.Errorf("%w: union member count %d out of range", ErrLimitExceeded, count)
		}
		fields := make([]StructFieldType, 0, count)
		fieldPlans := make([]columnFieldPlan, 0, count)
		for i := range count {
			namePtr := api.UnionTypeMemberName(lt, i)
			name := abi.GoString(namePtr)
			if namePtr != 0 {
				api.Free(namePtr)
			}
			childLT := api.UnionTypeMemberType(lt, i)
			childPlan, err := buildColumnPlan(api, childLT, depth+1, limits)
			api.DestroyLogicalType(&childLT)
			if err != nil {
				return columnPlan{}, err
			}
			fields = append(fields, StructFieldType{Name: name, Type: childPlan.lt})
			fieldPlans = append(fieldPlans, columnFieldPlan{name: name, plan: childPlan})
		}
		plan = columnPlan{
			lt:     LogicalType{Kind: TypeUnion, Fields: fields},
			fields: fieldPlans,
		}
	default:
		return columnPlan{}, fmt.Errorf("%w: native type id %d", ErrUnsupportedType, id)
	}
	plan.lt.Alias = alias
	return plan, nil
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

// decodeBignumWire decodes DuckDB's 3-byte sortable header + big-endian
// magnitude wire representation into an owned immutable Bignum without
// mutating raw.
func decodeBignumWire(raw []byte) (Bignum, error) {
	if len(raw) < 3 {
		return Bignum{}, fmt.Errorf("%w: truncated BIGNUM wire payload (%d bytes)", ErrInvalidInput, len(raw))
	}
	isNeg := raw[0]&0x80 == 0
	src := raw[3:]
	var mag []byte
	if len(src) > 0 {
		mag = make([]byte, len(src))
		if isNeg {
			for i, b := range src {
				mag[i] = ^b
			}
		} else {
			copy(mag, src)
		}
		trim := 0
		for trim < len(mag) && mag[trim] == 0 {
			trim++
		}
		mag = mag[trim:]
	}
	if len(mag) == 0 {
		return Bignum{}, nil
	}
	return Bignum{neg: isNeg, mag: mag}, nil
}

func attachPlanAlias(v Value, plan *columnPlan) Value {
	if plan.lt.Alias != "" && v.lt == nil {
		v.lt = &plan.lt
	}
	return v
}

func decodeVectorValue(api *abi.API, plan *columnPlan, vec uintptr, row uint64, limits DecodeLimits) (Value, error) {
	if plan.lt.Kind == TypeSQLNull {
		return typedNullFromPlan(&plan.lt), nil
	}
	if vec == 0 {
		return Value{}, fmt.Errorf("%w: null native vector", ErrInvalidInput)
	}
	if !rowValid(api.VectorGetValidity(vec), row) {
		return typedNullFromPlan(&plan.lt), nil
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
			kind: uint8(TypeStruct),
			ref:  fields,
			lt:   &plan.lt,
		}, nil

	case TypeUnion:
		tagVec := api.StructVectorGetChild(vec, 0)
		if tagVec == 0 {
			return Value{}, fmt.Errorf("%w: null union tag vector", ErrInvalidInput)
		}
		if !rowValid(api.VectorGetValidity(tagVec), row) {
			return typedNullFromPlan(&plan.lt), nil
		}
		tagData := api.VectorGetData(tagVec)
		if tagData == 0 {
			return Value{}, fmt.Errorf("%w: null union tag data pointer", ErrInvalidInput)
		}
		tag := readAt[uint8](tagData, row)
		if int(tag) >= len(plan.fields) {
			return Value{}, fmt.Errorf("%w: union tag %d out of range [0,%d)", ErrOutOfRange, tag, len(plan.fields))
		}
		memberVec := api.StructVectorGetChild(vec, uint64(tag)+1)
		v, err := decodeVectorValue(api, &plan.fields[tag].plan, memberVec, row, limits)
		if err != nil {
			return Value{}, err
		}
		return Value{
			kind: uint8(TypeUnion),
			lo:   uint64(tag),
			ref:  v,
			lt:   &plan.lt,
		}, nil

	case TypeVariant:
		v, err := decodeVariantValue(api, vec, row, limits)
		if err != nil {
			return Value{}, err
		}
		return attachPlanAlias(v, plan), nil

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
		for j := range size {
			v, err := decodeVectorValue(api, plan.elem, childVec, base+j, limits)
			if err != nil {
				return Value{}, err
			}
			elems[j] = v
		}
		return Value{
			kind: uint8(TypeArray),
			ref:  elems,
			lt:   &plan.lt,
		}, nil
	}

	data := api.VectorGetData(vec)
	if data == 0 {
		return Value{}, fmt.Errorf("%w: null vector data pointer for %s", ErrInvalidInput, plan.lt.Kind)
	}

	switch plan.lt.Kind {
	case TypeBoolean:
		return attachPlanAlias(BoolValue(readAt[byte](data, row) != 0), plan), nil
	case TypeTinyint:
		return attachPlanAlias(Int8Value(readAt[int8](data, row)), plan), nil
	case TypeSmallint:
		return attachPlanAlias(Int16Value(readAt[int16](data, row)), plan), nil
	case TypeInteger:
		return attachPlanAlias(Int32Value(readAt[int32](data, row)), plan), nil
	case TypeBigint:
		return attachPlanAlias(Int64Value(readAt[int64](data, row)), plan), nil
	case TypeUTinyint:
		return attachPlanAlias(Uint8Value(readAt[uint8](data, row)), plan), nil
	case TypeUSmallint:
		return attachPlanAlias(Uint16Value(readAt[uint16](data, row)), plan), nil
	case TypeUInteger:
		return attachPlanAlias(Uint32Value(readAt[uint32](data, row)), plan), nil
	case TypeUBigint:
		return attachPlanAlias(Uint64Value(readAt[uint64](data, row)), plan), nil
	case TypeHugeint:
		h := readAt[abi.Hugeint](data, row)
		return attachPlanAlias(HugeintValue(Hugeint{Lower: h.Lower, Upper: h.Upper}), plan), nil
	case TypeUhugeint:
		u := readAt[abi.Uhugeint](data, row)
		return attachPlanAlias(UhugeintValue(Uhugeint{Lower: u.Lower, Upper: u.Upper}), plan), nil
	case TypeFloat:
		return attachPlanAlias(Float32Value(readAt[float32](data, row)), plan), nil
	case TypeDouble:
		return attachPlanAlias(Float64Value(readAt[float64](data, row)), plan), nil
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
		v, err := DecimalValue(Decimal{
			Width: plan.lt.DecimalWidth,
			Scale: plan.lt.DecimalScale,
			Value: coef,
		})
		if err != nil {
			return Value{}, err
		}
		return attachPlanAlias(v, plan), nil
	case TypeVarchar, TypeBlob, TypeBit, TypeBignum, TypeGeometry:
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
		switch plan.lt.Kind {
		case TypeVarchar:
			return attachPlanAlias(VarcharValue(string(raw)), plan), nil
		case TypeBlob:
			return attachPlanAlias(Value{kind: uint8(TypeBlob), ref: raw}, plan), nil
		case TypeBit:
			if err := (Bit{Data: raw}).Validate(); err != nil {
				return Value{}, err
			}
			return attachPlanAlias(Value{kind: uint8(TypeBit), ref: raw}, plan), nil
		case TypeBignum:
			bn, err := decodeBignumWire(raw)
			if err != nil {
				return Value{}, err
			}
			return attachPlanAlias(BignumValue(bn), plan), nil
		default: // TypeGeometry
			var lt *LogicalType
			if plan.lt.needsHeapMeta() {
				lt = &plan.lt
			}
			return Value{kind: uint8(TypeGeometry), ref: raw, lt: lt}, nil
		}
	case TypeDate:
		return attachPlanAlias(DateValue(Date{Days: readAt[int32](data, row)}), plan), nil
	case TypeTime:
		return attachPlanAlias(TimeValue(Time{Micros: readAt[int64](data, row)}), plan), nil
	case TypeTimeNS:
		return attachPlanAlias(TimeNSValue(TimeNS{Nanos: readAt[int64](data, row)}), plan), nil
	case TypeTimeTZ:
		const (
			timeTZOffsetBits = 24
			timeTZOffsetMask = (uint64(1) << timeTZOffsetBits) - 1
		)
		bits := readAt[abi.TimeTZ](data, row).Bits
		micros := int64(bits >> timeTZOffsetBits)
		offset := timeTZMaxOffset - int32(bits&timeTZOffsetMask)
		v := Value{
			kind: uint8(TypeTimeTZ),
			lo:   uint64(micros),
			hi:   uint64(int64(offset)),
		}
		return attachPlanAlias(v, plan), nil
	case TypeTimestamp:
		return attachPlanAlias(Value{kind: uint8(TypeTimestamp), lo: uint64(readAt[int64](data, row)), tsU: uint8(TimestampUnitMicros)}, plan), nil
	case TypeTimestampS:
		return attachPlanAlias(Value{kind: uint8(TypeTimestampS), lo: uint64(readAt[int64](data, row)), tsU: uint8(TimestampUnitSeconds)}, plan), nil
	case TypeTimestampMS:
		return attachPlanAlias(Value{kind: uint8(TypeTimestampMS), lo: uint64(readAt[int64](data, row)), tsU: uint8(TimestampUnitMillis)}, plan), nil
	case TypeTimestampNS:
		return attachPlanAlias(Value{kind: uint8(TypeTimestampNS), lo: uint64(readAt[int64](data, row)), tsU: uint8(TimestampUnitNanos)}, plan), nil
	case TypeTimestampTZ:
		return attachPlanAlias(Value{kind: uint8(TypeTimestampTZ), lo: uint64(readAt[int64](data, row)), tsU: uint8(TimestampUnitTZ)}, plan), nil
	case TypeInterval:
		iv := readAt[abi.Interval](data, row)
		return attachPlanAlias(IntervalValue(Interval{Months: iv.Months, Days: iv.Days, Micros: iv.Micros}), plan), nil
	case TypeUUID:
		h := readAt[abi.Hugeint](data, row)
		// DuckDB stores UUID vectors as hugeint with the sign bit (MSB) flipped
		// so signed 128-bit ordering matches lexicographical UUID ordering.
		v := Value{
			kind: uint8(TypeUUID),
			lo:   uint64(h.Upper) ^ (uint64(1) << 63),
			hi:   h.Lower,
		}
		return attachPlanAlias(v, plan), nil
	case TypeEnum:
		var idx uint32
		switch plan.enumInternal {
		case abi.TypeUTinyint:
			idx = uint32(readAt[uint8](data, row))
		case abi.TypeUSmallint:
			idx = uint32(readAt[uint16](data, row))
		case abi.TypeUInteger:
			idx = readAt[uint32](data, row)
		default:
			return Value{}, fmt.Errorf("%w: unsupported ENUM physical type %d", ErrUnsupportedType, plan.enumInternal)
		}
		if uint64(idx) >= uint64(len(plan.lt.EnumValues)) {
			return Value{}, fmt.Errorf("%w: enum index %d out of range [0,%d)", ErrOutOfRange, idx, len(plan.lt.EnumValues))
		}
		return Value{
			kind: uint8(TypeEnum),
			lo:   uint64(idx),
			ref:  plan.lt.EnumValues[idx],
			lt:   &plan.lt,
		}, nil
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
			kind: uint8(TypeList),
			ref:  elems,
			lt:   &plan.lt,
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
			kind: uint8(TypeMap),
			ref:  entries,
			lt:   &plan.lt,
		}, nil
	default:
		return Value{}, fmt.Errorf("%w: %s", ErrUnsupportedType, plan.lt.Kind)
	}
}
