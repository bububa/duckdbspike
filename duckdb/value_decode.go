package duckdb

import (
	"fmt"
	"math"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

// decodeNativeValue decodes an owned or borrowed duckdb_value handle into an
// owned Go duckdb.Value without lossy string or float intermediates. Callers
// remain responsible for calling api.DestroyValue on val.
func decodeNativeValue(api *abi.API, val uintptr, depth int, limits DecodeLimits) (Value, error) {
	if val == 0 {
		return NullValue(), nil
	}
	if depth > limits.MaxNestingDepth {
		return Value{}, fmt.Errorf("%w: value nesting depth %d exceeds %d", ErrLimitExceeded, depth, limits.MaxNestingDepth)
	}
	// GetValueType returns a BORROWED logical type valid while val is alive;
	// it must NOT be passed to DestroyLogicalType.
	lt := api.GetValueType(val)
	if lt == 0 {
		if api.IsNullValue(val) {
			return NullValue(), nil
		}
		return Value{}, fmt.Errorf("%w: native value has null logical type", ErrInvalidInput)
	}
	plan, err := buildColumnPlan(api, lt, depth, limits)
	if err != nil {
		return Value{}, err
	}
	if api.IsNullValue(val) {
		return typedNullFromPlan(&plan.lt), nil
	}
	switch plan.lt.Kind {
	case TypeSQLNull:
		return typedNullFromPlan(&plan.lt), nil
	case TypeBoolean:
		return attachPlanAlias(BoolValue(api.GetBool(val)), &plan), nil
	case TypeTinyint:
		return attachPlanAlias(Int8Value(api.GetInt8(val)), &plan), nil
	case TypeSmallint:
		return attachPlanAlias(Int16Value(api.GetInt16(val)), &plan), nil
	case TypeInteger:
		return attachPlanAlias(Int32Value(api.GetInt32(val)), &plan), nil
	case TypeBigint:
		return attachPlanAlias(Int64Value(api.GetInt64(val)), &plan), nil
	case TypeUTinyint:
		return attachPlanAlias(Uint8Value(api.GetUint8(val)), &plan), nil
	case TypeUSmallint:
		return attachPlanAlias(Uint16Value(api.GetUint16(val)), &plan), nil
	case TypeUInteger:
		return attachPlanAlias(Uint32Value(api.GetUint32(val)), &plan), nil
	case TypeUBigint:
		return attachPlanAlias(Uint64Value(api.GetUint64(val)), &plan), nil
	case TypeHugeint:
		h := api.GetHugeint(val)
		return attachPlanAlias(HugeintValue(Hugeint{Lower: h.Lower, Upper: h.Upper}), &plan), nil
	case TypeUhugeint:
		u := api.GetUhugeint(val)
		return attachPlanAlias(UhugeintValue(Uhugeint{Lower: u.Lower, Upper: u.Upper}), &plan), nil
	case TypeDecimal:
		d := api.GetDecimal(val)
		v := Value{
			kind: uint8(TypeDecimal),
			lo:   d.Value.Lower,
			hi:   uint64(d.Value.Upper),
			decW: d.Width,
			decS: d.Scale,
		}
		return attachPlanAlias(v, &plan), nil
	case TypeFloat:
		return attachPlanAlias(Float32Value(api.GetFloat(val)), &plan), nil
	case TypeDouble:
		return attachPlanAlias(Float64Value(api.GetDouble(val)), &plan), nil
	case TypeVarchar:
		ptr := api.GetVarchar(val)
		s := abi.GoString(ptr)
		if ptr != 0 {
			api.Free(ptr)
		}
		if uint64(len(s)) > limits.MaxValueBytes {
			return Value{}, fmt.Errorf("%w: varchar length %d exceeds %d", ErrLimitExceeded, len(s), limits.MaxValueBytes)
		}
		return attachPlanAlias(VarcharValue(s), &plan), nil
	case TypeBlob:
		b := api.GetBlob(val)
		defer func() {
			if b.Data != 0 {
				api.Free(b.Data)
			}
		}()
		if b.Size > limits.MaxValueBytes || b.Size > math.MaxInt {
			return Value{}, fmt.Errorf("%w: blob length %d exceeds %d", ErrLimitExceeded, b.Size, limits.MaxValueBytes)
		}
		cp := make([]byte, int(b.Size))
		if b.Size > 0 && b.Data != 0 {
			copy(cp, unsafe.Slice((*byte)(abi.Pointer(b.Data)), int(b.Size)))
		}
		return attachPlanAlias(BlobValue(cp), &plan), nil
	case TypeBit:
		b := api.GetBit(val)
		defer func() {
			if b.Data != 0 {
				api.Free(b.Data)
			}
		}()
		if b.Size > limits.MaxValueBytes || b.Size > math.MaxInt {
			return Value{}, fmt.Errorf("%w: bit length %d exceeds %d", ErrLimitExceeded, b.Size, limits.MaxValueBytes)
		}
		cp := make([]byte, int(b.Size))
		if b.Size > 0 && b.Data != 0 {
			copy(cp, unsafe.Slice((*byte)(abi.Pointer(b.Data)), int(b.Size)))
		}
		bv, err := BitValue(Bit{Data: cp})
		if err != nil {
			return Value{}, err
		}
		return attachPlanAlias(bv, &plan), nil
	case TypeBignum:
		bn := api.GetBignum(val)
		defer func() {
			if bn.Data != 0 {
				api.Free(bn.Data)
			}
		}()
		if bn.Size > limits.MaxValueBytes || bn.Size > math.MaxInt {
			return Value{}, fmt.Errorf("%w: bignum length %d exceeds %d", ErrLimitExceeded, bn.Size, limits.MaxValueBytes)
		}
		mag := make([]byte, int(bn.Size))
		if bn.Size > 0 && bn.Data != 0 {
			copy(mag, unsafe.Slice((*byte)(abi.Pointer(bn.Data)), int(bn.Size)))
		}
		// Strip leading zero bytes to keep canonical magnitude representation.
		trim := 0
		for trim < len(mag) && mag[trim] == 0 {
			trim++
		}
		mag = mag[trim:]
		neg := bn.IsNegative && len(mag) > 0
		return attachPlanAlias(BignumValue(Bignum{neg: neg, mag: mag}), &plan), nil
	case TypeDate:
		d := api.GetDate(val)
		return attachPlanAlias(DateValue(Date{Days: d.Days}), &plan), nil
	case TypeTime:
		t := api.GetTime(val)
		return attachPlanAlias(TimeValue(Time{Micros: t.Micros}), &plan), nil
	case TypeTimeNS:
		t := api.GetTimeNS(val)
		return attachPlanAlias(TimeNSValue(TimeNS{Nanos: t.Nanos}), &plan), nil
	case TypeTimeTZ:
		const (
			timeTZOffsetBits = 24
			timeTZOffsetMask = (uint64(1) << timeTZOffsetBits) - 1
		)
		bits := api.GetTimeTZ(val).Bits
		micros := int64(bits >> timeTZOffsetBits)
		offset := timeTZMaxOffset - int32(bits&timeTZOffsetMask)
		return attachPlanAlias(Value{kind: uint8(TypeTimeTZ), lo: uint64(micros), hi: uint64(int64(offset))}, &plan), nil
	case TypeTimestamp:
		ts := api.GetTimestamp(val)
		return attachPlanAlias(Value{kind: uint8(TypeTimestamp), lo: uint64(ts.Micros), tsU: uint8(TimestampUnitMicros)}, &plan), nil
	case TypeTimestampS:
		ts := api.GetTimestampS(val)
		return attachPlanAlias(Value{kind: uint8(TypeTimestampS), lo: uint64(ts.Seconds), tsU: uint8(TimestampUnitSeconds)}, &plan), nil
	case TypeTimestampMS:
		ts := api.GetTimestampMS(val)
		return attachPlanAlias(Value{kind: uint8(TypeTimestampMS), lo: uint64(ts.Millis), tsU: uint8(TimestampUnitMillis)}, &plan), nil
	case TypeTimestampNS:
		ts := api.GetTimestampNS(val)
		return attachPlanAlias(Value{kind: uint8(TypeTimestampNS), lo: uint64(ts.Nanos), tsU: uint8(TimestampUnitNanos)}, &plan), nil
	case TypeTimestampTZ:
		ts := api.GetTimestampTZ(val)
		return attachPlanAlias(Value{kind: uint8(TypeTimestampTZ), lo: uint64(ts.Micros), tsU: uint8(TimestampUnitTZ)}, &plan), nil
	case TypeInterval:
		iv := api.GetInterval(val)
		return attachPlanAlias(IntervalValue(Interval{Months: iv.Months, Days: iv.Days, Micros: iv.Micros}), &plan), nil
	case TypeUUID:
		u := api.GetUUID(val)
		return attachPlanAlias(Value{kind: uint8(TypeUUID), lo: u.Upper, hi: u.Lower}, &plan), nil
	case TypeEnum:
		idx := api.GetEnumValue(val)
		if idx >= uint64(len(plan.lt.EnumValues)) {
			return Value{}, fmt.Errorf("%w: enum index %d out of range", ErrOutOfRange, idx)
		}
		return Value{kind: uint8(TypeEnum), lo: idx, lt: cloneLogicalType(plan.lt)}, nil
	case TypeList:
		n := api.GetListSize(val)
		if n > limits.MaxCollectionLen || n > math.MaxInt {
			return Value{}, fmt.Errorf("%w: list length %d exceeds %d", ErrLimitExceeded, n, limits.MaxCollectionLen)
		}
		elems := make([]Value, int(n))
		for i := uint64(0); i < n; i++ {
			child := api.GetListChild(val, i)
			v, cerr := decodeNativeValue(api, child, depth+1, limits)
			if child != 0 {
				api.DestroyValue(&child)
			}
			if cerr != nil {
				return Value{}, fmt.Errorf("list[%d]: %w", i, cerr)
			}
			elems[i] = v
		}
		return Value{kind: uint8(TypeList), ref: elems, lt: cloneLogicalType(plan.lt)}, nil
	case TypeStruct:
		n := len(plan.lt.Fields)
		fields := make([]StructFieldValue, n)
		for i := 0; i < n; i++ {
			child := api.GetStructChild(val, uint64(i))
			v, cerr := decodeNativeValue(api, child, depth+1, limits)
			if child != 0 {
				api.DestroyValue(&child)
			}
			if cerr != nil {
				return Value{}, fmt.Errorf("struct field %q: %w", plan.lt.Fields[i].Name, cerr)
			}
			fields[i] = StructFieldValue{
				Name:  plan.lt.Fields[i].Name,
				Type:  plan.lt.Fields[i].Type,
				Value: v,
			}
		}
		return Value{kind: uint8(TypeStruct), ref: fields, lt: cloneLogicalType(plan.lt)}, nil
	case TypeMap:
		n := api.GetMapSize(val)
		if n > limits.MaxCollectionLen || n > math.MaxInt {
			return Value{}, fmt.Errorf("%w: map size %d exceeds %d", ErrLimitExceeded, n, limits.MaxCollectionLen)
		}
		entries := make([]MapEntry, int(n))
		for i := uint64(0); i < n; i++ {
			kHandle := api.GetMapKey(val, i)
			kVal, kerr := decodeNativeValue(api, kHandle, depth+1, limits)
			if kHandle != 0 {
				api.DestroyValue(&kHandle)
			}
			if kerr != nil {
				return Value{}, fmt.Errorf("map key[%d]: %w", i, kerr)
			}
			vHandle := api.GetMapValue(val, i)
			vVal, verr := decodeNativeValue(api, vHandle, depth+1, limits)
			if vHandle != 0 {
				api.DestroyValue(&vHandle)
			}
			if verr != nil {
				return Value{}, fmt.Errorf("map value[%d]: %w", i, verr)
			}
			entries[i] = MapEntry{Key: kVal, Value: vVal}
		}
		return Value{kind: uint8(TypeMap), ref: entries, lt: cloneLogicalType(plan.lt)}, nil
	default:
		return Value{}, fmt.Errorf("%w: decoding native value of type %s", ErrUnsupportedType, plan.lt.Kind)
	}
}

// ValueAs extracts a strongly-typed Go value from v without lossy conversion.
// Supported target types T include Value, bool, int8..64, uint8..64, float32,
// float64, string, []byte, Hugeint, Uhugeint, Decimal, UUID, Date, Time,
// TimeNS, TimeTZ, Timestamp, Interval, Enum, Bit, Bignum, Geometry,
// UnionValue, ListValue, ArrayValue, StructValue, and MapValue.
func ValueAs[T any](v Value) (T, error) {
	var zero T
	switch any(zero).(type) {
	case Value:
		return any(v).(T), nil
	case bool:
		x, err := v.Bool()
		return any(x).(T), err
	case int8:
		x, err := v.Int8()
		return any(x).(T), err
	case int16:
		x, err := v.Int16()
		return any(x).(T), err
	case int32:
		x, err := v.Int32()
		return any(x).(T), err
	case int64:
		x, err := v.Int64()
		return any(x).(T), err
	case uint8:
		x, err := v.Uint8()
		return any(x).(T), err
	case uint16:
		x, err := v.Uint16()
		return any(x).(T), err
	case uint32:
		x, err := v.Uint32()
		return any(x).(T), err
	case uint64:
		x, err := v.Uint64()
		return any(x).(T), err
	case float32:
		x, err := v.Float32()
		return any(x).(T), err
	case float64:
		x, err := v.Float64()
		return any(x).(T), err
	case string:
		x, err := v.Varchar()
		return any(x).(T), err
	case []byte:
		x, err := v.Blob()
		return any(x).(T), err
	case Hugeint:
		x, err := v.Hugeint()
		return any(x).(T), err
	case Uhugeint:
		x, err := v.Uhugeint()
		return any(x).(T), err
	case Decimal:
		x, err := v.Decimal()
		return any(x).(T), err
	case UUID:
		x, err := v.UUID()
		return any(x).(T), err
	case Date:
		x, err := v.Date()
		return any(x).(T), err
	case Time:
		x, err := v.Time()
		return any(x).(T), err
	case TimeNS:
		x, err := v.TimeNS()
		return any(x).(T), err
	case TimeTZ:
		x, err := v.TimeTZ()
		return any(x).(T), err
	case Timestamp:
		x, err := v.Timestamp()
		return any(x).(T), err
	case Interval:
		x, err := v.Interval()
		return any(x).(T), err
	case Enum:
		x, err := v.Enum()
		return any(x).(T), err
	case Bit:
		x, err := v.Bit()
		return any(x).(T), err
	case Bignum:
		x, err := v.Bignum()
		return any(x).(T), err
	case Geometry:
		x, err := v.Geometry()
		return any(x).(T), err
	case UnionValue:
		x, err := v.Union()
		return any(x).(T), err
	case Variant:
		x, err := v.Variant()
		return any(x).(T), err
	case ListValue:
		x, err := v.List()
		return any(x).(T), err
	case ArrayValue:
		x, err := v.Array()
		return any(x).(T), err
	case StructValue:
		x, err := v.Struct()
		return any(x).(T), err
	case MapValue:
		x, err := v.Map()
		return any(x).(T), err
	default:
		return zero, fmt.Errorf("%w: unsupported target type %T", ErrUnsupportedType, zero)
	}
}

// ConfigOptionAs retrieves the runtime configuration option named name from c
// and extracts its strongly-typed Go value via ValueAs[T]. When the option does
// not exist, ok is false and err is nil.
func ConfigOptionAs[T any](c *Conn, name string) (T, ConfigOptionScope, bool, error) {
	var zero T
	opt, ok, err := c.ConfigOption(name)
	if err != nil || !ok {
		return zero, opt.Scope, ok, err
	}
	v, err := ValueAs[T](opt.Value)
	if err != nil {
		return zero, opt.Scope, true, fmt.Errorf("config option %q: %w", name, err)
	}
	return v, opt.Scope, true, nil
}
