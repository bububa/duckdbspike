package duckdb

import (
	"fmt"
	"math"
	"runtime"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

// NamedArg pairs a DuckDB named parameter ($name) with its typed Value.
type NamedArg struct {
	Name  string
	Value Value
}

// Named constructs a NamedArg for use with ExecNamed and QueryNamed.
func Named(name string, value Value) NamedArg {
	return NamedArg{Name: name, Value: value}
}

// emptyBindByte and emptyBindHandle provide non-nil pointers when binding
// zero-length slices/collections so native C APIs that reject NULL pointers
// even for count == 0 succeed safely.
var (
	emptyBindByte   byte
	emptyBindHandle uintptr
)

func (s *Stmt) bindLocked(args []Value) error {
	api := s.conn.api
	expected := api.NParams(s.handle)
	if uint64(len(args)) != expected {
		return fmt.Errorf("%w: statement expects %d parameters, got %d", ErrInvalidInput, expected, len(args))
	}
	if expected == 0 {
		return nil
	}
	if api.ClearBindings(s.handle) != abi.StateSuccess {
		return nativeError("clear_bindings", abi.ErrorInvalid, abi.GoString(api.PrepareError(s.handle)))
	}
	for i, arg := range args {
		idx := uint64(i + 1)
		if err := s.bindOneLocked(idx, arg); err != nil {
			_ = api.ClearBindings(s.handle)
			return err
		}
	}
	return nil
}

func (s *Stmt) bindNamedLocked(args []NamedArg) error {
	api := s.conn.api
	expected := api.NParams(s.handle)
	if expected == 0 && len(args) == 0 {
		return nil
	}
	seenNames := make(map[string]struct{}, len(args))
	cnames := make([][]byte, len(args))
	for i, arg := range args {
		if arg.Name == "" {
			return fmt.Errorf("%w: empty parameter name", ErrInvalidInput)
		}
		if _, dup := seenNames[arg.Name]; dup {
			return fmt.Errorf("%w: duplicate parameter name %q", ErrInvalidInput, arg.Name)
		}
		seenNames[arg.Name] = struct{}{}
		cname, err := abi.CString(arg.Name)
		if err != nil {
			return fmt.Errorf("%w: parameter name %q: %v", ErrInvalidInput, arg.Name, err)
		}
		cnames[i] = cname
	}
	if api.ClearBindings(s.handle) != abi.StateSuccess {
		return nativeError("clear_bindings", abi.ErrorInvalid, abi.GoString(api.PrepareError(s.handle)))
	}
	seenIdx := make(map[uint64]struct{}, len(args))
	for i, arg := range args {
		var idx uint64
		state := api.BindParameterIndex(s.handle, &idx, &cnames[i][0])
		runtime.KeepAlive(cnames[i])
		if state != abi.StateSuccess || idx == 0 {
			_ = api.ClearBindings(s.handle)
			return fmt.Errorf("%w: unknown parameter %q", ErrInvalidInput, arg.Name)
		}
		if _, dup := seenIdx[idx]; dup {
			_ = api.ClearBindings(s.handle)
			return fmt.Errorf("%w: duplicate binding for parameter index %d (%q)", ErrInvalidInput, idx, arg.Name)
		}
		seenIdx[idx] = struct{}{}
		if err := s.bindOneLocked(idx, arg.Value); err != nil {
			_ = api.ClearBindings(s.handle)
			return err
		}
	}
	if uint64(len(seenIdx)) != expected {
		_ = api.ClearBindings(s.handle)
		return fmt.Errorf("%w: statement expects %d parameters, bound %d", ErrInvalidInput, expected, len(seenIdx))
	}
	return nil
}

func (s *Stmt) bindOneLocked(idx uint64, arg Value) error {
	if arg.IsDefault() {
		return fmt.Errorf("%w: DefaultValue is only valid in Appender.AppendRow", ErrInvalidInput)
	}
	api := s.conn.api
	var state int32
	if arg.IsNull() {
		state = api.BindNull(s.handle, idx)
	} else {
		switch arg.Kind() {
		case TypeBoolean:
			state = api.BindBoolean(s.handle, idx, arg.lo != 0)
		case TypeTinyint:
			state = api.BindInt8(s.handle, idx, int8(int64(arg.lo)))
		case TypeSmallint:
			state = api.BindInt16(s.handle, idx, int16(int64(arg.lo)))
		case TypeInteger:
			state = api.BindInt32(s.handle, idx, int32(int64(arg.lo)))
		case TypeBigint:
			state = api.BindInt64(s.handle, idx, int64(arg.lo))
		case TypeUTinyint:
			state = api.BindUint8(s.handle, idx, uint8(arg.lo))
		case TypeUSmallint:
			state = api.BindUint16(s.handle, idx, uint16(arg.lo))
		case TypeUInteger:
			state = api.BindUint32(s.handle, idx, uint32(arg.lo))
		case TypeUBigint:
			state = api.BindUint64(s.handle, idx, arg.lo)
		case TypeHugeint:
			state = api.BindHugeint(s.handle, idx, abi.Hugeint{Lower: arg.lo, Upper: int64(arg.hi)})
		case TypeUhugeint:
			state = api.BindUhugeint(s.handle, idx, abi.Uhugeint{Lower: arg.lo, Upper: arg.hi})
		case TypeDecimal:
			dec := Decimal{Width: arg.decW, Scale: arg.decS, Value: Hugeint{Lower: arg.lo, Upper: int64(arg.hi)}}
			if err := dec.Validate(); err != nil {
				return err
			}
			state = api.BindDecimal(s.handle, idx, abi.Decimal{
				Width: dec.Width,
				Scale: dec.Scale,
				Value: abi.Hugeint{Lower: dec.Value.Lower, Upper: dec.Value.Upper},
			})
		case TypeFloat:
			state = api.BindFloat(s.handle, idx, math.Float32frombits(uint32(arg.lo)))
		case TypeDouble:
			state = api.BindDouble(s.handle, idx, math.Float64frombits(arg.lo))
		case TypeVarchar:
			str, _ := arg.ref.(string)
			if len(str) == 0 {
				state = api.BindVarcharLength(s.handle, idx, &emptyBindByte, 0)
			} else {
				b := []byte(str)
				state = api.BindVarcharLength(s.handle, idx, &b[0], uint64(len(b)))
				runtime.KeepAlive(b)
			}
		case TypeBlob:
			blob, _ := arg.ref.([]byte)
			if len(blob) == 0 {
				state = api.BindBlob(s.handle, idx, &emptyBindByte, 0)
			} else {
				state = api.BindBlob(s.handle, idx, &blob[0], uint64(len(blob)))
				runtime.KeepAlive(blob)
			}
		case TypeDate:
			state = api.BindDate(s.handle, idx, abi.Date{Days: int32(int64(arg.lo))})
		case TypeTime:
			state = api.BindTime(s.handle, idx, abi.Time{Micros: int64(arg.lo)})
		case TypeTimestamp:
			state = api.BindTimestamp(s.handle, idx, abi.Timestamp{Micros: int64(arg.lo)})
		case TypeTimestampTZ:
			state = api.BindTimestampTZ(s.handle, idx, abi.Timestamp{Micros: int64(arg.lo)})
		case TypeInterval:
			state = api.BindInterval(s.handle, idx, abi.Interval{
				Months: int32(uint32(arg.hi >> 32)),
				Days:   int32(uint32(arg.hi)),
				Micros: int64(arg.lo),
			})
		case TypeTimestampS, TypeTimestampMS, TypeTimestampNS, TypeTimeNS,
			TypeTimeTZ, TypeUUID, TypeEnum, TypeBit, TypeBignum,
			TypeList, TypeArray, TypeStruct, TypeMap, TypeUnion:
			nv, err := createNativeValue(api, arg, 0, s.conn.limits)
			if err != nil {
				return err
			}
			state = api.BindValue(s.handle, idx, nv)
			api.DestroyValue(&nv)
		default:
			return fmt.Errorf("%w: parameter %d has unsupported type %s", ErrUnsupportedType, idx, arg.Kind())
		}
	}
	if state != abi.StateSuccess {
		msg := abi.GoString(api.PrepareError(s.handle))
		return nativeError(fmt.Sprintf("bind[%d]", idx), abi.ErrorInvalid, msg)
	}
	return nil
}

func abiTypeIDForScalar(k TypeKind) (int32, bool) {
	switch k {
	case TypeBoolean:
		return abi.TypeBoolean, true
	case TypeTinyint:
		return abi.TypeTinyint, true
	case TypeSmallint:
		return abi.TypeSmallint, true
	case TypeInteger:
		return abi.TypeInteger, true
	case TypeBigint:
		return abi.TypeBigint, true
	case TypeUTinyint:
		return abi.TypeUTinyint, true
	case TypeUSmallint:
		return abi.TypeUSmallint, true
	case TypeUInteger:
		return abi.TypeUInteger, true
	case TypeUBigint:
		return abi.TypeUBigint, true
	case TypeHugeint:
		return abi.TypeHugeint, true
	case TypeUhugeint:
		return abi.TypeUHugeint, true
	case TypeFloat:
		return abi.TypeFloat, true
	case TypeDouble:
		return abi.TypeDouble, true
	case TypeVarchar:
		return abi.TypeVarchar, true
	case TypeBlob:
		return abi.TypeBlob, true
	case TypeDate:
		return abi.TypeDate, true
	case TypeTime:
		return abi.TypeTime, true
	case TypeTimeNS:
		return abi.TypeTimeNS, true
	case TypeTimeTZ:
		return abi.TypeTimeTZ, true
	case TypeTimestamp:
		return abi.TypeTimestamp, true
	case TypeTimestampS:
		return abi.TypeTimestampS, true
	case TypeTimestampMS:
		return abi.TypeTimestampMS, true
	case TypeTimestampNS:
		return abi.TypeTimestampNS, true
	case TypeTimestampTZ:
		return abi.TypeTimestampTZ, true
	case TypeInterval:
		return abi.TypeInterval, true
	case TypeUUID:
		return abi.TypeUUID, true
	case TypeSQLNull:
		return abi.TypeSQLNull, true
	case TypeBit:
		return abi.TypeBit, true
	case TypeBignum:
		return abi.TypeBignum, true
	case TypeGeometry:
		return abi.TypeGeometry, true
	default:
		return abi.TypeInvalid, false
	}
}

func applyLogicalTypeAlias(api *abi.API, h uintptr, alias string) error {
	if h == 0 || alias == "" || api.LogicalTypeSetAlias == nil {
		return nil
	}
	calias, err := abi.CString(alias)
	if err != nil {
		return fmt.Errorf("%w: logical type alias %q: %v", ErrInvalidInput, alias, err)
	}
	api.LogicalTypeSetAlias(h, &calias[0])
	runtime.KeepAlive(calias)
	return nil
}

func createNativeLogicalType(api *abi.API, lt LogicalType, depth int, limits DecodeLimits) (uintptr, error) {
	if depth > limits.MaxNestingDepth {
		return 0, fmt.Errorf("%w: bind type nesting depth %d exceeds %d", ErrLimitExceeded, depth, limits.MaxNestingDepth)
	}
	if id, ok := abiTypeIDForScalar(lt.Kind); ok {
		h := api.CreateLogicalType(id)
		if h == 0 {
			return 0, nativeError("create_logical_type", abi.ErrorInvalid, "")
		}
		if err := applyLogicalTypeAlias(api, h, lt.Alias); err != nil {
			api.DestroyLogicalType(&h)
			return 0, err
		}
		return h, nil
	}
	switch lt.Kind {
	case TypeDecimal:
		if lt.DecimalWidth < 1 || lt.DecimalWidth > 38 || lt.DecimalScale > lt.DecimalWidth {
			return 0, fmt.Errorf("%w: invalid DECIMAL(%d,%d)", ErrInvalidInput, lt.DecimalWidth, lt.DecimalScale)
		}
		h := api.CreateDecimalType(lt.DecimalWidth, lt.DecimalScale)
		if h == 0 {
			return 0, nativeError("create_decimal_type", abi.ErrorInvalid, "")
		}
		return h, nil
	case TypeEnum:
		if len(lt.EnumValues) == 0 {
			return 0, fmt.Errorf("%w: ENUM requires at least one dictionary member", ErrInvalidInput)
		}
		if uint64(len(lt.EnumValues)) > limits.MaxCollectionLen {
			return 0, fmt.Errorf("%w: enum dictionary size %d exceeds %d", ErrLimitExceeded, len(lt.EnumValues), limits.MaxCollectionLen)
		}
		var pinner runtime.Pinner
		defer pinner.Unpin()
		cnames := make([][]byte, len(lt.EnumValues))
		ptrs := make([]uintptr, len(lt.EnumValues))
		for i, label := range lt.EnumValues {
			cb, err := abi.CString(label)
			if err != nil {
				return 0, fmt.Errorf("%w: enum label[%d]: %v", ErrInvalidInput, i, err)
			}
			cnames[i] = cb
			pinner.Pin(&cb[0])
			ptrs[i] = uintptr(unsafe.Pointer(&cb[0]))
		}
		h := api.CreateEnumType(&ptrs[0], uint64(len(ptrs)))
		runtime.KeepAlive(cnames)
		runtime.KeepAlive(ptrs)
		if h == 0 {
			return 0, nativeError("create_enum_type", abi.ErrorInvalid, "")
		}
		return h, nil
	case TypeList:
		if lt.Elem == nil {
			return 0, fmt.Errorf("%w: LIST requires Elem logical type", ErrInvalidInput)
		}
		child, err := createNativeLogicalType(api, *lt.Elem, depth+1, limits)
		if err != nil {
			return 0, err
		}
		defer api.DestroyLogicalType(&child)
		h := api.CreateListType(child)
		if h == 0 {
			return 0, nativeError("create_list_type", abi.ErrorInvalid, "")
		}
		return h, nil
	case TypeArray:
		if lt.Elem == nil || lt.ArraySize == 0 {
			return 0, fmt.Errorf("%w: ARRAY requires Elem logical type and positive size", ErrInvalidInput)
		}
		if lt.ArraySize > limits.MaxCollectionLen {
			return 0, fmt.Errorf("%w: array size %d exceeds %d", ErrLimitExceeded, lt.ArraySize, limits.MaxCollectionLen)
		}
		child, err := createNativeLogicalType(api, *lt.Elem, depth+1, limits)
		if err != nil {
			return 0, err
		}
		defer api.DestroyLogicalType(&child)
		h := api.CreateArrayType(child, lt.ArraySize)
		if h == 0 {
			return 0, nativeError("create_array_type", abi.ErrorInvalid, "")
		}
		return h, nil
	case TypeMap:
		if lt.Key == nil || lt.Value == nil {
			return 0, fmt.Errorf("%w: MAP requires Key and Value logical types", ErrInvalidInput)
		}
		keyLT, err := createNativeLogicalType(api, *lt.Key, depth+1, limits)
		if err != nil {
			return 0, err
		}
		defer api.DestroyLogicalType(&keyLT)
		valLT, err := createNativeLogicalType(api, *lt.Value, depth+1, limits)
		if err != nil {
			return 0, err
		}
		defer api.DestroyLogicalType(&valLT)
		h := api.CreateMapType(keyLT, valLT)
		if h == 0 {
			return 0, nativeError("create_map_type", abi.ErrorInvalid, "")
		}
		return h, nil
	case TypeStruct, TypeUnion:
		if len(lt.Fields) == 0 {
			return 0, fmt.Errorf("%w: %s requires at least one member", ErrInvalidInput, lt.Kind)
		}
		if lt.Kind == TypeUnion && len(lt.Fields) > 256 {
			return 0, fmt.Errorf("%w: union member count %d exceeds 256", ErrLimitExceeded, len(lt.Fields))
		}
		if uint64(len(lt.Fields)) > limits.MaxCollectionLen {
			return 0, fmt.Errorf("%w: %s member count %d exceeds %d", ErrLimitExceeded, lt.Kind, len(lt.Fields), limits.MaxCollectionLen)
		}
		var pinner runtime.Pinner
		defer pinner.Unpin()
		memberTypes := make([]uintptr, len(lt.Fields))
		defer func() {
			for i := range memberTypes {
				if memberTypes[i] != 0 {
					api.DestroyLogicalType(&memberTypes[i])
				}
			}
		}()
		cnames := make([][]byte, len(lt.Fields))
		namePtrs := make([]uintptr, len(lt.Fields))
		for i, f := range lt.Fields {
			if f.Name == "" {
				return 0, fmt.Errorf("%w: %s member %d has empty name", ErrInvalidInput, lt.Kind, i)
			}
			cb, err := abi.CString(f.Name)
			if err != nil {
				return 0, fmt.Errorf("%w: %s member %q: %v", ErrInvalidInput, lt.Kind, f.Name, err)
			}
			cnames[i] = cb
			pinner.Pin(&cb[0])
			namePtrs[i] = uintptr(unsafe.Pointer(&cb[0]))
			mt, err := createNativeLogicalType(api, f.Type, depth+1, limits)
			if err != nil {
				return 0, err
			}
			memberTypes[i] = mt
		}
		var h uintptr
		var op string
		if lt.Kind == TypeStruct {
			op = "create_struct_type"
			h = api.CreateStructType(&memberTypes[0], &namePtrs[0], uint64(len(memberTypes)))
		} else {
			op = "create_union_type"
			h = api.CreateUnionType(&memberTypes[0], &namePtrs[0], uint64(len(memberTypes)))
		}
		runtime.KeepAlive(cnames)
		runtime.KeepAlive(namePtrs)
		runtime.KeepAlive(memberTypes)
		if h == 0 {
			return 0, nativeError(op, abi.ErrorInvalid, "")
		}
		return h, nil
	default:
		return 0, fmt.Errorf("%w: cannot build native logical type for %s", ErrUnsupportedType, lt.Kind)
	}
}

func destroyNativeValues(api *abi.API, vals []uintptr) {
	for i := range vals {
		if vals[i] != 0 {
			api.DestroyValue(&vals[i])
		}
	}
}

func createNativeValue(api *abi.API, v Value, depth int, limits DecodeLimits) (uintptr, error) {
	if depth > limits.MaxNestingDepth {
		return 0, fmt.Errorf("%w: bind value nesting depth %d exceeds %d", ErrLimitExceeded, depth, limits.MaxNestingDepth)
	}
	if v.IsNull() {
		nv := api.CreateNullValue()
		if nv == 0 {
			return 0, nativeError("create_null_value", abi.ErrorInvalid, "")
		}
		return nv, nil
	}

	var nv uintptr
	var op string
	switch v.Kind() {
	case TypeBoolean:
		op = "create_bool"
		nv = api.CreateBool(v.lo != 0)
	case TypeTinyint:
		op = "create_int8"
		nv = api.CreateInt8(int8(int64(v.lo)))
	case TypeSmallint:
		op = "create_int16"
		nv = api.CreateInt16(int16(int64(v.lo)))
	case TypeInteger:
		op = "create_int32"
		nv = api.CreateInt32(int32(int64(v.lo)))
	case TypeBigint:
		op = "create_int64"
		nv = api.CreateInt64(int64(v.lo))
	case TypeUTinyint:
		op = "create_uint8"
		nv = api.CreateUint8(uint8(v.lo))
	case TypeUSmallint:
		op = "create_uint16"
		nv = api.CreateUint16(uint16(v.lo))
	case TypeUInteger:
		op = "create_uint32"
		nv = api.CreateUint32(uint32(v.lo))
	case TypeUBigint:
		op = "create_uint64"
		nv = api.CreateUint64(v.lo)
	case TypeHugeint:
		op = "create_hugeint"
		nv = api.CreateHugeint(abi.Hugeint{Lower: v.lo, Upper: int64(v.hi)})
	case TypeUhugeint:
		op = "create_uhugeint"
		nv = api.CreateUhugeint(abi.Uhugeint{Lower: v.lo, Upper: v.hi})
	case TypeDecimal:
		dec := Decimal{Width: v.decW, Scale: v.decS, Value: Hugeint{Lower: v.lo, Upper: int64(v.hi)}}
		if err := dec.Validate(); err != nil {
			return 0, err
		}
		op = "create_decimal"
		nv = api.CreateDecimal(abi.Decimal{
			Width: dec.Width,
			Scale: dec.Scale,
			Value: abi.Hugeint{Lower: dec.Value.Lower, Upper: dec.Value.Upper},
		})
	case TypeFloat:
		op = "create_float"
		nv = api.CreateFloat(math.Float32frombits(uint32(v.lo)))
	case TypeDouble:
		op = "create_double"
		nv = api.CreateDouble(math.Float64frombits(v.lo))
	case TypeVarchar:
		op = "create_varchar_length"
		str, _ := v.ref.(string)
		if len(str) == 0 {
			nv = api.CreateVarcharLength(&emptyBindByte, 0)
		} else {
			b := []byte(str)
			nv = api.CreateVarcharLength(&b[0], uint64(len(b)))
			runtime.KeepAlive(b)
		}
	case TypeBlob:
		op = "create_blob"
		blob, _ := v.ref.([]byte)
		if len(blob) == 0 {
			nv = api.CreateBlob(&emptyBindByte, 0)
		} else {
			nv = api.CreateBlob(&blob[0], uint64(len(blob)))
			runtime.KeepAlive(blob)
		}
	case TypeDate:
		op = "create_date"
		nv = api.CreateDate(abi.Date{Days: int32(int64(v.lo))})
	case TypeTime:
		op = "create_time"
		nv = api.CreateTime(abi.Time{Micros: int64(v.lo)})
	case TypeTimeNS:
		op = "create_time_ns"
		nv = api.CreateTimeNS(abi.TimeNS{Nanos: int64(v.lo)})
	case TypeTimeTZ:
		tz := TimeTZ{Micros: int64(v.lo), OffsetSeconds: int32(int64(v.hi))}
		if err := tz.Validate(); err != nil {
			return 0, err
		}
		op = "create_time_tz_value"
		packed := api.CreateTimeTZ(tz.Micros, tz.OffsetSeconds)
		nv = api.CreateTimeTZValue(packed)
	case TypeTimestamp:
		op = "create_timestamp"
		nv = api.CreateTimestamp(abi.Timestamp{Micros: int64(v.lo)})
	case TypeTimestampS:
		op = "create_timestamp_s"
		nv = api.CreateTimestampS(abi.TimestampS{Seconds: int64(v.lo)})
	case TypeTimestampMS:
		op = "create_timestamp_ms"
		nv = api.CreateTimestampMS(abi.TimestampMS{Millis: int64(v.lo)})
	case TypeTimestampNS:
		op = "create_timestamp_ns"
		nv = api.CreateTimestampNS(abi.TimestampNS{Nanos: int64(v.lo)})
	case TypeTimestampTZ:
		op = "create_timestamp_tz"
		nv = api.CreateTimestampTZ(abi.Timestamp{Micros: int64(v.lo)})
	case TypeInterval:
		op = "create_interval"
		nv = api.CreateInterval(abi.Interval{
			Months: int32(uint32(v.hi >> 32)),
			Days:   int32(uint32(v.hi)),
			Micros: int64(v.lo),
		})
	case TypeUUID:
		op = "create_uuid"
		// Value stores UUID bytes [0:8] (high 64 bits) in v.lo and [8:16] (low 64 bits) in v.hi.
		nv = api.CreateUUID(abi.Uhugeint{Lower: v.hi, Upper: v.lo})
	case TypeBit:
		raw, _ := v.ref.([]byte)
		if err := (Bit{Data: raw}).Validate(); err != nil {
			return 0, err
		}
		var pinner runtime.Pinner
		pinner.Pin(&raw[0])
		op = "create_bit"
		nv = api.CreateBit(abi.Bit{
			Data: uintptr(unsafe.Pointer(&raw[0])),
			Size: uint64(len(raw)),
		})
		pinner.Unpin()
		runtime.KeepAlive(raw)
	case TypeBignum:
		mag, _ := v.ref.([]byte)
		isNeg := v.lo != 0 && len(mag) > 0
		if len(mag) == 0 {
			mag = []byte{0}
			isNeg = false
		}
		var pinner runtime.Pinner
		pinner.Pin(&mag[0])
		op = "create_bignum"
		nv = api.CreateBignum(abi.Bignum{
			Data:       uintptr(unsafe.Pointer(&mag[0])),
			Size:       uint64(len(mag)),
			IsNegative: isNeg,
		})
		pinner.Unpin()
		runtime.KeepAlive(mag)
	case TypeUnion:
		if v.lt == nil || v.lt.Kind != TypeUnion || int(v.lo) >= len(v.lt.Fields) {
			return 0, fmt.Errorf("%w: invalid UNION value", ErrInvalidInput)
		}
		unionLT, err := createNativeLogicalType(api, *v.lt, depth, limits)
		if err != nil {
			return 0, err
		}
		defer api.DestroyLogicalType(&unionLT)
		inner, _ := v.ref.(Value)
		memberVal, err := createNativeValue(api, inner, depth+1, limits)
		if err != nil {
			return 0, err
		}
		defer api.DestroyValue(&memberVal)
		op = "create_union_value"
		nv = api.CreateUnionValue(unionLT, v.lo, memberVal)
	case TypeEnum:
		if v.lt == nil || v.lt.Kind != TypeEnum || uint64(v.lo) >= uint64(len(v.lt.EnumValues)) {
			return 0, fmt.Errorf("%w: invalid ENUM value", ErrInvalidInput)
		}
		enumLT, err := createNativeLogicalType(api, *v.lt, depth, limits)
		if err != nil {
			return 0, err
		}
		defer api.DestroyLogicalType(&enumLT)
		op = "create_enum_value"
		nv = api.CreateEnumValue(enumLT, v.lo)
	case TypeList:
		if v.lt == nil || v.lt.Elem == nil {
			return 0, fmt.Errorf("%w: LIST value missing element type", ErrInvalidInput)
		}
		elems, _ := v.ref.([]Value)
		if uint64(len(elems)) > limits.MaxCollectionLen {
			return 0, fmt.Errorf("%w: list length %d exceeds %d", ErrLimitExceeded, len(elems), limits.MaxCollectionLen)
		}
		elemLT, err := createNativeLogicalType(api, *v.lt.Elem, depth+1, limits)
		if err != nil {
			return 0, err
		}
		defer api.DestroyLogicalType(&elemLT)
		childVals := make([]uintptr, len(elems))
		defer destroyNativeValues(api, childVals)
		for i, elem := range elems {
			cv, err := createNativeValue(api, elem, depth+1, limits)
			if err != nil {
				return 0, err
			}
			childVals[i] = cv
		}
		ptr := &emptyBindHandle
		if len(childVals) > 0 {
			ptr = &childVals[0]
		}
		op = "create_list_value"
		nv = api.CreateListValue(elemLT, ptr, uint64(len(childVals)))
		runtime.KeepAlive(childVals)
	case TypeArray:
		if v.lt == nil || v.lt.Elem == nil {
			return 0, fmt.Errorf("%w: ARRAY value missing element type", ErrInvalidInput)
		}
		elems, _ := v.ref.([]Value)
		if len(elems) == 0 || uint64(len(elems)) != v.lt.ArraySize {
			return 0, fmt.Errorf("%w: array element count %d does not match size %d", ErrInvalidInput, len(elems), v.lt.ArraySize)
		}
		if uint64(len(elems)) > limits.MaxCollectionLen {
			return 0, fmt.Errorf("%w: array length %d exceeds %d", ErrLimitExceeded, len(elems), limits.MaxCollectionLen)
		}
		elemLT, err := createNativeLogicalType(api, *v.lt.Elem, depth+1, limits)
		if err != nil {
			return 0, err
		}
		defer api.DestroyLogicalType(&elemLT)
		childVals := make([]uintptr, len(elems))
		defer destroyNativeValues(api, childVals)
		for i, elem := range elems {
			cv, err := createNativeValue(api, elem, depth+1, limits)
			if err != nil {
				return 0, err
			}
			childVals[i] = cv
		}
		op = "create_array_value"
		nv = api.CreateArrayValue(elemLT, &childVals[0], uint64(len(childVals)))
		runtime.KeepAlive(childVals)
	case TypeStruct:
		if v.lt == nil || len(v.lt.Fields) == 0 {
			return 0, fmt.Errorf("%w: STRUCT value missing field types", ErrInvalidInput)
		}
		fields, _ := v.ref.([]StructFieldValue)
		if len(fields) != len(v.lt.Fields) {
			return 0, fmt.Errorf("%w: struct field count %d does not match type %d", ErrInvalidInput, len(fields), len(v.lt.Fields))
		}
		structLT, err := createNativeLogicalType(api, *v.lt, depth, limits)
		if err != nil {
			return 0, err
		}
		defer api.DestroyLogicalType(&structLT)
		childVals := make([]uintptr, len(fields))
		defer destroyNativeValues(api, childVals)
		for i, f := range fields {
			cv, err := createNativeValue(api, f.Value, depth+1, limits)
			if err != nil {
				return 0, err
			}
			childVals[i] = cv
		}
		op = "create_struct_value"
		nv = api.CreateStructValue(structLT, &childVals[0])
		runtime.KeepAlive(childVals)
	case TypeMap:
		if v.lt == nil || v.lt.Key == nil || v.lt.Value == nil {
			return 0, fmt.Errorf("%w: MAP value missing key/value types", ErrInvalidInput)
		}
		entries, _ := v.ref.([]MapEntry)
		if uint64(len(entries)) > limits.MaxCollectionLen {
			return 0, fmt.Errorf("%w: map length %d exceeds %d", ErrLimitExceeded, len(entries), limits.MaxCollectionLen)
		}
		mapLT, err := createNativeLogicalType(api, *v.lt, depth, limits)
		if err != nil {
			return 0, err
		}
		defer api.DestroyLogicalType(&mapLT)
		keys := make([]uintptr, len(entries))
		vals := make([]uintptr, len(entries))
		defer destroyNativeValues(api, keys)
		defer destroyNativeValues(api, vals)
		for i, e := range entries {
			if e.Key.IsNull() {
				return 0, fmt.Errorf("%w: map[%d] key cannot be SQL NULL", ErrInvalidInput, i)
			}
			kv, err := createNativeValue(api, e.Key, depth+1, limits)
			if err != nil {
				return 0, err
			}
			keys[i] = kv
			vv, err := createNativeValue(api, e.Value, depth+1, limits)
			if err != nil {
				return 0, err
			}
			vals[i] = vv
		}
		kPtr, vPtr := &emptyBindHandle, &emptyBindHandle
		if len(entries) > 0 {
			kPtr = &keys[0]
			vPtr = &vals[0]
		}
		op = "create_map_value"
		nv = api.CreateMapValue(mapLT, kPtr, vPtr, uint64(len(entries)))
		runtime.KeepAlive(keys)
		runtime.KeepAlive(vals)
	default:
		return 0, fmt.Errorf("%w: unsupported value type %s", ErrUnsupportedType, v.Kind())
	}

	if nv == 0 {
		return 0, nativeError(op, abi.ErrorInvalid, "")
	}
	return nv, nil
}
