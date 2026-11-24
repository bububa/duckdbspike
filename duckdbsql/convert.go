package duckdbsql

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"time"

	"github.com/bububa/duckdbspike/duckdb"
)

func toDuckDBValue(v any) (duckdb.Value, error) {
	if v == nil {
		return duckdb.NullValue(), nil
	}
	switch x := v.(type) {
	case duckdb.Value:
		if x.IsDefault() {
			return duckdb.Value{}, fmt.Errorf("%w: DefaultValue cannot be bound to SQL parameters", duckdb.ErrInvalidInput)
		}
		return x, nil
	case bool:
		return duckdb.BoolValue(x), nil
	case int:
		return duckdb.Int64Value(int64(x)), nil
	case int8:
		return duckdb.Int8Value(x), nil
	case int16:
		return duckdb.Int16Value(x), nil
	case int32:
		return duckdb.Int32Value(x), nil
	case int64:
		return duckdb.Int64Value(x), nil
	case uint:
		return duckdb.Uint64Value(uint64(x)), nil
	case uint8:
		return duckdb.Uint8Value(x), nil
	case uint16:
		return duckdb.Uint16Value(x), nil
	case uint32:
		return duckdb.Uint32Value(x), nil
	case uint64:
		return duckdb.Uint64Value(x), nil
	case float32:
		return duckdb.Float32Value(x), nil
	case float64:
		return duckdb.Float64Value(x), nil
	case string:
		return duckdb.VarcharValue(x), nil
	case []byte:
		return duckdb.BlobValue(x), nil
	case duckdb.Hugeint:
		return duckdb.HugeintValue(x), nil
	case duckdb.Uhugeint:
		return duckdb.UhugeintValue(x), nil
	case duckdb.Decimal:
		return duckdb.DecimalValue(x)
	case duckdb.UUID:
		return duckdb.UUIDValue(x), nil
	case duckdb.Date:
		return duckdb.DateValue(x), nil
	case duckdb.Time:
		return duckdb.TimeValue(x), nil
	case duckdb.TimeNS:
		return duckdb.TimeNSValue(x), nil
	case duckdb.TimeTZ:
		return duckdb.TimeTZValue(x)
	case duckdb.Timestamp:
		return duckdb.TimestampValue(x)
	case duckdb.Interval:
		return duckdb.IntervalValue(x), nil
	case duckdb.Bit:
		return duckdb.BitValue(x)
	case duckdb.Bignum:
		return duckdb.BignumValue(x), nil
	case duckdb.ListValue:
		return duckdb.NewListValue(x.ElemType, x.Elements)
	case duckdb.ArrayValue:
		return duckdb.NewArrayValue(x.ElemType, x.Elements)
	case duckdb.StructValue:
		return duckdb.NewStructValue(x.Fields)
	case duckdb.MapValue:
		return duckdb.NewMapValue(x.KeyType, x.ValueType, x.Entries)
	case time.Time:
		return timeToDuckDBValue(x)
	case driver.Valuer:
		dv, err := x.Value()
		if err != nil {
			return duckdb.Value{}, err
		}
		if dv == nil {
			return duckdb.NullValue(), nil
		}
		if reflect.TypeOf(dv) == reflect.TypeOf(v) {
			return duckdb.Value{}, fmt.Errorf("%w: unsupported parameter type %T", duckdb.ErrUnsupportedType, v)
		}
		return toDuckDBValue(dv)
	default:
		return duckdb.Value{}, fmt.Errorf("%w: unsupported parameter type %T", duckdb.ErrUnsupportedType, v)
	}
}

func timeToDuckDBValue(t time.Time) (duckdb.Value, error) {
	u := t.UTC()
	if u.Nanosecond()%1000 != 0 {
		return duckdb.Value{}, fmt.Errorf("%w: time.Time %s has sub-microsecond precision; truncate to microseconds or pass duckdb.Timestamp{Unit: duckdb.TimestampUnitNanos}", duckdb.ErrInvalidInput, u.Format(time.RFC3339Nano))
	}
	sec := u.Unix()
	const maxMicrosSec = (1<<63 - 1) / 1_000_000
	const minMicrosSec = -(1 << 63) / 1_000_000
	if sec > maxMicrosSec || sec < minMicrosSec {
		return duckdb.Value{}, fmt.Errorf("%w: time.Time %s overflows TIMESTAMPTZ microsecond range", duckdb.ErrOutOfRange, u.Format(time.RFC3339Nano))
	}
	micros := sec*1_000_000 + int64(u.Nanosecond()/1000)
	return duckdb.TimestampValue(duckdb.Timestamp{Value: micros, Unit: duckdb.TimestampUnitTZ})
}

func fromDuckDBValue(v duckdb.Value, scanTime bool) (driver.Value, error) {
	if v.IsNull() {
		return nil, nil
	}
	switch v.Kind() {
	case duckdb.TypeBoolean:
		return v.Bool()
	case duckdb.TypeTinyint:
		return v.Int8()
	case duckdb.TypeSmallint:
		return v.Int16()
	case duckdb.TypeInteger:
		return v.Int32()
	case duckdb.TypeBigint:
		return v.Int64()
	case duckdb.TypeUTinyint:
		return v.Uint8()
	case duckdb.TypeUSmallint:
		return v.Uint16()
	case duckdb.TypeUInteger:
		return v.Uint32()
	case duckdb.TypeUBigint:
		return v.Uint64()
	case duckdb.TypeHugeint:
		return v.Hugeint()
	case duckdb.TypeUhugeint:
		return v.Uhugeint()
	case duckdb.TypeDecimal:
		return v.Decimal()
	case duckdb.TypeFloat:
		return v.Float32()
	case duckdb.TypeDouble:
		return v.Float64()
	case duckdb.TypeVarchar:
		return v.Varchar()
	case duckdb.TypeBlob:
		return v.Blob()
	case duckdb.TypeDate:
		d, err := v.Date()
		if err != nil {
			return nil, err
		}
		if scanTime {
			if !d.IsFinite() {
				return nil, fmt.Errorf("%w: non-finite DATE (%d) cannot be scanned into time.Time", duckdb.ErrOutOfRange, d.Days)
			}
			return time.Unix(int64(d.Days)*86400, 0).UTC(), nil
		}
		return d, nil
	case duckdb.TypeTime:
		return v.Time()
	case duckdb.TypeTimeNS:
		return v.TimeNS()
	case duckdb.TypeTimeTZ:
		return v.TimeTZ()
	case duckdb.TypeTimestamp, duckdb.TypeTimestampS, duckdb.TypeTimestampMS, duckdb.TypeTimestampNS, duckdb.TypeTimestampTZ:
		ts, err := v.Timestamp()
		if err != nil {
			return nil, err
		}
		if scanTime {
			return timestampToTime(ts)
		}
		return ts, nil
	case duckdb.TypeInterval:
		return v.Interval()
	case duckdb.TypeUUID:
		return v.UUID()
	case duckdb.TypeEnum:
		return v.Enum()
	case duckdb.TypeBit:
		return v.Bit()
	case duckdb.TypeBignum:
		return v.Bignum()
	case duckdb.TypeGeometry:
		return v.Geometry()
	case duckdb.TypeList:
		return v.List()
	case duckdb.TypeArray:
		return v.Array()
	case duckdb.TypeStruct:
		return v.Struct()
	case duckdb.TypeMap:
		return v.Map()
	case duckdb.TypeUnion:
		return v.Union()
	default:
		return nil, fmt.Errorf("%w: %s", duckdb.ErrUnsupportedType, v.Kind())
	}
}

func timestampToTime(ts duckdb.Timestamp) (time.Time, error) {
	if !ts.IsFinite() {
		return time.Time{}, fmt.Errorf("%w: non-finite TIMESTAMP (%d) cannot be scanned into time.Time", duckdb.ErrOutOfRange, ts.Value)
	}
	switch ts.Unit {
	case duckdb.TimestampUnitSeconds:
		return time.Unix(ts.Value, 0).UTC(), nil
	case duckdb.TimestampUnitMillis:
		return time.UnixMilli(ts.Value).UTC(), nil
	case duckdb.TimestampUnitMicros, duckdb.TimestampUnitTZ:
		return time.UnixMicro(ts.Value).UTC(), nil
	case duckdb.TimestampUnitNanos:
		return time.Unix(0, ts.Value).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("%w: unknown timestamp unit %d", duckdb.ErrInvalidInput, ts.Unit)
	}
}

func scanTypeForLogicalType(lt duckdb.LogicalType, scanTime bool) reflect.Type {
	switch lt.Kind {
	case duckdb.TypeBoolean:
		return reflect.TypeFor[bool]()
	case duckdb.TypeTinyint:
		return reflect.TypeFor[int8]()
	case duckdb.TypeSmallint:
		return reflect.TypeFor[int16]()
	case duckdb.TypeInteger:
		return reflect.TypeFor[int32]()
	case duckdb.TypeBigint:
		return reflect.TypeFor[int64]()
	case duckdb.TypeUTinyint:
		return reflect.TypeFor[uint8]()
	case duckdb.TypeUSmallint:
		return reflect.TypeFor[uint16]()
	case duckdb.TypeUInteger:
		return reflect.TypeFor[uint32]()
	case duckdb.TypeUBigint:
		return reflect.TypeFor[uint64]()
	case duckdb.TypeHugeint:
		return reflect.TypeFor[duckdb.Hugeint]()
	case duckdb.TypeUhugeint:
		return reflect.TypeFor[duckdb.Uhugeint]()
	case duckdb.TypeDecimal:
		return reflect.TypeFor[duckdb.Decimal]()
	case duckdb.TypeFloat:
		return reflect.TypeFor[float32]()
	case duckdb.TypeDouble:
		return reflect.TypeFor[float64]()
	case duckdb.TypeVarchar:
		return reflect.TypeFor[string]()
	case duckdb.TypeBlob:
		return reflect.TypeFor[[]byte]()
	case duckdb.TypeDate:
		if scanTime {
			return reflect.TypeFor[time.Time]()
		}
		return reflect.TypeFor[duckdb.Date]()
	case duckdb.TypeTime:
		return reflect.TypeFor[duckdb.Time]()
	case duckdb.TypeTimeNS:
		return reflect.TypeFor[duckdb.TimeNS]()
	case duckdb.TypeTimeTZ:
		return reflect.TypeFor[duckdb.TimeTZ]()
	case duckdb.TypeTimestamp, duckdb.TypeTimestampS, duckdb.TypeTimestampMS, duckdb.TypeTimestampNS, duckdb.TypeTimestampTZ:
		if scanTime {
			return reflect.TypeFor[time.Time]()
		}
		return reflect.TypeFor[duckdb.Timestamp]()
	case duckdb.TypeInterval:
		return reflect.TypeFor[duckdb.Interval]()
	case duckdb.TypeUUID:
		return reflect.TypeFor[duckdb.UUID]()
	case duckdb.TypeEnum:
		return reflect.TypeFor[duckdb.Enum]()
	case duckdb.TypeBit:
		return reflect.TypeFor[duckdb.Bit]()
	case duckdb.TypeBignum:
		return reflect.TypeFor[duckdb.Bignum]()
	case duckdb.TypeGeometry:
		return reflect.TypeFor[duckdb.Geometry]()
	case duckdb.TypeList:
		return reflect.TypeFor[duckdb.ListValue]()
	case duckdb.TypeArray:
		return reflect.TypeFor[duckdb.ArrayValue]()
	case duckdb.TypeStruct:
		return reflect.TypeFor[duckdb.StructValue]()
	case duckdb.TypeMap:
		return reflect.TypeFor[duckdb.MapValue]()
	case duckdb.TypeUnion:
		return reflect.TypeFor[duckdb.UnionValue]()
	default:
		return reflect.TypeFor[any]()
	}
}

func convertArgs(args []driver.NamedValue) (positional []duckdb.Value, named []duckdb.NamedArg, isNamed bool, err error) {
	if len(args) == 0 {
		return nil, nil, false, nil
	}
	hasNamed := false
	hasPos := false
	for _, a := range args {
		if a.Name != "" {
			hasNamed = true
		} else {
			hasPos = true
		}
	}
	if hasNamed && hasPos {
		return nil, nil, false, fmt.Errorf("%w: cannot mix positional and named parameters", duckdb.ErrInvalidInput)
	}
	if hasNamed {
		named = make([]duckdb.NamedArg, len(args))
		for i, a := range args {
			v, err := toDuckDBValue(a.Value)
			if err != nil {
				return nil, nil, false, fmt.Errorf("parameter %q: %w", a.Name, err)
			}
			named[i] = duckdb.Named(a.Name, v)
		}
		return nil, named, true, nil
	}
	positional = make([]duckdb.Value, len(args))
	for i, a := range args {
		v, err := toDuckDBValue(a.Value)
		if err != nil {
			return nil, nil, false, fmt.Errorf("parameter %d: %w", a.Ordinal, err)
		}
		positional[i] = v
	}
	return positional, nil, false, nil
}
