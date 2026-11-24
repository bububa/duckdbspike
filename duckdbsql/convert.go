package duckdbsql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/bububa/duckdbspike/duckdb"
)

func toDuckDBValue(v any) (duckdb.Value, error) {
	return toDuckDBValueDepth(v, 0)
}

func toDuckDBValueDepth(v any, depth int) (duckdb.Value, error) {
	if depth > 64 {
		return duckdb.Value{}, fmt.Errorf("%w: parameter nesting depth exceeds limit", duckdb.ErrLimitExceeded)
	}
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
		if x == nil {
			return duckdb.NullValue(), nil
		}
		return duckdb.BlobValue(x), nil
	case *big.Int:
		if x == nil {
			return duckdb.NullValue(), nil
		}
		bn, err := duckdb.BignumFromBigInt(x)
		if err != nil {
			return duckdb.Value{}, err
		}
		return duckdb.BignumValue(bn), nil
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
	case duckdb.Enum:
		if x.Label == "" {
			return duckdb.Value{}, fmt.Errorf("%w: duckdb.Enum requires a non-empty Label or explicit duckdb.EnumValue", duckdb.ErrInvalidInput)
		}
		return duckdb.VarcharValue(x.Label), nil
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
	case duckdb.UnionValue:
		if x.Type.Kind != duckdb.TypeUnion {
			return duckdb.Value{}, fmt.Errorf("%w: duckdb.UnionValue requires Type (UnionType) or explicit duckdb.NewUnionValue", duckdb.ErrInvalidInput)
		}
		return duckdb.NewUnionValue(x.Type, x.Tag, x.Value)
	case time.Time:
		return timeToDuckDBValue(x)
	}

	// Unwrap nil pointers before checking driver.Valuer so nil *T maps to SQL NULL.
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return duckdb.NullValue(), nil
		}
		if valuer, ok := v.(driver.Valuer); ok {
			return valuerToDuckDBValue(valuer, v, depth)
		}
		return toDuckDBValueDepth(rv.Elem().Interface(), depth+1)
	}

	if valuer, ok := v.(driver.Valuer); ok {
		return valuerToDuckDBValue(valuer, v, depth)
	}

	// Native Go slices (excluding []byte handled above) bind as DuckDB LIST.
	if rv.Kind() == reflect.Slice {
		if rv.IsNil() {
			return duckdb.NullValue(), nil
		}
		return sliceToDuckDBList(rv, depth)
	}

	// Native Go fixed-size arrays (excluding duckdb.UUID handled above) bind as DuckDB ARRAY[N].
	if rv.Kind() == reflect.Array {
		return arrayToDuckDBArray(rv, depth)
	}

	// Native Go maps bind as DuckDB MAP(K, V).
	if rv.Kind() == reflect.Map {
		if rv.IsNil() {
			return duckdb.NullValue(), nil
		}
		return mapToDuckDBMap(rv, depth)
	}

	return duckdb.Value{}, fmt.Errorf("%w: unsupported parameter type %T", duckdb.ErrUnsupportedType, v)
}

func valuerToDuckDBValue(valuer driver.Valuer, orig any, depth int) (duckdb.Value, error) {
	dv, err := valuer.Value()
	if err != nil {
		return duckdb.Value{}, err
	}
	if dv == nil {
		return duckdb.NullValue(), nil
	}
	if reflect.TypeOf(dv) == reflect.TypeOf(orig) {
		return duckdb.Value{}, fmt.Errorf("%w: unsupported parameter type %T", duckdb.ErrUnsupportedType, orig)
	}
	return toDuckDBValueDepth(dv, depth+1)
}

func sliceToDuckDBList(rv reflect.Value, depth int) (duckdb.Value, error) {
	n := rv.Len()
	elemGoType := rv.Type().Elem()
	elems := make([]duckdb.Value, n)
	var inferred duckdb.LogicalType
	for i := 0; i < n; i++ {
		ev := rv.Index(i)
		dv, err := toDuckDBValueDepth(ev.Interface(), depth+1)
		if err != nil {
			// Support slices of Go structs (e.g. []Address).
			et := elemGoType
			for et.Kind() == reflect.Ptr {
				et = et.Elem()
			}
			if et.Kind() == reflect.Struct && !isDirectScalarStructType(et) {
				st, stErr := encodeStructFromGo(ev, depth+1)
				if stErr != nil {
					return duckdb.Value{}, fmt.Errorf("slice[%d]: %w", i, stErr)
				}
				dv, err = duckdb.NewStructValue(st.Fields)
			}
			if err != nil {
				return duckdb.Value{}, fmt.Errorf("slice[%d]: %w", i, err)
			}
		}
		elems[i] = dv
		if inferred.Kind == duckdb.TypeInvalid && !dv.IsNull() {
			inferred = dv.Type()
		}
	}
	if inferred.Kind == duckdb.TypeInvalid {
		lt, err := inferLogicalTypeFromGoType(elemGoType, depth+1)
		if err != nil {
			return duckdb.Value{}, fmt.Errorf("%w: cannot infer DuckDB LIST element type for %s: %v", duckdb.ErrInvalidInput, rv.Type(), err)
		}
		inferred = lt
	}
	return duckdb.NewListValue(inferred, elems)
}

func convertElementOrStruct(ev reflect.Value, elemGoType reflect.Type, depth int) (duckdb.Value, error) {
	dv, err := toDuckDBValueDepth(ev.Interface(), depth+1)
	if err == nil {
		return dv, nil
	}
	et := elemGoType
	for et.Kind() == reflect.Ptr {
		et = et.Elem()
	}
	if et.Kind() == reflect.Struct && !isDirectScalarStructType(et) {
		st, stErr := encodeStructFromGo(ev, depth+1)
		if stErr != nil {
			return duckdb.Value{}, stErr
		}
		return duckdb.NewStructValue(st.Fields)
	}
	return duckdb.Value{}, err
}

func arrayToDuckDBArray(rv reflect.Value, depth int) (duckdb.Value, error) {
	n := rv.Len()
	if n == 0 {
		return duckdb.Value{}, fmt.Errorf("%w: zero-length Go array %s cannot bind as DuckDB ARRAY", duckdb.ErrInvalidInput, rv.Type())
	}
	elemGoType := rv.Type().Elem()
	elems := make([]duckdb.Value, n)
	var inferred duckdb.LogicalType
	for i := 0; i < n; i++ {
		dv, err := convertElementOrStruct(rv.Index(i), elemGoType, depth)
		if err != nil {
			return duckdb.Value{}, fmt.Errorf("array[%d]: %w", i, err)
		}
		elems[i] = dv
		if inferred.Kind == duckdb.TypeInvalid && !dv.IsNull() {
			inferred = dv.Type()
		}
	}
	if inferred.Kind == duckdb.TypeInvalid {
		lt, err := inferLogicalTypeFromGoType(elemGoType, depth+1)
		if err != nil {
			return duckdb.Value{}, fmt.Errorf("%w: cannot infer DuckDB ARRAY element type for %s: %v", duckdb.ErrInvalidInput, rv.Type(), err)
		}
		inferred = lt
	}
	return duckdb.NewArrayValue(inferred, elems)
}

func compareReflectKeys(a, b reflect.Value) int {
	switch a.Kind() {
	case reflect.String:
		sa, sb := a.String(), b.String()
		if sa < sb {
			return -1
		}
		if sa > sb {
			return 1
		}
		return 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		ia, ib := a.Int(), b.Int()
		if ia < ib {
			return -1
		}
		if ia > ib {
			return 1
		}
		return 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		ua, ub := a.Uint(), b.Uint()
		if ua < ub {
			return -1
		}
		if ua > ub {
			return 1
		}
		return 0
	case reflect.Float32, reflect.Float64:
		fa, fb := a.Float(), b.Float()
		if fa < fb {
			return -1
		}
		if fa > fb {
			return 1
		}
		return 0
	case reflect.Bool:
		ba, bb := a.Bool(), b.Bool()
		if !ba && bb {
			return -1
		}
		if ba && !bb {
			return 1
		}
		return 0
	default:
		sa, sb := fmt.Sprint(a.Interface()), fmt.Sprint(b.Interface())
		if sa < sb {
			return -1
		}
		if sa > sb {
			return 1
		}
		return 0
	}
}

func mapToDuckDBMap(rv reflect.Value, depth int) (duckdb.Value, error) {
	keyGoType := rv.Type().Key()
	valGoType := rv.Type().Elem()
	keys := rv.MapKeys()
	sort.Slice(keys, func(i, j int) bool {
		return compareReflectKeys(keys[i], keys[j]) < 0
	})
	entries := make([]duckdb.MapEntry, len(keys))
	var keyType, valType duckdb.LogicalType
	for i, kv := range keys {
		dk, err := convertElementOrStruct(kv, keyGoType, depth)
		if err != nil {
			return duckdb.Value{}, fmt.Errorf("map key[%d]: %w", i, err)
		}
		if dk.IsNull() {
			return duckdb.Value{}, fmt.Errorf("%w: map key[%d] must not be NULL", duckdb.ErrInvalidInput, i)
		}
		dv, err := convertElementOrStruct(rv.MapIndex(kv), valGoType, depth)
		if err != nil {
			return duckdb.Value{}, fmt.Errorf("map value[%v]: %w", kv.Interface(), err)
		}
		entries[i] = duckdb.MapEntry{Key: dk, Value: dv}
		if keyType.Kind == duckdb.TypeInvalid {
			keyType = dk.Type()
		}
		if valType.Kind == duckdb.TypeInvalid && !dv.IsNull() {
			valType = dv.Type()
		}
	}
	if keyType.Kind == duckdb.TypeInvalid {
		lt, err := inferLogicalTypeFromGoType(keyGoType, depth+1)
		if err != nil {
			return duckdb.Value{}, fmt.Errorf("%w: cannot infer DuckDB MAP key type for %s: %v", duckdb.ErrInvalidInput, rv.Type(), err)
		}
		keyType = lt
	}
	if valType.Kind == duckdb.TypeInvalid {
		lt, err := inferLogicalTypeFromGoType(valGoType, depth+1)
		if err != nil {
			return duckdb.Value{}, fmt.Errorf("%w: cannot infer DuckDB MAP value type for %s: %v", duckdb.ErrInvalidInput, rv.Type(), err)
		}
		valType = lt
	}
	return duckdb.NewMapValue(keyType, valType, entries)
}

func isDirectScalarStructType(t reflect.Type) bool {
	switch t {
	case reflect.TypeFor[time.Time](),
		reflect.TypeFor[duckdb.Value](),
		reflect.TypeFor[duckdb.Hugeint](),
		reflect.TypeFor[duckdb.Uhugeint](),
		reflect.TypeFor[duckdb.Decimal](),
		reflect.TypeFor[duckdb.Date](),
		reflect.TypeFor[duckdb.Time](),
		reflect.TypeFor[duckdb.TimeNS](),
		reflect.TypeFor[duckdb.TimeTZ](),
		reflect.TypeFor[duckdb.Timestamp](),
		reflect.TypeFor[duckdb.Interval](),
		reflect.TypeFor[duckdb.Enum](),
		reflect.TypeFor[duckdb.Bit](),
		reflect.TypeFor[duckdb.Bignum](),
		reflect.TypeFor[duckdb.Geometry](),
		reflect.TypeFor[duckdb.UnionValue](),
		reflect.TypeFor[duckdb.Variant](),
		reflect.TypeFor[duckdb.ListValue](),
		reflect.TypeFor[duckdb.ArrayValue](),
		reflect.TypeFor[duckdb.StructValue](),
		reflect.TypeFor[duckdb.MapValue](),
		reflect.TypeFor[big.Int](),
		reflect.TypeFor[sql.NullBool](),
		reflect.TypeFor[sql.NullByte](),
		reflect.TypeFor[sql.NullInt16](),
		reflect.TypeFor[sql.NullInt32](),
		reflect.TypeFor[sql.NullInt64](),
		reflect.TypeFor[sql.NullFloat64](),
		reflect.TypeFor[sql.NullString](),
		reflect.TypeFor[sql.NullTime]():
		return true
	default:
		return false
	}
}

func inferLogicalTypeFromGoType(t reflect.Type, depth int) (duckdb.LogicalType, error) {
	if depth > 64 {
		return duckdb.LogicalType{}, fmt.Errorf("%w: type nesting exceeds limit", duckdb.ErrLimitExceeded)
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	switch t {
	case reflect.TypeFor[[]byte]():
		return duckdb.ScalarType(duckdb.TypeBlob), nil
	case reflect.TypeFor[time.Time](), reflect.TypeFor[sql.NullTime]():
		return duckdb.ScalarType(duckdb.TypeTimestampTZ), nil
	case reflect.TypeFor[big.Int](), reflect.TypeFor[duckdb.Bignum]():
		return duckdb.ScalarType(duckdb.TypeBignum), nil
	case reflect.TypeFor[duckdb.Hugeint]():
		return duckdb.ScalarType(duckdb.TypeHugeint), nil
	case reflect.TypeFor[duckdb.Uhugeint]():
		return duckdb.ScalarType(duckdb.TypeUhugeint), nil
	case reflect.TypeFor[duckdb.UUID]():
		return duckdb.ScalarType(duckdb.TypeUUID), nil
	case reflect.TypeFor[duckdb.Date]():
		return duckdb.ScalarType(duckdb.TypeDate), nil
	case reflect.TypeFor[duckdb.Time]():
		return duckdb.ScalarType(duckdb.TypeTime), nil
	case reflect.TypeFor[duckdb.TimeNS]():
		return duckdb.ScalarType(duckdb.TypeTimeNS), nil
	case reflect.TypeFor[duckdb.TimeTZ]():
		return duckdb.ScalarType(duckdb.TypeTimeTZ), nil
	case reflect.TypeFor[duckdb.Interval]():
		return duckdb.ScalarType(duckdb.TypeInterval), nil
	case reflect.TypeFor[duckdb.Bit]():
		return duckdb.ScalarType(duckdb.TypeBit), nil
	case reflect.TypeFor[sql.NullBool]():
		return duckdb.ScalarType(duckdb.TypeBoolean), nil
	case reflect.TypeFor[sql.NullByte]():
		return duckdb.ScalarType(duckdb.TypeUTinyint), nil
	case reflect.TypeFor[sql.NullInt16]():
		return duckdb.ScalarType(duckdb.TypeSmallint), nil
	case reflect.TypeFor[sql.NullInt32]():
		return duckdb.ScalarType(duckdb.TypeInteger), nil
	case reflect.TypeFor[sql.NullInt64]():
		return duckdb.ScalarType(duckdb.TypeBigint), nil
	case reflect.TypeFor[sql.NullFloat64]():
		return duckdb.ScalarType(duckdb.TypeDouble), nil
	case reflect.TypeFor[sql.NullString]():
		return duckdb.ScalarType(duckdb.TypeVarchar), nil
	}
	switch t.Kind() {
	case reflect.Bool:
		return duckdb.ScalarType(duckdb.TypeBoolean), nil
	case reflect.Int8:
		return duckdb.ScalarType(duckdb.TypeTinyint), nil
	case reflect.Int16:
		return duckdb.ScalarType(duckdb.TypeSmallint), nil
	case reflect.Int32:
		return duckdb.ScalarType(duckdb.TypeInteger), nil
	case reflect.Int, reflect.Int64:
		return duckdb.ScalarType(duckdb.TypeBigint), nil
	case reflect.Uint8:
		return duckdb.ScalarType(duckdb.TypeUTinyint), nil
	case reflect.Uint16:
		return duckdb.ScalarType(duckdb.TypeUSmallint), nil
	case reflect.Uint32:
		return duckdb.ScalarType(duckdb.TypeUInteger), nil
	case reflect.Uint, reflect.Uint64:
		return duckdb.ScalarType(duckdb.TypeUBigint), nil
	case reflect.Float32:
		return duckdb.ScalarType(duckdb.TypeFloat), nil
	case reflect.Float64:
		return duckdb.ScalarType(duckdb.TypeDouble), nil
	case reflect.String:
		return duckdb.ScalarType(duckdb.TypeVarchar), nil
	case reflect.Slice:
		elem, err := inferLogicalTypeFromGoType(t.Elem(), depth+1)
		if err != nil {
			return duckdb.LogicalType{}, err
		}
		return duckdb.ListType(elem), nil
	case reflect.Array:
		if t.Len() == 0 {
			return duckdb.LogicalType{}, fmt.Errorf("%w: zero-length Go array %s is not valid as DuckDB ARRAY", duckdb.ErrInvalidInput, t)
		}
		elem, err := inferLogicalTypeFromGoType(t.Elem(), depth+1)
		if err != nil {
			return duckdb.LogicalType{}, err
		}
		return duckdb.ArrayType(elem, uint64(t.Len())), nil
	case reflect.Map:
		kt, err := inferLogicalTypeFromGoType(t.Key(), depth+1)
		if err != nil {
			return duckdb.LogicalType{}, err
		}
		vt, err := inferLogicalTypeFromGoType(t.Elem(), depth+1)
		if err != nil {
			return duckdb.LogicalType{}, err
		}
		return duckdb.MapType(kt, vt), nil
	case reflect.Struct:
		if isDirectScalarStructType(t) {
			return duckdb.LogicalType{}, fmt.Errorf("%w: ambiguous logical type for %s", duckdb.ErrInvalidInput, t)
		}
		fields := make([]duckdb.StructFieldType, 0, t.NumField())
		for i := 0; i < t.NumField(); i++ {
			sf := t.Field(i)
			if !sf.IsExported() {
				continue
			}
			tag := sf.Tag.Get("db")
			if tag == "-" {
				continue
			}
			name := sf.Name
			if tag != "" {
				if n, _, _ := strings.Cut(tag, ","); n != "" {
					name = n
				}
			}
			ft, err := inferLogicalTypeFromGoType(sf.Type, depth+1)
			if err != nil {
				return duckdb.LogicalType{}, fmt.Errorf("field %q: %w", name, err)
			}
			fields = append(fields, duckdb.StructFieldType{Name: name, Type: ft})
		}
		if len(fields) == 0 {
			return duckdb.LogicalType{}, fmt.Errorf("%w: struct %s has no exported fields", duckdb.ErrInvalidInput, t)
		}
		return duckdb.StructType(fields...), nil
	default:
		return duckdb.LogicalType{}, fmt.Errorf("%w: unsupported Go type %s", duckdb.ErrUnsupportedType, t)
	}
}

func timeToDuckDBValue(t time.Time) (duckdb.Value, error) {
	ts, err := duckdb.TimestampFromTime(t, duckdb.TimestampUnitTZ)
	if err != nil {
		return duckdb.Value{}, err
	}
	return duckdb.TimestampValue(ts)
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
			return d.Time()
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
			return ts.Time()
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
	case duckdb.TypeVariant:
		return v.Variant()
	default:
		return nil, fmt.Errorf("%w: %s", duckdb.ErrUnsupportedType, v.Kind())
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
	case duckdb.TypeVariant:
		return reflect.TypeFor[duckdb.Variant]()
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
