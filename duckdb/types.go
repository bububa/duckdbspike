package duckdb

import (
	"fmt"
	"math"
	"math/big"
	"strings"
)

// TypeKind is the closed enum of logical SQL types supported by this binding.
// Zero is reserved for TypeInvalid and rejected.
type TypeKind int

const (
	TypeInvalid TypeKind = iota
	TypeBoolean
	TypeTinyint
	TypeSmallint
	TypeInteger
	TypeBigint
	TypeUTinyint
	TypeUSmallint
	TypeUInteger
	TypeUBigint
	TypeHugeint
	TypeUhugeint
	TypeDecimal
	TypeFloat
	TypeDouble
	TypeVarchar
	TypeBlob
	TypeDate
	TypeTime
	TypeTimeNS
	TypeTimestamp
	TypeTimestampS
	TypeTimestampMS
	TypeTimestampNS
	TypeTimestampTZ
	TypeInterval
	TypeList
	TypeArray
	TypeStruct
	TypeMap
)

// String returns a diagnostic name for k. It is for display and error messages
// only, never for identity, branching, or round-tripping.
func (k TypeKind) String() string {
	switch k {
	case TypeBoolean:
		return "BOOLEAN"
	case TypeTinyint:
		return "TINYINT"
	case TypeSmallint:
		return "SMALLINT"
	case TypeInteger:
		return "INTEGER"
	case TypeBigint:
		return "BIGINT"
	case TypeUTinyint:
		return "UTINYINT"
	case TypeUSmallint:
		return "USMALLINT"
	case TypeUInteger:
		return "UINTEGER"
	case TypeUBigint:
		return "UBIGINT"
	case TypeHugeint:
		return "HUGEINT"
	case TypeUhugeint:
		return "UHUGEINT"
	case TypeDecimal:
		return "DECIMAL"
	case TypeFloat:
		return "FLOAT"
	case TypeDouble:
		return "DOUBLE"
	case TypeVarchar:
		return "VARCHAR"
	case TypeBlob:
		return "BLOB"
	case TypeDate:
		return "DATE"
	case TypeTime:
		return "TIME"
	case TypeTimeNS:
		return "TIME_NS"
	case TypeTimestamp:
		return "TIMESTAMP"
	case TypeTimestampS:
		return "TIMESTAMP_S"
	case TypeTimestampMS:
		return "TIMESTAMP_MS"
	case TypeTimestampNS:
		return "TIMESTAMP_NS"
	case TypeTimestampTZ:
		return "TIMESTAMPTZ"
	case TypeInterval:
		return "INTERVAL"
	case TypeList:
		return "LIST"
	case TypeArray:
		return "ARRAY"
	case TypeStruct:
		return "STRUCT"
	case TypeMap:
		return "MAP"
	default:
		return "INVALID"
	}
}

// StructFieldType describes one positional field of a STRUCT logical type.
type StructFieldType struct {
	Name string
	Type LogicalType
}

// LogicalType is an immutable Go description of a DuckDB logical type tree.
// All native handles are released before a LogicalType is returned.
type LogicalType struct {
	Kind         TypeKind
	DecimalWidth uint8
	DecimalScale uint8
	ArraySize    uint64
	Elem         *LogicalType
	Key          *LogicalType
	Value        *LogicalType
	Fields       []StructFieldType
}

// ScalarType constructs a LogicalType for a non-parameterized scalar kind.
func ScalarType(kind TypeKind) LogicalType {
	return LogicalType{Kind: kind}
}

// DecimalType constructs a DECIMAL(width, scale) LogicalType.
func DecimalType(width, scale uint8) LogicalType {
	return LogicalType{Kind: TypeDecimal, DecimalWidth: width, DecimalScale: scale}
}

// Equal reports exact structural equality of two LogicalType trees.
func (t LogicalType) Equal(other LogicalType) bool {
	if t.Kind != other.Kind ||
		t.DecimalWidth != other.DecimalWidth ||
		t.DecimalScale != other.DecimalScale ||
		t.ArraySize != other.ArraySize {
		return false
	}
	if !equalLogicalTypePtr(t.Elem, other.Elem) ||
		!equalLogicalTypePtr(t.Key, other.Key) ||
		!equalLogicalTypePtr(t.Value, other.Value) {
		return false
	}
	if len(t.Fields) != len(other.Fields) {
		return false
	}
	for i := range t.Fields {
		if t.Fields[i].Name != other.Fields[i].Name || !t.Fields[i].Type.Equal(other.Fields[i].Type) {
			return false
		}
	}
	return true
}

func equalLogicalTypePtr(a, b *LogicalType) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}

// String formats a diagnostic description of t.
func (t LogicalType) String() string {
	switch t.Kind {
	case TypeDecimal:
		return fmt.Sprintf("DECIMAL(%d,%d)", t.DecimalWidth, t.DecimalScale)
	case TypeList:
		if t.Elem != nil {
			return fmt.Sprintf("LIST<%s>", t.Elem.String())
		}
		return "LIST"
	case TypeArray:
		if t.Elem != nil {
			return fmt.Sprintf("ARRAY<%s,%d>", t.Elem.String(), t.ArraySize)
		}
		return "ARRAY"
	case TypeMap:
		if t.Key != nil && t.Value != nil {
			return fmt.Sprintf("MAP<%s,%s>", t.Key.String(), t.Value.String())
		}
		return "MAP"
	case TypeStruct:
		parts := make([]string, len(t.Fields))
		for i, f := range t.Fields {
			parts[i] = f.Name + ":" + f.Type.String()
		}
		return "STRUCT{" + strings.Join(parts, ",") + "}"
	default:
		return t.Kind.String()
	}
}

// Hugeint is an immutable signed 128-bit integer in two's complement
// (value = Upper*2^64 + Lower).
type Hugeint struct {
	Lower uint64
	Upper int64
}

var (
	hugeintMin = new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))
	hugeintMax = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 127), big.NewInt(1))
	uhugeMax   = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))
	twoPow64   = new(big.Int).Lsh(big.NewInt(1), 64)
	mask64     = new(big.Int).Sub(twoPow64, big.NewInt(1))
	pow10Table = func() [39]*big.Int {
		var t [39]*big.Int
		t[0] = big.NewInt(1)
		ten := big.NewInt(10)
		for i := 1; i < len(t); i++ {
			t[i] = new(big.Int).Mul(t[i-1], ten)
		}
		return t
	}()
)

// HugeintFromInt64 converts v to an exact 128-bit signed integer.
func HugeintFromInt64(v int64) Hugeint {
	if v < 0 {
		return Hugeint{Lower: uint64(v), Upper: -1}
	}
	return Hugeint{Lower: uint64(v), Upper: 0}
}

// HugeintFromBigInt converts v to Hugeint, rejecting nil or values outside
// [-2^127, 2^127-1]. v is borrowed and not retained.
func HugeintFromBigInt(v *big.Int) (Hugeint, error) {
	if v == nil {
		return Hugeint{}, fmt.Errorf("%w: nil big.Int", ErrInvalidInput)
	}
	if v.Cmp(hugeintMin) < 0 || v.Cmp(hugeintMax) > 0 {
		return Hugeint{}, fmt.Errorf("%w: %s outside HUGEINT range", ErrOutOfRange, v.String())
	}
	low := new(big.Int).And(v, mask64)
	high := new(big.Int).Rsh(v, 64)
	return Hugeint{Lower: low.Uint64(), Upper: high.Int64()}, nil
}

// BigInt returns a fresh owned *big.Int equal to h.
func (h Hugeint) BigInt() *big.Int {
	v := new(big.Int).SetInt64(h.Upper)
	v.Lsh(v, 64)
	v.Add(v, new(big.Int).SetUint64(h.Lower))
	return v
}

// Int64 narrows h to int64 if it fits in [math.MinInt64, math.MaxInt64].
func (h Hugeint) Int64() (int64, bool) {
	if h.Upper == 0 && h.Lower <= math.MaxInt64 {
		return int64(h.Lower), true
	}
	if h.Upper == -1 && h.Lower >= uint64(1)<<63 {
		return int64(h.Lower), true
	}
	return 0, false
}

// String formats h in base 10 without float conversion.
func (h Hugeint) String() string {
	return h.BigInt().String()
}

// Uhugeint is an immutable unsigned 128-bit integer (value = Upper*2^64 + Lower).
type Uhugeint struct {
	Lower uint64
	Upper uint64
}

// UhugeintFromUint64 converts v to an exact 128-bit unsigned integer.
func UhugeintFromUint64(v uint64) Uhugeint {
	return Uhugeint{Lower: v, Upper: 0}
}

// UhugeintFromBigInt converts v to Uhugeint, rejecting nil or values outside
// [0, 2^128-1]. v is borrowed and not retained.
func UhugeintFromBigInt(v *big.Int) (Uhugeint, error) {
	if v == nil {
		return Uhugeint{}, fmt.Errorf("%w: nil big.Int", ErrInvalidInput)
	}
	if v.Sign() < 0 || v.Cmp(uhugeMax) > 0 {
		return Uhugeint{}, fmt.Errorf("%w: %s outside UHUGEINT range", ErrOutOfRange, v.String())
	}
	low := new(big.Int).And(v, mask64)
	high := new(big.Int).Rsh(v, 64)
	return Uhugeint{Lower: low.Uint64(), Upper: high.Uint64()}, nil
}

// BigInt returns a fresh owned *big.Int equal to u.
func (u Uhugeint) BigInt() *big.Int {
	v := new(big.Int).SetUint64(u.Upper)
	v.Lsh(v, 64)
	v.Add(v, new(big.Int).SetUint64(u.Lower))
	return v
}

// Uint64 narrows u to uint64 if Upper == 0.
func (u Uhugeint) Uint64() (uint64, bool) {
	if u.Upper != 0 {
		return 0, false
	}
	return u.Lower, true
}

// String formats u in base 10.
func (u Uhugeint) String() string {
	return u.BigInt().String()
}

// Decimal is an exact base-10 fixed-point value: Value * 10^(-Scale).
// Width is in [1, 38] and Scale is in [0, Width].
type Decimal struct {
	Width uint8
	Scale uint8
	Value Hugeint
}

// NewDecimal validates width, scale, and |coef| < 10^width and builds an exact
// Decimal without rounding or float conversion. coef is borrowed.
func NewDecimal(width, scale uint8, coef *big.Int) (Decimal, error) {
	if width < 1 || width > 38 || scale > width {
		return Decimal{}, fmt.Errorf("%w: invalid DECIMAL(%d,%d)", ErrInvalidInput, width, scale)
	}
	if coef == nil {
		return Decimal{}, fmt.Errorf("%w: nil decimal coefficient", ErrInvalidInput)
	}
	abs := new(big.Int).Abs(coef)
	if abs.Cmp(pow10Table[width]) >= 0 {
		return Decimal{}, fmt.Errorf("%w: coefficient %s overflows DECIMAL(%d,%d)", ErrOutOfRange, coef.String(), width, scale)
	}
	h, err := HugeintFromBigInt(coef)
	if err != nil {
		return Decimal{}, err
	}
	return Decimal{Width: width, Scale: scale, Value: h}, nil
}

// DecimalFromInt64 builds a validated Decimal from an int64 coefficient.
func DecimalFromInt64(width, scale uint8, coef int64) (Decimal, error) {
	return NewDecimal(width, scale, big.NewInt(coef))
}

// Validate checks width, scale, and |Value| < 10^Width.
func (d Decimal) Validate() error {
	if d.Width < 1 || d.Width > 38 || d.Scale > d.Width {
		return fmt.Errorf("%w: invalid DECIMAL(%d,%d)", ErrInvalidInput, d.Width, d.Scale)
	}
	abs := d.Value.BigInt()
	abs.Abs(abs)
	if abs.Cmp(pow10Table[d.Width]) >= 0 {
		return fmt.Errorf("%w: coefficient %s overflows DECIMAL(%d,%d)", ErrOutOfRange, d.Value.String(), d.Width, d.Scale)
	}
	return nil
}

// Coefficient returns a fresh owned *big.Int of the unscaled integer value.
func (d Decimal) Coefficient() *big.Int {
	return d.Value.BigInt()
}

// String formats d as an exact decimal string without float conversion.
func (d Decimal) String() string {
	coef := d.Value.BigInt()
	neg := coef.Sign() < 0
	if neg {
		coef.Neg(coef)
	}
	digits := coef.String()
	scale := int(d.Scale)
	if scale > 0 {
		if len(digits) <= scale {
			digits = strings.Repeat("0", scale-len(digits)+1) + digits
		}
		cut := len(digits) - scale
		digits = digits[:cut] + "." + digits[cut:]
	}
	if neg {
		return "-" + digits
	}
	return digits
}

// Date is days since 1970-01-01 (proleptic Gregorian calendar).
type Date struct {
	Days int32
}

// Time is microseconds since 00:00:00.
type Time struct {
	Micros int64
}

// TimeNS is nanoseconds since 00:00:00.
type TimeNS struct {
	Nanos int64
}

// TimestampUnit identifies the exact unit and timezone semantics of Timestamp.
type TimestampUnit int

const (
	TimestampUnitUnknown TimestampUnit = iota
	TimestampUnitSeconds
	TimestampUnitMillis
	TimestampUnitMicros
	TimestampUnitNanos
	// TimestampUnitTZ is microseconds since UTC epoch with TIMESTAMPTZ semantics.
	TimestampUnitTZ
)

// Timestamp preserves DuckDB's exact timestamp unit without converting through
// UnixNano (which would overflow outside 1677..2262).
type Timestamp struct {
	Value int64
	Unit  TimestampUnit
}

// Interval preserves DuckDB's three-component interval without collapsing
// months or days into fixed nanoseconds.
type Interval struct {
	Months int32
	Days   int32
	Micros int64
}

// ListValue is an owned variable-length SQL LIST. Empty list (len(Elements)==0)
// is distinct from SQL NULL (Value.IsNull()).
type ListValue struct {
	ElemType LogicalType
	Elements []Value
}

// ArrayValue is an owned fixed-length SQL ARRAY.
type ArrayValue struct {
	ElemType LogicalType
	Size     uint64
	Elements []Value
}

// StructFieldValue is one positional field of a decoded STRUCT.
type StructFieldValue struct {
	Name  string
	Type  LogicalType
	Value Value
}

// StructValue is an owned decoded SQL STRUCT preserving field order and names.
type StructValue struct {
	Fields []StructFieldValue
}

// MapEntry is one key/value pair of a decoded SQL MAP.
type MapEntry struct {
	Key   Value
	Value Value
}

// MapValue is an owned decoded SQL MAP preserving entry order and NULL values.
type MapValue struct {
	KeyType   LogicalType
	ValueType LogicalType
	Entries   []MapEntry
}

// Value holds one SQL parameter or decoded column value with exact type and
// explicit SQL NULL state. All composite/byte data inside a Value is owned by
// Go and never aliases native chunk memory.
type Value struct {
	typ  LogicalType
	null bool

	b     bool
	i64   int64
	u64   uint64
	f32   float32
	f64   float64
	huge  Hugeint
	uhuge Uhugeint
	dec   Decimal
	str   string
	blob  []byte
	iv    Interval
	ts    Timestamp
	list  ListValue
	arr   ArrayValue
	st    StructValue
	mp    MapValue
}

// IsNull reports whether v represents SQL NULL.
func (v Value) IsNull() bool { return v.null }

// Type returns v's LogicalType (TypeInvalid for untyped NullValue()).
func (v Value) Type() LogicalType { return v.typ }

// Kind returns v.Type().Kind.
func (v Value) Kind() TypeKind { return v.typ.Kind }

// NullValue constructs an untyped SQL NULL parameter value.
func NullValue() Value { return Value{null: true} }

// TypedNullValue constructs a SQL NULL carrying a column's LogicalType.
func TypedNullValue(lt LogicalType) Value { return Value{typ: lt, null: true} }

func BoolValue(v bool) Value         { return Value{typ: ScalarType(TypeBoolean), b: v} }
func Int8Value(v int8) Value         { return Value{typ: ScalarType(TypeTinyint), i64: int64(v)} }
func Int16Value(v int16) Value       { return Value{typ: ScalarType(TypeSmallint), i64: int64(v)} }
func Int32Value(v int32) Value       { return Value{typ: ScalarType(TypeInteger), i64: int64(v)} }
func Int64Value(v int64) Value       { return Value{typ: ScalarType(TypeBigint), i64: v} }
func Uint8Value(v uint8) Value       { return Value{typ: ScalarType(TypeUTinyint), u64: uint64(v)} }
func Uint16Value(v uint16) Value     { return Value{typ: ScalarType(TypeUSmallint), u64: uint64(v)} }
func Uint32Value(v uint32) Value     { return Value{typ: ScalarType(TypeUInteger), u64: uint64(v)} }
func Uint64Value(v uint64) Value     { return Value{typ: ScalarType(TypeUBigint), u64: v} }
func HugeintValue(v Hugeint) Value   { return Value{typ: ScalarType(TypeHugeint), huge: v} }
func UhugeintValue(v Uhugeint) Value { return Value{typ: ScalarType(TypeUhugeint), uhuge: v} }
func Float32Value(v float32) Value   { return Value{typ: ScalarType(TypeFloat), f32: v} }
func Float64Value(v float64) Value   { return Value{typ: ScalarType(TypeDouble), f64: v} }
func VarcharValue(v string) Value    { return Value{typ: ScalarType(TypeVarchar), str: v} }
func DateValue(v Date) Value         { return Value{typ: ScalarType(TypeDate), i64: int64(v.Days)} }
func TimeValue(v Time) Value         { return Value{typ: ScalarType(TypeTime), i64: v.Micros} }
func TimeNSValue(v TimeNS) Value     { return Value{typ: ScalarType(TypeTimeNS), i64: v.Nanos} }
func IntervalValue(v Interval) Value { return Value{typ: ScalarType(TypeInterval), iv: v} }

// DecimalValue wraps a validated Decimal as a Value.
func DecimalValue(d Decimal) (Value, error) {
	if err := d.Validate(); err != nil {
		return Value{}, err
	}
	return Value{typ: DecimalType(d.Width, d.Scale), dec: d}, nil
}

// BlobValue copies b into an owned BLOB Value. An empty non-null BLOB is
// represented with a non-nil empty slice distinct from SQL NULL.
func BlobValue(b []byte) Value {
	cp := make([]byte, len(b))
	copy(cp, b)
	return Value{typ: ScalarType(TypeBlob), blob: cp}
}

// TimestampValue wraps a Timestamp, rejecting TimestampUnitUnknown.
func TimestampValue(ts Timestamp) (Value, error) {
	var kind TypeKind
	switch ts.Unit {
	case TimestampUnitSeconds:
		kind = TypeTimestampS
	case TimestampUnitMillis:
		kind = TypeTimestampMS
	case TimestampUnitMicros:
		kind = TypeTimestamp
	case TimestampUnitNanos:
		kind = TypeTimestampNS
	case TimestampUnitTZ:
		kind = TypeTimestampTZ
	default:
		return Value{}, fmt.Errorf("%w: invalid timestamp unit %d", ErrInvalidInput, ts.Unit)
	}
	return Value{typ: ScalarType(kind), ts: ts}, nil
}

func (v Value) check(want TypeKind) error {
	if v.null {
		return ErrNullValue
	}
	if v.typ.Kind != want {
		return fmt.Errorf("%w: have %s, want %s", ErrTypeMismatch, v.typ.Kind, want)
	}
	return nil
}

func (v Value) Bool() (bool, error) {
	if err := v.check(TypeBoolean); err != nil {
		return false, err
	}
	return v.b, nil
}

func (v Value) Int8() (int8, error) {
	if err := v.check(TypeTinyint); err != nil {
		return 0, err
	}
	return int8(v.i64), nil
}

func (v Value) Int16() (int16, error) {
	if err := v.check(TypeSmallint); err != nil {
		return 0, err
	}
	return int16(v.i64), nil
}

func (v Value) Int32() (int32, error) {
	if err := v.check(TypeInteger); err != nil {
		return 0, err
	}
	return int32(v.i64), nil
}

func (v Value) Int64() (int64, error) {
	if err := v.check(TypeBigint); err != nil {
		return 0, err
	}
	return v.i64, nil
}

func (v Value) Uint8() (uint8, error) {
	if err := v.check(TypeUTinyint); err != nil {
		return 0, err
	}
	return uint8(v.u64), nil
}

func (v Value) Uint16() (uint16, error) {
	if err := v.check(TypeUSmallint); err != nil {
		return 0, err
	}
	return uint16(v.u64), nil
}

func (v Value) Uint32() (uint32, error) {
	if err := v.check(TypeUInteger); err != nil {
		return 0, err
	}
	return uint32(v.u64), nil
}

func (v Value) Uint64() (uint64, error) {
	if err := v.check(TypeUBigint); err != nil {
		return 0, err
	}
	return v.u64, nil
}

// AsInt64 converts any exact integer Value (signed, unsigned, or 128-bit) to
// int64 with strict range checking. Non-integer types fail with ErrTypeMismatch.
func (v Value) AsInt64() (int64, error) {
	if v.null {
		return 0, ErrNullValue
	}
	switch v.typ.Kind {
	case TypeTinyint, TypeSmallint, TypeInteger, TypeBigint:
		return v.i64, nil
	case TypeUTinyint, TypeUSmallint, TypeUInteger, TypeUBigint:
		if v.u64 > math.MaxInt64 {
			return 0, fmt.Errorf("%w: uint value %d exceeds int64", ErrOutOfRange, v.u64)
		}
		return int64(v.u64), nil
	case TypeHugeint:
		n, ok := v.huge.Int64()
		if !ok {
			return 0, fmt.Errorf("%w: HUGEINT %s outside int64", ErrOutOfRange, v.huge.String())
		}
		return n, nil
	case TypeUhugeint:
		u, ok := v.uhuge.Uint64()
		if !ok || u > math.MaxInt64 {
			return 0, fmt.Errorf("%w: UHUGEINT %s exceeds int64", ErrOutOfRange, v.uhuge.String())
		}
		return int64(u), nil
	default:
		return 0, fmt.Errorf("%w: %s is not an integer", ErrTypeMismatch, v.typ.Kind)
	}
}

// AsUint64 converts any exact integer Value to uint64 with strict range checking.
func (v Value) AsUint64() (uint64, error) {
	if v.null {
		return 0, ErrNullValue
	}
	switch v.typ.Kind {
	case TypeTinyint, TypeSmallint, TypeInteger, TypeBigint:
		if v.i64 < 0 {
			return 0, fmt.Errorf("%w: negative integer %d", ErrOutOfRange, v.i64)
		}
		return uint64(v.i64), nil
	case TypeUTinyint, TypeUSmallint, TypeUInteger, TypeUBigint:
		return v.u64, nil
	case TypeHugeint:
		if v.huge.Upper != 0 {
			return 0, fmt.Errorf("%w: HUGEINT %s outside uint64", ErrOutOfRange, v.huge.String())
		}
		return v.huge.Lower, nil
	case TypeUhugeint:
		u, ok := v.uhuge.Uint64()
		if !ok {
			return 0, fmt.Errorf("%w: UHUGEINT %s exceeds uint64", ErrOutOfRange, v.uhuge.String())
		}
		return u, nil
	default:
		return 0, fmt.Errorf("%w: %s is not an integer", ErrTypeMismatch, v.typ.Kind)
	}
}

func (v Value) Hugeint() (Hugeint, error) {
	if err := v.check(TypeHugeint); err != nil {
		return Hugeint{}, err
	}
	return v.huge, nil
}

func (v Value) Uhugeint() (Uhugeint, error) {
	if err := v.check(TypeUhugeint); err != nil {
		return Uhugeint{}, err
	}
	return v.uhuge, nil
}

func (v Value) Decimal() (Decimal, error) {
	if err := v.check(TypeDecimal); err != nil {
		return Decimal{}, err
	}
	return v.dec, nil
}

func (v Value) Float32() (float32, error) {
	if err := v.check(TypeFloat); err != nil {
		return 0, err
	}
	return v.f32, nil
}

func (v Value) Float64() (float64, error) {
	if err := v.check(TypeDouble); err != nil {
		return 0, err
	}
	return v.f64, nil
}

func (v Value) Varchar() (string, error) {
	if err := v.check(TypeVarchar); err != nil {
		return "", err
	}
	return v.str, nil
}

// Blob returns a fresh owned byte slice copy.
func (v Value) Blob() ([]byte, error) {
	if err := v.check(TypeBlob); err != nil {
		return nil, err
	}
	cp := make([]byte, len(v.blob))
	copy(cp, v.blob)
	return cp, nil
}

func (v Value) Date() (Date, error) {
	if err := v.check(TypeDate); err != nil {
		return Date{}, err
	}
	return Date{Days: int32(v.i64)}, nil
}

func (v Value) Time() (Time, error) {
	if err := v.check(TypeTime); err != nil {
		return Time{}, err
	}
	return Time{Micros: v.i64}, nil
}

func (v Value) TimeNS() (TimeNS, error) {
	if err := v.check(TypeTimeNS); err != nil {
		return TimeNS{}, err
	}
	return TimeNS{Nanos: v.i64}, nil
}

func (v Value) Timestamp() (Timestamp, error) {
	if v.null {
		return Timestamp{}, ErrNullValue
	}
	switch v.typ.Kind {
	case TypeTimestamp, TypeTimestampS, TypeTimestampMS, TypeTimestampNS, TypeTimestampTZ:
		return v.ts, nil
	default:
		return Timestamp{}, fmt.Errorf("%w: have %s, want TIMESTAMP*", ErrTypeMismatch, v.typ.Kind)
	}
}

func (v Value) Interval() (Interval, error) {
	if err := v.check(TypeInterval); err != nil {
		return Interval{}, err
	}
	return v.iv, nil
}

func (v Value) List() (ListValue, error) {
	if err := v.check(TypeList); err != nil {
		return ListValue{}, err
	}
	return ListValue{
		ElemType: v.list.ElemType,
		Elements: append([]Value{}, v.list.Elements...),
	}, nil
}

func (v Value) Array() (ArrayValue, error) {
	if err := v.check(TypeArray); err != nil {
		return ArrayValue{}, err
	}
	return ArrayValue{
		ElemType: v.arr.ElemType,
		Size:     v.arr.Size,
		Elements: append([]Value{}, v.arr.Elements...),
	}, nil
}

func (v Value) Struct() (StructValue, error) {
	if err := v.check(TypeStruct); err != nil {
		return StructValue{}, err
	}
	return StructValue{
		Fields: append([]StructFieldValue{}, v.st.Fields...),
	}, nil
}

func (v Value) Map() (MapValue, error) {
	if err := v.check(TypeMap); err != nil {
		return MapValue{}, err
	}
	return MapValue{
		KeyType:   v.mp.KeyType,
		ValueType: v.mp.ValueType,
		Entries:   append([]MapEntry{}, v.mp.Entries...),
	}, nil
}
