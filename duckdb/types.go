package duckdb

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"slices"
	"strings"
	"time"
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
	TypeTimeTZ
	TypeTimestamp
	TypeTimestampS
	TypeTimestampMS
	TypeTimestampNS
	TypeTimestampTZ
	TypeInterval
	TypeUUID
	TypeEnum
	TypeSQLNull
	TypeBit
	TypeBignum
	TypeGeometry
	TypeList
	TypeArray
	TypeStruct
	TypeMap
	TypeUnion
	TypeVariant

	// typeDefault is an internal sentinel kind used exclusively by DefaultValue()
	// to request SQL column DEFAULT evaluation in Appender.AppendRow.
	typeDefault TypeKind = 255
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
	case TypeTimeTZ:
		return "TIMETZ"
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
	case TypeUUID:
		return "UUID"
	case TypeEnum:
		return "ENUM"
	case TypeSQLNull:
		return "SQLNULL"
	case TypeBit:
		return "BIT"
	case TypeBignum:
		return "BIGNUM"
	case TypeGeometry:
		return "GEOMETRY"
	case TypeList:
		return "LIST"
	case TypeArray:
		return "ARRAY"
	case TypeStruct:
		return "STRUCT"
	case TypeMap:
		return "MAP"
	case TypeUnion:
		return "UNION"
	case TypeVariant:
		return "VARIANT"
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
	Alias        string
	GeometryCRS  string
	DecimalWidth uint8
	DecimalScale uint8
	ArraySize    uint64
	Elem         *LogicalType
	Key          *LogicalType
	Value        *LogicalType
	Fields       []StructFieldType
	EnumValues   []string
}

var jsonLogicalType = LogicalType{Kind: TypeVarchar, Alias: "JSON"}

// ScalarType constructs a LogicalType for a non-parameterized scalar kind.
func ScalarType(kind TypeKind) LogicalType {
	return LogicalType{Kind: kind}
}

// JSONType constructs DuckDB's aliased JSON logical type (VARCHAR with alias "JSON").
func JSONType() LogicalType {
	return jsonLogicalType
}

// DecimalType constructs a DECIMAL(width, scale) LogicalType.
func DecimalType(width, scale uint8) LogicalType {
	return LogicalType{Kind: TypeDecimal, DecimalWidth: width, DecimalScale: scale}
}

// EnumType constructs an ENUM LogicalType from its ordered dictionary labels.
func EnumType(values ...string) LogicalType {
	return LogicalType{Kind: TypeEnum, EnumValues: append([]string(nil), values...)}
}

// ListType constructs a LIST<elem> LogicalType.
func ListType(elem LogicalType) LogicalType {
	e := cloneLogicalType(elem)
	return LogicalType{Kind: TypeList, Elem: e}
}

// ArrayType constructs a fixed-length ARRAY<elem, size> LogicalType.
func ArrayType(elem LogicalType, size uint64) LogicalType {
	e := cloneLogicalType(elem)
	return LogicalType{Kind: TypeArray, ArraySize: size, Elem: e}
}

// MapType constructs a MAP<key, val> LogicalType.
func MapType(key, val LogicalType) LogicalType {
	return LogicalType{Kind: TypeMap, Key: cloneLogicalType(key), Value: cloneLogicalType(val)}
}

// StructType constructs a STRUCT LogicalType from positional field definitions.
func StructType(fields ...StructFieldType) LogicalType {
	cp := make([]StructFieldType, len(fields))
	for i, f := range fields {
		cp[i] = StructFieldType{Name: f.Name, Type: *cloneLogicalType(f.Type)}
	}
	return LogicalType{Kind: TypeStruct, Fields: cp}
}

// UnionType constructs a UNION LogicalType from positional tagged member definitions.
func UnionType(members ...StructFieldType) LogicalType {
	cp := make([]StructFieldType, len(members))
	for i, m := range members {
		cp[i] = StructFieldType{Name: m.Name, Type: *cloneLogicalType(m.Type)}
	}
	return LogicalType{Kind: TypeUnion, Fields: cp}
}

// GeometryType constructs a GEOMETRY LogicalType with an optional CRS string.
func GeometryType(crs string) LogicalType {
	return LogicalType{Kind: TypeGeometry, GeometryCRS: crs}
}

// IsJSON reports whether t is DuckDB's JSON logical type (VARCHAR aliased as "JSON").
func (t LogicalType) IsJSON() bool {
	return t.Kind == TypeVarchar && t.Alias == "JSON"
}

func cloneLogicalType(t LogicalType) *LogicalType {
	cp := t
	if t.Elem != nil {
		cp.Elem = cloneLogicalType(*t.Elem)
	}
	if t.Key != nil {
		cp.Key = cloneLogicalType(*t.Key)
	}
	if t.Value != nil {
		cp.Value = cloneLogicalType(*t.Value)
	}
	if len(t.Fields) > 0 {
		cp.Fields = make([]StructFieldType, len(t.Fields))
		for i, f := range t.Fields {
			cp.Fields[i] = StructFieldType{Name: f.Name, Type: *cloneLogicalType(f.Type)}
		}
	}
	if len(t.EnumValues) > 0 {
		cp.EnumValues = append([]string(nil), t.EnumValues...)
	}
	return &cp
}

func (t LogicalType) needsHeapMeta() bool {
	if t.Alias != "" || t.GeometryCRS != "" {
		return true
	}
	switch t.Kind {
	case TypeEnum, TypeList, TypeArray, TypeStruct, TypeMap, TypeUnion:
		return true
	default:
		return false
	}
}

// Equal reports exact structural equality of two LogicalType trees.
func (t LogicalType) Equal(other LogicalType) bool {
	if t.Kind != other.Kind ||
		t.Alias != other.Alias ||
		t.GeometryCRS != other.GeometryCRS ||
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
	if !slices.Equal(t.EnumValues, other.EnumValues) {
		return false
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
	if t.IsJSON() {
		return "JSON"
	}
	var base string
	switch t.Kind {
	case TypeDecimal:
		base = fmt.Sprintf("DECIMAL(%d,%d)", t.DecimalWidth, t.DecimalScale)
	case TypeEnum:
		base = fmt.Sprintf("ENUM(%d)", len(t.EnumValues))
	case TypeList:
		if t.Elem != nil {
			base = fmt.Sprintf("LIST<%s>", t.Elem.String())
		} else {
			base = "LIST"
		}
	case TypeArray:
		if t.Elem != nil {
			base = fmt.Sprintf("ARRAY<%s,%d>", t.Elem.String(), t.ArraySize)
		} else {
			base = "ARRAY"
		}
	case TypeMap:
		if t.Key != nil && t.Value != nil {
			base = fmt.Sprintf("MAP<%s,%s>", t.Key.String(), t.Value.String())
		} else {
			base = "MAP"
		}
	case TypeStruct:
		parts := make([]string, len(t.Fields))
		for i, f := range t.Fields {
			parts[i] = f.Name + ":" + f.Type.String()
		}
		base = "STRUCT{" + strings.Join(parts, ",") + "}"
	case TypeUnion:
		parts := make([]string, len(t.Fields))
		for i, f := range t.Fields {
			parts[i] = f.Name + ":" + f.Type.String()
		}
		base = "UNION{" + strings.Join(parts, ",") + "}"
	case TypeGeometry:
		if t.GeometryCRS != "" {
			base = fmt.Sprintf("GEOMETRY(%q)", t.GeometryCRS)
		} else {
			base = "GEOMETRY"
		}
	default:
		base = t.Kind.String()
	}
	if t.Alias != "" {
		return fmt.Sprintf("%s(%s)", t.Alias, base)
	}
	return base
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

// UUID is a 16-byte RFC 4122 UUID in standard big-endian byte order.
type UUID [16]byte

// String formats u as a canonical lowercase hyphenated UUID string.
func (u UUID) String() string {
	var buf [36]byte
	hex.Encode(buf[0:8], u[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], u[10:16])
	return string(buf[:])
}

// ParseUUID parses a canonical 36-character hyphenated UUID string (case-insensitive).
func ParseUUID(s string) (UUID, error) {
	var u UUID
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return UUID{}, fmt.Errorf("%w: invalid UUID %q", ErrInvalidInput, s)
	}
	var hexBytes [32]byte
	copy(hexBytes[0:8], s[0:8])
	copy(hexBytes[8:12], s[9:13])
	copy(hexBytes[12:16], s[14:18])
	copy(hexBytes[16:20], s[19:23])
	copy(hexBytes[20:32], s[24:36])
	if _, err := hex.Decode(u[:], hexBytes[:]); err != nil {
		return UUID{}, fmt.Errorf("%w: invalid UUID %q: %v", ErrInvalidInput, s, err)
	}
	return u, nil
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

// BigRat returns an exact *big.Rat representing d without floating-point loss.
func (d Decimal) BigRat() *big.Rat {
	denom := new(big.Int)
	if int(d.Scale) < len(pow10Table) {
		denom.Set(pow10Table[d.Scale])
	} else {
		denom.Exp(big.NewInt(10), big.NewInt(int64(d.Scale)), nil)
	}
	return new(big.Rat).SetFrac(d.Coefficient(), denom)
}

// ParseDecimal parses an exact decimal string (for example "-123.45") into a
// validated Decimal(width, scale) without floating-point intermediates.
// Fractional digits exceeding scale are rejected with ErrOutOfRange rather than
// silently truncated.
func ParseDecimal(s string, width, scale uint8) (Decimal, error) {
	if width < 1 || width > 38 || scale > width {
		return Decimal{}, fmt.Errorf("%w: invalid DECIMAL(%d,%d)", ErrInvalidInput, width, scale)
	}
	if s == "" {
		return Decimal{}, fmt.Errorf("%w: empty decimal string", ErrInvalidInput)
	}
	raw := s
	neg := false
	switch raw[0] {
	case '-':
		neg = true
		raw = raw[1:]
	case '+':
		raw = raw[1:]
	}
	if raw == "" {
		return Decimal{}, fmt.Errorf("%w: invalid decimal %q", ErrInvalidInput, s)
	}
	intPart, fracPart, hasDot := strings.Cut(raw, ".")
	if hasDot && strings.ContainsRune(fracPart, '.') {
		return Decimal{}, fmt.Errorf("%w: invalid decimal %q", ErrInvalidInput, s)
	}
	if intPart == "" && fracPart == "" {
		return Decimal{}, fmt.Errorf("%w: invalid decimal %q", ErrInvalidInput, s)
	}
	for i := 0; i < len(intPart); i++ {
		if intPart[i] < '0' || intPart[i] > '9' {
			return Decimal{}, fmt.Errorf("%w: invalid decimal %q", ErrInvalidInput, s)
		}
	}
	for i := 0; i < len(fracPart); i++ {
		if fracPart[i] < '0' || fracPart[i] > '9' {
			return Decimal{}, fmt.Errorf("%w: invalid decimal %q", ErrInvalidInput, s)
		}
	}
	if len(fracPart) > int(scale) {
		return Decimal{}, fmt.Errorf("%w: decimal %q exceeds scale %d", ErrOutOfRange, s, scale)
	}
	var b strings.Builder
	b.Grow(len(intPart) + int(scale) + 1)
	if neg {
		b.WriteByte('-')
	}
	if intPart == "" {
		b.WriteByte('0')
	} else {
		b.WriteString(intPart)
	}
	b.WriteString(fracPart)
	for i := len(fracPart); i < int(scale); i++ {
		b.WriteByte('0')
	}
	coef, ok := new(big.Int).SetString(b.String(), 10)
	if !ok {
		return Decimal{}, fmt.Errorf("%w: invalid decimal %q", ErrInvalidInput, s)
	}
	return NewDecimal(width, scale, coef)
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

// Date infinity sentinels matching DuckDB's date_t::infinity / ninfinity.
const (
	DateInfinity    int32 = math.MaxInt32
	DateNegInfinity int32 = -math.MaxInt32
)

// Date is days since 1970-01-01 (proleptic Gregorian calendar).
type Date struct {
	Days int32
}

// IsInfinity reports whether d represents DuckDB's positive infinity DATE.
func (d Date) IsInfinity() bool { return d.Days == DateInfinity }

// IsNegInfinity reports whether d represents DuckDB's negative infinity DATE.
func (d Date) IsNegInfinity() bool { return d.Days == DateNegInfinity }

// IsFinite reports whether d is neither +infinity nor -infinity.
func (d Date) IsFinite() bool { return d.Days != DateInfinity && d.Days != DateNegInfinity }

// DateFromTime converts t's UTC calendar date into a finite DuckDB Date.
func DateFromTime(t time.Time) (Date, error) {
	u := t.UTC()
	midnight := time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
	days := midnight.Unix() / 86400
	if days >= int64(DateInfinity) || days <= int64(DateNegInfinity) {
		return Date{}, fmt.Errorf("%w: date %s overflows DuckDB DATE range", ErrOutOfRange, u.Format("2006-01-02"))
	}
	return Date{Days: int32(days)}, nil
}

// Time converts a finite Date into midnight UTC time.Time. Non-finite dates
// (±infinity) return ErrOutOfRange.
func (d Date) Time() (time.Time, error) {
	if !d.IsFinite() {
		return time.Time{}, fmt.Errorf("%w: non-finite DATE (%d) cannot be converted to time.Time", ErrOutOfRange, d.Days)
	}
	return time.Unix(int64(d.Days)*86400, 0).UTC(), nil
}

// String formats d as YYYY-MM-DD, "infinity", or "-infinity".
func (d Date) String() string {
	if d.IsInfinity() {
		return "infinity"
	}
	if d.IsNegInfinity() {
		return "-infinity"
	}
	return time.Unix(int64(d.Days)*86400, 0).UTC().Format("2006-01-02")
}

// Time is microseconds since 00:00:00.
type Time struct {
	Micros int64
}

// String formats t as HH:MM:SS[.ffffff].
func (t Time) String() string {
	return formatDayNanos(t.Micros*1000, 6)
}

// TimeNS is nanoseconds since 00:00:00.
type TimeNS struct {
	Nanos int64
}

// String formats t as HH:MM:SS[.fffffffff].
func (t TimeNS) String() string {
	return formatDayNanos(t.Nanos, 9)
}

func formatDayNanos(totalNanos int64, maxFrac int) string {
	neg := totalNanos < 0
	if neg {
		totalNanos = -totalNanos
	}
	secs := totalNanos / 1_000_000_000
	nanos := totalNanos % 1_000_000_000
	h := secs / 3600
	m := (secs % 3600) / 60
	s := secs % 60
	prefix := ""
	if neg {
		prefix = "-"
	}
	base := fmt.Sprintf("%s%02d:%02d:%02d", prefix, h, m, s)
	if nanos == 0 {
		return base
	}
	frac := fmt.Sprintf("%09d", nanos)
	if maxFrac < 9 {
		frac = frac[:maxFrac]
	}
	frac = strings.TrimRight(frac, "0")
	if frac == "" {
		return base
	}
	return base + "." + frac
}

const (
	timeTZMaxMicros = 24 * 60 * 60 * 1_000_000 // 86_400_000_000 (DuckDB allows 24:00:00)
	timeTZMaxOffset = 16*60*60 - 1             // ±15:59:59 (±57599s)
)

// TimeTZ preserves DuckDB's TIME WITH TIME ZONE value as microseconds since
// 00:00:00 and a fixed UTC offset in seconds (east of UTC, in [-57599, +57599]).
type TimeTZ struct {
	Micros        int64
	OffsetSeconds int32
}

// Validate checks that Micros and OffsetSeconds are within DuckDB's TIME_TZ bounds.
func (t TimeTZ) Validate() error {
	if t.Micros < 0 || t.Micros > timeTZMaxMicros {
		return fmt.Errorf("%w: TIMETZ micros %d outside [0, %d]", ErrOutOfRange, t.Micros, timeTZMaxMicros)
	}
	if t.OffsetSeconds < -timeTZMaxOffset || t.OffsetSeconds > timeTZMaxOffset {
		return fmt.Errorf("%w: TIMETZ offset %d outside [%d, %d]", ErrOutOfRange, t.OffsetSeconds, -timeTZMaxOffset, timeTZMaxOffset)
	}
	return nil
}

// String formats t as HH:MM:SS[.ffffff]±HH:MM[:SS].
func (t TimeTZ) String() string {
	base := formatDayNanos(t.Micros*1000, 6)
	off := t.OffsetSeconds
	sign := "+"
	if off < 0 {
		sign = "-"
		off = -off
	}
	oh := off / 3600
	om := (off % 3600) / 60
	os := off % 60
	if os == 0 {
		return fmt.Sprintf("%s%s%02d:%02d", base, sign, oh, om)
	}
	return fmt.Sprintf("%s%s%02d:%02d:%02d", base, sign, oh, om, os)
}

// Enum represents a SQL ENUM dictionary member with both its zero-based
// dictionary index and its exact label.
type Enum struct {
	Index uint32
	Label string
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

// Timestamp infinity sentinels matching DuckDB's timestamp_t::infinity / ninfinity.
const (
	TimestampInfinity    int64 = math.MaxInt64
	TimestampNegInfinity int64 = -math.MaxInt64
)

// Timestamp preserves DuckDB's exact timestamp unit without converting through
// UnixNano (which would overflow outside 1677..2262).
type Timestamp struct {
	Value int64
	Unit  TimestampUnit
}

// IsInfinity reports whether t represents DuckDB's positive infinity TIMESTAMP.
func (t Timestamp) IsInfinity() bool { return t.Value == TimestampInfinity }

// IsNegInfinity reports whether t represents DuckDB's negative infinity TIMESTAMP.
func (t Timestamp) IsNegInfinity() bool { return t.Value == TimestampNegInfinity }

// IsFinite reports whether t is neither +infinity nor -infinity.
func (t Timestamp) IsFinite() bool {
	return t.Value != TimestampInfinity && t.Value != TimestampNegInfinity
}

// TimestampFromTime converts t (in UTC) into a Timestamp of the requested unit.
// Sub-unit precision is rejected with ErrInvalidInput rather than silently
// truncated, and out-of-range instants return ErrOutOfRange.
func TimestampFromTime(t time.Time, unit TimestampUnit) (Timestamp, error) {
	u := t.UTC()
	sec := u.Unix()
	nsec := int64(u.Nanosecond())
	switch unit {
	case TimestampUnitSeconds:
		if nsec != 0 {
			return Timestamp{}, fmt.Errorf("%w: time.Time %s has sub-second precision", ErrInvalidInput, u.Format(time.RFC3339Nano))
		}
		if sec == TimestampInfinity || sec == TimestampNegInfinity {
			return Timestamp{}, fmt.Errorf("%w: time.Time %s collides with TIMESTAMP_S infinity sentinel", ErrOutOfRange, u.Format(time.RFC3339Nano))
		}
		return Timestamp{Value: sec, Unit: unit}, nil
	case TimestampUnitMillis:
		if nsec%1_000_000 != 0 {
			return Timestamp{}, fmt.Errorf("%w: time.Time %s has sub-millisecond precision", ErrInvalidInput, u.Format(time.RFC3339Nano))
		}
		const maxMillisSec = (math.MaxInt64 - 999) / 1000
		const minMillisSec = (-math.MaxInt64 + 999) / 1000
		if sec > maxMillisSec || sec < minMillisSec {
			return Timestamp{}, fmt.Errorf("%w: time.Time %s overflows TIMESTAMP_MS range", ErrOutOfRange, u.Format(time.RFC3339Nano))
		}
		v := sec*1000 + nsec/1_000_000
		if v == TimestampInfinity || v == TimestampNegInfinity {
			return Timestamp{}, fmt.Errorf("%w: time.Time %s overflows TIMESTAMP_MS range", ErrOutOfRange, u.Format(time.RFC3339Nano))
		}
		return Timestamp{Value: v, Unit: unit}, nil
	case TimestampUnitMicros, TimestampUnitTZ:
		if nsec%1000 != 0 {
			return Timestamp{}, fmt.Errorf("%w: time.Time %s has sub-microsecond precision", ErrInvalidInput, u.Format(time.RFC3339Nano))
		}
		const maxMicrosSec = (math.MaxInt64 - 999_999) / 1_000_000
		const minMicrosSec = (-math.MaxInt64 + 999_999) / 1_000_000
		if sec > maxMicrosSec || sec < minMicrosSec {
			return Timestamp{}, fmt.Errorf("%w: time.Time %s overflows TIMESTAMP microsecond range", ErrOutOfRange, u.Format(time.RFC3339Nano))
		}
		v := sec*1_000_000 + nsec/1000
		if v == TimestampInfinity || v == TimestampNegInfinity {
			return Timestamp{}, fmt.Errorf("%w: time.Time %s overflows TIMESTAMP microsecond range", ErrOutOfRange, u.Format(time.RFC3339Nano))
		}
		return Timestamp{Value: v, Unit: unit}, nil
	case TimestampUnitNanos:
		const maxNanosSec = (math.MaxInt64 - 999_999_999) / 1_000_000_000
		const minNanosSec = (-math.MaxInt64 + 999_999_999) / 1_000_000_000
		if sec > maxNanosSec || sec < minNanosSec {
			return Timestamp{}, fmt.Errorf("%w: time.Time %s overflows TIMESTAMP_NS range", ErrOutOfRange, u.Format(time.RFC3339Nano))
		}
		v := sec*1_000_000_000 + nsec
		if v == TimestampInfinity || v == TimestampNegInfinity {
			return Timestamp{}, fmt.Errorf("%w: time.Time %s overflows TIMESTAMP_NS range", ErrOutOfRange, u.Format(time.RFC3339Nano))
		}
		return Timestamp{Value: v, Unit: unit}, nil
	default:
		return Timestamp{}, fmt.Errorf("%w: unknown timestamp unit %d", ErrInvalidInput, unit)
	}
}

// Time converts a finite Timestamp into UTC time.Time. Non-finite timestamps
// (±infinity) return ErrOutOfRange.
func (t Timestamp) Time() (time.Time, error) {
	if !t.IsFinite() {
		return time.Time{}, fmt.Errorf("%w: non-finite TIMESTAMP (%d) cannot be converted to time.Time", ErrOutOfRange, t.Value)
	}
	switch t.Unit {
	case TimestampUnitSeconds:
		return time.Unix(t.Value, 0).UTC(), nil
	case TimestampUnitMillis:
		return time.UnixMilli(t.Value).UTC(), nil
	case TimestampUnitMicros, TimestampUnitTZ:
		return time.UnixMicro(t.Value).UTC(), nil
	case TimestampUnitNanos:
		return time.Unix(0, t.Value).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("%w: unknown timestamp unit %d", ErrInvalidInput, t.Unit)
	}
}

// String formats t in RFC3339Nano (UTC), "infinity", or "-infinity".
func (t Timestamp) String() string {
	if t.IsInfinity() {
		return "infinity"
	}
	if t.IsNegInfinity() {
		return "-infinity"
	}
	tm, err := t.Time()
	if err != nil {
		return fmt.Sprintf("TIMESTAMP(%d, unit=%d)", t.Value, t.Unit)
	}
	return tm.Format(time.RFC3339Nano)
}

// Interval preserves DuckDB's three-component interval without collapsing
// months or days into fixed nanoseconds.
type Interval struct {
	Months int32
	Days   int32
	Micros int64
}

// String formats iv as a readable multi-part interval string.
func (iv Interval) String() string {
	var parts []string
	if iv.Months != 0 {
		parts = append(parts, fmt.Sprintf("%d months", iv.Months))
	}
	if iv.Days != 0 {
		parts = append(parts, fmt.Sprintf("%d days", iv.Days))
	}
	if iv.Micros != 0 || len(parts) == 0 {
		parts = append(parts, formatDayNanos(iv.Micros*1000, 6))
	}
	return strings.Join(parts, " ")
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

// StructFieldValue is one positional field of a decoded or bound SQL STRUCT.
type StructFieldValue struct {
	Name  string
	Type  LogicalType
	Value Value
}

// StructValue is an owned decoded SQL STRUCT preserving field order and names.
type StructValue struct {
	Fields []StructFieldValue
}

// MapEntry is one key/value pair of a decoded or bound SQL MAP.
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

// Bit is an owned SQL BIT string in DuckDB's wire layout: Data[0] stores the
// number of leading padding bits (0..7) in Data[1], followed by big-endian bit
// payload bytes.
type Bit struct {
	Data []byte
}

// Validate checks that b is a well-formed DuckDB BIT payload.
func (b Bit) Validate() error {
	if len(b.Data) == 0 {
		return fmt.Errorf("%w: empty BIT payload (missing padding header byte)", ErrInvalidInput)
	}
	if b.Data[0] > 7 {
		return fmt.Errorf("%w: invalid BIT padding byte %d (must be 0..7)", ErrInvalidInput, b.Data[0])
	}
	if len(b.Data) == 1 && b.Data[0] != 0 {
		return fmt.Errorf("%w: zero-length BIT has non-zero padding %d", ErrInvalidInput, b.Data[0])
	}
	return nil
}

// BitLen returns the number of logical bits in b.
func (b Bit) BitLen() int {
	if len(b.Data) <= 1 {
		return 0
	}
	return (len(b.Data)-1)*8 - int(b.Data[0])
}

// String formats b as a string of '0' and '1' characters.
func (b Bit) String() string {
	n := b.BitLen()
	if n <= 0 {
		return ""
	}
	pad := int(b.Data[0])
	out := make([]byte, n)
	for i := range n {
		bitPos := pad + i
		byteIdx := 1 + bitPos/8
		shift := uint(7 - (bitPos % 8))
		if b.Data[byteIdx]&(1<<shift) != 0 {
			out[i] = '1'
		} else {
			out[i] = '0'
		}
	}
	return string(out)
}

// NewBitFromString parses a string of '0' and '1' characters into a Bit.
func NewBitFromString(s string) (Bit, error) {
	if len(s) == 0 {
		return Bit{Data: []byte{0}}, nil
	}
	pad := (8 - (len(s) % 8)) % 8
	nbytes := (len(s) + pad) / 8
	data := make([]byte, 1+nbytes)
	data[0] = byte(pad)
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '0':
		case '1':
			bitPos := pad + i
			data[1+bitPos/8] |= 1 << uint(7-(bitPos%8))
		default:
			return Bit{}, fmt.Errorf("%w: invalid bit character %q at index %d", ErrInvalidInput, s[i], i)
		}
	}
	return Bit{Data: data}, nil
}

// Bignum is an immutable arbitrary-precision signed integer matching DuckDB's
// BIGNUM (formerly VARINT) SQL type.
type Bignum struct {
	neg bool
	mag []byte // big-endian unsigned magnitude without leading zeros; empty for 0
}

// BignumFromBigInt copies v into an immutable Bignum. v is borrowed.
func BignumFromBigInt(v *big.Int) (Bignum, error) {
	if v == nil {
		return Bignum{}, fmt.Errorf("%w: nil big.Int", ErrInvalidInput)
	}
	if v.Sign() == 0 {
		return Bignum{}, nil
	}
	return Bignum{
		neg: v.Sign() < 0,
		mag: v.Bytes(), // fresh big-endian slice
	}, nil
}

// BignumFromInt64 constructs a Bignum from an int64.
func BignumFromInt64(v int64) Bignum {
	bn, _ := BignumFromBigInt(big.NewInt(v))
	return bn
}

// IsNegative reports whether b < 0.
func (b Bignum) IsNegative() bool { return b.neg && len(b.mag) > 0 }

// BigInt returns a fresh owned *big.Int equal to b.
func (b Bignum) BigInt() *big.Int {
	v := new(big.Int).SetBytes(b.mag)
	if b.neg && len(b.mag) > 0 {
		v.Neg(v)
	}
	return v
}

// String formats b in base 10.
func (b Bignum) String() string {
	return b.BigInt().String()
}

// Geometry holds an owned WKB (Well-Known Binary) payload and optional CRS
// string from a DuckDB GEOMETRY column.
type Geometry struct {
	WKB []byte
	CRS string
}

// UnionValue holds the active tagged member of a decoded or bound SQL UNION.
// Type carries the union's full member LogicalType when decoded from DuckDB or
// constructed explicitly, enabling round-trip parameter binding.
type UnionValue struct {
	Type    LogicalType
	Tag     uint8
	TagName string
	Value   Value
}

// Variant holds the decoded inner value of a non-NULL DuckDB VARIANT row.
// Outer SQL NULL rows decode as Value.IsNull() == true (Kind() == TypeVariant),
// whereas an inner DuckDB VARIANT_NULL decodes as a non-NULL VARIANT whose
// Variant.IsNull() is true (Value.Kind() == TypeSQLNull).
type Variant struct {
	Value Value
}

// IsNull reports whether v holds an inner VARIANT_NULL value.
func (v Variant) IsNull() bool { return v.Value.IsNull() }

// Value holds one SQL parameter or decoded column value with exact type and
// explicit SQL NULL state. All composite/byte data inside a Value is owned by
// Go and never aliases native chunk memory.
//
// Value uses a compact 48-byte tagged representation on 64-bit platforms:
// fixed-width scalars (including 128-bit integers, Decimal, UUID, Interval,
// and temporal types) are stored inline in lo/hi without heap allocation, and
// complex/aliased metadata shares an immutable *LogicalType pointer.
type Value struct {
	lo   uint64
	hi   uint64
	ref  any
	lt   *LogicalType
	kind uint8
	null bool
	decW uint8
	decS uint8
	tsU  uint8
}

// IsNull reports whether v represents SQL NULL.
func (v Value) IsNull() bool { return v.null }

// Type returns v's LogicalType (TypeInvalid for untyped NullValue()).
func (v Value) Type() LogicalType {
	if v.lt != nil {
		return *v.lt
	}
	k := TypeKind(v.kind)
	if k == TypeDecimal {
		return DecimalType(v.decW, v.decS)
	}
	return ScalarType(k)
}

// Kind returns v.Type().Kind.
func (v Value) Kind() TypeKind { return TypeKind(v.kind) }

// IsJSON reports whether v has DuckDB's JSON logical type (VARCHAR with alias "JSON").
func (v Value) IsJSON() bool {
	return v.lt != nil && v.lt.IsJSON()
}

// NullValue constructs an untyped SQL NULL parameter value.
func NullValue() Value { return Value{null: true} }

// DefaultValue constructs a sentinel Value instructing Appender.AppendRow to
// insert the target column's SQL DEFAULT expression. Passing DefaultValue to
// prepared statement parameter binding returns ErrInvalidInput.
func DefaultValue() Value { return Value{kind: uint8(typeDefault)} }

// IsDefault reports whether v is the Appender column-default sentinel.
func (v Value) IsDefault() bool { return !v.null && TypeKind(v.kind) == typeDefault }

// TypedNullValue constructs a SQL NULL carrying a column's LogicalType.
func TypedNullValue(lt LogicalType) Value {
	v := Value{
		kind: uint8(lt.Kind),
		null: true,
		decW: lt.DecimalWidth,
		decS: lt.DecimalScale,
	}
	if lt.needsHeapMeta() {
		v.lt = cloneLogicalType(lt)
	}
	return v
}

func typedNullFromPlan(lt *LogicalType) Value {
	v := Value{
		kind: uint8(lt.Kind),
		null: true,
		decW: lt.DecimalWidth,
		decS: lt.DecimalScale,
	}
	if lt.needsHeapMeta() {
		v.lt = lt
	}
	return v
}

func BoolValue(v bool) Value {
	var b uint64
	if v {
		b = 1
	}
	return Value{kind: uint8(TypeBoolean), lo: b}
}

func Int8Value(v int8) Value     { return Value{kind: uint8(TypeTinyint), lo: uint64(int64(v))} }
func Int16Value(v int16) Value   { return Value{kind: uint8(TypeSmallint), lo: uint64(int64(v))} }
func Int32Value(v int32) Value   { return Value{kind: uint8(TypeInteger), lo: uint64(int64(v))} }
func Int64Value(v int64) Value   { return Value{kind: uint8(TypeBigint), lo: uint64(v)} }
func Uint8Value(v uint8) Value   { return Value{kind: uint8(TypeUTinyint), lo: uint64(v)} }
func Uint16Value(v uint16) Value { return Value{kind: uint8(TypeUSmallint), lo: uint64(v)} }
func Uint32Value(v uint32) Value { return Value{kind: uint8(TypeUInteger), lo: uint64(v)} }
func Uint64Value(v uint64) Value { return Value{kind: uint8(TypeUBigint), lo: v} }

func HugeintValue(v Hugeint) Value {
	return Value{kind: uint8(TypeHugeint), lo: v.Lower, hi: uint64(v.Upper)}
}

func UhugeintValue(v Uhugeint) Value {
	return Value{kind: uint8(TypeUhugeint), lo: v.Lower, hi: v.Upper}
}

func Float32Value(v float32) Value {
	return Value{kind: uint8(TypeFloat), lo: uint64(math.Float32bits(v))}
}

func Float64Value(v float64) Value {
	return Value{kind: uint8(TypeDouble), lo: math.Float64bits(v)}
}

func VarcharValue(v string) Value {
	return Value{kind: uint8(TypeVarchar), ref: v}
}

// JSONValue constructs a VARCHAR Value tagged with DuckDB's JSON logical type alias.
func JSONValue(v string) Value {
	return Value{kind: uint8(TypeVarchar), ref: v, lt: &jsonLogicalType}
}

func DateValue(v Date) Value     { return Value{kind: uint8(TypeDate), lo: uint64(int64(v.Days))} }
func TimeValue(v Time) Value     { return Value{kind: uint8(TypeTime), lo: uint64(v.Micros)} }
func TimeNSValue(v TimeNS) Value { return Value{kind: uint8(TypeTimeNS), lo: uint64(v.Nanos)} }

// TimeTZValue validates t and wraps it as a TIMETZ Value.
func TimeTZValue(t TimeTZ) (Value, error) {
	if err := t.Validate(); err != nil {
		return Value{}, err
	}
	return Value{
		kind: uint8(TypeTimeTZ),
		lo:   uint64(t.Micros),
		hi:   uint64(int64(t.OffsetSeconds)),
	}, nil
}

func IntervalValue(v Interval) Value {
	return Value{
		kind: uint8(TypeInterval),
		lo:   uint64(v.Micros),
		hi:   uint64(uint32(v.Months))<<32 | uint64(uint32(v.Days)),
	}
}

// UUIDValue wraps a 16-byte UUID as a Value.
func UUIDValue(u UUID) Value {
	return Value{
		kind: uint8(TypeUUID),
		lo:   binary.BigEndian.Uint64(u[0:8]),
		hi:   binary.BigEndian.Uint64(u[8:16]),
	}
}

// EnumValue validates index against lt's ENUM dictionary and constructs an ENUM Value.
func EnumValue(lt LogicalType, index uint32) (Value, error) {
	if lt.Kind != TypeEnum || len(lt.EnumValues) == 0 {
		return Value{}, fmt.Errorf("%w: EnumValue requires an ENUM LogicalType with at least one member", ErrInvalidInput)
	}
	if uint64(index) >= uint64(len(lt.EnumValues)) {
		return Value{}, fmt.Errorf("%w: enum index %d out of range [0,%d)", ErrOutOfRange, index, len(lt.EnumValues))
	}
	cloned := cloneLogicalType(lt)
	return Value{
		kind: uint8(TypeEnum),
		lo:   uint64(index),
		ref:  cloned.EnumValues[index],
		lt:   cloned,
	}, nil
}

// DecimalValue wraps a validated Decimal as a Value.
func DecimalValue(d Decimal) (Value, error) {
	if err := d.Validate(); err != nil {
		return Value{}, err
	}
	return Value{
		kind: uint8(TypeDecimal),
		decW: d.Width,
		decS: d.Scale,
		lo:   d.Value.Lower,
		hi:   uint64(d.Value.Upper),
	}, nil
}

// BlobValue copies b into an owned BLOB Value. An empty non-null BLOB is
// represented with a non-nil empty slice distinct from SQL NULL.
func BlobValue(b []byte) Value {
	cp := make([]byte, len(b))
	copy(cp, b)
	return Value{kind: uint8(TypeBlob), ref: cp}
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
	return Value{kind: uint8(kind), lo: uint64(ts.Value), tsU: uint8(ts.Unit)}, nil
}

func validateLogicalType(lt LogicalType, depth int) error {
	if depth > 64 {
		return fmt.Errorf("%w: logical type nesting depth exceeds maximum", ErrLimitExceeded)
	}
	switch lt.Kind {
	case TypeInvalid:
		return fmt.Errorf("%w: invalid logical type", ErrInvalidInput)
	case TypeDecimal:
		if lt.DecimalWidth < 1 || lt.DecimalWidth > 38 || lt.DecimalScale > lt.DecimalWidth {
			return fmt.Errorf("%w: invalid DECIMAL(%d,%d)", ErrInvalidInput, lt.DecimalWidth, lt.DecimalScale)
		}
		return nil
	case TypeEnum:
		if len(lt.EnumValues) == 0 {
			return fmt.Errorf("%w: ENUM requires at least one dictionary member", ErrInvalidInput)
		}
		return nil
	case TypeList:
		if lt.Elem == nil {
			return fmt.Errorf("%w: LIST requires non-nil Elem type", ErrInvalidInput)
		}
		return validateLogicalType(*lt.Elem, depth+1)
	case TypeArray:
		if lt.Elem == nil || lt.ArraySize == 0 {
			return fmt.Errorf("%w: ARRAY requires non-nil Elem type and positive ArraySize", ErrInvalidInput)
		}
		return validateLogicalType(*lt.Elem, depth+1)
	case TypeMap:
		if lt.Key == nil || lt.Value == nil {
			return fmt.Errorf("%w: MAP requires non-nil Key and Value types", ErrInvalidInput)
		}
		if err := validateLogicalType(*lt.Key, depth+1); err != nil {
			return err
		}
		return validateLogicalType(*lt.Value, depth+1)
	case TypeStruct, TypeUnion:
		if len(lt.Fields) == 0 {
			return fmt.Errorf("%w: %s requires at least one member", ErrInvalidInput, lt.Kind)
		}
		if lt.Kind == TypeUnion && len(lt.Fields) > 256 {
			return fmt.Errorf("%w: UNION member count %d exceeds 256", ErrInvalidInput, len(lt.Fields))
		}
		for i, f := range lt.Fields {
			if f.Name == "" {
				return fmt.Errorf("%w: %s member %d has empty name", ErrInvalidInput, lt.Kind, i)
			}
			if err := validateLogicalType(f.Type, depth+1); err != nil {
				return err
			}
		}
		return nil
	default:
		return nil
	}
}

func checkElementType(elem Value, want LogicalType) error {
	if elem.IsNull() {
		if elem.Kind() != TypeInvalid && elem.Kind() != TypeSQLNull && !elem.Type().Equal(want) {
			return fmt.Errorf("%w: null element has type %s, want %s", ErrTypeMismatch, elem.Type(), want)
		}
		return nil
	}
	if !elem.Type().Equal(want) {
		return fmt.Errorf("%w: element has type %s, want %s", ErrTypeMismatch, elem.Type(), want)
	}
	return nil
}

// NewListValue validates elemType and elements and constructs an owned SQL LIST
// Value suitable for parameter binding. An empty elements slice produces a
// non-null empty list distinct from SQL NULL.
func NewListValue(elemType LogicalType, elements []Value) (Value, error) {
	if err := validateLogicalType(elemType, 0); err != nil {
		return Value{}, err
	}
	for i, elem := range elements {
		if err := checkElementType(elem, elemType); err != nil {
			return Value{}, fmt.Errorf("list[%d]: %w", i, err)
		}
	}
	lt := cloneLogicalType(ListType(elemType))
	cp := make([]Value, len(elements))
	copy(cp, elements)
	return Value{kind: uint8(TypeList), ref: cp, lt: lt}, nil
}

// NewArrayValue validates elemType and elements and constructs an owned
// fixed-length SQL ARRAY Value suitable for parameter binding.
func NewArrayValue(elemType LogicalType, elements []Value) (Value, error) {
	if len(elements) == 0 {
		return Value{}, fmt.Errorf("%w: ARRAY requires at least one element", ErrInvalidInput)
	}
	if err := validateLogicalType(elemType, 0); err != nil {
		return Value{}, err
	}
	for i, elem := range elements {
		if err := checkElementType(elem, elemType); err != nil {
			return Value{}, fmt.Errorf("array[%d]: %w", i, err)
		}
	}
	lt := cloneLogicalType(ArrayType(elemType, uint64(len(elements))))
	cp := make([]Value, len(elements))
	copy(cp, elements)
	return Value{kind: uint8(TypeArray), ref: cp, lt: lt}, nil
}

// NewStructValue validates fields and constructs an owned SQL STRUCT Value
// suitable for parameter binding. When a field's Type is zero (TypeInvalid) and
// its Value is non-null, the field Type is inferred from Value.Type().
func NewStructValue(fields []StructFieldValue) (Value, error) {
	if len(fields) == 0 {
		return Value{}, fmt.Errorf("%w: STRUCT requires at least one field", ErrInvalidInput)
	}
	ftypes := make([]StructFieldType, len(fields))
	cp := make([]StructFieldValue, len(fields))
	for i, f := range fields {
		if f.Name == "" {
			return Value{}, fmt.Errorf("%w: struct field %d has empty name", ErrInvalidInput, i)
		}
		ft := f.Type
		if ft.Kind == TypeInvalid {
			if f.Value.IsNull() && f.Value.Kind() == TypeInvalid {
				return Value{}, fmt.Errorf("%w: struct field %q requires an explicit Type when Value is untyped NULL", ErrInvalidInput, f.Name)
			}
			ft = f.Value.Type()
		}
		if err := validateLogicalType(ft, 0); err != nil {
			return Value{}, fmt.Errorf("struct field %q: %w", f.Name, err)
		}
		if err := checkElementType(f.Value, ft); err != nil {
			return Value{}, fmt.Errorf("struct field %q: %w", f.Name, err)
		}
		clonedFT := *cloneLogicalType(ft)
		ftypes[i] = StructFieldType{Name: f.Name, Type: clonedFT}
		cp[i] = StructFieldValue{Name: f.Name, Type: clonedFT, Value: f.Value}
	}
	lt := &LogicalType{Kind: TypeStruct, Fields: ftypes}
	return Value{kind: uint8(TypeStruct), ref: cp, lt: lt}, nil
}

// NewMapValue validates keyType, valueType, and entries and constructs an owned
// SQL MAP Value suitable for parameter binding. SQL NULL keys are rejected.
func NewMapValue(keyType, valueType LogicalType, entries []MapEntry) (Value, error) {
	if err := validateLogicalType(keyType, 0); err != nil {
		return Value{}, err
	}
	if err := validateLogicalType(valueType, 0); err != nil {
		return Value{}, err
	}
	for i, e := range entries {
		if e.Key.IsNull() {
			return Value{}, fmt.Errorf("%w: map[%d] key cannot be SQL NULL", ErrInvalidInput, i)
		}
		if !e.Key.Type().Equal(keyType) {
			return Value{}, fmt.Errorf("map[%d] key: %w: have %s, want %s", i, ErrTypeMismatch, e.Key.Type(), keyType)
		}
		if err := checkElementType(e.Value, valueType); err != nil {
			return Value{}, fmt.Errorf("map[%d] value: %w", i, err)
		}
	}
	lt := cloneLogicalType(MapType(keyType, valueType))
	cp := make([]MapEntry, len(entries))
	copy(cp, entries)
	return Value{kind: uint8(TypeMap), ref: cp, lt: lt}, nil
}

// BitValue validates b and copies its bytes into an owned BIT Value.
func BitValue(b Bit) (Value, error) {
	if err := b.Validate(); err != nil {
		return Value{}, err
	}
	cp := make([]byte, len(b.Data))
	copy(cp, b.Data)
	return Value{kind: uint8(TypeBit), ref: cp}, nil
}

// BignumValue copies b into an owned BIGNUM Value.
func BignumValue(b Bignum) Value {
	var neg uint64
	if b.IsNegative() {
		neg = 1
	}
	var cp []byte
	if len(b.mag) > 0 {
		cp = make([]byte, len(b.mag))
		copy(cp, b.mag)
	}
	return Value{kind: uint8(TypeBignum), lo: neg, ref: cp}
}

// GeometryValue copies g.WKB into an owned GEOMETRY Value.
func GeometryValue(g Geometry) Value {
	cp := make([]byte, len(g.WKB))
	copy(cp, g.WKB)
	var lt *LogicalType
	if g.CRS != "" {
		lt = cloneLogicalType(GeometryType(g.CRS))
	}
	return Value{kind: uint8(TypeGeometry), ref: cp, lt: lt}
}

// VariantValue wraps a decoded inner Value as a non-NULL TypeVariant Value.
// Passing NullValue() (or TypedNullValue(ScalarType(TypeSQLNull))) represents
// an inner VARIANT_NULL distinct from an outer SQL NULL (TypedNullValue(ScalarType(TypeVariant))).
func VariantValue(inner Value) Value {
	return Value{kind: uint8(TypeVariant), ref: inner}
}

// NewUnionValue validates unionType, tag, and member value and constructs an
// owned SQL UNION Value suitable for parameter binding.
func NewUnionValue(unionType LogicalType, tag uint8, val Value) (Value, error) {
	if unionType.Kind != TypeUnion {
		return Value{}, fmt.Errorf("%w: NewUnionValue requires a UNION LogicalType", ErrInvalidInput)
	}
	if err := validateLogicalType(unionType, 0); err != nil {
		return Value{}, err
	}
	if int(tag) >= len(unionType.Fields) {
		return Value{}, fmt.Errorf("%w: union tag %d out of range [0,%d)", ErrOutOfRange, tag, len(unionType.Fields))
	}
	member := unionType.Fields[tag]
	if err := checkElementType(val, member.Type); err != nil {
		return Value{}, fmt.Errorf("union member %q: %w", member.Name, err)
	}
	return Value{
		kind: uint8(TypeUnion),
		lo:   uint64(tag),
		ref:  val,
		lt:   cloneLogicalType(unionType),
	}, nil
}

func (v Value) check(want TypeKind) error {
	if v.null {
		return ErrNullValue
	}
	if TypeKind(v.kind) != want {
		return fmt.Errorf("%w: have %s, want %s", ErrTypeMismatch, TypeKind(v.kind), want)
	}
	return nil
}

func (v Value) Bool() (bool, error) {
	if err := v.check(TypeBoolean); err != nil {
		return false, err
	}
	return v.lo != 0, nil
}

func (v Value) Int8() (int8, error) {
	if err := v.check(TypeTinyint); err != nil {
		return 0, err
	}
	return int8(int64(v.lo)), nil
}

func (v Value) Int16() (int16, error) {
	if err := v.check(TypeSmallint); err != nil {
		return 0, err
	}
	return int16(int64(v.lo)), nil
}

func (v Value) Int32() (int32, error) {
	if err := v.check(TypeInteger); err != nil {
		return 0, err
	}
	return int32(int64(v.lo)), nil
}

func (v Value) Int64() (int64, error) {
	if err := v.check(TypeBigint); err != nil {
		return 0, err
	}
	return int64(v.lo), nil
}

func (v Value) Uint8() (uint8, error) {
	if err := v.check(TypeUTinyint); err != nil {
		return 0, err
	}
	return uint8(v.lo), nil
}

func (v Value) Uint16() (uint16, error) {
	if err := v.check(TypeUSmallint); err != nil {
		return 0, err
	}
	return uint16(v.lo), nil
}

func (v Value) Uint32() (uint32, error) {
	if err := v.check(TypeUInteger); err != nil {
		return 0, err
	}
	return uint32(v.lo), nil
}

func (v Value) Uint64() (uint64, error) {
	if err := v.check(TypeUBigint); err != nil {
		return 0, err
	}
	return v.lo, nil
}

// AsInt64 converts any exact integer Value (signed, unsigned, or 128-bit) to
// int64 with strict range checking. Non-integer types fail with ErrTypeMismatch.
func (v Value) AsInt64() (int64, error) {
	if v.null {
		return 0, ErrNullValue
	}
	switch TypeKind(v.kind) {
	case TypeTinyint, TypeSmallint, TypeInteger, TypeBigint:
		return int64(v.lo), nil
	case TypeUTinyint, TypeUSmallint, TypeUInteger, TypeUBigint:
		if v.lo > math.MaxInt64 {
			return 0, fmt.Errorf("%w: uint value %d exceeds int64", ErrOutOfRange, v.lo)
		}
		return int64(v.lo), nil
	case TypeHugeint:
		h := Hugeint{Lower: v.lo, Upper: int64(v.hi)}
		n, ok := h.Int64()
		if !ok {
			return 0, fmt.Errorf("%w: HUGEINT %s outside int64", ErrOutOfRange, h.String())
		}
		return n, nil
	case TypeUhugeint:
		u := Uhugeint{Lower: v.lo, Upper: v.hi}
		n, ok := u.Uint64()
		if !ok || n > math.MaxInt64 {
			return 0, fmt.Errorf("%w: UHUGEINT %s exceeds int64", ErrOutOfRange, u.String())
		}
		return int64(n), nil
	default:
		return 0, fmt.Errorf("%w: %s is not an integer", ErrTypeMismatch, TypeKind(v.kind))
	}
}

// AsUint64 converts any exact integer Value to uint64 with strict range checking.
func (v Value) AsUint64() (uint64, error) {
	if v.null {
		return 0, ErrNullValue
	}
	switch TypeKind(v.kind) {
	case TypeTinyint, TypeSmallint, TypeInteger, TypeBigint:
		n := int64(v.lo)
		if n < 0 {
			return 0, fmt.Errorf("%w: negative integer %d", ErrOutOfRange, n)
		}
		return uint64(n), nil
	case TypeUTinyint, TypeUSmallint, TypeUInteger, TypeUBigint:
		return v.lo, nil
	case TypeHugeint:
		h := Hugeint{Lower: v.lo, Upper: int64(v.hi)}
		if h.Upper != 0 {
			return 0, fmt.Errorf("%w: HUGEINT %s outside uint64", ErrOutOfRange, h.String())
		}
		return h.Lower, nil
	case TypeUhugeint:
		u := Uhugeint{Lower: v.lo, Upper: v.hi}
		n, ok := u.Uint64()
		if !ok {
			return 0, fmt.Errorf("%w: UHUGEINT %s exceeds uint64", ErrOutOfRange, u.String())
		}
		return n, nil
	default:
		return 0, fmt.Errorf("%w: %s is not an integer", ErrTypeMismatch, TypeKind(v.kind))
	}
}

func (v Value) Hugeint() (Hugeint, error) {
	if err := v.check(TypeHugeint); err != nil {
		return Hugeint{}, err
	}
	return Hugeint{Lower: v.lo, Upper: int64(v.hi)}, nil
}

func (v Value) Uhugeint() (Uhugeint, error) {
	if err := v.check(TypeUhugeint); err != nil {
		return Uhugeint{}, err
	}
	return Uhugeint{Lower: v.lo, Upper: v.hi}, nil
}

func (v Value) Decimal() (Decimal, error) {
	if err := v.check(TypeDecimal); err != nil {
		return Decimal{}, err
	}
	return Decimal{
		Width: v.decW,
		Scale: v.decS,
		Value: Hugeint{Lower: v.lo, Upper: int64(v.hi)},
	}, nil
}

func (v Value) Float32() (float32, error) {
	if err := v.check(TypeFloat); err != nil {
		return 0, err
	}
	return math.Float32frombits(uint32(v.lo)), nil
}

func (v Value) Float64() (float64, error) {
	if err := v.check(TypeDouble); err != nil {
		return 0, err
	}
	return math.Float64frombits(v.lo), nil
}

func (v Value) Varchar() (string, error) {
	if err := v.check(TypeVarchar); err != nil {
		return "", err
	}
	s, _ := v.ref.(string)
	return s, nil
}

// Blob returns a fresh owned byte slice copy.
func (v Value) Blob() ([]byte, error) {
	if err := v.check(TypeBlob); err != nil {
		return nil, err
	}
	src, _ := v.ref.([]byte)
	cp := make([]byte, len(src))
	copy(cp, src)
	return cp, nil
}

func (v Value) Date() (Date, error) {
	if err := v.check(TypeDate); err != nil {
		return Date{}, err
	}
	return Date{Days: int32(int64(v.lo))}, nil
}

func (v Value) Time() (Time, error) {
	if err := v.check(TypeTime); err != nil {
		return Time{}, err
	}
	return Time{Micros: int64(v.lo)}, nil
}

func (v Value) TimeNS() (TimeNS, error) {
	if err := v.check(TypeTimeNS); err != nil {
		return TimeNS{}, err
	}
	return TimeNS{Nanos: int64(v.lo)}, nil
}

func (v Value) TimeTZ() (TimeTZ, error) {
	if err := v.check(TypeTimeTZ); err != nil {
		return TimeTZ{}, err
	}
	return TimeTZ{
		Micros:        int64(v.lo),
		OffsetSeconds: int32(int64(v.hi)),
	}, nil
}

func (v Value) Timestamp() (Timestamp, error) {
	if v.null {
		return Timestamp{}, ErrNullValue
	}
	switch TypeKind(v.kind) {
	case TypeTimestamp, TypeTimestampS, TypeTimestampMS, TypeTimestampNS, TypeTimestampTZ:
		return Timestamp{Value: int64(v.lo), Unit: TimestampUnit(v.tsU)}, nil
	default:
		return Timestamp{}, fmt.Errorf("%w: have %s, want TIMESTAMP*", ErrTypeMismatch, TypeKind(v.kind))
	}
}

func (v Value) Interval() (Interval, error) {
	if err := v.check(TypeInterval); err != nil {
		return Interval{}, err
	}
	return Interval{
		Months: int32(uint32(v.hi >> 32)),
		Days:   int32(uint32(v.hi)),
		Micros: int64(v.lo),
	}, nil
}

func (v Value) UUID() (UUID, error) {
	if err := v.check(TypeUUID); err != nil {
		return UUID{}, err
	}
	var u UUID
	binary.BigEndian.PutUint64(u[0:8], v.lo)
	binary.BigEndian.PutUint64(u[8:16], v.hi)
	return u, nil
}

func (v Value) Enum() (Enum, error) {
	if err := v.check(TypeEnum); err != nil {
		return Enum{}, err
	}
	label, _ := v.ref.(string)
	return Enum{Index: uint32(v.lo), Label: label}, nil
}

func (v Value) List() (ListValue, error) {
	if err := v.check(TypeList); err != nil {
		return ListValue{}, err
	}
	elems, _ := v.ref.([]Value)
	var elemType LogicalType
	if v.lt != nil && v.lt.Elem != nil {
		elemType = *v.lt.Elem
	}
	return ListValue{
		ElemType: elemType,
		Elements: append([]Value{}, elems...),
	}, nil
}

func (v Value) Array() (ArrayValue, error) {
	if err := v.check(TypeArray); err != nil {
		return ArrayValue{}, err
	}
	elems, _ := v.ref.([]Value)
	var elemType LogicalType
	var size uint64
	if v.lt != nil {
		size = v.lt.ArraySize
		if v.lt.Elem != nil {
			elemType = *v.lt.Elem
		}
	}
	return ArrayValue{
		ElemType: elemType,
		Size:     size,
		Elements: append([]Value{}, elems...),
	}, nil
}

func (v Value) Struct() (StructValue, error) {
	if err := v.check(TypeStruct); err != nil {
		return StructValue{}, err
	}
	fields, _ := v.ref.([]StructFieldValue)
	return StructValue{
		Fields: append([]StructFieldValue{}, fields...),
	}, nil
}

func (v Value) Map() (MapValue, error) {
	if err := v.check(TypeMap); err != nil {
		return MapValue{}, err
	}
	entries, _ := v.ref.([]MapEntry)
	var keyType, valType LogicalType
	if v.lt != nil {
		if v.lt.Key != nil {
			keyType = *v.lt.Key
		}
		if v.lt.Value != nil {
			valType = *v.lt.Value
		}
	}
	return MapValue{
		KeyType:   keyType,
		ValueType: valType,
		Entries:   append([]MapEntry{}, entries...),
	}, nil
}

// Bit returns an owned Bit copy.
func (v Value) Bit() (Bit, error) {
	if err := v.check(TypeBit); err != nil {
		return Bit{}, err
	}
	src, _ := v.ref.([]byte)
	cp := make([]byte, len(src))
	copy(cp, src)
	return Bit{Data: cp}, nil
}

// Bignum returns an immutable Bignum copy.
func (v Value) Bignum() (Bignum, error) {
	if err := v.check(TypeBignum); err != nil {
		return Bignum{}, err
	}
	src, _ := v.ref.([]byte)
	var cp []byte
	if len(src) > 0 {
		cp = make([]byte, len(src))
		copy(cp, src)
	}
	return Bignum{neg: v.lo != 0 && len(cp) > 0, mag: cp}, nil
}

// Geometry returns an owned Geometry copy.
func (v Value) Geometry() (Geometry, error) {
	if err := v.check(TypeGeometry); err != nil {
		return Geometry{}, err
	}
	src, _ := v.ref.([]byte)
	cp := make([]byte, len(src))
	copy(cp, src)
	var crs string
	if v.lt != nil {
		crs = v.lt.GeometryCRS
	}
	return Geometry{WKB: cp, CRS: crs}, nil
}

// Union returns the active tagged member of a SQL UNION Value.
func (v Value) Union() (UnionValue, error) {
	if err := v.check(TypeUnion); err != nil {
		return UnionValue{}, err
	}
	tag := uint8(v.lo)
	var (
		tagName string
		lt      LogicalType
	)
	if v.lt != nil {
		lt = *cloneLogicalType(*v.lt)
		if int(tag) < len(v.lt.Fields) {
			tagName = v.lt.Fields[tag].Name
		}
	}
	inner, _ := v.ref.(Value)
	return UnionValue{
		Type:    lt,
		Tag:     tag,
		TagName: tagName,
		Value:   inner,
	}, nil
}

// Variant returns the decoded inner Variant payload. Returns ErrNullValue for
// outer SQL NULL VARIANT rows; inner VARIANT_NULL rows return a Variant with
// Variant.IsNull() == true.
func (v Value) Variant() (Variant, error) {
	if err := v.check(TypeVariant); err != nil {
		return Variant{}, err
	}
	inner, ok := v.ref.(Value)
	if !ok {
		return Variant{Value: TypedNullValue(ScalarType(TypeSQLNull))}, nil
	}
	return Variant{Value: inner}, nil
}
