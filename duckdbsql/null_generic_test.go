package duckdbsql

import (
	"database/sql"
	"database/sql/driver"
	"math/big"
	"reflect"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
)

func TestSQLNullGenericToDuckDBValue(t *testing.T) {
	u, err := duckdb.ParseUUID("12345678-1234-5678-9abc-def012345678")
	if err != nil {
		t.Fatal(err)
	}
	dec := duckdb.Decimal{Width: 10, Scale: 2, Value: duckdb.HugeintFromInt64(12345)}
	h := duckdb.HugeintFromInt64(-999)
	d := duckdb.Date{Days: 20000}
	ts := duckdb.Timestamp{Unit: duckdb.TimestampUnitMicros, Value: 1_700_000_000_000_000}
	ttz := duckdb.TimeTZ{Micros: 3600_000_000, OffsetSeconds: 7200}
	iv := duckdb.Interval{Months: 1, Days: 2, Micros: 3}
	bitVal, err := duckdb.NewBitFromString("10110")
	if err != nil {
		t.Fatal(err)
	}
	bn, err := duckdb.BignumFromBigInt(big.NewInt(987654321))
	if err != nil {
		t.Fatal(err)
	}
	geom := duckdb.Geometry{WKB: []byte{0x01, 0x01, 0x00, 0x00, 0x00}}

	// Verify Geometry driver.Valuer implementation (Decimal, Timestamp, and
	// UnionValue already expose a Value struct field so Go forbids a Value()
	// method on them; sql.Null[T] unwrapping in toDuckDBValueDepth handles them
	// directly without relying on driver.DefaultParameterConverter).
	var geomValuer driver.Valuer = geom
	if got, err := geomValuer.Value(); err != nil || got == nil {
		t.Fatalf("Geometry.Value() = (%v, %v), want non-nil", got, err)
	}

	// Valid sql.Null[T] values (including pointer to sql.Null[T]) convert without DefaultParameterConverter error.
	cases := []struct {
		name     string
		in       any
		wantKind duckdb.TypeKind
		wantNull bool
	}{
		{"NullUUID valid", sql.Null[duckdb.UUID]{V: u, Valid: true}, duckdb.TypeUUID, false},
		{"NullUUID invalid", sql.Null[duckdb.UUID]{}, duckdb.TypeSQLNull, true},
		{"PtrNullUUID valid", &sql.Null[duckdb.UUID]{V: u, Valid: true}, duckdb.TypeUUID, false},
		{"PtrNullUUID invalid", &sql.Null[duckdb.UUID]{}, duckdb.TypeSQLNull, true},
		{"NullDecimal valid", sql.Null[duckdb.Decimal]{V: dec, Valid: true}, duckdb.TypeDecimal, false},
		{"NullHugeint valid", sql.Null[duckdb.Hugeint]{V: h, Valid: true}, duckdb.TypeHugeint, false},
		{"NullDate valid", sql.Null[duckdb.Date]{V: d, Valid: true}, duckdb.TypeDate, false},
		{"NullTimestamp valid", sql.Null[duckdb.Timestamp]{V: ts, Valid: true}, duckdb.TypeTimestamp, false},
		{"NullTimeTZ valid", sql.Null[duckdb.TimeTZ]{V: ttz, Valid: true}, duckdb.TypeTimeTZ, false},
		{"NullInterval valid", sql.Null[duckdb.Interval]{V: iv, Valid: true}, duckdb.TypeInterval, false},
		{"NullBit valid", sql.Null[duckdb.Bit]{V: bitVal, Valid: true}, duckdb.TypeBit, false},
		{"NullBignum valid", sql.Null[duckdb.Bignum]{V: bn, Valid: true}, duckdb.TypeBignum, false},
		{"NullGeometry valid", sql.Null[duckdb.Geometry]{V: geom, Valid: true}, duckdb.TypeGeometry, false},
		{"NullSlice valid", sql.Null[[]int32]{V: []int32{1, 2}, Valid: true}, duckdb.TypeList, false},
		{"NullSlice invalid", sql.Null[[]int32]{}, duckdb.TypeSQLNull, true},
		{"SliceOfNullUUID", []sql.Null[duckdb.UUID]{{V: u, Valid: true}, {}}, duckdb.TypeList, false},
		{"EmptySliceOfNullUUID", []sql.Null[duckdb.UUID]{}, duckdb.TypeList, false},
	}
	for _, tc := range cases {
		got, err := toDuckDBValue(tc.in)
		if err != nil {
			t.Fatalf("%s: toDuckDBValue error: %v", tc.name, err)
		}
		if got.IsNull() != tc.wantNull {
			t.Fatalf("%s: IsNull() = %v, want %v", tc.name, got.IsNull(), tc.wantNull)
		}
		if !tc.wantNull && got.Kind() != tc.wantKind {
			t.Fatalf("%s: Kind() = %s, want %s", tc.name, got.Kind(), tc.wantKind)
		}
	}

	// Verify inferLogicalTypeFromGoType handles sql.Null[T] and duckdb.Geometry.
	if lt, err := inferLogicalTypeFromGoType(reflect.TypeFor[sql.Null[duckdb.UUID]](), 0); err != nil || lt.Kind != duckdb.TypeUUID {
		t.Fatalf("infer(sql.Null[UUID]) = (%v, %v), want UUID", lt, err)
	}
	if lt, err := inferLogicalTypeFromGoType(reflect.TypeFor[duckdb.Geometry](), 0); err != nil || lt.Kind != duckdb.TypeGeometry {
		t.Fatalf("infer(Geometry) = (%v, %v), want GEOMETRY", lt, err)
	}
}
