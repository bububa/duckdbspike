package duckdb_test

import (
	"bytes"
	"context"
	"errors"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
)

func querySingleRow(t *testing.T, conn *duckdb.Conn, sql string, args ...duckdb.Value) ([]duckdb.Column, []duckdb.Value) {
	t.Helper()
	rows, err := conn.Query(context.Background(), sql, args...)
	if err != nil {
		t.Fatalf("Query(%q): %v", sql, err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("Query(%q): expected 1 row, got 0 (err=%v)", sql, rows.Err())
	}
	vals, err := rows.DecodeRow(nil)
	if err != nil {
		t.Fatalf("DecodeRow(%q): %v", sql, err)
	}
	if rows.Next() {
		t.Fatalf("Query(%q): expected 1 row, got more", sql)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err(%q): %v", sql, err)
	}
	cols := rows.Columns()
	if err := rows.Close(); err != nil {
		t.Fatalf("Close(%q): %v", sql, err)
	}
	return cols, vals
}

func TestNativeBindAndDecodeScalarBoundaries(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()
	db, err := lib.Open(ctx, ":memory:", duckdb.Option{Name: "threads", Value: "2"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	min128Big, _ := new(big.Int).SetString("-170141183460469231731687303715884105728", 10)
	max128Big, _ := new(big.Int).SetString("170141183460469231731687303715884105727", 10)
	umax128Big, _ := new(big.Int).SetString("340282366920938463463374607431768211455", 10)
	min128, _ := duckdb.HugeintFromBigInt(min128Big)
	max128, _ := duckdb.HugeintFromBigInt(max128Big)
	umax128, _ := duckdb.UhugeintFromBigInt(umax128Big)

	d4, _ := duckdb.DecimalFromInt64(4, 2, 1234)
	d9, _ := duckdb.DecimalFromInt64(9, 4, -123456789)
	d18, _ := duckdb.DecimalFromInt64(18, 6, 123456789012345678)
	d38Coef, _ := new(big.Int).SetString("-12345678901234567890123456780123456789", 10)
	d38, _ := duckdb.NewDecimal(38, 10, d38Coef)
	vD4, _ := duckdb.DecimalValue(d4)
	vD9, _ := duckdb.DecimalValue(d9)
	vD18, _ := duckdb.DecimalValue(d18)
	vD38, _ := duckdb.DecimalValue(d38)

	tsSPreNS, _ := duckdb.TimestampValue(duckdb.Timestamp{Value: -9223372037, Unit: duckdb.TimestampUnitSeconds})
	tsSPostNS, _ := duckdb.TimestampValue(duckdb.Timestamp{Value: 253402300799, Unit: duckdb.TimestampUnitSeconds}) // 9999-12-31 23:59:59
	tsMS, _ := duckdb.TimestampValue(duckdb.Timestamp{Value: 1795352400123, Unit: duckdb.TimestampUnitMillis})
	tsUS, _ := duckdb.TimestampValue(duckdb.Timestamp{Value: 1795352400123456, Unit: duckdb.TimestampUnitMicros})
	tsNS, _ := duckdb.TimestampValue(duckdb.Timestamp{Value: 1795352400123456789, Unit: duckdb.TimestampUnitNanos})
	tsTZ, _ := duckdb.TimestampValue(duckdb.Timestamp{Value: 1795352400654321, Unit: duckdb.TimestampUnitTZ})

	args := []duckdb.Value{
		duckdb.BoolValue(true),
		duckdb.Int8Value(math.MinInt8),
		duckdb.Int16Value(math.MinInt16),
		duckdb.Int32Value(math.MinInt32),
		duckdb.Int64Value((1 << 53) + 1),
		duckdb.Int64Value(math.MinInt64),
		duckdb.Uint8Value(math.MaxUint8),
		duckdb.Uint16Value(math.MaxUint16),
		duckdb.Uint32Value(math.MaxUint32),
		duckdb.Uint64Value(math.MaxUint64),
		duckdb.HugeintValue(min128),
		duckdb.HugeintValue(max128),
		duckdb.UhugeintValue(umax128),
		vD4, vD9, vD18, vD38,
		duckdb.Float32Value(1.5),
		duckdb.Float64Value(2.25),
		duckdb.VarcharValue(""),
		duckdb.VarcharValue("a\x00b"),
		duckdb.VarcharValue("0123456789ab"), // exact 12-byte inline boundary
		duckdb.VarcharValue("0123456789abc\x00" + strings.Repeat("z", 40)),
		duckdb.BlobValue(nil),
		duckdb.BlobValue([]byte{0x00, 0xff, 0x10}),
		duckdb.BlobValue(bytes.Repeat([]byte{0xde, 0xad, 0x00}, 10)),
		duckdb.DateValue(duckdb.Date{Days: -719162}), // year 0001
		duckdb.TimeValue(duckdb.Time{Micros: 86399_999999}),
		duckdb.TimeNSValue(duckdb.TimeNS{Nanos: 86399_999999999}),
		tsSPreNS, tsSPostNS, tsMS, tsUS, tsNS, tsTZ,
		duckdb.IntervalValue(duckdb.Interval{Months: -14, Days: 31, Micros: 987654321}),
		duckdb.NullValue(),
	}
	placeholders := make([]string, len(args))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	// Cast the final untyped NULL parameter to BIGINT so the column has a concrete type.
	placeholders[len(placeholders)-1] = "?::BIGINT"
	sql := "SELECT " + strings.Join(placeholders, ", ")
	_, vals := querySingleRow(t, conn, sql, args...)

	if b, err := vals[0].Bool(); err != nil || !b {
		t.Fatalf("bool: %v %v", b, err)
	}
	if v, err := vals[1].Int8(); err != nil || v != math.MinInt8 {
		t.Fatalf("int8: %v %v", v, err)
	}
	if v, err := vals[2].Int16(); err != nil || v != math.MinInt16 {
		t.Fatalf("int16: %v %v", v, err)
	}
	if v, err := vals[3].Int32(); err != nil || v != math.MinInt32 {
		t.Fatalf("int32: %v %v", v, err)
	}
	if v, err := vals[4].Int64(); err != nil || v != (1<<53)+1 {
		t.Fatalf("2^53+1: %v %v", v, err)
	}
	if v, err := vals[5].Int64(); err != nil || v != math.MinInt64 {
		t.Fatalf("int64 min: %v %v", v, err)
	}
	if v, err := vals[6].Uint8(); err != nil || v != math.MaxUint8 {
		t.Fatalf("uint8: %v %v", v, err)
	}
	if v, err := vals[7].Uint16(); err != nil || v != math.MaxUint16 {
		t.Fatalf("uint16: %v %v", v, err)
	}
	if v, err := vals[8].Uint32(); err != nil || v != math.MaxUint32 {
		t.Fatalf("uint32: %v %v", v, err)
	}
	if v, err := vals[9].Uint64(); err != nil || v != math.MaxUint64 {
		t.Fatalf("uint64 max: %v %v", v, err)
	}
	if v, err := vals[10].Hugeint(); err != nil || v != min128 {
		t.Fatalf("hugeint min: %v %v", v, err)
	}
	if v, err := vals[11].Hugeint(); err != nil || v != max128 {
		t.Fatalf("hugeint max: %v %v", v, err)
	}
	if v, err := vals[12].Uhugeint(); err != nil || v != umax128 {
		t.Fatalf("uhugeint max: %v %v", v, err)
	}
	for idx, want := range []duckdb.Decimal{d4, d9, d18, d38} {
		got, err := vals[13+idx].Decimal()
		if err != nil || got != want {
			t.Fatalf("decimal[%d]: got %+v (%v), want %+v", idx, got, err, want)
		}
	}
	if v, err := vals[17].Float32(); err != nil || v != 1.5 {
		t.Fatalf("float32: %v %v", v, err)
	}
	if v, err := vals[18].Float64(); err != nil || v != 2.25 {
		t.Fatalf("float64: %v %v", v, err)
	}
	for idx, want := range []string{"", "a\x00b", "0123456789ab", "0123456789abc\x00" + strings.Repeat("z", 40)} {
		got, err := vals[19+idx].Varchar()
		if err != nil || got != want {
			t.Fatalf("varchar[%d]: got %q (%v), want %q", idx, got, err, want)
		}
	}
	for idx, want := range [][]byte{{}, {0x00, 0xff, 0x10}, bytes.Repeat([]byte{0xde, 0xad, 0x00}, 10)} {
		if vals[23+idx].IsNull() {
			t.Fatalf("blob[%d] unexpectedly NULL", idx)
		}
		got, err := vals[23+idx].Blob()
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("blob[%d]: got %x (%v), want %x", idx, got, err, want)
		}
	}
	if v, err := vals[26].Date(); err != nil || v.Days != -719162 {
		t.Fatalf("date: %+v %v", v, err)
	}
	if v, err := vals[27].Time(); err != nil || v.Micros != 86399_999999 {
		t.Fatalf("time: %+v %v", v, err)
	}
	if v, err := vals[28].TimeNS(); err != nil || v.Nanos != 86399_999999999 {
		t.Fatalf("time_ns: %+v %v", v, err)
	}
	wantTS := []duckdb.Timestamp{
		{Value: -9223372037, Unit: duckdb.TimestampUnitSeconds},
		{Value: 253402300799, Unit: duckdb.TimestampUnitSeconds},
		{Value: 1795352400123, Unit: duckdb.TimestampUnitMillis},
		{Value: 1795352400123456, Unit: duckdb.TimestampUnitMicros},
		{Value: 1795352400123456789, Unit: duckdb.TimestampUnitNanos},
		{Value: 1795352400654321, Unit: duckdb.TimestampUnitTZ},
	}
	for i, want := range wantTS {
		got, err := vals[29+i].Timestamp()
		if err != nil || got != want {
			t.Fatalf("timestamp[%d]: got %+v (%v), want %+v", i, got, err, want)
		}
	}
	if v, err := vals[35].Interval(); err != nil || v != (duckdb.Interval{Months: -14, Days: 31, Micros: 987654321}) {
		t.Fatalf("interval: %+v %v", v, err)
	}
	if !vals[36].IsNull() || vals[36].Kind() != duckdb.TypeBigint {
		t.Fatalf("null col: isNull=%v kind=%v", vals[36].IsNull(), vals[36].Kind())
	}
}

func TestNativeNestedTypesAndDuplicateColumns(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()
	db, err := lib.Open(ctx, ":memory:", duckdb.Option{Name: "threads", Value: "2"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	sql := `SELECT
		[1, NULL, 3] AS list_null_elem,
		[]::INTEGER[] AS list_empty,
		NULL::INTEGER[] AS list_null,
		[[1, 2], [], NULL, [NULL]] AS list_nested,
		[10, NULL, 30]::INTEGER[3] AS arr,
		NULL::INTEGER[3] AS arr_null,
		{'a': 1, 'b': NULL::VARCHAR, 'c': [NULL::BIGINT]} AS st,
		NULL::STRUCT(a INTEGER, b VARCHAR) AS st_null,
		MAP {'k1': 1, 'k2': NULL::INTEGER} AS mp,
		MAP {}::MAP(VARCHAR, INTEGER) AS mp_empty,
		NULL::MAP(VARCHAR, INTEGER) AS mp_null,
		100::BIGINT AS dup,
		200::BIGINT AS dup`

	cols, vals := querySingleRow(t, conn, sql)

	// LIST with NULL element vs empty vs NULL list.
	lv, err := vals[0].List()
	if err != nil || len(lv.Elements) != 3 || !lv.Elements[1].IsNull() {
		t.Fatalf("list_null_elem: %+v %v", lv, err)
	}
	if v, _ := lv.Elements[0].Int32(); v != 1 {
		t.Fatalf("list[0] = %d", v)
	}
	if v, _ := lv.Elements[2].Int32(); v != 3 {
		t.Fatalf("list[2] = %d", v)
	}
	if vals[1].IsNull() {
		t.Fatal("empty list must not be NULL")
	}
	if lv, err := vals[1].List(); err != nil || len(lv.Elements) != 0 {
		t.Fatalf("list_empty: %+v %v", lv, err)
	}
	if !vals[2].IsNull() || vals[2].Kind() != duckdb.TypeList {
		t.Fatalf("list_null: null=%v kind=%v", vals[2].IsNull(), vals[2].Kind())
	}
	nlv, err := vals[3].List()
	if err != nil || len(nlv.Elements) != 4 || !nlv.Elements[2].IsNull() {
		t.Fatalf("list_nested: %+v %v", nlv, err)
	}
	if inner, err := nlv.Elements[1].List(); err != nil || len(inner.Elements) != 0 {
		t.Fatalf("list_nested[1] empty: %+v %v", inner, err)
	}
	if inner, err := nlv.Elements[3].List(); err != nil || len(inner.Elements) != 1 || !inner.Elements[0].IsNull() {
		t.Fatalf("list_nested[3] [NULL]: %+v %v", inner, err)
	}

	// ARRAY vs NULL array.
	av, err := vals[4].Array()
	if err != nil || av.Size != 3 || len(av.Elements) != 3 || !av.Elements[1].IsNull() {
		t.Fatalf("arr: %+v %v", av, err)
	}
	if !vals[5].IsNull() || vals[5].Kind() != duckdb.TypeArray {
		t.Fatalf("arr_null: null=%v kind=%v", vals[5].IsNull(), vals[5].Kind())
	}

	// STRUCT vs NULL struct.
	sv, err := vals[6].Struct()
	if err != nil || len(sv.Fields) != 3 || sv.Fields[0].Name != "a" || !sv.Fields[1].Value.IsNull() {
		t.Fatalf("st: %+v %v", sv, err)
	}
	if !vals[7].IsNull() || vals[7].Kind() != duckdb.TypeStruct {
		t.Fatalf("st_null: null=%v kind=%v", vals[7].IsNull(), vals[7].Kind())
	}

	// MAP vs empty MAP vs NULL MAP.
	mv, err := vals[8].Map()
	if err != nil || len(mv.Entries) != 2 || !mv.Entries[1].Value.IsNull() {
		t.Fatalf("mp: %+v %v", mv, err)
	}
	if k, _ := mv.Entries[0].Key.Varchar(); k != "k1" {
		t.Fatalf("mp[0].Key = %q", k)
	}
	if vals[9].IsNull() {
		t.Fatal("empty map must not be NULL")
	}
	if mv, err := vals[9].Map(); err != nil || len(mv.Entries) != 0 {
		t.Fatalf("mp_empty: %+v %v", mv, err)
	}
	if !vals[10].IsNull() || vals[10].Kind() != duckdb.TypeMap {
		t.Fatalf("mp_null: null=%v kind=%v", vals[10].IsNull(), vals[10].Kind())
	}

	// Duplicate column names preserved by ordinal.
	if cols[11].Name != "dup" || cols[12].Name != "dup" {
		t.Fatalf("dup cols = %+v", cols[11:13])
	}
	v11, _ := vals[11].Int64()
	v12, _ := vals[12].Int64()
	if v11 != 100 || v12 != 200 {
		t.Fatalf("dup values = (%d, %d), want (100, 200)", v11, v12)
	}
}

func TestNativeUnsupportedTypesRejectedEvenAllNullOrEmpty(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()
	db, err := lib.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	queries := []string{
		"SELECT NULL::UUID AS u",
		"SELECT NULL::UNION(a INTEGER, b VARCHAR) AS u",
		"SELECT NULL::BIGNUM AS bn",
		"SELECT NULL::VARIANT AS v",
		"SELECT NULL::BIT AS b",
		"SELECT NULL::TIME WITH TIME ZONE AS t",
		"SELECT [NULL::UUID] AS nested_list",
		"SELECT {'x': NULL::UUID} AS nested_struct",
		"SELECT MAP {'k': NULL::UUID} AS nested_map",
		"SELECT NULL::UUID AS empty_res WHERE false",
	}
	for _, q := range queries {
		// Stmt.Columns() rejects at prepare time.
		stmt, err := conn.Prepare(ctx, q)
		if err != nil {
			t.Fatalf("Prepare(%q): %v", q, err)
		}
		if _, err := stmt.Columns(); !errors.Is(err, duckdb.ErrUnsupportedType) {
			_ = stmt.Close()
			t.Fatalf("Stmt.Columns(%q) = %v, want ErrUnsupportedType", q, err)
		}
		_ = stmt.Close()

		// Conn.Query() also rejects before returning Rows.
		if rows, err := conn.Query(ctx, q); !errors.Is(err, duckdb.ErrUnsupportedType) {
			if rows != nil {
				_ = rows.Close()
			}
			t.Fatalf("Query(%q) = %v, want ErrUnsupportedType", q, err)
		}
	}
}

func TestNativeStatementOutputTypeAuditAndOwnedValuesAfterClose(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()
	db, err := lib.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Audit expression result types before execution (P0 contract).
	cases := []struct {
		sql  string
		want duckdb.LogicalType
	}{
		{"SELECT 1.00::DECIMAL(18,2) / 3 AS r", duckdb.ScalarType(duckdb.TypeDouble)},
		{"SELECT avg(x) AS r FROM (VALUES (1.25::DECIMAL(18,2))) t(x)", duckdb.ScalarType(duckdb.TypeDouble)},
		{"SELECT sum(x) AS r FROM (VALUES (1.25::DECIMAL(18,2))) t(x)", duckdb.DecimalType(38, 2)},
		{"SELECT 1.25::DECIMAL(18,2) * 1.25::DECIMAL(18,2) AS r", duckdb.DecimalType(18, 4)},
	}
	for _, tc := range cases {
		stmt, err := conn.Prepare(ctx, tc.sql)
		if err != nil {
			t.Fatalf("Prepare(%q): %v", tc.sql, err)
		}
		cols, err := stmt.Columns()
		_ = stmt.Close()
		if err != nil {
			t.Fatalf("Stmt.Columns(%q): %v", tc.sql, err)
		}
		if len(cols) != 1 || !cols[0].Type.Equal(tc.want) {
			t.Fatalf("Stmt.Columns(%q) = %+v, want %s", tc.sql, cols, tc.want)
		}
	}

	// Owned values across multiple native chunks remain valid after Next and Close.
	rows, err := conn.Query(ctx, "SELECT i, repeat('k', 24) || i::VARCHAR AS s, encode(repeat('b', 20) || i::VARCHAR) AS bl FROM range(5000) t(i)")
	if err != nil {
		t.Fatal(err)
	}
	var savedS []string
	var savedB [][]byte
	var buf []duckdb.Value
	for rows.Next() {
		buf, err = rows.DecodeRow(buf)
		if err != nil {
			t.Fatalf("DecodeRow: %v", err)
		}
		if i, _ := buf[0].Int64(); i == 0 || i == 2048 || i == 4999 {
			s, _ := buf[1].Varchar()
			b, _ := buf[2].Blob()
			savedS = append(savedS, s)
			savedB = append(savedB, b)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	wantIdx := []string{"0", "2048", "4999"}
	for j, idx := range wantIdx {
		if savedS[j] != strings.Repeat("k", 24)+idx {
			t.Fatalf("saved string[%d] corrupted after close: %q", j, savedS[j])
		}
		if string(savedB[j]) != strings.Repeat("b", 20)+idx {
			t.Fatalf("saved blob[%d] corrupted after close: %q", j, savedB[j])
		}
	}

	// Decode limits enforced on real native chunks.
	if err := conn.SetDecodeLimits(duckdb.DecodeLimits{MaxNestingDepth: 2, MaxValueBytes: 16, MaxCollectionLen: 3}); err != nil {
		t.Fatal(err)
	}
	// String > 16 bytes rejected at DecodeRow.
	rLong, err := conn.Query(ctx, "SELECT repeat('x', 32)")
	if err != nil {
		t.Fatal(err)
	}
	if !rLong.Next() {
		t.Fatal("expected row")
	}
	if _, err := rLong.DecodeRow(nil); !errors.Is(err, duckdb.ErrLimitExceeded) {
		t.Fatalf("long string = %v, want ErrLimitExceeded", err)
	}
	_ = rLong.Close()

	// List len > 3 rejected at DecodeRow.
	rList, err := conn.Query(ctx, "SELECT [1, 2, 3, 4]")
	if err != nil {
		t.Fatal(err)
	}
	if !rList.Next() {
		t.Fatal("expected row")
	}
	if _, err := rList.DecodeRow(nil); !errors.Is(err, duckdb.ErrLimitExceeded) {
		t.Fatalf("long list = %v, want ErrLimitExceeded", err)
	}
	_ = rList.Close()

	// Nesting depth > 2 rejected at open.
	if _, err := conn.Query(ctx, "SELECT [[[1]]]"); !errors.Is(err, duckdb.ErrLimitExceeded) {
		t.Fatalf("deep nesting = %v, want ErrLimitExceeded", err)
	}
}
