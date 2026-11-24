package duckdb_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
	"github.com/bububa/duckdbspike/duckdbsql"
)

func TestNativeVariantAllTypesNullDistinctionAndTableShredding(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()

	// Use a file-backed DB with v1.5.0 storage so CHECKPOINT exercises DuckDB's
	// on-disk VARIANT shredding!
	dbPath := filepath.Join(t.TempDir(), "variant.duckdb")
	db, err := lib.Open(ctx, dbPath,
		duckdb.Option{Name: "threads", Value: "2"},
		duckdb.Option{Name: "storage_compatibility_version", Value: "v1.5.0"},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// 1. Outer SQL NULL vs inner VARIANT_NULL & all scalar/nested types in one table.
	if _, err := conn.Exec(ctx, `CREATE TABLE variant_all (id INTEGER, v VARIANT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO variant_all VALUES
		(1,  NULL::VARIANT),
		(2,  NULL::INTEGER::VARIANT),
		(3,  true::VARIANT),
		(4,  false::VARIANT),
		(5,  (-7)::TINYINT::VARIANT),
		(6,  (-32000)::SMALLINT::VARIANT),
		(7,  42::INTEGER::VARIANT),
		(8,  9000000000::BIGINT::VARIANT),
		(9,  (-170141183460469231731687303715884105727)::HUGEINT::VARIANT),
		(10, 250::UTINYINT::VARIANT),
		(11, 65000::USMALLINT::VARIANT),
		(12, 4000000000::UINTEGER::VARIANT),
		(13, 18000000000000000000::UBIGINT::VARIANT),
		(14, 340282366920938463463374607431768211455::UHUGEINT::VARIANT),
		(15, 1.5::FLOAT::VARIANT),
		(16, 3.141592653589793::DOUBLE::VARIANT),
		(17, 12.34::DECIMAL(4, 2)::VARIANT),
		(18, (-123456.78)::DECIMAL(9, 2)::VARIANT),
		(19, 123456789012.3456::DECIMAL(18, 4)::VARIANT),
		(20, 123456789012345678901234567890.123456::DECIMAL(38, 6)::VARIANT),
		(21, 'hello variant world!'::VARCHAR::VARIANT),
		(22, '\xDE\xAD\xBE\xEF'::BLOB::VARIANT),
		(23, '00112233-4455-6677-8899-aabbccddeeff'::UUID::VARIANT),
		(24, DATE '2026-11-24'::VARIANT),
		(25, TIME '13:14:15.123456'::VARIANT),
		(26, TIME_NS '01:02:03.123456789'::VARIANT),
		(27, TIMETZ '12:00:00+08:00'::VARIANT),
		(28, TIMESTAMP_S '2026-11-24 10:20:30'::VARIANT),
		(29, TIMESTAMP_MS '2026-11-24 10:20:30.456'::VARIANT),
		(30, TIMESTAMP '2026-11-24 10:20:30.123456'::VARIANT),
		(31, TIMESTAMP_NS '2026-11-24 10:20:30.123456789'::VARIANT),
		(32, TIMESTAMPTZ '2026-11-24 10:20:30.5+00'::VARIANT),
		(33, INTERVAL '2 months 3 days 4 hours'::VARIANT),
		(34, '-1234567890123456789012345678901234567890'::BIGNUM::VARIANT),
		(35, '10110'::BIT::VARIANT),
		(36, 'POINT (1.5 2.5)'::GEOMETRY::VARIANT),
		(37, []::INTEGER[]::VARIANT),
		(38, {'a': 1::INTEGER, 'b': 'two', 'c': NULL::INTEGER, 'd': [10::INTEGER::VARIANT, 'x'::VARIANT, {'deep': true}::VARIANT]}::VARIANT)
	`); err != nil {
		t.Fatalf("insert variant_all: %v", err)
	}
	// Trigger storage checkpoint / shredding.
	if _, err := conn.Exec(ctx, `CHECKPOINT`); err != nil {
		t.Fatalf("CHECKPOINT: %v", err)
	}

	rows, err := conn.Query(ctx, `SELECT id, v FROM variant_all ORDER BY id`)
	if err != nil {
		t.Fatalf("query variant_all: %v", err)
	}
	gotByID := map[int32]duckdb.Value{}
	for rows.Next() {
		vals, derr := rows.DecodeRow(nil)
		if derr != nil {
			_ = rows.Close()
			t.Fatalf("DecodeRow: %v", derr)
		}
		id, _ := vals[0].Int32()
		gotByID[id] = vals[1]
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("rows.Err: %v", err)
	}
	_ = rows.Close()

	if len(gotByID) != 38 {
		t.Fatalf("decoded %d rows, want 38", len(gotByID))
	}

	// Row 1: outer SQL NULL vs Row 2: inner VARIANT_NULL (note: DuckDB may represent
	// NULL::INTEGER::VARIANT as outer SQL NULL or inner VARIANT_NULL; also test
	// guaranteed inner VARIANT_NULL inside Row 38's field 'c').
	if !gotByID[1].IsNull() || gotByID[1].Kind() != duckdb.TypeVariant {
		t.Fatalf("row 1 should be outer SQL NULL VARIANT: %#v", gotByID[1])
	}
	if _, err := gotByID[1].Variant(); !errors.Is(err, duckdb.ErrNullValue) {
		t.Fatalf("row 1 Variant() = %v, want ErrNullValue", err)
	}
	if !gotByID[2].IsNull() {
		vr2, err := gotByID[2].Variant()
		if err != nil || !vr2.IsNull() {
			t.Fatalf("row 2 non-null VARIANT must hold inner VARIANT_NULL: %+v (%v)", vr2, err)
		}
	}

	mustInner := func(id int32) duckdb.Value {
		t.Helper()
		vr, err := gotByID[id].Variant()
		if err != nil {
			t.Fatalf("row %d Variant(): %v", id, err)
		}
		return vr.Value
	}

	if b, err := mustInner(3).Bool(); err != nil || !b {
		t.Fatalf("row 3 = %v (%v)", b, err)
	}
	if b, err := mustInner(4).Bool(); err != nil || b {
		t.Fatalf("row 4 = %v (%v)", b, err)
	}
	if v, err := mustInner(5).Int8(); err != nil || v != -7 {
		t.Fatalf("row 5 = %v (%v)", v, err)
	}
	if v, err := mustInner(6).Int16(); err != nil || v != -32000 {
		t.Fatalf("row 6 = %v (%v)", v, err)
	}
	if v, err := mustInner(7).Int32(); err != nil || v != 42 {
		t.Fatalf("row 7 = %v (%v)", v, err)
	}
	if v, err := mustInner(8).Int64(); err != nil || v != 9000000000 {
		t.Fatalf("row 8 = %v (%v)", v, err)
	}
	if v, err := mustInner(9).Hugeint(); err != nil || v.String() != "-170141183460469231731687303715884105727" {
		t.Fatalf("row 9 = %s (%v)", v.String(), err)
	}
	if v, err := mustInner(10).Uint8(); err != nil || v != 250 {
		t.Fatalf("row 10 = %v (%v)", v, err)
	}
	if v, err := mustInner(11).Uint16(); err != nil || v != 65000 {
		t.Fatalf("row 11 = %v (%v)", v, err)
	}
	if v, err := mustInner(12).Uint32(); err != nil || v != 4000000000 {
		t.Fatalf("row 12 = %v (%v)", v, err)
	}
	if v, err := mustInner(13).Uint64(); err != nil || v != 18000000000000000000 {
		t.Fatalf("row 13 = %v (%v)", v, err)
	}
	if v, err := mustInner(14).Uhugeint(); err != nil || v.String() != "340282366920938463463374607431768211455" {
		t.Fatalf("row 14 = %s (%v)", v.String(), err)
	}
	if v, err := mustInner(15).Float32(); err != nil || v != 1.5 {
		t.Fatalf("row 15 = %v (%v)", v, err)
	}
	if v, err := mustInner(16).Float64(); err != nil || v != 3.141592653589793 {
		t.Fatalf("row 16 = %v (%v)", v, err)
	}
	if v, err := mustInner(17).Decimal(); err != nil || v.String() != "12.34" {
		t.Fatalf("row 17 = %s (%v)", v.String(), err)
	}
	if v, err := mustInner(18).Decimal(); err != nil || v.String() != "-123456.78" {
		t.Fatalf("row 18 = %s (%v)", v.String(), err)
	}
	if v, err := mustInner(19).Decimal(); err != nil || v.String() != "123456789012.3456" {
		t.Fatalf("row 19 = %s (%v)", v.String(), err)
	}
	if v, err := mustInner(20).Decimal(); err != nil || v.String() != "123456789012345678901234567890.123456" {
		t.Fatalf("row 20 = %s (%v)", v.String(), err)
	}
	if v, err := mustInner(21).Varchar(); err != nil || v != "hello variant world!" {
		t.Fatalf("row 21 = %q (%v)", v, err)
	}
	if v, err := mustInner(22).Blob(); err != nil || len(v) != 4 || v[0] != 0xDE || v[3] != 0xEF {
		t.Fatalf("row 22 = %x (%v)", v, err)
	}
	if v, err := mustInner(23).UUID(); err != nil || v.String() != "00112233-4455-6677-8899-aabbccddeeff" {
		t.Fatalf("row 23 = %s (%v)", v.String(), err)
	}
	if v, err := mustInner(24).Date(); err != nil || v.String() != "2026-11-24" {
		t.Fatalf("row 24 = %s (%v)", v.String(), err)
	}
	if v, err := mustInner(25).Time(); err != nil || v.String() != "13:14:15.123456" {
		t.Fatalf("row 25 = %s (%v)", v.String(), err)
	}
	if v, err := mustInner(26).TimeNS(); err != nil || v.String() != "01:02:03.123456789" {
		t.Fatalf("row 26 = %s (%v)", v.String(), err)
	}
	if v, err := mustInner(27).TimeTZ(); err != nil || v.String() != "12:00:00+08:00" {
		t.Fatalf("row 27 = %s (%v)", v.String(), err)
	}
	if v, err := mustInner(28).Timestamp(); err != nil || v.Unit != duckdb.TimestampUnitSeconds || v.String() != "2026-11-24T10:20:30Z" {
		t.Fatalf("row 28 = %s (%v)", v.String(), err)
	}
	if v, err := mustInner(29).Timestamp(); err != nil || v.Unit != duckdb.TimestampUnitMillis || v.String() != "2026-11-24T10:20:30.456Z" {
		t.Fatalf("row 29 = %s (%v)", v.String(), err)
	}
	if v, err := mustInner(30).Timestamp(); err != nil || v.Unit != duckdb.TimestampUnitMicros || v.String() != "2026-11-24T10:20:30.123456Z" {
		t.Fatalf("row 30 = %s (%v)", v.String(), err)
	}
	if v, err := mustInner(31).Timestamp(); err != nil || v.Unit != duckdb.TimestampUnitNanos || v.String() != "2026-11-24T10:20:30.123456789Z" {
		t.Fatalf("row 31 = %s (%v)", v.String(), err)
	}
	if v, err := mustInner(32).Timestamp(); err != nil || v.Unit != duckdb.TimestampUnitTZ || v.String() != "2026-11-24T10:20:30.5Z" {
		t.Fatalf("row 32 = %s (%v)", v.String(), err)
	}
	if v, err := mustInner(33).Interval(); err != nil || v.Months != 2 || v.Days != 3 || v.Micros != 4*3600*1_000_000 {
		t.Fatalf("row 33 = %+v (%v)", v, err)
	}
	if v, err := mustInner(34).Bignum(); err != nil || v.String() != "-1234567890123456789012345678901234567890" {
		t.Fatalf("row 34 = %s (%v)", v.String(), err)
	}
	if v, err := mustInner(35).Bit(); err != nil || v.String() != "10110" {
		t.Fatalf("row 35 = %s (%v)", v.String(), err)
	}
	if v, err := mustInner(36).Geometry(); err != nil || len(v.WKB) == 0 {
		t.Fatalf("row 36 = %+v (%v)", v, err)
	}
	if v, err := mustInner(37).List(); err != nil || len(v.Elements) != 0 {
		t.Fatalf("row 37 empty array = %+v (%v)", v, err)
	}

	// Row 38: nested OBJECT with inner VARIANT_NULL field 'c' + heterogeneous ARRAY 'd'.
	obj, err := mustInner(38).Struct()
	if err != nil || len(obj.Fields) != 4 {
		t.Fatalf("row 38 object = %+v (%v)", obj, err)
	}
	fieldsByName := map[string]duckdb.Value{}
	for _, f := range obj.Fields {
		fieldsByName[f.Name] = f.Value
	}
	if a, err := fieldsByName["a"].Int32(); err != nil || a != 1 {
		t.Fatalf("obj.a = %d (%v)", a, err)
	}
	if b, err := fieldsByName["b"].Varchar(); err != nil || b != "two" {
		t.Fatalf("obj.b = %q (%v)", b, err)
	}
	if !fieldsByName["c"].IsNull() || fieldsByName["c"].Kind() != duckdb.TypeSQLNull {
		t.Fatalf("obj.c should be inner VARIANT_NULL: %#v", fieldsByName["c"])
	}
	arr, err := fieldsByName["d"].List()
	if err != nil || len(arr.Elements) != 3 {
		t.Fatalf("obj.d = %+v (%v)", arr, err)
	}
	if n, err := arr.Elements[0].Int32(); err != nil || n != 10 {
		t.Fatalf("obj.d[0] = %d (%v)", n, err)
	}
	if s, err := arr.Elements[1].Varchar(); err != nil || s != "x" {
		t.Fatalf("obj.d[1] = %q (%v)", s, err)
	}
	deepObj, err := arr.Elements[2].Struct()
	if err != nil || len(deepObj.Fields) != 1 || deepObj.Fields[0].Name != "deep" {
		t.Fatalf("obj.d[2] = %+v (%v)", deepObj, err)
	}
	if flag, err := deepObj.Fields[0].Value.Bool(); err != nil || !flag {
		t.Fatalf("obj.d[2].deep = %v (%v)", flag, err)
	}

	// 2. Homogeneous shredded VARIANT table (forces DuckDB shredding across 5000 rows).
	if _, err := conn.Exec(ctx, `
		CREATE TABLE variant_shredded AS
		SELECT i::INTEGER AS id,
		       CASE WHEN i % 100 = 0 THEN NULL::VARIANT
		            WHEN i % 100 = 1 THEN 'fallback'::VARIANT
		            ELSE {'k': i::BIGINT, 'ok': true}::VARIANT
		       END AS v
		FROM range(5000) t(i)
	`); err != nil {
		t.Fatalf("create shredded variant table: %v", err)
	}
	if _, err := conn.Exec(ctx, `CHECKPOINT`); err != nil {
		t.Fatalf("checkpoint shredded variant table: %v", err)
	}
	sRows, err := conn.Query(ctx, `SELECT id, v FROM variant_shredded ORDER BY id`)
	if err != nil {
		t.Fatalf("query variant_shredded: %v", err)
	}
	var nullCnt, strCnt, objCnt int
	for sRows.Next() {
		vals, derr := sRows.DecodeRow(nil)
		if derr != nil {
			_ = sRows.Close()
			t.Fatalf("shredded DecodeRow: %v", derr)
		}
		if vals[1].IsNull() {
			nullCnt++
			continue
		}
		vr, verr := vals[1].Variant()
		if verr != nil {
			_ = sRows.Close()
			t.Fatalf("shredded Variant(): %v", verr)
		}
		switch vr.Value.Kind() {
		case duckdb.TypeVarchar:
			strCnt++
		case duckdb.TypeStruct:
			objCnt++
		default:
			_ = sRows.Close()
			t.Fatalf("unexpected shredded variant kind %s", vr.Value.Kind())
		}
	}
	_ = sRows.Close()
	if nullCnt != 50 || strCnt != 50 || objCnt != 4900 {
		t.Fatalf("shredded counts = (null=%d, str=%d, obj=%d), want (50, 50, 4900)", nullCnt, strCnt, objCnt)
	}

	// 3. Outer SQL NULL vs inner VARIANT_NULL inside OBJECT/ARRAY.
	// Note: DuckDB v1.5.6 normalizes top-level NULL::VARIANT and variant_extract of a
	// NULL field to outer SQL NULL (IS NULL = true), while preserving VARIANT_NULL
	// (type_id = 0) inside OBJECT/ARRAY payloads (root-level VARIANT_NULL with valid
	// outer bit is also covered in TestVariantDecoderBoundsCycleAndLimitsMock).
	nRows, err := conn.Query(ctx, `SELECT
		NULL::VARIANT,
		[NULL::INTEGER, 99::INTEGER]::VARIANT,
		{'c': NULL::INTEGER, 'x': 7::INTEGER}::VARIANT
	`)
	if err != nil {
		t.Fatalf("query null vs variant_null: %v", err)
	}
	if !nRows.Next() {
		_ = nRows.Close()
		t.Fatal("expected row")
	}
	nVals, err := nRows.DecodeRow(nil)
	_ = nRows.Close()
	if err != nil {
		t.Fatalf("DecodeRow null vs variant_null: %v", err)
	}
	if !nVals[0].IsNull() || nVals[0].Kind() != duckdb.TypeVariant {
		t.Fatalf("col 0 must be outer SQL NULL VARIANT: %#v", nVals[0])
	}
	if nVals[1].IsNull() {
		t.Fatal("col 1 ([NULL, 99]::VARIANT) must be non-NULL VARIANT")
	}
	arrVar, err := duckdb.ValueAs[duckdb.Variant](nVals[1])
	if err != nil || arrVar.IsNull() {
		t.Fatalf("col 1 ValueAs[Variant] = %+v (%v)", arrVar, err)
	}
	arrList, err := arrVar.Value.List()
	if err != nil || len(arrList.Elements) != 2 {
		t.Fatalf("col 1 array = %+v (%v)", arrList, err)
	}
	if !arrList.Elements[0].IsNull() || arrList.Elements[0].Kind() != duckdb.TypeSQLNull {
		t.Fatalf("col 1[0] want inner VARIANT_NULL, got %#v", arrList.Elements[0])
	}
	if n99, err := arrList.Elements[1].Int32(); err != nil || n99 != 99 {
		t.Fatalf("col 1[1] = %d (%v), want 99", n99, err)
	}
	if nVals[2].IsNull() {
		t.Fatal("col 2 must be non-NULL VARIANT")
	}

	// 4. DecodeLimits enforcement on real DuckDB VARIANT.
	if err := conn.SetDecodeLimits(duckdb.DecodeLimits{MaxNestingDepth: 1}); err != nil {
		t.Fatal(err)
	}
	dRows, err := conn.Query(ctx, `SELECT {'a': {'b': 1}}::VARIANT`)
	if err != nil {
		t.Fatal(err)
	}
	if dRows.Next() {
		if _, err := dRows.DecodeRow(nil); !errors.Is(err, duckdb.ErrLimitExceeded) {
			_ = dRows.Close()
			t.Fatalf("deep VARIANT = %v, want ErrLimitExceeded", err)
		}
	}
	_ = dRows.Close()

	// 5. duckdbsql Variant Scan (distinguishing outer NULL vs inner VARIANT_NULL).
	connector, err := duckdbsql.NewConnector(lib, ":memory:?threads=1")
	if err != nil {
		t.Fatal(err)
	}
	sqlDB := sql.OpenDB(connector)
	defer sqlDB.Close()

	var sqlNullAny any
	var nullVar sql.Null[duckdb.Variant]
	var innerObj duckdb.Variant
	if err := sqlDB.QueryRowContext(ctx, `SELECT NULL::VARIANT, NULL::VARIANT, {'c': NULL::INTEGER, 'x': 7::INTEGER}::VARIANT`).Scan(&sqlNullAny, &nullVar, &innerObj); err != nil {
		t.Fatalf("duckdbsql Scan VARIANT: %v", err)
	}
	if sqlNullAny != nil {
		t.Fatalf("outer SQL NULL scanned as %#v, want nil", sqlNullAny)
	}
	if nullVar.Valid {
		t.Fatalf("sql.Null[Variant] expected Valid == false: %+v", nullVar)
	}
	st, err := innerObj.Value.Struct()
	if err != nil || len(st.Fields) != 2 {
		t.Fatalf("innerObj struct = %+v (%v)", st, err)
	}
	stByName := map[string]duckdb.Value{}
	for _, f := range st.Fields {
		stByName[f.Name] = f.Value
	}
	if !stByName["c"].IsNull() || stByName["c"].Kind() != duckdb.TypeSQLNull {
		t.Fatalf("duckdbsql innerObj.c = %#v, want VARIANT_NULL", stByName["c"])
	}
	if x, err := stByName["x"].Int32(); err != nil || x != 7 {
		t.Fatalf("duckdbsql innerObj.x = %d (%v), want 7", x, err)
	}
}
