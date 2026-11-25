package duckdb_test

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
)

func TestNativeChunkBufferedAppenderAllTypesBoundariesAndRollback(t *testing.T) {
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

	// 1. All-types table verifying chunk-buffered vector writers across every type.
	if _, err := conn.ExecScript(ctx, `
		CREATE TYPE p4c_mood AS ENUM ('calm', 'fast', 'turbo');
		CREATE TABLE p4c_all (
			id INTEGER,
			flag BOOLEAN DEFAULT true,
			i8 TINYINT,
			i16 SMALLINT,
			i64 BIGINT,
			h128 HUGEINT,
			u8 UTINYINT,
			u16 USMALLINT,
			u32 UINTEGER,
			u64 UBIGINT,
			uh128 UHUGEINT,
			f32 FLOAT,
			f64 DOUBLE,
			d4 DECIMAL(4, 2),
			d9 DECIMAL(9, 2),
			d18 DECIMAL(18, 4),
			d38 DECIMAL(38, 6),
			s VARCHAR,
			b BLOB,
			bt BIT,
			bn BIGNUM,
			u UUID,
			dt DATE,
			tm TIME,
			tmns TIME_NS,
			tmtz TIMETZ,
			ts TIMESTAMP,
			tss TIMESTAMP_S,
			tsms TIMESTAMP_MS,
			tsns TIMESTAMP_NS,
			tstz TIMESTAMPTZ,
			iv INTERVAL,
			en p4c_mood,
			lst INTEGER[],
			arr INTEGER[2],
			st STRUCT(k VARCHAR, v INTEGER),
			mp MAP(VARCHAR, INTEGER),
			un UNION(num INTEGER, txt VARCHAR)
		);
	`); err != nil {
		t.Fatalf("create p4c_all: %v", err)
	}

	app, err := conn.NewAppender(ctx, "", "", "p4c_all")
	if err != nil {
		t.Fatalf("NewAppender: %v", err)
	}

	d4, _ := duckdb.NewDecimal(4, 1, big.NewInt(123)) // 12.3 -> losslessly rescaled to 12.30!
	d9, _ := duckdb.NewDecimal(9, 2, big.NewInt(-12345678))
	d18, _ := duckdb.NewDecimal(18, 4, big.NewInt(1234567890123456))
	big38, _ := new(big.Int).SetString("123456789012345678901234567890123456", 10)
	d38, _ := duckdb.NewDecimal(38, 6, big38)
	d4v, _ := duckdb.DecimalValue(d4)
	d9v, _ := duckdb.DecimalValue(d9)
	d18v, _ := duckdb.DecimalValue(d18)
	d38v, _ := duckdb.DecimalValue(d38)
	bitVal, _ := duckdb.NewBitFromString("10110")
	bitV, _ := duckdb.BitValue(bitVal)
	bnNegBI, _ := new(big.Int).SetString("-9876543210987654321098765432109876543210", 10)
	bnNeg, _ := duckdb.BignumFromBigInt(bnNegBI)
	uuidVal, _ := duckdb.ParseUUID("00112233-4455-6677-8899-aabbccddeeff")
	tzVal, _ := duckdb.TimeTZValue(duckdb.TimeTZ{Micros: 12 * 3600 * 1_000_000, OffsetSeconds: 8 * 3600})
	tsV, _ := duckdb.TimestampValue(duckdb.Timestamp{Value: 1_700_000_000_123456, Unit: duckdb.TimestampUnitMicros})
	tssV, _ := duckdb.TimestampValue(duckdb.Timestamp{Value: 1_700_000_000, Unit: duckdb.TimestampUnitSeconds})
	tsmsV, _ := duckdb.TimestampValue(duckdb.Timestamp{Value: 1_700_000_000_123, Unit: duckdb.TimestampUnitMillis})
	tsnsV, _ := duckdb.TimestampValue(duckdb.Timestamp{Value: 1_700_000_000_123456789, Unit: duckdb.TimestampUnitNanos})
	tstzV, _ := duckdb.TimestampValue(duckdb.Timestamp{Value: 1_700_000_000_500000, Unit: duckdb.TimestampUnitTZ})
	lstV, _ := duckdb.NewListValue(duckdb.ScalarType(duckdb.TypeInteger), []duckdb.Value{duckdb.Int32Value(10), duckdb.NullValue(), duckdb.Int32Value(30)})
	arrV, _ := duckdb.NewArrayValue(duckdb.ScalarType(duckdb.TypeInteger), []duckdb.Value{duckdb.Int32Value(7), duckdb.Int32Value(8)})
	stV, _ := duckdb.NewStructValue([]duckdb.StructFieldValue{
		{Name: "k", Type: duckdb.ScalarType(duckdb.TypeVarchar), Value: duckdb.VarcharValue("hello")},
		{Name: "v", Type: duckdb.ScalarType(duckdb.TypeInteger), Value: duckdb.Int32Value(42)},
	})
	mpV, _ := duckdb.NewMapValue(duckdb.ScalarType(duckdb.TypeVarchar), duckdb.ScalarType(duckdb.TypeInteger), []duckdb.MapEntry{
		{Key: duckdb.VarcharValue("one"), Value: duckdb.Int32Value(1)},
		{Key: duckdb.VarcharValue("two"), Value: duckdb.NullValue()},
	})
	ut := duckdb.UnionType(
		duckdb.StructFieldType{Name: "num", Type: duckdb.ScalarType(duckdb.TypeInteger)},
		duckdb.StructFieldType{Name: "txt", Type: duckdb.ScalarType(duckdb.TypeVarchar)},
	)
	unV, _ := duckdb.NewUnionValue(ut, 1, duckdb.VarcharValue("union_text"))

	// Row 1: full populated row with DefaultValue() on flag.
	row1 := []duckdb.Value{
		duckdb.Int32Value(1),
		duckdb.DefaultValue(),
		duckdb.Int8Value(-7),
		duckdb.Int16Value(-32000),
		duckdb.Int64Value(9000000000),
		duckdb.HugeintValue(duckdb.HugeintFromInt64(-123456789)),
		duckdb.Uint8Value(250),
		duckdb.Uint16Value(65000),
		duckdb.Uint32Value(4000000000),
		duckdb.Uint64Value(18000000000000000000),
		duckdb.UhugeintValue(duckdb.UhugeintFromUint64(987654321)),
		duckdb.Float32Value(1.5),
		duckdb.Float64Value(3.141592653589793),
		d4v, d9v, d18v, d38v,
		duckdb.VarcharValue("long out-of-line varchar payload > 12 bytes"),
		duckdb.BlobValue([]byte{0xDE, 0xAD, 0xBE, 0xEF}),
		bitV,
		duckdb.BignumValue(bnNeg),
		duckdb.UUIDValue(uuidVal),
		duckdb.DateValue(duckdb.Date{Days: 20000}),
		duckdb.TimeValue(duckdb.Time{Micros: 3600_000_000}),
		duckdb.TimeNSValue(duckdb.TimeNS{Nanos: 1_000_000_001}),
		tzVal,
		tsV, tssV, tsmsV, tsnsV, tstzV,
		duckdb.IntervalValue(duckdb.Interval{Months: 2, Days: 3, Micros: 4000}),
		duckdb.VarcharValue("turbo"), // ENUM by label
		lstV, arrV, stV, mpV, unV,
	}
	if err := app.AppendRow(row1...); err != nil {
		_ = app.Close()
		t.Fatalf("AppendRow row1: %v", err)
	}

	// Invalid UTF-8 rejected cleanly without corrupting prior buffered row!
	badRow := append([]duckdb.Value(nil), row1...)
	badRow[0] = duckdb.Int32Value(999)
	badRow[17] = duckdb.VarcharValue(string([]byte{0xff, 0xfe}))
	if err := app.AppendRow(badRow...); !errors.Is(err, duckdb.ErrInvalidInput) {
		_ = app.Close()
		t.Fatalf("invalid UTF-8 = %v, want ErrInvalidInput", err)
	}

	// Row 2: all NULLs (except id=2 and bn=0 to verify BIGNUM zero wire format).
	row2 := make([]duckdb.Value, len(row1))
	for i := range row2 {
		row2[i] = duckdb.NullValue()
	}
	row2[0] = duckdb.Int32Value(2)
	row2[20] = duckdb.BignumValue(duckdb.Bignum{}) // BIGNUM 0
	if err := app.AppendRow(row2...); err != nil {
		_ = app.Close()
		t.Fatalf("AppendRow row2: %v", err)
	}
	if err := app.Close(); err != nil {
		t.Fatalf("Close p4c_all appender: %v", err)
	}

	// Verify Row 1 & Row 2 decoded from DuckDB!
	_, got1 := querySingleRow(t, conn, `SELECT flag, d4::VARCHAR, d38::VARCHAR, s, bt::VARCHAR, bn::VARCHAR, u::VARCHAR, en::VARCHAR, lst, arr, st, mp, un FROM p4c_all WHERE id = 1`)
	if flag, _ := got1[0].Bool(); !flag {
		t.Fatalf("row1.flag = %v, want true (DEFAULT)", flag)
	}
	if s, _ := got1[1].Varchar(); s != "12.30" {
		t.Fatalf("row1.d4 = %q, want 12.30", s)
	}
	if s, _ := got1[2].Varchar(); s != "123456789012345678901234567890.123456" {
		t.Fatalf("row1.d38 = %q", s)
	}
	if s, _ := got1[3].Varchar(); s != "long out-of-line varchar payload > 12 bytes" {
		t.Fatalf("row1.s = %q", s)
	}
	if s, _ := got1[4].Varchar(); s != "10110" {
		t.Fatalf("row1.bt = %q, want 10110", s)
	}
	if s, _ := got1[5].Varchar(); s != "-9876543210987654321098765432109876543210" {
		t.Fatalf("row1.bn = %q", s)
	}
	if s, _ := got1[6].Varchar(); s != "00112233-4455-6677-8899-aabbccddeeff" {
		t.Fatalf("row1.u = %q", s)
	}
	if s, _ := got1[7].Varchar(); s != "turbo" {
		t.Fatalf("row1.en = %q, want turbo", s)
	}
	if lv, err := got1[8].List(); err != nil || len(lv.Elements) != 3 || !lv.Elements[1].IsNull() {
		t.Fatalf("row1.lst = %+v (%v)", lv, err)
	}
	if av, err := got1[9].Array(); err != nil || len(av.Elements) != 2 {
		t.Fatalf("row1.arr = %+v (%v)", av, err)
	}
	if sv, err := got1[10].Struct(); err != nil || len(sv.Fields) != 2 {
		t.Fatalf("row1.st = %+v (%v)", sv, err)
	}
	if mv, err := got1[11].Map(); err != nil || len(mv.Entries) != 2 || !mv.Entries[1].Value.IsNull() {
		t.Fatalf("row1.mp = %+v (%v)", mv, err)
	}
	if uv, err := got1[12].Union(); err != nil || uv.TagName != "txt" {
		t.Fatalf("row1.un = %+v (%v)", uv, err)
	}

	_, got2 := querySingleRow(t, conn, `SELECT count(*)::BIGINT, min(bn::VARCHAR), count(st)::BIGINT FROM p4c_all WHERE id = 2`)
	if cnt, _ := got2[0].Int64(); cnt != 1 {
		t.Fatalf("row2 count = %d, want 1", cnt)
	}
	if bn0, _ := got2[1].Varchar(); bn0 != "0" {
		t.Fatalf("row2.bn = %q, want '0'", bn0)
	}
	if nonNullSt, _ := got2[2].Int64(); nonNullSt != 0 {
		t.Fatalf("row2.st should be NULL, non-null count = %d", nonNullSt)
	}

	// 2. Multi-chunk boundary test: 5,000 rows (> 2 full 2048-row chunks + remainder 904) via AppendRows + AddColumn/Clear.
	if _, err := conn.Exec(ctx, `CREATE TABLE p4c_bulk (id BIGINT, name VARCHAR, tag VARCHAR DEFAULT 'def')`); err != nil {
		t.Fatal(err)
	}
	bulkApp, err := conn.NewAppender(ctx, "", "", "p4c_bulk")
	if err != nil {
		t.Fatal(err)
	}
	if err := bulkApp.AddColumn("id"); err != nil {
		_ = bulkApp.Close()
		t.Fatal(err)
	}
	if err := bulkApp.AddColumn("name"); err != nil {
		_ = bulkApp.Close()
		t.Fatal(err)
	}
	const totalRows = 5000
	batch := make([][]duckdb.Value, totalRows)
	for i := range totalRows {
		batch[i] = []duckdb.Value{
			duckdb.Int64Value(int64(i + 1)),
			duckdb.VarcharValue(fmt.Sprintf("row_%05d", i+1)),
		}
	}
	if err := bulkApp.AppendRows(batch); err != nil {
		_ = bulkApp.Close()
		t.Fatalf("AppendRows(5000): %v", err)
	}
	if err := bulkApp.Close(); err != nil {
		t.Fatalf("bulkApp.Close: %v", err)
	}

	_, bRow := querySingleRow(t, conn, `SELECT count(*)::BIGINT, sum(id)::BIGINT, count(*) FILTER (WHERE tag = 'def')::BIGINT FROM p4c_bulk`)
	if cnt, _ := bRow[0].Int64(); cnt != totalRows {
		t.Fatalf("bulk count = %d, want %d", cnt, totalRows)
	}
	wantSum := int64(totalRows) * int64(totalRows+1) / 2
	if sum, _ := bRow[1].Int64(); sum != wantSum {
		t.Fatalf("bulk sum = %d, want %d", sum, wantSum)
	}
	if defCnt, _ := bRow[2].Int64(); defCnt != totalRows {
		t.Fatalf("bulk default tag count = %d, want %d", defCnt, totalRows)
	}
}

func TestNativeChunkAppenderGeometry(t *testing.T) {
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

	// Obtain canonical WKB payloads directly from DuckDB's built-in GEOMETRY cast.
	_, wkbRow := querySingleRow(t, conn, `SELECT 'POINT (1.5 2.5)'::GEOMETRY, 'LINESTRING (0 0, 3 4)'::GEOMETRY`)
	pt, err := wkbRow[0].Geometry()
	if err != nil {
		t.Fatal(err)
	}
	line, err := wkbRow[1].Geometry()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := conn.Exec(ctx, `CREATE TABLE geom_app (id INT, g GEOMETRY, g_crs GEOMETRY('OGC:CRS84'))`); err != nil {
		t.Fatal(err)
	}
	app, err := conn.NewAppender(ctx, "", "", "geom_app")
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	// Empty WKB and CRS mismatch must be rejected cleanly without corrupting the chunk.
	if err := app.AppendRow(duckdb.Int32Value(0), duckdb.GeometryValue(duckdb.Geometry{}), duckdb.NullValue()); !errors.Is(err, duckdb.ErrInvalidInput) {
		t.Fatalf("empty WKB err = %v, want ErrInvalidInput", err)
	}
	if err := app.AppendRow(duckdb.Int32Value(0), duckdb.GeometryValue(pt), duckdb.GeometryValue(duckdb.Geometry{WKB: pt.WKB, CRS: "EPSG:4326"})); !errors.Is(err, duckdb.ErrTypeMismatch) {
		t.Fatalf("CRS mismatch err = %v, want ErrTypeMismatch", err)
	}

	if err := app.AppendRows([][]duckdb.Value{
		{duckdb.Int32Value(1), duckdb.GeometryValue(pt), duckdb.GeometryValue(duckdb.Geometry{WKB: pt.WKB, CRS: "OGC:CRS84"})},
		{duckdb.Int32Value(2), duckdb.GeometryValue(line), duckdb.NullValue()},
		{duckdb.Int32Value(3), duckdb.NullValue(), duckdb.GeometryValue(duckdb.Geometry{WKB: line.WKB})},
	}); err != nil {
		t.Fatalf("AppendRows geometry: %v", err)
	}
	if err := app.Close(); err != nil {
		t.Fatalf("app.Close: %v", err)
	}

	rows, err := conn.Query(ctx, `SELECT id, g, g::VARCHAR, g_crs, g_crs::VARCHAR FROM geom_app ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var got [][]duckdb.Value
	for rows.Next() {
		r, err := rows.DecodeRow(nil)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("row count = %d, want 3", len(got))
	}
	if s, _ := got[0][2].Varchar(); s != "POINT (1.5 2.5)" {
		t.Fatalf("row 0 g::VARCHAR = %q, want POINT (1.5 2.5)", s)
	}
	gCRS0, err := got[0][3].Geometry()
	if err != nil || gCRS0.CRS != "OGC:CRS84" {
		t.Fatalf("row 0 g_crs = (%+v, %v), want CRS OGC:CRS84", gCRS0, err)
	}
	if s, _ := got[1][2].Varchar(); s != "LINESTRING (0 0, 3 4)" {
		t.Fatalf("row 1 g::VARCHAR = %q, want LINESTRING (0 0, 3 4)", s)
	}
	if !got[1][3].IsNull() {
		t.Fatalf("row 1 g_crs expected NULL, got %v", got[1][3])
	}
	if !got[2][1].IsNull() {
		t.Fatalf("row 2 g expected NULL, got %v", got[2][1])
	}
	if s, _ := got[2][4].Varchar(); s != "LINESTRING (0 0, 3 4)" {
		t.Fatalf("row 2 g_crs::VARCHAR = %q, want LINESTRING (0 0, 3 4)", s)
	}
}

func BenchmarkNativeAppenderRows(b *testing.B) {
	libPath := os.Getenv("DUCKDB_LIB_PATH")
	shaHex := os.Getenv("DUCKDB_LIB_SHA256")
	if libPath == "" || shaHex == "" {
		b.Skip("DUCKDB_LIB_PATH and DUCKDB_LIB_SHA256 are required")
	}
	lib, err := duckdb.Load(libPath, shaHex)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	db, err := lib.Open(ctx, ":memory:", duckdb.Option{Name: "threads", Value: "2"})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Connect(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	const n = 50_000
	rows := make([][]duckdb.Value, n)
	for i := range n {
		rows[i] = []duckdb.Value{
			duckdb.Int64Value(int64(i)),
			duckdb.Float64Value(float64(i) * 0.5),
			duckdb.VarcharValue("bench_payload"),
		}
	}

	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		if _, err := conn.Exec(ctx, `CREATE OR REPLACE TABLE bench_app (id BIGINT, val DOUBLE, s VARCHAR)`); err != nil {
			b.Fatal(err)
		}
		app, err := conn.NewAppender(ctx, "", "", "bench_app")
		if err != nil {
			b.Fatal(err)
		}
		if err := app.AppendRows(rows); err != nil {
			_ = app.Close()
			b.Fatal(err)
		}
		if err := app.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
