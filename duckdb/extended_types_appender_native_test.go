package duckdb_test

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
)

func TestNativeP2aTypesUnionBitBignumGeometryAndInfinity(t *testing.T) {
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

	// 1. BIT bind & decode round-trip across padding widths 0..7 and SQL text check.
	for _, s := range []string{"0", "1", "10110", "11111111", "0101010111001"} {
		bit, err := duckdb.NewBitFromString(s)
		if err != nil {
			t.Fatal(err)
		}
		bv, err := duckdb.BitValue(bit)
		if err != nil {
			t.Fatal(err)
		}
		_, row := querySingleRow(t, conn, "SELECT ? AS b, ?::VARCHAR AS txt", bv, bv)
		gotB, err := row[0].Bit()
		if err != nil || gotB.String() != s {
			t.Fatalf("BIT %q round-trip = %q (%v)", s, gotB.String(), err)
		}
		if txt, _ := row[1].Varchar(); txt != s {
			t.Fatalf("BIT %q SQL text = %q", s, txt)
		}
	}

	// 2. BIGNUM bind & decode round-trip: negative huge, -1, 0, 1, positive huge (> 256 bits).
	bignumCases := []string{
		"-9876543210987654321098765432109876543210987654321098765432109876543210987654321",
		"-256",
		"-1",
		"0",
		"1",
		"255",
		"1234567890123456789012345678901234567890123456789012345678901234567890123456789",
	}
	for _, s := range bignumCases {
		bi, _ := new(big.Int).SetString(s, 10)
		bn, err := duckdb.BignumFromBigInt(bi)
		if err != nil {
			t.Fatal(err)
		}
		v := duckdb.BignumValue(bn)
		_, row := querySingleRow(t, conn, "SELECT ? AS bn, ?::VARCHAR AS txt", v, v)
		gotBN, err := row[0].Bignum()
		if err != nil || gotBN.String() != s {
			t.Fatalf("BIGNUM %q round-trip = %q (%v)", s, gotBN.String(), err)
		}
		if txt, _ := row[1].Varchar(); txt != s {
			t.Fatalf("BIGNUM %q SQL text = %q", s, txt)
		}
	}

	// 3. UNION bind & decode round-trip (including active NULL member and SQL NULL union).
	ut := duckdb.UnionType(
		duckdb.StructFieldType{Name: "num", Type: duckdb.ScalarType(duckdb.TypeInteger)},
		duckdb.StructFieldType{Name: "txt", Type: duckdb.ScalarType(duckdb.TypeVarchar)},
	)
	uNum, err := duckdb.NewUnionValue(ut, 0, duckdb.Int32Value(42))
	if err != nil {
		t.Fatal(err)
	}
	uTxt, err := duckdb.NewUnionValue(ut, 1, duckdb.VarcharValue("hello"))
	if err != nil {
		t.Fatal(err)
	}
	_, uRow := querySingleRow(t, conn, "SELECT ? AS u1, ? AS u2, NULL::UNION(num INTEGER, txt VARCHAR) AS u3", uNum, uTxt)
	g1, err := uRow[0].Union()
	if err != nil || g1.Tag != 0 || g1.TagName != "num" {
		t.Fatalf("u1 = %+v (%v)", g1, err)
	}
	if n, _ := g1.Value.Int32(); n != 42 {
		t.Fatalf("u1.Value = %d, want 42", n)
	}
	g2, err := uRow[1].Union()
	if err != nil || g2.Tag != 1 || g2.TagName != "txt" {
		t.Fatalf("u2 = %+v (%v)", g2, err)
	}
	if txt, _ := g2.Value.Varchar(); txt != "hello" {
		t.Fatalf("u2.Value = %q, want hello", txt)
	}
	if !uRow[2].IsNull() || uRow[2].Kind() != duckdb.TypeUnion {
		t.Fatalf("u3 = null:%v kind:%s", uRow[2].IsNull(), uRow[2].Kind())
	}

	// 4. GEOMETRY decode (WKB + built-in OGC:CRS84 CRS).
	gCols, gRow := querySingleRow(t, conn, "SELECT 'POINT (1 2)'::GEOMETRY AS g, 'POINT (3 4)'::GEOMETRY('OGC:CRS84') AS gcrs, NULL::GEOMETRY AS gnull")
	if gCols[0].Type.Kind != duckdb.TypeGeometry || gCols[1].Type.GeometryCRS != "OGC:CRS84" {
		t.Fatalf("geometry cols = %+v %+v", gCols[0].Type, gCols[1].Type)
	}
	geom, err := gRow[0].Geometry()
	if err != nil || len(geom.WKB) == 0 {
		t.Fatalf("geom = %+v (%v)", geom, err)
	}
	geomCRS, err := gRow[1].Geometry()
	if err != nil || len(geomCRS.WKB) == 0 || geomCRS.CRS != "OGC:CRS84" {
		t.Fatalf("geomCRS = %+v (%v)", geomCRS, err)
	}
	if !gRow[2].IsNull() || gRow[2].Kind() != duckdb.TypeGeometry {
		t.Fatalf("gnull = null:%v kind:%s", gRow[2].IsNull(), gRow[2].Kind())
	}

	// 5. DATE and TIMESTAMP ±infinity round-trip.
	_, infRow := querySingleRow(t, conn, "SELECT 'infinity'::DATE, '-infinity'::DATE, 'infinity'::TIMESTAMP, '-infinity'::TIMESTAMPTZ")
	if d, _ := infRow[0].Date(); !d.IsInfinity() || d.IsFinite() {
		t.Fatalf("+inf DATE = %+v", d)
	}
	if d, _ := infRow[1].Date(); !d.IsNegInfinity() || d.IsFinite() {
		t.Fatalf("-inf DATE = %+v", d)
	}
	if ts, _ := infRow[2].Timestamp(); !ts.IsInfinity() || ts.IsFinite() {
		t.Fatalf("+inf TIMESTAMP = %+v", ts)
	}
	if ts, _ := infRow[3].Timestamp(); !ts.IsNegInfinity() || ts.IsFinite() {
		t.Fatalf("-inf TIMESTAMPTZ = %+v", ts)
	}
}

func TestNativeQueryAppenderUpsertAndErrorKinds(t *testing.T) {
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

	if _, err := conn.Exec(ctx, "CREATE TABLE kv (id BIGINT PRIMARY KEY, val BIGINT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO kv VALUES (1, 10), (2, 20)"); err != nil {
		t.Fatal(err)
	}

	// Query Appender performing bulk UPSERT (ON CONFLICT DO UPDATE) across >2048 rows.
	app, err := conn.NewQueryAppender(
		ctx,
		"INSERT INTO kv SELECT id, val FROM src_batch ON CONFLICT (id) DO UPDATE SET val = kv.val + EXCLUDED.val",
		[]duckdb.Column{
			{Name: "id", Type: duckdb.ScalarType(duckdb.TypeBigint)},
			{Name: "val", Type: duckdb.ScalarType(duckdb.TypeBigint)},
		},
		"src_batch",
	)
	if err != nil {
		t.Fatalf("NewQueryAppender: %v", err)
	}
	for i := int64(1); i <= 2500; i++ {
		if err := app.AppendRow(duckdb.Int64Value(i), duckdb.Int64Value(100)); err != nil {
			_ = app.Close()
			t.Fatalf("AppendRow(%d): %v", i, err)
		}
	}
	if err := app.Close(); err != nil {
		t.Fatalf("QueryAppender.Close: %v", err)
	}

	_, check := querySingleRow(t, conn, "SELECT count(*), (SELECT val FROM kv WHERE id = 1), (SELECT val FROM kv WHERE id = 2500) FROM kv")
	if cnt, _ := check[0].Int64(); cnt != 2500 {
		t.Fatalf("count = %d, want 2500", cnt)
	}
	if v1, _ := check[1].Int64(); v1 != 110 {
		t.Fatalf("kv[1] = %d, want 110 (10+100 upserted)", v1)
	}
	if v2500, _ := check[2].Int64(); v2500 != 100 {
		t.Fatalf("kv[2500] = %d, want 100", v2500)
	}

	// Verify fine-grained ErrorKind on native constraint violation.
	_, err = conn.Exec(ctx, "INSERT INTO kv VALUES (1, 999)")
	var derr *duckdb.Error
	if !errors.As(err, &derr) || derr.Kind != duckdb.ErrorKindConstraint {
		t.Fatalf("PK conflict error = %v (kind=%v), want ErrorKindConstraint", err, derr)
	}
}
