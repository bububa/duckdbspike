package duckdbsql_test

import (
	"context"
	"database/sql"
	"math/big"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
	"github.com/bububa/duckdbspike/duckdbsql"
)

func TestNativeSQLNullGenericRoundTrip(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()

	connector, err := duckdbsql.NewConnector(lib, ":memory:?threads=2")
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	u, err := duckdb.ParseUUID("01234567-89ab-cdef-0123-456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	dec := duckdb.Decimal{Width: 10, Scale: 2, Value: duckdb.HugeintFromInt64(98765)}
	h := duckdb.HugeintFromInt64(-1234567890)
	d := duckdb.Date{Days: 19723}
	ts := duckdb.Timestamp{Unit: duckdb.TimestampUnitMicros, Value: 1_700_000_000_123_456}
	ttz := duckdb.TimeTZ{Micros: 3_600_000_000, OffsetSeconds: -18000}
	iv := duckdb.Interval{Months: 3, Days: 4, Micros: 5000}
	bitVal, err := duckdb.NewBitFromString("1010101")
	if err != nil {
		t.Fatal(err)
	}
	bn, err := duckdb.BignumFromBigInt(big.NewInt(1234567890123456789))
	if err != nil {
		t.Fatal(err)
	}

	var (
		gotU   sql.Null[duckdb.UUID]
		gotUN  sql.Null[duckdb.UUID]
		gotDec sql.Null[duckdb.Decimal]
		gotH   sql.Null[duckdb.Hugeint]
		gotD   sql.Null[duckdb.Date]
		gotTS  sql.Null[duckdb.Timestamp]
		gotTTZ sql.Null[duckdb.TimeTZ]
		gotIV  sql.Null[duckdb.Interval]
		gotBit sql.Null[duckdb.Bit]
		gotBN  sql.Null[duckdb.Bignum]
	)
	err = db.QueryRowContext(ctx,
		"SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?",
		sql.Null[duckdb.UUID]{V: u, Valid: true},
		sql.Null[duckdb.UUID]{Valid: false},
		sql.Null[duckdb.Decimal]{V: dec, Valid: true},
		sql.Null[duckdb.Hugeint]{V: h, Valid: true},
		sql.Null[duckdb.Date]{V: d, Valid: true},
		sql.Null[duckdb.Timestamp]{V: ts, Valid: true},
		sql.Null[duckdb.TimeTZ]{V: ttz, Valid: true},
		sql.Null[duckdb.Interval]{V: iv, Valid: true},
		sql.Null[duckdb.Bit]{V: bitVal, Valid: true},
		sql.Null[duckdb.Bignum]{V: bn, Valid: true},
	).Scan(&gotU, &gotUN, &gotDec, &gotH, &gotD, &gotTS, &gotTTZ, &gotIV, &gotBit, &gotBN)
	if err != nil {
		t.Fatalf("QueryRow sql.Null[T] round-trip failed: %v", err)
	}
	if !gotU.Valid || gotU.V != u {
		t.Fatalf("UUID mismatch: got %+v, want %v", gotU, u)
	}
	if gotUN.Valid {
		t.Fatalf("expected NULL UUID, got %+v", gotUN)
	}
	if !gotDec.Valid || gotDec.V != dec {
		t.Fatalf("Decimal mismatch: got %+v, want %+v", gotDec, dec)
	}
	if !gotH.Valid || gotH.V != h {
		t.Fatalf("Hugeint mismatch: got %+v, want %+v", gotH, h)
	}
	if !gotD.Valid || gotD.V != d {
		t.Fatalf("Date mismatch: got %+v, want %+v", gotD, d)
	}
	if !gotTS.Valid || gotTS.V != ts {
		t.Fatalf("Timestamp mismatch: got %+v, want %+v", gotTS, ts)
	}
	if !gotTTZ.Valid || gotTTZ.V != ttz {
		t.Fatalf("TimeTZ mismatch: got %+v, want %+v", gotTTZ, ttz)
	}
	if !gotIV.Valid || gotIV.V != iv {
		t.Fatalf("Interval mismatch: got %+v, want %+v", gotIV, iv)
	}
	if !gotBit.Valid || gotBit.V.String() != bitVal.String() {
		t.Fatalf("Bit mismatch: got %+v, want %v", gotBit, bitVal)
	}
	if !gotBN.Valid || gotBN.V.String() != bn.String() {
		t.Fatalf("Bignum mismatch: got %+v, want %v", gotBN, bn)
	}

	// sql.Null[T] inside Composite[S] (both valid and NULL fields) and slice of sql.Null[duckdb.UUID].
	type Record struct {
		ID   sql.Null[duckdb.UUID]    `db:"id"`
		Cost sql.Null[duckdb.Decimal] `db:"cost"`
		When sql.Null[duckdb.Date]    `db:"when"`
	}
	inRec := Record{
		ID:   sql.Null[duckdb.UUID]{V: u, Valid: true},
		Cost: sql.Null[duckdb.Decimal]{V: dec, Valid: true},
		When: sql.Null[duckdb.Date]{Valid: false},
	}
	var outRec duckdbsql.Composite[Record]
	if err := db.QueryRowContext(ctx, "SELECT ?", duckdbsql.NewComposite(inRec)).Scan(&outRec); err != nil {
		t.Fatalf("Composite with sql.Null[T] failed: %v", err)
	}
	if !outRec.Valid() {
		t.Fatal("expected valid Composite")
	}
	gotRec := outRec.Get()
	if !gotRec.ID.Valid || gotRec.ID.V != u || !gotRec.Cost.Valid || gotRec.Cost.V != dec || gotRec.When.Valid {
		t.Fatalf("Composite sql.Null[T] mismatch: got %+v, want %+v", gotRec, inRec)
	}

	var listVal duckdb.ListValue
	if err := db.QueryRowContext(ctx, "SELECT ?", []sql.Null[duckdb.UUID]{{V: u, Valid: true}, {Valid: false}}).Scan(&listVal); err != nil {
		t.Fatalf("[]sql.Null[UUID] bind failed: %v", err)
	}
	if len(listVal.Elements) != 2 || !listVal.Elements[1].IsNull() {
		t.Fatalf("[]sql.Null[UUID] mismatch: %+v", listVal)
	}
	if gotElemU, err := listVal.Elements[0].UUID(); err != nil || gotElemU != u {
		t.Fatalf("[]sql.Null[UUID][0] = (%v, %v), want %v", gotElemU, err, u)
	}
}
