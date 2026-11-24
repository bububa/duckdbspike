package duckdbsql_test

import (
	"context"
	"database/sql"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/bububa/duckdbspike/duckdb"
	"github.com/bububa/duckdbspike/duckdbsql"
)

func TestNativeDatabaseSQLCompositeScannerInfinityGuardAndQueryScript(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()

	connector, err := duckdbsql.NewConnector(lib, ":memory:?threads=2")
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	// 1. Composite[T] scanning nested STRUCT + LIST + NULL + sql.Scanner fields.
	type Address struct {
		City string `db:"city"`
		Zip  *int32 `db:"zip"`
	}
	type Profile struct {
		ID      int64          `db:"id"`
		Name    string         `db:"name"`
		Balance duckdb.Decimal `db:"balance"`
		Scores  []int32        `db:"scores"`
		Addr    Address        `db:"addr"`
		Note    sql.NullString `db:"note"`
	}
	var prof duckdbsql.Composite[Profile]
	var nullProf duckdbsql.Composite[Profile]
	if err := db.QueryRowContext(ctx, `SELECT
		{
			'id': 42::BIGINT,
			'name': 'alice',
			'balance': 123.45::DECIMAL(10, 2),
			'scores': [10::INTEGER, 20::INTEGER],
			'addr': {'city': 'Shanghai', 'zip': 200000::INTEGER},
			'note': NULL::VARCHAR
		},
		NULL::STRUCT(id BIGINT, name VARCHAR)`).Scan(&prof, &nullProf); err != nil {
		t.Fatalf("Composite Scan: %v", err)
	}
	if !prof.Valid() {
		t.Fatal("expected valid prof")
	}
	p := prof.Get()
	if p.ID != 42 || p.Name != "alice" || p.Balance.String() != "123.45" ||
		len(p.Scores) != 2 || p.Scores[1] != 20 ||
		p.Addr.City != "Shanghai" || p.Addr.Zip == nil || *p.Addr.Zip != 200000 ||
		p.Note.Valid {
		t.Fatalf("unexpected decoded Profile: %+v", p)
	}
	if nullProf.Valid() {
		t.Fatal("expected NULL struct to set Composite.Valid() == false")
	}

	// 2. sql.Scanner / driver.Valuer on Bit, Bignum, UnionValue, UUID.
	bitIn, _ := duckdb.NewBitFromString("10110")
	bigIn, _ := new(big.Int).SetString("999999999999999999999999999999999999999", 10)
	bnIn, _ := duckdb.BignumFromBigInt(bigIn)
	uIn, _ := duckdb.ParseUUID("01234567-89ab-cdef-0123-456789abcdef")

	var gotBit duckdb.Bit
	var gotBN duckdb.Bignum
	var gotUUID duckdb.UUID
	var gotUnion duckdb.UnionValue
	if err := db.QueryRowContext(ctx,
		"SELECT ?, ?, ?, union_value(a := 7::INTEGER)::UNION(a INTEGER, b VARCHAR)",
		bitIn, bnIn, uIn,
	).Scan(&gotBit, &gotBN, &gotUUID, &gotUnion); err != nil {
		t.Fatalf("P2a Scan: %v", err)
	}
	if gotBit.String() != "10110" || gotBN.String() != bigIn.String() || gotUUID != uIn || gotUnion.TagName != "a" {
		t.Fatalf("P2a scanned = (%q, %q, %v, %+v)", gotBit.String(), gotBN.String(), gotUUID, gotUnion)
	}

	// 3. Multi-statement QueryContext (QueryScript).
	var total int64
	if err := db.QueryRowContext(ctx, `
		CREATE TEMP TABLE qscript_t (v BIGINT);
		INSERT INTO qscript_t VALUES (10), (20), (30);
		SELECT sum(v) FROM qscript_t WHERE v >= ?;
	`, int64(20)).Scan(&total); err != nil {
		t.Fatalf("multi-statement QueryRowContext: %v", err)
	}
	if total != 50 {
		t.Fatalf("total = %d, want 50", total)
	}

	// 4. WithTimeScan(true) rejects non-finite ±infinity DATE/TIMESTAMP with ErrOutOfRange.
	tsConn, err := duckdbsql.NewConnector(lib, ":memory:", duckdbsql.WithTimeScan(true))
	if err != nil {
		t.Fatal(err)
	}
	tsDB := sql.OpenDB(tsConn)
	defer tsDB.Close()

	var tm time.Time
	if err := tsDB.QueryRowContext(ctx, "SELECT 'infinity'::DATE").Scan(&tm); !errors.Is(err, duckdb.ErrOutOfRange) {
		t.Fatalf("+inf DATE with WithTimeScan = %v, want ErrOutOfRange", err)
	}
	if err := tsDB.QueryRowContext(ctx, "SELECT '-infinity'::TIMESTAMPTZ").Scan(&tm); !errors.Is(err, duckdb.ErrOutOfRange) {
		t.Fatalf("-inf TIMESTAMPTZ with WithTimeScan = %v, want ErrOutOfRange", err)
	}
}
