package duckdbsql_test

import (
	"context"
	"database/sql"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
	"github.com/bububa/duckdbspike/duckdbsql"
)

func TestNativeDBInitMultiStmtAndErgonomicBinders(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()

	// 1. Failing WithDBInit aborts Connector creation cleanly.
	boom := errors.New("db init failed")
	if _, err := duckdbsql.NewConnector(lib, ":memory:", duckdbsql.WithDBInit(func(ctx context.Context, conn *duckdb.Conn) error {
		return boom
	})); !errors.Is(err, boom) {
		t.Fatalf("failing WithDBInit = %v, want boom", err)
	}

	// 2. WithDBInit runs strictly once even when sql.DB opens multiple pooled connections.
	var dbInitCalls atomic.Int32
	var connInitCalls atomic.Int32
	connector, err := duckdbsql.NewConnector(
		lib,
		":memory:?threads=2",
		duckdbsql.WithDBInit(func(ctx context.Context, conn *duckdb.Conn) error {
			dbInitCalls.Add(1)
			_, err := conn.ExecScript(ctx, `
				CREATE TYPE mood AS ENUM ('happy', 'neutral', 'busy');
				CREATE TABLE events (id BIGINT, tags VARCHAR[], nums BIGINT[], big BIGNUM, state mood);
			`)
			return err
		}),
		duckdbsql.WithConnInit(func(ctx context.Context, conn *duckdb.Conn) error {
			connInitCalls.Add(1)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)

	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			var one int
			if qerr := db.QueryRowContext(ctx, "SELECT 1").Scan(&one); qerr != nil {
				t.Errorf("concurrent ping: %v", qerr)
			}
		})
	}
	wg.Wait()
	if got := dbInitCalls.Load(); got != 1 {
		t.Fatalf("WithDBInit ran %d times, want 1", got)
	}
	if connInitCalls.Load() < 1 {
		t.Fatal("expected WithConnInit to run at least once")
	}

	// 3. Parameterized & named multi-statement ExecContext and QueryContext.
	if _, err := db.ExecContext(ctx, `
		CREATE TEMP TABLE scratch_ms (v INT);
		INSERT INTO events VALUES (?, ?, ?, ?, ?);
	`,
		int64(1),
		[]string{"go", "duckdb"},
		[]int64{10, 20, 30},
		new(big.Int).Exp(big.NewInt(10), big.NewInt(40), nil),
		duckdb.Enum{Label: "happy"},
	); err != nil {
		t.Fatalf("multi-statement parameterized ExecContext: %v", err)
	}

	var gotTags duckdb.ListValue
	var gotBig duckdb.Bignum
	var gotMood duckdb.Enum
	if err := db.QueryRowContext(ctx, `
		SET threads = 2;
		SELECT tags, big, state FROM events WHERE id = $target;
	`, sql.Named("target", int64(1))).Scan(&gotTags, &gotBig, &gotMood); err != nil {
		t.Fatalf("multi-statement named QueryContext: %v", err)
	}
	if len(gotTags.Elements) != 2 || gotMood.Label != "happy" || gotBig.String() != "10000000000000000000000000000000000000000" {
		t.Fatalf("unexpected row: tags=%d mood=%+v big=%s", len(gotTags.Elements), gotMood, gotBig.String())
	}

	// 4. Composite[T] round-trip (Value() -> SQL -> Scan()), pointers, and UnionValue.
	type Point struct {
		X int32 `db:"x"`
		Y int32 `db:"y"`
	}
	type Item struct {
		Name   string  `db:"name"`
		Scores []int32 `db:"scores"`
		Origin Point   `db:"origin"`
		Note   *string `db:"note"`
	}
	noteStr := "hi"
	inItem := duckdbsql.NewComposite(Item{
		Name:   "widget",
		Scores: []int32{3, 7},
		Origin: Point{X: 1, Y: 2},
		Note:   &noteStr,
	})
	var outItem duckdbsql.Composite[Item]
	var outNull duckdbsql.Composite[Item]
	var nilPtr *int64
	nonNilPtr := int64(99)
	var gotNil sql.NullInt64
	var gotVal int64

	if err := db.QueryRowContext(ctx, `SELECT ?, ?, ?, ?`,
		inItem,
		duckdbsql.NullComposite[Item](),
		nilPtr,
		&nonNilPtr,
	).Scan(&outItem, &outNull, &gotNil, &gotVal); err != nil {
		t.Fatalf("Composite & pointer round-trip: %v", err)
	}
	if !outItem.Valid() || outNull.Valid() || gotNil.Valid || gotVal != 99 {
		t.Fatalf("unexpected flags: outItem=%v outNull=%v gotNil=%v gotVal=%d", outItem.Valid(), outNull.Valid(), gotNil.Valid, gotVal)
	}
	got := outItem.Get()
	if got.Name != "widget" || len(got.Scores) != 2 || got.Scores[1] != 7 || got.Origin.Y != 2 || got.Note == nil || *got.Note != "hi" {
		t.Fatalf("Composite round-trip mismatch: %+v", got)
	}

	// UnionValue round-trip binding via duckdb.UnionValue carrying its UnionType.
	var uOut duckdb.UnionValue
	if err := db.QueryRowContext(ctx, `SELECT union_value(num := 7::INTEGER)::UNION(num INTEGER, txt VARCHAR)`).Scan(&uOut); err != nil {
		t.Fatalf("scan UnionValue: %v", err)
	}
	var uRound duckdb.UnionValue
	if err := db.QueryRowContext(ctx, `SELECT ?`, uOut).Scan(&uRound); err != nil {
		t.Fatalf("bind UnionValue round-trip: %v", err)
	}
	if uRound.TagName != "num" {
		t.Fatalf("UnionValue round-trip = %+v", uRound)
	}
	if n, err := uRound.Value.Int32(); err != nil || n != 7 {
		t.Fatalf("UnionValue inner = %d (%v), want 7", n, err)
	}
}
