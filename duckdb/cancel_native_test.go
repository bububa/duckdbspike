package duckdb_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bububa/duckdbspike/duckdb"
)

func TestNativeCancelMidExecutionSortAndStreamFetch(t *testing.T) {
	lib := loadNativeLibrary(t)
	db, err := lib.Open(context.Background(), ":memory:", duckdb.Option{Name: "threads", Value: "4"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// 1) Long blocking aggregation canceled via context deadline.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel1()
	t0 := time.Now()
	_, err = conn.Query(ctx1, "SELECT sum(hash(i)) FROM range(50000000000) t(i)")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("long agg err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(t0); elapsed > 5*time.Second {
		t.Fatalf("cancel took too long: %s", elapsed)
	}
	// Connection immediately reusable after cancel joined.
	_, vals := querySingleRow(t, conn, "SELECT 42::BIGINT")
	if v, _ := vals[0].Int64(); v != 42 {
		t.Fatalf("reuse after agg cancel got %d, want 42", v)
	}

	// 2) Blocking sort canceled mid-execution.
	ctx2, cancel2 := context.WithCancel(context.Background())
	time.AfterFunc(150*time.Millisecond, cancel2)
	_, err = conn.Query(ctx2, "SELECT count(*) FROM (SELECT i FROM range(200000000) t(i) ORDER BY hash(i))")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("blocking sort err = %v, want context.Canceled", err)
	}
	_, vals = querySingleRow(t, conn, "SELECT 77::BIGINT")
	if v, _ := vals[0].Int64(); v != 77 {
		t.Fatalf("reuse after sort cancel got %d, want 77", v)
	}

	// 3) Streaming fetch canceled mid-stream must surface context.Canceled, not EOF.
	ctx3, cancel3 := context.WithCancel(context.Background())
	defer cancel3()
	rows, err := conn.Query(ctx3, "SELECT i, repeat('x', 64) FROM range(1000000000000) t(i)")
	if err != nil {
		t.Fatal(err)
	}
	var seen int
	for rows.Next() {
		seen++
		if seen == 10000 {
			cancel3()
		}
	}
	if !errors.Is(rows.Err(), context.Canceled) {
		_ = rows.Close()
		t.Fatalf("streaming cancel rows.Err() = %v, want context.Canceled", rows.Err())
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close: %v", err)
	}
	_, vals = querySingleRow(t, conn, "SELECT 99::BIGINT")
	if v, _ := vals[0].Int64(); v != 99 {
		t.Fatalf("reuse after stream cancel got %d, want 99", v)
	}
}

func TestNativeConcurrentCloseInterruptsAndJoins(t *testing.T) {
	lib := loadNativeLibrary(t)

	// 1) Concurrent Rows.Close while streaming.
	db, err := lib.Open(context.Background(), ":memory:", duckdb.Option{Name: "threads", Value: "2"})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Connect(context.Background())
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	rows, err := conn.Query(context.Background(), "SELECT i, hash(i) FROM range(1000000000000) t(i)")
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	started := make(chan struct{})
	iterDone := make(chan error, 1)
	go func() {
		n := 0
		for rows.Next() {
			n++
			if n == 5000 {
				close(started)
			}
		}
		iterDone <- rows.Err()
	}()
	<-started
	if err := rows.Close(); err != nil {
		t.Fatalf("concurrent Rows.Close: %v", err)
	}
	if err := <-iterDone; !errors.Is(err, duckdb.ErrClosed) {
		t.Fatalf("concurrent Next err = %v, want ErrClosed", err)
	}
	// Connection reusable after concurrent Rows.Close.
	_, vals := querySingleRow(t, conn, "SELECT 123::BIGINT")
	if v, _ := vals[0].Int64(); v != 123 {
		t.Fatalf("reuse got %d", v)
	}
	_ = db.Close()

	// 2) Concurrent Database.Close while two connections run blocking queries.
	db2, err := lib.Open(context.Background(), ":memory:", duckdb.Option{Name: "threads", Value: "4"})
	if err != nil {
		t.Fatal(err)
	}
	c1, _ := db2.Connect(context.Background())
	c2, _ := db2.Connect(context.Background())
	errs := make(chan error, 2)
	go func() {
		_, e := c1.Query(context.Background(), "SELECT sum(hash(i)) FROM range(50000000000) t(i)")
		errs <- e
	}()
	go func() {
		_, e := c2.Query(context.Background(), "SELECT sum(hash(i)) FROM range(50000000000) t(i)")
		errs <- e
	}()
	time.Sleep(100 * time.Millisecond)
	if err := db2.Close(); err != nil {
		t.Fatalf("db2.Close: %v", err)
	}
	for i := range 2 {
		if e := <-errs; !errors.Is(e, duckdb.ErrClosed) {
			t.Fatalf("query[%d] after db2.Close = %v, want ErrClosed", i, e)
		}
	}
}
