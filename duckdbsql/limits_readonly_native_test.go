package duckdbsql_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
	"github.com/bububa/duckdbspike/duckdbsql"
)

func TestNativeDatabaseSQLLimitsAndReadOnlyConnector(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()

	// 1. WithDecodeLimits & WithResultLimits enforcement.
	limitConn, err := duckdbsql.NewConnector(
		lib,
		":memory:?threads=1",
		duckdbsql.WithDecodeLimits(duckdb.DecodeLimits{MaxValueBytes: 8}),
		duckdbsql.WithResultLimits(duckdb.ResultLimits{MaxRows: 5}),
	)
	if err != nil {
		t.Fatal(err)
	}
	limitDB := sql.OpenDB(limitConn)
	defer limitDB.Close()

	var bigStr string
	err = limitDB.QueryRowContext(ctx, "SELECT repeat('x', 32)").Scan(&bigStr)
	if !errors.Is(err, duckdb.ErrLimitExceeded) {
		t.Fatalf("WithDecodeLimits error = %v, want ErrLimitExceeded", err)
	}

	rows, err := limitDB.QueryContext(ctx, "SELECT range FROM range(20)")
	if err != nil {
		t.Fatalf("QueryContext range(20): %v", err)
	}
	var count int
	for rows.Next() {
		var v int64
		if scanErr := rows.Scan(&v); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
		count++
	}
	rowsErr := rows.Err()
	_ = rows.Close()
	if !errors.Is(rowsErr, duckdb.ErrBudgetExceeded) {
		t.Fatalf("WithResultLimits error = %v (count=%d), want ErrBudgetExceeded", rowsErr, count)
	}

	// 2. NewReadOnlyConnector over a sealed ReadOnlySession with Parquet views.
	ws, err := duckdb.NewSessionWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()

	// Produce a sealed Parquet file inside ws.InputDir() using a separate writer DB.
	parquetPath := filepath.Join(ws.InputDir(), "orders.parquet")
	writerDB, err := lib.Open(ctx, ":memory:", duckdb.Option{Name: "threads", Value: "1"})
	if err != nil {
		t.Fatal(err)
	}
	wConn, err := writerDB.Connect(ctx)
	if err != nil {
		_ = writerDB.Close()
		t.Fatal(err)
	}
	if _, err := wConn.Exec(ctx, "COPY (SELECT i::BIGINT AS id, (i * 10)::BIGINT AS total FROM range(1, 51) t(i)) TO '"+parquetPath+"' (FORMAT PARQUET)"); err != nil {
		_ = wConn.Close()
		_ = writerDB.Close()
		t.Fatal(err)
	}
	_ = wConn.Close()
	_ = writerDB.Close()

	session, err := ws.OpenReadOnly(ctx, lib, duckdb.ReadOnlySessionSpec{
		Config: duckdb.InstanceConfig{
			NativeMemoryBytes: 128 << 20,
			Threads:           2,
		},
		Views: []duckdb.ViewSpec{
			{Name: "orders", Files: []string{parquetPath}},
		},
	})
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer session.Close()

	roConnector, err := duckdbsql.NewReadOnlyConnector(session,
		duckdbsql.WithResultLimits(duckdb.ResultLimits{MaxRows: 100}),
	)
	if err != nil {
		t.Fatal(err)
	}
	roDB := sql.OpenDB(roConnector)
	roDB.SetMaxOpenConns(4)

	// Concurrent pooled queries against the sealed read-only view.
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Go(func() {
			var cnt, sum int64
			if qerr := roDB.QueryRowContext(ctx, "SELECT count(*), sum(total) FROM orders").Scan(&cnt, &sum); qerr != nil {
				errs <- qerr
				return
			}
			if cnt != 50 || sum != 12750 {
				errs <- errors.New("unexpected aggregate result")
			}
		})
	}
	wg.Wait()
	close(errs)
	for qerr := range errs {
		t.Fatalf("pooled read-only query failed: %v", qerr)
	}

	// Mutations must fail on the read-only session.
	if _, err := roDB.ExecContext(ctx, "CREATE TABLE forbidden (x INT)"); err == nil {
		t.Fatal("expected write error on ReadOnlyConnector")
	}

	// Closing sql.DB / Connector must NOT close the caller-owned ReadOnlySession.
	if err := roDB.Close(); err != nil {
		t.Fatalf("roDB.Close: %v", err)
	}
	directConn, err := session.Connect(ctx)
	if err != nil {
		t.Fatalf("session.Connect after roDB.Close failed (session was wrongly closed): %v", err)
	}
	_ = directConn.Close()
}
