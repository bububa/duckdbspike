package duckdbsql_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bububa/duckdbspike/duckdb"
	"github.com/bububa/duckdbspike/duckdbarrow"
	"github.com/bububa/duckdbspike/duckdbsql"
)

func loadNativeLibrary(t *testing.T) *duckdb.Library {
	t.Helper()
	libPath := os.Getenv("DUCKDB_LIB_PATH")
	libSHA := os.Getenv("DUCKDB_LIB_SHA256")
	if libPath == "" || libSHA == "" {
		t.Skip("DUCKDB_LIB_PATH / DUCKDB_LIB_SHA256 not set; skipping real native test")
	}
	lib, err := duckdb.Load(libPath, libSHA)
	if err != nil {
		t.Fatalf("duckdb.Load: %v", err)
	}
	return lib
}

func TestNativeDatabaseSQLConnectorPoolTxAndRawConn(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()

	cache, err := lib.NewInstanceCache()
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	dbPath := filepath.Join(t.TempDir(), "sql.duckdb")
	var initCount int
	var initMu sync.Mutex
	connector, err := duckdbsql.NewConnector(
		lib,
		dbPath+"?threads=2",
		duckdbsql.WithInstanceCache(cache),
		duckdbsql.WithConnInit(func(ctx context.Context, conn *duckdb.Conn) error {
			initMu.Lock()
			initCount++
			initMu.Unlock()
			_, err := conn.Exec(ctx, "SET enable_profiling = 'no_output'")
			return err
		}),
	)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(4)

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}

	// Multi-statement ExecContext script.
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE items (id BIGINT PRIMARY KEY, name VARCHAR, price DECIMAL(10, 2), created_at TIMESTAMPTZ);
		INSERT INTO items VALUES (1, 'alpha', 12.50, TIMESTAMPTZ '2026-01-02 03:04:05.123456+00');
	`); err != nil {
		t.Fatalf("ExecContext script: %v", err)
	}

	// Named parameters + time.Time input + exact Option A default scanning.
	tm := time.Date(2026, 2, 3, 4, 5, 6, 654321000, time.UTC)
	dec, _ := duckdb.DecimalFromInt64(10, 2, 9995)
	if _, err := db.ExecContext(ctx,
		"INSERT INTO items VALUES ($id, $name, $price, $ts)",
		sql.Named("id", int64(2)),
		sql.Named("name", "beta"),
		sql.Named("price", dec),
		sql.Named("ts", tm),
	); err != nil {
		t.Fatalf("ExecContext named: %v", err)
	}

	// Prepared statement + ColumnTypes inspection + Option A exact types.
	stmt, err := db.PrepareContext(ctx, "SELECT id, name, price, created_at FROM items WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := stmt.QueryContext(ctx, int64(2))
	if err != nil {
		_ = stmt.Close()
		t.Fatal(err)
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	if len(colTypes) != 4 {
		t.Fatalf("colTypes len = %d, want 4", len(colTypes))
	}
	if p, s, ok := colTypes[2].DecimalSize(); !ok || p != 10 || s != 2 {
		t.Fatalf("DecimalSize = (%d, %d, %v), want (10, 2, true)", p, s, ok)
	}
	if !rows.Next() {
		t.Fatalf("expected row, err=%v", rows.Err())
	}
	var gotID int64
	var gotName string
	var gotPrice duckdb.Decimal
	var gotTS duckdb.Timestamp
	if err := rows.Scan(&gotID, &gotName, &gotPrice, &gotTS); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	_ = rows.Close()
	_ = stmt.Close()
	if gotID != 2 || gotName != "beta" || gotPrice != dec || gotTS.Unit != duckdb.TimestampUnitTZ || gotTS.Value != tm.UnixMicro() {
		t.Fatalf("scanned = (%d, %q, %+v, %+v)", gotID, gotName, gotPrice, gotTS)
	}

	// Transactions: commit, rollback, read-only, unsupported isolation.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO items VALUES (3, 'gamma', 1.00, TIMESTAMPTZ '2026-01-01 00:00:00+00')"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM items").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("count after rollback = %d, want 2", count)
	}
	if _, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable}); !errors.Is(err, duckdb.ErrInvalidInput) {
		t.Fatalf("unsupported isolation = %v, want ErrInvalidInput", err)
	}

	// RawConn escape hatch: Appender + duckdbarrow.Query + ProfilingInfo.
	sc, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := duckdbsql.RawConn(ctx, sc, func(dc *duckdb.Conn) error {
		app, err := dc.NewAppender(ctx, "", "", "items")
		if err != nil {
			return err
		}
		if err := app.AddColumn("id"); err != nil {
			_ = app.Close()
			return err
		}
		if err := app.AddColumn("name"); err != nil {
			_ = app.Close()
			return err
		}
		if err := app.AppendRow(duckdb.Int64Value(10), duckdb.VarcharValue("via-raw-appender")); err != nil {
			_ = app.Close()
			return err
		}
		if err := app.Close(); err != nil {
			return err
		}
		reader, err := duckdbarrow.Query(ctx, dc, nil, "SELECT id, name FROM items WHERE id = 10")
		if err != nil {
			return err
		}
		defer reader.Release()
		if !reader.Next() || reader.RecordBatch().NumRows() != 1 {
			return errors.New("expected 1 row from duckdbarrow.Query via RawConn")
		}
		return reader.Err()
	}); err != nil {
		_ = sc.Close()
		t.Fatalf("RawConn: %v", err)
	}
	if err := sc.Close(); err != nil {
		t.Fatal(err)
	}

	// Concurrent pooled queries across goroutines.
	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for g := range 8 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var n int64
			if err := db.QueryRowContext(ctx, "SELECT count(*) + ?", int64(id)).Scan(&n); err != nil {
				errCh <- err
				return
			}
			if n != int64(id)+1 {
				errCh <- errors.New("unexpected pooled result")
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Fatalf("concurrent query error: %v", e)
	}
	initMu.Lock()
	if initCount == 0 {
		t.Fatal("WithConnInit was never called")
	}
	initMu.Unlock()
}

func TestNativeDatabaseSQLWithTimeScanAndRegister(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()

	// Test WithTimeScan(true) via Register + sql.Open.
	const drvName = "duckdbspike_test_timescan"
	duckdbsql.Register(drvName, lib, duckdbsql.WithTimeScan(true))
	db, err := sql.Open(drvName, ":memory:?threads=2")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var gotDate, gotTS time.Time
	if err := db.QueryRowContext(ctx,
		"SELECT DATE '2026-07-08', TIMESTAMPTZ '2026-07-08 09:10:11.123456+00'",
	).Scan(&gotDate, &gotTS); err != nil {
		t.Fatalf("WithTimeScan Scan: %v", err)
	}
	wantDate := time.Date(2026, 7, 8, 0, 0, 0, 0, time.UTC)
	wantTS := time.Date(2026, 7, 8, 9, 10, 11, 123456000, time.UTC)
	if !gotDate.Equal(wantDate) {
		t.Fatalf("gotDate = %v, want %v", gotDate, wantDate)
	}
	if !gotTS.Equal(wantTS) {
		t.Fatalf("gotTS = %v, want %v", gotTS, wantTS)
	}
}
