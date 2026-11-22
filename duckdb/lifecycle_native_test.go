package duckdb_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
)

var (
	nativeLibOnce sync.Once
	nativeLib     *duckdb.Library
	nativeLibErr  error
)

func loadNativeLibrary(t *testing.T) *duckdb.Library {
	t.Helper()
	libPath := os.Getenv("DUCKDB_LIB_PATH")
	libSHA := os.Getenv("DUCKDB_LIB_SHA256")
	if libPath == "" || libSHA == "" {
		t.Skip("DUCKDB_LIB_PATH / DUCKDB_LIB_SHA256 not set; skipping real native test")
	}
	nativeLibOnce.Do(func() {
		nativeLib, nativeLibErr = duckdb.Load(libPath, libSHA)
	})
	if nativeLibErr != nil {
		t.Fatalf("duckdb.Load failed: %v", nativeLibErr)
	}
	return nativeLib
}

func TestNativeLifecycleStreamingAndErrors(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()

	// :memory: + READ_ONLY must be rejected cleanly.
	if _, err := lib.Open(ctx, ":memory:", duckdb.Option{Name: "access_mode", Value: "READ_ONLY"}); err == nil {
		t.Fatal("expected :memory: READ_ONLY to fail")
	}

	db, err := lib.Open(ctx, ":memory:", duckdb.Option{Name: "threads", Value: "2"})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	conn, err := db.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	// Exec handles non-streaming DDL and DML and copies RowsAffected.
	if _, err := conn.Exec(ctx, "CREATE TABLE items (id INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	res, err := conn.Exec(ctx, "INSERT INTO items VALUES (1), (2), (3)")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if res.RowsAffected != 3 {
		t.Fatalf("RowsAffected = %d, want 3", res.RowsAffected)
	}

	// Streaming query over 5000 rows, plus zero-row query and duplicate column names.
	for _, tc := range []struct {
		sql      string
		wantRows int
		wantCols []string
	}{
		{"SELECT 1 AS a, 2 AS a", 1, []string{"a", "a"}},
		{"SELECT 1 AS x WHERE false", 0, []string{"x"}},
		{"SELECT i FROM range(5000) t(i)", 5000, []string{"i"}},
	} {
		rows, err := conn.Query(ctx, tc.sql)
		if err != nil {
			t.Fatalf("Query(%q): %v", tc.sql, err)
		}
		var total int
		for rows.Next() {
			total += rows.ChunkRowCount()
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatalf("Rows.Err(%q): %v", tc.sql, err)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("Rows.Close(%q): %v", tc.sql, err)
		}
		if total != tc.wantRows {
			t.Fatalf("Query(%q) rows = %d, want %d", tc.sql, total, tc.wantRows)
		}
		cols := rows.Columns()
		if len(cols) != len(tc.wantCols) {
			t.Fatalf("Query(%q) cols = %+v, want %v", tc.sql, cols, tc.wantCols)
		}
		for i, name := range tc.wantCols {
			if cols[i].Name != name {
				t.Fatalf("Query(%q) col[%d] = %q, want %q", tc.sql, i, cols[i].Name, name)
			}
		}
	}

	// Prepare error and execution error must not break subsequent queries.
	if _, err := conn.Query(ctx, "SELECT FROM"); err == nil {
		t.Fatal("expected syntax error")
	}

	// Late streaming fetch error must be propagated via Rows.Err(), not treated as EOF.
	rows, err := conn.Query(ctx, "SELECT CASE WHEN i = 5000 THEN error('boom') ELSE i END FROM range(20000) t(i)")
	if err == nil {
		for rows.Next() {
		}
		err = rows.Err()
		_ = rows.Close()
	}
	var derr *duckdb.Error
	if !errors.As(err, &derr) {
		t.Fatalf("late fetch error = %v, want *duckdb.Error", err)
	}

	// Exclusive lease with prepared statement.
	stmt, err := conn.Prepare(ctx, "SELECT i FROM range(10) t(i)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if n, err := stmt.ParamCount(); err != nil || n != 0 {
		t.Fatalf("ParamCount = %d, %v", n, err)
	}
	r, err := stmt.Query(ctx)
	if err != nil {
		t.Fatalf("Stmt.Query: %v", err)
	}
	if err := stmt.Close(); !errors.Is(err, duckdb.ErrBusy) {
		t.Fatalf("Stmt.Close while Rows open = %v, want ErrBusy", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Rows.Close: %v", err)
	}
	if err := stmt.Close(); err != nil {
		t.Fatalf("Stmt.Close after Rows.Close: %v", err)
	}
}

func TestNativeFileDatabaseAndGCPressure(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test.duckdb")

	db, err := lib.Open(ctx, dbPath, duckdb.Option{Name: "threads", Value: "2"})
	if err != nil {
		t.Fatalf("Open file db: %v", err)
	}
	conn, err := db.Connect(ctx)
	if err != nil {
		_ = db.Close()
		t.Fatalf("Connect: %v", err)
	}
	for i := 0; i < 50; i++ {
		rows, err := conn.Query(ctx, "SELECT i FROM range(4096) t(i)")
		if err != nil {
			t.Fatalf("iter %d Query: %v", i, err)
		}
		var n int
		for rows.Next() {
			n += rows.ChunkRowCount()
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iter %d Err: %v", i, err)
		}
		cols := rows.Columns()
		if err := rows.Close(); err != nil {
			t.Fatalf("iter %d Close: %v", i, err)
		}
		if i%10 == 0 {
			runtime.GC()
		}
		if n != 4096 || len(cols) != 1 || cols[0].Name != "i" {
			t.Fatalf("iter %d unexpected n=%d cols=%+v", i, n, cols)
		}
	}
	// Cascade close from Database while conn is still open.
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second db.Close: %v", err)
	}
}
