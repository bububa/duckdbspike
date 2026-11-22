package duckdb_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
)

func sha256File(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func writeTestParquet(t *testing.T, lib *duckdb.Library, dest string, start, end int64) {
	t.Helper()
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
	q := "COPY (SELECT i AS seq, repeat('p', 96) AS payload FROM range(?, ?) t(i)) TO '" + strings.ReplaceAll(dest, "'", "''") + "' (FORMAT parquet)"
	if _, err := conn.Exec(ctx, q, duckdb.Int64Value(start), duckdb.Int64Value(end)); err != nil {
		t.Fatalf("write parquet %s: %v", dest, err)
	}
}

func TestNativeReadOnlySessionIsolationSpillAndCleanup(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()

	bc, err := duckdb.NewBudgetController(duckdb.GlobalBudget{
		MaxConcurrent:    1,
		MaxWaitQueue:     1,
		TotalMemoryBytes: 256 << 20,
		TotalThreads:     2,
		TotalSpillBytes:  1 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()

	baseDir := t.TempDir()
	ws, err := duckdb.NewSessionWorkspace(baseDir)
	if err != nil {
		t.Fatal(err)
	}
	rootDir := ws.RootDir()
	f1 := filepath.Join(ws.InputDir(), "part-1.parquet")
	f2 := filepath.Join(ws.InputDir(), "part-2.parquet")
	writeTestParquet(t, lib, f1, 0, 1_000_000)
	writeTestParquet(t, lib, f2, 1_000_000, 2_000_000)

	h1Before := sha256File(t, f1)
	h2Before := sha256File(t, f2)

	// Path escape outside workspace input/ must be rejected before opening DuckDB.
	outside := filepath.Join(baseDir, "outside.parquet")
	if err := os.WriteFile(outside, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.OpenReadOnly(ctx, lib, duckdb.ReadOnlySessionSpec{
		Config: duckdb.InstanceConfig{NativeMemoryBytes: 128 << 20, Threads: 2},
		Views:  []duckdb.ViewSpec{{Name: "facts", Files: []string{outside}}},
	}); !errors.Is(err, duckdb.ErrInvalidInput) {
		t.Fatalf("outside path = %v, want ErrInvalidInput", err)
	}

	// Unsafe SQL view name rejected.
	if _, err := ws.OpenReadOnly(ctx, lib, duckdb.ReadOnlySessionSpec{
		Config: duckdb.InstanceConfig{NativeMemoryBytes: 128 << 20, Threads: 2},
		Views:  []duckdb.ViewSpec{{Name: "facts; DROP TABLE x", Files: []string{f1}}},
	}); !errors.Is(err, duckdb.ErrInvalidInput) {
		t.Fatalf("bad view name = %v, want ErrInvalidInput", err)
	}

	session, err := ws.OpenReadOnly(ctx, lib, duckdb.ReadOnlySessionSpec{
		Budget: bc,
		Config: duckdb.InstanceConfig{
			NativeMemoryBytes: 128 << 20,
			Threads:           2,
			SpillBytes:        1 << 30,
		},
		Views: []duckdb.ViewSpec{
			{Name: "facts", Files: []string{f1, f2}},
		},
	})
	if err != nil {
		_ = ws.Close()
		t.Fatalf("OpenReadOnly: %v", err)
	}

	// Record catalog hash right after read-only reopen.
	catalogHashBefore := sha256File(t, ws.CatalogPath())

	// First connection reads persistent view & verifies re-applied settings.
	c1, err := session.Connect(ctx)
	if err != nil {
		_ = session.Close()
		t.Fatal(err)
	}
	_, vals := querySingleRow(t, c1, "SELECT count(*) FROM facts")
	if n, _ := vals[0].Int64(); n != 2_000_000 {
		t.Fatalf("count(*) = %d, want 2000000", n)
	}

	// Second connection sees persistent view and locked configuration.
	c2, err := session.Connect(ctx)
	if err != nil {
		_ = session.Close()
		t.Fatal(err)
	}
	_, vals2 := querySingleRow(t, c2, "SELECT max(seq) FROM facts")
	if mx, _ := vals2[0].Int64(); mx != 1_999_999 {
		t.Fatalf("max(seq) on c2 = %d, want 1999999", mx)
	}
	for _, check := range []struct {
		setting string
		want    string
	}{
		{"access_mode", "read_only"},
		{"enable_external_access", "false"},
		{"lock_configuration", "true"},
		{"threads", "2"},
	} {
		_, sv := querySingleRow(t, c2, "SELECT current_setting(?)::VARCHAR", duckdb.VarcharValue(check.setting))
		got, _ := sv[0].Varchar()
		if got != check.want {
			t.Fatalf("setting %s = %q, want %q", check.setting, got, check.want)
		}
	}

	// Unauthorized capabilities must all be denied.
	denied := []string{
		"CREATE TABLE x AS SELECT 1",
		"INSERT INTO facts VALUES (1, 'x')",
		"COPY (SELECT 1) TO '" + filepath.Join(ws.InputDir(), "evil.parquet") + "'",
		"SELECT * FROM read_csv('/etc/passwd')",
		"SELECT * FROM read_parquet('" + filepath.Join(ws.InputDir(), "..", "catalog", "query.duckdb") + "')",
		"SELECT * FROM glob('/etc/*')",
		"ATTACH '" + filepath.Join(ws.SpillDir(), "other.duckdb") + "'",
		"INSTALL httpfs",
		"LOAD httpfs",
		"SET memory_limit = '8GB'",
		"SET enable_external_access = true",
		"RESET lock_configuration",
		"SET temp_directory = '/tmp'",
	}
	for _, stmt := range denied {
		if _, err := c1.Exec(ctx, stmt); err == nil {
			t.Fatalf("expected denial for %q, succeeded", stmt)
		}
	}

	// Controlled sort spills under READ_ONLY + 128 MiB limit.
	rows, err := c1.Query(ctx, "SELECT seq, payload FROM facts ORDER BY hash(seq)")
	if err != nil {
		_ = session.Close()
		t.Fatalf("READ_ONLY spilling sort: %v", err)
	}
	var sorted int
	for rows.Next() {
		sorted++
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		_ = session.Close()
		t.Fatalf("READ_ONLY sort err: %v", err)
	}
	_ = rows.Close()
	if sorted != 2_000_000 {
		t.Fatalf("sorted rows = %d, want 2000000", sorted)
	}

	// Source Parquet files and catalog hashes must be byte-for-byte unchanged.
	if sha256File(t, f1) != h1Before || sha256File(t, f2) != h2Before {
		t.Fatal("source Parquet file was modified during READ_ONLY session")
	}
	if sha256File(t, ws.CatalogPath()) != catalogHashBefore {
		t.Fatal("catalog file was modified during READ_ONLY session")
	}

	// Close deletes the entire query-root (including 0400/0500 sealed files) and
	// releases the global budget reservation.
	if err := session.Close(); err != nil {
		t.Fatalf("session.Close: %v", err)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("idempotent session.Close: %v", err)
	}
	if _, err := os.Stat(rootDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("session root %s still exists after Close: %v", rootDir, err)
	}
	if snap := bc.Snapshot(); snap.ActiveInstances != 0 || snap.UsedMemoryBytes != 0 {
		t.Fatalf("budget leaked after session.Close: %+v", snap)
	}

	// Workspace closed before OpenReadOnly removes directory cleanly without
	// ever touching DuckDB.
	wsAbort, err := duckdb.NewSessionWorkspace(baseDir)
	if err != nil {
		t.Fatal(err)
	}
	abortRoot := wsAbort.RootDir()
	if err := wsAbort.Close(); err != nil {
		t.Fatalf("wsAbort.Close: %v", err)
	}
	if _, err := os.Stat(abortRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("aborted workspace %s leaked: %v", abortRoot, err)
	}
}
