package duckdb_test

import (
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
)

func TestNativeVFSFileCatalogStmtArrowSchemaAndQueryScriptArrow(t *testing.T) {
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

	// 1. DuckDB Client VFS File write/sync/seek/read round-trip + SQL read_text verification.
	vfsPath := filepath.Join(t.TempDir(), "vfs_payload.txt")
	wf, err := conn.OpenFile(ctx, vfsPath, duckdb.FileOpenOptions{
		Write:  true,
		Create: true,
	})
	if err != nil {
		t.Fatalf("OpenFile write: %v", err)
	}
	payload := "DuckDB v1.5.6 purego VFS!"
	if n, err := wf.Write([]byte(payload)); err != nil || n != len(payload) {
		_ = wf.Close()
		t.Fatalf("Write = (%d, %v)", n, err)
	}
	if err := wf.Sync(); err != nil {
		_ = wf.Close()
		t.Fatalf("Sync: %v", err)
	}
	if sz, err := wf.Size(); err != nil || sz != int64(len(payload)) {
		_ = wf.Close()
		t.Fatalf("Size = (%d, %v), want %d", sz, err, len(payload))
	}
	if err := wf.Close(); err != nil {
		t.Fatalf("wf.Close: %v", err)
	}

	rf, err := conn.OpenFile(ctx, vfsPath, duckdb.FileOpenOptions{Read: true})
	if err != nil {
		t.Fatalf("OpenFile read: %v", err)
	}
	if pos, err := rf.Seek(7, io.SeekStart); err != nil || pos != 7 {
		_ = rf.Close()
		t.Fatalf("SeekStart = (%d, %v)", pos, err)
	}
	if tell, err := rf.Tell(); err != nil || tell != 7 {
		_ = rf.Close()
		t.Fatalf("Tell = (%d, %v)", tell, err)
	}
	part, err := io.ReadAll(rf)
	if err != nil {
		_ = rf.Close()
		t.Fatalf("ReadAll: %v", err)
	}
	if string(part) != "v1.5.6 purego VFS!" {
		_ = rf.Close()
		t.Fatalf("ReadAll from offset 7 = %q", string(part))
	}
	if err := rf.Close(); err != nil {
		t.Fatalf("rf.Close: %v", err)
	}

	// Verify DuckDB SQL itself reads the exact file written through VFS.
	_, txtRow := querySingleRow(t, conn, "SELECT content FROM read_text(?)", duckdb.VarcharValue(vfsPath))
	if got, _ := txtRow[0].Varchar(); got != payload {
		t.Fatalf("read_text = %q, want %q", got, payload)
	}

	// 2. Catalog metadata API on both autocommit Conn and explicit Tx.
	if _, err := conn.ExecScript(ctx, `
		CREATE TABLE cat_orders (id BIGINT, total DECIMAL(10, 2));
		CREATE VIEW cat_orders_v AS SELECT id FROM cat_orders;
		CREATE SEQUENCE cat_seq;
		CREATE TYPE cat_status AS ENUM ('open', 'closed');
	`); err != nil {
		t.Fatalf("setup catalog objects: %v", err)
	}

	catType, ok, err := conn.CatalogTypeName(ctx, "memory")
	if err != nil || !ok || catType != "duckdb" {
		t.Fatalf("Conn.CatalogTypeName('memory') = (%q, %v, %v), want ('duckdb', true, nil)", catType, ok, err)
	}
	if _, ok, err := conn.CatalogTypeName(ctx, "nonexistent_db"); err != nil || ok {
		t.Fatalf("missing catalog = (%v, %v)", ok, err)
	}

	tbl, ok, err := conn.LookupCatalogEntry(ctx, "memory", duckdb.CatalogEntryTable, "main", "cat_orders")
	if err != nil || !ok || tbl.Type != duckdb.CatalogEntryTable || tbl.Name != "cat_orders" {
		t.Fatalf("LookupCatalogEntry table = (%+v, %v, %v)", tbl, ok, err)
	}
	if _, ok, err := conn.LookupCatalogEntry(ctx, "memory", duckdb.CatalogEntryTable, "main", "does_not_exist"); err != nil || ok {
		t.Fatalf("missing table = (%v, %v)", ok, err)
	}

	// Inside Tx: zero-SQL direct lookup of view, sequence, and enum type.
	tx, err := conn.BeginTx(ctx, duckdb.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	vw, ok, err := tx.LookupCatalogEntry(ctx, "memory", duckdb.CatalogEntryView, "main", "cat_orders_v")
	if err != nil || !ok || vw.Type != duckdb.CatalogEntryView || vw.Name != "cat_orders_v" {
		_ = tx.Rollback(ctx)
		t.Fatalf("tx.LookupCatalogEntry view = (%+v, %v, %v)", vw, ok, err)
	}
	seq, ok, err := tx.LookupCatalogEntry(ctx, "memory", duckdb.CatalogEntrySequence, "main", "cat_seq")
	if err != nil || !ok || seq.Type != duckdb.CatalogEntrySequence {
		_ = tx.Rollback(ctx)
		t.Fatalf("tx.LookupCatalogEntry sequence = (%+v, %v, %v)", seq, ok, err)
	}
	et, ok, err := tx.LookupCatalogEntry(ctx, "memory", duckdb.CatalogEntryTypeDef, "main", "cat_status")
	if err != nil || !ok || et.Type != duckdb.CatalogEntryTypeDef {
		_ = tx.Rollback(ctx)
		t.Fatalf("tx.LookupCatalogEntry type = (%+v, %v, %v)", et, ok, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// 3. Stmt.ArrowSchema() without executing statement.
	stmt, err := conn.Prepare(ctx, `SELECT id, total FROM cat_orders WHERE id > ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	rawSchema, err := stmt.ArrowSchema()
	if err != nil {
		t.Fatalf("Stmt.ArrowSchema: %v", err)
	}
	sv := rawSchema.View()
	if !sv.Valid() || sv.ChildCount() != 2 || sv.Child(0).Name() != "id" || sv.Child(0).Format() != "l" || sv.Child(1).Name() != "total" {
		rawSchema.Release()
		t.Fatalf("unexpected Stmt.ArrowSchema: children=%d", sv.ChildCount())
	}
	rawSchema.Release()

	// 4. Conn.QueryScriptArrow & QueryScriptArrowNamed.
	ar, err := conn.QueryScriptArrow(ctx, `
		INSERT INTO cat_orders VALUES (1, 10.50), (2, 25.00);
		SELECT id, total FROM cat_orders WHERE id >= ? ORDER BY id;
	`, duckdb.Int64Value(1))
	if err != nil {
		t.Fatalf("QueryScriptArrow: %v", err)
	}
	var totalRows int64
	for {
		arr, err := ar.Next()
		if err != nil {
			_ = ar.Close()
			t.Fatalf("ar.Next: %v", err)
		}
		if arr == nil {
			break
		}
		totalRows += arr.View().Length()
		arr.Release()
	}
	if err := ar.Close(); err != nil {
		t.Fatal(err)
	}
	if totalRows != 2 {
		t.Fatalf("QueryScriptArrow rows = %d, want 2", totalRows)
	}
}
