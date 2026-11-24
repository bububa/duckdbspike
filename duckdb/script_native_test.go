package duckdb_test

import (
	"context"
	"errors"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
)

func TestNativeMultiResultSetAndScriptParamDistribution(t *testing.T) {
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

	// 1. ExecScript distributing positional params across multiple statements.
	res, err := conn.ExecScript(ctx, `
		CREATE TEMP TABLE p4a_items (id INTEGER, tag VARCHAR);
		INSERT INTO p4a_items VALUES (?, ?);
		INSERT INTO p4a_items VALUES (?, ?);
	`,
		duckdb.Int32Value(1), duckdb.VarcharValue("alpha"),
		duckdb.Int32Value(2), duckdb.VarcharValue("beta"),
	)
	if err != nil {
		t.Fatalf("ExecScript multi-stmt positional: %v", err)
	}
	if res.RowsAffected != 2 {
		t.Fatalf("RowsAffected = %d, want 2", res.RowsAffected)
	}

	// Excess positional args rejected.
	if _, err := conn.ExecScript(ctx, `INSERT INTO p4a_items VALUES (?, ?)`,
		duckdb.Int32Value(3), duckdb.VarcharValue("gamma"), duckdb.Int32Value(999),
	); !errors.Is(err, duckdb.ErrInvalidInput) {
		t.Fatalf("excess positional args = %v, want ErrInvalidInput", err)
	}

	// 2. ExecScriptNamed routing named params across statements (including shared $tag).
	if _, err := conn.ExecScriptNamed(ctx, `
		INSERT INTO p4a_items VALUES ($id1, $shared);
		INSERT INTO p4a_items VALUES ($id2, $shared);
	`,
		duckdb.Named("id1", duckdb.Int32Value(3)),
		duckdb.Named("id2", duckdb.Int32Value(4)),
		duckdb.Named("shared", duckdb.VarcharValue("shared")),
	); err != nil {
		t.Fatalf("ExecScriptNamed multi-stmt: %v", err)
	}

	// Unknown unused named arg rejected.
	if _, err := conn.ExecScriptNamed(ctx, `SELECT $a`,
		duckdb.Named("a", duckdb.Int32Value(1)),
		duckdb.Named("unused", duckdb.Int32Value(2)),
	); !errors.Is(err, duckdb.ErrInvalidInput) {
		t.Fatalf("unused named arg = %v, want ErrInvalidInput", err)
	}

	// 3. QueryScript yielding multiple result sets (prelude DDL + DML + 3 SELECTs + trailing cleanup).
	rows, err := conn.QueryScript(ctx, `
		CREATE TEMP TABLE p4a_log (k INTEGER, label VARCHAR);
		INSERT INTO p4a_log VALUES (?, ?);
		SELECT k, label FROM p4a_log;
		SELECT count(*)::BIGINT AS cnt FROM p4a_items WHERE id >= ?;
		SELECT id, tag FROM p4a_items WHERE id = ?;
		DROP TABLE p4a_log;
	`,
		duckdb.Int32Value(77), duckdb.VarcharValue("logged"),
		duckdb.Int32Value(2),
		duckdb.Int32Value(1),
	)
	if err != nil {
		t.Fatalf("QueryScript multi-rs: %v", err)
	}

	// Result set 1: INSERT ... RETURNING k, label
	if cols := rows.Columns(); len(cols) != 2 || cols[0].Name != "k" || cols[1].Name != "label" {
		_ = rows.Close()
		t.Fatalf("rs1 cols = %+v", cols)
	}
	if !rows.Next() {
		_ = rows.Close()
		t.Fatalf("rs1 Next() = false, err = %v", rows.Err())
	}
	r1, err := rows.DecodeRow(nil)
	if err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	if k, _ := r1[0].Int32(); k != 77 {
		_ = rows.Close()
		t.Fatalf("rs1.k = %d, want 77", k)
	}
	if lbl, _ := r1[1].Varchar(); lbl != "logged" {
		_ = rows.Close()
		t.Fatalf("rs1.label = %q, want logged", lbl)
	}
	if rows.Next() {
		_ = rows.Close()
		t.Fatal("rs1 expected 1 row")
	}

	// Result set 2: SELECT count(*)
	if !rows.HasNextResultSet() || !rows.NextResultSet() {
		_ = rows.Close()
		t.Fatalf("advance to rs2 failed: err=%v", rows.Err())
	}
	if cols := rows.Columns(); len(cols) != 1 || cols[0].Name != "cnt" {
		_ = rows.Close()
		t.Fatalf("rs2 cols = %+v", cols)
	}
	if !rows.Next() {
		_ = rows.Close()
		t.Fatalf("rs2 Next() = false, err=%v", rows.Err())
	}
	r2, err := rows.DecodeRow(nil)
	if err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	if cnt, _ := r2[0].Int64(); cnt != 3 {
		_ = rows.Close()
		t.Fatalf("rs2 cnt = %d, want 3", cnt)
	}

	// Result set 3: SELECT id, tag
	if !rows.HasNextResultSet() || !rows.NextResultSet() {
		_ = rows.Close()
		t.Fatalf("advance to rs3 failed: err=%v", rows.Err())
	}
	if !rows.Next() {
		_ = rows.Close()
		t.Fatalf("rs3 Next() = false, err=%v", rows.Err())
	}
	r3, err := rows.DecodeRow(nil)
	if err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	if id, _ := r3[0].Int32(); id != 1 {
		_ = rows.Close()
		t.Fatalf("rs3 id = %d, want 1", id)
	}
	if tag, _ := r3[1].Varchar(); tag != "alpha" {
		_ = rows.Close()
		t.Fatalf("rs3 tag = %q, want alpha", tag)
	}

	// Trailing DROP TABLE executes cleanly and NextResultSet returns false with nil Err.
	if rows.NextResultSet() {
		_ = rows.Close()
		t.Fatal("expected no 4th result set")
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("final rows.Err = %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	// 4. Early Close() mid-script leaves connection reusable.
	early, err := conn.QueryScriptNamed(ctx, `SELECT $x AS a; SELECT $y AS b`,
		duckdb.Named("x", duckdb.Int32Value(1)),
		duckdb.Named("y", duckdb.Int32Value(2)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !early.HasNextResultSet() {
		_ = early.Close()
		t.Fatal("expected HasNextResultSet == true")
	}
	if err := early.Close(); err != nil {
		t.Fatal(err)
	}
	_, verifyRow := querySingleRow(t, conn, "SELECT 123")
	if n, _ := verifyRow[0].Int32(); n != 123 {
		t.Fatalf("conn reuse = %d, want 123", n)
	}
}
