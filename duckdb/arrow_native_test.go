package duckdb_test

import (
	"context"
	"errors"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
)

func TestNativeArrowStreamingAndAppenderRoundTrip(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()
	db, err := lib.Open(ctx, ":memory:", duckdb.Option{Name: "threads", Value: "2"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	c1, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	if _, err := c2.Exec(ctx, "CREATE TABLE arrow_dst (id BIGINT, label VARCHAR)"); err != nil {
		t.Fatal(err)
	}

	// Stream 5000 rows via QueryArrowNamed and ingest zero-copy into arrow_dst via AppendArrowChunk.
	ar, err := c1.QueryArrowNamed(ctx,
		"SELECT i::BIGINT AS id, CASE WHEN i % 10 = 0 THEN NULL ELSE 'row_' || i::VARCHAR END AS label FROM range($n) t(i)",
		duckdb.Named("n", duckdb.Int64Value(5000)),
	)
	if err != nil {
		t.Fatalf("QueryArrowNamed: %v", err)
	}
	defer ar.Close()

	schema, err := ar.Schema()
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	defer schema.Release()
	if sv := schema.View(); sv.Format() != "+s" || sv.ChildCount() != 2 || sv.Child(0).Name() != "id" || sv.Child(1).Name() != "label" {
		t.Fatalf("unexpected Arrow schema: format=%q children=%d", sv.Format(), sv.ChildCount())
	}

	app, err := c2.NewAppender(ctx, "", "", "arrow_dst")
	if err != nil {
		t.Fatalf("NewAppender: %v", err)
	}

	var chunks int
	var totalRows int64
	for {
		arr, err := ar.Next()
		if err != nil {
			_ = app.Close()
			t.Fatalf("ArrowRows.Next: %v", err)
		}
		if arr == nil {
			break
		}
		chunks++
		totalRows += arr.View().Length()
		if err := app.AppendArrowChunk(schema, arr); err != nil {
			arr.Release()
			_ = app.Close()
			t.Fatalf("AppendArrowChunk: %v", err)
		}
		if !arr.Released() {
			t.Fatal("expected ArrowArray to be marked released after ownership transfer")
		}
		arr.Release() // idempotent no-op
	}
	if err := ar.Close(); err != nil {
		t.Fatal(err)
	}
	if err := app.Close(); err != nil {
		t.Fatalf("Appender.Close: %v", err)
	}
	if chunks < 2 || totalRows != 5000 {
		t.Fatalf("streamed chunks=%d totalRows=%d, want >=2 chunks and 5000 rows", chunks, totalRows)
	}

	// Verify ingested table matches source exactly.
	_, check := querySingleRow(t, c2, "SELECT count(*), count(label), sum(id) FROM arrow_dst")
	if cnt, _ := check[0].Int64(); cnt != 5000 {
		t.Fatalf("count(*) = %d, want 5000", cnt)
	}
	if nonNull, _ := check[1].Int64(); nonNull != 4500 {
		t.Fatalf("count(label) = %d, want 4500", nonNull)
	}
	if sum, _ := check[2].AsInt64(); sum != 12497500 {
		t.Fatalf("sum(id) = %d, want 12497500", sum)
	}

	// ResultLimits.MaxRows enforced on ArrowRows.
	if err := c1.SetResultLimits(duckdb.ResultLimits{MaxRows: 100}); err != nil {
		t.Fatal(err)
	}
	arLim, err := c1.QueryArrow(ctx, "SELECT i FROM range(5000) t(i)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := arLim.Next(); !errors.Is(err, duckdb.ErrBudgetExceeded) {
		t.Fatalf("ArrowRows MaxRows err = %v, want ErrBudgetExceeded", err)
	}
	_ = arLim.Close()
}

func TestNativeRowsReturnTypeAndArrowScriptMultiResultSet(t *testing.T) {
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

	// Rows.ReturnType on SELECT remains readable after Close.
	rows, err := conn.Query(ctx, "SELECT 42 AS answer")
	if err != nil {
		t.Fatal(err)
	}
	if got := rows.ReturnType(); got != duckdb.ResultTypeQueryResult || got.String() != "QUERY_RESULT" {
		t.Fatalf("Rows.ReturnType() = %v (%q), want QUERY_RESULT", got, got.String())
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if got := rows.ReturnType(); got != duckdb.ResultTypeQueryResult {
		t.Fatalf("Rows.ReturnType() after Close = %v, want QUERY_RESULT", got)
	}

	// QueryScriptArrow with leading DDL/DML + two SELECT result sets + trailing DML.
	script := `
		CREATE TEMP TABLE ar_multi (id INT, tag VARCHAR);
		INSERT INTO ar_multi VALUES (1, 'a'), (2, 'b');
		SELECT id FROM ar_multi WHERE id >= ? ORDER BY id;
		SELECT tag FROM ar_multi WHERE id = ?;
		INSERT INTO ar_multi VALUES (3, 'c');
	`
	ar, err := conn.QueryScriptArrow(ctx, script, duckdb.Int32Value(1), duckdb.Int32Value(2))
	if err != nil {
		t.Fatalf("QueryScriptArrow: %v", err)
	}
	defer ar.Close()

	if ar.StatementType() != duckdb.StatementTypeSelect {
		t.Fatalf("RS1 StatementType = %v, want SELECT", ar.StatementType())
	}
	if ar.ReturnType() != duckdb.ResultTypeQueryResult {
		t.Fatalf("RS1 ReturnType = %v, want QUERY_RESULT", ar.ReturnType())
	}
	if ar.RowsAffected() != 2 {
		t.Fatalf("RS1 RowsAffected = %d, want 2 (from leading INSERT)", ar.RowsAffected())
	}
	if cols := ar.Columns(); len(cols) != 1 || cols[0].Name != "id" {
		t.Fatalf("RS1 Columns = %+v, want [id]", cols)
	}
	var rs1Rows int64
	for {
		arr, err := ar.Next()
		if err != nil {
			t.Fatalf("RS1 Next: %v", err)
		}
		if arr == nil {
			break
		}
		rs1Rows += arr.View().Length()
		arr.Release()
	}
	if rs1Rows != 2 {
		t.Fatalf("RS1 rows = %d, want 2", rs1Rows)
	}

	if !ar.HasNextResultSet() {
		t.Fatal("expected HasNextResultSet == true after RS1")
	}
	if !ar.NextResultSet() {
		t.Fatalf("NextResultSet to RS2 returned false (err=%v)", ar.Err())
	}
	if ar.StatementType() != duckdb.StatementTypeSelect || ar.ReturnType() != duckdb.ResultTypeQueryResult {
		t.Fatalf("RS2 metadata = (%v, %v), want (SELECT, QUERY_RESULT)", ar.StatementType(), ar.ReturnType())
	}
	if cols := ar.Columns(); len(cols) != 1 || cols[0].Name != "tag" {
		t.Fatalf("RS2 Columns = %+v, want [tag]", cols)
	}
	s2, err := ar.Schema()
	if err != nil {
		t.Fatalf("RS2 Schema: %v", err)
	}
	if s2.View().ChildCount() != 1 || s2.View().Child(0).Name() != "tag" {
		t.Fatalf("RS2 ArrowSchema child = %q, want tag", s2.View().Child(0).Name())
	}
	s2.Release()
	var rs2Rows int64
	for {
		arr, err := ar.Next()
		if err != nil {
			t.Fatalf("RS2 Next: %v", err)
		}
		if arr == nil {
			break
		}
		rs2Rows += arr.View().Length()
		arr.Release()
	}
	if rs2Rows != 1 {
		t.Fatalf("RS2 rows = %d, want 1", rs2Rows)
	}

	// Advancing past RS2 executes trailing INSERT and returns false cleanly.
	if !ar.HasNextResultSet() {
		t.Fatal("expected trailing statement pending before final NextResultSet")
	}
	if ar.NextResultSet() {
		t.Fatal("expected false after trailing non-row INSERT")
	}
	if err := ar.Err(); err != nil {
		t.Fatalf("unexpected Err after trailing INSERT: %v", err)
	}
	if ar.RowsAffected() != 3 {
		t.Fatalf("final RowsAffected = %d, want 3", ar.RowsAffected())
	}
	if err := ar.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify trailing INSERT actually committed into temp table.
	_, cntRow := querySingleRow(t, conn, "SELECT count(*)::BIGINT FROM ar_multi")
	if n, _ := cntRow[0].Int64(); n != 3 {
		t.Fatalf("ar_multi count = %d, want 3", n)
	}

	// QueryScriptArrowNamed + mid-stream Close before consuming RS2 leaves Conn usable.
	arNamed, err := conn.QueryScriptArrowNamed(ctx, "SELECT $x::INT AS a; SELECT $y::VARCHAR AS b;",
		duckdb.Named("x", duckdb.Int32Value(7)),
		duckdb.Named("y", duckdb.VarcharValue("ok")),
	)
	if err != nil {
		t.Fatalf("QueryScriptArrowNamed: %v", err)
	}
	if !arNamed.HasNextResultSet() {
		t.Fatal("expected HasNextResultSet == true")
	}
	if err := arNamed.Close(); err != nil {
		t.Fatal(err)
	}
	_, pingRow := querySingleRow(t, conn, "SELECT 99")
	if v, _ := pingRow[0].AsInt64(); v != 99 {
		t.Fatalf("post-close query = %d, want 99", v)
	}
}
