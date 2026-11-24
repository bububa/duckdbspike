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
