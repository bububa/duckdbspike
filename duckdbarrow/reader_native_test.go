package duckdbarrow_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/bububa/duckdbspike/duckdb"
	"github.com/bububa/duckdbspike/duckdbarrow"
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

func TestRecordReaderNilGuards(t *testing.T) {
	if _, err := duckdbarrow.NewRecordReader(nil, nil); !errors.Is(err, duckdb.ErrInvalidInput) {
		t.Fatalf("NewRecordReader(nil) = %v, want ErrInvalidInput", err)
	}
	if _, err := duckdbarrow.ImportSchema(nil); !errors.Is(err, duckdb.ErrInvalidInput) {
		t.Fatalf("ImportSchema(nil) = %v, want ErrInvalidInput", err)
	}
	if _, err := duckdbarrow.ImportRecordBatch(nil, nil, nil); !errors.Is(err, duckdb.ErrInvalidInput) {
		t.Fatalf("ImportRecordBatch(nil) = %v, want ErrInvalidInput", err)
	}
	if _, err := duckdbarrow.Query(context.Background(), nil, nil, "SELECT 1"); !errors.Is(err, duckdb.ErrClosed) {
		t.Fatalf("Query(nil conn) = %v, want ErrClosed", err)
	}
}

func TestNativeArrowRecordReaderAllTypesAndZeroLeaks(t *testing.T) {
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

	if _, err := conn.Exec(ctx, "CREATE TYPE color_enum AS ENUM ('red', 'green', 'blue')"); err != nil {
		t.Fatal(err)
	}

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	sql := `SELECT
		i::BIGINT AS id,
		(i % 2 = 0) AS flag,
		i::SMALLINT AS i16,
		i::UINTEGER AS u32,
		(i * 1.5)::DOUBLE AS dbl,
		(i * 1.25)::DECIMAL(18, 4) AS dec,
		CASE WHEN i % 3 = 0 THEN NULL ELSE repeat('s', 20) || i::VARCHAR END AS str,
		encode('bin_' || i::VARCHAR) AS blb,
		DATE '2026-01-01' + i::INTEGER AS dt,
		TIMESTAMP '2026-01-01 12:00:00' + to_seconds(i) AS ts,
		'green'::color_enum AS clr,
		[i::INTEGER, NULL, (i + 1)::INTEGER] AS lst,
		[i::INTEGER, (i + 2)::INTEGER]::INTEGER[2] AS arr,
		{'a': i::INTEGER, 'b': 'x'} AS st,
		MAP {'k': i::INTEGER} AS mp
	FROM range($n) t(i)`

	reader, err := duckdbarrow.QueryNamed(ctx, conn, alloc, sql, duckdb.Named("n", duckdb.Int64Value(2500)))
	if err != nil {
		t.Fatalf("duckdbarrow.QueryNamed: %v", err)
	}

	schema := reader.Schema()
	if schema.NumFields() != 15 {
		reader.Release()
		t.Fatalf("schema fields = %d, want 15", schema.NumFields())
	}

	var batches int
	var totalRows int64
	var saved arrow.RecordBatch
	for reader.Next() {
		rec := reader.RecordBatch()
		batches++
		totalRows += rec.NumRows()

		if batches == 1 {
			// Retain first batch to verify Go ownership survives reader.Release() & conn.Close().
			rec.Retain()
			saved = rec
		}

		// Spot-check typed Arrow columns.
		idCol, ok := rec.Column(0).(*array.Int64)
		if !ok {
			t.Fatalf("col 0 type = %T, want *array.Int64", rec.Column(0))
		}
		if idCol.Len() != int(rec.NumRows()) {
			t.Fatalf("col 0 len mismatch")
		}
		if _, ok := rec.Column(1).(*array.Boolean); !ok {
			t.Fatalf("col 1 type = %T, want *array.Boolean", rec.Column(1))
		}
		if _, ok := rec.Column(5).(*array.Decimal128); !ok {
			t.Fatalf("col 5 type = %T, want *array.Decimal128", rec.Column(5))
		}
		if _, ok := rec.Column(10).(*array.Dictionary); !ok {
			t.Fatalf("col 10 (ENUM) type = %T, want *array.Dictionary", rec.Column(10))
		}
		if _, ok := rec.Column(11).(*array.List); !ok {
			t.Fatalf("col 11 (LIST) type = %T, want *array.List", rec.Column(11))
		}
		if _, ok := rec.Column(12).(*array.FixedSizeList); !ok {
			t.Fatalf("col 12 (ARRAY) type = %T, want *array.FixedSizeList", rec.Column(12))
		}
		if _, ok := rec.Column(13).(*array.Struct); !ok {
			t.Fatalf("col 13 (STRUCT) type = %T, want *array.Struct", rec.Column(13))
		}
		if _, ok := rec.Column(14).(*array.Map); !ok {
			t.Fatalf("col 14 (MAP) type = %T, want *array.Map", rec.Column(14))
		}
	}
	if err := reader.Err(); err != nil {
		if saved != nil {
			saved.Release()
		}
		reader.Release()
		t.Fatalf("reader.Err: %v", err)
	}
	reader.Release()
	if err := conn.Close(); err != nil {
		if saved != nil {
			saved.Release()
		}
		t.Fatal(err)
	}

	if batches < 2 || totalRows != 2500 {
		if saved != nil {
			saved.Release()
		}
		t.Fatalf("batches=%d totalRows=%d, want >=2 batches and 2500 rows", batches, totalRows)
	}

	// Verify retained batch is still readable after reader.Release() and conn.Close().
	if saved == nil {
		t.Fatal("expected saved batch")
	}
	id0 := saved.Column(0).(*array.Int64).Value(0)
	if id0 != 0 {
		t.Fatalf("saved id[0] = %d, want 0", id0)
	}
	if !saved.Column(6).IsNull(0) {
		t.Fatal("expected str[0] (i=0, 0%3==0) to be NULL")
	}
	if saved.Column(6).IsNull(1) {
		t.Fatal("expected str[1] to be non-NULL")
	}
	saved.Release()
}
