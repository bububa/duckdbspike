package duckdbarrow_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/bububa/duckdbspike/duckdb"
	"github.com/bububa/duckdbspike/duckdbarrow"
)

func buildRichBatch(t *testing.T, alloc memory.Allocator, n int) arrow.RecordBatch {
	t.Helper()
	meta := arrow.NewMetadata([]string{"ARROW:extension:name", "custom"}, []string{"arrow.custom", "v1"})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 12, Scale: 2}, Nullable: true},
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		{Name: "meta", Type: arrow.StructOf(
			arrow.Field{Name: "k", Type: arrow.BinaryTypes.String, Nullable: true},
			arrow.Field{Name: "v", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		), Nullable: true, Metadata: meta},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	idB := rb.Field(0).(*array.Int64Builder)
	nameB := rb.Field(1).(*array.StringBuilder)
	amtB := rb.Field(2).(*array.Decimal128Builder)
	tagsB := rb.Field(3).(*array.ListBuilder)
	tagElemB := tagsB.ValueBuilder().(*array.StringBuilder)
	metaB := rb.Field(4).(*array.StructBuilder)
	metaKB := metaB.FieldBuilder(0).(*array.StringBuilder)
	metaVB := metaB.FieldBuilder(1).(*array.Int32Builder)

	for i := 0; i < n; i++ {
		idB.Append(int64(i + 1))
		if i%5 == 0 {
			nameB.AppendNull()
			amtB.AppendNull()
			tagsB.AppendNull()
			metaB.AppendNull()
		} else {
			nameB.Append("row-" + string(rune('a'+(i%26))))
			amtB.Append(decimal128.FromI64(int64((i + 1) * 125)))
			tagsB.Append(true)
			tagElemB.Append("t1")
			tagElemB.Append("t2")
			metaB.Append(true)
			metaKB.Append("key")
			metaVB.Append(int32(i * 10))
		}
	}
	return rb.NewRecordBatch()
}

func TestArrowExportImportRoundTripPureGo(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	rec := buildRichBatch(t, alloc, 16)
	cSchema, err := duckdbarrow.ExportSchema(rec.Schema())
	if err != nil {
		rec.Release()
		t.Fatalf("ExportSchema: %v", err)
	}
	cArr, err := duckdbarrow.ExportRecordBatch(rec)
	if err != nil {
		cSchema.Release()
		rec.Release()
		t.Fatalf("ExportRecordBatch: %v", err)
	}
	// Caller can drop its own reference immediately; cArr holds a Retain.
	rec.Release()

	impSchema, err := duckdbarrow.ImportSchema(cSchema)
	if err != nil {
		cArr.Release()
		cSchema.Release()
		t.Fatalf("ImportSchema: %v", err)
	}
	// Verify field metadata survived ExportSchema -> ImportSchema.
	metaField := impSchema.Field(4)
	extName, ok := metaField.Metadata.GetValue("ARROW:extension:name")
	if !ok || extName != "arrow.custom" {
		t.Fatalf("expected ARROW:extension:name=arrow.custom, got %q (%v)", extName, ok)
	}

	impRec, err := duckdbarrow.ImportRecordBatch(impSchema, cArr, alloc)
	cArr.Release()
	cSchema.Release()
	if err != nil {
		t.Fatalf("ImportRecordBatch: %v", err)
	}
	defer impRec.Release()

	if impRec.NumRows() != 16 || impRec.NumCols() != 5 {
		t.Fatalf("unexpected imported shape: %dx%d", impRec.NumRows(), impRec.NumCols())
	}
	if !impRec.Column(1).IsNull(0) || impRec.Column(1).(*array.String).Value(1) != "row-b" {
		t.Fatalf("unexpected string column values")
	}
}

func TestNativeArrowMetadataExportAppendAndViewZeroLeaks(t *testing.T) {
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

	// 1. Verify DuckDB extension metadata survives ImportSchema when
	// arrow_lossless_conversion is enabled.
	if _, err := conn.Exec(ctx, "SET arrow_lossless_conversion = true"); err != nil {
		t.Fatal(err)
	}
	rr, err := duckdbarrow.Query(ctx, conn, nil, `SELECT
		'00112233-4455-6677-8899-aabbccddeeff'::UUID AS u,
		'{"a":1}'::JSON AS j`)
	if err != nil {
		t.Fatalf("duckdbarrow.Query: %v", err)
	}
	uExt, uOk := rr.Schema().Field(0).Metadata.GetValue("ARROW:extension:name")
	jExt, jOk := rr.Schema().Field(1).Metadata.GetValue("ARROW:extension:name")
	rr.Release()
	if !uOk || uExt != "arrow.uuid" {
		t.Fatalf("UUID extension metadata = %q (%v), want arrow.uuid", uExt, uOk)
	}
	if !jOk || jExt != "arrow.json" {
		t.Fatalf("JSON extension metadata = %q (%v), want arrow.json", jExt, jOk)
	}
	if _, err := conn.Exec(ctx, "SET arrow_lossless_conversion = false"); err != nil {
		t.Fatal(err)
	}

	// 2. AppendRecordBatch & AppendRecordReader with CheckedAllocator zero-leak check.
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	if _, err := conn.Exec(ctx, `CREATE TABLE arrow_ingest (
		id BIGINT,
		name VARCHAR,
		amount DECIMAL(12, 2),
		tags VARCHAR[],
		meta STRUCT(k VARCHAR, v INTEGER)
	)`); err != nil {
		t.Fatal(err)
	}

	batch1 := buildRichBatch(t, alloc, 2500)
	app, err := conn.NewAppender(ctx, "", "", "arrow_ingest")
	if err != nil {
		batch1.Release()
		t.Fatal(err)
	}
	if err := duckdbarrow.AppendRecordBatch(app, batch1); err != nil {
		batch1.Release()
		_ = app.Close()
		t.Fatalf("AppendRecordBatch: %v", err)
	}
	batch1.Release()

	batch2 := buildRichBatch(t, alloc, 50)
	memReader, err := array.NewRecordReader(batch2.Schema(), []arrow.RecordBatch{batch2})
	if err != nil {
		batch2.Release()
		_ = app.Close()
		t.Fatal(err)
	}
	batch2.Release()
	if err := duckdbarrow.AppendRecordReader(app, memReader); err != nil {
		memReader.Release()
		_ = app.Close()
		t.Fatalf("AppendRecordReader: %v", err)
	}
	memReader.Release()
	if err := app.Close(); err != nil {
		t.Fatalf("app.Close: %v", err)
	}

	rows, err := conn.Query(ctx, "SELECT count(*), count(name), max(meta.v), max(amount) FROM arrow_ingest")
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		err = rows.Err()
		_ = rows.Close()
		t.Fatalf("count query returned no rows: %v", err)
	}
	vals, err := rows.DecodeRow(nil)
	_ = rows.Close()
	if err != nil {
		t.Fatalf("DecodeRow: %v", err)
	}
	cnt, _ := vals[0].Int64()
	nonNullNames, _ := vals[1].Int64()
	maxMetaV, _ := vals[2].Int32()
	maxAmt, _ := vals[3].Decimal()
	if cnt != 2550 || nonNullNames != 2040 || maxMetaV != 24990 || maxAmt.String() != "3125.00" {
		t.Fatalf("appended aggregates = (cnt=%d, names=%d, maxMetaV=%d, maxAmt=%s), want (2550, 2040, 24990, 3125.00)",
			cnt, nonNullNames, maxMetaV, maxAmt.String())
	}

	// 3. RegisterRecordBatch virtual view via duckdb_arrow_array_scan.
	viewBatch := buildRichBatch(t, alloc, 100)
	view, err := duckdbarrow.RegisterRecordBatch(ctx, conn, "v_arrow_batch", viewBatch)
	viewBatch.Release()
	if err != nil {
		t.Fatalf("RegisterRecordBatch: %v", err)
	}

	vRows, err := conn.Query(ctx, `SELECT count(*), sum(id), max(amount), max(len(tags)), max(meta.v) FROM v_arrow_batch WHERE name IS NOT NULL`)
	if err != nil {
		_ = view.Close()
		t.Fatalf("query arrow view: %v", err)
	}
	if !vRows.Next() {
		err = vRows.Err()
		_ = vRows.Close()
		_ = view.Close()
		t.Fatalf("arrow view returned no rows: %v", err)
	}
	vVals, err := vRows.DecodeRow(nil)
	_ = vRows.Close()
	if err != nil {
		_ = view.Close()
		t.Fatalf("arrow view DecodeRow: %v", err)
	}
	vCnt, _ := vVals[0].Int64()
	vSum, _ := vVals[1].Hugeint()
	vMaxAmt, _ := vVals[2].Decimal()
	vMaxTags, _ := vVals[3].Int64()
	vMaxMetaV, _ := vVals[4].Int32()
	if vCnt != 80 || vSum.String() != "4080" || vMaxAmt.String() != "125.00" || vMaxTags != 2 || vMaxMetaV != 990 {
		_ = view.Close()
		t.Fatalf("arrow view result = (cnt=%d, sum=%s, maxAmt=%s, maxTags=%d, maxMetaV=%d), want (80, 4080, 125.00, 2, 990)",
			vCnt, vSum.String(), vMaxAmt.String(), vMaxTags, vMaxMetaV)
	}
	if err := view.Close(); err != nil {
		t.Fatalf("view.Close: %v", err)
	}
}
