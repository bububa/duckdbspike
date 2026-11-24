package duckdbarrow_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/bububa/duckdbspike/duckdb"
	"github.com/bububa/duckdbspike/duckdbarrow"
)

func TestP3bArrowUnionFloat16DurationRoundTripPureGo(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	denseType := arrow.DenseUnionOf(
		[]arrow.Field{
			{Name: "num", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "txt", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		[]arrow.UnionTypeCode{0, 1},
	)
	sparseType := arrow.SparseUnionOf(
		[]arrow.Field{
			{Name: "flag", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			{Name: "big", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		},
		[]arrow.UnionTypeCode{2, 5},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "f16", Type: arrow.FixedWidthTypes.Float16, Nullable: true},
		{Name: "dur_us", Type: arrow.FixedWidthTypes.Duration_us, Nullable: true},
		{Name: "du", Type: denseType, Nullable: false},
		{Name: "su", Type: sparseType, Nullable: false},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	f16B := rb.Field(0).(*array.Float16Builder)
	durB := rb.Field(1).(*array.DurationBuilder)
	duB := rb.Field(2).(*array.DenseUnionBuilder)
	duNumB := duB.Child(0).(*array.Int32Builder)
	duTxtB := duB.Child(1).(*array.StringBuilder)
	suB := rb.Field(3).(*array.SparseUnionBuilder)
	suFlagB := suB.Child(0).(*array.BooleanBuilder)
	suBigB := suB.Child(1).(*array.Int64Builder)

	// Row 0
	f16B.Append(float16.New(1.5))
	durB.Append(arrow.Duration(123456))
	duB.Append(0)
	duNumB.Append(42)
	suB.Append(2)
	suFlagB.Append(true)
	suBigB.AppendNull()

	// Row 1
	f16B.AppendNull()
	durB.Append(-999)
	duB.Append(1)
	duTxtB.Append("hello")
	suB.Append(5)
	suFlagB.AppendNull()
	suBigB.Append(9876543210)

	// Row 2
	f16B.Append(float16.New(-3.25))
	durB.AppendNull()
	duB.Append(0)
	duNumB.Append(-7)
	suB.Append(2)
	suFlagB.Append(false)
	suBigB.AppendNull()

	rec := rb.NewRecordBatch()
	cSchema, err := duckdbarrow.ExportSchema(rec.Schema())
	if err != nil {
		rec.Release()
		t.Fatalf("ExportSchema: %v", err)
	}
	cArr, err := duckdbarrow.ExportRecordBatch(rec)
	rec.Release()
	if err != nil {
		cSchema.Release()
		t.Fatalf("ExportRecordBatch: %v", err)
	}

	impSchema, err := duckdbarrow.ImportSchema(cSchema)
	if err != nil {
		cArr.Release()
		cSchema.Release()
		t.Fatalf("ImportSchema: %v", err)
	}
	impRec, err := duckdbarrow.ImportRecordBatch(impSchema, cArr, alloc)
	cArr.Release()
	cSchema.Release()
	if err != nil {
		t.Fatalf("ImportRecordBatch: %v", err)
	}
	defer impRec.Release()

	f16Col := impRec.Column(0).(*array.Float16)
	if f16Col.Value(0).Float32() != 1.5 || !f16Col.IsNull(1) || f16Col.Value(2).Float32() != -3.25 {
		t.Fatalf("Float16 round-trip mismatch")
	}
	durCol := impRec.Column(1).(*array.Duration)
	if durCol.Value(0) != 123456 || durCol.Value(1) != -999 || !durCol.IsNull(2) {
		t.Fatalf("Duration round-trip mismatch")
	}
	duCol := impRec.Column(2).(*array.DenseUnion)
	if duCol.TypeCode(0) != 0 || duCol.Field(0).(*array.Int32).Value(int(duCol.ValueOffset(0))) != 42 ||
		duCol.TypeCode(1) != 1 || duCol.Field(1).(*array.String).Value(int(duCol.ValueOffset(1))) != "hello" ||
		duCol.TypeCode(2) != 0 || duCol.Field(0).(*array.Int32).Value(int(duCol.ValueOffset(2))) != -7 {
		t.Fatalf("DenseUnion round-trip mismatch")
	}
	suCol := impRec.Column(3).(*array.SparseUnion)
	if suCol.TypeCode(0) != 2 || !suCol.Field(0).(*array.Boolean).Value(0) ||
		suCol.TypeCode(1) != 5 || suCol.Field(1).(*array.Int64).Value(1) != 9876543210 ||
		suCol.TypeCode(2) != 2 || suCol.Field(0).(*array.Boolean).Value(2) {
		t.Fatalf("SparseUnion round-trip mismatch")
	}
}

func TestNativeP3bDuckDBUnionQueryAndStreamingRegisterRecordReader(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

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

	// 1. Native DuckDB UNION query imported via duckdbarrow (SparseUnion).
	rr, err := duckdbarrow.Query(ctx, conn, alloc, `SELECT u FROM (VALUES
		(union_value(num := 11::INTEGER)::UNION(num INTEGER, txt VARCHAR)),
		(union_value(txt := 'duck')::UNION(num INTEGER, txt VARCHAR)),
		(union_value(num := 99::INTEGER)::UNION(num INTEGER, txt VARCHAR))
	) t(u)`)
	if err != nil {
		t.Fatalf("duckdbarrow.Query(UNION): %v", err)
	}
	var gotNums []int32
	var gotTxts []string
	for rr.Next() {
		rec := rr.RecordBatch()
		col := rec.Column(0)
		su, ok := col.(*array.SparseUnion)
		if !ok {
			rr.Release()
			t.Fatalf("expected *array.SparseUnion, got %T", col)
		}
		for i := 0; i < su.Len(); i++ {
			switch su.ChildID(i) {
			case 0:
				gotNums = append(gotNums, su.Field(0).(*array.Int32).Value(i))
			case 1:
				gotTxts = append(gotTxts, su.Field(1).(*array.String).Value(i))
			default:
				cid := su.ChildID(i)
				rr.Release()
				t.Fatalf("unexpected union child id %d", cid)
			}
		}
	}
	rrErr := rr.Err()
	rr.Release()
	if rrErr != nil {
		t.Fatalf("UNION reader err: %v", rrErr)
	}
	if len(gotNums) != 2 || gotNums[0] != 11 || gotNums[1] != 99 || len(gotTxts) != 1 || gotTxts[0] != "duck" {
		t.Fatalf("UNION decoded = nums:%v txts:%v, want [11 99] [duck]", gotNums, gotTxts)
	}

	// 2. RegisterRecordReader streaming 5 batches (5 * 2100 = 10,500 rows) via duckdb_arrow_scan.
	batches := make([]arrow.RecordBatch, 5)
	for i := range batches {
		batches[i] = buildRichBatch(t, alloc, 2100)
	}
	reader, err := array.NewRecordReader(batches[0].Schema(), batches)
	for _, b := range batches {
		b.Release()
	}
	if err != nil {
		t.Fatalf("array.NewRecordReader: %v", err)
	}

	view, err := duckdbarrow.RegisterRecordReader(ctx, conn, "v_arrow_stream", reader)
	reader.Release() // RegisterRecordReader retains reader until view.Close().
	if err != nil {
		t.Fatalf("RegisterRecordReader: %v", err)
	}

	rows, err := conn.Query(ctx, `SELECT count(*), sum(id), count(name), max(amount), max(len(tags)), max(meta.v) FROM v_arrow_stream WHERE name IS NOT NULL`)
	if err != nil {
		_ = view.Close()
		t.Fatalf("query v_arrow_stream: %v", err)
	}
	if !rows.Next() {
		err = rows.Err()
		_ = rows.Close()
		_ = view.Close()
		t.Fatalf("v_arrow_stream returned no rows: %v", err)
	}
	vals, err := rows.DecodeRow(nil)
	_ = rows.Close()
	if err != nil {
		_ = view.Close()
		t.Fatalf("DecodeRow: %v", err)
	}
	cnt, _ := vals[0].Int64()
	names, _ := vals[2].Int64()
	maxAmt, _ := vals[3].Decimal()
	maxTags, _ := vals[4].Int64()
	maxMetaV, _ := vals[5].Int32()
	// 2100 rows per batch; 4/5 non-null = 1680 per batch; 5 batches = 8400 non-null rows.
	// Max amount = 2100 * 1.25 = 2625.00; max meta.v = 2099 * 10 = 20990.
	if cnt != 8400 || names != 8400 || maxAmt.String() != "2625.00" || maxTags != 2 || maxMetaV != 20990 {
		_ = view.Close()
		t.Fatalf("stream view aggregates = (cnt=%d, names=%d, maxAmt=%s, maxTags=%d, maxMetaV=%d), want (8400, 8400, 2625.00, 2, 20990)",
			cnt, names, maxAmt.String(), maxTags, maxMetaV)
	}
	if err := view.Close(); err != nil {
		t.Fatalf("view.Close: %v", err)
	}

	// 3. Verify Conn.Close() cascades cleanup of unclosed RegisterRecordReader views with zero leaks.
	conn2, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bSingle := buildRichBatch(t, alloc, 10)
	r2, err := array.NewRecordReader(bSingle.Schema(), []arrow.RecordBatch{bSingle})
	bSingle.Release()
	if err != nil {
		_ = conn2.Close()
		t.Fatal(err)
	}
	if _, err := duckdbarrow.RegisterRecordReader(ctx, conn2, "v_unclosed_stream", r2); err != nil {
		r2.Release()
		_ = conn2.Close()
		t.Fatalf("RegisterRecordReader(unclosed): %v", err)
	}
	r2.Release()
	if err := conn2.Close(); err != nil {
		t.Fatalf("conn2.Close: %v", err)
	}
}
