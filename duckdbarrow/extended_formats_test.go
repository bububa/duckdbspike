package duckdbarrow_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/bububa/duckdbspike/duckdbarrow"
	"github.com/bububa/duckdbspike/internal/abi"
)

func TestArrowDecimal32Decimal64ListViewAndRunEndEncodedRoundTripPureGo(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	before := abi.ExportedArrowHandleCount()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "d32", Type: &arrow.Decimal32Type{Precision: 6, Scale: 2}, Nullable: true},
		{Name: "d64", Type: &arrow.Decimal64Type{Precision: 15, Scale: 4}, Nullable: true},
		{Name: "lv", Type: arrow.ListViewOf(arrow.PrimitiveTypes.Int32), Nullable: true},
		{Name: "llv", Type: arrow.LargeListViewOf(arrow.BinaryTypes.String), Nullable: true},
		{Name: "ree", Type: arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64), Nullable: false},
	}, nil)

	// Build Decimal32 column: [12.34, NULL, -56.78, 0.00]
	d32b := array.NewDecimal32Builder(alloc, &arrow.Decimal32Type{Precision: 6, Scale: 2})
	d32b.Append(decimal.Decimal32(1234))
	d32b.AppendNull()
	d32b.Append(decimal.Decimal32(-5678))
	d32b.Append(decimal.Decimal32(0))
	d32Arr := d32b.NewArray()
	d32b.Release()
	defer d32Arr.Release()

	// Build Decimal64 column: [123456.7890, -1.0000, NULL, 999999.9999]
	d64b := array.NewDecimal64Builder(alloc, &arrow.Decimal64Type{Precision: 15, Scale: 4})
	d64b.Append(decimal.Decimal64(1234567890))
	d64b.Append(decimal.Decimal64(-10000))
	d64b.AppendNull()
	d64b.Append(decimal.Decimal64(9999999999))
	d64Arr := d64b.NewArray()
	d64b.Release()
	defer d64Arr.Release()

	// Build ListView<int32>: [[10, 20], NULL, [], [30]]
	lvb := array.NewListViewBuilder(alloc, arrow.PrimitiveTypes.Int32)
	lvElem := lvb.ValueBuilder().(*array.Int32Builder)
	lvb.AppendWithSize(true, 2)
	lvElem.AppendValues([]int32{10, 20}, nil)
	lvb.AppendNull()
	lvb.AppendEmptyValue()
	lvb.AppendWithSize(true, 1)
	lvElem.Append(30)
	lvArr := lvb.NewArray()
	lvb.Release()
	defer lvArr.Release()

	// Build LargeListView<string>: [["a"], ["b", "c"], NULL, []]
	llvb := array.NewLargeListViewBuilder(alloc, arrow.BinaryTypes.String)
	llvElem := llvb.ValueBuilder().(*array.StringBuilder)
	llvb.AppendWithSize(true, 1)
	llvElem.Append("a")
	llvb.AppendWithSize(true, 2)
	llvElem.AppendValues([]string{"b", "c"}, nil)
	llvb.AppendNull()
	llvb.AppendEmptyValue()
	llvArr := llvb.NewArray()
	llvb.Release()
	defer llvArr.Release()

	// Build RunEndEncoded<int32, int64>: logical [100, 100, 200, 200] -> run_ends=[2, 4], values=[100, 200]
	reeb := array.NewRunEndEncodedBuilder(alloc, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64)
	reeVal := reeb.ValueBuilder().(*array.Int64Builder)
	reeb.Append(2)
	reeVal.Append(100)
	reeb.Append(2)
	reeVal.Append(200)
	reeArr := reeb.NewArray()
	reeb.Release()
	defer reeArr.Release()

	inRec := array.NewRecordBatch(schema, []arrow.Array{d32Arr, d64Arr, lvArr, llvArr, reeArr}, 4)
	defer inRec.Release()

	rawSchema, err := duckdbarrow.ExportSchema(schema)
	if err != nil {
		t.Fatalf("ExportSchema: %v", err)
	}
	gotSchema, err := duckdbarrow.ImportSchema(rawSchema)
	rawSchema.Release()
	if err != nil {
		t.Fatalf("ImportSchema: %v", err)
	}
	if !gotSchema.Equal(schema) {
		t.Fatalf("schema mismatch:\ngot:  %v\nwant: %v", gotSchema, schema)
	}

	rawArr, err := duckdbarrow.ExportRecordBatch(inRec)
	if err != nil {
		t.Fatalf("ExportRecordBatch: %v", err)
	}
	outRec, err := duckdbarrow.ImportRecordBatch(gotSchema, rawArr, alloc)
	rawArr.Release()
	if err != nil {
		t.Fatalf("ImportRecordBatch: %v", err)
	}
	defer outRec.Release()

	if !array.RecordEqual(inRec, outRec) {
		t.Fatalf("record batch mismatch:\ngot:  %v\nwant: %v", outRec, inRec)
	}
	if after := abi.ExportedArrowHandleCount(); after != before {
		t.Fatalf("exported Arrow handle leak: before=%d after=%d", before, after)
	}
}
