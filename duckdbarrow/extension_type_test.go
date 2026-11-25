package duckdbarrow_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/bububa/duckdbspike/duckdb"
	"github.com/bububa/duckdbspike/duckdbarrow"
)

type testTagExtType struct {
	arrow.ExtensionBase
	tag string
}

type testTagExtArray struct {
	array.ExtensionArrayBase
}

func newTestTagExtType(tag string) *testTagExtType {
	return &testTagExtType{
		ExtensionBase: arrow.ExtensionBase{Storage: arrow.BinaryTypes.String},
		tag:           tag,
	}
}

func (t *testTagExtType) ArrayType() reflect.Type { return reflect.TypeFor[testTagExtArray]() }
func (t *testTagExtType) ExtensionName() string   { return "duckdbspike.test_tag" }
func (t *testTagExtType) Serialize() string       { return t.tag }
func (t *testTagExtType) ExtensionEquals(other arrow.ExtensionType) bool {
	o, ok := other.(*testTagExtType)
	return ok && t.tag == o.tag
}
func (t *testTagExtType) Deserialize(storage arrow.DataType, data string) (arrow.ExtensionType, error) {
	if !arrow.TypeEqual(storage, arrow.BinaryTypes.String) {
		return nil, fmt.Errorf("unexpected storage type %s", storage)
	}
	if data == "BAD" {
		return nil, errors.New("rejected metadata")
	}
	return newTestTagExtType(data), nil
}

func TestArrowExtensionTypeRoundTripAndFallback(t *testing.T) {
	if err := arrow.RegisterExtensionType(newTestTagExtType("")); err != nil {
		t.Fatalf("RegisterExtensionType: %v", err)
	}
	defer func() {
		_ = arrow.UnregisterExtensionType("duckdbspike.test_tag")
	}()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	extType := newTestTagExtType("v1")
	unregMeta := arrow.NewMetadata(
		[]string{"ARROW:extension:name", "ARROW:extension:metadata"},
		[]string{"duckdbspike.unregistered_ext", "opaque"},
	)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ext_col", Type: extType, Nullable: true},
		{Name: "nested", Type: arrow.StructOf(arrow.Field{Name: "inner_ext", Type: extType, Nullable: true}), Nullable: true},
		{Name: "unreg", Type: arrow.PrimitiveTypes.Int32, Nullable: true, Metadata: unregMeta},
	}, nil)

	sb := array.NewStringBuilder(alloc)
	sb.Append("alpha")
	sb.AppendNull()
	sb.Append("beta")
	storageArr := sb.NewArray()
	sb.Release()
	extArr := array.NewExtensionArrayWithStorage(extType, storageArr)
	storageArr.Release()

	// Build struct column containing an ExtensionArray child.
	sb2 := array.NewStringBuilder(alloc)
	sb2.Append("s1")
	sb2.Append("s2")
	sb2.AppendNull()
	storageArr2 := sb2.NewArray()
	sb2.Release()
	innerExtArr := array.NewExtensionArrayWithStorage(extType, storageArr2)
	storageArr2.Release()
	structData := array.NewData(
		schema.Field(1).Type,
		3,
		[]*memory.Buffer{nil},
		[]arrow.ArrayData{innerExtArr.Data()},
		0,
		0,
	)
	structArr := array.NewStructData(structData)
	structData.Release()
	innerExtArr.Release()

	ib := array.NewInt32Builder(alloc)
	ib.AppendValues([]int32{10, 20, 30}, nil)
	intArr := ib.NewArray()
	ib.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{extArr, structArr, intArr}, 3)
	extArr.Release()
	structArr.Release()
	intArr.Release()
	defer rec.Release()

	cSchema, err := duckdbarrow.ExportSchema(schema)
	if err != nil {
		t.Fatalf("ExportSchema: %v", err)
	}
	defer cSchema.Release()
	cArr, err := duckdbarrow.ExportRecordBatch(rec)
	if err != nil {
		t.Fatalf("ExportRecordBatch: %v", err)
	}
	defer cArr.Release()

	gotSchema, err := duckdbarrow.ImportSchema(cSchema)
	if err != nil {
		t.Fatalf("ImportSchema: %v", err)
	}
	gotExt, ok := gotSchema.Field(0).Type.(*testTagExtType)
	if !ok || gotExt.tag != "v1" {
		t.Fatalf("field 0 type = %T (%v), want *testTagExtType(v1)", gotSchema.Field(0).Type, gotSchema.Field(0).Type)
	}
	gotStruct, ok := gotSchema.Field(1).Type.(*arrow.StructType)
	if !ok {
		t.Fatalf("field 1 type = %T, want *arrow.StructType", gotSchema.Field(1).Type)
	}
	if innerExt, ok := gotStruct.Field(0).Type.(*testTagExtType); !ok || innerExt.tag != "v1" {
		t.Fatalf("nested field type = %T, want *testTagExtType(v1)", gotStruct.Field(0).Type)
	}
	// Unregistered extension falls back cleanly to storage type while preserving metadata.
	if !arrow.TypeEqual(gotSchema.Field(2).Type, arrow.PrimitiveTypes.Int32) {
		t.Fatalf("unregistered ext type = %s, want int32", gotSchema.Field(2).Type)
	}
	if idx := gotSchema.Field(2).Metadata.FindKey("ARROW:extension:name"); idx < 0 || gotSchema.Field(2).Metadata.Values()[idx] != "duckdbspike.unregistered_ext" {
		t.Fatalf("unregistered extension metadata not preserved: %v", gotSchema.Field(2).Metadata)
	}

	gotRec, err := duckdbarrow.ImportRecordBatch(gotSchema, cArr, alloc)
	if err != nil {
		t.Fatalf("ImportRecordBatch: %v", err)
	}
	defer gotRec.Release()

	extCol, ok := gotRec.Column(0).(*testTagExtArray)
	if !ok {
		t.Fatalf("col 0 = %T, want *testTagExtArray", gotRec.Column(0))
	}
	strStorage := extCol.Storage().(*array.String)
	if strStorage.Value(0) != "alpha" || !extCol.IsNull(1) || strStorage.Value(2) != "beta" {
		t.Fatalf("ext col values mismatch: [0]=%q null1=%v [2]=%q", strStorage.Value(0), extCol.IsNull(1), strStorage.Value(2))
	}
	nestedCol := gotRec.Column(1).(*array.Struct)
	nestedExt, ok := nestedCol.Field(0).(*testTagExtArray)
	if !ok {
		t.Fatalf("nested col = %T, want *testTagExtArray", nestedCol.Field(0))
	}
	nestedStr := nestedExt.Storage().(*array.String)
	if nestedStr.Value(0) != "s1" || nestedStr.Value(1) != "s2" || !nestedExt.IsNull(2) {
		t.Fatalf("nested ext mismatch")
	}

	// Invalid extension metadata fails cleanly with ErrInvalidInput.
	badSchema := arrow.NewSchema([]arrow.Field{
		{Name: "bad", Type: newTestTagExtType("BAD"), Nullable: true},
	}, nil)
	badCSchema, err := duckdbarrow.ExportSchema(badSchema)
	if err != nil {
		t.Fatalf("ExportSchema(bad): %v", err)
	}
	defer badCSchema.Release()
	if _, err := duckdbarrow.ImportSchema(badCSchema); !errors.Is(err, duckdb.ErrInvalidInput) {
		t.Fatalf("ImportSchema(bad ext metadata) err = %v, want ErrInvalidInput", err)
	}
}
