package duckdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

func TestMetadataMockZeroLeaks(t *testing.T) {
	var (
		cStrings      [][]byte
		liveLogTypes  int
		liveTableDesc int
		freedCount    int
	)
	allocCStr := func(s string) uintptr {
		b, _ := abi.CString(s)
		cStrings = append(cStrings, b)
		return uintptr(unsafe.Pointer(&b[0]))
	}
	nextHandle := uintptr(100)
	allocLT := func() uintptr {
		nextHandle++
		liveLogTypes++
		return nextHandle
	}

	ltTypes := map[uintptr]int32{}
	fakeAPI := &abi.API{
		ConfigCount: func() uintptr { return 2 },
		GetConfigFlag: func(index uintptr, outName, outDesc *uintptr) int32 {
			switch index {
			case 0:
				*outName = allocCStr("memory_limit")
				*outDesc = allocCStr("Maximum memory")
				return abi.StateSuccess
			case 1:
				*outName = allocCStr("threads")
				*outDesc = allocCStr("Worker threads")
				return abi.StateSuccess
			default:
				return abi.StateError
			}
		},
		Free: func(ptr uintptr) {
			if ptr != 0 {
				freedCount++
			}
		},
		PreparedStatementType: func(stmt uintptr) int32 {
			return abi.StatementTypeSelect
		},
		NParams: func(stmt uintptr) uint64 { return 2 },
		ParamLogicalType: func(stmt uintptr, idx uint64) uintptr {
			h := allocLT()
			if idx == 1 {
				ltTypes[h] = abi.TypeInteger
			} else {
				ltTypes[h] = abi.TypeAny
			}
			return h
		},
		GetTypeID: func(lt uintptr) int32 { return ltTypes[lt] },
		LogicalTypeGetAlias: func(lt uintptr) uintptr {
			return 0
		},
		DestroyLogicalType: func(lt *uintptr) {
			if lt != nil && *lt != 0 {
				liveLogTypes--
				*lt = 0
			}
		},
		TableDescriptionCreateExt: func(conn uintptr, catalog, schema, table *byte, out *uintptr) int32 {
			liveTableDesc++
			*out = 777
			if abi.GoString(uintptr(unsafe.Pointer(table))) == "missing" {
				return abi.StateError
			}
			return abi.StateSuccess
		},
		TableDescriptionDestroy: func(td *uintptr) {
			if td != nil && *td != 0 {
				liveTableDesc--
				*td = 0
			}
		},
		TableDescriptionError: func(td uintptr) uintptr {
			return allocCStr("Catalog Error: Table with name missing does not exist!")
		},
		TableDescriptionGetColumnCount: func(td uintptr) uint64 { return 2 },
		TableDescriptionGetColumnName: func(td uintptr, idx uint64) uintptr {
			if idx == 0 {
				return allocCStr("id")
			}
			return allocCStr("label")
		},
		TableDescriptionGetColumnType: func(td uintptr, idx uint64) uintptr {
			h := allocLT()
			if idx == 0 {
				ltTypes[h] = abi.TypeBigint
			} else {
				ltTypes[h] = abi.TypeVarchar
			}
			return h
		},
		ColumnHasDefault: func(td uintptr, idx uint64, out *bool) int32 {
			*out = idx == 1
			return abi.StateSuccess
		},
	}

	lib := &Library{api: fakeAPI}
	flags, err := lib.ConfigFlags()
	if err != nil {
		t.Fatalf("ConfigFlags: %v", err)
	}
	if len(flags) != 2 || flags[0].Name != "memory_limit" || flags[1].Description != "Worker threads" {
		t.Fatalf("unexpected flags: %+v", flags)
	}
	if freedCount != 0 {
		t.Fatalf("ConfigFlags must not free borrowed static strings; freed=%d", freedCount)
	}

	conn := &Conn{
		api:    fakeAPI,
		handle: 1,
		limits: DefaultDecodeLimits(),
		stmts:  make(map[*Stmt]struct{}),
	}
	stmt := &Stmt{conn: conn, handle: 2}
	st, err := stmt.StatementType()
	if err != nil || st != StatementTypeSelect || st.String() != "SELECT" {
		t.Fatalf("StatementType = %v (%v), want SELECT", st, err)
	}
	ptypes, err := stmt.ParamTypes()
	if err != nil {
		t.Fatalf("ParamTypes: %v", err)
	}
	if len(ptypes) != 2 || ptypes[0].Kind != TypeInteger || ptypes[1].Kind != TypeInvalid {
		t.Fatalf("ParamTypes = %+v, want [INTEGER, INVALID]", ptypes)
	}
	if liveLogTypes != 0 {
		t.Fatalf("leaked %d logical types after ParamTypes", liveLogTypes)
	}

	cols, err := conn.DescribeTable(context.Background(), "", "", "items")
	if err != nil {
		t.Fatalf("DescribeTable: %v", err)
	}
	if len(cols) != 2 || cols[0].Name != "id" || cols[0].HasDefault || cols[1].Name != "label" || !cols[1].HasDefault {
		t.Fatalf("DescribeTable = %+v", cols)
	}
	if liveTableDesc != 0 || liveLogTypes != 0 || freedCount != 2 {
		t.Fatalf("DescribeTable leak check: td=%d lt=%d freed=%d", liveTableDesc, liveLogTypes, freedCount)
	}

	_, err = conn.DescribeTable(context.Background(), "", "", "missing")
	if err == nil {
		t.Fatal("expected error for missing table")
	}
	var nativeErr *Error
	if !errors.As(err, &nativeErr) || nativeErr.Kind != ErrorKindCatalog {
		t.Fatalf("missing table error = %v, want ErrorKindCatalog", err)
	}
	if liveTableDesc != 0 {
		t.Fatalf("leaked %d table descriptions on error path", liveTableDesc)
	}
}

func TestArrowExportAndViewMockZeroLeaks(t *testing.T) {
	before := abi.ExportedArrowHandleCount()

	// Encode Arrow C Data Interface metadata with 1 pair: ARROW:extension:name = arrow.uuid.
	var md []byte
	md = binary.NativeEndian.AppendUint32(md, 1)
	k := "ARROW:extension:name"
	v := "arrow.uuid"
	md = binary.NativeEndian.AppendUint32(md, uint32(len(k)))
	md = append(md, k...)
	md = binary.NativeEndian.AppendUint32(md, uint32(len(v)))
	md = append(md, v...)

	schema, err := NewArrowSchema(ArrowSchemaSpec{
		Format: "+s",
		Children: []ArrowSchemaSpec{
			{Format: "w:16", Name: "id", Metadata: md, Flags: 2},
		},
	})
	if err != nil {
		t.Fatalf("NewArrowSchema: %v", err)
	}
	gotMD := schema.View().Child(0).MetadataBytes()
	if !bytes.Equal(gotMD, md) {
		t.Fatalf("MetadataBytes mismatch: got %x want %x", gotMD, md)
	}

	released := 0
	buf := make([]byte, 16)
	arr, err := NewArrowArray(ArrowArraySpec{
		Length:  1,
		Buffers: [][]byte{nil},
		Children: []ArrowArraySpec{
			{Length: 1, Buffers: [][]byte{nil, buf}},
		},
	}, func() { released++ })
	if err != nil {
		t.Fatalf("NewArrowArray: %v", err)
	}

	var (
		streamsLive int
		failScan    bool
	)
	fakeAPI := &abi.API{
		ArrowArrayScan: func(conn uintptr, name *byte, s *abi.ArrowSchema, a *abi.ArrowArray, out *uintptr) int32 {
			if failScan {
				return abi.StateError
			}
			streamsLive++
			*out = 999
			return abi.StateSuccess
		},
		DestroyArrowStream: func(stream *uintptr) {
			if stream != nil && *stream != 0 {
				streamsLive--
				*stream = 0
			}
		},
		Disconnect: func(conn *uintptr) { *conn = 0 },
	}
	conn := &Conn{
		api:        fakeAPI,
		handle:     1,
		stmts:      make(map[*Stmt]struct{}),
		arrowViews: make(map[*ArrowView]struct{}),
	}

	// Failure path leaves schema and array owned by caller.
	failScan = true
	if _, err := conn.RegisterArrowView(context.Background(), "v_fail", schema, arr); err == nil {
		t.Fatal("expected ArrowArrayScan failure")
	}
	if schema.Released() || arr.Released() {
		t.Fatal("schema/array must stay owned by caller on RegisterArrowView error")
	}

	// Success path transfers ownership to ArrowView, cleaned up by ArrowView.Close.
	failScan = false
	view, err := conn.RegisterArrowView(context.Background(), "v_ok", schema, arr)
	if err != nil {
		t.Fatalf("RegisterArrowView: %v", err)
	}
	if !schema.Released() || !arr.Released() {
		t.Fatal("schema/array should be marked released after ownership transfer")
	}
	if streamsLive != 1 {
		t.Fatalf("streamsLive = %d, want 1", streamsLive)
	}
	if err := view.Close(); err != nil {
		t.Fatalf("view.Close: %v", err)
	}
	if err := view.Close(); err != nil { // idempotent
		t.Fatalf("second view.Close: %v", err)
	}
	if streamsLive != 0 || released != 1 {
		t.Fatalf("after view.Close: streamsLive=%d released=%d", streamsLive, released)
	}
	if got := abi.ExportedArrowHandleCount(); got != before {
		t.Fatalf("leaked exported arrow handles: got %d, want %d", got, before)
	}
}
