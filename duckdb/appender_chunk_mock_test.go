package duckdb

import (
	"context"
	"errors"
	"testing"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

func TestP4cChunkBufferedAppenderMockAutoFlushRollbackAndZeroLeaks(t *testing.T) {
	const capRows = 2
	var (
		chunksAlive      int
		appChunksFlushed []uint64
		defaultCalled    int
		col0Data         [capRows]int32
		col0Valid        [1]uint64
		col1ListData     [capRows]abi.ListEntry
		col1ChildSize    uint64
		col1ChildData    [16]int32
		chunkCurSize     uint64
	)
	col0Valid[0] = ^uint64(0)

	const (
		ltInt   uintptr = 100
		ltList  uintptr = 101
		vecCol0 uintptr = 200
		vecCol1 uintptr = 201
		vecElem uintptr = 202
	)

	api := &abi.API{
		CreateConfig:  func(out *uintptr) int32 { *out = 1; return abi.StateSuccess },
		DestroyConfig: func(cfg *uintptr) { *cfg = 0 },
		OpenExt: func(path *byte, outDB *uintptr, cfg uintptr, outErr *uintptr) int32 {
			*outDB = 1
			return abi.StateSuccess
		},
		Close:               func(db *uintptr) { *db = 0 },
		Connect:             func(db uintptr, outConn *uintptr) int32 { *outConn = 2; return abi.StateSuccess },
		Disconnect:          func(conn *uintptr) { *conn = 0 },
		Interrupt:           func(conn uintptr) {},
		AppenderCreateExt:   func(conn uintptr, cat, sch, tbl *byte, out *uintptr) int32 { *out = 50; return abi.StateSuccess },
		AppenderColumnCount: func(app uintptr) uint64 { return 2 },
		AppenderColumnType: func(app uintptr, idx uint64) uintptr {
			if idx == 0 {
				return ltInt
			}
			return ltList
		},
		GetTypeID: func(lt uintptr) int32 {
			if lt == ltInt {
				return abi.TypeInteger
			}
			return abi.TypeList
		},
		ListTypeChildType:  func(lt uintptr) uintptr { return ltInt },
		DestroyLogicalType: func(lt *uintptr) { *lt = 0 },
		VectorSize:         func() uint64 { return capRows },
		CreateDataChunk: func(types *uintptr, n uint64) uintptr {
			chunksAlive++
			col1ChildSize = 0
			return 300
		},
		DestroyDataChunk: func(ch *uintptr) {
			if *ch != 0 {
				chunksAlive--
				*ch = 0
			}
		},
		DataChunkSetSize: func(ch uintptr, sz uint64) { chunkCurSize = sz },
		DataChunkReset: func(ch uintptr) {
			chunkCurSize = 0
			col1ChildSize = 0
			col0Valid[0] = ^uint64(0)
		},
		DataChunkGetVector: func(ch uintptr, col uint64) uintptr {
			if col == 0 {
				return vecCol0
			}
			return vecCol1
		},
		VectorGetData: func(vec uintptr) uintptr {
			switch vec {
			case vecCol0:
				return uintptr(unsafe.Pointer(&col0Data[0]))
			case vecCol1:
				return uintptr(unsafe.Pointer(&col1ListData[0]))
			case vecElem:
				return uintptr(unsafe.Pointer(&col1ChildData[0]))
			default:
				return 0
			}
		},
		VectorGetValidity: func(vec uintptr) uintptr {
			if vec == vecCol0 {
				return uintptr(unsafe.Pointer(&col0Valid[0]))
			}
			return 0
		},
		VectorEnsureValidityWritable:       func(vec uintptr) {},
		UnsafeVectorAssignStringElementLen: func(vec uintptr, idx uint64, s *byte, l uint64) {},
		ListVectorGetChild:                 func(vec uintptr) uintptr { return vecElem },
		ListVectorGetSize:                  func(vec uintptr) uint64 { return col1ChildSize },
		ListVectorReserve:                  func(vec uintptr, cap uint64) int32 { return abi.StateSuccess },
		ListVectorSetSize: func(vec uintptr, sz uint64) int32 {
			col1ChildSize = sz
			return abi.StateSuccess
		},
		AppendDefaultToChunk: func(app, ch uintptr, col, row uint64) int32 {
			defaultCalled++
			return abi.StateSuccess
		},
		AppendDataChunk: func(app, ch uintptr) int32 {
			appChunksFlushed = append(appChunksFlushed, chunkCurSize)
			return abi.StateSuccess
		},
		AppenderFlush:   func(app uintptr) int32 { return abi.StateSuccess },
		AppenderClear:   func(app uintptr) int32 { return abi.StateSuccess },
		AppenderClose:   func(app uintptr) int32 { return abi.StateSuccess },
		AppenderDestroy: func(app *uintptr) int32 { *app = 0; return abi.StateSuccess },
	}

	lib := &Library{api: api}
	ctx := context.Background()
	db, err := lib.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	app, err := conn.NewAppender(ctx, "", "", "t")
	if err != nil {
		t.Fatal(err)
	}
	if chunksAlive != 1 {
		t.Fatalf("expected 1 allocated chunk, got %d", chunksAlive)
	}

	list1, _ := NewListValue(ScalarType(TypeInteger), []Value{Int32Value(10), Int32Value(20)})
	listBad, _ := NewListValue(ScalarType(TypeVarchar), []Value{VarcharValue("bad")})

	// Row 0: buffered (no flush yet).
	if err := app.AppendRow(Int32Value(1), list1); err != nil {
		t.Fatalf("row 0: %v", err)
	}
	if len(appChunksFlushed) != 0 || col1ChildSize != 2 {
		t.Fatalf("after row 0: flushed=%v childSize=%d", appChunksFlushed, col1ChildSize)
	}

	// Row 1 fails on col 1 (wrong list element type): col1ChildSize must roll back to 2 and chunkRow stay 1!
	if err := app.AppendRow(Int32Value(2), listBad); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("bad list row = %v, want ErrTypeMismatch", err)
	}
	if col1ChildSize != 2 || len(appChunksFlushed) != 0 {
		t.Fatalf("after failed row: childSize=%d flushed=%v", col1ChildSize, appChunksFlushed)
	}

	// Valid row 1 with DefaultValue() on col 0 hits capRows=2 -> auto-flushes chunk of size 2!
	if err := app.AppendRow(DefaultValue(), list1); err != nil {
		t.Fatalf("row 1 default: %v", err)
	}
	if defaultCalled != 1 || len(appChunksFlushed) != 1 || appChunksFlushed[0] != 2 {
		t.Fatalf("after row 1: defaultCalled=%d flushed=%v", defaultCalled, appChunksFlushed)
	}

	// AppendRows(3 rows): flushes chunk of 2, leaves 1 buffered; Clear() discards the buffered row.
	if err := app.AppendRows([][]Value{
		{Int32Value(3), list1},
		{NullValue(), list1},
		{Int32Value(5), list1},
	}); err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	if len(appChunksFlushed) != 2 || appChunksFlushed[1] != 2 {
		t.Fatalf("after AppendRows: flushed=%v", appChunksFlushed)
	}
	if err := app.Clear(); err != nil {
		t.Fatal(err)
	}

	// Append 1 more row and Close(): flushes final chunk of size 1 and destroys DataChunk cleanly.
	if err := app.AppendRow(Int32Value(6), list1); err != nil {
		t.Fatal(err)
	}
	if err := app.Close(); err != nil {
		t.Fatal(err)
	}
	if len(appChunksFlushed) != 3 || appChunksFlushed[2] != 1 {
		t.Fatalf("final flushed = %v, want [2 2 1]", appChunksFlushed)
	}
	if chunksAlive != 0 {
		t.Fatalf("leaked chunks: %d", chunksAlive)
	}
}
