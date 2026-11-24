package duckdb

import (
	"context"
	"errors"
	"testing"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

func TestP4aMultiResultSetMockZeroLeaksAndEarlyClose(t *testing.T) {
	var (
		extractedAlive int
		stmtAlive      int
		resAlive       int
		chunkAlive     int
		nextStmtID     uintptr = 100
		stmtTypes              = map[uintptr]int32{}
		stmtData               = map[uintptr]int32{}
		chunkServed            = map[uintptr]bool{}
		chunkBuf               = map[uintptr]*int32{}
		colInt32               = uintptr(500)
	)
	colNameBuf := append([]byte("v"), 0)
	val10 := int32(10)
	val20 := int32(20)
	_ = &val10
	_ = &val20

	api := &abi.API{
		CreateConfig: func(out *uintptr) int32 {
			*out = 10
			return abi.StateSuccess
		},
		DestroyConfig: func(cfg *uintptr) { *cfg = 0 },
		OpenExt: func(path *byte, outDB *uintptr, config uintptr, outErr *uintptr) int32 {
			*outDB = 1
			return abi.StateSuccess
		},
		Close: func(db *uintptr) { *db = 0 },
		Connect: func(db uintptr, outConn *uintptr) int32 {
			*outConn = 2
			return abi.StateSuccess
		},
		Disconnect: func(conn *uintptr) { *conn = 0 },
		Interrupt:  func(conn uintptr) {},
		ExtractStatements: func(conn uintptr, query *byte, outExtracted *uintptr) uint64 {
			extractedAlive++
			*outExtracted = 900
			return 4 // [0]=CREATE, [1]=SELECT 10, [2]=SELECT 20, [3]=DROP
		},
		DestroyExtracted: func(extracted *uintptr) {
			if *extracted != 0 {
				extractedAlive--
				*extracted = 0
			}
		},
		PrepareExtractedStatement: func(conn uintptr, extracted uintptr, idx uint64, outStmt *uintptr) int32 {
			stmtAlive++
			nextStmtID++
			id := nextStmtID
			switch idx {
			case 0, 3:
				stmtTypes[id] = abi.StatementTypeCreate
			case 1:
				stmtTypes[id] = abi.StatementTypeSelect
				stmtData[id] = 10
			case 2:
				stmtTypes[id] = abi.StatementTypeSelect
				stmtData[id] = 20
			}
			*outStmt = id
			return abi.StateSuccess
		},
		PreparedStatementType: func(stmt uintptr) int32 {
			return stmtTypes[stmt]
		},
		DestroyPrepare: func(stmt *uintptr) {
			if *stmt != 0 {
				stmtAlive--
				*stmt = 0
			}
		},
		NParams:       func(stmt uintptr) uint64 { return 0 },
		ClearBindings: func(stmt uintptr) int32 { return abi.StateSuccess },
		PendingPreparedStreaming: func(stmt uintptr, outPending *uintptr) int32 {
			*outPending = stmt
			return abi.StateSuccess
		},
		PendingExecuteTask: func(pending uintptr) int32 { return abi.PendingResultReady },
		ExecutePending: func(pending uintptr, outRes *abi.Result) int32 {
			resAlive++
			*outRes = abi.Result{InternalData: pending}
			return abi.StateSuccess
		},
		DestroyPending:    func(pending *uintptr) { *pending = 0 },
		ResultIsStreaming: func(res abi.Result) bool { return true },
		ColumnCount:       func(res *abi.Result) uint64 { return 1 },
		ColumnName:        func(res *abi.Result, col uint64) uintptr { return uintptr(unsafe.Pointer(&colNameBuf[0])) },
		ColumnLogicalType: func(res *abi.Result, col uint64) uintptr { return colInt32 },
		GetTypeID:         func(lt uintptr) int32 { return abi.TypeInteger },
		LogicalTypeGetAlias: func(lt uintptr) uintptr {
			return 0
		},
		DestroyLogicalType: func(lt *uintptr) { *lt = 0 },
		RowsChanged:        func(res *abi.Result) uint64 { return 1 },
		ResultError:        func(res *abi.Result) uintptr { return 0 },
		ResultErrorType:    func(res *abi.Result) int32 { return abi.ErrorInvalid },
		DestroyResult: func(res *abi.Result) {
			if res.InternalData != 0 {
				resAlive--
				res.InternalData = 0
			}
		},
		FetchChunk: func(res abi.Result) uintptr {
			if chunkServed[res.InternalData] {
				return 0
			}
			chunkServed[res.InternalData] = true
			chunkAlive++
			cid := res.InternalData + 1000
			if stmtData[res.InternalData] == 10 {
				chunkBuf[cid] = &val10
			} else {
				chunkBuf[cid] = &val20
			}
			return cid
		},
		DataChunkGetSize:   func(chunk uintptr) uint64 { return 1 },
		DataChunkGetVector: func(chunk uintptr, col uint64) uintptr { return chunk },
		VectorGetValidity:  func(vec uintptr) uintptr { return 0 },
		VectorGetData: func(vec uintptr) uintptr {
			return uintptr(unsafe.Pointer(chunkBuf[vec]))
		},
		DestroyDataChunk: func(chunk *uintptr) {
			if *chunk != 0 {
				chunkAlive--
				*chunk = 0
			}
		},
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

	// 1. Full multi-result-set traversal: [0]=CREATE skipped, [1]=10, [2]=20, [3]=DROP executed on NextResultSet->false.
	rows, err := conn.QueryScript(ctx, "CREATE; SELECT 10; SELECT 20; DROP")
	if err != nil {
		t.Fatalf("QueryScript: %v", err)
	}
	if !rows.HasNextResultSet() {
		t.Fatal("expected HasNextResultSet == true after first SELECT")
	}
	if !rows.Next() {
		t.Fatalf("first result set Next() = false, err = %v", rows.Err())
	}
	vals, err := rows.DecodeRow(nil)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := vals[0].Int32(); n != 10 {
		t.Fatalf("rs1 = %d, want 10", n)
	}
	if rows.Next() {
		t.Fatal("expected rs1 EOF")
	}
	if !rows.NextResultSet() {
		t.Fatalf("NextResultSet() to rs2 = false, err = %v", rows.Err())
	}
	if !rows.Next() {
		t.Fatalf("rs2 Next() = false, err = %v", rows.Err())
	}
	vals, err = rows.DecodeRow(nil)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := vals[0].Int32(); n != 20 {
		t.Fatalf("rs2 = %d, want 20", n)
	}
	if rows.Next() {
		t.Fatal("expected rs2 EOF")
	}
	// Trailing statement [3]=DROP executes during NextResultSet() and returns false cleanly.
	if rows.NextResultSet() {
		t.Fatal("expected no third result set")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err = %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if extractedAlive != 0 || stmtAlive != 0 || resAlive != 0 || chunkAlive != 0 {
		t.Fatalf("leaked native handles after full walk: extracted=%d stmt=%d res=%d chunk=%d", extractedAlive, stmtAlive, resAlive, chunkAlive)
	}

	// 2. Early Close() before NextResultSet() must destroy extracted & current result with zero leaks.
	rows2, err := conn.QueryScript(ctx, "CREATE; SELECT 10; SELECT 20; DROP")
	if err != nil {
		t.Fatal(err)
	}
	if !rows2.HasNextResultSet() {
		t.Fatal("expected HasNextResultSet == true")
	}
	if err := rows2.Close(); err != nil {
		t.Fatal(err)
	}
	if rows2.HasNextResultSet() {
		t.Fatal("closed rows must report HasNextResultSet == false")
	}
	if extractedAlive != 0 || stmtAlive != 0 || resAlive != 0 || chunkAlive != 0 {
		t.Fatalf("leaked native handles after early Close: extracted=%d stmt=%d res=%d chunk=%d", extractedAlive, stmtAlive, resAlive, chunkAlive)
	}

	// 3. Excess positional parameters on ExecScript fail with ErrInvalidInput.
	if _, err := conn.ExecScript(ctx, "CREATE; SELECT 10; SELECT 20; DROP", Int32Value(1)); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("excess positional args = %v, want ErrInvalidInput", err)
	}
}
