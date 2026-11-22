package duckdb

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

// fakeNative deterministically models DuckDB's handle allocation/release
// semantics and tracks every live native resource so tests can assert zero
// leaks on every error and close path.
type fakeNative struct {
	mu     sync.Mutex
	nextID uintptr
	pinner runtime.Pinner

	configs  map[uintptr]struct{}
	dbs      map[uintptr]struct{}
	conns    map[uintptr]struct{}
	stmts    map[uintptr]*fakeStmt
	pendings map[uintptr]*fakePending
	results  map[uintptr]*fakeResult
	chunks   map[uintptr]uint64
	ltypes   map[uintptr]int32
	values   map[uintptr]struct{}
	ownedStr map[uintptr][]byte

	doubleFree               int
	interruptAfterDisconnect int
	interrupts               int
	onInterrupt              func(conn uintptr)

	// Failure / behavior injection hooks.
	failCreateConfig      bool
	failCreateConfigAlloc bool // allocate even on failure
	failSetConfigAt       int  // 1-based call index; 0 = never
	setConfigCalls        int
	failOpenExt           string
	failOpenExtAllocDB    bool
	failConnect           bool
	failConnectAlloc      bool
	failPrepare           string
	failPrepareNoAlloc    bool
	failPending           string
	failPendingNoAlloc    bool
	pendingStates         []int32
	pendingErrMsg         string
	failExecute           string
	failExecuteErrType    int32
	nonStreamingResult    bool
	resultColumns         []string
	resultRowsAffected    uint64
	chunkSizes            []uint64
	fetchErrMsg           string
	fetchErrType          int32
	nParams               uint64
}

type fakeStmt struct {
	errMsg string
}

type fakePending struct {
	errMsg   string
	stateIdx int
	states   []int32
}

type fakeResult struct {
	streaming    bool
	errMsg       string
	errType      int32
	columns      []string
	rowsAffected uint64
	chunkIdx     int
	chunkSizes   []uint64
	fetchErrMsg  string
	fetchErrType int32
}

func newFakeNative(t *testing.T) (*fakeNative, *Library) {
	t.Helper()
	f := &fakeNative{
		nextID:   1,
		configs:  make(map[uintptr]struct{}),
		dbs:      make(map[uintptr]struct{}),
		conns:    make(map[uintptr]struct{}),
		stmts:    make(map[uintptr]*fakeStmt),
		pendings: make(map[uintptr]*fakePending),
		results:  make(map[uintptr]*fakeResult),
		chunks:   make(map[uintptr]uint64),
		ltypes:   make(map[uintptr]int32),
		values:   make(map[uintptr]struct{}),
		ownedStr: make(map[uintptr][]byte),
	}
	t.Cleanup(func() {
		f.pinner.Unpin()
	})
	return f, &Library{api: f.api()}
}

func (f *fakeNative) allocID() uintptr {
	id := f.nextID
	f.nextID++
	return id
}

func (f *fakeNative) cstr(s string) uintptr {
	if s == "" {
		return 0
	}
	b := append([]byte(s), 0)
	f.pinner.Pin(&b[0])
	return uintptr(unsafe.Pointer(&b[0]))
}

func (f *fakeNative) ownedCstr(s string) uintptr {
	if s == "" {
		return 0
	}
	b := append([]byte(s), 0)
	f.pinner.Pin(&b[0])
	ptr := uintptr(unsafe.Pointer(&b[0]))
	f.ownedStr[ptr] = b
	return ptr
}

func (f *fakeNative) assertZeroLeaks(t *testing.T) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.doubleFree != 0 {
		t.Fatalf("detected %d double-free calls", f.doubleFree)
	}
	if f.interruptAfterDisconnect != 0 {
		t.Fatalf("detected %d duckdb_interrupt calls after disconnect", f.interruptAfterDisconnect)
	}
	if len(f.configs) != 0 || len(f.dbs) != 0 || len(f.conns) != 0 ||
		len(f.stmts) != 0 || len(f.pendings) != 0 || len(f.results) != 0 ||
		len(f.chunks) != 0 || len(f.ltypes) != 0 || len(f.values) != 0 || len(f.ownedStr) != 0 {
		t.Fatalf("native resource leak: configs=%d dbs=%d conns=%d stmts=%d pendings=%d results=%d chunks=%d ltypes=%d values=%d ownedStr=%d",
			len(f.configs), len(f.dbs), len(f.conns), len(f.stmts),
			len(f.pendings), len(f.results), len(f.chunks), len(f.ltypes), len(f.values), len(f.ownedStr))
	}
}

func (f *fakeNative) api() *abi.API {
	return &abi.API{
		CreateConfig: func(out *uintptr) int32 {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.failCreateConfig {
				if f.failCreateConfigAlloc {
					id := f.allocID()
					f.configs[id] = struct{}{}
					*out = id
				}
				return abi.StateError
			}
			id := f.allocID()
			f.configs[id] = struct{}{}
			*out = id
			return abi.StateSuccess
		},
		SetConfig: func(config uintptr, name, option *byte) int32 {
			f.mu.Lock()
			defer f.mu.Unlock()
			if _, ok := f.configs[config]; !ok {
				return abi.StateError
			}
			f.setConfigCalls++
			if f.failSetConfigAt > 0 && f.setConfigCalls == f.failSetConfigAt {
				return abi.StateError
			}
			return abi.StateSuccess
		},
		DestroyConfig: func(config *uintptr) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if config == nil || *config == 0 {
				return
			}
			if _, ok := f.configs[*config]; !ok {
				f.doubleFree++
			}
			delete(f.configs, *config)
			*config = 0
		},
		OpenExt: func(path *byte, out *uintptr, config uintptr, outError *uintptr) int32 {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.failOpenExt != "" {
				if outError != nil {
					*outError = f.ownedCstr(f.failOpenExt)
				}
				if f.failOpenExtAllocDB {
					id := f.allocID()
					f.dbs[id] = struct{}{}
					*out = id
				}
				return abi.StateError
			}
			id := f.allocID()
			f.dbs[id] = struct{}{}
			*out = id
			return abi.StateSuccess
		},
		Close: func(db *uintptr) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if db == nil || *db == 0 {
				return
			}
			if _, ok := f.dbs[*db]; !ok {
				f.doubleFree++
			}
			delete(f.dbs, *db)
			*db = 0
		},
		Free: func(ptr uintptr) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if ptr == 0 {
				return
			}
			if _, ok := f.ownedStr[ptr]; !ok {
				f.doubleFree++
			}
			delete(f.ownedStr, ptr)
		},
		Connect: func(db uintptr, out *uintptr) int32 {
			f.mu.Lock()
			defer f.mu.Unlock()
			if _, ok := f.dbs[db]; !ok {
				return abi.StateError
			}
			if f.failConnect {
				if f.failConnectAlloc {
					id := f.allocID()
					f.conns[id] = struct{}{}
					*out = id
				}
				return abi.StateError
			}
			id := f.allocID()
			f.conns[id] = struct{}{}
			*out = id
			return abi.StateSuccess
		},
		Disconnect: func(conn *uintptr) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if conn == nil || *conn == 0 {
				return
			}
			if _, ok := f.conns[*conn]; !ok {
				f.doubleFree++
			}
			delete(f.conns, *conn)
			*conn = 0
		},
		Interrupt: func(conn uintptr) {
			f.mu.Lock()
			if _, ok := f.conns[conn]; !ok {
				f.interruptAfterDisconnect++
			}
			f.interrupts++
			cb := f.onInterrupt
			f.mu.Unlock()
			if cb != nil {
				cb(conn)
			}
		},
		Prepare: func(conn uintptr, query *byte, out *uintptr) int32 {
			f.mu.Lock()
			defer f.mu.Unlock()
			if _, ok := f.conns[conn]; !ok {
				return abi.StateError
			}
			if f.failPrepare != "" {
				if !f.failPrepareNoAlloc {
					id := f.allocID()
					f.stmts[id] = &fakeStmt{errMsg: f.failPrepare}
					*out = id
				}
				return abi.StateError
			}
			id := f.allocID()
			f.stmts[id] = &fakeStmt{}
			*out = id
			return abi.StateSuccess
		},
		PrepareError: func(stmt uintptr) uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			s, ok := f.stmts[stmt]
			if !ok {
				return 0
			}
			return f.cstr(s.errMsg)
		},
		DestroyPrepare: func(stmt *uintptr) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if stmt == nil || *stmt == 0 {
				return
			}
			if _, ok := f.stmts[*stmt]; !ok {
				f.doubleFree++
			}
			delete(f.stmts, *stmt)
			*stmt = 0
		},
		NParams: func(stmt uintptr) uint64 {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.nParams
		},
		ClearBindings: func(stmt uintptr) int32 {
			f.mu.Lock()
			defer f.mu.Unlock()
			if _, ok := f.stmts[stmt]; !ok {
				return abi.StateError
			}
			return abi.StateSuccess
		},
		BindNull:  func(stmt uintptr, idx uint64) int32 { return abi.StateSuccess },
		BindInt64: func(stmt uintptr, idx uint64, val int64) int32 { return abi.StateSuccess },
		ColumnLogicalType: func(result *abi.Result, col uint64) uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			id := f.allocID()
			f.ltypes[id] = abi.TypeBigint
			return id
		},
		GetTypeID: func(lt uintptr) int32 {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.ltypes[lt]
		},
		DestroyLogicalType: func(lt *uintptr) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if lt == nil || *lt == 0 {
				return
			}
			if _, ok := f.ltypes[*lt]; !ok {
				f.doubleFree++
			}
			delete(f.ltypes, *lt)
			*lt = 0
		},
		PendingPreparedStreaming: func(stmt uintptr, out *uintptr) int32 {
			f.mu.Lock()
			defer f.mu.Unlock()
			if _, ok := f.stmts[stmt]; !ok {
				return abi.StateError
			}
			if f.failPending != "" {
				if !f.failPendingNoAlloc {
					id := f.allocID()
					f.pendings[id] = &fakePending{errMsg: f.failPending}
					*out = id
				}
				return abi.StateError
			}
			id := f.allocID()
			states := append([]int32(nil), f.pendingStates...)
			if len(states) == 0 {
				states = []int32{abi.PendingResultReady}
			}
			f.pendings[id] = &fakePending{states: states, errMsg: f.pendingErrMsg}
			*out = id
			return abi.StateSuccess
		},
		PendingExecuteTask: func(pending uintptr) int32 {
			f.mu.Lock()
			defer f.mu.Unlock()
			p, ok := f.pendings[pending]
			if !ok {
				return abi.PendingError
			}
			if p.stateIdx >= len(p.states) {
				return abi.PendingResultReady
			}
			st := p.states[p.stateIdx]
			p.stateIdx++
			return st
		},
		PendingError: func(pending uintptr) uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			p, ok := f.pendings[pending]
			if !ok {
				return 0
			}
			return f.cstr(p.errMsg)
		},
		ExecutePending: func(pending uintptr, out *abi.Result) int32 {
			f.mu.Lock()
			defer f.mu.Unlock()
			if _, ok := f.pendings[pending]; !ok {
				return abi.StateError
			}
			id := f.allocID()
			res := &fakeResult{
				streaming:    !f.nonStreamingResult,
				columns:      append([]string(nil), f.resultColumns...),
				rowsAffected: f.resultRowsAffected,
				chunkSizes:   append([]uint64(nil), f.chunkSizes...),
				fetchErrMsg:  f.fetchErrMsg,
				fetchErrType: f.fetchErrType,
			}
			if f.failExecute != "" {
				res.errMsg = f.failExecute
				res.errType = f.failExecuteErrType
				f.results[id] = res
				out.InternalData = id
				return abi.StateError
			}
			f.results[id] = res
			out.InternalData = id
			return abi.StateSuccess
		},
		DestroyPending: func(pending *uintptr) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if pending == nil || *pending == 0 {
				return
			}
			if _, ok := f.pendings[*pending]; !ok {
				f.doubleFree++
			}
			delete(f.pendings, *pending)
			*pending = 0
		},
		DestroyResult: func(result *abi.Result) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if result == nil || result.InternalData == 0 {
				return
			}
			if _, ok := f.results[result.InternalData]; !ok {
				f.doubleFree++
			}
			delete(f.results, result.InternalData)
			*result = abi.Result{}
		},
		ResultError: func(result *abi.Result) uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			r, ok := f.results[result.InternalData]
			if !ok {
				return 0
			}
			return f.cstr(r.errMsg)
		},
		ResultErrorType: func(result *abi.Result) int32 {
			f.mu.Lock()
			defer f.mu.Unlock()
			r, ok := f.results[result.InternalData]
			if !ok {
				return abi.ErrorInvalid
			}
			return r.errType
		},
		ResultIsStreaming: func(result abi.Result) bool {
			f.mu.Lock()
			defer f.mu.Unlock()
			r, ok := f.results[result.InternalData]
			return ok && r.streaming
		},
		RowsChanged: func(result *abi.Result) uint64 {
			f.mu.Lock()
			defer f.mu.Unlock()
			r, ok := f.results[result.InternalData]
			if !ok {
				return 0
			}
			return r.rowsAffected
		},
		ColumnCount: func(result *abi.Result) uint64 {
			f.mu.Lock()
			defer f.mu.Unlock()
			r, ok := f.results[result.InternalData]
			if !ok {
				return 0
			}
			return uint64(len(r.columns))
		},
		ColumnName: func(result *abi.Result, col uint64) uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			r, ok := f.results[result.InternalData]
			if !ok || col >= uint64(len(r.columns)) {
				return 0
			}
			return f.cstr(r.columns[col])
		},
		FetchChunk: func(result abi.Result) uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			r, ok := f.results[result.InternalData]
			if !ok {
				return 0
			}
			if r.chunkIdx < len(r.chunkSizes) {
				sz := r.chunkSizes[r.chunkIdx]
				r.chunkIdx++
				id := f.allocID()
				f.chunks[id] = sz
				return id
			}
			if r.fetchErrMsg != "" || r.fetchErrType != 0 {
				r.errMsg = r.fetchErrMsg
				r.errType = r.fetchErrType
			}
			return 0
		},
		DataChunkGetSize: func(chunk uintptr) uint64 {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.chunks[chunk]
		},
		DestroyDataChunk: func(chunk *uintptr) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if chunk == nil || *chunk == 0 {
				return
			}
			if _, ok := f.chunks[*chunk]; !ok {
				f.doubleFree++
			}
			delete(f.chunks, *chunk)
			*chunk = 0
		},
	}
}

func TestOpenFailurePathsZeroResources(t *testing.T) {
	ctx := context.Background()

	t.Run("create_config_fail", func(t *testing.T) {
		f, lib := newFakeNative(t)
		f.failCreateConfig = true
		f.failCreateConfigAlloc = true
		if _, err := lib.Open(ctx, ":memory:"); err == nil {
			t.Fatal("expected error")
		}
		f.assertZeroLeaks(t)
	})

	t.Run("set_config_fail", func(t *testing.T) {
		f, lib := newFakeNative(t)
		f.failSetConfigAt = 2
		_, err := lib.Open(ctx, ":memory:", Option{"threads", "2"}, Option{"bad", "x"})
		if err == nil {
			t.Fatal("expected error")
		}
		f.assertZeroLeaks(t)
	})

	t.Run("open_ext_fail_frees_error_and_db", func(t *testing.T) {
		f, lib := newFakeNative(t)
		f.failOpenExt = "cannot open read-only memory db"
		f.failOpenExtAllocDB = true
		_, err := lib.Open(ctx, ":memory:")
		var derr *Error
		if !errors.As(err, &derr) || derr.Message != f.failOpenExt {
			t.Fatalf("got err=%v, want copied message %q", err, f.failOpenExt)
		}
		f.assertZeroLeaks(t)
	})
}

func TestConnectPreparePendingExecuteFailuresZeroResources(t *testing.T) {
	ctx := context.Background()

	t.Run("connect_fail", func(t *testing.T) {
		f, lib := newFakeNative(t)
		db, err := lib.Open(ctx, ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		f.failConnect = true
		f.failConnectAlloc = true
		if _, err := db.Connect(ctx); err == nil {
			t.Fatal("expected error")
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		f.assertZeroLeaks(t)
	})

	t.Run("prepare_fail_copies_error_and_destroys_stmt", func(t *testing.T) {
		f, lib := newFakeNative(t)
		db, _ := lib.Open(ctx, ":memory:")
		conn, _ := db.Connect(ctx)
		f.failPrepare = "syntax error near SELECT"
		_, err := conn.Query(ctx, "SELECT")
		var derr *Error
		if !errors.As(err, &derr) || derr.Message != f.failPrepare {
			t.Fatalf("got err=%v, want %q", err, f.failPrepare)
		}
		_ = db.Close()
		f.assertZeroLeaks(t)
	})

	t.Run("pending_fail_releases_pending_and_owned_stmt", func(t *testing.T) {
		f, lib := newFakeNative(t)
		db, _ := lib.Open(ctx, ":memory:")
		conn, _ := db.Connect(ctx)
		f.failPending = "pending failed"
		_, err := conn.Query(ctx, "SELECT 1")
		var derr *Error
		if !errors.As(err, &derr) || derr.Message != f.failPending {
			t.Fatalf("got err=%v, want %q", err, f.failPending)
		}
		_ = db.Close()
		f.assertZeroLeaks(t)
	})

	t.Run("pending_task_loops_through_not_ready_and_no_tasks_then_fails", func(t *testing.T) {
		f, lib := newFakeNative(t)
		db, _ := lib.Open(ctx, ":memory:")
		conn, _ := db.Connect(ctx)
		f.pendingStates = []int32{abi.PendingNoTasks, abi.PendingResultNotReady, abi.PendingError}
		f.pendingErrMsg = "out of memory in task"
		_, err := conn.Query(ctx, "SELECT 1")
		var derr *Error
		if !errors.As(err, &derr) || derr.Message != f.pendingErrMsg {
			t.Fatalf("got err=%v, want %q", err, f.pendingErrMsg)
		}
		_ = db.Close()
		f.assertZeroLeaks(t)
	})

	t.Run("execute_fail_copies_error_type_and_releases", func(t *testing.T) {
		f, lib := newFakeNative(t)
		db, _ := lib.Open(ctx, ":memory:")
		conn, _ := db.Connect(ctx)
		f.failExecute = "oom"
		f.failExecuteErrType = abi.ErrorOutOfMemory
		_, err := conn.Query(ctx, "SELECT 1")
		var derr *Error
		if !errors.As(err, &derr) || derr.Kind != ErrorKindOutOfMemory || derr.Message != "oom" {
			t.Fatalf("got err=%v, want OOM", err)
		}
		_ = db.Close()
		f.assertZeroLeaks(t)
	})

	t.Run("non_streaming_query_rejected_and_conn_remains_usable", func(t *testing.T) {
		f, lib := newFakeNative(t)
		db, _ := lib.Open(ctx, ":memory:")
		conn, _ := db.Connect(ctx)
		f.nonStreamingResult = true
		if _, err := conn.Query(ctx, "SELECT 1"); !errors.Is(err, ErrNotStreaming) {
			t.Fatalf("got err=%v, want ErrNotStreaming", err)
		}
		// Connection must not remain busy.
		f.nonStreamingResult = false
		rows, err := conn.Query(ctx, "SELECT 1")
		if err != nil {
			t.Fatalf("subsequent query failed: %v", err)
		}
		_ = rows.Close()
		_ = db.Close()
		f.assertZeroLeaks(t)
	})
}

func TestFetchErrorNotEOFAndEmptyChunksReleased(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	db, _ := lib.Open(ctx, ":memory:")
	conn, _ := db.Connect(ctx)
	f.resultColumns = []string{"a", "a"}
	f.chunkSizes = []uint64{0, 10, 0, 5}
	f.fetchErrMsg = "Interrupted!"
	f.fetchErrType = abi.ErrorInterrupt

	rows, err := conn.Query(ctx, "SELECT 1 AS a, 2 AS a")
	if err != nil {
		t.Fatal(err)
	}
	var total int
	for rows.Next() {
		total++
	}
	if total != 15 {
		t.Fatalf("got %d rows across chunks [0,10,0,5], want 15", total)
	}
	var derr *Error
	if !errors.As(rows.Err(), &derr) || derr.Kind != ErrorKindInterrupt || derr.Message != "Interrupted!" {
		t.Fatalf("got rows.Err()=%v, want Interrupt error", rows.Err())
	}
	// Duplicate columns preserved and survive Close.
	_ = rows.Close()
	cols := rows.Columns()
	if len(cols) != 2 || cols[0].Name != "a" || cols[1].Name != "a" {
		t.Fatalf("columns after close = %+v", cols)
	}
	if _, err := rows.DecodeRow(nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("DecodeRow after close = %v, want ErrClosed", err)
	}
	_ = db.Close()
	f.assertZeroLeaks(t)
}

func TestCloseIdempotentCascadeAndBusyLease(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	db, _ := lib.Open(ctx, ":memory:")
	conn, _ := db.Connect(ctx)
	f.nParams = 2
	stmt, err := conn.Prepare(ctx, "SELECT ?, ?")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := stmt.ParamCount(); err != nil || n != 2 {
		t.Fatalf("ParamCount = %d, %v", n, err)
	}
	f.chunkSizes = []uint64{3}
	rows, err := stmt.Query(ctx, Int64Value(1), NullValue())
	if err != nil {
		t.Fatal(err)
	}
	// Exclusive lease blocks every other op on conn and stmt.
	if _, err := conn.Prepare(ctx, "SELECT 1"); !errors.Is(err, ErrBusy) {
		t.Fatalf("Prepare while busy: %v", err)
	}
	if _, err := conn.Query(ctx, "SELECT 1"); !errors.Is(err, ErrBusy) {
		t.Fatalf("Query while busy: %v", err)
	}
	if _, err := conn.Exec(ctx, "SET threads=1"); !errors.Is(err, ErrBusy) {
		t.Fatalf("Exec while busy: %v", err)
	}
	if _, err := stmt.Query(ctx, Int64Value(1), NullValue()); !errors.Is(err, ErrBusy) {
		t.Fatalf("Stmt.Query while busy: %v", err)
	}
	if _, err := stmt.Exec(ctx, Int64Value(1), NullValue()); !errors.Is(err, ErrBusy) {
		t.Fatalf("Stmt.Exec while busy: %v", err)
	}
	if err := stmt.Close(); !errors.Is(err, ErrBusy) {
		t.Fatalf("Stmt.Close while busy: %v", err)
	}

	if !rows.Next() {
		t.Fatal("expected first row")
	}
	// Closing Rows releases lease and chunk; calling Close twice is idempotent.
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("Next after Close must be false")
	}
	if !errors.Is(rows.Err(), ErrClosed) {
		t.Fatalf("Err after early Close + Next = %v, want ErrClosed", rows.Err())
	}

	// Now Stmt and Conn are usable again.
	f.nParams = 0
	f.nonStreamingResult = true
	f.resultRowsAffected = 7
	execRes, err := conn.Exec(ctx, "INSERT INTO t VALUES (1)")
	if err != nil || execRes.RowsAffected != 7 {
		t.Fatalf("Exec = %+v, %v", execRes, err)
	}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.Query(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("Stmt.Query after Close = %v", err)
	}

	// Open another Rows and cascade Close from Database.
	f.nonStreamingResult = false
	rows2, err := conn.Query(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	_ = rows2.Next()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Connect(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("Connect after db.Close = %v", err)
	}
	if _, err := conn.Query(ctx, "SELECT 1"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Query after db.Close = %v", err)
	}
	f.assertZeroLeaks(t)
}

func TestInputValidationGuardsZeroNativeCalls(t *testing.T) {
	f, lib := newFakeNative(t)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := (*Library)(nil).Open(context.Background(), ":memory:"); !errors.Is(err, ErrClosed) {
		t.Fatalf("nil Library.Open = %v", err)
	}
	if _, err := lib.Open(nil, ":memory:"); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("nil ctx Open = %v", err)
	}
	if _, err := lib.Open(canceled, ":memory:"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Open = %v", err)
	}
	if _, err := lib.Open(context.Background(), "bad\x00path"); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("NUL path = %v", err)
	}
	if _, err := lib.Open(context.Background(), ":memory:", Option{"a\x00b", "1"}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("NUL opt name = %v", err)
	}
	if _, err := lib.Open(context.Background(), ":memory:", Option{"a", "1\x002"}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("NUL opt val = %v", err)
	}
	db, err := lib.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Prepare(context.Background(), "SELECT '\x00'"); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("NUL sql = %v", err)
	}
	if _, err := conn.Query(canceled, "SELECT 1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Query = %v", err)
	}
	_ = db.Close()
	f.assertZeroLeaks(t)
}

func TestConcurrentCloseRaceSafe(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	db, _ := lib.Open(ctx, ":memory:")
	conn, _ := db.Connect(ctx)
	f.chunkSizes = []uint64{1, 1, 1}
	rows, _ := conn.Query(ctx, "SELECT 1")

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(3)
		go func() { defer wg.Done(); _ = rows.Close() }()
		go func() { defer wg.Done(); _ = conn.Close() }()
		go func() { defer wg.Done(); _ = db.Close() }()
	}
	wg.Wait()
	f.assertZeroLeaks(t)
}
