package abi

import (
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/ebitengine/purego"
)

type scalarProbeAPI struct {
	create       func() uintptr
	destroy      func(fn *uintptr)
	setName      func(fn uintptr, name *byte)
	addParam     func(fn uintptr, lt uintptr)
	setReturn    func(fn uintptr, lt uintptr)
	setExtraInfo func(fn uintptr, extra uintptr, destroyCB uintptr)
	setFunction  func(fn uintptr, cb uintptr)
	register     func(conn uintptr, fn uintptr) int32
	getExtraInfo func(info uintptr) uintptr
	setError     func(info uintptr, msg *byte)
}

type scalarProbeState struct {
	api            *API
	probe          *scalarProbeAPI
	calls          atomic.Int64
	rows           atomic.Int64
	inFlight       atomic.Int64
	peakConcurrent atomic.Int64
	destroyed      atomic.Int64
	delayPerChunk  time.Duration
}

var (
	probeCBOnce   sync.Once
	probeExecCB   uintptr
	probeDeleteCB uintptr
	probeSeq      atomic.Uintptr
	probeHandlers sync.Map // map[uintptr]*scalarProbeState
	probeAPIPtr   atomic.Pointer[scalarProbeAPI]
)

func ensureProbeCallbacks() {
	probeCBOnce.Do(func() {
		probeExecCB = purego.NewCallback(func(_ purego.CDecl, info, input, output uintptr) uintptr {
			// Never allow a Go panic to unwind across C++ frames.
			defer func() {
				_ = recover()
			}()
			if info == 0 || input == 0 || output == 0 {
				return 0
			}
			p := probeAPIPtr.Load()
			if p == nil {
				return 0
			}
			id := p.getExtraInfo(info)
			loaded, ok := probeHandlers.Load(id)
			if !ok {
				return 0
			}
			st := loaded.(*scalarProbeState)
			cur := st.inFlight.Add(1)
			defer st.inFlight.Add(-1)
			for {
				prev := st.peakConcurrent.Load()
				if cur <= prev || st.peakConcurrent.CompareAndSwap(prev, cur) {
					break
				}
			}

			if st.delayPerChunk > 0 {
				time.Sleep(st.delayPerChunk)
			}

			n := st.api.DataChunkGetSize(input)
			st.calls.Add(1)
			st.rows.Add(int64(n))

			inVec := st.api.DataChunkGetVector(input, 0)
			inData := st.api.VectorGetData(inVec)
			inValid := st.api.VectorGetValidity(inVec)
			outData := st.api.VectorGetData(output)

			inSlice := unsafe.Slice((*int64)(Pointer(inData)), int(n))
			outSlice := unsafe.Slice((*int64)(Pointer(outData)), int(n))
			for i := uint64(0); i < n; i++ {
				if inValid != 0 {
					word := *(*uint64)(Pointer(inValid + uintptr(i/64)*8))
					if word&(uint64(1)<<(i%64)) == 0 {
						st.api.VectorEnsureValidityWritable(output)
						outValid := st.api.VectorGetValidity(output)
						if outValid != 0 {
							ptr := (*uint64)(Pointer(outValid + uintptr(i/64)*8))
							*ptr &^= uint64(1) << (i % 64)
						}
						continue
					}
				}
				v := inSlice[i]
				if v < 0 {
					msg, _ := CString("poc_add_one: negative input")
					st.probe.setError(info, &msg[0])
					runtime.KeepAlive(msg)
					return 0
				}
				outSlice[i] = v + 1
			}
			return 0
		})
		probeDeleteCB = purego.NewCallback(func(_ purego.CDecl, data uintptr) uintptr {
			defer func() {
				_ = recover()
			}()
			if data == 0 {
				return 0
			}
			if v, ok := probeHandlers.LoadAndDelete(data); ok {
				v.(*scalarProbeState).destroyed.Add(1)
			}
			return 0
		})
	})
}

func bindScalarProbeAPI(t *testing.T, libPath string) *scalarProbeAPI {
	t.Helper()
	lookup, err := openLibrary(libPath)
	if err != nil {
		t.Fatalf("openLibrary: %v", err)
	}
	p := &scalarProbeAPI{}
	syms := []struct {
		field any
		name  string
	}{
		{&p.create, "duckdb_create_scalar_function"},
		{&p.destroy, "duckdb_destroy_scalar_function"},
		{&p.setName, "duckdb_scalar_function_set_name"},
		{&p.addParam, "duckdb_scalar_function_add_parameter"},
		{&p.setReturn, "duckdb_scalar_function_set_return_type"},
		{&p.setExtraInfo, "duckdb_scalar_function_set_extra_info"},
		{&p.setFunction, "duckdb_scalar_function_set_function"},
		{&p.register, "duckdb_register_scalar_function"},
		{&p.getExtraInfo, "duckdb_scalar_function_get_extra_info"},
		{&p.setError, "duckdb_scalar_function_set_error"},
	}
	for _, s := range syms {
		addr, err := lookup(s.name)
		if err != nil || addr == 0 {
			t.Fatalf("lookup %s failed: %v", s.name, err)
		}
		purego.RegisterFunc(s.field, addr)
	}
	return p
}

func probeExecSQL(t *testing.T, api *API, conn uintptr, sql string) (int64, string) {
	t.Helper()
	csql, err := CString(sql)
	if err != nil {
		t.Fatal(err)
	}
	var stmt uintptr
	if api.Prepare(conn, &csql[0], &stmt) != StateSuccess {
		msg := GoString(api.PrepareError(stmt))
		if stmt != 0 {
			api.DestroyPrepare(&stmt)
		}
		return 0, msg
	}
	runtime.KeepAlive(csql)
	defer api.DestroyPrepare(&stmt)

	var pending uintptr
	if api.PendingPreparedStreaming(stmt, &pending) != StateSuccess {
		msg := GoString(api.PendingError(pending))
		if pending != 0 {
			api.DestroyPending(&pending)
		}
		return 0, msg
	}
	for {
		st := api.PendingExecuteTask(pending)
		if st == PendingResultReady {
			break
		}
		if st == PendingNoTasks {
			runtime.Gosched()
			continue
		}
		if st != PendingResultNotReady {
			msg := GoString(api.PendingError(pending))
			api.DestroyPending(&pending)
			return 0, msg
		}
	}
	var res Result
	state := api.ExecutePending(pending, &res)
	api.DestroyPending(&pending)
	defer api.DestroyResult(&res)
	if state != StateSuccess {
		return 0, GoString(api.ResultError(&res))
	}
	var out int64
	for {
		chunk := api.FetchChunk(res)
		if chunk == 0 {
			if msg := GoString(api.ResultError(&res)); msg != "" {
				return 0, msg
			}
			break
		}
		sz := api.DataChunkGetSize(chunk)
		if sz > 0 && api.ColumnCount(&res) > 0 {
			vec := api.DataChunkGetVector(chunk, 0)
			data := api.VectorGetData(vec)
			out = *(*int64)(Pointer(data))
		}
		api.DestroyDataChunk(&chunk)
	}
	return out, ""
}

// TestNativePuregoMultithreadedScalarCallbackProbe empirically verifies whether
// purego.NewCallback safely handles concurrent callbacks from DuckDB's C++
// worker threads under CGO_ENABLED=0 and -race across all accepted platforms.
func TestNativePuregoMultithreadedScalarCallbackProbe(t *testing.T) {
	libPath := os.Getenv("DUCKDB_LIB_PATH")
	shaHex := os.Getenv("DUCKDB_LIB_SHA256")
	if libPath == "" || shaHex == "" {
		t.Skip("DUCKDB_LIB_PATH and DUCKDB_LIB_SHA256 are required")
	}
	api, err := Load(libPath, shaHex)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	probe := bindScalarProbeAPI(t, libPath)
	probeAPIPtr.Store(probe)
	ensureProbeCallbacks()

	runCase := func(name string, threads string, rows int64, delay time.Duration) {
		t.Run(name, func(t *testing.T) {
			var cfg uintptr
			if api.CreateConfig(&cfg) != StateSuccess {
				t.Fatal("CreateConfig failed")
			}
			defer api.DestroyConfig(&cfg)
			kThreads, _ := CString("threads")
			vThreads, _ := CString(threads)
			if api.SetConfig(cfg, &kThreads[0], &vThreads[0]) != StateSuccess {
				t.Fatal("SetConfig(threads) failed")
			}
			runtime.KeepAlive(kThreads)
			runtime.KeepAlive(vThreads)

			mem, _ := CString(":memory:")
			var db, errPtr uintptr
			st := api.OpenExt(&mem[0], &db, cfg, &errPtr)
			runtime.KeepAlive(mem)
			if errPtr != 0 {
				msg := GoString(errPtr)
				api.Free(errPtr)
				if st != StateSuccess {
					t.Fatalf("OpenExt: %s", msg)
				}
			}
			if st != StateSuccess {
				t.Fatal("OpenExt failed")
			}

			var conn uintptr
			if api.Connect(db, &conn) != StateSuccess {
				api.Close(&db)
				t.Fatal("Connect failed")
			}

			state := &scalarProbeState{
				api:           api,
				probe:         probe,
				delayPerChunk: delay,
			}
			id := probeSeq.Add(1)
			probeHandlers.Store(id, state)

			fn := probe.create()
			fnName, _ := CString("poc_add_one")
			probe.setName(fn, &fnName[0])
			runtime.KeepAlive(fnName)
			lt := api.CreateLogicalType(TypeBigint)
			probe.addParam(fn, lt)
			probe.setReturn(fn, lt)
			api.DestroyLogicalType(&lt)
			probe.setExtraInfo(fn, id, probeDeleteCB)
			probe.setFunction(fn, probeExecCB)

			regState := probe.register(conn, fn)
			probe.destroy(&fn)
			if regState != StateSuccess {
				api.Disconnect(&conn)
				api.Close(&db)
				t.Fatal("register scalar function failed")
			}

			// Materialize a table so DuckDB's morsel-driven parallelism spreads
			// scan chunks across worker threads when threads > 1.
			if _, errMsg := probeExecSQL(t, api, conn, "CREATE TABLE poc_nums AS SELECT i::BIGINT AS i FROM range("+itoa64(rows)+") t(i)"); errMsg != "" {
				api.Disconnect(&conn)
				api.Close(&db)
				t.Fatalf("CREATE TABLE: %s", errMsg)
			}

			got, errMsg := probeExecSQL(t, api, conn, "SELECT sum(poc_add_one(i))::BIGINT FROM poc_nums")
			if errMsg != "" {
				api.Disconnect(&conn)
				api.Close(&db)
				t.Fatalf("SELECT sum(poc_add_one(i)): %s", errMsg)
			}
			// sum_{i=0}^{rows-1} (i + 1) = rows * (rows + 1) / 2
			want := rows * (rows + 1) / 2
			if got != want {
				api.Disconnect(&conn)
				api.Close(&db)
				t.Fatalf("sum = %d, want %d", got, want)
			}

			// Verify error propagation across the C++/Go callback boundary.
			if _, errMsg := probeExecSQL(t, api, conn, "SELECT poc_add_one(-5::BIGINT)"); errMsg == "" {
				api.Disconnect(&conn)
				api.Close(&db)
				t.Fatal("expected error for negative input, got success")
			}

			t.Logf("%s: calls=%d rows=%d peakConcurrent=%d numCPU=%d goos=%s goarch=%s",
				name, state.calls.Load(), state.rows.Load(), state.peakConcurrent.Load(), runtime.NumCPU(), runtime.GOOS, runtime.GOARCH)

			api.Disconnect(&conn)
			api.Close(&db)

			if state.destroyed.Load() != 1 {
				t.Fatalf("expected delete callback to run once on db close, got %d", state.destroyed.Load())
			}
		})
	}

	runCase("single_thread", "1", 20000, 0)
	runCase("multi_thread_4", "4", 400000, 200*time.Microsecond)
}

func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
