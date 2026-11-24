package duckdb

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

func TestValueCompactLayoutSize(t *testing.T) {
	if sz := unsafe.Sizeof(Value{}); sz > 48 {
		t.Fatalf("sizeof(Value) = %d bytes, want <= 48", sz)
	}
}

func TestP0ScalarTypesValidationAndAccessors(t *testing.T) {
	// UUID parsing, canonical lowercase formatting, and Value round-trip.
	u, err := ParseUUID("01234567-89AB-CDEF-0123-456789abcdef")
	if err != nil {
		t.Fatalf("ParseUUID: %v", err)
	}
	if got := u.String(); got != "01234567-89ab-cdef-0123-456789abcdef" {
		t.Fatalf("UUID.String() = %q", got)
	}
	uv := UUIDValue(u)
	gotU, err := uv.UUID()
	if err != nil || gotU != u {
		t.Fatalf("UUID round-trip: %v %v", gotU, err)
	}
	if !uv.Type().Equal(ScalarType(TypeUUID)) {
		t.Fatalf("UUID Type = %s", uv.Type())
	}
	for _, bad := range []string{"", "not-a-uuid", "0123456789abcdef0123456789abcdef", "01234567-89ab-cdef-0123-456789abcdeg"} {
		if _, err := ParseUUID(bad); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("ParseUUID(%q) = %v, want ErrInvalidInput", bad, err)
		}
	}

	// TimeTZ bounds and negative offset round-trip.
	tz, err := TimeTZValue(TimeTZ{Micros: 45_296_789012, OffsetSeconds: -57599})
	if err != nil {
		t.Fatalf("TimeTZValue: %v", err)
	}
	gotTZ, err := tz.TimeTZ()
	if err != nil || gotTZ != (TimeTZ{Micros: 45_296_789012, OffsetSeconds: -57599}) {
		t.Fatalf("TimeTZ round-trip: %+v %v", gotTZ, err)
	}
	for _, bad := range []TimeTZ{
		{Micros: -1, OffsetSeconds: 0},
		{Micros: timeTZMaxMicros + 1, OffsetSeconds: 0},
		{Micros: 0, OffsetSeconds: -timeTZMaxOffset - 1},
		{Micros: 0, OffsetSeconds: timeTZMaxOffset + 1},
	} {
		if _, err := TimeTZValue(bad); !errors.Is(err, ErrOutOfRange) {
			t.Fatalf("TimeTZValue(%+v) = %v, want ErrOutOfRange", bad, err)
		}
	}

	// EnumValue validation and ownership.
	et := EnumType("sad", "ok", "happy")
	ev, err := EnumValue(et, 2)
	if err != nil {
		t.Fatalf("EnumValue: %v", err)
	}
	et.EnumValues[2] = "mutated"
	gotE, err := ev.Enum()
	if err != nil || gotE != (Enum{Index: 2, Label: "happy"}) {
		t.Fatalf("Enum round-trip: %+v %v", gotE, err)
	}
	if _, err := EnumValue(EnumType(), 0); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty enum = %v, want ErrInvalidInput", err)
	}
	if _, err := EnumValue(EnumType("a"), 1); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("oob enum = %v, want ErrOutOfRange", err)
	}

	// JSON logical type alias and Value helpers.
	jv := JSONValue(`{"k":1}`)
	if !jv.IsJSON() || !jv.Type().IsJSON() || jv.Type().String() != "JSON" {
		t.Fatalf("JSONValue metadata: isJSON=%v type=%s", jv.IsJSON(), jv.Type())
	}
	if jv.Type().Equal(ScalarType(TypeVarchar)) {
		t.Fatal("JSON logical type must not equal plain VARCHAR")
	}
	if s, err := jv.Varchar(); err != nil || s != `{"k":1}` {
		t.Fatalf("JSONValue.Varchar() = %q %v", s, err)
	}
	if VarcharValue(`{"k":1}`).IsJSON() {
		t.Fatal("plain VARCHAR must not report IsJSON()")
	}

	// SQLNULL vector decode does not require a native data or validity pointer.
	sqlNullPlan := columnPlan{lt: ScalarType(TypeSQLNull)}
	nv, err := decodeVectorValue(&abi.API{}, &sqlNullPlan, 0, 0, DefaultDecodeLimits())
	if err != nil || !nv.IsNull() || nv.Kind() != TypeSQLNull {
		t.Fatalf("SQLNULL decode = (%+v, %v)", nv, err)
	}
}

func TestCompositeValueConstructorsValidationAndIsolation(t *testing.T) {
	// LIST: empty non-null, element mutation isolation, type mismatch rejected.
	emptyL, err := NewListValue(ScalarType(TypeInteger), nil)
	if err != nil || emptyL.IsNull() {
		t.Fatalf("empty list = (%+v, %v)", emptyL, err)
	}
	if lv, err := emptyL.List(); err != nil || len(lv.Elements) != 0 {
		t.Fatalf("empty list accessor = %+v %v", lv, err)
	}
	src := []Value{Int32Value(1), NullValue(), Int32Value(3)}
	lv, err := NewListValue(ScalarType(TypeInteger), src)
	if err != nil {
		t.Fatalf("NewListValue: %v", err)
	}
	src[0] = Int32Value(99)
	gotL, err := lv.List()
	if err != nil {
		t.Fatal(err)
	}
	if v0, _ := gotL.Elements[0].Int32(); v0 != 1 {
		t.Fatalf("list mutated through source slice: got %d", v0)
	}
	if _, err := NewListValue(ScalarType(TypeInteger), []Value{Int64Value(1)}); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("list type mismatch = %v, want ErrTypeMismatch", err)
	}

	// ARRAY: rejects empty, checks element types.
	if _, err := NewArrayValue(ScalarType(TypeInteger), nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty array = %v, want ErrInvalidInput", err)
	}
	av, err := NewArrayValue(ScalarType(TypeInteger), []Value{Int32Value(10), NullValue()})
	if err != nil {
		t.Fatalf("NewArrayValue: %v", err)
	}
	if gotA, err := av.Array(); err != nil || gotA.Size != 2 || len(gotA.Elements) != 2 {
		t.Fatalf("array = %+v %v", gotA, err)
	}

	// STRUCT: infers field type from non-null value, rejects untyped NULL without Type.
	if _, err := NewStructValue([]StructFieldValue{{Name: "x", Value: NullValue()}}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("untyped null struct field = %v, want ErrInvalidInput", err)
	}
	sv, err := NewStructValue([]StructFieldValue{
		{Name: "a", Value: Int64Value(7)},
		{Name: "b", Type: ScalarType(TypeVarchar), Value: NullValue()},
	})
	if err != nil {
		t.Fatalf("NewStructValue: %v", err)
	}
	wantST := StructType(
		StructFieldType{Name: "a", Type: ScalarType(TypeBigint)},
		StructFieldType{Name: "b", Type: ScalarType(TypeVarchar)},
	)
	if !sv.Type().Equal(wantST) {
		t.Fatalf("struct type = %s, want %s", sv.Type(), wantST)
	}

	// MAP: rejects NULL keys and mismatched key/value types.
	if _, err := NewMapValue(ScalarType(TypeVarchar), ScalarType(TypeInteger), []MapEntry{{Key: NullValue(), Value: Int32Value(1)}}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("null map key = %v, want ErrInvalidInput", err)
	}
	mv, err := NewMapValue(ScalarType(TypeVarchar), ScalarType(TypeInteger), []MapEntry{
		{Key: VarcharValue("k1"), Value: Int32Value(1)},
		{Key: VarcharValue("k2"), Value: NullValue()},
	})
	if err != nil {
		t.Fatalf("NewMapValue: %v", err)
	}
	if gotM, err := mv.Map(); err != nil || len(gotM.Entries) != 2 {
		t.Fatalf("map = %+v %v", gotM, err)
	}
}

func TestP0ZeroLeaksOnBindDecodeAndNamedParamErrors(t *testing.T) {
	ctx := context.Background()

	// Enum dictionary exceeding MaxValueBytes frees every dictionary string and logical type.
	t.Run("enum_dict_limit_zero_leaks", func(t *testing.T) {
		f, lib := newFakeNative(t)
		api := lib.api
		api.ColumnLogicalType = func(r *abi.Result, col uint64) uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			id := f.allocID()
			f.ltypes[id] = abi.TypeEnum
			return id
		}
		api.LogicalTypeGetAlias = func(lt uintptr) uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.ownedCstr("custom_enum")
		}
		api.EnumInternalType = func(lt uintptr) int32 { return abi.TypeUTinyint }
		api.EnumDictionarySize = func(lt uintptr) uint32 { return 3 }
		api.EnumDictionaryValue = func(lt uintptr, idx uint64) uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			if idx == 1 {
				return f.ownedCstr(strings.Repeat("x", 32))
			}
			return f.ownedCstr("ok")
		}

		db, _ := lib.Open(ctx, ":memory:")
		conn, _ := db.Connect(ctx)
		_ = conn.SetDecodeLimits(DecodeLimits{MaxValueBytes: 8})
		f.resultColumns = []string{"e"}
		if _, err := conn.Query(ctx, "SELECT e"); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("got %v, want ErrLimitExceeded", err)
		}
		_ = db.Close()
		f.assertZeroLeaks(t)
	})

	// Composite bind failure mid-way destroys every allocated logical type and duckdb_value.
	t.Run("composite_bind_partial_failure_zero_leaks", func(t *testing.T) {
		f, lib := newFakeNative(t)
		api := lib.api

		allocLT := func(id int32) uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			h := f.allocID()
			f.ltypes[h] = id
			return h
		}
		allocVal := func() uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			h := f.allocID()
			f.values[h] = struct{}{}
			return h
		}
		api.CreateLogicalType = func(typeID int32) uintptr { return allocLT(typeID) }
		api.CreateListType = func(child uintptr) uintptr { return allocLT(abi.TypeList) }
		api.CreateStructType = func(mtypes, mnames *uintptr, count uint64) uintptr { return allocLT(abi.TypeStruct) }
		api.CreateMapType = func(k, v uintptr) uintptr { return allocLT(abi.TypeMap) }
		var int32Calls int
		api.CreateInt32 = func(v int32) uintptr {
			int32Calls++
			if int32Calls == 2 {
				return 0 // fail second element
			}
			return allocVal()
		}
		api.CreateVarcharLength = func(text *byte, length uint64) uintptr { return allocVal() }
		api.CreateNullValue = func() uintptr { return allocVal() }
		api.CreateListValue = func(elemType uintptr, vals *uintptr, count uint64) uintptr { return allocVal() }
		api.CreateStructValue = func(structType uintptr, vals *uintptr) uintptr { return 0 } // fail struct value creation
		api.CreateMapValue = func(mapType uintptr, keys, vals *uintptr, count uint64) uintptr { return allocVal() }
		api.BindValue = func(stmt uintptr, idx uint64, val uintptr) int32 { return abi.StateError }
		api.DestroyValue = func(val *uintptr) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if val == nil || *val == 0 {
				return
			}
			if _, ok := f.values[*val]; !ok {
				f.doubleFree++
			}
			delete(f.values, *val)
			*val = 0
		}

		db, _ := lib.Open(ctx, ":memory:")
		conn, _ := db.Connect(ctx)
		f.nParams = 1

		// Failure mid-way through LIST elements.
		listVal, _ := NewListValue(ScalarType(TypeInteger), []Value{Int32Value(1), Int32Value(2)})
		if _, err := conn.Query(ctx, "SELECT ?", listVal); err == nil {
			t.Fatal("expected list element creation failure")
		}

		// Failure creating STRUCT value after building all field logical types and child values.
		int32Calls = 10
		structVal, _ := NewStructValue([]StructFieldValue{
			{Name: "a", Value: Int32Value(1)},
			{Name: "b", Value: VarcharValue("hi")},
		})
		if _, err := conn.Query(ctx, "SELECT ?", structVal); err == nil {
			t.Fatal("expected struct value creation failure")
		}

		// Failure in BindValue for MAP destroys map value, all key/val children, and map logical types.
		mapVal, _ := NewMapValue(ScalarType(TypeVarchar), ScalarType(TypeInteger), []MapEntry{
			{Key: VarcharValue("k"), Value: Int32Value(1)},
		})
		if _, err := conn.Query(ctx, "SELECT ?", mapVal); err == nil {
			t.Fatal("expected BindValue failure")
		}

		_ = db.Close()
		f.assertZeroLeaks(t)
	})

	// Named parameter validation and ParamNames zero-leak check.
	t.Run("named_params_validation_and_zero_leaks", func(t *testing.T) {
		f, lib := newFakeNative(t)
		api := lib.api
		paramIdx := map[string]uint64{"a": 1, "b": 2}
		api.ParameterName = func(stmt uintptr, idx uint64) uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			switch idx {
			case 1:
				return f.ownedCstr("a")
			case 2:
				return f.ownedCstr("b")
			default:
				return 0
			}
		}
		api.BindParameterIndex = func(stmt uintptr, out *uint64, name *byte) int32 {
			k := abi.GoString(uintptr(unsafe.Pointer(name)))
			if idx, ok := paramIdx[k]; ok {
				*out = idx
				return abi.StateSuccess
			}
			return abi.StateError
		}

		db, _ := lib.Open(ctx, ":memory:")
		conn, _ := db.Connect(ctx)
		f.nParams = 2
		stmt, err := conn.Prepare(ctx, "SELECT $a, $b")
		if err != nil {
			t.Fatal(err)
		}
		names, err := stmt.ParamNames()
		if err != nil || len(names) != 2 || names[0] != "a" || names[1] != "b" {
			t.Fatalf("ParamNames = %v %v", names, err)
		}
		if _, err := stmt.ExecNamed(ctx, Named("", Int64Value(1))); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("empty name = %v, want ErrInvalidInput", err)
		}
		if _, err := stmt.ExecNamed(ctx, Named("a\x00", Int64Value(1))); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("nul name = %v, want ErrInvalidInput", err)
		}
		if _, err := stmt.ExecNamed(ctx, Named("a", Int64Value(1)), Named("a", Int64Value(2))); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("duplicate name = %v, want ErrInvalidInput", err)
		}
		if _, err := stmt.ExecNamed(ctx, Named("missing", Int64Value(1))); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("unknown name = %v, want ErrInvalidInput", err)
		}
		if _, err := stmt.ExecNamed(ctx, Named("a", Int64Value(1))); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("partial named bind = %v, want ErrInvalidInput", err)
		}
		if _, err := stmt.ExecNamed(ctx, Named("b", Int64Value(2)), Named("a", Int64Value(1))); err != nil {
			t.Fatalf("valid named exec failed: %v", err)
		}
		_ = stmt.Close()
		_ = db.Close()
		f.assertZeroLeaks(t)
	})
}

func TestObservabilityMockProgressJoinAndProfilingZeroLeaks(t *testing.T) {
	ctx := context.Background()

	// QueryProgress running concurrently with Conn.Close must complete before Disconnect.
	t.Run("query_progress_joins_before_disconnect", func(t *testing.T) {
		f, lib := newFakeNative(t)
		api := lib.api

		entered := make(chan struct{})
		release := make(chan struct{})
		var disconnectedWhileInProgress bool
		var inProgressMu sync.Mutex
		var inProgress bool

		api.QueryProgress = func(conn uintptr) abi.QueryProgress {
			inProgressMu.Lock()
			inProgress = true
			inProgressMu.Unlock()
			close(entered)
			<-release
			inProgressMu.Lock()
			inProgress = false
			inProgressMu.Unlock()
			return abi.QueryProgress{Percentage: 42.5, RowsProcessed: 100, TotalRowsToProcess: 235}
		}
		origDisconnect := api.Disconnect
		api.Disconnect = func(conn *uintptr) {
			inProgressMu.Lock()
			if inProgress {
				disconnectedWhileInProgress = true
			}
			inProgressMu.Unlock()
			origDisconnect(conn)
		}

		db, _ := lib.Open(ctx, ":memory:")
		conn, _ := db.Connect(ctx)

		progDone := make(chan QueryProgress, 1)
		progErr := make(chan error, 1)
		go func() {
			p, err := conn.QueryProgress()
			progDone <- p
			progErr <- err
		}()
		<-entered

		closeDone := make(chan error, 1)
		go func() {
			closeDone <- conn.Close()
		}()
		close(release)

		if err := <-progErr; err != nil {
			t.Fatalf("QueryProgress err: %v", err)
		}
		p := <-progDone
		if p.Percentage != 42.5 || p.RowsProcessed != 100 || p.TotalRowsToProcess != 235 {
			t.Fatalf("unexpected progress: %+v", p)
		}
		if err := <-closeDone; err != nil {
			t.Fatalf("Close err: %v", err)
		}
		if disconnectedWhileInProgress {
			t.Fatal("Disconnect ran while QueryProgress was still executing")
		}
		if _, err := conn.QueryProgress(); !errors.Is(err, ErrClosed) {
			t.Fatalf("post-close QueryProgress = %v, want ErrClosed", err)
		}
		_ = db.Close()
		f.assertZeroLeaks(t)
	})

	// ProfilingInfo: disabled -> nil, busy -> ErrBusy, nested tree -> zero leaks.
	t.Run("profiling_info_snapshot_and_zero_leaks", func(t *testing.T) {
		f, lib := newFakeNative(t)
		api := lib.api

		type fakeNode struct {
			metrics  [][2]string
			children []uintptr
		}
		nodes := map[uintptr]*fakeNode{
			100: {metrics: [][2]string{{"query_name", "SELECT 1"}, {"latency", "0.001"}}, children: []uintptr{101}},
			101: {metrics: [][2]string{{"operator_type", "PROJECTION"}, {"operator_cardinality", "1"}}},
		}
		valStr := map[uintptr]string{}
		valMap := map[uintptr][][2]string{}
		var rootNode uintptr

		api.GetProfilingInfo = func(conn uintptr) uintptr { return rootNode }
		api.ProfilingInfoGetMetrics = func(info uintptr) uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			id := f.allocID()
			f.values[id] = struct{}{}
			valMap[id] = nodes[info].metrics
			return id
		}
		api.ProfilingInfoGetChildCount = func(info uintptr) uint64 { return uint64(len(nodes[info].children)) }
		api.ProfilingInfoGetChild = func(info uintptr, idx uint64) uintptr { return nodes[info].children[idx] }
		api.GetMapSize = func(val uintptr) uint64 { return uint64(len(valMap[val])) }
		api.GetMapKey = func(val uintptr, idx uint64) uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			id := f.allocID()
			f.values[id] = struct{}{}
			valStr[id] = valMap[val][idx][0]
			return id
		}
		api.GetMapValue = func(val uintptr, idx uint64) uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			id := f.allocID()
			f.values[id] = struct{}{}
			valStr[id] = valMap[val][idx][1]
			return id
		}
		api.GetVarchar = func(val uintptr) uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.ownedCstr(valStr[val])
		}
		api.DestroyValue = func(val *uintptr) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if val == nil || *val == 0 {
				return
			}
			if _, ok := f.values[*val]; !ok {
				f.doubleFree++
			}
			delete(f.values, *val)
			delete(valStr, *val)
			delete(valMap, *val)
			*val = 0
		}

		db, _ := lib.Open(ctx, ":memory:")
		conn, _ := db.Connect(ctx)

		// Profiling disabled -> (nil, nil).
		if info, err := conn.ProfilingInfo(); err != nil || info != nil {
			t.Fatalf("disabled ProfilingInfo = (%+v, %v), want (nil, nil)", info, err)
		}

		// Busy while Rows is open -> ErrBusy.
		f.resultColumns = []string{"x"}
		rows, err := conn.Query(ctx, "SELECT 1")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ProfilingInfo(); !errors.Is(err, ErrBusy) {
			t.Fatalf("busy ProfilingInfo = %v, want ErrBusy", err)
		}
		_ = rows.Close()

		// Enabled -> full owned tree snapshot.
		rootNode = 100
		info, err := conn.ProfilingInfo()
		if err != nil || info == nil {
			t.Fatalf("ProfilingInfo = (%+v, %v)", info, err)
		}
		if info.Metrics["query_name"] != "SELECT 1" || len(info.Children) != 1 || info.Children[0].Metrics["operator_type"] != "PROJECTION" {
			t.Fatalf("unexpected profiling snapshot: %+v", info)
		}

		// Post-close -> ErrClosed.
		_ = conn.Close()
		if _, err := conn.ProfilingInfo(); !errors.Is(err, ErrClosed) {
			t.Fatalf("post-close ProfilingInfo = %v, want ErrClosed", err)
		}
		_ = db.Close()
		f.assertZeroLeaks(t)
	})
}
