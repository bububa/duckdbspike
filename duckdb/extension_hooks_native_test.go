package duckdb_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bububa/duckdbspike/duckdb"
	"github.com/bububa/duckdbspike/internal/abi"
)

func TestNativeCustomConfigOptionAndLogicalType(t *testing.T) {
	lib := loadNativeLibrary(t)
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

	// 1. Register custom config options with defaults and verify read + SQL SET.
	defEnv := duckdb.VarcharValue("dev")
	if err := conn.RegisterConfigOption(duckdb.ConfigOptionSpec{
		Name:         "app_env",
		Type:         duckdb.ScalarType(duckdb.TypeVarchar),
		DefaultValue: &defEnv,
		DefaultScope: duckdb.ConfigOptionScopeSession,
		Description:  "Application deployment environment",
	}); err != nil {
		t.Fatalf("RegisterConfigOption(app_env): %v", err)
	}

	defBatch := duckdb.Int64Value(100)
	if err := conn.RegisterConfigOption(duckdb.ConfigOptionSpec{
		Name:         "app_batch_size",
		Type:         duckdb.ScalarType(duckdb.TypeBigint),
		DefaultValue: &defBatch,
		DefaultScope: duckdb.ConfigOptionScopeSession,
		Description:  "Batch size hint",
	}); err != nil {
		t.Fatalf("RegisterConfigOption(app_batch_size): %v", err)
	}

	envVal, _, ok, err := duckdb.ConfigOptionAs[string](conn, "app_env")
	if err != nil || !ok || envVal != "dev" {
		t.Fatalf("initial app_env = (%q, %v, %v), want (dev, true, nil)", envVal, ok, err)
	}
	batchVal, _, ok, err := duckdb.ConfigOptionAs[int64](conn, "app_batch_size")
	if err != nil || !ok || batchVal != 100 {
		t.Fatalf("initial app_batch_size = (%d, %v, %v), want (100, true, nil)", batchVal, ok, err)
	}

	if _, err := conn.Exec(ctx, `SET app_env = 'production'`); err != nil {
		t.Fatalf("SET app_env: %v", err)
	}
	if _, err := conn.Exec(ctx, `SET app_batch_size = 512`); err != nil {
		t.Fatalf("SET app_batch_size: %v", err)
	}

	envVal, _, ok, err = duckdb.ConfigOptionAs[string](conn, "app_env")
	if err != nil || !ok || envVal != "production" {
		t.Fatalf("updated app_env = (%q, %v, %v), want (production, true, nil)", envVal, ok, err)
	}
	batchVal, _, ok, err = duckdb.ConfigOptionAs[int64](conn, "app_batch_size")
	if err != nil || !ok || batchVal != 512 {
		t.Fatalf("updated app_batch_size = (%d, %v, %v), want (512, true, nil)", batchVal, ok, err)
	}

	// 2. Register custom scalar and composite logical types and verify aliases.
	if err := conn.RegisterLogicalType("user_id", duckdb.ScalarType(duckdb.TypeBigint)); err != nil {
		t.Fatalf("RegisterLogicalType(user_id): %v", err)
	}
	geoPair := duckdb.StructType(
		duckdb.StructFieldType{Name: "lat", Type: duckdb.ScalarType(duckdb.TypeDouble)},
		duckdb.StructFieldType{Name: "lng", Type: duckdb.ScalarType(duckdb.TypeDouble)},
	)
	if err := conn.RegisterLogicalType("geo_pair", geoPair); err != nil {
		t.Fatalf("RegisterLogicalType(geo_pair): %v", err)
	}

	if _, err := conn.Exec(ctx, `CREATE TABLE places (uid user_id, pos geo_pair)`); err != nil {
		t.Fatalf("CREATE TABLE places: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO places VALUES (42::user_id, {'lat': 37.7749, 'lng': -122.4194}::geo_pair)`); err != nil {
		t.Fatalf("INSERT INTO places: %v", err)
	}

	cols, row := querySingleRow(t, conn, `SELECT uid, pos FROM places`)
	if cols[0].Type.Kind != duckdb.TypeBigint || cols[0].Type.Alias != "user_id" {
		t.Fatalf("col[0] type = %+v, want BIGINT with alias user_id", cols[0].Type)
	}
	if cols[1].Type.Kind != duckdb.TypeStruct || cols[1].Type.Alias != "geo_pair" {
		t.Fatalf("col[1] type = %+v, want STRUCT with alias geo_pair", cols[1].Type)
	}
	if uid, _ := row[0].Int64(); uid != 42 {
		t.Fatalf("uid = %d, want 42", uid)
	}
	st, err := row[1].Struct()
	if err != nil || len(st.Fields) != 2 {
		t.Fatalf("pos struct = (%+v, %v)", st, err)
	}
	if lat, _ := st.Fields[0].Value.Float64(); lat != 37.7749 {
		t.Fatalf("lat = %v, want 37.7749", lat)
	}
}

func TestNativeTableFunctionRichTypesNamedAndAnyParams(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()
	baseHooks := abi.HookHandleCount()

	db, err := lib.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Connect(ctx)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}

	type kvBind struct {
		count  int64
		prefix string
		failAt int64
	}
	type kvState struct {
		next int64
	}

	listStrType := duckdb.ListType(duckdb.ScalarType(duckdb.TypeVarchar))
	metaType := duckdb.StructType(
		duckdb.StructFieldType{Name: "k", Type: duckdb.ScalarType(duckdb.TypeVarchar)},
		duckdb.StructFieldType{Name: "v", Type: duckdb.ScalarType(duckdb.TypeInteger)},
	)

	err = conn.RegisterTableFunction(duckdb.TableFunctionSpec{
		Name:       "go_kv",
		Parameters: []duckdb.LogicalType{duckdb.ScalarType(duckdb.TypeBigint)},
		NamedParameters: map[string]duckdb.LogicalType{
			"prefix":  duckdb.ScalarType(duckdb.TypeVarchar),
			"fail_at": duckdb.ScalarType(duckdb.TypeBigint),
		},
		Bind: func(in duckdb.TableFunctionBindInput) (duckdb.TableFunctionBindResult, error) {
			n, err := in.Args[0].Int64()
			if err != nil {
				return duckdb.TableFunctionBindResult{}, err
			}
			if n < 0 {
				return duckdb.TableFunctionBindResult{}, errors.New("go_kv: negative row count")
			}
			if n == 999999 {
				panic("go_kv bind panic test")
			}
			prefix := "row"
			if p, ok := in.NamedArgs["prefix"]; ok && !p.IsNull() {
				prefix, _ = p.Varchar()
			}
			var failAt int64 = -1
			if f, ok := in.NamedArgs["fail_at"]; ok && !f.IsNull() {
				failAt, _ = f.Int64()
			}
			return duckdb.TableFunctionBindResult{
				Columns: []duckdb.Column{
					{Name: "id", Type: duckdb.ScalarType(duckdb.TypeBigint)},
					{Name: "label", Type: duckdb.ScalarType(duckdb.TypeVarchar)},
					{Name: "tags", Type: listStrType},
					{Name: "meta", Type: metaType},
				},
				Cardinality: uint64(n),
				ExactCard:   true,
				BindData:    &kvBind{count: n, prefix: prefix, failAt: failAt},
			}, nil
		},
		Init: func(in duckdb.TableFunctionInitInput) (duckdb.TableFunctionInitResult, error) {
			return duckdb.TableFunctionInitResult{InitData: &kvState{}}, nil
		},
		Execute: func(bindData, initData, _ any, chunk *duckdb.TableFunctionChunk) error {
			bd := bindData.(*kvBind)
			st := initData.(*kvState)
			for st.next < bd.count && chunk.Len() < chunk.Cap() {
				i := st.next
				if bd.failAt >= 0 && i == bd.failAt {
					return fmt.Errorf("go_kv: forced error at row %d", i)
				}
				st.next++
				tagsVal, err := duckdb.NewListValue(
					duckdb.ScalarType(duckdb.TypeVarchar),
					[]duckdb.Value{
						duckdb.VarcharValue(bd.prefix),
						duckdb.VarcharValue(fmt.Sprintf("t%d", i)),
					},
				)
				if err != nil {
					return err
				}
				metaVal, err := duckdb.NewStructValue([]duckdb.StructFieldValue{
					{Name: "k", Type: duckdb.ScalarType(duckdb.TypeVarchar), Value: duckdb.VarcharValue(fmt.Sprintf("k%d", i))},
					{Name: "v", Type: duckdb.ScalarType(duckdb.TypeInteger), Value: duckdb.Int32Value(int32(i * 10))},
				})
				if err != nil {
					return err
				}
				if err := chunk.AppendRow(
					duckdb.Int64Value(i),
					duckdb.VarcharValue(fmt.Sprintf("%s_%d", bd.prefix, i)),
					tagsVal,
					metaVal,
				); err != nil {
					return err
				}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("RegisterTableFunction(go_kv): %v", err)
	}

	// Query with named parameter + rich composite types.
	rows, err := conn.Query(ctx, `SELECT id, label, tags, meta FROM go_kv(3, prefix := 'item') ORDER BY id`)
	if err != nil {
		t.Fatalf("Query go_kv: %v", err)
	}
	var count int64
	for rows.Next() {
		vals, err := rows.DecodeRow(nil)
		if err != nil {
			t.Fatal(err)
		}
		id, _ := vals[0].Int64()
		label, _ := vals[1].Varchar()
		tags, err := vals[2].List()
		if err != nil {
			t.Fatal(err)
		}
		meta, err := vals[3].Struct()
		if err != nil {
			t.Fatal(err)
		}
		if id != count || label != fmt.Sprintf("item_%d", count) {
			t.Fatalf("row %d got (%d, %q)", count, id, label)
		}
		if len(tags.Elements) != 2 {
			t.Fatalf("row %d tags len = %d", count, len(tags.Elements))
		}
		if mv, _ := meta.Fields[1].Value.Int32(); mv != int32(count*10) {
			t.Fatalf("row %d meta.v = %d, want %d", count, mv, count*10)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if count != 3 {
		t.Fatalf("row count = %d, want 3", count)
	}

	// Verify bind error, bind panic, and execute error propagation.
	if _, err := conn.Query(ctx, `SELECT * FROM go_kv(-1)`); err == nil || !strings.Contains(err.Error(), "negative row count") {
		t.Fatalf("expected bind error, got %v", err)
	}
	if _, err := conn.Query(ctx, `SELECT * FROM go_kv(999999)`); err == nil || !strings.Contains(err.Error(), "bind panic") {
		t.Fatalf("expected bind panic error, got %v", err)
	}
	if r, err := conn.Query(ctx, `SELECT * FROM go_kv(5, fail_at := 2)`); err == nil {
		for r.Next() {
			_, _ = r.DecodeRow(nil)
		}
		err = r.Err()
		_ = r.Close()
		if err == nil || !strings.Contains(err.Error(), "forced error at row 2") {
			t.Fatalf("expected execute error, got %v", err)
		}
	} else if !strings.Contains(err.Error(), "forced error at row 2") {
		t.Fatalf("expected execute error, got %v", err)
	}

	// Polymorphic AnyType() parameter test.
	err = conn.RegisterTableFunction(duckdb.TableFunctionSpec{
		Name:       "go_echo_kind",
		Parameters: []duckdb.LogicalType{duckdb.AnyType()},
		Bind: func(in duckdb.TableFunctionBindInput) (duckdb.TableFunctionBindResult, error) {
			return duckdb.TableFunctionBindResult{
				Columns: []duckdb.Column{
					{Name: "kind", Type: duckdb.ScalarType(duckdb.TypeVarchar)},
				},
				BindData: in.Args[0].Kind().String(),
			}, nil
		},
		Init: func(in duckdb.TableFunctionInitInput) (duckdb.TableFunctionInitResult, error) {
			done := false
			return duckdb.TableFunctionInitResult{InitData: &done}, nil
		},
		Execute: func(bindData, initData, _ any, chunk *duckdb.TableFunctionChunk) error {
			done := initData.(*bool)
			if *done {
				return nil
			}
			*done = true
			return chunk.AppendRow(duckdb.VarcharValue(bindData.(string)))
		},
	})
	if err != nil {
		t.Fatalf("RegisterTableFunction(go_echo_kind): %v", err)
	}
	_, r1 := querySingleRow(t, conn, `SELECT kind FROM go_echo_kind(42::BIGINT)`)
	if k, _ := r1[0].Varchar(); k != "BIGINT" {
		t.Fatalf("go_echo_kind(42) = %q, want BIGINT", k)
	}
	_, r2 := querySingleRow(t, conn, `SELECT kind FROM go_echo_kind('hello')`)
	if k, _ := r2[0].Varchar(); k != "VARCHAR" {
		t.Fatalf("go_echo_kind('hello') = %q, want VARCHAR", k)
	}

	_ = conn.Close()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if got := abi.HookHandleCount(); got != baseHooks {
		t.Fatalf("HookHandleCount after db.Close = %d, want %d (leaked handles)", got, baseHooks)
	}
}

func TestNativeTableFunctionProjectionPushdownAndCountStar(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()
	baseHooks := abi.HookHandleCount()

	db, err := lib.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Connect(ctx)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}

	var lastProjected []uint64

	type projState struct {
		next      int64
		projected []uint64
	}

	err = conn.RegisterTableFunction(duckdb.TableFunctionSpec{
		Name:               "go_proj",
		Parameters:         []duckdb.LogicalType{duckdb.ScalarType(duckdb.TypeBigint)},
		ProjectionPushdown: true,
		Bind: func(in duckdb.TableFunctionBindInput) (duckdb.TableFunctionBindResult, error) {
			n, _ := in.Args[0].Int64()
			return duckdb.TableFunctionBindResult{
				Columns: []duckdb.Column{
					{Name: "id", Type: duckdb.ScalarType(duckdb.TypeBigint)},
					{Name: "label", Type: duckdb.ScalarType(duckdb.TypeVarchar)},
				},
				Cardinality: uint64(n),
				ExactCard:   true,
				BindData:    n,
			}, nil
		},
		Init: func(in duckdb.TableFunctionInitInput) (duckdb.TableFunctionInitResult, error) {
			lastProjected = append([]uint64(nil), in.ProjectedColumns...)
			return duckdb.TableFunctionInitResult{
				InitData: &projState{projected: lastProjected},
			}, nil
		},
		Execute: func(bindData, initData, _ any, chunk *duckdb.TableFunctionChunk) error {
			total := bindData.(int64)
			st := initData.(*projState)
			if st.next >= total {
				return nil
			}
			// Check for virtual COUNT(*) projection (only math.MaxUint64 / invalid columns).
			if len(st.projected) == 1 && st.projected[0] == math.MaxUint64 {
				rem := total - st.next
				if rem > int64(chunk.Cap()) {
					rem = int64(chunk.Cap())
				}
				st.next += rem
				return chunk.SetSize(int(rem))
			}
			for st.next < total && chunk.Len() < chunk.Cap() {
				i := st.next
				st.next++
				row := make([]duckdb.Value, len(st.projected))
				for idx, colID := range st.projected {
					switch colID {
					case 0:
						row[idx] = duckdb.Int64Value(i)
					case 1:
						row[idx] = duckdb.VarcharValue(fmt.Sprintf("lbl_%d", i))
					default:
						row[idx] = duckdb.NullValue()
					}
				}
				if err := chunk.AppendRow(row...); err != nil {
					return err
				}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("RegisterTableFunction(go_proj): %v", err)
	}

	// Project only column 1 (`label`).
	_, row := querySingleRow(t, conn, `SELECT max(label) FROM go_proj(100)`)
	if got, _ := row[0].Varchar(); got != "lbl_99" {
		t.Fatalf("max(label) = %q, want lbl_99", got)
	}
	if len(lastProjected) != 1 || lastProjected[0] != 1 {
		t.Fatalf("projected columns = %v, want [1]", lastProjected)
	}

	// Virtual COUNT(*) projection.
	_, cntRow := querySingleRow(t, conn, `SELECT count(*)::BIGINT FROM go_proj(5000)`)
	if cnt, _ := cntRow[0].Int64(); cnt != 5000 {
		t.Fatalf("count(*) = %d, want 5000", cnt)
	}

	_ = conn.Close()
	_ = db.Close()
	if got := abi.HookHandleCount(); got != baseHooks {
		t.Fatalf("HookHandleCount after db.Close = %d, want %d", got, baseHooks)
	}
}

func TestNativeTableFunctionMultithreadedAndReplacementScan(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()
	baseHooks := abi.HookHandleCount()

	db, err := lib.Open(ctx, ":memory:", duckdb.Option{Name: "threads", Value: "4"})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Connect(ctx)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}

	// 1. Multi-threaded parallel TableFunction (MaxThreads = 4 + LocalInit).
	type parGlobal struct {
		next           atomic.Int64
		total          int64
		inFlight       atomic.Int64
		peakConcurrent atomic.Int64
		localInits     atomic.Int64
	}
	type parLocal struct {
		workerID int64
	}

	var lastGlobal *parGlobal
	err = conn.RegisterTableFunction(duckdb.TableFunctionSpec{
		Name:       "go_parallel_range",
		Parameters: []duckdb.LogicalType{duckdb.ScalarType(duckdb.TypeBigint)},
		Bind: func(in duckdb.TableFunctionBindInput) (duckdb.TableFunctionBindResult, error) {
			n, _ := in.Args[0].Int64()
			return duckdb.TableFunctionBindResult{
				Columns: []duckdb.Column{
					{Name: "i", Type: duckdb.ScalarType(duckdb.TypeBigint)},
				},
				Cardinality: uint64(n),
				ExactCard:   true,
				BindData:    n,
			}, nil
		},
		Init: func(in duckdb.TableFunctionInitInput) (duckdb.TableFunctionInitResult, error) {
			g := &parGlobal{total: in.BindData.(int64)}
			lastGlobal = g
			return duckdb.TableFunctionInitResult{InitData: g, MaxThreads: 4}, nil
		},
		LocalInit: func(in duckdb.TableFunctionInitInput) (duckdb.TableFunctionLocalInitResult, error) {
			id := lastGlobal.localInits.Add(1)
			return duckdb.TableFunctionLocalInitResult{LocalState: &parLocal{workerID: id}}, nil
		},
		Execute: func(_ any, initData, localData any, chunk *duckdb.TableFunctionChunk) error {
			g := initData.(*parGlobal)
			if localData == nil || localData.(*parLocal).workerID <= 0 {
				return errors.New("missing worker LocalState")
			}
			cur := g.inFlight.Add(1)
			defer g.inFlight.Add(-1)
			for {
				prev := g.peakConcurrent.Load()
				if cur <= prev || g.peakConcurrent.CompareAndSwap(prev, cur) {
					break
				}
			}

			batchSize := int64(chunk.Cap())
			start := g.next.Add(batchSize) - batchSize
			if start >= g.total {
				return nil
			}
			end := start + batchSize
			if end > g.total {
				end = g.total
			}
			// Tiny delay so multiple DuckDB worker threads overlap deterministically.
			time.Sleep(200 * time.Microsecond)
			for i := start; i < end; i++ {
				if err := chunk.AppendRow(duckdb.Int64Value(i)); err != nil {
					return err
				}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("RegisterTableFunction(go_parallel_range): %v", err)
	}

	const totalRows int64 = 400_000
	_, sumRow := querySingleRow(t, conn, fmt.Sprintf("SELECT sum(i)::BIGINT, count(*)::BIGINT FROM go_parallel_range(%d)", totalRows))
	wantSum := totalRows * (totalRows - 1) / 2
	if gotSum, _ := sumRow[0].Int64(); gotSum != wantSum {
		t.Fatalf("parallel sum = %d, want %d", gotSum, wantSum)
	}
	if gotCnt, _ := sumRow[1].Int64(); gotCnt != totalRows {
		t.Fatalf("parallel count = %d, want %d", gotCnt, totalRows)
	}
	t.Logf("parallel table function: peakConcurrent=%d localInits=%d",
		lastGlobal.peakConcurrent.Load(), lastGlobal.localInits.Load())
	if lastGlobal.peakConcurrent.Load() < 2 {
		t.Logf("note: peakConcurrent=%d (runner may have limited CPU parallelism)", lastGlobal.peakConcurrent.Load())
	}

	// 2. Replacement Scan + Table Function end-to-end (`SELECT * FROM 'mem://orders'`).
	memTables := map[string][][2]any{
		"orders": {
			{int64(1), int64(5)},
			{int64(2), int64(25)},
			{int64(3), int64(50)},
		},
	}

	err = conn.RegisterTableFunction(duckdb.TableFunctionSpec{
		Name:       "read_mem_table",
		Parameters: []duckdb.LogicalType{duckdb.ScalarType(duckdb.TypeVarchar)},
		Bind: func(in duckdb.TableFunctionBindInput) (duckdb.TableFunctionBindResult, error) {
			name, _ := in.Args[0].Varchar()
			rows, ok := memTables[name]
			if !ok {
				return duckdb.TableFunctionBindResult{}, fmt.Errorf("unknown mem table %q", name)
			}
			return duckdb.TableFunctionBindResult{
				Columns: []duckdb.Column{
					{Name: "order_id", Type: duckdb.ScalarType(duckdb.TypeBigint)},
					{Name: "amount", Type: duckdb.ScalarType(duckdb.TypeBigint)},
				},
				Cardinality: uint64(len(rows)),
				ExactCard:   true,
				BindData:    rows,
			}, nil
		},
		Init: func(in duckdb.TableFunctionInitInput) (duckdb.TableFunctionInitResult, error) {
			idx := 0
			return duckdb.TableFunctionInitResult{InitData: &idx}, nil
		},
		Execute: func(bindData, initData, _ any, chunk *duckdb.TableFunctionChunk) error {
			rows := bindData.([][2]any)
			idx := initData.(*int)
			for *idx < len(rows) && chunk.Len() < chunk.Cap() {
				r := rows[*idx]
				*idx++
				if err := chunk.AppendRow(
					duckdb.Int64Value(r[0].(int64)),
					duckdb.Int64Value(r[1].(int64)),
				); err != nil {
					return err
				}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("RegisterTableFunction(read_mem_table): %v", err)
	}

	if err := db.AddReplacementScan(func(tableName string) (*duckdb.ReplacementScanResult, error) {
		if strings.HasPrefix(tableName, "mem://") {
			name := strings.TrimPrefix(tableName, "mem://")
			return &duckdb.ReplacementScanResult{
				FunctionName: "read_mem_table",
				Parameters:   []duckdb.Value{duckdb.VarcharValue(name)},
			}, nil
		}
		if tableName == "bad://trigger" {
			return nil, errors.New("replacement scan rejected bad://trigger")
		}
		return nil, nil
	}); err != nil {
		t.Fatalf("AddReplacementScan: %v", err)
	}

	_, orderRow := querySingleRow(t, conn, `SELECT count(*)::BIGINT, sum(amount)::BIGINT FROM 'mem://orders' WHERE amount > 10`)
	if cnt, _ := orderRow[0].Int64(); cnt != 2 {
		t.Fatalf("mem://orders count = %d, want 2", cnt)
	}
	if sum, _ := orderRow[1].Int64(); sum != 75 {
		t.Fatalf("mem://orders sum = %d, want 75", sum)
	}

	// Non-matching table name must fall through to DuckDB's standard error.
	if _, err := conn.Query(ctx, `SELECT * FROM nonexistent_normal_table`); err == nil {
		t.Fatal("expected normal table-not-found error")
	}
	// Explicit replacement scan error must surface cleanly.
	if _, err := conn.Query(ctx, `SELECT * FROM 'bad://trigger'`); err == nil || !strings.Contains(err.Error(), "rejected bad://trigger") {
		t.Fatalf("expected replacement scan error, got %v", err)
	}

	_ = conn.Close()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if got := abi.HookHandleCount(); got != baseHooks {
		t.Fatalf("HookHandleCount after db.Close = %d, want %d", got, baseHooks)
	}
}
