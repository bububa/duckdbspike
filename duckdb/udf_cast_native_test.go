package duckdb_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bububa/duckdbspike/duckdb"
	"github.com/bububa/duckdbspike/internal/abi"
)

func TestNativeScalarFunctionParallelSetVarargsBindInitAndErrors(t *testing.T) {
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

	// 1. Multi-threaded parallel ScalarFunction (400k rows across 4 workers) via ExecuteChunk.
	var inFlight, peakConcurrent atomic.Int64
	err = conn.RegisterScalarFunction(duckdb.ScalarFunctionSpec{
		Name:       "go_add_one",
		Parameters: []duckdb.LogicalType{duckdb.ScalarType(duckdb.TypeBigint)},
		ReturnType: duckdb.ScalarType(duckdb.TypeBigint),
		ExecuteChunk: func(_, _ any, chunk *duckdb.ScalarFunctionChunk) error {
			cur := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				prev := peakConcurrent.Load()
				if cur <= prev || peakConcurrent.CompareAndSwap(prev, cur) {
					break
				}
			}
			time.Sleep(150 * time.Microsecond)
			n := chunk.Len()
			for row := range n {
				v, err := chunk.Arg(0, row)
				if err != nil {
					return err
				}
				if v.IsNull() {
					if err := chunk.SetOutput(row, duckdb.NullValue()); err != nil {
						return err
					}
					continue
				}
				x, _ := v.Int64()
				if x < 0 {
					return errors.New("go_add_one: negative input")
				}
				if err := chunk.SetOutput(row, duckdb.Int64Value(x+1)); err != nil {
					return err
				}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("RegisterScalarFunction(go_add_one): %v", err)
	}

	const rowsN int64 = 400_000
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE sf_nums AS SELECT i::BIGINT AS i FROM range(%d) t(i)", rowsN)); err != nil {
		t.Fatal(err)
	}
	_, sumRow := querySingleRow(t, conn, "SELECT sum(go_add_one(i))::BIGINT FROM sf_nums")
	wantSum := rowsN * (rowsN + 1) / 2
	if got, _ := sumRow[0].Int64(); got != wantSum {
		t.Fatalf("parallel scalar sum = %d, want %d", got, wantSum)
	}
	t.Logf("parallel scalar function: peakConcurrent=%d", peakConcurrent.Load())

	// Error propagation across C++/Go boundary.
	if _, err := conn.Query(ctx, "SELECT go_add_one(-7::BIGINT)"); err == nil || !strings.Contains(err.Error(), "negative input") {
		t.Fatalf("expected scalar error, got %v", err)
	}

	// 2. Overloaded ScalarFunctionSet (`go_fmt(BIGINT)` vs `go_fmt(VARCHAR)`).
	err = conn.RegisterScalarFunctionSet("go_fmt",
		duckdb.ScalarFunctionSpec{
			Parameters: []duckdb.LogicalType{duckdb.ScalarType(duckdb.TypeBigint)},
			ReturnType: duckdb.ScalarType(duckdb.TypeVarchar),
			Execute: func(_, _ any, args []duckdb.Value) (duckdb.Value, error) {
				n, _ := args[0].Int64()
				return duckdb.VarcharValue(fmt.Sprintf("int:%d", n)), nil
			},
		},
		duckdb.ScalarFunctionSpec{
			Parameters: []duckdb.LogicalType{duckdb.ScalarType(duckdb.TypeVarchar)},
			ReturnType: duckdb.ScalarType(duckdb.TypeVarchar),
			Execute: func(_, _ any, args []duckdb.Value) (duckdb.Value, error) {
				s, _ := args[0].Varchar()
				return duckdb.VarcharValue("str:" + s), nil
			},
		},
	)
	if err != nil {
		t.Fatalf("RegisterScalarFunctionSet(go_fmt): %v", err)
	}
	_, fmtRow := querySingleRow(t, conn, "SELECT go_fmt(42::BIGINT), go_fmt('hi'), go_fmt(NULL::BIGINT)")
	if s, _ := fmtRow[0].Varchar(); s != "int:42" {
		t.Fatalf("go_fmt(42) = %q, want int:42", s)
	}
	if s, _ := fmtRow[1].Varchar(); s != "str:hi" {
		t.Fatalf("go_fmt('hi') = %q, want str:hi", s)
	}
	if !fmtRow[2].IsNull() {
		t.Fatalf("go_fmt(NULL) = %v, want NULL", fmtRow[2])
	}

	// 3. Varargs + SpecialNulls + Volatile.
	strType := duckdb.ScalarType(duckdb.TypeVarchar)
	err = conn.RegisterScalarFunction(duckdb.ScalarFunctionSpec{
		Name:         "go_join_csv",
		Varargs:      &strType,
		ReturnType:   duckdb.ScalarType(duckdb.TypeVarchar),
		SpecialNulls: true,
		Execute: func(_, _ any, args []duckdb.Value) (duckdb.Value, error) {
			parts := make([]string, len(args))
			for i, a := range args {
				if a.IsNull() {
					parts[i] = "<NULL>"
				} else {
					parts[i], _ = a.Varchar()
				}
			}
			return duckdb.VarcharValue(strings.Join(parts, ",")), nil
		},
	})
	if err != nil {
		t.Fatalf("RegisterScalarFunction(go_join_csv): %v", err)
	}
	_, csvRow := querySingleRow(t, conn, "SELECT go_join_csv('a', NULL, 'c')")
	if s, _ := csvRow[0].Varchar(); s != "a,<NULL>,c" {
		t.Fatalf("go_join_csv = %q, want a,<NULL>,c", s)
	}

	var volCounter atomic.Int64
	err = conn.RegisterScalarFunction(duckdb.ScalarFunctionSpec{
		Name:       "go_next_seq",
		ReturnType: duckdb.ScalarType(duckdb.TypeBigint),
		Volatile:   true,
		Execute: func(_, _ any, _ []duckdb.Value) (duckdb.Value, error) {
			return duckdb.Int64Value(volCounter.Add(1)), nil
		},
	})
	if err != nil {
		t.Fatalf("RegisterScalarFunction(go_next_seq): %v", err)
	}
	_, volRow := querySingleRow(t, conn, "SELECT count(DISTINCT go_next_seq())::BIGINT FROM range(10)")
	if cnt, _ := volRow[0].Int64(); cnt != 10 {
		t.Fatalf("volatile distinct count = %d, want 10", cnt)
	}

	// 4. Bind (constant expression folding + HookCopyCallback) + per-thread Init + panic recovery.
	var initCount atomic.Int64
	err = conn.RegisterScalarFunction(duckdb.ScalarFunctionSpec{
		Name:       "go_scale_by",
		Parameters: []duckdb.LogicalType{duckdb.ScalarType(duckdb.TypeBigint), duckdb.ScalarType(duckdb.TypeBigint)},
		ReturnType: duckdb.ScalarType(duckdb.TypeBigint),
		Bind: func(in duckdb.ScalarFunctionBindInput) (any, error) {
			if len(in.Args) != 2 || !in.Args[1].IsFoldable {
				return nil, errors.New("second argument must be a constant multiplier")
			}
			mult, err := in.Args[1].Value.AsInt64()
			if err != nil {
				return nil, err
			}
			return mult, nil
		},
		Init: func(bindData any) (any, error) {
			initCount.Add(1)
			return bindData.(int64), nil
		},
		Execute: func(bindData, localState any, args []duckdb.Value) (duckdb.Value, error) {
			x, _ := args[0].Int64()
			if x == 9999 {
				panic("scalar panic test")
			}
			return duckdb.Int64Value(x * localState.(int64)), nil
		},
	})
	if err != nil {
		t.Fatalf("RegisterScalarFunction(go_scale_by): %v", err)
	}
	_, scaleRow := querySingleRow(t, conn, "SELECT sum(go_scale_by(i::BIGINT, 3))::BIGINT FROM range(10) t(i)")
	if got, _ := scaleRow[0].Int64(); got != 135 { // (0..9)=45 * 3 = 135
		t.Fatalf("go_scale_by sum = %d, want 135", got)
	}
	if initCount.Load() == 0 {
		t.Fatal("expected scalar Init callback to execute at least once")
	}
	if _, err := conn.Query(ctx, "SELECT go_scale_by(9999::BIGINT, 2)"); err == nil || !strings.Contains(err.Error(), "scalar panic test") {
		t.Fatalf("expected scalar panic error, got %v", err)
	}

	_ = conn.Close()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if got := abi.HookHandleCount(); got != baseHooks {
		t.Fatalf("HookHandleCount after db.Close = %d, want %d", got, baseHooks)
	}
}

func TestNativeAggregateFunctionParallelGroupBySetAndSpecialNulls(t *testing.T) {
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

	// 1. Parallel GROUP BY aggregate (200k rows across 64 groups, threads=4).
	type sumState struct {
		sum     int64
		hasData bool
	}
	var combineCalls atomic.Int64
	err = conn.RegisterAggregateFunction(duckdb.AggregateFunctionSpec{
		Name:       "go_sum",
		Parameters: []duckdb.LogicalType{duckdb.ScalarType(duckdb.TypeBigint)},
		ReturnType: duckdb.ScalarType(duckdb.TypeBigint),
		Init:       func() any { return &sumState{} },
		Update: func(st any, args []duckdb.Value) error {
			s := st.(*sumState)
			v, _ := args[0].Int64()
			s.sum += v
			s.hasData = true
			return nil
		},
		Combine: func(src, dst any) error {
			combineCalls.Add(1)
			s := src.(*sumState)
			d := dst.(*sumState)
			if s.hasData {
				d.sum += s.sum
				d.hasData = true
			}
			return nil
		},
		Finalize: func(st any) (duckdb.Value, error) {
			s := st.(*sumState)
			if !s.hasData {
				return duckdb.NullValue(), nil
			}
			return duckdb.Int64Value(s.sum), nil
		},
	})
	if err != nil {
		t.Fatalf("RegisterAggregateFunction(go_sum): %v", err)
	}

	const nRows int64 = 200_000
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE agg_tbl AS SELECT (i %% 64)::BIGINT AS grp, CASE WHEN i %% 10 = 0 THEN NULL ELSE i::BIGINT END AS v FROM range(%d) t(i)", nRows)); err != nil {
		t.Fatal(err)
	}

	// Compare go_sum(v) against built-in sum(v) across all 64 groups!
	_, cmpRow := querySingleRow(t, conn, `
		WITH g AS (
			SELECT grp, go_sum(v) AS got, sum(v)::BIGINT AS want
			FROM agg_tbl
			GROUP BY grp
		)
		SELECT count(*)::BIGINT, sum(CASE WHEN got = want THEN 1 ELSE 0 END)::BIGINT FROM g
	`)
	if total, _ := cmpRow[0].Int64(); total != 64 {
		t.Fatalf("group count = %d, want 64", total)
	}
	if matched, _ := cmpRow[1].Int64(); matched != 64 {
		t.Fatalf("matched groups = %d, want 64", matched)
	}
	t.Logf("parallel aggregate: combineCalls=%d", combineCalls.Load())

	// Empty / all-NULL group returns SQL NULL.
	_, nullRow := querySingleRow(t, conn, "SELECT go_sum(NULL::BIGINT)")
	if !nullRow[0].IsNull() {
		t.Fatalf("go_sum(NULL) = %v, want NULL", nullRow[0])
	}

	// 2. SpecialNulls aggregate counting NULLs explicitly + overloaded AggregateFunctionSet.
	type nullCountState struct{ nulls int64 }
	err = conn.RegisterAggregateFunctionSet("go_null_count",
		duckdb.AggregateFunctionSpec{
			Parameters:   []duckdb.LogicalType{duckdb.ScalarType(duckdb.TypeBigint)},
			ReturnType:   duckdb.ScalarType(duckdb.TypeBigint),
			SpecialNulls: true,
			Init:         func() any { return &nullCountState{} },
			Update: func(st any, args []duckdb.Value) error {
				if args[0].IsNull() {
					st.(*nullCountState).nulls++
				}
				return nil
			},
			Combine: func(src, dst any) error {
				dst.(*nullCountState).nulls += src.(*nullCountState).nulls
				return nil
			},
			Finalize: func(st any) (duckdb.Value, error) {
				return duckdb.Int64Value(st.(*nullCountState).nulls), nil
			},
		},
		duckdb.AggregateFunctionSpec{
			Parameters:   []duckdb.LogicalType{duckdb.ScalarType(duckdb.TypeVarchar)},
			ReturnType:   duckdb.ScalarType(duckdb.TypeBigint),
			SpecialNulls: true,
			Init:         func() any { return &nullCountState{} },
			Update: func(st any, args []duckdb.Value) error {
				if args[0].IsNull() {
					st.(*nullCountState).nulls++
				}
				return nil
			},
			Combine: func(src, dst any) error {
				dst.(*nullCountState).nulls += src.(*nullCountState).nulls
				return nil
			},
			Finalize: func(st any) (duckdb.Value, error) {
				return duckdb.Int64Value(st.(*nullCountState).nulls), nil
			},
		},
	)
	if err != nil {
		t.Fatalf("RegisterAggregateFunctionSet(go_null_count): %v", err)
	}
	_, ncRow := querySingleRow(t, conn, "SELECT go_null_count(v), go_null_count(CASE WHEN v IS NULL THEN NULL ELSE 'ok' END) FROM agg_tbl")
	if n1, _ := ncRow[0].Int64(); n1 != nRows/10 {
		t.Fatalf("go_null_count(BIGINT) = %d, want %d", n1, nRows/10)
	}
	if n2, _ := ncRow[1].Int64(); n2 != nRows/10 {
		t.Fatalf("go_null_count(VARCHAR) = %d, want %d", n2, nRows/10)
	}

	_ = conn.Close()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if got := abi.HookHandleCount(); got != baseHooks {
		t.Fatalf("HookHandleCount after db.Close = %d, want %d (leaked aggregate states)", got, baseHooks)
	}
}

func TestNativeCastFunctionCustomTypesNormalAndTryCast(t *testing.T) {
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

	// Register custom logical types `celsius` and `fahrenheit`.
	if err := conn.RegisterLogicalType("celsius", duckdb.ScalarType(duckdb.TypeDouble)); err != nil {
		t.Fatalf("RegisterLogicalType(celsius): %v", err)
	}
	if err := conn.RegisterLogicalType("fahrenheit", duckdb.ScalarType(duckdb.TypeDouble)); err != nil {
		t.Fatalf("RegisterLogicalType(fahrenheit): %v", err)
	}

	celsiusType := duckdb.ScalarType(duckdb.TypeDouble).WithAlias("celsius")
	fahrenheitType := duckdb.ScalarType(duckdb.TypeDouble).WithAlias("fahrenheit")

	// 1. Register implicit + explicit cast: celsius -> fahrenheit (c * 9/5 + 32).
	cost := int64(10)
	if err := conn.RegisterCastFunction(duckdb.CastFunctionSpec{
		SourceType:       celsiusType,
		TargetType:       fahrenheitType,
		ImplicitCastCost: &cost,
		Cast: func(_ duckdb.CastMode, val duckdb.Value) (duckdb.Value, error) {
			c, err := val.Float64()
			if err != nil {
				return duckdb.Value{}, err
			}
			return duckdb.Float64Value(c*9.0/5.0 + 32.0), nil
		},
	}); err != nil {
		t.Fatalf("RegisterCastFunction(celsius->fahrenheit): %v", err)
	}

	// 2. Register parsing cast: VARCHAR -> celsius (e.g. "100C" -> 100.0; bad input errors or TRY_CAST NULL).
	if err := conn.RegisterCastFunction(duckdb.CastFunctionSpec{
		SourceType: duckdb.ScalarType(duckdb.TypeVarchar),
		TargetType: celsiusType,
		Cast: func(_ duckdb.CastMode, val duckdb.Value) (duckdb.Value, error) {
			s, err := val.Varchar()
			if err != nil {
				return duckdb.Value{}, err
			}
			if !strings.HasSuffix(s, "C") {
				return duckdb.Value{}, fmt.Errorf("invalid celsius literal %q", s)
			}
			f, err := strconv.ParseFloat(strings.TrimSuffix(s, "C"), 64)
			if err != nil {
				return duckdb.Value{}, fmt.Errorf("invalid celsius number %q: %w", s, err)
			}
			return duckdb.Float64Value(f), nil
		},
	}); err != nil {
		t.Fatalf("RegisterCastFunction(varchar->celsius): %v", err)
	}

	// Explicit chained CAST: '100C' -> celsius -> fahrenheit = 212.0.
	cols, row := querySingleRow(t, conn, "SELECT CAST(CAST('100C' AS celsius) AS fahrenheit)")
	if cols[0].Type.Alias != "fahrenheit" {
		t.Fatalf("result alias = %q, want fahrenheit", cols[0].Type.Alias)
	}
	if f, _ := row[0].Float64(); f != 212.0 {
		t.Fatalf("100C in F = %v, want 212", f)
	}

	// Normal CAST on bad literal must fail with our Go error.
	if _, err := conn.Query(ctx, "SELECT CAST('not_a_temp' AS celsius)"); err == nil || !strings.Contains(err.Error(), "invalid celsius literal") {
		t.Fatalf("expected CAST error, got %v", err)
	}

	// TRY_CAST over a multi-row batch with valid, invalid, and NULL inputs.
	rows, err := conn.Query(ctx, `
		SELECT idx, TRY_CAST(raw AS celsius) AS c
		FROM (VALUES (1, '0C'), (2, 'bad'), (3, NULL), (4, '25C')) t(idx, raw)
		ORDER BY idx
	`)
	if err != nil {
		t.Fatalf("TRY_CAST query: %v", err)
	}
	type wantRow struct {
		isNull bool
		val    float64
	}
	want := []wantRow{
		{false, 0.0},
		{true, 0.0},
		{true, 0.0},
		{false, 25.0},
	}
	gotIdx := 0
	for rows.Next() {
		vals, err := rows.DecodeRow(nil)
		if err != nil {
			t.Fatal(err)
		}
		if want[gotIdx].isNull {
			if !vals[1].IsNull() {
				t.Fatalf("row %d got %v, want NULL", gotIdx, vals[1])
			}
		} else {
			gotV, err := vals[1].Float64()
			if err != nil || gotV != want[gotIdx].val {
				t.Fatalf("row %d got (%v, %v), want %v", gotIdx, gotV, err, want[gotIdx].val)
			}
		}
		gotIdx++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if gotIdx != len(want) {
		t.Fatalf("TRY_CAST row count = %d, want %d", gotIdx, len(want))
	}

	_ = conn.Close()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if got := abi.HookHandleCount(); got != baseHooks {
		t.Fatalf("HookHandleCount after db.Close = %d, want %d", got, baseHooks)
	}
}
