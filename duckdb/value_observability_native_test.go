package duckdb_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bububa/duckdbspike/duckdb"
)

func TestNativeExtendedScalarTypesRoundTrip(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()
	db, err := lib.Open(ctx, ":memory:", duckdb.Option{Name: "threads", Value: "2"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// 1. SQLNULL: DuckDB's binder resolves bare `SELECT NULL` to INTEGER,
	// whereas `SELECT ?` (before parameter binding via Stmt.Columns) and
	// explicit `NULL::SQLNULL` or `SELECT []` (empty untyped list) can carry SQLNULL.
	// Verify both Stmt.Columns() on an untyped parameter and SELECT [] / NULL.
	stmtNull, err := conn.Prepare(ctx, "SELECT NULL AS n, [] AS empty_list")
	if err != nil {
		t.Fatal(err)
	}
	prepCols, err := stmtNull.Columns()
	_ = stmtNull.Close()
	if err != nil {
		t.Fatalf("Stmt.Columns(SELECT NULL, []): %v", err)
	}
	if prepCols[0].Type.Kind != duckdb.TypeSQLNull && prepCols[0].Type.Kind != duckdb.TypeInteger {
		t.Fatalf("unexpected prepared NULL col type: %s", prepCols[0].Type)
	}
	_, valsNull := querySingleRow(t, conn, "SELECT NULL AS n, [] AS empty_list")
	if !valsNull[0].IsNull() {
		t.Fatal("SELECT NULL must decode as SQL NULL")
	}
	if lv, err := valsNull[1].List(); err != nil || len(lv.Elements) != 0 {
		t.Fatalf("SELECT [] = %+v (%v)", lv, err)
	}

	// 2. UUID bind, decode, SQL string verification, and NULL.
	uuidStrings := []string{
		"00000000-0000-0000-0000-000000000000",
		"01234567-89ab-cdef-0123-456789abcdef",
		"80000000-0000-0000-0000-000000000000",
		"ffffffff-ffff-ffff-ffff-ffffffffffff",
	}
	for _, s := range uuidStrings {
		u, err := duckdb.ParseUUID(s)
		if err != nil {
			t.Fatalf("ParseUUID(%q): %v", s, err)
		}
		_, row := querySingleRow(t, conn, "SELECT ? AS u, ?::VARCHAR AS txt", duckdb.UUIDValue(u), duckdb.UUIDValue(u))
		gotU, err := row[0].UUID()
		if err != nil || gotU != u {
			t.Fatalf("UUID round-trip %q: got %v (%v)", s, gotU, err)
		}
		gotTxt, err := row[1].Varchar()
		if err != nil || gotTxt != s {
			t.Fatalf("UUID SQL text %q: got %q (%v)", s, gotTxt, err)
		}
	}
	_, nullUUIDRow := querySingleRow(t, conn, "SELECT NULL::UUID AS u")
	if !nullUUIDRow[0].IsNull() || nullUUIDRow[0].Kind() != duckdb.TypeUUID {
		t.Fatalf("NULL::UUID = null:%v kind:%s", nullUUIDRow[0].IsNull(), nullUUIDRow[0].Kind())
	}

	// 3. TIMETZ bind, decode, SQL text check, and NULL.
	tzCases := []struct {
		tz   duckdb.TimeTZ
		text string
	}{
		{duckdb.TimeTZ{Micros: 0, OffsetSeconds: 0}, "00:00:00+00"},
		{duckdb.TimeTZ{Micros: 12*3600_000000 + 34*60_000000 + 56_789012, OffsetSeconds: 8 * 3600}, "12:34:56.789012+08"},
		{duckdb.TimeTZ{Micros: 23*3600_000000 + 59*60_000000 + 59_999999, OffsetSeconds: -(15*3600 + 59*60 + 59)}, "23:59:59.999999-15:59:59"},
		{duckdb.TimeTZ{Micros: 8*3600_000000 + 15*60_000000 + 30_000000, OffsetSeconds: 15*3600 + 59*60 + 59}, "08:15:30+15:59:59"},
	}
	for _, tc := range tzCases {
		tv, err := duckdb.TimeTZValue(tc.tz)
		if err != nil {
			t.Fatalf("TimeTZValue(%+v): %v", tc.tz, err)
		}
		_, row := querySingleRow(t, conn, "SELECT ? AS tz, ?::VARCHAR AS txt", tv, tv)
		gotTZ, err := row[0].TimeTZ()
		if err != nil || gotTZ != tc.tz {
			t.Fatalf("TIMETZ round-trip %+v: got %+v (%v)", tc.tz, gotTZ, err)
		}
		gotTxt, err := row[1].Varchar()
		if err != nil || gotTxt != tc.text {
			t.Fatalf("TIMETZ SQL text %+v: got %q, want %q (%v)", tc.tz, gotTxt, tc.text, err)
		}
	}
	_, nullTZRow := querySingleRow(t, conn, "SELECT NULL::TIMETZ AS tz")
	if !nullTZRow[0].IsNull() || nullTZRow[0].Kind() != duckdb.TypeTimeTZ {
		t.Fatalf("NULL::TIMETZ = null:%v kind:%s", nullTZRow[0].IsNull(), nullTZRow[0].Kind())
	}

	// 4. ENUM (UTINYINT and USMALLINT dictionaries), NULL, and parameter bind.
	if _, err := conn.Exec(ctx, "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')"); err != nil {
		t.Fatal(err)
	}
	enumCols, enumVals := querySingleRow(t, conn, "SELECT 'happy'::mood AS m, NULL::mood AS m_null")
	if enumCols[0].Type.Kind != duckdb.TypeEnum || len(enumCols[0].Type.EnumValues) != 3 || enumCols[0].Type.EnumValues[2] != "happy" {
		t.Fatalf("enum col type = %+v", enumCols[0].Type)
	}
	gotEnum, err := enumVals[0].Enum()
	if err != nil || gotEnum != (duckdb.Enum{Index: 2, Label: "happy"}) {
		t.Fatalf("enum decode = %+v (%v)", gotEnum, err)
	}
	if !enumVals[1].IsNull() || enumVals[1].Kind() != duckdb.TypeEnum {
		t.Fatalf("NULL::mood = null:%v kind:%s", enumVals[1].IsNull(), enumVals[1].Kind())
	}
	// Bind ENUM value back as a parameter.
	ev, err := duckdb.EnumValue(duckdb.EnumType("sad", "ok", "happy"), 1)
	if err != nil {
		t.Fatal(err)
	}
	_, boundEnumRow := querySingleRow(t, conn, "SELECT ? AS e, ?::VARCHAR AS txt", ev, ev)
	if ge, err := boundEnumRow[0].Enum(); err != nil || ge != (duckdb.Enum{Index: 1, Label: "ok"}) {
		t.Fatalf("bound enum = %+v (%v)", ge, err)
	}
	if txt, err := boundEnumRow[1].Varchar(); err != nil || txt != "ok" {
		t.Fatalf("bound enum text = %q (%v)", txt, err)
	}

	// Large ENUM (> 256 members -> USMALLINT physical storage).
	labels := make([]string, 300)
	quoted := make([]string, 300)
	for i := range labels {
		labels[i] = fmt.Sprintf("item_%03d", i)
		quoted[i] = fmt.Sprintf("'%s'", labels[i])
	}
	if _, err := conn.Exec(ctx, "CREATE TYPE big_enum AS ENUM ("+strings.Join(quoted, ",")+")"); err != nil {
		t.Fatal(err)
	}
	_, bigEnumRow := querySingleRow(t, conn, "SELECT 'item_299'::big_enum AS b")
	if ge, err := bigEnumRow[0].Enum(); err != nil || ge != (duckdb.Enum{Index: 299, Label: "item_299"}) {
		t.Fatalf("big_enum decode = %+v (%v)", ge, err)
	}

	// 5. Logical type alias: DuckDB JSON columns carry VARCHAR with alias "JSON".
	jCols, jVals, jErr := tryQuerySingleRow(conn, `SELECT '{"a":1,"b":[true,null]}'::JSON AS j, NULL::JSON AS jn, 'plain'::VARCHAR AS s`)
	if jErr != nil {
		t.Fatalf("JSON query failed: %v", jErr)
	}
	if !jCols[0].Type.IsJSON() || !jVals[0].IsJSON() || jCols[0].Type.Alias != "JSON" {
		t.Fatalf("JSON type not detected: col=%+v val=%+v", jCols[0].Type, jVals[0].Type())
	}
	if raw, err := jVals[0].Varchar(); err != nil || raw != `{"a":1,"b":[true,null]}` {
		t.Fatalf("JSON value = %q (%v)", raw, err)
	}
	if !jVals[1].IsNull() || !jVals[1].IsJSON() {
		t.Fatalf("NULL::JSON = null:%v isJSON:%v", jVals[1].IsNull(), jVals[1].IsJSON())
	}
	if jVals[2].IsJSON() || jVals[2].Type().Alias != "" {
		t.Fatalf("plain VARCHAR unexpectedly had alias %q", jVals[2].Type().Alias)
	}
}

func tryQuerySingleRow(conn *duckdb.Conn, sql string, args ...duckdb.Value) ([]duckdb.Column, []duckdb.Value, error) {
	rows, err := conn.Query(context.Background(), sql, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, nil, fmt.Errorf("no rows: %w", rows.Err())
	}
	vals, err := rows.DecodeRow(nil)
	if err != nil {
		return nil, nil, err
	}
	cols := rows.Columns()
	return cols, vals, rows.Close()
}

func TestNativeCompositeParamBinding(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()
	db, err := lib.Open(ctx, ":memory:", duckdb.Option{Name: "threads", Value: "2"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// LIST with NULL element, empty LIST, and nested LIST<LIST<VARCHAR>>.
	listVal, err := duckdb.NewListValue(duckdb.ScalarType(duckdb.TypeInteger), []duckdb.Value{
		duckdb.Int32Value(10),
		duckdb.NullValue(),
		duckdb.Int32Value(30),
	})
	if err != nil {
		t.Fatal(err)
	}
	emptyListVal, err := duckdb.NewListValue(duckdb.ScalarType(duckdb.TypeInteger), nil)
	if err != nil {
		t.Fatal(err)
	}
	innerA, _ := duckdb.NewListValue(duckdb.ScalarType(duckdb.TypeVarchar), []duckdb.Value{duckdb.VarcharValue("a"), duckdb.VarcharValue("b")})
	innerEmpty, _ := duckdb.NewListValue(duckdb.ScalarType(duckdb.TypeVarchar), nil)
	nestedListVal, err := duckdb.NewListValue(duckdb.ListType(duckdb.ScalarType(duckdb.TypeVarchar)), []duckdb.Value{
		innerA,
		innerEmpty,
		duckdb.NullValue(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// ARRAY with NULL element.
	arrVal, err := duckdb.NewArrayValue(duckdb.ScalarType(duckdb.TypeInteger), []duckdb.Value{
		duckdb.Int32Value(1),
		duckdb.NullValue(),
		duckdb.Int32Value(3),
	})
	if err != nil {
		t.Fatal(err)
	}

	// STRUCT with nested LIST and NULL field.
	structVal, err := duckdb.NewStructValue([]duckdb.StructFieldValue{
		{Name: "id", Value: duckdb.Int64Value(42)},
		{Name: "note", Type: duckdb.ScalarType(duckdb.TypeVarchar), Value: duckdb.NullValue()},
		{Name: "tags", Value: innerA},
	})
	if err != nil {
		t.Fatal(err)
	}

	// MAP with NULL value + empty MAP.
	mapVal, err := duckdb.NewMapValue(duckdb.ScalarType(duckdb.TypeVarchar), duckdb.ScalarType(duckdb.TypeInteger), []duckdb.MapEntry{
		{Key: duckdb.VarcharValue("k1"), Value: duckdb.Int32Value(100)},
		{Key: duckdb.VarcharValue("k2"), Value: duckdb.NullValue()},
	})
	if err != nil {
		t.Fatal(err)
	}
	emptyMapVal, err := duckdb.NewMapValue(duckdb.ScalarType(duckdb.TypeVarchar), duckdb.ScalarType(duckdb.TypeInteger), nil)
	if err != nil {
		t.Fatal(err)
	}

	_, row := querySingleRow(t, conn, "SELECT ?, ?, ?, ?, ?, ?, ?",
		listVal, emptyListVal, nestedListVal, arrVal, structVal, mapVal, emptyMapVal)

	// Verify LIST round-trip.
	gotL, err := row[0].List()
	if err != nil || len(gotL.Elements) != 3 || !gotL.Elements[1].IsNull() {
		t.Fatalf("list round-trip = %+v (%v)", gotL, err)
	}
	if v0, _ := gotL.Elements[0].Int32(); v0 != 10 {
		t.Fatalf("list[0] = %d", v0)
	}
	if v2, _ := gotL.Elements[2].Int32(); v2 != 30 {
		t.Fatalf("list[2] = %d", v2)
	}
	// Empty LIST.
	if row[1].IsNull() {
		t.Fatal("empty list became NULL")
	}
	if gotEmptyL, err := row[1].List(); err != nil || len(gotEmptyL.Elements) != 0 {
		t.Fatalf("empty list = %+v (%v)", gotEmptyL, err)
	}
	// Nested LIST.
	gotNL, err := row[2].List()
	if err != nil || len(gotNL.Elements) != 3 || !gotNL.Elements[2].IsNull() {
		t.Fatalf("nested list = %+v (%v)", gotNL, err)
	}
	if sub0, err := gotNL.Elements[0].List(); err != nil || len(sub0.Elements) != 2 {
		t.Fatalf("nested list[0] = %+v (%v)", sub0, err)
	}
	// ARRAY.
	gotA, err := row[3].Array()
	if err != nil || gotA.Size != 3 || len(gotA.Elements) != 3 || !gotA.Elements[1].IsNull() {
		t.Fatalf("array round-trip = %+v (%v)", gotA, err)
	}
	// STRUCT.
	gotS, err := row[4].Struct()
	if err != nil || len(gotS.Fields) != 3 || gotS.Fields[0].Name != "id" || !gotS.Fields[1].Value.IsNull() {
		t.Fatalf("struct round-trip = %+v (%v)", gotS, err)
	}
	if id, _ := gotS.Fields[0].Value.Int64(); id != 42 {
		t.Fatalf("struct.id = %d", id)
	}
	// MAP and empty MAP.
	gotM, err := row[5].Map()
	if err != nil || len(gotM.Entries) != 2 || !gotM.Entries[1].Value.IsNull() {
		t.Fatalf("map round-trip = %+v (%v)", gotM, err)
	}
	if k0, _ := gotM.Entries[0].Key.Varchar(); k0 != "k1" {
		t.Fatalf("map[0].Key = %q", k0)
	}
	if v0, _ := gotM.Entries[0].Value.Int32(); v0 != 100 {
		t.Fatalf("map[0].Value = %d", v0)
	}
	if row[6].IsNull() {
		t.Fatal("empty map became NULL")
	}
	if gotEM, err := row[6].Map(); err != nil || len(gotEM.Entries) != 0 {
		t.Fatalf("empty map = %+v (%v)", gotEM, err)
	}

	// Use LIST parameter inside a filter predicate (list_contains).
	filterList, _ := duckdb.NewListValue(duckdb.ScalarType(duckdb.TypeBigint), []duckdb.Value{
		duckdb.Int64Value(2),
		duckdb.Int64Value(4),
		duckdb.Int64Value(6),
	})
	_, cntRow := querySingleRow(t, conn, "SELECT count(*) FROM range(10) t(i) WHERE list_contains(?, i)", filterList)
	if cnt, err := cntRow[0].Int64(); err != nil || cnt != 3 {
		t.Fatalf("list_contains filter count = %d (%v), want 3", cnt, err)
	}

	// Re-bind a decoded composite Value directly back into a new Query.
	_, rebindRow := querySingleRow(t, conn, "SELECT ?", row[4])
	gotS2, err := rebindRow[0].Struct()
	if err != nil || len(gotS2.Fields) != 3 || gotS2.Fields[0].Name != "id" {
		t.Fatalf("rebound struct = %+v (%v)", gotS2, err)
	}
}

func TestNativeNamedParameters(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()
	db, err := lib.Open(ctx, ":memory:", duckdb.Option{Name: "threads", Value: "2"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare(ctx, "SELECT $tenant AS t1, $min_id AS m, $tenant AS t2, list_contains($ids, $min_id) AS hit")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	names, err := stmt.ParamNames()
	if err != nil {
		t.Fatalf("ParamNames: %v", err)
	}
	if len(names) != 3 {
		t.Fatalf("ParamNames = %v, want 3 unique named params", names)
	}

	ids, _ := duckdb.NewListValue(duckdb.ScalarType(duckdb.TypeBigint), []duckdb.Value{
		duckdb.Int64Value(5),
		duckdb.Int64Value(10),
	})
	// Pass named arguments out of declaration order.
	rows, err := stmt.QueryNamed(ctx,
		duckdb.Named("ids", ids),
		duckdb.Named("min_id", duckdb.Int64Value(10)),
		duckdb.Named("tenant", duckdb.VarcharValue("acme")),
	)
	if err != nil {
		t.Fatalf("QueryNamed: %v", err)
	}
	if !rows.Next() {
		t.Fatalf("expected 1 row, err=%v", rows.Err())
	}
	vals, err := rows.DecodeRow(nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()

	if t1, _ := vals[0].Varchar(); t1 != "acme" {
		t.Fatalf("t1 = %q", t1)
	}
	if m, _ := vals[1].Int64(); m != 10 {
		t.Fatalf("m = %d", m)
	}
	if t2, _ := vals[2].Varchar(); t2 != "acme" {
		t.Fatalf("t2 = %q", t2)
	}
	if hit, _ := vals[3].Bool(); !hit {
		t.Fatal("expected hit=true")
	}

	// Missing and unknown named parameters fail with ErrInvalidInput.
	if _, err := stmt.QueryNamed(ctx, duckdb.Named("tenant", duckdb.VarcharValue("acme"))); !errors.Is(err, duckdb.ErrInvalidInput) {
		t.Fatalf("missing named param = %v, want ErrInvalidInput", err)
	}
	if _, err := stmt.QueryNamed(ctx,
		duckdb.Named("tenant", duckdb.VarcharValue("acme")),
		duckdb.Named("min_id", duckdb.Int64Value(1)),
		duckdb.Named("ids", ids),
		duckdb.Named("extra", duckdb.Int64Value(0)),
	); !errors.Is(err, duckdb.ErrInvalidInput) {
		t.Fatalf("unknown named param = %v, want ErrInvalidInput", err)
	}

	// ExecNamed + QueryNamed on Conn directly.
	if _, err := conn.Exec(ctx, "CREATE TEMP TABLE t_named (id BIGINT, name VARCHAR)"); err != nil {
		t.Fatal(err)
	}
	res, err := conn.ExecNamed(ctx, "INSERT INTO t_named VALUES ($id, $name)",
		duckdb.Named("name", duckdb.VarcharValue("alpha")),
		duckdb.Named("id", duckdb.Int64Value(7)),
	)
	if err != nil || res.RowsAffected != 1 {
		t.Fatalf("ExecNamed = %+v (%v)", res, err)
	}
	rows2, err := conn.QueryNamed(ctx, "SELECT name FROM t_named WHERE id = $id", duckdb.Named("id", duckdb.Int64Value(7)))
	if err != nil {
		t.Fatal(err)
	}
	defer rows2.Close()
	if !rows2.Next() {
		t.Fatalf("expected row, err=%v", rows2.Err())
	}
	v2, err := rows2.DecodeRow(nil)
	if err != nil {
		t.Fatal(err)
	}
	if name, _ := v2[0].Varchar(); name != "alpha" {
		t.Fatalf("got name=%q, want alpha", name)
	}
}

func TestNativeQueryProgressAndProfilingInfo(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()
	db, err := lib.Open(ctx, ":memory:", duckdb.Option{Name: "threads", Value: "2"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// ProfilingInfo returns (nil, nil) before profiling is enabled.
	if info, err := conn.ProfilingInfo(); err != nil || info != nil {
		t.Fatalf("disabled ProfilingInfo = (%+v, %v), want (nil, nil)", info, err)
	}

	// Idle QueryProgress succeeds cleanly.
	idleProg, err := conn.QueryProgress()
	if err != nil {
		t.Fatalf("idle QueryProgress: %v", err)
	}
	if idleProg.Percentage >= 0 {
		t.Fatalf("idle QueryProgress.Percentage = %v, expected < 0", idleProg.Percentage)
	}

	// Enable profiling and poll QueryProgress concurrently during a query.
	if _, err := conn.Exec(ctx, "SET enable_profiling = 'no_output'"); err != nil {
		t.Fatal(err)
	}
	var stopPolling atomic.Bool
	pollDone := make(chan error, 1)
	go func() {
		for !stopPolling.Load() {
			if _, err := conn.QueryProgress(); err != nil {
				pollDone <- err
				return
			}
			time.Sleep(time.Millisecond)
		}
		pollDone <- nil
	}()

	_, sumRow := querySingleRow(t, conn, "SELECT sum(i) FROM range(200000) t(i)")
	stopPolling.Store(true)
	if err := <-pollDone; err != nil {
		t.Fatalf("concurrent QueryProgress failed: %v", err)
	}
	if sum, err := sumRow[0].AsInt64(); err != nil || sum != 19999900000 {
		t.Fatalf("sum = %d (%v)", sum, err)
	}

	// ProfilingInfo returns an owned snapshot tree with metrics and child operators.
	info, err := conn.ProfilingInfo()
	if err != nil {
		t.Fatalf("ProfilingInfo: %v", err)
	}
	if info == nil {
		t.Fatal("expected non-nil ProfilingInfo after enabling profiling")
	}
	if len(info.Metrics) == 0 {
		t.Fatalf("expected root profiling metrics, got empty map")
	}
	if len(info.Children) == 0 {
		t.Fatalf("expected operator children in profiling tree, got 0")
	}

	// ProfilingMetric fetches a single root metric cleanly and returns (false, nil) for unknown keys.
	var sampleKey string
	for k := range info.Metrics {
		sampleKey = k
		break
	}
	if val, ok, err := conn.ProfilingMetric(sampleKey); err != nil || !ok || val != info.Metrics[sampleKey] {
		t.Fatalf("ProfilingMetric(%q) = (%q, %v, %v), want %q", sampleKey, val, ok, err, info.Metrics[sampleKey])
	}
	if val, ok, err := conn.ProfilingMetric("definitely_not_a_metric_key"); err != nil || ok || val != "" {
		t.Fatalf("ProfilingMetric(unknown) = (%q, %v, %v), want ('', false, nil)", val, ok, err)
	}

	// Rows.StatementType reports SELECT and updates across NextResultSet (SELECT -> EXPLAIN).
	stRows, err := conn.QueryScript(ctx, "SELECT 1; EXPLAIN SELECT 1;")
	if err != nil {
		t.Fatalf("QueryScript statement types: %v", err)
	}
	if got := stRows.StatementType(); got != duckdb.StatementTypeSelect {
		_ = stRows.Close()
		t.Fatalf("rs1 StatementType = %v, want SELECT", got)
	}
	if !stRows.NextResultSet() {
		_ = stRows.Close()
		t.Fatalf("NextResultSet to EXPLAIN failed: %v", stRows.Err())
	}
	if got := stRows.StatementType(); got != duckdb.StatementTypeExplain {
		_ = stRows.Close()
		t.Fatalf("rs2 StatementType = %v, want EXPLAIN", got)
	}
	_ = stRows.Close()
	if got := stRows.StatementType(); got != duckdb.StatementTypeExplain {
		t.Fatalf("post-close StatementType = %v, want EXPLAIN", got)
	}

	// Snapshot remains readable after closing the connection.
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if len(info.Metrics) == 0 || len(info.Children) == 0 {
		t.Fatal("profiling snapshot lost data after Conn.Close")
	}
}
