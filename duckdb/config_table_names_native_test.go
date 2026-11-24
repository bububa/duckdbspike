package duckdb_test

import (
	"context"
	"slices"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
)

func TestNativeScriptsTableNamesConnectionIDAndConfigOption(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()

	db, err := lib.Open(ctx, ":memory:", duckdb.Option{Name: "threads", Value: "2"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	conn1, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	conn2, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	// 1. ConnectionID distinct across connections.
	id1, err := conn1.ConnectionID()
	if err != nil {
		t.Fatalf("conn1.ConnectionID: %v", err)
	}
	id2, err := conn2.ConnectionID()
	if err != nil {
		t.Fatalf("conn2.ConnectionID: %v", err)
	}
	if id1 == id2 {
		t.Fatalf("expected distinct connection IDs, both were %d", id1)
	}

	// 2. ConfigOption & generic ConfigOptionAs[T] strongly-typed decoding.
	threadsOpt, ok, err := conn1.ConfigOption("threads")
	if err != nil || !ok {
		t.Fatalf("ConfigOption(threads) = (%+v, %v, %v)", threadsOpt, ok, err)
	}
	threads, err := duckdb.ValueAs[int64](threadsOpt.Value)
	if err != nil || threads != 2 {
		t.Fatalf("threads = %d (kind=%s err=%v), want 2", threads, threadsOpt.Value.Kind(), err)
	}

	mem, scope, ok, err := duckdb.ConfigOptionAs[string](conn1, "memory_limit")
	if err != nil || !ok || mem == "" || scope == duckdb.ConfigOptionScopeInvalid {
		t.Fatalf("ConfigOptionAs(memory_limit) = (%q, %s, %v, %v)", mem, scope, ok, err)
	}

	if _, err := conn1.Exec(ctx, "SET enable_object_cache = true"); err != nil {
		t.Fatal(err)
	}
	objCache, objScope, ok, err := duckdb.ConfigOptionAs[bool](conn1, "enable_object_cache")
	if err != nil || !ok || !objCache {
		t.Fatalf("ConfigOptionAs(enable_object_cache) = (%v, %s, %v, %v)", objCache, objScope, ok, err)
	}

	if _, ok, err := conn1.ConfigOption("definitely_nonexistent_option_xyz"); err != nil || ok {
		t.Fatalf("missing ConfigOption = (%v, %v), want (false, nil)", ok, err)
	}

	// 3. ExecScript with positional args, ExecScriptNamed, QueryScriptNamed.
	res, err := conn1.ExecScript(ctx, `
		CREATE TABLE script_items (id INTEGER, label VARCHAR);
		INSERT INTO script_items VALUES (?, ?);
	`, duckdb.Int32Value(1), duckdb.VarcharValue("alpha"))
	if err != nil {
		t.Fatalf("ExecScript with args: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("ExecScript RowsAffected = %d, want 1", res.RowsAffected)
	}

	if _, err := conn1.ExecScriptNamed(ctx, `
		CREATE TEMP TABLE script_tmp (k VARCHAR);
		INSERT INTO script_items VALUES ($id, $label);
	`, duckdb.Named("id", duckdb.Int32Value(2)), duckdb.Named("label", duckdb.VarcharValue("beta"))); err != nil {
		t.Fatalf("ExecScriptNamed: %v", err)
	}

	rows, err := conn1.QueryScriptNamed(ctx, `
		SET threads = 2;
		SELECT label FROM script_items WHERE id = $target;
	`, duckdb.Named("target", duckdb.Int32Value(2)))
	if err != nil {
		t.Fatalf("QueryScriptNamed: %v", err)
	}
	if !rows.Next() {
		err = rows.Err()
		_ = rows.Close()
		t.Fatalf("QueryScriptNamed returned no rows: %v", err)
	}
	vals, err := rows.DecodeRow(nil)
	_ = rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := vals[0].Varchar(); got != "beta" {
		t.Fatalf("QueryScriptNamed label = %q, want beta", got)
	}

	// 4. TableNames (unqualified & qualified).
	names, err := conn1.TableNames(ctx, `
		WITH cte AS (SELECT id FROM script_items)
		SELECT c.id FROM cte c JOIN other_tbl o ON c.id = o.id
	`, false)
	if err != nil {
		t.Fatalf("TableNames(unqualified): %v", err)
	}
	slices.Sort(names)
	if !slices.Equal(names, []string{"other_tbl", "script_items"}) {
		t.Fatalf("TableNames = %v, want [other_tbl script_items]", names)
	}
	qnames, err := conn1.TableNames(ctx, `SELECT * FROM mycat.myschema.events`, true)
	if err != nil {
		t.Fatalf("TableNames(qualified): %v", err)
	}
	if len(qnames) != 1 || qnames[0] != "mycat.myschema.events" {
		t.Fatalf("TableNames(qualified) = %v", qnames)
	}
}
