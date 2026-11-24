package duckdbsql_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
	"github.com/bububa/duckdbspike/duckdbsql"
)

func TestNativeP4aMapArrayBindingsEnumValuerAndNextResultSet(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()

	connector, err := duckdbsql.NewConnector(lib, ":memory:?threads=2",
		duckdbsql.WithDBInit(func(ctx context.Context, conn *duckdb.Conn) error {
			_, err := conn.Exec(ctx, `CREATE TYPE color AS ENUM ('red', 'green', 'blue')`)
			return err
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	// 1. Go map[K]V and [N]T direct parameter binding (including empty map, nil map, nested map/array, struct array).
	type Pair struct {
		X int32  `db:"x"`
		S string `db:"s"`
	}
	inMap := map[string]int64{"b": 2, "a": 1}
	emptyMap := map[string]int64{}
	var nilMap map[string]int64
	nestedMap := map[string][]int32{"k1": {10, 20}}
	inArr := [3]int32{7, 8, 9}
	inStructArr := [2]Pair{{X: 1, S: "one"}, {X: 2, S: "two"}}

	var (
		gotMap       duckdb.MapValue
		gotEmptyMap  duckdb.MapValue
		gotNilMap    any
		gotNestedMap duckdb.MapValue
		gotArr       duckdb.ArrayValue
		gotStructArr duckdb.ArrayValue
	)
	if err := db.QueryRowContext(ctx, `SELECT ?, ?, ?, ?, ?, ?`,
		inMap, emptyMap, nilMap, nestedMap, inArr, inStructArr,
	).Scan(&gotMap, &gotEmptyMap, &gotNilMap, &gotNestedMap, &gotArr, &gotStructArr); err != nil {
		t.Fatalf("map/array bind+scan: %v", err)
	}
	// Deterministic sorted key order ("a", "b").
	if len(gotMap.Entries) != 2 {
		t.Fatalf("gotMap len = %d, want 2", len(gotMap.Entries))
	}
	if k0, _ := gotMap.Entries[0].Key.Varchar(); k0 != "a" {
		t.Fatalf("gotMap[0].Key = %q, want a", k0)
	}
	if v1, _ := gotMap.Entries[1].Value.Int64(); v1 != 2 {
		t.Fatalf("gotMap[1].Value = %d, want 2", v1)
	}
	if len(gotEmptyMap.Entries) != 0 {
		t.Fatalf("gotEmptyMap len = %d, want 0", len(gotEmptyMap.Entries))
	}
	if gotNilMap != nil {
		t.Fatalf("gotNilMap = %#v, want nil", gotNilMap)
	}
	if len(gotNestedMap.Entries) != 1 {
		t.Fatalf("gotNestedMap len = %d, want 1", len(gotNestedMap.Entries))
	}
	if lv, err := gotNestedMap.Entries[0].Value.List(); err != nil || len(lv.Elements) != 2 {
		t.Fatalf("gotNestedMap[0].Value = %+v (%v)", lv, err)
	}
	if len(gotArr.Elements) != 3 {
		t.Fatalf("gotArr len = %d, want 3", len(gotArr.Elements))
	}
	if n2, _ := gotArr.Elements[2].Int32(); n2 != 9 {
		t.Fatalf("gotArr[2] = %d, want 9", n2)
	}
	if len(gotStructArr.Elements) != 2 {
		t.Fatalf("gotStructArr len = %d, want 2", len(gotStructArr.Elements))
	}

	// Zero-length Go array rejected with ErrInvalidInput.
	var zeroArr [0]int32
	if err := db.QueryRowContext(ctx, `SELECT ?`, zeroArr).Scan(&gotNilMap); !errors.Is(err, duckdb.ErrInvalidInput) {
		t.Fatalf("zero-length array = %v, want ErrInvalidInput", err)
	}

	// 2. Composite[T] round-trip with Go map[K]V and [N]T fields.
	type Record struct {
		Coords  [3]int32         `db:"coords"`
		Metrics map[string]int32 `db:"metrics"`
	}
	inRec := duckdbsql.NewComposite(Record{
		Coords:  [3]int32{1, 2, 3},
		Metrics: map[string]int32{"cpu": 80, "mem": 64},
	})
	var outRec duckdbsql.Composite[Record]
	if err := db.QueryRowContext(ctx, `SELECT ?`, inRec).Scan(&outRec); err != nil {
		t.Fatalf("Composite map/array round-trip: %v", err)
	}
	if !outRec.Valid() || outRec.Get().Coords != [3]int32{1, 2, 3} || outRec.Get().Metrics["cpu"] != 80 || outRec.Get().Metrics["mem"] != 64 {
		t.Fatalf("Composite round-trip mismatch: %+v", outRec.Get())
	}

	// 3. duckdb.Enum implements driver.Valuer -> works with sql.Null[duckdb.Enum].
	inEnum := sql.Null[duckdb.Enum]{V: duckdb.Enum{Label: "green"}, Valid: true}
	nullEnum := sql.Null[duckdb.Enum]{Valid: false}
	var gotEnum sql.Null[duckdb.Enum]
	var gotNullEnum sql.Null[duckdb.Enum]
	if err := db.QueryRowContext(ctx, `SELECT ?::color, ?::color`, inEnum, nullEnum).Scan(&gotEnum, &gotNullEnum); err != nil {
		t.Fatalf("sql.Null[Enum] round-trip: %v", err)
	}
	if !gotEnum.Valid || gotEnum.V.Label != "green" || gotNullEnum.Valid {
		t.Fatalf("sql.Null[Enum] got (%+v, %+v)", gotEnum, gotNullEnum)
	}

	// 4. driver.RowsNextResultSet via standard sql.Rows.NextResultSet().
	rows, err := db.QueryContext(ctx, `
		CREATE TEMP TABLE rs_t (id INT, name VARCHAR);
		INSERT INTO rs_t VALUES (?, ?), (?, ?);
		SELECT id FROM rs_t ORDER BY id;
		SELECT name, id * 10 AS scaled FROM rs_t WHERE id = ?;
		SELECT count(*)::BIGINT AS total FROM rs_t;
	`, int32(1), "first", int32(2), "second", int32(2))
	if err != nil {
		t.Fatalf("multi-result-set QueryContext: %v", err)
	}
	defer rows.Close()

	// Result set 1: SELECT id
	cols1, err := rows.Columns()
	if err != nil || len(cols1) != 1 || cols1[0] != "id" {
		t.Fatalf("rs1 cols = %v (%v)", cols1, err)
	}
	var ids []int32
	for rows.Next() {
		var id int32
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("rs1 ids = %v, want [1 2]", ids)
	}

	// Result set 2: SELECT name, scaled
	if !rows.NextResultSet() {
		t.Fatalf("expected rs2, err = %v", rows.Err())
	}
	cols2, err := rows.Columns()
	if err != nil || len(cols2) != 2 || cols2[0] != "name" || cols2[1] != "scaled" {
		t.Fatalf("rs2 cols = %v (%v)", cols2, err)
	}
	if !rows.Next() {
		t.Fatalf("rs2 Next() = false, err = %v", rows.Err())
	}
	var name string
	var scaled int32
	if err := rows.Scan(&name, &scaled); err != nil {
		t.Fatal(err)
	}
	if name != "second" || scaled != 20 {
		t.Fatalf("rs2 = (%q, %d), want (second, 20)", name, scaled)
	}

	// Result set 3: SELECT count(*)
	if !rows.NextResultSet() {
		t.Fatalf("expected rs3, err = %v", rows.Err())
	}
	cols3, err := rows.Columns()
	if err != nil || len(cols3) != 1 || cols3[0] != "total" {
		t.Fatalf("rs3 cols = %v (%v)", cols3, err)
	}
	if !rows.Next() {
		t.Fatalf("rs3 Next() = false, err = %v", rows.Err())
	}
	var total int64
	if err := rows.Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Fatalf("rs3 total = %d, want 2", total)
	}
	if rows.NextResultSet() {
		t.Fatal("expected no 4th result set")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err = %v", err)
	}
}
