package duckdb_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
)

func TestNativeInstanceCacheSharedFileDB(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()

	cache, err := lib.NewInstanceCache()
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	dbPath := filepath.Join(t.TempDir(), "shared.duckdb")
	bc, err := duckdb.NewBudgetController(duckdb.GlobalBudget{
		MaxConcurrent:    4,
		TotalMemoryBytes: 512 << 20,
		TotalThreads:     4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()

	// Open two Database handles to the exact same on-disk file through InstanceCache
	// with matching configuration; mismatched options must fail cleanly.
	cfg := duckdb.InstanceConfig{
		NativeMemoryBytes: 128 << 20,
		Threads:           2,
	}
	db1, err := bc.OpenCached(ctx, cache, dbPath, cfg)
	if err != nil {
		t.Fatalf("db1 OpenCached: %v", err)
	}
	if _, err := cache.Open(ctx, dbPath, duckdb.Option{Name: "threads", Value: "1"}); err == nil {
		_ = db1.Close()
		t.Fatal("expected error when opening cached DB with mismatched config")
	}
	db2, err := bc.OpenCached(ctx, cache, dbPath, cfg)
	if err != nil {
		_ = db1.Close()
		t.Fatalf("db2 OpenCached: %v", err)
	}

	c1, err := db1.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := db2.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := c1.Exec(ctx, "CREATE TABLE shared_t (id BIGINT, name VARCHAR)"); err != nil {
		t.Fatal(err)
	}
	if _, err := c1.Exec(ctx, "INSERT INTO shared_t VALUES (1, 'from-db1')"); err != nil {
		t.Fatal(err)
	}

	_, row := querySingleRow(t, c2, "SELECT id, name FROM shared_t")
	if id, _ := row[0].Int64(); id != 1 {
		t.Fatalf("db2 read id = %d, want 1", id)
	}
	if name, _ := row[1].Varchar(); name != "from-db1" {
		t.Fatalf("db2 read name = %q, want from-db1", name)
	}

	if err := c1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
	if bc.Snapshot().ActiveInstances != 0 {
		t.Fatalf("budget snapshot after close = %+v", bc.Snapshot())
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeTxCommitRollbackAndReadOnly(t *testing.T) {
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

	if _, err := conn.Exec(ctx, "CREATE TABLE ledger (id BIGINT, amt BIGINT)"); err != nil {
		t.Fatal(err)
	}

	// Commit persists rows (including Appender inside Tx).
	tx1, err := conn.BeginTx(ctx, duckdb.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.BeginTx(ctx, duckdb.TxOptions{}); !errors.Is(err, duckdb.ErrTxActive) {
		t.Fatalf("nested BeginTx = %v, want ErrTxActive", err)
	}
	if _, err := tx1.ExecNamed(ctx, "INSERT INTO ledger VALUES ($id, $amt)",
		duckdb.Named("id", duckdb.Int64Value(1)),
		duckdb.Named("amt", duckdb.Int64Value(100)),
	); err != nil {
		t.Fatal(err)
	}
	app, err := tx1.NewAppender(ctx, "", "", "ledger")
	if err != nil {
		t.Fatal(err)
	}
	if err := app.AppendRow(duckdb.Int64Value(2), duckdb.Int64Value(200)); err != nil {
		t.Fatal(err)
	}
	if err := app.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Rollback(ctx); !errors.Is(err, duckdb.ErrTxDone) {
		t.Fatalf("Rollback after Commit = %v, want ErrTxDone", err)
	}

	// Rollback discards rows.
	tx2, err := conn.BeginTx(ctx, duckdb.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx2.Exec(ctx, "INSERT INTO ledger VALUES (3, 999)"); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	// Read-only transaction reads committed data and rejects writes.
	txRO, err := conn.BeginTx(ctx, duckdb.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := txRO.Query(ctx, "SELECT count(*), sum(amt) FROM ledger")
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatalf("expected row, err=%v", rows.Err())
	}
	vals, err := rows.DecodeRow(nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if cnt, _ := vals[0].Int64(); cnt != 2 {
		t.Fatalf("committed count = %d, want 2", cnt)
	}
	if sum, _ := vals[1].AsInt64(); sum != 300 {
		t.Fatalf("committed sum = %d, want 300", sum)
	}
	if _, err := txRO.Exec(ctx, "INSERT INTO ledger VALUES (4, 400)"); err == nil {
		t.Fatal("expected write error inside READ ONLY transaction")
	}
	if err := txRO.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestNativeExecAndQueryScript(t *testing.T) {
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

	// Empty / comment-only script succeeds cleanly.
	if res, err := conn.ExecScript(ctx, "  -- nothing here\n  "); err != nil || res.RowsAffected != 0 {
		t.Fatalf("empty ExecScript = (%+v, %v)", res, err)
	}

	// Multi-statement DDL + DML script.
	res, err := conn.ExecScript(ctx, `
		CREATE TABLE events (id BIGINT, tag VARCHAR);
		INSERT INTO events VALUES (1, 'a'), (2, 'b');
		INSERT INTO events VALUES (3, 'c');
	`)
	if err != nil {
		t.Fatalf("ExecScript: %v", err)
	}
	if res.RowsAffected != 3 {
		t.Fatalf("ExecScript RowsAffected = %d, want 3", res.RowsAffected)
	}

	// QueryScript runs preceding statements and streams the final parameterized SELECT.
	rows, err := conn.QueryScript(ctx, `
		INSERT INTO events VALUES (4, 'd');
		SELECT count(*), max(id) FROM events WHERE id >= ?;
	`, duckdb.Int64Value(2))
	if err != nil {
		t.Fatalf("QueryScript: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("expected row, err=%v", rows.Err())
	}
	vals, err := rows.DecodeRow(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cnt, _ := vals[0].Int64(); cnt != 3 {
		t.Fatalf("count = %d, want 3", cnt)
	}
	if mx, _ := vals[1].Int64(); mx != 4 {
		t.Fatalf("max = %d, want 4", mx)
	}
}

func TestNativeAppenderBulkAndComplexTypes(t *testing.T) {
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

	if _, err := conn.ExecScript(ctx, `
		CREATE TYPE status_enum AS ENUM ('pending', 'done');
		CREATE TABLE records (
			id BIGINT PRIMARY KEY,
			note VARCHAR DEFAULT 'default-note',
			amount DECIMAL(18, 4),
			uid UUID,
			tz TIMETZ,
			st status_enum,
			tags INTEGER[],
			meta STRUCT(k VARCHAR, v BIGINT),
			attrs MAP(VARCHAR, INTEGER)
		);
	`); err != nil {
		t.Fatal(err)
	}

	app, err := conn.NewAppender(ctx, "", "", "records")
	if err != nil {
		t.Fatalf("NewAppender: %v", err)
	}
	if cols := app.Columns(); len(cols) != 9 {
		t.Fatalf("appender columns = %d, want 9", len(cols))
	}

	u1, _ := duckdb.ParseUUID("01234567-89ab-cdef-0123-456789abcdef")
	dec1, _ := duckdb.DecimalFromInt64(18, 4, 1234567)
	decVal, _ := duckdb.DecimalValue(dec1)
	tzVal, _ := duckdb.TimeTZValue(duckdb.TimeTZ{Micros: 3600_000000, OffsetSeconds: 8 * 3600})
	enumVal, _ := duckdb.EnumValue(duckdb.EnumType("pending", "done"), 1)
	listVal, _ := duckdb.NewListValue(duckdb.ScalarType(duckdb.TypeInteger), []duckdb.Value{
		duckdb.Int32Value(10), duckdb.NullValue(), duckdb.Int32Value(20),
	})
	structVal, _ := duckdb.NewStructValue([]duckdb.StructFieldValue{
		{Name: "k", Value: duckdb.VarcharValue("env")},
		{Name: "v", Value: duckdb.Int64Value(7)},
	})
	mapVal, _ := duckdb.NewMapValue(duckdb.ScalarType(duckdb.TypeVarchar), duckdb.ScalarType(duckdb.TypeInteger), []duckdb.MapEntry{
		{Key: duckdb.VarcharValue("a"), Value: duckdb.Int32Value(1)},
		{Key: duckdb.VarcharValue("b"), Value: duckdb.NullValue()},
	})

	// Row 1: full complex types + DefaultValue() on note column.
	if err := app.AppendRow(
		duckdb.Int64Value(1),
		duckdb.DefaultValue(),
		decVal,
		duckdb.UUIDValue(u1),
		tzVal,
		enumVal,
		listVal,
		structVal,
		mapVal,
	); err != nil {
		t.Fatalf("AppendRow(1): %v", err)
	}

	// Bulk 3000 rows with NULLs across chunk boundaries (> 2048).
	for i := int64(2); i <= 3001; i++ {
		if err := app.AppendRow(
			duckdb.Int64Value(i),
			duckdb.VarcharValue("bulk"),
			duckdb.NullValue(),
			duckdb.NullValue(),
			duckdb.NullValue(),
			duckdb.NullValue(),
			duckdb.NullValue(),
			duckdb.NullValue(),
			duckdb.NullValue(),
		); err != nil {
			t.Fatalf("AppendRow(%d): %v", i, err)
		}
	}
	if err := app.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Clear discards unflushed buffered row.
	if err := app.AppendRow(
		duckdb.Int64Value(99999),
		duckdb.VarcharValue("discarded"),
		duckdb.NullValue(),
		duckdb.NullValue(),
		duckdb.NullValue(),
		duckdb.NullValue(),
		duckdb.NullValue(),
		duckdb.NullValue(),
		duckdb.NullValue(),
	); err != nil {
		t.Fatal(err)
	}
	if err := app.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}

	// Column subset via AddColumn (note receives SQL DEFAULT).
	if err := app.AddColumn("id"); err != nil {
		t.Fatalf("AddColumn: %v", err)
	}
	if cols := app.Columns(); len(cols) != 1 {
		t.Fatalf("subset columns = %d, want 1", len(cols))
	}
	if err := app.AppendRow(duckdb.Int64Value(3002)); err != nil {
		t.Fatalf("subset AppendRow: %v", err)
	}
	if err := app.ClearColumns(); err != nil {
		t.Fatalf("ClearColumns: %v", err)
	}
	if err := app.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify count and complex row 1 + subset row 3002.
	_, cntRow := querySingleRow(t, conn, "SELECT count(*) FROM records")
	if cnt, _ := cntRow[0].Int64(); cnt != 3002 {
		t.Fatalf("total rows = %d, want 3002", cnt)
	}
	_, r1 := querySingleRow(t, conn, "SELECT note, amount, uid, tz, st, tags, meta, attrs FROM records WHERE id = 1")
	if note, _ := r1[0].Varchar(); note != "default-note" {
		t.Fatalf("row1 note = %q, want default-note", note)
	}
	if d, _ := r1[1].Decimal(); d != dec1 {
		t.Fatalf("row1 decimal = %+v, want %+v", d, dec1)
	}
	if u, _ := r1[2].UUID(); u != u1 {
		t.Fatalf("row1 uuid = %v, want %v", u, u1)
	}
	if tz, _ := r1[3].TimeTZ(); tz != (duckdb.TimeTZ{Micros: 3600_000000, OffsetSeconds: 8 * 3600}) {
		t.Fatalf("row1 tz = %+v", tz)
	}
	if e, _ := r1[4].Enum(); e != (duckdb.Enum{Index: 1, Label: "done"}) {
		t.Fatalf("row1 enum = %+v", e)
	}
	if lv, _ := r1[5].List(); len(lv.Elements) != 3 || !lv.Elements[1].IsNull() {
		t.Fatalf("row1 list = %+v", lv)
	}
	if sv, _ := r1[6].Struct(); len(sv.Fields) != 2 || sv.Fields[0].Name != "k" {
		t.Fatalf("row1 struct = %+v", sv)
	}
	if mv, _ := r1[7].Map(); len(mv.Entries) != 2 || !mv.Entries[1].Value.IsNull() {
		t.Fatalf("row1 map = %+v", mv)
	}
	_, r3002 := querySingleRow(t, conn, "SELECT note FROM records WHERE id = 3002")
	if note, _ := r3002[0].Varchar(); note != "default-note" {
		t.Fatalf("row3002 note = %q, want default-note", note)
	}

	// Duplicate primary key surfaces native error on Close and leaves Conn usable.
	appDup, err := conn.NewAppender(ctx, "", "", "records")
	if err != nil {
		t.Fatal(err)
	}
	if err := appDup.AddColumn("id"); err != nil {
		t.Fatal(err)
	}
	_ = appDup.AppendRow(duckdb.Int64Value(1))
	if err := appDup.Close(); err == nil {
		t.Fatal("expected primary key violation error on Appender.Close")
	}
	_, checkRow := querySingleRow(t, conn, "SELECT count(*) FROM records")
	if cnt, _ := checkRow[0].Int64(); cnt != 3002 {
		t.Fatalf("post-error count = %d, want 3002", cnt)
	}
}
