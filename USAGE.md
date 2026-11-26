# Usage Guide

Practical examples for `github.com/bububa/duckdbspike` (`duckdb`, `duckdbsql`, and `duckdbarrow`). All examples build with `CGO_ENABLED=0` and target DuckDB `v1.5.6`.

## Table of Contents

1. [Loading `libduckdb` & Basic Streaming Queries](#1-loading-libduckdb--basic-streaming-queries)
2. [Exact SQL Types, `VARIANT`, & Generic `ValueAs[T]`](#2-exact-sql-types-variant--generic-valueast)
3. [`database/sql` Driver (`duckdbsql`)](#3-databasesql-driver-duckdbsql)
4. [High-Throughput Bulk Ingestion (`Appender` & `QueryAppender`)](#4-high-throughput-bulk-ingestion-appender--queryappender)
5. [Pure-Go Apache Arrow Pipelines (`duckdbarrow`)](#5-pure-go-apache-arrow-pipelines-duckdbarrow)
6. [Transactions, `InstanceCache`, Multi-Statement Scripts, Observability, VFS & `Catalog`](#6-transactions-instancecache-multi-statement-scripts-observability-vfs--catalog)
7. [Production Safety (`BudgetController`, Limits, Cancellation, & `ReadOnlySession`)](#7-production-safety-budgetcontroller-limits-cancellation--readonlysession)
8. [External Worker Thread Scheduling (`TaskState`)](#8-external-worker-thread-scheduling-taskstate)
9. [Pure-Go Extensions: Scalar, Aggregate, Cast, Table Functions & Replacement Scans](#9-pure-go-extensions-scalar-aggregate-cast-table-functions--replacement-scans)

---

## 1. Loading `libduckdb` & Basic Streaming Queries

`duckdb.Load` verifies platform acceptance, path permissions, SHA-256 checksum, symbol resolution, and DuckDB version (`v1.5.6`) atomically before exposing `*duckdb.Library`.

If you prefer automatic discovery across platform library paths (`DYLD_*` / `LD_LIBRARY_PATH` / `/etc/ld.so.conf` / `PATH` / standard OS dirs) with SHA-256-verified fallback download into a versioned `0700` cache directory, use `duckdb.AutoLoad`:

```go
lib, err := duckdb.AutoLoad(
	ctx,
	"/var/cache/myapp/duckdb", // empty string defaults to os.UserCacheDir()/duckdbspike
	// Optional:
	// duckdb.WithSearchPaths("/opt/custom/lib"),
	// duckdb.WithDownloadBaseURL("https://artifacts.internal/duckdb"),
	// duckdb.WithDisableDownload(true), // strict offline mode
)
```

For explicit manual loading with a pinned path and checksum:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/bububa/duckdbspike/duckdb"
)

func main() {
	// Pin both the library path and its expected SHA-256 hex digest (see README.md).
	lib, err := duckdb.Load(
		"/path/to/libduckdb.dylib",
		"75a519cb914ae36ca22cff0995a8a2e8c9a46990b8c0a479514f741a0d93f344",
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := lib.Open(ctx, ":memory:",
		duckdb.Option{Name: "threads", Value: "2"},
		duckdb.Option{Name: "memory_limit", Value: "512MB"},
	)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	conn, err := db.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.Exec(ctx, `CREATE TABLE users (id BIGINT, name VARCHAR)`); err != nil {
		log.Fatal(err)
	}
	if _, err := conn.ExecNamed(ctx,
		`INSERT INTO users VALUES ($id, $name)`,
		duckdb.Named("id", duckdb.Int64Value(1)),
		duckdb.Named("name", duckdb.VarcharValue("alice")),
	); err != nil {
		log.Fatal(err)
	}

	rows, err := conn.Query(ctx, `SELECT id, name FROM users WHERE id >= ?`, duckdb.Int64Value(1))
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var buf []duckdb.Value
	for rows.Next() {
		buf, err = rows.DecodeRow(buf)
		if err != nil {
			log.Fatal(err)
		}
		id, _ := buf[0].Int64()
		name, _ := buf[1].Varchar()
		fmt.Printf("id=%d name=%q\n", id, name)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
```

---

## 2. Exact SQL Types, `VARIANT`, & Generic `ValueAs[T]`

All 38 runtime DuckDB types decode losslessly into owned Go values. `VARIANT` is supported read-only with recursive decoding of all 34 `VariantLogicalType` tags, cleanly distinguishing outer SQL `NULL` (`v.IsNull() == true`) from inner `VARIANT_NULL` (`vr.IsNull() == true`).

```go
// Construct exact parameters without float64 intermediates.
dec, _ := duckdb.NewDecimal(10, 2, big.NewInt(12345)) // 123.45
decVal, _ := duckdb.DecimalValue(dec)
uid, _ := duckdb.ParseUUID("00112233-4455-6677-8899-aabbccddeeff")

rows, err := conn.Query(ctx, `
	SELECT
		?::DECIMAL(10, 2) AS amount,
		?::UUID AS uid,
		{'user': 'alice', 'score': 99::INTEGER, 'note': NULL::VARCHAR}::VARIANT AS payload,
		NULL::VARIANT AS sql_null_variant
`, decVal, duckdb.UUIDValue(uid))
if err != nil {
	log.Fatal(err)
}
defer rows.Close()

if rows.Next() {
	vals, err := rows.DecodeRow(nil)
	if err != nil {
		log.Fatal(err)
	}
	amount, _ := duckdb.ValueAs[duckdb.Decimal](vals[0])
	gotUUID, _ := duckdb.ValueAs[duckdb.UUID](vals[1])
	vr, _ := duckdb.ValueAs[duckdb.Variant](vals[2])

	// Inspect recursive VARIANT object payload.
	obj, _ := vr.Value.Struct()
	for _, f := range obj.Fields {
		if f.Value.IsNull() {
			fmt.Printf("field %s = VARIANT_NULL\n", f.Name)
		} else {
			fmt.Printf("field %s (%s)\n", f.Name, f.Value.Kind())
		}
	}

	// Outer SQL NULL VARIANT row vs inner VARIANT_NULL:
	fmt.Printf("amount=%s uuid=%s outerNull=%v\n", amount, gotUUID, vals[3].IsNull())
}
```

> **Note**: File-backed DuckDB databases storing `VARIANT` columns require `duckdb.Option{Name: "storage_compatibility_version", Value: "v1.5.0"}` when opening the database file.

---

## 3. `database/sql` Driver (`duckdbsql`)

`duckdbsql` wraps `*duckdb.Library` with `sql.OpenDB` / `sql.Register`, preserving exact types by default (enable `WithTimeScan(true)` to scan `DATE`/`TIMESTAMP*` into `time.Time`).

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/bububa/duckdbspike/duckdb"
	"github.com/bububa/duckdbspike/duckdbsql"
)

type Address struct {
	City string `db:"city"`
	Zip  *int32 `db:"zip"`
}

type Profile struct {
	Name    string           `db:"name"`
	Coords  [3]int32         `db:"coords"`
	Metrics map[string]int32 `db:"metrics"`
	Addr    Address          `db:"addr"`
}

func exampleSQL(ctx context.Context, lib *duckdb.Library) error {
	connector, err := duckdbsql.NewConnector(
		lib,
		":memory:?threads=2",
		duckdbsql.WithDBInit(func(ctx context.Context, conn *duckdb.Conn) error {
			_, err := conn.Exec(ctx, `CREATE TABLE items (id BIGINT, tags VARCHAR[], meta MAP(VARCHAR, BIGINT))`)
			return err
		}),
		duckdbsql.WithDecodeLimits(duckdb.DefaultDecodeLimits()),
		duckdbsql.WithResultLimits(duckdb.ResultLimits{MaxRows: 100_000}),
	)
	if err != nil {
		return err
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	// Bind native Go slices, fixed arrays, maps, and Composite[T] directly.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO items VALUES (?, ?, ?)`,
		int64(1),
		[]string{"go", "purego"},
		map[string]int64{"views": 42, "likes": 7},
	); err != nil {
		return err
	}

	// Composite[T] and Go 1.22+ sql.Null[T] round-trip exact DuckDB types.
	zip := int32(10001)
	in := duckdbsql.NewComposite(Profile{
		Name:    "alice",
		Coords:  [3]int32{1, 2, 3},
		Metrics: map[string]int32{"cpu": 80},
		Addr:    Address{City: "NYC", Zip: &zip},
	})
	var out duckdbsql.Composite[Profile]
	if err := db.QueryRowContext(ctx, `SELECT ?`, in).Scan(&out); err != nil {
		return err
	}
	fmt.Printf("profile=%+v\n", out.Get())

	// sql.Null[T] works for both parameter binding and scanning on exact duckdb.* types.
	var gotDec sql.Null[duckdb.Decimal]
	inDec := sql.Null[duckdb.Decimal]{
		V:     duckdb.Decimal{Width: 10, Scale: 2, Value: duckdb.HugeintFromInt64(12345)},
		Valid: true,
	}
	if err := db.QueryRowContext(ctx, `SELECT ?`, inDec).Scan(&gotDec); err != nil {
		return err
	}

	// Multi-result-set scripts via standard sql.Rows.NextResultSet().
	rows, err := db.QueryContext(ctx, `
		SELECT count(*)::BIGINT AS cnt FROM items;
		SELECT id FROM items ORDER BY id;
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for {
		cols, _ := rows.Columns()
		for rows.Next() {
			var v int64
			_ = rows.Scan(&v)
			fmt.Printf("cols=%v val=%d\n", cols, v)
		}
		if !rows.NextResultSet() {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Escape hatch to *duckdb.Conn (for Appender, QueryArrow, ProfilingInfo, etc.).
	sqlConn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer sqlConn.Close()

	return duckdbsql.RawConn(ctx, sqlConn, func(raw *duckdb.Conn) error {
		id, err := raw.ConnectionID()
		if err != nil {
			return err
		}
		fmt.Printf("duckdb connection_id=%d\n", id)
		return nil
	})
}
```

---

## 4. High-Throughput Bulk Ingestion (`Appender` & `QueryAppender`)

`Appender` internally buffers up to `duckdb_vector_size()` (2,048) rows in a reusable native `DataChunk` and flushes via `duckdb_append_data_chunk`. Use `AppendRows` to amortize lock acquisition across batches, and `NewQueryAppender` for bulk `ON CONFLICT DO UPDATE` / `MERGE INTO` upserts.

```go
// 1. Direct table Appender with AddColumn + DefaultValue + batch AppendRows.
if _, err := conn.Exec(ctx, `CREATE TABLE metrics (ts BIGINT, name VARCHAR, source VARCHAR DEFAULT 'agent')`); err != nil {
	return err
}
app, err := conn.NewAppender(ctx, "", "", "metrics")
if err != nil {
	return err
}
if err := app.AddColumn("ts"); err != nil {
	_ = app.Close()
	return err
}
if err := app.AddColumn("name"); err != nil {
	_ = app.Close()
	return err
}

batch := make([][]duckdb.Value, 10_000)
for i := range batch {
	batch[i] = []duckdb.Value{
		duckdb.Int64Value(int64(i)),
		duckdb.VarcharValue("cpu_usage"),
	}
}
if err := app.AppendRows(batch); err != nil {
	_ = app.Close()
	return err
}
if err := app.Close(); err != nil {
	return err
}

// 2. QueryAppender for bulk UPSERT (ON CONFLICT DO UPDATE).
if _, err := conn.Exec(ctx, `CREATE TABLE kv (k VARCHAR PRIMARY KEY, v BIGINT)`); err != nil {
	return err
}
qapp, err := conn.NewQueryAppender(
	ctx,
	`INSERT INTO kv SELECT k, v FROM staged ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v`,
	[]duckdb.Column{
		{Name: "k", Type: duckdb.ScalarType(duckdb.TypeVarchar)},
		{Name: "v", Type: duckdb.ScalarType(duckdb.TypeBigint)},
	},
	"staged",
)
if err != nil {
	return err
}
_ = qapp.AppendRow(duckdb.VarcharValue("a"), duckdb.Int64Value(1))
_ = qapp.AppendRow(duckdb.VarcharValue("a"), duckdb.Int64Value(2))
if err := qapp.Close(); err != nil {
	return err
}
```

---

## 5. Pure-Go Apache Arrow Pipelines (`duckdbarrow`)

`duckdbarrow` integrates with `github.com/apache/arrow-go/v18` with **`CGO_ENABLED=0`** (no `arrow/cdata` CGO dependency). `Query`, `QueryNamed`, `QueryScript`, `RegisterRecordBatch`, and `RegisterRecordReader` accept either `*duckdb.Conn` or `*duckdb.Tx`.

```go
package main

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/bububa/duckdbspike/duckdb"
	"github.com/bububa/duckdbspike/duckdbarrow"
)

func exampleArrow(ctx context.Context, conn *duckdb.Conn) error {
	alloc := memory.DefaultAllocator

	// 1. Inspect a prepared statement's Arrow schema without executing it.
	stmt, err := conn.Prepare(ctx, `SELECT i::BIGINT AS id, (i * 2)::DOUBLE AS val FROM range($n) t(i)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	arrowSchema, err := duckdbarrow.StmtSchema(stmt)
	if err != nil {
		return err
	}
	fmt.Printf("arrow schema: %v\n", arrowSchema)

	// 2. Stream DuckDB query results as an arrow-go array.RecordReader.
	rr, err := duckdbarrow.QueryStmtNamed(ctx, stmt, alloc, duckdb.Named("n", duckdb.Int64Value(1000)))
	if err != nil {
		return err
	}
	defer rr.Release()

	for rr.Next() {
		rec := rr.RecordBatch()
		fmt.Printf("batch rows=%d cols=%d\n", rec.NumRows(), rec.NumCols())
	}
	if err := rr.Err(); err != nil {
		return err
	}

	// 3. Export an arrow.RecordBatch and stream/register it into DuckDB.
	inSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "label", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	rb := array.NewRecordBuilder(alloc, inSchema)
	rb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	rb.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)
	rec := rb.NewRecordBatch()
	rb.Release()
	defer rec.Release()

	// Option A: Bulk ingest into a table via Appender (supports arbitrary downstream SQL projections).
	if _, err := conn.Exec(ctx, `CREATE TEMP TABLE arrow_tbl (id BIGINT, label VARCHAR)`); err != nil {
		return err
	}
	app, err := conn.NewAppender(ctx, "", "", "arrow_tbl")
	if err != nil {
		return err
	}
	if err := duckdbarrow.AppendRecordBatch(app, rec); err != nil {
		_ = app.Close()
		return err
	}
	if err := app.Close(); err != nil {
		return err
	}

	// Option B: Streaming virtual view via duckdb_arrow_scan (RegisterRecordReader).
	// Note: Due to a DuckDB v1.5.6 projection-pushdown quirk, project all columns
	// when scanning Arrow views directly, or materialize into a temp table first.
	memReader, err := array.NewRecordReader(inSchema, []arrow.RecordBatch{rec})
	if err != nil {
		return err
	}
	view, err := duckdbarrow.RegisterRecordReader(ctx, conn, "v_arrow_stream", memReader)
	memReader.Release()
	if err != nil {
		return err
	}
	defer view.Close()

	rows, err := conn.Query(ctx, `SELECT id, label FROM v_arrow_stream ORDER BY id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	return nil
}
```

> **Extension metadata note**: `duckdbarrow.ImportSchema` preserves `ARROW:extension:name` and `ARROW:extension:metadata` inside `arrow.Field.Metadata` while keeping `arrow.Field.Type` as the underlying storage type (e.g., `FixedSizeBinary(16)` for `arrow.uuid`). `ExportSchema` writes `ARROW:extension:*` metadata automatically when exporting `arrow.ExtensionType` fields.

---

## 6. Transactions, `InstanceCache`, Multi-Statement Scripts, Observability, VFS & `Catalog`

```go
// Shared file database across multiple Database handles via InstanceCache.
cache, err := lib.NewInstanceCache()
if err != nil {
	return err
}
defer cache.Close()

db1, err := cache.Open(ctx, "/path/to/app.duckdb", duckdb.Option{Name: "threads", Value: "2"})
if err != nil {
	return err
}
defer db1.Close()

// Explicit transaction with zero-SQL Catalog metadata inspection.
tx, err := conn.BeginTx(ctx, duckdb.TxOptions{ReadOnly: true})
if err != nil {
	return err
}
entry, found, err := tx.LookupCatalogEntry(ctx, "memory", duckdb.CatalogEntryTable, "main", "users")
if err != nil {
	_ = tx.Rollback(ctx)
	return err
}
fmt.Printf("catalog table found=%v entry=%+v\n", found, entry)
_ = tx.Commit(ctx)

// Strongly-typed runtime configuration lookup without SQL.
threads, ok, err := duckdb.ConfigOptionAs[uint64](conn, "threads")
if err == nil && ok {
	fmt.Printf("threads=%d\n", threads)
}

// Profiling snapshot & single root metric lookup.
_, _ = conn.Exec(ctx, `SET enable_profiling = 'no_output'`)
rows, _ := conn.Query(ctx, `SELECT sum(i) FROM range(100000) t(i)`)
for rows.Next() {
	_, _ = rows.DecodeRow(nil)
}
fmt.Printf("statement type=%s\n", rows.StatementType())
_ = rows.Close()

if latency, ok, err := conn.ProfilingMetric("latency"); err == nil && ok {
	fmt.Printf("root latency=%s\n", latency)
}
if tree, err := conn.ProfilingInfo(); err == nil && tree != nil {
	fmt.Printf("root metrics=%d children=%d\n", len(tree.Metrics), len(tree.Children))
}

// Client VFS File (io.ReadWriteSeeker + io.Closer).
f, err := conn.OpenFile(ctx, "/path/to/output.txt", duckdb.FileOpenOptions{
	Write:  true,
	Create: true,
})
if err != nil {
	return err
}
_, _ = f.Write([]byte("written via DuckDB VFS"))
_ = f.Sync()
_ = f.Close()
```

---

## 7. Production Safety (`BudgetController`, Limits, Cancellation, & `ReadOnlySession`)

Use `BudgetController` to cap aggregate memory, threads, spill space, and concurrent queries across instances, and `SessionWorkspace.OpenReadOnly` + `duckdbsql.NewReadOnlyConnector` for isolated read-only Parquet query workloads.

```go
// 1. Global resource admission controller.
bc, err := duckdb.NewBudgetController(duckdb.GlobalBudget{
	MaxConcurrent:    4,
	TotalMemoryBytes: 2 << 30, // 2 GiB
	TotalThreads:     8,
	TotalSpillBytes:  4 << 30, // 4 GiB
})
if err != nil {
	return err
}
defer bc.Close()

// 2. Sealed read-only session over explicit Parquet files inside ws.InputDir().
ws, err := duckdb.NewSessionWorkspace("/var/tmp/duckdb-sessions")
if err != nil {
	return err
}
defer ws.Close()

// Place input files into ws.InputDir() (globs are intentionally rejected).
session, err := ws.OpenReadOnly(ctx, lib, duckdb.ReadOnlySessionSpec{
	Budget: bc,
	Config: duckdb.InstanceConfig{
		NativeMemoryBytes: 512 << 20,
		Threads:           2,
		MaxSpillBytes:     1 << 30,
	},
	Views: []duckdb.ViewSpec{
		{Name: "events", Files: []string{filepath.Join(ws.InputDir(), "events_2026.parquet")}},
	},
})
if err != nil {
	return err
}
defer session.Close()

// Wrap the sealed session in a database/sql pool (Connector.Close does not close session).
roConnector, err := duckdbsql.NewReadOnlyConnector(
	session,
	duckdbsql.WithResultLimits(duckdb.ResultLimits{MaxRows: 50_000, MaxBytes: 64 << 20}),
)
if err != nil {
	return err
}
sqlDB := sql.OpenDB(roConnector)
defer sqlDB.Close()

// Cooperative cancellation via context.Context interrupts DuckDB mid-query and joins cleanly.
qCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
defer cancel()
var count int64
if err := sqlDB.QueryRowContext(qCtx, `SELECT count(*) FROM events`).Scan(&count); err != nil {
	return err
}
```

---

## 8. External Worker Thread Scheduling (`TaskState`)

When opening a database with `external_threads` (where `threads >= external_threads`), Go goroutines can cooperatively execute DuckDB tasks via `Database.NewTaskState` / `Database.ExecuteTasks`. `TaskState.Close` (and `Database.Close`) signals and joins every active worker before destroying the native handle.

```go
db, err := lib.Open(ctx, ":memory:",
	duckdb.Option{Name: "threads", Value: "3"},
	duckdb.Option{Name: "external_threads", Value: "2"},
)
if err != nil {
	return err
}
defer db.Close()

ts, err := db.NewTaskState()
if err != nil {
	return err
}
defer ts.Close() // wakes and joins all worker goroutines before native destroy

var wg sync.WaitGroup
for range 2 {
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = ts.Execute()
	}()
}

// Queries on connections from db will now utilize both external workers.
// ...
ts.Finish()
_ = ts.Close()
wg.Wait()
```

---

## 9. Pure-Go Extensions: Scalar, Aggregate, Cast, Table Functions & Replacement Scans

Pure-Go (`CGO_ENABLED=0`) extensions run on a fixed 17-slot process-wide singleton `purego` callback switchboard (no callback slot exhaustion, GC-safe integer handle IDs, and panic-safe C++/Go boundaries).

```go
// 1. Register a custom runtime config option readable via ConfigOptionAs / SQL SET.
defaultEnv := duckdb.VarcharValue("dev")
if err := conn.RegisterConfigOption(duckdb.ConfigOptionSpec{
	Name:         "app_env",
	Type:         duckdb.ScalarType(duckdb.TypeVarchar),
	DefaultValue: &defaultEnv,
	DefaultScope: duckdb.ConfigOptionScopeSession,
	Description:  "Application environment name",
}); err != nil {
	return err
}
_, _ = conn.Exec(ctx, `SET app_env = 'prod'`)
env, _, _, _ := duckdb.ConfigOptionAs[string](conn, "app_env")
fmt.Printf("app_env=%s\n", env)

// 2. Register a custom aliased logical type usable in DDL and casts.
if err := conn.RegisterLogicalType("user_id", duckdb.ScalarType(duckdb.TypeBigint)); err != nil {
	return err
}
_, _ = conn.Exec(ctx, `CREATE TABLE accounts (id user_id)`)

// 3. Register a pure-Go Table Function (supports named params, AnyType, ProjectionPushdown, MaxThreads + LocalInit).
if err := conn.RegisterTableFunction(duckdb.TableFunctionSpec{
	Name:       "mem_source",
	Parameters: []duckdb.LogicalType{duckdb.ScalarType(duckdb.TypeVarchar)},
	Bind: func(in duckdb.TableFunctionBindInput) (duckdb.TableFunctionBindResult, error) {
		name, _ := in.Args[0].Varchar()
		return duckdb.TableFunctionBindResult{
			Columns: []duckdb.Column{
				{Name: "source", Type: duckdb.ScalarType(duckdb.TypeVarchar)},
				{Name: "seq", Type: duckdb.ScalarType(duckdb.TypeBigint)},
			},
			Cardinality: 3,
			ExactCard:   true,
			BindData:    name,
		}, nil
	},
	Init: func(in duckdb.TableFunctionInitInput) (duckdb.TableFunctionInitResult, error) {
		idx := int64(0)
		return duckdb.TableFunctionInitResult{InitData: &idx}, nil
	},
	Execute: func(bindData, initData, _ any, chunk *duckdb.TableFunctionChunk) error {
		src := bindData.(string)
		idx := initData.(*int64)
		for *idx < 3 && chunk.Len() < chunk.Cap() {
			if err := chunk.AppendRow(duckdb.VarcharValue(src), duckdb.Int64Value(*idx)); err != nil {
				return err
			}
			*idx++
		}
		return nil
	},
}); err != nil {
	return err
}

// 4. Register a Replacement Scan rewriting `SELECT * FROM 'mem://events'` into `mem_source('events')`.
if err := db.AddReplacementScan(func(tableName string) (*duckdb.ReplacementScanResult, error) {
	if strings.HasPrefix(tableName, "mem://") {
		return &duckdb.ReplacementScanResult{
			FunctionName: "mem_source",
			Parameters:   []duckdb.Value{duckdb.VarcharValue(strings.TrimPrefix(tableName, "mem://"))},
		}, nil
	}
	return nil, nil // decline so DuckDB continues normal resolution
}); err != nil {
	return err
}

rows, err := conn.Query(ctx, `SELECT source, seq FROM 'mem://events' ORDER BY seq`)
if err != nil {
	return err
}
defer rows.Close()

// 5. Register a Scalar Function (supports Bind/Init/Varargs/Volatile/SpecialNulls + FunctionSets).
if err := conn.RegisterScalarFunction(duckdb.ScalarFunctionSpec{
	Name:       "double_it",
	Parameters: []duckdb.LogicalType{duckdb.ScalarType(duckdb.TypeBigint)},
	ReturnType: duckdb.ScalarType(duckdb.TypeBigint),
	Execute: func(_, _ any, args []duckdb.Value) (duckdb.Value, error) {
		v, _ := args[0].Int64()
		return duckdb.Int64Value(v * 2), nil
	},
}); err != nil {
	return err
}

// 6. Register an Aggregate Function (parallel GROUP BY safe via handle-ID state).
type sumAcc struct{ sum int64 }
if err := conn.RegisterAggregateFunction(duckdb.AggregateFunctionSpec{
	Name:       "go_sum",
	Parameters: []duckdb.LogicalType{duckdb.ScalarType(duckdb.TypeBigint)},
	ReturnType: duckdb.ScalarType(duckdb.TypeBigint),
	Init:       func() any { return &sumAcc{} },
	Update: func(st any, args []duckdb.Value) error {
		v, _ := args[0].Int64()
		st.(*sumAcc).sum += v
		return nil
	},
	Combine: func(src, dst any) error {
		dst.(*sumAcc).sum += src.(*sumAcc).sum
		return nil
	},
	Finalize: func(st any) (duckdb.Value, error) {
		return duckdb.Int64Value(st.(*sumAcc).sum), nil
	},
}); err != nil {
	return err
}

// 7. Register a Custom Cast Function (supports CAST and TRY_CAST).
_ = conn.RegisterLogicalType("celsius", duckdb.ScalarType(duckdb.TypeDouble))
_ = conn.RegisterLogicalType("fahrenheit", duckdb.ScalarType(duckdb.TypeDouble))
if err := conn.RegisterCastFunction(duckdb.CastFunctionSpec{
	SourceType: duckdb.ScalarType(duckdb.TypeDouble).WithAlias("celsius"),
	TargetType: duckdb.ScalarType(duckdb.TypeDouble).WithAlias("fahrenheit"),
	Cast: func(_ duckdb.CastMode, val duckdb.Value) (duckdb.Value, error) {
		c, _ := val.Float64()
		return duckdb.Float64Value(c*9.0/5.0 + 32.0), nil
	},
}); err != nil {
	return err
}
```
