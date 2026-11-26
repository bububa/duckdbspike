# DuckDB Spike

[![Go Reference](https://pkg.go.dev/badge/github.com/bububa/duckdbspike.svg)](https://pkg.go.dev/github.com/bububa/duckdbspike)
[![Go](https://github.com/bububa/duckdbspike/actions/workflows/go.yml/badge.svg)](https://github.com/bububa/duckdbspike/actions/workflows/go.yml)
[![Goreleaser](https://github.com/bububa/duckdbspike/actions/workflows/goreleaser.yml/badge.svg)](https://github.com/bububa/duckdbspike/actions/workflows/goreleaser.yml)
[![GitHub go.mod Go version of a Go module](https://img.shields.io/github/go-mod/go-version/bububa/duckdbspike.svg)](https://github.com/bububa/duckdbspike)
[![GitHub license](https://img.shields.io/github/license/bububa/duckdbspike.svg)](https://github.com/bububa/duckdbspike/blob/master/LICENSE)
[![GitHub release](https://img.shields.io/github/release/bububa/duckdbspike.svg)](https://github.com/bububa/duckdbspike/releases/)

> Pure-Go (`CGO_ENABLED=0`) [`purego`](https://github.com/ebitengine/purego) binding for DuckDB `v1.5.6` with streaming-only queries, cooperative cancellation, exact SQL types (including `VARIANT`), chunk-buffered bulk `Appender`, `database/sql` driver, and `CGO_ENABLED=0` Apache Arrow integration.

See **[USAGE.md](USAGE.md)** for complete code examples and **[DEVELOPMENT.md](DEVELOPMENT.md)** for architecture, testing, and upgrade workflows.

---

## Packages & Dependencies

| Package       | Purpose                                                                                                                                                                                                                                                                                                                     | External Dependencies                 |
| :------------ | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------ |
| `duckdb`      | Core engine API: verified `Load` & `AutoLoad` (cross-platform discovery + SHA-256-verified download), `Database`, `InstanceCache`, `TaskState`/`ExecuteTasks`, `Conn`, `Tx`, `Stmt`, streaming `Rows`/`ArrowRows` (multi-result-set), chunk-buffered `Appender` & `QueryAppender` (incl. `GEOMETRY` & `VARIANT`), pure-Go extensions (`RegisterScalarFunction[Set]`, `RegisterAggregateFunction[Set]`, `RegisterCastFunction`, `RegisterTableFunction`, `AddReplacementScan`, `RegisterConfigOption`, `RegisterLogicalType`), Arrow C Data/Stream interop, Client VFS (`File`), `Catalog` metadata, `BudgetController`, `ReadOnlySession`, `QueryProgress`/`ProfilingInfo`. | `github.com/ebitengine/purego`        |
| `duckdbsql`   | Full `database/sql/driver` adapter (`NewConnector`, `NewReadOnlyConnector`, `Register`, `WithDBInit`/`WithConnInit`, `WithTimeScan`, `Composite[T]`, `sql.Null[T]` + Go `[]T`/`[N]T`/`map[K]V` binding, `driver.RowsNextResultSet`, `RawConn`).                                                                                                                                      | `duckdb` (stdlib `database/sql` only) |
| `duckdbarrow` | Pure-Go (`CGO_ENABLED=0`) `apache/arrow-go/v18` integration (`Query`/`QueryStmt`/`QueryScript` -> `array.RecordReader`, `StmtSchema`, bidirectional `ExtensionType` + C Data `ExportSchema`/`ExportRecordBatch`, `AppendRecordBatch`/`AppendRecordReader`, `RegisterRecordBatch`/`RegisterRecordReader`).                                                                           | `github.com/apache/arrow-go/v18`      |

---

## Comparison with Official `duckdb/duckdb-go/v2`

`duckdbspike` targets Go services that want **zero-cgo builds (`CGO_ENABLED=0`)**, **pinned & SHA-256-verified native library loading**, **strict streaming & memory/cancellation isolation**, and full core SQL/Arrow/`database/sql` ergonomics without sacrificing type exactness.

### Feature Matrix

| Capability                                           | `duckdb/duckdb-go/v2` (Official CGO)                        | `bububa/duckdbspike` (`CGO_ENABLED=0`)                                                                                                                |
| :--------------------------------------------------- | :---------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------- |
| Build model                                          | Requires CGO + C/C++ toolchain                              | **Pure Go (`CGO_ENABLED=0`)** via `purego`                                                                                                            |
| Native library verification                          | Linked at build time                                        | Runtime path/permission guards + **SHA-256 checksum** + pinned `v1.5.6` version check + **`AutoLoad`** cross-platform discovery/download                  |
| Streaming query execution                            | Optional / chunk fetch                                      | **Streaming-only** (`duckdb_pending_prepared_streaming` + `duckdb_result_is_streaming == true`)                                                       |
| Cooperative cancellation                             | `duckdb_interrupt`                                          | Latched cancellation with periodic `duckdb_interrupt` re-assertion & **no-deadline join**                                                             |
| Core SQL types (38/38 runtime `DUCKDB_TYPE`)         | Supported                                                   | Supported (exact types, no lossy float/JSON intermediates; **recursive `VARIANT` decoding + chunk-appender `GEOMETRY`/`VARIANT` writes**)                  |
| `database/sql` driver                                | Built-in (`Connector`, `Composite[T]`, `WithBootQueries`)   | `duckdbsql` (`NewConnector`, `NewReadOnlyConnector`, `WithDBInit`/`WithConnInit`, `Composite[T]`, `sql.Null[T]`, Go `[]T`/`[N]T`/`map[K]V`, **`RowsNextResultSet`**) |
| Bulk `Appender` & `QueryAppender`                    | Chunk-buffered `Appender` + `QueryAppender`                 | Chunk-buffered (2048-row `DataChunk`) `AppendRow` / `AppendRows` (incl. `GEOMETRY` & `VARIANT`), `DefaultValue`, `AddColumn`, `QueryAppender`              |
| Apache Arrow (C Data & C Stream)                     | CGO `arrow/cdata` + `Arrow` / `RegisterView`                | **Pure-Go (`CGO_ENABLED=0`)** `duckdbarrow`: full Arrow C Data (incl. `ExtensionType`) + streaming `duckdb_arrow_scan` + multi-result-set `ArrowRows`      |
| Client VFS (`duckdb_file_system_*`) & `Catalog`      | Not exposed in Go API                                       | **`Conn.OpenFile` (`io.ReadWriteSeeker`)** & **`CatalogTypeName`/`LookupCatalogEntry`**                                                                    |
| External Threading (`duckdb_execute_tasks*`)         | Not exposed in Go API                                       | **`Database.ExecuteTasks`** & **`Database.NewTaskState` (`*TaskState`)** with UAF-safe shutdown join                                                       |
| Resource budgeting & read-only isolation             | Manual SQL / OS limits                                      | Built-in **`BudgetController`**, `DecodeLimits`/`ResultLimits`, and **`ReadOnlySession`**                                                                  |
| **Extensions & Engine Hooks (UDFs/Table/Copy/Cast)** | **Supported** (Scalar/Aggregate/Table/Copy/ReplacementScan) | **Supported (`CGO_ENABLED=0`)**: **`RegisterScalarFunction[Set]`**, **`RegisterAggregateFunction[Set]`**, **`RegisterCastFunction`**, **`RegisterTableFunction`**, **`AddReplacementScan`**, **`RegisterConfigOption`**, **`RegisterLogicalType`**; Copy Functions & Log Storage not yet implemented |

### Important Unimplemented / Intentionally Excluded Features

Please review these explicit boundaries before adopting `duckdbspike`:

1. **Remaining Unimplemented Engine Extension Hooks**:
   - **Supported (`CGO_ENABLED=0`)**: **Scalar Functions & Sets** (`Bind`/`Init`/varargs/volatile/`SpecialNulls`), **Aggregate Functions & Sets** (parallel `GROUP BY` + GC-safe handle-ID state), **Custom Cast Functions** (`CAST`/`TRY_CAST`/implicit cost), **Table Functions** (positional/named/`AnyType`, `ProjectionPushdown`, multi-threaded `MaxThreads` + `LocalInit`), **Replacement Scans**, **Custom Config Options**, and **Custom Logical Types**.
   - **Not yet implemented**: **Custom Copy Functions** (`COPY TO`) and **Custom Log Storage**.
   - _(Note: DuckDB's own built-in/official SQL extensions can still be loaded via SQL `INSTALL`/`LOAD` when external access is enabled.)_
2. **Intentionally Disallowed Native Paths**:
   - **Non-streaming queries (`duckdb_pending_prepared`)** are never used; `Query` always enforces streaming results (`ErrNotStreaming` if DuckDB materializes a result such as `INSERT ... RETURNING`).
   - **Lossy numeric C helpers** (`duckdb_decimal_to_double`, `duckdb_hugeint_to_double`, etc.) are never used; all exact numeric values stay lossless.
   - **Library unloading (`dlclose` / `FreeLibrary`)** is never called; mapped libraries live for the process lifetime.
3. **DuckDB `v1.5.6` Upstream C API Boundaries**:
   - **`GEOMETRY` and `VARIANT` prepared-statement parameter binding**: DuckDB `v1.5.6` C API does not provide `duckdb_create_geometry` or `duckdb_create_variant` constructors. Both types support full decoding **and** bulk chunk `Appender` / `QueryAppender` writes, while direct SQL parameter binding returns `ErrUnsupportedType` (use SQL casts such as `ST_GeomFromWKB(?::BLOB)` / `CAST(? AS VARIANT)` or `NewQueryAppender`).
   - **`duckdb_arrow_scan` / `duckdb_arrow_array_scan` projection-pushdown quirk**: In DuckDB `v1.5.6`, querying a registered Arrow view while skipping leading columns (e.g. `SELECT col2 FROM arrow_view`) can crash inside DuckDB's C++ `FactoryGetNext`. Project all columns from Arrow views, or ingest into a temporary table via `duckdbarrow.AppendRecordReader` for arbitrary column projections.
4. **Platform Gate (`darwin/amd64` unsupported)**:
   - Only platforms with recorded real-native acceptance runs are allowed at load time (`darwin/arm64`, `linux/amd64`, `linux/arm64`, `windows/amd64`). Unverified platforms fail with `ErrUnsupportedPlatform`.

---

## Approved Native Artifact & Platform Matrix

| Platform        | Status                              | Artifact                                        | SHA-256 (shared library)                                           | License |
| --------------- | ----------------------------------- | ----------------------------------------------- | ------------------------------------------------------------------ | ------- |
| `darwin/arm64`  | **Accepted** (real native verified) | DuckDB `v1.5.6` (`libduckdb-osx-universal.zip`) | `75a519cb914ae36ca22cff0995a8a2e8c9a46990b8c0a479514f741a0d93f344` | MIT     |
| `linux/amd64`   | **Accepted** (real native verified) | DuckDB `v1.5.6` (`libduckdb-linux-amd64.zip`)   | `3f4962cdd266eab2b479a5b5a4a6d063d279b2954ae814ed00385d553b747633` | MIT     |
| `linux/arm64`   | **Accepted** (real native verified) | DuckDB `v1.5.6` (`libduckdb-linux-arm64.zip`)   | `23686593dba4ec22c6e581bb1c843b3964e84c4bc4d0063d8b4daddd2e20e30a` | MIT     |
| `windows/amd64` | **Accepted** (real native verified) | DuckDB `v1.5.6` (`libduckdb-windows-amd64.zip`) | `7e90bdef028d57b45490d6f53530e3c012a5d9abdc4c6723449200663dc84ed0` | MIT     |
| `darwin/amd64`  | Unsupported (unverified)            | —                                               | —                                                                  | —       |

Platforms without a real native acceptance record are rejected at load time with `ErrUnsupportedPlatform`. Cross-compilation is not acceptance.

---

## Guarantees & Explicit Boundaries

- **Streaming only**: `Query` uses `duckdb_pending_prepared_streaming` (isolated in `internal/abi`, pinned to `v1.5.6`) and asserts `duckdb_result_is_streaming == true`.
- **Exact types & owned Go memory**: Signed/unsigned integers, 128-bit `Hugeint`/`Uhugeint`, all four physical widths of `Decimal`, `Varchar`/`Blob`/`Bit`/`Bignum`/`UUID`/`Enum`/`Geometry`, unit-preserving temporal types (including `±infinity`), `List`/`Array`/`Struct`/`Map`/`Union`, and recursive `VARIANT` values are decoded into Go-owned memory that never aliases native buffers. Unsupported logical types fail at prepare/result inspection even when all rows are `NULL` or empty.
- **Cooperative native cancellation & no-deadline join**: Cancellation latches and re-asserts `duckdb_interrupt` (surviving DuckDB's `InitialCleanup` interrupt reset) until the native scope exits. `Rows.Close`, `ArrowRows.Close`, `Conn.Close`, and `Database.Close` interrupt in-flight work and join all native calls and cancel executors before releasing or reusing handles.
- **Read-only session isolation**: `SessionWorkspace.OpenReadOnly` seals input Parquet files and the temporary catalog read-only, checkpoints and closes the write instance, reopens `READ_ONLY`, and applies `allowed_paths`, `enable_external_access=false`, and `lock_configuration=true`. Two DuckDB engine residuals remain (`CREATE TEMP TABLE` in memory and `COPY TO spill/`), so `ReadOnlySession` is intended for trusted, parameterized internal query templates.

---

## Quick Links

- **[USAGE.md](USAGE.md)** — Practical examples for `duckdb`, `duckdbsql`, `duckdbarrow`, `Appender`, `VARIANT`, Client VFS, `Catalog`, `BudgetController`, and `ReadOnlySession`.
- **[DEVELOPMENT.md](DEVELOPMENT.md)** — Repository layout, build/test commands, CI matrix, and `abi.PinnedVersion` upgrade procedure.

## Native Upgrade Contract

Upgrading `abi.PinnedVersion` requires re-running the full real-native matrix on every accepted platform:

```sh
DUCKDB_HEADER_DIR=/path/to/headers \
DUCKDB_LIB_PATH=/path/to/libduckdb.dylib \
DUCKDB_LIB_SHA256=<sha256> \
CGO_ENABLED=0 go test -count=1 ./...

DUCKDB_HEADER_DIR=/path/to/headers \
DUCKDB_LIB_PATH=/path/to/libduckdb.dylib \
DUCKDB_LIB_SHA256=<sha256> \
go test -race -count=1 ./...
```
