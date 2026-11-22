# github.com/bububa/duckdbspike

Minimal, `CGO_ENABLED=0` purego binding for a single pinned DuckDB native C API
release, designed for read-only telemetry reporting.

## Approved Native Artifact & Platform Matrix

| Platform | Status | Artifact | SHA-256 (`libduckdb.dylib`) | License |
|---|---|---|---|---|
| `darwin/arm64` | **Accepted** (real native verified) | DuckDB `v1.5.6` (`libduckdb-osx-universal.zip`) | `75a519cb914ae36ca22cff0995a8a2e8c9a46990b8c0a479514f741a0d93f344` | MIT |
| `linux/amd64` | Unsupported (unverified) | — | — | — |
| `linux/arm64` | Unsupported (unverified) | — | — | — |
| `darwin/amd64` | Unsupported (unverified) | — | — | — |
| `windows/amd64` | Unsupported (unverified) | — | — | — |

Platforms without a real native acceptance record are rejected at load time with
`ErrUnsupportedPlatform`. Cross-compilation is not acceptance.

## Guarantees & Explicit Boundaries

- **Streaming only**: `Query` uses `duckdb_pending_prepared_streaming` (deprecated
  upstream, isolated in `internal/abi`, pinned to `v1.5.6`) and asserts
  `duckdb_result_is_streaming == true`. Non-streaming `duckdb_pending_prepared`
  materializes outside `memory_limit` and is never used.
- **Exact types**: No `float64`, string, or JSON intermediate layer. Signed/unsigned
  integers, 128-bit `Hugeint`/`Uhugeint`, all four physical widths of `Decimal`,
  length-aware `Varchar`/`Blob` (preserving embedded NUL and empty vs NULL),
  unit-preserving temporal types, and nested `List`/`Array`/`Struct`/`Map` are
  decoded into owned Go values. Unsupported logical types fail at statement/result
  inspection even if all rows are NULL or empty.
- **Cooperative native cancellation & no-deadline join**: Cancellation latches and
  re-asserts `duckdb_interrupt` (surviving DuckDB's `InitialCleanup` interrupt
  reset) until the native scope exits. `Rows.Close`, `Conn.Close`, and
  `Database.Close` interrupt in-flight work and join all native calls and cancel
  executors before releasing or reusing handles.
- **Read-only session isolation**: `SessionWorkspace.OpenReadOnly` seals input
  Parquet files and the temporary catalog read-only, checkpoints and fully closes
  the write instance, reopens `READ_ONLY`, applies `allowed_paths`,
  `enable_external_access=false`, and `lock_configuration=true`. Two DuckDB engine
  residuals remain (`CREATE TEMP TABLE` in memory and `COPY TO spill/`), so
  `ReadOnlySession` is strictly for trusted, parameterized internal query templates.

## Native Upgrade Contract

Upgrading `abi.PinnedVersion` requires re-running the full real-native matrix on
every accepted platform:

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
