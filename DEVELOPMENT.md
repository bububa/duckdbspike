# Development Guide

Architecture, repository layout, build/test commands, CI matrix, and native upgrade workflow for `github.com/bububa/duckdbspike`.

## Overview

Pure-Go (`CGO_ENABLED=0`) [`purego`](https://github.com/ebitengine/purego) binding for a single pinned DuckDB native C API release (`abi.PinnedVersion`, currently `v1.5.6`), providing streaming-only queries, cooperative cancellation, exact SQL types (including read-only recursive `VARIANT`), chunk-buffered bulk `Appender`, Client VFS & `Catalog` APIs, a `database/sql` driver (`duckdbsql`), and pure-Go Apache Arrow integration (`duckdbarrow`).

- **Go version**: `>= 1.27` (see `go.mod` and `.github/workflows/go.yml`)
- **Direct external dependencies**:
  - `github.com/ebitengine/purego` (core FFI loader & singleton callbacks)
  - `github.com/apache/arrow-go/v18` (used only by package `duckdbarrow`, pure Go / `CGO_ENABLED=0`)

## Repository Layout

- `internal/abi/` — Pinned C ABI definitions, singleton callback dispatchers, and safe loader; owns no native resources.
  - `types.go` — `PinnedVersion`, C struct mirrors (`Result`, `Hugeint`, `Uhugeint`, `Decimal`, `StringT`, `ListEntry`, `Bit`, `Bignum`, `ArrowSchema`, `ArrowArray`, `ArrowArrayStream`, temporal types), and C enum constants.
  - `api.go` — Complete `API` function-pointer table with ownership annotations (`owned` vs `borrowed`) and `symbols()` registry.
  - `arrow_export.go` — 6 process-wide singleton `purego.NewCallback` trampolines (schema/array release + 4 Arrow C Stream Interface callbacks) with handle-ID `sync.Map` dispatch.
  - `load.go` — `Load(path, wantSHA256)`: platform gate, path/permission checks, SHA-256 verification, atomic all-or-nothing symbol resolution, and `PinnedVersion` check.
  - `platform_accepted.go` / `platform_windows_amd64.go` / `platform_other.go` — Build-tag gate (`darwin/arm64`, `linux/amd64`, `linux/arm64`, `windows/amd64`) and native library openers (`purego.Dlopen` on Unix, `syscall.LoadLibrary` on Windows).
  - `perms_unix.go` / `perms_windows.go` — OS-specific library path/permission guards (POSIX `0o022` check on Unix, UNC path rejection on Windows).
  - `abi_test.go` — Symbol coverage check, loader validation tests, singleton callback tests, C header layout probe (`DUCKDB_HEADER_DIR`), and native load test.
- `duckdb/` — Public Go API and resource/safety kernel.
  - `doc.go` — Package docs, sentinel errors, `Error`/`ErrorKind`, `noCopy`.
  - `database.go` / `cache.go` — `Library`, `Database`, `InstanceCache`, `Option`, `Load`, `Open`, `Close`.
  - `conn.go` / `tx.go` / `stmt.go` / `rows.go` / `script.go` — `Conn`, `Tx`, `Stmt`, streaming `Rows` (with `HasNextResultSet`/`NextResultSet` and `StatementType`), and multi-statement script execution with parameter distribution.
  - `bind.go` / `decode.go` / `value_decode.go` / `variant.go` / `types.go` / `scan.go` — Compact 48-byte `Value`, exact typed parameter binding, vector & `duckdb_value` decoding, read-only recursive DuckDB `v1.5.6` `VARIANT` decoder, generic `ValueAs[T]`/`ConfigOptionAs[T]`, `sql.Scanner`/`driver.Valuer`, and `DecodeLimits`.
  - `appender.go` / `appender_chunk.go` — Chunk-buffered (2,048-row `DataChunk`) `Appender` (`AppendRow`, `AppendRows`, `AddColumn`, `ClearColumns`, `AppendArrowChunk`) and `QueryAppender`.
  - `arrow.go` / `arrow_export.go` — Streaming `ArrowRows`, `Stmt.ArrowSchema()`, `QueryScriptArrow`, `NewArrowSchema`/`NewArrowArray`, `RegisterArrowView`, and streaming `RegisterArrowStream` (`ArrowView`).
  - `filesystem.go` / `catalog.go` / `metadata.go` / `observability.go` — Client VFS (`File` implementing `io.ReadWriteSeeker`), `Catalog` lookups, `DescribeTable`/`TableNames`/`ConnectionID`/`ConfigOption`/`ConfigFlags`, `QueryProgress`, `ProfilingInfo`, and `ProfilingMetric`.
  - `cancel.go` — `cancelScope`: latched cancellation with periodic `duckdb_interrupt` re-assertion and no-deadline `join()`.
  - `budget.go` / `session.go` — `InstanceConfig`, `ResultLimits`, `BudgetController`, `SessionWorkspace`, and `ReadOnlySession`.
  - `*_mock_test.go`, `budget_test.go`, `types_test.go` — Deterministic domain-named tests using fake `abi.API` tables (no native library needed).
  - `*_native_test.go` — Domain-named end-to-end tests against a real `libduckdb` binary (auto-skip when env vars are unset).
- `duckdbsql/` — `database/sql/driver` adapter (`driver.go`, `conn.go`, `stmt.go`, `rows.go`, `convert.go`, `composite.go`, and domain-named tests).
- `duckdbarrow/` — Pure-Go (`CGO_ENABLED=0`) `apache/arrow-go/v18` integration (`convert.go`, `export.go`, `reader.go`, and domain-named unit & native tests).

## Build, Vet, and Test Commands

### Unit, mock, and cross-compile checks (no `libduckdb` required)

```sh
test -z "$(gofmt -l duckdb duckdbarrow duckdbsql internal)"
CGO_ENABLED=0 go build -v ./...
CGO_ENABLED=0 go vet ./...
CGO_ENABLED=0 go test -count=1 ./...
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build ./...
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build ./...
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...
```

### Real-native verification matrix

Required for ABI, loader, cancellation, budget, session, Arrow, Appender, or `PinnedVersion` changes:

```sh
DUCKDB_HEADER_DIR=/path/to/headers \
DUCKDB_LIB_PATH=/path/to/libduckdb.dylib \
DUCKDB_LIB_SHA256=<64-hex-sha256> \
CGO_ENABLED=0 go test -count=1 ./...

DUCKDB_HEADER_DIR=/path/to/headers \
DUCKDB_LIB_PATH=/path/to/libduckdb.dylib \
DUCKDB_LIB_SHA256=<64-hex-sha256> \
go test -race -count=1 ./...
```

Notes:
- Native tests (`*_native_test.go`, `TestNativeLoad`) skip automatically unless both `DUCKDB_LIB_PATH` and `DUCKDB_LIB_SHA256` are set.
- The C header struct-layout probe in `internal/abi/abi_test.go` skips unless `DUCKDB_HEADER_DIR` is set and a `cc` compiler is on `PATH`.
- GitHub Actions (`.github/workflows/go.yml`) runs both the mock/cross-compile `build` job and the 4-platform `native-acceptance` matrix (`linux/amd64`, `linux/arm64`, `darwin/arm64`, `windows/amd64`, both `CGO_ENABLED=0` and `-race`) on pushes/PRs to `main`, `master`, and `develop`.

## Key Architectural Details

- **Resource hierarchy**: Ownership follows `Library → Database / InstanceCache → Conn → Tx / Stmt / Rows / ArrowRows / Appender / ArrowView / File`. A `Conn` grants an exclusive lease to at most one open `Rows`, `ArrowRows`, or `Appender`; concurrent operations on that `Conn` return `ErrBusy`.
- **Streaming-only execution**: `Query` and `QueryArrow` use `duckdb_pending_prepared_streaming` and assert `duckdb_result_is_streaming == true`. Non-streaming `duckdb_pending_prepared` materializes results outside `memory_limit` and is never used.
- **Singleton callbacks**: Arrow C Data Interface export (`RegisterArrowView`) and C Stream Interface export (`RegisterArrowStream`) share exactly 6 process-wide `purego.NewCallback` trampolines (safe on Windows callback slot limits and across C++ shallow copies).
- **Latched cancellation**: DuckDB's `InitialCleanup` clears the connection interrupt flag during setup, so `cancelScope` latches cancellation and periodically re-asserts `duckdb_interrupt` until the native scope exits and `join()` completes.
- **Read-only session isolation**: `SessionWorkspace.OpenReadOnly` seals `input/` and `catalog/` read-only, checkpoints and closes the write database, reopens `READ_ONLY`, and applies `allowed_paths`, `enable_external_access=false`, and `lock_configuration=true`.

## Upgrading `abi.PinnedVersion`

1. Update `abi.PinnedVersion` in `internal/abi/types.go` (and any changed ABI structs/symbols in `internal/abi/`).
2. Update the approved artifact and SHA-256 table in `README.md` and `.github/workflows/go.yml`.
3. Run both the `CGO_ENABLED=0` and `-race` native verification commands above on every accepted platform.
