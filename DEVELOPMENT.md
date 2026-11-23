# Development Guide

Architecture, repository layout, build/test commands, and native upgrade workflow for `github.com/bububa/duckdbspike`.

## Overview

Minimal, `CGO_ENABLED=0` [`purego`](https://github.com/ebitengine/purego) binding for a single pinned DuckDB native C API release (`abi.PinnedVersion`, currently `v1.5.6`), designed for read-only telemetry reporting.

- **Go version**: `>= 1.27` (see `go.mod` and `.github/workflows/go.yml`)
- **Single external dependency**: `github.com/ebitengine/purego`

## Repository Layout

- `internal/abi/` — Pinned C ABI definitions and safe loader; owns no native resources.
  - `types.go` — `PinnedVersion`, C struct mirrors (`Result`, `Hugeint`, `Uhugeint`, `Decimal`, `StringT`, `ListEntry`, temporal types), and C enum constants.
  - `api.go` — Complete `API` function-pointer table with ownership annotations (`owned` vs `borrowed`) and `symbols()` registry.
  - `load.go` — `Load(path, wantSHA256)`: platform gate, path/permission checks, SHA-256 verification, atomic all-or-nothing symbol resolution, and `PinnedVersion` check.
  - `platform_accepted.go` / `platform_windows_amd64.go` / `platform_other.go` — Build-tag gate (`darwin/arm64`, `linux/amd64`, `linux/arm64`, `windows/amd64`) and native library openers (`purego.Dlopen` on Unix, `syscall.LoadLibrary` on Windows).
  - `perms_unix.go` / `perms_windows.go` — OS-specific library path/permission guards (POSIX `0o022` check on Unix, UNC path rejection on Windows).
  - `abi_test.go` — Symbol coverage check, loader validation tests, C header layout probe (`DUCKDB_HEADER_DIR`), and native load test.
- `duckdb/` — Public Go API and resource/safety kernel.
  - `doc.go` — Package docs, sentinel errors, `Error`/`ErrorKind`, `noCopy`.
  - `database.go` — `Library`, `Database`, `Option`, `Load`, `Open`, `Close`.
  - `conn.go` — `Conn`, `ExecResult`, connection-level serialization (`mu`) and cancellation coordination (`cancelMu`).
  - `stmt.go` — Prepared statements (`Stmt`), streaming execution via `duckdb_pending_prepared_streaming`.
  - `rows.go` — Streaming `Rows` iteration, exclusive `Conn` lease (`ErrBusy`), row/byte limit enforcement.
  - `bind.go` / `decode.go` / `types.go` — Exact typed parameter binding, vector decoding into owned Go values, schema audit, and `DecodeLimits`.
  - `cancel.go` — `cancelScope`: latched cancellation with periodic `duckdb_interrupt` re-assertion and no-deadline `join()`.
  - `budget.go` — `InstanceConfig`, `ResultLimits`, `BudgetController` (memory, thread, spill, and query admission budgets).
  - `session.go` — `SessionWorkspace` (`input/`, `catalog/`, `spill/`) and `ReadOnlySession` isolation flow.
  - `*_mock_test.go`, `budget_test.go`, `types_test.go` — Deterministic tests using fake `abi.API` tables (no native library needed).
  - `*_native_test.go` — End-to-end tests against a real `libduckdb` binary (auto-skip when env vars are unset).

## Build, Vet, and Test Commands

### Unit and mock tests (no `libduckdb` required)

```sh
CGO_ENABLED=0 go build -v ./...
CGO_ENABLED=0 go vet ./...
CGO_ENABLED=0 go test -count=1 ./...
```

### Real-native verification matrix

Required for ABI, loader, cancellation, budget, session, or `PinnedVersion` changes:

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

## Key Architectural Details

- **Resource hierarchy**: Ownership follows `Library → Database → Conn → Stmt / Rows`. A `Conn` grants an exclusive lease to at most one open `Rows`; concurrent operations on that `Conn` return `ErrBusy`.
- **Streaming-only execution**: `Query` uses `duckdb_pending_prepared_streaming` and asserts `duckdb_result_is_streaming == true`. Non-streaming `duckdb_pending_prepared` materializes results outside `memory_limit` and is not used.
- **Latched cancellation**: DuckDB's `InitialCleanup` clears the connection interrupt flag during setup, so `cancelScope` latches cancellation and periodically re-asserts `duckdb_interrupt` until the native scope exits and `join()` completes.
- **Read-only session isolation**: `SessionWorkspace.OpenReadOnly` seals `input/` and `catalog/` read-only, checkpoints and closes the write database, reopens `READ_ONLY`, and applies `allowed_paths`, `enable_external_access=false`, and `lock_configuration=true`.

## Upgrading `abi.PinnedVersion`

1. Update `abi.PinnedVersion` in `internal/abi/types.go` (and any changed ABI structs/symbols in `internal/abi/`).
2. Update the approved artifact and SHA-256 table in `README.md`.
3. Run both the `CGO_ENABLED=0` and `-race` native verification commands above on every accepted platform.
