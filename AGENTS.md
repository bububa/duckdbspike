# AGENTS.md

Strict rules, restrictions, and coding instructions for agents working in this repository.
See [DEVELOPMENT.md](DEVELOPMENT.md) for repo layout, architecture, build/test commands, and upgrade procedures.

## Hard Restrictions

- **No cgo in production**: Never add `import "C"`. All production code must build with `CGO_ENABLED=0` (only the test-only C layout probe in `internal/abi/abi_test.go` may invoke `cc`).
- **No unverified platforms**: Do not widen build tags in `internal/abi/platform_accepted.go` without a real-native acceptance run and an updated artifact/SHA-256 entry in `README.md`.
- **No extra dependencies**: Keep direct external dependencies restricted to `github.com/ebitengine/purego` (core) and `github.com/apache/arrow-go/v18` (`duckdbarrow`) unless explicitly asked otherwise.
- **No non-streaming `Query`**: Never switch `Query` away from `duckdb_pending_prepared_streaming` or remove the `duckdb_result_is_streaming == true` assertion.
- **No library unload**: Never call `dlclose` on mapped native libraries.
- **No lossy type conversions**: Never pass bound or decoded SQL values through `float64`, string, or JSON intermediates. Reject unsupported logical types during prepare/result inspection even when all rows are `NULL` or the result is empty.
- **No globs in views**: `ViewSpec` must bind explicit file paths inside `input/` only.

## Native ABI, Memory & Concurrency Rules

- **Complete symbol registration**: Every field added to `abi.API` must be a `func` registered in `(*API).symbols()` (verified by `TestSymbolsCoverEveryAPIField`).
- **Explicit ownership**: Document `owned` vs. `borrowed` lifetimes on every `abi.API` entry. Copy borrowed C strings/buffers before releasing the parent handle; release owned handles on both success and error paths. Never rely on finalizers for correctness.
- **Locking discipline**: `Conn.mu` serializes native calls; `Conn.cancelMu` coordinates cancellation. Never hold `Conn.cancelMu` across a native call, and never let `duckdb_interrupt` block on `Conn.mu`.
- **No-deadline join**: Always `join()` an active `cancelScope` before reusing or closing any native handle.

## Go Best Practices

- **Handle safety**: Embed `noCopy` in handle structs (checked by `go vet`). Make `Close` idempotent and return `ErrClosed` for post-close calls.
- **Owned Go values**: Decoded values returned to callers must own their Go memory and never alias native or chunk-backed buffers.
- **Error handling**: Return and wrap sentinel errors with `%w` so callers can match with `errors.Is`; do not swallow cleanup errors silently when they indicate resource/state failure.
- **Context & goroutines**: Respect `context.Context` cancellation on blocking operations, reject `nil` contexts with `ErrInvalidInput`, and ensure every spawned goroutine has a deterministic shutdown/join path.
- **Idiomatic, minimal diffs**: Follow standard Go formatting (`gofmt`), keep exported API surfaces small and documented, and avoid speculative abstractions.

## Testing & Verification Rules

- **Mock first, native paired**: Add deterministic mock tests (using fake `abi.API` literals) for lifecycle, error, budget, and race edge cases so CI (`ubuntu-latest`, no `libduckdb`) covers them; pair native behavior changes with `*_native_test.go` tests. Use descriptive domain-based test filenames and test function names (never include internal plan phase prefixes such as `p0_` or `TestP1a...`).
- **Required checks before finishing**:
  - Run `CGO_ENABLED=0 go build ./...`, `CGO_ENABLED=0 go vet ./...`, and `CGO_ENABLED=0 go test -count=1 ./...`.
  - When changing ABI, loader, cancellation, budget, session, or `PinnedVersion`, also run the full real-native + `-race` matrix documented in [DEVELOPMENT.md](DEVELOPMENT.md).
