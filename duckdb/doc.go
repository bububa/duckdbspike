// Package duckdb is a minimal, cgo-free binding to one pinned DuckDB native
// release, loaded with purego.
//
// Resource ownership follows Library → Database → Conn → Stmt / Rows. Every
// handle is used through a pointer, Close is idempotent, and operations after
// Close return ErrClosed. No finalizer participates in correctness: callers
// must Close what they open. A Conn grants an exclusive lease to at most one
// open Rows; any other operation on that Conn returns ErrBusy until the Rows is
// closed, because DuckDB invalidates an open result when the same connection
// starts another statement.
//
// Query results are always streaming. The non-streaming pending path
// materializes the entire result in native memory outside memory_limit, so it
// is not exposed.
//
// Type precision: every supported SQL type is bound and decoded with its exact
// width, scale, unit, and SQL NULL semantics without passing through float64,
// string, or JSON. Unsupported logical types fail when inspecting a statement
// or opening a result, even if every row is NULL or the result is empty. All
// decoded Values own their Go memory and remain valid after Rows advances or
// closes.
package duckdb

import (
	"errors"
	"fmt"

	"github.com/bububa/duckdbspike/internal/abi"
)

// Sentinel errors.
var (
	ErrClosed          = errors.New("duckdb: handle is closed")
	ErrBusy            = errors.New("duckdb: connection has an open Rows")
	ErrNotStreaming    = errors.New("duckdb: native result is not streaming")
	ErrInvalidInput    = errors.New("duckdb: invalid input")
	ErrUnsupportedType = errors.New("duckdb: unsupported logical type")
	ErrTypeMismatch    = errors.New("duckdb: value type mismatch")
	ErrNullValue       = errors.New("duckdb: value is SQL NULL")
	ErrOutOfRange      = errors.New("duckdb: value out of target range")
	ErrLimitExceeded   = errors.New("duckdb: decode limit exceeded")
	ErrBudgetExceeded  = errors.New("duckdb: resource budget exceeded")
	ErrQueueFull       = errors.New("duckdb: query admission queue is full")

	ErrUnsupportedPlatform = abi.ErrUnsupportedPlatform
	ErrUnsafeLibraryPath   = abi.ErrUnsafeLibraryPath
	ErrChecksumMismatch    = abi.ErrChecksumMismatch
	ErrMissingSymbol       = abi.ErrMissingSymbol
	ErrVersionMismatch     = abi.ErrVersionMismatch
	ErrLibraryOpen         = abi.ErrLibraryOpen
)

// ErrorKind classifies native errors that callers act on.
type ErrorKind int

const (
	// ErrorKindUnknown is the invalid zero value.
	ErrorKindUnknown ErrorKind = iota
	// ErrorKindNative is any native error without a more specific kind.
	ErrorKindNative
	// ErrorKindInterrupt reports a native interrupt. Pending-stage errors carry
	// no native type, so cancellation must be decided by the caller's own
	// latched state, never by this kind alone.
	ErrorKindInterrupt
	// ErrorKindOutOfMemory reports native memory or spill exhaustion.
	ErrorKindOutOfMemory
	// ErrorKindPermission reports a denied file or capability access.
	ErrorKindPermission
)

func kindOf(nativeType int32) ErrorKind {
	switch nativeType {
	case abi.ErrorInterrupt:
		return ErrorKindInterrupt
	case abi.ErrorOutOfMemory:
		return ErrorKindOutOfMemory
	case abi.ErrorPermission:
		return ErrorKindPermission
	default:
		return ErrorKindNative
	}
}

// Error is a native failure. Message is copied before the native object that
// owned it is released.
type Error struct {
	Op         string
	Kind       ErrorKind
	NativeType int32
	Message    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("duckdb %s: %s", e.Op, e.Message)
}

func nativeError(op string, nativeType int32, message string) *Error {
	if message == "" {
		message = "native call failed without a message"
	}
	return &Error{Op: op, Kind: kindOf(nativeType), NativeType: nativeType, Message: message}
}

// noCopy makes go vet's copylocks check reject copies of handle structs. It is
// a static check only, not a runtime guard.
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
