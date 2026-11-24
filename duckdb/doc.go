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
	"strings"

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
	ErrTxDone          = errors.New("duckdb: transaction already committed or rolled back")
	ErrTxActive        = errors.New("duckdb: connection already has an active transaction")

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
	// ErrorKindConstraint reports a PRIMARY KEY, UNIQUE, NOT NULL, CHECK, or
	// FOREIGN KEY constraint violation.
	ErrorKindConstraint
	// ErrorKindCatalog reports a missing or conflicting catalog object (table, view, schema, type).
	ErrorKindCatalog
	// ErrorKindSyntax reports a SQL syntax error.
	ErrorKindSyntax
	// ErrorKindParser reports a SQL parser error.
	ErrorKindParser
	// ErrorKindBinder reports a SQL binding error (unresolved column/function/ambiguity).
	ErrorKindBinder
	// ErrorKindConversion reports a type cast or value conversion failure.
	ErrorKindConversion
	// ErrorKindOutOfRange reports a numeric or temporal overflow/range error.
	ErrorKindOutOfRange
	// ErrorKindInvalidInput reports an invalid input error reported by DuckDB.
	ErrorKindInvalidInput
	// ErrorKindIO reports a filesystem or I/O error.
	ErrorKindIO
	// ErrorKindTransaction reports a transaction conflict or invalid transaction state.
	ErrorKindTransaction
	// ErrorKindConnection reports a connection-level error.
	ErrorKindConnection
	// ErrorKindInvalidType reports an invalid or mismatched SQL type error.
	ErrorKindInvalidType
	// ErrorKindSerialization reports a serialization/deserialization error.
	ErrorKindSerialization
	// ErrorKindNotImplemented reports an unimplemented engine feature.
	ErrorKindNotImplemented
	// ErrorKindHTTP reports an HTTP/network error.
	ErrorKindHTTP
	// ErrorKindMissingExtension reports a missing or unautoloaded extension.
	ErrorKindMissingExtension
	// ErrorKindConfiguration reports an invalid configuration option or locked setting.
	ErrorKindConfiguration
)

func kindOf(nativeType int32) ErrorKind {
	switch nativeType {
	case abi.ErrorInterrupt:
		return ErrorKindInterrupt
	case abi.ErrorOutOfMemory:
		return ErrorKindOutOfMemory
	case abi.ErrorPermission:
		return ErrorKindPermission
	case abi.ErrorConstraint:
		return ErrorKindConstraint
	case abi.ErrorCatalog:
		return ErrorKindCatalog
	case abi.ErrorSyntax:
		return ErrorKindSyntax
	case abi.ErrorParser:
		return ErrorKindParser
	case abi.ErrorBinder:
		return ErrorKindBinder
	case abi.ErrorConversion, abi.ErrorDecimal:
		return ErrorKindConversion
	case abi.ErrorOutOfRange, abi.ErrorDivideByZero:
		return ErrorKindOutOfRange
	case abi.ErrorInvalidInput, abi.ErrorParameterNotResolved, abi.ErrorParameterNotAllowed:
		return ErrorKindInvalidInput
	case abi.ErrorIO:
		return ErrorKindIO
	case abi.ErrorTransaction:
		return ErrorKindTransaction
	case abi.ErrorConnection:
		return ErrorKindConnection
	case abi.ErrorInvalidType, abi.ErrorMismatchType, abi.ErrorUnknownType:
		return ErrorKindInvalidType
	case abi.ErrorSerialization:
		return ErrorKindSerialization
	case abi.ErrorNotImplemented:
		return ErrorKindNotImplemented
	case abi.ErrorHTTP, abi.ErrorNetwork:
		return ErrorKindHTTP
	case abi.ErrorMissingExtension, abi.ErrorAutoload:
		return ErrorKindMissingExtension
	case abi.ErrorInvalidConfiguration, abi.ErrorSettings:
		return ErrorKindConfiguration
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

func kindFromMessage(message string) ErrorKind {
	switch {
	case strings.HasPrefix(message, "Constraint Error:"):
		return ErrorKindConstraint
	case strings.HasPrefix(message, "Catalog Error:"):
		return ErrorKindCatalog
	case strings.HasPrefix(message, "Binder Error:"):
		return ErrorKindBinder
	case strings.HasPrefix(message, "Parser Error:"):
		return ErrorKindParser
	case strings.HasPrefix(message, "Syntax Error:"):
		return ErrorKindSyntax
	case strings.HasPrefix(message, "Conversion Error:"):
		return ErrorKindConversion
	case strings.HasPrefix(message, "Out of Range Error:"):
		return ErrorKindOutOfRange
	case strings.HasPrefix(message, "Invalid Input Error:"):
		return ErrorKindInvalidInput
	case strings.HasPrefix(message, "IO Error:"):
		return ErrorKindIO
	case strings.HasPrefix(message, "TransactionContext Error:"), strings.HasPrefix(message, "Transaction Error:"):
		return ErrorKindTransaction
	case strings.HasPrefix(message, "Connection Error:"):
		return ErrorKindConnection
	case strings.HasPrefix(message, "Invalid type Error:"), strings.HasPrefix(message, "Invalid Type Error:"):
		return ErrorKindInvalidType
	case strings.HasPrefix(message, "Serialization Error:"):
		return ErrorKindSerialization
	case strings.HasPrefix(message, "Not implemented Error:"):
		return ErrorKindNotImplemented
	case strings.HasPrefix(message, "HTTP Error:"):
		return ErrorKindHTTP
	case strings.HasPrefix(message, "Out of Memory Error:"):
		return ErrorKindOutOfMemory
	case strings.HasPrefix(message, "Permission Error:"):
		return ErrorKindPermission
	case strings.HasPrefix(message, "Interrupt Error:"):
		return ErrorKindInterrupt
	case strings.HasPrefix(message, "Invalid Configuration Error:"):
		return ErrorKindConfiguration
	default:
		return ErrorKindNative
	}
}

func nativeError(op string, nativeType int32, message string) *Error {
	if message == "" {
		message = "native call failed without a message"
	}
	kind := kindOf(nativeType)
	if nativeType == abi.ErrorInvalid {
		kind = kindFromMessage(message)
	}
	return &Error{Op: op, Kind: kind, NativeType: nativeType, Message: message}
}

func consumeErrorData(api *abi.API, op string, errData uintptr) *Error {
	if errData == 0 {
		return nil
	}
	defer api.DestroyErrorData(&errData)
	if !api.ErrorDataHasError(errData) {
		return nil
	}
	return nativeError(op, api.ErrorDataErrorType(errData), abi.GoString(api.ErrorDataMessage(errData)))
}

// noCopy makes go vet's copylocks check reject copies of handle structs. It is
// a static check only, not a runtime guard.
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
