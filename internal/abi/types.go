// Package abi declares the pinned DuckDB C ABI used by package duckdb.
//
// Every declaration mirrors duckdb.h from exactly one native release
// (PinnedVersion). The layouts below are verified against the C header by the
// test-only probe in internal/abi; product builds never require cgo.
// Nothing in this package owns native resources: ownership is modelled by the
// public duckdb package, which documents for each call whether the returned
// pointer is owned or borrowed and how it is released.
package abi

// PinnedVersion is the only native library version this package accepts.
const PinnedVersion = "v1.5.6"

// Result mirrors struct duckdb_result. It contains no Go pointers, so it may be
// passed by address to native code. Only InternalData is meaningful; the
// deprecated fields must not be read.
type Result struct {
	DeprecatedColumnCount  uint64
	DeprecatedRowCount     uint64
	DeprecatedRowsChanged  uint64
	DeprecatedColumns      uintptr
	DeprecatedErrorMessage uintptr
	InternalData           uintptr
}

// Hugeint mirrors struct duckdb_hugeint.
type Hugeint struct {
	Lower uint64
	Upper int64
}

// Uhugeint mirrors struct duckdb_uhugeint.
type Uhugeint struct {
	Lower uint64
	Upper uint64
}

// Decimal mirrors struct duckdb_decimal (value at offset 8, size 24).
type Decimal struct {
	Width uint8
	Scale uint8
	Value Hugeint
}

// StringT mirrors duckdb_string_t. Strings of at most InlineStringLength bytes
// are stored inline starting at offset 4; longer strings use Ptr.
type StringT struct {
	Length uint32
	Prefix [4]byte
	Ptr    uintptr
}

// InlineStringLength is the maximum inline duckdb_string_t payload.
const InlineStringLength = 12

// ListEntry mirrors struct duckdb_list_entry.
type ListEntry struct {
	Offset uint64
	Length uint64
}

// Interval mirrors struct duckdb_interval.
type Interval struct {
	Months int32
	Days   int32
	Micros int64
}

// Date mirrors struct duckdb_date (days since 1970-01-01).
type Date struct {
	Days int32
}

// Time mirrors struct duckdb_time (microseconds since 00:00:00).
type Time struct {
	Micros int64
}

// TimeNS mirrors struct duckdb_time_ns (nanoseconds since 00:00:00).
type TimeNS struct {
	Nanos int64
}

// TimeTZ mirrors struct duckdb_time_tz (40 bits micros + 24 bits biased offset).
type TimeTZ struct {
	Bits uint64
}

// QueryProgress mirrors struct duckdb_query_progress_type.
type QueryProgress struct {
	Percentage         float64
	RowsProcessed      uint64
	TotalRowsToProcess uint64
}

// ArrowSchema mirrors struct ArrowSchema from the Apache Arrow C Data Interface.
// It contains no Go pointers; Release is a C function pointer invoked via
// ReleaseArrowSchema.
type ArrowSchema struct {
	Format      uintptr
	Name        uintptr
	Metadata    uintptr
	Flags       int64
	NChildren   int64
	Children    uintptr
	Dictionary  uintptr
	Release     uintptr
	PrivateData uintptr
}

// ArrowArray mirrors struct ArrowArray from the Apache Arrow C Data Interface.
// It contains no Go pointers; Release is a C function pointer invoked via
// ReleaseArrowArray.
type ArrowArray struct {
	Length      int64
	NullCount   int64
	Offset      int64
	NBuffers    int64
	NChildren   int64
	Buffers     uintptr
	Children    uintptr
	Dictionary  uintptr
	Release     uintptr
	PrivateData uintptr
}

// Bit mirrors struct duckdb_bit.
type Bit struct {
	Data uintptr
	Size uint64
}

// Bignum mirrors struct duckdb_bignum.
type Bignum struct {
	Data       uintptr
	Size       uint64
	IsNegative bool
}

// Timestamp mirrors struct duckdb_timestamp (microseconds since 1970-01-01).
type Timestamp struct {
	Micros int64
}

// TimestampS mirrors struct duckdb_timestamp_s (seconds since 1970-01-01).
type TimestampS struct {
	Seconds int64
}

// TimestampMS mirrors struct duckdb_timestamp_ms (milliseconds since 1970-01-01).
type TimestampMS struct {
	Millis int64
}

// TimestampNS mirrors struct duckdb_timestamp_ns (nanoseconds since 1970-01-01).
type TimestampNS struct {
	Nanos int64
}

// State mirrors duckdb_state.
const (
	StateSuccess int32 = 0
	StateError   int32 = 1
)

// Pending states mirror duckdb_pending_state.
const (
	PendingResultReady    int32 = 0
	PendingResultNotReady int32 = 1
	PendingError          int32 = 2
	PendingNoTasks        int32 = 3
)

// Result types mirror duckdb_result_type.
const (
	ResultTypeInvalid     int32 = 0
	ResultTypeChangedRows int32 = 1
	ResultTypeNothing     int32 = 2
	ResultTypeQueryResult int32 = 3
)

// Type identifiers mirror duckdb_type.
const (
	TypeInvalid     int32 = 0
	TypeBoolean     int32 = 1
	TypeTinyint     int32 = 2
	TypeSmallint    int32 = 3
	TypeInteger     int32 = 4
	TypeBigint      int32 = 5
	TypeUTinyint    int32 = 6
	TypeUSmallint   int32 = 7
	TypeUInteger    int32 = 8
	TypeUBigint     int32 = 9
	TypeFloat       int32 = 10
	TypeDouble      int32 = 11
	TypeTimestamp   int32 = 12
	TypeDate        int32 = 13
	TypeTime        int32 = 14
	TypeInterval    int32 = 15
	TypeHugeint     int32 = 16
	TypeVarchar     int32 = 17
	TypeBlob        int32 = 18
	TypeDecimal     int32 = 19
	TypeTimestampS  int32 = 20
	TypeTimestampMS int32 = 21
	TypeTimestampNS int32 = 22
	TypeEnum        int32 = 23
	TypeList        int32 = 24
	TypeStruct      int32 = 25
	TypeMap         int32 = 26
	TypeUUID        int32 = 27
	TypeUnion       int32 = 28
	TypeBit         int32 = 29
	TypeTimeTZ      int32 = 30
	TypeTimestampTZ int32 = 31
	TypeUHugeint    int32 = 32
	TypeArray       int32 = 33
	TypeAny         int32 = 34
	TypeBignum      int32 = 35
	TypeSQLNull     int32 = 36
	TypeTimeNS      int32 = 39
	TypeGeometry    int32 = 40
	TypeVariant     int32 = 41
)

// Error types mirror duckdb_error_type (v1.5.6).
const (
	ErrorInvalid              int32 = 0
	ErrorOutOfRange           int32 = 1
	ErrorConversion           int32 = 2
	ErrorUnknownType          int32 = 3
	ErrorDecimal              int32 = 4
	ErrorMismatchType         int32 = 5
	ErrorDivideByZero         int32 = 6
	ErrorObjectSize           int32 = 7
	ErrorInvalidType          int32 = 8
	ErrorSerialization        int32 = 9
	ErrorTransaction          int32 = 10
	ErrorNotImplemented       int32 = 11
	ErrorExpression           int32 = 12
	ErrorCatalog              int32 = 13
	ErrorParser               int32 = 14
	ErrorPlanner              int32 = 15
	ErrorScheduler            int32 = 16
	ErrorExecutor             int32 = 17
	ErrorConstraint           int32 = 18
	ErrorIndex                int32 = 19
	ErrorStat                 int32 = 20
	ErrorConnection           int32 = 21
	ErrorSyntax               int32 = 22
	ErrorSettings             int32 = 23
	ErrorBinder               int32 = 24
	ErrorNetwork              int32 = 25
	ErrorOptimizer            int32 = 26
	ErrorNullPointer          int32 = 27
	ErrorIO                   int32 = 28
	ErrorInterrupt            int32 = 29
	ErrorFatal                int32 = 30
	ErrorInternal             int32 = 31
	ErrorInvalidInput         int32 = 32
	ErrorOutOfMemory          int32 = 33
	ErrorPermission           int32 = 34
	ErrorParameterNotResolved int32 = 35
	ErrorParameterNotAllowed  int32 = 36
	ErrorDependency           int32 = 37
	ErrorHTTP                 int32 = 38
	ErrorMissingExtension     int32 = 39
	ErrorAutoload             int32 = 40
	ErrorSequence             int32 = 41
	ErrorInvalidConfiguration int32 = 42
)

// Statement types mirror duckdb_statement_type (v1.5.6).
const (
	StatementTypeInvalid          int32 = 0
	StatementTypeSelect           int32 = 1
	StatementTypeInsert           int32 = 2
	StatementTypeUpdate           int32 = 3
	StatementTypeExplain          int32 = 4
	StatementTypeDelete           int32 = 5
	StatementTypePrepare          int32 = 6
	StatementTypeCreate           int32 = 7
	StatementTypeExecute          int32 = 8
	StatementTypeAlter            int32 = 9
	StatementTypeTransaction      int32 = 10
	StatementTypeCopy             int32 = 11
	StatementTypeAnalyze          int32 = 12
	StatementTypeVariableSet      int32 = 13
	StatementTypeCreateFunc       int32 = 14
	StatementTypeDrop             int32 = 15
	StatementTypeExport           int32 = 16
	StatementTypePragma           int32 = 17
	StatementTypeVacuum           int32 = 18
	StatementTypeCall             int32 = 19
	StatementTypeSet              int32 = 20
	StatementTypeLoad             int32 = 21
	StatementTypeRelation         int32 = 22
	StatementTypeExtension        int32 = 23
	StatementTypeLogicalPlan      int32 = 24
	StatementTypeAttach           int32 = 25
	StatementTypeDetach           int32 = 26
	StatementTypeMulti            int32 = 27
	StatementTypeCopyDatabase     int32 = 28
	StatementTypeUpdateExtensions int32 = 29
	StatementTypeMergeInto        int32 = 30
)
