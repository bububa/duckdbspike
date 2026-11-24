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

// Error types mirror the duckdb_error_type values that callers distinguish.
const (
	ErrorInvalid     int32 = 0
	ErrorInterrupt   int32 = 29
	ErrorOutOfMemory int32 = 33
	ErrorPermission  int32 = 34
)
