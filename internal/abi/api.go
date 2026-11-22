package abi

// API is the complete table of native entry points used by package duckdb.
//
// Production values come only from Load, which resolves every symbol before
// returning, so a partially resolved table is never published. Tests in this
// module may construct an API literal with fake functions to drive allocation
// failure and race paths deterministically.
//
// Ownership notes use these terms: "owned" results must be released with the
// named destructor by the caller; "borrowed" results are valid only while the
// named parent object is alive and must be copied before it is released.
type API struct {
	// LibraryVersion returns a borrowed static C string.
	LibraryVersion func() uintptr

	// CreateConfig allocates an owned config released by DestroyConfig, even
	// when later SetConfig calls fail.
	CreateConfig  func(out *uintptr) int32
	SetConfig     func(config uintptr, name, option *byte) int32
	DestroyConfig func(config *uintptr)

	// OpenExt allocates an owned database released by Close. outError, when
	// set, is an owned C string released by Free, on success or failure.
	OpenExt func(path *byte, out *uintptr, config uintptr, outError *uintptr) int32
	Close   func(database *uintptr)
	Free    func(ptr uintptr)

	// Connect allocates an owned connection released by Disconnect.
	Connect    func(database uintptr, out *uintptr) int32
	Disconnect func(connection *uintptr)
	// Interrupt stores an atomic flag; it takes no client-context lock.
	Interrupt func(connection uintptr)

	// Prepare always allocates an owned statement released by DestroyPrepare,
	// including when it fails. PrepareError is borrowed from the statement.
	Prepare        func(connection uintptr, query *byte, out *uintptr) int32
	PrepareError   func(statement uintptr) uintptr
	DestroyPrepare func(statement *uintptr)
	NParams        func(statement uintptr) uint64
	ClearBindings  func(statement uintptr) int32
	// PreparedStatementColumnName returns an OWNED C string released by Free.
	PreparedStatementColumnCount       func(statement uintptr) uint64
	PreparedStatementColumnName        func(statement uintptr, colIdx uint64) uintptr
	PreparedStatementColumnLogicalType func(statement uintptr, colIdx uint64) uintptr

	BindNull          func(statement uintptr, index uint64) int32
	BindBoolean       func(statement uintptr, index uint64, value bool) int32
	BindInt8          func(statement uintptr, index uint64, value int8) int32
	BindInt16         func(statement uintptr, index uint64, value int16) int32
	BindInt32         func(statement uintptr, index uint64, value int32) int32
	BindInt64         func(statement uintptr, index uint64, value int64) int32
	BindUint8         func(statement uintptr, index uint64, value uint8) int32
	BindUint16        func(statement uintptr, index uint64, value uint16) int32
	BindUint32        func(statement uintptr, index uint64, value uint32) int32
	BindUint64        func(statement uintptr, index uint64, value uint64) int32
	BindHugeint       func(statement uintptr, index uint64, value Hugeint) int32
	BindUhugeint      func(statement uintptr, index uint64, value Uhugeint) int32
	BindDecimal       func(statement uintptr, index uint64, value Decimal) int32
	BindFloat         func(statement uintptr, index uint64, value float32) int32
	BindDouble        func(statement uintptr, index uint64, value float64) int32
	BindDate          func(statement uintptr, index uint64, value Date) int32
	BindTime          func(statement uintptr, index uint64, value Time) int32
	BindTimestamp     func(statement uintptr, index uint64, value Timestamp) int32
	BindTimestampTZ   func(statement uintptr, index uint64, value Timestamp) int32
	BindInterval      func(statement uintptr, index uint64, value Interval) int32
	BindVarcharLength func(statement uintptr, index uint64, value *byte, length uint64) int32
	BindBlob          func(statement uintptr, index uint64, data *byte, length uint64) int32
	BindValue         func(statement uintptr, index uint64, value uintptr) int32

	// CreateTimestamp* / CreateTimeNS allocate an owned duckdb_value released
	// by DestroyValue.
	CreateTimestampS  func(value TimestampS) uintptr
	CreateTimestampMS func(value TimestampMS) uintptr
	CreateTimestampNS func(value TimestampNS) uintptr
	CreateTimeNS      func(value TimeNS) uintptr
	DestroyValue      func(value *uintptr)

	// PendingPreparedStreaming is deprecated upstream but is the only entry
	// point that yields a streaming result in PinnedVersion. It always
	// allocates an owned pending result released by DestroyPending.
	PendingPreparedStreaming func(statement uintptr, out *uintptr) int32
	PendingExecuteTask       func(pending uintptr) int32
	PendingExecuteCheckState func(pending uintptr) int32
	// PendingError is borrowed from the pending result.
	PendingError   func(pending uintptr) uintptr
	ExecutePending func(pending uintptr, out *Result) int32
	DestroyPending func(pending *uintptr)

	// DestroyResult releases a result filled by ExecutePending, on success or
	// failure. ResultError and ColumnName are borrowed from the result.
	DestroyResult     func(result *Result)
	ResultError       func(result *Result) uintptr
	ResultErrorType   func(result *Result) int32
	ResultIsStreaming func(result Result) bool
	ResultReturnType  func(result Result) int32
	RowsChanged       func(result *Result) uint64
	ColumnCount       func(result *Result) uint64
	ColumnName        func(result *Result, column uint64) uintptr
	// ColumnLogicalType returns an owned logical type.
	ColumnLogicalType func(result *Result, column uint64) uintptr
	// FetchChunk returns an owned chunk released by DestroyDataChunk, or 0 at
	// end of stream or on error; ResultError distinguishes the two.
	FetchChunk func(result Result) uintptr

	GetTypeID           func(logicalType uintptr) int32
	DecimalWidth        func(logicalType uintptr) uint8
	DecimalScale        func(logicalType uintptr) uint8
	DecimalInternalType func(logicalType uintptr) int32
	// Child logical types and names below are owned by the caller.
	ListTypeChildType    func(logicalType uintptr) uintptr
	ArrayTypeChildType   func(logicalType uintptr) uintptr
	ArrayTypeArraySize   func(logicalType uintptr) uint64
	MapTypeKeyType       func(logicalType uintptr) uintptr
	MapTypeValueType     func(logicalType uintptr) uintptr
	StructTypeChildCount func(logicalType uintptr) uint64
	StructTypeChildName  func(logicalType uintptr, index uint64) uintptr
	StructTypeChildType  func(logicalType uintptr, index uint64) uintptr
	DestroyLogicalType   func(logicalType *uintptr)

	// Vectors and their data are borrowed from the owning chunk.
	DataChunkGetSize        func(chunk uintptr) uint64
	DataChunkGetColumnCount func(chunk uintptr) uint64
	DataChunkGetVector      func(chunk uintptr, column uint64) uintptr
	DestroyDataChunk        func(chunk *uintptr)
	VectorGetData           func(vector uintptr) uintptr
	VectorGetValidity       func(vector uintptr) uintptr
	ListVectorGetChild      func(vector uintptr) uintptr
	ListVectorGetSize       func(vector uintptr) uint64
	StructVectorGetChild    func(vector uintptr, index uint64) uintptr
	ArrayVectorGetChild     func(vector uintptr) uintptr
}

// symbol pairs an API field with its exported native name.
type symbol struct {
	field any
	name  string
}

// symbols lists every entry point. Load fails unless all of them resolve.
func (a *API) symbols() []symbol {
	return []symbol{
		{&a.LibraryVersion, "duckdb_library_version"},
		{&a.CreateConfig, "duckdb_create_config"},
		{&a.SetConfig, "duckdb_set_config"},
		{&a.DestroyConfig, "duckdb_destroy_config"},
		{&a.OpenExt, "duckdb_open_ext"},
		{&a.Close, "duckdb_close"},
		{&a.Free, "duckdb_free"},
		{&a.Connect, "duckdb_connect"},
		{&a.Disconnect, "duckdb_disconnect"},
		{&a.Interrupt, "duckdb_interrupt"},
		{&a.Prepare, "duckdb_prepare"},
		{&a.PrepareError, "duckdb_prepare_error"},
		{&a.DestroyPrepare, "duckdb_destroy_prepare"},
		{&a.NParams, "duckdb_nparams"},
		{&a.ClearBindings, "duckdb_clear_bindings"},
		{&a.PreparedStatementColumnCount, "duckdb_prepared_statement_column_count"},
		{&a.PreparedStatementColumnName, "duckdb_prepared_statement_column_name"},
		{&a.PreparedStatementColumnLogicalType, "duckdb_prepared_statement_column_logical_type"},
		{&a.BindNull, "duckdb_bind_null"},
		{&a.BindBoolean, "duckdb_bind_boolean"},
		{&a.BindInt8, "duckdb_bind_int8"},
		{&a.BindInt16, "duckdb_bind_int16"},
		{&a.BindInt32, "duckdb_bind_int32"},
		{&a.BindInt64, "duckdb_bind_int64"},
		{&a.BindUint8, "duckdb_bind_uint8"},
		{&a.BindUint16, "duckdb_bind_uint16"},
		{&a.BindUint32, "duckdb_bind_uint32"},
		{&a.BindUint64, "duckdb_bind_uint64"},
		{&a.BindHugeint, "duckdb_bind_hugeint"},
		{&a.BindUhugeint, "duckdb_bind_uhugeint"},
		{&a.BindDecimal, "duckdb_bind_decimal"},
		{&a.BindFloat, "duckdb_bind_float"},
		{&a.BindDouble, "duckdb_bind_double"},
		{&a.BindDate, "duckdb_bind_date"},
		{&a.BindTime, "duckdb_bind_time"},
		{&a.BindTimestamp, "duckdb_bind_timestamp"},
		{&a.BindTimestampTZ, "duckdb_bind_timestamp_tz"},
		{&a.BindInterval, "duckdb_bind_interval"},
		{&a.BindVarcharLength, "duckdb_bind_varchar_length"},
		{&a.BindBlob, "duckdb_bind_blob"},
		{&a.BindValue, "duckdb_bind_value"},
		{&a.CreateTimestampS, "duckdb_create_timestamp_s"},
		{&a.CreateTimestampMS, "duckdb_create_timestamp_ms"},
		{&a.CreateTimestampNS, "duckdb_create_timestamp_ns"},
		{&a.CreateTimeNS, "duckdb_create_time_ns"},
		{&a.DestroyValue, "duckdb_destroy_value"},
		{&a.PendingPreparedStreaming, "duckdb_pending_prepared_streaming"},
		{&a.PendingExecuteTask, "duckdb_pending_execute_task"},
		{&a.PendingExecuteCheckState, "duckdb_pending_execute_check_state"},
		{&a.PendingError, "duckdb_pending_error"},
		{&a.ExecutePending, "duckdb_execute_pending"},
		{&a.DestroyPending, "duckdb_destroy_pending"},
		{&a.DestroyResult, "duckdb_destroy_result"},
		{&a.ResultError, "duckdb_result_error"},
		{&a.ResultErrorType, "duckdb_result_error_type"},
		{&a.ResultIsStreaming, "duckdb_result_is_streaming"},
		{&a.ResultReturnType, "duckdb_result_return_type"},
		{&a.RowsChanged, "duckdb_rows_changed"},
		{&a.ColumnCount, "duckdb_column_count"},
		{&a.ColumnName, "duckdb_column_name"},
		{&a.ColumnLogicalType, "duckdb_column_logical_type"},
		{&a.FetchChunk, "duckdb_fetch_chunk"},
		{&a.GetTypeID, "duckdb_get_type_id"},
		{&a.DecimalWidth, "duckdb_decimal_width"},
		{&a.DecimalScale, "duckdb_decimal_scale"},
		{&a.DecimalInternalType, "duckdb_decimal_internal_type"},
		{&a.ListTypeChildType, "duckdb_list_type_child_type"},
		{&a.ArrayTypeChildType, "duckdb_array_type_child_type"},
		{&a.ArrayTypeArraySize, "duckdb_array_type_array_size"},
		{&a.MapTypeKeyType, "duckdb_map_type_key_type"},
		{&a.MapTypeValueType, "duckdb_map_type_value_type"},
		{&a.StructTypeChildCount, "duckdb_struct_type_child_count"},
		{&a.StructTypeChildName, "duckdb_struct_type_child_name"},
		{&a.StructTypeChildType, "duckdb_struct_type_child_type"},
		{&a.DestroyLogicalType, "duckdb_destroy_logical_type"},
		{&a.DataChunkGetSize, "duckdb_data_chunk_get_size"},
		{&a.DataChunkGetColumnCount, "duckdb_data_chunk_get_column_count"},
		{&a.DataChunkGetVector, "duckdb_data_chunk_get_vector"},
		{&a.DestroyDataChunk, "duckdb_destroy_data_chunk"},
		{&a.VectorGetData, "duckdb_vector_get_data"},
		{&a.VectorGetValidity, "duckdb_vector_get_validity"},
		{&a.ListVectorGetChild, "duckdb_list_vector_get_child"},
		{&a.ListVectorGetSize, "duckdb_list_vector_get_size"},
		{&a.StructVectorGetChild, "duckdb_struct_vector_get_child"},
		{&a.ArrayVectorGetChild, "duckdb_array_vector_get_child"},
	}
}
