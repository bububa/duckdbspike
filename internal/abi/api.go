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
	// ConfigCount returns the total number of DuckDB configuration flags.
	// GetConfigFlag writes borrowed static C string pointers into outName and
	// outDescription; callers must NOT free them.
	ConfigCount   func() uintptr
	GetConfigFlag func(index uintptr, outName *uintptr, outDescription *uintptr) int32

	// OpenExt allocates an owned database released by Close. outError, when
	// set, is an owned C string released by Free, on success or failure.
	OpenExt func(path *byte, out *uintptr, config uintptr, outError *uintptr) int32
	// CreateInstanceCache allocates an owned instance cache released by
	// DestroyInstanceCache. GetOrCreateFromCache allocates an owned database
	// handle released by Close; outError, when set, is owned and freed by Free.
	CreateInstanceCache  func() uintptr
	GetOrCreateFromCache func(cache uintptr, path *byte, out *uintptr, config uintptr, outError *uintptr) int32
	DestroyInstanceCache func(cache *uintptr)
	Close                func(database *uintptr)
	Free                 func(ptr uintptr)

	// ErrorData* inspect an owned duckdb_error_data released by DestroyErrorData.
	// ErrorDataMessage returns a borrowed C string valid while errorData is alive.
	ErrorDataHasError  func(errorData uintptr) bool
	ErrorDataErrorType func(errorData uintptr) int32
	ErrorDataMessage   func(errorData uintptr) uintptr
	DestroyErrorData   func(errorData *uintptr)

	// Connect allocates an owned connection released by Disconnect.
	Connect    func(database uintptr, out *uintptr) int32
	Disconnect func(connection *uintptr)
	// ConnectionGetClientContext allocates an owned client_context released by
	// DestroyClientContext. ClientContextGetConfigOption returns an owned
	// duckdb_value released by DestroyValue (or 0 if option does not exist).
	ConnectionGetClientContext   func(connection uintptr, outContext *uintptr)
	ClientContextGetConnectionID func(context uintptr) uint64
	ClientContextGetConfigOption func(context uintptr, name *byte, outScope *int32) uintptr
	DestroyClientContext         func(context *uintptr)
	// GetTableNames returns an owned LIST<VARCHAR> duckdb_value released by
	// DestroyValue (or 0 on invalid SQL).
	GetTableNames func(connection uintptr, query *byte, qualified bool) uintptr
	// Interrupt stores an atomic flag; it takes no client-context lock.
	Interrupt func(connection uintptr)
	// QueryProgress snapshots live execution progress for connection.
	QueryProgress func(connection uintptr) QueryProgress
	// GetProfilingInfo returns a borrowed root profiling node (or 0 when
	// profiling is disabled). Child nodes returned by ProfilingInfoGetChild are
	// borrowed from the root; ProfilingInfoGetMetrics returns an owned
	// duckdb_value released by DestroyValue.
	GetProfilingInfo           func(connection uintptr) uintptr
	ProfilingInfoGetMetrics    func(info uintptr) uintptr
	ProfilingInfoGetChildCount func(info uintptr) uint64
	ProfilingInfoGetChild      func(info uintptr, index uint64) uintptr

	// Prepare always allocates an owned statement released by DestroyPrepare,
	// including when it fails. PrepareError is borrowed from the statement.
	Prepare        func(connection uintptr, query *byte, out *uintptr) int32
	PrepareError   func(statement uintptr) uintptr
	DestroyPrepare func(statement *uintptr)
	// ExtractStatements always allocates an owned extracted_statements handle
	// released by DestroyExtracted, even when count is 0. ExtractStatementsError
	// is borrowed from extracted. PrepareExtractedStatement always allocates an
	// owned statement released by DestroyPrepare, even on failure.
	ExtractStatements         func(connection uintptr, query *byte, out *uintptr) uint64
	PrepareExtractedStatement func(connection uintptr, extracted uintptr, index uint64, out *uintptr) int32
	ExtractStatementsError    func(extracted uintptr) uintptr
	DestroyExtracted          func(extracted *uintptr)
	NParams                   func(statement uintptr) uint64
	// ParamLogicalType returns an OWNED logical type released by DestroyLogicalType
	// (or 0 if out of range or not prepared).
	ParamLogicalType func(statement uintptr, paramIdx uint64) uintptr
	// PreparedStatementType returns the duckdb_statement_type enum for statement.
	PreparedStatementType func(statement uintptr) int32
	// ParameterName returns an OWNED C string released by Free (or 0 if out of range).
	ParameterName      func(statement uintptr, index uint64) uintptr
	BindParameterIndex func(statement uintptr, paramIdxOut *uint64, name *byte) int32
	ClearBindings      func(statement uintptr) int32
	// PreparedStatementColumnName returns an OWNED C string released by Free.
	PreparedStatementColumnCount       func(statement uintptr) uint64
	PreparedStatementColumnName        func(statement uintptr, colIdx uint64) uintptr
	PreparedStatementColumnLogicalType func(statement uintptr, colIdx uint64) uintptr

	// TableDescriptionCreateExt always allocates an owned table description
	// released by TableDescriptionDestroy, even when it returns StateError.
	// TableDescriptionError returns a borrowed C string valid until destroy.
	// TableDescriptionGetColumnName returns an owned C string released by Free.
	// TableDescriptionGetColumnType returns an owned logical type released by
	// DestroyLogicalType.
	TableDescriptionCreateExt      func(connection uintptr, catalog, schema, table *byte, out *uintptr) int32
	TableDescriptionDestroy        func(tableDescription *uintptr)
	TableDescriptionError          func(tableDescription uintptr) uintptr
	TableDescriptionGetColumnCount func(tableDescription uintptr) uint64
	TableDescriptionGetColumnName  func(tableDescription uintptr, index uint64) uintptr
	TableDescriptionGetColumnType  func(tableDescription uintptr, index uint64) uintptr
	ColumnHasDefault               func(tableDescription uintptr, index uint64, out *bool) int32

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

	// CreateTimeTZ packs micros and timezone offset into a duckdb_time_tz value.
	CreateTimeTZ func(micros int64, offset int32) TimeTZ

	// Create* allocate an owned duckdb_value released by DestroyValue.
	CreateNullValue     func() uintptr
	CreateBool          func(value bool) uintptr
	CreateInt8          func(value int8) uintptr
	CreateInt16         func(value int16) uintptr
	CreateInt32         func(value int32) uintptr
	CreateInt64         func(value int64) uintptr
	CreateUint8         func(value uint8) uintptr
	CreateUint16        func(value uint16) uintptr
	CreateUint32        func(value uint32) uintptr
	CreateUint64        func(value uint64) uintptr
	CreateHugeint       func(value Hugeint) uintptr
	CreateUhugeint      func(value Uhugeint) uintptr
	CreateDecimal       func(value Decimal) uintptr
	CreateFloat         func(value float32) uintptr
	CreateDouble        func(value float64) uintptr
	CreateDate          func(value Date) uintptr
	CreateTime          func(value Time) uintptr
	CreateTimeNS        func(value TimeNS) uintptr
	CreateTimeTZValue   func(value TimeTZ) uintptr
	CreateTimestamp     func(value Timestamp) uintptr
	CreateTimestampS    func(value TimestampS) uintptr
	CreateTimestampMS   func(value TimestampMS) uintptr
	CreateTimestampNS   func(value TimestampNS) uintptr
	CreateTimestampTZ   func(value Timestamp) uintptr
	CreateInterval      func(value Interval) uintptr
	CreateVarcharLength func(text *byte, length uint64) uintptr
	CreateBlob          func(data *byte, length uint64) uintptr
	CreateBit           func(input Bit) uintptr
	CreateBignum        func(input Bignum) uintptr
	CreateUUID          func(value Uhugeint) uintptr
	CreateEnumValue     func(enumType uintptr, value uint64) uintptr
	CreateListValue     func(elemType uintptr, values *uintptr, count uint64) uintptr
	CreateArrayValue    func(elemType uintptr, values *uintptr, count uint64) uintptr
	CreateStructValue   func(structType uintptr, values *uintptr) uintptr
	CreateMapValue      func(mapType uintptr, keys *uintptr, values *uintptr, count uint64) uintptr
	CreateUnionValue    func(unionType uintptr, tagIndex uint64, value uintptr) uintptr
	// Value inspection helpers. GetValueType returns a BORROWED logical type
	// valid while value is alive (must NOT be destroyed). GetBlob, GetBit, and
	// GetBignum return structs whose Data pointer is owned and freed by Free.
	// GetListChild, GetStructChild, GetMapKey, and GetMapValue return owned
	// duckdb_value handles released by DestroyValue. GetVarchar returns an owned
	// C string released by Free.
	GetValueType   func(value uintptr) uintptr
	IsNullValue    func(value uintptr) bool
	GetBool        func(value uintptr) bool
	GetInt8        func(value uintptr) int8
	GetInt16       func(value uintptr) int16
	GetInt32       func(value uintptr) int32
	GetInt64       func(value uintptr) int64
	GetUint8       func(value uintptr) uint8
	GetUint16      func(value uintptr) uint16
	GetUint32      func(value uintptr) uint32
	GetUint64      func(value uintptr) uint64
	GetHugeint     func(value uintptr) Hugeint
	GetUhugeint    func(value uintptr) Uhugeint
	GetDecimal     func(value uintptr) Decimal
	GetFloat       func(value uintptr) float32
	GetDouble      func(value uintptr) float64
	GetDate        func(value uintptr) Date
	GetTime        func(value uintptr) Time
	GetTimeNS      func(value uintptr) TimeNS
	GetTimeTZ      func(value uintptr) TimeTZ
	GetTimestamp   func(value uintptr) Timestamp
	GetTimestampS  func(value uintptr) TimestampS
	GetTimestampMS func(value uintptr) TimestampMS
	GetTimestampNS func(value uintptr) TimestampNS
	GetTimestampTZ func(value uintptr) Timestamp
	GetInterval    func(value uintptr) Interval
	GetBlob        func(value uintptr) Blob
	GetBit         func(value uintptr) Bit
	GetBignum      func(value uintptr) Bignum
	GetUUID        func(value uintptr) Uhugeint
	GetEnumValue   func(value uintptr) uint64
	GetListSize    func(value uintptr) uint64
	GetListChild   func(value uintptr, index uint64) uintptr
	GetStructChild func(value uintptr, index uint64) uintptr
	GetMapSize     func(value uintptr) uint64
	GetMapKey      func(value uintptr, index uint64) uintptr
	GetMapValue    func(value uintptr, index uint64) uintptr
	GetVarchar     func(value uintptr) uintptr
	DestroyValue   func(value *uintptr)

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

	GetTypeID func(logicalType uintptr) int32
	// LogicalTypeGetAlias returns an OWNED C string released by Free (or 0 if unset).
	LogicalTypeGetAlias func(logicalType uintptr) uintptr
	LogicalTypeSetAlias func(logicalType uintptr, alias *byte)
	DecimalWidth        func(logicalType uintptr) uint8
	DecimalScale        func(logicalType uintptr) uint8
	DecimalInternalType func(logicalType uintptr) int32
	// EnumDictionaryValue returns an OWNED C string released by Free.
	EnumInternalType    func(logicalType uintptr) int32
	EnumDictionarySize  func(logicalType uintptr) uint32
	EnumDictionaryValue func(logicalType uintptr, index uint64) uintptr
	// Child logical types and names below are owned by the caller.
	ListTypeChildType    func(logicalType uintptr) uintptr
	ArrayTypeChildType   func(logicalType uintptr) uintptr
	ArrayTypeArraySize   func(logicalType uintptr) uint64
	MapTypeKeyType       func(logicalType uintptr) uintptr
	MapTypeValueType     func(logicalType uintptr) uintptr
	StructTypeChildCount func(logicalType uintptr) uint64
	StructTypeChildName  func(logicalType uintptr, index uint64) uintptr
	StructTypeChildType  func(logicalType uintptr, index uint64) uintptr
	UnionTypeMemberCount func(logicalType uintptr) uint64
	UnionTypeMemberName  func(logicalType uintptr, index uint64) uintptr
	UnionTypeMemberType  func(logicalType uintptr, index uint64) uintptr
	// GeometryTypeGetCRS returns an OWNED C string released by Free (or 0 if unset).
	GeometryTypeGetCRS func(logicalType uintptr) uintptr
	// Create*Type allocate an owned logical type released by DestroyLogicalType.
	CreateLogicalType  func(typeID int32) uintptr
	CreateDecimalType  func(width, scale uint8) uintptr
	CreateListType     func(childType uintptr) uintptr
	CreateArrayType    func(childType uintptr, arraySize uint64) uintptr
	CreateMapType      func(keyType, valueType uintptr) uintptr
	CreateStructType   func(memberTypes *uintptr, memberNames *uintptr, memberCount uint64) uintptr
	CreateUnionType    func(memberTypes *uintptr, memberNames *uintptr, memberCount uint64) uintptr
	CreateEnumType     func(memberNames *uintptr, memberCount uint64) uintptr
	DestroyLogicalType func(logicalType *uintptr)

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

	// AppenderCreateExt always allocates an owned appender released by
	// AppenderDestroy, even on failure. AppenderColumnType returns an owned
	// logical type; AppenderErrorData returns an owned duckdb_error_data.
	AppenderCreateExt    func(connection uintptr, catalog, schema, table *byte, out *uintptr) int32
	AppenderCreateQuery  func(connection uintptr, query *byte, colCount uint64, types *uintptr, tableName *byte, colNames *uintptr, out *uintptr) int32
	AppenderColumnCount  func(appender uintptr) uint64
	AppenderColumnType   func(appender uintptr, colIdx uint64) uintptr
	AppenderErrorData    func(appender uintptr) uintptr
	AppenderFlush        func(appender uintptr) int32
	AppenderClear        func(appender uintptr) int32
	AppenderClose        func(appender uintptr) int32
	AppenderDestroy      func(appender *uintptr) int32
	AppenderAddColumn    func(appender uintptr, name *byte) int32
	AppenderClearColumns func(appender uintptr) int32
	AppenderEndRow       func(appender uintptr) int32
	AppendDefault        func(appender uintptr) int32
	AppendNull           func(appender uintptr) int32
	AppendBool           func(appender uintptr, value bool) int32
	AppendInt8           func(appender uintptr, value int8) int32
	AppendInt16          func(appender uintptr, value int16) int32
	AppendInt32          func(appender uintptr, value int32) int32
	AppendInt64          func(appender uintptr, value int64) int32
	AppendUint8          func(appender uintptr, value uint8) int32
	AppendUint16         func(appender uintptr, value uint16) int32
	AppendUint32         func(appender uintptr, value uint32) int32
	AppendUint64         func(appender uintptr, value uint64) int32
	AppendHugeint        func(appender uintptr, value Hugeint) int32
	AppendUhugeint       func(appender uintptr, value Uhugeint) int32
	AppendFloat          func(appender uintptr, value float32) int32
	AppendDouble         func(appender uintptr, value float64) int32
	AppendDate           func(appender uintptr, value Date) int32
	AppendTime           func(appender uintptr, value Time) int32
	AppendTimestamp      func(appender uintptr, value Timestamp) int32
	AppendInterval       func(appender uintptr, value Interval) int32
	AppendVarcharLength  func(appender uintptr, val *byte, length uint64) int32
	AppendBlob           func(appender uintptr, data *byte, length uint64) int32
	AppendValue          func(appender uintptr, value uintptr) int32
	AppendDataChunk      func(appender uintptr, chunk uintptr) int32

	// Arrow C Data Interface (v1.5.6 stable).
	// ConnectionGetArrowOptions allocates owned options released by
	// DestroyArrowOptions. ToArrowSchema / DataChunkToArrow / SchemaFromArrow /
	// DataChunkFromArrow return an owned duckdb_error_data released by
	// DestroyErrorData. outSchema / outArray are owned and released via their
	// C release callback (ReleaseArrowSchema / ReleaseArrowArray).
	// DataChunkFromArrow transfers array ownership to the returned DataChunk on
	// success.
	// ConnectionGetArrowOptions and ResultGetArrowOptions allocate owned
	// options released by DestroyArrowOptions.
	ConnectionGetArrowOptions   func(connection uintptr, out *uintptr)
	ResultGetArrowOptions       func(result *Result) uintptr
	DestroyArrowOptions         func(options *uintptr)
	ToArrowSchema               func(options uintptr, types *uintptr, names *uintptr, colCount uint64, outSchema *ArrowSchema) uintptr
	DataChunkToArrow            func(options uintptr, chunk uintptr, outArray *ArrowArray) uintptr
	SchemaFromArrow             func(connection uintptr, schema *ArrowSchema, outConverted *uintptr) uintptr
	DataChunkFromArrow          func(connection uintptr, array *ArrowArray, converted uintptr, outChunk *uintptr) uintptr
	DestroyArrowConvertedSchema func(converted *uintptr)
	// ArrowScan registers a temporary Arrow view backed by a borrowed
	// ArrowArrayStream; DuckDB does not invoke stream.Release, so callers must
	// drop the view and release the stream via ReleaseArrowStream.
	// ArrowArrayScan registers a single-use temporary Arrow view backed by
	// arrowSchema and arrowArray and allocates an owned duckdb_arrow_stream
	// released by DestroyArrowStream. DuckDB borrows arrowSchema and arrowArray
	// until the view is dropped and the stream is destroyed.
	ArrowScan          func(connection uintptr, tableName *byte, stream *ArrowArrayStream) int32
	ArrowArrayScan     func(connection uintptr, tableName *byte, arrowSchema *ArrowSchema, arrowArray *ArrowArray, outStream *uintptr) int32
	DestroyArrowStream func(stream *uintptr)

	// Client VFS / FileSystem (v1.5.6 stable).
	// ClientContextGetFileSystem returns an owned duckdb_file_system released
	// by DestroyFileSystem. CreateFileOpenOptions returns owned options released
	// by DestroyFileOpenOptions. FileSystemOpen writes an owned file handle
	// released by DestroyFileHandle. FileSystemErrorData and
	// FileHandleErrorData return owned duckdb_error_data released by
	// DestroyErrorData.
	ClientContextGetFileSystem func(clientCtx uintptr) uintptr
	DestroyFileSystem          func(fs *uintptr)
	FileSystemErrorData        func(fs uintptr) uintptr
	FileSystemOpen             func(fs uintptr, path *byte, options uintptr, outFile *uintptr) int32
	CreateFileOpenOptions      func() uintptr
	FileOpenOptionsSetFlag     func(options uintptr, flag int32, value bool) int32
	DestroyFileOpenOptions     func(options *uintptr)
	DestroyFileHandle          func(fh *uintptr)
	FileHandleErrorData        func(fh uintptr) uintptr
	FileHandleRead             func(fh uintptr, buf *byte, size int64) int64
	FileHandleWrite            func(fh uintptr, buf *byte, size int64) int64
	FileHandleTell             func(fh uintptr) int64
	FileHandleSize             func(fh uintptr) int64
	FileHandleSeek             func(fh uintptr, position int64) int32
	FileHandleSync             func(fh uintptr) int32
	FileHandleClose            func(fh uintptr) int32

	// Catalog metadata inspection (v1.5.6 stable).
	// ClientContextGetCatalog (requires an active transaction) returns an owned
	// duckdb_catalog released by DestroyCatalog. CatalogGetEntry returns an
	// owned duckdb_catalog_entry released by DestroyCatalogEntry.
	// CatalogGetTypeName and CatalogEntryGetName return borrowed C strings owned
	// by their parent handles.
	ClientContextGetCatalog func(clientCtx uintptr, catalogName *byte) uintptr
	CatalogGetTypeName      func(catalog uintptr) uintptr
	CatalogGetEntry         func(catalog uintptr, clientCtx uintptr, entryType int32, schemaName *byte, entryName *byte) uintptr
	DestroyCatalog          func(catalog *uintptr)
	CatalogEntryGetType     func(entry uintptr) int32
	CatalogEntryGetName     func(entry uintptr) uintptr
	DestroyCatalogEntry     func(entry *uintptr)
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
		{&a.ConfigCount, "duckdb_config_count"},
		{&a.GetConfigFlag, "duckdb_get_config_flag"},
		{&a.OpenExt, "duckdb_open_ext"},
		{&a.CreateInstanceCache, "duckdb_create_instance_cache"},
		{&a.GetOrCreateFromCache, "duckdb_get_or_create_from_cache"},
		{&a.DestroyInstanceCache, "duckdb_destroy_instance_cache"},
		{&a.Close, "duckdb_close"},
		{&a.Free, "duckdb_free"},
		{&a.ErrorDataHasError, "duckdb_error_data_has_error"},
		{&a.ErrorDataErrorType, "duckdb_error_data_error_type"},
		{&a.ErrorDataMessage, "duckdb_error_data_message"},
		{&a.DestroyErrorData, "duckdb_destroy_error_data"},
		{&a.Connect, "duckdb_connect"},
		{&a.Disconnect, "duckdb_disconnect"},
		{&a.ConnectionGetClientContext, "duckdb_connection_get_client_context"},
		{&a.ClientContextGetConnectionID, "duckdb_client_context_get_connection_id"},
		{&a.ClientContextGetConfigOption, "duckdb_client_context_get_config_option"},
		{&a.DestroyClientContext, "duckdb_destroy_client_context"},
		{&a.GetTableNames, "duckdb_get_table_names"},
		{&a.Interrupt, "duckdb_interrupt"},
		{&a.QueryProgress, "duckdb_query_progress"},
		{&a.GetProfilingInfo, "duckdb_get_profiling_info"},
		{&a.ProfilingInfoGetMetrics, "duckdb_profiling_info_get_metrics"},
		{&a.ProfilingInfoGetChildCount, "duckdb_profiling_info_get_child_count"},
		{&a.ProfilingInfoGetChild, "duckdb_profiling_info_get_child"},
		{&a.Prepare, "duckdb_prepare"},
		{&a.PrepareError, "duckdb_prepare_error"},
		{&a.DestroyPrepare, "duckdb_destroy_prepare"},
		{&a.ExtractStatements, "duckdb_extract_statements"},
		{&a.PrepareExtractedStatement, "duckdb_prepare_extracted_statement"},
		{&a.ExtractStatementsError, "duckdb_extract_statements_error"},
		{&a.DestroyExtracted, "duckdb_destroy_extracted"},
		{&a.NParams, "duckdb_nparams"},
		{&a.ParamLogicalType, "duckdb_param_logical_type"},
		{&a.PreparedStatementType, "duckdb_prepared_statement_type"},
		{&a.ParameterName, "duckdb_parameter_name"},
		{&a.BindParameterIndex, "duckdb_bind_parameter_index"},
		{&a.ClearBindings, "duckdb_clear_bindings"},
		{&a.PreparedStatementColumnCount, "duckdb_prepared_statement_column_count"},
		{&a.PreparedStatementColumnName, "duckdb_prepared_statement_column_name"},
		{&a.PreparedStatementColumnLogicalType, "duckdb_prepared_statement_column_logical_type"},
		{&a.TableDescriptionCreateExt, "duckdb_table_description_create_ext"},
		{&a.TableDescriptionDestroy, "duckdb_table_description_destroy"},
		{&a.TableDescriptionError, "duckdb_table_description_error"},
		{&a.TableDescriptionGetColumnCount, "duckdb_table_description_get_column_count"},
		{&a.TableDescriptionGetColumnName, "duckdb_table_description_get_column_name"},
		{&a.TableDescriptionGetColumnType, "duckdb_table_description_get_column_type"},
		{&a.ColumnHasDefault, "duckdb_column_has_default"},
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
		{&a.CreateTimeTZ, "duckdb_create_time_tz"},
		{&a.CreateNullValue, "duckdb_create_null_value"},
		{&a.CreateBool, "duckdb_create_bool"},
		{&a.CreateInt8, "duckdb_create_int8"},
		{&a.CreateInt16, "duckdb_create_int16"},
		{&a.CreateInt32, "duckdb_create_int32"},
		{&a.CreateInt64, "duckdb_create_int64"},
		{&a.CreateUint8, "duckdb_create_uint8"},
		{&a.CreateUint16, "duckdb_create_uint16"},
		{&a.CreateUint32, "duckdb_create_uint32"},
		{&a.CreateUint64, "duckdb_create_uint64"},
		{&a.CreateHugeint, "duckdb_create_hugeint"},
		{&a.CreateUhugeint, "duckdb_create_uhugeint"},
		{&a.CreateDecimal, "duckdb_create_decimal"},
		{&a.CreateFloat, "duckdb_create_float"},
		{&a.CreateDouble, "duckdb_create_double"},
		{&a.CreateDate, "duckdb_create_date"},
		{&a.CreateTime, "duckdb_create_time"},
		{&a.CreateTimeNS, "duckdb_create_time_ns"},
		{&a.CreateTimeTZValue, "duckdb_create_time_tz_value"},
		{&a.CreateTimestamp, "duckdb_create_timestamp"},
		{&a.CreateTimestampS, "duckdb_create_timestamp_s"},
		{&a.CreateTimestampMS, "duckdb_create_timestamp_ms"},
		{&a.CreateTimestampNS, "duckdb_create_timestamp_ns"},
		{&a.CreateTimestampTZ, "duckdb_create_timestamp_tz"},
		{&a.CreateInterval, "duckdb_create_interval"},
		{&a.CreateVarcharLength, "duckdb_create_varchar_length"},
		{&a.CreateBlob, "duckdb_create_blob"},
		{&a.CreateBit, "duckdb_create_bit"},
		{&a.CreateBignum, "duckdb_create_bignum"},
		{&a.CreateUUID, "duckdb_create_uuid"},
		{&a.CreateEnumValue, "duckdb_create_enum_value"},
		{&a.CreateListValue, "duckdb_create_list_value"},
		{&a.CreateArrayValue, "duckdb_create_array_value"},
		{&a.CreateStructValue, "duckdb_create_struct_value"},
		{&a.CreateMapValue, "duckdb_create_map_value"},
		{&a.CreateUnionValue, "duckdb_create_union_value"},
		{&a.GetValueType, "duckdb_get_value_type"},
		{&a.IsNullValue, "duckdb_is_null_value"},
		{&a.GetBool, "duckdb_get_bool"},
		{&a.GetInt8, "duckdb_get_int8"},
		{&a.GetInt16, "duckdb_get_int16"},
		{&a.GetInt32, "duckdb_get_int32"},
		{&a.GetInt64, "duckdb_get_int64"},
		{&a.GetUint8, "duckdb_get_uint8"},
		{&a.GetUint16, "duckdb_get_uint16"},
		{&a.GetUint32, "duckdb_get_uint32"},
		{&a.GetUint64, "duckdb_get_uint64"},
		{&a.GetHugeint, "duckdb_get_hugeint"},
		{&a.GetUhugeint, "duckdb_get_uhugeint"},
		{&a.GetDecimal, "duckdb_get_decimal"},
		{&a.GetFloat, "duckdb_get_float"},
		{&a.GetDouble, "duckdb_get_double"},
		{&a.GetDate, "duckdb_get_date"},
		{&a.GetTime, "duckdb_get_time"},
		{&a.GetTimeNS, "duckdb_get_time_ns"},
		{&a.GetTimeTZ, "duckdb_get_time_tz"},
		{&a.GetTimestamp, "duckdb_get_timestamp"},
		{&a.GetTimestampS, "duckdb_get_timestamp_s"},
		{&a.GetTimestampMS, "duckdb_get_timestamp_ms"},
		{&a.GetTimestampNS, "duckdb_get_timestamp_ns"},
		{&a.GetTimestampTZ, "duckdb_get_timestamp_tz"},
		{&a.GetInterval, "duckdb_get_interval"},
		{&a.GetBlob, "duckdb_get_blob"},
		{&a.GetBit, "duckdb_get_bit"},
		{&a.GetBignum, "duckdb_get_bignum"},
		{&a.GetUUID, "duckdb_get_uuid"},
		{&a.GetEnumValue, "duckdb_get_enum_value"},
		{&a.GetListSize, "duckdb_get_list_size"},
		{&a.GetListChild, "duckdb_get_list_child"},
		{&a.GetStructChild, "duckdb_get_struct_child"},
		{&a.GetMapSize, "duckdb_get_map_size"},
		{&a.GetMapKey, "duckdb_get_map_key"},
		{&a.GetMapValue, "duckdb_get_map_value"},
		{&a.GetVarchar, "duckdb_get_varchar"},
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
		{&a.LogicalTypeGetAlias, "duckdb_logical_type_get_alias"},
		{&a.LogicalTypeSetAlias, "duckdb_logical_type_set_alias"},
		{&a.DecimalWidth, "duckdb_decimal_width"},
		{&a.DecimalScale, "duckdb_decimal_scale"},
		{&a.DecimalInternalType, "duckdb_decimal_internal_type"},
		{&a.EnumInternalType, "duckdb_enum_internal_type"},
		{&a.EnumDictionarySize, "duckdb_enum_dictionary_size"},
		{&a.EnumDictionaryValue, "duckdb_enum_dictionary_value"},
		{&a.ListTypeChildType, "duckdb_list_type_child_type"},
		{&a.ArrayTypeChildType, "duckdb_array_type_child_type"},
		{&a.ArrayTypeArraySize, "duckdb_array_type_array_size"},
		{&a.MapTypeKeyType, "duckdb_map_type_key_type"},
		{&a.MapTypeValueType, "duckdb_map_type_value_type"},
		{&a.StructTypeChildCount, "duckdb_struct_type_child_count"},
		{&a.StructTypeChildName, "duckdb_struct_type_child_name"},
		{&a.StructTypeChildType, "duckdb_struct_type_child_type"},
		{&a.UnionTypeMemberCount, "duckdb_union_type_member_count"},
		{&a.UnionTypeMemberName, "duckdb_union_type_member_name"},
		{&a.UnionTypeMemberType, "duckdb_union_type_member_type"},
		{&a.GeometryTypeGetCRS, "duckdb_geometry_type_get_crs"},
		{&a.CreateLogicalType, "duckdb_create_logical_type"},
		{&a.CreateDecimalType, "duckdb_create_decimal_type"},
		{&a.CreateListType, "duckdb_create_list_type"},
		{&a.CreateArrayType, "duckdb_create_array_type"},
		{&a.CreateMapType, "duckdb_create_map_type"},
		{&a.CreateStructType, "duckdb_create_struct_type"},
		{&a.CreateUnionType, "duckdb_create_union_type"},
		{&a.CreateEnumType, "duckdb_create_enum_type"},
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
		{&a.AppenderCreateExt, "duckdb_appender_create_ext"},
		{&a.AppenderCreateQuery, "duckdb_appender_create_query"},
		{&a.AppenderColumnCount, "duckdb_appender_column_count"},
		{&a.AppenderColumnType, "duckdb_appender_column_type"},
		{&a.AppenderErrorData, "duckdb_appender_error_data"},
		{&a.AppenderFlush, "duckdb_appender_flush"},
		{&a.AppenderClear, "duckdb_appender_clear"},
		{&a.AppenderClose, "duckdb_appender_close"},
		{&a.AppenderDestroy, "duckdb_appender_destroy"},
		{&a.AppenderAddColumn, "duckdb_appender_add_column"},
		{&a.AppenderClearColumns, "duckdb_appender_clear_columns"},
		{&a.AppenderEndRow, "duckdb_appender_end_row"},
		{&a.AppendDefault, "duckdb_append_default"},
		{&a.AppendNull, "duckdb_append_null"},
		{&a.AppendBool, "duckdb_append_bool"},
		{&a.AppendInt8, "duckdb_append_int8"},
		{&a.AppendInt16, "duckdb_append_int16"},
		{&a.AppendInt32, "duckdb_append_int32"},
		{&a.AppendInt64, "duckdb_append_int64"},
		{&a.AppendUint8, "duckdb_append_uint8"},
		{&a.AppendUint16, "duckdb_append_uint16"},
		{&a.AppendUint32, "duckdb_append_uint32"},
		{&a.AppendUint64, "duckdb_append_uint64"},
		{&a.AppendHugeint, "duckdb_append_hugeint"},
		{&a.AppendUhugeint, "duckdb_append_uhugeint"},
		{&a.AppendFloat, "duckdb_append_float"},
		{&a.AppendDouble, "duckdb_append_double"},
		{&a.AppendDate, "duckdb_append_date"},
		{&a.AppendTime, "duckdb_append_time"},
		{&a.AppendTimestamp, "duckdb_append_timestamp"},
		{&a.AppendInterval, "duckdb_append_interval"},
		{&a.AppendVarcharLength, "duckdb_append_varchar_length"},
		{&a.AppendBlob, "duckdb_append_blob"},
		{&a.AppendValue, "duckdb_append_value"},
		{&a.AppendDataChunk, "duckdb_append_data_chunk"},
		{&a.ConnectionGetArrowOptions, "duckdb_connection_get_arrow_options"},
		{&a.ResultGetArrowOptions, "duckdb_result_get_arrow_options"},
		{&a.DestroyArrowOptions, "duckdb_destroy_arrow_options"},
		{&a.ToArrowSchema, "duckdb_to_arrow_schema"},
		{&a.DataChunkToArrow, "duckdb_data_chunk_to_arrow"},
		{&a.SchemaFromArrow, "duckdb_schema_from_arrow"},
		{&a.DataChunkFromArrow, "duckdb_data_chunk_from_arrow"},
		{&a.DestroyArrowConvertedSchema, "duckdb_destroy_arrow_converted_schema"},
		{&a.ArrowScan, "duckdb_arrow_scan"},
		{&a.ArrowArrayScan, "duckdb_arrow_array_scan"},
		{&a.DestroyArrowStream, "duckdb_destroy_arrow_stream"},
		{&a.ClientContextGetFileSystem, "duckdb_client_context_get_file_system"},
		{&a.DestroyFileSystem, "duckdb_destroy_file_system"},
		{&a.FileSystemErrorData, "duckdb_file_system_error_data"},
		{&a.FileSystemOpen, "duckdb_file_system_open"},
		{&a.CreateFileOpenOptions, "duckdb_create_file_open_options"},
		{&a.FileOpenOptionsSetFlag, "duckdb_file_open_options_set_flag"},
		{&a.DestroyFileOpenOptions, "duckdb_destroy_file_open_options"},
		{&a.DestroyFileHandle, "duckdb_destroy_file_handle"},
		{&a.FileHandleErrorData, "duckdb_file_handle_error_data"},
		{&a.FileHandleRead, "duckdb_file_handle_read"},
		{&a.FileHandleWrite, "duckdb_file_handle_write"},
		{&a.FileHandleTell, "duckdb_file_handle_tell"},
		{&a.FileHandleSize, "duckdb_file_handle_size"},
		{&a.FileHandleSeek, "duckdb_file_handle_seek"},
		{&a.FileHandleSync, "duckdb_file_handle_sync"},
		{&a.FileHandleClose, "duckdb_file_handle_close"},
		{&a.ClientContextGetCatalog, "duckdb_client_context_get_catalog"},
		{&a.CatalogGetTypeName, "duckdb_catalog_get_type_name"},
		{&a.CatalogGetEntry, "duckdb_catalog_get_entry"},
		{&a.DestroyCatalog, "duckdb_destroy_catalog"},
		{&a.CatalogEntryGetType, "duckdb_catalog_entry_get_type"},
		{&a.CatalogEntryGetName, "duckdb_catalog_entry_get_name"},
		{&a.DestroyCatalogEntry, "duckdb_destroy_catalog_entry"},
	}
}
