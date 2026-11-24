package duckdb

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"strings"

	"github.com/bububa/duckdbspike/internal/abi"
)

// StatementType classifies a prepared SQL statement as reported by DuckDB.
type StatementType int32

const (
	StatementTypeInvalid          StatementType = StatementType(abi.StatementTypeInvalid)
	StatementTypeSelect           StatementType = StatementType(abi.StatementTypeSelect)
	StatementTypeInsert           StatementType = StatementType(abi.StatementTypeInsert)
	StatementTypeUpdate           StatementType = StatementType(abi.StatementTypeUpdate)
	StatementTypeExplain          StatementType = StatementType(abi.StatementTypeExplain)
	StatementTypeDelete           StatementType = StatementType(abi.StatementTypeDelete)
	StatementTypePrepare          StatementType = StatementType(abi.StatementTypePrepare)
	StatementTypeCreate           StatementType = StatementType(abi.StatementTypeCreate)
	StatementTypeExecute          StatementType = StatementType(abi.StatementTypeExecute)
	StatementTypeAlter            StatementType = StatementType(abi.StatementTypeAlter)
	StatementTypeTransaction      StatementType = StatementType(abi.StatementTypeTransaction)
	StatementTypeCopy             StatementType = StatementType(abi.StatementTypeCopy)
	StatementTypeAnalyze          StatementType = StatementType(abi.StatementTypeAnalyze)
	StatementTypeVariableSet      StatementType = StatementType(abi.StatementTypeVariableSet)
	StatementTypeCreateFunc       StatementType = StatementType(abi.StatementTypeCreateFunc)
	StatementTypeDrop             StatementType = StatementType(abi.StatementTypeDrop)
	StatementTypeExport           StatementType = StatementType(abi.StatementTypeExport)
	StatementTypePragma           StatementType = StatementType(abi.StatementTypePragma)
	StatementTypeVacuum           StatementType = StatementType(abi.StatementTypeVacuum)
	StatementTypeCall             StatementType = StatementType(abi.StatementTypeCall)
	StatementTypeSet              StatementType = StatementType(abi.StatementTypeSet)
	StatementTypeLoad             StatementType = StatementType(abi.StatementTypeLoad)
	StatementTypeRelation         StatementType = StatementType(abi.StatementTypeRelation)
	StatementTypeExtension        StatementType = StatementType(abi.StatementTypeExtension)
	StatementTypeLogicalPlan      StatementType = StatementType(abi.StatementTypeLogicalPlan)
	StatementTypeAttach           StatementType = StatementType(abi.StatementTypeAttach)
	StatementTypeDetach           StatementType = StatementType(abi.StatementTypeDetach)
	StatementTypeMulti            StatementType = StatementType(abi.StatementTypeMulti)
	StatementTypeCopyDatabase     StatementType = StatementType(abi.StatementTypeCopyDatabase)
	StatementTypeUpdateExtensions StatementType = StatementType(abi.StatementTypeUpdateExtensions)
	StatementTypeMergeInto        StatementType = StatementType(abi.StatementTypeMergeInto)
)

// String returns a human-readable name for the statement type.
func (t StatementType) String() string {
	switch t {
	case StatementTypeInvalid:
		return "INVALID"
	case StatementTypeSelect:
		return "SELECT"
	case StatementTypeInsert:
		return "INSERT"
	case StatementTypeUpdate:
		return "UPDATE"
	case StatementTypeExplain:
		return "EXPLAIN"
	case StatementTypeDelete:
		return "DELETE"
	case StatementTypePrepare:
		return "PREPARE"
	case StatementTypeCreate:
		return "CREATE"
	case StatementTypeExecute:
		return "EXECUTE"
	case StatementTypeAlter:
		return "ALTER"
	case StatementTypeTransaction:
		return "TRANSACTION"
	case StatementTypeCopy:
		return "COPY"
	case StatementTypeAnalyze:
		return "ANALYZE"
	case StatementTypeVariableSet:
		return "VARIABLE_SET"
	case StatementTypeCreateFunc:
		return "CREATE_FUNC"
	case StatementTypeDrop:
		return "DROP"
	case StatementTypeExport:
		return "EXPORT"
	case StatementTypePragma:
		return "PRAGMA"
	case StatementTypeVacuum:
		return "VACUUM"
	case StatementTypeCall:
		return "CALL"
	case StatementTypeSet:
		return "SET"
	case StatementTypeLoad:
		return "LOAD"
	case StatementTypeRelation:
		return "RELATION"
	case StatementTypeExtension:
		return "EXTENSION"
	case StatementTypeLogicalPlan:
		return "LOGICAL_PLAN"
	case StatementTypeAttach:
		return "ATTACH"
	case StatementTypeDetach:
		return "DETACH"
	case StatementTypeMulti:
		return "MULTI"
	case StatementTypeCopyDatabase:
		return "COPY_DATABASE"
	case StatementTypeUpdateExtensions:
		return "UPDATE_EXTENSIONS"
	case StatementTypeMergeInto:
		return "MERGE_INTO"
	default:
		return fmt.Sprintf("STATEMENT_TYPE(%d)", int32(t))
	}
}

// StatementType returns the SQL statement category (SELECT, INSERT, CREATE,
// MERGE_INTO, etc.) for the prepared statement.
func (s *Stmt) StatementType() (StatementType, error) {
	if s == nil {
		return StatementTypeInvalid, ErrClosed
	}
	c := s.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if s.closed || c.closed {
		return StatementTypeInvalid, ErrClosed
	}
	return StatementType(c.api.PreparedStatementType(s.handle)), nil
}

// ParamTypes returns the expected DuckDB logical type for each declared
// parameter in 1-based ordinal order. Parameters whose types are polymorphic or
// not yet constrained by DuckDB (e.g. `SELECT ?`) are reported with Kind ==
// TypeInvalid.
func (s *Stmt) ParamTypes() ([]LogicalType, error) {
	if s == nil {
		return nil, ErrClosed
	}
	c := s.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if s.closed || c.closed {
		return nil, ErrClosed
	}
	api := c.api
	n := api.NParams(s.handle)
	if n > math.MaxInt {
		return nil, fmt.Errorf("%w: parameter count %d overflows int", ErrInvalidInput, n)
	}
	types := make([]LogicalType, int(n))
	for i := uint64(0); i < n; i++ {
		lt := api.ParamLogicalType(s.handle, i+1)
		if lt == 0 {
			types[i] = LogicalType{Kind: TypeInvalid}
			continue
		}
		typeID := api.GetTypeID(lt)
		if typeID == abi.TypeAny || typeID == abi.TypeInvalid {
			api.DestroyLogicalType(&lt)
			types[i] = LogicalType{Kind: TypeInvalid}
			continue
		}
		plan, err := buildColumnPlan(api, lt, 0, c.limits)
		api.DestroyLogicalType(&lt)
		if err != nil {
			return nil, fmt.Errorf("parameter %d: %w", i+1, err)
		}
		types[i] = plan.lt
	}
	return types, nil
}

// TableColumn describes one column of a DuckDB table returned by DescribeTable.
type TableColumn struct {
	Name       string
	Type       LogicalType
	HasDefault bool
}

// DescribeTable inspects the column schema and DEFAULT presence of a table in
// catalog.schema.table. Empty catalog or schema selects DuckDB's default
// catalog or schema respectively.
func (c *Conn) DescribeTable(ctx context.Context, catalog, schema, table string) ([]TableColumn, error) {
	return c.describeTableWithTx(ctx, nil, catalog, schema, table)
}

// DescribeTable inspects a table's column metadata within this transaction.
func (t *Tx) DescribeTable(ctx context.Context, catalog, schema, table string) ([]TableColumn, error) {
	if t == nil {
		return nil, ErrTxDone
	}
	return t.conn.describeTableWithTx(ctx, t, catalog, schema, table)
}

func (c *Conn) describeTableWithTx(ctx context.Context, tx *Tx, catalog, schema, table string) ([]TableColumn, error) {
	if c == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if table == "" {
		return nil, fmt.Errorf("%w: table name must not be empty", ErrInvalidInput)
	}
	ctable, err := abi.CString(table)
	if err != nil {
		return nil, fmt.Errorf("%w: table: %v", ErrInvalidInput, err)
	}
	var ccat, csch []byte
	var catPtr, schPtr *byte
	if catalog != "" {
		ccat, err = abi.CString(catalog)
		if err != nil {
			return nil, fmt.Errorf("%w: catalog: %v", ErrInvalidInput, err)
		}
		catPtr = &ccat[0]
	}
	if schema != "" {
		csch, err = abi.CString(schema)
		if err != nil {
			return nil, fmt.Errorf("%w: schema: %v", ErrInvalidInput, err)
		}
		schPtr = &csch[0]
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkTxLocked(tx); err != nil {
		return nil, err
	}
	if err := c.usableLocked(); err != nil {
		return nil, err
	}
	api := c.api
	var td uintptr
	state := api.TableDescriptionCreateExt(c.handle, catPtr, schPtr, &ctable[0], &td)
	runtime.KeepAlive(ccat)
	runtime.KeepAlive(csch)
	runtime.KeepAlive(ctable)
	defer func() {
		if td != 0 {
			api.TableDescriptionDestroy(&td)
		}
	}()
	if state != abi.StateSuccess {
		msg := abi.GoString(api.TableDescriptionError(td))
		errType := abi.ErrorInvalid
		if strings.Contains(msg, "No table with that") {
			errType = abi.ErrorCatalog
		}
		return nil, nativeError("describe_table", errType, msg)
	}
	count := api.TableDescriptionGetColumnCount(td)
	if count > math.MaxInt {
		return nil, fmt.Errorf("%w: column count %d overflows int", ErrInvalidInput, count)
	}
	cols := make([]TableColumn, int(count))
	for i := uint64(0); i < count; i++ {
		namePtr := api.TableDescriptionGetColumnName(td, i)
		name := abi.GoString(namePtr)
		if namePtr != 0 {
			api.Free(namePtr)
		}
		lt := api.TableDescriptionGetColumnType(td, i)
		plan, planErr := buildColumnPlan(api, lt, 0, c.limits)
		api.DestroyLogicalType(&lt)
		if planErr != nil {
			return nil, fmt.Errorf("column %d (%q): %w", i, name, planErr)
		}
		var hasDefault bool
		if api.ColumnHasDefault(td, i, &hasDefault) != abi.StateSuccess {
			msg := abi.GoString(api.TableDescriptionError(td))
			return nil, nativeError("column_has_default", abi.ErrorInvalid, msg)
		}
		cols[i] = TableColumn{
			Name:       name,
			Type:       plan.lt,
			HasDefault: hasDefault,
		}
	}
	return cols, nil
}

// ConfigFlag describes one DuckDB configuration option available for Open.
type ConfigFlag struct {
	Name        string
	Description string
}

// ConfigFlags returns all configuration options supported by the loaded DuckDB
// library.
func (l *Library) ConfigFlags() ([]ConfigFlag, error) {
	if l == nil || l.api == nil {
		return nil, ErrClosed
	}
	api := l.api
	n := api.ConfigCount()
	flags := make([]ConfigFlag, 0, n)
	for i := uintptr(0); i < n; i++ {
		var namePtr, descPtr uintptr
		if api.GetConfigFlag(i, &namePtr, &descPtr) != abi.StateSuccess {
			break
		}
		flags = append(flags, ConfigFlag{
			Name:        abi.GoString(namePtr),
			Description: abi.GoString(descPtr),
		})
	}
	return flags, nil
}

// ConfigOptionScope reports the scope from which a DuckDB runtime configuration
// option was resolved.
type ConfigOptionScope int32

const (
	ConfigOptionScopeInvalid ConfigOptionScope = ConfigOptionScope(abi.ConfigOptionScopeInvalid)
	ConfigOptionScopeLocal   ConfigOptionScope = ConfigOptionScope(abi.ConfigOptionScopeLocal)
	ConfigOptionScopeSession ConfigOptionScope = ConfigOptionScope(abi.ConfigOptionScopeSession)
	ConfigOptionScopeGlobal  ConfigOptionScope = ConfigOptionScope(abi.ConfigOptionScopeGlobal)
)

// String returns a human-readable scope name.
func (s ConfigOptionScope) String() string {
	switch s {
	case ConfigOptionScopeInvalid:
		return "INVALID"
	case ConfigOptionScopeLocal:
		return "LOCAL"
	case ConfigOptionScopeSession:
		return "SESSION"
	case ConfigOptionScopeGlobal:
		return "GLOBAL"
	default:
		return fmt.Sprintf("CONFIG_OPTION_SCOPE(%d)", int32(s))
	}
}

// ConfigOptionValue holds the strongly-typed value and effective scope of a
// runtime DuckDB configuration option retrieved via Conn.ConfigOption.
type ConfigOptionValue struct {
	Value Value
	Scope ConfigOptionScope
}

// ConnectionID returns DuckDB's unique connection identifier for c.
func (c *Conn) ConnectionID() (uint64, error) {
	if c == nil {
		return 0, ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return 0, err
	}
	api := c.api
	var clientCtx uintptr
	api.ConnectionGetClientContext(c.handle, &clientCtx)
	if clientCtx == 0 {
		return 0, nativeError("connection_get_client_context", abi.ErrorInvalid, "")
	}
	defer api.DestroyClientContext(&clientCtx)
	return api.ClientContextGetConnectionID(clientCtx), nil
}

// ConfigOption retrieves the current strongly-typed value and scope of the
// configuration option named name on c without running a SQL query. When name
// does not match any known option, ok is false and err is nil.
func (c *Conn) ConfigOption(name string) (ConfigOptionValue, bool, error) {
	if c == nil {
		return ConfigOptionValue{}, false, ErrClosed
	}
	if name == "" {
		return ConfigOptionValue{}, false, fmt.Errorf("%w: config option name must not be empty", ErrInvalidInput)
	}
	cname, err := abi.CString(name)
	if err != nil {
		return ConfigOptionValue{}, false, fmt.Errorf("%w: config option name: %v", ErrInvalidInput, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return ConfigOptionValue{}, false, err
	}
	api := c.api
	var clientCtx uintptr
	api.ConnectionGetClientContext(c.handle, &clientCtx)
	if clientCtx == 0 {
		return ConfigOptionValue{}, false, nativeError("connection_get_client_context", abi.ErrorInvalid, "")
	}
	defer api.DestroyClientContext(&clientCtx)

	var scope int32
	valHandle := api.ClientContextGetConfigOption(clientCtx, &cname[0], &scope)
	runtime.KeepAlive(cname)
	if valHandle == 0 || scope == abi.ConfigOptionScopeInvalid {
		if valHandle != 0 {
			api.DestroyValue(&valHandle)
		}
		return ConfigOptionValue{Scope: ConfigOptionScopeInvalid}, false, nil
	}
	defer api.DestroyValue(&valHandle)

	val, err := decodeNativeValue(api, valHandle, 0, c.limits)
	if err != nil {
		return ConfigOptionValue{}, true, fmt.Errorf("config option %q: %w", name, err)
	}
	return ConfigOptionValue{
		Value: val,
		Scope: ConfigOptionScope(scope),
	}, true, nil
}

// TableNames parses sql without executing it and returns the table names
// referenced by the query. When qualified is true, DuckDB returns fully
// qualified catalog.schema.table names.
func (c *Conn) TableNames(ctx context.Context, sql string, qualified bool) ([]string, error) {
	return c.tableNamesWithTx(ctx, nil, sql, qualified)
}

// TableNames parses sql without executing it within tx and returns the
// referenced table names.
func (t *Tx) TableNames(ctx context.Context, sql string, qualified bool) ([]string, error) {
	if t == nil {
		return nil, ErrTxDone
	}
	return t.conn.tableNamesWithTx(ctx, t, sql, qualified)
}

func (c *Conn) tableNamesWithTx(ctx context.Context, tx *Tx, sql string, qualified bool) ([]string, error) {
	if c == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	csql, err := abi.CString(sql)
	if err != nil {
		return nil, fmt.Errorf("%w: sql: %v", ErrInvalidInput, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkTxLocked(tx); err != nil {
		return nil, err
	}
	if err := c.usableLocked(); err != nil {
		return nil, err
	}
	api := c.api
	listVal := api.GetTableNames(c.handle, &csql[0], qualified)
	runtime.KeepAlive(csql)
	if listVal == 0 {
		return nil, nativeError("get_table_names", abi.ErrorParser, "failed to parse SQL for table names")
	}
	defer api.DestroyValue(&listVal)

	n := api.GetListSize(listVal)
	if n > c.limits.MaxCollectionLen || n > math.MaxInt {
		return nil, fmt.Errorf("%w: table name count %d exceeds %d", ErrLimitExceeded, n, c.limits.MaxCollectionLen)
	}
	names := make([]string, 0, int(n))
	for i := uint64(0); i < n; i++ {
		elem := api.GetListChild(listVal, i)
		if elem == 0 {
			continue
		}
		ptr := api.GetVarchar(elem)
		name := abi.GoString(ptr)
		if ptr != 0 {
			api.Free(ptr)
		}
		api.DestroyValue(&elem)
		names = append(names, name)
	}
	return names, nil
}
