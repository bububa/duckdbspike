package duckdb

import (
	"context"
	"fmt"
	"runtime"

	"github.com/bububa/duckdbspike/internal/abi"
)

// CatalogEntryType classifies an object stored in a DuckDB catalog.
type CatalogEntryType int32

const (
	CatalogEntryInvalid           CatalogEntryType = CatalogEntryType(abi.CatalogEntryTypeInvalid)
	CatalogEntryTable             CatalogEntryType = CatalogEntryType(abi.CatalogEntryTypeTable)
	CatalogEntrySchema            CatalogEntryType = CatalogEntryType(abi.CatalogEntryTypeSchema)
	CatalogEntryView              CatalogEntryType = CatalogEntryType(abi.CatalogEntryTypeView)
	CatalogEntryIndex             CatalogEntryType = CatalogEntryType(abi.CatalogEntryTypeIndex)
	CatalogEntryPreparedStatement CatalogEntryType = CatalogEntryType(abi.CatalogEntryTypePreparedStatement)
	CatalogEntrySequence          CatalogEntryType = CatalogEntryType(abi.CatalogEntryTypeSequence)
	CatalogEntryCollation         CatalogEntryType = CatalogEntryType(abi.CatalogEntryTypeCollation)
	CatalogEntryTypeDef           CatalogEntryType = CatalogEntryType(abi.CatalogEntryTypeType)
	CatalogEntryDatabase          CatalogEntryType = CatalogEntryType(abi.CatalogEntryTypeDatabase)
)

// String returns a human-readable catalog entry type name.
func (t CatalogEntryType) String() string {
	switch t {
	case CatalogEntryInvalid:
		return "INVALID"
	case CatalogEntryTable:
		return "TABLE"
	case CatalogEntrySchema:
		return "SCHEMA"
	case CatalogEntryView:
		return "VIEW"
	case CatalogEntryIndex:
		return "INDEX"
	case CatalogEntryPreparedStatement:
		return "PREPARED_STATEMENT"
	case CatalogEntrySequence:
		return "SEQUENCE"
	case CatalogEntryCollation:
		return "COLLATION"
	case CatalogEntryTypeDef:
		return "TYPE"
	case CatalogEntryDatabase:
		return "DATABASE"
	default:
		return fmt.Sprintf("CATALOG_ENTRY_TYPE(%d)", int32(t))
	}
}

// CatalogEntry holds copied metadata for one DuckDB catalog entry.
type CatalogEntry struct {
	Type CatalogEntryType
	Name string
}

// CatalogTypeName returns the catalog driver type name (for example "duckdb")
// for the named catalog (for example "memory", "system", "temp", or an
// attached database name). When c has no active transaction, CatalogTypeName
// transparently wraps the lookup in a short-lived read-only transaction as
// required by DuckDB's C API.
func (c *Conn) CatalogTypeName(ctx context.Context, catalogName string) (string, bool, error) {
	return c.catalogTypeNameWithTx(ctx, nil, catalogName)
}

// CatalogTypeName returns the catalog driver type name within tx without
// executing any SQL.
func (tx *Tx) CatalogTypeName(ctx context.Context, catalogName string) (string, bool, error) {
	if tx == nil || tx.conn == nil {
		return "", false, ErrTxDone
	}
	return tx.conn.catalogTypeNameWithTx(ctx, tx, catalogName)
}

// LookupCatalogEntry looks up a catalog object by catalogName, entryType,
// schema, and name (for example catalogName "memory", schema "main"). Empty
// schema selects "main". Returns (zero, false, nil) if the catalog or entry
// does not exist. When c has no active transaction, LookupCatalogEntry
// transparently wraps the lookup in a short-lived read-only transaction.
func (c *Conn) LookupCatalogEntry(ctx context.Context, catalogName string, entryType CatalogEntryType, schema, name string) (CatalogEntry, bool, error) {
	return c.lookupCatalogEntryWithTx(ctx, nil, catalogName, entryType, schema, name)
}

// LookupCatalogEntry looks up a catalog object within tx without executing SQL.
func (tx *Tx) LookupCatalogEntry(ctx context.Context, catalogName string, entryType CatalogEntryType, schema, name string) (CatalogEntry, bool, error) {
	if tx == nil || tx.conn == nil {
		return CatalogEntry{}, false, ErrTxDone
	}
	return tx.conn.lookupCatalogEntryWithTx(ctx, tx, catalogName, entryType, schema, name)
}

// withActiveTxLocked invokes fn with a guaranteed active DuckDB transaction on
// c. If c already has an active transaction (c.tx != nil), fn runs directly
// without executing SQL; otherwise a temporary read-only transaction is begun
// and rolled back immediately after fn returns.
func (c *Conn) withActiveTxLocked(ctx context.Context, fn func() error) error {
	if c.tx != nil {
		return fn()
	}
	scope, err := c.beginScopeLocked(ctx)
	if err != nil {
		return err
	}
	defer c.endScopeLocked(scope)

	beginStmt, err := c.prepareLocked("BEGIN TRANSACTION READ ONLY", scope)
	if err != nil {
		return err
	}
	_, execErr := beginStmt.execNoFinishLocked(func() error { return beginStmt.bindLocked(nil) }, scope)
	beginStmt.releaseLocked()
	if execErr != nil {
		return execErr
	}

	fnErr := fn()

	rbStmt, rbPrepErr := c.prepareLocked("ROLLBACK", scope)
	if rbPrepErr == nil {
		_, _ = rbStmt.execNoFinishLocked(func() error { return rbStmt.bindLocked(nil) }, scope)
		rbStmt.releaseLocked()
	}
	if !scope.tryFinish() && fnErr == nil {
		return scope.err()
	}
	return fnErr
}

func (c *Conn) catalogTypeNameWithTx(ctx context.Context, tx *Tx, catalogName string) (string, bool, error) {
	if c == nil {
		return "", false, ErrClosed
	}
	if ctx == nil {
		return "", false, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	if catalogName == "" {
		return "", false, fmt.Errorf("%w: catalogName must not be empty", ErrInvalidInput)
	}
	ccat, err := abi.CString(catalogName)
	if err != nil {
		return "", false, fmt.Errorf("%w: catalogName: %v", ErrInvalidInput, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkTxLocked(tx); err != nil {
		return "", false, err
	}
	if err := c.usableLocked(); err != nil {
		return "", false, err
	}

	var (
		typeName string
		found    bool
	)
	runErr := c.withActiveTxLocked(ctx, func() error {
		api := c.api
		var clientCtx uintptr
		api.ConnectionGetClientContext(c.handle, &clientCtx)
		if clientCtx == 0 {
			return nativeError("connection_get_client_context", abi.ErrorInvalid, "")
		}
		defer api.DestroyClientContext(&clientCtx)

		cat := api.ClientContextGetCatalog(clientCtx, &ccat[0])
		runtime.KeepAlive(ccat)
		if cat == 0 {
			return nil
		}
		defer api.DestroyCatalog(&cat)

		// Borrowed string owned by cat; copy before DestroyCatalog.
		typeName = abi.GoString(api.CatalogGetTypeName(cat))
		found = true
		return nil
	})
	if runErr != nil {
		return "", false, runErr
	}
	return typeName, found, nil
}

func (c *Conn) lookupCatalogEntryWithTx(ctx context.Context, tx *Tx, catalogName string, entryType CatalogEntryType, schema, name string) (CatalogEntry, bool, error) {
	if c == nil {
		return CatalogEntry{}, false, ErrClosed
	}
	if ctx == nil {
		return CatalogEntry{}, false, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return CatalogEntry{}, false, err
	}
	if entryType == CatalogEntryInvalid {
		return CatalogEntry{}, false, fmt.Errorf("%w: invalid catalog entry type", ErrInvalidInput)
	}
	if name == "" {
		return CatalogEntry{}, false, fmt.Errorf("%w: catalog entry name must not be empty", ErrInvalidInput)
	}
	if schema == "" {
		schema = "main"
	}
	if catalogName == "" {
		return CatalogEntry{}, false, fmt.Errorf("%w: catalogName must not be empty", ErrInvalidInput)
	}
	ccat, err := abi.CString(catalogName)
	if err != nil {
		return CatalogEntry{}, false, fmt.Errorf("%w: catalogName: %v", ErrInvalidInput, err)
	}
	csch, err := abi.CString(schema)
	if err != nil {
		return CatalogEntry{}, false, fmt.Errorf("%w: schema: %v", ErrInvalidInput, err)
	}
	cname, err := abi.CString(name)
	if err != nil {
		return CatalogEntry{}, false, fmt.Errorf("%w: name: %v", ErrInvalidInput, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkTxLocked(tx); err != nil {
		return CatalogEntry{}, false, err
	}
	if err := c.usableLocked(); err != nil {
		return CatalogEntry{}, false, err
	}

	var (
		out   CatalogEntry
		found bool
	)
	runErr := c.withActiveTxLocked(ctx, func() error {
		api := c.api
		var clientCtx uintptr
		api.ConnectionGetClientContext(c.handle, &clientCtx)
		if clientCtx == 0 {
			return nativeError("connection_get_client_context", abi.ErrorInvalid, "")
		}
		defer api.DestroyClientContext(&clientCtx)

		cat := api.ClientContextGetCatalog(clientCtx, &ccat[0])
		runtime.KeepAlive(ccat)
		if cat == 0 {
			return nil
		}
		defer api.DestroyCatalog(&cat)

		entry := api.CatalogGetEntry(cat, clientCtx, int32(entryType), &csch[0], &cname[0])
		runtime.KeepAlive(csch)
		runtime.KeepAlive(cname)
		if entry == 0 {
			return nil
		}
		defer api.DestroyCatalogEntry(&entry)

		// Borrowed string owned by entry; copy before DestroyCatalogEntry.
		out = CatalogEntry{
			Type: CatalogEntryType(api.CatalogEntryGetType(entry)),
			Name: abi.GoString(api.CatalogEntryGetName(entry)),
		}
		found = true
		return nil
	})
	if runErr != nil {
		return CatalogEntry{}, false, runErr
	}
	return out, found, nil
}
