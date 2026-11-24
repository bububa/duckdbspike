// Package duckdbsql provides a database/sql/driver adapter backed by package
// duckdb. It preserves streaming-only queries, cooperative cancellation,
// explicit library verification, and exact SQL types, while giving callers
// standard sql.DB pooling and a RawConn escape hatch to *duckdb.Conn.
package duckdbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/bububa/duckdbspike/duckdb"
)

type connectorConfig struct {
	cache           *duckdb.InstanceCache
	budget          *duckdb.BudgetController
	budgetCf        duckdb.InstanceConfig
	options         []duckdb.Option
	dbInit          func(ctx context.Context, conn *duckdb.Conn) error
	connInit        func(ctx context.Context, conn *duckdb.Conn) error
	scanTime        bool
	hasDecodeLimits bool
	decodeLimits    duckdb.DecodeLimits
	hasResultLimits bool
	resultLimits    duckdb.ResultLimits
}

// ConnectorOption customizes a duckdbsql Connector or registered driver.
type ConnectorOption func(*connectorConfig)

// WithInstanceCache opens the Connector's Database through cache so multiple
// Connectors or Databases in the same process can share a database file.
func WithInstanceCache(cache *duckdb.InstanceCache) ConnectorOption {
	return func(c *connectorConfig) { c.cache = cache }
}

// WithBudget enforces bc and cfg when opening the Connector's Database.
func WithBudget(bc *duckdb.BudgetController, cfg duckdb.InstanceConfig) ConnectorOption {
	return func(c *connectorConfig) {
		c.budget = bc
		c.budgetCf = cfg
	}
}

// WithOptions appends explicit native DuckDB configuration options applied
// after any options parsed from the DSN query string.
func WithOptions(opts ...duckdb.Option) ConnectorOption {
	return func(c *connectorConfig) {
		c.options = append(c.options, opts...)
	}
}

// WithDBInit registers a one-time database initialization hook executed once
// when NewConnector opens a *duckdb.Database (useful for schema DDL, global
// SET statements, or initial bootstrapping). Ignored by NewReadOnlyConnector.
func WithDBInit(fn func(ctx context.Context, conn *duckdb.Conn) error) ConnectorOption {
	return func(c *connectorConfig) { c.dbInit = fn }
}

// WithConnInit registers a callback executed on every newly created underlying
// *duckdb.Conn before it is returned to the database/sql pool.
func WithConnInit(fn func(ctx context.Context, conn *duckdb.Conn) error) ConnectorOption {
	return func(c *connectorConfig) { c.connInit = fn }
}

// WithTimeScan controls whether DATE and TIMESTAMP* columns are decoded into
// Go time.Time (UTC) in Rows.Next. When false (the default), exact duckdb
// temporal structs (duckdb.Date, duckdb.Timestamp) are returned.
func WithTimeScan(enabled bool) ConnectorOption {
	return func(c *connectorConfig) { c.scanTime = enabled }
}

// WithDecodeLimits applies per-value decoding limits (depth, string/blob bytes,
// collection elements) to every connection opened by the Connector.
func WithDecodeLimits(limits duckdb.DecodeLimits) ConnectorOption {
	return func(c *connectorConfig) {
		c.hasDecodeLimits = true
		c.decodeLimits = limits
	}
}

// WithResultLimits applies per-query row and decoded-byte limits to every
// connection opened by the Connector.
func WithResultLimits(limits duckdb.ResultLimits) ConnectorOption {
	return func(c *connectorConfig) {
		c.hasResultLimits = true
		c.resultLimits = limits
	}
}

// ParseDSN parses a DuckDB DSN of the form "[path][?opt1=val1&opt2=val2]".
// An empty path selects ":memory:". Query parameters preserve declaration order.
func ParseDSN(dsn string) (string, []duckdb.Option, error) {
	if strings.IndexByte(dsn, 0) >= 0 {
		return "", nil, fmt.Errorf("%w: DSN contains NUL", duckdb.ErrInvalidInput)
	}
	path := dsn
	var rawQuery string
	if idx := strings.IndexByte(dsn, '?'); idx >= 0 {
		path = dsn[:idx]
		rawQuery = dsn[idx+1:]
	}
	if path == "" {
		path = ":memory:"
	}
	var opts []duckdb.Option
	if rawQuery != "" {
		for _, pair := range strings.Split(rawQuery, "&") {
			if pair == "" {
				continue
			}
			kRaw, vRaw, hasEq := strings.Cut(pair, "=")
			if !hasEq || kRaw == "" {
				return "", nil, fmt.Errorf("%w: invalid DSN query parameter %q", duckdb.ErrInvalidInput, pair)
			}
			k, err := url.QueryUnescape(kRaw)
			if err != nil {
				return "", nil, fmt.Errorf("%w: invalid DSN key %q: %v", duckdb.ErrInvalidInput, kRaw, err)
			}
			v, err := url.QueryUnescape(vRaw)
			if err != nil {
				return "", nil, fmt.Errorf("%w: invalid DSN value for %q: %v", duckdb.ErrInvalidInput, k, err)
			}
			opts = append(opts, duckdb.Option{Name: k, Value: v})
		}
	}
	return path, opts, nil
}

// Connector implements database/sql/driver.Connector and io.Closer, owning one
// *duckdb.Database (or borrowing a caller-owned *duckdb.ReadOnlySession) shared
// across connections opened by sql.OpenDB.
type Connector struct {
	lib     *duckdb.Library
	db      *duckdb.Database
	session *duckdb.ReadOnlySession
	cfg     connectorConfig
	once    sync.Once
	closed  bool
	mu      sync.Mutex
}

var _ driver.Connector = (*Connector)(nil)

// NewReadOnlyConnector wraps an existing sealed *duckdb.ReadOnlySession as a
// driver.Connector suitable for sql.OpenDB. Ownership of session remains with
// the caller: Connector.Close stops opening new connections but does NOT close
// session. Options that apply only when opening a new database (WithInstanceCache,
// WithBudget, WithOptions) are ignored.
func NewReadOnlyConnector(session *duckdb.ReadOnlySession, opts ...ConnectorOption) (*Connector, error) {
	if session == nil {
		return nil, duckdb.ErrClosed
	}
	var cfg connectorConfig
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	return &Connector{
		session: session,
		cfg:     cfg,
	}, nil
}

// NewConnector opens a DuckDB Database according to dsn and opts and returns a
// driver.Connector suitable for sql.OpenDB. Call Close when finished (or close
// the *sql.DB returned by sql.OpenDB, which closes its Connector in Go >= 1.17).
func NewConnector(lib *duckdb.Library, dsn string, opts ...ConnectorOption) (*Connector, error) {
	if lib == nil {
		return nil, duckdb.ErrClosed
	}
	path, dsnOpts, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	var cfg connectorConfig
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	allOpts := make([]duckdb.Option, 0, len(dsnOpts)+len(cfg.options))
	allOpts = append(allOpts, dsnOpts...)
	allOpts = append(allOpts, cfg.options...)

	ctx := context.Background()
	var db *duckdb.Database
	switch {
	case cfg.budget != nil && cfg.cache != nil:
		bcfg := cfg.budgetCf
		bcfg.ExtraOptions = append(append([]duckdb.Option(nil), bcfg.ExtraOptions...), allOpts...)
		db, err = cfg.budget.OpenCached(ctx, cfg.cache, path, bcfg)
	case cfg.budget != nil:
		bcfg := cfg.budgetCf
		bcfg.ExtraOptions = append(append([]duckdb.Option(nil), bcfg.ExtraOptions...), allOpts...)
		db, err = cfg.budget.Open(ctx, lib, path, bcfg)
	case cfg.cache != nil:
		db, err = cfg.cache.Open(ctx, path, allOpts...)
	default:
		db, err = lib.Open(ctx, path, allOpts...)
	}
	if err != nil {
		return nil, err
	}
	if cfg.dbInit != nil {
		initConn, initErr := db.Connect(ctx)
		if initErr != nil {
			_ = db.Close()
			return nil, initErr
		}
		if cfg.hasDecodeLimits {
			if err := initConn.SetDecodeLimits(cfg.decodeLimits); err != nil {
				_ = initConn.Close()
				_ = db.Close()
				return nil, err
			}
		}
		if cfg.hasResultLimits {
			if err := initConn.SetResultLimits(cfg.resultLimits); err != nil {
				_ = initConn.Close()
				_ = db.Close()
				return nil, err
			}
		}
		initErr = cfg.dbInit(ctx, initConn)
		_ = initConn.Close()
		if initErr != nil {
			_ = db.Close()
			return nil, initErr
		}
	}
	return &Connector{
		lib: lib,
		db:  db,
		cfg: cfg,
	}, nil
}

// Connect opens a new *duckdb.Conn from the Connector's Database and runs any
// configured WithConnInit hook.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	if c == nil {
		return nil, duckdb.ErrClosed
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, duckdb.ErrClosed
	}
	var (
		conn *duckdb.Conn
		err  error
	)
	if c.session != nil {
		conn, err = c.session.Connect(ctx)
	} else {
		conn, err = c.db.Connect(ctx)
	}
	if err != nil {
		return nil, err
	}
	if c.cfg.hasDecodeLimits {
		if err := conn.SetDecodeLimits(c.cfg.decodeLimits); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	if c.cfg.hasResultLimits {
		if err := conn.SetResultLimits(c.cfg.resultLimits); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	if c.cfg.connInit != nil {
		if err := c.cfg.connInit(ctx, conn); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	return &sqlConn{
		conn:     conn,
		scanTime: c.cfg.scanTime,
	}, nil
}

// Driver returns the underlying driver.Driver associated with this Connector.
func (c *Connector) Driver() driver.Driver {
	return &sqlDriver{lib: c.lib}
}

// Close closes the underlying *duckdb.Database. Idempotent.
func (c *Connector) Close() error {
	if c == nil {
		return nil
	}
	var err error
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		if c.db != nil {
			err = c.db.Close()
		}
	})
	return err
}

type sqlDriver struct {
	lib  *duckdb.Library
	opts []ConnectorOption
}

var (
	_ driver.Driver        = (*sqlDriver)(nil)
	_ driver.DriverContext = (*sqlDriver)(nil)
)

// Register registers a named database/sql driver backed by a verified
// *duckdb.Library and default ConnectorOptions so callers can use sql.Open.
func Register(name string, lib *duckdb.Library, opts ...ConnectorOption) {
	sql.Register(name, &sqlDriver{
		lib:  lib,
		opts: append([]ConnectorOption(nil), opts...),
	})
}

func (d *sqlDriver) Open(dsn string) (driver.Conn, error) {
	conn, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	c, err := conn.Connect(context.Background())
	if err != nil {
		if closer, ok := conn.(*Connector); ok {
			_ = closer.Close()
		}
		return nil, err
	}
	// Tie standalone database lifetime to this connection when opened via Driver.Open.
	if sc, ok := c.(*sqlConn); ok {
		if closer, ok := conn.(*Connector); ok {
			sc.ownedConnector = closer
		}
	}
	return c, nil
}

func (d *sqlDriver) OpenConnector(dsn string) (driver.Connector, error) {
	return NewConnector(d.lib, dsn, d.opts...)
}

// RawConn invokes fn with the underlying *duckdb.Conn checked out by conn,
// enabling direct access to NewAppender, QueryArrow, ProfilingInfo, and
// QueryProgress from database/sql callers.
func RawConn(ctx context.Context, conn *sql.Conn, fn func(*duckdb.Conn) error) error {
	if conn == nil || fn == nil {
		return fmt.Errorf("%w: nil sql.Conn or callback", duckdb.ErrInvalidInput)
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return conn.Raw(func(driverConn any) error {
		sc, ok := driverConn.(*sqlConn)
		if !ok || sc == nil || sc.conn == nil {
			return fmt.Errorf("%w: connection is not a duckdbsql connection", duckdb.ErrInvalidInput)
		}
		return fn(sc.conn)
	})
}
