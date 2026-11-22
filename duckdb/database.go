package duckdb

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/bububa/duckdbspike/internal/abi"
)

// Library is a verified native library mapped for the life of the process.
// There is deliberately no Close or unload: native library lifetime matches
// the process while Database, Conn, Stmt, and Rows are explicitly released.
type Library struct {
	_   noCopy
	api *abi.API
}

// Load verifies and opens the approved native library. See abi.Load for the
// path, checksum, platform, symbol, and version checks.
func Load(path, sha256Hex string) (*Library, error) {
	api, err := abi.Load(path, sha256Hex)
	if err != nil {
		return nil, err
	}
	return &Library{api: api}, nil
}

// Option is one native configuration entry, applied in order before open.
type Option struct {
	Name  string
	Value string
}

// Database owns one native database instance.
type Database struct {
	_      noCopy
	lib    *Library
	mu     sync.Mutex
	handle uintptr
	closed bool
	conns  map[*Conn]struct{}
	once   sync.Once
}

// Open opens path (":memory:" or a file path) with options applied in order.
// A failure releases every native object allocated so far.
func (l *Library) Open(ctx context.Context, path string, options ...Option) (*Database, error) {
	if l == nil || l.api == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cpath, err := abi.CString(path)
	if err != nil {
		return nil, fmt.Errorf("%w: path: %v", ErrInvalidInput, err)
	}
	api := l.api
	var config uintptr
	if api.CreateConfig(&config) != abi.StateSuccess {
		if config != 0 {
			api.DestroyConfig(&config)
		}
		return nil, nativeError("create_config", abi.ErrorInvalid, "")
	}
	defer api.DestroyConfig(&config)
	for _, o := range options {
		name, err := abi.CString(o.Name)
		if err != nil {
			return nil, fmt.Errorf("%w: option name: %v", ErrInvalidInput, err)
		}
		value, err := abi.CString(o.Value)
		if err != nil {
			return nil, fmt.Errorf("%w: option %s value: %v", ErrInvalidInput, o.Name, err)
		}
		state := api.SetConfig(config, &name[0], &value[0])
		runtime.KeepAlive(name)
		runtime.KeepAlive(value)
		if state != abi.StateSuccess {
			return nil, nativeError("set_config", abi.ErrorInvalid, fmt.Sprintf("option %q rejected", o.Name))
		}
	}
	var handle, errPtr uintptr
	state := api.OpenExt(&cpath[0], &handle, config, &errPtr)
	runtime.KeepAlive(cpath)
	message := abi.GoString(errPtr)
	if errPtr != 0 {
		api.Free(errPtr)
	}
	if state != abi.StateSuccess {
		if handle != 0 {
			api.Close(&handle)
		}
		return nil, nativeError("open", abi.ErrorInvalid, message)
	}
	return &Database{lib: l, handle: handle, conns: make(map[*Conn]struct{})}, nil
}

// Connect opens a connection owned by the database.
func (d *Database) Connect(ctx context.Context) (*Conn, error) {
	if d == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, ErrClosed
	}
	api := d.lib.api
	var handle uintptr
	if api.Connect(d.handle, &handle) != abi.StateSuccess {
		if handle != 0 {
			api.Disconnect(&handle)
		}
		return nil, nativeError("connect", abi.ErrorInvalid, "")
	}
	c := &Conn{
		db:                d,
		api:               api,
		handle:            handle,
		stmts:             make(map[*Stmt]struct{}),
		limits:            DefaultDecodeLimits(),
		interruptInterval: defaultInterruptInterval,
	}
	d.conns[c] = struct{}{}
	return c, nil
}

// Close stops admitting connections, closes every open connection (and thus
// its Stmts and Rows), then releases the database. Concurrent callers wait
// for the first Close to finish. Close is idempotent.
func (d *Database) Close() error {
	if d == nil {
		return nil
	}
	var err error
	d.once.Do(func() {
		d.mu.Lock()
		d.closed = true
		conns := make([]*Conn, 0, len(d.conns))
		for c := range d.conns {
			conns = append(conns, c)
		}
		d.mu.Unlock()
		// Interrupt all active queries across connections first so shutdown
		// drains concurrent native calls in parallel rather than serially.
		for _, c := range conns {
			c.beginClose()
		}
		for _, c := range conns {
			err = errors.Join(err, c.Close())
		}
		d.mu.Lock()
		if len(d.conns) != 0 {
			// Unreachable by construction; keep the instance rather than free
			// it under live connections.
			err = errors.Join(err, fmt.Errorf("duckdb: %d connections still registered at close; database retained", len(d.conns)))
		} else {
			d.lib.api.Close(&d.handle)
		}
		d.mu.Unlock()
	})
	return err
}

func (d *Database) forget(c *Conn) {
	d.mu.Lock()
	delete(d.conns, c)
	d.mu.Unlock()
}
