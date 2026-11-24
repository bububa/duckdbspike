package duckdb

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/bububa/duckdbspike/internal/abi"
)

// InstanceCache shares underlying DuckDB database instances across multiple
// Open calls targeting the same database file path within the same process.
// Callers must Close every Database and the InstanceCache itself; Close closes
// any remaining Databases opened from the cache before destroying the native
// cache handle.
type InstanceCache struct {
	_      noCopy
	lib    *Library
	mu     sync.Mutex
	handle uintptr
	closed bool
	dbs    map[*Database]struct{}
	once   sync.Once
}

// NewInstanceCache creates a native DuckDB database instance cache.
func (l *Library) NewInstanceCache() (*InstanceCache, error) {
	if l == nil || l.api == nil {
		return nil, ErrClosed
	}
	handle := l.api.CreateInstanceCache()
	if handle == 0 {
		return nil, nativeError("create_instance_cache", abi.ErrorInvalid, "")
	}
	return &InstanceCache{
		lib:    l,
		handle: handle,
		dbs:    make(map[*Database]struct{}),
	}, nil
}

// Open opens or retrieves a cached Database for path (":memory:" or file path)
// with options applied in order. The returned Database must still be closed by
// the caller.
func (ic *InstanceCache) Open(ctx context.Context, path string, options ...Option) (*Database, error) {
	if ic == nil {
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

	ic.mu.Lock()
	defer ic.mu.Unlock()
	if ic.closed {
		return nil, ErrClosed
	}

	api := ic.lib.api
	config, err := buildNativeConfig(api, options)
	if err != nil {
		return nil, err
	}
	defer api.DestroyConfig(&config)

	var handle, errPtr uintptr
	state := api.GetOrCreateFromCache(ic.handle, &cpath[0], &handle, config, &errPtr)
	runtime.KeepAlive(cpath)
	message := abi.GoString(errPtr)
	if errPtr != 0 {
		api.Free(errPtr)
	}
	if state != abi.StateSuccess {
		if handle != 0 {
			api.Close(&handle)
		}
		return nil, nativeError("get_or_create_from_cache", abi.ErrorInvalid, message)
	}
	db := &Database{
		lib:           ic.lib,
		cache:         ic,
		handle:        handle,
		conns:         make(map[*Conn]struct{}),
		defaultDecode: DefaultDecodeLimits(),
	}
	ic.dbs[db] = struct{}{}
	return db, nil
}

// Close stops admitting new opens, closes every remaining Database opened from
// this cache, and destroys the native instance cache. Close is idempotent.
func (ic *InstanceCache) Close() error {
	if ic == nil {
		return nil
	}
	var err error
	ic.once.Do(func() {
		ic.mu.Lock()
		ic.closed = true
		dbs := make([]*Database, 0, len(ic.dbs))
		for d := range ic.dbs {
			dbs = append(dbs, d)
		}
		ic.mu.Unlock()
		for _, d := range dbs {
			err = errors.Join(err, d.Close())
		}
		ic.mu.Lock()
		if len(ic.dbs) != 0 {
			err = errors.Join(err, fmt.Errorf("duckdb: %d databases still registered at instance cache close; cache retained", len(ic.dbs)))
		} else {
			ic.lib.api.DestroyInstanceCache(&ic.handle)
		}
		ic.mu.Unlock()
	})
	return err
}

func (ic *InstanceCache) forget(d *Database) {
	ic.mu.Lock()
	delete(ic.dbs, d)
	ic.mu.Unlock()
}
