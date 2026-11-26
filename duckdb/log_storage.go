package duckdb

import (
	"fmt"
	"runtime"

	"github.com/bububa/duckdbspike/internal/abi"
)

// LogEntry holds one log record emitted by DuckDB's internal logging subsystem
// to a custom LogStorage registered via Database.RegisterLogStorage.
type LogEntry struct {
	Timestamp Timestamp
	Level     string
	Type      string
	Message   string
}

// LogStorageFunc receives log entries emitted by DuckDB when this log storage
// is activated (for example via `CALL enable_logging(storage = '<name>')`).
// Implementations must be safe for concurrent calls from DuckDB worker threads.
type LogStorageFunc func(entry LogEntry)

type logStorageHandler struct {
	fn LogStorageFunc
}

func (h *logStorageHandler) Write(timestampMicros int64, level, logType, message string) {
	defer func() {
		_ = recover()
	}()
	h.fn(LogEntry{
		Timestamp: Timestamp{Value: timestampMicros, Unit: TimestampUnitMicros},
		Level:     level,
		Type:      logType,
		Message:   message,
	})
}

// RegisterLogStorage registers a named custom Go log storage sink on d.
// Once registered, activate it in SQL via `CALL enable_logging(storage = '<name>')`.
func (d *Database) RegisterLogStorage(name string, fn LogStorageFunc) error {
	if d == nil {
		return ErrClosed
	}
	if name == "" {
		return fmt.Errorf("%w: log storage name must not be empty", ErrInvalidInput)
	}
	if fn == nil {
		return fmt.Errorf("%w: nil log storage callback", ErrInvalidInput)
	}
	cname, err := abi.CString(name)
	if err != nil {
		return fmt.Errorf("%w: log storage name %q: %v", ErrInvalidInput, name, err)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}

	api := d.lib.api
	ls := api.CreateLogStorage()
	if ls == 0 {
		return nativeError("create_log_storage", abi.ErrorOutOfMemory, "")
	}

	id := abi.RegisterHookHandle(&logStorageHandler{fn: fn})
	api.LogStorageSetName(ls, &cname[0])
	runtime.KeepAlive(cname)
	api.LogStorageSetExtraData(ls, id, abi.HookDeleteCallback())
	api.LogStorageSetWriteLogEntry(ls, abi.LogStorageWriteCallback())

	state := api.RegisterLogStorage(d.handle, ls)
	api.DestroyLogStorage(&ls)
	if state != abi.StateSuccess {
		// ReleaseHookHandle is idempotent (DuckDB v1.5.6 may invoke
		// delete_callback twice on registration failure).
		abi.ReleaseHookHandle(id)
		return nativeError("register_log_storage", abi.ErrorInvalid, fmt.Sprintf("failed to register log storage %q", name))
	}
	return nil
}
