package duckdb

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/bububa/duckdbspike/internal/abi"
)

// ReplacementScanResult describes the table function invocation that replaces
// an unrecognized table name in SQL (e.g., `SELECT * FROM 'mem://users'`).
type ReplacementScanResult struct {
	FunctionName string
	Parameters   []Value
}

// ReplacementScanFunc intercepts unrecognized table names during SQL binding.
// Returning (nil, nil) declines the scan so DuckDB falls through to subsequent
// replacement scans or standard table/file resolution. Returning a non-nil
// error surfaces the error to the SQL query.
type ReplacementScanFunc func(tableName string) (*ReplacementScanResult, error)

// AddReplacementScan registers fn as a database-level replacement scan callback.
// The callback remains active until d.Close releases the database.
func (d *Database) AddReplacementScan(fn ReplacementScanFunc) error {
	if d == nil {
		return ErrClosed
	}
	if fn == nil {
		return fmt.Errorf("%w: nil replacement scan function", ErrInvalidInput)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	handler := &replacementScanHandler{
		api:    d.lib.api,
		limits: d.defaultDecode,
		fn:     fn,
	}
	id := abi.RegisterHookHandle(handler)
	d.lib.api.AddReplacementScan(d.handle, abi.ReplacementScanCallback(), id, abi.HookDeleteCallback())
	return nil
}

type replacementScanHandler struct {
	api    *abi.API
	limits DecodeLimits
	fn     ReplacementScanFunc
}

func (h *replacementScanHandler) setError(info uintptr, msg string) {
	cmsg := cErrorString(msg)
	h.api.ReplacementScanSetError(info, &cmsg[0])
	runtime.KeepAlive(cmsg)
}

func (h *replacementScanHandler) Scan(info uintptr, tableName string) {
	defer func() {
		if r := recover(); r != nil {
			h.setError(info, fmt.Sprintf("replacement scan panic: %v", r))
		}
	}()

	res, err := h.fn(tableName)
	if err != nil {
		h.setError(info, err.Error())
		return
	}
	if res == nil {
		return
	}
	if res.FunctionName == "" {
		h.setError(info, "replacement scan returned empty function name")
		return
	}
	cfn, err := abi.CString(res.FunctionName)
	if err != nil {
		h.setError(info, fmt.Sprintf("replacement scan function name %q: %v", res.FunctionName, err))
		return
	}

	paramHandles := make([]uintptr, len(res.Parameters))
	defer destroyNativeValues(h.api, paramHandles)
	for i, param := range res.Parameters {
		if param.IsDefault() {
			h.setError(info, fmt.Sprintf("replacement scan parameter %d cannot use DefaultValue", i))
			return
		}
		nv, err := createNativeValue(h.api, param, 0, h.limits)
		if err != nil {
			h.setError(info, fmt.Sprintf("replacement scan parameter %d: %v", i, err))
			return
		}
		paramHandles[i] = nv
	}

	h.api.ReplacementScanSetFunctionName(info, &cfn[0])
	runtime.KeepAlive(cfn)
	for _, nv := range paramHandles {
		h.api.ReplacementScanAddParameter(info, nv)
	}
}

// cErrorString converts msg into a NUL-terminated byte slice guaranteed never
// to fail, replacing any embedded NUL bytes so C error callbacks never drop a
// diagnostic message.
func cErrorString(msg string) []byte {
	if msg == "" {
		msg = "unknown error"
	}
	if strings.IndexByte(msg, 0) >= 0 {
		msg = strings.ReplaceAll(msg, "\x00", " ")
	}
	b := make([]byte, len(msg)+1)
	copy(b, msg)
	return b
}
