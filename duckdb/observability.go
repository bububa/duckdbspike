package duckdb

import (
	"fmt"
	"math"

	"github.com/bububa/duckdbspike/internal/abi"
)

const maxProfilingDepth = 256

// QueryProgress reports live execution progress for the query currently
// running on a Conn. Percentage is negative (-1) when no query is active or
// DuckDB has not yet determined progress.
type QueryProgress struct {
	Percentage         float64
	RowsProcessed      uint64
	TotalRowsToProcess uint64
}

// ProfilingInfo is an owned snapshot of one node in DuckDB's query profiling
// tree. All native handles and strings are copied and released before
// Conn.ProfilingInfo returns.
type ProfilingInfo struct {
	Metrics  map[string]string
	Children []*ProfilingInfo
}

// QueryProgress returns live progress for the query currently running on c.
// It is safe to call concurrently from another goroutine while Query, Exec, or
// Rows.Next is executing on c, and is joined before Disconnect on Close.
func (c *Conn) QueryProgress() (QueryProgress, error) {
	if c == nil {
		return QueryProgress{}, ErrClosed
	}
	c.cancelMu.Lock()
	if c.closing {
		c.cancelMu.Unlock()
		return QueryProgress{}, ErrClosed
	}
	c.obsWG.Add(1)
	handle := c.handle
	c.cancelMu.Unlock()
	defer c.obsWG.Done()

	raw := c.api.QueryProgress(handle)
	return QueryProgress{
		Percentage:         raw.Percentage,
		RowsProcessed:      raw.RowsProcessed,
		TotalRowsToProcess: raw.TotalRowsToProcess,
	}, nil
}

// ProfilingInfo returns an owned snapshot of the root profiling tree for the
// last profiled query on c, or (nil, nil) when profiling is disabled or no
// profiling data is available. Enable profiling on c first (for example via
// SET enable_profiling = 'no_output'). Returns ErrBusy while a Rows cursor is
// open on c.
func (c *Conn) ProfilingInfo() (*ProfilingInfo, error) {
	if c == nil {
		return nil, ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return nil, err
	}
	root := c.api.GetProfilingInfo(c.handle)
	if root == 0 {
		return nil, nil
	}
	return snapshotProfilingNode(c.api, root, 0, c.limits)
}

// ProfilingMetric retrieves a single metric value by key from the root
// profiling node for the last profiled query on c without snapshotting child
// operator nodes. Returns ("", false, nil) when profiling is disabled or the
// metric key does not exist. Returns ErrBusy while a cursor or appender is open
// on c.
//
// Note: DuckDB v1.5.6's duckdb_profiling_info_get_value throws an uncaught C++
// exception (aborting the process) on unrecognized metric names, so
// ProfilingMetric safely inspects the root node's metrics map via
// duckdb_profiling_info_get_metrics instead.
func (c *Conn) ProfilingMetric(key string) (string, bool, error) {
	if c == nil {
		return "", false, ErrClosed
	}
	if key == "" {
		return "", false, fmt.Errorf("%w: profiling metric key must not be empty", ErrInvalidInput)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return "", false, err
	}
	api := c.api
	root := api.GetProfilingInfo(c.handle)
	if root == 0 {
		return "", false, nil
	}
	metricsVal := api.ProfilingInfoGetMetrics(root)
	if metricsVal == 0 {
		return "", false, nil
	}
	defer api.DestroyValue(&metricsVal)
	n := api.GetMapSize(metricsVal)
	if n > c.limits.MaxCollectionLen || n > math.MaxInt {
		return "", false, fmt.Errorf("%w: profiling metrics count %d exceeds %d", ErrLimitExceeded, n, c.limits.MaxCollectionLen)
	}
	for i := uint64(0); i < n; i++ {
		kVal := api.GetMapKey(metricsVal, i)
		var kStr string
		if kVal != 0 {
			if p := api.GetVarchar(kVal); p != 0 {
				kStr = abi.GoString(p)
				api.Free(p)
			}
			api.DestroyValue(&kVal)
		}
		if kStr != key {
			continue
		}
		vVal := api.GetMapValue(metricsVal, i)
		var vStr string
		if vVal != 0 {
			if p := api.GetVarchar(vVal); p != 0 {
				vStr = abi.GoString(p)
				api.Free(p)
			}
			api.DestroyValue(&vVal)
		}
		return vStr, true, nil
	}
	return "", false, nil
}

func snapshotProfilingNode(api *abi.API, node uintptr, depth int, limits DecodeLimits) (*ProfilingInfo, error) {
	if depth > maxProfilingDepth {
		return nil, fmt.Errorf("%w: profiling tree depth %d exceeds %d", ErrLimitExceeded, depth, maxProfilingDepth)
	}
	info := &ProfilingInfo{
		Metrics: make(map[string]string),
	}
	metricsVal := api.ProfilingInfoGetMetrics(node)
	if metricsVal != 0 {
		defer api.DestroyValue(&metricsVal)
		n := api.GetMapSize(metricsVal)
		if n > limits.MaxCollectionLen || n > math.MaxInt {
			return nil, fmt.Errorf("%w: profiling metrics count %d exceeds %d", ErrLimitExceeded, n, limits.MaxCollectionLen)
		}
		for i := uint64(0); i < n; i++ {
			kVal := api.GetMapKey(metricsVal, i)
			vVal := api.GetMapValue(metricsVal, i)
			var kStr, vStr string
			if kVal != 0 {
				if p := api.GetVarchar(kVal); p != 0 {
					kStr = abi.GoString(p)
					api.Free(p)
				}
				api.DestroyValue(&kVal)
			}
			if vVal != 0 {
				if p := api.GetVarchar(vVal); p != 0 {
					vStr = abi.GoString(p)
					api.Free(p)
				}
				api.DestroyValue(&vVal)
			}
			info.Metrics[kStr] = vStr
		}
	}

	childCount := api.ProfilingInfoGetChildCount(node)
	if childCount > limits.MaxCollectionLen || childCount > math.MaxInt {
		return nil, fmt.Errorf("%w: profiling child count %d exceeds %d", ErrLimitExceeded, childCount, limits.MaxCollectionLen)
	}
	if childCount > 0 {
		info.Children = make([]*ProfilingInfo, 0, int(childCount))
		for i := uint64(0); i < childCount; i++ {
			child := api.ProfilingInfoGetChild(node, i)
			if child == 0 {
				continue
			}
			snap, err := snapshotProfilingNode(api, child, depth+1, limits)
			if err != nil {
				return nil, err
			}
			info.Children = append(info.Children, snap)
		}
	}
	return info, nil
}
