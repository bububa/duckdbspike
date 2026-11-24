package duckdb

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// ResultLimits bounds per-query Go-side output consumption. Zero means no
// additional per-query cap beyond DecodeLimits.
type ResultLimits struct {
	MaxRows  uint64
	MaxBytes uint64
}

// InstanceConfig declares explicit native and Go-side resource caps for one
// Database instance. NativeMemoryBytes is DuckDB's memory_limit (not a process
// RSS cap); callers must budget native overhead above NativeMemoryBytes.
type InstanceConfig struct {
	NativeMemoryBytes uint64
	Threads           int
	SpillDir          string
	SpillBytes        uint64
	DecodeLimits      DecodeLimits
	ResultLimits      ResultLimits
	ExtraOptions      []Option
}

func (c InstanceConfig) validate() (InstanceConfig, []Option, error) {
	if c.NativeMemoryBytes == 0 {
		return InstanceConfig{}, nil, fmt.Errorf("%w: NativeMemoryBytes must be positive", ErrInvalidInput)
	}
	if c.Threads <= 0 {
		return InstanceConfig{}, nil, fmt.Errorf("%w: Threads must be positive", ErrInvalidInput)
	}
	if c.SpillDir != "" {
		if strings.IndexByte(c.SpillDir, 0) >= 0 || !filepath.IsAbs(c.SpillDir) {
			return InstanceConfig{}, nil, fmt.Errorf("%w: SpillDir must be an absolute path without NUL", ErrInvalidInput)
		}
	}
	dl, err := c.DecodeLimits.normalized()
	if err != nil {
		return InstanceConfig{}, nil, err
	}
	c.DecodeLimits = dl

	opts := make([]Option, 0, 4+len(c.ExtraOptions))
	opts = append(opts,
		Option{Name: "memory_limit", Value: formatBytesOption(c.NativeMemoryBytes)},
		Option{Name: "threads", Value: strconv.Itoa(c.Threads)},
	)
	if c.SpillDir != "" {
		opts = append(opts, Option{Name: "temp_directory", Value: c.SpillDir})
	}
	if c.SpillBytes > 0 {
		opts = append(opts, Option{Name: "max_temp_directory_size", Value: formatBytesOption(c.SpillBytes)})
	}
	opts = append(opts, c.ExtraOptions...)
	return c, opts, nil
}

func formatBytesOption(b uint64) string {
	return strconv.FormatUint(b, 10) + "B"
}

// GlobalBudget defines shared process-wide ceilings across all Database
// instances opened through one BudgetController.
type GlobalBudget struct {
	MaxConcurrent    int
	MaxWaitQueue     int
	TotalMemoryBytes uint64
	TotalThreads     int
	TotalSpillBytes  uint64
}

// BudgetSnapshot reports live accounting of a BudgetController.
type BudgetSnapshot struct {
	ActiveInstances int
	Waiting         int
	UsedMemoryBytes uint64
	UsedThreads     int
	UsedSpillBytes  uint64
}

type budgetWaiter struct {
	req     budgetRequest
	ready   chan struct{}
	granted bool
}

type budgetRequest struct {
	memoryBytes uint64
	threads     int
	spillBytes  uint64
}

// BudgetController enforces global concurrency, wait-queue depth, native memory
// reservations, thread count, and spill disk budgets across multiple Database
// instances so concurrent sessions cannot multiply budgets independently.
type BudgetController struct {
	_      noCopy
	limits GlobalBudget

	mu      sync.Mutex
	closed  bool
	active  int
	usedMem uint64
	usedThr int
	usedSpl uint64
	waiters []*budgetWaiter
}

// NewBudgetController validates limits and returns a controller.
func NewBudgetController(limits GlobalBudget) (*BudgetController, error) {
	if limits.MaxConcurrent <= 0 {
		return nil, fmt.Errorf("%w: MaxConcurrent must be positive", ErrInvalidInput)
	}
	if limits.MaxWaitQueue < 0 {
		return nil, fmt.Errorf("%w: MaxWaitQueue cannot be negative", ErrInvalidInput)
	}
	if limits.TotalMemoryBytes == 0 {
		return nil, fmt.Errorf("%w: TotalMemoryBytes must be positive", ErrInvalidInput)
	}
	if limits.TotalThreads <= 0 {
		return nil, fmt.Errorf("%w: TotalThreads must be positive", ErrInvalidInput)
	}
	return &BudgetController{limits: limits}, nil
}

// Snapshot returns current global budget usage.
func (bc *BudgetController) Snapshot() BudgetSnapshot {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	return BudgetSnapshot{
		ActiveInstances: bc.active,
		Waiting:         len(bc.waiters),
		UsedMemoryBytes: bc.usedMem,
		UsedThreads:     bc.usedThr,
		UsedSpillBytes:  bc.usedSpl,
	}
}

// Close rejects new admissions and wakes all queued waiters with ErrClosed.
// Already-open Databases keep running and still release their reservations on
// Close.
func (bc *BudgetController) Close() {
	bc.mu.Lock()
	if bc.closed {
		bc.mu.Unlock()
		return
	}
	bc.closed = true
	for _, w := range bc.waiters {
		close(w.ready)
	}
	bc.waiters = nil
	bc.mu.Unlock()
}

// Open reserves cfg's global budget slice, opens a Database with cfg's native
// resource options, installs cfg's decode/result limits as connection defaults,
// and binds release of the global reservation to Database.Close (after native
// duckdb_close completes). If Open fails, the reservation is released
// immediately.
func (bc *BudgetController) Open(ctx context.Context, lib *Library, path string, cfg InstanceConfig) (*Database, error) {
	return bc.openWith(ctx, cfg, func(opts []Option) (*Database, error) {
		return lib.Open(ctx, path, opts...)
	})
}

// OpenCached behaves like Open but obtains the Database from cache instead of
// calling Library.Open directly.
func (bc *BudgetController) OpenCached(ctx context.Context, cache *InstanceCache, path string, cfg InstanceConfig) (*Database, error) {
	return bc.openWith(ctx, cfg, func(opts []Option) (*Database, error) {
		return cache.Open(ctx, path, opts...)
	})
}

func (bc *BudgetController) openWith(ctx context.Context, cfg InstanceConfig, opener func([]Option) (*Database, error)) (*Database, error) {
	if bc == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	norm, opts, err := cfg.validate()
	if err != nil {
		return nil, err
	}
	req := budgetRequest{
		memoryBytes: norm.NativeMemoryBytes,
		threads:     norm.Threads,
		spillBytes:  norm.SpillBytes,
	}
	if err := bc.acquire(ctx, req); err != nil {
		return nil, err
	}
	db, err := opener(opts)
	if err != nil {
		bc.release(req)
		return nil, err
	}
	db.defaultDecode = norm.DecodeLimits
	db.defaultResult = norm.ResultLimits
	db.onClose = func() { bc.release(req) }
	return db, nil
}

func (bc *BudgetController) exceedsTotalLocked(req budgetRequest) error {
	if req.memoryBytes > bc.limits.TotalMemoryBytes {
		return fmt.Errorf("%w: requested memory %d exceeds global %d", ErrBudgetExceeded, req.memoryBytes, bc.limits.TotalMemoryBytes)
	}
	if req.threads > bc.limits.TotalThreads {
		return fmt.Errorf("%w: requested threads %d exceeds global %d", ErrBudgetExceeded, req.threads, bc.limits.TotalThreads)
	}
	if bc.limits.TotalSpillBytes > 0 && req.spillBytes > bc.limits.TotalSpillBytes {
		return fmt.Errorf("%w: requested spill %d exceeds global %d", ErrBudgetExceeded, req.spillBytes, bc.limits.TotalSpillBytes)
	}
	return nil
}

func (bc *BudgetController) fitsLocked(req budgetRequest) bool {
	if bc.active >= bc.limits.MaxConcurrent {
		return false
	}
	if bc.usedMem+req.memoryBytes > bc.limits.TotalMemoryBytes {
		return false
	}
	if bc.usedThr+req.threads > bc.limits.TotalThreads {
		return false
	}
	if bc.limits.TotalSpillBytes > 0 && bc.usedSpl+req.spillBytes > bc.limits.TotalSpillBytes {
		return false
	}
	return true
}

func (bc *BudgetController) reserveLocked(req budgetRequest) {
	bc.active++
	bc.usedMem += req.memoryBytes
	bc.usedThr += req.threads
	bc.usedSpl += req.spillBytes
}

func (bc *BudgetController) acquire(ctx context.Context, req budgetRequest) error {
	bc.mu.Lock()
	if bc.closed {
		bc.mu.Unlock()
		return ErrClosed
	}
	if err := bc.exceedsTotalLocked(req); err != nil {
		bc.mu.Unlock()
		return err
	}
	if len(bc.waiters) == 0 && bc.fitsLocked(req) {
		bc.reserveLocked(req)
		bc.mu.Unlock()
		return nil
	}
	if len(bc.waiters) >= bc.limits.MaxWaitQueue {
		bc.mu.Unlock()
		return fmt.Errorf("%w: %d waiters already queued", ErrQueueFull, len(bc.waiters))
	}
	w := &budgetWaiter{req: req, ready: make(chan struct{})}
	bc.waiters = append(bc.waiters, w)
	bc.mu.Unlock()

	select {
	case <-w.ready:
		return bc.finalizeWaiter(w)
	case <-ctx.Done():
		return bc.cancelWaiter(w, ctx.Err())
	}
}

func (bc *BudgetController) finalizeWaiter(w *budgetWaiter) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	if w.granted {
		return nil
	}
	return ErrClosed
}

func (bc *BudgetController) cancelWaiter(w *budgetWaiter, ctxErr error) error {
	bc.mu.Lock()
	if w.granted {
		// Grant raced with ctx.Done; hand the reservation back cleanly and wake others.
		bc.mu.Unlock()
		bc.release(w.req)
		return ctxErr
	}
	for i, cand := range bc.waiters {
		if cand == w {
			bc.waiters = append(bc.waiters[:i], bc.waiters[i+1:]...)
			break
		}
	}
	bc.wakeLocked()
	bc.mu.Unlock()
	return ctxErr
}

func (bc *BudgetController) release(req budgetRequest) {
	bc.mu.Lock()
	bc.active--
	bc.usedMem -= req.memoryBytes
	bc.usedThr -= req.threads
	bc.usedSpl -= req.spillBytes
	bc.wakeLocked()
	bc.mu.Unlock()
}

// wakeLocked grants waiting requests in FIFO order while resources fit.
func (bc *BudgetController) wakeLocked() {
	if bc.closed {
		return
	}
	for len(bc.waiters) > 0 {
		next := bc.waiters[0]
		if !bc.fitsLocked(next.req) {
			return
		}
		bc.waiters = bc.waiters[1:]
		bc.reserveLocked(next.req)
		next.granted = true
		close(next.ready)
	}
}

// valueBytes estimates the Go-side heap/data footprint of a decoded Value for
// ResultLimits.MaxBytes enforcement.
func valueBytes(v Value) uint64 {
	if v.null {
		return 8
	}
	switch v.Kind() {
	case TypeBoolean, TypeTinyint, TypeUTinyint:
		return 1
	case TypeSmallint, TypeUSmallint:
		return 2
	case TypeInteger, TypeUInteger, TypeFloat, TypeDate:
		return 4
	case TypeBigint, TypeUBigint, TypeDouble, TypeTime, TypeTimeNS,
		TypeTimestamp, TypeTimestampS, TypeTimestampMS, TypeTimestampNS, TypeTimestampTZ:
		return 8
	case TypeTimeTZ, TypeHugeint, TypeUhugeint, TypeInterval, TypeUUID:
		return 16
	case TypeDecimal:
		return 24
	case TypeVarchar:
		s, _ := v.ref.(string)
		return 16 + uint64(len(s))
	case TypeEnum:
		s, _ := v.ref.(string)
		return 16 + uint64(len(s))
	case TypeBlob, TypeBit, TypeBignum, TypeGeometry:
		b, _ := v.ref.([]byte)
		return 24 + uint64(len(b))
	case TypeUnion:
		inner, _ := v.ref.(Value)
		return 24 + valueBytes(inner)
	case TypeList:
		elems, _ := v.ref.([]Value)
		total := uint64(24)
		for _, elem := range elems {
			total += valueBytes(elem)
		}
		return total
	case TypeArray:
		elems, _ := v.ref.([]Value)
		total := uint64(24)
		for _, elem := range elems {
			total += valueBytes(elem)
		}
		return total
	case TypeStruct:
		fields, _ := v.ref.([]StructFieldValue)
		total := uint64(24)
		for _, f := range fields {
			total += 16 + uint64(len(f.Name)) + valueBytes(f.Value)
		}
		return total
	case TypeMap:
		entries, _ := v.ref.([]MapEntry)
		total := uint64(24)
		for _, e := range entries {
			total += valueBytes(e.Key) + valueBytes(e.Value)
		}
		return total
	default:
		return 16
	}
}
