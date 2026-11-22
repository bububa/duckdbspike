package duckdb

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bububa/duckdbspike/internal/abi"
)

const defaultInterruptInterval = time.Millisecond

type scopeState int

const (
	scopeStateUnknown scopeState = iota
	scopeStateActive
	scopeStateCompleted
	scopeStateCanceled
)

// cancelScope coordinates cooperative native cancellation for one query, exec,
// or prepare lifecycle from start through final fetch/decode.
//
// Why re-assertion is required: DuckDB's ClientContext::InitialCleanup clears
// the connection's interrupted flag when entering prepare/pending/transaction
// setup, so a single duckdb_interrupt delivered before or during setup is lost.
// cancelScope therefore latches cancellation and periodically re-asserts
// duckdb_interrupt (which only performs an atomic store without taking DuckDB's
// context lock) until the native scope exits and join() completes.
//
// Invariant: join() MUST be called before any native object owned by the scope
// is reused or disconnected, guaranteeing that a stale re-asserter can never
// interrupt a subsequent query or touch a disconnected connection.
type cancelScope struct {
	mu      sync.Mutex
	api     *abi.API
	conn    uintptr
	period  time.Duration
	stopCtx func() bool
	doneCh  chan struct{}
	wg      sync.WaitGroup
	state   scopeState
	exited  bool
	reason  error
	asserts atomic.Int64
}

func newCancelScope(ctx context.Context, api *abi.API, conn uintptr, period time.Duration) *cancelScope {
	if period <= 0 {
		period = defaultInterruptInterval
	}
	s := &cancelScope{
		api:    api,
		conn:   conn,
		period: period,
		doneCh: make(chan struct{}),
		state:  scopeStateActive,
	}
	s.stopCtx = context.AfterFunc(ctx, func() {
		s.cancel(ctx.Err())
	})
	return s
}

// cancel latches reason and, if the native scope has not yet exited, starts
// the re-asserting interrupt loop. Safe to call concurrently from any goroutine
// without holding Conn.mu.
func (s *cancelScope) cancel(reason error) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.state == scopeStateCompleted || s.state == scopeStateCanceled {
		s.mu.Unlock()
		return
	}
	s.state = scopeStateCanceled
	if reason == nil {
		reason = context.Canceled
	}
	s.reason = reason
	if s.exited {
		s.mu.Unlock()
		return
	}
	s.wg.Add(1)
	s.mu.Unlock()
	go s.reassertLoop()
}

func (s *cancelScope) reassertLoop() {
	defer s.wg.Done()
	s.api.Interrupt(s.conn)
	s.asserts.Add(1)
	ticker := time.NewTicker(s.period)
	defer ticker.Stop()
	for {
		select {
		case <-s.doneCh:
			return
		case <-ticker.C:
			s.api.Interrupt(s.conn)
			s.asserts.Add(1)
		}
	}
}

// tryFinish linearizes clean completion against concurrent cancellation. It
// returns true if completion won; if cancellation already latched, it returns
// false and err() reports the cancellation reason.
func (s *cancelScope) tryFinish() bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == scopeStateCanceled {
		return false
	}
	s.state = scopeStateCompleted
	return true
}

// err returns the latched cancellation reason, or nil if not canceled.
func (s *cancelScope) err() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == scopeStateCanceled {
		return s.reason
	}
	return nil
}

// join stops the context watcher and waits for any re-asserting interrupt
// goroutine to exit. It has no timeout or deadline: it does not return until
// no further duckdb_interrupt calls can be issued by this scope.
func (s *cancelScope) join() {
	if s == nil {
		return
	}
	if s.stopCtx != nil {
		s.stopCtx()
	}
	s.mu.Lock()
	if !s.exited {
		s.exited = true
		close(s.doneCh)
	}
	s.mu.Unlock()
	s.wg.Wait()
}
