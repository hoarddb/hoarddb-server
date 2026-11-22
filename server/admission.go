package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Query admission (query-pipelining spec §3.1, QP-1, and §3.1.1). A query
// executes only while it holds one of the node's execution slots, and a
// coordinator holds a forwarded query's result only while it holds one of
// its forwarded-result slots. Both pools are HOARDDB_QUERY_CONCURRENCY
// tokens. A query past them waits — it is never refused (R-5) — and the slot
// is held through the write of its answer: it is released by the answer's
// frame once written, or at once on a path that writes no answer.

// queryGate is a node's two slot pools. They are separate on purpose: a
// coordinator waiting on an owner must never hold what the owner needs to
// answer it, or two nodes forwarding to each other deadlock (RC-2,
// TestCrossForwardingDoesNotDeadlock).
type queryGate struct {
	execute chan struct{}
	forward chan struct{}
	// retainedBytes is what queries executing on this node hold in their
	// result budgets right now; forwardedBytes is what forwarded read answers
	// this node holds for its clients. Both are node-wide, O(1), updated by
	// the query's or the forwarding goroutine, and measure §3.2's and §3.1.1's
	// bounds directly rather than inferring them from the process heap.
	retainedBytes  atomic.Int64
	forwardedBytes atomic.Int64
}

// queryGate returns the node's gate, built on first use rather than in
// NewService: many tests build a Server literal, and a pool sized at
// construction would be a nil channel there, on which an acquire blocks
// forever.
func (s *Server) queryGate() *queryGate {
	s.queryGateOnce.Do(func() {
		slots := queryConcurrencyOf(s)
		s.gate = &queryGate{execute: make(chan struct{}, slots), forward: make(chan struct{}, slots)}
	})
	return s.gate
}

// queryConcurrencyOf is the node's slot count per pool: the configured one,
// or the default when the server was built without config.Load.
func queryConcurrencyOf(s *Server) int {
	if s.config != nil && s.config.Query.Concurrency > 0 {
		return s.config.Query.Concurrency
	}
	return config.DefaultQueryConcurrency()
}

// queryPool is one pool, with its gauge and its admission histogram label.
type queryPool struct {
	name     string
	tokens   chan struct{}
	inFlight prometheus.Gauge
}

func (g *queryGate) executePool() queryPool {
	return queryPool{name: "execute", tokens: g.execute, inFlight: queryExecuteInFlight}
}

func (g *queryGate) forwardPool() queryPool {
	return queryPool{name: "forward", tokens: g.forward, inFlight: queryForwardInFlight}
}

// acquire waits for a slot in pool, or for ctx to end. The release it returns
// gives the slot back once however many times it is called: a second give
// would take another query's slot, silently.
func (g *queryGate) acquire(ctx context.Context, pool queryPool) (func(), error) {
	waitStart := time.Now()
	select {
	case pool.tokens <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("waiting for a %s query slot: %w", pool.name, ctx.Err())
	}
	queryAdmissionWait.WithLabelValues(pool.name).Observe(time.Since(waitStart).Seconds())
	pool.inFlight.Inc()
	return sync.OnceFunc(func() {
		<-pool.tokens
		pool.inFlight.Dec()
	}), nil
}

// isQueryOpcode reports whether op is a query: a multi-entry read whose cost
// grows with the bucket. Point reads, writes and dumps are not (R-7).
func isQueryOpcode(op transport.Opcode) bool {
	switch op {
	case transport.OpScan, transport.OpSearch, transport.OpBTreeRange, transport.OpCount, transport.OpDistinct:
		return true
	}
	return false
}

// admitQuery runs one query under an execution slot. The slot rides on the
// answer's frame and is released when ServeFrame has written it; a path that
// ends without a frame to write — a handler error, or no frame — releases it
// at once.
func (s *Server) admitQuery(ctx context.Context, frame *transport.Frame, run func(context.Context) (*transport.Frame, error)) (*transport.Frame, error) {
	gate := s.queryGate()
	release, err := gate.acquire(ctx, gate.executePool())
	if err != nil {
		if errors.Is(err, context.Canceled) {
			queryCancelled.Inc()
		}
		return nil, fmt.Errorf("admitting %s: %w", frame.Opcode, err)
	}
	resp, err := run(ctx)
	if err != nil || resp == nil {
		release()
		return resp, err
	}
	resp.AddWrittenHook(release)
	return resp, nil
}

// queryInFlight is how many execution slots are held on this node.
func (s *Server) queryInFlight() int {
	return len(s.queryGate().execute)
}

// forwardInFlight is how many forwarded-result slots are held on this node.
func (s *Server) forwardInFlight() int {
	return len(s.queryGate().forward)
}

// forwardedResultBytes is the bytes of forwarded read answers this node holds
// for its clients right now: the direct measure of §3.1.1's forwarded-result
// bound, independent of the process heap.
func (s *Server) forwardedResultBytes() int64 {
	return s.queryGate().forwardedBytes.Load()
}

// queryRetainedBytes is the encoded bytes queries executing on this node hold
// in their result budgets right now: the direct measure of §3.2's execution
// bound (S × C), independent of the process heap.
func (s *Server) queryRetainedBytes() int64 {
	return s.queryGate().retainedBytes.Load()
}
