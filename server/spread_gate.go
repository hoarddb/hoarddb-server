package server

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
)

// The router gate (document placement spread MVP note, section 2.3): one
// sync.RWMutex per spread template at the template's router, in a map shaped
// like Server.fences, which holds an entry only while a routed operation of
// the template holds or awaits it. A routed operation (a keyed write or read,
// a bulk write, a BATCH item) takes it shared, once; a drop of the collection
// takes it exclusively (step 4), so a drop orders against every routed
// operation of its collection at this router.
//
// Lock order: gate(T) before any fence. The argument, which
// TestRouterGateNeverDeadlocks runs:
//
//  1. The gate is taken at exactly three sites: the top of
//     Cluster.applyAsOwner (reached from Cluster.write and
//     Cluster.serveForwardedEntry, neither of which holds a fence; the fence
//     is taken later, in applyOwned's fenceAsSettledOwner), the read router
//     (serveRead's owner branch, serveForwardedEntry's forwarded read, the
//     one-member read door: readers take no fence, spec §5.2.2), and the
//     one-member write door in Server.dispatchInFence, before its fence is
//     taken. So no goroutine that holds a fence ever waits on a gate.
//  2. It is never re-entered. The router's own writes to the template (the
//     born-set marks, and the template's drop in step 4) carry
//     withRouterGateHeld in their context, and the router skips a template
//     write that carries it, so they reach the template's fence without
//     taking the gate again. A re-entered shared hold would deadlock behind a
//     queued exclusive one (the reviewer's rw/main.go).
//  3. Under the gate, fences are taken one at a time and released before the
//     next: the template's for a mark, a part's for its create, write or drop,
//     here or on the part's owner. A part belongs to one template, and a part
//     is never a template, so no routed operation of a part takes a gate.
//
// A goroutine therefore holds at most one gate and, under it, at most one
// fence, always in the order gate then fence: there is no cycle. Waiting on a
// remote part's owner under a shared gate waits on that owner's fence, never
// on this gate.
//
// Cost (rule 11): nothing per template while idle; one map entry and one
// RWMutex per template with an operation in flight.
type routerGateMap struct {
	mu    sync.Mutex
	gates map[placementKey]*routerGate
}

// routerGate is one template's gate and the count of its holders and waiters.
type routerGate struct {
	mu   sync.RWMutex
	refs int
}

// holdRouterGate takes the gate of the template placed by key, shared or
// exclusive, and returns its release. The caller holds no fence (see the lock
// order above) and does not already hold this gate.
func (s *Server) holdRouterGate(key placementKey, exclusive bool) (release func()) {
	if hook := routerGateTakenHook.Load(); hook != nil {
		(*hook)(key, exclusive)
	}
	gate := s.routerGates.reference(key)
	if exclusive {
		gate.mu.Lock()
	} else {
		gate.mu.RLock()
	}
	return func() {
		if exclusive {
			gate.mu.Unlock()
		} else {
			gate.mu.RUnlock()
		}
		s.routerGates.dereference(key, gate)
	}
}

// routerGateTakenHook, when set, is called each time a router gate is taken,
// before it is waited for, with the template and whether the hold is
// exclusive. Nil in production.
var routerGateTakenHook atomic.Pointer[func(key placementKey, exclusive bool)]

// reference returns the gate of key, created if nobody holds or waits for it,
// with one more reference counted.
func (m *routerGateMap) reference(key placementKey) *routerGate {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.gates == nil {
		m.gates = make(map[placementKey]*routerGate)
	}
	gate, ok := m.gates[key]
	if !ok {
		gate = &routerGate{}
		m.gates[key] = gate
	}
	gate.refs++
	return gate
}

// dereference drops one reference, deleting the entry when nobody else holds
// or waits for it.
func (m *routerGateMap) dereference(key placementKey, gate *routerGate) {
	m.mu.Lock()
	defer m.mu.Unlock()
	gate.refs--
	if gate.refs == 0 {
		delete(m.gates, key)
	}
}

// inFlight is how many templates have a gate entry: zero once every routed
// operation has ended. A test reads it to see that nothing is left behind.
func (m *routerGateMap) inFlight() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.gates)
}

// routerGateHeldCtxKey marks a context whose caller holds the gate of the
// template it names: the router's own writes to the template carry it, so the
// router lets them through to the template instead of routing them again.
type routerGateHeldCtxKey struct{}

// withRouterGateHeld says the caller holds the gate of the template placed by
// key.
func withRouterGateHeld(ctx context.Context, key placementKey) context.Context {
	return context.WithValue(ctx, routerGateHeldCtxKey{}, key)
}

// routerGateHeld reports whether ctx says its caller holds key's gate.
func routerGateHeld(ctx context.Context, key placementKey) bool {
	held, ok := ctx.Value(routerGateHeldCtxKey{}).(placementKey)
	return ok && held == key
}

// batchGatesCtxKey carries the templates whose gate a BATCH holds shared for
// the items it runs at this router.
type batchGatesCtxKey struct{}

// batchGateHeld reports whether ctx belongs to a BATCH that holds key's gate:
// the routed item takes no gate of its own, since a second shared hold behind
// a queued exclusive one would wait for the first (section 2.3, never
// re-entered).
func batchGateHeld(ctx context.Context, key placementKey) bool {
	held, ok := ctx.Value(batchGatesCtxKey{}).([]placementKey)
	if !ok {
		return false
	}
	for _, heldKey := range held {
		if heldKey == key {
			return true
		}
	}
	return false
}

// batchSpreadTemplates are the templates this node holds among the items i
// for which routed says this node runs them, each once, in the order every
// BATCH takes their gates.
func (s *Server) batchSpreadTemplates(items []batchItem, routed func(i int) bool) []placementKey {
	seen := make(map[placementKey]bool)
	var keys []placementKey
	for i, item := range items {
		key := placementOf(item.database, item.bucket)
		if seen[key] || !routed(i) || !s.spreadTemplateHeld(item.database, item.bucket) {
			continue
		}
		seen[key] = true
		keys = append(keys, key)
	}
	sort.Slice(keys, func(a, b int) bool { return keys[a].encoded < keys[b].encoded })
	return keys
}

// holdBatchGates takes the gate of each template shared, once, in key order,
// and returns the context its items run in and the release of them all. Key
// order is what keeps two BATCHes that name the same two templates from
// closing a cycle through a queued exclusive taker (a drop) on each: neither
// holds the later gate while waiting for the earlier. The caller holds no
// fence (the BATCH has run no item), and waits for no answer of another node
// while it holds the gates (batchPlan.holdLocal).
func (s *Server) holdBatchGates(ctx context.Context, keys []placementKey) (context.Context, func()) {
	if len(keys) == 0 {
		return ctx, func() {}
	}
	releases := make([]func(), 0, len(keys))
	for _, key := range keys {
		releases = append(releases, s.holdRouterGate(key, false))
	}
	return context.WithValue(ctx, batchGatesCtxKey{}, keys), func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}
}

// batchGateHolder is the batchPlan.holdLocal of a BATCH run at this node, or
// nil when none of the items it runs here names a spread template this node
// holds, so a batch of whole buckets runs exactly as it did.
func (s *Server) batchGateHolder(items []batchItem, routed func(i int) bool) func(ctx context.Context) (context.Context, func()) {
	keys := s.batchSpreadTemplates(items, routed)
	if len(keys) == 0 {
		return nil
	}
	return func(ctx context.Context) (context.Context, func()) { return s.holdBatchGates(ctx, keys) }
}
