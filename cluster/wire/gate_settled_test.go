package wire

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

type settledSlots struct{}

func (settledSlots) Serves(uint16, uint64) bool { return true }

func (settledSlots) State(uint16, uint64) machine.SlotState { return machine.SlotReady }

// TestAMarkedPairIsHeldByTheGatesWhileTheHandoffLagsTheEpoch (BUG-0758, hazard A): a
// non-owner's first touch marks the pair replayed and stopProved; when an epoch then makes
// this node the owner, the gates must hold it until the Handoff has the epoch.
func TestAMarkedPairIsHeldByTheGatesWhileTheHandoffLagsTheEpoch(t *testing.T) {
	pairs := &cluster.PairSet{}
	NewPairOwnership(pairs).Acquire(5, testDB)
	pair := pairs.Snapshot(5, testDB)
	if pair.Flags != flagReplayed|flagStopProved {
		t.Fatalf("setup: flags %#b, want both bits from the non-owner's touch", pair.Flags)
	}
	lagging := func(uint64) bool { return false }
	fx := newFixture(t)
	fx.topo.committed = testEpoch + 1

	gate := NewReadGate(ReadGateConfig{Topology: fx.topo, Pairs: pairs, Slots: settledSlots{}})
	req := ReadRequest{Slot: 5, DBID: testDB}
	if cause := gate.holdCause(pair, true, false, req); cause == "" {
		t.Fatal("the read gate does not hold a rank-1 pair the Handoff has not caught up to")
	}
	if cause := gate.holdCause(pair, false, false, req); cause != "" {
		t.Fatalf("a non-owner is held: %q", cause)
	}
	if cause := gate.holdCause(pair, true, true, req); cause != "" {
		t.Fatalf("a settled marked pair is held: %q", cause)
	}

	fx.w.cfg.Settled = lagging
	if !fx.w.held(pair, true, 5, testDB) {
		t.Fatal("the writer orders on a rank-1 pair the Handoff has not caught up to")
	}
	if fx.w.held(pair, false, 5, testDB) {
		t.Fatal("the writer holds a non-owner")
	}
	fx.w.cfg.Settled = nil
	if fx.w.held(pair, true, 5, testDB) {
		t.Fatal("a nil Settled holds the writer")
	}
}

// TestHandoffAppliedBeforeAnyStateIsSettled (BUG-0758): a node never fed an epoch (single
// node, no supervisor) must not wedge.
func TestHandoffAppliedBeforeAnyStateIsSettled(t *testing.T) {
	node := newU33Node("n1")
	if !node.h.Applied(7) {
		t.Fatal("a Handoff with no state yet reports unsettled")
	}
}

// TestNodeHandoffWiringFollowsTheHandoff (BUG-0758): the Node's two forms agree with Applied
// and a node without a Handoff is settled.
func TestNodeHandoffWiringFollowsTheHandoff(t *testing.T) {
	handoff := newU33Node("n1").h
	handoff.OnEpoch(u33State(1, 2, u33Active("n1", "n2")))
	node := &Node{handoff: handoff}
	if !node.handoffApplied(1) || node.handoffApplied(2) {
		t.Fatalf("handoffApplied(1)=%v (want true), (2)=%v (want false)", node.handoffApplied(1), node.handoffApplied(2))
	}
	if !(&Node{}).handoffApplied(9) {
		t.Fatal("a node with no Handoff is unsettled")
	}
}

// TestHandoffAppliedTakesNoHandoffLock (BUG-0758): the gates call Applied under a pair lock,
// and the Handoff takes its own lock before a pair's, so Applied must not wait for it.
func TestHandoffAppliedTakesNoHandoffLock(t *testing.T) {
	handoff := newU33Node("n1").h
	handoff.OnEpoch(u33State(1, 2, u33Active("n1", "n2")))
	handoff.mu.Lock()
	defer handoff.mu.Unlock()
	done := make(chan bool)
	go func() { done <- handoff.Applied(1) }()
	select {
	case settled := <-done:
		if !settled {
			t.Fatal("Applied(1) is false after epoch 1 was fed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Applied waits for the Handoff lock")
	}
}
