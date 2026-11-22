package wire

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// TestTheNodeWiresTheDrain: a networked node's seed puller holds what the drain needs: the
// applier that writes a buffered write, the replicator that catches up a hole, and the dedupe
// window the drained write is recorded under. [BREAK: leave any one of the three nil in
// Node.newSeedParts]
func TestTheNodeWiresTheDrain(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	parts := node.cl.seeding.Load()
	if parts == nil {
		t.Fatal("the node has no seed parts")
	}
	cfg := parts.puller.cfg
	if cfg.Applier == nil {
		t.Error("PullerConfig.Applier is nil: the drain has nothing to write a buffered write with")
	}
	if cfg.Replicator == nil {
		t.Error("PullerConfig.Replicator is nil: a hole in the buffer has no replicator to catch up from")
	}
	if cfg.DedupeWindow.Window == 0 {
		t.Error("PullerConfig.DedupeWindow is zero: a drained write's dedupe entry would have no window")
	}
}

// TestTheNodeWiresThePacer: a networked node's seed puller is paced by the node's seed byte
// rate, so a pull cannot take more than replication.seed_bytes_per_second between them.
// [BREAK: leave PullerConfig.Pace unset in Node.newSeedParts]
func TestTheNodeWiresThePacer(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	parts := node.cl.seeding.Load()
	if parts == nil {
		t.Fatal("the node has no seed parts")
	}
	if parts.puller.cfg.Pace == nil {
		t.Error("PullerConfig.Pace is nil: the node's seed byte rate paces no pull")
	}
}

// TestTheNodeWiresTheReceiverDiskCheck: a networked node's seed puller reads its own disk before it
// pulls, so a receiver above its high-water mark pauses its pulls. [BREAK: leave
// PullerConfig.DiskUsedPct unset in Node.newSeedParts]
func TestTheNodeWiresTheReceiverDiskCheck(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	parts := node.cl.seeding.Load()
	if parts == nil {
		t.Fatal("the node has no seed parts")
	}
	if parts.puller.cfg.DiskUsedPct == nil {
		t.Error("PullerConfig.DiskUsedPct is nil: a receiver on a full disk pulls anyway")
	}
}

// TestTheNodeWiresTheDeficitOrder: a networked node's seed puller ranks its queue by the committed
// state's deficits, so the pair with the fewest copies is seeded first.
// [BREAK: leave PullerConfig.Deficits unset in Node.newSeedParts]
func TestTheNodeWiresTheDeficitOrder(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	parts := node.cl.seeding.Load()
	if parts == nil {
		t.Fatal("the node has no seed parts")
	}
	if parts.puller.cfg.Deficits == nil {
		t.Error("PullerConfig.Deficits is nil: the node seeds its pairs in arrival order")
	}
}

// TestASinkActionKicksTheGate: each gate action and a RETIRE proposal leave one wake pending
// on the gate, so the gate runs after every reconcile pass that touches a retired pair.
// [BREAK: drop the ProposeRetire kick in seedSink.Do]
func TestASinkActionKicksTheGate(t *testing.T) {
	cases := []struct {
		name   string
		action Action
	}{
		{"SlotRetire", Action{SlotAction: machine.SlotAction{Do: machine.SlotRetire}}},
		{"SlotGate", Action{SlotAction: machine.SlotAction{Do: machine.SlotGate}}},
		{"SlotDeleteRange", Action{SlotAction: machine.SlotAction{Do: machine.SlotDeleteRange}}},
		{"SlotDeleteRangeThenSeed", Action{SlotAction: machine.SlotAction{Do: machine.SlotDeleteRangeThenSeed}}},
		{"SlotFinishDeleteFirst", Action{SlotAction: machine.SlotAction{Do: machine.SlotFinishDeleteFirst}}},
		{"ProposeRetire", Action{SlotAction: machine.SlotAction{Do: machine.SlotDoNothing, ProposeRetire: true}}},
	}
	for _, tc := range cases {
		gate := newGateDriver(gateDeps{})
		sink := seedSink{node: &Node{cl: &clusterParts{gate: gate}}}
		if err := sink.Do(tc.action); err != nil {
			t.Fatalf("%s: Do: %v", tc.name, err)
		}
		if pending := len(gate.wake); pending != 1 {
			t.Errorf("%s: %d wakes pending on the gate, want 1", tc.name, pending)
		}
	}
}
