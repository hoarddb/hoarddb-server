package wire

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// TestADroppedPairOfARealNodeForgetsItsSweepCursor proves Node.wire connects PairSet.Drop to
// the dedupe sweeper; the sweep rig wires OnDrop by hand, so it cannot.
func TestADroppedPairOfARealNodeForgetsItsSweepCursor(t *testing.T) {
	node, err := OpenNode(context.Background(), NodeConfig{Dir: filepath.Join(t.TempDir(), "ring"), DedupeWindow: 4})
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	defer func() { _ = node.Close() }()
	const slot, db = 1, uint64(9)
	node.pairs.With(slot, db, func(pair *cluster.Pair) { pair.Ordinal = 40 })
	ref := storage.BucketRef{Slot: slot, DatabaseID: db, BucketID: 7}
	for i := uint64(1); i <= 40; i++ {
		var id [16]byte
		binary.BigEndian.PutUint64(id[8:], i)
		u31PutEntry(t, node.kv, ref, machine.DedupeEntry{Key: machine.DedupeKey{RequestID: id}, Pos: cluster.Position{Epoch: 1, Seq: i}, Ordinal: i})
	}
	if _, _, err := node.dedupe.Pass(slot, db, 3); err != nil {
		t.Fatal(err)
	}
	cursors := func() int {
		node.dedupe.mu.Lock()
		defer node.dedupe.mu.Unlock()
		return len(node.dedupe.cursors)
	}
	if cursors() != 1 {
		t.Fatalf("%d cursors after a partial pass, want 1", cursors())
	}
	node.pairs.Drop(slot, db)
	if got := cursors(); got != 0 {
		t.Fatalf("%d cursors after the node dropped the pair, want 0: Node.wire must call pairs.OnDrop(dedupe.Forget)", got)
	}
}
