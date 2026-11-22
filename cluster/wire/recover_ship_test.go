package wire

import (
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// TestARecoveredPairIsQueuedForItsCopies (BUG-0865, F2): a replay moves a pair's TailCursor
// without queueing anything for the copies, and the bounded lane read never looks past what was
// queued. A pair this node owns and has just recovered (a promoted copy whose logged record had
// failed to apply) is queued at its new tail, so that record does not wait for the pair's next
// write. The stream here has forgotten the pair, as a promoted node's has.
// [GUARD: no ShipTail after Recovered]
func TestARecoveredPairIsQueuedForItsCopies(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	dbID := s0Schema(t, node)
	ids, slot := s0IDsInOneSlot(t, 1)
	if _, err := node.Insert(context.Background(), u30iDB, "User", bson.D{{Key: "_id_", Value: ids[0]}, {Key: "Name", Value: ids[0]}}); err != nil {
		t.Fatalf("the insert: %v", err)
	}
	tail := node.pairs.Snapshot(slot, dbID).TailCursor
	if tail == 0 {
		t.Fatal("the pair has no tail cursor after a write")
	}
	node.cl.repl.mu.Lock()
	delete(node.cl.repl.pairs, pairKey{slot, dbID})
	node.cl.repl.mu.Unlock()

	if err := node.recoverPair(context.Background(), slot, dbID); err != nil {
		t.Fatalf("recoverPair: %v", err)
	}
	node.cl.repl.mu.Lock()
	sent := node.cl.repl.pairs[pairKey{slot, dbID}]
	node.cl.repl.mu.Unlock()
	if sent == nil || sent.lastCursor != tail {
		t.Fatalf("after the recovery the stream knows the pair at cursor %v, want its tail %d: the recovered pair was not queued", sent, tail)
	}
}
