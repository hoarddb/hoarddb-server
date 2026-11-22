package harness_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// TestACopyRestartDoesNotReapplyAPop (BUG-0752): a copy applies a pop from the replication
// stream and restarts. The copy must persist its pair record with the data, or its boot
// replays every log record above its seed cut: pre-fix the open fails on the first store
// verb it replays (the declaration is not loaded yet), and a replay that got through would
// pop a second element on the copy only. After the restart the copy reports the position
// it had and its pair holds exactly the bytes it held, three elements left and the
// dedupe entry of the first pop.
// [BREAK: Replicator.applyDurably commits through commit() again, without the AdvanceStep]
func TestACopyRestartDoesNotReapplyAPop(t *testing.T) {
	w := bug0749Setup(t)
	request := [16]byte{0x52, 0x07, 1}
	first, err := w.c.Node(w.owner).Pop(proofCtx(t), wire.PopRequest{StoreTarget: w.target()}, wire.WithRequestID(request))
	if err != nil || !first.Found || bug0749Item(t, first.Element) != "a" {
		t.Fatalf("the first pop = %+v, %v, want element a", first, err)
	}
	w.copyCaughtUp(t)

	slot, err := wire.PlacementOf(proofDB, bug0749Bucket, w.id)
	if err != nil {
		t.Fatalf("placing %s: %v", w.id, err)
	}
	dbID := uint64(w.c.Node(w.copyNode).State().Databases[0].ID)
	beforePos := mustPosition(t, w, w.copyNode)
	_, beforeKeys, err := w.c.Node(w.copyNode).PairDump(slot, dbID)
	if err != nil {
		t.Fatalf("dumping the copy before the restart: %v", err)
	}

	w.c.Kill(w.copyNode)
	w.c.Restart(w.copyNode)
	w.c.Await("the restarted copy to report its position", 20*time.Second, func() bool {
		got, err := w.c.Node(w.copyNode).PairPosition(proofCtx(t), proofDB, bug0749Bucket, w.id)
		return err == nil && got == beforePos
	})
	if got := mustPosition(t, w, w.copyNode); got != beforePos {
		t.Fatalf("the restarted copy is at %s, want the position %s it had before", got, beforePos)
	}
	_, afterKeys, err := w.c.Node(w.copyNode).PairDump(slot, dbID)
	if err != nil {
		t.Fatalf("dumping the copy after the restart: %v", err)
	}
	if len(afterKeys) != len(beforeKeys) {
		t.Fatalf("the pair holds %d keys after the restart, %d before", len(afterKeys), len(beforeKeys))
	}
	for i := range beforeKeys {
		if !bytes.Equal(beforeKeys[i].Key, afterKeys[i].Key) || !bytes.Equal(beforeKeys[i].Value, afterKeys[i].Value) {
			t.Fatalf("key %x changed across the restart: the copy re-applied a record", beforeKeys[i].Key)
		}
	}
}

func mustPosition(t *testing.T, w *bug0749World, node string) cluster.Position {
	t.Helper()
	got, err := w.c.Node(node).PairPosition(proofCtx(t), proofDB, bug0749Bucket, w.id)
	if err != nil {
		t.Fatalf("reading the position of %s on %s: %v", w.id, node, err)
	}
	return got
}
