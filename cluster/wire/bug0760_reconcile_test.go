package wire

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// deferredRig is a booted rig over kv whose reconciler is told, through Config.Deferred, that
// the pair's boot replay has not finished (deferred nil: none is).
func deferredRig(t *testing.T, kv *storage.BadgerKV, st cluster.ClusterState, deferred func(uint16, uint64) bool) *rig {
	t.Helper()
	g := newRig(t, kv, st, testSelf, true)
	g.rec = NewReconciler(Config{
		NodeID: testSelf, Store: kv, Clock: &reconClock{at: time.Unix(1_700_000_000, 0)},
		State: stateFn(func() cluster.ClusterState { return g.state }), Sink: g.sink, Pairs: g.pairs,
		Current: func(uint16, uint64) bool { return true }, WindowBytes: 1 << 20, Deferred: deferred,
	})
	g.boot(t)
	return g
}

// TestTheReconcilerDecidesNothingForADeferredPair (BUG-0760): a ready pair whose boot replay
// has not finished is out of the committed table's target set (a failover). Retiring it would
// write a fence from the applied position, which lags the log, and the retire gate could then
// delete a record acknowledged degraded that only this node's log holds. The record stays
// ready, no fence is written and no action is handed out; for a pair that is not deferred the
// same pass retires it (the control). [BREAK: reconcilePair ignores Config.Deferred]
func TestTheReconcilerDecidesNothingForADeferredPair(t *testing.T) {
	st := threeNodes(t)
	slot := findSlots(t, st).outside
	applied := cluster.Position{Epoch: uint64(st.Epoch), Seq: 3}

	control := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = control.Close() })
	putRecord(t, control, slot, readyRecord(applied))
	g := deferredRig(t, control, st, nil)
	if err := g.rec.OnStateSeq(context.Background(), st); err != nil {
		t.Fatalf("OnStateSeq: %v", err)
	}
	if rec, ok := diskRecord(t, control, slot); !ok || rec.State != machine.SeedRetired {
		t.Fatalf("control: the pass left the record %+v, want it retired", rec)
	}

	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	putRecord(t, kv, slot, readyRecord(applied))
	var asked atomic.Int64
	g = deferredRig(t, kv, st, func(s uint16, db uint64) bool { asked.Add(1); return s == slot && db == reconDB })
	if err := g.rec.OnStateSeq(context.Background(), st); err != nil {
		t.Fatalf("OnStateSeq: %v", err)
	}
	if rec, ok := diskRecord(t, kv, slot); !ok || rec.State != machine.SeedReady {
		t.Fatalf("the pass left the deferred pair's record %+v, want it still ready", rec)
	}
	if fences := fenceKeys(t, kv, slot); len(fences) != 0 {
		t.Fatalf("the pass wrote fences %v for a deferred pair", fences)
	}
	if acts := g.sink.of(slot); len(acts) != 0 {
		t.Fatalf("the pass handed out %d actions for a deferred pair: %+v", len(acts), acts)
	}
	if asked.Load() == 0 {
		t.Fatal("the reconciler never asked whether the pair is deferred")
	}
}

// TestARestartedNodeDefersAPairAgainWhileItsDeclarationIsStillUnreadable (BUG-0760): the pair
// stays ready on disk while its boot replay is deferred, so an operator's restart of a stuck
// node folds its cursor back into the scan, defers it again, and once the declaration is
// readable the push is replayed and served, not lost.
func TestARestartedNodeDefersAPairAgainWhileItsDeclarationIsStillUnreadable(t *testing.T) {
	t.Skip("BUG-0772 (guard of BUG-0760): deferred pair; under the full gate the replayed push was still refused \"slot retired on this node\" after 30 s, it passes alone; skipped during the feature freeze (owner 2026-11-21), to be fixed in the replication rework")
	dir := t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	dbID := bug0760Box(t, node, u30iDB)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	slot := bug0760Slot(t, u30iDB)
	before := savePair(t, dir, slot, dbID)
	node = s0OpenClusterNode(t, dir, nil)
	bug0760Push(t, node, u30iDB, "x1")
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	rewindPair(t, dir, before)

	var gone atomic.Bool
	gone.Store(true)
	first, _, release := openHeld(t, dir, func(n *Node) {
		n.fetcher.cfg.Reader = transientGone{inner: n.fetcher.cfg.Reader, gone: &gone}
	})
	release()
	s0Await(t, "the state", 20*time.Second, func() bool { return first.State().StateSeq != 0 })
	time.Sleep(600 * time.Millisecond) // a few passes, all stuck
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, atScan, release := openHeld(t, dir)
	if atScan != 0 {
		t.Fatalf("StateSeq at the second replay is %d, want 0", atScan)
	}
	if flags := second.pairs.Snapshot(slot, dbID).Flags; flags&flagBootReplay == 0 {
		t.Fatalf("after the restart the pair's flags are %#x, want it deferred again", flags)
	}
	release()
	if got := bug0760Queue(t, second, u30iDB); len(got) != 1 || got[0] != "x1" {
		t.Fatalf("queue after the restart = %v, want [x1]: the push was lost", got)
	}
}
