package wire

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
)

// TestPrepareAdvanceLockedAnswersWhatAdvanceWouldBeforeItsStage: the first section of an owner write
// asks the reconciler the same questions ApplyAdvance asks before it stages: a ready pair passes, a pair
// with a retire staged is busy, and a pair that retired is not ready.
func TestPrepareAdvanceLockedAnswersWhatAdvanceWouldBeforeItsStage(t *testing.T) {
	st := threeNodes(t)
	slot := findSlots(t, st).copyOnly
	e := uint64(st.Epoch)
	shrunk := st
	alterRF(t, &shrunk, 1)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	putRecord(t, kv, slot, readyRecord(cluster.Position{Epoch: e, Seq: 7}))
	var armed atomic.Bool
	store := blockingStore{kv, &armed, make(chan struct{}, 1), make(chan struct{})}
	g := newRigOn(t, kv, store, st, testSelf, true)
	g.boot(t)

	if recordless, err := g.rec.PrepareAdvanceLocked(slot, reconDB); err != nil || recordless {
		t.Fatalf("a ready pair: PrepareAdvanceLocked = %v, %v, want a record and nil", recordless, err)
	}

	armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- g.rec.OnStateSeq(t.Context(), shrunk) }()
	<-store.started
	if _, err := g.rec.PrepareAdvanceLocked(slot, reconDB); !errors.Is(err, ErrPairBusy) {
		close(store.release)
		t.Fatalf("a pair with a retire staged: PrepareAdvanceLocked = %v, want ErrPairBusy", err)
	}
	close(store.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the retire never landed")
	}
	if _, err := g.rec.PrepareAdvanceLocked(slot, reconDB); !errors.Is(err, ErrPairNotReady) {
		t.Fatalf("a pair that retired: PrepareAdvanceLocked = %v, want ErrPairNotReady", err)
	}
}

// TestLocalAdvancerPrepareLockedChecksNothing: a node without a seed record per pair has nothing to
// check, and PrepareLocked changes nothing.
func TestLocalAdvancerPrepareLockedChecksNothing(t *testing.T) {
	advancer := NewLocalAdvancer(&cluster.PairSet{}, nil)
	pair := &cluster.Pair{Flags: flagReplayed}
	if recordless, held, err := advancer.PrepareLocked(pair, 3, 4); err != nil || recordless || held {
		t.Fatalf("PrepareLocked = %v, %v, %v, want false, false, nil (an advancer with no floor tracker holds nothing)", recordless, held, err)
	}
	if *pair != (cluster.Pair{Flags: flagReplayed}) {
		t.Fatalf("PrepareLocked changed the pair: %+v", *pair)
	}
	var _ PipelineAdvancer = advancer
	var _ PipelineAdvancer = seedAwareAdvancer{}
}

// TestAnAdmittedStepAppliesToARecordlessPairWhoseCopySetMoved: [FAIL-FIRST, review MAJ-1]. The first
// section of an owner write is admitted while this node is in the copy set of a pair that has no record;
// the committed state then moves it out during the fsync. The second section applies what the first
// admitted: it must not re-judge the copy set, because its record is already durable and a refusal there
// leaves a position no replay can apply.
func TestAnAdmittedStepAppliesToARecordlessPairWhoseCopySetMoved(t *testing.T) {
	st := threeNodes(t)
	slot := findSlots(t, st).owner
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	g := newRig(t, kv, st, testSelf, true)
	if err := g.rec.LoadRecords(); err != nil {
		t.Fatal(err)
	}
	if recordless, err := g.rec.PrepareAdvanceLocked(slot, reconDB); err != nil || !recordless {
		t.Fatalf("first section, in the copy set: PrepareAdvanceLocked = %v, %v, want a record-less pair and nil", recordless, err)
	}
	moved := st
	moved.Databases = nil // the committed state no longer places this pair here
	g.state = moved

	pos := cluster.Position{Epoch: uint64(st.Epoch), Seq: 1}
	staged := false
	req := AdvanceRequest{
		Admitted: true,
		Stage: func(storage.KVBatch, *cluster.Pair) (Advance, error) {
			staged = true
			return Advance{Applied: pos, WALCursor: 1, Ordinal: 1}, nil
		},
		Commit: func(batch storage.KVBatch, _ bool) error { return batch.Commit(false) },
	}
	var err error
	g.pairs.With(slot, reconDB, func(shared *cluster.Pair) {
		err = g.rec.ApplyAdvanceLocked(slot, reconDB, shared, req)
	})
	if err != nil || !staged {
		t.Fatalf("an admitted step on a record-less pair = %v (staged %v), want applied", err, staged)
	}
	if rec, ok := g.rec.Record(slot, reconDB); !ok || rec.Applied != pos {
		t.Fatalf("the record is %+v (present %v), want applied at %s", rec, ok, pos)
	}

	// The guard stays for an unadmitted step: a live step is still refused outside the copy set.
	other := findSlots(t, st).outside
	req.Admitted = false
	g.pairs.With(other, reconDB, func(shared *cluster.Pair) {
		err = g.rec.ApplyAdvanceLocked(other, reconDB, shared, req)
	})
	if !errors.Is(err, ErrPairNotReady) {
		t.Fatalf("an unadmitted step outside the copy set = %v, want ErrPairNotReady", err)
	}
}

// TestTheNodesAdvancerCarriesAdmittedToTheReconciler: [GUARD: advanceRequestOf drops Admitted, re-review M2].
// The production path of the second section is seedAwareAdvancer.AdvanceLocked: an admitted step applies to a
// record-less pair whose copy set moved after the first section, and an unadmitted one is still refused.
func TestTheNodesAdvancerCarriesAdmittedToTheReconciler(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	g := newRig(t, kv, st, testSelf, true)
	if err := g.rec.LoadRecords(); err != nil {
		t.Fatal(err)
	}
	moved := st
	moved.Databases = nil
	g.state = moved
	advancer := seedAwareAdvancer{recon: g.rec}
	step := func(admitted bool) AdvanceStep {
		return AdvanceStep{
			Admitted: admitted,
			Stage: func(storage.KVBatch, *cluster.Pair) (AdvanceTo, error) {
				return AdvanceTo{Applied: cluster.Position{Epoch: uint64(st.Epoch), Seq: 1}, WALCursor: 1, Ordinal: 1}, nil
			},
			Commit: func(batch storage.KVBatch, _ bool) error { return batch.Commit(false) },
		}
	}
	var err error
	g.pairs.With(slots.outside, reconDB, func(pair *cluster.Pair) {
		err = advancer.AdvanceLocked(pair, slots.outside, reconDB, step(false))
	})
	if !errors.Is(err, ErrPairNotReady) {
		t.Fatalf("an unadmitted step on a record-less pair outside the copy set = %v, want ErrPairNotReady", err)
	}
	g.pairs.With(slots.owner, reconDB, func(pair *cluster.Pair) {
		err = advancer.AdvanceLocked(pair, slots.owner, reconDB, step(true))
	})
	if err != nil {
		t.Fatalf("an admitted step through the node's advancer = %v, want applied", err)
	}
}
