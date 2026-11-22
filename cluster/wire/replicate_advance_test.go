package wire

import (
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// markerApplier fills one marker key per record, so a test can tell whether a record's data
// was staged and whether it landed in the same batch as the pair record.
type markerApplier struct{ fills atomic.Int64 }

func (*markerApplier) Handles(byte) bool { return true }

func (*markerApplier) Precondition(uint16, uint64, cluster.Op) (cluster.Code, error) { return 0, nil }

func (a *markerApplier) Fill(batch storage.KVBatch, rec AppliedRecord) error {
	a.fills.Add(1)
	return batch.Set(markerKey(rec.Slot), []byte("applied"))
}

func markerKey(slot uint16) []byte {
	return storage.Key(slot, reconDB, 5, storage.NSDocuments, []byte("d1"))
}

// copyPath is a Replicator wired the way a networked node wires it: the copy's advancer is
// the seed-aware one over a Reconciler, whose placement decides which pairs the copy takes.
type copyPath struct {
	g       *rig
	slots   slotsOf
	rep     *Replicator
	applier *markerApplier
	wal     *storage.WAL
	watch   *batchWatch
	epoch   uint64
}

func newCopyPath(t *testing.T) *copyPath {
	t.Helper()
	g, st, slots := s0ReconcilerRig(t, nil)
	watch := &batchWatch{KV: g.kv}
	g.rec.cfg.Store = watch
	wal, err := storage.OpenWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	t.Cleanup(func() {
		if err := wal.Close(); err != nil {
			t.Errorf("closing the WAL: %v", err)
		}
	})
	path := &copyPath{g: g, slots: slots, applier: &markerApplier{}, wal: wal, watch: watch, epoch: uint64(st.Epoch)}
	parts := &atomic.Pointer[seedParts]{}
	parts.Store(&seedParts{recon: g.rec})
	path.rep = mustReplicator(t, ReplicatorConfig{
		Self: testSelf, Committed: func() uint64 { return path.epoch }, Pairs: g.pairs, Log: wal, KV: g.kv,
		Applier: path.applier, Advancer: copyAdvancer{seeding: parts},
		OnError: func(err error) { t.Errorf("replicator: %v", err) },
	})
	return path
}

// frame is the first record of the pair at slot, from the owner.
func (p *copyPath) frame(t *testing.T, slot uint16) WriteFrame {
	t.Helper()
	pos := cluster.Position{Epoch: p.epoch, Seq: 1}
	return WriteFrame{Slot: slot, DB: reconDB, From: "n1", Epoch: p.epoch, PairRecord: u32Record(t, pos, cluster.Position{})}
}

func (p *copyPath) write(t *testing.T, slot uint16) WriteAck {
	t.Helper()
	reply, err := p.rep.OnWrite(p.frame(t, slot))
	if err != nil {
		t.Fatalf("OnWrite: %v", err)
	}
	return reply.Ack
}

// requireUntouched fails unless the refused frame left no trace: nothing in the replica
// log, no Stage (so no fill), no record on disk or in memory, no position in the pair set.
func (p *copyPath) requireUntouched(t *testing.T, slot uint16, why string) {
	t.Helper()
	if last := p.wal.LastSequence(); last != 0 {
		t.Fatalf("%s: the replica log holds sequence %d, want nothing appended", why, last)
	}
	if fills := p.applier.fills.Load(); fills != 0 {
		t.Fatalf("%s: Stage ran (%d fills), want it never called", why, fills)
	}
	if _, found := diskRecord(t, p.g.kv, slot); found {
		t.Fatalf("%s: a pair record was written", why)
	}
	if pair := p.g.pairs.Snapshot(slot, reconDB); !pair.AppliedPos.Zero() || pair.Ordinal != 0 {
		t.Fatalf("%s: the pair set moved to %s ordinal %d", why, pair.AppliedPos, pair.Ordinal)
	}
}

// TestACopyRefusesAFrameForAPairOutsideItsTargetSet (BUG-0752, the copy path of
// TestAnAbsentPairOutsideTheTargetSetIsStillRefused): a frame for a pair this node holds no
// record of and is not a copy of is answered AckNotApplied, and nothing is appended, staged
// or written, so the owner keeps the frame and ships it again.
// [BREAK: give the Replicator in newCopyPath NewLocalAdvancer(g.pairs, g.kv) instead of copyAdvancer]
func TestACopyRefusesAFrameForAPairOutsideItsTargetSet(t *testing.T) {
	p := newCopyPath(t)
	if ack := p.write(t, p.slots.outside); ack.Outcome != AckNotApplied {
		t.Fatalf("the frame for a pair outside the target set was answered %v, want AckNotApplied", ack.Outcome)
	}
	p.requireUntouched(t, p.slots.outside, "outside the target set")
}

// TestACopyCreatesItsRecordInTheBatchOfItsFirstAppliedFrame (BUG-0752): a frame for an
// in-target pair with no record applies, and the ready record it creates rides the batch that
// carries the frame's data, at the frame's position, ordinal 1 and the replica log's cursor.
// [BREAK: refuse a record-less pair in Reconciler.advanceLocked with ErrPairNotReady]
func TestACopyCreatesItsRecordInTheBatchOfItsFirstAppliedFrame(t *testing.T) {
	p := newCopyPath(t)
	if ack := p.write(t, p.slots.copyOnly); ack.Outcome != AckApplied {
		t.Fatalf("the frame for an in-target pair was answered %v, want AckApplied", ack.Outcome)
	}
	want := cluster.Position{Epoch: p.epoch, Seq: 1}
	record, found := diskRecord(t, p.g.kv, p.slots.copyOnly)
	if !found || record.State != machine.SeedReady || record.Applied != want || record.Ordinal != 1 || record.WALCursor != p.wal.LastSequence() || record.WALCursor == 0 {
		t.Fatalf("the created record is %+v (found %v), want ready at %s, ordinal 1, cursor %d", record, found, want, p.wal.LastSequence())
	}
	if pair := p.g.pairs.Snapshot(p.slots.copyOnly, reconDB); pair.AppliedPos != want || pair.Ordinal != 1 {
		t.Fatalf("the pair set is at %s ordinal %d, want %s and 1", pair.AppliedPos, pair.Ordinal, want)
	}
	dataKey, recordKey := string(markerKey(p.slots.copyOnly)), string(SeedRecordKey(p.slots.copyOnly, reconDB))
	p.watch.mu.Lock()
	defer p.watch.mu.Unlock()
	for _, keys := range p.watch.committed {
		hasData, hasRecord := false, false
		for _, key := range keys {
			hasData, hasRecord = hasData || key == dataKey, hasRecord || key == recordKey
		}
		if hasData && hasRecord {
			return
		}
		if hasRecord != hasData {
			t.Fatalf("a batch carried the data (%v) without the record (%v)", hasData, hasRecord)
		}
	}
	t.Fatalf("no committed batch carried the frame's data and the pair record together: %v", p.watch.committed)
}

// TestACopyAnswersNotAppliedForAPairAReconcilePassHolds (BUG-0752): while a pass holds the
// pair busy, the copy's locked advance cannot wait (the pass commits under the pair lock the
// apply holds), so the frame is answered AckNotApplied with nothing appended, and the same
// frame applies once the pass is done.
// [BREAK: make AdvanceLocked wait for ErrPairBusy as Advance does (the test then hangs)]
func TestACopyAnswersNotAppliedForAPairAReconcilePassHolds(t *testing.T) {
	p := newCopyPath(t)
	key := reconcileKey{p.slots.copyOnly, reconDB}
	p.g.rec.mu.Lock()
	p.g.rec.busy[key] = struct{}{}
	p.g.rec.mu.Unlock()
	if ack := p.write(t, p.slots.copyOnly); ack.Outcome != AckNotApplied {
		t.Fatalf("the frame for a busy pair was answered %v, want AckNotApplied", ack.Outcome)
	}
	p.requireUntouched(t, p.slots.copyOnly, "a busy pair")
	p.g.rec.mu.Lock()
	delete(p.g.rec.busy, key)
	p.g.rec.mu.Unlock()
	if ack := p.write(t, p.slots.copyOnly); ack.Outcome != AckApplied {
		t.Fatalf("the same frame after the pass was answered %v, want AckApplied", ack.Outcome)
	}
}

// TestAReplicatorWithoutAnAdvancerIsRefused (BUG-0752): a copy that applies without an
// advancer persists no pair record, so the constructor refuses the wiring instead of
// defaulting one that would hide a missing seed-aware advancer.
// [BREAK: default a nil Advancer to NewLocalAdvancer in NewReplicator]
func TestAReplicatorWithoutAnAdvancerIsRefused(t *testing.T) {
	_, err := NewReplicator(ReplicatorConfig{Self: "copy", Pairs: &cluster.PairSet{}})
	if err == nil {
		t.Fatal("NewReplicator accepted a config with no Advancer")
	}
}
