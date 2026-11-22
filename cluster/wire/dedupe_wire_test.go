package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// u31CountingSource counts the batches a writer commits, so a write that needs a second
// batch for its dedupe entry or its ordinal is visible.
type u31CountingSource struct {
	storage.KV
	commits *atomic.Int64
}

func (s u31CountingSource) NewBatch() storage.KVBatch {
	return u31CountingBatch{KVBatch: s.KV.NewBatch(), commits: s.commits}
}

type u31CountingBatch struct {
	storage.KVBatch
	commits *atomic.Int64
}

func (b u31CountingBatch) Commit(sync bool) error {
	b.commits.Add(1)
	return b.KVBatch.Commit(sync)
}

// u31Applier applies inserts through the real DocumentApplier and "pops" through a
// small stand-in whose result is a pure function of the record, as a real pop's is.
type u31Applier struct {
	docs    *DocumentApplier
	applied *atomic.Int64
}

func (a u31Applier) Handles(class byte) bool {
	return class == cluster.OpInsert || class == cluster.OpDelete || class == cluster.OpStoreVerb
}

func (a u31Applier) Precondition(slot uint16, db uint64, op cluster.Op) (cluster.Code, error) {
	if op.Class == cluster.OpInsert || op.Class == cluster.OpDelete {
		return a.docs.Precondition(slot, db, op)
	}
	return 0, nil
}

func (a u31Applier) Fill(batch storage.KVBatch, rec AppliedRecord) error {
	_, err := a.FillWithResult(batch, rec)
	return err
}

func (a u31Applier) FillWithResult(batch storage.KVBatch, rec AppliedRecord) ([]byte, error) {
	if rec.Op.Class == cluster.OpInsert || rec.Op.Class == cluster.OpDelete {
		return nil, a.docs.Fill(batch, rec)
	}
	a.applied.Add(1)
	if err := batch.Set(storage.Key(rec.Slot, rec.DB, rec.Op.BucketID, storage.NSFieldStore, rec.Op.EncID), []byte("popped")); err != nil {
		return nil, err
	}
	if err := (pairRecorder{}).RecordApplied(batch, rec); err != nil {
		return nil, err
	}
	return append([]byte("popped:"), rec.Op.EncID...), nil
}

type u31Rig struct {
	*fixture
	commits atomic.Int64
	applied atomic.Int64
	window  machine.WindowConfig
}

func newU31Rig(t *testing.T, window uint64) *u31Rig {
	t.Helper()
	rig := &u31Rig{fixture: newFixture(t), window: machine.WindowConfig{Window: window, MaxResult: 1024}}
	rig.rebuild(rig.pairs)
	return rig
}

// rebuild is a restarted node: a Writer over pairs and the rig's current store, with the
// real dedupe reader.
func (r *u31Rig) rebuild(pairs *cluster.PairSet) {
	r.pairs = pairs
	r.w = NewWriter(WriterConfig{
		Topology: r.topo, Pairs: pairs, Log: r.wal, KV: u31CountingSource{KV: r.kv, commits: &r.commits},
		Applier: u31Applier{docs: NewDocumentApplier(r.kv, pairRecorder{}), applied: &r.applied},
		Clock:   fixedClock{}, Lease: r.lease, Dedupe: NewKVDedupeReader(r.kv), Schema: r.schema,
		Replication: r.repl, WCTimeout: time.Second, DedupeWindow: r.window,
	})
}

// freshStore is a node whose Badger lost every batch the log still holds.
func (r *u31Rig) freshStore() {
	r.t.Helper()
	badger, err := storage.OpenBadgerKV(r.t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		r.t.Fatalf("OpenBadgerKV: %v", err)
	}
	r.t.Cleanup(func() {
		if err := badger.Close(); err != nil {
			r.t.Errorf("closing Badger: %v", err)
		}
	})
	r.kv = &failKV{BadgerKV: badger}
}

func u31PopOp(id string) cluster.Op {
	encoded, err := store.EncodeComponent(id)
	if err != nil {
		panic(err)
	}
	return cluster.Op{
		Class: cluster.OpStoreVerb, BucketID: testBucket, DeclVersion: testSeen, SchemaVersionSeen: testSeen,
		EncID: encoded, Fields: []cluster.FieldOp{{FieldID: 2, Verb: cluster.VerbPop}},
	}
}

func u31PopRequest(id string, reqByte byte) WriteRequest {
	return WriteRequest{Slot: testSlot, DBID: testDB, Op: u31PopOp(id), RequestID: [16]byte{9, reqByte}, Deadline: time.Second}
}

func (r *u31Rig) mustPop(id string, reqByte byte) WriteAnswer {
	r.t.Helper()
	answer, err := r.w.Write(context.Background(), u31PopRequest(id, reqByte))
	if err != nil {
		r.t.Fatalf("pop %q: %v", id, err)
	}
	return answer
}

func (r *u31Rig) storedEntry(key machine.DedupeKey) *machine.DedupeEntry {
	r.t.Helper()
	entry, err := NewKVDedupeReader(r.kv).Lookup(testSlot, testDB, testBucket, key)
	if err != nil {
		r.t.Fatalf("Lookup: %v", err)
	}
	return entry
}

// pairRecordOrdinal reads the stand-in pair record, which carries the ordinal at [24:32].
func (r *u31Rig) pairRecordOrdinal() (uint64, bool) {
	r.t.Helper()
	snapshot := r.kv.NewSnapshot()
	defer snapshot.Discard()
	raw, err := snapshot.Get(pairRecordKey(testSlot, testDB))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return 0, false
	}
	if err != nil {
		r.t.Fatalf("reading the pair record: %v", err)
	}
	return binary.BigEndian.Uint64(raw[24:32]), true
}

// reloadPair is the pair state a restart builds from the pair record: the applied
// position and the ordinal, the latter through CutOrdinal.
func (r *u31Rig) reloadPair() *cluster.PairSet {
	r.t.Helper()
	pairs := &cluster.PairSet{}
	pairs.With(testSlot, testDB, func(p *cluster.Pair) {
		p.Flags = flagReplayed | flagStopProved
		if ordinal, ok := r.pairRecordOrdinal(); ok {
			snapshot := r.kv.NewSnapshot()
			defer snapshot.Discard()
			raw, err := snapshot.Get(pairRecordKey(testSlot, testDB))
			if err != nil {
				r.t.Fatalf("reading the pair record: %v", err)
			}
			p.AppliedPos = cluster.Position{Epoch: binary.BigEndian.Uint64(raw[0:8]), Seq: binary.BigEndian.Uint64(raw[8:16])}
			CutOrdinal(p, ordinal)
		}
	})
	return pairs
}

// offerEveryLoggedRecord hands each logged record straight to Replay, applied or not, as
// a duplicate delivery would.
func (r *u31Rig) offerEveryLoggedRecord() {
	r.t.Helper()
	for _, entry := range r.logged() {
		slot, db, pos, err := storage.DecodePositionKey(entry.Key)
		if err != nil {
			r.t.Fatalf("DecodePositionKey: %v", err)
		}
		record, err := storage.DecodeWALRecord(entry.Op, entry.Data)
		if err != nil {
			r.t.Fatalf("DecodeWALRecord: %v", err)
		}
		if err := r.w.Replay(cluster.WALRecord{WALRecord: record, Slot: slot, DB: db, Pos: cluster.Position(pos), Cursor: cluster.Cursor(entry.Seq)}); err != nil {
			r.t.Fatalf("Replay of %v: %v", pos, err)
		}
	}
}

func u31Key(reqByte byte) machine.DedupeKey {
	return machine.DedupeKey{RequestID: [16]byte{9, reqByte}}
}

func TestDedupeEntryIsWrittenInTheWritesBatch(t *testing.T) {
	rig := newU31Rig(t, 8192)
	request := rig.request(testSlot, "d1")
	request.OpIndex = 3
	answer, err := rig.w.Write(context.Background(), request)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := rig.commits.Load(); got != 1 {
		t.Fatalf("a write committed %d batches, want exactly 1: the document, its dedupe entry and the pair record share one", got)
	}
	entry := rig.storedEntry(machine.DedupeKey{RequestID: request.RequestID, OpIndex: 3})
	if entry == nil {
		t.Fatal("the write left no dedupe entry at (request id, op index 3)")
	}
	if entry.Pos != answer.Pos || entry.Ordinal != 1 {
		t.Fatalf("entry = %+v, want position %s and ordinal 1", entry, answer.Pos)
	}
	if !rig.hasDocument(testSlot, "d1") || !rig.hasPairRecord(testSlot) {
		t.Fatal("the document and the pair record must be in the same batch as the entry")
	}

	// A batch that fails loses all three together: no entry without the position.
	rig.kv.failing.Store(true)
	failed := rig.request(testSlot, "d2")
	failed.RequestID = [16]byte{7, 7}
	lost, err := rig.w.Write(context.Background(), failed)
	if err != nil || lost.ApplyFailure == nil {
		t.Fatalf("Write on a failing store = %+v, %v, want an apply failure", lost, err)
	}
	if rig.storedEntry(machine.DedupeKey{RequestID: failed.RequestID}) != nil {
		t.Fatal("a failed batch left a dedupe entry behind")
	}
	if rig.hasDocument(testSlot, "d2") {
		t.Fatal("a failed batch left its document behind")
	}
}

func TestRetriedPopAfterRestartReturnsTheRecordedValue(t *testing.T) {
	for _, tc := range []struct {
		name        string
		lostBatch   bool
		wantApplied int64
	}{
		{"batch durable", false, 1},
		{"batch lost", true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newU31Rig(t, 8192)
			first := rig.mustPop("e1", 1)
			if string(first.Result) != "popped:"+string(u31PopOp("e1").EncID) {
				t.Fatalf("first pop result = %q, want the applied element", first.Result)
			}
			if first.Deduped {
				t.Fatal("the first attempt cannot be deduped")
			}

			var pairs *cluster.PairSet
			if tc.lostBatch {
				rig.freshStore()
				pairs = &cluster.PairSet{}
				pairs.With(testSlot, testDB, func(p *cluster.Pair) { p.Flags = flagReplayed | flagStopProved })
			} else {
				pairs = rig.reloadPair()
			}
			rig.rebuild(pairs)
			rig.replayAll(pairs)

			retry, err := rig.w.Write(context.Background(), u31PopRequest("e1", 1))
			if err != nil {
				t.Fatalf("retry: %v", err)
			}
			if !retry.Deduped || retry.Pos != first.Pos || !bytes.Equal(retry.Result, first.Result) {
				t.Fatalf("retry = %+v, want the first answer's position %s and result %q", retry, first.Pos, first.Result)
			}
			if got := rig.applied.Load(); got != tc.wantApplied {
				t.Fatalf("the pop was applied %d times, want %d (replay re-applies a lost batch; a retry applies nothing)", got, tc.wantApplied)
			}
			if got := len(rig.logged()); got != 1 {
				t.Fatalf("log holds %d entries, want 1: a deduped retry appends nothing", got)
			}
		})
	}
}

func TestARetryOutsideTheWindowAppliesAgain(t *testing.T) {
	rig := newU31Rig(t, 2)
	rig.mustPop("w1", 1)
	rig.mustPop("w2", 2)
	third := rig.mustPop("w3", 3)

	if got := rig.mustPop("w3", 3); !got.Deduped || got.Pos != third.Pos {
		t.Fatalf("retry of the newest = %+v, want deduped at %s", got, third.Pos)
	}
	again := rig.mustPop("w1", 1)
	if again.Deduped || again.Pos.Seq != 4 {
		t.Fatalf("retry of the oldest = %+v, want a new application at seq 4: its entry left the 2-record window", again)
	}
}

func TestAnEpochChangeDoesNotKillTheWindow(t *testing.T) {
	rig := newU31Rig(t, 8192)
	first := rig.mustPop("p1", 1)
	rig.topo.mu.Lock()
	rig.topo.committed++
	rig.topo.mu.Unlock()
	retry := rig.mustPop("p1", 1)
	if !retry.Deduped || retry.Pos != first.Pos || !bytes.Equal(retry.Result, first.Result) {
		t.Fatalf("retry after an epoch change = %+v, want the recorded answer at %s", retry, first.Pos)
	}
}

func TestASeedNeedsNoDedupeSideChannel(t *testing.T) {
	owner := newU31Rig(t, 8192)
	var firsts []WriteAnswer
	for i, id := range []string{"s1", "s2", "s3"} {
		firsts = append(firsts, owner.mustPop(id, byte(i+1)))
	}
	ordinal, _ := owner.pairRecordOrdinal()

	// The seed is the pair's range and nothing else: no record bodies, no dedupe message.
	copyRig := newU31Rig(t, 8192)
	copyRig.freshStore()
	batch := copyRig.kv.NewBatch()
	snapshot := owner.kv.NewSnapshot()
	prefix := storage.PairPrefix(testSlot, testDB)
	iter := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: append(append([]byte(nil), prefix[:9]...), prefix[9]+1)})
	for iter.Seek(nil); iter.Valid(); iter.Next() {
		value, err := iter.Value()
		if err != nil {
			t.Fatalf("reading the seed: %v", err)
		}
		if err := batch.Set(iter.Key(), value); err != nil {
			t.Fatalf("copying the seed: %v", err)
		}
	}
	iter.Close()
	snapshot.Discard()
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing the seed: %v", err)
	}

	for _, tc := range []struct {
		name string
		cut  bool
	}{{"ordinal never fed", false}, {"ordinal cut from the seed", true}} {
		t.Run(tc.name, func(t *testing.T) {
			pairs := &cluster.PairSet{}
			pairs.With(testSlot, testDB, func(p *cluster.Pair) {
				p.Flags = flagReplayed | flagStopProved
				if tc.cut {
					CutOrdinal(p, ordinal)
				}
			})
			copyRig.rebuild(pairs)
			for i, id := range []string{"s1", "s2", "s3"} {
				retry, err := copyRig.w.Write(context.Background(), u31PopRequest(id, byte(i+1)))
				if err != nil {
					t.Fatalf("retry of %s: %v", id, err)
				}
				if !retry.Deduped || retry.Pos != firsts[i].Pos || !bytes.Equal(retry.Result, firsts[i].Result) {
					t.Fatalf("retry of %s at the seeded copy = %+v, want the owner's %+v", id, retry, firsts[i])
				}
			}
			if got := copyRig.applied.Load(); got != 0 {
				t.Fatalf("the seeded copy applied %d pops for retries, want 0", got)
			}
			if got := len(copyRig.logged()); got != 0 {
				t.Fatalf("the seeded copy's log holds %d entries, want 0: a deduped retry appends nothing", got)
			}
		})
	}
}

func TestTheOrdinalIsWrittenInTheWritesBatch(t *testing.T) {
	rig := newU31Rig(t, 2)
	for i, id := range []string{"o1", "o2", "o3"} {
		rig.mustPop(id, byte(i+1))
		stored, ok := rig.pairRecordOrdinal()
		entry := rig.storedEntry(u31Key(byte(i + 1)))
		if !ok || entry == nil || stored != uint64(i+1) || entry.Ordinal != stored {
			t.Fatalf("after write %d: pair record ordinal %d (%v), entry %+v; want both %d from one batch", i+1, stored, ok, entry, i+1)
		}
	}

	// A batch that fails moves nothing: not the stored ordinal, not the in-memory one.
	rig.kv.failing.Store(true)
	lost, err := rig.w.Write(context.Background(), u31PopRequest("o4", 4))
	if err != nil || lost.ApplyFailure == nil {
		t.Fatalf("Write on a failing store = %+v, %v, want an apply failure", lost, err)
	}
	if got := rig.pairs.Snapshot(testSlot, testDB).Ordinal; got != 3 {
		t.Fatalf("in-memory ordinal = %d after a failed batch, want 3", got)
	}
	if stored, _ := rig.pairRecordOrdinal(); stored != 3 {
		t.Fatalf("stored ordinal = %d after a failed batch, want 3", stored)
	}
	rig.kv.failing.Store(false)

	// Replaying the log, twice, applies the lost record once and counts every record once.
	rig.replayAll(rig.pairs)
	rig.replayAll(rig.pairs)
	rig.offerEveryLoggedRecord()
	if got := rig.pairs.Snapshot(testSlot, testDB).Ordinal; got != 4 {
		t.Fatalf("ordinal after replaying twice = %d, want 4: a replay of an applied record must not count it again", got)
	}
	if stored, _ := rig.pairRecordOrdinal(); stored != 4 {
		t.Fatalf("stored ordinal after replay = %d, want 4", stored)
	}

	// A drifted ordinal would put the two live entries (window 2) outside the window.
	sweeper := NewDedupeSweeper(rig.kv, rig.pairs, rig.window)
	if _, _, err := sweeper.Pass(testSlot, testDB, 100); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	for _, reqByte := range []byte{3, 4} {
		if rig.storedEntry(u31Key(reqByte)) == nil {
			t.Fatalf("the sweep deleted the live entry of request %d: the ordinal drifted", reqByte)
		}
	}
	for _, reqByte := range []byte{1, 2} {
		if rig.storedEntry(u31Key(reqByte)) != nil {
			t.Fatalf("the sweep kept the dead entry of request %d", reqByte)
		}
	}
}

func TestCutOrdinalMovesTheOrdinalEitherWay(t *testing.T) {
	var pair cluster.Pair
	CutOrdinal(&pair, 50)
	if pair.Ordinal != 50 {
		t.Fatalf("ordinal = %d after a cut to 50", pair.Ordinal)
	}
	CutOrdinal(&pair, 7)
	if pair.Ordinal != 7 {
		t.Fatalf("ordinal = %d after a cut back to 7: a seed may lower it", pair.Ordinal)
	}
}

// u31Entry puts one dedupe entry straight into the store, as a seed would.
func u31PutEntry(t *testing.T, kv storage.KV, ref storage.BucketRef, entry machine.DedupeEntry) {
	t.Helper()
	key, value, err := EncodeDedupeEntry(ref, entry)
	if err != nil {
		t.Fatalf("EncodeDedupeEntry: %v", err)
	}
	batch := kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(key, value); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func u31Junk(t *testing.T, kv storage.KV, ref storage.BucketRef) {
	t.Helper()
	batch := kv.NewBatch()
	defer batch.Discard()
	for _, ns := range []byte{storage.NSDocuments, storage.NSFieldStore, storage.NSDelivery, storage.NSSeedState} {
		if err := batch.Set(storage.Key(ref.Slot, ref.DatabaseID, ref.BucketID, ns, []byte("junk")), []byte("junk")); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

type u31SweepRig struct {
	t       *testing.T
	kv      storage.KV
	pairs   *cluster.PairSet
	cfg     machine.WindowConfig
	sweeper *DedupeSweeper
	live    map[byte]bool
}

// newU31SweepRig fills one pair with ten entries over two buckets, the newest with the
// smallest keys so a sweep that deletes from the front kills live entries, then adds
// junk in other namespaces and a neighbouring database's dead entry.
func newU31SweepRig(t *testing.T) *u31SweepRig {
	t.Helper()
	badger, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("OpenBadgerKV: %v", err)
	}
	t.Cleanup(func() {
		if err := badger.Close(); err != nil {
			t.Errorf("closing Badger: %v", err)
		}
	})
	rig := &u31SweepRig{t: t, kv: badger, pairs: &cluster.PairSet{}, cfg: machine.WindowConfig{Window: 4, MaxResult: 1024}, live: map[byte]bool{}}
	rig.sweeper = NewDedupeSweeper(badger, rig.pairs, rig.cfg)
	for bucket := uint64(40); bucket <= 43; bucket++ {
		u31Junk(t, badger, storage.BucketRef{Slot: testSlot, DatabaseID: testDB, BucketID: bucket})
	}
	for i := 0; i < 10; i++ {
		ref := storage.BucketRef{Slot: testSlot, DatabaseID: testDB, BucketID: 41 + uint64(i%2)}
		reqByte := byte(255 - i)
		u31PutEntry(t, badger, ref, machine.DedupeEntry{
			Key: machine.DedupeKey{RequestID: [16]byte{reqByte}}, Pos: cluster.Position{Epoch: 1, Seq: uint64(i + 1)},
			Ordinal: uint64(i + 1), Result: []byte("r"),
		})
		rig.live[reqByte] = i+1 > 6
	}
	rig.pairs.With(testSlot, testDB, func(p *cluster.Pair) { p.Ordinal = 10 })
	u31PutEntry(t, badger, storage.BucketRef{Slot: testSlot, DatabaseID: testDB + 1, BucketID: 41},
		machine.DedupeEntry{Key: machine.DedupeKey{RequestID: [16]byte{1}}, Ordinal: 1})
	u31PutEntry(t, badger, storage.BucketRef{Slot: testSlot + 1, DatabaseID: testDB, BucketID: 41},
		machine.DedupeEntry{Key: machine.DedupeKey{RequestID: [16]byte{1}}, Ordinal: 1})
	return rig
}

// surviving lists the first request-id byte of every dedupe entry left in the pair.
func (r *u31SweepRig) surviving() map[byte]bool {
	r.t.Helper()
	left := map[byte]bool{}
	snapshot := r.kv.NewSnapshot()
	defer snapshot.Discard()
	prefix := storage.PairPrefix(testSlot, testDB)
	iter := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: append(append([]byte(nil), prefix[:9]...), prefix[9]+1)})
	defer iter.Close()
	for iter.Seek(nil); iter.Valid(); iter.Next() {
		if key := iter.Key(); len(key) == 36 && key[18] == storage.NSDedupe {
			left[key[19]] = true
		}
	}
	return left
}

func (r *u31SweepRig) count(prefix []byte) int {
	r.t.Helper()
	snapshot := r.kv.NewSnapshot()
	defer snapshot.Discard()
	upper := append([]byte(nil), prefix...)
	upper[len(upper)-1]++
	iter := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: upper})
	defer iter.Close()
	n := 0
	for iter.Seek(nil); iter.Valid(); iter.Next() {
		n++
	}
	return n
}

func (r *u31SweepRig) requireNeighboursAndJunk() {
	r.t.Helper()
	for bucket := uint64(40); bucket <= 43; bucket++ {
		ref := storage.BucketRef{Slot: testSlot, DatabaseID: testDB, BucketID: bucket}
		for _, ns := range []byte{storage.NSDocuments, storage.NSFieldStore, storage.NSDelivery, storage.NSSeedState} {
			snapshot := r.kv.NewSnapshot()
			_, err := snapshot.Get(storage.Key(ref.Slot, ref.DatabaseID, ref.BucketID, ns, []byte("junk")))
			snapshot.Discard()
			if err != nil {
				r.t.Fatalf("the sweep deleted junk %#x of bucket %d: %v", ns, bucket, err)
			}
		}
	}
	for _, ref := range []storage.BucketRef{{Slot: testSlot, DatabaseID: testDB + 1, BucketID: 41}, {Slot: testSlot + 1, DatabaseID: testDB, BucketID: 41}} {
		if got := r.count(storage.Key(ref.Slot, ref.DatabaseID, ref.BucketID, storage.NSDedupe)); got != 1 {
			r.t.Fatalf("the sweep touched the entries of %+v: %d left, want 1", ref, got)
		}
	}
}

func TestTheSweepNeverDeletesALiveEntry(t *testing.T) {
	t.Run("one big pass", func(t *testing.T) {
		rig := newU31SweepRig(t)
		if _, _, err := rig.sweeper.Pass(testSlot, testDB, 1000); err != nil {
			t.Fatalf("Pass: %v", err)
		}
		left := rig.surviving()
		if len(left) != 4 {
			t.Fatalf("%d entries left, want exactly the 4 inside the window", len(left))
		}
		for reqByte, live := range rig.live {
			if live && !left[reqByte] {
				t.Fatalf("live entry %d was deleted", reqByte)
			}
		}
		rig.requireNeighboursAndJunk()
	})
	t.Run("small passes converge and never delete a live entry", func(t *testing.T) {
		rig := newU31SweepRig(t)
		for pass := 0; pass < 200 && len(rig.surviving()) > 4; pass++ {
			if _, _, err := rig.sweeper.Pass(testSlot, testDB, 3); err != nil {
				t.Fatalf("Pass %d: %v", pass, err)
			}
			left := rig.surviving()
			for reqByte, live := range rig.live {
				if live && !left[reqByte] {
					t.Fatalf("pass %d deleted live entry %d", pass, reqByte)
				}
			}
		}
		if got := len(rig.surviving()); got != 4 {
			t.Fatalf("%d entries left after the passes, want 4", got)
		}
		rig.requireNeighboursAndJunk()
	})
	t.Run("budget bounds the deletions of a pass", func(t *testing.T) {
		rig := newU31SweepRig(t)
		if _, _, err := rig.sweeper.Pass(testSlot, testDB, 2); err != nil {
			t.Fatalf("Pass: %v", err)
		}
		if got := len(rig.surviving()); got < 8 {
			t.Fatalf("%d entries left after a budget of 2, want at least 8", got)
		}
		if _, _, err := rig.sweeper.Pass(testSlot, testDB, 0); err != nil {
			t.Fatalf("Pass with no budget: %v", err)
		}
	})
	t.Run("a write that races the sweep survives it", func(t *testing.T) {
		rig := newU31SweepRig(t)
		scanned, err := rig.sweeper.scan(testSlot, testDB, nil, 1000)
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		// The dead entry of request 255 is rewritten by a retry that applied again.
		rig.pairs.With(testSlot, testDB, func(p *cluster.Pair) { p.Ordinal = 11 })
		u31PutEntry(t, rig.kv, storage.BucketRef{Slot: testSlot, DatabaseID: testDB, BucketID: 41},
			machine.DedupeEntry{Key: machine.DedupeKey{RequestID: [16]byte{255}}, Ordinal: 11, Result: []byte("fresh")})
		if _, err := rig.sweeper.reap(testSlot, testDB, scanned.candidates, 1000); err != nil {
			t.Fatalf("reap: %v", err)
		}
		left := rig.surviving()
		if !left[255] {
			t.Fatal("the sweep deleted an entry rewritten after it was scanned")
		}
		if left[254] {
			t.Fatal("the sweep kept a dead entry that nobody rewrote")
		}
	})
}

func TestDedupeCodecLayout(t *testing.T) {
	ref := storage.BucketRef{Slot: 5, DatabaseID: 3, BucketID: 41}
	entry := machine.DedupeEntry{
		Key: machine.DedupeKey{RequestID: [16]byte{1, 2, 3}, OpIndex: 9}, Pos: cluster.Position{Epoch: 7, Seq: 11},
		Ordinal: 13, Fingerprint: 0x1122334455667788, Result: []byte("abc"),
	}
	key, value, err := EncodeDedupeEntry(ref, entry)
	if err != nil {
		t.Fatalf("EncodeDedupeEntry: %v", err)
	}
	if len(key) != 36 || !bytes.Equal(key[:18], storage.Scope(5, 3, 41)) || key[18] != storage.NSDedupe || key[19] != 1 || key[35] != 9 {
		t.Fatalf("key = %x, want Scope ‖ 0x0E ‖ request id ‖ op index", key)
	}
	want := []byte{3, 13, 7, 11, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 3, 'a', 'b', 'c'}
	if !bytes.Equal(value, want) {
		t.Fatalf("value = %x, want %x", value, want)
	}
	if !bytes.Equal(DedupeEntryKey(ref, entry.Key), key) {
		t.Fatal("DedupeEntryKey and EncodeDedupeEntry disagree")
	}
	gotRef, got, err := DecodeDedupeEntry(key, value)
	if err != nil || gotRef != ref || got.Key != entry.Key || got.Pos != entry.Pos || got.Ordinal != 13 || got.Fingerprint != 0x1122334455667788 || string(got.Result) != "abc" {
		t.Fatalf("DecodeDedupeEntry = %+v %+v, %v", gotRef, got, err)
	}
}

func TestDedupeCodecRefusesWhatItCannotRead(t *testing.T) {
	ref := storage.BucketRef{Slot: 1, DatabaseID: 2, BucketID: 3}
	key, value, err := EncodeDedupeEntry(ref, machine.DedupeEntry{Result: []byte("xy")})
	if err != nil {
		t.Fatalf("EncodeDedupeEntry: %v", err)
	}
	wrongNamespace := append([]byte(nil), key...)
	wrongNamespace[18] = storage.NSDocuments
	wrongVersion := append([]byte(nil), value...)
	wrongVersion[0] = 2 // the fixed-width format of the build before format 3
	for name, tc := range map[string]struct{ key, value []byte }{
		"short key":        {key[:35], value},
		"long key":         {append(append([]byte(nil), key...), 0), value},
		"wrong namespace":  {wrongNamespace, value},
		"wrong version":    {key, wrongVersion},
		"truncated header": {key, value[:7]},
		"missing result":   {key, value[:len(value)-1]},
		"trailing bytes":   {key, append(append([]byte(nil), value...), 0)},
		"empty value":      {key, nil},
	} {
		if _, _, err := DecodeDedupeEntry(tc.key, tc.value); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	_, _, err = EncodeDedupeEntry(ref, machine.DedupeEntry{Result: make([]byte, 65536)})
	if !errors.Is(err, ErrDedupeResultTooLarge) {
		t.Fatalf("a 65536-byte result: err = %v, want ErrDedupeResultTooLarge", err)
	}
	if _, _, err := DecodeDedupeEntry(key, wrongVersion); err == nil || !strings.Contains(err.Error(), "format 0x02") || !strings.Contains(err.Error(), "format 0x03") {
		t.Fatalf("a format-2 value: err = %v, want a refusal naming 0x02 and this build's 0x03", err)
	}
}

func TestDedupeCodecSurvivesMutation(t *testing.T) {
	rng := rand.New(rand.NewSource(31))
	var seeds [][2][]byte
	for _, result := range [][]byte{nil, []byte("x"), bytes.Repeat([]byte{0xAB}, 300)} {
		key, value, err := EncodeDedupeEntry(storage.BucketRef{Slot: 8191, DatabaseID: 1 << 40, BucketID: 77},
			machine.DedupeEntry{Key: machine.DedupeKey{RequestID: [16]byte{5, 6}, OpIndex: 200}, Pos: cluster.Position{Epoch: 3, Seq: 4}, Ordinal: 5, Result: result})
		if err != nil {
			t.Fatalf("EncodeDedupeEntry: %v", err)
		}
		seeds = append(seeds, [2][]byte{key, value})
	}
	accepted := 0
	for i := 0; i < 100000; i++ {
		seed := seeds[rng.Intn(len(seeds))]
		key, value := append([]byte(nil), seed[0]...), append([]byte(nil), seed[1]...)
		for flips := rng.Intn(4) + 1; flips > 0; flips-- {
			target := &value
			if rng.Intn(3) == 0 {
				target = &key
			}
			switch rng.Intn(3) {
			case 0:
				if len(*target) > 0 {
					(*target)[rng.Intn(len(*target))] ^= byte(1 << rng.Intn(8))
				}
			case 1:
				if len(*target) > 0 {
					*target = (*target)[:rng.Intn(len(*target))]
				}
			default:
				*target = append(*target, byte(rng.Intn(256)))
			}
		}
		ref, entry, err := DecodeDedupeEntry(key, value)
		if err != nil {
			continue
		}
		accepted++
		if len(entry.Result) > len(value) {
			t.Fatalf("decoded a %d-byte result out of a %d-byte value", len(entry.Result), len(value))
		}
		againKey, againValue, err := EncodeDedupeEntry(ref, entry)
		if err != nil || !bytes.Equal(againKey, key) || !bytes.Equal(againValue, value) {
			t.Fatalf("accepted input did not re-encode identically: %x %x -> %x %x (%v)", key, value, againKey, againValue, err)
		}
	}
	if accepted == 0 {
		t.Fatal("the mutator never produced an acceptable input, so the round trip was never exercised")
	}
}

func TestRecordDedupeRefusesAnOversizeResultAndChangesNothing(t *testing.T) {
	badger, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("OpenBadgerKV: %v", err)
	}
	t.Cleanup(func() { _ = badger.Close() })
	batch := badger.NewBatch()
	defer batch.Discard()
	window := machine.Window{Ordinal: 4}
	ref := storage.BucketRef{Slot: 1, DatabaseID: 2, BucketID: 3}
	cfg := machine.WindowConfig{Window: 8, MaxResult: 4}
	refused, err := RecordDedupe(batch, ref, window, machine.DedupeWrite{Result: []byte("12345")}, cfg)
	if !errors.Is(err, ErrDedupeResultTooLarge) {
		t.Fatalf("err = %v, want ErrDedupeResultTooLarge", err)
	}
	if refused.Ordinal != 4 {
		t.Fatalf("a refused write moved the ordinal to %d", refused.Ordinal)
	}
	window, err = RecordDedupe(batch, ref, window, machine.DedupeWrite{Result: []byte("1234")}, cfg)
	if err != nil {
		t.Fatalf("RecordDedupe: %v", err)
	}
	if window.Ordinal != 5 {
		t.Fatalf("ordinal = %d after a stored write, want 5", window.Ordinal)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	entry, err := NewKVDedupeReader(badger).Lookup(1, 2, 3, machine.DedupeKey{})
	if err != nil || entry == nil || entry.Ordinal != 5 || string(entry.Result) != "1234" {
		t.Fatalf("Lookup = %+v, %v, want the stored entry at ordinal 5", entry, err)
	}
	missing, err := NewKVDedupeReader(badger).Lookup(1, 2, 3, machine.DedupeKey{OpIndex: 1})
	if err != nil || missing != nil {
		t.Fatalf("Lookup of an absent key = %+v, %v, want nil, nil", missing, err)
	}
}

// TestRecordDedupeNeverMovesTheCallersWindowBeforeTheBatchCommits: a batch that is
// discarded or fails must leave the caller's window where it was, or a live window
// inflates and entries are swept early.
func TestRecordDedupeNeverMovesTheCallersWindowBeforeTheBatchCommits(t *testing.T) {
	badger, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("OpenBadgerKV: %v", err)
	}
	t.Cleanup(func() { _ = badger.Close() })
	batch := badger.NewBatch()
	defer batch.Discard()
	original := machine.Window{Ordinal: 4}
	window := original
	ref := storage.BucketRef{Slot: 1, DatabaseID: 2, BucketID: 3}
	next, err := RecordDedupe(batch, ref, window, machine.DedupeWrite{}, machine.WindowConfig{Window: 8, MaxResult: 4})
	if err != nil {
		t.Fatalf("RecordDedupe: %v", err)
	}
	if window != original {
		t.Fatalf("the caller's window moved to %+v before any commit; want %+v", window, original)
	}
	if next.Ordinal != 5 {
		t.Fatalf("returned window = %+v, want ordinal 5", next)
	}
}
