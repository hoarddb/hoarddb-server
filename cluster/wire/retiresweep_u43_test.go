package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// u43 rig: a real Badger, a pair set and a declaration source the test controls, under a
// RetireSweep with no goroutine of its own running (tests call Pass and Drain).

const (
	u43DB     uint64 = 7
	u43Bucket uint64 = 0x1f4a2c9b7de30015
	u43Other  uint64 = 0x2222222222222222
	u43Slot   uint16 = 1234
)

// u43Decls is a DeclarationSource over a map of declarations by name.
type u43Decls struct {
	mu    sync.Mutex
	decls map[string]machine.Declaration
	names map[uint64]string
	err   error // fails the name read and the declaration read
	// readErr fails the declaration read only, after the name was found.
	readErr error
	reads   atomic.Int64
}

func (d *u43Decls) Read(_ context.Context, _ uint64, name string) (machine.Declaration, []byte, bool, error) {
	d.reads.Add(1)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return machine.Declaration{}, nil, false, d.err
	}
	if d.readErr != nil {
		return machine.Declaration{}, nil, false, d.readErr
	}
	decl, ok := d.decls[name]
	return decl, nil, ok, nil
}

func (d *u43Decls) bucketName(_ context.Context, _ uint64, bucketID uint64) (string, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return "", false, d.err
	}
	name, ok := d.names[bucketID]
	return name, ok, nil
}

// u43Table is a bucket { _id_, Name 2, Age 3, City 4, Notes 6 } whose field 5 was dropped.
func u43Table() machine.FieldTable {
	return machine.FieldTable{
		NextFieldID: 8,
		Entries: []machine.FieldEntry{
			{ID: 1, Type: "any", Name: "_id_"}, {ID: 2, Type: "string", Name: "Name"}, {ID: 3, Type: "int", Name: "Age"},
			{ID: 4, Type: "string", Name: "City"}, {ID: 6, Type: "fifoStore", Name: "Notes"}, {ID: 7, Type: "fifoStore", Name: "Log"},
		},
		Retired: []machine.RetiredID{{ID: 5, Epoch: 9}},
	}
}

type u43Rig struct {
	t     *testing.T
	kv    storage.KV
	pairs *cluster.PairSet
	decls *u43Decls
	deps  RetireDeps
	sweep *RetireSweep
}

func newU43Rig(t *testing.T) *u43Rig {
	t.Helper()
	badger := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = badger.Close() })
	return newU43RigOn(t, badger)
}

func newU43RigOn(t *testing.T, kv storage.KV) *u43Rig {
	t.Helper()
	rig := &u43Rig{t: t, kv: kv, pairs: &cluster.PairSet{}, decls: &u43Decls{
		decls: map[string]machine.Declaration{"Person": {BucketID: u43Bucket, State: machine.StateActive, Fields: u43Table()}},
		names: map[uint64]string{u43Bucket: "Person"},
	}}
	rig.pairs.With(u43Slot, u43DB, func(*cluster.Pair) {})
	rig.deps = RetireDeps{
		KV: kv, Pairs: rig.pairs, Declarations: rig.decls, BucketName: rig.decls.bucketName,
		Published: func(uint64) int64 { return 5 }, KeysPerSecond: 1_000_000, StepKeys: 1000,
		Sleep: func(context.Context, time.Duration) error { return nil },
	}
	rig.sweep = NewRetireSweep()
	rig.deps.Manual = true
	rig.sweep.Start(context.Background(), rig.deps)
	t.Cleanup(rig.sweep.Close)
	return rig
}

// put writes keys with a one-byte value.
func (r *u43Rig) put(keys ...[]byte) {
	r.t.Helper()
	batch := r.kv.NewBatch()
	for _, key := range keys {
		if err := batch.Set(key, []byte{1}); err != nil {
			r.t.Fatal(err)
		}
	}
	if err := batch.Commit(false); err != nil {
		r.t.Fatal(err)
	}
}

// all lists every key of the store, copied.
func (r *u43Rig) all() [][]byte {
	r.t.Helper()
	snap := r.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{})
	defer it.Close()
	var keys [][]byte
	for it.Seek(nil); it.Valid(); it.Next() {
		keys = append(keys, append([]byte(nil), it.Key()...))
	}
	if err := it.Err(); err != nil {
		r.t.Fatal(err)
	}
	return keys
}

func (r *u43Rig) has(key []byte) bool {
	r.t.Helper()
	for _, have := range r.all() {
		if bytes.Equal(have, key) {
			return true
		}
	}
	return false
}

func (r *u43Rig) record(slot uint16, field uint16) RetireRecord {
	return RetireRecord{Slot: slot, DBID: u43DB, BucketID: u43Bucket, FieldID: field, Epoch: 9}
}

// u43Layout is one bucket's keys under the retired id 5 and its neighbours, and the keys that
// must survive. Document ids share byte prefixes ("d", "d1", "d1\x00x"), and field ids differ
// from 5 in one byte (4, 6, 0x0105, 0x0500).
type u43Layout struct {
	doomed, kept [][]byte
}

func u43Build(rig *u43Rig, bucket uint64) u43Layout {
	rig.t.Helper()
	var layout u43Layout
	for _, id := range []string{"d", "d1", "d1\x00x", "\x00\x05", "z"} {
		enc, err := store.EncodeComponent(id)
		if err != nil {
			rig.t.Fatal(err)
		}
		doc := store.DocumentPrefix(u43Slot, u43DB, bucket, enc)
		layout.kept = append(layout.kept, doc)
		for _, field := range []uint16{2, 3, 4, 0x0105, 0x0500, 6} {
			layout.kept = append(layout.kept, store.FieldKey(doc, field))
		}
		layout.doomed = append(layout.doomed, store.FieldKey(doc, 5))
		target := store.StorePrefix(u43Slot, u43DB, bucket, enc, 5)
		for _, inner := range []struct {
			kind byte
			rest []byte
		}{{0x01, []byte("k")}, {0x04, []byte{0, 0, 0, 0, 0, 0, 0, 1}}, {0x05, nil}, {0x0D, []byte("h")}, {0x07, []byte{0, 5, 1, 'x'}}} {
			layout.doomed = append(layout.doomed, store.FieldElementKey(target, inner.kind, inner.rest))
		}
		for _, field := range []uint16{4, 6, 7, 0x0105, 0x0500} {
			sibling := store.StorePrefix(u43Slot, u43DB, bucket, enc, field)
			layout.kept = append(layout.kept,
				store.FieldElementKey(sibling, 0x04, []byte{0, 0, 0, 0, 0, 0, 0, 1}), store.FieldElementKey(sibling, 0x05, nil))
		}
	}
	rig.put(layout.doomed...)
	rig.put(layout.kept...)
	return layout
}

// TestRetiredSweepTouchesOnlyThatFieldAndRunsOnce: two buckets, documents whose ids share
// byte prefixes, fields whose ids differ from the retired one in one byte, and a store field
// whose inner keys and nested key bytes contain the retired id: only the retired id's
// keys in the retired bucket go, the 't' record goes with them, and a second pass writes
// and deletes nothing.
func TestRetiredSweepTouchesOnlyThatFieldAndRunsOnce(t *testing.T) {
	rig := newU43Rig(t)
	layout := u43Build(rig, u43Bucket)
	other := u43Build(rig, u43Other)
	rig.put(RetireRecord{Slot: u43Slot, DBID: u43DB, BucketID: u43Bucket, FieldID: 5, Epoch: 9}.Key())

	if err := rig.sweep.Pass(u43DB, u43Bucket, 5, 9); err != nil {
		t.Fatal(err)
	}
	for _, key := range layout.doomed {
		if rig.has(key) {
			t.Fatalf("key %x of the retired field survived", key)
		}
	}
	for _, key := range append(layout.kept, append(other.doomed, other.kept...)...) {
		if !rig.has(key) {
			t.Fatalf("key %x was deleted and is not the retired field of the retired bucket", key)
		}
	}
	if rig.has(rig.record(u43Slot, 5).Key()) {
		t.Fatal("the 't' record survived a pass that found the field gone")
	}

	counting := &u43CountingKV{KV: rig.kv}
	rig.sweep.deps.KV = counting
	before := rig.all()
	if err := rig.sweep.Pass(u43DB, u43Bucket, 5, 9); err != nil {
		t.Fatal(err)
	}
	if counting.commits.Load() != 0 {
		t.Fatalf("a second pass committed %d batches, want none", counting.commits.Load())
	}
	if after := rig.all(); len(after) != len(before) {
		t.Fatalf("a second pass changed the key count from %d to %d", len(before), len(after))
	}
}

// u43CountingKV counts the batches committed through it and lets a test hook each commit.
type u43CountingKV struct {
	storage.KV
	commits      atomic.Int64
	synced       atomic.Int64
	beforeCommit func()
	failAt       int64
}

func (k *u43CountingKV) NewBatch() storage.KVBatch {
	return &u43CountingBatch{KVBatch: k.KV.NewBatch(), kv: k}
}

type u43CountingBatch struct {
	storage.KVBatch
	kv *u43CountingKV
}

var errU43Killed = errors.New("the process was killed mid-batch")

func (b *u43CountingBatch) Commit(sync bool) error {
	n := b.kv.commits.Add(1)
	if sync {
		b.kv.synced.Add(1)
	}
	if b.kv.failAt != 0 && n == b.kv.failAt {
		return errU43Killed
	}
	if b.kv.beforeCommit != nil {
		b.kv.beforeCommit()
	}
	return b.KVBatch.Commit(sync)
}

// TestSweepHoldsThePairLockAcrossReReadAndDelete: the step reads the keys and deletes them
// under one hold of the pair's lock, so no writer can put a key under the retired id between
// the read and the delete; and it lets go of the lock between steps, so a sweep never holds a
// pair across its whole run.
func TestSweepHoldsThePairLockAcrossReReadAndDelete(t *testing.T) {
	rig := newU43Rig(t)
	u43Build(rig, u43Bucket)
	rig.put(rig.record(u43Slot, 5).Key())
	counting := &u43CountingKV{KV: rig.kv}
	rig.sweep.deps.KV = counting
	rig.sweep.deps.StepKeys = 4

	var enteredDuringCommit atomic.Bool
	counting.beforeCommit = func() {
		done := make(chan struct{})
		go func() {
			rig.pairs.With(u43Slot, u43DB, func(*cluster.Pair) {})
			close(done)
		}()
		select {
		case <-done:
			enteredDuringCommit.Store(true)
		case <-time.After(100 * time.Millisecond):
		}
	}
	var enteredBetweenSteps atomic.Int64
	rig.sweep.afterStep = func() {
		done := make(chan struct{})
		go func() {
			rig.pairs.With(u43Slot, u43DB, func(*cluster.Pair) {})
			close(done)
		}()
		select {
		case <-done:
			enteredBetweenSteps.Add(1)
		case <-time.After(time.Second):
		}
	}
	if err := rig.sweep.Pass(u43DB, u43Bucket, 5, 9); err != nil {
		t.Fatal(err)
	}
	if enteredDuringCommit.Load() {
		t.Fatal("a writer got the pair lock between the sweep's read and its delete")
	}
	if enteredBetweenSteps.Load() < 3 {
		t.Fatalf("a writer got the pair lock %d times between steps, want one per step (the sweep holds the pair across its run)", enteredBetweenSteps.Load())
	}
}

// TestSweepWritesOneRecordPerHeldPair: a note for two retired fields writes one 't' record per
// held pair and field, in the pairs this node holds and no others, and no record of any other
// shape.
func TestSweepWritesOneRecordPerHeldPair(t *testing.T) {
	rig := newU43Rig(t)
	for _, slot := range []uint16{3, 900} {
		rig.pairs.With(slot, u43DB, func(*cluster.Pair) {})
	}
	rig.pairs.With(5, u43DB+1, func(*cluster.Pair) {})
	rig.sweep.Note(u43DB, 6, cluster.BumpNote{BucketID: u43Bucket, Retired: []uint16{5, 9}, Epoch: 9})

	prefix := storage.NodeKey(storage.NSReclaim, []byte{reclaimRetireKind})
	var got []RetireRecord
	for _, key := range rig.all() {
		if !bytes.HasPrefix(key, prefix) {
			continue
		}
		rec, err := DecodeRetireKey(key)
		if err != nil {
			t.Fatalf("a record that does not decode: %x: %v", key, err)
		}
		got = append(got, rec)
	}
	want := map[RetireRecord]bool{}
	for _, slot := range []uint16{3, 900, u43Slot} {
		for _, field := range []uint16{5, 9} {
			want[RetireRecord{Slot: slot, DBID: u43DB, BucketID: u43Bucket, FieldID: field, Epoch: 9}] = true
		}
	}
	if len(got) != len(want) {
		t.Fatalf("%d records, want %d (3 held pairs x 2 fields): %+v", len(got), len(want), got)
	}
	for _, rec := range got {
		if !want[rec] {
			t.Fatalf("unexpected record %+v", rec)
		}
	}
}

// TestTheSweepRerunsAfterASeedCompletes: a retired field's keys that a seed delivers after
// the bump's own pass has finished are removed by the re-run on ready, and the re-run takes
// nothing else.
func TestTheSweepRerunsAfterASeedCompletes(t *testing.T) {
	rig := newU43Rig(t)
	layout := u43Build(rig, u43Bucket)
	if err := rig.sweep.RerunOnReady(u43Slot, u43DB); err != nil {
		t.Fatal(err)
	}
	if err := rig.sweep.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, key := range layout.doomed {
		if rig.has(key) {
			t.Fatalf("key %x of the retired field survived the re-run on ready", key)
		}
	}
	for _, key := range layout.kept {
		if !rig.has(key) {
			t.Fatalf("the re-run deleted key %x of a live field", key)
		}
	}
}

// TestASweepNeverDeletesALiveField: a note that names a field the declaration still has live
// (the loser of two alters at one schema_version) deletes nothing and leaves no record.
func TestASweepNeverDeletesALiveField(t *testing.T) {
	rig := newU43Rig(t)
	layout := u43Build(rig, u43Bucket)
	rig.put(RetireRecord{Slot: u43Slot, DBID: u43DB, BucketID: u43Bucket, FieldID: 4, Epoch: 9}.Key())
	rig.put(layout.kept...)
	if err := rig.sweep.Pass(u43DB, u43Bucket, 4, 9); err != nil {
		t.Fatal(err)
	}
	for _, key := range layout.kept {
		if !rig.has(key) {
			t.Fatalf("a note naming live field 4 deleted key %x", key)
		}
	}
	if rig.has(RetireRecord{Slot: u43Slot, DBID: u43DB, BucketID: u43Bucket, FieldID: 4, Epoch: 9}.Key()) {
		t.Fatal("the record of a field that is not retired survived")
	}
}

// TestASweepWaitsWhenTheDeclarationCannotBeRead: an unreadable declaration keeps the record
// and deletes nothing, and the sweep finishes once it can be read.
func TestASweepWaitsWhenTheDeclarationCannotBeRead(t *testing.T) {
	for name, fail := range map[string]func(*u43Decls, error){
		"the name read":        func(d *u43Decls, err error) { d.err = err },
		"the declaration read": func(d *u43Decls, err error) { d.readErr = err },
	} {
		t.Run(name, func(t *testing.T) {
			rig := newU43Rig(t)
			layout := u43Build(rig, u43Bucket)
			rig.put(rig.record(u43Slot, 5).Key())
			fail(rig.decls, errors.New("the declaration's owner is down"))
			if err := rig.sweep.Pass(u43DB, u43Bucket, 5, 9); err == nil {
				t.Fatal("a pass that could not read the declaration reported success")
			}
			for _, key := range layout.doomed {
				if !rig.has(key) {
					t.Fatalf("key %x was deleted without a declaration to prove the field retired", key)
				}
			}
			if !rig.has(rig.record(u43Slot, 5).Key()) {
				t.Fatal("the record was dropped while the declaration was unreadable")
			}
			rig.decls.err, rig.decls.readErr = nil, nil
			if err := rig.sweep.Pass(u43DB, u43Bucket, 5, 9); err != nil {
				t.Fatal(err)
			}
			if rig.has(layout.doomed[0]) {
				t.Fatal("the sweep did not finish once the declaration could be read")
			}
		})
	}
}

// TestASweepKilledMidBatchResumes: the process dies while a batch commits. Nothing live was
// deleted, keys already swept stay swept, the record is still there, and a new sweep over the
// same store finishes the job.
func TestASweepKilledMidBatchResumes(t *testing.T) {
	rig := newU43Rig(t)
	layout := u43Build(rig, u43Bucket)
	rig.put(rig.record(u43Slot, 5).Key())
	counting := &u43CountingKV{KV: rig.kv, failAt: 3}
	rig.sweep.deps.KV = counting
	rig.sweep.deps.StepKeys = 5
	err := rig.sweep.Pass(u43DB, u43Bucket, 5, 9)
	if !errors.Is(err, errU43Killed) {
		t.Fatalf("Pass = %v, want the kill", err)
	}
	for _, key := range layout.kept {
		if !rig.has(key) {
			t.Fatalf("the kill left key %x of a live field deleted", key)
		}
	}
	if !rig.has(rig.record(u43Slot, 5).Key()) {
		t.Fatal("the record is gone although the sweep was killed before it finished")
	}
	surviving := 0
	for _, key := range layout.doomed {
		if rig.has(key) {
			surviving++
		}
	}
	if surviving == 0 || surviving == len(layout.doomed) {
		t.Fatalf("%d of %d doomed keys survive; the test needs the kill to land mid-sweep", surviving, len(layout.doomed))
	}

	fresh := newU43RigOn(t, rig.kv)
	fresh.decls = rig.decls
	fresh.sweep.deps.Declarations = rig.decls
	if err := fresh.sweep.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, key := range layout.doomed {
		if fresh.has(key) {
			t.Fatalf("key %x survived the resumed sweep", key)
		}
	}
	if fresh.has(rig.record(u43Slot, 5).Key()) {
		t.Fatal("the record survived the resumed sweep")
	}
}

// TestASweepRefusesAKeyItCannotParse: a key in the document namespace whose id component is
// malformed is never deleted by guesswork; the pass reports it and keeps its record.
func TestASweepRefusesAKeyItCannotParse(t *testing.T) {
	rig := newU43Rig(t)
	u43Build(rig, u43Bucket)
	bad := storage.Key(u43Slot, u43DB, u43Bucket, storage.NSDocuments, []byte{0x50, 'a', 'b'})
	rig.put(bad, rig.record(u43Slot, 5).Key())
	if err := rig.sweep.Pass(u43DB, u43Bucket, 5, 9); err == nil {
		t.Fatal("a pass over a malformed key reported success")
	}
	if !rig.has(bad) || !rig.has(rig.record(u43Slot, 5).Key()) {
		t.Fatal("the malformed key or the record was deleted")
	}
}

// TestASweepIsBudgeted: with a budget of 100 keys a second the sweep asks to sleep for the
// time its deletions are worth.
func TestASweepIsBudgeted(t *testing.T) {
	rig := newU43Rig(t)
	layout := u43Build(rig, u43Bucket)
	rig.put(rig.record(u43Slot, 5).Key())
	var slept time.Duration
	clock := time.Unix(0, 0)
	rig.sweep.deps.KeysPerSecond = 100
	rig.sweep.deps.StepKeys = 10
	rig.sweep.deps.Now = func() time.Time { return clock }
	rig.sweep.deps.Sleep = func(_ context.Context, d time.Duration) error { slept += d; clock = clock.Add(d); return nil }
	rig.sweep.budget = newReclaimBudget(reclaimBudgetConfig{KeysPerSecond: rig.sweep.deps.KeysPerSecond, StepKeys: rig.sweep.deps.StepKeys, Now: rig.sweep.deps.Now, Sleep: rig.sweep.deps.Sleep})
	if err := rig.sweep.Pass(u43DB, u43Bucket, 5, 9); err != nil {
		t.Fatal(err)
	}
	// The budget holds one step's credit (10 keys); every deletion beyond it is paid for in time.
	floor := time.Duration(len(layout.doomed)-10) * time.Second / 100
	if slept < floor {
		t.Fatalf("slept %v for %d deletions at 100 a second with 10 in credit, want at least %v", slept, len(layout.doomed), floor)
	}
}

// TestAPairThisNodeNoLongerHoldsHasNothingToSweep: a record for a pair that is gone is
// dropped, with no read of any key.
func TestAPairThisNodeNoLongerHoldsHasNothingToSweep(t *testing.T) {
	rig := newU43Rig(t)
	rig.put(rig.record(77, 5).Key())
	if err := rig.sweep.Pass(u43DB, u43Bucket, 5, 9); err != nil {
		t.Fatal(err)
	}
	if rig.has(rig.record(77, 5).Key()) {
		t.Fatal("the record of a pair this node does not hold survived")
	}
}

// TestDroppedFieldValuesAreDeletedInTheBackground: the live test of alter's promise. A bucket
// of documents with scalar fields and two store fields; drop one scalar and one store; the
// node's own sweep deletes exactly those fields' keys, counted before and after, and every
// other field and document reads as before.
func TestDroppedFieldValuesAreDeletedInTheBackground(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	for i := range 20 {
		id := fmt.Sprintf("u%02d", i)
		if _, err := node.Insert(ctx, u30iDB, "User", u30iUser(id, "Ada"+id, int32(30+i))); err != nil {
			t.Fatal(err)
		}
		for range 3 {
			if err := node.Push(ctx, PushRequest{StoreTarget: StoreTarget{Database: u30iDB, Bucket: "User", ID: id, Field: "Purchases"}, Element: u43Element()}); err != nil {
				t.Fatal(err)
			}
		}
	}
	dbID, _ := node.database(u30iDB)
	ref, err := node.bucket(ctx, u30iDB, "User")
	if err != nil {
		t.Fatal(err)
	}
	cityID, purchasesID := u40ID(t, ref.table, "City"), u40ID(t, ref.table, "Purchases")
	cityBefore := u43CountField(t, node, dbID, ref.entry.Decl.BucketID, cityID)
	purchasesBefore := u43CountField(t, node, dbID, ref.entry.Decl.BucketID, purchasesID)
	totalBefore := u43CountBucket(t, node, dbID, ref.entry.Decl.BucketID)
	if cityBefore != 20 || purchasesBefore < 60 {
		t.Fatalf("setup: %d City keys and %d Purchases keys", cityBefore, purchasesBefore)
	}
	if _, err := node.AlterBucket(ctx, u30iDB, "User", AlterChange{Drop: []string{"City", "Purchases"}}); err != nil {
		t.Fatal(err)
	}
	if err := node.retire.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if got := u43CountField(t, node, dbID, ref.entry.Decl.BucketID, cityID); got != 0 {
		t.Fatalf("%d City keys remain after the sweep", got)
	}
	if got := u43CountField(t, node, dbID, ref.entry.Decl.BucketID, purchasesID); got != 0 {
		t.Fatalf("%d Purchases keys remain after the sweep", got)
	}
	if got := u43CountBucket(t, node, dbID, ref.entry.Decl.BucketID); got != totalBefore-cityBefore-purchasesBefore {
		t.Fatalf("the bucket holds %d keys, want %d - %d - %d = %d: the sweep took something else", got, totalBefore, cityBefore, purchasesBefore, totalBefore-cityBefore-purchasesBefore)
	}
	for i := range 20 {
		id := fmt.Sprintf("u%02d", i)
		doc, err := node.Get(ctx, u30iDB, "User", id)
		if err != nil || u30iString(t, doc, "Name") != "Ada"+id {
			t.Fatalf("document %s after the sweep: %v %v", id, doc, err)
		}
	}
	if left := u43RecordCount(t, node); left != 0 {
		t.Fatalf("%d 't' records remain after the sweep", left)
	}
}

func u43Element() bson.D { return bson.D{{Key: "Item", Value: "tea"}} }

// u43Walk calls visit with every key of the bucket in the document and field-store spaces,
// with its field id (0 for a header), parsed here and not by the code under test.
func u43Walk(t *testing.T, node *Node, dbID, bucketID uint64, visit func(key []byte, field uint16)) {
	t.Helper()
	snap := node.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{})
	defer it.Close()
	for it.Seek(nil); it.Valid(); it.Next() {
		key := it.Key()
		if len(key) < 19 || key[0] == 0xFF || binary.BigEndian.Uint64(key[2:10]) != dbID || binary.BigEndian.Uint64(key[10:18]) != bucketID {
			continue
		}
		if key[18] != storage.NSDocuments && key[18] != storage.NSFieldStore {
			continue
		}
		n, err := storage.EncodedComponentLen(key[19:])
		if err != nil {
			t.Fatalf("key %x: %v", key, err)
		}
		var field uint16
		if rest := key[19+n:]; len(rest) >= 2 {
			field = binary.BigEndian.Uint16(rest)
		}
		visit(key, field)
	}
}

func u43CountField(t *testing.T, node *Node, dbID, bucketID uint64, field uint16) int {
	t.Helper()
	count := 0
	u43Walk(t, node, dbID, bucketID, func(_ []byte, have uint16) {
		if have == field {
			count++
		}
	})
	return count
}

func u43CountBucket(t *testing.T, node *Node, dbID, bucketID uint64) int {
	t.Helper()
	count := 0
	u43Walk(t, node, dbID, bucketID, func([]byte, uint16) { count++ })
	return count
}

func u43RecordCount(t *testing.T, node *Node) int {
	t.Helper()
	snap := node.kv.NewSnapshot()
	defer snap.Discard()
	prefix := storage.NodeKey(storage.NSReclaim, []byte{reclaimRetireKind})
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: storage.PrefixEnd(prefix)})
	defer it.Close()
	count := 0
	for it.Seek(prefix); it.Valid(); it.Next() {
		count++
	}
	return count
}

func u43RecordCountKind(t *testing.T, node *Node, kind byte) int {
	t.Helper()
	snap := node.kv.NewSnapshot()
	defer snap.Discard()
	prefix := storage.NodeKey(storage.NSReclaim, []byte{kind})
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: storage.PrefixEnd(prefix)})
	defer it.Close()
	count := 0
	for it.Seek(prefix); it.Valid(); it.Next() {
		count++
	}
	return count
}

// TestARestartResumesTheRetiredFieldSweep: an alter drops a field and the node stops before its
// loop swept anything; the 't' records are in the store, and the reopened node deletes the
// field's keys and only those.
func TestARestartResumesTheRetiredFieldSweep(t *testing.T) {
	node, dir := u30iFresh(t)
	ctx := context.Background()
	for i := range 10 {
		if _, err := node.Insert(ctx, u30iDB, "User", u30iUser(fmt.Sprintf("u%d", i), "Ada", int32(i))); err != nil {
			t.Fatal(err)
		}
	}
	dbID, _ := node.database(u30iDB)
	ref, err := node.bucket(ctx, u30iDB, "User")
	if err != nil {
		t.Fatal(err)
	}
	bucketID, cityID := ref.entry.Decl.BucketID, u40ID(t, ref.table, "City")
	total := u43CountBucket(t, node, dbID, bucketID)
	node.retire.loopMu.Lock()
	if _, err := node.AlterBucket(ctx, u30iDB, "User", AlterChange{Drop: []string{"City"}}); err != nil {
		t.Fatal(err)
	}
	if got := u43CountField(t, node, dbID, bucketID, cityID); got != 10 {
		t.Fatalf("the paused sweep left %d City keys, want 10", got)
	}
	// Stop the loop before letting it run again, so it cannot sweep on its way out.
	node.retire.cancel()
	node.retire.loopMu.Unlock()
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := u30iOpen(t, dir)
	if err := reopened.retire.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if got := u43CountField(t, reopened, dbID, bucketID, cityID); got != 0 {
		t.Fatalf("%d City keys remain after the restart", got)
	}
	if got := u43CountBucket(t, reopened, dbID, bucketID); got != total-10 {
		t.Fatalf("the bucket holds %d keys, want %d", got, total-10)
	}
}

// TestANoteIsDurableBeforeTheBumpReturns: the records a note writes are synced in the same call.
// The Raft group may compact the bump's entry once this node has applied it, and nothing delivers
// the note again, so a record that was only in the page cache when the process died would leave
// the dead keys for ever.
func TestANoteIsDurableBeforeTheBumpReturns(t *testing.T) {
	rig := newU43Rig(t)
	counting := &u43CountingKV{KV: rig.kv}
	rig.sweep.deps.KV = counting
	rig.sweep.Note(u43DB, 6, cluster.BumpNote{BucketID: u43Bucket, Retired: []uint16{5}, Epoch: 9})
	rig.sweep.Note(u43DB, 7, cluster.BumpNote{BucketID: u43Bucket, Drop: true})
	if counting.commits.Load() != 2 || counting.synced.Load() != 2 {
		t.Fatalf("two notes made %d commits, %d of them synced; want both synced", counting.commits.Load(), counting.synced.Load())
	}
}
