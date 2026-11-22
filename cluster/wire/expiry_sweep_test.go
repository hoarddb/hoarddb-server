package wire

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// rigRemover issues the sweep's removals the way the Writer would on the owner: the same
// precondition, then the same apply, with the owner's clock stamped into the record.
type rigRemover struct {
	rig    *expiryRig
	before func() // runs between the sweep's read and the removal: a write that slips in
	skip   bool   // this node holds only a copy
	calls  int
	issued []cluster.Op // the system writes the owner ordered, as logged
}

func (r *rigRemover) RemoveDocument(_ context.Context, entry storage.ExpiryEntry) (bool, error) {
	return r.send(cluster.Op{Class: cluster.OpDelete, BucketID: entry.BucketID, EncID: entry.EncID, System: true})
}

func (r *rigRemover) RemoveElement(_ context.Context, entry storage.ExpiryEntry) (bool, error) {
	op := cluster.Op{
		Class: cluster.OpStoreVerb, BucketID: entry.BucketID, EncID: entry.EncID, System: true,
		Fields: []cluster.FieldOp{{FieldID: entry.FieldID, Verb: cluster.VerbTake, Value: takeArgAt(entry.Position, entry.AtMillis)}},
	}
	return r.send(op)
}

func takeArgAt(position uint64, at int64) []byte {
	arg, err := encodeVerbDocForTest(fmt.Sprintf("%d@%d", position, at))
	if err != nil {
		panic(err)
	}
	return arg
}

func (r *rigRemover) send(op cluster.Op) (bool, error) {
	r.calls++
	if r.before != nil {
		r.before()
	}
	if r.skip {
		return true, nil
	}
	op.Expire = expireItemsOf(expiryTable(), op)
	op.ExpireNow = r.rig.now
	code, err := r.rig.applier.Precondition(testSlot, testDB, op)
	if err != nil {
		return false, err
	}
	if code != 0 {
		return false, &cluster.Error{Code: code, Message: "the system write was refused at the precondition"}
	}
	r.rig.apply(op)
	r.issued = append(r.issued, op)
	return false, nil
}

func newSweepRig(t *testing.T) (*expiryRig, *rigRemover, *ExpirySweeper) {
	t.Helper()
	rig := newExpiryRig(t)
	rig.applier.WithWallClock(func() int64 { return rig.now })
	remover := &rigRemover{rig: rig}
	sweeper := NewExpirySweeper(rig.kv, remover, func() int64 { return rig.now }).
		WithExpiryTable(func(storage.ExpiryEntry) (cluster.AdmittedExpire, bool) { return *rig.declared, true })
	return rig, remover, sweeper
}

func headerState(t *testing.T, rig *expiryRig) HeaderState {
	t.Helper()
	snap := rig.kv.NewSnapshot()
	defer snap.Discard()
	state, err := ReadHeaderState(snap, DocumentKey{Ref: storage.BucketRef{Slot: testSlot, DatabaseID: testDB, BucketID: testBucket}, EncID: claim34EncID0()})
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestTheSweepRemovesAnExpiredDocumentAndNothingElse(t *testing.T) {
	rig, remover, sweeper := newSweepRig(t)
	rig.apply(expiryInsert(1_000))
	rig.now = 500
	if result, err := sweeper.Pass(context.Background(), 10); err != nil || result.Visited != 0 || remover.calls != 0 {
		t.Fatalf("before the time: %+v, %v, %d removals; the sweep touched an entry that is not due", result, err, remover.calls)
	}
	rig.now = 2_000
	result, err := sweeper.Pass(context.Background(), 10)
	if err != nil || result.Removed != 1 {
		t.Fatalf("pass = %+v, %v, want one removal", result, err)
	}
	if headerState(t, rig) == HeaderLive {
		t.Fatal("the expired document is still live after the sweep")
	}
	if entries := expiryQueue(t, rig.kv); len(entries) != 0 {
		t.Fatalf("entries remain after the removal: %+v", entries)
	}
}

func TestTheSweepNeverDeletesADocumentMovedLaterAfterItLooked(t *testing.T) {
	rig, remover, sweeper := newSweepRig(t)
	rig.apply(expiryInsert(1_000))
	rig.now = 2_000
	// The kill window: the sweep has read the entry and verified the document; before its
	// delete is ordered, a client moves the document's time forward.
	remover.before = func() { rig.apply(expiryUpdate(cluster.UpdateSet, 90_000)) }
	result, err := sweeper.Pass(context.Background(), 10)
	if err == nil || result.Removed != 0 {
		t.Fatalf("pass = %+v, %v; the delete of a document that was no longer expired must be refused", result, err)
	}
	if headerState(t, rig) != HeaderLive {
		t.Fatal("the sweep deleted a document that was moved later: that is data loss")
	}
	if got := expiryTimes(expiryQueue(t, rig.kv)); len(got) != 1 || got[0] != 90_000 {
		t.Fatalf("queue after the refused delete = %v, want [90000]", got)
	}
}

func TestOrphanEntriesAreDroppedWithoutARemoval(t *testing.T) {
	rig, remover, sweeper := newSweepRig(t)
	// An entry whose document was never there: the leftover of a dropped bucket or a retire.
	batch := rig.kv.NewBatch()
	if err := setExpiryEntry(batch, storage.ExpiryEntry{AtMillis: 1_000, Slot: testSlot, DBID: testDB, BucketID: testBucket, Kind: storage.ExpiryKindDocument, EncID: claim34EncID0()}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	rig.now = 2_000
	result, err := sweeper.Pass(context.Background(), 10)
	if err != nil || result.Dropped != 1 || remover.calls != 0 {
		t.Fatalf("pass = %+v, %v, %d removals; an orphan is dropped and nothing is removed", result, err, remover.calls)
	}
	if len(expiryQueue(t, rig.kv)) != 0 {
		t.Fatal("the orphan entry is still there")
	}
}

func TestACopyLeavesTheEntryForTheOwner(t *testing.T) {
	rig, remover, sweeper := newSweepRig(t)
	remover.skip = true
	rig.apply(expiryInsert(1_000))
	rig.now = 2_000
	result, err := sweeper.Pass(context.Background(), 10)
	if err != nil || result.Skipped != 1 || result.Removed != 0 || result.Dropped != 0 {
		t.Fatalf("pass = %+v, %v; a copy skips", result, err)
	}
	if len(expiryQueue(t, rig.kv)) != 1 || headerState(t, rig) != HeaderLive {
		t.Fatal("a copy changed the entry or the document")
	}
}

func TestTheSweepTakesExpiredQueueElementsByPosition(t *testing.T) {
	rig, _, sweeper := newSweepRig(t)
	rig.apply(expiryInsert(0))
	rig.apply(expiryPush(t, expiryElement(t, 1_000, "gone")))
	rig.apply(expiryPush(t, expiryElement(t, 90_000, "live")))
	rig.now = 2_000
	result, err := sweeper.Pass(context.Background(), 10)
	if err != nil || result.Removed != 1 {
		t.Fatalf("pass = %+v, %v, want one element removed", result, err)
	}
	if got := expiryTimes(expiryQueue(t, rig.kv)); len(got) != 1 || got[0] != 90_000 {
		t.Fatalf("queue = %v, want only the live element's entry", got)
	}
	popped := rig.apply(expiryPop(t, 0))
	if claim34Text(t, popped) != "live" {
		t.Fatalf("the queue's head is %q after the sweep, want live", claim34Text(t, popped))
	}
}

func TestALeasedExpiredElementWaitsForTheLease(t *testing.T) {
	rig, _, sweeper := newSweepRig(t)
	rig.apply(expiryInsert(0))
	rig.apply(expiryPush(t, expiryElement(t, 1_000, "leased")))
	claim := claim34Op(cluster.OpClaim)
	claim.Count = 1
	claim = StampClaim(claim, 500, [16]byte{7})
	leaseID := rig.apply(claim)
	rig.now = 2_000
	result, err := sweeper.Pass(context.Background(), 10)
	if err != nil || result.Skipped != 1 || result.Dropped != 0 || result.Removed != 0 {
		t.Fatalf("pass = %+v, %v; a leased element's entry must wait, not be dropped", result, err)
	}
	if len(expiryQueue(t, rig.kv)) != 1 {
		t.Fatal("the entry of a leased element was dropped: when the lease returns the element it would never expire")
	}
	ack := claim34Lease(cluster.OpAck, [16]byte(leaseID[:16]), 1_000)
	ack.Expire = []cluster.ExpireItem{{FieldID: expiryQueueField, Path: []byte(expiryPath)}}
	rig.apply(ack)
	if len(expiryQueue(t, rig.kv)) != 0 {
		t.Fatal("the ack left the entry")
	}
}

func TestAStaleElementEntryWithNoLeaseIsDropped(t *testing.T) {
	rig, remover, sweeper := newSweepRig(t)
	rig.apply(expiryInsert(0))
	batch := rig.kv.NewBatch()
	if err := setExpiryEntry(batch, storage.ExpiryEntry{AtMillis: 1_000, Slot: testSlot, DBID: testDB, BucketID: testBucket, Kind: storage.ExpiryKindElement, EncID: claim34EncID0(), FieldID: expiryQueueField, Position: 42}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	rig.now = 2_000
	result, err := sweeper.Pass(context.Background(), 10)
	if err != nil || result.Dropped != 1 || remover.calls != 0 {
		t.Fatalf("pass = %+v, %v; a gone element's entry is dropped with no removal", result, err)
	}
}

func TestThePassIsBoundedAndTheCursorResumes(t *testing.T) {
	rig, _, sweeper := newSweepRig(t)
	rig.apply(expiryInsert(0))
	for i := range 5 {
		rig.apply(expiryPush(t, expiryElement(t, int64(1_000+i), "e")))
	}
	rig.now = 2_000
	first, err := sweeper.Pass(context.Background(), 2)
	if err != nil || first.Visited != 2 || first.Removed != 2 {
		t.Fatalf("first pass = %+v, %v, want 2 visited and removed", first, err)
	}
	second, err := sweeper.Pass(context.Background(), 10)
	if err != nil || second.Removed != 3 {
		t.Fatalf("second pass = %+v, %v, want the remaining 3", second, err)
	}
	if len(expiryQueue(t, rig.kv)) != 0 {
		t.Fatal("entries remain after the backlog drained")
	}
}

// TestADropIsReVerifiedAfterAReInsertSlipsBetweenTheReadAndTheDrop: the sweep reads an orphan
// entry (the document is deleted); before it drops the entry, the same id is inserted again at the
// same time, which sets the very same key for a live document. The drop must see that and leave it.
func TestADropIsReVerifiedAfterAReInsertSlipsBetweenTheReadAndTheDrop(t *testing.T) {
	rig, _, sweeper := newSweepRig(t)
	rig.apply(expiryInsert(1_000))
	rig.apply(expiryDelete())
	// A delete drops the entry; put it back to model the orphan the sweep would meet.
	batch := rig.kv.NewBatch()
	if err := setExpiryEntry(batch, storage.ExpiryEntry{AtMillis: 1_000, Slot: testSlot, DBID: testDB, BucketID: testBucket, Kind: storage.ExpiryKindDocument, EncID: claim34EncID0()}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	rig.now = 500 // the re-insert is not yet expired when it lands
	sweeper.now = func() int64 { return 2_000 }
	sweeper.afterVerify = func() { rig.apply(expiryInsert(1_000)) }
	result, err := sweeper.Pass(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Dropped != 0 {
		t.Fatalf("pass = %+v: the entry of a live document was dropped, so it would never expire", result)
	}
	if got := expiryTimes(expiryQueue(t, rig.kv)); len(got) != 1 || got[0] != 1_000 {
		t.Fatalf("queue = %v, want the re-inserted document's entry [1000]", got)
	}
}

// TestAStaleEntryNeverTakesALiveElementAtTheSamePosition: positions can be reused (a lifo moves its
// tail back), so an entry can name a position that now holds a different, live element. The
// sweep must compare the element's datetime to the entry's and drop the entry, taking nothing.
func TestAStaleEntryNeverTakesALiveElementAtTheSamePosition(t *testing.T) {
	rig, remover, sweeper := newSweepRig(t)
	rig.apply(expiryInsert(0))
	rig.apply(expiryPush(t, expiryElement(t, 90_000, "live")))
	var position uint64
	for _, entry := range expiryQueue(t, rig.kv) {
		position = entry.Position
	}
	batch := rig.kv.NewBatch()
	if err := setExpiryEntry(batch, storage.ExpiryEntry{AtMillis: 1_000, Slot: testSlot, DBID: testDB, BucketID: testBucket, Kind: storage.ExpiryKindElement, EncID: claim34EncID0(), FieldID: expiryQueueField, Position: position}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	rig.now = 2_000
	result, err := sweeper.Pass(context.Background(), 10)
	if err != nil || result.Dropped != 1 || remover.calls != 0 {
		t.Fatalf("pass = %+v, %v, %d removals; the stale entry is dropped and nothing is taken", result, err, remover.calls)
	}
	if text := claim34Text(t, rig.apply(expiryPop(t, 0))); text != "live" {
		t.Fatalf("the queue's head is %q, want the live element to have survived", text)
	}
}

// TestTheSystemTakeRefusesAnElementWhoseTimeIsNotTheEntrys: the record names the datetime the
// entry promised, and the precondition and the apply both hold the element to it.
func TestTheSystemTakeRefusesAnElementWhoseTimeIsNotTheEntrys(t *testing.T) {
	rig, _, _ := newSweepRig(t)
	rig.apply(expiryInsert(0))
	rig.apply(expiryPush(t, expiryElement(t, 90_000, "live")))
	var position uint64
	for _, entry := range expiryQueue(t, rig.kv) {
		position = entry.Position
	}
	take := func(at int64) cluster.Op {
		return cluster.Op{
			Class: cluster.OpStoreVerb, BucketID: testBucket, EncID: claim34EncID0(), System: true,
			Fields: []cluster.FieldOp{{FieldID: expiryQueueField, Verb: cluster.VerbTake, Value: takeArgAt(position, at)}},
		}
	}
	rig.now = 2_000
	if code, err := rig.applier.Precondition(testSlot, testDB, take(1_000)); err != nil || code != cluster.CodeNotFound {
		t.Fatalf("take naming 1000 over an element at 90000: precondition = (%v, %v), want NOT_FOUND", code, err)
	}
	rig.now = 100_000 // the element is past its time at this attempt's instant
	if code, err := rig.applier.Precondition(testSlot, testDB, take(90_000)); err != nil || code != 0 {
		t.Fatalf("take naming the element's own time: precondition = (%v, %v), want accepted", code, err)
	}
}

// TestALifoElementReturnedToItsPositionIsTheOnlyOneTheSweepTakes is the lifo reuse case from the
// review, run end to end: A is pushed with an expiry, claimed and released back to its origin
// position, and B is pushed after it. A field store's push takes the next fresh position (only
// the lifo's tail cursor moves back), so B can never sit at A's position and a stale entry cannot
// take it; the sweep removes A, B stays, and the owner, a copy and a replay of the logged writes
// hold the same queue.
func TestALifoElementReturnedToItsPositionIsTheOnlyOneTheSweepTakes(t *testing.T) {
	owner, copied, replayed := newExpiryRigOf(t, store.FieldLIFO), newExpiryRigOf(t, store.FieldLIFO), newExpiryRigOf(t, store.FieldLIFO)
	owner.applier.WithWallClock(func() int64 { return owner.now })
	remover := &rigRemover{rig: owner}
	sweeper := NewExpirySweeper(owner.kv, remover, func() int64 { return owner.now }).
		WithExpiryTable(func(storage.ExpiryEntry) (cluster.AdmittedExpire, bool) { return *owner.declared, true })
	history := func(rig *expiryRig) {
		rig.apply(expiryInsert(0))
		rig.apply(expiryPush(t, expiryElement(t, 1_000, "A")))
		claim := claim34Op(cluster.OpClaim)
		claim.Count = 1
		claim = StampClaim(claim, 100, [16]byte{4})
		claim.Expire = []cluster.ExpireItem{{FieldID: expiryQueueField, Path: []byte(expiryPath)}}
		leaseID := rig.apply(claim)
		release := claim34Lease(cluster.OpRelease, [16]byte(leaseID[:16]), 200)
		rig.apply(release)
		rig.apply(expiryPush(t, expiryElement(t, 90_000, "B")))
	}
	for _, rig := range []*expiryRig{owner, copied, replayed} {
		history(rig)
	}
	positions := map[uint64]string{}
	for _, entry := range expiryQueue(t, owner.kv) {
		positions[entry.Position] = fmt.Sprint(entry.AtMillis)
	}
	if len(positions) != 2 {
		t.Fatalf("A and B share a position: %v", positions)
	}
	owner.now = 2_000
	result, err := sweeper.Pass(context.Background(), 10)
	if err != nil || result.Removed != 1 {
		t.Fatalf("pass = %+v, %v, want exactly A removed", result, err)
	}
	for _, rig := range []*expiryRig{copied, replayed} {
		rig.now = 2_000
		for _, op := range remover.issued {
			rig.apply(op)
		}
	}
	want := expiryQueue(t, owner.kv)
	if len(want) != 1 || want[0].AtMillis != 90_000 {
		t.Fatalf("owner queue = %+v, want only B's entry", want)
	}
	for name, rig := range map[string]*expiryRig{"owner": owner, "copy": copied, "replay": replayed} {
		if !reflect.DeepEqual(want, expiryQueue(t, rig.kv)) {
			t.Fatalf("%s holds %+v, want %+v", name, expiryQueue(t, rig.kv), want)
		}
		if text := claim34Text(t, rig.apply(expiryPop(t, 0))); text != "B" {
			t.Fatalf("%s: the queue's element is %q, want B to have survived", name, text)
		}
	}
}

// countingKV counts the iterator work a sweep pass does on the snapshots it opens.
type countingKV struct {
	storage.KV
	seeks, nexts, visits int64
}

type countingSnapshot struct {
	storage.KVSnapshot
	owner *countingKV
}

type countingIterator struct {
	storage.KVIterator
	owner *countingKV
}

func (kv *countingKV) NewSnapshot() storage.KVSnapshot {
	return countingSnapshot{KVSnapshot: kv.KV.NewSnapshot(), owner: kv}
}

func (s countingSnapshot) NewIterator(opts storage.IterOptions) storage.KVIterator {
	return countingIterator{KVIterator: s.KVSnapshot.NewIterator(opts), owner: s.owner}
}

func (it countingIterator) Seek(key []byte) { it.owner.seeks++; it.KVIterator.Seek(key) }
func (it countingIterator) Next()           { it.owner.nexts++; it.KVIterator.Next() }
func (it countingIterator) Close() {
	it.owner.visits += it.Visits()
	it.KVIterator.Close()
}

// TestTickCostIsOneSeekWithNoExpiredEntriesAtAHundredThousandBuckets: the pass is one structure per
// node whatever the bucket count. With 100 000 expiring documents in 100 000 buckets, none due,
// a pass opens one iterator, seeks once, steps never, and touches a handful of stored versions.
func TestTickCostIsOneSeekWithNoExpiredEntriesAtAHundredThousandBuckets(t *testing.T) {
	kv := claim34New(t)
	const buckets = 100_000
	for start := 0; start < buckets; start += 5_000 {
		batch := kv.NewBatch()
		for bucket := start; bucket < start+5_000; bucket++ {
			entry := storage.ExpiryEntry{AtMillis: 9_000_000_000_000 + int64(bucket), Slot: testSlot, DBID: testDB, BucketID: uint64(bucket + 1), Kind: storage.ExpiryKindDocument, EncID: claim34EncID0()}
			if err := setExpiryEntry(batch, entry); err != nil {
				t.Fatal(err)
			}
		}
		if err := batch.Commit(false); err != nil {
			t.Fatal(err)
		}
	}
	counting := &countingKV{KV: kv}
	remover := &rigRemover{rig: nil}
	sweeper := NewExpirySweeper(counting, remover, func() int64 { return 1_000 })
	result, err := sweeper.Pass(context.Background(), 1_000)
	if err != nil || result.Visited != 0 || remover.calls != 0 {
		t.Fatalf("pass = %+v, %v, %d removals: nothing is due", result, err, remover.calls)
	}
	if counting.seeks != 1 || counting.nexts != 0 || counting.visits > 4 {
		t.Fatalf("the pass did %d seeks, %d steps and touched %d versions; it must be one seek and no walk, whatever the bucket count", counting.seeks, counting.nexts, counting.visits)
	}
}

// TestIdleExpiringBucketsAddNoResidentBytes is the ring's form of the idle-bucket gate. It is a
// STRUCTURAL gate only: it measures no heap and counts no GC-traced object (the server's
// TestIdleBucketsAddNoGCTracedObjects does, for the old engine), and a ring-engine measurement at 10^6
// buckets is still to be written. It pins what would break it: no map in the sweeper, and a declaration
// that grows by a few stored bytes. (Pending also grows by one pointer per cached declaration.) The ring
// keeps nothing per bucket in memory for expiry: a bucket's expiry declaration is a few stored
// bytes in its declaration (field 11), and the sweeper owns one cursor, no map or slice per
// bucket, per slot or per pair. The old server's GC-traced-object test measures a different engine.
func TestIdleExpiringBucketsAddNoResidentBytes(t *testing.T) {
	sweeperType := reflect.TypeOf(ExpirySweeper{})
	for i := range sweeperType.NumField() {
		field := sweeperType.Field(i)
		if field.Type.Kind() == reflect.Map {
			t.Errorf("ExpirySweeper.%s is a map: nothing is held per bucket or per pair", field.Name)
		}
	}
	base := machine.Declaration{BucketID: 1, State: machine.StateActive, Fields: machine.SystemDeclaration().Fields}
	withExpiry := base
	withExpiry.Expire = machine.ExpireTable{BucketField: 2}
	plain, err := store.EncodeDeclaration(base)
	if err != nil {
		t.Fatal(err)
	}
	expiring, err := store.EncodeDeclaration(withExpiry)
	if err != nil {
		t.Fatal(err)
	}
	size := func(stored map[uint16][]byte) int {
		total := 0
		for _, value := range stored {
			total += len(value)
		}
		return total
	}
	if delta := size(expiring) - size(plain); delta > 16 {
		t.Fatalf("an expiring bucket's declaration is %d bytes larger, the spec prices it at a few bytes", delta)
	}
}

func TestTheSweepCountsWhatItDoesWithoutALabel(t *testing.T) {
	rig, _, sweeper := newSweepRig(t)
	rig.apply(expiryInsert(1_000))
	rig.now = 500
	if counts, err := sweeper.Counts(100); err != nil || counts.Backlog != 0 {
		t.Fatalf("counts before the time = %+v, %v, want an empty backlog", counts, err)
	}
	rig.now = 2_000
	if counts, err := sweeper.Counts(100); err != nil || counts.Backlog != 1 {
		t.Fatalf("counts when due = %+v, %v, want a backlog of 1", counts, err)
	}
	if _, err := sweeper.Pass(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	counts, err := sweeper.Counts(100)
	if err != nil || counts.RemovedDocuments != 1 || counts.Backlog != 0 {
		t.Fatalf("counts after the sweep = %+v, %v, want one document removed and no backlog", counts, err)
	}
}

// TestABackwardClockStepNeverTakesAnElementThatReadsLive: the clock steps back between the sweep
// choosing an entry and the take being ordered; an element that reads as live at the owner's clock
// is not removed. The take's precondition and its apply both hold it to at <= the attempt's instant.
func TestABackwardClockStepNeverTakesAnElementThatReadsLive(t *testing.T) {
	rig, remover, sweeper := newSweepRig(t)
	rig.apply(expiryInsert(0))
	rig.apply(expiryPush(t, expiryElement(t, 1_000, "A")))
	rig.now = 1_000
	remover.before = func() { rig.now = 500 }
	if _, err := sweeper.Pass(context.Background(), 10); err == nil {
		t.Log("the pass reported no error")
	}
	snap := rig.kv.NewSnapshot()
	defer snap.Discard()
	ref := rig.applier.fieldRef(testSlot, testDB, testBucket, claim34EncID0(), expiryQueueField, store.FieldFIFO)
	if count, err := store.FieldLen(snap, ref); err != nil || count != 1 {
		t.Fatalf("the element was taken (%d left, %v) while the owner's clock reads 500 and it expires at 1000", count, err)
	}
	// And the apply alone, with a record that names a take the precondition would have refused.
	var position uint64
	for _, entry := range expiryQueue(t, rig.kv) {
		position = entry.Position
	}
	take := cluster.Op{Class: cluster.OpStoreVerb, BucketID: testBucket, EncID: claim34EncID0(), System: true,
		Fields: []cluster.FieldOp{{FieldID: expiryQueueField, Verb: cluster.VerbTake, Value: takeArgAt(position, 1_000)}},
		Expire: []cluster.ExpireItem{{FieldID: expiryQueueField, Path: []byte(expiryPath)}}}
	rig.now = 500
	rig.apply(take)
	snap2 := rig.kv.NewSnapshot()
	defer snap2.Discard()
	if count, err := store.FieldLen(snap2, ref); err != nil || count != 1 {
		t.Fatalf("the apply took an element that is not expired at the record's instant (%d left, %v)", count, err)
	}
}

// TestAnOrphanDropDoesNotResurrectAPairTheNodeNeverHeld: the entry of a pair the node gave up is an
// orphan; dropping it must not take the pair lock through a call that creates the pair, or the node
// lists a slot it does not hold and the retire and dedupe rounds visit it. The same sweeper wired with
// a plain lock that creates the pair fails this test, which is what pins the wiring.
func TestAnOrphanDropDoesNotResurrectAPairTheNodeNeverHeld(t *testing.T) {
	kv := claim34New(t)
	var pairs cluster.PairSet
	batch := kv.NewBatch()
	if err := setExpiryEntry(batch, storage.ExpiryEntry{AtMillis: 10, Slot: 77, DBID: 99, BucketID: 1, Kind: storage.ExpiryKindDocument, EncID: claim34EncID0()}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	sweeper := NewExpirySweeper(kv, &rigRemover{}, func() int64 { return 1_000 }).WithPairLock(heldPairLock(&pairs))
	result, err := sweeper.Pass(context.Background(), 10)
	if err != nil || result.Dropped != 1 {
		t.Fatalf("pass = %+v, %v, want the orphan dropped", result, err)
	}
	if held := pairs.HeldSlots(99); len(held) != 0 {
		t.Fatalf("the drop left this node holding slots %v of a database it never held", held)
	}
	// A pair the node does hold still takes the lock: the re-verify runs under it.
	pairs.With(5, 98, func(*cluster.Pair) {})
	locked := false
	lock := heldPairLock(&pairs)
	if !lock(5, 98, func() { locked = true }) || !locked {
		t.Fatal("a held pair did not run the function under its lock")
	}
}

// TestDropExpireDrainsTheQueueWithoutRemovingAnything: after `drop expire at` the entries of the bucket's
// documents are dropped, not removed and not retried: the sweep reads the declaration, finds no expiry,
// and deletes only the entries (spec 2.1 and 3.3 step 3).
func TestDropExpireDrainsTheQueueWithoutRemovingAnything(t *testing.T) {
	rig, remover, sweeper := newSweepRig(t)
	rig.apply(expiryInsert(1_000))
	rig.declared = &cluster.AdmittedExpire{} // drop expire at published
	rig.now = 2_000
	for range 3 {
		if _, err := sweeper.Pass(context.Background(), 10); err != nil {
			t.Fatalf("a pass after drop expire errored: %v", err)
		}
	}
	if remover.calls != 0 {
		t.Fatalf("%d removals were issued after drop expire: the sweep must read the declaration first", remover.calls)
	}
	if headerState(t, rig) != HeaderLive {
		t.Fatal("the document was removed after drop expire")
	}
	if left := expiryQueue(t, rig.kv); len(left) != 0 {
		t.Fatalf("entries %+v remain after drop expire", left)
	}
}

// TestAQueueElementEntryIsDrainedWhenTheStoreNoLongerExpires: the same for the store form, whose entry
// would otherwise wait forever (the declaration names no path for it).
func TestAQueueElementEntryIsDrainedWhenTheStoreNoLongerExpires(t *testing.T) {
	rig, remover, sweeper := newSweepRig(t)
	rig.apply(expiryInsert(0))
	rig.apply(expiryPush(t, expiryElement(t, 1_000, "A")))
	rig.declared = &cluster.AdmittedExpire{BucketField: expiryDocField}
	rig.now = 2_000
	if _, err := sweeper.Pass(context.Background(), 10); err != nil || remover.calls != 0 {
		t.Fatalf("pass err %v, %d removals", err, remover.calls)
	}
	if left := expiryQueue(t, rig.kv); len(left) != 0 {
		t.Fatalf("the element's entry %+v is not drained", left)
	}
}

// TestADocumentWhoseFieldMovedIsDroppedNotRemoved: an entry whose document field is absent or not the
// entry's time (set expire at another field) is stale: dropped after the re-verify, never removed.
func TestADocumentWhoseFieldIsNotTheEntrysTimeIsDroppedNotRemoved(t *testing.T) {
	rig, remover, sweeper := newSweepRig(t)
	rig.apply(expiryInsert(1_000))
	rig.apply(expiryUpdate(cluster.UpdateUnset, 0)) // the update drops the entry; put a stale one back
	batch := rig.kv.NewBatch()
	if err := setExpiryEntry(batch, storage.ExpiryEntry{AtMillis: 1_000, Slot: testSlot, DBID: testDB, BucketID: testBucket, Kind: storage.ExpiryKindDocument, EncID: claim34EncID0()}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	rig.now = 2_000
	result, err := sweeper.Pass(context.Background(), 10)
	if err != nil || result.Dropped != 1 || remover.calls != 0 {
		t.Fatalf("pass = %+v, %v, %d removals; a stale document entry is dropped without a removal", result, err, remover.calls)
	}
}

// TestAPassIsBoundedByWallTimeAsWellAsByKeys: each removal is a replicated write that may wait on a slow
// copy, and the pass shares a goroutine with the dedupe round and the reclaim records. A pass that has
// spent its wall budget stops, leaves the rest for the next tick, and resumes after the last entry.
func TestAPassIsBoundedByWallTimeAsWellAsByKeys(t *testing.T) {
	rig, remover, sweeper := newSweepRig(t)
	rig.apply(expiryInsert(0))
	for i := range 10 {
		rig.apply(expiryPush(t, expiryElement(t, int64(1_000+i), "e")))
	}
	rig.now = 2_000
	clock := time.Unix(0, 0)
	sweeper.WithWallBudget(250*time.Millisecond, func() time.Time { return clock })
	remover.before = func() { clock = clock.Add(100 * time.Millisecond) } // a slow write
	first, err := sweeper.Pass(context.Background(), 1_000)
	if err != nil {
		t.Fatal(err)
	}
	if first.Visited < 1 || first.Visited > 4 {
		t.Fatalf("a pass of 1000 keys with a 250 ms budget over 100 ms writes visited %d entries, want 3 or so", first.Visited)
	}
	remover.before = nil
	second, err := sweeper.Pass(context.Background(), 1_000)
	if err != nil || first.Visited+second.Visited != 10 {
		t.Fatalf("the next pass visited %d after %d, want all 10 between them (%v)", second.Visited, first.Visited, err)
	}
}

// TestAnAnswerThatFailedToApplyIsNotARemoval: a write whose apply failed after the append is not a
// removal; the counter must not claim it.
func TestAnAnswerThatFailedToApplyIsNotARemoval(t *testing.T) {
	if skipped, err := systemAnswerOutcome(WriteAnswer{Held: true}); !skipped || err != nil {
		t.Fatalf("a held write = %v, %v, want skipped", skipped, err)
	}
	if skipped, err := systemAnswerOutcome(WriteAnswer{Forward: true}); !skipped || err != nil {
		t.Fatalf("a forwarded write = %v, %v, want skipped", skipped, err)
	}
	if _, err := systemAnswerOutcome(WriteAnswer{ApplyFailure: errors.New("the batch failed after the append")}); err == nil {
		t.Fatal("an answer with an apply failure was counted as a removal")
	}
	if skipped, err := systemAnswerOutcome(WriteAnswer{}); skipped || err != nil {
		t.Fatalf("an applied write = %v, %v, want a removal", skipped, err)
	}
}

// TestTheApplyOfASystemTakeLeavesAnElementThatIsNotTheEntrys: the datetime check also runs in the apply,
// from the record and the stored element alone, so a copy or a replay that applies a take whose entry
// no longer matches the element leaves the element where it is.
func TestTheApplyOfASystemTakeLeavesAnElementThatIsNotTheEntrys(t *testing.T) {
	rig := newExpiryRig(t)
	rig.apply(expiryInsert(0))
	rig.apply(expiryPush(t, expiryElement(t, 90_000, "live")))
	var position uint64
	for _, entry := range expiryQueue(t, rig.kv) {
		position = entry.Position
	}
	rig.now = 200_000
	take := cluster.Op{Class: cluster.OpStoreVerb, BucketID: testBucket, EncID: claim34EncID0(), System: true,
		Fields: []cluster.FieldOp{{FieldID: expiryQueueField, Verb: cluster.VerbTake, Value: takeArgAt(position, 1_000)}},
		Expire: []cluster.ExpireItem{{FieldID: expiryQueueField, Path: []byte(expiryPath)}}}
	rig.apply(take) // names 1000; the element carries 90000
	snap := rig.kv.NewSnapshot()
	defer snap.Discard()
	ref := rig.applier.fieldRef(testSlot, testDB, testBucket, claim34EncID0(), expiryQueueField, store.FieldFIFO)
	if count, err := store.FieldLen(snap, ref); err != nil || count != 1 {
		t.Fatalf("the apply took an element that is not the entry's (%d left, %v)", count, err)
	}
	rig.apply(cluster.Op{Class: cluster.OpStoreVerb, BucketID: testBucket, EncID: claim34EncID0(), System: true,
		Fields: []cluster.FieldOp{{FieldID: expiryQueueField, Verb: cluster.VerbTake, Value: takeArgAt(position, 90_000)}},
		Expire: []cluster.ExpireItem{{FieldID: expiryQueueField, Path: []byte(expiryPath)}}})
	snap2 := rig.kv.NewSnapshot()
	defer snap2.Discard()
	if count, err := store.FieldLen(snap2, ref); err != nil || count != 0 {
		t.Fatalf("the apply did not take the element its entry names (%d left, %v)", count, err)
	}
}

// TestANodeDropsAnOrphanEntryWithoutHoldingItsPair is the Node-level form of the orphan-drop test: the
// sweeper the node builds (its own wiring of the pair lock, not the helper alone) drops the entry of a
// pair the node never held and the node still holds no slot of that database afterwards.
func TestANodeDropsAnOrphanEntryWithoutHoldingItsPair(t *testing.T) {
	node, _ := u30iFresh(t)
	const orphanDB = 9_999
	batch := node.kv.NewBatch()
	if err := setExpiryEntry(batch, storage.ExpiryEntry{AtMillis: 10, Slot: 77, DBID: orphanDB, BucketID: 1, Kind: storage.ExpiryKindDocument, EncID: claim34EncID0()}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	node.expiry.now = func() int64 { return 1_000 }
	result, err := node.expiry.Pass(context.Background(), 10)
	if err != nil || result.Dropped != 1 {
		t.Fatalf("pass = %+v, %v, want the orphan dropped", result, err)
	}
	if held := node.pairs.HeldSlots(orphanDB); len(held) != 0 {
		t.Fatalf("the node's sweep left it holding slots %v of a database it never held", held)
	}
}
