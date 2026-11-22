package wire

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// copySnapshotHalf copies every key of from except the node-local expiry queue into to, the way a
// seed's snapshot half copies a pair's raw keys.
func copySnapshotHalf(t *testing.T, from, to storage.KV) {
	t.Helper()
	snap := from.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{})
	defer it.Close()
	expiry := storage.ExpiryPrefix()
	batch := to.NewBatch()
	for it.Seek(nil); it.Valid(); it.Next() {
		key := it.Key()
		if len(key) >= len(expiry) && string(key[:len(expiry)]) == string(expiry) {
			continue
		}
		stored, err := it.Value()
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Set(append([]byte(nil), key...), append([]byte(nil), stored...)); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
}

// rebuildSweepFor is a retention sweep over kv whose only declared bucket is the rig's, expiring
// as expiryTable says.
func rebuildSweepFor(t *testing.T, kv storage.KV) *RetireSweep {
	t.Helper()
	decls := &u43Decls{
		decls: map[string]machine.Declaration{"Session": {BucketID: testBucket, State: machine.StateActive, Expire: expiryTable()}},
		names: map[uint64]string{testBucket: "Session"},
	}
	sweep := NewRetireSweep()
	sweep.Start(context.Background(), RetireDeps{
		KV: kv, Pairs: &cluster.PairSet{}, Declarations: decls, BucketName: decls.bucketName, Manual: true,
		Published: func(uint64) int64 { return 5 }, KeysPerSecond: 1_000_000, StepKeys: 1000,
		Sleep: func(context.Context, time.Duration) error { return nil },
	})
	t.Cleanup(sweep.Close)
	return sweep
}

// seededOwner is an owner holding one expiring document and one expiring queue element.
func seededOwner(t *testing.T) *expiryRig {
	owner := newSweepRigOnly(t)
	owner.apply(expiryInsert(1_000))
	owner.apply(expiryPush(t, expiryElement(t, 2_000, "a")))
	return owner
}

func newSweepRigOnly(t *testing.T) *expiryRig {
	rig := newExpiryRig(t)
	rig.applier.WithWallClock(func() int64 { return rig.now })
	return rig
}

// TestASeededCopyBuildsItsQueueFromTheDrain: a copy seeded after the inserts holds the raw keys and
// no expiry entry; rebuilding from the stored data gives it the owner's queue, and a write drained
// afterwards keeps the two equal.
func TestASeededCopyBuildsItsQueueFromTheDrain(t *testing.T) {
	owner := seededOwner(t)
	copied := newSweepRigOnly(t)
	copySnapshotHalf(t, owner.kv, copied.kv)
	if got := expiryQueue(t, copied.kv); len(got) != 0 {
		t.Fatalf("the snapshot half carried %d expiry entries, want none", len(got))
	}
	if err := rebuildSweepFor(t, copied.kv).RebuildExpiry(context.Background(), testSlot, testDB); err != nil {
		t.Fatal(err)
	}
	if got, want := expiryQueue(t, copied.kv), expiryQueue(t, owner.kv); len(want) != 2 || !reflect.DeepEqual(got, want) {
		t.Fatalf("the seeded copy's queue is %+v, want the owner's %+v", got, want)
	}
	push := expiryPush(t, expiryElement(t, 3_000, "b"))
	owner.apply(push)
	copied.apply(push)
	if got, want := expiryQueue(t, copied.kv), expiryQueue(t, owner.kv); len(want) != 3 || !reflect.DeepEqual(got, want) {
		t.Fatalf("after a drained push the copy's queue is %+v, want %+v", got, want)
	}
}

// TestARebuildSkipsATombstoneAndAnElementOfADeletedDocument: a deleted document's header is the
// tombstone, and neither it nor its elements get an entry.
func TestARebuildSkipsATombstoneAndAnElementOfADeletedDocument(t *testing.T) {
	owner := seededOwner(t)
	owner.apply(expiryDelete())
	copied := newSweepRigOnly(t)
	copySnapshotHalf(t, owner.kv, copied.kv)
	if err := rebuildSweepFor(t, copied.kv).RebuildExpiry(context.Background(), testSlot, testDB); err != nil {
		t.Fatal(err)
	}
	if got := expiryQueue(t, copied.kv); len(got) != 0 {
		t.Fatalf("a deleted document left %+v in the rebuilt queue", got)
	}
}

// TestARebuildThatCannotReadTheDeclarationFailsSoThePullRetries: the seed must not become ready
// with an incomplete queue because the declaration was unreadable at that moment.
func TestARebuildThatCannotReadTheDeclarationFailsSoThePullRetries(t *testing.T) {
	owner := seededOwner(t)
	decls := &u43Decls{err: errors.New("no owner answered"), names: map[uint64]string{testBucket: "Session"}}
	sweep := NewRetireSweep()
	sweep.Start(context.Background(), RetireDeps{KV: owner.kv, Pairs: &cluster.PairSet{}, Declarations: decls, BucketName: decls.bucketName, Manual: true, Published: func(uint64) int64 { return 5 }})
	t.Cleanup(sweep.Close)
	if err := sweep.RebuildExpiry(context.Background(), testSlot, testDB); err == nil {
		t.Fatal("an unreadable declaration was taken for a bucket that does not expire")
	}
}

// TestAPromotedCopySweepsWhatTheOldOwnerLeft: the failover case. The copy was seeded after the
// document and the element expired; once it is the owner, its sweep removes both.
func TestAPromotedCopySweepsWhatTheOldOwnerLeft(t *testing.T) {
	owner := seededOwner(t)
	rig, remover, sweeper := newSweepRig(t)
	copySnapshotHalf(t, owner.kv, rig.kv)
	if err := rebuildSweepFor(t, rig.kv).RebuildExpiry(context.Background(), testSlot, testDB); err != nil {
		t.Fatal(err)
	}
	rig.now = 10_000
	if _, err := sweeper.Pass(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	// The document goes first, so the element's entry is dropped as that of a deleted document.
	if remover.calls != 1 {
		t.Fatalf("the promoted copy issued %d removals, want 1 (the document)", remover.calls)
	}
	if state := headerState(t, rig); state != HeaderTombstone {
		t.Fatalf("the expired document is %v on the promoted copy, want a tombstone", state)
	}
	if left := expiryQueue(t, rig.kv); len(left) != 0 {
		t.Fatalf("the promoted copy's queue still holds %+v", left)
	}
}

// TestASeedDerivesTheExpiryQueueBeforeThePairIsReady: finishSeed runs the derive hook once, while the
// pair is still not ready, and a hook that fails keeps it from becoming ready.
func TestASeedDerivesTheExpiryQueueBeforeThePairIsReady(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner = "x"
	rig.sources = map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 10, t3Cut(t, rig), 42)
	calls, readyAtCall, failures := 0, int64(-1), 1
	rig.puller.cfg.DeriveExpiry = func(context.Context, uint16, uint64) error {
		calls++
		readyAtCall = rig.ready.Load()
		if failures > 0 {
			failures--
			return errors.New("the declaration could not be read")
		}
		return nil
	}
	rig.pullUntil("the pair to be ready", rig.isReady)
	if calls != 2 || readyAtCall != 0 {
		t.Fatalf("the hook ran %d times (want 2: one failure, one retry), with %d pairs ready at its last call (want 0)", calls, readyAtCall)
	}
	if rig.ready.Load() != 1 {
		t.Fatalf("OnReady ran %d times, want 1", rig.ready.Load())
	}
}

// TestTheNodeWiresTheExpiryRebuildIntoItsPuller: the node's puller derives the expiry queue through
// the retention sweep, which reads the buckets the pair holds. [BREAK: leave DeriveExpiry unset]
func TestTheNodeWiresTheExpiryRebuildIntoItsPuller(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	dbID := s0Schema(t, node)
	s0AwaitFirstPass(t, node, dbID)
	s0Insert(t, node, "u1")
	slot := s0SlotOf(t, "u1")

	var nameReads atomic.Int32
	counting := NewRetireSweep()
	counting.Start(context.Background(), RetireDeps{
		KV: node.kv, Pairs: node.pairs, Manual: true, Declarations: node.fetcher,
		BucketName: func(context.Context, uint64, uint64) (string, bool, error) {
			nameReads.Add(1)
			return "", false, nil
		},
	})
	original := node.retire
	node.retire = counting
	t.Cleanup(func() {
		counting.Close()
		node.retire = original
	})
	derive := node.cl.seeding.Load().puller.cfg.DeriveExpiry
	if derive == nil {
		t.Fatal("the puller has no DeriveExpiry")
	}
	if err := derive(context.Background(), slot, dbID); err != nil {
		t.Fatal(err)
	}
	if nameReads.Load() == 0 {
		t.Fatal("the derive hook did not read the buckets of the pair")
	}
}

// expiryTickRig is a retention sweep over an owner whose queue holds one expired document and three
// expired elements, with a budget so small that charging the pass must overdraw it.
type expiryTickRig struct {
	rig     *expiryRig
	sweeper *ExpirySweeper
	sweep   *RetireSweep
	sleeps  atomic.Int32
	errs    chan error
}

func newExpiryTickRig(t *testing.T, tick <-chan time.Time, sleepErr error) *expiryTickRig {
	t.Helper()
	rig, _, sweeper := newSweepRig(t)
	rig.apply(expiryInsert(1_000))
	for _, label := range []string{"a", "b", "c"} {
		rig.apply(expiryPush(t, expiryElement(t, 2_000, label)))
	}
	rig.now = 10_000
	tickRig := &expiryTickRig{rig: rig, sweeper: sweeper, sweep: NewRetireSweep(), errs: make(chan error, 4)}
	tickRig.sweep.Start(context.Background(), RetireDeps{
		KV: rig.kv, Pairs: &cluster.PairSet{}, Expiry: sweeper, Tick: tick, TickInterval: time.Minute,
		Manual: tick == nil, KeysPerSecond: 1, StepKeys: 4,
		Sleep: func(context.Context, time.Duration) error {
			tickRig.sleeps.Add(1)
			return sleepErr
		},
		OnError: func(err error) { tickRig.errs <- err },
	})
	t.Cleanup(tickRig.sweep.Close)
	return tickRig
}

// TestTheSweepRunsOnTheRetentionTickAndIsChargedToTheBudget: a tick runs one expiry pass, and the
// keys it walked are spent from the reclaim budget the dedupe sweep shares. [BREAK: charge nothing,
// or run no pass on the tick]
func TestTheSweepRunsOnTheRetentionTickAndIsChargedToTheBudget(t *testing.T) {
	tick := make(chan time.Time)
	tickRig := newExpiryTickRig(t, tick, nil)
	tick <- time.Now()
	deadline := time.Now().Add(10 * time.Second)
	for tickRig.sleeps.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if tickRig.sweeper.removedDocuments.Load() != 1 {
		t.Fatalf("the tick removed %d documents, want 1: the pass did not run", tickRig.sweeper.removedDocuments.Load())
	}
	if tickRig.sleeps.Load() == 0 {
		t.Fatal("the pass walked 7 keys against a budget of 4 and waited for none of them: it spent nothing from the reclaim budget")
	}
}

// TestAnExpiryRoundWhoseBudgetWaitEndsEarlyReportsItOnce: the error is handed to OnError, once, and
// the round ends; it is not swallowed.
func TestAnExpiryRoundWhoseBudgetWaitEndsEarlyReportsItOnce(t *testing.T) {
	tickRig := newExpiryTickRig(t, nil, errors.New("the node is closing"))
	tickRig.sweep.expiryRound(context.Background())
	select {
	case err := <-tickRig.errs:
		if !strings.Contains(err.Error(), "reclaim budget") || !strings.Contains(err.Error(), "the node is closing") {
			t.Fatalf("the report %q names neither the budget nor the cause", err)
		}
	default:
		t.Fatal("a budget wait that ended early was swallowed")
	}
	if len(tickRig.errs) != 0 {
		t.Fatalf("%d further reports in one round, want exactly one", len(tickRig.errs))
	}
}

// TestAnUnreadableExpiryQueueLeavesTheBacklogUnknown: the gauge is marked unknown instead of
// reporting 0, which would tell an operator the sweep is caught up.
func TestAnUnreadableExpiryQueueLeavesTheBacklogUnknown(t *testing.T) {
	rig, _, sweeper := newSweepRig(t)
	batch := rig.kv.NewBatch()
	if err := batch.Set(append(storage.ExpiryPrefix(), 0x01), []byte{}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	node := &Node{expiry: sweeper}
	counts := node.ExpiryCounts()
	if !counts.BacklogUnknown || counts.Backlog != 0 {
		t.Fatalf("an unreadable queue gave %+v, want the backlog marked unknown", counts)
	}
}

// dedupeKeys counts the idempotency entries a node holds.
func dedupeKeys(t *testing.T, kv storage.KV) int {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{})
	defer it.Close()
	count := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		key := it.Key()
		if len(key) > 18 && (key[0] != 0xFF || key[1] != 0xFF) && key[18] == storage.NSDedupe {
			count++
		}
	}
	return count
}

// TestTheSystemRemovalWritesNoDedupeEntry: the node's removal of an expired document is a write nobody
// can retry, so it leaves no idempotency entry. [BREAK: give the system write a request id]
func TestTheSystemRemovalWritesNoDedupeEntry(t *testing.T) {
	r := newU40Rig(t)
	r.run(u40Add("ExpiresAt", "datetime"))
	r.run(AlterChange{SetExpire: "ExpiresAt"})
	if err := r.insertExpiring("old", time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	entries := expiryQueue(t, r.node.kv)
	if len(entries) != 1 {
		t.Fatalf("the queue holds %d entries, want 1", len(entries))
	}
	before := dedupeKeys(t, r.node.kv)
	if before == 0 {
		t.Fatal("the client insert left no dedupe entry, so this test would pass on anything")
	}
	skipped, err := r.node.RemoveDocument(context.Background(), entries[0])
	if err != nil || skipped {
		t.Fatalf("the system removal = skipped %v, %v, want applied", skipped, err)
	}
	if after := dedupeKeys(t, r.node.kv); after != before {
		t.Fatalf("the system removal left %d dedupe entries, want %d: it must carry no request id", after, before)
	}
	if left := expiryQueue(t, r.node.kv); len(left) != 0 {
		t.Fatalf("the removal left %+v in the queue", left)
	}
}

// TestAWriteOfANonDatetimeIntoTheExpireFieldIsRefused: the expire field is declared datetime, so the
// ordinary field-type check refuses a string or a number written into it, on insert and on update.
// A document never silently becomes permanent because its expire value was the wrong type.
func TestAWriteOfANonDatetimeIntoTheExpireFieldIsRefused(t *testing.T) {
	r := newU40Rig(t)
	r.run(u40Add("ExpiresAt", "datetime"))
	r.run(AlterChange{SetExpire: "ExpiresAt"})
	ctx := context.Background()
	for name, value := range map[string]any{"a string": "tomorrow", "a number": int64(1_900_000_000_000)} {
		doc := append(u30iUser("bad-"+name, "Ada", 36), bson.E{Key: "ExpiresAt", Value: value})
		_, err := r.node.Insert(ctx, u30iDB, "User", doc)
		var refusal *cluster.Error
		if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument || !strings.Contains(refusal.Message, "ExpiresAt") || !strings.Contains(refusal.Message, "never expire") {
			t.Errorf("insert with %s in ExpiresAt: %v, want INVALID_ARGUMENT naming the field and the fix", name, err)
		}
	}
	if err := r.insertExpiring("good", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	_, err := r.node.Update(ctx, UpdateRequest{Database: u30iDB, Bucket: "User", ID: "good", Set: bson.D{{Key: "ExpiresAt", Value: "never"}}})
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument {
		t.Errorf("update setting a string into ExpiresAt: %v, want INVALID_ARGUMENT", err)
	}
	if got := len(expiryQueue(t, r.node.kv)); got != 1 {
		t.Errorf("the queue holds %d entries after the refused writes, want the one good document's", got)
	}
}
