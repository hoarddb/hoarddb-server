package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// claim34* helpers belong to U34. Every test runs on real Badger directories.

const claim34Field uint16 = 9

func claim34Open(t *testing.T, dir string) storage.KV {
	t.Helper()
	kv, err := storage.OpenBadgerKV(dir, storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	return kv
}

func claim34New(t *testing.T) storage.KV {
	t.Helper()
	kv := claim34Open(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	return kv
}

func claim34EncID(t *testing.T) []byte {
	t.Helper()
	encoded, err := store.EncodeComponent("doc")
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func claim34Target(engine store.FieldEngine, seq uint64) LeaseTarget {
	return LeaseTarget{Slot: testSlot, DB: testDB, Engine: engine, Position: cluster.Position{Epoch: 3, Seq: seq}}
}

func claim34Doc(t *testing.T, text string) []byte {
	t.Helper()
	encoded, err := bson.Marshal(map[string]interface{}{"v": text})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func claim34Text(t *testing.T, element []byte) string {
	t.Helper()
	var decoded struct {
		V string `bson:"v"`
	}
	if err := bson.Unmarshal(element, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded.V
}

// claim34Push puts elements into the document's fifo through the field-store write.
func claim34Push(t *testing.T, kv storage.KV, engine store.FieldEngine, texts ...string) {
	t.Helper()
	owner := storage.Key(testSlot, testDB, testBucket, storage.NSFieldStore, claim34EncID(t))
	ref := store.FieldRef{Owner: owner, FieldID: claim34Field, Engine: engine}
	for _, text := range texts {
		snap := kv.NewSnapshot()
		batch := kv.NewBatch()
		var parent store.BucketState
		if _, err := store.FieldPush(store.FieldWrite{Snap: snap, State: &parent, Batch: batch}, ref, claim34Doc(t, text)); err != nil {
			t.Fatal(err)
		}
		if err := batch.Commit(true); err != nil {
			t.Fatal(err)
		}
		snap.Discard()
	}
}

func claim34Op(class byte) cluster.Op {
	return cluster.Op{Class: class, BucketID: testBucket, EncID: claim34EncID0(), Fields: []cluster.FieldOp{{FieldID: claim34Field}}}
}

// claim34EncID0 is the encoding of "doc" without a *testing.T, for table values.
func claim34EncID0() []byte {
	encoded, err := store.EncodeComponent("doc")
	if err != nil {
		return nil
	}
	return encoded
}

// claim34Logged sends op through the retained log's body, as every copy receives it.
func claim34Logged(t *testing.T, op cluster.Op) cluster.Op {
	t.Helper()
	body, err := cluster.EncodeOp(op)
	if err != nil {
		t.Fatalf("encoding the stamped op: %v", err)
	}
	logged, err := cluster.DecodeOp(body)
	if err != nil {
		t.Fatalf("decoding the logged op: %v", err)
	}
	return logged
}

func claim34Claim(count uint16, now int64, nonce byte) cluster.Op {
	op := claim34Op(cluster.OpClaim)
	op.Count = count
	return StampClaim(op, now, [16]byte{nonce, 2, 3})
}

func claim34Lease(class byte, leaseID [16]byte, now int64) cluster.Op {
	op := claim34Op(class)
	op.LeaseID = leaseID
	return StampLease(op, now)
}

func claim34Apply(t *testing.T, kv storage.KV, target LeaseTarget, op cluster.Op) (ClaimOutcome, LeaseOutcome) {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	batch := kv.NewBatch()
	var claimed ClaimOutcome
	var leased LeaseOutcome
	var err error
	if op.Class == cluster.OpClaim {
		claimed, err = ApplyClaim(snap, batch, target, claim34Logged(t, op))
	} else {
		leased, err = ApplyLease(snap, batch, target, claim34Logged(t, op))
	}
	if err != nil {
		t.Fatalf("apply of class 0x%02x: %v", op.Class, err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	return claimed, leased
}

// claim34Rebuild copies every key into a fresh store: the state a new owner
// holds from disk, never a value the test constructed.
func claim34Rebuild(t *testing.T, kv storage.KV) storage.KV {
	t.Helper()
	fresh := claim34New(t)
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{})
	defer it.Close()
	batch := fresh.NewBatch()
	for it.Seek(nil); it.Valid(); it.Next() {
		stored, err := it.Value()
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Set(append([]byte(nil), it.Key()...), stored); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	return fresh
}

func claim34Dump(t *testing.T, kv storage.KV) map[string][]byte {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{})
	defer it.Close()
	dump := map[string][]byte{}
	for it.Seek(nil); it.Valid(); it.Next() {
		stored, err := it.Value()
		if err != nil {
			t.Fatal(err)
		}
		dump[string(it.Key())] = stored
	}
	return dump
}

func claim34Remaining(t *testing.T, kv storage.KV, engine store.FieldEngine) []string {
	t.Helper()
	owner := storage.Key(testSlot, testDB, testBucket, storage.NSFieldStore, claim34EncID(t))
	snap := kv.NewSnapshot()
	defer snap.Discard()
	var left []string
	if err := store.FieldPeekN(snap, store.FieldRef{Owner: owner, FieldID: claim34Field, Engine: engine}, 100, func(entry store.FieldEntry) error {
		left = append(left, claim34Text(t, entry.Value))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return left
}

// claim34WantLease is the lease id conventions section 9.1 fixes:
// [deadline:8 BE][first 8 bytes of the logged nonce].
func claim34WantLease(until int64, nonce [16]byte) [16]byte {
	var want [16]byte
	binary.BigEndian.PutUint64(want[0:8], uint64(until))
	copy(want[8:], nonce[:8])
	return want
}

func TestClaimLeaseSurvivesADrainAndRedeliversOtherwise(t *testing.T) {
	const now = int64(1_000_000)
	owner := claim34New(t)
	claim34Push(t, owner, store.FieldFIFO, "a", "b", "c")
	claimed, _ := claim34Apply(t, owner, claim34Target(store.FieldFIFO, 10), claim34Claim(2, now, 1))
	if want := claim34WantLease(now+store.DefaultLeaseMs, [16]byte{1, 2, 3}); claimed.LeaseID != want {
		t.Fatalf("lease id = %x, want [deadline][logged nonce] = %x", claimed.LeaseID, want)
	}
	if claimed.Until != now+store.DefaultLeaseMs || len(claimed.Elements) != 2 || claim34Text(t, claimed.Elements[0]) != "a" {
		t.Fatalf("claim = %+v, want two messages and a lease ending at %d", claimed, now+store.DefaultLeaseMs)
	}

	// Drained: the new owner is rebuilt from the owner's disk, then acks.
	drained := claim34Rebuild(t, owner)
	_, acked := claim34Apply(t, drained, claim34Target(store.FieldFIFO, 11), claim34Lease(cluster.OpAck, claimed.LeaseID, now+500))
	if acked.State != store.LeaseAcked {
		t.Fatalf("ack on the rebuilt new owner = %s, want acked", acked.State)
	}
	if left := claim34Remaining(t, drained, store.FieldFIFO); len(left) != 1 || left[0] != "c" {
		t.Fatalf("after the ack the field holds %v, want [c]", left)
	}

	// Not drained: the new owner never applied the claim.
	undrained := claim34New(t)
	claim34Push(t, undrained, store.FieldFIFO, "a", "b", "c")
	_, lost := claim34Apply(t, undrained, claim34Target(store.FieldFIFO, 11), claim34Lease(cluster.OpAck, claimed.LeaseID, now+500))
	if lost.State == store.LeaseAcked {
		t.Fatal("an ack of a lease the new owner never saw was acked")
	}
	again, _ := claim34Apply(t, undrained, claim34Target(store.FieldFIFO, 12), claim34Claim(2, now+600, 2))
	if len(again.Elements) != 2 || claim34Text(t, again.Elements[0]) != "a" {
		t.Fatalf("the redelivery handed out %d messages, want a and b again", len(again.Elements))
	}
}

func TestReplayProducesTheSameLease(t *testing.T) {
	logged := claim34Claim(2, 5_000, 7)
	original := claim34New(t)
	claim34Push(t, original, store.FieldFIFO, "a", "b", "c", "d")
	copyStore := claim34Rebuild(t, original) // before the claim: a copy that replays the log
	first, _ := claim34Apply(t, original, claim34Target(store.FieldFIFO, 10), logged)
	// The copy's clock is far ahead of the stamp: nothing it reads locally may reach the lease.
	second, _ := claim34Apply(t, copyStore, claim34Target(store.FieldFIFO, 10), logged)
	if first.Until != 5_000+store.DefaultLeaseMs || second.Until != first.Until {
		t.Fatalf("the deadline is %d and %d, want the stamped now plus the lease length (%d)", first.Until, second.Until, 5_000+store.DefaultLeaseMs)
	}
	if want := claim34WantLease(5_000+store.DefaultLeaseMs, [16]byte{7, 2, 3}); first.LeaseID != want || second.LeaseID != want {
		t.Fatalf("lease ids %x and %x, want [deadline][logged nonce] = %x", first.LeaseID, second.LeaseID, want)
	}
	if first.LeaseID != second.LeaseID || first.Until != second.Until {
		t.Fatalf("replay = (%x, %d), original = (%x, %d)", second.LeaseID, second.Until, first.LeaseID, first.Until)
	}
	if !bytes.Equal(first.Result(), second.Result()) {
		t.Fatal("the dedupe result differs between the owner and the copy")
	}
	a, b := claim34Dump(t, original), claim34Dump(t, copyStore)
	if len(a) != len(b) {
		t.Fatalf("the owner holds %d keys and the replaying copy %d", len(a), len(b))
	}
	for key, stored := range a {
		if !bytes.Equal(b[key], stored) {
			t.Fatalf("key %x differs between the owner and its replaying copy", key)
		}
	}
	other, _ := claim34Apply(t, claim34Rebuild(t, copyStore), claim34Target(store.FieldFIFO, 11), claim34Claim(2, 5_000, 8))
	if other.LeaseID == first.LeaseID {
		t.Fatal("two nonces produced one lease id")
	}
}

func TestAClaimRecordAppliedTwiceLeasesNothingEvenAfterExpiry(t *testing.T) {
	kv := claim34New(t)
	claim34Push(t, kv, store.FieldFIFO, "a", "b", "c")
	record := claim34Claim(1, 5_000, 1)
	first, _ := claim34Apply(t, kv, claim34Target(store.FieldFIFO, 10), record)
	// Its lease expires and the message goes back at the next claim.
	claim34Apply(t, kv, claim34Target(store.FieldFIFO, 11), claim34Claim(1, first.Until+10, 2))
	before := claim34Dump(t, kv)
	replayed, _ := claim34Apply(t, kv, claim34Target(store.FieldFIFO, 10), record)
	if !replayed.Replayed || len(replayed.Elements) != 0 || replayed.LeaseID != ([16]byte{}) {
		t.Fatalf("replay of an older claim = %+v, want nothing leased", replayed)
	}
	if second, _ := claim34Apply(t, kv, claim34Target(store.FieldFIFO, 10), record); len(second.Elements) != 0 {
		t.Fatal("a second replay leased")
	}
	after := claim34Dump(t, kv)
	if len(before) != len(after) {
		t.Fatalf("the replay changed the key count from %d to %d", len(before), len(after))
	}
	for key, stored := range before {
		if !bytes.Equal(after[key], stored) {
			t.Fatalf("the replay rewrote key %x", key)
		}
	}
}

func TestALeaseSurvivesARestartOfTheNode(t *testing.T) {
	dir := t.TempDir()
	kv := claim34Open(t, dir)
	claim34Push(t, kv, store.FieldLIFO, "a", "b")
	claimed, _ := claim34Apply(t, kv, claim34Target(store.FieldLIFO, 10), claim34Claim(1, 9_000, 1))
	if claim34Text(t, claimed.Elements[0]) != "b" {
		t.Fatalf("a lifo claim took %q, want b", claim34Text(t, claimed.Elements[0]))
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := claim34Open(t, dir)
	defer func() { _ = reopened.Close() }()
	_, acked := claim34Apply(t, reopened, claim34Target(store.FieldLIFO, 11), claim34Lease(cluster.OpAck, claimed.LeaseID, 9_100))
	if acked.State != store.LeaseAcked {
		t.Fatalf("ack after a restart = %s, want acked", acked.State)
	}
}

func TestAckPastTheDeadlineAtTheNewOwnersNowIsExpired(t *testing.T) {
	kv := claim34New(t)
	claim34Push(t, kv, store.FieldFIFO, "a", "b")
	claimed, _ := claim34Apply(t, kv, claim34Target(store.FieldFIFO, 10), claim34Claim(1, 1_000, 1))
	// The new owner's clock is past the deadline: the lease is expired, the
	// message is back, and a claim hands it out again.
	_, acked := claim34Apply(t, kv, claim34Target(store.FieldFIFO, 11), claim34Lease(cluster.OpAck, claimed.LeaseID, claimed.Until+1))
	if acked.State == store.LeaseAcked {
		t.Fatal("an ack stamped after the deadline was acked")
	}
	if left := claim34Remaining(t, kv, store.FieldFIFO); len(left) != 2 || left[0] != "a" {
		t.Fatalf("after the expiry the field holds %v, want a and b back", left)
	}
}

func TestReleaseExtendAndTheDeliveryLimit(t *testing.T) {
	kv := claim34New(t)
	claim34Push(t, kv, store.FieldFIFO, "a")
	now := int64(10_000)
	seq := uint64(10)
	for delivery := 1; delivery <= store.MaxDeliveries; delivery++ {
		claimed, _ := claim34Apply(t, kv, claim34Target(store.FieldFIFO, seq), claim34Claim(1, now, byte(delivery)))
		seq++
		if len(claimed.Elements) != 1 {
			t.Fatalf("delivery %d handed out %d messages, want 1", delivery, len(claimed.Elements))
		}
		if delivery == 1 {
			extend := claim34Lease(cluster.OpExtend, claimed.LeaseID, now+1_000)
			extend.Until = now + 1_000 + 60_000
			_, extended := claim34Apply(t, kv, claim34Target(store.FieldFIFO, seq), extend)
			seq++
			if extended.State != store.LeaseExtended || extended.Until != now+61_000 {
				t.Fatalf("extend = %+v, want extended to %d", extended, now+61_000)
			}
		}
		_, released := claim34Apply(t, kv, claim34Target(store.FieldFIFO, seq), claim34Lease(cluster.OpRelease, claimed.LeaseID, now+2_000))
		seq++
		if released.State != store.LeaseReleased {
			t.Fatalf("release %d = %s", delivery, released.State)
		}
		left := claim34Remaining(t, kv, store.FieldFIFO)
		if dead := delivery >= store.MaxDeliveries; dead == (len(left) != 0) {
			t.Fatalf("after delivery %d the field holds %v; dead-lettered should be %v", delivery, left, dead)
		}
		now += 100_000
	}
	_, again := claim34Apply(t, kv, claim34Target(store.FieldFIFO, seq), claim34Lease(cluster.OpRelease, [16]byte{1}, now))
	if again.State == store.LeaseReleased {
		t.Fatal("a release of a lease that does not exist was released")
	}
}

func TestExtendOutsideTheLeaseBoundsIsRefusedAtBothEnds(t *testing.T) {
	kv := claim34New(t)
	claim34Push(t, kv, store.FieldFIFO, "a")
	claimed, _ := claim34Apply(t, kv, claim34Target(store.FieldFIFO, 10), claim34Claim(1, 1_000, 1))
	for name, length := range map[string]int64{"too short": store.MinLeaseMs - 1, "too long": store.MaxLeaseMs + 1} {
		extend := claim34Lease(cluster.OpExtend, claimed.LeaseID, 2_000)
		extend.Until = 2_000 + length
		snap := kv.NewSnapshot()
		_, err := ApplyLease(snap, kv.NewBatch(), claim34Target(store.FieldFIFO, 11), extend)
		snap.Discard()
		var refusal *cluster.Error
		if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument {
			t.Fatalf("%s: extend error = %v, want INVALID_ARGUMENT", name, err)
		}
	}
}

func TestAClaimOfAnEmptyFieldGrantsNoLease(t *testing.T) {
	kv := claim34New(t)
	claimed, _ := claim34Apply(t, kv, claim34Target(store.FieldFIFO, 10), claim34Claim(3, 5, 1))
	if claimed.LeaseID != ([16]byte{}) || claimed.Until != 0 || len(claimed.Elements) != 0 {
		t.Fatalf("an empty claim granted %+v", claimed)
	}
}

// claim34LiveHeader gives the document the claim tests push into a live header, which the
// claim classes' precondition requires (placement §7).
func claim34LiveHeader(t *testing.T, kv storage.KV) {
	t.Helper()
	batch := kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(store.DocumentPrefix(testSlot, testDB, testBucket, claim34EncID0()), []byte{0x02, 1, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
}

type claim34Engines struct{ engine store.FieldEngine }

func (e claim34Engines) FieldEngine(uint64, uint64, uint16) (store.FieldEngine, error) {
	return e.engine, nil
}

func TestDocumentApplierAppliesTheClaimClassesInTheRecordsBatch(t *testing.T) {
	kv := claim34New(t)
	claim34Push(t, kv, store.FieldFIFO, "a", "b")
	claim34LiveHeader(t, kv)
	applier := NewDocumentApplier(kv, pairRecorder{})
	for _, class := range []byte{cluster.OpClaim, cluster.OpAck, cluster.OpRelease, cluster.OpExtend} {
		if applier.Handles(class) {
			t.Fatalf("class 0x%02x is handled without a field engine source", class)
		}
	}
	applier.WithFieldEngines(claim34Engines{engine: store.FieldFIFO})
	for _, class := range []byte{cluster.OpClaim, cluster.OpAck, cluster.OpRelease, cluster.OpExtend} {
		if !applier.Handles(class) {
			t.Fatalf("class 0x%02x is not handled", class)
		}
	}
	if code, err := applier.Precondition(testSlot, testDB, claim34Op(cluster.OpClaim)); err != nil || code != 0 {
		t.Fatalf("claim precondition = (%v, %v), want none", code, err)
	}
	batch := kv.NewBatch()
	rec := AppliedRecord{Slot: testSlot, DB: testDB, Op: claim34Logged(t, claim34Claim(1, 7_000, 3)), Pos: cluster.Position{Epoch: 3, Seq: 20}, Cursor: 1, Ordinal: 1}
	result, err := applier.FillWithResult(batch, rec)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 24 {
		t.Fatalf("the claim's dedupe result is %d bytes, want 24 (lease id and deadline)", len(result))
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	if left := claim34Remaining(t, kv, store.FieldFIFO); len(left) != 1 || left[0] != "b" {
		t.Fatalf("after the record the field holds %v, want [b]", left)
	}
	snap := kv.NewSnapshot()
	defer snap.Discard()
	if _, err := snap.Get(pairRecordKey(testSlot, testDB)); err != nil {
		t.Fatalf("the pair record is not in the record's batch: %v", err)
	}
}

func TestClaimOnAHeapFieldIsAnInvalidArgumentThatNamesTheFix(t *testing.T) {
	kv := claim34New(t)
	snap := kv.NewSnapshot()
	defer snap.Discard()
	_, err := ApplyClaim(snap, kv.NewBatch(), claim34Target(store.FieldHeap, 10), claim34Logged(t, claim34Claim(1, 5, 1)))
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument {
		t.Fatalf("claim on a heap field = %v, want a *cluster.Error INVALID_ARGUMENT", err)
	}
	if !strings.Contains(refusal.Message, "only fifo and lifo fields can be claimed; pop the heap field instead") {
		t.Fatalf("the refusal does not name the fix: %q", refusal.Message)
	}
}

func TestAReplayedEmptyClaimLeasesNothingEvenWhenMessagesArrivedSince(t *testing.T) {
	kv := claim34New(t)
	record := claim34Claim(2, 5_000, 1)
	empty, _ := claim34Apply(t, kv, claim34Target(store.FieldFIFO, 10), record)
	if empty.LeaseID != ([16]byte{}) || empty.Until != 0 {
		t.Fatalf("an empty claim answered %+v", empty)
	}
	claim34Push(t, kv, store.FieldFIFO, "late")
	replayed, _ := claim34Apply(t, kv, claim34Target(store.FieldFIFO, 10), record)
	if !replayed.Replayed || replayed.LeaseID != ([16]byte{}) || replayed.Until != 0 || len(replayed.Elements) != 0 {
		t.Fatalf("the replay of an empty claim answered %+v, want zeros", replayed)
	}
	if left := claim34Remaining(t, kv, store.FieldFIFO); len(left) != 1 || left[0] != "late" {
		t.Fatalf("the message that arrived since holds %v, want it still pending", left)
	}
}
