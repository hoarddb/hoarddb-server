package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
)

// fd* helpers are this file's; every test runs on a real Badger directory.

func fdOpen(t *testing.T, dir string) storage.KV {
	t.Helper()
	kv, err := storage.OpenBadgerKV(dir, storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	return kv
}

func fdRef(t *testing.T, engine FieldEngine) FieldRef {
	t.Helper()
	owner, err := FieldOwnerPrefix(fieldTestSlot, 1, fieldTestID, "doc")
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	return FieldRef{Owner: owner, FieldID: 9, Engine: engine}
}

// fdCommit runs verb on one snapshot and commits its batch, as one record's apply.
func fdCommit(t *testing.T, kv storage.KV, verb func(snap storage.KVSnapshot, batch storage.KVBatch) error) {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	batch := kv.NewBatch()
	if err := verb(snap, batch); err != nil {
		t.Fatalf("verb: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// fdDoc is a queue element: the engines store BSON documents.
func fdDoc(t *testing.T, text string) []byte {
	t.Helper()
	encoded, err := bson.Marshal(map[string]interface{}{"v": text})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func fdText(t *testing.T, element []byte) string {
	t.Helper()
	var decoded struct {
		V string `bson:"v"`
	}
	if err := bson.Unmarshal(element, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded.V
}

func fdPush(t *testing.T, kv storage.KV, ref FieldRef, payloads ...string) {
	t.Helper()
	for _, payload := range payloads {
		fdCommit(t, kv, func(snap storage.KVSnapshot, batch storage.KVBatch) error {
			var parent BucketState
			_, err := FieldPush(FieldWrite{Snap: snap, State: &parent, Batch: batch}, ref, fdDoc(t, payload))
			return err
		})
	}
}

func fdClaim(t *testing.T, kv storage.KV, ref FieldRef, args FieldClaimArgs) (Claim, []string) {
	t.Helper()
	var claimed []string
	var claim Claim
	fdCommit(t, kv, func(snap storage.KVSnapshot, batch storage.KVBatch) error {
		var err error
		claim, err = FieldClaim(snap, batch, ref, args, func(entry []byte) error {
			claimed = append(claimed, fdText(t, entry))
			return nil
		})
		return err
	})
	return claim, claimed
}

func fdArgs(n int, now int64, nonce byte) FieldClaimArgs {
	return FieldClaimArgs{ClaimArgs: ClaimArgs{N: n, LeaseMs: DefaultLeaseMs, Now: now, Nonce: [8]byte{nonce}}}
}

func fdLeaseVerb(t *testing.T, kv storage.KV, verb func(storage.KVSnapshot, storage.KVBatch) (LeaseResult, error)) LeaseResult {
	t.Helper()
	var result LeaseResult
	fdCommit(t, kv, func(snap storage.KVSnapshot, batch storage.KVBatch) error {
		var err error
		result, err = verb(snap, batch)
		return err
	})
	return result
}

func fdAck(t *testing.T, kv storage.KV, ref FieldRef, lease [leaseIDBytes]byte, now int64) LeaseResult {
	return fdLeaseVerb(t, kv, func(snap storage.KVSnapshot, batch storage.KVBatch) (LeaseResult, error) {
		return FieldAck(snap, batch, ref, LeaseArgs{LeaseID: lease, Now: now})
	})
}

func fdPending(t *testing.T, kv storage.KV, ref FieldRef) []string {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	var pending []string
	if err := FieldPeekN(snap, ref, 100, func(entry FieldEntry) error {
		pending = append(pending, fdText(t, entry.Value))
		return nil
	}); err != nil {
		t.Fatalf("peeking: %v", err)
	}
	return pending
}

// fdCopy rebuilds a fresh store from every key of kv: what a new owner holds
// after it applied the log, as bytes on disk.
func fdCopy(t *testing.T, kv storage.KV) storage.KV {
	t.Helper()
	fresh := fdOpen(t, t.TempDir())
	t.Cleanup(func() { _ = fresh.Close() })
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

func TestFieldClaimMovesElementsUnderALeaseThatIsOnDisk(t *testing.T) {
	kv := fdOpen(t, t.TempDir())
	defer func() { _ = kv.Close() }()
	ref := fdRef(t, FieldFIFO)
	fdPush(t, kv, ref, "a", "b", "c")
	claim, claimed := fdClaim(t, kv, ref, fdArgs(2, 1_000_000, 7))
	if claim.Deadline != 1_000_000+DefaultLeaseMs || len(claim.Items) != 2 || len(claimed) != 2 || claimed[0] != "a" {
		t.Fatalf("claim = %+v, visited %v", claim, claimed)
	}
	if got := fdPending(t, kv, ref); len(got) != 1 || got[0] != "c" {
		t.Fatalf("left in the field: %v, want [c]", got)
	}
	snap := kv.NewSnapshot()
	defer snap.Discard()
	if _, err := snap.Get((&fieldQueue{ref: ref}).kindKey(kindLeaseHeader, claim.LeaseID[:])); err != nil {
		t.Fatalf("the lease header is not on disk: %v", err)
	}
}

func TestFieldLeaseSurvivesARebuiltCopyAndIsAckedThere(t *testing.T) {
	kv := fdOpen(t, t.TempDir())
	defer func() { _ = kv.Close() }()
	ref := fdRef(t, FieldFIFO)
	fdPush(t, kv, ref, "a", "b", "c")
	claim, _ := fdClaim(t, kv, ref, fdArgs(2, 1_000_000, 7))
	rebuilt := fdCopy(t, kv)
	result := fdAck(t, rebuilt, ref, claim.LeaseID, 1_000_010)
	if result.State != LeaseAcked || result.Acked != 2 {
		t.Fatalf("ack on the rebuilt copy = %+v, want acked 2", result)
	}
	if got := fdPending(t, rebuilt, ref); len(got) != 1 || got[0] != "c" {
		t.Fatalf("rebuilt copy holds %v after the ack, want [c]", got)
	}
	// A copy that never received the claim: the lease is unknown and the
	// elements are still there to be claimed again.
	other := fdOpen(t, t.TempDir())
	defer func() { _ = other.Close() }()
	fdPush(t, other, ref, "a", "b", "c")
	if got := fdAck(t, other, ref, claim.LeaseID, 1_000_010); got.State != LeaseUnknown {
		t.Fatalf("ack of a lease the copy never saw = %s, want unknown", got.State)
	}
	if _, again := fdClaim(t, other, ref, fdArgs(2, 1_000_020, 8)); len(again) != 2 || again[0] != "a" {
		t.Fatalf("redelivery = %v, want a and b", again)
	}
}

func TestFieldLeaseSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	kv := fdOpen(t, dir)
	ref := fdRef(t, FieldLIFO)
	fdPush(t, kv, ref, "a", "b", "c")
	claim, claimed := fdClaim(t, kv, ref, fdArgs(1, 5_000, 3))
	if len(claimed) != 1 || claimed[0] != "c" {
		t.Fatalf("a lifo claim took %v, want c", claimed)
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := fdOpen(t, dir)
	defer func() { _ = reopened.Close() }()
	if got := fdAck(t, reopened, ref, claim.LeaseID, 5_100); got.State != LeaseAcked {
		t.Fatalf("ack after a restart = %s, want acked", got.State)
	}
}

func TestFieldExpiryIsLazyAndRedelivers(t *testing.T) {
	kv := fdOpen(t, t.TempDir())
	defer func() { _ = kv.Close() }()
	ref := fdRef(t, FieldFIFO)
	fdPush(t, kv, ref, "a", "b")
	first, _ := fdClaim(t, kv, ref, fdArgs(1, 1_000, 1))
	// Nothing happens at the deadline; the next operation returns the lease.
	later := first.Deadline + 1
	second, claimed := fdClaim(t, kv, ref, fdArgs(1, later, 2))
	if len(claimed) != 1 || claimed[0] != "a" || second.Delivery.Returned != 1 {
		t.Fatalf("second claim took %v (returned %d), want a redelivered", claimed, second.Delivery.Returned)
	}
	if got := fdAck(t, kv, ref, first.LeaseID, later+1); got.State != LeaseUnknown && got.State != LeaseExpired {
		t.Fatalf("ack of the returned lease = %s, want expired or unknown", got.State)
	}
}

func TestFieldClaimRecordAppliedTwiceLeasesNothingEvenAfterExpiry(t *testing.T) {
	kv := fdOpen(t, t.TempDir())
	defer func() { _ = kv.Close() }()
	ref := fdRef(t, FieldFIFO)
	fdPush(t, kv, ref, "a", "b", "c")
	args := fdArgs(1, 1_000, 1)
	args.Position = [16]byte{15: 5}
	first, _ := fdClaim(t, kv, ref, args)
	// The lease expires and its element goes back; then the same record is applied again.
	expiry := fdArgs(1, first.Deadline+10, 2)
	expiry.Position = [16]byte{15: 6}
	fdClaim(t, kv, ref, expiry)
	replayed, claimed := fdClaim(t, kv, ref, args)
	if !replayed.Replayed || len(claimed) != 0 || replayed.LeaseID != ([leaseIDBytes]byte{}) || replayed.Deadline != 0 {
		t.Fatalf("replay of an older claim = %+v visited %v, want nothing leased", replayed, claimed)
	}
	last, _ := fdClaim(t, kv, ref, expiry)
	if !last.Replayed || len(claimed) != 0 || last.Deadline != expiry.Now+expiry.LeaseMs {
		t.Fatalf("replay at the last position = %+v, want the recorded lease", last)
	}
}

func TestFieldReleaseCountsAReturnAndTheDeliveryLimitDeadLetters(t *testing.T) {
	kv := fdOpen(t, t.TempDir())
	defer func() { _ = kv.Close() }()
	ref := fdRef(t, FieldFIFO)
	fdPush(t, kv, ref, "a")
	now := int64(1_000)
	for delivery := 1; delivery <= MaxDeliveries; delivery++ {
		claim, claimed := fdClaim(t, kv, ref, fdArgs(1, now, byte(delivery)))
		if len(claimed) != 1 {
			t.Fatalf("delivery %d took %v, want the element still deliverable", delivery, claimed)
		}
		released := fdLeaseVerb(t, kv, func(snap storage.KVSnapshot, batch storage.KVBatch) (LeaseResult, error) {
			return FieldRelease(snap, batch, ref, LeaseArgs{LeaseID: claim.LeaseID, Now: now})
		})
		if released.State != LeaseReleased {
			t.Fatalf("release %d = %s", delivery, released.State)
		}
		wantDead := delivery >= MaxDeliveries
		if got := fdPending(t, kv, ref); wantDead == (len(got) != 0) {
			t.Fatalf("after release %d the field holds %v, dead-lettered = %v expected", delivery, got, wantDead)
		}
		now += 10
	}
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: (&fieldQueue{ref: ref}).kindKey(kindDead), Upper: prefixEnd((&fieldQueue{ref: ref}).kindKey(kindDead))})
	defer it.Close()
	it.Seek(nil)
	if !it.Valid() {
		t.Fatal("no dead record on disk after the delivery limit")
	}
}

func TestFieldExtendMovesTheDeadlineAndUnknownLeaseIsAResult(t *testing.T) {
	kv := fdOpen(t, t.TempDir())
	defer func() { _ = kv.Close() }()
	ref := fdRef(t, FieldFIFO)
	fdPush(t, kv, ref, "a")
	claim, _ := fdClaim(t, kv, ref, fdArgs(1, 1_000, 1))
	extended := fdLeaseVerb(t, kv, func(snap storage.KVSnapshot, batch storage.KVBatch) (LeaseResult, error) {
		return FieldExtend(snap, batch, ref, LeaseArgs{LeaseID: claim.LeaseID, Now: 2_000}, 60_000)
	})
	if extended.State != LeaseExtended || extended.Deadline != 62_000 {
		t.Fatalf("extend = %+v, want extended to 62000", extended)
	}
	missing := fdLeaseVerb(t, kv, func(snap storage.KVSnapshot, batch storage.KVBatch) (LeaseResult, error) {
		return FieldExtend(snap, batch, ref, LeaseArgs{LeaseID: [leaseIDBytes]byte{1}, Now: 2_000}, 60_000)
	})
	if missing.State != LeaseUnknown {
		t.Fatalf("extend of a lease that does not exist = %s, want unknown", missing.State)
	}
}

func TestFieldDeliveryKeysAllLiveUnderTheDocumentsOwnerRange(t *testing.T) {
	kv := fdOpen(t, t.TempDir())
	defer func() { _ = kv.Close() }()
	ref := fdRef(t, FieldFIFO)
	fdPush(t, kv, ref, "a", "b")
	claim, _ := fdClaim(t, kv, ref, fdArgs(2, 1_000, 1))
	fdAck(t, kv, ref, claim.LeaseID, 1_100)
	fdPush(t, kv, ref, "c")
	fdClaim(t, kv, ref, fdArgs(1, 2_000, 2))
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{})
	defer it.Close()
	delivery := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		if !bytes.HasPrefix(it.Key(), ref.Owner) {
			t.Fatalf("key %x is outside the document's field-store range, so a delete would leave it", it.Key())
		}
		if bytes.HasPrefix(it.Key(), FieldElementKey(ref.StorePrefix(), storage.NSDelivery, nil)) {
			delivery++
		}
	}
	if delivery == 0 {
		t.Fatal("no delivery key was written")
	}
}

func TestFieldClaimRefusesAStoreThatIsNotAQueue(t *testing.T) {
	kv := fdOpen(t, t.TempDir())
	defer func() { _ = kv.Close() }()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	_, err := FieldClaim(snap, kv.NewBatch(), fdRef(t, FieldHeap), fdArgs(1, 1, 1), func([]byte) error { return nil })
	if !errors.Is(err, ErrFieldNotClaimable) {
		t.Fatalf("claim on a heap field = %v, want ErrFieldNotClaimable", err)
	}
}

func TestAClaimingFieldAcceptsPushesAndKeepsItsCursors(t *testing.T) {
	kv := fdOpen(t, t.TempDir())
	defer func() { _ = kv.Close() }()
	ref := fdRef(t, FieldFIFO)
	fdPush(t, kv, ref, "a", "b")
	claim, _ := fdClaim(t, kv, ref, fdArgs(1, 1_000, 1))
	fdPush(t, kv, ref, "c")
	fdLeaseVerb(t, kv, func(snap storage.KVSnapshot, batch storage.KVBatch) (LeaseResult, error) {
		return FieldRelease(snap, batch, ref, LeaseArgs{LeaseID: claim.LeaseID, Now: 1_010})
	})
	if got := fdPending(t, kv, ref); len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("after a release the field holds %v, want a b c in order", got)
	}
}

func fdRaw(t *testing.T, kv storage.KV, ref FieldRef, kind byte) []byte {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	stored, err := snap.Get((&fieldQueue{ref: ref}).kindKey(kind))
	if err != nil {
		t.Fatalf("reading record %q: %v", kind, err)
	}
	return stored
}

// TestFieldClaimPositionRecordIsByteExact pins the 'p' value of conventions
// section 1: [epoch:8 BE][seq:8 BE][leaseID:16][until:8 BE], the outcome of the
// claim at that position, with the lease id zero when nothing was leased.
func TestFieldClaimPositionRecordIsByteExact(t *testing.T) {
	kv := fdOpen(t, t.TempDir())
	defer func() { _ = kv.Close() }()
	ref := fdRef(t, FieldFIFO)
	empty := fdArgs(1, 1_000, 1)
	empty.Position = [16]byte{7: 3, 15: 4}
	fdClaim(t, kv, ref, empty)
	want := append(append([]byte{}, empty.Position[:]...), make([]byte, 24)...)
	if got := fdRaw(t, kv, ref, kindClaimPosition); !bytes.Equal(got, want) {
		t.Fatalf("p after an empty claim = %x, want %x", got, want)
	}
	fdPush(t, kv, ref, "a")
	leased := fdArgs(1, 2_000, 9)
	leased.Position = [16]byte{7: 3, 15: 5}
	claim, _ := fdClaim(t, kv, ref, leased)
	want = append(append([]byte{}, leased.Position[:]...), claim.LeaseID[4:]...)
	want = append(want, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(want[32:], uint64(2_000+DefaultLeaseMs))
	if got := fdRaw(t, kv, ref, kindClaimPosition); !bytes.Equal(got, want) || len(got) != 40 {
		t.Fatalf("p after a lease = %x, want %x", got, want)
	}
}

func TestAReplayedEmptyFieldClaimAnswersZerosAndLeavesLaterMessages(t *testing.T) {
	kv := fdOpen(t, t.TempDir())
	defer func() { _ = kv.Close() }()
	ref := fdRef(t, FieldFIFO)
	args := fdArgs(1, 1_000, 1)
	args.Position = [16]byte{15: 1}
	fdClaim(t, kv, ref, args)
	fdPush(t, kv, ref, "late")
	replayed, claimed := fdClaim(t, kv, ref, args)
	if !replayed.Replayed || replayed.LeaseID != ([leaseIDBytes]byte{}) || replayed.Deadline != 0 || len(claimed) != 0 {
		t.Fatalf("replay of an empty claim = %+v visited %v, want zeros", replayed, claimed)
	}
	if got := fdPending(t, kv, ref); len(got) != 1 {
		t.Fatalf("pending %v, want the late message", got)
	}
}

// TestFieldDeliveryStateRoundTrips pins the 's' value: the BSON of
// fieldDeliveryState, read back to the counters a claim leaves.
func TestFieldDeliveryStateRoundTrips(t *testing.T) {
	kv := fdOpen(t, t.TempDir())
	defer func() { _ = kv.Close() }()
	ref := fdRef(t, FieldFIFO)
	fdPush(t, kv, ref, "a", "b")
	fdClaim(t, kv, ref, fdArgs(2, 1_000, 1))
	stored := fdRaw(t, kv, ref, kindState)
	var state fieldDeliveryState
	if err := bson.Unmarshal(stored, &state); err != nil {
		t.Fatal(err)
	}
	if state.Leases != 1 || state.Pending != 2 || state.LeaseClock != 1_000 || state.PendingBytes <= 0 {
		t.Fatalf("delivery state = %+v, want 1 lease, 2 pending, clock 1000", state)
	}
	again, err := bson.Marshal(state)
	if err != nil || !bytes.Equal(again, stored) {
		t.Fatalf("the state does not re-encode to the stored bytes: %v", err)
	}
}
