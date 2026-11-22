package store

import (
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// commitFailingKV is a storage.KV whose next failCommits commits fail as the
// store (not the operation) failing, and which counts the commits that land.
type commitFailingKV struct {
	storage.KV
	failCommits atomic.Int32
	commits     atomic.Int32
}

var errDiskGone = errors.New("the disk went away")

func (k *commitFailingKV) NewBatch() storage.KVBatch {
	return &commitFailingBatch{KVBatch: k.KV.NewBatch(), kv: k}
}

type commitFailingBatch struct {
	storage.KVBatch
	kv *commitFailingKV
}

func (b *commitFailingBatch) Commit(sync bool) error {
	if b.kv.failCommits.Load() > 0 {
		b.kv.failCommits.Add(-1)
		b.Discard()
		return errDiskGone
	}
	if err := b.KVBatch.Commit(sync); err != nil {
		return err
	}
	b.kv.commits.Add(1)
	return nil
}

const applyTestSlot, applyTestID = 77, 9

// putOp writes one document key, the way an engine's Op would.
func putOp(key string) func(storage.KVSnapshot, *BucketState, storage.KVBatch) error {
	return func(_ storage.KVSnapshot, st *BucketState, b storage.KVBatch) error {
		st.Entries++
		return b.Set(storage.Key(applyTestSlot, 1, applyTestID, storage.NSDocuments, []byte(key)), []byte("v"))
	}
}

func applyAt(kv storage.KV, owed *OwedApplies, info ApplyInfo, op func(storage.KVSnapshot, *BucketState, storage.KVBatch) error) (bool, error) {
	return Apply(ApplyRequest{KV: kv, Slot: applyTestSlot, DatabaseID: 1, ID: applyTestID, Keyed: true, Info: info, Owed: owed, Op: op})
}

func stateOf(t *testing.T, kv storage.KV) BucketState {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, applyTestSlot, 1, applyTestID)
	if err != nil {
		t.Fatalf("loading state: %v", err)
	}
	return st
}

// TestApplyCarriesAStoreFailureAndNotARefusal is spec §4.4 rule 5's "what
// counts as a failure": the store failing is carried, the operation's own
// refusal is consumed, and nothing is carried where recording is off.
func TestApplyCarriesAStoreFailureAndNotARefusal(t *testing.T) {
	kv := &commitFailingKV{KV: kvfault.New()}
	var owed OwedApplies

	kv.failCommits.Store(1)
	_, err := applyAt(kv, &owed, ApplyInfo{Seq: 5, Record: true}, putOp("a"))
	if !errors.Is(err, ErrApplyFailed) {
		t.Fatalf("a failed commit returned %v, want ErrApplyFailed", err)
	}
	if !reflect.DeepEqual(owed.pending, []uint64{5}) {
		t.Fatalf("after a failed commit at seq 5 the carry holds %v, want [5]", owed.pending)
	}

	refusal := errors.New("field type mismatch")
	var refused OwedApplies
	_, err = applyAt(kv, &refused, ApplyInfo{Seq: 6, Record: true}, func(storage.KVSnapshot, *BucketState, storage.KVBatch) error {
		return refusal
	})
	if !errors.Is(err, refusal) || errors.Is(err, ErrApplyFailed) {
		t.Fatalf("an operation's own refusal returned %v, want the refusal and not ErrApplyFailed", err)
	}
	if len(refused.pending) != 0 {
		t.Fatalf("a refusal was carried: %v", refused.pending)
	}

	var unrecorded OwedApplies
	kv.failCommits.Store(1)
	if _, err := applyAt(kv, &unrecorded, ApplyInfo{Seq: 7}, putOp("b")); !errors.Is(err, ErrApplyFailed) {
		t.Fatalf("a failed commit with recording off returned %v, want ErrApplyFailed", err)
	}
	if len(unrecorded.pending) != 0 {
		t.Fatalf("with Record false the carry holds %v, want nothing (a replica's live apply records nothing)", unrecorded.pending)
	}
}

// TestApplyFoldsTheCarryIntoTheNextWritesBatch: the failure is recorded in
// the batch that advances the mark past it, never in a separate one (§1 trap
// 4 (b)).
func TestApplyFoldsTheCarryIntoTheNextWritesBatch(t *testing.T) {
	kv := &commitFailingKV{KV: kvfault.New()}
	var owed OwedApplies
	kv.failCommits.Store(1)
	if _, err := applyAt(kv, &owed, ApplyInfo{Seq: 3, Record: true}, putOp("a")); !errors.Is(err, ErrApplyFailed) {
		t.Fatalf("the first apply returned %v, want ErrApplyFailed", err)
	}
	before := kv.commits.Load()
	applied, err := applyAt(kv, &owed, ApplyInfo{Seq: 8, Record: true}, putOp("b"))
	if err != nil || !applied {
		t.Fatalf("the next apply = (%v, %v), want (true, nil)", applied, err)
	}
	if n := kv.commits.Load() - before; n != 1 {
		t.Fatalf("the next write took %d commits, want 1: the failure and the mark must share its batch", n)
	}
	st := stateOf(t, kv)
	if st.AppliedSeq != 8 || !reflect.DeepEqual(st.FailedSeqs, []uint64{3}) {
		t.Fatalf("state after the next write: mark %d, FailedSeqs %v; want mark 8, FailedSeqs [3]", st.AppliedSeq, st.FailedSeqs)
	}
	if len(owed.pending) != 0 {
		t.Fatalf("the carry still holds %v after it was recorded", owed.pending)
	}
}

// TestApplyDropsARecordedSequenceBelowTheLogFloor is rule 5's prune (d): a
// recorded sequence the log no longer holds cannot be replayed, and the next
// batch that writes the state document removes it.
func TestApplyDropsARecordedSequenceBelowTheLogFloor(t *testing.T) {
	kv := &commitFailingKV{KV: kvfault.New()}
	seed := kv.NewBatch()
	if err := (BucketState{AppliedSeq: 10, FailedSeqs: []uint64{2, 9}}).Put(seed, applyTestSlot, 1, applyTestID); err != nil {
		t.Fatalf("seeding state: %v", err)
	}
	if err := seed.Commit(false); err != nil {
		t.Fatalf("seeding state: %v", err)
	}
	var owed OwedApplies
	floor := func() (uint64, error) { return 5, nil }
	if _, err := applyAt(kv, &owed, ApplyInfo{Seq: 11, LogFloor: floor}, putOp("a")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if st := stateOf(t, kv); !reflect.DeepEqual(st.FailedSeqs, []uint64{9}) {
		t.Fatalf("FailedSeqs {2, 9} with log floor 5: the next batch kept %v, want [9]", st.FailedSeqs)
	}
	if !reflect.DeepEqual(owed.Unreplayable(), []uint64{2}) {
		t.Fatalf("the dropped sequence was not reported for the ERROR log: %v", owed.Unreplayable())
	}
}

// TestApplyConsumesALiveRefusal checks at the store that a refusal is the
// operation's answer (spec §4.4 rule 5), so the live apply that gave it moves
// the mark past it, and replay of the same entry is skipped rather than given
// a second chance to answer differently. A batch too large for the store is
// the refusal that answered differently: live it carries a version key per
// document that replay does not write.
func TestApplyConsumesALiveRefusal(t *testing.T) {
	for _, refusal := range []error{
		errors.New("field type mismatch"),
		&storage.BatchTooLargeError{Bytes: 4096, Entries: 100, EntryCountReached: true},
	} {
		t.Run(refusal.Error(), func(t *testing.T) {
			kv := kvfault.New()
			var owed OwedApplies
			_, err := applyAt(kv, &owed, ApplyInfo{Seq: 6, Record: true}, func(storage.KVSnapshot, *BucketState, storage.KVBatch) error {
				return refusal
			})
			if !errors.Is(err, refusal) || errors.Is(err, ErrApplyFailed) {
				t.Fatalf("the live refusal returned %v, want the refusal and not ErrApplyFailed", err)
			}
			if st := stateOf(t, kv); st.AppliedSeq != 6 {
				t.Fatalf("after the live refusal of sequence 6 the mark is %d, want 6: replay would apply the refused entry", st.AppliedSeq)
			}
			applied, err := applyAt(kv, &owed, ApplyInfo{Seq: 6, Replay: true, Record: true}, putOp("a"))
			if err != nil || applied {
				t.Fatalf("replay of the refused sequence 6 = (%v, %v), want (false, nil): skipped", applied, err)
			}
		})
	}
}

// TestApplyConsumesARefusalAtTheCommitDurably: the batch can also outgrow the
// bound only when the state document joins it, at the commit. That refusal is
// consumed too, and its state-only batch is synced: a power loss that kept
// the log entry and lost the mark would let replay apply what the client was
// told was not written.
func TestApplyConsumesARefusalAtTheCommitDurably(t *testing.T) {
	const bound = 4096
	kv := kvfault.New()
	kv.SetMaxBatchBytes(bound)
	var owed OwedApplies
	key := storage.Key(applyTestSlot, 1, applyTestID, storage.NSDocuments, []byte("big"))
	// kvfault charges 16 bytes per write over its key and value; this one
	// write leaves a byte, so the state document is what crosses the bound.
	fill := func(_ storage.KVSnapshot, st *BucketState, b storage.KVBatch) error {
		st.Entries++
		return b.Set(key, make([]byte, bound-1-16-len(key)))
	}
	if _, err := applyAt(kv, &owed, ApplyInfo{Seq: 4, Record: true}, fill); !errors.Is(err, storage.ErrBatchTooLarge) || errors.Is(err, ErrApplyFailed) {
		t.Fatalf("the apply whose state document crossed the bound returned %v, want the batch-too-large refusal", err)
	}
	kv.PowerLoss()
	if st := stateOf(t, kv); st.AppliedSeq != 4 {
		t.Fatalf("after the refusal at the commit and a power loss the mark is %d, want 4", st.AppliedSeq)
	}
	applied, err := applyAt(kv, &owed, ApplyInfo{Seq: 4, Replay: true, Record: true}, putOp("small"))
	if err != nil || applied {
		t.Fatalf("replay of the refused sequence 4 = (%v, %v), want (false, nil): skipped", applied, err)
	}
}

// TestApplyDoesNotConsumeAStoreFailure guards the other
// side: a store failure is not an answer. The live apply leaves the mark
// below it and replay retries it (spec §4.4 rule 5).
func TestApplyDoesNotConsumeAStoreFailure(t *testing.T) {
	kv := &commitFailingKV{KV: kvfault.New()}
	var owed OwedApplies
	kv.failCommits.Store(1)
	if _, err := applyAt(kv, &owed, ApplyInfo{Seq: 5, Record: true}, putOp("a")); !errors.Is(err, ErrApplyFailed) {
		t.Fatalf("a failed commit returned %v, want ErrApplyFailed", err)
	}
	if st := stateOf(t, kv); st.AppliedSeq != 0 {
		t.Fatalf("a store failure at sequence 5 moved the mark to %d: replay would skip the write it owes", st.AppliedSeq)
	}
	applied, err := applyAt(kv, &owed, ApplyInfo{Seq: 5, Replay: true, Record: true}, putOp("a"))
	if err != nil || !applied {
		t.Fatalf("replay of the failed sequence 5 = (%v, %v), want (true, nil): retried", applied, err)
	}
}

// TestApplyLeavesAnUnknownRefusalAboveTheMark: a refusal is consumed only
// where it is known to be the answer. A replica's batch-too-large for an entry
// its owner committed is its own smaller memtable, and a refusal
// met at replay was never answered live; both leave the mark where it was, so
// a resized member's replay can still apply the entry.
func TestApplyLeavesAnUnknownRefusalAboveTheMark(t *testing.T) {
	tooLarge := &storage.BatchTooLargeError{Bytes: 4096}
	for _, unknown := range []struct {
		name string
		info ApplyInfo
	}{
		{"a replica's batch too large for an entry its owner committed", ApplyInfo{Seq: 3, OwnerCommitted: true}},
		{"a batch too large at replay", ApplyInfo{Seq: 3, Replay: true, Record: true}},
	} {
		t.Run(unknown.name, func(t *testing.T) {
			kv := kvfault.New()
			var owed OwedApplies
			_, err := applyAt(kv, &owed, unknown.info, func(storage.KVSnapshot, *BucketState, storage.KVBatch) error {
				return tooLarge
			})
			if !errors.Is(err, storage.ErrBatchTooLarge) {
				t.Fatalf("the refusal returned %v, want the batch-too-large refusal", err)
			}
			if st := stateOf(t, kv); st.AppliedSeq != 0 {
				t.Fatalf("the refusal moved the mark to %d: a resized member's replay would skip the entry", st.AppliedSeq)
			}
			applied, err := applyAt(kv, &owed, ApplyInfo{Seq: 3, Replay: true, Record: true}, putOp("a"))
			if err != nil || !applied {
				t.Fatalf("replay of sequence 3 once it fits = (%v, %v), want (true, nil)", applied, err)
			}
		})
	}
	// Any other refusal of an owner-committed entry is still the answer.
	kv := kvfault.New()
	var owed OwedApplies
	if _, err := applyAt(kv, &owed, ApplyInfo{Seq: 3, OwnerCommitted: true}, func(storage.KVSnapshot, *BucketState, storage.KVBatch) error {
		return errors.New("field type mismatch")
	}); err == nil {
		t.Fatal("the refusal returned nil")
	}
	if st := stateOf(t, kv); st.AppliedSeq != 3 {
		t.Fatalf("a replica's own refusal of an owner-committed entry left the mark at %d, want 3", st.AppliedSeq)
	}
}
