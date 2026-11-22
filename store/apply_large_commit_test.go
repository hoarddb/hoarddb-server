package store

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// sizedPutOp writes one document of size bytes.
func sizedPutOp(key string, size int) func(storage.KVSnapshot, *BucketState, storage.KVBatch) error {
	return func(_ storage.KVSnapshot, st *BucketState, b storage.KVBatch) error {
		st.Entries++
		return b.Set(storage.Key(applyTestSlot, 1, applyTestID, storage.NSDocuments, []byte(key)), []byte(strings.Repeat("v", size)))
	}
}

// TestApplyRecordsALargeCommitWithItsEffect: an apply that writes
// more than NoteCommitAbove records its sequence in the state document it
// commits with; a smaller one, one with noting off, and a refused one do not.
func TestApplyRecordsALargeCommitWithItsEffect(t *testing.T) {
	kv := kvfault.New()
	var owed OwedApplies
	steps := []struct {
		name  string
		info  ApplyInfo
		op    func(storage.KVSnapshot, *BucketState, storage.KVBatch) error
		large []uint64
		fails bool
	}{
		{"a small apply", ApplyInfo{Seq: 1, NoteCommitAbove: 1000}, sizedPutOp("a", 10), nil, false},
		{"a large apply", ApplyInfo{Seq: 2, NoteCommitAbove: 1000}, sizedPutOp("b", 2000), []uint64{2}, false},
		{"a large apply with noting off", ApplyInfo{Seq: 3}, sizedPutOp("c", 2000), []uint64{2}, false},
		{"a large apply the operation refuses", ApplyInfo{Seq: 4, NoteCommitAbove: 1000}, func(snap storage.KVSnapshot, st *BucketState, b storage.KVBatch) error {
			if err := sizedPutOp("d", 2000)(snap, st, b); err != nil {
				return err
			}
			return errors.New("refused by the operation")
		}, []uint64{2}, true},
		{"a second large apply", ApplyInfo{Seq: 5, NoteCommitAbove: 1000}, sizedPutOp("e", 2000), []uint64{2, 5}, false},
	}
	for _, step := range steps {
		if _, err := applyAt(kv, &owed, step.info, step.op); (err != nil) != step.fails {
			t.Fatalf("%s: apply returned %v", step.name, err)
		}
		st := stateOf(t, kv)
		if !reflect.DeepEqual(st.LargeCommits, step.large) {
			t.Fatalf("after %s: LargeCommits is %v, want %v", step.name, st.LargeCommits, step.large)
		}
		for _, seq := range step.large {
			if !st.CommittedLarge(seq) {
				t.Fatalf("after %s: CommittedLarge(%d) is false", step.name, seq)
			}
		}
	}
}

// keyedPutOp writes count small documents.
func keyedPutOp(prefix string, count int) func(storage.KVSnapshot, *BucketState, storage.KVBatch) error {
	return func(_ storage.KVSnapshot, st *BucketState, b storage.KVBatch) error {
		for i := range count {
			st.Entries++
			if err := b.Set(storage.Key(applyTestSlot, 1, applyTestID, storage.NSDocuments, fmt.Appendf(nil, "%s%d", prefix, i)), []byte("v")); err != nil {
				return err
			}
		}
		return nil
	}
}

// TestApplyRecordsACommitOverTheEntryThreshold: a batch of many
// small documents can be over a smaller member's entry bound while far under
// the byte threshold, so an apply that writes more keys than
// NoteCommitAboveEntries, its state document included, records its sequence
// too. One at the threshold does not, nor one with entry noting off.
func TestApplyRecordsACommitOverTheEntryThreshold(t *testing.T) {
	kv := kvfault.New()
	var owed OwedApplies
	const bytesAbove, keysAbove = 1 << 20, 10
	steps := []struct {
		name  string
		info  ApplyInfo
		op    func(storage.KVSnapshot, *BucketState, storage.KVBatch) error
		large []uint64
	}{
		{"nine documents and the state, at the threshold", ApplyInfo{Seq: 1, NoteCommitAbove: bytesAbove, NoteCommitAboveEntries: keysAbove}, keyedPutOp("a", 9), nil},
		{"ten documents and the state, over it", ApplyInfo{Seq: 2, NoteCommitAbove: bytesAbove, NoteCommitAboveEntries: keysAbove}, keyedPutOp("b", 10), []uint64{2}},
		{"ten documents with entry noting off", ApplyInfo{Seq: 3, NoteCommitAbove: bytesAbove}, keyedPutOp("c", 10), []uint64{2}},
		{"a thousand documents", ApplyInfo{Seq: 4, NoteCommitAbove: bytesAbove, NoteCommitAboveEntries: keysAbove}, keyedPutOp("d", 1000), []uint64{2, 4}},
	}
	for _, step := range steps {
		if _, err := applyAt(kv, &owed, step.info, step.op); err != nil {
			t.Fatalf("%s: apply returned %v", step.name, err)
		}
		if st := stateOf(t, kv); !reflect.DeepEqual(st.LargeCommits, step.large) {
			t.Fatalf("after %s: LargeCommits is %v, want %v", step.name, st.LargeCommits, step.large)
		}
	}
}

// TestApplyDropsALargeCommitBelowTheLogFloor: an entry below the log's first
// sequence is never re-sent from the log, so its record goes with the next
// batch, as a recorded failure's does.
func TestApplyDropsALargeCommitBelowTheLogFloor(t *testing.T) {
	kv := kvfault.New()
	seed := kv.NewBatch()
	if err := (BucketState{AppliedSeq: 10, LargeCommits: []uint64{2, 9}}).Put(seed, applyTestSlot, 1, applyTestID); err != nil {
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
	if st := stateOf(t, kv); !reflect.DeepEqual(st.LargeCommits, []uint64{9}) {
		t.Fatalf("LargeCommits {2, 9} with log floor 5: the next batch kept %v, want [9]", st.LargeCommits)
	}
	floor = func() (uint64, error) { return 12, nil }
	if _, err := applyAt(kv, &owed, ApplyInfo{Seq: 12, LogFloor: floor}, putOp("b")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if st := stateOf(t, kv); st.LargeCommits != nil {
		t.Fatalf("LargeCommits {9} with log floor 12: the next batch kept %v, want none", st.LargeCommits)
	}
}
