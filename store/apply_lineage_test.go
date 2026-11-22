package store

import (
	"errors"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// seedLineage writes a state document at term, through, with its applied
// sequence at seq.
func seedLineage(t *testing.T, kv storage.KV, term, through, seq uint64) {
	t.Helper()
	batch := kv.NewBatch()
	defer batch.Discard()
	if err := (BucketState{Term: term, Through: through, AppliedSeq: seq}).Put(batch, applyTestSlot, 1, applyTestID); err != nil {
		t.Fatalf("seeding the state: %v", err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("committing the seed: %v", err)
	}
}

// TestTheStoreAppliesAnEntryOnlyInItsTermAndAboveItsPosition is the receiver's
// rule in the store (build doc step 10b.2, spec §20.8.3 R-7): a replica's live
// apply of an entry of its own term above its position applies; one at or
// below the position, or of an older term, is passed over and its local
// sequence consumed, so a replay of the log does not apply it either; one of
// a later term writes nothing. Guard: drop the term comparison — the older
// term's entry is applied over the copy.
func TestTheStoreAppliesAnEntryOnlyInItsTermAndAboveItsPosition(t *testing.T) {
	cases := []struct {
		name          string
		term          uint64
		originSeq     uint64
		positionKnown bool
		wantErr       error
		wantKey       bool
		wantMarked    bool
	}{
		{name: "the copy's term, above its position", term: 4, originSeq: 21, positionKnown: true, wantKey: true, wantMarked: true},
		{name: "the copy's term, at its position", term: 4, originSeq: 20, positionKnown: true, wantErr: ErrAlreadyHeld, wantMarked: true},
		{name: "the copy's term, below its position", term: 4, originSeq: 3, positionKnown: true, wantErr: ErrAlreadyHeld, wantMarked: true},
		{name: "an older term", term: 2, originSeq: 900, positionKnown: true, wantErr: ErrTermBehind, wantMarked: true},
		{name: "a later term", term: 7, originSeq: 1, positionKnown: true, wantErr: ErrTermAhead, wantMarked: true},
		// A source whose log went backwards repeats sequences the position
		// counts: judged by term only, its entry applies.
		{name: "the copy's term, below its position, from a source whose log went backwards", term: 4, originSeq: 3, wantKey: true, wantMarked: true},
		{name: "an older term from a source whose log went backwards", term: 2, originSeq: 3, wantErr: ErrTermBehind, wantMarked: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kv := kvfault.New()
			seedLineage(t, kv, 4, 20, 10)
			var owed OwedApplies
			lineageInfo := ApplyInfo{Seq: 11, Term: tc.term, CheckLineage: true, CheckPosition: tc.positionKnown, OriginSeq: tc.originSeq}
			applied, err := applyAt(kv, &owed, lineageInfo, putOp("k"))
			if !errors.Is(err, tc.wantErr) || (tc.wantErr == nil && err != nil) {
				t.Fatalf("applying an entry of term %d at %d to a copy at (4, 20) answered %v, want %v", tc.term, tc.originSeq, err, tc.wantErr)
			}
			if applied != tc.wantKey {
				t.Errorf("applied = %v, want %v", applied, tc.wantKey)
			}
			snap := kv.NewSnapshot()
			_, getErr := snap.Get(storage.Key(applyTestSlot, 1, applyTestID, storage.NSDocuments, []byte("k")))
			snap.Discard()
			if held := getErr == nil; held != tc.wantKey {
				t.Errorf("the entry's effect is in the store: %v, want %v", held, tc.wantKey)
			}
			st := stateOf(t, kv)
			if marked := st.AppliedSeq == 11; marked != tc.wantMarked {
				t.Errorf("the local sequence 11 is marked applied: %v (applied_seq %d), want %v", marked, st.AppliedSeq, tc.wantMarked)
			}
			if st.Term != 4 {
				t.Errorf("the copy's term moved to %d", st.Term)
			}
			wantThrough := uint64(20)
			if tc.wantKey {
				wantThrough = max(wantThrough, tc.originSeq)
			}
			if st.Through != wantThrough {
				t.Errorf("the copy's position is %d, want %d", st.Through, wantThrough)
			}
			// Replay of the same local sequence, which carries no term, passes
			// over what the live apply consumed.
			replayed, err := applyAt(kv, &owed, ApplyInfo{Seq: 11, Replay: true, OriginSeq: tc.originSeq}, putOp("replayed"))
			if err != nil || replayed {
				t.Errorf("a replay of local sequence 11 answered applied=%v err=%v; the live apply consumed it", replayed, err)
			}
		})
	}
}

// TestReplayDoesNotJudgeARecordedFailureBeneathTheMarkByLineage: replay judges
// an entry by the receiver's rule from the term its log carries (BUG-0588),
// but only above the mark. Beneath it Gate admits only a recorded failure,
// which never applied: later entries moved the copy's position past its
// origin sequence, and judged by it the failed write would be passed over as
// already held. Guard: judge lineage before the beneath check.
func TestReplayDoesNotJudgeARecordedFailureBeneathTheMarkByLineage(t *testing.T) {
	kv := kvfault.New()
	seed := kv.NewBatch()
	if err := (BucketState{Term: 4, Through: 20, AppliedSeq: 10, FailedSeqs: []uint64{6}}).Put(seed, applyTestSlot, 1, applyTestID); err != nil {
		t.Fatalf("seeding the state: %v", err)
	}
	if err := seed.Commit(false); err != nil {
		t.Fatalf("committing the seed: %v", err)
	}
	seed.Discard()
	var owed OwedApplies
	recordedInfo := ApplyInfo{Seq: 6, Replay: true, Record: true, Term: 4, CheckLineage: true, CheckPosition: true, OriginSeq: 15,
		Commutes: func(uint64) bool { return true }}
	applied, err := applyAt(kv, &owed, recordedInfo, putOp("k"))
	if err != nil || !applied {
		t.Fatalf("replaying the recorded failure at 6 (origin 15, the copy at 20) answered applied=%v err=%v, want it applied", applied, err)
	}
	if st := stateOf(t, kv); len(st.FailedSeqs) != 0 {
		t.Errorf("the recorded failure is still owed after its replay: %v", st.FailedSeqs)
	}
}
