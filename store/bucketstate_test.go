package store

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// TestGateSkipsReplayAtOrBelowAppliedSeq is spec §4.4 rule 2 on the replay
// path: an entry at or below the bucket's applied_seq is skipped, and
// skipping is not an error.
func TestGateSkipsReplayAtOrBelowAppliedSeq(t *testing.T) {
	st := BucketState{AppliedSeq: 10}
	for _, tc := range []struct {
		seq   uint64
		apply bool
	}{{5, false}, {10, false}, {11, true}} {
		apply, err := Gate(st, tc.seq, true)
		if err != nil {
			t.Fatalf("replay of seq %d over applied_seq 10 returned an error: %v", tc.seq, err)
		}
		if apply != tc.apply {
			t.Errorf("replay of seq %d over applied_seq 10: apply = %v, want %v", tc.seq, apply, tc.apply)
		}
	}
}

// TestGateErrorsOnLivePathAtOrBelowAppliedSeq is trap 4 of the build doc's
// silent list: a live apply at or below applied_seq means applies left
// sequence order, and skipping it would turn that bug into silent loss.
func TestGateErrorsOnLivePathAtOrBelowAppliedSeq(t *testing.T) {
	st := BucketState{AppliedSeq: 10}
	for _, seq := range []uint64{3, 10} {
		apply, err := Gate(st, seq, false)
		if !errors.Is(err, ErrApplyOutOfOrder) {
			t.Fatalf("live apply of seq %d over applied_seq 10 returned (%v, %v), want ErrApplyOutOfOrder", seq, apply, err)
		}
		if apply {
			t.Errorf("live apply of seq %d over applied_seq 10 was allowed alongside its error", seq)
		}
		for _, want := range []string{"10", strconv.FormatUint(seq, 10)} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the error must name both sequences (%d and 10), got: %v", seq, err)
			}
		}
	}
	if apply, err := Gate(st, 11, false); err != nil || !apply {
		t.Errorf("live apply of seq 11 over applied_seq 10 = (%v, %v), want (true, nil)", apply, err)
	}
}

// TestGateAppliesEverythingWithoutAWAL: a standalone node has no WAL and no
// sequence, so sequence 0 always applies.
func TestGateAppliesEverythingWithoutAWAL(t *testing.T) {
	st := BucketState{AppliedSeq: 10}
	for _, replay := range []bool{true, false} {
		if apply, err := Gate(st, 0, replay); err != nil || !apply {
			t.Errorf("seq 0 (replay=%v) over applied_seq 10 = (%v, %v), want (true, nil)", replay, apply, err)
		}
	}
}

// TestBucketStateRoundTrips: the document lives at [s][id][0x05], an absent
// one reads as the zero value, and every field survives a write.
func TestBucketStateRoundTrips(t *testing.T) {
	kv := kvfault.New()
	const slot, id = 1234, 42
	snap := kv.NewSnapshot()
	absent, err := LoadBucketState(snap, slot, 1, id)
	snap.Discard()
	if err != nil || !reflect.DeepEqual(absent, BucketState{}) {
		t.Fatalf("an absent state document read as (%+v, %v), want the zero value", absent, err)
	}

	want := BucketState{Head: 1, Tail: 2, Next: 3, Entries: 4, AppliedSeq: 5, LogicalBytes: 6}
	batch := kv.NewBatch()
	if err := want.Put(batch, slot, 1, id); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	snap = kv.NewSnapshot()
	defer snap.Discard()
	if _, err := snap.Get(storage.Key(slot, 1, id, storage.NSState)); err != nil {
		t.Fatalf("the state document is not at [s][id][0x05]: %v", err)
	}
	got, err := LoadBucketState(snap, slot, 1, id)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("LoadBucketState = (%+v, %v), want %+v", got, err, want)
	}
}

// TestGateAdmitsAFailedSequenceOnlyOnReplay is spec §4.4 rule 2's exception
// (rev 5.2): on replay a sequence at or below the mark applies only if it is
// a recorded failure; on the live path it is an ordering fault either way.
func TestGateAdmitsAFailedSequenceOnlyOnReplay(t *testing.T) {
	st := BucketState{AppliedSeq: 10, FailedSeqs: []uint64{4}}
	if apply, err := Gate(st, 4, true); err != nil || !apply {
		t.Errorf("replay of recorded failure 4 under mark 10 = (%v, %v), want (true, nil)", apply, err)
	}
	if apply, err := Gate(st, 5, true); err != nil || apply {
		t.Errorf("replay of seq 5 under mark 10, not recorded = (%v, %v), want (false, nil)", apply, err)
	}
	if apply, err := Gate(st, 4, false); !errors.Is(err, ErrApplyOutOfOrder) || apply {
		t.Errorf("live apply of recorded failure 4 under mark 10 = (%v, %v), want ErrApplyOutOfOrder", apply, err)
	}
}

// TestOweRecordsOnlyAboveTheMark: a carried sequence at or below the mark is
// an ambiguous commit that took effect, and recording it would make replay
// apply it twice (§1 trap 4 (c)).
func TestOweRecordsOnlyAboveTheMark(t *testing.T) {
	st := BucketState{AppliedSeq: 4}
	st.Owe([]uint64{3, 5})
	if want := []uint64{5}; !reflect.DeepEqual(st.FailedSeqs, want) {
		t.Fatalf("mark 4, pending {3, 5}: FailedSeqs = %v, want %v", st.FailedSeqs, want)
	}
	st.Owe([]uint64{9, 5, 7})
	if want := []uint64{5, 7, 9}; !reflect.DeepEqual(st.FailedSeqs, want) {
		t.Fatalf("FailedSeqs = %v, want %v: sorted and without duplicates", st.FailedSeqs, want)
	}
}

// TestAdvanceNeverLowersTheMarkAndSettles: Advance is a max, never an
// assignment (§1 trap 4 (a)), and it removes the sequence from the set.
func TestAdvanceNeverLowersTheMarkAndSettles(t *testing.T) {
	st := BucketState{AppliedSeq: 10, FailedSeqs: []uint64{3}}
	st.Advance(3)
	if st.AppliedSeq != 10 {
		t.Errorf("Advance(3) over mark 10 moved the mark to %d", st.AppliedSeq)
	}
	if len(st.FailedSeqs) != 0 {
		t.Errorf("Advance(3) left FailedSeqs = %v, want it empty", st.FailedSeqs)
	}
	st.Advance(12)
	if st.AppliedSeq != 12 {
		t.Errorf("Advance(12) over mark 10 = %d, want 12", st.AppliedSeq)
	}
}

// TestFailedSeqsIsAbsentWhenEmpty: a bucket with no failure pays nothing at
// rest for the field (spec §3.4, §6).
func TestFailedSeqsIsAbsentWhenEmpty(t *testing.T) {
	encoded, err := bson.Marshal(BucketState{AppliedSeq: 7})
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	if _, err := bson.Raw(encoded).LookupErr("failed_seqs"); err == nil {
		t.Fatalf("a state with no failures encodes a failed_seqs element: %v", bson.Raw(encoded))
	}
	encoded, err = bson.Marshal(BucketState{AppliedSeq: 7, FailedSeqs: []uint64{3}})
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	if _, err := bson.Raw(encoded).LookupErr("failed_seqs"); err != nil {
		t.Fatalf("a state with a failure has no failed_seqs element: %v", bson.Raw(encoded))
	}
}

// TestIdleBucketStateIsByteIdentical is CA-34's pin: a bucket that has never
// claimed encodes exactly as it did before the ten delivery fields existed, so
// a million idle buckets pay nothing for them (AGENTS.md rule 11). The goldens
// are the bson of the struct as it stood on the base, captured before the
// fields were added; a field that loses its omitempty moves every one of them.
func TestIdleBucketStateIsByteIdentical(t *testing.T) {
	for _, tc := range []struct {
		name   string
		state  BucketState
		golden string
	}{
		{name: "fifo", state: BucketState{Head: 3, Tail: 9, Next: 10, Entries: 7, AppliedSeq: 12, LogicalBytes: 700},
			golden: "6c0000001268656164000300000000000000127461696c000900000000000000126e657874000a0000000000000012656e7472696573000700000000000000126170706c6965645f736571000c00000000000000126c6f676963616c5f627974657300bc0200000000000000"},
		{name: "lifo", state: BucketState{Head: 0, Tail: 9, Next: 10, Entries: 10, AppliedSeq: 40, LogicalBytes: 1000},
			golden: "6c0000001268656164000000000000000000127461696c000900000000000000126e657874000a0000000000000012656e7472696573000a00000000000000126170706c6965645f736571002800000000000000126c6f676963616c5f627974657300e80300000000000000"},
		{name: "heap", state: BucketState{Entries: 5, AppliedSeq: 8, LogicalBytes: 640},
			golden: "6c0000001268656164000000000000000000127461696c000000000000000000126e65787400000000000000000012656e7472696573000500000000000000126170706c6965645f736571000800000000000000126c6f676963616c5f627974657300800200000000000000"},
		{name: "fifo-failed", state: BucketState{Head: 3, Tail: 9, Next: 10, Entries: 7, AppliedSeq: 12, LogicalBytes: 700, FailedSeqs: []uint64{4, 6}},
			golden: "940000001268656164000300000000000000127461696c000900000000000000126e657874000a0000000000000012656e7472696573000700000000000000126170706c6965645f736571000c00000000000000126c6f676963616c5f627974657300bc02000000000000046661696c65645f73657173001b000000123000040000000000000012310006000000000000000000"},
		{name: "lifo-failed", state: BucketState{Head: 0, Tail: 9, Next: 10, Entries: 10, AppliedSeq: 40, LogicalBytes: 1000, FailedSeqs: []uint64{9}},
			golden: "890000001268656164000000000000000000127461696c000900000000000000126e657874000a0000000000000012656e7472696573000a00000000000000126170706c6965645f736571002800000000000000126c6f676963616c5f627974657300e803000000000000046661696c65645f73657173001000000012300009000000000000000000"},
		{name: "heap-failed", state: BucketState{Entries: 5, AppliedSeq: 8, LogicalBytes: 640, FailedSeqs: []uint64{2, 3, 5}, Incarnation: 77},
			golden: "b40000001268656164000000000000000000127461696c000000000000000000126e65787400000000000000000012656e7472696573000500000000000000126170706c6965645f736571000800000000000000126c6f676963616c5f6279746573008002000000000000046661696c65645f7365717300260000001230000200000000000000123100030000000000000012320005000000000000000012696e6361726e6174696f6e004d0000000000000000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := bson.Marshal(tc.state)
			if err != nil {
				t.Fatalf("encoding the state: %v", err)
			}
			if got := hex.EncodeToString(encoded); got != tc.golden {
				t.Fatalf("an idle bucket's state encodes as\n  %s\nwant the base's\n  %s", got, tc.golden)
			}
		})
	}
}

// TestDeliveryFieldsAreEncodedWhenSet is the pin's positive control: each of
// the ten fields travels under the name claim-ack-spec §3.3 gives it, so the
// byte-identical rows above are absent fields and not fields that never
// encode.
func TestDeliveryFieldsAreEncodedWhenSet(t *testing.T) {
	encoded, err := bson.Marshal(BucketState{
		Leases: 1, Pending: 2, PendingBytes: 3, Returned: 4, Dead: 5,
		DeadBytes: 6, DeadSeq: 7, LeaseClock: 8, ResumeAt: 9, ResumeBelow: 10,
	})
	if err != nil {
		t.Fatalf("encoding the state: %v", err)
	}
	var fields bson.M
	if err := bson.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("decoding the state: %v", err)
	}
	for _, name := range []string{"leases", "pending", "pending_bytes", "returned", "dead", "dead_bytes", "dead_seq", "lease_clock", "resume_at", "resume_below"} {
		if _, present := fields[name]; !present {
			t.Errorf("field %q is not in the encoded state %v", name, fields)
		}
	}
	var decoded BucketState
	if err := bson.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decoding the state into a BucketState: %v", err)
	}
	if decoded.Leases != 1 || decoded.ResumeBelow != 10 || decoded.DeadSeq != 7 {
		t.Errorf("the delivery fields did not round-trip: %+v", decoded)
	}
}

// TestResumeCursorTravelsAndClears: ResumeAt and ResumeBelow survive the state
// document, and neither is in it at zero, so a bucket with no armed run carries
// no byte of them. The last take of a run clears both, for fifo and lifo.
func TestResumeCursorTravelsAndClears(t *testing.T) {
	armed, err := bson.Marshal(BucketState{ResumeAt: 41, ResumeBelow: 3})
	if err != nil {
		t.Fatalf("encoding an armed state: %v", err)
	}
	var decoded BucketState
	if err := bson.Unmarshal(armed, &decoded); err != nil {
		t.Fatalf("decoding an armed state: %v", err)
	}
	if decoded.ResumeAt != 41 || decoded.ResumeBelow != 3 {
		t.Fatalf("an armed state decoded as resume_at %d, resume_below %d, want 41 and 3", decoded.ResumeAt, decoded.ResumeBelow)
	}
	idle, err := bson.Marshal(BucketState{})
	if err != nil {
		t.Fatalf("encoding a zero state: %v", err)
	}
	for _, name := range []string{"resume_at", "resume_below"} {
		if bytes.Contains(idle, []byte(name)) {
			t.Errorf("a zero state encodes %q: %x", name, idle)
		}
	}

	// The clear half: arm a run by a write-back, take it, and neither field
	// is left in the encoded document.
	for _, mode := range []QueueMode{QueueFIFO, QueueLIFO} {
		st := BucketState{Head: 5, Tail: 5, Next: 6, Entries: 1}
		position := uint64(2)
		if mode == QueueLIFO {
			position = 8
		}
		st.writtenBack(mode, position)
		if st.ResumeBelow != 1 {
			t.Fatalf("%s: a write-back at %d did not arm the cursor: %+v", modeName(mode), position, st)
		}
		if !st.tookLive(mode, position) {
			t.Fatalf("%s: taking the run's only entry did not report the jump: %+v", modeName(mode), st)
		}
		if st.ResumeAt != 0 || st.ResumeBelow != 0 {
			t.Errorf("%s: the cursor is armed at resume_at %d, resume_below %d after the run was taken", modeName(mode), st.ResumeAt, st.ResumeBelow)
		}
		cleared, err := bson.Marshal(st)
		if err != nil {
			t.Fatalf("encoding the cleared state: %v", err)
		}
		for _, name := range []string{"resume_at", "resume_below"} {
			if bytes.Contains(cleared, []byte(name)) {
				t.Errorf("%s: a cleared state still encodes %q", modeName(mode), name)
			}
		}
	}
}

// TestPopNStateCountsOnlyEntries is CA-2, the test pop-n's build doc moved
// here: the cursor-lost rule compares what a walk found with Entries, which
// counts live 0x04 keys only. A claimed entry is counted in Pending, so a
// rule that added it would call a healthy bucket's pop lost. n is above
// Entries on purpose: the rule's min is n otherwise and cannot fire.
func TestPopNStateCountsOnlyEntries(t *testing.T) {
	for _, engine := range bucketPopNEngines() {
		t.Run(engine.name, func(t *testing.T) {
			subject := engine.build(t)
			subject.push(0, 2)
			plantPendingCount(t, subject, 3)
			visited, removed, err := popNCollect(subject, ApplyInfo{}, 5)
			if err != nil {
				t.Fatalf("PopN(5) over Entries 2 and Pending 3 = %v, want the two entries taken", err)
			}
			if removed != 2 || len(visited) != 2 {
				t.Fatalf("PopN(5) removed %d and visited %d, want 2 and 2", removed, len(visited))
			}
		})
	}
}

// plantPendingCount sets the state document's Pending to pending beside the
// live 0x04 keys, the way a claim leaves it, without moving any key.
func plantPendingCount(t *testing.T, subject popNSubject, pending int64) {
	t.Helper()
	snap := subject.kv.NewSnapshot()
	st, err := LoadBucketState(snap, 1, 1, 1)
	snap.Discard()
	if err != nil {
		t.Fatalf("loading the state: %v", err)
	}
	st.Pending = pending
	batch := subject.kv.NewBatch()
	defer batch.Discard()
	if err := st.Put(batch, 1, 1, 1); err != nil {
		t.Fatalf("encoding the planted state: %v", err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("committing the planted state: %v", err)
	}
}
