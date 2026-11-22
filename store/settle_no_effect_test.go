package store

import (
	"errors"
	"reflect"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// recordedFailureAt is a store holding a bucket whose sequence 2 is a recorded
// failure beneath its mark at 3, as a failed pre-write read leaves it after a
// later write applied.
func recordedFailureAt(t *testing.T) (*kvfault.Store, *OwedApplies) {
	t.Helper()
	kv := kvfault.New()
	var owed OwedApplies
	owed.CarryFailure(ApplyInfo{Seq: 2, Record: true})
	if _, err := applyAt(kv, &owed, ApplyInfo{Seq: 3, Record: true}, putOp("later")); err != nil {
		t.Fatalf("the write that records the failure: %v", err)
	}
	if st := stateOf(t, kv); len(st.FailedSeqs) != 1 || st.FailedSeqs[0] != 2 || st.AppliedSeq != 3 {
		t.Fatalf("test setup: state = %+v, want failed_seqs [2] and mark 3", st)
	}
	return kv, &owed
}

func settleRequest(kv storage.KV, owed *OwedApplies, info ApplyInfo) ApplyRequest {
	return ApplyRequest{KV: kv, Slot: applyTestSlot, DatabaseID: 1, ID: applyTestID, Info: info, Owed: owed}
}

// TestUnreplayableIsResetByTheCallsThatRunBesideApply: the server logs the
// "can never be replayed" ERROR from the list the last apply left, on every
// call that follows, so a call that does not run Apply must clear it as Apply
// does or the line repeats.
func TestUnreplayableIsResetByTheCallsThatRunBesideApply(t *testing.T) {
	t.Run("CarryFailure", func(t *testing.T) {
		var owed OwedApplies
		owed.unreplayable = []uint64{5}
		owed.CarryFailure(ApplyInfo{Seq: 8, Record: true})
		if len(owed.Unreplayable()) != 0 {
			t.Fatalf("after CarryFailure Unreplayable = %v, want none", owed.Unreplayable())
		}
	})
	t.Run("SettleNoEffect", func(t *testing.T) {
		kv, owed := recordedFailureAt(t)
		owed.unreplayable = []uint64{5}
		if err := SettleNoEffect(settleRequest(kv, owed, ApplyInfo{Seq: 2, Replay: true, Record: true})); err != nil {
			t.Fatalf("SettleNoEffect: %v", err)
		}
		if len(owed.Unreplayable()) != 0 {
			t.Fatalf("after SettleNoEffect Unreplayable = %v, want none", owed.Unreplayable())
		}
	})
}

// TestSettleNoEffectConsumesOnlyAReplayedRecordedFailure pins both guards: a
// live entry and a sequence that is no recorded failure leave the state
// exactly as it was. The carried 7 shows it: any batch that wrote the state
// would fold it into failed_seqs.
func TestSettleNoEffectConsumesOnlyAReplayedRecordedFailure(t *testing.T) {
	for _, tt := range []struct {
		name string
		info ApplyInfo
	}{
		{name: "not a replay", info: ApplyInfo{Seq: 2, Record: true}},
		{name: "not a recorded failure", info: ApplyInfo{Seq: 1, Replay: true, Record: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			kv, owed := recordedFailureAt(t)
			owed.CarryFailure(ApplyInfo{Seq: 7, Record: true})
			if err := SettleNoEffect(settleRequest(kv, owed, tt.info)); err != nil {
				t.Fatalf("SettleNoEffect: %v", err)
			}
			if st := stateOf(t, kv); !reflect.DeepEqual(st.FailedSeqs, []uint64{2}) || st.AppliedSeq != 3 {
				t.Fatalf("state = %+v, want it untouched: failed_seqs [2], mark 3", st)
			}
		})
	}
}

// TestSettleNoEffectSettlesAReplayedRecordedFailure: quietly for an update of
// one document, and for a filtered one with ErrNotReplayedOutOfOrder beneath
// the mark (SA-27), the record settled either way.
func TestSettleNoEffectSettlesAReplayedRecordedFailure(t *testing.T) {
	replayed := ApplyInfo{Seq: 2, Replay: true, Record: true}
	t.Run("quiet", func(t *testing.T) {
		kv, owed := recordedFailureAt(t)
		if err := SettleNoEffect(settleRequest(kv, owed, replayed)); err != nil {
			t.Fatalf("SettleNoEffect: %v", err)
		}
		if st := stateOf(t, kv); len(st.FailedSeqs) != 0 {
			t.Fatalf("failed_seqs = %v, want none", st.FailedSeqs)
		}
	})
	t.Run("refused beneath the mark", func(t *testing.T) {
		kv, owed := recordedFailureAt(t)
		err := RefuseNoEffectBeneathMark(settleRequest(kv, owed, replayed))
		if !errors.Is(err, ErrNotReplayedOutOfOrder) {
			t.Fatalf("RefuseNoEffectBeneathMark = %v, want ErrNotReplayedOutOfOrder", err)
		}
		if st := stateOf(t, kv); len(st.FailedSeqs) != 0 {
			t.Fatalf("failed_seqs = %v, want none: the refusal settles the record", st.FailedSeqs)
		}
	})
}

// TestRefuseRecordedFailureRefusesAboveTheMarkToo: a recorded failure the
// handler refused at replay is reported wherever the mark is (BUG-0586), unlike
// a filtered no-effect replay, which is quiet above it.
func TestRefuseRecordedFailureRefusesAboveTheMarkToo(t *testing.T) {
	aboveTheMark := func(t *testing.T) (*kvfault.Store, *OwedApplies) {
		t.Helper()
		kv := kvfault.New()
		var owed OwedApplies
		owed.CarryFailure(ApplyInfo{Seq: 5, Record: true})
		if _, err := applyAt(kv, &owed, ApplyInfo{Seq: 3, Record: true}, putOp("earlier")); err != nil {
			t.Fatalf("the write that records the failure: %v", err)
		}
		if st := stateOf(t, kv); !reflect.DeepEqual(st.FailedSeqs, []uint64{5}) || st.AppliedSeq != 3 {
			t.Fatalf("test setup: state = %+v, want failed_seqs [5] above the mark 3", st)
		}
		return kv, &owed
	}
	replayed := ApplyInfo{Seq: 5, Replay: true, Record: true}

	kv, owed := aboveTheMark(t)
	if err := RefuseNoEffectBeneathMark(settleRequest(kv, owed, replayed)); err != nil {
		t.Fatalf("RefuseNoEffectBeneathMark above the mark = %v, want nil (quiet)", err)
	}

	kv, owed = aboveTheMark(t)
	err := RefuseRecordedFailure(settleRequest(kv, owed, replayed))
	if !errors.Is(err, ErrNotReplayedOutOfOrder) {
		t.Fatalf("RefuseRecordedFailure above the mark = %v, want ErrNotReplayedOutOfOrder", err)
	}
	if st := stateOf(t, kv); len(st.FailedSeqs) != 0 {
		t.Fatalf("failed_seqs = %v, want none: the refusal settles the record", st.FailedSeqs)
	}
}
