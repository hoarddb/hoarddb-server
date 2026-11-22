package store

import (
	"errors"
	"slices"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

func flView(t *testing.T, kv storage.KV, ref FieldRef, lease [leaseIDBytes]byte, now int64, maxBytes int) (LeaseView, error) {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	return FieldLeaseElements(snap, ref, LeaseArgs{LeaseID: lease, Now: now}, maxBytes)
}

func flTexts(t *testing.T, view LeaseView) []string {
	t.Helper()
	var texts []string
	for _, entry := range view.Entries {
		texts = append(texts, fdText(t, entry))
	}
	return texts
}

func TestFieldLeaseElementsReturnsTheLeaseInClaimOrder(t *testing.T) {
	for _, tc := range []struct {
		engine FieldEngine
		want   []string
	}{{FieldFIFO, []string{"a", "b"}}, {FieldLIFO, []string{"d", "c"}}} {
		kv := fdOpen(t, t.TempDir())
		ref := fdRef(t, tc.engine)
		fdPush(t, kv, ref, "a", "b", "c", "d")
		claim, claimed := fdClaim(t, kv, ref, fdArgs(2, 1000, 1))
		view, err := flView(t, kv, ref, claim.LeaseID, 1000, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if !view.Live || view.Deadline != claim.Deadline || !slices.Equal(flTexts(t, view), claimed) || !slices.Equal(claimed, tc.want) {
			t.Errorf("%s: view %+v texts %v, claimed %v, want %v", tc.engine, view, flTexts(t, view), claimed, tc.want)
		}
		_ = kv.Close()
	}
}

func TestFieldLeaseElementsIsNotLiveAfterAckReleaseOrExpiry(t *testing.T) {
	kv := fdOpen(t, t.TempDir())
	defer func() { _ = kv.Close() }()
	ref := fdRef(t, FieldFIFO)
	fdPush(t, kv, ref, "a", "b", "c")
	acked, _ := fdClaim(t, kv, ref, fdArgs(1, 1000, 1))
	released, _ := fdClaim(t, kv, ref, fdArgs(1, 1000, 2))
	lapsing, _ := fdClaim(t, kv, ref, fdArgs(1, 1000, 3))
	fdAck(t, kv, ref, acked.LeaseID, 1000)
	fdLeaseVerb(t, kv, func(snap storage.KVSnapshot, batch storage.KVBatch) (LeaseResult, error) {
		return FieldRelease(snap, batch, ref, LeaseArgs{LeaseID: released.LeaseID, Now: 1000})
	})
	for name, tc := range map[string]struct {
		lease [leaseIDBytes]byte
		now   int64
		live  bool
	}{
		"acked": {acked.LeaseID, 1000, false}, "released": {released.LeaseID, 1000, false},
		"unknown": {[leaseIDBytes]byte{9}, 1000, false},
		"live":    {lapsing.LeaseID, 1000, true}, "past its deadline": {lapsing.LeaseID, lapsing.Deadline, false},
		"far past": {lapsing.LeaseID, lapsing.Deadline + 5, false},
		// A clock that jumped back puts the deadline further ahead than any lease may be:
		// the return step has returned it, so a read must not show it.
		"deadline beyond the longest lease": {lapsing.LeaseID, lapsing.Deadline - MaxLeaseMs - 1, false},
	} {
		view, err := flView(t, kv, ref, tc.lease, tc.now, 1<<20)
		if err != nil || view.Live != tc.live || (!tc.live && len(view.Entries) != 0) {
			t.Errorf("%s: live=%v entries=%d err=%v, want live=%v", name, view.Live, len(view.Entries), err, tc.live)
		}
	}
}

func TestFieldLeaseElementsRefusesPastTheByteBoundWithoutCopyingIt(t *testing.T) {
	kv := fdOpen(t, t.TempDir())
	defer func() { _ = kv.Close() }()
	ref := fdRef(t, FieldFIFO)
	fdPush(t, kv, ref, "aaaaaaaaaa", "bbbbbbbbbb", "cccccccccc")
	claim, _ := fdClaim(t, kv, ref, fdArgs(3, 1000, 1))
	view, err := flView(t, kv, ref, claim.LeaseID, 1000, 30)
	if !errors.Is(err, ErrLeaseTooLarge) || len(view.Entries) != 0 {
		t.Fatalf("a bound under the lease's size: %+v, %v", view, err)
	}
}
