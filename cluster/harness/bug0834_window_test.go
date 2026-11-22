package harness_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// bug0834DataDelay holds the owner's replication frames to the joiner back for longer than the
// test needs, so the joiner cannot catch up before the owner is killed.
const bug0834DataDelay = 5 * time.Second

// bug0834WindowSetup is the window's construction: the owner's data-channel writes to the
// joiner are delayed from before the activation, so the joiner is still behind the owner when
// the activation converges, and the old copy has retired out of the copy set holding every
// acknowledged push.
func bug0834WindowSetup(t *testing.T) *bug0834World {
	t.Helper()
	w := bug0834SetupWith(t, harness.Options{FastDetection: true, WCTimeout: 5 * time.Second}, func(w *bug0834World) {
		w.c.DelayChannel(w.owner, w.joiner, transport.ChannelData, bug0834DataDelay)
		t.Logf("owner %s to joiner %s data writes delayed %s from before the activation", w.owner, w.joiner, bug0834DataDelay)
	})
	w.requireJoinerBehind(t, "after the activation")
	// The record the promotion must read from is the retired one: the activation took the old
	// copy out of the copy set, and its gate cannot pass while the joiner is behind.
	w.c.Await("the old copy to retire its record of the slot", 30*time.Second, func() bool {
		return w.c.Node(w.oldCopy).SlotRecordState(w.slot, w.dbID) == machine.SlotRetired
	})
	// The wait above can outlast bug0834DataDelay on a slow host: checked again here, just
	// before the caller's kill, so a joiner that caught up meanwhile fails the construction
	// instead of passing on the steady state.
	pos := w.requireJoinerBehind(t, "before the kill")
	oldPos, _, _ := w.c.Node(w.oldCopy).PairDump(w.slot, w.dbID)
	t.Logf("before the kill: joiner %s at %s, owner %s at %s; old copy %s retired at %s", w.joiner, pos, w.owner, w.ownerPos, w.oldCopy, oldPos)
	return w
}

// requireJoinerBehind fails the test when the joiner has reached the owner's position: the
// window is then closed and nothing the test does next can show a loss.
func (w *bug0834World) requireJoinerBehind(t *testing.T, when string) cluster.Position {
	t.Helper()
	pos, _, _ := w.c.Node(w.joiner).PairDump(w.slot, w.dbID)
	if !pos.Less(w.ownerPos) {
		t.Fatalf("the construction failed %s: the joiner %s is already at %s, the owner's position %s; the delay did not hold the catch-up back", when, w.joiner, pos, w.ownerPos)
	}
	return pos
}

// TestAnOwnerKilledBeforeAJoinersCatchUpKeepsEveryAcknowledgedPush (BUG-0834, the window the
// owner-ships-its-tails fix leaves): the owner is killed the moment the activation has
// converged, so the joiner is still behind when it is promoted. The retired copy holds every
// acknowledged push; promotion must take its position and catch up from it before serving.
// Before the promotion rule asked the retired holder (Handoff.Target, Node.catchUpSource), the
// new owner opened at 0:0 and served "is not in" for an acknowledged document.
func TestAnOwnerKilledBeforeAJoinersCatchUpKeepsEveryAcknowledgedPush(t *testing.T) {
	w := bug0834WindowSetup(t)
	w.killOwnerAndAudit(t)
}

// TestARetiredHolderIsNotDeletedWhileItHoldsTheOnlyCopy (BUG-0834, the retire gate): the same
// window, then the dead owner is declared dead, which ranks the old copy into the copy set
// again, and the new owner orders a write of its own. The old copy's deletion gate accepts a
// position in a later epoch as covering its fence (machine.slotCovers), so a new owner that
// opened below the old copy would let it delete the only copy of the acknowledged pushes. After
// all that, the pushes must still be held by a running member and served.
func TestARetiredHolderIsNotDeletedWhileItHoldsTheOnlyCopy(t *testing.T) {
	w := bug0834WindowSetup(t)
	bug0834Retry(t, "committing a short dead_ms", func(ctx context.Context) error {
		return w.c.Node(w.c.AwaitLeader(30*time.Second)).SetSetting(ctx, "dead_ms", 3000)
	})
	w.requireJoinerBehind(t, "after committing dead_ms, before the kill")
	w.c.Kill(w.owner)
	w.c.Await("the survivors to commit the owner dead", 90*time.Second, func() bool {
		for _, id := range w.c.Running() {
			if !bug0834MemberIs(w.c.Node(id).State(), w.owner, cluster.MemberDead) {
				return false
			}
		}
		return true
	})
	w.c.AwaitLeader(30 * time.Second)
	w.c.AwaitLeases(30 * time.Second)
	other := bug0834SameSlotID(t, w.slot, w.id)
	bug0834Retry(t, "inserting a document of the same slot through the new owner", func(ctx context.Context) error {
		_, err := w.c.Node(w.joiner).Insert(ctx, bug0834DB, bug0834Bucket, bson.D{{Key: "_id_", Value: other}, {Key: "Name", Value: other}})
		return err
	})
	// The old copy's gate runs on its own clock: give it the time to pass, which a new owner's
	// write in the later epoch lets it do, and the old copy, ranked back into the copy set, the
	// time to finish its delete and seed the pair again from the new owner.
	w.c.Await("the old copy to be a ready copy of the slot again", 120*time.Second, func() bool {
		return w.c.Node(w.oldCopy).SlotRecordState(w.slot, w.dbID) == machine.SlotReady
	})
	holders := 0
	for _, id := range w.c.Running() {
		pos, entries, err := w.c.Node(id).PairDump(w.slot, w.dbID)
		var joined []byte
		for _, entry := range entries {
			joined = append(joined, entry.Value...)
		}
		notOnce := w.heldOnce(joined)
		t.Logf("after the new owner's write: %s holds slot %d at %s in record state %d, pushes not held once %d (%v)",
			id, w.slot, pos, w.c.Node(id).SlotRecordState(w.slot, w.dbID), notOnce, err)
		if err == nil && notOnce == 0 {
			holders++
		}
	}
	if holders == 0 {
		t.Fatalf("PERMANENT LOSS: no running member holds the %d acknowledged pushes after the old copy %s seeded the slot again", len(w.acked), w.oldCopy)
	}
	w.auditServed(t, 30*time.Second)
}

// bug0834MemberIs reports whether state lists id in want.
func bug0834MemberIs(state cluster.ClusterState, id string, want cluster.MemberState) bool {
	for _, member := range state.Members {
		if member.ID == id {
			return member.State == want
		}
	}
	return false
}

// bug0834SameSlotID returns a document id other than taken that is placed on slot.
func bug0834SameSlotID(t *testing.T, slot uint16, taken string) string {
	t.Helper()
	for i := range 1_000_000 {
		candidate := fmt.Sprintf("s-%d", i)
		placed, err := wire.PlacementOf(bug0834DB, bug0834Bucket, candidate)
		if err != nil {
			t.Fatal(err)
		}
		if placed == slot && candidate != taken {
			return candidate
		}
	}
	t.Fatalf("no document id of a million lands on slot %d", slot)
	return ""
}
