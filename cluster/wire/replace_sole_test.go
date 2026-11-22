package wire

import (
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
)

// soleState is four active voters n1..n4 and database "app" (id 1) at RF 2, with n2 committed down:
// the member a replace-node would take the copies of.
func soleState(t *testing.T) cluster.ClusterState {
	t.Helper()
	st := threeNodes(t)
	register(t, &st, "n4", 1)
	setState(t, &st, "n4", "active")
	setState(t, &st, "n2", "down")
	return st
}

// ownedBy lists the slots of reconDB whose RF 2 copy set names node, ranked as if node were active.
func ownedBy(st cluster.ClusterState, node string) []int {
	table := cluster.BuildTable(st)
	var slots []int
	for slot := range storage.SlotCount {
		for _, copyOf := range table.Copies(uint16(slot), 2) {
			if copyOf.ID == node {
				slots = append(slots, slot)
			}
		}
	}
	return slots
}

// sharedBy lists the slots of reconDB whose RF 2 copy set is exactly a and b.
func sharedBy(st cluster.ClusterState, a, b string) []int {
	table := cluster.BuildTable(st)
	var slots []int
	for slot := range storage.SlotCount {
		copies := table.Copies(uint16(slot), 2)
		if len(copies) == 2 && ((copies[0].ID == a && copies[1].ID == b) || (copies[0].ID == b && copies[1].ID == a)) {
			slots = append(slots, slot)
		}
	}
	return slots
}

// allSlots is every slot of the table.
func allSlots() []int {
	slots := make([]int, storage.SlotCount)
	for i := range slots {
		slots[i] = i
	}
	return slots
}

// TestReplaceCountsAnOwnedPairWhoseBitIsNotReportedYet: [GUARD BUG-0870 review B-1: a pair the
// target owns by placement, whose SEEDED bit nobody has committed yet (the reporter batches its
// bits), is a pair only the target may hold: replace-node without --accept-loss must count it].
// Red while soleCopiesOf walks only the bits the target has committed.
func TestReplaceCountsAnOwnedPairWhoseBitIsNotReportedYet(t *testing.T) {
	st := soleState(t)
	owned := ownedBy(st, testSelf)
	if len(owned) == 0 {
		t.Fatal("n2 owns no pair; the test proves nothing")
	}
	if got := soleCopiesOf(st, testSelf); got.pairs != len(owned) || got.database != "app" || got.databases != 1 {
		t.Fatalf("with no bit committed by anyone soleCopiesOf = %+v, want the %d pairs n2 owns in app", got, len(owned))
	}
	// Every other member reports every slot: nothing is held only by n2.
	for _, other := range []string{"n1", "n3", "n4"} {
		withBitsOf(t, &st, other, allSlots()...)
	}
	if got := soleCopiesOf(st, testSelf); got.pairs != 0 {
		t.Fatalf("with every other member's bit committed soleCopiesOf = %+v, want none", got)
	}
}

// TestReplaceCountsNoCopyOnADownOrDeadMember: [GUARD BUG-0870 review B-3: only an active or joining
// member's committed bit is a copy replace-node may rely on; a down member may never come back, and a
// dead one will not, so a pair whose other copy is on a down or dead member counts as only the
// target's]. BREAK: count a down holder (M9) fails the down half; count a dead holder (M5) fails the
// dead half.
func TestReplaceCountsNoCopyOnADownOrDeadMember(t *testing.T) {
	st := soleState(t)
	for _, other := range []string{"n1", "n3", "n4"} {
		withBitsOf(t, &st, other, ownedBy(st, other)...) // each reports the pairs it holds
	}
	if got := soleCopiesOf(st, testSelf); got.pairs != 0 {
		t.Fatalf("with every other copy reported soleCopiesOf = %+v, want none", got)
	}
	shared := sharedBy(st, testSelf, "n3")
	if len(shared) == 0 {
		t.Fatal("no pair has the copy set {n2, n3}; the test proves nothing")
	}
	setState(t, &st, "n3", "down")
	if got := soleCopiesOf(st, testSelf); got.pairs != len(shared) {
		t.Fatalf("with n3 down soleCopiesOf = %+v, want the %d pairs n2 shares only with n3", got, len(shared))
	}
	setState(t, &st, "n3", "dead")
	if got := soleCopiesOf(st, testSelf); got.pairs != len(shared) {
		t.Fatalf("with n3 dead soleCopiesOf = %+v, want the %d pairs n2 shares only with n3", got, len(shared))
	}
}

// TestReplaceCountsTheOwnedPairsOfADeadTarget: [GUARD BUG-0870 review B-1: a dead target is not
// ranked, so its pairs are found by ranking it as if it were active (its id ranks as it always
// did)]. BREAK: build the table from the committed state as it is.
func TestReplaceCountsTheOwnedPairsOfADeadTarget(t *testing.T) {
	st := soleState(t)
	owned := ownedBy(st, testSelf)
	setState(t, &st, testSelf, "dead")
	if got := soleCopiesOf(st, testSelf); got.pairs != len(owned) {
		t.Fatalf("a dead n2 with no bit committed by anyone: soleCopiesOf = %+v, want its %d pairs", got, len(owned))
	}
}

// TestReplaceCountsAJoiningCopyOnlyOnceItsBitIsCommitted: [GUARD BUG-0870 re-review F-3 (a), (b),
// (c): a joining member's committed bit is a copy (the reporter commits it only once the joiner's
// record of the pair is ready); a joining member with no committed bit is none; a witness's bit is
// never a copy]. BREAK: drop joining holders (a), count every joining member as a holder (b), or
// count witnesses (c).
func TestReplaceCountsAJoiningCopyOnlyOnceItsBitIsCommitted(t *testing.T) {
	st := soleState(t)
	withBitsOf(t, &st, "n3", allSlots()...)
	if got := soleCopiesOf(st, testSelf); got.pairs != 0 {
		t.Fatalf("an active n3 reports every pair, yet soleCopiesOf = %+v", got)
	}
	setState(t, &st, "n3", "joining")
	if got := soleCopiesOf(st, testSelf); got.pairs != 0 {
		t.Fatalf("(a) a joining n3 with every bit committed: soleCopiesOf = %+v, want none", got)
	}
	setState(t, &st, "n3", "down")
	replace(t, &st, "n3", 2) // joining again, its bits cleared
	if got, owned := soleCopiesOf(st, testSelf), ownedBy(st, testSelf); got.pairs != len(owned) {
		t.Fatalf("(b) a joining n3 with no bit committed: soleCopiesOf = %+v, want n2's %d pairs", got, len(owned))
	}
	apply(t, &st, cluster.CommandRegisterMember, bsonDoc(bsonStr("id", "w5"), bsonI64("incarnation", 1),
		bsonStr("data_addr", "w5:1"), bsonBool("witness", true), bsonBin("placing_set", make([]byte, 1024)), bsonLevels()))
	setState(t, &st, "w5", "active")
	withBitsOf(t, &st, "w5", allSlots()...)
	if got, owned := soleCopiesOf(st, testSelf), ownedBy(st, testSelf); got.pairs != len(owned) {
		t.Fatalf("(c) an active witness with every bit committed: soleCopiesOf = %+v, want n2's %d pairs", got, len(owned))
	}
}

// TestTheLossGateRefusesOrNamesTheLoss: [GUARD BUG-0870 re-review F-1: the R4 gate is one function
// for the first replace and for a repeat that clears a held joining node's bits: without
// accept_loss a sole copy is refused with the R4 text; with it the loss is returned to be named].
func TestTheLossGateRefusesOrNamesTheLoss(t *testing.T) {
	st := soleState(t)
	setState(t, &st, testSelf, "joining")
	withBitsOf(t, &st, testSelf, ownedBy(st, "n3")...) // n2 seeded n3's pairs; n3 is down
	setState(t, &st, "n3", "down")
	if _, err := replaceLossGate(st, testSelf, "n2:1", false); err == nil {
		t.Fatal("a joining n2 holding the only reported copy passed the gate without accept_loss; want the R4 refusal")
	}
	_, err := replaceLossGate(st, testSelf, "n2:1", false)
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument || !strings.Contains(refusal.Message, "--accept-loss") {
		t.Fatalf("the refusal is %v, want INVALID_ARGUMENT naming --accept-loss", err)
	}
	if sole, err := replaceLossGate(st, testSelf, "n2:1", true); err != nil || sole.pairs == 0 {
		t.Fatalf("with accept_loss the gate = %+v, %v; want the loss and no error", sole, err)
	}
}
