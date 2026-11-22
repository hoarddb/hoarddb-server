package wire

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

func stepTwoBTable(states map[string]cluster.MemberState) *cluster.SlotTable {
	var state cluster.ClusterState
	for id, memberState := range states {
		state.Members = append(state.Members, cluster.Member{ID: id, State: memberState})
	}
	return cluster.BuildTable(state)
}

// TestCatchUpIsServedToACopyOfThePairAtTheCommittedOrThePreviousEpoch: option C of S2B-5.
// [GUARD: serve every member; ignore the previous epoch; ignore the committed one]
func TestCatchUpIsServedToACopyOfThePairAtTheCommittedOrThePreviousEpoch(t *testing.T) {
	active := map[string]cluster.MemberState{"a": cluster.MemberActive, "b": cluster.MemberActive, "c": cluster.MemberActive, "d": cluster.MemberActive}
	now := stepTwoBTable(active)
	const slot = 77
	copies := now.Copies(slot, 2)
	copyID := copies[0].ID
	var outsider string
	for _, member := range now.Copies(slot, 4) {
		if member.ID != copies[0].ID && member.ID != copies[1].ID {
			outsider = member.ID
			break
		}
	}
	if !catchUpEligible(now, nil, 2, 0, slot, copyID) {
		t.Errorf("a copy of the pair was refused")
	}
	if !catchUpEligible(now, nil, 2, 0, slot, copies[1].ID) {
		t.Errorf("the second copy (the new owner while a barrier is open) was refused")
	}
	if catchUpEligible(now, nil, 2, 0, slot, outsider) {
		t.Errorf("a member that holds no copy of the pair was served")
	}
	if catchUpEligible(now, nil, 2, 0, slot, "ghost") {
		t.Errorf("an unknown node was served")
	}

	// The epoch before: the outsider was a copy then and has not applied the change.
	older := map[string]cluster.MemberState{"a": cluster.MemberActive, "b": cluster.MemberActive, "c": cluster.MemberActive, "d": cluster.MemberActive}
	older[copyID] = cluster.MemberDead // before, the current first copy did not exist; the ranking differs
	before := stepTwoBTable(older)
	var formerCopy string
	for _, member := range before.Copies(slot, 2) {
		if !catchUpEligible(now, nil, 2, 0, slot, member.ID) {
			formerCopy = member.ID
		}
	}
	if formerCopy == "" {
		t.Skip("the two rankings share their copy set at this slot; pick another slot")
	}
	if !catchUpEligible(now, before, 2, 2, slot, formerCopy) {
		t.Errorf("a copy at the previous epoch was refused during an epoch change")
	}
	if catchUpEligible(now, nil, 2, 0, slot, formerCopy) {
		t.Errorf("a former copy was served with no previous table")
	}
}

// TestAGainerThatIsStillJoiningIsNotServedCatchUp: a joining member is in no ranking, so it
// is refused here and uses the seed channel. [GUARD: serve every member]
func TestAGainerThatIsStillJoiningIsNotServedCatchUp(t *testing.T) {
	table := stepTwoBTable(map[string]cluster.MemberState{"a": cluster.MemberActive, "b": cluster.MemberActive, "j": cluster.MemberJoining})
	for slot := uint16(0); slot < 50; slot++ {
		if catchUpEligible(table, nil, 2, 0, slot, "j") {
			t.Fatalf("a joining member was served catch-up at slot %d", slot)
		}
	}
}
