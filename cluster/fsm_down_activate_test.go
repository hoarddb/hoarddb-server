package cluster

import "testing"

// TestAnActivationOfADownMemberCarriesTheCommittedIncarnation is a guard: it passes before the
// BUG-0754 fix by design, because the FSM already accepts down to active. It is here because the
// bug's third acceptance (a down member at another incarnation is not activated by this path)
// rests on the compare-and-set on the committed row, and nothing else pins it.
func TestAnActivationOfADownMemberCarriesTheCommittedIncarnation(t *testing.T) {
	st := fsmCluster(t)
	fsmApply(t, &st, CommandRegisterMember, fsmRegisterBody("n4", 7, "n4:1", false))
	fsmApply(t, &st, CommandSetMemberState, fsmSetMemberStateBody("n4", "active"))
	fsmApply(t, &st, CommandSetMemberState, fsmSetMemberStateBody("n4", "down"))
	if row, _ := fsmMember(st, "n4"); row.State != MemberDown || row.Incarnation != 7 {
		t.Fatalf("n4 = %s at incarnation %d, want down at 7", row.State, row.Incarnation)
	}

	activate := func(incarnation int64) []byte {
		return fsmDoc(fsmStr("id", "n4"), fsmStr("state", "active"), fsmStr("expect_state", "down"), fsmI64("incarnation", incarnation))
	}
	casRefusal(t, &st, CommandSetMemberState, activate(6), CodeConflict)
	casRefusal(t, &st, CommandSetMemberState, activate(8), CodeConflict)

	epoch := st.Epoch
	fsmApply(t, &st, CommandSetMemberState, activate(7))
	row, _ := fsmMember(st, "n4")
	if row.State != MemberActive || row.Incarnation != 7 {
		t.Fatalf("n4 = %s at incarnation %d after the activation, want active at 7", row.State, row.Incarnation)
	}
	if st.Epoch != epoch+1 {
		t.Fatalf("epoch %d after the activation, want %d (rank 1 becomes rank 2)", st.Epoch, epoch+1)
	}
}
