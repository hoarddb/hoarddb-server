package cluster

import (
	"errors"
	"strings"
	"testing"
)

func casRefusal(t *testing.T, st *ClusterState, kind byte, body []byte, want Code) {
	t.Helper()
	before := fsmEncoded(t, *st)
	_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: kind, BaseSeq: uint64(st.StateSeq), Body: body})
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Code != want {
		t.Fatalf("kind 0x%02x: %v, want a %s refusal", kind, err, want)
	}
	if string(before) != string(fsmEncoded(t, *st)) {
		t.Fatalf("a refused command changed the state")
	}
}

func TestCreateDatabaseAppliesTheNamePolicyInTheFSM(t *testing.T) {
	st := fsmCluster(t)
	for name, want := range map[string]Code{
		"_system": CodeReservedName, "..": CodeInvalidArgument, "a/b": CodeInvalidArgument,
		"nul\x00": CodeInvalidArgument, strings.Repeat("d", 256): CodeInvalidArgument,
	} {
		body := fsmCreateDatabaseBody(name, st.NextDatabaseID, 1)
		casRefusal(t, &st, CommandCreateDatabase, body, want)
	}
	fsmApply(t, &st, CommandCreateDatabase, fsmCreateDatabaseBody(strings.Repeat("d", 255), st.NextDatabaseID, 1))
}

func TestCreateDatabaseRefusesMoreCopiesThanRankedDataMembers(t *testing.T) {
	st := fsmCluster(t)
	casRefusal(t, &st, CommandCreateDatabase, fsmCreateDatabaseBody("big", st.NextDatabaseID, 4), CodeRFUnsupported)
	casRefusal(t, &st, CommandCreateDatabase, fsmCreateDatabaseBody("huge", st.NextDatabaseID, 1<<30), CodeRFUnsupported)
	fsmApply(t, &st, CommandCreateDatabase, fsmCreateDatabaseBody("three", st.NextDatabaseID, 3))
}

// TestSetMemberStateIsACompareAndSet: a replay of a change finds the member in another state
// and is refused, so a retried proposal cannot undo a newer one.
func TestSetMemberStateIsACompareAndSet(t *testing.T) {
	st := fsmCluster(t)
	down := fsmDoc(fsmStr("id", "n3"), fsmStr("state", "down"), fsmStr("expect_state", "active"))
	fsmApply(t, &st, CommandSetMemberState, down)
	casRefusal(t, &st, CommandSetMemberState, down, CodeConflict) // down already
	// n3 comes back through a replace: older incarnation refused, newer applied with its bits cleared.
	fsmApply(t, &st, CommandSetMemberState, fsmDoc(fsmStr("id", "n3"), fsmStr("state", "joining"), fsmI64("incarnation", 2), fsmBool("clear_bits", true), fsmStr("expect_state", "down")))
	fsmApply(t, &st, CommandSetMemberState, fsmDoc(fsmStr("id", "n3"), fsmStr("state", "active"), fsmStr("expect_state", "joining")))
	fsmApply(t, &st, CommandSeeded, fsmNodeBitsBody("n3", 1, fsmBitsAt(9)))
	// The replace replayed after the re-seed would clear n3's bits again: it is refused.
	replay := fsmDoc(fsmStr("id", "n3"), fsmStr("state", "joining"), fsmI64("incarnation", 3), fsmBool("clear_bits", true), fsmStr("expect_state", "down"))
	casRefusal(t, &st, CommandSetMemberState, replay, CodeConflict)
	if len(st.SeedBitmaps) == 0 {
		t.Fatal("the re-seeded bits were cleared")
	}
}

func TestSetSettingIsACompareAndSet(t *testing.T) {
	st := fsmCluster(t)
	was := st.Settings.DeadMs
	move := fsmDoc(fsmStr("key", "dead_ms"), fsmI64("value", 1234), fsmI64("expect_value", was))
	fsmApply(t, &st, CommandSetSetting, move)
	casRefusal(t, &st, CommandSetSetting, move, CodeConflict)
	casRefusal(t, &st, CommandSetSetting, fsmDoc(fsmStr("key", "stale_reads"), fsmBool("value", true), fsmBool("expect_value", true)), CodeConflict)
	casRefusal(t, &st, CommandSetSetting, fsmDoc(fsmStr("key", "probe_failures"), fsmI32("value", 5), fsmI32("expect_value", 99)), CodeConflict)
	fsmApply(t, &st, CommandSetSetting, fsmDoc(fsmStr("key", "stale_reads"), fsmBool("value", true), fsmBool("expect_value", false)))
	fsmApply(t, &st, CommandSetSetting, fsmDoc(fsmStr("key", "dead_ms"), fsmI64("value", 99)))
}
