package cluster

import (
	"bytes"
	"errors"
	"testing"
)

// flaggedFSM is an FSM that has refused one committed entry (an unread field), so its flag is
// set and its state is frozen at the returned state_seq.
func flaggedFSM(t *testing.T) (f *clusterFSM, frozenSeq int64) {
	t.Helper()
	f = divergedFSM(t)
	frozenSeq = f.state.StateSeq
	bad := divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(frozenSeq), Body: unreadFieldBody("x", 9)})
	if out := f.Apply(divergedEntry(30, bad)).(applyOutcome); out.err == nil {
		t.Fatal("the refused entry was accepted")
	}
	if f.diverged.Load() == nil {
		t.Fatal("the refused entry did not set the flag")
	}
	return f, frozenSeq
}

// TestADivergedFSMAppliesNothingMore: once the flag is set an entry whose BaseSeq equals the
// frozen StateSeq, which would be accepted on a healthy node, is refused and the state does not
// move, so the first record keeps describing a state that nothing has changed. Break: remove the
// short-circuit at the top of Apply.
func TestADivergedFSMAppliesNothingMore(t *testing.T) {
	f, frozen := flaggedFSM(t)
	before := fsmEncoded(t, f.state)
	good := divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(frozen), Body: fsmCreateDatabaseBody("later", f.state.NextDatabaseID, 1)})
	out := f.Apply(divergedEntry(31, good)).(applyOutcome)
	if out.err == nil {
		t.Fatal("a flagged node applied an entry stamped at its frozen state_seq")
	}
	var refusal *Error
	if !errors.As(out.err, &refusal) || refusal.Code != CodeUnavailable {
		t.Fatalf("the refusal is %v, want UNAVAILABLE naming the divergence", out.err)
	}
	if !bytes.Equal(before, fsmEncoded(t, f.state)) || f.state.StateSeq != frozen {
		t.Fatalf("a flagged node's state moved from state_seq %d to %d", frozen, f.state.StateSeq)
	}
	if f.applied.Load() != 31 {
		t.Fatalf("the applied index is %d, want 31: the library must see the entry as handled", f.applied.Load())
	}
	if got := f.diverged.Load(); got.Index != 30 {
		t.Fatalf("the record names index %d, want the first refused entry, 30", got.Index)
	}
}

// TestAFlaggedNodeNeverRealignsItsCounterWithTheCluster is the G1 review's MINOR-6: a flagged node
// that kept applying entries stamped at its frozen StateSeq would reach the cluster's counter
// with a different state (the realigned node of section 3.3). A run of commands, each stamped
// at the counter the node would have reached, must leave StateSeq and the state where the first
// refusal froze them. Break: remove the short-circuit in Apply (the counter then climbs).
func TestAFlaggedNodeNeverRealignsItsCounterWithTheCluster(t *testing.T) {
	f, frozen := flaggedFSM(t)
	before := fsmEncoded(t, f.state)
	for i := range 5 {
		seq := uint64(f.state.StateSeq) // the stamp a healthy node's next command carries
		raw := divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: seq, Body: fsmCreateDatabaseBody("db"+string(rune('a'+i)), f.state.NextDatabaseID, 1)})
		f.Apply(divergedEntry(uint64(40+i), raw))
	}
	if f.state.StateSeq != frozen || !bytes.Equal(before, fsmEncoded(t, f.state)) {
		t.Fatalf("a flagged node's counter moved from %d to %d: it can realign with the cluster holding another state", frozen, f.state.StateSeq)
	}
}
