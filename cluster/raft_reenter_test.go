package cluster

import (
	"context"
	"slices"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

// TestAReplacedVoterIsNotCountedAtItsOldMatchIndex: [GUARD BUG-0870 replace, the Raft re-entry
// (design section 5.2 step 6): a voter whose node lost its log is taken out of the configuration and
// added back as a nonvoter, so the leader's count of its acknowledgements starts again from nothing
// and it does not vote until it is promoted. The library keeps a voter's match index while the
// voter stays in the configuration (commitment.match never lowers it) and rebuilds the count, voters
// only, on every configuration change (commitment.setConfiguration); AddNonvoter of a server that is
// already a voter leaves it a voter, so the removal is what resets the count]. Here the evidence is
// the count itself: once the re-entered node stops, the leader still commits alone, which it could
// not if the node were still counted as a voter. Red when the re-entry only adds the nonvoter.
func TestAReplacedVoterIsNotCountedAtItsOldMatchIndex(t *testing.T) {
	leader := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	second, cfg := joinSecond(t, leader)
	if _, err := leader.Promote("n2"); err != nil {
		t.Fatalf("promote: %v", err)
	}
	createDatabase(t, leader, "before", 1) // n2 acknowledges entries as a voter
	if err := leader.ReenterAsJoiner("n2", cfg.Listener.Addr().String()); err != nil {
		t.Fatalf("re-entering n2: %v", err)
	}
	conf, err := leader.configuration()
	if err != nil {
		t.Fatal(err)
	}
	for _, server := range conf.Servers {
		if string(server.ID) == "n2" && server.Suffrage != hraft.Nonvoter {
			t.Fatalf("after the re-entry n2 is a %v in the configuration, want a nonvoter", server.Suffrage)
		}
	}
	if voters := leader.State().Voters; slices.Contains(voters, "n2") {
		t.Fatalf("after the re-entry the committed voters are %v; n2 must not vote until promoted", voters)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := leader.ProposeContext(ctx, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(leader.State().StateSeq),
		Body: fsmCreateDatabaseBody("after", 2, 1)}); err != nil {
		t.Fatalf("with the re-entered node stopped the leader could not commit alone, so it still counts n2 as a voter: %v", err)
	}
}
