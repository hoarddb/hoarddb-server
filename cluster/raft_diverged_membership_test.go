package cluster

import (
	"errors"
	"testing"
	"time"
)

// TestAFlaggedLeaderChangesNoRaftConfiguration (review m2): AddJoiner, Join, Promote, RemoveServer
// and the voter reconcile each look at the divergence flag before the library's configuration
// change, so a flagged leader (which is about to exit 70) neither adds, promotes nor removes a
// server and then fails its SET_VOTERS or REGISTER_MEMBER half way. Break: drop the check from one
// path (that path's subtest changes the configuration and fails).
func TestAFlaggedLeaderChangesNoRaftConfiguration(t *testing.T) {
	leaderCfg := quickRaft(t, t.TempDir(), "n1")
	leaderCfg.DataAddr = leaderCfg.Listener.Addr().String()
	leader := openOrFail(t, leaderCfg)
	followerCfg := quickRaft(t, t.TempDir(), "n2")
	followerCfg.Bootstrap = false
	followerAddr := followerCfg.Listener.Addr().String()
	follower := openOrFail(t, followerCfg)
	_ = follower
	if _, err := leader.Join(JoinRequest{Member: Member{ID: "n2", Incarnation: 1, DataAddr: followerAddr}, RaftAddr: followerAddr, PlacingSet: make([]byte, 1024)}); err != nil {
		t.Fatalf("join: %v", err)
	}
	spareCfg := quickRaft(t, t.TempDir(), "n3")
	spareAddr := spareCfg.Listener.Addr().String()
	_ = spareCfg.Listener.Close()

	servers := func() map[string]string {
		conf, err := leader.configuration()
		if err != nil {
			t.Fatal(err)
		}
		suffrage := map[string]string{}
		for _, server := range conf.Servers {
			suffrage[string(server.ID)] = server.Suffrage.String()
		}
		return suffrage
	}
	before := servers()
	voters := leader.State().Voters

	leader.fsm.diverged.Store(&Divergence{Index: 99, Term: 1, Reason: "a test divergence"})

	refused := func(name string, err error) {
		t.Helper()
		var refusal *Error
		if !errors.As(err, &refusal) || refusal.Code != CodeUnavailable {
			t.Errorf("%s on a flagged leader returned %v, want the UNAVAILABLE divergence refusal", name, err)
		}
	}
	refused("AddJoiner", leader.AddJoiner("n3", spareAddr))
	_, err := leader.Join(JoinRequest{Member: Member{ID: "n3", Incarnation: 1, DataAddr: spareAddr}, RaftAddr: spareAddr, PlacingSet: make([]byte, 1024)})
	refused("Join", err)
	_, err = leader.Promote("n2")
	refused("Promote", err)
	_, err = leader.PromoteWithin("n2", 3)
	refused("PromoteWithin", err)
	_, err = leader.RemoveServer("n2")
	refused("RemoveServer", err)
	if changed, err := leader.ReconcileVoters(); err != nil || changed != 0 {
		t.Errorf("ReconcileVoters on a flagged leader changed %d (error %v), want 0", changed, err)
	}
	time.Sleep(50 * time.Millisecond)
	after := servers()
	if len(after) != len(before) || after["n2"] != before["n2"] || after["n3"] != "" {
		t.Errorf("the raft configuration changed on a flagged leader: before %v, after %v", before, after)
	}
	if got := leader.State().Voters; len(got) != len(voters) {
		t.Errorf("the voter list changed on a flagged leader: before %v, after %v", voters, got)
	}
}
