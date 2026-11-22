package wire

import (
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestJoinRefusesALoopbackAddressWhenTheClusterHasARemoteMember: the reachability rule of
// validateClusterReachability, applied to a join. A cluster whose members are all on loopback is
// one machine and takes loopback joiners; once one member is elsewhere, a loopback or link-local
// joiner could not be dialled by it.
// [BREAK: checkJoinAddress returns nil].
func TestJoinRefusesALoopbackAddressWhenTheClusterHasARemoteMember(t *testing.T) {
	local := cluster.ClusterState{Members: []cluster.Member{{ID: "n1", DataAddr: "127.0.0.1:4433"}}}
	remote := cluster.ClusterState{Members: []cluster.Member{{ID: "n1", DataAddr: "127.0.0.1:4433"}, {ID: "n2", DataAddr: "192.0.2.7:4433"}}}
	for _, addr := range []string{"127.0.0.1:4443", "[::1]:4443", "localhost:4443", "169.254.1.1:4443", "[fe80::1]:4443"} {
		if err := checkJoinAddress("n3", addr, local); err != nil {
			t.Errorf("%s on a one-machine cluster: %v", addr, err)
		}
		err := checkJoinAddress("n3", addr, remote)
		var refusal *cluster.Error
		if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument ||
			!strings.Contains(refusal.Message, "HOARDDB_LISTEN") || !strings.Contains(refusal.Message, "n2 (192.0.2.7:4433)") {
			t.Errorf("%s beside a remote member: %v", addr, err)
		}
	}
	if err := checkJoinAddress("n3", "192.0.2.9:4443", remote); err != nil {
		t.Errorf("a routable joiner on a mixed cluster: %v", err)
	}
	if err := checkJoinAddress("n3", "node-c.example:4443", remote); err != nil {
		t.Errorf("a named joiner on a mixed cluster: %v", err)
	}
}

// TestAJoinOwesWorkOnlyToAJoiningRow: promote, the witness's activation and the data joiner's
// activation are owed by a joining row alone. An active member's repeat join must not take a voter
// slot or commit anything (pi S1). [BREAK: joinStepsFor returns promote for every state]
func TestAJoinOwesWorkOnlyToAJoiningRow(t *testing.T) {
	if got := joinStepsFor(cluster.Member{State: cluster.MemberJoining}); got != (joinSteps{promote: true, activate: true}) {
		t.Errorf("a joining data member owes %+v", got)
	}
	if got := joinStepsFor(cluster.Member{State: cluster.MemberJoining, Witness: true}); got != (joinSteps{promote: true, activateWitness: true}) {
		t.Errorf("a joining witness owes %+v", got)
	}
	for _, witness := range []bool{false, true} {
		for _, state := range []cluster.MemberState{cluster.MemberActive, cluster.MemberDead} {
			if got := joinStepsFor(cluster.Member{State: state, Witness: witness}); got != (joinSteps{}) {
				t.Errorf("a %s member (witness %v) owes %+v after its join completed", state, witness, got)
			}
		}
	}
}

// TestJoinRefusesAZonedDataAddress: a data address with a zone (fe80::1%eth0) names an interface of
// the joining host, so no other machine can dial it, on a one-machine cluster as well as a mixed
// one. Plain loopback, routable and named addresses keep their behaviour.
// [BREAK: checkJoinAddress skips the zone check].
func TestJoinRefusesAZonedDataAddress(t *testing.T) {
	local := cluster.ClusterState{Members: []cluster.Member{{ID: "n1", DataAddr: "127.0.0.1:4433"}}}
	remote := cluster.ClusterState{Members: []cluster.Member{{ID: "n1", DataAddr: "127.0.0.1:4433"}, {ID: "n2", DataAddr: "192.0.2.7:4433"}}}
	zoned := map[string]string{
		"[fe80::1%eth0]:4433":   "(%eth0)",
		"[fe80::1%25eth0]:4433": "(%25eth0)",
	}
	for addr, zone := range zoned {
		for name, state := range map[string]cluster.ClusterState{"one machine": local, "mixed": remote} {
			err := checkJoinAddress("n3", addr, state)
			var refusal *cluster.Error
			if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument ||
				!strings.Contains(refusal.Message, "names a network zone "+zone) ||
				!strings.Contains(refusal.Message, "192.0.2.1:4433") {
				t.Errorf("%s joining on a %s cluster: %v, want a zone refusal naming %s and the routable example", addr, name, err, zone)
			}
		}
	}
	for _, addr := range []string{"[::1]:4433", "192.0.2.1:4433", "[2001:db8::1]:4433", "node-a:4433"} {
		if err := checkJoinAddress("n3", addr, local); err != nil && strings.Contains(err.Error(), "network zone") {
			t.Errorf("%s on a one-machine cluster refused as zoned: %v", addr, err)
		}
		if err := checkJoinAddress("n3", addr, remote); err != nil && strings.Contains(err.Error(), "network zone") {
			t.Errorf("%s on a mixed cluster refused as zoned: %v", addr, err)
		}
	}
	if err := checkJoinAddress("n3", "[::1]:4433", local); err != nil {
		t.Errorf("loopback on a one-machine cluster: %v", err)
	}
	for _, addr := range []string{"192.0.2.1:4433", "[2001:db8::1]:4433", "node-a:4433"} {
		if err := checkJoinAddress("n3", addr, remote); err != nil {
			t.Errorf("%s on a mixed cluster: %v", addr, err)
		}
	}
}
