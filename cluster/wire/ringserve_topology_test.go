package wire

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestTheTopologyDoesNotPublishAWitnessAsALoginEndpoint: [BUG-0876: a witness holds no password key, so the
// topology must not list its client address; its id, data address and voter flag stay, ranked members are
// unchanged, and the committed state keeps the address].
func TestTheTopologyDoesNotPublishAWitnessAsALoginEndpoint(t *testing.T) {
	state := cluster.ClusterState{Epoch: 7, StateSeq: 42, Members: []cluster.Member{
		{ID: "node-a", DataAddr: "192.0.2.1:4433", ClientAddr: "a.example:7434", State: cluster.MemberActive, Suffrage: "voter"},
		{ID: "node-w", DataAddr: "192.0.2.4:4433", ClientAddr: "w.example:7434", State: cluster.MemberActive, Suffrage: "voter", Witness: true},
	}}
	server := &ringServer{node: &Node{topology: NewClusterTopology("node-a", c5Source{state: state})}, authz: u70AllowAll{}}
	reply, err := server.serveTopology(t.Context(), RingTopologyRequest{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRingTopologyReply(u50EncodeReply(t, reply))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Witnesses) != 1 {
		t.Fatalf("decoded %d witnesses, want 1", len(got.Witnesses))
	}
	if w := got.Witnesses[0]; w.ID != "node-w" || w.ClientAddr != "" || w.DataAddr != "192.0.2.4:4433" || w.Suffrage != "voter" || !w.Witness {
		t.Errorf("witness row %+v: want the client address empty and the rest intact", w)
	}
	if members := got.Table.Members(); len(members) != 1 || members[0].ClientAddr != "a.example:7434" {
		t.Errorf("ranked members %+v: the data member's client address must be unchanged", members)
	}
	if state.Members[1].ClientAddr != "w.example:7434" {
		t.Error("serving the topology cleared the address in the committed state")
	}
}
