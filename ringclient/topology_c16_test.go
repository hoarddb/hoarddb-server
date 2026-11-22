package ringclient

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

func c16Topology(t *testing.T) *Topology {
	t.Helper()
	state := cluster.ClusterState{Epoch: 4, StateSeq: 9, Members: []cluster.Member{
		{ID: "node-a", DataAddr: "192.0.2.1:4433", ClientAddr: "a.example:7434", State: cluster.MemberActive, Suffrage: "voter"},
		{ID: "node-b", DataAddr: "192.0.2.2:4433", State: cluster.MemberActive, Suffrage: "nonvoter"},
		{ID: "node-w", DataAddr: "192.0.2.9:4433", ClientAddr: "w.example:7434", State: cluster.MemberActive, Suffrage: "voter", Witness: true}}}
	fixture := wire.RingTopologyReply{Epoch: 4, StateSeq: 9, Table: cluster.BuildTable(state), Witnesses: []cluster.Member{state.Members[2]},
		Databases: []wire.RingTopologyDatabase{{Name: "app", ID: 1, RF: 2}}}
	server := startFakeServer(t, func(conn *fakeConn, frame *transport.Frame) {
		payload, err := wire.EncodeRingTopologyReply(fixture)
		if err != nil {
			t.Error(err)
			return
		}
		conn.reply(frame, payload)
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	topology, err := client.Topology(testContext(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	return topology
}

// TestC16ADriverConnectsToTheClientAddrAndNeverToTheDataAddr: [GUARD Member.Addr, and so every owner the
// driver routes to, is the client address when the member published one and the data address when it did not].
func TestC16ADriverConnectsToTheClientAddrAndNeverToTheDataAddr(t *testing.T) {
	topology := c16Topology(t)
	if len(topology.Members) != 2 {
		t.Fatalf("members %+v", topology.Members)
	}
	a, b := topology.Members[0], topology.Members[1]
	if a.Addr != "a.example:7434" || a.ClientAddr != "a.example:7434" || a.DataAddr != "192.0.2.1:4433" || !a.Voter {
		t.Errorf("node-a is %+v", a)
	}
	if b.Addr != "" || b.ClientAddr != "" || b.DataAddr != "192.0.2.2:4433" || b.Voter {
		t.Errorf("node-b is %+v", b)
	}
	seen := map[string]bool{}
	for slot := range uint16(8192) {
		owner, ok := topology.Owner("app", slot)
		if !ok {
			t.Fatalf("slot %d has no owner", slot)
		}
		want := map[string]string{"node-a": "a.example:7434", "node-b": ""}[owner.ID]
		if owner.Addr != want {
			t.Fatalf("slot %d: owner %s routes to %q, want %q", slot, owner.ID, owner.Addr, want)
		}
		seen[owner.ID] = true
	}
	if len(seen) != 2 {
		t.Errorf("owners seen: %v", seen)
	}
}

// TestC5AWitnessIsListedAndNeverAnOwner: [GUARD witnesses come back apart from the ranked members, with their
// voter flag, and no slot is ever routed to one].
func TestC5AWitnessIsListedAndNeverAnOwner(t *testing.T) {
	topology := c16Topology(t)
	if len(topology.Witnesses) != 1 {
		t.Fatalf("witnesses %+v", topology.Witnesses)
	}
	if w := topology.Witnesses[0]; w.ID != "node-w" || !w.Voter || w.Addr != "w.example:7434" || w.State != "active" {
		t.Errorf("witness %+v", w)
	}
	for slot := range uint16(8192) {
		if owner, _ := topology.Owner("app", slot); owner.ID == "node-w" {
			t.Fatalf("slot %d routed to the witness", slot)
		}
	}
}

// TestC5TheTopologyCarriesTheVoterCount: [GUARD Topology.Voters is the reply's committed voter count, which includes a
// voter on neither list, and a matching view held keeps it].
func TestC5TheTopologyCarriesTheVoterCount(t *testing.T) {
	state := cluster.ClusterState{Epoch: 4, StateSeq: 9, Members: []cluster.Member{{ID: "node-a", DataAddr: "192.0.2.1:4433", State: cluster.MemberActive, Suffrage: "voter"}}}
	fixture := wire.RingTopologyReply{Epoch: 4, StateSeq: 9, Table: cluster.BuildTable(state), Voters: 3}
	server := startFakeServer(t, func(conn *fakeConn, frame *transport.Frame) {
		request, err := wire.DecodeRingTopology(frame.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		reply := fixture
		if request.Epoch == 4 && request.StateSeq == 9 {
			reply = wire.RingTopologyReply{Epoch: 4, StateSeq: 9, Unchanged: true}
		}
		payload, err := wire.EncodeRingTopologyReply(reply)
		if err != nil {
			t.Error(err)
			return
		}
		conn.reply(frame, payload)
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	topology, err := client.Topology(testContext(t), nil)
	if err != nil || topology.Voters != 3 {
		t.Fatalf("voters %d, %v", topology.Voters, err)
	}
	again, err := client.Topology(testContext(t), topology)
	if err != nil || again.Voters != 3 {
		t.Fatalf("the held view lost its voter count: %d, %v", again.Voters, err)
	}
}
