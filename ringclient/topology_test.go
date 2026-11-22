package ringclient

import (
	"errors"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

func topologyFixture() wire.RingTopologyReply {
	state := cluster.ClusterState{Epoch: 4, StateSeq: 9, Members: []cluster.Member{
		{ID: "node-a", DataAddr: "192.0.2.1:4433", State: cluster.MemberActive},
		{ID: "node-b", DataAddr: "192.0.2.2:4433", State: cluster.MemberDown},
		{ID: "node-c", DataAddr: "192.0.2.3:4433", State: cluster.MemberActive}}}
	return wire.RingTopologyReply{Epoch: 4, StateSeq: 9, Table: cluster.BuildTable(state),
		Databases: []wire.RingTopologyDatabase{{Name: "app", ID: 1, RF: 2}, {Name: "logs", ID: 2, RF: 1}}}
}

// TestTopologyDecodesTheMembersAndRoutesAsTheServerDoes: [GUARD the owner of a slot is the first name of the
// first RF names of the ranking whose state is not down, per database].
func TestTopologyDecodesTheMembersAndRoutesAsTheServerDoes(t *testing.T) {
	fixture := topologyFixture()
	var asked []wire.RingTopologyRequest
	server := startFakeServer(t, func(conn *fakeConn, frame *transport.Frame) {
		request, err := wire.DecodeRingTopology(frame.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		asked = append(asked, request)
		reply := fixture
		if request.Epoch == fixture.Epoch && request.StateSeq == fixture.StateSeq {
			reply = wire.RingTopologyReply{Epoch: fixture.Epoch, StateSeq: fixture.StateSeq, Unchanged: true}
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
	defer func() { _ = client.Close() }()
	topology, err := client.Topology(testContext(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if topology.Epoch != 4 || topology.StateSeq != 9 || len(topology.Members) != 3 || topology.Members[1].DataAddr != "192.0.2.2:4433" || topology.Members[1].Addr != "" || topology.Members[1].State != "down" {
		t.Fatalf("topology %+v", topology)
	}
	if len(topology.Databases) != 2 || topology.Databases[0] != (TopologyDatabase{Name: "app", ID: 1, RF: 2}) {
		t.Fatalf("databases %+v", topology.Databases)
	}
	for slot := range uint16(8192) {
		for _, database := range []string{"app", "logs"} {
			rf := 2
			if database == "logs" {
				rf = 1
			}
			want, wantOK := fixture.Table.Owner(slot, rf)
			got, ok := topology.Owner(database, slot)
			if ok != wantOK || got.ID != want.ID || got.DataAddr != want.DataAddr || got.Addr != want.DriverAddr() {
				t.Fatalf("%s slot %d: owner %+v/%v, want %+v/%v", database, slot, got, ok, want, wantOK)
			}
		}
	}
	if _, ok := topology.Owner("missing", 0); ok {
		t.Error("a database the topology does not list has an owner")
	}
	again, err := client.Topology(testContext(t), topology)
	if err != nil || again != topology {
		t.Fatalf("a matching view should give back the topology held: %v", err)
	}
	if len(asked) != 2 || asked[0] != (wire.RingTopologyRequest{}) || asked[1] != (wire.RingTopologyRequest{Epoch: 4, StateSeq: 9}) {
		t.Fatalf("requests %+v", asked)
	}
}

// TestTopologyRefusalsAreTypedErrors: [GUARD an OpError becomes the typed *Error the other calls return].
func TestTopologyRefusalsAreTypedErrors(t *testing.T) {
	server := startFakeServer(t, func(conn *fakeConn, frame *transport.Frame) {
		conn.refuse(frame, cluster.CodePermissionDenied, "this session cannot read the topology")
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	_, err = client.Topology(testContext(t), nil)
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodePermissionDenied {
		t.Fatalf("a refused topology call returned %v, want a PERMISSION_DENIED *Error", err)
	}
}

// TestAWitnessInTheTopologyHasNoDriverAddress: [BUG-0876: a witness the server lists has no client address, so
// Member.Addr is empty and a caller that skips empty addresses can never pick it as a login endpoint].
func TestAWitnessInTheTopologyHasNoDriverAddress(t *testing.T) {
	fixture := topologyFixture()
	witness := cluster.Member{ID: "node-w", DataAddr: "192.0.2.9:4433", ClientAddr: "w.example:7434", State: cluster.MemberActive, Suffrage: "voter", Witness: true}
	fixture.Witnesses = wire.TopologyWitnessRows([]cluster.Member{witness})
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
	defer func() { _ = client.Close() }()
	topology, err := client.Topology(testContext(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(topology.Witnesses) != 1 || topology.Witnesses[0].ID != "node-w" || topology.Witnesses[0].Addr != "" || topology.Witnesses[0].ClientAddr != "" {
		t.Fatalf("witnesses %+v: want one with no address", topology.Witnesses)
	}
	if topology.Witnesses[0].DataAddr != "192.0.2.9:4433" {
		t.Errorf("witness data address %q lost", topology.Witnesses[0].DataAddr)
	}
}
