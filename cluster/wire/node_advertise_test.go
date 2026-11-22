package wire

import (
	"context"
	"net"
	"testing"
)

// TestAdvertiseNamesTheNodeToItsPeers: [GUARD a node bound to a wildcard or private address
// commits NodeConfig.Advertise as its data address, in its identity and in its committed
// member row, never the bound address; Witness reaches the identity a joiner is registered with].
func TestAdvertiseNamesTheNodeToItsPeers(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	node, err := OpenNode(context.Background(), NodeConfig{
		Dir: t.TempDir(), NodeID: "node-a", Advertise: "node-a.example.test:4433", Witness: true,
		Cluster: &ClusterOptions{Listener: listener, InsecureTestOnly: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	identity := node.Identity()
	if identity.DataAddr != "node-a.example.test:4433" || !identity.Witness {
		t.Fatalf("identity %+v, want the advertised address and the witness flag", identity)
	}
	members := node.State().Members
	if len(members) != 1 || members[0].DataAddr != "node-a.example.test:4433" {
		t.Fatalf("committed members %+v, want one at the advertised address", members)
	}
}

// TestNoAdvertiseCommitsTheBoundAddress: a node with no Advertise names the address it listens on,
// as every earlier build did.
func TestNoAdvertiseCommitsTheBoundAddress(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	node, err := OpenNode(context.Background(), NodeConfig{
		Dir: t.TempDir(), NodeID: "node-a", Cluster: &ClusterOptions{Listener: listener, InsecureTestOnly: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	if got, want := node.Identity().DataAddr, listener.Addr().String(); got != want {
		t.Fatalf("identity data address %q, want %q", got, want)
	}
	if node.Identity().Witness {
		t.Fatal("a node with no Witness set reports itself a witness")
	}
}
