package harness_test

import (
	"context"
	"net"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

type c16AllowAll struct{}

func (c16AllowAll) Authorize(context.Context, wire.RingAccess) error { return nil }

// TestC16ANodesClientAddrIsInTopologyOnEveryNode: [GUARD a client address a node registers is committed, reaches
// every node's TOPOLOGY reply and the voter flags come with it; a node that registers none shows none].
func TestC16ANodesClientAddrIsInTopologyOnEveryNode(t *testing.T) {
	cl := harness.NewCluster(t, 3, harness.Options{NoDetection: true, ClientAddr: func(id, dataAddr string) string {
		if id == "n3" {
			return ""
		}
		host, _, err := net.SplitHostPort(dataAddr)
		if err != nil {
			t.Fatal(err)
		}
		return net.JoinHostPort(host, c16ClientPort(id))
	}})
	body, err := wire.EncodeRingTopology(wire.RingTopologyRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range cl.IDs() {
		handler, err := wire.NewRingHandler(cl.Node(id), func(context.Context, *transport.Frame) (*transport.Frame, error) {
			t.Error("a topology request was handed to the next handler")
			return nil, nil
		}, c16AllowAll{})
		if err != nil {
			t.Fatal(err)
		}
		answer, err := handler(context.Background(), transport.NewFrameWithID(transport.OpRingTopology, [16]byte{1}, body))
		if err != nil || answer.Opcode != transport.OpRingReply {
			t.Fatalf("%s: topology answered %v, %v", id, answer, err)
		}
		reply, err := wire.DecodeRingTopologyReply(answer.Payload)
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		members := reply.Table.Members()
		if len(members) != 3 {
			t.Fatalf("%s sees %d ranked members", id, len(members))
		}
		for _, member := range members {
			want := ""
			if member.ID != "n3" {
				host, _, _ := net.SplitHostPort(member.DataAddr)
				want = net.JoinHostPort(host, c16ClientPort(member.ID))
			}
			if member.ClientAddr != want {
				t.Errorf("%s sees %s with client address %q, want %q", id, member.ID, member.ClientAddr, want)
			}
			if member.DataAddr == "" || member.Suffrage != "voter" {
				t.Errorf("%s sees %s as %+v, want a data address and a vote", id, member.ID, member)
			}
		}
	}
}

// c16ClientPort is a port number for a node's client address; nothing listens on it.
func c16ClientPort(id string) string { return "6" + id[1:] + "000" }
