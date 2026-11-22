package cli

import (
	"context"
	"errors"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// TestU83TheRingAPIDescribesTheNode: [GUARD DescribeNode is part of RingAPI, the real adapter
// provides it from the client, and the fake answers a healthy node, a node set by a test, and
// a failure injected under its name].
func TestU83TheRingAPIDescribesTheNode(t *testing.T) {
	var _ RingAPI = ringConnection{}
	fake := newU52Fake()
	var api RingAPI = fake
	node, err := api.DescribeNode(context.Background())
	if err != nil || node.NodeID != "node-a" || node.Serving != "ok" || node.Members != 1 || node.Host != nil {
		t.Fatalf("the default fake node is %+v, %v", node, err)
	}
	fake.node = ringclient.NodeDescription{NodeID: "node-c", Members: 4, Voters: 3, Serving: "no_quorum", Host: &ringclient.HostDescription{CPUs: 4}}
	if node, err = api.DescribeNode(context.Background()); err != nil || node.NodeID != "node-c" || node.Host == nil || node.Host.CPUs != 4 {
		t.Fatalf("the fake ignored the node a test set: %+v, %v", node, err)
	}
	refusal := &ringclient.Error{Code: cluster.CodeInvalidArgument, Message: `cmd "describe_node" is not an admin command`}
	fake.failures["DescribeNode"] = refusal
	if _, err := api.DescribeNode(context.Background()); !errors.Is(err, refusal) {
		t.Fatalf("the injected failure came back as %v", err)
	}
}
