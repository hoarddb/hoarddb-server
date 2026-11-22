package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// refusedVerbThenPing sends one operator membership verb that the node
// refuses, then a PING on the same connection, and returns the refusal. The
// PING must be served: a refusal is an answer, not a fault.
func refusedVerbThenPing(t *testing.T, tc *testCluster, op transport.Opcode, request any) ErrorResponse {
	t.Helper()
	payload, err := bson.Marshal(request)
	if err != nil {
		t.Fatalf("encoding the %s request: %v", op, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	frame, err := tc.nodes[0].client.SendRequest(ctx, op, payload)
	if err != nil {
		t.Fatalf("the refused %s got no answer: %v", op, err)
	}
	refusal := refusalOf(t, frame)

	next, err := tc.nodes[0].client.SendRequest(ctx, transport.OpPing, nil)
	if err != nil {
		t.Errorf("the connection does not serve a PING after a refused %s: %v", op, err)
	} else if next.Opcode == transport.OpError {
		t.Errorf("the PING after a refused %s was refused: %s", op, answerText(next))
	}
	return refusal
}

// TestARefusedRemoveNodeKeepsTheConnectionAndItsCode is BUG-0509 and BUG-0303
// on remove-node: removing a node that is not a member answers NOT_FOUND with
// a sentence naming 'nodes', and the connection serves the next request.
// Guard: return the refusal as the handler's error — the transport answers
// INTERNAL and closes the connection.
func TestARefusedRemoveNodeKeepsTheConnectionAndItsCode(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()

	refusal := refusedVerbThenPing(t, tc, transport.OpClusterRemove, ClusterRemoveRequest{NodeID: "127.0.0.1:1"})
	if transport.ErrorCode(refusal.Code) != transport.ErrNotFound {
		t.Errorf("removing a non-member answers code %d, want %d (not found): %q",
			refusal.Code, transport.ErrNotFound, refusal.Message)
	}
	if !strings.Contains(refusal.Message, "127.0.0.1:1") || !strings.Contains(refusal.Message, "'nodes'") {
		t.Errorf("removing a non-member reads %q, want the node and the command that lists the members", refusal.Message)
	}
}

// TestARefusedAddNodeKeepsTheConnectionAndItsCode is BUG-0509 and BUG-0303 on
// add-node: adding a node that is already a member answers ALREADY_EXISTS with
// a sentence naming 'nodes', and the connection serves the next request.
// Guard: return the refusal as the handler's error.
func TestARefusedAddNodeKeepsTheConnectionAndItsCode(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()
	member := clusterOf(tc, 1).self

	refusal := refusedVerbThenPing(t, tc, transport.OpClusterAdd, ClusterAddRequest{Address: member.ID + "@" + member.Addr})
	if transport.ErrorCode(refusal.Code) != transport.ErrAlreadyExists {
		t.Errorf("adding a member again answers code %d, want %d (already exists): %q",
			refusal.Code, transport.ErrAlreadyExists, refusal.Message)
	}
	if !strings.Contains(refusal.Message, member.ID) || !strings.Contains(refusal.Message, "'nodes'") {
		t.Errorf("adding a member again reads %q, want the node and the command that lists the members", refusal.Message)
	}
}

// TestMembershipRefusalsCarryTheirCode pins the codes of the membership
// refusals an operator verb answers. ErrEpochInProgress is retryable ("ask
// again in a moment"), so it must not read as INTERNAL, which a driver does
// not retry.
func TestMembershipRefusalsCarryTheirCode(t *testing.T) {
	cases := []struct {
		err  error
		want transport.ErrorCode
	}{
		{ErrEpochInProgress, transport.ErrUnavailable},
		{ErrNotMember, transport.ErrNotFound},
		{ErrAlreadyMember, transport.ErrAlreadyExists},
	}
	for _, c := range cases {
		wrapped := fmt.Errorf("%w: node-x", c.err)
		if got := codeFor(wrapped); got != c.want {
			t.Errorf("codeFor(%v) = %s, want %s", wrapped, got, c.want)
		}
	}
}
