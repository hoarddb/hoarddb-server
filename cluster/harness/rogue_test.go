package harness_test

import (
	"errors"
	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestARoguePeerReachesNoHandler: a peer whose key every node pins is still refused
// before any handler runs when it is not the member it claims to be. Two rogues: a
// pinned non-member under a made-up name, and a pinned key speaking as member n1; each
// with the true cluster id and with the zero one. A pin proves a key, not an identity.
func TestARoguePeerReachesNoHandler(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	body, err := wire.EncodeForwardReadBody(wire.ForwardReadBody{Req: wire.ReadRequest{EncID: []byte("x")}})
	if err != nil {
		t.Fatal(err)
	}
	report := transport.Message{Env: transport.Envelope{Version: transport.EnvelopeVersion, Kind: transport.KindClusterNotice}, Body: make([]byte, 16)}
	for _, tc := range []struct {
		name, claim string
		zero        bool
	}{
		{"non-member", "ghost", false},
		{"non-member zero cluster id", "ghost", true},
		{"impersonating a member", "n1", false},
		{"impersonating a member zero cluster id", "n1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rogue := c.NewRogue(tc.claim, tc.zero)
			target := "n2"
			for _, call := range []struct {
				name string
				ch   transport.Channel
				msg  transport.Message
			}{
				{"forward read", transport.ChannelData, transport.Message{Env: transport.Envelope{Version: transport.EnvelopeVersion, Kind: transport.KindForwardRead, DBID: 1}, Body: body}},
				{"write ack", transport.ChannelData, transport.Message{Env: transport.Envelope{Version: transport.EnvelopeVersion, Kind: 0x02, DBID: 1}, Body: append(make([]byte, 16), 1, 0)}},
				{"catch-up request", transport.ChannelData, transport.Message{Env: transport.Envelope{Version: transport.EnvelopeVersion, Kind: 0x03, DBID: 1}, Body: make([]byte, 24)}},
				{"cluster notice", transport.ChannelControl, report},
			} {
				_, err := rogue.Call(target, call.ch, call.msg)
				var refusal *transport.WireError
				if err == nil || errors.As(err, &refusal) {
					t.Errorf("%s as %q: reached the handler (err %v); the connection must be dropped before any handler runs", call.name, tc.claim, err)
				}
			}
			if !rogue.RaftClosed(target) {
				t.Errorf("raft channel as %q: the connection stayed open; only a current member speaking as itself may reach the Raft library", tc.claim)
			}
		})
	}
}

// TestAMemberSpeaksOnlyForItsRole: a genuine member, authenticated as itself, still may not
// push a WRITE for a pair it does not own, or ack a pair it is not a copy of.
func TestAMemberSpeaksOnlyForItsRole(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	id, owner, copyNode, outsider := proofID(t, c, "role")
	slot, err := wire.PlacementOf(proofDB, proofBucket, id)
	if err != nil {
		t.Fatal(err)
	}
	dbID := c.Node(owner).State().Databases[0].ID
	epoch := c.Node(owner).State().Epoch
	record := make([]byte, 16+8+1+1+16+4) // pos 0:0 cursor 0 class 0 index 0 id 0 empty body
	write := transport.Message{Env: transport.Envelope{Version: transport.EnvelopeVersion, Kind: 0x01, Epoch: uint64(epoch), Slot: slot, DBID: uint64(dbID)}, Body: record}
	_, err = c.CallAs(outsider, copyNode, transport.ChannelData, write)
	if err == nil || !strings.Contains(err.Error(), "not the owner") {
		t.Errorf("a WRITE from %s, which does not own the pair (owner %s): %v; want a refusal naming that it is not the owner", outsider, owner, err)
	}
	ack := transport.Message{Env: transport.Envelope{Version: transport.EnvelopeVersion, Kind: 0x02, Slot: slot, DBID: uint64(dbID)}, Body: append(make([]byte, 16), 1, 0)}
	_, err = c.CallAs(outsider, owner, transport.ChannelData, ack)
	if err == nil || !strings.Contains(err.Error(), "not a copy") {
		t.Errorf("a WRITE_ACK from %s, which holds no copy of the pair: %v; want a refusal naming that it is not a copy", outsider, err)
	}
}

// TestAMemberWithoutTheClusterIDIsRefused: the right key and the right name are not
// enough once the cluster id exists; a handshake with the zero id is dropped.
func TestAMemberWithoutTheClusterIDIsRefused(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	msg := transport.Message{Env: transport.Envelope{Version: transport.EnvelopeVersion, Kind: transport.KindClusterNotice}, Body: make([]byte, 16)}
	if _, err := c.CallAs("n1", "n2", transport.ChannelControl, msg); err != nil {
		t.Fatalf("a genuine member with the cluster id was refused: %v", err)
	}
	_, err := c.CallAsWithoutClusterID("n1", "n2", transport.ChannelControl, msg)
	var refusal *transport.WireError
	if err == nil || errors.As(err, &refusal) {
		t.Fatalf("a handshake with the zero cluster id reached the handler: %v", err)
	}
}

// TestAForwardedWriteIsNeverForwardedAgain: a node that is not the owner and receives a
// write already forwarded once refuses NOT_OWNER; it does not pass it on (no loop from two
// stale tables).
func TestAForwardedWriteIsNeverForwardedAgain(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	id, owner, copyNode, outsider := proofID(t, c, "hop")
	slot, err := wire.PlacementOf(proofDB, proofBucket, id)
	if err != nil {
		t.Fatal(err)
	}
	state := c.Node(owner).State()
	enc, err := store.EncodeComponent(id)
	if err != nil {
		t.Fatal(err)
	}
	body, err := wire.EncodeForwardWriteBody(wire.WriteRequest{Hop: 1, Op: cluster.Op{Class: cluster.OpDelete, BucketID: 1, EncID: enc}})
	if err != nil {
		t.Fatal(err)
	}
	msg := transport.Message{Env: transport.Envelope{Version: transport.EnvelopeVersion, Kind: transport.KindForwardWrite, Epoch: uint64(state.Epoch), Slot: slot, DBID: uint64(state.Databases[0].ID)}, Body: body}
	reply, err := c.CallAs(copyNode, outsider, transport.ChannelData, msg)
	if err != nil {
		t.Fatalf("the forward was dropped instead of refused: %v", err)
	}
	_, err = wire.DecodeForwardWriteReply(reply.Body)
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeNotOwner {
		t.Fatalf("a hop-1 write at %s, which does not own it (owner %s): %v, want NOT_OWNER", outsider, owner, err)
	}
}

// TestAnAlterFromANonOwnerNamesTheNodeThatOwnsTheDeclaration: an alter closes over the
// owner's state and cannot be forwarded; the refusal names the node to send it to. With the
// proposal relay the owner can always finish it, so no refusal points at the Raft leader.
func TestAnAlterFromANonOwnerNamesTheNodeThatOwnsTheDeclaration(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	leader := c.AwaitLeader(10 * time.Second)
	for _, id := range c.IDs() {
		_, err := c.Node(id).AlterBucket(proofCtx(t), proofDB, proofBucket, wire.AlterChange{Add: []wire.FieldEntry{{Name: "age" + id, Type: "int"}}})
		if err == nil {
			continue
		}
		if !strings.Contains(err.Error(), "owns the declaration") || strings.Contains(err.Error(), "not the Raft leader") {
			t.Fatalf("alter through %s (leader %s): %v; want a refusal that says which node owns the declaration and to run the alter there", id, leader, err)
		}
	}
	_, err := c.Node(leader).AlterBucket(proofCtx(t), proofDB, proofBucket, wire.AlterChange{Add: []wire.FieldEntry{{Name: "agez", Type: "int"}}})
	if err != nil && !strings.Contains(err.Error(), "Connect to") {
		t.Fatalf("the leader's refusal %v does not say where to run the alter", err)
	}
}
