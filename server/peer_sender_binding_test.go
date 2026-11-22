package server

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// senderRefusal is the text every internode handler answers with when a
// request names a sender other than the member its connection speaks for.
const senderRefusal = "names a sender other than the member this connection speaks for"

// memberConnection opens one internode connection from member from to member
// to, with from's cluster token and pinned trust, and binds it to from by
// sending from's own gossip on it first. It returns the connection.
func memberConnection(t *testing.T, tc *testCluster, from, to int) *transport.TCPPeerClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dialer := clusterOf(tc, from)
	internodeAddr, err := transport.InternodePeerAddr(tc.addrs[to])
	if err != nil {
		t.Fatalf("deriving the internode address of %s: %v", tc.addrs[to], err)
	}
	tlsCfg, err := dialer.internodeClientConfig(internodeAddr)
	if err != nil {
		t.Fatalf("building the pinned client config for %s: %v", internodeAddr, err)
	}
	cli, err := transport.NewTCPPeerClient(ctx, internodeAddr, tlsCfg, dialer.localToken(), 1)
	if err != nil {
		t.Fatalf("dialing %s as %s: %v", internodeAddr, tc.addrs[from], err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	resp, err := peerOf(t, tc, from, to).exchangeGossip(ctx, cli, 0)
	if err != nil {
		t.Fatalf("gossip from %s on its own connection: %v", tc.addrs[from], err)
	}
	if !resp.Success {
		t.Fatalf("setup: %s's own gossip on its own connection was refused: %s", tc.addrs[from], resp.Error)
	}
	return cli
}

// sendInternodeOn sends one internode request on cli and decodes the answer into answer.
func sendInternodeOn(t *testing.T, cli *transport.TCPPeerClient, op transport.Opcode, req, answer any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	payload, err := bson.Marshal(req)
	if err != nil {
		t.Fatalf("encoding the %s request: %v", op, err)
	}
	frame, err := cli.SendRequest(ctx, transport.LaneControl, op, payload)
	if err != nil {
		t.Fatalf("sending the %s request: %v", op, err)
	}
	if err := decodePeerAnswer(frame.Payload, answer); err != nil {
		t.Fatalf("decoding the %s answer: %v", op, err)
	}
}

// TestARequestNamingAnotherMemberIsRefusedOnAMembersConnection: a connection
// that has spoken as member A is A's. A request on it that names member B as
// its sender is refused before anything of it is applied, for every
// internode request that names its sender: gossip, a replication push (a
// stream handshake here), a phase of a membership change, a pull's start and
// a versioned read. Before the binding, each was served on the sender's own
// word: B's gossip reset what the receiver knew of B's features, and B's
// handshake moved the receiver's record of B's stream onto another log.
func TestARequestNamingAnotherMemberIsRefusedOnAMembersConnection(t *testing.T) {
	tc := newTestClusterTuned(t, 3, 1, autoRebalanceOff, nil)
	t.Cleanup(tc.stop)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeFIFO})

	const speaker, receiverIndex, named = 0, 1, 2
	receiver := clusterOf(tc, receiverIndex)
	victim := tc.addrs[named]
	token := clusterOf(tc, speaker).localToken()
	cli := memberConnection(t, tc, speaker, receiverIndex)

	t.Run("gossip", func(t *testing.T) {
		feature := clusterFeatures()[0]
		pollUntil(t, "the receiver has heard "+victim+"'s features", func() bool { return receiver.memberAdvertises(victim, feature) })
		var resp GossipResponse
		sendInternodeOn(t, cli, transport.OpGossip, GossipRequest{Token: token, NodeID: victim, Addr: victim,
			Epoch: receiver.Epoch(), Nodes: receiver.memberNodes(), Members: receiver.memberIDs()}, &resp)
		if resp.Success || !strings.Contains(resp.Error, senderRefusal) {
			t.Fatalf("gossip naming %s on %s's connection was answered %+v; want the sender refusal", victim, tc.addrs[speaker], resp)
		}
		if !receiver.memberAdvertises(victim, feature) {
			t.Fatalf("the refused gossip still reset what the receiver knows of %s's features", victim)
		}
	})

	t.Run("replication push", func(t *testing.T) {
		recorded := openedStream(t, receiver, victim).Incarnation
		other := bytes.Repeat([]byte{0x7e}, 16)
		var resp ReplPushResponse
		sendInternodeOn(t, cli, transport.OpReplPush, ReplPushRequest{Token: token, FromNode: victim, Epoch: receiver.Epoch(),
			Stream: &ReplStream{Incarnation: other, Open: true, Rebase: &ReplRebase{From: recorded, Log: other}}}, &resp)
		if resp.Success || !strings.Contains(resp.Error, senderRefusal) {
			t.Fatalf("a handshake naming %s on %s's connection was answered %+v; want the sender refusal", victim, tc.addrs[speaker], resp)
		}
		if now := receiver.recordedPosition(victim).Incarnation; !bytes.Equal(now, recorded) {
			t.Fatalf("the refused handshake moved the receiver's record of %s's stream from %x to %x", victim, recorded, now)
		}
	})

	t.Run("membership change", func(t *testing.T) {
		var resp MembershipChangeResponse
		sendInternodeOn(t, cli, transport.OpMembershipChange, MembershipChangeRequest{Token: token, FromNode: victim, Phase: phasePrepare,
			Epoch: receiver.Epoch() + 1, Ballot: ballot{Round: 1 << 40, Node: victim}}, &resp)
		if resp.Answer != string(answerRefused) || !strings.Contains(resp.Reason, senderRefusal) {
			t.Fatalf("a prepare naming %s on %s's connection was answered %+v; want the sender refusal", victim, tc.addrs[speaker], resp)
		}
	})

	t.Run("pull start", func(t *testing.T) {
		var resp MigrateStartResponse
		sendInternodeOn(t, cli, transport.OpMigrateStart, MigrateStartRequest{Token: token, Database: testDatabase, Bucket: testBucket,
			FromNode: victim, Epoch: receiver.Epoch()}, &resp)
		if resp.Ok || !strings.Contains(resp.Error, senderRefusal) {
			t.Fatalf("a pull start naming %s on %s's connection was answered %+v; want the sender refusal", victim, tc.addrs[speaker], resp)
		}
	})

	t.Run("versioned read", func(t *testing.T) {
		var resp VersionedGetResponse
		sendInternodeOn(t, cli, transport.OpVersionedGet, VersionedGetRequest{Token: token, Database: testDatabase, Bucket: testBucket,
			Key: "k", FromNode: victim}, &resp)
		if !strings.Contains(resp.Error, senderRefusal) {
			t.Fatalf("a versioned read naming %s on %s's connection was answered %+v; want the sender refusal", victim, tc.addrs[speaker], resp)
		}
	})

	// The connection is still the speaker's: its own requests are served.
	t.Run("own requests still served", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resp, err := peerOf(t, tc, speaker, receiverIndex).exchangeGossip(ctx, cli, 0)
		if err != nil || !resp.Success {
			t.Fatalf("%s's own gossip on its connection after the refusals: %+v, %v", tc.addrs[speaker], resp, err)
		}
	})
}
