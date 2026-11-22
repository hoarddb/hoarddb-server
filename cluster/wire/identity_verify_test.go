package wire

import (
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

func identityReplyFor(id, addr string) IdentityReply {
	return IdentityReply{Nonce: 9, Incarnation: 4, NodeID: id, DataAddr: addr}
}

// TestANodeThatBootstrappedAloneCannotBeJoined (I3): a reply that holds Raft state and names no
// cluster is a node whose data directory already runs a cluster of its own.
// [BREAK: drop the HoldsRaftState clause of verifyIdentity].
func TestANodeThatBootstrappedAloneCannotBeJoined(t *testing.T) {
	reply := identityReplyFor("n2", "h:2")
	reply.HoldsRaftState = true
	_, err := verifyIdentity(reply, 9, "n2", "h:2", [16]byte{1})
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeClusterMismatch || !strings.Contains(refusal.Message, "bootstrapped a cluster of its own") {
		t.Fatalf("a node with Raft state and no cluster: %v", err)
	}
	reply.HoldsRaftState = false
	if _, err := verifyIdentity(reply, 9, "n2", "h:2", [16]byte{1}); err != nil {
		t.Fatalf("a fresh node was refused: %v", err)
	}
	reply.HoldsRaftState, reply.ClusterID = true, [16]byte{1}
	if got, err := verifyIdentity(reply, 9, "n2", "h:2", [16]byte{1}); err != nil || !got.HoldsRaftState {
		t.Fatalf("a returning member of this cluster was refused: %v", err)
	}
}

// TestAnIdentityAddressMismatchIsRefused (I4): the node advertises another address than the one typed.
func TestAnIdentityAddressMismatchIsRefused(t *testing.T) {
	_, err := verifyIdentity(identityReplyFor("n2", "other:2"), 9, "n2", "h:2", [16]byte{})
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument ||
		!strings.Contains(refusal.Message, "advertises other:2") || !strings.Contains(refusal.Message, "'join n2@other:2'") {
		t.Fatalf("an address the node does not advertise: %v", err)
	}
}

func TestAnIdentityOfAnotherClusterIsRefusedAndAStaleNonceIsNot(t *testing.T) {
	reply := identityReplyFor("n2", "h:2")
	reply.ClusterID = [16]byte{7}
	if _, err := verifyIdentity(reply, 9, "n2", "h:2", [16]byte{1}); err == nil {
		t.Fatal("a node of another cluster was accepted")
	}
	if _, err := verifyIdentity(identityReplyFor("n2", "h:2"), 8, "n2", "h:2", [16]byte{}); err == nil {
		t.Fatal("a reply to another request was accepted")
	}
}

// TestAClientAddrOnAnotherHostIsRefusedByTheJoin (I6): the client address a node publishes is held
// to the data address its pinned key vouches for, so a node cannot send drivers to another host.
// [BREAK: drop the ValidateClientAddr call in verifyIdentity]
func TestAClientAddrOnAnotherHostIsRefusedByTheJoin(t *testing.T) {
	reply := identityReplyFor("n2", "192.0.2.5:4433")
	reply.ClientAddr = "198.51.100.9:7434"
	_, err := verifyIdentity(reply, 9, "n2", "192.0.2.5:4433", [16]byte{})
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument ||
		!strings.Contains(refusal.Message, "198.51.100.9:7434") || !strings.Contains(refusal.Message, "tcp.advertise") || !strings.Contains(refusal.Message, "'join n2@192.0.2.5:4433'") {
		t.Fatalf("a client address on another host: %v", err)
	}
	reply.ClientAddr = "192.0.2.5:7434"
	got, err := verifyIdentity(reply, 9, "n2", "192.0.2.5:4433", [16]byte{})
	if err != nil || got.ClientAddr != "192.0.2.5:7434" {
		t.Fatalf("a client address on the node's own host: %+v, %v", got, err)
	}
}

// TestAnUnreachableIdentityNamesTheStandaloneAndWrongPinCause (I5, pi S10): a node started with no
// seeds pins only its own key, so the leader's connection is refused at TLS before any reply exists;
// the operator who sees "did not answer" is told that alternative too, and the fix.
// [BREAK: drop the sentence from identityUnreachable]
func TestAnUnreachableIdentityNamesTheStandaloneAndWrongPinCause(t *testing.T) {
	err := identityUnreachable("n2", "h:2", errors.New("tls: bad certificate"))
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeUnavailable {
		t.Fatalf("I5: %v", err)
	}
	for _, text := range []string{"hold a cluster of its own", "pinned to other keys", "before it can answer", "fresh data directory", "hash_ring.seed_fingerprints", "'join n2@h:2'", "tls: bad certificate"} {
		if !strings.Contains(refusal.Message, text) {
			t.Errorf("I5 lacks %q:\n%s", text, refusal.Message)
		}
	}
}
