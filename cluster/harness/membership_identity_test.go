package harness_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// identityNodes is a one-node cluster whose leader asks late identities who they are.
func identityNodes(t *testing.T, late int) (*harness.Cluster, *wire.Node) {
	t.Helper()
	c := harness.NewCluster(t, 1, harness.Options{Late: late, NoDetection: true})
	return c, c.Node(c.AwaitLeader(10 * time.Second))
}

func askCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestAJoinerServesIdentityBeforeItIsAMember: a node started to be joined holds no cluster state
// and still answers the leader's identity request, with the facts the join needs.
func TestAJoinerServesIdentityBeforeItIsAMember(t *testing.T) {
	c, leader := identityNodes(t, 1)
	joiner := c.OpenJoiner()
	got, err := leader.AskIdentity(askCtx(t), joiner, c.Addr(joiner))
	if err != nil {
		t.Fatalf("asking an unjoined node who it is: %v", err)
	}
	if got.ID != joiner || got.Addr != c.Addr(joiner) || got.Incarnation < 1 {
		t.Fatalf("identity %+v, want %s at %s with an incarnation", got, joiner, c.Addr(joiner))
	}
	if got.ClusterID != [16]byte{} || got.HoldsRaftState || got.Witness {
		t.Fatalf("an unjoined node reports cluster %x, state %v, witness %v; it holds none", got.ClusterID, got.HoldsRaftState, got.Witness)
	}
	if want := c.Node(joiner).Identity().Incarnation; got.Incarnation != want {
		t.Fatalf("incarnation %d, the node's own is %d", got.Incarnation, want)
	}
}

// TestIdentityIsRefusedToAnUnpinnedKey: a caller whose key no node pins is refused at TLS, and a
// pinned key speaking as a node it is not (n1's name, another key) is refused at the handshake;
// neither reaches the identity handler. A pinned key under its own id is served, member or not.
func TestIdentityIsRefusedToAnUnpinnedKey(t *testing.T) {
	c, _ := identityNodes(t, 0)
	// Nine bytes: IDENTITY_REQUEST is [nonce:8][flags:1] since the BUG-0870 guard, and the 8-byte form
	// is refused. All zero is nonce 0 with the admit bit clear, so the served ask admits nothing.
	request := transport.Message{Env: transport.Envelope{Version: transport.EnvelopeVersion, Kind: transport.KindIdentityRequest}, Body: make([]byte, 9)}
	for name, rogue := range map[string]*harness.Rogue{
		"an unpinned key":             c.NewUnpinnedRogue("ghost"),
		"a pinned key speaking as n1": c.NewRogue("n1", false),
	} {
		_, err := rogue.Call("n1", transport.ChannelIdentity, request)
		var refusal *transport.WireError
		if err == nil || errors.As(err, &refusal) {
			t.Errorf("%s reached the identity handler (err %v); the connection must be dropped first", name, err)
		}
	}
	// [BREAK: AdmitIdentity admits any pinned key whatever id it claims]: a ghost pinned under
	// its own id is a pinned non-member and is answered.
	if reply, err := c.NewRogue("ghost", false).Call("n1", transport.ChannelIdentity, request); err != nil || reply.Env.Kind != transport.KindIdentityReply {
		t.Errorf("a pinned key under its own id was not answered: %v", err)
	}
}

// TestAnIdentityReplyNamingAnotherIdIsRefused (I1): the operator typed n2 and pinned n3's key
// under it; the key authenticates, the reply says n3, and the join is refused naming the fix.
// [BREAK: skip the node_id compare in verifyIdentity].
func TestAnIdentityReplyNamingAnotherIdIsRefused(t *testing.T) {
	c, leader := identityNodes(t, 1)
	other := c.OpenJoiner()
	_, err := leader.AskIdentity(askCtx(t), "n1", c.Addr(other))
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeClusterMismatch {
		t.Fatalf("asking the key of %s as n1: %v, want CLUSTER_MISMATCH", other, err)
	}
	for _, want := range []string{"presented the key pinned under n1 but says it is " + other, "run 'nodes'", "hash_ring.seed_fingerprints", "retry 'join n1@" + c.Addr(other) + "'"} {
		if !strings.Contains(refusal.Message, want) {
			t.Errorf("I1 text lacks %q:\n%s", want, refusal.Message)
		}
	}
}

// TestAnIdentityToANodeNobodyPinnedIsRefusedBeforeAnyDial (I2): no pin under the id typed.
func TestAnIdentityToANodeNobodyPinnedIsRefusedBeforeAnyDial(t *testing.T) {
	c, leader := identityNodes(t, 0)
	_, err := leader.AskIdentity(askCtx(t), "ghost", c.Addr("n1"))
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument || !strings.Contains(refusal.Message, "hash_ring.seed_fingerprints") {
		t.Fatalf("asking an id nobody pinned: %v", err)
	}
}

// TestAnUnansweredIdentityRequestSaysNothingWasChanged (I5).
func TestAnUnansweredIdentityRequestSaysNothingWasChanged(t *testing.T) {
	c, leader := identityNodes(t, 1)
	// The late node's key is pinned but nothing listens for it: it was never opened.
	_, err := leader.AskIdentity(askCtx(t), "n2", c.Addr("n2"))
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeUnavailable || !strings.Contains(refusal.Message, "Nothing was changed") {
		t.Fatalf("asking a node that is not running: %v", err)
	}
}

// TestAnAlreadyBootstrappedNodeNamesAnotherCluster: a node that started a cluster of its own
// names that cluster, and the join is refused CLUSTER_MISMATCH.
func TestAnAlreadyBootstrappedNodeNamesAnotherCluster(t *testing.T) {
	c, leader := identityNodes(t, 1)
	alone := c.OpenBootstrapped()
	c.Await("the lone node to commit its own cluster", 15*time.Second, func() bool { return c.Node(alone).State().StateSeq > 0 })
	_, err := leader.AskIdentity(askCtx(t), alone, c.Addr(alone))
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeClusterMismatch {
		t.Fatalf("asking a node of another cluster: %v", err)
	}
}
