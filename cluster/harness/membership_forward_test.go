package harness_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// joinCluster is a one-node cluster with voters capped at three and late identities to join.
func joinCluster(t *testing.T, size, late int, extra func(*harness.Options)) *harness.Cluster {
	t.Helper()
	opts := harness.Options{Late: late, Voters: 3, NoDetection: true}
	if extra != nil {
		extra(&opts)
	}
	return harness.NewCluster(t, size, opts)
}

func joinAt(t *testing.T, c *harness.Cluster, entry, id string, witness bool) (wire.MembershipOutcome, error) {
	t.Helper()
	return c.Membership(entry).Join(askCtx(t), id, c.Addr(id), witness)
}

func mustJoin(t *testing.T, c *harness.Cluster, entry, id string) wire.MembershipOutcome {
	t.Helper()
	got, err := joinAt(t, c, entry, id, false)
	if err != nil {
		t.Fatalf("join %s at %s: %v", id, entry, err)
	}
	return got
}

func memberOf(state cluster.ClusterState, id string) (cluster.Member, bool) {
	for _, m := range state.Members {
		if m.ID == id {
			return m, true
		}
	}
	return cluster.Member{}, false
}

// TestAJoinEnteredOnAFollowerRunsOnTheLeader: the statement runs on any member; a follower
// forwards it and the answer is the leader's.
func TestAJoinEnteredOnAFollowerRunsOnTheLeader(t *testing.T) {
	c := joinCluster(t, 2, 1, nil)
	leader := c.AwaitLeader(10 * time.Second)
	follower := "n1"
	if leader == "n1" {
		follower = "n2"
	}
	joiner := c.OpenJoiner()
	got := mustJoin(t, c, follower, joiner)
	if !strings.HasPrefix(got.Result, "Added "+joiner+"@") {
		t.Fatalf("line %q", got.Result)
	}
	state := c.AwaitConverged(15 * time.Second)
	if m, ok := memberOf(state, joiner); !ok || m.State != cluster.MemberActive {
		t.Fatalf("after a forwarded join %s is %+v", joiner, m)
	}
	if c.Node(follower).State().StateSeq < got.StateSeq {
		t.Fatalf("the entry node has not applied the position it answered: %d < %d", c.Node(follower).State().StateSeq, got.StateSeq)
	}
}

func membershipMessage(t *testing.T, id byte, cmd byte, node, addr string, witness bool) transport.Message {
	t.Helper()
	msg, err := transport.NewControlMessage(transport.Envelope{}, transport.MembershipRelay{
		RequestID: [transport.RelayRequestIDLen]byte{id}, Cmd: cmd, Witness: witness, NodeID: node, DataAddr: addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	return msg
}

func membershipReplyOf(t *testing.T, msg *transport.Message, err error) transport.MembershipRelayReply {
	t.Helper()
	if err != nil {
		t.Fatalf("the membership relay call: %v", err)
	}
	decoded, err := msg.Control()
	if err != nil {
		t.Fatal(err)
	}
	reply, ok := decoded.(transport.MembershipRelayReply)
	if !ok {
		t.Fatalf("answered %T", decoded)
	}
	return reply
}

// TestTheLeaderNeverForwardsAMembershipCommand: a node that does not lead answers NOT_LEADER with
// the leader it knows and changes nothing; the command is never passed on a second hop.
// [BREAK: serveMembershipRelay forwards on NOT_LEADER instead of answering it].
func TestTheLeaderNeverForwardsAMembershipCommand(t *testing.T) {
	c := joinCluster(t, 2, 1, nil)
	leader := c.AwaitLeader(10 * time.Second)
	follower := "n1"
	if leader == "n1" {
		follower = "n2"
	}
	before := c.AwaitConverged(10 * time.Second).StateSeq
	joiner := c.OpenJoiner()
	msg, err := c.CallAs(leader, follower, transport.ChannelControl, membershipMessage(t, 1, transport.MembershipJoin, joiner, c.Addr(joiner), false))
	reply := membershipReplyOf(t, msg, err)
	if reply.Status != transport.MembershipNotLeader || reply.Leader != leader {
		t.Fatalf("a follower answered %+v, want NOT_LEADER naming %s", reply, leader)
	}
	if after := c.Node(leader).State().StateSeq; after != before {
		t.Fatalf("the follower changed the state: state_seq %d -> %d", before, after)
	}
}

// TestAForwardedJoinRetriedAfterALostReplyIsAnsweredFromTheLedger: the same request id from the same
// sender gets the stored answer, not a second execution.
func TestAForwardedJoinRetriedAfterALostReplyIsAnsweredFromTheLedger(t *testing.T) {
	c := joinCluster(t, 2, 1, nil)
	leader := c.AwaitLeader(10 * time.Second)
	follower := "n1"
	if leader == "n1" {
		follower = "n2"
	}
	joiner := c.OpenJoiner()
	send := func() transport.MembershipRelayReply {
		msg, err := c.CallAs(follower, leader, transport.ChannelControl, membershipMessage(t, 7, transport.MembershipJoin, joiner, c.Addr(joiner), false))
		return membershipReplyOf(t, msg, err)
	}
	first := send()
	second := send()
	if first.Status != transport.MembershipDone || second != first {
		t.Fatalf("first %+v, repeat %+v: want the same stored answer", first, second)
	}
	if got := c.Node(leader).MembershipReplays(); got != 1 {
		t.Fatalf("the leader answered %d repeats from its ledger, want 1", got)
	}
}

// TestARequestIdReusedForAnotherCommandIsNotAnsweredFromTheLedger (pi N2): the ledger remembers what
// a request id was asked, so a member that reuses an id for another node is refused instead of being
// handed the first command's stored answer. [BREAK: begin ignores the ask]
func TestARequestIdReusedForAnotherCommandIsNotAnsweredFromTheLedger(t *testing.T) {
	c := joinCluster(t, 2, 2, nil)
	leader := c.AwaitLeader(10 * time.Second)
	follower := "n1"
	if leader == "n1" {
		follower = "n2"
	}
	first, second := c.OpenJoiner(), c.OpenJoiner()
	msg, err := c.CallAs(follower, leader, transport.ChannelControl, membershipMessage(t, 21, transport.MembershipJoin, first, c.Addr(first), false))
	if reply := membershipReplyOf(t, msg, err); reply.Status != transport.MembershipDone {
		t.Fatalf("the first join: %+v", reply)
	}
	msg, err = c.CallAs(follower, leader, transport.ChannelControl, membershipMessage(t, 21, transport.MembershipJoin, second, c.Addr(second), false))
	reply := membershipReplyOf(t, msg, err)
	if reply.Status != transport.MembershipRefused || !strings.Contains(reply.Message, "already used for a different membership command") {
		t.Fatalf("a reused request id answered %+v", reply)
	}
	if _, ok := memberOf(c.Node(leader).State(), second); ok {
		t.Fatal("the reused id registered the second node")
	}
}

// TestAnActivateRelayFromAClientIsRefused: an activation is the joiner's own ask. A node that is no
// member is dropped at the handshake; a member may ask only for itself.
func TestAnActivateRelayFromAClientIsRefused(t *testing.T) {
	c := joinCluster(t, 2, 1, nil)
	leader := c.AwaitLeader(10 * time.Second)
	other := "n1"
	if leader == "n1" {
		other = "n2"
	}
	joiner := c.OpenJoiner()
	mustJoin(t, c, leader, joiner)
	ask := membershipMessage(t, 3, transport.MembershipActivate, joiner, "", false)
	var refusal *transport.WireError
	if _, err := c.NewRogue("ghost", false).Call(leader, transport.ChannelControl, ask); err == nil || errors.As(err, &refusal) {
		t.Fatalf("a pinned non-member reached the leader's handler: %v", err)
	}
	msg, err := c.CallAs(other, leader, transport.ChannelControl, ask)
	reply := membershipReplyOf(t, msg, err)
	if reply.Status != transport.MembershipRefused || cluster.Code(reply.Code) != cluster.CodePermissionDenied || !strings.Contains(reply.Message, "only itself") {
		t.Fatalf("%s asked to activate %s and was answered %+v", other, joiner, reply)
	}
}

// TestAnActivateAskForAnActiveMemberCommitsNothingNew (pi S2): the joiner's ask changes a member
// that is joining; an active member that asks again is told so and the state does not move, however
// fresh the request id. [BREAK: activateOnLeader activates whatever the row says]
func TestAnActivateAskForAnActiveMemberCommitsNothingNew(t *testing.T) {
	c := joinCluster(t, 2, 1, nil)
	leader := c.AwaitLeader(10 * time.Second)
	joiner := c.OpenJoiner()
	mustJoin(t, c, leader, joiner)
	before := c.Node(leader).State().StateSeq
	for id := byte(10); id < 13; id++ {
		msg, err := c.CallAs(joiner, leader, transport.ChannelControl, membershipMessage(t, id, transport.MembershipActivate, joiner, "", false))
		if reply := membershipReplyOf(t, msg, err); reply.Status != transport.MembershipDone || !strings.Contains(reply.Result, "already active") {
			t.Fatalf("ask %d answered %+v", id, reply)
		}
	}
	if after := c.Node(leader).State().StateSeq; after != before {
		t.Fatalf("three asks from an active member moved state_seq %d -> %d", before, after)
	}
}

// TestAForwardWithNoLeaderRefusesNamingNodes (F1): a member that is not the leader and knows none
// refuses with the fix and changes nothing.
func TestAForwardWithNoLeaderRefusesNamingNodes(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{Late: 1, Voters: 3, NoDetection: true})
	joiner := c.OpenJoiner()
	survivor := "n3"
	c.Kill("n1")
	c.Kill("n2")
	c.Await("the survivor to lose its leader", 20*time.Second, func() bool { return c.Node(survivor).LeaderID() == "" })
	_, err := joinAt(t, c, survivor, joiner, false)
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeUnavailable {
		t.Fatalf("a join with no leader: %v", err)
	}
	for _, text := range []string{"join could not reach the Raft leader", "no leader is elected", "Nothing was changed", "Run 'nodes'"} {
		if !strings.Contains(refusal.Message, text) {
			t.Errorf("F1 text lacks %q:\n%s", text, refusal.Message)
		}
	}
}
