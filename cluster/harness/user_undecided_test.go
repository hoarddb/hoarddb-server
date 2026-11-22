package harness_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A user command tells its proposer whether its outcome is known (ring user store spec 6.1 step
// 6, 6.2 B3, step M2a.3): the binding answers "nothing changed" only for a decided ending and the
// may-have-been-applied text after any attempt whose outcome was lost.

// userCreateFrom builds a create of name by root on node's applied table.
func userCreateFrom(t *testing.T, node *wire.Node, key userCredential, name string) []byte {
	t.Helper()
	view := node.Users()
	root, ok := view.User("root")
	if !ok {
		t.Fatal("no root row")
	}
	return cluster.UserCreateBody{Request: userRequestID(), By: "root", BySeq: root.CreatedSeq, Name: name, ExpectDropSeq: view.DropSeq,
		Roles: nil, Credential: key.cred, Enabled: true, CreatedMs: 1}.Encode()
}

// TestAUserCommandRelayedToALeaderWithoutAQuorumIsUndecided (B3): the leader took the relayed
// command but could not commit it, so the proposer cannot know whether it will be: the error is
// a *wire.UndecidedError. Break: return the bare refusal; red.
func TestAUserCommandRelayedToALeaderWithoutAQuorumIsUndecided(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{ClusterPepper: true, HoldSeedReports: true, NoDetection: true, ProposalBudget: 2 * time.Second})
	leader, followers := relayLeaderAndFollowers(t, c)
	pepper, _ := bootstrapUsersOn(t, c, leader)
	awaitHeld(t, c, followers[0], 10*time.Second)
	c.AwaitConverged(10 * time.Second)
	cred := credentialUnder(pepper.Key(c.Node(leader).State().ClusterID), pepper.KID(), "ada-password-12", 0x61)
	body := userCreateFrom(t, c.Node(followers[0]), cred, "ada")
	c.Kill(followers[1])
	c.DropChannel(leader, followers[0], transport.ChannelRaft, 1_000_000)
	c.DropChannel(followers[0], leader, transport.ChannelRaft, 1_000_000)
	_, err := c.Node(followers[0]).ProposeUserCommand(proofCtx(t), cluster.CommandUserCreate, body)
	var undecided *wire.UndecidedError
	if !errors.As(err, &undecided) {
		t.Fatalf("a relayed command the leader could not commit answered %v, want a *wire.UndecidedError", err)
	}
}

// TestAUserCommandWithNoLeaderKnownIsDecided (B3): a node that knows no leader sends nothing, so
// its ending is decided: the error is not a *wire.UndecidedError. Break: mark every ending
// undecided; red.
func TestAUserCommandWithNoLeaderKnownIsDecided(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{ClusterPepper: true, HoldSeedReports: true, NoDetection: true, ProposalBudget: 2 * time.Second})
	leader, followers := relayLeaderAndFollowers(t, c)
	pepper, _ := bootstrapUsersOn(t, c, leader)
	awaitHeld(t, c, followers[0], 10*time.Second)
	c.AwaitConverged(10 * time.Second)
	cred := credentialUnder(pepper.Key(c.Node(leader).State().ClusterID), pepper.KID(), "ada-password-12", 0x62)
	alone := followers[0]
	body := userCreateFrom(t, c.Node(alone), cred, "ada")
	c.Kill(leader)
	c.Kill(followers[1])
	c.Await(alone+" to know no leader", 10*time.Second, func() bool { return c.Node(alone).LeaderID() == "" })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := c.Node(alone).ProposeUserCommand(ctx, cluster.CommandUserCreate, body)
	var undecided *wire.UndecidedError
	if err == nil || errors.As(err, &undecided) {
		t.Fatalf("a node with no leader known answered %v, want a decided refusal", err)
	}
}
