package harness_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// guardWindow is how long a held node is watched for state it must not receive. The harness's Raft
// timeout is 150 ms, so it is more than thirty heartbeats: an unguarded node is caught up within the
// first few (the red runs of these tests catch up in well under a second).
const guardWindow = 5 * time.Second

// requireNoClusterState fails the test if node id applies any committed state within guardWindow.
func requireNoClusterState(t *testing.T, c *harness.Cluster, id, while string) {
	t.Helper()
	deadline := time.Now().Add(guardWindow)
	for time.Now().Before(deadline) {
		if seq := c.Node(id).State().StateSeq; seq != 0 {
			t.Fatalf("%s: %s applied cluster state (state_seq %d) although no member admitted it", while, id, seq)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// awaitOwnRow waits until node id has applied a state in which its own row is active.
func awaitOwnRow(t *testing.T, c *harness.Cluster, id string) {
	t.Helper()
	c.Await(id+" to hold its own active row", 30*time.Second, func() bool {
		row, ok := memberOf(c.Node(id).State(), id)
		return ok && row.State == cluster.MemberActive
	})
}

// followerOf is a running node that does not lead now.
func followerOf(t *testing.T, c *harness.Cluster) string {
	t.Helper()
	leader := c.AwaitLeader(10 * time.Second)
	for _, id := range c.Running() {
		if id != leader {
			return id
		}
	}
	t.Fatal("no follower is running")
	return ""
}

// TestAMemberWhoseRingWasMovedAsideStaysOutOfRaftUntilAdmitted: [GUARD BUG-0870 guard (design
// section 3): three voters hold an RF 2 database with documents; one member's ring directory is
// removed while its key and pins stay, and it is started again on the empty directory. It takes no
// Raft traffic: its state_seq stays 0 for the whole window, the leader's configuration still lists
// it as a voter, and a 'join' of its known id answers from the row without admitting it]. Red when
// the node admits the leader's Raft dial at once, is caught up as the voter it was with an empty log
// (Raft amnesia: the leader still counts it at its old match index) and its pairs with a stale own bit
// become ready empty.
func TestAMemberWhoseRingWasMovedAsideStaysOutOfRaftUntilAdmitted(t *testing.T) {
	const (
		database = "gd"
		bucket   = "Box"
	)
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true, WCTimeout: 5 * time.Second})
	bug0834Retry(t, "creating the database", func(ctx context.Context) error {
		_, err := c.Node(c.AwaitLeader(30*time.Second)).CreateDatabaseRF(ctx, database, 2)
		return err
	})
	c.AwaitConverged(30 * time.Second)
	bug0834Retry(t, "creating the bucket", func(ctx context.Context) error {
		_, err := c.Node(c.AwaitLeader(30*time.Second)).CreateBucket(ctx, database, bucket, wire.Field{Name: "name", Type: "string"})
		return err
	})
	for i := range 6 {
		id := fmt.Sprintf("doc-%d", i)
		owner, _, err := c.Node(c.IDs()[0]).Owner(proofCtx(t), database, bucket, id)
		if err != nil {
			t.Fatal(err)
		}
		bug0834Retry(t, "inserting "+id, func(ctx context.Context) error {
			_, err := c.Node(owner).Insert(ctx, database, bucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}})
			return err
		})
	}
	before := c.AwaitConverged(30 * time.Second)
	victim := followerOf(t, c)
	if !slices.Contains(before.Voters, victim) {
		t.Fatalf("%s is not a voter before the wipe: %v", victim, before.Voters)
	}

	c.WipeRing(victim)
	c.RestartUnsynced(victim)
	requireNoClusterState(t, c, victim, "after the restart on an empty ring directory")

	leader := c.AwaitLeader(10 * time.Second)
	if state := c.Node(leader).State(); !slices.Contains(state.Voters, victim) || state.StateSeq < before.StateSeq {
		t.Fatalf("the leader's state changed under the held node: voters %v, state_seq %d (was %d)", state.Voters, state.StateSeq, before.StateSeq)
	}
	// A join of the known id is the "already a member" answer: it asks the node nothing, so it must not
	// admit it either.
	if _, err := joinAt(t, c, leader, victim, false); err != nil {
		t.Fatalf("join of the known id %s: %v", victim, err)
	}
	requireNoClusterState(t, c, victim, "after a join of its known id")
}

// TestAnIdentityAskWithoutAdmitLeavesTheRaftChannelClosed: [GUARD BUG-0870 guard: only an identity
// request with the admit bit opens a held node's Raft channel; the plain ask (AskIdentity, which
// changes nothing on either node) answers and leaves it held]. Red when any ask, or none, admits it.
func TestAnIdentityAskWithoutAdmitLeavesTheRaftChannelClosed(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true})
	victim := followerOf(t, c)
	c.WipeRing(victim)
	c.RestartUnsynced(victim)
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	got, err := leader.AskIdentity(askCtx(t), victim, c.Addr(victim))
	if err != nil {
		t.Fatalf("the held node did not answer the identity ask: %v", err)
	}
	if got.HoldsRaftState || got.ClusterID != [16]byte{} {
		t.Fatalf("the wiped node says it holds state: %+v", got)
	}
	requireNoClusterState(t, c, victim, "after an identity ask without the admit bit")
}

// TestAFreshJoinerIsAdmittedByItsJoinsIdentityAsk: [GUARD BUG-0870 guard: the guard costs a normal
// join nothing; the join's own identity ask admits the new node, and the one statement ends with the
// node active and holding the cluster's state]. Red when runJoin asks without the admit bit: the new
// voter never takes the leader's Raft traffic and its promotion cannot commit.
func TestAFreshJoinerIsAdmittedByItsJoinsIdentityAsk(t *testing.T) {
	c := joinCluster(t, 1, 1, nil)
	joiner := c.OpenJoiner()
	got := joinAdmitting(t, c, joiner)
	row, ok := memberOf(c.Node(c.AwaitLeader(10*time.Second)).State(), joiner)
	if !ok || row.State != cluster.MemberActive {
		t.Fatalf("after one join the row is %+v (found %v); outcome %q", row, ok, got.Result)
	}
	awaitOwnRow(t, c, joiner)
}

// TestAJoinerRestartedBeforeItsFirstRaftWriteIsAdmittedByTheRetriedJoin: [GUARD BUG-0870 guard: a
// node whose joining row is committed but which holds no Raft state (here: registered, then its ring
// directory removed, which is the directory a joiner restarted before its first Raft write starts
// on) is held after its restart, and the retried 'join' (checkJoinerRange's identity ask) admits it
// and finishes the join]. Red on the first half when the restarted node is caught up without being
// asked, and on the second when the retry asks without the admit bit.
func TestAJoinerRestartedBeforeItsFirstRaftWriteIsAdmittedByTheRetriedJoin(t *testing.T) {
	t.Skip("BUG-0886: a retried join promotes a joiner the leader has no Raft contact with yet, so the sole voter can step down, and a later retry can be refused as a lost voter; skipped during the feature freeze (owner 2026-11-21), to be fixed after release")
	c := joinCluster(t, 1, 1, nil)
	joiner := c.OpenJoiner()
	leader := c.AwaitLeader(10 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Node(leader).JoinRegisterOnly(ctx, joiner, c.Addr(joiner)); err != nil {
		t.Fatalf("registering %s: %v", joiner, err)
	}
	c.WipeRing(joiner)
	c.RestartUnsynced(joiner)
	requireNoClusterState(t, c, joiner, "after the restart with no Raft state")

	joinAdmitting(t, c, joiner)
	awaitOwnRow(t, c, joiner)
}

// TestARetriedJoinDoesNotAdmitAVoterThatLostItsRaftState: [GUARD BUG-0870 guard, review F2: a member
// registered and promoted but not yet activated (row joining, a voter) whose ring directory is lost is
// held after its restart, and the retried 'join' refuses it, naming that a voter that lost its data
// cannot be brought back, instead of admitting it: a voter caught up with an empty log has forgotten
// its term and vote]. Reachable only on a cluster with no database, between promote and activate. Red
// while checkJoinerRange sends the admit bit to a voter.
func TestARetriedJoinDoesNotAdmitAVoterThatLostItsRaftState(t *testing.T) {
	c, leader, victim := promotedJoiners(t)
	state := c.Node(leader).State()
	row, _ := memberOf(state, victim)
	if row.State != cluster.MemberJoining || !slices.Contains(state.Voters, victim) {
		t.Fatalf("the scenario needs a joining voter: row %+v, voters %v", row, state.Voters)
	}
	c.WipeRing(victim)
	c.RestartUnsynced(victim)
	requireNoClusterState(t, c, victim, "after the restart with no Raft state")
	_, err := joinAt(t, c, c.AwaitLeader(10*time.Second), victim, false)
	if err == nil {
		t.Fatalf("the retried join of the voter %s that lost its Raft state was accepted", victim)
	}
	for _, want := range []string{"is a voter", "holds no cluster state", "'replace-node " + victim + " --with " + victim + "@" + c.Addr(victim) + "'", "Nothing was changed"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal lacks %q: %v", want, err)
		}
	}
	if !c.Node(victim).AwaitingAdmission() {
		t.Fatal("the refused join admitted the voter")
	}
	requireNoClusterState(t, c, victim, "after the refused join")
}

// joinAdmitting runs 'join id' on the leader and fails the test naming the admission when the node is
// still held afterwards: a join whose identity ask did not carry the admit bit leaves the node held,
// and its later steps fail for that reason only.
func joinAdmitting(t *testing.T, c *harness.Cluster, id string) wire.MembershipOutcome {
	t.Helper()
	got, err := joinAt(t, c, c.AwaitLeader(10*time.Second), id, false)
	if c.Node(id).AwaitingAdmission() {
		t.Fatalf("the join's identity ask did not admit %s: it is still held (join answered %q, %v)", id, got.Result, err)
	}
	if err != nil {
		t.Fatalf("join %s: %v", id, err)
	}
	return got
}

// promotedJoiners forms one member and two joiners registered and promoted but not activated, and
// returns the leader and the second joiner.
func promotedJoiners(t *testing.T) (*harness.Cluster, string, string) {
	t.Helper()
	c := joinCluster(t, 1, 2, nil)
	leader := c.AwaitLeader(10 * time.Second)
	var last string
	for range 2 {
		last = c.OpenJoiner()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := c.Node(leader).JoinRegisterOnly(ctx, last, c.Addr(last)); err != nil {
			cancel()
			t.Fatalf("registering %s: %v", last, err)
		}
		if err := c.Node(leader).Promote(ctx, last); err != nil {
			cancel()
			t.Fatalf("promoting %s: %v", last, err)
		}
		cancel()
	}
	return c, leader, last
}

// TestARetriedJoinDoesNotAdmitANodeTheLibraryCountsAsAVoterBeforeSetVoters: [GUARD BUG-0870 guard,
// review round 2 A-1: PromoteWithin commits the library's AddVoter first and SET_VOTERS second, so in
// between the library counts the node as a voter and the committed voters list does not. A wiped node
// in that window is refused by the retried join, not admitted at its old match index]. The window is
// held by a seam between the two steps (it also holds voterMu, so ReconcileVoters cannot close it).
// Red while checkJoinerRange reads only the committed voters list.
func TestARetriedJoinDoesNotAdmitANodeTheLibraryCountsAsAVoterBeforeSetVoters(t *testing.T) {
	c := joinCluster(t, 1, 2, nil)
	leader := c.AwaitLeader(10 * time.Second)
	first := c.OpenJoiner()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := c.Node(leader).JoinRegisterOnly(ctx, first, c.Addr(first)); err != nil {
		t.Fatalf("registering %s: %v", first, err)
	}
	if err := c.Node(leader).Promote(ctx, first); err != nil {
		t.Fatalf("promoting %s: %v", first, err)
	}
	victim := c.OpenJoiner()
	if err := c.Node(leader).JoinRegisterOnly(ctx, victim, c.Addr(victim)); err != nil {
		t.Fatalf("registering %s: %v", victim, err)
	}
	inWindow, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	cluster.HoldPromotionsForTest(func(id string) {
		if id == victim {
			close(inWindow)
			<-release
		}
	})
	unhold := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unhold(); cluster.HoldPromotionsForTest(nil) })
	promoted := make(chan error, 1)
	go func() { promoted <- c.Node(leader).Promote(ctx, victim) }()
	<-inWindow
	if slices.Contains(c.Node(leader).State().Voters, victim) {
		t.Fatal("the committed voters list already names the victim: the window is not held")
	}

	c.WipeRing(victim)
	c.RestartUnsynced(victim)
	requireNoClusterState(t, c, victim, "after the restart with no Raft state")
	// An admitted node would be promoted by the join, which waits on the held promotion's lock: the
	// join runs aside and the admission is judged on the node, not on the join's answer.
	joined := make(chan error, 1)
	go func() {
		_, err := c.Membership(leader).Join(ctx, victim, c.Addr(victim), false)
		joined <- err
	}()
	select {
	case err := <-joined:
		if err == nil || !strings.Contains(err.Error(), "is a voter") {
			t.Errorf("the retried join of %s, a voter in the library's configuration, answered %v; want the voter refusal", victim, err)
		}
	case <-time.After(20 * time.Second):
		t.Error("the retried join did not answer within 20 s: it went on to promote the node, so it admitted it")
	}
	if !c.Node(victim).AwaitingAdmission() {
		unhold()
		t.Fatal("the retried join admitted a node the library counts as a voter, with no Raft state")
	}
	requireNoClusterState(t, c, victim, "after the refused join")
	unhold()
	if err := <-promoted; err != nil {
		t.Logf("the held promotion ended with %v", err)
	}
}

// TestAPromotedVoterRestartedWithItsStateIsNotBlockedByTheRetriedJoin: [GUARD BUG-0870 guard, review
// round 2: the F2 refusal is for a voter that holds no Raft state only; a promoted joining voter
// restarted on its own directory says it holds state, is not held at start, and the retried join
// finishes it as before]. Red if the voter rule refused every voter.
func TestAPromotedVoterRestartedWithItsStateIsNotBlockedByTheRetriedJoin(t *testing.T) {
	c, leader, victim := promotedJoiners(t)
	c.Kill(victim)
	c.RestartUnsynced(victim)
	if c.Node(victim).AwaitingAdmission() {
		t.Fatal("a voter restarted on its own Raft state is held")
	}
	if _, err := joinAt(t, c, leader, victim, false); err != nil {
		t.Fatalf("the retried join of a voter that kept its state was refused: %v", err)
	}
	awaitOwnRow(t, c, victim)
}
