package harness_test

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

var relayOwnsPattern = regexp.MustCompile(`(\S+) owns the declaration`)

// TestAlterOnADeclarationOwnerThatIsNotTheLeaderCompletes: the declaration's owner is a
// follower, so its BUMP_SCHEMA must reach the Raft leader, which is another node. Before the
// relay the alter wrote its pending table, then dead-ended on "not the Raft leader" (and a
// run on the leader said "connect to the owner": the two refusals pointed at each other).
func TestAlterOnADeclarationOwnerThatIsNotTheLeaderCompletes(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	leaderID := c.AwaitLeader(10 * time.Second)
	leader := c.Node(leaderID)
	for i := 0; i < 16; i++ {
		name := fmt.Sprintf("relayalt%d", i)
		if _, err := leader.CreateBucket(proofCtx(t), proofDB, name, wire.Field{Name: "name", Type: "string"}); err != nil {
			t.Fatalf("creating bucket %s: %v", name, err)
		}
		// The leader either owns this declaration (the alter runs, try another bucket) or
		// refuses by naming the owner without writing anything.
		_, err := leader.AlterBucket(proofCtx(t), proofDB, name, wire.AlterChange{Add: []wire.FieldEntry{{Name: "probe", Type: "int"}}})
		if err == nil {
			continue
		}
		found := relayOwnsPattern.FindStringSubmatch(err.Error())
		if found == nil || !slices.Contains(c.IDs(), found[1]) {
			t.Fatalf("the leader's refusal does not name the declaration owner: %v", err)
		}
		owner := found[1]
		if owner == leaderID {
			t.Fatalf("the leader %s refused an alter for a declaration it owns: %v", leaderID, err)
		}
		table, err := c.Node(owner).AlterBucket(proofCtx(t), proofDB, name, wire.AlterChange{Add: []wire.FieldEntry{{Name: "age", Type: "int"}}})
		if err != nil {
			t.Fatalf("alter of %s on its declaration owner %s (a follower; the leader is %s): %v", name, owner, leaderID, err)
		}
		if !slices.ContainsFunc(table.Entries, func(entry machine.FieldEntry) bool { return entry.Name == "age" }) {
			t.Fatalf("the alter returned a table without the new field: %+v", table)
		}
		c.AwaitConverged(10 * time.Second)
		return
	}
	t.Fatalf("no bucket in 16 had a declaration owner other than the leader %s", leaderID)
}

// TestAProposalDuringLeaderLossAppliesOnceAfterReelection: the leader dies while a follower
// proposes CREATE_DATABASE. The proposal waits for the new leader and commits exactly once:
// one database, one id taken, one state_seq step.
func TestAProposalDuringLeaderLossAppliesOnceAfterReelection(t *testing.T) {
	// A held reporter: its background SEEDED and RETIRE move state_seq, and this test needs it exact.
	c := harness.NewCluster(t, 3, harness.Options{HoldSeedReports: true, NoDetection: true})
	leaderID := c.AwaitLeader(10 * time.Second)
	var proposer string
	for _, id := range c.IDs() {
		if id != leaderID {
			proposer = id
			break
		}
	}
	before := c.AwaitConverged(10 * time.Second)
	c.Kill(leaderID)
	id, err := c.Node(proposer).CreateDatabaseRF(proofCtx(t), "relaydb", 1)
	if err != nil {
		t.Fatalf("create database from %s while the leader %s was lost: %v", proposer, leaderID, err)
	}
	after := c.AwaitConverged(15 * time.Second)
	count := 0
	for _, db := range after.Databases {
		if db.Name == "relaydb" {
			count++
			if uint64(db.ID) != id {
				t.Fatalf("database relaydb has id %d, the proposal returned %d", db.ID, id)
			}
		}
	}
	if count != 1 {
		t.Fatalf("%d databases named relaydb, want exactly 1", count)
	}
	if after.NextDatabaseID != before.NextDatabaseID+1 || after.StateSeq != before.StateSeq+1 {
		t.Fatalf("next_database_id %d -> %d and state_seq %d -> %d, want one step each", before.NextDatabaseID, after.NextDatabaseID, before.StateSeq, after.StateSeq)
	}
}

// relaySetting is a SET_SETTING body for dead_ms, a setting nothing in these tests reads.
func relaySetting(millis int64) []byte {
	return bsoncore.BuildDocument(nil, append(bsoncore.AppendStringElement(nil, "key", "dead_ms"), bsoncore.AppendInt64Element(nil, "value", millis)...))
}

func relayMessage(t *testing.T, id byte, kind byte, body []byte) transport.Message {
	t.Helper()
	msg, err := transport.NewControlMessage(transport.Envelope{}, transport.RelayProposal{RequestID: [transport.RelayRequestIDLen]byte{id}, Kind: kind, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	return msg
}

func relayCall(t *testing.T, c *harness.Cluster, from, to string, ch transport.Channel, msg transport.Message) transport.RelayReply {
	t.Helper()
	reply, err := c.CallAs(from, to, ch, msg)
	if err != nil {
		t.Errorf("the relayed proposal got no answer: %v", err)
		return transport.RelayReply{}
	}
	decoded, err := reply.Control()
	if err != nil {
		t.Errorf("the answer to a relayed proposal is unreadable: %v", err)
		return transport.RelayReply{}
	}
	answer, ok := decoded.(transport.RelayReply)
	if !ok {
		t.Errorf("answered with %T, want RELAY_PROPOSAL_REPLY", decoded)
	}
	return answer
}

func relayLeaderAndFollowers(t *testing.T, c *harness.Cluster) (leader string, followers []string) {
	t.Helper()
	leader = c.AwaitLeader(10 * time.Second)
	for _, id := range c.IDs() {
		if id != leader {
			followers = append(followers, id)
		}
	}
	return leader, followers
}

// TestARelayedProposalFromARoguePeerReachesNoHandler: the relay handler sits behind the same
// admission as every other peer message. A pinned non-member, and a pinned key speaking as a
// member, with the true cluster id and with the zero one, get the connection dropped; the
// cluster's state does not move.
func TestARelayedProposalFromARoguePeerReachesNoHandler(t *testing.T) {
	// A held reporter: its background SEEDED and RETIRE move state_seq, and this test needs it exact.
	c := proofSetup(t, harness.Options{HoldSeedReports: true, NoDetection: true})
	leader, _ := relayLeaderAndFollowers(t, c)
	before := c.Node(leader).State()
	for _, tc := range []struct {
		name, claim string
		zero        bool
	}{
		{"non-member", "ghost", false}, {"non-member zero cluster id", "ghost", true},
		{"impersonating a member", "n1", false}, {"impersonating a member zero cluster id", "n1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rogue := c.NewRogue(tc.claim, tc.zero)
			_, err := rogue.Call(leader, transport.ChannelControl, relayMessage(t, 1, cluster.CommandSetSetting, relaySetting(111)))
			var refusal *transport.WireError
			if err == nil || errors.As(err, &refusal) {
				t.Errorf("a relayed proposal as %q reached the handler (err %v); the connection must be dropped before it runs", tc.claim, err)
			}
		})
	}
	if after := c.Node(leader).State(); after.StateSeq != before.StateSeq || after.Settings.DeadMs == 111 {
		t.Fatalf("a rogue's proposal moved the cluster: state_seq %d -> %d, dead_ms %d", before.StateSeq, after.StateSeq, after.Settings.DeadMs)
	}
	genuine := c.CallAsWithoutClusterID
	if _, err := genuine("n1", leader, transport.ChannelControl, relayMessage(t, 2, cluster.CommandSetSetting, relaySetting(112))); err == nil || errors.As(err, new(*transport.WireError)) {
		t.Fatalf("a member without the cluster id reached the handler: %v", err)
	}
}

// TestARelayedProposalIsNeverRelayedAgain: a member that receives a RELAY_PROPOSAL and is not
// the leader refuses with the leader it knows. It does not pass the command on, so two nodes
// that disagree about the leader cannot loop it.
func TestARelayedProposalIsNeverRelayedAgain(t *testing.T) {
	// A held reporter: its background SEEDED and RETIRE move state_seq, and this test needs it exact.
	c := proofSetup(t, harness.Options{HoldSeedReports: true, NoDetection: true})
	leader, followers := relayLeaderAndFollowers(t, c)
	before := c.Node(leader).State()
	answer := relayCall(t, c, followers[0], followers[1], transport.ChannelControl, relayMessage(t, 3, cluster.CommandSetSetting, relaySetting(113)))
	if answer.Status != transport.RelayNotLeader || answer.Leader != leader {
		t.Fatalf("a follower answered a relayed proposal with %+v, want NOT_LEADER naming %s", answer, leader)
	}
	c.AwaitConverged(5 * time.Second)
	if after := c.Node(leader).State(); after.StateSeq != before.StateSeq {
		t.Fatalf("the follower applied or forwarded the command: state_seq %d -> %d", before.StateSeq, after.StateSeq)
	}
}

// TestARepeatedRelayedProposalAppliesOnce: the leader answers a repeat of (sender, request id)
// from its ledger. A second sender using the same id is a different proposal.
func TestARepeatedRelayedProposalAppliesOnce(t *testing.T) {
	// A held reporter: its background SEEDED and RETIRE move state_seq, and this test needs it exact.
	c := proofSetup(t, harness.Options{HoldSeedReports: true, NoDetection: true})
	leader, followers := relayLeaderAndFollowers(t, c)
	before := c.AwaitConverged(5 * time.Second)
	was := before.Settings.DeadMs
	send := func(from string, expect, value int64) transport.RelayReply {
		return relayCall(t, c, from, leader, transport.ChannelControl, relayMessage(t, 4, cluster.CommandSetSetting, relaySettingCAS(expect, value)))
	}
	first, second := send(followers[0], was, 114), send(followers[0], was, 114)
	if first.Status != transport.RelayApplied || first != second {
		t.Fatalf("first answer %+v, repeat %+v; want the same APPLIED answer", first, second)
	}
	if got := c.AwaitConverged(5 * time.Second); got.StateSeq != before.StateSeq+1 {
		t.Fatalf("a proposal sent twice moved state_seq %d -> %d, want one step", before.StateSeq, got.StateSeq)
	}
	other := send(followers[1], 114, 115)
	if other.Status != transport.RelayApplied || other.StateSeq != first.StateSeq+1 {
		t.Fatalf("the same request id from another member answered %+v, want a new application at state_seq %d", other, first.StateSeq+1)
	}
}

// TestARelayRefusesACommandKindOnlyTheLeaderOwns: a member may relay a small set of kinds;
// REMOVE_MEMBER, SET_VOTERS and the rest are refused INVALID_ARGUMENT and change nothing.
func TestARelayRefusesACommandKindOnlyTheLeaderOwns(t *testing.T) {
	// A held reporter: its background SEEDED and RETIRE move state_seq, and this test needs it exact.
	c := proofSetup(t, harness.Options{HoldSeedReports: true, NoDetection: true})
	leader, followers := relayLeaderAndFollowers(t, c)
	before := c.AwaitConverged(5 * time.Second)
	body := bsoncore.BuildDocument(nil, bsoncore.AppendStringElement(nil, "id", followers[1]))
	answer := relayCall(t, c, followers[0], leader, transport.ChannelControl, relayMessage(t, 5, cluster.CommandRemoveMember, body))
	if answer.Status != transport.RelayRefused || cluster.Code(answer.Code) != cluster.CodeInvalidArgument {
		t.Fatalf("REMOVE_MEMBER relayed by %s answered %+v, want a REFUSED INVALID_ARGUMENT", followers[0], answer)
	}
	if got := c.AwaitConverged(5 * time.Second); got.StateSeq != before.StateSeq || len(got.Members) != len(before.Members) {
		t.Fatalf("a refused relay changed the cluster: %d -> %d", before.StateSeq, got.StateSeq)
	}
}

// TestAProposalFromAFollowerIsAppliedOnThatFollowerBeforeItReturns: the caller reads its own
// write on the node it asked.
func TestAProposalFromAFollowerIsAppliedOnThatFollowerBeforeItReturns(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	_, followers := relayLeaderAndFollowers(t, c)
	follower := c.Node(followers[0])
	if err := follower.SetSetting(proofCtx(t), "dead_ms", 777777); err != nil {
		t.Fatalf("set from a follower: %v", err)
	}
	if got := follower.State().Settings.DeadMs; got != 777777 {
		t.Fatalf("the follower returned before applying its own proposal: dead_ms %d", got)
	}
}

// TestAProposalWithNoQuorumGivesUpWithinItsBudget: the leader is reachable on the control
// channel (so the follower's proposal is relayed) but has lost its Raft quorum, so nothing
// can commit. The proposal ends at its 2 s budget, not at the 5 s call timeout, and is
// UNAVAILABLE.
func TestAProposalWithNoQuorumGivesUpWithinItsBudget(t *testing.T) {
	// A held reporter: its background SEEDED and RETIRE move state_seq, and this test needs it exact.
	c := harness.NewCluster(t, 3, harness.Options{HoldSeedReports: true, NoDetection: true, ProposalBudget: 2 * time.Second})
	leader, followers := relayLeaderAndFollowers(t, c)
	c.Kill(followers[1])
	c.DropChannel(leader, followers[0], transport.ChannelRaft, 1_000_000)
	c.DropChannel(followers[0], leader, transport.ChannelRaft, 1_000_000)
	before := c.Node(followers[0]).State()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	began := time.Now()
	err := c.Node(followers[0]).SetSetting(ctx, "dead_ms", 888888)
	took := time.Since(began)
	if err == nil {
		t.Fatal("a proposal committed without a quorum")
	}
	if took > 3500*time.Millisecond {
		t.Fatalf("the proposal ran %v past its 2 s budget", took)
	}
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeUnavailable {
		t.Fatalf("the proposal gave up with %v, want an UNAVAILABLE refusal that says whether anything changed", err)
	}
	if after := c.Node(followers[0]).State(); after.StateSeq != before.StateSeq {
		t.Fatalf("state_seq moved %d -> %d without a quorum", before.StateSeq, after.StateSeq)
	}
}

// TestRepeatsOfOneRelayedProposalSentTogetherApplyOnce: copies that arrive while the first is
// still committing wait for it instead of committing again.
func TestRepeatsOfOneRelayedProposalSentTogetherApplyOnce(t *testing.T) {
	// A held reporter: its background SEEDED and RETIRE move state_seq, and this test needs it exact.
	c := proofSetup(t, harness.Options{HoldSeedReports: true, NoDetection: true})
	leader, followers := relayLeaderAndFollowers(t, c)
	before := c.AwaitConverged(5 * time.Second)
	const copies = 8
	answers := make([]transport.RelayReply, copies)
	var sent sync.WaitGroup
	for i := range copies {
		sent.Add(1)
		go func() {
			defer sent.Done()
			answers[i] = relayCall(t, c, followers[0], leader, transport.ChannelControl, relayMessage(t, 6, cluster.CommandSetSetting, relaySettingCAS(before.Settings.DeadMs, 115)))
		}()
	}
	sent.Wait()
	for i, answer := range answers {
		if answer.Status != transport.RelayApplied || answer != answers[0] {
			t.Fatalf("copy %d answered %+v, copy 0 answered %+v; want one APPLIED answer for all", i, answer, answers[0])
		}
	}
	if got := c.AwaitConverged(5 * time.Second); got.StateSeq != before.StateSeq+1 {
		t.Fatalf("%d copies of one proposal moved state_seq %d -> %d, want one step", copies, before.StateSeq, got.StateSeq)
	}
}

// relayLostReply starts a SET_SETTING from a follower whose answers from the leader are lost,
// waits until the leader has committed it, and returns the pieces a test then perturbs.
func relayLostReply(t *testing.T, c *harness.Cluster) (leader, follower string, before cluster.ClusterState, done chan error) {
	t.Helper()
	leader, followers := relayLeaderAndFollowers(t, c)
	follower = followers[0]
	before = c.AwaitConverged(5 * time.Second)
	c.DropReplies(follower, leader, transport.ChannelControl, true)
	done = make(chan error, 1)
	go func() { done <- c.Node(follower).SetSetting(proofCtx(t), "dead_ms", 424242) }()
	c.Await("the leader to commit the proposal whose answer is lost", 10*time.Second, func() bool {
		return c.Node(leader).State().Settings.DeadMs == 424242
	})
	return leader, follower, before, done
}

// TestALostReplyIsRecognisedWhenTheSameLeaderAnswersAgain: the leader committed, the answer was
// lost, the proposer retried into the leader's ledger and got the one answer; nothing applied twice.
func TestALostReplyIsRecognisedWhenTheSameLeaderAnswersAgain(t *testing.T) {
	// A held reporter: its background SEEDED and RETIRE move state_seq, and this test needs it exact.
	c := harness.NewCluster(t, 3, harness.Options{HoldSeedReports: true, NoDetection: true})
	leader, follower, before, done := relayLostReply(t, c)
	c.DropReplies(follower, leader, transport.ChannelControl, false)
	if err := <-done; err != nil {
		t.Fatalf("the proposal whose answer was lost: %v", err)
	}
	if replays := c.Node(leader).RelayReplays(); replays == 0 {
		t.Fatalf("the leader's ledger answered no repeat, so the retry was settled some other way")
	}
	if after := c.AwaitConverged(5 * time.Second); after.StateSeq != before.StateSeq+1 || after.Settings.DeadMs != 424242 {
		t.Fatalf("state_seq %d -> %d, dead_ms %d: want one application", before.StateSeq, after.StateSeq, after.Settings.DeadMs)
	}
}

// TestALostReplyIsRecognisedAfterTheLeaderChanges: the leader committed and died before the
// proposer heard; the new leader has no ledger, and the proposer finds the effect in the
// committed state instead of proposing again.
func TestALostReplyIsRecognisedAfterTheLeaderChanges(t *testing.T) {
	// A held reporter: its background SEEDED and RETIRE move state_seq, and this test needs it exact.
	c := harness.NewCluster(t, 3, harness.Options{HoldSeedReports: true, NoDetection: true})
	leader, _, before, done := relayLostReply(t, c)
	c.Kill(leader)
	if err := <-done; err != nil {
		t.Fatalf("the proposal whose leader died after committing: %v", err)
	}
	if after := c.AwaitConverged(15 * time.Second); after.StateSeq != before.StateSeq+1 || after.Settings.DeadMs != 424242 {
		t.Fatalf("state_seq %d -> %d, dead_ms %d: want one application", before.StateSeq, after.StateSeq, after.Settings.DeadMs)
	}
}
