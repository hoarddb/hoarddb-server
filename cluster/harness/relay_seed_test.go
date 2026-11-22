package harness_test

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/transport"
)

// seedBits is a 1,024-byte seed bitmap with the named slots set, the body of SEEDED and
// RETIRE.
func seedBits(slots ...int) []byte {
	bits := make([]byte, 1024)
	for _, slot := range slots {
		bits[slot/8] |= 1 << (slot % 8)
	}
	return bits
}

// seedRow is the row of state the SEEDED and RETIRE commands fill in for (node, db).
func seedRow(state cluster.ClusterState, node string, dbID int64) []byte {
	for _, row := range state.SeedBitmaps {
		if row.Node == node && row.DBID == dbID {
			return row.Bits
		}
	}
	return make([]byte, 1024)
}

// seedRowHas reports whether the row holds every named slot.
func seedRowHas(state cluster.ClusterState, node string, dbID int64, slots ...int) bool {
	bits := seedRow(state, node, dbID)
	for _, slot := range slots {
		if bits[slot/8]>>(slot%8)&1 == 0 {
			return false
		}
	}
	return true
}

// TestARelayedSeededForAnotherNodeIsDenied: the seed commands carry a node name, and only
// that node may speak for it. A follower relaying bits named for another member is refused
// PERMISSION_DENIED and moves nothing.
// [BREAK: delete the node != sender check in relayAuthority.]
func TestARelayedSeededForAnotherNodeIsDenied(t *testing.T) {
	// A held reporter: its background SEEDED and RETIRE move state_seq, and this test needs it exact.
	c := proofSetup(t, harness.Options{HoldSeedReports: true, NoDetection: true})
	leader, followers := relayLeaderAndFollowers(t, c)
	before := c.AwaitConverged(5 * time.Second)
	db := before.Databases[0]
	body := cluster.SeedBitmapBody(followers[1], db.ID, seedBits(7))
	answer := relayCall(t, c, followers[0], leader, transport.ChannelControl, relayMessage(t, 60, cluster.CommandSeeded, body))
	relayRefused(t, "a SEEDED naming another member", answer, cluster.CodePermissionDenied)
	if after := c.AwaitConverged(5 * time.Second); after.StateSeq != before.StateSeq {
		t.Fatalf("a refused SEEDED moved state_seq %d -> %d", before.StateSeq, after.StateSeq)
	}
	if seedRowHas(c.Node(leader).State(), followers[1], db.ID, 7) {
		t.Fatalf("%s's row holds a bit %s named in a refused relay", followers[1], followers[0])
	}
}

// TestAJoinerMayRelayItsBits: a member that is still joining is a committed member, and its
// bits are the point of the report, so it may relay them for itself.
// [BREAK: add an active-only check for the sender in relayAuthority.]
func TestAJoinerMayRelayItsBits(t *testing.T) {
	// A held reporter: its background SEEDED and RETIRE move state_seq, and this test needs it exact.
	c := harness.NewCluster(t, 4, harness.Options{HoldSeedReports: true, NoDetection: true, Learners: 1})
	leader, _ := relayLeaderAndFollowers(t, c)
	joiner := "n4"
	before := c.AwaitConverged(10 * time.Second)
	member, ok := memberOf(before, joiner)
	if !ok || member.State != cluster.MemberJoining {
		t.Fatalf("%s is %+v, want a member that is still joining", joiner, member)
	}
	id, err := c.Node(leader).CreateDatabaseRF(proofCtx(t), proofDB, 1)
	if err != nil {
		t.Fatalf("creating the database the report names: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	body := cluster.SeedBitmapBody(joiner, int64(id), seedBits(9))
	answer := relayCall(t, c, joiner, leader, transport.ChannelControl, relayMessage(t, 61, cluster.CommandSeeded, body))
	if answer.Status != transport.RelayApplied {
		t.Fatalf("a joining member's own SEEDED answered %+v, want APPLIED", answer)
	}
	c.Await("the joiner's committed row to hold its bit", 10*time.Second, func() bool {
		return seedRowHas(c.Node(joiner).State(), joiner, int64(id), 9)
	})
}

// TestAWitnessMayNotRelayBits: a witness holds no documents, so it has no seed bits to
// report; its own SEEDED is PERMISSION_DENIED and moves nothing.
// [BREAK: delete the witness check in relayAuthority.]
func TestAWitnessMayNotRelayBits(t *testing.T) {
	// A held reporter: its background SEEDED and RETIRE move state_seq, and this test needs it exact.
	c := harness.NewCluster(t, 3, harness.Options{HoldSeedReports: true, NoDetection: true, Witnesses: []string{"n3"}})
	leader, _ := relayLeaderAndFollowers(t, c)
	witness := "n3"
	before := c.AwaitConverged(10 * time.Second)
	member, ok := memberOf(before, witness)
	if !ok || !member.Witness {
		t.Fatalf("%s is %+v, want a committed witness to relay from", witness, member)
	}
	id, err := c.Node(leader).CreateDatabaseRF(proofCtx(t), proofDB, 1)
	if err != nil {
		t.Fatalf("creating the database the report names: %v", err)
	}
	converged := c.AwaitConverged(10 * time.Second)
	body := cluster.SeedBitmapBody(witness, int64(id), seedBits(11))
	answer := relayCall(t, c, witness, leader, transport.ChannelControl, relayMessage(t, 62, cluster.CommandSeeded, body))
	relayRefused(t, "a witness's own SEEDED", answer, cluster.CodePermissionDenied)
	if after := c.AwaitConverged(5 * time.Second); after.StateSeq != converged.StateSeq {
		t.Fatalf("a refused SEEDED moved state_seq %d -> %d", converged.StateSeq, after.StateSeq)
	}
}

// TestARelayedSeededIsAnsweredFromTheLedgerOnRetry: the same (sender, request id) twice is
// one proposal. The second is answered from the leader's ledger, so state_seq moves once.
// [BREAK: put a time.Now byte in the ledger key.]
func TestARelayedSeededIsAnsweredFromTheLedgerOnRetry(t *testing.T) {
	// A held reporter: its background SEEDED and RETIRE move state_seq, and this test needs it exact.
	c := proofSetup(t, harness.Options{HoldSeedReports: true, NoDetection: true})
	leader, followers := relayLeaderAndFollowers(t, c)
	before := c.AwaitConverged(5 * time.Second)
	db := before.Databases[0]
	msg := relayMessage(t, 63, cluster.CommandSeeded, cluster.SeedBitmapBody(followers[0], db.ID, seedBits(13)))
	first := relayCall(t, c, followers[0], leader, transport.ChannelControl, msg)
	if first.Status != transport.RelayApplied {
		t.Fatalf("the first SEEDED answered %+v, want APPLIED", first)
	}
	replays := c.Node(leader).RelayReplays()
	second := relayCall(t, c, followers[0], leader, transport.ChannelControl, msg)
	if second != first {
		t.Fatalf("the repeat answered %+v, want the stored answer %+v", second, first)
	}
	if got := c.Node(leader).RelayReplays(); got != replays+1 {
		t.Fatalf("the ledger answered %d repeats, want %d", got, replays+1)
	}
	if after := c.AwaitConverged(5 * time.Second); after.StateSeq != before.StateSeq+1 {
		t.Fatalf("the same proposal sent twice moved state_seq %d -> %d, want one step", before.StateSeq, after.StateSeq)
	}
}

// TestANonLeaderIsAnsweredNotLeaderOnce: a node that is not the Raft leader answers
// NOT_LEADER naming the leader and forwards nothing, so two nodes that disagree about the
// leader cannot loop a seed report.
// [BREAK: forward the proposal in serveRelay instead of answering NOT_LEADER.]
func TestANonLeaderIsAnsweredNotLeaderOnce(t *testing.T) {
	// A held reporter: its background SEEDED and RETIRE move state_seq, and this test needs it exact.
	c := proofSetup(t, harness.Options{HoldSeedReports: true, NoDetection: true})
	leader, followers := relayLeaderAndFollowers(t, c)
	before := c.AwaitConverged(5 * time.Second)
	db := before.Databases[0]
	body := cluster.SeedBitmapBody(followers[0], db.ID, seedBits(15))
	answer := relayCall(t, c, followers[0], followers[1], transport.ChannelControl, relayMessage(t, 64, cluster.CommandSeeded, body))
	if answer.Status != transport.RelayNotLeader || answer.Leader != leader {
		t.Fatalf("a follower answered a SEEDED with %+v, want NOT_LEADER naming %s", answer, leader)
	}
	if after := c.AwaitConverged(5 * time.Second); after.StateSeq != before.StateSeq {
		t.Fatalf("the follower applied or forwarded the SEEDED: state_seq %d -> %d", before.StateSeq, after.StateSeq)
	}
	if seedRowHas(c.Node(leader).State(), followers[0], db.ID, 15) {
		t.Fatal("a SEEDED answered NOT_LEADER reached the log")
	}
}
