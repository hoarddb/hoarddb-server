package harness_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/transport"
)

func relayDoc(elems ...[]byte) []byte {
	var all []byte
	for _, e := range elems {
		all = append(all, e...)
	}
	return bsoncore.BuildDocument(nil, all)
}

func relayCreateBody(name string, id int64, rf int32) []byte {
	return relayDoc(bsoncore.AppendStringElement(nil, "name", name), bsoncore.AppendInt64Element(nil, "id", id),
		bsoncore.AppendInt32Element(nil, "rf", rf), bsoncore.AppendInt64Element(nil, "schema_version", 1))
}

// relaySettingCAS is a SET_SETTING body for dead_ms that applies only while dead_ms is expect.
func relaySettingCAS(expect, value int64) []byte {
	return relayDoc(bsoncore.AppendStringElement(nil, "key", "dead_ms"), bsoncore.AppendInt64Element(nil, "value", value),
		bsoncore.AppendInt64Element(nil, "expect_value", expect))
}

func relayRefused(t *testing.T, what string, answer transport.RelayReply, want cluster.Code) {
	t.Helper()
	if answer.Status != transport.RelayRefused || cluster.Code(answer.Code) != want {
		t.Fatalf("%s answered %+v, want REFUSED %s", what, answer, want)
	}
}

// TestARelayedDatabaseCreationFollowsTheNamePolicy: a member cannot mint a database the local
// door would refuse (_system, a path, an over-long name), and the leader's own path refuses a
// replication factor no cluster of this size can meet.
func TestARelayedDatabaseCreationFollowsTheNamePolicy(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	leader, followers := relayLeaderAndFollowers(t, c)
	before := c.AwaitConverged(5 * time.Second)
	next := before.NextDatabaseID
	for name, want := range map[string]cluster.Code{
		"_system": cluster.CodeReservedName, "../x": cluster.CodeInvalidArgument, strings.Repeat("d", 300): cluster.CodeInvalidArgument,
	} {
		// Seed reports from the setup's database can still be landing; the leader refuses a
		// stale-built command before it looks at the name, so ask again until the state is still.
		var answer transport.RelayReply
		for attempt := 0; attempt < 50; attempt++ {
			answer = relayCall(t, c, followers[0], leader, transport.ChannelControl, relayMessage(t, 21, cluster.CommandCreateDatabase, relayCreateBody(name, next, 1)))
			if cluster.Code(answer.Code) != cluster.CodeStateMoved {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		relayRefused(t, fmt.Sprintf("a relayed CREATE_DATABASE %.12q", name), answer, want)
	}
	_, err := c.Node(leader).CreateDatabaseRF(proofCtx(t), "toomany", 99)
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeRFUnsupported {
		t.Fatalf("a database with 99 copies on three nodes: %v, want RF_UNSUPPORTED", err)
	}
	if after := c.AwaitConverged(5 * time.Second); len(after.Databases) != len(before.Databases) {
		t.Fatalf("a refused creation changed the databases: %d -> %d", len(before.Databases), len(after.Databases))
	}
}

// TestARelayedMemberStateIsTheSendersOwnActivationOnly: a member may relay itself joining or
// active; marking another member, or itself, down or dead is the detector's evidence path.
func TestARelayedMemberStateIsTheSendersOwnActivationOnly(t *testing.T) {
	// A held reporter: its background SEEDED and RETIRE move state_seq, and this test needs it exact.
	c := proofSetup(t, harness.Options{HoldSeedReports: true, NoDetection: true})
	leader, followers := relayLeaderAndFollowers(t, c)
	before := c.AwaitConverged(5 * time.Second)
	state := func(id, to, expect string) []byte {
		return relayDoc(bsoncore.AppendStringElement(nil, "id", id), bsoncore.AppendStringElement(nil, "state", to), bsoncore.AppendStringElement(nil, "expect_state", expect))
	}
	for name, body := range map[string][]byte{
		"another member down": state(followers[1], "down", "active"), "another member joining": state(followers[1], "joining", "active"),
		"itself down": state(followers[0], "down", "active"), "itself dead": state(followers[0], "dead", "active"),
	} {
		answer := relayCall(t, c, followers[0], leader, transport.ChannelControl, relayMessage(t, 22, cluster.CommandSetMemberState, body))
		relayRefused(t, "SET_MEMBER_STATE "+name, answer, cluster.CodePermissionDenied)
	}
	if after := c.AwaitConverged(5 * time.Second); after.StateSeq != before.StateSeq {
		t.Fatalf("a refused relay moved state_seq %d -> %d", before.StateSeq, after.StateSeq)
	}
}

// TestARelayedSchemaBumpNeedsANoteOrTheNextVersion: a bump to a far version, with a note or
// without, is the cluster-wide fence anyone could otherwise pull; only the next version is relayed.
func TestARelayedSchemaBumpNeedsANoteOrTheNextVersion(t *testing.T) {
	// A held reporter: its background SEEDED and RETIRE move state_seq, and this test needs it exact.
	c := proofSetup(t, harness.Options{HoldSeedReports: true, NoDetection: true})
	leader, followers := relayLeaderAndFollowers(t, c)
	before := c.AwaitConverged(5 * time.Second)
	db := before.Databases[0]
	body := cluster.BumpSchemaBody(db.ID, db.SchemaVersion, db.SchemaVersion+1000)
	answer := relayCall(t, c, followers[0], leader, transport.ChannelControl, relayMessage(t, 23, cluster.CommandBumpSchema, body))
	relayRefused(t, "a note-free bump to +1000", answer, cluster.CodePermissionDenied)
	noted := cluster.BumpSchemaNoteBody(db.ID, db.SchemaVersion, db.SchemaVersion+1000, cluster.BumpNote{BucketID: 1, Drop: true})
	answer = relayCall(t, c, followers[0], leader, transport.ChannelControl, relayMessage(t, 24, cluster.CommandBumpSchema, noted))
	relayRefused(t, "a noted bump to +1000", answer, cluster.CodePermissionDenied)
	if after := c.AwaitConverged(5 * time.Second); after.StateSeq != before.StateSeq {
		t.Fatalf("a refused bump moved state_seq")
	}
}

// TestAProposalOverTheRelayLimitIsRefusedAtOnce: a body the codec cannot carry is a definite
// INVALID_ARGUMENT, not an undecided attempt retried until the budget ends.
func TestAProposalOverTheRelayLimitIsRefusedAtOnce(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	_, followers := relayLeaderAndFollowers(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	began := time.Now()
	err := c.Node(followers[0]).SetSetting(ctx, strings.Repeat("k", transport.MaxRelayBody+1), 5)
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument || strings.Contains(err.Error(), "may have been committed") {
		t.Fatalf("a 1 MiB setting key: %v, want a definite INVALID_ARGUMENT", err)
	}
	if time.Since(began) > 3*time.Second {
		t.Fatalf("the refusal took %v", time.Since(began))
	}
}

// TestALateDuplicateAfterTheLedgerForgotItIsRefused: past 256 applied relays the leader no
// longer remembers a proposal, so the command itself must refuse the duplicate: the setting
// moves only from the value the sender saw.
func TestALateDuplicateAfterTheLedgerForgotItIsRefused(t *testing.T) {
	// A held reporter: its background SEEDED and RETIRE move state_seq, and this test needs it exact.
	c := proofSetup(t, harness.Options{HoldSeedReports: true, NoDetection: true})
	leader, followers := relayLeaderAndFollowers(t, c)
	current := c.AwaitConverged(5 * time.Second).Settings.DeadMs
	send := func(from string, id byte, expect, value int64) transport.RelayReply {
		return relayCall(t, c, from, leader, transport.ChannelControl, relayMessage(t, id, cluster.CommandSetSetting, relaySettingCAS(expect, value)))
	}
	if first := send(followers[0], 30, current, 600001); first.Status != transport.RelayApplied {
		t.Fatalf("the first proposal answered %+v", first)
	}
	value := int64(600001)
	for i := range 260 {
		next := int64(700000 + i)
		if filler := send(followers[i/130], byte(31+i%130), value, next); filler.Status != transport.RelayApplied {
			t.Fatalf("filler %d answered %+v", i, filler)
		}
		value = next
	}
	seq := c.AwaitConverged(5 * time.Second).StateSeq
	relayRefused(t, "the duplicate of a forgotten proposal", send(followers[0], 30, current, 600001), cluster.CodeConflict)
	if after := c.AwaitConverged(5 * time.Second); after.StateSeq != seq || after.Settings.DeadMs != value {
		t.Fatalf("the duplicate changed the setting: dead_ms %d, want %d", after.Settings.DeadMs, value)
	}
}

// TestAMemberThatIsNotAVoterCannotRelaySettingsOrDatabases: a joined, unpromoted member may
// activate itself through the relay, and nothing else.
func TestAMemberThatIsNotAVoterCannotRelaySettingsOrDatabases(t *testing.T) {
	c := harness.NewCluster(t, 4, harness.Options{NoDetection: true, Learners: 1})
	leader := c.AwaitLeader(10 * time.Second)
	learner := "n4"
	before := c.AwaitConverged(10 * time.Second)
	if slices.Contains(before.Voters, learner) {
		t.Fatalf("%s is a voter: %v", learner, before.Voters)
	}
	call := func(id byte, kind byte, body []byte) transport.RelayReply {
		return relayCall(t, c, learner, leader, transport.ChannelControl, relayMessage(t, id, kind, body))
	}
	relayRefused(t, "a setting from a non-voter", call(40, cluster.CommandSetSetting, relaySettingCAS(before.Settings.DeadMs, 555)), cluster.CodePermissionDenied)
	relayRefused(t, "a database from a non-voter", call(41, cluster.CommandCreateDatabase, relayCreateBody("fromlearner", before.NextDatabaseID, 1)), cluster.CodePermissionDenied)
	activate := relayDoc(bsoncore.AppendStringElement(nil, "id", learner), bsoncore.AppendStringElement(nil, "state", "active"), bsoncore.AppendStringElement(nil, "expect_state", "joining"))
	if answer := call(42, cluster.CommandSetMemberState, activate); answer.Status != transport.RelayApplied {
		t.Fatalf("a non-voter's own activation answered %+v, want APPLIED", answer)
	}
}

// TestAPromotionSurvivesRelayedCommandsCommittingAroundIt: a relayed command that commits
// between the promotion's read of state_seq and its append must not leave a real Raft voter
// out of ClusterState.Voters, which is what the relay's voter check reads.
func TestAPromotionSurvivesRelayedCommandsCommittingAroundIt(t *testing.T) {
	c := harness.NewCluster(t, 4, harness.Options{NoDetection: true, Learners: 1})
	leader, followers := relayLeaderAndFollowers(t, c)
	stop := make(chan struct{})
	var hammer sync.WaitGroup
	for _, id := range append(followers[:2:2], leader) {
		hammer.Add(1)
		go func() {
			defer hammer.Done()
			for value := int64(100000); ; value++ {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_ = c.Node(id).SetSetting(ctx, "dead_ms", value+int64(len(id))*1000000) // a lost CAS race is expected
				cancel()
			}
		}()
	}
	err := c.Node(leader).Promote(proofCtx(t), "n4")
	close(stop)
	hammer.Wait()
	if err != nil {
		t.Fatalf("promote under load: %v", err)
	}
	if voters := c.AwaitConverged(10 * time.Second).Voters; !slices.Contains(voters, "n4") {
		t.Fatalf("n4 was promoted in the Raft group but is absent from ClusterState.Voters %v", voters)
	}
}

// TestALeaderProposalWithNoQuorumGivesUpWithinItsBudget: the leader's own path is bounded by the
// budget too.
func TestALeaderProposalWithNoQuorumGivesUpWithinItsBudget(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true, ProposalBudget: 2 * time.Second})
	leader, followers := relayLeaderAndFollowers(t, c)
	for _, id := range followers {
		c.Kill(id)
	}
	began := time.Now()
	err := c.Node(leader).SetSetting(proofCtx(t), "dead_ms", 999999)
	if err == nil {
		t.Fatal("a proposal committed without a quorum")
	}
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeUnavailable {
		t.Fatalf("gave up with %v, want UNAVAILABLE", err)
	}
	if took := time.Since(began); took > 3500*time.Millisecond {
		t.Fatalf("the leader's proposal ran %v past its 2 s budget", took)
	}
}
