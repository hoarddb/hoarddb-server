package harness_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The BUG-0870 replace-node tests (design section 7.2): a member whose ring directory was lost is
// started again on an empty one with its key, id and address, and 'replace-node <node> --with
// <node>@<addr>' gives it its identity back; it re-seeds every pair it owns from the other copies.

const replaceBucket = "Box"

// replaceSeedStall and replaceSeedCeiling bound the wait for a replaced node to be active. The node
// pulls every pair it owns (about 5,400 at three members and RF 2); on a 7 GB test VM that took 1 min
// 48 s to over 4 min per run on the vouched cut, slowing as the run went on, so the wait fails on a
// seeding that stopped for the stall bound, not on a fixed time.
const (
	replaceSeedStall   = 90 * time.Second
	replaceSeedCeiling = 10 * time.Minute
)

// replaceCtx bounds one replace-node statement: the leader's own budget is a minute.
func replaceCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// replaceSeed creates database at rf with one bucket and inserts count documents, each through its
// owner, and returns their ids.
func replaceSeed(t *testing.T, c *harness.Cluster, database string, rf, count int) []string {
	t.Helper()
	bug0834Retry(t, "creating the database", func(ctx context.Context) error {
		_, err := c.Node(c.AwaitLeader(30*time.Second)).CreateDatabaseRF(ctx, database, rf)
		return err
	})
	c.AwaitConverged(30 * time.Second)
	bug0834Retry(t, "creating the bucket", func(ctx context.Context) error {
		_, err := c.Node(c.AwaitLeader(30*time.Second)).CreateBucket(ctx, database, replaceBucket, wire.Field{Name: "name", Type: "string"})
		return err
	})
	c.AwaitLeases(30 * time.Second)
	ids := make([]string, 0, count)
	for i := range count {
		id := fmt.Sprintf("doc-%d", i)
		replaceInsert(t, c, database, id)
		ids = append(ids, id)
	}
	return ids
}

// replaceInsert inserts id through its owner, retrying while the cluster settles.
func replaceInsert(t *testing.T, c *harness.Cluster, database, id string) {
	t.Helper()
	bug0834Retry(t, "inserting "+id, func(ctx context.Context) error {
		owner, _, err := c.Node(c.Running()[0]).Owner(ctx, database, replaceBucket, id)
		if err != nil {
			return err
		}
		_, err = c.Node(owner).Insert(ctx, database, replaceBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada " + id}})
		return err
	})
}

// awaitRowIn waits until the leader's committed row of id is in one of states.
func awaitRowIn(t *testing.T, c *harness.Cluster, id string, timeout time.Duration, states ...cluster.MemberState) cluster.Member {
	t.Helper()
	var row cluster.Member
	c.Await(fmt.Sprintf("%s to be %v", id, states), timeout, func() bool {
		var ok bool
		row, ok = memberOf(c.Node(c.AwaitLeader(10*time.Second)).State(), id)
		return ok && slices.Contains(states, row.State)
	})
	return row
}

// wipeAndAwaitDown removes victim's ring directory, starts it again on the empty directory and
// waits until the cluster has committed it down or dead.
func wipeAndAwaitDown(t *testing.T, c *harness.Cluster, victim string) cluster.Member {
	t.Helper()
	c.WipeRing(victim)
	c.RestartUnsynced(victim)
	return awaitRowIn(t, c, victim, 30*time.Second, cluster.MemberDown, cluster.MemberDead)
}

// otherRunning is a running node that is neither of the ones named.
func otherRunning(t *testing.T, c *harness.Cluster, not ...string) string {
	t.Helper()
	for _, id := range c.Running() {
		if !slices.Contains(not, id) {
			return id
		}
	}
	t.Fatalf("no running node besides %v", not)
	return ""
}

// requireRefusal checks that err is a *cluster.Error with code and every one of words.
func requireRefusal(t *testing.T, err error, code cluster.Code, words ...string) {
	t.Helper()
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != code {
		t.Fatalf("got %v, want a %v refusal", err, code)
	}
	for _, word := range words {
		if !strings.Contains(refusal.Message, word) {
			t.Errorf("the refusal lacks %q: %s", word, refusal.Message)
		}
	}
}

// requireRowUnchanged checks that the leader's row of id still has the state and incarnation of before.
func requireRowUnchanged(t *testing.T, c *harness.Cluster, before cluster.Member) {
	t.Helper()
	row, ok := memberOf(c.Node(c.AwaitLeader(10*time.Second)).State(), before.ID)
	if !ok || row.State != before.State || row.Incarnation != before.Incarnation {
		t.Fatalf("a refused replace changed the row of %s: %+v, was %+v", before.ID, row, before)
	}
}

// requireCopiesHeld waits until victim itself has applied its own row active, then checks that it
// holds, byte for byte, every document of ids whose copy set names it, and returns how many that is.
func requireCopiesHeld(t *testing.T, c *harness.Cluster, database, victim string, ids []string) int {
	t.Helper()
	awaitOwnRow(t, c, victim)
	// No wait for the declaration or the documents: a node that is active holds them (BUG-0877).
	held := 0
	for _, id := range ids {
		_, copies, err := c.Node(victim).Owner(proofCtx(t), database, replaceBucket, id)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(copies, victim) {
			continue
		}
		other := copies[0]
		if other == victim {
			other = copies[len(copies)-1]
		}
		want, found, err := c.Node(other).LocalDocument(proofCtx(t), database, replaceBucket, id)
		if err != nil || !found {
			t.Fatalf("the copy on %s lost %s: found %v err %v", other, id, found, err)
		}
		got, found, err := c.Node(victim).LocalDocument(proofCtx(t), database, replaceBucket, id)
		if err != nil || !found {
			t.Fatalf("the replaced %s does not hold %s, which it owns a copy of: found %v err %v; %s", victim, id, found, err, describePair(c, database, victim, id))
		}
		if fmt.Sprint(got.Fields) != fmt.Sprint(want.Fields) {
			t.Fatalf("the replaced %s holds %v for %s, %s holds %v", victim, got.Fields, id, other, want.Fields)
		}
		held++
	}
	return held
}

// TestAWipedNodeOfAClusterHoldingDataCanBeReplacedAndCatchesUp: [GUARD BUG-0870 replace (design
// section 5.2): three voters hold an RF 2 database with documents; one member's ring directory is
// removed (its key, id and address kept) and it is started again, held out of Raft by the guard, and
// committed down. 'replace-node' entered on another follower (so the statement and its accept_loss
// flag are forwarded) is accepted, the node joins under its own id, re-seeds, is activated and votes
// again, holds every document it owns a copy of, and the cluster serves a new write]. Red while the
// statement answers "is not available in this build".
func TestAWipedNodeOfAClusterHoldingDataCanBeReplacedAndCatchesUp(t *testing.T) {
	const database = "rp"
	c := harness.NewCluster(t, 3, harness.Options{FastDetection: true, JoinerActivation: true, WCTimeout: 5 * time.Second})
	ids := replaceSeed(t, c, database, 2, 24)
	victim := followerOf(t, c)
	wipeAndAwaitDown(t, c, victim)

	entry := otherRunning(t, c, victim, c.AwaitLeader(10*time.Second))
	got, err := replaceOnceReported(t, c, entry, victim)
	if err != nil {
		t.Fatalf("replace-node %s at %s: %v", victim, c.Addr(victim), err)
	}
	if want := "Replacing " + victim + "@" + c.Addr(victim); !strings.HasPrefix(got.Result, want) || !strings.Contains(got.Result, "'status seeding'") {
		t.Fatalf("the outcome line is %q, want it to start %q and name 'status seeding'", got.Result, want)
	}
	c.AwaitActiveWhileSeedingMoves(victim, replaceSeedStall, replaceSeedCeiling)
	c.Await(victim+" to vote again", 30*time.Second, func() bool {
		return slices.Contains(c.Node(c.AwaitLeader(10*time.Second)).State().Voters, victim)
	})
	held := requireCopiesHeld(t, c, database, victim, ids)
	t.Logf("%s holds its copy of %d of the %d documents", victim, held, len(ids))
	if held == 0 {
		t.Fatalf("none of the %d documents has a copy on %s; the test proves nothing", len(ids), victim)
	}
	replaceInsert(t, c, database, "after-replace")
	requireCopiesHeld(t, c, database, victim, []string{"after-replace"})
}

// TestReplaceRefusesWhenTheTargetIsTheOnlyHolderOfAPair: [GUARD BUG-0870 replace R4 (owner ruling
// 2026-11-19, design section 9 Q1): at replication factor 1 the lost node held the only copy of its
// pairs; replace-node without --accept-loss is refused INVALID_ARGUMENT naming the count, the
// database and the flag, and changes nothing (the row and the node's seed bits stay); with
// accept_loss the same statement is accepted and the node becomes active and serves]. Red while
// replace-node is refused by name, or (BREAK: drop the only-holder check) when the statement
// without the flag goes through.
func TestReplaceRefusesWhenTheTargetIsTheOnlyHolderOfAPair(t *testing.T) {
	const database = "solo"
	c := harness.NewCluster(t, 3, harness.Options{FastDetection: true, JoinerActivation: true, WCTimeout: 5 * time.Second})
	replaceSeed(t, c, database, 1, 12)
	victim := followerOf(t, c)
	dbID := c.Node(victim).State().Databases[0].ID
	c.Await(victim+" to report the pairs it holds", 30*time.Second, func() bool {
		for _, row := range c.Node(c.AwaitLeader(10 * time.Second)).State().SeedBitmaps {
			if row.Node == victim && row.DBID == dbID {
				return true
			}
		}
		return false
	})
	before := wipeAndAwaitDown(t, c, victim)

	_, err := c.Membership(c.AwaitLeader(10*time.Second)).ReplaceNode(replaceCtx(t), victim, c.Addr(victim), false)
	requireRefusal(t, err, cluster.CodeInvalidArgument, "replace-node "+victim, "pairs", `database "`+database+`"`, "replication factor 1", "Nothing was changed", "--accept-loss")
	requireRowUnchanged(t, c, before)
	if !slices.ContainsFunc(c.Node(c.AwaitLeader(10*time.Second)).State().SeedBitmaps, func(row cluster.SeedBitmap) bool { return row.Node == victim }) {
		t.Fatalf("a refused replace cleared the seed bits of %s", victim)
	}

	if _, err := c.Membership(c.AwaitLeader(10*time.Second)).ReplaceNode(replaceCtx(t), victim, c.Addr(victim), true); err != nil {
		t.Fatalf("replace-node %s --accept-loss: %v", victim, err)
	}
	c.AwaitActiveWhileSeedingMoves(victim, replaceSeedStall, replaceSeedCeiling)
	replaceInsert(t, c, database, "after-replace")
}

// TestReplaceRefusesAMemberThatIsActive: [GUARD BUG-0870 replace R2: a member that is active (here:
// every member of a cluster with no database) is refused CONFLICT naming its state and the fix, and
// nothing is committed]. BREAK: drop the state check.
func TestReplaceRefusesAMemberThatIsActive(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true})
	victim := followerOf(t, c)
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	before := leader.State().StateSeq
	_, err := c.Membership(leader.Identity().ID).ReplaceNode(replaceCtx(t), victim, c.Addr(victim), false)
	requireRefusal(t, err, cluster.CodeConflict, "node "+victim+" is active", "Nothing was changed", "'replace-node "+victim+" --with "+victim+"@"+c.Addr(victim)+"'")
	// Re-review F-3 (e): --accept-loss waives R4 only.
	_, err = c.Membership(leader.Identity().ID).ReplaceNode(replaceCtx(t), victim, c.Addr(victim), true)
	requireRefusal(t, err, cluster.CodeConflict, "node "+victim+" is active")
	if after := leader.State().StateSeq; after != before {
		t.Fatalf("a refused replace committed: state_seq %d -> %d", before, after)
	}
}

// TestReplaceRefusesAnotherAddress: [GUARD BUG-0870 replace R3 (owner ruling: same address only): a
// down member named at an address other than its committed one is refused INVALID_ARGUMENT naming
// the committed address and the statement with it, and its row is unchanged]. BREAK: drop the
// address check (the identity ask at the typed address then fails another way).
func TestReplaceRefusesAnotherAddress(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{FastDetection: true})
	victim := followerOf(t, c)
	before := wipeAndAwaitDown(t, c, victim)
	_, err := c.Membership(c.AwaitLeader(10*time.Second)).ReplaceNode(replaceCtx(t), victim, "127.0.0.1:1", false)
	requireRefusal(t, err, cluster.CodeInvalidArgument, "127.0.0.1:1", "its own address, "+c.Addr(victim), "Nothing was changed", "'replace-node "+victim+" --with "+victim+"@"+c.Addr(victim)+"'")
	_, err = c.Membership(c.AwaitLeader(10*time.Second)).ReplaceNode(replaceCtx(t), victim, "127.0.0.1:1", true) // F-3 (e)
	requireRefusal(t, err, cluster.CodeInvalidArgument, "its own address, "+c.Addr(victim))
	requireRowUnchanged(t, c, before)
}

// TestReplaceRefusesANodeThatHoldsClusterState: [GUARD BUG-0870 replace R5: a member committed down
// whose process restarted on its own ring directory still holds the cluster's state, so it did not
// lose its data; replace-node is refused CONFLICT saying so, and its row (and its seed bits) are
// left alone]. The harness holds a down member's activation, so the restarted node stays down.
// BREAK: drop the holds-state check (the replace then clears the bits of a node that has its data).
func TestReplaceRefusesANodeThatHoldsClusterState(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{FastDetection: true})
	victim := followerOf(t, c)
	c.Kill(victim)
	before := awaitRowIn(t, c, victim, 30*time.Second, cluster.MemberDown, cluster.MemberDead)
	c.RestartUnsynced(victim)
	c.Await(victim+" to apply the cluster's state again", 30*time.Second, func() bool { return c.Node(victim).State().StateSeq > 0 })
	_, err := c.Membership(c.AwaitLeader(10*time.Second)).ReplaceNode(replaceCtx(t), victim, c.Addr(victim), false)
	// BUG-0870 review B-4: the conventions catalog's REPLACE_TARGET_HAS_DATA is this refusal, word for word.
	requireRefusal(t, err, cluster.CodeReplaceTargetHasData, `node "`+victim+`" cannot be replaced: the node at `+c.Addr(victim)+` holds cluster state`, "did not lose its data", "Nothing was changed")
	_, err = c.Membership(c.AwaitLeader(10*time.Second)).ReplaceNode(replaceCtx(t), victim, c.Addr(victim), true) // F-3 (e)
	requireRefusal(t, err, cluster.CodeReplaceTargetHasData, "did not lose its data")
	requireRowUnchanged(t, c, before)
}

// TestAReplaceRetriedAfterTheNodeRestartedMidSeedFinishes: [GUARD BUG-0870 replace, idempotence
// (design section 5.2 step 8): the replaced node is killed while it pulls its pairs and starts again
// on the Raft state it had taken; the repeated statement finds the row joining and a node that
// holds state, commits nothing new and answers, and the node finishes its seed and becomes active].
// BREAK: refuse a joining row like an active one (R2).
func TestAReplaceRetriedAfterTheNodeRestartedMidSeedFinishes(t *testing.T) {
	const database = "mid"
	c := harness.NewCluster(t, 3, harness.Options{FastDetection: true, JoinerActivation: true, WCTimeout: 5 * time.Second})
	ids := replaceSeed(t, c, database, 2, 24)
	victim := followerOf(t, c)
	wipeAndAwaitDown(t, c, victim)
	for _, other := range c.IDs() {
		if other != victim {
			c.DelayChannel(victim, other, transport.ChannelSeed, 300*time.Millisecond)
		}
	}
	leader := c.AwaitLeader(10 * time.Second)
	if _, err := replaceOnceReported(t, c, leader, victim); err != nil {
		t.Fatalf("replace-node %s: %v", victim, err)
	}
	c.Await(victim+" to take the cluster's state as a joiner", 30*time.Second, func() bool {
		row, ok := memberOf(c.Node(victim).State(), victim)
		return ok && row.State == cluster.MemberJoining
	})
	c.Kill(victim)
	for _, other := range c.IDs() {
		if other != victim {
			c.DelayChannel(victim, other, transport.ChannelSeed, 0)
		}
	}
	c.RestartUnsynced(victim)
	// The seed is slowed so the node is still joining here; a node that finished first would be
	// active, and the repeat would rightly be refused.
	before := awaitRowIn(t, c, victim, 10*time.Second, cluster.MemberJoining)
	if _, err := c.Membership(c.AwaitLeader(10*time.Second)).ReplaceNode(replaceCtx(t), victim, c.Addr(victim), false); err != nil {
		t.Fatalf("the repeated replace-node %s: %v", victim, err)
	}
	if row, _ := memberOf(c.Node(c.AwaitLeader(10*time.Second)).State(), victim); row.Incarnation != before.Incarnation {
		t.Fatalf("the repeated replace committed a new incarnation for a node that holds state: %d, was %d", row.Incarnation, before.Incarnation)
	}
	c.AwaitActiveWhileSeedingMoves(victim, replaceSeedStall, replaceSeedCeiling)
	requireCopiesHeld(t, c, database, victim, ids)
}

// TestAReplaceRetriedForANodeThatNeverTookRaftStateFinishes: [GUARD BUG-0870 replace, idempotence
// (design section 5.2 step 8; the state a leader change between the commit and the admission
// leaves): the first replace commits the row joining and re-enters the node while the leader's
// Raft dials to it are cut, so the node never takes any Raft state; it is restarted, held again by
// the guard, and the repeated statement re-enters and admits it, and it seeds and becomes active].
// BREAK: answer a joining row from the row without asking the node (it then stays held).
func TestAReplaceRetriedForANodeThatNeverTookRaftStateFinishes(t *testing.T) {
	const database = "cut"
	c := harness.NewCluster(t, 3, harness.Options{FastDetection: true, JoinerActivation: true, WCTimeout: 5 * time.Second})
	ids := replaceSeed(t, c, database, 2, 24)
	victim := followerOf(t, c)
	wipeAndAwaitDown(t, c, victim)
	for _, other := range c.IDs() {
		if other != victim {
			c.DropChannel(other, victim, transport.ChannelRaft, 1<<20)
		}
	}
	if _, err := replaceOnceReported(t, c, c.AwaitLeader(10*time.Second), victim); err != nil {
		t.Fatalf("replace-node %s with its Raft channel cut: %v", victim, err)
	}
	awaitRowIn(t, c, victim, 10*time.Second, cluster.MemberJoining)
	if seq := c.Node(victim).State().StateSeq; seq != 0 {
		t.Fatalf("%s took Raft state (state_seq %d) although every Raft dial to it was cut", victim, seq)
	}
	c.Kill(victim)
	for _, other := range c.IDs() {
		if other != victim {
			c.DropChannel(other, victim, transport.ChannelRaft, 0)
		}
	}
	c.RestartUnsynced(victim)
	requireNoClusterState(t, c, victim, "after a restart before its first Raft write")
	if _, err := replaceOnceReported(t, c, c.AwaitLeader(10*time.Second), victim); err != nil {
		t.Fatalf("the repeated replace-node %s: %v", victim, err)
	}
	c.AwaitActiveWhileSeedingMoves(victim, replaceSeedStall, replaceSeedCeiling)
	requireCopiesHeld(t, c, database, victim, ids)
}

// TestReplaceRefusesAnOwnerWhoseCopiesAreNotReportedYet: [GUARD BUG-0870 review B-1 and B-2: the
// seed reporters are held, so no member has committed a SEEDED bit; the lost node owned documents at
// replication factor 1. replace-node without --accept-loss is refused naming --accept-loss and
// changes nothing; with it the statement goes through and its line says how many pairs, in how many
// databases, it accepted to lose]. Red while the only-holder check walks only committed bits (the
// statement then goes through with the normal line).
func TestReplaceRefusesAnOwnerWhoseCopiesAreNotReportedYet(t *testing.T) {
	const database = "held"
	c := harness.NewCluster(t, 3, harness.Options{FastDetection: true, HoldSeedReports: true, WCTimeout: 5 * time.Second, CaptureLog: true})
	ids := replaceSeed(t, c, database, 1, 12)
	victim := followerOf(t, c)
	owns := 0
	for _, id := range ids {
		owner, _, err := c.Node(victim).Owner(proofCtx(t), database, replaceBucket, id)
		if err != nil {
			t.Fatal(err)
		}
		if owner == victim {
			owns++
		}
	}
	if owns == 0 {
		t.Fatalf("%s owns none of the %d documents; the test proves nothing", victim, len(ids))
	}
	if rows := c.Node(c.AwaitLeader(10 * time.Second)).State().SeedBitmaps; len(rows) != 0 {
		t.Fatalf("the reporters are held, yet %d SEEDED rows are committed", len(rows))
	}
	before := wipeAndAwaitDown(t, c, victim)

	_, err := c.Membership(c.AwaitLeader(10*time.Second)).ReplaceNode(replaceCtx(t), victim, c.Addr(victim), false)
	requireRefusal(t, err, cluster.CodeInvalidArgument, "replace-node "+victim, `database "`+database+`"`, "Nothing was changed", "--accept-loss")
	requireRowUnchanged(t, c, before)

	got, err := c.Membership(c.AwaitLeader(10*time.Second)).ReplaceNode(replaceCtx(t), victim, c.Addr(victim), true)
	if err != nil {
		t.Fatalf("replace-node %s --accept-loss: %v", victim, err)
	}
	for _, want := range []string{"accepted the loss of", "pairs in 1 database", "only " + victim + " held"} {
		if !strings.Contains(got.Result, want) {
			t.Errorf("the accepted-loss line lacks %q: %s", want, got.Result)
		}
	}
	// Re-review F-3 (d): the leader logs the loss too.
	requireLogged(t, c, "replace-node "+victim+" accepted the loss of")

	// Re-review F-1: the node loses its ring directory again while it is joining. The repeat clears
	// its bits, so it passes the same gate: refused without --accept-loss, named with it.
	c.WipeRing(victim)
	c.RestartUnsynced(victim)
	requireNoClusterState(t, c, victim, "after its second wipe")
	// Narrow review B-2: the refused repeat commits nothing, its bit clear included. The reporters are
	// held and the node's row is joining, so nothing else moves the state in between.
	seqBefore := c.Node(c.AwaitLeader(10 * time.Second)).State().StateSeq
	_, err = c.Membership(c.AwaitLeader(10*time.Second)).ReplaceNode(replaceCtx(t), victim, c.Addr(victim), false)
	requireRefusal(t, err, cluster.CodeInvalidArgument, "replace-node "+victim, "Nothing was changed", "--accept-loss")
	if seqAfter := c.Node(c.AwaitLeader(10 * time.Second)).State().StateSeq; seqAfter != seqBefore {
		t.Fatalf("the refused repeat committed: state_seq %d -> %d (its bit clear must come after the gate)", seqBefore, seqAfter)
	}
	if !c.Node(victim).AwaitingAdmission() {
		t.Fatalf("a refused repeat admitted %s", victim)
	}
	got, err = c.Membership(c.AwaitLeader(10*time.Second)).ReplaceNode(replaceCtx(t), victim, c.Addr(victim), true)
	if err != nil || !strings.Contains(got.Result, "accepted the loss of") {
		t.Fatalf("the repeated replace-node %s --accept-loss: %q, %v; want the accepted-loss line", victim, got.Result, err)
	}
}

// replaceOnceReported runs replace-node without --accept-loss through entry, repeating it while R4
// refuses it: right after the documents are written the members have not all reported their copies
// (the reporters batch them about every two seconds), and replace-node counts an unreported copy as
// none by design (review B-1). Any other answer is returned at once.
func replaceOnceReported(t *testing.T, c *harness.Cluster, entry, victim string) (wire.MembershipOutcome, error) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		got, err := c.Membership(entry).ReplaceNode(replaceCtx(t), victim, c.Addr(victim), false)
		var refusal *cluster.Error
		if err == nil || time.Now().After(deadline) || !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument || !strings.Contains(refusal.Message, "--accept-loss") {
			return got, err
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// requireLogged waits briefly for a WARN line containing want (Options.CaptureLog).
func requireLogged(t *testing.T, c *harness.Cluster, want string) {
	t.Helper()
	c.Await("the log line "+strconv.Quote(want), 10*time.Second, func() bool {
		return slices.ContainsFunc(c.LogLines(), func(line string) bool { return strings.Contains(line, want) })
	})
}

// TestAReplaceRetriedForAHeldJoiningNodeClearsItsBitsAgain: [GUARD BUG-0870 review B-3: a replaced
// node that reported pairs and then lost its ring directory again is held, with its row still
// joining and its bits committed; the repeated replace-node clears those bits before it admits the
// node, so no later replace counts a copy the node no longer has]. BREAK: resume without the clear.
func TestAReplaceRetriedForAHeldJoiningNodeClearsItsBitsAgain(t *testing.T) {
	const database = "twice"
	c := harness.NewCluster(t, 3, harness.Options{FastDetection: true, WCTimeout: 5 * time.Second})
	replaceSeed(t, c, database, 2, 12)
	victim := followerOf(t, c)
	wipeAndAwaitDown(t, c, victim)
	leader := c.AwaitLeader(10 * time.Second)
	if _, err := replaceOnceReported(t, c, leader, victim); err != nil {
		t.Fatalf("replace-node %s: %v", victim, err)
	}
	bitsOf := func() int {
		count := 0
		for _, row := range c.Node(c.AwaitLeader(10 * time.Second)).State().SeedBitmaps {
			if row.Node == victim {
				for _, b := range row.Bits {
					for ; b != 0; b &= b - 1 {
						count++
					}
				}
			}
		}
		return count
	}
	// The harness holds the joiner's activation, so it stays joining while it reports its pairs.
	c.Await(victim+" to report some of its pairs", 60*time.Second, func() bool { return bitsOf() > 0 })
	c.WipeRing(victim)
	c.RestartUnsynced(victim)
	// Re-review F-3: the node cannot report again (its proposals travel on the control channel), so the
	// count after the statement is the clear's alone, not a race with the next two-second batch.
	for _, other := range c.IDs() {
		if other != victim {
			c.DropChannel(victim, other, transport.ChannelControl, 1<<20)
		}
	}
	reported := bitsOf()
	if row, _ := memberOf(c.Node(c.AwaitLeader(10*time.Second)).State(), victim); row.State != cluster.MemberJoining || reported == 0 {
		t.Fatalf("the scenario needs a joining row with bits: %+v, %d bits", row, reported)
	}
	if _, err := replaceOnceReported(t, c, c.AwaitLeader(10*time.Second), victim); err != nil {
		t.Fatalf("the repeated replace-node %s: %v", victim, err)
	}
	if now := bitsOf(); now != 0 {
		t.Fatalf("after the repeated replace %s still holds %d committed bits (it held %d before its second wipe)", victim, now, reported)
	}
}

// describePair is the diagnostic a failed copy check prints (BUG-0877): the pair the document
// places in, the victim's record of it and its pull status, the committed SEEDED bits of every
// node for it, and the victim's own row and state_seq beside the leader's.
func describePair(c *harness.Cluster, database, victim, id string) string {
	slot, err := wire.PlacementOf(database, replaceBucket, id)
	if err != nil {
		return "placing " + id + ": " + err.Error()
	}
	node := c.Node(victim)
	state := node.State()
	var dbID int64 = -1
	for _, db := range state.Databases {
		if db.Name == database {
			dbID = db.ID
		}
	}
	var bitsOn []string
	for _, row := range state.SeedBitmaps {
		if row.DBID == dbID && len(row.Bits) > int(slot)/8 && row.Bits[slot/8]>>(slot%8)&1 == 1 {
			bitsOn = append(bitsOn, row.Node)
		}
	}
	own, _ := memberOf(state, victim)
	status, pulling := node.SeedStatus(slot, uint64(dbID))
	leaderSeq := c.Node(c.AwaitLeader(10 * time.Second)).State().StateSeq
	return fmt.Sprintf("slot %d of database %d: %s's record %v, pulling %v %+v, bits on %v, %s's row %v at state_seq %d (leader %d), %d pairs still seeding",
		slot, dbID, victim, node.SlotRecordState(slot, uint64(dbID)), pulling, status, bitsOn, victim, own.State, state.StateSeq, leaderSeq, node.SeedingPairs())
}

// holdReplayAfterCreate arms wire.HoldAppliesForTest for the next open of victim: once, right after
// victim's Raft has applied the creation of a database and before it applies the next command, the
// apply is held until victim's seeding driver has finished a pass or two seconds pass. A replaced
// node starts on an empty directory and applies the cluster's whole log, so the hold keeps it on a
// state from before its replacement in which it is an active copy of a database whose pairs no one
// has reported yet (BUG-0877). It returns whether the hold happened, which a test must check: a node
// that took a snapshot instead never sees that state, and the test would prove nothing.
func holdReplayAfterCreate(t *testing.T, victim string) (arm func(*wire.Node), held func() bool) {
	t.Helper()
	var node atomic.Pointer[wire.Node]
	var created, holding atomic.Bool
	wire.HoldAppliesForTest(func(id string, kind byte) {
		if id != victim || holding.Load() {
			return
		}
		if kind == cluster.CommandCreateDatabase {
			created.Store(true)
			return
		}
		if !created.Load() {
			return
		}
		holding.Store(true)
		deadline := time.Now().Add(2 * time.Second)
		var passes int64 = -1
		if held := node.Load(); held != nil {
			passes = held.SeedPasses()
		}
		for time.Now().Before(deadline) {
			if held := node.Load(); held != nil && passes >= 0 && held.SeedPasses() != passes {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	t.Cleanup(func() { wire.HoldAppliesForTest(nil) })
	return func(opened *wire.Node) { node.Store(opened); wire.HoldAppliesForTest(nil) }, holding.Load
}

// TestAReplacedNodeServesEveryDocumentTheMomentItIsActive: [GUARD BUG-0877 (document symptom): a
// replaced node starts on an empty directory and applies the cluster's log from the start; held on
// the state right after the database's creation, in which it is an active copy and no pair has a
// SEEDED bit, its seeding driver must not judge its pairs there. Once its row is active it holds,
// at once and with no wait, every document of every pair it owns a copy of]. Red while the driver
// runs a pass on that old state: it writes every pair it holds ready and empty, reports their bits,
// passes the activation gate and is active without the documents.
func TestAReplacedNodeServesEveryDocumentTheMomentItIsActive(t *testing.T) {
	const database = "rd"
	c := harness.NewCluster(t, 3, harness.Options{FastDetection: true, JoinerActivation: true, WCTimeout: 5 * time.Second})
	ids := replaceSeed(t, c, database, 2, 24)
	victim := followerOf(t, c)
	arm, held := holdReplayAfterCreate(t, victim)
	c.WipeRing(victim)
	c.RestartUnsynced(victim)
	arm(c.Node(victim))
	awaitRowIn(t, c, victim, 30*time.Second, cluster.MemberDown, cluster.MemberDead)
	if _, err := replaceOnceReported(t, c, otherRunning(t, c, victim), victim); err != nil {
		t.Fatalf("replace-node %s: %v", victim, err)
	}
	c.AwaitActiveWhileSeedingMoves(victim, replaceSeedStall, replaceSeedCeiling)
	if !held() {
		t.Fatalf("%s never applied the database's creation while it caught up, so it was never held on the old state; the test proves nothing", victim)
	}
	if count := requireCopiesHeld(t, c, database, victim, ids); count == 0 {
		t.Fatalf("none of the %d documents has a copy on %s; the test proves nothing", len(ids), victim)
	}
}

// TestAReplacedNodeResolvesEveryBucketTheMomentItIsActive: [GUARD BUG-0877 (declaration symptom):
// the replaced node is the owner of the declaration slot of some buckets (their _buckets document
// lives in a pair it owns). Held on the old state as above, then replaced and active: at once and
// with no wait it resolves every bucket (Owner answers, no "no such bucket") and lists, from the
// pairs it owns, every document of each bucket that it owns]. Red while the driver judges the old
// state: the _buckets pair is ready and empty, so the node's own declaration read answers not found.
func TestAReplacedNodeResolvesEveryBucketTheMomentItIsActive(t *testing.T) {
	t.Skip("BUG-0878: a replaced node goes active while primary handoffs of its pairs are open and refuses reads for about a minute (this test fails with that refusal about one run in four alone on a loaded host, on the pre-merge base too); skipped during the feature freeze (owner 2026-11-21), it is also the guard of BUG-0877, so that guard is off until BUG-0878 is fixed in the replication rework")
	const database = "rb"
	c := harness.NewCluster(t, 3, harness.Options{FastDetection: true, JoinerActivation: true, WCTimeout: 5 * time.Second})
	bug0834Retry(t, "creating the database", func(ctx context.Context) error {
		_, err := c.Node(c.AwaitLeader(30*time.Second)).CreateDatabaseRF(ctx, database, 2)
		return err
	})
	c.AwaitConverged(30 * time.Second)
	victim := followerOf(t, c)
	buckets := bucketsDeclaredOn(t, c, database, victim)
	for _, bucket := range buckets {
		bug0834Retry(t, "creating bucket "+bucket, func(ctx context.Context) error {
			_, err := c.Node(c.AwaitLeader(30*time.Second)).CreateBucket(ctx, database, bucket, wire.Field{Name: "name", Type: "string"})
			return err
		})
	}
	c.AwaitLeases(30 * time.Second)
	owned := map[string]int{}
	for _, bucket := range buckets {
		for i := range 8 {
			id := fmt.Sprintf("%s-%d", bucket, i)
			var owner string
			bug0834Retry(t, "inserting "+id, func(ctx context.Context) error {
				var err error
				if owner, _, err = c.Node(c.Running()[0]).Owner(ctx, database, bucket, id); err != nil {
					return err
				}
				_, err = c.Node(owner).Insert(ctx, database, bucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada " + id}})
				return err
			})
			if owner == victim {
				owned[bucket]++
			}
		}
	}
	arm, held := holdReplayAfterCreate(t, victim)
	c.WipeRing(victim)
	c.RestartUnsynced(victim)
	arm(c.Node(victim))
	awaitRowIn(t, c, victim, 30*time.Second, cluster.MemberDown, cluster.MemberDead)
	if _, err := replaceOnceReported(t, c, otherRunning(t, c, victim), victim); err != nil {
		t.Fatalf("replace-node %s: %v", victim, err)
	}
	c.AwaitActiveWhileSeedingMoves(victim, replaceSeedStall, replaceSeedCeiling)
	if !held() {
		t.Fatalf("%s never applied the database's creation while it caught up, so it was never held on the old state; the test proves nothing", victim)
	}
	awaitOwnRow(t, c, victim)
	listedOwned := 0
	for _, bucket := range buckets {
		if _, _, err := c.Node(victim).Owner(proofCtx(t), database, bucket, bucket+"-0"); err != nil {
			t.Fatalf("the replaced %s, active, cannot resolve bucket %s: %v", victim, bucket, err)
		}
		// A slot whose ownership just came back to the node is held until its primary handoff
		// closes, and the list says so with a retryable UNAVAILABLE that returns nothing; a lease
		// that lapses on a loaded host says LEASE_LAPSED. Neither is the defect. Not found, or a
		// short list, is.
		var listed []wire.Listed
		var err error
		began := time.Now()
		for deadline := began.Add(2 * time.Minute); ; time.Sleep(100 * time.Millisecond) {
			listed, err = c.Node(victim).List(proofCtx(t), database, bucket, nil, 0)
			var refusal *cluster.Error
			retryable := harness.IsTransientRefusal(err) || (errors.As(err, &refusal) && refusal.Code == cluster.CodeUnavailable)
			if !retryable || time.Now().After(deadline) {
				break
			}
		}
		if err != nil {
			t.Fatalf("the replaced %s, active, cannot list bucket %s: %v", victim, bucket, err)
		}
		if waited := time.Since(began); waited > time.Second {
			t.Logf("the list of bucket %s on %s was refused as retryable for %s before it answered", bucket, victim, waited.Round(time.Millisecond))
		}
		if len(listed) != owned[bucket] {
			t.Fatalf("the replaced %s lists %d documents of bucket %s from the pairs it owns, want %d", victim, len(listed), bucket, owned[bucket])
		}
		listedOwned += len(listed)
	}
	t.Logf("%s resolved %d buckets and listed the %d documents it owns", victim, len(buckets), listedOwned)
}

// bucketsDeclaredOn picks three bucket names for database: two whose declaration (their _buckets
// document) lives in a slot node owns, and one whose declaration slot another node owns. The owner
// is the leader's, at a state in which node is active, which is the state after its replacement.
func bucketsDeclaredOn(t *testing.T, c *harness.Cluster, database, node string) []string {
	t.Helper()
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	dbID := uint64(leader.State().Databases[0].ID)
	var mine, other []string
	for i := 0; len(mine) < 2 || len(other) < 1; i++ {
		if i > 1000 {
			t.Fatalf("no bucket name among 1000 has its declaration slot owned by %s", node)
		}
		name := fmt.Sprintf("Box%d", i)
		slot, err := wire.PlacementOf(database, "_buckets", name)
		if err != nil {
			t.Fatal(err)
		}
		owner, ok := leader.OwnerOfSlot(slot, dbID)
		switch {
		case !ok:
			t.Fatalf("slot %d of database %s has no owner", slot, database)
		case owner == node && len(mine) < 2:
			mine = append(mine, name)
		case owner != node && len(other) < 1:
			other = append(other, name)
		}
	}
	return append(mine, other...)
}
