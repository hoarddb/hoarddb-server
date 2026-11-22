package harness_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// The five-voter RF 3 failure table of the membership-and-failure spec (majority of voters
// 3, majority of copies 2), one test per row B1-B8, each judged by the ledger at profile P0.
// Gate: HOARDDB_HARNESS_FAULT=1. Every write of one id comes from one goroutine at a time and
// every client name from one goroutine (the ledger's record-order preconditions).

const fiveBucket = "fiveledger"

// fiveGate skips the row unless the fault tables are asked for.
func fiveGate(t *testing.T) {
	t.Helper()
	if os.Getenv("HOARDDB_HARNESS_FAULT") == "" {
		t.Skip("a fault-table row; set HOARDDB_HARNESS_FAULT=1")
	}
}

// fiveSetup forms five voters at profile with an RF 3 database and the ledger's bucket.
func fiveSetup(t *testing.T, profile harness.Profile) *harness.Cluster {
	t.Helper()
	fiveGate(t)
	c := harness.NewCluster(t, 5, harness.Options{Timings: profile})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, 3); err != nil {
		t.Fatalf("creating the RF=3 database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), proofDB, fiveBucket, wire.Field{Name: "name", Type: "string"}, wire.Field{Name: "seq", Type: "int"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	return c
}

// sameSet says copies holds exactly the nodes in want, in any order.
func sameSet(copies []string, want ...string) bool {
	if len(copies) != len(want) {
		return false
	}
	for _, node := range want {
		if !slices.Contains(copies, node) {
			return false
		}
	}
	return true
}

// fiveIDs returns count ids of the ledger bucket, named prefix-N, whose placement the
// predicate accepts. The placement is read before any fault, so it is the ranking the
// row's first writes see.
func fiveIDs(t *testing.T, c *harness.Cluster, count int, prefix string, accept func(owner string, copies []string) bool) []string {
	t.Helper()
	asker := c.Node(c.Running()[0])
	var ids []string
	for i := 0; len(ids) < count; i++ {
		if i > 40000 {
			t.Fatalf("found %d of %d ids for %s", len(ids), count, prefix)
		}
		id := fmt.Sprintf("%s-%d", prefix, i)
		owner, copies, err := asker.Owner(proofCtx(t), proofDB, fiveBucket, id)
		if err != nil {
			t.Fatalf("placing %s: %v", id, err)
		}
		if accept(owner, copies) {
			ids = append(ids, id)
		}
	}
	return ids
}

// fiveHolds waits until every copy of id holds it, so losing any two of the three loses nothing
// that was acknowledged (SPEC-GAP U57-15).
func fiveHolds(t *testing.T, c *harness.Cluster, id string) {
	t.Helper()
	_, copies, err := c.Node(c.Running()[0]).Owner(proofCtx(t), proofDB, fiveBucket, id)
	if err != nil {
		t.Fatalf("placing %s: %v", id, err)
	}
	for _, copyNode := range copies {
		c.Await("copy "+copyNode+" to hold "+id, 20*time.Second, func() bool {
			_, found, err := c.Node(copyNode).LocalDocument(proofCtx(t), proofDB, fiveBucket, id)
			return err == nil && found
		})
	}
}

// fiveWritePre inserts each id through via before any fault and demands applied with a
// majority of copies, then waits for every copy to hold it.
func fiveWritePre(t *testing.T, l *harness.Ledger, c *harness.Cluster, via, client string, ids []string) {
	t.Helper()
	for _, id := range ids {
		got, err := l.Write(proofCtx(t), c, via, client, id)
		if err != nil || got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed < 2 {
			t.Fatalf("before the fault %s answered %v/%d err %v, want applied with a majority of 2", id, got.Outcome, got.CopiesConfirmed, err)
		}
		fiveHolds(t, c, id)
	}
}

// fiveAwaitMember waits until every observer sees node in state, within the profile's
// detection time plus the failover budget.
func fiveAwaitMember(t *testing.T, c *harness.Cluster, observers []string, node string, state cluster.MemberState, within time.Duration) {
	t.Helper()
	c.Await(fmt.Sprintf("%s to be %v on %v", node, state, observers), within, func() bool {
		for _, observer := range observers {
			seen := false
			for _, member := range c.Node(observer).State().Members {
				if member.ID == node && member.State == state {
					seen = true
				}
			}
			if !seen {
				return false
			}
		}
		return true
	})
}

// fiveWriteUntil upserts id through via until it is acknowledged or the budget runs out;
// an upsert is safe to repeat after an answer that never came.
func fiveWriteUntil(t *testing.T, l *harness.Ledger, c *harness.Cluster, via, client, id string, options ...wire.WriteOption) wire.Inserted {
	t.Helper()
	deadline := time.Now().Add(quorumFailoverBudget)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		got, err := l.Upsert(ctx, c, via, client, id, options...)
		cancel()
		if err == nil {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("a write of %s through %s was not acknowledged within %s: %v", id, via, quorumFailoverBudget, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// fiveReadUntil reads id through via until it is served and returns how long that took; a read
// held past the budget fails the row.
func fiveReadUntil(t *testing.T, l *harness.Ledger, c *harness.Cluster, via, client, id string, budget time.Duration) time.Duration {
	t.Helper()
	started := time.Now()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		_, err := l.Read(ctx, c, via, client, id)
		cancel()
		if err == nil {
			return time.Since(started)
		}
		if time.Since(started) >= budget {
			t.Fatalf("a read of %s through %s was not served within %s: %v", id, via, budget, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// fiveJudge runs the ledger's six invariants and fails the row for each that is violated,
// printing all of them.
func fiveJudge(t *testing.T, l *harness.Ledger, c *harness.Cluster, lost map[string]string) {
	t.Helper()
	for _, verdict := range l.Check(context.Background(), c, harness.CheckOptions{Lost: lost, Settle: 20 * time.Second}) {
		if !verdict.OK {
			t.Errorf("%s was violated: %s (after %+v)", verdict.Name, verdict.Detail, verdict.After)
		}
	}
}

// others lists every node of the cluster but the ones named.
func others(c *harness.Cluster, gone ...string) []string {
	var rest []string
	for _, id := range c.IDs() {
		if !slices.Contains(gone, id) {
			rest = append(rest, id)
		}
	}
	return rest
}

// refusalCode is the cluster code of err, or zero when it carries none.
func refusalCode(err error) cluster.Code {
	var coded *cluster.Error
	if errors.As(err, &coded) {
		return coded.Code
	}
	return 0
}

// TestFiveVotersOneUnreachable (B1): n2 is killed; a majority commits it down within the
// detection time, its slots move to the next copy, and a write that lost one of three copies
// still has the other two: confirmed 2 of configured 3 is a majority, so `applied`.
func TestFiveVotersOneUnreachable(t *testing.T) {
	c := fiveSetup(t, harness.P0)
	const gone = "n2"
	ownedByGone := func(owner string, _ []string) bool { return owner == gone }
	pre := fiveIDs(t, c, 3, "b1pre", ownedByGone)
	post := fiveIDs(t, c, 3, "b1post", ownedByGone)
	l := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: fiveBucket, ProbeID: pre[0]})
	fiveWritePre(t, l, c, "n1", "ada", pre)
	l.Note(c, "before the kill")
	survivors := others(c, gone)
	l.Fault("kill " + gone)
	c.Kill(gone)
	for _, observer := range survivors {
		fiveAwaitMember(t, c, []string{observer}, gone, cluster.MemberDown, harness.P0.Detection()+quorumFailoverBudget)
	}
	c.AwaitLeader(10 * time.Second)
	c.AwaitLeases(20 * time.Second)
	l.Note(c, "after the down commit")
	for _, id := range pre {
		if owner, _, err := c.Node("n1").Owner(proofCtx(t), proofDB, fiveBucket, id); err != nil || owner == gone {
			t.Fatalf("after the down commit the owner of %s is %q (err %v), want a survivor", id, owner, err)
		}
		fiveReadUntil(t, l, c, "n3", "bea", id, quorumFailoverBudget)
	}
	for _, id := range post {
		got := fiveWriteUntil(t, l, c, "n4", "ada", id)
		if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed < 2 {
			t.Errorf("%s, owned by the killed %s, answered %v/%d, want applied with at least 2 copies", id, gone, got.Outcome, got.CopiesConfirmed)
		}
	}
	l.Note(c, "after the new writes")
	fiveJudge(t, l, c, nil)
}

// fiveTwoGoneSetup is the cluster, ledger and ids of rows B2 and B3: n2 and n3 are the two
// that will become unreachable, so ids of class A have the copy set {n2,n3,n4} and ids of
// class B the set {n1,n4,n5}.
type fiveTwoGone struct {
	c            *harness.Cluster
	l            *harness.Ledger
	preA, freshA []string
	preB, freshB []string
}

func fiveTwoGoneSetup(t *testing.T) fiveTwoGone {
	t.Helper()
	c := fiveSetup(t, harness.P0)
	classA := func(_ string, copies []string) bool { return sameSet(copies, "n2", "n3", "n4") }
	classB := func(_ string, copies []string) bool { return sameSet(copies, "n1", "n4", "n5") }
	row := fiveTwoGone{c: c}
	row.preA = fiveIDs(t, c, 2, "b2preA", classA)
	row.freshA = fiveIDs(t, c, 3, "b2newA", classA)
	row.preB = fiveIDs(t, c, 2, "b2preB", classB)
	row.freshB = fiveIDs(t, c, 3, "b2newB", classB)
	// The probe pair is a class B pair: n2 and n3 never hold it, so the quiescence check
	// compares only nodes that were never cut off.
	row.l = harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: fiveBucket, ProbeID: row.preB[0]})
	fiveWritePre(t, row.l, c, "n1", "ada", row.preA)
	fiveWritePre(t, row.l, c, "n1", "ada", row.preB)
	return row
}

// cutTwo partitions {n2,n3} from the other three and waits until all three of them have
// committed both down.
func (r fiveTwoGone) cutTwo(t *testing.T) {
	t.Helper()
	r.l.Note(r.c, "before the partition")
	r.l.Fault("partition n2,n3|n1,n4,n5")
	r.c.Partition([]string{"n2", "n3"}, []string{"n1", "n4", "n5"})
	for _, gone := range []string{"n2", "n3"} {
		fiveAwaitMember(t, r.c, []string{"n1", "n4", "n5"}, gone, cluster.MemberDown, harness.P0.Detection()+quorumFailoverBudget)
	}
	r.c.AwaitLeader(10 * time.Second)
	r.l.Note(r.c, "after both down commits")
}

// heal ends the partition and waits until every node applies the same state. The cut-off
// members stay `down` in the committed state: nothing in this build commits `active` for a
// member that was partitioned and never restarted (see the report of slice H5), so the row
// does not wait for it.
func (r fiveTwoGone) heal(t *testing.T) {
	t.Helper()
	r.l.Fault("heal")
	r.c.Heal()
	r.c.AwaitConverged(30 * time.Second)
	r.c.AwaitLeader(10 * time.Second)
	for _, member := range r.c.Node("n1").State().Members {
		t.Logf("after the heal %s is %v", member.ID, member.State)
	}
	r.l.Note(r.c, "after the heal")
}

// TestFiveVotersTwoUnreachableTwoAnswerClasses (B2): two of five are cut off and the other
// three still hold a majority. Ids whose copies are {n2,n3,n4} have one survivor and answer
// `applied_degraded` with 1 copy; ids on {n1,n4,n5} answer `applied` with a majority (the
// ack returns at the second copy) and all three copies hold the id soon after.
func TestFiveVotersTwoUnreachableTwoAnswerClasses(t *testing.T) {
	row := fiveTwoGoneSetup(t)
	row.cutTwo(t)
	for _, id := range row.freshA {
		got := fiveWriteUntil(t, row.l, row.c, "n1", "ada", id)
		if got.Outcome != machine.OutcomeAppliedDegraded || got.CopiesConfirmed != 1 {
			t.Errorf("%s, on {n2,n3,n4}, answered %v/%d, want applied_degraded with 1 copy", id, got.Outcome, got.CopiesConfirmed)
		}
	}
	for _, id := range row.freshB {
		got := fiveWriteUntil(t, row.l, row.c, "n1", "ada", id)
		if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed < 2 {
			t.Errorf("%s, on {n1,n4,n5}, answered %v/%d, want applied with at least 2 copies", id, got.Outcome, got.CopiesConfirmed)
		}
		// The ack returns at the second copy; the spec says the write confirms 3 of 3, so the
		// third copy must arrive after it (bounded by fiveHolds).
		fiveHolds(t, row.c, id)
	}
	row.l.Note(row.c, "after the new writes")
	row.heal(t)
	fiveJudge(t, row.l, row.c, nil)
}

// TestFiveVotersTwoUnreachableReadsOpenOnTheDegradedTarget (B3): the same two cut off; a
// pair whose copies {n2,n3,n4} leave one survivor opens on the highest position that
// survivor reports instead of holding for answers that cannot come (BUG-0742, conventions
// section 10 rule 2), so its acknowledged ids are served within the failover budget.
func TestFiveVotersTwoUnreachableReadsOpenOnTheDegradedTarget(t *testing.T) {
	row := fiveTwoGoneSetup(t)
	began := time.Now()
	row.cutTwo(t)
	for _, id := range row.preA {
		fiveReadUntil(t, row.l, row.c, "n1", "bea", id, quorumFailoverBudget-time.Since(began))
	}
	if took := time.Since(began); took > quorumFailoverBudget {
		t.Errorf("the degraded pairs were served %s after the partition, want within %s", took, quorumFailoverBudget)
	}
	for _, id := range row.preB {
		fiveReadUntil(t, row.l, row.c, "n5", "bea", id, quorumFailoverBudget)
	}
	row.l.Note(row.c, "after the reads")
	row.heal(t)
	fiveJudge(t, row.l, row.c, nil)
}

// TestFiveVotersThreeUnreachableLapseEverything (B4): three of five stop answering. Two of
// five is not a majority, so nothing is committed, stage 1 never runs and the survivors keep
// their ranks; every request to them is LEASE_LAPSED once the stop bound has passed. The first
// request is sent at the stop bound: before it a request for an id owned by a stalled node is
// forwarded and unanswered (the spec's client timeout), which the ledger would call an
// unnamed refusal from a running node.
func TestFiveVotersThreeUnreachableLapseEverything(t *testing.T) {
	c := fiveSetup(t, harness.P0)
	stalled := []string{"n3", "n4", "n5"}
	survivors := []string{"n1", "n2"}
	pre := fiveIDs(t, c, 4, "b4pre", func(string, []string) bool { return true })
	l := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: fiveBucket, ProbeID: pre[0]})
	fiveWritePre(t, l, c, "n1", "ada", pre)
	l.Note(c, "before the stall")
	epoch := c.Node("n1").State().Epoch
	ranks := map[string]string{}
	for _, id := range pre {
		owner, _, err := c.Node("n1").Owner(proofCtx(t), proofDB, fiveBucket, id)
		if err != nil {
			t.Fatalf("placing %s: %v", id, err)
		}
		ranks[id] = owner
	}
	l.Fault("stall n3,n4,n5")
	faultAt := time.Now()
	for _, node := range stalled {
		c.Stall(node, true)
	}
	time.Sleep(time.Until(faultAt.Add(harness.P0.StopBound())))
	answers := 0
	for turn := 0; time.Since(faultAt) < 5*time.Second; turn++ {
		via := survivors[turn%len(survivors)]
		id := pre[turn%len(pre)]
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		_, readErr := l.Read(ctx, c, via, "bea", id)
		_, writeErr := l.Upsert(ctx, c, via, "ada", fmt.Sprintf("b4late-%d", turn))
		cancel()
		for what, err := range map[string]error{"read": readErr, "write": writeErr} {
			if code := refusalCode(err); code != cluster.CodeLeaseLapsed {
				t.Errorf("%s of %s through %s at +%s answered %v, want LEASE_LAPSED", what, id, via, time.Since(faultAt).Round(time.Millisecond), err)
			}
			answers++
		}
		time.Sleep(200 * time.Millisecond)
	}
	if answers == 0 {
		t.Fatal("no request was answered in the window")
	}
	for _, node := range survivors {
		state := c.Node(node).State()
		if state.Epoch != epoch {
			t.Errorf("%s saw epoch %d, want it unchanged at %d: two of five voters must commit nothing", node, state.Epoch, epoch)
		}
		for _, member := range state.Members {
			if member.State != cluster.MemberActive {
				t.Errorf("%s sees %s as %v, want active: nothing may be committed down", node, member.ID, member.State)
			}
		}
		for _, id := range pre {
			if owner, _, err := c.Node(node).Owner(proofCtx(t), proofDB, fiveBucket, id); err != nil || owner != ranks[id] {
				t.Errorf("%s ranks %s under %q (err %v), want the unchanged owner %q", node, id, owner, err, ranks[id])
			}
		}
	}
	l.Fault("unstall n3,n4,n5")
	for _, node := range stalled {
		c.Stall(node, false)
	}
	c.AwaitLeader(10 * time.Second)
	c.AwaitLeases(20 * time.Second)
	l.Note(c, "after the stall")
	fiveJudge(t, l, c, nil)
}

// TestFiveVotersPartitionTwoThree (B5): {n1,n2} are cut off from {n3,n4,n5}. The three
// commit the two down; the minority's `majority`-concern write is answered `unknown` (lease
// lapsed at wc_timeout) or `applied_degraded` with 1 copy (lease still valid; the harness
// wc_timeout of 500 ms is shorter than the lease lifetime), both in the loss set; a `one`-concern write inside the lease window is `applied` and discarded, then
// everything the minority is asked is LEASE_LAPSED. A client stream on the majority side
// keeps being acknowledged. After the heal the minority's acknowledgements are exactly the
// declared loss set and none of them is readable.
func TestFiveVotersPartitionTwoThree(t *testing.T) {
	c := fiveSetup(t, harness.P0)
	minority, majority := []string{"n1", "n2"}, []string{"n3", "n4", "n5"}
	inMajority := func(_ string, copies []string) bool { return sameSet(copies, majority...) }
	// The minority ids are owned by n1 with both other copies in the majority, so a
	// majority-concern write cannot gather a second copy.
	ownedByN1 := func(owner string, copies []string) bool { return owner == "n1" && !slices.Contains(copies, "n2") }
	streamIDs := fiveIDs(t, c, 40, "b5s", inMajority)
	minIDs := fiveIDs(t, c, 3, "b5min", ownedByN1)
	l := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: fiveBucket, ProbeID: streamIDs[0]})
	fiveWritePre(t, l, c, "n3", "ada", streamIDs[:2])
	fiveWritePre(t, l, c, "n1", "cat", minIDs[2:])
	l.Note(c, "before the partition")

	stop := make(chan struct{})
	streamed := make(chan []string)
	go func() {
		var acked []string
		defer func() { streamed <- acked }()
		for _, id := range streamIDs[2 : len(streamIDs)-3] {
			select {
			case <-stop:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			got, err := l.Write(ctx, c, majority[len(acked)%len(majority)], "ada", id)
			cancel()
			if err == nil {
				acked = append(acked, id)
				if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed < 2 {
					t.Errorf("the majority side answered %s with %v/%d, want applied with a majority of 2", id, got.Outcome, got.CopiesConfirmed)
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	l.Fault("partition n1,n2|n3,n4,n5")
	cutAt := time.Now()
	c.Partition(minority, majority)
	oneCtx, cancelOne := context.WithTimeout(context.Background(), 5*time.Second)
	one, oneErr := l.Write(oneCtx, c, "n1", "bea", minIDs[0], wire.WithConcern(machine.ConcernOne))
	cancelOne()
	if oneErr != nil || one.Outcome != machine.OutcomeApplied {
		t.Errorf("a concern-one write at the cut answered %v/%d err %v, want applied", one.Outcome, one.CopiesConfirmed, oneErr)
	}
	majCtx, cancelMaj := context.WithTimeout(context.Background(), 5*time.Second)
	maj, majErr := l.Write(majCtx, c, "n1", "bea", minIDs[1])
	cancelMaj()
	// With the harness wc_timeout (500 ms) shorter than the lease lifetime, the owner either
	// finds its lease lapsed at wc_timeout (`unknown`) or still valid (`applied_degraded`
	// with its own copy, spec section 9 row B: "not a promise of survival"). Both are the
	// spec; the second is durable on the owner only and the majority discards it.
	majDegraded := false
	t.Logf("the majority-concern write at the cut answered %v/%d after %s", maj.Outcome, maj.CopiesConfirmed, time.Since(cutAt))
	switch took := time.Since(cutAt); {
	case majErr != nil:
		t.Errorf("a majority write at the cut was refused: %v", majErr)
	case took > harness.P0.StopBound():
		t.Errorf("a majority write at the cut answered %v/%d after %s, want an answer by %s", maj.Outcome, maj.CopiesConfirmed, took, harness.P0.StopBound())
	case maj.Outcome == machine.OutcomeUnknown:
	case maj.Outcome == machine.OutcomeAppliedDegraded && maj.CopiesConfirmed == 1:
		majDegraded = true
	default:
		t.Errorf("a majority write at the cut answered %v/%d, want unknown or applied_degraded with 1 copy", maj.Outcome, maj.CopiesConfirmed)
	}
	// The late requests wait for the stop bound: before it an id owned by the majority is a
	// forward that nobody answers, the spec's client timeout, not a refusal to judge.
	time.Sleep(time.Until(cutAt.Add(harness.P0.StopBound() + 100*time.Millisecond)))
	for _, via := range minority {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		_, readErr := l.Read(ctx, c, via, "bea", streamIDs[0])
		_, writeErr := l.Upsert(ctx, c, via, "bea", "b5late-"+via)
		cancel()
		for what, err := range map[string]error{"read": readErr, "write": writeErr} {
			if code := refusalCode(err); code != cluster.CodeLeaseLapsed {
				t.Errorf("the minority node %s answered a %s %s after the cut with %v, want LEASE_LAPSED", via, what, time.Since(cutAt).Round(time.Millisecond), err)
			}
		}
	}
	// The stream stops before the down commits: a write that crosses an epoch bump may be
	// answered STALE_EPOCH, the fence's client face, which the ledger's list of named
	// refusals does not include (reported with this slice). The majority side's writes after
	// the commits are made one by one below.
	close(stop)
	acked := <-streamed
	if len(acked) == 0 {
		t.Error("the majority side acknowledged nothing during the partition")
	}
	for _, gone := range minority {
		fiveAwaitMember(t, c, majority, gone, cluster.MemberDown, harness.P0.Detection()+quorumFailoverBudget)
	}
	c.AwaitLeader(10 * time.Second)
	l.Note(c, "after both down commits")
	for _, id := range streamIDs[len(streamIDs)-3:] {
		got := fiveWriteUntil(t, l, c, "n4", "ada", id)
		if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed < 2 {
			t.Errorf("after the down commits the majority side answered %s with %v/%d, want applied with a majority of 2", id, got.Outcome, got.CopiesConfirmed)
		}
	}

	l.Fault("heal")
	c.Heal()
	c.AwaitConverged(30 * time.Second)
	l.Note(c, "after the heal")
	lost := map[string]string{minIDs[0]: "minority one-concern write, discarded by spec"}
	if majDegraded {
		lost[minIDs[1]] = "minority majority-concern write answered applied_degraded/1 (lease still valid at wc_timeout), may be lost by spec section 9 row B"
	} else {
		lost[minIDs[1]] = "minority majority-concern write answered unknown, discarded by spec"
	}
	// The minority's acknowledgements, taken from the ledger, are exactly the declared set.
	got := map[string]bool{}
	for _, ack := range l.Acks() {
		if slices.Contains(minority, ack.Via) && ack.At.After(cutAt) {
			got[ack.ID] = true
		}
	}
	for id := range got {
		if _, declared := lost[id]; !declared {
			t.Errorf("the minority acknowledged %s after the cut and the loss set does not name it", id)
		}
	}
	for id := range lost {
		if !got[id] {
			t.Errorf("the loss set names %s but the minority never acknowledged it", id)
		}
		if _, err := c.Node("n3").Get(proofCtx(t), proofDB, fiveBucket, id); refusalCode(err) != cluster.CodeNotFound {
			t.Errorf("the excused id %s is readable through n3 after the heal (err %v), want NOT_FOUND: a discarded write must be gone", id, err)
		}
	}
	fiveJudge(t, l, c, lost)
}

// TestFiveVotersLeaderDies (B6): the Raft leader dies with a write in flight at it. A dead
// process sends no answer, so the write is an error and never an Inserted, not even `unknown`
// (record section 3 as amended); a new leader is elected within 2 s; leases are re-granted by
// the stop bound; the dead leader's slots move once it is committed down. The in-flight write
// is held up in the owner's own append (a slow disk) so that it is still in flight when the
// kill lands 50 ms later.
func TestFiveVotersLeaderDies(t *testing.T) {
	c := fiveSetup(t, harness.P0)
	leader := c.AwaitLeader(10 * time.Second)
	survivors := others(c, leader)
	ownedByLeader := func(owner string, _ []string) bool { return owner == leader }
	pre := fiveIDs(t, c, 3, "b6pre", ownedByLeader)
	flying := fiveIDs(t, c, 1, "b6fly", ownedByLeader)[0]
	post := fiveIDs(t, c, 3, "b6post", ownedByLeader)
	l := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: fiveBucket, ProbeID: pre[0]})
	fiveWritePre(t, l, c, survivors[0], "ada", pre)
	l.Note(c, "before the kill")

	type answer struct {
		got wire.Inserted
		err error
	}
	answered := make(chan answer, 1)
	c.SlowDisk(leader, 500*time.Millisecond)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		got, err := l.Write(ctx, c, leader, "bea", flying)
		answered <- answer{got, err}
	}()
	time.Sleep(50 * time.Millisecond)
	l.Fault("kill " + leader)
	c.Kill(leader)
	killedAt := time.Now()
	res := <-answered
	if res.err == nil {
		t.Errorf("the write in flight at the dying leader answered %v/%d, want an error and no Inserted", res.got.Outcome, res.got.CopiesConfirmed)
	}
	if newLeader := c.AwaitLeader(2 * time.Second); newLeader == leader {
		t.Errorf("the dead node %s is still the leader", leader)
	}
	electedIn := time.Since(killedAt)
	c.AwaitLeases(harness.P0.StopBound())
	t.Logf("a new leader after %s, leases re-granted after %s", electedIn, time.Since(killedAt))
	l.Note(c, "after the leases")
	fiveAwaitMember(t, c, survivors, leader, cluster.MemberDown, harness.P0.Detection()+quorumFailoverBudget)
	l.Note(c, "after the down commit")
	for _, id := range pre {
		fiveReadUntil(t, l, c, survivors[1], "cat", id, quorumFailoverBudget)
	}
	for _, id := range post {
		got := fiveWriteUntil(t, l, c, survivors[2], "ada", id)
		if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed < 2 {
			t.Errorf("%s, owned by the dead leader, answered %v/%d, want applied with at least 2 copies", id, got.Outcome, got.CopiesConfirmed)
		}
	}
	fiveJudge(t, l, c, nil)
}

// fiveDeadSetup commits dead_ms 3000 and writes three ids, one of whose copies is n2.
func fiveDeadSetup(t *testing.T) (*harness.Cluster, *harness.Ledger, []string) {
	t.Helper()
	c := fiveSetup(t, harness.P0)
	if err := c.Node(c.AwaitLeader(10*time.Second)).SetSetting(proofCtx(t), "dead_ms", 3000); err != nil {
		t.Fatalf("setting dead_ms: %v", err)
	}
	ids := fiveIDs(t, c, 6, "b7", func(_ string, copies []string) bool { return slices.Contains(copies, "n2") })
	// The probe pair does not include n2: once n2 is dead its place in a pair goes to a
	// node that no seed fills in this build, and the quiescence check would compare a copy
	// that holds nothing (position 0/0) with the ones that hold the pair.
	probe := fiveIDs(t, c, 1, "b7probe", func(_ string, copies []string) bool { return !slices.Contains(copies, "n2") })
	l := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: fiveBucket, ProbeID: probe[0]})
	fiveWritePre(t, l, c, "n1", "ada", append(probe, ids[:3]...))
	return c, l, ids
}

// fiveKillToDead kills n2 and waits until the survivors have it committed down and then dead,
// calling between once the down commit is in.
func fiveKillToDead(t *testing.T, c *harness.Cluster, l *harness.Ledger, between func()) {
	t.Helper()
	survivors := others(c, "n2")
	l.Fault("kill n2")
	c.Kill("n2")
	fiveAwaitMember(t, c, survivors, "n2", cluster.MemberDown, harness.P0.Detection()+quorumFailoverBudget)
	c.AwaitLeader(10 * time.Second)
	c.AwaitLeases(20 * time.Second)
	between()
	fiveAwaitMember(t, c, survivors, "n2", cluster.MemberDead, 3*time.Second+quorumFailoverBudget)
	l.Note(c, "after the dead commit")
	c.AwaitLeases(20 * time.Second)
}

// TestFiveVotersOneDeadNoOwnershipChangeUntilReady (B7): n2 goes down and, past dead_ms, dead.
// Removal from the ranking would hand its slots to nodes that hold nothing for them, so no
// ownership changes until a gainer is `ready`; no seed completes in this build, so unchanged
// is the whole assertion: the owner of every id is the same after `dead` as after `down`.
func TestFiveVotersOneDeadNoOwnershipChangeUntilReady(t *testing.T) {
	c, l, ids := fiveDeadSetup(t)
	pre, post := ids[:3], ids[3:]
	ownersOf := func() map[string]string {
		owners := map[string]string{}
		for _, id := range ids {
			owner, _, err := c.Node("n1").Owner(proofCtx(t), proofDB, fiveBucket, id)
			if err != nil {
				t.Fatalf("placing %s: %v", id, err)
			}
			owners[id] = owner
		}
		return owners
	}
	var whenDown map[string]string
	fiveKillToDead(t, c, l, func() { whenDown = ownersOf() })
	whenDead := ownersOf()
	for _, id := range ids {
		if whenDown[id] != whenDead[id] {
			t.Errorf("the owner of %s changed from %s to %s between down and dead", id, whenDown[id], whenDead[id])
		}
	}
	for _, id := range pre {
		fiveReadUntil(t, l, c, "n3", "bea", id, quorumFailoverBudget)
	}
	for _, id := range post {
		fiveWriteUntil(t, l, c, "n4", "ada", id)
	}
	fiveJudge(t, l, c, nil)
}

// TestFiveVotersADeadMemberKeepsItsVote (B8): `dead` removes a member from the ranking, never
// from the voter set: five voters before and after, a new bucket still commits, and RF 3 is
// never refused as unsupported.
func TestFiveVotersADeadMemberKeepsItsVote(t *testing.T) {
	c, l, ids := fiveDeadSetup(t)
	voters := func(when string) {
		for _, id := range c.Running() {
			if got := c.Node(id).State().Voters; len(got) != 5 {
				t.Errorf("%s sees %d voters %s, want 5: %v", id, len(got), when, got)
			}
		}
	}
	voters("before the kill")
	fiveKillToDead(t, c, l, func() {})
	voters("after the dead commit")
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateBucket(proofCtx(t), proofDB, "fiveextra", wire.Field{Name: "name", Type: "string"}); err != nil {
		t.Errorf("creating a bucket with a dead member: %v", err)
	}
	for _, id := range ids[3:] {
		fiveWriteUntil(t, l, c, "n4", "ada", id)
	}
	for _, o := range l.Obs() {
		if o.Code == cluster.CodeRFUnsupported {
			t.Errorf("%s through %s was refused RF_UNSUPPORTED with a dead member", o.ID, o.Via)
		}
	}
	fiveJudge(t, l, c, nil)
}
