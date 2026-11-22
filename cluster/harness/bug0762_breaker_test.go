package harness_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// breakerWCTimeout is long enough that a write which waits it out cannot be mistaken for one
// that did not.
const breakerWCTimeout = 3 * time.Second

// breakerCopyDown forms the RF 2 proof cluster with detection and probes off, so only the
// replicator's own view of the peer can end a concern wait, kills the copy holder of a pair and
// returns the ids of two documents on pairs that copy holder held, with the owner that orders them.
func breakerCopyDown(t *testing.T) (c *harness.Cluster, ids [2]string, owner string) {
	t.Helper()
	c = proofSetup(t, harness.Options{NoDetection: true, NoProbes: true, WCTimeout: breakerWCTimeout})
	first, owner, copyNode, outsider := proofID(t, c, "brk-a")
	second := ""
	for i := 0; i < 50 && second == ""; i++ {
		id, o, cp, _ := proofID(t, c, "brk-b"+string(rune('a'+i)))
		if o == owner && cp == copyNode {
			second = id
		}
	}
	if second == "" {
		t.Fatalf("no second id placed on owner %s with copy %s", owner, copyNode)
	}
	// Each survivor resolves the bucket's declaration before the kill, as concernCopyDown does.
	for i, via := range []string{owner, outsider} {
		proofInsert(t, c, via, "brk-warm-"+string(rune('a'+i)), "warm")
	}
	c.Kill(copyNode)
	return c, [2]string{first, second}, owner
}

// TestADownCopyHolderDoesNotCostTheWriteConcernWait: at RF 2 with the copy holder gone and
// nothing committed about it, writes to the owner answer applied_degraded without waiting
// wc_timeout (BUG-0762). Before the breaker each took the whole 3 s.
// [BREAK: Replicator.Await ignores peer health, or the timeout is never reported].
func TestADownCopyHolderDoesNotCostTheWriteConcernWait(t *testing.T) {
	c, ids, owner := breakerCopyDown(t)
	for i, id := range ids {
		start := time.Now()
		got := concernInsert(t, c, owner, id)
		took := time.Since(start)
		if got.Outcome != machine.OutcomeAppliedDegraded || got.CopiesConfirmed != 1 {
			t.Fatalf("write %d answered %v/%d, want applied_degraded/1", i+1, got.Outcome, got.CopiesConfirmed)
		}
		t.Logf("write %d with the copy holder down: %v (wc_timeout %v)", i+1, took.Round(time.Millisecond), breakerWCTimeout)
		if took >= time.Second {
			t.Fatalf("write %d took %v with its copy holder down, want under 1s (wc_timeout is %v)", i+1, took, breakerWCTimeout)
		}
	}
}

// breakerIDs returns count document ids whose pair is owned by owner with copyNode as its copy.
func breakerIDs(t *testing.T, c *harness.Cluster, seed, owner, copyNode string, count int) []string {
	t.Helper()
	probe := c.Node(owner)
	var ids []string
	for i := 0; len(ids) < count && i < 20000; i++ {
		id := fmt.Sprintf("%s-%d", seed, i)
		o, copies, err := probe.Owner(proofCtx(t), proofDB, proofBucket, id)
		if err != nil {
			t.Fatalf("placing %s: %v", id, err)
		}
		if o == owner && len(copies) == 2 && copies[1] == copyNode {
			ids = append(ids, id)
		}
	}
	if len(ids) < count {
		t.Fatalf("found %d ids owned by %s with copy %s, want %d", len(ids), owner, copyNode, count)
	}
	return ids
}

// TestDegradedWritesWithACopyHolderDownRunAtOwnerSpeed: the throughput the bug measured. Before
// the breaker each write took wc_timeout (3 s here), so a sequential writer managed under one
// write a second at best; now it runs at the speed of the owner's own append and one failed send.
// The log line is the measured rate.
// [BREAK: Await ignores peer health].
func TestDegradedWritesWithACopyHolderDownRunAtOwnerSpeed(t *testing.T) {
	c, _, owner := breakerCopyDown(t)
	_, copies, err := c.Node(owner).Owner(proofCtx(t), proofDB, proofBucket, "brk-a-0")
	if err != nil || len(copies) != 2 {
		t.Fatalf("placing the first id: %v %v", copies, err)
	}
	ids := breakerIDs(t, c, "rate", owner, copies[1], 100)
	start := time.Now()
	for _, id := range ids {
		got := concernInsert(t, c, owner, id)
		if got.Outcome != machine.OutcomeAppliedDegraded || got.CopiesConfirmed != 1 {
			t.Fatalf("write of %s answered %v/%d, want applied_degraded/1", id, got.Outcome, got.CopiesConfirmed)
		}
	}
	took := time.Since(start)
	t.Logf("%d sequential writes with the copy holder down: %v, %.0f writes/s (before the breaker: %v each, at most %.2f writes/s)",
		len(ids), took.Round(time.Millisecond), float64(len(ids))/took.Seconds(), breakerWCTimeout, 1/breakerWCTimeout.Seconds())
	if took >= breakerWCTimeout {
		t.Fatalf("%d writes took %v, no better than one write before the breaker (%v)", len(ids), took, breakerWCTimeout)
	}
}

// TestOwnerKillAfterDegradedAckLosesNothingAnAppliedAckSaid: a write answered applied is held by
// the copy, so it survives the owner's kill; a write answered while the copy holder was down is
// labelled applied_degraded with one copy confirmed, never applied, however early it was sent.
// [BREAK: Await returns the number of copies wanted instead of the number that acked].
func TestOwnerKillAfterDegradedAckLosesNothingAnAppliedAckSaid(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true, NoProbes: true, WCTimeout: breakerWCTimeout})
	id, owner, copyNode, _ := proofID(t, c, "brk-own")
	appliedID := id
	got := concernInsert(t, c, owner, appliedID)
	if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed != 2 {
		t.Fatalf("a healthy write answered %v/%d, want applied/2", got.Outcome, got.CopiesConfirmed)
	}
	c.Kill(owner)
	doc, found, err := c.Node(copyNode).LocalDocument(proofCtx(t), proofDB, proofBucket, appliedID)
	if err != nil || !found {
		t.Fatalf("the copy %s does not hold the write the owner acknowledged as applied: found %v err %v (doc %v)", copyNode, found, err, doc)
	}

	// The other direction, on a fresh cluster: the copy holder gone, every answer is labelled.
	d, ids, downOwner := breakerCopyDown(t)
	for _, degraded := range ids {
		answer := concernInsert(t, d, downOwner, degraded)
		if answer.Outcome == machine.OutcomeApplied || answer.CopiesConfirmed != 1 {
			t.Fatalf("a write with the copy holder down answered %v/%d, want applied_degraded/1 and never applied", answer.Outcome, answer.CopiesConfirmed)
		}
	}
}

// TestCommittedDownCopyEndsTheWaitAtOnce: with the failure detector on, once the committed state
// calls the copy holder down it is no longer a copy of the pair, and a write answers
// applied_degraded at once instead of waiting wc_timeout (finding 1 of the design note).
// [BREAK: Await waits for the timeout whatever the copy set and the peer marks say].
func TestCommittedDownCopyEndsTheWaitAtOnce(t *testing.T) {
	c := proofSetup(t, harness.Options{FastDetection: true, WCTimeout: breakerWCTimeout})
	id, owner, copyNode, outsider := proofID(t, c, "brk-cd")
	for i, via := range []string{owner, outsider} {
		proofInsert(t, c, via, "brk-cd-warm-"+string(rune('a'+i)), "warm")
	}
	c.Kill(copyNode)
	survivors := []string{owner, outsider}
	c.Await("the survivors to commit the copy holder down and hold a lease", 30*time.Second, func() bool {
		for _, node := range survivors {
			down := false
			for _, member := range c.Node(node).State().Members {
				down = down || (member.ID == copyNode && member.State == cluster.MemberDown)
			}
			if !down || !c.Node(node).LeaseValid() {
				return false
			}
		}
		return true
	})
	deadline := time.Now().Add(30 * time.Second)
	for {
		start := time.Now()
		got, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bsonDoc(id, "ada"))
		took := time.Since(start)
		if err == nil {
			if got.Outcome != machine.OutcomeAppliedDegraded || got.CopiesConfirmed != 1 {
				t.Fatalf("a write with the copy holder committed down answered %v/%d, want applied_degraded/1", got.Outcome, got.CopiesConfirmed)
			}
			t.Logf("write with the copy holder committed down: %v (wc_timeout %v)", took.Round(time.Millisecond), breakerWCTimeout)
			if took >= time.Second {
				t.Fatalf("the write took %v with its copy holder committed down, want under 1s", took)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no write was accepted within 30s: %v (background: %v)", err, c.BackgroundErrors())
		}
		time.Sleep(100 * time.Millisecond) // a pair held by an open handoff refuses, and nothing was written
	}
}

// TestSixteenWritersMarkAndClearTheBreakerWithoutARace: sixteen writers on pairs of one owner keep
// writing while the copy holder is killed and restarted, so the mark is made and cleared under load.
// Run with -race: a data race, a stuck wait or a lost wake-up fails it.
// [BREAK: suspectChanged is replaced outside the replicator's lock].
func TestSixteenWritersMarkAndClearTheBreakerWithoutARace(t *testing.T) {
	c, _, owner := breakerCopyDown(t)
	_, copies, err := c.Node(owner).Owner(proofCtx(t), proofDB, proofBucket, "brk-a-0")
	if err != nil || len(copies) != 2 {
		t.Fatalf("placing the first id: %v %v", copies, err)
	}
	copyNode := copies[1]
	ids := breakerIDs(t, c, "race", owner, copyNode, 16*8)
	var wg sync.WaitGroup
	var restart sync.Once
	for writer := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 8 {
				if writer == 0 && i == 4 {
					restart.Do(func() { c.Restart(copyNode) })
				}
				id := ids[writer*8+i]
				if _, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bsonDoc(id, "ada")); err != nil {
					t.Errorf("writer %d insert of %s: %v", writer, id, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestTheMarkClearsWhenTheCopyReturns: with detection and probes off, only the replicator's own
// view can tell that a restarted copy holder is back: its applied ack clears the mark and a write
// is applied with both copies again. A hold also ends by itself, so this test shows recovery, not
// which of the two ended it; TestSuspectPeerIsClearedByItsNextAck in cluster/wire pins the clear.
// [BREAK: sends to a suspect peer stop, so the restarted copy is never written to again].
func TestTheMarkClearsWhenTheCopyReturns(t *testing.T) {
	c, _, owner := breakerCopyDown(t)
	_, copies, err := c.Node(owner).Owner(proofCtx(t), proofDB, proofBucket, "brk-a-0")
	if err != nil || len(copies) != 2 {
		t.Fatalf("placing the first id: %v %v", copies, err)
	}
	copyNode := copies[1]
	ids := breakerIDs(t, c, "back", owner, copyNode, 400)
	for _, id := range ids[:3] {
		if got := concernInsert(t, c, owner, id); got.Outcome != machine.OutcomeAppliedDegraded {
			t.Fatalf("a write with the copy down answered %v", got.Outcome)
		}
	}
	c.Restart(copyNode)
	deadline := time.Now().Add(40 * time.Second)
	for _, id := range ids[3:] {
		got, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bsonDoc(id, "ada"))
		if err == nil && got.Outcome == machine.OutcomeApplied && got.CopiesConfirmed == 2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no write was applied with two copies within 40s of the copy holder's restart (last: %v %v)", got, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("ran out of ids before a write was applied with two copies")
}

func bsonDoc(id, name string) bson.D {
	return bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: name}}
}

// TestAStalledCopyIsMarkedByTheTimeoutAloneAndLaterWritesAreFree: the copy is up and silent, so no send
// fails (a send's reply wait is longer than this test's wc_timeout) and only the timeout trigger
// can mark it. The first write pays the whole wc_timeout; the next twenty answer applied_degraded
// at once. This pins the timeout trigger end to end, apart from the send-failure trigger.
// [BREAK: Writer.conclude never calls NoteWaitTimeout].
func TestAStalledCopyIsMarkedByTheTimeoutAloneAndLaterWritesAreFree(t *testing.T) {
	const wcTimeout = 1500 * time.Millisecond
	c := proofSetup(t, harness.Options{NoDetection: true, NoProbes: true, WCTimeout: wcTimeout, RaftTimeout: 600 * time.Millisecond})
	leader := c.AwaitLeader(10 * time.Second)
	var owner, copyNode, outsider string
	for attempt := 0; copyNode == "" || copyNode == leader; attempt++ {
		_, owner, copyNode, outsider = proofID(t, c, fmt.Sprintf("st%d", attempt))
	}
	for i, via := range []string{owner, outsider} {
		proofInsert(t, c, via, fmt.Sprintf("st-warm-%d", i), "warm")
	}
	ids := breakerIDs(t, c, "stall", owner, copyNode, 21)
	c.Stall(copyNode, true)
	t.Cleanup(func() { c.Stall(copyNode, false) }) // a stalled node cannot close
	start := time.Now()
	first := concernInsert(t, c, owner, ids[0])
	firstTook := time.Since(start)
	if first.Outcome != machine.OutcomeAppliedDegraded || firstTook < wcTimeout-100*time.Millisecond {
		t.Fatalf("the first write answered %v after %v, want applied_degraded after about %v", first.Outcome, firstTook, wcTimeout)
	}
	start = time.Now()
	for _, id := range ids[1:] {
		if got := concernInsert(t, c, owner, id); got.Outcome != machine.OutcomeAppliedDegraded || got.CopiesConfirmed != 1 {
			t.Fatalf("a later write answered %v/%d, want applied_degraded/1", got.Outcome, got.CopiesConfirmed)
		}
	}
	laterTook := time.Since(start)
	t.Logf("first write %v, the next %d writes %v together (wc_timeout %v)", firstTook.Round(time.Millisecond), len(ids)-1, laterTook.Round(time.Millisecond), wcTimeout)
	if laterTook >= wcTimeout {
		t.Fatalf("%d writes after the mark took %v, want well under one wc_timeout (%v)", len(ids)-1, laterTook, wcTimeout)
	}
}
