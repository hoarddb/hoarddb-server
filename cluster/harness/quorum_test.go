package harness_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// quorumSetup forms five voters with an RF=3 database.
func quorumSetup(t *testing.T) *harness.Cluster {
	t.Helper()
	c := harness.NewCluster(t, 5, harness.Options{FastDetection: true})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, 3); err != nil {
		t.Fatalf("creating the RF=3 database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), proofDB, proofBucket, wire.Field{Name: "name", Type: "string"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	return c
}

// quorumIDs returns count ids whose slots share one ranked copy set, and that set.
func quorumIDs(t *testing.T, c *harness.Cluster, count int) (ids []string, copies []string) {
	t.Helper()
	for i := 0; len(ids) < count; i++ {
		if i > 20000 {
			t.Fatal("no ids with one copy set")
		}
		id := fmt.Sprintf("q-%d", i)
		_, cs, err := c.Node("n1").Owner(proofCtx(t), proofDB, proofBucket, id)
		if err != nil {
			t.Fatal(err)
		}
		if copies == nil {
			copies = cs
		}
		if fmt.Sprint(cs) == fmt.Sprint(copies) {
			ids = append(ids, id)
		}
	}
	return ids, copies
}

// TestAMinorityOwnerAnswersNoQuorumNotApplied: 5 voters, RF 3. The owner and one copy are
// cut off from the other three. The owner's lease is still valid for up to step_down_ms,
// but it can no longer see a majority, so within the quorum bound it answers NO_QUORUM
// and never `applied`; the failover and the ledger are the next test.
func TestAMinorityOwnerAnswersNoQuorumNotApplied(t *testing.T) {
	c := quorumSetup(t)
	ids, copies := quorumIDs(t, c, 60)
	owner, minorityCopy := copies[0], copies[1]
	acked := map[string]bool{}
	for _, id := range ids[:3] {
		got := proofInsert(t, c, owner, id, "before")
		if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed < 2 {
			t.Fatalf("before the partition %s answered %v/%d, want applied with a majority of 2", id, got.Outcome, got.CopiesConfirmed)
		}
		acked[id] = true
		// The third copy may trail by a frame; a ledger id must be on it before the cut, or
		// losing the other two would lose it (SPEC-GAP U57-15, within contract).
		c.Await("the third copy to hold "+id, 20*time.Second, func() bool {
			_, found, err := c.Node(copies[2]).LocalDocument(proofCtx(t), proofDB, proofBucket, id)
			return err == nil && found
		})
	}
	var majority []string
	for _, id := range c.IDs() {
		if id != owner && id != minorityCopy {
			majority = append(majority, id)
		}
	}
	c.Partition([]string{owner, minorityCopy}, majority)
	began := time.Now()
	var refusal error
	for try := 3; time.Since(began) < 1800*time.Millisecond && try < len(ids); try++ {
		got, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, docOf(ids[try], "minority"))
		if err != nil {
			refusal = err
			break
		}
		if got.Outcome == machine.OutcomeApplied && time.Since(began) > 800*time.Millisecond {
			t.Fatalf("the minority-side owner answered applied %s after the partition", time.Since(began))
		}
		time.Sleep(50 * time.Millisecond) // a fresh id each try; none of these enters the ledger
	}
	if refusal == nil || !strings.Contains(refusal.Error(), "cannot see a majority") {
		t.Fatalf("within 1.8 s of the partition the minority owner's answer was %v; want NO_QUORUM (cannot see a majority)", refusal)
	}
}

// quorumFailoverBudget is how long the majority side may take to serve an acknowledged
// write after a double failure: the stop bound (about 2 s) plus detection, not the 30 s a
// held pair used to burn.
const quorumFailoverBudget = 15 * time.Second

// quorumInsertLedger inserts three ids at owner and waits until the third copy holds each,
// so that losing the owner and the first copy loses none of them (SPEC-GAP U57-15).
func quorumInsertLedger(t *testing.T, c *harness.Cluster, owner, thirdCopy string, ids []string) {
	t.Helper()
	for _, id := range ids {
		proofInsert(t, c, owner, id, "before")
		c.Await("the third copy to hold "+id, 20*time.Second, func() bool {
			_, found, err := c.Node(thirdCopy).LocalDocument(proofCtx(t), proofDB, proofBucket, id)
			return err == nil && found
		})
	}
}

// quorumAwaitServed reads every id through via until it is served, failing after the
// budget, and returns how long the first read that worked took from start.
func quorumAwaitServed(t *testing.T, c *harness.Cluster, via string, ids []string, start time.Time) time.Duration {
	t.Helper()
	for _, id := range ids {
		deadline := time.Now().Add(quorumFailoverBudget)
		for {
			_, err := c.Node(via).Get(proofCtx(t), proofDB, proofBucket, id)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("acknowledged write %s is not readable through %s %s after the double failure: %v", id, via, quorumFailoverBudget, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return time.Since(start)
}

// TestAfterAMinorityPartitionTheMajoritySideServesTheLedger: the majority marks the owner
// and its copy down, the third copy becomes owner and serves every acknowledged write.
// The two commits are separate epochs; the new owner's pair opens on the one surviving
// copy instead of holding for answers that cannot come (BUG-0742, SPEC-GAP U57-17).
func TestAfterAMinorityPartitionTheMajoritySideServesTheLedger(t *testing.T) {
	c := quorumSetup(t)
	ids, copies := quorumIDs(t, c, 6)
	owner, minorityCopy := copies[0], copies[1]
	quorumInsertLedger(t, c, owner, copies[2], ids[:3])
	var majority []string
	for _, id := range c.IDs() {
		if id != owner && id != minorityCopy {
			majority = append(majority, id)
		}
	}
	c.Partition([]string{owner, minorityCopy}, majority)
	began := time.Now()
	took := quorumAwaitServed(t, c, majority[0], ids[:3], began)
	t.Logf("the majority side served the ledger %s after the partition", took)
}

// TestAPlainDoubleKillOpensTheBarrierToo: the same two losses by Kill. The failure detector
// notices the second loss on its own path, so the kill flavour is pinned separately.
func TestAPlainDoubleKillOpensTheBarrierToo(t *testing.T) {
	c := quorumSetup(t)
	ids, copies := quorumIDs(t, c, 6)
	owner, firstCopy, survivor := copies[0], copies[1], copies[2]
	quorumInsertLedger(t, c, owner, survivor, ids[:3])
	epoch := c.Node(survivor).State().Epoch
	c.Kill(owner)
	c.Await("the survivors to commit the owner down", 30*time.Second, func() bool {
		return c.Node(survivor).State().Epoch > epoch
	})
	epoch = c.Node(survivor).State().Epoch
	c.Kill(firstCopy)
	c.Await("the survivors to commit the first copy down", 30*time.Second, func() bool {
		return c.Node(survivor).State().Epoch > epoch
	})
	c.AwaitLeader(10 * time.Second)
	c.AwaitLeases(20 * time.Second)
	took := quorumAwaitServed(t, c, survivor, ids[:3], time.Now())
	t.Logf("the survivor served the ledger %s after the second commit", took)
}

// TestAnRF2PairWhoseCopiesAreBothGoneOpensEmptyNotHeld: RF 2, five voters. Both copies of a
// pair are lost. While they are only down the slot has no owner (conventions section 4: a
// node outside the copy set never owns); when they are declared dead (a tiny dead_ms) the
// next-ranked nodes take the copy set and the new owner, outside the old copy set, has
// nobody to ask. The documented loss (U57-15) is NOT_FOUND, not a hold.
func TestAnRF2PairWhoseCopiesAreBothGoneOpensEmptyNotHeld(t *testing.T) {
	c := harness.NewCluster(t, 5, harness.Options{FastDetection: true})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, 2); err != nil {
		t.Fatalf("creating the RF=2 database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), proofDB, proofBucket, wire.Field{Name: "name", Type: "string"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	if err := leader.SetSetting(proofCtx(t), "dead_ms", 3000); err != nil {
		t.Fatalf("setting dead_ms: %v", err)
	}
	ids, copies := quorumIDs(t, c, 4)
	owner, firstCopy := copies[0], copies[1]
	var survivors []string
	for _, id := range c.IDs() {
		if id != owner && id != firstCopy {
			survivors = append(survivors, id)
		}
	}
	got := proofInsert(t, c, owner, ids[0], "before")
	if got.Outcome != machine.OutcomeApplied {
		t.Fatalf("before the kills %s answered %v, want applied", ids[0], got.Outcome)
	}
	epoch := c.Node(survivors[0]).State().Epoch
	c.Kill(owner)
	c.Await("the owner to be committed down", 30*time.Second, func() bool { return c.Node(survivors[0]).State().Epoch > epoch })
	epoch = c.Node(survivors[0]).State().Epoch
	c.Kill(firstCopy)
	c.Await("the first copy to be committed down", 30*time.Second, func() bool { return c.Node(survivors[0]).State().Epoch > epoch })
	c.AwaitLeader(10 * time.Second)
	c.AwaitLeases(20 * time.Second)
	quorumAwaitOpenedEmpty(t, c, survivors[0], ids[0], survivors)
	// A fresh write is not asserted: the bucket's declaration lives in the _buckets pair of
	// its own slot, and when that pair's two copies were the lost ones the declaration is gone
	// with them (RF 2 loses what only its two copies held), so the write can be refused for
	// that reason on some placements. The read above is the point: opened, not held.
}

// quorumSeedingBudget bounds the whole time a read of a lost pair may answer "still seeding":
// a double loss moves thousands of pairs, the new copy pulls each from the node that opened it
// empty, and the read waits while that copy ranks first. The typical time to NOT_FOUND is
// about 31 s, and two of five runs at dev-host load 19 to 24 failed at exactly a 90 s budget,
// so this is a generous ceiling; the real guard against a stuck seeding is
// quorumSeedingStall.
const quorumSeedingBudget = 240 * time.Second

// quorumSeedingStall is how long the survivors' queued-pair count may stay unchanged while a
// read still answers "still seeding" before the seeding counts as stuck. A slow host that keeps
// moving passes; a seeding that stopped fails at this bound instead of the ceiling.
const quorumSeedingStall = 90 * time.Second

// quorumHeldCauses are the refusals that say the pair is held, the outcome
// TestAnRF2PairWhoseCopiesAreBothGoneOpensEmptyNotHeld forbids. A read refused with one of them
// came after the holder had nothing more to wait for (ReadGate.holdRefusal), so it is never a
// step on the way to opening and fails at once.
var quorumHeldCauses = []string{
	"a primary handoff for it is still open",
	"has not yet been proved stopped",
	"proving that its copies have stopped",
}

// quorumAwaitOpenedEmpty reads id through via until the read answers NOT_FOUND: the lost pair
// opened empty. A held refusal fails at once. "Still seeding" must show progress: the sum of
// the survivors' queued seeding pairs (Node.SeedingPairs) has to change at least every
// quorumSeedingStall, and the whole wait is bounded by quorumSeedingBudget; every other refusal (no owner until the dead commit, a membership change
// not yet applied, catching up) keeps the budget it had, counting only the time spent on such
// refusals. It logs how long each took.
func quorumAwaitOpenedEmpty(t *testing.T, c *harness.Cluster, via, id string, survivors []string) {
	t.Helper()
	queued := func() int {
		total := 0
		for _, survivor := range survivors {
			total += c.Node(survivor).SeedingPairs()
		}
		return total
	}
	const seedingCause = "this node's copy is still seeding"
	otherBudget := quorumFailoverBudget + 10*time.Second
	began := time.Now()
	var seeding, other time.Duration
	last := began
	lastQueued, lastMoved := queued(), began
	for {
		_, err := c.Node(via).Get(proofCtx(t), proofDB, proofBucket, id)
		now := time.Now()
		var refusal *cluster.Error
		if errors.As(err, &refusal) && refusal.Code == cluster.CodeNotFound {
			t.Logf("the lost id %s read NOT_FOUND %s after the dead commit (%s of it still seeding, %s on other refusals)",
				id, now.Sub(began).Round(time.Millisecond), seeding.Round(time.Millisecond), other.Round(time.Millisecond))
			return
		}
		for _, cause := range quorumHeldCauses {
			if err != nil && strings.Contains(err.Error(), cause) {
				t.Fatalf("a read of the lost id %s was held after the dead commit, want it opened empty (NOT_FOUND): %v", id, err)
			}
		}
		if err != nil && strings.Contains(err.Error(), seedingCause) {
			seeding += now.Sub(last)
		} else {
			other += now.Sub(last)
		}
		last = now
		if now := queued(); now != lastQueued {
			lastQueued, lastMoved = now, time.Now()
		}
		if err != nil && strings.Contains(err.Error(), seedingCause) && time.Since(lastMoved) > quorumSeedingStall {
			t.Fatalf("a read of the lost id %s still answered 'still seeding' and the survivors' queued pairs stayed at %d for %s (stall bound %s), want NOT_FOUND: %v", id, lastQueued, time.Since(lastMoved).Round(time.Second), quorumSeedingStall, err)
		}
		if seeding > quorumSeedingBudget {
			t.Fatalf("a read of the lost id %s still answered 'still seeding' after %s of seeding (budget %s), want NOT_FOUND: %v", id, seeding, quorumSeedingBudget, err)
		}
		if other > otherBudget {
			t.Fatalf("a read of the lost id %s was not NOT_FOUND after the dead commit (%s on refusals other than seeding, budget %s): %v", id, other, otherBudget, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestTheDownCopiesGoingDeadChangesTheBarrierNotTheOwnership: with a tiny dead_ms the two
// down copies are declared dead soon after. The surviving owner serves before and after, and
// ownership never moves: dead removal takes names out of the ranking below the survivor.
func TestTheDownCopiesGoingDeadChangesTheBarrierNotTheOwnership(t *testing.T) {
	c := quorumSetup(t)
	ids, copies := quorumIDs(t, c, 6)
	owner, firstCopy, survivor := copies[0], copies[1], copies[2]
	quorumInsertLedger(t, c, owner, survivor, ids[:3])
	var majority []string
	for _, id := range c.IDs() {
		if id != owner && id != firstCopy {
			majority = append(majority, id)
		}
	}
	setCtx := proofCtx(t)
	if err := c.Node(c.AwaitLeader(10*time.Second)).SetSetting(setCtx, "dead_ms", 3000); err != nil {
		t.Fatalf("setting dead_ms: %v", err)
	}
	c.Partition([]string{owner, firstCopy}, majority)
	quorumAwaitServed(t, c, survivor, ids[:3], time.Now())
	c.Await("both lost copies to be declared dead", 30*time.Second, func() bool {
		dead := 0
		for _, member := range c.Node(survivor).State().Members {
			if member.State == cluster.MemberDead && (member.ID == owner || member.ID == firstCopy) {
				dead++
			}
		}
		return dead == 2
	})
	for _, id := range ids[:3] {
		now, _, err := c.Node(survivor).Owner(proofCtx(t), proofDB, proofBucket, id)
		if err != nil || now != survivor {
			t.Fatalf("after the dead commit the owner of %s is %q (err %v), want %s", id, now, err, survivor)
		}
	}
	quorumAwaitServed(t, c, survivor, ids[:3], time.Now())
}

// TestANodeRestartingMidHandoffDoesNotServeBeforeItsTarget: the owner is lost and committed
// down, the next copy becomes owner and opens its awaiting barrier, and that new owner
// restarts before it sends READY. The barrier is memory only, so a restarted node cannot tell
// this from an ownership it always held; it must serve a pair only after step_down_ms +
// heartbeat_ms from boot and a catch-up to its copies (BUG-0743). step_down_ms is widened so
// the restart cannot outlast the bound, and the default detector is kept slow enough that the
// quick restart is not itself committed down.
// Before the fix the restarted owner served at once: Acquire marked the unflagged pair
// replayed and stop-proved (SPEC-GAP U57-19).
func TestANodeRestartingMidHandoffDoesNotServeBeforeItsTarget(t *testing.T) {
	c := quorumSetup(t)
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	const bound = 6100 * time.Millisecond // step_down_ms 6000 + heartbeat_ms 100
	if err := leader.SetSetting(proofCtx(t), "step_down_ms", 6000); err != nil {
		t.Fatalf("setting step_down_ms: %v", err)
	}
	ids, copies := quorumIDs(t, c, 4)
	fresh := ids[3]
	ids = ids[:3]
	owner, newOwner, thirdCopy := copies[0], copies[1], copies[2]
	quorumInsertLedger(t, c, owner, thirdCopy, ids)
	epoch := c.Node(thirdCopy).State().Epoch
	c.Kill(owner)
	c.Await("the owner to be committed down", 30*time.Second, func() bool { return c.Node(thirdCopy).State().Epoch > epoch })
	// The new owner must stay the owner: were it committed down while it restarts, the third
	// copy would become owner and serve once the new owner reports it stopped, which is a
	// different (correct) scenario from the one under test. Stop the detector now that the
	// owner is down. The restart must also see the commit, so wait for it on both survivors.
	c.Await("the new owner to hold the down commit", 30*time.Second, func() bool { return c.Node(newOwner).State().Epoch > epoch })
	if err := c.Node(c.AwaitLeader(10*time.Second)).SetSetting(proofCtx(t), "probe_failures", 1000); err != nil {
		t.Fatalf("stopping the detector: %v", err)
	}
	c.Kill(newOwner)
	booted := time.Now()
	c.Restart(newOwner)
	for _, member := range c.Node(newOwner).State().Members {
		if member.ID == newOwner && member.State != cluster.MemberActive {
			t.Skipf("%s was committed down while it restarted (%v); the scenario needs it to stay owner", newOwner, member.State)
		}
	}

	if time.Since(booted) > bound-2*time.Second {
		t.Skipf("the restart took %s, too close to the %s bound to tell a held pair from a served one", time.Since(booted), bound)
	}
	// Restart returns once the state is applied; the supervisor feeds the handoff table
	// asynchronously, so until that feed lands the node refuses with "not in this node's
	// committed state" (Handoff.settled). Either refusal keeps the property under test; the hold's
	// wording is what must be reached, so poll for it and fail on any served read.
	var err error
	holdBy := time.Now().Add(2 * time.Second)
	for {
		short, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		_, err = c.Node(newOwner).Get(short, proofDB, proofBucket, ids[0])
		cancel()
		if err == nil {
			t.Fatalf("the restarted new owner %s served %s %s after boot, before the %s lease bound and any catch-up", newOwner, ids[0], time.Since(booted), bound)
		}
		if strings.Contains(err.Error(), "restarted") || time.Now().After(holdBy) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The refusal is the hold's, not a node that is merely not serving yet.
	for _, want := range []string{"restarted", "Retry in a few seconds"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the read held by the restart said %q, want it to contain %q", err, want)
		}
	}
	// A write to a held pair is refused at once with the cause, not the internal flags.
	_, err = c.Node(newOwner).Insert(proofCtx(t), proofDB, proofBucket, docOf(fresh, "held"))
	if err == nil {
		t.Fatalf("the restarted new owner %s ordered a write %s after boot, before the %s lease bound", newOwner, time.Since(booted), bound)
	}
	for _, want := range []string{"restarted", "Retry"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the write held by the restart said %q, want it to contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "held true") {
		t.Fatalf("the write refusal prints internal flags: %q", err)
	}
	quorumAwaitServed(t, c, newOwner, ids, booted)
}
