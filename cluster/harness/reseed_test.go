package harness_test

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// reseedWindow is replication.repl_window as the harness runs it: Options has no knob for it
// (cluster.go belongs to another slice), so the test writes twice the default of 64.
const reseedWindow = 64

// reseedIDsInOneSlot returns count ids whose slot is the same as seed's, so every write lands
// in one pair with one owner and one copy.
func reseedIDsInOneSlot(t *testing.T, seed string, count int) []string {
	t.Helper()
	slot, err := wire.PlacementOf(proofDB, proofBucket, seed)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{seed}
	for i := 0; len(ids) < count && i < 50_000_000; i++ {
		candidate := fmt.Sprintf("r-%d", i)
		if got, err := wire.PlacementOf(proofDB, proofBucket, candidate); err == nil && got == slot {
			ids = append(ids, candidate)
		}
	}
	if len(ids) < count {
		t.Fatalf("found %d ids in slot %d, want %d", len(ids), slot, count)
	}
	return ids
}

// TestACopyDownPastTheWindowReseedsAndServes is the evidence storage-format F7 asks for before
// log truncation may ship (seeding-completion plan §1.7): a copy that stays a member but stops
// hearing the owner's data pushes falls past the owner's log, is answered LOG_BELOW_HORIZON,
// resets its pair, pulls it again, and then serves every document and confirms new writes.
// The cut is one channel in one direction, so the copy stays active in the committed state.
// [BREAK 1: make CatchUp swallow the horizon answer: the final write stays applied_degraded]
// [BREAK 2: truncate the owner's log again after the reseed's registration: the test then
// meets SEED_RESTART 1 and a fresh cut, not a hole]
func TestACopyDownPastTheWindowReseedsAndServes(t *testing.T) {
	started := time.Now()
	c := proofSetup(t, harness.Options{Seed: wire.SeedOptions{ChunkEntries: 2}})
	seed, owner, copyNode, _ := proofID(t, c, "f7")
	ids := reseedIDsInOneSlot(t, seed, 2*reseedWindow+28)
	slot, err := wire.PlacementOf(proofDB, proofBucket, seed)
	if err != nil {
		t.Fatal(err)
	}
	dbID := uint64(c.Node(owner).State().Databases[0].ID)
	c.AwaitLeases(30 * time.Second)
	c.SmallLogSegments(owner, 512)

	insert := func(id string, options ...wire.WriteOption) wire.Inserted {
		t.Helper()
		got, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada " + id}}, options...)
		if err != nil {
			t.Fatalf("insert of %s through the owner %s: %v (background: %v)", id, owner, err, c.BackgroundErrors())
		}
		return got
	}

	// Before the cut the copy holds what the owner acknowledged with two copies.
	if first := insert(ids[0]); first.Outcome != machine.OutcomeApplied || first.CopiesConfirmed != 2 {
		t.Fatalf("the first write: outcome %v copies_confirmed %d, want applied and 2", first.Outcome, first.CopiesConfirmed)
	}

	// The cut: the owner's data pushes to the copy stop. Raft, control, the seed channel and the
	// copy's own dials stay up.
	c.DropChannel(owner, copyNode, transport.ChannelData, 1<<20)
	acknowledged := []string{ids[0]}
	for _, id := range ids[1 : 2*reseedWindow+4] {
		insert(id, wire.WithConcern(machine.ConcernOne))
		acknowledged = append(acknowledged, id)
	}
	assertCopyStaysActive(t, c, owner, copyNode, seed)

	// The owner's log is cut below what the copy needs. Nothing on the owner pins it: a copy's
	// shipping cursor pins nothing.
	if err := c.TruncateLogBelow(owner, math.MaxUint64); err != nil {
		t.Fatalf("truncating the owner's log: %v", err)
	}

	// Slow the pull so the test can see the pair seeding, then lift the cut. Heal does not lift
	// a channel drop; zero dials does.
	c.DelayChannel(copyNode, owner, transport.ChannelSeed, 800*time.Millisecond)
	cutLifted := time.Now()
	c.DropChannel(owner, copyNode, transport.ChannelData, 0)

	// A write while the copy is pulled cannot be confirmed by it.
	degraded := insert(ids[2*reseedWindow+4])
	acknowledged = append(acknowledged, ids[2*reseedWindow+4])
	if degraded.Outcome != machine.OutcomeAppliedDegraded {
		t.Fatalf("a write during the pull: outcome %v copies_confirmed %d, want applied_degraded", degraded.Outcome, degraded.CopiesConfirmed)
	}
	c.Await("the copy's record to go back to seeding", 30*time.Second, func() bool {
		return c.Node(copyNode).SlotRecordState(slot, dbID) == machine.SlotSeeding
	})
	c.DelayChannel(copyNode, owner, transport.ChannelSeed, 0)
	c.Await("the copy's pair to be ready again", 60*time.Second, func() bool {
		return c.Node(copyNode).SlotRecordState(slot, dbID) == machine.SlotReady
	})
	horizonToReady := time.Since(cutLifted)

	// Every acknowledged document is readable at the copy, byte for byte.
	for _, id := range acknowledged {
		want, found, err := c.Node(owner).LocalDocument(proofCtx(t), proofDB, proofBucket, id)
		if err != nil || !found {
			t.Fatalf("the owner lost %s: found %v err %v", id, found, err)
		}
		got, found, err := c.Node(copyNode).LocalDocument(proofCtx(t), proofDB, proofBucket, id)
		if err != nil || !found {
			t.Fatalf("the copy %s did not get %s back: found %v err %v", copyNode, id, found, err)
		}
		if fmt.Sprint(got.Fields) != fmt.Sprint(want.Fields) {
			t.Fatalf("the copy holds %v for %s, the owner %v", got.Fields, id, want.Fields)
		}
	}

	// A write after ready is confirmed by both copies again: the owner pushes to the reset copy.
	// The ids are from the reseeded pair's own slot, so the probe checks that pair.
	probes := ids[2*reseedWindow+5:]
	nextProbe := 0
	var final wire.Inserted
	c.Await("a write to be applied with two copies after the reseed", 30*time.Second, func() bool {
		if nextProbe >= len(probes) {
			t.Fatalf("no probe id left in the slot after %d tries", nextProbe)
		}
		id := probes[nextProbe]
		// Before the degraded-ack breaker (BUG-0762) each probe that the copy had not confirmed
		// waited the 500 ms wc_timeout, which paced this loop for the owner's retry of the reset
		// copy (a backoff the copy's not-applied answers grew while it seeded). A probe now answers
		// degraded at once while the copy is marked, so the loop is paced by hand to the same rate.
		if nextProbe > 0 {
			time.Sleep(500 * time.Millisecond)
		}
		nextProbe++
		got, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}})
		final = got
		return err == nil && got.Outcome == machine.OutcomeApplied && got.CopiesConfirmed == 2
	})
	t.Logf("F7: repl_window %d, writes while cut %d, wall time %v, lift to ready %v, final write %v with %d copies",
		reseedWindow, len(acknowledged)-1, time.Since(started).Round(time.Millisecond), horizonToReady.Round(time.Millisecond), final.Outcome, final.CopiesConfirmed)
}

// assertCopyStaysActive checks that the copy is still an active member and still in the pair's
// copy set while its channel is cut: the scenario is a copy that fell behind, not one that left.
func assertCopyStaysActive(t *testing.T, c *harness.Cluster, owner, copyNode, id string) {
	t.Helper()
	for _, member := range c.Node(owner).State().Members {
		if member.ID == copyNode && member.State != cluster.MemberActive {
			t.Fatalf("while the channel is cut %s is %v in the committed state, want active", copyNode, member.State)
		}
	}
	_, copies, err := c.Node(owner).Owner(proofCtx(t), proofDB, proofBucket, id)
	if err != nil {
		t.Fatal(err)
	}
	for _, held := range copies {
		if held == copyNode {
			return
		}
	}
	t.Fatalf("while the channel is cut the copy set of the pair is %v, want it to hold %s", copies, copyNode)
}
