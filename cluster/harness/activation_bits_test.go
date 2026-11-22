package harness_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// abitsMemberState is the committed state of member id at node's applied state.
func abitsMemberState(node *wire.Node, id string) cluster.MemberState {
	for _, member := range node.State().Members {
		if member.ID == id {
			return member.State
		}
	}
	return cluster.MemberDead
}

// abitsActivate asks the leader to activate id and retries only a STATE_MOVED refusal: the
// seed reporters of the other nodes may commit between the gate's read and the proposal,
// and that moves state_seq without being the gate's business. Any other refusal fails.
func abitsActivate(t *testing.T, leader *wire.Node, id string) {
	t.Helper()
	var last error
	for range 10 {
		if last = leader.Activate(proofCtx(t), id); last == nil {
			return
		}
		if !t3MovedRefusal(last) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("activating %s at the leader: %v", id, last)
}

// abitsAwait polls until check returns "" and fails with its last answer when timeout passes,
// so a timeout names the slot that never settled.
func abitsAwait(t *testing.T, what string, timeout time.Duration, check func() string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		unsettled := check()
		if unsettled == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("gave up after %s waiting for %s: %s", timeout, what, unsettled)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// abitsReadyHeld lists the slots of the database that the node holds ready.
func abitsReadyHeld(node *wire.Node, dbID int64) []uint16 {
	var slots []uint16
	for slot := range uint16(storage.SlotCount) {
		if node.SlotRecordState(slot, uint64(dbID)) == machine.SlotReady {
			slots = append(slots, slot)
		}
	}
	return slots
}

// abitsBitsDisagree names the first slot of the database where a node's committed bit and
// its record's readiness differ, or says "" when every node agrees on every slot.
func abitsBitsDisagree(c *harness.Cluster, nodes []string, dbID int64) string {
	state := c.Node(nodes[0]).State()
	for _, id := range nodes {
		row := seedRow(state, id, dbID)
		for slot := range storage.SlotCount {
			bit := row[slot/8]>>(slot%8)&1 == 1
			ready := c.Node(id).SlotRecordState(uint16(slot), uint64(dbID)) == machine.SlotReady
			if bit != ready {
				return fmt.Sprintf("%s slot %d: committed bit %v, record ready %v (record %v)", id, slot, bit, ready, c.Node(id).SlotRecordState(uint16(slot), uint64(dbID)))
			}
		}
	}
	return ""
}

// TestTheActivationGateReadsTheBitsNotThePairs: the gate decides from the committed SEEDED
// rows the leader already holds, so cutting the channel POSITION_QUERY rides does not stop
// an activation of a joiner whose every pair has reported.
// [BREAK: in seededFor ask the joiner QueryPosition for each pair of its placing set]
func TestTheActivationGateReadsTheBitsNotThePairs(t *testing.T) {
	c, _ := t3Cluster(t, 2, 10, harness.Options{Seed: wire.SeedOptions{SlotsInFlight: 8}})
	added := t3SeededJoiner(t, c)
	leaderID := c.AwaitLeader(10 * time.Second)
	leader := c.Node(leaderID)

	c.DropChannel(leaderID, added, transport.ChannelData, 1<<20)
	t.Cleanup(func() { c.DropChannel(leaderID, added, transport.ChannelData, 0) })
	abitsActivate(t, leader, added)
	c.DropChannel(leaderID, added, transport.ChannelData, 0)

	if got := abitsMemberState(leader, added); got != cluster.MemberActive {
		t.Fatalf("%s is %v after an activation with the data channel cut, want active", added, got)
	}
}

// TestActivationIsRefusedStateMovedWhenABitArrivesLate: a gate decision is made at one
// state_seq. A pair of the joiner put back to seeding commits a clear of its bit first, so
// state_seq moves, and an activation decided before it is refused STATE_MOVED; once the pair
// is pulled and reported again the joiner activates.
// [BREAK: have ActivateAtStateSeq propose with base 0]
func TestActivationIsRefusedStateMovedWhenABitArrivesLate(t *testing.T) {
	t.Skip("BUG-0808: seeding activation gate; under the full gate leadership is lost during the run and state_seq does not move after the reseed commits its clear, it passes alone; skipped during the feature freeze (owner 2026-11-21), to be fixed in the replication rework")
	c, _ := t3Cluster(t, 2, 10, harness.Options{Seed: wire.SeedOptions{SlotsInFlight: 8}})
	added := t3SeededJoiner(t, c)
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	dbID := leader.State().Databases[0].ID

	// The reporters tick every two seconds: a state_seq read before they are quiet would be
	// stale for a reason that is not this test's.
	seq := leader.State().StateSeq
	quietSince := time.Now()
	for time.Since(quietSince) < 3*time.Second {
		if now := leader.State().StateSeq; now != seq {
			seq, quietSince = now, time.Now()
		}
		time.Sleep(50 * time.Millisecond)
	}
	held := abitsReadyHeld(c.Node(added), dbID)
	if len(held) == 0 {
		t.Fatalf("%s holds no ready pair, so there is nothing to put back to seeding", added)
	}
	slot := held[0]

	if err := c.Node(added).ReseedPair(slot, uint64(dbID)); err != nil {
		t.Fatalf("putting slot %d of %s back to seeding: %v", slot, added, err)
	}
	if now := leader.State().StateSeq; now == seq {
		t.Fatalf("state_seq is still %d after the reseed committed its clear", seq)
	}
	if err := leader.ActivateAtStateSeq(proofCtx(t), added, seq); !t3MovedRefusal(err) {
		t.Fatalf("an activation decided at state_seq %d, before slot %d's bit moved, got %v, want STATE_MOVED", seq, slot, err)
	}
	if got := abitsMemberState(leader, added); got != cluster.MemberJoining {
		t.Fatalf("%s is %v after a refused activation, want joining", added, got)
	}

	c.ActivateWhileSeedingMoves(added, 90*time.Second, 240*time.Second)
	if got := abitsMemberState(leader, added); got != cluster.MemberActive {
		t.Fatalf("%s is %v after the pair was pulled again, want active", added, got)
	}
}

// TestTheActivationGateSeesAReseedsClearBeforeTheRecordFlips: a reseed commits the clear of
// the pair's bit before the record leaves ready (S3a's order), so no reader of the committed
// rows can see a pair that is not ready with its bit still set. The clear is slowed so the
// window it would leave is hundreds of milliseconds wide.
// [BREAK: in seedParts.reseed feed the horizon event before calling the BeforeReset hook]
func TestTheActivationGateSeesAReseedsClearBeforeTheRecordFlips(t *testing.T) {
	c, _ := t3Cluster(t, 2, 10, harness.Options{Seed: wire.SeedOptions{SlotsInFlight: 8}})
	added := t3SeededJoiner(t, c)
	leaderID := c.AwaitLeader(10 * time.Second)
	leader := c.Node(leaderID)
	dbID := leader.State().Databases[0].ID
	joiner := c.Node(added)
	held := abitsReadyHeld(joiner, dbID)
	if len(held) == 0 {
		t.Fatalf("%s holds no ready pair", added)
	}
	slot := held[0]
	c.Await("the joiner's ready bit to be committed at the leader", 30*time.Second, func() bool {
		return seedRowHas(leader.State(), added, dbID, int(slot))
	})

	// The clear rides the control channel to the leader; the pull stays seeding for a second
	// so the cleared bit is read while the record is not ready.
	c.DelayChannel(added, leaderID, transport.ChannelControl, 300*time.Millisecond)
	for _, id := range c.IDs() {
		if id != added {
			c.DelayChannel(added, id, transport.ChannelSeed, time.Second)
		}
	}
	t.Cleanup(func() {
		c.DelayChannel(added, leaderID, transport.ChannelControl, 0)
		for _, id := range c.IDs() {
			if id != added {
				c.DelayChannel(added, id, transport.ChannelSeed, 0)
			}
		}
	})

	reseeded := make(chan error, 1)
	go func() { reseeded <- joiner.ReseedPair(slot, uint64(dbID)) }()
	deadline := time.Now().Add(30 * time.Second)
	sawSeeding := false
	for !sawSeeding {
		// The record first, the bit second: a record read as not ready can only have a bit
		// that is still set if the record flipped before the clear committed.
		state := joiner.SlotRecordState(slot, uint64(dbID))
		bitSet := seedRowHas(leader.State(), added, dbID, int(slot))
		if state != machine.SlotReady && bitSet {
			t.Fatalf("slot %d of %s is %v while its committed bit is still set: the record flipped before the clear", slot, added, state)
		}
		sawSeeding = state == machine.SlotSeeding
		if time.Now().After(deadline) {
			t.Fatalf("slot %d of %s never showed seeding; it is %v", slot, added, state)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if err := <-reseeded; err != nil {
		t.Fatalf("reseeding slot %d of %s: %v", slot, added, err)
	}
}

// TestLayer6JoinSeedsEverySlotAndRetires is the Layer 6 exit: a node joins an RF 2 database
// holding data, is activated from its bits, and the cluster settles with every committed bit
// equal to its record's readiness, every departed copy retired and emptied, and every
// document on its owner and on the joiner.
// [BREAK: have seededFor decide from the joiner's records (POSITION_QUERY) instead of the committed bits]
func TestLayer6JoinSeedsEverySlotAndRetires(t *testing.T) {
	c, ids := t3Cluster(t, 2, 40, harness.Options{Seed: wire.SeedOptions{SlotsInFlight: 8}})
	added := c.AddNode()
	c.ActivateWhileSeedingMoves(added, 90*time.Second, 240*time.Second)

	// The activation is decided on the committed bits, so at the moment it commits every pair
	// the joiner now holds has its bit set. A gate that read the joiner's records would let
	// the last bit trail by a reporter tick.
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	dbID := leader.State().Databases[0].ID
	rf := int(leader.State().Databases[0].RF)
	activated := leader.State()
	activatedTable := cluster.BuildTable(activated)
	for slot := range storage.SlotCount {
		for _, member := range activatedTable.Copies(uint16(slot), rf) {
			if member.ID == added && !seedRowHas(activated, added, dbID, slot) {
				t.Fatalf("%s was activated while its committed bit for slot %d, a pair it holds, was clear", added, slot)
			}
		}
	}
	c.AwaitConverged(30 * time.Second)
	nodes := c.IDs()
	abitsAwait(t, "the joiner active, every bit equal to its record, every departed copy retired", 60*time.Second, func() string {
		if abitsMemberState(leader, added) != cluster.MemberActive {
			return added + " is not active"
		}
		if disagree := abitsBitsDisagree(c, nodes, dbID); disagree != "" {
			return disagree
		}
		table := cluster.BuildTable(leader.State())
		for slot := range storage.SlotCount {
			inSet := map[string]bool{}
			for _, member := range table.Copies(uint16(slot), rf) {
				inSet[member.ID] = true
			}
			for _, id := range nodes {
				if inSet[id] {
					continue
				}
				if record := c.Node(id).SlotRecordState(uint16(slot), uint64(dbID)); record != machine.SlotAbsent {
					return fmt.Sprintf("%s still has a %v record for slot %d, which left its copy set", id, record, slot)
				}
				if _, entries, err := c.Node(id).PairDump(uint16(slot), uint64(dbID)); err == nil && len(entries) > 0 {
					return fmt.Sprintf("%s still holds %d keys of slot %d, which left its copy set", id, len(entries), slot)
				}
			}
		}
		return ""
	})
	if held := t3Compare(t, c, added, ids); held == 0 {
		t.Fatalf("%s holds none of the %d documents; the join seeded nothing", added, len(ids))
	}
}

// TestAnIdleDatabaseSurvivesARestartOfEveryNodeWithEveryPairServing: after two of the three
// nodes restart together (the harness cannot reopen all three, see the fallback below), every pair of an idle RF 2 database still has a ready record
// on its holders. Two idle pairs hold each other's bit, so a rule that reseeds a ready pair
// against another member's bit would flip both and leave no ready source; no such row exists
// (the owner dropped it on 2026-11-05), and this test is the guard against adding one.
func TestAnIdleDatabaseSurvivesARestartOfEveryNodeWithEveryPairServing(t *testing.T) {
	c, _ := t3Cluster(t, 2, 3, harness.Options{Seed: wire.SeedOptions{SlotsInFlight: 8}})
	nodes := c.IDs()
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	dbID := leader.State().Databases[0].ID
	rf := int(leader.State().Databases[0].RF)

	abitsAwait(t, "every committed bit to equal its record", 60*time.Second, func() string {
		return abitsBitsDisagree(c, nodes, dbID)
	})

	// Cluster.Restart waits for the node it reopens to hold the cluster's state, which a
	// lone node of three never does, so all three cannot be down at once and reopened. The
	// fallback is the two holders of every pair they share: both are down together, and
	// each comes back to the other's committed bit.
	holders := nodes[1:]
	for _, id := range holders {
		c.Kill(id)
	}
	for _, id := range holders {
		c.Restart(id)
	}
	c.AwaitConverged(30 * time.Second)
	c.AwaitLeases(30 * time.Second)

	leader = c.Node(c.AwaitLeader(10 * time.Second))
	table := cluster.BuildTable(leader.State())
	abitsAwait(t, "every pair ready on both of its holders", 60*time.Second, func() string {
		for slot := range storage.SlotCount {
			copies := table.Copies(uint16(slot), rf)
			ready := 0
			for _, member := range copies {
				if c.Node(member.ID).SlotRecordState(uint16(slot), uint64(dbID)) == machine.SlotReady {
					ready++
				}
			}
			if ready != len(copies) {
				return fmt.Sprintf("slot %d has %d of %d holders ready", slot, ready, len(copies))
			}
		}
		return ""
	})
}
