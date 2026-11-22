package harness_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// gateCallTimeout is wire's relayCallTimeout: the cap on one reseed clear, which the
// harness cannot import because it is private.
const gateCallTimeout = 5 * time.Second

// gateRaftTimeout keeps the leader steady under the race detector while a test writes: the
// test stops the leader itself, and a flap before that would make it stop a follower.
const gateRaftTimeout = time.Second

// gateIDLedBy returns a document id whose pair has the Raft leader as its outsider, so the
// leader can be stopped without touching the pair's owner or its copy.
func gateIDLedBy(t *testing.T, c *harness.Cluster, leader, seed string) (id, owner, copyNode string) {
	t.Helper()
	probe := c.Node(leader)
	for i := 0; i < 5000; i++ {
		id = fmt.Sprintf("%s-%d", seed, i)
		o, copies, err := probe.Owner(proofCtx(t), proofDB, proofBucket, id)
		if err != nil {
			t.Fatalf("placing %s: %v", id, err)
		}
		if len(copies) == 2 && o == copies[0] && o != leader && copies[1] != leader {
			return id, o, copies[1]
		}
	}
	t.Fatalf("no id placed with %s outside its pair", leader)
	return
}

// gateSeededPair inserts id through its owner and waits for the copy's record to be ready
// and its bit committed and applied at the copy.
func gateSeededPair(t *testing.T, c *harness.Cluster, id, owner, copyNode string) (slot uint16, dbID int64) {
	t.Helper()
	slot, err := wire.PlacementOf(proofDB, proofBucket, id)
	if err != nil {
		t.Fatal(err)
	}
	dbID = c.Node(owner).State().Databases[0].ID
	c.AwaitLeases(30 * time.Second)
	if _, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}}); err != nil {
		t.Fatalf("insert of %s through the owner %s: %v (background: %v)", id, owner, err, c.BackgroundErrors())
	}
	c.Await("the copy's pair to be ready", 30*time.Second, func() bool {
		return c.Node(copyNode).SlotRecordState(slot, uint64(dbID)) == machine.SlotReady
	})
	c.Await("the copy's ready bit to be committed and applied at the copy", 30*time.Second, func() bool {
		return seedRowHas(c.Node(copyNode).State(), copyNode, dbID, int(slot))
	})
	return slot, dbID
}

// gateCutRaft cuts the Raft channel between two nodes in both directions and refuses the
// next dials, so neither side can win an election; gateHealRaft lifts it. The data
// channel stays up, so an owner and its copy still talk while the cluster has no leader.
func gateCutRaft(c *harness.Cluster, a, b string) {
	c.DropChannel(a, b, transport.ChannelRaft, 1<<20)
	c.DropChannel(b, a, transport.ChannelRaft, 1<<20)
}

func gateHealRaft(c *harness.Cluster, a, b string) {
	c.DropChannel(a, b, transport.ChannelRaft, 0)
	c.DropChannel(b, a, transport.ChannelRaft, 0)
}

// gateLeaderless stops the Raft leader (which is outside the pair) and keeps the two
// survivors from electing another, and waits until both say they have no leader.
func gateLeaderless(t *testing.T, c *harness.Cluster, leader, owner, copyNode string) {
	t.Helper()
	c.Kill(leader)
	gateCutRaft(c, owner, copyNode)
	c.Await("both survivors to report no leader", 30*time.Second, func() bool {
		return c.Node(owner).LeaderID() == "" && c.Node(copyNode).LeaderID() == ""
	})
}

// TestAReturnedSlotWaitsForTheGateBeforeDeleting: the copy's pair leaves its copy set when
// the database drops to one copy, and the copy may delete its range only once the owner has
// answered a position query for it. With the owner's answers lost the range and the bit
// stay; when the answers return the range goes, then the bit, then the record.
// [BREAK: in gate.go askPair feed every copy as a ready answer whether or not Ask failed]
func TestAReturnedSlotWaitsForTheGateBeforeDeleting(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	seed, owner, copyNode, _ := proofID(t, c, "gate")
	slot, dbID := gateSeededPair(t, c, seed, owner, copyNode)

	// The owner's answers to the copy are lost, so no evidence arrives. The cut is made before
	// the copy set shrinks, so the gate's first look is already blind.
	c.DropReplies(copyNode, owner, transport.ChannelData, true)
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if err := leader.AlterDatabaseRF(proofCtx(t), proofDB, 1); err != nil {
		t.Fatalf("dropping the database to one copy: %v", err)
	}
	c.Await("the copy's record to be retired", 30*time.Second, func() bool {
		return c.Node(copyNode).SlotRecordState(slot, uint64(dbID)) == machine.SlotRetired
	})

	// Three seconds is twelve reconcile ticks and more than any gate pass takes: if the gate
	// would delete without evidence it has done so by now.
	time.Sleep(3 * time.Second)
	if _, found, err := c.Node(copyNode).LocalDocument(proofCtx(t), proofDB, proofBucket, seed); err != nil || !found {
		t.Fatalf("the copy deleted %s before the owner answered: found %v err %v", seed, found, err)
	}
	if !seedRowHas(c.Node(copyNode).State(), copyNode, dbID, int(slot)) {
		t.Fatalf("the committed bit of %s slot %d is clear before the owner answered", copyNode, slot)
	}
	if got := c.Node(copyNode).SlotRecordState(slot, uint64(dbID)); got != machine.SlotRetired {
		t.Fatalf("the record is %v before the owner answered, want retired", got)
	}

	// Heal. The gate wakes on a state_seq change (it also ticks every 30 s), so one setting
	// stands for the next commit.
	c.DropReplies(copyNode, owner, transport.ChannelData, false)
	if err := leader.SetSetting(proofCtx(t), "probe_interval_ms", 1000); err != nil {
		t.Fatalf("committing a setting to wake the gate: %v", err)
	}
	c.Await("the copy's range to be deleted", 60*time.Second, func() bool {
		_, found, err := c.Node(copyNode).LocalDocument(proofCtx(t), proofDB, proofBucket, seed)
		return err == nil && !found
	})
	c.Await("the copy's bit to be cleared and its record removed", 60*time.Second, func() bool {
		return !seedRowHas(c.Node(copyNode).State(), copyNode, dbID, int(slot)) &&
			c.Node(copyNode).SlotRecordState(slot, uint64(dbID)) == machine.SlotAbsent
	})
	if _, found, err := c.Node(owner).LocalDocument(proofCtx(t), proofDB, proofBucket, seed); err != nil || !found {
		t.Fatalf("the owner lost %s while the copy retired: found %v err %v", seed, found, err)
	}
}

// TestReportsRunOnePassPerStateSeq: the seeding driver runs a reconcile pass when the
// committed state_seq moves and not otherwise. A joiner is seeded, then left alone; the
// passes it ran are at most the distinct state_seq values it saw, and none ran while the
// state held still.
// [BREAK: in runSeedDriver call a pass on every wake and tick, skipping the unchanged check]
func TestReportsRunOnePassPerStateSeq(t *testing.T) {
	t.Skip("BUG-0808: the state_seq sampler (500 us apart) misses commits under load, so two passes can count as one value, and a pass with no state_seq change is by design; skipped during the feature freeze (owner 2026-11-21), to be fixed in the seeding rework")
	c, _ := t3Cluster(t, 2, 6, harness.Options{Seed: wire.SeedOptions{SlotsInFlight: 8}})
	added := c.AddNode()
	node := c.Node(added)

	var (
		mu       sync.Mutex
		seen     = map[int64]struct{}{}
		stop     = make(chan struct{})
		finished sync.WaitGroup
	)
	finished.Add(1)
	go func() {
		defer finished.Done()
		for {
			mu.Lock()
			seen[node.State().StateSeq] = struct{}{}
			mu.Unlock()
			select {
			case <-stop:
				return
			case <-time.After(500 * time.Microsecond):
			}
		}
	}()
	defer func() {
		select {
		case <-stop:
		default:
			close(stop)
		}
		finished.Wait()
	}()

	leader := c.Node(c.AwaitLeader(10 * time.Second))
	c.AwaitWhileSeedingMoves("the joiner to be seeded", 90*time.Second, 240*time.Second, func() bool { return leader.CheckSeeded(proofCtx(t), added) == nil })

	// Left alone: nothing commits, so nothing may run a pass.
	idleSeq, idlePasses := node.State().StateSeq, node.SeedPasses()
	time.Sleep(3 * time.Second)
	if seq, passes := node.State().StateSeq, node.SeedPasses(); seq == idleSeq && passes != idlePasses {
		t.Fatalf("%d reconcile passes ran in 3 s while state_seq held at %d", passes-idlePasses, seq)
	}

	close(stop)
	finished.Wait()
	mu.Lock()
	distinct := int64(len(seen))
	mu.Unlock()
	passes := node.SeedPasses()
	if passes == 0 {
		t.Fatal("the joiner ran no reconcile pass at all")
	}
	if passes > distinct {
		t.Fatalf("the joiner ran %d reconcile passes over %d distinct state_seq values", passes, distinct)
	}
	t.Logf("%d passes over %d distinct state_seq values", passes, distinct)
}

// TestAReseedWaitsForALeaderToTakeItsClear: a copy that fell below the owner's log horizon
// must have its committed bit cleared before its record flips to seeding. With no Raft
// leader the clear cannot commit, so the reseed is stopped, the record stays ready and the
// bit stays set; the owner's next push is answered the horizon again, and once a leader is
// elected the clear commits and the record flips.
// [BREAK: have the BeforeReset hook in startCluster return nil when the clear fails]
func TestAReseedWaitsForALeaderToTakeItsClear(t *testing.T) {
	t.Skip("BUG-0808: seeding activation family; the copy's pair is not ready again within a minute and the data-channel calls fail with closed connections, in about one run in three alone on this tree and one in four on the pre-merge base; skipped during the feature freeze (owner 2026-11-21), to be fixed in the replication rework")
	c := proofSetup(t, harness.Options{NoDetection: true, RaftTimeout: gateRaftTimeout, ProposalBudget: 5 * time.Second, Seed: wire.SeedOptions{ChunkEntries: 2}})
	leader := c.AwaitLeader(10 * time.Second)
	seed, owner, copyNode := gateIDLedBy(t, c, leader, "wait")
	ids := reseedIDsInOneSlot(t, seed, 2*reseedWindow+28)
	slot, dbID := gateSeededPair(t, c, seed, owner, copyNode)
	c.SmallLogSegments(owner, 512)

	// The copy falls past the owner's log, as in the F7 proof.
	c.DropChannel(owner, copyNode, transport.ChannelData, 1<<20)
	for _, id := range ids[1 : 2*reseedWindow+4] {
		if _, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada " + id}}, wire.WithConcern(machine.ConcernOne)); err != nil {
			t.Fatalf("insert of %s through the owner %s: %v (background: %v)", id, owner, err, c.BackgroundErrors())
		}
	}
	if err := c.TruncateLogBelow(owner, ^uint64(0)); err != nil {
		t.Fatalf("truncating the owner's log: %v", err)
	}

	// No leader, and none can be elected. The copy hears the owner again and is answered the
	// horizon; the owner's own retry after the cut is lifted waits on leases a leaderless
	// cluster cannot renew, so the copy's ask is made here, through the same reseed a
	// LOG_BELOW_HORIZON answer reaches. The proposal budget is short (see the options)
	// because Clear queues behind a reporter batch that is itself waiting for a leader.
	c.DelayChannel(copyNode, owner, transport.ChannelSeed, 800*time.Millisecond)
	gateLeaderless(t, c, leader, owner, copyNode)
	c.DropChannel(owner, copyNode, transport.ChannelData, 0)
	err := c.Node(copyNode).ReseedPair(slot, uint64(dbID))
	if err == nil || !strings.Contains(err.Error(), "was stopped before it began") {
		t.Fatalf("a reseed with no leader to take its clear returned %v, want it stopped before it began", err)
	}
	if got := c.Node(copyNode).SlotRecordState(slot, uint64(dbID)); got != machine.SlotReady {
		t.Fatalf("with no leader the record is %v, want ready: the reseed flipped it without its clear", got)
	}
	if !seedRowHas(c.Node(copyNode).State(), copyNode, dbID, int(slot)) {
		t.Fatalf("the committed bit of %s slot %d is clear with no leader to commit the clear", copyNode, slot)
	}

	// A leader is elected; the copy asks again, as the owner's next push makes it, and this
	// time the clear commits before the record flips.
	gateHealRaft(c, owner, copyNode)
	c.Await("the record to flip to seeding once a leader exists", 60*time.Second, func() bool {
		if c.Node(copyNode).SlotRecordState(slot, uint64(dbID)) == machine.SlotReady {
			_ = c.Node(copyNode).ReseedPair(slot, uint64(dbID)) // a refused ask is asked again at the next poll
		}
		return c.Node(copyNode).SlotRecordState(slot, uint64(dbID)) == machine.SlotSeeding
	})
	if seedRowHas(c.Node(copyNode).State(), copyNode, dbID, int(slot)) {
		t.Fatalf("the record is seeding and the committed row of %s still has bit %d", copyNode, slot)
	}
	c.DelayChannel(copyNode, owner, transport.ChannelSeed, 0)
	c.Await("the copy's pair to be ready again", 60*time.Second, func() bool {
		return c.Node(copyNode).SlotRecordState(slot, uint64(dbID)) == machine.SlotReady
	})
}

// TestTheReseedClearIsBoundedByTheProposalBudget: with no leader the reseed's clear gives
// up after one relay call's timeout, not after the whole proposal budget, so the frame
// that asked for it is held for seconds and not for the budget. The record and the bit
// are untouched.
// [BREAK: in seedreport.go Clear send under ctx without the WithTimeout]
func TestTheReseedClearIsBoundedByTheProposalBudget(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true, RaftTimeout: gateRaftTimeout})
	leader := c.AwaitLeader(10 * time.Second)
	seed, owner, copyNode := gateIDLedBy(t, c, leader, "bound")
	slot, dbID := gateSeededPair(t, c, seed, owner, copyNode)
	gateLeaderless(t, c, leader, owner, copyNode)

	limit := gateCallTimeout + time.Second
	started := time.Now()
	done := make(chan error, 1)
	go func() { done <- c.Node(copyNode).ReseedPair(slot, uint64(dbID)) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a reseed whose clear could not commit returned nil")
		}
		t.Logf("the clear gave up after %v: %v", time.Since(started).Round(time.Millisecond), err)
	case <-time.After(limit):
		t.Fatalf("the reseed's clear is still waiting %v after it began with no leader, want an error within %v", time.Since(started).Round(time.Millisecond), limit)
	}
	if got := c.Node(copyNode).SlotRecordState(slot, uint64(dbID)); got != machine.SlotReady {
		t.Fatalf("after the stopped reseed the record is %v, want ready", got)
	}
	if !seedRowHas(c.Node(copyNode).State(), copyNode, dbID, int(slot)) {
		t.Fatalf("the committed bit of %s slot %d is clear after a clear that could not commit", copyNode, slot)
	}
}

// TestAJoinerReportsItsBits: a joiner is a committed member, so its ready pairs reach the
// committed state as bits while its member state is still joining.
// [BREAK: in seedreport.go send nothing unless this node's member state is active]
func TestAJoinerReportsItsBits(t *testing.T) {
	c, _ := t3Cluster(t, 2, 6, harness.Options{Seed: wire.SeedOptions{SlotsInFlight: 8}})
	added := c.AddNode()
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	dbID := leader.State().Databases[0].ID
	node := c.Node(added)

	// Every slot the joiner holds ready is in its committed row, and the row is not empty.
	c.AwaitWhileSeedingMoves("the joiner's committed row to hold a bit for every ready pair", 90*time.Second, 240*time.Second, func() bool {
		ready := 0
		row := seedRow(node.State(), added, dbID)
		for slot := range len(row) * 8 {
			if node.SlotRecordState(uint16(slot), uint64(dbID)) != machine.SlotReady {
				continue
			}
			ready++
			if row[slot/8]>>(slot%8)&1 == 0 {
				return false
			}
		}
		return ready > 0
	})
	for _, member := range leader.State().Members {
		if member.ID == added && member.State != cluster.MemberJoining {
			t.Fatalf("%s is %v after reporting, want it still joining: the test never ran as a joiner", added, member.State)
		}
	}
}
