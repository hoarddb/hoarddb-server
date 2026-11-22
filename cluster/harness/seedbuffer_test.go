package harness_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestWritesDuringASeedLandOnTheNewCopy: a write the owner acknowledges for a pair the
// fourth node is still seeding is kept for the drain and applied once the pull ends, so the
// new copy holds every acknowledged write and the next write on the pair answers applied with
// both copies. [BREAK: do not set ReplicatorConfig.Buffer]
func TestWritesDuringASeedLandOnTheNewCopy(t *testing.T) {
	c, ids := t3Cluster(t, 2, 2, harness.Options{Seed: wire.SeedOptions{SlotsInFlight: 16, ChunkEntries: 1}})
	// Slow every seed call to the new node so its pairs stay queued while the writes arrive;
	// the delay is cleared after them so the rest of the seed does not hold up the test.
	sources := []string{"n1", "n2", "n3"}
	for _, source := range sources {
		c.DelayChannel("n4", source, transport.ChannelSeed, 2*time.Millisecond)
	}
	added := c.AddNode()
	dbID := uint64(c.Node("n1").State().Databases[0].ID)
	slot, owner := queuedSeedPair(t, c, added, dbID)
	targets := idsInSlot(t, slot, 3)
	var acked []string
	for _, id := range targets {
		if state := c.Node(added).SlotRecordState(slot, dbID); state != machine.SlotSeeding {
			t.Fatalf("the pair (%d, %d) became %v before the write: the test needed it seeding", slot, dbID, state)
		}
		insertWhileSeeding(t, c, owner, id)
		acked = append(acked, id)
	}
	for _, source := range sources {
		c.DelayChannel("n4", source, transport.ChannelSeed, 0)
	}
	// Wait for this pair, not the whole node: the writes are on the pair's seed, and the rest
	// of the node's pulls are still running as it becomes ready.
	c.AwaitWhileSeedingMoves(fmt.Sprintf("pair (%d, %d) to finish seeding on %s", slot, dbID, added), 90*time.Second, 240*time.Second, func() bool {
		return c.Node(added).SlotRecordState(slot, dbID) == machine.SlotReady
	})
	for _, id := range append(append([]string{}, ids...), acked...) {
		ownerOf, copies, err := c.Node("n1").Owner(proofCtx(t), proofDB, proofBucket, id)
		if err != nil {
			t.Fatal(err)
		}
		held := false
		for _, copyOf := range copies {
			held = held || copyOf == added
		}
		if !held {
			continue
		}
		if _, found, err := c.Node(added).LocalDocument(proofCtx(t), proofDB, proofBucket, id); err != nil || !found {
			t.Errorf("%s was acknowledged while the pair seeded, but the new copy of %s does not hold it: found %v err %v", id, ownerOf, found, err)
			continue
		}
		served, err := c.Node(added).Get(proofCtx(t), proofDB, proofBucket, id)
		if err != nil {
			t.Errorf("a read of %s through the new copy after it was seeded: %v", id, err)
		} else if !bytes.Contains(served.Body, []byte(id)) {
			t.Errorf("a read of %s through the new copy answered a body without the id", id)
		}
	}
	// The pair is ready now: the next write must apply on both copies, not be answered
	// SEQ_GAP because the seed dropped the writes that arrived while it ran.
	next := idsInSlot(t, slot, len(targets)+1)[len(targets)]
	if state := c.Node(added).SlotRecordState(slot, dbID); state != machine.SlotReady {
		t.Fatalf("the pair (%d, %d) is %v before the first write after ready", slot, dbID, state)
	}
	got, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: next}, {Key: "name", Value: "Ada " + next}})
	if err != nil {
		t.Fatalf("the first write after the new copy became ready: %v (a SEQ_GAP here means the seed dropped a live write)", err)
	}
	if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed != 2 {
		t.Fatalf("the first write after the new copy became ready answered %v with %d copies, want applied with 2: a SEQ_GAP means the seed dropped a live write", got.Outcome, got.CopiesConfirmed)
	}
}

// queuedSeedPair returns a pair the added node holds a seeding record for and whose pull has
// not started, so a write sent now reaches it before its snapshot ends.
func queuedSeedPair(t *testing.T, c *harness.Cluster, added string, dbID uint64) (uint16, string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		for slot := range uint16(storage.SlotCount) {
			if c.Node(added).SlotRecordState(slot, dbID) != machine.SlotSeeding {
				continue
			}
			status, ok := c.Node(added).SeedStatus(slot, dbID)
			if !ok || status.State != wire.PullQueued {
				continue
			}
			if owner, ok := c.Node(added).OwnerOfSlot(slot, dbID); ok && owner != added {
				return slot, owner
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no pair is queued for the added node to seed: the test cannot put a write in its buffer")
	return 0, ""
}

// idsInSlot returns want document ids whose placement is slot, so a write of each lands on
// the same pair.
func idsInSlot(t *testing.T, slot uint16, want int) []string {
	t.Helper()
	ids := make([]string, 0, want)
	for i := 0; len(ids) < want && i < 500000; i++ {
		id := fmt.Sprintf("seed-%04d-%06d", slot, i)
		got, err := wire.PlacementOf(proofDB, proofBucket, id)
		if err != nil {
			t.Fatalf("placing %s: %v", id, err)
		}
		if got == slot {
			ids = append(ids, id)
		}
	}
	if len(ids) < want {
		t.Fatalf("found %d ids in slot %d, want %d", len(ids), slot, want)
	}
	return ids
}

// insertWhileSeeding inserts id through node, retrying the lease lapse a membership change
// can cause under load: the write itself is what the test is about.
func insertWhileSeeding(t *testing.T, c *harness.Cluster, node, id string) {
	t.Helper()
	var last error
	for range 40 {
		_, last = c.Node(node).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada " + id}})
		if last == nil {
			return
		}
		if !harness.IsTransientRefusal(last) && !strings.Contains(last.Error(), "held") {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("the write of %s through %s while the new copy seeded: %v", id, node, last)
}
