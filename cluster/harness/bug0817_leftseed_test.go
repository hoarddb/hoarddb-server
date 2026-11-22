package harness_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// leftSeedInsert inserts id through node, trying again while the cluster refuses for a
// reason that passes on its own (a lapsed lease, a seeding copy): the formation and the RF
// changes are the setting of the test, not what it asserts.
func leftSeedInsert(t *testing.T, c *harness.Cluster, node, id string) wire.Inserted {
	t.Helper()
	var last error
	for range 40 {
		got, err := c.Node(node).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada " + id}})
		if err == nil {
			return got
		}
		last = err
		if harness.IsTransientRefusal(err) {
			c.AwaitLeases(30 * time.Second)
			continue
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("insert of %s through %s: %v", id, node, last)
	return wire.Inserted{}
}

// TestACopySetThatLeavesAndReturnsDuringASeedKeepsEveryDocument (BUG-0817) is the bug's
// sequence on a cluster: three active members, one pair holding every document, RF 1 to 2
// with the new copy's pull held part-way by the seed byte rate, RF 2 to 1 (the new copy
// drops its partial seed at a low reclaim rate, so the delete takes seconds), RF 1 to 2
// while that delete runs, and one more write. Before the fix the returning copy admitted
// the write over the range under the delete, caught up from the pair's first record, and
// the delete then removed what the catch-up had rewritten: a ready copy with acknowledged
// documents missing. The test waits until a write is confirmed by both copies and compares
// the copy's documents with the owner's.
func TestACopySetThatLeavesAndReturnsDuringASeedKeepsEveryDocument(t *testing.T) {
	const documents = 2000
	c := harness.NewCluster(t, 3, harness.Options{
		NoDetection: true, ReclaimKeysPerSecond: 400,
		Seed: wire.SeedOptions{SlotsInFlight: 8, ChunkEntries: 128, BytesPerSecond: 48 << 10},
	})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	dbID, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, 1)
	if err != nil {
		t.Fatalf("creating the RF=1 database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	var bucketErr error
	for range 20 {
		c.AwaitLeases(30 * time.Second)
		if _, bucketErr = leader.CreateBucket(proofCtx(t), proofDB, proofBucket, wire.Field{Name: "name", Type: "string"}); !harness.IsTransientRefusal(bucketErr) {
			break
		}
	}
	if bucketErr != nil {
		t.Fatalf("creating the bucket: %v", bucketErr)
	}
	c.AwaitLeases(30 * time.Second)

	ids := reseedIDsInOneSlot(t, "left-seed", documents+40)
	extra, ids := ids[documents:], ids[:documents]
	slot, err := wire.PlacementOf(proofDB, proofBucket, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	owner, _, err := leader.Owner(proofCtx(t), proofDB, proofBucket, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		leftSeedInsert(t, c, owner, id)
	}

	alter := func(rf int) {
		t.Helper()
		var last error
		for range 20 {
			if last = c.Node(c.AwaitLeader(10*time.Second)).AlterDatabaseRF(proofCtx(t), proofDB, rf); last == nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("setting RF %d: %v", rf, last)
	}
	alter(2)
	second := ""
	c.Await("the owner to see the pair's second copy at RF 2", 30*time.Second, func() bool {
		_, copies, err := c.Node(owner).Owner(proofCtx(t), proofDB, proofBucket, ids[0])
		if err != nil {
			return false
		}
		for _, id := range copies {
			if id != owner {
				second = id
			}
		}
		return second != ""
	})
	// The new copy's pull of the pair is part-way: more than two delete batches of keys are in
	// its range, and the pair is still seeding.
	c.Await("the new copy to have pulled part of the pair", 8*time.Minute, func() bool {
		status, pulling := c.Node(second).SeedStatus(slot, dbID)
		return pulling && status.Entries >= 2600
	})
	if state := c.Node(second).SlotRecordState(slot, dbID); state != machine.SlotSeeding {
		t.Fatalf("the new copy's pair is %v with part of its range pulled, want seeding: the pull was not held", state)
	}

	alter(1)
	time.Sleep(300 * time.Millisecond) // the copy's pass sees it out of the copy set and starts the drop
	alter(2)
	// One write right away: before the fix this is the record the returning copy admitted.
	leftSeedInsert(t, c, owner, extra[0])

	confirmed := false
	for _, id := range extra[1:] {
		if got := leftSeedInsert(t, c, owner, id); got.Outcome == machine.OutcomeApplied && got.CopiesConfirmed == 2 {
			confirmed = true
			break
		}
		time.Sleep(5 * time.Second)
	}
	if !confirmed {
		status, _ := c.Node(second).SeedStatus(slot, dbID)
		t.Fatalf("no write was confirmed by both copies after the copy set returned; the copy's pair is %v, pull status %+v", c.Node(second).SlotRecordState(slot, dbID), status)
	}
	if state := c.Node(second).SlotRecordState(slot, dbID); state != machine.SlotReady {
		t.Fatalf("the copy confirmed a write while its pair is %v", state)
	}

	missing, differing := 0, 0
	for _, id := range append(append([]string(nil), ids...), extra[0]) {
		want, held, err := c.Node(owner).LocalDocument(proofCtx(t), proofDB, proofBucket, id)
		if err != nil || !held {
			t.Fatalf("the owner %s does not hold %s (held %v, err %v)", owner, id, held, err)
		}
		got, held, err := c.Node(second).LocalDocument(proofCtx(t), proofDB, proofBucket, id)
		switch {
		case err != nil:
			t.Fatalf("reading %s on the copy %s: %v", id, second, err)
		case !held:
			missing++
		case !reflect.DeepEqual(got.Fields, want.Fields):
			differing++
		}
	}
	if missing != 0 || differing != 0 {
		t.Fatalf("the copy %s is ready and confirms writes, and of the %d documents the owner acknowledged it is missing %d and holds %d that differ", second, len(ids)+1, missing, differing)
	}
	// The copy dropped thousands of left seeds beside its reconcile passes; none of those drops
	// may have failed (a drop that meets a pass's batch waits, it does not report).
	for _, reported := range c.BackgroundErrors() {
		if strings.Contains(reported, "left seed") {
			t.Fatalf("a left seed's drop reported an error: %s", reported)
		}
	}
}
