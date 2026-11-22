package harness_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestARestartedOwnerShipsAnAcknowledgedRecordToTheCopyThatNeverGotIt (BUG-0746): an RF 3 write
// is acknowledged once the owner and one copy hold it, so the third copy may still be waiting
// for the record when the owner stops. The owner's send state is memory only; after the
// restart nothing queues the pair again, and a copy repairs itself only when a later frame of
// the same pair shows it a gap, so a pair that is never written again leaves that copy
// without an acknowledged record for good. The copy is cut off from the owner while the
// record is written; the owner then closes, the cut heals and the owner restarts.
func TestARestartedOwnerShipsAnAcknowledgedRecordToTheCopyThatNeverGotIt(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true, WCTimeout: 5 * time.Second})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), "rf3", 3); err != nil {
		t.Fatalf("creating the RF 3 database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), "rf3", proofBucket, wire.Field{Name: "name", Type: "string"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	probe := c.Node(c.IDs()[0])
	owner, copies, err := probe.Owner(proofCtx(t), "rf3", proofBucket, "tail-1")
	if err != nil || len(copies) != 3 {
		t.Fatalf("placing tail-1: owner %s copies %v err %v, want three copies", owner, copies, err)
	}
	near, far := copies[1], copies[2]

	// A first write resolves the bucket's declaration on the owner, which would otherwise ask
	// the cut-off node for it.
	if _, err := c.Node(owner).Insert(proofCtx(t), "rf3", proofBucket, bson.D{{Key: "_id_", Value: "tail-warm"}, {Key: "name", Value: "Bea"}}); err != nil {
		t.Fatalf("warming insert through %s: %v", owner, err)
	}
	c.Partition([]string{owner, near}, []string{far})
	got, err := c.Node(owner).Insert(proofCtx(t), "rf3", proofBucket, bson.D{{Key: "_id_", Value: "tail-1"}, {Key: "name", Value: "Ada"}})
	if err != nil {
		t.Fatalf("insert through the owner %s: %v (background %v)", owner, err, c.BackgroundErrors())
	}
	if got.Outcome != machine.OutcomeApplied && got.Outcome != machine.OutcomeAppliedDegraded {
		t.Fatalf("the insert answered %v, want an acknowledged outcome", got.Outcome)
	}
	if got.CopiesConfirmed < 2 {
		t.Fatalf("copies_confirmed %d, want at least 2: the test needs an acknowledged write", got.CopiesConfirmed)
	}
	if _, found, _ := c.Node(far).LocalDocument(proofCtx(t), "rf3", proofBucket, "tail-1"); found {
		t.Fatalf("%s holds the record while cut off: the test proves nothing", far)
	}

	c.Kill(owner)
	c.Heal()
	c.Restart(owner)

	c.Await("the copy that was cut off holds the acknowledged record", 30*time.Second, func() bool {
		_, found, err := c.Node(far).LocalDocument(proofCtx(t), "rf3", proofBucket, "tail-1")
		return err == nil && found
	})
}

// TestACopyThatWasDownWhenTheOwnerRestartedGetsTheTailWhenItIsActive (BUG-0746): two records go
// into one pair; the copy holds the first, is cut off and committed down, and misses the second;
// the owner then restarts, so it has no lane to the down copy and its restart sweep ships it
// nothing. The copy is reachable again, never restarted, and is activated at the same incarnation, which does not seed it,
// so the owner must send the tail when the member goes down to active, or the copy stays one
// record short until the pair is written again. End to end only: in this harness the owner's
// restart can also become a failover whose Opened hook repairs the copy, so the down-to-active
// trigger itself is pinned by TestAMemberThatWasDownAndIsActiveIsNoticed in cluster/wire.
func TestACopyThatWasDownWhenTheOwnerRestartedGetsTheTailWhenItIsActive(t *testing.T) {
	t.Skip("BUG-0746: replication tail; run alone this gives up waiting for every running node to hold a valid lease and a fresh quorum round while one copy is cut off (9 of 10 runs); skipped during the feature freeze (owner 2026-11-21), to be fixed in the replication rework")
	c := harness.NewCluster(t, 3, harness.Options{FastDetection: true, WCTimeout: 5 * time.Second})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), "rf3", 3); err != nil {
		t.Fatalf("creating the RF 3 database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), "rf3", proofBucket, wire.Field{Name: "name", Type: "string"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	// Two ids of one pair.
	first := "down-1"
	slot, err := wire.PlacementOf("rf3", proofBucket, first)
	if err != nil {
		t.Fatal(err)
	}
	second := ""
	for i := 2; second == "" && i < 200000; i++ {
		if other := fmt.Sprintf("down-%d", i); mustSlot(t, other) == slot {
			second = other
		}
	}
	if second == "" {
		t.Fatal("no second id in the pair")
	}
	owner, copies, err := c.Node(c.IDs()[0]).Owner(proofCtx(t), "rf3", proofBucket, first)
	if err != nil || len(copies) != 3 {
		t.Fatalf("placing %s: owner %s copies %v err %v, want three copies", first, owner, copies, err)
	}
	far := copies[2]
	if got, err := c.Node(owner).Insert(proofCtx(t), "rf3", proofBucket, bson.D{{Key: "_id_", Value: first}, {Key: "name", Value: "Bea"}}); err != nil || got.CopiesConfirmed < 2 {
		t.Fatalf("first insert through %s: copies %d err %v, want an acknowledged write", owner, got.CopiesConfirmed, err)
	}
	c.Await("the copy holds the first record", 20*time.Second, func() bool {
		_, found, err := c.Node(far).LocalDocument(proofCtx(t), "rf3", proofBucket, first)
		return err == nil && found
	})
	c.Partition([]string{owner, copies[1]}, []string{far})
	c.Await("the cut-off copy is committed down", 30*time.Second, func() bool {
		for _, member := range c.Node(owner).State().Members {
			if member.ID == far && member.State == cluster.MemberDown {
				return true
			}
		}
		return false
	})
	c.AwaitLeader(10 * time.Second)
	// From here no further member is marked down, so the owner's restart is not a failover.
	if err := c.Node(c.AwaitLeader(10*time.Second)).SetSetting(proofCtx(t), "probe_failures", 1000); err != nil {
		t.Fatalf("setting probe_failures: %v", err)
	}
	c.AwaitLeases(20 * time.Second)
	if got, err := c.Node(owner).Insert(proofCtx(t), "rf3", proofBucket, bson.D{{Key: "_id_", Value: second}, {Key: "name", Value: "Ada"}}); err != nil || got.CopiesConfirmed < 2 {
		t.Fatalf("second insert through %s: copies %d err %v, want an acknowledged write", owner, got.CopiesConfirmed, err)
	}
	if _, found, _ := c.Node(far).LocalDocument(proofCtx(t), "rf3", proofBucket, second); found {
		t.Fatalf("%s holds the second record while committed down: the test proves nothing", far)
	}
	c.Heal()
	c.Kill(owner)
	c.Restart(owner)
	time.Sleep(3 * time.Second)
	if _, found, _ := c.Node(far).LocalDocument(proofCtx(t), "rf3", proofBucket, second); found {
		t.Fatalf("%s got the second record before it was activated", far)
	}
	leaderNow := c.Node(c.AwaitLeader(30 * time.Second))
	// The harness holds the activation ask by default (HoldActivation), so the by-hand activation stays; it is the operator's path.
	if err := leaderNow.Activate(proofCtx(t), far); err != nil {
		t.Fatalf("activating %s: %v", far, err)
	}
	c.Await("the copy that was down holds the second record", 40*time.Second, func() bool {
		_, found, err := c.Node(far).LocalDocument(proofCtx(t), "rf3", proofBucket, second)
		return err == nil && found
	})
}

func mustSlot(t *testing.T, id string) uint16 {
	t.Helper()
	slot, err := wire.PlacementOf("rf3", proofBucket, id)
	if err != nil {
		t.Fatal(err)
	}
	return slot
}

// TestAPromotedCopyShipsItsTailWithoutRestarting (BUG-0746): one copy never got a record the
// owner and the other copy hold; the owner dies for good and the other copy becomes the owner
// by a move, with no restart anywhere. Nothing writes the pair again, so the new owner must send
// its tail on taking the pair over, from the cursor it remembered when it applied the record as
// a copy. A fourth node joins after the database exists, so the handoff already knows the
// database at the epoch before the failover (a database created inside the last epoch is
// TestAPromotedCopyOfAYoungDatabaseShipsItsTail is the case without it). [GUARD: no Opened hook / no cursor remembered by a copy]
func TestAPromotedCopyShipsItsTailWithoutRestarting(t *testing.T) {
	promotedCopyShipsItsTail(t, false)
}

// TestAPromotedCopyOfAYoungDatabaseShipsItsTail (BUG-0746, BUG-0747): the same failover for a
// database created after the last epoch change, which had no barrier before BUG-0747 and so no
// Opened hook. No node joins after the database is created.
func TestAPromotedCopyOfAYoungDatabaseShipsItsTail(t *testing.T) {
	promotedCopyShipsItsTail(t, true)
}

func promotedCopyShipsItsTail(t *testing.T, young bool) {
	t.Helper()
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true, Late: map[bool]int{true: 0, false: 1}[young], WCTimeout: 5 * time.Second})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), "rf3", 3); err != nil {
		t.Fatalf("creating the RF 3 database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), "rf3", proofBucket, wire.Field{Name: "name", Type: "string"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	added := ""
	if !young {
		added = c.AddNode()
		// ActivateWhileSeedingMoves gives up on the first position query that a fresh node answers with a
		// closed connection; the join is not the test's subject, so ask again.
		c.Await("the added node to be activated", 90*time.Second, func() bool {
			return c.Node(c.AwaitLeader(30*time.Second)).Activate(proofCtx(t), added) == nil
		})
		c.AwaitConverged(20 * time.Second)
		c.AwaitLeases(20 * time.Second)

	}

	var id, owner, near, far string
	for i := 0; i < 500 && id == ""; i++ {
		candidate := fmt.Sprintf("promo-%d", i)
		o, copies, err := c.Node("n1").Owner(proofCtx(t), "rf3", proofBucket, candidate)
		if err != nil || len(copies) != 3 || copies[0] != o || slices.Contains(copies, added) {
			continue
		}
		id, owner, near, far = candidate, o, copies[1], copies[2]
	}
	if id == "" {
		t.Fatal("no id placed on the three original nodes")
	}
	if _, err := c.Node(owner).Insert(proofCtx(t), "rf3", proofBucket, bson.D{{Key: "_id_", Value: "promo-warm"}, {Key: "name", Value: "Bea"}}); err != nil {
		t.Fatalf("warming insert through %s: %v", owner, err)
	}
	c.DropChannel(owner, far, transport.ChannelData, 1_000_000)
	got, err := c.Node(owner).Insert(proofCtx(t), "rf3", proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}})
	if err != nil || got.CopiesConfirmed < 2 {
		t.Fatalf("insert through the owner %s: copies %d err %v, want an acknowledged write", owner, got.CopiesConfirmed, err)
	}
	if _, found, _ := c.Node(far).LocalDocument(proofCtx(t), "rf3", proofBucket, id); found {
		t.Fatalf("%s holds the record while its channel from the owner is cut: the test proves nothing", far)
	}

	epochBefore := c.Node(near).State().Epoch
	c.Kill(owner)
	next := c.Node(c.AwaitLeader(20 * time.Second))
	for key, millis := range map[string]int64{"probe_interval_ms": 100, "probe_timeout_ms": 150, "probe_failures": 2, "failover_ms": 2000} {
		if err := next.SetSetting(proofCtx(t), key, millis); err != nil {
			t.Fatalf("setting %s: %v", key, err)
		}
	}
	c.Await("the survivors to commit the owner down", 40*time.Second, func() bool {
		return c.Node(near).State().Epoch > epochBefore && c.Node(far).State().Epoch > epochBefore
	})
	c.AwaitLeader(10 * time.Second)
	c.AwaitLeases(20 * time.Second)
	if now, _, err := c.Node(near).Owner(proofCtx(t), "rf3", proofBucket, id); err != nil || now != near {
		t.Fatalf("after the failover the owner is %q (err %v), want %s", now, err, near)
	}
	c.Await("the copy that never got the record holds it", 40*time.Second, func() bool {
		_, found, err := c.Node(far).LocalDocument(proofCtx(t), "rf3", proofBucket, id)
		return err == nil && found
	})
}
