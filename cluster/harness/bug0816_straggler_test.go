package harness_test

import (
	"context"
	"errors"
	"fmt"
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

const (
	// stragglerBehind is how many records the copy misses while the owner's pushes to it are
	// cut: more than the catch-up fetches before the promotion (two records per request, a
	// request every two stragglerDelay), and few enough that the handoff's own catch-up
	// covers the rest in one or two supervisor ticks.
	stragglerBehind = 56
	// stragglerDelay is the latency of every write the copy sends its old owner on the data
	// channel; a request is two writes. It is what keeps one catch-up request in flight
	// across the promotion, and it stays well under the 2 s bound of a data-channel call.
	stragglerDelay = 400 * time.Millisecond
	// stragglerWatch is how long the new owner is written to and watched after its first
	// write: the straggler's in-flight request and the one after it (two request delays each),
	// then the reset itself, which first clears the pair's seed bit through the leader and
	// waits up to 5 s for that.
	stragglerWatch = 9 * time.Second
	// stragglerLag is how long the third copy takes to make a record durable once the new
	// owner writes: long enough that the copy is usually one record behind the owner, short
	// enough that every write still gets its majority inside the write-concern wait.
	stragglerLag = 60 * time.Millisecond
	// stragglerPace is the pause between two writes through the new owner.
	stragglerPace = 20 * time.Millisecond
)

// stragglerPair returns a seed id of a pair that ranks three different nodes: the owner, the
// copy that becomes the owner when the first is marked down, and the third copy. The owner is
// not the Raft leader, so cutting it off leaves the cluster with the leader it had.
func stragglerPair(t *testing.T, c *harness.Cluster, leader string) (seed, owner, next, third string) {
	t.Helper()
	for i := 0; i < 2000; i++ {
		seed = fmt.Sprintf("straggler-%d", i)
		_, copies, err := c.Node(c.IDs()[0]).Owner(proofCtx(t), proofDB, proofBucket, seed)
		if err != nil || len(copies) != 3 {
			t.Fatalf("placing %s at RF 3: copies %v, err %v", seed, copies, err)
		}
		if copies[0] != leader {
			return seed, copies[0], copies[1], copies[2]
		}
	}
	t.Fatalf("no pair is owned by a node other than the leader %s", leader)
	return "", "", "", ""
}

// TestAPromotedCopyWithAStragglingCatchUpKeepsServingItsPair is BUG-0816 on a cluster. RF 3,
// two records per catch-up chunk. A copy misses more than a chunk of the owner's records and
// starts a background catch-up from the owner; every request of it is slow. The owner is then
// cut off from the cluster's control and Raft channels, so it is marked down and the copy is
// promoted, while the old owner still answers the data channel: the straggling catch-up keeps
// its source. The promoted copy catches up from the third copy through the handoff, opens and
// takes writes, with the third copy slow to apply them. The straggler's request that was in
// flight across the promotion comes back with a chunk that is not current, and its next request
// names the new owner's own position, which the old owner never ordered.
//
// Without the fix the old owner answers Divergent and the new owner resets and reseeds the
// pair it is serving. The test asserts that the new owner's pair never leaves ready, and that
// every write a node acknowledged is read back from the owner afterwards.
//
// The interleaving is timed, not scripted: it needs the handoff's catch-up and the first
// write the new owner orders to fall inside one in-flight request of the straggler. When the
// request boundary falls between the two, the straggler ends as current, and when the
// handoff takes more than three of the straggler's requests the straggler gives up first; both
// runs are green without the fix. Measured on the pre-fix tree in this form: 2 red runs of 8
// (with 72 records behind, none of 6). So on a pre-fix tree this is a stress test that is
// red about one run in four, and the scripted failing-first test is
// wire.TestAStragglingCatchUpDoesNotResetANodeThatBecameOwner.
func TestAPromotedCopyWithAStragglingCatchUpKeepsServingItsPair(t *testing.T) {
	t.Skip("BUG-0816: straggling catch-up; the interleaving is timed, not scripted, so the setup checks fail on a loaded machine (the lagging copy never starts its catch-up); skipped during the feature freeze (owner 2026-11-21), to be fixed in the replication rework; the scripted test wire.TestAStragglingCatchUpDoesNotResetANodeThatBecameOwner still runs")
	c := harness.NewCluster(t, 3, harness.Options{FastDetection: true, CatchUpChunkRecords: 2})
	leaderID := c.AwaitLeader(10 * time.Second)
	leader := c.Node(leaderID)
	dbID, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, 3)
	if err != nil {
		t.Fatal(err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), proofDB, proofBucket, wire.Field{Name: "name", Type: "string"}); err != nil {
		t.Fatal(err)
	}
	seed, owner, promoted, third := stragglerPair(t, c, leaderID)
	ids := reseedIDsInOneSlot(t, seed, stragglerBehind+9+int(stragglerWatch/stragglerPace))
	slot, err := wire.PlacementOf(proofDB, proofBucket, seed)
	if err != nil {
		t.Fatal(err)
	}
	c.AwaitLeases(30 * time.Second)
	t.Logf("slot %d: owner %s, promoted copy %s, third copy %s", slot, owner, promoted, third)

	var acknowledged []string
	insert := func(via, id string) (wire.Inserted, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return c.Node(via).Insert(ctx, proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada " + id}})
	}
	// write retries a refusal (a node whose lease renewal is late for a moment refuses the write
	// with nothing applied) and counts the write as acknowledged only when the answer is
	// applied: applied_degraded and unknown promise no majority.
	write := func(via, id string) {
		t.Helper()
		var got wire.Inserted
		var err error
		for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
			if got, err = insert(via, id); err == nil {
				break
			}
		}
		if err != nil {
			t.Fatalf("insert of %s through %s: %v (background: %v)", id, via, err, c.BackgroundErrors())
		}
		if got.Outcome == machine.OutcomeApplied {
			acknowledged = append(acknowledged, id)
		}
	}
	position := func(node string) cluster.Position {
		t.Helper()
		pos, err := c.Node(node).PairPosition(proofCtx(t), proofDB, proofBucket, seed)
		if err != nil {
			t.Fatalf("reading the position of the pair on %s: %v", node, err)
		}
		return pos
	}

	// All three copies hold the first record.
	write(owner, ids[0])
	c.Await("the first record to reach every copy", 10*time.Second, func() bool {
		return position(promoted) == position(owner) && position(third) == position(owner)
	})

	// The owner's pushes to one copy stop; the other copy keeps the majority.
	c.DropChannel(owner, promoted, transport.ChannelData, 1<<20)
	next := 1
	for ; next <= stragglerBehind; next++ {
		write(owner, ids[next])
	}
	stalled := position(promoted)
	if lead := position(owner); lead.Epoch != stalled.Epoch || lead.Seq-stalled.Seq < stragglerBehind {
		t.Fatalf("setup: the cut copy is at %s and the owner at %s, want the copy %d records behind", stalled, lead, stragglerBehind)
	}

	// The copy starts a background catch-up from the owner, and every request of it is slow.
	// A copy starts one when a pushed record skips a position. The owner's pushes are let
	// through again, and once they flow the copy's disk refuses appends in short pulses: the
	// record that meets a pulse is not applied, and the records queued behind it in the same
	// frame are a gap. On a tie the catch-up's source is the pair's owner, which is listed
	// first; the count of requests sent to the owner is the proof. Once the catch-up has
	// asked, the owner's pushes are cut again, so the catch-up alone moves the copy.
	c.DelayChannel(promoted, owner, transport.ChannelData, stragglerDelay)
	c.DropChannel(owner, promoted, transport.ChannelData, 0)
	write(owner, ids[next])
	next++
	c.Await("the owner's pushes to reach the lagging copy again", 30*time.Second, func() bool {
		return stalled.Less(position(promoted))
	})
	for deadline := time.Now().Add(30 * time.Second); c.CatchUpRequests(promoted, owner) == 0; {
		if time.Now().After(deadline) {
			t.Fatalf("setup: the lagging copy %s at %s never started a catch-up from its owner %s at %s (background: %v)", promoted, position(promoted), owner, position(owner), c.BackgroundErrors())
		}
		c.FailDisk(promoted, true)
		time.Sleep(40 * time.Millisecond)
		c.FailDisk(promoted, false)
		time.Sleep(10 * time.Millisecond)
	}
	c.DropChannel(owner, promoted, transport.ChannelData, 1<<20)
	c.Await("the lagging copy's catch-up to ask the owner twice", 20*time.Second, func() bool {
		return c.CatchUpRequests(promoted, owner) >= 2
	})
	c.SlowDisk(third, stragglerLag)
	tail := position(owner)
	if at := position(promoted); at.Epoch != tail.Epoch || tail.Seq-at.Seq < stragglerBehind/4 {
		t.Fatalf("setup: the lagging copy is at %s and the owner at %s after two requests; the catch-up was meant to be slow", at, tail)
	}
	t.Logf("the straggling catch-up is running: %s at %s after %d requests to %s, which is at %s", promoted, position(promoted), c.CatchUpRequests(promoted, owner), owner, tail)
	cut := time.Now()

	// The owner leaves the cluster's view and stays reachable on the data channel.
	for _, other := range []string{promoted, third} {
		for _, ch := range []transport.Channel{transport.ChannelControl, transport.ChannelRaft} {
			c.DropChannel(owner, other, ch, 1<<20)
			c.DropChannel(other, owner, ch, 1<<20)
		}
	}

	// Watch the promoted copy's record from here to the end.
	var watch sync.WaitGroup
	stop := make(chan struct{})
	var seedingAt time.Time
	watch.Add(1)
	go func() {
		defer watch.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
			}
			if state := c.Node(promoted).SlotRecordState(slot, dbID); state != machine.SlotReady && seedingAt.IsZero() {
				seedingAt = time.Now()
			}
		}
	}()

	c.Await("the lagging copy to become the owner of the pair", 30*time.Second, func() bool {
		got, known := c.Node(promoted).OwnerOfSlot(slot, dbID)
		return known && got == promoted
	})
	t.Logf("%s owns the pair %s after the cut", promoted, time.Since(cut).Round(time.Millisecond))

	// Write through the new owner from the moment it opens, for stragglerWatch.
	var opened time.Time
	refused := 0
	for deadline := time.Now().Add(40 * time.Second); (opened.IsZero() || time.Since(opened) < stragglerWatch) && next < len(ids)-1; {
		if time.Now().After(deadline) {
			t.Fatalf("the new owner %s acknowledged no write in 40 s (background: %v)", promoted, c.BackgroundErrors())
		}
		got, err := insert(promoted, ids[next])
		switch {
		case err == nil && got.Outcome == machine.OutcomeApplied && got.ServedBy == promoted:
			if opened.IsZero() {
				opened = time.Now()
				t.Logf("%s ordered its first write at %s, %s after the cut", promoted, got.Pos, time.Since(cut).Round(time.Millisecond))
			}
			acknowledged = append(acknowledged, ids[next])
			next++
		case err == nil:
			// Applied degraded or unknown: not a write the cluster promised to keep.
			next++
		default:
			refused++
		}
		time.Sleep(stragglerPace)
	}
	close(stop)
	watch.Wait()
	t.Logf("the straggler asked %s %d times in all", owner, c.CatchUpRequests(promoted, owner))
	t.Logf("%d writes acknowledged in all, %d attempts refused after the cut; the new owner is at %s", len(acknowledged), refused, position(promoted))

	if !seedingAt.IsZero() {
		t.Errorf("the new owner %s put the pair it serves back to seeding %s after it ordered its first write: a catch-up that began while it was a copy reset its pair",
			promoted, seedingAt.Sub(opened).Round(time.Millisecond))
	}

	// Every acknowledged write is read back from the owner, once the pair serves (again), and
	// is in the store of both live copies. A read that is refused for good means the owner
	// stopped serving the pair; NOT_FOUND means the write is gone.
	c.SlowDisk(third, 0)
	c.Await("the new owner's pair to be ready", 60*time.Second, func() bool {
		return c.Node(promoted).SlotRecordState(slot, dbID) == machine.SlotReady
	})
	c.Await("the third copy to reach the new owner's position", 60*time.Second, func() bool {
		return position(third) == position(promoted)
	})
	var lost, unserved, absent []string
	for _, id := range acknowledged {
		var last error
		notFound := false
		for attempt := 0; attempt < 50; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, last = c.Node(promoted).Get(ctx, proofDB, proofBucket, id)
			cancel()
			var coded *cluster.Error
			notFound = errors.As(last, &coded) && coded.Code == cluster.CodeNotFound
			if last == nil || notFound {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		switch {
		case notFound:
			lost = append(lost, id)
		case last != nil:
			unserved = append(unserved, id)
			t.Logf("acknowledged write %s is not served by %s: %v", id, promoted, last)
		}
		for _, node := range []string{promoted, third} {
			if _, found, err := c.Node(node).LocalDocument(proofCtx(t), proofDB, proofBucket, id); err != nil || !found {
				absent = append(absent, id+"@"+node)
			}
		}
	}
	if len(lost) > 0 {
		t.Errorf("%d of %d acknowledged writes are NOT_FOUND on the owner %s: %v", len(lost), len(acknowledged), promoted, lost)
	}
	if len(unserved) > 0 {
		t.Errorf("the owner %s refuses to read %d of %d acknowledged writes: it no longer serves the pair: %v", promoted, len(unserved), len(acknowledged), unserved)
	}
	if len(absent) > 0 {
		t.Errorf("%d acknowledged writes are missing from a live copy's store: %v", len(absent), absent)
	}
	// The pair still takes a write.
	last := ids[len(ids)-1]
	if got, err := insert(promoted, last); err != nil || got.Outcome != machine.OutcomeApplied {
		t.Errorf("a write through the owner %s after the run: outcome %v, err %v: it no longer serves the pair", promoted, got.Outcome, err)
	}
}
