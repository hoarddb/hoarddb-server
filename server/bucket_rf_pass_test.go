package server

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// An alter and a membership change at one epoch (bucket-replication-rev8-spec
// §6.4 S1, rule F4; G3 build doc step 4). A member's placement pass reads the
// bucket's factor once, and the alter may reach the member before or after
// that read, so the pass may compare the rings at either factor. Whichever it
// reads, the alter's own batch marks the copies that missed entries, and a
// mark is never cleared by a later pass. One order did lose one: a pass that
// read a bucket's marks before the alter marked the copy, and committed after
// it, wrote its older merge over the mark (BUG-0589, fixed:
// TestAMarkRecordedWhileAPassIsBetweenItsReadAndItsCommitSurvives).

// adoptionRow is a bucket of the test database whose set a joiner's adoption
// changes as one row of §6.4's table needs, its owner, which keeps it, and the
// member the row is about.
type adoptionRow struct {
	bucket        string
	owner, member int
}

// adoptionRows lists, for each owner, the first bucket name under prefix
// whose walk at 3 across the join is accepted by fits, given the walk before
// and the walk after, owner first, and names the member the row is about. A
// ring walks the members after an owner in one fixed order, so where the
// joiner lands in a walk depends on the owner alone.
func adoptionRows(t *testing.T, tc *testCluster, joiner, prefix string, fits func(before, after []string) (member string, ok bool)) map[int]adoptionRow {
	t.Helper()
	before := ringWith(t, 3, tc.addrs...)
	after := ringWith(t, 3, append(append([]string(nil), tc.addrs...), joiner)...)
	rows := make(map[int]adoptionRow)
	for attempt := 0; attempt < 4000 && len(rows) < len(tc.addrs); attempt++ {
		name := fmt.Sprintf("%s%04d", prefix, attempt)
		key := placementOf(testDatabase, name).encoded
		oldSet, err := before.Replicas(key, 3)
		if err != nil {
			t.Fatalf("placing %s before the join: %v", name, err)
		}
		newSet, err := after.Replicas(key, 3)
		if err != nil {
			t.Fatalf("placing %s after the join: %v", name, err)
		}
		owner := nodeIndexByAddr(t, tc.addrs, oldSet[0].ID)
		if _, found := rows[owner]; found || oldSet[0].ID != newSet[0].ID {
			continue
		}
		if member, ok := fits(nodeIDs(oldSet), nodeIDs(newSet)); ok {
			rows[owner] = adoptionRow{bucket: name, owner: owner, member: nodeIndexByAddr(t, tc.addrs, member)}
		}
	}
	return rows
}

// leftRows are buckets at 2 whose second holder the joiner displaces to
// third: the member leaves the set at 2 at the adoption and stays in the set
// at 3, so an increase to 3 at the adoption's epoch places it again on a copy
// that missed the entries logged since.
func leftRows(t *testing.T, tc *testCluster, joiner string) map[int]adoptionRow {
	t.Helper()
	return adoptionRows(t, tc, joiner, "left", func(before, after []string) (string, bool) {
		return before[1], after[1] == joiner && after[2] == before[1]
	})
}

// leftAtTheAdoption is one of leftRows, whichever owner it has.
func leftAtTheAdoption(t *testing.T, tc *testCluster, joiner string) adoptionRow {
	t.Helper()
	for _, row := range leftRows(t, tc, joiner) {
		return row
	}
	t.Fatalf("no bucket name is displaced from its set at 2 by %s's join", joiner)
	return adoptionRow{}
}

// demotedBesideTheAdoption is a bucket at 3 whose set the join leaves alone:
// a decrease to 2 at the adoption's epoch demotes its third holder, whose
// pass sees no change at either factor.
func demotedBesideTheAdoption(t *testing.T, tc *testCluster, joiner string) adoptionRow {
	t.Helper()
	rows := adoptionRows(t, tc, joiner, "demoted", func(before, after []string) (string, bool) {
		return after[2], slices.Equal(before, after)
	})
	for _, row := range rows {
		return row
	}
	t.Fatalf("no bucket name keeps its set at 3 across %s's join", joiner)
	return adoptionRow{}
}

// holdSweepsOn holds every sweep and rearm on the nodes named, so a copy that
// left its set at the join is not released before the alter reaches it.
func holdSweepsOn(t *testing.T, ids ...string) (release func()) {
	t.Helper()
	released := make(chan struct{})
	hook := func(nodeID string, _ uint64, stage sweepStage) {
		if (stage == sweepStarting || stage == rearmPassing) && slices.Contains(ids, nodeID) {
			<-released
		}
	}
	sweepStageHook.Store(&hook)
	var once sync.Once
	release = func() { once.Do(func() { close(released) }) }
	t.Cleanup(func() {
		release()
		sweepStageHook.Store(nil)
	})
	return release
}

// holdPassReads holds the placement pass on each named node where it has read
// the named bucket's factor and not yet computed its marks, and closes the
// node's channel in held as it does.
func holdPassReads(t *testing.T, holds map[string]string) (held map[string]chan struct{}, release func()) {
	t.Helper()
	held = make(map[string]chan struct{}, len(holds))
	for nodeID := range holds {
		held[nodeID] = make(chan struct{})
	}
	released := make(chan struct{})
	var mu sync.Mutex
	reached := make(map[string]bool)
	hook := func(nodeID string, key placementKey, _ int) {
		bucket, wanted := holds[nodeID]
		if !wanted || key != placementOf(testDatabase, bucket) {
			return
		}
		mu.Lock()
		first := !reached[nodeID]
		reached[nodeID] = true
		mu.Unlock()
		if first {
			close(held[nodeID])
			<-released
		}
	}
	placementFactorReadHook.Store(&hook)
	var once sync.Once
	release = func() { once.Do(func() { close(released) }) }
	t.Cleanup(func() {
		release()
		placementFactorReadHook.Store(nil)
	})
	return held, release
}

// joinFreezeOff lifts the join's pause of schema changes for the rest of the
// test: a member whose sweep is held never reports the join settled.
func joinFreezeOff(t *testing.T) {
	t.Helper()
	joinFreezeOffHook.Store(true)
	t.Cleanup(func() { joinFreezeOffHook.Store(false) })
}

// joinFifth adds the node ports reserves to tc through node 1 and waits until
// every member and the joiner have adopted the join.
func joinFifth(t *testing.T, tc *testCluster, ports *testNodePorts) *testNode {
	t.Helper()
	via := clusterOf(tc, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := via.proposeJoin(ctx, ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	joiner := startJoinTarget(t, tc, ports, tc.token)
	epoch := via.Epoch()
	pollUntil(t, "every node adopts the join", func() bool {
		for index := range tc.nodes {
			if clusterOf(tc, index).Epoch() < epoch {
				return false
			}
		}
		return joiner.server.core.cluster.Epoch() >= epoch
	})
	return joiner
}

// waitPassFinished waits until node index's placement pass for its epoch has
// finished.
func waitPassFinished(t *testing.T, tc *testCluster, index int) {
	t.Helper()
	pollUntil(t, fmt.Sprintf("node %d's placement pass finishes", index+1), clusterOf(tc, index).placementPassFinished)
}

// TestAPassAndAnAlterInEitherOrderMarkEveryCopyThatMissedEntries is T-154.
// A joiner's adoption at epoch E and an alter logged at E reach a member in
// either order against its pass: the alter applied while the pass is held
// between its factor read and its marks, or after the pass has finished. Two
// rows of §6.4's table, each on its own cluster: a copy that left the set at
// the adoption, which an increase to 3 places again, and a copy a decrease at
// E demotes. Each ends marked outside. The third row, a stale copy an
// increase re-places, is marked by the alter alone whatever the pass reads
// (the pass sees it outside both sets): TestARepromotedNodeIsReseededNotResurrected's
// cases 1 and 4.
func TestAPassAndAnAlterInEitherOrderMarkEveryCopyThatMissedEntries(t *testing.T) {
	rows := []struct {
		name    string
		find    func(t *testing.T, tc *testCluster, joiner string) adoptionRow
		created int
		altered int32
	}{
		{name: "the copy that left the set at the adoption", find: leftAtTheAdoption, created: 2, altered: 3},
		{name: "the copy the decrease demotes", find: demotedBesideTheAdoption, created: 3, altered: 2},
	}
	for _, row := range rows {
		for _, held := range []bool{true, false} {
			order := "the alter after the pass finished"
			if held {
				order = "the alter while the pass is held after its factor read"
			}
			t.Run(row.name+", "+order, func(t *testing.T) {
				// The take-over would copy a re-placed copy afresh, clearing
				// its mark, before the mark could be read.
				takeOverOff(t)
				joinFreezeOff(t)
				tc := newTestCluster(t, 3, 1)
				defer tc.stop()
				ports := reserveNodePorts(t)
				placed := row.find(t, tc, ports.ringNode().ID)
				tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
				waitForAlterFeature(t, tc)
				createBucketAt(tc, placed.owner, testDatabase, placed.bucket, row.created)
				waitForServing(t, tc, testDatabase, placed.bucket, row.created)

				releaseSweeps := holdSweepsOn(t, tc.addrs[placed.member])
				defer releaseSweeps()
				var reached map[string]chan struct{}
				releasePass := func() {}
				if held {
					reached, releasePass = holdPassReads(t, map[string]string{tc.addrs[placed.member]: placed.bucket})
				}
				defer releasePass()
				joiner := joinFifth(t, tc, ports)
				defer func() { _ = joiner.client.Close() }()
				if held {
					awaitHeld(t, reached[tc.addrs[placed.member]], fmt.Sprintf("node %d's placement pass", placed.member+1))
				} else {
					waitPassFinished(t, tc, placed.member)
				}

				waitPassFinished(t, tc, placed.owner)
				alterWithin(t, tc, placed.owner, AlterBucketRequest{Database: testDatabase, Name: placed.bucket, ReplicationFactor: row.altered})
				// The entry reaches the demoted copy on the owner's ship, which a
				// starved host can hold up for far longer than 20 s (BUG-0487).
				waitForWALCountWithin(t, tc, placed.member, transport.OpAlterBucket, placed.bucket, 1, 60*time.Second, func() string {
					stream := peerOf(t, tc, placed.owner, placed.member)
					state, reason := stream.streamStatus()
					acked, opened := stream.cursor(clusterOf(tc, placed.owner).self.ID)
					return fmt.Sprintf("node %d, which owns the bucket, holds %d of its %s entries, and its stream to node %d is %v (%q), in sync %v, acknowledged through %d (cursor opened %v)",
						placed.owner+1, walOpCount(t, tc.nodes[placed.owner].server.core.wal, transport.OpAlterBucket, placed.bucket), transport.OpAlterBucket,
						placed.member+1, state, reason, stream.inSync(), acked, opened)
				})
				pollUntil(t, fmt.Sprintf("node %d applies the alter", placed.member+1), func() bool {
					return heldFactorOf(t, tc.nodes[placed.member].server.core, placed.bucket).catalog == row.altered
				})
				releasePass()
				waitPassFinished(t, tc, placed.member)
				if mark := outsideMarkOf(tc.nodes[placed.member].server.core, placed.bucket); mark == 0 {
					t.Errorf("%s, node %d's copy of %q, carries no outside mark once the pass and the alter are both done",
						row.name, placed.member+1, placed.bucket)
				}
			})
		}
	}
}

// repromotedAfterAJoin is T-34's case 4: a copy that left its set at a join
// is marked outside by its pass and kept (its sweep is held), and an increase
// at the join's epoch places it again. With auto-rebalance on, its registry
// at the new factor puts it in its set, so the take-over copies it afresh from
// the owner, whose snapshot clears the member's awaiting record. Until then
// its reads are forwarded; afterwards it holds every acknowledged write.
func repromotedAfterAJoin(t *testing.T) {
	joinFreezeOff(t)
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	ports := reserveNodePorts(t)
	left := leftAtTheAdoption(t, tc, ports.ringNode().ID)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	createBucketAt(tc, left.owner, testDatabase, left.bucket, 2)
	waitForServing(t, tc, testDatabase, left.bucket, 2)
	keys := []string{"before"}
	putDocumentAt(tc, left.owner, testDatabase, left.bucket, "before", "all")

	releaseSweeps := holdSweepsOn(t, tc.addrs[left.member])
	defer releaseSweeps()
	joiner := joinFifth(t, tc, ports)
	defer func() { _ = joiner.client.Close() }()
	waitPassFinished(t, tc, left.member)
	waitPassFinished(t, tc, left.owner)
	memberCore := tc.nodes[left.member].server.core
	if outsideMarkOf(memberCore, left.bucket) == 0 {
		t.Fatalf("setup: node %d's pass did not mark its copy of %q, which left the set at the join", left.member+1, left.bucket)
	}
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("after-the-join-%d", i)
		putDocumentAt(tc, left.owner, testDatabase, left.bucket, key, "one")
		keys = append(keys, key)
	}

	alterWithin(t, tc, left.owner, AlterBucketRequest{Database: testDatabase, Name: left.bucket, ReplicationFactor: 3})
	waitForWALCount(t, tc, left.member, transport.OpAlterBucket, left.bucket, 1)
	requireReadsFound(t, tc, left.member, left.bucket, keys, "once the increase placed the stale copy again")
	waitFreshLocally(t, tc, left.member, left.bucket, keys)
	key := placementOf(testDatabase, left.bucket)
	if onDisk := awaitingOnDisk(t, tc.nodes[left.owner].server.core, key); len(onDisk) != 0 {
		t.Errorf("once node %d's copy was copied afresh the owner's store still holds awaiting records for %v", left.member+1, onDisk)
	}
	putDocument(tc, left.owner, testDatabase, left.bucket, "after")
	waitForBarrier(t, tc, left.owner, "after-the-copy", left.member)
	requireHeldLocally(t, memberCore, left.bucket, append(keys, "after"))
}
