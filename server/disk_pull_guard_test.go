package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0104, owner ruling 2026-10-14 (fail closed): a node at or above the disk
// high-water mark never lands a bucket it does not already hold, whatever
// asked for the copy, and it still adopts every membership epoch. The disk is
// reported through the node's own probe set (setCapacityProbes); no real disk
// fills.

// TestRedNodeDoesNotLandABucketByAnyPath drives every caller that can copy a
// bucket onto a node. Three members at factor 2 and a joiner J that is over the
// mark from its start. J misses the join's commit, so it learns the new epoch
// by gossip, and the join places two buckets on it that it never held: P, which
// a green member owns and J is a replica of, and Q, which J now owns. Then:
//
//   - the sweep's per-bucket step on J (migrateIfMissing) for P;
//   - a peer's hand-off: P's owner's handOffBucket asks J to migrate P;
//   - the operator's migrate of P and of Q on J;
//   - J's take-over of Q, which a write to Q on J wakes.
//
// Each must be refused with a text that names J, the bucket and the fix; no
// copy may be materialised; the epoch must be adopted; P's owner must keep its
// copy; a write to Q must say the disk is why it is refused, not give the
// generic join-fence text; and J must write a WARN.
func TestRedNodeDoesNotLandABucketByAnyPath(t *testing.T) {
	tc := newTestCluster(t, 3, 2)
	defer tc.stop()
	ports := reserveNodePorts(t)
	redID := ports.ringAddr
	ca := clusterOf(tc, 0)

	before := ringOf(t, ca.Members(), 2)
	joined := ringOf(t, append(ca.Members(), ports.ringNode()), 2)
	placedOn := func(placement *ring.Ring, name string) []string {
		set, err := placement.Replicas(placementOf(testDatabase, name).encoded, 2)
		if err != nil {
			t.Fatalf("placing %s: %v", name, err)
		}
		return nodeIDs(set)
	}
	// The landing count below is process-wide: beforeMaterializeHook does not
	// say which node landed. So the join must give no green member a copy it
	// does not already hold, or that member's legitimate pull would be counted
	// as red J's. The ports are drawn per run, and in about 1 draw in 200 the
	// join put a bucket on an old member that held neither of its old copies
	// (BUG-0487).
	onlyJGainsACopy := func(name string) bool {
		held := placedOn(before, name)
		for _, node := range placedOn(joined, name) {
			if node != redID && !slices.Contains(held, node) {
				return false
			}
		}
		return true
	}
	replica := bucketNamed(t, "redrep", func(name string) bool {
		set := placedOn(joined, name)
		return set[0] != redID && slices.Contains(set, redID) && onlyJGainsACopy(name)
	})
	owned := bucketNamed(t, "redown", func(name string) bool {
		return placedOn(joined, name)[0] == redID && onlyJGainsACopy(name)
	})

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, name := range []string{replica, owned} {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: name, Key: "doc", Data: map[string]interface{}{"n": 1}})
		waitForServing(t, tc, testDatabase, name, 2)
	}
	// The members' logs are left unpruned, so the owner's stream may also
	// replay a bucket's create to J; J refuses that too (BUG-0550), which
	// TestRedNodeDoesNotLandAReplicaByStreamReplay asserts on its own.
	// The members are pinned green rather than left to the host's real disk.
	for _, node := range tc.nodes {
		setCapacityProbes(t, node.server.core, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(10)) })
	}

	// materializeRange is reached only through materializeBucket, which this
	// hook runs immediately before: a count of zero is "no copy landed".
	var landed atomic.Int64
	materialized := func(database, bucket string) {
		if database == testDatabase && (bucket == replica || bucket == owned) {
			landed.Add(1)
		}
	}
	beforeMaterializeHook.Store(&materialized)
	t.Cleanup(func() { beforeMaterializeHook.Store(nil) })

	// J refuses the join's commit, so it learns the epoch by gossip.
	missed := func(nodeID, phase, _ string, _ uint64) error {
		if nodeID == redID && phase == phaseCommit {
			return errors.New("test: this node misses the commit and learns the join by gossip")
		}
		return nil
	}
	membershipPhaseHook.Store(&missed)
	t.Cleanup(func() { membershipPhaseHook.Store(nil) })

	redLog := &syncBuffer{}
	red := startJoinTargetPrepared(t, tc, ports, tc.token, func(svc *Service) {
		setCapacityProbes(t, svc.core, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(92)) })
		svc.core.cluster.logger = slog.New(slog.NewTextHandler(redLog, &slog.HandlerOptions{Level: slog.LevelWarn}))
	})
	rc := red.server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := ca.proposeJoin(ctx, ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	pollUntil(t, "the red joiner adopts the join by gossip", func() bool {
		return rc.Epoch() == ca.Epoch() && !rc.bootstrapped.Load()
	})
	membershipPhaseHook.Store(nil)

	namesTheFix := func(what, text, bucket string) {
		t.Helper()
		for _, phrase := range []string{redID, fmt.Sprintf("%q", bucket), "high-water mark", "Free space",
			fmt.Sprintf("'migrate %s %s' on %s", testDatabase, bucket, redID), "remove-node " + redID} {
			if !strings.Contains(text, phrase) {
				t.Errorf("%s: the refusal does not name %q: %s", what, phrase, text)
			}
		}
	}

	// The sweep's per-bucket step on J.
	err := rc.migrateIfMissing(ctx, testDatabase, replica)
	if !errors.Is(err, ErrMemberDiskFull) {
		t.Errorf("J's sweep step for %s = %v, want ErrMemberDiskFull", replica, err)
	} else {
		namesTheFix("sweep", err.Error(), replica)
	}

	// A peer's hand-off: P's owner asks J to migrate the replica it now
	// belongs to.
	ownerIndex := slices.Index(tc.addrs, placedOn(joined, replica)[0])
	owner := clusterOf(tc, ownerIndex)
	replicas, err := owner.Replicas(placementOf(testDatabase, replica))
	if err != nil {
		t.Fatalf("placing %s on its owner: %v", replica, err)
	}
	if err := owner.handOffBucket(ctx, testDatabase, replica, replicas); err == nil {
		t.Errorf("the owner's hand-off of %s to red J was confirmed; J must refuse the copy", replica)
	} else {
		namesTheFix("peer hand-off", err.Error(), replica)
	}
	if !tc.nodes[ownerIndex].server.core.bucketStoreOpen(testDatabase, replica) {
		t.Errorf("the owner released its copy of %s although J refused to take one", replica)
	}

	// The operator's migrate, of the replica and of the bucket J owns.
	for _, name := range []string{replica, owned} {
		migrate := func() (*transport.Frame, MigrateResponse) {
			frame := tc.frameOn(red, transport.OpMigrate, MigrateRequest{Database: testDatabase, Bucket: name})
			return frame, decodedMigrate(t, frame)
		}
		frame, answer := migrate()
		if name == owned {
			// BUG-0579: the operator's migrate of Q reaches J before the
			// take-over can reach Q's other holders, so J answers the
			// take-over refusal first. Poll the way the write below does,
			// until the disk is named (or an unexpected answer, which the
			// assertions below report).
			pollUntil(t, "the operator migrate of the bucket red J owns names the disk", func() bool {
				frame, answer = migrate()
				return answer.Success || strings.Contains(answer.Error, "high-water mark")
			})
		}
		if answer.Success {
			t.Errorf("operator migrate of %s on red J succeeded: %s", name, answer.Message)
			continue
		}
		namesTheFix("operator migrate of "+name, answer.Error, name)
		// BUG-0552: the refusal is a defined code, so a driver, the CLI or a
		// monitor does not read a self-protecting node as a server fault.
		if code := refusalOf(t, frame).Code; code != int(transport.ErrUnavailable) {
			t.Errorf("operator migrate of %s on red J carries code %d (0x%02x), want %d (UNAVAILABLE)",
				name, code, code, int(transport.ErrUnavailable))
		}
	}

	// The owner-to-be: a write to Q on J wakes its take-over, and is refused
	// with the disk named, not the generic join-fence text.
	var refusal string
	pollUntil(t, "a write to the bucket red J owns names the disk", func() bool {
		frame := tc.frameOn(red, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: owned, Key: "after", Data: map[string]interface{}{"n": 2}})
		if frame.Opcode != transport.OpError {
			refusal = "the write was accepted"
			return true
		}
		refusal = refusalOf(t, frame).Message
		return strings.Contains(refusal, "high-water mark")
	})
	namesTheFix("write to the owned bucket", refusal, owned)

	if n := landed.Load(); n != 0 {
		t.Errorf("red J materialised %d bucket copies; it must land none", n)
	}
	for _, name := range []string{replica, owned} {
		if red.server.core.bucketStoreOpen(testDatabase, name) {
			t.Errorf("red J holds %s", name)
		}
	}
	warned := redLog.String()
	if !strings.Contains(warned, "refused to copy a bucket in") || !strings.Contains(warned, redID) || !strings.Contains(warned, "remove-node") {
		t.Errorf("red J wrote no WARN naming itself, the refused copy and the fix; its WARN log:\n%s", warned)
	}
}

// TestRedNodeDoesNotLandAReplicaByStreamReplay is BUG-0550: a bucket can reach
// a node with no pull, when its owner's stream replays the bucket's create from
// a log that still holds it. The owner cannot see the node's disk (BUG-0550's
// step 0), so the ruling's fallback holds: the red node refuses the replayed
// create, the owner's stream to it stalls on it with the disk named, and the
// stream resumes once the node is below the mark.
//
// The replay is made certain, not raced: no log is pruned, so the owner's log
// holds the bucket's create, and J is red from its first start, before any
// stream to it opens. A red J refuses every pull (BUG-0104), so the only way P
// can land on it is the stream, and the wait below ends on one of the two
// outcomes the stream reaching the create can have: P open on J, or the
// owner's stream to J stalled on the disk refusal.
func TestRedNodeDoesNotLandAReplicaByStreamReplay(t *testing.T) {
	tc := newTestCluster(t, 3, 2)
	defer tc.stop()
	ports := reserveNodePorts(t)
	redID := ports.ringAddr
	ca := clusterOf(tc, 0)

	before := ringOf(t, ca.Members(), 2)
	joined := ringOf(t, append(ca.Members(), ports.ringNode()), 2)
	placedOn := func(placement *ring.Ring, name string) []string {
		set, err := placement.Replicas(placementOf(testDatabase, name).encoded, 2)
		if err != nil {
			t.Fatalf("placing %s: %v", name, err)
		}
		return nodeIDs(set)
	}
	// The join must leave the bucket's owner where it is. A bucket whose owner
	// moves at the join reaches the new owner by a pull, and the new owner holds
	// the create as a replica's entry, which it never replays (sequence numbers
	// mean something only in the stream that assigned them): no stream then
	// carries the create to J, and the wait below ends in its timeout. The ports
	// are drawn per run, so about 1 draw in 100 placed the bucket that way.
	replica := bucketNamed(t, "replayrep", func(name string) bool {
		set := placedOn(joined, name)
		return set[0] != redID && slices.Contains(set, redID) && placedOn(before, name)[0] == set[0]
	})
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: replica, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: replica, Key: "doc", Data: map[string]interface{}{"n": 1}})
	waitForServing(t, tc, testDatabase, replica, 2)
	for _, node := range tc.nodes {
		setCapacityProbes(t, node.server.core, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(10)) })
	}

	missed := func(nodeID, phase, _ string, _ uint64) error {
		if nodeID == redID && phase == phaseCommit {
			return errors.New("test: this node misses the commit and learns the join by gossip")
		}
		return nil
	}
	membershipPhaseHook.Store(&missed)
	t.Cleanup(func() { membershipPhaseHook.Store(nil) })

	red := startJoinTargetPrepared(t, tc, ports, tc.token, func(svc *Service) {
		setCapacityProbes(t, svc.core, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(92)) })
	})
	rc := red.server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := ca.proposeJoin(ctx, ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	pollUntil(t, "the red joiner adopts the join by gossip", func() bool {
		return rc.Epoch() == ca.Epoch() && !rc.bootstrapped.Load()
	})
	membershipPhaseHook.Store(nil)

	owner := clusterOf(tc, slices.Index(tc.addrs, placedOn(joined, replica)[0]))
	streamToRed := func() *peer {
		owner.peersMu.RLock()
		defer owner.peersMu.RUnlock()
		return owner.peers[redID]
	}
	var stallReason string
	pollUntil(t, "the owner's stream reaches the bucket's create on red J", func() bool {
		if red.server.core.bucketStoreOpen(testDatabase, replica) {
			return true
		}
		p := streamToRed()
		if p == nil {
			return false
		}
		reason, stalled := p.stalledReason()
		if stalled && strings.Contains(reason, "high-water mark") {
			stallReason = reason
			return true
		}
		return false
	})
	if red.server.core.bucketStoreOpen(testDatabase, replica) {
		t.Fatalf("red J holds %s: the owner's stream replayed its create and J applied it", replica)
	}
	for _, phrase := range []string{redID, fmt.Sprintf("%q", replica), "high-water mark", "stalled", "every bucket", "Free space", "remove-node " + redID} {
		if !strings.Contains(stallReason, phrase) {
			t.Errorf("the owner's stall reason does not name %q: %s", phrase, stallReason)
		}
	}
	if streamToRed().inSync() {
		t.Errorf("the owner counts its stream to red J as in sync while J refuses it")
	}

	// Below the mark, the stream resumes and lands the copy. The sender's
	// retry wait has backed off by now; a wake ends it, as a later retry would.
	setCapacityProbes(t, red.server.core, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(10)) })
	pollUntil(t, "the owner's stream to J resumes once J is below the mark", func() bool {
		p := streamToRed()
		p.wakeSender()
		return red.server.core.bucketStoreOpen(testDatabase, replica) && p.inSync()
	})
}

// TestThePullGuardFailsOpenWhereItCannotMeasure is refuseLandingOnFullDisk's
// promise: a mark of 0 and a filesystem that cannot be read both let a pull
// through, while the same 99% reading above a mark of 85 refuses it.
func TestThePullGuardFailsOpenWhereItCannotMeasure(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	node := tc.nodes[0].server.core
	unreadable := func(string) (diskStat, error) { return diskStat{}, errors.New("test: statfs failed") }
	for _, tt := range []struct {
		name     string
		markPct  int
		statfs   func(string) (diskStat, error)
		wantFull bool
	}{
		{"mark 85 and a 99% disk refuses", 85, fixedDisk(diskFixture(99)), true},
		{"mark 0 and a 99% disk lands", 0, fixedDisk(diskFixture(99)), false},
		{"mark 85 and an unreadable disk lands", 85, unreadable, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			node.config.Server.DiskHighWaterPct = tt.markPct
			setCapacityProbes(t, node, func(p *capacityProbes) { p.diskStatfs = tt.statfs })
			err := node.cluster.refuseLandingOnFullDisk(testDatabase, testBucket)
			if got := errors.Is(err, ErrMemberDiskFull); got != tt.wantFull {
				t.Errorf("refuseLandingOnFullDisk = %v, want a disk-full refusal: %v", err, tt.wantFull)
			}
		})
	}
}

// TestRefusedPullsWarnOncePerInterval: refusals inside one reclaimWarnInterval
// write one WARN, and the first refusal of the next interval writes another
// that carries the count since the first. The guard reads time.Now, so the
// test moves the interval by rewinding diskRefusalWarnedAt.
func TestRefusedPullsWarnOncePerInterval(t *testing.T) {
	warnings := &syncBuffer{}
	tc := newTestClusterTuned(t, 1, 1, nil, func(_ int, c *Cluster) {
		c.logger = slog.New(slog.NewTextHandler(warnings, &slog.HandlerOptions{Level: slog.LevelWarn}))
	})
	defer tc.stop()
	node := tc.nodes[0].server.core
	setCapacityProbes(t, node, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(99)) })

	refuse := func(times int) {
		for range times {
			if err := node.cluster.refuseLandingOnFullDisk(testDatabase, testBucket); !errors.Is(err, ErrMemberDiskFull) {
				t.Fatalf("refusal = %v, want ErrMemberDiskFull", err)
			}
		}
	}
	refuse(5)
	if got := strings.Count(warnings.String(), "refused to copy a bucket in"); got != 1 || !strings.Contains(warnings.String(), "refused_since_last_warning=1 ") {
		t.Fatalf("5 refusals in one interval wrote %d WARNs, want 1 carrying a count of 1:\n%s", got, warnings.String())
	}
	node.cluster.diskRefusalWarnedAt.Store(time.Now().Add(-2 * reclaimWarnInterval).UnixNano())
	refuse(1)
	if got := strings.Count(warnings.String(), "refused to copy a bucket in"); got != 2 || !strings.Contains(warnings.String(), "refused_since_last_warning=5 ") {
		t.Errorf("the next interval's first refusal wrote %d WARNs in all, want 2, the second carrying the other 5:\n%s", got, warnings.String())
	}
}

// BUG-0551, owner ruling 2026-10-14 (keep the copy): a node above the mark
// checks the disk guard before it releases a copy it means to replace, so a
// refused pull leaves the node exactly as it was.

// recopyPath is one of the three steps that release this node's copy of a
// bucket and then pull it again. actor is the index of the node that runs it;
// run runs it on that node's cluster, peer being the other member's.
type recopyPath struct {
	name  string
	actor int
	run   func(ctx context.Context, actor, peer *Cluster, bucket string) error
}

// recopyPaths are copyAfresh (a replica that cannot show it holds its owner's
// term), reseedDiverged (migrate's remedy for a diverged replica) and
// copyForTakeOver (an owner taking over a bucket whose best copy is the
// replica's, over its own stale copy).
var recopyPaths = []recopyPath{
	{name: "follower copies afresh", actor: 1, run: func(ctx context.Context, actor, peer *Cluster, bucket string) error {
		held := heldBucket{database: testDatabase, bucket: bucket}
		_, err := actor.copyAfresh(ctx, held, placementOf(testDatabase, bucket), peer.self)
		return err
	}},
	{name: "diverged replica is re-seeded", actor: 1, run: func(ctx context.Context, actor, _ *Cluster, bucket string) error {
		st, err := actor.s.bucketStateNamed(testDatabase, bucket)
		if err != nil {
			return fmt.Errorf("reading the replica's incarnation: %w", err)
		}
		return actor.reseedDiverged(ctx, divergenceRecord{Database: testDatabase, Bucket: bucket, Incarnation: st.Incarnation, Reason: refusalDiverged})
	}},
	{name: "take-over over a stale own copy", actor: 0, run: func(ctx context.Context, actor, peer *Cluster, bucket string) error {
		ref := ReplBucketRef{Database: testDatabase, Bucket: bucket}
		own, err := actor.answerCopy(ref)
		if err != nil {
			return fmt.Errorf("reading the owner's copy: %w", err)
		}
		theirs, err := peer.answerCopy(ref)
		if err != nil {
			return fmt.Errorf("reading the replica's copy: %w", err)
		}
		best := heldCopy{node: peer.self, answer: theirs, current: true}
		return actor.copyForTakeOver(ctx, heldBucket{database: testDatabase, bucket: bucket}, placementOf(testDatabase, bucket), own, takeOverPlan{best: best})
	}},
}

// TestRedNodeKeepsItsStaleCopyWhenTheReplacingPullIsRefused: two members at
// factor 2 hold a bucket of five keys. The node a path runs on is pinned above
// the mark, and the path is run. It must be refused with ErrMemberDiskFull
// naming the node, the bucket and the fix, and the node must still hold its
// copy as it was: open, the same lineage, the same count, every key with the
// same bytes. No copy may land, and the other member keeps its copy. Pinned
// green again, the same path releases and pulls, and the copy is replaced.
// Guard: the release ahead of the guard, as at a0b6ad88; the copy is gone
// after the refusal.
func TestRedNodeKeepsItsStaleCopyWhenTheReplacingPullIsRefused(t *testing.T) {
	keys := []string{"k0", "k1", "k2", "k3", "k4"}
	for _, path := range recopyPaths {
		t.Run(path.name, func(t *testing.T) {
			tc := newTestCluster(t, 2, 2)
			defer tc.stop()
			pair := ringWith(t, 2, tc.addrs...)
			bucket := bucketNamed(t, "keepstale", func(name string) bool { return ownerOn(pair, name) == tc.addrs[0] })
			tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
			tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
			waitForServing(t, tc, testDatabase, bucket, 2)
			for i, key := range keys {
				tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: key,
					Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
			}
			pollUntil(t, "both members hold every key", func() bool {
				return holdsKeyLocally(tc.nodes[0], bucket, keys[4]) && holdsKeyLocally(tc.nodes[1], bucket, keys[4])
			})
			peerIndex := 1 - path.actor
			actorNode, actor, peer := tc.nodes[path.actor], clusterOf(tc, path.actor), clusterOf(tc, peerIndex)
			ref := ReplBucketRef{Database: testDatabase, Bucket: bucket}

			// heldAsItWas is the actor's copy, read the same way before and
			// after: its lineage and every key's stored bytes.
			heldAsItWas := func() (ReplCopy, map[string]string) {
				t.Helper()
				lineage, err := actor.answerCopy(ref)
				if err != nil {
					t.Fatalf("reading %s's copy of %s: %v", actor.self.ID, bucket, err)
				}
				stored := map[string]string{}
				for _, key := range keys {
					found, err := actorNode.server.core.Find(FindRequest{Database: testDatabase, Bucket: bucket, Key: key})
					if err == nil && found != nil && found.Found {
						stored[key] = string(found.Data)
					}
				}
				return lineage, stored
			}
			lineageBefore, storedBefore := heldAsItWas()
			if !lineageBefore.Holds || len(storedBefore) != len(keys) {
				t.Fatalf("setup: %s holds %d of %d keys of %s (%+v)", actor.self.ID, len(storedBefore), len(keys), bucket, lineageBefore)
			}
			countBefore := localCountOf(t, actorNode.server.core, bucket)

			var landed atomic.Int64
			materialized := func(database, name string) {
				if database == testDatabase && name == bucket {
					landed.Add(1)
				}
			}
			beforeMaterializeHook.Store(&materialized)
			t.Cleanup(func() { beforeMaterializeHook.Store(nil) })

			setCapacityProbes(t, actorNode.server.core, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(95)) })
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			err := path.run(ctx, actor, peer, bucket)
			if !errors.Is(err, ErrMemberDiskFull) {
				t.Errorf("%s on red %s = %v, want ErrMemberDiskFull", path.name, actor.self.ID, err)
			} else {
				for _, phrase := range []string{actor.self.ID, fmt.Sprintf("%q", bucket), "high-water mark", "Free space",
					fmt.Sprintf("'migrate %s %s' on %s", testDatabase, bucket, actor.self.ID), "remove-node " + actor.self.ID} {
					if !strings.Contains(err.Error(), phrase) {
						t.Errorf("the refusal does not name %q: %v", phrase, err)
					}
				}
			}

			if !actorNode.server.core.bucketStoreOpen(testDatabase, bucket) {
				t.Fatalf("red %s released its copy of %s before the pull it then refused", actor.self.ID, bucket)
			}
			lineageAfter, storedAfter := heldAsItWas()
			if !reflect.DeepEqual(lineageAfter, lineageBefore) {
				t.Errorf("red %s's copy of %s changed lineage: before %+v, after %+v", actor.self.ID, bucket, lineageBefore, lineageAfter)
			}
			if !reflect.DeepEqual(storedAfter, storedBefore) {
				t.Errorf("red %s's copy of %s changed: before %v, after %v", actor.self.ID, bucket, storedBefore, storedAfter)
			}
			if countAfter := localCountOf(t, actorNode.server.core, bucket); countAfter != countBefore {
				t.Errorf("red %s counts %d documents in %s, had %d", actor.self.ID, countAfter, bucket, countBefore)
			}
			if n := landed.Load(); n != 0 {
				t.Errorf("red %s landed %d copies of %s; it must land none", actor.self.ID, n, bucket)
			}
			if !holdsKeyLocally(tc.nodes[peerIndex], bucket, keys[4]) {
				t.Errorf("%s no longer holds its copy of %s", peer.self.ID, bucket)
			}

			// Green again: the same path releases and pulls.
			setCapacityProbes(t, actorNode.server.core, harnessProbes)
			if err := path.run(ctx, actor, peer, bucket); err != nil {
				t.Fatalf("%s on green %s: %v", path.name, actor.self.ID, err)
			}
			if n := landed.Load(); n != 1 {
				t.Errorf("green %s landed %d copies of %s, want 1", actor.self.ID, n, bucket)
			}
			if _, stored := heldAsItWas(); len(stored) != len(keys) {
				t.Errorf("green %s holds %d of %d keys of %s after the re-copy", actor.self.ID, len(stored), len(keys), bucket)
			}
		})
	}
}
