package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Write concern is per bucket: a write that names no concern takes the default
// of the bucket it is for, resolved by the bucket's owner, and `majority` and
// `all` count the copies of that bucket. Since D-2 (replication-stream step 7.1)
// they count the copies that are in sync and never refuse for want of copies, so
// these tests observe a concern through what the write waits for and through
// the copy count it is answered with.

// stopEveryNodeExcept stops all nodes but the named ones.
func stopEveryNodeExcept(t *testing.T, tc *testCluster, keep ...int) {
	t.Helper()
	kept := map[int]bool{}
	for _, index := range keep {
		kept[index] = true
	}
	for index := range tc.nodes {
		if !kept[index] {
			stopNode(t, tc, index)
		}
	}
}

// timedPut sends an insert with the given concern through node index and
// returns its answer and how long it took.
func timedPut(tc *testCluster, index int, database, bucket, key, concern string) (*transport.Frame, time.Duration) {
	started := time.Now()
	frame := tc.frame(index, transport.OpPut, InsertRequest{
		Database: database, Bucket: bucket, Key: key, Data: map[string]interface{}{"n": 1}, WriteConcern: concern,
	})
	return frame, time.Since(started)
}

// TestDefaultWriteConcernResolvesPerBucketPerRequest is T-21. The durability
// default was derived once, at construction, from the NODE's replication
// factor, and resolved on the coordinator. A node started the way the standalone
// refusal recommends — seed nodes set, factor left at 1 — gave every bucket
// `one`; and a coordinator that never held the bucket, which cannot know its
// factor, chose the default for a bucket it knew nothing about and forwarded it
// as if the client had named it. The owner honoured it.
//
// Ported from ed270f2, re-aimed: the unqualified write is sent through a node
// that does not hold the bucket (five nodes at default 1, a bucket at 3), and
// the owner's links to the bucket's other two holders are frozen. Re-aimed
// again by replication-stream step 7.1 (D-2): `majority` no longer refuses when
// the copies are gone, so the concern is observed by what the write waits for —
// `majority` waits for the frozen ships to give up, `one` waits for nobody.
func TestDefaultWriteConcernResolvesPerBucketPerRequest(t *testing.T) {
	tc := newTestCluster(t, 5, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, "wide", 3)
	waitForServing(t, tc, testDatabase, "wide", 3)

	var holders, outsiders []int
	for index, node := range tc.nodes {
		if node.server.core.bucketStoreOpen(testDatabase, "wide") {
			holders = append(holders, index)
		} else {
			outsiders = append(outsiders, index)
		}
	}
	placed := mustReplicaIDs(t, tc.nodes[holders[0]].server.core.cluster, testDatabase, "wide")
	ownerIndex := nodeIndexByAddr(t, tc.addrs, placed[0])
	coordinator := outsiders[0]

	// The bucket's other two holders are frozen: a ship to either gives up
	// after replShipTimeout. `majority` waits for that; `one` does not.
	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	for _, index := range holders {
		if index != ownerIndex {
			defer freezePeer(ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[index])))()
		}
	}
	ship := ownerCluster.replShipTimeout()

	frame, took := timedPut(tc, coordinator, testDatabase, "wide", "unqualified", "")
	if frame.Opcode == transport.OpError {
		t.Fatalf("the unqualified write was refused: %s", frame.Payload)
	}
	if took < ship {
		t.Fatalf("an unqualified write to a three-copy bucket, sent through node %d which does not hold it, "+
			"was answered after %v with two copies frozen, before a ship to them could give up (%v): "+
			"the coordinator chose the default for a bucket it knows nothing about", coordinator+1, took, ship)
	}
	if confirmed, factor, _, _ := copiesConfirmedOf(frame.Payload); confirmed != 1 || factor != 3 {
		t.Errorf("the unqualified write was answered copies_confirmed %d of %d; want 1 of 3", confirmed, factor)
	}
	frame, took = timedPut(tc, coordinator, testDatabase, "wide", "explicit", "one")
	if frame.Opcode == transport.OpError || took >= ship/2 {
		t.Fatalf("a write that names `one` must not wait for the copies that are frozen: answered %s after %v", frame.Opcode, took)
	}
}

// TestDefaultWriteConcernOfAnOverrideOutranksTheBucket: the per-bucket rule is
// the default, not a policy that outranks the operator's configuration.
func TestDefaultWriteConcernOfAnOverrideOutranksTheBucket(t *testing.T) {
	overridden := &Cluster{rf: 1, wcOverride: WcAll, bucketFactors: map[placementKey]int32{}}
	overridden.recordBucketFactor(placementOf("app", "wide"), 3)
	if got := overridden.defaultWriteConcern(placementOf("app", "wide")); got != WcAll {
		t.Errorf("a configured write concern of all resolved to %s on a three-copy bucket", got)
	}
	if got := overridden.defaultWriteConcern(placementOf("app", "narrow")); got != WcAll {
		t.Errorf("a configured write concern of all resolved to %s on a bucket with no factor", got)
	}

	plain := &Cluster{rf: 1, bucketFactors: map[placementKey]int32{}}
	plain.recordBucketFactor(placementOf("app", "wide"), 3)
	if got := plain.defaultWriteConcern(placementOf("app", "wide")); got != WcMajority {
		t.Errorf("the default for a three-copy bucket is %s, want majority", got)
	}
	if got := plain.defaultWriteConcern(placementOf("app", "narrow")); got != WcOne {
		t.Errorf("the default for a one-copy bucket is %s, want 1", got)
	}
}

// TestWriteConcernArithmeticUsesTheBucketsOwnFactor: `all` and `majority` count
// the bucket's copies, not the node's. On three nodes at default 1, a bucket at
// 3 answers `all` with copies_confirmed 3 of 3 and `majority` with at least 2;
// at default 3, `majority` and `all` on a bucket at 1 succeed with 1 of 1, where
// the node's arithmetic asked for two copies of a bucket that has one.
// Re-aimed by replication-stream step 7.1 (D-2): with the copies gone the
// writes no longer refuse, so the counts are read from healthy copies.
func TestWriteConcernArithmeticUsesTheBucketsOwnFactor(t *testing.T) {
	t.Run("a bucket above the node default", func(t *testing.T) {
		tc := newTestCluster(t, 3, 1)
		defer tc.stop()

		ownerIndex := 0
		wide := bucketOwnedBy(t, tc, ownerIndex, "wide")
		tc.ok(ownerIndex, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		createBucketAt(tc, ownerIndex, testDatabase, wide, 3)
		waitForServing(t, tc, testDatabase, wide, 3)

		for concern, least := range map[string]int32{"all": 3, "majority": 2} {
			frame, _ := timedPut(tc, ownerIndex, testDatabase, wide, "k-"+concern, concern)
			confirmed, factor, _, _ := copiesConfirmedOf(frame.Payload)
			if frame.Opcode == transport.OpError || confirmed < least || factor != 3 {
				t.Errorf("`%s` on a healthy bucket at 3 was answered %s, copies_confirmed %d of %d; want at least %d of 3: %s",
					concern, frame.Opcode, confirmed, factor, least, frame.Payload)
			}
		}
	})

	t.Run("a bucket below the node default", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()

		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		createBucketAt(tc, 0, testDatabase, "solo", 1)
		for _, concern := range []string{"majority", "all"} {
			frame, _ := timedPut(tc, 0, testDatabase, "solo", "k-"+concern, concern)
			confirmed, factor, _, _ := copiesConfirmedOf(frame.Payload)
			if frame.Opcode == transport.OpError || confirmed != 1 || factor != 1 {
				t.Fatalf("`%s` on a bucket at 1 needs one copy and must succeed with 1 of 1: answered %s, %d of %d: %s",
					concern, frame.Opcode, confirmed, factor, frame.Payload)
			}
		}
	})
}

// TestTheCreateOfABucketAboveTheNodeDefaultTakesItsOwnDefaultConcern is F-6b.
// The default concern was resolved before the write reached the owner's apply,
// so the create that registers a bucket's factor was itself taken at the node's:
// `one`. It is resolved after the apply, where the factor is known.
// Re-aimed by replication-stream step 7.1 (D-2): with the other copies frozen
// the create no longer refuses; at `majority` it waits for the frozen ships to
// give up, where `one` would have answered at once.
func TestTheCreateOfABucketAboveTheNodeDefaultTakesItsOwnDefaultConcern(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	ownerIndex := 0
	wide := bucketOwnedBy(t, tc, ownerIndex, "wide")
	tc.ok(ownerIndex, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2)
	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	for index := range tc.nodes {
		if index != ownerIndex {
			defer freezePeer(ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[index])))()
		}
	}

	started := time.Now()
	frame := tc.frame(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase, Name: wide, Type: BucketTypeHash, ReplicationFactor: 3,
	})
	took := time.Since(started)
	if frame.Opcode == transport.OpError {
		t.Fatalf("the create was refused: %s", frame.Payload)
	}
	if ship := ownerCluster.replShipTimeout(); took < ship {
		t.Fatalf("the create of a bucket at 3, with its other copies frozen, was answered after %v, before a ship to them could give up (%v): "+
			"it was taken at the node's default concern", took, ship)
	}
}

// bucketOwnedBy names a bucket the ring places on the node at index.
func bucketOwnedBy(t *testing.T, tc *testCluster, index int, prefix string) string {
	t.Helper()
	cluster := tc.nodes[index].server.core.cluster
	for attempt := 0; attempt < 500; attempt++ {
		name := fmt.Sprintf("%s%03d", prefix, attempt)
		if owner, err := cluster.Owner(placementOf(testDatabase, name)); err == nil && owner.ID == cluster.self.ID {
			return name
		}
	}
	t.Fatalf("no candidate bucket name out of 500 is placed on node %d", index+1)
	return ""
}

// TestRestoreDropThroughANodeThatHoldsNothingReplacesTheBucket is F-5. A
// restore decides exists, type mismatch, --drop and merge from the state of the
// node it runs on. Served on a node that only computes itself into the bucket's
// replica set — a bucket declared at 1 on nodes whose default is 3 — that state
// is empty, so --drop dropped nothing and the restore merged into the owner's
// bucket. A restore is served by the owner alone.
func TestRestoreDropThroughANodeThatHoldsNothingReplacesTheBucket(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, "solo", 1)
	putDocument(tc, 0, testDatabase, "solo", "kept")
	manifest, dumped := dumpBucketOverWire(t, tc, 0, testDatabase, "solo", 1)
	putDocument(tc, 0, testDatabase, "solo", "added-after-the-dump")

	holderIndex, holdsNothing := -1, -1
	for index, node := range tc.nodes {
		if node.server.core.bucketStoreOpen(testDatabase, "solo") {
			holderIndex = index
		} else if holdsNothing < 0 {
			holdsNothing = index
		}
	}
	if holderIndex < 0 || holdsNothing < 0 {
		t.Fatalf("test setup: holder %d, node without the bucket %d", holderIndex, holdsNothing)
	}

	restoreBucketOverWire(t, tc, holdsNothing, RestoreStartRequest{
		Database: testDatabase, Bucket: "solo", FormatVersion: manifest.FormatVersion,
		StoreType: manifest.StoreType, Bytes: int64(len(dumped)), Drop: true,
	}, dumped, 1)

	holder := tc.nodes[holderIndex].server.core
	if found, err := holder.Find(FindRequest{Database: testDatabase, Bucket: "solo", Key: "added-after-the-dump"}); err != nil || found.Found {
		t.Fatalf("restore --drop through node %d left a document written after the dump on the owner (found %v, %v): it merged instead of replacing",
			holdsNothing+1, found != nil && found.Found, err)
	}
	if found, err := holder.Find(FindRequest{Database: testDatabase, Bucket: "solo", Key: "kept"}); err != nil || !found.Found {
		t.Fatalf("restore --drop lost the dumped document (%v)", err)
	}
}
