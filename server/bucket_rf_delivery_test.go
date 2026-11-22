package server

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The addressees of an entry are decided by the factor the entry was placed at,
// never by the registry as it stands when the entry is looked at again
// (verifier D-1). These tests recreate the case that showed the difference: a
// drop that catch-up had to deliver after a name was re-created at another
// factor.

// TestADropOfAnInheritingBucketReachesAReplicaThatWasDownAfterTheNameIsRecreated
// drives, on three nodes, a replica that is down while an inheriting bucket is
// dropped and its name re-created at another factor, an owner restart that
// loses the live queue, and the replica's return.
func TestADropOfAnInheritingBucketReachesAReplicaThatWasDownAfterTheNameIsRecreated(t *testing.T) {
	cases := []struct {
		name             string
		nodeDefault      int
		recreatedAt      int
		recreatedIsOnAll bool
	}{
		{"recreated lower (the verifier's case)", 3, 1, false},
		{"recreated inheriting (the verifier's control)", 3, 0, true},
		{"recreated at the same factor", 3, 3, true},
		{"recreated higher", 2, 3, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tc := newTestCluster(t, 3, c.nodeDefault)
			defer tc.stop()

			tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
			bucket := bucketOwnedBy(t, tc, 0, "x")
			createBucketAt(tc, 0, testDatabase, bucket, 0)
			putDocument(tc, 0, testDatabase, bucket, "old")

			// The replica that will be down: a holder that is not the owner.
			var replica = -1
			for index, node := range tc.nodes {
				if index != 0 && node.server.core.bucketStoreOpen(testDatabase, bucket) {
					replica = index
				}
			}
			if replica < 0 {
				deadline := time.Now().Add(15 * time.Second)
				for replica < 0 && time.Now().Before(deadline) {
					for index, node := range tc.nodes {
						if index != 0 && node.server.core.bucketStoreOpen(testDatabase, bucket) {
							replica = index
						}
					}
					time.Sleep(50 * time.Millisecond)
				}
			}
			if replica < 0 {
				t.Fatal("test setup: no replica holds the bucket")
			}
			waitForLocalCopies(t, tc, testDatabase, bucket, "old", c.nodeDefault)
			start := stopNode(t, tc, replica)

			// At `one`: with the replica down, a majority of two cannot confirm.
			tc.ok(0, transport.OpDeleteBucket, bson.M{"database": testDatabase, "name": bucket, "write_concern": "one"})
			createBucketAt(tc, 0, testDatabase, bucket, c.recreatedAt)
			putDocument(tc, 0, testDatabase, bucket, "new")

			tc.restartNode(0)
			revived := start()

			holds := func(key string) bool {
				resp, err := revived.server.core.Find(FindRequest{Database: testDatabase, Bucket: bucket, Key: key})
				return err == nil && resp != nil && resp.Found
			}
			deadline := time.Now().Add(25 * time.Second)
			for {
				staleGone := !holds("old")
				placed := c.recreatedIsOnAll
				newOK := !placed || holds("new")
				notHeld := placed || !revived.server.core.bucketStoreOpen(testDatabase, bucket)
				if staleGone && newOK && notHeld {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("after the replica returned: holds the dropped document %v; holds the new one %v (want %v); serves a bucket it is not placed on %v",
						!staleGone, holds("new"), placed, !notHeld)
				}
				time.Sleep(100 * time.Millisecond)
			}

			// And a read through the returned replica sees the new bucket, not
			// the dropped one.
			frame := tc.frame(replica, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "new"})
			if frame.Opcode == transport.OpError {
				t.Fatalf("a read of the new document through the returned replica failed: %s", string(frame.Payload))
			}
			old := tc.frame(replica, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "old"})
			if old.Opcode != transport.OpError {
				var resp FindResponse
				if err := bson.Unmarshal(old.Payload, &resp); err == nil && resp.Found {
					t.Fatal("a read through the returned replica still finds the dropped bucket's document")
				}
			}
		})
	}
}

// TestEveryCallSiteOfTheDeliverySetAgrees is rev 4 §18.2's requirement and the
// concrete cause of D-1: the set an entry is addressed to was decided in three
// places (the owner's live delivery, catch-up's filter, retention's and
// relevance's bookkeeping) and they disagreed when the registry no longer said
// what it said when the entry was placed. One function of (entry, key) decides
// it now, and this table asks the call sites that remain — live delivery,
// deliverTo and the replay — what they say, for a create and a drop, declared
// and inheriting, with the registry as it was and as it is after the apply.
// Retention keeps no bookkeeping of its own since its bound is every member's
// cursor (stream dispatch 5).
func TestEveryCallSiteOfTheDeliverySetAgrees(t *testing.T) {
	tc := newTestCluster(t, 3, 2)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	oc := tc.nodes[0].server.core.cluster
	bucket := bucketOwnedBy(t, tc, 0, "table")
	key := placementOf(testDatabase, bucket)

	createPayload := func(factor int) []byte {
		return mustMarshal(t, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash, ReplicationFactor: factor})
	}
	dropPayload := func(previous int32) []byte {
		return mustMarshal(t, DeleteBucketRequest{Database: testDatabase, Name: bucket, PreviousFactor: previous})
	}
	entries := []struct {
		name  string
		entry ReplEntry
		// factor is what the entry was placed at: the set every call site must
		// name, whatever the registry says now.
		factor int
	}{
		{"create declared 3", ReplEntry{Op: int32(transport.OpCreateBucket), Bucket: bucket, Payload: createPayload(3)}, 3},
		{"create declared 1", ReplEntry{Op: int32(transport.OpCreateBucket), Bucket: bucket, Payload: createPayload(1)}, 1},
		{"create inheriting", ReplEntry{Op: int32(transport.OpCreateBucket), Bucket: bucket, Payload: createPayload(0)}, 2},
		{"drop of a declared 3", ReplEntry{Op: int32(transport.OpDeleteBucket), Bucket: bucket, Payload: dropPayload(3)}, 3},
		{"drop of a declared 1", ReplEntry{Op: int32(transport.OpDeleteBucket), Bucket: bucket, Payload: dropPayload(1)}, 1},
		{"drop of an inheriting bucket, stamped", ReplEntry{Op: int32(transport.OpDeleteBucket), Bucket: bucket, Payload: dropPayload(2)}, 2},
		{"drop written before stamping existed", ReplEntry{Op: int32(transport.OpDeleteBucket), Bucket: bucket, Payload: dropPayload(0)}, 2},
	}
	registryStates := []struct {
		name   string
		factor int32
	}{{"registry empty", 0}, {"registry at 1", 1}, {"registry at 3", 3}}

	ids := func(nodes []ring.Node) map[string]bool {
		set := map[string]bool{}
		for _, node := range nodes {
			if node.ID != oc.self.ID {
				set[node.ID] = true
			}
		}
		return set
	}
	for _, e := range entries {
		for _, state := range registryStates {
			t.Run(e.name+"/"+state.name, func(t *testing.T) {
				oc.recordBucketFactor(key, state.factor)
				defer oc.forgetBucketFactor(key)
				want, err := oc.replicasAtFactor(key, e.factor)
				if err != nil {
					t.Fatal(err)
				}
				wantOthers := ids(want)
				// A drop is routed to every member and waited for only at its
				// set (replication dispatch 10, R8-C): deliverySet is the
				// concern, deliverTo and the replay the route.
				wantRouted := wantOthers
				if transport.Opcode(e.entry.Op) == transport.OpDeleteBucket {
					wantRouted = ids(oc.Members())
				}

				live, err := oc.deliverySet(e.entry, key)
				if err != nil {
					t.Fatal(err)
				}
				if got := ids(live); fmt.Sprint(got) != fmt.Sprint(wantOthers) {
					t.Errorf("live delivery names %v, want %v", got, wantOthers)
				}
				routed, err := oc.deliverTo(transport.Opcode(e.entry.Op), e.entry.Bucket, e.entry.Payload)
				if err != nil {
					t.Fatal(err)
				}
				if got := ids(routed); fmt.Sprint(got) != fmt.Sprint(wantRouted) {
					t.Errorf("deliverTo names %v, want %v", got, wantRouted)
				}

				own := storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: oc.Epoch()}
				seq, err := oc.appendWAL(e.entry, own)
				if err != nil {
					t.Fatalf("append: %v", err)
				}
				walEntry := storage.WALEntry{Seq: seq, Op: uint8(e.entry.Op), Bucket: e.entry.Bucket, Data: e.entry.Payload, Origin: own}
				for _, node := range oc.Members() {
					if node.ID == oc.self.ID {
						continue
					}
					replayed, _, err := oc.replayableEntries([]storage.WALEntry{walEntry}, oc.peerFor(node))
					if err != nil {
						t.Fatal(err)
					}
					if got := len(replayed) == 1; got != wantRouted[node.ID] {
						t.Errorf("the replay addresses %s: %v, want %v", node.ID, got, wantRouted[node.ID])
					}
				}
			})
		}
	}

	// Database DDL has no bucket and goes to every other member; a bucket this
	// node does not own is somebody else's to deliver, at every factor.
	t.Run("database DDL is addressed to every other member", func(t *testing.T) {
		routed, err := oc.deliverTo(transport.OpCreateDB, "", mustMarshal(t, CreateDatabaseRequest{Name: "other"}))
		if err != nil {
			t.Fatal(err)
		}
		want := ids(oc.Members())
		if got := ids(routed); len(want) != 2 || fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("deliverTo names %v for a create database, want every other member %v", got, want)
		}
	})
	t.Run("a bucket this node does not own is addressed to nobody", func(t *testing.T) {
		elsewhere := bucketOwnedBy(t, tc, 1, "elsewhere")
		elsewhereKey := placementOf(testDatabase, elsewhere)
		oc.recordBucketFactor(elsewhereKey, 3)
		defer oc.forgetBucketFactor(elsewhereKey)
		for _, op := range []transport.Opcode{transport.OpPut, transport.OpCreateBucket, transport.OpDeleteBucket} {
			payload := mustMarshal(t, CreateBucketRequest{Database: testDatabase, Name: elsewhere, Type: BucketTypeHash, ReplicationFactor: 3})
			routed, err := oc.deliverTo(op, elsewhere, payload)
			if err != nil {
				t.Fatal(err)
			}
			if len(routed) != 0 {
				t.Errorf("deliverTo names %v for %s of a bucket node 2 owns, want nobody", ids(routed), op)
			}
		}
	})
}

// TestAClientCannotChooseWhoAStampedDropIsAddressedTo is verifier D-5. The
// stamp was appended as a second previous_factor key and the first was read, so
// a client's own value beat the owner's. The owner's stamp replaces it: a drop of
// a bucket declared at 3 is delivered to all three however the client stamped it.
func TestAClientCannotChooseWhoAStampedDropIsAddressedTo(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, "wide", 3)
	waitForServing(t, tc, testDatabase, "wide", 3)

	oc := tc.nodes[0].server.core.cluster
	key := placementOf(testDatabase, "wide")
	oc.recordBucketFactor(key, 3)
	forged := ReplEntry{Op: int32(transport.OpDeleteBucket), Bucket: "wide",
		Payload: mustMarshal(t, bson.D{{Key: "database", Value: testDatabase}, {Key: "name", Value: "wide"}, {Key: "previous_factor", Value: int32(1)}})}
	stamped, placedAt, err := oc.stampDrop(forged, key)
	if err != nil {
		t.Fatal(err)
	}
	if placedAt != 3 || previousFactorOf(stamped.Payload) != 3 {
		t.Fatalf("the owner stamped %d and the payload reads %d, want 3 in both", placedAt, previousFactorOf(stamped.Payload))
	}
	elements, err := bson.Raw(stamped.Payload).Elements()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, element := range elements {
		if element.Key() == "previous_factor" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("the stamped payload carries previous_factor %d times, want once", count)
	}

	// End to end: a client's drop that names a smaller set still reaches every copy.
	tc.ok(0, transport.OpDeleteBucket, bson.M{"database": testDatabase, "name": "wide", "previous_factor": int32(1)})
	waitForServing(t, tc, testDatabase, "wide", 0)
}

// TestACreateOverAnExistingBucketChangesNoNodesView is verifier N-1. entryFactor
// reads a create's factor from its payload and the owner computed its targets
// before the apply, so a second create of a name that exists, naming another
// factor, was shipped to the set at the REQUESTED factor: nodes outside the
// bucket's real set created an empty copy and answered reads from it. A create
// of a name that exists must ship nowhere and change no node's view; it
// answers, from the owner, that the bucket exists and keeps its stored factor,
// and names it.
func TestACreateOverAnExistingBucketChangesNoNodesView(t *testing.T) {
	cases := []struct {
		name               string
		nodeDefault        int
		existing, again    int
		wantKeptInMessage  string
		wantNotChangedNote bool
	}{
		{"default 3, exists at 1, again inheriting", 3, 1, 0, "", false},
		{"default 3, exists at 1, again at 3 (higher)", 3, 1, 3, "replication_factor 1", true},
		{"default 1, exists at 1, again at 3 (higher)", 1, 1, 3, "replication_factor 1", true},
		{"default 1, exists inheriting, again at 3 (higher)", 1, 0, 3, "no replication_factor of its own", true},
		{"default 3, exists at 2, again at 3 (higher)", 3, 2, 3, "replication_factor 2", true},
		{"default 3, exists at 3, again at 1 (lower)", 3, 3, 1, "replication_factor 3", true},
		{"default 3, exists at 2, again at 2 (equal)", 3, 2, 2, "", false},
		{"default 1, exists at 3, again inheriting", 1, 3, 0, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tc := newTestCluster(t, 3, c.nodeDefault)
			defer tc.stop()
			tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
			createBucketAt(tc, 0, testDatabase, "x", c.existing)
			putDocument(tc, 0, testDatabase, "x", "k")
			holds := c.existing
			if holds == 0 {
				holds = c.nodeDefault
			}
			waitForServing(t, tc, testDatabase, "x", holds)
			waitForLocalCopies(t, tc, testDatabase, "x", "k", holds)

			// The same answer whichever node the client asked.
			var messages []string
			for index := range tc.nodes {
				frame := tc.frame(index, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "x", Type: BucketTypeHash, ReplicationFactor: c.again})
				var resp CreateBucketResponse
				if err := bson.Unmarshal(frame.Payload, &resp); err != nil || frame.Opcode == transport.OpError || !resp.Success {
					t.Fatalf("a create over an existing bucket through node %d was refused (%s, %+v, %v)", index+1, frame.Opcode, resp, err)
				}
				messages = append(messages, resp.Message)
			}
			for _, message := range messages[1:] {
				if message != messages[0] {
					t.Fatalf("the answer depends on the node asked: %q against %q", messages[0], message)
				}
			}
			if !strings.Contains(messages[0], "already exists") {
				t.Fatalf("the answer does not say the bucket exists: %q", messages[0])
			}
			if c.wantNotChangedNote {
				for _, want := range []string{c.wantKeptInMessage, "not changed"} {
					if !strings.Contains(messages[0], want) {
						t.Fatalf("a create naming another factor does not say the bucket keeps its stored one (%q missing): %q", want, messages[0])
					}
				}
			}

			time.Sleep(3 * time.Second) // anything shipped by mistake has arrived
			open := 0
			for index, node := range tc.nodes {
				if node.server.core.bucketStoreOpen(testDatabase, "x") {
					open++
				}
				read := tc.frame(index, transport.OpGet, FindRequest{Database: testDatabase, Bucket: "x", Key: "k"})
				var found FindResponse
				if err := bson.Unmarshal(read.Payload, &found); err != nil || read.Opcode == transport.OpError || !found.Found {
					t.Errorf("a read of the document through node %d (holds the bucket %v) misses after the second create",
						index+1, node.server.core.bucketStoreOpen(testDatabase, "x"))
				}
				if creates := walCreates(t, node.server.core.wal, "x"); creates > 1 {
					t.Errorf("node %d logged %d creates of x: a create that changes nothing was logged", index+1, creates)
				}
			}
			if open != holds {
				t.Errorf("the bucket is open on %d nodes after the second create, it was on %d", open, holds)
			}
		})
	}
}

// TestTwoCreatesOfOneNameAtDifferentFactorsLeaveOneIncarnation closes the
// window between "the name does not exist yet" and the create's apply: the two
// creates are decided one after the other under the bucket's fence, the loser
// is answered from the winner's bucket and is not logged, so no node outside
// the winner's set ever holds a copy.
func TestTwoCreatesOfOneNameAtDifferentFactorsLeaveOneIncarnation(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})

	for round := 0; round < 8; round++ {
		name := fmt.Sprintf("race%02d", round)
		var wg sync.WaitGroup
		for index, factor := range []int{1, 3} {
			wg.Add(1)
			go func(index, factor int) {
				defer wg.Done()
				tc.frame(index, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash, ReplicationFactor: factor})
			}(index, factor)
		}
		wg.Wait()
		time.Sleep(500 * time.Millisecond)

		entry, owned := catalogFactorOnTheOwner(t, tc, name)
		if !owned {
			t.Fatalf("%s: no node holds either incarnation", name)
		}
		holders := 0
		for _, node := range tc.nodes {
			if node.server.core.bucketStoreOpen(testDatabase, name) {
				holders++
			}
		}
		want := int(entry)
		if want == 0 {
			want = 1
		}
		if holders != want {
			t.Fatalf("%s was won at factor %d and is held by %d nodes", name, want, holders)
		}
		created := 0
		for _, node := range tc.nodes {
			created += walCreates(t, node.server.core.wal, name)
		}
		if created > want {
			t.Fatalf("%s: %d creates were logged across the cluster, more than the %d copies its winner has", name, created, want)
		}
	}
}

// catalogFactorOnTheOwner returns the declared factor of a bucket in its
// owner's catalog.
func catalogFactorOnTheOwner(t *testing.T, tc *testCluster, name string) (int32, bool) {
	t.Helper()
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, name))
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	entry, catalogued, err := tc.nodes[nodeIndexByAddr(t, tc.addrs, owner.ID)].server.core.readCatalogEntry(testDatabase, name)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	return entry.ReplicationFactor, catalogued
}

// TestStampingADropAddsTwentyOneBytesAndAnOlderReaderIgnoresThem pins the wire
// and log change of stamping every drop: one BSON int32 element named
// previous_factor (1 type byte, 16 name bytes with the NUL, 4 value bytes), so
// the payload of a drop grows by exactly 21 bytes whatever the names (37 to 58
// for a drop of a one-letter bucket in a five-letter database, 39 to 60 here).
// A reader that predates the field decodes the same drop, because BSON
// decoding ignores an element it has no field for.
func TestStampingADropAddsTwentyOneBytesAndAnOlderReaderIgnoresThem(t *testing.T) {
	c := &Cluster{rf: 1, bucketFactors: map[placementKey]int32{}}
	plain := mustMarshal(t, DeleteBucketRequest{Database: "app", Name: "users"})
	stamped, placedAt, err := c.stampDrop(ReplEntry{Op: int32(transport.OpDeleteBucket), Bucket: "users", Payload: plain}, placementOf("app", "users"))
	if err != nil {
		t.Fatal(err)
	}
	if placedAt != 1 || len(stamped.Payload)-len(plain) != 21 {
		t.Fatalf("placed at %d; payload %d -> %d bytes, want placed at 1 and 21 bytes more", placedAt, len(plain), len(stamped.Payload))
	}
	var older struct {
		Database string `bson:"database"`
		Name     string `bson:"name"`
	}
	if err := bson.Unmarshal(stamped.Payload, &older); err != nil || older.Database != "app" || older.Name != "users" {
		t.Fatalf("a reader without the field decodes the stamped drop as %+v (%v)", older, err)
	}
}
