package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestEmptyMigrateKeyRefused proves that migratedValue reads
// record.Key[0] to tell a blob-metadata key record from an ordinary one, and
// without a length guard an empty or nil Key is an index-out-of-range panic
// rather than a returned error. The migration stream is internode-only and
// cluster-token-authenticated (OpMigrateStart/Data/Done), so an empty key
// reaching here is a damaged frame or an already-trusted-but-compromised
// peer — the target's pull must fail that entry, not crash the process.
func TestEmptyMigrateKeyRefused(t *testing.T) {
	t.Run("nil key is refused as a damaged stream, not a panic", func(t *testing.T) {
		_, err := migratedValue(migrateStreamRecord{Key: nil, Value: []byte("v")}, nil)
		if !errors.Is(err, errMigrateEmptyKey) {
			t.Fatalf("migratedValue(nil key) = %v, want errMigrateEmptyKey", err)
		}
	})

	t.Run("empty key is refused as a damaged stream, not a panic", func(t *testing.T) {
		_, err := migratedValue(migrateStreamRecord{Key: []byte{}, Value: []byte("v")}, nil)
		if !errors.Is(err, errMigrateEmptyKey) {
			t.Fatalf("migratedValue(empty key) = %v, want errMigrateEmptyKey", err)
		}
	})

	t.Run("an ordinary key still passes its value through unchanged", func(t *testing.T) {
		record := migrateStreamRecord{Key: []byte("users/42"), Value: []byte("payload")}
		value, err := migratedValue(record, nil)
		if err != nil {
			t.Fatalf("migratedValue(ordinary key): %v", err)
		}
		if string(value) != "payload" {
			t.Fatalf("migratedValue(ordinary key) = %q, want %q", value, "payload")
		}
	})

	t.Run("a blob-metadata key still routes to the payload-path branch", func(t *testing.T) {
		inlineMeta, err := bson.Marshal(bson.M{})
		if err != nil {
			t.Fatalf("marshal inline blob metadata: %v", err)
		}
		record := migrateStreamRecord{
			Key:   append([]byte{storage.NSBlobMeta}, []byte("blob-1")...),
			Value: inlineMeta,
		}
		value, err := migratedValue(record, nil)
		if err != nil {
			t.Fatalf("migratedValue(blob-meta key, inline content): %v", err)
		}
		if string(value) != string(inlineMeta) {
			t.Fatalf("migratedValue(blob-meta key) changed an inline (path-less) metadata value")
		}
	})
}

// unreadableMigrateAnswerConn answers every send with an OpMigrate frame whose
// payload is not valid BSON, standing in for a peer that answered something
// this build cannot decode.
type unreadableMigrateAnswerConn struct {
	stubPeerConn
}

func (u *unreadableMigrateAnswerConn) SendRequest(context.Context, transport.Lane, transport.Opcode, []byte) (*transport.Frame, error) {
	return &transport.Frame{Opcode: transport.OpMigrate, Payload: []byte{0x01}}, nil
}

// TestAnUnreadableMigrateAnswerSaysSo: a peer answers OpMigrate with a frame the
// asker cannot decode; askNodeToMigrate must name the node and say the answer
// could not be read, not report an empty refusal "node <id>: ". Guard: discard
// the decode error with `_ = bson.Unmarshal` — resp stays at its zero value and
// the call answers an empty refusal (BUG-0420).
func TestAnUnreadableMigrateAnswerSaysSo(t *testing.T) {
	asker := &Cluster{self: ring.Node{ID: "asker"}, peers: make(map[string]*peer)}
	member := ring.Node{ID: "member-1", Addr: "127.0.0.1:1"}
	asker.peerFor(member).installClient(&unreadableMigrateAnswerConn{})

	err := asker.askNodeToMigrate(context.Background(), member, testDatabase, testBucket)
	if err == nil {
		t.Fatal("an unreadable migrate answer reached the asker as a success")
	}
	if !strings.Contains(err.Error(), member.ID) {
		t.Errorf("an unreadable migrate answer reads %q, want it to name member %q", err, member.ID)
	}
	if !strings.Contains(err.Error(), "decoding") {
		t.Errorf("an unreadable migrate answer reads %q, want it to say the answer could not be decoded", err)
	}
}

// The seeding tests all need the same placement: a
// bucket with a live owner that gains a non-owner replica when a node joins.
// seededReplicaScenario names such a bucket of testDatabase, computed from the
// same ring the cluster builds (ring.New + AddNode, membership.go's
// rebuildRing) over its placement key (storage.PlacementKey), so a test
// creates exactly that bucket instead of creating many and hoping placement
// hands it one.
type seededReplicaScenario struct {
	bucket     string
	ownerIndex int // tc.addrs index of the owner, the same before and after the join
}

func findSeededReplicaScenario(t *testing.T, tc *testCluster, newcomer ring.Node, prefix string) seededReplicaScenario {
	t.Helper()
	before := ring.New(tc.rf)
	after := ring.New(tc.rf)
	for _, addr := range tc.addrs {
		member := ring.Node{ID: addr, Addr: addr}
		for _, r := range []*ring.Ring{before, after} {
			if err := r.AddNode(member); err != nil {
				t.Fatalf("add %s to the placement ring: %v", addr, err)
			}
		}
	}
	if err := after.AddNode(newcomer); err != nil {
		t.Fatalf("add newcomer %s to the placement ring: %v", newcomer.ID, err)
	}
	for i := 0; i < 64; i++ {
		name := fmt.Sprintf("%s%02d", prefix, i)
		key := string(storage.PlacementKey(testDatabase, name, nil))
		ownerBefore, err := before.Owner(key)
		if err != nil {
			t.Fatalf("owner of %s before the join: %v", name, err)
		}
		replicasAfter, err := after.Replicas(key, tc.rf)
		if err != nil {
			t.Fatalf("replicas of %s after the join: %v", name, err)
		}
		if replicasAfter[0].ID != ownerBefore.ID {
			continue
		}
		for _, r := range replicasAfter[1:] {
			if r.ID == newcomer.ID {
				return seededReplicaScenario{bucket: name, ownerIndex: nodeIndexByAddr(t, tc.addrs, ownerBefore.ID)}
			}
		}
	}
	t.Fatalf("no bucket %s00..%s63 keeps its owner and gains %s as a non-owner replica", prefix, prefix, newcomer.ID)
	return seededReplicaScenario{}
}

// joinLiveReplica proposes the node at ports into tc's cluster and starts it,
// with the background rebalance sweep disabled on the proposer (node 0, whose
// sweep would otherwise run on this goroutine) and on the newcomer, so the
// test drives the seeding steps itself in the order it is testing. prepare
// runs on the newcomer's Service before it starts (nil for none). The other
// original members keep their default and may ask the newcomer to pull in
// the background; ensureMigrated serializes those with the test's own pull.
func joinLiveReplica(t *testing.T, tc *testCluster, ports *testNodePorts, prepare func(*Service)) *testNode {
	t.Helper()
	return joinLiveReplicaBy(t, tc, ports, prepare, func(oc0 *Cluster, newcomer ring.Node) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return oc0.proposeJoin(ctx, newcomer)
	})
}

// joinLiveReplicaBy is joinLiveReplica with the way node 0 makes the newcomer
// a member: a test that needs a member a sealed node would refuse at join
// adopts the membership instead of proposing it. Node 0 must have been built
// with autoRebalanceOffOnNodeZero (or stricter): the flag is read by running
// goroutines, so it cannot be switched here.
func joinLiveReplicaBy(t *testing.T, tc *testCluster, ports *testNodePorts, prepare func(*Service), add func(oc0 *Cluster, newcomer ring.Node) error) *testNode {
	t.Helper()
	oc0 := tc.nodes[0].server.core.cluster
	if err := add(oc0, ports.ringNode()); err != nil {
		t.Fatalf("join: %v", err)
	}
	// A proposal stops asking voters once it has a majority, so on a
	// three-node cluster one member learns the new epoch only from a later
	// gossip round (up to 20 s away). Bring every original member to the
	// committed epoch now, the way that gossip would, so whichever node
	// placement made the owner already lists the newcomer.
	for i, n := range tc.nodes {
		if i == 0 {
			continue
		}
		if err := n.server.core.cluster.reconcileMembership(tc.addrs[0], oc0.memberNodes(), oc0.memberIDs(), oc0.Epoch()); err != nil {
			t.Fatalf("member %d resync after the join: %v", i+1, err)
		}
	}
	joiner := startJoinTargetPrepared(t, tc, ports, tc.token, prepare, func(cfg *config.Config) {
		cfg.Server.AutoRebalance = false
	})
	jc := joiner.server.core.cluster
	if err := jc.reconcileMembership(tc.addrs[0], oc0.memberNodes(), oc0.memberIDs(), oc0.Epoch()); err != nil {
		t.Fatalf("joiner resync: %v", err)
	}
	return joiner
}

// joinAndPullFirst joins the node at ports and pulls sc's bucket onto it from
// the owner before the owner's stream delivers it anything: the owner's
// handshake to the newcomer passes, and its entries wait until the pull has
// landed. A source serves a pull only over a stream that has opened in its
// process (A-4.2), so the pull can no longer be made before the newcomer
// listens; holding the entries is what keeps the ordering pull-first (SA-4.1).
func joinAndPullFirst(ctx context.Context, t *testing.T, tc *testCluster, ports *testNodePorts, sc seededReplicaScenario) *testNode {
	t.Helper()
	pulled := make(chan struct{})
	owner := tc.nodes[sc.ownerIndex].server.core.cluster
	joiner := joinLiveReplica(t, tc, ports, func(*Service) {
		newcomer, ok := owner.memberByID(ports.ringNode().ID)
		if !ok {
			t.Fatalf("the owner %s has not adopted the join of %s", owner.self.ID, ports.ringNode().ID)
		}
		wrapPeerConn(owner.peerFor(newcomer), func(inner peerConn) peerConn {
			return &heldStreamConn{peerConn: inner, entryRelease: pulled}
		})
	})
	defer close(pulled)
	// The way a replica is seeded, from its owner: another member's sweep may
	// ask the newcomer to pull the same bucket, and ensureMigrated is what
	// serialises the two.
	if err := joiner.server.core.cluster.ensureMigrated(ctx, testDatabase, sc.bucket); err != nil {
		t.Fatalf("snapshot pull onto the new replica: %v", err)
	}
	return joiner
}

// markPullPending records a snapshot pull of bucket as in flight on c, the
// state ensureMigrated keeps for a real pull; the returned func clears it.
func markPullPending(c *Cluster, bucket string) func() {
	key := migrateKey(testDatabase, bucket)
	pending := make(chan struct{})
	c.migrateMu.Lock()
	c.migrateActive[key] = pending
	c.migrateMu.Unlock()
	return func() {
		c.migrateMu.Lock()
		delete(c.migrateActive, key)
		close(pending)
		c.migrateMu.Unlock()
	}
}

// walEntriesForBucket counts the entries of w that name bucket. A replica
// appends every entry it applies from a peer to its own log first
// (handleReplPush), so on a replica the count is how many times the peer's
// history for the bucket has been applied there.
func walEntriesForBucket(t *testing.T, w *storage.WAL, bucket string) int {
	t.Helper()
	entries, err := w.ReadFrom(1)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	n := 0
	for _, e := range entries {
		if e.Bucket == bucket {
			n++
		}
	}
	return n
}

// localCounter reads field "n" of key from a node's own store, bypassing the
// cluster: it asks what this replica actually holds.
func localCounter(s *Server, bucket, key string) (int64, bool, error) {
	resp, err := s.Find(FindRequest{Database: testDatabase, Bucket: bucket, Key: key})
	if err != nil {
		return 0, false, err
	}
	if !resp.Found {
		return 0, false, nil
	}
	var doc struct {
		N interface{} `bson:"n"`
	}
	if err := bson.Unmarshal(resp.Data, &doc); err != nil {
		return 0, false, err
	}
	switch n := doc.N.(type) {
	case int32:
		return int64(n), true, nil
	case int64:
		return n, true, nil
	case float64:
		return int64(n), true, nil
	}
	return 0, false, fmt.Errorf("field n of %s is %T, not a number", key, doc.N)
}

// heapElementID returns the _id_ of a heap element carried in a pop or peek
// response document.
func heapElementID(t *testing.T, raw bson.Raw) string {
	t.Helper()
	var doc struct {
		ID string `bson:"_id_"`
	}
	if err := bson.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode heap element: %v", err)
	}
	return doc.ID
}

// The wire accepts a per-request write concern on any mutation (frameMeta
// reads "writeconcern"), but UpdateRequest, PopRequest and DeleteRequest carry
// no field for it. These shapes add it, so a test can require every original
// replica to hold an operation before a snapshot is taken.
type updateWithConcern struct {
	Database     string           `bson:"database"`
	Bucket       string           `bson:"bucket"`
	Key          string           `bson:"key"`
	Inc          map[string]int64 `bson:"inc"`
	WriteConcern string           `bson:"writeconcern"`
}

type popWithConcern struct {
	Database     string `bson:"database"`
	Bucket       string `bson:"bucket"`
	WriteConcern string `bson:"writeconcern"`
}

type deleteWithConcern struct {
	Database     string `bson:"database"`
	Bucket       string `bson:"bucket"`
	Key          string `bson:"key"`
	WriteConcern string `bson:"writeconcern"`
}

// TestSnapshotAndCatchUpDoNotRevertAReplica is the acceptance test for
// the pull-first ordering: a replica seeded by snapshot from a live owner must
// not have the bucket's history replayed over that snapshot by the owner's
// catch-up. Before the fix a reader on the newcomer saw its counter go
// 5 → 0 → 5 as the initial put and the five increments were re-applied; the
// replay is also visible durably, because a replica appends every entry it
// applies from a peer to its own WAL.
func TestSnapshotAndCatchUpDoNotRevertAReplica(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, autoRebalanceOffOnNodeZero)
	defer tc.stop()
	ports := reserveNodePorts(t)
	sc := findSeededReplicaScenario(t, tc, ports.ringNode(), "rev")

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(sc.ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: sc.bucket, Type: BucketTypeHash})
	tc.ok(sc.ownerIndex, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: sc.bucket, Key: "counter",
		Data: map[string]interface{}{"n": 0}, WriteConcern: "all",
	})
	const increments = 5
	for i := 0; i < increments; i++ {
		tc.ok(sc.ownerIndex, transport.OpUpdate, updateWithConcern{
			Database: testDatabase, Bucket: sc.bucket, Key: "counter",
			Inc: map[string]int64{"n": 1}, WriteConcern: "all",
		})
	}
	owner := tc.nodes[sc.ownerIndex].server.core
	if n, found, err := localCounter(owner, sc.bucket, "counter"); err != nil || !found || n != increments {
		t.Fatalf("owner holds n=%d found=%v err=%v before the join, want %d", n, found, err, increments)
	}

	// The snapshot is in place before the owner can ship or replay anything to
	// the newcomer: this is the pull-first ordering, forced.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	joiner := joinAndPullFirst(ctx, t, tc, ports, sc)
	if n, found, err := localCounter(joiner.server.core, sc.bucket, "counter"); err != nil || !found || n != increments {
		t.Fatalf("snapshot landed with n=%d found=%v err=%v, want %d", n, found, err, increments)
	}
	joinerWal := joiner.server.core.wal
	if joinerWal == nil {
		t.Fatal("joiner has no WAL")
	}

	// Watch the new replica's copy while the owner's catch-up runs: a reader
	// must never see the counter below what the snapshot delivered.
	var lowest atomic.Int64
	lowest.Store(increments)
	stop := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if n, found, err := localCounter(joiner.server.core, sc.bucket, "counter"); err == nil && found && n < lowest.Load() {
				lowest.Store(n)
			}
			time.Sleep(100 * time.Microsecond)
		}
	}()
	oc := tc.nodes[sc.ownerIndex].server.core.cluster
	catchUpErr := replayToPeer(ctx, oc.peerFor(mustNode(t, oc, joiner.addr)))
	close(stop)
	readers.Wait()
	if catchUpErr != nil {
		t.Fatalf("catch-up to the seeded replica: %v", catchUpErr)
	}
	if got := lowest.Load(); got < increments {
		t.Fatalf("a reader on the new replica saw n=%d after its snapshot delivered %d: catch-up replayed history over the snapshot", got, increments)
	}
	// A snapshot never passes through the receiver's log, and nothing was
	// written after it, so every entry of the bucket in the newcomer's WAL is
	// history the owner replayed over the snapshot — whichever catch-up round
	// did it.
	if replayed := walEntriesForBucket(t, joinerWal, sc.bucket); replayed != 0 {
		t.Fatalf("the owner replayed %d entries of bucket %q onto a replica whose snapshot already held them", replayed, sc.bucket)
	}
	if n, found, err := localCounter(joiner.server.core, sc.bucket, "counter"); err != nil || !found || n != increments {
		t.Fatalf("new replica holds n=%d found=%v err=%v after catch-up, want %d", n, found, err, increments)
	}
}

// TestSeededHeapReplicaDoesNotResurrectPoppedElements is the
// non-idempotent case of the same ordering. On a heap bucket the history push a(1), pop, push s(5)
// leaves {s}; replaying it over a snapshot of {s} pops s — the highest
// priority present — instead of a, and ends at {a, s}: the popped element
// resurrected on the replica, permanently. Unlike the counter above, this
// divergence is in the final state, not just in a reader's window.
func TestSeededHeapReplicaDoesNotResurrectPoppedElements(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, autoRebalanceOffOnNodeZero)
	defer tc.stop()
	ports := reserveNodePorts(t)
	sc := findSeededReplicaScenario(t, tc, ports.ringNode(), "heap")

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(sc.ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: sc.bucket, Type: BucketTypeHeap})
	tc.ok(sc.ownerIndex, transport.OpHeapPush, PushRequest{
		Database: testDatabase, Bucket: sc.bucket, Data: map[string]interface{}{"_id_": "a"}, Priority: 1, WriteConcern: "all",
	})
	popFrame := tc.ok(sc.ownerIndex, transport.OpHeapPop, popWithConcern{Database: testDatabase, Bucket: sc.bucket, WriteConcern: "all"})
	var popped PopResponse
	if err := bson.Unmarshal(popFrame.Payload, &popped); err != nil {
		t.Fatalf("decode pop response: %v", err)
	}
	if !popped.Found || heapElementID(t, popped.Items[0]) != "a" {
		t.Fatalf("pop took %v/%q, want a", popped.Found, heapElementID(t, popped.Items[0]))
	}
	tc.ok(sc.ownerIndex, transport.OpHeapPush, PushRequest{
		Database: testDatabase, Bucket: sc.bucket, Data: map[string]interface{}{"_id_": "s"}, Priority: 5, WriteConcern: "all",
	})

	// Pull-first, forced: the snapshot is in place before the newcomer can
	// receive anything (see TestSnapshotAndCatchUpDoNotRevertAReplica).
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	joiner := joinAndPullFirst(ctx, t, tc, ports, sc)

	oc := tc.nodes[sc.ownerIndex].server.core.cluster
	if err := replayToPeer(ctx, oc.peerFor(mustNode(t, oc, joiner.addr))); err != nil {
		t.Fatalf("catch-up to the seeded replica: %v", err)
	}

	count, err := joiner.server.core.Count(CountRequest{Database: testDatabase, Bucket: sc.bucket})
	if err != nil {
		t.Fatalf("count on the new replica: %v", err)
	}
	if count.Count != 1 {
		t.Fatalf("new replica holds %d heap elements after seeding, want 1: the popped element was resurrected by replay over the snapshot", count.Count)
	}
	peek, err := joiner.server.core.Peek(PeekRequest{Database: testDatabase, Bucket: sc.bucket})
	if err != nil || !peek.Found {
		t.Fatalf("peek on the new replica (found=%v err=%v)", peek != nil && peek.Found, err)
	}
	if id := heapElementID(t, peek.Items[0]); id != "s" {
		t.Fatalf("new replica's head element is %q, want s", id)
	}
}

// TestMigrateBucketAcrossNodes pulls a hash bucket from its owner to a node
// that did not hold it, over the real wire, and verifies the target serves the
// data while the source dropped its copy. RF=1 makes the bucket owned by
// exactly one node so the target genuinely lacks it.
func TestMigrateBucketAcrossNodes(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	targetIndex := -1
	for i, a := range tc.addrs {
		if a != owner.ID {
			targetIndex = i
			break
		}
	}
	if targetIndex < 0 {
		t.Fatal("no non-owner node to act as the migration target")
	}

	// Write the bucket's data on the owner: small keys plus one payload big
	// enough to force multi-chunk transfer.
	const small = 10
	keys := make([]string, 0, small)
	for i := 0; i < small; i++ {
		key := fmt.Sprintf("m%02d", i)
		keys = append(keys, key)
		tc.ok(ownerIndex, transport.OpPut, InsertRequest{
			Database: testDatabase, Bucket: testBucket, Key: key,
			Data: map[string]interface{}{"n": i},
		})
	}
	big := strings.Repeat("x", 9<<20) // > snapshotChunkSize
	tc.ok(ownerIndex, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: testBucket, Key: "big",
		Data: map[string]interface{}{"blob": big},
	})
	keys = append(keys, "big")

	for _, key := range keys {
		if found, _ := tc.readFromNode(ownerIndex, key); !found {
			t.Fatalf("owner missing %s before migration", key)
		}
	}

	// The target pulls the bucket from the owner over the wire.
	targetCluster := tc.nodes[targetIndex].server.core.cluster
	source := mustNode(t, targetCluster, owner.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := targetCluster.pullBucket(ctx, testDatabase, testBucket, source); err != nil {
		t.Fatalf("pullBucket: %v", err)
	}

	// The target now serves every document locally (this primitive moved the data,
	// not the placement — the ring still points at the former owner, so reads on
	// the target are answered from its own store).
	targetServer := tc.nodes[targetIndex].server.core
	for _, key := range keys {
		resp, err := targetServer.Find(FindRequest{Database: testDatabase, Bucket: testBucket, Key: key})
		if err != nil {
			t.Fatalf("target local find %s after migration: %v", key, err)
		}
		if !resp.Found {
			t.Fatalf("target missing %s after migration", key)
		}
	}
	bigResp, err := targetServer.Find(FindRequest{Database: testDatabase, Bucket: testBucket, Key: "big"})
	if err != nil || !bigResp.Found {
		t.Fatalf("target missing the multi-chunk payload (found=%v err=%v)", bigResp != nil && bigResp.Found, err)
	}

	// The source keeps its copy: this primitive moved the data, not the
	// placement, so the source is still the ring owner/replica and a legitimate
	// holder. The join scenario (where the source is no longer owner by the
	// time the pull completes) exercises the source-drop path.
	if !tc.nodes[ownerIndex].server.core.bucketStoreOpen(testDatabase, testBucket) {
		t.Fatal("source dropped a copy it should still hold as the ring owner")
	}
}

// TestMaterializedBucketUsesTheTargetsFloor is build doc SA-18: a bucket on
// the shared store migrates as its key range, and the range carries the
// source's state document, whose applied_seq is the source's WAL sequence. The
// target compares its own sequences against it, so materialize rewrites it to
// the target's floor — its log's last sequence — and keeps the count.
func TestMaterializedBucketUsesTheTargetsFloor(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	targetIndex := (ownerIndex + 1) % len(tc.addrs)
	for i := 0; i < 3; i++ {
		tc.ok(ownerIndex, transport.OpPut, InsertRequest{
			Database: testDatabase, Bucket: testBucket, Key: fmt.Sprintf("m%d", i),
			Data: map[string]interface{}{"n": i},
		})
	}
	sourceState := bucketStateOf(t, tc.nodes[ownerIndex].server.core, testDatabase, testBucket)

	target := tc.nodes[targetIndex].server.core
	// Put the target's log well past the source's, so the two floors differ.
	for i := 0; i < 5; i++ {
		if _, err := target.cluster.appendWAL(ReplEntry{Op: int32(transport.OpPut), Bucket: "elsewhere", Payload: mustMarshalPut(t, "elsewhere", "k")}, storage.WALOrigin{Kind: storage.WALOriginOwn}); err != nil {
			t.Fatalf("shifting the target's log: %v", err)
		}
	}
	targetFloor := target.wal.LastSequence()
	if targetFloor == sourceState.AppliedSeq {
		t.Fatalf("the target's floor equals the source's applied_seq (%d); the test cannot tell them apart", targetFloor)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := target.cluster.pullBucket(ctx, testDatabase, testBucket, mustNode(t, target.cluster, owner.ID)); err != nil {
		t.Fatalf("pullBucket: %v", err)
	}
	got := bucketStateOf(t, target, testDatabase, testBucket)
	if got.AppliedSeq != targetFloor {
		t.Fatalf("the materialized bucket's applied_seq is %d, want the target's floor %d (the source's is %d)", got.AppliedSeq, targetFloor, sourceState.AppliedSeq)
	}
	if got.Entries != sourceState.Entries || len(got.FailedSeqs) != 0 {
		t.Fatalf("the materialized state is entries %d, failed_seqs %v; want the source's %d entries and no failures", got.Entries, got.FailedSeqs, sourceState.Entries)
	}
}

// TestMigrateTriggerMakesJoinedNodeServe is the acceptance test for 1d: a node
// joins a live cluster (so it owns buckets it has no data for), then `migrate`
// pulls each of those buckets onto it and it serves them — and the fence that
// refused writes lifts once the data lands.
func TestMigrateTriggerMakesJoinedNodeServe(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, autoRebalanceOffOnNodeZero)
	defer tc.stop()
	// Drive migration explicitly (below) so this test is deterministic; the
	// background auto-rebalance sweep is disabled.

	const buckets = 8
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for i := 0; i < buckets; i++ {
		name := fmt.Sprintf("mig%02d", i)
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
		// Every original node holds the bucket's data (w=all).
		tc.frame(0, transport.OpPut, InsertRequest{
			Database: testDatabase, Bucket: name, Key: "seed",
			Data: map[string]interface{}{"i": i}, WriteConcern: "all",
		})
	}

	oc0 := tc.nodes[0].server.core.cluster
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := oc0.proposeJoin(ctx, newcomer); err != nil {
		t.Fatalf("join: %v", err)
	}

	joiner := startJoinTarget(t, tc, ports, tc.token)
	jc := joiner.server.core.cluster
	if err := jc.reconcileMembership(tc.addrs[0], oc0.memberNodes(), oc0.memberIDs(), oc0.Epoch()); err != nil {
		t.Fatalf("joiner resync: %v", err)
	}

	// Buckets this node now owns on the ring but does not hold.
	owned := []string{}
	for i := 0; i < buckets; i++ {
		name := fmt.Sprintf("mig%02d", i)
		if o, err := jc.Owner(placementOf(testDatabase, name)); err == nil && o.ID == newcomer.ID {
			owned = append(owned, name)
		}
	}
	if len(owned) == 0 {
		t.Skip("the joined node owns none of the created buckets; placement did not hand it one")
	}

	// Before migration, a write to an owned-but-empty bucket is fenced.
	payload, _ := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: owned[0], Key: "pre", Data: map[string]interface{}{"n": 0}})
	if f, _ := joiner.client.SendRequest(ctx, transport.OpPut, payload); f.Opcode == transport.OpGetResponse {
		t.Fatal("a write to an owned-but-empty bucket was served before migration")
	}

	// `migrate` each owned bucket; the joined node then serves it.
	for _, b := range owned {
		if err := jc.ensureMigrated(ctx, testDatabase, b); err != nil {
			t.Fatalf("migrate %s: %v", b, err)
		}
		resp, err := joiner.server.core.Find(FindRequest{Database: testDatabase, Bucket: b, Key: "seed"})
		if err != nil || !resp.Found {
			t.Fatalf("joined node does not serve %s after migrate (found=%v err=%v)", b, resp != nil && resp.Found, err)
		}
	}

	// The fence for the migrated bucket is gone: a write through the joined
	// node now lands.
	w, _ := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: owned[0], Key: "post", Data: map[string]interface{}{"n": 1}})
	if f, err := joiner.client.SendRequest(ctx, transport.OpPut, w); err != nil || f.Opcode == transport.OpError {
		t.Fatalf("write to a migrated bucket was refused (err=%v frame=%s)", err, f.Opcode)
	}
}

// migrateAnswer sends the operator's migrate verb to node index and decodes the
// reply. It retries while the pull is refused as premature, which a node that
// has just been told of the bucket can answer, and returns the first success.
func migrateAnswer(t *testing.T, tc *testCluster, index int) MigrateResponse {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		answer := tc.frame(index, transport.OpMigrate, MigrateRequest{Database: testDatabase, Bucket: testBucket})
		if answer.Opcode == transport.OpError {
			t.Fatalf("migrate was refused: %s", answerText(answer))
		}
		var migrated MigrateResponse
		if err := bson.Unmarshal(answer.Payload, &migrated); err != nil {
			t.Fatalf("decoding the migrate answer: %v", err)
		}
		if migrated.Success || time.Now().After(deadline) {
			return migrated
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestMigrateOnAnAlreadyHoldingNodeSaysNothingWasPulled: migrate on
// a node that is placed on the bucket and already holds it pulls nothing, and
// its reply says so instead of "is now served by" (on a follower: that it
// follows its owner's term), and names what to do for a replica that is
// behind. It is still a success: nothing is wrong with the node.
func TestMigrateOnAnAlreadyHoldingNodeSaysNothingWasPulled(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.frame(0, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: testBucket, Key: "seed",
		Data: map[string]interface{}{"i": 1}, WriteConcern: "all",
	})

	migrated := migrateAnswer(t, tc, 1)
	if !migrated.Success {
		t.Fatalf("migrate on a node that holds the bucket was refused: %s", migrated.Error)
	}
	if strings.Contains(migrated.Message, "now served by") {
		t.Errorf("migrate pulled nothing and said %q", migrated.Message)
	}
	// A follower's reply says that it follows its owner's term (BUG-0545)
	// before the advice for a replica that is behind; the owner's says
	// nothing was pulled.
	wants := []string{"already holds", "nothing was pulled", "count", "store", "wal"}
	if set, err := clusterOf(tc, 1).Replicas(placementOf(testDatabase, testBucket)); err != nil {
		t.Fatalf("placing %s: %v", testBucket, err)
	} else if set[0].ID != tc.addrs[1] {
		wants = []string{"already follows its owner's term", "nothing to do", "count", "store", "wal"}
	}
	for _, want := range wants {
		if !strings.Contains(migrated.Message, want) {
			t.Errorf("message %q does not contain %q", migrated.Message, want)
		}
	}
}

// TestMigrateOnANodeThatHoldsNothingStillSaysNowServed is the working case,
// unchanged: a joined node that owns a bucket it has no copy of pulls
// it, and the reply says it is now served.
func TestMigrateOnANodeThatHoldsNothingStillSaysNowServed(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, autoRebalanceOff)
	defer tc.stop()
	// No node pulls on its own: the members would otherwise tell the joiner to
	// migrate, and the verb would then correctly say it was already held.

	const buckets = 8
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for i := 0; i < buckets; i++ {
		name := fmt.Sprintf("mig%02d", i)
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
		tc.frame(0, transport.OpPut, InsertRequest{
			Database: testDatabase, Bucket: name, Key: "seed",
			Data: map[string]interface{}{"i": i}, WriteConcern: "all",
		})
	}
	oc0 := tc.nodes[0].server.core.cluster
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := oc0.proposeJoin(ctx, newcomer); err != nil {
		t.Fatalf("join: %v", err)
	}
	// No background sweep on the joiner: it would pull the bucket before the
	// verb is asked to, and the verb would then correctly say it held it.
	joiner := startJoinTarget(t, tc, ports, tc.token, func(cfg *config.Config) { cfg.Server.AutoRebalance = false })
	jc := joiner.server.core.cluster
	if err := jc.reconcileMembership(tc.addrs[0], oc0.memberNodes(), oc0.memberIDs(), oc0.Epoch()); err != nil {
		t.Fatalf("joiner resync: %v", err)
	}
	owned := ""
	for i := 0; i < buckets && owned == ""; i++ {
		name := fmt.Sprintf("mig%02d", i)
		if o, err := jc.Owner(placementOf(testDatabase, name)); err == nil && o.ID == newcomer.ID {
			owned = name
		}
	}
	if owned == "" {
		t.Skip("the joined node owns none of the created buckets; placement did not hand it one")
	}

	if joiner.server.core.bucketStoreOpen(testDatabase, owned) {
		t.Skip("the joined node was handed the bucket before the verb ran; nothing is left for migrate to pull")
	}
	payload, err := bson.Marshal(MigrateRequest{Database: testDatabase, Bucket: owned})
	if err != nil {
		t.Fatalf("marshal migrate: %v", err)
	}
	answer, err := joiner.client.SendRequest(ctx, transport.OpMigrate, payload)
	if err != nil || answer.Opcode == transport.OpError {
		t.Fatalf("migrate on the joined node = %v, %v", answer, err)
	}
	var migrated MigrateResponse
	if err := bson.Unmarshal(answer.Payload, &migrated); err != nil {
		t.Fatalf("decoding the migrate answer: %v", err)
	}
	if !migrated.Success || !strings.Contains(migrated.Message, "is now served by") {
		t.Errorf("migrate that pulled a bucket answered %+v, want success with \"is now served by\"", migrated)
	}
}

// TestRebalanceSweepMigratesEveryBucketAfterJoin proves a join is self-healing:
// one sweep on a node that knows the catalog migrates every bucket the newly
// joined owner lacks, with no per-bucket `migrate` from the operator. This is
// the automatic rebalancing the design promises (§6.4).
func TestRebalanceSweepMigratesEveryBucketAfterJoin(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, autoRebalanceOffOnNodeZero)
	defer tc.stop()
	// Deliberately drive the sweep below (single explicit call), so the
	// background auto-rebalance is disabled and this test is deterministic.

	const buckets = 8
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for i := 0; i < buckets; i++ {
		name := fmt.Sprintf("sweep%02d", i)
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
		tc.frame(0, transport.OpPut, InsertRequest{
			Database: testDatabase, Bucket: name, Key: "seed",
			Data: map[string]interface{}{"i": i}, WriteConcern: "all",
		})
	}

	oc0 := tc.nodes[0].server.core.cluster
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := oc0.proposeJoin(ctx, newcomer); err != nil {
		t.Fatalf("join: %v", err)
	}

	joiner := startJoinTarget(t, tc, ports, tc.token)
	jc := joiner.server.core.cluster
	if err := jc.reconcileMembership(tc.addrs[0], oc0.memberNodes(), oc0.memberIDs(), oc0.Epoch()); err != nil {
		t.Fatalf("joiner resync: %v", err)
	}
	// Every member learns the join now, as gossip would: a source serves a
	// pull only over its own stream to the puller (A-4.2), and a member that
	// has not heard of the joiner has none; gossip brings it within twenty
	// seconds, which the single sweep below does not wait for.
	tc.bringMembershipTo(t, oc0)

	owned := []string{}
	for i := 0; i < buckets; i++ {
		name := fmt.Sprintf("sweep%02d", i)
		if o, err := jc.Owner(placementOf(testDatabase, name)); err == nil && o.ID == newcomer.ID {
			owned = append(owned, name)
		}
	}
	if len(owned) == 0 {
		t.Skip("the joined node owns none of the created buckets; placement did not hand it one")
	}

	// No per-bucket migrate: a single sweep must rebalance them all.
	oc0.sweep(ctx)

	for _, b := range owned {
		resp, err := joiner.server.core.Find(FindRequest{Database: testDatabase, Bucket: b, Key: "seed"})
		if err != nil || !resp.Found {
			t.Fatalf("sweep did not migrate %s onto its owner (found=%v err=%v)", b, resp != nil && resp.Found, err)
		}
	}

	// The sweep must backfill replica positions, not just ownership: a join
	// re-keys the ring, so an existing bucket can now list the empty joiner as
	// a replica, and a majority write to it would stall on that empty copy.
	for i := 0; i < buckets; i++ {
		name := fmt.Sprintf("sweep%02d", i)
		inSet := false
		if replicas, err := jc.Replicas(placementOf(testDatabase, name)); err == nil {
			for _, r := range replicas {
				if r.ID == newcomer.ID {
					inSet = true
					break
				}
			}
		}
		if !inSet {
			continue
		}
		resp, err := joiner.server.core.Find(FindRequest{Database: testDatabase, Bucket: name, Key: "seed"})
		if err != nil || !resp.Found {
			t.Fatalf("sweep did not backfill %s to a new replica (found=%v err=%v)", name, resp != nil && resp.Found, err)
		}
	}
}

// The same-named-bucket tests put two same-named buckets in two databases on a
// two-node, RF=2 cluster, so both nodes hold both buckets, and seed one of
// them. These helpers are the only lines that name the seeded-position API,
// so the tests themselves read the same before and after the fix.

// recordSnapshotSeed records on replica c what Cluster.pullBucket records
// when a snapshot of database/bucket pulled from `from` lands at seq.
func recordSnapshotSeed(t *testing.T, c *Cluster, from, database, bucket string, seq uint64) {
	t.Helper()
	if err := c.recordSeeded(from, database, bucket, seq); err != nil {
		t.Fatalf("recording the snapshot position of %s/%s from %s: %v", database, bucket, from, err)
	}
}

// snapshotFloorFor returns the snapshot position replica c holds for an
// entry of database/bucket from `from`.
func snapshotFloorFor(c *Cluster, from, database, bucket string) uint64 {
	return c.seededThroughFor(from, database, bucket)
}

// sameNamedBucketCluster is a two-node, RF=2 cluster holding a hash bucket
// of one name in each of the databases, created through the buckets' owner
// and settled: the owner's stream has been applied on the replica and
// acknowledged by it through the last entry the owner logged. The name is one
// whose placement keys in every database share an owner — placement is by
// (database, bucket) so that is chance, not a given, and it is
// the precondition these tests need: both buckets streamed by one owner.
type sameNamedBucketCluster struct {
	tc      *testCluster
	owner   *testNode
	replica *testNode
	bucket  string
}

func newSameNamedBucketCluster(t *testing.T, databases ...string) *sameNamedBucketCluster {
	t.Helper()
	tc := newTestCluster(t, 2, 2)
	bucket, ownerID := sharedOwnerBucketName(t, tc.nodes[0].server.core.cluster, databases)
	ownerIndex := nodeIndexByAddr(t, tc.addrs, ownerID)
	sc := &sameNamedBucketCluster{tc: tc, owner: tc.nodes[ownerIndex], replica: tc.nodes[1-ownerIndex], bucket: bucket}
	for _, database := range databases {
		tc.ok(ownerIndex, transport.OpCreateDB, CreateDatabaseRequest{Name: database})
		tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeHash})
		// Shipping is in sequence order, so a write every replica holds is a
		// write after which the replica holds everything before it too.
		tc.ok(ownerIndex, transport.OpPut, InsertRequest{
			Database: database, Bucket: bucket, Key: "settle",
			Data: map[string]interface{}{"n": 1}, WriteConcern: "all",
		})
	}
	oc := sc.owner.server.core.cluster
	p := oc.peerFor(mustNode(t, oc, sc.replica.addr))
	deadline := time.Now().Add(20 * time.Second)
	for p.ackedThrough(oc.self.ID) < oc.s.wal.LastSequence() {
		if time.Now().After(deadline) {
			tc.stop()
			t.Fatalf("replica acknowledged %d of the owner's stream, want %d", p.ackedThrough(oc.self.ID), oc.s.wal.LastSequence())
		}
		time.Sleep(10 * time.Millisecond)
	}
	return sc
}

// sharedOwnerBucketName returns a bucket name, from n, n1, n2 …, whose
// placement keys in every one of databases have one owner, and that owner.
func sharedOwnerBucketName(t *testing.T, c *Cluster, databases []string) (string, string) {
	t.Helper()
	for i := 0; i < 256; i++ {
		name := "n"
		if i > 0 {
			name = fmt.Sprintf("n%d", i)
		}
		ownerID := ""
		shared := true
		for _, database := range databases {
			owner, err := c.Owner(placementOf(database, name))
			if err != nil {
				t.Fatalf("owner of %s/%s: %v", database, name, err)
			}
			if ownerID != "" && owner.ID != ownerID {
				shared = false
				break
			}
			ownerID = owner.ID
		}
		if shared {
			return name, ownerID
		}
	}
	t.Fatalf("no name in n..n255 has one owner in every database of %v", databases)
	return "", ""
}

// putPayload is the request body a client put of key into database/bucket
// carries, which is what a replicated entry's payload is.
func putPayload(t *testing.T, database, bucket, key string) []byte {
	t.Helper()
	payload, err := bson.Marshal(InsertRequest{Database: database, Bucket: bucket, Key: key, Data: map[string]interface{}{"n": 1}})
	if err != nil {
		t.Fatalf("marshal put of %s/%s/%s: %v", database, bucket, key, err)
	}
	return payload
}

// holdsKey reports whether a node's own store holds key in database/bucket.
func holdsKey(t *testing.T, s *Server, database, bucket, key string) bool {
	t.Helper()
	resp, err := s.Find(FindRequest{Database: database, Bucket: bucket, Key: key})
	if err != nil {
		t.Fatalf("find %s/%s/%s: %v", database, bucket, key, err)
	}
	return resp.Found
}

// TestSeededPositionOfOneDatabaseDoesNotSkipSameNamedBucketInAnother is
// The receiver-side test. A snapshot of db2/n seeded on the replica at
// A+10 covers db2/n's entries up to A+10 and nothing else. Before the fix the
// replica keyed that position by the bare name, so it skipped db1/n's entry
// at A+1 as held, never applied it, and still acknowledged it — the owner
// then counted a write the replica does not have as applied there.
func TestSeededPositionOfOneDatabaseDoesNotSkipSameNamedBucketInAnother(t *testing.T) {
	sc := newSameNamedBucketCluster(t, "db1", "db2")
	bucket := sc.bucket
	defer sc.tc.stop()
	ownerID := sc.owner.server.core.cluster.self.ID
	rc := sc.replica.server.core.cluster

	rc.appliedMu.Lock()
	appliedFromOwner := rc.appliedFrom[ownerID]
	rc.appliedMu.Unlock()
	recordSnapshotSeed(t, rc, ownerID, "db2", bucket, appliedFromOwner+10)

	push, err := bson.Marshal(ReplPushRequest{
		Token:    rc.localToken(),
		FromNode: ownerID,
		Epoch:    rc.Epoch(),
		Entries: []ReplEntry{
			{Seq: appliedFromOwner + 1, Op: int32(transport.OpPut), Bucket: bucket, Payload: putPayload(t, "db1", bucket, "x"),
				Term: bucketTermOn(t, sc.replica.server.core, "db1", bucket)},
			// db2/n's own history below its snapshot stays skipped.
			{Seq: appliedFromOwner + 2, Op: int32(transport.OpPut), Bucket: bucket, Payload: putPayload(t, "db2", bucket, "y"),
				Term: bucketTermOn(t, sc.replica.server.core, "db2", bucket)},
		},
		Stream: streamOf(t, rc, ownerID),
	})
	if err != nil {
		t.Fatalf("marshal replication push: %v", err)
	}
	respFrame, err := handleReplPush(context.Background(), transport.NewFrame(transport.OpReplPush, push), sc.replica.server.core)
	if err != nil {
		t.Fatalf("replication push: %v", err)
	}
	var resp ReplPushResponse
	if err := bson.Unmarshal(respFrame.Payload, &resp); err != nil {
		t.Fatalf("decode replication ack: %v", err)
	}
	if !resp.Success {
		t.Fatalf("replica rejected the push: %s", resp.Error)
	}
	if !holdsKey(t, sc.replica.server.core, "db1", bucket, "x") {
		t.Fatalf("replica acknowledged through %d but db1/%s lacks x: a snapshot of db2/%s made it skip db1/%s's entry at %d as already held",
			resp.AppliedThrough, bucket, bucket, bucket, appliedFromOwner+1)
	}
	if holdsKey(t, sc.replica.server.core, "db2", bucket, "y") {
		t.Fatalf("replica applied db2/%s's entry at %d below its own snapshot at %d: history replayed over the snapshot",
			bucket, appliedFromOwner+2, appliedFromOwner+10)
	}
}

// TestCatchUpShipsSameNamedBucketEntriesBelowAnotherDatabasesSnapshot is
// The owner-side test. The owner adopted a snapshot of db2/n on the
// replica at H; db1/n entries logged below H that the replica has not
// acknowledged must still be shipped, and the replica's position for
// retention must be its acknowledgement, not db2/n's snapshot. Before the fix
// the owner keyed the floor by the bare name: catch-up dropped the db1/n
// entries and peerNeed placed the replica at H, so retention could prune
// them and no horizon alarm would ever fire.
func TestCatchUpShipsSameNamedBucketEntriesBelowAnotherDatabasesSnapshot(t *testing.T) {
	sc := newSameNamedBucketCluster(t, "db1", "db2")
	bucket := sc.bucket
	defer sc.tc.stop()
	oc := sc.owner.server.core.cluster
	p := oc.peerFor(mustNode(t, oc, sc.replica.addr))

	// Logged, not shipped: this is the owner's stream holding db1/n writes the
	// replica has not received, the lag window the bug needs.
	db1Keys := []string{"k1", "k2", "k3"}
	var lastDB1 uint64
	// Logged as the owner's own, under its epoch, as applyAsOwner logs: an
	// entry with no origin ships only while its bucket is in term 0 (10b.4).
	own := storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: oc.Epoch()}
	for _, key := range db1Keys {
		seq, err := oc.s.wal.AppendFrom(bucket, byte(transport.OpPut), []byte(key), putPayload(t, "db1", bucket, key), own)
		if err != nil {
			t.Fatalf("log db1/%s/%s: %v", bucket, key, err)
		}
		lastDB1 = seq
	}
	snapshotAt, err := oc.s.wal.AppendFrom(bucket, byte(transport.OpPut), []byte("s"), putPayload(t, "db2", bucket, "s"), own)
	if err != nil {
		t.Fatalf("log db2/%s/s: %v", bucket, err)
	}
	// The replica records the snapshot's position, as a pull does; the owner
	// keeps none since the stream handshake (spec §19.3.3), so the replica's
	// skip is what keeps db2/n's history off its copy.
	recordSnapshotSeed(t, sc.replica.server.core.cluster, oc.self.ID, "db2", bucket, snapshotAt)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := replayToPeer(ctx, p); err != nil {
		t.Fatalf("catch-up to the replica: %v", err)
	}
	for _, key := range db1Keys {
		if !holdsKey(t, sc.replica.server.core, "db1", bucket, key) {
			t.Fatalf("catch-up did not ship db1/%s/%s: a snapshot of db2/%s at %d made the owner treat db1/%s's entries as held",
				bucket, key, bucket, snapshotAt, bucket)
		}
	}
	if holdsKey(t, sc.replica.server.core, "db2", bucket, "s") {
		t.Fatalf("catch-up shipped db2/%s's entry at %d over the replica's snapshot at %d", bucket, snapshotAt, snapshotAt)
	}
	acked := p.ackedThrough(oc.self.ID)
	if start := oc.replayStart(p); acked < lastDB1 || start != acked+1 {
		t.Fatalf("replica acknowledged %d (want at least %d) and a replay to it starts at %d, want %d: db1/%s's position is its acknowledgement, not db2/%s's snapshot at %d",
			acked, lastDB1, start, acked+1, bucket, bucket, snapshotAt)
	}
}

// TestLegacySeededPositionsLoadForEverySameNamedBucket is the upgrade
// rule. A replication state file written before the fix keys snapshot
// positions by bare bucket name, without saying which database's snapshot it
// was. Dropping such a position would let the owner's catch-up replay that
// bucket's history over its snapshot, so a legacy position loads
// as a floor for every database's bucket of that name — what the node did
// before the fix — and survives a restart.
func TestLegacySeededPositionsLoadForEverySameNamedBucket(t *testing.T) {
	dataDir := t.TempDir()
	const (
		source = "192.0.2.1:7000"
		bucket = "n"
		floor  = uint64(40)
	)
	legacyFile, err := bson.Marshal(replicationState{
		AppliedFrom:   map[string]uint64{source: 5},
		SeededThrough: map[string]map[string]uint64{source: {bucket: floor}},
	})
	if err != nil {
		t.Fatalf("marshal legacy state: %v", err)
	}
	cluster := newStateTestCluster(t, dataDir)
	if err := os.WriteFile(legacyStatePathOf(cluster), legacyFile, 0o644); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}

	for round, c := range []*Cluster{cluster, restartPositionTestCluster(t, cluster)} {
		if err := importLegacyStateOf(t, c); err != nil {
			t.Fatalf("round %d: import legacy state: %v", round, err)
		}
		if err := c.loadPositions(); err != nil {
			t.Fatalf("round %d: load legacy state: %v", round, err)
		}
		for _, database := range []string{"db1", "db2"} {
			if got := snapshotFloorFor(c, source, database, bucket); got != floor {
				t.Fatalf("round %d: legacy position for %s/%s = %d, want %d", round, database, bucket, got, floor)
			}
		}
		if got := snapshotFloorFor(c, source, "db1", "other"); got != 0 {
			t.Fatalf("round %d: legacy position of %q leaked onto bucket other: %d", round, bucket, got)
		}
		// The second round reads what the first one imported into the store.
	}
}

// TestQualifiedSeededPositionsSurviveARestartBesideLegacyOnes checks the
// persisted form of the qualified positions: a snapshot recorded for db2/n after
// an upgrade is written with its database, reads back as db2/n's alone, and
// leaves the legacy bare-name position it was recorded beside in force for
// every other database's n. A position that did not survive a restart is
// the double apply again: the owner's catch-up would replay db2/n over its snapshot.
func TestQualifiedSeededPositionsSurviveARestartBesideLegacyOnes(t *testing.T) {
	dataDir := t.TempDir()
	const (
		source      = "192.0.2.1:7000"
		bucket      = "n"
		legacyFloor = uint64(40)
		seededAt    = uint64(55)
	)
	legacyFile, err := bson.Marshal(replicationState{
		SeededThrough: map[string]map[string]uint64{source: {bucket: legacyFloor}},
	})
	if err != nil {
		t.Fatalf("marshal legacy state: %v", err)
	}
	upgraded := newStateTestCluster(t, dataDir)
	if err := os.WriteFile(legacyStatePathOf(upgraded), legacyFile, 0o644); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}
	if err := importLegacyStateOf(t, upgraded); err != nil {
		t.Fatalf("import legacy state: %v", err)
	}
	if err := upgraded.loadPositions(); err != nil {
		t.Fatalf("load legacy state: %v", err)
	}
	recordSnapshotSeed(t, upgraded, source, "db2", bucket, seededAt)

	restarted := restartPositionTestCluster(t, upgraded)
	if err := restarted.loadPositions(); err != nil {
		t.Fatalf("load state after the restart: %v", err)
	}
	for _, want := range []struct {
		database string
		floor    uint64
	}{
		{database: "db2", floor: seededAt},
		{database: "db1", floor: legacyFloor},
	} {
		if got := snapshotFloorFor(restarted, source, want.database, bucket); got != want.floor {
			t.Fatalf("after a restart the snapshot position of %s/%s = %d, want %d", want.database, bucket, got, want.floor)
		}
	}
}

// The seeded-replica release tests need a bucket whose RF 2 replica set is {owner, leaver}
// on three nodes and {owner, newcomer} once a fourth node joins — the owner
// stays and the newcomer replaces the other member. findSeededReplicaScenario
// finds a bucket that keeps its owner and gains the newcomer as its non-owner
// replica; at RF 2 that means the old non-owner replica leaves the set.
type leavingHolderScenario struct {
	tc          *testCluster
	bucket      string
	ownerIndex  int
	leaverIndex int
	joiner      *testNode
}

// leaverOf returns the tc.addrs index of the non-owner member of bucket's
// replica set on the cluster's original three members, built the way the
// cluster builds its ring (ring.New + AddNode over the placement key).
func leaverOf(t *testing.T, tc *testCluster, bucket string) int {
	t.Helper()
	before := ring.New(tc.rf)
	for _, addr := range tc.addrs {
		if err := before.AddNode(ring.Node{ID: addr, Addr: addr}); err != nil {
			t.Fatalf("add %s to the placement ring: %v", addr, err)
		}
	}
	replicas, err := before.Replicas(string(storage.PlacementKey(testDatabase, bucket, nil)), tc.rf)
	if err != nil {
		t.Fatalf("replicas of %s before the join: %v", bucket, err)
	}
	return nodeIndexByAddr(t, tc.addrs, replicas[1].ID)
}

// putEach writes each key into the scenario's bucket through its owner at
// w=all, then waits until every member of the replica set that serves the
// bucket holds it. Since D-2 `all` is answered by the copies in sync, so the
// answer alone no longer says a member whose stream is reopening holds it
// (re-aimed by replication-stream step 7.6).
func (sc leavingHolderScenario) putEach(keys ...string) {
	owner := sc.tc.nodes[sc.ownerIndex].server.core
	for _, key := range keys {
		sc.tc.ok(sc.ownerIndex, transport.OpPut, InsertRequest{
			Database: testDatabase, Bucket: sc.bucket, Key: key,
			Data: map[string]interface{}{"key": key}, WriteConcern: "all",
		})
		placed, err := owner.cluster.Replicas(placementOf(testDatabase, sc.bucket))
		if err != nil {
			sc.tc.t.Fatalf("the replica set of %s: %v", sc.bucket, err)
		}
		nodes := append([]*testNode{sc.joiner}, sc.tc.nodes...)
		deadline := time.Now().Add(20 * time.Second)
		for _, member := range placed {
			for _, node := range nodes {
				if node == nil || node.server == nil || node.addr != member.ID || !node.server.core.bucketStoreOpen(testDatabase, sc.bucket) {
					continue
				}
				for !holdsKeyIn(node, testDatabase, sc.bucket, key) {
					if time.Now().After(deadline) {
						sc.tc.t.Fatalf("%s, written at all, is not on %s, a member of %s's set that serves it, 20 s later", key, node.addr, sc.bucket)
					}
					time.Sleep(20 * time.Millisecond)
				}
			}
		}
	}
}

// sweepAll runs the rebalance sweep once on every original member, owner
// first, and on the joiner while it is still a member.
func (sc leavingHolderScenario) sweepAll(ctx context.Context, withJoiner bool) {
	order := []int{sc.ownerIndex, sc.leaverIndex}
	for i := range sc.tc.nodes {
		if i != sc.ownerIndex && i != sc.leaverIndex {
			order = append(order, i)
		}
	}
	for _, i := range order {
		sc.tc.nodes[i].server.core.cluster.sweep(ctx)
	}
	if withJoiner {
		sc.joiner.server.core.cluster.sweep(ctx)
	}
}

// leaveReplicaSet builds the scenario: the bucket is created and written at
// w=all on {owner, leaver}, a fourth node joins so the set becomes
// {owner, newcomer}, and every node sweeps.
func leaveReplicaSet(t *testing.T, ctx context.Context, prefix string, keys ...string) leavingHolderScenario {
	t.Helper()
	tc := newTestClusterWith(t, 3, 2, autoRebalanceOffOnNodeZero)
	t.Cleanup(tc.stop)
	ports := reserveNodePorts(t)
	seeded := findSeededReplicaScenario(t, tc, ports.ringNode(), prefix)
	sc := leavingHolderScenario{tc: tc, bucket: seeded.bucket, ownerIndex: seeded.ownerIndex, leaverIndex: leaverOf(t, tc, seeded.bucket)}

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(sc.ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: sc.bucket, Type: BucketTypeHash})
	sc.putEach(keys...)
	leaver := tc.nodes[sc.leaverIndex].server.core
	for _, key := range keys {
		if !holdsKey(t, leaver, testDatabase, sc.bucket, key) {
			t.Fatalf("precondition: the leaver (node %d) does not hold %q before the join", sc.leaverIndex+1, key)
		}
	}

	sc.joiner = joinLiveReplica(t, tc, ports, nil)
	if !tc.nodes[sc.ownerIndex].server.core.cluster.Hosts(placementOf(testDatabase, sc.bucket)) {
		t.Fatalf("precondition: node %d is not in %s's replica set after the join", sc.ownerIndex+1, sc.bucket)
	}
	if leaver.cluster.Hosts(placementOf(testDatabase, sc.bucket)) {
		t.Fatalf("precondition: node %d is still in %s's replica set after the join", sc.leaverIndex+1, sc.bucket)
	}
	sc.sweepAll(ctx, true)
	for _, key := range keys {
		if !holdsKey(t, sc.joiner.server.core, testDatabase, sc.bucket, key) {
			t.Fatalf("precondition: the sweep did not seed the newcomer with %q", key)
		}
	}
	return sc
}

// TestLeavingHolderReleasesItsCopy is the first half of the release rule. A node that
// leaves a bucket's replica set, and whose copy nobody pulled (the newcomer
// is seeded from the owner), must release that copy once every
// member of the set holds one. Before the fix only the source of a pull ever
// dropped its copy (handleMigrateDone), so the leaver kept it indefinitely.
func TestLeavingHolderReleasesItsCopy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sc := leaveReplicaSet(t, ctx, "leave", "k0", "k1", "k2")

	if !sc.tc.nodes[sc.ownerIndex].server.core.bucketStoreOpen(testDatabase, sc.bucket) {
		t.Fatalf("the owner (node %d) no longer holds %s", sc.ownerIndex+1, sc.bucket)
	}
	if sc.tc.nodes[sc.leaverIndex].server.core.bucketStoreOpen(testDatabase, sc.bucket) {
		t.Fatalf("node %d left %s/%s's replica set and every member holds a copy, but it still holds its own: "+
			"a copy may only exist on a node in the bucket's replica set", sc.leaverIndex+1, testDatabase, sc.bucket)
	}
}

// TestRejoiningReplicaIsReseededNotTrusted is the second half: the
// same node re-enters the set when the newcomer is removed, and must then hold
// every write made while it was out. Before the fix it kept its old copy,
// nothing re-seeded it (its store was open), and the owner's catch-up started
// above the missed entries — its per-peer acknowledgement had kept advancing
// on other buckets — so it served the bucket without them.
func TestRejoiningReplicaIsReseededNotTrusted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	sc := leaveReplicaSet(t, ctx, "rejoin", "k0", "k1", "k2")
	tc := sc.tc

	// Written while the leaver is outside the set: shipped to the owner and
	// the newcomer only.
	sc.putEach("out0", "out1", "out2")

	oc0 := tc.nodes[0].server.core.cluster
	if err := oc0.proposeRemoval(ctx, sc.joiner.addr); err != nil {
		t.Fatalf("remove the newcomer: %v", err)
	}
	for i, n := range tc.nodes {
		if i == 0 {
			continue
		}
		if err := n.server.core.cluster.reconcileMembership(tc.addrs[0], oc0.memberNodes(), oc0.memberIDs(), oc0.Epoch()); err != nil {
			t.Fatalf("member %d resync after the removal: %v", i+1, err)
		}
	}
	leaver := tc.nodes[sc.leaverIndex].server.core
	if !leaver.cluster.Hosts(placementOf(testDatabase, sc.bucket)) {
		t.Fatalf("precondition: node %d is not back in %s's replica set after the removal", sc.leaverIndex+1, sc.bucket)
	}
	sc.sweepAll(ctx, false)

	for _, key := range []string{"k0", "k1", "k2", "out0", "out1", "out2"} {
		if !holdsKey(t, leaver, testDatabase, sc.bucket, key) {
			t.Fatalf("node %d re-entered %s/%s's replica set and does not hold %q, written while it was out: "+
				"it serves a copy that missed those writes", sc.leaverIndex+1, testDatabase, sc.bucket, key)
		}
	}
	sc.putEach("back0")
	if !holdsKey(t, leaver, testDatabase, sc.bucket, "back0") {
		t.Fatalf("node %d does not hold a w=all write made after it re-entered the set", sc.leaverIndex+1)
	}
}

// frozenLayout is the placement of TestBackfillDoesNotSeedFromAFrozenCopy:
// the owner it removes, its buckets, each bucket's frozen holder, how many of
// them move to their frozen holder, and the ring once the owner is removed.
type frozenLayout struct {
	owner            int
	buckets          []string
	frozenHolder     map[string]string
	ownedByTheFrozen int
	removed          *ring.Ring
}

// frozenLayoutOf chooses the frozenLayout of founders at factor 2 joined by
// newcomer; ownedByTheFrozen is 0 when the ring has none.
func frozenLayoutOf(t *testing.T, founders []string, newcomer string) frozenLayout {
	t.Helper()
	before := ringWith(t, 2, founders...)
	joined := ringWith(t, 2, append(append([]string(nil), founders...), newcomer)...)
	// Every bucket has the one owner the test removes: the node whose
	// buckets the joiner becomes the second holder of.
	// The owner is fixed first, so the ring after its removal is known while
	// names are chosen: at least four of the twelve buckets have the frozen
	// holder as their new owner, which builds the condition whatever order the
	// random ports put the members in (a new owner that held nothing is
	// asserted only when the frozen holder is first in member order).
	layout := frozenLayout{owner: -1, frozenHolder: make(map[string]string)}
	for attempt := 0; attempt < 20000 && layout.owner < 0; attempt++ {
		key := placementOf(testDatabase, fmt.Sprintf("frozen%04d", attempt)).encoded
		old, _ := before.Replicas(key, 2)
		next, _ := joined.Replicas(key, 2)
		if old[0].ID == next[0].ID && next[1].ID == newcomer {
			layout.owner = nodeIndexByAddr(t, founders, old[0].ID)
		}
	}
	if layout.owner < 0 {
		return layout
	}
	var rest []string
	for index, addr := range founders {
		if index != layout.owner {
			rest = append(rest, addr)
		}
	}
	layout.removed = ringWith(t, 2, append(rest, newcomer)...)
	for attempt := 0; attempt < 20000 && (len(layout.buckets) < 12 || layout.ownedByTheFrozen < 4); attempt++ {
		name := fmt.Sprintf("frozen%04d", attempt)
		key := placementOf(testDatabase, name).encoded
		old, _ := before.Replicas(key, 2)
		next, _ := joined.Replicas(key, 2)
		if old[0].ID != founders[layout.owner] || next[0].ID != old[0].ID || next[1].ID != newcomer {
			continue
		}
		newOwner, _ := layout.removed.Owner(key)
		toTheFrozen := newOwner.ID == old[1].ID
		if !toTheFrozen && len(layout.buckets)-layout.ownedByTheFrozen >= 8 {
			continue
		}
		if toTheFrozen {
			layout.ownedByTheFrozen++
		}
		layout.buckets = append(layout.buckets, name)
		layout.frozenHolder[name] = old[1].ID
	}
	return layout
}

// TestBackfillDoesNotSeedFromAFrozenCopy is the frozen copy's seed half (rev 7 step
// 10.1, A-10.10 F-8 item 3): three nodes at factor 2 and a fourth joined, so
// that each bucket's set drops one holder, which keeps a frozen copy (its
// hand-off to the joiner is held); ten writes at all; then the owner is
// removed. For every bucket whose new owner is the frozen holder or held
// nothing, and whose first holder in member order is the frozen one, the new
// owner's copy holds the ten. Guard: take the first holder in member order.
func TestBackfillDoesNotSeedFromAFrozenCopy(t *testing.T) {
	tc := newTestCluster(t, 3, 2)
	defer tc.stop()
	shortenSweeps(t, 0)
	var layout frozenLayout
	ports := reserveJoinerPlaced(t, func(joiner string) bool {
		layout = frozenLayoutOf(t, tc.addrs, joiner)
		return layout.ownedByTheFrozen >= 4
	})
	newcomer := ports.ringNode()
	owner, buckets, frozenHolder, removed := layout.owner, layout.buckets, layout.frozenHolder, layout.removed
	others := []int{(owner + 1) % 3, (owner + 2) % 3}
	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, bucket := range buckets {
		tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, bucket, 2)
	}
	// The dropped holder's hand-off to the joiner is never confirmed, so it
	// keeps its copy; nothing ships to it once it is outside the set.
	refuse := func(fromID, memberID, database, bucket string) error {
		if memberID == newcomer.ID && frozenHolder[bucket] == fromID {
			return errors.New("test: the frozen holder's hand-off to the joiner is held")
		}
		return nil
	}
	askNodeToMigrateHook.Store(&refuse)
	defer askNodeToMigrateHook.Store(nil)
	joinFreezeOffHook.Store(true)
	defer joinFreezeOffHook.Store(false)
	joiner := joinNewcomer(t, tc, owner, ports)
	pollUntil(t, "the joiner holds every bucket", func() bool {
		for _, bucket := range buckets {
			if !joiner.node.server.core.bucketStoreOpen(testDatabase, bucket) {
				return false
			}
		}
		return true
	})
	for _, bucket := range buckets {
		for i := 0; i < 10; i++ {
			tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("w%d", i),
				Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
		}
		frozen := tc.nodes[nodeIndexByAddr(t, tc.addrs, frozenHolder[bucket])]
		if !frozen.server.core.bucketStoreOpen(testDatabase, bucket) || holdsKeyIn(frozen, testDatabase, bucket, "w0") {
			t.Fatalf("setup: the dropped holder of %s does not keep a frozen copy", bucket)
		}
	}

	proposer := clusterOf(tc, others[0])
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := proposer.proposeRemoval(ctx, tc.addrs[owner]); err != nil {
		t.Fatalf("removing the owner: %v", err)
	}
	for _, c := range []*Cluster{clusterOf(tc, owner), clusterOf(tc, others[1]), joiner.cluster()} {
		if err := c.reconcileMembership(proposer.self.ID, proposer.memberNodes(), proposer.memberIDs(), proposer.Epoch()); err != nil {
			t.Fatalf("bringing %s to the removal: %v", c.self.ID, err)
		}
	}
	nodes := map[string]*testNode{tc.addrs[others[0]]: tc.nodes[others[0]], tc.addrs[others[1]]: tc.nodes[others[1]], newcomer.ID: joiner.node}
	asserted := 0
	deadline := time.Now().Add(takeOverWait)
	for _, bucket := range buckets {
		set, _ := removed.Replicas(placementOf(testDatabase, bucket).encoded, 2)
		newOwnerID := set[0].ID
		if newOwnerID == newcomer.ID {
			continue
		}
		newOwner := nodes[newOwnerID]
		// The frozen holder as the new owner opens its own frozen copy on a
		// first-holder rule; a new owner that held nothing is seeded from the
		// frozen copy only when the frozen holder comes first in member order.
		first := frozenHolder[bucket]
		if newOwnerID != frozenHolder[bucket] {
			first = ""
			for _, member := range newOwner.server.core.cluster.Members() {
				if member.ID != newOwnerID && (member.ID == frozenHolder[bucket] || member.ID == newcomer.ID) {
					first = member.ID
					break
				}
			}
		}
		if first != frozenHolder[bucket] {
			continue
		}
		asserted++
		// A frozen holder serves its own copy from the start: the writes are
		// read once the new owner has taken the bucket over, or the shared
		// deadline has passed.
		takenOver := holdsWithin(time.Until(deadline), func() bool {
			core := newOwner.server.core
			if !core.bucketStoreOpen(testDatabase, bucket) {
				return false
			}
			_, opened, err := core.bucketLineage(testDatabase, bucket)
			return err == nil && opened && core.cluster.placementMarksOf(placementOf(testDatabase, bucket)).Moved == 0
		})
		if !takenOver {
			t.Errorf("%s, the new owner of %s, never took it over", newOwnerID, bucket)
		}
		var missing []string
		for i := 0; i < 10; i++ {
			if key := fmt.Sprintf("w%d", i); !holdsKeyIn(newOwner, testDatabase, bucket, key) {
				missing = append(missing, key)
			}
		}
		if len(missing) > 0 {
			t.Errorf("%s, the new owner of %s, serves a copy without %d of the 10 writes acknowledged at all (%v): it was seeded from the frozen copy of %s",
				newOwnerID, bucket, len(missing), missing, frozenHolder[bucket])
		}
	}
	if asserted < 4 {
		t.Fatalf("setup: %d buckets' new owner is the frozen holder or held nothing with the frozen holder first in member order, want at least 4", asserted)
	}
}

// TestMigrateDoneKeepsACopyOutsideTheSet is the frozen copy's delete half (rev 7
// step 10.1): the only complete copy is on the node being removed — its
// stream to the other holder is held through five writes at one — and the
// owner is removed while it runs. The new owner, a node that held nothing,
// holds the five, and the removed node's copy still exists after the pull.
// Guard: delete in handleMigrateDone.
func TestMigrateDoneKeepsACopyOutsideTheSet(t *testing.T) {
	tc := newTestCluster(t, 3, 2)
	defer tc.stop()
	const owner = 0
	before := ringWith(t, 2, tc.addrs...)
	after := ringWith(t, 2, tc.addrs[1], tc.addrs[2])
	var holder, nothing int
	bucket := bucketNamed(t, "complete", func(name string) bool {
		key := placementOf(testDatabase, name).encoded
		old, _ := before.Replicas(key, 2)
		next, _ := after.Replicas(key, 2)
		if old[0].ID != tc.addrs[owner] || nodeListed(old, next[0].ID) {
			return false
		}
		holder, nothing = nodeIndexByAddr(t, tc.addrs, old[1].ID), nodeIndexByAddr(t, tc.addrs, next[0].ID)
		return true
	})
	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 2)
	release := holdStreamPushesOnly(peerOf(t, tc, owner, holder))
	defer release()
	for i := 0; i < 5; i++ {
		tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("w%d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "one"})
	}
	if holdsKeyIn(tc.nodes[holder], testDatabase, bucket, "w0") {
		t.Fatal("setup: the other holder received the writes; its stream was not held")
	}
	removeAndTell(t, tc, nothing, owner)
	for _, index := range []int{holder} {
		via := clusterOf(tc, nothing)
		if err := clusterOf(tc, index).reconcileMembership(via.self.ID, via.memberNodes(), via.memberIDs(), via.Epoch()); err != nil {
			t.Fatalf("bringing node %d to the removal: %v", index+1, err)
		}
	}
	newOwner := tc.nodes[nothing]
	if !holdsWithin(takeOverWait, func() bool { return newOwner.server.core.bucketStoreOpen(testDatabase, bucket) }) {
		t.Fatalf("node %d, the new owner, never came to hold %s", nothing+1, bucket)
	}
	for i := 0; i < 5; i++ {
		if key := fmt.Sprintf("w%d", i); !holdsKeyIn(newOwner, testDatabase, bucket, key) {
			t.Errorf("node %d, the new owner, lacks %s: the only complete copy was on the removed node %d", nothing+1, key, owner+1)
		}
	}
	if !tc.nodes[owner].server.core.bucketStoreOpen(testDatabase, bucket) {
		t.Errorf("the removed node %d deleted its copy of %s after the pull", owner+1, bucket)
	}
}
