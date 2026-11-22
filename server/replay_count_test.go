package server

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// localCount asks one node how many entries it holds in the test bucket,
// reading that node's own store rather than a peer's.
func (tc *testCluster) localCount(index int) int64 {
	tc.t.Helper()
	frame := tc.ok(index, transport.OpCount, CountRequest{
		Database: testDatabase,
		Bucket:   testBucket,
	})
	var resp CountResponse
	if len(frame.Payload) > 0 {
		if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
			tc.t.Fatalf("decode count response from node %d: %v", index+1, err)
		}
	}
	return resp.Count
}

// restartNodeAndWait stops a node, starts it again on the same data directory,
// and returns the replacement. Startup replays the write-ahead log before the
// node accepts a client, so anything read afterwards is post-replay.
func (tc *testCluster) restartNode(index int) *testNode {
	tc.t.Helper()

	oldCfg := tc.nodes[index].cfg
	if tc.nodes[index].client != nil {
		_ = tc.nodes[index].client.Close()
		tc.nodes[index].client = nil
	}
	if err := tc.nodes[index].server.Stop(); err != nil {
		tc.t.Fatalf("stop node %d: %v", index+1, err)
	}
	tc.nodes[index].server = nil

	restarted := tc.startNode(index, filepath.Dir(oldCfg.Storage.DataDir), tc.token)
	tc.nodes[index] = restarted
	return restarted
}

// TestRestartDoesNotInflateEntryCount is the regression test for the count a
// crashed node reported after restarting: a replica holding 864 documents
// answered 1727.
//
// The mechanism was replay, not storage. A node's write-ahead log is replayed
// through the ordinary put path on every start, and the hash store's cached
// counter incremented on every put, including puts that overwrote a key that
// was already there. Replaying the whole log therefore roughly doubled the
// count while leaving the data correct — the worst kind of wrong number,
// because every read still answered properly.
//
// The contract this pins: restarting a node does not change what it reports,
// and the number it reports is the number of documents that were written.
func TestRestartDoesNotInflateEntryCount(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase,
		Name:     testBucket,
		Type:     BucketTypeHash,
	})

	const docs = 20
	for i := 0; i < docs; i++ {
		key := fmt.Sprintf("doc%02d", i)
		if frame := tc.writeConcerned(0, key, "majority"); frame.Opcode == transport.OpError {
			t.Fatalf("write %s at w=majority: %s", key, string(frame.Payload))
		}
	}

	// Restart a node that actually holds the bucket: a node with no copy of it
	// has an empty log for it and would prove nothing.
	replicas := mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, testBucket)
	restartIndex := nodeIndexByAddr(t, tc.addrs, replicas[0])

	before := tc.localCount(restartIndex)
	if before != docs {
		t.Fatalf("node %d counts %d entries before restart, want %d", restartIndex+1, before, docs)
	}

	// The writes must be durable in this node's log before it goes down, or the
	// restart would be a test of an empty log.
	waitForWALSequence(t, tc.nodes[restartIndex], docs)

	tc.restartNode(restartIndex)

	after := tc.localCount(restartIndex)
	if after != before {
		t.Fatalf("node %d counted %d entries before restart and %d after: replay changed what it reports",
			restartIndex+1, before, after)
	}
	if after != docs {
		t.Fatalf("node %d counts %d entries after restart, want the %d documents written", restartIndex+1, after, docs)
	}

	// The documents must still be readable, not just counted.
	for i := 0; i < docs; i++ {
		key := fmt.Sprintf("doc%02d", i)
		if found, message := tc.readFromNode(restartIndex, key); !found {
			t.Fatalf("document %s missing after restart: %s", key, message)
		}
	}
}

// TestKeylessInsertIsDeterministicAcrossReplicas pins what an insert without a
// key means in a cluster. The id has to be minted once, by the coordinator,
// before the entry is logged and shipped: the payload is what the owner, every
// replica and every replay applies, so an id minted deeper in the stack is
// minted again each time the entry is applied.
//
// Both halves of that were wrong before this test existed. Each replica stored
// the same document under its own id, so a document written through one node
// was missing by key from another; and a restart re-inserted every keyless
// document under a fresh id — measured on a real cluster, 5 documents became 10.
//
// Re-aimed by step 7.6 (D-2): each insert must be answered copies_confirmed 3 before every node is read.
func TestKeylessInsertIsDeterministicAcrossReplicas(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase,
		Name:     testBucket,
		Type:     BucketTypeHash,
	})

	// w=all, not majority: this test then reads the document back from *every*
	// node, and a majority write only guarantees two of the three have applied
	// it. Reading a replica that legitimately has not caught up yet is a race in
	// the test, not a bug in the server.
	const docs = 5
	ids := make([]string, 0, docs)
	for i := 0; i < docs; i++ {
		frame := tc.ok(0, transport.OpPut, InsertRequest{
			Database:     testDatabase,
			Bucket:       testBucket,
			Data:         map[string]interface{}{"n": i},
			WriteConcern: "all",
		})
		requireEveryCopy(t, frame, fmt.Sprintf("insert %d", i))
		var resp InsertResponse
		if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
			t.Fatalf("decode insert response: %v", err)
		}
		if resp.ID == "" {
			t.Fatalf("insert %d returned no id", i)
		}
		ids = append(ids, resp.ID)
	}

	replicas := mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, testBucket)

	// The document must be readable by its returned id from every node that
	// holds the bucket, not just from the node the client talked to.
	for _, id := range ids {
		for i := range tc.nodes {
			found, message := tc.readFromNode(i, id)
			if !found {
				t.Errorf("node %d cannot read the document inserted as %s: %s", i+1, id, message)
			}
		}
	}

	// Every holder counts the same number of documents.
	for _, id := range replicas {
		index := nodeIndexByAddr(t, tc.addrs, id)
		if got := tc.localCount(index); got != docs {
			t.Errorf("node %d counts %d entries, want %d", index+1, got, docs)
		}
	}

	// And a restart must not manufacture copies of them.
	restartIndex := nodeIndexByAddr(t, tc.addrs, replicas[0])
	waitForWALSequence(t, tc.nodes[restartIndex], docs)
	tc.restartNode(restartIndex)

	if got := tc.localCount(restartIndex); got != docs {
		t.Fatalf("node %d counts %d entries after a restart, want the %d documents written: replay duplicated them",
			restartIndex+1, got, docs)
	}
	for _, id := range ids {
		if found, message := tc.readFromNode(restartIndex, id); !found {
			t.Errorf("document %s missing from the restarted node: %s", id, message)
		}
	}
}

// TestQueueRestartDoesNotDuplicateElements is the regression test for the queue
// half of the replay problem, and it pins the rule in the rebalance-design record
// §5.2: a store that fsyncs per entry is already the log, so its ops must never
// be replayed.
//
// Before this, a restart re-applied the bucket's whole push/pop history on top
// of a store that already had those effects. Measured on a real cluster: 3
// pushes into a fifo bucket, one restart, 6 elements on that node.
//
// LIFO is covered by the same loop because the mechanism is identical — both are
// segment-backed, and both persisted their state before returning.
func TestQueueRestartDoesNotDuplicateElements(t *testing.T) {
	for _, storeType := range []BucketType{BucketTypeFIFO, BucketTypeLIFO} {
		t.Run(string(storeType), func(t *testing.T) {
			tc := newTestCluster(t, 3, 3)
			defer tc.stop()

			tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
			tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
				Database: testDatabase,
				Name:     testBucket,
				Type:     storeType,
			})

			const elements = 6
			for i := 0; i < elements; i++ {
				frame := tc.ok(0, transport.OpFIFOPush, PushRequest{
					Database:     testDatabase,
					Bucket:       testBucket,
					Data:         map[string]interface{}{"n": i},
					WriteConcern: "all",
				})
				if frame.Opcode == transport.OpError {
					t.Fatalf("push %d failed: %s", i, string(frame.Payload))
				}
			}

			replicas := mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, testBucket)
			restartIndex := nodeIndexByAddr(t, tc.addrs, replicas[0])

			before := tc.localCount(restartIndex)
			if before != elements {
				t.Fatalf("node %d holds %d elements before restart, want %d", restartIndex+1, before, elements)
			}
			waitForWALSequence(t, tc.nodes[restartIndex], elements)

			tc.restartNode(restartIndex)

			after := tc.localCount(restartIndex)
			if after != before {
				t.Fatalf("node %d held %d elements before restart and %d after: replay re-applied the queue's history",
					restartIndex+1, before, after)
			}
		})
	}
}

// appliedSnapshot returns a copy of the node's per-source applied positions.
func appliedSnapshot(c *Cluster) map[string]uint64 {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	out := make(map[string]uint64, len(c.appliedFrom))
	for source, seq := range c.appliedFrom {
		out[source] = seq
	}
	return out
}

// TestAppliedPositionSurvivesRestart covers the persistence half of the receive
// path: a node that has applied entries must still know it after a restart, or a
// sender whose own position reset will re-send them and a FIFO/LIFO bucket will
// silently gain elements.
func TestAppliedPositionSurvivesRestart(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase,
		Name:     testBucket,
		Type:     BucketTypeFIFO,
	})

	const elements = 4
	for i := 0; i < elements; i++ {
		if frame := tc.ok(0, transport.OpFIFOPush, PushRequest{
			Database:     testDatabase,
			Bucket:       testBucket,
			Data:         map[string]interface{}{"n": i},
			WriteConcern: "all",
		}); frame.Opcode == transport.OpError {
			t.Fatalf("push %d failed: %s", i, string(frame.Payload))
		}
	}

	// Pick a replica that is *not* the owner. The owner applies its own entries
	// locally and never receives them back, so it records no position for its
	// own stream; only a receiver does. Which member the ring picks as owner
	// depends on the random ports this test binds, so it has to be resolved
	// rather than assumed.
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	restartIndex := -1
	for _, id := range mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, testBucket) {
		if id != owner.ID {
			restartIndex = nodeIndexByAddr(t, tc.addrs, id)
			break
		}
	}
	if restartIndex == -1 {
		t.Fatalf("bucket %q has no replica other than its owner %s", testBucket, owner.ID)
	}
	waitForWALSequence(t, tc.nodes[restartIndex], elements)

	before := appliedSnapshot(tc.nodes[restartIndex].server.core.cluster)
	if len(before) == 0 {
		t.Fatal("node applied entries but recorded no positions")
	}

	tc.restartNode(restartIndex)

	after := appliedSnapshot(tc.nodes[restartIndex].server.core.cluster)
	for source, seq := range before {
		if after[source] != seq {
			t.Fatalf("applied position for source %s was %d before restart and %d after", source, seq, after[source])
		}
	}

	// The recorded position must also be in force, not merely loaded.
	cluster := tc.nodes[restartIndex].server.core.cluster
	for source, seq := range before {
		// A handshake records a position for every source that opened a
		// stream, at zero for one that has sent nothing yet, and nothing is
		// ever sent at sequence zero.
		if seq > 0 && cluster.shouldApply(source, seq) {
			t.Fatalf("node would re-apply sequence %d from %s after a restart", seq, source)
		}
	}
}

// TestDoubleRestartDoesNotRedeliverOldQueueEntries is the end-to-end version of
// the same bug, and it reproduces the only way to trigger it in production: both
// ends restarting.
//
// A restarted sender forgets how far each peer had acknowledged and replays from
// its log, and a restarted receiver used to forget what it had applied — so the
// old entries were applied a second time. For FIFO/LIFO that is destructive, not
// merely wasteful: the push lands twice and a replayed pop takes a different
// element than it originally took.
func TestDoubleRestartDoesNotRedeliverOldQueueEntries(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase,
		Name:     testBucket,
		Type:     BucketTypeFIFO,
	})

	const elements = 4
	for i := 0; i < elements; i++ {
		if frame := tc.ok(0, transport.OpFIFOPush, PushRequest{
			Database:     testDatabase,
			Bucket:       testBucket,
			Data:         map[string]interface{}{"n": i},
			WriteConcern: "all",
		}); frame.Opcode == transport.OpError {
			t.Fatalf("push %d failed: %s", i, string(frame.Payload))
		}
	}

	// Restart the owner and a replica that is not the owner. The owner replays
	// its log to its peers on startup; the replica has to recognise the entries
	// it already holds.
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	replicaIndex, ownerIndex := -1, -1
	for _, id := range mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, testBucket) {
		index := nodeIndexByAddr(t, tc.addrs, id)
		if id == owner.ID {
			ownerIndex = index
		} else if replicaIndex == -1 {
			replicaIndex = index
		}
	}
	if replicaIndex == -1 || ownerIndex == -1 {
		t.Fatalf("need one owner and one other replica, got owner=%d replica=%d", ownerIndex, replicaIndex)
	}

	before := tc.localCount(replicaIndex)
	if before != elements {
		t.Fatalf("replica %d holds %d elements before the restarts, want %d", replicaIndex+1, before, elements)
	}

	for _, index := range []int{replicaIndex, ownerIndex} {
		waitForWALSequence(t, tc.nodes[index], 1)
		tc.restartNode(index)
	}

	// Give the restarted owner time to replay its log into the restarted replica.
	// catch-up runs on the background peer loop; the count must not move.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(250 * time.Millisecond)
		if got := tc.localCount(replicaIndex); got != elements {
			t.Fatalf("replica %d held %d elements before the restarts and %d after: %d entries were re-delivered and re-applied",
				replicaIndex+1, before, got, got-elements)
		}
	}
}

// TestHeapRestartDoesNotDuplicateEntries is a cluster-level check that a heap
// bucket's count survives a restart. The discriminating test for the id rule
// lives at the store level (store/heap_test.go): the in-memory heap is rebuilt
// from Badger when a store opens, so a replayed push lands on top of an
// already-rebuilt entry and the count alone cannot show whether the *stored*
// element was overwritten or duplicated. This guards the end-to-end path anyway:
// every holder must agree, and a restart must not move the number.
func TestHeapRestartDoesNotDuplicateEntries(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase,
		Name:     testBucket,
		Type:     BucketTypeHeap,
	})

	const elements = 5
	for i := 0; i < elements; i++ {
		if frame := tc.ok(0, transport.OpHeapPush, PushRequest{
			Database:     testDatabase,
			Bucket:       testBucket,
			Data:         map[string]interface{}{"n": i},
			Priority:     int64(i),
			WriteConcern: "all",
		}); frame.Opcode == transport.OpError {
			t.Fatalf("heap push %d failed: %s", i, string(frame.Payload))
		}
	}

	// Every holder must count the same, and a restart must not change it.
	for _, id := range mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, testBucket) {
		index := nodeIndexByAddr(t, tc.addrs, id)
		before := tc.localCount(index)
		if before != elements {
			t.Fatalf("node %d holds %d heap entries, want %d", index+1, before, elements)
		}
		waitForWALSequence(t, tc.nodes[index], elements)

		tc.restartNode(index)

		if after := tc.localCount(index); after != before {
			t.Fatalf("node %d held %d heap entries before restart and %d after: replay added %d", index+1, before, after, after-before)
		}
	}
}

// waitForWALSequence blocks until the node's write-ahead log has recorded at
// least n entries, so a restart is guaranteed to have something to replay.
func waitForWALSequence(t *testing.T, n *testNode, want uint64) {
	t.Helper()
	wal := n.server.core.wal
	if wal == nil {
		t.Fatalf("node at %s has no write-ahead log; the restart would not replay anything", n.addr)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if wal.LastSequence() >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("node at %s logged %d entries, want at least %d before restart", n.addr, wal.LastSequence(), want)
}

// newReplayTestServer builds a bare server on dataDir with its own write-ahead
// log. logger is where the replay summary lands, which is the only place the
// applied/skipped counts are observable.
func newReplayTestServer(t *testing.T, dataDir string, logger *slog.Logger) *Server {
	t.Helper()

	s := &Server{
		config: &config.Config{
			Storage: config.StorageConfig{
				DataDir:            dataDir,
				BadgerBlockCacheMB: 64,
				BadgerIndexCacheMB: 64,
			},
			Encryption: config.EncryptionConfig{Mode: "off", Keystore: "off"},
		},
		logger:      logger,
		dataStore:   &serverData{dbs: make(map[string]*inMemoryDB)},
		kv:          sharedTestNodeStore(dataDir),
		authReqSeen: make(map[string]time.Time),
		knownNodes:  make(map[string]*NodeInfo),
	}

	wal, err := storage.OpenWAL(filepath.Join(dataDir, "wal"), nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	s.wal = wal
	return s
}

// closeReplayTestServer releases the log, so a second server can open the
// same data directory. Every bucket is a view of the node's store, which the
// test's shared store outlives, so there is nothing else to release.
func closeReplayTestServer(t *testing.T, s *Server) {
	t.Helper()

	if s.wal != nil {
		if err := s.wal.Close(); err != nil {
			t.Fatalf("closing the WAL: %v", err)
		}
	}
}

// appendReplayPut records one ordinary put in the log, the way a replicated
// write does, so a restart has something to replay.
func appendReplayPut(t *testing.T, s *Server, database, bucket, key string) {
	t.Helper()

	payload, err := bson.Marshal(InsertRequest{
		Database: database,
		Bucket:   bucket,
		Key:      key,
		Data:     map[string]interface{}{"n": 1},
	})
	if err != nil {
		t.Fatalf("marshal put payload: %v", err)
	}
	if _, err := s.wal.Append(bucket, uint8(transport.OpPut), []byte(key), payload); err != nil {
		t.Fatalf("appending to the WAL: %v", err)
	}
}

// TestReplayStillCountsOrdinaryFailures keeps the halt from being over-applied.
// A bucket that is simply gone — dropped, or created later in the log — is an
// ordinary failure: replay counts it, reports it, and carries on.
func TestReplayStillCountsOrdinaryFailures(t *testing.T) {
	dataDir := t.TempDir()
	var logged bytes.Buffer
	s := newReplayTestServer(t, dataDir,
		slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer closeReplayTestServer(t, s)

	appendReplayPut(t, s, "appdb", "neverexisted", "k1")

	if err := s.replayWALOnce(context.Background()); err != nil {
		t.Fatalf("an ordinary dispatch failure must not halt replay: %v", err)
	}

	summary := logged.String()
	if !strings.Contains(summary, "skipped=1") {
		t.Errorf("the entry must be counted as skipped, got: %s", summary)
	}
	if !strings.Contains(summary, "applied=0") {
		t.Errorf("a failed entry must not be counted as applied, got: %s", summary)
	}
}
