package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The follow-ups to the mixed-mode refusal: the three paths the
// first fix left open, by which a sealed node's document still reached a
// plaintext node's disk, the two regressions it introduced, and the tests that
// kill the mutants its own tests let live.

// bucketPlacedAfterJoin returns a bucket name of testDatabase whose replica set
// satisfies before on tc's ring and after once newcomer has joined it. It is
// computed from the same ring the cluster builds, so a test creates exactly the
// bucket a join re-homes the way it is about.
func bucketPlacedAfterJoin(t *testing.T, tc *testCluster, newcomer ring.Node, prefix string, before, after func(replicas []ring.Node) bool) string {
	t.Helper()
	ringBefore, ringAfter := ring.New(tc.rf), ring.New(tc.rf)
	for _, addr := range tc.addrs {
		member := ring.Node{ID: addr, Addr: addr}
		for _, r := range []*ring.Ring{ringBefore, ringAfter} {
			if err := r.AddNode(member); err != nil {
				t.Fatalf("add %s to the placement ring: %v", addr, err)
			}
		}
	}
	if err := ringAfter.AddNode(newcomer); err != nil {
		t.Fatalf("add newcomer %s to the placement ring: %v", newcomer.ID, err)
	}
	for i := 0; i < 512; i++ {
		name := fmt.Sprintf("%s%03d", prefix, i)
		key := placementOf(testDatabase, name).encoded
		replicasBefore, err := ringBefore.Replicas(key, tc.rf)
		if err != nil {
			t.Fatalf("placing %s before the join: %v", name, err)
		}
		replicasAfter, err := ringAfter.Replicas(key, tc.rf)
		if err != nil {
			t.Fatalf("placing %s after the join: %v", name, err)
		}
		if before(replicasBefore) && after(replicasAfter) {
			return name
		}
	}
	t.Fatalf("no bucket %s000..%s511 is re-homed the wanted way by the join of %s", prefix, prefix, newcomer.ID)
	return ""
}

// joinAndResync makes newcomer a member on the node at proposerIndex and
// brings every other started node of tc, and the joiner, to that epoch. It
// adopts the membership rather than proposing it: since the join freeze a
// sealed node refuses to add a plaintext joiner, and these tests are about
// what a plaintext member may read and pull once it is one — as a member that
// was added sealed and came back with an unencrypted store is.
func joinAndResync(t *testing.T, tc *testCluster, proposerIndex int, joiner *testNode, newcomer ring.Node) {
	t.Helper()
	proposer := tc.nodes[proposerIndex].server.core.cluster
	if err := addMemberUnasked(proposer, newcomer); err != nil {
		t.Fatalf("making %s a member: %v", newcomer.ID, err)
	}
	members := append([]*testNode{joiner}, tc.nodes...)
	for index, node := range members {
		if node == tc.nodes[proposerIndex] {
			continue
		}
		c := node.server.core.cluster
		if err := c.reconcileMembership(tc.addrs[proposerIndex], proposer.memberNodes(), proposer.memberIDs(), proposer.Epoch()); err != nil {
			t.Fatalf("resync of node %d after the join: %v", index, err)
		}
	}
}

// addMemberUnasked makes newcomer a member on c by adopting the next epoch,
// without the join's question to a sealed node: since the join freeze a node
// in encryption.mode=on refuses a plaintext newcomer, and one it cannot ask.
// The tests that use it are about a member that is plaintext or silent once
// it is one — as a member added sealed that came back with an unencrypted
// store, or went down, is.
func addMemberUnasked(c *Cluster, newcomer ring.Node) error {
	return c.adoptMembership(c.Epoch()+1, append(c.Members(), newcomer))
}

// markerWindow is the length of the marker fragment a scan looks for. The
// plaintext store compresses its blocks, and a compressor can replace a run at
// either edge of the marker with a back-reference, so a scan for the whole
// marker can miss a document that is there. Any eight-byte window of
// the marker is looked for instead: a chance match is 2^-64 per offset, and no
// compression can hide every window.
const markerWindow = 8

// markerWindowUnder reports the first file under dir that holds any
// markerWindow-byte window of plaintextMarker, and which bytes of the marker.
func markerWindowUnder(t *testing.T, dir string) (string, bool) {
	t.Helper()
	marker := []byte(plaintextMarker)
	var holder string
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || holder != "" {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		for offset := 0; offset+markerWindow <= len(marker); offset++ {
			if bytes.Contains(content, marker[offset:offset+markerWindow]) {
				holder = fmt.Sprintf("%s (marker bytes %d..%d)", path, offset, offset+markerWindow)
				return nil
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", dir, err)
	}
	return holder, holder != ""
}

// TestMarkerScanFindsAPlaintextDocument is the positive control for every
// assertion in the mixed-mode tests that a plaintext node holds no marker: on a
// plaintext node that does hold the document, both scans find it — while the
// node runs, where it is in the memtable's log, and after a stop, where it is
// in a compressed table. A scan that went blind would fail here, and nowhere
// else.
func TestMarkerScanFindsAPlaintextDocument(t *testing.T) {
	tc := newMixedCluster(t, 2, "", "")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "control", Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: "control", Key: "k", Data: markerDocument(), WriteConcern: "all"})

	for index, node := range tc.nodes {
		if _, found := markerUnder(t, node.cfg.Storage.DataDir); !found {
			t.Errorf("the whole-marker scan does not find the document on running plaintext node %d", index+1)
		}
		if _, found := markerWindowUnder(t, node.cfg.Storage.DataDir); !found {
			t.Errorf("the window scan does not find the document on running plaintext node %d", index+1)
		}
	}

	replicaDir := tc.nodes[1].cfg.Storage.DataDir
	stopNode(t, tc, 1)
	where, found := markerWindowUnder(t, replicaDir)
	if !found {
		t.Fatalf("the window scan does not find the document on a stopped plaintext node: the absence assertions prove nothing")
	}
	t.Logf("stopped plaintext node holds the marker in %s", where)
}

// TestSealedReplicaDoesNotAnswerAQuorumReadFromAPlaintextOwner is the
// verifier's first leak. A plaintext node joins three sealed ones and the ring
// makes it the owner of a bucket they hold. It holds nothing of the bucket. A
// quorum read coordinated by it asks the sealed replicas for the key, finds one
// of them stale, and repairs through itself: the sealed nodes' document lands in
// its write-ahead log.
func TestSealedReplicaDoesNotAnswerAQuorumReadFromAPlaintextOwner(t *testing.T) {
	tc := newMixedCluster(t, 3, "on", "on", "on")
	defer tc.stop()
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	bucket := bucketPlacedAfterJoin(t, tc, newcomer, "quorum",
		func([]ring.Node) bool { return true }, ownedBy(newcomer.ID))

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: markerDocument(), WriteConcern: "all"})

	joiner := startJoinTarget(t, tc, ports, tc.token, func(cfg *config.Config) { cfg.Server.AutoRebalance = false })
	joinAndResync(t, tc, 0, joiner, newcomer)
	joined := joiner.server.core.cluster
	replicas, err := joined.Replicas(placementOf(testDatabase, bucket))
	if err != nil || replicas[0].ID != newcomer.ID {
		t.Fatalf("test setup: the plaintext node must own %q after the join: %v, %v", bucket, nodeIDs(replicas), err)
	}
	if where, found := markerWindowUnder(t, joiner.cfg.Storage.DataDir); found {
		t.Fatalf("test setup: the plaintext node already holds the marker, in %s", where)
	}

	// One sealed replica of the new set loses the key, so the read finds a
	// holder to repair.
	staleID := replicas[len(replicas)-1].ID
	stale := tc.nodes[nodeIndexByAddr(t, tc.addrs, staleID)]
	if _, err := stale.server.core.Delete(DeleteRequest{Database: testDatabase, Bucket: bucket, Key: "k"}); err != nil {
		t.Fatalf("making %s stale: %v", staleID, err)
	}

	payload, err := bson.Marshal(ConsistentGetRequest{Database: testDatabase, Bucket: bucket, Key: "k"})
	if err != nil {
		t.Fatalf("encoding the consistent get: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	answer, err := joiner.client.SendRequest(ctx, transport.OpConsistentGet, payload)
	if err != nil {
		t.Fatalf("consistent get through the plaintext owner: %v", err)
	}
	if bytes.Contains(answer.Payload, []byte(plaintextMarker)) {
		t.Errorf("the plaintext owner answered the quorum read with the sealed nodes' document")
	}
	// The repair is a write through the owner's own log; give it the time the
	// verifier's probe did.
	time.Sleep(500 * time.Millisecond)
	if where, found := markerWindowUnder(t, joiner.cfg.Storage.DataDir); found {
		t.Errorf("the plaintext ring owner %s holds a sealed node's document in the clear, in %s", newcomer.ID, where)
	}
}

// TestSealedNodeRefusesAVersionedReadFromAPlaintextPeer is that leak at its
// door: the node-to-node versioned read a quorum read is made of. A sealed node
// must not answer it to a peer that is not known to be sealed, and says why.
func TestSealedNodeRefusesAVersionedReadFromAPlaintextPeer(t *testing.T) {
	tc := newMixedCluster(t, 2, "on", "")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	const bucket = "versioned"
	createLocally(t, tc.nodes[0], bucket)
	createLocally(t, tc.nodes[1], bucket)
	if _, err := tc.nodes[0].server.core.Insert(InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: markerDocument()}); err != nil {
		t.Fatalf("writing the key on the sealed node directly: %v", err)
	}

	plaintext := tc.nodes[1].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	found, _, document, err := plaintext.readVersionedFromPeer(ctx, mustNode(t, plaintext, tc.addrs[0]),
		ConsistentGetRequest{Database: testDatabase, Bucket: bucket, Key: "k"})
	if err == nil {
		t.Fatalf("a sealed node answered a plaintext peer's versioned read (found=%v, %d bytes)", found, len(document))
	}
	t.Logf("versioned read refused with: %v", err)
	assertPlaintextPeerRefusal(t, "versioned read", err.Error(), tc.addrs[1])
}

// TestSealedOwnerDoesNotServeABucketThroughAPlaintextCoordinator: a plaintext
// member that coordinates a client's request for a bucket a sealed node owns is
// handed the bucket's data to pass on. The sealed owner refuses the read, the
// dump and the write alike, and tells the client which node to connect to.
func TestSealedOwnerDoesNotServeABucketThroughAPlaintextCoordinator(t *testing.T) {
	tc := newMixedCluster(t, 1, "on", "")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketPlaced(t, tc.nodes[0].server.core.cluster, "served", ownedBy(tc.addrs[0]))
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: markerDocument()})

	for _, forwarded := range []struct {
		name    string
		op      transport.Opcode
		request any
	}{
		{name: "get", op: transport.OpGet, request: FindRequest{Database: testDatabase, Bucket: bucket, Key: "k"}},
		{name: "dump", op: transport.OpDumpStart, request: DumpStartRequest{Database: testDatabase, Bucket: bucket}},
		{name: "put", op: transport.OpPut, request: InsertRequest{Database: testDatabase, Bucket: bucket, Key: "other", Data: map[string]interface{}{"n": 1}}},
	} {
		t.Run(forwarded.name, func(t *testing.T) {
			answer := tc.frame(1, forwarded.op, forwarded.request)
			if bytes.Contains(answer.Payload, []byte(plaintextMarker)) {
				t.Errorf("the sealed owner handed the bucket's document to the plaintext coordinator %s", tc.addrs[1])
			}
			if answer.Opcode != transport.OpError {
				t.Fatalf("the sealed owner served %s for its bucket through the plaintext coordinator %s", forwarded.op, tc.addrs[1])
			}
			text := printable(string(answer.Payload))
			t.Logf("%s through a plaintext coordinator refused with: %s", forwarded.op, text)
			for _, want := range []string{tc.addrs[1], "is not encrypted", tc.addrs[0], "remove-node"} {
				if !strings.Contains(text, want) {
					t.Errorf("the refusal does not say %q", want)
				}
			}
		})
	}
	if locallyFound(t, tc.nodes[0], bucket, "other") {
		t.Errorf("the refused forwarded write was applied on the sealed owner")
	}

	// A bucket drop is served: it carries names only and is the way a bucket is
	// removed whichever node the operator is connected to.
	tc.ok(1, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: bucket})
	if tc.nodes[0].server.core.bucketStoreOpen(testDatabase, bucket) {
		t.Errorf("a bucket drop through the plaintext coordinator did not drop the bucket on its sealed owner")
	}
}

// TestQueuedEntriesAreNotShippedToAPeerThatReturnsPlaintext is the verifier's
// second leak. A sealed replica goes down with the owner's entries queued for
// it, and its host comes back under the same identity with a wiped data
// directory and encryption off. The owner's record of it says sealed — from
// before, and from membership.json — and its sender re-sent the queue to the
// new connection without asking again.
func TestQueuedEntriesAreNotShippedToAPeerThatReturnsPlaintext(t *testing.T) {
	var modeMu sync.Mutex
	replicaPlaintext := false
	tc := newTestClusterWith(t, 2, 2, func(index int, cfg *config.Config) {
		cfg.Server.AutoRebalance = false
		modeMu.Lock()
		defer modeMu.Unlock()
		if index == 1 && replicaPlaintext {
			return
		}
		sealNode(t, cfg, "on", byte(0x40+0x10*index))
	})
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	deadline := time.Now().Add(15 * time.Second)
	for {
		record, found, err := readMembershipRecord(owner.membershipPath())
		if err == nil && found && len(record.SealedPeers) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("test setup: the owner never recorded its replica as sealed")
		}
		time.Sleep(50 * time.Millisecond)
	}
	bucket := bucketPlaced(t, owner, "queued", ownedBy(tc.addrs[0]))
	replicaDir := tc.nodes[1].cfg.Storage.DataDir
	// A sealed key belongs to its node and a plaintext start refuses it, so the
	// TLS identity the re-provisioned host keeps is the key as the operator
	// holds it in plaintext: opened here while the sealed node still has its
	// master, written back below.
	identityKey, wasSealed, err := CredentialKeys{nodeMaster: tc.nodes[1].server.core.nodeMaster, seal: true}.readCredential(replicaDir, tlsKeyCredential)
	if err != nil || !wasSealed {
		t.Fatalf("test setup: opening the sealed replica's TLS key (sealed: %v): %v", wasSealed, err)
	}
	stopNode(t, tc, 1)

	// Everything the returning node would need is queued for it while it is
	// down: the database, the bucket and the document. The create cannot reach
	// its write concern and says so; it is applied and queued all the same.
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: markerDocument(), WriteConcern: "one"})

	// The host is re-provisioned: its store, keys, sealed credential files and
	// membership go, and it starts with encryption off under the same TLS
	// identity (restored in plaintext below) and the same cluster token, which
	// startNode hands it as an operator would through the environment.
	entries, err := os.ReadDir(replicaDir)
	if err != nil {
		t.Fatalf("reading the replica's data directory: %v", err)
	}
	for _, entry := range entries {
		switch entry.Name() {
		case "keys":
			keyFiles, err := os.ReadDir(filepath.Join(replicaDir, "keys"))
			if err != nil {
				t.Fatalf("reading the replica's keys: %v", err)
			}
			for _, keyFile := range keyFiles {
				if strings.HasPrefix(keyFile.Name(), "server.") && keyFile.Name() != "server.key" {
					continue
				}
				if err := os.RemoveAll(filepath.Join(replicaDir, "keys", keyFile.Name())); err != nil {
					t.Fatalf("wiping %s: %v", keyFile.Name(), err)
				}
			}
		default:
			if err := os.RemoveAll(filepath.Join(replicaDir, entry.Name())); err != nil {
				t.Fatalf("wiping %s: %v", entry.Name(), err)
			}
		}
	}
	if err := os.WriteFile(filepath.Join(replicaDir, "keys", "server.key"), identityKey, 0o600); err != nil {
		t.Fatalf("restoring the replica's TLS identity in plaintext: %v", err)
	}
	modeMu.Lock()
	replicaPlaintext = true
	modeMu.Unlock()
	tc.nodes[1] = tc.startNode(1, filepath.Dir(replicaDir), tc.token)
	if tc.nodes[1].server.core.nodeMaster != nil {
		t.Fatalf("test setup: the returning node must be plaintext")
	}

	// The owner's sender retries every half second and its gossip loop every
	// two while the peer was down; five seconds covers both several times over.
	watchUntil := time.Now().Add(5 * time.Second)
	for time.Now().Before(watchUntil) {
		if where, found := markerWindowUnder(t, replicaDir); found {
			t.Fatalf("a sealed node's queued document was shipped to a peer that came back plaintext: %s (bucket open there: %v)",
				where, tc.nodes[1].server.core.bucketStoreOpen(testDatabase, bucket))
		}
		time.Sleep(20 * time.Millisecond)
	}
	if tc.nodes[1].server.core.bucketStoreOpen(testDatabase, bucket) {
		t.Errorf("the returning plaintext node holds bucket %q", bucket)
	}
}

// TestEmptyBucketMutationIsNotBroadcast is the verifier's third leak. A
// mutation that names a database and no bucket took the path meant for
// database DDL: it was logged and sent to every member before the handler
// refused it, so its payload reached a plaintext member's write-ahead log.
func TestEmptyBucketMutationIsNotBroadcast(t *testing.T) {
	tc := newMixedCluster(t, 1, "on", "")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	sealedLog := tc.nodes[0].server.core.wal
	logged := sealedLog.LastSequence()

	for _, mutation := range []struct {
		name    string
		op      transport.Opcode
		request any
	}{
		{name: "put", op: transport.OpPut, request: InsertRequest{Database: testDatabase, Bucket: "", Key: "k", Data: markerDocument()}},
		{name: "batch put", op: transport.OpBatchPut, request: BatchInsertRequest{Database: testDatabase, Bucket: "", Data: []map[string]interface{}{markerDocument()}}},
		{name: "fifo push", op: transport.OpFIFOPush, request: PushRequest{Database: testDatabase, Bucket: "", Data: markerDocument()}},
	} {
		answer := tc.frame(0, mutation.op, mutation.request)
		if answer.Opcode != transport.OpError {
			t.Errorf("%s with no bucket was accepted", mutation.name)
		}
	}
	if after := sealedLog.LastSequence(); after != logged {
		t.Errorf("a mutation with no bucket was appended to the log (sequence %d, was %d): it must be refused before the append", after, logged)
	}
	time.Sleep(1500 * time.Millisecond)
	if where, found := markerWindowUnder(t, tc.nodes[1].cfg.Storage.DataDir); found {
		t.Errorf("the plaintext member holds the payload of a refused mutation, in %s", where)
	}

	// Database DDL is what the path is for, and still reaches every member.
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "broadcastdb"})
	deadline := time.Now().Add(10 * time.Second)
	for {
		listed, err := tc.nodes[1].server.core.ListDatabases(ListDatabasesRequest{})
		if err == nil && strings.Contains(fmt.Sprint(listed), "broadcastdb") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("a database create no longer reaches the other member: %v, %v", listed, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestPlaintextOwnerPullsPastASealedNonHolder is the first regression. A
// plaintext node becomes the owner of a bucket only a plaintext peer holds, in
// a cluster that also has a sealed member. The sealed member refused the pull
// before it looked whether it held the bucket at all, and the puller stopped at
// the refusal instead of asking the next member.
func TestPlaintextOwnerPullsPastASealedNonHolder(t *testing.T) {
	tc := newMixedCluster(t, 1, "on", "")
	defer tc.stop()
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	bucket := bucketPlacedAfterJoin(t, tc, newcomer, "plain", ownedBy(tc.addrs[1]), ownedBy(newcomer.ID))
	tc.ok(1, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(1, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	tc.ok(1, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": 1}})

	joiner := startJoinTarget(t, tc, ports, tc.token, func(cfg *config.Config) { cfg.Server.AutoRebalance = false })
	joinAndResync(t, tc, 1, joiner, newcomer)
	joined := joiner.server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The sealed member does not hold the bucket, and says that — the answer a
	// puller moves on from — rather than refusing a bucket it has not got.
	err := joined.pullBucket(ctx, testDatabase, bucket, mustNode(t, joined, tc.addrs[0]))
	if !errors.Is(err, ErrMigrateBucketNotFound) {
		t.Errorf("a pull from the sealed member that does not hold the bucket answered: %v; want %v", err, ErrMigrateBucketNotFound)
	}

	if err := joined.ensureMigrated(ctx, testDatabase, bucket); err != nil {
		t.Fatalf("the plaintext new owner could not pull the bucket from its plaintext holder: %v", err)
	}
	if !locallyFound(t, joiner, bucket, "k") {
		t.Errorf("the plaintext new owner does not serve the bucket it pulled")
	}
}

// TestPullerTriesTheNextMemberAfterARefusal is the other half of that
// regression: a member that holds the bucket and refuses to hand it over does
// not end the pull while another member holds it too.
func TestPullerTriesTheNextMemberAfterARefusal(t *testing.T) {
	tc := newMixedCluster(t, 1, "on", "")
	defer tc.stop()
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	bucket := bucketPlacedAfterJoin(t, tc, newcomer, "both", func([]ring.Node) bool { return true }, ownedBy(newcomer.ID))
	tc.ok(1, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	// Both members hold a copy: the state a cluster built before the refusal
	// existed is in.
	for _, node := range tc.nodes {
		createLocally(t, node, bucket)
		if _, err := node.server.core.Insert(InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": 1}}); err != nil {
			t.Fatalf("writing the key on %s directly: %v", node.addr, err)
		}
	}
	joiner := startJoinTarget(t, tc, ports, tc.token, func(cfg *config.Config) { cfg.Server.AutoRebalance = false })
	joinAndResync(t, tc, 1, joiner, newcomer)
	joined := joiner.server.core.cluster
	_, members := joined.epochMembers()
	if members[0].ID != tc.addrs[0] {
		t.Fatalf("test setup: the sealed member must be asked first, the order is %v", nodeIDs(members))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := joined.ensureMigrated(ctx, testDatabase, bucket); err != nil {
		t.Fatalf("the pull stopped at the sealed member's refusal although the plaintext member holds the bucket too: %v", err)
	}
	if !locallyFound(t, joiner, bucket, "k") {
		t.Errorf("the plaintext new owner does not serve the bucket it pulled")
	}
	if !locallyFound(t, tc.nodes[0], bucket, "k") {
		t.Errorf("the sealed member gave up its copy")
	}
}

// TestUnreportingPeerIsNotCalledPlaintext is the second regression. A sealed
// node running a version from before the flag answers without it, and was told
// apart from nothing: the refusal said its store is not encrypted and to start
// it from a fresh data directory, which is false and would wipe a good node.
func TestUnreportingPeerIsNotCalledPlaintext(t *testing.T) {
	tc := newMixedCluster(t, 2, "on", "on")
	defer tc.stop()
	sealed := tc.nodes[0].server.core.cluster
	older := mustNode(t, sealed, tc.addrs[1])
	// What a node of the earlier version sends: its own record, with no word on
	// its store.
	sealed.learnStoreFromGossip(GossipRequest{
		NodeID: older.ID, Addr: older.Addr, Nodes: []Peer{{ID: older.ID, Addr: older.Addr}},
	})
	refusal := sealed.refuseKnownPlaintextPeer(older, `replicate PUT for bucket "b"`)
	if refusal == nil {
		t.Fatalf("a peer that does not report its store was not refused")
	}
	text := refusal.Error()
	t.Logf("refusal for a peer that does not report: %s", text)
	for _, forbidden := range []string{"is not encrypted", "fresh data directory"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("the refusal says %q of a peer that only failed to report", forbidden)
		}
	}
	for _, want := range []string{older.ID, "version", "pgrad", "remove-node"} {
		if !strings.Contains(text, want) {
			t.Errorf("the refusal does not say %q", want)
		}
	}
}

// TestUnreachablePeerIsNotToldToRecreateItsStore: the same rule for a peer that
// has never answered. It may be a sealed node that is only down.
func TestUnreachablePeerIsNotToldToRecreateItsStore(t *testing.T) {
	// A sealed cluster never places data on the plaintext joiner, so the join
	// never settles and the freeze would answer first; the test is about the
	// placement refusal behind it.
	switchJoinFreezeOff(t)
	tc := newMixedCluster(t, 2, "on")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	sealed := tc.nodes[0].server.core.cluster
	silent := fabricatedMemberAddr(t)
	if err := addMemberUnasked(sealed, ring.Node{ID: silent, Addr: silent}); err != nil {
		t.Fatalf("adding a member that is not up yet: %v", err)
	}
	created := tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "unreached", Type: BucketTypeHash})
	if created.Opcode != transport.OpError {
		t.Fatalf("a sealed node created a bucket whose replica set holds %s, which never answered", silent)
	}
	text := printable(string(created.Payload))
	t.Logf("refusal for a peer that never answered: %s", text)
	for _, forbidden := range []string{"is not encrypted", "fresh data directory"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("the refusal says %q of a peer that never answered", forbidden)
		}
	}
	for _, want := range []string{silent, "has not answered", "remove-node"} {
		if !strings.Contains(text, want) {
			t.Errorf("the refusal does not say %q", want)
		}
	}
}

// TestBucketDropReachesAPlaintextReplica kills the mutant that removes the
// drop's exemption: a bucket both nodes hold is dropped through the sealed
// owner, and the plaintext replica's copy goes too. That is the way an operator
// clears a bucket a plaintext node already holds.
func TestBucketDropReachesAPlaintextReplica(t *testing.T) {
	tc := newMixedCluster(t, 2, "on", "")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, owner := range []string{tc.addrs[0], tc.addrs[1]} {
		bucket := bucketPlaced(t, tc.nodes[0].server.core.cluster, "drop", ownedBy(owner))
		createLocally(t, tc.nodes[0], bucket)
		createLocally(t, tc.nodes[1], bucket)
		dropped := tc.frame(0, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: bucket})
		if dropped.Opcode == transport.OpError {
			t.Fatalf("a sealed node refused to drop bucket %q (owner %s): %s", bucket, owner, printable(string(dropped.Payload)))
		}
		deadline := time.Now().Add(10 * time.Second)
		for tc.nodes[1].server.core.bucketStoreOpen(testDatabase, bucket) {
			if time.Now().After(deadline) {
				t.Fatalf("the drop of %q (owner %s) never reached the plaintext replica", bucket, owner)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// writeConfirmedMembership writes the epoch-1 membership record of cfg's seed
// list, not bootstrapped, into its data directory before the node starts.
func writeConfirmedMembership(t *testing.T, cfg *config.Config) {
	t.Helper()
	members := make([]ring.Node, 0, len(cfg.HashRing.SeedNodes))
	for _, seed := range cfg.HashRing.SeedNodes {
		member, err := parseMember(seed)
		if err != nil {
			t.Fatalf("parsing seed %q: %v", seed, err)
		}
		members = append(members, member)
	}
	if err := saveMembershipRecord(membershipPathFor(cfg.Storage.DataDir),
		membershipRecordOf(membershipState{epoch: InitialEpoch, members: members})); err != nil {
		t.Fatalf("writing a confirmed membership record: %v", err)
	}
}

// TestRequireVoterRefusesAPlaintextProposer kills the mutant that removes the
// proposer check: the newcomer is sealed, so nothing but the proposer's own
// store can refuse this vote.
func TestRequireVoterRefusesAPlaintextProposer(t *testing.T) {
	// The require node exchanges no membership with the plaintext one, so
	// neither founder can ever have its seed view confirmed by agreement (2 of
	// 2), and the fixture waits for that (join-freeze spec §5.4). This test is
	// about the vote, so both start from a confirmed membership record — what a
	// data directory from before the bootstrapped mark holds.
	modes := []string{"", "require"}
	tc := newTestClusterWith(t, len(modes), 1, func(index int, cfg *config.Config) {
		cfg.Server.AutoRebalance = false
		if modes[index] != "" {
			sealNode(t, cfg, modes[index], byte(0x40+0x10*index))
		}
		writeConfirmedMembership(t, cfg)
	})
	defer tc.stop()
	ports := reserveNodePorts(t)
	startJoinTarget(t, tc, ports, tc.token, func(cfg *config.Config) {
		cfg.Server.AutoRebalance = false
		sealNode(t, cfg, "on", 0x90)
	})
	voter := tc.nodes[1].server.core.cluster
	newcomer := ports.ringNode()
	proposed := append(append([]ring.Node{}, voter.Members()...), newcomer)
	_, err := voter.acceptMembershipChange(acceptRequestOf(tc.addrs[0], InitialEpoch+1, proposed,
		Peer{ID: newcomer.ID, Addr: newcomer.Addr}))
	if err == nil {
		t.Fatalf("a voter in encryption.mode=require accepted a membership change proposed by the plaintext member %s", tc.addrs[0])
	}
	t.Logf("vote refused with: %v", err)
	for _, want := range []string{tc.addrs[0], "encryption.mode=require", "is not encrypted"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q", want)
		}
	}
	if voter.Epoch() != InitialEpoch {
		t.Errorf("the refused vote moved the voter to epoch %d", voter.Epoch())
	}
}

// TestRelocationDoesNotHandASealedBucketToAPlaintextPeer is the load-time
// relocation's own test: a sealed node holds a bucket outside its replica set,
// the set is a plaintext node, and the relocation must leave the bucket where
// it is.
func TestRelocationDoesNotHandASealedBucketToAPlaintextPeer(t *testing.T) {
	tc := newMixedCluster(t, 1, "on", "")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	sealed := tc.nodes[0].server.core.cluster
	bucket := bucketPlaced(t, sealed, "misplaced", ownedBy(tc.addrs[1]))
	createLocally(t, tc.nodes[0], bucket)
	if _, err := tc.nodes[0].server.core.Insert(InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: markerDocument()}); err != nil {
		t.Fatalf("writing the key on the sealed node directly: %v", err)
	}
	misplaced := sealed.misplacedBuckets()
	if len(misplaced) != 1 || misplaced[0].bucket != bucket {
		t.Fatalf("test setup: the sealed node must hold exactly %q outside its replica set, it holds %v", bucket, misplaced)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if remaining := sealed.relocatePass(ctx, misplaced); len(remaining) != 1 {
		t.Errorf("the relocation reports the bucket handed to the plaintext node")
	}
	assertHoldsNothing(t, tc.nodes[1], bucket)
	if where, found := markerWindowUnder(t, tc.nodes[1].cfg.Storage.DataDir); found {
		t.Errorf("the plaintext node holds the relocated document, in %s", where)
	}
	if !locallyFound(t, tc.nodes[0], bucket, "k") {
		t.Errorf("the sealed node gave up the bucket it could not hand over")
	}
}

// TestQueuedEntriesReachAPeerThatReturnsSealed is the control for the queued
// case: a sealed replica that restarts sealed is asked again on its new
// connection, says sealed, and receives what was queued for it while it was
// down. Tying the claim to the connection must not strand a sealed peer.
func TestQueuedEntriesReachAPeerThatReturnsSealed(t *testing.T) {
	tc := newMixedCluster(t, 2, "on", "on")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketPlaced(t, owner, "returns", ownedBy(tc.addrs[0]))
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "before", Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})

	stopNode(t, tc, 1)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "queued", Data: map[string]interface{}{"n": 2}, WriteConcern: "one"})
	tc.nodes[1] = tc.startNode(1, filepath.Dir(tc.nodes[0].cfg.Storage.DataDir), tc.token)

	deadline := time.Now().Add(20 * time.Second)
	for !locallyFound(t, tc.nodes[1], bucket, "queued") {
		if time.Now().After(deadline) {
			t.Fatalf("a sealed replica that restarted sealed never received the entry queued for it while it was down")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
