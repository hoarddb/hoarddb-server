package server

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The mixed-mode cluster tests: a node whose store is sealed (encryption.mode
// on or require) beside a node whose store is plaintext. They are the sealed-to-plaintext check at
// the current tree and the at-rest-encryption spec's §5.5: every path that
// moves a bucket's data from the sealed node to the plaintext one must refuse,
// naming the peer and the way out, and the plaintext node must end up holding
// nothing. Each path has its own test, because each is a different sender.

// plaintextMarker is a value no test writes anywhere else, so finding it in a
// plaintext node's data directory means a sealed node's document landed there.
const plaintextMarker = "D3-PLAINTEXT-MARKER-5f1c9a70e2b44d18"

// sealNode configures cfg as a sealed node in mode under its own key file, so
// two sealed nodes of one test cluster never share a KEK or a node master.
func sealNode(t testing.TB, cfg *config.Config, mode string, seed byte) {
	t.Helper()
	keyDir := t.TempDir()
	keyPath := filepath.Join(keyDir, "kek")
	rawKey := make([]byte, 32)
	for i := range rawKey {
		rawKey[i] = seed + byte(i)
	}
	if err := os.WriteFile(keyPath, rawKey, 0o600); err != nil {
		t.Fatalf("writing key file: %v", err)
	}
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.KeyFile = keyPath
	cfg.Encryption.Mode = mode
}

// newMixedCluster starts one node per mode at replication factor rf: "" is a
// plaintext node, "on" and "require" are sealed ones. The background rebalance
// sweep is off on every node, so a test moves data only by the step it names.
func newMixedCluster(t *testing.T, rf int, modes ...string) *testCluster {
	t.Helper()
	return newTestClusterWith(t, len(modes), rf, func(index int, cfg *config.Config) {
		cfg.Server.AutoRebalance = false
		if modes[index] != "" {
			sealNode(t, cfg, modes[index], byte(0x40+0x10*index))
		}
	})
}

// bucketPlaced returns a bucket name of testDatabase whose replica set on c's
// ring satisfies want, so a test creates exactly the placement it is about.
func bucketPlaced(t *testing.T, c *Cluster, prefix string, want func(replicas []ring.Node) bool) string {
	t.Helper()
	for i := 0; i < 512; i++ {
		name := fmt.Sprintf("%s%03d", prefix, i)
		replicas, err := c.Replicas(placementOf(testDatabase, name))
		if err != nil {
			t.Fatalf("placing %s: %v", name, err)
		}
		if want(replicas) {
			return name
		}
	}
	t.Fatalf("no bucket %s000..%s511 has the wanted placement", prefix, prefix)
	return ""
}

func ownedBy(id string) func([]ring.Node) bool {
	return func(replicas []ring.Node) bool { return replicas[0].ID == id }
}

func placedWithout(id string) func([]ring.Node) bool {
	return func(replicas []ring.Node) bool {
		for _, replica := range replicas {
			if replica.ID == id {
				return false
			}
		}
		return true
	}
}

// assertPlaintextPeerRefusal requires a refusal to name the plaintext peer, to
// say its store is not encrypted, and to give both ways out.
func assertPlaintextPeerRefusal(t *testing.T, what, answer, peer string) {
	t.Helper()
	// Logged so a -v run shows the operator's text, not only that it matched.
	t.Logf("%s refused with: %s", what, printable(answer))
	for _, want := range []string{peer, "is not encrypted", "fresh data directory", "remove-node"} {
		if !strings.Contains(answer, want) {
			t.Errorf("%s: the refusal does not say %q: %s", what, want, answer)
		}
	}
}

// printable drops the BSON framing an error frame's payload carries around its
// message, so a logged refusal reads as the sentence it is.
func printable(answer string) string {
	return strings.Map(func(r rune) rune {
		if r < ' ' || r == 0xfffd {
			return -1
		}
		return r
	}, answer)
}

// assertHoldsNothing requires a plaintext node to hold neither the bucket nor,
// anywhere under its data directory, the marker a sealed node's document carries.
func assertHoldsNothing(t *testing.T, node *testNode, bucket string) {
	t.Helper()
	if node.server.core.bucketStoreOpen(testDatabase, bucket) {
		t.Errorf("the plaintext node %s holds bucket %q from a sealed node, in the clear", node.addr, bucket)
	}
	if path, found := markerUnder(t, node.cfg.Storage.DataDir); found {
		t.Errorf("the plaintext node %s holds a sealed node's document in the clear, in %s", node.addr, path)
	}
}

// markerUnder reports the first file under dir that contains plaintextMarker.
func markerUnder(t *testing.T, dir string) (string, bool) {
	t.Helper()
	var holder string
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || holder != "" {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		if bytes.Contains(content, []byte(plaintextMarker)) {
			holder = path
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", dir, err)
	}
	return holder, holder != ""
}

// createLocally creates bucket on a node's own store, bypassing the cluster:
// the state of a replica set that already spans a sealed and a plaintext node,
// as a cluster built before the refusal existed has. The create is applied as
// a stamped one (term 0, incarnation 0) because a bare create at a node's own
// door now mints an incarnation (BUG-0624), and each node would mint its own:
// copies of one bucket on two nodes must carry one number.
func createLocally(t *testing.T, node *testNode, bucket string) {
	t.Helper()
	resp, err := node.server.core.createBucketLocked(withClusterApply(context.Background()), CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	if err != nil || !resp.Success {
		t.Fatalf("creating %q on %s directly: err=%v resp=%+v", bucket, node.addr, err, resp)
	}
}

func locallyFound(t *testing.T, node *testNode, bucket, key string) bool {
	t.Helper()
	resp, err := node.server.core.Find(FindRequest{Database: testDatabase, Bucket: bucket, Key: key})
	return err == nil && resp != nil && resp.Found
}

func markerDocument() map[string]interface{} {
	return map[string]interface{}{"secret": plaintextMarker}
}

// TestSealedNodeDoesNotPlaceDataOnAPlaintextPeer is the sealed-to-plaintext check at the current
// tree, in the form the at-rest-encryption-spec record §5.5 names: two
// nodes, one sealed and one plaintext, and an operation that would place data
// from the sealed node onto the plaintext one. It must fail naming the peer,
// and the plaintext node's store must hold no key of the bucket.
func TestSealedNodeDoesNotPlaceDataOnAPlaintextPeer(t *testing.T) {
	keyPath := makeKeyFile(t, t.TempDir(), 0x40)
	tc := newTestClusterWith(t, 2, 2, func(index int, cfg *config.Config) {
		if index == 0 {
			cfg.Encryption.Keystore = "keyfile"
			cfg.Encryption.KeyFile = keyPath
			cfg.Encryption.Mode = "on"
		}
	})
	defer tc.stop()
	plaintextPeer := tc.addrs[1]
	if tc.nodes[0].server.core.nodeMaster == nil || tc.nodes[1].server.core.nodeMaster != nil {
		t.Fatalf("test setup: node 1 must be sealed and node 2 plaintext")
	}

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	created := tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "sealed", Type: BucketTypeHash})
	var createResp CreateBucketResponse
	if created.Opcode != transport.OpError {
		if err := bson.Unmarshal(created.Payload, &createResp); err != nil {
			t.Fatalf("decoding the create's response: %v", err)
		}
	}
	refused := created.Opcode == transport.OpError || !createResp.Success
	if refused {
		answer := string(created.Payload) + createResp.Message
		if !strings.Contains(answer, plaintextPeer) {
			t.Errorf("the create was refused without naming the plaintext peer %s: %s", plaintextPeer, answer)
		}
	} else {
		written := tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: "sealed", Key: "k", Data: map[string]interface{}{"secret": "v"}})
		if written.Opcode != transport.OpError {
			t.Errorf("a sealed node created a bucket at replication factor 2 and wrote to it with a plaintext peer (%s) in its set; the operation must fail naming the peer", plaintextPeer)
		}
	}

	if tc.nodes[1].server.core.bucketStoreOpen(testDatabase, "sealed") {
		t.Errorf("the plaintext peer %s holds bucket %q from a sealed node, in the clear", plaintextPeer, "sealed")
	}
}

// TestSealedNodeRefusesToCreateABucketSpanningAPlaintextPeer is the create at
// both placements: the sealed node as owner, which would ship the create to a
// plaintext replica, and the sealed node as coordinator, which would forward it
// to a plaintext owner. Neither node may end up with the bucket: a bucket that
// looks created on one node only is the quiet lie spec §5.5 forbids.
func TestSealedNodeRefusesToCreateABucketSpanningAPlaintextPeer(t *testing.T) {
	tc := newMixedCluster(t, 2, "on", "")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	sealed := tc.nodes[0].server.core.cluster

	for _, placement := range []struct {
		name  string
		owner string
	}{
		{name: "the sealed node owns it", owner: tc.addrs[0]},
		{name: "the plaintext peer owns it", owner: tc.addrs[1]},
	} {
		t.Run(placement.name, func(t *testing.T) {
			bucket := bucketPlaced(t, sealed, "span", ownedBy(placement.owner))
			created := tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
			if created.Opcode != transport.OpError {
				t.Fatalf("a sealed node created bucket %q with the plaintext peer %s in its replica set", bucket, tc.addrs[1])
			}
			assertPlaintextPeerRefusal(t, "create bucket", string(created.Payload), tc.addrs[1])
			assertHoldsNothing(t, tc.nodes[1], bucket)
			if tc.nodes[0].server.core.bucketStoreOpen(testDatabase, bucket) {
				t.Errorf("the refused create left bucket %q on the sealed node: a bucket that looks created", bucket)
			}
		})
	}
}

// TestSealedNodeRefusesACreateWhoseDeclaredFactorReachesAPlaintextPeer is the
// same refusal where the plaintext peer is in the set only because the create
// declares a factor above the node's: at the node factor the sealed owner is the
// whole set. The owner's check before the append must look at the set the create
// is shipped to (deliverySet), or the create is logged and applied here and
// refused only at the ship — a bucket that looks created on one node.
func TestSealedNodeRefusesACreateWhoseDeclaredFactorReachesAPlaintextPeer(t *testing.T) {
	tc := newMixedCluster(t, 1, "on", "")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	sealed := tc.nodes[0].server.core.cluster
	bucket := bucketPlaced(t, sealed, "declared", ownedBy(tc.addrs[0]))

	created := tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase, Name: bucket, Type: BucketTypeHash, ReplicationFactor: 2,
	})
	var createResp CreateBucketResponse
	if created.Opcode != transport.OpError {
		if err := bson.Unmarshal(created.Payload, &createResp); err != nil {
			t.Fatalf("decoding the create's response: %v", err)
		}
		if createResp.Success {
			t.Fatalf("a sealed node created bucket %q at replication_factor 2 with the plaintext peer %s in that set", bucket, tc.addrs[1])
		}
	}
	assertPlaintextPeerRefusal(t, "create bucket at a declared factor", string(created.Payload)+createResp.Message, tc.addrs[1])
	assertHoldsNothing(t, tc.nodes[1], bucket)
	if tc.nodes[0].server.core.bucketStoreOpen(testDatabase, bucket) {
		t.Errorf("the refused create left bucket %q on the sealed node: a bucket that looks created", bucket)
	}
}

// TestSealedNodeDoesNotWriteToAPlaintextReplicaSetMember is the replication
// placement of a new write into a bucket both nodes already hold: as owner the
// sealed node would ship the entry to the plaintext replica, and as coordinator
// it would forward the write to the plaintext owner.
func TestSealedNodeDoesNotWriteToAPlaintextReplicaSetMember(t *testing.T) {
	tc := newMixedCluster(t, 2, "on", "")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	sealed := tc.nodes[0].server.core.cluster

	for _, placement := range []struct {
		name  string
		owner string
	}{
		{name: "owner ships to a plaintext replica", owner: tc.addrs[0]},
		{name: "coordinator forwards to a plaintext owner", owner: tc.addrs[1]},
	} {
		t.Run(placement.name, func(t *testing.T) {
			bucket := bucketPlaced(t, sealed, "write", ownedBy(placement.owner))
			createLocally(t, tc.nodes[0], bucket)
			createLocally(t, tc.nodes[1], bucket)

			written := tc.frame(0, transport.OpPut, InsertRequest{
				Database: testDatabase, Bucket: bucket, Key: "k", Data: markerDocument(), WriteConcern: "all",
			})
			if written.Opcode != transport.OpError {
				t.Fatalf("a sealed node accepted a write to bucket %q, whose replica set holds the plaintext peer %s", bucket, tc.addrs[1])
			}
			assertPlaintextPeerRefusal(t, "put", string(written.Payload), tc.addrs[1])
			if locallyFound(t, tc.nodes[1], bucket, "k") {
				t.Errorf("the plaintext peer %s holds the sealed node's document", tc.addrs[1])
			}
			if _, found := markerUnder(t, tc.nodes[1].cfg.Storage.DataDir); found {
				t.Errorf("the plaintext peer %s holds the sealed node's document in the clear on disk", tc.addrs[1])
			}
			if locallyFound(t, tc.nodes[0], bucket, "k") {
				t.Errorf("the refused write was applied on the sealed node: a write reported failed must not be half placed")
			}
		})
	}
}

// TestMigrateToAPlaintextTargetIsRefusedNamingThePeer is the pull itself
// (MIGRATE_START), the one door every migration goes through: a plaintext node
// asks a sealed node for a bucket.
func TestMigrateToAPlaintextTargetIsRefusedNamingThePeer(t *testing.T) {
	tc := newMixedCluster(t, 1, "on", "")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	sealed, plaintext := tc.nodes[0].server.core.cluster, tc.nodes[1].server.core.cluster
	bucket := bucketPlaced(t, sealed, "pull", ownedBy(tc.addrs[0]))
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: markerDocument()})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := plaintext.pullBucket(ctx, testDatabase, bucket, mustNode(t, plaintext, tc.addrs[0]))
	if err == nil {
		t.Fatalf("a plaintext node pulled bucket %q from a sealed node", bucket)
	}
	assertPlaintextPeerRefusal(t, "migrate", err.Error(), tc.addrs[1])
	assertHoldsNothing(t, tc.nodes[1], bucket)
	if !locallyFound(t, tc.nodes[0], bucket, "k") {
		t.Errorf("the sealed node no longer serves bucket %q after refusing to hand it over", bucket)
	}
}

// TestMigrateReEncryptsOnTarget is the sealed-to-sealed move: two sealed nodes under different keys, a bucket pulled from one to
// the other, and the target's store asserted structurally with the node
// stopped — the digest matching is not evidence the copy is sealed.
func TestMigrateReEncryptsOnTarget(t *testing.T) {
	tc := newMixedCluster(t, 1, "on", "on")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	source, target := tc.nodes[0].server.core.cluster, tc.nodes[1].server.core.cluster
	bucket := bucketPlaced(t, source, "reseal", ownedBy(tc.addrs[0]))
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	for i := 0; i < 20; i++ {
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("k%02d", i), Data: markerDocument()})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// pullBucket returns only once the source has compared its digest of the
	// stream with the target's (MIGRATE_DONE), so nil is both sides agreeing on
	// the bytes that crossed; the target then seals them under its own key.
	if err := target.pullBucket(ctx, testDatabase, bucket, mustNode(t, target, tc.addrs[0])); err != nil {
		t.Fatalf("a sealed node could not pull bucket %q from a sealed node: %v", bucket, err)
	}
	for i := 0; i < 20; i++ {
		if key := fmt.Sprintf("k%02d", i); !locallyFound(t, tc.nodes[1], bucket, key) {
			t.Fatalf("the target does not serve %s of the migrated bucket", key)
		}
	}

	targetDir := tc.nodes[1].cfg.Storage.DataDir
	_ = tc.nodes[1].client.Close()
	tc.nodes[1].client = nil
	if err := tc.nodes[1].server.Stop(); err != nil {
		t.Fatalf("stopping the target: %v", err)
	}
	tc.nodes[1].server = nil
	AssertBadgerStructureEncrypted(t, targetDir)
	if path, found := markerUnder(t, targetDir); found {
		t.Errorf("the migrated documents are in the clear on the sealed target, in %s", path)
	}
}

// TestRebalanceSweepDoesNotPlaceOnAPlaintextPeer is the sweep after a join,
// and the operator's `migrate` verb on the joined node: a plaintext node joins
// a sealed one, the ring hands it buckets, and neither the sweep nor the verb
// may move them there.
func TestRebalanceSweepDoesNotPlaceOnAPlaintextPeer(t *testing.T) {
	tc := newMixedCluster(t, 1, "on")
	defer tc.stop()
	const buckets = 16
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for i := 0; i < buckets; i++ {
		name := fmt.Sprintf("sweep%02d", i)
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: name, Key: "k", Data: markerDocument()})
	}

	sealed := tc.nodes[0].server.core.cluster
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := addMemberUnasked(sealed, newcomer); err != nil {
		t.Fatalf("making the plaintext node %s a member: %v", newcomer.ID, err)
	}
	joiner := startJoinTarget(t, tc, ports, tc.token, func(cfg *config.Config) { cfg.Server.AutoRebalance = false })
	joined := joiner.server.core.cluster
	if err := joined.reconcileMembership(tc.addrs[0], sealed.memberNodes(), sealed.memberIDs(), sealed.Epoch()); err != nil {
		t.Fatalf("joiner resync: %v", err)
	}

	var owned []string
	for i := 0; i < buckets; i++ {
		name := fmt.Sprintf("sweep%02d", i)
		if owner, err := joined.Owner(placementOf(testDatabase, name)); err == nil && owner.ID == newcomer.ID {
			owned = append(owned, name)
		}
	}
	if len(owned) == 0 {
		t.Fatalf("the ring handed the joined node none of %d buckets", buckets)
	}

	sealed.sweep(ctx)

	for _, name := range owned {
		assertHoldsNothing(t, joiner, name)
		if !locallyFound(t, tc.nodes[0], name, "k") {
			t.Errorf("the sealed node no longer serves %q after the sweep", name)
		}
	}

	// ensureMigrated is what the operator's `migrate` verb runs (handleMigrate);
	// its error is the verb's answer.
	err := joined.ensureMigrated(ctx, testDatabase, owned[0])
	if err == nil {
		t.Fatalf("'migrate' pulled %q from a sealed node onto the plaintext node", owned[0])
	}
	assertPlaintextPeerRefusal(t, "migrate", err.Error(), newcomer.ID)
	assertHoldsNothing(t, joiner, owned[0])
}

// TestRemoveNodeDoesNotHandOffToAPlaintextPeer is the hand-off a removal
// causes: two sealed nodes hold a bucket, one is removed, and the ring puts
// the plaintext node in its place.
func TestRemoveNodeDoesNotHandOffToAPlaintextPeer(t *testing.T) {
	tc := newMixedCluster(t, 2, "on", "", "on")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	sealed := tc.nodes[0].server.core.cluster
	bucket := bucketPlaced(t, sealed, "leave", placedWithout(tc.addrs[1]))
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: markerDocument(), WriteConcern: "all"})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := sealed.proposeRemoval(ctx, tc.addrs[2]); err != nil {
		t.Fatalf("removing the second sealed node: %v", err)
	}
	plaintext := tc.nodes[1].server.core.cluster
	if err := plaintext.reconcileMembership(tc.addrs[0], sealed.memberNodes(), sealed.memberIDs(), sealed.Epoch()); err != nil {
		t.Fatalf("plaintext node resync after the removal: %v", err)
	}
	if !plaintext.stillReplica(placementOf(testDatabase, bucket)) {
		t.Fatalf("test setup: the removal must put the plaintext node in the replica set of %q", bucket)
	}

	sealed.sweep(ctx)
	assertHoldsNothing(t, tc.nodes[1], bucket)

	err := plaintext.ensureMigrated(ctx, testDatabase, bucket)
	if err == nil {
		t.Fatalf("the plaintext node pulled %q after the removal", bucket)
	}
	assertPlaintextPeerRefusal(t, "migrate after remove-node", err.Error(), tc.addrs[1])
	assertHoldsNothing(t, tc.nodes[1], bucket)
	if !locallyFound(t, tc.nodes[0], bucket, "k") {
		t.Errorf("the sealed node no longer serves %q", bucket)
	}
}

// TestCatchUpDoesNotReplayASealedBucketOntoAPlaintextPeer is the log replay a
// node gives a peer that is behind: a plaintext node joins, the ring makes it a
// replica of a bucket the sealed owner has history for, and the owner's
// catch-up must not ship that history — the create and every write — to it.
func TestCatchUpDoesNotReplayASealedBucketOntoAPlaintextPeer(t *testing.T) {
	tc := newMixedCluster(t, 2, "on", "on")
	defer tc.stop()
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	scenario := findSeededReplicaScenario(t, tc, newcomer, "replay")
	owner := tc.nodes[scenario.ownerIndex]

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(scenario.ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: scenario.bucket, Type: BucketTypeHash})
	tc.ok(scenario.ownerIndex, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: scenario.bucket, Key: "k", Data: markerDocument(), WriteConcern: "all",
	})

	joiner := joinLiveReplicaBy(t, tc, ports, nil, addMemberUnasked)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ownerCluster := owner.server.core.cluster
	// The error is not the assertion: a refused catch-up may report or stay
	// quiet, and what matters is what reached the plaintext node.
	if err := replayToPeer(ctx, ownerCluster.peerFor(newcomer)); err != nil {
		t.Logf("catch-up to the plaintext node: %v", err)
	}
	assertHoldsNothing(t, joiner, scenario.bucket)
}

// TestReadRepairDoesNotWriteToAPlaintextReplica is the repair a quorum read
// makes: the sealed node holds a key the plaintext replica lacks, and the read
// must answer without pushing the value to it.
func TestReadRepairDoesNotWriteToAPlaintextReplica(t *testing.T) {
	tc := newMixedCluster(t, 2, "on", "")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	const bucket = "repair"
	createLocally(t, tc.nodes[0], bucket)
	createLocally(t, tc.nodes[1], bucket)
	if _, err := tc.nodes[0].server.core.Insert(InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: markerDocument()}); err != nil {
		t.Fatalf("writing the key on the sealed node directly: %v", err)
	}

	// Holders whose stores differ are not compared at all: the read is
	// refused, so nothing is repaired either way.
	requireConsistentGetRefused(t, tc.frame(0, transport.OpConsistentGet, ConsistentGetRequest{Database: testDatabase, Bucket: bucket, Key: "k"}),
		transport.ErrUnavailable, tc.addrs[1])
	if locallyFound(t, tc.nodes[1], bucket, "k") {
		t.Errorf("read repair wrote the sealed node's document to the plaintext replica %s", tc.addrs[1])
	}
}

// TestConsistentGetIsRefusedAcrossSealedAndPlaintextHolders: a plaintext
// owner holds no write that only a sealed copy has — the encryption boundary
// keeps sealed data off it — so its absence proves nothing. A consistent get
// of such a key, from either node, is refused UNAVAILABLE, naming the
// plaintext holder and how to finish sealing it; nothing is answered "not
// found" and the sealed copy is not repaired away. Guard: no check of the
// holders' stores (the sealed coordinator answers not found).
func TestConsistentGetIsRefusedAcrossSealedAndPlaintextHolders(t *testing.T) {
	tc := newMixedCluster(t, 2, "on", "")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketPlaced(t, tc.nodes[0].server.core.cluster, "mixed", ownedBy(tc.addrs[1]))
	createLocally(t, tc.nodes[0], bucket)
	createLocally(t, tc.nodes[1], bucket)
	if _, err := tc.nodes[0].server.core.Insert(InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: markerDocument()}); err != nil {
		t.Fatalf("writing the key on the sealed node directly: %v", err)
	}
	for index := range tc.nodes {
		frame := tc.frame(index, transport.OpConsistentGet, ConsistentGetRequest{Database: testDatabase, Bucket: bucket, Key: "k"})
		message := requireConsistentGetRefused(t, frame, transport.ErrUnavailable, tc.addrs[1], "encryption.mode", "hoarddb dump")
		t.Logf("from node %d, refused: %s", index+1, message)
	}
	if !locallyFound(t, tc.nodes[0], bucket, "k") {
		t.Errorf("the sealed copy of k was removed")
	}
}

// TestRestoreIsNotForwardedToAPlaintextOwner is the one client write that does
// not travel as a replicated entry: a restore is routed to the bucket's owner
// like a read, so a sealed coordinator would hand the dump to a plaintext owner.
func TestRestoreIsNotForwardedToAPlaintextOwner(t *testing.T) {
	tc := newMixedCluster(t, 1, "on", "")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketPlaced(t, tc.nodes[0].server.core.cluster, "restore", ownedBy(tc.addrs[1]))

	started := tc.frame(0, transport.OpRestoreStart, RestoreStartRequest{
		Database: testDatabase, Bucket: bucket, FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash,
	})
	if started.Opcode != transport.OpError {
		t.Fatalf("a sealed node forwarded a restore of %q to its plaintext owner %s", bucket, tc.addrs[1])
	}
	assertPlaintextPeerRefusal(t, "restore", string(started.Payload), tc.addrs[1])
	assertHoldsNothing(t, tc.nodes[1], bucket)
}

// TestSealedNodeTreatsAnUnconfirmedPeerAsPlaintext: a member that has never
// said its store is sealed — it is down, or it runs a build that does not
// report it — is given nothing, and the refusal says which of the two it is.
func TestSealedNodeTreatsAnUnconfirmedPeerAsPlaintext(t *testing.T) {
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

	created := tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "unconfirmed", Type: BucketTypeHash})
	if created.Opcode != transport.OpError {
		t.Fatalf("a sealed node created a bucket whose replica set holds %s, which never confirmed its store is encrypted", silent)
	}
	answer := string(created.Payload)
	t.Logf("create bucket refused with: %s", printable(answer))
	for _, want := range []string{silent, "has not answered", "remove-node"} {
		if !strings.Contains(answer, want) {
			t.Errorf("the refusal does not say %q: %s", want, answer)
		}
	}
	if tc.nodes[0].server.core.bucketStoreOpen(testDatabase, "unconfirmed") {
		t.Errorf("the refused create left the bucket on the sealed node")
	}
}

// TestRequireNodeRefusesToAdmitAPlaintextPeer: encryption.mode=require refuses
// a plaintext joiner, as the proposer and as a voter, names it and the fix, and
// leaves the membership where it was. A sealed joiner is admitted.
func TestRequireNodeRefusesToAdmitAPlaintextPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	assertJoinRefusal := func(t *testing.T, err error, joiner string) {
		t.Helper()
		t.Logf("join refused with: %v", err)
		for _, want := range []string{joiner, "encryption.mode=require", "is not encrypted", "fresh data directory"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the join refusal does not say %q: %v", want, err)
			}
		}
	}

	t.Run("as the proposer", func(t *testing.T) {
		tc := newMixedCluster(t, 1, "require")
		defer tc.stop()
		ports := reserveNodePorts(t)
		startJoinTarget(t, tc, ports, tc.token, func(cfg *config.Config) { cfg.Server.AutoRebalance = false })
		requiring := tc.nodes[0].server.core.cluster
		err := requiring.proposeJoin(ctx, ports.ringNode())
		if err == nil {
			t.Fatalf("a node in encryption.mode=require admitted the plaintext node %s", ports.ringAddr)
		}
		assertJoinRefusal(t, err, ports.ringAddr)
		if requiring.Epoch() != InitialEpoch || len(requiring.Members()) != 1 {
			t.Errorf("the refused join changed the membership: epoch %d, %d members", requiring.Epoch(), len(requiring.Members()))
		}
	})

	t.Run("as a voter", func(t *testing.T) {
		tc := newMixedCluster(t, 1, "on", "require")
		defer tc.stop()
		ports := reserveNodePorts(t)
		startJoinTarget(t, tc, ports, tc.token, func(cfg *config.Config) { cfg.Server.AutoRebalance = false })
		proposer, voter := tc.nodes[0].server.core.cluster, tc.nodes[1].server.core.cluster
		newcomer := ports.ringNode()

		proposed := append(append([]ring.Node{}, voter.Members()...), newcomer)
		_, err := voter.acceptMembershipChange(acceptRequestOf(tc.addrs[0], InitialEpoch+1, proposed,
			Peer{ID: newcomer.ID, Addr: newcomer.Addr}))
		if err == nil {
			t.Fatalf("a voter in encryption.mode=require accepted the plaintext node %s", newcomer.ID)
		}
		assertJoinRefusal(t, err, newcomer.ID)

		// Two members need both votes, so the voter's refusal fails the join.
		if err := proposer.proposeJoin(ctx, newcomer); err == nil {
			t.Fatalf("the join committed over the refusal of the encryption.mode=require voter")
		}
		if voter.Epoch() != InitialEpoch || len(voter.Members()) != 2 {
			t.Errorf("the refused join changed the voter's membership: epoch %d, %d members", voter.Epoch(), len(voter.Members()))
		}
	})

	t.Run("a sealed joiner is admitted", func(t *testing.T) {
		tc := newMixedCluster(t, 1, "require")
		defer tc.stop()
		ports := reserveNodePorts(t)
		startJoinTarget(t, tc, ports, tc.token, func(cfg *config.Config) {
			cfg.Server.AutoRebalance = false
			sealNode(t, cfg, "on", 0x90)
		})
		requiring := tc.nodes[0].server.core.cluster
		if err := requiring.proposeJoin(ctx, ports.ringNode()); err != nil {
			t.Fatalf("a node in encryption.mode=require refused a sealed joiner: %v", err)
		}
		if requiring.Epoch() != InitialEpoch+1 {
			t.Errorf("epoch after admitting a sealed joiner = %d, want %d", requiring.Epoch(), InitialEpoch+1)
		}
	})
}

// TestRequireNodeWillNotJoinThroughAPlaintextSeed is the other direction: a
// plaintext cluster adds a node in encryption.mode=require, and that node must
// not take its membership from the plaintext seed. Both sides are told why.
func TestRequireNodeWillNotJoinThroughAPlaintextSeed(t *testing.T) {
	tc := newMixedCluster(t, 1, "")
	defer tc.stop()
	seed := tc.nodes[0].server.core.cluster
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := seed.proposeJoin(ctx, newcomer); err != nil {
		t.Fatalf("the plaintext seed adding the node: %v", err)
	}
	joiner := startJoinTarget(t, tc, ports, tc.token, func(cfg *config.Config) {
		cfg.Server.AutoRebalance = false
		sealNode(t, cfg, "require", 0xa0)
	})
	requiring := joiner.server.core.cluster

	err := requiring.peerFor(mustNode(t, requiring, tc.addrs[0])).gossip(ctx)
	if err == nil {
		t.Fatalf("a node in encryption.mode=require exchanged membership with the plaintext seed %s", tc.addrs[0])
	}
	t.Logf("the joining node refused with: %v", err)
	for _, want := range []string{tc.addrs[0], "encryption.mode=require", "is not encrypted", "fresh data directory"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal on the joining node does not say %q: %v", want, err)
		}
	}
	if requiring.Epoch() != InitialEpoch {
		t.Errorf("the node adopted epoch %d from the plaintext seed; it must stay at %d", requiring.Epoch(), InitialEpoch)
	}

	err = seed.peerFor(newcomer).gossip(ctx)
	if err == nil {
		t.Fatalf("the node in encryption.mode=require accepted the plaintext seed's gossip")
	}
	t.Logf("the plaintext seed was told: %v", err)
	if !strings.Contains(err.Error(), "encryption.mode=require") {
		t.Errorf("the plaintext seed is not told why it was refused: %v", err)
	}
}

// TestOnNodeRefusesAPlaintextJoiner is the coordinator's ruling on the join
// freeze: encryption.mode=on never places data on a plaintext member, so a
// plaintext node it admitted held every schema change paused for as long as it
// stayed. The join is refused, naming the encryption mismatch and the fix, and
// no membership change — and so no pause — begins.
func TestOnNodeRefusesAPlaintextJoiner(t *testing.T) {
	tc := newMixedCluster(t, 2, "on")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	startJoinTarget(t, tc, ports, tc.token, func(cfg *config.Config) { cfg.Server.AutoRebalance = false })
	sealed := tc.nodes[0].server.core.cluster
	before := sealed.Epoch()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := sealed.proposeJoin(ctx, newcomer)
	if err == nil {
		t.Fatalf("a node in encryption.mode=on added the plaintext node %s", newcomer.ID)
	}
	t.Logf("the join was refused: %v", err)
	for _, want := range []string{newcomer.ID, "is not encrypted", "encryption.mode=on", "fresh data directory"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the join's refusal does not say %q: %v", want, err)
		}
	}
	if epoch := sealed.Epoch(); epoch != before {
		t.Errorf("the refused join moved the membership from epoch %d to %d", before, epoch)
	}
	if report := sealed.joinFreeze.Load(); report.Paused {
		t.Errorf("a refused join paused schema changes: %+v", *report)
	}
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "after_refused_join", Type: BucketTypeHash})
}

// TestOnNodeRefusesANewcomerItCannotAsk is the verifier's probe B-1: a node in
// encryption.mode=on admitted a newcomer that had not answered — it was not
// running yet — and once it started plaintext the join could never settle.
// The join now fails closed (the coordinator's ruling): a newcomer that cannot
// be asked whether its store is encrypted is refused, and nothing pauses.
func TestOnNodeRefusesANewcomerItCannotAsk(t *testing.T) {
	tc := newMixedCluster(t, 2, "on")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	newcomer := reserveNodePorts(t).ringNode()
	sealed := tc.nodes[0].server.core.cluster
	before := sealed.Epoch()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := sealed.proposeJoin(ctx, newcomer)
	if err == nil {
		t.Fatalf("a node in encryption.mode=on added %s, which it could not ask whether its store is encrypted", newcomer.ID)
	}
	t.Logf("the join was refused: %v", err)
	for _, want := range []string{"could not ask " + newcomer.ID + " whether its store is encrypted", "encrypted at rest", "start " + newcomer.ID + " and run 'join' again"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the join's refusal does not say %q: %v", want, err)
		}
	}
	if epoch := sealed.Epoch(); epoch != before {
		t.Errorf("the refused join moved the membership from epoch %d to %d", before, epoch)
	}
	if report := sealed.joinFreeze.Load(); report.Paused {
		t.Errorf("a refused join paused schema changes: %+v", *report)
	}
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "after_refused_join", Type: BucketTypeHash})
}

// TestOnNodeWarnsOnceAndRefusesPlacementOnAPlaintextMember: a plaintext member
// of an encryption.mode=on cluster — a founder, now that a plaintext joiner is
// refused — is said once in the log however often the two gossip, and no
// bucket is placed on it.
func TestOnNodeWarnsOnceAndRefusesPlacementOnAPlaintextMember(t *testing.T) {
	logs := recordClusterLogs(t)
	tc := newMixedCluster(t, 2, "on", "")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	sealed := tc.nodes[0].server.core.cluster
	plaintext := mustNode(t, sealed, tc.addrs[1])

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for round := 0; round < 3; round++ {
		if err := sealed.peerFor(plaintext).gossip(ctx); err != nil {
			t.Fatalf("gossip round %d with the plaintext member: %v", round, err)
		}
	}
	warnings := 0
	for _, entry := range logs.matching(slog.LevelWarn, "store is not encrypted") {
		if entry.attrs["node_id"] == plaintext.ID {
			warnings++
		}
	}
	if warnings != 1 {
		t.Errorf("the sealed node warned %d times about the plaintext member %s; want exactly once", warnings, plaintext.ID)
	}

	bucket := bucketPlaced(t, sealed, "afterwarn", ownedBy(tc.addrs[0]))
	created := tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	if created.Opcode != transport.OpError {
		t.Fatalf("a sealed node created a bucket with the plaintext member %s in its replica set", plaintext.ID)
	}
	assertPlaintextPeerRefusal(t, "create bucket", string(created.Payload), plaintext.ID)
	assertHoldsNothing(t, tc.nodes[1], bucket)
}

// TestPlaintextClusterIsUnaffected: two plaintext nodes replicate as they
// always did, and exchange membership with no refusal.
func TestPlaintextClusterIsUnaffected(t *testing.T) {
	tc := newMixedCluster(t, 2, "", "")
	defer tc.stop()
	assertReplicates(t, tc)
}

// TestSealedPeersReplicateToEachOther: two sealed nodes under different keys,
// one in each mode, place data on each other, and exchange membership.
func TestSealedPeersReplicateToEachOther(t *testing.T) {
	tc := newMixedCluster(t, 2, "on", "require")
	defer tc.stop()
	assertReplicates(t, tc)
}

// assertReplicates creates a bucket through node 0 of a two-node cluster at
// replication factor 2, writes to it at w=all through each node, and requires
// both to hold both documents and to gossip cleanly in both directions.
func assertReplicates(t *testing.T, tc *testCluster) {
	t.Helper()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	for index, key := range []string{"from-node-1", "from-node-2"} {
		tc.ok(index, transport.OpPut, InsertRequest{
			Database: testDatabase, Bucket: testBucket, Key: key, Data: map[string]interface{}{"n": index}, WriteConcern: "all",
		})
	}
	for _, node := range tc.nodes {
		for _, key := range []string{"from-node-1", "from-node-2"} {
			if !locallyFound(t, node, testBucket, key) {
				t.Errorf("node %s does not hold %q", node.addr, key)
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for index, node := range tc.nodes {
		c := node.server.core.cluster
		other := mustNode(t, c, tc.addrs[1-index])
		if err := c.peerFor(other).gossip(ctx); err != nil {
			t.Errorf("gossip from %s to %s: %v", node.addr, other.ID, err)
		}
	}
}
