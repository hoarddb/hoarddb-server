package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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

// TestNewReplicaReceivesEntriesAfterOwnerPrunedWAL is the acceptance
// test: an owner that has run a real retention pass gains a non-owner replica
// by join; once that replica's snapshot has landed, a later write must reach
// it and a write at `all` must succeed. Before the fix the owner's view of
// the new peer's position was zero — below the retained horizon — so both
// ship and catch-up refused it forever, and `all` timed out on every write.
func TestNewReplicaReceivesEntriesAfterOwnerPrunedWAL(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, autoRebalanceOffOnNodeZero)
	defer tc.stop()
	ports := reserveNodePorts(t)
	sc := findSeededReplicaScenario(t, tc, ports.ringNode(), "hz")

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(sc.ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: sc.bucket, Type: BucketTypeHash})
	put := func(key string, doc map[string]interface{}, wc string) {
		t.Helper()
		if frame := tc.frame(sc.ownerIndex, transport.OpPut, InsertRequest{
			Database: testDatabase, Bucket: sc.bucket, Key: key, Data: doc, WriteConcern: wc,
		}); frame.Opcode == transport.OpError {
			t.Fatalf("put %s at w=%s: %s", key, wc, string(frame.Payload))
		}
	}
	put("seed", map[string]interface{}{"n": 0}, "all")

	// 20 MB payloads overflow the 64 MB segment, so a real retention pass can
	// delete whole files and move the horizon (the technique of
	// TestCatchUpRefusesWhenPeerIsBelowRetainedHorizon). They are deleted
	// again so the snapshot the newcomer pulls stays small; the deletes are
	// log entries like any other.
	blob := strings.Repeat("x", 20<<20)
	const blobs = 6
	for i := 0; i < blobs; i++ {
		put(fmt.Sprintf("big%02d", i), map[string]interface{}{"blob": blob}, "all")
	}
	for i := 0; i < blobs; i++ {
		if frame := tc.frame(sc.ownerIndex, transport.OpDelete, deleteWithConcern{
			Database: testDatabase, Bucket: sc.bucket, Key: fmt.Sprintf("big%02d", i), WriteConcern: "all",
		}); frame.Opcode == transport.OpError {
			t.Fatalf("delete big%02d: %s", i, string(frame.Payload))
		}
	}
	oc := tc.nodes[sc.ownerIndex].server.core.cluster
	ownerWal := tc.nodes[sc.ownerIndex].server.core.wal
	if ownerWal == nil {
		t.Fatal("owner has no WAL")
	}
	if err := oc.pruneWAL(); err != nil {
		t.Fatalf("retention pass on the owner: %v", err)
	}
	first, err := ownerWal.FirstSequence()
	if err != nil {
		t.Fatalf("first sequence: %v", err)
	}
	if first <= 1 {
		t.Fatalf("test setup: the retention pass left the horizon at %d; the payloads did not rotate a segment", first)
	}

	joiner := joinLiveReplica(t, tc, ports, nil)
	jc := joiner.server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := jc.ensureMigrated(ctx, testDatabase, sc.bucket); err != nil {
		t.Fatalf("seed the new replica: %v", err)
	}
	if resp, err := joiner.server.core.Find(FindRequest{Database: testDatabase, Bucket: sc.bucket, Key: "seed"}); err != nil || !resp.Found {
		t.Fatalf("snapshot did not land on the new replica (found=%v err=%v)", resp != nil && resp.Found, err)
	}

	// A write after the seeding must reach the new replica.
	put("after", map[string]interface{}{"n": 1}, "majority")
	newPeer := oc.peerFor(mustNode(t, oc, joiner.addr))
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := joiner.server.core.Find(FindRequest{Database: testDatabase, Bucket: sc.bucket, Key: "after"})
		if err == nil && resp.Found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the new replica never received a write made after its snapshot landed (owner's view of its position: %d, retained horizon: %d)",
				newPeer.ackedThrough(oc.self.ID), first)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if pos := newPeer.ackedThrough(oc.self.ID); pos == 0 {
		t.Fatal("the owner's view of the new replica's position is still zero after it received a write")
	}

	// ... and the strictest write concern is achievable again.
	put("after-all", map[string]interface{}{"n": 2}, "all")
}

// TestCatchUpDoesNotCreateBucketPendingSnapshot is the acceptance test
// for the catch-up-first ordering: while a snapshot pull of a bucket is
// landing on a new replica (its position recorded, its copy not yet in
// place), the owner's catch-up must not be able to open the bucket there by
// replaying its CREATE_BUCKET. Before the fix the DDL exemption in
// Cluster.applyLocal let the replay create and populate the store, and the
// pull then failed on Badger's directory lock. Since BUG-0729 the refusal
// holds only while the pull lands: a pull that has not recorded its position
// finds the bucket served and stops, so once the landing ends the replay opens
// the bucket.
func TestCatchUpDoesNotCreateBucketPendingSnapshot(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, autoRebalanceOffOnNodeZero)
	defer tc.stop()
	ports := reserveNodePorts(t)
	sc := findSeededReplicaScenario(t, tc, ports.ringNode(), "pend")

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(sc.ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: sc.bucket, Type: BucketTypeHash})
	tc.ok(sc.ownerIndex, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: sc.bucket, Key: "seed",
		Data: map[string]interface{}{"n": 0}, WriteConcern: "all",
	})

	// The pull is marked pending, which also keeps the new replica's own sweep
	// from pulling the bucket, and landing.
	var clearPending, endLanding func()
	joiner := joinLiveReplica(t, tc, ports, func(svc *Service) {
		clearPending = markPullPending(svc.core.cluster, sc.bucket)
		endLanding = svc.core.cluster.beginLanding(testDatabase, sc.bucket)
	})
	defer clearPending()
	defer endLanding()

	oc := tc.nodes[sc.ownerIndex].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// The replay to the new replica is refused at the bucket's create, and the
	// refusal names the bucket; nothing is opened while the pull is pending.
	joinerPeer := oc.peerFor(mustNode(t, oc, joiner.addr))
	joinerPeer.stall("a test asked for a replay")
	err := waitStalledOn(ctx, joinerPeer, sc.bucket)
	if joiner.server.core.bucketStoreOpen(testDatabase, sc.bucket) {
		t.Fatalf("the replay opened bucket %q on the new replica while its snapshot pull was pending", sc.bucket)
	}
	if err != nil {
		t.Fatalf("the replay to the new replica was not refused naming the bucket, although the replica cannot accept the bucket's history until its snapshot lands: %v", err)
	}

	// The landing ends with no copy put in place (as a refused materialise
	// does) and the pull still marked pending: the replay then delivers the
	// bucket's whole history.
	endLanding()
	for !holdsKeyLocally(joiner, sc.bucket, "seed") {
		if ctx.Err() != nil {
			t.Fatalf("the replay did not deliver bucket %q to the new replica once the landing ended", sc.bucket)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestNewClusterWarnsOnTwoMemberCluster verifies the F-2 startup warning:
// a cluster with exactly two members gets a loud, explicit warning that
// losing either node permanently blocks all future membership changes
// (majority-of-2 is 2), while a single node or three-or-more members stays
// silent about it.
func TestNewClusterWarnsOnTwoMemberCluster(t *testing.T) {
	for _, tc := range []struct {
		name     string
		members  int
		wantWarn bool
	}{
		{"two members warns", 2, true},
		{"three members is silent", 3, false},
		{"single node is silent", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			token, err := GenerateClusterToken()
			if err != nil {
				t.Fatalf("generate cluster token: %v", err)
			}
			if err := SaveClusterToken(dir, ClusterToken{
				Token:     token,
				CreatedAt: time.Now(),
				ExpiresAt: time.Now().Add(time.Hour),
			}); err != nil {
				t.Fatalf("save cluster token: %v", err)
			}

			addrs := make([]string, tc.members)
			for i := range addrs {
				addrs[i] = fabricatedMemberAddr(t)
			}

			cfg := config.DefaultConfig()
			cfg.Storage.DataDir = dir
			cfg.Server.Listen = addrs[0]
			cfg.Server.NodeID = addrs[0]
			cfg.Server.ReplicationFactor = 1
			cfg.HashRing.SeedNodes = addrs

			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))

			s := &Server{config: cfg, kv: openPositionTestStore(t, t.TempDir())}
			if _, err := NewCluster(s, cfg, logger); err != nil {
				t.Fatalf("NewCluster: %v", err)
			}

			gotWarn := strings.Contains(logs.String(), "exactly two members")
			if gotWarn != tc.wantWarn {
				t.Fatalf("members=%d: two-member warning present = %v, want %v\nlog output:\n%s",
					tc.members, gotWarn, tc.wantWarn, logs.String())
			}
		})
	}
}

// TestSameNamedBucketsInDifferentDatabasesArePlacedIndependently is the
// acceptance test: two buckets with one name in two databases are each placed
// by their own (database, bucket) placement key, not both by the bare name.
// The expected owner is computed through a ring.Ring built from the members,
// over storage.PlacementKey — the frozen encoding (entity-placement §A.3, Q4)
// — so the test does not trust the placement code it is checking. A name is
// searched for whose two qualified keys have different owners; with one
// shared owner the test could not tell the fix from the defect.
func TestSameNamedBucketsInDifferentDatabasesArePlacedIndependently(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	databases := []string{"db1", "db2"}
	expected := ring.New(1)
	for _, addr := range tc.addrs {
		if err := expected.AddNode(ring.Node{ID: addr, Addr: addr}); err != nil {
			t.Fatalf("building the expected ring: %v", err)
		}
	}
	qualifiedOwner := func(database, bucket string) string {
		t.Helper()
		owner, err := expected.Owner(string(storage.PlacementKey(database, bucket, nil)))
		if err != nil {
			t.Fatalf("expected owner of %s/%s: %v", database, bucket, err)
		}
		return owner.ID
	}
	name := ""
	for i := 0; i < 256 && name == ""; i++ {
		candidate := fmt.Sprintf("n%d", i)
		if qualifiedOwner(databases[0], candidate) != qualifiedOwner(databases[1], candidate) {
			name = candidate
		}
	}
	if name == "" {
		t.Fatal("precondition: no name in n0..n255 whose two qualified keys have different owners")
	}

	for _, database := range databases {
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: database})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: database, Name: name, Type: BucketTypeHash})
		tc.ok(0, transport.OpPut, InsertRequest{
			Database: database, Bucket: name, Key: "origin",
			Data: map[string]interface{}{"database": database},
		})
	}

	for _, database := range databases {
		wantOwner := qualifiedOwner(database, name)
		for i, node := range tc.nodes {
			open := node.server.core.bucketStoreOpen(database, name)
			if want := tc.addrs[i] == wantOwner; open != want {
				t.Errorf("%s/%s open on node %d (%s) = %v, want %v: its placement-key owner is %s",
					database, name, i+1, tc.addrs[i], open, want, wantOwner)
			}
		}
		// Every node, holder or not, answers a read of each bucket with that
		// bucket's own document: the read is routed by the same key.
		for i := range tc.nodes {
			frame := tc.ok(i, transport.OpGet, FindRequest{Database: database, Bucket: name, Key: "origin"})
			var found FindResponse
			if err := bson.Unmarshal(frame.Payload, &found); err != nil {
				t.Fatalf("decoding the read of %s/%s through node %d: %v", database, name, i+1, err)
			}
			stored, _ := found.Data.Lookup("database").StringValueOK()
			if !found.Found || stored != database {
				t.Errorf("read of %s/%s through node %d: found=%v database=%q, want the document written to %s",
					database, name, i+1, found.Found, stored, database)
			}
		}
	}
}

// shippedSequences records the sequence of every entry pushed on it.
type shippedSequences struct {
	peerConn
	mu   *sync.Mutex
	seqs *[]uint64
}

func (s *shippedSequences) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op == transport.OpReplPush {
		var push ReplPushRequest
		if err := bson.Unmarshal(payload, &push); err != nil {
			return nil, fmt.Errorf("decoding a recorded push: %w", err)
		}
		s.mu.Lock()
		for _, entry := range push.Entries {
			*s.seqs = append(*s.seqs, entry.Seq)
		}
		s.mu.Unlock()
	}
	return s.peerConn.SendRequest(ctx, lane, op, payload)
}

// prunedOwnerLog writes until the owner's log has rotated many small segments,
// at write concern all, and runs a real retention pass on it: its horizon is
// then far above 1.
func prunedOwnerLog(t *testing.T, tc *testCluster, ownerIndex int, bucket string, writes int) uint64 {
	t.Helper()
	tc.nodes[ownerIndex].server.core.wal.SetMaxSegmentBytes(512)
	for i := 0; i < writes; i++ {
		if frame := tc.frame(ownerIndex, transport.OpPut, InsertRequest{
			Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("pruned%03d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "all",
		}); frame.Opcode == transport.OpError {
			t.Fatalf("write %d at all: %s", i, frame.Payload)
		}
	}
	if err := tc.nodes[ownerIndex].server.core.cluster.pruneWAL(); err != nil {
		t.Fatalf("retention pass on the owner: %v", err)
	}
	first, err := tc.nodes[ownerIndex].server.core.wal.FirstSequence()
	if err != nil {
		t.Fatalf("the owner's first retained sequence: %v", err)
	}
	if first <= 2 {
		t.Fatalf("test setup: the retention pass left the owner's horizon at %d", first)
	}
	return first
}

// TestOwnerRestartDoesNotWedgeAReplicaBehindTheHorizon is T-85. An
// owner's view of where each replica is was memory only, so after a restart
// with a pruned log every replica was at zero, below the horizon, and refused
// for good. The handshake makes the restarted owner start each stream from
// what the replica persisted: a write at all lands inside its timeout, and no
// entry at or below the position the replica reported is sent to it again.
func TestOwnerRestartDoesNotWedgeAReplicaBehindTheHorizon(t *testing.T) {
	var recordRestarted atomic.Bool
	var replicaID atomic.Value
	var shippedMu sync.Mutex
	var shipped []uint64
	tc := newTestClusterTuned(t, 3, 3, nil, func(index int, c *Cluster) {
		id, _ := replicaID.Load().(string)
		if !recordRestarted.Load() || id == "" {
			return
		}
		if node, ok := c.memberByID(id); ok {
			wrapPeerConn(c.peerFor(node), func(inner peerConn) peerConn {
				return &shippedSequences{peerConn: inner, mu: &shippedMu, seqs: &shipped}
			})
		}
	})
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	replicaIndex := (ownerIndex + 1) % 3
	ownerID := tc.addrs[ownerIndex]
	first := prunedOwnerLog(t, tc, ownerIndex, testBucket, 60)
	reported := appliedSnapshot(tc.nodes[replicaIndex].server.core.cluster)[ownerID]
	if reported+1 < first {
		t.Fatalf("test setup: the replica's position %d is below the owner's horizon %d before the restart", reported, first)
	}

	replicaID.Store(tc.addrs[replicaIndex])
	recordRestarted.Store(true)
	tc.restartNode(ownerIndex)
	recordRestarted.Store(false)

	if frame := tc.writeConcerned(ownerIndex, "after-the-restart", "all"); frame.Opcode == transport.OpError {
		t.Fatalf("a write at all after the owner restarted with its log pruned to %d: %s", first, frame.Payload)
	}
	if found, _ := tc.readFromNode(replicaIndex, "after-the-restart"); !found {
		t.Fatal("the replica does not hold a write at all made after the owner restarted")
	}
	shippedMu.Lock()
	defer shippedMu.Unlock()
	for _, seq := range shipped {
		if seq <= reported {
			t.Fatalf("the restarted owner sent the replica sequence %d, at or below the position %d it had persisted (sent %v)", seq, reported, shipped)
		}
	}
}

// TestAReplicaSeededAfterThePruneAndIdleSinceIsNotWedgedByAnOwnerRestart is
// T-86: a node joins a cluster whose owner's log is pruned, pulls its copy, and
// is written nothing; the owner restarts. Its position must survive the
// restart although no entry of the stream ever reached it — only a snapshot.
// On the starting tree the owner kept the joiner's snapshot position in memory
// only and refused it below the horizon after the restart.
func TestAReplicaSeededAfterThePruneAndIdleSinceIsNotWedgedByAnOwnerRestart(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, func(index int, cfg *config.Config) {
		cfg.Server.AutoRebalance = false
	})
	defer tc.stop()
	ports := reserveNodePorts(t)
	sc := findSeededReplicaScenario(t, tc, ports.ringNode(), "idle")
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(sc.ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: sc.bucket, Type: BucketTypeHash})
	prunedOwnerLog(t, tc, sc.ownerIndex, sc.bucket, 40)

	joiner := joinLiveReplica(t, tc, ports, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := joiner.server.core.cluster.ensureMigrated(ctx, testDatabase, sc.bucket); err != nil {
		t.Fatalf("seeding the joiner: %v", err)
	}
	if !joiner.server.core.bucketStoreOpen(testDatabase, sc.bucket) {
		t.Fatal("setup: the joiner's copy did not land")
	}

	tc.restartNode(sc.ownerIndex)
	if frame := tc.frame(sc.ownerIndex, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: sc.bucket, Key: "after-the-restart", Data: map[string]interface{}{"n": 1}, WriteConcern: "all",
	}); frame.Opcode == transport.OpError {
		t.Fatalf("a write at all after the owner restarted, with an idle seeded joiner: %s", frame.Payload)
	}
	if !holdsKey(t, joiner.server.core, testDatabase, sc.bucket, "after-the-restart") {
		t.Fatal("the idle seeded joiner does not hold a write at all made after the owner restarted")
	}
}

// TestMemberListMatchesSelfByID is BUG-0318: a node whose seed entry names it
// by id is a listed member even when its listen address is a wildcard.
// normalizeAddr rewrites 0.0.0.0 to 127.0.0.1, so the address match can never
// find the advertised address in the seed list, and the id was not consulted.
func TestMemberListMatchesSelfByID(t *testing.T) {
	t.Run("listed by id with a wildcard listen address", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.Server.Listen = "0.0.0.0:4433"
		cfg.Server.NodeID = "n1"
		cfg.HashRing.SeedNodes = []string{"n1@192.0.2.5:4433", "n2@192.0.2.6:4433"}

		self, err := selfNode(cfg)
		if err != nil {
			t.Fatalf("selfNode: %v", err)
		}
		if self.Addr != "127.0.0.1:4433" {
			t.Fatalf("precondition: wildcard listen normalised to %q, want 127.0.0.1:4433", self.Addr)
		}

		_, resolved, implicit, err := memberList(cfg, self)
		if err != nil {
			t.Fatalf("memberList: %v", err)
		}
		if implicit {
			t.Errorf("node %q listed by id in the seeds is implicit; want explicit membership", self.ID)
		}
		if resolved.ID != "n1" || resolved.Addr != "192.0.2.5:4433" {
			t.Errorf("resolved self = %+v, want the seed entry n1@192.0.2.5:4433", resolved)
		}
	})

	// The add-node first start: a wildcard node with its own id, not listed
	// yet, beside id@address seeds on other machines. 'join' on a member gives
	// it its address; BUG-0319's refusal must leave this path alone.
	t.Run("absent from the seeds stays implicit", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.Server.Listen = "0.0.0.0:4433"
		cfg.Server.NodeID = "n9"
		cfg.HashRing.SeedNodes = []string{"n1@192.0.2.5:4433", "n2@192.0.2.6:4433"}

		self, err := selfNode(cfg)
		if err != nil {
			t.Fatalf("selfNode: %v", err)
		}
		if _, _, implicit, err := memberList(cfg, self); err != nil {
			t.Fatalf("memberList: %v", err)
		} else if !implicit {
			t.Error("a node absent from the seed list is not implicit; want implicit")
		}
	})
}

// TestMemberListWildcardListenHasNoPhantomMember is BUG-0319. normalizeAddr
// rewrites a wildcard listen address to 127.0.0.1, so a node whose seed entry
// names it by its real address never matches that entry: the ring held both
// the real-address entry and an implicit self on loopback, one member more
// than exists. A real listen address with a loopback seed for its own port is
// the same phantom the other way round. Each must be refused with the fix.
func TestMemberListWildcardListenHasNoPhantomMember(t *testing.T) {
	tests := []struct {
		name      string
		listen    string
		nodeID    string
		defaulted bool
		seeds     []string
		wantInErr []string
	}{
		{
			name:      "wildcard listen, plain seeds, node id defaulted to the hostname",
			listen:    "0.0.0.0:4433",
			nodeID:    "host-a",
			defaulted: true,
			seeds:     []string{"192.0.2.5:4433", "192.0.2.6:4433"},
			wantInErr: []string{"0.0.0.0:4433", "127.0.0.1:4433", "HOARDDB_NODE_ID", "<node-id>@<address>", "HOARDDB_LISTEN"},
		},
		{
			name:      "wildcard listen, plain seeds, explicit node id not listed",
			listen:    "0.0.0.0:4433",
			nodeID:    "n1",
			seeds:     []string{"192.0.2.5:4433", "192.0.2.6:4433"},
			wantInErr: []string{"0.0.0.0:4433", "HOARDDB_NODE_ID", "n1@<address>", "HOARDDB_LISTEN"},
		},
		{
			name:      "real listen address with a loopback seed for its own port",
			listen:    "192.0.2.5:4433",
			nodeID:    "host-a",
			defaulted: true,
			seeds:     []string{"127.0.0.1:4433", "192.0.2.6:4433"},
			wantInErr: []string{"127.0.0.1:4433", "192.0.2.5:4433", "HOARDDB_SEED_NODES"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.Server.Listen = tt.listen
			cfg.Server.NodeID = tt.nodeID
			cfg.Server.NodeIDDefaulted = tt.defaulted
			cfg.HashRing.SeedNodes = tt.seeds

			self, err := selfNode(cfg)
			if err != nil {
				t.Fatalf("selfNode: %v", err)
			}
			members, resolved, _, err := memberList(cfg, self)
			if err == nil {
				t.Fatalf("memberList accepted a phantom: %d members %v for %d seeds, self %+v",
					len(members), memberIDsOf(members), len(tt.seeds), resolved)
			}
			for _, want := range tt.wantInErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error does not name %q: %v", want, err)
				}
			}
		})
	}
}

// tokenTestCluster is the least a Cluster needs to check a presented cluster
// token against the one in its data directory.
func tokenTestCluster(t *testing.T, dataDir string) *Cluster {
	t.Helper()
	stored, err := LoadClusterToken(dataDir)
	if err != nil {
		t.Fatalf("LoadClusterToken: %v", err)
	}
	return &Cluster{
		s:      &Server{config: &config.Config{Storage: config.StorageConfig{DataDir: dataDir}}},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		token:  *stored,
	}
}

// TestClusterTokenSurvivesPastOldExpiry: the token a first start generates is
// the secret every member presents for the life of the cluster, so it is still
// accepted a day later. The file is moved past the 24 hours the first start
// used to stamp into it, which is the state a cluster founded on it reaches a
// day on; nothing reads the clock once the fix is in, so there is no clock to
// advance. A token file an earlier build wrote with an expires_at is honoured
// the same way, and an empty token is still refused.
func TestClusterTokenSurvivesPastOldExpiry(t *testing.T) {
	dataDir := t.TempDir()
	token, generated, err := InitClusterToken(dataDir)
	if err != nil || !generated {
		t.Fatalf("InitClusterToken = %v, generated %v", err, generated)
	}
	stored, err := LoadClusterToken(dataDir)
	if err != nil {
		t.Fatalf("LoadClusterToken: %v", err)
	}
	aDayOn := time.Now().Add(-25 * time.Hour)
	stored.CreatedAt = aDayOn.Add(-time.Hour)
	// What a first start of an earlier build wrote, a day and an hour ago: a
	// token stamped to expire 24 hours after it was made.
	stored.ExpiresAt = aDayOn
	if err := SaveClusterToken(dataDir, *stored); err != nil {
		t.Fatalf("SaveClusterToken: %v", err)
	}

	c := tokenTestCluster(t, dataDir)
	if !c.ValidateToken(token) {
		t.Fatal("a member presenting the cluster's generated token is refused a day after the first start")
	}
	if c.ValidateToken("") {
		t.Fatal("an empty cluster token was accepted")
	}

	fresh := t.TempDir()
	if _, _, err := InitClusterToken(fresh); err != nil {
		t.Fatalf("InitClusterToken: %v", err)
	}
	written, err := LoadClusterToken(fresh)
	if err != nil {
		t.Fatalf("LoadClusterToken: %v", err)
	}
	if !written.ExpiresAt.IsZero() {
		t.Fatalf("a first start stamps its cluster token to expire at %s; a standing cluster token must not expire", written.ExpiresAt)
	}
}

// TestRotatedClusterTokenIsRefusedAtAnyTime: replacing the token file is how a
// token is revoked, and removing the expiry must not keep a replaced token
// working. The old token is refused and the new one accepted, whatever expiry
// either file carries.
func TestRotatedClusterTokenIsRefusedAtAnyTime(t *testing.T) {
	dataDir := t.TempDir()
	old := ClusterToken{Token: strings.Repeat("a", 64), CreatedAt: time.Now()}
	if err := SaveClusterToken(dataDir, old); err != nil {
		t.Fatalf("SaveClusterToken: %v", err)
	}
	c := tokenTestCluster(t, dataDir)
	if !c.ValidateToken(old.Token) {
		t.Fatal("the current cluster token was refused")
	}

	replacement := ClusterToken{Token: strings.Repeat("b", 64), CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}
	if err := SaveClusterToken(dataDir, replacement); err != nil {
		t.Fatalf("SaveClusterToken (replacement): %v", err)
	}
	later := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(filepath.Join(dataDir, "cluster.token"), later, later); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	if !c.ValidateToken(replacement.Token) {
		t.Fatal("the replacement cluster token was refused")
	}
	if c.ValidateToken(old.Token) {
		t.Fatal("the replaced cluster token still works")
	}
}
