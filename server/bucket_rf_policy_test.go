package server

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The refusals a create with a factor needs are about the cluster's shape at
// the moment the DDL is accepted, so they are decided once, by the owner (or
// by a node that does not route) before the create is logged — never by a
// replica applying it, never by a replay (review F-3).

// createRefusal returns why a create was refused, or "" when it was accepted. A
// refusal is either an error frame or a response that says Success false.
func createRefusal(t *testing.T, frame *transport.Frame) string {
	t.Helper()
	if frame.Opcode == transport.OpError {
		return string(frame.Payload)
	}
	var resp CreateBucketResponse
	if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
		t.Fatalf("decoding the create's response: %v", err)
	}
	if resp.Success {
		return ""
	}
	return resp.Message
}

// requireNoBucketAnywhere fails unless no node has a catalog entry for the
// bucket or serves it.
func requireNoBucketAnywhere(t *testing.T, tc *testCluster, database, bucket string) {
	t.Helper()
	for index, node := range tc.nodes {
		core := node.server.core
		if _, catalogued, err := core.readCatalogEntry(database, bucket); err != nil || catalogued {
			t.Fatalf("node %d has a catalog entry for %s.%s (%v)", index+1, database, bucket, err)
		}
		if core.bucketStoreOpen(database, bucket) {
			t.Fatalf("node %d serves %s.%s", index+1, database, bucket)
		}
	}
}

// walCreates counts the creates of a bucket in a node's write-ahead log.
func walCreates(t *testing.T, wal *storage.WAL, bucket string) int {
	t.Helper()
	entries, err := wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading the WAL: %v", err)
	}
	creates := 0
	for _, entry := range entries {
		if transport.Opcode(entry.Op) == transport.OpCreateBucket && entry.Bucket == bucket {
			creates++
		}
	}
	return creates
}

// TestFactorAboveMemberCountIsRefusedAtDDL is T-09. Ring.Replicas clamps a
// factor larger than the member count silently, so without this refusal a
// bucket reports rf 5 while holding 3 copies — a durability claim the system
// does not honour and nobody is told about. Refusing a new instruction costs an
// error message; refusing to serve existing data because the cluster shrank
// would cost an outage, which is why only the DDL path refuses.
//
// Ported from ea34944, re-aimed: the create goes through the client path, where
// the owner judges it, and "no directory" is "no catalog entry and no attached
// bucket on any node".
func TestFactorAboveMemberCountIsRefusedAtDDL(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	refusal := createRefusal(t, tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase, Name: "toowide", Type: BucketTypeHash, ReplicationFactor: 5,
	}))
	if refusal == "" {
		t.Fatal("replication_factor 5 was accepted on a 3-node cluster")
	}
	for _, want := range []string{"5", "3 members", "toowide", "replication_factor 3"} {
		if !strings.Contains(refusal, want) {
			t.Fatalf("the refusal does not name %q: %s", want, refusal)
		}
	}
	requireNoBucketAnywhere(t, tc, testDatabase, "toowide")

	if refusal := createRefusal(t, tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase, Name: "justright", Type: BucketTypeHash, ReplicationFactor: 3,
	})); refusal != "" {
		t.Fatalf("replication_factor 3 was refused on a 3-node cluster: %s", refusal)
	}
}

// TestARefusedCreateIsNotInAnyLogAndARestartDoesNotCreateIt is review F-3(a). A
// create the policy refuses must be refused before it is appended: an owner
// appends before it applies and ships whether or not the apply succeeded, so a
// refusal made inside the handler leaves the create in the owner's log and in
// every replica's, and replay after a restart dispatches it again. The policy
// that lets a replay pass then creates a bucket the client was told was
// refused.
func TestARefusedCreateIsNotInAnyLogAndARestartDoesNotCreateIt(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	if createRefusal(t, tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase, Name: "toowide", Type: BucketTypeHash, ReplicationFactor: 5,
	})) == "" {
		t.Fatal("replication_factor 5 was accepted on a 3-node cluster")
	}

	for index, node := range tc.nodes {
		if got := walCreates(t, node.server.core.wal, "toowide"); got != 0 {
			t.Fatalf("node %d logged %d creates of a bucket the policy refused", index+1, got)
		}
	}
	for index := range tc.nodes {
		tc.restartNode(index)
	}
	requireNoBucketAnywhere(t, tc, testDatabase, "toowide")
}

// TestStandaloneServerRefusesAFactorAboveOne is T-19. A server with no seed
// nodes and a factor of 1 has no write-ahead log and no cluster and cannot
// acquire either without a restart, so a factor above one is refused rather than
// stored for later: a stored factor the process cannot honour survives into a
// catalog a later reader will believe.
//
// Ported from ea34944, re-aimed: the entry point is the one a server that does
// not route uses (fencedDispatch), and the refusal leaves no catalog entry.
func TestStandaloneServerRefusesAFactorAboveOne(t *testing.T) {
	s := newTestServer(t)
	// The premise first, or the test proves nothing: a server that happened to
	// have a cluster would refuse for another reason.
	if s.cluster != nil || s.wal != nil {
		t.Fatalf("this server has a cluster (%v) or a WAL (%v); it is not standalone", s.cluster != nil, s.wal != nil)
	}
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}

	create := func(name string, factor int) string {
		frame, err := s.fencedDispatch(context.Background(), transport.NewFrame(transport.OpCreateBucket,
			mustMarshal(t, CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeHash, ReplicationFactor: factor})))
		if err != nil {
			t.Fatalf("creating %q: %v", name, err)
		}
		return createRefusal(t, frame)
	}

	refusal := create("Sessions", 3)
	if refusal == "" {
		t.Fatal("a standalone server accepted replication_factor 3")
	}
	for _, want := range []string{"standalone", "the replication page of the user docs", "leave replication_factor out"} {
		if !strings.Contains(refusal, want) {
			t.Fatalf("the refusal does not name %q: %s", want, refusal)
		}
	}
	// D-3: not a step that can stop the node from starting.
	for _, advice := range []string{"HOARDDB_SEED_NODES", "HOARDDB_REPLICATION_FACTOR", "restart"} {
		if strings.Contains(refusal, advice) {
			t.Fatalf("the refusal recommends %q, which can break a node that holds encrypted or user data: %s", advice, refusal)
		}
	}
	if _, catalogued, err := s.readCatalogEntry("app", "Sessions"); err != nil || catalogued {
		t.Fatalf("the refused create left a catalog entry (%v)", err)
	}
	if s.bucketStoreOpen("app", "Sessions") {
		t.Fatal("the refused create left the bucket attached")
	}
	// One copy is what it can do, so one copy is accepted — and so is saying
	// nothing.
	if refusal := create("Sessions", 1); refusal != "" {
		t.Fatalf("a standalone server refused replication_factor 1: %s", refusal)
	}
	if refusal := create("Plain", 0); refusal != "" {
		t.Fatalf("a standalone server refused a bucket that declared no factor: %s", refusal)
	}
}

// startCatalogServerLogging is startCatalogServerAt with a logger the test can
// read, for the assertions about what a start says.
func startCatalogServerLogging(t *testing.T, dataDir string, logger *slog.Logger) *catalogServer {
	t.Helper()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("loading default config: %v", err)
	}
	cfg.Storage.DataDir = dataDir
	cfg.Server.ReplicationFactor = 1
	qs, err := NewService(cfg, logger)
	if err != nil {
		t.Fatalf("creating server: %v", err)
	}
	cs := &catalogServer{qs: qs, core: qs.core}
	t.Cleanup(func() {
		if !cs.stopped {
			cs.stopped = true
			_ = qs.Stop()
		}
	})
	return cs
}

// TestPortableCatalogStartsStandaloneAndKeepsTheStoredFactor is T-20, the half
// that matters more than the refusal. A data directory is portable: a catalog
// carrying replication_factor 3 can be started by a standalone binary — a
// restore onto a laptop, a single-node recovery, an operator who dropped the
// environment variable. The node must start, because the commonest way to reach
// this state is a restore, and a restore that produces a server which will not
// start is a recovery tool that fails when it is needed. It keeps the value
// rather than quietly rewriting it to 1, and says once, with the count and at
// most ten names, that it cannot honour it.
//
// It is also what catches the refusal being put one function too deep: move it
// into createBucketLocked and this fails, because LoadCatalog reaches that
// function and T-19 still passes.
//
// Ported from ea34944, rewritten: the portable entry is a catalogEntry written
// into a node store, not a JSON file.
func TestPortableCatalogStartsStandaloneAndKeepsTheStoredFactor(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	if resp, err := first.core.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	const declared = 12
	for i := 0; i < declared; i++ {
		name := "Sessions" + string(rune('A'+i))
		if resp, err := first.core.CreateBucket(CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeHash}); err != nil || !resp.Success {
			t.Fatalf("CreateBucket(%s) = %+v, %v", name, resp, err)
		}
		entry, catalogued, err := first.core.readCatalogEntry("app", name)
		if err != nil || !catalogued {
			t.Fatalf("readCatalogEntry(%s) = %v, %v", name, catalogued, err)
		}
		entry.ReplicationFactor = 3
		encoded, err := encodeCatalogEntry(entry)
		if err != nil {
			t.Fatalf("encoding: %v", err)
		}
		batch := first.core.kv.NewBatch()
		if err := batch.Set(catalogKey("app", name), encoded); err != nil {
			t.Fatalf("stamping %s: %v", name, err)
		}
		if err := batch.Commit(true); err != nil {
			t.Fatalf("committing %s: %v", name, err)
		}
		batch.Discard()
	}
	first.stop(t)

	logged := &syncBuffer{}
	second := startCatalogServerLogging(t, dataDir, slog.New(slog.NewTextHandler(logged, &slog.HandlerOptions{Level: slog.LevelWarn})))
	if err := second.qs.LoadCatalog(); err != nil {
		t.Fatalf("a standalone server refused to start on a catalog carrying replication_factor 3: %v", err)
	}
	if second.core.cluster != nil {
		t.Fatal("the second server has a cluster; the test is not exercising the standalone path")
	}
	entry, _, err := second.core.readCatalogEntry("app", "SessionsA")
	if err != nil || entry.ReplicationFactor != 3 {
		t.Fatalf("the catalog entry's factor is %d (%v), want 3 preserved", entry.ReplicationFactor, err)
	}
	if resp, err := second.core.Insert(InsertRequest{Database: "app", Bucket: "SessionsA", Key: "k", Data: map[string]interface{}{"n": 1}}); err != nil || !resp.Success {
		t.Fatalf("the bucket does not accept writes: %+v, %v", resp, err)
	}
	if resp, err := second.core.Find(FindRequest{Database: "app", Bucket: "SessionsA", Key: "k"}); err != nil || !resp.Found {
		t.Fatalf("the bucket does not serve reads: %+v, %v", resp, err)
	}

	warnings := strings.Count(logged.String(), "cannot keep")
	if warnings != 1 {
		t.Fatalf("the start logged %d warnings about the factors it cannot honour, want exactly 1:\n%s", warnings, logged.String())
	}
	line := logged.String()
	if !strings.Contains(line, "buckets=12") || !strings.Contains(line, "the replication page of the user docs") {
		t.Fatalf("the warning does not carry the count (12) and where to read: %s", line)
	}
	if named := strings.Count(line, "app.Sessions"); named != 10 {
		t.Fatalf("the warning names %d buckets, want at most ten (exactly 10 of 12): %s", named, line)
	}
}

// TestWALReplayKeepsABucketWhosePolicyWouldNowRefuseIt is T-31. A create that
// was legal when it was logged is applied on replay although the cluster has
// since changed shape: the entry committed, and the shape refusals are
// questions about the moment the DDL was issued. Here the log holds a create at
// 5 on a three-member cluster, which a client would now be refused.
//
// It is a freeze: with the refusal decided before the append it holds by
// construction. Its guard is the design review F-3 rejected — a refusal in the
// create handler, which a replay dispatches.
func TestWALReplayKeepsABucketWhosePolicyWouldNowRefuseIt(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	restarted := restartNodeAfterEditingItsWAL(t, tc, 2, func(wal *storage.WAL) {
		appendCreateBucketToWAL(t, wal, "replayed", "wide", 5)
	})
	if !restarted.server.core.bucketStoreOpen("replayed", "wide") {
		t.Fatal("replay dropped a create that was legal when it was logged: the bucket is missing after the restart")
	}
	if entry, catalogued, err := restarted.server.core.readCatalogEntry("replayed", "wide"); err != nil || !catalogued || entry.ReplicationFactor != 5 {
		t.Fatalf("the replayed bucket's catalog entry is %+v (catalogued %v, %v), want factor 5", entry, catalogued, err)
	}
}
