package server

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// retentionTestCluster attaches a one-member, RF=1 cluster and a plaintext WAL
// to a bare server over the fault store. With no other member no peer is
// relevant, so replicaAckBound falls back to the log's last sequence — the
// power-loss case. Nothing is started: the test drives the write path
// and the retention pass itself.
func retentionTestCluster(t *testing.T, s *Server, dataDir string) *Cluster {
	t.Helper()
	token, err := GenerateClusterToken()
	if err != nil {
		t.Fatalf("generating a cluster token: %v", err)
	}
	if err := SaveClusterToken(dataDir, ClusterToken{Token: token, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("saving the cluster token: %v", err)
	}
	const addr = "127.0.0.1:1"
	s.config.Server.Listen = addr
	s.config.Server.NodeID = addr
	s.config.Server.ReplicationFactor = 1
	s.config.HashRing.SeedNodes = []string{addr}

	w, err := storage.OpenWAL(filepath.Join(dataDir, "wal"), nil)
	if err != nil {
		t.Fatalf("opening the WAL: %v", err)
	}
	s.wal = w
	c, err := NewCluster(s, s.config, discardLogger())
	if err != nil {
		t.Fatalf("building the cluster: %v", err)
	}
	s.cluster = c
	return c
}

// applyPath is one of the two ways an entry reaches this node's log and
// store: as the bucket's owner, or as a replica applying what an owner shipped.
type applyPath func(c *Cluster, entry ReplEntry) (ReplResponse, error)

func asOwner(c *Cluster, entry ReplEntry) (ReplResponse, error) {
	return c.applyAsOwner(context.Background(), entry, entryPlacement(entry), WcOne)
}

func asReplica(c *Cluster, entry ReplEntry) (ReplResponse, error) {
	// Stamped with the term the copy follows, as its owner's entry would be
	// (steps 9.2 and 10b.2).
	if entry.Term == 0 && judgedByLineage(transport.Opcode(entry.Op), entry.Bucket) {
		_, database := decodeFrameMeta(entry.Payload).scope(transport.Opcode(entry.Op))
		if term, _, err := c.s.bucketLineage(database, entry.Bucket); err == nil {
			entry.Term = term
		}
	}
	return c.applyAsReplica(context.Background(), entry, true, "test-owner", false)
}

// putThrough writes key=value into bucket along path and returns an error
// unless the write is acknowledged.
func putThrough(c *Cluster, path applyPath, bucket, key string, value interface{}) error {
	payload, err := bson.Marshal(InsertRequest{Database: "app", Bucket: bucket, Key: key, Data: map[string]interface{}{"v": value}})
	if err != nil {
		return fmt.Errorf("encoding the put of %q: %w", key, err)
	}
	resp, err := path(c, ReplEntry{Op: int32(transport.OpPut), Bucket: bucket, Key: []byte(key), Payload: payload})
	if err != nil {
		return fmt.Errorf("putting %q into %q: %w", key, bucket, err)
	}
	if transport.Opcode(resp.Opcode) == transport.OpError {
		return fmt.Errorf("putting %q into %q: answered with an error frame: %s", key, bucket, resp.Payload)
	}
	return nil
}

// TestRetentionKeepsAnAcknowledgedWriteAcrossAPowerLoss reproduces the
// power-loss case on the fault store. At RF=1 the retention bound is the WAL's
// last sequence, which counts a write that is appended and not yet applied.
// The window: the held write is appended (in segment 1) and stopped before its
// apply; a later write to another bucket is appended, rotates the log into
// segment 2, and applies; a retention pass runs, syncs the store and truncates.
// Then the held write applies — unsynced — and is acknowledged, and the power
// goes. The write must survive: either the store or the log still holds it.
//
// Both paths into the log are driven: the owner's, and a replica's — the bug's
// "a node that owns no bucket but applies as a replica" has no relevant peer
// either, so its bound is the same fallback.
func TestRetentionKeepsAnAcknowledgedWriteAcrossAPowerLoss(t *testing.T) {
	for _, tt := range []struct {
		name string
		held applyPath
	}{
		{name: "owner", held: asOwner},
		{name: "replica", held: asReplica},
	} {
		t.Run(tt.name, func(t *testing.T) { retentionPowerLoss(t, tt.held) })
	}
}

func retentionPowerLoss(t *testing.T, heldPath applyPath) {
	dataDir := t.TempDir()
	kv := kvfault.New()
	// The fillers are 20 MB documents, so segment 1 fills in a few writes; the
	// fault store's default batch bound would refuse them.
	kv.SetMaxBatchBytes(128 << 20)
	s := newKVFaultServer(dataDir, kv)
	for _, name := range []string{"held", "other"} {
		if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeHash}); err != nil || !resp.Success {
			t.Fatalf("creating bucket %q: %+v, %v", name, resp, err)
		}
	}
	if err := kv.Sync(); err != nil {
		t.Fatalf("syncing the catalog: %v", err)
	}
	c := retentionTestCluster(t, s, dataDir)
	defer c.cancel()

	// 60 MB in segment 1, below the 64 MB rotation point.
	blob := strings.Repeat("x", 20<<20)
	for _, key := range []string{"fill0", "fill1", "fill2"} {
		if err := putThrough(c, asOwner, "other", key, blob); err != nil {
			t.Fatalf("filler %s: %v", key, err)
		}
	}

	reached := make(chan struct{})
	release := make(chan struct{})
	hold := func(_ string, entry ReplEntry) {
		if entry.Bucket == "held" {
			close(reached)
			<-release
		}
	}
	beforeLocalApplyHook.Store(&hold)
	t.Cleanup(func() { beforeLocalApplyHook.Store(nil) })

	acked := make(chan error, 1)
	go func() { acked <- putThrough(c, heldPath, "held", "k", "acknowledged") }()
	waitFor(t, reached, "the held write to be appended and stopped before its apply")
	heldSeq := s.wal.LastSequence()

	// Another bucket's write does not wait for the held one's apply turn. It
	// does not fit in segment 1, so it opens segment 2 and seals the held
	// write's segment.
	if err := putThrough(c, asOwner, "other", "fill3", blob); err != nil {
		t.Fatalf("the write that seals segment 1: %v", err)
	}
	if last := s.wal.LastSequence(); last != heldSeq+1 {
		t.Fatalf("test setup: the log's last sequence is %d, want %d (held write at %d, then one more)", last, heldSeq+1, heldSeq)
	}
	if n := s.owed.count(); n != 0 {
		t.Fatalf("test setup: %d applies failed, so the owed registry, not this bug's term, would hold the bound", n)
	}

	if bound := c.retentionBound(); bound >= heldSeq {
		t.Errorf("the retention bound is %d with seq %d appended and not applied: a pass may truncate it", bound, heldSeq)
	}
	if err := c.pruneWAL(); err != nil {
		t.Fatalf("retention pass: %v", err)
	}

	close(release)
	select {
	case err := <-acked:
		if err != nil {
			t.Fatalf("the held write was not acknowledged: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the held write to be acknowledged")
	}

	kv.PowerLoss()
	if err := s.wal.Close(); err != nil {
		t.Fatalf("closing the WAL: %v", err)
	}
	restarted := newKVFaultServer(dataDir, kv)
	if err := restarted.LoadCatalog(); err != nil {
		t.Fatalf("LoadCatalog after the power loss: %v", err)
	}
	w, err := storage.OpenWAL(filepath.Join(dataDir, "wal"), nil)
	if err != nil {
		t.Fatalf("reopening the WAL: %v", err)
	}
	defer func() { _ = w.Close() }()
	restarted.wal = w
	if err := restarted.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the WAL: %v", err)
	}

	found, err := restarted.Find(FindRequest{Database: "app", Bucket: "held", Key: "k"})
	if err != nil {
		t.Fatalf("reading the acknowledged write after the power loss: %v", err)
	}
	if !found.Found {
		first, firstErr := w.FirstSequence()
		t.Fatalf("an acknowledged write (seq %d) is lost after a power loss: the store never synced it and the log starts at %d (%v) — retention truncated it before it applied",
			heldSeq, first, firstErr)
	}
}

// TestRetentionBoundStaysBelowAReplicaApplyInFlight is the same bound on a
// node that does have a relevant peer. Node A owns X and is a replica of Y. A
// write to Y reaches A's log and is held before A applies it; A then takes a
// write to X, which B acknowledges. B's acknowledgement is a position in A's
// log, and it is above the held entry, which A appended as a replica and never
// ships — so the peer bound covers an entry that has not applied. The bug
// file's "when peers exist the bound is already safe" holds only for entries
// this node ships.
func TestRetentionBoundStaysBelowAReplicaApplyInFlight(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})

	a := tc.nodes[0].server.core
	var ownedByA, ownedByB string
	for i := 0; i < 64 && (ownedByA == "" || ownedByB == ""); i++ {
		name := fmt.Sprintf("bucket%02d", i)
		owner, err := a.cluster.Owner(placementOf(testDatabase, name))
		if err != nil {
			t.Fatalf("owner of %q: %v", name, err)
		}
		switch {
		case owner.ID == a.cluster.self.ID && ownedByA == "":
			ownedByA = name
		case owner.ID != a.cluster.self.ID && ownedByB == "":
			ownedByB = name
		}
	}
	if ownedByA == "" || ownedByB == "" {
		t.Fatalf("test setup: no pair of bucket names split between the two nodes (A owns %q, B owns %q)", ownedByA, ownedByB)
	}
	// RF=2 on two nodes: the creates return once both copies exist, so no
	// create is still on its way to A when the hook goes in.
	for _, name := range []string{ownedByA, ownedByB} {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
	}

	// The first apply of the Y write is B's own, as owner; the second is A's,
	// as replica. Hold the second.
	var applies atomic.Int32
	reached := make(chan struct{})
	release := make(chan struct{})
	hold := func(_ string, entry ReplEntry) {
		if entry.Bucket == ownedByB && applies.Add(1) == 2 {
			close(reached)
			<-release
		}
	}
	beforeLocalApplyHook.Store(&hold)
	t.Cleanup(func() { beforeLocalApplyHook.Store(nil) })
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce()

	heldDone := make(chan error, 1)
	go func() {
		client := tc.dial(1)
		defer func() { _ = client.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		payload, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: ownedByB, Key: "held", Data: map[string]interface{}{"v": 1}, WriteConcern: "majority"})
		if err != nil {
			heldDone <- fmt.Errorf("encoding the held write: %w", err)
			return
		}
		frame, err := client.SendRequest(ctx, transport.OpPut, payload)
		if err == nil && frame.Opcode == transport.OpError {
			err = fmt.Errorf("answered with an error frame: %s", frame.Payload)
		}
		heldDone <- err
	}()
	waitFor(t, reached, "node A's replica apply of the Y write to be appended and held")
	heldSeq := a.wal.LastSequence()

	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: ownedByA, Key: "later", Data: map[string]interface{}{"v": 2}, WriteConcern: "majority"})
	if a.wal.LastSequence() <= heldSeq {
		t.Fatalf("test setup: A's write to %q did not reach A's log after the held entry %d", ownedByA, heldSeq)
	}

	if bound := a.cluster.retentionBound(); bound >= heldSeq {
		t.Errorf("A's retention bound is %d with its seq %d appended as a replica and not applied: a pass may truncate it", bound, heldSeq)
	}

	releaseOnce()
	select {
	case err := <-heldDone:
		if err != nil {
			t.Fatalf("the held write: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the held write")
	}
}
