package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// attachTwoNodeRing gives a bucketless test server a two-member ring without
// starting a listener: NewCluster builds the ring, the membership file and the
// token state, and spawns nothing. The members are seeded in the explicit
// "id@host:port" form so the node identity and the dial address differ, which
// is what a holder-naming error has to print separately.
func attachTwoNodeRing(t *testing.T, s *Server, selfAddr, peerAddr string) *Cluster {
	t.Helper()

	dir := s.config.Storage.DataDir
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

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dir
	cfg.Server.Listen = selfAddr
	cfg.Server.NodeID = "node-1"
	cfg.Server.ReplicationFactor = 1
	cfg.HashRing.SeedNodes = []string{"node-1@" + selfAddr, "node-2@" + peerAddr}

	cluster, err := NewCluster(s, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	s.cluster = cluster
	return cluster
}

// bucketNameOwnedBy returns a bucket name the ring places on nodeID when it is
// created in database. Placement is a hash of the (database, bucket) placement
// key, so the name is searched for rather than computed; the
// search is bounded so a ring that can never place on nodeID fails the test
// instead of looping.
func bucketNameOwnedBy(t *testing.T, c *Cluster, database, nodeID string) string {
	t.Helper()
	for i := 0; i < 512; i++ {
		name := fmt.Sprintf("bucket-%d", i)
		owner, err := c.Owner(placementOf(database, name))
		if err != nil {
			t.Fatalf("ring owner of %q: %v", name, err)
		}
		if owner.ID == nodeID {
			return name
		}
	}
	t.Fatalf("no bucket name in 512 tries is placed on %q", nodeID)
	return ""
}

// TestResolveBucketNamesTheHolderOnANonHolderNode is GAP-26: on a two-node
// ring, a read for a bucket this node is not the ring owner of must name the
// owner and its address instead of reporting the bucket absent, because "not
// found" from a node that never held the bucket is a wrong answer that looks
// like a right one. A bucket this node *is* the owner of keeps the plain
// "not found" recovery message, so the two stay distinguishable.
func TestResolveBucketNamesTheHolderOnANonHolderNode(t *testing.T) {
	const database = "app"

	s := newTestServer(t)
	selfAddr := fabricatedMemberAddr(t)
	peerAddr := fabricatedMemberAddr(t)
	cluster := attachTwoNodeRing(t, s, selfAddr, peerAddr)

	if _, err := s.CreateDatabase(database); err != nil {
		t.Fatalf("CreateDatabase(%q): %v", database, err)
	}

	heldElsewhere := bucketNameOwnedBy(t, cluster, database, "node-2")
	ownedHere := bucketNameOwnedBy(t, cluster, database, "node-1")

	_, err := s.resolveBucket(database, heldElsewhere)
	if err == nil {
		t.Fatalf("resolveBucket(%q, %q) succeeded; the bucket does not exist on this node", database, heldElsewhere)
	}
	elsewhereMsg := err.Error()
	for _, want := range []string{"node-2", peerAddr, "does not hold"} {
		if !strings.Contains(elsewhereMsg, want) {
			t.Errorf("error for a bucket held elsewhere does not mention %q.\ngot: %s", want, elsewhereMsg)
		}
	}

	_, err = s.resolveBucket(database, ownedHere)
	if err == nil {
		t.Fatalf("resolveBucket(%q, %q) succeeded; the bucket was never created", database, ownedHere)
	}
	absentMsg := err.Error()
	if !strings.Contains(absentMsg, "not found in database") {
		t.Errorf("a genuinely absent bucket this node owns lost its 'not found' recovery message.\ngot: %s", absentMsg)
	}
	if strings.Contains(absentMsg, peerAddr) {
		t.Errorf("a genuinely absent bucket this node owns was blamed on the peer %s.\ngot: %s", peerAddr, absentMsg)
	}
	if elsewhereMsg == absentMsg {
		t.Errorf("a bucket held elsewhere and a genuinely absent bucket produce the same error, "+
			"so a client cannot tell an empty result from a misrouted read.\ngot: %s", absentMsg)
	}
}

// TestResolveBucketKeepsTheJoinFenceDistinct pins the second half of GAP-26:
// when this node *is* the ring owner, the holder-naming message must not fire.
// The owner-with-no-local-copy condition belongs to
// cluster_join.go:ErrJoinSyncIncomplete, and masking it with "connect to a
// holder" would send an operator to a node that is already the holder.
func TestResolveBucketKeepsTheJoinFenceDistinct(t *testing.T) {
	const database = "app"

	s := newTestServer(t)
	selfAddr := fabricatedMemberAddr(t)
	peerAddr := fabricatedMemberAddr(t)
	cluster := attachTwoNodeRing(t, s, selfAddr, peerAddr)

	if _, err := s.CreateDatabase(database); err != nil {
		t.Fatalf("CreateDatabase(%q): %v", database, err)
	}

	ownedHere := bucketNameOwnedBy(t, cluster, database, "node-1")
	if _, err := cluster.fenceUnheldBucket(context.Background(), transport.OpPut, database, ownedHere, nil); err == nil {
		t.Fatalf("the join fence did not fence a bucket this node owns but does not hold")
	}

	_, err := s.resolveBucket(database, ownedHere)
	if err == nil {
		t.Fatalf("resolveBucket(%q, %q) succeeded; the bucket was never created", database, ownedHere)
	}
	if strings.Contains(err.Error(), "does not hold") {
		t.Errorf("the holder-naming message fired for a bucket this node is the ring owner of, "+
			"masking the join fence.\ngot: %s", err.Error())
	}
}

// TestFailedMaterialiseDoesNotReadAsADrop: a pull that fails after
// reserveBucketID has reserved an id, but before its catalog batch commits,
// leaves a reclaim record (reclaimFailedMaterialize) carrying the pulled
// bucket's Database and Name — the same fields recentlyDroppedBucket matches a
// genuine drop by. A write to that bucket's name on its ring owner must get
// ErrJoinSyncIncomplete ("rebalancing has not moved the data here yet"), the
// same as any other bucket this node owns but has not yet received, never
// bucketDroppedErr ("create it again"): the data was never dropped, it is
// still on the previous holder, and "create it again" strands it there.
func TestFailedMaterialiseDoesNotReadAsADrop(t *testing.T) {
	const database = "app"

	s := newTestServer(t)
	selfAddr := fabricatedMemberAddr(t)
	peerAddr := fabricatedMemberAddr(t)
	cluster := attachTwoNodeRing(t, s, selfAddr, peerAddr)

	if _, err := s.CreateDatabase(database); err != nil {
		t.Fatalf("CreateDatabase(%q): %v", database, err)
	}
	source := newMigrationSource(t, s, "source")
	failing := bucketNameOwnedBy(t, cluster, database, "node-1")

	// Hold the reclaimer at the top of its lap before the failed materialise
	// exists, so its own background lap cannot race adoptFailedMaterializations
	// or the range pass against the direct reclaimFailedMaterialize call below —
	// the record must still be present, unprocessed, when the join fence reads it.
	holdReclaimer(t)

	reservedID := failNextMaterializeCatalog(t)
	err := s.materializeRange(migratedBucket{database: database, name: failing, storeType: BucketTypeBlob}, source.stream, 0)
	if !errors.Is(err, errMaterializeCrash) {
		t.Fatalf("the materialise of %q returned %v, want the simulated crash", failing, err)
	}
	failedID := reservedID()

	converted, err := s.reclaimFailedMaterialize(failedID)
	if err != nil {
		t.Fatalf("reclaimFailedMaterialize(%d): %v", failedID, err)
	}
	if !converted {
		t.Fatalf("reclaimFailedMaterialize(%d) did not convert a record; precondition failed", failedID)
	}
	if _, catalogued, err := s.readCatalogEntry(database, failing); err != nil || catalogued {
		t.Fatalf("precondition: %q is catalogued (%v, %v); the failed materialise must leave it uncatalogued", failing, catalogued, err)
	}

	_, err = cluster.fenceUnheldBucket(context.Background(), transport.OpPut, database, failing, nil)
	if err == nil {
		t.Fatalf("the join fence did not fence %q, a bucket this node owns but does not hold", failing)
	}
	if !errors.Is(err, ErrJoinSyncIncomplete) {
		t.Fatalf("a write to %q — whose materialise failed and left only a reclaim record — got %q, want ErrJoinSyncIncomplete "+
			"(\"rebalancing has not moved the data here yet\"): the failed materialise's reclaim record was read as a drop", failing, err)
	}
}

// TestReadOfABucketHeldElsewhereIsNotAnInternalFault is the not-holder part of
// the refusal-code fix: the refusal names the holder, and its wire code says "ask another
// node" (UNAVAILABLE), not a server fault (INTERNAL).
func TestReadOfABucketHeldElsewhereIsNotAnInternalFault(t *testing.T) {
	const database = "app"

	s := newTestServer(t)
	cluster := attachTwoNodeRing(t, s, fabricatedMemberAddr(t), fabricatedMemberAddr(t))
	if _, err := s.CreateDatabase(database); err != nil {
		t.Fatalf("CreateDatabase(%q): %v", database, err)
	}
	heldElsewhere := bucketNameOwnedBy(t, cluster, database, "node-2")

	_, err := s.resolveBucket(database, heldElsewhere)
	if err == nil {
		t.Fatalf("resolveBucket(%q, %q) succeeded; the bucket does not exist on this node", database, heldElsewhere)
	}
	if got := codeFor(err); got != transport.ErrUnavailable {
		t.Errorf("codeFor(not-holder refusal) = %s, want %s", got, transport.ErrUnavailable)
	}
}
