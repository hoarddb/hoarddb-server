package server

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// refuseMigrateOn makes every migrate of bucket on node nodeID answer refusal
// until the test ends.
func refuseMigrateOn(t *testing.T, nodeID, bucket string, refusal func() error) {
	t.Helper()
	hook := func(onNode, _, name string) error {
		if onNode != nodeID || name != bucket {
			return nil
		}
		return refusal()
	}
	ensureMigratedHook.Store(&hook)
	t.Cleanup(func() { ensureMigratedHook.Store(nil) })
}

// TestAnOperatorsMigrateRefusedForAHandOffInProgressAnswersUnavailable is
// BUG-0507 on the operator's path: a migrate refused because ownership of the
// bucket is moving to the node answers UNAVAILABLE, which codeFor gives
// ErrHandoffInProgress and a driver retries, with the refusal's sentence, and
// the connection serves the next request. Guard: return the refusal as a
// handler error — the transport answers INTERNAL and closes the connection.
func TestAnOperatorsMigrateRefusedForAHandOffInProgressAnswersUnavailable(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()
	refuser := clusterOf(tc, 0)
	refuseMigrateOn(t, refuser.self.ID, testBucket, func() error { return refuser.handoffInProgress(testDatabase, testBucket) })

	payload, err := bson.Marshal(MigrateRequest{Database: testDatabase, Bucket: testBucket})
	if err != nil {
		t.Fatalf("encoding the migrate request: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	frame, err := tc.nodes[0].client.SendRequest(ctx, transport.OpMigrate, payload)
	if err != nil {
		t.Fatalf("the refused migrate got no answer: %v", err)
	}
	refusal := refusalOf(t, frame)
	if transport.ErrorCode(refusal.Code) != transport.ErrUnavailable {
		t.Errorf("a migrate refused for a hand-off in progress answers code %d, want %d (unavailable): %q",
			refusal.Code, transport.ErrUnavailable, refusal.Message)
	}
	if !strings.Contains(refusal.Message, "is moving to this node") || !strings.Contains(refusal.Message, "Retry in a moment") {
		t.Errorf("a migrate refused for a hand-off in progress reads %q, want the refusal's reason and what to do", refusal.Message)
	}

	next, err := tc.nodes[0].client.SendRequest(ctx, transport.OpListDBs, nil)
	if err != nil {
		t.Fatalf("the connection does not serve a request after a refused migrate: %v", err)
	}
	if next.Opcode == transport.OpError {
		t.Fatalf("the request after a refused migrate was refused: %s", answerText(next))
	}
}

// TestAPeersRefusedMigrateCarriesItsReason is BUG-0507 on the hand-off's path:
// a member asked by a peer to migrate, refusing, tells the asker why. Guard:
// return the refusal as a handler error — the asker reads an error frame as an
// empty MigrateResponse and reports "node <id>: " with no reason.
func TestAPeersRefusedMigrateCarriesItsReason(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()
	asker, member := clusterOf(tc, 0), clusterOf(tc, 1)
	refuseMigrateOn(t, member.self.ID, testBucket, func() error { return member.handoffInProgress(testDatabase, testBucket) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := asker.askNodeToMigrate(ctx, mustNode(t, asker, member.self.ID), testDatabase, testBucket)
	if err == nil {
		t.Fatal("a refused migrate reached the asker as a success")
	}
	if !strings.Contains(err.Error(), "is moving to this node") {
		t.Errorf("a peer's refused migrate reads %q, want the refusal's reason", err)
	}
}

// TestADroppedIncarnationAnswerReachesTheAskerWhichDropsItsCopy is BUG-0507's
// dead branch (R8-C): a member that refuses a hand-off because it dropped the
// incarnation the asker holds says which one, and the asker's hand-off drops
// its copy of it. Guard: return the refusal as a handler error — the dropped
// incarnation never crosses the wire, and the asker keeps its copy.
func TestADroppedIncarnationAnswerReachesTheAskerWhichDropsItsCopy(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 2)
	asker, member := clusterOf(tc, 0), clusterOf(tc, 1)
	incarnation := lineageOf(t, asker.s, testDatabase, testBucket).Incarnation
	if incarnation == 0 {
		t.Fatal("setup: the asker's copy has no incarnation")
	}
	refuseMigrateOn(t, member.self.ID, testBucket, func() error {
		return &pullOfDroppedIncarnationError{database: testDatabase, bucket: testBucket, incarnation: incarnation}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := asker.askNodeToMigrate(ctx, mustNode(t, asker, member.self.ID), testDatabase, testBucket)
	var droppedThere *droppedHereError
	if !errors.As(err, &droppedThere) {
		t.Fatalf("a member's dropped-incarnation refusal reached the asker as %v; want the incarnation it dropped", err)
	}
	if droppedThere.incarnation != incarnation {
		t.Fatalf("the member answered incarnation %d dropped, want %d", droppedThere.incarnation, incarnation)
	}

	replicas, err := asker.Replicas(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("placing the bucket: %v", err)
	}
	if err := asker.handOffBucket(ctx, testDatabase, testBucket, replicas); err != nil {
		t.Fatalf("the hand-off told its copy's incarnation was dropped answered %v; want the copy dropped", err)
	}
	if asker.s.bucketStoreOpen(testDatabase, testBucket) {
		t.Fatal("the asker keeps its copy of an incarnation a member answered dropped")
	}
}
