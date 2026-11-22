package server

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Regression: a LIFO push minted a different _id_ per applier. resolveDocumentIDs
// (server/write_path.go) resolves every other replicated write op's id before
// the entry is logged and shipped, so the owner, every replica, and every
// future replay apply the identical bytes — but it had no transport.OpLIFOPush
// case, so a LIFO push's payload shipped without a resolved _id_, and each
// node's Server.PushCtx (server/service.go) fell back to minting its own
// ids.NewDocumentID(). The owner stored the element under one id and the
// replica stored it under a different one, so a read or a delete keyed on
// _id_ hit one copy and missed the other, with no error anywhere.
//
// When this test was written the divergence was invisible to
// transport.OpLIFOPeek: that opcode was absent from transport.Opcode.IsReadOnly
// until peeks were made read-only, so a peek travelled the replicated write path and was always
// answered by the bucket's owner. A client peek is now served like any read,
// by the node asked when it holds a copy, but which node answers still
// depends on placement. This test therefore reads each node's own copy
// directly — a Server.Peek call, bypassing Cluster routing — so it names the
// copy it reads.

// lifoPushIDMintCluster starts a two-node cluster at RF=2 (both nodes hold
// every write) with a LIFO bucket, so a keyless push's divergence between the
// two nodes is directly observable by peeking each node's own store.
func lifoPushIDMintCluster(t *testing.T) *testCluster {
	t.Helper()
	tc := newTestCluster(t, 2, 2)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeLIFO})
	return tc
}

// lifoPeekLocal peeks the head of the LIFO bucket directly against one node's
// own Server, in-process, the way blobReadFromNode reads a blob key from that
// node's own copy. It does not go through Cluster.handleClientFrame, whose
// routing decides which copy answers.
func (tc *testCluster) lifoPeekLocal(index int) PeekResponse {
	tc.t.Helper()
	resp, err := tc.nodes[index].server.core.Peek(PeekRequest{
		Database: testDatabase,
		Bucket:   testBucket,
	})
	if err != nil {
		tc.t.Fatalf("node %d: local peek: %v", index+1, err)
	}
	return *resp
}

// lifoPeekedElementID extracts _id_ from a found peek response, failing the
// test if the peek came up empty or the element carries no id.
func lifoPeekedElementID(t *testing.T, index int, resp PeekResponse) string {
	t.Helper()
	if !resp.Found {
		t.Fatalf("node %d: peek found nothing: %s", index+1, resp.Message)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("node %d: peek answered %d items, want 1", index+1, len(resp.Items))
	}
	id, ok := bson.Raw(resp.Items[0]).Lookup("_id_").StringValueOK()
	if !ok {
		t.Fatalf("node %d: peeked element %v has no _id_", index+1, resp.Items[0])
	}
	return id
}

// TestLIFOPushSameElementSameIDOnAllReplicas is the acceptance test for per-applier id minting.
// It fails on the pre-fix tree because the two nodes each mint their own id
// for the same logical push: resolveDocumentIDs has no OpLIFOPush case, so
// the payload both nodes apply carries no _id_, and Server.PushCtx's
// per-applier fallback mints one independently on each.
func TestLIFOPushSameElementSameIDOnAllReplicas(t *testing.T) {
	tc := lifoPushIDMintCluster(t)
	defer tc.stop()

	if frame := tc.ok(0, transport.OpLIFOPush, PushRequest{
		Database:     testDatabase,
		Bucket:       testBucket,
		Data:         map[string]interface{}{"n": 1},
		WriteConcern: "all",
	}); frame.Opcode == transport.OpError {
		t.Fatalf("push: %s", string(frame.Payload))
	}

	firstID := lifoPeekedElementID(t, 0, tc.lifoPeekLocal(0))
	secondID := lifoPeekedElementID(t, 1, tc.lifoPeekLocal(1))
	if firstID != secondID {
		t.Fatalf("node 1 stored the pushed element under _id_ %q, node 2 under %q — resolveDocumentIDs has no OpLIFOPush case",
			firstID, secondID)
	}
}

// lifoPeekedElementIDWait polls one node's own store until the pushed
// element is visible (a restart replays its WAL before this node's own
// applied state catches up, and that race is not the bug under test), then
// returns its _id_.
func lifoPeekedElementIDWait(t *testing.T, tc *testCluster, index int) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var resp PeekResponse
	for time.Now().Before(deadline) {
		resp = tc.lifoPeekLocal(index)
		if resp.Found {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return lifoPeekedElementID(t, index, resp)
}

// TestLIFOPushIDStableAfterDelayedReplay is the WAL-replay arm of that check. A push
// whose store apply fails on the owner (a crash between the WAL append and
// the local apply, the same fault injection failed_apply_test.go's rule-5
// tests use) is recorded as a failed write and retried by WAL replay on the
// owner's restart — replay is the only way this node applies the entry a
// second time; an entry that already applied successfully is gated (Gate,
// store/bucketstate.go) and never re-runs, so a plain restart with no
// injected failure cannot exercise this path at all.
//
// The replica's own live apply is unaffected by the owner's fault, so it
// already holds the element under whatever id its own apply produced by the
// time the owner's replay runs. Pre-fix, resolveDocumentIDs never touches
// OpLIFOPush, so the owner's delayed replay decodes the same unresolved
// payload and mints its own fresh id — independent of, and so almost always
// different from, the replica's — and the replay of the *same* logical push
// stores it under a *second* id, matching neither the replica's copy nor the
// discarded first (faulted) mint attempt on the owner. Post-fix the id is
// resolved once, before the WAL append, so the owner's replay and the
// replica's live apply both read the same _id_ from the payload and neither
// ever mints.
func TestLIFOPushIDStableAfterDelayedReplay(t *testing.T) {
	tc := lifoPushIDMintCluster(t)
	defer tc.stop()

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner of %q: %v", testBucket, err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	replicaIndex := 1 - ownerIndex

	failed := failNextApplyOn(t, tc.nodes[ownerIndex].server.core.kv, anySeq)

	// Not tc.ok: the owner's own local apply is made to fail, and
	// Cluster.applyAsOwner returns that failure to the client regardless of
	// write concern (server/write_path.go) even though the entry was still
	// shipped to the replica. An error frame here is expected, not a bug.
	tc.frame(0, transport.OpLIFOPush, PushRequest{
		Database:     testDatabase,
		Bucket:       testBucket,
		Data:         map[string]interface{}{"n": 1},
		WriteConcern: "majority",
	})
	if failed.Load() == 0 {
		t.Fatal("the owner's apply never reached the store; the test built no crash-recovery scenario")
	}

	if got := tc.localCount(ownerIndex); got != 0 {
		t.Fatalf("owner holds %d elements before its restart, want 0: the injected fault should have left nothing stored", got)
	}
	replicaID := lifoPeekedElementIDWait(t, tc, replicaIndex)

	tc.restartNode(ownerIndex)

	ownerID := lifoPeekedElementIDWait(t, tc, ownerIndex)
	if ownerID != replicaID {
		t.Fatalf("the owner's delayed replay minted _id_ %q, the replica already holds %q for the same push — replay must reuse the resolved id, not mint its own",
			ownerID, replicaID)
	}
	if got := tc.localCount(ownerIndex); got != 1 {
		t.Fatalf("owner holds %d elements after replay, want 1 — replay added a duplicate element", got)
	}
}
