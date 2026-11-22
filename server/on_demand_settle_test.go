package server

import (
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// A node holds its settle mark while its take-over has a name it has not found
// a copy for (owner ruling R6, 2026-10-21): the settling window then stays open
// until the copy is found, so a read and a restore of a moved bucket take the
// on-demand wait instead of answering from an empty store.

// factorOneRemovalGap is three nodes at factor 1 whose owner of a bucket
// holding "held" is removed while it still runs: the bucket's only copy is on
// the removed node, and the successor that now owns it holds nothing. The
// successor's take-over is paused, so its copy cannot land; what the test
// reads is whether the successor says it has settled meanwhile.
type factorOneRemovalGap struct {
	tc        *testCluster
	bucket    string
	successor int
	resume    func()
}

func setUpFactorOneRemovalGap(t *testing.T, prefix string) factorOneRemovalGap {
	t.Helper()
	tc := newTestCluster(t, 3, 1)
	t.Cleanup(tc.stop)
	before := ringWith(t, 1, tc.addrs...)
	after := ringWith(t, 1, tc.addrs[1], tc.addrs[2])
	bucket := bucketNamed(t, prefix, func(name string) bool { return ownerOn(before, name) == tc.addrs[0] })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 1)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "held",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	successor := nodeIndexByAddr(t, tc.addrs, ownerOn(after, bucket))
	other := 3 - successor
	resume := pauseWorkerOf(t, clusterOf(tc, successor))
	removeAndTell(t, tc, successor, 0)
	via := clusterOf(tc, successor)
	if err := clusterOf(tc, other).reconcileMembership(via.self.ID, via.memberNodes(), via.memberIDs(), via.Epoch()); err != nil {
		t.Fatalf("bringing node %d to the removal: %v", other+1, err)
	}
	// The removed owner's hand-off names the bucket to its successor.
	if !holdsWithin(takeOverWait, func() bool {
		c := clusterOf(tc, successor)
		c.takeOver.mu.Lock()
		defer c.takeOver.mu.Unlock()
		_, named := c.takeOver.named[placementOf(testDatabase, bucket)]
		return named
	}) {
		t.Fatalf("setup: the removed owner's hand-off never named %s to its successor", bucket)
	}
	return factorOneRemovalGap{tc: tc, bucket: bucket, successor: successor, resume: resume}
}

// stillUnsettled fails the test unless the successor, once the removed
// owner's notice that it named every bucket has reached it, stays unsettled at
// its epoch for a while with its take-over paused.
func (g factorOneRemovalGap) stillUnsettled(t *testing.T) {
	t.Helper()
	c := clusterOf(g.tc, g.successor)
	removedID := g.tc.addrs[0]
	if !holdsWithin(takeOverWait, func() bool {
		c.removedHandOffMu.Lock()
		defer c.removedHandOffMu.Unlock()
		state, heard := c.removedHandOffs[removedID]
		return heard && state.namedAllAt != 0
	}) {
		t.Errorf("the removed owner's notice that it named every bucket never reached its successor")
	}
	if holdsWithin(2*time.Second, func() bool { return c.settled.Load() >= c.Epoch() }) {
		t.Errorf("the successor reported itself settled at epoch %d while its take-over had not found %s's copy", c.Epoch(), g.bucket)
	}
}

// TestAReadInAFactorOneRemovalGapIsNeverNotFound: the read case of R6. While the
// successor's take-over cannot land the copy, the node does not settle, and a
// get there is never "not found"; once the take-over runs, the get finds the
// document. Red before R6: the successor settled within 100 ms and the get
// answered `bucket "…" not found … create bucket`. Guard: the take-over's
// unfound names hold the settle mark.
func TestAReadInAFactorOneRemovalGapIsNeverNotFound(t *testing.T) {
	g := setUpFactorOneRemovalGap(t, "r6read")
	g.stillUnsettled(t)
	get := FindRequest{Database: testDatabase, Bucket: g.bucket, Key: "held"}
	frame := g.tc.frame(g.successor, transport.OpGet, get)
	if frame.Opcode == transport.OpError {
		refusedAwaitingCopyOrRetry(t, "a get at the successor in the removal gap", frame)
	} else {
		foundHeld(t, "a get at the successor in the removal gap", frame)
	}
	g.resume()
	clusterOf(g.tc, g.successor).signalTakeOver()
	if !holdsWithin(takeOverWait, func() bool { return foundThrough(g.tc, g.successor, testDatabase, g.bucket, "held") }) {
		t.Errorf("\"held\" never read back through the successor once its take-over ran")
	}
	c := clusterOf(g.tc, g.successor)
	if !holdsWithin(takeOverWait, func() bool { return c.settled.Load() >= c.Epoch() }) {
		t.Errorf("the successor never settled at epoch %d once its take-over landed the copy", c.Epoch())
	}
}

// TestARestoreInAFactorOneRemovalGapNeverLandsBeside: the restore case of R6.
// While the successor's take-over cannot land the copy, a restore start there
// does not create the bucket; once the take-over lands it, the original data
// remain. Red before R6: the restore created the bucket beside the removed
// owner's copy. Guard: the take-over's unfound names hold the settle mark.
func TestARestoreInAFactorOneRemovalGapNeverLandsBeside(t *testing.T) {
	g := setUpFactorOneRemovalGap(t, "r6rest")
	g.stillUnsettled(t)
	frame := g.tc.frame(g.successor, transport.OpRestoreStart, RestoreStartRequest{
		Database: testDatabase, Bucket: g.bucket, FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash, Bytes: 1,
	})
	g.resume()
	clusterOf(g.tc, g.successor).signalTakeOver()
	neverLandedBeside(t, g.tc, frame, g.successor, g.bucket)
}

// neverLandedBeside fails the test unless the restore start did not create a
// bucket — refused, never with "not found", or started against the bucket
// that landed — and unless "held" reads back through the successor once the
// take-over lands.
func neverLandedBeside(t *testing.T, tc *testCluster, frame *transport.Frame, successor int, bucket string) {
	t.Helper()
	if frame.Opcode == transport.OpError {
		if text := errorFrameText(frame); strings.Contains(text, "not found") {
			t.Errorf("a restore start at the successor was told the bucket does not exist: %s", text)
		}
	} else {
		var started RestoreStartResponse
		if err := bson.Unmarshal(frame.Payload, &started); err == nil && started.Created {
			t.Errorf("the restore at the successor created %s beside the bucket the removed owner holds", bucket)
		}
	}
	if !holdsWithin(takeOverWait, func() bool { return foundThrough(tc, successor, testDatabase, bucket, "held") }) {
		t.Errorf("\"held\" does not read back through the successor: the original data did not remain")
	}
}

// TestARemovedOwnerThatNeverAnswersReleasesTheSettleAfterTheBound is owner
// rulings R7 and R8: the owner of a factor-1 bucket crashes and is then
// removed, so no hand-off from it ever arrives. Its successor does not wait
// for that hand-off past the removed-node bound (removedNodeAnswerWait, the
// bound a create already waits for a removed node): it settles, and a get of
// the bucket there is then an ordinary miss — not found, with the create
// advice — that names the removed node the bucket was placed on, never a
// retry (R8: a never-created name must not read retry forever after a crash).
// Guards: the probe that confirms the removed node gone, and the miss's note.
func TestARemovedOwnerThatNeverAnswersReleasesTheSettleAfterTheBound(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	t.Cleanup(tc.stop)
	before := ringWith(t, 1, tc.addrs...)
	after := ringWith(t, 1, tc.addrs[1], tc.addrs[2])
	bucket := bucketNamed(t, "r7gone", func(name string) bool { return ownerOn(before, name) == tc.addrs[0] })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 1)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "held",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	removedID := tc.addrs[0]
	successor := nodeIndexByAddr(t, tc.addrs, ownerOn(after, bucket))
	tc.stopNodeKeepingData(0)
	removeNodeFrom(t, tc, successor, 0)

	c := clusterOf(tc, successor)
	began := time.Now()
	if !holdsWithin(takeOverWait, func() bool { return c.settled.Load() >= c.Epoch() }) {
		t.Fatalf("the successor never settled at epoch %d with the removed owner gone", c.Epoch())
	}
	if took := time.Since(began); took > removedNodeAnswerWait+5*time.Second {
		t.Errorf("the successor settled %s after the removal; a removed node that never answers holds it at most about %s", took, removedNodeAnswerWait)
	}
	frame := tc.frame(successor, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "held"})
	missNamingRemovedNode(t, "a get of the bucket whose only copy is on the removed owner, after the bound", frame, removedID)
}

// missNamingRemovedNode fails the test unless frame is the ordinary not-found,
// with its create advice, and names node, the removed node the name was placed
// on (owner ruling R8).
func missNamingRemovedNode(t *testing.T, what string, frame *transport.Frame, node string) {
	t.Helper()
	var refusal ErrorResponse
	if frame.Opcode != transport.OpError || bson.Unmarshal(frame.Payload, &refusal) != nil {
		t.Errorf("%s was answered %s, want not found naming %s: %s", what, frame.Opcode, node, frame.Payload)
		return
	}
	if transport.ErrorCode(refusal.Code) != transport.ErrNotFound || !strings.Contains(refusal.Message, "to create it") ||
		!strings.Contains(refusal.Message, node) {
		t.Errorf("%s was answered code %d %q; want not found, its create advice, and the removed node %s", what, refusal.Code, refusal.Message, node)
	}
}
