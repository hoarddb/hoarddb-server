package server

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestAnAlreadyOpenCreateRacingADropCreatesNothingUnlogged is BUG-0465. A
// create of a bucket the owner serves is held just before its local apply
// while a drop of the same name arrives. The drop must wait for the create's
// turn, or the create must not re-create the bucket: either way a bucket the
// drop removed must not exist at the end, because a bucket the create made
// after the drop is in no log and carries the client's lineage.
func TestAnAlreadyOpenCreateRacingADropCreatesNothingUnlogged(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	owner := clusterOf(tc, 0)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	const bucket = "racing"
	createBucketAt(tc, 0, testDatabase, bucket, 1)

	held, release := holdAt(t, &beforeOpenCreateApplyHook, owner.self.ID, transport.OpCreateBucket, bucket)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	create := sendInBackground(t, owner, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase, Name: bucket, Type: BucketTypeHash,
	})
	awaitHeld(t, held, "the create of the open bucket")
	drop := sendInBackground(t, owner, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: bucket})
	// Before the fix the drop runs to the end here; after it, the drop waits
	// for the create's fence and has not answered.
	time.Sleep(500 * time.Millisecond)
	close(release)
	awaitAnswerOrRefusal(t, create, "the held create")
	awaitAnswer(t, drop, "the drop")

	if _, stillHeld := owner.s.peekBucket(testDatabase, bucket); stillHeld {
		t.Errorf("bucket %q exists after the drop answered: the create landed behind it, unlogged", bucket)
	}
}

// awaitAnswerOrRefusal waits for a background request's answer and accepts
// an error frame: a create that loses to a drop may be refused.
func awaitAnswerOrRefusal(t *testing.T, answered <-chan backgroundAnswer, what string) {
	t.Helper()
	select {
	case <-answered:
	case <-time.After(30 * time.Second):
		t.Fatalf("%s was never answered", what)
	}
}
