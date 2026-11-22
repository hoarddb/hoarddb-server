package server

import (
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0685: an alter of a bucket that exists nowhere is NOT_FOUND with the
// standard missing-bucket text, at the bucket's owner and through any other
// node. It was UNAVAILABLE ("has no local copy"), the class a driver retries.

// requireNotFoundNotApplied fails unless frame is the standard missing-bucket
// refusal of bucket in testDatabase: NOT_FOUND, write_outcome not_applied and
// the text every other verb gives (bucketNotFoundErr).
func requireNotFoundNotApplied(t *testing.T, frame *transport.Frame, bucket string) {
	t.Helper()
	refused := refusalOf(t, frame)
	if refused.Code != int(transport.ErrNotFound) {
		t.Errorf("code %d, want NOT_FOUND (%d): %q", refused.Code, transport.ErrNotFound, refused.Message)
	}
	if refused.WriteOutcome != outcomeNotApplied {
		t.Errorf("write_outcome %q, want %q: %q", refused.WriteOutcome, outcomeNotApplied, refused.Message)
	}
	if want := bucketNotFoundErr(testDatabase, bucket).Error(); refused.Message != want {
		t.Errorf("message %q, want the standard missing-bucket text %q", refused.Message, want)
	}
}

func TestAnAlterOfAMissingBucketIsNotFoundAtTheOwnerAndThroughANonOwner(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "ghost")
	req := AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2}

	t.Run("at the owner", func(t *testing.T) {
		requireNotFoundNotApplied(t, alterFrame(t, tc, 0, req), bucket)
	})
	t.Run("through a non-owner", func(t *testing.T) {
		requireNotFoundNotApplied(t, alterFrame(t, tc, 1, req), bucket)
	})
	requireNoAlterLogged(t, tc, bucket)
}

// TestAnAlterOfAHeldBucketIsStillAcceptedThroughANonOwner guards the fix's
// other side: the question a missing bucket now meets must not touch a bucket
// the owner holds.
func TestAnAlterOfAHeldBucketIsStillAcceptedThroughANonOwner(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	if answer := alterAccepted(t, alterFrame(t, tc, 1, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2})); answer.ReplicationFactor != 2 {
		t.Errorf("the alter through a non-owner answered factor %d, want 2", answer.ReplicationFactor)
	}
}

// TestAnAlterOnAStandaloneNodeIsRefusedInBand pins the answer a standalone
// node gives an alter that asks for a second copy: success false with the
// words that name the fix, whether or not the bucket exists, because the shape
// of the node is judged before the bucket is looked up.
func TestAnAlterOnAStandaloneNodeIsRefusedInBand(t *testing.T) {
	s := newTestServer(t)
	if created, err := s.CreateDatabase(testDatabase); err != nil || !created.Success {
		t.Fatalf("creating the database: %+v, %v", created, err)
	}
	if created, err := s.CreateBucket(CreateBucketRequest{Database: testDatabase, Name: "Sessions", Type: BucketTypeHash}); err != nil || !created.Success {
		t.Fatalf("creating the bucket: %+v, %v", created, err)
	}
	for _, bucket := range []string{"Sessions", "Nobody"} {
		payload, err := bson.Marshal(AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2})
		if err != nil {
			t.Fatalf("encoding the alter: %v", err)
		}
		frame, err := s.fencedDispatch(context.Background(), transport.NewFrame(transport.OpAlterBucket, payload))
		if err != nil {
			t.Fatalf("dispatching the alter of %q: %v", bucket, err)
		}
		requireAlterWords(t, alterRefusalBody(t, frame), `cannot be raised to replication_factor 2`, "running standalone",
			"then run the alter once 'nodes' shows the members")
	}
}
