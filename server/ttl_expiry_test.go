package server

import (
	"context"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestConditionalDeleteOnABtreeBucketIsRefused (BUG-0127): a delete that
// carries expire_if means "only if still expired". A btree bucket has no
// expiry evaluation, so it must refuse the delete; applying it would delete a
// document the sweep's snapshot saw as expired but a client has since
// refreshed.
func TestConditionalDeleteOnABtreeBucketIsRefused(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "sessions", BucketTypeBTree)
	now := time.Now()
	mustInsert(t, s, "app", "sessions", "refreshed", map[string]interface{}{"CreatedAt": primitive.NewDateTimeFromTime(now)})

	payload := expireIfPayload(t, "app", "sessions", "refreshed", "CreatedAt", now.Add(-time.Hour))
	frame, err := s.dispatch(context.Background(), transport.NewFrameWithID(transport.OpDelete, [16]byte{1}, payload))
	if err != nil {
		t.Fatalf("dispatch delete: %v", err)
	}
	if documentOf(t, s, "app", "sessions", "refreshed") == nil {
		t.Fatal("a delete carrying expire_if removed a btree document whose field is not expired")
	}
	if frame.Opcode != transport.OpError {
		if applied, _ := bson.Raw(frame.Payload).Lookup("success").BooleanOK(); applied {
			t.Errorf("the btree bucket answered success to a conditional delete it did not evaluate: %v", bson.Raw(frame.Payload))
		}
	}
}

// TestAttachedNonHashBucketIsNotRegisteredForTTL (BUG-0127): the create path
// refuses an index on a non-hash bucket, but attach and migration reach
// prepareBucketLocked without it. A btree bucket carrying a TTL declaration
// must not enter the sweeper's registry, since nothing maintains its index.
func TestAttachedNonHashBucketIsNotRegisteredForTTL(t *testing.T) {
	s := newTestServer(t)
	bucket := &inMemoryBucket{}
	resp, err := s.prepareBucketLocked(bucket, CreateBucketRequest{
		Database: "app", Name: "sessions", Type: BucketTypeBTree,
		Options: map[string]interface{}{"indices": []interface{}{ttlDeclaration("CreatedAt", 3600)}},
	})
	if err != nil {
		t.Fatalf("prepareBucketLocked: %v", err)
	}
	if registered := s.ttl.list(); len(registered) != 0 {
		t.Fatalf("a btree bucket was registered with the TTL sweeper (prepare answered %+v): %+v", resp, registered)
	}
}
