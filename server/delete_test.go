package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestDeleteOfAMissingKeyReportsNotFound: deleting a key that is not
// there answers the same not-found result on a hash, a btree and a blob bucket,
// and deleting a key that is there still answers success. The answer says
// nothing was stored or removed (write_outcome not_applied), so a retry is known
// to be safe.
func TestDeleteOfAMissingKeyReportsNotFound(t *testing.T) {
	for _, bucketType := range []BucketType{BucketTypeHash, BucketTypeBTree, BucketTypeBlob} {
		t.Run(string(bucketType), func(t *testing.T) {
			s := newTestServer(t)
			createTestBucket(t, s, "app", "things", bucketType)

			missing, err := s.Delete(DeleteRequest{Database: "app", Bucket: "things", Key: "never-written"})
			if err != nil {
				t.Fatalf("Delete of a missing key: %v", err)
			}
			if missing.Success {
				t.Errorf("Delete of a missing key = %+v, want Success:false", missing)
			}
			if want := "not found"; !strings.Contains(missing.Message, want) {
				t.Errorf("message %q does not say %q", missing.Message, want)
			}
			if missing.WriteOutcome != outcomeNotApplied {
				t.Errorf("write_outcome = %q, want %q: nothing was removed", missing.WriteOutcome, outcomeNotApplied)
			}

			if bucketType == BucketTypeBlob {
				return
			}
			stored, err := s.Insert(InsertRequest{Database: "app", Bucket: "things", Key: "there", Data: map[string]interface{}{"n": 1}})
			if err != nil || !stored.Success {
				t.Fatalf("Insert = %+v, %v", stored, err)
			}
			removed, err := s.Delete(DeleteRequest{Database: "app", Bucket: "things", Key: "there"})
			if err != nil || !removed.Success {
				t.Fatalf("Delete of a stored key = %+v, %v, want success", removed, err)
			}
			again, err := s.Delete(DeleteRequest{Database: "app", Bucket: "things", Key: "there"})
			if err != nil || again.Success {
				t.Errorf("second Delete of the same key = %+v, %v, want the not-found result", again, err)
			}
		})
	}
}

// TestDeleteOfAMissingKeyStillReplicates holds the other half of the not-found fix: the
// not-found answer is the client's reply only. A delete of a key the owner no
// longer has is logged and applied by the replicas like any delete, and a
// replica that lacks the key accepts it, so replay and TTL expiry stay
// idempotent.
func TestDeleteOfAMissingKeyStillReplicates(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	deleteKey := func(key string) DeleteResponse {
		t.Helper()
		frame := tc.ok(0, transport.OpDelete, DeleteRequest{Database: testDatabase, Bucket: testBucket, Key: key})
		var answer DeleteResponse
		if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
			t.Fatalf("decode delete answer: %v", err)
		}
		return answer
	}

	if answer := deleteKey("never-written"); answer.Success {
		t.Errorf("delete of a key that was never written = %+v, want not found", answer)
	}
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k", Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	if answer := deleteKey("k"); !answer.Success {
		t.Errorf("delete of a stored key = %+v, want success on a cluster", answer)
	}
	if answer := deleteKey("k"); answer.Success {
		t.Errorf("second delete of the same key = %+v, want not found", answer)
	}
}
