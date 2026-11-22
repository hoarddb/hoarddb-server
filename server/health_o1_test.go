package server

import (
	"reflect"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// TestHealthCheckDoesNotWaitOnABucket proves HEALTH_CHECK touches no bucket
// lock: a probe that waits on a bucket takes longer the more buckets a node
// holds (catalog-gc spec Open Question 1, owner ruling 41).
func TestHealthCheckDoesNotWaitOnABucket(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)
	held := s.serverDataAccessor().getDB("app").getBucket("users")

	held.mu.Lock()
	released := false
	release := func() {
		if !released {
			released = true
			held.mu.Unlock()
		}
	}
	defer release()

	answered := make(chan struct{})
	go func() {
		defer close(answered)
		if _, err := s.HealthCheck(); err != nil {
			t.Errorf("HealthCheck: %v", err)
		}
	}()
	select {
	case <-answered:
	case <-time.After(2 * time.Second):
		release()
		<-answered
		t.Fatal("HealthCheck waited on a bucket's lock; it must answer from node totals alone")
	}
}

// TestHealthBucketCountIsAttachedBuckets proves BucketCount is the number
// hoarddb_node_buckets reports: a ghost left by a failed first create is not a
// bucket a client can use.
func TestHealthBucketCountIsAttachedBuckets(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)
	createTestBucket(t, s, "app", "posts", BucketTypeLIFO)

	failed, err := s.CreateBucket(CreateBucketRequest{Database: "ghost", Name: "x", Type: BucketTypeBTree,
		Options: map[string]interface{}{"indices": []interface{}{"name"}}})
	if err != nil || failed.Success {
		t.Fatalf("precondition: the create was meant to fail: %+v, %v", failed, err)
	}

	health, err := s.HealthCheck()
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if health.BucketCount != 2 {
		t.Errorf("BucketCount = %d, want 2 (the ghost is not a served bucket)", health.BucketCount)
	}
	if attached := int(s.bucketsAttached.Load()); health.BucketCount != attached {
		t.Errorf("BucketCount = %d, bucketsAttached = %d, want them equal", health.BucketCount, attached)
	}
}

// TestHealthResponseCarriesNoEntryCount proves the field is gone from the
// type and from the wire: a field that always reports 0 is a lie a dashboard
// would graph.
func TestHealthResponseCarriesNoEntryCount(t *testing.T) {
	if _, present := reflect.TypeOf(HealthResponse{}).FieldByName("EntryCount"); present {
		t.Error("HealthResponse still has an EntryCount field")
	}
	encoded, err := bson.Marshal(HealthResponse{NodeID: "n1", Healthy: true})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, lookupErr := bson.Raw(encoded).LookupErr("entrycount"); lookupErr == nil {
		t.Error("the encoded health reply still carries an entrycount element")
	}
}
