package server

import "testing"

// TestFailedCreateLeavesNoListedBucket: a create that fails after the registry
// claimed its name must not leave the name counted. ListDatabases reported the
// database with one bucket while ListBuckets listed none (BUG-0257).
func TestFailedCreateLeavesNoListedBucket(t *testing.T) {
	const database, bucket = "fresh", "failed"
	s := newTestServer(t)
	if resp, err := s.CreateDatabase(database); err != nil || !resp.Success {
		t.Fatalf("create database %q: %+v, %v", database, resp, err)
	}
	refusedIndex := map[string]interface{}{"indices": []interface{}{map[string]interface{}{"field": 7}}}
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeBTree, Options: refusedIndex}); err == nil && resp.Success {
		t.Fatalf("a create with a malformed index declaration was accepted: %+v", resp)
	}

	listed, err := s.ListDatabases(ListDatabasesRequest{})
	if err != nil {
		t.Fatalf("list databases: %v", err)
	}
	for _, info := range listed.Databases {
		if info.Name == database && info.BucketCount != 0 {
			t.Errorf("ListDatabases counts %d bucket(s) in %q after a failed create, want 0", info.BucketCount, database)
		}
	}
	if left := lookupBucket(s, database, bucket); left != nil {
		t.Errorf("the failed create left a registry entry under %q", bucket)
	}

	createTestBucket(t, s, database, bucket, BucketTypeHash)
	listed, err = s.ListDatabases(ListDatabasesRequest{})
	if err != nil {
		t.Fatalf("list databases: %v", err)
	}
	for _, info := range listed.Databases {
		if info.Name == database && info.BucketCount != 1 {
			t.Errorf("ListDatabases counts %d bucket(s) after a valid create over the failed name, want 1", info.BucketCount)
		}
	}
}
