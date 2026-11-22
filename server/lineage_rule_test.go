package server

import "testing"

// bucketTermOn is the term s's copy of database/bucket follows: what an entry
// of its owner's stream ships in while nothing has moved (step 10b.2). A test
// that hands a replica an entry of its own making stamps it with this, as an
// owner does.
func bucketTermOn(t *testing.T, s *Server, database, bucket string) uint64 {
	t.Helper()
	term, _, err := s.bucketLineage(database, bucket)
	if err != nil {
		t.Fatalf("reading the term of %s/%s: %v", database, bucket, err)
	}
	return term
}
