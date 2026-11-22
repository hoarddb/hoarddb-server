package server

import "testing"

// Test access to the registry after the swap. The product resolves a name to a
// record and builds a handle per operation; these give a test the two things
// it reached for when the registry held structs: a bucket's record, to read
// its facts or hold its lock, and a handle with engines, to call one directly.

// getBucket is the record named name, nil when the database has none.
func (db *inMemoryDB) getBucket(name string) *bucketRec {
	held, found := db.findBucket(name)
	if !found {
		return nil
	}
	return held.rec
}

// resolveBucket is a handle on database/bucket with engines on the node's
// store, built from the facts a peek copies under db.mu: it takes no record
// lock, so a test may call it while a create or a writer holds one. A bucket
// whose create has not committed has a handle with no engine. A test that must
// hold the bucket's lock while it calls an engine takes handle.rec.mu.
func (s *Server) resolveBucket(database, bucket string) (*inMemoryBucket, error) {
	if err := requireNames(database, bucket); err != nil {
		return nil, err
	}
	db := s.serverDataAccessor().getDB(database)
	if db == nil {
		return nil, databaseNotFoundErr(database)
	}
	db.mu.RLock()
	held, found := db.table.get(bucket)
	var facts bucketFacts
	if found {
		facts = s.factsOf(database, held.rec)
	}
	db.mu.RUnlock()
	if !found {
		return nil, s.missingBucketErr(database, bucket)
	}
	// The type, like the slot, is read only once the id says the create
	// wrote it (I-1); a create in flight is a handle with neither.
	handle := &inMemoryBucket{rec: held.rec, epoch: held.epoch, bucketType: facts.bucketType, slot: facts.slot, databaseID: facts.databaseID, fieldTypes: facts.typedFields}
	handle.id.Store(facts.id)
	handle.decl.Store(facts.decl)
	if facts.id != 0 {
		if err := s.buildEngines(handle, s.kv, facts.slot, facts.id); err != nil {
			return nil, err
		}
	}
	return handle, nil
}

// claimForTest claims a record for name in db, as a create does before its
// attach (generation 1, creating), and returns it.
func claimForTest(tb testing.TB, db *inMemoryDB, name string, btype BucketType) *bucketRec {
	tb.Helper()
	db.mu.Lock()
	held, err := db.table.claim(name, btype, 1, true)
	db.mu.Unlock()
	if err != nil {
		tb.Fatalf("claiming %q: %v", name, err)
	}
	return held.rec
}

// closeRecordForTest closes rec as a drop's second phase does, and reports
// whether it was attached.
func closeRecordForTest(s *Server, rec *bucketRec) bool {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return s.closeRecordLocked(rec)
}

// declOf is the declaration table's entry for bucket id, nil for none.
func declOf(s *Server, id uint64) *bucketDecl {
	decl, _ := s.decls.get(id)
	return decl
}
