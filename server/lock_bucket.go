package server

import "fmt"

// lockBucket is every writer's preamble (catalog-gc spec §5.3 "Writes", §5.4
// class W): resolve the name to its record under a db.mu read hold, run
// afterResolveHook in the window a drop can land in, take the record's lock,
// and refuse a bucket a drop closed meanwhile — or whose slot a create of
// another name has since been given, which the epoch tells — with the same
// not-found answer a missing name gets. A writer that wrote to a closed
// bucket would put keys into a range the reclaimer owns; one that wrote
// through a recycled record would write to another bucket.
//
// It refuses a bucket whose id is still 0 under the lock the same way: its
// create failed, leaving the record it claimed, or has not taken the lock
// yet. Either way no engine exists to write to, and the create has not been
// acknowledged, so not found is the true answer; reads treat id 0 so too.
//
// The handle, and its engine, are built under the record's lock, not before
// it: a declare that lands between the lookup and the lock changes the
// declaration the engine's index manager is built from, and an engine built
// before it would index this write under the old one (spec N-8).
//
// On success the caller holds the record's lock and calls unlock when its
// apply is done. The lock order is the house one: db.mu is released before
// the record's lock is taken.
func (s *Server) lockBucket(database, bucket string) (handle *inMemoryBucket, unlock func(), err error) {
	held, err := s.resolveRecord(database, bucket)
	if err != nil {
		return nil, nil, err
	}
	if hook := afterResolveHook.Load(); hook != nil {
		(*hook)(database, bucket)
	}
	held.rec.mu.Lock()
	if !held.current() || held.rec.id.Load() == 0 {
		held.rec.mu.Unlock()
		return nil, nil, bucketNotFoundErr(database, bucket)
	}
	handle = s.handleLocked(database, held)
	if err := s.buildEngines(handle, s.kv, handle.slot, handle.id.Load()); err != nil {
		held.rec.mu.Unlock()
		return nil, nil, fmt.Errorf("opening bucket %q in database %q to write: %w", bucket, database, err)
	}
	return handle, held.rec.mu.Unlock, nil
}
