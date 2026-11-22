package server

import (
	"fmt"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// betweenUnlinkAndCloseHook, when set, runs in retireRecords after a drop
// has unlinked its records and before it closes them: the window in which a
// create of another name must not be given a slot the drop still holds. Nil in
// production.
var betweenUnlinkAndCloseHook atomic.Pointer[func()]

// Handles and the drop's phases (catalog-gc spec §5.3). A bucket is its
// record in the database's table; an operation that needs an engine builds a
// handle on it, and nothing keeps the handle after the operation. The record
// owns the bucket's one mutex and is never moved, but its slot is reused
// once its bucket is dropped, so every path that locks a record it found
// earlier checks the epoch it found it under (heldRecord.current) before it
// trusts anything in it.

// handleLocked builds a handle on held's record. The caller holds the
// record's mutex, for reading or for writing, and has checked held.current().
// It copies the record's facts and, once the bucket is attached, its
// declarations; it builds no engine (buildEngines does, for a caller that
// needs one).
func (s *Server) handleLocked(database string, held heldRecord) *inMemoryBucket {
	rec := held.rec
	handle := &inMemoryBucket{rec: rec, epoch: held.epoch, bucketType: bucketTypeOfCode(rec.bucketType), slot: rec.slot, databaseID: storage.LegacyDatabaseID(database)}
	id := rec.id.Load()
	handle.id.Store(id)
	if id != 0 {
		handle.decl.Store(s.declarationFor(rec, id))
		handle.fieldTypes = s.fieldTypes.holder(rec.typesID.Load())
	}
	return handle
}

// resolveRecord finds database/bucket's record, with the user-facing errors
// every caller reports the same way: no database, or the name not here (with
// the node that holds it when the ring puts it elsewhere).
func (s *Server) resolveRecord(database, bucket string) (heldRecord, error) {
	if err := requireNames(database, bucket); err != nil {
		return heldRecord{}, err
	}
	db := s.serverDataAccessor().getDB(database)
	if db == nil {
		return heldRecord{}, databaseNotFoundErr(database)
	}
	held, found := db.findBucket(bucket)
	if !found {
		return heldRecord{}, s.missingBucketErr(database, bucket)
	}
	return held, nil
}

// resolveCommittedBucket is a handle on database/bucket with engines on the
// node's store, for a caller that reads the live bucket without writing it
// (restore's checks; spec §5.4 class H). It is built under the record's read
// lock, which waits out a create or a write in flight, and answers not found
// for a name whose create failed after the registry claimed it, as lockBucket
// does. The caller does not hold the lock afterwards: the handle's facts are
// copies, and its engines read the store.
func (s *Server) resolveCommittedBucket(database, bucket string) (*inMemoryBucket, error) {
	held, err := s.resolveRecord(database, bucket)
	if err != nil {
		return nil, err
	}
	held.rec.mu.RLock()
	defer held.rec.mu.RUnlock()
	if !held.current() || held.rec.id.Load() == 0 {
		return nil, bucketNotFoundErr(database, bucket)
	}
	handle := s.handleLocked(database, held)
	if err := s.buildEngines(handle, s.kv, handle.slot, handle.id.Load()); err != nil {
		return nil, fmt.Errorf("opening bucket %q in database %q: %w", bucket, database, err)
	}
	return handle, nil
}

// lockServedRecord finds database/bucket and locks its record, for a caller
// that reads or rewrites what the record and the state document hold without
// an engine (spec §5.4 class PW). It returns nil, holding nothing, when the
// name is not here, or names a bucket dropped since it was found, or one
// whose create has not committed. Otherwise the caller holds the record's
// mutex and unlocks it.
func (s *Server) lockServedRecord(database, bucket string) *bucketRec {
	db := s.serverDataAccessor().getDB(database)
	if db == nil {
		return nil
	}
	held, found := db.findBucket(bucket)
	if !found {
		return nil
	}
	held.rec.mu.Lock()
	if !held.current() || held.rec.id.Load() == 0 {
		held.rec.mu.Unlock()
		return nil
	}
	return held.rec
}

// tryFillLineage fills the lineage cache of database/bucket when no one holds
// the record's lock, so a read never waits on the bucket's mutex (S-2). The
// fill reads the state document itself, under the lock it just took: every
// lineage writer commits and updates the cache under that lock, so a document
// read there is the latest committed one, and a document a reader carried
// from before it could be older than a writer's commit and must not reach the
// cache. It fills only a cache still unknown under the lock; a writer that
// got there first has left it right. It returns the document it filled from
// and true, or false when it did nothing — the lock was held, the name is
// gone or names another bucket now, or the cache was already known — and the
// read that called it then answers from the document it already has.
func (s *Server) tryFillLineage(database, bucket string, id uint64) (store.BucketState, bool, error) {
	db := s.serverDataAccessor().getDB(database)
	if db == nil {
		return store.BucketState{}, false, nil
	}
	held, found := db.findBucket(bucket)
	if !found {
		return store.BucketState{}, false, nil
	}
	if !held.rec.mu.TryLock() {
		return store.BucketState{}, false, nil
	}
	defer held.rec.mu.Unlock()
	if !held.current() || held.rec.id.Load() != id || held.rec.lineageFlags.Load()&lineageKnown != 0 {
		return store.BucketState{}, false, nil
	}
	snapshot := s.kv.NewSnapshot()
	defer snapshot.Discard()
	st, err := store.LoadBucketState(snapshot, held.rec.slot, storage.LegacyDatabaseID(database), id)
	if err != nil {
		return store.BucketState{}, false, fmt.Errorf("reading the lineage of bucket %d to fill its cache: %w", id, err)
	}
	held.rec.setLineageStateLocked(st)
	return st, true, nil
}

// closeRecordLocked is phase two of a drop, for a record the caller unlinked
// and holds locked: the bucket is closed, so a holder that locks the record
// after finds it gone, and its declarations leave their tables. It reports
// whether the bucket was attached, for hoarddb_node_buckets, which does not
// count the parts of spread collections.
func (s *Server) closeRecordLocked(rec *bucketRec) bool {
	attached := !rec.closed && rec.id.Load() != 0 && !rec.isPart()
	rec.closed = true
	s.dropDeclarationsLocked(rec)
	return attached
}

// retireRecords runs phases two and three of a drop for records unlink took
// out of db's table (spec §5.3): each is closed under its own mutex with no
// db.mu held, and only then are the slots freed, under db.mu, at most one
// record chunk's worth per hold. Closing before freeing is what keeps a create
// that arrives between the phases from being given the slot the drop is
// still closing. It returns how many of the buckets were attached.
func (s *Server) retireRecords(db *inMemoryDB, unlinked []*bucketRec) int64 {
	if hook := betweenUnlinkAndCloseHook.Load(); hook != nil {
		(*hook)()
	}
	var detached int64
	for _, rec := range unlinked {
		rec.mu.Lock()
		if s.closeRecordLocked(rec) {
			detached++
		}
		rec.mu.Unlock()
	}
	for start := 0; start < len(unlinked); start += maxRecordChunk {
		db.mu.Lock()
		for _, rec := range unlinked[start:min(start+maxRecordChunk, len(unlinked))] {
			if err := db.table.free(rec); err != nil {
				s.logger.Error("a dropped bucket's record could not be given back; its slot stays allocated until the node restarts",
					"error", err)
			}
		}
		db.mu.Unlock()
	}
	return detached
}

// closeAllRecords closes every bucket of every database at shutdown, so a
// writer still in flight finds its bucket closed. Nothing is counted or
// freed: the process is ending. One record chunk is read per db.mu hold, and
// each record is locked outside it.
func (s *Server) closeAllRecords() {
	data := s.serverDataAccessor()
	data.mu.RLock()
	databases := make([]*inMemoryDB, 0, len(data.dbs))
	for _, db := range data.dbs {
		databases = append(databases, db)
	}
	data.mu.RUnlock()
	for _, db := range databases {
		db.table.scanRecords(func(chunk []heldRecord) {
			for _, held := range chunk {
				held.rec.mu.Lock()
				if held.current() {
					s.closeRecordLocked(held.rec)
				}
				held.rec.mu.Unlock()
			}
		})
	}
}

// setLineageStateLocked records the copy's term, opened flag and whether its
// position is zero, from the state document it names. The caller holds rec.mu
// and has committed that document.
func (rec *bucketRec) setLineageStateLocked(st store.BucketState) {
	flags := lineageKnown
	if st.Opened {
		flags |= lineageOpened
	}
	if st.Through == 0 {
		flags |= lineagePositionZero
	}
	rec.term.Store(st.Term)
	rec.lineageFlags.Store(flags)
}

// setLineageLocked records a copy's term and opened flag for a caller with no
// state document (tests). Production callers use setLineageStateLocked, which
// also records whether the position is zero.
func (rec *bucketRec) setLineageLocked(term uint64, opened bool) {
	rec.setLineageStateLocked(store.BucketState{Term: term, Opened: opened})
}

// noteAppliedLocked records that an apply committed, as the state document
// Follow records it: an apply that carries the origin's sequence moves the
// copy's position off zero, and one this node originated marks the copy
// opened. A cache that is unknown stays unknown: the next fill reads the
// document, which says so. The caller holds rec.mu.
func (rec *bucketRec) noteAppliedLocked(originSeq uint64, originated bool) {
	flags := rec.lineageFlags.Load()
	if flags&lineageKnown == 0 {
		return
	}
	if originSeq != 0 {
		flags &^= lineagePositionZero
	}
	if originated {
		flags |= lineageOpened
	}
	rec.lineageFlags.Store(flags)
}
