package server

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-server/storage"
)

// The reader rule (shared-storage spec §5.2.2, N-18): a read resolves the
// name and opens its snapshot under one db.mu read hold. A drop removes the
// name under db.mu held for writing before it hands the bucket's id to the
// reclaimer, so a read either resolved first — and its snapshot predates every
// reclaim batch, so it reads the whole bucket as of before the drop — or it
// resolves after and gets ErrBucketNotFound. Resolving and then opening the
// snapshot later, under a second hold or none, passes every single-threaded
// test and reads a bucket the reclaimer is already deleting.
//
// Readers take no fence and no bucket lock: resolveForRead hands back a view
// of the bucket whose engines read nothing but that one snapshot, private to
// the caller. Writers do not use it — they resolve under the fence
// (lockBucket), and use the bucket only under the same hold.

// betweenResolveAndSnapshotHook, when set, runs inside resolveForRead after
// the name is looked up and before the snapshot is opened — inside the db.mu
// read hold, which is the property under test. Nil in production.
var betweenResolveAndSnapshotHook atomic.Pointer[func()]

// afterReadResolvedHook, when set, runs inside resolveForRead after the db.mu
// read hold is released and before the view is built: the window in which a
// drop and a create can recycle the record the reader found. Nil in
// production.
var afterReadResolvedHook atomic.Pointer[func()]

// resolveForRead resolves database/bucket and opens a snapshot of the node's
// store under one db.mu read hold, and returns a view of the bucket whose
// engines read only that snapshot. The caller discards the snapshot when its
// read is done. A bucket in the table that has no id yet — its create has not
// committed — is not served, and resolves as not found.
func (s *Server) resolveForRead(database, bucket string) (*inMemoryBucket, storage.KVSnapshot, error) {
	if err := requireNames(database, bucket); err != nil {
		return nil, nil, err
	}
	db := s.serverDataAccessor().getDB(database)
	if db == nil {
		return nil, nil, databaseNotFoundErr(database)
	}
	return s.resolveForReadIn(db, database, bucket)
}

// resolveForReadIn is resolveForRead once the database's struct is in hand. A
// struct fetched before its database was dropped is marked dropped, with its
// table emptied, by the time this takes its lock (spec §5.2.2, N-10).
func (s *Server) resolveForReadIn(db *inMemoryDB, database, bucket string) (*inMemoryBucket, storage.KVSnapshot, error) {
	db.mu.RLock()
	if db.dropped {
		db.mu.RUnlock()
		return nil, nil, bucketNotFoundErr(database, bucket)
	}
	held, found := db.table.get(bucket)
	var (
		snap  storage.KVSnapshot
		facts bucketFacts
	)
	if found {
		if hook := betweenResolveAndSnapshotHook.Load(); hook != nil {
			(*hook)()
		}
		snap = s.kv.NewSnapshot()
		// Everything the view needs is copied inside the hold (spec N-7): a
		// drop unlinks the name under db.mu held for writing before it closes
		// the record and frees its slot, so the record a reader found here is
		// the bucket its snapshot was taken under, and after the hold the
		// slot may name another bucket.
		facts = s.factsOf(database, held.rec)
	}
	db.mu.RUnlock()
	if !found {
		return nil, nil, s.missingBucketErr(database, bucket)
	}
	if hook := afterReadResolvedHook.Load(); hook != nil {
		(*hook)()
	}

	view, err := s.readView(facts, snap)
	if err != nil || view == nil {
		snap.Discard()
		if err != nil {
			return nil, nil, fmt.Errorf("reading bucket %q in database %q: %w", bucket, database, err)
		}
		return nil, nil, bucketNotFoundErr(database, bucket)
	}
	return view, snap, nil
}

// readView builds a bucket's engines over snap alone, from facts copied under
// the hold the snapshot was taken in, or returns nil when the bucket was not
// served yet then (its id was 0). It takes no lock and reads no record
// (query-pipelining spec §3.5, I-1; catalog-gc spec N-7).
//
// facts.decl is the declaration the caller copied from the declaration table:
// a flagged bucket's is entered there before the flag that says to look is
// set. It can grow after id is published (a declare); its lists are published
// whole, so the view reads them with no lock.
func (s *Server) readView(facts bucketFacts, snap storage.KVSnapshot) (*inMemoryBucket, error) {
	if facts.id == 0 {
		return nil, nil
	}
	view := &inMemoryBucket{bucketType: facts.bucketType, slot: facts.slot, databaseID: facts.databaseID, fieldTypes: facts.typedFields}
	view.id.Store(facts.id)
	view.decl.Store(facts.decl)
	if err := s.buildEngines(view, snapshotKV{snap: snap}, view.slot, facts.id); err != nil {
		return nil, err
	}
	return view, nil
}

// errReadView is what a read view answers to a write: a reader never writes.
var errReadView = errors.New("a read view of one snapshot cannot write")

// snapshotKV is the node's store as one reader sees it: every snapshot it
// opens is the one resolveForRead took, so every engine read of the view reads
// it, and it refuses writes.
type snapshotKV struct{ snap storage.KVSnapshot }

func (v snapshotKV) NewBatch() storage.KVBatch { return refusedBatch{} }

func (v snapshotKV) NewSnapshot() storage.KVSnapshot { return sharedSnapshot{KVSnapshot: v.snap} }

func (v snapshotKV) Sync() error { return errReadView }

func (v snapshotKV) MaxBatchBytes() int64 { return 0 }

// Close releases nothing: the snapshot belongs to the reader that opened it.
func (v snapshotKV) Close() error { return nil }

// sharedSnapshot is the reader's snapshot handed to an engine read, which
// discards what it opens; the reader discards it once, when it is done.
type sharedSnapshot struct{ storage.KVSnapshot }

func (sharedSnapshot) Discard() {}

// refusedBatch is a read view's batch.
type refusedBatch struct{}

func (refusedBatch) Set(_, _ []byte) error { return errReadView }
func (refusedBatch) Delete(_ []byte) error { return errReadView }
func (refusedBatch) Commit(bool) error     { return errReadView }
func (refusedBatch) Discard()              {}
