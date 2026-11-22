package server

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// The upgrade rebuild (GAP-14 build doc Step 4, S-3). An index declared before
// this build (a descriptor with no version) indexes a document missing its
// field with no entry; this build indexes it as null. The two cannot share an
// index, so a pre-build index is rebuilt from the documents once, at attach,
// before the bucket takes a write: synchronously, under the bucket mutex the
// attach holds. The rebuild is local — index entries are not counted in the
// bucket's state and no WAL entry is written — and every replica runs the same
// rebuild over the same documents.
//
// It is also what a crash part-way through leaves behind to finish: an index
// persisted as building is rebuilt again, from a cleared range, at the next
// attach.

// indexRebuildHook, when set, runs after each committed batch of a rebuild's
// document walk, with the 1-based batch number; a non-nil return fails the
// rebuild there, as a crash would stop it. Nil in production.
var indexRebuildHook atomic.Pointer[func(spec store.IndexSpec, batch int) error]

// rebuildBatchEntries bounds the keys one rebuild batch writes or deletes,
// beside the store's own byte bound. A variable so a test can stop a rebuild
// between two batches of a small bucket.
var rebuildBatchEntries = 4096

// specNeedsRebuild reports whether attach must rebuild an index: declared
// before this build, left building by a rebuild that did not finish, or
// conflicted — a restart after the data is fixed is how a conflicted index
// clears. An index on a path is never rebuilt: it is unusable, and its
// record is kept as written (indexesAPath).
func specNeedsRebuild(spec store.IndexSpec) bool {
	return !indexesAPath(spec) && (spec.Version < store.IndexSpecVersion || spec.State == store.IndexBuilding || spec.State == store.IndexConflicted)
}

// rebuildIndexesLocked rebuilds every index of an attached bucket that needs
// it. The caller holds bucket.mu. A failure is returned with the indexes left
// building, which search refuses and the next attach rebuilds.
func (s *Server) rebuildIndexesLocked(bucket *inMemoryBucket, database, name string) error {
	// Only the hash engine maintains an index. A bucket of another type created
	// before the refusal existed can still carry a declaration nothing writes;
	// there is nothing to rebuild it from.
	if bucket.bucketType != BucketTypeHash {
		return nil
	}
	specs := bucket.declaredIndexes()
	var stale []int
	for position, spec := range specs {
		if specNeedsRebuild(spec) {
			stale = append(stale, position)
		}
	}
	if len(stale) == 0 {
		return nil
	}
	started := time.Now()

	building := cloneSpecs(specs)
	for _, position := range stale {
		building[position].State = store.IndexBuilding
	}
	if err := s.persistIndexSpecs(bucket, database, name, building); err != nil {
		return err
	}

	rebuilt := cloneSpecs(building)
	for _, position := range stale {
		rebuilt[position].Version = store.IndexSpecVersion
	}
	slot, id := bucket.slot, bucket.id.Load()
	targets := make([]store.IndexSpec, 0, len(stale))
	for _, position := range stale {
		targets = append(targets, rebuilt[position])
		if err := s.clearIndexRange(slot, bucket.databaseID, id, rebuilt[position]); err != nil {
			return fmt.Errorf("rebuilding index %s of bucket %q in database %q: %w", rebuilt[position].Name, name, database, err)
		}
	}
	documents, conflicts, err := s.writeRebuiltEntries(bucket, targets)
	if err != nil {
		return fmt.Errorf("rebuilding the indexes of bucket %q in database %q: %w", name, database, err)
	}
	if err := s.kv.Sync(); err != nil {
		return fmt.Errorf("syncing the rebuilt indexes of bucket %q in database %q: %w", name, database, err)
	}

	names := make([]string, 0, len(targets))
	for _, position := range stale {
		conflict, conflicted := conflicts[rebuilt[position].ID]
		if !conflicted {
			rebuilt[position].State = store.IndexReady
			names = append(names, rebuilt[position].Name)
			continue
		}
		rebuilt[position].State = store.IndexConflicted
		s.logConflictedIndex(database, name, rebuilt[position].Name, conflict)
	}
	if err := s.persistIndexSpecs(bucket, database, name, rebuilt); err != nil {
		return err
	}
	s.logger.Info("rebuilt indexes declared before this version", "database", database, "bucket", name,
		"indexes", strings.Join(names, ", "), "documents", documents, "elapsed", time.Since(started).String())
	return nil
}

// persistIndexSpecs records a bucket's descriptors in its catalog entry, in one
// synced batch of that key alone, then publishes them in the bucket's
// declaration and builds the handle's engine again from it, for the rest of
// the operation that holds it; the next handle builds its engine from the
// declaration anyway. The engine's index manager is only ever built from the
// declaration (buildEngines), never set apart from it, which is what leaves no
// stale manager behind (catalog-gc spec N-8). The caller holds the record's
// mutex, so no write sees the declaration change under it.
func (s *Server) persistIndexSpecs(bucket *inMemoryBucket, database, name string, specs []store.IndexSpec) error {
	entry, catalogued, err := s.readCatalogEntry(database, name)
	if err != nil {
		return err
	}
	if !catalogued {
		return fmt.Errorf("recording the indexes of bucket %q in database %q: it has no catalog entry", name, database)
	}
	entry.Indices = recordIndexSpecs(specs)
	encoded, err := encodeCatalogEntry(entry)
	if err != nil {
		return err
	}
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(catalogKey(database, name), encoded); err != nil {
		return fmt.Errorf("recording the indexes of bucket %q in database %q: %w", name, database, err)
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("recording the indexes of bucket %q in database %q: %w", name, database, err)
	}
	s.publishIndexSpecs(bucket, specs)
	if err := s.buildEngines(bucket, s.kv, bucket.slot, bucket.id.Load()); err != nil {
		return fmt.Errorf("attaching the indexes of bucket %q in database %q: %w", name, database, err)
	}
	return nil
}

// clearIndexRange deletes every entry of one index, in bounded batches. It is
// the rebuild's only destructive step, and it destroys only what the walk
// rewrites.
func (s *Server) clearIndexRange(slot uint16, databaseID, id uint64, spec store.IndexSpec) error {
	bounds := store.NewSecondaryIndex(s.kv, slot, databaseID, id, spec.ID).EntryRange()
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(bounds)
	defer it.Close()
	batch := newBoundedBatch(s.kv)
	defer batch.discard()
	for it.Seek(nil); it.Valid(); it.Next() {
		if err := batch.remove(it.Key()); err != nil {
			return fmt.Errorf("clearing the entries of index %s: %w", spec.Name, err)
		}
	}
	if err := it.Err(); err != nil {
		return fmt.Errorf("clearing the entries of index %s: %w", spec.Name, err)
	}
	if err := batch.flush(); err != nil {
		return fmt.Errorf("clearing the entries of index %s: %w", spec.Name, err)
	}
	return nil
}

// rebuildConflict is two documents a rebuild or a verify walk found holding
// one value of a unique index.
type rebuildConflict struct {
	first, second string
}

// logConflictedIndex is the one Error line for an index marked conflicted. It
// names the index and both documents and never the value (build doc RD-10):
// the value is user data, and a log leaves the authorisation boundary a
// response stays inside.
func (s *Server) logConflictedIndex(database, bucket, index string, conflict rebuildConflict) {
	s.logger.Error(fmt.Sprintf("Unique index %s on bucket %q in database %q holds one value for two documents, %q and %q, "+
		"so it is marked conflicted and search will not use it; nothing was deleted. Change or delete one of the two documents, "+
		"then restart the node to rebuild the index.", index, bucket, database, conflict.first, conflict.second),
		"database", database, "bucket", bucket, "index", index, "first", conflict.first, "second", conflict.second)
}

// writeRebuiltEntries walks the bucket's documents in one snapshot and writes
// each one's entries in every target index, in bounded batches, and returns
// how many documents it walked and, per unique target, the first duplicate it
// met. A unique index is checked as it is written — against the batch being
// written and a snapshot of the batches committed before it — and a duplicate
// is recorded, not skipped: the index is written whole, so the ordinary check
// can refuse a third holder while the index is conflicted (build doc §3.5.1).
func (s *Server) writeRebuiltEntries(bucket *inMemoryBucket, targets []store.IndexSpec) (int, map[uint16]rebuildConflict, error) {
	indexes := make([]*store.SecondaryIndex, len(targets))
	for i, spec := range targets {
		indexes[i] = store.NewSecondaryIndex(s.kv, bucket.slot, bucket.databaseID, bucket.id.Load(), spec.ID)
	}
	batch := newBoundedBatch(s.kv)
	defer batch.discard()
	checks := newRebuildChecks(s.kv)
	defer checks.close()
	hook := indexRebuildHook.Load()
	batch.committed = func(number int) error {
		checks.committedBatch()
		if hook == nil {
			return nil
		}
		return (*hook)(targets[0], number)
	}
	conflicts := make(map[uint16]rebuildConflict)
	names := rebuiltFieldNames(targets)
	documents := 0
	err := bucket.hashStore.IterateDocuments(func(key, value []byte) error {
		// The walk reads this node's own stored bytes, so only their framing
		// is checked, and only the fields the targets name are read.
		stored, err := bson.TrustStored(value)
		if err != nil {
			return fmt.Errorf("decoding document %q: %w", key, err)
		}
		document, err := store.StoredFields(stored, names)
		if err != nil {
			return fmt.Errorf("decoding document %q: %w", key, err)
		}
		for i, spec := range targets {
			encoded, indexed, err := store.EntryKey(spec, document)
			if err != nil {
				return fmt.Errorf("document %q: %w", key, err)
			}
			if !indexed {
				continue
			}
			if _, seen := conflicts[spec.ID]; spec.Unique && !seen {
				holder, held, err := checks.holder(indexes[i], spec.ID, encoded, key)
				if err != nil {
					return fmt.Errorf("checking unique index %s: %w", spec.Name, err)
				}
				if held {
					conflicts[spec.ID] = rebuildConflict{first: holder, second: string(key)}
				}
			}
			if err := batch.add(func(b storage.KVBatch) error { return indexes[i].Add(b, encoded, key) }, len(key)+len(encoded)); err != nil {
				return err
			}
			if spec.Unique {
				checks.claim(spec.ID, encoded, key)
			}
		}
		documents++
		return nil
	})
	if err != nil {
		return documents, nil, err
	}
	return documents, conflicts, batch.flush()
}

// rebuiltFieldNames are the top-level fields the targets' entries read.
func rebuiltFieldNames(targets []store.IndexSpec) []string {
	var names []string
	for _, spec := range targets {
		names = append(names, spec.FieldNames()...)
	}
	return names
}

// rebuildChecks is what a rebuild checks a unique key against: the keys of
// the batch it is writing, which no snapshot sees yet, and a snapshot of the
// batches it has committed. The claims are one batch's, so the memory is
// bounded by the batch, not the index.
type rebuildChecks struct {
	kv        storage.KV
	committed storage.KVSnapshot
	claims    map[string]string
}

func newRebuildChecks(kv storage.KV) *rebuildChecks {
	return &rebuildChecks{kv: kv, committed: kv.NewSnapshot(), claims: make(map[string]string)}
}

// committedBatch moves the checks past a batch that has just committed.
func (c *rebuildChecks) committedBatch() {
	c.committed.Discard()
	c.committed = c.kv.NewSnapshot()
	c.claims = make(map[string]string)
}

func (c *rebuildChecks) claim(indexID uint16, encoded, document []byte) {
	c.claims[fmt.Sprintf("%d/%x", indexID, encoded)] = string(document)
}

// holder returns another document holding encoded in the index, if any.
func (c *rebuildChecks) holder(index *store.SecondaryIndex, indexID uint16, encoded, document []byte) (string, bool, error) {
	if claimed, taken := c.claims[fmt.Sprintf("%d/%x", indexID, encoded)]; taken && claimed != string(document) {
		return claimed, true, nil
	}
	holder, held, err := index.Holder(c.committed, encoded, document)
	return string(holder), held, err
}

func (c *rebuildChecks) close() { c.committed.Discard() }

// verifyUniqueIndexesLocked walks each ready unique index of a bucket whose
// range arrived without being checked — a migration writes the source's keys
// raw (C-46) — and marks one that holds a value twice conflicted. It is
// detection, not prevention (spec §4.5): O(entries of each unique index), one
// snapshot each. The caller holds bucket.mu.
func (s *Server) verifyUniqueIndexesLocked(bucket *inMemoryBucket, database, name string) error {
	specs := bucket.declaredIndexes()
	verified := cloneSpecs(specs)
	changed := false
	for position, spec := range specs {
		if !spec.Unique || spec.State != store.IndexReady || indexesAPath(spec) {
			continue
		}
		first, second, found, err := store.NewSecondaryIndex(s.kv, bucket.slot, bucket.databaseID, bucket.id.Load(), spec.ID).FirstDuplicate(len(spec.Fields))
		if err != nil {
			return fmt.Errorf("verifying unique index %s of bucket %q in database %q: %w", spec.Name, name, database, err)
		}
		if !found {
			continue
		}
		verified[position].State = store.IndexConflicted
		changed = true
		s.logConflictedIndex(database, name, spec.Name, rebuildConflict{first: string(first), second: string(second)})
	}
	if !changed {
		return nil
	}
	return s.persistIndexSpecs(bucket, database, name, verified)
}

// boundedBatch commits every rebuildBatchEntries keys, or sooner when the
// batch nears a quarter of what the store commits atomically.
type boundedBatch struct {
	kv        storage.KV
	batch     storage.KVBatch
	entries   int
	bytes     int64
	number    int
	committed func(number int) error
}

func newBoundedBatch(kv storage.KV) *boundedBatch {
	return &boundedBatch{kv: kv, batch: kv.NewBatch()}
}

func (b *boundedBatch) remove(key []byte) error {
	return b.add(func(batch storage.KVBatch) error { return batch.Delete(key) }, len(key))
}

func (b *boundedBatch) add(write func(storage.KVBatch) error, size int) error {
	if err := write(b.batch); err != nil {
		return err
	}
	b.entries++
	b.bytes += int64(size)
	if b.entries >= rebuildBatchEntries || (b.kv.MaxBatchBytes() > 0 && b.bytes >= b.kv.MaxBatchBytes()/4) {
		return b.flush()
	}
	return nil
}

// flush commits what the batch holds and starts the next one.
func (b *boundedBatch) flush() error {
	if b.entries == 0 {
		return nil
	}
	if err := b.batch.Commit(false); err != nil {
		return fmt.Errorf("committing a batch of %d keys: %w", b.entries, err)
	}
	b.batch.Discard()
	b.batch = b.kv.NewBatch()
	b.entries, b.bytes = 0, 0
	b.number++
	if b.committed != nil {
		if err := b.committed(b.number); err != nil {
			return fmt.Errorf("the rebuild stopped after batch %d: %w", b.number, err)
		}
	}
	return nil
}

func (b *boundedBatch) discard() { b.batch.Discard() }

// cloneSpecs copies a descriptor list so a published one is never changed in
// place.
func cloneSpecs(specs []store.IndexSpec) []store.IndexSpec {
	return append([]store.IndexSpec(nil), specs...)
}
