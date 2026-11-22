package server

import (
	"sync"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-server/store"
)

// The declaration table (catalog-gc spec §5.2 "declaration table", §5.3
// "post-publication writes"). Most buckets declare nothing, and a bucket's
// index list and store fields are the pointer-bearing part of it that cannot
// be a fixed-size record. So they are kept here, keyed by bucket id, for the
// buckets that have them; the record says whether to look (recHasDecl), and a
// plain bucket costs nothing.
//
// A typed-field declaration is not here: buckets share it, and the record
// holds its id in the interned set (fieldTypeSet), so a bucket that declares
// typed fields and nothing else has no entry in this table.

// bucketDecl is what a bucket declares beyond its type and its typed fields,
// and nothing else. Both lists are published whole behind atomic pointers,
// so a reader that loaded one reads it with no lock.
type bucketDecl struct {
	indexSpecs  publishedSpecs
	fieldStores atomic.Pointer[storeFieldSet]
}

// idShardCount spreads an id-keyed table over this many locks, so readers of
// different buckets do not all meet on one.
const idShardCount = 64

// idShards maps a bucket or declaration id to a value. Each shard's lock is a
// leaf: nothing is acquired while it is held. Its zero value is ready to use.
type idShards[V any] struct {
	shards [idShardCount]idShard[V]
}

type idShard[V any] struct {
	mu   sync.RWMutex
	byID map[uint64]V
}

func (t *idShards[V]) shard(id uint64) *idShard[V] { return &t.shards[id%idShardCount] }

// get returns id's value, and false when there is none.
func (t *idShards[V]) get(id uint64) (V, bool) {
	shard := t.shard(id)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	found, present := shard.byID[id]
	return found, present
}

// put enters id's value.
func (t *idShards[V]) put(id uint64, entry V) {
	shard := t.shard(id)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.byID == nil {
		shard.byID = make(map[uint64]V)
	}
	shard.byID[id] = entry
}

// remove drops id's value.
func (t *idShards[V]) remove(id uint64) {
	shard := t.shard(id)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	delete(shard.byID, id)
}

// count is the number of ids in the table.
func (t *idShards[V]) count() int {
	entries := 0
	for i := range t.shards {
		shard := &t.shards[i]
		shard.mu.RLock()
		entries += len(shard.byID)
		shard.mu.RUnlock()
	}
	return entries
}

// declTable maps a bucket id to its declaration, for flagged buckets only. A
// writer takes a shard's lock inside bucket.mu; a reader inside no lock, or
// db.mu's read hold.
type declTable = idShards[*bucketDecl]

// declaredIndexes is the bucket's published index list, nil for a bucket that
// declares none. It takes no lock.
func (b *inMemoryBucket) declaredIndexes() []store.IndexSpec {
	if decl := b.decl.Load(); decl != nil {
		return decl.indexSpecs.load()
	}
	return nil
}

// publishIndexSpecs publishes a bucket's index list into its declaration,
// giving a bucket that had none a declaration only when the list is not empty.
// The caller holds bucket.mu.
func (s *Server) publishIndexSpecs(bucket *inMemoryBucket, specs []store.IndexSpec) {
	if decl := bucket.decl.Load(); decl != nil {
		decl.indexSpecs.publish(specs)
		return
	}
	if len(specs) == 0 {
		return
	}
	decl := &bucketDecl{}
	decl.indexSpecs.publish(specs)
	s.adoptDeclaration(bucket, decl)
}

// setStoreFields publishes a bucket's store-field set into its declaration,
// giving a bucket that had none a declaration only for a set that is not nil.
// The caller holds bucket.mu.
func (s *Server) setStoreFields(bucket *inMemoryBucket, set *storeFieldSet) {
	if decl := bucket.decl.Load(); decl != nil {
		decl.fieldStores.Store(set)
		return
	}
	if set == nil {
		return
	}
	decl := &bucketDecl{}
	decl.fieldStores.Store(set)
	s.adoptDeclaration(bucket, decl)
}

// declaredFieldTypes is the bucket's typed-field declaration, nil for a bucket
// that declares none. It takes no lock.
func (b *inMemoryBucket) declaredFieldTypes() *bucketFieldTypes {
	return b.fieldTypes
}

// setFieldTypes gives a bucket the typed-field declaration holder, nil for
// none. An attached bucket's record moves to the holder's interned id at once
// (a post-publication write, spec §5.3); a bucket not attached yet takes it
// at its attach (publishAttachLocked). The caller holds bucket.mu.
func (s *Server) setFieldTypes(bucket *inMemoryBucket, holder *bucketFieldTypes) {
	if bucket.id.Load() != 0 {
		next := s.fieldTypes.acquire(holder)
		if previous := bucket.rec.typesID.Swap(next); previous != 0 {
			s.fieldTypes.release(previous)
		}
		holder = s.fieldTypes.holder(next)
	}
	bucket.fieldTypes = holder
}

// adoptDeclaration gives a plain bucket the declaration decl, already filled.
// An attached bucket is a post-publication write (spec §5.3): the declaration
// is entered in the table and only then flagged, so a reader that sees the flag
// finds it, and one that does not sees a plain bucket, a valid ordering of the
// declare. A bucket not attached yet is entered at its attach, before its id is
// stored (enterDeclarationsLocked). The caller holds bucket.mu.
func (s *Server) adoptDeclaration(bucket *inMemoryBucket, decl *bucketDecl) {
	if id := bucket.id.Load(); id != 0 {
		s.decls.put(id, decl)
		bucket.rec.setFlag(recHasDecl, true)
	}
	bucket.decl.Store(decl)
}

// enterDeclarationsLocked enters an attaching bucket's declarations under the
// id it is being attached with: its index list and store fields in the table,
// flagged, and its typed fields by their interned id. The attach calls it
// before it stores id, so a reader that sees the id sees both (I-1). The
// caller holds bucket.mu.
func (s *Server) enterDeclarationsLocked(bucket *inMemoryBucket, id uint64) {
	if decl := bucket.decl.Load(); decl != nil {
		s.decls.put(id, decl)
		bucket.rec.setFlag(recHasDecl, true)
	}
	if bucket.fieldTypes != nil {
		typesID := s.fieldTypes.acquire(bucket.fieldTypes)
		bucket.rec.typesID.Store(typesID)
		bucket.fieldTypes = s.fieldTypes.holder(typesID)
	}
}

// declarationFor is the declaration of an attached bucket's record: the
// table's entry when it is flagged, none when it is plain. id is the record's,
// loaded before this, and the caller holds db.mu's read lock or the record's
// mutex, under which a drop cannot remove the entry.
func (s *Server) declarationFor(rec *bucketRec, id uint64) *bucketDecl {
	if !rec.hasDecl() {
		return nil
	}
	decl, _ := s.decls.get(id)
	return decl
}

// dropDeclarationsLocked takes a closing bucket's declarations out of the
// hoarddb_indexes gauge, out of the table, and out of the interned set. The
// caller holds the record's mutex.
func (s *Server) dropDeclarationsLocked(rec *bucketRec) {
	if rec.hasDecl() {
		id := rec.id.Load()
		if decl, found := s.decls.get(id); found {
			decl.indexSpecs.uncount()
		}
		s.decls.remove(id)
		rec.setFlag(recHasDecl, false)
	}
	if typesID := rec.typesID.Swap(0); typesID != 0 {
		s.fieldTypes.release(typesID)
	}
}
