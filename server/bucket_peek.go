package server

import (
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// Peeks (catalog-gc spec §5.4, classes P and PL). Most lookup sites need a
// handful of a bucket's facts — its id, type, slot, declaration — and not the
// bucket: no engine, no write, no lock held across their work. A peek copies
// those facts inside one lock hold and hands back the copies, so no site
// keeps a pointer to the bucket's record. A record's slot is recycled once its
// bucket is dropped: a site that held the record could read a recycled one; a
// site that holds a copy cannot.

// bucketFacts is what a peek copies out of a bucket. type, slot and the
// declaration are copied only once id is non-zero, because a create writes
// them while id is 0 and stores id after them (I-1); a bucket whose create
// has not committed has an id of 0 and no type, slot or declaration here.
type bucketFacts struct {
	id         uint64
	generation int64
	slot       uint16
	// databaseID is storage.LegacyDatabaseID of the database the facts were
	// copied from; it stands in until the server has real database ids.
	databaseID        uint64
	bucketType        BucketType
	replicationFactor int32
	// spread is recSpread: a hash bucket recorded placement spread.
	spread bool
	// term and lineageFlags are the lineage cache (lineage.go), which may
	// not be loaded yet: lineageFlags&lineageKnown says whether it is.
	term         uint64
	lineageFlags uint32
	// decl is the declaration the table held when the facts were copied, nil
	// for a plain bucket, and typedFields the typed-field declaration, nil
	// for none.
	decl        *bucketDecl
	typedFields *bucketFieldTypes
}

// indexes is the bucket's published index list, nil when it declares none.
func (f bucketFacts) indexes() []store.IndexSpec {
	if f.decl == nil {
		return nil
	}
	return f.decl.indexSpecs.load()
}

// fieldTypes is the bucket's typed-field declaration, nil when it declares none.
func (f bucketFacts) fieldTypes() *bucketFieldTypes {
	return f.typedFields
}

// storeFields is the bucket's declared store fields, none for a plain bucket.
func (f bucketFacts) storeFields() *storeFieldSet {
	if f.decl != nil {
		if set := f.decl.fieldStores.Load(); set != nil {
			return set
		}
	}
	return noStoreFields
}

// peekBucket copies the facts of database/name under one db.mu read hold
// (class P). It takes no bucket lock, so it does not wait out a create in
// flight: such a bucket is reported held, with an id of 0. The second result
// is false when the name is not in the registry.
func (s *Server) peekBucket(database, name string) (bucketFacts, bool) {
	db := s.serverDataAccessor().getDB(database)
	if db == nil {
		return bucketFacts{}, false
	}
	return s.peekBucketIn(database, db, name)
}

// peekBucketIn is peekBucket once the database's struct is in hand.
func (s *Server) peekBucketIn(database string, db *inMemoryDB, name string) (bucketFacts, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	held, found := db.table.get(name)
	if !found {
		return bucketFacts{}, false
	}
	return s.factsOf(database, held.rec), true
}

// peekSettled is a peek that waits out an attach in flight (class PL): it
// takes the record's read lock, which a create holds until its id is stored
// or it has failed, and copies the facts under it. A bucket a drop closed
// meanwhile, or whose record now names another bucket, is reported not held.
func (s *Server) peekSettled(database, name string) (bucketFacts, bool) {
	db := s.serverDataAccessor().getDB(database)
	if db == nil {
		return bucketFacts{}, false
	}
	held, found := db.findBucket(name)
	if !found {
		return bucketFacts{}, false
	}
	held.rec.mu.RLock()
	defer held.rec.mu.RUnlock()
	if !held.current() {
		return bucketFacts{}, false
	}
	return s.factsOf(database, held.rec), true
}

// factsOf copies rec's facts. The caller holds db.mu's read lock, having
// found rec by name, or rec.mu, having checked its epoch: a drop frees a
// record only after it has unlinked the name under db.mu and closed the
// bucket under rec.mu (spec §5.3), so under either the copy is of the bucket
// the caller found.
func (s *Server) factsOf(database string, rec *bucketRec) bucketFacts {
	facts := bucketFacts{
		databaseID:        storage.LegacyDatabaseID(database),
		id:                rec.id.Load(),
		generation:        rec.generation.Load(),
		replicationFactor: rec.replicationFactor.Load(),
		spread:            rec.isSpread(),
		term:              rec.term.Load(),
		lineageFlags:      rec.lineageFlags.Load(),
	}
	if facts.id == 0 {
		return facts
	}
	facts.slot, facts.bucketType = rec.slot, bucketTypeOfCode(rec.bucketType)
	facts.decl = s.declarationFor(rec, facts.id)
	facts.typedFields = s.fieldTypes.holder(rec.typesID.Load())
	return facts
}
