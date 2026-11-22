package server

import "sync"

// fieldTypeSet is the interned set of typed-field declarations (catalog-gc
// build doc, the 2026-10-05 note on CG-13): every bucket that declares the
// same typed fields shares one holder, named by a small id that its record
// carries (bucketRec.typesID). 10^6 buckets of one entity's schema hold
// one declaration and a million four-byte ids, not a million table entries.
//
// It is memory only. The catalog entry still records each bucket's
// declaration (no on-disk change); the set is rebuilt as the catalog loads,
// and an id means nothing across a restart.
//
// Two declarations are the same when their holders share an interned schema
// (schemaInterner, by the encoded declaration and mode) and neither carries
// an unenforceable recorded form: such a holder is per bucket, because what
// makes it unusable may be the bucket's own store fields or indexes, so it
// gets an id of its own. That is the rare case, and costs what a table entry
// did.
//
// mu guards the ids and counts and is a leaf; a reader of a holder by id
// takes only its shard's read lock, so writes to typed buckets do not meet on
// one lock.
type fieldTypeSet struct {
	mu     sync.Mutex
	byKey  map[fieldTypesKey]uint32
	shared map[uint32]*sharedFieldTypes
	nextID uint32
	freed  []uint32

	holders idShards[*bucketFieldTypes]
}

// fieldTypesKey is what makes two holders the same declaration: the interned
// schema, and for an unenforceable declaration the holder's own recorded form.
type fieldTypesKey struct {
	schema   *declaredSchema
	unusable *unusableDeclaration
}

// sharedFieldTypes is one interned declaration and how many records hold it.
type sharedFieldTypes struct {
	key        fieldTypesKey
	holder     *bucketFieldTypes
	references int
}

// acquire returns the id of holder's declaration, interning holder if no
// bucket holds an equal one, and counts one more record holding it. A nil
// holder is id 0, which is not counted. The caller hands the id to a record
// and calls release when the record lets it go.
func (set *fieldTypeSet) acquire(holder *bucketFieldTypes) uint32 {
	if holder == nil {
		return 0
	}
	key := fieldTypesKey{schema: holder.schema, unusable: holder.unusable}
	set.mu.Lock()
	defer set.mu.Unlock()
	if id, found := set.byKey[key]; found {
		set.shared[id].references++
		return id
	}
	id := set.takeID()
	if set.byKey == nil {
		set.byKey = make(map[fieldTypesKey]uint32)
		set.shared = make(map[uint32]*sharedFieldTypes)
	}
	set.byKey[key] = id
	set.shared[id] = &sharedFieldTypes{key: key, holder: holder, references: 1}
	set.holders.put(uint64(id), holder)
	return id
}

// takeID returns a freed id, or the next one. The caller holds mu. An id is
// reused only once no record holds it, and a reader copies the holder, never
// the id, out of the hold it found the record under, so a reused id cannot
// reach a reader of the declaration it used to name.
func (set *fieldTypeSet) takeID() uint32 {
	if last := len(set.freed) - 1; last >= 0 {
		id := set.freed[last]
		set.freed = set.freed[:last]
		return id
	}
	set.nextID++
	return set.nextID
}

// release counts one record fewer holding id, and forgets the declaration
// with its last holder. id is one acquire returned and not 0.
func (set *fieldTypeSet) release(id uint32) {
	set.mu.Lock()
	defer set.mu.Unlock()
	interned, found := set.shared[id]
	if !found {
		return
	}
	interned.references--
	if interned.references > 0 {
		return
	}
	delete(set.byKey, interned.key)
	delete(set.shared, id)
	set.holders.remove(uint64(id))
	set.freed = append(set.freed, id)
}

// holder returns the declaration id names, nil for 0. The caller holds a lock
// under which the record carrying id cannot release it: db.mu, for reading,
// with the record found by name, or the record's mutex.
func (set *fieldTypeSet) holder(id uint32) *bucketFieldTypes {
	if id == 0 {
		return nil
	}
	found, _ := set.holders.get(uint64(id))
	return found
}

// count is the number of distinct declarations interned.
func (set *fieldTypeSet) count() int {
	set.mu.Lock()
	defer set.mu.Unlock()
	return len(set.shared)
}
