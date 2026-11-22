// Package store provides store type implementations for hoardDB buckets.
package store

import (
	"errors"
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
)

// HashStore is a hash bucket on the node's shared store: its documents are
// the keys [s][id][0x01][user key], their read-repair versions [s][id][0x02]
// [user key], and its count lives in the bucket's state document
// (shared-storage spec §3.2). It opens nothing, so a hash bucket costs no
// descriptor, mapping or goroutine.
//
// Every mutation goes through Apply, under the bucket mutex the caller holds:
// the document, its version, the count and the bucket's applied_seq commit in
// one batch. owed is the bucket's failed-apply carry, which the caller owns:
// it outlives the engine (Carry).
type HashStore struct {
	owed       *OwedApplies
	kv         storage.KV
	slot       uint16
	databaseID uint64
	id         uint64
	idxMgr     *IndexManager
}

// HashRecountHook, when set, runs each time HashStore.Recount does. Recount
// is the audit path and never runs on the open path; tests count the calls to
// prove it. Nil in production. Exported because the server's tests set it.
var HashRecountHook atomic.Pointer[func()]

// NewHashStore returns the hash store of bucket id in slot, which must be the
// bucket's recorded slot, carrying its failed applies in owed.
func NewHashStore(kv storage.KV, slot uint16, databaseID, id uint64, owed *OwedApplies) *HashStore {
	return &HashStore{owed: owed, kv: kv, slot: slot, databaseID: databaseID, id: id}
}

// Carry is the bucket's failed-apply carry the store was built with.
func (h *HashStore) Carry() *OwedApplies { return h.owed }

// SetIndexManager attaches an optional IndexManager. When set, every write
// adds its index entries to its own batch, so a document and its entries
// commit together.
func (h *HashStore) SetIndexManager(im *IndexManager) {
	h.idxMgr = im
}

func (h *HashStore) documentKey(key string) []byte {
	return storage.Key(h.slot, h.databaseID, h.id, storage.NSDocuments, []byte(key))
}

// documentRange bounds [s][id][0x01], half-open.
func (h *HashStore) documentRange() storage.IterOptions {
	return storage.IterOptions{
		Lower: storage.Key(h.slot, h.databaseID, h.id, storage.NSDocuments),
		Upper: storage.Key(h.slot, h.databaseID, h.id, storage.NSDocuments+1),
	}
}

// apply runs op as one hash mutation.
func (h *HashStore) apply(info ApplyInfo, op func(storage.KVSnapshot, *BucketState, storage.KVBatch) error) (bool, error) {
	return Apply(ApplyRequest{KV: h.kv, Slot: h.slot, DatabaseID: h.databaseID, ID: h.id, Keyed: true, Info: info, Owed: h.owed, Op: op})
}

// putDocument adds one document write and its index entries to b and updates
// st: a new key grows entries, an overwrite does not and replaces the old
// version's index entries, and the logical size follows the key and value
// lengths (spec §11 Q22). The fields every index the bucket declares reads
// are read once, from the bytes being stored (build doc Step 3, C-57).
func (h *HashStore) putDocument(snap storage.KVSnapshot, claims *UniqueClaims, st *BucketState, b storage.KVBatch, doc hashDocument) error {
	key, encoded, version := doc.key, doc.encoded, doc.version
	if doc.check.refusedUnread() {
		return &DocumentExistsError{}
	}
	docKey := h.documentKey(key)
	previous, err := snap.Get(docKey)
	if err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
		return fmt.Errorf("reading key %s: %w", key, err)
	}
	stored := err == nil
	// Before anything is added to the batch: the read is the one an overwrite
	// needs anyway, so an insert's check costs nothing more.
	if doc.check.refuses(stored) {
		return &DocumentExistsError{Key: key}
	}
	if h.idxMgr != nil {
		if err := h.indexDocument(snap, claims, b, key, previous, stored, encoded); err != nil {
			return err
		}
	}
	if stored {
		st.LogicalBytes += int64(len(encoded) - len(previous))
	} else {
		st.Entries++
		st.LogicalBytes += int64(len(key) + len(encoded))
	}
	if err := b.Set(docKey, encoded); err != nil {
		return fmt.Errorf("putting key %s: %w", key, err)
	}
	if version > 0 {
		if err := b.Set(versionKey(h.slot, h.databaseID, h.id, []byte(key)), versionBytes(version)); err != nil {
			return fmt.Errorf("putting the version of key %s: %w", key, err)
		}
	}
	return nil
}

// indexDocument adds a document write's index entries to b: the new
// document's, and for an overwrite the removal of the previous version's.
// Every unique index is checked first, against snap, so a refusal returns
// before anything is added to the batch.
//
// Only the fields the indexes name are read, never the whole document
// (bson-codec-spec 7.2). The new document is the bytes this write is about to
// store, encoded here, so it is checked whole, at the library's ceiling, which
// accepts what a decode accepts; the previous version is read back from this
// store, so only its framing is checked, and the field read walks its top
// level.
func (h *HashStore) indexDocument(snap storage.KVSnapshot, claims *UniqueClaims, b storage.KVBatch, key string, previous []byte, stored bool, encoded []byte) error {
	written, err := bson.Validate(encoded, bson.Limits{Depth: bson.MaxNestingCeiling})
	if err != nil {
		return fmt.Errorf("decoding key %s to index it: %w", key, err)
	}
	document, err := h.idxMgr.IndexedFields(written)
	if err != nil {
		return fmt.Errorf("decoding key %s to index it: %w", key, err)
	}
	var previousDoc map[string]interface{}
	if stored {
		stale, err := bson.TrustStored(previous)
		if err == nil {
			previousDoc, err = h.idxMgr.IndexedFields(stale)
		}
		if err != nil {
			return fmt.Errorf("decoding the stored key %s to reindex it: %w", key, err)
		}
	}
	if err := h.idxMgr.CheckUnique(snap, claims, []byte(key), previousDoc, document); err != nil {
		return fmt.Errorf("indexing key %s: %w", key, err)
	}
	if !stored {
		if err := h.idxMgr.OnInsert(b, []byte(key), document); err != nil {
			return fmt.Errorf("indexing key %s: %w", key, err)
		}
		return nil
	}
	if err := h.idxMgr.OnUpdate(b, []byte(key), previousDoc, document); err != nil {
		return fmt.Errorf("reindexing key %s: %w", key, err)
	}
	return nil
}

// Put stores a document under key. An existing key is an upsert: the document
// is replaced and the count does not change. version, when non-zero, is the
// write's read-repair version and is stamped in the same batch; zero leaves any
// earlier version in place. A replayed entry the gate skips changes nothing.
func (h *HashStore) Put(info ApplyInfo, key string, value interface{}, version uint64) error {
	_, err := h.PutBelow(info, PutRequest{Key: key, Value: value, Version: version})
	return err
}

// hashDocument is one document putDocument writes: its key, its encoded
// form, its version and the insert's existence check.
type hashDocument struct {
	key     string
	encoded []byte
	version uint64
	check   insertCheck
}

// PutBelow is Put bounded by a version: a consistent get's repair of a stale
// or missing copy (BUG-0480). The document is written only if the key's
// version side-key, as stored when the put applies, is below req.Below (an
// absent key or side-key is version 0); a key at or above it was written after
// the read that sent the repair, and is left whole, which spared reports. The
// bound is evaluated inside the apply, so every copy, applying the repair and
// that write in one log order, decides alike. A bound of 0 is no bound. With
// req.RefuseExisting a held key is refused with *DocumentExistsError.
func (h *HashStore) PutBelow(info ApplyInfo, req PutRequest) (spared bool, err error) {
	key, ok := req.Key.(string)
	if !ok {
		return false, fmt.Errorf("a hash bucket's key is a string, not %T", req.Key)
	}
	encoded, err := bson.Marshal(req.Value)
	if err != nil {
		return false, fmt.Errorf("marshaling value for key %s: %w", key, err)
	}
	doc := hashDocument{key: key, encoded: encoded, version: req.Version, check: newInsertCheck(info, req.RefuseExisting)}
	_, err = h.apply(info, func(snap storage.KVSnapshot, st *BucketState, b storage.KVBatch) error {
		spared = false
		if req.Below != 0 {
			newer, err := storedAtOrAbove(snap, versionKey(h.slot, h.databaseID, h.id, []byte(key)), req.Below)
			if err != nil || newer {
				spared = newer
				return err
			}
		}
		return h.putDocument(snap, nil, st, b, doc)
	})
	if err != nil {
		return false, fmt.Errorf("putting key %s: %w", key, err)
	}
	return spared, nil
}

// PutBatch stores every document of req.Items under its "_id_" in one batch
// with the bucket's state, and returns the ids in order. Two documents with
// one id collapse into the last, as an upsert does. The batch is all or
// nothing: a refusal of any document — a unique index's among them, checked
// in frame order against the batch's own earlier documents as well as the
// store — writes none of them (spec §4.2.1). With req.RefuseExisting, every
// document is checked before the refusal returns, so *DocumentExistsError
// names every position whose key is held.
func (h *HashStore) PutBatch(info ApplyInfo, req PutBatchRequest) ([]string, error) {
	ids := make([]string, 0, len(req.Items))
	encodedByID := make(map[string][]byte, len(req.Items))
	positionsByID := make(map[string][]int, len(req.Items))
	claims := &UniqueClaims{}
	for position, doc := range req.Items {
		id, _ := doc["_id_"].(string)
		encoded, err := bson.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("marshaling document %q: %w", id, err)
		}
		if _, seen := encodedByID[id]; !seen {
			ids = append(ids, id)
		}
		encodedByID[id] = encoded
		positionsByID[id] = append(positionsByID[id], position+1)
		claims.At(id, position+1)
	}
	check := newInsertCheck(info, req.RefuseExisting)
	_, err := h.apply(info, func(snap storage.KVSnapshot, st *BucketState, b storage.KVBatch) error {
		var exists *DocumentExistsError
		for _, id := range ids {
			// Once one key is refused nothing more is put: the rest are only
			// read, to name every held position.
			if exists != nil {
				held, err := h.holds(snap, id)
				if err != nil {
					return err
				}
				if held {
					exists.Positions = append(exists.Positions, positionsByID[id]...)
				}
				continue
			}
			err := h.putDocument(snap, claims, st, b, hashDocument{key: id, encoded: encodedByID[id], version: req.Version, check: check})
			if errors.As(err, &exists) && exists.Key != nil {
				exists.Positions = append(exists.Positions, positionsByID[id]...)
				continue
			}
			if err != nil {
				return err
			}
		}
		if exists != nil {
			slices.Sort(exists.Positions)
			return exists
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("batch put: %w", err)
	}
	returned := make([]string, 0, len(req.Items))
	for _, doc := range req.Items {
		id, _ := doc["_id_"].(string)
		returned = append(returned, id)
	}
	return returned, nil
}

// holds reports whether the bucket stores a document under key in snap.
func (h *HashStore) holds(snap storage.KVSnapshot, key string) (bool, error) {
	_, err := snap.Get(h.documentKey(key))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reading key %s: %w", key, err)
	}
	return true, nil
}

// Delete removes a document, its version and its field stores, for a bucket
// that declares no blobStore field (DeleteDocument's nil layout). Deleting a
// key that is not there succeeds and changes no count.
func (h *HashStore) Delete(info ApplyInfo, key string) error {
	_, err := h.DeleteDocument(info, key, nil)
	return err
}

// DeleteDocument is Delete for a bucket with its field layout: a blobStore
// field's elements take their metadata with them in the same batch, and
// their payload paths are returned for the caller to unlink after the commit.
func (h *HashStore) DeleteDocument(info ApplyInfo, key string, layout FieldLayout) ([]string, error) {
	_, payloads, err := h.deleteCascading(info, key, layout, nil, 0)
	if err != nil {
		return nil, fmt.Errorf("deleting key %s: %w", key, err)
	}
	return payloads, nil
}

// DeleteIfExpired is DeleteDocument for a TTL expiry: the same delete, with
// the same cascade, made only if the document's field still holds an expired
// value under condition. The condition is evaluated inside the apply against
// the document as stored now — so a document refreshed after the sweep
// scanned it is spared, on every node that applies the entry (GAP-14 build
// doc Step 16, S-7). A spared or absent document leaves the bucket as it is,
// and spared reports the first.
func (h *HashStore) DeleteIfExpired(info ApplyInfo, key string, condition ExpiryCondition, layout FieldLayout) (spared bool, payloads []string, err error) {
	spared, payloads, err = h.deleteCascading(info, key, layout, &condition, 0)
	if err != nil {
		return false, nil, fmt.Errorf("expiring key %s: %w", key, err)
	}
	return spared, payloads, nil
}

// DeleteDocumentBelow is DeleteDocument bounded by a version: a consistent
// get's repair of a key its owner no longer holds. The document goes only if
// its version side-key, as stored when the delete applies, is below below (an
// absent side-key is version 0); a document at or above it was written after
// the read that sent the repair, and is spared whole, with its index entries,
// its version and its field stores. The bound is evaluated inside the apply,
// so every copy, applying the repair and that write in one log order, decides
// alike. An absent document is not deleted under a bound either. A bound of 0
// is no bound.
func (h *HashStore) DeleteDocumentBelow(info ApplyInfo, key string, layout FieldLayout, below uint64) (spared bool, payloads []string, err error) {
	spared, payloads, err = h.deleteCascading(info, key, layout, nil, below)
	if err != nil {
		return false, nil, fmt.Errorf("deleting key %s below version %d: %w", key, below, err)
	}
	return spared, payloads, nil
}

// deleteCascading is the one delete of a document: the document, its index
// entries, its version and its field stores in one batch, so a user's delete
// and a TTL expiry cannot differ in what they remove (owner ruling
// 2026-09-30). A non-nil condition makes it an expiry, which deletes nothing
// — not the orphaned stores of an absent document either — unless the stored
// document is still expired. A non-zero below makes it a repair bounded by a
// version, which deletes nothing unless the stored document's version is
// below it.
func (h *HashStore) deleteCascading(info ApplyInfo, key string, layout FieldLayout, condition *ExpiryCondition, below uint64) (spared bool, payloads []string, err error) {
	_, err = h.apply(info, func(snap storage.KVSnapshot, st *BucketState, b storage.KVBatch) error {
		spared, payloads = false, nil
		previous, err := snap.Get(h.documentKey(key))
		if err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
			return fmt.Errorf("reading key %s: %w", key, err)
		}
		stored := err == nil
		if condition != nil {
			if !stored {
				return nil
			}
			// Read back from this store: only the one field is read.
			document, err := bson.TrustStored(previous)
			if err != nil {
				return fmt.Errorf("decoding key %s to check its expiry: %w", key, err)
			}
			fields, err := StoredFields(document, []string{condition.Field})
			if err != nil {
				return fmt.Errorf("decoding key %s to check its expiry: %w", key, err)
			}
			if !condition.Expired(fields[condition.Field]) {
				spared = true
				return nil
			}
		}
		if below != 0 {
			if !stored {
				return nil
			}
			if spared, err = storedAtOrAbove(snap, versionKey(h.slot, h.databaseID, h.id, []byte(key)), below); err != nil || spared {
				return err
			}
		}
		if stored {
			if err := h.removeStored(st, b, key, previous); err != nil {
				return err
			}
		}
		if err := b.Delete(versionKey(h.slot, h.databaseID, h.id, []byte(key))); err != nil {
			return fmt.Errorf("deleting the version of key %s: %w", key, err)
		}
		// The document's field stores go in the same batch, and whether or
		// not it existed: elements that outlive their document are adopted
		// by the next document with the same key (nested buckets §4.6).
		owner, err := FieldOwnerPrefix(h.slot, h.databaseID, h.id, key)
		if err != nil {
			return err
		}
		if payloads, err = deleteFieldStores(FieldWrite{Snap: snap, State: st, Batch: b}, owner, layout); err != nil {
			return fmt.Errorf("deleting the field stores of key %s: %w", key, err)
		}
		return nil
	})
	return spared, payloads, err
}

// removeStored adds the removal of a stored document and its index entries
// to b, and takes it off st.
func (h *HashStore) removeStored(st *BucketState, b storage.KVBatch, key string, previous []byte) error {
	if h.idxMgr != nil {
		// Read back from this store: only the indexed fields are read.
		document, err := bson.TrustStored(previous)
		if err != nil {
			return fmt.Errorf("decoding deleted key %s to unindex it: %w", key, err)
		}
		previousDoc, err := h.idxMgr.IndexedFields(document)
		if err != nil {
			return fmt.Errorf("decoding deleted key %s to unindex it: %w", key, err)
		}
		if err := h.idxMgr.OnDelete(b, []byte(key), previousDoc); err != nil {
			return fmt.Errorf("unindexing key %s: %w", key, err)
		}
	}
	st.Entries--
	st.LogicalBytes -= int64(len(key) + len(previous))
	if err := b.Delete(h.documentKey(key)); err != nil {
		return fmt.Errorf("deleting key %s: %w", key, err)
	}
	return nil
}

// Get retrieves the BSON-encoded document for a key, or nil, nil when the key
// does not exist.
func (h *HashStore) Get(key string) ([]byte, error) {
	snap := h.kv.NewSnapshot()
	defer snap.Discard()
	encoded, err := snap.Get(h.documentKey(key))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting key %s: %w", key, err)
	}
	return encoded, nil
}

// GetVersion returns the read-repair version of the write that last set a key,
// or 0 when the key was written without one or is absent.
func (h *HashStore) GetVersion(key string) (uint64, error) {
	snap := h.kv.NewSnapshot()
	defer snap.Discard()
	encoded, err := snap.Get(versionKey(h.slot, h.databaseID, h.id, []byte(key)))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("getting version for key %s: %w", key, err)
	}
	return versionFromBytes(encoded), nil
}

// ClearVersion removes a key's read-repair version, so GetVersion reports 0.
// Tests use it to manufacture a stale holder; it is count-neutral and does not
// move the bucket's applied_seq.
func (h *HashStore) ClearVersion(key string) error {
	batch := h.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(versionKey(h.slot, h.databaseID, h.id, []byte(key))); err != nil {
		return fmt.Errorf("clearing version for key %s: %w", key, err)
	}
	if err := batch.Commit(false); err != nil {
		return fmt.Errorf("clearing version for key %s: %w", key, err)
	}
	return nil
}

// Len returns the bucket's entry count from its state document: persisted in
// the batch of every write, so it is exact after a restart without a scan.
func (h *HashStore) Len() (int64, error) {
	snap := h.kv.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, h.slot, h.databaseID, h.id)
	if err != nil {
		return 0, fmt.Errorf("reading the entry count: %w", err)
	}
	return st.Entries, nil
}

// Recount counts the documents by walking the bucket's range. It is the
// audit and repair path, never the open path (spec §3.2), and it changes
// nothing: the count Len reports is the state document's.
func (h *HashStore) Recount() (int64, error) {
	if hook := HashRecountHook.Load(); hook != nil {
		(*hook)()
	}
	return h.DocCount()
}

// DocCount reports how many documents this store holds, walked from storage
// rather than read from the state document, because a dump's manifest must
// not trust a counter kept for something else.
func (h *HashStore) DocCount() (int64, error) {
	var n int64
	err := h.IterateDocuments(func(_, _ []byte) error {
		n++
		return nil
	})
	return n, err
}

// IterateDocuments walks every document in one snapshot and invokes fn with
// the user key and the stored bytes, exactly as Put wrote them — never
// unmarshalled and re-marshalled, which would reorder BSON fields and lose the
// byte-for-byte match a dump comparison depends on. Versions live in their own
// namespace and are never reached.
func (h *HashStore) IterateDocuments(fn func(key, value []byte) error) error {
	snap := h.kv.NewSnapshot()
	defer snap.Discard()
	bounds := h.documentRange()
	it := snap.NewIterator(bounds)
	defer it.Close()
	for it.Seek(nil); it.Valid(); it.Next() {
		key := append([]byte(nil), it.Key()[len(bounds.Lower):]...)
		value, err := it.Value()
		if err != nil {
			return fmt.Errorf("reading value for key %q: %w", key, err)
		}
		if err := fn(key, value); err != nil {
			return fmt.Errorf("iterating key %q: %w", key, err)
		}
	}
	if err := it.Err(); err != nil {
		return fmt.Errorf("walking the documents: %w", err)
	}
	return nil
}

// Keys returns every user key in the store.
func (h *HashStore) Keys() ([]string, error) {
	var keys []string
	err := h.IterateDocuments(func(key, _ []byte) error {
		keys = append(keys, string(key))
		return nil
	})
	return keys, err
}

// DeleteAtPosition removes the pos-th document in key order (0-indexed) by
// walking pos documents — O(pos) (spec §3.3). It carries no WAL sequence: no
// server path calls it.
func (h *HashStore) DeleteAtPosition(pos int) error {
	callReclaimVerbHook("hash DeleteAtPosition")
	errFound := errors.New("found")
	var victim string
	walked := 0
	err := h.IterateDocuments(func(key, _ []byte) error {
		if walked == pos {
			victim = string(key)
			return errFound
		}
		walked++
		return nil
	})
	if err == nil || pos < 0 {
		return fmt.Errorf("position %d out of range [0, %d)", pos, walked)
	}
	if !errors.Is(err, errFound) {
		return err
	}
	return h.Delete(ApplyInfo{}, victim)
}
