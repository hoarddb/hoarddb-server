package store

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
)

// BTreeEntry represents a key-value pair returned from B+Tree range queries.
// Key is the user key's display form: a string's own bytes, a number or a
// date written out, never the stored encoding.
//
// Text is the same display form as a string, set only for a number, date,
// bool or null key: text the store formatted itself, so it is valid UTF-8 and
// a caller can put it on the wire without copying or scanning it again
// (BUG-0617). It shares Key's memory, so neither may be modified.
type BTreeEntry struct {
	Key   []byte
	Text  string
	Value []byte
}

// BTreeStore is a btree bucket on the node's shared store: its documents are
// [s][id][0x01][EncodeComponent(user key)], so the store's own key order is
// the btree's order, typed and order-preserving (shared-storage spec §3.2,
// §3.8). Its versions are [s][id][0x02][encoded key] and its count lives in
// the bucket's state document. It opens nothing.
//
// Every mutation goes through Apply under the bucket mutex the caller holds.
// owed is the bucket's failed-apply carry, which the caller owns: it outlives
// the engine (Carry).
type BTreeStore struct {
	owed       *OwedApplies
	kv         storage.KV
	slot       uint16
	databaseID uint64
	id         uint64
}

// NewBTreeStore returns the btree store of bucket id in slot, which must be
// the bucket's recorded slot, carrying its failed applies in owed.
func NewBTreeStore(kv storage.KV, slot uint16, databaseID, id uint64, owed *OwedApplies) *BTreeStore {
	return &BTreeStore{owed: owed, kv: kv, slot: slot, databaseID: databaseID, id: id}
}

// Carry is the bucket's failed-apply carry the store was built with.
func (b *BTreeStore) Carry() *OwedApplies { return b.owed }

// documentPrefix is [s][id][0x01], under which every document key lies.
func (b *BTreeStore) documentPrefix() []byte {
	return storage.Key(b.slot, b.databaseID, b.id, storage.NSDocuments)
}

// encodeUserKey is spec §3.8's encoding of a btree key, refused at write
// time with rule 3's message.
func encodeUserKey(key interface{}) ([]byte, error) {
	encoded, err := EncodeComponent(key)
	if err != nil {
		return nil, &unindexableValueError{field: "_id_", value: key, cause: err}
	}
	return encoded, nil
}

func (b *BTreeStore) apply(info ApplyInfo, op func(storage.KVSnapshot, *BucketState, storage.KVBatch) error) error {
	_, err := Apply(ApplyRequest{KV: b.kv, Slot: b.slot, DatabaseID: b.databaseID, ID: b.id, Keyed: true, Info: info, Owed: b.owed, Op: op})
	return err
}

// btreeDocument is one document putDocument writes: its user key, encoded,
// its encoded form, its version and the insert's existence check.
type btreeDocument struct {
	key        interface{}
	encodedKey []byte
	encoded    []byte
	version    uint64
	check      insertCheck
}

// putDocument adds one document write to batch and updates st.
func (b *BTreeStore) putDocument(snap storage.KVSnapshot, st *BucketState, batch storage.KVBatch, doc btreeDocument) error {
	if doc.check.refusedUnread() {
		return &DocumentExistsError{}
	}
	docKey := append(b.documentPrefix(), doc.encodedKey...)
	previous, err := snap.Get(docKey)
	if err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
		return fmt.Errorf("reading a document: %w", err)
	}
	stored := err == nil
	// Before anything is added to the batch: the read is the one an overwrite
	// needs anyway.
	if doc.check.refuses(stored) {
		return &DocumentExistsError{Key: doc.key}
	}
	if stored {
		st.LogicalBytes += int64(len(doc.encoded) - len(previous))
	} else {
		st.Entries++
		st.LogicalBytes += int64(len(doc.encodedKey) + len(doc.encoded))
	}
	if err := batch.Set(docKey, doc.encoded); err != nil {
		return fmt.Errorf("putting a document: %w", err)
	}
	if doc.version > 0 {
		if err := batch.Set(versionKey(b.slot, b.databaseID, b.id, doc.encodedKey), versionBytes(doc.version)); err != nil {
			return fmt.Errorf("putting the document's version: %w", err)
		}
	}
	return nil
}

// Put stores a document under key, an upsert that does not grow the count
// when the key exists. version, when non-zero, is stamped in the same batch;
// zero leaves any earlier version in place.
func (b *BTreeStore) Put(info ApplyInfo, key interface{}, value interface{}, version uint64) error {
	_, err := b.PutBelow(info, PutRequest{Key: key, Value: value, Version: version})
	return err
}

// PutBelow is Put bounded by a version, as HashStore.PutBelow is: the
// document is written only if the key's stored version is below req.Below,
// and spared reports a key left whole. A bound of 0 is no bound. With
// req.RefuseExisting a held key is refused with *DocumentExistsError.
func (b *BTreeStore) PutBelow(info ApplyInfo, req PutRequest) (spared bool, err error) {
	encodedKey, err := encodeUserKey(req.Key)
	if err != nil {
		return false, err
	}
	encoded, err := bson.Marshal(req.Value)
	if err != nil {
		return false, fmt.Errorf("marshaling value: %w", err)
	}
	doc := btreeDocument{key: req.Key, encodedKey: encodedKey, encoded: encoded, version: req.Version, check: newInsertCheck(info, req.RefuseExisting)}
	if err := b.apply(info, func(snap storage.KVSnapshot, st *BucketState, batch storage.KVBatch) error {
		spared = false
		if req.Below != 0 {
			newer, err := storedAtOrAbove(snap, versionKey(b.slot, b.databaseID, b.id, encodedKey), req.Below)
			if err != nil || newer {
				spared = newer
				return err
			}
		}
		return b.putDocument(snap, st, batch, doc)
	}); err != nil {
		return false, fmt.Errorf("putting key: %w", err)
	}
	return spared, nil
}

// PutBatch stores every document of req.Items under its "_id_" in one batch
// with the bucket's state, and returns the ids in order. With
// req.RefuseExisting every document is checked before the refusal returns, so
// *DocumentExistsError names every position whose key is held.
func (b *BTreeStore) PutBatch(info ApplyInfo, req PutBatchRequest) ([]string, error) {
	writes := make([]btreeDocument, 0, len(req.Items))
	ids := make([]string, 0, len(req.Items))
	check := newInsertCheck(info, req.RefuseExisting)
	for _, doc := range req.Items {
		id, _ := doc["_id_"].(string)
		encodedKey, err := encodeUserKey(id)
		if err != nil {
			return nil, err
		}
		encoded, err := bson.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("marshaling document %q: %w", id, err)
		}
		writes = append(writes, btreeDocument{key: id, encodedKey: encodedKey, encoded: encoded, version: req.Version, check: check})
		ids = append(ids, id)
	}
	err := b.apply(info, func(snap storage.KVSnapshot, st *BucketState, batch storage.KVBatch) error {
		// A key repeated in the batch counts once: the batch's own earlier
		// set is not in the snapshot, so it is tracked here.
		written := make(map[string][]byte, len(writes))
		var exists *DocumentExistsError
		for index, w := range writes {
			// Once one key is refused nothing more is put: the rest are only
			// read, to name every held position.
			if exists != nil {
				if _, repeated := written[string(w.encodedKey)]; repeated {
					continue
				}
				held, err := b.holds(snap, w.encodedKey)
				if err != nil {
					return err
				}
				if held {
					exists.Positions = append(exists.Positions, index+1)
				}
				continue
			}
			if earlier, repeated := written[string(w.encodedKey)]; repeated {
				st.LogicalBytes += int64(len(w.encoded) - len(earlier))
				if err := batch.Set(append(b.documentPrefix(), w.encodedKey...), w.encoded); err != nil {
					return fmt.Errorf("putting a document: %w", err)
				}
				written[string(w.encodedKey)] = w.encoded
				continue
			}
			err := b.putDocument(snap, st, batch, w)
			if errors.As(err, &exists) && exists.Key != nil {
				exists.Positions = append(exists.Positions, index+1)
				continue
			}
			if err != nil {
				return err
			}
			written[string(w.encodedKey)] = w.encoded
		}
		if exists != nil {
			return exists
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("batch put: %w", err)
	}
	return ids, nil
}

// holds reports whether the bucket stores a document under encodedKey in snap.
func (b *BTreeStore) holds(snap storage.KVSnapshot, encodedKey []byte) (bool, error) {
	_, err := snap.Get(append(b.documentPrefix(), encodedKey...))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reading a document: %w", err)
	}
	return true, nil
}

// Delete removes a key, its version and its field stores. Deleting a key that
// is not there succeeds and changes no count.
func (b *BTreeStore) Delete(info ApplyInfo, key interface{}) error {
	_, err := b.DeleteDocument(info, key, nil)
	return err
}

// DeleteDocument is Delete for a bucket with its field layout: a blobStore
// field's elements take their metadata with them in the same batch, and
// their payload paths are returned for the caller to unlink after the commit.
func (b *BTreeStore) DeleteDocument(info ApplyInfo, key interface{}, layout FieldLayout) ([]string, error) {
	_, payloads, err := b.DeleteDocumentBelow(info, key, layout, 0)
	return payloads, err
}

// DeleteDocumentBelow is DeleteDocument bounded by a version, as
// HashStore.DeleteDocumentBelow is: the document goes only if its stored
// version is below below, and spared reports one left whole. A bound of 0 is
// no bound.
func (b *BTreeStore) DeleteDocumentBelow(info ApplyInfo, key interface{}, layout FieldLayout, below uint64) (spared bool, payloads []string, err error) {
	encodedKey, err := encodeUserKey(key)
	if err != nil {
		return false, nil, err
	}
	return b.deleteEncoded(info, encodedKey, layout, below)
}

// deleteEncoded deletes a document by its encoded key, with its version and
// its field stores; under a non-zero below it deletes nothing unless the
// stored document's version is below it, and reports spared.
func (b *BTreeStore) deleteEncoded(info ApplyInfo, encodedKey []byte, layout FieldLayout, below uint64) (spared bool, payloads []string, err error) {
	err = b.apply(info, func(snap storage.KVSnapshot, st *BucketState, batch storage.KVBatch) error {
		spared, payloads = false, nil
		docKey := append(b.documentPrefix(), encodedKey...)
		previous, err := snap.Get(docKey)
		if below != 0 {
			if err != nil {
				if errors.Is(err, storage.ErrKeyNotFound) {
					return nil
				}
				return fmt.Errorf("reading a document: %w", err)
			}
			if spared, err = storedAtOrAbove(snap, versionKey(b.slot, b.databaseID, b.id, encodedKey), below); err != nil || spared {
				return err
			}
		}
		switch {
		case errors.Is(err, storage.ErrKeyNotFound):
		case err != nil:
			return fmt.Errorf("reading a document: %w", err)
		default:
			st.Entries--
			st.LogicalBytes -= int64(len(encodedKey) + len(previous))
			if err := batch.Delete(docKey); err != nil {
				return fmt.Errorf("deleting a document: %w", err)
			}
		}
		if err := batch.Delete(versionKey(b.slot, b.databaseID, b.id, encodedKey)); err != nil {
			return fmt.Errorf("deleting the document's version: %w", err)
		}
		// The owner is the encoded key itself: for a string key it is
		// FieldOwnerPrefix's own bytes (nested buckets RD-2).
		owner := storage.Key(b.slot, b.databaseID, b.id, storage.NSFieldStore, encodedKey)
		if payloads, err = deleteFieldStores(FieldWrite{Snap: snap, State: st, Batch: batch}, owner, layout); err != nil {
			return fmt.Errorf("deleting the document's field stores: %w", err)
		}
		return nil
	})
	if err != nil {
		return false, nil, fmt.Errorf("deleting key: %w", err)
	}
	return spared, payloads, nil
}

// Get retrieves the BSON-encoded value for a key, or nil, nil when absent.
func (b *BTreeStore) Get(key interface{}) ([]byte, error) {
	encodedKey, err := encodeUserKey(key)
	if err != nil {
		return nil, err
	}
	snap := b.kv.NewSnapshot()
	defer snap.Discard()
	encoded, err := snap.Get(append(b.documentPrefix(), encodedKey...))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting key: %w", err)
	}
	return encoded, nil
}

// GetVersion returns the read-repair version of the write that last set a key,
// or 0 when it was written without one or is absent.
func (b *BTreeStore) GetVersion(key interface{}) (uint64, error) {
	encodedKey, err := encodeUserKey(key)
	if err != nil {
		return 0, err
	}
	snap := b.kv.NewSnapshot()
	defer snap.Discard()
	encoded, err := snap.Get(versionKey(b.slot, b.databaseID, b.id, encodedKey))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("getting version for key: %w", err)
	}
	return versionFromBytes(encoded), nil
}

// First returns the entry with the lowest key — a forward seek — or an empty
// BTreeEntry for an empty bucket. Over mixed key types it is the lowest type
// tag (spec §3.8 rule 1).
func (b *BTreeStore) First() (BTreeEntry, error) {
	return b.edge(false)
}

// Last returns the entry with the highest key — a reverse seek — or an empty
// BTreeEntry for an empty bucket.
func (b *BTreeStore) Last() (BTreeEntry, error) {
	return b.edge(true)
}

func (b *BTreeStore) edge(reverse bool) (BTreeEntry, error) {
	prefix := b.documentPrefix()
	snap := b.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix), Reverse: reverse})
	defer it.Close()
	it.Seek(nil)
	if !it.Valid() {
		if err := it.Err(); err != nil {
			return BTreeEntry{}, fmt.Errorf("reading the bucket's edge entry: %w", err)
		}
		return BTreeEntry{}, nil
	}
	return b.entryAt(it, prefix)
}

// entryAt reads the iterator's current document as a BTreeEntry.
func (b *BTreeStore) entryAt(it storage.KVIterator, prefix []byte) (BTreeEntry, error) {
	value, err := it.Value()
	if err != nil {
		return BTreeEntry{}, fmt.Errorf("reading a document's value: %w", err)
	}
	display, err := displayKeyOf(it.Key()[len(prefix):])
	if err != nil {
		return BTreeEntry{}, err
	}
	entry := BTreeEntry{Key: display, Value: value}
	if tag := it.Key()[len(prefix)]; tag != tagString && tag != tagBinary {
		// displayKeyOf built display for this call alone and never changes it
		// afterwards, so a string may share its bytes instead of copying them.
		entry.Text = unsafe.String(unsafe.SliceData(display), len(display))
	}
	return entry, nil
}

// maxRangePresize caps the capacity Range reserves from a caller's limit.
const maxRangePresize = 1024

// BTreeRangeHook, when set, runs at the start of every Range call with its
// arguments. The server's tests count the whole-bucket calls,
// Range(nil, nil, 0), to prove a read path walks by callback instead
// (query-pipelining spec T-6). Nil in production. Exported because the
// server's tests set it.
var BTreeRangeHook atomic.Pointer[func(low, high interface{}, limit int)]

// Range returns the entries whose keys lie between low and high, both
// inclusive, in key order, as a bounded walk of the encoded keys. A nil bound
// is unbounded on that side. A positive limit stops the walk at that many
// entries, so a page costs what the page holds rather than what the band
// holds; zero or less returns the whole band. Two bounds of
// different types are refused: order between types is fixed but means
// nothing, and a scan never compares across them (spec §3.8 rule 1).
//
// Range is RangeEach collecting into a slice, so the two cannot disagree on
// which entries a band holds. A caller that does not need the band at once
// walks it with RangeEach instead.
func (b *BTreeStore) Range(low, high interface{}, limit int) ([]BTreeEntry, error) {
	if hook := BTreeRangeHook.Load(); hook != nil {
		(*hook)(low, high, limit)
	}
	// A positive limit bounds the page, so size for it up front instead of
	// regrowing from nil; the cap keeps a client's huge limit over a small
	// band from reserving memory the band will never fill.
	var entries []BTreeEntry
	if limit > 0 {
		entries = make([]BTreeEntry, 0, min(limit, maxRangePresize))
	}
	err := b.RangeEach(low, high, limit, func(key, value []byte) error {
		entries = append(entries, BTreeEntry{Key: key, Value: value})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// RangeEach calls fn with each entry Range would return — the same bounds,
// the same type check, the same order, over one snapshot — without holding
// them: the key's display form and the stored bytes, which fn may keep. It
// stops after limit entries when limit is positive, or at the first error
// from fn, which it returns unwrapped: that error is the caller's own and
// already worded for its client (query-pipelining spec §9 hazard 8).
func (b *BTreeStore) RangeEach(low, high interface{}, limit int, fn func(key, value []byte) error) error {
	return b.RangeEntries(low, high, limit, func(entry BTreeEntry) error { return fn(entry.Key, entry.Value) })
}

// RangeEntries is RangeEach handing fn the whole entry, so a caller can read
// BTreeEntry.Text and skip converting a key the store already formatted.
func (b *BTreeStore) RangeEntries(low, high interface{}, limit int, fn func(entry BTreeEntry) error) error {
	prefix := b.documentPrefix()
	bounds, err := rangeBounds(prefix, low, high)
	if err != nil {
		return err
	}
	snap := b.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(bounds)
	defer it.Close()
	visited := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		entry, err := b.entryAt(it, prefix)
		if err != nil {
			return err
		}
		if err := fn(entry); err != nil {
			return err
		}
		visited++
		if limit > 0 && visited == limit {
			return nil
		}
	}
	if err := it.Err(); err != nil {
		return fmt.Errorf("walking the range: %w", err)
	}
	return nil
}

// rangeBounds turns a range's bounds into the iterator bounds over the
// bucket's documents, refusing bounds of two different types.
func rangeBounds(prefix []byte, low, high interface{}) (storage.IterOptions, error) {
	bounds := storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)}
	var encodedLow, encodedHigh []byte
	if low != nil {
		var err error
		if encodedLow, err = EncodeComponent(low); err != nil {
			return bounds, fmt.Errorf("encoding the low bound: %w", err)
		}
		bounds.Lower = append(append([]byte(nil), prefix...), encodedLow...)
	}
	if high != nil {
		var err error
		if encodedHigh, err = EncodeComponent(high); err != nil {
			return bounds, fmt.Errorf("encoding the high bound: %w", err)
		}
		// The smallest key above the high bound's own: encodings are
		// self-delimiting, so nothing else begins with it.
		bounds.Upper = append(append(append([]byte(nil), prefix...), encodedHigh...), 0x00)
	}
	if encodedLow != nil && encodedHigh != nil && encodedLow[0] != encodedHigh[0] {
		return bounds, fmt.Errorf("a range's bounds must be one type, and %v (%T) and %v (%T) are not: "+
			"keys of different types have no order a range could mean", low, low, high, high)
	}
	return bounds, nil
}

// Len returns the bucket's entry count from its state document.
func (b *BTreeStore) Len() (int64, error) {
	snap := b.kv.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, b.slot, b.databaseID, b.id)
	if err != nil {
		return 0, fmt.Errorf("reading the entry count: %w", err)
	}
	return st.Entries, nil
}

// Recount counts the documents by walking the bucket's range: the audit path,
// never the open path (spec §3.2). It changes nothing.
func (b *BTreeStore) Recount() (int64, error) {
	return b.DocCount()
}

// DocCount reports how many documents this store holds, walked from storage.
// It reads no key and no value: a count only needs to know that the snapshot
// holds a document there, and reading them would copy every key and value of
// the bucket to throw them away.
func (b *BTreeStore) DocCount() (int64, error) {
	prefix := b.documentPrefix()
	snap := b.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	var n int64
	for it.Seek(nil); it.Valid(); it.Next() {
		n++
	}
	if err := it.Err(); err != nil {
		return 0, fmt.Errorf("walking the documents: %w", err)
	}
	return n, nil
}

// IterateDocuments walks every document in key order in one snapshot and
// invokes fn with the key's display form and the stored bytes, exactly as Put
// wrote them. Versions live in their own namespace and are never reached.
func (b *BTreeStore) IterateDocuments(fn func(key, value []byte) error) error {
	prefix := b.documentPrefix()
	snap := b.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	for it.Seek(nil); it.Valid(); it.Next() {
		entry, err := b.entryAt(it, prefix)
		if err != nil {
			return err
		}
		if err := fn(entry.Key, entry.Value); err != nil {
			return fmt.Errorf("iterating key %q: %w", entry.Key, err)
		}
	}
	if err := it.Err(); err != nil {
		return fmt.Errorf("walking the documents: %w", err)
	}
	return nil
}

// DeleteAtPosition removes the pos-th entry in key order (0-indexed) by
// walking pos entries — O(pos) (spec §3.3). It carries no WAL sequence: no
// server path calls it.
func (b *BTreeStore) DeleteAtPosition(pos int) error {
	callReclaimVerbHook("btree DeleteAtPosition")
	prefix := b.documentPrefix()
	snap := b.kv.NewSnapshot()
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	var victim []byte
	walked := 0
	for it.Seek(nil); it.Valid() && pos >= 0; it.Next() {
		if walked == pos {
			victim = append([]byte(nil), it.Key()[len(prefix):]...)
			break
		}
		walked++
	}
	walkErr := it.Err()
	it.Close()
	snap.Discard()
	if walkErr != nil {
		return fmt.Errorf("walking to position %d: %w", pos, walkErr)
	}
	if victim == nil {
		return fmt.Errorf("position %d out of range [0, %d)", pos, walked)
	}
	_, _, err := b.deleteEncoded(ApplyInfo{}, victim, nil, 0)
	return err
}

// formattedKeyCap holds the longest formatted key — a nanosecond RFC 3339
// time is 30 bytes — so appending to it never regrows into a second
// allocation.
const formattedKeyCap = 32

// displayKeyOf renders an encoded key the way a user wrote it: a string's or
// binary's own bytes, a number, bool or date in text.
func displayKeyOf(encoded []byte) ([]byte, error) {
	length, err := componentLen(encoded)
	if err != nil {
		return nil, fmt.Errorf("decoding a btree key: %w", err)
	}
	encoded = encoded[:length]
	switch encoded[0] {
	case tagString, tagBinary:
		raw := encoded[1 : length-2]
		// The common case has no 0x00 in the key, so nothing to unescape:
		// bytes.ReplaceAll always allocates and copies even with zero
		// matches, so skip it and return raw as-is. raw is a
		// subslice of the key Key already copied out for this call, so no one
		// else can see or mutate it through this return.
		if !bytes.Contains(raw, []byte{0x00, 0xFF}) {
			return raw, nil
		}
		return bytes.ReplaceAll(raw, []byte{0x00, 0xFF}, []byte{0x00}), nil
	case tagNull:
		return []byte("null"), nil
	case tagBool:
		return []byte(strconv.FormatBool(encoded[1] == 0x01)), nil
	case tagDateTime:
		millis := int64(binary.BigEndian.Uint64(encoded[1:9]) ^ (1 << 63))
		return time.UnixMilli(millis).UTC().AppendFormat(make([]byte, 0, formattedKeyCap), time.RFC3339Nano), nil
	case tagObjectID:
		// The hex of the twelve raw bytes is the form a user pastes into a
		// query, so the listing shows exactly what they would type.
		display := make([]byte, hex.EncodedLen(len(encoded)-1))
		hex.Encode(display, encoded[1:])
		return display, nil
	default:
		// The Append forms build the text straight into the returned bytes;
		// Format then []byte(...) formatted it to a string and copied it again.
		payload := binary.BigEndian.Uint64(encoded[10:18])
		switch encoded[9] {
		case classSignedInteger:
			return strconv.AppendInt(make([]byte, 0, formattedKeyCap), int64(payload^(1<<63)), 10), nil
		case classUnsignedInteger:
			return strconv.AppendUint(make([]byte, 0, formattedKeyCap), payload, 10), nil
		default:
			return strconv.AppendFloat(make([]byte, 0, formattedKeyCap), floatOfImage(binary.BigEndian.Uint64(encoded[1:9])), 'g', -1, 64), nil
		}
	}
}

// floatOfImage inverts orderedImage.
func floatOfImage(image uint64) float64 {
	if image>>63 == 1 {
		return math.Float64frombits(image &^ (1 << 63))
	}
	return math.Float64frombits(^image)
}
