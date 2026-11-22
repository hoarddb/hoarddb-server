package store

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/storage"
)

// The unique constraint (GAP-14 spec §4, build doc Step 5). unique is an
// obligation the write path discharges inside Apply's operation, before the
// batch commits, against the snapshot Apply opened — which no other writer of
// the bucket can change underneath it, because every writer holds the bucket
// mutex from before that snapshot until after the commit (build doc RD-2,
// C-18). It is not an index type: a unique index's entries are laid out like
// any other's, with the document key in the key, so a duplicate that ever
// slipped past the check would be two visible entries, never one silent
// overwrite (RD-1, S-2).

// ErrDuplicateKey is under every refusal of a write that would give a unique
// index a second document with the same key.
var ErrDuplicateKey = errors.New("duplicate value for a unique index")

// DuplicateKeyError is the refusal (spec §4.6). It is an operation refusal,
// never a store failure: Apply returns it unmarked, so a refused sequence is
// settled rather than owed (build doc §4 F-2(c)). Value is rendered from the
// decoded document, never from key bytes; it reaches a response, never a log
// line (RD-10).
type DuplicateKeyError struct {
	// Bucket is filled in by the server, which knows the bucket's name.
	Bucket string
	Index  string
	Fields []string
	Value  string
	Holder string
	// Document and Position name the refused document and its 1-based place
	// in its batch; Position is 0 for a write of one document.
	Document string
	Position int
	// HolderPosition is the holder's place in the same batch, 0 when the
	// holder was already stored.
	HolderPosition int
}

func (e *DuplicateKeyError) Error() string {
	where := ""
	if e.Bucket != "" {
		where = fmt.Sprintf(" on bucket %q", e.Bucket)
	}
	holder := fmt.Sprintf("%q", e.Holder)
	if e.HolderPosition > 0 {
		holder = fmt.Sprintf("%q (document %d of the same batch)", e.Holder, e.HolderPosition)
	}
	message := fmt.Sprintf("Duplicate value for unique index %q%s: %s is already used by document %s. "+
		"Change the value, update that document instead, or delete it first.", e.Index, where, e.Value, holder)
	if e.Position > 0 {
		message += fmt.Sprintf(" Document %q, document %d of the batch, was refused, so nothing from the batch was written: "+
			"re-send the batch without it.", e.Document, e.Position)
	}
	return message
}

// Unwrap makes the refusal match ErrDuplicateKey.
func (e *DuplicateKeyError) Unwrap() error { return ErrDuplicateKey }

// UniqueClaims is one operation's view of what its own earlier documents did
// to the unique indexes: Apply's snapshot does not see the batch's own writes,
// so two documents of one batch carrying one new value would both pass a
// snapshot check (build doc §4 F-5). Documents are checked in frame order: a
// key claimed by an earlier document of the batch is taken, and a key an
// earlier document moved off is free, though the snapshot still shows it
// held. The zero value is empty and ready; nil is a write of one document.
type UniqueClaims struct {
	claimed   map[string]claim
	released  map[string]bool
	positions map[string]int
}

// claim is the batch document holding a key.
type claim struct {
	document string
	position int
}

// At records the 1-based frame position of document, which a refusal names.
func (c *UniqueClaims) At(document string, position int) {
	if c.positions == nil {
		c.positions = make(map[string]int)
	}
	c.positions[document] = position
}

func claimKey(indexID uint16, encoded []byte) string {
	return string([]byte{byte(indexID >> 8), byte(indexID)}) + string(encoded)
}

func releaseKey(indexID uint16, encoded []byte, document string) string {
	return claimKey(indexID, encoded) + "\x00" + document
}

// CheckUnique refuses a document write that would give a unique index a
// second holder of one key. dbID is the key the document is stored under,
// previousDoc the version it replaces (nil for a new key) and newDoc the
// document as it will be stored; claims is the operation's batch-local table,
// nil for a write of one document. An index whose key does not change is not
// checked: the document already holds it, and refusing it would make every
// update of a holder its own duplicate. Each check reads at most two entries
// of one key from snap.
func (im *IndexManager) CheckUnique(snap storage.KVSnapshot, claims *UniqueClaims, dbID []byte, previousDoc, newDoc map[string]interface{}) error {
	if err := im.writable(); err != nil {
		return err
	}
	document := string(dbID)
	for _, spec := range im.specs {
		if !spec.Unique || spec.State == IndexUnreadable {
			continue
		}
		encoded, indexed, err := EntryKey(spec, newDoc)
		if err != nil {
			return err
		}
		previous, previouslyIndexed := previousKey(spec, previousDoc)
		if !indexed || (previouslyIndexed && bytes.Equal(previous, encoded)) {
			continue
		}
		refusal := &DuplicateKeyError{Index: spec.Name, Fields: spec.FieldNames(), Value: RenderIndexValue(spec, newDoc), Document: document}
		if claims != nil {
			refusal.Position = claims.positions[document]
			if earlier, taken := claims.claimed[claimKey(spec.ID, encoded)]; taken {
				refusal.Holder, refusal.HolderPosition = earlier.document, earlier.position
				return refusal
			}
		}
		holder, held, err := im.indexes[spec.ID].Holder(snap, encoded, dbID)
		if err != nil {
			return fmt.Errorf("checking unique index %s: %w", spec.Name, err)
		}
		if held && (claims == nil || !claims.released[releaseKey(spec.ID, encoded, string(holder))]) {
			refusal.Holder = string(holder)
			return refusal
		}
		if claims != nil {
			claims.record(spec.ID, encoded, previous, previouslyIndexed, document)
		}
	}
	return nil
}

// record notes that document took encoded, and released previous.
func (c *UniqueClaims) record(indexID uint16, encoded, previous []byte, previouslyIndexed bool, document string) {
	if c.claimed == nil {
		c.claimed = make(map[string]claim)
		c.released = make(map[string]bool)
	}
	c.claimed[claimKey(indexID, encoded)] = claim{document: document, position: c.positions[document]}
	if previouslyIndexed {
		c.released[releaseKey(indexID, previous, document)] = true
	}
}

// previousKey is previousDoc's key in spec's index, if it had one.
func previousKey(spec IndexSpec, previousDoc map[string]interface{}) ([]byte, bool) {
	if previousDoc == nil {
		return nil, false
	}
	previous, indexed, err := EntryKey(spec, previousDoc)
	return previous, err == nil && indexed
}

// Holder returns the document key of an entry under encodedKey that is not
// docID, reading at most two entries from snap: the entries of one key are
// ordered by document key, so if the first is docID's own the second is the
// only other that needs reading.
func (s *SecondaryIndex) Holder(snap storage.KVSnapshot, encodedKey, docID []byte) ([]byte, bool, error) {
	lower := s.makePrefix(encodedKey)
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: prefixEnd(lower)})
	defer it.Close()
	read := 0
	for it.Seek(nil); it.Valid() && read < 2; it.Next() {
		read++
		other := it.Key()[len(lower):]
		if !bytes.Equal(other, docID) {
			return append([]byte(nil), other...), true, nil
		}
	}
	if err := it.Err(); err != nil {
		return nil, false, fmt.Errorf("reading the holders of an index value: %w", err)
	}
	return nil, false, nil
}

// RenderIndexValue renders a document's value in an index the way a developer
// wrote it: `Email = "dalek@example.com"`, `Email = null`, and for a compound
// index `(CustomerID, CreatedAt) = ("c1", 2026-09-29T10:00:00Z)`.
func RenderIndexValue(spec IndexSpec, doc map[string]interface{}) string {
	rendered := make([]string, 0, len(spec.Fields))
	for _, part := range spec.Fields {
		rendered = append(rendered, renderValue(doc[part.Field]))
	}
	if len(spec.Fields) == 1 {
		return spec.Fields[0].Field + " = " + rendered[0]
	}
	return "(" + strings.Join(spec.FieldNames(), ", ") + ") = (" + strings.Join(rendered, ", ") + ")"
}

func renderValue(value interface{}) string {
	switch typed := value.(type) {
	case nil, primitive.Null:
		return "null"
	case string:
		return fmt.Sprintf("%q", typed)
	case primitive.DateTime:
		return typed.Time().UTC().Format(time.RFC3339Nano)
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	case primitive.Binary:
		return fmt.Sprintf("binary(%d bytes)", len(typed.Data))
	case []byte:
		return fmt.Sprintf("binary(%d bytes)", len(typed))
	default:
		return fmt.Sprintf("%v", typed)
	}
}

// FirstDuplicate walks the index in key order and returns the document keys
// of the first two adjacent entries whose encoded keys are equal — two
// documents holding one value of a unique index. arity is the index's field
// count. It is O(entries of this index), in one snapshot; it is how a range
// that arrived without being checked (a migration, a rebuild) is verified.
func (s *SecondaryIndex) FirstDuplicate(arity int) (first, second []byte, found bool, err error) {
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(s.EntryRange())
	defer it.Close()
	var previousKey, previousDoc []byte
	for it.Seek(nil); it.Valid(); it.Next() {
		entry := it.Key()[len(s.prefix):]
		keyLength := 0
		for component := 0; component < arity; component++ {
			length, err := componentLen(entry[keyLength:])
			if err != nil {
				return nil, nil, false, &storage.KeyError{Operation: "reading an index entry", Key: bytes.Clone(it.Key()), Cause: err}
			}
			keyLength += length
		}
		encodedKey, document := entry[:keyLength], entry[keyLength:]
		if previousKey != nil && bytes.Equal(previousKey, encodedKey) {
			return append([]byte(nil), previousDoc...), append([]byte(nil), document...), true, nil
		}
		previousKey = append(previousKey[:0], encodedKey...)
		previousDoc = append(previousDoc[:0], document...)
	}
	if err := it.Err(); err != nil {
		return nil, nil, false, fmt.Errorf("walking the index entries: %w", err)
	}
	return nil, nil, false, nil
}

// ConflictedIndexError refuses a search on a unique index whose documents
// hold one value twice: it cannot say which of them the value belongs to, and
// nothing is chosen for the operator. Value is rendered from a holder's
// document, for a caller already authorised on the bucket; it never reaches a
// log line (build doc RD-10).
type ConflictedIndexError struct {
	Index   string
	Holders []string
	Value   string
}

func (e *ConflictedIndexError) Error() string {
	if len(e.Holders) < 2 {
		return fmt.Sprintf("unique index %s is conflicted: two of its documents held one value when it was last checked, so search "+
			"will not use it. If the data is fixed, restart the node to rebuild the index; find does not use an index and answers now", e.Index)
	}
	return fmt.Sprintf("unique index %s is conflicted: documents %q and %q both hold %s, so search will not use it. "+
		"Change or delete one of the two, then restart the node to rebuild the index; find does not use an index and answers now",
		e.Index, e.Holders[0], e.Holders[1], e.Value)
}

// conflictedIndexError describes a conflicted index from its entries, as of
// the manager's store.
func (im *IndexManager) conflictedIndexError(spec IndexSpec) error {
	first, second, found, err := im.indexes[spec.ID].FirstDuplicate(len(spec.Fields))
	if err != nil {
		return fmt.Errorf("reading conflicted index %s: %w", spec.Name, err)
	}
	refusal := &ConflictedIndexError{Index: spec.Name}
	if !found {
		return refusal
	}
	refusal.Holders = []string{string(first), string(second)}
	snap := im.kv.NewSnapshot()
	defer snap.Discard()
	stored, err := snap.Get(storage.Key(im.slot, im.databaseID, im.id, storage.NSDocuments, first))
	if err != nil {
		return refusal
	}
	// Read back from this store: only the fields the index names are read. A
	// holder whose top level does not walk is named without its value.
	document, err := bson.TrustStored(stored)
	if err != nil {
		return refusal
	}
	if fields, err := im.IndexedFields(document); err == nil {
		refusal.Value = RenderIndexValue(spec, fields)
	}
	return refusal
}
