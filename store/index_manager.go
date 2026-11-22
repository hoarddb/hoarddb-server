package store

import (
	"errors"
	"fmt"
	"math"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/storage"
)

// IndexManager maintains a bucket's secondary indexes on the node's shared
// store. It opens nothing and holds no lock of its own: its writes go into the
// batch of the document write they belong to, under the bucket mutex that
// write holds, so a document, its version and its index entries commit
// together (shared-storage spec §3.6, T-11); its reads each take a snapshot.
type IndexManager struct {
	kv         storage.KV
	slot       uint16
	databaseID uint64
	id         uint64
	specs      []IndexSpec
	indexes    map[uint16]*SecondaryIndex // keyed by IndexSpec.ID
}

// NewIndexManager returns the index manager of bucket id in slot, over the
// bucket's declared indexes. Each index's entries carry its spec's ID, which
// for an index declared before this build is its position in the catalog
// list — so their bytes are unchanged (build doc RD-4).
func NewIndexManager(kv storage.KV, slot uint16, databaseID, id uint64, specs []IndexSpec) (*IndexManager, error) {
	if len(specs) > math.MaxUint16+1 {
		return nil, fmt.Errorf("a bucket can declare at most %d indexes, not %d", math.MaxUint16+1, len(specs))
	}
	im := &IndexManager{
		kv:         kv,
		slot:       slot,
		databaseID: databaseID,
		id:         id,
		specs:      append([]IndexSpec(nil), specs...),
		indexes:    make(map[uint16]*SecondaryIndex, len(specs)),
	}
	for _, spec := range specs {
		if len(spec.Fields) == 0 && spec.State != IndexUnreadable {
			return nil, fmt.Errorf("index %q of bucket %d names no field", spec.Name, id)
		}
		if _, taken := im.indexes[spec.ID]; taken {
			return nil, fmt.Errorf("two indexes of bucket %d share id %d; the catalog record is damaged", id, spec.ID)
		}
		im.indexes[spec.ID] = NewSecondaryIndex(kv, slot, databaseID, id, spec.ID)
	}
	return im, nil
}

// OnInsert adds a stored document's index entries to b. dbID is the key the
// document is stored under, and doc the document as stored, decoded once by
// the caller for every index. A value no index can encode refuses the write —
// inside its batch, so nothing of it is written (spec §3.8 rule 3).
func (im *IndexManager) OnInsert(b storage.KVBatch, dbID []byte, doc map[string]interface{}) error {
	return im.eachEntry(dbID, doc, func(idx *SecondaryIndex, encoded []byte) error {
		return idx.Add(b, encoded, dbID)
	})
}

// OnDelete adds the removal of a document's index entries to b.
func (im *IndexManager) OnDelete(b storage.KVBatch, dbID []byte, doc map[string]interface{}) error {
	return im.eachEntry(dbID, doc, func(idx *SecondaryIndex, encoded []byte) error {
		return idx.Remove(b, encoded, dbID)
	})
}

// OnUpdate adds to b the removal of a document's old entries and the entries
// of its new version.
func (im *IndexManager) OnUpdate(b storage.KVBatch, dbID []byte, oldDoc, newDoc map[string]interface{}) error {
	if err := im.OnDelete(b, dbID, oldDoc); err != nil {
		return fmt.Errorf("removing old index entries: %w", err)
	}
	if err := im.OnInsert(b, dbID, newDoc); err != nil {
		return fmt.Errorf("adding new index entries: %w", err)
	}
	return nil
}

// eachEntry runs fn for each index doc has an entry in, with the entry's
// encoded key. An index this binary cannot read is not maintained: its rules
// are a newer binary's, which rebuilds it.
func (im *IndexManager) eachEntry(dbID []byte, doc map[string]interface{}, fn func(*SecondaryIndex, []byte) error) error {
	if err := im.writable(); err != nil {
		return err
	}
	for _, spec := range im.specs {
		if spec.State == IndexUnreadable {
			continue
		}
		encoded, indexed, err := EntryKey(spec, doc)
		if err != nil {
			return err
		}
		if !indexed {
			continue
		}
		if err := fn(im.indexes[spec.ID], encoded); err != nil {
			var keyTooLarge *storage.KeyTooLargeError
			if errors.As(err, &keyTooLarge) {
				return &IndexEntryTooLargeError{Index: spec.Name, Fields: indexFieldNames(spec), Document: string(dbID), Cause: keyTooLarge}
			}
			return fmt.Errorf("index %s of document %q: %w", spec.Name, dbID, err)
		}
	}
	return nil
}

// IndexEntryTooLargeError is a document refused because an index entry's key,
// which holds the indexed values, is longer than the store holds. It names the
// index, its fields and the document so the layer that answers a client can
// say what to shorten; Cause carries the sizes and the key for the log.
type IndexEntryTooLargeError struct {
	Index    string
	Fields   []string
	Document string
	Cause    *storage.KeyTooLargeError
}

// Error names the index and the document, never the key.
func (e *IndexEntryTooLargeError) Error() string {
	return fmt.Sprintf("index %s of document %q: the entry for fields %v is too large: %v", e.Index, e.Document, e.Fields, e.Cause)
}

// Unwrap exposes storage.ErrKeyTooLarge to errors.Is and the cause to errors.As.
func (e *IndexEntryTooLargeError) Unwrap() error { return e.Cause }

func indexFieldNames(spec IndexSpec) []string {
	names := make([]string, len(spec.Fields))
	for i, part := range spec.Fields {
		names[i] = part.Field
	}
	return names
}

// EntryKey is the encoded key doc has in the index spec describes:
// EncodeKey over the spec's fields in order. A field that is absent or holds
// BSON null is null (0x10) under this build's rules (spec §5.5, build doc
// RD-3), so every document has an entry. An index declared before this build
// keeps the old rule — no entry — until the upgrade rebuild rewrites it: null
// entries in half of it would be a mixture nothing could tell apart (§4 F-9).
// indexed is false when doc has no entry.
func EntryKey(spec IndexSpec, doc map[string]interface{}) (encoded []byte, indexed bool, err error) {
	for _, part := range spec.Fields {
		value, present := doc[part.Field]
		if _, isNull := value.(primitive.Null); !present || value == nil || isNull {
			if spec.Version < IndexSpecVersion {
				return nil, false, nil
			}
			value = nil
		}
		component, err := EncodeComponent(value)
		if err != nil {
			return nil, false, &unindexableValueError{field: part.Field, index: spec.Name, value: value, cause: err}
		}
		encoded = append(encoded, component...)
	}
	return encoded, true, nil
}

// unindexableValueError is spec §3.8 rule 3's refusal, composed by the write
// path because the encoder sees only the value, not the field.
type unindexableValueError struct {
	field string
	index string
	value interface{}
	cause error
}

func (e *unindexableValueError) Error() string {
	if errors.Is(e.cause, ErrNonFiniteNumber) {
		return fmt.Sprintf("Field %q holds %v, and NaN and infinite numbers cannot be a btree key or an indexed field (index %s). "+
			"Store a finite number, or leave the field out of the document.", e.field, e.value, e.index)
	}
	return fmt.Sprintf("Field %q holds %s, which cannot be a btree key or an indexed field in this release (index %s; "+
		"supported: %s). Index a string form of the value, or use a hash bucket, "+
		"which accepts any key as bytes.", e.field, bsonTypeName(e.value), e.index, supportedKeyTypes)
}

func (e *unindexableValueError) Unwrap() error { return e.cause }

// bsonTypeName names a decoded value's BSON type the way a developer knows it.
func bsonTypeName(value interface{}) string {
	switch value.(type) {
	case primitive.Decimal128:
		return "a decimal128"
	case bson.A, []interface{}:
		return "an array"
	case bson.M, bson.D, map[string]interface{}:
		return "a sub-document"
	default:
		return fmt.Sprintf("a %T", value)
	}
}

// UnreadableIndexError refuses what an index written by a newer binary would
// be needed for: a search it would serve, or — for a unique index — any write
// to its bucket, because a write that skips a declared constraint is a
// downgrade nothing can repair afterwards (spec §3.7).
type UnreadableIndexError struct {
	Index   string
	Version uint8
	Write   bool
}

func (e *UnreadableIndexError) Error() string {
	if e.Write {
		return fmt.Sprintf("unique index %s was written by a newer version of hoardDB (descriptor version %d; this binary reads up to %d), "+
			"so this binary cannot enforce it and refuses writes to its bucket. Run the newer version of hoardDB again, "+
			"or dump the bucket with it and restore it here without that index", e.Index, e.Version, IndexSpecVersion)
	}
	return fmt.Sprintf("index %s was written by a newer version of hoardDB (descriptor version %d; this binary reads up to %d), "+
		"so search will not use it. Run the newer version of hoardDB, which keeps it current, or use find, which does not use an index",
		e.Index, e.Version, IndexSpecVersion)
}

// writable refuses a write to a bucket holding a unique index this binary
// cannot read.
func (im *IndexManager) writable() error {
	for _, spec := range im.specs {
		if spec.State == IndexUnreadable && spec.Unique {
			return &UnreadableIndexError{Index: spec.Name, Version: spec.Version, Write: true}
		}
	}
	return nil
}

// IndexNotReadyError refuses a search on an index that is not ready: one being
// rebuilt answers from half its entries.
type IndexNotReadyError struct {
	Index string
	State IndexState
}

func (e *IndexNotReadyError) Error() string {
	return fmt.Sprintf("index %s cannot serve search while it is %s: the node is rebuilding it from the documents after an upgrade. "+
		"Retry once the node has finished starting, or use find, which does not use an index", e.Index, e.State)
}

// ListIndexes returns the bucket's declared indexes.
func (im *IndexManager) ListIndexes() []IndexSpec {
	return append([]IndexSpec(nil), im.specs...)
}
