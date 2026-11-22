package store

import "strings"

// IndexSpec describes one secondary index of a bucket (GAP-14 spec §3.3). It is
// what the catalog records and what the index manager is built from.
//
// Unique is a constraint the write path enforces, not a kind of index: a
// unique index's entries are laid out and read exactly like any other's
// (build doc RD-1), which is why there is no IndexType for it.
type IndexSpec struct {
	// ID is written into every entry of the index. It is stable for the life
	// of the index and never reused within the bucket; for an index declared
	// before this build it is the index's position in the catalog list, which
	// is what its entries already carry (RD-4).
	ID uint16
	// Name defaults to DefaultIndexName(Fields) and may be overridden.
	Name string
	// Fields is at least one field, in declaration order.
	Fields []IndexKeyPart
	// Unique refuses a second document with the same key.
	Unique bool
	// TTL is nil unless this is a TTL index.
	TTL *TTLSpec
	// State is IndexReady for almost every index; see IndexState.
	State IndexState
	// Version is 0 for an index declared before this build — its entries
	// follow the old rules until the upgrade rebuild (RD-6) — and
	// IndexSpecVersion for one this build wrote.
	Version uint8
	// Recorded is the catalog's bytes for an index this binary cannot read
	// (State IndexUnreadable): written back unchanged, never interpreted.
	Recorded []byte
}

// IndexKeyPart is one field of an index key. Desc is always false in 1.0: a
// descending component would only matter to index-assisted sort, which is not
// built, and the declaration parser refuses it.
type IndexKeyPart struct {
	Field string
	Desc  bool
}

// TTLSpec is a TTL index's expiry: a document expires ExpireAfterSeconds
// after the date its field holds. Zero is a legal value.
type TTLSpec struct {
	ExpireAfterSeconds int64
}

// IndexState is an index's lifecycle state. The empty string is ready, so a
// ready index costs its catalog record nothing.
type IndexState string

const (
	// IndexReady serves search.
	IndexReady IndexState = ""
	// IndexBuilding is being rebuilt from the documents; search refuses it.
	IndexBuilding IndexState = "building"
	// IndexConflicted is a unique index whose documents hold a duplicate;
	// search refuses it and nothing is chosen for the operator.
	IndexConflicted IndexState = "conflicted"
	// IndexUnreadable was written by a newer binary; it is held in memory only
	// and never persisted as a state.
	IndexUnreadable IndexState = "unreadable"
)

// IndexSpecVersion is the descriptor version this build writes.
const IndexSpecVersion uint8 = 2

// DefaultIndexName is an index's name when the declaration gives none: each
// field followed by "_1", joined by "_" — "Email_1",
// "CustomerID_1_CreatedAt_1" (spec §5.1).
func DefaultIndexName(fields []IndexKeyPart) string {
	parts := make([]string, 0, len(fields))
	for _, part := range fields {
		parts = append(parts, part.Field+"_1")
	}
	return strings.Join(parts, "_")
}

// FieldNames returns the index's fields in declaration order.
func (spec IndexSpec) FieldNames() []string {
	names := make([]string, 0, len(spec.Fields))
	for _, part := range spec.Fields {
		names = append(names, part.Field)
	}
	return names
}
