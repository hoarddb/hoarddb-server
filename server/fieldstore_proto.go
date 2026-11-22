package server

import "github.com/hoarddb/hoarddb-bson/bson"

// Wire types of the field-store verbs (nested buckets, GAP-22; opcodes
// 0xD0–0xD7). Every member carries an explicit bson tag: most data requests
// rely on the driver's lowercase default, which a field rename would change
// silently, and these names are a wire contract.

// FieldRequest addresses one store instance — the bucket, the parent
// document's key and the declared field — and carries one verb's operands.
//
// Key is the PARENT DOCUMENT's key, under the top-level name "key", because
// that is the name the write path logs as the WAL entry's key, and replay
// decides whether a recorded failure commutes with a later write by comparing
// those keys. A field write that logged its element key instead would commute
// with its parent's delete, and replaying it beneath the mark would recreate
// an element of a deleted document. The element key is Element.
type FieldRequest struct {
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
	Key      string `bson:"key"`
	Field    string `bson:"field"`
	Element  string `bson:"element,omitempty"`
	// Path addresses an L3 store: [<L2 field>, <L2 element key>] — the key
	// an element key (hash, btree), a position (fifo, lifo) or
	// {priority, id} (heap). Field then names the L3 store.
	Path []interface{}          `bson:"path,omitempty"`
	Data map[string]interface{} `bson:"data,omitempty"`
	// Content is a blobStore field element's bytes, for put; Data is then its
	// metadata, as a blob write's is.
	Content      []byte `bson:"content,omitempty"`
	Priority     int64  `bson:"priority,omitempty"`
	WriteConcern string `bson:"writeconcern,omitempty"`
}

// FieldListRequest is a FieldRequest plus the page bounds of a list. After
// is where the page starts, exclusive: an element key (hash, btree, blob), a
// position (fifo, lifo) or the opaque cursor a previous page returned as Next
// (every engine). Limit defaults to 100 and is clamped to 1,000.
type FieldListRequest struct {
	FieldRequest `bson:",inline"`
	After        bson.RawValue `bson:"after,omitempty"`
	Limit        int           `bson:"limit,omitempty"`
}

// FieldPeekRequest is a FieldRequest plus the count of a peek, shaped as
// FieldListRequest is. Limit absent or zero means 1; the server refuses a
// negative one and one above maxPeekLimit. Only peek has a count: a Limit on
// FieldRequest would be a field push, pop, put, get, delete and len accept and
// silently ignore.
type FieldPeekRequest struct {
	FieldRequest `bson:",inline"`
	Limit        int `bson:"limit,omitempty"`
}

// FieldPopRequest is a FieldRequest plus the count of a pop, shaped as
// FieldPeekRequest is. Limit absent or zero means 1; the server refuses a
// negative one and one above maxPopLimit. ResultCeiling is PopRequest's: set
// by the bucket's owner before it logs the pop, overwritten there, and ignored
// by a node that logs nothing.
type FieldPopRequest struct {
	FieldRequest  `bson:",inline"`
	Limit         int   `bson:"limit,omitempty"`
	ResultCeiling int64 `bson:"result_ceiling,omitempty"`
}

// FieldResponse answers every field verb but list. Element is the element
// document a get read; a peek or a pop answers Items instead and leaves
// Element and the position fields below unset. ElementKey, Position, Priority and
// EntryID say where it was: the element key for hash, btree and blob, the
// position for fifo and lifo, the priority and entry id for heap. Len is the
// instance's element count, for len.
//
// Position and Priority are pointers: nil is an engine that has none and is
// omitted; a pointer to 0 is a real zero (the first element of a fifo or
// lifo, a heap element at its default priority) and is sent.
type FieldResponse struct {
	Success    bool     `bson:"success"`
	Message    string   `bson:"message,omitempty"`
	Found      bool     `bson:"found"`
	Element    bson.Raw `bson:"element,omitempty"`
	ElementKey string   `bson:"element_key,omitempty"`
	Position   *int64   `bson:"position,omitempty"`
	Priority   *int64   `bson:"priority,omitempty"`
	EntryID    string   `bson:"entry_id,omitempty"`
	Len        int64    `bson:"len"`
	// Items is a peek's or a pop's answer: the next elements in pop order,
	// each with where it was, as a list entry carries it.
	Items []FieldListEntry `bson:"items,omitempty"`
	// BlobID and Content are a blobStore field element's: its blob id, and
	// for get its bytes, with its metadata as Element.
	BlobID  string `bson:"blob_id,omitempty"`
	Content []byte `bson:"content,omitempty"`
	// CopiesConfirmed and ReplicationFactor are a field write's copy count
	// (server/proto.go, above InsertResponse).
	CopiesConfirmed   int    `bson:"copies_confirmed,omitempty" json:"copies_confirmed,omitempty"`
	ReplicationFactor int    `bson:"replication_factor,omitempty" json:"replication_factor,omitempty"`
	WriteOutcome      string `bson:"write_outcome,omitempty" json:"write_outcome,omitempty"`
}

// DeclareStoreFieldsRequest adds store fields to an existing bucket. Name,
// not Bucket, because this is bucket DDL, as CreateBucketRequest is. Stores
// holds only the added declarations, each {name, engine, fields}; there is no
// removal form.
type DeclareStoreFieldsRequest struct {
	Database string                   `bson:"database"`
	Name     string                   `bson:"name"`
	Stores   []map[string]interface{} `bson:"stores"`
}

// DeclareStoreFieldsResponse answers a declare.
type DeclareStoreFieldsResponse struct {
	Success bool   `bson:"success"`
	Message string `bson:"message,omitempty"`
}

// StoreStats is one declared store field in a bucket's stats: its name, its
// engine, the L2 store it is declared on (for an L3 store) and its element
// count summed over every document of the bucket — the aggregate, never one
// document's, which is len.
type StoreStats struct {
	Name     string `json:"name"`
	Engine   string `json:"engine"`
	Parent   string `json:"parent,omitempty"`
	Elements int64  `json:"elements"`
}

// FieldListEntry is one element of a list page. Position and Priority are
// pointers for the reason FieldResponse's are: nil means the engine has none.
type FieldListEntry struct {
	// Element is the element document; a blobStore field's entry has none,
	// only its BlobID, and its content is read with get.
	Element    bson.Raw `bson:"element,omitempty"`
	BlobID     string   `bson:"blob_id,omitempty"`
	ElementKey string   `bson:"element_key,omitempty"`
	Position   *int64   `bson:"position,omitempty"`
	Priority   *int64   `bson:"priority,omitempty"`
	EntryID    string   `bson:"entry_id,omitempty"`
}

// FieldListResponse is one page of a list, in the engine's order. Next is
// the cursor the following page passes as After; HasMore says whether there
// is one.
type FieldListResponse struct {
	Success bool             `bson:"success"`
	Message string           `bson:"message,omitempty"`
	Entries []FieldListEntry `bson:"entries"`
	HasMore bool             `bson:"has_more"`
	Next    []byte           `bson:"next,omitempty"`
}
