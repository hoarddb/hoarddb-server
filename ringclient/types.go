package ringclient

import (
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// Options says where to connect, who to be and whom to trust.
type Options struct {
	// Addr is the host:port of the node's TCP listener.
	Addr string
	// User and Password are the AUTH credentials.
	User, Password string
	// Fingerprint pins the server's certificate: the ed25519 fingerprint the server prints
	// when it starts. Set this or Insecure; Dial refuses a connection that does neither.
	Fingerprint string
	// Fingerprints are more pins: the server may present any of Fingerprint and these. A tool
	// that reads every member of a cluster (dump, restore) passes one per member, because each
	// member has its own certificate.
	Fingerprints []string
	// Insecure skips certificate verification. It is for local development against a node
	// you started yourself: anyone on the path can read and change the traffic, password
	// included. Use Fingerprint for anything else.
	Insecure bool
	// DialTimeout bounds the TCP and TLS connect and the AUTH round trip; 0 means 10 seconds.
	DialTimeout time.Duration
	// RequestTimeout bounds a request whose context has no deadline; 0 means 30 seconds.
	RequestTimeout time.Duration
}

// Doc is the scalar fields of a document by declared field name. A value is marshalled as
// BSON, so a Go string, int64, time.Time, []string or bson.D all work. A store field is
// not a Doc member: it is written with the store methods of [Bucket].
type Doc map[string]any

// WriteResult is what the owner said about a write.
type WriteResult struct {
	// Outcome is applied, applied_degraded or unknown (conventions section 17).
	Outcome string
	// Copies is how many copies confirmed the write.
	Copies int
	// Position is the applied record's epoch:seq, empty when the owner sent none.
	Position string
	// Deduped is true when the answer came from the owner's dedupe window: an earlier
	// attempt under the same request id had already been decided.
	Deduped bool
	// NotApplied is true when the owner refused the write because its guard did not hold
	// (Outcome is then "not_applied"). Nothing was written and no position was taken.
	NotApplied bool
	// Reason is why the write was not applied: "guard".
	Reason string
	// GuardField is the first guarded field that differed.
	GuardField string
	// Document is the document the owner returned: after or before an applied write when
	// [Return] asked for it, or the current document when the guard refused the write.
	Document bson.Raw
	// DocumentTruncated is true when a deduped retry's document was too large to repeat;
	// read it with Get.
	DocumentTruncated bool
}

// Guard is the equalities a document must satisfy for an Update or Delete to apply: field
// name to value, in the order written. A nil value matches a field that is null or was
// never set. Only declared scalar fields can be guarded.
type Guard bson.D

// ReturnMode says which document an Update or Delete sends back.
type ReturnMode string

const (
	// ReturnAfter returns the document as the update left it. A delete has no after.
	ReturnAfter ReturnMode = "after"
	// ReturnBefore returns the document as it was before the write.
	ReturnBefore ReturnMode = "before"
)

// Applied reports whether the write is acknowledged as applied. applied_degraded and
// unknown are not durable acknowledgements; a caller that needs one checks this.
func (w WriteResult) Applied() bool { return w.Outcome == "applied" }

// FieldDecl is one field of a bucket to create: a name and a canonical type word, a
// scalar (string, int, datetime, array ...) or a store (fifoStore, lifoStore, hashStore,
// btreeStore, heapStore, blobStore).
type FieldDecl struct {
	Name, Type string
	// ExpirePath is the element path of the datetime a fifoStore or lifoStore field expires its
	// elements at; empty for every other field.
	ExpirePath string
	// Of is "<Bucket>" or "<Database>.<Bucket>" when the field holds that bucket's ids;
	// empty for none. Expanding the field then needs no bucket name.
	Of string
	// Aggregates are the maintained aggregates of a store field, in the order written.
	Aggregates []AggregateDecl
}

// AggregateDecl is one aggregate of a store field: Kind is "sum", "sum by" or "count by",
// Field the summed element path (empty for count by) and By the grouping path (empty for sum).
type AggregateDecl struct {
	Kind, Field, By string
}

// AggregateInfo is one maintained aggregate of a bucket, as describe bucket reports it. Store
// is the field that keeps it and State is ready or building.
type AggregateInfo struct {
	Store, Kind, Field, By, State string
	ID                            uint8
}

// FieldInfo is one row of a bucket's field table, with the id the wire names it by.
type FieldInfo struct {
	ID         uint16
	Name, Type string
	// Of is the bucket the field is declared of: "<Bucket>", "<Database>.<Bucket>" when it
	// lives in another database, or "#<id>" when that bucket is gone; empty for none.
	Of string
}

// BucketInfo is the declaration in force, which is also the header of a keyed request.
type BucketInfo struct {
	// Database and Name locate the bucket.
	Database, Name string
	// ID is the bucket id a keyed request carries.
	ID uint64
	// State is active or dropping.
	State string
	// DeclVersion and SchemaVersionSeen are the versions a keyed request carries.
	DeclVersion, SchemaVersionSeen int64
	// Fields is the field table in id order, _id_ first.
	Fields []FieldInfo
	// ExpireField is the datetime field the bucket expires its documents at, empty when none;
	// ExpireStores lists each queue field that expires its elements.
	ExpireField  string
	ExpireStores []ExpireStoreInfo
	// ExpirePending is how many documents and queue elements are past their time and not yet
	// removed (describe only); ExpirePendingMore says there are at least that many.
	ExpirePending     int64
	ExpirePendingMore bool
	// Aggregates are the maintained aggregates of the store fields, by field then id.
	Aggregates []AggregateInfo
}

// ExpireStoreInfo is one expiring queue field of a bucket: its name and the element path of its datetime.
type ExpireStoreInfo struct {
	Field, Path string
}

// DatabaseInfo answers create_database.
type DatabaseInfo struct {
	ID      int64
	RF      int32
	Storage string
}

// DatabaseSummary is one row of a ShowDatabases page.
type DatabaseSummary struct {
	Name string
	ID   int64
	RF   int32
}

// DatabasePage is one page of ShowDatabases. When More is true, pass Cursor back for the next.
type DatabasePage struct {
	Databases []DatabaseSummary
	More      bool
	Cursor    []byte
}

// DatabaseDescription is a database and one page of its buckets. When More is true, pass
// Cursor back to DescribeDatabase for the next page of buckets.
type DatabaseDescription struct {
	Name                        string
	ID                          int64
	RF                          int32
	Storage                     string
	MemoryBytes, MemoryCapBytes int64
	Buckets                     []BucketSummary
	More                        bool
	Cursor                      []byte
}

// PageRequest asks for one page of a listing. Limit 0 takes the server's default.
type PageRequest struct {
	Limit  int32
	Cursor []byte
	// Fields names the scalar fields a bucket List returns besides _id_; nil returns every
	// field. It is for Bucket.List only; the listings of buckets and databases ignore it.
	Fields []string
	// Expand names the far buckets a bucket List brings the documents of; see ExpandRequest.
	Expand []ExpandRequest
	// Local lets a ready copy on the node answer the expanded reads (a stale read the caller
	// accepts); the page says which side answered in Served.
	Local bool
}

// ExpandRequest names one expansion of a list or a get: the documents a reference points at,
// read by key. Field is the scalar field holding the far id (empty on a store list, where the
// element keys are the ids). Database and Bucket name the far bucket and are empty when the
// field is declared 'of' it. Fields projects the far document (nil: every scalar). Where is a
// BSON document of equalities on the far document that removes the rows that do not match from
// the page; it is not an index.
type ExpandRequest struct {
	Field, Database, Bucket string
	Fields                  []string
	Where                   any
}

// BucketSummary is one row of a ShowBuckets page.
type BucketSummary struct {
	Name  string
	ID    uint64
	State string
}

// BucketPage is one page of ShowBuckets. When More is true, pass Cursor back for the next.
type BucketPage struct {
	Buckets []BucketSummary
	More    bool
	Cursor  []byte
}

// ElementPage is one page of a store: the elements, whether another page exists and the
// cursor to ask for it with.
type ElementPage struct {
	Elements []bson.Raw
	// Keys are the element keys of a hashStore or btreeStore, or the entry ids of a heapStore,
	// one per element in the same order; none for a fifoStore or lifoStore.
	Keys []string
	// Priorities are the priorities of a heapStore's elements, in the same order as Keys.
	Priorities []int64
	More       bool
	Cursor     []byte
	// Expanded answers Elements row for row after a StoreListExpanded: one document keyed by
	// the store's name whose value is {expanded: true, doc} or {expanded: false, reason}.
	Expanded []bson.Raw
	// Served is "local" or "owner" for an expanded page, empty otherwise.
	Served string
}

// Entry is one document of a bucket listing.
type Entry struct {
	// ID is the document id.
	ID string
	// Body is the stored document.
	Body bson.Raw
}

// EntryPage is one page of a bucket listing.
type EntryPage struct {
	Entries []Entry
	More    bool
	Cursor  []byte
	// Expanded answers Entries row for row when the list expanded: one document keyed by each
	// expanded field whose value is {expanded: true, doc} or {expanded: false, reason}.
	Expanded []bson.Raw
	// ExpandFiltered is how many rows a where removed from the page.
	ExpandFiltered int
	// Served is "local" or "owner" for an expanded page, empty otherwise.
	Served string
}

// Lease is what a claim returned: the lease id and the messages it holds.
type Lease struct {
	// ID is the 16-byte lease id; all zeros when the store had nothing to lease.
	ID [16]byte
	// Elements are the leased messages in claim order.
	Elements []bson.Raw
	// Write is the owner's answer to the claim.
	Write WriteResult
}

// LeaseChange is the answer to an ack, release or extend.
type LeaseChange struct {
	// Write is the owner's answer.
	Write WriteResult
	// Live is, for a release or an extend, whether the lease was live. It is false for an
	// expired or unknown lease, which is an answer and not an error. An ack leaves it false.
	Live bool
}

// LeaseRead is what a lease_get found.
type LeaseRead struct {
	// Found is false when the lease is unknown, acked, released or past its deadline.
	Found bool
	// Elements are the messages the live lease holds, in claim order.
	Elements []bson.Raw
}

// NodeDescription is the node that answered DescribeNode.
type NodeDescription struct {
	NodeID  string
	Version string
	// UptimeMS is how long the server process has run, in milliseconds.
	UptimeMS int64
	// MemberState is this node's own row in the cluster (active, joining, ...); empty when it has none.
	MemberState string
	Members     int32
	Voters      int32
	Epoch       int64
	// Leader is the node id of the Raft leader this node knows; empty when there is none.
	Leader string
	// DefaultRF is the replication factor a database gets when create database names none.
	DefaultRF int32
	// Serving is "ok", "no_quorum" or "lease_lapsed".
	Serving string
	// StateRange is the state-level range the node declares ("min-max"), empty when it sends none.
	StateRange string
	// RefusedPeers is one line per peer the node refused for a state range that does not overlap its own.
	RefusedPeers []string
	// ClusterVersion is the cluster version of the state the node has applied, 0 when it holds none.
	ClusterVersion int32
	// PasswordKey is a cluster member's password key status: "held", "fetching", "none_yet" or
	// "witness"; empty on a node that is not a cluster member. A script after 'join' waits for "held".
	PasswordKey string
	// Host is the machine the node runs on; nil when the server could not read it.
	Host *HostDescription
}

// HostDescription is the optional machine part of a NodeDescription. A member the server could
// not read is its zero value (a nil Load or Process, a zero MemTotal), never a measured zero.
type HostDescription struct {
	CPUs int32
	// HostCPUs is the machine's CPUs, set only when a quota made CPUs smaller.
	HostCPUs int32
	Load     *Load
	// MemUsed and MemTotal are the machine's (or the cgroup's) memory in bytes; MemSource is "host" or "cgroup".
	MemUsed   int64
	MemTotal  int64
	MemSource string
	Process   *ProcessMemory
}

// Load is the machine's load average over one, five and fifteen minutes.
type Load struct{ One, Five, Fifteen float64 }

// ProcessMemory is what the server process uses and the limit it is measured against, in bytes.
type ProcessMemory struct{ Used, Limit int64 }

// SeedingRow is one slot of one database in the seeding view of the node the client is on.
type SeedingRow struct {
	Database string
	// Slot is 0 to 8191.
	Slot int
	// State is "placing", "seeding", "stalled", "ready" or "retired".
	State string
	// Source is the node the pull reads from; empty when none is chosen.
	Source string
	// BytesDone is what has been applied; BytesKnown is what the source said the range holds, or 0 while unknown.
	BytesDone, BytesKnown int64
	Entries               int64
	// Idle is how long ago the last chunk was applied; 0 when none has been.
	Idle time.Duration
	// Refusal is the last refusal text of the source, or empty.
	Refusal string
}

// SeedingQuery narrows a SeedingStatus: Database alone keeps one database's rows, with Slot one
// slot's. Slot needs Database.
type SeedingQuery struct {
	Database string
	Slot     *int
	Page     PageRequest
}

// SeedingPage is one page of SeedingStatus, ordered by database then slot.
type SeedingPage struct {
	Rows   []SeedingRow
	More   bool
	Cursor []byte
}

// NearestQuery is a nearest read. Vector is required; the other members take their defaults when zero.
type NearestQuery struct {
	Vector []byte // 4 to 16384 bytes of little-endian float32
	K      int    // 1 to 100; 0 means 10
	Metric string // "cosine" (the default when empty), "dot" or "l2"
	Field  string // the element field that holds the vector; "" means "Vector"
}

// NearestResult is a nearest's answer, best first. Keys is empty for a fifoStore or lifoStore.
type NearestResult struct {
	Elements []bson.Raw
	Keys     []string
	Scores   []float64
	Scanned  int
	Skipped  int
}
