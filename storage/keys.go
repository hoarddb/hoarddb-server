package storage

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/cespare/xxhash/v2"
)

// The key layout of the node's shared store (entry-ring conventions §1): every
// document key is
//
//	[slot : 2 bytes BE][database id : 8 bytes BE][bucket id : 8 bytes BE][namespace : 1 byte] ‖ body
//
// The database id comes before the bucket id, so a drop database is one range
// per slot: PairPrefix(slot, database id) is its bound. The node block lives
// under NodeRecordSlot, a separate byte space from every slot.

// SlotCount is the number of storage slots, fixed forever: a slot is 13 bits.
// Thirteen and not fourteen because a 14-bit slot reaches first byte 0x21,
// Badger's reserved "!badger!" prefix; every 13-bit slot keeps its first byte
// at 0x00–0x1F.
const SlotCount = 8192

// NodeRecordSlot is the reserved slot of the node's own records — the catalog,
// the reclaim records and the allocator — always with bucket id 0. It lies
// outside [0, SlotCount), so those records sort after every slot and inside
// no slot's range: a slot-0 range scan cannot ship the catalog.
const NodeRecordSlot uint16 = 0xFFFF

// Namespaces inside a bucket's range (spec §3.1's table).
const (
	NSDocuments byte = 0x01
	NSVersions  byte = 0x02
	NSIndex     byte = 0x03
	NSQueue     byte = 0x04
	NSState     byte = 0x05
	NSBlobMeta  byte = 0x06
	// NSFieldStore holds every store field of a document:
	// encID ‖ fieldID:2 ‖ inner:1 ‖ … (entry-ring conventions §1).
	NSFieldStore byte = 0x07
	// NSDelivery holds a queue or heap bucket's leases, its claimed entries,
	// the return counts of entries back in the queue, and its dead entries
	// (claim-ack-spec.md §3). A bucket that never claims holds no key here.
	NSDelivery byte = 0x0D
	// NSDedupe holds the idempotency entry of one client operation,
	// [requestID:16][opIndex:1], written in the same batch as the write it
	// describes (entry-ring conventions §1, §18).
	NSDedupe byte = 0x0E
)

// Namespaces under [NodeRecordSlot][0] only. The two numberings are separate
// (bucket keys live under slots below SlotCount, node records under
// NodeRecordSlot), and every claim on a byte inside a bucket's range is:
//
//	0x01-0x07  the bucket namespaces above, on main
//	0x08       local summaries (an unpublished design note on cross-bucket summaries,
//	           Tier 1): a note only, nothing writes it
//	0x0D       NSDelivery (claim-ack-spec.md §3.1)
//	0x0E       NSDedupe (entry-ring conventions §1)
//	0x11       NSExpiry (expiry spec §3.1)
//
// A user bucket never holds 0x09 to 0x0C, and the constants below stay out of
// the bucket block; TestBucketNamespacesAreDistinct reads both blocks.
const (
	NSCatalog byte = 0x01
	NSReclaim byte = 0x08
	// NSExpiry holds the node's one expiry queue, ordered by time (expiry spec §3.1).
	NSExpiry    byte = 0x11
	NSAllocator byte = 0x09
	// NSMaterialize holds one record per bucket id reserved for a migrated
	// range whose catalog entry has not committed yet.
	NSMaterialize byte = 0x0A
	// NSBlobRegistry holds the one record that says this node has had a blob
	// key registry, keys/blobs.wrapped (at-rest encryption spec §4.2.1 g). It
	// holds no key material. A start that finds the record and no registry
	// refuses instead of minting a registry whose keys open no existing blob.
	NSBlobRegistry byte = 0x0B
	// NSReplication holds the node's replication records, one record kind per
	// first byte of the key after the namespace, and none of them may be used
	// for anything else:
	//
	//	'p'  a source's stream position
	//	'f'  a snapshot position of one (source, database, bucket)
	//	'l'  a snapshot position recorded by bare bucket name, before the
	//	     database was part of the name
	//	'm'  the membership history
	//	'h'  a copy that is not settled after a membership change
	//	'x'  the record of this node's own removal
	//	'g'  a dropped database generation
	//	'd'  a dropped bucket's incarnation
	//	'v'  a source's recorded divergence
	//	's'  a source's recorded size refusal
	//	'a'  the owner's awaiting record of a GAP-23 alter (AwaitingRecordKind)
	//	'n'  a member's needs-a-copy record of a GAP-23 alter
	//	     (NeedsCopyRecordKind)
	//
	// Tests use 'z'. Each kind's constant ends in RecordKind, which is how
	// TestEveryReplicationRecordKindIsUniqueAndListed finds it.
	NSReplication byte = 0x0C
	// NSSeedState holds the per-(slot, database) seed record: absent, seeding,
	// ready, retired or retiring (entry-ring conventions §12). It sits in the
	// node block, so a range delete over a document scope never reaches it.
	NSSeedState byte = 0x0F
)

// AwaitingRecordKind is the NSReplication kind of the record a bucket's owner
// keeps for each member an increase of the bucket's replication factor newly
// places, until that member's copy is seeded (bucket-replication-rev8-spec
// §3.3): 'a' ‖ PlacementKey(database, bucket) ‖ uvarint(len(member)) ‖ member.
// Every key of the kind starts with the database, so one prefix scan finds a
// database's records.
const AwaitingRecordKind byte = 'a'

// NeedsCopyRecordKind is the NSReplication kind of the record a member keeps
// when an increase of a bucket's replication factor places it and it holds
// nothing of the bucket, until its copy lands or a later change un-places it
// (bucket-replication-rev8-spec §3.3): 'n' ‖ PlacementKey(database, bucket).
// Every key of the kind starts with the database, so one prefix scan finds a
// database's records.
const NeedsCopyRecordKind byte = 'n'

// badgerReservedPrefix is the prefix Badger reserves for its own keys
// (db.go:badgerPrefix, unexported). It is the one literal the id band is
// derived from, and TestBandIsDerivedFromBadger fails if Badger stops
// refusing it.
const badgerReservedPrefix = "!badger!"

// ReservedBandLow and ReservedBandHigh bound, half-open, the bucket ids whose
// first four bytes spell "!bad". No id in the band is ever allocated: a range
// operation over such an id would cover Badger's own "!badger!txn" keys. Under
// the slot no *key* begins with an id any more, so the band guards only
// id-keyed operations; it is kept because it is cheap.
var (
	ReservedBandLow  = uint64(binary.BigEndian.Uint32([]byte(badgerReservedPrefix)[:4])) << 32
	ReservedBandHigh = ReservedBandLow + 1<<32
)

// MaxBucketID is the allocator's ceiling. It is math.MaxInt64 and not just
// below the sentinels because every id is written into a BSON value, and the
// BSON library refuses a uint64 above math.MaxInt64.
const MaxBucketID uint64 = math.MaxInt64

// Sentinel ids that live only in keys under [NodeRecordSlot][0][NSReclaim],
// never in a BSON value and never allocated.
const (
	// OrphanCursorID keys the orphan pass's cursor record.
	OrphanCursorID uint64 = 0xFFFFFFFFFFFFFFFE
	// DatabaseDropID keys the database-drop records.
	DatabaseDropID uint64 = 0xFFFFFFFFFFFFFFFF
)

// ErrBucketIDsExhausted is returned by NextBucketID once the ceiling is
// reached. Ids are never reused, so there is no wrap.
var ErrBucketIDsExhausted = errors.New("every bucket id this node can allocate has been allocated")

// NextBucketID returns the id after prev, skipping the reserved band, or
// ErrBucketIDsExhausted when that id would exceed MaxBucketID.
func NextBucketID(prev uint64) (uint64, error) {
	if prev >= MaxBucketID {
		return 0, ErrBucketIDsExhausted
	}
	next := prev + 1
	if next >= ReservedBandLow && next < ReservedBandHigh {
		next = ReservedBandHigh
	}
	if next > MaxBucketID {
		return 0, ErrBucketIDsExhausted
	}
	return next, nil
}

// Scope is the 18-byte prefix of every key of one bucket of one database in
// slot: [slot][database id][bucket id]. It is the bound of a bucket's range
// and the prefix of every document key the bucket owns.
func Scope(slot uint16, databaseID, bucketID uint64) []byte {
	scope := make([]byte, 18)
	binary.BigEndian.PutUint16(scope[0:2], slot)
	binary.BigEndian.PutUint64(scope[2:10], databaseID)
	binary.BigEndian.PutUint64(scope[10:18], bucketID)
	return scope
}

// PairPrefix is the 10-byte [slot][database id] prefix of every key of one
// database in slot. It is the bound of the seed's reset range and of the
// gate's delete range, so the 10-byte width appears only here.
func PairPrefix(slot uint16, databaseID uint64) []byte {
	prefix := make([]byte, 10)
	binary.BigEndian.PutUint16(prefix[0:2], slot)
	binary.BigEndian.PutUint64(prefix[2:10], databaseID)
	return prefix
}

// Key builds a document key: the 19-byte prefix Scope(slot, databaseID,
// bucketID) ‖ ns, followed by each part of rest, concatenated.
func Key(slot uint16, databaseID, bucketID uint64, ns byte, rest ...[]byte) []byte {
	size := ScopeNamespaceLen
	for _, part := range rest {
		size += len(part)
	}
	key := make([]byte, 0, size)
	key = append(key, Scope(slot, databaseID, bucketID)...)
	key = append(key, ns)
	for _, part := range rest {
		key = append(key, part...)
	}
	return key
}

// ScopeNamespaceLen is the length of the prefix every key in a bucket's range starts with:
// slot, database id, bucket id and the namespace byte. What follows is the key's body.
const ScopeNamespaceLen = 19

// The ids of the two prefix rules of the table bloom filter (prefix bloom spec §3.1). An id
// is written into every table built under its rule and names the function below that hashed
// the table's prefixes: a change to a function, or to the key layout, that moves one output
// needs a new id and a ring format bump, or a reader skips a table that holds its keys.
// TestPrefixLenFunctions freezes the outputs.
const (
	DocumentPrefixRule uint32 = 1
	StorePrefixRule    uint32 = 2
)

// DocumentPrefixLen is the length of the document prefix key starts with, or 0 when key is
// not a document's header or scalar field key: Key(slot, database, bucket, NSDocuments, encID),
// where encID is the one encoded component that follows the namespace. It reads no byte past
// that component, so the prefix answers its own length whatever follows it.
func DocumentPrefixLen(key []byte) int {
	return ruleComponentEnd(key, NSDocuments)
}

// StorePrefixLen is the length of the store prefix key starts with, or 0 when key is not a
// key of a store field: Key(slot, database, bucket, NSFieldStore, encID) ‖ fieldID:2. A key of
// a store declared on a store's element starts with the outer store's prefix, and that is the
// length returned. It reads no byte past the component, and requires the two bytes of the
// field id to be there without reading them.
func StorePrefixLen(key []byte) int {
	end := ruleComponentEnd(key, NSFieldStore)
	if end == 0 || len(key) < end+2 {
		return 0
	}
	return end + 2
}

// ruleComponentEnd is the offset after the first component of key's body when key lies in a
// slot, in namespace ns, and that component decodes; 0 otherwise. Node records (slot 0xFFFF)
// and Badger's own keys (first bytes above every slot) get 0.
func ruleComponentEnd(key []byte, ns byte) int {
	if len(key) <= ScopeNamespaceLen || key[ScopeNamespaceLen-1] != ns || binary.BigEndian.Uint16(key[0:2]) >= SlotCount {
		return 0
	}
	width, err := EncodedComponentLen(key[ScopeNamespaceLen:])
	if err != nil {
		return 0
	}
	return ScopeNamespaceLen + width
}

// NodeKey builds a node-block key: [0xFFFF][0 × 8][ns] followed by each part of
// rest. The node block is a byte space of its own, outside every slot's range.
func NodeKey(ns byte, rest ...[]byte) []byte {
	size := 11
	for _, part := range rest {
		size += len(part)
	}
	key := make([]byte, 11, size)
	binary.BigEndian.PutUint16(key[0:2], NodeRecordSlot)
	key[10] = ns
	for _, part := range rest {
		key = append(key, part...)
	}
	return key
}

// PlacementKey is a document's placement key: uvarint(len(database)) ‖
// database ‖ uvarint(len(bucket)) ‖ bucket ‖ encodedID, where encodedID is the
// EncodeComponent encoding of _id_. The length prefixes make ("ab", "c") and
// ("a", "bc") two keys. The encoding is frozen by
// storage/testdata/slot-vectors.json.
func PlacementKey(database, bucket string, encodedID []byte) []byte {
	key := make([]byte, 0, len(database)+len(bucket)+len(encodedID)+2*binary.MaxVarintLen64)
	key = binary.AppendUvarint(key, uint64(len(database)))
	key = append(key, database...)
	key = binary.AppendUvarint(key, uint64(len(bucket)))
	key = append(key, bucket...)
	key = append(key, encodedID...)
	return key
}

// SlotOf is the storage slot of a placement key: the top 13 bits of its
// xxhash64, frozen by storage/testdata/slot-vectors.json. It is computed on
// every operation and never stored, so a wrong one reads and writes the wrong
// range with no error; that is why this function never changes.
func SlotOf(placementKey []byte) uint16 {
	return uint16(xxhash.Sum64(placementKey) >> 51)
}

// BucketRef names one bucket's key range: the slot, database id and bucket id
// every Key, Scope and engine constructor takes. It exists so a function that
// needs all three stays under six parameters.
type BucketRef struct {
	Slot       uint16
	DatabaseID uint64
	BucketID   uint64
}

// Scope is the bucket's range prefix, Scope(Slot, DatabaseID, BucketID).
func (ref BucketRef) Scope() []byte { return Scope(ref.Slot, ref.DatabaseID, ref.BucketID) }
