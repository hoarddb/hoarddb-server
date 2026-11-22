package store

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/cespare/xxhash/v2"
	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/storage"
)

// The bounds every node agrees on (claim-ack-spec §9.3). They are exported from
// here so the server's checks and the driver-vector generator read one
// definition and cannot drift.
const (
	// MaxClaimLimit is the most entries one claim takes.
	MaxClaimLimit = 100
	// DefaultLeaseMs is a lease's length when the claim names none.
	DefaultLeaseMs = 30_000
	// MinLeaseMs and MaxLeaseMs bound a lease's length, at the door and in the
	// apply (MaxLeaseMs is also the forward-jump guard's bound).
	MinLeaseMs = 1_000
	MaxLeaseMs = 3_600_000
	// MaxDeliveries is how many times an entry is delivered before it is
	// dead-lettered instead of returned.
	MaxDeliveries = 5
	// MaxDeadMoveLimit is the most dead entries one requeue or discard moves.
	MaxDeadMoveLimit = 100
	// MaxDeadListLimit is the most dead entries one list answers.
	MaxDeadListLimit = 1000
	// MaxItemIDsPerAck is the most item ids one ack names: the most a claim
	// hands out.
	MaxItemIDsPerAck = MaxClaimLimit
)

// leaseClockToleranceMs is how far a stamped time may fall below the bucket's
// lease clock before the stamp wins over the clock (claim-ack-spec §4.8).
const leaseClockToleranceMs = 5_000

// The reasons a dead entry carries (claim-ack-spec §3.4).
const (
	DeadReasonMaxDeliveries = "max_deliveries"
	DeadReasonCollided      = "collided"
)

// The record kinds under NSDelivery, the byte after the namespace.
const (
	kindLeaseHeader byte = 'h'
	kindDeadline    byte = 'd'
	kindClaimed     byte = 'e'
	kindReturnCount byte = 'c'
	kindDead        byte = 'x'
)

const (
	leaseIDBytes = 20
	// leaseHeaderBytes is the fixed width of an 'h' value: deadline 8, count 4,
	// bytes 8. A fixed binary record rather than BSON: three numbers compared
	// on every lease verb, the same width on every copy.
	leaseHeaderBytes = 20
	// originPrefixBytes is a 0x04 key up to its origin suffix: the 18-byte
	// bucket prefix and the namespace byte.
	originPrefixBytes = scopeBytes + 1
)

// kindKey is the key of one record: the bucket's NSDelivery prefix, the kind
// and the parts. Integers in the parts are big-endian, so a deadline sorts as
// the number it is.
func kindKey(slot uint16, databaseID, id uint64, kind byte, parts ...[]byte) []byte {
	return storage.Key(slot, databaseID, id, storage.NSDelivery, append([][]byte{{kind}}, parts...)...)
}

func bigEndian(value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return encoded[:]
}

// originSuffix is the remainder of an entry's 0x04 key after the bucket prefix
// and the namespace byte: the 8-byte position of a fifo or lifo entry,
// [inverted priority : 8][entry id] of a heap's.
func originSuffix(entryKey []byte) []byte {
	return entryKey[originPrefixBytes:]
}

// leaseHeaderKey is P 'h' [lease id].
func leaseHeaderKey(slot uint16, databaseID, id uint64, lease [leaseIDBytes]byte) []byte {
	return kindKey(slot, databaseID, id, kindLeaseHeader, lease[:])
}

// deadlineKey is P 'd' [deadline ms : 8][lease id]. The first 'd' key of a
// bucket is its soonest deadline.
func deadlineKey(slot uint16, databaseID, id uint64, deadlineMs int64, lease [leaseIDBytes]byte) []byte {
	return kindKey(slot, databaseID, id, kindDeadline, bigEndian(uint64(deadlineMs)), lease[:])
}

// claimedKey is P 'e' [lease id][origin suffix].
func claimedKey(slot uint16, databaseID, id uint64, lease [leaseIDBytes]byte, origin []byte) []byte {
	return kindKey(slot, databaseID, id, kindClaimed, lease[:], origin)
}

// returnCountKey is P 'c' [origin suffix].
func returnCountKey(slot uint16, databaseID, id uint64, origin []byte) []byte {
	return kindKey(slot, databaseID, id, kindReturnCount, origin)
}

// deadKey is P 'x' [dead suffix].
func deadKey(slot uint16, databaseID, id uint64, dead []byte) []byte {
	return kindKey(slot, databaseID, id, kindDead, dead)
}

// deadSuffix is the key suffix of a dead entry: the origin suffix for fifo and
// lifo, whose positions are never reused, and [origin suffix][dead sequence : 8]
// for a heap, where a stable _id_ pushed again after it died, or a collided
// element and its live twin both dying, would otherwise land on one key.
func deadSuffix(layout claimLayout, origin []byte, deadSeq uint64) []byte {
	if layout != claimLayoutHeap {
		return origin
	}
	return append(append([]byte(nil), origin...), bigEndian(deadSeq)...)
}

// deadIDString is dead_id, the handle an operator names a dead entry by: the
// dead suffix in lowercase hex.
func deadIDString(dead []byte) string {
	return hex.EncodeToString(dead)
}

// leaseHeader is the 'h' value of one outstanding lease.
type leaseHeader struct {
	// Deadline is the lease's deadline on the bucket clock, in ms.
	Deadline int64
	// Count is how many 'e' keys are still under the lease.
	Count uint32
	// Bytes is the sum of len(key)+len(value) of those 'e' keys.
	Bytes int64
}

func (header leaseHeader) encode() []byte {
	encoded := make([]byte, leaseHeaderBytes)
	binary.BigEndian.PutUint64(encoded[0:8], uint64(header.Deadline))
	binary.BigEndian.PutUint32(encoded[8:12], header.Count)
	binary.BigEndian.PutUint64(encoded[12:20], uint64(header.Bytes))
	return encoded
}

func decodeLeaseHeader(encoded []byte) (leaseHeader, error) {
	if len(encoded) != leaseHeaderBytes {
		return leaseHeader{}, fmt.Errorf("a lease header is %d bytes, this one is %d", leaseHeaderBytes, len(encoded))
	}
	return leaseHeader{
		Deadline: int64(binary.BigEndian.Uint64(encoded[0:8])),
		Count:    binary.BigEndian.Uint32(encoded[8:12]),
		Bytes:    int64(binary.BigEndian.Uint64(encoded[12:20])),
	}, nil
}

// claimedValue is the 'e' value: uvarint(r), the number of times the entry has
// been returned, then the exact bytes the entry had under 0x04.
func claimedValue(returns uint64, entry []byte) []byte {
	return append(binary.AppendUvarint(nil, returns), entry...)
}

func parseClaimedValue(encoded []byte) (returns uint64, entry []byte, err error) {
	returns, width := binary.Uvarint(encoded)
	if width <= 0 {
		return 0, nil, errors.New("a claimed entry's value does not start with its return count")
	}
	return returns, encoded[width:], nil
}

// returnCountValue is the 'c' value: uvarint(r), r at least 1.
func returnCountValue(returns uint64) []byte {
	return binary.AppendUvarint(nil, returns)
}

func parseReturnCountValue(encoded []byte) (uint64, error) {
	returns, width := binary.Uvarint(encoded)
	if width != len(encoded) || width <= 0 {
		return 0, errors.New("a return count is one uvarint")
	}
	if returns < 1 {
		return 0, errors.New("a return count is at least 1: an entry never returned has no record")
	}
	return returns, nil
}

// deadRecord is the BSON value of an 'x' key (claim-ack-spec §3.4). The field
// order is the struct's, so every copy encodes the same bytes.
type deadRecord struct {
	// Data is the stored entry: a document for fifo and lifo, {_id_, priority,
	// value} for a heap.
	Data bson.Raw `bson:"data"`
	// Deliveries is the claims recorded, r' at dead-lettering.
	Deliveries int32 `bson:"deliveries"`
	// DeadAt is the effective now of the apply that dead-lettered it, in ms.
	DeadAt primitive.DateTime `bson:"dead_at"`
	// Reason is DeadReasonMaxDeliveries or DeadReasonCollided.
	Reason string `bson:"reason"`
}

func encodeDeadRecord(record deadRecord) ([]byte, error) {
	encoded, err := bson.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("encoding a dead record: %w", err)
	}
	return encoded, nil
}

func decodeDeadRecord(encoded []byte) (deadRecord, error) {
	var record deadRecord
	if err := bson.Unmarshal(encoded, &record); err != nil {
		return deadRecord{}, fmt.Errorf("decoding a dead record: %w", err)
	}
	return record, nil
}

// DeliveryValueDocument is the BSON document the value of a delivery record
// holds, for a reader that stores the value as it is: the stored entry a
// claimed record carries after its return count, or a dead record, whose data
// holds the stored entry (dead is then true). rest is the key after the
// bucket's [s][id][0x0D]. A lease header, a deadline and a return count are
// fixed binary and hold none, nor does a kind this build does not write,
// which no reader of it reads.
func DeliveryValueDocument(rest, value []byte) (document []byte, dead bool, err error) {
	if len(rest) == 0 {
		return nil, false, errors.New("a delivery key holds no record kind")
	}
	switch rest[0] {
	case kindClaimed:
		_, entry, err := parseClaimedValue(value)
		return entry, false, err
	case kindDead:
		return value, true, nil
	}
	return nil, false, nil
}

// BucketNameHash is xxhash64(database 0x00 bucket), the name a lease's tag is
// built from. Bucket ids are allocated per node, so the tag uses names, which
// are the same on every copy; the server computes the hash and passes it in.
func BucketNameHash(database, bucket string) uint64 {
	return xxhash.Sum64String(database + "\x00" + bucket)
}

// leaseTag is the high four bytes of xxhash64(nameHash ‖ incarnation : 8): what
// binds a lease id to the bucket it was granted in, across a drop and a create
// of the same name.
func leaseTag(nameHash, incarnation uint64) uint32 {
	return uint32(xxhash.Sum64(append(bigEndian(nameHash), bigEndian(incarnation)...)) >> 32)
}

// composeLeaseID is [tag : 4][deadline at grant, ms : 8][nonce : 8]. It reads
// no clock and no entropy: the deadline is derived in the apply from the
// effective now, the nonce is stamped by the owner, and every copy of the apply
// composes the same 20 bytes (claim-ack-spec CA-13).
func composeLeaseID(nameHash, incarnation uint64, deadlineMs int64, nonce [8]byte) [leaseIDBytes]byte {
	var lease [leaseIDBytes]byte
	binary.BigEndian.PutUint32(lease[0:4], leaseTag(nameHash, incarnation))
	binary.BigEndian.PutUint64(lease[4:12], uint64(deadlineMs))
	copy(lease[12:], nonce[:])
	return lease
}

// leaseIDString is the lease id as it is handed to a client: 40 lowercase hex
// characters.
func leaseIDString(lease [leaseIDBytes]byte) string {
	return hex.EncodeToString(lease[:])
}

// parseLeaseID reads the 40 hex characters of a lease id.
func parseLeaseID(text string) ([leaseIDBytes]byte, error) {
	var lease [leaseIDBytes]byte
	if len(text) != 2*leaseIDBytes {
		return lease, fmt.Errorf("a lease id is %d hex characters, this one is %d", 2*leaseIDBytes, len(text))
	}
	if _, err := hex.Decode(lease[:], []byte(text)); err != nil {
		return lease, fmt.Errorf("a lease id is hex: %w", err)
	}
	return lease, nil
}

// LeaseIDBytes is the length of a lease id: 40 hex characters on the wire.
const LeaseIDBytes = leaseIDBytes

// ParseLeaseID reads the 40 hex characters a claim returned in lease_id. The
// server checks it in its handler (claim-ack-spec 9.3), never in a store verb.
func ParseLeaseID(text string) ([LeaseIDBytes]byte, error) {
	return parseLeaseID(text)
}

// LeaseIDString is a lease id as a claim answers it.
func LeaseIDString(lease [LeaseIDBytes]byte) string {
	return leaseIDString(lease)
}

// effectiveNow is the time every lease decision of an apply uses (claim-ack-spec
// §4.8). A stamp at or above the bucket's lease clock is the time. One below it by
// no more than the tolerance is absorbed: the clock stands, so an owner clock
// that stepped back a little cannot reopen a lease. One further below wins over
// the clock and is reported as a step, for a paced WARN: the clock was ahead
// and was corrected, and holding it would hold leases for as long as it was
// ahead.
func effectiveNow(stamped int64, st *BucketState) (now int64, stepped bool) {
	switch {
	case st.LeaseClock == 0 || stamped >= st.LeaseClock:
		return stamped, false
	case stamped >= st.LeaseClock-leaseClockToleranceMs:
		return st.LeaseClock, false
	default:
		return stamped, true
	}
}
