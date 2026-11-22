package store

import (
	"encoding/binary"

	"github.com/hoarddb/hoarddb-server/storage"
)

// The key scope of a delivery apply. A queue or heap bucket keeps its lease
// records under the bucket's own NSDelivery prefix; a document's store field
// keeps them under the field's store prefix, inner byte NSDelivery, so that
// the document's delete cascade (one range) removes them with it. Every key a
// deliveryOp builds goes through here, so the two cannot drift.

func (op *deliveryOp) kindKey(kind byte, parts ...[]byte) []byte {
	if op.field != nil {
		return op.field.kindKey(kind, parts...)
	}
	return kindKey(op.slot, op.databaseID, op.id, kind, parts...)
}

func (op *deliveryOp) leaseHeaderKey(lease [leaseIDBytes]byte) []byte {
	return op.kindKey(kindLeaseHeader, lease[:])
}

func (op *deliveryOp) deadlineKey(deadlineMs int64, lease [leaseIDBytes]byte) []byte {
	return op.kindKey(kindDeadline, bigEndian(uint64(deadlineMs)), lease[:])
}

func (op *deliveryOp) claimedKey(lease [leaseIDBytes]byte, origin []byte) []byte {
	return op.kindKey(kindClaimed, lease[:], origin)
}

func (op *deliveryOp) returnCountKey(origin []byte) []byte {
	return op.kindKey(kindReturnCount, origin)
}

func (op *deliveryOp) deadKey(dead []byte) []byte {
	return op.kindKey(kindDead, dead)
}

// originOf is the origin suffix of the 0x04 key. A field queue is a fifo or a
// lifo, whose suffix is always the 8-byte position at the end of the key.
func (op *deliveryOp) originOf(key []byte) []byte {
	if op.field != nil {
		return key[len(key)-positionBytes:]
	}
	return originSuffix(key)
}

// originKey is the 0x04 key an entry with origin lives at.
func (op *deliveryOp) originKey(origin []byte) []byte {
	if op.field != nil {
		return append(append([]byte(nil), op.field.entryPrefix()...), origin...)
	}
	return storage.Key(op.slot, op.databaseID, op.id, storage.NSQueue, origin)
}

// originPrefixLen is the length of a 0x04 key before its origin suffix.
func (op *deliveryOp) originPrefixLen() int {
	if op.field != nil {
		return len(op.field.entryPrefix())
	}
	return originPrefixBytes
}

func (op *deliveryOp) returnCountKeyOf(entryKey []byte) []byte {
	return op.returnCountKey(op.originOf(entryKey))
}

// composeLease is the lease id of a claim: the bucket's tag-bound form, or, on
// a field, the tag left zero because every key is already scoped by the bucket
// id and the document (conventions section 9.1 fixes the wire form at 16 bytes,
// which is the id without the tag).
func (op *deliveryOp) composeLease(args ClaimArgs, deadline int64) [leaseIDBytes]byte {
	if op.field != nil {
		var lease [leaseIDBytes]byte
		binary.BigEndian.PutUint64(lease[4:12], uint64(deadline))
		copy(lease[12:], args.Nonce[:])
		return lease
	}
	return composeLeaseID(args.NameHash, op.scope.st.Incarnation, deadline, args.Nonce)
}

// positionBytes is the width of a fifo or lifo origin suffix.
const positionBytes = 8
