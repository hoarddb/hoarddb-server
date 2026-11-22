package store

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/hoarddb/hoarddb-server/storage"
)

// ErrLeaseTooLarge is a lease whose messages hold more bytes than the caller said a reply
// may carry.
var ErrLeaseTooLarge = errors.New("the lease holds more bytes than one reply carries")

// LeaseView is what a read of a lease found.
type LeaseView struct {
	// Live is true when the lease exists and has not run out.
	Live bool
	// Deadline is the live lease's deadline in ms.
	Deadline int64
	// Entries are the messages under the lease in claim order.
	Entries [][]byte
}

// FieldLeaseElements reads the messages held under one lease of a fifo or lifo field, from
// one snapshot, writing nothing and running no return step. A lease that is unknown on this
// field, acked, released, or past its deadline is not live: the answer is an empty view, not
// an error. The deadline test is the one ack and extend apply (namedLease), so a read never
// shows a lease the next write would call expired. The leases of another field or another
// document are not visible, because the lease's records sit under this field's own prefix.
//
// The bytes of the messages are checked against maxBytes before each one is copied, so a
// lease larger than a reply is refused with ErrLeaseTooLarge having copied at most
// maxBytes. The lease holds at most MaxClaimLimit messages, which a header claiming more
// contradicts.
func FieldLeaseElements(snap storage.KVSnapshot, ref FieldRef, args LeaseArgs, maxBytes int) (LeaseView, error) {
	queue, err := newFieldQueue(snap, nil, ref)
	if err != nil {
		return LeaseView{}, fmt.Errorf("reading a lease of a field store: %w", err)
	}
	encoded, err := snap.Get(queue.kindKey(kindLeaseHeader, args.LeaseID[:]))
	switch {
	case errors.Is(err, storage.ErrKeyNotFound):
		return LeaseView{}, nil
	case err != nil:
		return LeaseView{}, fmt.Errorf("reading the header of lease %x: %w", args.LeaseID, err)
	}
	header, err := decodeLeaseHeader(encoded)
	if err != nil {
		return LeaseView{}, fmt.Errorf("%w: lease %x: %w", ErrDeliveryInvariant, args.LeaseID, err)
	}
	if header.Deadline <= args.Now || header.Deadline-args.Now > MaxLeaseMs {
		return LeaseView{}, nil
	}
	if header.Count > MaxClaimLimit {
		return LeaseView{}, fmt.Errorf("%w: lease %x holds %d entries and a claim leases at most %d", ErrDeliveryInvariant, args.LeaseID, header.Count, MaxClaimLimit)
	}
	prefix := queue.kindKey(kindClaimed, args.LeaseID[:])
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	entries := make([][]byte, 0, header.Count)
	held := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		value, err := it.Value()
		if err != nil {
			return LeaseView{}, fmt.Errorf("reading a claimed entry of lease %x: %w", args.LeaseID, err)
		}
		_, entry, err := parseClaimedValue(value)
		if err != nil {
			return LeaseView{}, fmt.Errorf("%w: lease %x: %w", ErrDeliveryInvariant, args.LeaseID, err)
		}
		if held += len(entry); held > maxBytes {
			return LeaseView{}, fmt.Errorf("lease %x: %w (more than %d bytes)", args.LeaseID, ErrLeaseTooLarge, maxBytes)
		}
		entries = append(entries, bytes.Clone(entry))
	}
	if err := it.Err(); err != nil {
		return LeaseView{}, fmt.Errorf("walking the entries of lease %x: %w", args.LeaseID, err)
	}
	if uint32(len(entries)) != header.Count {
		return LeaseView{}, fmt.Errorf("%w: lease %x has %d claimed entries and its header says %d", ErrDeliveryInvariant, args.LeaseID, len(entries), header.Count)
	}
	// The 'e' keys sort by position, which is the order a fifo claims in; a lifo claims
	// from its tail, so the newest position comes first.
	if ref.Engine == FieldLIFO {
		slices.Reverse(entries)
	}
	return LeaseView{Live: true, Deadline: header.Deadline, Entries: entries}, nil
}
