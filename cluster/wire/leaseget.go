package wire

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// leaseReplyBytes is the most message bytes one lease read returns, the same bound a peek
// has: the lease holds up to store.MaxClaimLimit messages of up to 1 MiB each, so without
// it a lease could build a reply the frame cannot carry.
const leaseReplyBytes = peekReplyBytes

// LeaseField is a lease to read: the document its field belongs to, the field's id and
// engine (resolved from the declaration by the caller) and the lease id a claim returned.
type LeaseField struct {
	Request ReadRequest
	FieldID uint16
	Engine  store.FieldEngine
	// Lease is the 16-byte lease id of a claim.
	Lease [16]byte
}

// LeaseRead is what a read of a lease found.
type LeaseRead struct {
	// Found is true when the lease is live on this field of this document. It is false for a
	// lease that is unknown there, acked, released or past its deadline, and for a document
	// that does not exist: an answer, not an error.
	Found bool
	// Until is the live lease's deadline in Unix milliseconds.
	Until int64
	// Elements are the messages held under the lease, in claim order.
	Elements [][]byte
}

func (r *Reader) wallMillis() int64 {
	if r.cfg.WallMillis != nil {
		return r.cfg.WallMillis()
	}
	return time.Now().UnixMilli()
}

// LeaseElements returns the messages held under a lease of a fifo or lifo field, from the
// owner's own store. It takes no position and writes nothing, and it does not run the return
// step: a lease that has run out but whose messages no write has returned yet is already
// not found. The gate is the one a peek goes through, checked before and after the snapshot,
// so a lease, hold, owner or declaration that moved under the read is refused. A node that
// is not the owner forwards the read to the owner and a held pair parks it (BUG-0766).
func (r *Reader) LeaseElements(ctx context.Context, field LeaseField) (LeaseRead, error) {
	return routeField(ctx, r, fieldTarget{Request: field.Request, FieldID: field.FieldID, Engine: field.Engine},
		func(snapshot storage.KVSnapshot, ref store.FieldRef, admitted ReadVerdict) (LeaseRead, error) {
			return r.readLease(snapshot, ref, field)
		},
		func() (LeaseRead, error) { return LeaseRead{}, nil },
		func(ctx context.Context, owner string, hopped ReadRequest) (LeaseRead, error) {
			answer, err := r.cfg.Forward.ForwardFieldRead(ctx, owner, FieldForward{Req: hopped, Op: FieldOpLease, FieldID: field.FieldID, Engine: field.Engine, Lease: field.Lease})
			return answer.Lease, err
		})
}

func (r *Reader) readLease(snapshot storage.KVSnapshot, ref store.FieldRef, field LeaseField) (LeaseRead, error) {
	req := field.Request
	limit := r.replyBytes(req)
	view, err := store.FieldLeaseElements(snapshot, ref, store.LeaseArgs{LeaseID: storeLeaseID(field.Lease), Now: r.wallMillis()}, limit)
	if errors.Is(err, store.ErrLeaseTooLarge) {
		return LeaseRead{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"lease %x of field %d of %s holds more than the %d bytes one reply carries. Nothing was read. Release the lease with 'release' and claim fewer messages",
			field.Lease, field.FieldID, req.idText(), limit)}
	}
	if err != nil {
		return LeaseRead{}, fmt.Errorf("reading lease %x of field %d of %s: %w", field.Lease, field.FieldID, req.idText(), err)
	}
	return LeaseRead{Found: view.Live, Until: view.Deadline, Elements: view.Entries}, nil
}

// LeaseElementsRequest names a lease to read.
type LeaseElementsRequest struct {
	StoreTarget
	// Lease is the id Claim returned.
	Lease [16]byte
}

// LeaseElements returns the messages a lease still holds, in claim order, so a client that
// lost the reply of a claim reads them back after the retry returned the lease id. The lease
// is found only while it is live: acked, released and expired leases, and leases of another
// field or document, answer Found false. It is a read: no position, no log record.
func (n *Node) LeaseElements(ctx context.Context, req LeaseElementsRequest) (LeaseRead, error) {
	call, err := n.fieldCall(ctx, req.StoreTarget, "lease_get")
	if err != nil {
		return LeaseRead{}, err
	}
	return n.leaseElementsOf(ctx, call, req.ID, req.Lease)
}

func (n *Node) leaseElementsOf(ctx context.Context, call fieldCall, id string, lease [16]byte) (LeaseRead, error) {
	slot := call.ref.slot(call.encID)
	n.own.Acquire(slot, call.ref.dbID)
	found, err := n.reader.LeaseElements(ctx, LeaseField{
		Request: ReadRequest{Slot: slot, DBID: call.ref.dbID, BucketID: call.ref.entry.Decl.BucketID, EncID: call.encID, ID: id},
		FieldID: call.fieldID, Engine: call.engine, Lease: lease,
	})
	if err != nil {
		return LeaseRead{}, fmt.Errorf("reading a lease of %s.%s of %q: %w", call.ref.name, call.fieldName(), id, err)
	}
	return found, nil
}
