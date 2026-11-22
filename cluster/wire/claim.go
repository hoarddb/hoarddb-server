package wire

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// Claim, ack, release and extend as records of the log (write section 10).
// The functions here decode nothing and decide nothing: they turn a logged
// cluster.Op into a call of store:FieldClaim, FieldAck, FieldRelease or
// FieldExtend, which read one snapshot and add their lease, claimed-entry,
// return-count and dead records to the record's batch. The owner stamps the
// clock and the nonce once, before the append; every copy, every replay and a
// new owner after a drain then compose the same lease id and deadline from the
// logged bytes alone.

// leaseTagPad is the four zero bytes in front of a wire lease id: the
// store's lease id has a tag slot that a document field leaves zero.
const leaseTagPad = 4

// errUndeclaredField marks a FieldEngine failure that is the caller's: the bucket declares
// no store under that field id, which a drop or alter between admission and the apply can
// cause. Any other failure is a failed read of the declaration.
var errUndeclaredField = errors.New("the field is not a declared store")

// errRetiredField marks an undeclared field id that an alter retired. A record admitted
// before the alter can reach the apply after it; the field's values are semantically deleted
// (the retired-field sweep removes them), so the apply of such a record writes nothing and
// records an empty result instead of failing after the append, which would fence the pair and
// make every replay of the log fail. Every copy and every replay that sees the id retired
// does the same; one that has not published the alter yet writes under the retired id, and
// that is swept.
var errRetiredField = errors.New("the field was retired")

// FieldEngineSource answers which engine a declared store field has. A record
// carries field ids, never declarations (placement section 4 step 8), so the
// apply learns fifo from lifo here; an engine never changes for a field id.
type FieldEngineSource interface {
	// FieldEngine returns the engine of fieldID in bucketID, or an error that
	// names the field when it is not declared. An undeclared field wraps
	// errUndeclaredField, so an applier can refuse it plainly.
	FieldEngine(db, bucketID uint64, fieldID uint16) (store.FieldEngine, error)
}

// ExpireTableSource is the optional second face of a FieldEngineSource: it answers a bucket's
// expiry declaration, which the owner needs to refuse a push that would never expire and to
// stamp expire items. A copy never asks: the record carries the items.
type ExpireTableSource interface {
	// ExpireTable returns the expiry declaration of bucketID, the zero table when it has none.
	ExpireTable(db, bucketID uint64) (machine.ExpireTable, error)
}

// LeaseTarget is where and under which position a lease operation applies.
type LeaseTarget struct {
	Slot uint16
	DB   uint64
	// Engine is the field's engine; only a fifo or a lifo is claimable.
	Engine store.FieldEngine
	// Position is the record's own position, which lets a claim recognise a
	// record already applied.
	Position cluster.Position
	// Acked, when set, sees each element an ack removes with its queue position; the expiry
	// queue drops its entry for it. Nil for every other caller.
	Acked func(position uint64, element []byte)
	// Expired and Discard make a claim skip and remove the elements past their time; both nil
	// for a queue that expires nothing.
	Expired func(entry []byte) bool
	Discard func(position uint64, entry []byte)
	// Aggregates, when not nil, is told of every element that leaves the field for good in
	// this apply: an ack removes it, or the return step dead-letters it. A leased element
	// stays summed until then (owner ruling 2026-11-05). Nil for a field with no aggregates.
	Aggregates *store.AggregateBatch
	// LeaveBytes and LeaveKeys are the most bytes and keys the aggregates add to the batch for one leaving
	// entry; the return step charges them to the delivery budget for each entry it dead-letters.
	LeaveBytes, LeaveKeys int
}

// ClaimOutcome is what a claim did.
type ClaimOutcome struct {
	// LeaseID and Until are the lease's id and absolute deadline in ms. Both
	// are zero when the field had nothing to lease.
	LeaseID [16]byte
	Until   int64
	// Elements are the leased messages in claim order.
	Elements [][]byte
	// Replayed says the record had already been applied: nothing was leased
	// now, and LeaseID and Until are the ones it composed.
	Replayed bool
}

// Result is the 24 bytes the dedupe entry records for a claim: the lease id
// and the deadline (conventions section 18), so a deduped retry answers the
// same lease.
func (outcome ClaimOutcome) Result() []byte {
	recorded := make([]byte, 0, 24)
	recorded = append(recorded, outcome.LeaseID[:]...)
	return binary.BigEndian.AppendUint64(recorded, uint64(outcome.Until))
}

// LeaseOutcome is the answer of an ack, release or extend.
type LeaseOutcome struct {
	// State is acked, released, extended, expired or unknown. Expired and
	// unknown are results, not errors: the lease is gone, and the messages
	// come back by the return step (write section 10).
	State store.LeaseState
	// Until is the lease's deadline: the new one after an extend.
	Until int64
}

// StampClaim returns op with the owner's clock and a fresh nonce. The owner
// calls it once, before the append; no apply reads a clock or draws a random
// number. Any other class is returned unchanged: StampLease stamps those.
func StampClaim(op cluster.Op, now int64, nonce [16]byte) cluster.Op {
	if op.Class != cluster.OpClaim {
		return op
	}
	op.Now = now
	op.Nonce = nonce
	return op
}

// StampLease returns op with the owner's clock, for an ack, a release or an
// extend. An extend arrives carrying the lease length it asks for in Until, because a
// client cannot know the owner's clock; the stamp turns it into the absolute deadline
// now+length that the log record carries and ApplyLease reads (build SPEC-GAP U67-1). Any
// other class is returned unchanged.
func StampLease(op cluster.Op, now int64) cluster.Op {
	switch op.Class {
	case cluster.OpAck, cluster.OpRelease:
		op.Now = now
	case cluster.OpExtend:
		op.Now, op.Until = now, now+op.Until
	}
	return op
}

// onLeave is the OnLeave of the store call: it subtracts a leaving element from the field's
// aggregates, and is nil when the field keeps none so the store reads no entry value on an ack.
func (target LeaseTarget) onLeave() func(entry []byte) error {
	if target.Aggregates == nil {
		return nil
	}
	return func(entry []byte) error { return aggAdd(target.Aggregates, entry, -1) }
}

func refOf(target LeaseTarget, op cluster.Op) (store.FieldRef, error) {
	if len(op.Fields) != 1 {
		return store.FieldRef{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"operation class 0x%02x names %d store fields; a claim, ack, release or extend names one", op.Class, len(op.Fields))}
	}
	owner := storage.Key(target.Slot, target.DB, op.BucketID, storage.NSFieldStore, op.EncID)
	return store.FieldRef{Owner: owner, FieldID: op.Fields[0].FieldID, Engine: target.Engine}, nil
}

func storeLeaseID(wire [16]byte) [store.LeaseIDBytes]byte {
	var leaseID [store.LeaseIDBytes]byte
	copy(leaseID[leaseTagPad:], wire[:])
	return leaseID
}

func wireLeaseID(stored [store.LeaseIDBytes]byte) [16]byte {
	var leaseID [16]byte
	copy(leaseID[:], stored[leaseTagPad:])
	return leaseID
}

func positionBytes(position cluster.Position) [16]byte {
	var encoded [16]byte
	binary.BigEndian.PutUint64(encoded[0:8], position.Epoch)
	binary.BigEndian.PutUint64(encoded[8:16], position.Seq)
	return encoded
}

// ApplyClaim leases up to op.Count messages (1 to store.MaxClaimLimit) of the
// op's store field, adding the lease records to batch. The lease id is
// [deadline:8][nonce first 8 bytes] and the deadline is op.Now plus
// store.DefaultLeaseMs, both from the logged op alone. A record whose position
// was already applied leases nothing, even after its lease expired.
func ApplyClaim(snap storage.KVSnapshot, batch storage.KVBatch, target LeaseTarget, op cluster.Op) (ClaimOutcome, error) {
	if op.Class != cluster.OpClaim {
		return ClaimOutcome{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf("operation class 0x%02x is not a claim", op.Class)}
	}
	ref, err := refOf(target, op)
	if err != nil {
		return ClaimOutcome{}, err
	}
	args := store.FieldClaimArgs{
		ClaimArgs: store.ClaimArgs{N: min(max(int(op.Count), 1), store.MaxClaimLimit), LeaseMs: store.DefaultLeaseMs, Now: op.Now,
			LeaveBytes: target.LeaveBytes, LeaveKeys: target.LeaveKeys},
		Position: positionBytes(target.Position),
	}
	copy(args.Nonce[:], op.Nonce[:8])
	args.Expired, args.Discard = target.Expired, target.Discard
	var outcome ClaimOutcome
	args.OnLeave = target.onLeave()
	claim, err := store.FieldClaim(snap, batch, ref, args, func(entry []byte) error {
		outcome.Elements = append(outcome.Elements, entry)
		return nil
	})
	if errors.Is(err, store.ErrFieldNotClaimable) {
		return ClaimOutcome{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"field %d is a %s; only fifo and lifo fields can be claimed; pop the heap field instead (or declare the field as a fifoStore or lifoStore). Nothing was leased",
			op.Fields[0].FieldID, target.Engine)}
	}
	if err != nil {
		return ClaimOutcome{}, fmt.Errorf("applying the claim at position %s: %w", target.Position, err)
	}
	if claim.Refused != nil {
		return ClaimOutcome{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"the first message of the field is %d bytes to deliver, over the %d a claim moves. Nothing was leased; take it with a pop and store large payloads in a blobStore",
			claim.Refused.Footprint, claim.Refused.FootprintLimit)}
	}
	if len(claim.Items) > 0 || claim.Replayed {
		outcome.LeaseID, outcome.Until = wireLeaseID(claim.LeaseID), claim.Deadline
	}
	outcome.Replayed = claim.Replayed
	return outcome, nil
}

// ApplyLease applies an ack, a release or an extend. The deadline is compared
// with op.Now, the owner's clock at the append. An extend asks for a lease of
// op.Until-op.Now ms, which must lie within store.MinLeaseMs and
// store.MaxLeaseMs.
func ApplyLease(snap storage.KVSnapshot, batch storage.KVBatch, target LeaseTarget, op cluster.Op) (LeaseOutcome, error) {
	ref, err := refOf(target, op)
	if err != nil {
		return LeaseOutcome{}, err
	}
	args := store.LeaseArgs{LeaseID: storeLeaseID(op.LeaseID), Now: op.Now, Acked: target.Acked, OnLeave: target.onLeave(), LeaveBytes: target.LeaveBytes, LeaveKeys: target.LeaveKeys}
	var result store.LeaseResult
	switch op.Class {
	case cluster.OpAck:
		result, err = store.FieldAck(snap, batch, ref, args)
	case cluster.OpRelease:
		result, err = store.FieldRelease(snap, batch, ref, args)
	case cluster.OpExtend:
		length := op.Until - op.Now
		if length < store.MinLeaseMs || length > store.MaxLeaseMs {
			return LeaseOutcome{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
				"extend asks for a lease of %d ms; a lease is %d to %d ms. Nothing was changed; send a deadline within that range of now",
				length, store.MinLeaseMs, store.MaxLeaseMs)}
		}
		result, err = store.FieldExtend(snap, batch, ref, args, length)
	default:
		return LeaseOutcome{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf("operation class 0x%02x is not an ack, release or extend", op.Class)}
	}
	if err != nil {
		return LeaseOutcome{}, fmt.Errorf("applying class 0x%02x at position %s: %w", op.Class, target.Position, err)
	}
	return LeaseOutcome{State: result.State, Until: result.Deadline}, nil
}
