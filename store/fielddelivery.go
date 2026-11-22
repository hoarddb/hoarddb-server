package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
)

// Claim, ack, release and extend on one document's fifo or lifo store field.
// They are the bucket verbs of delivery.go run against a field instance: the
// same lease, claimed-entry, return-count and dead records, the same return
// step, budget and clock rules, kept under the field's store prefix at inner
// byte NSDelivery. Every verb reads one snapshot and adds its effect to the
// caller's batch, committing nothing, so the record's apply carries it with
// the dedupe entry and the pair record (write section 3.3).
//
// A field that is never claimed holds no delivery key. Nothing here is held
// per bucket or per document.

// kindState and kindClaimPosition are the field-only records beside the bucket
// ones: the delivery counters, and the position of the last claim applied.
const (
	kindState         byte = 's'
	kindClaimPosition byte = 'p'
)

// ErrFieldNotClaimable is a claim verb on a store field that is not a fifo or
// a lifo.
var ErrFieldNotClaimable = errors.New("the store field is not a fifo or a lifo")

// FieldClaimArgs is one claim on a field: the bucket claim's input and the
// position of the record that carries it.
type FieldClaimArgs struct {
	ClaimArgs
	// Position is the record's [epoch:8][seq:8]. A claim at or below the
	// position of the last claim applied on the field is a replay of a record
	// already applied, and leases nothing, however long ago its lease expired.
	Position [16]byte
}

// fieldDeliveryState is the delivery counters of a field instance: the part of
// BucketState a field has no other place for. The cursors and the count are the
// instance state's own (fieldkey.go).
type fieldDeliveryState struct {
	Leases       int64  `bson:"leases,omitempty"`
	Pending      int64  `bson:"pending,omitempty"`
	PendingBytes int64  `bson:"pending_bytes,omitempty"`
	Returned     int64  `bson:"returned,omitempty"`
	Dead         int64  `bson:"dead,omitempty"`
	DeadBytes    int64  `bson:"dead_bytes,omitempty"`
	DeadSeq      uint64 `bson:"dead_seq,omitempty"`
	LeaseClock   int64  `bson:"lease_clock,omitempty"`
}

// fieldQueue is the deliveryEngine of one field instance. It embeds a
// QueueStore whose entry prefix is the field's, so the queue walk is the
// bucket queue's.
type fieldQueue struct {
	*QueueStore
	snap  storage.KVSnapshot
	batch storage.KVBatch
	ref   FieldRef
}

func newFieldQueue(snap storage.KVSnapshot, batch storage.KVBatch, ref FieldRef) (*fieldQueue, error) {
	mode := QueueFIFO
	switch ref.Engine {
	case FieldFIFO:
	case FieldLIFO:
		mode = QueueLIFO
	default:
		return nil, fmt.Errorf("%w: it is a %s", ErrFieldNotClaimable, ref.Engine)
	}
	slot, databaseID, id := bucketOfOwner(ref.Owner)
	queue := &QueueStore{slot: slot, databaseID: databaseID, id: id, mode: mode,
		entry: FieldElementKey(ref.StorePrefix(), storage.NSQueue, nil)}
	return &fieldQueue{QueueStore: queue, snap: snap, batch: batch, ref: ref}, nil
}

func (f *fieldQueue) kindKey(kind byte, parts ...[]byte) []byte {
	key := FieldElementKey(f.ref.StorePrefix(), storage.NSDelivery, []byte{kind})
	for _, part := range parts {
		key = append(key, part...)
	}
	return key
}

// wroteBack moves the cursor onto a returned entry. It never arms the
// remembered cursor: a plain field pop does not maintain one, so a field would
// otherwise strand entries when claim and pop mix.
func (f *fieldQueue) wroteBack(st *BucketState, origin []byte) {
	position := binary.BigEndian.Uint64(origin)
	if f.mode == QueueLIFO {
		st.Tail = max(st.Tail, position)
		return
	}
	st.Head = min(st.Head, position)
}

// apply loads the instance's state, runs the verb and adds the state back to
// the batch. The ApplyInfo of a bucket apply has no meaning here: the record's
// position gate is the write path's.
func (f *fieldQueue) apply(_ ApplyInfo, run func(storage.KVSnapshot, *BucketState, storage.KVBatch) error) error {
	stateKey := FieldStateKey(f.ref.StorePrefix())
	instance, instanceExisted, err := loadFieldState(f.snap, stateKey)
	if err != nil {
		return err
	}
	var delivery fieldDeliveryState
	deliveryExisted, err := f.loadDelivery(&delivery)
	if err != nil {
		return err
	}
	st := BucketState{Head: instance.Head, Tail: instance.Tail, Next: instance.Next, Entries: int64(instance.Count),
		Leases: delivery.Leases, Pending: delivery.Pending, PendingBytes: delivery.PendingBytes, Returned: delivery.Returned,
		Dead: delivery.Dead, DeadBytes: delivery.DeadBytes, DeadSeq: delivery.DeadSeq, LeaseClock: delivery.LeaseClock}
	if err := run(f.snap, &st, f.batch); err != nil {
		return err
	}
	if instanceExisted || st.Entries != 0 || st.Head != 0 || st.Tail != 0 || st.Next != 0 {
		next := FieldInstanceState{Head: st.Head, Tail: st.Tail, Next: st.Next, Count: uint64(st.Entries)}
		if err := f.batch.Set(stateKey, EncodeFieldInstanceState(next)); err != nil {
			return fmt.Errorf("writing the field-store instance state: %w", err)
		}
	}
	return f.saveDelivery(deliveryExisted, fieldDeliveryState{Leases: st.Leases, Pending: st.Pending, PendingBytes: st.PendingBytes,
		Returned: st.Returned, Dead: st.Dead, DeadBytes: st.DeadBytes, DeadSeq: st.DeadSeq, LeaseClock: st.LeaseClock})
}

func (f *fieldQueue) loadDelivery(into *fieldDeliveryState) (bool, error) {
	encoded, err := f.snap.Get(f.kindKey(kindState))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reading the delivery state of a field store: %w", err)
	}
	if err := bson.Unmarshal(encoded, into); err != nil {
		return false, fmt.Errorf("%w: the delivery state of a field store: %w", ErrDeliveryInvariant, err)
	}
	return true, nil
}

func (f *fieldQueue) saveDelivery(existed bool, state fieldDeliveryState) error {
	key := f.kindKey(kindState)
	if state == (fieldDeliveryState{}) {
		if !existed {
			return nil
		}
		if err := f.batch.Delete(key); err != nil {
			return fmt.Errorf("deleting the delivery state of a field store: %w", err)
		}
		return nil
	}
	encoded, err := bson.Marshal(state)
	if err != nil {
		return fmt.Errorf("encoding the delivery state of a field store: %w", err)
	}
	if err := f.batch.Set(key, encoded); err != nil {
		return fmt.Errorf("writing the delivery state of a field store: %w", err)
	}
	return nil
}

// claimOutcomeBytes is the width of the 'p' value: the position of the last
// claim applied, the lease id it made (zero when it leased nothing, 16 bytes
// without the store's tag slot) and the deadline (zero likewise).
const claimOutcomeBytes = 16 + 16 + 8

// claimOutcome reads the last claim's recorded outcome; position is zero when
// no claim carrying a position was applied.
func (f *fieldQueue) claimOutcome() (position [16]byte, claim Claim, err error) {
	encoded, err := f.snap.Get(f.kindKey(kindClaimPosition))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return position, Claim{}, nil
	}
	if err != nil {
		return position, Claim{}, fmt.Errorf("reading the last claim outcome of a field store: %w", err)
	}
	if len(encoded) != claimOutcomeBytes {
		return position, Claim{}, fmt.Errorf("%w: the last claim outcome is %d bytes, want %d", ErrDeliveryInvariant, len(encoded), claimOutcomeBytes)
	}
	copy(position[:], encoded[:16])
	copy(claim.LeaseID[leaseIDBytes-16:], encoded[16:32])
	claim.Deadline = int64(binary.BigEndian.Uint64(encoded[32:]))
	return position, claim, nil
}

func (f *fieldQueue) recordClaimOutcome(position [16]byte, claim Claim) error {
	encoded := make([]byte, 0, claimOutcomeBytes)
	encoded = append(encoded, position[:]...)
	encoded = append(encoded, claim.LeaseID[leaseIDBytes-16:]...)
	encoded = binary.BigEndian.AppendUint64(encoded, uint64(claim.Deadline))
	if err := f.batch.Set(f.kindKey(kindClaimPosition), encoded); err != nil {
		return fmt.Errorf("writing the last claim outcome of a field store: %w", err)
	}
	return nil
}

// FieldClaim leases up to args.N elements of the field under a new lease. A
// record whose position is at or below the last claim applied leases nothing
// and leases nothing (Claim.Replayed): at exactly the last position it answers
// the outcome recorded there, zeros for a claim that leased nothing, however
// much has arrived since; an older position answers zeros. The empty-field and refused-entry answers are the
// bucket claim's.
func FieldClaim(snap storage.KVSnapshot, batch storage.KVBatch, ref FieldRef, args FieldClaimArgs, visit func(entry []byte) error) (Claim, error) {
	queue, err := newFieldQueue(snap, batch, ref)
	if err != nil {
		return Claim{}, fmt.Errorf("claiming a field store: %w", err)
	}
	if args.Position != ([16]byte{}) {
		last, recorded, err := queue.claimOutcome()
		if err != nil {
			return Claim{}, err
		}
		switch order := bytes.Compare(args.Position[:], last[:]); {
		case order == 0:
			recorded.Replayed = true
			return recorded, nil
		case order < 0:
			// A claim older than the last one: its outcome is not kept here (the
			// dedupe entry holds it), and leasing again would double-apply it.
			return Claim{Replayed: true}, nil
		}
	}
	claim, err := claimN(queue, ApplyInfo{}, args.ClaimArgs, visit)
	if err != nil {
		return Claim{}, fmt.Errorf("claiming field store elements (limit %d): %w", args.N, err)
	}
	if args.Position != ([16]byte{}) {
		outcome := Claim{}
		if len(claim.Items) > 0 {
			outcome = Claim{LeaseID: claim.LeaseID, Deadline: claim.Deadline}
		}
		if err := queue.recordClaimOutcome(args.Position, outcome); err != nil {
			return Claim{}, err
		}
	}
	return claim, nil
}

// FieldAck acknowledges every element of a lease on the field. An unknown or
// expired lease is a result, not an error.
func FieldAck(snap storage.KVSnapshot, batch storage.KVBatch, ref FieldRef, args LeaseArgs) (LeaseResult, error) {
	queue, err := newFieldQueue(snap, batch, ref)
	if err != nil {
		return LeaseResult{}, fmt.Errorf("acknowledging on a field store: %w", err)
	}
	result, err := ack(queue, ApplyInfo{}, args, nil)
	if err != nil {
		return LeaseResult{}, fmt.Errorf("acknowledging lease %x: %w", args.LeaseID, err)
	}
	return result, nil
}

// FieldRelease writes a live lease's elements back to the field by the return
// rule, which counts toward MaxDeliveries.
func FieldRelease(snap storage.KVSnapshot, batch storage.KVBatch, ref FieldRef, args LeaseArgs) (LeaseResult, error) {
	queue, err := newFieldQueue(snap, batch, ref)
	if err != nil {
		return LeaseResult{}, fmt.Errorf("releasing on a field store: %w", err)
	}
	result, err := release(queue, ApplyInfo{}, args)
	if err != nil {
		return LeaseResult{}, fmt.Errorf("releasing lease %x: %w", args.LeaseID, err)
	}
	return result, nil
}

// FieldExtend moves a live lease's deadline to the effective now plus leaseMs.
func FieldExtend(snap storage.KVSnapshot, batch storage.KVBatch, ref FieldRef, args LeaseArgs, leaseMs int64) (LeaseResult, error) {
	queue, err := newFieldQueue(snap, batch, ref)
	if err != nil {
		return LeaseResult{}, fmt.Errorf("extending on a field store: %w", err)
	}
	result, err := extend(queue, ApplyInfo{}, args, leaseMs)
	if err != nil {
		return LeaseResult{}, fmt.Errorf("extending lease %x: %w", args.LeaseID, err)
	}
	return result, nil
}
