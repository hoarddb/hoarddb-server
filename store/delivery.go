package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/storage"
)

// The delivery verbs shared by the queue and the heap (claim-ack-spec §4): claim,
// ack, release and extend, the return step every one of them and pop runs
// first, and dead-lettering. Each is one Apply with Keyed false, as pop is, and
// reads the clock only from its stamped Now, so every copy of a logged entry
// returns the same leases, composes the same lease and moves the same entries
// (CA-13, CA-17).

// ErrDeliveryInvariant is the bucket's delivery records disagreeing with each
// other or with a write the apply is about to make: a composed lease id that
// already has a header, a lease whose 'e' keys are not the count its header
// holds. No write of these verbs leaves either behind; it is a tripwire, and a
// refusal, never an overwrite.
var ErrDeliveryInvariant = errors.New("the bucket's delivery records disagree")

// ClaimArgs is one claim's input (claim-ack-spec §4.1).
type ClaimArgs struct {
	// N is how many entries to claim, at most MaxClaimLimit.
	N int
	// LeaseMs is the lease's length, within MinLeaseMs and MaxLeaseMs.
	LeaseMs int64
	// Now is the owner's stamped time in ms (lease_now).
	Now int64
	// Nonce is the owner's stamped entropy (lease_nonce).
	Nonce [8]byte
	// NameHash is BucketNameHash of the bucket: the store knows its slot and
	// id, which are a node's own, and the lease's tag must be the same on
	// every copy.
	NameHash uint64
	// Expired, when set, tells a claim of a field store which entries are past their
	// time: each is removed instead of leased, counts toward N, and goes to Discard with its
	// position. A nil Expired is the claim as it was.
	Expired func(entry []byte) bool
	// Discard sees each element Expired removed, so the caller drops what it keeps for it.
	Discard func(position uint64, entry []byte)
	// OnLeave is called once per entry that leaves the store for good during this apply:
	// each entry the return step moves to the dead list. A non-nil error stops the apply.
	// Nil for a bucket-level queue.
	OnLeave func(entry []byte) error
	// LeaveBytes and LeaveKeys are the most bytes and keys OnLeave adds to the batch for one entry; the
	// return step charges them to the delivery budget for each entry it dead-letters, so what a return
	// writes through OnLeave stays inside the batch bound.
	LeaveBytes, LeaveKeys int
}

// LeaseArgs names one outstanding lease, for ack, release and extend.
type LeaseArgs struct {
	LeaseID  [leaseIDBytes]byte
	Now      int64
	NameHash uint64
	// Acked, when set, is called by an ack of a field store for each element it removes, with
	// the element's queue position and bytes, so the caller can drop what it keeps per element
	// (the expiry queue). Nil reads no element value: the walk stays keys only.
	Acked func(position uint64, element []byte)
	// OnLeave is called once per entry that leaves the store for good during this apply:
	// each entry an ack removes and each entry the return step or a release moves to the
	// dead list, never an entry written back to the queue. A non-nil error stops the apply.
	// Nil for a bucket-level queue, which then reads no entry value on an ack.
	OnLeave func(entry []byte) error
	// LeaveBytes and LeaveKeys are as in ClaimArgs.
	LeaveBytes, LeaveKeys int
}

// DeliveryReport is what a delivery apply did besides its verb's own work
// (build doc B-3), for the answer, the metrics and the sync decision.
type DeliveryReport struct {
	// Returned is how many entries the return step wrote back to the queue.
	Returned int
	// DeadLettered is how many entries the apply moved to the dead list.
	DeadLettered int
	// Collided is how many of DeadLettered died of DeadReasonCollided; the rest
	// died of DeadReasonMaxDeliveries. The server counts them by reason.
	Collided int
	// Wrote is false only when the apply changed nothing but the mark and the
	// state document's bookkeeping: no entry moved and no lease changed.
	Wrote bool
	// ClockStep is how far, in ms, the stamped time fell behind the bucket's
	// lease clock when it was further than the tolerance, so the stamp won;
	// 0 otherwise (claim-ack-spec §4.8).
	ClockStep int64
}

// PopResult is what PopN did (build doc B-3): how many entries it removed, and
// what its return step did first.
type PopResult struct {
	Removed  int
	Delivery DeliveryReport
	// Pending and Dead are the bucket's counts after the apply, from the state
	// the apply holds.
	Pending, Dead int64
}

// ClaimedItem is one entry a claim moved under its lease.
type ClaimedItem struct {
	// ItemID is the entry's origin suffix: what ack names it by.
	ItemID []byte
	// Deliveries is how many times the entry has now been claimed.
	Deliveries int
}

// ClaimRefusal is a claim's first entry over the hard cap (claim-ack-spec §4.4
// Edge 2), named so the server can say which entry and why. It is a result,
// not an error, so the apply commits whatever the return step returned (Q-2).
type ClaimRefusal struct {
	// Footprint is the entry's delivery footprint in bytes, and FootprintLimit
	// the most a claim moves.
	Footprint, FootprintLimit int64
	// EntryID is a heap element's id; empty for a queue.
	EntryID string
	// IDTooLong says the heap id, not the footprint, refused the entry.
	IDTooLong bool
}

// Claim is what ClaimN did.
type Claim struct {
	// LeaseID and Deadline are the new lease's, set only when Items is not
	// empty.
	LeaseID  [leaseIDBytes]byte
	Deadline int64
	// Items are the claimed entries in walk order; the server's visitor saw
	// their bytes in the same order.
	Items []ClaimedItem
	// Refused is set when the first entry was over the hard cap.
	Refused *ClaimRefusal
	// Pending and Dead are the bucket's counts after the apply.
	Pending, Dead int64
	Delivery      DeliveryReport
	// Replayed is set by a field claim that found its record already applied
	// (FieldClaimArgs.Position): LeaseID and Deadline are the lease it made, and
	// nothing was leased now.
	Replayed bool
}

// LeaseState is the answer a lease verb gives about its lease.
type LeaseState string

// The lease states of claim-ack-spec §9.2.
const (
	LeaseAcked    LeaseState = "acked"
	LeaseReleased LeaseState = "released"
	LeaseExtended LeaseState = "extended"
	LeaseExpired  LeaseState = "expired"
	LeaseUnknown  LeaseState = "unknown"
)

// LeaseResult is what an ack, a release or an extend did.
type LeaseResult struct {
	State LeaseState
	// Acked and Missing count an ack's item ids; Remaining is what is still
	// under the lease after it.
	Acked, Missing, Remaining int
	// Released counts the entries a release took off its lease.
	Released int
	// Deadline is the lease's deadline: the new one after an extend, the one
	// that passed for an expired lease.
	Deadline int64
	Delivery DeliveryReport
}

// opScope is what Apply hands an operation.
type opScope struct {
	snap  storage.KVSnapshot
	st    *BucketState
	batch storage.KVBatch
}

// takeWalk is one take walk: up to n entries in pop order, each handed to visit
// with its 0x04 key and then to take.
type takeWalk struct {
	n         int
	allowStop bool
	visit     func(key, entry []byte) error
	take      func(st *BucketState, batch storage.KVBatch, key, entry []byte) error
	// over and loaded are a delivery apply's. While over holds written-back
	// keys the walk merges them with the snapshot (claim-ack-spec §4.3), and
	// loaded is the state as the apply loaded it, before the return step moved
	// the cursor: the snapshot side starts at the loaded cursor, where the
	// snapshot's live keys are.
	over   *overlay
	loaded BucketState
}

// deliveryEngine is what the shared delivery logic needs of a queue or a heap.
type deliveryEngine interface {
	bucketKey() (slot uint16, databaseID, id uint64)
	deliveryLayout() claimLayout
	apply(info ApplyInfo, op func(storage.KVSnapshot, *BucketState, storage.KVBatch) error) error
	walkTake(scope opScope, w takeWalk) (int, error)
	// removeEntry adds the delete of a taken 0x04 entry and its bookkeeping.
	removeEntry(st *BucketState, batch storage.KVBatch, key, entry []byte) error
	// wroteBack applies the cursor rule for a live 0x04 key put back at its
	// origin (claim-ack-spec §4.6).
	wroteBack(st *BucketState, origin []byte)
}

// deliverySpend is an apply's delivery budget (claim-ack-spec §4.4): what it has
// charged, against the limit, and whether its one exempt unit is written.
type deliverySpend struct {
	limit, spent deliveryCharge
	unitWritten  bool
}

func newDeliverySpend() deliverySpend {
	return deliverySpend{limit: deliveryCharge{Bytes: deliveryBudgetBytes, Keys: deliveryBudgetKeys}}
}

// admit charges one unit and reports whether it may be written: the apply's
// first unit always (Edge 1: exemptions do not stack, and it is charged, so
// what follows it sees what is left), any later one only inside the limit.
func (s *deliverySpend) admit(cost deliveryCharge) bool {
	over := s.spent.Bytes+cost.Bytes > s.limit.Bytes || s.spent.Keys+cost.Keys > s.limit.Keys
	if s.unitWritten && over {
		return false
	}
	s.unitWritten = true
	s.spent.Bytes += cost.Bytes
	s.spent.Keys += cost.Keys
	return true
}

// deliveryOp is one delivery apply in progress: the scope Apply opened, the
// state as loaded, the effective now, the budget, the overlay, and what it has
// done.
type deliveryOp struct {
	engine     deliveryEngine
	scope      opScope
	loaded     BucketState
	slot       uint16
	databaseID uint64
	id         uint64
	layout     claimLayout
	now        int64
	spend      deliverySpend
	over       overlay
	report     DeliveryReport
	// field is set when the apply works on one document's store field rather
	// than on a queue bucket; the key scope and the lease id differ (delivery_scope.go).
	field *fieldQueue
	// onLeave is set from the verb's args; see LeaseArgs.OnLeave.
	onLeave func(entry []byte) error
	// leaveBytes and leaveKeys are what onLeave may add to the batch per entry.
	leaveBytes, leaveKeys int
}

func newDeliveryOp(engine deliveryEngine, scope opScope) *deliveryOp {
	slot, databaseID, id := engine.bucketKey()
	op := &deliveryOp{engine: engine, scope: scope, loaded: *scope.st, slot: slot, databaseID: databaseID, id: id, layout: engine.deliveryLayout(), spend: newDeliverySpend()}
	op.field, _ = engine.(*fieldQueue)
	return op
}

// startClock sets the effective now of stamped (claim-ack-spec §4.2 step 1).
func (op *deliveryOp) startClock(stamped int64) {
	now, stepped := effectiveNow(stamped, op.scope.st)
	if stepped {
		op.report.ClockStep = op.scope.st.LeaseClock - stamped
	}
	op.now = now
}

// walk is a take walk over this apply's overlay.
func (op *deliveryOp) walk(n int, allowStop bool, visit func(key, entry []byte) error,
	take func(st *BucketState, batch storage.KVBatch, key, entry []byte) error) takeWalk {
	return takeWalk{n: n, allowStop: allowStop, visit: visit, take: take, over: &op.over, loaded: op.loaded}
}

// settleClock is §4.2 step 4: the lease clock is the effective now while a
// lease is out, and 0 when none is.
func (op *deliveryOp) settleClock() {
	if op.scope.st.Leases > 0 {
		op.scope.st.LeaseClock = op.now
		return
	}
	op.scope.st.LeaseClock = 0
}

func (op *deliveryOp) set(key, value []byte, what string) error {
	if err := op.scope.batch.Set(key, value); err != nil {
		return fmt.Errorf("writing %s: %w", what, err)
	}
	return nil
}

func (op *deliveryOp) delete(key []byte, what string) error {
	if err := op.scope.batch.Delete(key); err != nil {
		return fmt.Errorf("deleting %s: %w", what, err)
	}
	return nil
}

// claimCharge is what claiming one entry adds to the batch: the 0x04 delete,
// the 'e' set, the reserve for its return (§4.4 C-1), and the delete of its
// 'c' record when the bucket has returned entries.
func claimCharge(keyLen, originLen, entryLen int, dropsCount bool) deliveryCharge {
	var cost deliveryCharge
	cost.add(keyLen, 0)
	cost.add(entryKeyBytes+originLen, binary.MaxVarintLen64+entryLen)
	cost.add(returnReserve(originLen, entryLen))
	if dropsCount {
		cost.add(returnCountKeyBytes+originLen, 0)
	}
	return cost
}

// returnReserve is the most a return writes for one claimed entry besides
// deleting its 'e' key: the larger of its two records, an 'x' with its dead
// sequence and, on a heap, its id again inside the record, over a 'c'
// (claim-ack-spec §4.4, C-1). With it charged at the claim, the return of any
// lease fits the budget its claim fitted.
func returnReserve(originLen, entryLen int) (keyLen, valueLen int) {
	return returnReserveKeyBytes + originLen, entryLen + originLen + deadWrapperBytes
}

// returnCountKeyBytes is a 'c' key without its origin suffix.
const returnCountKeyBytes = scopeBytes + 1 + 1

// returnReserveKeyBytes is an 'x' key without its origin suffix: the bucket
// prefix, the namespace and kind bytes, and a heap's dead sequence.
const returnReserveKeyBytes = scopeBytes + 1 + 1 + 8

// claimTaker is a claim's per-entry work, the composed visitor of build doc
// B-4: the store's checks first, then the server's visitor, then the move.
type claimTaker struct {
	expired func(entry []byte) bool
	discard func(position uint64, entry []byte)
	// want is how many live entries the claim leases; the walk stops before the next live one.
	want  int
	op    *deliveryOp
	lease [leaseIDBytes]byte
	visit func(entry []byte) error
	claim *Claim
	bytes int64
}

// check runs before the server's visitor: the hard cap, then the budget. An
// entry that fails either ends the walk short; the first entry over the hard
// cap is named in the claim, which takes nothing (Q-2).
func (c *claimTaker) check(key, entry []byte) error {
	if c.expired != nil && c.expired(entry) {
		// Not leased and not shown to the server's visitor: move removes it.
		return nil
	}
	if c.expired != nil && len(c.claim.Items) >= c.want {
		return ErrStopShort
	}
	origin := c.op.originOf(key)
	if refusal := claimable(c.op.layout, len(entry), len(origin)); refusal != claimOK {
		if len(c.claim.Items) == 0 {
			c.claim.Refused = c.op.refusalOf(refusal, origin, len(entry))
		}
		return ErrStopShort
	}
	if !c.op.spend.admit(claimCharge(len(key), len(origin), len(entry), c.op.scope.st.Returned > 0)) {
		return ErrStopShort
	}
	return c.visit(entry)
}

// move is the per-entry take: the 0x04 entry leaves the queue and its bytes go
// under 'e', keyed by the lease and the origin suffix, with the number of
// times it has been returned, which its 'c' record held.
func (c *claimTaker) move(st *BucketState, batch storage.KVBatch, key, entry []byte) error {
	returns, err := c.op.dropReturnCount(key)
	if err != nil {
		return err
	}
	if err := c.op.engine.removeEntry(st, batch, key, entry); err != nil {
		return err
	}
	if c.expired != nil && c.expired(entry) {
		// An expired element leaves the store for good, so a maintained aggregate must drop it too.
		if err := c.op.leaveEntry(entry); err != nil {
			return err
		}
		if c.discard != nil {
			c.discard(binary.BigEndian.Uint64(c.op.originOf(key)), bytes.Clone(entry))
		}
		return nil
	}
	origin := c.op.originOf(key)
	claimed := c.op.claimedKey(c.lease, origin)
	value := claimedValue(returns, entry)
	if err := c.op.set(claimed, value, "a claimed entry"); err != nil {
		return err
	}
	size := int64(len(claimed) + len(value))
	c.bytes += size
	st.PendingBytes += size
	st.LogicalBytes += size
	c.claim.Items = append(c.claim.Items, ClaimedItem{ItemID: append([]byte(nil), origin...), Deliveries: int(returns) + 1})
	return nil
}

func (op *deliveryOp) refusalOf(refusal claimRefusal, origin []byte, entryLen int) *ClaimRefusal {
	named := &ClaimRefusal{
		Footprint:      claimFootprint(op.layout, entryLen, len(origin)),
		FootprintLimit: maxClaimEntryBytes,
		IDTooLong:      refusal != claimFootprintTooLarge,
	}
	if op.layout == claimLayoutHeap {
		named.EntryID = string(origin[8:])
	}
	return named
}

// claim is ClaimN's operation after the clock (claim-ack-spec §4.9). The empty
// check follows the return step, never precedes it: a bucket whose every entry
// sits under an expired lease must not answer empty.
func (op *deliveryOp) claim(args ClaimArgs, visit func(entry []byte) error, claim *Claim) error {
	st := op.scope.st
	if err := op.returnStep(); err != nil {
		return err
	}
	if st.Entries == 0 {
		return nil
	}
	deadline := op.now + args.LeaseMs
	lease := op.composeLease(args, deadline)
	if err := op.requireNewLease(lease); err != nil {
		return err
	}
	taker := &claimTaker{op: op, lease: lease, visit: visit, claim: claim, expired: args.Expired, discard: args.Discard, want: args.N}
	// An expired element is removed, not leased, and does not count toward N, as it does not in a pop:
	// the walk may take up to MaxExpiredPerVerb of them besides the N it leases.
	walkN := args.N
	if args.Expired != nil {
		walkN += MaxExpiredPerVerb
	}
	if _, err := op.engine.walkTake(op.scope, op.walk(walkN, true, taker.check, taker.move)); err != nil {
		return err
	}
	if len(claim.Items) == 0 {
		return nil
	}
	header := leaseHeader{Deadline: deadline, Count: uint32(len(claim.Items)), Bytes: taker.bytes}
	if err := op.set(op.leaseHeaderKey(lease), header.encode(), "a lease header"); err != nil {
		return err
	}
	if err := op.set(op.deadlineKey(deadline, lease), nil, "a lease deadline"); err != nil {
		return err
	}
	st.Leases++
	st.Pending += int64(len(claim.Items))
	claim.LeaseID, claim.Deadline = lease, deadline
	op.report.Wrote = true
	return nil
}

// requireNewLease refuses a composed lease id that already has a header: a
// nonce collision, 2^-64 per pair of leases with one deadline (§4.9).
func (op *deliveryOp) requireNewLease(lease [leaseIDBytes]byte) error {
	if op.scope.st.Leases == 0 {
		return nil
	}
	_, err := op.scope.snap.Get(op.leaseHeaderKey(lease))
	switch {
	case errors.Is(err, storage.ErrKeyNotFound):
		return nil
	case err != nil:
		return fmt.Errorf("reading the header of lease %x: %w", lease, err)
	}
	return fmt.Errorf("%w: lease %x is composed a second time", ErrDeliveryInvariant, lease)
}

// namedLease reads the header of the lease a verb names, from the snapshot:
// claim-ack-spec §4.3's view rule, so a lease the return step returned in this
// very apply is still found, with its passed deadline, and answers expired. It
// answers unknown for no header or another bucket's tag, and "" for a live
// lease. It answers expired for a deadline at or before the effective now, and
// for one further ahead than MaxLeaseMs, which the return step's forward-jump
// guard returns (§4.11): the guard may have returned it in this apply, and a
// verb that acted on it as live would count its entries a second time.
func (op *deliveryOp) namedLease(args LeaseArgs) (leaseHeader, LeaseState, error) {
	encoded, err := op.scope.snap.Get(op.leaseHeaderKey(args.LeaseID))
	switch {
	case errors.Is(err, storage.ErrKeyNotFound):
		return leaseHeader{}, LeaseUnknown, nil
	case err != nil:
		return leaseHeader{}, "", fmt.Errorf("reading the header of lease %x: %w", args.LeaseID, err)
	}
	if op.field == nil && binary.BigEndian.Uint32(args.LeaseID[0:4]) != leaseTag(args.NameHash, op.scope.st.Incarnation) {
		return leaseHeader{}, LeaseUnknown, nil
	}
	header, err := decodeLeaseHeader(encoded)
	if err != nil {
		return leaseHeader{}, "", fmt.Errorf("%w: lease %x: %w", ErrDeliveryInvariant, args.LeaseID, err)
	}
	if header.Deadline <= op.now || header.Deadline-op.now > MaxLeaseMs {
		return header, LeaseExpired, nil
	}
	return header, "", nil
}

// dropLease deletes a lease's header and deadline.
func (op *deliveryOp) dropLease(lease [leaseIDBytes]byte, header leaseHeader) error {
	if err := op.delete(op.leaseHeaderKey(lease), "a lease header"); err != nil {
		return err
	}
	if err := op.delete(op.deadlineKey(header.Deadline, lease), "a lease deadline"); err != nil {
		return err
	}
	op.scope.st.Leases--
	return nil
}

// ackItems acknowledges the named entries of a live lease. The ids are made
// distinct before any Get: the Gets read the snapshot, which still holds a key
// this apply has already deleted, so an id named twice would count twice.
func (op *deliveryOp) ackItems(lease [leaseIDBytes]byte, header leaseHeader, itemIDs [][]byte, result *LeaseResult, acked func(position uint64, element []byte)) error {
	st := op.scope.st
	seen := make(map[string]bool, len(itemIDs))
	for _, itemID := range itemIDs {
		if seen[string(itemID)] {
			continue
		}
		seen[string(itemID)] = true
		key := op.claimedKey(lease, itemID)
		value, err := op.scope.snap.Get(key)
		if errors.Is(err, storage.ErrKeyNotFound) {
			result.Missing++
			continue
		}
		if err != nil {
			return fmt.Errorf("reading claimed entry %x of lease %x: %w", itemID, lease, err)
		}
		if acked != nil && op.field != nil {
			_, entry, err := parseClaimedValue(value)
			if err != nil {
				return fmt.Errorf("%w: lease %x: %w", ErrDeliveryInvariant, lease, err)
			}
			acked(binary.BigEndian.Uint64(itemID), bytes.Clone(entry))
		}
		if err := op.leave(value); err != nil {
			return err
		}
		if err := op.delete(key, "a claimed entry"); err != nil {
			return err
		}
		size := int64(len(key) + len(value))
		header.Count--
		header.Bytes -= size
		st.Pending--
		st.PendingBytes -= size
		st.LogicalBytes -= size
		result.Acked++
	}
	result.Remaining = int(header.Count)
	if result.Acked > 0 {
		op.report.Wrote = true
	}
	if header.Count == 0 {
		return op.dropLease(lease, header)
	}
	if result.Acked == 0 {
		return nil
	}
	return op.set(op.leaseHeaderKey(lease), header.encode(), "a lease header")
}

// ackWhole acknowledges every entry of a live lease: a walk of its 'e' prefix,
// at most MaxClaimLimit keys, reading keys only unless the caller asked to see the
// elements (acked); the header holds their count and bytes.
func (op *deliveryOp) ackWhole(lease [leaseIDBytes]byte, header leaseHeader, result *LeaseResult, acked func(position uint64, element []byte)) error {
	prefix := op.claimedKey(lease, nil)
	it := op.scope.snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	for it.Seek(nil); it.Valid(); it.Next() {
		key := append([]byte(nil), it.Key()...)
		if acked != nil && op.field != nil {
			value, err := it.Value()
			if err != nil {
				return fmt.Errorf("reading claimed entry %x of lease %x: %w", key, lease, err)
			}
			_, entry, err := parseClaimedValue(value)
			if err != nil {
				return fmt.Errorf("%w: lease %x: %w", ErrDeliveryInvariant, lease, err)
			}
			acked(binary.BigEndian.Uint64(op.originOf(key)), bytes.Clone(entry))
		}
		if op.onLeave != nil {
			claimedElement, err := it.Value()
			if err != nil {
				return fmt.Errorf("reading an entry of lease %x: %w", lease, err)
			}
			if err := op.leave(claimedElement); err != nil {
				return err
			}
		}
		if err := op.delete(key, "a claimed entry"); err != nil {
			return err
		}
		result.Acked++
	}
	if err := it.Err(); err != nil {
		return fmt.Errorf("walking the entries of lease %x: %w", lease, err)
	}
	if result.Acked != int(header.Count) {
		return fmt.Errorf("%w: lease %x counts %d entries and holds %d", ErrDeliveryInvariant, lease, header.Count, result.Acked)
	}
	st := op.scope.st
	st.Pending -= int64(header.Count)
	st.PendingBytes -= header.Bytes
	st.LogicalBytes -= header.Bytes
	op.report.Wrote = true
	return op.dropLease(lease, header)
}

// leave tells the verb's OnLeave that the entry in a claimed value (return count, then the
// entry) is leaving the store for good. Without an OnLeave it reads nothing.
func (op *deliveryOp) leave(claimedValue []byte) error {
	if op.onLeave == nil {
		return nil
	}
	_, entry, err := parseClaimedValue(claimedValue)
	if err != nil {
		return fmt.Errorf("%w: a claimed entry: %w", ErrDeliveryInvariant, err)
	}
	return op.leaveEntry(entry)
}

// leaveEntry is leave for an entry already parsed.
func (op *deliveryOp) leaveEntry(entry []byte) error {
	if op.onLeave == nil {
		return nil
	}
	if err := op.onLeave(entry); err != nil {
		return fmt.Errorf("an entry leaving the store: %w", err)
	}
	return nil
}

// extend moves a live lease's deadline to the effective now plus leaseMs: the
// 'd' key moves and the header is rewritten; the 'e' keys carry the lease id,
// not the deadline, and stay.
func (op *deliveryOp) extend(lease [leaseIDBytes]byte, header leaseHeader, leaseMs int64, result *LeaseResult) error {
	if err := op.delete(op.deadlineKey(header.Deadline, lease), "a lease deadline"); err != nil {
		return err
	}
	header.Deadline = op.now + leaseMs
	if err := op.set(op.deadlineKey(header.Deadline, lease), nil, "a lease deadline"); err != nil {
		return err
	}
	if err := op.set(op.leaseHeaderKey(lease), header.encode(), "a lease header"); err != nil {
		return err
	}
	result.State, result.Deadline = LeaseExtended, header.Deadline
	op.report.Wrote = true
	return nil
}

// claimN is ClaimN for either engine.
func claimN(engine deliveryEngine, info ApplyInfo, args ClaimArgs, visit func(entry []byte) error) (Claim, error) {
	var claim Claim
	err := engine.apply(info, func(snap storage.KVSnapshot, st *BucketState, batch storage.KVBatch) error {
		op := newDeliveryOp(engine, opScope{snap: snap, st: st, batch: batch})
		op.onLeave, op.leaveBytes, op.leaveKeys = args.OnLeave, args.LeaveBytes, args.LeaveKeys
		op.startClock(args.Now)
		if err := op.claim(args, visit, &claim); err != nil {
			return err
		}
		op.settleClock()
		claim.Pending, claim.Dead, claim.Delivery = st.Pending, st.Dead, op.report
		return nil
	})
	if err != nil {
		return Claim{}, err
	}
	return claim, nil
}

// leaseVerb runs ack or extend on a named lease: the clock, the return step,
// the lease's header, and the verb's own work when the lease is live. An
// unknown or expired lease is answered after the return step, so its work
// commits (claim-ack-spec §4.11).
func leaseVerb(engine deliveryEngine, info ApplyInfo, args LeaseArgs, work func(op *deliveryOp, header leaseHeader, result *LeaseResult) error) (LeaseResult, error) {
	var result LeaseResult
	err := engine.apply(info, func(snap storage.KVSnapshot, st *BucketState, batch storage.KVBatch) error {
		op := newDeliveryOp(engine, opScope{snap: snap, st: st, batch: batch})
		op.onLeave, op.leaveBytes, op.leaveKeys = args.OnLeave, args.LeaveBytes, args.LeaveKeys
		op.startClock(args.Now)
		if err := op.returnStep(); err != nil {
			return err
		}
		header, state, err := op.namedLease(args)
		if err != nil {
			return err
		}
		result = LeaseResult{State: state, Deadline: header.Deadline}
		if state == "" {
			if err := work(op, header, &result); err != nil {
				return err
			}
		}
		op.settleClock()
		result.Delivery = op.report
		return nil
	})
	if err != nil {
		return LeaseResult{}, err
	}
	return result, nil
}

// ack is Ack for either engine (claim-ack-spec §4.11).
func ack(engine deliveryEngine, info ApplyInfo, args LeaseArgs, itemIDs [][]byte) (LeaseResult, error) {
	return leaseVerb(engine, info, args, func(op *deliveryOp, header leaseHeader, result *LeaseResult) error {
		result.State = LeaseAcked
		if len(itemIDs) > 0 {
			return op.ackItems(args.LeaseID, header, itemIDs, result, args.Acked)
		}
		return op.ackWhole(args.LeaseID, header, result, args.Acked)
	})
}

// extend is Extend for either engine (claim-ack-spec §4.11).
func extend(engine deliveryEngine, info ApplyInfo, args LeaseArgs, leaseMs int64) (LeaseResult, error) {
	return leaseVerb(engine, info, args, func(op *deliveryOp, header leaseHeader, result *LeaseResult) error {
		return op.extend(args.LeaseID, header, leaseMs, result)
	})
}

// release is Release for either engine (claim-ack-spec §4.11, CA-10).
func release(engine deliveryEngine, info ApplyInfo, args LeaseArgs) (LeaseResult, error) {
	var result LeaseResult
	err := engine.apply(info, func(snap storage.KVSnapshot, st *BucketState, batch storage.KVBatch) error {
		op := newDeliveryOp(engine, opScope{snap: snap, st: st, batch: batch})
		op.onLeave, op.leaveBytes, op.leaveKeys = args.OnLeave, args.LeaveBytes, args.LeaveKeys
		op.startClock(args.Now)
		header, state, err := op.namedLease(args)
		if err != nil {
			return err
		}
		result = LeaseResult{State: state, Deadline: header.Deadline}
		if state == "" {
			err = op.release(args.LeaseID, header, &result)
		} else {
			err = op.returnStep()
		}
		if err != nil {
			return err
		}
		op.settleClock()
		result.Delivery = op.report
		return nil
	})
	if err != nil {
		return LeaseResult{}, err
	}
	return result, nil
}

// popN is PopN for either engine: step 0, the return step, then pop-n's walk
// (claim-ack-spec §4.10). A pop logged before this build carries no now, and no
// bucket had a lease then, so now 0 skips the return step and the clock.
func popN(engine deliveryEngine, info ApplyInfo, n int, now int64, visit func(entry []byte) error) (PopResult, error) {
	var result PopResult
	err := engine.apply(info, func(snap storage.KVSnapshot, st *BucketState, batch storage.KVBatch) error {
		op := newDeliveryOp(engine, opScope{snap: snap, st: st, batch: batch})
		if now != 0 {
			op.startClock(now)
			if err := op.returnStep(); err != nil {
				return err
			}
		}
		// The empty check follows the return step, never precedes it.
		removed := 0
		if st.Entries > 0 {
			var err error
			removed, err = engine.walkTake(op.scope, op.walk(n, false, func(_, entry []byte) error { return visit(entry) }, op.popTake))
			if err != nil {
				return err
			}
		}
		if now != 0 {
			op.settleClock()
		}
		result = PopResult{Removed: removed, Delivery: op.report, Pending: st.Pending, Dead: st.Dead}
		result.Delivery.Wrote = op.report.Wrote || removed > 0
		return nil
	})
	if err != nil {
		return PopResult{}, err
	}
	return result, nil
}

// popTake is pop's per-entry take: the 0x04 delete, and the entry's 'c' record
// with it when it had been returned.
func (op *deliveryOp) popTake(st *BucketState, batch storage.KVBatch, key, entry []byte) error {
	if err := op.engine.removeEntry(st, batch, key, entry); err != nil {
		return err
	}
	_, err := op.dropReturnCount(key)
	return err
}

// dropReturnCount is the return count of an entry a walk takes, and the delete
// of its 'c' record: from the overlay for an entry this apply wrote back, and
// from the snapshot only while the bucket counts a returned entry, so a bucket
// whose entries were never returned pays no lookup.
func (op *deliveryOp) dropReturnCount(key []byte) (uint64, error) {
	if written, ok := op.over.get(key); ok {
		op.over.take(key)
		if err := op.delete(op.returnCountKey(op.originOf(key)), "a return count"); err != nil {
			return 0, err
		}
		op.scope.st.Returned--
		return written.returns, nil
	}
	if op.scope.st.Returned == 0 {
		return 0, nil
	}
	return dropStoredReturnCount(op.scope, op.returnCountKeyOf(key))
}

// returnCountKeyOf is the 'c' key of the entry at entryKey.
func returnCountKeyOf(entryKey []byte) []byte {
	countKey := make([]byte, 0, len(entryKey)+1)
	countKey = append(countKey, entryKey[:originPrefixBytes-1]...)
	countKey = append(countKey, storage.NSDelivery, kindReturnCount)
	return append(countKey, entryKey[originPrefixBytes:]...)
}

// dropStoredReturnCount deletes the 'c' record at countKey, if the snapshot
// holds one, and returns the count it held.
func dropStoredReturnCount(scope opScope, countKey []byte) (uint64, error) {
	encoded, err := scope.snap.Get(countKey)
	switch {
	case errors.Is(err, storage.ErrKeyNotFound):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("reading a return count: %w", err)
	}
	returns, err := parseReturnCountValue(encoded)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDeliveryInvariant, err)
	}
	if err := scope.batch.Delete(countKey); err != nil {
		return 0, fmt.Errorf("deleting a return count: %w", err)
	}
	scope.st.Returned--
	return returns, nil
}

// returnStep is claim-ack-spec §4.5. It runs only while the bucket has a lease,
// with no seek otherwise, and before the verb's empty check. Every lease whose
// deadline is at or before the effective now goes back whole, soonest deadline
// first, while the budget lasts, the apply's first unit always, so no bucket
// can wedge. Then the forward-jump guard: one more seek, to the first deadline
// further than MaxLeaseMs ahead, which no grant or extend sets unless the
// owner's clock jumped forward, and those leases go back too, under the same
// budget. Every input is the stamped now, the stored keys and constants, so
// every copy returns the same leases.
func (op *deliveryOp) returnStep() error {
	if op.scope.st.Leases == 0 {
		return nil
	}
	prefix := op.kindKey(kindDeadline)
	it := op.scope.snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	full, err := op.returnDue(it, nil, len(prefix), func(deadline int64) bool { return deadline <= op.now })
	if err != nil || full {
		return err
	}
	guard := op.kindKey(kindDeadline, bigEndian(uint64(op.now+MaxLeaseMs+1)))
	_, err = op.returnDue(it, guard, len(prefix), func(int64) bool { return true })
	return err
}

// returnDue returns the leases of the 'd' index from seek on, in deadline
// order, while due holds for their deadline. It reports full when the budget
// ended it.
func (op *deliveryOp) returnDue(it storage.KVIterator, seek []byte, prefixLen int, due func(deadline int64) bool) (full bool, err error) {
	for it.Seek(seek); it.Valid(); it.Next() {
		rest := it.Key()[prefixLen:]
		deadline := int64(binary.BigEndian.Uint64(rest[:8]))
		if !due(deadline) {
			return false, nil
		}
		var lease [leaseIDBytes]byte
		copy(lease[:], rest[8:])
		plan, err := op.planReturn(lease)
		if err != nil {
			return false, err
		}
		// A lease is never split: it fits whole or the step ends before it.
		if !op.spend.admit(plan.cost) {
			return true, nil
		}
		if err := op.writeBack(plan, true); err != nil {
			return false, err
		}
		if err := op.dropLease(lease, leaseHeader{Deadline: deadline}); err != nil {
			return false, err
		}
	}
	if err := it.Err(); err != nil {
		return false, fmt.Errorf("walking the lease deadlines: %w", err)
	}
	return false, nil
}

// returnItem is one claimed entry a return or a release takes off its lease.
type returnItem struct {
	// claimed is the 'e' key and size its key and value bytes.
	claimed []byte
	size    int64
	origin  []byte
	entry   []byte
	// returns is r', the entry's return count after this return.
	returns uint64
	// dead is the reason it is dead-lettered, "" when it goes back.
	dead   string
	record []byte
}

// returnPlan is what returning one lease writes, decided before any of it is
// written, so the budget can refuse the lease whole.
type returnPlan struct {
	items []returnItem
	// cost is the plan's charge; reserve is the most it could cost, every
	// entry dead-lettered, which a release sets aside before the return step
	// can change a heap entry's collision.
	cost, reserve deliveryCharge
}

// planReturn reads one lease's 'e' keys from the snapshot, in key order, and
// decides each entry's fate: at r' >= MaxDeliveries it is dead-lettered
// max_deliveries; on a heap an origin key already occupied, in the snapshot or
// by an entry this apply wrote back, dead-letters it collided; otherwise it
// goes back to its origin key.
func (op *deliveryOp) planReturn(lease [leaseIDBytes]byte) (returnPlan, error) {
	var plan returnPlan
	plan.cost.add(len(op.leaseHeaderKey(lease)), 0)
	plan.cost.add(len(op.deadlineKey(0, lease)), 0)
	plan.reserve = plan.cost
	prefix := op.claimedKey(lease, nil)
	it := op.scope.snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	for it.Seek(nil); it.Valid(); it.Next() {
		key := append([]byte(nil), it.Key()...)
		value, err := it.Value()
		if err != nil {
			return returnPlan{}, fmt.Errorf("reading a claimed entry of lease %x: %w", lease, err)
		}
		returns, entry, err := parseClaimedValue(value)
		if err != nil {
			return returnPlan{}, fmt.Errorf("%w: lease %x: %w", ErrDeliveryInvariant, lease, err)
		}
		item := returnItem{claimed: key, size: int64(len(key) + len(value)), origin: key[len(prefix):], entry: entry, returns: returns + 1}
		if err := op.decideFate(&item); err != nil {
			return returnPlan{}, err
		}
		plan.cost.add(len(key), 0)
		plan.reserve.add(len(key), 0)
		plan.reserve.add(returnReserve(len(item.origin), len(entry)))
		plan.reserve.Bytes += int64(op.leaveBytes)
		plan.reserve.Keys += int64(op.leaveKeys)
		if item.dead != "" {
			plan.cost.Bytes += int64(op.leaveBytes)
			plan.cost.Keys += int64(op.leaveKeys)
			plan.cost.add(len(op.deadKey(deadSuffix(op.layout, item.origin, 0))), len(item.record))
		} else {
			plan.cost.add(op.originPrefixLen()+len(item.origin), len(entry))
			plan.cost.add(returnCountKeyBytes+len(item.origin), len(returnCountValue(item.returns)))
		}
		plan.items = append(plan.items, item)
	}
	if err := it.Err(); err != nil {
		return returnPlan{}, fmt.Errorf("walking the entries of lease %x: %w", lease, err)
	}
	return plan, nil
}

// decideFate sets item's dead reason, and its record when it has one.
func (op *deliveryOp) decideFate(item *returnItem) error {
	switch {
	case item.returns >= MaxDeliveries:
		item.dead = DeadReasonMaxDeliveries
	case op.layout == claimLayoutHeap:
		occupied, err := op.occupied(op.originKey(item.origin))
		if err != nil {
			return err
		}
		if occupied {
			item.dead = DeadReasonCollided
		}
	}
	if item.dead == "" {
		return nil
	}
	data := item.entry
	if op.layout == claimLayoutHeap {
		document, err := heapEntryDocument(item.origin, item.entry)
		if err != nil {
			return err
		}
		data = document
	}
	record, err := encodeDeadRecord(deadRecord{Data: data, Deliveries: int32(item.returns), DeadAt: primitive.DateTime(op.now), Reason: item.dead})
	if err != nil {
		return err
	}
	item.record = record
	return nil
}

// occupied reports whether a live 0x04 key is at key in this apply's view: the
// overlay first, then the snapshot.
func (op *deliveryOp) occupied(key []byte) (bool, error) {
	if _, ok := op.over.get(key); ok {
		return true, nil
	}
	if op.over.taken[string(key)] {
		return false, nil
	}
	_, err := op.scope.snap.Get(key)
	switch {
	case errors.Is(err, storage.ErrKeyNotFound):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("reading the origin key of a returned entry: %w", err)
	}
	return true, nil
}

// writeBack writes a plan: each entry's 'e' key goes, and the entry goes back
// to its origin key with its 'c' record, or to the dead list. byReturnStep
// counts what went back as the return step's.
func (op *deliveryOp) writeBack(plan returnPlan, byReturnStep bool) error {
	st := op.scope.st
	for _, item := range plan.items {
		if err := op.delete(item.claimed, "a claimed entry"); err != nil {
			return err
		}
		st.Pending--
		st.PendingBytes -= item.size
		st.LogicalBytes -= item.size
		var err error
		switch {
		case item.dead != "":
			if err = op.leaveEntry(item.entry); err == nil {
				err = op.deadLetter(item)
			}
		default:
			err = op.putBack(item)
			if byReturnStep {
				op.report.Returned++
			}
		}
		if err != nil {
			return err
		}
	}
	op.report.Wrote = true
	return nil
}

// putBack writes an entry back to its origin key, through the batch and the
// overlay, with its 'c' record (claim-ack-spec §4.5, §4.6).
func (op *deliveryOp) putBack(item returnItem) error {
	st := op.scope.st
	key := op.originKey(item.origin)
	if err := op.set(key, item.entry, "a returned entry"); err != nil {
		return err
	}
	if err := op.set(op.returnCountKey(item.origin), returnCountValue(item.returns), "a return count"); err != nil {
		return err
	}
	op.over.put(key, item.entry, item.returns)
	st.Entries++
	st.Returned++
	st.LogicalBytes += int64(len(key) + len(item.entry))
	op.engine.wroteBack(st, item.origin)
	return nil
}

// deadLetter writes an entry's dead record (claim-ack-spec §4.7). A heap's dead
// key carries the next dead sequence, because one origin suffix can die more
// than once. A dead key that is already there is refused, never overwritten:
// no write of this engine leaves one, so it is a tripwire.
func (op *deliveryOp) deadLetter(item returnItem) error {
	st := op.scope.st
	suffix := deadSuffix(op.layout, item.origin, st.DeadSeq)
	if op.layout == claimLayoutHeap {
		st.DeadSeq++
	}
	if err := op.writeDead(suffix, item.record); err != nil {
		return err
	}
	op.report.DeadLettered++
	if item.dead == DeadReasonCollided {
		op.report.Collided++
	}
	return nil
}

// writeDead writes a dead entry's 'x' key and counts it. A dead key that is
// already there is refused, never overwritten: no write of this engine leaves
// one, so it is a tripwire.
func (op *deliveryOp) writeDead(suffix, record []byte) error {
	st := op.scope.st
	key := op.deadKey(suffix)
	_, err := op.scope.snap.Get(key)
	switch {
	case err == nil:
		return fmt.Errorf("%w: dead entry %s is already there", ErrDeliveryInvariant, deadIDString(suffix))
	case !errors.Is(err, storage.ErrKeyNotFound):
		return fmt.Errorf("reading dead entry %s: %w", deadIDString(suffix), err)
	}
	if err := op.set(key, record, "a dead entry"); err != nil {
		return err
	}
	size := int64(len(key) + len(record))
	st.Dead++
	st.DeadBytes += size
	st.LogicalBytes += size
	return nil
}

// release writes a live lease's entries back by the return rule and drops it
// (claim-ack-spec §4.4, §4.11). The return step runs first against what is
// left once the release's own lease is set aside, and with no exempt unit of
// its own: a lease it cannot admit by that limit stays out. If even the
// release's own lease does not fit the whole budget, the release is the
// apply's exempt unit and the return step returns nothing in this apply.
func (op *deliveryOp) release(lease [leaseIDBytes]byte, header leaseHeader, result *LeaseResult) error {
	own, err := op.planReturn(lease)
	if err != nil {
		return err
	}
	limit := op.spend.limit
	if own.reserve.Bytes <= limit.Bytes && own.reserve.Keys <= limit.Keys {
		op.spend.limit = deliveryCharge{Bytes: limit.Bytes - own.reserve.Bytes, Keys: limit.Keys - own.reserve.Keys}
		// No exemption (owner ruling 2026-10-18): the release's own lease is
		// the apply's one exempt unit, so a lease the return step may not
		// admit by the limit stays out, and the apply fits the floor batch.
		op.spend.unitWritten = true
		if err := op.returnStep(); err != nil {
			return err
		}
		op.spend.limit = limit
		op.spend.unitWritten = false
		// The return step may have written back a heap key one of the own
		// entries now collides with.
		if own, err = op.planReturn(lease); err != nil {
			return err
		}
	}
	if err := op.writeBack(own, false); err != nil {
		return err
	}
	result.State, result.Released = LeaseReleased, len(own.items)
	return op.dropLease(lease, header)
}
