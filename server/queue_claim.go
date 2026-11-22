package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The lease verbs on the wire (claim-ack-spec 9): claim, ack, release and
// extend on a fifo, lifo or heap bucket. A single node logs nothing, so these
// handlers read their own clock and crypto/rand; on a cluster the owner stamps
// both before the entry is logged and every copy decodes the same values.

// ErrClaimLimit is under every refusal of a claim's limit: negative, or above
// store.MaxClaimLimit. It reaches the wire as INVALID_ARGUMENT.
var ErrClaimLimit = errors.New("invalid claim limit")

// ErrLeaseOption is under every refusal of a lease verb's options: a lease
// length outside its bounds, a lease id or an item id that is not one claim
// returned, too many item ids. It reaches the wire as INVALID_ARGUMENT.
var ErrLeaseOption = errors.New("invalid lease")

// ErrClaimEntryTooLarge is a claim whose first entry is over the hard cap
// (claim-ack-spec 4.4): too large to claim and then return in one batch. The
// store answers it as a result, so the return step's work committed; nothing
// was claimed.
var ErrClaimEntryTooLarge = errors.New("claim refused")

// claimLimit is requestLimit for a claim. bucket names the fix in the user's
// own words.
func claimLimit(requested int, bucket string) (int, error) {
	advice := fmt.Sprintf("Claim takes at most %d at a time: repeat db.%s.claim({limit: %d}), or read them without taking any with db.%s.peek({limit: %d})",
		store.MaxClaimLimit, bucket, store.MaxClaimLimit, bucket, min(requested, maxPeekLimit))
	return requestLimit("claim", requested, store.MaxClaimLimit, ErrClaimLimit, advice)
}

// leaseSpelling is a lease length in the unit a developer would type: the
// whole hours, minutes or seconds it is, else milliseconds.
func leaseSpelling(ms int64) string {
	switch {
	case ms%3_600_000 == 0:
		return fmt.Sprintf("%dh", ms/3_600_000)
	case ms%60_000 == 0:
		return fmt.Sprintf("%dm", ms/60_000)
	case ms%1_000 == 0:
		return fmt.Sprintf("%ds", ms/1_000)
	}
	return fmt.Sprintf("%d ms", ms)
}

// leaseLength is the lease a claim asks for, in ms: zero is the default, and
// anything else must lie within the bounds every node agrees on. extending
// words the fix for an extend, which has no default and names its lease.
func leaseLength(requested int64, bucket string, extending bool) (int64, error) {
	switch {
	case requested == 0 && extending:
		return 0, fmt.Errorf("%w: extend needs a new length. Extend with db.%s.extend({lease_id: \"…\", lease: \"60s\"}), within %s to %s",
			ErrLeaseOption, bucket, leaseSpelling(store.MinLeaseMs), leaseSpelling(store.MaxLeaseMs))
	case requested == 0:
		return store.DefaultLeaseMs, nil
	case requested > store.MaxLeaseMs:
		return 0, fmt.Errorf("%w: %d ms is above the lease maximum of %s. A lease of more than an hour is not granted: claim with lease %q and call db.%s.extend({lease_id: \"…\", lease: %q}) while the work runs",
			ErrLeaseOption, requested, leaseSpelling(store.MaxLeaseMs), leaseSpelling(store.MaxLeaseMs), bucket, leaseSpelling(store.MaxLeaseMs))
	case requested < store.MinLeaseMs:
		return 0, fmt.Errorf("%w: %d ms is below the lease minimum of %s. Claim with lease %q (the default is %s), or any length up to %s",
			ErrLeaseOption, requested, leaseSpelling(store.MinLeaseMs), leaseSpelling(store.DefaultLeaseMs), leaseSpelling(store.DefaultLeaseMs), leaseSpelling(store.MaxLeaseMs))
	}
	return requested, nil
}

// leaseIDOf reads the lease id a request names. A malformed one is refused
// here, never in a store verb.
func leaseIDOf(text, bucket string) ([store.LeaseIDBytes]byte, error) {
	lease, err := store.ParseLeaseID(text)
	if err != nil {
		return lease, fmt.Errorf("%w: lease id %q is not a lease id (%v). A lease id is the %d hexadecimal characters claim answered in lease_id: claim again with db.%s.claim() and use the lease_id it returns",
			ErrLeaseOption, abbreviated(text, 64), err, 2*store.LeaseIDBytes, bucket)
	}
	return lease, nil
}

// abbreviated is text cut to at most limit bytes, so an error never echoes a
// client's megabyte. The cut backs off to a character boundary: half a
// character would reach the message as a \x escape, or as invalid UTF-8 that a
// driver validating its strings cannot decode.
func abbreviated(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut] + "…"
}

// itemIDCount refuses an ack that names more item ids than a claim takes.
func itemIDCount(count int, bucket string) error {
	if count > store.MaxItemIDsPerAck {
		return fmt.Errorf("%w: ack names %d item ids and takes at most %d. Acknowledge the lease whole with db.%s.ack({lease_id: \"…\"}), or in batches of %d",
			ErrLeaseOption, count, store.MaxItemIDsPerAck, bucket, store.MaxItemIDsPerAck)
	}
	return nil
}

// itemIDsOf reads an ack's item ids. An id must be the hex of a well-formed
// origin suffix for the bucket's type: 8 bytes for fifo and lifo, an 8-byte
// priority and the element's id for a heap. The type-dependent check lives
// here and not at the door, which may not hold the bucket.
func itemIDsOf(texts []string, bucketType BucketType, bucket string) ([][]byte, error) {
	itemIDs := make([][]byte, 0, len(texts))
	for _, text := range texts {
		decoded, err := hex.DecodeString(text)
		wellFormed := err == nil
		if bucketType == BucketTypeHeap {
			wellFormed = wellFormed && len(decoded) > 8 && len(decoded) <= 8+store.MaxClaimIDBytes
		} else {
			wellFormed = wellFormed && len(decoded) == 8
		}
		if !wellFormed {
			return nil, fmt.Errorf("%w: item id %q is not a handle claim returned. item_ids are the values claim answered in its item_ids, not the documents' _id_ values. "+
				"Pass the item_ids of the claim, or leave item_ids out to acknowledge the whole lease: db.%s.ack({lease_id: \"…\"})",
				ErrLeaseOption, abbreviated(text, 64), bucket)
		}
		itemIDs = append(itemIDs, decoded)
	}
	return itemIDs, nil
}

// resolveClaim refuses, before anything is logged, a claim whose limit or
// lease is out of bounds. The bounds are the same on every node, so the door
// can apply them; the handler repeats them, because on a node that logs
// nothing the door never runs (claim-ack-spec 9.3).
func resolveClaim(payload []byte) error {
	raw := bson.Raw(payload)
	bucket, _ := raw.Lookup("bucket").StringValueOK()
	if limit, isNumber := raw.Lookup("limit").AsInt64OK(); isNumber {
		if _, err := claimLimit(int(limit), bucket); err != nil {
			return err
		}
	}
	if leaseMs, isNumber := raw.Lookup("lease_ms").AsInt64OK(); isNumber {
		if _, err := leaseLength(leaseMs, bucket, false); err != nil {
			return err
		}
	}
	return nil
}

// resolveLease is resolveClaim for ack, release and extend: the lease id, the
// number of item ids and an extend's length. The item ids' form depends on the
// bucket's type, which a coordinator may not hold, so it is the handler's.
func resolveLease(payload []byte, extending bool) error {
	raw := bson.Raw(payload)
	bucket, _ := raw.Lookup("bucket").StringValueOK()
	leaseText, _ := raw.Lookup("lease_id").StringValueOK()
	if _, err := leaseIDOf(leaseText, bucket); err != nil {
		return err
	}
	if itemIDs, isArray := raw.Lookup("item_ids").ArrayOK(); isArray {
		values, err := itemIDs.Values()
		if err != nil {
			return fmt.Errorf("%w: item_ids is not a list of text values: %w", ErrMalformedPayload, err)
		}
		if err := itemIDCount(len(values), bucket); err != nil {
			return err
		}
	}
	if extending {
		leaseMs, _ := raw.Lookup("lease_ms").AsInt64OK()
		if _, err := leaseLength(leaseMs, bucket, true); err != nil {
			return err
		}
	}
	return nil
}

// leaseEngine is what the lease verbs need of a bucket's queue or heap store.
type leaseEngine struct {
	// noun is how a message names the store: "queue" or "heap".
	noun    string
	claimN  func(info store.ApplyInfo, args store.ClaimArgs, visit func(entry []byte) error) (store.Claim, error)
	ack     func(info store.ApplyInfo, args store.LeaseArgs, itemIDs [][]byte) (store.LeaseResult, error)
	release func(info store.ApplyInfo, args store.LeaseArgs) (store.LeaseResult, error)
	extend  func(info store.ApplyInfo, args store.LeaseArgs, leaseMs int64) (store.LeaseResult, error)
	entries func() (int64, error)
	// idleClaim is store.QueueStore.IdleClaim or store.HeapStore.IdleClaim.
	idleClaim func(now int64) (store.DeliveryCounts, bool, error)
}

// leaseEngineOf is the engine of a fifo, lifo or heap bucket, and false for any
// other type.
func leaseEngineOf(bucket *inMemoryBucket) (leaseEngine, bool) {
	switch bucket.bucketType {
	case BucketTypeFIFO, BucketTypeLIFO:
		queue := bucket.queueStore
		return leaseEngine{"queue", queue.ClaimN, queue.Ack, queue.Release, queue.Extend, queue.Len, queue.IdleClaim}, true
	case BucketTypeHeap:
		heap := bucket.heapStore
		return leaseEngine{"heap", heap.ClaimN, heap.Ack, heap.Release, heap.Extend, heap.Len, heap.IdleClaim}, true
	}
	return leaseEngine{}, false
}

// claimRefusal is the in-band answer to a lease verb on a bucket type that
// issues no leases: why, and the verbs that do read and remove it.
func claimRefusal(verb string, bucketType BucketType) string {
	reading := `Read with get("<key>") or find(), and remove with delete("<key>").`
	if bucketType == BucketTypeBlob {
		reading = `Read its metadata with info("<key>"), and remove with delete("<key>").`
	}
	switch verb {
	case "claim":
		return fmt.Sprintf(`claim is not defined for %s buckets: a %s has no order to take from. %s`, bucketType, bucketType, reading)
	case "dead", "requeue", "discard":
		return fmt.Sprintf(`%s is not defined for %s buckets: only fifo, lifo and heap buckets have a dead list. %s`, verb, bucketType, reading)
	}
	return fmt.Sprintf(`%s is not defined for %s buckets: only fifo, lifo and heap buckets issue leases. %s`, verb, bucketType, reading)
}

// leaseClockMillis is this node's clock for lease time, in ms.
func (s *Server) leaseClockMillis() int64 {
	if s.leaseClock != nil {
		return s.leaseClock().UnixMilli()
	}
	return time.Now().UnixMilli()
}

// leaseNow is the time a lease verb runs under (claim-ack-spec 5.1, the trust
// rule). A logged entry carries the time its owner stamped, which every copy
// decodes; a request nothing logged is run at this node's clock, and any value
// the client put in the payload is ignored. A logged lease verb without a
// stamp cannot be produced by this build: every copy refuses it alike.
func (s *Server) leaseNow(ctx context.Context, verb string, stamped int64) (int64, error) {
	if applySeqFrom(ctx) == 0 {
		return s.leaseClockMillis(), nil
	}
	if stamped == 0 {
		return 0, fmt.Errorf("%w: a logged %s carries no lease_now, which its owner stamps before the entry is logged", ErrMalformedPayload, verb)
	}
	return stamped, nil
}

// leaseHasEnded is the hint a read gives (claim-ack-spec 8.2): the soonest lease
// of the state it read has ended by this node's clock, so the next claim or pop
// returns it first. It is never logged and is a hint on a replica, whose clock
// is its own.
func (s *Server) leaseHasEnded(counts store.DeliveryCounts) bool {
	return counts.NextDeadline != 0 && counts.NextDeadline <= s.leaseClockMillis()
}

// popLeaseNow is leaseNow for a pop: a logged pop without a stamp is one
// written before leases existed, so it skips the return step (now 0).
func (s *Server) popLeaseNow(ctx context.Context, stamped int64) int64 {
	if applySeqFrom(ctx) == 0 {
		return s.leaseClockMillis()
	}
	return stamped
}

// leaseNonce is the entropy a new lease id is composed with, under the same
// trust rule as leaseNow.
func leaseNonce(ctx context.Context, stamped []byte) ([8]byte, error) {
	var nonce [8]byte
	if applySeqFrom(ctx) == 0 {
		if _, err := rand.Read(nonce[:]); err != nil {
			return nonce, fmt.Errorf("drawing a lease nonce: %w", err)
		}
		return nonce, nil
	}
	if len(stamped) != len(nonce) {
		return nonce, fmt.Errorf("%w: a logged claim carries a lease_nonce of %d bytes, which its owner stamps as %d", ErrMalformedPayload, len(stamped), len(nonce))
	}
	copy(nonce[:], stamped)
	return nonce, nil
}

// ClaimCtx claims up to req.Limit entries of a fifo, lifo or heap bucket under
// one lease, in pop order, in one apply. Like PopCtx it is a write on the
// bucket lock, and its context carries the apply sequence of a replicated or
// replayed claim.
func (s *Server) ClaimCtx(ctx context.Context, req ClaimRequest) (*ClaimResponse, error) {
	bucket, unlock, err := s.lockBucket(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer unlock()

	engine, ok := leaseEngineOf(bucket)
	if !ok {
		return nil, wrongTypeSentence(claimRefusal("claim", bucket.bucketType), bucket.bucketType)
	}
	limit, err := claimLimit(req.Limit, req.Bucket)
	if err != nil {
		return nil, err
	}
	leaseMs, err := leaseLength(req.LeaseMs, req.Bucket, false)
	if err != nil {
		return nil, err
	}
	now, err := s.leaseNow(ctx, "claim", req.LeaseNow)
	if err != nil {
		return nil, err
	}
	nonce, err := leaseNonce(ctx, req.LeaseNonce)
	if err != nil {
		return nil, err
	}

	// A claim at the ceiling stops short and answers what it took (Q-4), so its
	// budget never refuses and never counts a refusal.
	ceiling, _ := s.popCeiling(ctx, req.ResultCeiling)
	budget := s.newResultBudgetWithCeiling(queryMethodClaim, ceiling, false, resultTooLargeError{})
	defer budget.report()
	// Background, never ctx: the walk runs inside the apply, and a caller who
	// has gone must not fail the owner's apply alone (see PopCtx).
	walk := newQueryWalk(context.Background(), queryMethodClaim, req.Database, req.Bucket)
	var items []bson.Raw
	visit := func(entry []byte) error {
		if err := walk.visit(); err != nil {
			return err
		}
		cost := int64(len(entry)) + elementHeader(len(items))
		if len(items) == 0 {
			budget.chargeFirst(cost)
		} else if budget.chargeDocument(cost) != nil {
			return store.ErrStopShort
		}
		items = append(items, bson.Raw(entry))
		return nil
	}

	args := store.ClaimArgs{N: limit, LeaseMs: leaseMs, Now: now, Nonce: nonce, NameHash: store.BucketNameHash(req.Database, req.Bucket)}
	claimed, err := engine.claimN(applyInfoFrom(ctx, s), args, visit)
	s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "CLAIM",
		emptyPop: err == nil && len(claimed.Items) == 0 && !claimed.Delivery.Wrote}, err)
	if err != nil {
		return &ClaimResponse{Found: false, Message: s.claimFailureMessage(req, engine, limit, err)}, nil
	}
	s.noteDelivery(ctx, req.Database, req.Bucket, claimed.Delivery)
	if claimed.Refused != nil {
		if answeringApply(ctx) {
			queueClaimsEntryTooLarge.Inc()
			s.noteDeadLettered(req.Database, req.Bucket, 0, 1)
		}
		return nil, withOutcome(outcomeNotApplied, claimTooLarge(req, engine, claimed))
	}
	if answeringApply(ctx) {
		queueClaims.Inc()
		queueClaimedEntries.Add(float64(len(claimed.Items)))
	}
	return claimAnswer(req, engine, leaseMs, items, claimed), nil
}

// unloggedEmptyClaim is the owner's answer to a claim that would take nothing
// and return nothing (claim-ack-spec 5.3, CA-15), read from a fresh snapshot
// before anything is logged: found false, the type's absence message, pending
// and dead when non-zero, and no write outcome, because nothing was written.
// It is nil when the claim must be logged: a bucket this node does not hold,
// or not a fifo, lifo or heap bucket, takes the logged path, whose handler
// answers bucket-not-found or the type refusal exactly as before; so does a
// claim whose state says it would take or return an entry.
func (s *Server) unloggedEmptyClaim(payload []byte) (*ReplResponse, error) {
	var req ClaimRequest
	if err := bson.Unmarshal(payload, &req); err != nil {
		// The logged path's handler refuses it, in its own words.
		return nil, nil
	}
	bucket, snap, err := s.resolveForRead(req.Database, req.Bucket)
	if err != nil {
		// Not held here: the logged path answers bucket-not-found.
		return nil, nil
	}
	defer snap.Discard()
	engine, ok := leaseEngineOf(bucket)
	if !ok {
		// The logged path answers the type refusal (claimRefusal).
		return nil, nil
	}
	counts, idle, err := engine.idleClaim(s.leaseClockMillis())
	if err != nil || !idle {
		// A state the read cannot judge is the logged apply's to judge, and
		// to refuse in its own words if it must.
		if err != nil {
			s.logger.Warn("answering a claim from its log: its idle check could not read the bucket",
				"database", req.Database, "bucket", req.Bucket, "error", err)
		}
		return nil, nil
	}
	answer := claimAnswer(req, engine, 0, nil, store.Claim{Pending: counts.Pending, Dead: counts.Dead})
	encoded, err := bson.Marshal(answer)
	if err != nil {
		return nil, fmt.Errorf("encoding the empty claim of bucket %q in database %q: %w", req.Bucket, req.Database, err)
	}
	queueClaims.Inc()
	queueEmptyClaimsUnlogged.Inc()
	return &ReplResponse{Opcode: int32(transport.OpQueueClaim), Payload: encoded}, nil
}

// claimAnswer is the response of a claim that was not refused.
func claimAnswer(req ClaimRequest, engine leaseEngine, leaseMs int64, items []bson.Raw, claimed store.Claim) *ClaimResponse {
	answer := &ClaimResponse{Pending: claimed.Pending, Dead: claimed.Dead, Returned: int64(claimed.Delivery.Returned)}
	if len(claimed.Items) == 0 {
		answer.Message = engine.noun + " is empty"
		return answer
	}
	answer.Found, answer.Items = true, items
	answer.LeaseID = store.LeaseIDString(claimed.LeaseID)
	answer.LeaseMs = leaseMs
	answer.LeaseDeadline = time.UnixMilli(claimed.Deadline).UTC()
	answer.ItemIDs = make([]string, len(claimed.Items))
	redelivered := false
	for index, item := range claimed.Items {
		answer.ItemIDs[index] = hex.EncodeToString(item.ItemID)
		redelivered = redelivered || item.Deliveries != 1
	}
	if redelivered {
		answer.Deliveries = make([]int32, len(claimed.Items))
		for index, item := range claimed.Items {
			answer.Deliveries[index] = int32(item.Deliveries)
		}
	}
	return answer
}

// mebibytes is a size as the messages spell it: MiB with two decimals.
func mebibytes(size int64) string {
	return fmt.Sprintf("%.2f MiB", float64(size)/(1<<20))
}

// claimTooLarge is the refusal of a claim whose first entry is over the hard
// cap. It names the entry's size and the fix, and carries no entry content.
func claimTooLarge(req ClaimRequest, engine leaseEngine, claimed store.Claim) error {
	refused := claimed.Refused
	returned := ""
	if claimed.Delivery.Returned > 0 {
		returned = "; expired leases were returned"
	}
	if refused.IDTooLong {
		return fmt.Errorf("%w: the element at the front of the heap %s.%s has an id that is too long, and claim moves elements whose id is at most %d bytes. "+
			"Nothing was taken%s. Take it with db.%s.pop(), which removes it at most once, or push it again with a shorter _id_",
			ErrClaimEntryTooLarge, req.Database, req.Bucket, store.MaxClaimIDBytes, returned, req.Bucket)
	}
	return fmt.Errorf("%w: the entry at the front of %s.%s needs %s to claim, and claim moves entries of at most %s (the largest batch every member can commit). "+
		"Nothing was taken%s. Take it with db.%s.pop(), which removes it at most once, or store payloads that large in a blob bucket and queue a reference",
		ErrClaimEntryTooLarge, req.Database, req.Bucket, mebibytes(refused.Footprint), mebibytes(refused.FootprintLimit), returned, req.Bucket)
}

// claimFailureMessage words a claim the store failed, as popFailureMessage
// does a pop: a state that counts entries the range does not hold and a batch
// too large each name the cause and the fix, and neither carries a storage key.
func (s *Server) claimFailureMessage(req ClaimRequest, engine leaseEngine, limit int, err error) string {
	failure := engine.noun + " claim failed"
	switch {
	case errors.Is(err, store.ErrQueueCursorLost):
		counted, _ := engine.entries()
		return fmt.Sprintf("%s: the %s's state counts %d entries and none is at its cursor, so nothing was claimed. "+
			"On a cluster, restore this bucket from a replica with 'hoarddb dump' on the replica and 'restore' here. "+
			"On a standalone node, restore it from a dump or backup", failure, engine.noun, counted)
	case errors.Is(err, storage.ErrBatchTooLarge):
		var tooLarge *storage.BatchTooLargeError
		if errors.As(err, &tooLarge) {
			s.logger.Warn("a claim did not fit in one batch; the message to the client leaves out the storage key",
				"database", req.Database, "bucket", req.Bucket, "limit", limit, "bound_bytes", tooLarge.Bytes, "key_bytes", len(tooLarge.Key))
		}
		return fmt.Sprintf("%s: claiming %d entries at once does not fit in the largest batch the store commits, so nothing was claimed. "+
			"Claim fewer at a time: db.%s.claim({limit: %d})", failure, limit, req.Bucket, max(limit/2, 1))
	}
	return s.readFailureMessage(req.Database, req.Bucket, failure, err)
}

// noteDelivery counts what a delivery apply did besides its verb's own work
// (claim-ack-spec 11.1), once, on the apply that answers the client.
func (s *Server) noteDelivery(ctx context.Context, database, bucket string, report store.DeliveryReport) {
	if !answeringApply(ctx) {
		return
	}
	queueEntriesReturned.Add(float64(report.Returned))
	s.noteDeadLettered(database, bucket, report.DeadLettered, 0)
	queueEntriesDeadLettered.WithLabelValues(store.DeadReasonMaxDeliveries).Add(float64(report.DeadLettered - report.Collided))
	queueEntriesDeadLettered.WithLabelValues(store.DeadReasonCollided).Add(float64(report.Collided))
	if report.ClockStep == 0 {
		return
	}
	queueLeaseClockSteps.Inc()
	if logged, unlogged := s.leaseClockSteps.due(time.Now()); logged {
		s.logger.Warn("a lease time was behind its bucket's lease clock by more than the tolerance, so the clock the bucket had already reached stood. "+
			"A node clock that steps back this far is worth finding: check ntp or chrony on the node that owns the bucket",
			"database", database, "bucket", bucket, "step_ms", report.ClockStep, "further_steps_not_logged", unlogged)
	}
}

// AckCtx acknowledges the named entries of a lease, or all of them.
func (s *Server) AckCtx(ctx context.Context, req LeaseRequest) (*LeaseResponse, error) {
	return s.leaseVerbCtx(ctx, "ack", req, func(engine leaseEngine, info store.ApplyInfo, args store.LeaseArgs, itemIDs [][]byte, _ int64) (store.LeaseResult, error) {
		return engine.ack(info, args, itemIDs)
	})
}

// ReleaseCtx gives a lease's entries back to the queue.
func (s *Server) ReleaseCtx(ctx context.Context, req LeaseRequest) (*LeaseResponse, error) {
	return s.leaseVerbCtx(ctx, "release", req, func(engine leaseEngine, info store.ApplyInfo, args store.LeaseArgs, _ [][]byte, _ int64) (store.LeaseResult, error) {
		return engine.release(info, args)
	})
}

// ExtendCtx moves a lease's deadline out.
func (s *Server) ExtendCtx(ctx context.Context, req LeaseRequest) (*LeaseResponse, error) {
	return s.leaseVerbCtx(ctx, "extend", req, func(engine leaseEngine, info store.ApplyInfo, args store.LeaseArgs, _ [][]byte, leaseMs int64) (store.LeaseResult, error) {
		return engine.extend(info, args, leaseMs)
	})
}

// leaseVerbCtx is the shared body of ack, release and extend: every check the
// door makes is repeated here (claim-ack-spec 9.3), the lease is looked up
// once, and an unknown or expired lease is an in-band answer, never an error.
func (s *Server) leaseVerbCtx(ctx context.Context, verb string, req LeaseRequest,
	run func(engine leaseEngine, info store.ApplyInfo, args store.LeaseArgs, itemIDs [][]byte, leaseMs int64) (store.LeaseResult, error)) (*LeaseResponse, error) {
	bucket, unlock, err := s.lockBucket(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer unlock()

	engine, ok := leaseEngineOf(bucket)
	if !ok {
		return nil, wrongTypeSentence(claimRefusal(verb, bucket.bucketType), bucket.bucketType)
	}
	lease, err := leaseIDOf(req.LeaseID, req.Bucket)
	if err != nil {
		return nil, err
	}
	var itemIDs [][]byte
	var leaseMs int64
	switch verb {
	case "ack":
		if err := itemIDCount(len(req.ItemIDs), req.Bucket); err != nil {
			return nil, err
		}
		if itemIDs, err = itemIDsOf(req.ItemIDs, bucket.bucketType, req.Bucket); err != nil {
			return nil, err
		}
	case "extend":
		if leaseMs, err = leaseLength(req.LeaseMs, req.Bucket, true); err != nil {
			return nil, err
		}
	}
	now, err := s.leaseNow(ctx, verb, req.LeaseNow)
	if err != nil {
		return nil, err
	}

	args := store.LeaseArgs{LeaseID: lease, Now: now, NameHash: store.BucketNameHash(req.Database, req.Bucket)}
	result, err := run(engine, applyInfoFrom(ctx, s), args, itemIDs, leaseMs)
	s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: strings.ToUpper(verb),
		emptyPop: err == nil && !result.Delivery.Wrote}, err)
	if err != nil {
		return &LeaseResponse{Message: s.leaseFailureMessage(req, verb, engine, err)}, nil
	}
	s.noteDelivery(ctx, req.Database, req.Bucket, result.Delivery)
	if answeringApply(ctx) {
		queueLeaseSettlements.WithLabelValues(verb, string(result.State)).Inc()
		// A release writes its own entries back outside the return step, so
		// DeliveryReport.Returned does not hold them; the counter's help
		// promises both.
		queueEntriesReturned.Add(float64(result.Released))
	}
	return leaseAnswer(req, verb, leaseMs, result), nil
}

// leaseAnswer is the in-band answer to a lease verb. A lease that is unknown or
// expired is Success false with its state and no write_outcome, because the
// apply that answered it was a write (Q-7).
func leaseAnswer(req LeaseRequest, verb string, leaseMs int64, result store.LeaseResult) *LeaseResponse {
	answer := &LeaseResponse{LeaseState: string(result.State)}
	switch result.State {
	case store.LeaseAcked:
		answer.Success, answer.Acked, answer.Missing, answer.Remaining = true, result.Acked, result.Missing, result.Remaining
	case store.LeaseReleased:
		answer.Success, answer.Released = true, result.Released
	case store.LeaseExtended:
		answer.Success, answer.LeaseMs = true, leaseMs
		answer.LeaseDeadline = time.UnixMilli(result.Deadline).UTC()
	case store.LeaseExpired:
		answer.Message = fmt.Sprintf("lease %s on %s.%s expired at %s (bucket clock); its entries go back to the queue on the next claim or pop, and will be delivered again. "+
			"Nothing was %s. Extend a lease before it ends with db.%s.extend({lease_id: \"…\", lease: \"60s\"})",
			shortLease(req.LeaseID), req.Database, req.Bucket, time.UnixMilli(result.Deadline).UTC().Format(time.RFC3339), settledWord(verb), req.Bucket)
	default:
		answer.Message = fmt.Sprintf("lease %s is unknown on %s.%s: it was acknowledged, released or returned already, or %s.%s did not issue it. "+
			"Nothing was changed. Claim again with db.%s.claim()", shortLease(req.LeaseID), req.Database, req.Bucket, req.Database, req.Bucket, req.Bucket)
	}
	return answer
}

// shortLease is a lease id as a message spells it: its first four and last two
// characters, enough to find it in a log.
func shortLease(text string) string {
	if len(text) < 8 {
		return text
	}
	return text[:4] + "…" + text[len(text)-2:]
}

// settledWord is what an unsettled lease verb did not do.
func settledWord(verb string) string {
	switch verb {
	case "ack":
		return "acknowledged"
	case "release":
		return "released"
	}
	return "extended"
}

// leaseFailureMessage words a lease verb the store failed.
func (s *Server) leaseFailureMessage(req LeaseRequest, verb string, engine leaseEngine, err error) string {
	failure := engine.noun + " " + verb + " failed"
	if errors.Is(err, store.ErrQueueCursorLost) {
		counted, _ := engine.entries()
		return fmt.Sprintf("%s: the %s's state counts %d entries and none is at its cursor. "+
			"On a cluster, restore this bucket from a replica with 'hoarddb dump' on the replica and 'restore' here. "+
			"On a standalone node, restore it from a dump or backup", failure, engine.noun, counted)
	}
	return s.readFailureMessage(req.Database, req.Bucket, failure, err)
}
