package server

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// The dead verbs on the wire (claim-ack-spec 8.3, 9): list the entries that were
// claimed too many times, write them back to the queue, or drop them.

// ErrDeadOption is under every refusal of a dead verb's options: a dead_id that
// is not one a list returned, both or neither of dead_id and all, a list limit
// out of bounds, a restore record no live path could have written. It reaches
// the wire as INVALID_ARGUMENT.
var ErrDeadOption = errors.New("invalid dead option")

// ErrDeadRestoreOnly is a client frame that carries DEAD_RESTORE, which only a
// restore sends. It reaches the wire as PERMISSION_DENIED.
var ErrDeadRestoreOnly = errors.New("restore-only: DEAD_RESTORE is sent by a restore, never by a client. Run: hoarddb restore <dir>")

// deadItemOverhead is what a listed record costs on the wire beyond its data:
// the id, the delivery count, the time and the reason, rounded up.
const deadItemOverhead = 160

// maxDeadIDBytes is the longest dead suffix there can be: a heap's 8-byte
// priority, an id of at most store.MaxClaimIDBytes and the 8-byte dead sequence.
const maxDeadIDBytes = 8 + store.MaxClaimIDBytes + 8

// deadListLimit is the page size a dead list asks for: 0 is 10, and the most is
// store.MaxDeadListLimit.
func deadListLimit(requested int, bucket string) (int, error) {
	switch {
	case requested < 0:
		return 0, fmt.Errorf("%w: limit %d is not valid; db.%s.dead({limit: N}) takes a whole number of 1 or more", ErrDeadOption, requested, bucket)
	case requested > store.MaxDeadListLimit:
		return 0, fmt.Errorf("%w: limit %d is above the dead list maximum of %d. List a page at a time: db.%s.dead({limit: %d}), then pass the next it returns as after",
			ErrDeadOption, requested, store.MaxDeadListLimit, bucket, store.MaxDeadListLimit)
	case requested == 0:
		return 10, nil
	}
	return requested, nil
}

// deadIDOf reads a dead_id: the hex of a dead suffix. A malformed one is refused
// here, never in a store verb.
func deadIDOf(text, bucket string) ([]byte, error) {
	decoded, err := hex.DecodeString(text)
	if err != nil || len(decoded) == 0 || len(decoded) > maxDeadIDBytes {
		return nil, fmt.Errorf("%w: dead_id %q is not a dead id. A dead_id is the value db.%s.dead() answered in dead_id; list what is there and copy it",
			ErrDeadOption, abbreviated(text, 64), bucket)
	}
	return decoded, nil
}

// deadSelectorOf reads which dead entries a requeue or a discard names: one by
// its dead_id, or all, and exactly one of the two.
func deadSelectorOf(req DeadMoveRequest, verb string) (store.DeadSelector, error) {
	if req.After != "" && !req.All {
		return store.DeadSelector{}, fmt.Errorf("%w: %s's after pages an all request: db.%s.%s({all: true, after: \"<dead_id>\"}); a single entry is named by dead_id alone",
			ErrDeadOption, verb, req.Bucket, verb)
	}
	if req.All == (req.DeadID != "") {
		return store.DeadSelector{}, fmt.Errorf("%w: %s names one dead entry or all of them, not both and not neither: db.%s.%s({dead_id: \"…\"}) or db.%s.%s({all: true})",
			ErrDeadOption, verb, req.Bucket, verb, req.Bucket, verb)
	}
	if req.All {
		after, err := deadMoveAfter(req)
		if err != nil {
			return store.DeadSelector{}, err
		}
		return store.DeadSelector{All: true, After: after}, nil
	}
	deadID, err := deadIDOf(req.DeadID, req.Bucket)
	if err != nil {
		return store.DeadSelector{}, err
	}
	return store.DeadSelector{DeadID: deadID}, nil
}

// deadMoveAfter reads an all request's after: nil for none, else a dead_id.
func deadMoveAfter(req DeadMoveRequest) ([]byte, error) {
	if req.After == "" {
		return nil, nil
	}
	return deadIDOf(req.After, req.Bucket)
}

// resolveDeadList refuses, before anything is read, a dead list whose limit or
// after is out of bounds.
func resolveDeadList(payload []byte) error {
	raw := bson.Raw(payload)
	bucket, _ := raw.Lookup("bucket").StringValueOK()
	if limit, isNumber := raw.Lookup("limit").AsInt64OK(); isNumber {
		if _, err := deadListLimit(int(limit), bucket); err != nil {
			return err
		}
	}
	if after, _ := raw.Lookup("after").StringValueOK(); after != "" {
		if _, err := deadIDOf(after, bucket); err != nil {
			return err
		}
	}
	return nil
}

// resolveDeadMove is resolveDeadList for a requeue or a discard.
func resolveDeadMove(payload []byte, verb string) error {
	raw := bson.Raw(payload)
	bucket, _ := raw.Lookup("bucket").StringValueOK()
	deadID, _ := raw.Lookup("dead_id").StringValueOK()
	all, _ := raw.Lookup("all").BooleanOK()
	after, _ := raw.Lookup("after").StringValueOK()
	_, err := deadSelectorOf(DeadMoveRequest{Bucket: bucket, DeadID: deadID, All: all, After: after}, verb)
	return err
}

// deadEngine is what the dead verbs need of a bucket's queue or heap store.
type deadEngine struct {
	list    func(limit int, after []byte, visit func(store.DeadRecord) error) ([]byte, bool, error)
	requeue func(info store.ApplyInfo, sel store.DeadSelector) (store.DeadMove, error)
	discard func(info store.ApplyInfo, sel store.DeadSelector) (store.DeadMove, error)
	restore func(info store.ApplyInfo, record store.DeadRecord) error
	isHeap  bool
}

// deadEngineOf is the dead verbs of a fifo, lifo or heap bucket, and false for
// any other type.
func deadEngineOf(bucket *inMemoryBucket) (deadEngine, bool) {
	switch bucket.bucketType {
	case BucketTypeFIFO, BucketTypeLIFO:
		queue := bucket.queueStore
		return deadEngine{queue.DeadList, queue.Requeue, queue.Discard, queue.RestoreDead, false}, true
	case BucketTypeHeap:
		heap := bucket.heapStore
		return deadEngine{heap.DeadList, heap.Requeue, heap.Discard, heap.RestoreDead, true}, true
	}
	return deadEngine{}, false
}

// deadBanner is the line that tells a developer a bucket holds dead entries and
// the three commands that deal with them.
func deadBanner(bucket string, dead int64) string {
	noun := "entries"
	if dead == 1 {
		noun = "entry"
	}
	return fmt.Sprintf("db.%s has %d dead %s (claimed %d times without an acknowledgement).\n"+
		"  Read them:     db.%s.dead()\n  Retry them:    db.%s.requeue({all: true})\n  Drop them:     db.%s.discard({all: true})",
		bucket, dead, noun, store.MaxDeliveries, bucket, bucket, bucket)
}

// DeadList lists a bucket's dead entries from one snapshot, oldest first, taking
// no lock and writing nothing. Each record is charged to the result budget as a
// peek charges its entries, under the DEAD label.
func (s *Server) DeadList(req DeadListRequest) (*DeadListResponse, error) {
	bucket, snap, err := s.resolveForRead(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer snap.Discard()

	engine, ok := deadEngineOf(bucket)
	if !ok {
		return nil, wrongTypeSentence(claimRefusal("dead", bucket.bucketType), bucket.bucketType)
	}
	limit, err := deadListLimit(req.Limit, req.Bucket)
	if err != nil {
		return nil, err
	}
	var after []byte
	if req.After != "" {
		if after, err = deadIDOf(req.After, req.Bucket); err != nil {
			return nil, err
		}
	}

	refusal := resultTooLargeError{query: fmt.Sprintf("dead({limit: %d}) on %s.%s", limit, req.Database, req.Bucket), shape: resultShapeDead, limit: limit}
	budget := s.newResultBudget(queryMethodDead, refusal)
	defer budget.report()
	walk := newQueryWalk(context.Background(), queryMethodDead, req.Database, req.Bucket)
	var items []DeadItem
	visit := func(record store.DeadRecord) error {
		if err := walk.visit(); err != nil {
			return err
		}
		cost := int64(len(record.Data)) + deadItemOverhead + elementHeader(len(items))
		if len(items) == 0 {
			budget.chargeFirst(cost)
		} else if err := budget.chargeDocument(cost); err != nil {
			return err
		}
		items = append(items, DeadItem{
			DeadID: hex.EncodeToString(record.DeadID), Data: record.Data, Deliveries: record.Deliveries,
			DeadAt: time.UnixMilli(record.DeadAt).UTC(), Reason: record.Reason,
		})
		return nil
	}
	next, stoppedShort, err := engine.list(limit, after, visit)
	if err != nil {
		// A refusal leaves as an error so errorFrame sets RESULT_TOO_LARGE; as
		// found:false it would read as a list with nothing in it.
		if errors.Is(err, ErrResultTooLarge) {
			return nil, err
		}
		return &DeadListResponse{Message: s.readFailureMessage(req.Database, req.Bucket, "dead list failed", err)}, nil
	}
	return deadListAnswer(items, next, stoppedShort, budget.refusal, deadBannerOf(bucket, snap, req.Bucket))
}

// deadListAnswer is the response to a list that returned. A nil next with
// stoppedShort false is the only complete list; a list the visitor stopped
// before its first record has listed nothing and is not complete, so it is
// answered as the refusal it is, never as an empty page (coordinator decision
// 2026-10-18, D3 verification error 3).
func deadListAnswer(items []DeadItem, next []byte, stoppedShort bool, refusal resultTooLargeError, banner string) (*DeadListResponse, error) {
	if stoppedShort && len(items) == 0 {
		refusal.documents = 0
		return nil, &refusal
	}
	answer := &DeadListResponse{Found: len(items) > 0, Items: items}
	if next != nil {
		answer.Next = hex.EncodeToString(next)
	}
	if answer.Found {
		answer.Message = banner
	}
	return answer, nil
}

// deadBannerOf is the banner for a bucket's dead count, read from the snapshot
// the list was served from; empty when the state cannot be read, because a
// listing is no worse for lacking its banner.
func deadBannerOf(bucket *inMemoryBucket, snap storage.KVSnapshot, name string) string {
	state, err := store.LoadBucketState(snap, bucket.slot, bucket.databaseID, bucket.id.Load())
	if err != nil || state.Dead == 0 {
		return ""
	}
	return deadBanner(name, state.Dead)
}

// DeadRequeueCtx writes dead entries back to the queue, with their return
// count reset.
func (s *Server) DeadRequeueCtx(ctx context.Context, req DeadMoveRequest) (*DeadMoveResponse, error) {
	return s.deadMoveCtx(ctx, "requeue", req)
}

// DeadDiscardCtx drops dead entries.
func (s *Server) DeadDiscardCtx(ctx context.Context, req DeadMoveRequest) (*DeadMoveResponse, error) {
	return s.deadMoveCtx(ctx, "discard", req)
}

// deadMoveCtx is the shared body of requeue and discard: one apply moves at
// most store.MaxDeadMoveLimit entries, answers how many and how many are left,
// and names every entry it refused. A refusal is in band.
func (s *Server) deadMoveCtx(ctx context.Context, verb string, req DeadMoveRequest) (*DeadMoveResponse, error) {
	bucket, unlock, err := s.lockBucket(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer unlock()

	engine, ok := deadEngineOf(bucket)
	if !ok {
		return nil, wrongTypeSentence(claimRefusal(verb, bucket.bucketType), bucket.bucketType)
	}
	selector, err := deadSelectorOf(req, verb)
	if err != nil {
		return nil, err
	}
	run := engine.requeue
	if verb == "discard" {
		run = engine.discard
	}
	move, err := run(applyInfoFrom(ctx, s), selector)
	s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: strings.ToUpper(verb), emptyPop: err == nil && !move.Wrote}, err)
	if err != nil {
		return &DeadMoveResponse{Message: s.readFailureMessage(req.Database, req.Bucket, verb+" failed", err)}, nil
	}
	if answeringApply(ctx) {
		if verb == "requeue" {
			queueDeadRequeued.Add(float64(move.Done))
		} else {
			queueDeadDiscarded.Add(float64(move.Done))
		}
	}
	return deadMoveAnswer(req, verb, engine, move), nil
}

// deadMoveAnswer is the in-band answer to a requeue or a discard.
func deadMoveAnswer(req DeadMoveRequest, verb string, engine deadEngine, move store.DeadMove) *DeadMoveResponse {
	answer := &DeadMoveResponse{Success: len(move.Refused) == 0, Done: move.Done, Left: move.Left}
	for _, refused := range move.Refused {
		answer.Refused = append(answer.Refused, DeadRefusal{DeadID: hex.EncodeToString(refused.DeadID), Message: deadRefusalMessage(req, verb, engine, refused)})
	}
	if move.Left > 0 && req.All {
		answer.Message = fmt.Sprintf("%d dead entries are left in %s.%s", move.Left, req.Database, req.Bucket)
		if len(move.Refused) > 0 {
			answer.Message += fmt.Sprintf(", %d of them refused by name above and staying where they are", len(move.Refused))
		}
		answer.Message += fmt.Sprintf(". %s moves at most %d at a time: repeat db.%s.%s({all: true}) while done is above 0", verb, store.MaxDeadMoveLimit, req.Bucket, verb)
		if len(answer.Refused) > 0 {
			answer.Message += fmt.Sprintf("; refused entries count toward the %d, so to reach the ones behind them repeat with after: %q (the last refused dead_id)", store.MaxDeadMoveLimit, answer.Refused[len(answer.Refused)-1].DeadID)
		}
	}
	return answer
}

// deadRefusalMessage words one entry a dead verb left where it was.
func deadRefusalMessage(req DeadMoveRequest, verb string, engine deadEngine, refused store.DeadRefusal) string {
	deadID := hex.EncodeToString(refused.DeadID)
	if refused.Cause == store.DeadOriginOccupied {
		return fmt.Sprintf("%s of %q refused: the heap %s.%s already holds an element with id %q at priority %d.\n"+
			"  Discard the dead copy with db.%s.discard({dead_id: %q}), or remove the live one first.",
			verb, deadID, req.Database, req.Bucket, refused.EntryID, refused.Priority, req.Bucket, deadID)
	}
	return fmt.Sprintf("no dead entry %q in %s.%s: it may have been requeued or discarded already.\n  List what is there: db.%s.dead()",
		deadID, req.Database, req.Bucket, req.Bucket)
}

// DeadRestoreCtx writes one dead entry from a dump. Only a restore reaches it:
// the dispatcher refuses the opcode on a client frame. Its record is validated
// on every copy by the store.
func (s *Server) DeadRestoreCtx(ctx context.Context, req DeadRestoreRequest) (*DeadMoveResponse, error) {
	bucket, unlock, err := s.lockBucket(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer unlock()

	engine, ok := deadEngineOf(bucket)
	if !ok {
		return nil, fmt.Errorf("%w: %s.%s is a %s bucket, and only fifo, lifo and heap buckets have dead entries to restore", ErrDeadOption, req.Database, req.Bucket, bucket.bucketType)
	}
	record := store.DeadRecord{Data: req.Data, Deliveries: req.Deliveries, DeadAt: req.DeadAt.UnixMilli(), Reason: req.Reason}
	err = engine.restore(applyInfoFrom(ctx, s), record)
	s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "DEAD_RESTORE"}, err)
	if errors.Is(err, store.ErrDeadRecordInvalid) {
		return nil, fmt.Errorf("%w: %w", ErrDeadOption, err)
	}
	if err != nil {
		return nil, fmt.Errorf("restoring a dead entry into %s.%s: %w", req.Database, req.Bucket, err)
	}
	return &DeadMoveResponse{Success: true, Done: 1}, nil
}
