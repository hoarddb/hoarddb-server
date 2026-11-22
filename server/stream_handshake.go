package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The stream handshake (spec §19.3.3 as §20.4 replaces its tables).
//
// A position is a fact about one life of one log. Every push of a sender's
// stream names the sender's current incarnation (storage.WAL.Incarnation),
// and a receiver applies and acknowledges an entry only under the incarnation
// it has recorded for that sender. Before a sender ships anything on a stream
// it has not opened in this process, or after a refusal, it opens: the
// receiver answers with what it has recorded, and the sender judges that
// against its incarnation chain and asks the receiver to rebase — clamp its
// position to what is still valid in the log as it is now, and record the
// current incarnation — durably, before the answer leaves.
//
// This file is the receiver's half and the sender's handshake call.

// The refusals a stream push can be answered with (ReplPushResponse.Refusal).
const (
	// refusalReopen: the push names an incarnation the receiver has not
	// recorded for the sender, a rebase names a record the receiver no
	// longer holds, or the push is neither a stream push nor a forwarded
	// request. Nothing was applied or acknowledged; the sender opens again.
	refusalReopen = "reopen"
	// refusalAwaitingCopy: the push carries an entry of a bucket this node is
	// placed on and does not hold yet. Nothing at or past it is acknowledged.
	refusalAwaitingCopy = "awaiting_copy"
	// refusalSchemaPaused: a forwarded schema change reached an owner whose
	// view pauses schema changes while a node joins (ErrSchemaChangePaused).
	// Nothing was logged or applied; Error carries the owner's text.
	refusalSchemaPaused = "schema_paused"
	// refusalNotOwner: a forwarded write reached a node that does not own its
	// bucket (spec §19.3.6 R-2). Nothing was logged or applied.
	refusalNotOwner = "not_owner"
	// refusalEpoch: a push that changes state came from a node on another
	// membership epoch (spec §19.3.7). ReplPushResponse.Epoch carries the
	// receiver's. Nothing was applied or acknowledged.
	refusalEpoch = "epoch"
	// refusalMemberMismatch: the receiver's membership disagrees with a
	// peer's (Cluster.mismatch), so it applies nothing and answers nothing as
	// owner until the disagreement clears (BUG-0704). Transient: the
	// coordinator answers its client ErrOwnerMembershipMismatch, a retry.
	refusalMemberMismatch = "member_mismatch"
	// refusalConcernNotMet: a forwarded write is stored on the owner and its
	// write concern was not met in time (ErrWriteConcernTimeout).
	refusalConcernNotMet = "write_concern"
	// refusalOwnershipMoved: the owner stopped owning a forwarded write's
	// bucket while the write was in flight (ErrOwnershipMoved).
	refusalOwnershipMoved = "ownership_moved"
	// refusalAlterMemberLacksFeature, refusalAlterAwaitsPass,
	// refusalAlterCopyPending and refusalAlterNotCovered: a forwarded alter the
	// owner refused for a reason a retry clears (R4, R7, R8, R11; BUG-0643).
	// Nothing was logged; Error carries the owner's sentence, which the
	// coordinator hands its client unchanged.
	refusalAlterMemberLacksFeature = "alter_member_lacks_feature"
	refusalAlterAwaitsPass         = "alter_awaits_placement_pass"
	refusalAlterCopyPending        = "alter_copy_pending"
	refusalAlterNotCovered         = "alter_decrease_not_covered"
	// refusalAlterBucketNotFound: a forwarded alter of a bucket no member holds
	// (BUG-0685). Not a retry: the coordinator answers NOT_FOUND with the
	// owner's text, the standard missing-bucket sentence.
	refusalAlterBucketNotFound = "alter_bucket_not_found"
	// refusalBucketNotFound: any other forwarded mutation of a bucket no member
	// holds (BUG-0686), answered as the alter's is.
	refusalBucketNotFound = "bucket_not_found"
)

// recordedStream is what a receiver holds about the log its position for a
// source was learned from: the source's incarnation and its log id. Both are
// empty on a record from a build before the handshake.
type recordedStream struct {
	incarnation []byte
	log         []byte
	// clamped says a rebase lowered this node's position for the source: its
	// log went backwards (a restored or re-created log), and the sequences it
	// writes now repeat ones a copy's position counts. Its entries are then
	// judged by term and by this per-source position only, never by the
	// copy's (step 10b.2).
	clamped bool
}

// lockStreamFrom serialises the pushes of one source's stream and returns the
// unlock. The sender sends one at a time; what this orders is a push still in
// flight from before a stall against the handshake that follows it.
func (c *Cluster) lockStreamFrom(source string) func() {
	c.streamFromMuLock.Lock()
	sourceMu, ok := c.streamFromMu[source]
	if !ok {
		sourceMu = &sync.Mutex{}
		c.streamFromMu[source] = sourceMu
	}
	c.streamFromMuLock.Unlock()
	sourceMu.Lock()
	return sourceMu.Unlock
}

// recordedPosition is what this node has recorded for source, as a handshake
// reports it. Known is false only when it holds neither a position nor a
// snapshot position for source.
func (c *Cluster) recordedPosition(source string) ReplPosition {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	applied, hasPosition := c.appliedFrom[source]
	recorded := c.recordedStream[source]
	return ReplPosition{
		Known:          hasPosition || len(c.seededThrough[source]) > 0 || len(c.legacySeeded[source]) > 0,
		Incarnation:    bytes.Clone(recorded.incarnation),
		Log:            bytes.Clone(recorded.log),
		AppliedThrough: applied,
		HighWater:      max(applied, c.seedCeiling[source]),
	}
}

// streamIncarnationIs reports whether incarnation is the one this node has
// recorded for source. Nothing matches an empty record: a source whose
// incarnation this node has not recorded is asked to open first.
func (c *Cluster) streamIncarnationIs(source string, incarnation []byte) bool {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	recorded := c.recordedStream[source].incarnation
	return len(recorded) > 0 && bytes.Equal(recorded, incarnation)
}

// answerHandshake serves a push with Stream.Open set. Without a rebase it
// reports what this node has recorded for the sender and changes nothing.
// With one it rebases first, durably, and reports the result.
func (c *Cluster) answerHandshake(source string, stream *ReplStream) ReplPushResponse {
	if stream.Rebase == nil {
		position := c.recordedPosition(source)
		return ReplPushResponse{Success: true, AppliedThrough: position.AppliedThrough, Position: &position}
	}
	position, err := c.rebaseStream(source, stream.Incarnation, *stream.Rebase)
	if errors.Is(err, errRebaseOfAnotherRecord) {
		return ReplPushResponse{Success: false, Refusal: refusalReopen, Error: err.Error()}
	}
	if err != nil {
		c.logger.Error("could not rebase the replication position of a source; its stream is not opened", "from", source, "error", err)
		return ReplPushResponse{Success: false, Error: err.Error()}
	}
	return ReplPushResponse{Success: true, AppliedThrough: position.AppliedThrough, Position: &position}
}

// errRebaseOfAnotherRecord refuses a rebase that names a record this node no
// longer holds: the sender judged an answer that is out of date.
var errRebaseOfAnotherRecord = errors.New("the rebase names a record of the source's log that this node no longer holds; open the stream again")

// snapshotPositionAbove is one snapshot position a rebase lowers.
type snapshotPositionAbove struct {
	key      []byte
	database string // empty for a legacy bare-name position
	bucket   string
}

// rebaseStream moves source's record onto the incarnation the sender names
// (spec §19.3.3 step 3): if what is recorded is still rebase.From, the
// position becomes max(min(position, ValidThrough), StartAt), every snapshot
// position above ValidThrough is lowered to it, the incarnation and log id are
// recorded, and rebased_at becomes this node's own last sequence, so the log
// rebuild at start counts none of this source's entries logged before it. All
// of it is one synced batch, and memory changes only once it is committed.
func (c *Cluster) rebaseStream(source string, incarnation []byte, rebase ReplRebase) (ReplPosition, error) {
	c.positionMu.Lock()
	defer c.positionMu.Unlock()

	c.appliedMu.Lock()
	if !bytes.Equal(c.recordedStream[source].incarnation, rebase.From) {
		c.appliedMu.Unlock()
		return ReplPosition{}, fmt.Errorf("%w (%s)", errRebaseOfAnotherRecord, source)
	}
	applied := max(min(c.appliedFrom[source], rebase.ValidThrough), rebase.StartAt)
	var lowered []snapshotPositionAbove
	for bucket, perDatabase := range c.seededThrough[source] {
		for database, floor := range perDatabase {
			if floor > rebase.ValidThrough {
				lowered = append(lowered, snapshotPositionAbove{key: seededKey(source, database, bucket), database: database, bucket: bucket})
			}
		}
	}
	for bucket, floor := range c.legacySeeded[source] {
		if floor > rebase.ValidThrough {
			lowered = append(lowered, snapshotPositionAbove{key: legacySeededKey(source, bucket), bucket: bucket})
		}
	}
	// A snapshot position lowered is as much a log gone backwards as a stream
	// position lowered: the copy's lineage position still carries the old
	// log's sequence, which the new log reuses (BUG-0621). The clamp costs
	// nothing per bucket and the lowered positions still cover the valid prefix.
	clamped := c.recordedStream[source].clamped || applied < c.appliedFrom[source] || len(lowered) > 0
	// A rebase keeps the orphan void: the entries it names are still in this
	// node's log, and replay must still pass over them.
	void := c.orphanVoids[source]
	c.appliedMu.Unlock()

	// A rebase that carries the position at or past a recorded size refusal
	// passes that refusal: the entry the source was stalled on is at or below
	// the position this record declares. It is cleared in the rebase's own
	// batch, because a delete left for later (or for the next start's scan)
	// would lower the position below an entry the rebase has passed.
	c.memtableMu.Lock()
	refusal, refused := c.batchSizeRefusals[source]
	c.memtableMu.Unlock()
	clearSizeRefusal := refused && applied >= refusal.Seq

	record := positionRecord{
		Incarnation: bytes.Clone(incarnation), Log: bytes.Clone(rebase.Log),
		AppliedThrough: applied, RebasedAt: c.walLastSequence(), Clamped: clamped,
		VoidedAbove: void.above, VoidedThrough: void.through,
	}
	if err := c.commitRebase(source, record, lowered, rebase.ValidThrough, clearSizeRefusal); err != nil {
		return ReplPosition{}, err
	}
	if clearSizeRefusal {
		// Only the record the rebase read: a refusal recorded for a newer
		// sequence since then is not passed by this rebase (as in
		// settleSizeRefusal).
		c.memtableMu.Lock()
		if c.batchSizeRefusals[source] == refusal {
			delete(c.batchSizeRefusals, source)
		}
		c.memtableMu.Unlock()
	}

	c.appliedMu.Lock()
	c.appliedFrom[source] = applied
	c.rebasedAt[source] = record.RebasedAt
	c.recordedStream[source] = recordedStream{incarnation: record.Incarnation, log: record.Log, clamped: record.Clamped}
	c.capSnapshotsInFlightLocked(source, rebase.ValidThrough)
	c.lowerSnapshotPositionsLocked(source, lowered, rebase.ValidThrough)
	// The record now in the store is the position in memory: no checkpoint
	// is owed for a move made before it.
	delete(c.checkpointOwed, source)
	c.appliedMu.Unlock()
	return c.recordedPosition(source), nil
}

// commitRebase writes a rebase's "p" record and its lowered snapshot positions
// in one batch and syncs it. A position lowered to zero covers nothing and is
// deleted. A rebase that passed a recorded size refusal deletes that record in
// the same batch, so no crash can leave a record below the position it
// declares.
func (c *Cluster) commitRebase(source string, record positionRecord, lowered []snapshotPositionAbove, validThrough uint64, clearSizeRefusal bool) error {
	encoded, err := bson.Marshal(record)
	if err != nil {
		return fmt.Errorf("encoding the rebased position record of source %q: %w", source, err)
	}
	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(positionKey(source), encoded); err != nil {
		return fmt.Errorf("adding the rebased position record of source %q: %w", source, err)
	}
	if clearSizeRefusal {
		if err := batch.Delete(sizeRefusalKey(source)); err != nil {
			return fmt.Errorf("clearing the size refusal of source %q in its rebase: %w", source, err)
		}
	}
	for _, position := range lowered {
		if validThrough == 0 {
			err = batch.Delete(position.key)
		} else {
			err = batch.Set(position.key, encodeFloor(validThrough))
		}
		if err != nil {
			return fmt.Errorf("lowering a snapshot position of %s/%s from %q: %w", position.database, position.bucket, source, err)
		}
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("committing the rebase of source %q's position: %w", source, err)
	}
	return nil
}

// lowerSnapshotPositionsLocked applies a committed rebase's lowered snapshot
// positions in memory. The caller holds appliedMu.
func (c *Cluster) lowerSnapshotPositionsLocked(source string, lowered []snapshotPositionAbove, validThrough uint64) {
	floors := c.seededThrough[source]
	for _, position := range lowered {
		if position.database == "" {
			if validThrough == 0 {
				delete(c.legacySeeded[source], position.bucket)
			} else {
				c.legacySeeded[source][position.bucket] = validThrough
			}
			continue
		}
		if validThrough == 0 {
			floors.drop(position.database, position.bucket)
		} else {
			floors[position.bucket][position.database] = validThrough
		}
	}
	if len(floors) == 0 {
		delete(c.seededThrough, source)
	}
	if len(c.legacySeeded[source]) == 0 {
		delete(c.legacySeeded, source)
	}
	var ceiling uint64
	for _, perDatabase := range floors {
		for _, floor := range perDatabase {
			ceiling = max(ceiling, floor)
		}
	}
	c.seedCeiling[source] = ceiling
}

// refusePush is the answer to a stream push this node refuses before it reads
// any entry of it.
func refusePush(frame *transport.Frame, refusal string, cause error) (*transport.Frame, error) {
	return nodeResponse(transport.OpReplAck, frame, ReplPushResponse{Success: false, Refusal: refusal, Error: cause.Error()}, nil)
}

// wireSequence is a sequence as BSON can carry it: the encoder refuses a uint64
// above math.MaxInt64, which is what an unbounded limit is. No log reaches it.
func wireSequence(sequence uint64) uint64 {
	return min(sequence, math.MaxInt64)
}

// pushChangesState reports whether any entry of a push changes state.
func pushChangesState(entries []ReplEntry) bool {
	for _, entry := range entries {
		if replicationRequired(transport.Opcode(entry.Op)) {
			return true
		}
	}
	return false
}

// handshake opens this node's stream to p (spec §19.3.3, with §20.4's
// tables): it asks what p has recorded for this node, judges that against the
// incarnation chain, asks p to rebase when the judgement says so, and adopts
// the position p then reports as the stream's cursor. Nothing p says makes this
// node mint a link: links are minted only by opening the log.
//
// p's first answer also says how far p holds this node's log, whichever life
// of it p learned that in. The start hold hears it, and the handshake waits for
// the hold to open before it judges: a rebase lowers p's position, and the log
// must have moved past what p held first, or a crash between the two would
// forget it (start_hold.go).
func (p *peer) handshake() error {
	chain := p.c.s.wal.Incarnations()
	current := chain[len(chain)-1].ID
	position, err := p.openWithin(&ReplStream{Incarnation: current[:], Open: true})
	if err != nil {
		return err
	}
	if err := p.c.startHold.heardFrom(p.node.ID, max(position.AppliedThrough, position.HighWater)); err != nil {
		return err
	}
	if !p.c.startHold.await(p.stopCh) {
		return errStreamLeft
	}
	rebase, err := p.judge(position, chain)
	if err != nil {
		return err
	}
	if rebase != nil {
		logID := chain[0].ID
		rebase.From, rebase.Log = position.Incarnation, logID[:]
		if position, err = p.openWithin(&ReplStream{Incarnation: current[:], Open: true, Rebase: rebase}); err != nil {
			return err
		}
	}
	p.setCursor(position.AppliedThrough)
	return nil
}

// openWithin is open bounded by one ship's timeout.
func (p *peer) openWithin(stream *ReplStream) (ReplPosition, error) {
	ctx, cancel := context.WithTimeout(p.c.ctx, p.c.replShipTimeout())
	defer cancel()
	return p.open(ctx, stream)
}

// noteDivergence logs, once per horizonWarnInterval, that p had applied this
// node's log further than the log now reaches (spec §19.3.3, the second
// message): it holds writes this node no longer has.
func (p *peer) noteDivergence(appliedThrough, validThrough uint64) {
	if appliedThrough <= validThrough {
		return
	}
	p.logRateLimited(slog.LevelWarn, fmt.Sprintf("%s had applied this node's log through %d; this log now ends at %d. "+
		"It holds up to %d writes this node no longer has. New writes replicate normally. "+
		"To make its copies identical, remove it ('remove-node %s'), empty its data directory and add it again",
		p.node.ID, appliedThrough, validThrough, appliedThrough-validThrough, p.node.ID))
}

// errHandshakeRefusedForGood marks a refusal to open a stream that no retry
// clears: the same handshake meets the same answer until an operator acts. The
// sender goes on trying (the peer may be replaced), but a pull is told so
// instead of waiting for the stream to open (BUG-0310).
var errHandshakeRefusedForGood = errors.New("the stream is refused for good")

// refuseLostChain is the refusal to open a stream when this node's log has no
// incarnation chain of its own and p holds a position under one (spec §19.3.3,
// the third message, and SA-4.3): this node cannot tell which of p's positions
// are current, and does not guess.
func (p *peer) refuseLostChain() error {
	// Named by id, never "this node": a puller reads this text too (migrate's
	// output, a sweep's WARN), where "this node" is the puller, whose data
	// directory may hold the only good copy (BUG-0310).
	source := p.c.self.ID
	err := fmt.Errorf("%w: %s's wal/incarnation.bson is missing and its log is not: %s cannot tell which of %s's positions are current. "+
		"Restore that file on %s, or run 'remove-node %s', empty %s's data directory and add it again",
		errHandshakeRefusedForGood, source, source, p.node.ID, source, source, source)
	p.logRateLimited(slog.LevelError, err.Error())
	return err
}

// judge decides, from what p reports, whether p's record is still valid in
// this log as it is now, and returns the rebase that makes it so, or nil when
// p is on the current incarnation. Row by row (spec §20.4):
//
//	nothing recorded                              rebase, start_at = first retained − 1
//	the current incarnation                       open; no rebase
//	an earlier link, or no id and a legacy first  rebase, valid_through = limit(link)
//	no id, and the first link is not legacy       a wipe: valid_through = 0
//	an id not in the chain, a legacy first link   refuse to open: the chain was lost (SA-4.3)
//	an id not in the chain, this log's id         a restore: valid_through = the current link's base
//	an id not in the chain, another log's id      a wipe: valid_through = 0
//	an id not in the chain, no log id             refuse to open: this build cannot produce it
func (p *peer) judge(position ReplPosition, chain []storage.WALIncarnation) (*ReplRebase, error) {
	wal := p.c.s.wal
	current := chain[len(chain)-1]
	logID := chain[0].ID
	if !position.Known {
		first, err := wal.FirstSequence()
		if err != nil {
			return nil, fmt.Errorf("reading the first sequence of the log to open the stream to %s: %w", p.node.ID, err)
		}
		return &ReplRebase{StartAt: first - 1}, nil
	}
	if bytes.Equal(position.Incarnation, current.ID[:]) {
		if tail := wal.LastSequence(); position.AppliedThrough > tail {
			err := fmt.Errorf("%s reports a position %d in this node's log, under the incarnation this process opened, "+
				"and the log ends at %d: the stream to it is not opened", p.node.ID, position.AppliedThrough, tail)
			p.logRateLimited(slog.LevelError, err.Error())
			return nil, err
		}
		return nil, nil
	}
	if limit, found := storage.ValidThrough(chain, position.Incarnation); found {
		p.noteDivergence(position.AppliedThrough, limit)
		return &ReplRebase{ValidThrough: wireSequence(limit)}, nil
	}
	switch {
	case len(position.Incarnation) == 0:
		// A record from a build before the handshake, meeting a log this
		// build created: the log it was learned in is gone — wiped, or lost
		// across the upgrade — so it is the wipe row.
		return &ReplRebase{}, nil
	case chain[0].Legacy:
		return nil, p.refuseLostChain()
	case bytes.Equal(position.Log, logID[:]):
		p.noteDivergence(position.AppliedThrough, current.Base)
		return &ReplRebase{ValidThrough: current.Base}, nil
	case len(position.Log) > 0:
		return &ReplRebase{}, nil
	default:
		err := fmt.Errorf("%w: %s holds a position under an incarnation of this node's log that the log does not hold, with no log id: "+
			"this build does not write such a record, so the stream to it is not opened. Remove %s ('remove-node %s'), empty its data directory "+
			"and add it again", errHandshakeRefusedForGood, p.node.ID, p.node.ID, p.node.ID)
		p.logRateLimited(slog.LevelError, err.Error())
		return nil, err
	}
}

// sourceClamped reports whether a rebase lowered this node's position for
// source (recordedStream.clamped).
func (c *Cluster) sourceClamped(source string) bool {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	return c.recordedStream[source].clamped
}
