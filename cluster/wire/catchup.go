package wire

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
)

// readBatchBytes bounds the log read per batch while a pair's records are collected.
const readBatchBytes = 1 << 20

// pairRead bounds one collection of a pair's records. upTo, when not zero, stops the
// read at the first record past it.
type pairRead struct {
	// from is the log sequence the read starts at, and after the position the read
	// skips up to.
	from           uint64
	after          cluster.Position
	records, bytes int
	upTo           cluster.Position
	// through, when not zero, is the log sequence of the pair's last record the sender knows of:
	// the read does not look past it, because the log is shared by every pair and only a read
	// to its end could find that the pair has nothing later (BUG-0865). A record appended after
	// the sender took it is queued for the pair afterwards, which makes the pair dirty again.
	through uint64
	// cursor, when set, is an open cursor whose next sequence is from: the read continues on it
	// and does not seek the log again.
	cursor *storage.WALCursor
	// gapFrom and gapTo, when gapTo is not zero, are two records of the pair with no record of the
	// history the second extends between them (pairSend.gapFrom; the copy's boundary check, then a
	// catch-up, is the backstop). A read that reaches gapFrom seeks gapTo instead of walking the
	// entries between, which after an epoch move are most of the log (BUG-0872).
	gapFrom, gapTo uint64
}

type pairReadResult struct {
	records []PairRecord
	// next is the log sequence after the last entry examined.
	next uint64
	// cursor is the log cursor where the read stopped, usable for the next read when cursorOK:
	// every entry it passed was examined, so its next sequence is next.
	cursor   storage.WALCursor
	cursorOK bool
}

// before is the position a requester holds when it asks from pos: the last one of the
// same epoch, or the end of the previous epoch for an opener.
func before(pos cluster.Position) cluster.Position {
	switch {
	case pos.Seq > 0:
		return cluster.Position{Epoch: pos.Epoch, Seq: pos.Seq - 1}
	case pos.Epoch > 0:
		return cluster.Position{Epoch: pos.Epoch - 1, Seq: math.MaxUint64}
	}
	return cluster.Position{}
}

// readPairRecords collects the records of one pair from log sequence from, in the log's
// order, skipping other pairs' and any record at or before after. It reads only
// what the log has fsynced. A record out of position order is an error: the stream
// never ships out of order. On an error the records read so far are still returned.
func readPairRecords(ctx context.Context, log cluster.LogReader, key pairKey, lim pairRead) (pairReadResult, error) {
	from, after := lim.from, lim.after
	out := pairReadResult{next: from}
	// stop is the last sequence the read may examine: what the log has fsynced, and no further
	// than the pair's last known record.
	stop := log.SyncedSequence()
	if lim.through != 0 {
		stop = min(stop, lim.through)
	}
	if from > stop {
		if lim.cursor != nil {
			out.cursor, out.cursorOK = *lim.cursor, true
		}
		return out, nil
	}
	var cursor storage.WALCursor
	reused := lim.cursor != nil
	if reused {
		cursor = *lim.cursor
	} else {
		// Before the seek, which scans a segment and takes tens of milliseconds on a large one.
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("reading the log for slot %d: %w", key.slot, err)
		}
		var err error
		if cursor, err = log.OpenCursor(from); err != nil {
			return out, fmt.Errorf("opening the log at sequence %d: %w", from, err)
		}
	}
	// limit is where the current stretch of the read ends: the start of the pair's gap while the read
	// is before it, else stop.
	limit := stop
	if lim.gapTo > lim.gapFrom+1 && from <= lim.gapFrom && lim.gapFrom < stop {
		limit = lim.gapFrom
	}
	last, size := after, 0
	for {
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("reading the log for slot %d: %w", key.slot, err)
		}
		batch, next, err := log.ReadBatch(cursor, limit, readBatchBytes)
		if err != nil && reused {
			// A cursor kept across calls can point into a segment the log has since dropped:
			// seek again once before giving up.
			reused = false
			if cursor, err = log.OpenCursor(from); err == nil {
				continue
			}
		}
		if err != nil {
			return out, fmt.Errorf("reading the log from sequence %d: %w", cursor, err)
		}
		if len(batch) == 0 && limit < stop && cursor.NextSeq() > limit && lim.gapTo <= stop {
			// The read reached the gap's start: the entries up to its end are other pairs'. (A gap whose
			// end is past stop ends the read here; the next one starts past the gap.) The context is
			// checked before this seek for the reason it is before the first one.
			if err := ctx.Err(); err != nil {
				return out, fmt.Errorf("reading the log for slot %d: %w", key.slot, err)
			}
			if cursor, err = log.OpenCursor(lim.gapTo); err != nil {
				return out, fmt.Errorf("opening the log at sequence %d, past the other pairs' entries after %d: %w", lim.gapTo, lim.gapFrom, err)
			}
			out.next, limit, reused = lim.gapTo, stop, false
			continue
		}
		if len(batch) == 0 {
			out.cursor, out.cursorOK = cursor, cursor.NextSeq() == out.next
			return out, nil
		}
		cursor, reused = next, false
		for _, entry := range batch {
			slot, db, logged, err := storage.DecodePositionKey(entry.Key)
			if err != nil {
				return out, fmt.Errorf("log sequence %d is not a position-keyed record: %w", entry.Seq, err)
			}
			if slot != key.slot || db != key.db {
				out.next = entry.Seq + 1
				continue
			}
			pos := cluster.Position(logged)
			if !after.Less(pos) {
				out.next = entry.Seq + 1
				continue
			}
			if !lim.upTo.Zero() && lim.upTo.Less(pos) {
				return out, nil
			}
			if !last.Less(pos) {
				return out, fmt.Errorf("log sequence %d holds position %s of slot %d after %s: the pair's records are out of order", entry.Seq, pos, slot, last)
			}
			record, err := storage.DecodeWALRecord(entry.Op, entry.Data)
			if err != nil {
				return out, fmt.Errorf("log sequence %d (position %s): %w", entry.Seq, pos, err)
			}
			out.records = append(out.records, PairRecord{Pos: pos, Cursor: entry.Seq, Record: record})
			out.next, last = entry.Seq+1, pos
			size += storage.WALRecordHeaderLen + len(record.Body) + 16
			if len(out.records) >= lim.records || size >= lim.bytes {
				return out, nil
			}
		}
	}
}

// current reports whether the requester that applied this chunk is at the source's
// tail: the chunk says caught up and its last record is the source's appended position
// (conv §10). A short chunk, a read that raced a truncation or a cap that won is never
// current.
func current(chunk CatchUpChunk) bool {
	return chunk.CaughtUp && chunk.LastPosition == chunk.SourceAppended
}

// catchUpAttempts is how many times a copy asks a source that answers EPOCH_FENCED.
const catchUpAttempts = 5

// askCatchUp is one CATCHUP_REQ, asked again with a growing wait when the source answers
// EPOCH_FENCED: it has not applied the epoch change that made this node a copy yet, which
// is the retryable case the role check's uniform text exists for.
func (r *Replicator) askCatchUp(ctx context.Context, source string, req CatchUpRequest) (CatchUpChunk, error) {
	wait := r.retryBackoff()
	for attempt := 1; ; attempt++ {
		chunk, err := r.cfg.Link.CatchUp(ctx, source, req)
		if err == nil || !catchUpFenced(err) || attempt == catchUpAttempts {
			return chunk, err
		}
		select {
		case <-ctx.Done():
			return CatchUpChunk{}, fmt.Errorf("waiting to ask %s for a catch-up again: %w", source, ctx.Err())
		case <-time.After(wait):
		}
		wait *= 2
	}
}

// catchUpFenced reports a source's EPOCH_FENCED refusal of a catch-up, whether it arrived
// as a coded error or as the text of a peer error frame.
func catchUpFenced(err error) bool {
	var coded *cluster.Error
	if errors.As(err, &coded) && coded.Code == cluster.CodeEpochFenced {
		return true
	}
	return strings.Contains(err.Error(), "refused CATCHUP_REQ from")
}

// belowHorizonMarker is the part of the LOG_BELOW_HORIZON text that names the oldest record the
// source still holds; no other refusal says it.
const belowHorizonMarker = "the log's oldest record for the slot is"

// isBelowHorizon reports a source's LOG_BELOW_HORIZON answer to a catch-up. A source in this
// process answers with the coded error; one across the peer channel answers with a wire error
// that carries the text and not the code, as catchUpFenced reads EPOCH_FENCED.
func isBelowHorizon(err error) bool {
	if err == nil {
		return false
	}
	var coded *cluster.Error
	if errors.As(err, &coded) && coded.Code == cluster.CodeLogBelowHorizon {
		return true
	}
	return strings.Contains(err.Error(), belowHorizonMarker)
}

// IsCurrent reports whether a completed catch-up has marked the pair current. A copy
// that is ready but not current is not promotable (write path §6 step 4).
func (r *Replicator) IsCurrent(slot uint16, db uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.current[pairKey{slot, db}]
	return ok
}

// PinKind says what holds a log floor: a catch-up this node is serving, or a seed it is
// serving once the seed source registers its pins.
type PinKind string

const (
	// PinCatchUp is the floor of a CATCHUP_REQ being served.
	PinCatchUp PinKind = "catch-up"
	// PinSeed is the floor of a seed being served.
	PinSeed PinKind = "seed"
)

// PinnedFloor names one floor on the log: who holds it, for which pair, and the lowest log
// sequence the holder still needs.
type PinnedFloor struct {
	Kind  PinKind
	Slot  uint16
	DB    uint64
	Floor uint64
}

// PinnedFloor is the lowest log sequence an in-flight catch-up needs, and false when
// none is. Retention must not truncate at or above it (conv §9, floor pinning).
func (r *Replicator) PinnedFloor() (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	floor, found := uint64(0), false
	for _, pinned := range r.pins {
		if !found || pinned.Floor < floor {
			floor, found = pinned.Floor, true
		}
	}
	return floor, found
}

// PinnedFloors lists the floors in-flight catch-ups hold, each with the pair it serves, so a
// refusal to truncate can name the pin that stopped it.
func (r *Replicator) PinnedFloors() []PinnedFloor {
	r.mu.Lock()
	defer r.mu.Unlock()
	floors := make([]PinnedFloor, 0, len(r.pins))
	for _, pinned := range r.pins {
		floors = append(floors, pinned)
	}
	return floors
}

// pinFloor pins the log from the sequence a request starts at for the life of the
// request, and returns that sequence. The pin is taken, then the floor is read again,
// so a truncation between the two is seen and the pin is moved up.
func (r *Replicator) pinFloor(req CatchUpRequest) (start uint64, release func(), err error) {
	r.mu.Lock()
	r.pinID++
	id := r.pinID
	r.mu.Unlock()
	release = func() {
		r.mu.Lock()
		delete(r.pins, id)
		r.mu.Unlock()
	}
	for attempt := 0; attempt < 4; attempt++ {
		first, err := r.cfg.Log.FirstSequence()
		if err != nil {
			release()
			return 0, nil, fmt.Errorf("reading the first retained sequence: %w", err)
		}
		start = first
		if r.cfg.Seek != nil {
			start = max(first, r.cfg.Seek(req))
		}
		r.mu.Lock()
		r.pins[id] = PinnedFloor{Kind: PinCatchUp, Slot: req.Slot, DB: req.DB, Floor: start}
		r.mu.Unlock()
		again, err := r.cfg.Log.FirstSequence()
		if err != nil {
			release()
			return 0, nil, fmt.Errorf("reading the first retained sequence: %w", err)
		}
		if again <= start || again == first {
			return start, release, nil
		}
	}
	release()
	return 0, nil, errors.New("the log's first sequence kept moving while a catch-up pinned it")
}

// appendedPos is the source's appended position for the pair: the owner's last
// assigned position, or what a copy has applied.
func (r *Replicator) appendedPos(key pairKey) cluster.Position {
	p := r.cfg.Pairs.Snapshot(key.slot, key.db)
	appended := p.AppliedPos
	// An append counts only at the epoch it was made at (write path §3.1): after an epoch
	// move the owner's old appends are not positions of the new epoch.
	if committed := r.cfg.Committed(); p.AppendedSeq > 0 && p.AppendedEpoch == committed {
		if tip := (cluster.Position{Epoch: committed, Seq: p.AppendedSeq}); appended.Less(tip) {
			appended = tip
		}
	}
	return appended
}

func (r *Replicator) belowHorizon(req CatchUpRequest, oldest cluster.Position) error {
	return &cluster.Error{Code: cluster.CodeLogBelowHorizon, Message: cluster.CodeLogBelowHorizon.Text(cluster.CondLogBelowHorizon, map[string]string{
		"node": req.Node, "slot": strconv.Itoa(int(req.Slot)), "db": strconv.FormatUint(req.DB, 10),
		"need_pos": req.From.String(), "oldest_pos": oldest.String(), "source": r.cfg.Self,
	})}
}

// ServeCatchUp answers one CATCHUP_REQ. The floor is pinned for the life of the call.
// A request whose first record is not the one right after the requester's position is
// LOG_BELOW_HORIZON, never a short range; a read that raced a truncation answers a
// chunk that is not caught up, so the requester asks again and gets the horizon.
func (r *Replicator) ServeCatchUp(ctx context.Context, req CatchUpRequest) (CatchUpChunk, error) {
	return r.serveCatchUp(ctx, req, false)
}

// ServePinnedCatchUp answers the CATCHUP_REQ of the registered receiver of a live seed pin,
// which is the closing round of its seed (seeding completion 2.3). It differs from ServeCatchUp
// in one answer: the receiver asks from the first position of the committed epoch when its
// snapshot was cut in an earlier one (the seed machine's next position), so a source that has
// appended nothing in that epoch holds no tail the receiver lacks and answers caught up,
// where ServeCatchUp would read the epoch's boundary as a position the source never ordered.
func (r *Replicator) ServePinnedCatchUp(ctx context.Context, req CatchUpRequest) (CatchUpChunk, error) {
	return r.serveCatchUp(ctx, req, true)
}

func (r *Replicator) serveCatchUp(ctx context.Context, req CatchUpRequest, pinHolder bool) (CatchUpChunk, error) {
	if r.cfg.Deferred != nil && r.cfg.Deferred(req.Slot, req.DB) {
		// Before anything is pinned or read. The appended position of a pair whose boot replay
		// has not finished can be its low applied position (its unapplied tail is in an older
		// epoch than the committed one), and a requester that holds more would be answered
		// Divergent and reset a good copy. An error makes it ask again.
		return CatchUpChunk{}, pairReplayingError(r.cfg.Self, req.Slot, req.DB)
	}
	key := pairKey{req.Slot, req.DB}
	start, release, err := r.pinFloor(req)
	if err != nil {
		return CatchUpChunk{}, fmt.Errorf("pinning the log for a catch-up of slot %d: %w", req.Slot, err)
	}
	defer release()

	chunk := CatchUpChunk{SourceAppended: r.appendedPos(key)}
	if r.catchUpHook != nil {
		r.catchUpHook()
	}
	held := before(req.From)
	read, err := readPairRecords(ctx, r.cfg.Log, key, pairRead{from: start, after: held, records: r.cfg.ChunkRecords, bytes: r.cfg.ChunkBytes, upTo: chunk.SourceAppended})
	chunk.Records = read.records
	if len(read.records) > 0 {
		chunk.LastPosition = read.records[len(read.records)-1].Pos
	}
	if err != nil {
		if errors.Is(err, storage.ErrBelowHorizon) {
			return chunk, nil
		}
		return CatchUpChunk{}, fmt.Errorf("serving a catch-up of slot %d from %s: %w", req.Slot, req.From, err)
	}
	if len(read.records) == 0 {
		switch {
		case pinHolder && held.Seq == 0 && chunk.SourceAppended.Epoch < held.Epoch:
			// The receiver asked from the opener of an epoch the source has not written in:
			// its snapshot's cut is in an earlier epoch, at or below the source's tail, so
			// nothing is missing. This is what the cut of a pair that is empty or quiet in
			// the committed epoch looks like.
			chunk.CaughtUp, chunk.LastPosition = true, chunk.SourceAppended
			return chunk, nil
		case chunk.SourceAppended.Less(held):
			// The requester holds a position the source never ordered: a tail nobody owns.
			chunk.Divergent = true
			return chunk, nil
		case held == chunk.SourceAppended:
			chunk.CaughtUp, chunk.LastPosition = true, chunk.SourceAppended
			return chunk, nil
		}
		return CatchUpChunk{}, r.belowHorizon(req, chunk.SourceAppended)
	}
	divergent, err := r.firstRecordFollows(req, held, read.records[0])
	if err != nil {
		return CatchUpChunk{}, err
	}
	if divergent {
		return CatchUpChunk{SourceAppended: chunk.SourceAppended, Divergent: true}, nil
	}
	chunk.CaughtUp = chunk.LastPosition == chunk.SourceAppended
	return chunk, nil
}

// firstRecordFollows checks the front of a range: the first record must be the one
// right after the requester's position, or the opener of the next epoch whose prevEnd
// is that position. An opener whose prevEnd is below the requester's position means the
// requester holds a tail nobody owns. Anything else means records the requester needs are
// no longer in the log.
func (r *Replicator) firstRecordFollows(req CatchUpRequest, held cluster.Position, first PairRecord) (divergent bool, err error) {
	if first.Pos == (cluster.Position{Epoch: held.Epoch, Seq: held.Seq + 1}) {
		return false, nil
	}
	if first.Pos.Seq == 1 {
		op, err := cluster.DecodeOp(first.Record.Body)
		if err != nil {
			return false, fmt.Errorf("decoding the opener %s of slot %d: %w", first.Pos, req.Slot, err)
		}
		switch {
		case op.PrevEnd == held:
			return false, nil
		case op.PrevEnd.Less(held):
			// The source's epoch began before the requester's position: the requester
			// holds records past the boundary that nobody owns.
			return true, nil
		}
	}
	return false, r.belowHorizon(req, first.Pos)
}

// maxCatchUpStalls is how many requests in a row may move the copy nowhere before the
// catch-up reports it. A truncation that raced one read is answered by the next.
const maxCatchUpStalls = 3

// ErrResetAndSeed is returned by CatchUp when the Seeder was told to reset and seed the pair:
// a record showed the copy holds a tail nobody owns, or the source's log no longer holds what
// the copy needs.
var ErrResetAndSeed = errors.New("the copy holds a tail nobody owns; the pair is reset and seeded")

// ErrNoCatchUpSource is what a CatchUpSource returns when no reachable node holds a
// position beyond this copy's own: there is nothing to catch up from.
var ErrNoCatchUpSource = errors.New("no reachable node holds more than this copy")

// catchUpFlight is one catch-up in flight for a pair. It exists only while the catch-up runs:
// an idle pair holds none, so the set costs nothing per bucket or per pair at rest.
type catchUpFlight struct {
	// asCopy says the catch-up began on a node that held the pair as a copy.
	asCopy bool
	cancel context.CancelFunc
	done   chan struct{}
	// preempted is set by an owner's catch-up that cancelled this one.
	preempted atomic.Bool
}

// owns reports whether this node is the owner of the pair at the committed epoch.
func (r *Replicator) owns(key pairKey) bool {
	owner, known := r.cfg.Owner(key.slot, key.db)
	return known && owner == r.cfg.Self
}

// claimCatchUp makes the caller the one catch-up of the pair, so two never apply to, ask for
// or reset one pair at the same time (BUG-0816). A copy's catch-up that finds one running
// leaves it to finish (claimed is false). An owner's catch-up, the handoff's, cancels a
// copy's that is still running, because that one was started under an ownership that has
// ended, and waits for whatever is running to end before it starts.
func (r *Replicator) claimCatchUp(ctx context.Context, key pairKey, asCopy bool) (flightCtx context.Context, flight *catchUpFlight, claimed bool, err error) {
	for {
		r.mu.Lock()
		running := r.catching[key]
		if running == nil {
			flightCtx, cancel := context.WithCancel(ctx)
			flight = &catchUpFlight{asCopy: asCopy, cancel: cancel, done: make(chan struct{})}
			r.catching[key] = flight
			r.mu.Unlock()
			return flightCtx, flight, true, nil
		}
		r.mu.Unlock()
		if asCopy {
			return nil, nil, false, nil
		}
		if running.asCopy {
			running.preempted.Store(true)
			running.cancel()
		}
		select {
		case <-running.done:
		case <-ctx.Done():
			return nil, nil, false, fmt.Errorf("waiting for the catch-up of slot %d of database %d that is already running to end: %w", key.slot, key.db, ctx.Err())
		}
	}
}

func (r *Replicator) releaseCatchUp(key pairKey, flight *catchUpFlight) {
	flight.cancel()
	r.mu.Lock()
	delete(r.catching, key)
	r.mu.Unlock()
	close(flight.done)
}

// CatchUp brings this node's copy of the pair to the source's tail. It asks from
// the first position the copy still needs, applies every record through the same rules
// as a live push, and is done only when a chunk is current. Every applied record advances
// the cumulative ack to the owner. A chunk that is not current, including one
// that raced a truncation, asks again from the new applied position; a round that
// moves nothing is an error, not a silent stop. The from parameter is only a hint and is
// ignored: the start is always the copy's applied position + 1.
//
// One catch-up runs per pair at a time. A catch-up that begins while this node holds the
// pair as a copy ends, without an error and without a reset, as soon as the node is the
// pair's owner at the committed epoch (see CatchUpAsCopy). One that begins on the owner is
// the handoff's: it runs before the pair opens and may reset it.
func (r *Replicator) CatchUp(ctx context.Context, slot uint16, db uint64, _ cluster.Position, cursor uint64) error {
	key := pairKey{slot, db}
	return r.catchUp(ctx, key, cursor, !r.owns(key))
}

// CatchUpAsCopy is the catch-up a copy asks for when a pushed record skipped a position
// (RequestCatchUp). It is a copy's business from start to end: it does nothing on a node
// that owns the pair, and one that outlives a promotion stops before its next request,
// before it applies another record and before any reset (BUG-0816). Its source was chosen
// while the node was behind it; once the node is the owner and has ordered a record, that
// source is the stale side, its answer would read as a divergent tail, and the reset would
// rebuild the owner's pair from a node that is behind it. The handoff's own catch-up
// brings a new owner to its target.
func (r *Replicator) CatchUpAsCopy(ctx context.Context, slot uint16, db uint64) error {
	return r.catchUp(ctx, pairKey{slot, db}, 0, true)
}

func (r *Replicator) catchUp(ctx context.Context, key pairKey, cursor uint64, asCopy bool) error {
	if asCopy && r.owns(key) {
		return nil
	}
	flightCtx, flight, claimed, err := r.claimCatchUp(ctx, key, asCopy)
	if err != nil || !claimed {
		return err
	}
	defer r.releaseCatchUp(key, flight)
	err = r.runCatchUp(flightCtx, key, cursor, asCopy)
	if flight.preempted.Load() && errors.Is(err, context.Canceled) {
		// The handoff's catch-up cancelled this one, between two records of a chunk: that is
		// not a failure. (A request it cut short never gets here: the node is the owner by
		// then, and the check after the answer returns first.) Anything else, an apply or a
		// disk error, is still reported.
		return nil
	}
	return err
}

// resetAsCopy is the reset a pushed record asks for (ackFor), run as a copy's catch-up
// flight so the handoff waits for it. Without the flight the owner check and the reset are
// seconds apart (the reseed first clears the pair's seed bit through the leader): the epoch
// that makes this node the owner can commit in between, the handoff's catch-up can finish and
// the pair open, and the reset then lands on a serving owner (BUG-0816). Inside the flight
// the handoff's catch-up waits, so the pair cannot open before the reset is done. When a
// catch-up already holds the pair nothing is reset here: that catch-up meets the same tail.
func (r *Replicator) resetAsCopy(key pairKey, cause ReseedCause) error {
	_, flight, claimed, err := r.claimCatchUp(context.Background(), key, true)
	if err != nil || !claimed {
		return err
	}
	defer r.releaseCatchUp(key, flight)
	// Read again inside the flight: before it, a handoff could have run from start to end.
	if r.owns(key) {
		return nil
	}
	return r.reset(key, cause)
}

// runCatchUp is the catch-up itself, for the one caller that holds the pair's flight.
func (r *Replicator) runCatchUp(ctx context.Context, key pairKey, cursor uint64, asCopy bool) error {
	slot, db := key.slot, key.db
	source, err := r.cfg.CatchUpSource(slot, db)
	if asCopy && r.owns(key) {
		// Choosing the source asks every copy for its position; the node became the owner
		// meanwhile. Whether the pair is current is the handoff's catch-up to say.
		return nil
	}
	if errors.Is(err, ErrNoCatchUpSource) {
		// Nobody that can be reached holds more than this copy, so it is as current as the
		// survivors allow: the new owner of a pair whose other copies are gone opens on what
		// it has (the loss, if any, is the contract's: write path section 7).
		r.mu.Lock()
		r.current[key] = struct{}{}
		r.mu.Unlock()
		return nil
	}
	if err != nil {
		return fmt.Errorf("choosing a catch-up source for slot %d of database %d: %w", slot, db, err)
	}
	r.mu.Lock()
	delete(r.current, key)
	r.mu.Unlock()
	// The start is always derived from the copy's own state, applied + 1 (the first
	// position of an empty pair): a caller's from can only be stale or wrong, and
	// trusting it would make a legit copy look divergent or hide a tail above the
	// source's.
	applied := r.cfg.Pairs.Snapshot(slot, db).AppliedPos
	from := cluster.Position{Epoch: applied.Epoch, Seq: applied.Seq + 1}
	stalls := 0
	for {
		if asCopy && r.owns(key) {
			return nil
		}
		req := CatchUpRequest{Slot: slot, DB: db, Node: r.cfg.Self, Epoch: r.cfg.Committed(), From: from, FromCursor: cursor}
		chunk, err := r.askCatchUp(ctx, source, req)
		if asCopy && r.owns(key) {
			// The node became the owner while the request was out: whatever came back answers
			// a copy's question, and neither its records nor a reset are for an owner.
			return nil
		}
		if isBelowHorizon(err) {
			// The source's log no longer holds what this copy needs: no catch-up can close
			// that, so the pair is reset and seeded from the source (plan §1.1).
			if err := r.reset(key, ReseedHorizon); err != nil {
				return err
			}
			return ErrResetAndSeed
		}
		if err != nil {
			return fmt.Errorf("catching up slot %d of database %d from %s at %s: %w", slot, db, source, from, err)
		}
		if chunk.Divergent {
			if err := r.reset(key, ReseedDivergent); err != nil {
				return err
			}
			return ErrResetAndSeed
		}
		start := r.cfg.Pairs.Snapshot(slot, db).AppliedPos
		whole, err := r.applyChunk(ctx, source, req, chunk, asCopy)
		if err != nil {
			return err
		}
		if !whole && r.cfg.Ready != nil && !r.cfg.Ready(slot, db) {
			// The pair was reset while this catch-up held a chunk: its pull replaces the catch-up.
			return nil
		}
		now := r.cfg.Pairs.Snapshot(slot, db).AppliedPos
		r.ackCatchUp(ctx, key, now)
		// The copy may have passed the source's snapshot through live pushes meanwhile.
		if whole && current(chunk) && !now.Less(chunk.SourceAppended) {
			r.mu.Lock()
			r.current[key] = struct{}{}
			r.mu.Unlock()
			return nil
		}
		if start.Less(now) {
			stalls = 0
		} else if stalls++; stalls >= maxCatchUpStalls {
			return fmt.Errorf("catch-up of slot %d of database %d from %s made no progress at %s in %d requests: the source is at %s and its last chunk was not current", slot, db, source, now, stalls, chunk.SourceAppended)
		}
		from = cluster.Position{Epoch: now.Epoch, Seq: now.Seq + 1}
	}
}

// CatchUpIntoRequest is one catch-up that hands records to a sink instead of applying them.
type CatchUpIntoRequest struct {
	Slot   uint16
	DB     uint64
	From   cluster.Position // first position wanted; the caller owns it, unlike CatchUp
	Source string           // the attempt's pinned source; never chosen here
	Sink   func(rec PairRecord) error
}

// CatchUpInto fetches chunks from req.Source exactly as CatchUp does and calls Sink for each record in order.
// It returns the source's SourceAppended position when a chunk is current, or ErrResetAndSeed on LOG_BELOW_HORIZON.
// It never applies a record, moves the pair's applied position or resets the pair: the caller stages what the
// sink receives and decides on a reset.
func (r *Replicator) CatchUpInto(ctx context.Context, req CatchUpIntoRequest) (cluster.Position, error) {
	from := req.From
	now := before(req.From)
	stalls := 0
	for {
		catchUp := CatchUpRequest{Slot: req.Slot, DB: req.DB, Node: r.cfg.Self, Epoch: r.cfg.Committed(), From: from}
		chunk, err := r.askCatchUp(ctx, req.Source, catchUp)
		if isBelowHorizon(err) {
			return cluster.Position{}, ErrResetAndSeed
		}
		if err != nil {
			return cluster.Position{}, fmt.Errorf("catching up slot %d of database %d from %s at %s: %w", req.Slot, req.DB, req.Source, from, err)
		}
		if chunk.Divergent {
			return cluster.Position{}, ErrResetAndSeed
		}
		start := now
		for _, rec := range chunk.Records {
			if err := req.Sink(rec); err != nil {
				return cluster.Position{}, fmt.Errorf("handing position %s of slot %d to the sink: %w", rec.Pos, req.Slot, err)
			}
			now = rec.Pos
		}
		if current(chunk) && !now.Less(chunk.SourceAppended) {
			return chunk.SourceAppended, nil
		}
		if start == now {
			if stalls++; stalls >= maxCatchUpStalls {
				return cluster.Position{}, fmt.Errorf("catch-up of slot %d of database %d from %s made no progress past %s in %d requests: the source is at %s and its last chunk was not current", req.Slot, req.DB, req.Source, now, stalls, chunk.SourceAppended)
			}
			continue
		}
		stalls = 0
		from = cluster.Position{Epoch: now.Epoch, Seq: now.Seq + 1}
	}
}

// ackCatchUp sends the cumulative ack for what a catch-up applied, so a concern met
// through catch-up is confirmed (write path §4).
func (r *Replicator) ackCatchUp(ctx context.Context, key pairKey, applied cluster.Position) {
	owner, known := r.cfg.Owner(key.slot, key.db)
	if !known || owner == r.cfg.Self || applied.Zero() {
		return
	}
	ack := WriteAck{From: r.cfg.Self, Slot: key.slot, DB: key.db, Pos: applied, Outcome: AckApplied}
	if err := r.cfg.Link.Ack(ctx, owner, ack); err != nil {
		r.report(fmt.Errorf("acking position %s of slot %d to %s after a catch-up: %w", applied, key.slot, owner, err))
	}
}
