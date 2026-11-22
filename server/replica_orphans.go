package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A replica logs a run of a stream push's entries and syncs the log before it
// applies any of them (replica_run.go, BUG-0521). When an apply stops the run
// (a batch too large for this node, a divergence, a store failure), the
// entries after it are in the log, synced, and never applied: the run's
// orphans. Everything else about a replica's log assumes that what it logged it
// applies or owes to its own replay: the start rebuilds a source's position
// from the log (rebuildPositionsFromLog) and replay applies the log. An orphan
// breaks both. Counted, it raises the position past entries the owner must
// re-send; replayed, it lands ahead of the stopped entry it follows in its
// source's stream, which a size refusal or a divergence holds back; and once
// the owner re-sends it, this node logs it a second time, so a replay after a
// power cut applies both copies, and one whose bucket has since moved past it
// is passed over while counted as applied.
//
// So a run that stops with orphans voids them before the push is answered: a
// synced checkpoint records, per source, the void's bounds, and from then on
// an entry of the source logged at or below VoidedThrough whose own sequence
// is above VoidedAbove is neither counted nor replayed. VoidedAbove is the
// source's position when the run stopped, so the void names exactly what was
// logged from the source and not applied, the stopped entry included; the
// owner re-sends all of it. The synced commit also syncs the store (a synced
// batch syncs every commit before it), so what was applied from the source
// before the stop is durable, and replay never needs an entry of the void.
//
// The stopped entry is voided whether or not entries follow it, when its
// apply left the store without it: a store failure, or a run whose log sync
// failed (entryLeftUnappliedError). It is then logged and unapplied like an
// orphan, counted by the rebuild and above the bucket's mark, and the owner's
// re-send logs it again, so a replay after a power cut applied both copies
// (BUG-0570). The same holds for an entry logged and applied on its own
// (applyAsReplica). A size refusal is not voided unless orphans follow it: it
// is held back by its record, and the store leaves it unconsumed.
//
// A stop the store judges and consumes, an entry of a term above the copy's
// or a divergence, is voided in the store's batch that consumes it, through
// the run's last entry, with a divergence's record beside it, and that batch
// is synced (stopRecorder). Its local sequence is consumed, so replay passes
// over it: a void written after the stop left a crash in between counting the
// stopped entry and replaying its orphans past it, and the owner's re-send was
// taken for a duplicate (BUG-0571). The entry is never consumed durably
// without its void, so the start never counts it, and the owner re-sends it.
//
// A crash after any other stop and before its void is written leaves the
// orphans counted and replayable. Only a recorded size refusal makes that
// wrong, and it is recorded before the push is answered: replay holds back
// every entry of the source above a standing record's sequence (replayHolds),
// the start lowers the position below the record, and replay voids what it
// held back before the node serves a peer. A divergence recorded on its own,
// one the store never reached, is held back the same way, and one whose record
// the crash took is found again by replay, from the owner's outcome its log
// entry carries, and recorded and held back as it goes (BUG-0581). Without a
// record the crash is a crash during a run: the stopped entry was not
// consumed, a store failure or a refusal before the store its owner gave too,
// and replay applies it and its orphans in order, as their source sent them.
//
// Cost: one orphanVoid per source that ever stopped a run with orphans or on
// an entry its store left unapplied or judged, two numbers in its "p" record,
// and one synced write per such stop. Nothing is kept per bucket.

// orphanVoid is one source's void: its entries at or below local sequence
// through whose own sequence is above above.
type orphanVoid struct {
	above   uint64
	through uint64
}

// entryLeftUnappliedError is the failure of a replica entry this node logged
// and whose apply left the store without it: the bucket's applied mark is
// below the entry, so replay would apply it, and the source's position is
// below it, so the owner re-sends it. Its text is the failure's own.
type entryLeftUnappliedError struct{ err error }

func (e *entryLeftUnappliedError) Error() string { return e.err.Error() }
func (e *entryLeftUnappliedError) Unwrap() error { return e.err }

// leftUnapplied reports whether err is the failure of an entry its store left
// unapplied, which is voided even with nothing logged after it.
func leftUnapplied(err error) bool {
	var unapplied *entryLeftUnappliedError
	return errors.As(err, &unapplied)
}

// voidUnapplied voids what a stop left logged and unapplied of source's
// stream, through local sequence through, and says so in the error log when
// the void cannot be recorded: it is then owed, and the source's pushes are
// refused until it is (payOrphanVoidDebt).
func (c *Cluster) voidUnapplied(source string, through uint64) {
	if err := c.voidOrphans(source, through); err != nil {
		c.logger.Error("could not record the entries a stop left logged and unapplied; the sender's pushes are refused until it is recorded",
			"from", source, "through", through, "error", err)
	}
}

// covers reports whether entry, read from this node's log, is in the void.
func (v orphanVoid) covers(entry storage.WALEntry) bool {
	return entry.Seq <= v.through && entry.Origin.Seq > v.above
}

// voidOrphans voids every entry of source's stream in this node's log at or
// below local sequence through that is above the source's position, and
// checkpoints the void, synced. The caller holds the source's stream lock or
// runs before the node serves its peers, so the position cannot move under it.
// A void whose entries the log still holds is widened, never replaced: its
// entries are as unapplied as before. What the widening adds was applied
// before this checkpoint, which syncs the store and records a position past
// it, so neither the replay nor the rebuild needs it. If the checkpoint fails,
// the void stays owed and the source's next push pays it before anything is
// logged (payOrphanVoidDebt).
//
// The void is made widened and owed before the log's first sequence is read,
// and the read only decides whether a void the log no longer holds is
// replaced instead. A void made after the read was never made when the read
// failed, and the source's next push was logged beside the entries it should
// have named (BUG-0569). A failed read keeps the widened void: it voids
// only more of what the owner re-sends, and the owner re-sends all of it.
func (c *Cluster) voidOrphans(source string, through uint64) error {
	if hook := orphanVoidFaultHook.Load(); hook != nil {
		if err := (*hook)(c, source); err != nil {
			return fmt.Errorf("recording the entries of %s's stream this node logged past a stopped run: %w", source, err)
		}
	}
	c.appliedMu.Lock()
	fresh := orphanVoid{above: c.appliedFrom[source], through: through}
	previous, voided := c.orphanVoids[source]
	if voided {
		c.orphanVoids[source] = orphanVoid{above: min(previous.above, fresh.above), through: max(previous.through, fresh.through)}
	} else {
		c.orphanVoids[source] = fresh
	}
	c.orphanVoidOwed[source] = struct{}{}
	c.appliedMu.Unlock()
	if voided {
		first, err := c.s.wal.FirstSequence()
		switch {
		case err != nil:
			c.logger.Warn("could not read the log's first sequence; the void of a stopped run is widened over the earlier one instead of replacing it",
				"source", source, "through", through, "error", err)
		case previous.through < first:
			c.appliedMu.Lock()
			c.orphanVoids[source] = fresh
			c.appliedMu.Unlock()
		}
	}
	return c.payOrphanVoidDebt(source)
}

// payOrphanVoidDebt writes source's void in a synced checkpoint if it is owed.
// A push of source's stream calls it before it logs anything: a copy of an
// orphan logged before its void is durable would be replayed beside it.
func (c *Cluster) payOrphanVoidDebt(source string) error {
	c.appliedMu.Lock()
	_, owed := c.orphanVoidOwed[source]
	c.appliedMu.Unlock()
	if !owed {
		return nil
	}
	if err := c.checkpointPosition(source, true); err != nil {
		return fmt.Errorf("recording the entries of %s's stream this node logged past a stopped run: %w", source, err)
	}
	c.appliedMu.Lock()
	delete(c.orphanVoidOwed, source)
	c.appliedMu.Unlock()
	return nil
}

// stopRecorder is a replica's record of a stop its store judges and consumes
// (BUG-0571): an entry of a term above the copy's, or a refusal of an entry
// its owner applied, a divergence. The store consumes such an entry's local
// sequence, so replay passes over it, and the void that keeps the start from
// counting it and replay from applying the run's orphans past it was written
// only after the stop: a crash between the two lost the entry and replayed
// its orphans ahead of it. So the void, and a divergence's record, go into the
// store's batch that consumes the entry, synced (store.ApplyInfo.RecordStop):
// one synced write per stop, which the void after the stop no longer needs.
// It is attached only to a push of the source's stream, under its stream lock.
type stopRecorder struct {
	c      *Cluster
	source string
	entry  ReplEntry
	// through is the void's local sequence: the run's last entry, or the
	// entry's own when nothing was logged after it.
	through uint64
	// staged says record added the void to the store's batch, and diverged is
	// the divergence record it added beside it. Both are durable once the
	// apply returned without a store failure.
	staged   bool
	diverged *divergenceRecord
}

type stopRecorderCtxKey struct{}

// withStopRecorder attaches stop to a replica's live apply.
func withStopRecorder(ctx context.Context, stop *stopRecorder) context.Context {
	return context.WithValue(ctx, stopRecorderCtxKey{}, stop)
}

// stopRecorderFrom returns what withStopRecorder attached, or nil.
func stopRecorderFrom(ctx context.Context) *stopRecorder {
	stop, _ := ctx.Value(stopRecorderCtxKey{}).(*stopRecorder)
	return stop
}

// stops reports whether the store's refusal of the entry stops its source's
// stream: an entry of a later term always does, and a refusal of an entry its
// owner applied does unless a constraint refused it, which the replica answers
// with the owner's outcome (applyOutcomeOf; codeFor names the constraint the
// same way the answer's error frame does). A batch too large for this node is
// not consumed and is recorded on its own (recordSizeRefusal).
func (r *stopRecorder) stops(refusal error) bool {
	if errors.Is(refusal, store.ErrTermAhead) {
		return true
	}
	if r.entry.OwnerOutcome != ownerOutcomeApplied || errors.Is(refusal, storage.ErrBatchTooLarge) {
		return false
	}
	switch codeFor(refusal) {
	case transport.ErrDuplicateKey, transport.ErrFieldType:
		return false
	}
	return true
}

// record is store.ApplyInfo.RecordStop: for a refusal that stops the stream it
// adds the void, and for a divergence its record, to batch, which consumes the
// entry in st's bucket.
func (r *stopRecorder) record(batch storage.KVBatch, st store.BucketState, refusal error) (bool, error) {
	if !r.stops(refusal) {
		return false, nil
	}
	if !errors.Is(refusal, store.ErrTermAhead) {
		_, database := decodeFrameMeta(r.entry.Payload).scope(transport.Opcode(r.entry.Op))
		diverged := divergenceRecord{Seq: r.entry.Seq, Database: database, Bucket: r.entry.Bucket, Incarnation: st.Incarnation, Reason: refusalDiverged}
		encoded, err := bson.Marshal(diverged)
		if err != nil {
			return false, fmt.Errorf("encoding the divergence from %s at sequence %d: %w", r.source, r.entry.Seq, err)
		}
		if err := batch.Set(divergenceKey(r.source), encoded); err != nil {
			return false, fmt.Errorf("recording the divergence from %s at sequence %d: %w", r.source, r.entry.Seq, err)
		}
		r.diverged = &diverged
	}
	if err := r.c.stageStopVoid(batch, r.source, r.through); err != nil {
		return false, err
	}
	r.staged = true
	return true, nil
}

// stageStopVoid makes source's void reach local sequence through, as
// voidOrphans does, and adds its "p" record to batch rather than committing
// it. The void is in memory and owed before positionMu is released, so a
// checkpoint after it writes the void too, and one before it has committed;
// it stays owed until the batch is known committed (settleStagedVoid), and a
// batch that fails leaves it to payOrphanVoidDebt. The void is widened over
// an earlier one and never replaced: that would need the log's first
// sequence, read under the bucket's lock, and a wider void voids only more of
// what the owner re-sends (voidOrphans).
func (c *Cluster) stageStopVoid(batch storage.KVBatch, source string, through uint64) error {
	c.positionMu.Lock()
	defer c.positionMu.Unlock()
	c.appliedMu.Lock()
	void := orphanVoid{above: c.appliedFrom[source], through: through}
	if previous, voided := c.orphanVoids[source]; voided {
		void = orphanVoid{above: min(previous.above, void.above), through: max(previous.through, void.through)}
	}
	c.orphanVoids[source] = void
	c.orphanVoidOwed[source] = struct{}{}
	c.appliedMu.Unlock()
	record, _ := c.positionRecordOf(source)
	encoded, err := bson.Marshal(record)
	if err != nil {
		return fmt.Errorf("encoding the position record of source %q with the void of a stopped run: %w", source, err)
	}
	if err := batch.Set(positionKey(source), encoded); err != nil {
		return fmt.Errorf("adding the void of %s's stopped run to the batch that consumes the stopped entry: %w", source, err)
	}
	return nil
}

// settleStagedVoid clears the debt stageStopVoid left once the batch carrying
// the void has committed, synced.
func (c *Cluster) settleStagedVoid(source string) {
	c.appliedMu.Lock()
	delete(c.orphanVoidOwed, source)
	c.appliedMu.Unlock()
}

// replayHolds is what one replay holds back of each source's stream: every
// entry above a size refusal or a divergence recorded for the source, standing
// when replay began or recorded by it. above is the lowest such sequence per
// source, and through the highest local sequence held back.
type replayHolds struct {
	above   map[string]uint64
	through map[string]uint64
}

// newReplayHolds starts a replay's holds from the records standing now.
func (c *Cluster) newReplayHolds() *replayHolds {
	holds := &replayHolds{above: make(map[string]uint64), through: make(map[string]uint64)}
	c.memtableMu.Lock()
	for source, refusal := range c.batchSizeRefusals {
		holds.holdAbove(source, refusal.Seq)
	}
	c.memtableMu.Unlock()
	c.divergenceMu.Lock()
	for source, record := range c.divergences {
		holds.holdAbove(source, record.Seq)
	}
	c.divergenceMu.Unlock()
	return holds
}

// holdAbove holds back source's entries above seq for the rest of the replay,
// even once the record that asked for it settles: an entry past it that the
// replay applied would sit above a position the start lowered, and the
// owner's re-send would apply it again.
func (h *replayHolds) holdAbove(source string, seq uint64) {
	if held, holding := h.above[source]; !holding || seq < held {
		h.above[source] = seq
	}
}

// passesOver reports whether replay must not apply entry, an entry this node
// logged from another node's stream: it is in the source's void, or above a
// record holding the source back. An entry at or below the source's
// rebased_at is of a log the source has since gone back on, and its sequence
// is not compared.
func (c *Cluster) passesOver(holds *replayHolds, entry storage.WALEntry) bool {
	source := entry.Origin.Source
	c.appliedMu.Lock()
	void := c.orphanVoids[source]
	rebased := c.rebasedAt[source]
	c.appliedMu.Unlock()
	if void.covers(entry) {
		return true
	}
	held, holding := holds.above[source]
	if entry.Seq <= rebased || !holding || entry.Origin.Seq <= held {
		return false
	}
	holds.through[source] = max(holds.through[source], entry.Seq)
	return true
}

// voidHeldBack voids, once replay has finished, every entry it held back, so
// that the owner's re-sends, logged again, are the only copies replay ever
// applies. A source whose void cannot be written owes it, and its first push
// pays it before anything is logged.
func (c *Cluster) voidHeldBack(holds *replayHolds) {
	for source, through := range holds.through {
		if err := c.voidOrphans(source, through); err != nil {
			c.logger.Error("could not record the entries replay held back; the source's pushes are refused until it is recorded",
				"source", source, "through", through, "error", err)
		}
	}
}
