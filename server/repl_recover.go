package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// WAL replay rebuilds a node's state from its write-ahead log.
//
// The WAL is the authority for what this node has durably accepted: every
// replicated entry is appended before it is applied, so replaying the log
// reconstructs the same state on a restart. Entries are replayed through the
// ordinary dispatch path, which is the same path a live request takes.
//
// Replay is best-effort per entry. A replayed create for a bucket that already
// exists on disk, or a delete for something already gone, is expected after a
// restart: those entries are counted and reported rather than being allowed to
// abort startup, because refusing to start on an idempotent replay would make
// a crash unrecoverable.
//
// It runs at most once per process. Start can be retried when a listener cannot
// bind yet (a restarting node briefly finds its own port still in use), and
// re-applying the log on that retry would duplicate every non-idempotent entry.
func (s *Server) replayWAL(ctx context.Context) error {
	if s.wal == nil {
		return nil
	}
	s.replayOnce.Do(func() {
		s.replayErr = s.replayWALOnce(ctx)
		// The owner's awaiting records are loaded after the replay, which can
		// have written some: an alter whose append was synced and whose apply
		// did not commit is replayed by the owner's own path (spec §5.2).
		if s.replayErr == nil && s.cluster != nil {
			s.replayErr = s.cluster.loadAwaitingCopies()
		}
		// The pending-copy worker's queue is filled after the replay too: a
		// database drop replayed from the log deletes its needs-a-copy records
		// by prefix, and a queue filled before it would keep entries whose
		// records are gone.
		if s.replayErr == nil && s.cluster != nil {
			s.replayErr = s.cluster.loadPendingCopies()
		}
	})
	return s.replayErr
}

func (s *Server) replayWALOnce(ctx context.Context) error {
	entries, err := s.wal.ReadFrom(1)
	if err != nil {
		return fmt.Errorf("read WAL: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}

	// Snapshotted once, before the loop dispatches anything: the generation
	// of every database a logged drop names, as LoadCatalog left it
	// (the gate in databaseDropGenerations). Taken any
	// later than this it would be fooled by replay's own dispatches — a
	// dropped generation's own, still-undispatched CreateDB and CreateBucket
	// entries re-create it in memory well after this loop starts (for a
	// bucket, catalogedCreate's "create over a pending expansion" path, on
	// any bucket name whose catalog entry is still at or below a pending
	// drop's max_id) — and a live re-check at the OpDeleteDB entry would then
	// see a database that has nothing to do with a later generation, and
	// gate or refuse a drop that must take effect.
	generations, err := s.databaseDropGenerations(entries)
	if err != nil {
		return fmt.Errorf("snapshotting database generations before replay: %w", err)
	}

	// What replay holds back of other nodes' streams: their orphans and the
	// entries above a refusal recorded for them (replica_orphans.go).
	var holds *replayHolds
	if s.cluster != nil {
		holds = s.cluster.newReplayHolds()
	}
	applied, rejected, skipped, gated, heldBack, passedOver := 0, 0, 0, 0, 0, 0
	for i, e := range entries {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if holds != nil && e.Origin.Kind == storage.WALOriginReplica && s.cluster.passesOver(holds, e) {
			heldBack++
			continue
		}
		op := transport.Opcode(e.Op)
		// A logged entry was validated where it entered; one that does not
		// validate is passed over before anything decodes it, as an entry that
		// did not apply.
		if _, err := validateRequest(op, e.Data); err != nil {
			skipped++
			s.logger.Debug("replayed entry not applied", "seq", e.Seq, "op", op, "bucket", e.Bucket, "error", err)
			continue
		}
		// A declare and an alter are gated like a create: logged against a
		// dropped generation of their bucket, they would change a re-created
		// bucket that never had them.
		if op == transport.OpCreateBucket || op == transport.OpDeleteBucket || op == transport.OpDeclareStoreFields ||
			op == transport.OpAlterBucket {
			belowMark, err := s.ddlAtOrBelowMark(op, e, generations)
			if err != nil {
				return fmt.Errorf("replaying the bucket DDL at sequence %d: %w", e.Seq, err)
			}
			if belowMark {
				gated++
				continue
			}
		}
		// A drop that names generations removes exactly those wherever it is
		// applied, so it needs no gate: gated by the live marker's log
		// position it would be skipped for a database re-created since, and
		// the buckets of the generation it dropped would come back.
		if op == transport.OpDeleteDB && !dropNamesGenerations(e.Data) {
			staleGeneration, err := s.databaseDropAtOrBelowMark(e, generations)
			if err != nil {
				return fmt.Errorf("replaying the database drop at sequence %d: %w", e.Seq, err)
			}
			if staleGeneration {
				gated++
				continue
			}
		}
		frame := &transport.Frame{
			Opcode:  op,
			Payload: e.Data,
		}
		// The entry's sequence is this node's own: the apply sequence, which
		// the bucket's applied_seq is compared against. It is not the write
		// version — for an entry this node applied as a replica it is not the
		// owner's sequence (build doc SA-17); replayVersion is. Replay records
		// a store failure, as the owner's live apply does (spec §4.4 rule 5),
		// and answers SA-27 for the entry from the entries after it.
		later := entries[i+1:]
		// In a bucket declaring a unique index, two different keys holding one
		// value do not commute: a recorded failure replayed beneath the mark
		// can meet the opposite outcome its replicas met. It never commutes,
		// and Apply takes SA-27's loud path (GAP-14 build doc S-6, §4 F-3).
		commutes := func(appliedSeq uint64) bool {
			return !s.bucketDeclaresUnique(e) && commutesWithLater(e, later, appliedSeq)
		}
		replayCtx := withCommutes(withRecordFailures(withReplay(withApplySeq(ctx, e.Seq))), commutes)
		// An entry this node applied from another node's stream is that
		// node's: a drop in it can name generations this node had not logged
		// a drop of when it stopped (G-12).
		replayCtx = withPeerEntry(replayCtx, e.Origin.Kind == storage.WALOriginReplica)
		// The lineage moves to the origin's sequence: this log's own for an
		// entry this node originated, the one the origin block records for
		// another node's. An entry logged before origins says neither.
		switch e.Origin.Kind {
		case storage.WALOriginOwn:
			replayCtx = withLineage(replayCtx, e.Seq, true)
		case storage.WALOriginReplica:
			replayCtx = withLoggedOwnerDecision(withLineage(replayCtx, e.Origin.Seq, false), e.Origin)
		}
		replayCtx = s.withReplayedEntryTerm(replayCtx, e)
		version, stamped, err := s.replayVersion(e)
		if errors.Is(err, ErrVersionLimit) {
			// Refused before the handler ran: a recorded failure of the entry
			// is settled and reported like any other refusal at replay.
			s.settleReplayedRefusal(replayCtx, e, &applyOutcome{})
			skipped++
			s.logger.Debug("replayed entry not applied", "seq", e.Seq, "op", op, "bucket", e.Bucket, "error", err)
			continue
		}
		if err != nil {
			return fmt.Errorf("replaying sequence %d: %w", e.Seq, err)
		}
		if stamped {
			replayCtx = withWriteVersion(replayCtx, version)
		}
		replayCtx, replayed := withApplyOutcome(replayCtx)
		resp, err := s.dispatch(replayCtx, frame)
		// The owner's replay of its own entries records a refusal as the live
		// apply does, healing a crash between the apply and its record (R8-E).
		if e.Origin.Kind == storage.WALOriginOwn {
			var answer ReplResponse
			if resp != nil {
				answer = ReplResponse{Opcode: int32(resp.Opcode), Payload: resp.Payload}
			}
			if recordErr := s.recordOriginRefusal(op, ReplEntry{Op: int32(e.Op), Bucket: e.Bucket, Payload: e.Data}, e.Seq,
				applyOutcomeOf(answer, err), replayed.storeFailure != nil); recordErr != nil {
				return fmt.Errorf("replaying sequence %d: %w", e.Seq, recordErr)
			}
		}
		// Another node's entry is judged as its live apply was: a divergence
		// whose record a crash took is found again, recorded, and its source
		// held below it (BUG-0581).
		if e.Origin.Kind == storage.WALOriginReplica && s.cluster != nil {
			refused, diverged, judgeErr := s.cluster.replayedDivergence(e, later, resp, err, replayed)
			if judgeErr != nil {
				return fmt.Errorf("replaying sequence %d: %w", e.Seq, judgeErr)
			}
			if diverged {
				if recordErr := s.cluster.replayRecordsDivergence(holds, refused); recordErr != nil {
					return fmt.Errorf("replaying sequence %d: %w", e.Seq, recordErr)
				}
				skipped++
				continue
			}
		}
		// The store passed the entry over by the receiver's rule, as its live
		// apply did, and consumed its sequence: the answer the live apply gave
		// it, not a failure to apply it. One the copy holds, or of an older
		// term, moves its source past it as an applied entry does; one of a
		// later term was refused live, and its source re-sends it.
		if replayed.lineage != nil {
			passedOver++
			if lineageSkipped(replayed.lineage) && s.cluster != nil {
				if err := s.cluster.replayedFromSource(e.Origin.Source, e.Origin.Seq); err != nil {
					return fmt.Errorf("replaying sequence %d: %w", e.Seq, err)
				}
			}
			s.logger.Debug("replayed entry passed over by its lineage", "seq", e.Seq, "op", op, "bucket", e.Bucket, "reason", replayed.lineage)
			continue
		}
		if replayed.heldUnknownID && replayed.committed && s.cluster != nil {
			s.cluster.noteHeldID(e.Origin.Source, e.Data, int32(e.Op), e.Bucket, e.Origin.Seq)
		}
		// A unique index's refusal is the entry's answer, the same one it got
		// when it was live, not a failure to apply it. Its message carries the
		// value, so it is not logged (RD-10).
		if err == nil && resp != nil && resp.Opcode == transport.OpError && refusedByConstraint(resp.Payload) {
			// A declared field type or a path rule refuses before the store
			// too; a recorded failure of the entry is not answered by it.
			s.settleReplayedRefusal(replayCtx, e, replayed)
			rejected++
			continue
		}
		failure := replayDispatchFailure(resp, err)
		if failure != nil {
			s.settleReplayedRefusal(replayCtx, e, replayed)
			skipped++
			s.logger.Debug("replayed entry not applied",
				"seq", e.Seq, "op", op, "bucket", e.Bucket, "error", failure)
			continue
		}
		// A batch too large for this node's store is answered without an
		// error frame; it did not apply, and its refusal is recorded and its
		// source's position held below it (BUG-0248). The refusal is read
		// from the apply's outcome, as the live apply reads it, never from
		// the handler's wording.
		if e.Origin.Kind == storage.WALOriginReplica && s.cluster != nil {
			if replayed.tooLarge {
				if err := s.cluster.replayRefusedAsTooLarge(holds, e.Origin.Source, e.Seq, e.Origin.Seq); err != nil {
					return fmt.Errorf("replaying sequence %d: %w", e.Seq, err)
				}
			} else if err := s.cluster.replayedFromSource(e.Origin.Source, e.Origin.Seq); err != nil {
				return fmt.Errorf("replaying sequence %d: %w", e.Seq, err)
			}
		}
		applied++
	}
	if holds != nil {
		s.cluster.voidHeldBack(holds)
	}

	// "skipped" is the operator-facing name for entries that should have
	// applied and did not; it is kept, not renamed, because an operator may
	// alert on it. "held_for_resend" are other nodes' entries their senders
	// re-send: this node logged them and never applied them.
	// "passed_over_by_lineage" are other nodes' entries the receiver's rule
	// answered without applying, as their live apply did.
	s.logger.Info("replayed write-ahead log",
		"entries", len(entries), "applied", applied, "rejected_by_constraint", rejected, "skipped", skipped, "ddl_below_mark", gated,
		"held_for_resend", heldBack, "passed_over_by_lineage", passedOver)
	return nil
}

// withReplayedEntryTerm attaches to the replay of another node's bucket entry
// the term its log record carries, so the store judges it by the receiver's
// rule as Cluster.finishAsReplica's live apply did. The live apply's pass-over
// consumes the entry in a batch it does not sync, which a power cut can take
// while the entry stays in the log above the mark (BUG-0588). The copy's term
// and position are the ones the live apply met: a term change is synced and
// moves the mark past every entry logged before it, and replay rebuilds the
// position in log order. The position is judged unless the source is clamped
// now; the flag only ever goes from false to true, so replay never judges by a
// position the live apply did not. An entry an earlier build logged (kind 2,
// OutcomeNotLogged) carries no term and replays as that build replayed it,
// judged for nothing.
func (s *Server) withReplayedEntryTerm(ctx context.Context, e storage.WALEntry) context.Context {
	if e.Origin.Kind != storage.WALOriginReplica || e.Origin.OutcomeNotLogged || !judgedByLineage(transport.Opcode(e.Op), e.Bucket) {
		return ctx
	}
	checkPosition := s.cluster != nil && !s.cluster.sourceClamped(e.Origin.Source)
	return withEntryTerm(ctx, e.Origin.Term, checkPosition)
}

// replayVersion is the version a replayed bucket entry is stamped with: the
// one every copy of it carries, packed from the copy's term and the origin's
// sequence — this log's own for an entry this node originated, the one the
// origin block records for another node's. Without it a replayed write is
// version 0, below every position a consistent get compares it with, and the
// absence of the key on the bucket's owner would delete it. The copy's term is
// the term the entry was logged in: taking or accepting a term moves the
// copy's applied_seq to the log's end (Cluster.openTerm, Cluster.acceptTerm),
// so replay applies no entry logged before it, and replay runs before the
// take-over worker starts. The term is read from the bucket's lineage cache,
// so a bucket's state document is read at most once per replay. An entry
// logged before origins, or of no bucket, is not stamped.
func (s *Server) replayVersion(e storage.WALEntry) (uint64, bool, error) {
	var seq uint64
	switch {
	case e.Bucket == "":
		return 0, false, nil
	case e.Origin.Kind == storage.WALOriginOwn:
		seq = e.Seq
	case e.Origin.Kind == storage.WALOriginReplica:
		seq = e.Origin.Seq
	default:
		return 0, false, nil
	}
	op := transport.Opcode(e.Op)
	_, database := decodeFrameMeta(e.Data).scope(op)
	term, _, err := s.bucketLineage(database, e.Bucket)
	if err != nil {
		return 0, false, fmt.Errorf("reading the term to stamp the replay of bucket %q in database %q: %w", e.Bucket, database, err)
	}
	version, err := packVersion(term, seq)
	if err != nil {
		return 0, false, versionLimitRefusal(database, e.Bucket, err)
	}
	return version, true, nil
}

// ddlAtOrBelowMark is spec §5.2.2's replayed-DDL gate (N-09 rules 2 and 3): an
// OpCreateBucket or OpDeleteBucket whose name resolves applies only if its
// sequence is above the resolved bucket's applied_seq. A bucket's applied_seq
// starts at the sequence of its own create — or, for a materialised copy, at
// this node's floor when it was materialised (SA-18) — so every DDL entry that
// came before it is at or below the mark: an old drop cannot hide the bucket
// now under the name, and an old create cannot replace it. A name that does
// not resolve is not gated by a bucket: the log is then the only record, and
// a replayed create followed by its own replayed drop nets out as the log
// says.
//
// Before either, the entry is gated by its database's generation: at or below
// the live generation's created_seq it was appended before that generation
// existed, so it belongs to a dropped one whose own drop replay skips as
// stale (databaseDropAtOrBelowMark). Replaying it would bring that
// generation's bucket — and its replayed writes — back inside the re-created
// database, or, for a drop of a name the new generation reuses, take the new
// bucket (a directly created bucket's applied_seq floor is 0, below every
// entry).
func (s *Server) ddlAtOrBelowMark(op transport.Opcode, e storage.WALEntry, generations map[string]databaseGeneration) (bool, error) {
	name, database := decodeFrameMeta(e.Data).scope(op)
	if generation := generations[database]; generation.live && generation.createdSeq != 0 && e.Seq <= generation.createdSeq {
		return true, nil
	}
	st, err := s.bucketStateNamed(database, name)
	if err != nil {
		return false, err
	}
	return e.Seq <= st.AppliedSeq, nil
}

// bucketStateNamed reads the state document of a bucket by name; a bucket
// this node does not serve has the zero state.
func (s *Server) bucketStateNamed(database, name string) (store.BucketState, error) {
	facts, held := s.peekSettled(database, name)
	if !held || facts.id == 0 {
		return store.BucketState{}, nil
	}
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	st, err := store.LoadBucketState(snap, facts.slot, facts.databaseID, facts.id)
	if err != nil {
		return store.BucketState{}, fmt.Errorf("reading the state of bucket %q in database %q: %w", name, database, err)
	}
	return st, nil
}

// databaseGeneration is what replay's OpDeleteDB gate knows about one
// database, read before replay dispatched anything (databaseDropGenerations).
type databaseGeneration struct {
	// live: LoadCatalog registered the database — its marker, or a bucket
	// entry above any pending drop's max_id, was on disk when replay began.
	live bool
	// createdSeq is the live generation's marker's created_seq; 0 when there
	// is no marker or it predates the field.
	createdSeq uint64
	// pendingDrop and recreated are the original evidence, kept for a
	// marker with no created_seq: a pending drop record exists, and (live
	// only) a bucket above its max_id is attached.
	pendingDrop bool
	recreated   bool
}

// databaseDropGenerations is the replay gate's setup, run once before
// replayWALOnce dispatches its first entry. For every database a logged
// OpDeleteDB names, it records the database's generation as LoadCatalog left
// it: whether it is live, the log position its marker says the live
// generation was created at, and — for a marker that predates that field —
// the original evidence: whether a pending drop record exists and
// whether a bucket above its max_id is attached. LoadCatalog's readCatalog
// already skipped every catalog entry at or below a pending drop's max_id
// (catalog.go:readCatalog; replayWAL always runs after LoadCatalog —
// cmd/server/main.go — never before), and a drop deletes the marker in its
// own batch, so a database live at this moment is a generation nothing
// replayed has touched.
//
// It has to be read now, once, rather than at each OpDeleteDB entry as it is
// reached: the dropped generation's own, still-undispatched CreateDB and
// CreateBucket entries re-create it in memory earlier in the same replay pass
// (service.go:catalogedCreate's "create over a pending expansion" path for a
// bucket whose catalog entry is still at or below a pending drop's max_id).
// A live check at the drop entry would then find a database that belongs to
// nothing but replay's own resurrection of the generation being dropped —
// and either skip a drop that must take effect
// (TestOrdinaryDropStaysDroppedAfterReplay) or, once the drop record had been
// expanded away, refuse to start at all
// (TestCommittedDropThenRestartSucceeds).
//
// O(databases named by logged drops) — one marker read each, plus
// readDatabaseDrops' O(pending database drops) — never O(buckets).
func (s *Server) databaseDropGenerations(entries []storage.WALEntry) (map[string]databaseGeneration, error) {
	drops, err := s.readDatabaseDrops()
	if err != nil {
		return nil, fmt.Errorf("reading pending database drops: %w", err)
	}
	data := s.serverDataAccessor()
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	generations := make(map[string]databaseGeneration)
	for _, e := range entries {
		if transport.Opcode(e.Op) != transport.OpDeleteDB {
			continue
		}
		_, database := decodeFrameMeta(e.Data).scope(transport.OpDeleteDB)
		if _, seen := generations[database]; seen || database == "" {
			continue
		}
		_, pending := drops[database]
		generation := databaseGeneration{pendingDrop: pending}
		if db := data.getDB(database); db != nil {
			generation.live = true
			db.mu.RLock()
			generation.recreated = pending && db.table.count() > 0
			db.mu.RUnlock()
			createdSeq, err := markerCreatedSeq(snap, database)
			if err != nil {
				return nil, err
			}
			generation.createdSeq = createdSeq
		}
		generations[database] = generation
	}
	return generations, nil
}

// markerCreatedSeq reads the created_seq of database's marker: 0 when the
// database has no marker (a legacy database known only from its bucket
// entries) or its marker predates the field.
func markerCreatedSeq(snap storage.KVSnapshot, database string) (uint64, error) {
	encoded, err := snap.Get(catalogKey(database, ""))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("reading the catalog entry of database %q: %w", database, err)
	}
	var marker databaseMarker
	if err := bson.Unmarshal(encoded, &marker); err != nil {
		return 0, fmt.Errorf("decoding the catalog entry of database %q: %w", database, err)
	}
	return uint64(marker.CreatedSeq), nil
}

// databaseDropAtOrBelowMark is the replay gate for OpDeleteDB, symmetric to
// ddlAtOrBelowMark for bucket DDL (N-09): it reports whether the logged drop
// targets a generation older than the one live now, and must be skipped
// rather than take the newer one with it. It decides from
// generations, the snapshot databaseDropGenerations took before replay
// dispatched anything:
//
//   - Not live when replay began: nothing of the name was on disk, so
//     whatever is live now replay itself rebuilt from entries before this
//     one — the generation this drop took. It replays (this is
//     every ordinary drop once the expander has deleted its record, and
//     refusing it kept the node from starting).
//   - Live, with a marker that records its created_seq: the generation's own
//     log position, the database's counterpart of a bucket's applied_seq. A
//     drop at or below it was appended before the generation existed and is
//     skipped; one above it is the live generation's drop — a crash between
//     its WAL append and its own commit — and replays.
//   - Live, with a marker written before created_seq existed: the
//     original evidence. A pending drop record with a bucket above its
//     max_id attached means re-created (skip); a record with none means not
//     (replay); no record leaves an uncommitted drop indistinguishable from a
//     re-created name whose drop was already expanded, and guessing wrong
//     silently drops a re-created database — so replay refuses the entry,
//     loud failure over silent data loss. A generation created after this
//     fix always carries created_seq, so this arm shrinks to databases
//     created before it.
func (s *Server) databaseDropAtOrBelowMark(e storage.WALEntry, generations map[string]databaseGeneration) (bool, error) {
	_, database := decodeFrameMeta(e.Data).scope(transport.OpDeleteDB)
	if database == "" {
		return false, nil
	}
	if s.serverDataAccessor().getDB(database) == nil {
		return false, nil
	}
	generation := generations[database]
	switch {
	case !generation.live:
		return false, nil
	case generation.createdSeq != 0:
		return e.Seq <= generation.createdSeq, nil
	case generation.pendingDrop:
		return generation.recreated, nil
	}
	return false, fmt.Errorf("database %q is live, its catalog marker predates the created_seq field, and no pending drop record remains to check its generation against; "+
		"refusing to replay its drop rather than risk taking a database that was re-created since this entry was appended", database)
}

// commutesWithLater is SA-27's question for a recorded failure that replay
// reaches beneath its bucket's mark (spec §11 Q23): can it apply now and give
// the result its replicas have? Only if no retained entry of the same bucket
// with a sequence in (failed.Seq, appliedSeq] names a key it names. An empty
// key names every key — a batch, a DDL entry — so it never commutes. The
// bucket is the entry's bare name, so two databases' buckets of one name are
// compared together: conservative, and harmless. later is ascending.
func commutesWithLater(failed storage.WALEntry, later []storage.WALEntry, appliedSeq uint64) bool {
	if len(failed.Key) == 0 {
		return false
	}
	for _, l := range later {
		if l.Seq > appliedSeq {
			break
		}
		if l.Bucket != failed.Bucket {
			continue
		}
		if len(l.Key) == 0 || bytes.Equal(l.Key, failed.Key) {
			return false
		}
	}
	return true
}

// replayDispatchFailure reports why an entry did not apply, or nil when it did.
//
// A handler answers an ordinary failure with an error *frame* and a nil Go
// error, deliberately: a handler that returns a Go error has the client's
// stream reset, so the caller sees a torn connection instead of a reason (see
// errors.go). Replay is not a client session, and reading only the Go error
// counted every one of those failures as a successful apply — a replay that
// applied nothing reported "applied=N, skipped=0" and startup carried on.
func replayDispatchFailure(resp *transport.Frame, err error) error {
	if err != nil {
		return err
	}
	if resp == nil || resp.Opcode != transport.OpError {
		return nil
	}
	var errResp ErrorResponse
	if unmarshalErr := bson.Unmarshal(resp.Payload, &errResp); unmarshalErr != nil {
		return fmt.Errorf("dispatch refused the entry and its error frame could not be decoded: %w", unmarshalErr)
	}
	return fmt.Errorf("%s (code %d)", errResp.Message, errResp.Code)
}

// withLoggedOwnerDecision is withOwnerDecided for the replay of an entry
// applied from another node's stream: the owner's verdict is read from the
// log, as the live apply read it from the push, so a restart does not decide
// the insert again. An entry an earlier build logged carries none.
func withLoggedOwnerDecision(ctx context.Context, origin storage.WALOrigin) context.Context {
	if origin.OutcomeNotLogged {
		return ctx
	}
	return withOwnerDecided(ctx, ownerDecisionOf(int32(origin.OwnerOutcome)))
}
