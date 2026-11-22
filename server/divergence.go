package server

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A replica that does not apply what its owner shipped (replication dispatch
// 10, R8-E; review M-7).
//
// When a replica refuses an entry for a reason the owner did not share — a
// create of another incarnation of a bucket it holds (10a.6), or an entry the
// owner applied and this node cannot (10a.8) — it does not acknowledge it. It
// records the refusal durably, before it answers:
//
//	'v' ‖ source  →  {seq, database, bucket, incarnation, reason}
//
// under storage.NSReplication, and while the record stands it refuses a
// re-send of that sequence before logging it. The stream from that source is
// per sender and in order, so it stalls at the refused entry: nothing of the
// source's past it reaches this node, not after a re-send and not after a
// restart. There is at most one record per source. It settles when an entry
// of that source at or past its sequence applies here, or is skipped under a
// snapshot floor.

const divergenceRecordKind byte = 'v'

// The reasons a divergence is recorded for, which are also the refusals a
// stream push is answered with (ReplPushResponse.Refusal).
const (
	// refusalAnotherIncarnation: a create of a bucket this node holds in
	// another incarnation (10a.6). Nothing was created.
	refusalAnotherIncarnation = "another_incarnation"
	// refusalDiverged: an entry its owner applied and this node did not, for
	// a reason the owner did not share (10a.8). Nothing was acknowledged.
	refusalDiverged = "diverged"
)

// errReplicaDiverged is a replica's refusal of an entry its owner applied and
// it could not.
var errReplicaDiverged = errors.New("this node did not apply an entry its owner applied")

// errAnotherIncarnation is a replica's refusal of a create of another
// incarnation of a bucket it holds.
var errAnotherIncarnation = errors.New("this node holds another incarnation of the bucket")

// divergenceRecord is the value of a 'v' record.
type divergenceRecord struct {
	Seq         uint64 `bson:"seq"`
	Database    string `bson:"database"`
	Bucket      string `bson:"bucket"`
	Incarnation uint64 `bson:"incarnation,omitempty"`
	Reason      string `bson:"reason"`
	// Created is the incarnation a refused create named (another_incarnation).
	Created uint64 `bson:"created,omitempty"`
}

// replicaDivergence is the refusal a replica answers a diverged entry with. It
// matches the reason's sentinel.
type replicaDivergence struct {
	source string
	record divergenceRecord
}

func (e *replicaDivergence) Error() string {
	switch e.record.Reason {
	case refusalAnotherIncarnation:
		// Owner ruling 53: exactly the two ways out that end the stall.
		return fmt.Sprintf("bucket %q in database %q: this node holds incarnation %d and %s's create names incarnation %d, so the create was refused "+
			"and nothing of incarnation %d is applied here. Replication from %s to this node is stopped for every bucket, and %s's log is held, "+
			"until one of two things happens: incarnation %d is dropped through %s ('drop bucket %s' in database %s while %s owns it, "+
			"or a drop of the whole database), "+
			"which ends this and leaves this node's copy as it is; or 'migrate %s %s' is run on this node, "+
			"which replaces this node's copy with %s's incarnation %d",
			e.record.Bucket, e.record.Database, e.record.Incarnation, e.source, e.record.Created,
			e.record.Created, e.source, e.source,
			e.record.Created, e.source, e.record.Bucket, e.record.Database, e.source,
			e.record.Database, e.record.Bucket,
			e.source, e.record.Created)
	default:
		return fmt.Sprintf("bucket %q in database %q: %s applied its entry at sequence %d and this node could not, so it was refused here. "+
			"Replication from %s to this node is stopped for every bucket until this is repaired, and %s's log is held: "+
			"run 'migrate %s %s' on this node",
			e.record.Bucket, e.record.Database, e.source, e.record.Seq, e.source, e.source, e.record.Database, e.record.Bucket)
	}
}

func (e *replicaDivergence) Unwrap() error {
	if e.record.Reason == refusalAnotherIncarnation {
		return errAnotherIncarnation
	}
	return errReplicaDiverged
}

func divergenceKey(source string) []byte {
	return sourceKeyPrefix(divergenceRecordKind, source)
}

// divergenceRecordFaultHook, when set, runs in recordDivergence before the
// record commits; an error it returns leaves the record unwritten, as a crash
// between the replica's apply and the record does (BUG-0571). A divergence the
// store consumed never reaches it: its record commits with the consumption
// (stopRecorder). Nil in production.
var divergenceRecordFaultHook atomic.Pointer[func(c *Cluster, source string, seq uint64) error]

// recordDivergence durably records source's refused entry, before the refusal
// is answered, and returns the refusal. It writes the record of a divergence
// the store did not consume (an entry it never reached, or a create of another
// incarnation); one it consumed is recorded in the consuming batch.
func (c *Cluster) recordDivergence(source string, refused *replicaDivergence) error {
	if hook := divergenceRecordFaultHook.Load(); hook != nil {
		if err := (*hook)(c, source, refused.record.Seq); err != nil {
			return fmt.Errorf("recording the divergence from %s at sequence %d: %w", source, refused.record.Seq, err)
		}
	}
	encoded, err := bson.Marshal(refused.record)
	if err != nil {
		return fmt.Errorf("encoding the divergence from %s at sequence %d: %w", source, refused.record.Seq, err)
	}
	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(divergenceKey(source), encoded); err != nil {
		return fmt.Errorf("recording the divergence from %s at sequence %d: %w", source, refused.record.Seq, err)
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("recording the divergence from %s at sequence %d: %w", source, refused.record.Seq, err)
	}
	c.holdDivergence(refused)
	return refused
}

// holdDivergence holds a divergence whose record is durable: a re-send of its
// entry is refused before it is logged, and the bucket is named to the
// take-over worker.
func (c *Cluster) holdDivergence(refused *replicaDivergence) {
	c.divergenceMu.Lock()
	c.divergences[refused.source] = refused.record
	c.divergenceMu.Unlock()
	// The worker re-seeds a diverged copy of its owner's incarnation (step
	// 10b.6); the bucket carries no placement mark, so only this names it.
	c.wakeTakeOver(refused.record.Database, refused.record.Bucket)
}

// refuseRecordedDivergence refuses, before it is logged, a re-send of the
// entry this node recorded a divergence for.
func (c *Cluster) refuseRecordedDivergence(sender string, entry ReplEntry) error {
	c.divergenceMu.Lock()
	record, recorded := c.divergences[sender]
	c.divergenceMu.Unlock()
	if !recorded || record.Seq != entry.Seq {
		return nil
	}
	return &replicaDivergence{source: sender, record: record}
}

// divergenceFrom is the divergence recorded for source, if any.
func (c *Cluster) divergenceFrom(source string) (divergenceRecord, bool) {
	c.divergenceMu.Lock()
	defer c.divergenceMu.Unlock()
	record, recorded := c.divergences[source]
	return record, recorded
}

// settleDivergence forgets source's divergence once an entry of source at or
// past it has applied, or been skipped under a snapshot floor, here.
func (c *Cluster) settleDivergence(source string, seq uint64) error {
	c.divergenceMu.Lock()
	record, recorded := c.divergences[source]
	c.divergenceMu.Unlock()
	if !recorded || seq < record.Seq {
		return nil
	}
	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(divergenceKey(source)); err != nil {
		return fmt.Errorf("settling the divergence from %s at sequence %d: %w", source, record.Seq, err)
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("settling the divergence from %s at sequence %d: %w", source, record.Seq, err)
	}
	c.divergenceMu.Lock()
	if c.divergences[source] == record {
		delete(c.divergences, source)
	}
	c.divergenceMu.Unlock()
	c.logger.Info("a recorded divergence settled; replication from the source resumes", "source", source, "sequence", record.Seq,
		"database", record.Database, "bucket", record.Bucket)
	return nil
}

// lowerPastDivergences loads the recorded divergences at start and holds each
// source's position, just rebuilt from the log, below its refused entry: the
// entry may be in the log, and it did not apply. So may entries of the source
// after it, which a run logged before its apply was refused (BUG-0521); replay
// holds them back (replayHolds), so none of them reaches the copy.
func (c *Cluster) lowerPastDivergences() error {
	snapshot := c.s.kv.NewSnapshot()
	defer snapshot.Discard()
	lower := storage.NodeKey(storage.NSReplication, []byte{divergenceRecordKind})
	iterator := snapshot.NewIterator(storage.IterOptions{Lower: lower, Upper: keyPrefixEnd(lower)})
	defer iterator.Close()
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		decoded, err := decodeReplicationKey(iterator.Key())
		if err != nil {
			return fmt.Errorf("reading the divergences: %w", err)
		}
		stored, err := iterator.Value()
		if err != nil {
			return fmt.Errorf("reading the divergence from %q: %w", decoded.source, err)
		}
		var record divergenceRecord
		if err := bson.Unmarshal(stored, &record); err != nil {
			return fmt.Errorf("decoding the divergence from %q: %w", decoded.source, err)
		}
		c.divergenceMu.Lock()
		c.divergences[decoded.source] = record
		c.divergenceMu.Unlock()
		c.wakeTakeOver(record.Database, record.Bucket)
		c.appliedMu.Lock()
		if c.appliedFrom[decoded.source] >= record.Seq {
			c.appliedFrom[decoded.source] = record.Seq - 1
		}
		c.appliedMu.Unlock()
	}
	if err := iterator.Err(); err != nil {
		return fmt.Errorf("walking the divergences: %w", err)
	}
	return nil
}

// replayedDivergence judges the replay of e, an entry this node logged from
// another node's stream, as finishAsReplica judged its live apply, from the
// owner's outcome its log entry carries: a create of another incarnation of a
// bucket held here, or a refusal of an entry its owner applied for a reason
// the owner did not share. Its record was written after the live apply, in a
// synced write of its own, and a crash before that write left the entry
// logged, counted applied by the start and refused again by replay with
// nothing recorded (BUG-0581). Only an entry above its held bucket's applied
// mark is judged: one at or below it was applied, and a replayed entry of a
// bucket dropped or re-created since is refused for that, not for a
// divergence. An entry of a log its source has gone back on is not judged,
// nor one the source's stream moved past before this node stopped
// (sourceMovedPast): its divergence was recorded and has settled since. later
// is the log after e.
func (c *Cluster) replayedDivergence(e storage.WALEntry, later []storage.WALEntry, resp *transport.Frame, dispatchErr error,
	outcome *applyOutcome) (*replicaDivergence, bool, error) {
	refused, diverged, err := c.replayedRefusal(e, resp, dispatchErr, outcome)
	if err != nil || !diverged {
		return nil, false, err
	}
	movedPast, err := c.sourceMovedPast(e, later)
	if err != nil || movedPast {
		return nil, false, err
	}
	return refused, true, nil
}

// replayedRefusal is replayedDivergence's judgement of e alone.
func (c *Cluster) replayedRefusal(e storage.WALEntry, resp *transport.Frame, dispatchErr error, outcome *applyOutcome) (*replicaDivergence, bool, error) {
	// A pass-over by the store's lineage rule is the entry's answer, as in
	// finishAsReplica: never a divergence (BUG-0588).
	// An entry an earlier build logged carries no owner outcome, and the owner
	// decided on 2026-10-16 that it replays as that build replayed it, judged
	// for nothing. The outcome check below would keep it from a refusal's
	// judgement; another incarnation's reads no outcome, so it is skipped here.
	if dispatchErr != nil || e.Bucket == "" || outcome.storeFailure != nil || outcome.lineage != nil || e.Origin.OutcomeNotLogged {
		return nil, false, nil
	}
	c.appliedMu.Lock()
	rebased := e.Seq <= c.rebasedAt[e.Origin.Source]
	c.appliedMu.Unlock()
	if rebased {
		return nil, false, nil
	}
	op := transport.Opcode(e.Op)
	_, database := decodeFrameMeta(e.Data).scope(op)
	if outcome.anotherIncarnation != nil {
		return &replicaDivergence{source: e.Origin.Source, record: divergenceRecord{Seq: e.Origin.Seq, Database: database, Bucket: e.Bucket,
			Incarnation: outcome.anotherIncarnation.held, Created: outcome.anotherIncarnation.created, Reason: refusalAnotherIncarnation}}, true, nil
	}
	var answer ReplResponse
	if resp != nil {
		answer = ReplResponse{Opcode: int32(resp.Opcode), Payload: resp.Payload}
	}
	// A batch too large for this node's store is a size refusal, which the
	// replay loop stalls on as the live apply does (ownerCommittedWhatThisNodeCannot),
	// never a divergence. It is read from the apply's outcome, not the
	// answer's wording, which a pop's refusal does not share.
	if int32(e.Origin.OwnerOutcome) != ownerOutcomeApplied || outcome.tooLarge ||
		applyOutcomeOf(answer, nil) != ownerOutcomeUnknown {
		return nil, false, nil
	}
	if facts, held := c.s.peekSettled(database, e.Bucket); !held || facts.id == 0 {
		return nil, false, nil
	}
	st, err := c.s.bucketStateNamed(database, e.Bucket)
	if err != nil {
		return nil, false, fmt.Errorf("judging the replay of %s's entry at sequence %d: %w", e.Origin.Source, e.Origin.Seq, err)
	}
	if e.Seq <= st.AppliedSeq {
		return nil, false, nil
	}
	return &replicaDivergence{source: e.Origin.Source, record: divergenceRecord{Seq: e.Origin.Seq, Database: database, Bucket: e.Bucket,
		Incarnation: st.Incarnation, Reason: refusalDiverged}}, true, nil
}

// sourceMovedPast reports whether this node applied, before it stopped, an
// entry of e's source with a later sequence of the source than e's: one in
// later at or below its held bucket's applied mark. A divergence stalls its
// source, and nothing of the source past the refused entry applies until the
// divergence settles (a re-seed, or a drop of the refused incarnation); in the
// crash this replay repairs, the entries of the source after e are the
// orphans of its run, logged and never applied. So an applied one says the
// divergence was recorded and settled, and judging e again would stall the
// stream on a copy repaired since and lower a position this node
// acknowledged. It reads the log after e only for an entry judged diverged,
// and each bucket's state once.
func (c *Cluster) sourceMovedPast(e storage.WALEntry, later []storage.WALEntry) (bool, error) {
	type named struct{ database, bucket string }
	marks := make(map[named]uint64)
	for _, after := range later {
		if after.Origin.Kind != storage.WALOriginReplica || after.Origin.Source != e.Origin.Source ||
			after.Origin.Seq <= e.Origin.Seq || after.Bucket == "" {
			continue
		}
		_, database := decodeFrameMeta(after.Data).scope(transport.Opcode(after.Op))
		bucket := named{database: database, bucket: after.Bucket}
		mark, read := marks[bucket]
		if !read {
			if facts, held := c.s.peekSettled(database, after.Bucket); held && facts.id != 0 {
				st, err := c.s.bucketStateNamed(database, after.Bucket)
				if err != nil {
					return false, fmt.Errorf("reading whether %s's stream moved past its entry at sequence %d: %w", e.Origin.Source, e.Origin.Seq, err)
				}
				mark = st.AppliedSeq
			}
			marks[bucket] = mark
		}
		if after.Seq <= mark {
			return true, nil
		}
	}
	return false, nil
}

// replayRecordsDivergence records a divergence replay found (replayedDivergence)
// unless the same record stands, and holds its source as a standing record
// does at start (lowerPastDivergences, newReplayHolds): the position below the
// refused entry, so the owner re-sends it and the re-send is refused before it
// is logged, and every later entry of the source this replay meets held back
// and voided when it ends. One synced write per divergence, at replay.
func (c *Cluster) replayRecordsDivergence(holds *replayHolds, refused *replicaDivergence) error {
	if standing, recorded := c.divergenceFrom(refused.source); !recorded || standing != refused.record {
		var recordedNow *replicaDivergence
		if err := c.recordDivergence(refused.source, refused); !errors.As(err, &recordedNow) {
			return err
		}
		c.noteDivergence(refused)
	}
	c.appliedMu.Lock()
	if c.appliedFrom[refused.source] >= refused.record.Seq {
		c.appliedFrom[refused.source] = refused.record.Seq - 1
	}
	c.appliedMu.Unlock()
	holds.holdAbove(refused.source, refused.record.Seq)
	return nil
}

// divergedOnce is the (bucket, sender) pairs this node has logged a
// divergence ERROR for, so each is said once.
type divergedPair struct{ database, bucket, sender string }

// noteDivergence counts a divergence and says it, once per bucket and sender,
// with its reach and its remedy (review M-6).
func (c *Cluster) noteDivergence(refused *replicaDivergence) {
	replicaDivergences.Inc()
	pair := divergedPair{database: refused.record.Database, bucket: refused.record.Bucket, sender: refused.source}
	c.divergenceMu.Lock()
	if c.divergedLogged == nil {
		c.divergedLogged = make(map[divergedPair]struct{})
	}
	_, said := c.divergedLogged[pair]
	c.divergedLogged[pair] = struct{}{}
	c.divergenceMu.Unlock()
	if !said {
		c.logger.Error(refused.Error(), "database", pair.database, "bucket", pair.bucket, "sender", pair.sender, "sequence", refused.record.Seq)
	}
}

// noteHeldID says, once per bucket and sender, and counts, that this copy held
// an _id_ its owner did not when the owner's insert of it arrived (spec §6.5
// item 2). The insert was put, so the copies agree for that key and the
// source's stream is not held, as Cluster.recordDivergence would hold it. It
// names the bucket and the sequence, never the document (RD-10).
func (c *Cluster) noteHeldID(sender string, payload []byte, op int32, bucket string, seq uint64) {
	replicaDivergences.Inc()
	_, database := decodeFrameMeta(payload).scope(transport.Opcode(op))
	pair := divergedPair{database: database, bucket: bucket, sender: sender}
	c.divergenceMu.Lock()
	if c.heldIDLogged == nil {
		c.heldIDLogged = make(map[divergedPair]struct{})
	}
	_, said := c.heldIDLogged[pair]
	c.heldIDLogged[pair] = struct{}{}
	c.divergenceMu.Unlock()
	if said {
		return
	}
	c.logger.Error(fmt.Sprintf(
		"Bucket %q in database %q: the replica held an _id_ its owner did not when the owner's insert at sequence %d arrived. "+
			"The owner's document replaced it, so the copies agree for that _id_ now, but they differed before it. "+
			"Compare the bucket on its owner and here with 'hoarddb dump' if other documents may differ too.",
		bucket, database, seq),
		"database", database, "bucket", bucket, "sender", sender, "sequence", seq)
}

// recordRefusal records, in the owner's apply turn, that this node as the
// origin did not apply its entry at seq of database/bucket (R8-E, N-1). The
// state document is rewritten under the bucket's mutex in a state-only batch,
// ordered with the bucket's other applies by the turn. A bucket not held here
// records nothing.
func (s *Server) recordRefusal(database, bucket string, seq uint64, constraint bool) error {
	// A refusal is kept only while a member may still be re-sent its entry:
	// above the lowest member cursor (stop-and-ask S-13; 10,000 refusals made
	// a 219 KB state document, rewritten by each one) and above the log's
	// first sequence. A member never heard from bounds it at 0, keeping all.
	var acked uint64
	if s.cluster != nil {
		acked = s.cluster.replicaAckBound()
	}
	b := s.lockServedRecord(database, bucket)
	if b == nil {
		return nil
	}
	defer b.mu.Unlock()
	var floor uint64
	if s.wal != nil {
		first, err := s.wal.FirstSequence()
		if err != nil {
			return fmt.Errorf("reading the log's first sequence to record a refusal of bucket %q: %w", bucket, err)
		}
		floor = first
	}
	floor = max(floor, acked+1)
	snapshot := s.kv.NewSnapshot()
	st, err := store.LoadBucketState(snapshot, b.slot, storage.LegacyDatabaseID(database), b.id.Load())
	snapshot.Discard()
	if err != nil {
		return fmt.Errorf("recording the refusal of sequence %d of bucket %q: %w", seq, bucket, err)
	}
	st.Refuse(seq, constraint, floor)
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if err := st.Put(batch, b.slot, storage.LegacyDatabaseID(database), b.id.Load()); err != nil {
		return err
	}
	if err := batch.Commit(false); err != nil {
		return fmt.Errorf("recording the refusal of sequence %d of bucket %q: %w", seq, bucket, err)
	}
	if len(st.Refusals) > failedSeqsWarnAt && s.refusalsWarned.CompareAndSwap(false, true) {
		s.logger.Warn("a bucket's state document records many refused writes the log still holds; it shrinks as the log is pruned",
			"database", database, "bucket", bucket, "refusals", len(st.Refusals))
	}
	return nil
}

// recordOriginRefusal records the outcome of an apply this node originated
// when it is neither applied nor a store failure: the owner's live apply
// (applyInTurn) and its replay of its own log both call it. storeFailed is a
// store failure this node owes (FailedSeqs holds it); a batch too large for
// this node's memtable is not owed — no retry here can fit it — so it is
// recorded, and a replica re-sent it from the log takes its own answer
// (the memtable-bound refusal).
func (s *Server) recordOriginRefusal(op transport.Opcode, entry ReplEntry, seq uint64, outcome int32, storeFailed bool) error {
	if entry.Bucket == "" || outcome == ownerOutcomeApplied || storeFailed {
		return nil
	}
	_, database := decodeFrameMeta(entry.Payload).scope(op)
	return s.recordRefusal(database, entry.Bucket, seq, outcome == ownerOutcomeRefusedByConstraint)
}

// divergenceNaming is the divergence recorded against database/bucket, from
// whichever source.
func (c *Cluster) divergenceNaming(database, bucket string) (divergenceRecord, bool) {
	c.divergenceMu.Lock()
	defer c.divergenceMu.Unlock()
	for _, record := range c.divergences {
		if record.Database == database && record.Bucket == bucket {
			return record, true
		}
	}
	return divergenceRecord{}, false
}

// reseedDiverged is migrate's remedy for a replica whose copy diverged (R8-E,
// review M-6). When the owner's copy is of this copy's incarnation, this copy
// is deleted under the exclusive fence — a release, no drop record — and
// pulled again from the owner; the pull's snapshot covers the refused entry,
// so the owner's re-send of it is skipped and the record settles. Across
// incarnations nothing is copied: the refusal names both and the two ways out.
// A holder that refused a create of another incarnation is the exception
// (owner ruling 53): migrate is one of that stall's two exits, so it re-seeds
// the owner's incarnation over this node's copy.
func (c *Cluster) reseedDiverged(ctx context.Context, record divergenceRecord) error {
	database, bucket := record.Database, record.Bucket
	replicas, err := c.Replicas(placementOf(database, bucket))
	if err != nil {
		return err
	}
	owner := replicas[0]
	if owner.ID == c.self.ID {
		return fmt.Errorf("bucket %q in database %q diverged on this node, which owns it; 'migrate' re-seeds a replica from its owner. "+
			"Drop the bucket, or dump it on the node whose copy is wanted and restore it", bucket, database)
	}
	copies, err := c.askCopies(ctx, owner, []ReplBucketRef{{Database: database, Bucket: bucket}})
	if err != nil {
		return fmt.Errorf("asking the owner %s which copy of bucket %q it holds before re-seeding this one: %w", owner.ID, bucket, err)
	}
	local, err := c.s.bucketStateNamed(database, bucket)
	if err != nil {
		return err
	}
	acrossIncarnations := record.Reason == refusalAnotherIncarnation
	if !copies[0].Holds {
		return fmt.Errorf("bucket %q in database %q: its owner %s holds no copy of it, so there is nothing to re-seed this node's from. "+
			"Run 'show buckets in %s' on %s to see what it holds", bucket, database, owner.ID, database, owner.ID)
	}
	if !acrossIncarnations && copies[0].Incarnation != local.Incarnation {
		return fmt.Errorf("bucket %q in database %q: this node holds incarnation %d and its owner %s holds incarnation %d, "+
			"which are two buckets of one name, so nothing was copied over either. "+
			"Drop the bucket, or dump it on the node whose copy is wanted and restore it",
			bucket, database, local.Incarnation, owner.ID, copies[0].Incarnation)
	}
	// Refused before the release, so a node above the disk mark keeps the
	// copy it cannot replace (BUG-0551; refuseLandingOnFullDisk says why).
	if err := c.refuseLandingOnFullDisk(database, bucket); err != nil {
		return fmt.Errorf("re-seeding the diverged copy of bucket %q from its owner %s: %w", bucket, owner.ID, err)
	}
	if err := c.releaseDivergedCopy(database, bucket); err != nil {
		return err
	}
	if err := c.pullBucketPlaced(ctx, database, bucket, owner, true); err != nil {
		return fmt.Errorf("re-seeding the diverged copy of bucket %q from its owner %s: %w", bucket, owner.ID, err)
	}
	c.logger.Info("re-seeded a diverged replica from its owner", "database", database, "bucket", bucket, "owner", owner.ID)
	return nil
}

// releaseDivergedCopy deletes this node's diverged copy under the bucket's
// exclusive fence, as a release does: no drop entry, no drop record.
func (c *Cluster) releaseDivergedCopy(database, bucket string) error {
	fence := c.s.acquireFence(placementOf(database, bucket), fenceExclusive)
	defer fence.release()
	resp, err := c.s.DeleteBucket(DeleteBucketRequest{Database: database, Name: bucket})
	if err != nil {
		return fmt.Errorf("deleting the diverged copy of bucket %q to re-seed it: %w", bucket, err)
	}
	if !resp.Success {
		return fmt.Errorf("deleting the diverged copy of bucket %q to re-seed it: %s", bucket, resp.Message)
	}
	return nil
}

// The sender's side of an another_incarnation stall, and its exit (owner
// ruling 53). A peer that refused a create of this node's stalls the stream at
// it. Once this node's own log carries a drop of the created incarnation,
// every entry of that bucket from the create up to the drop is the dropped
// incarnation's, and none of them is the peer's: the replay ships the peer
// none of them, and ships the drop. The peer's first applied entry past the
// create settles its record (settleDivergence), and the stream moves on for
// every bucket. Nothing is lost: the peer's copy is another incarnation, and a
// drop of the created one leaves it alone (R8-C).
//
// The note lives in memory. A sender that restarts ships the create again,
// the peer refuses it again (its record is durable), and the note is rebuilt
// from that refusal and this node's log.

// refusedCreate is a create of this node's that a peer refused as another
// incarnation of a bucket it holds.
type refusedCreate struct {
	seq              uint64
	database, bucket string
	incarnation      uint64
	// scannedThrough is how far this node's log has been read for a drop of
	// the incarnation; dropSeq is that drop's sequence, 0 until one is found.
	scannedThrough uint64
	dropSeq        uint64
}

// noteRefusedCreate records the create a peer's another_incarnation refusal of
// entries names — the first entry above what the peer applied — and reads the
// log written since for this node's own drop of that incarnation. A found drop
// wakes the sender, so the replay that skips the dropped incarnation's entries
// runs now rather than after the stall's back-off.
func (p *peer) noteRefusedCreate(entries []ReplEntry, appliedThrough uint64) error {
	var refused ReplEntry
	for _, entry := range entries {
		if entry.Seq > appliedThrough {
			refused = entry
			break
		}
	}
	if transport.Opcode(refused.Op) != transport.OpCreateBucket {
		return nil
	}
	p.refusedMu.Lock()
	note := p.refused
	if note == nil || note.seq != refused.Seq {
		var create CreateBucketRequest
		if err := bson.Unmarshal(refused.Payload, &create); err != nil {
			p.refusedMu.Unlock()
			return fmt.Errorf("reading the create %s refused at sequence %d: %w", p.node.ID, refused.Seq, err)
		}
		note = &refusedCreate{seq: refused.Seq, database: create.Database, bucket: refused.Bucket, incarnation: create.Incarnation,
			scannedThrough: refused.Seq}
		p.refused = note
	}
	looked := *note
	p.refusedMu.Unlock()
	if looked.dropSeq != 0 {
		return nil
	}
	dropSeq, scanned, err := p.c.ownDropAfter(looked)
	if err != nil {
		return err
	}
	p.refusedMu.Lock()
	if p.refused == note {
		note.scannedThrough, note.dropSeq = scanned, dropSeq
	}
	p.refusedMu.Unlock()
	if dropSeq != 0 {
		p.c.logger.Info("a peer refused a create of another incarnation, and this node has since dropped that incarnation: "+
			"the peer is sent nothing of it, and replication to the peer resumes",
			"node_id", p.node.ID, "database", looked.database, "bucket", looked.bucket, "create_sequence", looked.seq, "drop_sequence", dropSeq)
		p.wakeSender()
	}
	return nil
}

// ownDropAfter reads this node's log after what refused has scanned, up to the
// synced tail, for a drop this node logged of refused's bucket naming its
// incarnation. It returns the drop's sequence (0 if none) and how far it read.
// One read per stall resumes where the last stopped, so a stall reads its log
// once.
func (c *Cluster) ownDropAfter(refused refusedCreate) (uint64, uint64, error) {
	upTo := c.s.wal.SyncedSequence()
	scanned := refused.scannedThrough
	if upTo <= scanned {
		return 0, scanned, nil
	}
	cursor, err := c.s.wal.OpenCursor(scanned + 1)
	if err != nil {
		return 0, scanned, fmt.Errorf("reading the log for a drop of bucket %q after sequence %d: %w", refused.bucket, scanned, err)
	}
	for scanned < upTo {
		entries, next, err := c.s.wal.ReadBatch(cursor, upTo, catchUpReadBytes)
		if err != nil {
			return 0, scanned, fmt.Errorf("reading the log for a drop of bucket %q after sequence %d: %w", refused.bucket, scanned, err)
		}
		if len(entries) == 0 {
			break
		}
		cursor = next
		for _, entry := range entries {
			scanned = entry.Seq
			if entry.Origin.Kind != storage.WALOriginOwn || transport.Opcode(entry.Op) != transport.OpDeleteBucket || entry.Bucket != refused.bucket {
				continue
			}
			var drop DeleteBucketRequest
			if err := bson.Unmarshal(entry.Data, &drop); err != nil {
				return 0, scanned, fmt.Errorf("reading the drop of bucket %q at sequence %d: %w", refused.bucket, entry.Seq, err)
			}
			if drop.Database == refused.database && drop.Incarnation == refused.incarnation {
				return entry.Seq, scanned, nil
			}
		}
	}
	return 0, scanned, nil
}

// skipsRefusedIncarnation reports whether entry, of this node's own log, is
// one of the dropped incarnation's entries the peer refused the create of:
// the create itself and every later entry of its bucket before the drop.
func (p *peer) skipsRefusedIncarnation(entry storage.WALEntry) bool {
	p.refusedMu.Lock()
	note := p.refused
	var skip bool
	if note != nil {
		skip = note.dropSeq != 0 && entry.Bucket == note.bucket && entry.Seq >= note.seq && entry.Seq < note.dropSeq
	}
	p.refusedMu.Unlock()
	if !skip {
		return false
	}
	_, database := decodeFrameMeta(entry.Data).scope(transport.Opcode(entry.Op))
	return database == note.database
}

// settleRefusedCreate forgets the refused create once the peer has applied
// the drop that ended its stall.
func (p *peer) settleRefusedCreate(appliedThrough uint64) {
	p.refusedMu.Lock()
	defer p.refusedMu.Unlock()
	if p.refused == nil {
		return
	}
	// Past the drop the window is shipped; past the create with no drop
	// found, the peer took the create some other way (a migrate re-seeded it).
	if (p.refused.dropSeq != 0 && appliedThrough >= p.refused.dropSeq) || (p.refused.dropSeq == 0 && appliedThrough >= p.refused.seq) {
		p.refused = nil
	}
}

// wakeSendersRefusedOn wakes the sender of every peer stalled on a refused
// create of database/bucket: this node has just logged a drop of the bucket,
// which may be the exit from that stall. O(members).
func (c *Cluster) wakeSendersRefusedOn(database, bucket string) {
	c.peersMu.RLock()
	defer c.peersMu.RUnlock()
	for _, p := range c.peers {
		p.refusedMu.Lock()
		refused := p.refused != nil && p.refused.bucket == bucket && p.refused.database == database
		p.refusedMu.Unlock()
		if refused {
			p.wakeSender()
		}
	}
}
