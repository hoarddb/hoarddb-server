package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
	"github.com/hoarddb/hoarddb-server/version"
)

// Every member must run the same memtable, because 15% of it is the largest
// batch a member can apply in one transaction: a replica at a smaller size
// cannot apply an owner's batch whole (cluster-settings-spec.md §9.3). Stage 1
// of the host-scaled memtable (memtable-host-scaled-design.md §3) refuses a
// join that would mix sizes and logs members that already do; asking the
// seeds before the store opens is GAP-38's.

// advertisedLimitBytes is the memory limit this start read, 0 when it read none.
func (r memtableResolution) advertisedLimitBytes() int64 {
	if r.limitErr != nil {
		return 0
	}
	return r.limit.bytes
}

// admitNewcomerMemtable asks a newcomer its memtable size and refuses the join
// when it differs from this node's. A newcomer that cannot be asked, one not
// started yet, is admitted: joining a node before it runs is supported, and
// its size is compared on its first gossip, where a mismatch is an ERROR on
// every member and in status.
func (c *Cluster) admitNewcomerMemtable(ctx context.Context, newcomer ring.Node) error {
	resp, _, probeErr := c.probeGossipWithin(ctx, newcomer.Addr, optionalProbeWait)
	if err := checkNewcomerMemtable(newcomer, c.s.memtable.appliedMB, resp, probeErr); err != nil {
		return err
	}
	if probeErr != nil {
		c.logger.Warn(fmt.Sprintf("memtable: %s could not be asked its memtable size (%v), so the join goes ahead without the check. "+
			"Every member must run %d MiB; if %s runs another size, every member logs an ERROR when it first gossips. "+
			"Set storage.memtable_mb: %d in its config file before you start it",
			describePeer(newcomer), probeErr, c.s.memtable.appliedMB, newcomer.ID, c.s.memtable.appliedMB), "component", "storage")
	}
	return nil
}

// checkNewcomerMemtable is the join's decision on a newcomer's answer to the
// probe. A node of this build puts its size on every answer to an
// authenticated exchange, including a refusal of the membership (which a node
// not yet in the cluster gives), and on no answer to an unauthenticated one.
// A node that names itself without the memtable_mb feature runs a different
// build. Anything else (a dial that failed, a node without replication) has
// not answered.
func checkNewcomerMemtable(newcomer ring.Node, clusterMB int, resp GossipResponse, probeErr error) error {
	switch {
	case probeErr != nil:
		return nil
	case containsString(resp.Features, featureMemtableMB):
		if resp.AppliedMemtableMB == 0 || resp.AppliedMemtableMB == clusterMB {
			return nil
		}
		return memtableJoinRefusal(newcomer, clusterMB, resp)
	case resp.Success || resp.Self.ID != "":
		return fmt.Errorf("cannot add %s (%s) to the cluster: %s runs a build that does not report its memtable size, and every member must run the same size. "+
			"Start %s on this build (%s), with storage.memtable_mb: %d in its config file, then run 'join %s' again",
			newcomer.ID, newcomer.Addr, newcomer.ID, newcomer.ID, version.Version, clusterMB, newcomer.Addr)
	}
	return nil
}

// memtableJoinRefusal is the design note's §5.4 refusal. The memory line, and
// the memory part of the fix, appear only when the newcomer would warn at the
// cluster's size: the ruling's "scale the host up to join", said in the
// refusal itself.
func memtableJoinRefusal(newcomer ring.Node, clusterMB int, resp GossipResponse) error {
	text := fmt.Sprintf("cannot add %s (%s) to the cluster: %s runs a %d MiB memtable and this cluster runs %d MiB. "+
		"Every member must run the same size, because it sets the largest batch a member can apply in one transaction (%s at %d MiB, %s at %d MiB).\n",
		newcomer.ID, newcomer.Addr, newcomer.ID, resp.AppliedMemtableMB, clusterMB,
		formatBatch(clusterMB), clusterMB, formatBatch(resp.AppliedMemtableMB), resp.AppliedMemtableMB)
	fit := memtableFit{memtableMB: clusterMB, limitBytes: resp.MemoryLimitBytes}
	memoryFix := ""
	if resp.MemoryLimitBytes > 0 && fit.tooLarge() {
		text += fmt.Sprintf("%s's memory limit is %s; at %d MiB its write bursts can peak near %s.\n",
			newcomer.ID, formatBytesIEC(resp.MemoryLimitBytes), clusterMB, formatPeak(config.PredictedPeakBytes(clusterMB)))
		memoryFix = fmt.Sprintf("give %s at least %s of memory", newcomer.ID, formatGiBUp(fit.memoryNeeded()))
		if clusterMB > config.MinMemtableMB && config.HostMemtableMB(config.HostMemtableMinLimitBytes(clusterMB)) == clusterMB {
			memoryFix += fmt.Sprintf(" (from %s a new store chooses %d MiB by itself)", formatBytesIEC(config.HostMemtableMinLimitBytes(clusterMB)), clusterMB)
		}
		memoryFix += ", "
	}
	text += fmt.Sprintf("To fix it: %sset storage.memtable_mb: %d in %s's config file, restart %s (stop it with SIGTERM, not kill -9), then run 'join %s' again.",
		memoryFix, clusterMB, newcomer.ID, newcomer.ID, newcomer.Addr)
	return errors.New(text)
}

// observePeerMemtable compares a gossiping peer's memtable with this node's.
// A peer that did not advertise the feature said nothing and is not compared.
// A mismatch is logged once per (peer, value) change, not once per round.
func (c *Cluster) observePeerMemtable(nodeID string, features []string, peerMB int) {
	if nodeID == "" || !containsString(features, featureMemtableMB) {
		return
	}
	ownMB := c.s.memtable.appliedMB
	c.memtableMu.Lock()
	defer c.memtableMu.Unlock()
	if peerMB == ownMB {
		delete(c.peerMemtables, nodeID)
		return
	}
	if previous, known := c.peerMemtables[nodeID]; known && previous == peerMB {
		return
	}
	c.peerMemtables[nodeID] = peerMB
	c.logger.Error(fmt.Sprintf("memtable: member %s runs a %d MiB memtable and this node runs %d MiB. "+
		"Every member must run the same size, because it sets the largest batch a member can apply in one transaction (%s at %d MiB, %s at %d MiB): "+
		"the smaller member cannot apply the larger member's biggest batches, and its replication stalls on each one until it is resized. "+
		"To fix it, choose one size, set storage.memtable_mb to it in every member's config file, and restart the members one at a time, "+
		"stopping each cleanly (SIGTERM, not kill -9)",
		nodeID, peerMB, ownMB, formatBatch(peerMB), peerMB, formatBatch(ownMB), ownMB), "component", "storage")
}

// memtableDisagreements lists the peers whose last-gossiped memtable differs
// from this node's, in node order.
func (c *Cluster) memtableDisagreements() []MemtablePeer {
	c.memtableMu.Lock()
	defer c.memtableMu.Unlock()
	differs := make([]MemtablePeer, 0, len(c.peerMemtables))
	for nodeID, peerMB := range c.peerMemtables {
		differs = append(differs, MemtablePeer{NodeID: nodeID, MB: peerMB})
	}
	slices.SortFunc(differs, func(a, b MemtablePeer) int { return strings.Compare(a.NodeID, b.NodeID) })
	return differs
}

// memtableDisagreements is the cluster's list for status, or nothing on a
// standalone node.
func (s *Server) memtableDisagreements() []MemtablePeer {
	if s.cluster == nil {
		return nil
	}
	return s.cluster.memtableDisagreements()
}

// A batch too large for the store is the operation's answer only when every
// member has the same bound. A replica at a smaller memtable than the owner
// that committed a batch cannot commit it in one transaction, and taking that
// refusal for the answer would mark the entry applied with none of it written
//. On an entry its owner reports applied, the replica refuses the
// push instead: nothing is marked applied, the owner re-sends it, and the
// stream stalls with this reason until the member is resized. An entry whose
// owner reports no outcome (an older build) or a refusal keeps the refusal as
// its answer, as before.

// largeCommitBytes is the batch size above which the owner records that it
// committed an entry (store.BucketState.LargeCommits), so a re-send rebuilt
// from its log still says so: half the bound of the smallest memtable a member
// may run. The store counts keys and values only; the half absorbs the
// per-entry overhead Badger adds to its own estimate.
var largeCommitBytes = config.MemtableBatchBytes(config.MinMemtableMB) / 2

// largeCommitEntries is the same record's threshold in keys: the
// store bounds a batch by its entries as well as its bytes, and a batch of
// many small documents reaches the entry bound of the smallest memtable
// (52,428 at 32 MiB) while far under largeCommitBytes. It is a quarter of that
// bound, not a half, because the owner's own replay writes one key per
// document where a replica's live apply writes two (the read-repair version):
// an entry the owner first commits at replay counts half the keys the replica
// will, and the second half absorbs keys that differ between two copies, such
// as index entries an overwrite removes.
var largeCommitEntries = storage.BatchEntryBound(config.MemtableBatchBytes(config.MinMemtableMB)) / 4

// loggedBucket names a bucket an entry read from the log writes to.
type loggedBucket struct {
	database string
	bucket   string
}

// loggedState is the state of the bucket an entry read from the log writes
// to, as this node holds it now: the zero state for an entry of no bucket, or
// of one this node does not hold. states caches each bucket's state for one
// read of the log: one store read per bucket the read touches.
func (c *Cluster) loggedState(states map[loggedBucket]store.BucketState, e storage.WALEntry) (store.BucketState, error) {
	bucket, database := decodeFrameMeta(e.Data).scope(transport.Opcode(e.Op))
	if bucket == "" {
		return store.BucketState{}, nil
	}
	named := loggedBucket{database: database, bucket: bucket}
	if st, cached := states[named]; cached {
		return st, nil
	}
	loaded, err := c.s.bucketStateNamed(database, bucket)
	if err != nil {
		return store.BucketState{}, err
	}
	states[named] = loaded
	return loaded, nil
}

// loggedOwnerOutcome is the owner's outcome for an entry it re-sends from its
// log, which holds the request and not what its apply did: applied when the
// bucket's state records the entry as a large commit, owed when it is one the
// owner has not applied, unknown otherwise. An
// entry not recorded there wrote no more than largeCommitBytes and no more
// than largeCommitEntries keys, which every member's store commits whole, or
// was not committed here — refused, which every member answers alike. An entry
// the owner's store failed and owes to its replay is owed, not unknown: no
// replica may take its own batch-too-large as the answer (BUG-0247).
func loggedOwnerOutcome(st store.BucketState, seq uint64) int32 {
	if st.CommittedLarge(seq) {
		return ownerOutcomeApplied
	}
	if slices.Contains(st.FailedSeqs, seq) {
		return ownerOutcomeOwed
	}
	// The owner recorded every outcome it computed that was not applied and
	// not a store failure (R8-E, N-1), so a sequence at or below applied_seq
	// that is not recorded applied.
	if refused, constraint := st.RefusalAt(seq); refused {
		if constraint {
			return ownerOutcomeRefusedByConstraint
		}
		return ownerOutcomeUnknown
	}
	if seq <= st.AppliedSeq {
		return ownerOutcomeApplied
	}
	// Above the applied mark, and neither applied nor refused: the owner's
	// apply did not commit it, and it is owed to replay (BUG-0247).
	return ownerOutcomeOwed
}

// unappliedWaitLimit bounds how long a replay waits for this node's own
// applies of the entries it read; past it the stream fails and retries.
const unappliedWaitLimit = 10 * time.Second

// A replica logs a pushed entry before it applies it, so the first push of a
// batch too large for this node's store is in its log when the apply refuses
// it, and a restart rebuilds the sender's position from the log
// (rebuildPositionsFromLog) as if the entry had applied: the owner's re-send
// would then be skipped with nothing of the batch written. The refusal is
// therefore recorded durably, per source, before the push is answered: at
// start the position is held below it (lowerPastSizeRefusals) until the
// entry applies, from this node's own log at replay once the member is
// resized, or from a re-send. While recorded at this node's memtable size,
// a re-send is refused before it is logged, so a stalled stream does not grow
// the log by a batch per retry. One record per source at most —
// the stream stalls at the refused entry — and none on a healthy node.
type batchSizeRefusal struct {
	Seq        uint64 `bson:"seq"`
	MemtableMB int    `bson:"memtable_mb"`
}

// sizeRefusalRecordKind is the replication namespace's record of a source's
// entry refused as too large for this node's store; its value is a
// batchSizeRefusal.
const sizeRefusalRecordKind byte = 's'

func sizeRefusalKey(source string) []byte {
	return sourceKeyPrefix(sizeRefusalRecordKind, source)
}

// refuseRecordedBatchTooLarge refuses, before it is logged, a re-send of the
// entry this node last refused from sender as too large for its store, unless
// the node has been resized since, or the owner says it did not commit the
// entry: a record replay wrote (replayRefusedAsTooLarge) can stand for a
// batch the owner refused, and that re-send is logged and its refusal
// consumed as the operation's answer, which settles the record.
func (c *Cluster) refuseRecordedBatchTooLarge(sender string, entry ReplEntry) error {
	c.memtableMu.Lock()
	refusal, recorded := c.batchSizeRefusals[sender]
	c.memtableMu.Unlock()
	if !recorded || refusal.Seq != entry.Seq || refusal.MemtableMB < c.s.memtable.appliedMB {
		return nil
	}
	if entry.OwnerOutcome != ownerOutcomeApplied && entry.OwnerOutcome != ownerOutcomeOwed {
		return nil
	}
	return c.replicaBatchTooLarge(entry)
}

// sizeRefusalRecordFaultHook, when set, runs in recordSizeRefusal before the
// record commits; an error it returns leaves the record unwritten, as a crash
// between the replica's apply and the record's synced commit does (BUG-0248).
// Nil in production.
var sizeRefusalRecordFaultHook atomic.Pointer[func(c *Cluster, sender string, seq uint64) error]

// recordSizeRefusal durably records that sender's entry at seq was refused as
// too large for this node's store, before the refusal is answered.
func (c *Cluster) recordSizeRefusal(sender string, seq uint64) error {
	if hook := sizeRefusalRecordFaultHook.Load(); hook != nil {
		if err := (*hook)(c, sender, seq); err != nil {
			return fmt.Errorf("recording the size refusal of %s's sequence %d: %w", sender, seq, err)
		}
	}
	refusal := batchSizeRefusal{Seq: seq, MemtableMB: c.s.memtable.appliedMB}
	encoded, err := bson.Marshal(refusal)
	if err != nil {
		return fmt.Errorf("encoding the size refusal of %s's sequence %d: %w", sender, seq, err)
	}
	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(sizeRefusalKey(sender), encoded); err != nil {
		return fmt.Errorf("recording the size refusal of %s's sequence %d: %w", sender, seq, err)
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("recording the size refusal of %s's sequence %d: %w", sender, seq, err)
	}
	c.memtableMu.Lock()
	c.batchSizeRefusals[sender] = refusal
	c.memtableMu.Unlock()
	return nil
}

// settleSizeRefusal forgets sender's recorded refusal once an entry at or past
// it has applied here. It reports whether one was settled.
func (c *Cluster) settleSizeRefusal(sender string, seq uint64) (bool, error) {
	c.memtableMu.Lock()
	refusal, recorded := c.batchSizeRefusals[sender]
	c.memtableMu.Unlock()
	if !recorded || seq < refusal.Seq {
		return false, nil
	}
	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(sizeRefusalKey(sender)); err != nil {
		return false, fmt.Errorf("settling the size refusal of %s's sequence %d: %w", sender, refusal.Seq, err)
	}
	if err := batch.Commit(true); err != nil {
		return false, fmt.Errorf("settling the size refusal of %s's sequence %d: %w", sender, refusal.Seq, err)
	}
	c.memtableMu.Lock()
	if c.batchSizeRefusals[sender] == refusal {
		delete(c.batchSizeRefusals, sender)
	}
	c.memtableMu.Unlock()
	return true, nil
}

// lowerPastSizeRefusals loads the recorded refusals and holds each source's
// position, just rebuilt from the log, below its refused entry: the entry is
// in the log and did not apply. The position is lowered wherever it stands at
// or past the entry: a run logs the entries after it before its apply is
// refused (BUG-0521), and a crash before those are voided leaves them counted.
// Replay holds them back (replayHolds). No entry of the source above the
// refused one has applied while the record stands: the stream stalls there,
// and an apply at or past it settles the record before the push moves on.
func (c *Cluster) lowerPastSizeRefusals() error {
	snapshot := c.s.kv.NewSnapshot()
	defer snapshot.Discard()
	lower := storage.NodeKey(storage.NSReplication, []byte{sizeRefusalRecordKind})
	iterator := snapshot.NewIterator(storage.IterOptions{Lower: lower, Upper: keyPrefixEnd(lower)})
	defer iterator.Close()
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		decoded, err := decodeReplicationKey(iterator.Key())
		if err != nil {
			return fmt.Errorf("reading the size refusals: %w", err)
		}
		stored, err := iterator.Value()
		if err != nil {
			return fmt.Errorf("reading the size refusal of source %q: %w", decoded.source, err)
		}
		var refusal batchSizeRefusal
		if err := bson.Unmarshal(stored, &refusal); err != nil {
			return fmt.Errorf("decoding the size refusal of source %q: %w", decoded.source, err)
		}
		c.memtableMu.Lock()
		c.batchSizeRefusals[decoded.source] = refusal
		c.memtableMu.Unlock()
		c.appliedMu.Lock()
		if c.appliedFrom[decoded.source] >= refusal.Seq {
			c.appliedFrom[decoded.source] = refusal.Seq - 1
		}
		c.appliedMu.Unlock()
	}
	if err := iterator.Err(); err != nil {
		return fmt.Errorf("walking the size refusals: %w", err)
	}
	return nil
}

// pruneRemovedSizeRefusals deletes the size-refusal records of sources that
// are no longer members. Nothing will re-send from a source that is gone, so
// the record is stale: left in the store, the next start's scan
// (lowerPastSizeRefusals) would load it and hold a position for a member that
// no longer exists. It is called on every adoption, so it also clears a record
// left by a source an earlier epoch removed (BUG-0250).
func (c *Cluster) pruneRemovedSizeRefusals(members []ring.Node) {
	type staleRefusal struct {
		source string
		seq    uint64
	}
	c.memtableMu.Lock()
	stale := make([]staleRefusal, 0, len(c.batchSizeRefusals))
	for source, refusal := range c.batchSizeRefusals {
		if !nodeListed(members, source) {
			stale = append(stale, staleRefusal{source: source, seq: refusal.Seq})
		}
	}
	c.memtableMu.Unlock()
	for _, record := range stale {
		if _, err := c.settleSizeRefusal(record.source, record.seq); err != nil {
			c.logger.Error("could not clear the size refusal of a removed source; its record stays until the next adoption or restart",
				"source", record.source, "seq", record.seq, "error", err)
		}
	}
}

// replayedFromSource is replay's report that an entry this node logged from
// source's stream applied. If it is the entry a refusal was recorded for —
// the member was resized — the position is raised past it again and the
// refusal settled. Only that entry settles it: an entry of the source logged
// after the refused one applying at replay says nothing of the refused one.
func (c *Cluster) replayedFromSource(source string, seq uint64) error {
	c.memtableMu.Lock()
	refusal, recorded := c.batchSizeRefusals[source]
	c.memtableMu.Unlock()
	if !recorded || refusal.Seq != seq {
		return nil
	}
	settled, err := c.settleSizeRefusal(source, seq)
	if err != nil || !settled {
		return err
	}
	c.markApplied(source, seq)
	return nil
}

// replayRefusedAsTooLarge is replay's report that an entry this node logged
// at localSeq from source's stream, the source's seq, was refused again as too
// large for this node's store. It records the refusal at this node's size, as
// the live apply does, and holds the source's position below the entry.
//
// The replica logs the entry before its apply refuses it and writes the
// record after, so a crash between the two leaves the entry in the log,
// counted by rebuildPositionsFromLog, and no record (BUG-0248). Held below
// it, the position makes the owner re-send the entry; the record makes the
// live apply refuse that re-send before it is logged, so the log holds the
// batch once. A second copy would be applied too, by the resized member's
// replay, which judges no lineage: a fifo or lifo bucket would hold the batch
// twice. Replay reaches the entry only above the bucket's mark: a too-large
// refusal that is the operation's answer is consumed by a synced state-only
// batch (store.ApplyRequest.consume), so replay skips it. What replay refuses
// here is a batch the owner committed, or one whose consume the crash took;
// the record does not refuse the re-send of the latter, which says the owner
// did not commit it (refuseRecordedBatchTooLarge), and that apply consumes it
// and settles the record. The position is held below the entry wherever it
// stands at or past it: the entry is above the mark and unapplied, so the
// source must deliver it again whatever was logged from the source after it.
// One below it, a record already lowered it. An entry at or below the
// source's rebased_at is of a log the source has since gone back on, and its
// sequence is not the position's.
//
// The record is written only when this node does not already hold it at its
// size: one synced write per crash, or per resize that still leaves the
// member too small. Replay runs before the node serves its peers, so no
// re-send reaches the apply before the record is in place. Every entry of the
// source this replay meets after the refused one is held back (holds): the
// position is below them, so the source re-sends them, and a run can have
// logged them behind the refused entry (BUG-0521).
func (c *Cluster) replayRefusedAsTooLarge(holds *replayHolds, source string, localSeq, seq uint64) error {
	c.appliedMu.Lock()
	position := c.appliedFrom[source]
	held := localSeq > c.rebasedAt[source] && position >= seq-1
	c.appliedMu.Unlock()
	if !held {
		return nil
	}
	c.memtableMu.Lock()
	refusal, recorded := c.batchSizeRefusals[source]
	c.memtableMu.Unlock()
	if !recorded || refusal != (batchSizeRefusal{Seq: seq, MemtableMB: c.s.memtable.appliedMB}) {
		if err := c.recordSizeRefusal(source, seq); err != nil {
			return err
		}
	}
	c.appliedMu.Lock()
	c.appliedFrom[source] = seq - 1
	c.appliedMu.Unlock()
	holds.holdAbove(source, seq)
	return nil
}

// ownerCommittedWhatThisNodeCannot reports whether this node's apply refused,
// as too large for its store, an entry its owner applied. The refusal is read
// from the apply's outcome, which noteApplyOutcome sets from the store's typed
// error, and never from the handler's answer: each handler words the refusal
// its own way, and a pop's wording once lost the sentence a text match needed.
func ownerCommittedWhatThisNodeCannot(entry ReplEntry, outcome *applyOutcome) bool {
	return (entry.OwnerOutcome == ownerOutcomeApplied || entry.OwnerOutcome == ownerOutcomeOwed) && outcome.tooLarge
}

// replicaBatchTooLarge is the refusal the owner's stream stalls on, logged
// here once per entry.
func (c *Cluster) replicaBatchTooLarge(entry ReplEntry) error {
	err := errors.New(batchTooLargeReason(c.s.memtable.appliedMB, entry))
	c.memtableMu.Lock()
	logged := c.batchStallLogged == entry.Seq && c.batchStallBucket == entry.Bucket
	c.batchStallLogged, c.batchStallBucket = entry.Seq, entry.Bucket
	c.memtableMu.Unlock()
	if !logged {
		c.logger.Error("memtable: "+err.Error(), "component", "storage", "bucket", entry.Bucket, "seq", entry.Seq)
	}
	return err
}

// isBatchOfDocuments reports whether op is a batch put or batch upsert: the
// two whose committed batch is their request payload.
func isBatchOfDocuments(op transport.Opcode) bool {
	return op == transport.OpBatchPut || op == transport.OpBatchUpsert
}

// batchKeyFloor is the fewest keys a replica's live apply of entry writes, or
// zero when the entry is not a batch of documents. Each document is a key and
// its read-repair version, and the batch adds the state document and Badger's
// commit marker (27,000 documents are 54,002 keys). It is a floor:
// every index entry a document produces comes on top, and the replica cannot
// see those without applying the batch.
func batchKeyFloor(entry ReplEntry) int64 {
	if !isBatchOfDocuments(transport.Opcode(entry.Op)) {
		return 0
	}
	documents, err := bson.Raw(entry.Payload).LookupErr("data")
	if err != nil {
		return 0
	}
	elements, err := documents.Array().Elements()
	if err != nil {
		return 0
	}
	return 2*int64(len(elements)) + 2
}

// memtableFor is the smallest memtable, doubling from startMB, whose batch
// bound commits batchBytes and batchKeys in one transaction, capped at the
// largest memtable a member may run.
func memtableFor(startMB int, batchBytes, batchKeys int64) int {
	neededMB := startMB
	for neededMB < config.MaxMemtableMB &&
		(config.MemtableBatchBytes(neededMB) < batchBytes || storage.BatchEntryBound(config.MemtableBatchBytes(neededMB)) < batchKeys) {
		neededMB *= 2
	}
	return neededMB
}

// batchTooLargeReason is the operator's message for an entry a member of
// memtable size ownMB cannot commit in one transaction. Only a batch put's
// committed batch is its request payload, so only a batch put is sized, by
// both bounds the store enforces, bytes and keys: a batch of many small
// documents is over the key bound while its payload is small. Every other
// op commits more than its request shows (a pop's deletes, a delete's
// cascade over the document's store fields and index entries), so it gets a
// resize reason that prints no size.
func batchTooLargeReason(ownMB int, entry ReplEntry) string {
	// A claim takes the unsized reason, not a pop's: its batch is bounded by
	// the delivery budget at the floor memtable (store.DeliveryBudget), so a
	// member that cannot commit one is below the floor config.MinMemtableMB
	// forbids, and resizing is the whole fix (claim-ack build B-8).
	if isPopOpcode(transport.Opcode(entry.Op)) {
		return popTooLargeReason(ownMB, entry)
	}
	if !isBatchOfDocuments(transport.Opcode(entry.Op)) {
		return unsizedTooLargeReason(ownMB, entry)
	}
	batchBytes := int64(len(entry.Payload))
	batchKeys := batchKeyFloor(entry)
	neededMB := memtableFor(ownMB, batchBytes, batchKeys)
	ownBound := memtableBoundText(ownMB)
	committed := formatMB(batchBytes)
	if batchKeys > 0 {
		committed = fmt.Sprintf("%s and %s keys", committed, groupThousands(int(batchKeys)))
	}
	lead := fmt.Sprintf("this member cannot apply the owner's batch at sequence %d for bucket %q: this member runs a %d MiB memtable, whose largest atomic batch is %s, and the owner committed a batch of about %s",
		entry.Seq, entry.Bucket, ownMB, ownBound, committed)
	if neededMB <= ownMB {
		// The same size fits the batch, so a resize would change nothing. An
		// unresized restart applies it from the member's own log, which writes
		// one key per document where the live apply writes two.
		return lead + ". This member already runs the size the batch fits, so do not resize it: " +
			"restart it (SIGTERM, not kill -9) and it applies the batch from its own log. " +
			"Nothing of it is written here until then and the owner re-sends it"
	}
	return fmt.Sprintf("%s, which needs at least %d MiB. Nothing of it is written here and the owner re-sends it until it is. "+
		"Set storage.memtable_mb to the cluster's size on this member (at least %d MiB) and restart it (SIGTERM, not kill -9)",
		lead, neededMB, neededMB)
}

// memtableBoundText names the largest atomic batch a memtable of ownMB
// commits, by bytes and by keys.
func memtableBoundText(ownMB int) string {
	return fmt.Sprintf("%s and %s keys", formatBatch(ownMB), groupThousands(int(storage.BatchEntryBound(config.MemtableBatchBytes(ownMB)))))
}

// popTooLargeReason is batchTooLargeReason for a pop. A pop's batch is the
// deletes of the entries it removes, which its request does not show, so it
// cannot be sized from the payload; what is known is that it does not fit
// this member's bound, and a restart at the same size refuses the same
// deletes again from the log. The only fix is a larger memtable.
func popTooLargeReason(ownMB int, entry ReplEntry) string {
	limit := int64(1)
	if requested, ok := bson.Raw(entry.Payload).Lookup("limit").AsInt64OK(); ok && requested > 1 {
		limit = requested
	}
	entries := "entries"
	if limit == 1 {
		entries = "entry"
	}
	return resizeOnlyReason(ownMB, entry, fmt.Sprintf("pop of up to %d %s", limit, entries),
		"deleting the entries the owner removed", "removed")
}

// unsizedTooLargeReason is batchTooLargeReason for an op other than a batch
// put or a pop, chiefly a delete: it removes the document with every
// element of its store fields and every index entry, and a TTL expiry is the
// same delete with a condition. The request is a key, so the batch is not
// sized from it and no key is printed (it can be long). Like a pop, a
// restart at the same size refuses the same batch again from the log.
func unsizedTooLargeReason(ownMB int, entry ReplEntry) string {
	operation, cascade := "write", "committing it"
	if transport.Opcode(entry.Op) == transport.OpDelete {
		operation, cascade = "delete", "deleting the document with its store-field elements and index entries"
		if _, expires := bson.Raw(entry.Payload).Lookup("expire_if").DocumentOK(); expires {
			operation = "TTL expiry"
		}
	}
	return resizeOnlyReason(ownMB, entry, operation, cascade, "applied")
}

// resizeOnlyReason is the stall reason for an entry whose committed batch
// cannot be sized from its request: it prints no size, names the operation
// and bucket, and says the member must run a larger memtable.
func resizeOnlyReason(ownMB int, entry ReplEntry, operation, whatDoesNotFit, nothingIs string) string {
	return fmt.Sprintf("this member cannot apply the owner's %s at sequence %d for bucket %q: this member runs a %d MiB memtable, "+
		"whose largest atomic batch is %s, and %s does not fit in it. "+
		"Nothing of it is %s here and the owner re-sends it until it is. "+
		"Set storage.memtable_mb to the cluster's size on this member (more than %d MiB) and restart it (SIGTERM, not kill -9)",
		operation, entry.Seq, entry.Bucket, ownMB, memtableBoundText(ownMB), whatDoesNotFit, nothingIs, ownMB)
}

// formatMB prints a byte count in decimal MB, to one decimal place.
func formatMB(byteCount int64) string {
	return fmt.Sprintf("%.1f MB", float64(byteCount)/1e6)
}
