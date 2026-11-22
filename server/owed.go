package server

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// The owed-sequence registry (spec §11 Q24, build doc SA-28). A write whose
// store apply failed on the owner is recorded in its bucket's failed_seqs and
// retried by replay — but only while the WAL still holds it, and the replicas
// acknowledge it (they applied it), so retention would otherwise prune the only
// copy. The registry holds every such sequence this node still owes, and
// Cluster.retentionBound keeps the log below the lowest of them.
//
// It is node-wide, never per bucket: O(outstanding failures), zero when
// nothing has failed. Replay rebuilds it before the first retention pass,
// because replay runs before the listeners start and a restarted node's first
// bound is 0. The cost, stated as SA-28 requires: one outstanding failure
// holds the whole node's log until the next restart, because replay is the
// only retry.

// owedBucket names the bucket an owed sequence belongs to. id is what a drop
// forgets it by: a bucket of the same name created since is another id, and
// owes its own writes. The names are for the WARN that
// holds retention.
type owedBucket struct {
	id       uint64
	database string
	bucket   string
}

// owedRegistry maps each owed sequence to its bucket. Its zero value is ready
// to use. Its mutex is a leaf: nothing else is taken while it is held.
type owedRegistry struct {
	mu   sync.Mutex
	owed map[uint64]owedBucket
	// carries is each owing bucket's failed-apply carry, the in-memory half
	// of what the registry's sequences are owed for. A drop forgets both.
	carries owedCarries
}

// add records that this node owes seq to the bucket.
func (r *owedRegistry) add(seq uint64, owner owedBucket) {
	r.mu.Lock()
	if r.owed == nil {
		r.owed = make(map[uint64]owedBucket)
	}
	r.owed[seq] = owner
	owedCount := len(r.owed)
	r.mu.Unlock()
	applyFailuresOwed.Set(float64(owedCount))
}

// remove forgets seq: it applied, was consumed, or took the loud path.
func (r *owedRegistry) remove(seq uint64) {
	r.mu.Lock()
	if _, owed := r.owed[seq]; !owed {
		r.mu.Unlock()
		return
	}
	delete(r.owed, seq)
	owedCount := len(r.owed)
	r.mu.Unlock()
	applyFailuresOwed.Set(float64(owedCount))
}

// dropBucketID forgets every sequence owed to the dropped bucket id, and its
// carry: its state document goes with its range, so nothing will retry them.
// It goes by id, never by name: a database drop forgets after it has released
// the registry's locks, and a bucket of the same name created in between owes
// its own writes.
func (r *owedRegistry) dropBucketID(id uint64) {
	r.mu.Lock()
	for seq, owner := range r.owed {
		if owner.id == id {
			delete(r.owed, seq)
		}
	}
	owedCount := len(r.owed)
	r.mu.Unlock()
	applyFailuresOwed.Set(float64(owedCount))
	r.carries.forgetBucketID(id)
}

// lowest returns the lowest owed sequence and its bucket.
func (r *owedRegistry) lowest() (uint64, owedBucket, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var (
		lowestSeq uint64
		owner     owedBucket
		found     bool
	)
	for seq, bucket := range r.owed {
		if !found || seq < lowestSeq {
			lowestSeq, owner, found = seq, bucket, true
		}
	}
	return lowestSeq, owner, found
}

// count is the number of owed sequences.
func (r *owedRegistry) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.owed)
}

// failedSeqsWarnAt is the size of a bucket's failed_seqs past which the node
// warns, once per start: each write rewrites the state document, so a large
// set costs every write of that bucket its size (spec §4.4 rule 5).
const failedSeqsWarnAt = 64

// largeCommitsWarnAt is the size of a bucket's large_commits past which the
// node warns, once per start: like failed_seqs, the list is rewritten by every
// write of the bucket, and while a peer holds the log below it settleCarry
// prunes nothing, so it grows with each large batch (BUG-0291).
const largeCommitsWarnAt = failedSeqsWarnAt

// appliedWrite names one mutation of one bucket for noteApplyOutcome.
type appliedWrite struct {
	database string
	bucket   string
	b        *inMemoryBucket
	// op is the operation as the ERROR line names it: PUT, DELETE, UPDATE…
	op string
	// emptyPop is a pop that found nothing. Its batch rewrote the state
	// document unchanged, so its answer waits for no sync: a worker polling
	// an empty queue must not cost the disk a flush per poll.
	emptyPop bool
}

// noteApplyOutcome is the server's side of spec §4.4 rule 5 for one mutation
// of a bucket on the shared store, called under the bucket mutex with the
// store's answer. A store failure where failures are recorded is owed and
// holds WAL retention (SA-28); any other outcome settles the sequence. The
// loud path (SA-27) logs its ERROR and counts it. A recorded failure the log no
// longer holds is logged as lost, and a set past failedSeqsWarnAt warns once.
// Last, the bucket's carry is settled in the carry table, so the next engine
// built for the bucket carries what this one owes. The caller still returns
// the error to its client: the write failed.
func (s *Server) noteApplyOutcome(ctx context.Context, write appliedWrite, applyErr error) {
	s.logKeyReadFailure(applyErr)
	seq := applySeqFrom(ctx)
	if outcome := applyOutcomeFrom(ctx); outcome != nil {
		outcome.noted = true
		switch {
		case applyErr == nil:
			outcome.committed = true
		case errors.Is(applyErr, store.ErrApplyFailed):
			outcome.storeFailure = applyErr
		case errors.Is(applyErr, storage.ErrBatchTooLarge):
			outcome.tooLarge = true
		case lineageSkipped(applyErr), errors.Is(applyErr, store.ErrTermAhead):
			outcome.lineage = applyErr
		}
	}
	if seq != 0 {
		if recordsFailures(ctx) && errors.Is(applyErr, store.ErrApplyFailed) {
			s.owed.add(seq, owedBucket{id: write.b.id.Load(), database: write.database, bucket: write.bucket})
		} else {
			s.owed.remove(seq)
		}
	}
	if applyErr == nil && !write.emptyPop {
		noteCommitted(ctx)
	}
	if applyErr == nil {
		applied := lineageFrom(ctx)
		write.b.rec.noteAppliedLocked(applied.originSeq, applied.originated)
	}
	if errors.Is(applyErr, store.ErrNotReplayedOutOfOrder) {
		applyFailuresNotReplayed.Inc()
		s.logger.Error(notReplayedMessage(write, seq),
			"database", write.database, "bucket", write.bucket, "seq", seq, "op", write.op)
	}
	owed := write.b.owedApplies()
	for _, lost := range owed.Unreplayable() {
		s.logger.Error(fmt.Sprintf(
			"Bucket %q in database %q recorded a failed write at sequence %d, and the write-ahead log no longer holds it, so it can never be replayed: "+
				"that write is missing from this node's copy of the bucket, though its replicas applied it. "+
				"Restore the bucket from a replica with 'hoarddb dump' on the replica and 'restore' here.",
			write.bucket, write.database, lost),
			"database", write.database, "bucket", write.bucket, "seq", lost)
	}
	if owed.Recorded() > failedSeqsWarnAt && !write.b.rec.flag(recFailedSeqsWarned) {
		write.b.rec.setFlag(recFailedSeqsWarned, true)
		s.logger.Warn(fmt.Sprintf(
			"Bucket %q in database %q holds %d failed writes waiting to be replayed, and every write to it rewrites that list. "+
				"The store is failing applies for this bucket: check the node's disk and its log for the errors, then restart the node, which retries them.",
			write.bucket, write.database, owed.Recorded()),
			"database", write.database, "bucket", write.bucket, "failed_seqs", owed.Recorded())
	}
	if owed.LargeCommits() > largeCommitsWarnAt && !write.b.rec.flag(recLargeCommitsWarned) {
		write.b.rec.setFlag(recLargeCommitsWarned, true)
		s.logger.Warn(fmt.Sprintf(
			"Bucket %q in database %q holds %d large commits waiting to be pruned, and every write to it rewrites that list. "+
				"This node's write-ahead log still starts below %d of this bucket's large commits, so a lagging peer or the log's retention is holding them: "+
				"run 'nodes' to see the members, and read this node's 'WAL retention on this node is held' warning, which names the one holding them. "+
				"The list shrinks as the log advances.",
			write.bucket, write.database, owed.LargeCommits(), owed.LargeCommits()),
			"database", write.database, "bucket", write.bucket, "large_commits", owed.LargeCommits())
	}
	s.owed.carries.settle(write.b.id.Load(), owed)
}

// bucketOwesApplies reports whether this node owes an apply of the bucket — a
// store failure carried for its next batch or recorded in its state document —
// and so may lag the bucket's replicas. It reads the carry table by the
// bucket's id: O(1), with no bucket lock and no engine. An uncommitted bucket
// (id 0) has no entry, and a dropped one's goes with the drop.
func (s *Server) bucketOwesApplies(database, bucket string) bool {
	facts, held := s.peekBucket(database, bucket)
	return held && s.owed.carries.owes(facts.id)
}

// owedApplies is the failed-apply carry the bucket's engine was built with.
// Called only after that engine applied a write, so it has one.
func (b *inMemoryBucket) owedApplies() *store.OwedApplies {
	switch b.bucketType {
	case BucketTypeBTree:
		return b.btreeStore.Carry()
	case BucketTypeHeap:
		return b.heapStore.Carry()
	case BucketTypeFIFO, BucketTypeLIFO:
		return b.queueStore.Carry()
	case BucketTypeBlob:
		return b.blobStore.Carry()
	default:
		return b.hashStore.Carry()
	}
}

// notReplayedMessage is SA-27's ERROR text, verbatim with its values filled in.
func notReplayedMessage(write appliedWrite, seq uint64) string {
	return fmt.Sprintf("Bucket %q in database %q has a write at sequence %d (%s) that failed on this node and "+
		"was applied on its replicas, and a later write to the same key has already applied here. Applying "+
		"the failed write now would put an older value over a newer one, so it was not applied: this "+
		"node's copy of the bucket differs from its replicas'. Restore the bucket from a replica with "+
		"'hoarddb dump' on the replica and 'restore' here.",
		write.bucket, write.database, seq, write.op)
}
