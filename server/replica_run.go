package server

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-server/transport"
)

// orphanVoidFaultHook, when set, runs before a replica records that entries a
// run logged past a stopped entry are not to be replayed or counted; an error
// it returns leaves the record unmade, in memory and on disk, as a crash
// between the stop and the record does. Nil in production.
var orphanVoidFaultHook atomic.Pointer[func(c *Cluster, source string) error]

// replicaRun is a run of consecutive entries of one stream push that a replica
// has logged and not yet synced or applied (BUG-0521). Applied one at a time,
// each entry of a frame waited for its own fsync, because nothing else was
// appending beside it to share one: a frame of 64 entries cost 64 fsyncs, and
// a stream applied no faster than one entry per fsync. A run appends its
// entries without waiting, syncs the log once, and only then applies them in
// order: the store is synced on its own schedule, so an entry applied before
// its log record is durable could reach the disk without it, and the log's next
// sequences after a power loss would fall at or below the store's applied_seq.
// The push is answered after the run is settled, so an acknowledgement still
// follows the sync of every entry it covers.
//
// A run costs one replicaLogged per entry, at most a frame's maxReplBatch, for
// the length of one push; nothing is kept per bucket. It holds the shared fence
// of each entry's bucket until it is settled, and takes a second fence only if
// that does not wait (tryAcquireFenceShared).
type replicaRun struct {
	c      *Cluster
	sender string
	logged []*replicaLogged
}

// joins reports whether e, an entry of push req, can be logged into the run:
// a bucket entry of the sender's stream that takes its bucket's fence shared,
// past every entry already in the run, not yet applied here and not covered by
// a snapshot. Anything else is answered as before, one entry at a time, after
// the run has been settled.
func (r *replicaRun) joins(req *ReplPushRequest, e ReplEntry) bool {
	op := transport.Opcode(e.Op)
	if req.Stream == nil || req.ReturnResponse || e.Bucket == "" || e.Seq == 0 ||
		!replicationRequired(op) || fenceModeFor(op) != fenceShared {
		return false
	}
	if last := len(r.logged); last > 0 && e.Seq <= r.logged[last-1].entry.Seq {
		return false
	}
	return r.c.shouldApply(r.sender, e.Seq) && !r.c.seededCovers(r.sender, e)
}

// admit logs e into the run. It returns false with a nil error when e's fence
// cannot be taken without waiting while the run holds others. The admission
// checks of logAsReplica run here, before any earlier entry of the run is
// applied, so a caller answers a refusal only when the run is empty; otherwise
// it settles the run and asks again.
func (r *replicaRun) admit(e ReplEntry) (bool, error) {
	key := entryPlacement(e)
	var fence *fenceHold
	if len(r.logged) == 0 {
		fence = r.c.s.acquireFence(key, fenceShared)
	} else if fence = r.c.s.tryAcquireFenceShared(key); fence == nil {
		return false, nil
	}
	logged, err := r.c.logAsReplica(fence, e, r.sender, r.c.appendWALUnsynced)
	if err != nil {
		fence.release()
		return false, err
	}
	r.logged = append(r.logged, logged)
	return true, nil
}

// settle syncs the run's entries with one fsync, then applies them in order and
// hands each outcome to take, as handleReplPush answers an entry applied on its
// own. Once take says stop, the entries after it are released unapplied and
// voided (replica_orphans.go): they are in the log and were never applied, so
// they are neither marked applied nor acknowledged, the sender re-sends them,
// and neither the position rebuilt at start nor replay may take them for
// applied or owed. The stopped entry is voided with them, and on its own when
// none follow it if the store left it unapplied. A stop the store judged and
// consumed, an entry of a later term or a divergence, was voided through the
// run's last entry in the batch that consumed it, so nothing is written here.
// settle reports whether take said stop. The run is empty afterwards.
func (r *replicaRun) settle(ctx context.Context, take func(ReplEntry, ReplResponse, error) bool) bool {
	logged := r.logged
	r.logged = nil
	if len(logged) == 0 {
		return false
	}
	var syncErr error
	if err := r.c.s.wal.SyncThrough(logged[len(logged)-1].localSeq); err != nil {
		syncErr = &entryLeftUnappliedError{err: fmt.Errorf("syncing the log through the %d entries of %s's push before applying them: %w", len(logged), r.sender, err)}
	}
	ctx = withPeerEntry(ctx, true)
	for _, entry := range logged {
		entry.voidThrough = logged[len(logged)-1].localSeq
	}
	for i, entry := range logged {
		var entryResp ReplResponse
		err := syncErr
		if err == nil {
			entryResp, err = r.c.finishAsReplica(ctx, entry, r.sender, true)
		}
		entry.release(r.c)
		if take(entry.entry, entryResp, err) {
			orphans := logged[i+1:]
			for _, unapplied := range orphans {
				unapplied.release(r.c)
			}
			switch {
			case entry.stopVoided:
				// The void through the run's last entry committed with the
				// stopped entry's consumption (stopRecorder).
			case len(orphans) > 0:
				r.c.voidUnapplied(r.sender, orphans[len(orphans)-1].localSeq)
			case leftUnapplied(err):
				r.c.voidUnapplied(r.sender, entry.localSeq)
			}
			return true
		}
	}
	return false
}
