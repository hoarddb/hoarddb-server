package server

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
)

// The placement pass (replication spec §20.8.2, build doc step 9.5).
//
// After a node adopts an epoch, it compares the previous ring with the new one
// for every bucket it holds — the walk the sweep already makes — and records,
// for the buckets whose placement changed for it and for no others, two marks:
//
//	'h' ‖ storage.PlacementKey(database, bucket)  →  {moved, outside}
//
// moved: the bucket's owner at this epoch is not its owner at the previous
// one. outside: this node was in the bucket's set and no longer is. A mark is
// never overwritten by a later pass; dispatch 10 clears them. The record 'h'
// with no suffix holds the epoch the last pass finished for, so a node that
// stopped in the middle of one runs it again at its next start. At rest, with
// nothing unsettled, there is no mark at all. The marks are also held in
// memory (Cluster.unsettled), loaded at start by one prefix scan.

const (
	placementRecordKind byte = 'h'
	// placementBatchRecords is how many marks one batch of a pass writes.
	placementBatchRecords = 1000
)

// placementMarks is the value of an 'h' record: the epoch each mark was set
// at, 0 when unset.
type placementMarks struct {
	Moved   uint64 `bson:"moved,omitempty"`
	Outside uint64 `bson:"outside,omitempty"`
}

// placementPassRecord is the value of the 'h' record with no suffix. Settled
// is this node's Settled epoch (join-freeze spec §8.1); both writers of the
// record (placementPass, persistSettled) write both fields under
// passRecordMu, so neither drops the other's.
type placementPassRecord struct {
	Finished uint64 `bson:"finished"`
	Settled  uint64 `bson:"settled,omitempty"`
}

// passRecordLocked is the record as it stands in memory, with finished in
// place of the finished epoch. The caller holds passRecordMu.
func (c *Cluster) passRecordLocked(finished uint64) ([]byte, error) {
	encoded, err := bson.Marshal(placementPassRecord{Finished: finished, Settled: c.settled.Load()})
	if err != nil {
		return nil, fmt.Errorf("encoding the placement pass record: %w", err)
	}
	return encoded, nil
}

// persistSettled writes this node's Settled epoch into the placement pass
// record. Not synced: a loss keeps the node unsettled after a restart, and the
// start settles it again — resumeSettling's sweep of the last change, or, for
// a founder still at the first epoch, its founding settle (ruling 52).
func (c *Cluster) persistSettled() error {
	c.passRecordMu.Lock()
	defer c.passRecordMu.Unlock()
	c.placementMu.Lock()
	finished := c.passFinished
	c.placementMu.Unlock()
	encoded, err := c.passRecordLocked(finished)
	if err != nil {
		return err
	}
	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(placementPrefix(), encoded); err != nil {
		return fmt.Errorf("writing the settled epoch: %w", err)
	}
	if err := batch.Commit(false); err != nil {
		return fmt.Errorf("committing the settled epoch: %w", err)
	}
	return nil
}

// placementPrefix is the prefix of every 'h' record, and the key of the one
// that records the finished epoch.
func placementPrefix() []byte {
	return storage.NodeKey(storage.NSReplication, []byte{placementRecordKind})
}

// placementMarkKey is the key of a bucket's marks.
func placementMarkKey(key placementKey) []byte {
	return storage.NodeKey(storage.NSReplication, []byte{placementRecordKind}, []byte(key.encoded))
}

// putPlacementMarks puts a bucket's marks into b.
func putPlacementMarks(b storage.KVBatch, key placementKey, marks placementMarks) error {
	encoded, err := bson.Marshal(marks)
	if err != nil {
		return fmt.Errorf("encoding placement marks: %w", err)
	}
	if err := b.Set(placementMarkKey(key), encoded); err != nil {
		return fmt.Errorf("writing placement marks: %w", err)
	}
	return nil
}

// publishPlacementMarks holds a bucket's marks in memory once the batch that
// wrote them has committed.
func (c *Cluster) publishPlacementMarks(key placementKey, marks placementMarks) {
	c.placementMu.Lock()
	c.unsettled[key] = marks
	c.noteUnsettledLocked()
	c.placementMu.Unlock()
}

// recordedMarks is a bucket's marks as recorded, without those the pass for
// the current epoch has not reached yet. A writer that merges into them holds
// marksWriteMu from this read to its publish.
func (c *Cluster) recordedMarks(key placementKey) placementMarks {
	c.placementMu.Lock()
	defer c.placementMu.Unlock()
	return c.unsettled[key]
}

// forgetPlacementMarks drops a bucket's marks from memory once a batch that
// deletes its 'h' record has committed: the copy they described was released,
// landed afresh, or took a term (spec §20.8.2, "cleared when").
func (c *Cluster) forgetPlacementMarks(key placementKey) {
	c.placementMu.Lock()
	delete(c.unsettled, key)
	c.noteUnsettledLocked()
	c.placementMu.Unlock()
}

// noteUnsettledLocked publishes the mark count and the finished epoch to the
// atomics the write path reads. The caller holds placementMu.
func (c *Cluster) noteUnsettledLocked() {
	c.unsettledCount.Store(int64(len(c.unsettled)))
	c.passFinishedAt.Store(c.passFinished)
}

// mayBeUnsettled reports whether a bucket placed at epoch may carry a mark:
// some mark is held, or the placement pass of epoch has not finished, in which
// case placementMarksOf computes the marks from the rings. Two atomic reads.
func (c *Cluster) mayBeUnsettled(epoch uint64) bool {
	return c.unsettledCount.Load() != 0 || (epoch > InitialEpoch && c.passFinishedAt.Load() < epoch)
}

// loadPlacementMarks reads every mark and the finished epoch at start: one
// prefix scan, O(unsettled buckets).
func (c *Cluster) loadPlacementMarks() error {
	prefix := placementPrefix()
	snapshot := c.s.kv.NewSnapshot()
	defer snapshot.Discard()
	iterator := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyPrefixEnd(prefix)})
	defer iterator.Close()
	marks := make(map[placementKey]placementMarks)
	var finished uint64
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		stored, err := iterator.Value()
		if err != nil {
			return fmt.Errorf("reading a placement record: %w", err)
		}
		suffix := iterator.Key()[len(prefix):]
		if len(suffix) == 0 {
			var record placementPassRecord
			if err := bson.Unmarshal(stored, &record); err != nil {
				return fmt.Errorf("decoding the record of the last placement pass: %w", err)
			}
			finished = record.Finished
			c.settled.Store(record.Settled)
			continue
		}
		var mark placementMarks
		if err := bson.Unmarshal(stored, &mark); err != nil {
			return fmt.Errorf("decoding the placement marks of %x: %w", suffix, err)
		}
		marks[placementKey{encoded: string(suffix)}] = mark
	}
	if err := iterator.Err(); err != nil {
		return fmt.Errorf("walking the placement records: %w", err)
	}
	c.placementMu.Lock()
	c.unsettled = marks
	c.passFinished = finished
	c.noteUnsettledLocked()
	c.placementMu.Unlock()
	return nil
}

// placementFactorReadHook, when set, runs in placementMarksAt once the
// bucket's factor is read and before the marks are computed from it, with this
// node's id: a test holds a pass there while an alter is applied (T-154). Nil
// in production.
var placementFactorReadHook atomic.Pointer[func(nodeID string, key placementKey, factor int)]

// placementMarksAt is the marks a bucket earns at epoch: its owner on the ring
// of epoch − 1 against the ring of epoch, and whether this node is in its set
// on each. False when either ring is not in the history.
func (c *Cluster) placementMarksAt(key placementKey, epoch uint64) (placementMarks, bool) {
	before, recorded := c.ringAt(epoch - 1)
	if !recorded {
		return placementMarks{}, false
	}
	after, recorded := c.ringAt(epoch)
	if !recorded {
		return placementMarks{}, false
	}
	factor := c.effectiveRF(key)
	if hook := placementFactorReadHook.Load(); hook != nil {
		(*hook)(c.self.ID, key, factor)
	}
	oldSet, err := before.Replicas(key.encoded, factor)
	if err != nil || len(oldSet) == 0 {
		return placementMarks{}, false
	}
	newSet, err := after.Replicas(key.encoded, factor)
	if err != nil || len(newSet) == 0 {
		return placementMarks{}, false
	}
	var marks placementMarks
	if oldSet[0].ID != newSet[0].ID {
		marks.Moved = epoch
	}
	if nodeListed(oldSet, c.self.ID) && !nodeListed(newSet, c.self.ID) {
		marks.Outside = epoch
	}
	return marks, true
}

// merged is existing with every mark it lacks taken from earned: a mark once
// set is never overwritten.
func (existing placementMarks) merged(earned placementMarks) placementMarks {
	if existing.Moved == 0 {
		existing.Moved = earned.Moved
	}
	if existing.Outside == 0 {
		existing.Outside = earned.Outside
	}
	return existing
}

// placementPass marks the buckets this node holds whose placement changed at
// epoch, and clears the awaiting records of members that are no longer in
// their bucket's set. It writes in batches of placementBatchRecords, then the finished
// epoch, in the last batch: a crash in the middle leaves the finished epoch
// below, and the next start runs the pass again, which overwrites no mark it
// set. Each batch is synced. The last sync alone would carry the earlier
// batches — the patched Badger's DB.Sync syncs rotated memtables too
// — so the others only make the marks durable as they are written,
// at one sync per thousand marks.
func (c *Cluster) placementPass(epoch uint64) error {
	var held []placementKey
	c.forEachKnownBucket(func(database, bucket string) {
		if c.s.bucketStoreOpen(database, bucket) {
			held = append(held, placementOf(database, bucket))
		}
	})
	if _, recorded := c.ringAt(epoch - 1); !recorded && epoch > InitialEpoch {
		c.logger.Warn("this node does not remember the membership before the one it adopted, so its placement pass marks nothing; "+
			"the hand-off after this change treats every bucket as unmarked",
			"epoch", epoch, "buckets_held", len(held))
	}
	pending := make(map[placementKey]placementMarks, placementBatchRecords)
	commit := func(finished bool) error {
		// The marks merged into pending were read before this commit; a mark
		// another writer recorded since is merged in again here, under the
		// writers' mutex, so the commit never writes an older merge over it.
		c.marksWriteMu.Lock()
		defer c.marksWriteMu.Unlock()
		c.placementMu.Lock()
		for key, marks := range pending {
			pending[key] = c.unsettled[key].merged(marks)
		}
		c.placementMu.Unlock()
		batch := c.s.kv.NewBatch()
		defer batch.Discard()
		for key, marks := range pending {
			encoded, err := bson.Marshal(marks)
			if err != nil {
				return fmt.Errorf("encoding placement marks: %w", err)
			}
			if err := batch.Set(placementMarkKey(key), encoded); err != nil {
				return fmt.Errorf("writing placement marks: %w", err)
			}
		}
		finishedEpoch := epoch
		if finished {
			// Held to the in-memory update below, so markSettled's write of the
			// record neither precedes this one with an older finished epoch nor
			// is overwritten by it with an older Settled.
			c.passRecordMu.Lock()
			defer c.passRecordMu.Unlock()
			// The finished epoch only moves forward: a pass for an older epoch
			// that finishes after a newer one's would otherwise leave the node
			// unfinished at its own epoch, with nothing to run the pass again
			// until a restart (BUG-0511). Read under passRecordMu, the only
			// hold under which a pass writes it.
			c.placementMu.Lock()
			finishedEpoch = max(c.passFinished, epoch)
			c.placementMu.Unlock()
			encoded, err := c.passRecordLocked(finishedEpoch)
			if err != nil {
				return err
			}
			if err := batch.Set(placementPrefix(), encoded); err != nil {
				return fmt.Errorf("writing the finished placement pass: %w", err)
			}
		}
		if err := batch.Commit(true); err != nil {
			return fmt.Errorf("committing the placement marks of epoch %d: %w", epoch, err)
		}
		c.placementMu.Lock()
		for key, marks := range pending {
			c.unsettled[key] = marks
		}
		if finished {
			c.passFinished = finishedEpoch
		}
		c.noteUnsettledLocked()
		c.placementMu.Unlock()
		clear(pending)
		return nil
	}
	for _, key := range held {
		// An interrupted pass is not finished: it runs again at the next start.
		if err := c.ctx.Err(); err != nil {
			return fmt.Errorf("the placement pass for epoch %d was interrupted: %w", epoch, err)
		}
		earned, known := c.placementMarksAt(key, epoch)
		if !known || earned == (placementMarks{}) {
			continue
		}
		c.placementMu.Lock()
		existing := c.unsettled[key]
		c.placementMu.Unlock()
		if marks := existing.merged(earned); marks != existing {
			pending[key] = marks
		}
		if len(pending) == placementBatchRecords {
			if err := commit(false); err != nil {
				return err
			}
		}
	}
	// Before the pass records itself finished, so a finished pass implies the
	// records of members that left their sets are gone, and R7, which refuses
	// an alter until it finishes, never meets a stale one (OQ-G3-9).
	if err := c.clearAwaitingOutsideTheSet(epoch); err != nil {
		return fmt.Errorf("the placement pass for epoch %d: %w", epoch, err)
	}
	return commit(true)
}

// placementPassRetry bounds how long a pass that failed waits before it runs
// again.
const placementPassRetry = 5 * time.Second

// startPlacementPass runs the pass for epoch on a goroutine the cluster waits
// for, and retries it until it finishes or the node stops: a node whose pass
// has not finished adopts no further epoch and votes for none.
func (c *Cluster) startPlacementPass(epoch uint64) {
	done := make(chan struct{})
	c.placementMu.Lock()
	c.passRunning = done
	c.placementMu.Unlock()
	if !c.startStoreGoroutine() {
		close(done)
		return
	}
	go func() {
		defer c.storeWG.Done()
		defer close(done)
		for {
			err := c.placementPass(epoch)
			if err == nil || c.ctx.Err() != nil {
				return
			}
			c.logger.Error("the placement pass failed; it runs again, and this node adopts no further membership change until it finishes",
				"epoch", epoch, "error", err)
			select {
			case <-time.After(placementPassRetry):
			case <-c.ctx.Done():
				return
			}
		}
	}()
}

// errPlacementPassRunning is a wait for the pass that ended with the node.
var errPlacementPassRunning = errors.New("the placement pass for this node's membership epoch has not finished")

// waitForPlacementPass waits for the pass of the current epoch, if one runs.
func (c *Cluster) waitForPlacementPass(ctx context.Context) error {
	c.placementMu.Lock()
	running := c.passRunning
	c.placementMu.Unlock()
	if running == nil {
		return nil
	}
	select {
	case <-running:
	case <-ctx.Done():
		return fmt.Errorf("%w: %w", errPlacementPassRunning, ctx.Err())
	}
	return nil
}

// placementPassFinished reports whether the pass for the current epoch has
// finished. The first epoch has no previous one, and so no pass.
func (c *Cluster) placementPassFinished() bool {
	epoch := c.Epoch()
	if epoch <= InitialEpoch {
		return true
	}
	c.placementMu.Lock()
	defer c.placementMu.Unlock()
	return c.passFinished >= epoch
}

// placementMarksOf is a bucket's marks for the current epoch: as recorded, or,
// while the pass for the current epoch has not reached it, computed here from
// the two rings as the pass would (A-9.4).
func (c *Cluster) placementMarksOf(key placementKey) placementMarks {
	c.placementMu.Lock()
	recorded := c.unsettled[key]
	finished := c.passFinished
	c.placementMu.Unlock()
	epoch := c.Epoch()
	if finished >= epoch {
		return recorded
	}
	earned, _ := c.placementMarksAt(key, epoch)
	return recorded.merged(earned)
}
