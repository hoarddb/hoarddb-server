package server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
)

// The member's pending-copy worker (bucket-replication-rev8-spec §7.5; the G4
// to G7 build doc steps 1 to 3). A member an increase newly places that holds
// nothing of the bucket keeps a needs-a-copy record (bucket_rf_needs_copy.go);
// this worker makes the copy. Its queue holds one entry per record, filled by
// one prefix scan after the WAL replay and by every alter that writes one, and
// emptied by every delete of a record, after that delete commits. With
// auto-rebalance on it pulls each bucket from its owner by this node's ring,
// one pull at a time, and backs off per bucket; a copy refused for this node's
// disk waits at the ceiling and says why once an hour. Nothing at rest: an
// entry lives only while its record does.

// pendingCopyRetryFloor and pendingCopyRetryCeiling bound one bucket's wait
// between two attempts, as relocation's do: one second doubling to thirty.
// They are atomics so a test shortens them while the worker runs.
//
// An attempt itself is bounded too: each request a pull sends its source ends
// after pullRequestTimeout (two minutes) or, for MIGRATE_START, pullStartTimeout
// (thirty minutes; both in migrate.go), so a source that
// accepts the connection and never answers fails the attempt, which then backs
// off here like any other, and the worker moves on to the next bucket.
var (
	pendingCopyRetryFloor   = newSweepBound(relocationRetryFloor)
	pendingCopyRetryCeiling = newSweepBound(relocationRetryCeiling)
)

// pendingCopyWarnInterval is the least time between two lines saying one
// bucket's copy waits for this node's disk (spec §7.5: one line per bucket per
// hour).
const pendingCopyWarnInterval = time.Hour

// pendingCopyListInterval is how often a worker that moves nothing, with
// auto-rebalance off, lists what is pending (build doc SA-1): at its first
// pass and then once each interval while anything is. An atomic, so a test
// shortens it.
var pendingCopyListInterval = newSweepBound(5 * time.Minute)

// pendingCopiesListed is how many pending copies one listing names by its
// command; the count covers the rest.
const pendingCopiesListed = 10

// pendingCopyPullHook, when set, runs before each of the worker's pulls with
// this node's id and the bucket, and an error it returns is the attempt's
// outcome, in place of the pull: a test makes a source fail without stopping
// it. Nil in production.
var pendingCopyPullHook atomic.Pointer[func(nodeID, database, bucket string) error]

// errPendingCopyOwnedHere is an attempt passed over because this node is the
// bucket's owner by its own ring: the take-over worker copies a bucket its
// owner does not hold.
var errPendingCopyOwnedHere = errors.New("this node is the bucket's owner by its ring, so the take-over copies it in, not the pending-copy worker")

// errPullAlreadyRunning is an attempt passed over because a pull of the bucket
// is already running on this node, an operator's migrate's: it is tried again
// once that one has ended.
var errPullAlreadyRunning = errors.New("a pull of the bucket is already running on this node")

// pendingCopyQueue is the worker's queue: one entry per needs-a-copy record,
// by placement key. mu is a leaf, taken after a record's commit and never
// across one. wake is buffered by one: a wake while a pass runs is kept for
// the next. listedAt is when the auto-rebalance-off listing was last logged.
type pendingCopyQueue struct {
	mu       sync.Mutex
	entries  map[placementKey]*pendingCopy
	wake     chan struct{}
	listedAt time.Time
}

func newPendingCopyQueue() pendingCopyQueue {
	return pendingCopyQueue{entries: make(map[placementKey]*pendingCopy), wake: make(chan struct{}, 1)}
}

// pendingCopy is one queued bucket: its names and generation, the record's
// write time, the last attempt's time and reason, the next retry, the wait
// that gave it, and when its disk refusal was last logged.
type pendingCopy struct {
	database, bucket string
	generation       int64
	since            time.Time
	lastAttempt      time.Time
	reason           string
	nextRetry        time.Time
	wait             time.Duration
	warnedAt         time.Time
}

// pendingCopyOutcome is what this node says of one pending copy (spec §7.5,
// "what the member remembers"): when its record was written, the last
// attempt's time and reason, and the next retry. Zero times before the first
// attempt.
type pendingCopyOutcome struct {
	since, lastAttempt, nextRetry time.Time
	reason                        string
}

// needsCopyPrefix is the prefix of every needs-a-copy record.
func needsCopyPrefix() []byte {
	return storage.NodeKey(storage.NSReplication, []byte{storage.NeedsCopyRecordKind})
}

// queuePendingCopy puts the bucket of record in the queue, due now, and wakes
// the worker. A bucket already queued keeps its outcome and its back-off: a
// replayed alter writes the same record again.
func (c *Cluster) queuePendingCopy(record needsCopyRecord) {
	key := placementOf(record.Database, record.Bucket)
	c.pendingCopies.mu.Lock()
	if _, queued := c.pendingCopies.entries[key]; !queued {
		c.pendingCopies.entries[key] = &pendingCopy{database: record.Database, bucket: record.Bucket, generation: record.Generation,
			since: time.Unix(record.Since, 0), nextRetry: time.Now()}
	}
	c.pendingCopies.mu.Unlock()
	c.wakePendingCopy()
}

// wakePendingCopy wakes the worker; it never blocks.
func (c *Cluster) wakePendingCopy() {
	select {
	case c.pendingCopies.wake <- struct{}{}:
	default:
	}
}

// forgetPendingCopy takes the bucket placed by key out of the queue, with its
// last outcome and next retry, once the batch that deleted its record has
// committed (spec §3.3, RF-9). A record is deleted in six places, and each
// calls this or forgetPendingCopiesOf after its commit: the landing
// (Server.commitMigratedCatalog), the bucket's drop on a node that holds no
// copy (Server.recordDropOfNoCopy), a database drop of the record's
// generation (Server.commitGenerationDrop) or of every generation
// (Server.commitDatabaseDrop), an alter that no longer places this node
// (Server.alterOfNoCopy, row 5), and the worker's own end of a copy that is
// held or confirmed not placed (Server.deleteNeedsCopy). A bucket that is not
// queued is nothing to do: a drop replayed at start runs before the start-up
// scan has filled the queue.
func (c *Cluster) forgetPendingCopy(key placementKey) {
	c.pendingCopies.mu.Lock()
	defer c.pendingCopies.mu.Unlock()
	delete(c.pendingCopies.entries, key)
}

// forgetPendingCopiesOf takes out of the queue every bucket of database whose
// record's generation is one of generations, every one when generations is
// nil, once the drop's batch that deleted their records by prefix has
// committed. It walks the queue, O(pending buckets); the records themselves
// are found by the drop's prefix scan, never through the queue.
func (c *Cluster) forgetPendingCopiesOf(database string, generations []int64) {
	scope := string(databaseRecordScope(database))
	c.pendingCopies.mu.Lock()
	defer c.pendingCopies.mu.Unlock()
	for key, entry := range c.pendingCopies.entries {
		if strings.HasPrefix(key.encoded, scope) && (generations == nil || slices.Contains(generations, entry.generation)) {
			delete(c.pendingCopies.entries, key)
		}
	}
}

// pendingCopyOf is this node's pending copy of the bucket placed by key, and
// whether there is one.
func (c *Cluster) pendingCopyOf(key placementKey) (pendingCopyOutcome, bool) {
	c.pendingCopies.mu.Lock()
	defer c.pendingCopies.mu.Unlock()
	entry, queued := c.pendingCopies.entries[key]
	if !queued {
		return pendingCopyOutcome{}, false
	}
	return pendingCopyOutcome{since: entry.since, lastAttempt: entry.lastAttempt, nextRetry: entry.nextRetry, reason: entry.reason}, true
}

// loadPendingCopies is the start-up scan: every needs-a-copy record into the
// queue, by one prefix scan, O(pending records), never O(buckets). It runs
// after the WAL replay, whose drops have deleted the records of what they
// dropped, and adds to what the replay's alters queued.
func (c *Cluster) loadPendingCopies() error {
	prefix := needsCopyPrefix()
	snapshot := c.s.kv.NewSnapshot()
	defer snapshot.Discard()
	iterator := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyPrefixEnd(prefix)})
	defer iterator.Close()
	var records []needsCopyRecord
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		stored, err := iterator.Value()
		if err != nil {
			return fmt.Errorf("reading a needs-a-copy record: %w", err)
		}
		var record needsCopyRecord
		if err := bson.Unmarshal(stored, &record); err != nil {
			return fmt.Errorf("decoding the needs-a-copy record at %x: %w", iterator.Key(), err)
		}
		records = append(records, record)
	}
	if err := iterator.Err(); err != nil {
		return fmt.Errorf("walking the needs-a-copy records: %w", err)
	}
	for _, record := range records {
		c.queuePendingCopy(record)
	}
	return nil
}

// runPendingCopyWorker is the pending-copy worker: one goroutine per node, on
// storeWG, woken by an alter that queues a bucket and by the nearest retry. It
// holds no fence and no lock across an exchange with another node.
func (c *Cluster) runPendingCopyWorker() {
	defer c.storeWG.Done()
	for {
		next := c.pendingCopyPass(c.ctx)
		var retry <-chan time.Time
		var timer *time.Timer
		if !next.IsZero() {
			timer = time.NewTimer(time.Until(next))
			retry = timer.C
		}
		select {
		case <-c.ctx.Done():
		case <-c.pendingCopies.wake:
		case <-retry:
		}
		if timer != nil {
			timer.Stop()
		}
		if c.ctx.Err() != nil {
			return
		}
	}
}

// pendingCopyPass makes one attempt at each bucket that is due, one at a time
// (build doc SA-20), and returns when the next one is due, zero for none. With
// auto-rebalance off it moves nothing and lists what is pending instead.
func (c *Cluster) pendingCopyPass(ctx context.Context) time.Time {
	if !c.s.config.Server.AutoRebalance {
		return c.listPendingCopies(time.Now())
	}
	for _, key := range c.duePendingCopies(time.Now()) {
		if ctx.Err() != nil {
			return time.Time{}
		}
		c.copyPending(ctx, key)
	}
	return c.nextPendingRetry()
}

// listPendingCopies is the worker's pass with auto-rebalance off (spec §7.5,
// build doc SA-1): when anything is pending and no listing has been logged
// for an interval, one line with the count and the command that copies each
// of the first ten by hand, 'migrate <database> <bucket>' on this node, which
// leaves the placement decision to the source. It returns when the next
// listing is due, zero when nothing is pending. Nothing per bucket is logged.
func (c *Cluster) listPendingCopies(now time.Time) time.Time {
	interval := pendingCopyListInterval.get()
	c.pendingCopies.mu.Lock()
	count := len(c.pendingCopies.entries)
	if count == 0 {
		c.pendingCopies.mu.Unlock()
		return time.Time{}
	}
	if !c.pendingCopies.listedAt.IsZero() && now.Sub(c.pendingCopies.listedAt) < interval {
		next := c.pendingCopies.listedAt.Add(interval)
		c.pendingCopies.mu.Unlock()
		return next
	}
	c.pendingCopies.listedAt = now
	keys := slices.SortedFunc(maps.Keys(c.pendingCopies.entries), func(a, b placementKey) int { return strings.Compare(a.encoded, b.encoded) })
	commands := make([]string, 0, min(count, pendingCopiesListed))
	for _, key := range keys[:min(count, pendingCopiesListed)] {
		entry := c.pendingCopies.entries[key]
		commands = append(commands, fmt.Sprintf("migrate %s %s", entry.database, entry.bucket))
	}
	c.pendingCopies.mu.Unlock()
	c.logger.Warn("copies of buckets are pending on this node and auto_rebalance is off, so nothing copies them: run each command on this node "+
		"(the first ten are listed)",
		"node_id", c.self.ID, "pending", count, "commands", commands)
	return now.Add(interval)
}

// listAlterPendingCopies is the owner's listing of what an increase it just
// applied waits for (spec §7.5; build doc SA-21): one confirm pass over the
// bucket, and one line with the copies still to make. With auto-rebalance
// off each carries the 'migrate' command to run on its member; with it on,
// each member's pending-copy worker makes it. It runs on a goroutine of its
// own on storeWG, started once the alter's batch has committed, because the
// pass asks the members over the network and reads this node's copy under the
// bucket's fence, which the alter's caller still holds. A node whose cluster
// has not started, replaying its log at start, lists nothing. One goroutine
// and one question per member per increase; nothing per bucket at rest.
func (c *Cluster) listAlterPendingCopies(database, bucket string) {
	if !c.startCalled.Load() || !c.startStoreGoroutine() {
		return
	}
	go func() {
		defer c.storeWG.Done()
		_, unconfirmed, waitingOn := c.confirmPass(c.ctx, []heldBucket{{database: database, bucket: bucket}})
		if len(unconfirmed) == 0 {
			return
		}
		commands := make([]string, 0, min(len(unconfirmed), pendingCopiesListed))
		for _, copyToMake := range unconfirmed[:min(len(unconfirmed), pendingCopiesListed)] {
			commands = append(commands, fmt.Sprintf("migrate %s %s on %s", copyToMake.Database, copyToMake.Bucket, copyToMake.Node))
		}
		if !c.s.config.Server.AutoRebalance {
			c.logger.Warn("an increase placed copies of a bucket on members that hold none yet, and auto_rebalance is off, so nothing copies them: "+
				"run each command on the member it names",
				"node_id", c.self.ID, "database", database, "bucket", bucket, "pending", len(unconfirmed), "commands", commands, "not_answering", waitingOn)
			return
		}
		c.logger.Info("an increase placed copies of a bucket on members that hold none yet; each member copies it in by itself",
			"node_id", c.self.ID, "database", database, "bucket", bucket, "pending", len(unconfirmed), "copies", commands, "not_answering", waitingOn)
	}()
}

// duePendingCopies is the queued buckets whose next retry is at or before now,
// in key order.
func (c *Cluster) duePendingCopies(now time.Time) []placementKey {
	c.pendingCopies.mu.Lock()
	defer c.pendingCopies.mu.Unlock()
	var due []placementKey
	for key, entry := range c.pendingCopies.entries {
		if !entry.nextRetry.After(now) {
			due = append(due, key)
		}
	}
	slices.SortFunc(due, func(a, b placementKey) int { return strings.Compare(a.encoded, b.encoded) })
	return due
}

// nextPendingRetry is the earliest next retry in the queue, zero when it is
// empty.
func (c *Cluster) nextPendingRetry() time.Time {
	c.pendingCopies.mu.Lock()
	defer c.pendingCopies.mu.Unlock()
	var next time.Time
	for _, entry := range c.pendingCopies.entries {
		if next.IsZero() || entry.nextRetry.Before(next) {
			next = entry.nextRetry
		}
	}
	return next
}

// copyPending is one attempt at the bucket placed by key: a copy already held,
// landed by another path, ends its record (rev 4 F-18's mirror case);
// otherwise it is pulled from the owner, and a pull that does not land keeps
// the record and backs off.
func (c *Cluster) copyPending(ctx context.Context, key placementKey) {
	c.pendingCopies.mu.Lock()
	entry, queued := c.pendingCopies.entries[key]
	var database, bucket string
	if queued {
		database, bucket = entry.database, entry.bucket
	}
	c.pendingCopies.mu.Unlock()
	if !queued {
		return
	}
	if c.s.bucketStoreOpen(database, bucket) {
		c.endHeldPendingCopy(key, database, bucket)
		return
	}
	err := c.pullPendingCopy(ctx, key, database, bucket)
	if err == nil {
		// The landing deleted the record in its batch and the entry after it;
		// a pull that found the bucket served from the stream lands nothing,
		// and the record goes here.
		c.endHeldPendingCopy(key, database, bucket)
		return
	}
	if errors.Is(err, errNotPlacedHere) {
		confirmed, why := c.confirmNotPlaced(ctx, key, database, bucket)
		if confirmed {
			c.endUnplacedPendingCopy(key, database, bucket, err)
			return
		}
		err = fmt.Errorf("%w; %w: %w", err, errNotPlacedUnconfirmed, why)
	}
	c.notePendingCopyFailure(key, err)
}

// errNotPlacedUnconfirmed marks a NotPlaced the bucket's owner did not
// confirm: the copy stays pending and is tried again.
var errNotPlacedUnconfirmed = errors.New("the bucket's owner does not confirm it")

// confirmNotPlaced asks the bucket's owner, over askCopies, one bucket in one
// frame, whether a NotPlaced is final (owner R9-Q6): only when the owner holds
// an opened copy whose factor does not place this node in this node's ring at
// its current epoch. A factor of 0 is the cluster default. The source's
// NotPlaced may come from a registry behind the alter, and this node's own
// second opinion from a snapshot's factor; neither says the owner is settled
// (spec §7.5). When it does not confirm, the error says why.
func (c *Cluster) confirmNotPlaced(ctx context.Context, key placementKey, database, bucket string) (bool, error) {
	owner, err := c.Owner(key)
	if err != nil {
		return false, fmt.Errorf("placing the bucket to ask its owner: %w", err)
	}
	copies, err := c.askCopies(ctx, owner, []ReplBucketRef{{Database: database, Bucket: bucket}})
	if err != nil {
		return false, fmt.Errorf("asking the owner %s: %w", owner.ID, err)
	}
	answer := copies[0]
	if !answer.Holds || !answer.Opened {
		return false, fmt.Errorf("the owner %s holds no opened copy (holds %v, opened %v)", owner.ID, answer.Holds, answer.Opened)
	}
	factor := int(answer.Factor)
	if factor == 0 {
		factor = c.rf
	}
	set, err := c.setAt(c.Epoch(), key, factor)
	if err != nil {
		return false, fmt.Errorf("placing the bucket at the owner's replication_factor %d: %w", factor, err)
	}
	if nodeListed(set, c.self.ID) {
		return false, fmt.Errorf("the owner %s's copy, at replication_factor %d, places this node", owner.ID, factor)
	}
	return true, nil
}

// endUnplacedPendingCopy ends a pending copy the owner confirmed does not
// place this node: a later alter un-placed it and has not reached this node
// yet. The record goes, then the queue entry, and one INFO line says so.
func (c *Cluster) endUnplacedPendingCopy(key placementKey, database, bucket string, notPlaced error) {
	if err := c.s.deleteNeedsCopy(key, func() bool { return true }); err != nil {
		c.notePendingCopyFailure(key, err)
		return
	}
	c.logger.Info("a pending copy of a bucket ended: its owner confirms its factor no longer places this node, "+
		"so nothing is copied; the alter that un-placed it reaches this node later",
		"node_id", c.self.ID, "database", database, "bucket", bucket, "answer", notPlaced)
}

// pullPendingCopy pulls the bucket from its owner by this node's ring, never
// from another holder, which may not have applied the alter and would answer
// NotPlaced for good (spec §7.5 step 2). pullBucketPlaced's guards apply, the
// disk guard first. The pull is registered as an operator's migrate registers
// one, so a migrate of the bucket meanwhile waits for this pull's outcome.
func (c *Cluster) pullPendingCopy(ctx context.Context, key placementKey, database, bucket string) error {
	if hook := pendingCopyPullHook.Load(); hook != nil {
		if err := (*hook)(c.self.ID, database, bucket); err != nil {
			return err
		}
	}
	if c.removed() {
		return errRemovedNodeDoesNotPull
	}
	owner, err := c.Owner(key)
	if err != nil {
		return fmt.Errorf("placing bucket %q in database %q to copy it from its owner: %w", bucket, database, err)
	}
	if owner.ID == c.self.ID {
		return errPendingCopyOwnedHere
	}
	release, claimed := c.claimPull(database, bucket)
	if !claimed {
		return errPullAlreadyRunning
	}
	defer release()
	if err := c.pullBucketPlaced(ctx, database, bucket, owner, true); err != nil {
		return fmt.Errorf("copying bucket %q in database %q from its owner %s: %w", bucket, database, owner.ID, err)
	}
	return nil
}

// endHeldPendingCopy deletes the needs-a-copy record of a bucket this node
// holds, then takes it out of the queue. The hold is read again under the
// allocator's mutex, which alterOfNoCopy writes the record under.
func (c *Cluster) endHeldPendingCopy(key placementKey, database, bucket string) {
	if err := c.s.deleteNeedsCopy(key, func() bool { return c.s.bucketStoreOpen(database, bucket) }); err != nil {
		c.notePendingCopyFailure(key, err)
	}
}

// notePendingCopyFailure records a failed attempt on the bucket's entry, if
// it is still queued, and backs it off: to the ceiling for this node's disk,
// otherwise one second doubling to the ceiling. A disk refusal is logged at
// most once an hour per bucket, with the remedy the refusal carries.
func (c *Cluster) notePendingCopyFailure(key placementKey, err error) {
	now := time.Now()
	diskFull := errors.Is(err, ErrMemberDiskFull)
	c.pendingCopies.mu.Lock()
	entry, queued := c.pendingCopies.entries[key]
	if !queued {
		c.pendingCopies.mu.Unlock()
		return
	}
	entry.lastAttempt, entry.reason = now, pendingCopyReason(err)
	switch {
	case diskFull:
		entry.wait = pendingCopyRetryCeiling.get()
	case entry.wait == 0:
		entry.wait = pendingCopyRetryFloor.get()
	default:
		entry.wait = min(2*entry.wait, pendingCopyRetryCeiling.get())
	}
	entry.nextRetry = now.Add(entry.wait)
	warn := diskFull && (entry.warnedAt.IsZero() || now.Sub(entry.warnedAt) >= pendingCopyWarnInterval)
	if warn {
		entry.warnedAt = now
	}
	database, bucket, wait := entry.database, entry.bucket, entry.wait
	c.pendingCopies.mu.Unlock()
	if warn {
		c.logger.Warn("a pending copy of a bucket waits: this node's disk is above the high-water mark, so the copy is tried again later "+
			"and this is said once an hour for the bucket",
			"node_id", c.self.ID, "database", database, "bucket", bucket, "next_retry_in", wait, "error", err)
		return
	}
	c.logger.Debug("a pending copy of a bucket did not land; it is tried again",
		"node_id", c.self.ID, "database", database, "bucket", bucket, "next_retry_in", wait, "error", err)
}

// pendingCopyReason is a failed attempt's reason in the words of spec §7.5's
// table.
func pendingCopyReason(err error) string {
	switch {
	case errors.Is(err, ErrMemberDiskFull):
		return "refused for this node's disk: " + err.Error()
	case errors.Is(err, errNotPlacedUnconfirmed):
		return "not placed by the source, and its owner does not confirm it: " + err.Error()
	case errors.Is(err, ErrMigrateBucketNotFound):
		return "not found: the owner has no copy yet: " + err.Error()
	}
	return err.Error()
}

// deleteNeedsCopy deletes this node's needs-a-copy record of the bucket placed
// by key, when still says it should go, under the allocator's mutex every
// write and delete of the record takes, then takes the bucket out of the
// worker's queue. Not synced: a record a power loss brings back is ended
// again by the worker's next attempt.
func (s *Server) deleteNeedsCopy(key placementKey, still func() bool) error {
	s.bucketIDs.mu.Lock()
	defer s.bucketIDs.mu.Unlock()
	if !still() {
		return nil
	}
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(needsCopyKey(key)); err != nil {
		return fmt.Errorf("deleting a needs-a-copy record: %w", err)
	}
	if err := batch.Commit(false); err != nil {
		return fmt.Errorf("committing the delete of a needs-a-copy record: %w", err)
	}
	s.cluster.forgetPendingCopy(key)
	return nil
}
