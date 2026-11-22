package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// The reclaimer (shared-storage spec §5.2.3–§5.2.6, build doc Step 15): one
// goroutine per node deletes the key ranges of dropped buckets in the
// background. A drop hides a bucket in one synced batch and leaves a reclaim
// record at [0xFFFF][0][0x08][id]; the reclaimer pages those records from the
// store — it keeps one record cursor, so its memory is O(1) in the number of
// pending records — and gives each record one bounded batch per lap, round
// robin, so one huge bucket cannot starve ten thousand small ones.
//
// Each batch deletes keys with the store's plain Delete and never through an
// engine's verbs (spec §5.2.5): it holds only the store, and store's
// ReclaimVerbHook counts every verb so a test can prove it. It writes no WAL
// entry, takes no catalog lock and no fence: the id is unreachable, which is
// the point.

const (
	// defaultReclaimKeysPerSecond is the budget when storage.
	// reclaim_keys_per_second is zero (spec §11 Q19, decided): visits plus
	// unlinks per second, node-wide. [unmeasured] — a starting point chosen so
	// 10⁴ small drops drain in minutes.
	defaultReclaimKeysPerSecond = 10_000
	// reclaimStepVisits bounds one batch in visits: every stored version the
	// walk passes, delete markers included, so a run of markers left before
	// the drop is walked in bounded steps rather than inside one (N-08). A
	// key deleted is a key visited, so it bounds the batch's deletes too.
	reclaimStepVisits = 1_000
	// reclaimStepBytes bounds one batch's deleted keys in bytes, below half
	// the store's atomic batch bound as well (reclaimer.byteBound).
	reclaimStepBytes = 1 << 20
	// reclaimRetryDelay is how long the loop waits after a failed lap.
	reclaimRetryDelay = time.Second
	// reclaimWarnAge and reclaimWarnInterval are Q19's trigger: past a day,
	// warn once an hour.
	reclaimWarnAge      = 24 * time.Hour
	reclaimWarnInterval = time.Hour
	// bucketIDLen is a bucket id's width in a key.
	bucketIDLen = 8
)

// reclaimRecordKeyLen is a bucket reclaim record's key length: the node-record
// prefix [0xFFFF][0][0x08] and an 8-byte id — 2 + 8 + 1 + 8 = 19 under the
// slot (spec §3.1.1), where rev 4 said 17. A filter left at 17 matches no
// record: nothing is ever reclaimed, with no error and no log line (build doc
// §1 item 6). The length alone does not exclude the orphan pass's cursor,
// whose key is 19 bytes too; the lap's exclusive upper bound does.
var reclaimRecordKeyLen = len(storage.NodeKey(storage.NSReclaim)) + bucketIDLen

// reclaimRecordsRange bounds the bucket reclaim records, half-open: from the
// namespace's start to the orphan cursor, exclusive, so neither the cursor nor
// the database-drop records above it are ever read as a bucket.
func reclaimRecordsRange() (lower, upper []byte) {
	return storage.NodeKey(storage.NSReclaim), reclaimKey(storage.OrphanCursorID)
}

// reclaimStep is what one step of the range reclaim is about to commit. Tests
// read it through reclaimStepHook.
type reclaimStep struct {
	ID   uint64
	Slot uint16
	// From is where the step seeked: the record's cursor, or the start of the
	// range on a record's first step.
	From []byte
	// First is the first key the step visited, nil when it visited none.
	First   []byte
	Visits  int64
	Deletes int
	Bytes   int
	Files   int
	// Done is a step that found nothing at or after the cursor: it deletes the
	// record, and the bucket is gone.
	Done bool
}

// reclaimStepHook, when set, runs once a step has collected its batch and
// before it unlinks or commits anything; an error ends the lap there, as a
// crash would. Nil in production.
var reclaimStepHook atomic.Pointer[func(reclaimStep) error]

// afterUnlinkHook, when set, runs after each payload file a step unlinks and
// before the step's batch commits — the window T-26(c) cuts the power in; an
// error ends the lap there. Nil in production.
var afterUnlinkHook atomic.Pointer[func(path string) error]

// expandBucketHook, when set, runs before the expander converts each catalog
// entry of a dropped database into a reclaim record; an error ends the lap
// there, as a crash would. Nil in production.
var expandBucketHook atomic.Pointer[func(database, bucket string) error]

// expandWriteBackHook, when set, runs after the expander has converted a page
// of a database drop record's entries and before it writes the record's
// progress back. Tests hold an expansion there while a second drop of the
// database merges into the record. Nil in production.
var expandWriteBackHook atomic.Pointer[func(database string)]

// reclaimLapHook, when set, runs at the top of every lap. A test blocks in it
// to hold the reclaimer while it sets up. Nil in production.
var reclaimLapHook atomic.Pointer[func()]

// reclaimClockHook, when set, is the clock a reclaimer created afterwards
// charges its budget against, so a test can run a budget of 10⁴ visits a
// second over 10⁶ visits without waiting 100 s. Nil in production.
var reclaimClockHook atomic.Pointer[reclaimClock]

// reclaimClock is the time the budget is measured in.
type reclaimClock interface {
	Now() time.Time
	Sleep(ctx context.Context, d time.Duration) error
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

func (wallClock) Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// reclaimBudget is the node-wide token bucket, charged after the work: a
// charge spends its cost, and when the bucket is overdrawn it waits until the
// refill at perSecond has paid the overdraft back. The bucket holds at most
// reclaimStepVisits of credit, so over any window of W seconds the work
// charged is at most perSecond·W plus that credit plus the one charge made
// last. The credit is what keeps the rate honest: a sleep that overshoots —
// every timer does, by about a millisecond — is paid back as credit, where a
// bucket that forgot it lost the overshoot on every one of tens of thousands
// of small charges and ran at a third of its rate (T-05's drain).
type reclaimBudget struct {
	clock     reclaimClock
	perSecond float64
	// tokens is the credit left: at most reclaimStepVisits, negative when
	// overdrawn.
	tokens  float64
	settled time.Time
}

func (b *reclaimBudget) charge(ctx context.Context, cost int64) error {
	b.refill()
	b.tokens -= float64(cost)
	if b.tokens >= 0 {
		return nil
	}
	wait := time.Duration(-b.tokens / b.perSecond * float64(time.Second))
	if err := b.clock.Sleep(ctx, wait); err != nil {
		return fmt.Errorf("waiting for the reclaim budget: %w", err)
	}
	b.refill()
	return nil
}

func (b *reclaimBudget) refill() {
	now := b.clock.Now()
	if !b.settled.IsZero() {
		b.tokens = min(reclaimStepVisits, b.tokens+b.perSecond*now.Sub(b.settled).Seconds())
	}
	b.settled = now
}

// reclaimer is the node's one reclaim loop. Only its own goroutine touches it.
type reclaimer struct {
	s      *Server
	clock  reclaimClock
	budget *reclaimBudget
	// byteBound is min(1 MiB, half the store's atomic batch bound), so a batch
	// of deletes and its record update always commits in one piece.
	byteBound  int
	lastWarned time.Time
	// laps counts laps, so one in orphanLapRatio runs an orphan tick; the
	// first lap after start runs one, which is the start-time pass.
	laps int
	// orphanDue asks the next lap for an orphan tick whatever the count: set
	// when the loop wakes from idle, so the tree is still swept while no drop
	// is pending.
	orphanDue bool
}

func newReclaimer(s *Server) *reclaimer {
	clock := reclaimClock(wallClock{})
	if hooked := reclaimClockHook.Load(); hooked != nil {
		clock = *hooked
	}
	perSecond := s.config.Storage.ReclaimKeysPerSecond
	if perSecond <= 0 {
		perSecond = defaultReclaimKeysPerSecond
	}
	return &reclaimer{
		s:         s,
		clock:     clock,
		budget:    &reclaimBudget{clock: clock, perSecond: float64(perSecond)},
		byteBound: int(min(reclaimStepBytes, s.kv.MaxBatchBytes()/2)),
	}
}

// startReclaimer starts the node's one reclaim goroutine. It stops when the
// server's context is cancelled, and Stop waits for it before closing the
// store.
func (s *Server) startReclaimer() {
	r := newReclaimer(s)
	s.reclaimWG.Add(1)
	go func() {
		defer s.reclaimWG.Done()
		r.run(s.ctx)
	}()
}

// wakeReclaimer tells an idle reclaimer that a drop left work. The records are
// in the store, so the signal carries nothing and coalesces: one pending wake
// is as good as a thousand.
func (s *Server) wakeReclaimer() {
	select {
	case s.reclaimWake <- struct{}{}:
	default:
	}
}

// unlinkWindow keeps the reclaimer off a drop between its commit and its
// unlink (BUG-0527). The record is in the store from the commit, because a
// crash after the hide must still reclaim the range, so the reclaimer can read
// it while the name is still linked and a reader that resolved the name, and
// holds db.mu for reading, has yet to take its snapshot. Spec §5.2.2's reader
// rule rests on the reclaimer not receiving the id until the unlink, so a drop
// holds its bucket id, and a database drop its database, from before its
// commit until the name is unlinked (windowHold), and a lap skips any record
// either still names. The drop's wake after the unlink brings the next lap.
//
// It holds only the drops in flight, never per-bucket state, and the reclaimer
// reads it under its own mutex, never db.mu, so a lap does not queue behind
// the reader that holds the drop's unlink back. It is empty after a restart,
// and needs nothing then: a committed drop deleted the catalog entry in the
// same batch as its record, and LoadCatalog skips the entries a pending
// database drop covers, so no name with a record is linked.
type unlinkWindow struct {
	mu sync.Mutex
	// ids counts the bucket drops in the window by bucket id. Not named
	// buckets: TestNoCodeReachesIntoTheBucketMap reads any .buckets selector
	// as a reach into the registry's bucket map.
	ids       map[uint64]int
	databases map[string]int
}

// windowHold is one drop's entry in the unlink window. The drop takes it
// before its commit and defers release, so every way out of the drop —
// a refusal, a failed commit, a panic — leaves the window, except one: a
// drop whose record committed and whose names are not yet unlinked keeps its
// entry, because leaving there would hand the reclaimer a name a reader can
// still resolve. That entry stays until the node restarts, and the restart
// reclaims the range with an empty window. Only the drop's own goroutine
// touches a hold.
type windowHold struct {
	w        *unlinkWindow
	id       uint64 // the bucket a bucket drop holds; 0 for a database drop
	database string // the database a database drop holds; "" for a bucket drop
	recorded bool   // the drop's record is committed
	gone     bool   // the drop's names are unlinked, or there were none
	left     bool
}

// holdBucket enters a drop of bucket id into the window.
func (w *unlinkWindow) holdBucket(id uint64) *windowHold {
	w.adjust(id, "", 1)
	return &windowHold{w: w, id: id}
}

// holdDatabase enters a drop of database into the window.
func (w *unlinkWindow) holdDatabase(database string) *windowHold {
	w.adjust(0, database, 1)
	return &windowHold{w: w, database: database}
}

func (w *unlinkWindow) adjust(id uint64, database string, delta int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if database != "" {
		w.databases = countIn(w.databases, database, delta)
		return
	}
	w.ids = countIn(w.ids, id, delta)
}

// recordCommitted marks the drop's record durable.
func (h *windowHold) recordCommitted() { h.recorded = true }

// namesGone marks the drop's names unlinked — or found none to unlink — and
// leaves the window, before the drop wakes the reclaimer.
func (h *windowHold) namesGone() {
	h.gone = true
	h.release()
}

// release leaves the window once, unless the record committed and the names
// are not yet gone.
func (h *windowHold) release() {
	if h.left || (h.recorded && !h.gone) {
		return
	}
	h.left = true
	h.w.adjust(h.id, h.database, -1)
}

// holds reports whether a drop of the record's bucket, or of its database, is
// between its commit and its unlink.
func (w *unlinkWindow) holds(record reclaimRecord) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ids[uint64(record.ID)] > 0 || (record.Database != "" && w.databases[record.Database] > 0)
}

// countIn adds delta to counts[key], deleting the key at zero so the map
// holds only drops in flight. Two drops of one bucket or database can overlap
// (a release and a drop entry, two drops of one database), hence a count.
func countIn[K comparable](counts map[K]int, key K, delta int) map[K]int {
	if counts == nil {
		counts = make(map[K]int)
	}
	if counts[key] += delta; counts[key] <= 0 {
		delete(counts, key)
	}
	return counts
}

// run laps until the context ends. While no record is pending it sleeps until
// a drop wakes it, or orphanIdleInterval passes and the next lap owes the
// orphan pass a tick.
func (r *reclaimer) run(ctx context.Context) {
	idle := time.NewTimer(orphanIdleInterval)
	defer idle.Stop()
	for {
		pending, err := r.lap(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			r.s.logger.Error("reclaiming the space of dropped buckets failed; retrying",
				"error", err, "retry_in", reclaimRetryDelay)
			if (wallClock{}).Sleep(ctx, reclaimRetryDelay) != nil {
				return
			}
			continue
		}
		if pending > 0 {
			continue
		}
		idle.Reset(orphanIdleInterval)
		select {
		case <-ctx.Done():
			return
		case <-r.s.reclaimWake:
		case <-idle.C:
			r.orphanDue = true
		}
	}
}

// reclaimTally is what a lap saw of the records it leaves pending.
type reclaimTally struct {
	pending int
	oldest  time.Time
}

func (t *reclaimTally) note(droppedAt time.Time) {
	if t.pending == 0 || droppedAt.Before(t.oldest) {
		t.oldest = droppedAt
	}
	t.pending++
}

// lap expands the pending database drops, converts the records of failed
// materialises into reclaim records, then gives every bucket reclaim record one
// step, in key order, and returns how many drop and reclaim records are still
// pending after it.
func (r *reclaimer) lap(ctx context.Context) (int, error) {
	if hook := reclaimLapHook.Load(); hook != nil {
		(*hook)()
	}
	var tally reclaimTally
	if err := r.expandDatabaseDrops(ctx, &tally); err != nil {
		return 0, err
	}
	if err := r.adoptFailedMaterializations(ctx); err != nil {
		return 0, err
	}
	after, upper := reclaimRecordsRange()
	for {
		key, record, found, err := nextNodeRecord[reclaimRecord](ctx, r, after, upper, reclaimRecordKeyLen)
		if err != nil {
			return 0, err
		}
		if !found {
			break
		}
		// Read after the record, the window cannot miss: a drop enters it
		// before committing the record, so a record this lap can see is either
		// still entered or its name is unlinked. Not counted pending, so the
		// loop does not spin on it; the drop wakes it once the name is gone.
		if r.s.unlinkWindow.holds(record) {
			after = append(key, 0)
			continue
		}
		done, err := r.step(ctx, key, record)
		if err != nil {
			return 0, err
		}
		if !done {
			tally.note(record.DroppedAt)
		}
		after = append(key, 0)
	}
	r.publish(tally)
	if err := r.orphanTickIfDue(ctx); err != nil {
		return 0, err
	}
	return tally.pending, nil
}

// nextNodeRecord reads the first record in [after, upper) — of keyLen bytes
// when keyLen is not 0 — from a snapshot of its own, and charges the walk to
// the budget once the snapshot is released.
func nextNodeRecord[T any](ctx context.Context, r *reclaimer, after, upper []byte, keyLen int) ([]byte, T, bool, error) {
	key, record, visits, err := readNextNodeRecord[T](r.s.kv, after, upper, keyLen)
	if err != nil {
		return nil, record, false, err
	}
	if err := r.budget.charge(ctx, visits); err != nil {
		return nil, record, false, err
	}
	return key, record, key != nil, nil
}

func readNextNodeRecord[T any](kv storage.KV, after, upper []byte, keyLen int) ([]byte, T, int64, error) {
	var record T
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: after, Upper: upper})
	defer it.Close()
	for it.Seek(nil); it.Valid(); it.Next() {
		if keyLen != 0 && len(it.Key()) != keyLen {
			continue
		}
		encoded, err := it.Value()
		if err != nil {
			return nil, record, it.Visits(), fmt.Errorf("reading the node record at %x: %w", it.Key(), err)
		}
		if err := bson.Unmarshal(encoded, &record); err != nil {
			return nil, record, it.Visits(), fmt.Errorf("decoding the node record at %x: %w", it.Key(), err)
		}
		return bytes.Clone(it.Key()), record, it.Visits(), nil
	}
	if err := it.Err(); err != nil {
		return nil, record, it.Visits(), fmt.Errorf("walking the node records: %w", err)
	}
	return nil, record, it.Visits(), nil
}

// expandDatabaseDrops is the top of every lap (spec §5.2.3's record table):
// database-drop records sort after every bucket record, so the lap seeks to
// them explicitly and expands them before any range work. Each gets one
// bounded page of its catalog entries per lap, so a dropped database of 10⁵
// buckets does not hold the range reclaim off for its whole expansion.
func (r *reclaimer) expandDatabaseDrops(ctx context.Context, tally *reclaimTally) error {
	after, upper := databaseDropRange()
	for {
		key, record, found, err := nextNodeRecord[databaseDropRecord](ctx, r, after, upper, 0)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		done, err := r.expand(ctx, key, record)
		if err != nil {
			return err
		}
		if !done {
			tally.note(record.DroppedAt)
		}
		after = append(key, 0)
	}
}

// adoptFailedMaterializations is the reclaimer's orphaned-range source
// : every materialise record whose materialise is not running is
// converted into a reclaim record, and the lap's range pass then reclaims the
// id's keys and payload files as it reclaims a drop's — unlink first, then the
// unsynced batch, on the same budget. It reads only materialise records, which
// exist only for ids reserved and not yet catalogued, so on a node with no
// migration failing or in flight it is one seek over an empty range. A record
// whose materialise is running is skipped and not counted as pending, so a
// long pull does not keep the loop spinning.
func (r *reclaimer) adoptFailedMaterializations(ctx context.Context) error {
	after, upper := materializeRecordsRange()
	for {
		key, record, found, err := nextNodeRecord[materializeRecord](ctx, r, after, upper, 0)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		converted, err := r.s.reclaimFailedMaterialize(uint64(record.ID))
		if err != nil {
			return err
		}
		if converted {
			r.s.logger.Warn(fmt.Sprintf("A migration of bucket %q in database %q stopped before the bucket was catalogued on this node; "+
				"its partial copy (bucket id %d) is being reclaimed in the background. The bucket is pulled again by the next rebalance, "+
				"or run 'migrate %s %s' on this node.", record.Name, record.Database, record.ID, record.Database, record.Name),
				"database", record.Database, "bucket", record.Name, "bucket_id", record.ID)
			if err := r.budget.charge(ctx, 1); err != nil {
				return err
			}
		}
		after = append(key, 0)
	}
}

// droppedEntries is one page of a dropped database's catalog entries.
type droppedEntries struct {
	names  []string
	last   []byte
	visits int64
}

// expand converts one page of a dropped database's catalog entries into
// bucket reclaim records, and deletes the database's record once no entry at
// or after its cursor is left. It reports whether the record is gone.
func (r *reclaimer) expand(ctx context.Context, recordKey []byte, record databaseDropRecord) (bool, error) {
	page, err := r.pageDroppedEntries(record)
	if err != nil {
		return false, err
	}
	if page.last == nil {
		done, err := r.s.advanceDropRecord(recordKey, record, nil)
		if err != nil || !done {
			return false, err
		}
		return true, r.budget.charge(ctx, page.visits)
	}
	for _, name := range page.names {
		if err := r.convertEntry(record, name); err != nil {
			return false, err
		}
	}
	if hook := expandWriteBackHook.Load(); hook != nil {
		(*hook)(record.Database)
	}
	if _, err := r.s.advanceDropRecord(recordKey, record, append(page.last, 0)); err != nil {
		return false, err
	}
	return false, r.budget.charge(ctx, page.visits+int64(len(page.names)))
}

// advanceDropRecord moves a database drop record's expansion to cursor, or
// deletes the record when cursor is nil, unless a drop of the same database
// merged into the record since the expander read it: the record then names
// generations this page did not look for, and it is left as the merge wrote it
// — its cursor back at the start — for the next lap. Merges are made under the
// allocator's mutex, so the check and the write are made under it too.
// It reports whether the record is gone.
func (s *Server) advanceDropRecord(recordKey []byte, read databaseDropRecord, cursor []byte) (bool, error) {
	s.bucketIDs.mu.Lock()
	defer s.bucketIDs.mu.Unlock()
	current, pending, err := s.pendingDatabaseDrop(read.Database)
	if err != nil {
		return false, err
	}
	if !pending || current.MaxID != read.MaxID || !slices.Equal(current.Generations, read.Generations) || !bytes.Equal(current.Cursor, read.Cursor) {
		return false, nil
	}
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if cursor == nil {
		if err := batch.Delete(recordKey); err != nil {
			return false, fmt.Errorf("deleting the drop record of database %q: %w", read.Database, err)
		}
	} else {
		read.Cursor = cursor
		encoded, err := bson.Marshal(read)
		if err != nil {
			return false, fmt.Errorf("encoding the drop record of database %q: %w", read.Database, err)
		}
		if err := batch.Set(recordKey, encoded); err != nil {
			return false, fmt.Errorf("advancing the drop record of database %q: %w", read.Database, err)
		}
	}
	if err := batch.Commit(false); err != nil {
		return false, fmt.Errorf("committing the expansion progress of database %q: %w", read.Database, err)
	}
	return cursor == nil, nil
}

// pageDroppedEntries walks the database's catalog keys from the record's
// cursor, up to the visit bound, collecting the buckets whose entries are the
// dropped database's — id at or below max_id. An entry above it belongs to a
// database re-created under the name and is skipped (spec §5.2.2, Q21); so is
// the re-created database's marker.
func (r *reclaimer) pageDroppedEntries(record databaseDropRecord) (droppedEntries, error) {
	lower, upper := databaseCatalogRange(record.Database)
	from := lower
	if len(record.Cursor) > 0 {
		from = record.Cursor
	}
	var page droppedEntries
	snap := r.s.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: upper, IncludeDeleted: true})
	defer it.Close()
	for it.Seek(from); it.Valid(); it.Next() {
		page.last = bytes.Clone(it.Key())
		name := string(it.Key()[len(lower):])
		encoded, err := it.Value()
		switch {
		case errors.Is(err, storage.ErrKeyNotFound) || name == "":
		case err != nil:
			return page, fmt.Errorf("reading the catalog entry of bucket %q in database %q: %w", name, record.Database, err)
		default:
			var entry catalogEntry
			if err := bson.Unmarshal(encoded, &entry); err != nil {
				return page, fmt.Errorf("decoding the catalog entry of bucket %q in database %q: %w", name, record.Database, err)
			}
			entry.database = record.Database
			if record.covers(entry) {
				page.names = append(page.names, name)
			}
		}
		if it.Visits() >= reclaimStepVisits {
			break
		}
	}
	// A page cut short by an error is not the end of the database's entries,
	// and must not be taken for it.
	if err := it.Err(); err != nil {
		return page, fmt.Errorf("walking the catalog entries of database %q: %w", record.Database, err)
	}
	page.visits = it.Visits()
	return page, nil
}

// convertEntry turns one catalog entry of a dropped database into its
// bucket's reclaim record, in one batch, under that bucket's fence held
// exclusively: a write that resolved the bucket before the database vanished
// has finished before its range can be reclaimed — the single drop's
// guarantee, paid one bucket at a time (spec §5.2.2). The entry is read again
// under the fence, because a create may have converted it first. The slot is
// the entry's: the expander has no in-memory bucket, and the entry is the only
// place the slot is recorded.
func (r *reclaimer) convertEntry(record databaseDropRecord, bucket string) error {
	if hook := expandBucketHook.Load(); hook != nil {
		if err := (*hook)(record.Database, bucket); err != nil {
			return err
		}
	}
	hold := r.s.acquireFence(placementOf(record.Database, bucket), fenceExclusive)
	defer hold.release()
	entry, catalogued, err := r.s.readCatalogEntry(record.Database, bucket)
	if err != nil {
		return err
	}
	if !catalogued || !record.covers(entry) {
		return nil
	}
	encoded, err := bson.Marshal(reclaimRecord{
		ID: entry.ID, Slot: entry.Slot, Type: entry.Type, Database: record.Database, Name: bucket,
		DroppedSeq: record.DroppedSeq, DroppedAt: record.DroppedAt,
	})
	if err != nil {
		return fmt.Errorf("encoding the reclaim record of bucket %d: %w", entry.ID, err)
	}
	batch := r.s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(catalogKey(record.Database, bucket)); err != nil {
		return fmt.Errorf("deleting the catalog entry of bucket %q of dropped database %q: %w", bucket, record.Database, err)
	}
	if err := batch.Set(reclaimKey(uint64(entry.ID)), encoded); err != nil {
		return fmt.Errorf("writing the reclaim record of bucket %d: %w", entry.ID, err)
	}
	if err := batch.Commit(false); err != nil {
		return fmt.Errorf("committing the expansion of bucket %q of dropped database %q: %w", bucket, record.Database, err)
	}
	return nil
}

// reclaimBatch is one step's collected work.
type reclaimBatch struct {
	from, first, last []byte
	deletes           [][]byte
	files             []string
	bytes             int
	visits            int64
}

// step is one batch of one record (spec §5.2.3 steps 2–7): collect from the
// record's cursor, unlink the payload files the collected metadata names,
// then commit the deletes and the record's progress together, unsynced. It
// reports whether the record is gone.
func (r *reclaimer) step(ctx context.Context, recordKey []byte, record reclaimRecord) (bool, error) {
	slot, id := uint16(record.Slot), uint64(record.ID)
	collected, err := r.collect(slot, record.rangeDatabaseID(), id, record.Cursor)
	if err != nil {
		return false, fmt.Errorf("collecting a reclaim batch of dropped bucket %d: %w", id, err)
	}
	done := collected.last == nil
	if hook := reclaimStepHook.Load(); hook != nil {
		if err := (*hook)(reclaimStep{
			ID: id, Slot: slot, From: collected.from, First: collected.first, Visits: collected.visits,
			Deletes: len(collected.deletes), Bytes: collected.bytes, Files: len(collected.files), Done: done,
		}); err != nil {
			return false, err
		}
	}
	if done {
		if err := r.commitRecordDone(recordKey, fmt.Sprintf("bucket %d", id)); err != nil {
			return false, err
		}
		reclaimKeysVisited.Add(float64(collected.visits))
		return true, r.budget.charge(ctx, collected.visits)
	}

	// Files first: a crash after an unlink and before the commit leaves
	// metadata naming a missing file in an unreachable bucket, which the resume
	// unlinks again (ENOENT) and deletes; the other order could leave a file no
	// metadata names (spec §5.2.4).
	for _, rel := range collected.files {
		if err := r.unlinkPayload(slot, id, rel); err != nil {
			return false, err
		}
	}
	record.Cursor = append(bytes.Clone(collected.last), 0)
	record.KeysDeleted += int64(len(collected.deletes))
	record.FilesUnlinked += int64(len(collected.files))
	if err := r.commitProgress(recordKey, record, collected.deletes); err != nil {
		return false, err
	}
	reclaimKeysDeleted.Add(float64(len(collected.deletes)))
	reclaimKeysVisited.Add(float64(collected.visits))
	reclaimFilesUnlinked.Add(float64(len(collected.files)))
	return false, r.budget.charge(ctx, collected.visits+int64(len(collected.files)))
}

// collect walks the record's range from its cursor — never from the range's
// start, which would walk every marker earlier steps wrote (the tombstone wall)
// — until the visit bound or the byte bound, collecting the keys to delete.
// The slot is the record's: never storage.SlotOf over the names the record
// also carries (build doc §1 item 6).
func (r *reclaimer) collect(slot uint16, databaseID, id uint64, cursor []byte) (reclaimBatch, error) {
	lower, upper := storage.Scope(slot, databaseID, id), storage.Scope(slot, databaseID, id+1)
	batch := reclaimBatch{from: lower}
	if len(cursor) > 0 {
		batch.from = cursor
	}
	snap := r.s.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: upper, IncludeDeleted: true})
	defer it.Close()
	for it.Seek(batch.from); it.Valid(); it.Next() {
		stored, err := it.Value()
		live := err == nil
		if err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
			return batch, fmt.Errorf("reading key %x: %w", it.Key(), err)
		}
		if live && batch.last != nil && batch.bytes+len(it.Key()) > r.byteBound {
			break
		}
		key := bytes.Clone(it.Key())
		if batch.first == nil {
			batch.first = key
		}
		batch.last = key
		if live {
			batch.deletes = append(batch.deletes, key)
			batch.bytes += len(key)
			if key[len(lower)] == storage.NSBlobMeta {
				rel, err := store.PayloadPathOf(stored)
				if err != nil {
					return batch, fmt.Errorf("reading the blob metadata at %x: %w", key, err)
				}
				if rel != "" {
					batch.files = append(batch.files, rel)
				}
			}
		}
		if it.Visits() >= reclaimStepVisits {
			break
		}
	}
	// A batch cut short by an error is not the end of the range, and must
	// not be taken for it: the range would be recorded reclaimed.
	if err := it.Err(); err != nil {
		return batch, fmt.Errorf("walking the range: %w", err)
	}
	batch.visits = it.Visits()
	return batch, nil
}

// unlinkPayload removes one payload file of a dropped blob bucket; a file
// already gone is success, because a resumed step unlinks again what the
// crashed one had. A name that does not parse as this bucket's is left alone:
// the reclaimer deletes files it did not write in this process, and it must
// never delete one it cannot prove is the dropped bucket's.
func (r *reclaimer) unlinkPayload(slot uint16, id uint64, rel string) error {
	parsed, ok := store.ParsePayloadFileName(filepath.Base(rel))
	if !ok || parsed.Temporary || parsed.Slot != slot || parsed.BucketID != id {
		r.s.logger.Error("a dropped blob bucket's metadata names a file that is not its own; leaving the file in place",
			"bucket_id", id, "slot", slot, "file", rel)
		return nil
	}
	path := filepath.Join(r.s.blobRoot(), rel)
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("unlinking the payload file %s of dropped bucket %d: %w", path, id, err)
	}
	if hook := afterUnlinkHook.Load(); hook != nil {
		if err := (*hook)(path); err != nil {
			return err
		}
	}
	return nil
}

// commitProgress commits a step's deletes and the record's advanced cursor and
// counters in one batch, unsynced: a lost batch loses its own progress with it
// and is redone, and deleting an absent key is a no-op.
func (r *reclaimer) commitProgress(recordKey []byte, record reclaimRecord, deletes [][]byte) error {
	encoded, err := bson.Marshal(record)
	if err != nil {
		return fmt.Errorf("encoding the reclaim record of bucket %d: %w", record.ID, err)
	}
	batch := r.s.kv.NewBatch()
	defer batch.Discard()
	for _, key := range deletes {
		if err := batch.Delete(key); err != nil {
			return fmt.Errorf("deleting key %x of dropped bucket %d: %w", key, record.ID, err)
		}
	}
	if err := batch.Set(recordKey, encoded); err != nil {
		return fmt.Errorf("advancing the reclaim record of bucket %d: %w", record.ID, err)
	}
	if err := batch.Commit(false); err != nil {
		return fmt.Errorf("committing a reclaim batch of bucket %d: %w", record.ID, err)
	}
	return nil
}

// commitRecordDone deletes a record whose work is done: a bucket's whose
// range is empty, or a database's whose entries are all converted. what names
// it in an error.
func (r *reclaimer) commitRecordDone(recordKey []byte, what string) error {
	batch := r.s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(recordKey); err != nil {
		return fmt.Errorf("deleting the reclaim record of %s: %w", what, err)
	}
	if err := batch.Commit(false); err != nil {
		return fmt.Errorf("committing the end of the reclaim of %s: %w", what, err)
	}
	return nil
}

// publish sets the two gauges from what the lap left pending, and warns when
// the oldest record is past a day old.
//
// The warning is spec §11 Q19's age trigger only. Its second trigger — the
// backlog past 10% of the store's estimated size — needs the backlog's size,
// a sum over every pending record that nothing maintains incrementally and a
// start-time scan may not compute; it is deferred until a persisted aggregate
// is specified (build doc §5 SA-9). The warning names the maintenance window
// and no command: the escape hatch has none in this build (SA-8).
func (r *reclaimer) publish(tally reclaimTally) {
	reclaimPendingRecords.Set(float64(tally.pending))
	if tally.pending == 0 {
		reclaimOldestPendingSeconds.Set(0)
		return
	}
	now := r.clock.Now()
	age := now.Sub(tally.oldest)
	reclaimOldestPendingSeconds.Set(age.Seconds())
	if age <= reclaimWarnAge || (!r.lastWarned.IsZero() && now.Sub(r.lastWarned) < reclaimWarnInterval) {
		return
	}
	r.lastWarned = now
	r.s.logger.Warn(fmt.Sprintf("Dropped buckets are still waiting to be reclaimed: %d reclaim record(s) are pending "+
		"and the oldest was dropped %s ago, so their space is still on disk. Raise storage.reclaim_keys_per_second "+
		"(HOARDDB_RECLAIM_KEYS_PER_SECOND, default %d) so the background reclaimer deletes faster, or schedule a "+
		"maintenance window in which the node's large dropped buckets are reclaimed at once.",
		tally.pending, age.Round(time.Minute), defaultReclaimKeysPerSecond),
		"pending_records", tally.pending, "oldest_pending_seconds", int64(age.Seconds()))
}

// reclaimTarget is one pending record's range, by the record's own slot and
// id: never an id alone. An 8-byte [id] prefix compared against slotted keys
// matches slot 0's keys whose bucket ids begin with the id's low six bytes —
// live buckets on a node with enough of them (build doc §1 item 6).
type reclaimTarget struct {
	slot       uint16
	databaseID uint64
	id         uint64
}

// prefixDropper is a store that can drop whole key prefixes at once: the
// Badger store, and only on its maintenance-window path.
type prefixDropper interface {
	DropPrefixes(prefixes [][]byte) error
}

// dropPendingPrefixes is spec §5.2.6's escape hatch: one DropPrefix call over
// the 18-byte [slot][database id][id] scope of every target. It blocks every writer on the
// node while it runs, so it is for a scheduled maintenance window, and in this
// build it is reachable from tests and the engine spike only — no opcode, no
// CLI verb, no config key (build doc SA-8). It never deletes a reclaim record:
// DropPrefix can return nil with keys surviving, and the ordinary loop deletes
// each record once it finds its range empty (N-07).
func (s *Server) dropPendingPrefixes(targets []reclaimTarget) error {
	dropper, ok := s.kv.(prefixDropper)
	if !ok {
		return fmt.Errorf("the node store (%T) cannot drop key prefixes; only the Badger store can", s.kv)
	}
	prefixes := make([][]byte, 0, len(targets))
	for _, target := range targets {
		prefixes = append(prefixes, storage.Scope(target.slot, target.databaseID, target.id))
	}
	return dropper.DropPrefixes(prefixes)
}

// pendingReclaimTargets lists every pending bucket reclaim record's slot and
// id, for dropPendingPrefixes. It is O(pending records) in memory, which is why
// only the maintenance window uses it.
func (s *Server) pendingReclaimTargets() ([]reclaimTarget, error) {
	lower, upper := reclaimRecordsRange()
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: upper})
	defer it.Close()
	var targets []reclaimTarget
	for it.Seek(nil); it.Valid(); it.Next() {
		if len(it.Key()) != reclaimRecordKeyLen {
			continue
		}
		encoded, err := it.Value()
		if err != nil {
			return nil, fmt.Errorf("reading the reclaim record at %x: %w", it.Key(), err)
		}
		var record reclaimRecord
		if err := bson.Unmarshal(encoded, &record); err != nil {
			return nil, fmt.Errorf("decoding the reclaim record at %x: %w", it.Key(), err)
		}
		targets = append(targets, reclaimTarget{slot: uint16(record.Slot), databaseID: record.rangeDatabaseID(), id: uint64(record.ID)})
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("walking the reclaim records: %w", err)
	}
	return targets, nil
}

// The orphan pass (spec §5.6, §5.2.4; build doc Step 17): the reclaimer's
// second work source. It finds the payload files whose metadata never
// committed — a put that stopped between its rename and its commit (spec §3.5)
// — which the range reclaim cannot, because an orphan has no key to walk to.
//
// It walks the node-wide blob tree one leaf per tick. A tick reads its leaf's
// directory whole (os.ReadDir returns the names sorted), examines the names
// after the cursor's, up to orphanTickFiles, and records how far it got in
// the cursor record, so a restart resumes inside the leaf by name — directory
// order is not sorted and ReadDir's own position does not survive a restart.
//
// Each name is parsed into (slot, bucket id, blob id) and nothing else is
// consulted: the pass holds no in-memory bucket and keeps no id → slot map, so
// the metadata key it reads is built from the parsed slot alone. Read with
// slot 0 or a computed slot, the point read misses on a live bucket and the
// pass unlinks a live blob's file (build doc §1 item 6, review M-1).

const (
	// orphanLapRatio is spec §5.2.4's ratio: at most one orphan tick in this
	// many laps, so a drop backlog is never starved and the tree is still
	// swept.
	orphanLapRatio = 10
	// orphanTickFiles is the number of names one tick examines. [unmeasured]
	// A leaf holds about 1,500 names at 10⁸ blobs (spec §3.5), so a full leaf
	// takes about fifteen ticks.
	orphanTickFiles = 100
	// orphanGraceAge is spec §5.6 rule 1: a file younger than this is never an
	// orphan, because a live put's rename-to-commit window is inside it. Not a
	// config key for 1.0. [unmeasured]
	orphanGraceAge = time.Hour
	// orphanIdleInterval is how often an idle reclaimer wakes for one orphan
	// tick. At one leaf a second the 65,536 leaves take about 18 hours a
	// sweep; an orphan reclaimed a day late costs nothing (spec §5.6).
	// [unmeasured]
	orphanIdleInterval = time.Second
	// blobLeafCount is the blob tree's leaves, <h[0:2]>/<h[2:4]> (spec §3.5).
	blobLeafCount = 1 << 16
)

// orphanCursor is the orphan pass's position, the value of the point key
// [0xFFFF][0][0x08][OrphanCursorID]: the leaf being walked, as "ab/cd", and
// the last name examined in it. Only the orphan pass reads or writes it; the
// lap's bucket-record range stops short of its key.
type orphanCursor struct {
	Leaf     string `bson:"leaf"`
	LastName string `bson:"last_name"`
}

// orphanReadDirHook, when set, runs each time the orphan pass reads a leaf
// directory, with the pass's blob root and the leaf. Nil in production. The
// root is part of the signature, not just the leaf, because a watcher must
// tell its own pass apart from another Server's in the same process
// — including a restart, which is a new *Server over the same
// data directory and so the same root.
var orphanReadDirHook atomic.Pointer[func(root, leaf string)]

// orphanExamineHook, when set, runs for each name the orphan pass examines,
// with the pass's blob root. Nil in production. See orphanReadDirHook for
// why the root is carried.
var orphanExamineHook atomic.Pointer[func(root, leaf, name string)]

// blobLeafName is leaf index's path under the blob root, with a forward slash
// whatever the platform, as the cursor records it.
func blobLeafName(index int) string {
	return fmt.Sprintf("%02x/%02x", index>>8, index&0xff)
}

// blobLeafIndex is blobLeafName's inverse; the empty leaf, a cursor never
// written, is the first.
func blobLeafIndex(leaf string) (int, error) {
	if leaf == "" {
		return 0, nil
	}
	var high, low int
	if len(leaf) != 5 || leaf[2] != '/' {
		return 0, fmt.Errorf("the orphan cursor names leaf %q, which is not of the form ab/cd", leaf)
	}
	if _, err := fmt.Sscanf(leaf, "%02x/%02x", &high, &low); err != nil {
		return 0, fmt.Errorf("the orphan cursor names leaf %q, which is not of the form ab/cd: %w", leaf, err)
	}
	return high<<8 | low, nil
}

// orphanTickIfDue runs the lap's orphan tick when one is due. A tick that
// fails is logged and does not fail the lap: the range reclaim is the first
// work source, and a leaf the pass cannot read must not stop it.
func (r *reclaimer) orphanTickIfDue(ctx context.Context) error {
	due := r.orphanDue || r.laps%orphanLapRatio == 0
	r.laps++
	if !due {
		return nil
	}
	r.orphanDue = false
	err := r.orphanTick(ctx)
	if err == nil || ctx.Err() != nil {
		return err
	}
	r.s.logger.Error("the orphan pass over the blob tree failed; it retries on a later lap",
		"error", err, "blob_root", r.s.blobRoot())
	return nil
}

// orphanTick is one tick of the orphan pass: one leaf read, at most
// orphanTickFiles names examined after the cursor's, the cursor advanced —
// to the next leaf, from the empty name, once this one is done — and the work
// charged to the reclaimer's budget.
func (r *reclaimer) orphanTick(ctx context.Context) error {
	cursor, err := r.readOrphanCursor()
	if err != nil {
		return err
	}
	leafIndex, err := blobLeafIndex(cursor.Leaf)
	if err != nil {
		return err
	}
	leaf := blobLeafName(leafIndex)
	names, err := r.readBlobLeaf(leaf)
	if err != nil {
		return err
	}
	start := sort.SearchStrings(names, cursor.LastName)
	if start < len(names) && names[start] == cursor.LastName {
		start++
	}
	end := min(start+orphanTickFiles, len(names))
	unlinked := 0
	for _, name := range names[start:end] {
		if hook := orphanExamineHook.Load(); hook != nil {
			(*hook)(r.s.blobRoot(), leaf, name)
		}
		removed, err := r.collectOrphan(leaf, name)
		if err != nil {
			return err
		}
		if removed {
			unlinked++
		}
	}
	next := orphanCursor{Leaf: leaf, LastName: cursor.LastName}
	if end > start {
		next.LastName = names[end-1]
	}
	if end == len(names) {
		next = orphanCursor{Leaf: blobLeafName((leafIndex + 1) % blobLeafCount)}
	}
	if err := r.writeOrphanCursor(next); err != nil {
		return err
	}
	reclaimFilesUnlinked.Add(float64(unlinked))
	// The directory read is charged as one visit so a tick over an empty
	// leaf is not free.
	return r.budget.charge(ctx, int64(1+end-start+unlinked))
}

// readBlobLeaf returns the names in one leaf directory, sorted; a leaf no
// payload was ever written to does not exist and holds none.
func (r *reclaimer) readBlobLeaf(leaf string) ([]string, error) {
	if hook := orphanReadDirHook.Load(); hook != nil {
		(*hook)(r.s.blobRoot(), leaf)
	}
	dir := filepath.Join(r.s.blobRoot(), filepath.FromSlash(leaf))
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading the blob leaf %s: %w", dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.Type().IsRegular() {
			names = append(names, entry.Name())
		}
	}
	return names, nil
}

// collectOrphan examines one name and unlinks it if it is an orphan: a name
// that parses, whose file is older than the grace age, and which is either a
// temporary — metadata never names one, so no read is made (SA-25) — or a
// final name whose metadata key, built from the parsed slot, bucket id and
// blob id, holds nothing. A name that does not parse is never unlinked: a
// foreign file is not an orphan. A bucket with a reclaim record is the range
// reclaim's, and its files are left to it.
func (r *reclaimer) collectOrphan(leaf, name string) (bool, error) {
	parsed, ok := store.ParsePayloadFileName(name)
	if !ok {
		return false, nil
	}
	path := filepath.Join(r.s.blobRoot(), filepath.FromSlash(leaf), name)
	stat, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reading the age of blob payload %s: %w", path, err)
	}
	if time.Since(stat.ModTime()) < orphanGraceAge {
		return false, nil
	}
	if !parsed.Temporary {
		owned, err := r.payloadIsOwned(parsed)
		if err != nil || owned {
			return false, err
		}
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return false, fmt.Errorf("unlinking the orphan blob payload %s: %w", path, err)
	}
	return true, nil
}

// payloadIsOwned reports whether a parsed payload name is accounted for: its
// bucket has a reclaim record, or its metadata key exists. The key is built
// from the name's own slot — the one right source the pass has (build doc §1
// item 6).
func (r *reclaimer) payloadIsOwned(parsed store.PayloadName) (bool, error) {
	snap := r.s.kv.NewSnapshot()
	defer snap.Discard()
	keys := [][]byte{reclaimKey(parsed.BucketID)}
	// A payload file name carries the slot and bucket id but no database, and
	// the metadata key leads with the database id, so the database comes from
	// the bucket's catalog entry. A bucket with no entry has no metadata key
	// to find, which is what an orphan is.
	database, catalogued, err := databaseOfBucketID(snap, parsed.BucketID)
	if err != nil {
		return false, fmt.Errorf("finding the database of bucket %d for blob %s: %w", parsed.BucketID, parsed.BlobID, err)
	}
	if catalogued {
		keys = append(keys, storage.Key(parsed.Slot, storage.LegacyDatabaseID(database), parsed.BucketID, storage.NSBlobMeta, []byte(parsed.BlobID)))
	}
	for _, key := range keys {
		_, err := snap.Get(key)
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, storage.ErrKeyNotFound) {
			return false, fmt.Errorf("reading %x for blob %s of bucket %d: %w", key, parsed.BlobID, parsed.BucketID, err)
		}
	}
	return false, nil
}

// databaseOfBucketID is the database whose catalog entry holds bucket id. It
// walks the catalog, O(buckets), and runs only for a payload file older than
// the orphan grace age that is not a temporary. Removed with the legacy
// database id (storage.LegacyDatabaseID), when a payload file name carries a
// database id of its own.
func databaseOfBucketID(snap storage.KVSnapshot, id uint64) (string, bool, error) {
	lower, upper := catalogPrefix()
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: upper})
	defer it.Close()
	for it.Seek(nil); it.Valid(); it.Next() {
		database, bucket, ok := strings.Cut(string(it.Key()[len(lower):]), "/")
		if !ok || bucket == "" {
			continue
		}
		encoded, err := it.Value()
		if err != nil {
			return "", false, fmt.Errorf("reading the catalog entry of bucket %q in database %q: %w", bucket, database, err)
		}
		var entry catalogEntry
		if err := bson.Unmarshal(encoded, &entry); err != nil {
			return "", false, fmt.Errorf("decoding the catalog entry of bucket %q in database %q: %w", bucket, database, err)
		}
		if uint64(entry.ID) == id {
			return database, true, nil
		}
	}
	if err := it.Err(); err != nil {
		return "", false, fmt.Errorf("walking the catalog: %w", err)
	}
	return "", false, nil
}

func (r *reclaimer) readOrphanCursor() (orphanCursor, error) {
	var cursor orphanCursor
	snap := r.s.kv.NewSnapshot()
	defer snap.Discard()
	encoded, err := snap.Get(reclaimKey(storage.OrphanCursorID))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return cursor, nil
	}
	if err != nil {
		return cursor, fmt.Errorf("reading the orphan pass's cursor: %w", err)
	}
	if err := bson.Unmarshal(encoded, &cursor); err != nil {
		return cursor, fmt.Errorf("decoding the orphan pass's cursor: %w", err)
	}
	return cursor, nil
}

// writeOrphanCursor commits the cursor unsynced: a lost write only makes the
// pass examine a few names again, and examining is idempotent.
func (r *reclaimer) writeOrphanCursor(cursor orphanCursor) error {
	encoded, err := bson.Marshal(cursor)
	if err != nil {
		return fmt.Errorf("encoding the orphan pass's cursor: %w", err)
	}
	batch := r.s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(reclaimKey(storage.OrphanCursorID), encoded); err != nil {
		return fmt.Errorf("writing the orphan pass's cursor: %w", err)
	}
	if err := batch.Commit(false); err != nil {
		return fmt.Errorf("committing the orphan pass's cursor at leaf %s: %w", cursor.Leaf, err)
	}
	return nil
}
