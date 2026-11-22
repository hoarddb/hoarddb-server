package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// The retired-field sweep (placement 5, conv 12 kind 't'): when a bump retires field ids, each
// node deletes those ids' keys from the pairs it holds, in the background, and nowhere else.
//
// The state of a sweep is derived, not stored beside the work. A 't' record says "this pair
// still may hold keys of this retired field"; the declaration's retired list says which ids are
// retired; and the keys themselves say how far the sweep got, because a deleted key is gone. A
// crash therefore resumes by reading the records again and scanning the bucket from its start:
// the keys already deleted are not found again, and the scan costs one pass over the bucket's
// field and store spaces, which is what the first pass costs too. A cursor kept in memory makes
// one run of a record linear; nothing about it needs to survive a restart.
//
// Cost (rule 11): one 39-byte record per held pair and retired field while the work is
// pending, then none; no goroutine per bucket (one per node), no file descriptor, no metric.
// Work is O(keys in the one bucket) per held pair, budgeted by
// config:Config.Storage.ReclaimKeysPerSecond, and it touches no other bucket.

// reclaimRetireKind is the kind byte of a retired-field record (conv 12).
const reclaimRetireKind byte = 't'

// reclaimBucketKind is the kind byte of a dropped-bucket record (conv 12): every node that
// holds a key of the bucket's range writes one per held pair when it applies the bump that
// marks the bucket dropping, and the same loop that sweeps retired fields works it off.
const reclaimBucketKind byte = 'b'

const (
	// defaultSweepKeysPerSecond is the budget when storage.reclaim_keys_per_second is zero.
	defaultSweepKeysPerSecond = 10_000
	// defaultSweepStepKeys bounds the keys one step visits.
	defaultSweepStepKeys = 1000
	// defaultSweepRetry is how long the loop waits after a lap that failed.
	defaultSweepRetry = time.Second
	// recordPage is how many records a lap reads at a time, so a lap's memory is O(1).
	recordPage = 64
	// defaultRetentionCheck is replication.retention_check_seconds when the node gives none.
	defaultRetentionCheck = time.Minute
	// dedupeCycleFloor is the shortest cycle of the dedupe round robin: every held pair is
	// visited at least once per max(dedupeCycleFloor, held × 2 × step / keys per second).
	dedupeCycleFloor = time.Minute
)

// RetireRecord is a node-local 't' record (conv 12):
// [0xFFFF][0x00×8][0x08]['t'][slot:2][db:8][bucket:8][fieldID:2][epoch:8], empty value.
type RetireRecord struct {
	Slot     uint16
	DBID     uint64
	BucketID uint64
	FieldID  uint16
	Epoch    uint64
}

// retireKeyLen is a 't' key's length: the 11-byte node prefix, the kind and 28 bytes.
const retireKeyLen = reclaimHead + reclaimScope + 2 + 8

// Key encodes the record's key.
func (r RetireRecord) Key() []byte {
	key := storage.NodeKey(storage.NSReclaim, []byte{reclaimRetireKind}, storage.Scope(r.Slot, r.DBID, r.BucketID))
	key = binary.BigEndian.AppendUint16(key, r.FieldID)
	return binary.BigEndian.AppendUint64(key, r.Epoch)
}

// DecodeRetireKey parses a 't' key. The scope is [slot:2][db:8][bucket:8], the same shape as a
// 'd' record's.
func DecodeRetireKey(key []byte) (RetireRecord, error) {
	head := storage.NodeKey(storage.NSReclaim, []byte{reclaimRetireKind})
	if len(key) != retireKeyLen || !bytes.HasPrefix(key, head) {
		return RetireRecord{}, errors.New("the key is not a retired-field record")
	}
	scope := key[reclaimHead : reclaimHead+reclaimScope]
	return RetireRecord{
		Slot:     binary.BigEndian.Uint16(scope[0:2]),
		DBID:     binary.BigEndian.Uint64(scope[2:10]),
		BucketID: binary.BigEndian.Uint64(scope[10:18]),
		FieldID:  binary.BigEndian.Uint16(key[reclaimHead+reclaimScope:]),
		Epoch:    binary.BigEndian.Uint64(key[reclaimHead+reclaimScope+2:]),
	}, nil
}

// BucketRecord is a node-local 'b' record (conv 12):
// [0xFFFF][0x00×8][0x08]['b'][slot:2][db:8][bucket:8], empty value.
type BucketRecord struct {
	Slot     uint16
	DBID     uint64
	BucketID uint64
}

// bucketKeyLen is a 'b' key's length: the node prefix, the kind and the 18-byte scope.
const bucketKeyLen = reclaimHead + reclaimScope

// Key encodes the record's key.
func (r BucketRecord) Key() []byte {
	return storage.NodeKey(storage.NSReclaim, []byte{reclaimBucketKind}, storage.Scope(r.Slot, r.DBID, r.BucketID))
}

// DecodeBucketKey parses a 'b' key.
func DecodeBucketKey(key []byte) (BucketRecord, error) {
	head := storage.NodeKey(storage.NSReclaim, []byte{reclaimBucketKind})
	if len(key) != bucketKeyLen || !bytes.HasPrefix(key, head) {
		return BucketRecord{}, errors.New("the key is not a dropped-bucket record")
	}
	scope := key[reclaimHead:]
	return BucketRecord{
		Slot:     binary.BigEndian.Uint16(scope[0:2]),
		DBID:     binary.BigEndian.Uint64(scope[2:10]),
		BucketID: binary.BigEndian.Uint64(scope[10:18]),
	}, nil
}

// RetireDeps is what the retired-field sweep needs.
type RetireDeps struct {
	// KV is the node's Badger.
	KV storage.KV
	// Pairs is the node's pair set: the held pairs and their locks.
	Pairs *cluster.PairSet
	// Declarations reads a declaration by name from its owner.
	Declarations DeclarationSource
	// BucketName reads the name a bucket id belongs to from the reverse index.
	BucketName func(ctx context.Context, dbID, bucketID uint64) (name string, found bool, err error)
	// Published is the database's published schema_version on this node.
	Published func(dbID uint64) int64
	// KeysPerSecond is config:Config.Storage.ReclaimKeysPerSecond; 0 takes the default.
	KeysPerSecond int
	// StepKeys bounds the keys one step visits and deletes; 0 takes the default.
	StepKeys int
	// Sleep waits for the budget; nil sleeps on the wall clock.
	Sleep func(ctx context.Context, d time.Duration) error
	// Now is the budget's clock; nil is time.Now.
	Now func() time.Time
	// Manual starts no goroutine: the caller drives Pass and Drain, which is how a test makes a
	// sweep deterministic.
	Manual bool
	// Retry is how long the loop waits after a failed lap; 0 takes a second.
	Retry time.Duration
	// Tick is the node's retention tick: each tick runs the records and then a round of the
	// dedupe sweep. Nil means no tick, which is a loop that sweeps no dedupe entries.
	Tick <-chan time.Time
	// TickInterval is the period of Tick, which sizes the round robin; 0 takes a minute.
	TickInterval time.Duration
	// Dedupe is the node's dedupe sweeper; nil sweeps no dedupe entries.
	Dedupe *DedupeSweeper
	// Expiry is the node's expiry sweeper; nil sweeps no expired items. It runs after the dedupe
	// round on every tick, each charged to the same reclaim budget.
	Expiry *ExpirySweeper
	// OnError receives an error the sweep has no caller for, such as a budget wait that ended
	// early; nil drops it.
	OnError func(error)
	// Budget is the node's one reclaim budget, shared with the seed reset; nil makes the
	// sweep its own from KeysPerSecond and StepKeys.
	Budget *reclaimBudget
}

// RetireSweep is the node's one background pass over its retired-field and dropped-bucket
// records. It owns one goroutine and no per-bucket state.
type RetireSweep struct {
	deps   RetireDeps
	budget *reclaimBudget

	mu      sync.Mutex
	started bool
	// queued are notes that arrived before Start, or whose records could not be written yet.
	queued  []queuedNote
	lastErr error
	cancel  context.CancelFunc
	ctx     context.Context
	wake    chan struct{}
	wg      sync.WaitGroup
	closed  bool

	// onSwept, when set, is run once the last 'b' record of a bucket has swept its pair: it
	// frees the name and the id (the declaration, then the reverse document). The record is
	// deleted only after it returns, so a failure leaves the record and the loop tries again.
	onSwept func(ctx context.Context, dbID, bucketID uint64) error
	// loopMu is held for reading by each lap of the goroutine, so a test that takes it for
	// writing holds the background loop off while it looks at a half-finished drop.
	loopMu sync.RWMutex
	// afterStep, when set by a test, runs after each step has released the pair's lock.
	afterStep func()
	// dedupeAfter is the round robin's cursor: the last pair the dedupe sweep visited, in
	// (slot, db) order; the next tick resumes after it. Only the loop goroutine touches it.
	dedupeAfter sweepPair
	dedupeBegun bool
}

type queuedNote struct {
	dbID uint64
	note cluster.BumpNote
}

// NewRetireSweep returns a sweep that queues notes until Start gives it its collaborators.
func NewRetireSweep() *RetireSweep {
	return &RetireSweep{wake: make(chan struct{}, 1)}
}

// StartRetireSweep starts the node's one sweep goroutine; it stops when ctx ends or Close is called.
func StartRetireSweep(ctx context.Context, deps RetireDeps) *RetireSweep {
	sweep := NewRetireSweep()
	sweep.Start(ctx, deps)
	return sweep
}

// Start gives the sweep its collaborators, writes the records of every note that came before,
// and starts its goroutine unless deps.Manual is set.
func (s *RetireSweep) Start(ctx context.Context, deps RetireDeps) {
	if deps.KeysPerSecond <= 0 {
		deps.KeysPerSecond = defaultSweepKeysPerSecond
	}
	if deps.StepKeys <= 0 {
		deps.StepKeys = defaultSweepStepKeys
	}
	if deps.Retry <= 0 {
		deps.Retry = defaultSweepRetry
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.Sleep == nil {
		deps.Sleep = sleepOn
	}
	if deps.TickInterval <= 0 {
		deps.TickInterval = defaultRetentionCheck
	}
	s.mu.Lock()
	s.deps = deps
	// The node builds one budget for every background pass and hands it in; a sweep started
	// without one (a test, a standalone sweep) makes its own.
	s.budget = deps.Budget
	if s.budget == nil {
		s.budget = newReclaimBudget(reclaimBudgetConfig{KeysPerSecond: deps.KeysPerSecond, StepKeys: deps.StepKeys, Now: deps.Now, Sleep: deps.Sleep})
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.started = true
	queued := s.queued
	s.queued = nil
	s.mu.Unlock()
	for _, item := range queued {
		s.Note(item.dbID, 0, item.note)
	}
	if !deps.Manual {
		s.wg.Add(1)
		go s.run()
	}
	s.wakeUp()
}

// Note records the work a committed bump asks of this node: one 't' record per held pair and
// retired field. It runs on the FSM goroutine after the bump is published, writes one batch per
// half batch bound of records and takes no pair lock. A note that cannot be written yet (the
// sweep has not started, or the store refused) is kept and tried again by the loop, and a
// restart replays the Raft log from its snapshot, which delivers it again.
func (s *RetireSweep) Note(dbID uint64, _ int64, note cluster.BumpNote) {
	s.mu.Lock()
	if !s.started || s.closed {
		s.queued = append(s.queued, queuedNote{dbID: dbID, note: note})
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	if err := s.begin(dbID, note); err != nil {
		s.mu.Lock()
		s.queued = append(s.queued, queuedNote{dbID: dbID, note: note})
		s.lastErr = err
		s.mu.Unlock()
	}
	s.wakeUp()
}

// begin writes the records one note asks for.
func (s *RetireSweep) begin(dbID uint64, note cluster.BumpNote) error {
	var keys [][]byte
	if note.Drop {
		for _, slot := range s.deps.Pairs.HeldSlots(dbID) {
			keys = append(keys, BucketRecord{Slot: slot, DBID: dbID, BucketID: note.BucketID}.Key())
		}
		return s.writeRecords(keys)
	}
	for _, slot := range s.deps.Pairs.HeldSlots(dbID) {
		for _, field := range note.Retired {
			keys = append(keys, RetireRecord{Slot: slot, DBID: dbID, BucketID: note.BucketID, FieldID: field, Epoch: note.Epoch}.Key())
		}
	}
	return s.writeRecords(keys)
}

// writeRecords sets empty-valued records in batches that stay inside the store's bound, each batch
// synced. Every caller's records are the only memory of their work: a note's, because the Raft
// group may compact the bump once this node applied it and nothing delivers the note again; a
// ready pair's and a sweep's, because nothing derives them again after a cut of the store
// (store-vouched-cut-spec.md §3.7).
func (s *RetireSweep) writeRecords(keys [][]byte) error {
	limit := s.deps.KV.MaxBatchBytes() / 2
	for len(keys) > 0 {
		batch := s.deps.KV.NewBatch()
		var size int64
		taken := 0
		for taken < len(keys) && (taken == 0 || size+int64(len(keys[taken])+batchEntryOverhead) <= limit) {
			if err := batch.Set(keys[taken], nil); err != nil {
				batch.Discard()
				return fmt.Errorf("adding record %x to a batch: %w", keys[taken], err)
			}
			size += int64(len(keys[taken]) + batchEntryOverhead)
			taken++
		}
		if err := batch.Commit(true); err != nil {
			batch.Discard()
			return fmt.Errorf("writing %d sweep records: %w", taken, err)
		}
		keys = keys[taken:]
	}
	return nil
}

func (s *RetireSweep) wakeUp() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// LastError is the error of the most recent lap that failed, or nil. It is for status output
// and tests; the loop retries on its own.
func (s *RetireSweep) LastError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErr
}

// Close stops the goroutine and waits for it. It is safe to call twice.
func (s *RetireSweep) Close() {
	s.mu.Lock()
	s.closed = true
	cancel := s.cancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.wg.Wait()
}

// run is the node's one sweep goroutine. A wake or a retry runs the records; the retention
// tick runs the records and then a round of the dedupe sweep (storage-format spec §2.4), the
// only thing that drives that sweep.
func (s *RetireSweep) run() {
	defer s.wg.Done()
	ticked := false
	for {
		s.loopMu.RLock()
		if s.ctx.Err() != nil {
			s.loopMu.RUnlock()
			return
		}
		worked, err := s.lap(s.ctx)
		if ticked {
			ticked = false
			s.dedupeRound(s.ctx)
			s.expiryRound(s.ctx)
		}
		s.loopMu.RUnlock()
		s.mu.Lock()
		s.lastErr = err
		s.mu.Unlock()
		var retry <-chan time.Time
		var timer *time.Timer
		switch {
		case err != nil:
			timer = time.NewTimer(s.deps.Retry)
			retry = timer.C
		case worked > 0:
			continue
		}
		select {
		case <-s.ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		case <-s.wake:
		case <-retry:
		case <-s.deps.Tick:
			ticked = true
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

// tickBetweenRecords is the non-blocking tick check a lap makes before each record and a
// record makes between its steps, so a long 'b' lap or one huge record is
// interleaved with the dedupe round rather than ahead of it. A tick taken
// here is consumed here.
func (s *RetireSweep) tickBetweenRecords(ctx context.Context) {
	select {
	case <-s.deps.Tick:
		s.dedupeRound(ctx)
		s.expiryRound(ctx)
	default:
	}
}

// expiryRound runs one pass of the expiry sweeper, bounded by one step of keys and charged to
// the reclaim budget the dedupe round spends from. It is one Seek whatever the bucket count.
func (s *RetireSweep) expiryRound(ctx context.Context) {
	if s.deps.Expiry == nil {
		return
	}
	pass, err := s.deps.Expiry.Pass(ctx, s.deps.StepKeys)
	if err != nil {
		s.mu.Lock()
		s.lastErr = err
		s.mu.Unlock()
	}
	if err := s.budget.charge(ctx, pass.Visited+pass.Dropped); err != nil && s.deps.OnError != nil {
		// The wait for the budget ended early (the node is closing): the round ends here and the
		// next tick starts a new one. Said once per round; nothing is retried inside it.
		s.deps.OnError(fmt.Errorf("charging the expiry pass of %d keys to the reclaim budget: %w", pass.Visited+pass.Dropped, err))
	}
}

// dedupeRound sweeps the next K held pairs in (slot, db) order, round robin across ticks,
// each for one step, with every visit and delete charged to the reclaim budget. K is chosen
// so every held pair is visited at least once per max(a minute, held × 2 × step / keys per
// second): the ticks in that cycle divide the held pairs among them.
func (s *RetireSweep) dedupeRound(ctx context.Context) {
	if s.deps.Dedupe == nil {
		return
	}
	held := s.heldPairsSorted()
	if len(held) == 0 {
		return
	}
	for i := 0; i < s.dedupeFanout(len(held)); i++ {
		pair := s.nextDedupePair(held)
		visited, deleted, err := s.deps.Dedupe.Pass(pair.slot, pair.db, s.deps.StepKeys)
		if err != nil {
			s.mu.Lock()
			s.lastErr = err
			s.mu.Unlock()
		}
		if err := s.budget.charge(ctx, visited+deleted); err != nil {
			return
		}
	}
}

// dedupeFanout is K: the pairs one tick sweeps. A visit is charged the keys it walked plus
// the keys it deleted, up to 2 x StepKeys when every entry is dead, so the cycle is sized on
// that worst charge and the stated bound holds on a dense node.
func (s *RetireSweep) dedupeFanout(held int) int {
	work := time.Duration(float64(held) * 2 * float64(s.deps.StepKeys) / float64(s.deps.KeysPerSecond) * float64(time.Second))
	cycle := max(dedupeCycleFloor, work)
	ticksPerCycle := max(1, int(cycle/s.deps.TickInterval))
	return min(held, (held+ticksPerCycle-1)/ticksPerCycle)
}

// heldPairsSorted lists the held pairs in (slot, db) order, the round robin's order.
func (s *RetireSweep) heldPairsSorted() []sweepPair {
	var held []sweepPair
	s.deps.Pairs.Held(func(slot uint16, db uint64) { held = append(held, sweepPair{slot: slot, db: db}) })
	slices.SortFunc(held, func(a, b sweepPair) int {
		if a.slot != b.slot {
			return int(a.slot) - int(b.slot)
		}
		return cmpUint64(a.db, b.db)
	})
	return held
}

// nextDedupePair is the first held pair after the cursor, wrapping to the first.
func (s *RetireSweep) nextDedupePair(held []sweepPair) sweepPair {
	next := held[0]
	if s.dedupeBegun {
		at := slices.IndexFunc(held, func(p sweepPair) bool {
			return p.slot > s.dedupeAfter.slot || (p.slot == s.dedupeAfter.slot && p.db > s.dedupeAfter.db)
		})
		if at >= 0 {
			next = held[at]
		}
	}
	s.dedupeAfter, s.dedupeBegun = next, true
	return next
}

func cmpUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// lap works every 't' record and then every 'b' record once to completion, in key order, and
// reports how many it worked. A record that fails does not stop the lap: the others still run,
// and the first error is returned so the loop retries after a pause.
func (s *RetireSweep) lap(ctx context.Context) (int, error) {
	if err := s.flushQueued(); err != nil {
		return 0, err
	}
	var first error
	worked := 0
	for _, kind := range []byte{reclaimRetireKind, reclaimBucketKind} {
		var after []byte
		for {
			page, err := s.recordPage(kind, after)
			if err != nil {
				return worked, errors.Join(first, err)
			}
			for _, key := range page {
				after = key
				worked++
				s.tickBetweenRecords(ctx)
				if err := s.workRecord(ctx, kind, key); err != nil {
					if ctx.Err() != nil {
						return worked, fmt.Errorf("sweeping stopped: %w", ctx.Err())
					}
					first = errors.Join(first, err)
				}
			}
			if len(page) < recordPage {
				break
			}
		}
	}
	return worked, first
}

// workRecord decodes one record of kind and works it to completion.
func (s *RetireSweep) workRecord(ctx context.Context, kind byte, key []byte) error {
	if kind == reclaimBucketKind {
		rec, err := DecodeBucketKey(key)
		if err != nil {
			return fmt.Errorf("record %x: %w", key, err)
		}
		return s.sweepBucketRecord(ctx, rec)
	}
	rec, err := DecodeRetireKey(key)
	if err != nil {
		return fmt.Errorf("record %x: %w", key, err)
	}
	return s.sweepRecord(ctx, rec)
}

// flushQueued writes the records of every note kept for later.
func (s *RetireSweep) flushQueued() error {
	s.mu.Lock()
	queued := s.queued
	s.queued = nil
	s.mu.Unlock()
	for i, item := range queued {
		if err := s.begin(item.dbID, item.note); err != nil {
			s.mu.Lock()
			s.queued = append(slices.Clone(queued[i:]), s.queued...)
			s.mu.Unlock()
			return err
		}
	}
	return nil
}

// recordPage reads up to recordPage record keys of kind after the key after.
func (s *RetireSweep) recordPage(kind byte, after []byte) ([][]byte, error) {
	prefix := storage.NodeKey(storage.NSReclaim, []byte{kind})
	lower := prefix
	if after != nil {
		lower = append(slices.Clone(after), 0x00)
	}
	snap := s.deps.KV.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: storage.PrefixEnd(prefix)})
	defer it.Close()
	var page [][]byte
	for it.Seek(lower); it.Valid() && len(page) < recordPage; it.Next() {
		page = append(page, append([]byte(nil), it.Key()...))
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("reading sweep records of kind %q: %w", kind, err)
	}
	return page, nil
}

// Drain works every record until none is left, or one cannot be worked.
func (s *RetireSweep) Drain(ctx context.Context) error {
	for {
		worked, err := s.lap(ctx)
		if err != nil {
			return err
		}
		if worked == 0 {
			return nil
		}
	}
}

// Pass sweeps field fieldID of bucketID in every pair of database dbID that has a 't' record
// for it, step by step, and returns when none is left. It writes no record: a pass over a
// field with no record, which is every pass after the first, reads one page of records and
// writes nothing.
func (s *RetireSweep) Pass(dbID, bucketID uint64, fieldID uint16, epoch uint64) error {
	var after []byte
	var first error
	for {
		page, err := s.recordPage(reclaimRetireKind, after)
		if err != nil {
			return err
		}
		for _, key := range page {
			after = key
			rec, err := DecodeRetireKey(key)
			if err != nil || rec.DBID != dbID || rec.BucketID != bucketID || rec.FieldID != fieldID || rec.Epoch != epoch {
				continue
			}
			if err := s.sweepRecord(s.context(), rec); err != nil {
				first = errors.Join(first, err)
			}
		}
		if len(page) < recordPage {
			return first
		}
	}
}

func (s *RetireSweep) context() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

// verdict is what the declaration says about a record.
type verdict int

const (
	// verdictSweep means the field is retired in the declaration in force.
	verdictSweep verdict = iota
	// verdictObsolete means the record names work that is not this sweep's to do: the bucket
	// is gone, being dropped (its own sweep takes everything), or the field is not retired.
	verdictObsolete
)

// judge reads the declaration, outside every pair lock, and decides whether the record's field
// is retired. A retired id is retired for good (an id is never reused), so one judgement holds
// for the whole run of the record. An unreadable declaration is an error, never a verdict:
// the sweep waits rather than guess.
func (s *RetireSweep) judge(ctx context.Context, rec RetireRecord) (verdict, error) {
	name, found, err := s.deps.BucketName(ctx, rec.DBID, rec.BucketID)
	if err != nil {
		return 0, fmt.Errorf("reading the name of bucket %016x: %w", rec.BucketID, err)
	}
	if !found {
		return verdictObsolete, nil
	}
	decl, _, found, err := s.deps.Declarations.Read(ctx, rec.DBID, name)
	if err != nil {
		return 0, fmt.Errorf("reading the declaration of bucket %q: %w", name, err)
	}
	if !found || decl.BucketID != rec.BucketID || decl.State != machine.StateActive {
		return verdictObsolete, nil
	}
	table, _ := machine.EffectiveTable(decl, s.deps.Published(rec.DBID))
	if slices.ContainsFunc(table.Entries, func(entry machine.FieldEntry) bool { return entry.ID == rec.FieldID }) {
		// A live field. Never swept, whatever a note said.
		return verdictObsolete, nil
	}
	if !slices.ContainsFunc(table.Retired, func(retired machine.RetiredID) bool { return retired.ID == rec.FieldID }) {
		return verdictObsolete, nil
	}
	return verdictSweep, nil
}

// sweepRecord runs one record to completion: judge it, then step until the scan reaches the end
// of the bucket's field and store spaces. Each step is one hold of the pair's lock; the lock is
// free between steps, and the budget is paid after each.
func (s *RetireSweep) sweepRecord(ctx context.Context, rec RetireRecord) error {
	got, err := s.judge(ctx, rec)
	if err != nil {
		return err
	}
	if got == verdictObsolete {
		return s.dropRecord(rec.Key())
	}
	var cursor []byte
	for {
		out, err := s.retireStep(rec, cursor)
		if err != nil {
			return fmt.Errorf("sweeping field %d of bucket %016x in slot %d: %w", rec.FieldID, rec.BucketID, rec.Slot, err)
		}
		if s.afterStep != nil {
			s.afterStep()
		}
		if err := s.budget.charge(ctx, out.visited+out.deleted); err != nil {
			return err
		}
		if out.done {
			return nil
		}
		// No pair lock is held between steps, so the dedupe round can run here.
		s.tickBetweenRecords(ctx)
		cursor = out.last
	}
}

// stepOutcome is what one step did.
type stepOutcome struct {
	// recordGone is true when the step deleted the record itself.
	recordGone       bool
	visited, deleted int
	last             []byte
	done             bool
}

// retireStep is one step: under the pair's lock, read the next keys of the bucket's field and
// store spaces, take those that carry the record's field id, delete them, and, when the scan
// reached the end, delete the record in the same batch. The read and the delete are one hold of
// the lock, so no write can put a key under the id in between. A pair this node no longer
// holds has no range here, so its record is dropped.
func (s *RetireSweep) retireStep(rec RetireRecord, cursor []byte) (stepOutcome, error) {
	var out stepOutcome
	var stepErr error
	held := s.deps.Pairs.WithHeld(rec.Slot, rec.DBID, func(*cluster.Pair) {
		out, stepErr = s.retireLocked(rec, cursor)
	})
	if !held {
		return stepOutcome{done: true}, s.dropRecord(rec.Key())
	}
	return out, stepErr
}

func (s *RetireSweep) retireLocked(rec RetireRecord, cursor []byte) (stepOutcome, error) {
	snap := s.deps.KV.NewSnapshot()
	defer snap.Discard()
	reserve := int64(retireKeyLen + batchEntryOverhead)
	scan, err := scanRanges(snap, retiredFieldRanges(rec.Slot, rec.DBID, rec.BucketID), cursor,
		sweepWindow{keys: s.deps.StepKeys, bytes: s.deps.KV.MaxBatchBytes() - reserve},
		func(key []byte) (bool, error) { return keyCarriesField(key, rec.FieldID) })
	if err != nil {
		return stepOutcome{}, err
	}
	batch := s.deps.KV.NewBatch()
	defer batch.Discard()
	for _, key := range scan.doomed {
		if err := batch.Delete(key); err != nil {
			return stepOutcome{}, fmt.Errorf("adding the delete of key %x to a batch: %w", key, err)
		}
	}
	if scan.done {
		if err := batch.Delete(rec.Key()); err != nil {
			return stepOutcome{}, fmt.Errorf("adding the delete of the record to a batch: %w", err)
		}
	}
	if len(scan.doomed) > 0 || scan.done {
		if err := batch.Commit(false); err != nil {
			return stepOutcome{}, fmt.Errorf("committing a sweep step: %w", err)
		}
	}
	return stepOutcome{visited: scan.visited, deleted: len(scan.doomed), last: scan.last, done: scan.done}, nil
}

// dropRecord deletes one record key.
func (s *RetireSweep) dropRecord(key []byte) error {
	batch := s.deps.KV.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(key); err != nil {
		return fmt.Errorf("adding the delete of record %x to a batch: %w", key, err)
	}
	if err := batch.Commit(false); err != nil {
		return fmt.Errorf("deleting record %x: %w", key, err)
	}
	return nil
}

// RerunOnReady re-checks the buckets of one pair that has just become ready or completed a seed
// (conv 12): a range pulled from a source, or a snapshot, can carry keys written before a bump,
// and a pass that ran before the pair existed here would never see them. It reads the buckets
// that have keys in the pair, a page of ids at a time, and for each active bucket writes a 't'
// record for every field its declaration retires. The records are then worked like any other.
// Cost: one name read and one declaration read per bucket that has keys in the pair, which is
// the order of the seed that just copied them.
func (s *RetireSweep) RerunOnReady(slot uint16, dbID uint64) error {
	ctx := s.context()
	var keys [][]byte
	var after uint64
	first := true
	for {
		page, err := s.bucketsOfPair(slot, dbID, after, first)
		if err != nil {
			return err
		}
		first = false
		for _, bucket := range page {
			more, err := s.retiredOf(ctx, slot, dbID, bucket)
			if err != nil {
				return err
			}
			keys = append(keys, more...)
		}
		if len(page) < recordPage {
			break
		}
		after = page[len(page)-1]
	}
	// Synced (store-vouched-cut-spec.md §3.7): these records are written after the pair's synced
	// ready record and nothing derives them again, so a cut that dropped them unsynced would leave
	// the retired fields unswept for good. One store sync per pair that becomes ready with a
	// retired field; none when there is nothing to write.
	if err := s.writeRecords(keys); err != nil {
		return err
	}
	s.wakeUp()
	return nil
}

// retiredOf returns the records one bucket of a pair needs: none unless it is an active bucket
// whose declaration retires fields.
func (s *RetireSweep) retiredOf(ctx context.Context, slot uint16, dbID, bucket uint64) ([][]byte, error) {
	name, found, err := s.deps.BucketName(ctx, dbID, bucket)
	if err != nil {
		return nil, fmt.Errorf("reading the name of bucket %016x: %w", bucket, err)
	}
	if !found {
		return nil, nil
	}
	decl, _, found, err := s.deps.Declarations.Read(ctx, dbID, name)
	if err != nil {
		return nil, fmt.Errorf("reading the declaration of bucket %q: %w", name, err)
	}
	if found && decl.BucketID == bucket && decl.State == machine.StateDropping {
		return [][]byte{BucketRecord{Slot: slot, DBID: dbID, BucketID: bucket}.Key()}, nil
	}
	if !found || decl.BucketID != bucket || decl.State != machine.StateActive {
		return nil, nil
	}
	table, _ := machine.EffectiveTable(decl, s.deps.Published(dbID))
	keys := make([][]byte, 0, len(table.Retired))
	for _, retired := range table.Retired {
		keys = append(keys, RetireRecord{Slot: slot, DBID: dbID, BucketID: bucket, FieldID: retired.ID, Epoch: retired.Epoch}.Key())
	}
	return keys, nil
}

// bucketsOfPair lists, ascending, up to recordPage ids of buckets that have a key in the pair,
// starting strictly after the id after (or from the first when first is set). It skips from one
// bucket's range to the next, so it costs one seek per bucket and not one step per key. The
// system bucket (the largest id) is not a user bucket and is not listed.
func (s *RetireSweep) bucketsOfPair(slot uint16, dbID, after uint64, first bool) ([]uint64, error) {
	pair := storage.PairPrefix(slot, dbID)
	snap := s.deps.KV.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: pair, Upper: storage.PrefixEnd(pair)})
	defer it.Close()
	var buckets []uint64
	next := pair
	if !first {
		if after >= machine.SystemBucketID-1 {
			return nil, nil
		}
		next = binary.BigEndian.AppendUint64(slices.Clone(pair), after+1)
	}
	for len(buckets) < recordPage {
		it.Seek(next)
		if !it.Valid() {
			break
		}
		key := it.Key()
		if len(key) < 18 {
			return nil, fmt.Errorf("the key %x in a pair's range is shorter than a bucket scope", key)
		}
		bucket := binary.BigEndian.Uint64(key[10:18])
		if bucket >= machine.SystemBucketID {
			break
		}
		buckets = append(buckets, bucket)
		next = binary.BigEndian.AppendUint64(slices.Clone(pair), bucket+1)
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("listing the buckets of slot %d: %w", slot, err)
	}
	return buckets, nil
}

// setOnSwept sets the function that frees a bucket's name once its sweep is done; nil frees
// nothing (a drop run synchronously frees them itself).
func (s *RetireSweep) setOnSwept(finish func(ctx context.Context, dbID, bucketID uint64) error) {
	s.mu.Lock()
	s.onSwept = finish
	s.mu.Unlock()
}

func (s *RetireSweep) sweptHook() func(ctx context.Context, dbID, bucketID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.onSwept
}

// Enqueue writes the 'b' record of every pair this node holds for database dbID and wakes the
// loop, which sweeps them in the background. A drop that returns to its caller after the bump
// leaves its deletion to this.
func (s *RetireSweep) Enqueue(dbID, bucketID uint64) error {
	defer s.wakeUp()
	// The bump's note wrote these records durable already; only the missing ones are written.
	_, err := s.recordSweeps(dbID, bucketID, s.deps.Pairs.HeldSlots(dbID))
	return err
}

// RecordSweeps makes the 'b' record of every slot in slots durable and does not wake the loop: the
// missing ones are written in synced batches, and when none is missing one sync covers any record a
// concurrent writer committed that is not on disk yet. A drop that sweeps inline calls it once before
// its first SweepBucket, which then has no record to write: one store sync for the whole drop instead
// of one per slot (BUG-0861). Waking the loop here would start a second, background sweep of the same
// records.
func (s *RetireSweep) RecordSweeps(dbID, bucketID uint64, slots []uint16) error {
	wrote, err := s.recordSweeps(dbID, bucketID, slots)
	if err != nil {
		return err
	}
	if !wrote {
		if err := s.deps.KV.Sync(); err != nil {
			return fmt.Errorf("syncing the sweep records of bucket %016x: %w", bucketID, err)
		}
	}
	return nil
}

// recordSweeps finds the missing 'b' records of slots in one snapshot pass and writes them, and
// reports whether it wrote any.
func (s *RetireSweep) recordSweeps(dbID, bucketID uint64, slots []uint16) (wrote bool, err error) {
	snap := s.deps.KV.NewSnapshot()
	var missing [][]byte
	for _, slot := range slots {
		key := BucketRecord{Slot: slot, DBID: dbID, BucketID: bucketID}.Key()
		if _, err := snap.Get(key); errors.Is(err, storage.ErrKeyNotFound) {
			missing = append(missing, key)
		} else if err != nil {
			snap.Discard()
			return false, fmt.Errorf("looking for the sweep record %x: %w", key, err)
		}
	}
	snap.Discard()
	if len(missing) == 0 {
		return false, nil
	}
	return true, s.writeRecords(missing)
}

// otherBucketRecords reports whether a 'b' record other than rec names the same bucket.
func (s *RetireSweep) otherBucketRecords(rec BucketRecord) (bool, error) {
	var after []byte
	for {
		page, err := s.recordPage(reclaimBucketKind, after)
		if err != nil {
			return false, err
		}
		for _, key := range page {
			after = key
			other, err := DecodeBucketKey(key)
			if err == nil && other != rec && other.DBID == rec.DBID && other.BucketID == rec.BucketID {
				return true, nil
			}
		}
		if len(page) < recordPage {
			return false, nil
		}
	}
}

// HeldSlots lists, ascending, the slots of database dbID this node holds a pair for. It
// implements BucketSweeper: a drop waits for exactly the pairs this node has.
func (s *RetireSweep) HeldSlots(dbID uint64) []uint16 { return s.deps.Pairs.HeldSlots(dbID) }

// SweepBucket deletes every key of bucketID from one held pair, step by step, and returns when
// none is left. It writes no record: the drop calls RecordSweeps first, so by the time a slot is
// logged as swept its record has been at or below the cut (§3.7). A record that is gone now was
// swept and removed by the background loop (the common case on a node whose note queued every
// record), and with it the keys it covered: the record's removal commits after its keys' deletes, so
// a state without the record also lacks the keys, and a cut that dropped the removal restores the
// record, which the loop then works. The key deletes stay unsynced.
func (s *RetireSweep) SweepBucket(ctx context.Context, slot uint16, dbID, bucketID uint64) error {
	return s.sweepBucketRecord(ctx, BucketRecord{Slot: slot, DBID: dbID, BucketID: bucketID})
}

// errLiveBucket is the refusal to sweep a bucket whose declaration is active under the very id
// a 'b' record names. A record is written only for a dropping bucket, so this is a damaged
// record, and nothing is deleted on its word.
var errLiveBucket = errors.New("a dropped-bucket record names a bucket whose declaration is active")

// judgeDropped reads the declaration, outside every pair lock, and refuses to go on when the
// bucket is live. Dropping, gone, or re-created under a new id all mean the id is dead. An
// unreadable declaration is an error: the sweep waits.
func (s *RetireSweep) judgeDropped(ctx context.Context, rec BucketRecord) error {
	name, found, err := s.deps.BucketName(ctx, rec.DBID, rec.BucketID)
	if err != nil {
		return fmt.Errorf("reading the name of bucket %016x: %w", rec.BucketID, err)
	}
	if !found {
		return nil
	}
	decl, _, found, err := s.deps.Declarations.Read(ctx, rec.DBID, name)
	if err != nil {
		return fmt.Errorf("reading the declaration of bucket %q: %w", name, err)
	}
	if found && decl.BucketID == rec.BucketID && decl.State == machine.StateActive {
		return fmt.Errorf("bucket %q (%016x): %w", name, rec.BucketID, errLiveBucket)
	}
	return nil
}

// sweepBucketRecord runs one 'b' record to completion: every key under the bucket's scope in
// the pair, a bounded batch per hold of the pair's lock.
func (s *RetireSweep) sweepBucketRecord(ctx context.Context, rec BucketRecord) error {
	if err := s.judgeDropped(ctx, rec); err != nil {
		return err
	}
	var cursor []byte
	for {
		out, err := s.bucketStep(rec, cursor)
		if err != nil {
			return fmt.Errorf("sweeping bucket %016x in slot %d: %w", rec.BucketID, rec.Slot, err)
		}
		if s.afterStep != nil {
			s.afterStep()
		}
		if err := s.budget.charge(ctx, out.visited+out.deleted); err != nil {
			return err
		}
		if out.done {
			return s.bucketSwept(ctx, rec, out)
		}
		// No pair lock is held between steps, so the dedupe round can run here.
		s.tickBetweenRecords(ctx)
		cursor = out.last
	}
}

// bucketSwept is the end of a 'b' record. With a hook set the step left the record in place;
// the last record of the bucket runs the hook first, so a crash or a failure in freeing the
// name leaves the record and the loop does it again.
func (s *RetireSweep) bucketSwept(ctx context.Context, rec BucketRecord, out stepOutcome) error {
	if out.recordGone {
		return nil
	}
	others, err := s.otherBucketRecords(rec)
	if err != nil {
		return err
	}
	if !others {
		if finish := s.sweptHook(); finish != nil {
			if err := finish(ctx, rec.DBID, rec.BucketID); err != nil {
				return fmt.Errorf("freeing the name of swept bucket %016x: %w", rec.BucketID, err)
			}
		}
	}
	return s.dropRecord(rec.Key())
}

// bucketStep is one step of a 'b' record, under the pair's lock like retireStep: read the next
// keys under the scope, delete them, and with the last of them delete the record.
func (s *RetireSweep) bucketStep(rec BucketRecord, cursor []byte) (stepOutcome, error) {
	var out stepOutcome
	var stepErr error
	held := s.deps.Pairs.WithHeld(rec.Slot, rec.DBID, func(*cluster.Pair) {
		out, stepErr = s.bucketLocked(rec, cursor)
	})
	if !held {
		return stepOutcome{done: true, recordGone: true}, s.dropRecord(rec.Key())
	}
	return out, stepErr
}

func (s *RetireSweep) bucketLocked(rec BucketRecord, cursor []byte) (stepOutcome, error) {
	snap := s.deps.KV.NewSnapshot()
	defer snap.Discard()
	reserve := int64(bucketKeyLen + batchEntryOverhead)
	scan, err := scanRanges(snap, bucketRange(rec.Slot, rec.DBID, rec.BucketID), cursor,
		sweepWindow{keys: s.deps.StepKeys, bytes: s.deps.KV.MaxBatchBytes() - reserve},
		func([]byte) (bool, error) { return true, nil })
	if err != nil {
		return stepOutcome{}, err
	}
	batch := s.deps.KV.NewBatch()
	defer batch.Discard()
	for _, key := range scan.doomed {
		if err := batch.Delete(key); err != nil {
			return stepOutcome{}, fmt.Errorf("adding the delete of key %x to a batch: %w", key, err)
		}
	}
	// With a hook the record outlives its keys until the name is freed (bucketSwept).
	dropWithKeys := scan.done && s.sweptHook() == nil
	if dropWithKeys {
		if err := batch.Delete(rec.Key()); err != nil {
			return stepOutcome{}, fmt.Errorf("adding the delete of the record to a batch: %w", err)
		}
	}
	if len(scan.doomed) > 0 || dropWithKeys {
		if err := batch.Commit(false); err != nil {
			return stepOutcome{}, fmt.Errorf("committing a sweep step: %w", err)
		}
	}
	return stepOutcome{visited: scan.visited, deleted: len(scan.doomed), last: scan.last, done: scan.done, recordGone: dropWithKeys}, nil
}

// sleepOn waits d on the wall clock or until ctx ends.
func sleepOn(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("sleep interrupted: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}
