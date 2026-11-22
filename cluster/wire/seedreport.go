package wire

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// reportDueSlots is how many pending bits make a database due before reportInterval. A due
// database sends every pending bit in one SEEDED and one RETIRE: the body is the whole
// 1,024-byte bitmap however few bits it carries, and 64 slots per command put 43 to 86
// entries per node per new database into the Raft log, enough to push a three-node
// cluster's quorum rounds past their bound (BUG-0808).
const reportDueSlots = 64

// reportTick is how often Run looks for a state_seq change and a due batch.
const reportTick = 250 * time.Millisecond

// reportInterval bounds how long a pending bit waits for its flush.
const reportInterval = 2 * time.Second

// seedReportDeps is everything the reporter needs from its node; tests fill it with fakes.
type seedReportDeps struct {
	Self      string
	Committed func() cluster.ClusterState
	Record    func(slot uint16, db uint64) (state uint8, found bool)  // Reconciler.Record, State only
	Records   func(visit func(slot uint16, db uint64, state uint8))   // Reconciler.Records
	Send      func(ctx context.Context, kind byte, body []byte) error // Node.propose, result dropped
	OnError   func(error)
	Now       func() time.Time
	// Hold stops Run from deriving and sending bits (ClusterOptions.HoldSeedReports); Clear
	// is not Run's and still sends.
	Hold bool
}

// dbPending is the candidate bits of one database. It exists only while a bit is
// pending, so an idle database costs the reporter nothing.
type dbPending struct {
	set     []byte
	clear   []byte
	pending int
	first   time.Time // when the oldest unsent bit was noted
}

// seedReporter batches the seed bits this node reports through the relay. Run is its one
// goroutine per node; the methods below are also callable on their own, which the tests do.
type seedReporter struct {
	deps seedReportDeps

	// sendSlot serialises Flush and Clear, so a Clear waits for a batch in flight and a
	// batch never carries a bit a Clear is committing. It is held across the proposal, and
	// a proposal is bounded by relayCallTimeout, so it is never held longer than that. It
	// is a one-slot channel and not a sync.Mutex so a waiter can give up with its context.
	sendSlot chan struct{}

	// mu guards the maps below and is never held across a call into deps.
	mu   sync.Mutex
	dbs  map[uint64]*dbPending
	done map[reconcileKey]struct{}
}

// newSeedReporter returns a reporter with nothing pending.
func newSeedReporter(deps seedReportDeps) *seedReporter {
	return &seedReporter{
		deps:     deps,
		sendSlot: make(chan struct{}, 1),
		dbs:      make(map[uint64]*dbPending),
		done:     make(map[reconcileKey]struct{}),
	}
}

// Run derives the candidates from the records at every state_seq change, the first tick
// included (the boot pass), and flushes what is due. It returns when ctx is done. A held
// reporter only waits, so the node still owns a reporter for Clear.
func (r *seedReporter) Run(ctx context.Context) {
	if r.deps.Hold {
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(reportTick)
	defer ticker.Stop()
	lastSeq := int64(-1)
	seeding := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		// A seed finishes between commits: its record turns ready after the pass that
		// followed the last state_seq change, and with nothing else committing nothing
		// would note its bit. So while any record is still seeding the scan repeats.
		if seq := r.deps.Committed().StateSeq; seq != lastSeq || seeding {
			lastSeq = seq
			seeding = r.Reconcile()
		}
		r.Flush(ctx)
	}
}

// Note records one candidate bit for a database; it never sends.
func (r *seedReporter) Note(db uint64, slot uint16, clear bool) {
	now := r.deps.Now()
	r.mu.Lock()
	err := r.addLocked(db, slot, clear, now)
	r.mu.Unlock()
	if err != nil {
		r.deps.OnError(fmt.Errorf("noting seed bit %d of database %d: %w", slot, db, err))
	}
}

// addLocked ORs one bit into the matching mask. The caller holds r.mu.
func (r *seedReporter) addLocked(db uint64, slot uint16, clear bool, first time.Time) error {
	p := r.dbs[db]
	if p == nil {
		p = &dbPending{set: make([]byte, machine.BitmapBytes), clear: make([]byte, machine.BitmapBytes)}
	}
	mask := p.set
	if clear {
		mask = p.clear
	}
	on, err := machine.Bit(mask, slot)
	if err != nil {
		return err
	}
	if on {
		return nil
	}
	if err := machine.Set(mask, slot); err != nil {
		return err
	}
	if p.pending == 0 || first.Before(p.first) {
		p.first = first
	}
	p.pending++
	r.dbs[db] = p
	return nil
}

// lockSend takes the send slot, or returns the context's error when ctx ends first.
func (r *seedReporter) lockSend(ctx context.Context) error {
	select {
	case r.sendSlot <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// unlockSend gives the send slot back; only the holder calls it.
func (r *seedReporter) unlockSend() { <-r.sendSlot }

// Clear is the reseed hook: it commits a clear of one bit before the reseed resets the pair.
// One relayCallTimeout, started here, bounds the whole call, the wait for a batch in flight
// included, so a reseed with no leader gives up in seconds and not behind a stuck batch.
func (r *seedReporter) Clear(ctx context.Context, db uint64, slot uint16) error {
	callCtx, cancel := context.WithTimeout(ctx, relayCallTimeout)
	defer cancel()
	if err := r.lockSend(callCtx); err != nil {
		return fmt.Errorf("clearing seed bit %d of database %d: the reseed waited %v for the reporter's in-flight batch and gave up (%w); the cluster has no leader or the leader is slow, and the reseed retries on the next catch-up", slot, db, relayCallTimeout, err)
	}
	defer r.unlockSend()
	if !committedBit(r.deps.Committed(), r.deps.Self, db, slot) {
		return nil
	}
	body, err := r.body(db, []uint16{slot})
	if err != nil {
		return fmt.Errorf("clearing seed bit %d of database %d: %w", slot, db, err)
	}
	if err := r.deps.Send(callCtx, cluster.CommandRetire, body); err != nil {
		return fmt.Errorf("clearing seed bit %d of database %d before the reseed: %w", slot, db, err)
	}
	return nil
}

// Reconcile notes the candidates that both directions of the records imply, and reports
// whether any record of a known database is still seeding, so that its bit may come later.
func (r *seedReporter) Reconcile() (seeding bool) {
	state := r.deps.Committed()
	known := make(map[int64]struct{}, len(state.Databases))
	for _, database := range state.Databases {
		known[database.ID] = struct{}{}
	}
	r.deps.Records(func(slot uint16, db uint64, recordState uint8) {
		if _, ok := known[int64(db)]; !ok {
			return
		}
		bit := committedBit(state, r.deps.Self, db, slot)
		if recordState == machine.SeedSeeding {
			seeding = true
		}
		switch {
		case recordState == machine.SeedReady && !bit:
			r.Note(db, slot, false)
		case recordState == machine.SeedSeeding && bit:
			r.Note(db, slot, true)
		}
	})
	return seeding
}

// Flush sends every database whose batch is due.
func (r *seedReporter) Flush(ctx context.Context) {
	if err := r.lockSend(ctx); err != nil {
		return // ctx ended, so Run is stopping; the candidates stay for a later Flush
	}
	defer r.unlockSend()
	state := r.deps.Committed()
	known := make(map[int64]struct{}, len(state.Databases))
	for _, database := range state.Databases {
		known[database.ID] = struct{}{}
	}
	for _, db := range r.pendingDatabases() {
		_, live := known[int64(db)]
		r.flushBatch(ctx, db, live)
	}
}

// pendingDatabases lists the databases with a bit pending, in a stable order.
func (r *seedReporter) pendingDatabases() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]uint64, 0, len(r.dbs))
	for db := range r.dbs {
		ids = append(ids, db)
	}
	slices.Sort(ids)
	return ids
}

// reportBatch is the candidates one send took out of the masks.
type reportBatch struct {
	set, clear []uint16
	first      time.Time
}

// takeBatch removes every candidate of db when the database is due, and reports whether it
// took any. A database that is gone loses all its candidates.
func (r *seedReporter) takeBatch(db uint64, live bool) (reportBatch, bool) {
	now := r.deps.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.dbs[db]
	if p == nil {
		return reportBatch{}, false
	}
	if !live {
		delete(r.dbs, db)
		return reportBatch{}, false
	}
	if p.pending < reportDueSlots && now.Sub(p.first) < reportInterval {
		return reportBatch{}, false
	}
	delete(r.dbs, db)
	return reportBatch{set: maskSlots(p.set), clear: maskSlots(p.clear), first: p.first}, true
}

// maskSlots lists the set bits of mask, lowest slot first.
func maskSlots(mask []byte) []uint16 {
	var slots []uint16
	for i, b := range mask {
		for b != 0 {
			bit := bits.TrailingZeros8(b)
			slots = append(slots, uint16(i*8+bit))
			b &^= 1 << bit
		}
	}
	return slots
}

// restore puts a batch back after a send error, so the next Flush sends it again.
func (r *seedReporter) restore(db uint64, set, clear []uint16, first time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, slot := range set {
		r.restoreBit(db, slot, false, first)
	}
	for _, slot := range clear {
		r.restoreBit(db, slot, true, first)
	}
}

// restoreBit re-adds a slot that came out of a mask a moment ago. A refusal here would
// mean the slot was never valid, and it could not have been taken.
func (r *seedReporter) restoreBit(db uint64, slot uint16, clear bool, first time.Time) {
	if err := r.addLocked(db, slot, clear, first); err != nil {
		r.deps.OnError(fmt.Errorf("restoring seed bit %d of database %d: %w", slot, db, err))
	}
}

// flushBatch takes and sends every pending bit of db when it is due: at most one SEEDED and
// one RETIRE. The caller holds the send slot.
func (r *seedReporter) flushBatch(ctx context.Context, db uint64, live bool) {
	batch, took := r.takeBatch(db, live)
	if !took {
		return
	}
	// The bits are derived from the records now, after the masks were released: the
	// reconciler calls Note while it holds its own lock, so r.mu must not wait on it.
	var seeded, retire, retired []uint16
	for _, slot := range batch.set {
		if state, found := r.deps.Record(slot, db); found && state == machine.SeedReady {
			seeded = append(seeded, slot)
		}
	}
	for _, slot := range batch.clear {
		state, found := r.deps.Record(slot, db)
		if found && (state == machine.SeedReady || state == machine.SeedRetiring) {
			continue
		}
		retire = append(retire, slot)
		if found && state == machine.SeedRetired {
			retired = append(retired, slot)
		}
	}
	if len(seeded) > 0 {
		keep, err := r.propose(ctx, cluster.CommandSeeded, db, seeded)
		if keep {
			r.restore(db, seeded, retire, batch.first)
			r.deps.OnError(err)
			return
		}
		if err != nil {
			r.deps.OnError(err)
		}
	}
	if len(retire) > 0 {
		keep, err := r.propose(ctx, cluster.CommandRetire, db, retire)
		if keep {
			r.restore(db, nil, retire, batch.first)
			r.deps.OnError(err)
			return
		}
		if err != nil {
			r.deps.OnError(err)
		} else {
			r.markDone(db, retired)
		}
	}
}

// propose sends one SEEDED or RETIRE for slots under its own relayCallTimeout, so a stuck
// proposal gives up and the send slot is free again. keep is true when the error is not a
// refusal, so the bits stay candidates; a refusal means the leader read the bits and
// said no, and sending them again would be refused again.
func (r *seedReporter) propose(ctx context.Context, kind byte, db uint64, slots []uint16) (keep bool, err error) {
	body, err := r.body(db, slots)
	if err != nil {
		return false, fmt.Errorf("building the seed report for database %d: %w", db, err)
	}
	sendCtx, cancel := context.WithTimeout(ctx, relayCallTimeout)
	defer cancel()
	if err := r.deps.Send(sendCtx, kind, body); err != nil {
		var refusal *cluster.Error
		return !errors.As(err, &refusal), fmt.Errorf("reporting %d seed bits of database %d: %w", len(slots), db, err)
	}
	return false, nil
}

// body is the SEEDED or RETIRE body that names slots.
func (r *seedReporter) body(db uint64, slots []uint16) ([]byte, error) {
	mask := make([]byte, machine.BitmapBytes)
	for _, slot := range slots {
		if err := machine.Set(mask, slot); err != nil {
			return nil, err
		}
	}
	return cluster.SeedBitmapBody(r.deps.Self, int64(db), mask), nil
}

// markDone remembers the slots whose RETIRE committed for the gate.
func (r *seedReporter) markDone(db uint64, slots []uint16) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, slot := range slots {
		r.done[reconcileKey{slot, db}] = struct{}{}
	}
}

// RetireDone reports true once, after the RETIRE of that slot has committed.
func (r *seedReporter) RetireDone(db uint64, slot uint16) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := reconcileKey{slot, db}
	_, ok := r.done[key]
	delete(r.done, key)
	return ok
}

// committedBit reports this node's committed bit for a slot; a database with no row has every bit clear.
func committedBit(state cluster.ClusterState, node string, db uint64, slot uint16) bool {
	for _, row := range state.SeedBitmaps {
		if row.Node == node && row.DBID == int64(db) {
			on, err := machine.Bit(row.Bits, slot)
			return err == nil && on
		}
	}
	return false
}

// Records visits every seed record the node holds on disk. The visit runs under the
// reconciler's lock, so it must not call back into the reconciler.
func (r *Reconciler) Records(visit func(slot uint16, db uint64, state uint8)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, p := range r.pairs {
		if p.present {
			visit(key.slot, key.db, p.disk.State)
		}
	}
}
