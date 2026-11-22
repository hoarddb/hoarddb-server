package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"slices"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// cursorSubject is the queue cursor under test, behind two implementations:
// modelCursor runs BucketState's cursor rules over an in-memory set of live
// positions, fast enough for thousands of seeded runs, and storeCursor runs
// the same operations through a real QueueStore, which proves the rules are
// wired into the walk. Entry n of either is the entry at position n, so a pop
// reports the positions it took.
type cursorSubject interface {
	push() error
	popN(n int) ([]uint64, error)
	deleteAt(index int) error
	// writeBack puts a live entry back at a dead position, the way a return, a
	// release or a requeue will (claim-ack-spec §4.6).
	writeBack(position uint64) error
	// requeueDead, discardDead and restoreDead are the dead verbs (claim-ack-spec
	// §4.12): a requeue is a write-back the dead list's entry names, a discard
	// writes nothing live, and a restore adds a dead entry at a fresh position,
	// Next, which it spends.
	requeueDead(position uint64) error
	discardDead(position uint64) error
	restoreDead() error
	live() []uint64
	next() uint64
	// deletable is how many live entries DeleteAtPosition can name: the ones at
	// or after Head, which is where its walk starts. A fifo reaches all of
	// them. A lifo does not maintain Head (only the head rule of a delete
	// moves it), so an entry written back below it is out of that walk's sight;
	// nothing on a server path calls DeleteAtPosition.
	deletable() int
	violations() []string
	// claim takes n like a pop, under a lease at the subject's clock; ack and
	// release act on its oldest live lease; expire moves the clock past every
	// deadline, so the next verb's return step returns every lease.
	claim(n int) ([]uint64, error)
	ack() error
	release() error
	expire()
	// leases is the harness's record of the subject's leases.
	leases() *leaseBook
}

// leaseBook is the harness's record of a cursor subject's leases and of each
// position's return count: what the model needs to run claim, ack, release and
// expiry, and what the store subject needs to know which positions its next
// verb's return step writes back.
type leaseBook struct {
	now    int64
	leases []bookLease
	// returns is a position's return count while it is in the queue or under
	// a lease; dead is the positions dead-lettered.
	returns map[uint64]uint64
	dead    map[uint64]bool
}

// bookLease is one lease. Every lease has one length and the clock only
// rises, so the slice's order is deadline order, and among equal deadlines
// the store's 'd' order too, its nonces counting up.
type bookLease struct {
	id        [leaseIDBytes]byte
	deadline  int64
	positions []uint64
}

func newLeaseBook() *leaseBook {
	return &leaseBook{now: testNow, returns: map[uint64]uint64{}, dead: map[uint64]bool{}}
}

// settle takes off the leases the next verb's return step returns and gives
// the positions that go back, in the order the step writes them: lease by
// lease, each in key order, which is position order.
func (b *leaseBook) settle() []uint64 {
	var back []uint64
	for len(b.leases) > 0 && b.leases[0].deadline <= b.now {
		back = append(back, b.returnLease(b.leases[0])...)
		b.leases = b.leases[1:]
	}
	return back
}

// returnLease counts one return for each of lease's positions and gives the
// ones that go back; at MaxDeliveries a position is dead-lettered.
func (b *leaseBook) returnLease(lease bookLease) []uint64 {
	var back []uint64
	for _, position := range slices.Sorted(slices.Values(lease.positions)) {
		returns := b.returns[position] + 1
		if returns >= MaxDeliveries {
			delete(b.returns, position)
			b.dead[position] = true
			continue
		}
		b.returns[position] = returns
		back = append(back, position)
	}
	return back
}

// due is what settle would give, without settling.
func (b *leaseBook) due() []uint64 {
	twin := &leaseBook{now: b.now, leases: slices.Clone(b.leases), returns: maps.Clone(b.returns), dead: maps.Clone(b.dead)}
	return twin.settle()
}

func (b *leaseBook) claimed(id [leaseIDBytes]byte, positions []uint64) {
	if len(positions) > 0 {
		b.leases = append(b.leases, bookLease{id: id, deadline: b.now + testLeaseMs, positions: positions})
	}
}

// gone forgets positions that left for good: popped, deleted or acked.
func (b *leaseBook) gone(positions []uint64) {
	for _, position := range positions {
		delete(b.returns, position)
	}
}

func (b *leaseBook) leased(position uint64) bool {
	for _, lease := range b.leases {
		if slices.Contains(lease.positions, position) {
			return true
		}
	}
	return false
}

// takeOldest removes and returns the oldest lease, false when none is out.
func (b *leaseBook) takeOldest() (bookLease, bool) {
	if len(b.leases) == 0 {
		return bookLease{}, false
	}
	oldest := b.leases[0]
	b.leases = b.leases[1:]
	return oldest, true
}

// modelCursor is the cursor rules with no store.
type modelCursor struct {
	mode      QueueMode
	st        BucketState
	livePos   []uint64
	positions *QueueStore
	book      *leaseBook
	nextLease uint64
	// returned counts the return step's write-backs, so a test can see its
	// runs reached them.
	returned int
}

func newModelCursor(mode QueueMode) *modelCursor {
	return &modelCursor{mode: mode, positions: &QueueStore{mode: mode}, book: newLeaseBook()}
}

func (m *modelCursor) kind() invariantKind {
	if m.mode == QueueLIFO {
		return invariantLIFO
	}
	return invariantFIFO
}

func (m *modelCursor) push() error {
	position := m.st.Next
	if m.mode == QueueLIFO {
		m.st.pushedLifo(position)
	}
	m.st.Next = max(m.st.Next, position+1)
	m.st.Tail = max(m.st.Tail, position)
	m.st.Entries++
	m.livePos = append(m.livePos, position)
	return nil
}

func (m *modelCursor) popN(n int) ([]uint64, error) {
	m.settle()
	taken, err := m.take(n)
	m.book.gone(taken)
	return taken, err
}

// settle is the return step: every due lease's positions go back.
func (m *modelCursor) settle() {
	for _, position := range m.book.settle() {
		m.putBack(position)
		m.returned++
	}
}

func (m *modelCursor) claim(n int) ([]uint64, error) {
	m.settle()
	taken, err := m.take(n)
	m.nextLease++
	var id [leaseIDBytes]byte
	binary.BigEndian.PutUint64(id[:8], m.nextLease)
	m.book.claimed(id, taken)
	return taken, err
}

func (m *modelCursor) ack() error {
	m.settle()
	if lease, ok := m.book.takeOldest(); ok {
		m.book.gone(lease.positions)
	}
	return nil
}

func (m *modelCursor) release() error {
	m.settle()
	if lease, ok := m.book.takeOldest(); ok {
		for _, position := range m.book.returnLease(lease) {
			m.putBack(position)
		}
	}
	return nil
}

func (m *modelCursor) expire()            { m.book.now += 2 * testLeaseMs }
func (m *modelCursor) leases() *leaseBook { return m.book }

// take is the walk from the cursor, as a pop or a claim takes.
func (m *modelCursor) take(n int) ([]uint64, error) {
	var taken []uint64
	for len(taken) < n && m.st.Entries > 0 {
		// The walk from the cursor: the first live key at or after Head, or the
		// last at or before Tail.
		found := false
		var position uint64
		if m.mode == QueueLIFO {
			for _, candidate := range slices.Backward(m.livePos) {
				if candidate <= m.st.Tail {
					position, found = candidate, true
					break
				}
			}
		} else {
			for _, candidate := range m.livePos {
				if candidate >= m.st.Head {
					position, found = candidate, true
					break
				}
			}
		}
		if !found {
			return taken, fmt.Errorf("%w: the model counts %d entries and none lies at its cursor", ErrQueueCursorLost, m.st.Entries)
		}
		m.livePos = slices.DeleteFunc(m.livePos, func(p uint64) bool { return p == position })
		m.st.Entries--
		m.positions.moveCursorPast(&m.st, position)
		m.st.tookLive(m.mode, position)
		taken = append(taken, position)
	}
	return taken, nil
}

func (m *modelCursor) firstDeletable() int {
	first, _ := slices.BinarySearch(m.livePos, m.st.Head)
	return first
}

func (m *modelCursor) deletable() int { return len(m.livePos) - m.firstDeletable() }

func (m *modelCursor) deleteAt(index int) error {
	first := m.firstDeletable()
	position := m.livePos[first+index]
	m.book.gone([]uint64{position})
	m.livePos = slices.Delete(m.livePos, first+index, first+index+1)
	m.st.Entries--
	m.st.tookLive(m.mode, position)
	if index == 0 {
		// deleteWhere's head rule: the deleted entry was the first from Head.
		m.st.Head = m.st.Next
		if first < len(m.livePos) {
			m.st.Head = m.livePos[first]
		}
	}
	return nil
}

func (m *modelCursor) writeBack(position uint64) error {
	m.putBack(position)
	return nil
}

func (m *modelCursor) requeueDead(position uint64) error {
	delete(m.book.dead, position)
	m.putBack(position)
	return nil
}

func (m *modelCursor) discardDead(position uint64) error {
	delete(m.book.dead, position)
	return nil
}

func (m *modelCursor) restoreDead() error {
	m.book.dead[m.st.Next] = true
	m.st.Next++
	return nil
}

// putBack is a live entry written back at position, the cursor rule with it.
func (m *modelCursor) putBack(position uint64) {
	m.livePos = append(m.livePos, position)
	slices.Sort(m.livePos)
	m.st.Entries++
	m.st.writtenBack(m.mode, position)
}

func (m *modelCursor) live() []uint64 { return m.livePos }
func (m *modelCursor) next() uint64   { return m.st.Next }
func (m *modelCursor) violations() []string {
	return cursorViolations(m.st, m.livePos, m.kind())
}

// storeCursor is the same operations through a QueueStore, its clock and its
// leases kept in a lease book so the harness knows what the store's own
// return step will write back.
type storeCursor struct {
	t      testing.TB
	queue  *QueueStore
	kv     storage.KV
	book   *leaseBook
	nonces uint64
}

func newStoreCursor(t testing.TB, kv storage.KV, mode QueueMode) *storeCursor {
	return &storeCursor{t: t, queue: NewQueueStore(kv, storage.BucketRef{Slot: 1, DatabaseID: 1, BucketID: 1}, new(OwedApplies), mode), kv: kv, book: newLeaseBook()}
}

func (s *storeCursor) claim(n int) ([]uint64, error) {
	s.book.settle()
	s.nonces++
	args := ClaimArgs{N: n, LeaseMs: testLeaseMs, Now: s.book.now, NameHash: testNameHash}
	binary.BigEndian.PutUint64(args.Nonce[:], s.nonces)
	var taken []uint64
	claim, err := s.queue.ClaimN(ApplyInfo{}, args, func(entry []byte) error {
		taken = append(taken, uint64(entryNumber(s.t, entry)))
		return nil
	})
	s.book.claimed(claim.LeaseID, taken)
	return taken, err
}

// leaseVerb runs ack or release on the oldest live lease, which must answer
// as its verb does: the book has already settled what the return step returns.
func (s *storeCursor) leaseVerb(verb func(ApplyInfo, LeaseArgs) (LeaseResult, error), want LeaseState) error {
	s.book.settle()
	lease, ok := s.book.takeOldest()
	if !ok {
		// The verb still runs, on a lease no bucket issued: its return step is
		// what the book has just settled.
		want = LeaseUnknown
	}
	result, err := verb(ApplyInfo{}, LeaseArgs{LeaseID: lease.id, Now: s.book.now, NameHash: testNameHash})
	if err != nil {
		return err
	}
	if result.State != want {
		return fmt.Errorf("lease %x answered %q, want %q", lease.id, result.State, want)
	}
	switch want {
	case LeaseReleased:
		s.book.returnLease(lease)
	case LeaseAcked:
		s.book.gone(lease.positions)
	}
	return nil
}

func (s *storeCursor) ack() error {
	return s.leaseVerb(func(info ApplyInfo, args LeaseArgs) (LeaseResult, error) { return s.queue.Ack(info, args, nil) }, LeaseAcked)
}

func (s *storeCursor) release() error { return s.leaseVerb(s.queue.Release, LeaseReleased) }

func (s *storeCursor) expire()            { s.book.now += 2 * testLeaseMs }
func (s *storeCursor) leases() *leaseBook { return s.book }

func (s *storeCursor) state() BucketState {
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, 1, 1, 1)
	if err != nil {
		s.t.Fatalf("loading the state: %v", err)
	}
	return st
}

func (s *storeCursor) push() error {
	position := s.state().Next
	got, err := s.queue.Push(ApplyInfo{}, queueEntry(s.t, int(position)))
	if err == nil && got != position {
		return fmt.Errorf("push took position %d, the harness expected %d", got, position)
	}
	return err
}

func (s *storeCursor) popN(n int) ([]uint64, error) {
	s.book.settle()
	var taken []uint64
	_, err := s.queue.PopN(ApplyInfo{}, n, s.book.now, func(entry []byte) error {
		taken = append(taken, uint64(entryNumber(s.t, entry)))
		return nil
	})
	s.book.gone(taken)
	return taken, err
}

func (s *storeCursor) deleteAt(index int) error {
	head := s.state().Head
	var deletable []uint64
	for _, position := range s.live() {
		if position >= head {
			deletable = append(deletable, position)
		}
	}
	s.book.gone([]uint64{deletable[index]})
	return s.queue.DeleteAtPosition(index)
}

func (s *storeCursor) requeueDead(position uint64) error {
	move, err := s.queue.Requeue(ApplyInfo{}, DeadSelector{DeadID: bigEndian(position)})
	if err != nil {
		return err
	}
	if move.Done != 1 {
		return fmt.Errorf("requeue of dead position %d moved %d entries, refused %+v", position, move.Done, move.Refused)
	}
	delete(s.book.dead, position)
	return nil
}

func (s *storeCursor) discardDead(position uint64) error {
	move, err := s.queue.Discard(ApplyInfo{}, DeadSelector{DeadID: bigEndian(position)})
	if err != nil {
		return err
	}
	if move.Done != 1 {
		return fmt.Errorf("discard of dead position %d moved %d entries, refused %+v", position, move.Done, move.Refused)
	}
	delete(s.book.dead, position)
	return nil
}

func (s *storeCursor) restoreDead() error {
	position := s.state().Next
	record := DeadRecord{Data: queueEntry(s.t, int(position)), Deliveries: MaxDeliveries, DeadAt: s.book.now, Reason: DeadReasonMaxDeliveries}
	if err := s.queue.RestoreDead(ApplyInfo{}, record); err != nil {
		return err
	}
	s.book.dead[position] = true
	return nil
}

func (s *storeCursor) writeBack(position uint64) error {
	entry := queueEntry(s.t, int(position))
	return s.queue.apply(ApplyInfo{}, func(_ storage.KVSnapshot, st *BucketState, batch storage.KVBatch) error {
		key := s.queue.positionKey(position)
		if err := batch.Set(key, entry); err != nil {
			return err
		}
		st.Entries++
		st.LogicalBytes += int64(len(key) + len(entry))
		st.writtenBack(s.queue.mode, position)
		return nil
	})
}

func (s *storeCursor) live() []uint64 {
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	prefix := s.queue.entryPrefix()
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	var positions []uint64
	for it.Seek(nil); it.Valid(); it.Next() {
		positions = append(positions, positionOf(it.Key()))
	}
	return positions
}

func (s *storeCursor) next() uint64 { return s.state().Next }

func (s *storeCursor) deletable() int {
	head := s.state().Head
	deletable := 0
	for _, position := range s.live() {
		if position >= head {
			deletable++
		}
	}
	return deletable
}

func (s *storeCursor) violations() []string {
	kind := invariantFIFO
	if s.queue.mode == QueueLIFO {
		kind = invariantLIFO
	}
	violations := deliveryInvariantViolations(s.kv, 1, 1, kind)
	if st := s.state(); st.Leases != int64(len(s.book.leases)) || st.Dead != int64(len(s.book.dead)) {
		violations = append(violations, fmt.Sprintf("the store holds %d leases and %d dead, the harness expects %d and %d",
			st.Leases, st.Dead, len(s.book.leases), len(s.book.dead)))
	}
	return violations
}

// semanticOrder is the order a pop must take: the lowest live positions first
// for fifo, the highest first for lifo.
func semanticOrder(mode QueueMode, live []uint64, n int) []uint64 {
	ordered := slices.Clone(live)
	slices.Sort(ordered)
	if mode == QueueLIFO {
		slices.Reverse(ordered)
	}
	return ordered[:min(n, len(ordered))]
}

// cursorStep applies one verb to subject and checks what a pop took against
// the semantic order and the invariants after it. A non-nil return is the
// failure, worded for the seed that produced it.
func cursorStep(subject cursorSubject, mode QueueMode, verb string, argument int) error {
	liveBefore := slices.Clone(subject.live())
	slices.Sort(liveBefore)
	switch verb {
	case "push":
		if err := subject.push(); err != nil {
			return fmt.Errorf("push: %w", err)
		}
	case "pop", "claim":
		// The verb's own return step writes back first: the semantic order is
		// over what is live once it has.
		available := append(slices.Clone(liveBefore), subject.leases().due()...)
		want := semanticOrder(mode, available, argument)
		take := subject.popN
		if verb == "claim" {
			take = subject.claim
		}
		got, err := take(argument)
		if err != nil {
			return fmt.Errorf("%s %d over live %v: %w", verb, argument, available, err)
		}
		if !slices.Equal(got, want) {
			return fmt.Errorf("%s %d over live %v took %v, want %v", verb, argument, available, got, want)
		}
	case "ack":
		if err := subject.ack(); err != nil {
			return fmt.Errorf("ack: %w", err)
		}
	case "release":
		if err := subject.release(); err != nil {
			return fmt.Errorf("release: %w", err)
		}
	case "expire":
		subject.expire()
	case "delete":
		if err := subject.deleteAt(argument); err != nil {
			return fmt.Errorf("delete index %d over live %v: %w", argument, liveBefore, err)
		}
	case "writeback":
		if err := subject.writeBack(uint64(argument)); err != nil {
			return fmt.Errorf("write back %d: %w", argument, err)
		}
	case "requeue":
		if err := subject.requeueDead(uint64(argument)); err != nil {
			return fmt.Errorf("requeue dead %d: %w", argument, err)
		}
	case "discard":
		if err := subject.discardDead(uint64(argument)); err != nil {
			return fmt.Errorf("discard dead %d: %w", argument, err)
		}
	case "restore":
		if err := subject.restoreDead(); err != nil {
			return fmt.Errorf("restore dead: %w", err)
		}
	default:
		return fmt.Errorf("unknown verb %q", verb)
	}
	if violations := subject.violations(); len(violations) > 0 {
		return fmt.Errorf("after %s %d over live %v the invariants break: %v", verb, argument, liveBefore, violations)
	}
	return nil
}

// randomCursorRun drives subject for steps seeded steps. It returns the first
// failure with the verbs that led to it.
func randomCursorRun(subject cursorSubject, mode QueueMode, seed int64, steps int) error {
	random := rand.New(rand.NewSource(seed))
	var trail []string
	for step := 0; step < steps; step++ {
		verb, argument := "push", 0
		switch action := random.Intn(100); {
		case action < 30:
		case action < 40:
			verb, argument = "pop", 1
		case action < 48:
			verb, argument = "pop", 2+random.Intn(4)
		case action < 58:
			verb, argument = "claim", 1+random.Intn(4)
		case action < 62:
			verb = "ack"
		case action < 66:
			verb = "release"
		case action < 70:
			verb = "expire"
		case action < 75:
			deletable := subject.deletable()
			if deletable == 0 {
				continue
			}
			verb, argument = "delete", random.Intn(deletable)
		case action < 88:
			dead := deadPositions(subject)
			if len(dead) == 0 {
				continue
			}
			verb, argument = "writeback", int(dead[random.Intn(len(dead))])
		case action < 93:
			onList := slices.Sorted(maps.Keys(subject.leases().dead))
			if len(onList) == 0 {
				continue
			}
			verb, argument = "requeue", int(onList[random.Intn(len(onList))])
		case action < 96:
			onList := slices.Sorted(maps.Keys(subject.leases().dead))
			if len(onList) == 0 {
				continue
			}
			verb, argument = "discard", int(onList[random.Intn(len(onList))])
		default:
			verb = "restore"
		}
		trail = append(trail, fmt.Sprintf("%s %d", verb, argument))
		if err := cursorStep(subject, mode, verb, argument); err != nil {
			return fmt.Errorf("seed %d, step %d (%v): %w", seed, step, trail[max(0, len(trail)-12):], err)
		}
	}
	return nil
}

// deadPositions are the positions below next that hold no live entry and are
// neither under a lease nor on the dead list: the ones a write-back can land
// on without putting an entry in two places.
func deadPositions(subject cursorSubject) []uint64 {
	liveSet := make(map[uint64]bool)
	for _, position := range subject.live() {
		liveSet[position] = true
	}
	book := subject.leases()
	var dead []uint64
	for position := uint64(0); position < subject.next(); position++ {
		if !liveSet[position] && !book.leased(position) && !book.dead[position] {
			dead = append(dead, position)
		}
	}
	return dead
}

// TestRememberedCursorKeepsPopOrder is the model test (claim-ack-spec §4.6,
// CA-6): push, pop, PopN, DeleteAtPosition and write-back in seeded order, and
// every entry taken is checked against the semantic order, with the invariants
// after every step. The rules run over a model 20,000 times for each mode, and
// through a real store on the in-memory KV and on Badger, so a rule that holds
// in the model but is not wired into the walk fails too. Dropping the lifo
// push count-up fails nearly every lifo run: push 0, push 1, pop 1, push 2,
// push 3, pop 3, and the next pop takes 0 while 2 is live.
func TestRememberedCursorKeepsPopOrder(t *testing.T) {
	const steps = 400
	for _, mode := range []QueueMode{QueueFIFO, QueueLIFO} {
		t.Run(modeName(mode)+"/model", func(t *testing.T) {
			runs := 20_000
			if testing.Short() || raceEnabled {
				runs = 2_000
			}
			returned, dead := 0, 0
			for run := 0; run < runs; run++ {
				model := newModelCursor(mode)
				if err := randomCursorRun(model, mode, int64(run), steps); err != nil {
					t.Fatalf("run %d of %d: %v", run, runs, err)
				}
				returned += model.returned
				dead += len(model.book.dead)
			}
			// Runs that never return or dead-letter prove nothing of either.
			if returned == 0 || dead == 0 {
				t.Fatalf("%d runs returned %d entries and dead-lettered %d", runs, returned, dead)
			}
			t.Logf("%d runs: %d entries returned by the return step, %d dead-lettered", runs, returned, dead)
		})
		t.Run(modeName(mode)+"/store", func(t *testing.T) {
			for run := 0; run < 60; run++ {
				subject := newStoreCursor(t, kvfault.New(), mode)
				if err := randomCursorRun(subject, mode, int64(run), steps); err != nil {
					t.Fatalf("run %d: %v", run, err)
				}
			}
		})
		t.Run(modeName(mode)+"/badger", func(t *testing.T) {
			kv, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
			if err != nil {
				t.Fatalf("opening the store: %v", err)
			}
			t.Cleanup(func() {
				if err := kv.Close(); err != nil {
					t.Errorf("closing the store: %v", err)
				}
			})
			if err := randomCursorRun(newStoreCursor(t, kv, mode), mode, 7, 200); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// scriptedCursor is one verb of a scripted sequence.
type scriptedCursor struct {
	verb     string
	argument int
	// wantTaken is the lifo order a pop must take, checked beside the semantic
	// order, for the verbs that name one.
	wantTaken []uint64
}

// TestLifoStaysInStackOrderAcrossAGap runs a gap, a nested gap and deletes
// below and inside the run through a lifo (and the same verbs through a fifo,
// whose semantic order the step checks): Entries is kept and every pop takes
// the newest live entry.
func TestLifoStaysInStackOrderAcrossAGap(t *testing.T) {
	scripts := map[string][]scriptedCursor{
		// push 0, push 1, pop 1 leaves Tail on a marker; push 2 lands two above it.
		"one gap": {
			{"push", 0, nil}, {"push", 0, nil}, {"pop", 1, []uint64{1}},
			{"push", 0, nil}, {"push", 0, nil}, {"pop", 1, []uint64{3}},
			{"pop", 1, []uint64{2}}, {"pop", 1, []uint64{0}}, {"pop", 1, nil},
		},
		// a second gap opens inside the first run: push 4 lands over the marker
		// of 3 while the run is still armed.
		"nested gap": {
			{"push", 0, nil}, {"push", 0, nil}, {"pop", 1, []uint64{1}},
			{"push", 0, nil}, {"push", 0, nil}, {"pop", 1, []uint64{3}},
			{"push", 0, nil}, {"pop", 1, []uint64{4}},
			{"pop", 1, []uint64{2}}, {"pop", 1, []uint64{0}},
		},
		// the entry below the floor is deleted: it is not on the run's side.
		"delete below the floor": {
			{"push", 0, nil}, {"push", 0, nil}, {"pop", 1, []uint64{1}},
			{"push", 0, nil}, {"delete", 0, nil},
			{"pop", 1, []uint64{2}}, {"pop", 1, nil},
			{"push", 0, nil}, {"pop", 1, []uint64{3}},
		},
		// the run's only entry is deleted: the count reaches zero without a pop.
		"delete the run": {
			{"push", 0, nil}, {"push", 0, nil}, {"pop", 1, []uint64{1}},
			{"push", 0, nil}, {"delete", 1, nil},
			{"pop", 1, []uint64{0}}, {"pop", 1, nil},
		},
		// a batch pop crosses the run and its floor in one apply.
		"pop across the run": {
			{"push", 0, nil}, {"push", 0, nil}, {"pop", 1, []uint64{1}},
			{"push", 0, nil}, {"push", 0, nil}, {"push", 0, nil},
			{"pop", 5, []uint64{4, 3, 2, 0}},
		},
	}
	for name, script := range scripts {
		for _, mode := range []QueueMode{QueueFIFO, QueueLIFO} {
			t.Run(name+"/"+modeName(mode), func(t *testing.T) {
				subject := newStoreCursor(t, kvfault.New(), mode)
				for index, verb := range script {
					if err := cursorStep(subject, mode, verb.verb, verb.argument); err != nil {
						t.Fatalf("verb %d (%s %d): %v", index, verb.verb, verb.argument, err)
					}
				}
			})
		}
	}
	t.Run("explicit lifo order", func(t *testing.T) {
		subject := newStoreCursor(t, kvfault.New(), QueueLIFO)
		for index, verb := range scripts["one gap"] {
			if verb.verb != "pop" {
				if err := cursorStep(subject, QueueLIFO, verb.verb, verb.argument); err != nil {
					t.Fatalf("verb %d: %v", index, err)
				}
				continue
			}
			taken, err := subject.popN(verb.argument)
			if err != nil {
				t.Fatalf("verb %d: %v", index, err)
			}
			if !slices.Equal(taken, verb.wantTaken) {
				t.Fatalf("verb %d took %v, want %v", index, taken, verb.wantTaken)
			}
		}
	})
}

// TestCursorHarnessAgreesOnAnEmptyBucket keeps the harness honest: the model and the
// store agree on what an empty bucket does.
func TestCursorHarnessAgreesOnAnEmptyBucket(t *testing.T) {
	for _, mode := range []QueueMode{QueueFIFO, QueueLIFO} {
		for name, subject := range map[string]cursorSubject{
			"model": newModelCursor(mode),
			"store": newStoreCursor(t, kvfault.New(), mode),
		} {
			taken, err := subject.popN(3)
			if err != nil || len(taken) != 0 {
				t.Errorf("%s %s: popN on an empty bucket = %v, %v, want nothing", name, modeName(mode), taken, err)
			}
			if errors.Is(err, ErrQueueCursorLost) {
				t.Errorf("%s %s: an empty bucket is not a lost cursor", name, modeName(mode))
			}
		}
	}
}
