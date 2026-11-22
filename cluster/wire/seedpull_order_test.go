package wire

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// orderPuller is a Puller over seeding records in the given slots, none of them reachable: Sources
// is recorded and answers with no node, so a pull ends its first attempt at once and waits out an
// hour-long backoff. The order of the Sources calls is the order the pairs were taken in.
type orderPuller struct {
	puller *Puller
	seq    atomic.Uint64

	mu    sync.Mutex
	asked []uint16
	clock time.Time // the puller's clock; tests move it with advance
}

// advance moves the puller's clock forward.
func (o *orderPuller) advance(by time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.clock = o.clock.Add(by)
}

func newOrderPuller(t *testing.T, deficits func() func(slot uint16, db uint64) int, slots ...uint16) *orderPuller {
	t.Helper()
	st := threeNodes(t)
	kv := t3OpenKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	cut := cluster.Position{Epoch: uint64(st.Epoch), Seq: 3}
	for _, slot := range slots {
		putRecord(t, kv, slot, seedingRecord(machine.PhaseSnapshot, cut, "k"))
	}
	rig := newRig(t, kv, st, testSelf, true)
	if err := rig.rec.LoadRecords(); err != nil {
		t.Fatal(err)
	}
	order := &orderPuller{clock: time.Unix(1_000_000, 0)}
	order.seq.Store(1)
	order.puller = NewPuller(PullerConfig{
		Self: testSelf, Recon: rig.rec, KV: kv,
		Options:   SeedOptions{SlotsInFlight: 1, RetryBackoff: time.Hour},
		Committed: func() uint64 { return uint64(st.Epoch) },
		StateSeq:  order.seq.Load,
		Sources: func(slot uint16, _ uint64) []string {
			order.mu.Lock()
			defer order.mu.Unlock()
			order.asked = append(order.asked, slot)
			return nil
		},
		Deficits: deficits,
	})
	order.puller.now = func() time.Time {
		order.mu.Lock()
		defer order.mu.Unlock()
		return order.clock
	}
	return order
}

// deficitsOf is a Deficits that reads one table per call, the way the node reads one state.
func deficitsOf(table func() map[uint16]int) func() func(slot uint16, db uint64) int {
	return func() func(slot uint16, db uint64) int {
		read := table()
		return func(slot uint16, _ uint64) int { return read[slot] }
	}
}

// queueAll queues one pair per slot, in the order given.
func (o *orderPuller) queueAll(slots ...uint16) {
	for _, slot := range slots {
		o.puller.Queue(slot, reconDB)
	}
}

// taken is the slots the pull has asked for sources of, in order.
func (o *orderPuller) taken() []uint16 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]uint16(nil), o.asked...)
}

// takeNext hands the next pair to a worker the way Run's worker does, and returns its slot.
func (o *orderPuller) takeNext(t *testing.T) uint16 {
	t.Helper()
	key, _, ok := o.puller.next()
	if !ok {
		t.Fatal("the puller had no pair to take")
	}
	return key.slot
}

// TestUnderReplicatedSlotsSeedFirst: three pairs queued in slot order with deficits 0, 2 and 1 are
// pulled most-missing first, so the pair with no other copy is not made to wait behind the one
// that has both.
// [BREAK: order the queue by slot only]
func TestUnderReplicatedSlotsSeedFirst(t *testing.T) {
	const a, b, c = uint16(3), uint16(7), uint16(11)
	order := newOrderPuller(t, deficitsOf(func() map[uint16]int { return map[uint16]int{a: 0, b: 2, c: 1} }), a, b, c)
	order.queueAll(a, b, c)
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		order.puller.Run(ctx)
	}()
	defer func() {
		cancel()
		<-finished
	}()
	stallWait(t, "three pulls to ask for a source", func() bool { return len(order.taken()) >= 3 })
	got := order.taken()
	if len(got) != 3 || got[0] != b || got[1] != c || got[2] != a {
		t.Fatalf("the pairs were pulled in slot order %v, want [%d %d %d] (deficits 2, 1, 0)", got, b, c, a)
	}
}

// TestTheQueueIsRerankedWhenTheStateSeqMoves: the deficits change with the committed state, and the
// pair taken next follows the new order even though nothing was added to the queue.
// [BREAK: re-rank only when a pair was added]
func TestTheQueueIsRerankedWhenTheStateSeqMoves(t *testing.T) {
	const a, b, c = uint16(3), uint16(7), uint16(11)
	var table atomic.Pointer[map[uint16]int]
	table.Store(&map[uint16]int{a: 0, b: 2, c: 1})
	order := newOrderPuller(t, deficitsOf(func() map[uint16]int { return *table.Load() }), a, b, c)
	order.queueAll(a, b, c)
	if got := order.takeNext(t); got != b {
		t.Fatalf("the first pair taken is slot %d, want %d (deficit 2)", got, b)
	}
	table.Store(&map[uint16]int{a: 2, b: 0, c: 1})
	order.seq.Add(1)
	order.advance(rerankMinInterval)
	if got := order.takeNext(t); got != a {
		t.Fatalf("after the state moved the next pair taken is slot %d, want %d (deficit 2 now)", got, a)
	}
	if got := order.takeNext(t); got != c {
		t.Fatalf("the last pair taken is slot %d, want %d", got, c)
	}
}

// TestAnUnsetDeficitFunctionKeepsArrivalOrder: a puller with no Deficits takes pairs in the order
// they were queued, whatever their slots.
// [BREAK: rank the queue even when Deficits is unset]
func TestAnUnsetDeficitFunctionKeepsArrivalOrder(t *testing.T) {
	const a, b, c = uint16(3), uint16(7), uint16(11)
	order := newOrderPuller(t, nil, a, b, c)
	order.queueAll(c, a, b)
	for _, want := range []uint16{c, a, b} {
		if got := order.takeNext(t); got != want {
			t.Fatalf("the pair taken is slot %d, want %d (arrival order c, a, b)", got, want)
		}
	}
}

// TestARerankIsAtMostOncePerInterval: a node that seeds thousands of pairs moves state_seq all the time, so
// a move marks the queue dirty and the re-rank waits for the interval; ten moves inside one interval cost one
// re-rank, and the first move after the interval costs the second.
// [BREAK: drop the interval check in Puller.rerankDue]
func TestARerankIsAtMostOncePerInterval(t *testing.T) {
	slots := make([]uint16, 13)
	for i := range slots {
		slots[i] = uint16(i + 1)
	}
	var reranks atomic.Int32
	counting := func() func(slot uint16, db uint64) int {
		reranks.Add(1)
		return func(slot uint16, _ uint64) int { return int(slot) }
	}
	order := newOrderPuller(t, counting, slots...)
	order.queueAll(slots...)
	order.takeNext(t)
	if got := reranks.Load(); got != 1 {
		t.Fatalf("the first ranking of the queue took %d re-ranks, want 1 and immediate", got)
	}
	for range 10 {
		order.seq.Add(1)
		order.advance(rerankMinInterval / 100)
		order.takeNext(t)
	}
	if got := reranks.Load(); got != 1 {
		t.Fatalf("ten state_seq moves inside one interval took %d re-ranks in all, want 1", got)
	}
	order.advance(rerankMinInterval)
	order.takeNext(t)
	if got := reranks.Load(); got != 2 {
		t.Fatalf("the first take after the interval took %d re-ranks in all, want 2", got)
	}
}
