package wire

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// queueEvents is the order in which a copy made records durable and sent acknowledgements.
type queueEvents struct {
	mu   sync.Mutex
	list []string
}

func (e *queueEvents) add(event string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.list = append(e.list, event)
}

func (e *queueEvents) index(event string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, got := range e.list {
		if got == event {
			return i
		}
	}
	return -1
}

func queueEventName(kind string, slot uint16, seq uint64) string {
	return kind + ":" + string(rune('A'+slot)) + ":" + string(rune('0'+seq))
}

// queueGateLog holds appends at the log, like a slow fsync: an append whose key has a gate
// waits for the test to close it, and every append reports when it entered and when it
// became durable.
type queueGateLog struct {
	Log
	events  *queueEvents
	mu      sync.Mutex
	gates   map[string]chan struct{}
	all     chan struct{}
	entered chan string
}

func (l *queueGateLog) gate(slot uint16, seq uint64) chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	ch := make(chan struct{})
	l.gates[string(queueKey(slot, seq))] = ch
	return ch
}

func queueKey(slot uint16, seq uint64) []byte {
	return storage.PositionKey(slot, u32DB, storage.LogPosition(u32Pos(u32Epoch, seq)))
}

func (l *queueGateLog) AppendFrom(bucket string, op byte, key, record []byte, origin storage.WALOrigin) (uint64, error) {
	l.mu.Lock()
	hold := l.gates[string(key)]
	all := l.all
	l.mu.Unlock()
	l.entered <- string(key)
	if hold != nil {
		<-hold
	}
	if all != nil {
		<-all
	}
	cursor, err := l.Log.AppendFrom(bucket, op, key, record, origin)
	if err == nil {
		slot, seq := queueSlotSeq(key)
		l.events.add(queueEventName("durable", slot, seq))
	}
	return cursor, err
}

// queueSlotSeq reads a position key written by queueKey.
func queueSlotSeq(key []byte) (uint16, uint64) {
	for slot := uint16(0); slot < 16; slot++ {
		for seq := uint64(1); seq < 10; seq++ {
			if bytes.Equal(key, queueKey(slot, seq)) {
				return slot, seq
			}
		}
	}
	return 0, 0
}

// queueLink records every acknowledgement a copy sends, in order, and hands it to owner
// when one is set.
type queueLink struct {
	*u32Link
	events *queueEvents
	acks   chan WriteAck
	owner  *Replicator
}

func (l *queueLink) Ack(_ context.Context, _ string, ack WriteAck) error {
	l.events.add(queueEventName("ack", ack.Slot, ack.Pos.Seq))
	l.acks <- ack
	if l.owner != nil {
		l.owner.OnWriteAck(ack)
	}
	return nil
}

type queueCopy struct {
	*u32Node
	log      *queueGateLog
	link     *queueLink
	events   *queueEvents
	catchUps chan pairKey
}

// newQueueCopy is a copy node whose apply workers run, over a log that can hold appends.
func newQueueCopy(t *testing.T, mutate func(*ReplicatorConfig)) *queueCopy {
	t.Helper()
	events := &queueEvents{}
	qc := &queueCopy{events: events, catchUps: make(chan pairKey, 16)}
	qc.u32Node = newU32Node(t, "copy", func(cfg *ReplicatorConfig) {
		qc.log = &queueGateLog{Log: cfg.Log, events: events, gates: map[string]chan struct{}{}, entered: make(chan string, 4096)}
		qc.link = &queueLink{u32Link: newU32Link(), events: events, acks: make(chan WriteAck, 4096)}
		cfg.Log, cfg.Link = qc.log, qc.link
		cfg.RequestCatchUp = func(slot uint16, db uint64) { qc.catchUps <- pairKey{slot, db} }
		if mutate != nil {
			mutate(cfg)
		}
	})
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); qc.rep.Run(ctx) }()
	t.Cleanup(func() {
		qc.logRelease()
		stop()
		<-done
		qc.rep.Close()
	})
	return qc
}

// logRelease opens every gate so a cleanup never waits on a held append.
func (c *queueCopy) logRelease() {
	c.log.mu.Lock()
	defer c.log.mu.Unlock()
	for key, gate := range c.log.gates {
		select {
		case <-gate:
		default:
			close(gate)
		}
		delete(c.log.gates, key)
	}
	if c.log.all != nil {
		select {
		case <-c.log.all:
		default:
			close(c.log.all)
		}
	}
}

func queueFrame(t *testing.T, slot uint16, seq uint64) WriteFrame {
	t.Helper()
	frame := u32Frame(t, u32Epoch, u32Pos(u32Epoch, seq), cluster.Position{})
	frame.Slot = slot
	return frame
}

// queueSend admits and enqueues one frame the way the connection does, and reports whether
// it was queued; a frame that was not comes back with the ack the connection would reply.
func queueSend(t *testing.T, c *queueCopy, slot uint16, seq uint64) (WriteAck, bool) {
	t.Helper()
	adm, err := c.rep.AdmitWrite(queueFrame(t, slot, seq))
	if err != nil {
		t.Fatalf("AdmitWrite %d/%d: %v", slot, seq, err)
	}
	if !adm.admitted {
		return adm.ack, false
	}
	return c.rep.EnqueueWrite(adm)
}

func queueWaitAck(t *testing.T, c *queueCopy) WriteAck {
	t.Helper()
	select {
	case ack := <-c.link.acks:
		return ack
	case <-time.After(10 * time.Second):
		t.Fatal("no acknowledgement arrived")
		return WriteAck{}
	}
}

func queueWaitEntered(t *testing.T, c *queueCopy) {
	t.Helper()
	select {
	case <-c.log.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("no append started")
	}
}

func (c *queueCopy) noAckYet(t *testing.T) {
	t.Helper()
	select {
	case ack := <-c.link.acks:
		t.Fatalf("an acknowledgement %+v was sent before the record was durable", ack)
	default:
	}
}

// TestAQueuedWriteIsAckedOnlyAfterTheFsync (BUG-0745): the copy queues a WRITE and answers
// the connection at once, but the acknowledgement that counts toward the write concern is
// sent only after the append returned and the record was applied.
// [GUARD: send the ack when the frame is queued]
func TestAQueuedWriteIsAckedOnlyAfterTheFsync(t *testing.T) {
	c := newQueueCopy(t, nil)
	release := c.log.gate(5, 1)
	if _, queued := queueSend(t, c, 5, 1); !queued {
		t.Fatal("the frame was not queued")
	}
	queueWaitEntered(t, c)
	c.noAckYet(t)
	if c.events.index(queueEventName("durable", 5, 1)) >= 0 {
		t.Fatal("the record is durable while its append has not returned")
	}
	close(release)
	ack := queueWaitAck(t, c)
	if ack.Outcome != AckApplied || ack.Pos != u32Pos(u32Epoch, 1) || ack.From != "copy" || ack.Slot != 5 {
		t.Fatalf("ack %+v, want applied at 7:1 from copy for slot 5", ack)
	}
	if d, a := c.events.index(queueEventName("durable", 5, 1)), c.events.index(queueEventName("ack", 5, 1)); d < 0 || a < d {
		t.Fatalf("the ack (event %d) was sent before the record was durable (event %d)", a, d)
	}
}

// TestAQueuedApplyKeepsPerPairOrderAndAppliesOtherPairsConcurrently: a pair whose append is
// held does not hold another pair, and a pair's own records apply in position order.
// [GUARD: apply every record on the connection goroutine / one worker for all pairs]
func TestAQueuedApplyKeepsPerPairOrderAndAppliesOtherPairsConcurrently(t *testing.T) {
	c := newQueueCopy(t, nil)
	release := c.log.gate(5, 1)
	for seq := uint64(1); seq <= 3; seq++ {
		if _, queued := queueSend(t, c, 5, seq); !queued {
			t.Fatalf("5/%d was not queued", seq)
		}
	}
	if _, queued := queueSend(t, c, 6, 1); !queued {
		t.Fatal("6/1 was not queued")
	}
	got := queueWaitAck(t, c)
	if got.Slot != 6 || got.Outcome != AckApplied {
		t.Fatalf("with pair 5 held, the first ack is %+v, want pair 6 applied", got)
	}
	if c.events.index(queueEventName("durable", 5, 1)) >= 0 {
		t.Fatal("pair 5 applied while its first append was held")
	}
	close(release)
	var seqs []uint64
	for len(seqs) < 3 {
		if ack := queueWaitAck(t, c); ack.Slot == 5 {
			if ack.Outcome != AckApplied {
				t.Fatalf("pair 5 ack %+v", ack)
			}
			seqs = append(seqs, ack.Pos.Seq)
		}
	}
	if seqs[0] != 1 || seqs[1] != 2 || seqs[2] != 3 {
		t.Fatalf("pair 5 was acknowledged at %v, want 1, 2, 3 in order", seqs)
	}
}

// TestTheCopyQueueAddsNoGoroutinePerPair: frames queued for hundreds of pairs run on the
// fixed worker pool, so the goroutine count does not grow with the pairs.
// [GUARD: start a goroutine for each pair with queued frames]
func TestTheCopyQueueAddsNoGoroutinePerPair(t *testing.T) {
	c := newQueueCopy(t, func(cfg *ReplicatorConfig) { cfg.CopyWorkers = 4 })
	c.log.mu.Lock()
	c.log.all = make(chan struct{})
	c.log.mu.Unlock()
	if _, queued := queueSend(t, c, 0, 1); !queued {
		t.Fatal("0/1 was not queued")
	}
	queueWaitEntered(t, c)
	before := goroutineCount()
	const pairs = 400
	for i := 1; i <= pairs; i++ {
		slot := uint16(100 + i)
		if _, queued := queueSend(t, c, slot, 1); !queued {
			t.Fatalf("%d/1 was not queued", slot)
		}
	}
	if after := goroutineCount(); after > before+2 {
		t.Fatalf("%d queued pairs grew the goroutines from %d to %d", pairs, before, after)
	}
}

// TestACopyQueueOverItsBoundIsRefusedAndTheOwnerRearms: a pair holds at most twice the window
// of frames and the node a byte budget; a frame over either gets the not-applied reply at
// once, and the owner re-arms on it with its backoff instead of buffering without bound.
// [GUARD: no per-pair bound / no byte bound]
func TestACopyQueueOverItsBoundIsRefusedAndTheOwnerRearms(t *testing.T) {
	c := newQueueCopy(t, func(cfg *ReplicatorConfig) {
		cfg.Window = 2
		cfg.CopyQueueBytes = 2 * writeCost(queueFrame(t, 5, 1))
	})
	c.log.mu.Lock()
	c.log.all = make(chan struct{})
	c.log.mu.Unlock()
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) { cfg.RetryBackoff = 10 * time.Millisecond })
	owner.copies = []string{"copy"}
	for seq := uint64(1); seq <= 2; seq++ {
		if _, queued := queueSend(t, c, 5, seq); !queued {
			t.Fatalf("5/%d was refused under both bounds", seq)
		}
	}
	ack, queued := queueSend(t, c, 6, 1)
	if queued || ack.Outcome != AckNotApplied {
		t.Fatalf("a frame over the node's byte budget: queued %v ack %+v, want a not-applied reply", queued, ack)
	}
	// The per-pair bound is twice the window; a byte budget this small hits first, so widen it.
	wide := newQueueCopy(t, func(cfg *ReplicatorConfig) { cfg.Window = 2 })
	wide.log.mu.Lock()
	wide.log.all = make(chan struct{})
	wide.log.mu.Unlock()
	for seq := uint64(1); seq <= 4; seq++ {
		if _, queued := queueSend(t, wide, 5, seq); !queued {
			t.Fatalf("5/%d was refused inside twice the window of 2", seq)
		}
	}
	ack, queued = queueSend(t, wide, 5, 5)
	if queued || ack.Outcome != AckNotApplied {
		t.Fatalf("a fifth frame on a window-2 pair: queued %v ack %+v, want a not-applied reply", queued, ack)
	}
	owner.rep.OnWriteAck(ack)
	owner.rep.mu.Lock()
	rearm := owner.rep.pairs[pairKey{5, u32DB}].peers["copy"].rearmAt
	owner.rep.mu.Unlock()
	if rearm.IsZero() {
		t.Fatal("the owner did not schedule a re-arm after the copy's queue was full")
	}
}

// TestADuplicateFrameIsAckedAtTheAppliedPosition: a frame the copy already holds is not
// appended again, and its ack is the cumulative applied position.
func TestADuplicateFrameIsAckedAtTheAppliedPosition(t *testing.T) {
	c := newQueueCopy(t, nil)
	for seq := uint64(1); seq <= 2; seq++ {
		queueSend(t, c, 5, seq)
		if ack := queueWaitAck(t, c); ack.Outcome != AckApplied || ack.Pos.Seq != seq {
			t.Fatalf("ack %+v for %d", ack, seq)
		}
	}
	queueSend(t, c, 5, 1)
	if ack := queueWaitAck(t, c); ack.Outcome != AckApplied || ack.Pos != u32Pos(u32Epoch, 2) {
		t.Fatalf("the duplicate of 7:1 was acked %+v, want applied and cumulative at 7:2", ack)
	}
	if c.wal.LastSequence() != 2 {
		t.Fatalf("a duplicate grew the log to %d entries", c.wal.LastSequence())
	}
}

// TestAnEpochChangeDropsQueuedRecordsInsteadOfApplyingThem: a frame admitted at one epoch is
// checked again when a worker reaches it; after an epoch change it is answered not-applied and
// never appended, so a new owner never applies the old owner's queued records.
// [GUARD: drop the epoch re-check in the worker]
func TestAnEpochChangeDropsQueuedRecordsInsteadOfApplyingThem(t *testing.T) {
	c := newQueueCopy(t, nil)
	release := c.log.gate(5, 1)
	for seq := uint64(1); seq <= 3; seq++ {
		queueSend(t, c, 5, seq)
	}
	queueWaitEntered(t, c)
	c.committed.Store(u32Epoch + 1)
	close(release)
	if ack := queueWaitAck(t, c); ack.Outcome != AckApplied || ack.Pos != u32Pos(u32Epoch, 1) {
		t.Fatalf("first ack %+v, want 7:1 applied (it was appending before the change)", ack)
	}
	if ack := queueWaitAck(t, c); ack.Outcome != AckNotApplied || ack.Pos != u32Pos(u32Epoch, 1) {
		t.Fatalf("second ack %+v, want not-applied at 7:1", ack)
	}
	if c.applied() != u32Pos(u32Epoch, 1) || c.wal.LastSequence() != 1 || c.hasDocument(u32Pos(u32Epoch, 2)) {
		t.Fatalf("a queued record of the old epoch was applied: applied %s, log %d", c.applied(), c.wal.LastSequence())
	}
}

// TestAGapInTheQueueClearsThePairAndAsksForACatchUp: a record that does not follow the applied
// one is refused, the pair's other queued records are dropped, and the copy asks for a
// catch-up. [GUARD: skip the catch-up request; the clearing of the queue behind the gap is TestAGapClearsTheRecordsQueuedBehindIt]
func TestAGapInTheQueueClearsThePairAndAsksForACatchUp(t *testing.T) {
	c := newQueueCopy(t, nil)
	release := c.log.gate(5, 1)
	queueSend(t, c, 5, 1)
	queueWaitEntered(t, c)
	queueSend(t, c, 5, 3)
	queueSend(t, c, 5, 4)
	close(release)
	if ack := queueWaitAck(t, c); ack.Outcome != AckApplied || ack.Pos.Seq != 1 {
		t.Fatalf("first ack %+v", ack)
	}
	if ack := queueWaitAck(t, c); ack.Outcome != AckRefused || ack.Pos.Seq != 1 {
		t.Fatalf("the gap was answered %+v, want refused with the copy still at 7:1", ack)
	}
	select {
	case key := <-c.catchUps:
		if key != (pairKey{5, u32DB}) {
			t.Fatalf("catch-up asked for %+v", key)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a gap did not ask for a catch-up")
	}
	if c.applied() != u32Pos(u32Epoch, 1) || c.wal.LastSequence() != 1 {
		t.Fatalf("a record behind the gap was applied: applied %s, log %d", c.applied(), c.wal.LastSequence())
	}
}

// queueWaitIdle waits until no pair has frames queued or being applied.
func queueWaitIdle(t *testing.T, c *queueCopy) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		c.rep.copyQ.mu.Lock()
		idle := len(c.rep.copyQ.pairs) == 0 && c.rep.copyQ.bytes == 0
		c.rep.copyQ.mu.Unlock()
		if idle {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the copy queue never went idle")
		}
		time.Sleep(time.Millisecond) // polling for the end of background work, not ordering logic
	}
}

// TestACatchUpRacingTheLiveQueueStillAppliesOnce: a catch-up chunk and the live queue hold the
// same records; the pair lock and the sequence rules apply each once.
func TestACatchUpRacingTheLiveQueueStillAppliesOnce(t *testing.T) {
	c := newQueueCopy(t, nil)
	release := c.log.gate(5, 1)
	queueSend(t, c, 5, 1)
	queueSend(t, c, 5, 2)
	queueWaitEntered(t, c)
	chunk := CatchUpChunk{Records: []PairRecord{queueFrame(t, 5, 1).PairRecord, queueFrame(t, 5, 2).PairRecord}}
	done := make(chan error, 1)
	go func() {
		_, err := c.rep.applyChunk(context.Background(), "owner", CatchUpRequest{Slot: 5, DB: u32DB}, chunk, true)
		done <- err
	}()
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("catch-up: %v", err)
	}
	acks := 0
	for acks < 2 {
		queueWaitAck(t, c)
		acks++
	}
	if c.wal.LastSequence() != 2 || c.applied() != u32Pos(u32Epoch, 2) {
		t.Fatalf("the records were applied %d times, applied %s", c.wal.LastSequence(), c.applied())
	}
}

// ---- the owner's side ----------------------------------------------------------------

type queueClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *queueClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *queueClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newQueueOwner(t *testing.T, window int, copies ...string) (*u32Node, *queueClock) {
	t.Helper()
	clock := &queueClock{now: time.Unix(1_000_000, 0)}
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) {
		cfg.Now, cfg.AckTimeout, cfg.Window = clock.Now, 10*time.Second, window
	})
	owner.copies = copies
	owner.ownRun(u32Epoch, 1, 3)
	owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, 3))
	if err := owner.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	return owner, clock
}

func (n *u32Node) retryAndFlush(t *testing.T) {
	t.Helper()
	n.rep.Retry()
	if err := n.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestALostAckIsRecoveredByTheAckDeadline: the WRITE reply is a receipt, so an ack that is
// lost leaves frames in flight with nothing to wake them. After the ack deadline the owner
// ships them again from the last acknowledged position. [GUARD: no ack deadline]
func TestALostAckIsRecoveredByTheAckDeadline(t *testing.T) {
	owner, clock := newQueueOwner(t, 64, "copy")
	if got := len(owner.link.sent("copy")); got != 3 {
		t.Fatalf("first shipment sent %d frames, want 3", got)
	}
	clock.advance(9 * time.Second)
	owner.retryAndFlush(t)
	if got := len(owner.link.sent("copy")); got != 3 {
		t.Fatalf("before the deadline the owner sent %d frames in all, want still 3", got)
	}
	clock.advance(2 * time.Second)
	owner.retryAndFlush(t)
	sent := owner.link.sent("copy")
	if len(sent) != 6 || sent[3].Pos != u32Pos(u32Epoch, 1) {
		t.Fatalf("after the deadline %d frames were sent in all, want the 3 again from 7:1", len(sent))
	}
}

// TestAnAckDeadlineClearedByRetryDuringTheReadIsArmedWhenTheFramesGoOut: a pass reads a pair, a
// Retry tick falls before the frames are sent and clears the read's deadline (nothing is in flight
// yet), and the frames then go out. They must carry a deadline of their own, or a lost ack leaves
// them in flight with nothing to wake the pair (BUG-0866). [GUARD: arm at the read]
func TestAnAckDeadlineClearedByRetryDuringTheReadIsArmedWhenTheFramesGoOut(t *testing.T) {
	t.Run("one pair at a time", func(t *testing.T) {
		clock := &queueClock{now: time.Unix(1_000_000, 0)}
		owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) {
			cfg.Now, cfg.AckTimeout = clock.Now, 10*time.Second
		})
		owner.copies = []string{"copy"}
		owner.ownRun(u32Epoch, 1, 3)
		owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, 3))
		owner.rep.mu.Lock()
		clear(owner.rep.dirty)
		owner.rep.mu.Unlock()

		sh, ok, err := owner.rep.readShipment(context.Background(), shipKey, "copy")
		if err != nil || !ok {
			t.Fatalf("readShipment = ok %v, err %v, want a shipment", ok, err)
		}
		clock.advance(time.Second)
		owner.rep.Retry()
		if err := owner.rep.sendShipment(context.Background(), sh); err != nil {
			t.Fatal(err)
		}
		if got := len(owner.link.sent("copy")); got != 3 {
			t.Fatalf("the first send put %d frames on the link, want 3", got)
		}
		clock.advance(11 * time.Second)
		owner.retryAndFlush(t)
		if got := len(owner.link.sent("copy")); got != 6 {
			t.Fatalf("after the deadline %d frames were sent in all, want the 3 again (6)", got)
		}
	})
	t.Run("one frame for the whole pass", func(t *testing.T) {
		clock := &queueClock{now: time.Unix(1_000_000, 0)}
		owner, link := batchOwner(t, batchSlots(1), 3, func(cfg *ReplicatorConfig) {
			cfg.Now, cfg.AckTimeout = clock.Now, 10*time.Second
		})
		key := pairKey{batchSlots(1)[0], u32DB}
		owner.rep.mu.Lock()
		clear(owner.rep.dirty)
		owner.rep.mu.Unlock()

		sh, ok, err := owner.rep.readShipment(context.Background(), key, "copy")
		if err != nil || !ok {
			t.Fatalf("readShipment = ok %v, err %v, want a shipment", ok, err)
		}
		clock.advance(time.Second)
		owner.rep.Retry()
		if err := owner.rep.sendShipments(context.Background(), "copy", []shipment{sh}); err != nil {
			t.Fatal(err)
		}
		if got := len(link.sent("copy")); got != 3 {
			t.Fatalf("the first send put %d frames on the link, want 3", got)
		}
		clock.advance(11 * time.Second)
		owner.rep.Retry()
		owner.shipAll("copy")
		if got := len(link.sent("copy")); got != 6 {
			t.Fatalf("after the deadline %d frames were sent in all, want the 3 again (6)", got)
		}
	})
}

// TestAFullWindowWithNoAckIsRecoveredByTheAckDeadline: the same with the window full, which
// is where a lost ack stops the pair for good. [GUARD: no ack deadline]
func TestAFullWindowWithNoAckIsRecoveredByTheAckDeadline(t *testing.T) {
	owner, clock := newQueueOwner(t, 2, "copy")
	if got := len(owner.link.sent("copy")); got != 2 {
		t.Fatalf("a window of 2 sent %d frames", got)
	}
	clock.advance(11 * time.Second)
	owner.retryAndFlush(t)
	if got := len(owner.link.sent("copy")); got != 4 {
		t.Fatalf("after the deadline %d frames were sent in all, want 4", got)
	}
}

// TestTheAckDeadlineIsPerCopy: one copy's acks do not cancel another copy's deadline at RF 3.
// [GUARD: key the retry set by pair only]
func TestTheAckDeadlineIsPerCopy(t *testing.T) {
	owner, clock := newQueueOwner(t, 64, "a", "b")
	owner.rep.OnWriteAck(WriteAck{From: "a", Slot: u32Slot, DB: u32DB, Pos: u32Pos(u32Epoch, 3), Outcome: AckApplied})
	clock.advance(11 * time.Second)
	owner.retryAndFlush(t)
	if a, b := len(owner.link.sent("a")), len(owner.link.sent("b")); a != 3 || b != 6 {
		t.Fatalf("a was sent %d frames and b %d, want a untouched (3) and b shipped again (6)", a, b)
	}
}

// TestAnAckThatMakesProgressRenewsTheDeadline: a slow copy that keeps acknowledging is not
// rewound; the deadline is for silence. [GUARD: fix the deadline at the first send]
func TestAnAckThatMakesProgressRenewsTheDeadline(t *testing.T) {
	owner, clock := newQueueOwner(t, 64, "copy")
	clock.advance(6 * time.Second)
	owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Pos: u32Pos(u32Epoch, 1), Outcome: AckApplied})
	clock.advance(6 * time.Second)
	owner.retryAndFlush(t)
	if got := len(owner.link.sent("copy")); got != 3 {
		t.Fatalf("a copy that acked 6 s ago was shipped to again: %d frames sent in all", got)
	}
}

// TestABurstOfNotAppliedAcksIsOneBackoffStep: a copy that drops its queue answers each frame
// it had queued; the owner takes the burst as one answer, not as a dozen steps up the backoff.
// [GUARD: count every ack of the burst]
func TestABurstOfNotAppliedAcksIsOneBackoffStep(t *testing.T) {
	owner, _ := newQueueOwner(t, 64, "copy")
	for range 3 {
		owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Outcome: AckNotApplied})
	}
	owner.rep.mu.Lock()
	steps := owner.rep.pairs[pairKey{u32Slot, u32DB}].peers["copy"].backoffs
	owner.rep.mu.Unlock()
	if steps != 1 {
		t.Fatalf("three answers to three queued frames counted %d backoff steps, want 1", steps)
	}
}

// ---- the receipt ---------------------------------------------------------------------

// TestAnEmptyWriteReplyIsAReceiptAndNeverAnAck: the reply to a WRITE is empty when the copy
// queued the frame, which is not an acknowledgement; a 0x02 reply is the not-applied answer
// and goes to the replicator; anything else is refused. [GUARD: treat the receipt as applied]
func TestAnEmptyWriteReplyIsAReceiptAndNeverAnAck(t *testing.T) {
	var got []WriteAck
	links := &peerLinks{onAck: func(ack WriteAck) { got = append(got, ack) }}
	frame := WriteFrame{Slot: 5, DB: u32DB, PairRecord: PairRecord{Pos: u32Pos(u32Epoch, 1)}}
	if err := links.readWriteReply("copy", frame, nil); err != nil || len(got) != 0 {
		t.Fatalf("an empty reply: err %v, acks %v, want a receipt and no ack", err, got)
	}
	body := EncodeWriteAckBody(WriteAck{Pos: u32Pos(u32Epoch, 0), Outcome: AckNotApplied})
	reply := transportReply(0x02, body)
	if err := links.readWriteReply("copy", frame, reply); err != nil || len(got) != 1 || got[0].Outcome != AckNotApplied || got[0].From != "copy" || got[0].Slot != 5 {
		t.Fatalf("a not-applied reply: err %v, acks %+v", err, got)
	}
	if err := links.readWriteReply("copy", frame, transportReply(0x02, []byte{1, 2, 3})); err == nil || len(got) != 1 {
		t.Fatalf("a malformed ack reply: err %v, acks %+v, want a refusal", err, got)
	}
	if err := links.readWriteReply("copy", frame, transportReply(0x04, body)); err == nil || len(got) != 1 {
		t.Fatalf("a reply of the wrong kind: err %v, acks %+v, want a refusal", err, got)
	}
}

func transportReply(kind byte, body []byte) *transport.Message {
	return &transport.Message{Env: transport.Envelope{Version: transport.EnvelopeVersion, Kind: kind}, Body: body}
}

// TestAdmissionNeverWaitsForAWorkersFsync (BUG-0745 review D7): admission runs on the copy's one
// connection goroutine, so a refusal for a pair whose worker is inside its append must not take
// the pair lock. A seeding copy, an old-epoch frame and a full queue all answer at once.
// [GUARD: read the applied position or feed the refusal to the machine under the pair lock]
func TestAdmissionNeverWaitsForAWorkersFsync(t *testing.T) {
	c := newQueueCopy(t, nil)
	release := c.log.gate(5, 1)
	if _, queued := queueSend(t, c, 5, 1); !queued {
		t.Fatal("5/1 was not queued")
	}
	queueWaitEntered(t, c)
	answered := make(chan string, 3)
	go func() {
		c.ready.Store(false)
		adm, err := c.rep.AdmitWrite(queueFrame(t, 5, 2))
		if err != nil || adm.admitted || adm.ack.Outcome != AckNotApplied {
			answered <- "seeding copy: not a plain not-applied answer"
			return
		}
		c.ready.Store(true)
		old := queueFrame(t, 5, 2)
		old.Epoch = u32Epoch + 1
		if _, err := c.rep.AdmitWrite(old); u32Code(err) != cluster.CodeEpochFenced {
			answered <- "other epoch: not EPOCH_FENCED"
			return
		}
		broken := queueFrame(t, 5, 2)
		broken.Record.Body = broken.Record.Body[:5]
		if _, err := c.rep.AdmitWrite(broken); u32Code(err) != cluster.CodeInvalidArgument {
			answered <- "undecodable: not INVALID_ARGUMENT"
			return
		}
		answered <- ""
	}()
	select {
	case msg := <-answered:
		if msg != "" {
			t.Fatal(msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an admission refusal waited for the worker's append")
	}
	close(release)
	queueWaitAck(t, c)
}

// TestAGapClearsTheRecordsQueuedBehindIt: the worker is inside the sequence rules for 5/3 (the
// test holds the pair lock) when 5/4 is queued; the gap drops it, so exactly one refusal and one
// catch-up request follow. A queue that was kept would refuse 5/4 as a second gap.
// [GUARD: keep the pair's queue when a record is not applied]
func TestAGapClearsTheRecordsQueuedBehindIt(t *testing.T) {
	c := newQueueCopy(t, nil)
	queueSend(t, c, 5, 1)
	if ack := queueWaitAck(t, c); ack.Outcome != AckApplied {
		t.Fatalf("first ack %+v", ack)
	}
	key := pairKey{5, u32DB}
	c.pairs.With(5, u32DB, func(*cluster.Pair) {
		queueSend(t, c, 5, 3)
		deadline := time.Now().Add(10 * time.Second)
		for { // the worker has taken 5/3 and waits for the lock we hold
			c.rep.copyQ.mu.Lock()
			pq := c.rep.copyQ.pairs[key]
			taken := pq != nil && pq.active && len(pq.items) == 0
			c.rep.copyQ.mu.Unlock()
			if taken {
				break
			}
			if time.Now().After(deadline) {
				t.Error("the worker never took 5/3")
				return
			}
			time.Sleep(time.Millisecond) // polling for a worker to reach a lock
		}
		queueSend(t, c, 5, 4)
	})
	if ack := queueWaitAck(t, c); ack.Outcome != AckRefused {
		t.Fatalf("5/3 was answered %+v, want refused", ack)
	}
	queueWaitIdle(t, c)
	select {
	case ack := <-c.link.acks:
		t.Fatalf("a second ack %+v followed the gap: the queue behind it was not cleared", ack)
	default:
	}
	<-c.catchUps
	select {
	case got := <-c.catchUps:
		t.Fatalf("a second catch-up %+v was asked for", got)
	default:
	}
}

// TestABusyPairGivesUpItsTurnAfterOneWindow (BUG-0745 review L1): a worker applies at most one
// window of a pair's frames, then the pair goes to the back, so a small pair queued behind a full
// one is not made to wait for all of it. One worker, window 2. [GUARD: drain the whole queue]
func TestABusyPairGivesUpItsTurnAfterOneWindow(t *testing.T) {
	c := newQueueCopy(t, func(cfg *ReplicatorConfig) { cfg.CopyWorkers, cfg.Window = 1, 2 })
	hold := c.log.gate(7, 1)
	queueSend(t, c, 7, 1) // the only worker is inside this append
	queueWaitEntered(t, c)
	for seq := uint64(1); seq <= 4; seq++ {
		if _, queued := queueSend(t, c, 5, seq); !queued {
			t.Fatalf("5/%d was refused", seq)
		}
	}
	queueSend(t, c, 6, 1)
	close(hold)
	var order []string
	for len(order) < 6 {
		ack := queueWaitAck(t, c)
		order = append(order, queueEventName("ack", ack.Slot, ack.Pos.Seq))
	}
	b, a3 := indexOf(order, queueEventName("ack", 6, 1)), indexOf(order, queueEventName("ack", 5, 3))
	if b < 0 || a3 < 0 || b > a3 {
		t.Fatalf("acks arrived in the order %v: pair 6 waited for all four frames of pair 5, want it after at most the first window", order)
	}
}

func indexOf(list []string, want string) int {
	for i, got := range list {
		if got == want {
			return i
		}
	}
	return -1
}

// TestASeedingCopyNeverQueuesAWrite: a pair that is not ready answers not-applied in the reply and
// queues nothing; a frame queued before the pair began seeding is answered not-applied by the worker,
// never appended. [GUARD: skip the Ready check in admission / in the worker]
func TestASeedingCopyNeverQueuesAWrite(t *testing.T) {
	c := newQueueCopy(t, nil)
	c.ready.Store(false)
	adm, err := c.rep.AdmitWrite(queueFrame(t, 5, 1))
	if err != nil || adm.Admitted() || adm.ack.Outcome != AckNotApplied {
		t.Fatalf("a seeding copy admitted a write: %+v err %v", adm, err)
	}
	c.rep.copyQ.mu.Lock()
	queued := len(c.rep.copyQ.pairs)
	c.rep.copyQ.mu.Unlock()
	if queued != 0 || c.wal.LastSequence() != 0 {
		t.Fatalf("a seeding copy queued %d pairs and wrote %d log entries", queued, c.wal.LastSequence())
	}
	c.ready.Store(true)
	release := c.log.gate(5, 1)
	queueSend(t, c, 5, 1)
	queueWaitEntered(t, c)
	queueSend(t, c, 5, 2)
	c.ready.Store(false) // the pair begins seeding while 5/2 waits
	close(release)
	if ack := queueWaitAck(t, c); ack.Outcome != AckApplied {
		t.Fatalf("first ack %+v", ack)
	}
	if ack := queueWaitAck(t, c); ack.Outcome != AckNotApplied {
		t.Fatalf("a frame queued before seeding began was answered %+v, want not-applied", ack)
	}
	if c.wal.LastSequence() != 1 {
		t.Fatalf("a record was appended to a pair that began seeding: log %d", c.wal.LastSequence())
	}
}
