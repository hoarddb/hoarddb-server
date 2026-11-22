package wire

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// writeBatchLink is a Link that can also take a frame of records: every WriteBatch call is kept
// whole, so a test sees how the owner cut its pass, and a call that does not fail is also handed
// to u32Link.frames so the helpers that read what reached the copy still work.
type writeBatchLink struct {
	*u32Link
	batchMu sync.Mutex
	calls   [][]WriteFrame
	// failCall, when set, is the 1-based call that fails after being kept; its frames never reach
	// the copy, as a frame lost on the wire.
	failCall int
}

func (l *writeBatchLink) WriteBatch(_ context.Context, peer string, frames []WriteFrame) error {
	l.batchMu.Lock()
	l.calls = append(l.calls, slices.Clone(frames))
	failing := len(l.calls) == l.failCall
	l.batchMu.Unlock()
	if failing {
		return errors.New("the connection to the copy broke")
	}
	l.mu.Lock()
	l.frames[peer] = append(l.frames[peer], frames...)
	l.mu.Unlock()
	return nil
}

func (l *writeBatchLink) sentCalls() [][]WriteFrame {
	l.batchMu.Lock()
	defer l.batchMu.Unlock()
	return slices.Clone(l.calls)
}

// batchSlots are the slots of the pairs a test ships; every pair has database u32DB.
func batchSlots(count int) []uint16 {
	slots := make([]uint16, count)
	for index := range slots {
		slots[index] = uint16(10 + index)
	}
	return slots
}

// batchOwner is an owner whose link batches and whose pairs, one per slot, each hold records
// 1 to perPair in the log. Every pair is queued for the copy named "copy".
func batchOwner(t *testing.T, slots []uint16, perPair uint64, mutate func(*ReplicatorConfig)) (*u32Node, *writeBatchLink) {
	t.Helper()
	var link *writeBatchLink
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) {
		link = &writeBatchLink{u32Link: newU32Link()}
		cfg.Link = link
		if mutate != nil {
			mutate(cfg)
		}
	})
	owner.copies = []string{"copy"}
	for _, slot := range slots {
		for seq := uint64(1); seq <= perPair; seq++ {
			owner.ownAppend(slot, u32Pos(u32Epoch, seq), cluster.Position{})
		}
		owner.rep.Push(slot, u32DB, u32Pos(u32Epoch, perPair))
	}
	return owner, link
}

// shipPass runs one lane pass over the pairs queued now, the way dispatch and runLane do but on
// this goroutine, so the pass is whole when the test looks at it. It reports whether any pair was
// queued.
func (n *u32Node) shipPass(peer string) bool {
	n.t.Helper()
	lane := &sendLane{busy: true, queue: make(map[pairKey]struct{})}
	n.rep.mu.Lock()
	for key := range n.rep.dirty {
		lane.queue[key] = struct{}{}
	}
	clear(n.rep.dirty)
	n.rep.mu.Unlock()
	if len(lane.queue) == 0 {
		return false
	}
	n.rep.laneWG.Add(1)
	n.rep.runLane(context.Background(), peer, lane)
	return true
}

// shipAll runs passes until no pair is queued.
func (n *u32Node) shipAll(peer string) {
	n.t.Helper()
	for n.shipPass(peer) {
	}
}

// positionsBySlot lists, per pair, the log sequences of the frames in the order they were sent.
func positionsBySlot(calls [][]WriteFrame) map[uint16][]uint64 {
	sent := make(map[uint16][]uint64)
	for _, call := range calls {
		for _, frame := range call {
			sent[frame.Slot] = append(sent[frame.Slot], frame.Pos.Seq)
		}
	}
	return sent
}

// recordBodyBytes is the size of the record body every test record has.
func recordBodyBytes(t *testing.T) int {
	t.Helper()
	return len(u32Record(t, u32Pos(u32Epoch, 2), cluster.Position{}).Record.Body)
}

// TestRecordsOfSeveralPairsLeaveInOneFrame: the lane's pass is one call however many pairs it
// holds, which is what lets 16 writers on 16 pairs share one round trip (I2, I7).
// [BREAK: sendShipments calls WriteBatch once per record]
func TestRecordsOfSeveralPairsLeaveInOneFrame(t *testing.T) {
	slots := batchSlots(16)
	owner, link := batchOwner(t, slots, 1, nil)
	owner.shipAll("copy")

	calls := link.sentCalls()
	if len(calls) != 1 || len(calls[0]) != 16 {
		sizes := make([]int, len(calls))
		for index, call := range calls {
			sizes[index] = len(call)
		}
		t.Fatalf("16 pairs of one record left in %d calls of sizes %v, want one call of 16 frames", len(calls), sizes)
	}
	seen := make(map[uint16]bool)
	for _, frame := range calls[0] {
		seen[frame.Slot] = true
	}
	if len(seen) != 16 {
		t.Fatalf("the call carried frames of %d distinct pairs, want 16", len(seen))
	}
}

// TestABatchKeepsEachPairsRecordsInPositionOrder: whatever order the pairs come in, one pair's
// frames are ascending and consecutive across the whole pass, also when the byte cap cuts the
// pass in the middle of a pair (I2).
// [BREAK: cut chunks from the records interleaved across pairs in reverse]
func TestABatchKeepsEachPairsRecordsInPositionOrder(t *testing.T) {
	body := recordBodyBytes(t)
	for _, tc := range []struct {
		name       string
		chunkBytes int
		perPair    uint64
		wantCalls  int
	}{
		{"one frame", 0, 5, 1},
		// 4 bodies and a half: 4 entries a chunk, so 9 records are cut at 4, 4 and 1.
		{"cut by bytes", 4*body + body/2, 3, 3},
	} {
		owner, link := batchOwner(t, batchSlots(3), tc.perPair, func(cfg *ReplicatorConfig) { cfg.ChunkBytes = tc.chunkBytes })
		owner.shipAll("copy")

		calls := link.sentCalls()
		if len(calls) != tc.wantCalls {
			t.Fatalf("%s: %d calls, want %d", tc.name, len(calls), tc.wantCalls)
		}
		sent := positionsBySlot(calls)
		if len(sent) != 3 {
			t.Fatalf("%s: frames of %d pairs, want 3", tc.name, len(sent))
		}
		for slot, seqs := range sent {
			for index, seq := range seqs {
				if seq != uint64(index+1) {
					t.Fatalf("%s: slot %d was sent sequences %v, want 1 to %d ascending and consecutive", tc.name, slot, seqs, tc.perPair)
				}
			}
			if uint64(len(seqs)) != tc.perPair {
				t.Fatalf("%s: slot %d was sent %d frames, want %d", tc.name, slot, len(seqs), tc.perPair)
			}
		}
		for index, call := range calls {
			if size := bodyBytesOf(call); tc.chunkBytes > 0 && size > tc.chunkBytes {
				t.Fatalf("%s: call %d carried %d body bytes, over the cap of %d", tc.name, index, size, tc.chunkBytes)
			}
		}
	}
}

func bodyBytesOf(frames []WriteFrame) int {
	size := 0
	for _, frame := range frames {
		size += len(frame.Record.Body)
	}
	return size
}

// TestAFrameCarriesOneEpochAndNoMoreThanTheCap: 300 records of five pairs leave as 256 and 44,
// and every frame of the pass carries the epoch read once, however many calls it took (I9).
// The committed epoch moves each time it is read, so a second read shows as a second epoch.
// [BREAK: cap of 257 entries; read the epoch again for each chunk]
func TestAFrameCarriesOneEpochAndNoMoreThanTheCap(t *testing.T) {
	var reads atomic.Uint64
	owner, link := batchOwner(t, batchSlots(5), 60, func(cfg *ReplicatorConfig) {
		cfg.Committed = func() uint64 { return u32Epoch + reads.Add(1) }
	})
	owner.shipAll("copy")

	calls := link.sentCalls()
	if len(calls) != 2 || len(calls[0]) != maxBatchEntries || len(calls[1]) != 300-maxBatchEntries {
		sizes := make([]int, len(calls))
		for index, call := range calls {
			sizes[index] = len(call)
		}
		t.Fatalf("300 records left in calls of sizes %v, want %d and %d", sizes, maxBatchEntries, 300-maxBatchEntries)
	}
	epoch := calls[0][0].Epoch
	for index, call := range calls {
		for _, frame := range call {
			if frame.Epoch != epoch {
				t.Fatalf("call %d carries epoch %d beside epoch %d: a pass reads the committed epoch once", index, frame.Epoch, epoch)
			}
		}
	}
	if got := reads.Load(); got != 1 {
		t.Fatalf("the committed epoch was read %d times for one pass, want 1", got)
	}
}

// TestABatchNeverExceedsAPairsWindow: a pair with ten records and a window of four has four
// frames in flight and no more, however many passes run, because the read asks for the room
// and the chunks are cut from what was read (I7).
// [BREAK: readShipment asks for room+1 records]
func TestABatchNeverExceedsAPairsWindow(t *testing.T) {
	owner, link := batchOwner(t, batchSlots(1), 10, func(cfg *ReplicatorConfig) { cfg.Window = 4 })
	owner.shipAll("copy")
	owner.rep.Push(batchSlots(1)[0], u32DB, u32Pos(u32Epoch, 10))
	owner.shipAll("copy")

	sent := 0
	for _, call := range link.sentCalls() {
		sent += len(call)
	}
	if sent != 4 {
		t.Fatalf("%d frames were sent for a pair with a window of 4 and no acks, want 4", sent)
	}
	owner.rep.mu.Lock()
	defer owner.rep.mu.Unlock()
	if held := len(owner.rep.pairs[pairKey{batchSlots(1)[0], u32DB}].peers["copy"].inflight); held != 4 {
		t.Fatalf("%d frames are in flight, want 4", held)
	}
}

// TestAFailedFrameRewindsEveryPairInItAndLater: the second of three calls fails. Every pair with
// a record in it or in the call that never started is rewound to its last ack with nothing in
// flight, a pair wholly in the first call is left alone, and the next pass ships what is
// missing, so each record applies once on a copy that is handed the frames that arrived (I1, I10).
// [BREAK: rewind only the first pair of the failed chunk]
func TestAFailedFrameRewindsEveryPairInItAndLater(t *testing.T) {
	body := recordBodyBytes(t)
	slots := batchSlots(4)
	var errs []error
	owner, link := batchOwner(t, slots, 3, func(cfg *ReplicatorConfig) {
		// 4 entries a chunk: 12 records leave as 4, 4 and 4.
		cfg.ChunkBytes = 4*body + body/2
		cfg.OnError = func(err error) { errs = append(errs, err) }
	})
	link.failCall = 2
	owner.shipPass("copy")

	calls := link.sentCalls()
	if len(calls) != 2 {
		t.Fatalf("%d calls were made, want 2: the pass stops at the first failed call", len(calls))
	}
	if len(errs) != 1 {
		t.Fatalf("%d errors were reported for one failed call, want 1: %v", len(errs), errs)
	}
	for _, want := range []string{"copy", "4 WRITEs"} {
		if !strings.Contains(errs[0].Error(), want) {
			t.Errorf("the error %q does not name %q", errs[0], want)
		}
	}
	firstCall := positionsBySlot(calls[:1])
	rewound, untouched := 0, 0
	owner.rep.mu.Lock()
	for _, slot := range slots {
		st := owner.rep.pairs[pairKey{slot, u32DB}].peers["copy"]
		if len(firstCall[slot]) == 3 {
			untouched++
			if len(st.inflight) != 3 || st.generation != 0 {
				t.Errorf("slot %d lies wholly in the call that succeeded and has %d in flight at generation %d, want 3 and 0", slot, len(st.inflight), st.generation)
			}
			continue
		}
		rewound++
		if len(st.inflight) != 0 || st.generation == 0 || st.next != 1 || !st.shipped.Zero() {
			t.Errorf("slot %d has a record in the failed call or after it and has %d in flight, generation %d, next %d, shipped %s: want it rewound to the start with nothing in flight",
				slot, len(st.inflight), st.generation, st.next, st.shipped)
		}
		if _, queued := owner.rep.dirty[pairKey{slot, u32DB}]; !queued {
			t.Errorf("slot %d was rewound and not queued to ship again", slot)
		}
	}
	owner.rep.mu.Unlock()
	if untouched != 1 || rewound != 3 {
		t.Fatalf("%d pairs were left alone and %d rewound, want 1 and 3", untouched, rewound)
	}

	link.batchMu.Lock()
	link.failCall = 0
	link.batchMu.Unlock()
	owner.shipAll("copy")

	copyNode := newU32Node(t, "copy", nil)
	copyNode.ownerID = "owner"
	for _, frame := range link.sent("copy") {
		if _, err := copyNode.rep.OnWrite(frame); err != nil {
			t.Fatalf("OnWrite of %s in slot %d: %v", frame.Pos, frame.Slot, err)
		}
	}
	for _, slot := range slots {
		if got := copyNode.pairs.Snapshot(slot, u32DB).AppliedPos; got != u32Pos(u32Epoch, 3) {
			t.Errorf("slot %d is applied at %s on the copy, want 7:3", slot, got)
		}
	}
	if got := copyNode.wal.LastSequence(); got != 12 {
		t.Fatalf("the copy's log holds %d records, want 12: each applied once", got)
	}
}

// TestAPairRefusedBetweenReadAndSendIsNotSent: three pairs are read; before the send one copy
// answers refused (rearmAt is set) and another was rewound (its generation moved). Neither
// pair's records go on the link nor into flight, and the third pair's do (I5, I10).
// [BREAK: sendShipments skips the generation and rearmAt filter]
func TestAPairRefusedBetweenReadAndSendIsNotSent(t *testing.T) {
	slots := batchSlots(3)
	owner, link := batchOwner(t, slots, 2, nil)
	owner.rep.mu.Lock()
	clear(owner.rep.dirty)
	owner.rep.mu.Unlock()

	var shipments []shipment
	for _, slot := range slots {
		sh, ok, err := owner.rep.readShipment(context.Background(), pairKey{slot, u32DB}, "copy")
		if err != nil || !ok {
			t.Fatalf("readShipment of slot %d = ok %v, err %v, want a shipment", slot, ok, err)
		}
		shipments = append(shipments, sh)
	}
	owner.rep.mu.Lock()
	shipments[0].st.rearmAt = owner.rep.cfg.Now().Add(time.Minute)
	shipments[1].st.generation++
	owner.rep.mu.Unlock()

	if err := owner.rep.sendShipments(context.Background(), "copy", shipments); err != nil {
		t.Fatalf("sendShipments: %v", err)
	}
	sent := positionsBySlot(link.sentCalls())
	if len(sent) != 1 || len(sent[slots[2]]) != 2 {
		t.Fatalf("frames were sent for %v, want only the two records of slot %d", sent, slots[2])
	}
	owner.rep.mu.Lock()
	defer owner.rep.mu.Unlock()
	for index, want := range []int{0, 0, 2} {
		if held := len(shipments[index].st.inflight); held != want {
			t.Errorf("slot %d has %d frames in flight, want %d", slots[index], held, want)
		}
	}
}

// plainLink is a Link with no WriteBatch: the shape of every link before the peer link learns to
// batch, and of the test fakes that must keep their meaning.
type plainLink struct {
	*u32Link
	writes atomic.Int64
}

func (l *plainLink) Write(ctx context.Context, peer string, frame WriteFrame) error {
	l.writes.Add(1)
	return l.u32Link.Write(ctx, peer, frame)
}

// TestWithoutABatchLinkTheLaneShipsOneRecordPerCall: a link that cannot batch is shipped to one
// record per Write, as before this build, and the Replicator holds no batcher for it.
// [BREAK: runLane always batches]
func TestWithoutABatchLinkTheLaneShipsOneRecordPerCall(t *testing.T) {
	var link *plainLink
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) {
		link = &plainLink{u32Link: newU32Link()}
		cfg.Link = link
	})
	owner.copies = []string{"copy"}
	slots := batchSlots(3)
	for _, slot := range slots {
		owner.ownAppend(slot, u32Pos(u32Epoch, 1), cluster.Position{})
		owner.ownAppend(slot, u32Pos(u32Epoch, 2), cluster.Position{})
		owner.rep.Push(slot, u32DB, u32Pos(u32Epoch, 2))
	}
	if owner.rep.writeBatch != nil {
		t.Fatal("a link without WriteBatch left the Replicator with a batcher")
	}
	owner.shipAll("copy")

	if got := link.writes.Load(); got != 6 {
		t.Fatalf("%d Write calls for 6 records, want one each", got)
	}
	for _, slot := range slots {
		var seqs []uint64
		for _, frame := range link.sent("copy") {
			if frame.Slot == slot {
				seqs = append(seqs, frame.Pos.Seq)
			}
		}
		if !slices.Equal(seqs, []uint64{1, 2}) {
			t.Errorf("slot %d was sent sequences %v, want [1 2]", slot, seqs)
		}
	}
}

// stuckBatchLink holds a WriteBatch until released or until its context ends.
type stuckBatchLink struct {
	*u32Link
	release chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (l *stuckBatchLink) WriteBatch(ctx context.Context, _ string, _ []WriteFrame) error {
	l.once.Do(func() { close(l.entered) })
	select {
	case <-l.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TestALaneStuckInAFrameEndsOnClose: Close does not wait for a copy that never answers a frame;
// the lane's context ends with Run, and the call is given that context (I10).
// [BREAK: WriteBatch is called with a context that never ends]
func TestALaneStuckInAFrameEndsOnClose(t *testing.T) {
	var link *stuckBatchLink
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) {
		link = &stuckBatchLink{u32Link: newU32Link(), release: make(chan struct{}), entered: make(chan struct{})}
		cfg.Link = link
	})
	owner.copies = []string{"stuck"}
	owner.ownRun(u32Epoch, 1, 2)
	ctx, stop := context.WithCancel(context.Background())
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(link.release) }); stop() })
	go owner.rep.Run(ctx)
	owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, 2))
	select {
	case <-link.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the frame was never sent")
	}
	closed := make(chan struct{})
	go func() { owner.rep.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not return within a second while a frame to a stuck copy was in flight")
	}
}
