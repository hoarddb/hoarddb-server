package wire

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

const (
	t2Slot uint16 = 7
	t2DB   uint64 = 3
)

// t2KV counts the snapshots that are open, so a pin that is never released is visible.
type t2KV struct {
	*storage.BadgerKV
	open atomic.Int64
}

func (k *t2KV) NewSnapshot() storage.KVSnapshot {
	k.open.Add(1)
	return &t2Snapshot{KVSnapshot: k.BadgerKV.NewSnapshot(), kv: k}
}

type t2Snapshot struct {
	storage.KVSnapshot
	kv   *t2KV
	once sync.Once
}

func (s *t2Snapshot) Discard() {
	s.once.Do(func() {
		s.kv.open.Add(-1)
		s.KVSnapshot.Discard()
	})
}

// t2Rig is a SeedSource over a real Badger and a real pair set, with a clock and a topology
// the test moves.
type t2Rig struct {
	t          *testing.T
	kv         *t2KV
	pairs      *cluster.PairSet
	adv        *LocalAdvancer
	src        *SeedSource
	now        atomic.Int64
	epoch      atomic.Uint64
	mu         sync.Mutex
	owner      string
	holds      bool
	state      machine.SlotState
	intact     bool // RetiredIntact: the record retired in this process and its delete has not started
	dbExists   bool
	diskPct    int
	diskError  error
	diskDelay  time.Duration // makes the disk read slow, to widen a race window
	afterOwner func()        // runs after the source read the owner, to move the world under it
	cuts       sync.Map      // seed id -> the cut its last SEED_ACCEPT named: a receiver's applied position
}

func newT2Rig(t *testing.T, opt SeedOptions) *t2Rig {
	t.Helper()
	badger, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening Badger: %v", err)
	}
	t.Cleanup(func() {
		if err := badger.Close(); err != nil {
			t.Errorf("closing Badger: %v", err)
		}
	})
	rig := &t2Rig{t: t, kv: &t2KV{BadgerKV: badger}, pairs: &cluster.PairSet{}, owner: "n1", holds: true, state: machine.SlotReady, dbExists: true}
	rig.adv = NewLocalAdvancer(rig.pairs, rig.kv)
	rig.epoch.Store(1)
	rig.now.Store(time.Unix(1000, 0).UnixNano())
	opt.Now = func() time.Time { return time.Unix(0, rig.now.Load()) }
	opt.DiskUsedPct = func() (int, error) {
		rig.mu.Lock()
		pct, err, delay := rig.diskPct, rig.diskError, rig.diskDelay
		rig.mu.Unlock()
		time.Sleep(delay)
		return pct, err
	}
	rig.src = NewSeedSource(SeedSourceConfig{
		Self: "n1", Options: opt, Pairs: rig.pairs, KV: rig.kv,
		Committed: rig.epoch.Load, StateSeq: func() uint64 { return 5 },
		Database: func(uint64) bool { rig.mu.Lock(); defer rig.mu.Unlock(); return rig.dbExists },
		Owner: func(uint16, uint64) (string, bool) {
			rig.mu.Lock()
			owner, hook := rig.owner, rig.afterOwner
			rig.mu.Unlock()
			if hook != nil {
				hook()
			}
			return owner, owner != ""
		},
		Holds: func(uint16, uint64, string) bool { rig.mu.Lock(); defer rig.mu.Unlock(); return rig.holds },
		SlotState: func(uint16, uint64) machine.SlotState {
			rig.mu.Lock()
			defer rig.mu.Unlock()
			return rig.state
		},
		RetiredIntact: func(uint16, uint64) bool {
			rig.mu.Lock()
			defer rig.mu.Unlock()
			return rig.intact
		},
	})
	t.Cleanup(rig.src.Close)
	return rig
}

func (r *t2Rig) dropPinForTest(id [16]byte) { r.src.dropPin(id) }

func (r *t2Rig) set(change func(*t2Rig)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	change(r)
}

func (r *t2Rig) advance(by time.Duration) { r.now.Add(int64(by)) }

func t2Key(seq uint64) []byte {
	return append(storage.PairPrefix(t2Slot, t2DB), fmt.Sprintf("k%08d", seq)...)
}

// write applies one position that sets the key of its own sequence, the way an apply
// batch does: the data and the pair record move together under the pair's lock.
func (r *t2Rig) write(value []byte) cluster.Position {
	r.t.Helper()
	var at cluster.Position
	err := r.adv.Advance(t2Slot, t2DB, AdvanceStep{
		Stage: func(batch storage.KVBatch, pair *cluster.Pair) (AdvanceTo, error) {
			at = cluster.Position{Epoch: 1, Seq: pair.AppliedPos.Seq + 1}
			if err := batch.Set(t2Key(at.Seq), value); err != nil {
				return AdvanceTo{}, err
			}
			return AdvanceTo{Applied: at}, nil
		},
		Commit: func(batch storage.KVBatch, _ bool) error { return batch.Commit(false) },
	})
	if err != nil {
		r.t.Fatalf("applying a write: %v", err)
	}
	return at
}

func (r *t2Rig) writes(count int) {
	r.t.Helper()
	for range count {
		r.write([]byte("value"))
	}
}

func t2ID(n byte) (id [16]byte) {
	id[0] = n
	return id
}

func t2Message(payload []byte, err error) transport.Message {
	if err != nil {
		panic(err)
	}
	msg, mErr := messageOf(payload)
	if mErr != nil {
		panic(mErr)
	}
	return *msg
}

func (r *t2Rig) register(from string, id [16]byte, mask byte, cursor []byte) *transport.Message {
	r.t.Helper()
	return r.registerAt(from, id, mask, cursor, r.epoch.Load())
}

func (r *t2Rig) registerAt(from string, id [16]byte, mask byte, cursor []byte, epoch uint64) *transport.Message {
	r.t.Helper()
	msg := t2Message(transport.EncodeSeedRegister(transport.SeedRegister{Header: transport.Envelope{Epoch: epoch, Slot: t2Slot, DBID: t2DB}, SeedID: id, Mask: mask, Cursor: cursor}))
	reply, err := r.src.Serve(from, msg)
	if err != nil {
		r.t.Fatalf("SEED_REGISTER from %s: %v", from, err)
	}
	return reply
}

func (r *t2Rig) ack(from string, id [16]byte, credit uint32) *transport.Message {
	r.t.Helper()
	applied, _ := r.cuts.Load(id)
	cut, _ := applied.(transport.SeedPosition)
	msg := t2Message(transport.EncodeSeedAck(transport.SeedAck{Header: transport.Envelope{Epoch: r.epoch.Load(), Slot: t2Slot, DBID: t2DB}, SeedID: id, Applied: cut, CreditBytes: credit}), nil)
	reply, err := r.src.Serve(from, msg)
	if err != nil {
		r.t.Fatalf("SEED_ACK from %s: %v", from, err)
	}
	return reply
}

func t2Kind(reply *transport.Message) byte {
	if reply == nil {
		return 0
	}
	return reply.Env.Kind
}

func (r *t2Rig) mustAccept(reply *transport.Message) transport.SeedAccept {
	r.t.Helper()
	if t2Kind(reply) != transport.KindSeedAccept {
		r.t.Fatalf("got kind 0x%02x, want SEED_ACCEPT: %s", t2Kind(reply), r.describe(reply))
	}
	accept, err := transport.DecodeSeedAccept(payloadOf(*reply))
	if err != nil {
		r.t.Fatalf("decoding SEED_ACCEPT: %v", err)
	}
	r.cuts.Store(accept.SeedID, accept.Cut)
	return accept
}

func (r *t2Rig) describe(reply *transport.Message) string {
	if reply == nil {
		return "no reply"
	}
	if reply.Env.Kind == transport.KindSeedRefused {
		if refused, err := transport.DecodeSeedRefused(payloadOf(*reply)); err == nil {
			return fmt.Sprintf("SEED_REFUSED reason %d: %s", refused.Reason, refused.Detail)
		}
	}
	if reply.Env.Kind == transport.KindSeedRestart {
		if restart, err := transport.DecodeSeedRestart(payloadOf(*reply)); err == nil {
			return fmt.Sprintf("SEED_RESTART reason %d", restart.Reason)
		}
	}
	return fmt.Sprintf("kind 0x%02x", reply.Env.Kind)
}

func (r *t2Rig) refusedReason(reply *transport.Message) byte {
	r.t.Helper()
	if t2Kind(reply) != transport.KindSeedRefused {
		r.t.Fatalf("got %s, want SEED_REFUSED", r.describe(reply))
	}
	refused, err := transport.DecodeSeedRefused(payloadOf(*reply))
	if err != nil {
		r.t.Fatalf("decoding SEED_REFUSED: %v", err)
	}
	return refused.Reason
}

func (r *t2Rig) restartReason(reply *transport.Message) byte {
	r.t.Helper()
	if t2Kind(reply) != transport.KindSeedRestart {
		r.t.Fatalf("got %s, want SEED_RESTART", r.describe(reply))
	}
	restart, err := transport.DecodeSeedRestart(payloadOf(*reply))
	if err != nil {
		r.t.Fatalf("decoding SEED_RESTART: %v", err)
	}
	return restart.Reason
}

// stream acks until the final chunk or an empty reply and returns the chunks it got.
func (r *t2Rig) stream(from string, id [16]byte, credit uint32) []transport.SeedChunk {
	r.t.Helper()
	var chunks []transport.SeedChunk
	for range 10000 {
		reply := r.ack(from, id, credit)
		if reply == nil {
			return chunks
		}
		if reply.Env.Kind != transport.KindSeedChunk {
			r.t.Fatalf("got %s while streaming, want SEED_CHUNK", r.describe(reply))
		}
		chunk, err := transport.DecodeSeedChunk(append([]byte(nil), payloadOf(*reply)...))
		if err != nil {
			r.t.Fatalf("decoding SEED_CHUNK: %v", err)
		}
		chunks = append(chunks, chunk)
		if chunk.Final {
			// The ACK that follows the final chunk ends the seed.
			if end := r.ack(from, id, credit); end != nil {
				r.t.Fatalf("the ack of the final chunk got %s, want no reply", r.describe(end))
			}
			return chunks
		}
	}
	r.t.Fatal("the stream never ended")
	return nil
}

func t2Keys(chunks []transport.SeedChunk) [][]byte {
	var keys [][]byte
	for _, chunk := range chunks {
		for _, entry := range chunk.Entries {
			keys = append(keys, entry.Key)
		}
	}
	return keys
}

// TestTheSourcePinsOneSnapshotPerSeedIDAndDropsItAfterTheHold: a seed_id holds one
// snapshot however often it registers, a second seed_id holds its own, and a pin the
// receiver stops calling is released when the hold passes.
func TestTheSourcePinsOneSnapshotPerSeedIDAndDropsItAfterTheHold(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{ResumeHold: 10 * time.Second})
	rig.writes(5)
	first := rig.mustAccept(rig.register("n2", t2ID(1), 0, nil))
	if first.Resume {
		t.Error("the first registration of a seed_id answered resume=1")
	}
	rig.mustAccept(rig.register("n2", t2ID(1), 0, nil))
	if rig.src.Pins() != 1 || rig.kv.open.Load() != 1 {
		t.Fatalf("after two registrations of one seed_id: %d pins and %d open snapshots, want 1 and 1", rig.src.Pins(), rig.kv.open.Load())
	}
	rig.mustAccept(rig.register("n3", t2ID(2), 0, nil))
	if rig.src.Pins() != 2 || rig.kv.open.Load() != 2 {
		t.Fatalf("with a second seed_id: %d pins and %d open snapshots, want 2 and 2", rig.src.Pins(), rig.kv.open.Load())
	}
	rig.advance(6 * time.Second)
	rig.ack("n3", t2ID(2), 1<<20) // n3 keeps calling; n2 goes quiet
	rig.advance(6 * time.Second)
	rig.src.sweep()
	if rig.src.Pins() != 1 || rig.kv.open.Load() != 1 {
		t.Fatalf("after the hold for the silent receiver: %d pins and %d open snapshots, want 1 and 1", rig.src.Pins(), rig.kv.open.Load())
	}
	if reason := rig.restartReason(rig.ack("n2", t2ID(1), 1<<20)); reason != seedRestartPinGone {
		t.Errorf("an ack after the pin was dropped got SEED_RESTART %d, want %d (pin lost)", reason, seedRestartPinGone)
	}
	rig.src.Close()
	if rig.src.Pins() != 0 || rig.kv.open.Load() != 0 {
		t.Errorf("after Close: %d pins and %d open snapshots, want 0 and 0", rig.src.Pins(), rig.kv.open.Load())
	}
}

// TestEveryWriteAtOrBelowTheCutIsInTheSnapshotAndNoneIsInBoth: a writer applies positions
// as fast as it can while cuts are taken; for every cut the snapshot holds exactly the
// keys of positions 1..cut and none above it, and every position above the cut is applied
// after the snapshot (it is in the live store and so reaches the receiver as a WRITE).
func TestEveryWriteAtOrBelowTheCutIsInTheSnapshotAndNoneIsInBoth(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{})
	const total = 4000
	done := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		defer close(done)
		for range total {
			rig.write([]byte("value"))
		}
	}()
	cuts := 0
	for ; ; cuts++ {
		finished := false
		select {
		case <-done:
			finished = true
		default:
		}
		pin := &seedPin{slot: t2Slot, db: t2DB}
		rig.src.takeCut(pin)
		iter := pin.snapshot.NewIterator(storage.IterOptions{Lower: storage.PairPrefix(t2Slot, t2DB), Upper: keyRangeEnd(storage.PairPrefix(t2Slot, t2DB))})
		var seen uint64
		for iter.Seek(nil); iter.Valid(); iter.Next() {
			seen++
			if want := t2Key(seen); !bytes.Equal(iter.Key(), want) {
				t.Fatalf("cut %s: the snapshot's key %d is %q, want %q (a hole or a write above the cut)", pin.cut, seen, iter.Key(), want)
			}
		}
		iter.Close()
		pin.snapshot.Discard()
		if seen != pin.cut.Seq {
			t.Fatalf("cut %s but the snapshot holds %d positions: a write at or below the cut is missing, or one above it is in", pin.cut, seen)
		}
		if finished {
			writer.Wait()
			if cuts == 0 {
				t.Fatal("no cut was taken while the writer ran")
			}
			if got := rig.pairs.Snapshot(t2Slot, t2DB).AppliedPos.Seq; got != total {
				t.Fatalf("the writer applied %d positions, want %d", got, total)
			}
			return
		}
	}
}

// TestASeedingNodeIsRefusedReason4AndANonHolderReason3 are the rows of
// machine.SeedSourceAsked: a source that is not ready is not current (4), one that holds
// no copy is not a holder (3), and a node outside the copy set holds nothing whatever its
// record says.
func TestASeedingNodeIsRefusedReason4AndANonHolderReason3(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{})
	for _, tc := range []struct {
		name   string
		change func(*t2Rig)
		want   byte
	}{
		{"seeding", func(r *t2Rig) { r.state = machine.SlotSeeding }, seedRefusedNotCurrent},
		{"retired", func(r *t2Rig) { r.state = machine.SlotRetired }, seedRefusedNotCurrent},
		{"retiring", func(r *t2Rig) { r.state = machine.SlotRetiring }, seedRefusedNotCurrent},
		{"no record", func(r *t2Rig) { r.state = machine.SlotAbsent }, seedRefusedNotHolder},
		{"outside the copy set", func(r *t2Rig) { r.state, r.holds = machine.SlotReady, false }, seedRefusedNotHolder},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig.set(func(r *t2Rig) { r.state, r.holds = machine.SlotReady, true })
			rig.set(tc.change)
			if got := rig.refusedReason(rig.register("n2", t2ID(9), 0, nil)); got != tc.want {
				t.Errorf("reason %d, want %d", got, tc.want)
			}
			if rig.src.Pins() != 0 || rig.kv.open.Load() != 0 {
				t.Errorf("a refusal left %d pins and %d snapshots", rig.src.Pins(), rig.kv.open.Load())
			}
		})
	}
}

// TestTheFifthIncomingSeedGetsBusyWithRetryAfter: incoming_seeds_max pins at once; the next
// registration is told when to come back, and a known seed_id is never busy.
func TestTheFifthIncomingSeedGetsBusyWithRetryAfter(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{IncomingMax: 4, BusyRetryAfter: 1500 * time.Millisecond})
	// Four different receivers: one receiver has one pin per pair (SPEC-GAP T3-8).
	for n := byte(1); n <= 4; n++ {
		rig.mustAccept(rig.register(fmt.Sprintf("r%d", n), t2ID(n), 0, nil))
	}
	busy := rig.register("n3", t2ID(5), 0, nil)
	if t2Kind(busy) != transport.KindSeedBusy {
		t.Fatalf("the fifth seed got %s, want SEED_BUSY", rig.describe(busy))
	}
	msg, err := transport.DecodeSeedBusy(payloadOf(*busy))
	if err != nil || msg.RetryAfterMs != 1500 {
		t.Fatalf("SEED_BUSY retryAfterMs %d (err %v), want 1500", msg.RetryAfterMs, err)
	}
	if rig.src.Pins() != 4 || rig.kv.open.Load() != 4 {
		t.Errorf("a busy answer changed the pins: %d pins, %d snapshots", rig.src.Pins(), rig.kv.open.Load())
	}
	rig.mustAccept(rig.register("r1", t2ID(1), 0, nil))
	rig.stream("r1", t2ID(1), 1<<20)
	rig.mustAccept(rig.register("n3", t2ID(5), 0, nil))
}

// TestTheFinalChunkSetsTheFlagAndAnEarlyEndRestarts: only the last chunk carries the final
// flag, an empty range is one empty final chunk, and a stream that stops early leaves the
// receiver without the flag; the source then restarts it once its pin is gone.
func TestTheFinalChunkSetsTheFlagAndAnEarlyEndRestarts(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{ChunkEntries: 3, ResumeHold: 5 * time.Second})
	rig.writes(10)
	rig.mustAccept(rig.register("n2", t2ID(1), 0, nil))
	chunks := rig.stream("n2", t2ID(1), 1<<20)
	if len(chunks) != 4 {
		t.Fatalf("10 entries at 3 per chunk made %d chunks, want 4", len(chunks))
	}
	for index, chunk := range chunks {
		if chunk.Index != uint64(index) || chunk.Final != (index == 3) {
			t.Errorf("chunk %d: index %d final %v", index, chunk.Index, chunk.Final)
		}
	}
	if rig.src.Pins() != 0 {
		t.Errorf("the ack of the final chunk left %d pins", rig.src.Pins())
	}

	empty := newT2Rig(t, SeedOptions{})
	empty.mustAccept(empty.register("n2", t2ID(1), 0, nil))
	only := empty.stream("n2", t2ID(1), 1<<20)
	if len(only) != 1 || !only[0].Final || len(only[0].Entries) != 0 {
		t.Fatalf("an empty range streamed %d chunks, want one final chunk with no entries", len(only))
	}

	rig.mustAccept(rig.register("n2", t2ID(2), 0, nil))
	if reply := rig.ack("n2", t2ID(2), 1<<20); t2Kind(reply) != transport.KindSeedChunk {
		t.Fatalf("got %s, want the first chunk", rig.describe(reply))
	}
	rig.advance(6 * time.Second)
	rig.src.sweep()
	if reason := rig.restartReason(rig.ack("n2", t2ID(2), 1<<20)); reason != seedRestartPinGone {
		t.Errorf("a stream that ended early got SEED_RESTART %d, want %d", reason, seedRestartPinGone)
	}
}

// TestAResumeAnswersWithThePinnedCut: a re-registration of a known seed_id inside the hold
// gets the pinned cut and resume=1, whatever was written since, and the stream continues
// after the cursor with chunk indices from 0; a resume whose pin is gone is SEED_RESTART 1.
func TestAResumeAnswersWithThePinnedCut(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{ChunkEntries: 2, ResumeHold: 30 * time.Second})
	rig.writes(7)
	first := rig.mustAccept(rig.register("n2", t2ID(1), 0, nil))
	want := transport.SeedPosition{Epoch: 1, Seq: 7}
	if first.Cut != want {
		t.Fatalf("cut %v, want %v", first.Cut, want)
	}
	reply := rig.ack("n2", t2ID(1), 1<<20)
	firstChunk, err := transport.DecodeSeedChunk(append([]byte(nil), payloadOf(*reply)...))
	if err != nil {
		t.Fatal(err)
	}
	rig.writes(4) // positions 8..11 move on; the pin does not
	cursor := firstChunk.Entries[len(firstChunk.Entries)-1].Key
	resumed := rig.mustAccept(rig.register("n2", t2ID(1), seedMaskResume, cursor))
	if !resumed.Resume || resumed.Cut != want {
		t.Fatalf("resume=%v cut %v, want resume=1 and the pinned cut %v", resumed.Resume, resumed.Cut, want)
	}
	rest := rig.stream("n2", t2ID(1), 1<<20)
	if rest[0].Index != 0 {
		t.Errorf("the resumed stream starts at chunk index %d, want 0", rest[0].Index)
	}
	got := append(t2Keys([]transport.SeedChunk{firstChunk}), t2Keys(rest)...)
	if len(got) != 7 {
		t.Fatalf("the two streams carried %d keys, want the 7 of the snapshot", len(got))
	}
	for index, key := range got {
		if !bytes.Equal(key, t2Key(uint64(index)+1)) {
			t.Errorf("key %d is %q, want %q", index, key, t2Key(uint64(index)+1))
		}
	}

	if reason := rig.restartReason(rig.register("n2", t2ID(1), seedMaskResume, cursor)); reason != seedRestartPinGone {
		t.Errorf("resuming a finished seed got SEED_RESTART %d, want %d", reason, seedRestartPinGone)
	}
	if reason := rig.restartReason(rig.register("n2", t2ID(77), seedMaskResume, nil)); reason != seedRestartPinGone {
		t.Errorf("resuming an unknown seed got SEED_RESTART %d, want %d", reason, seedRestartPinGone)
	}
	fresh := rig.mustAccept(rig.register("n2", t2ID(1), 0, nil))
	if fresh.Resume || fresh.Cut != (transport.SeedPosition{Epoch: 1, Seq: 11}) {
		t.Errorf("a new registration got resume=%v cut %v, want resume=0 and a new cut at 1:11", fresh.Resume, fresh.Cut)
	}
	// A seed_id is bound to the node that registered it.
	other := rig.register("n3", t2ID(1), 0, nil)
	if t2Kind(other) == transport.KindSeedAccept {
		t.Error("another node registering a known seed_id was accepted")
	}
	if rig.src.Pins() != 1 || rig.kv.open.Load() != 1 {
		t.Errorf("after the collision: %d pins and %d open snapshots, want 1 and 1", rig.src.Pins(), rig.kv.open.Load())
	}
}

// TestTheSourceNeverSendsPastTheCredit: no chunk is larger than the credit of the ack it
// answers, none exceeds seed_chunk_bytes, and a credit smaller than the next entry gets no
// chunk at all.
func TestTheSourceNeverSendsPastTheCredit(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{ChunkBytes: 4096, ChunkEntries: 100})
	for range 50 {
		rig.write(bytes.Repeat([]byte("v"), 100))
	}
	rig.mustAccept(rig.register("n2", t2ID(1), 0, nil))
	entry := uint32(entryBytes(t2Key(1), bytes.Repeat([]byte("v"), 100)))
	for _, credit := range []uint32{entry, 3*entry + 5, 10 * entry} {
		rig.mustAccept(rig.register("n2", t2ID(1), 0, nil))
		chunks := rig.stream("n2", t2ID(1), credit)
		if len(chunks) == 0 {
			t.Fatalf("credit %d streamed nothing", credit)
		}
		total := 0
		for index, chunk := range chunks {
			size := 0
			for _, e := range chunk.Entries {
				size += entryBytes(e.Key, e.Value)
			}
			if size > int(credit) || size > 4096 {
				t.Errorf("credit %d: chunk %d carries %d entry bytes", credit, index, size)
			}
			total += len(chunk.Entries)
		}
		if total != 50 {
			t.Errorf("credit %d streamed %d entries, want 50", credit, total)
		}
	}
	rig.mustAccept(rig.register("n2", t2ID(1), 0, nil))
	if reply := rig.ack("n2", t2ID(1), entry-1); reply != nil {
		t.Errorf("a credit of %d bytes, one short of an entry, got %s, want no chunk", entry-1, rig.describe(reply))
	}
}

// TestAReservedMaskBitIsRefusedReason5: a reserved mask bit, a cursor without the resume
// bit, and a cursor that is not a key of the pair are all SEED_REFUSED 5.
func TestAReservedMaskBitIsRefusedReason5(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{})
	for _, tc := range []struct {
		name   string
		mask   byte
		cursor []byte
	}{
		{"bit 1", 0x02, nil},
		{"bit 7", 0x80, nil},
		{"bit 0 and bit 3", 0x09, nil},
		{"cursor without the resume bit", 0, t2Key(1)},
		{"cursor of another pair", seedMaskResume, append(storage.PairPrefix(t2Slot+1, t2DB), 'k')},
	} {
		if got := rig.refusedReason(rig.register("n2", t2ID(1), tc.mask, tc.cursor)); got != seedRefusedMask {
			t.Errorf("%s: reason %d, want %d", tc.name, got, seedRefusedMask)
		}
	}
	if rig.src.Pins() != 0 {
		t.Errorf("a refusal left %d pins", rig.src.Pins())
	}
}

// TestTheSourceAnswersDiskDatabaseRedirectAndEpoch: the refusals that depend on the node's
// view: the disk high-water mark (1), a dropped database (2), a non-owner (one redirect,
// except when the requester is the owner), a later epoch of the requester (restart 3), and
// an owner that moves away mid-stream (restart 3 on the next ack).
func TestTheSourceAnswersDiskDatabaseRedirectAndEpoch(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{DiskHighWaterPct: 85})
	rig.writes(3)
	rig.set(func(r *t2Rig) { r.diskPct = 85 })
	if got := rig.refusedReason(rig.register("n2", t2ID(1), 0, nil)); got != seedRefusedDisk {
		t.Errorf("a disk at the high-water mark: reason %d, want %d", got, seedRefusedDisk)
	}
	rig.set(func(r *t2Rig) { r.diskPct = 84 })
	rig.mustAccept(rig.register("n2", t2ID(1), 0, nil))
	rig.set(func(r *t2Rig) { r.diskPct = 99 })
	if !rig.mustAccept(rig.register("n2", t2ID(1), 0, nil)).Resume {
		t.Error("a resume was refused for a full disk; it adds no data to this node")
	}
	rig.set(func(r *t2Rig) { r.diskPct, r.dbExists = 0, false })
	if got := rig.refusedReason(rig.register("n2", t2ID(2), 0, nil)); got != seedRefusedDropped {
		t.Errorf("a dropped database: reason %d, want %d", got, seedRefusedDropped)
	}
	rig.set(func(r *t2Rig) { r.dbExists, r.owner = true, "n3" })
	redirect := rig.register("n2", t2ID(3), 0, nil)
	if t2Kind(redirect) != transport.KindSeedRedirect {
		t.Fatalf("asking a non-owner got %s, want SEED_REDIRECT", rig.describe(redirect))
	}
	if msg, err := transport.DecodeSeedRedirect(payloadOf(*redirect)); err != nil || msg.Source != "n3" {
		t.Errorf("redirect to %q (err %v), want n3", msg.Source, err)
	}
	// The requester is the owner and its catch-up failed: any ready holder is a source.
	rig.mustAccept(rig.register("n3", t2ID(4), 0, nil))
	rig.set(func(r *t2Rig) { r.owner = "n1" })
	if reason := rig.restartReason(rig.registerAt("n2", t2ID(5), 0, nil, 2)); reason != seedRestartEpochChanged {
		t.Errorf("a requester at a later epoch: SEED_RESTART %d, want %d", reason, seedRestartEpochChanged)
	}
	rig.mustAccept(rig.register("n2", t2ID(6), 0, nil))
	rig.epoch.Store(2)
	// An earlier epoch than the source's is not a mismatch (conventions 12 contract 3).
	rig.mustAccept(rig.registerAt("n4", t2ID(8), 0, nil, 1))
	rig.dropPinForTest(t2ID(8))
	if reply := rig.ack("n2", t2ID(6), 1<<20); t2Kind(reply) != transport.KindSeedChunk {
		t.Fatalf("a new epoch with the same owner got %s, want the stream to go on", rig.describe(reply))
	}
	rig.epoch.Store(3)
	rig.set(func(r *t2Rig) { r.owner = "n3" })
	if reason := rig.restartReason(rig.ack("n2", t2ID(6), 1<<20)); reason != seedRestartEpochChanged {
		t.Errorf("an owner that moved: SEED_RESTART %d, want %d", reason, seedRestartEpochChanged)
	}
	// Seed 1 went when n2 registered seed 6 for the same pair (SPEC-GAP T3-8), seed 6 with the
	// restart; seed 4 is another receiver's.
	if rig.src.Pins() != 1 || rig.kv.open.Load() != 1 {
		t.Errorf("the restart should have released that pin: %d pins and %d snapshots, want the 1 left (seed 4)", rig.src.Pins(), rig.kv.open.Load())
	}
}

// TestOnlyRegisterAndAckAreRequests: the other seed kinds are answers and are refused as
// requests.
func TestOnlyRegisterAndAckAreRequests(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{})
	for _, kind := range []byte{transport.KindSeedAccept, transport.KindSeedRedirect, transport.KindSeedRefused, transport.KindSeedBusy, transport.KindSeedChunk, transport.KindSeedRestart} {
		if _, err := rig.src.Serve("n2", transport.Message{Env: transport.Envelope{Version: transport.EnvelopeVersion, Kind: kind}}); err == nil {
			t.Errorf("kind 0x%02x was served as a request", kind)
		}
	}
}

// TestSeedOptionsDefaultsAreTheConfigDefaults keeps the constants of this package equal to
// the config package's, which it cannot import in its own code.
func TestSeedOptionsDefaultsAreTheConfigDefaults(t *testing.T) {
	want := config.DefaultReplicationConfig()
	got := SeedOptions{}.withDefaults()
	if got.WindowBytes != want.SeedWindowBytes || got.ChunkBytes != want.SeedChunkBytes || got.ChunkEntries != want.SeedChunkEntries ||
		got.ResumeHold != time.Duration(want.SeedResumeHoldSeconds)*time.Second || got.Idle != time.Duration(want.SeedIdleSeconds)*time.Second ||
		got.DiskHighWaterPct != want.SeedDiskHighWaterPct || got.IncomingMax != want.IncomingSeedsMax ||
		got.SlotsInFlight != want.SeedSlotsInFlight || got.SyncBytes != int64(want.SeedSyncBytes) || got.BufferBytes != int64(want.SeedBufferBytes) {
		t.Errorf("defaults %+v differ from config %+v", got, want)
	}
}

// TestASeedIDHeldByAnotherRequesterIsNeverOverwritten: a second node naming a live seed_id
// gets SEED_RESTART 1 and the first node's pin, and its snapshot, stay; nothing is leaked
// and Close releases what is there.
func TestASeedIDHeldByAnotherRequesterIsNeverOverwritten(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{})
	rig.writes(3)
	rig.mustAccept(rig.register("n2", t2ID(1), 0, nil))
	if reason := rig.restartReason(rig.register("n3", t2ID(1), 0, nil)); reason != seedRestartPinGone {
		t.Errorf("a second requester of a live seed_id got SEED_RESTART %d, want %d", reason, seedRestartPinGone)
	}
	if rig.src.Pins() != 1 || rig.kv.open.Load() != 1 {
		t.Fatalf("after the collision: %d pins and %d open snapshots, want 1 and 1", rig.src.Pins(), rig.kv.open.Load())
	}
	if !rig.mustAccept(rig.register("n2", t2ID(1), 0, nil)).Resume {
		t.Error("the first requester lost its pin to the collision")
	}
	rig.mustAccept(rig.register("n3", t2ID(2), 0, nil))
	if rig.kv.open.Load() != 2 {
		t.Fatalf("%d open snapshots, want 2", rig.kv.open.Load())
	}
	rig.src.Close()
	if rig.kv.open.Load() != 0 {
		t.Errorf("after Close %d snapshots are still open", rig.kv.open.Load())
	}
}

// TestARefusalToOneRequesterNeverDropsAnothersPin: a refusal or a redirect of a registration
// that names someone else's live seed_id leaves that pin alone.
func TestARefusalToOneRequesterNeverDropsAnothersPin(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{})
	rig.writes(2)
	rig.mustAccept(rig.register("n2", t2ID(1), 0, nil))
	rig.set(func(r *t2Rig) { r.holds = false })
	rig.refusedReason(rig.register("n3", t2ID(1), 0, nil))
	rig.set(func(r *t2Rig) { r.holds, r.owner = true, "" })
	rig.refusedReason(rig.register("n3", t2ID(1), 0, nil))
	rig.set(func(r *t2Rig) { r.owner = "n4" })
	if kind := t2Kind(rig.register("n3", t2ID(1), 0, nil)); kind != transport.KindSeedRedirect {
		t.Fatalf("got kind 0x%02x, want SEED_REDIRECT", kind)
	}
	if rig.src.Pins() != 1 || rig.kv.open.Load() != 1 {
		t.Fatalf("n2's pin was dropped by n3's refusals: %d pins, %d snapshots", rig.src.Pins(), rig.kv.open.Load())
	}
}

// TestAPairDroppedBetweenTheChecksAndTheCutIsRestartedNotAccepted: the node held the pair
// when it read its record and lost it before the cut, so the cut would be a made-up empty
// pair. The epoch is read before the cut and the role is checked again after it.
func TestAPairDroppedBetweenTheChecksAndTheCutIsRestartedNotAccepted(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{})
	rig.writes(3)
	rig.set(func(r *t2Rig) {
		r.afterOwner = func() {
			rig.pairs.Drop(t2Slot, t2DB)
			rig.epoch.Store(2)
			rig.set(func(r *t2Rig) { r.holds, r.afterOwner = false, nil })
		}
	})
	if reason := rig.restartReason(rig.register("n2", t2ID(1), 0, nil)); reason != seedRestartEpochChanged {
		t.Errorf("SEED_RESTART %d, want %d", reason, seedRestartEpochChanged)
	}
	if rig.src.Pins() != 0 || rig.kv.open.Load() != 0 {
		t.Errorf("the restart left %d pins and %d snapshots", rig.src.Pins(), rig.kv.open.Load())
	}
}

// TestAnEntryLargerThanTheWindowIsRefusedWithItsSize: an entry that cannot fit even a full
// window can never be sent, so the source says so (SEED_REFUSED, with the entry's size and
// seed_window_bytes in the text) and ends the seed; a credit that is only temporarily short
// still gets no chunk.
func TestAnEntryLargerThanTheWindowIsRefusedWithItsSize(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{WindowBytes: 200})
	rig.write(bytes.Repeat([]byte("v"), 500))
	rig.mustAccept(rig.register("n2", t2ID(1), 0, nil))
	reply := rig.ack("n2", t2ID(1), 200)
	if t2Kind(reply) != transport.KindSeedRefused {
		t.Fatalf("got %s, want SEED_REFUSED for an entry of 500 bytes in a window of 200", rig.describe(reply))
	}
	refused, err := transport.DecodeSeedRefused(payloadOf(*reply))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains([]byte(refused.Detail), []byte("seed_window_bytes")) || !bytes.Contains([]byte(refused.Detail), []byte("200")) {
		t.Errorf("the text does not name the window: %q", refused.Detail)
	}
	if rig.src.Pins() != 0 || rig.kv.open.Load() != 0 {
		t.Errorf("the refusal left %d pins and %d snapshots", rig.src.Pins(), rig.kv.open.Load())
	}
}

// TestAnAckForAnotherPairThanTheSeedsIsRestarted: the ack names the pair in its envelope, and
// a chunk of the pinned pair must never come back labelled as another.
func TestAnAckForAnotherPairThanTheSeedsIsRestarted(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{})
	rig.writes(2)
	accepted := rig.mustAccept(rig.register("n2", t2ID(1), 0, nil))
	for _, env := range []transport.Envelope{
		{Epoch: 1, Slot: t2Slot + 1, DBID: t2DB},
		{Epoch: 1, Slot: t2Slot, DBID: t2DB + 1},
	} {
		msg := t2Message(transport.EncodeSeedAck(transport.SeedAck{Header: env, SeedID: t2ID(1), Applied: accepted.Cut, CreditBytes: 1 << 20}), nil)
		reply, err := rig.src.Serve("n2", msg)
		if err != nil {
			t.Fatal(err)
		}
		if reason := rig.restartReason(reply); reason != seedRestartPinGone {
			t.Errorf("an ack for slot %d database %d: SEED_RESTART %d, want %d", env.Slot, env.DBID, reason, seedRestartPinGone)
		}
	}
}

// TestIncomingSeedsMaxHoldsUnderConcurrentRegistrations: the check against incoming_seeds_max
// and the insert of the pin are one step, however many registrations arrive together.
func TestIncomingSeedsMaxHoldsUnderConcurrentRegistrations(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{IncomingMax: 2})
	rig.set(func(r *t2Rig) { r.diskDelay = 10 * time.Millisecond })
	rig.writes(2)
	var wg sync.WaitGroup
	var accepted, busy atomic.Int64
	for n := range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := t2Message(transport.EncodeSeedRegister(transport.SeedRegister{Header: transport.Envelope{Epoch: 1, Slot: t2Slot, DBID: t2DB}, SeedID: t2ID(byte(n + 1))}))
			reply, err := rig.src.Serve(fmt.Sprintf("r%d", n), msg)
			if err != nil {
				t.Error(err)
				return
			}
			switch t2Kind(reply) {
			case transport.KindSeedAccept:
				accepted.Add(1)
			case transport.KindSeedBusy:
				busy.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 2 || busy.Load() != 22 || rig.src.Pins() != 2 || rig.kv.open.Load() != 2 {
		t.Errorf("accepted %d busy %d pins %d snapshots %d, want 2, 22, 2 and 2", accepted.Load(), busy.Load(), rig.src.Pins(), rig.kv.open.Load())
	}
}

// TestARegistrationRacingCloseLeavesNoSnapshot: a pin is either refused because the source
// is closed or is in the map Close empties; none is left open.
func TestARegistrationRacingCloseLeavesNoSnapshot(t *testing.T) {
	for range 30 {
		rig := newT2Rig(t, SeedOptions{IncomingMax: 64})
		rig.set(func(r *t2Rig) { r.diskDelay = time.Millisecond })
		var wg sync.WaitGroup
		for n := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				msg := t2Message(transport.EncodeSeedRegister(transport.SeedRegister{Header: transport.Envelope{Epoch: 1, Slot: t2Slot, DBID: t2DB}, SeedID: t2ID(byte(n + 1))}))
				_, _ = rig.src.Serve("n2", msg) // the error of a closed source is the point
			}()
		}
		rig.src.Close()
		wg.Wait()
		if rig.kv.open.Load() != 0 || rig.src.Pins() != 0 {
			t.Fatalf("after Close raced registrations: %d snapshots, %d pins", rig.kv.open.Load(), rig.src.Pins())
		}
	}
}

// t2RetireIntact makes the rig's node a retired holder of the pair whose range is whole: out
// of the copy set, its record retired in this process with no delete started, and n2 the
// pair's owner (BUG-0835).
func t2RetireIntact(r *t2Rig) {
	r.state, r.intact, r.holds, r.owner = machine.SlotRetired, true, false, "n2"
}

// t2DeleteStarts is the gate's RetireStarted on the rig's node: the record is retiring and
// no longer intact, before the first delete batch.
func t2DeleteStarts(r *t2Rig) {
	r.state, r.intact = machine.SlotRetiring, false
}

// TestARetiredHolderWithAnIntactRangeServesASeed: a node retired out of the pair's copy set
// whose range delete has not started holds a whole copy, and serves it to the pair's owner
// (BUG-0835): the cut is its applied position, every key comes through, the pin outlives an
// epoch that keeps the requester the owner, and the closing round is the pinned one. Any
// other requester is sent to the owner. [guard: drop the retired-intact exception in
// sourceRefusal (refused 3); refuse retired in the machine's source rows (refused 4); leave
// holdsRole at copy-set holders only (restart at the epoch move); leave SeedSource.Holds at
// ready records only (the closing round is not the pin's)]
func TestARetiredHolderWithAnIntactRangeServesASeed(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{ChunkEntries: 2})
	rig.writes(5)
	rig.set(t2RetireIntact)
	accept := rig.mustAccept(rig.register("n2", t2ID(1), 0, nil))
	if want := (transport.SeedPosition{Epoch: 1, Seq: 5}); accept.Cut != want {
		t.Errorf("cut %+v, want %+v, the holder's applied position", accept.Cut, want)
	}
	// An epoch that keeps n2 the owner and this node outside the copy set: the range is still
	// whole, so the seed goes on.
	rig.epoch.Store(2)
	first := rig.ack("n2", t2ID(1), 1<<20)
	if t2Kind(first) != transport.KindSeedChunk {
		t.Fatalf("the first ack after the epoch moved got %s, want SEED_CHUNK", rig.describe(first))
	}
	chunk, err := transport.DecodeSeedChunk(append([]byte(nil), payloadOf(*first)...))
	if err != nil {
		t.Fatalf("decoding SEED_CHUNK: %v", err)
	}
	if !rig.src.Holds(t2Slot, t2DB, "n2") {
		t.Error("Holds is false for the owner's live pin, so its closing round would be answered as a plain catch-up from an epoch the holder never wrote in")
	}
	keys := t2Keys(append([]transport.SeedChunk{chunk}, rig.stream("n2", t2ID(1), 1<<20)...))
	if len(keys) != 5 {
		t.Fatalf("the seed carried %d keys, want 5", len(keys))
	}
	for i, key := range keys {
		if !bytes.Equal(key, t2Key(uint64(i+1))) {
			t.Errorf("key %d is %x, want %x", i, key, t2Key(uint64(i+1)))
		}
	}
	if reply := rig.register("n3", t2ID(2), 0, nil); t2Kind(reply) != transport.KindSeedRedirect {
		t.Errorf("a requester that is not the owner got %s, want SEED_REDIRECT to the owner", rig.describe(reply))
	}
	if rig.src.Pins() != 0 || rig.kv.open.Load() != 0 {
		t.Errorf("after the seed ended: %d pins and %d snapshots, want 0 and 0", rig.src.Pins(), rig.kv.open.Load())
	}
}

// TestARetiredHolderWhoseDeleteHasBegunRefusesASeed: a retiring record, a retired one whose
// delete finished, and a retired one read after a restart (the two read alike: the flag is
// memory) hold no whole copy, so they are refused whether or not the node is in the copy
// set and even when the requester is the owner. This is the BUG-0817 and BUG-0819
// protection and passes before the fix too. [guard: serve every retired record (drop the
// intact test)] Reading the intact flag without the retired state in sourceRefusal is not a
// guard this test can catch: machine.SourceStanding and SlotRecord.RetiredIntact both check
// the state again, so the "retiring, with a stale intact reading" rows refuse either way.
func TestARetiredHolderWhoseDeleteHasBegunRefusesASeed(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{})
	rig.writes(3)
	for _, tc := range []struct {
		name   string
		state  machine.SlotState
		intact bool
	}{
		{"retiring", machine.SlotRetiring, false},
		{"retiring, with a stale intact reading", machine.SlotRetiring, true},
		{"retired, delete finished or read after a restart", machine.SlotRetired, false},
	} {
		for _, holds := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, in the copy set %v", tc.name, holds), func(t *testing.T) {
				rig.set(func(r *t2Rig) { r.state, r.intact, r.holds, r.owner = tc.state, tc.intact, holds, "n2" })
				reply := rig.register("n2", t2ID(1), 0, nil)
				if t2Kind(reply) != transport.KindSeedRefused {
					t.Fatalf("got %s, want SEED_REFUSED", rig.describe(reply))
				}
				if rig.src.Pins() != 0 || rig.kv.open.Load() != 0 {
					t.Errorf("a refusal left %d pins and %d snapshots", rig.src.Pins(), rig.kv.open.Load())
				}
			})
		}
	}
}

// TestASeedPinOnARetiredHolderStopsWhenTheDeleteStarts: once the gate's RetireStarted flips
// the record, a pin served by a retired holder ends at its next call, in the same epoch as
// well as after a move, its closing round is no longer the pin's, and a registration whose
// cut lands after the flip is not accepted. [guard: check the retired standing only when the
// epoch moved (sourceStands' shortcut); skip the standing check after the cut in pinFor]
func TestASeedPinOnARetiredHolderStopsWhenTheDeleteStarts(t *testing.T) {
	t.Run("at the next ack, in the same epoch", func(t *testing.T) {
		rig := newT2Rig(t, SeedOptions{ChunkEntries: 2})
		rig.writes(5)
		rig.set(t2RetireIntact)
		rig.mustAccept(rig.register("n2", t2ID(1), 0, nil))
		if reply := rig.ack("n2", t2ID(1), 1<<20); t2Kind(reply) != transport.KindSeedChunk {
			t.Fatalf("the first ack got %s, want SEED_CHUNK", rig.describe(reply))
		}
		rig.set(t2DeleteStarts)
		if rig.src.Holds(t2Slot, t2DB, "n2") {
			t.Error("Holds is still true after the delete started, so the closing round would read a range being deleted")
		}
		if reason := rig.restartReason(rig.ack("n2", t2ID(1), 1<<20)); reason != seedRestartEpochChanged {
			t.Errorf("SEED_RESTART %d, want %d", reason, seedRestartEpochChanged)
		}
		if rig.src.Pins() != 0 || rig.kv.open.Load() != 0 {
			t.Errorf("the stopped seed left %d pins and %d snapshots", rig.src.Pins(), rig.kv.open.Load())
		}
	})
	t.Run("between the record check and the cut", func(t *testing.T) {
		rig := newT2Rig(t, SeedOptions{})
		rig.writes(3)
		rig.set(func(r *t2Rig) {
			t2RetireIntact(r)
			r.afterOwner = func() { rig.set(func(r *t2Rig) { t2DeleteStarts(r); r.afterOwner = nil }) }
		})
		if reason := rig.restartReason(rig.register("n2", t2ID(1), 0, nil)); reason != seedRestartEpochChanged {
			t.Errorf("SEED_RESTART %d, want %d", reason, seedRestartEpochChanged)
		}
		if rig.src.Pins() != 0 || rig.kv.open.Load() != 0 {
			t.Errorf("the restart left %d pins and %d snapshots", rig.src.Pins(), rig.kv.open.Load())
		}
	})
}

// TestARetiredHolderServesOnlyTheOwnerAndNeverWhileItIsTheOwner: a retired holder with a
// whole range serves the pair's owner only, and only when the owner is another node. A
// retired record stays retired when its node comes back into the copy set before the gate
// passes, and the owner comes from the topology alone, so that node can be the owner again;
// its range then stops where it retired and misses every write acknowledged while it was
// out, so it refuses (reason 4) whoever asks, as a retired record did before BUG-0835. Any
// other requester is sent to the owner. [guard: drop the owner-is-self refusal for a retired
// holder in register (the first row gets SEED_ACCEPT)]
func TestARetiredHolderServesOnlyTheOwnerAndNeverWhileItIsTheOwner(t *testing.T) {
	for _, tc := range []struct {
		name      string
		holds     bool
		owner     string
		requester string
		wantKind  byte
	}{
		{"back in the copy set and the owner, asked by a non-owner", true, "n1", "n3", transport.KindSeedRefused},
		{"back in the copy set, owner another node, asked by a third", true, "n2", "n3", transport.KindSeedRedirect},
		{"back in the copy set, asked by the owner", true, "n2", "n2", transport.KindSeedAccept},
		{"out of the copy set, asked by the owner", false, "n2", "n2", transport.KindSeedAccept},
		{"out of the copy set, asked by a non-owner", false, "n2", "n3", transport.KindSeedRedirect},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newT2Rig(t, SeedOptions{ChunkEntries: 2})
			rig.writes(5)
			rig.set(func(r *t2Rig) {
				r.state, r.intact, r.holds, r.owner = machine.SlotRetired, true, tc.holds, tc.owner
			})
			reply := rig.register(tc.requester, t2ID(1), 0, nil)
			if t2Kind(reply) != tc.wantKind {
				t.Fatalf("got %s, want kind 0x%02x", rig.describe(reply), tc.wantKind)
			}
			switch tc.wantKind {
			case transport.KindSeedRefused:
				if reason := rig.refusedReason(reply); reason != seedRefusedNotCurrent {
					t.Errorf("SEED_REFUSED reason %d, want %d (not current)", reason, seedRefusedNotCurrent)
				}
				if rig.src.Pins() != 0 || rig.kv.open.Load() != 0 {
					t.Errorf("the refusal left %d pins and %d snapshots", rig.src.Pins(), rig.kv.open.Load())
				}
			case transport.KindSeedRedirect:
				redirect, err := transport.DecodeSeedRedirect(payloadOf(*reply))
				if err != nil {
					t.Fatalf("decoding SEED_REDIRECT: %v", err)
				}
				if redirect.Source != tc.owner {
					t.Errorf("redirected to %q, want the owner %q", redirect.Source, tc.owner)
				}
			}
		})
	}
}

// TestASnapshotOpenedBeforeTheRetireDeleteKeepsEveryKey: the retire delete is
// Puller.resetRange, transactional deletes in batches at new commit timestamps, so a snapshot
// opened before its first batch still walks every key and value of the range after the
// delete finished. A seed pinned by a retired holder (BUG-0835) relies on this for the chunks
// it serves while the delete runs: the flag re-read in pinFor only proves the snapshot opened
// before the delete started. [guard: delete the range with BadgerKV.DropPrefixes, which does
// not respect open snapshots]
func TestASnapshotOpenedBeforeTheRetireDeleteKeepsEveryKey(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	clock := &s1VirtualClock{now: time.Unix(1_700_000_000, 0)}
	puller := NewPuller(PullerConfig{Self: testSelf, Recon: rig.rec, KV: rig.kv, Budget: s1Budget(clock, 1_000_000, 4096)})
	keyCount := 2*resetRangeBatch + 10
	s1FillRange(t, rig.kv, rig.slot, reconDB, keyCount)

	pinned := rig.kv.NewSnapshot()
	defer pinned.Discard()
	if err := puller.resetRange(context.Background(), reconcileKey{rig.slot, reconDB}); err != nil {
		t.Fatal(err)
	}
	if left := s1CountRange(t, rig.kv, rig.slot, reconDB); left != 0 {
		t.Fatalf("%d keys survive the delete in a snapshot opened after it", left)
	}
	prefix := storage.PairPrefix(rig.slot, reconDB)
	iter := pinned.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyRangeEnd(prefix)})
	defer iter.Close()
	seen := 0
	for iter.Seek(prefix); iter.Valid(); iter.Next() {
		if want := t3Key(rig.slot, reconDB, seen); !bytes.Equal(iter.Key(), want) {
			t.Fatalf("key %d of the pinned snapshot is %x, want %x", seen, iter.Key(), want)
		}
		stored, err := iter.Value()
		if err != nil {
			t.Fatalf("reading key %d of the pinned snapshot: %v", seen, err)
		}
		if string(stored) != "old" {
			t.Fatalf("key %d of the pinned snapshot holds %q, want %q", seen, stored, "old")
		}
		seen++
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("walking the pinned snapshot: %v", err)
	}
	if seen != keyCount {
		t.Fatalf("the snapshot opened before the delete walks %d keys, want all %d", seen, keyCount)
	}
}
