package wire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// t3 tests drive the Puller against a real SeedSource over real Badger stores and a real
// Reconciler on the receiver; the seed channel is a function call, so every message is the
// codec's.

// t3Source is one seed source: its store, its pair set and its SeedSource.
type t3Source struct {
	id    string
	kv    *storage.BadgerKV
	pairs *cluster.PairSet
	src   *SeedSource
	owner string
	holds atomic.Bool
	// log holds the records above the cut that the source has appended, in position order; a
	// catch-up answers from it.
	log []PairRecord
	// horizonFrom, when set, is a catch-up from that position answered LOG_BELOW_HORIZON.
	horizonFrom *cluster.Position
}

func t3OpenKV(t *testing.T, dir string) *storage.BadgerKV {
	t.Helper()
	kv, err := storage.OpenBadgerKV(dir, storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening Badger: %v", err)
	}
	return kv
}

func t3NewSource(t *testing.T, id, owner string, epoch uint64, opt SeedOptions) *t3Source {
	t.Helper()
	kv := t3OpenKV(t, t.TempDir())
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Errorf("closing the source store: %v", err)
		}
	})
	source := &t3Source{id: id, kv: kv, pairs: &cluster.PairSet{}, owner: owner}
	source.holds.Store(true)
	source.src = NewSeedSource(SeedSourceConfig{
		Self: id, Options: opt, Pairs: source.pairs, KV: kv,
		Committed: func() uint64 { return epoch }, StateSeq: func() uint64 { return 9 },
		Database: func(uint64) bool { return true },
		Owner:    func(uint16, uint64) (string, bool) { return source.owner, true },
		Holds:    func(uint16, uint64, string) bool { return source.holds.Load() },
		SlotState: func(uint16, uint64) machine.SlotState {
			return machine.SlotReady
		},
		RetiredIntact: func(uint16, uint64) bool { return false },
	})
	t.Cleanup(source.src.Close)
	return source
}

func t3Key(slot uint16, db uint64, number int) []byte {
	return append(storage.PairPrefix(slot, db), fmt.Sprintf("k%06d", number)...)
}

// fill writes count keys, one dedupe entry of the given ordinal, and the pair's position.
func (s *t3Source) fill(t *testing.T, slot uint16, db uint64, count int, cut cluster.Position, ordinal uint64) {
	t.Helper()
	batch := s.kv.NewBatch()
	defer batch.Discard()
	for number := range count {
		if err := batch.Set(t3Key(slot, db, number), fmt.Appendf(nil, "value-%d", number)); err != nil {
			t.Fatal(err)
		}
	}
	if ordinal != 0 {
		key, value, err := EncodeDedupeEntry(storage.BucketRef{Slot: slot, DatabaseID: db, BucketID: 9}, machine.DedupeEntry{
			Key: machine.DedupeKey{RequestID: [16]byte{1}, OpIndex: 0}, Ordinal: ordinal, Pos: cut,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	s.pairs.With(slot, db, func(pair *cluster.Pair) { pair.AppliedPos, pair.Ordinal = cut, ordinal })
}

// hold records rec in the source's log at its position, as the owner holds a write it ships.
func (s *t3Source) hold(rec PairRecord) {
	for i, held := range s.log {
		if held.Pos == rec.Pos {
			s.log[i] = rec
			return
		}
		if rec.Pos.Less(held.Pos) {
			s.log = slices.Insert(s.log, i, rec)
			return
		}
	}
	s.log = append(s.log, rec)
}

func (s *t3Source) dump(t *testing.T, slot uint16, db uint64) []transport.SeedEntry {
	t.Helper()
	return t3Dump(t, s.kv, slot, db)
}

// catchUp answers a catch-up from the source's log the way Replicator.ServeCatchUp does: the
// records from req.From, the source's tail, and whether the answer is current or divergent.
// The tail is the last logged record, or the pair's cut when the log is empty.
func (s *t3Source) catchUp(slot uint16, db uint64, req CatchUpRequest) (CatchUpChunk, error) {
	if s.horizonFrom != nil && *s.horizonFrom == req.From {
		return CatchUpChunk{}, &cluster.Error{Code: cluster.CodeLogBelowHorizon, Message: fmt.Sprintf("the log's oldest record for the slot is past %s", req.From)}
	}
	var appended cluster.Position
	s.pairs.With(slot, db, func(pair *cluster.Pair) { appended = pair.AppliedPos })
	if n := len(s.log); n > 0 {
		appended = s.log[n-1].Pos
	}
	chunk := CatchUpChunk{SourceAppended: appended}
	for _, rec := range s.log {
		if !rec.Pos.Less(req.From) {
			chunk.Records = append(chunk.Records, rec)
		}
	}
	if n := len(chunk.Records); n > 0 {
		chunk.LastPosition = chunk.Records[n-1].Pos
		chunk.CaughtUp = chunk.LastPosition == appended
		return chunk, nil
	}
	held := before(req.From)
	switch {
	case held.Seq == 0 && appended.Epoch < held.Epoch:
		// The pinned answer of Replicator.ServePinnedCatchUp: a receiver asking from the opener of
		// an epoch the source has not written in has nothing missing at the source's tail.
		chunk.CaughtUp, chunk.LastPosition = true, appended
	case appended.Less(held):
		chunk.Divergent = true
	case held == appended:
		chunk.CaughtUp, chunk.LastPosition = true, appended
	default:
		return CatchUpChunk{}, fmt.Errorf("the fixture source holds nothing from %s to %s", req.From, appended)
	}
	return chunk, nil
}

func t3Dump(t *testing.T, kv storage.KV, slot uint16, db uint64) []transport.SeedEntry {
	t.Helper()
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	prefix := storage.PairPrefix(slot, db)
	iter := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyRangeEnd(prefix)})
	defer iter.Close()
	var entries []transport.SeedEntry
	for iter.Seek(prefix); iter.Valid(); iter.Next() {
		raw, err := iter.Value()
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, transport.SeedEntry{Key: append([]byte(nil), iter.Key()...), Value: raw})
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	return entries
}

func t3SameEntries(t *testing.T, got, want []transport.SeedEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("the receiver holds %d keys, the source %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].Key, want[i].Key) || !bytes.Equal(got[i].Value, want[i].Value) {
			t.Fatalf("key %d differs: receiver %x=%q, source %x=%q", i, got[i].Key, got[i].Value, want[i].Key, want[i].Value)
		}
	}
}

// t3Rig is the receiver n2 of threeNodes, pulling slot from the sources.
type t3Rig struct {
	t       *testing.T
	state   cluster.ClusterState
	slot    uint16
	kv      storage.KV
	rec     *Reconciler
	pairs   *cluster.PairSet
	puller  *Puller
	sources map[string]*t3Source
	owner   string

	mu       sync.Mutex
	seen     []transport.Message // every request sent, in order
	wrap     func(source string, msg transport.Message, reply *transport.Message, err error) (*transport.Message, error)
	ready    atomic.Int64
	reported []error
	// rep is the node's Replicator; its catch-up reaches the rig's sources through t3Link.
	rep *Replicator
	// sourceLacksOffered says the owner's log does not take the writes a test offers to the
	// buffer: the source then holds only what the test gave it, as a source that never held them.
	sourceLacksOffered bool
	// catchUps is every catch-up the Replicator sent, in order; catchHook, when set, sees each
	// answer after it is computed and before the Replicator reads it.
	catchUps  []catchUpCall
	catchHook func(source string, req CatchUpRequest, chunk CatchUpChunk)
}

// catchUpCall is one catch-up the rig's Replicator sent: the source it dialled and the request.
type catchUpCall struct {
	source string
	req    CatchUpRequest
}

type t3Options struct {
	seed    SeedOptions
	sources map[string]*t3Source
	kv      storage.KV
	// Buffers is the node's live-write buffer budget the pull uses; nil buffers nothing.
	Buffers *seedBuffers
	// Applier and Window are what the drain applies buffered writes with.
	Applier Applier
	Window  machine.WindowConfig
	// Pace is the node's seed byte rate the pull is paced by; nil paces nothing.
	Pace *seedPacer
	// DiskUsedPct is the receiver's disk the pull checks before it goes on; nil never pauses.
	DiskUsedPct func() (int, error)
	// OnReady, when set, runs inside the puller's OnReady for each pair that became ready.
	OnReady func(slot uint16, db uint64)
}

// newT3Rig boots a Reconciler for n2, joining (the member whose absent pairs the pass seeds;
// an active member gets the interim, seeding S0), over kv (a new store when nil), finds a
// slot where n2 is a non-owner copy and builds the Puller. The queued actions go nowhere:
// the test queues the one pair it wants.
func newT3Rig(t *testing.T, opt t3Options) *t3Rig {
	t.Helper()
	state := joiningNodes(t)
	kv := opt.kv
	if kv == nil {
		badger := t3OpenKV(t, t.TempDir())
		t.Cleanup(func() {
			if err := badger.Close(); err != nil {
				t.Errorf("closing the receiver store: %v", err)
			}
		})
		kv = badger
	}
	slots := findSlots(t, state)
	rig := &t3Rig{t: t, state: state, slot: slots.copyOnly, kv: kv, pairs: &cluster.PairSet{}, sources: opt.sources}
	table := cluster.BuildTable(state)
	owner, _ := table.Owner(rig.slot, 2)
	rig.owner = owner.ID
	rig.rec = NewReconciler(Config{
		NodeID: testSelf, Store: kv, Clock: &reconClock{at: time.Unix(1_700_000_000, 0)},
		State: stateFn(func() cluster.ClusterState { return rig.state }), Sink: &recordingSink{}, Pairs: rig.pairs,
		Current: func(uint16, uint64) bool { return true }, WindowBytes: 1 << 20,
	})
	rep, err := NewReplicator(ReplicatorConfig{
		Self: testSelf, Committed: func() uint64 { return uint64(state.Epoch) }, Pairs: rig.pairs,
		Advancer: t3Advancer{}, Link: t3Link{rig: rig},
	})
	if err != nil {
		t.Fatalf("NewReplicator: %v", err)
	}
	rig.rep = rep
	// The joiner's boot as driveSeeding runs it: the records loaded, then the resume when it
	// holds any (a restarted pull keeps its cursor) and the joining commit's pass otherwise.
	if err := rig.rec.LoadRecords(); err != nil {
		t.Fatalf("LoadRecords: %v", err)
	}
	if rig.rec.HasRecords() {
		if err := rig.rec.ResumeJoining(t.Context()); err != nil {
			t.Fatalf("ResumeJoining: %v", err)
		}
	} else if err := rig.rec.OnJoiningCommit(state); err != nil {
		t.Fatalf("OnJoiningCommit: %v", err)
	}
	seed := opt.seed
	if seed.RetryBackoff == 0 {
		seed.RetryBackoff = 5 * time.Millisecond
	}
	rig.puller = NewPuller(PullerConfig{
		Self: testSelf, Recon: rig.rec, KV: kv, Options: seed, Call: rig.call,
		Committed: func() uint64 { return uint64(state.Epoch) }, StateSeq: func() uint64 { return uint64(state.StateSeq) },
		Sources: rig.candidates, Buffers: opt.Buffers, Applier: opt.Applier, DedupeWindow: opt.Window,
		Replicator: rig.rep, Pace: opt.Pace, DiskUsedPct: opt.DiskUsedPct,
		OnReady: func(slot uint16, db uint64) {
			if opt.OnReady != nil {
				opt.OnReady(slot, db)
			}
			rig.ready.Add(1)
		},
		OnError: func(err error) { rig.mu.Lock(); rig.reported = append(rig.reported, err); rig.mu.Unlock() },
	})
	return rig
}

func (r *t3Rig) candidates(uint16, uint64) []string {
	ids := []string{r.owner}
	for id := range r.sources {
		if id != r.owner {
			ids = append(ids, id)
		}
	}
	return ids
}

func (r *t3Rig) call(_ context.Context, source string, msg transport.Message) (*transport.Message, error) {
	r.mu.Lock()
	r.seen = append(r.seen, msg)
	wrap := r.wrap
	r.mu.Unlock()
	target := r.sources[source]
	if target == nil {
		return nil, fmt.Errorf("no node %s", source)
	}
	reply, err := target.src.Serve(testSelf, msg)
	if wrap != nil {
		return wrap(source, msg, reply, err)
	}
	return reply, err
}

// catchUp answers one catch-up the rig's Replicator sent to source, from that source's log.
func (r *t3Rig) catchUp(source string, req CatchUpRequest) (CatchUpChunk, error) {
	r.mu.Lock()
	r.catchUps = append(r.catchUps, catchUpCall{source: source, req: req})
	hook := r.catchHook
	r.mu.Unlock()
	target := r.sources[source]
	if target == nil {
		return CatchUpChunk{}, fmt.Errorf("no node %s", source)
	}
	chunk, err := target.catchUp(r.slot, reconDB, req)
	if hook != nil {
		hook(source, req, chunk)
	}
	return chunk, err
}

// askedFrom reports whether the Replicator asked source for the records from pos.
func (r *t3Rig) askedFrom(source string, pos cluster.Position) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, call := range r.catchUps {
		if call.source == source && call.req.From == pos {
			return true
		}
	}
	return false
}

// t3Link is the data channel of the rig's Replicator. The pull's catch-ups reach the sources'
// logs through it; no WRITE or ACK goes through these tests.
type t3Link struct{ rig *t3Rig }

func (t3Link) Write(context.Context, string, WriteFrame) error { return nil }

func (t3Link) Ack(context.Context, string, WriteAck) error { return nil }

func (l t3Link) CatchUp(_ context.Context, source string, req CatchUpRequest) (CatchUpChunk, error) {
	return l.rig.catchUp(source, req)
}

// t3Advancer is the Replicator's LockedAdvancer in the rig. A catch-up never applies, so a call
// here is a bug and fails loudly.
type t3Advancer struct{}

func (t3Advancer) AdvanceLocked(*cluster.Pair, uint16, uint64, AdvanceStep) error {
	return errors.New("the rig's catch-up applied a record through the Replicator's own path")
}

func (r *t3Rig) requests(kind byte) []transport.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []transport.Message
	for _, msg := range r.seen {
		if msg.Env.Kind == kind {
			out = append(out, msg)
		}
	}
	return out
}

// pullUntil queues the pair and runs the puller until done reports true.
func (r *t3Rig) pullUntil(what string, done func() bool) {
	r.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		r.puller.Run(ctx)
	}()
	r.puller.Queue(r.slot, reconDB)
	deadline := time.Now().Add(20 * time.Second)
	for !done() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	ok := done()
	cancel()
	<-finished
	if !ok {
		status, _ := r.puller.Status(r.slot, reconDB)
		r.t.Fatalf("timed out waiting for %s; pull status %+v", what, status)
	}
}

func (r *t3Rig) isReady() bool {
	rec, found := r.rec.Record(r.slot, reconDB)
	return found && rec.State == machine.SeedReady
}

func t3Cut(t *testing.T, rig *t3Rig) cluster.Position {
	return cluster.Position{Epoch: uint64(rig.state.Epoch), Seq: 40}
}

// --- the tests -----------------------------------------------------------

func TestAPullFillsTheRangeResetsItFirstAndMakesTheRecordReady(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner = "x"
	rig.sources = map[string]*t3Source{"x": owner}
	cut := t3Cut(t, rig)
	owner.fill(t, rig.slot, reconDB, 10, cut, 42)

	// A stray key left by an earlier life of the range must not survive the reset.
	stray := t3Key(rig.slot, reconDB, 999)
	batch := rig.kv.NewBatch()
	if err := batch.Set(stray, []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	batch.Discard()

	rig.pullUntil("the pair to be ready", rig.isReady)

	t3SameEntries(t, t3Dump(t, rig.kv, rig.slot, reconDB), owner.dump(t, rig.slot, reconDB))
	record, _ := rig.rec.Record(rig.slot, reconDB)
	if record.Applied != cut || record.Ordinal != 42 {
		t.Errorf("the ready record is at %v with ordinal %d, want %v and 42", record.Applied, record.Ordinal, cut)
	}
	if shared := rig.pairs.Snapshot(rig.slot, reconDB); shared.AppliedPos != cut || shared.Ordinal != 42 {
		t.Errorf("the pair set holds %v ordinal %d, want %v and 42", shared.AppliedPos, shared.Ordinal, cut)
	}
	if got := len(rig.requests(transport.KindSeedAck)); got < 5 {
		t.Errorf("%d acks for 10 keys in chunks of 3, want one per chunk and the closing one", got)
	}
	if rig.ready.Load() != 1 {
		t.Errorf("OnReady ran %d times, want 1", rig.ready.Load())
	}
	if owner.src.Pins() != 0 {
		t.Errorf("the source still holds %d pins after the closing ack", owner.src.Pins())
	}
	for _, msg := range rig.requests(transport.KindSeedRegister) {
		reg, err := transport.DecodeSeedRegister(payloadOf(msg))
		if err != nil {
			t.Fatal(err)
		}
		if reg.Mask != 0 || len(reg.Cursor) != 0 {
			t.Errorf("a fresh attempt registered with mask %d and cursor %x", reg.Mask, reg.Cursor)
		}
	}
}

func TestAnEmptyPairBecomesReadyAtItsCut(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	cut := t3Cut(t, rig)
	owner.pairs.With(rig.slot, reconDB, func(pair *cluster.Pair) { pair.AppliedPos = cut })
	rig.pullUntil("the empty pair to be ready", rig.isReady)
	if got := len(t3Dump(t, rig.kv, rig.slot, reconDB)); got != 0 {
		t.Errorf("an empty pair holds %d keys", got)
	}
	if shared := rig.pairs.Snapshot(rig.slot, reconDB); shared.AppliedPos != cut {
		t.Errorf("the pair is at %v, want the cut %v", shared.AppliedPos, cut)
	}
}

// t3FailingKV fails the Commit of the nth batch that contains a seed-record write, to play
// a crash between the chunk's data and its cursor.
type t3FailingKV struct {
	storage.KV
	commits atomic.Int64
	failAt  int64
	failed  atomic.Bool
}

func (k *t3FailingKV) NewBatch() storage.KVBatch {
	return &t3FailingBatch{KVBatch: k.KV.NewBatch(), kv: k}
}

type t3FailingBatch struct {
	storage.KVBatch
	kv      *t3FailingKV
	hasData bool
}

func (b *t3FailingBatch) Set(key, value []byte) error {
	if len(key) > 10 && key[0] != 0xFF { // a pair-range key; the seed record lives under the node prefix 0xFFFF
		b.hasData = true
	}
	return b.KVBatch.Set(key, value)
}

func (b *t3FailingBatch) Commit(sync bool) error {
	if b.hasData && b.kv.commits.Add(1) == b.kv.failAt {
		b.kv.failed.Store(true)
		return errors.New("the disk failed under the chunk")
	}
	return b.KVBatch.Commit(sync)
}

func TestTheCursorMovesOnlyInTheBatchOfItsChunk(t *testing.T) {
	badger := t3OpenKV(t, t.TempDir())
	t.Cleanup(func() { _ = badger.Close() })
	failing := &t3FailingKV{KV: badger, failAt: 2}
	rig := newT3Rig(t, t3Options{kv: failing})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 10, t3Cut(t, rig), 0)

	rig.pullUntil("the failed commit", failing.failed.Load)
	// The second chunk's batch failed: the record's cursor is the last key of the first chunk
	// and the failed chunk's keys are not on disk.
	record, found := rig.rec.Record(rig.slot, reconDB)
	if !found || record.State != machine.SeedSeeding {
		t.Fatalf("after the failed commit the record is %+v, want seeding", record)
	}
	disk := t3Dump(t, badger, rig.slot, reconDB)
	if len(disk) != 3 {
		t.Fatalf("%d keys on disk after the first chunk of 3 and a failed second, want 3", len(disk))
	}
	if !bytes.Equal(record.Cursor, disk[len(disk)-1].Key) {
		t.Errorf("the cursor is %x but the last key on disk is %x: the cursor and its data diverged", record.Cursor, disk[len(disk)-1].Key)
	}
	// The same record is on disk, not only in memory.
	snapshot := badger.NewSnapshot()
	raw, err := snapshot.Get(SeedRecordKey(rig.slot, reconDB))
	snapshot.Discard()
	if err != nil {
		t.Fatal(err)
	}
	onDisk, err := DecodeSeedRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(onDisk.Cursor, record.Cursor) {
		t.Errorf("the record on disk has cursor %x, memory has %x", onDisk.Cursor, record.Cursor)
	}
}

func TestAKilledPullResumesFromItsCursorAndEndsWithTheSourcesBytes(t *testing.T) {
	badger := t3OpenKV(t, t.TempDir())
	t.Cleanup(func() { _ = badger.Close() })
	rig := newT3Rig(t, t3Options{kv: badger})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 12, t3Cut(t, rig), 0)

	// The connection dies after the second chunk: the pull stops with a cursor.
	var chunks atomic.Int64
	rig.wrap = func(_ string, _ transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if reply != nil && reply.Env.Kind == transport.KindSeedChunk && chunks.Add(1) > 2 {
			return nil, errors.New("connection reset")
		}
		return reply, err
	}
	rig.pullUntil("two chunks and a dead connection", func() bool { return chunks.Load() > 2 })
	first, _ := rig.rec.Record(rig.slot, reconDB)
	if len(first.Cursor) == 0 {
		t.Fatal("the record has no cursor after two chunks")
	}

	// The process dies: a new Reconciler and Puller read the record back from the disk.
	rebooted := newT3Rig(t, t3Options{kv: badger})
	rebooted.owner, rebooted.sources = "x", map[string]*t3Source{"x": owner}
	record, found := rebooted.rec.Record(rebooted.slot, reconDB)
	if !found || record.State != machine.SeedSeeding || !bytes.Equal(record.Cursor, first.Cursor) {
		t.Fatalf("the rebooted record is %+v, want seeding with cursor %x", record, first.Cursor)
	}
	rebooted.pullUntil("the resumed pull to finish", rebooted.isReady)

	registers := rebooted.requests(transport.KindSeedRegister)
	if len(registers) == 0 {
		t.Fatal("the resumed pull never registered")
	}
	reg, err := transport.DecodeSeedRegister(payloadOf(registers[0]))
	if err != nil {
		t.Fatal(err)
	}
	if reg.Mask&seedMaskResume == 0 || !bytes.Equal(reg.Cursor, first.Cursor) || reg.SeedID != first.SeedID {
		t.Errorf("the re-registration is mask %d cursor %x seed %x, want resume from %x of seed %x", reg.Mask, reg.Cursor, reg.SeedID, first.Cursor, first.SeedID)
	}
	t3SameEntries(t, t3Dump(t, badger, rebooted.slot, reconDB), owner.dump(t, rebooted.slot, reconDB))
}

func TestACorruptedChunkIsRefusedAndRequestedAgainFromTheCursor(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 9, t3Cut(t, rig), 0)

	var flipped atomic.Bool
	var chunks atomic.Int64
	rig.wrap = func(_ string, _ transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if reply != nil && reply.Env.Kind == transport.KindSeedChunk && chunks.Add(1) == 2 && flipped.CompareAndSwap(false, true) {
			bad := *reply
			bad.Body = append([]byte(nil), reply.Body...)
			bad.Body[len(bad.Body)-1] ^= 0xFF // the last byte of the last entry: the CRC no longer holds
			return &bad, nil
		}
		return reply, err
	}
	rig.pullUntil("the pull to finish despite one damaged chunk", rig.isReady)
	if !flipped.Load() {
		t.Fatal("the test never damaged a chunk")
	}
	t3SameEntries(t, t3Dump(t, rig.kv, rig.slot, reconDB), owner.dump(t, rig.slot, reconDB))
	resumes := 0
	for _, msg := range rig.requests(transport.KindSeedRegister) {
		if reg, err := transport.DecodeSeedRegister(payloadOf(msg)); err == nil && reg.Mask&seedMaskResume != 0 {
			resumes++
		}
	}
	if resumes != 1 {
		t.Errorf("%d resume registrations after one damaged chunk, want 1", resumes)
	}
}

func TestALostPinRestartsTheSeedOnAResetRange(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 9, t3Cut(t, rig), 0)

	var dropped atomic.Bool
	var chunks atomic.Int64
	rig.wrap = func(_ string, msg transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if reply != nil && reply.Env.Kind == transport.KindSeedChunk && chunks.Add(1) == 2 && dropped.CompareAndSwap(false, true) {
			// The source loses its pin (a restart) and the key the receiver already holds goes
			// away at the source: only a reset can remove it from the receiver.
			ack, ackErr := transport.DecodeSeedAck(payloadOf(msg))
			if ackErr != nil {
				t.Error(ackErr)
			}
			owner.src.dropPin(ack.SeedID)
			batch := owner.kv.NewBatch()
			if delErr := batch.Delete(t3Key(rig.slot, reconDB, 0)); delErr != nil {
				t.Error(delErr)
			}
			if commitErr := batch.Commit(true); commitErr != nil {
				t.Error(commitErr)
			}
			batch.Discard()
			return reply, err
		}
		return reply, err
	}
	rig.pullUntil("the restarted pull to finish", rig.isReady)
	if !dropped.Load() {
		t.Fatal("the test never dropped the pin")
	}
	t3SameEntries(t, t3Dump(t, rig.kv, rig.slot, reconDB), owner.dump(t, rig.slot, reconDB))
	if got := len(t3Dump(t, rig.kv, rig.slot, reconDB)); got != 8 {
		t.Errorf("the receiver holds %d keys, want 8 (key 0 was deleted at the source and the reset removed it here)", got)
	}
}

func TestBusyIsRetriedAfterItsHint(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{IncomingMax: 1, BusyRetryAfter: 5 * time.Millisecond})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 4, t3Cut(t, rig), 0)

	// Another receiver holds the only slot.
	other := t2Message(transport.EncodeSeedRegister(transport.SeedRegister{Header: transport.Envelope{Epoch: uint64(rig.state.Epoch), Slot: rig.slot, DBID: reconDB}, SeedID: [16]byte{77}}))
	if reply, err := owner.src.Serve("n3", other); err != nil || reply.Env.Kind != transport.KindSeedAccept {
		t.Fatalf("the other receiver was not accepted: %v %v", reply, err)
	}
	var busy atomic.Int64
	rig.wrap = func(_ string, _ transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if reply != nil && reply.Env.Kind == transport.KindSeedBusy && busy.Add(1) == 3 {
			owner.src.dropPin([16]byte{77})
		}
		return reply, err
	}
	rig.pullUntil("the pull to finish once the slot frees", rig.isReady)
	if busy.Load() < 3 {
		t.Errorf("%d BUSY answers, want at least 3 before the slot freed", busy.Load())
	}
}

func TestAnOversizedEntryStallsWithTheSourcesDetail(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{WindowBytes: 64})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	batch := owner.kv.NewBatch()
	if err := batch.Set(t3Key(rig.slot, reconDB, 1), bytes.Repeat([]byte("v"), 500)); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	batch.Discard()
	owner.pairs.With(rig.slot, reconDB, func(pair *cluster.Pair) { pair.AppliedPos = t3Cut(t, rig) })

	rig.pullUntil("a stalled status", func() bool {
		status, ok := rig.puller.Status(rig.slot, reconDB)
		return ok && status.State == PullStalled
	})
	status, _ := rig.puller.Status(rig.slot, reconDB)
	if !bytes.Contains([]byte(status.Detail), []byte("seed_window_bytes")) {
		t.Errorf("the stalled status says %q, want the source's detail naming seed_window_bytes", status.Detail)
	}
	if record, _ := rig.rec.Record(rig.slot, reconDB); record.State != machine.SeedSeeding {
		t.Errorf("a stalled pair has record state %d, want seeding", record.State)
	}
}

func TestANotHolderRefusalMovesThePullToTheNextCopy(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	first := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{})
	second := t3NewSource(t, "y", "x", uint64(rig.state.Epoch), SeedOptions{})
	first.holds.Store(false)
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": first, "y": second}
	second.owner = "y"
	second.fill(t, rig.slot, reconDB, 5, t3Cut(t, rig), 0)
	rig.pullUntil("the pull to finish from the second copy", rig.isReady)
	t3SameEntries(t, t3Dump(t, rig.kv, rig.slot, reconDB), second.dump(t, rig.slot, reconDB))
}

func TestAtMostSlotsInFlightPullsRunAndTheyAllEndWithTheContext(t *testing.T) {
	rig := newT3Rig(t, t3Options{seed: SeedOptions{SlotsInFlight: 2}})
	var running, peak atomic.Int64
	release := make(chan struct{})
	rig.puller.cfg.Call = func(ctx context.Context, _ string, _ transport.Message) (*transport.Message, error) {
		now := running.Add(1)
		for {
			seen := peak.Load()
			if now <= seen || peak.CompareAndSwap(seen, now) {
				break
			}
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
		running.Add(-1)
		return nil, errors.New("held")
	}
	rig.puller.cfg.Sources = func(uint16, uint64) []string { return []string{"x"} }
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() { defer close(finished); rig.puller.Run(ctx) }()
	// Six pairs that this node seeds: any slots where it is a copy.
	table := cluster.BuildTable(rig.state)
	queued := 0
	for slot := range uint16(storage.SlotCount) {
		if queued == 6 {
			break
		}
		for _, member := range table.Copies(slot, 2) {
			if member.ID == testSelf {
				if _, found := rig.rec.Record(slot, reconDB); found {
					rig.puller.Queue(slot, reconDB)
					queued++
				}
			}
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for running.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if peak.Load() > 2 {
		t.Errorf("%d pulls ran at once, want at most seed_slots_in_flight 2", peak.Load())
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after its context ended")
	}
}

// TestANewSeedIDForTheSamePairSupersedesTheAbandonedPin: a receiver pulls one pair at a time,
// so a registration with a new seed_id for the pair it already has a pin for means it gave
// the first attempt up. The source releases that snapshot at once instead of holding one of
// its incoming_seeds_max slots for the resume hold (SPEC-GAP T3-8).
func TestANewSeedIDForTheSamePairSupersedesTheAbandonedPin(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{IncomingMax: 2})
	rig.writes(3)
	rig.mustAccept(rig.register("n2", t2ID(1), 0, nil))
	rig.mustAccept(rig.register("n2", t2ID(2), 0, nil))
	if rig.src.Pins() != 1 || rig.kv.open.Load() != 1 {
		t.Fatalf("%d pins and %d open snapshots after a new seed_id for the same pair, want 1 and 1", rig.src.Pins(), rig.kv.open.Load())
	}
	// Another requester's pin for the same pair is not touched.
	rig.mustAccept(rig.register("n3", t2ID(3), 0, nil))
	if rig.src.Pins() != 2 {
		t.Fatalf("%d pins, want 2: a second requester's pin must stay", rig.src.Pins())
	}
}

// t3Tamper decodes the chunk in reply, lets change edit it, and encodes it again (the codec
// computes a fresh CRC), so the damage is in what the chunk says and not in its checksum.
func t3Tamper(t *testing.T, reply *transport.Message, change func(*transport.SeedChunk)) *transport.Message {
	t.Helper()
	chunk, err := transport.DecodeSeedChunk(append([]byte(nil), payloadOf(*reply)...))
	if err != nil {
		t.Fatal(err)
	}
	change(&chunk)
	payload, err := transport.EncodeSeedChunk(chunk)
	if err != nil {
		t.Fatal(err)
	}
	out, err := messageOf(payload)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestAChunkWithAKeyOutsideThePairOrOutOfOrderIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*transport.SeedChunk, uint16)
	}{
		{"a key past the pair's range", func(c *transport.SeedChunk, slot uint16) {
			// It sorts after every key of the pair, so the order check alone does not catch it.
			last := &c.Entries[len(c.Entries)-1]
			last.Key = append(keyRangeEnd(storage.PairPrefix(slot, reconDB)), 'z')
		}},
		{"keys out of order", func(c *transport.SeedChunk, _ uint16) {
			c.Entries[0], c.Entries[1] = c.Entries[1], c.Entries[0]
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newT3Rig(t, t3Options{})
			owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
			rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
			owner.fill(t, rig.slot, reconDB, 9, t3Cut(t, rig), 0)
			var tampered atomic.Bool
			rig.wrap = func(_ string, _ transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
				if reply != nil && reply.Env.Kind == transport.KindSeedChunk && tampered.CompareAndSwap(false, true) {
					return t3Tamper(t, reply, func(c *transport.SeedChunk) { tc.change(c, rig.slot) }), nil
				}
				return reply, err
			}
			rig.pullUntil("the pull to finish after one refused chunk", rig.isReady)
			if !tampered.Load() {
				t.Fatal("nothing was tampered with")
			}
			t3SameEntries(t, t3Dump(t, rig.kv, rig.slot, reconDB), owner.dump(t, rig.slot, reconDB))
			// The refused chunk is asked for again by a resume: a second registration.
			if got := len(rig.requests(transport.KindSeedRegister)); got < 2 {
				t.Errorf("%d registrations: the damaged chunk was applied instead of refused", got)
			}
			// Nothing outside the pair's range was written.
			snapshot := rig.kv.NewSnapshot()
			defer snapshot.Discard()
			foreign := append(keyRangeEnd(storage.PairPrefix(rig.slot, reconDB)), 'z')
			if _, err := snapshot.Get(foreign); !errors.Is(err, storage.ErrKeyNotFound) {
				t.Errorf("a key outside the pair was written: %v", err)
			}
		})
	}
}

func TestAChunkIndexGapRestartsTheAttemptOnAFreshCut(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 12, t3Cut(t, rig), 0)
	var skipped atomic.Bool
	var chunks atomic.Int64
	rig.wrap = func(source string, msg transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if reply != nil && reply.Env.Kind == transport.KindSeedChunk && chunks.Add(1) == 2 && skipped.CompareAndSwap(false, true) {
			// The reply to this ack is lost on the wire and the next ack's chunk arrives instead.
			return owner.src.Serve(testSelf, msg)
		}
		return reply, err
	}
	first, _ := rig.rec.Record(rig.slot, reconDB)
	rig.pullUntil("the pull to finish after a skipped chunk", rig.isReady)
	if !skipped.Load() {
		t.Fatal("no chunk was skipped")
	}
	t3SameEntries(t, t3Dump(t, rig.kv, rig.slot, reconDB), owner.dump(t, rig.slot, reconDB))
	registers := rig.requests(transport.KindSeedRegister)
	last, err := transport.DecodeSeedRegister(payloadOf(registers[len(registers)-1]))
	if err != nil {
		t.Fatal(err)
	}
	if last.SeedID == first.SeedID || last.Mask != 0 {
		t.Errorf("after a gap the pull registered seed %x mask %d, want a new seed_id and mask 0", last.SeedID, last.Mask)
	}
}

func TestAnAcceptFromALaterEpochWaitsInsteadOfAbandoningItsPin(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch)+1, SeedOptions{})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 3, cluster.Position{Epoch: uint64(rig.state.Epoch) + 1, Seq: 4}, 0)
	before, _ := rig.rec.Record(rig.slot, reconDB)
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() { defer close(finished); rig.puller.Run(ctx) }()
	rig.puller.Queue(rig.slot, reconDB)
	deadline := time.Now().Add(5 * time.Second)
	for len(rig.requests(transport.KindSeedRegister)) < 4 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-finished
	after, _ := rig.rec.Record(rig.slot, reconDB)
	if after.SeedID != before.SeedID {
		t.Errorf("the attempt moved from seed %x to %x while its epoch lagged the source's", before.SeedID, after.SeedID)
	}
	if owner.src.Pins() != 1 {
		t.Errorf("the source holds %d pins, want the one pin of the waiting attempt", owner.src.Pins())
	}
	if rig.isReady() {
		t.Error("a pair became ready from a source whose epoch this node had not applied")
	}
}

func TestSlotStatesComeFromTheSeedRecords(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	parts := &seedParts{recon: rig.rec}
	parts.observed.Store(true)
	slots := recordSlots{parts: parts}
	if got := slots.State(rig.slot, reconDB); got != machine.SlotSeeding {
		t.Errorf("a pair with a seeding record is %v, want seeding", got)
	}
	if got := slots.State(rig.slot+1, reconDB+9); got != machine.SlotAbsent || !slots.Admits(rig.slot+1, reconDB+9) {
		t.Errorf("a pair with no record on a node that is not joining is %v (admits %v), want absent and admitted (seeding S0)", got, slots.Admits(rig.slot+1, reconDB+9))
	}
	parts.joining.Store(true)
	if got := slots.State(rig.slot+1, reconDB+9); got != machine.SlotAbsent || slots.Admits(rig.slot+1, reconDB+9) {
		t.Errorf("a pair with no record on a joining node is %v (admits %v), want absent and refused", got, slots.Admits(rig.slot+1, reconDB+9))
	}
	if positionState(machine.SlotAbsent) != PositionStateSeeding || positionState(machine.SlotSeeding) != PositionStateSeeding || positionState(machine.SlotReady) != PositionStateReady {
		t.Error("POSITION_REPLY must call a seeding or absent pair seeding and only a ready one ready")
	}
	rig.pullUntilReadyWithOwner()
	if got := slots.State(rig.slot, reconDB); got != machine.SlotReady {
		t.Errorf("a pulled pair is %v, want ready", got)
	}
}

// pullUntilReadyWithOwner pulls the rig's pair from an empty owner.
func (r *t3Rig) pullUntilReadyWithOwner() {
	r.t.Helper()
	owner := t3NewSource(r.t, "x", "x", uint64(r.state.Epoch), SeedOptions{})
	r.owner, r.sources = "x", map[string]*t3Source{"x": owner}
	owner.pairs.With(r.slot, reconDB, func(pair *cluster.Pair) { pair.AppliedPos = t3Cut(r.t, r) })
	r.pullUntil("the pair to be ready", r.isReady)
}

func TestResumeJoiningKeepsASeedingRecordAndRewritesAReadyOne(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	setState(t, &st, testSelf, "down")
	replace(t, &st, testSelf, 2) // n2 is joining again
	kv := t3OpenKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	cut := cluster.Position{Epoch: uint64(st.Epoch), Seq: 3}
	seeding := seedingRecord(machine.PhaseSnapshot, cut, "cursor-key")
	putRecord(t, kv, slots.copyOnly, seeding)
	putRecord(t, kv, slots.owner, readyRecord(cut))
	rig := newRig(t, kv, st, testSelf, true)
	if err := rig.rec.LoadRecords(); err != nil {
		t.Fatal(err)
	}
	if !rig.rec.HasRecords() {
		t.Fatal("HasRecords is false after loading two records")
	}
	if err := rig.rec.ResumeJoining(t.Context()); err != nil {
		t.Fatal(err)
	}
	kept, _ := rig.rec.Record(slots.copyOnly, reconDB)
	if kept.State != machine.SeedSeeding || kept.SeedID != seeding.SeedID || string(kept.Cursor) != "cursor-key" || kept.Phase != machine.PhaseSnapshot {
		t.Errorf("the seeding record after a joiner's restart is %+v, want it untouched", kept)
	}
	reset, _ := rig.rec.Record(slots.owner, reconDB)
	if reset.State != machine.SeedSeeding || reset.Phase != machine.PhaseReset || reset.SeedID == readyRecord(cut).SeedID {
		t.Errorf("the ready record on a joiner is %+v, want a fresh seeding attempt from reset", reset)
	}
}

// t3RecordKV watches every batch for a write of the pair's seed record and keeps the first
// ready one it decodes, with the order of syncs around it.
type t3RecordKV struct {
	storage.KV
	mu     sync.Mutex
	events []string
	ready  *SeedRecord
	leads  []string
	// lastCursor is the cursor of the last record committed: only a cursor that moved must
	// travel with its key.
	lastCursor string
}

func (k *t3RecordKV) Sync() error {
	k.mu.Lock()
	k.events = append(k.events, "sync")
	k.mu.Unlock()
	return k.KV.Sync()
}

func (k *t3RecordKV) NewBatch() storage.KVBatch {
	return &t3RecordBatch{KVBatch: k.KV.NewBatch(), kv: k}
}

type t3RecordBatch struct {
	storage.KVBatch
	kv      *t3RecordKV
	records []SeedRecord
	keys    map[string]bool
	deletes bool
}

func (b *t3RecordBatch) Set(key, value []byte) error {
	if b.keys == nil {
		b.keys = map[string]bool{}
	}
	b.keys[string(key)] = true
	if len(key) == len(SeedRecordKey(0, 0)) && key[0] == 0xFF {
		if record, err := DecodeSeedRecord(value); err == nil {
			b.records = append(b.records, record)
		}
	}
	return b.KVBatch.Set(key, value)
}

func (b *t3RecordBatch) Delete(key []byte) error {
	b.deletes = true
	return b.KVBatch.Delete(key)
}

func (b *t3RecordBatch) Commit(sync bool) error {
	err := b.KVBatch.Commit(sync)
	b.kv.mu.Lock()
	defer b.kv.mu.Unlock()
	if err == nil && b.deletes {
		b.kv.events = append(b.kv.events, "reset")
	}
	for key := range b.keys {
		if key[0] != 0xFF {
			b.kv.events = append(b.kv.events, "chunk")
			break
		}
	}
	for _, record := range b.records {
		if len(record.Cursor) != 0 && err == nil && string(record.Cursor) != b.kv.lastCursor && !b.keys[string(record.Cursor)] {
			b.kv.leads = append(b.kv.leads, fmt.Sprintf("cursor %x committed without its key", record.Cursor))
		}
		if err == nil {
			b.kv.lastCursor = string(record.Cursor)
		}
		if record.State == machine.SeedReady && b.kv.ready == nil {
			copied := record
			b.kv.ready = &copied
			// The ready record's own commit is the attempt's last sync (seeding-speed-note.md):
			// it is recorded as such, so an unsynced ready record reads differently.
			if sync {
				b.kv.events = append(b.kv.events, "synced-ready")
			} else {
				b.kv.events = append(b.kv.events, "ready")
			}
		}
	}
	return err
}

// TestTheReadyRecordCarriesTheOrdinalInTheSameBatch: the pair must never serve with an
// ordinal of 0 while its range holds dedupe entries numbered up to 42 (review F1): the batch
// that writes the ready record is the batch that carries the ordinal.
func TestTheReadyRecordCarriesTheOrdinalInTheSameBatch(t *testing.T) {
	badger := t3OpenKV(t, t.TempDir())
	t.Cleanup(func() { _ = badger.Close() })
	watching := &t3RecordKV{KV: badger}
	rig := newT3Rig(t, t3Options{kv: watching})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 5, t3Cut(t, rig), 42)
	stray := badger.NewBatch() // old data the reset must remove
	if err := stray.Set(t3Key(rig.slot, reconDB, 999), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := stray.Commit(true); err != nil {
		t.Fatal(err)
	}
	stray.Discard()
	rig.pullUntil("the pair to be ready", rig.isReady)
	watching.mu.Lock()
	defer watching.mu.Unlock()
	if watching.ready == nil {
		t.Fatal("no batch wrote a ready record")
	}
	if watching.ready.Ordinal != 42 {
		t.Errorf("the batch that made the pair ready carried ordinal %d, want 42", watching.ready.Ordinal)
	}
	// The order of the whole pull: the reset is committed, then synced; the chunks follow; the
	// ready record is written by a commit that syncs the store, which makes the chunks durable
	// with it before the record is installed.
	got := strings.Join(watching.events, " ")
	compact := t3Compact(watching.events)
	if compact != "reset sync chunk synced-ready" {
		t.Errorf("the pull ran %q (compacted %q), want the reset, a sync, the chunks and then the ready record in a synced commit", got, compact)
	}
}

func TestAJoinerThatHasNotObservedItsStateYetHoldsNothing(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	parts := &seedParts{recon: rig.rec}
	slots := recordSlots{parts: parts}
	if got := slots.State(rig.slot+1, reconDB+9); got != machine.SlotAbsent {
		t.Errorf("before the first look at the cluster state a pair with no record is %v, want absent: a restarted joiner must not answer ready", got)
	}
}

// TestACopyThatIsSeedingAdvertisingTheHighestCutIsNotACatchUpSource (review F4b): a seeding
// copy reports the cut as its position though it holds no log for it; choosing it wedges a
// failover, so only a ready reply is an offer.
func TestACopyThatIsSeedingAdvertisingTheHighestCutIsNotACatchUpSource(t *testing.T) {
	own := cluster.Position{Epoch: 1, Seq: 3}
	offers := []catchUpOffer{
		{ID: "seeding", Reply: PositionReply{State: PositionStateSeeding, AppliedPos: cluster.Position{Epoch: 1, Seq: 90}}},
		{ID: "ready", Reply: PositionReply{State: PositionStateReady, AppliedPos: cluster.Position{Epoch: 1, Seq: 20}}},
	}
	if best, found := chooseCatchUpSource(own, offers); !found || best != "ready" {
		t.Errorf("chose %q (found %v), want the ready copy", best, found)
	}
	if _, found := chooseCatchUpSource(own, offers[:1]); found {
		t.Error("a lone seeding copy was chosen as a source")
	}
}

// TestTheWriteOfASeededPairKeepsItsSeedRecord (review F4): LocalAdvancer wrote 32 bytes over
// the seed record under the same key; the node's advancer moves a pair that has a record
// through the Reconciler, so the record stays a seed record that says where the pair is.
func TestTheWriteOfASeededPairKeepsItsSeedRecord(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	kv := t3OpenKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	putRecord(t, kv, slots.owner, readyRecord(cluster.Position{Epoch: uint64(st.Epoch), Seq: 3}))
	rig := newRig(t, kv, st, testSelf, true)
	if err := rig.rec.LoadRecords(); err != nil {
		t.Fatal(err)
	}
	advancer := seedAwareAdvancer{recon: rig.rec}
	to := cluster.Position{Epoch: uint64(st.Epoch), Seq: 4}
	err := advancer.Advance(slots.owner, reconDB, AdvanceStep{
		Stage: func(batch storage.KVBatch, _ *cluster.Pair) (AdvanceTo, error) {
			return AdvanceTo{Applied: to, Ordinal: 23}, batch.Set(append(storage.PairPrefix(slots.owner, reconDB), "k"...), []byte("v"))
		},
		Commit: func(batch storage.KVBatch, _ bool) error { return batch.Commit(false) },
	})
	if err != nil {
		t.Fatal(err)
	}
	record, found := diskRecord(t, kv, slots.owner)
	if !found || record.State != machine.SeedReady || record.Applied != to || record.Ordinal != 23 {
		t.Errorf("the record on disk after a write is %+v (found %v), want a ready seed record at %v ordinal 23", record, found, to)
	}
	// A pair with no seed record outside the target set takes no write and no 32-byte record
	// (seeding S0: one record shape; the write side of the caller-dependent refusal).
	err = advancer.Advance(slots.outside, reconDB, AdvanceStep{
		Stage:  func(_ storage.KVBatch, _ *cluster.Pair) (AdvanceTo, error) { return AdvanceTo{Applied: to}, nil },
		Commit: func(batch storage.KVBatch, _ bool) error { return batch.Commit(false) },
	})
	if !errors.Is(err, ErrPairNotReady) {
		t.Fatalf("a write to a record-less pair outside the target set: %v, want ErrPairNotReady", err)
	}
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	if raw, err := snapshot.Get(LocalPairRecordKey(slots.outside, reconDB)); err == nil {
		t.Errorf("a pair that was never seeded and is out of the target set has a %d-byte record, want none", len(raw))
	}
}

func TestAChunkOverTheCreditTheReceiverGrantedIsRefused(t *testing.T) {
	rig := newT3Rig(t, t3Options{seed: SeedOptions{WindowBytes: 2000}})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 6, t3Cut(t, rig), 0)
	var tampered atomic.Bool
	rig.wrap = func(_ string, _ transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if reply != nil && reply.Env.Kind == transport.KindSeedChunk && tampered.CompareAndSwap(false, true) {
			return t3Tamper(t, reply, func(c *transport.SeedChunk) {
				c.Entries = append(c.Entries, transport.SeedEntry{Key: t3Key(rig.slot, reconDB, 500), Value: bytes.Repeat([]byte("v"), 3000)})
			}), nil
		}
		return reply, err
	}
	rig.pullUntil("the pull to finish after one oversized chunk", rig.isReady)
	t3SameEntries(t, t3Dump(t, rig.kv, rig.slot, reconDB), owner.dump(t, rig.slot, reconDB))
}

// TestTheCursorNeverLeadsItsDataOnTheSuccessPath: every committed batch that moves the cursor
// carries the key the cursor names (review: the success direction of the atomicity argument).
func TestTheCursorNeverLeadsItsDataOnTheSuccessPath(t *testing.T) {
	badger := t3OpenKV(t, t.TempDir())
	t.Cleanup(func() { _ = badger.Close() })
	watching := &t3RecordKV{KV: badger}
	rig := newT3Rig(t, t3Options{kv: watching})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 10, t3Cut(t, rig), 0)
	rig.pullUntil("the pair to be ready", rig.isReady)
	watching.mu.Lock()
	defer watching.mu.Unlock()
	if len(watching.leads) != 0 {
		t.Errorf("a cursor moved without its data: %v", watching.leads)
	}
}

func TestResumeJoiningRefusesANodeThatIsNotJoining(t *testing.T) {
	kv := t3OpenKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	rig := newRig(t, kv, threeNodes(t), testSelf, true)
	if err := rig.rec.LoadRecords(); err != nil {
		t.Fatal(err)
	}
	if err := rig.rec.ResumeJoining(t.Context()); err == nil {
		t.Error("ResumeJoining ran for a node that is not joining")
	}
}

// TestTheReceiverBoundsTheWindowByItsOwnSetting (re-review 2): a source may advertise any
// window, up to 0xFFFFFFFF; the credit the receiver acks and the chunk size it accepts are
// bounded by the receiver's own seed_window_bytes.
func TestTheReceiverBoundsTheWindowByItsOwnSetting(t *testing.T) {
	rig := newT3Rig(t, t3Options{seed: SeedOptions{WindowBytes: 4096}})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{WindowBytes: 1<<32 - 1, ChunkBytes: 1 << 20, ChunkEntries: 1000})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	batch := owner.kv.NewBatch()
	for number := range 12 {
		if err := batch.Set(t3Key(rig.slot, reconDB, number), bytes.Repeat([]byte("v"), 1000)); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	batch.Discard()
	owner.pairs.With(rig.slot, reconDB, func(pair *cluster.Pair) { pair.AppliedPos = t3Cut(t, rig) })
	rig.pullUntil("the pair to be ready", rig.isReady)
	for _, msg := range rig.requests(transport.KindSeedAck) {
		ack, err := transport.DecodeSeedAck(payloadOf(msg))
		if err != nil {
			t.Fatal(err)
		}
		if ack.CreditBytes > 4096 {
			t.Errorf("an ack granted %d bytes of credit, over this node's seed_window_bytes 4096", ack.CreditBytes)
		}
	}
	t3SameEntries(t, t3Dump(t, rig.kv, rig.slot, reconDB), owner.dump(t, rig.slot, reconDB))
}

// TestAnOrdinalIsWrittenOnlyWithTheReadyRecord (re-review 3).
func TestAnOrdinalIsWrittenOnlyWithTheReadyRecord(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	kv := t3OpenKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	putRecord(t, kv, slots.copyOnly, seedingRecord(machine.PhaseSnapshot, cluster.Position{Epoch: uint64(st.Epoch), Seq: 3}, "k"))
	rig := newRig(t, kv, st, testSelf, true)
	if err := rig.rec.LoadRecords(); err != nil {
		t.Fatal(err)
	}
	if _, err := rig.rec.FeedSeed(slots.copyOnly, reconDB, FeedSeedRequest{Event: machine.RegisterRefused{Reason: seedRefusedDisk}, Cfg: machine.SeedConfig{Epoch: uint64(st.Epoch)}, Ordinal: 99}); err != nil {
		t.Fatal(err)
	}
	record, _ := rig.rec.Record(slots.copyOnly, reconDB)
	if record.Ordinal != 0 {
		t.Errorf("an event that did not make the pair ready left ordinal %d in the record", record.Ordinal)
	}
}

// t3Compact collapses runs of the same event.
func t3Compact(events []string) string {
	var out []string
	for _, event := range events {
		if len(out) == 0 || out[len(out)-1] != event {
			out = append(out, event)
		}
	}
	return strings.Join(out, " ")
}

// TestABootingNodeThatCannotSeeItsStateAnswersLikeASeedingOne (re-review): through the same
// constructor the node uses, a node that has not seen the cluster state, and a joining one,
// say seeding on POSITION_QUERY, are not ready for a pushed write, and fail the activation
// gate's reading of them; an active node with no record admits a write and a pushed record
// (seeding S0) while its state answer stays absent.
func TestABootingNodeThatCannotSeeItsStateAnswersLikeASeedingOne(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	joining := threeNodes(t)
	setState(t, &joining, testSelf, "down")
	replace(t, &joining, testSelf, 2)
	for _, tc := range []struct {
		name  string
		state cluster.ClusterState
		ready bool
	}{
		{"state not loaded yet", cluster.ClusterState{}, false},
		{"joining", joining, false},
		{"active", threeNodes(t), true},
	} {
		parts := newSeedPartsFor(rig.rec, tc.state, testSelf)
		slots := recordSlots{parts: parts}
		// Seeding S0: the state answer is honest on every node (absent, so POSITION_REPLY never
		// says ready for a pair the node has no record of); only the admission varies.
		state := slots.State(rig.slot+1, reconDB+9)
		if state != machine.SlotAbsent || positionState(state) == PositionStateReady {
			t.Errorf("%s: a pair with no record is %v, want absent and not ready to POSITION_QUERY", tc.name, state)
		}
		if got := slots.Admits(rig.slot+1, reconDB+9); got != tc.ready {
			t.Errorf("%s: a write to a pair with no record is admitted: %v, want %v", tc.name, got, tc.ready)
		}
		if got := replicaReady(slots)(rig.slot+1, reconDB+9); got != tc.ready {
			t.Errorf("%s: a pushed write is applied: %v, want %v", tc.name, got, tc.ready)
		}
		// The Writer's seam is Admits first, then the state's wording (Node.peers).
		if got := slots.Admits(rig.slot+1, reconDB+9) || notReadyCause(state) == ""; got != tc.ready {
			t.Errorf("%s: the writer sees the pair ready: %v, want %v", tc.name, got, tc.ready)
		}
	}
}

// TestASeededPairsOrdinalComesFromFormatThree is storage-format slice A's F2 test of the
// seed's ordinal path: the highest ordinal of a pair range of format-3 entries is what the
// pull adopts (an ordinal of 0 would let the first sweep reap the seeded window), and one
// damaged entry in the range is an error naming it.
func TestASeededPairsOrdinalComesFromFormatThree(t *testing.T) {
	badger, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("OpenBadgerKV: %v", err)
	}
	t.Cleanup(func() { _ = badger.Close() })
	const slot, db = uint16(9), uint64(4)
	ref := storage.BucketRef{Slot: slot, DatabaseID: db, BucketID: 41}
	for i, ordinal := range []uint64{5_000_000, 7, 2 << 30, 300} {
		u31PutEntry(t, badger, ref, machine.DedupeEntry{Key: machine.DedupeKey{RequestID: [16]byte{byte(i + 1)}}, Pos: cluster.Position{Epoch: 1, Seq: uint64(i + 1)}, Ordinal: ordinal})
	}
	puller := &Puller{cfg: PullerConfig{KV: badger}}
	got, err := puller.highestOrdinal(context.Background(), reconcileKey{slot: slot, db: db})
	if err != nil || got != 2<<30 {
		t.Fatalf("highestOrdinal = %d, %v; want %d", got, err, 2<<30)
	}

	damagedKey := DedupeEntryKey(ref, machine.DedupeKey{RequestID: [16]byte{0xD0}})
	batch := badger.NewBatch()
	if err := batch.Set(damagedKey, []byte{0x03, 0x80, 0x00}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	if _, err := puller.highestOrdinal(context.Background(), reconcileKey{slot: slot, db: db}); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("%x", damagedKey)) {
		t.Fatalf("highestOrdinal over a damaged entry = %v, want an error naming %x", err, damagedKey)
	}
}
