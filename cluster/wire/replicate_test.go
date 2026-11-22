package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

const (
	u32Epoch  uint64 = 7
	u32DB     uint64 = 3
	u32Bucket uint64 = 41
	u32Slot   uint16 = 5
)

// u32Link is the data channel between two nodes of a test. Writes are recorded, not
// delivered: a test hands each frame to the copy itself, which is what makes the order
// and the window observable.
type u32Link struct {
	mu       sync.Mutex
	frames   map[string][]WriteFrame
	acks     []WriteAck
	writeErr error
	source   *Replicator
	owner    *Replicator
	requests []CatchUpRequest
	// afterCatchUp, when set, runs after the source has served a request and before the copy
	// sees the chunk: the moment a test flips the copy's record under a running catch-up.
	afterCatchUp func(CatchUpRequest)
	// beforeCatchUp, when set, runs before the source is asked, with the request's context,
	// as a real link's call does; an error it returns is the request's.
	beforeCatchUp func(context.Context, CatchUpRequest) error
}

func newU32Link() *u32Link { return &u32Link{frames: map[string][]WriteFrame{}} }

func (l *u32Link) Write(_ context.Context, peer string, frame WriteFrame) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.writeErr != nil {
		return l.writeErr
	}
	l.frames[peer] = append(l.frames[peer], frame)
	return nil
}

func (l *u32Link) Ack(_ context.Context, _ string, ack WriteAck) error {
	l.mu.Lock()
	l.acks = append(l.acks, ack)
	owner := l.owner
	l.mu.Unlock()
	if owner != nil {
		owner.OnWriteAck(ack)
	}
	return nil
}

func (l *u32Link) CatchUp(ctx context.Context, _ string, req CatchUpRequest) (CatchUpChunk, error) {
	l.mu.Lock()
	l.requests = append(l.requests, req)
	source, afterServe, beforeServe := l.source, l.afterCatchUp, l.beforeCatchUp
	l.mu.Unlock()
	if beforeServe != nil {
		if err := beforeServe(ctx, req); err != nil {
			return CatchUpChunk{}, err
		}
	}
	chunk, err := source.ServeCatchUp(ctx, req)
	if afterServe != nil {
		afterServe(req)
	}
	return chunk, err
}

func (l *u32Link) sent(peer string) []WriteFrame {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]WriteFrame(nil), l.frames[peer]...)
}

type u32Seeds struct {
	mu     sync.Mutex
	calls  []pairKey
	causes []ReseedCause
}

func (s *u32Seeds) ResetAndSeed(slot uint16, db uint64, cause ReseedCause) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, pairKey{slot, db})
	s.causes = append(s.causes, cause)
	return nil
}

func (s *u32Seeds) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

// u32Node is one node: a real WAL and a real Badger, and a Replicator over them.
type u32Node struct {
	t         *testing.T
	id        string
	wal       *storage.WAL
	kv        *storage.BadgerKV
	pairs     *cluster.PairSet
	link      *u32Link
	seeds     *u32Seeds
	rep       *Replicator
	committed atomic.Uint64
	copies    []string
	ownerID   string
	sourceID  string
	floor     atomic.Uint64
	ready     atomic.Bool
	errs      chan error
}

func newU32Node(t *testing.T, id string, mutate func(*ReplicatorConfig)) *u32Node {
	t.Helper()
	wal, err := storage.OpenWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	t.Cleanup(func() {
		if err := wal.Close(); err != nil {
			t.Errorf("closing the WAL: %v", err)
		}
	})
	badger, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("OpenBadgerKV: %v", err)
	}
	t.Cleanup(func() {
		if err := badger.Close(); err != nil {
			t.Errorf("closing Badger: %v", err)
		}
	})
	node := &u32Node{t: t, id: id, wal: wal, kv: badger, pairs: &cluster.PairSet{}, link: newU32Link(), seeds: &u32Seeds{}, errs: make(chan error, 16)}
	node.committed.Store(u32Epoch)
	node.floor.Store(1)
	node.ready.Store(true)
	cfg := ReplicatorConfig{
		Self: id, Committed: node.committed.Load, Pairs: node.pairs, Log: wal, KV: badger,
		Applier: NewDocumentApplier(badger, nil), Link: node.link, Seeds: node.seeds,
		Copies:        func(uint16, uint64) []string { return node.copies },
		Owner:         func(uint16, uint64) (string, bool) { return node.ownerID, node.ownerID != "" },
		CatchUpSource: func(uint16, uint64) (string, error) { return node.sourceID, nil },
		FloorCursor:   func(uint16, uint64) uint64 { return node.floor.Load() },
		Ready:         func(uint16, uint64) bool { return node.ready.Load() },
		OnError:       func(err error) { node.errs <- err },
	}
	if mutate != nil {
		mutate(&cfg)
	}
	if cfg.Advancer == nil {
		// Built after mutate, so a test that wraps cfg.KV counts the advancer's batches too.
		cfg.Advancer = NewLocalAdvancer(node.pairs, cfg.KV)
	}
	node.rep, err = NewReplicator(cfg)
	if err != nil {
		t.Fatalf("NewReplicator: %v", err)
	}
	return node
}

// mustReplicator builds a Replicator for a test whose wiring is valid.
func mustReplicator(t *testing.T, cfg ReplicatorConfig) *Replicator {
	t.Helper()
	replicator, err := NewReplicator(cfg)
	if err != nil {
		t.Fatalf("NewReplicator: %v", err)
	}
	return replicator
}

func u32Pos(epoch, seq uint64) cluster.Position { return cluster.Position{Epoch: epoch, Seq: seq} }

func u32Insert(t *testing.T, id string, prevEnd cluster.Position) cluster.Op {
	t.Helper()
	encoded, err := store.EncodeComponent(id)
	if err != nil {
		t.Fatalf("EncodeComponent: %v", err)
	}
	idValue := append([]byte{0x02, byte(len(id) + 1), 0, 0, 0}, append([]byte(id), 0)...)
	return cluster.Op{
		Class: cluster.OpInsert, BucketID: u32Bucket, DeclVersion: 7, SchemaVersionSeen: 7, PrevEnd: prevEnd,
		EncID: encoded, IDValue: idValue,
		Fields: []cluster.FieldOp{{FieldID: 2, Value: []byte{0x02, 4, 0, 0, 0, 'A', 'd', 'a', 0}}},
	}
}

func u32ID(pos cluster.Position) string { return fmt.Sprintf("p%d-%d", pos.Epoch, pos.Seq) }

// u32Record is the record the owner ordered at pos; an opener carries prevEnd.
func u32Record(t *testing.T, pos, prevEnd cluster.Position) PairRecord {
	t.Helper()
	if pos.Seq != 1 {
		prevEnd = cluster.Position{}
	}
	body, err := cluster.EncodeOp(u32Insert(t, u32ID(pos), prevEnd))
	if err != nil {
		t.Fatalf("EncodeOp: %v", err)
	}
	var requestID [16]byte
	binary.BigEndian.PutUint64(requestID[:8], pos.Epoch)
	binary.BigEndian.PutUint64(requestID[8:], pos.Seq)
	return PairRecord{Pos: pos, Record: storage.WALRecord{Class: cluster.OpInsert, RequestID: requestID, Body: body}}
}

func u32Frame(t *testing.T, epoch uint64, pos, prevEnd cluster.Position) WriteFrame {
	t.Helper()
	return WriteFrame{Slot: u32Slot, DB: u32DB, From: "owner", Epoch: epoch, PairRecord: u32Record(t, pos, prevEnd)}
}

// ownAppend appends pos for slot to this node's log as the owner would, and moves the
// pair's applied and appended state.
func (n *u32Node) ownAppend(slot uint16, pos, prevEnd cluster.Position) {
	n.t.Helper()
	rec := u32Record(n.t, pos, prevEnd)
	record := storage.EncodeWALRecord(0, rec.Record.RequestID, rec.Record.Body)
	cursor, err := n.wal.AppendFrom("3.41", rec.Record.Class, storage.PositionKey(slot, u32DB, storage.LogPosition(pos)), record, storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: pos.Epoch})
	if err != nil {
		n.t.Fatalf("AppendFrom %s: %v", pos, err)
	}
	n.pairs.With(slot, u32DB, func(p *cluster.Pair) {
		p.AppliedPos, p.AppendedSeq, p.AppendedEpoch, p.TailCursor = pos, pos.Seq, pos.Epoch, cursor
	})
}

func (n *u32Node) ownRun(epoch, first, last uint64) {
	n.t.Helper()
	for seq := first; seq <= last; seq++ {
		n.ownAppend(u32Slot, u32Pos(epoch, seq), cluster.Position{})
	}
}

func (n *u32Node) applied() cluster.Position {
	return n.pairs.Snapshot(u32Slot, u32DB).AppliedPos
}

func (n *u32Node) hasDocument(pos cluster.Position) bool {
	n.t.Helper()
	encoded, err := store.EncodeComponent(u32ID(pos))
	if err != nil {
		n.t.Fatalf("EncodeComponent: %v", err)
	}
	snapshot := n.kv.NewSnapshot()
	defer snapshot.Discard()
	_, err = snapshot.Get(store.DocumentPrefix(u32Slot, u32DB, u32Bucket, encoded))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return false
	}
	if err != nil {
		n.t.Fatalf("reading document %s: %v", pos, err)
	}
	return true
}

func (n *u32Node) write(frame WriteFrame) WriteResult {
	n.t.Helper()
	reply, err := n.rep.OnWrite(frame)
	if err != nil {
		n.t.Fatalf("OnWrite %s: %v", frame.Pos, err)
	}
	return reply
}

func u32Code(err error) cluster.Code {
	var coded *cluster.Error
	if errors.As(err, &coded) {
		return coded.Code
	}
	return 0
}

func TestACopyAppliesOnlyFromAppliedPlusOne(t *testing.T) {
	copyNode := newU32Node(t, "copy", nil)

	first := copyNode.write(u32Frame(t, u32Epoch, u32Pos(7, 1), cluster.Position{}))
	if first.Ack.Outcome != AckApplied || first.Ack.Pos != u32Pos(7, 1) {
		t.Fatalf("the opener: ack %+v, want applied at 7:1", first.Ack)
	}

	gap := copyNode.write(u32Frame(t, u32Epoch, u32Pos(7, 3), cluster.Position{}))
	if gap.Ack.Outcome != AckRefused || gap.Ack.Pos != u32Pos(7, 1) {
		t.Fatalf("7:3 while at 7:1: ack %+v, want refused with the copy still at 7:1", gap.Ack)
	}
	if gap.CatchUpFrom == nil || *gap.CatchUpFrom != u32Pos(7, 2) {
		t.Fatalf("7:3 while at 7:1 asks for %v, want a catch-up from 7:2", gap.CatchUpFrom)
	}
	if got := copyNode.applied(); got != u32Pos(7, 1) {
		t.Fatalf("a refused gap moved the applied position to %s", got)
	}
	if copyNode.hasDocument(u32Pos(7, 3)) || copyNode.wal.LastSequence() != 1 {
		t.Fatalf("a refused gap left a document or a log entry behind: log at %d", copyNode.wal.LastSequence())
	}

	for _, seq := range []uint64{2, 3} {
		next := copyNode.write(u32Frame(t, u32Epoch, u32Pos(7, seq), cluster.Position{}))
		if next.Ack.Outcome != AckApplied || next.Ack.Pos != u32Pos(7, seq) {
			t.Fatalf("7:%d in order: ack %+v", seq, next.Ack)
		}
	}
	ordinal := copyNode.pairs.Snapshot(u32Slot, u32DB).Ordinal
	again := copyNode.write(u32Frame(t, u32Epoch, u32Pos(7, 2), cluster.Position{}))
	if again.Ack.Outcome != AckApplied || again.Ack.Pos != u32Pos(7, 3) {
		t.Fatalf("a duplicate of 7:2: ack %+v, want applied and cumulative at 7:3", again.Ack)
	}
	if after := copyNode.pairs.Snapshot(u32Slot, u32DB).Ordinal; after != ordinal || copyNode.wal.LastSequence() != 3 {
		t.Fatalf("a duplicate moved the ordinal %d to %d or grew the log to %d", ordinal, after, copyNode.wal.LastSequence())
	}
}

// TestACopyPersistsItsPairRecordWithEachAppliedRecord (BUG-0752): the copy is built the way
// production builds it, with an applier that records no pair, and the pair record must still
// land in the batch of each record it applies. A second PairSet loaded from the same Badger
// is the restarted copy: it must hold the last frame's position, ordinal and log cursor.
// [BREAK: Replicator.applyDurably commits the data through commit() without the AdvanceStep]
func TestACopyPersistsItsPairRecordWithEachAppliedRecord(t *testing.T) {
	copyNode := newU32Node(t, "copy", nil)
	copyNode.ownerID = "owner"
	var prevEnd cluster.Position
	for seq := uint64(1); seq <= 3; seq++ {
		pos := u32Pos(u32Epoch, seq)
		if reply := copyNode.write(u32Frame(t, u32Epoch, pos, prevEnd)); reply.Ack.Outcome != AckApplied {
			t.Fatalf("frame %s: outcome %v, want applied", pos, reply.Ack.Outcome)
		}
		live := copyNode.pairs.Snapshot(u32Slot, u32DB)

		restarted := &cluster.PairSet{}
		if _, err := LoadLocalPairRecords(copyNode.kv, restarted, false); err != nil {
			t.Fatalf("loading the pair records after frame %s: %v", pos, err)
		}
		got := restarted.Snapshot(u32Slot, u32DB)
		if got.AppliedPos != pos || got.Ordinal != seq || got.TailCursor != live.TailCursor || got.TailCursor == 0 {
			t.Fatalf("after frame %s a restart loads position %s ordinal %d cursor %d, want %s, %d and the live cursor %d",
				pos, got.AppliedPos, got.Ordinal, got.TailCursor, pos, seq, live.TailCursor)
		}
	}
}

func TestAFencedOrUndecodableRecordChangesNothing(t *testing.T) {
	copyNode := newU32Node(t, "copy", nil)
	copyNode.write(u32Frame(t, u32Epoch, u32Pos(7, 1), cluster.Position{}))

	if _, err := copyNode.rep.OnWrite(u32Frame(t, u32Epoch+1, u32Pos(7, 2), cluster.Position{})); u32Code(err) != cluster.CodeEpochFenced {
		t.Fatalf("a frame from another epoch: %v, want EPOCH_FENCED", err)
	}
	broken := u32Frame(t, u32Epoch, u32Pos(7, 2), cluster.Position{})
	broken.Record.Body = broken.Record.Body[:5]
	if _, err := copyNode.rep.OnWrite(broken); u32Code(err) != cluster.CodeInvalidArgument {
		t.Fatalf("a record that does not decode: %v, want INVALID_ARGUMENT", err)
	}
	if copyNode.applied() != u32Pos(7, 1) || copyNode.wal.LastSequence() != 1 || copyNode.hasDocument(u32Pos(7, 2)) {
		t.Fatalf("a refused record changed the pair: applied %s, log %d", copyNode.applied(), copyNode.wal.LastSequence())
	}
	copyNode.write(u32Frame(t, u32Epoch, u32Pos(7, 2), cluster.Position{}))
}

func TestASeedingCopyAppliesNothing(t *testing.T) {
	copyNode := newU32Node(t, "copy", nil)
	copyNode.ready.Store(false)
	reply := copyNode.write(u32Frame(t, u32Epoch, u32Pos(7, 1), cluster.Position{}))
	if reply.Ack.Outcome != AckNotApplied || copyNode.wal.LastSequence() != 0 {
		t.Fatalf("a seeding copy answered %+v and wrote %d log entries", reply.Ack, copyNode.wal.LastSequence())
	}
}

func TestDivergentTailResetsAndSeeds(t *testing.T) {
	copyNode := newU32Node(t, "copy", nil)
	for seq := uint64(1); seq <= 3; seq++ {
		copyNode.write(u32Frame(t, u32Epoch, u32Pos(7, seq), cluster.Position{}))
	}
	copyNode.committed.Store(8)

	// Behind the boundary: catch up, and reset nothing.
	behind := copyNode.write(u32Frame(t, 8, u32Pos(8, 1), u32Pos(7, 5)))
	if copyNode.seeds.count() != 0 || behind.CatchUpFrom == nil || *behind.CatchUpFrom != u32Pos(7, 4) {
		t.Fatalf("an opener past this copy: resets %d, asks %v, want a catch-up from 7:4", copyNode.seeds.count(), behind.CatchUpFrom)
	}

	// Ahead of the boundary: a tail nobody owns.
	reply := copyNode.write(u32Frame(t, 8, u32Pos(8, 1), u32Pos(7, 2)))
	if copyNode.seeds.count() != 1 || reply.Ack.Outcome != AckRefused {
		t.Fatalf("an opener behind this copy: resets %d, ack %+v, want one reset and a refusal", copyNode.seeds.count(), reply.Ack)
	}
	if copyNode.applied() != u32Pos(7, 3) || copyNode.wal.LastSequence() != 3 || copyNode.hasDocument(u32Pos(8, 1)) {
		t.Fatalf("the stream itself merged or discarded the tail: applied %s, log %d", copyNode.applied(), copyNode.wal.LastSequence())
	}

	// The boundary matches: the opener is accepted and nothing resets.
	exact := copyNode.write(u32Frame(t, 8, u32Pos(8, 1), u32Pos(7, 3)))
	if exact.Ack.Outcome != AckApplied || copyNode.seeds.count() != 1 {
		t.Fatalf("an opener at the boundary: ack %+v, resets %d", exact.Ack, copyNode.seeds.count())
	}
}

func TestSlowCopyGrowsNoBuffer(t *testing.T) {
	owner := newU32Node(t, "owner", nil)
	owner.copies = []string{"slow", "fast"}
	owner.ownRun(u32Epoch, 1, 200)
	ctx := context.Background()
	for seq := uint64(1); seq <= 200; seq++ {
		owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, seq))
	}
	flushUntilQuiet(t, owner, func() { // the fast copy acks everything it is sent
		for _, frame := range owner.link.sent("fast") {
			owner.rep.OnWriteAck(WriteAck{From: "fast", Slot: u32Slot, DB: u32DB, Pos: frame.Pos, Outcome: AckApplied})
		}
	})

	slow := owner.link.sent("slow")
	if len(slow) != defaultWindow {
		t.Fatalf("the copy that never acks was sent %d frames, want the window of %d", len(slow), defaultWindow)
	}
	for i, frame := range slow {
		if frame.Pos != u32Pos(u32Epoch, uint64(i+1)) {
			t.Fatalf("frame %d to the slow copy is %s, want position order", i, frame.Pos)
		}
	}
	if got := len(owner.link.sent("fast")); got != 200 {
		t.Fatalf("the fast copy was sent %d frames, want all 200: a slow copy must not hold it up", got)
	}
	if held := owner.rep.heldFrames(u32Slot, u32DB, "slow"); held > defaultWindow {
		t.Fatalf("the sender holds %d frames for the slow copy, want at most %d", held, defaultWindow)
	}

	// Acking part of the window opens exactly that much room.
	owner.rep.OnWriteAck(WriteAck{From: "slow", Slot: u32Slot, DB: u32DB, Pos: u32Pos(u32Epoch, 10), Outcome: AckApplied})
	if err := owner.flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := len(owner.link.sent("slow")); got != defaultWindow+10 {
		t.Fatalf("after acking 10 frames the slow copy was sent %d, want %d", got, defaultWindow+10)
	}
	if held := owner.rep.heldFrames(u32Slot, u32DB, "slow"); held != defaultWindow {
		t.Fatalf("after the ack the sender holds %d frames, want the full window %d", held, defaultWindow)
	}
}

// flushUntilQuiet flushes, lets react see what was sent, and repeats until a flush sends
// nothing new.
func flushUntilQuiet(t *testing.T, node *u32Node, react func()) {
	t.Helper()
	for round := 0; round < 100; round++ {
		before := node.link.total()
		if err := node.flush(context.Background()); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		react()
		if node.link.total() == before {
			return
		}
	}
	t.Fatal("the stream never went quiet")
}

func (l *u32Link) total() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	count := 0
	for _, frames := range l.frames {
		count += len(frames)
	}
	return count
}

func TestPushShipsOnePairInPositionOrderFromASharedLog(t *testing.T) {
	owner := newU32Node(t, "owner", nil)
	owner.copies = []string{"copy"}
	for seq := uint64(1); seq <= 5; seq++ {
		owner.ownAppend(u32Slot, u32Pos(u32Epoch, seq), cluster.Position{})
		owner.ownAppend(u32Slot+1, u32Pos(u32Epoch, seq), cluster.Position{})
		owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, seq))
	}
	if err := owner.flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	frames := owner.link.sent("copy")
	if len(frames) != 5 {
		t.Fatalf("shipped %d frames, want the 5 of slot %d only", len(frames), u32Slot)
	}
	for i, frame := range frames {
		if frame.Slot != u32Slot || frame.Pos != u32Pos(u32Epoch, uint64(i+1)) || frame.Epoch != u32Epoch || frame.From != "owner" {
			t.Fatalf("frame %d is %+v", i, frame)
		}
	}
}

func TestARefusingCopyIsNotPushedUntilItAnswersApplied(t *testing.T) {
	owner := newU32Node(t, "owner", nil)
	owner.copies = []string{"copy"}
	owner.ownRun(u32Epoch, 1, 6)
	owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, 6))
	if err := owner.flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := len(owner.link.sent("copy")); got != 6 {
		t.Fatalf("shipped %d frames, want 6", got)
	}

	owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Pos: u32Pos(u32Epoch, 2), Outcome: AckRefused})
	owner.ownRun(u32Epoch, 7, 7)
	owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, 7))
	if err := owner.flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := len(owner.link.sent("copy")); got != 6 {
		t.Fatalf("a refusing copy was sent %d frames, want it left alone at 6", got)
	}

	// Its catch-up applies 2 to 5, and the ack that says so resumes the push after it.
	owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Pos: u32Pos(u32Epoch, 5), Outcome: AckApplied})
	if err := owner.flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	frames := owner.link.sent("copy")
	if len(frames) != 8 || frames[6].Pos != u32Pos(u32Epoch, 6) || frames[7].Pos != u32Pos(u32Epoch, 7) {
		t.Fatalf("after the applied ack the push resumed with %d frames, want 6:6 and 7:7 appended", len(frames))
	}
}

func TestASendFailureShipsAgainFromTheLastAck(t *testing.T) {
	owner := newU32Node(t, "owner", nil)
	owner.copies = []string{"copy"}
	owner.ownRun(u32Epoch, 1, 3)
	owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, 3))
	owner.link.mu.Lock()
	owner.link.writeErr = errors.New("connection reset")
	owner.link.mu.Unlock()
	if err := owner.flush(context.Background()); err == nil {
		t.Fatal("a failed send was not reported")
	}
	owner.link.mu.Lock()
	owner.link.writeErr = nil
	owner.link.mu.Unlock()
	if err := owner.flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := len(owner.link.sent("copy")); got != 3 {
		t.Fatalf("after the failure %d frames were sent, want all 3", got)
	}
}

func TestAwaitAckedAndCovered(t *testing.T) {
	owner := newU32Node(t, "owner", nil)
	owner.copies = []string{"b", "c"}
	owner.ownerID = "b"
	pos := u32Pos(u32Epoch, 4)

	if got := owner.rep.Acked(u32Slot, u32DB, pos); got != 0 {
		t.Fatalf("Acked before any ack = %d", got)
	}
	done := make(chan int, 1)
	go func() { done <- owner.rep.Await(context.Background(), u32Slot, u32DB, pos, 2) }()
	owner.rep.OnWriteAck(WriteAck{From: "b", Slot: u32Slot, DB: u32DB, Pos: u32Pos(u32Epoch, 9), Outcome: AckApplied})
	owner.rep.OnWriteAck(WriteAck{From: "c", Slot: u32Slot, DB: u32DB, Pos: u32Pos(u32Epoch, 3), Outcome: AckApplied})
	select {
	case <-done:
		t.Fatal("Await returned with one copy below the position")
	case <-time.After(20 * time.Millisecond):
	}
	owner.rep.OnWriteAck(WriteAck{From: "c", Slot: u32Slot, DB: u32DB, Pos: pos, Outcome: AckApplied})
	if got := <-done; got != 2 {
		t.Fatalf("Await = %d, want 2", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := owner.rep.Await(ctx, u32Slot, u32DB, u32Pos(u32Epoch, 99), 2); got != 0 {
		t.Fatalf("Await past every ack with a dead context = %d, want the count it has: 0", got)
	}

	if !owner.rep.Covered(u32Slot, u32DB, u32Pos(u32Epoch, 9)) || owner.rep.Covered(u32Slot, u32DB, u32Pos(u32Epoch, 10)) {
		t.Fatal("Covered must follow the ack of the copy that owns the pair now")
	}
	owner.rep.NoteDrained(u32Slot, u32DB, u32Pos(u32Epoch, 12))
	if !owner.rep.Covered(u32Slot, u32DB, u32Pos(u32Epoch, 10)) {
		t.Fatal("Covered must follow a HANDOFF_READY drain")
	}
}

func TestRunShipsWhatPushQueuedAndCloseEndsIt(t *testing.T) {
	owner := newU32Node(t, "owner", nil)
	owner.copies = []string{"copy"}
	owner.ownRun(u32Epoch, 1, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	go func() {
		close(started)
		owner.rep.Run(ctx)
	}()
	<-started
	owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, 2))
	deadline := time.Now().Add(5 * time.Second)
	for len(owner.link.sent("copy")) < 2 {
		if time.Now().After(deadline) {
			t.Fatal("Run never shipped the pushed positions")
		}
		time.Sleep(time.Millisecond)
	}
	owner.rep.Close()
}

// serve connects copyNode's catch-up to source.
func (n *u32Node) catchUpFrom(source *u32Node, owner *u32Node) {
	n.sourceID = source.id
	n.ownerID = owner.id
	n.link.mu.Lock()
	n.link.source = source.rep
	n.link.owner = owner.rep
	n.link.mu.Unlock()
}

func TestCatchUpIsCurrentOnlyOnTheCompletionMarker(t *testing.T) {
	source := newU32Node(t, "source", func(cfg *ReplicatorConfig) { cfg.ChunkRecords = 4 })
	source.ownRun(u32Epoch, 1, 10)
	// An ack counts only from a node in the pair's current copy set.
	source.copies = []string{"copy"}
	copyNode := newU32Node(t, "copy", nil)
	copyNode.catchUpFrom(source, source)

	if err := copyNode.rep.CatchUp(context.Background(), u32Slot, u32DB, u32Pos(0, 1), 1); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}
	if got := copyNode.applied(); got != u32Pos(u32Epoch, 10) {
		t.Fatalf("applied %s, want 7:10", got)
	}
	if !copyNode.rep.IsCurrent(u32Slot, u32DB) {
		t.Fatal("a catch-up that reached the source's tail did not mark the pair current")
	}
	requests := copyNode.link.requests
	if len(requests) != 3 || requests[1].From != u32Pos(u32Epoch, 5) || requests[2].From != u32Pos(u32Epoch, 9) {
		t.Fatalf("requests %+v, want three rounds re-requesting from the new applied position", requests)
	}
	if len(copyNode.link.acks) != 3 || copyNode.link.acks[2].Pos != u32Pos(u32Epoch, 10) {
		t.Fatalf("acks %+v, want a cumulative ack after each chunk, the last at 7:10", copyNode.link.acks)
	}
	if got := source.rep.Acked(u32Slot, u32DB, u32Pos(u32Epoch, 10)); got != 1 {
		t.Fatalf("the owner counts %d copies at 7:10 after the catch-up, want 1", got)
	}
}

func TestCatchUpReportsTheSourceAppendedPosition(t *testing.T) {
	source := newU32Node(t, "source", func(cfg *ReplicatorConfig) { cfg.ChunkRecords = 5 })
	source.ownRun(u32Epoch, 1, 50)
	ctx := context.Background()

	chunk, err := source.rep.ServeCatchUp(ctx, CatchUpRequest{Slot: u32Slot, DB: u32DB, From: u32Pos(u32Epoch, 10)})
	if err != nil {
		t.Fatalf("ServeCatchUp: %v", err)
	}
	if chunk.SourceAppended != u32Pos(u32Epoch, 50) {
		t.Fatalf("SourceAppended %s, want the source's tail 7:50, not the requested position", chunk.SourceAppended)
	}
	if len(chunk.Records) != 5 || chunk.LastPosition != u32Pos(u32Epoch, 14) || chunk.CaughtUp || current(chunk) {
		t.Fatalf("a capped chunk: %d records, last %s, caughtUp %v, current %v", len(chunk.Records), chunk.LastPosition, chunk.CaughtUp, current(chunk))
	}

	tail, err := source.rep.ServeCatchUp(ctx, CatchUpRequest{Slot: u32Slot, DB: u32DB, From: u32Pos(u32Epoch, 46)})
	if err != nil {
		t.Fatalf("ServeCatchUp: %v", err)
	}
	if !tail.CaughtUp || !current(tail) || tail.LastPosition != u32Pos(u32Epoch, 50) {
		t.Fatalf("the last chunk: caughtUp %v, last %s, want current at 7:50", tail.CaughtUp, tail.LastPosition)
	}

	beyond, err := source.rep.ServeCatchUp(ctx, CatchUpRequest{Slot: u32Slot, DB: u32DB, From: u32Pos(u32Epoch, 51)})
	if err != nil || !current(beyond) || len(beyond.Records) != 0 {
		t.Fatalf("a requester that holds everything: %+v, %v, want an empty current chunk", beyond, err)
	}
}

// u32TruncateHonoringPins is retention as conv §9 states it: it never cuts at or above
// a floor an in-flight catch-up pinned.
func u32TruncateHonoringPins(t *testing.T, node *u32Node) {
	t.Helper()
	upTo := node.wal.LastSequence()
	if floor, pinned := node.rep.PinnedFloor(); pinned {
		if floor <= 1 {
			return
		}
		upTo = min(upTo, floor-1)
	}
	if err := node.wal.Truncate(upTo); err != nil {
		t.Errorf("Truncate: %v", err)
	}
}

// u32NeverAShortRange fails unless the answer is the horizon, a chunk that is not caught
// up, or a caught-up chunk that really holds every record from 7:1 to the tail.
func u32NeverAShortRange(t *testing.T, chunk CatchUpChunk, err error, tail uint64) {
	t.Helper()
	if err != nil {
		if u32Code(err) != cluster.CodeLogBelowHorizon {
			t.Fatalf("a raced read answered %v, want LOG_BELOW_HORIZON or a chunk", err)
		}
		return
	}
	if !chunk.CaughtUp {
		if current(chunk) {
			t.Fatal("a chunk that is not caught up is current")
		}
		return
	}
	if uint64(len(chunk.Records)) != tail || chunk.Records[0].Pos != u32Pos(u32Epoch, 1) || chunk.LastPosition != u32Pos(u32Epoch, tail) {
		t.Fatalf("a short range marked caught up: %d records of %d, last %s", len(chunk.Records), tail, chunk.LastPosition)
	}
}

func TestTruncationDuringACatchupIsNotSilent(t *testing.T) {
	req := CatchUpRequest{Slot: u32Slot, DB: u32DB, From: u32Pos(u32Epoch, 1)}

	// Retention that honours the pin cannot cut under the read: the answer is whole.
	pinned := newU32Node(t, "pinned", nil)
	pinned.wal.SetMaxSegmentBytes(2048)
	pinned.ownRun(u32Epoch, 1, 80)
	var once sync.Once
	pinned.rep.catchUpHook = func() { once.Do(func() { u32TruncateHonoringPins(t, pinned) }) }
	chunk, err := pinned.rep.ServeCatchUp(context.Background(), req)
	u32NeverAShortRange(t, chunk, err, 80)
	if err != nil || !chunk.CaughtUp || len(chunk.Records) != 80 {
		t.Fatalf("with the floor pinned the catch-up must complete: %v, caughtUp %v, %d records", err, chunk.CaughtUp, len(chunk.Records))
	}

	// The byte cap wins over the pin: the cut happens under the read.
	capped := newU32Node(t, "capped", nil)
	capped.wal.SetMaxSegmentBytes(2048)
	capped.ownRun(u32Epoch, 1, 80)
	var cut sync.Once
	capped.rep.catchUpHook = func() {
		cut.Do(func() {
			if err := capped.wal.Truncate(capped.wal.LastSequence()); err != nil {
				t.Errorf("Truncate: %v", err)
			}
		})
	}
	chunk, err = capped.rep.ServeCatchUp(context.Background(), req)
	u32NeverAShortRange(t, chunk, err, 80)
	if err == nil && (chunk.CaughtUp || current(chunk)) {
		t.Fatalf("a truncation raced the read and the chunk says caughtUp %v, current %v", chunk.CaughtUp, current(chunk))
	}
	_, err = capped.rep.ServeCatchUp(context.Background(), req)
	if u32Code(err) != cluster.CodeLogBelowHorizon {
		t.Fatalf("the re-request: %v, want LOG_BELOW_HORIZON, never a short range", err)
	}

	copyNode := newU32Node(t, "copy", nil)
	copyNode.catchUpFrom(capped, capped)
	err = copyNode.rep.CatchUp(context.Background(), u32Slot, u32DB, u32Pos(u32Epoch, 1), 1)
	if !errors.Is(err, ErrResetAndSeed) {
		t.Fatalf("CatchUp over a truncated range: %v, want ErrResetAndSeed", err)
	}
	if copyNode.rep.IsCurrent(u32Slot, u32DB) || copyNode.applied() != (cluster.Position{}) {
		t.Fatalf("a copy missing records became current or moved: applied %s", copyNode.applied())
	}
}

// TestACatchUpAnsweredBelowTheHorizonResetsThePair: a source that no longer holds what the copy
// needs answers LOG_BELOW_HORIZON, and the copy's catch-up asks the Seeder to reset and seed
// the pair with the horizon as the cause, instead of returning the answer and stopping
// (seeding-completion plan §1.1). [BREAK: return the error as before]
func TestACatchUpAnsweredBelowTheHorizonResetsThePair(t *testing.T) {
	source := newU32Node(t, "source", nil)
	source.ownRun(u32Epoch, 5, 9) // 7:1 to 7:4 are gone
	copyNode := newU32Node(t, "copy", nil)
	copyNode.catchUpFrom(source, source)

	err := copyNode.rep.CatchUp(context.Background(), u32Slot, u32DB, u32Pos(u32Epoch, 1), 1)
	if !errors.Is(err, ErrResetAndSeed) {
		t.Fatalf("CatchUp answered %v, want ErrResetAndSeed", err)
	}
	if copyNode.seeds.count() != 1 || copyNode.seeds.causes[0] != ReseedHorizon {
		t.Fatalf("the Seeder was called %d times with causes %v, want once with ReseedHorizon", copyNode.seeds.count(), copyNode.seeds.causes)
	}
	if copyNode.rep.IsCurrent(u32Slot, u32DB) {
		t.Fatal("a copy that was reset is current")
	}
}

func TestACatchUpOverAHoleInTheFrontIsTheHorizon(t *testing.T) {
	source := newU32Node(t, "source", nil)
	source.ownRun(u32Epoch, 5, 9) // 7:1 to 7:4 never existed here
	_, err := source.rep.ServeCatchUp(context.Background(), CatchUpRequest{Slot: u32Slot, DB: u32DB, From: u32Pos(u32Epoch, 1)})
	if u32Code(err) != cluster.CodeLogBelowHorizon {
		t.Fatalf("a range whose front is missing: %v, want LOG_BELOW_HORIZON", err)
	}
}

func TestACatchUpPinsTheFloorForTheLifeOfTheRequest(t *testing.T) {
	source := newU32Node(t, "source", nil)
	source.ownRun(u32Epoch, 1, 3)
	var seen uint64
	var pinned bool
	source.rep.catchUpHook = func() { seen, pinned = source.rep.PinnedFloor() }
	if _, err := source.rep.ServeCatchUp(context.Background(), CatchUpRequest{Slot: u32Slot, DB: u32DB, From: u32Pos(u32Epoch, 1)}); err != nil {
		t.Fatalf("ServeCatchUp: %v", err)
	}
	if !pinned || seen != 1 {
		t.Fatalf("during the request the floor was pinned=%v at %d, want pinned at 1", pinned, seen)
	}
	if _, still := source.rep.PinnedFloor(); still {
		t.Fatal("the pin outlived the request")
	}
}

func TestACatchUpThatFindsADivergentTailResets(t *testing.T) {
	source := newU32Node(t, "source", nil)
	source.ownRun(u32Epoch, 1, 2)
	source.committed.Store(8)
	source.ownAppend(u32Slot, u32Pos(8, 1), u32Pos(7, 2))
	copyNode := newU32Node(t, "copy", nil)
	for seq := uint64(1); seq <= 3; seq++ {
		copyNode.write(u32Frame(t, u32Epoch, u32Pos(7, seq), cluster.Position{}))
	}
	copyNode.committed.Store(8)
	copyNode.catchUpFrom(source, source)

	err := copyNode.rep.CatchUp(context.Background(), u32Slot, u32DB, u32Pos(7, 3), 1)
	if err == nil {
		t.Fatal("a copy holding a tail nobody owns finished its catch-up")
	}
	if copyNode.seeds.count() != 1 || copyNode.rep.IsCurrent(u32Slot, u32DB) {
		t.Fatalf("resets %d, current %v: want one reset and not current", copyNode.seeds.count(), copyNode.rep.IsCurrent(u32Slot, u32DB))
	}
	if copyNode.applied() != u32Pos(7, 3) || copyNode.wal.LastSequence() != 3 {
		t.Fatalf("the stream discarded the tail itself: applied %s, log %d", copyNode.applied(), copyNode.wal.LastSequence())
	}
}

func TestAnIdleStreamAddsNoGoroutinePerPair(t *testing.T) {
	owner := newU32Node(t, "owner", nil)
	before := goroutineCount()
	for slot := uint16(0); slot < 2000; slot++ {
		owner.rep.Push(slot, u32DB, u32Pos(u32Epoch, 1))
	}
	if after := goroutineCount(); after > before {
		t.Fatalf("2000 pushed pairs grew the goroutines from %d to %d", before, after)
	}
}

func goroutineCount() int { return runtime.NumGoroutine() }

// heldFrames is how many shipped frames the sender still counts as unacknowledged for
// peer: the only buffer the stream keeps for a copy.
func (r *Replicator) heldFrames(slot uint16, db uint64, peer string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ps := r.pairs[pairKey{slot, db}]; ps != nil && ps.peers[peer] != nil {
		return len(ps.peers[peer].inflight)
	}
	return 0
}

func TestACopyAboveTheSourceTailResetsAndIsNeverCurrent(t *testing.T) {
	cases := []struct {
		name       string
		sourceTo   uint64
		copyTo     uint64
		laterEpoch bool
		wantReset  bool
		wantAt     cluster.Position
	}{
		{"above the tail in the same epoch", 3, 4, false, true, u32Pos(8, 4)},
		{"above the tail in a later epoch", 3, 3, true, true, u32Pos(9, 1)},
		{"an empty source and a copy with records", 0, 1, false, true, u32Pos(8, 1)},
		{"exactly at the tail", 3, 3, false, false, u32Pos(8, 3)},
		{"one behind the tail", 3, 2, false, false, u32Pos(8, 3)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := newU32Node(t, "source", nil)
			source.committed.Store(8)
			if tc.sourceTo > 0 {
				source.ownRun(8, 1, tc.sourceTo)
			}
			copyNode := newU32Node(t, "copy", nil)
			copyNode.committed.Store(8)
			for seq := uint64(1); seq <= tc.copyTo; seq++ {
				copyNode.write(u32Frame(t, 8, u32Pos(8, seq), cluster.Position{}))
			}
			if tc.laterEpoch {
				copyNode.committed.Store(9)
				copyNode.write(u32Frame(t, 9, u32Pos(9, 1), u32Pos(8, 3)))
			}
			copyNode.catchUpFrom(source, source)

			held := copyNode.applied()
			err := copyNode.rep.CatchUp(context.Background(), u32Slot, u32DB, u32Pos(held.Epoch, held.Seq+1), 1)
			current := copyNode.rep.IsCurrent(u32Slot, u32DB)
			if tc.wantReset {
				if !errors.Is(err, ErrResetAndSeed) || copyNode.seeds.count() != 1 || current {
					t.Fatalf("err %v, resets %d, current %v: want ErrResetAndSeed, one reset, never current", err, copyNode.seeds.count(), current)
				}
			} else if err != nil || copyNode.seeds.count() != 0 || !current {
				t.Fatalf("err %v, resets %d, current %v: want a normal catch-up", err, copyNode.seeds.count(), current)
			}
			if copyNode.applied() != tc.wantAt {
				t.Fatalf("applied %s, want %s: the stream must not discard or merge a tail", copyNode.applied(), tc.wantAt)
			}
		})
	}
}

func TestServeCatchUpHonoursItsContext(t *testing.T) {
	source := newU32Node(t, "source", nil)
	source.ownRun(u32Epoch, 1, 5)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := source.rep.ServeCatchUp(ctx, CatchUpRequest{Slot: u32Slot, DB: u32DB, From: u32Pos(u32Epoch, 1)})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled catch-up read answered %v, want context.Canceled", err)
	}
	if _, pinned := source.rep.PinnedFloor(); pinned {
		t.Fatal("a cancelled catch-up left its pin behind")
	}
}

func TestCloseBeforeRunIsABarrier(t *testing.T) {
	node := newU32Node(t, "node", nil)
	node.rep.Close()
	finished := make(chan struct{})
	go func() {
		node.rep.Run(context.Background())
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("Run after Close did not return")
	}
}

func TestCatchUpFromAtOrBelowTheAppliedPositionStillDetectsAnAboveTailCopy(t *testing.T) {
	for _, from := range []cluster.Position{u32Pos(8, 4), u32Pos(8, 2)} {
		t.Run(from.String(), func(t *testing.T) {
			source := newU32Node(t, "source", nil)
			source.committed.Store(8)
			source.ownRun(8, 1, 3)
			copyNode := newU32Node(t, "copy", nil)
			copyNode.committed.Store(8)
			for seq := uint64(1); seq <= 4; seq++ {
				copyNode.write(u32Frame(t, 8, u32Pos(8, seq), cluster.Position{}))
			}
			copyNode.catchUpFrom(source, source)
			err := copyNode.rep.CatchUp(context.Background(), u32Slot, u32DB, from, 1)
			if !errors.Is(err, ErrResetAndSeed) || copyNode.seeds.count() != 1 || copyNode.rep.IsCurrent(u32Slot, u32DB) {
				t.Fatalf("from %s: err %v, resets %d, current %v: want a reset and never current", from, err, copyNode.seeds.count(), copyNode.rep.IsCurrent(u32Slot, u32DB))
			}
		})
	}
}

// u32CancelApplier cancels a context once it has applied its first record.
type u32CancelApplier struct {
	Applier
	cancel context.CancelFunc
	once   sync.Once
}

func (a *u32CancelApplier) Fill(batch storage.KVBatch, rec AppliedRecord) error {
	err := a.Applier.Fill(batch, rec)
	a.once.Do(a.cancel)
	return err
}

func TestACancelledCatchUpStopsApplyingMidChunk(t *testing.T) {
	source := newU32Node(t, "source", nil)
	source.ownRun(u32Epoch, 1, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	copyNode := newU32Node(t, "copy", func(cfg *ReplicatorConfig) {
		cfg.Applier = &u32CancelApplier{Applier: cfg.Applier, cancel: cancel}
	})
	copyNode.catchUpFrom(source, source)
	err := copyNode.rep.CatchUp(ctx, u32Slot, u32DB, u32Pos(0, 1), 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled catch-up returned %v, want context.Canceled", err)
	}
	if got := copyNode.applied(); got != u32Pos(u32Epoch, 1) {
		t.Fatalf("applied %s after the cancel, want only the in-flight record 7:1", got)
	}
	if copyNode.rep.IsCurrent(u32Slot, u32DB) {
		t.Fatal("a cancelled catch-up marked the pair current")
	}
}

func TestACatchUpHintNeverMakesALegitCopyLookDivergent(t *testing.T) {
	cases := []struct {
		name  string
		build func(t *testing.T) (source, copyNode *u32Node)
		hint  cluster.Position
		want  cluster.Position
	}{
		{"a legit copy at 7:3 with a hint in the next epoch", func(t *testing.T) (*u32Node, *u32Node) {
			source := newU32Node(t, "source", nil)
			source.ownRun(7, 1, 3)
			source.committed.Store(8)
			source.ownAppend(u32Slot, u32Pos(8, 1), u32Pos(7, 3))
			copyNode := newU32Node(t, "copy", nil)
			for seq := uint64(1); seq <= 3; seq++ {
				copyNode.write(u32Frame(t, 7, u32Pos(7, seq), cluster.Position{}))
			}
			copyNode.committed.Store(8)
			return source, copyNode
		}, u32Pos(9, 1), u32Pos(8, 1)},
		{"an empty copy with a hint in the next epoch", func(t *testing.T) (*u32Node, *u32Node) {
			source := newU32Node(t, "source", nil)
			source.ownRun(7, 1, 3)
			return source, newU32Node(t, "copy", nil)
		}, u32Pos(9, 1), u32Pos(7, 3)},
		{"a copy at 8:1 with a hint in the next epoch", func(t *testing.T) (*u32Node, *u32Node) {
			source := newU32Node(t, "source", nil)
			source.committed.Store(8)
			source.ownRun(8, 1, 3)
			copyNode := newU32Node(t, "copy", nil)
			copyNode.committed.Store(8)
			copyNode.write(u32Frame(t, 8, u32Pos(8, 1), cluster.Position{}))
			return source, copyNode
		}, u32Pos(9, 1), u32Pos(8, 3)},
		{"an empty pair on both sides", func(t *testing.T) (*u32Node, *u32Node) {
			return newU32Node(t, "source", nil), newU32Node(t, "copy", nil)
		}, u32Pos(7, 1), cluster.Position{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source, copyNode := tc.build(t)
			copyNode.catchUpFrom(source, source)
			err := copyNode.rep.CatchUp(context.Background(), u32Slot, u32DB, tc.hint, 1)
			if err != nil || copyNode.seeds.count() != 0 || !copyNode.rep.IsCurrent(u32Slot, u32DB) || copyNode.applied() != tc.want {
				t.Fatalf("err %v, resets %d, current %v, applied %s: want a normal catch-up to %s", err, copyNode.seeds.count(), copyNode.rep.IsCurrent(u32Slot, u32DB), copyNode.applied(), tc.want)
			}
		})
	}
}

// u32GateLink blocks the first Write until released, and records when it finished.
type u32GateLink struct {
	Link
	entered chan struct{}
	release chan struct{}
	done    atomic.Bool
	once    sync.Once
}

func (l *u32GateLink) Write(ctx context.Context, peer string, frame WriteFrame) error {
	l.once.Do(func() { close(l.entered) })
	<-l.release
	l.done.Store(true)
	return l.Link.Write(ctx, peer, frame)
}

func TestCloseReturnsOnlyAfterTheSenderExited(t *testing.T) {
	gate := &u32GateLink{entered: make(chan struct{}), release: make(chan struct{})}
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) {
		gate.Link = cfg.Link
		cfg.Link = gate
	})
	owner.copies = []string{"copy"}
	owner.ownRun(u32Epoch, 1, 1)
	go owner.rep.Run(context.Background())
	owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, 1))
	<-gate.entered // the sender is inside a send
	go func() {
		time.Sleep(20 * time.Millisecond)
		close(gate.release)
	}()
	owner.rep.Close()
	if !gate.done.Load() {
		t.Fatal("Close returned while the sender was still inside a send")
	}
}

// TestAnAckFromANodeThatIsNotACopyCountsNothing: a departed copy's old ack, or a rogue
// ack, confirms nothing for the pair's current copy set.
func TestAnAckFromANodeThatIsNotACopyCountsNothing(t *testing.T) {
	owner := newU32Node(t, "owner", nil)
	owner.copies = []string{"copy"}
	pos := u32Pos(u32Epoch, 3)
	owner.rep.OnWriteAck(WriteAck{From: "stranger", Slot: u32Slot, DB: u32DB, Pos: pos, Outcome: AckApplied})
	if got := owner.rep.Acked(u32Slot, u32DB, pos); got != 0 {
		t.Fatalf("an ack from a node outside the copy set counts %d, want 0", got)
	}
	owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Pos: pos, Outcome: AckApplied})
	if got := owner.rep.Acked(u32Slot, u32DB, pos); got != 1 {
		t.Fatalf("an ack from the copy counts %d, want 1", got)
	}
}

// TestACopyBehindOnTheSchemaRefusesBeforeItAppends: a record written under a declaration
// version the copy has not published is refused with a retryable code and leaves no log
// entry, no document and no fence, so the owner's retry applies it cleanly later.
func TestACopyBehindOnTheSchemaRefusesBeforeItAppends(t *testing.T) {
	behind := true
	copyNode := newU32Node(t, "copy", func(cfg *ReplicatorConfig) {
		cfg.SchemaReady = func(uint64, cluster.Op) error {
			if behind {
				return &cluster.Error{Code: cluster.CodeUnavailable, Message: "this copy has not published the schema the record was written under. Retry"}
			}
			return nil
		}
	})
	_, err := copyNode.rep.OnWrite(u32Frame(t, u32Epoch, u32Pos(7, 1), cluster.Position{}))
	if u32Code(err) != cluster.CodeUnavailable {
		t.Fatalf("a record ahead of the copy's schema: %v, want a retryable UNAVAILABLE", err)
	}
	if copyNode.wal.LastSequence() != 0 || copyNode.hasDocument(u32Pos(7, 1)) || !copyNode.applied().Zero() {
		t.Fatalf("a refused record left traces: log at %d, applied %s", copyNode.wal.LastSequence(), copyNode.applied())
	}
	if flags := copyNode.pairs.Snapshot(u32Slot, u32DB).Flags; flags&flagReplayNeeded != 0 {
		t.Fatal("a refused record fenced the pair for replay; nothing was appended, so nothing needs replaying")
	}
	behind = false
	if got := copyNode.write(u32Frame(t, u32Epoch, u32Pos(7, 1), cluster.Position{})); got.Ack.Outcome != AckApplied {
		t.Fatalf("the retry once the schema is published: %+v, want applied", got.Ack)
	}
}

// TestACatchUpShipsV2RecordsAndTheCopyReadsThemBack (storage-format F4, data-safety b): the
// source's format-2 records cross a catch-up and the copy applies and reads every one back.
func TestACatchUpShipsV2RecordsAndTheCopyReadsThemBack(t *testing.T) {
	source := newU32Node(t, "source", nil)
	source.committed.Store(8)
	source.ownRun(8, 1, 5)
	copyNode := newU32Node(t, "copy", nil)
	copyNode.committed.Store(8)
	copyNode.catchUpFrom(source, source)
	if err := copyNode.rep.CatchUp(context.Background(), u32Slot, u32DB, u32Pos(8, 1), 1); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}
	for seq := uint64(1); seq <= 5; seq++ {
		if !copyNode.hasDocument(u32Pos(8, seq)) {
			t.Fatalf("the copy does not hold position 8:%d after the catch-up", seq)
		}
	}
	entries, err := copyNode.wal.ReadFrom(1)
	if err != nil || len(entries) != 5 {
		t.Fatalf("the copy's log holds %d entries, %v; want 5", len(entries), err)
	}
	for _, entry := range entries {
		record, err := storage.DecodeWALRecord(entry.Op, entry.Data)
		if err != nil || entry.Data[0] != 0x02 || record.Body[0] != cluster.OpInsert || record.Body[1] != 0x00 {
			t.Fatalf("seq %d: a copy's record %x does not decode as format 2 (%v)", entry.Seq, entry.Data[:20], err)
		}
	}
	if got := copyNode.applied(); got != u32Pos(8, 5) {
		t.Fatalf("the copy applied up to %s, want 8:5", got)
	}
}

// u32DedupeEntry reads the copy's dedupe entry of rec, and the format byte of its stored
// value (0 when there is no entry).
func (n *u32Node) u32DedupeEntry(rec PairRecord) (*machine.DedupeEntry, byte) {
	n.t.Helper()
	key := machine.DedupeKey{RequestID: rec.Record.RequestID, OpIndex: rec.Record.OpIndex}
	entry, err := NewKVDedupeReader(n.kv).Lookup(u32Slot, u32DB, u32Bucket, key)
	if err != nil {
		n.t.Fatalf("Lookup of the entry of %s: %v", rec.Pos, err)
	}
	if entry == nil {
		return nil, 0
	}
	snapshot := n.kv.NewSnapshot()
	defer snapshot.Discard()
	raw, err := snapshot.Get(DedupeEntryKey(storage.BucketRef{Slot: u32Slot, DatabaseID: u32DB, BucketID: u32Bucket}, key))
	if err != nil {
		n.t.Fatalf("reading the entry of %s: %v", rec.Pos, err)
	}
	return entry, raw[0]
}

// u32WantEntry asserts the copy holds the owner's entry for rec: format 3, the record's
// position and ordinal, and the fingerprint of the logged operation.
func (n *u32Node) u32WantEntry(rec PairRecord, ordinal uint64) {
	n.t.Helper()
	entry, format := n.u32DedupeEntry(rec)
	if entry == nil {
		n.t.Fatalf("%s applied %s with request id %x and wrote no dedupe entry: a retry after a failover to this copy would apply twice (BUG-0749)", n.id, rec.Pos, rec.Record.RequestID)
	}
	op, err := cluster.DecodeOp(rec.Record.Body)
	if err != nil {
		n.t.Fatalf("DecodeOp: %v", err)
	}
	if format != 0x03 || entry.Pos != rec.Pos || entry.Ordinal != ordinal || entry.Fingerprint != opFingerprint(op) {
		n.t.Fatalf("entry of %s = format %#02x %+v, want format 3 at %s with ordinal %d and fingerprint %x", rec.Pos, format, entry, rec.Pos, ordinal, opFingerprint(op))
	}
}

// TestACopyWritesTheDedupeEntryOfWhatItApplies: BUG-0749. A copy that applies a record,
// pushed or caught up, writes the same dedupe entry the owner wrote, in the batch that
// applies the record; a copy seeded at a cut then caught up holds an entry for every record.
func TestACopyWritesTheDedupeEntryOfWhatItApplies(t *testing.T) {
	t.Run("live stream", func(t *testing.T) {
		var commits atomic.Int64
		copyNode := newU32Node(t, "copy", func(cfg *ReplicatorConfig) {
			cfg.KV = u31CountingSource{KV: cfg.KV.(storage.KV), commits: &commits}
		})
		frame := u32Frame(t, u32Epoch, u32Pos(7, 1), cluster.Position{})
		if reply := copyNode.write(frame); reply.Ack.Outcome != AckApplied {
			t.Fatalf("ack = %+v, want applied", reply.Ack)
		}
		copyNode.u32WantEntry(frame.PairRecord, 1)
		if got := commits.Load(); got != 1 {
			t.Fatalf("the apply committed %d batches, want exactly 1: the record, its pair record and its entry share one", got)
		}
		second := u32Frame(t, u32Epoch, u32Pos(7, 2), cluster.Position{})
		copyNode.write(second)
		copyNode.u32WantEntry(second.PairRecord, 2)
		// A duplicate delivery applies nothing and moves no entry.
		copyNode.write(frame)
		copyNode.u32WantEntry(frame.PairRecord, 1)
	})

	t.Run("catch-up", func(t *testing.T) {
		copyNode := newU32Node(t, "copy", nil)
		chunk := CatchUpChunk{Records: []PairRecord{u32Record(t, u32Pos(7, 1), cluster.Position{}), u32Record(t, u32Pos(7, 2), cluster.Position{})}}
		whole, err := copyNode.rep.applyChunk(context.Background(), "owner", CatchUpRequest{Slot: u32Slot, DB: u32DB}, chunk, true)
		if err != nil || !whole {
			t.Fatalf("applyChunk = %v, %v; want the whole chunk applied", whole, err)
		}
		copyNode.u32WantEntry(chunk.Records[0], 1)
		copyNode.u32WantEntry(chunk.Records[1], 2)
	})

	t.Run("seeded at a cut then caught up", func(t *testing.T) {
		// The seed is the source's pair range as it stood at the cut, entries included; the
		// records after the cut reach the copy by catch-up and must bring their own entries.
		source := newU32Node(t, "source", nil)
		atCut := u32Frame(t, u32Epoch, u32Pos(7, 1), cluster.Position{})
		source.write(atCut)
		copyNode := newU32Node(t, "copy", nil)
		batch := copyNode.kv.NewBatch()
		snapshot := source.kv.NewSnapshot()
		prefix := storage.PairPrefix(u32Slot, u32DB)
		iter := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: append(append([]byte(nil), prefix[:9]...), prefix[9]+1)})
		for iter.Seek(nil); iter.Valid(); iter.Next() {
			value, err := iter.Value()
			if err != nil {
				t.Fatalf("reading the seed: %v", err)
			}
			if err := batch.Set(iter.Key(), value); err != nil {
				t.Fatalf("copying the seed: %v", err)
			}
		}
		iter.Close()
		snapshot.Discard()
		if err := batch.Commit(true); err != nil {
			t.Fatalf("committing the seed: %v", err)
		}
		copyNode.pairs.With(u32Slot, u32DB, func(p *cluster.Pair) {
			p.AppliedPos = u32Pos(7, 1)
			CutOrdinal(p, 1)
		})
		after := CatchUpChunk{Records: []PairRecord{u32Record(t, u32Pos(7, 2), cluster.Position{}), u32Record(t, u32Pos(7, 3), cluster.Position{})}}
		if whole, err := copyNode.rep.applyChunk(context.Background(), "source", CatchUpRequest{Slot: u32Slot, DB: u32DB}, after, true); err != nil || !whole {
			t.Fatalf("applyChunk = %v, %v; want the whole chunk applied", whole, err)
		}
		copyNode.u32WantEntry(atCut.PairRecord, 1)
		copyNode.u32WantEntry(after.Records[0], 2)
		copyNode.u32WantEntry(after.Records[1], 3)
	})
}

// TestACopyKeysTheEntryByTheRecordsOpIndex: BUG-0749. A record that is one operation of a
// client batch carries its index, and the entry must sit under that index, not under 0:
// the owner keys it that way, and a retry of operation 3 looks up (id, 3).
func TestACopyKeysTheEntryByTheRecordsOpIndex(t *testing.T) {
	copyNode := newU32Node(t, "copy", nil)
	frame := u32Frame(t, u32Epoch, u32Pos(7, 1), cluster.Position{})
	frame.Record.OpIndex = 3
	if reply := copyNode.write(frame); reply.Ack.Outcome != AckApplied {
		t.Fatalf("ack = %+v, want applied", reply.Ack)
	}
	copyNode.u32WantEntry(frame.PairRecord, 1)
	atZero := frame.PairRecord
	atZero.Record.OpIndex = 0
	if entry, _ := copyNode.u32DedupeEntry(atZero); entry != nil {
		t.Fatalf("an entry %+v sits under op index 0 for an operation at index 3", entry)
	}
}

// TestARecordWithoutARequestIdWritesNoDedupeEntry: a record that names no client operation
// gets no entry, because nothing could retry it, and a keyed one next to it still does.
// Before this, every anonymous record collided on the one all-zero key.
func TestARecordWithoutARequestIdWritesNoDedupeEntry(t *testing.T) {
	copyNode := newU32Node(t, "copy", nil)
	anonymous := u32Frame(t, u32Epoch, u32Pos(7, 1), cluster.Position{})
	anonymous.Record.RequestID = [16]byte{}
	if reply := copyNode.write(anonymous); reply.Ack.Outcome != AckApplied {
		t.Fatalf("ack = %+v, want applied", reply.Ack)
	}
	if entry, _ := copyNode.u32DedupeEntry(anonymous.PairRecord); entry != nil {
		t.Fatalf("an anonymous record wrote the entry %+v under the zero request id", entry)
	}
	keyed := u32Frame(t, u32Epoch, u32Pos(7, 2), cluster.Position{})
	copyNode.write(keyed)
	copyNode.u32WantEntry(keyed.PairRecord, 2)
}
