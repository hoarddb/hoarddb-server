package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// pairLockRequest is fx.request with a request id of its own, so two writes never share a dedupe key.
func (fx *fixture) pairLockRequest(slot uint16, id string, n uint64) WriteRequest {
	req := fx.request(slot, id)
	req.RequestID = [16]byte{9, 9}
	binary.BigEndian.PutUint64(req.RequestID[8:], n)
	return req
}

// pairLockResult is one concurrent writer's answer.
type pairLockResult struct {
	id     string
	answer WriteAnswer
	err    error
}

// writeConcurrently starts one writer per document id on one pair, each with its own request id, and
// returns a function that waits for all of them.
func (fx *fixture) writeConcurrently(slot uint16, ids []string) func() []pairLockResult {
	results := make([]pairLockResult, len(ids))
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			answer, err := fx.w.Write(context.Background(), fx.pairLockRequest(slot, id, uint64(i+1)))
			results[i] = pairLockResult{id: id, answer: answer, err: err}
		}()
	}
	return func() []pairLockResult {
		wg.Wait()
		return results
	}
}

func pairLockIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("doc%02d", i)
	}
	return ids
}

// TestSamePairWritersShareOneGroupCommit: [FAIL-FIRST on the base]. Eight concurrent upserts of eight
// documents in one pair, with a slow flush, take at most 3 flushes where the pair lock held across the
// fsync takes 8, and every document is applied and readable after its answer.
func TestSamePairWritersShareOneGroupCommit(t *testing.T) {
	fx := newFixture(t)
	fx.wal.SetFsyncHook(func() {
		fx.fsyncs.Add(1)
		time.Sleep(200 * time.Millisecond)
	})
	ids := pairLockIDs(8)
	for _, got := range fx.writeConcurrently(testSlot, ids)() {
		if got.err != nil || got.answer.Outcome != machine.OutcomeApplied {
			t.Fatalf("Write(%s) = %+v, %v, want applied", got.id, got.answer, got.err)
		}
		if !fx.hasDocument(testSlot, got.id) {
			t.Fatalf("document %s was answered applied and is not readable", got.id)
		}
	}
	if flushes := fx.fsyncs.Load(); flushes > 3 {
		t.Fatalf("8 writers on one pair took %d flushes, want at most 3 (the lock held across the fsync takes 8)", flushes)
	}
}

// TestPipelinedWritesApplyInPositionOrder: [FAIL-FIRST on the base, structural]. With the first flush
// blocked, a second writer's record reaches the log: the pair lock is not held across the fsync. After
// the release every position 1..N is present once, in the order the pair assigned, and all documents apply.
func TestPipelinedWritesApplyInPositionOrder(t *testing.T) {
	fx := newFixture(t)
	release := make(chan struct{})
	var releaseOnce sync.Once
	letGo := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(letGo)
	fx.wal.SetFsyncHook(func() { <-release })

	const writers = 16
	base := fx.wal.LastSequence()
	wait := fx.writeConcurrently(testSlot, pairLockIDs(writers))
	deadline := time.Now().Add(2 * time.Second)
	for fx.wal.LastSequence() < base+2 {
		if time.Now().After(deadline) {
			held := fx.wal.LastSequence() - base
			letGo()
			wait()
			t.Fatalf("the log holds %d new records while the first flush is blocked, want at least 2: the pair lock is held across the fsync", held)
		}
		time.Sleep(5 * time.Millisecond)
	}
	letGo()
	for _, got := range wait() {
		if got.err != nil || got.answer.Outcome != machine.OutcomeApplied {
			t.Fatalf("Write(%s) = %+v, %v, want applied", got.id, got.answer, got.err)
		}
	}
	seen := map[uint64]bool{}
	for _, entry := range fx.logged() {
		_, _, pos, err := storage.DecodePositionKey(entry.Key)
		if err != nil {
			t.Fatal(err)
		}
		if seen[pos.Seq] {
			t.Fatalf("position %d is in the log twice", pos.Seq)
		}
		seen[pos.Seq] = true
	}
	for seq := uint64(1); seq <= writers; seq++ {
		if !seen[seq] {
			t.Fatalf("position %d is missing: positions must be dense", seq)
		}
	}
	if pair := fx.pairs.Snapshot(testSlot, testDB); pair.AppliedPos.Seq != writers || pair.Ordinal != writers {
		t.Fatalf("pair = %+v, want applied through %d with ordinal %d", pair, writers, writers)
	}
}

// TestNoPendingWriteIsVisibleBeforeItsFlush: [GUARD: an apply in the first section, before the fsync].
// With the flush blocked no pending document is readable, the applied position has not moved and no
// writer has been answered; after the release all are applied.
func TestNoPendingWriteIsVisibleBeforeItsFlush(t *testing.T) {
	fx := newFixture(t)
	release := make(chan struct{})
	var releaseOnce sync.Once
	letGo := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(letGo)
	reached := make(chan struct{}, 16)
	fx.wal.SetFsyncHook(func() {
		reached <- struct{}{}
		<-release
	})

	ids := pairLockIDs(3)
	var answered atomic.Int64
	done := make(chan []pairLockResult, 1)
	go func() {
		wait := fx.writeConcurrently(testSlot, ids)
		results := wait()
		answered.Add(int64(len(results)))
		done <- results
	}()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("no flush started")
	}
	time.Sleep(200 * time.Millisecond)
	for _, id := range ids {
		if fx.hasDocument(testSlot, id) {
			t.Errorf("document %s is readable before its flush returned", id)
		}
	}
	if view := fx.pairs.ReadView(testSlot, testDB); !view.AppliedPos.Zero() {
		t.Errorf("the published view says applied through %s before any flush returned", view.AppliedPos)
	}
	if got := answered.Load(); got != 0 {
		t.Errorf("%d writers were answered before the flush returned", got)
	}
	letGo()
	for _, got := range <-done {
		if got.err != nil || got.answer.Outcome != machine.OutcomeApplied {
			t.Fatalf("Write(%s) = %+v, %v, want applied", got.id, got.answer, got.err)
		}
		if !fx.hasDocument(testSlot, got.id) {
			t.Fatalf("document %s is not applied after its answer", got.id)
		}
	}
}

// TestSameDocumentDrainsBeforeTheSecondWrite: [GUARD: no conflict rule]. Two concurrent inserts of one
// document: exactly one applies and the other is DOCUMENT_EXISTS, whichever the pair orders first.
func TestSameDocumentDrainsBeforeTheSecondWrite(t *testing.T) {
	fx := newFixture(t)
	fx.wal.SetFsyncHook(func() { time.Sleep(50 * time.Millisecond) })
	wait := fx.writeConcurrently(testSlot, []string{"same", "same"})
	applied, exists := 0, 0
	for _, got := range wait() {
		var coded *cluster.Error
		switch {
		case got.err == nil && got.answer.Outcome == machine.OutcomeApplied:
			applied++
		case errors.As(got.err, &coded) && coded.Code == cluster.CodeDocumentExists:
			exists++
		default:
			t.Fatalf("Write(%s) = %+v, %v, want applied or DOCUMENT_EXISTS", got.id, got.answer, got.err)
		}
	}
	if applied != 1 || exists != 1 {
		t.Fatalf("%d applied and %d DOCUMENT_EXISTS, want exactly one of each", applied, exists)
	}
	if got := len(fx.logged()); got != 1 {
		t.Fatalf("the log holds %d records, want 1: the refused insert must consume no position", got)
	}
}

// TestSameRequestIDWhilePendingIsDeduped: [GUARD: no document key in the conflict rule; the retry carries
// the original's document, so the document key drains it]. A retry of a request, sent while its original is
// still waiting for its flush, is answered from the original and is never appended or applied a second time.
// The dedupe key's own guard is TestSameRequestIDOnAnotherDocumentWhilePendingIsRefused.
func TestSameRequestIDWhilePendingIsDeduped(t *testing.T) {
	fx := newFixture(t)
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv,
		Applier: NewDocumentApplier(fx.kv, pairRecorder{}), Clock: fixedClock{}, Lease: fx.lease,
		Dedupe: NewKVDedupeReader(fx.kv), Schema: fx.schema, Replication: fx.repl, WCTimeout: time.Second,
		DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
	})
	release := make(chan struct{})
	var releaseOnce sync.Once
	letGo := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(letGo)
	fx.wal.SetFsyncHook(func() { <-release })

	first := make(chan pairLockResult, 1)
	req := fx.pairLockRequest(testSlot, "retry", 1)
	go func() {
		answer, err := fx.w.Write(context.Background(), req)
		first <- pairLockResult{id: "retry", answer: answer, err: err}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for fx.wal.LastSequence() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("the original never reached the log")
		}
		time.Sleep(5 * time.Millisecond)
	}
	second := make(chan pairLockResult, 1)
	go func() {
		answer, err := fx.w.Write(context.Background(), req)
		second <- pairLockResult{id: "retry", answer: answer, err: err}
	}()
	time.Sleep(100 * time.Millisecond)
	letGo()
	original, retry := <-first, <-second
	if original.err != nil || retry.err != nil {
		t.Fatalf("original %+v / %v, retry %+v / %v, want both answered", original.answer, original.err, retry.answer, retry.err)
	}
	if !retry.answer.Deduped || retry.answer.Pos != original.answer.Pos {
		t.Fatalf("retry = %+v, want deduped at the original's position %s", retry.answer, original.answer.Pos)
	}
	if got := len(fx.logged()); got != 1 {
		t.Fatalf("the log holds %d records, want 1: the retry must not be appended again", got)
	}
}

// pairLockHold blocks every flush until the returned release is called, and releases on cleanup.
func (fx *fixture) pairLockHold(t *testing.T) (release func()) {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	fx.wal.SetFsyncHook(func() { <-gate })
	return release
}

// waitForLog waits until the log holds n records past base.
func (fx *fixture) waitForLog(t *testing.T, base uint64, n uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for fx.wal.LastSequence() < base+n {
		if time.Now().After(deadline) {
			t.Fatalf("the log holds %d new records, want %d", fx.wal.LastSequence()-base, n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// writeOne starts one write and returns a channel for its outcome.
func (fx *fixture) writeOne(slot uint16, id string, n uint64) <-chan pairLockResult {
	done := make(chan pairLockResult, 1)
	go func() {
		answer, err := fx.w.Write(context.Background(), fx.pairLockRequest(slot, id, n))
		done <- pairLockResult{id: id, answer: answer, err: err}
	}()
	return done
}

func (fx *fixture) mustAnswer(t *testing.T, done <-chan pairLockResult) pairLockResult {
	t.Helper()
	select {
	case got := <-done:
		return got
	case <-time.After(15 * time.Second):
		t.Fatal("a write was not answered within 15 s")
		return pairLockResult{}
	}
}

// TestAFailedFlushAppliesTheDurablePrefix: [GUARD: fail every pending entry]. Entry 1 is durable and its
// writer is asleep; entry 2's flush fails. Entry 1 is applied by the second writer's section and answered
// applied, entry 2 is the refusal, no entry above the log's synced sequence is applied, and the pair is
// replay-needed.
func TestAFailedFlushAppliesTheDurablePrefix(t *testing.T) {
	fx := newFixture(t)
	errFlush := errors.New("the flush failed")
	holdFirst := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-holdFirst:
		default:
			close(holdFirst)
		}
	})
	override := func(seq uint64) (bool, error) {
		if seq == 1 {
			if err := fx.wal.WAL.SyncThrough(seq); err != nil {
				return true, err
			}
			<-holdFirst // the first writer sleeps after its flush, before its second section
			return true, nil
		}
		return true, errFlush
	}
	fx.wal.syncOverride.Store(&override)
	floor := uint64(1)
	fx.wal.syncedFloor.Store(&floor)

	first := fx.writeOne(testSlot, "d1", 1)
	fx.waitForLog(t, 0, 1)
	second := fx.writeOne(testSlot, "d2", 2)
	got2 := fx.mustAnswer(t, second)
	requireUnknownOutcome(t, got2.err)
	if !fx.hasDocument(testSlot, "d1") {
		t.Fatal("entry 1 was durable and must be applied by the second writer's section")
	}
	if fx.hasDocument(testSlot, "d2") {
		t.Fatal("entry 2 was above the synced sequence and must not be applied")
	}
	close(holdFirst)
	got1 := fx.mustAnswer(t, first)
	if got1.err != nil || got1.answer.Outcome != machine.OutcomeApplied {
		t.Fatalf("the sleeping writer got %+v, %v, want applied", got1.answer, got1.err)
	}
	pair := fx.pairs.Snapshot(testSlot, testDB)
	if pair.AppliedPos.Seq != 1 || pair.Flags&flagReplayNeeded == 0 {
		t.Fatalf("pair = %+v, want applied through 1 and replay-needed", pair)
	}
}

// TestARefusedAppendDoesNotFailEarlierPendingEntries: [GUARD: the second section reads flagReplayNeeded
// instead of the pipeline's own mark]. Entry 1 is pending with its flush about to succeed; the next first
// section's append is refused without poisoning the log. Entry 1 is applied and answered applied, the
// refused write answers UNAVAILABLE, and the pair is replay-needed.
func TestARefusedAppendDoesNotFailEarlierPendingEntries(t *testing.T) {
	fx := newFixture(t)
	release := fx.pairLockHold(t)
	first := fx.writeOne(testSlot, "d1", 1)
	fx.waitForLog(t, 0, 1)
	refusal := errors.New("no segment could be made")
	fx.wal.refuseUnsynced.Store(&refusal)
	refused := fx.mustAnswer(t, fx.writeOne(testSlot, "d2", 2))
	if code := codeOf(t, refused.err); code != cluster.CodeUnavailable {
		t.Fatalf("the refused write got code %v, want UNAVAILABLE", code)
	}
	release()
	got := fx.mustAnswer(t, first)
	if got.err != nil || got.answer.Outcome != machine.OutcomeApplied || got.answer.ApplyFailure != nil {
		t.Fatalf("the earlier write got %+v, %v, want applied with no apply failure", got.answer, got.err)
	}
	if !fx.hasDocument(testSlot, "d1") || fx.hasDocument(testSlot, "d2") {
		t.Fatal("entry 1 must be applied and the refused write must not be")
	}
	if pair := fx.pairs.Snapshot(testSlot, testDB); pair.Flags&flagReplayNeeded == 0 || pair.AppliedPos.Seq != 1 {
		t.Fatalf("pair = %+v, want applied through 1 and replay-needed", pair)
	}
}

// TestAnApplyFailureAnswersTheRestDurable: [GUARD: apply later entries after a failure]. The head's Badger
// commit fails; it and every later pending entry answer durable with an apply failure, none is filled, and
// the pair's replay applies them all. Their enqueue is TestEntriesAfterAnApplyFailureAreEnqueued.
func TestAnApplyFailureAnswersTheRestDurable(t *testing.T) {
	fx := newFixture(t)
	release := fx.pairLockHold(t)
	const writers = 3
	var pending []<-chan pairLockResult
	for i := range writers {
		pending = append(pending, fx.writeOne(testSlot, fmt.Sprintf("d%d", i+1), uint64(i+1)))
		fx.waitForLog(t, 0, uint64(i+1))
	}
	fx.kv.failing.Store(true)
	release()
	for i, done := range pending {
		got := fx.mustAnswer(t, done)
		if got.err != nil || got.answer.Outcome != machine.OutcomeApplied || got.answer.ApplyFailure == nil {
			t.Fatalf("write %d got %+v, %v, want durable with an apply failure", i+1, got.answer, got.err)
		}
	}
	for i := range writers {
		if fx.hasDocument(testSlot, fmt.Sprintf("d%d", i+1)) {
			t.Fatalf("document d%d was applied after the head's apply failed", i+1)
		}
	}
	fx.kv.failing.Store(false)
	replayer := cluster.NewReplayer(fx.wal.WAL, func(slot uint16, db uint64) cluster.Position {
		return fx.pairs.Snapshot(slot, db).AppliedPos
	})
	if err := replayer.Replay(0, cluster.Position{}, fx.w.Replay); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	for i := range writers {
		if !fx.hasDocument(testSlot, fmt.Sprintf("d%d", i+1)) {
			t.Fatalf("document d%d was not applied by the replay", i+1)
		}
	}
}

// TestEntriesAfterAnApplyFailureAreEnqueued: [GUARD: enqueue only on the apply branch]. After the head's
// Badger commit fails, every later pending entry still reaches the copies' queue once, in position order
// and cursor order, because each one is durable and the stream ships what is durable.
func TestEntriesAfterAnApplyFailureAreEnqueued(t *testing.T) {
	fx := newFixture(t)
	release := fx.pairLockHold(t)
	const writers = 3
	var pending []<-chan pairLockResult
	for i := range writers {
		pending = append(pending, fx.writeOne(testSlot, fmt.Sprintf("d%d", i+1), uint64(i+1)))
		fx.waitForLog(t, 0, uint64(i+1))
	}
	fx.kv.failing.Store(true)
	release()
	for _, done := range pending {
		fx.mustAnswer(t, done)
	}
	fx.repl.mu.Lock()
	queued := append([]cluster.Position(nil), fx.repl.queued...)
	cursors := append([]uint64(nil), fx.repl.cursors...)
	fx.repl.mu.Unlock()
	if len(queued) != writers || len(cursors) != writers {
		t.Fatalf("%d positions and %d cursors reached the copies' queue, want %d of each: every durable entry is shipped", len(queued), len(cursors), writers)
	}
	for i := range writers {
		if queued[i].Seq != uint64(i+1) {
			t.Fatalf("queue = %v, want positions 1..%d in order", queued, writers)
		}
		if i > 0 && cursors[i] <= cursors[i-1] {
			t.Fatalf("cursors = %v, want strictly increasing in position order", cursors)
		}
	}
}

// requireUnknownOutcome: a write whose flush failed answers with no refusal code, because its record may or may
// not be on disk. A refusal code such as UNAVAILABLE would tell the client the write was never made.
func requireUnknownOutcome(t *testing.T, err error) {
	t.Helper()
	var coded *cluster.Error
	if err == nil || errors.As(err, &coded) {
		t.Fatalf("got %v, want an unknown outcome with no refusal code", err)
	}
}

// TestAFailedFlushIsAnUnknownOutcomeAndTheRetryIsDeduped: [FAIL-FIRST, owner ruling on F3 and F12]. A flush
// that fails poisons the log, so the client is told the outcome is unknown, with no refusal code. The record
// may be on disk. After the replay, which stands in for the restart, a retry with the same request id is
// deduped at the original position and nothing is appended again. The failed flush is an override, so the
// poison itself is covered by the storage tests, not here.
func TestAFailedFlushIsAnUnknownOutcomeAndTheRetryIsDeduped(t *testing.T) {
	fx := newFixture(t)
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv,
		Applier: NewDocumentApplier(fx.kv, pairRecorder{}), Clock: fixedClock{}, Lease: fx.lease,
		Dedupe: NewKVDedupeReader(fx.kv), Schema: fx.schema, Replication: fx.repl, WCTimeout: time.Second,
		DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
	})
	override := func(seq uint64) (bool, error) {
		return true, fmt.Errorf("the flush through sequence %d failed: injected EIO", seq)
	}
	fx.wal.syncOverride.Store(&override)
	req := fx.pairLockRequest(testSlot, "d1", 1)
	_, err := fx.w.Write(context.Background(), req)
	requireUnknownOutcome(t, err)
	if fx.hasDocument(testSlot, "d1") {
		t.Fatal("a record whose flush failed must not be applied")
	}
	fx.wal.syncOverride.Store(nil)
	if err := fx.wal.WAL.SyncThrough(1); err != nil {
		t.Fatal(err)
	}
	replayer := cluster.NewReplayer(fx.wal.WAL, func(slot uint16, db uint64) cluster.Position {
		return fx.pairs.Snapshot(slot, db).AppliedPos
	})
	if err := replayer.Replay(0, cluster.Position{}, fx.w.Replay); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	fx.markReady(testSlot)
	if !fx.hasDocument(testSlot, "d1") {
		t.Fatal("the replay did not apply the record that was on disk")
	}
	retry, err := fx.w.Write(context.Background(), req)
	if err != nil || !retry.Deduped {
		t.Fatalf("retry = %+v, %v, want deduped", retry, err)
	}
	if got := len(fx.logged()); got != 1 {
		t.Fatalf("the log holds %d records, want 1: the retry must not be appended again", got)
	}
}

// TestASyncThroughOnAClosedLogIsNotARefusal: [GUARD: answer UNAVAILABLE on close]. The flush reports the
// log closed after the append: the answer carries no refusal code, nothing is applied, and the boot replay
// applies the record once; a retry with the same request id is then deduped.
func TestASyncThroughOnAClosedLogIsNotARefusal(t *testing.T) {
	fx := newFixture(t)
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv,
		Applier: NewDocumentApplier(fx.kv, pairRecorder{}), Clock: fixedClock{}, Lease: fx.lease,
		Dedupe: NewKVDedupeReader(fx.kv), Schema: fx.schema, Replication: fx.repl, WCTimeout: time.Second,
		DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
	})
	override := func(seq uint64) (bool, error) {
		return true, fmt.Errorf("fsyncing WAL through sequence %d: %w", seq, storage.ErrLogClosed)
	}
	fx.wal.syncOverride.Store(&override)
	req := fx.pairLockRequest(testSlot, "d1", 1)
	_, err := fx.w.Write(context.Background(), req)
	var coded *cluster.Error
	if err == nil || errors.As(err, &coded) {
		t.Fatalf("Write = %v, want an error with no refusal code: the record may be durable", err)
	}
	if fx.hasDocument(testSlot, "d1") {
		t.Fatal("a record whose flush did not complete must not be applied")
	}
	fx.wal.syncOverride.Store(nil)
	// Close fsyncs the active file and a reopened log counts what is on disk as synced: stand in for both.
	if err := fx.wal.WAL.SyncThrough(1); err != nil {
		t.Fatal(err)
	}
	replayer := cluster.NewReplayer(fx.wal.WAL, func(slot uint16, db uint64) cluster.Position {
		return fx.pairs.Snapshot(slot, db).AppliedPos
	})
	if err := replayer.Replay(0, cluster.Position{}, fx.w.Replay); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	fx.markReady(testSlot)
	if !fx.hasDocument(testSlot, "d1") {
		t.Fatal("the replay did not apply the record")
	}
	retry, err := fx.w.Write(context.Background(), req)
	if err != nil || !retry.Deduped {
		t.Fatalf("retry = %+v, %v, want deduped", retry, err)
	}
	if got := len(fx.logged()); got != 1 {
		t.Fatalf("the log holds %d records, want 1", got)
	}
}

// TestAnEpochMoveDrainsBeforeTheOpener: [GUARD: overlap across epochs]. Entry 1 is pending at epoch e and
// the epoch moves to e+1 before the next write: that write opens (e+1, 1) and carries PrevEnd = the last
// pending position, after entry 1 applied.
func TestAnEpochMoveDrainsBeforeTheOpener(t *testing.T) {
	fx := newFixture(t)
	release := fx.pairLockHold(t)
	first := fx.writeOne(testSlot, "d1", 1)
	fx.waitForLog(t, 0, 1)
	fx.topo.setCommitted(testEpoch + 1)
	second := fx.writeOne(testSlot, "d2", 2)
	time.Sleep(100 * time.Millisecond)
	if got := fx.wal.LastSequence(); got != 1 {
		t.Fatalf("the log holds %d records, want 1: a write that opens a new epoch must wait for the pending ones to apply", got)
	}
	release()
	if got := fx.mustAnswer(t, first); got.err != nil {
		t.Fatalf("first write: %v", got.err)
	}
	if got := fx.mustAnswer(t, second); got.err != nil || got.answer.Pos != (cluster.Position{Epoch: testEpoch + 1, Seq: 1}) {
		t.Fatalf("second write = %+v, %v, want the opener (e+1, 1)", got.answer, got.err)
	}
	if prev := fx.openerPrevEnd(t, 1); prev != (cluster.Position{Epoch: testEpoch, Seq: 1}) {
		t.Fatalf("the opener's PrevEnd = %s, want the last position of the previous epoch %d:1", prev, testEpoch)
	}
}

// openerPrevEnd is the PrevEnd the log entry at index i carries.
func (fx *fixture) openerPrevEnd(t *testing.T, i int) cluster.Position {
	t.Helper()
	entries := fx.logged()
	record, err := storage.DecodeWALRecord(entries[i].Op, entries[i].Data)
	if err != nil {
		t.Fatal(err)
	}
	op, err := cluster.DecodeOp(record.Body)
	if err != nil {
		t.Fatal(err)
	}
	return op.PrevEnd
}

// secondWriteSeesTheEpochMove makes the epoch move to e+1 at the read the plan stage of a write does
// (the owner check inside mayOrderPair), which is after the first section's read of it and before the
// assignment, and, when back is set, return to e at the Target read the assignment itself makes (the one
// held() makes inside the owner check comes earlier), before the first section's re-check. It arms after the first write is pending, and moves once.
func (fx *fixture) secondWriteSeesTheEpochMove(back bool) {
	var moved, returned atomic.Bool
	fx.topo.onTable(func() {
		if calledFrom("mayOrderPair") && moved.CompareAndSwap(false, true) {
			fx.topo.setCommitted(testEpoch + 1)
		}
	})
	if back {
		fx.topo.onTarget(func() {
			if moved.Load() && !calledFrom("mayOrderPair") && returned.CompareAndSwap(false, true) {
				fx.topo.setCommitted(testEpoch)
			}
		})
	}
}

// TestTheOverlapAndTheAssignmentReadOneEpoch: [GUARD: a second Committed read in the plan stage]. Entry 1
// is pending at e. The epoch reads e+1 once, at the read the plan stage makes after the section read it, and
// is e again at the section's re-check: only a plan stage that reads it a second time sees the move, assigns
// (e+1, 1) with a PrevEnd below the pending position, and the re-check, which sees e, does not catch it.
// The write must assign (e, 2) with no opener (review MAJ-2; fails with the mutation in a scratch copy).
func TestTheOverlapAndTheAssignmentReadOneEpoch(t *testing.T) {
	fx := newFixture(t)
	release := fx.pairLockHold(t)
	first := fx.writeOne(testSlot, "d1", 1)
	fx.waitForLog(t, 0, 1)
	fx.secondWriteSeesTheEpochMove(true)
	second := fx.writeOne(testSlot, "d2", 2)
	fx.waitForLog(t, 0, 2)
	release()
	if got := fx.mustAnswer(t, first); got.err != nil {
		t.Fatalf("first write: %v", got.err)
	}
	got := fx.mustAnswer(t, second)
	if got.err != nil {
		t.Fatalf("second write: %v", got.err)
	}
	switch got.answer.Pos {
	case cluster.Position{Epoch: testEpoch, Seq: 2}:
	case cluster.Position{Epoch: testEpoch + 1, Seq: 1}:
		if prev := fx.openerPrevEnd(t, 1); prev != (cluster.Position{Epoch: testEpoch, Seq: 1}) {
			t.Fatalf("opener (e+1, 1) carries PrevEnd %s, below the pending position %d:1: every copy would reset the pair", prev, testEpoch)
		}
		t.Fatalf("the plan stage assigned on an epoch the section did not decide on: position %s", got.answer.Pos)
	default:
		t.Fatalf("second write took position %s", got.answer.Pos)
	}
}

// TestTheEpochRecheckDecidesAgainOnAMovedEpoch: [GUARD: no epoch re-check before the append]. Entry 1 is
// pending at e and the epoch stays at e+1 once the plan stage has read it. The section, which decided on e,
// must decide again: it drains and assigns the opener (e+1, 1) with PrevEnd the last pending position,
// where without the re-check it appends (e, 2), an entry of an epoch that already ended (min-4: the
// re-check is a backstop of the one-read rule; this is the test that needs it).
func TestTheEpochRecheckDecidesAgainOnAMovedEpoch(t *testing.T) {
	fx := newFixture(t)
	release := fx.pairLockHold(t)
	first := fx.writeOne(testSlot, "d1", 1)
	fx.waitForLog(t, 0, 1)
	fx.secondWriteSeesTheEpochMove(false)
	second := fx.writeOne(testSlot, "d2", 2)
	time.Sleep(100 * time.Millisecond)
	release()
	if got := fx.mustAnswer(t, first); got.err != nil {
		t.Fatalf("first write: %v", got.err)
	}
	got := fx.mustAnswer(t, second)
	if got.err != nil || got.answer.Pos != (cluster.Position{Epoch: testEpoch + 1, Seq: 1}) {
		t.Fatalf("second write = %+v, %v, want the opener (e+1, 1)", got.answer, got.err)
	}
	if prev := fx.openerPrevEnd(t, 1); prev != (cluster.Position{Epoch: testEpoch, Seq: 1}) {
		t.Fatalf("the opener's PrevEnd = %s, want the last position of the previous epoch %d:1", prev, testEpoch)
	}
}

// TestAnAnsweredPipelinedWriteIsReadableThroughTheReadView: [GUARD: the second section does not publish].
// Right after each answer the published view says the write is applied.
func TestAnAnsweredPipelinedWriteIsReadableThroughTheReadView(t *testing.T) {
	fx := newFixture(t)
	fx.wal.SetFsyncHook(func() { time.Sleep(20 * time.Millisecond) })
	var wg sync.WaitGroup
	for i, id := range pairLockIDs(8) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			answer, err := fx.w.Write(context.Background(), fx.pairLockRequest(testSlot, id, uint64(i+1)))
			if err != nil {
				t.Errorf("Write(%s): %v", id, err)
				return
			}
			if view := fx.pairs.ReadView(testSlot, testDB); view.AppliedPos.Less(answer.Pos) {
				t.Errorf("after the answer to %s at %s the published view says applied %s", id, answer.Pos, view.AppliedPos)
			}
		}()
	}
	wg.Wait()
}

// TestAnRF2WriteKeepsTheLockedPath: [GUARD: pipeline every RF]. With the first flush blocked, a second
// same-pair write to an RF 2 database does not append until the first returns.
func TestAnRF2WriteKeepsTheLockedPath(t *testing.T) {
	fx := newFixture(t)
	slot := fx.withCluster(2, machine.ConcernOne)
	release := fx.pairLockHold(t)
	first := fx.writeOne(slot, "d1", 1)
	fx.waitForLog(t, 0, 1)
	second := fx.writeOne(slot, "d2", 2)
	time.Sleep(200 * time.Millisecond)
	if got := fx.wal.LastSequence(); got != 1 {
		t.Fatalf("the log holds %d records, want 1: an RF 2 write keeps the pair lock across its fsync", got)
	}
	if calls := fx.wal.unsyncedAppends.Load(); calls != 0 {
		t.Fatalf("%d split appends for an RF 2 database, want none", calls)
	}
	release()
	for _, done := range []<-chan pairLockResult{first, second} {
		if got := fx.mustAnswer(t, done); got.err != nil {
			t.Fatalf("write: %v", got.err)
		}
	}
}

// TestHandoffDrainsBeforeSending: [GUARD: handoff without the drain]. The handoff's section (a With that
// sets flagSending, as Handoff.apply does) does not run while an entry is pending: when it runs, everything
// appended is applied, so the barrier's last appended position is the last applied one; and a write after
// it is held, so no position is assigned once flagSending is set.
func TestHandoffDrainsBeforeSending(t *testing.T) {
	fx := newFixture(t)
	release := fx.pairLockHold(t)
	first := fx.writeOne(testSlot, "d1", 1)
	fx.waitForLog(t, 0, 1)

	type barrier struct{ appended, applied uint64 }
	sawBarrier := make(chan barrier, 1)
	go func() {
		fx.pairs.With(testSlot, testDB, func(p *cluster.Pair) {
			sawBarrier <- barrier{appended: p.AppendedSeq, applied: p.AppliedPos.Seq}
			p.Flags |= flagSending
			p.DrainTo = p.AppendedSeq
		})
	}()
	select {
	case got := <-sawBarrier:
		t.Fatalf("the handoff section ran with an entry pending: %+v", got)
	case <-time.After(150 * time.Millisecond):
	}
	release()
	if got := fx.mustAnswer(t, first); got.err != nil {
		t.Fatalf("first write: %v", got.err)
	}
	select {
	case got := <-sawBarrier:
		if got.appended != 1 || got.applied != 1 {
			t.Fatalf("the handoff section saw appended %d applied %d, want 1 and 1", got.appended, got.applied)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the handoff section never ran after the pending write applied")
	}
	held, err := fx.write(testSlot, "d2")
	if err != nil || !held.Held {
		t.Fatalf("a write after flagSending = %+v, %v, want held", held, err)
	}
	if got := fx.wal.LastSequence(); got != 1 {
		t.Fatalf("the log holds %d records, want 1: no position is assigned once flagSending is set", got)
	}
}

// admittingAdvancer is a PipelineAdvancer that refuses, as the reconciler does for a record-less pair
// whose copy set moved, any second-section step the first section did not mark admitted.
type admittingAdvancer struct {
	*LocalAdvancer
	unadmitted atomic.Int64
	// recordless is what PrepareLocked reports while the pair has applied nothing.
	recordless bool
}

// PrepareLocked registers no floor hold (the fixture has no tracker); AbandonLocked is the embedded
// LocalAdvancer's, a no-op without one.
func (a *admittingAdvancer) PrepareLocked(pair *cluster.Pair, _ uint16, _ uint64) (bool, bool, error) {
	return a.recordless && pair.AppliedPos.Zero(), false, nil
}

func (a *admittingAdvancer) AdvanceLocked(pair *cluster.Pair, slot uint16, db uint64, step AdvanceStep) error {
	if !step.Admitted {
		a.unadmitted.Add(1)
		return fmt.Errorf("the copy set moved: %w", ErrPairNotReady)
	}
	return a.LocalAdvancer.AdvanceLocked(pair, slot, db, step)
}

// TestTheSecondSectionAppliesWhatTheFirstAdmitted: [FAIL-FIRST, review MAJ-1]. A pipelined write is
// admitted by its first section; whatever the advancer would now say to an unadmitted step, the second
// section applies it and answers applied, with no apply failure and no replay fence.
func TestTheSecondSectionAppliesWhatTheFirstAdmitted(t *testing.T) {
	fx := newFixture(t)
	advancer := &admittingAdvancer{LocalAdvancer: NewLocalAdvancer(fx.pairs, fx.kv)}
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv, Advancer: advancer,
		Applier: NewDocumentApplier(fx.kv, pairRecorder{}), Clock: fixedClock{}, Lease: fx.lease,
		Dedupe: fx.dedupe, Schema: fx.schema, Replication: fx.repl, WCTimeout: time.Second,
		DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
	})
	got := fx.mustAnswer(t, fx.writeOne(testSlot, "d1", 1))
	if got.err != nil || got.answer.Outcome != machine.OutcomeApplied || got.answer.ApplyFailure != nil {
		t.Fatalf("write = %+v, %v (unadmitted steps %d), want applied with no apply failure", got.answer, got.err, advancer.unadmitted.Load())
	}
	if !fx.hasDocument(testSlot, "d1") {
		t.Fatal("the admitted write was answered and is not applied")
	}
	if pair := fx.pairs.Snapshot(testSlot, testDB); pair.Flags&flagReplayNeeded != 0 {
		t.Fatalf("pair = %+v, want no replay fence", pair)
	}
}

// recoveryScanLog counts the tail scans that open a cursor and holds each until released.
type recoveryScanLog struct {
	cluster.LogReader
	opens   atomic.Int64
	release chan struct{}
}

func (l *recoveryScanLog) OpenCursor(from uint64) (storage.WALCursor, error) {
	l.opens.Add(1)
	<-l.release
	return l.LogReader.OpenCursor(from)
}

// TestARowNineRecoversThePairOnce: [FAIL-FIRST, review MAJ-3]. A row 9 answers up to pipelineCap writers
// of one pair with an apply failure, and every one of them asks the node to recover the pair. The node
// scans the log tail once for all of them, and each caller still gets the scan's result; a later failure,
// after that scan ended, scans again.
func TestARowNineRecoversThePairOnce(t *testing.T) {
	fx := newFixture(t)
	fx.mustAnswer(t, fx.writeOne(testSlot, "d1", 1)) // the log has a tail to scan
	log := &recoveryScanLog{LogReader: fx.wal, release: make(chan struct{})}
	node := &Node{
		kv:    fx.kv.BadgerKV,
		pairs: fx.pairs,
		own:   NewPairOwnership(fx.pairs),
		tail:  NewTailScanner(log, fx.w, func(slot uint16, db uint64) cluster.Position { return fx.pairs.Snapshot(slot, db).AppliedPos }),
	}
	const callers = pipelineCap
	errs := make(chan error, callers)
	for range callers {
		go func() { errs <- node.recoverPair(context.Background(), testSlot, testDB) }()
	}
	deadline := time.Now().Add(10 * time.Second)
	for log.opens.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no scan started")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond) // let the other callers reach recoverPair while the scan is held
	close(log.release)
	for range callers {
		if err := <-errs; err != nil {
			t.Fatalf("recoverPair = %v", err)
		}
	}
	if scans := log.opens.Load(); scans != 1 {
		t.Fatalf("%d concurrent recoveries of one pair opened %d scans, want 1", callers, scans)
	}
	if err := node.recoverPair(context.Background(), testSlot, testDB); err != nil {
		t.Fatal(err)
	}
	if scans := log.opens.Load(); scans != 2 {
		t.Fatalf("a recovery after the first ended opened %d scans in all, want 2", scans)
	}
}

// TestAClosedLogIsNotRecoveredByTheNodeSender: [FAIL-FIRST, review min-2]. A write whose flush found the
// log closed fences the pair, as every failed flush does; the node is closing, so the sender does not scan
// the closed log's tail for it, and the client's error is the closed log's, not a joined replay failure.
func TestAClosedLogIsNotRecoveredByTheNodeSender(t *testing.T) {
	fx := newFixture(t)
	log := &recoveryScanLog{LogReader: fx.wal, release: make(chan struct{})}
	close(log.release)
	node := &Node{
		kv: fx.kv.BadgerKV, pairs: fx.pairs, writer: fx.w, own: NewPairOwnership(fx.pairs),
		tail: NewTailScanner(log, fx.w, func(slot uint16, db uint64) cluster.Position { return fx.pairs.Snapshot(slot, db).AppliedPos }),
	}
	override := func(seq uint64) (bool, error) {
		return true, fmt.Errorf("fsyncing WAL through sequence %d: %w", seq, storage.ErrLogClosed)
	}
	fx.wal.syncOverride.Store(&override)
	_, err := nodeSender{node: node}.Write(context.Background(), fx.pairLockRequest(testSlot, "d1", 1))
	if !errors.Is(err, storage.ErrLogClosed) {
		t.Fatalf("Write = %v, want the closed log's error", err)
	}
	if scans := log.opens.Load(); scans != 0 {
		t.Fatalf("the sender opened %d scans of a closed log, want 0", scans)
	}
	if flags := fx.pairs.Snapshot(testSlot, testDB).Flags; flags&flagReplayNeeded == 0 {
		t.Fatalf("pair flags = %b, want the pair fenced for the next boot's replay", flags)
	}
}

// TestSameRequestIDOnAnotherDocumentWhilePendingIsRefused: [FAIL-FIRST for the dedupe key, review min-5].
// A write that reuses a pending request's id for a different document shares no document key with it, so
// only the dedupe key makes it wait for the original's dedupe entry; it is then refused as a reused request
// id and appends nothing. Without the key it is appended as a new position.
func TestSameRequestIDOnAnotherDocumentWhilePendingIsRefused(t *testing.T) {
	fx := newFixture(t)
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv,
		Applier: NewDocumentApplier(fx.kv, pairRecorder{}), Clock: fixedClock{}, Lease: fx.lease,
		Dedupe: NewKVDedupeReader(fx.kv), Schema: fx.schema, Replication: fx.repl, WCTimeout: time.Second,
		DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
	})
	release := fx.pairLockHold(t)
	first := fx.writeOne(testSlot, "original", 1)
	fx.waitForLog(t, 0, 1)
	second := fx.writeOne(testSlot, "other", 1) // the same request id, another document
	time.Sleep(100 * time.Millisecond)
	release()
	if got := fx.mustAnswer(t, first); got.err != nil || got.answer.Outcome != machine.OutcomeApplied {
		t.Fatalf("original = %+v, %v, want applied", got.answer, got.err)
	}
	got := fx.mustAnswer(t, second)
	if !errors.Is(got.err, errRequestIDReused) {
		t.Fatalf("the reused request id = %+v, %v, want errRequestIDReused", got.answer, got.err)
	}
	if fx.hasDocument(testSlot, "other") || len(fx.logged()) != 1 {
		t.Fatalf("the refused write was appended or applied: %d log records", len(fx.logged()))
	}
}

// TestTheWriteAfterAFailedFlushIsAppliedNotAnApplyFailure: [FAIL-FIRST, flash F1]. The failure mark of a
// pipeline ends when its list empties; a flush failure that popped the last entry then marked it failed
// again, so the first write after the pair's replay was enqueued, never filled, and answered with an apply
// failure. After the replay the next write is applied.
func TestTheWriteAfterAFailedFlushIsAppliedNotAnApplyFailure(t *testing.T) {
	fx := newFixture(t)
	errFlush := errors.New("the flush failed")
	override := func(uint64) (bool, error) { return true, errFlush }
	fx.wal.syncOverride.Store(&override)
	got := fx.mustAnswer(t, fx.writeOne(testSlot, "d1", 1))
	requireUnknownOutcome(t, got.err)
	fx.wal.syncOverride.Store(nil)
	// The pair's replay: it applies what is durable and lifts the fence.
	replayer := cluster.NewReplayer(fx.wal.WAL, func(slot uint16, db uint64) cluster.Position {
		return fx.pairs.Snapshot(slot, db).AppliedPos
	})
	if err := fx.wal.WAL.SyncThrough(1); err != nil {
		t.Fatal(err)
	}
	if err := replayer.Replay(0, cluster.Position{}, fx.w.Replay); err != nil {
		t.Fatal(err)
	}
	fx.markReady(testSlot)
	next := fx.mustAnswer(t, fx.writeOne(testSlot, "d2", 2))
	if next.err != nil || next.answer.Outcome != machine.OutcomeApplied || next.answer.ApplyFailure != nil {
		t.Fatalf("the write after the replay = %+v, %v, want applied with no apply failure", next.answer, next.err)
	}
	if !fx.hasDocument(testSlot, "d2") {
		t.Fatal("the write after the replay was answered and is not applied")
	}
}

// TestTwoFailedFlushesLeaveNoStaleFailureMark: [FAIL-FIRST for the empty-list case of flash F1]. Two pending
// writers' flushes both fail; the second to reach its section finds the list already emptied by the first's.
// It must not mark the empty list failed, or the first write after the replay is answered with an apply failure.
func TestTwoFailedFlushesLeaveNoStaleFailureMark(t *testing.T) {
	fx := newFixture(t)
	gates := map[uint64]chan struct{}{1: make(chan struct{}), 2: make(chan struct{})}
	var once1, once2 sync.Once
	letGo := func() { once1.Do(func() { close(gates[1]) }); once2.Do(func() { close(gates[2]) }) }
	t.Cleanup(letGo)
	errFlush := errors.New("the flush failed")
	override := func(seq uint64) (bool, error) {
		<-gates[seq]
		return true, errFlush
	}
	fx.wal.syncOverride.Store(&override)
	first := fx.writeOne(testSlot, "d1", 1)
	fx.waitForLog(t, 0, 1)
	second := fx.writeOne(testSlot, "d2", 2)
	fx.waitForLog(t, 0, 2)
	// The later entry's section runs first and pops both; the earlier one then finds the list empty.
	once2.Do(func() { close(gates[2]) })
	requireUnknownOutcome(t, fx.mustAnswer(t, second).err)
	once1.Do(func() { close(gates[1]) })
	requireUnknownOutcome(t, fx.mustAnswer(t, first).err)
	fx.wal.syncOverride.Store(nil)
	if err := fx.wal.WAL.SyncThrough(2); err != nil {
		t.Fatal(err)
	}
	replayer := cluster.NewReplayer(fx.wal.WAL, func(slot uint16, db uint64) cluster.Position {
		return fx.pairs.Snapshot(slot, db).AppliedPos
	})
	if err := replayer.Replay(0, cluster.Position{}, fx.w.Replay); err != nil {
		t.Fatal(err)
	}
	fx.markReady(testSlot)
	next := fx.mustAnswer(t, fx.writeOne(testSlot, "d3", 3))
	if next.err != nil || next.answer.Outcome != machine.OutcomeApplied || next.answer.ApplyFailure != nil {
		t.Fatalf("the write after the replay = %+v, %v, want applied with no apply failure", next.answer, next.err)
	}
}

// TestBumpCycleWaitsForPendingWrites (T5): [GUARD: a bump publishes while a write is pending]. The schema
// bump's publication cycles the pair's lock with WithHeld, which drains first: with a write appended and its
// flush blocked, Apply does not return, and when it does the write is applied, so a writer either commits
// before the publication or sees the new version at check 4.
func TestBumpCycleWaitsForPendingWrites(t *testing.T) {
	fx := newFixture(t)
	release := fx.pairLockHold(t)
	first := fx.writeOne(testSlot, "d1", 1)
	fx.waitForLog(t, 0, 1)
	var bumps cluster.BumpApplier
	bumps.Stage(testDB, 1)
	done := make(chan struct{})
	go func() {
		bumps.Apply(testDB, 2, fx.pairs)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("the bump published while a write of the pair was pending")
	case <-time.After(300 * time.Millisecond):
	}
	if published, _ := bumps.Published(testDB); published != 1 {
		t.Fatalf("published version = %d during the pending write, want 1", published)
	}
	release()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the bump never published after the write drained")
	}
	if got := fx.mustAnswer(t, first); got.err != nil || got.answer.Outcome != machine.OutcomeApplied {
		t.Fatalf("write = %+v, %v, want applied", got.answer, got.err)
	}
	if !fx.hasDocument(testSlot, "d1") {
		t.Fatal("the bump returned before the pending write was applied")
	}
}

// TestARotatedEntryIsNotAppliedBeyondTheSyncedSequence (T16b): [GUARD: apply on the failed flush's own
// cursor]. Three entries are appended; entry 1 is durable and the log's synced sequence stays at 1 (what
// it reports after a rotation flushed an older segment and the active file's flush then failed), and the
// flush of the highest fails. Only entry 1 is applied; 2 and 3 are not, and both writers are told the flush
// failed. The log's own rotation is not exercised: the synced sequence is the test's floor.
func TestARotatedEntryIsNotAppliedBeyondTheSyncedSequence(t *testing.T) {
	fx := newFixture(t)
	errFlush := errors.New("the flush failed")
	gate := make(chan struct{})
	var gateOnce sync.Once
	letGo := func() { gateOnce.Do(func() { close(gate) }) }
	t.Cleanup(letGo)
	override := func(seq uint64) (bool, error) {
		if seq == 1 {
			return true, fx.wal.WAL.SyncThrough(1)
		}
		<-gate
		return true, errFlush
	}
	fx.wal.syncOverride.Store(&override)
	floor := uint64(1)
	fx.wal.syncedFloor.Store(&floor)
	if got := fx.mustAnswer(t, fx.writeOne(testSlot, "d1", 1)); got.err != nil {
		t.Fatalf("entry 1: %v", got.err)
	}
	second := fx.writeOne(testSlot, "d2", 2)
	third := fx.writeOne(testSlot, "d3", 3)
	fx.waitForLog(t, 0, 3)
	letGo()
	for _, done := range []<-chan pairLockResult{second, third} {
		requireUnknownOutcome(t, fx.mustAnswer(t, done).err)
	}
	if !fx.hasDocument(testSlot, "d1") || fx.hasDocument(testSlot, "d2") || fx.hasDocument(testSlot, "d3") {
		t.Fatal("only the entry at or below the synced sequence may be applied")
	}
	if pair := fx.pairs.Snapshot(testSlot, testDB); pair.AppliedPos.Seq != 1 {
		t.Fatalf("applied through %d, want 1", pair.AppliedPos.Seq)
	}
}

// TestASnapshotUnderABlockedFlushAnswersTheAppliedPosition: [GUARD: Snapshot drains]. Snapshot is what
// POSITION_QUERY, a copy's catch-up and the handoff read. With a write appended and its flush blocked it
// does not report the appended position as the pair's: it waits for the flush, and then answers the
// applied position, which has caught up with the append.
func TestASnapshotUnderABlockedFlushAnswersTheAppliedPosition(t *testing.T) {
	fx := newFixture(t)
	release := fx.pairLockHold(t)
	first := fx.writeOne(testSlot, "d1", 1)
	fx.waitForLog(t, 0, 1)
	if view := fx.pairs.ReadView(testSlot, testDB); !view.AppliedPos.Zero() {
		t.Fatalf("the published view says applied %s before the flush", view.AppliedPos)
	}
	answered := make(chan cluster.Pair, 1)
	go func() { answered <- fx.pairs.Snapshot(testSlot, testDB) }()
	select {
	case pair := <-answered:
		t.Fatalf("Snapshot answered %+v while a pending write's flush was blocked: it reports a state the write is about to change", pair)
	case <-time.After(300 * time.Millisecond):
	}
	release()
	select {
	case pair := <-answered:
		if pair.AppliedPos.Seq != 1 || pair.AppendedSeq != 1 {
			t.Fatalf("Snapshot = %+v, want applied and appended through 1", pair)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Snapshot did not answer after the flush returned")
	}
	fx.mustAnswer(t, first)
}

// TestARowNineRecoveryCoversTheWritesOfALaterGroupCommit: [FAIL-FIRST, re-review B1]. Entry 1 fails its apply; entry 2 is in a later group commit.
// Entry 2 is answered durable with an apply failure and must be applied by the pair's recovery.
func TestARowNineRecoveryCoversTheWritesOfALaterGroupCommit(t *testing.T) {
	fx := newFixture(t)
	gates := []chan struct{}{make(chan struct{}), make(chan struct{})}
	var flushes atomic.Int64
	var once [2]sync.Once
	t.Cleanup(func() {
		for i := range gates {
			once[i].Do(func() { close(gates[i]) })
		}
	})
	fx.wal.SetFsyncHook(func() {
		n := flushes.Add(1)
		if n <= 2 {
			<-gates[n-1]
		}
	})
	log := &recoveryScanLog{LogReader: fx.wal, release: make(chan struct{})}
	node := &Node{
		kv: fx.kv.BadgerKV, pairs: fx.pairs, writer: fx.w, own: NewPairOwnership(fx.pairs),
		tail: NewTailScanner(log, fx.w, func(slot uint16, db uint64) cluster.Position { return fx.pairs.Snapshot(slot, db).AppliedPos }),
	}
	send := func(id string, n uint64) <-chan pairLockResult {
		done := make(chan pairLockResult, 1)
		go func() {
			answer, err := nodeSender{node: node}.Write(context.Background(), fx.pairLockRequest(testSlot, id, n))
			done <- pairLockResult{id: id, answer: answer, err: err}
		}()
		return done
	}
	waitFor := func(what string, cond func() bool) {
		deadline := time.Now().Add(10 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(time.Millisecond)
		}
	}
	a := send("d1", 1)
	fx.waitForLog(t, 0, 1)
	waitFor("flush 1 to start", func() bool { return flushes.Load() >= 1 })
	b := send("d2", 2)
	fx.waitForLog(t, 0, 2)
	fx.kv.failing.Store(true)
	once[0].Do(func() { close(gates[0]) })
	// A's apply fails and fences the pair; its recovery starts.
	waitFor("A's apply failure", func() bool { return fx.pairs.ReadView(testSlot, testDB).Flags&flagReplayNeeded != 0 })
	time.Sleep(100 * time.Millisecond) // A's recovery reads its tail
	fx.kv.failing.Store(false)
	waitFor("flush 2 to start", func() bool { return flushes.Load() >= 2 })
	once[1].Do(func() { close(gates[1]) })
	time.Sleep(300 * time.Millisecond) // B answers and asks for its recovery
	close(log.release)
	gotA, gotB := fx.mustAnswer(t, a), fx.mustAnswer(t, b)
	t.Logf("A: applyFailure=%v err=%v", gotA.answer.ApplyFailure, gotA.err)
	t.Logf("B: applyFailure=%v err=%v", gotB.answer.ApplyFailure, gotB.err)
	t.Logf("scans opened: %d", log.opens.Load())
	pair := fx.pairs.Snapshot(testSlot, testDB)
	t.Logf("pair after recovery: applied %v appended %d flags %b", pair.AppliedPos, pair.AppendedSeq, pair.Flags)
	if !fx.hasDocument(testSlot, "d1") {
		t.Errorf("d1 not applied by the recovery")
	}
	if !fx.hasDocument(testSlot, "d2") {
		t.Errorf("d2 was answered durable (%v) and the pair's fence is %v, but d2 is not applied", gotB.answer.ApplyFailure, pair.Flags&flagReplayNeeded != 0)
	}
	// A third write, then a full replay: is d2 recoverable at all?
	got3 := fx.mustAnswer(t, send("d3", 3))
	t.Logf("d3: outcome %v err %v", got3.answer.Outcome, got3.err)
	replayer := cluster.NewReplayer(fx.wal.WAL, func(slot uint16, db uint64) cluster.Position {
		return fx.pairs.Snapshot(slot, db).AppliedPos
	})
	if err := replayer.Replay(0, cluster.Position{}, fx.w.Replay); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if !fx.hasDocument(testSlot, "d2") {
		t.Errorf("after d3 and a full replay d2 is still not applied: an acknowledged-durable write is lost")
	}
}

// TestARecordlessPairNeverHasTwoPendingEntries: [FAIL-FIRST, re-review M1]. The write that creates a pair's
// record cannot be replayed if its apply fails (a replay stages nothing for a pair with no record), so
// nothing may be appended behind it until it has applied: a second write to another document waits for
// the first to drain.
func TestARecordlessPairNeverHasTwoPendingEntries(t *testing.T) {
	fx := newFixture(t)
	advancer := &admittingAdvancer{LocalAdvancer: NewLocalAdvancer(fx.pairs, fx.kv), recordless: true}
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv, Advancer: advancer,
		Applier: NewDocumentApplier(fx.kv, pairRecorder{}), Clock: fixedClock{}, Lease: fx.lease,
		Dedupe: fx.dedupe, Schema: fx.schema, Replication: fx.repl, WCTimeout: time.Second,
		DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
	})
	release := fx.pairLockHold(t)
	first := fx.writeOne(testSlot, "d1", 1)
	fx.waitForLog(t, 0, 1)
	second := fx.writeOne(testSlot, "d2", 2)
	time.Sleep(300 * time.Millisecond)
	if got := fx.wal.LastSequence(); got != 1 {
		t.Fatalf("the log holds %d records while the record-creating write is pending, want 1", got)
	}
	release()
	for _, done := range []<-chan pairLockResult{first, second} {
		if got := fx.mustAnswer(t, done); got.err != nil || got.answer.Outcome != machine.OutcomeApplied || got.answer.ApplyFailure != nil {
			t.Fatalf("%s = %+v, %v, want applied", got.id, got.answer, got.err)
		}
	}
}

// TestAPanickingRecoveryDoesNotWedgeThePair: [FAIL-FIRST, flash M1]. A recovery that panics is re-raised to its
// own caller, but its flight must end: a later recovery of the pair runs instead of waiting for ever, and a
// caller already waiting on the flight is released with an error.
func TestAPanickingRecoveryDoesNotWedgeThePair(t *testing.T) {
	var flights pairFlights
	key := pairKey{testSlot, testDB}
	started := make(chan struct{})
	proceed := make(chan struct{})
	panicked := make(chan any, 1)
	go func() {
		defer func() { panicked <- recover() }()
		_ = flights.do(context.Background(), key, func() error {
			close(started)
			<-proceed
			panic("the replay panicked")
		})
	}()
	<-started
	joined := make(chan error, 1)
	go func() { joined <- flights.do(context.Background(), key, func() error { return nil }) }()
	time.Sleep(100 * time.Millisecond)
	close(proceed)
	if got := <-panicked; got == nil {
		t.Fatal("the panic was swallowed; it must reach the caller whose recovery panicked")
	}
	select {
	case err := <-joined:
		if err == nil {
			t.Fatal("a caller that joined a recovery that panicked got success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a caller waiting on a recovery that panicked was never released")
	}
	next := make(chan error, 1)
	go func() { next <- flights.do(context.Background(), key, func() error { return nil }) }()
	select {
	case err := <-next:
		if err != nil {
			t.Fatalf("the next recovery of the pair = %v, want it to run", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the next recovery of the pair waits for ever on the dead flight")
	}
}

// TestAJoinerLeavesWhenItsContextEnds: [FAIL-FIRST, flash M2]. A caller waiting on another writer's recovery
// answers when its own context ends, with an error, and does not disturb the flight, which still finishes
// for its leader.
func TestAJoinerLeavesWhenItsContextEnds(t *testing.T) {
	var flights pairFlights
	key := pairKey{testSlot, testDB}
	started := make(chan struct{})
	proceed := make(chan struct{})
	leader := make(chan error, 1)
	go func() {
		leader <- flights.do(context.Background(), key, func() error {
			close(started)
			<-proceed
			return nil
		})
	}()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	joined := make(chan error, 1)
	go func() { joined <- flights.do(ctx, key, func() error { return nil }) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-joined:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the joiner = %v, want its context's error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the joiner kept waiting after its context ended")
	}
	close(proceed)
	if err := <-leader; err != nil {
		t.Fatalf("the flight's leader = %v, want its own result", err)
	}
}

// TestAFailedRecoveryAnswersEveryJoinerAndTheNextRuns: [GUARD: a joiner that waits on a failed flight]. A
// recovery that returns an error, not a panic, answers every caller that joined it with that error, and the
// next recovery of the pair starts a fresh flight and runs. Nothing about the failure is kept for later.
func TestAFailedRecoveryAnswersEveryJoinerAndTheNextRuns(t *testing.T) {
	var flights pairFlights
	key := pairKey{testSlot, testDB}
	started := make(chan struct{})
	proceed := make(chan struct{})
	scanErr := errors.New("the scan failed")
	leader := make(chan error, 1)
	go func() {
		leader <- flights.do(context.Background(), key, func() error {
			close(started)
			<-proceed
			return scanErr
		})
	}()
	<-started
	joined := make(chan error, 1)
	go func() { joined <- flights.do(context.Background(), key, func() error { return nil }) }()
	time.Sleep(100 * time.Millisecond)
	close(proceed)
	if err := <-leader; !errors.Is(err, scanErr) {
		t.Fatalf("the leader = %v, want its scan's error", err)
	}
	select {
	case err := <-joined:
		if !errors.Is(err, scanErr) {
			t.Fatalf("the joiner = %v, want the leader's scan error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a caller waiting on a failed recovery was never released")
	}
	if err := flights.do(context.Background(), key, func() error { return nil }); err != nil {
		t.Fatalf("the next recovery of the pair = %v, want it to run and succeed", err)
	}
}

// failOnceAdvancer fails the first advance it is given, then behaves like LocalAdvancer.
type failOnceAdvancer struct {
	*LocalAdvancer
	fail atomic.Bool
}

func (a *failOnceAdvancer) AdvanceLocked(pair *cluster.Pair, slot uint16, db uint64, step AdvanceStep) error {
	if a.fail.CompareAndSwap(true, false) {
		return errors.New("injected apply failure")
	}
	return a.LocalAdvancer.AdvanceLocked(pair, slot, db, step)
}

// TestAFailureDuringARecoveryIsCoveredByItsScan: [GUARD: no drain at the top of the flight, flash B1]. A refused
// append starts a recovery while d2 is pending; d2's flush then lands and its apply fails (row 9) while the
// recovery is still running, and its writer joins the flight. The recovery drains first, so d2 pops before the
// scan's end is read, the scan covers it, and d2 is applied by the same recovery.
func TestAFailureDuringARecoveryIsCoveredByItsScan(t *testing.T) {
	fx := newFixture(t)
	advancer := &failOnceAdvancer{LocalAdvancer: NewLocalAdvancer(fx.pairs, fx.kv)}
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv, Advancer: advancer,
		Applier: NewDocumentApplier(fx.kv, pairRecorder{}), Clock: fixedClock{}, Lease: fx.lease,
		Dedupe: fx.dedupe, Schema: fx.schema, Replication: fx.repl, WCTimeout: time.Second,
		DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
	})
	fx.mustAnswer(t, fx.writeOne(testSlot, "d1", 1)) // applied at seq 1

	gate := make(chan struct{})
	var once sync.Once
	letGo := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(letGo)
	override := func(seq uint64) (bool, error) {
		if seq == 2 {
			<-gate
		}
		return false, nil
	}
	fx.wal.syncOverride.Store(&override)
	advancer.fail.Store(true)

	second := fx.writeOne(testSlot, "d2", 2) // appended; its flush is held
	fx.waitForLog(t, 0, 2)

	scanLog := &recoveryScanLog{LogReader: fx.wal, release: make(chan struct{})}
	node := &Node{
		kv: fx.kv.BadgerKV, pairs: fx.pairs, writer: fx.w,
		own:  NewPairOwnership(fx.pairs),
		tail: NewTailScanner(scanLog, fx.w, func(slot uint16, db uint64) cluster.Position { return fx.pairs.Snapshot(slot, db).AppliedPos }),
	}
	leader := make(chan error, 1)
	go func() { leader <- node.recoverPair(context.Background(), testSlot, testDB) }()
	// The flight is held in its drain by d2's pending flush: no scan has started, and no scan end is fixed.
	time.Sleep(100 * time.Millisecond)
	if scanLog.opens.Load() != 0 {
		t.Fatal("the recovery scanned while a write of the pair was pending: it did not drain first")
	}
	// d2 flushes (durable) and its apply fails: fence set, answered durable+ApplyFailure.
	letGo()
	got := fx.mustAnswer(t, second)
	if got.answer.ApplyFailure == nil {
		t.Fatalf("d2 = %+v, %v, want an apply failure", got.answer, got.err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for scanLog.opens.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no scan started after the drain")
		}
		time.Sleep(time.Millisecond)
	}

	// The joiner a nodeSender would run, while the leader's scan is still held.
	joined := make(chan error, 1)
	go func() { joined <- node.recoverPair(context.Background(), testSlot, testDB) }()
	time.Sleep(100 * time.Millisecond)
	close(scanLog.release)
	if err := <-leader; err != nil {
		t.Fatalf("leader = %v", err)
	}
	if err := <-joined; err != nil {
		t.Fatalf("joiner = %v", err)
	}

	pair := fx.pairs.Snapshot(testSlot, testDB)
	if pair.Flags&flagReplayNeeded != 0 || pair.AppliedPos.Seq != 2 || !fx.hasDocument(testSlot, "d2") {
		t.Fatalf("after the recovery: flags %b applied %s d2 applied %v, want d2 applied by the recovery and the fence lifted", pair.Flags, pair.AppliedPos, fx.hasDocument(testSlot, "d2"))
	}
}
