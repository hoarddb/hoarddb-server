package wire

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// BUG-0760 follow-up: what a pair that cannot finish its boot replay does to the node and to
// the other pairs.

// stuckReader is a DeclarationReader that cannot answer for one database while down is set,
// as a declaration slot whose owner is gone cannot.
type stuckReader struct {
	inner DeclarationReader
	db    uint64
	down  *atomic.Bool
}

var errOwnerGone = errors.New("the owner of the declaration slot is down")

func (r stuckReader) ReadDeclaration(ctx context.Context, owner cluster.Member, read DeclarationRead) (map[uint16][]byte, bool, error) {
	if read.DBID == r.db && r.down.Load() {
		return nil, false, errOwnerGone
	}
	return r.inner.ReadDeclaration(ctx, owner, read)
}

func (r stuckReader) ReadBucketName(ctx context.Context, owner cluster.Member, read DeclarationRead) (string, bool, error) {
	if read.DBID == r.db && r.down.Load() {
		return "", false, errOwnerGone
	}
	return r.inner.ReadBucketName(ctx, owner, read)
}

// bug0760TwoDatabases builds databases app and other, each with Box b1 and a push the pair
// records have not counted, and returns the closed node's directory and the database ids.
func bug0760TwoDatabases(t *testing.T) (dir string, ids map[string]uint64) {
	t.Helper()
	dir = t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	ids = map[string]uint64{}
	for _, db := range []string{u30iDB, "other"} {
		ids[db] = bug0760Box(t, node, db)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	before := map[string]savedPair{}
	for db, id := range ids {
		before[db] = savePair(t, dir, bug0760Slot(t, db), id)
	}
	node = s0OpenClusterNode(t, dir, nil)
	bug0760Push(t, node, u30iDB, "a1")
	bug0760Push(t, node, "other", "b1", "b2")
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	for db := range ids {
		rewindPair(t, dir, before[db])
	}
	return dir, ids
}

func bug0760Flags(n *Node, db string, id uint64, t *testing.T) uint8 {
	t.Helper()
	return n.pairs.Snapshot(bug0760Slot(t, db), id).Flags
}

// TestAPairThatCannotFinishItsReplayDoesNotHoldAnotherPairFenced (BUG-0760): the declaration
// slot of database other has no live owner, so its pair stops at every pass; the pair of app,
// which could replay, is released at the first pass and serves, and the stuck pair replays
// when its owner is back. [BREAK: release only when a whole pass succeeds]
func TestAPairThatCannotFinishItsReplayDoesNotHoldAnotherPairFenced(t *testing.T) {
	dir, ids := bug0760TwoDatabases(t)
	var down atomic.Bool
	down.Store(true)
	reopened, _, release := openHeld(t, dir, func(n *Node) {
		n.fetcher.cfg.Reader = stuckReader{inner: n.fetcher.cfg.Reader, db: ids["other"], down: &down}
	})
	release()
	if got := bug0760Queue(t, reopened, u30iDB); len(got) != 1 || got[0] != "a1" {
		t.Fatalf("Queue of app = %v, want [a1] while the pair of other is stuck", got)
	}
	if flags := bug0760Flags(reopened, "other", ids["other"], t); flags&flagReplayNeeded == 0 {
		t.Fatalf("the stuck pair's flags are %#x, want it fenced while its declaration cannot be read", flags)
	}
	down.Store(false)
	if got := bug0760Queue(t, reopened, "other"); len(got) != 2 || got[0] != "b1" || got[1] != "b2" {
		t.Fatalf("Queue of other after its owner is back = %v, want [b1 b2]", got)
	}
}

// transientGone answers "not found" while gone is set, then reads normally: a declaration slot
// owner whose Badger does not hold the bucket's _buckets documents yet (seeding, catching up,
// its own declaration pair stopped in its boot replay, a create lost to a power cut).
type transientGone struct {
	inner DeclarationReader
	gone  *atomic.Bool
}

func (r transientGone) ReadDeclaration(ctx context.Context, o cluster.Member, read DeclarationRead) (map[uint16][]byte, bool, error) {
	if r.gone.Load() {
		return nil, false, nil
	}
	return r.inner.ReadDeclaration(ctx, o, read)
}

func (r transientGone) ReadBucketName(ctx context.Context, o cluster.Member, read DeclarationRead) (string, bool, error) {
	if r.gone.Load() {
		return "", false, nil
	}
	return r.inner.ReadBucketName(ctx, o, read)
}

// TestALiveBucketThatReadsAsNotFoundLosesNothingInTheReplay (BUG-0760): the bucket was never
// dropped, two pushes to it were acknowledged, and the declaration owner answers "not found"
// while the node replays. A not-found proves nothing at a boot, so the pair stays stopped and
// fenced, and once the owner answers both elements are applied, in order.
// [BREAK: ErrBucketNotFound becomes an empty write during the boot replay]
func TestALiveBucketThatReadsAsNotFoundLosesNothingInTheReplay(t *testing.T) {
	dir := t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	dbID := bug0760Box(t, node, u30iDB)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	slot := bug0760Slot(t, u30iDB)
	before := savePair(t, dir, slot, dbID)
	node = s0OpenClusterNode(t, dir, nil)
	bug0760Push(t, node, u30iDB, "x1", "x2")
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	rewindPair(t, dir, before)
	var gone atomic.Bool
	gone.Store(true)
	reopened, _, release := openHeld(t, dir, func(n *Node) {
		n.fetcher.cfg.Reader = transientGone{inner: n.fetcher.cfg.Reader, gone: &gone}
	})
	release()
	s0Await(t, "the state", 20*time.Second, func() bool { return reopened.State().StateSeq != 0 })
	time.Sleep(1500 * time.Millisecond) // several passes
	if flags := reopened.pairs.Snapshot(slot, dbID).Flags; flags&flagReplayNeeded == 0 {
		t.Fatalf("the pair is released (flags %#x) after a declaration read that said not found", flags)
	}
	gone.Store(false) // the declaration owner now answers
	got := bug0760Queue(t, reopened, u30iDB)
	if len(got) != 2 || got[0] != "x1" || got[1] != "x2" {
		t.Fatalf("queue = %v, want [x1 x2]: an acknowledged push on a live bucket was lost", got)
	}
}

// TestABootReplayEmptiesARecordOnlyForAProvenDrop (BUG-0760, widened by BUG-0771): the field
// engine of a cold cache. A not-found bucket is an empty write only when the node holds the 'b'
// record of that bucket, which the drop's bump wrote, on every path, boot replay or not; a bare
// not-found is "declaration unavailable", so the pair waits or the copy refuses.
func TestABootReplayEmptiesARecordOnlyForAProvenDrop(t *testing.T) {
	dir := t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	dbID := bug0760Box(t, node, u30iDB)
	const deadBucket = 0xDEAD0001
	engines := declaredEngines{node: node}

	if _, err := engines.FieldEngine(dbID, deadBucket, 1); !errors.Is(err, ErrDeclUnavailable) || errors.Is(err, errRetiredField) {
		t.Fatalf("a bare not-found gives %v, want ErrDeclUnavailable and no empty write", err)
	}
	// The retire loop deletes any 'b' record it finds on a live node, so it is held off until the
	// test ends, or it can sweep the record below before FieldEngine reads it.
	node.retire.loopMu.Lock()
	t.Cleanup(node.retire.loopMu.Unlock)
	batch := node.kv.NewBatch()
	if err := batch.Set(BucketRecord{Slot: 1, DBID: dbID, BucketID: deadBucket}.Key(), nil); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	if _, err := engines.FieldEngine(dbID, deadBucket, 1); !errors.Is(err, errRetiredField) {
		t.Fatalf("with the 'b' record of the bucket the field engine gives %v, want errRetiredField", err)
	}
}

// TestADeferredPairIsNeverOpenedToTheRetireGate (BUG-0760): a retired copy whose boot replay has
// not finished holds acknowledged records its fence does not cover, so the gate asks nobody and
// deletes nothing for it, whatever the copies answer; it passes as soon as the pair is not
// deferred. [BREAK: the gate ignores gateDeps.Deferred]
func TestADeferredPairIsNeverOpenedToTheRetireGate(t *testing.T) {
	fx := newGateFixture(t)
	asks := &gateAsks{answers: fx.readyAnswers()}
	var errs []error
	deps := fx.deps(asks, &errs)
	deps.Deferred = func(uint16, uint64) bool { return true }
	if err := newGateDriver(deps).Pass(t.Context()); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if n := asks.asked(); n != 0 {
		t.Fatalf("the gate asked %d members about a deferred pair, want none", n)
	}
	fx.requireRetiredAndNothingDeleted(t)

	deps.Deferred = func(uint16, uint64) bool { return false }
	if err := newGateDriver(deps).Pass(t.Context()); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if n := fx.deletes.Load(); n != 1 {
		t.Fatalf("the gate deleted %d ranges once the pair was not deferred, want 1", n)
	}
}

// failingAdvancer answers every advance with err and counts them.
type failingAdvancer struct {
	err   error
	calls int
}

func (a *failingAdvancer) Advance(uint16, uint64, AdvanceStep) error {
	a.calls++
	return a.err
}

func bug0760Record(t *testing.T, slot uint16, db uint64, seq uint64) cluster.WALRecord {
	t.Helper()
	encID, err := store.EncodeComponent("k")
	if err != nil {
		t.Fatal(err)
	}
	body, err := cluster.EncodeOp(cluster.Op{Class: cluster.OpUpsert, BucketID: 7, EncID: encID, IDValue: []byte{0x02, 2, 0, 0, 0, 'k', 0}})
	if err != nil {
		t.Fatalf("encoding an op: %v", err)
	}
	return cluster.WALRecord{WALRecord: storage.WALRecord{Body: body}, Slot: slot, DB: db, Pos: cluster.Position{Epoch: 1, Seq: seq}, Cursor: cluster.Cursor(seq)}
}

// TestAPairThatIsNotReadyStaysStoppedNotAbandoned (BUG-0760): a pair an earlier pass stopped that
// is retired now keeps its tail: it stays stopped, and fenced, instead of being released with
// records only this node's log holds (the retire fence lags a deferred pair). A pair no earlier
// pass stopped still fails the scan with ErrPairNotReady.
func TestAPairThatIsNotReadyStaysStoppedNotAbandoned(t *testing.T) {
	advancer := &failingAdvancer{err: errors.Join(ErrPairNotReady, errors.New("retired"))}
	writer := &Writer{cfg: WriterConfig{Pairs: &cluster.PairSet{}, Advancer: advancer}}
	prior := map[pairKey]struct{}{{3, 9}: {}}
	pass := newReplayDeferrals(prior, nil)
	if err := writer.replay(bug0760Record(t, 3, 9, 1), pass); err != nil {
		t.Fatalf("the replay of a prior pair that is not ready failed the scan: %v", err)
	}
	if err := writer.replay(bug0760Record(t, 3, 9, 2), pass); err != nil || advancer.calls != 1 {
		t.Fatalf("after the stop the second record is %v with %d advances, want skipped with 1", err, advancer.calls)
	}
	if stopped, _, _ := pass.stopped(); len(stopped) != 1 {
		t.Fatalf("the pair is listed as %v, want it still stopped", stopped)
	}
	if err := writer.replay(bug0760Record(t, 4, 9, 3), pass); !errors.Is(err, ErrPairNotReady) {
		t.Fatalf("a pair no earlier pass stopped: %v, want the scan to fail with ErrPairNotReady", err)
	}
}

// TestTheRetryOfAStuckReplayBacksOffAndReportsOnce (BUG-0760): while the state never arrives
// the passes follow the capped doubling pace, not the timer of a fixed 200 ms, the same
// failure is reported once, and the pair replays within a second of the state arriving.
func TestTheRetryOfAStuckReplayBacksOffAndReportsOnce(t *testing.T) {
	dir := t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	dbID := bug0760Box(t, node, u30iDB)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	before := savePair(t, dir, bug0760Slot(t, u30iDB), dbID)
	node = s0OpenClusterNode(t, dir, nil)
	bug0760Push(t, node, u30iDB, "p1")
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	rewindPair(t, dir, before)

	var passes atomic.Int64
	bootReplayPass = func() { passes.Add(1) }
	t.Cleanup(func() { bootReplayPass = nil })
	bug0760Errors = &s0Errors{}
	reopened, _, release := openHeld(t, dir)
	time.Sleep(1500 * time.Millisecond)
	held := passes.Load()
	if held > 5 {
		t.Fatalf("%d passes in 1.5 s with no state: the retry does not back off (a fixed 200 ms would make 8)", held)
	}
	release()
	start := time.Now()
	if got := bug0760Queue(t, reopened, u30iDB); len(got) != 1 || got[0] != "p1" {
		t.Fatalf("Queue = %v, want [p1]", got)
	}
	if took := time.Since(start); took > 1200*time.Millisecond {
		t.Fatalf("the pair replayed %v after the state arrived: the retry waits out its timer instead of waking on the new state", took)
	}
	reports := 0
	for _, text := range bug0760Errors.all() {
		if strings.Contains(text, "finishing the replay") {
			reports++
		}
	}
	if reports > 2 {
		t.Fatalf("%d reports of the same failure, want it reported once", reports)
	}
}

// TestADeferredPairSaysWhatItWaitsFor (BUG-0760): between the state arriving and the replay
// finishing, a read and a write are refused with the cause, not with the text of a failed
// apply or a poisoned log.
func TestADeferredPairSaysWhatItWaitsFor(t *testing.T) {
	dir := t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	dbID := bug0760Box(t, node, u30iDB)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	before := savePair(t, dir, bug0760Slot(t, u30iDB), dbID)
	node = s0OpenClusterNode(t, dir, nil)
	bug0760Push(t, node, u30iDB, "p1")
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	rewindPair(t, dir, before)

	pass := make(chan struct{})
	var hold atomic.Bool
	hold.Store(true)
	bootReplayPass = func() {
		if hold.Load() {
			select {
			case <-pass:
			case <-time.After(15 * time.Second): // a failed test must not hang Close
			}
		}
	}
	t.Cleanup(func() { bootReplayPass = nil; hold.Store(false); close(pass) })
	reopened, _, release := openHeld(t, dir)
	release()
	s0Await(t, "the state", 20*time.Second, func() bool { return reopened.State().StateSeq != 0 })
	target := StoreTarget{Database: u30iDB, Bucket: "Box", ID: "b1", Field: "Queue"}
	deadline := time.Now().Add(20 * time.Second)
	var readErr, writeErr error
	for time.Now().Before(deadline) {
		_, readErr = reopened.Peek(context.Background(), PeekRequest{StoreTarget: target, Count: 1})
		writeErr = reopened.Push(context.Background(), PushRequest{StoreTarget: target, Element: bson.D{{Key: "item", Value: "early"}}})
		if readErr != nil && writeErr != nil && strings.Contains(readErr.Error(), "replaying this slot") && strings.Contains(writeErr.Error(), "replaying this slot") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	for what, err := range map[string]error{"read": readErr, "write": writeErr} {
		if err == nil || !strings.Contains(err.Error(), bootReplayHoldCause) {
			t.Fatalf("the %s of a deferred pair says %v, want the cause %q", what, err, bootReplayHoldCause)
		}
		if strings.Contains(err.Error(), "failed apply") || strings.Contains(err.Error(), "retained log") {
			t.Fatalf("the %s of a deferred pair names a failed apply or a poisoned log: %v", what, err)
		}
	}
	hold.Store(false)
	pass <- struct{}{}
}

// goneReader answers that no bucket of its database exists: what a bucket dropped while the node
// was down gives once the sweep has deleted its reverse-index document.
type goneReader struct {
	inner DeclarationReader
	db    uint64
}

func (r goneReader) ReadDeclaration(ctx context.Context, owner cluster.Member, read DeclarationRead) (map[uint16][]byte, bool, error) {
	if read.DBID == r.db {
		return nil, false, nil
	}
	return r.inner.ReadDeclaration(ctx, owner, read)
}

func (r goneReader) ReadBucketName(ctx context.Context, owner cluster.Member, read DeclarationRead) (string, bool, error) {
	if read.DBID == r.db {
		return "", false, nil
	}
	return r.inner.ReadBucketName(ctx, owner, read)
}

// TestAPairWithAProvenDroppedBucketIsReleased (BUG-0760): the node holds the 'b' record of the
// bucket (the drop's bump reached it), and the ring says the bucket does not exist. The record
// replays as an empty write, the pair is applied past it and released.
func TestAPairWithAProvenDroppedBucketIsReleased(t *testing.T) {
	dir := t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	dbID := bug0760Box(t, node, u30iDB)
	bucketID := bug0760BucketID(t, node)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	slot := bug0760Slot(t, u30iDB)
	before := savePair(t, dir, slot, dbID)
	node = s0OpenClusterNode(t, dir, nil)
	bug0760Push(t, node, u30iDB, "x1")
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	rewindPair(t, dir, before)
	kv := s0Badger(t, dir)
	batch := kv.NewBatch()
	if err := batch.Set(BucketRecord{Slot: slot, DBID: dbID, BucketID: bucketID}.Key(), nil); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	batch.Discard()
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _, release := openHeld(t, dir, func(n *Node) {
		n.fetcher.cfg.Reader = goneReader{inner: n.fetcher.cfg.Reader, db: dbID}
	})
	release()
	s0Await(t, "the pair of the dropped bucket to be released", 30*time.Second, func() bool {
		return reopened.pairs.Snapshot(slot, dbID).Flags&(flagReplayNeeded|flagBootReplay) == 0
	})
	if applied := reopened.pairs.Snapshot(slot, dbID).AppliedPos; applied.Seq < 2 {
		t.Fatalf("the pair is applied to %s, want it past the record of the dropped bucket", applied)
	}
}

// bug0760BucketID is the id of bucket Box of database app on node.
func bug0760BucketID(t *testing.T, node *Node) uint64 {
	t.Helper()
	ref, err := node.bucket(context.Background(), u30iDB, "Box")
	if err != nil {
		t.Fatal(err)
	}
	return ref.entry.Decl.BucketID
}
