package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// Storage-format slice A, unit F3: the dedupe sweep runs on the retention tick, on the one
// reclaim loop, charged to the one reclaim budget, round robin over the held pairs, and a
// dropped pair forgets its cursor.

const (
	sweepTestDB     uint64 = 7
	sweepTestBucket uint64 = 41
	sweepTestWindow uint64 = 4
)

// sweepLoopRig is a reclaim loop with a dedupe sweeper and a test-driven tick.
type sweepLoopRig struct {
	t       *testing.T
	kv      storage.KV
	pairs   *cluster.PairSet
	sweeper *DedupeSweeper
	sweep   *RetireSweep
	tick    chan time.Time
	deps    RetireDeps
	slept   atomic.Int64
}

func newSweepLoopRig(t *testing.T, manual bool, tune func(*RetireDeps)) *sweepLoopRig {
	t.Helper()
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	rig := &sweepLoopRig{t: t, kv: kv, pairs: &cluster.PairSet{}, tick: make(chan time.Time)}
	cfg := machine.WindowConfig{Window: sweepTestWindow, MaxResult: 1024}
	rig.sweeper = NewDedupeSweeper(kv, rig.pairs, cfg)
	rig.pairs.OnDrop(rig.sweeper.Forget)
	decls := &u43Decls{decls: map[string]machine.Declaration{}, names: map[uint64]string{}}
	rig.deps = RetireDeps{
		KV: kv, Pairs: rig.pairs, Declarations: decls, BucketName: decls.bucketName,
		Published: func(uint64) int64 { return 1 }, KeysPerSecond: 1_000_000, StepKeys: 1000,
		Now: func() time.Time { return time.Unix(1_700_000_000, 0) },
		Sleep: func(_ context.Context, d time.Duration) error {
			rig.slept.Add(int64(d))
			return nil
		},
		Tick: rig.tick, TickInterval: time.Minute, Dedupe: rig.sweeper, Manual: manual,
	}
	if tune != nil {
		tune(&rig.deps)
	}
	rig.sweep = NewRetireSweep()
	rig.sweep.Start(context.Background(), rig.deps)
	t.Cleanup(rig.sweep.Close)
	return rig
}

// fill holds the pair and writes n entries with ordinals 1..n, the pair's ordinal at n, so
// the first n-window are dead.
func (r *sweepLoopRig) fill(slot uint16, n uint64) {
	r.t.Helper()
	r.pairs.With(slot, sweepTestDB, func(p *cluster.Pair) { p.Ordinal = n })
	ref := storage.BucketRef{Slot: slot, DatabaseID: sweepTestDB, BucketID: sweepTestBucket}
	for i := uint64(1); i <= n; i++ {
		var id [16]byte
		binary.BigEndian.PutUint64(id[8:], i)
		u31PutEntry(r.t, r.kv, ref, machine.DedupeEntry{Key: machine.DedupeKey{RequestID: id}, Pos: cluster.Position{Epoch: 1, Seq: i}, Ordinal: i})
	}
}

func (r *sweepLoopRig) dedupeCount(slot uint16) int {
	r.t.Helper()
	return countDedupeKeys(r.t, r.kv, slot, sweepTestDB)
}

func countDedupeKeys(t *testing.T, kv storage.KV, slot uint16, db uint64) int {
	t.Helper()
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	prefix := storage.PairPrefix(slot, db)
	iter := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyRangeEnd(prefix)})
	defer iter.Close()
	n := 0
	for iter.Seek(prefix); iter.Valid(); iter.Next() {
		if key := iter.Key(); len(key) == dedupeKeyLen && key[18] == storage.NSDedupe {
			n++
		}
	}
	return n
}

// sendTick hands the loop one tick, or fails when nothing receives it: the loop must select
// on the tick, and a loop that does not would block the test forever.
func (r *sweepLoopRig) sendTick() {
	r.t.Helper()
	select {
	case r.tick <- time.Now():
	case <-time.After(5 * time.Second):
		r.t.Fatal("the reclaim loop never received the retention tick")
	}
}

func (r *sweepLoopRig) waitFor(what string, cond func() bool) {
	r.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			r.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestTheDedupeSweepRunsOnATick(t *testing.T) {
	rig := newSweepLoopRig(t, false, nil)
	rig.fill(1, 3*sweepTestWindow)
	// The Start woke the loop once and nothing else wakes it: no tick, no sweep.
	rig.sweep.wakeUp()
	time.Sleep(150 * time.Millisecond)
	if got := rig.dedupeCount(1); got != int(3*sweepTestWindow) {
		t.Fatalf("%d entries before any tick, want %d: only the retention tick drives the sweep", got, 3*sweepTestWindow)
	}
	rig.sendTick()
	rig.waitFor("the tick's sweep", func() bool {
		return rig.dedupeCount(1) <= int(sweepTestWindow)+rig.deps.StepKeys && rig.dedupeCount(1) < int(3*sweepTestWindow)
	})
	rig.waitFor("the window to hold", func() bool { return rig.dedupeCount(1) == int(sweepTestWindow) })
}

func TestTheDedupeSweepIsChargedToTheReclaimBudget(t *testing.T) {
	rig := newSweepLoopRig(t, false, func(deps *RetireDeps) { deps.KeysPerSecond = 1; deps.StepKeys = 4 })
	rig.fill(1, 3*sweepTestWindow)
	rig.sendTick()
	rig.waitFor("a charged sweep", func() bool { return rig.slept.Load() > 0 })
	// A step of 4 visits and its deletes against a 4-token bucket at one key per second is
	// seconds of sleep, not microseconds: the cost reached the budget whole.
	if got := time.Duration(rig.slept.Load()); got < time.Second {
		t.Fatalf("the sweep slept %v on a 1 key/s budget, want at least a second: the visits and deletes were not charged", got)
	}
}

func TestEveryHeldPairIsSweptWithinTheBound(t *testing.T) {
	const held = 10
	// cycle = max(60 s, 10 pairs × 1000 step / 100 keys per second = 100 s) = 100 s; a 10 s tick
	// gives 10 ticks per cycle, so K = 1 pair per tick and every pair is swept in 10 ticks.
	rig := newSweepLoopRig(t, false, func(deps *RetireDeps) { deps.KeysPerSecond = 100; deps.TickInterval = 10 * time.Second })
	for slot := uint16(1); slot <= held; slot++ {
		rig.fill(slot, 2*sweepTestWindow)
	}
	for i := 0; i < held; i++ {
		rig.sendTick()
	}
	rig.waitFor("every pair swept", func() bool {
		for slot := uint16(1); slot <= held; slot++ {
			if rig.dedupeCount(slot) != int(sweepTestWindow) {
				return false
			}
		}
		return true
	})
}

func TestALongBucketDropDoesNotStarveTheSweep(t *testing.T) {
	rig := newSweepLoopRig(t, true, func(deps *RetireDeps) { deps.StepKeys = 2 })
	rig.tick = make(chan time.Time, 1)
	rig.deps.Tick = rig.tick
	rig.sweep.deps.Tick = rig.tick
	rig.fill(1, 3*sweepTestWindow)
	// Forty dropped buckets in the pair, each with three keys and a 'b' record: a long lap.
	const dropped = 40
	batch := rig.kv.NewBatch()
	for bucket := uint64(100); bucket < 100+dropped; bucket++ {
		for i := 0; i < 3; i++ {
			if err := batch.Set(storage.Key(1, sweepTestDB, bucket, storage.NSDocuments, []byte{byte(i)}), []byte{1}); err != nil {
				t.Fatal(err)
			}
		}
		if err := batch.Set(BucketRecord{Slot: 1, DBID: sweepTestDB, BucketID: bucket}.Key(), nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	remainingRecords := func() int {
		page, err := rig.sweep.recordPage(reclaimBucketKind, nil)
		if err != nil {
			t.Fatal(err)
		}
		return len(page)
	}
	rig.tick <- time.Now()
	interleaved := false
	rig.sweep.afterStep = func() {
		if rig.dedupeCount(1) < int(3*sweepTestWindow) && remainingRecords() > 0 {
			interleaved = true
		}
	}
	if err := rig.sweep.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !interleaved {
		t.Fatal("the dedupe pass never ran while 'b' records remained: a long bucket drop starved it")
	}
	if got := remainingRecords(); got != 0 {
		t.Fatalf("%d 'b' records remain after the drain", got)
	}
}

// One record that takes many steps must not hold the dedupe round off until it ends: the
// tick that arrives after its first step is served before its second.
func TestOneLongRecordIsInterleavedStepByStep(t *testing.T) {
	rig := newSweepLoopRig(t, true, func(deps *RetireDeps) { deps.StepKeys = 2 })
	rig.tick = make(chan time.Time, 1)
	rig.deps.Tick = rig.tick
	rig.sweep.deps.Tick = rig.tick
	rig.fill(1, 3*sweepTestWindow)
	const bucket = 100
	batch := rig.kv.NewBatch()
	for i := 0; i < 20; i++ {
		if err := batch.Set(storage.Key(1, sweepTestDB, bucket, storage.NSDocuments, []byte{byte(i)}), []byte{1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Set(BucketRecord{Slot: 1, DBID: sweepTestDB, BucketID: bucket}.Key(), nil); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	steps, countAtFirstStep := 0, 0
	ranBetweenSteps := false
	rig.sweep.afterStep = func() {
		steps++
		switch steps {
		case 1:
			countAtFirstStep = rig.dedupeCount(1)
			rig.tick <- time.Now()
		case 2:
			ranBetweenSteps = rig.dedupeCount(1) < countAtFirstStep
		}
	}
	if err := rig.sweep.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if steps < 3 {
		t.Fatalf("the record took %d steps, want a record of many", steps)
	}
	if !ranBetweenSteps {
		t.Fatal("a tick that arrived after the record's first step was not served before its second: the step loop never checks the tick")
	}
}

// A visit is charged for the keys it walked and the keys it deleted, so a pair of dead
// entries costs up to twice the step; the fanout's cycle must be sized on that cost.
func TestDedupeFanoutSizesTheCycleOnTheWorstChargePerVisit(t *testing.T) {
	sweep := &RetireSweep{deps: RetireDeps{StepKeys: 1000, KeysPerSecond: 100, TickInterval: time.Minute}}
	// 10 pairs x 2 x 1000 keys at 100 keys/s = 200 s = 3 one-minute ticks, so 4 pairs a tick.
	if got := sweep.dedupeFanout(10); got != 4 {
		t.Fatalf("dedupeFanout(10) = %d, want 4: a visit is charged up to 2 x StepKeys", got)
	}
}

func TestADroppedPairForgetsItsCursor(t *testing.T) {
	rig := newSweepLoopRig(t, true, nil)
	rig.fill(1, 20)
	if _, _, err := rig.sweeper.Pass(1, sweepTestDB, 5); err != nil {
		t.Fatal(err)
	}
	rig.sweeper.mu.Lock()
	cursors := len(rig.sweeper.cursors)
	rig.sweeper.mu.Unlock()
	if cursors != 1 {
		t.Fatalf("%d cursors after a partial pass, want 1", cursors)
	}
	rig.pairs.Drop(1, sweepTestDB)
	rig.sweeper.mu.Lock()
	cursors = len(rig.sweeper.cursors)
	rig.sweeper.mu.Unlock()
	if cursors != 0 {
		t.Fatalf("%d cursors after the pair was dropped, want 0: Drop must call Forget", cursors)
	}
}

func TestASweepNeverResurrectsADroppedPair(t *testing.T) {
	rig := newSweepLoopRig(t, true, nil)
	rig.fill(1, 3*sweepTestWindow)
	rig.pairs.Drop(1, sweepTestDB)
	visited, deleted, err := rig.sweeper.Pass(1, sweepTestDB, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 0 {
		t.Fatalf("a pass over a dropped pair deleted %d entries (visited %d)", deleted, visited)
	}
	if held := rig.pairs.WithHeld(1, sweepTestDB, func(*cluster.Pair) {}); held {
		t.Fatal("the pass resurrected the dropped pair: reap must use WithHeld, never With")
	}
	if slots := rig.pairs.HeldSlots(sweepTestDB); len(slots) != 0 {
		t.Fatalf("held slots after the pass = %v, want none", slots)
	}
}

// TestASeedingPairAtOrdinalZeroIsNeverReaped is the data-safety proof that an entry ahead of
// the pair's ordinal is live: a seeding pair's ordinal is 0 until its cut is adopted.
func TestASeedingPairAtOrdinalZeroIsNeverReaped(t *testing.T) {
	rig := newSweepLoopRig(t, true, nil)
	rig.fill(1, 3*sweepTestWindow)
	rig.pairs.With(1, sweepTestDB, func(p *cluster.Pair) { p.Ordinal = 0 })
	if _, deleted, err := rig.sweeper.Pass(1, sweepTestDB, 1000); err != nil || deleted != 0 {
		t.Fatalf("Pass at ordinal 0 deleted %d, %v; want nothing: every entry is ahead of the ordinal", deleted, err)
	}
	if got := rig.dedupeCount(1); got != int(3*sweepTestWindow) {
		t.Fatalf("%d entries after a pass at ordinal 0, want %d", got, 3*sweepTestWindow)
	}
}

// TestASweptWindowStillAnswersARetryInsideIt drives a real node's ticker: window + K pops on
// one pair, the sweep reaps the dead entries, and the most recent pop's retry is answered
// from its entry, before and after a restart.
func TestASweptWindowStillAnswersARetryInsideIt(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ring")
	cfg := NodeConfig{Dir: dir, DedupeWindow: 4, RetentionCheck: 20 * time.Millisecond}
	open := func() *Node {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		node, err := OpenNode(ctx, cfg)
		if err != nil {
			t.Fatalf("OpenNode: %v", err)
		}
		return node
	}
	node := open()
	u30iSchema(t, node)
	ctx := context.Background()
	if _, err := node.Insert(ctx, u30iDB, "User", u30iUser("u1", "Ada", 36)); err != nil {
		t.Fatal(err)
	}
	target := u30iTarget("u1", "Purchases")
	const pops = 10 // window 4 + K 6
	for i := 0; i < pops+2; i++ {
		if err := node.Push(ctx, PushRequest{StoreTarget: target, Element: bson.D{{Key: "item", Value: fmt.Sprintf("item%02d", i)}}}); err != nil {
			t.Fatal(err)
		}
	}
	var last Popped
	var lastID [16]byte
	for i := 0; i < pops; i++ {
		lastID = [16]byte{0xB0, byte(i + 1)}
		popped, err := node.Pop(ctx, PopRequest{StoreTarget: target}, WithRequestID(lastID))
		if err != nil || !popped.Found {
			t.Fatalf("pop %d = %+v, %v", i, popped, err)
		}
		last = popped
	}
	slot := node.pairs.HeldSlots(1)
	dbID, _ := node.database(u30iDB)
	countAll := func() int {
		total := 0
		for _, s := range node.pairs.HeldSlots(dbID) {
			total += countDedupeKeys(t, node.kv, s, dbID)
		}
		return total
	}
	_ = slot
	before := countAll()
	deadline := time.Now().Add(10 * time.Second)
	for countAll() >= before {
		if time.Now().After(deadline) {
			t.Fatalf("no sweep ran on the node's ticker: %d entries still", countAll())
		}
		time.Sleep(10 * time.Millisecond)
	}
	marker := func(n *Node) string {
		doc, err := n.Get(ctx, u30iDB, "User", "u1")
		if err != nil {
			t.Fatal(err)
		}
		return u30iString(t, doc, "Purchases")
	}
	if got := marker(node); got != "fifoStore (2 elements)" {
		t.Fatalf("Purchases = %q after %d pops of %d pushes", got, pops, pops+2)
	}
	retry, err := node.Pop(ctx, PopRequest{StoreTarget: target}, WithRequestID(lastID))
	if err != nil || !retry.Found || !bytes.Equal(retry.Element, last.Element) {
		t.Fatalf("retry after the sweep = %+v, %v; want the recorded element %v", retry, err, last.Element)
	}
	if got := marker(node); got != "fifoStore (2 elements)" {
		t.Fatalf("Purchases = %q after the retry: it was applied again", got)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	node = open()
	defer func() { _ = node.Close() }()
	retry, err = node.Pop(ctx, PopRequest{StoreTarget: target}, WithRequestID(lastID))
	if err != nil || !retry.Found || !bytes.Equal(retry.Element, last.Element) {
		t.Fatalf("retry after the restart = %+v, %v; want the recorded element %v", retry, err, last.Element)
	}
	if got := marker(node); got != "fifoStore (2 elements)" {
		t.Fatalf("Purchases = %q after the retry on the restarted node: it was applied again", got)
	}
}

// Keep the race run honest: a Drop during a pass is the case the WithHeld rule exists for.
func TestADropDuringAPassIsSafe(t *testing.T) {
	rig := newSweepLoopRig(t, true, nil)
	for slot := uint16(1); slot <= 8; slot++ {
		rig.fill(slot, 3*sweepTestWindow)
	}
	var wg sync.WaitGroup
	for slot := uint16(1); slot <= 8; slot++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if _, _, err := rig.sweeper.Pass(slot, sweepTestDB, 3); err != nil {
					t.Error(err)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			rig.pairs.Drop(slot, sweepTestDB)
		}()
	}
	wg.Wait()
	if slots := rig.pairs.HeldSlots(sweepTestDB); len(slots) != 0 {
		t.Fatalf("held slots after the drops = %v, want none", slots)
	}
}
