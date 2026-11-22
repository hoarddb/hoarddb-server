package wire

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Seeding slice S1, step 3: the node's one reclaim budget and the budgeted reset
// (the entry ring seeding completion spec §1.3).

// s1VirtualClock is a budget clock that only the budget's sleeps move, so a test reads the
// time a reset was made to wait without waiting.
type s1VirtualClock struct {
	mu    sync.Mutex
	now   time.Time
	slept time.Duration
}

func (c *s1VirtualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *s1VirtualClock) Sleep(_ context.Context, d time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	c.slept += d
	return nil
}

func (c *s1VirtualClock) total() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.slept
}

func s1Budget(clock *s1VirtualClock, keysPerSecond, stepKeys int) *reclaimBudget {
	return newReclaimBudget(reclaimBudgetConfig{KeysPerSecond: keysPerSecond, StepKeys: stepKeys, Now: clock.Now, Sleep: clock.Sleep})
}

// s1FillRange writes count keys into the pair's range.
func s1FillRange(t *testing.T, kv storage.KV, slot uint16, db uint64, count int) {
	t.Helper()
	batch := kv.NewBatch()
	defer batch.Discard()
	for number := range count {
		if err := batch.Set(t3Key(slot, db, number), []byte("old")); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
}

// s1CountRange counts the keys of the pair's range.
func s1CountRange(t *testing.T, kv storage.KV, slot uint16, db uint64) int {
	t.Helper()
	return len(t3Dump(t, kv, slot, db))
}

// s1BatchKV counts the batches committed through it and the DropPrefixes calls it is offered.
type s1BatchKV struct {
	storage.KV
	commits atomic.Int64
	drops   atomic.Int64
}

func (k *s1BatchKV) NewBatch() storage.KVBatch {
	return &s1CountingBatch{KVBatch: k.KV.NewBatch(), kv: k}
}

// DropPrefixes is what a reset that dropped its range in one call would reach for.
func (k *s1BatchKV) DropPrefixes([][]byte) error {
	k.drops.Add(1)
	return nil
}

type s1CountingBatch struct {
	storage.KVBatch
	kv *s1BatchKV
}

func (b *s1CountingBatch) Commit(sync bool) error {
	b.kv.commits.Add(1)
	return b.KVBatch.Commit(sync)
}

// TestTheResetIsChargedToTheReclaimBudget: a reset of 3,000 keys against a 1,000-key credit at
// 1,000 keys per second waits for the other 2,000, and the keys are gone afterwards.
// [BREAK: charge nothing in resetRange]
func TestTheResetIsChargedToTheReclaimBudget(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	clock := &s1VirtualClock{now: time.Unix(1_700_000_000, 0)}
	puller := NewPuller(PullerConfig{Self: testSelf, Recon: rig.rec, KV: rig.kv, Budget: s1Budget(clock, 1000, 1000)})
	s1FillRange(t, rig.kv, rig.slot, reconDB, 3000)

	if err := puller.resetRange(context.Background(), reconcileKey{rig.slot, reconDB}); err != nil {
		t.Fatal(err)
	}
	if left := s1CountRange(t, rig.kv, rig.slot, reconDB); left != 0 {
		t.Fatalf("%d keys survive the reset", left)
	}
	if got := clock.total(); got < 1900*time.Millisecond || got > 2100*time.Millisecond {
		t.Fatalf("the reset waited %v for 3,000 keys at 1,000 per second with a 1,000-key credit, want about 2s", got)
	}
}

// TestTheRetireSweepAndTheResetShareOneBudget: the node builds one budget and the sweep, the
// puller (and so the reset) hold that same object. [BREAK: two objects, by not passing the
// node's budget to the sweep]
func TestTheRetireSweepAndTheResetShareOneBudget(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	if node.reclaim == nil {
		t.Fatal("the node built no reclaim budget")
	}
	if node.retire.budget != node.reclaim {
		t.Fatal("the retire sweep charges a budget of its own, not the node's")
	}
	if node.cl.seeding.Load().puller.cfg.Budget != node.reclaim {
		t.Fatal("the puller charges a budget of its own, not the node's")
	}
}

// TestTheDedupeSweepStillChargesTheMovedBudget: the dedupe pass charges the budget handed to
// the sweep, not one it made for itself, so a reset and a dedupe round draw on one rate.
// [BREAK: leave the dedupe pass on a budget of its own]
func TestTheDedupeSweepStillChargesTheMovedBudget(t *testing.T) {
	clock := &s1VirtualClock{now: time.Unix(1_700_000_000, 0)}
	shared := s1Budget(clock, 1, 4)
	rig := newSweepLoopRig(t, false, func(deps *RetireDeps) { deps.KeysPerSecond = 1_000_000; deps.StepKeys = 4; deps.Budget = shared })
	rig.fill(1, 3*sweepTestWindow)
	rig.sendTick()
	rig.waitFor("a charge on the shared budget", func() bool { return clock.total() > 0 })
	if got := clock.total(); got < time.Second {
		t.Fatalf("the shared budget made the dedupe pass wait %v, want at least a second on a 1 key/s budget", got)
	}
	if rig.slept.Load() != 0 {
		t.Fatalf("the sweep slept %v on a budget of its own", time.Duration(rig.slept.Load()))
	}
}

// TestTheResetNeverCallsDropPrefixes: the reset deletes in batches of resetRangeBatch keys;
// DropPrefixes stops compaction and refuses every writer on the node. [BREAK: use it]
func TestTheResetNeverCallsDropPrefixes(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	counting := &s1BatchKV{KV: rig.kv}
	clock := &s1VirtualClock{now: time.Unix(1_700_000_000, 0)}
	puller := NewPuller(PullerConfig{Self: testSelf, Recon: rig.rec, KV: counting, Budget: s1Budget(clock, 1_000_000, 4096)})
	s1FillRange(t, rig.kv, rig.slot, reconDB, 2*resetRangeBatch+10)

	if err := puller.resetRange(context.Background(), reconcileKey{rig.slot, reconDB}); err != nil {
		t.Fatal(err)
	}
	if counting.drops.Load() != 0 {
		t.Fatalf("the reset called DropPrefixes %d times", counting.drops.Load())
	}
	if got := counting.commits.Load(); got != 3 {
		t.Fatalf("the reset committed %d batches for %d keys, want 3 batches of at most %d", got, 2*resetRangeBatch+10, resetRangeBatch)
	}
	if left := s1CountRange(t, rig.kv, rig.slot, reconDB); left != 0 {
		t.Fatalf("%d keys survive the reset", left)
	}
}

// TestTheStoreIsSyncedAfterTheResetAndBeforeTheRecordIsReady records the order of a whole pull
// with the register in it: SEED_REGISTER first, because the range is reset only once a source
// has accepted (BUG-0819: a range is not deleted while nothing can be pulled), then the
// reset's deletes and their sync, the chunks, and the ready record in a commit that syncs the
// store (the attempt's one sync after the reset, seeding-speed-note.md).
// [BREAK: no sync between the reset and the first chunk; commit the ready record without a sync]
func TestTheStoreIsSyncedAfterTheResetAndBeforeTheRecordIsReady(t *testing.T) {
	badger := t3OpenKV(t, t.TempDir())
	t.Cleanup(func() { _ = badger.Close() })
	watching := &t3RecordKV{KV: badger}
	rig := newT3Rig(t, t3Options{kv: watching})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 5, t3Cut(t, rig), 42)
	s1FillRange(t, badger, rig.slot, reconDB, 3) // old data the reset must remove
	rig.wrap = func(_ string, msg transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if msg.Env.Kind == transport.KindSeedRegister {
			watching.mu.Lock()
			watching.events = append(watching.events, "register")
			watching.mu.Unlock()
		}
		return reply, err
	}
	rig.pullUntil("the pair to be ready", rig.isReady)
	watching.mu.Lock()
	defer watching.mu.Unlock()
	if got := t3Compact(watching.events); got != "register reset sync chunk synced-ready" {
		t.Fatalf("the pull ran %q, want the register, the reset, a sync, the chunks and the ready record in a synced commit", got)
	}
}
