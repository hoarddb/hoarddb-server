package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Build doc Step 15: the reclaimer. T-05, T-15, T-26 (a)–(e), T-27 (a) and
// (c), T-34, and SA-8's test of the escape hatch.

// errReclaimCrash is what a hook returns to stop a lap where a crash would.
var errReclaimCrash = errors.New("simulated crash inside the reclaimer")

// fakeReclaimClock is a clock the reclaimer's budget sleeps on without
// waiting: a sleep moves it forward. It lets a test run the default budget of
// 10⁴ visits a second over 10⁶ visits and read off where each step fell.
type fakeReclaimClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeReclaimClock() *fakeReclaimClock {
	return &fakeReclaimClock{now: time.Now()}
}

func (c *fakeReclaimClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeReclaimClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	return nil
}

func (c *fakeReclaimClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// useReclaimClock makes every reclaimer created from here on charge its budget
// against clock.
func useReclaimClock(t *testing.T, clock *fakeReclaimClock) {
	t.Helper()
	var asClock reclaimClock = clock
	reclaimClockHook.Store(&asClock)
	t.Cleanup(func() { reclaimClockHook.Store(nil) })
}

// holdReclaimer blocks the reclaimer at the top of its next lap until release
// is called. Call it after the server has started, so the release registered
// here runs before the server's own cleanup stops it.
func holdReclaimer(t *testing.T) (release func()) {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	hook := func() { <-gate }
	reclaimLapHook.Store(&hook)
	release = func() {
		once.Do(func() {
			reclaimLapHook.Store(nil)
			close(gate)
		})
	}
	t.Cleanup(release)
	return release
}

// watchSteps records every reclaim step of the given ids — every step when
// none is given — and passes each to also, whose error ends the lap there.
func watchSteps(t *testing.T, also func(reclaimStep) error, ids ...uint64) func() []reclaimStep {
	t.Helper()
	var mu sync.Mutex
	var steps []reclaimStep
	watched := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		watched[id] = true
	}
	hook := func(step reclaimStep) error {
		if len(watched) > 0 && !watched[step.ID] {
			return nil
		}
		mu.Lock()
		steps = append(steps, step)
		mu.Unlock()
		if also != nil {
			return also(step)
		}
		return nil
	}
	reclaimStepHook.Store(&hook)
	t.Cleanup(func() { reclaimStepHook.Store(nil) })
	return func() []reclaimStep {
		mu.Lock()
		defer mu.Unlock()
		return append([]reclaimStep(nil), steps...)
	}
}

// pendingReclaimKeys counts the keys stored under the bucket reclaim records'
// range, whatever their length: the tests read it independently of the
// reclaimer's own filter, so a filter that matches nothing cannot make a
// backlog look drained.
func pendingReclaimKeys(t testing.TB, kv storage.KV) int {
	t.Helper()
	lower, upper := storage.NodeKey(storage.NSReclaim), reclaimKey(storage.OrphanCursorID)
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: upper})
	defer it.Close()
	count := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		count++
	}
	return count
}

// waitReclaimed waits until no bucket reclaim record is left.
func waitReclaimed(t testing.TB, kv storage.KV, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		pending := pendingReclaimKeys(t, kv)
		if pending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after %s, %d reclaim record(s) are still pending: the reclaimer never finished (or never visited) them", within, pending)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// liveKeysIn counts the live keys of bucket id in slot.
func liveKeysIn(t testing.TB, kv storage.KV, slot uint16, databaseID, id uint64) int {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: storage.Scope(slot, databaseID, id), Upper: storage.Scope(slot, databaseID, id+1)})
	defer it.Close()
	count := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		count++
	}
	return count
}

// writeKeys commits count keys built by keyAt, in batches, straight into the
// store: building a million-key bucket through the verbs would take the test
// minutes and add nothing — the reclaimer reads keys, not how they got there.
func writeKeys(t testing.TB, kv storage.KV, count int, keyAt func(i int) []byte, value []byte, deleting bool) {
	t.Helper()
	const perBatch, bytesPerBatch = 5_000, 1 << 20
	for start := 0; start < count; {
		batch := kv.NewBatch()
		end, held := start, 0
		for ; end < count && end-start < perBatch && held < bytesPerBatch; end++ {
			key := keyAt(end)
			held += len(key) + len(value)
			var err error
			if deleting {
				err = batch.Delete(key)
			} else {
				err = batch.Set(key, value)
			}
			if err != nil {
				t.Fatalf("adding key %d to a batch: %v", end, err)
			}
		}
		if err := batch.Commit(false); err != nil {
			t.Fatalf("committing keys %d…: %v", start, err)
		}
		batch.Discard()
		start = end
	}
}

func queuePositionKey(slot uint16, databaseID, id uint64, position uint64) []byte {
	return storage.Key(slot, databaseID, id, storage.NSQueue, binary.BigEndian.AppendUint64(nil, position))
}

func dropOrFail(t testing.TB, s *Server, database, bucket string) {
	t.Helper()
	if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: database, Name: bucket}); err != nil || !resp.Success {
		t.Fatalf("drop %s/%s: %+v, %v", database, bucket, resp, err)
	}
}

// TestReclaimIsBoundedInVisits is T-26(a): no batch exceeds the visit or the
// byte bound, and visits plus unlinks stay within the budget in every 60 s
// window — run on a dropped fifo whose head carries 10⁶ uncompacted pop
// markers, where one step that walked the run would pass a per-batch count of
// deletes while doing a million visits.
func TestReclaimIsBoundedInVisits(t *testing.T) {
	const database = "app"

	t.Run("a fifo head of a million pop markers", func(t *testing.T) {
		const markers, liveEntries = 1_000_000, 10
		clock := newFakeReclaimClock()
		useReclaimClock(t, clock)
		s := newTestServer(t)
		createTestBucket(t, s, database, "markers", BucketTypeFIFO)
		b := lookupBucket(s, database, "markers")
		slot, databaseID, id := b.slot, b.databaseID, b.id.Load()

		// A snapshot opened before the writes keeps every version they make
		// visible to compaction, so the markers stay uncompacted for the test.
		pin := s.kv.NewSnapshot()
		defer pin.Discard()
		keyAt := func(i int) []byte { return queuePositionKey(slot, databaseID, id, uint64(i)) }
		writeKeys(t, s.kv, markers, keyAt, []byte("popped"), false)
		writeKeys(t, s.kv, markers, keyAt, nil, true)
		writeKeys(t, s.kv, liveEntries, func(i int) []byte { return keyAt(markers + i) }, []byte("live"), false)

		type timedStep struct {
			at   time.Time
			step reclaimStep
		}
		var mu sync.Mutex
		var steps []timedStep
		watchSteps(t, func(step reclaimStep) error {
			mu.Lock()
			steps = append(steps, timedStep{at: clock.Now(), step: step})
			mu.Unlock()
			return nil
		}, id)
		dropOrFail(t, s, database, "markers")
		waitReclaimed(t, s.kv, 5*time.Minute)

		mu.Lock()
		defer mu.Unlock()
		var visited int64
		for _, ts := range steps {
			visited += ts.step.Visits
			if ts.step.Visits > reclaimStepVisits+2 {
				t.Fatalf("a reclaim step visited %d stored versions; the bound is %d (plus one key's versions)", ts.step.Visits, reclaimStepVisits)
			}
		}
		if visited < markers {
			t.Fatalf("the reclaim visited %d versions over a head of %d markers: the markers were compacted away or never walked, and the test proves nothing", visited, markers)
		}
		const perSecond, window = defaultReclaimKeysPerSecond, 60 * time.Second
		// The budget's bound: the refill over the window, the bucket's credit
		// of one step, and the one step charged last (plus one key's versions).
		limit := int64(perSecond*window.Seconds()) + 2*reclaimStepVisits + 2
		start, inWindow := 0, int64(0)
		for end := range steps {
			inWindow += steps[end].step.Visits + int64(steps[end].step.Files)
			for steps[end].at.Sub(steps[start].at) >= window {
				inWindow -= steps[start].step.Visits + int64(steps[start].step.Files)
				start++
			}
			if inWindow > limit {
				t.Fatalf("%d visits plus unlinks fell inside one %s window (from %s to %s of the budget's clock); the budget allows %d",
					inWindow, window, steps[start].at.Sub(steps[0].at), steps[end].at.Sub(steps[0].at), limit)
			}
		}
		if n := liveKeysIn(t, s.kv, slot, databaseID, id); n != 0 {
			t.Fatalf("after the drain the dropped fifo still holds %d live keys", n)
		}
	})

	t.Run("keys that fill the byte bound", func(t *testing.T) {
		const keys, keyBytes = 1_000, 8 << 10
		s := newTestServer(t)
		createTestBucket(t, s, database, "wide", BucketTypeHash)
		b := lookupBucket(s, database, "wide")
		slot, databaseID, id := b.slot, b.databaseID, b.id.Load()
		writeKeys(t, s.kv, keys, func(i int) []byte {
			userKey := bytes.Repeat([]byte{'k'}, keyBytes)
			copy(userKey, fmt.Sprintf("%05d", i))
			return storage.Key(slot, databaseID, id, storage.NSDocuments, userKey)
		}, []byte("v"), false)

		bound := int(min(reclaimStepBytes, s.kv.MaxBatchBytes()/2))
		oversize := make(chan int, 1)
		watchSteps(t, func(step reclaimStep) error {
			if step.Bytes > bound {
				select {
				case oversize <- step.Bytes:
				default:
				}
				return errReclaimCrash
			}
			return nil
		}, id)
		dropOrFail(t, s, database, "wide")
		deadline := time.Now().Add(time.Minute)
		for pendingReclaimKeys(t, s.kv) != 0 {
			select {
			case held := <-oversize:
				t.Fatalf("a reclaim batch held %d bytes of keys; the bound is %d", held, bound)
			case <-time.After(10 * time.Millisecond):
			}
			if time.Now().After(deadline) {
				t.Fatal("the wide bucket was not reclaimed within a minute")
			}
		}
		if n := liveKeysIn(t, s.kv, slot, databaseID, id); n != 0 {
			t.Fatalf("after the drain the dropped bucket still holds %d live keys", n)
		}
	})
}

// TestReclaimIsFair is T-26(b): one 10⁶-key bucket and 10³ small ones
// pending, one batch per record per lap — every small record is gone before
// the large one.
func TestReclaimIsFair(t *testing.T) {
	const database, largeKeys, smallBuckets = "app", 1_000_000, 1_000
	useReclaimClock(t, newFakeReclaimClock())
	s := newTestServer(t)
	release := holdReclaimer(t)

	createTestBucket(t, s, database, "large", BucketTypeHash)
	large := lookupBucket(s, database, "large")
	writeKeys(t, s.kv, largeKeys, func(i int) []byte {
		return storage.Key(large.slot, large.databaseID, large.id.Load(), storage.NSDocuments, []byte(fmt.Sprintf("k%07d", i)))
	}, []byte("v"), false)
	smallIDs := make(map[uint64]bool, smallBuckets)
	for i := 0; i < smallBuckets; i++ {
		name := fmt.Sprintf("small%04d", i)
		createTestBucket(t, s, database, name, BucketTypeHash)
		if resp, err := s.Insert(InsertRequest{Database: database, Bucket: name, Key: "k", Data: map[string]interface{}{"n": i}}); err != nil || !resp.Success {
			t.Fatalf("insert into %s: %+v, %v", name, resp, err)
		}
		smallIDs[lookupBucket(s, database, name).id.Load()] = true
	}
	largeID := large.id.Load()
	dropOrFail(t, s, database, "large")
	for i := 0; i < smallBuckets; i++ {
		dropOrFail(t, s, database, fmt.Sprintf("small%04d", i))
	}

	var mu sync.Mutex
	var finished []uint64
	watchSteps(t, func(step reclaimStep) error {
		if step.Done {
			mu.Lock()
			finished = append(finished, step.ID)
			mu.Unlock()
		}
		return nil
	})
	release()
	waitReclaimed(t, s.kv, 10*time.Minute)

	mu.Lock()
	defer mu.Unlock()
	if len(finished) != smallBuckets+1 {
		t.Fatalf("%d records finished, want %d", len(finished), smallBuckets+1)
	}
	for position, id := range finished {
		if id == largeID && position != len(finished)-1 {
			t.Fatalf("the 10⁶-key bucket's record finished %dth of %d, before %d small ones: one record was drained ahead of the queue behind it",
				position+1, len(finished), len(finished)-1-position)
		}
	}
}

// payloadFilesOf lists the payload files under root that belong to bucket id.
func payloadFilesOf(t *testing.T, root string, id uint64) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if parsed, ok := store.ParsePayloadFileName(entry.Name()); ok && parsed.BucketID == id {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	return files
}

// lapUntilDrained runs a stopped server's reclaimer lap by lap until no
// record is pending.
func lapUntilDrained(t *testing.T, s *Server) {
	t.Helper()
	r := newReclaimer(s)
	for lap := 0; lap < 1000; lap++ {
		pending, err := r.lap(context.Background())
		if err != nil {
			t.Fatalf("lap %d: %v", lap, err)
		}
		if pending == 0 {
			return
		}
	}
	t.Fatal("the reclaimer did not drain in 1000 laps")
}

func writeLargeBlob(t *testing.T, s *Server, bucket, key string, fill byte) {
	t.Helper()
	content := bytes.Repeat([]byte{fill}, store.InlineThreshold+1)
	if resp, err := s.BlobWrite(BlobWriteRequest{Database: "app", Bucket: bucket, Key: key, Data: content}); err != nil || !resp.Success {
		t.Fatalf("blob write %s/%s: %+v, %v", bucket, key, resp, err)
	}
}

// TestReclaimBlobSurvivesPowerLoss is T-26(c): a dropped blob bucket's payload
// files are unlinked before the batch that deletes their metadata commits, and
// a power loss cut into that window — through the fault store, since a process
// kill keeps the page cache — leaves neither a leaked file nor a stuck record,
// and touches no live bucket's file. The orphan pass (Step 17) is off in
// effect: a lap runs a tick, but every file here is younger than its grace
// age, so nothing but the range reclaim can remove a leaked file for the test.
func TestReclaimBlobSurvivesPowerLoss(t *testing.T) {
	useReclaimClock(t, newFakeReclaimClock())

	// crashAtFirstUnlink stops the lap after its first unlink, before the
	// step's batch commits — cutting the power first when powerLoss is set.
	crashAtFirstUnlink := func(t *testing.T, kv *kvfault.Store, powerLoss bool) {
		var cut atomic.Bool
		hook := func(string) error {
			if cut.CompareAndSwap(false, true) {
				if powerLoss {
					kv.PowerLoss()
				}
				return errReclaimCrash
			}
			return nil
		}
		afterUnlinkHook.Store(&hook)
		t.Cleanup(func() { afterUnlinkHook.Store(nil) })
	}

	// The power loss is the clause's own crash (spec T-26(c)). The process
	// crash is what shows the order: a step that committed its deletes before
	// unlinking would leave, after a crash at its first unlink, files whose
	// metadata is already gone — which a power loss hides by discarding that
	// unsynced commit.
	for _, crash := range []struct {
		name      string
		powerLoss bool
	}{{"synced hide, power loss", true}, {"synced hide, process crash", false}} {
		t.Run(crash.name, func(t *testing.T) { reclaimBlobAcrossACrash(t, crashAtFirstUnlink, crash.powerLoss) })
	}

	// The negative proof of spec §5.2.2's durability rule: the same crash
	// over a hide committed without its sync brings the bucket back, its
	// metadata naming a file the reclaimer had already unlinked.
	t.Run("unsynced hide by hand brings the bucket back naming deleted files", func(t *testing.T) {
		dataDir := t.TempDir()
		kv := kvfault.New()
		s := newKVFaultServer(dataDir, kv)
		if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "lost", Type: BucketTypeBlob}); err != nil || !resp.Success {
			t.Fatalf("create: %+v, %v", resp, err)
		}
		writeLargeBlob(t, s, "lost", "a", 'a')
		if err := kv.Sync(); err != nil {
			t.Fatalf("sync: %v", err)
		}
		b := lookupBucket(s, "app", "lost")
		record := reclaimRecord{ID: int64(b.id.Load()), Slot: int32(b.slot), Type: BucketTypeBlob, Database: "app", Name: "lost", DroppedAt: time.Now().UTC()}
		encoded, err := bson.Marshal(record)
		if err != nil {
			t.Fatalf("encoding the record: %v", err)
		}
		hide := kv.NewBatch()
		if err := hide.Delete(catalogKey("app", "lost")); err != nil {
			t.Fatalf("hide: %v", err)
		}
		if err := hide.Set(reclaimKey(b.id.Load()), encoded); err != nil {
			t.Fatalf("hide: %v", err)
		}
		if err := hide.Commit(false); err != nil {
			t.Fatalf("hide: %v", err)
		}

		crashAtFirstUnlink(t, kv, true)
		if _, err := newReclaimer(s).lap(context.Background()); !errors.Is(err, errReclaimCrash) {
			t.Fatalf("the lap ended with %v, want the simulated crash after the first unlink", err)
		}
		afterUnlinkHook.Store(nil)

		restarted := newKVFaultServer(dataDir, kv)
		if err := restarted.LoadCatalog(); err != nil {
			t.Fatalf("LoadCatalog after the power loss: %v", err)
		}
		if _, err := restarted.resolveBucket("app", "lost"); err != nil {
			t.Fatalf("with its hide unsynced the dropped bucket should have come back, and did not: %v", err)
		}
		if read, err := restarted.BlobRead(BlobReadRequest{Database: "app", Bucket: "lost", Key: "a"}); err == nil && read.Found {
			t.Fatal("the resurrected bucket still served its blob: the unlink did not happen, so this arm shows nothing")
		}
	})
}

// reclaimBlobAcrossACrash is T-26(c)'s synced arm: drop a blob bucket, crash
// the reclaimer after its first unlink, restart, drain, and find no leaked
// file, no stuck record, and the live neighbour's blob intact.
func reclaimBlobAcrossACrash(t *testing.T, crash func(*testing.T, *kvfault.Store, bool), powerLoss bool) {
	dataDir := t.TempDir()
	kv := kvfault.New()
	s := newKVFaultServer(dataDir, kv)
	for _, bucket := range []string{"gone", "kept"} {
		if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeBlob}); err != nil || !resp.Success {
			t.Fatalf("create %s: %+v, %v", bucket, resp, err)
		}
	}
	for i, key := range []string{"a", "b", "c"} {
		writeLargeBlob(t, s, "gone", key, byte('a'+i))
	}
	writeLargeBlob(t, s, "kept", "live", 'z')
	if err := kv.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	goneID := lookupBucket(s, "app", "gone").id.Load()
	if files := payloadFilesOf(t, s.blobRoot(), goneID); len(files) != 3 {
		t.Fatalf("the dropped bucket has %d payload files before the drop, want 3", len(files))
	}
	dropOrFail(t, s, "app", "gone")

	crash(t, kv, powerLoss)
	if _, err := newReclaimer(s).lap(context.Background()); !errors.Is(err, errReclaimCrash) {
		t.Fatalf("the lap ended with %v, want the simulated crash after the first unlink", err)
	}
	afterUnlinkHook.Store(nil)

	restarted := newKVFaultServer(dataDir, kv)
	if err := restarted.LoadCatalog(); err != nil {
		t.Fatalf("LoadCatalog after the crash: %v", err)
	}
	if _, err := restarted.resolveBucket("app", "gone"); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("after the crash the dropped bucket resolves again (err %v)", err)
	}
	lapUntilDrained(t, restarted)
	if leaked := payloadFilesOf(t, restarted.blobRoot(), goneID); len(leaked) != 0 {
		t.Fatalf("after the drain the dropped bucket's files are still on disk: %v", leaked)
	}
	if n := pendingReclaimKeys(t, kv); n != 0 {
		t.Fatalf("after the drain %d reclaim record(s) are stuck", n)
	}
	read, err := restarted.BlobRead(BlobReadRequest{Database: "app", Bucket: "kept", Key: "live"})
	if err != nil || !read.Found || len(read.Data) != store.InlineThreshold+1 || read.Data[0] != 'z' {
		t.Fatalf("a live bucket's blob did not survive the reclaim of its neighbour: %+v, %v", read, err)
	}
}

// TestReclaimTouchesNoVerb is T-26(d): a dropped fifo and lifo are reclaimed
// with the store's plain Delete and never through a queue verb, and leave no
// key — the 0x05 state document included.
func TestReclaimTouchesNoVerb(t *testing.T) {
	const database = "app"
	s := newTestServer(t)
	var calls atomic.Int64
	var mu sync.Mutex
	var verbs []string
	count := func(verb string) {
		calls.Add(1)
		mu.Lock()
		verbs = append(verbs, verb)
		mu.Unlock()
	}

	// Positive control: the hook is wired to the engines.
	createTestBucket(t, s, database, "control", BucketTypeFIFO)
	if _, err := s.Push(PushRequest{Database: database, Bucket: "control", Data: map[string]interface{}{"n": 1}}); err != nil {
		t.Fatalf("push: %v", err)
	}
	store.ReclaimVerbHook.Store(&count)
	t.Cleanup(func() { store.ReclaimVerbHook.Store(nil) })
	if _, err := s.Pop(PopRequest{Database: database, Bucket: "control"}); err != nil {
		t.Fatalf("pop: %v", err)
	}
	if calls.Load() == 0 {
		t.Fatal("a live pop did not move the verb hook's counter: the hook counts nothing, so a zero below would prove nothing")
	}
	store.ReclaimVerbHook.Store(nil)

	type dropped struct {
		name       string
		slot       uint16
		databaseID uint64
		id         uint64
	}
	var queues []dropped
	for _, q := range []struct {
		name string
		kind BucketType
	}{{"jobs", BucketTypeFIFO}, {"stack", BucketTypeLIFO}} {
		createTestBucket(t, s, database, q.name, q.kind)
		for i := 0; i < 20; i++ {
			if _, err := s.Push(PushRequest{Database: database, Bucket: q.name, Data: map[string]interface{}{"n": i}}); err != nil {
				t.Fatalf("push: %v", err)
			}
		}
		for i := 0; i < 5; i++ {
			if _, err := s.Pop(PopRequest{Database: database, Bucket: q.name}); err != nil {
				t.Fatalf("pop: %v", err)
			}
		}
		b := lookupBucket(s, database, q.name)
		queues = append(queues, dropped{name: q.name, slot: b.slot, databaseID: b.databaseID, id: b.id.Load()})
	}

	calls.Store(0)
	mu.Lock()
	verbs = nil
	mu.Unlock()
	store.ReclaimVerbHook.Store(&count)
	for _, q := range queues {
		dropOrFail(t, s, database, q.name)
	}
	waitReclaimed(t, s.kv, time.Minute)
	store.ReclaimVerbHook.Store(nil)

	if n := calls.Load(); n != 0 {
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("reclaiming a dropped fifo and lifo reached %d engine verb(s): %v", n, verbs)
	}
	for _, q := range queues {
		if n := liveKeysIn(t, s.kv, q.slot, q.databaseID, q.id); n != 0 {
			t.Fatalf("after the drain %s still holds %d live keys", q.name, n)
		}
		if _, found := readStoreKey(t, s.kv, storage.Key(q.slot, q.databaseID, q.id, storage.NSState)); found {
			t.Fatalf("after the drain %s's 0x05 state document is still there", q.name)
		}
	}
}

// metricSeries is every series the default registry exports, and every label
// value on them.
func metricSeries(t *testing.T) (int, map[string]bool) {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	series := 0
	labelValues := make(map[string]bool)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			series++
			for _, label := range metric.GetLabel() {
				labelValues[label.GetValue()] = true
			}
		}
	}
	return series, labelValues
}

// gaugeValue reads an unlabelled gauge from the default registry, as a scrape
// of /metrics would.
func gaugeValue(t *testing.T, name string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() == name && len(family.GetMetric()) == 1 {
			return family.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("the registry exports no single-series gauge %s", name)
	return 0
}

// counterValue reads an unlabelled counter from the default registry.
func counterValue(t *testing.T, name string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() == name && len(family.GetMetric()) == 1 {
			return family.GetMetric()[0].GetCounter().GetValue()
		}
	}
	t.Fatalf("the registry exports no single-series counter %s", name)
	return 0
}

// TestReclaimAddsNoPerBucketSeries is T-26(e): the series count after 10 drops
// equals the count after 10³ drops, snapshotted both mid-reclaim and after the
// drain, and no label value names a dropped bucket or its id.
func TestReclaimAddsNoPerBucketSeries(t *testing.T) {
	const database = "app"
	s := newTestServer(t)

	type snapshot struct{ mid, after int }
	phase := func(prefix string, drops int) snapshot {
		// A label value the registry exported before these buckets existed —
		// an HTTP status of "200", say — is not about them, whatever their ids.
		_, before := metricSeries(t)
		var names []string
		for i := 0; i < drops; i++ {
			name := fmt.Sprintf("%s%04d", prefix, i)
			createTestBucket(t, s, database, name, BucketTypeHash)
			for k := 0; k < 3; k++ {
				if resp, err := s.Insert(InsertRequest{Database: database, Bucket: name, Key: fmt.Sprintf("k%d", k), Data: map[string]interface{}{"n": k}}); err != nil || !resp.Success {
					t.Fatalf("insert: %+v, %v", resp, err)
				}
			}
			names = append(names, name)
		}
		ids := make([]string, 0, drops)
		for _, name := range names {
			ids = append(ids, strconv.FormatUint(lookupBucket(s, database, name).id.Load(), 10))
		}

		// Mid-reclaim is the first step that finishes a record: by then every
		// record has committed its first batch in the lap before — which is
		// when a per-bucket series would exist — and none has been drained.
		mid, resume := make(chan struct{}), make(chan struct{})
		var midOnce sync.Once
		watchSteps(t, func(step reclaimStep) error {
			if step.Done {
				midOnce.Do(func() {
					close(mid)
					<-resume
				})
			}
			return nil
		})
		for _, name := range names {
			dropOrFail(t, s, database, name)
		}
		waitFor(t, mid, "the reclaimer's first finished record")
		var shot snapshot
		var labels map[string]bool
		shot.mid, labels = metricSeries(t)
		close(resume)
		waitReclaimed(t, s.kv, 2*time.Minute)
		reclaimStepHook.Store(nil)
		var after map[string]bool
		shot.after, after = metricSeries(t)
		for value := range after {
			labels[value] = true
		}
		for _, named := range append(names, ids...) {
			if labels[named] && !before[named] {
				t.Fatalf("a metric label carries the dropped bucket name or id %q", named)
			}
		}
		return shot
	}

	few := phase("few", 10)
	many := phase("many", 1_000)
	if few.mid != many.mid {
		t.Fatalf("mid-reclaim the registry exports %d series after 10 drops and %d after 10³: something adds a series per dropped bucket", few.mid, many.mid)
	}
	if few.after != many.after {
		t.Fatalf("after the drain the registry exports %d series after 10 drops and %d after 10³", few.after, many.after)
	}
}

// TestReclaimResumesFromItsCursor is T-15: after a crash mid-reclaim, the
// resume seeks to the record's persisted cursor and visits none of the markers
// already written before it; the name is never served; the record is deleted
// once the range is empty.
func TestReclaimResumesFromItsCursor(t *testing.T) {
	const database, bucket, documents = "app", "resumed", 5_000
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	createTestBucket(t, first.core, database, bucket, BucketTypeHash)
	b := lookupBucket(first.core, database, bucket)
	slot, databaseID, id := b.slot, b.databaseID, b.id.Load()
	writeKeys(t, first.core.kv, documents, func(i int) []byte {
		return storage.Key(slot, databaseID, id, storage.NSDocuments, []byte(fmt.Sprintf("k%05d", i)))
	}, []byte("v"), false)

	crashed := make(chan struct{})
	var crashOnce sync.Once
	var stepsTaken atomic.Int64
	watchSteps(t, func(reclaimStep) error {
		if stepsTaken.Add(1) > 2 {
			crashOnce.Do(func() { close(crashed) })
			return errReclaimCrash
		}
		return nil
	}, id)
	dropOrFail(t, first.core, database, bucket)
	waitFor(t, crashed, "the reclaimer to crash after two committed steps")
	reclaimStepHook.Store(nil)
	first.stop(t)

	// Held at its first lap, so the record can be read as the crash left it.
	gate := make(chan struct{})
	hold := func() { <-gate }
	reclaimLapHook.Store(&hold)
	var released sync.Once
	release := func() {
		released.Do(func() {
			reclaimLapHook.Store(nil)
			close(gate)
		})
	}
	t.Cleanup(release)
	second := restartCatalog(t, first, dataDir)
	t.Cleanup(release)

	encoded, found := readStoreKey(t, second.core.kv, reclaimKey(id))
	if !found {
		t.Fatal("after the crash the reclaim record is gone before its range was emptied")
	}
	var record reclaimRecord
	if err := bson.Unmarshal(encoded, &record); err != nil {
		t.Fatalf("decoding the record: %v", err)
	}
	if len(record.Cursor) == 0 || record.KeysDeleted == 0 {
		t.Fatalf("after two committed steps the record has cursor %x and %d keys deleted: nothing to resume from", record.Cursor, record.KeysDeleted)
	}
	if found, err := second.core.Find(FindRequest{Database: database, Bucket: bucket, Key: "k04999"}); err == nil && found.Found {
		t.Fatal("the dropped name is served after the restart")
	}

	var resumeOnce sync.Once
	var resumed reclaimStep
	resumedStep := make(chan struct{})
	watchSteps(t, func(step reclaimStep) error {
		resumeOnce.Do(func() {
			resumed = step
			close(resumedStep)
		})
		return nil
	}, id)
	release()
	waitFor(t, resumedStep, "the resumed reclaimer's first step")

	if !bytes.Equal(resumed.From, record.Cursor) {
		t.Fatalf("the resume seeked to %x; the record's cursor is %x — a resume from the range's start walks every marker it already wrote", resumed.From, record.Cursor)
	}
	if bytes.Compare(resumed.First, record.Cursor) < 0 {
		t.Fatalf("the resume's first visit was %x, before the cursor %x", resumed.First, record.Cursor)
	}
	if resumed.Visits != int64(resumed.Deletes) {
		t.Fatalf("the resume's first step visited %d versions to delete %d keys: it walked markers the crashed run had written", resumed.Visits, resumed.Deletes)
	}
	waitReclaimed(t, second.core.kv, time.Minute)
	if n := liveKeysIn(t, second.core.kv, slot, databaseID, id); n != 0 {
		t.Fatalf("after the drain the range still holds %d live keys", n)
	}
	if found, err := second.core.Find(FindRequest{Database: database, Bucket: bucket, Key: "k04999"}); err == nil && found.Found {
		t.Fatal("the dropped name is served after the drain")
	}
}

// isBucketNotFoundFrame reports whether an error frame says the bucket is not
// found — ErrBucketNotFound as a client sees it.
func isBucketNotFoundFrame(frame *transport.Frame) bool {
	return frame.Opcode == transport.OpError && bytes.Contains(frame.Payload, []byte(ErrBucketNotFound.Error()))
}

// hammerDuringDrop runs writers against bucket while drop runs, keeps them
// going for a moment after it returns, and fails on any outcome that is
// neither a success nor what refused accepts as a refusal of a bucket that is
// gone. drop must return within 5 s. A write the client never heard back
// about (an i/o timeout) is not tolerated: a write racing the drop once could
// end that way, and it fails the test like any other unexpected
// outcome rather than being counted and let through.
func hammerDuringDrop(t *testing.T, send func(key string) (*transport.Frame, error), refused func(*transport.Frame) bool, drop func() error) {
	t.Helper()
	stop := make(chan struct{})
	var writers sync.WaitGroup
	var mu sync.Mutex
	var unexpected []string
	for w := 0; w < 4; w++ {
		writers.Add(1)
		go func(w int) {
			defer writers.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				frame, err := send(fmt.Sprintf("w%d-%d", w, i))
				switch {
				case err != nil:
					mu.Lock()
					unexpected = append(unexpected, err.Error())
					mu.Unlock()
				case frame.Opcode == transport.OpError && !refused(frame):
					mu.Lock()
					unexpected = append(unexpected, string(frame.Payload))
					mu.Unlock()
				case frame.Opcode != transport.OpError:
					// A refusal can also come back as Success false in an
					// ordinary frame; that is not a success either.
					var put InsertResponse
					if err := bson.Unmarshal(frame.Payload, &put); err != nil || !put.Success {
						mu.Lock()
						unexpected = append(unexpected, fmt.Sprintf("refused in an ordinary frame: %s (%v)", put.Message, err))
						mu.Unlock()
					}
				}
				time.Sleep(time.Millisecond)
			}
		}(w)
	}
	time.Sleep(100 * time.Millisecond)
	dropped := make(chan error, 1)
	go func() { dropped <- drop() }()
	select {
	case err := <-dropped:
		if err != nil {
			close(stop)
			writers.Wait()
			t.Fatalf("the drop failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		close(stop)
		writers.Wait()
		t.Fatal("the drop did not return within 5 s with writers in flight")
	}
	time.Sleep(100 * time.Millisecond)
	close(stop)
	writers.Wait()
	mu.Lock()
	defer mu.Unlock()
	if len(unexpected) > 0 {
		t.Fatalf("%d write(s) neither succeeded nor got ErrBucketNotFound; the first: %s", len(unexpected), unexpected[0])
	}
}

// isRefusedAsGoneOnACluster accepts the two refusals a cluster gives a write
// to a dropped bucket: ErrBucketNotFound, and — from the ring owner, whose join
// fence runs before the handler — ErrJoinSyncIncomplete, which says "no write
// was accepted". The second predates this build (Cluster.fenceUnheldBucket); its
// wording after a drop is a finding in the Step 15 report, not a change here.
func isRefusedAsGoneOnACluster(frame *transport.Frame) bool {
	return isBucketNotFoundFrame(frame) ||
		(frame.Opcode == transport.OpError && bytes.Contains(frame.Payload, []byte(ErrJoinSyncIncomplete.Error())))
}

// TestDropUnderWritersLeavesNothing is T-27(a): writers hammer a bucket while
// it is dropped, on a standalone node and on a cluster. Every writer succeeds
// or gets ErrBucketNotFound, the drop returns within 5 s, and once the
// reclaimer has drained, nothing is left in the range — no write that resolved
// the old id landed behind it.
func TestDropUnderWritersLeavesNothing(t *testing.T) {
	t.Run("standalone", func(t *testing.T) {
		s := newTestServer(t)
		handler := NewHandler(s)
		handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
		handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "hot", Type: BucketTypeHash})
		b := lookupBucket(s, "app", "hot")
		slot, databaseID, id := b.slot, b.databaseID, b.id.Load()

		hammerDuringDrop(t, func(key string) (*transport.Frame, error) {
			return handler(context.Background(), transport.NewFrame(transport.OpPut,
				mustMarshalBSON(t, InsertRequest{Database: "app", Bucket: "hot", Key: key, Data: map[string]interface{}{"k": key}})))
		}, isBucketNotFoundFrame, func() error {
			frame, err := handler(context.Background(), transport.NewFrame(transport.OpDeleteBucket,
				mustMarshalBSON(t, DeleteBucketRequest{Database: "app", Name: "hot"})))
			if err == nil && frame.Opcode == transport.OpError {
				err = fmt.Errorf("refused: %s", frame.Payload)
			}
			return err
		})
		waitReclaimed(t, s.kv, time.Minute)
		if n := liveKeysIn(t, s.kv, slot, databaseID, id); n != 0 {
			t.Fatalf("after the drain the dropped bucket's range holds %d live keys: a write landed behind the reclaimer", n)
		}
	})

	t.Run("cluster", func(t *testing.T) {
		tc := newTestCluster(t, 2, 2)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
		owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
		if err != nil {
			t.Fatalf("owner: %v", err)
		}
		ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
		type placed struct {
			core       *Server
			slot       uint16
			databaseID uint64
			id         uint64
		}
		var copies []placed
		for _, n := range tc.nodes {
			waitForBucket(t, n, testDatabase, testBucket)
			b := lookupBucket(n.server.core, testDatabase, testBucket)
			copies = append(copies, placed{core: n.server.core, slot: b.slot, databaseID: b.databaseID, id: b.id.Load()})
		}

		client := tc.dial(ownerIndex)
		defer func() { _ = client.Close() }()
		var clientMu sync.Mutex

		// The race — a write that clears the owner join fence before
		// the drop and takes the apply fence after it — hit on only one run
		// in four under free-running timing (the tranche-5 T-27(a) report:
		// "writes never acknowledged across the drop: 4" on one run of
		// four). A hook forces it every run: hold the first writer right
		// after it clears the join fence, drop the bucket while it waits,
		// then release it into the apply fence behind the drop — the same
		// hold-and-release shape as SA-29's and Step 14's in-flight hooks,
		// which make their own narrow windows certain instead of hoped-for.
		held := make(chan struct{})
		releaseHeld := make(chan struct{})
		var captured atomic.Bool
		hook := func(op transport.Opcode, database, bucket string) {
			if op != transport.OpPut {
				return
			}
			if !captured.CompareAndSwap(false, true) {
				return
			}
			close(held)
			<-releaseHeld
		}
		afterJoinFenceHook.Store(&hook)
		defer afterJoinFenceHook.Store(nil)

		// The clause this build owns — nothing lands behind the reclaimer —
		// is asserted below on both nodes; hammerDuringDrop itself now
		// requires every writer to succeed or be cleanly refused, so a write
		// left unacknowledged across the drop fails the test.
		hammerDuringDrop(t, func(key string) (*transport.Frame, error) {
			clientMu.Lock()
			defer clientMu.Unlock()
			// The deadline is scoped from here, after the mutex is acquired,
			// not from when this goroutine was scheduled: a writer queued
			// behind the held one must get its own full budget once it is
			// actually its turn to call SendRequest, not whatever is left of
			// a budget that started ticking while it waited for the lock.
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			return client.SendRequest(ctx, transport.OpPut, mustMarshalBSON(t, InsertRequest{
				Database: testDatabase, Bucket: testBucket, Key: key, Data: map[string]interface{}{"k": key},
			}))
		}, isRefusedAsGoneOnACluster, func() error {
			select {
			case <-held:
			case <-time.After(5 * time.Second):
				return fmt.Errorf("no writer reached the join-fence race window within 5 s")
			}
			dropper := tc.dial(ownerIndex)
			defer func() { _ = dropper.Close() }()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			frame, err := dropper.SendRequest(ctx, transport.OpDeleteBucket, mustMarshalBSON(t, map[string]interface{}{
				"database": testDatabase, "name": testBucket, "write_concern": "one",
			}))
			close(releaseHeld)
			if err == nil && frame.Opcode == transport.OpError {
				err = fmt.Errorf("refused: %s", frame.Payload)
			}
			return err
		})
		for _, c := range copies {
			deadline := time.Now().Add(5 * time.Second)
			for c.core.bucketStoreOpen(testDatabase, testBucket) {
				if time.Now().After(deadline) {
					t.Fatal("a replica did not apply the drop within 5 s")
				}
				time.Sleep(20 * time.Millisecond)
			}
			waitReclaimed(t, c.core.kv, time.Minute)
			if n := liveKeysIn(t, c.core.kv, c.slot, c.databaseID, c.id); n != 0 {
				t.Fatalf("after the drain a node's copy of the dropped bucket holds %d live keys: a write landed behind the reclaimer", n)
			}
		}
	})
}

// TestWriteToDroppedBucketNamesTheDrop: once a bucket is dropped,
// a write to its name — still owned, per the ring, by the node that dropped
// it — must not tell the operator that rebalancing has not moved the data
// here yet. There is no rebalance in progress and no previous owner holds
// the data; the refusal must name the drop instead.
func TestWriteToDroppedBucketNamesTheDrop(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)

	dropper := tc.dial(ownerIndex)
	defer func() { _ = dropper.Close() }()
	dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dcancel()
	dframe, derr := dropper.SendRequest(dctx, transport.OpDeleteBucket, mustMarshalBSON(t, map[string]interface{}{
		"database": testDatabase, "name": testBucket, "write_concern": "one",
	}))
	if derr != nil {
		t.Fatalf("the drop did not return: %v", derr)
	}
	if dframe.Opcode == transport.OpError {
		t.Fatalf("the drop was refused: %s", string(dframe.Payload))
	}

	client := tc.dial(ownerIndex)
	defer func() { _ = client.Close() }()
	wctx, wcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer wcancel()
	frame, werr := client.SendRequest(wctx, transport.OpPut, mustMarshalBSON(t, InsertRequest{
		Database: testDatabase, Bucket: testBucket, Key: "k", Data: map[string]interface{}{"n": 1},
	}))
	if werr != nil {
		t.Fatalf("the write to the dropped bucket was left unacknowledged: %v", werr)
	}
	if frame.Opcode != transport.OpError {
		t.Fatalf("a write to a dropped bucket must be refused, got opcode %v", frame.Opcode)
	}
	message := string(frame.Payload)
	if strings.Contains(message, "rebalancing") || strings.Contains(message, "previous owner") {
		t.Fatalf("the refusal for a dropped bucket still guesses a rebalance that nothing is doing: %s", message)
	}
	if !strings.Contains(message, "dropped") {
		t.Fatalf("the refusal for a dropped bucket does not name the drop: %s", message)
	}
}

// clusterSettleTimeout bounds a wait for a change to reach every node of a test
// cluster. A fixed bound chosen for an idle machine fails when the race detector
// and other suites compete for CPU, so the bound is a minute, or less when the
// test's own deadline comes sooner.
func clusterSettleTimeout(t *testing.T) time.Duration {
	settle := time.Minute
	if deadline, ok := t.Deadline(); ok {
		if left := time.Until(deadline); left < settle {
			settle = left
		}
	}
	return settle
}

// waitForBucket waits until node serves database/bucket. Callers wait node by
// node, so the failure names the first node that never got the bucket.
func waitForBucket(t *testing.T, node *testNode, database, bucket string) {
	t.Helper()
	bound := clusterSettleTimeout(t)
	deadline := time.Now().Add(bound)
	for !node.server.core.bucketStoreOpen(database, bucket) {
		if time.Now().After(deadline) {
			t.Fatalf("%s/%s was not created on node %s within %s", database, bucket, node.addr, bound)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestIDNotReusedAfterReclaim is T-27(c): once a dropped bucket's record is
// gone — nothing left in the store names its id — a restart and a create still
// hand out a greater id.
func TestIDNotReusedAfterReclaim(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	createTestBucket(t, first.core, "app", "x", BucketTypeHash)
	droppedID := lookupBucket(first.core, "app", "x").id.Load()
	dropOrFail(t, first.core, "app", "x")
	waitReclaimed(t, first.core.kv, time.Minute)

	second := restartCatalog(t, first, dataDir)
	createTestBucket(t, second.core, "app", "y", BucketTypeHash)
	if id := lookupBucket(second.core, "app", "y").id.Load(); id <= droppedID {
		t.Fatalf("after the reclaim and a restart a new bucket got id %d; the dropped one had %d", id, droppedID)
	}
}

// forceSlot makes the next creates of names record slot instead of the one
// they hash to, until the returned function clears it.
func forceSlot(t *testing.T, slot uint16, names ...string) (clear func()) {
	t.Helper()
	forced := make(map[string]bool, len(names))
	for _, name := range names {
		forced[name] = true
		if storage.SlotOf(storage.PlacementKey("app", name, nil)) == slot {
			t.Fatalf("%q hashes to slot %d already; the forced slot must differ", name, slot)
		}
	}
	hook := func(_, bucket string) (uint16, bool) { return slot, forced[bucket] }
	slotOverrideHook.Store(&hook)
	clear = func() { slotOverrideHook.Store(nil) }
	t.Cleanup(clear)
	return clear
}

func insertDocuments(t *testing.T, s *Server, bucket string, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: bucket, Key: fmt.Sprintf("k%03d", i), Data: map[string]interface{}{"n": i}}); err != nil || !resp.Success {
			t.Fatalf("insert into %s: %+v, %v", bucket, resp, err)
		}
	}
}

// TestDropReclaimsTheSlottedRange is T-34: a bucket whose recorded slot a hook
// forced away from its name's slot is reclaimed under the recorded one. A
// reclaimer that recomputes the slot from the names walks an empty range,
// finds nothing, deletes the record, and leaves every key.
func TestDropReclaimsTheSlottedRange(t *testing.T) {
	s := newTestServer(t)
	forced := (storage.SlotOf(storage.PlacementKey("app", "forced", nil)) + 1) % storage.SlotCount
	clear := forceSlot(t, forced, "forced")
	createTestBucket(t, s, "app", "forced", BucketTypeHash)
	clear()
	b := lookupBucket(s, "app", "forced")
	if b.slot != forced {
		t.Fatalf("the bucket recorded slot %d, want the forced %d", b.slot, forced)
	}
	insertDocuments(t, s, "forced", 50)
	if n := liveKeysIn(t, s.kv, forced, b.databaseID, b.id.Load()); n == 0 {
		t.Fatal("the documents are not under the forced slot, so the test proves nothing")
	}
	dropOrFail(t, s, "app", "forced")
	waitReclaimed(t, s.kv, 30*time.Second)
	if n := liveKeysIn(t, s.kv, forced, b.databaseID, b.id.Load()); n != 0 {
		t.Fatalf("after the drain the forced slot's range still holds %d live keys: the reclaimer walked another slot", n)
	}
}

// TestDropPendingPrefixesTakesTheSlot is SA-8's test (Part A): the escape
// hatch drops each pending record's [slot][id] prefix — the dropped bucket's
// range empties, a live bucket in the same forced slot keeps every key, and no
// record is deleted.
func TestDropPendingPrefixesTakesTheSlot(t *testing.T) {
	s := newTestServer(t)
	forced := (storage.SlotOf(storage.PlacementKey("app", "dropped", nil)) + 7) % storage.SlotCount
	clear := forceSlot(t, forced, "dropped", "live")
	createTestBucket(t, s, "app", "dropped", BucketTypeHash)
	createTestBucket(t, s, "app", "live", BucketTypeHash)
	clear()
	insertDocuments(t, s, "dropped", 20)
	insertDocuments(t, s, "live", 20)
	dropped, live := lookupBucket(s, "app", "dropped"), lookupBucket(s, "app", "live")
	liveBefore := liveKeysIn(t, s.kv, forced, live.databaseID, live.id.Load())
	holdReclaimer(t)
	dropOrFail(t, s, "app", "dropped")

	targets, err := s.pendingReclaimTargets()
	if err != nil {
		t.Fatalf("listing the pending records: %v", err)
	}
	if len(targets) != 1 || targets[0] != (reclaimTarget{slot: forced, databaseID: dropped.databaseID, id: dropped.id.Load()}) {
		t.Fatalf("pending targets = %+v, want the dropped bucket's {slot %d, id %d}", targets, forced, dropped.id.Load())
	}
	if err := s.dropPendingPrefixes(targets); err != nil {
		t.Fatalf("dropPendingPrefixes: %v", err)
	}
	if n := liveKeysIn(t, s.kv, forced, dropped.databaseID, dropped.id.Load()); n != 0 {
		t.Fatalf("after the escape hatch the dropped bucket's range still holds %d live keys", n)
	}
	if n := liveKeysIn(t, s.kv, forced, live.databaseID, live.id.Load()); n != liveBefore {
		t.Fatalf("the escape hatch changed the live bucket's range from %d to %d keys", liveBefore, n)
	}
	if n := pendingReclaimKeys(t, s.kv); n != 1 {
		t.Fatalf("after the escape hatch %d reclaim record(s) remain, want the one it must leave to the reclaimer", n)
	}
}

// TestReclaimGaugesAndTheDayOldWarning: the pending and oldest-age gauges are
// set by the lap from what it left pending, and a record past a day old warns
// once an hour, naming the fix — the metrics have write sites, and the
// trigger fires.
func TestReclaimGaugesAndTheDayOldWarning(t *testing.T) {
	clock := newFakeReclaimClock()
	useReclaimClock(t, clock)
	kv := kvfault.New()
	s := newKVFaultServer(t.TempDir(), kv)
	var logged bytes.Buffer
	s.logger = slog.New(slog.NewTextHandler(&logged, nil))
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "big", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("create: %+v, %v", resp, err)
	}
	b := lookupBucket(s, "app", "big")
	writeKeys(t, kv, 4*reclaimStepVisits, func(i int) []byte {
		return storage.Key(b.slot, b.databaseID, b.id.Load(), storage.NSDocuments, []byte(fmt.Sprintf("k%05d", i)))
	}, []byte("v"), false)
	dropOrFail(t, s, "app", "big")

	r := newReclaimer(s)
	clock.advance(25 * time.Hour)
	lap := func() {
		t.Helper()
		if _, err := r.lap(context.Background()); err != nil {
			t.Fatalf("lap: %v", err)
		}
	}
	lap()
	if pending := gaugeValue(t, "hoarddb_reclaim_pending_records"); pending != 1 {
		t.Fatalf("hoarddb_reclaim_pending_records = %v after a lap that left one record, want 1", pending)
	}
	if age := gaugeValue(t, "hoarddb_reclaim_oldest_pending_seconds"); age < (25 * time.Hour).Seconds() {
		t.Fatalf("hoarddb_reclaim_oldest_pending_seconds = %v, want at least a day and an hour", age)
	}
	warnings := func() int { return strings.Count(logged.String(), "reclaim_keys_per_second") }
	if warnings() != 1 {
		t.Fatalf("a record past a day old logged %d warnings naming the fix, want 1:\n%s", warnings(), logged.String())
	}
	clock.advance(30 * time.Minute)
	lap()
	if warnings() != 1 {
		t.Fatalf("a second lap within the hour warned again: %d warnings", warnings())
	}
	clock.advance(time.Hour)
	lap()
	if warnings() != 2 {
		t.Fatalf("a lap an hour later did not warn again: %d warnings", warnings())
	}
	lapUntilDrained(t, s)
	if pending := gaugeValue(t, "hoarddb_reclaim_pending_records"); pending != 0 {
		t.Fatalf("hoarddb_reclaim_pending_records = %v after the drain, want 0", pending)
	}
	if age := gaugeValue(t, "hoarddb_reclaim_oldest_pending_seconds"); age != 0 {
		t.Fatalf("hoarddb_reclaim_oldest_pending_seconds = %v after the drain, want 0", age)
	}
}

// percentile returns the p-th percentile of durations (0 < p ≤ 1).
func percentile(durations []time.Duration, p float64) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), durations...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[min(len(sorted)-1, int(float64(len(sorted))*p))]
}

// pacedWriter writes to live buckets at a fixed rate until stop closes, and
// reports each write's latency and every failure.
func pacedWriter(t *testing.T, handler transport.RequestHandler, buckets []string, perSecond int, stop <-chan struct{}) (latencies []time.Duration, failures []string) {
	t.Helper()
	ticker := time.NewTicker(time.Second / time.Duration(perSecond))
	defer ticker.Stop()
	for i := 0; ; i++ {
		select {
		case <-stop:
			return latencies, failures
		case <-ticker.C:
		}
		began := time.Now()
		frame, err := handler(context.Background(), transport.NewFrame(transport.OpUpsert, mustMarshalBSON(t, InsertRequest{
			Database: "live", Bucket: buckets[i%len(buckets)], Key: fmt.Sprintf("w%07d", i), Data: map[string]interface{}{"n": i},
		})))
		latencies = append(latencies, time.Since(began))
		// A mutating handler reports an ordinary failure — a store that
		// refuses the write, say — as Success false in a normal frame, not
		// as an error frame, so both are failures here.
		var put InsertResponse
		switch {
		case err != nil:
			failures = append(failures, err.Error())
		case frame.Opcode == transport.OpError:
			failures = append(failures, string(frame.Payload))
		case bson.Unmarshal(frame.Payload, &put) != nil || !put.Success:
			failures = append(failures, fmt.Sprintf("refused: %s", put.Message))
		}
	}
}

// TestDropUnderLoad is T-05, the acceptance test of spec §5.2: a writer at a
// fixed rate on live buckets while 10⁴ small buckets are dropped at the
// default budget. (a) The writer's error rate is exactly zero. (b) Its p99
// against the same writer without drops is recorded, not asserted, until the
// §2.7 record fixes the multiple (build doc SA-7). (c) Every range is empty and
// every record gone within the budget's predicted drain time plus 50%. (d) A
// drop's latency does not grow with the bucket's entry count. Exempt from the
// pre-build clause (spec §13 item 2); long, so skipped under -short.
func TestDropUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("T-05 drops 10⁴ buckets under load; run it explicitly")
	}
	const (
		dropped, entries = 10_000, 10
		writesPerSecond  = 200
		baselineFor      = 5 * time.Second
	)
	s := newTestServer(t)
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "live"})
	liveBuckets := []string{"l0", "l1", "l2", "l3"}
	for _, name := range liveBuckets {
		handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "live", Name: name, Type: BucketTypeHash})
	}

	stop := make(chan struct{})
	time.AfterFunc(baselineFor, func() { close(stop) })
	baseline, baselineFailures := pacedWriter(t, handler, liveBuckets, writesPerSecond, stop)
	if len(baselineFailures) > 0 {
		t.Fatalf("the writer failed %d times with no drop running: %s", len(baselineFailures), baselineFailures[0])
	}

	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "doomed"})
	type placed struct {
		slot       uint16
		databaseID uint64
		id         uint64
	}
	var ranges []placed
	docs := make([]map[string]interface{}, entries)
	for i := 0; i < dropped; i++ {
		name := fmt.Sprintf("d%05d", i)
		handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "doomed", Name: name, Type: BucketTypeHash})
		for k := range docs {
			docs[k] = map[string]interface{}{"_id_": fmt.Sprintf("k%02d", k)}
		}
		handlerSend(t, handler, transport.OpBatchPut, BatchInsertRequest{Database: "doomed", Bucket: name, Data: docs})
		b := lookupBucket(s, "doomed", name)
		ranges = append(ranges, placed{slot: b.slot, databaseID: b.databaseID, id: b.id.Load()})
	}

	stop = make(chan struct{})
	written := make(chan struct{})
	var loaded []time.Duration
	var loadedFailures []string
	go func() {
		defer close(written)
		loaded, loadedFailures = pacedWriter(t, handler, liveBuckets, writesPerSecond, stop)
	}()
	visitsBefore := counterValue(t, "hoarddb_reclaim_keys_visited_total")
	dropsBegan := time.Now()
	for i := 0; i < dropped; i++ {
		handlerSend(t, handler, transport.OpDeleteBucket, DeleteBucketRequest{Database: "doomed", Name: fmt.Sprintf("d%05d", i)})
	}
	dropsTook := time.Since(dropsBegan)
	// Prediction: each record costs its entries, its state document, one
	// visit to find it and one final empty step, at the default budget.
	predicted := time.Duration(float64(dropped*(entries+3)) / defaultReclaimKeysPerSecond * float64(time.Second))
	allowed := time.Duration(1.5 * float64(max(predicted, dropsTook)))
	waitReclaimed(t, s.kv, allowed+time.Minute)
	drained := time.Since(dropsBegan)
	close(stop)
	<-written

	if len(loadedFailures) > 0 {
		t.Fatalf("(a) the writer failed %d of %d writes while buckets were dropped; the first: %s", len(loadedFailures), len(loaded), loadedFailures[0])
	}
	t.Logf("(b) writer p99 at %d writes/s: %s without drops, %s during %d drops and their reclaim at %d visits/s (%.2fx); recorded, not asserted (SA-7)",
		writesPerSecond, percentile(baseline, 0.99), percentile(loaded, 0.99), dropped, defaultReclaimKeysPerSecond,
		float64(percentile(loaded, 0.99))/float64(percentile(baseline, 0.99)))
	t.Logf("(c) drops took %s; drained %s after the first drop; the budget predicts %s; allowed %s; the reclaimer charged %.0f visits",
		dropsTook, drained, predicted, allowed, counterValue(t, "hoarddb_reclaim_keys_visited_total")-visitsBefore)
	if drained > allowed {
		t.Fatalf("(c) the reclaim drained %s after the first drop; allowed %s (1.5 × max(prediction %s, drop phase %s))", drained, allowed, predicted, dropsTook)
	}
	for _, r := range ranges {
		if n := liveKeysIn(t, s.kv, r.slot, r.databaseID, r.id); n != 0 {
			t.Fatalf("(c) after the drain bucket %d still holds %d live keys", r.id, n)
		}
	}

	// (d): the drop is two keys and one fsync whatever the bucket holds.
	dropLatency := func(prefix string, keys int) time.Duration {
		var took []time.Duration
		for i := 0; i < 5; i++ {
			name := fmt.Sprintf("%s%d", prefix, i)
			handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "doomed", Name: name, Type: BucketTypeHash})
			b := lookupBucket(s, "doomed", name)
			writeKeys(t, s.kv, keys, func(k int) []byte {
				return storage.Key(b.slot, b.databaseID, b.id.Load(), storage.NSDocuments, []byte(fmt.Sprintf("k%06d", k)))
			}, []byte("v"), false)
			began := time.Now()
			handlerSend(t, handler, transport.OpDeleteBucket, DeleteBucketRequest{Database: "doomed", Name: name})
			took = append(took, time.Since(began))
		}
		return percentile(took, 0.5)
	}
	small, large := dropLatency("ten", 10), dropLatency("tenk", 10_000)
	t.Logf("(d) median drop latency: %s with 10 entries, %s with 10⁴", small, large)
	if large > 2*small+20*time.Millisecond {
		t.Fatalf("(d) a drop of a 10⁴-entry bucket took %s against %s for 10 entries: the drop grows with the bucket", large, small)
	}
}
