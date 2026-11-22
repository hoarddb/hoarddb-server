package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// The swap's gates (catalog-gc spec §8, build doc CG-13): an idle bucket is a
// record in a pointer-free chunk, so a catalog of them adds almost nothing
// the collector traces. Both are measured over the production create path,
// because a fixture that built records directly would measure the fixture.

// idleBucketGateFirst and idleBucketGateTotal are the spec's 1,000 then 21,000:
// the first thousand absorb what a database's first buckets allocate once
// (its registry, its first chunks), and the next 20,000 are what is divided.
const (
	idleBucketGateFirst = 1_000
	idleBucketGateTotal = 21_000
)

// idleBucketShapes are the bucket kinds the gates hold for: the spec's empty
// LIFO, and a hash bucket that declares typed fields and nothing else, which
// shares its declaration with every bucket that declares the same fields.
var idleBucketShapes = []catalogGCShape{catalogGCShapes[0], catalogGCShapes[len(catalogGCShapes)-1]}

// idleBucketDelta creates idleBucketGateTotal buckets of shape in one
// database and returns the traced heap objects and the scannable heap bytes
// that the last 20,000 added, per bucket, with two collections before each
// read so that what is read is what stays live.
func idleBucketDelta(t *testing.T, shape catalogGCShape) (objectsPerBucket, scannablePerBucket float64) {
	t.Helper()
	s := newTestServer(t)
	if created, err := s.CreateDatabase(catalogGCDatabase); err != nil || !created.Success {
		t.Fatalf("creating database %q: %+v, %v", catalogGCDatabase, created, err)
	}
	create := func(from, to int) {
		for i := from; i < to; i++ {
			created, err := s.CreateBucket(shape.request(catalogGCDatabase, catalogGCBucketName(i)))
			if err != nil || !created.Success {
				t.Fatalf("creating %s bucket %d: %+v, %v", shape.name, i, created, err)
			}
		}
	}
	create(0, idleBucketGateFirst)
	settleCollector()
	objectsBefore, scannableBefore := readCollectorCounts()
	create(idleBucketGateFirst, idleBucketGateTotal)
	settleCollector()
	objectsAfter, scannableAfter := readCollectorCounts()
	measured := float64(idleBucketGateTotal - idleBucketGateFirst)
	objectsPerBucket = (float64(objectsAfter) - float64(objectsBefore)) / measured
	scannablePerBucket = (float64(scannableAfter) - float64(scannableBefore)) / measured
	t.Logf("%s: %.4f traced objects and %.2f scannable bytes per idle bucket (load average %s)",
		shape.name, objectsPerBucket, scannablePerBucket, hostLoad())
	return objectsPerBucket, scannablePerBucket
}

// TestIdleBucketsAddNoGCTracedObjects is the bug's criterion: an idle bucket
// adds under a tenth of a heap object. Before the swap each added three (the
// struct, its engine, its name) and a typed-only bucket five.
func TestIdleBucketsAddNoGCTracedObjects(t *testing.T) {
	if testing.Short() {
		t.Skip("creates 21,000 buckets per shape; skipped under -short")
	}
	for _, shape := range idleBucketShapes {
		t.Run(shape.name, func(t *testing.T) {
			if objects, _ := idleBucketDelta(t, shape); objects >= 0.1 {
				t.Errorf("an idle %s bucket adds %.4f heap objects the collector traces; the limit is 0.1", shape.name, objects)
			}
		})
	}
}

// TestIdleBucketsAddNoScannableHeap catches what the object count cannot: a
// design that keeps a few large pointer-bearing objects passes the count and
// still makes the collector read every bucket's bytes.
func TestIdleBucketsAddNoScannableHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("creates 21,000 buckets per shape; skipped under -short")
	}
	for _, shape := range idleBucketShapes {
		t.Run(shape.name, func(t *testing.T) {
			if _, scannable := idleBucketDelta(t, shape); scannable >= 16 {
				t.Errorf("an idle %s bucket adds %.2f bytes of heap the collector scans; the limit is 16", shape.name, scannable)
			}
		})
	}
}

// TestTypedOnlyBucketsShareOneDeclaration is the 2026-10-05 addition to
// CG-13: buckets that declare the same typed fields and nothing else hold no
// declaration table entry of their own; they share one declaration, and they
// still enforce it.
func TestTypedOnlyBucketsShareOneDeclaration(t *testing.T) {
	s := newTestServer(t)
	typed := idleBucketShapes[1]
	const count = 50
	if built := buildCatalogGCFixture(t, s, typed, count); built != count {
		t.Fatalf("built %d buckets, want %d", built, count)
	}
	if entries := s.decls.count(); entries != 0 {
		t.Errorf("the declaration table holds %d entries for %d typed-only buckets; want 0, a typed-only bucket costs the id of a shared declaration", entries, count)
	}
	for _, i := range []int{0, count - 1} {
		name := catalogGCBucketName(i)
		resp, err := s.Insert(InsertRequest{Database: catalogGCDatabase, Bucket: name, Key: "k", Data: map[string]interface{}{"Name": 7}})
		if err == nil && resp.Success {
			t.Errorf("%s took a document whose Name is a number; its declaration says string", name)
		}
		resp, err = s.Insert(InsertRequest{Database: catalogGCDatabase, Bucket: name, Key: "k", Data: map[string]interface{}{"Name": "Ada", "Age": int32(36)}})
		if err != nil || !resp.Success {
			t.Errorf("%s refused a document its declaration allows: %+v, %v", name, resp, err)
		}
	}
	stats, err := s.BucketStats(BucketStatsRequest{Database: catalogGCDatabase, Bucket: catalogGCBucketName(1)})
	if err != nil || !stats.Found || len(stats.Fields) != 2 {
		t.Fatalf("describing %s: %+v, %v; want its two typed fields", catalogGCBucketName(1), stats, err)
	}
	if got := fmt.Sprint(stats.Fields[0].Name, " ", stats.Fields[0].Type, " ", stats.Fields[0].Required); got != "Name string true" {
		t.Errorf("describe shows the first typed field as %q, want %q", got, "Name string true")
	}
}

// TestOneMutexPerBucket (spec §5.1 item 1): every writer of a bucket meets on
// the one mutex its record owns, while other names are created and dropped
// so that slots are recycled around it. Each insert rewrites the bucket's
// state document (read, add its bytes, write), so a second mutex for the
// bucket would lose some of those additions. Run it under -race.
func TestOneMutexPerBucket(t *testing.T) {
	const writers, writesEach = 8, 60
	s := newTestServer(t)
	createTestBucket(t, s, "app", "counted", BucketTypeHash)
	createTestBucket(t, s, "app", "control", BucketTypeHash)
	document := map[string]interface{}{"v": int32(1)}
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "control", Key: "w0-k0000", Data: document}); err != nil || !resp.Success {
		t.Fatalf("control insert: %+v, %v", resp, err)
	}
	control, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: "control"})
	if err != nil {
		t.Fatalf("control stats: %v", err)
	}
	perWrite := control.LogicalBytes

	stop := make(chan struct{})
	var churn sync.WaitGroup
	churn.Add(1)
	go func() {
		defer churn.Done()
		for round := 0; ; round++ {
			select {
			case <-stop:
				return
			default:
			}
			name := fmt.Sprintf("churn-%d", round%4)
			if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeLIFO}); err != nil || !resp.Success {
				t.Errorf("churn create %s: %+v, %v", name, resp, err)
				return
			}
			if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: name}); err != nil || !resp.Success {
				t.Errorf("churn drop %s: %+v, %v", name, resp, err)
				return
			}
		}
	}()
	var writing sync.WaitGroup
	for writer := range writers {
		writing.Add(1)
		go func() {
			defer writing.Done()
			for write := range writesEach {
				key := fmt.Sprintf("w%d-k%04d", writer, write)
				if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "counted", Key: key, Data: document}); err != nil || !resp.Success {
					t.Errorf("insert %s: %+v, %v", key, resp, err)
					return
				}
			}
		}()
	}
	writing.Wait()
	close(stop)
	churn.Wait()

	counted, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: "counted"})
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if want := perWrite * writers * writesEach; counted.LogicalBytes != want || counted.Entries != writers*writesEach {
		t.Fatalf("after %d writes the bucket records %d logical bytes and %d entries, want %d and %d: a write's state update was lost, so two writers held different mutexes",
			writers*writesEach, counted.LogicalBytes, counted.Entries, want, writers*writesEach)
	}
}

// TestStaleHandleAfterSlotReuse (spec §5.3 "stale holders"): a writer that
// resolved a bucket, lost the race to its drop, and reaches the record after
// a create of another name was given the same slot, answers not found and
// writes nothing — not into the new bucket, whose record is now open with an
// id, which closed alone cannot tell.
func TestStaleHandleAfterSlotReuse(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "victim", BucketTypeHash)
	victim := lookupBucket(s, "app", "victim").rec

	resolved, release := make(chan struct{}), make(chan struct{})
	var resolvedOnce sync.Once
	hold := func(_, name string) {
		if name != "victim" {
			return
		}
		resolvedOnce.Do(func() { close(resolved) })
		<-release
	}
	afterResolveHook.Store(&hold)
	t.Cleanup(func() { afterResolveHook.Store(nil) })
	writeErr := make(chan error, 1)
	go func() {
		_, err := s.InsertCtx(context.Background(), InsertRequest{Database: "app", Bucket: "victim", Key: "late", Data: map[string]interface{}{"v": 1}})
		writeErr <- err
	}()
	waitFor(t, resolved, "the writer to resolve the bucket")
	afterResolveHook.Store(nil)

	if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "victim"}); err != nil || !resp.Success {
		t.Fatalf("drop: %+v, %v", resp, err)
	}
	createTestBucket(t, s, "app", "heir", BucketTypeHash)
	heir := lookupBucket(s, "app", "heir")
	if heir.rec != victim {
		t.Fatal("the create of heir was not given victim's freed slot; the test needs the slot reused")
	}
	close(release)
	if err := <-writeErr; !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("a writer that resolved victim before its drop wrote after its slot was reused and returned %v, want ErrBucketNotFound", err)
	}
	if count, err := s.Count(CountRequest{Database: "app", Bucket: "heir"}); err != nil || count.Count != 0 {
		t.Fatalf("heir, which nobody wrote to, holds %+v documents (%v); the stale writer wrote into it", count, err)
	}
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "heir", Key: "k", Data: map[string]interface{}{"v": 1}}); err != nil || !resp.Success {
		t.Fatalf("heir does not take a write of its own: %+v, %v", resp, err)
	}
}

// TestDropClosesTheRecordItResolved (spec §5.3, N-2): a drop holds its slot
// from the unlink to the close, so a create that arrives between the two is
// given another slot, and the close lands on the dropped bucket, never on
// the new one.
func TestDropClosesTheRecordItResolved(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "leaving", BucketTypeHash)
	leaving := lookupBucket(s, "app", "leaving").rec

	unlinked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	park := func() {
		once.Do(func() {
			close(unlinked)
			<-release
		})
	}
	betweenUnlinkAndCloseHook.Store(&park)
	t.Cleanup(func() { betweenUnlinkAndCloseHook.Store(nil) })
	dropped := make(chan error, 1)
	go func() {
		resp, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "leaving"})
		if err == nil && !resp.Success {
			err = fmt.Errorf("drop refused: %s", resp.Message)
		}
		dropped <- err
	}()
	waitFor(t, unlinked, "the drop to unlink its record")
	createTestBucket(t, s, "app", "arriving", BucketTypeHash)
	arriving := lookupBucket(s, "app", "arriving").rec
	if arriving == leaving {
		t.Error("a create between a drop's unlink and its close was given the slot the drop still holds")
	}
	close(release)
	if err := <-dropped; err != nil {
		t.Fatalf("drop: %v", err)
	}
	leaving.mu.RLock()
	closedLeaving := leaving.closed
	leaving.mu.RUnlock()
	if !closedLeaving && leaving != arriving {
		t.Error("the drop did not close the record it unlinked")
	}
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "arriving", Key: "k", Data: map[string]interface{}{"v": 1}}); err != nil || !resp.Success {
		t.Fatalf("arriving was closed by a drop of another bucket: %+v, %v", resp, err)
	}
}

// TestReaderCopiesUnderTheHold (spec N-7): a reader copies what it builds its
// view from inside the hold it took its snapshot in. Parked after that hold,
// across a drop of the bucket and a create of the same name that reuses the
// slot, it reads the old bucket as of its snapshot — never the new bucket's id
// over the old snapshot, which would answer an empty bucket.
func TestReaderCopiesUnderTheHold(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "shelf", BucketTypeHash)
	mustInsert(t, s, "app", "shelf", "book", map[string]interface{}{"v": "old"})
	old := lookupBucket(s, "app", "shelf")

	parked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	park := func() {
		once.Do(func() {
			close(parked)
			<-release
		})
	}
	afterReadResolvedHook.Store(&park)
	t.Cleanup(func() { afterReadResolvedHook.Store(nil) })
	type readResult struct {
		id    uint64
		found bool
		err   error
	}
	read := make(chan readResult, 1)
	go func() {
		view, snap, err := s.resolveForRead("app", "shelf")
		if err != nil {
			read <- readResult{err: err}
			return
		}
		defer snap.Discard()
		encoded, getErr := view.hashStore.Get("book")
		read <- readResult{id: view.id.Load(), found: encoded != nil, err: getErr}
	}()
	waitFor(t, parked, "the reader to release its hold")
	afterReadResolvedHook.Store(nil)
	if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "shelf"}); err != nil || !resp.Success {
		t.Fatalf("drop: %+v, %v", resp, err)
	}
	createTestBucket(t, s, "app", "shelf", BucketTypeHash)
	if lookupBucket(s, "app", "shelf").rec != old.rec {
		t.Fatal("the re-create was not given the dropped bucket's slot; the test needs the slot reused")
	}
	close(release)
	got := <-read
	if got.err != nil {
		if !errors.Is(got.err, ErrBucketNotFound) {
			t.Fatalf("the parked reader returned %v, want the old bucket or not found", got.err)
		}
		return
	}
	if got.id != old.id.Load() || !got.found {
		t.Fatalf("the parked reader read id %d (found the old document: %v), want the old bucket %d as of its snapshot",
			got.id, got.found, old.id.Load())
	}
}

// TestUncommittedClaimIsFoundByName (spec §5.1 item 4): a record claimed and
// not yet committed is in the table from its claim. bucketStoreOpen waits out
// its attach, a peek reports it held and not served, and a drop that names
// no generation leaves a create's record for the create.
func TestUncommittedClaimIsFoundByName(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "settled", BucketTypeHash)
	db := s.serverDataAccessor().getDB("app")
	pending := claimForTest(t, db, "pending", BucketTypeHash)

	if facts, held := s.peekBucket("app", "pending"); !held || facts.id != 0 {
		t.Fatalf("a peek of a claimed record = %+v, held %v; want held with id 0", facts, held)
	}
	if s.bucketServed("app", "pending") {
		t.Fatal("a claimed, uncommitted record is reported served")
	}
	pending.mu.Lock()
	opened := make(chan bool, 1)
	go func() { opened <- s.bucketStoreOpen("app", "pending") }()
	select {
	case early := <-opened:
		pending.mu.Unlock()
		t.Fatalf("bucketStoreOpen answered %v without waiting for the claim's attach", early)
	case <-time.After(50 * time.Millisecond):
	}
	pending.mu.Unlock()
	if <-opened {
		t.Fatal("bucketStoreOpen reports a record no create committed as open")
	}

	if removed := s.detachDroppedGenerations("app", nil, false); removed != 1 {
		t.Fatalf("a drop naming no generation removed %d buckets, want 1 (settled, not the create's pending record)", removed)
	}
	if _, held := s.peekBucket("app", "pending"); !held {
		t.Fatal("a drop naming no generation took a create's uncommitted record")
	}
}

// TestDropOfPlainAndFlaggedKeepsGauges (spec §5.1 item 6, §5.8): dropping a
// plain, an indexed and a typed-only bucket brings hoarddb_node_buckets,
// hoarddb_indexes and the interned typed-field set back to where they were. A
// create refused after its registry claim takes the claim back (BUG-0257), so
// it leaves no ghost record: it moves no gauge and there is nothing to drop.
func TestDropOfPlainAndFlaggedKeepsGauges(t *testing.T) {
	s := newTestServer(t)
	ready := func() float64 { return gatheredGauge(t, "hoarddb_indexes", "state", "ready") }
	attachedBefore, readyBefore, typesBefore := s.bucketsAttached.Load(), ready(), s.fieldTypes.count()

	createTestBucket(t, s, "app", "plain", BucketTypeLIFO)
	createTestBucketWithOptions(t, s, "app", "indexed", BucketTypeHash, map[string]interface{}{"indices": []interface{}{"Email"}})
	if resp, err := s.CreateBucket(idleBucketShapes[1].request("app", "typed")); err != nil || !resp.Success {
		t.Fatalf("creating typed: %+v, %v", resp, err)
	}
	refused, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "refused", Type: BucketTypeHash,
		Options: map[string]interface{}{"indices": []interface{}{map[string]interface{}{"field": 7}}}})
	if err == nil && refused.Success {
		t.Fatal("a create with an index on a number was taken; the test needs it refused")
	}
	if left := lookupBucket(s, "app", "refused"); left != nil {
		t.Fatalf("the refused create left a registry entry under %q (BUG-0257)", "refused")
	}
	if attached := s.bucketsAttached.Load() - attachedBefore; attached != 3 {
		t.Fatalf("three created buckets moved hoarddb_node_buckets by %d, want 3", attached)
	}
	if moved := ready() - readyBefore; moved != 1 {
		t.Fatalf("one ready index moved hoarddb_indexes by %v, want 1", moved)
	}
	if interned := s.fieldTypes.count() - typesBefore; interned != 1 {
		t.Fatalf("one typed declaration added %d to the interned set, want 1", interned)
	}

	for _, name := range []string{"plain", "indexed", "typed"} {
		if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: name}); err != nil || !resp.Success {
			t.Fatalf("dropping %s: %+v, %v", name, resp, err)
		}
	}
	if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "refused"}); err == nil && resp.Success {
		t.Error("dropping the refused create's name succeeded; no bucket should exist under it")
	}
	if attached := s.bucketsAttached.Load(); attached != attachedBefore {
		t.Errorf("after the drops hoarddb_node_buckets is %d, want %d", attached, attachedBefore)
	}
	if moved := ready() - readyBefore; moved != 0 {
		t.Errorf("after the drops hoarddb_indexes is %v away from where it started", moved)
	}
	if interned := s.fieldTypes.count(); interned != typesBefore {
		t.Errorf("after the drops the interned set holds %d declarations, want %d", interned, typesBefore)
	}
	if entries := s.decls.count(); entries != 0 {
		t.Errorf("after the drops the declaration table holds %d entries, want 0", entries)
	}
}

// catalogReadCountingKV is the fault store with its snapshots and its reads
// of catalog keys counted.
type catalogReadCountingKV struct {
	*kvfault.Store
	snapshots   atomic.Int64
	catalogGets atomic.Int64
}

func (kv *catalogReadCountingKV) NewSnapshot() storage.KVSnapshot {
	kv.snapshots.Add(1)
	return catalogCountingSnapshot{KVSnapshot: kv.Store.NewSnapshot(), counter: &kv.catalogGets}
}

type catalogCountingSnapshot struct {
	storage.KVSnapshot
	counter *atomic.Int64
}

func (snap catalogCountingSnapshot) Get(key []byte) ([]byte, error) {
	if lower, upper := catalogPrefix(); bytes.Compare(key, lower) >= 0 && bytes.Compare(key, upper) < 0 {
		snap.counter.Add(1)
	}
	return snap.KVSnapshot.Get(key)
}

// TestLookupOfAnIdleBucketReadsNoStore (owner ruling 41.2: no cold catalog):
// after a restart, finding an idle bucket reads nothing from the store. A
// writer's resolve and a peek open no snapshot and read no catalog key, and a
// read opens exactly the one snapshot it reads from.
func TestLookupOfAnIdleBucketReadsNoStore(t *testing.T) {
	dataDir := t.TempDir()
	kv := &catalogReadCountingKV{Store: kvfault.New()}
	first := newKVFaultServer(dataDir, kv)
	if resp, err := first.CreateBucket(CreateBucketRequest{Database: "app", Name: "idle", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("create: %+v, %v", resp, err)
	}
	restarted := newKVFaultServer(dataDir, kv)
	if err := restarted.LoadCatalog(); err != nil {
		t.Fatalf("LoadCatalog: %v", err)
	}

	snapshots, catalogGets := kv.snapshots.Load(), kv.catalogGets.Load()
	_, unlock, err := restarted.lockBucket("app", "idle")
	if err != nil {
		t.Fatalf("lockBucket: %v", err)
	}
	unlock()
	if _, held := restarted.peekBucket("app", "idle"); !held {
		t.Fatal("a peek did not find the bucket")
	}
	if got := kv.snapshots.Load() - snapshots; got != 0 {
		t.Errorf("a writer's resolve and a peek of an idle bucket opened %d snapshots, want 0", got)
	}
	view, snap, err := restarted.resolveForRead("app", "idle")
	if err != nil || view == nil {
		t.Fatalf("resolveForRead: %v", err)
	}
	snap.Discard()
	if got := kv.snapshots.Load() - snapshots; got != 1 {
		t.Errorf("the lookups and a read opened %d snapshots, want 1 (the read's own)", got)
	}
	if got := kv.catalogGets.Load() - catalogGets; got != 0 {
		t.Errorf("finding an idle bucket read %d catalog keys from the store, want 0: the catalog is resident", got)
	}
}
