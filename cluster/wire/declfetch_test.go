package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"reflect"
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

const u38DBName = "app"

// u38Reader is the keyed read: it serves declarations from a map and counts the round trips.
type u38Reader struct {
	mu       sync.Mutex
	docs     map[string]map[uint16][]byte // by bucket name
	names    map[uint64]string            // reverse index
	reads    atomic.Int64
	owners   []cluster.Member
	slots    []uint16
	err      error
	entered  chan struct{} // receives once per ReadDeclaration when non-nil
	release  chan struct{} // ReadDeclaration blocks on it when non-nil
	onRead   func()
	nameRead atomic.Int64
}

func (r *u38Reader) ReadDeclaration(ctx context.Context, owner cluster.Member, read DeclarationRead) (map[uint16][]byte, bool, error) {
	r.reads.Add(1)
	r.mu.Lock()
	r.owners = append(r.owners, owner)
	r.slots = append(r.slots, read.Slot)
	r.mu.Unlock()
	if r.entered != nil {
		r.entered <- struct{}{}
	}
	if r.release != nil {
		select {
		case <-r.release:
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
	if r.onRead != nil {
		r.onRead()
	}
	if r.err != nil {
		return nil, false, r.err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, doc := range r.docs {
		enc, _ := store.EncodeComponent(name)
		if string(enc) == string(read.EncodedID) {
			return doc, true, nil
		}
	}
	return nil, false, nil
}

func (r *u38Reader) ReadBucketName(ctx context.Context, owner cluster.Member, read DeclarationRead) (string, bool, error) {
	r.nameRead.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, name := range r.names {
		var raw [8]byte
		binary.BigEndian.PutUint64(raw[:], id)
		enc, _ := store.EncodeComponent(raw[:])
		if string(enc) == string(read.EncodedID) {
			return name, true, nil
		}
	}
	return "", false, nil
}

func u38Decl(bucketID uint64, version int64, state string) machine.Declaration {
	return machine.Declaration{
		BucketID: bucketID, Version: version, State: state,
		Fields: machine.FieldTable{NextFieldID: 3, Entries: []machine.FieldEntry{
			{ID: 1, Type: "any", Name: "_id_"}, {ID: 2, Type: "string", Name: "title"},
		}},
	}
}

func (r *u38Reader) put(t *testing.T, name string, decl machine.Declaration) {
	t.Helper()
	doc, err := store.EncodeDeclaration(decl)
	if err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.docs == nil {
		r.docs = map[string]map[uint16][]byte{}
		r.names = map[uint64]string{}
	}
	r.docs[name] = doc
	r.names[decl.BucketID] = name
}

type u38Rig struct {
	fetcher   *Fetcher
	reader    *u38Reader
	published atomic.Int64
	topology  *fakeTopology
}

func u38NewRig(t *testing.T, cfg machine.CacheConfig) *u38Rig {
	t.Helper()
	table := cluster.BuildTable(cluster.ClusterState{Members: []cluster.Member{{ID: "n1", State: cluster.MemberActive}}})
	rig := &u38Rig{
		reader:   &u38Reader{},
		topology: &fakeTopology{self: "n1", members: 1, table: table, dbRow: DatabaseInfo{RF: 1}},
	}
	rig.published.Store(1)
	if cfg.MaxEntries == 0 {
		cfg = machine.CacheConfig{MaxEntries: 4096, MaxBytes: 32 << 20, MaxDeclBytes: 4 << 20}
	}
	rig.fetcher = NewFetcher(FetcherConfig{
		Topology:     rig.topology,
		Reader:       rig.reader,
		DatabaseName: func(db uint64) (string, bool) { return u38DBName, db == testDB },
		Published:    func(uint64) int64 { return rig.published.Load() },
		Cache:        cfg,
	})
	return rig
}

func TestCacheHitCostsNoRoundTrip(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{})
	rig.reader.put(t, "users", u38Decl(10, 1, machine.StateActive))
	for i := 0; i < 100; i++ {
		entry, err := rig.fetcher.Lookup(context.Background(), testDB, "users", 1)
		if err != nil {
			t.Fatal(err)
		}
		if entry.Decl.BucketID != 10 || entry.Seen != 1 {
			t.Fatalf("entry = %+v", entry)
		}
	}
	if got := rig.reader.reads.Load(); got != 1 {
		t.Fatalf("100 lookups made %d reads, want 1", got)
	}
	if stats := rig.fetcher.Stats(); stats.Hits != 99 || stats.Fetches != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestMissRoutesToTheDeclarationSlotOwner(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{})
	rig.reader.put(t, "users", u38Decl(10, 1, machine.StateActive))
	if _, err := rig.fetcher.Lookup(context.Background(), testDB, "users", 1); err != nil {
		t.Fatal(err)
	}
	enc, _ := store.EncodeComponent("users")
	wantSlot := storage.SlotOf(storage.PlacementKey(u38DBName, "_buckets", enc))
	if rig.reader.slots[0] != wantSlot || rig.reader.owners[0].ID != "n1" {
		t.Fatalf("read slot %d owner %q, want slot %d owner n1", rig.reader.slots[0], rig.reader.owners[0].ID, wantSlot)
	}
}

func TestSingleFlightCollapsesABurst(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{})
	rig.reader.put(t, "users", u38Decl(10, 1, machine.StateActive))
	rig.reader.entered = make(chan struct{}, 64)
	rig.reader.release = make(chan struct{})

	const burst = 32
	var wg sync.WaitGroup
	entries := make([]machine.Entry, burst)
	errs := make([]error, burst)
	for i := 0; i < burst; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			entries[i], errs[i] = rig.fetcher.Lookup(context.Background(), testDB, "users", 1)
		}()
	}
	release := u38ReleaseOnce(t, rig.reader)
	u38WaitEntered(t, rig.reader)
	u38WaitCollapsed(t, rig, burst-1)
	release()
	wg.Wait()
	if got := rig.reader.reads.Load(); got != 1 {
		t.Fatalf("a burst of %d misses made %d reads, want 1", burst, got)
	}
	for i := range entries {
		if errs[i] != nil || entries[i].Decl.BucketID != 10 {
			t.Fatalf("caller %d: %+v %v", i, entries[i], errs[i])
		}
	}
}

func TestDeclCacheMetricsAreUnlabelled(t *testing.T) {
	statsType := reflect.TypeOf(DeclCacheStats{})
	for i := 0; i < statsType.NumField(); i++ {
		switch statsType.Field(i).Type.Kind() {
		case reflect.Int, reflect.Int64, reflect.Uint64:
		default:
			t.Fatalf("stats field %s is a %s: only plain numbers, never a per-bucket label", statsType.Field(i).Name, statsType.Field(i).Type)
		}
	}
	rig := u38NewRig(t, machine.CacheConfig{})
	for id := uint64(1); id <= 50; id++ {
		rig.reader.put(t, string(rune('a'+id%26))+string(rune('a'+id/26)), u38Decl(id, 1, machine.StateActive))
	}
	before := reflect.TypeOf(rig.fetcher.Stats()).Size()
	for name := range rig.reader.docs {
		if _, err := rig.fetcher.Lookup(context.Background(), testDB, name, 1); err != nil {
			t.Fatal(err)
		}
	}
	if after := reflect.TypeOf(rig.fetcher.Stats()).Size(); after != before {
		t.Fatalf("stats grew with buckets: %d -> %d", before, after)
	}
	if got := rig.fetcher.Stats().Entries; got != 50 {
		t.Fatalf("entries = %d, want 50", got)
	}
}

func TestMissingDeclarationIsNeverCached(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{})
	for i := 0; i < 2; i++ {
		if _, err := rig.fetcher.Lookup(context.Background(), testDB, "ghost", 1); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("lookup %d: err = %v, want ErrBucketNotFound", i, err)
		}
	}
	if got := rig.reader.reads.Load(); got != 2 {
		t.Fatalf("a missing bucket made %d reads over two lookups, want 2 (never cached)", got)
	}
	rig.reader.err = errors.New("disk on fire")
	if _, err := rig.fetcher.Lookup(context.Background(), testDB, "users", 1); !errors.Is(err, ErrDeclUnavailable) {
		t.Fatalf("failed read: err = %v, want ErrDeclUnavailable", err)
	}
	rig.reader.err = nil
	rig.reader.put(t, "gone", u38Decl(11, 1, machine.StateDropping))
	entry, err := rig.fetcher.Lookup(context.Background(), testDB, "gone", 1)
	if err != nil || entry.Decl.State != machine.StateDropping {
		t.Fatalf("dropping declaration: %+v %v", entry, err)
	}
	if got := rig.fetcher.Stats().Entries; got != 0 {
		t.Fatalf("cache holds %d entries after a miss, a failure and a dropping declaration, want 0", got)
	}
}

func TestAFetchIssuedBeforeABumpIsDiscarded(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{})
	rig.reader.put(t, "users", u38Decl(10, 1, machine.StateActive))
	rig.reader.onRead = func() { rig.published.Store(2) }
	entry, err := rig.fetcher.Lookup(context.Background(), testDB, "users", 1)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Seen != 1 {
		t.Fatalf("the raced entry is tagged %d, want the version it was issued at, 1", entry.Seen)
	}
	stats := rig.fetcher.Stats()
	if stats.Entries != 0 || stats.Discarded != 1 {
		t.Fatalf("stats = %+v: a fetch that raced a bump must not be stored", stats)
	}
	rig.reader.onRead = nil
	entry, err = rig.fetcher.Lookup(context.Background(), testDB, "users", 2)
	if err != nil || entry.Seen != 2 {
		t.Fatalf("after the bump: %+v %v", entry, err)
	}
	if got := rig.reader.reads.Load(); got != 2 {
		t.Fatalf("reads = %d, want 2", got)
	}
}

func TestABumpMakesCachedEntriesMiss(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{})
	rig.reader.put(t, "users", u38Decl(10, 1, machine.StateActive))
	if _, err := rig.fetcher.Lookup(context.Background(), testDB, "users", 1); err != nil {
		t.Fatal(err)
	}
	rig.published.Store(2)
	entry, err := rig.fetcher.Lookup(context.Background(), testDB, "users", 2)
	if err != nil || entry.Seen != 2 || rig.reader.reads.Load() != 2 {
		t.Fatalf("entry %+v err %v reads %d", entry, err, rig.reader.reads.Load())
	}
}

func TestTheCacheStaysWithinItsBound(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{MaxEntries: 8, MaxBytes: 1 << 20, MaxDeclBytes: 1 << 20})
	for id := uint64(1); id <= 100; id++ {
		name := "b" + string(rune('a'+id%26)) + string(rune('a'+id/26))
		rig.reader.put(t, name, u38Decl(id, 1, machine.StateActive))
		if _, err := rig.fetcher.Lookup(context.Background(), testDB, name, 1); err != nil {
			t.Fatal(err)
		}
	}
	if got := rig.fetcher.Stats().Entries; got != 8 {
		t.Fatalf("entries = %d, want the bound 8", got)
	}
}

func TestForgetDropsADatabasesEntries(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{})
	rig.reader.put(t, "users", u38Decl(10, 1, machine.StateActive))
	if _, err := rig.fetcher.Lookup(context.Background(), testDB, "users", 1); err != nil {
		t.Fatal(err)
	}
	rig.fetcher.Forget(testDB)
	if got := rig.fetcher.Stats().Entries; got != 0 {
		t.Fatalf("entries = %d after Forget, want 0", got)
	}
}

func TestSystemBucketIsNeverFetchedOrCached(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{})
	entry, err := rig.fetcher.Lookup(context.Background(), testDB, "_buckets", 5)
	if err != nil || entry.Decl.BucketID != machine.SystemBucketID || entry.Seen != 5 {
		t.Fatalf("entry %+v err %v", entry, err)
	}
	if rig.reader.reads.Load() != 0 || rig.fetcher.Stats().Entries != 0 {
		t.Fatal("the system bucket was fetched or cached")
	}
}

func TestReadKeepsTheSweptBitmapForARewrite(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{})
	decl := u38Decl(12, 1, machine.StateDropping)
	swept := make([]byte, store.SweptBytes)
	swept[3] = 0x80
	doc, err := store.EncodeDeclarationWithSwept(decl, swept)
	if err != nil {
		t.Fatal(err)
	}
	rig.reader.docs = map[string]map[uint16][]byte{"dying": doc}
	_, got, found, err := rig.fetcher.Read(context.Background(), testDB, "dying")
	if err != nil || !found || len(got) != store.SweptBytes || got[3] != 0x80 {
		t.Fatalf("found %v err %v %d swept bytes: a rewrite must keep field 6", found, err, len(got))
	}
}

func TestMalformedDeclarationIsUnavailableNotCached(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{})
	rig.reader.docs = map[string]map[uint16][]byte{"bad": {2: {1, 2, 3}}}
	if _, err := rig.fetcher.Lookup(context.Background(), testDB, "bad", 1); !errors.Is(err, ErrDeclUnavailable) {
		t.Fatalf("err = %v, want ErrDeclUnavailable", err)
	}
	if rig.fetcher.Stats().Entries != 0 {
		t.Fatal("a malformed declaration was cached")
	}
}

// u38Fence evaluates the owner's fence for a write to bucketID the way the Writer builds it.
func u38Fence(schema Schema, bucketID uint64, readmitted bool) machine.FenceResult {
	view := schema.View(testDB, bucketID)
	_, version := machine.EffectiveTable(view.Decl, view.Published)
	return machine.Fence(machine.Admission{
		Present: view.Present, Missing: view.Missing, Decl: view.Decl, Seen: view.Seen,
		Published: view.Published, ReAdmitted: readmitted, PayloadBucketID: bucketID,
		AdmittedVersion: version, WrittenFieldIDs: []uint16{2},
	})
}

func TestColdOwnerFetchIsNotARefusal(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{})
	rig.reader.put(t, "users", u38Decl(10, 1, machine.StateActive))
	schema := NewCacheSchema(rig.fetcher)

	cold := u38Fence(schema, 10, false)
	if !cold.ReAdmit || cold.Code != 0 {
		t.Fatalf("cold fence = %+v, want a re-admission and no refusal", cold)
	}
	if err := schema.Refetch(context.Background(), testDB, 10); err != nil {
		t.Fatal(err)
	}
	if warm := u38Fence(schema, 10, true); warm.Code != 0 || warm.ReAdmit {
		t.Fatalf("after the fetch the fence = %+v, want admitted", warm)
	}
	if got := rig.reader.reads.Load(); got != 1 {
		t.Fatalf("reads = %d, want 1", got)
	}
}

func TestSchemaViewOfTheSystemBucketNeedsNoFetch(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{})
	view := NewCacheSchema(rig.fetcher).View(testDB, machine.SystemBucketID)
	if !view.Present || view.Seen != 1 || view.Published != 1 {
		t.Fatalf("view = %+v", view)
	}
}

func TestRefetchAfterABumpRefreshesAStaleEntry(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{})
	rig.reader.put(t, "users", u38Decl(10, 1, machine.StateActive))
	schema := NewCacheSchema(rig.fetcher)
	if err := schema.Refetch(context.Background(), testDB, 10); err != nil {
		t.Fatal(err)
	}
	rig.published.Store(2)
	if view := schema.View(testDB, 10); !view.Present || view.Seen != 1 || view.Published != 2 {
		t.Fatalf("stale view = %+v", view)
	}
	if err := schema.Refetch(context.Background(), testDB, 10); err != nil {
		t.Fatal(err)
	}
	if view := schema.View(testDB, 10); view.Seen != 2 {
		t.Fatalf("refetched view = %+v", view)
	}
	if got := rig.reader.nameRead.Load(); got != 1 {
		t.Fatalf("name reads = %d, want 1: a stale entry already knows its name", got)
	}
}

func TestRefetchOfAnUnknownBucketCachesNothing(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{})
	schema := NewCacheSchema(rig.fetcher)
	if err := schema.Refetch(context.Background(), testDB, 99); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("an absent bucket is ErrBucketNotFound, which the owner path answers NOT_FOUND: %v", err)
	}
	if view := schema.View(testDB, 99); view.Present {
		t.Fatal("an unknown bucket is present")
	}
}

func TestRefetchFailureIsReported(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{})
	rig.reader.put(t, "users", u38Decl(10, 1, machine.StateActive))
	rig.reader.err = errors.New("owner unreachable")
	if err := NewCacheSchema(rig.fetcher).Refetch(context.Background(), testDB, 10); !errors.Is(err, ErrDeclUnavailable) {
		t.Fatalf("err = %v, want ErrDeclUnavailable", err)
	}
}

func TestNoLiveOwnerIsUnavailable(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{})
	rig.topology.table = cluster.BuildTable(cluster.ClusterState{Members: []cluster.Member{{ID: "n1", State: cluster.MemberDown}}})
	_, err := rig.fetcher.Lookup(context.Background(), testDB, "users", 1)
	if !errors.Is(err, ErrDeclUnavailable) || rig.reader.reads.Load() != 0 {
		t.Fatalf("err = %v reads %d", err, rig.reader.reads.Load())
	}
}

func TestACancelledLeaderDoesNotFailItsFollowers(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{})
	rig.reader.put(t, "users", u38Decl(10, 1, machine.StateActive))
	rig.reader.entered = make(chan struct{}, 8)
	rig.reader.release = make(chan struct{})
	leaderCtx, cancel := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, err := rig.fetcher.Lookup(leaderCtx, testDB, "users", 1)
		leaderDone <- err
	}()
	release := u38ReleaseOnce(t, rig.reader)
	u38WaitEntered(t, rig.reader)
	followerDone := make(chan error, 1)
	go func() {
		_, err := rig.fetcher.Lookup(context.Background(), testDB, "users", 1)
		followerDone <- err
	}()
	u38WaitCollapsed(t, rig, 1)
	cancel()
	select {
	case err := <-leaderDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("leader err = %v, want the cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the cancelled leader never returned")
	}
	release()
	select {
	case err := <-followerDone:
		if err != nil {
			t.Fatalf("follower err = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the follower never returned")
	}
}

// TestDeclFetcherMutationsNeverCorruptTheCache feeds mutated stored documents: every one is
// either refused with ErrDeclUnavailable or accepted with a stable canonical form, and nothing panics.
func TestDeclFetcherMutationsNeverCorruptTheCache(t *testing.T) {
	base, err := store.EncodeDeclaration(u38Decl(10, 1, machine.StateActive))
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(38))
	rig := u38NewRig(t, machine.CacheConfig{MaxEntries: 4, MaxBytes: 1 << 16, MaxDeclBytes: 1 << 16})
	for i := 0; i < 100000; i++ {
		doc := map[uint16][]byte{}
		for id, raw := range base {
			doc[id] = append([]byte(nil), raw...)
		}
		id := uint16(rng.Intn(5) + 2)
		if raw := doc[id]; len(raw) > 0 {
			raw[rng.Intn(len(raw))] ^= byte(1 << rng.Intn(8))
		}
		rig.reader.docs = map[string]map[uint16][]byte{"users": doc}
		entry, err := rig.fetcher.Lookup(context.Background(), testDB, "users", 1)
		if err != nil {
			if !errors.Is(err, ErrDeclUnavailable) {
				t.Fatalf("mutation %d: %v", i, err)
			}
			continue
		}
		again, err := store.EncodeDeclaration(entry.Decl)
		if err != nil {
			t.Fatalf("mutation %d accepted but cannot re-encode: %v", i, err)
		}
		// Byte identity with the mutated input does not hold: U37's decoder reads neither the BSON
		// length prefix, the binary subtype nor the name of entry 1 (reported with U38). What the
		// Fetcher owes is that what it accepted is stable under another round trip.
		reread, err := store.DecodeDeclaration(again)
		if err != nil {
			t.Fatalf("mutation %d: the canonical form does not decode: %v", i, err)
		}
		if !reflect.DeepEqual(reread, entry.Decl) {
			t.Fatalf("mutation %d: the canonical form decodes differently", i)
		}
		rig.published.Store(rig.published.Load() + 1)
	}
	if rig.fetcher.Stats().Entries > 4 {
		t.Fatal("cache exceeded its bound")
	}
}

// u38WaitEntered waits for the reader to be entered, or fails the test after a few seconds.
func u38WaitEntered(t *testing.T, reader *u38Reader) {
	t.Helper()
	select {
	case <-reader.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the reader was never entered")
	}
}

// u38WaitCollapsed waits until want misses have joined a flight, or fails the test after a few
// seconds, so a broken single flight is an assertion and not a hang.
func u38WaitCollapsed(t *testing.T, rig *u38Rig, want uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for rig.fetcher.Stats().Collapsed < want {
		if time.Now().After(deadline) {
			t.Fatalf("only %d misses joined the flight, want %d", rig.fetcher.Stats().Collapsed, want)
		}
		runtime.Gosched()
	}
}

// u38ReleaseOnce lets the blocked reader go when the test ends, so a failed test leaks no goroutine.
func u38ReleaseOnce(t *testing.T, reader *u38Reader) func() {
	var once sync.Once
	release := func() { once.Do(func() { close(reader.release) }) }
	t.Cleanup(release)
	return release
}

func TestAFailedRouteIsNotCountedAsAFetch(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{})
	rig.topology.table = cluster.BuildTable(cluster.ClusterState{Members: []cluster.Member{{ID: "n1", State: cluster.MemberDown}}})
	if _, err := rig.fetcher.Lookup(context.Background(), testDB, "users", 1); !errors.Is(err, ErrDeclUnavailable) {
		t.Fatalf("err = %v", err)
	}
	if got := rig.fetcher.Stats().Fetches; got != 0 {
		t.Fatalf("fetches = %d, want 0: no keyed read was issued", got)
	}
}

func TestTheByteBoundEvictsTheLeastRecentlyUsed(t *testing.T) {
	probe := u38NewRig(t, machine.CacheConfig{})
	probe.reader.put(t, "aa", u38Decl(1, 1, machine.StateActive))
	if _, err := probe.fetcher.Lookup(context.Background(), testDB, "aa", 1); err != nil {
		t.Fatal(err)
	}
	one := probe.fetcher.Stats().Bytes

	rig := u38NewRig(t, machine.CacheConfig{MaxEntries: 100, MaxBytes: 2*one + one/2, MaxDeclBytes: one})
	for id, name := range []string{"aa", "bb", "cc"} {
		rig.reader.put(t, name, u38Decl(uint64(id+1), 1, machine.StateActive))
		if _, err := rig.fetcher.Lookup(context.Background(), testDB, name, 1); err != nil {
			t.Fatal(err)
		}
	}
	stats := rig.fetcher.Stats()
	if stats.Entries != 2 || stats.Bytes > 2*one+one/2 {
		t.Fatalf("stats = %+v: the byte bound must hold two entries of %d bytes", stats, one)
	}
	reads := rig.reader.reads.Load()
	for _, name := range []string{"bb", "cc"} {
		if _, err := rig.fetcher.Lookup(context.Background(), testDB, name, 1); err != nil {
			t.Fatal(err)
		}
	}
	if rig.reader.reads.Load() != reads {
		t.Fatal("the two most recent entries were not kept")
	}
	if _, err := rig.fetcher.Lookup(context.Background(), testDB, "aa", 1); err != nil || rig.reader.reads.Load() != reads+1 {
		t.Fatalf("the least recently used entry was not the one evicted: err %v", err)
	}
}
