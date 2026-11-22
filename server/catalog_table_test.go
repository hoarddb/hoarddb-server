package server

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// claimNamed claims name and fails the test on error.
func claimNamed(tb testing.TB, table *catalogTable, name string) *bucketRec {
	tb.Helper()
	rec, err := table.claim(name, 3, 1, false)
	if err != nil {
		tb.Fatalf("claiming %q: %v", name, err)
	}
	return rec
}

// dropNamed runs the drop's phases on a record: unlink, close, free.
func dropNamed(tb testing.TB, table *catalogTable, rec *bucketRec) {
	tb.Helper()
	if err := table.unlink(rec); err != nil {
		tb.Fatalf("unlinking record %d: %v", rec.self, err)
	}
	rec.mu.Lock()
	rec.closed = true
	rec.mu.Unlock()
	if err := table.free(rec); err != nil {
		tb.Fatalf("freeing record %d: %v", rec.self, err)
	}
}

// containsPointer reports whether a value of type candidate can hold a pointer
// the collector would trace.
func containsPointer(candidate reflect.Type) bool {
	switch candidate.Kind() {
	case reflect.Struct:
		for field := 0; field < candidate.NumField(); field++ {
			if containsPointer(candidate.Field(field).Type) {
				return true
			}
		}
		return false
	case reflect.Array:
		return candidate.Len() > 0 && containsPointer(candidate.Elem())
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return false
	default:
		return true
	}
}

// TestCatalogTableRecordHoldsNoPointerAndFitsTheSpec keeps a record a noscan
// allocation of the size the spec measured (section 5.2): a pointer, a string
// or a slice added to bucketRec would put every idle bucket back on the
// collector's mark list, which is the bug this table exists to remove.
func TestCatalogTableRecordHoldsNoPointerAndFitsTheSpec(t *testing.T) {
	record := reflect.TypeFor[bucketRec]()
	if containsPointer(record) {
		t.Fatal("bucketRec holds a pointer-bearing field; a chunk of them would be scanned by every GC cycle")
	}
	if record.Size() > 88 {
		t.Fatalf("bucketRec is %d bytes, the spec's measured size is 88", record.Size())
	}
}

func TestCatalogTableAddFindUnlinkReuse(t *testing.T) {
	table := newCatalogTable()

	first := claimNamed(t, table, "orders")
	if got := table.find("orders"); got != first {
		t.Fatalf("find(orders) = %p, want the claimed record %p", got, first)
	}
	if table.find("users") != nil {
		t.Fatal("find(users) found a record that was never claimed")
	}
	firstEpoch := first.epoch.Load()
	firstSelf := first.self

	if err := table.unlink(first); err != nil {
		t.Fatalf("unlink: %v", err)
	}
	if table.find("orders") != nil {
		t.Fatal("an unlinked name still finds its record")
	}
	// Between unlink and free the slot must not be recycled: a create arriving
	// in that gap would otherwise be closed by the drop that is still running.
	between := claimNamed(t, table, "users")
	if between == first {
		t.Fatal("a claim between unlink and free recycled the slot")
	}
	if err := table.free(first); err != nil {
		t.Fatalf("free: %v", err)
	}

	reused := claimNamed(t, table, "invoices")
	if reused != first || reused.self != firstSelf {
		t.Fatalf("the freed slot was not reused: got record %d, want %d", reused.self, firstSelf)
	}
	if reused.epoch.Load() <= firstEpoch {
		t.Fatalf("a reused slot's epoch is %d, want more than %d", reused.epoch.Load(), firstEpoch)
	}
	if table.find("orders") != nil || table.find("invoices") != reused {
		t.Fatal("after reuse the old name finds a record or the new name does not")
	}
}

func TestCatalogTableClaimIsIdempotentAndRestampsUncommitted(t *testing.T) {
	table := newCatalogTable()
	first, err := table.claim("orders", 3, 5, true)
	if err != nil {
		t.Fatal(err)
	}
	again, err := table.claim("orders", 4, 9, false)
	if err != nil {
		t.Fatal(err)
	}
	if again != first || again.generation.Load() != 9 || again.isCreating() || again.bucketType != 3 {
		t.Fatalf("re-claim of an uncommitted record: same=%v generation=%d creating=%v type=%d, want same, 9, false, 3",
			again == first, again.generation.Load(), again.isCreating(), again.bucketType)
	}
	epoch := first.epoch.Load()
	first.id.Store(42)
	if _, err := table.claim("orders", 3, 11, true); err != nil {
		t.Fatal(err)
	}
	if first.generation.Load() != 9 || first.isCreating() {
		t.Fatal("a claim of a committed record overwrote its generation or creating flag")
	}
	if first.epoch.Load() != epoch {
		t.Fatalf("a claim of a name that already has a record changed its epoch from %d to %d", epoch, first.epoch.Load())
	}
}

func TestCatalogTableRefusesADoubleUnlinkAndDoubleFree(t *testing.T) {
	table := newCatalogTable()
	rec := claimNamed(t, table, "orders")
	if err := table.free(rec); err == nil {
		t.Fatal("free of a linked record succeeded; it would leave the name finding a recycled slot")
	}
	if err := table.unlink(rec); err != nil {
		t.Fatal(err)
	}
	if err := table.unlink(rec); err == nil {
		t.Fatal("a second unlink succeeded")
	}
	if err := table.free(rec); err != nil {
		t.Fatal(err)
	}
	if err := table.free(rec); err == nil {
		t.Fatal("a second free succeeded; the slot would be on the free list twice")
	}
}

func TestCatalogTableForcedCollisionReturnsTheRightBucket(t *testing.T) {
	table := newCatalogTableWithHasher(func(string) uint64 { return 7 })
	const names = 1000
	records := make(map[string]*bucketRec, names)
	for i := 0; i < names; i++ {
		name := fmt.Sprintf("bucket-%d", i)
		records[name] = claimNamed(t, table, name)
	}
	for name, want := range records {
		if got := table.find(name); got != want {
			t.Fatalf("find(%q) returned record %v, want its own %v", name, got, want)
		}
	}
	if table.find("bucket-absent") != nil {
		t.Fatal("a name that was never claimed found a record through a colliding chain")
	}
	// Removing from the middle and the head of one chain must not lose the others.
	for i := 0; i < names; i += 3 {
		name := fmt.Sprintf("bucket-%d", i)
		if err := table.unlink(records[name]); err != nil {
			t.Fatal(err)
		}
		delete(records, name)
	}
	for name, want := range records {
		if got := table.find(name); got != want {
			t.Fatalf("after unlinking a third, find(%q) returned %v, want %v", name, got, want)
		}
	}
}

func TestCatalogTableUsesASeededHash(t *testing.T) {
	first, second := newCatalogTable(), newCatalogTable()
	if first.hasher("orders") == second.hasher("orders") {
		t.Fatal("two tables hashed one name to the same value; the seed is not random per table")
	}
}

func TestCatalogTableHandlesLongAndMultibyteNames(t *testing.T) {
	table := newCatalogTable()
	longest := strings.Repeat("x", 65000)
	multibyte := []string{"café", "データ", "日本語のバケット", "🙂🙃", strings.Repeat("é", 30000)}
	records := map[string]*bucketRec{longest: claimNamed(t, table, longest)}
	for _, name := range multibyte {
		records[name] = claimNamed(t, table, name)
	}
	for name, want := range records {
		got := table.find(name)
		if got != want {
			t.Fatalf("find of a %d-byte name returned the wrong record", len(name))
		}
		if table.name(got) != name {
			t.Fatalf("the stored name of a %d-byte name differs from what was claimed", len(name))
		}
	}

	atLimit := strings.Repeat("y", maxCatalogNameBytes)
	claimNamed(t, table, atLimit)
	if _, err := table.claim(strings.Repeat("z", maxCatalogNameBytes+1), 3, 1, false); err == nil {
		t.Fatal("a name of 65,536 bytes was accepted; nameLen would wrap")
	}
}

func TestCatalogTableRecordAddressIsStable(t *testing.T) {
	table := newCatalogTable()
	first := claimNamed(t, table, "first")
	address := fmt.Sprintf("%p", first)
	for i := 0; i < 100000; i++ {
		claimNamed(t, table, fmt.Sprintf("bucket-%d", i))
	}
	if got := fmt.Sprintf("%p", table.find("first")); got != address {
		t.Fatalf("record 0 moved from %s to %s after 100,000 claims", address, got)
	}
	if table.find("bucket-99999") == nil {
		t.Fatal("the last claim cannot be found")
	}
}

func TestCatalogTableReleasedChunkLeavesEarlierPointerReadable(t *testing.T) {
	table := newCatalogTable()
	// Chunks 0, 1 and 2 hold 8, 16 and 32 records; 100 claims fill them and
	// start chunk 3, so chunks 1 and 2 can be emptied without touching the tail.
	records := make([]*bucketRec, 0, 100)
	for i := 0; i < 100; i++ {
		records = append(records, claimNamed(t, table, fmt.Sprintf("bucket-%d", i)))
	}
	byChunk := map[int][]*bucketRec{}
	for _, rec := range records {
		chunkNo, _ := chunkOf(rec.self)
		byChunk[chunkNo] = append(byChunk[chunkNo], rec)
	}
	held := byChunk[2][0]
	heldEpoch := held.epoch.Load()

	// The first emptied chunk becomes the one spare; the second is released.
	for _, chunkNo := range []int{1, 2} {
		for _, rec := range byChunk[chunkNo] {
			dropNamed(t, table, rec)
		}
	}
	if table.chunks[1] == nil {
		t.Fatal("the first emptied chunk was released; one empty chunk should stay as the spare")
	}
	if table.chunks[2] != nil {
		t.Fatal("the second emptied chunk is still resident")
	}
	if !held.closed || held.epoch.Load() != heldEpoch {
		t.Fatalf("a pointer into the released chunk reads closed=%v epoch=%d, want true and %d", held.closed, held.epoch.Load(), heldEpoch)
	}
	// The table keeps working: survivors are found and new claims succeed.
	if table.find("bucket-0") == nil || table.find("bucket-99") == nil {
		t.Fatal("a survivor of the dropped chunks is no longer found")
	}
	for i := 0; i < 200; i++ {
		claimNamed(t, table, fmt.Sprintf("later-%d", i))
	}
}

func TestCatalogTableShardRebuildAndArenaCompactionKeepLookups(t *testing.T) {
	table := newCatalogTable()
	const total = 40000
	names := make([]string, total)
	records := make([]*bucketRec, total)
	for i := range names {
		names[i] = fmt.Sprintf("a-reasonably-long-bucket-name-%08d", i)
		records[i] = claimNamed(t, table, names[i])
	}
	if !table.sharded {
		t.Fatal("40,000 records did not move the index to shards")
	}
	arenaBefore := table.arenaBytes

	survivors := map[string]*bucketRec{}
	for i := range names {
		if i%10 == 0 {
			survivors[names[i]] = records[i]
			continue
		}
		dropNamed(t, table, records[i])
	}

	steps, mostWork := 0, 0
	for {
		work, more, err := table.maintain()
		if err != nil {
			t.Fatalf("maintenance step %d: %v", steps, err)
		}
		steps++
		mostWork = max(mostWork, work)
		if !more {
			break
		}
		if steps > 10000 {
			t.Fatal("maintenance did not finish in 10,000 steps")
		}
	}
	if mostWork > maxRecordChunk {
		t.Fatalf("one maintenance step did %d units of work; the bound is one record chunk of %d", mostWork, maxRecordChunk)
	}
	for name, want := range survivors {
		if got := table.find(name); got != want {
			t.Fatalf("after maintenance find(%q) returned %v, want %v", name, got, want)
		}
		if table.name(want) != name {
			t.Fatalf("after maintenance record for %q holds name %q", name, table.name(want))
		}
	}
	if table.arenaBytes >= arenaBefore {
		t.Fatalf("arena stayed at %d bytes after dropping 90%%; compaction released nothing (before: %d)", table.arenaBytes, arenaBefore)
	}
	t.Logf("maintenance took %d steps, the largest %d units; arena %d -> %d bytes", steps, mostWork, arenaBefore, table.arenaBytes)
}

func TestCatalogTableClaimAllocatesBounded(t *testing.T) {
	const claims = 100000
	names := make([]string, claims)
	for i := range names {
		names[i] = fmt.Sprintf("bucket-%09d", i)
	}
	table := newCatalogTable()
	chunkBytes := uint64(maxRecordChunk) * uint64(recordSize)
	limit := chunkBytes + 64*1024

	var before, after runtime.MemStats
	var worst uint64
	worstAt := 0
	for i, name := range names {
		runtime.ReadMemStats(&before)
		if _, err := table.claim(name, 3, 1, false); err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&after)
		if spent := after.TotalAlloc - before.TotalAlloc; spent > worst {
			worst, worstAt = spent, i
		}
	}
	if worst > limit {
		t.Fatalf("claim #%d allocated %d bytes; the bound is one record chunk (%d) plus 64 KB = %d", worstAt, worst, chunkBytes, limit)
	}
	t.Logf("the most one claim allocated in %d claims was %d bytes (claim #%d); the bound is %d", claims, worst, worstAt, limit)
}

// plainBucketBytesToday is what one plain bucket costs the registry today,
// measured at 10,000 buckets in the catalog-density-2026-10-02 record:
// the struct, its engine, its name and its share of the map.
const plainBucketBytesToday = 335

// inMemoryDBBaselineBytes is what a database holding bucketCount plain buckets
// costs today: the registry struct, an empty map's header and the measured
// per-bucket figure. It is arithmetic on a recorded measurement, so the test
// that uses it asserts a deterministic number and not HeapAlloc.
func inMemoryDBBaselineBytes(bucketCount int) int64 {
	const mapHeader = 48
	return int64(reflect.TypeFor[inMemoryDB]().Size()) + mapHeader + int64(bucketCount)*plainBucketBytesToday
}

func TestCatalogTableEmptyCostIsSmall(t *testing.T) {
	const allowance = 1024
	table := newCatalogTable()
	emptyBytes, emptyToday := table.residentBytes(), inMemoryDBBaselineBytes(0)
	if emptyBytes > emptyToday+allowance {
		t.Fatalf("an empty table is %d bytes, today's empty database is %d: over by more than %d", emptyBytes, emptyToday, allowance)
	}

	claimNamed(t, table, "orders")
	oneBytes, oneToday := table.residentBytes(), inMemoryDBBaselineBytes(1)
	if oneBytes > oneToday+allowance {
		t.Fatalf("a table with one bucket is %d bytes, today's is %d: over by more than %d", oneBytes, oneToday, allowance)
	}
	t.Logf("empty: table %d B, today %d B; one bucket: table %d B, today %d B", emptyBytes, emptyToday, oneBytes, oneToday)
}

func TestCatalogTableScanVisitsEachNameOnce(t *testing.T) {
	table := newCatalogTable()
	var lock sync.RWMutex
	want := map[string]bool{}
	for i := 0; i < 9000; i++ {
		name := fmt.Sprintf("bucket-%d", i)
		claimNamed(t, table, name)
		want[name] = true
	}
	dropped := table.find("bucket-5")
	if err := table.unlink(dropped); err != nil {
		t.Fatal(err)
	}
	delete(want, "bucket-5")

	seen := map[string]int{}
	table.scan(&lock, func(names []string) {
		if len(names) > maxRecordChunk {
			t.Errorf("a step carried %d names; one chunk holds at most %d", len(names), maxRecordChunk)
		}
		if lock.TryLock() { // the callback runs outside the lock
			lock.Unlock()
		} else {
			t.Error("the lock was held while the visitor ran")
		}
		for _, name := range names {
			seen[name]++
		}
	})
	if len(seen) != len(want) {
		t.Fatalf("scan saw %d names, want %d", len(seen), len(want))
	}
	for name, times := range seen {
		if times != 1 || !want[name] {
			t.Fatalf("scan saw %q %d times (wanted: %v)", name, times, want[name])
		}
	}
}

// TestCatalogTableConcurrentReadersSeeStablePointers is meant for -race: one
// writer claims and drops names under the write lock while readers look up
// under the read lock and check that what they get back carries the name they
// asked for.
func TestCatalogTableConcurrentReadersSeeStablePointers(t *testing.T) {
	table := newCatalogTable()
	var lock sync.RWMutex
	const stable = 200
	for i := 0; i < stable; i++ {
		claimNamed(t, table, fmt.Sprintf("stable-%d", i))
	}

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for reader := 0; reader < 4; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for round := 0; ; round++ {
				select {
				case <-stop:
					return
				default:
				}
				name := fmt.Sprintf("stable-%d", round%stable)
				lock.RLock()
				rec := table.find(name)
				var got string
				if rec != nil {
					got = table.name(rec)
				}
				lock.RUnlock()
				if rec == nil || got != name {
					t.Errorf("find(%q) returned a record named %q", name, got)
					return
				}
			}
		}()
	}

	for i := 0; i < 20000; i++ {
		name := fmt.Sprintf("churn-%d", i%700)
		lock.Lock()
		if rec := table.find(name); rec != nil {
			dropNamed(t, table, rec)
		} else {
			claimNamed(t, table, name)
		}
		if _, _, err := table.maintain(); err != nil {
			t.Fatalf("maintaining after churning %q: %v", name, err)
		}
		lock.Unlock()
	}
	close(stop)
	readers.Wait()
}

// FuzzCatalogTableAgainstMap drives random claim, find, unlink, free and
// maintenance sequences against a map. The first byte picks a hasher: the real
// one, or one that sends every name to a few values so chains are long.
func FuzzCatalogTableAgainstMap(f *testing.F) {
	f.Add([]byte{0, 0, 1, 2, 3, 4, 0, 1, 2, 3})
	f.Add([]byte{1, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4})
	f.Add([]byte{0, 0, 5, 0, 6, 0, 7, 2, 5, 2, 6, 3, 3, 4, 4, 4, 0, 5, 1, 5})
	f.Add([]byte{1, 0, 9, 0, 10, 0, 11, 0, 12, 2, 9, 3, 2, 10, 3, 4, 1, 11, 1, 12})
	f.Fuzz(func(t *testing.T, script []byte) {
		if len(script) == 0 {
			return
		}
		table := newCatalogTable()
		if script[0]%2 == 1 {
			table = newCatalogTableWithHasher(func(name string) uint64 { return uint64(len(name) % 3) })
		}
		linked := map[string]*bucketRec{}
		var pending []*bucketRec
		nameOf := func(operand byte) string { return strings.Repeat("n", int(operand%5)) + fmt.Sprint(operand%64) }

		for position := 1; position+1 < len(script); position += 2 {
			operation, name := script[position]%5, nameOf(script[position+1])
			switch operation {
			case 0:
				rec, err := table.claim(name, 3, 1, false)
				if err != nil {
					t.Fatal(err)
				}
				if want, exists := linked[name]; exists && want != rec {
					t.Fatalf("claim(%q) returned a second record for a linked name", name)
				}
				linked[name] = rec
			case 1:
				if got := table.find(name); got != linked[name] {
					t.Fatalf("find(%q) = %v, the model has %v", name, got, linked[name])
				}
			case 2:
				if rec, exists := linked[name]; exists {
					if err := table.unlink(rec); err != nil {
						t.Fatal(err)
					}
					delete(linked, name)
					pending = append(pending, rec)
				}
			case 3:
				if len(pending) > 0 {
					if err := table.free(pending[0]); err != nil {
						t.Fatal(err)
					}
					pending = pending[1:]
				}
			case 4:
				if _, _, err := table.maintain(); err != nil {
					t.Fatal(err)
				}
			}
		}
		for name, want := range linked {
			if got := table.find(name); got != want {
				t.Fatalf("at the end find(%q) = %v, the model has %v", name, got, want)
			}
			if table.name(want) != name {
				t.Fatalf("at the end the record for %q holds %q", name, table.name(want))
			}
		}
	})
}
