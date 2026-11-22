package machine

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

// tableOf builds a field table with the given ids, each a string field named
// after its id, and next_field_id one past the highest.
func tableOf(ids ...uint16) FieldTable {
	table := FieldTable{NextFieldID: 1}
	for _, id := range ids {
		table.Entries = append(table.Entries, FieldEntry{ID: id, Type: "string", Name: fmt.Sprintf("f%d", id)})
		if id >= table.NextFieldID {
			table.NextFieldID = id + 1
		}
	}
	return table
}

// declOf builds an active declaration over the fields 1 and 2.
func declOf(bucketID uint64, version int64, pending *Pending) Declaration {
	return Declaration{BucketID: bucketID, Version: version, State: "active", Fields: tableOf(1, 2), Pending: pending}
}

func entryOf(dbID uint64, name string, seen int64, decl Declaration, size int64) Entry {
	return Entry{DBID: dbID, Name: name, Seen: seen, Decl: decl, Size: size}
}

// keysOf prints the cache in LRU order, oldest first, as db/name.
func keysOf(c *CacheState) []string {
	var keys []string
	for _, e := range c.Entries {
		keys = append(keys, fmt.Sprintf("%d/%s", e.DBID, e.Name))
	}
	return keys
}

func totalSize(c *CacheState) int64 {
	var sum int64
	for _, e := range c.Entries {
		sum += e.Size
	}
	return sum
}

// cloneState copies a cache so a later Apply cannot change what was captured.
func cloneState(c *CacheState) CacheState {
	return CacheState{Entries: append([]Entry(nil), c.Entries...)}
}

// testConfig bounds the cache tightly: 3 entries, 100 bytes, 40 bytes per declaration.
var testConfig = CacheConfig{MaxEntries: 3, MaxBytes: 100, MaxDeclBytes: 40}

// fetchedAt delivers a fetch that was issued and completed at one published version.
func fetchedAt(c *CacheState, e Entry, version int64, cfg CacheConfig) CacheDecision {
	return c.Apply(Fetched{Entry: e, IssuedAt: version, Published: version}, cfg)
}

// TestDecisionTable covers every row of the brief's decision table: one case per row,
// each with its starting cache, its event, the decision and the cache that must remain.
// [Guard for the whole table: change any one row's behaviour and its case fails.]
func TestDecisionTable(t *testing.T) {
	d := declOf(7, 1, nil)
	dropping := Declaration{BucketID: 7, Version: 1, State: "dropping", Fields: tableOf(1, 2)}
	withPending := declOf(7, 1, &Pending{Version: 3, Table: tableOf(1, 2, 3)})

	a := entryOf(1, "a", 5, d, 10)
	b := entryOf(1, "b", 5, d, 10)
	c := entryOf(2, "c", 5, d, 10)
	stale := entryOf(1, "s", 4, d, 10)

	cases := []struct {
		name     string
		setup    []Entry
		event    CacheEvent
		cfg      CacheConfig
		want     CacheDecision
		wantKeys []string
	}{
		{
			name:     "Lookup, entry exists and IssuedAt == Published: move to MRU, Hit",
			setup:    []Entry{a, b},
			event:    Lookup{DBID: 1, Name: "a", Published: 5},
			cfg:      testConfig,
			want:     CacheDecision{Entry: a, Hit: true},
			wantKeys: []string{"1/b", "1/a"},
		},
		{
			name:     "Lookup, entry exists and IssuedAt != Published: drop it, Fetch",
			setup:    []Entry{a, stale, b},
			event:    Lookup{DBID: 1, Name: "s", Published: 5},
			cfg:      testConfig,
			want:     CacheDecision{Fetch: true},
			wantKeys: []string{"1/a", "1/b"},
		},
		{
			name:     "Lookup, absent: Fetch and never cached",
			setup:    []Entry{a},
			event:    Lookup{DBID: 1, Name: "zzz", Published: 5},
			cfg:      testConfig,
			want:     CacheDecision{Fetch: true},
			wantKeys: []string{"1/a"},
		},
		{
			name:     "Lookup, same name in another database is absent",
			setup:    []Entry{a},
			event:    Lookup{DBID: 2, Name: "a", Published: 5},
			cfg:      testConfig,
			want:     CacheDecision{Fetch: true},
			wantKeys: []string{"1/a"},
		},
		{
			name:     "FetchIssued: record the flight, change no entry, decide nothing",
			setup:    []Entry{a},
			event:    FetchIssued{DBID: 1, Name: "n", At: 5},
			cfg:      testConfig,
			want:     CacheDecision{},
			wantKeys: []string{"1/a"},
		},
		{
			name:     "Fetched, IssuedAt != Published (a bump raced the fetch): discard",
			setup:    []Entry{a},
			event:    Fetched{Entry: entryOf(1, "n", 0, d, 10), IssuedAt: 5, Published: 6},
			cfg:      testConfig,
			want:     CacheDecision{},
			wantKeys: []string{"1/a"},
		},
		{
			name:     "Fetched, a dropping declaration: discard",
			setup:    []Entry{a},
			event:    Fetched{Entry: entryOf(1, "n", 0, dropping, 10), IssuedAt: 5, Published: 5},
			cfg:      testConfig,
			want:     CacheDecision{},
			wantKeys: []string{"1/a"},
		},
		{
			name:     "Fetched, Size > MaxDeclBytes: not stored, counted",
			setup:    []Entry{a},
			event:    Fetched{Entry: entryOf(1, "n", 0, d, 41), IssuedAt: 5, Published: 5},
			cfg:      testConfig,
			want:     CacheDecision{Counted: true},
			wantKeys: []string{"1/a"},
		},
		{
			name:     "Fetched, Size == MaxDeclBytes is still stored",
			setup:    nil,
			event:    Fetched{Entry: entryOf(1, "n", 0, d, 40), IssuedAt: 5, Published: 5},
			cfg:      testConfig,
			want:     CacheDecision{Entry: entryOf(1, "n", 5, d, 40), Store: true},
			wantKeys: []string{"1/n"},
		},
		{
			name:     "Fetched, Size > MaxBytes while <= MaxDeclBytes: cannot ever fit, counted",
			setup:    []Entry{a},
			event:    Fetched{Entry: entryOf(1, "n", 0, d, 31), IssuedAt: 5, Published: 5},
			cfg:      CacheConfig{MaxEntries: 3, MaxBytes: 30, MaxDeclBytes: 40},
			want:     CacheDecision{Counted: true},
			wantKeys: []string{"1/a"},
		},
		{
			name:     "Fetched, otherwise: insert at MRU, declaration stored as read with pending, tagged with IssuedAt",
			setup:    []Entry{a},
			event:    Fetched{Entry: entryOf(1, "n", 99, withPending, 10), IssuedAt: 5, Published: 5},
			cfg:      testConfig,
			want:     CacheDecision{Entry: entryOf(1, "n", 5, withPending, 10), Store: true},
			wantKeys: []string{"1/a", "1/n"},
		},
		{
			name:     "Fetched for a key already cached replaces it at MRU, no duplicate",
			setup:    []Entry{a, b},
			event:    Fetched{Entry: entryOf(1, "a", 0, withPending, 12), IssuedAt: 5, Published: 5},
			cfg:      testConfig,
			want:     CacheDecision{Entry: entryOf(1, "a", 5, withPending, 12), Store: true},
			wantKeys: []string{"1/b", "1/a"},
		},
		{
			name:     "entries bound: len > MaxEntries after an insert evicts the front, Evict=[0]",
			setup:    []Entry{a, b, c},
			event:    Fetched{Entry: entryOf(2, "d", 0, d, 10), IssuedAt: 5, Published: 5},
			cfg:      testConfig,
			want:     CacheDecision{Entry: entryOf(2, "d", 5, d, 10), Store: true, Evict: []int{0}},
			wantKeys: []string{"1/b", "2/c", "2/d"},
		},
		{
			name:     "bytes bound: sum > MaxBytes evicts from the front until it fits, Evict=[0 0]",
			setup:    []Entry{entryOf(1, "a", 5, d, 30), entryOf(1, "b", 5, d, 30), entryOf(2, "c", 5, d, 30)},
			event:    Fetched{Entry: entryOf(2, "d", 0, d, 50), IssuedAt: 5, Published: 5},
			cfg:      CacheConfig{MaxEntries: 10, MaxBytes: 100, MaxDeclBytes: 60},
			want:     CacheDecision{Entry: entryOf(2, "d", 5, d, 50), Store: true, Evict: []int{0, 0}},
			wantKeys: []string{"2/c", "2/d"},
		},
		{
			name:     "bytes bound wins over a generous entries bound; both bounds at once evict by the larger need",
			setup:    []Entry{entryOf(1, "a", 5, d, 30), entryOf(1, "b", 5, d, 30), entryOf(2, "c", 5, d, 30)},
			event:    Fetched{Entry: entryOf(2, "d", 0, d, 30), IssuedAt: 5, Published: 5},
			cfg:      CacheConfig{MaxEntries: 3, MaxBytes: 100, MaxDeclBytes: 40},
			want:     CacheDecision{Entry: entryOf(2, "d", 5, d, 30), Store: true, Evict: []int{0}},
			wantKeys: []string{"1/b", "2/c", "2/d"},
		},
		{
			name:     "Bumped touches no entry and decides nothing",
			setup:    []Entry{a, b, c},
			event:    Bumped{DBID: 1, To: 6},
			cfg:      testConfig,
			want:     CacheDecision{},
			wantKeys: []string{"1/a", "1/b", "2/c"},
		},
		{
			name:     "Dropped removes that database's entries, Evict lists the indices in order",
			setup:    []Entry{a, c, b},
			event:    Dropped{DBID: 1},
			cfg:      testConfig,
			want:     CacheDecision{Evict: []int{0, 1}},
			wantKeys: []string{"2/c"},
		},
		{
			name:     "Dropped for a database with no entries changes nothing",
			setup:    []Entry{a},
			event:    Dropped{DBID: 9},
			cfg:      testConfig,
			want:     CacheDecision{},
			wantKeys: []string{"1/a"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := CacheState{Entries: append([]Entry(nil), tc.setup...)}
			got := state.Apply(tc.event, tc.cfg)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("decision = %+v, want %+v", got, tc.want)
			}
			if gotKeys := keysOf(&state); !reflect.DeepEqual(gotKeys, tc.wantKeys) {
				t.Fatalf("cache = %v, want %v", gotKeys, tc.wantKeys)
			}
		})
	}
}

// TestFlightIsRecordedAndCleared pins the single-flight record: FetchIssued remembers
// (db, name) -> At, and Fetched, FetchFailed and Dropped forget it.
// [Guard: never clear the record, so a finished fetch still looks in flight.]
func TestFlightIsRecordedAndCleared(t *testing.T) {
	var state CacheState
	if _, ok := state.InFlight(1, "a"); ok {
		t.Fatal("a zero cache reports a fetch in flight")
	}
	state.Apply(FetchIssued{DBID: 1, Name: "a", At: 5}, testConfig)
	if at, ok := state.InFlight(1, "a"); !ok || at != 5 {
		t.Fatalf("InFlight = (%d, %v), want (5, true)", at, ok)
	}
	if _, ok := state.InFlight(1, "other"); ok {
		t.Fatal("another name reports a flight")
	}
	state.Apply(Fetched{Entry: entryOf(1, "a", 0, declOf(7, 1, nil), 10), IssuedAt: 5, Published: 6}, testConfig)
	if _, ok := state.InFlight(1, "a"); ok {
		t.Fatal("a discarded fetch left its flight recorded")
	}

	state.Apply(FetchIssued{DBID: 1, Name: "a", At: 6}, testConfig)
	state.Apply(FetchFailed{DBID: 1, Name: "a"}, testConfig)
	if _, ok := state.InFlight(1, "a"); ok {
		t.Fatal("a failed fetch left its flight recorded")
	}

	state.Apply(FetchIssued{DBID: 1, Name: "a", At: 6}, testConfig)
	state.Apply(FetchIssued{DBID: 2, Name: "a", At: 6}, testConfig)
	state.Apply(Dropped{DBID: 1}, testConfig)
	if _, ok := state.InFlight(1, "a"); ok {
		t.Fatal("a dropped database kept its flight")
	}
	if _, ok := state.InFlight(2, "a"); !ok {
		t.Fatal("a drop of database 1 cleared database 2's flight")
	}
}

// TestDeclarationFetchStartedBeforeABumpIsDiscarded: a fetch issued at s whose completion
// finds the published version at s' != s is discarded, because storing it would tag a
// pre-bump declaration with a post-bump version (conv 20).
// [Guard: tag with the current version instead of the issue version, storing the raced fetch.]
func TestDeclarationFetchStartedBeforeABumpIsDiscarded(t *testing.T) {
	var state CacheState
	read := declOf(7, 1, nil)

	state.Apply(FetchIssued{DBID: 1, Name: "users", At: 5}, testConfig)
	state.Apply(Bumped{DBID: 1, To: 6}, testConfig)
	got := state.Apply(Fetched{Entry: entryOf(1, "users", 0, read, 10), IssuedAt: 5, Published: 6}, testConfig)

	if got.Store {
		t.Fatal("a fetch issued at 5 and completed at published 6 was stored")
	}
	if len(state.Entries) != 0 {
		t.Fatalf("cache = %v, want empty", keysOf(&state))
	}
	// The bucket is a miss again at the new version, never a hit on the pre-bump text.
	if again := state.Apply(Lookup{DBID: 1, Name: "users", Published: 6}, testConfig); again.Hit || !again.Fetch {
		t.Fatalf("lookup after a discarded fetch = %+v, want a fresh Fetch", again)
	}

	// A fetch issued after the bump, at 6, completes at 6 and is stored under 6.
	stored := fetchedAt(&state, entryOf(1, "users", 0, read, 10), 6, testConfig)
	if !stored.Store || stored.Entry.Seen != 6 {
		t.Fatalf("fetch issued at the new version = %+v, want stored with Seen 6", stored)
	}

	// A raced fetch must not overwrite a good entry either.
	before := cloneState(&state)
	state.Apply(Fetched{Entry: entryOf(1, "users", 0, declOf(7, 0, nil), 10), IssuedAt: 5, Published: 6}, testConfig)
	if !sameEntries(before.Entries, state.Entries) {
		t.Fatal("a raced fetch changed the cache")
	}
}

// TestEntryTaggedWithAnOlderVersionIsNeverServed: the comparison is the whole
// invalidation, so an entry whose tag differs from the published version is a miss.
// [Guard: compare with < instead of !=, or serve any entry that exists.]
func TestEntryTaggedWithAnOlderVersionIsNeverServed(t *testing.T) {
	var state CacheState
	fetchedAt(&state, entryOf(1, "users", 0, declOf(7, 1, nil), 10), 5, testConfig)

	if got := state.Apply(Lookup{DBID: 1, Name: "users", Published: 5}, testConfig); !got.Hit {
		t.Fatalf("lookup at the tag version = %+v, want Hit", got)
	}
	got := state.Apply(Lookup{DBID: 1, Name: "users", Published: 6}, testConfig)
	if got.Hit || !got.Fetch {
		t.Fatalf("lookup at a newer published version = %+v, want a miss with Fetch", got)
	}
	if len(state.Entries) != 0 {
		t.Fatalf("the stale entry stayed in the cache: %v", keysOf(&state))
	}

	// The comparison is equality, not order: a tag AHEAD of the published version is
	// just as wrong to serve.
	fetchedAt(&state, entryOf(1, "users", 0, declOf(7, 1, nil), 10), 5, testConfig)
	if got := state.Apply(Lookup{DBID: 1, Name: "users", Published: 4}, testConfig); got.Hit || !got.Fetch {
		t.Fatalf("lookup at a lower published version = %+v, want a miss with Fetch", got)
	}
}

// TestLookupHitMovesToMRU pins the LRU order: the hit entry is evicted last.
// [Guard: leave the entry where it was on a hit.]
func TestLookupHitMovesToMRU(t *testing.T) {
	var state CacheState
	d := declOf(7, 1, nil)
	for _, name := range []string{"a", "b", "c"} {
		fetchedAt(&state, entryOf(1, name, 0, d, 10), 5, testConfig)
	}
	state.Apply(Lookup{DBID: 1, Name: "a", Published: 5}, testConfig)
	got := fetchedAt(&state, entryOf(1, "d", 0, d, 10), 5, testConfig)

	if want := []int{0}; !reflect.DeepEqual(got.Evict, want) {
		t.Fatalf("Evict = %v, want %v", got.Evict, want)
	}
	if want := []string{"1/c", "1/a", "1/d"}; !reflect.DeepEqual(keysOf(&state), want) {
		t.Fatalf("cache = %v, want %v (b was the least recently used)", keysOf(&state), want)
	}
}

// TestDeclarationCacheIsBounded: a stream of inserts never leaves the cache over
// either bound, and the byte bound wins when the entry bound is generous.
// [Guard: bound by entries only.]
func TestDeclarationCacheIsBounded(t *testing.T) {
	d := declOf(7, 1, nil)

	t.Run("entries", func(t *testing.T) {
		var state CacheState
		cfg := CacheConfig{MaxEntries: 4, MaxBytes: 1 << 20, MaxDeclBytes: 1 << 10}
		for i := 0; i < 50; i++ {
			fetchedAt(&state, entryOf(1, fmt.Sprintf("b%d", i), 0, d, 8), 3, cfg)
			if len(state.Entries) > cfg.MaxEntries {
				t.Fatalf("after %d inserts the cache holds %d entries, bound is %d", i+1, len(state.Entries), cfg.MaxEntries)
			}
		}
		if len(state.Entries) != cfg.MaxEntries {
			t.Fatalf("cache holds %d entries, want exactly the bound %d", len(state.Entries), cfg.MaxEntries)
		}
	})

	t.Run("bytes win", func(t *testing.T) {
		var state CacheState
		cfg := CacheConfig{MaxEntries: 1000, MaxBytes: 100, MaxDeclBytes: 60}
		for i := 0; i < 50; i++ {
			fetchedAt(&state, entryOf(1, fmt.Sprintf("b%d", i), 0, d, int64(10+i%50)), 3, cfg)
			if got := totalSize(&state); got > cfg.MaxBytes {
				t.Fatalf("after %d inserts the cache holds %d bytes, bound is %d", i+1, got, cfg.MaxBytes)
			}
		}
		if len(state.Entries) == 0 {
			t.Fatal("the byte bound emptied the cache")
		}
	})
}

// TestNotADeclarationIsNeverCached: a miss, a not-found and a failed fetch leave no entry,
// however many times the key is looked up.
// [Guard: cache the miss.]
func TestNotADeclarationIsNeverCached(t *testing.T) {
	var state CacheState
	for i := 0; i < 3; i++ {
		got := state.Apply(Lookup{DBID: 1, Name: "ghost", Published: 5}, testConfig)
		if got.Hit || !got.Fetch {
			t.Fatalf("lookup %d of an unknown bucket = %+v, want Fetch and no Hit", i+1, got)
		}
		state.Apply(FetchIssued{DBID: 1, Name: "ghost", At: 5}, testConfig)
		state.Apply(FetchFailed{DBID: 1, Name: "ghost"}, testConfig)
		if len(state.Entries) != 0 {
			t.Fatalf("after lookup %d the cache holds %v, want nothing", i+1, keysOf(&state))
		}
	}
}

// TestDroppingDeclarationIsNeverStored: a dropping declaration is a refusal, never an
// entry, because a cached one would outlive the name (conv 20).
// [Guard: store it.]
func TestDroppingDeclarationIsNeverStored(t *testing.T) {
	var state CacheState
	dropping := Declaration{BucketID: 7, Version: 2, State: "dropping", Fields: tableOf(1, 2)}

	got := fetchedAt(&state, entryOf(1, "users", 0, dropping, 10), 5, testConfig)
	if got.Store || got.Counted {
		t.Fatalf("decision = %+v, want a plain discard", got)
	}
	if len(state.Entries) != 0 {
		t.Fatalf("cache = %v, want empty", keysOf(&state))
	}
	// A dropping fetch must not displace the good entry it replaces either; the stale
	// one is removed only by the lazy comparison, not by this fetch.
	active := entryOf(1, "users", 0, declOf(7, 1, nil), 10)
	fetchedAt(&state, active, 5, testConfig)
	fetchedAt(&state, entryOf(1, "users", 0, dropping, 10), 5, testConfig)
	if len(state.Entries) != 1 || state.Entries[0].Decl.State != "active" {
		t.Fatalf("cache = %+v, want the active entry untouched", state.Entries)
	}
}

// TestBumpTouchesNoEntry: a bump is O(1) per database and invalidates lazily; no entry
// is read, moved or removed, and each entry goes stale only when it is next looked up.
// [Guard: invalidate eagerly.]
func TestBumpTouchesNoEntry(t *testing.T) {
	var state CacheState
	d := declOf(7, 1, &Pending{Version: 6, Table: tableOf(1, 2, 3)})
	fetchedAt(&state, entryOf(1, "a", 0, d, 10), 5, testConfig)
	fetchedAt(&state, entryOf(1, "b", 0, d, 10), 5, testConfig)
	fetchedAt(&state, entryOf(2, "c", 0, d, 10), 5, testConfig)
	before := cloneState(&state)

	got := state.Apply(Bumped{DBID: 1, To: 6}, testConfig)
	if !reflect.DeepEqual(got, CacheDecision{}) {
		t.Fatalf("Bumped decided %+v, want nothing", got)
	}
	if !sameEntries(before.Entries, state.Entries) {
		t.Fatalf("Bumped changed the cache: %v -> %v", keysOf(&before), keysOf(&state))
	}

	// Staleness shows up only at the lookup: database 1 misses, database 2 still hits.
	if got := state.Apply(Lookup{DBID: 1, Name: "a", Published: 6}, testConfig); got.Hit || !got.Fetch {
		t.Fatalf("lookup in the bumped database = %+v, want a miss", got)
	}
	if got := state.Apply(Lookup{DBID: 2, Name: "c", Published: 5}, testConfig); !got.Hit {
		t.Fatalf("lookup in the other database = %+v, want Hit", got)
	}
}

// TestPendingTableIsEffectiveOnlyWhenPublished: field 8 is the table in force only when
// pending.Version <= published, and then its version is the effective version.
// [Guard: use field 8 whenever it is present.]
func TestPendingTableIsEffectiveOnlyWhenPublished(t *testing.T) {
	pending := &Pending{Version: 4, Table: tableOf(1, 2, 3)}
	d := declOf(7, 2, pending)

	cases := []struct {
		published   int64
		wantPending bool
	}{
		{published: 1, wantPending: false},
		{published: 3, wantPending: false}, // one below: the bump has not published it
		{published: 4, wantPending: true},  // exactly at: <= holds
		{published: 9, wantPending: true},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("published=%d", tc.published), func(t *testing.T) {
			table, version := EffectiveTable(d, tc.published)
			wantTable, wantVersion := d.Fields, d.Version
			if tc.wantPending {
				wantTable, wantVersion = pending.Table, pending.Version
			}
			if !reflect.DeepEqual(table, wantTable) || version != wantVersion {
				t.Fatalf("EffectiveTable = (%+v, %d), want (%+v, %d)", table, version, wantTable, wantVersion)
			}

			eff := Effective(d, tc.published)
			if !reflect.DeepEqual(eff.Fields, wantTable) || eff.Version != wantVersion {
				t.Fatalf("Effective = (%+v, %d), want (%+v, %d)", eff.Fields, eff.Version, wantTable, wantVersion)
			}
			if eff.Pending != nil {
				t.Fatal("an effective declaration still carries a pending table")
			}
			if eff.BucketID != d.BucketID || eff.State != d.State {
				t.Fatalf("Effective changed the id or state: %+v", eff)
			}
			if again := Effective(eff, tc.published); !reflect.DeepEqual(again, eff) {
				t.Fatalf("Effective is not idempotent: %+v then %+v", eff, again)
			}
		})
	}

	// The cache stores the declaration as read, pending included, and a reader asks
	// Effective: there is one rule, and a read never folds.
	var state CacheState
	fetchedAt(&state, entryOf(1, "users", 0, d, 10), 2, testConfig)
	hit := state.Apply(Lookup{DBID: 1, Name: "users", Published: 2}, testConfig)
	if !hit.Hit || hit.Entry.Decl.Pending == nil || !reflect.DeepEqual(*hit.Entry.Decl.Pending, *pending) {
		t.Fatalf("the cached declaration lost its pending table: %+v", hit.Entry.Decl)
	}
	if table, version := EffectiveTable(hit.Entry.Decl, 2); !reflect.DeepEqual(table, d.Fields) || version != 2 {
		t.Fatalf("a pending table above published 2 was effective: (%+v, %d)", table, version)
	}
}

// TestFoldBringsAPendingTableIntoFieldFive: field 3 := pending.version, field 5 :=
// pending.table, field 8 deleted; it is a write-time operation and never runs on a read.
// [Guard: fold without its version, or fold at read time.]
func TestFoldBringsAPendingTableIntoFieldFive(t *testing.T) {
	pending := &Pending{Version: 4, Table: tableOf(1, 2, 3)}
	d := declOf(7, 2, pending)
	snapshot := declOf(7, 2, &Pending{Version: 4, Table: tableOf(1, 2, 3)})

	folded := Fold(d)
	if folded.Version != 4 {
		t.Fatalf("folded Version = %d, want the pending version 4", folded.Version)
	}
	if !reflect.DeepEqual(folded.Fields, pending.Table) {
		t.Fatalf("folded Fields = %+v, want %+v", folded.Fields, pending.Table)
	}
	if folded.Pending != nil {
		t.Fatal("a folded declaration still carries field 8")
	}
	if folded.BucketID != 7 || folded.State != "active" {
		t.Fatalf("Fold changed the id or state: %+v", folded)
	}
	if !reflect.DeepEqual(d, snapshot) {
		t.Fatalf("Fold changed its argument: %+v", d)
	}

	// A published pending reads the same before and after the fold.
	if got, want := Effective(folded, 4), Effective(d, 4); !reflect.DeepEqual(got, want) {
		t.Fatalf("Effective(Fold(d)) = %+v, Effective(d) = %+v", got, want)
	}

	// No pending: a fold is the identity.
	plain := declOf(7, 2, nil)
	if got := Fold(plain); !reflect.DeepEqual(got, plain) {
		t.Fatalf("Fold of a declaration with no pending table = %+v, want it unchanged", got)
	}

	// Reading never folds: neither Effective nor a cache hit rewrites the stored declaration.
	Effective(d, 9)
	var state CacheState
	fetchedAt(&state, entryOf(1, "users", 0, d, 10), 9, testConfig)
	hit := state.Apply(Lookup{DBID: 1, Name: "users", Published: 9}, testConfig)
	if !hit.Hit || !reflect.DeepEqual(hit.Entry.Decl, snapshot) {
		t.Fatalf("a read folded the declaration: %+v", hit.Entry.Decl)
	}
	if !reflect.DeepEqual(d, snapshot) {
		t.Fatalf("Effective changed its argument: %+v", d)
	}
}

// TestColdOwnerFetchIsNotARefusal: an owner that has not cached a forwarded write's
// declaration runs the same Lookup, Fetch, Fetched path as any node and then serves it.
// The machine has no refusal to give: a cold miss decides Fetch and nothing else.
// [Guard: refuse (decide no Fetch) on a cold cache.]
func TestColdOwnerFetchIsNotARefusal(t *testing.T) {
	var state CacheState
	cold := state.Apply(Lookup{DBID: 1, Name: "users", Published: 5}, testConfig)
	if !reflect.DeepEqual(cold, CacheDecision{Fetch: true}) {
		t.Fatalf("cold lookup = %+v, want exactly Fetch", cold)
	}
	state.Apply(FetchIssued{DBID: 1, Name: "users", At: 5}, testConfig)
	filled := state.Apply(Fetched{Entry: entryOf(1, "users", 0, declOf(7, 1, nil), 10), IssuedAt: 5, Published: 5}, testConfig)
	if !filled.Store {
		t.Fatalf("the cold owner's fetch = %+v, want Store", filled)
	}
	warm := state.Apply(Lookup{DBID: 1, Name: "users", Published: 5}, testConfig)
	if !warm.Hit || warm.Fetch {
		t.Fatalf("lookup after the fill = %+v, want Hit", warm)
	}
}

// TestSystemDeclarationHasAllEightFields: the compiled _buckets declaration carries
// ids 1 to 8 (_id_ included; conv 19's "seven" excludes _id_), version 0, state active.
// [Guard: drop _id_ so only seven fields remain.]
func TestSystemDeclarationHasAllEightFields(t *testing.T) {
	if SystemBucketID != 0xFFFFFFFFFFFFFFFF {
		t.Fatalf("SystemBucketID = %#x, want all ones", uint64(SystemBucketID))
	}
	d := SystemDeclaration()
	if d.BucketID != SystemBucketID || d.Version != 0 || d.State != "active" || d.Pending != nil {
		t.Fatalf("SystemDeclaration = %+v, want the system id, version 0, active, no pending", d)
	}
	want := []FieldEntry{
		{ID: 1, Type: "any", Name: "_id_"},
		{ID: 2, Type: "binary", Name: "bucket_id"},
		{ID: 3, Type: "int", Name: "version"},
		{ID: 4, Type: "string", Name: "state"},
		{ID: 5, Type: "binary", Name: "fields"},
		{ID: 6, Type: "binary", Name: "swept"},
		{ID: 7, Type: "string", Name: "name"},
		{ID: 8, Type: "binary", Name: "pending"},
	}
	if !reflect.DeepEqual(d.Fields.Entries, want) {
		t.Fatalf("fields = %+v, want %+v", d.Fields.Entries, want)
	}
	if d.Fields.NextFieldID != 9 || len(d.Fields.Retired) != 0 {
		t.Fatalf("table = %+v, want next_field_id 9 and nothing retired", d.Fields)
	}
	// Both document families are admitted through the one table, and the effective rule
	// leaves it alone at every published version, version 0 being <= any s.
	for _, published := range []int64{0, 1, 99} {
		if table, version := EffectiveTable(d, published); !reflect.DeepEqual(table, d.Fields) || version != 0 {
			t.Fatalf("EffectiveTable at %d = (%+v, %d), want the built-in table at version 0", published, table, version)
		}
	}
}

// TestSystemDeclarationIsFreshEachCall: a caller that edits the value it got cannot
// change the next caller's built-in declaration.
// [Guard: return one shared package-level table.]
func TestSystemDeclarationIsFreshEachCall(t *testing.T) {
	first := SystemDeclaration()
	first.Fields.Entries[0].Name = "mutated"
	first.State = "dropping"
	second := SystemDeclaration()
	if second.Fields.Entries[0].Name != "_id_" || second.State != "active" {
		t.Fatalf("a second SystemDeclaration saw the first caller's edit: %+v", second)
	}
}

// ---------------------------------------------------------------------------
// The model test: 20 fixed seeds of random events against a reference model.
// ---------------------------------------------------------------------------

type refKey struct {
	dbID uint64
	name string
}

type refSlot struct {
	entry Entry
	used  int
}

// refCache is the trivially correct reference: a map and a logical clock, no ordered
// slice. The least recently used entry is the one with the smallest clock reading.
type refCache struct {
	slots   map[refKey]*refSlot
	clock   int
	flights map[refKey]int64
}

func newRefCache() *refCache {
	return &refCache{slots: map[refKey]*refSlot{}, flights: map[refKey]int64{}}
}

func (r *refCache) tick() int {
	r.clock++
	return r.clock
}

// lruOrder returns the keys oldest first.
func (r *refCache) lruOrder() []refKey {
	keys := make([]refKey, 0, len(r.slots))
	for key := range r.slots {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return r.slots[keys[i]].used < r.slots[keys[j]].used })
	return keys
}

func (r *refCache) bytes() int64 {
	var sum int64
	for _, slot := range r.slots {
		sum += slot.entry.Size
	}
	return sum
}

// apply returns the decision without Evict, and the evicted keys in removal order.
func (r *refCache) apply(ev CacheEvent, cfg CacheConfig) (CacheDecision, []refKey) {
	switch ev := ev.(type) {
	case Lookup:
		key := refKey{ev.DBID, ev.Name}
		slot, ok := r.slots[key]
		if !ok {
			return CacheDecision{Fetch: true}, nil
		}
		if slot.entry.Seen != ev.Published {
			delete(r.slots, key)
			return CacheDecision{Fetch: true}, nil
		}
		slot.used = r.tick()
		return CacheDecision{Entry: slot.entry, Hit: true}, nil
	case FetchIssued:
		r.flights[refKey{ev.DBID, ev.Name}] = ev.At
		return CacheDecision{}, nil
	case FetchFailed:
		delete(r.flights, refKey{ev.DBID, ev.Name})
		return CacheDecision{}, nil
	case Fetched:
		key := refKey{ev.Entry.DBID, ev.Entry.Name}
		delete(r.flights, key)
		switch {
		case ev.IssuedAt != ev.Published:
			return CacheDecision{}, nil
		case ev.Entry.Decl.State == "dropping":
			return CacheDecision{}, nil
		case ev.Entry.Size > cfg.MaxDeclBytes || ev.Entry.Size > cfg.MaxBytes:
			return CacheDecision{Counted: true}, nil
		case cfg.MaxEntries < 1:
			return CacheDecision{}, nil
		}
		stored := ev.Entry
		stored.Seen = ev.IssuedAt
		r.slots[key] = &refSlot{entry: stored, used: r.tick()}
		var evicted []refKey
		for len(r.slots) > cfg.MaxEntries || r.bytes() > cfg.MaxBytes {
			victim := r.lruOrder()[0]
			delete(r.slots, victim)
			evicted = append(evicted, victim)
		}
		return CacheDecision{Entry: stored, Store: true}, evicted
	case Bumped:
		return CacheDecision{}, nil
	case Dropped:
		var evicted []refKey
		for _, key := range r.lruOrder() {
			if key.dbID == ev.DBID {
				delete(r.slots, key)
				evicted = append(evicted, key)
			}
		}
		for key := range r.flights {
			if key.dbID == ev.DBID {
				delete(r.flights, key)
			}
		}
		return CacheDecision{}, evicted
	default:
		panic(fmt.Sprintf("test model: unhandled event %T", ev))
	}
}

// evictedKeys replays Evict on a copy of the pre-event entries: each index is valid at
// the moment its removal happens, so they are applied in order.
func evictedKeys(pre []Entry, evict []int) []refKey {
	work := append([]Entry(nil), pre...)
	var keys []refKey
	for _, index := range evict {
		keys = append(keys, refKey{work[index].DBID, work[index].Name})
		work = append(work[:index], work[index+1:]...)
	}
	return keys
}

// world is the simulated database the fetches read: the declarations, and for every
// database the committed and the published schema_version, which are not the same number.
type world struct {
	committed, published map[uint64]int64
	decls                map[refKey]Declaration
	nextBucketID         uint64
	inFlight             []flight
}

type flight struct {
	key  refKey
	at   int64
	decl Declaration
	size int64
	miss bool // the declaration did not exist when the fetch read it
}

var modelNames = []string{"a", "b", "c", "d", "e", "f"}

const modelDatabases = 3

// modelConfig is small enough that every bound, the entry count, the byte total and the
// per-declaration limit, is hit within a few steps.
var modelConfig = CacheConfig{MaxEntries: 4, MaxBytes: 70, MaxDeclBytes: 30}

// coverage counts how often each row of the decision table fired across all seeds, so a
// generator that never reaches a row cannot make the model test vacuously pass.
type coverage map[string]int

func TestDeclCacheMatchesReferenceModel(t *testing.T) {
	seen := coverage{}
	for seed := int64(1); seed <= 20; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runModelSeed(t, seed, seen)
		})
	}
	for _, row := range []string{
		"hit", "stale-lookup", "absent-lookup", "raced-discard", "dropping-discard", "oversize-counted",
		"stored", "replaced", "entry-bound-evict", "byte-bound-evict", "bump", "dropped-evict", "failed-fetch",
		"pending-published", "pending-inert",
	} {
		if seen[row] == 0 {
			t.Errorf("the 20 seeds never exercised %q", row)
		}
	}
}

func runModelSeed(t *testing.T, seed int64, seen coverage) {
	rng := rand.New(rand.NewSource(seed))
	w := &world{
		committed:    map[uint64]int64{},
		published:    map[uint64]int64{},
		decls:        map[refKey]Declaration{},
		nextBucketID: 100,
	}
	for db := uint64(1); db <= modelDatabases; db++ {
		w.committed[db], w.published[db] = 1, 1
	}
	var state CacheState
	ref := newRefCache()

	randomKey := func() refKey {
		return refKey{uint64(1 + rng.Intn(modelDatabases)), modelNames[rng.Intn(len(modelNames))]}
	}

	// step applies one event to both the machine and the reference, compares every output,
	// and then asserts the invariants that must hold after any event.
	step := func(i int, ev CacheEvent) CacheDecision {
		pre := append([]Entry(nil), state.Entries...)
		got := state.Apply(ev, modelConfig)
		want, wantEvicted := ref.apply(ev, modelConfig)

		replay := pre
		if got.Store {
			// Evict indices count from the list after the insert: the replaced entry is gone
			// and the new one is last.
			replay = nil
			for _, e := range pre {
				if e.DBID != got.Entry.DBID || e.Name != got.Entry.Name {
					replay = append(replay, e)
				}
			}
			replay = append(replay, got.Entry)
		}
		gotEvicted := evictedKeys(replay, got.Evict)
		if !reflect.DeepEqual(gotEvicted, wantEvicted) {
			t.Fatalf("step %d %T %+v: evicted %v, reference evicted %v", i, ev, ev, gotEvicted, wantEvicted)
		}
		withoutEvict := got
		withoutEvict.Evict = nil
		if !reflect.DeepEqual(withoutEvict, want) {
			t.Fatalf("step %d %T %+v: decision %+v, reference %+v", i, ev, ev, got, want)
		}
		wantOrder := ref.lruOrder()
		if len(state.Entries) != len(wantOrder) {
			t.Fatalf("step %d: cache holds %v, reference holds %v", i, keysOf(&state), wantOrder)
		}
		for index, key := range wantOrder {
			if !reflect.DeepEqual(state.Entries[index], ref.slots[key].entry) {
				t.Fatalf("step %d: cache entry %d = %+v, reference %+v", i, index, state.Entries[index], ref.slots[key].entry)
			}
		}
		for db := uint64(1); db <= modelDatabases; db++ {
			for _, name := range modelNames {
				gotAt, gotOK := state.InFlight(db, name)
				wantAt, wantOK := ref.flights[refKey{db, name}]
				if gotAt != wantAt || gotOK != wantOK {
					t.Fatalf("step %d: InFlight(%d,%s) = (%d,%v), reference (%d,%v)", i, db, name, gotAt, gotOK, wantAt, wantOK)
				}
			}
		}

		// Invariants, stated directly rather than through the reference.
		if len(state.Entries) > modelConfig.MaxEntries {
			t.Fatalf("step %d: %d entries, bound %d", i, len(state.Entries), modelConfig.MaxEntries)
		}
		if got := totalSize(&state); got > modelConfig.MaxBytes {
			t.Fatalf("step %d: %d bytes, bound %d", i, got, modelConfig.MaxBytes)
		}
		for _, e := range state.Entries {
			if e.Seen > w.published[e.DBID] {
				t.Fatalf("step %d: entry %d/%s tagged %d, ahead of published %d", i, e.DBID, e.Name, e.Seen, w.published[e.DBID])
			}
			if e.Decl.State == "dropping" {
				t.Fatalf("step %d: entry %d/%s is a dropping declaration", i, e.DBID, e.Name)
			}
			if e.Decl.Pending != nil {
				assertPendingRule(t, i, e.Decl, w.published[e.DBID], seen)
				assertPendingRule(t, i, e.Decl, w.published[e.DBID]-1, seen)
			}
		}
		return got
	}

	for i := 0; i < 400; i++ {
		switch roll := rng.Intn(100); {
		case roll < 32: // lookup, and issue a fetch for a miss most of the time
			key := randomKey()
			published := w.published[key.dbID]
			pre := cloneState(&state)
			got := step(i, Lookup{DBID: key.dbID, Name: key.name, Published: published})
			switch {
			case got.Hit:
				seen["hit"]++
				if got.Entry.Seen != published {
					t.Fatalf("step %d: served %d/%s tagged %d at published %d", i, key.dbID, key.name, got.Entry.Seen, published)
				}
			case !got.Fetch:
				t.Fatalf("step %d: a lookup decided neither Hit nor Fetch: %+v", i, got)
			default:
				if hadEntry(&pre, key) {
					seen["stale-lookup"]++
				} else {
					seen["absent-lookup"]++
				}
				if hadEntry(&state, key) {
					t.Fatalf("step %d: a miss left %d/%s in the cache", i, key.dbID, key.name)
				}
				if rng.Intn(10) < 8 {
					w.issue(rng, key, published)
					step(i, FetchIssued{DBID: key.dbID, Name: key.name, At: published})
				}
			}
		case roll < 58: // a pending fetch completes
			if len(w.inFlight) == 0 {
				continue
			}
			index := rng.Intn(len(w.inFlight))
			f := w.inFlight[index]
			w.inFlight = append(w.inFlight[:index], w.inFlight[index+1:]...)
			if f.miss {
				before := cloneState(&state)
				step(i, FetchFailed{DBID: f.key.dbID, Name: f.key.name})
				seen["failed-fetch"]++
				if !sameEntries(before.Entries, state.Entries) {
					t.Fatalf("step %d: a not-found fetch changed the cache", i)
				}
				continue
			}
			published := w.published[f.key.dbID]
			before := cloneState(&state)
			existed := hadEntry(&before, f.key)
			got := step(i, Fetched{
				Entry:    Entry{DBID: f.key.dbID, Name: f.key.name, Decl: f.decl, Size: f.size},
				IssuedAt: f.at, Published: published,
			})
			classifyFetch(t, fetchCase{step: i, got: got, f: f, published: published, before: before, after: state, existed: existed}, seen)
		case roll < 66: // an alter writes a pending table at committed+1
			key := randomKey()
			if decl, ok := w.decls[key]; ok && decl.State == "active" {
				decl.Pending = &Pending{Version: w.committed[key.dbID] + 1, Table: tableOf(1, 2, uint16(3+rng.Intn(5)))}
				w.decls[key] = decl
			}
		case roll < 72: // a bump commits, publication lags
			w.committed[uint64(1+rng.Intn(modelDatabases))]++
		case roll < 82: // a committed bump is published
			db := uint64(1 + rng.Intn(modelDatabases))
			if w.published[db] < w.committed[db] {
				w.published[db]++
				step(i, Bumped{DBID: db, To: w.published[db]})
				seen["bump"]++
			}
		case roll < 88: // create or drop a bucket in the world
			key := randomKey()
			if decl, ok := w.decls[key]; ok && rng.Intn(2) == 0 {
				decl.State = "dropping"
				w.decls[key] = decl
			} else {
				w.nextBucketID++
				w.decls[key] = declOf(w.nextBucketID, w.published[key.dbID], nil)
			}
		case roll < 94: // cache pressure: a large declaration fetched and completed at once
			key := randomKey()
			if decl, ok := w.decls[key]; ok {
				published := w.published[key.dbID]
				step(i, FetchIssued{DBID: key.dbID, Name: key.name, At: published})
				before := cloneState(&state)
				existed := hadEntry(&before, key)
				f := flight{key: key, at: published, decl: decl, size: int64(20 + rng.Intn(15))}
				got := step(i, Fetched{
					Entry:    Entry{DBID: key.dbID, Name: key.name, Decl: f.decl, Size: f.size},
					IssuedAt: published, Published: published,
				})
				classifyFetch(t, fetchCase{step: i, got: got, f: f, published: published, before: before, after: state, existed: existed}, seen)
			}
		default: // drop a database: the final bump, then the eager removal
			db := uint64(1 + rng.Intn(modelDatabases))
			w.committed[db]++
			w.published[db] = w.committed[db]
			step(i, Bumped{DBID: db, To: w.published[db]})
			before := cloneState(&state)
			got := step(i, Dropped{DBID: db})
			for _, e := range state.Entries {
				if e.DBID == db {
					t.Fatalf("step %d: dropped database %d still has entry %s", i, db, e.Name)
				}
			}
			if len(got.Evict) > 0 || countDB(&before, db) > 0 {
				seen["dropped-evict"]++
			}
			for key := range w.decls {
				if key.dbID == db {
					delete(w.decls, key)
				}
			}
		}
	}
}

// sameEntries compares two caches; a nil slice and an empty one are the same cache.
func sameEntries(a, b []Entry) bool {
	return len(a) == 0 && len(b) == 0 || reflect.DeepEqual(a, b)
}

func hadEntry(c *CacheState, key refKey) bool {
	for _, e := range c.Entries {
		if e.DBID == key.dbID && e.Name == key.name {
			return true
		}
	}
	return false
}

func countDB(c *CacheState, dbID uint64) int {
	count := 0
	for _, e := range c.Entries {
		if e.DBID == dbID {
			count++
		}
	}
	return count
}

// issue starts a simulated fetch: it reads the declaration as it is now, so a bump or an
// alter that lands before completion is exactly the race the cache must survive.
func (w *world) issue(rng *rand.Rand, key refKey, at int64) {
	decl, ok := w.decls[key]
	if !ok && rng.Intn(3) != 0 {
		w.nextBucketID++
		decl = declOf(w.nextBucketID, at, nil)
		w.decls[key] = decl
		ok = true
	}
	w.inFlight = append(w.inFlight, flight{key: key, at: at, decl: decl, size: int64(5 + rng.Intn(40)), miss: !ok})
}

// fetchCase is one completed fetch and the cache on either side of it.
type fetchCase struct {
	step          int
	got           CacheDecision
	f             flight
	published     int64
	before, after CacheState
	existed       bool
}

// classifyFetch asserts what a completed fetch must have decided, from the rules in the
// brief alone, and counts the table row that fired.
func classifyFetch(t *testing.T, c fetchCase, seen coverage) {
	t.Helper()
	step, got, f, published := c.step, c.got, c.f, c.published
	before, after, existed := c.before, c.after, c.existed
	switch {
	case f.at != published:
		seen["raced-discard"]++
		if got.Store || got.Counted || !sameEntries(before.Entries, after.Entries) {
			t.Fatalf("step %d: a fetch issued at %d and completed at %d was not discarded: %+v", step, f.at, published, got)
		}
	case f.decl.State == "dropping":
		seen["dropping-discard"]++
		if got.Store || !sameEntries(before.Entries, after.Entries) {
			t.Fatalf("step %d: a dropping declaration was stored: %+v", step, got)
		}
	case f.size > modelConfig.MaxDeclBytes:
		seen["oversize-counted"]++
		if got.Store || !got.Counted || !sameEntries(before.Entries, after.Entries) {
			t.Fatalf("step %d: an oversize declaration decided %+v", step, got)
		}
	default:
		seen["stored"]++
		if !got.Store || got.Entry.Seen != f.at {
			t.Fatalf("step %d: a current fetch decided %+v, want Store tagged %d", step, got, f.at)
		}
		if existed {
			seen["replaced"]++
		}
		if len(before.Entries)+boolInt(!existed) > modelConfig.MaxEntries {
			seen["entry-bound-evict"]++
		}
		if totalSize(&before)+f.size-replacedSize(&before, f.key) > modelConfig.MaxBytes {
			seen["byte-bound-evict"]++
		}
		if len(got.Evict) == 0 && (len(before.Entries)+boolInt(!existed) > modelConfig.MaxEntries) {
			t.Fatalf("step %d: the cache went over its entry bound without evicting", step)
		}
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func replacedSize(c *CacheState, key refKey) int64 {
	for _, e := range c.Entries {
		if e.DBID == key.dbID && e.Name == key.name {
			return e.Size
		}
	}
	return 0
}

// assertPendingRule states the effective-table rule independently of Effective: the
// pending table is in force exactly when its version has been published.
func assertPendingRule(t *testing.T, step int, d Declaration, published int64, seen coverage) {
	t.Helper()
	table, version := EffectiveTable(d, published)
	inForce := reflect.DeepEqual(table, d.Pending.Table) && version == d.Pending.Version
	if want := d.Pending.Version <= published; inForce != want {
		t.Fatalf("step %d: pending v%d at published %d: in force = %v, want %v", step, d.Pending.Version, published, inForce, want)
	}
	if d.Pending.Version <= published {
		seen["pending-published"]++
	} else {
		seen["pending-inert"]++
	}
}
