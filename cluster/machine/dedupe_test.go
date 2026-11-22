package machine

import (
	"bytes"
	"encoding/binary"
	"math"
	"reflect"
	"testing"
)

var dedupeCfg = WindowConfig{Window: 8192, MaxResult: 1024}

// dedupeKey is the dedupe key of the n-th distinct request.
func dedupeKey(n int) DedupeKey {
	var k DedupeKey
	binary.BigEndian.PutUint64(k.RequestID[:8], uint64(n))
	k.RequestID[15] = 0xDD
	return k
}

// dedupeEntry builds an entry for a window-age test.
func dedupeEntry(n int, ordinal uint64) DedupeEntry {
	return DedupeEntry{Key: dedupeKey(n), Pos: pos(1, ordinal), Ordinal: ordinal}
}

// dedupeWrite applies one write and returns the entry the decision asks to store.
func dedupeWrite(t *testing.T, w *Window, cfg WindowConfig, ev DedupeWrite) *DedupeEntry {
	t.Helper()
	d := w.Apply(ev, cfg)
	if !d.Store || d.Write == nil || d.Refuse {
		t.Fatalf("write %+v: decision %+v, want Store with an entry", ev, d)
	}
	return d.Write
}

// TestDedupeWindowIsAPositionSpan: the first of 8193 writes is deduped while
// fewer than dedupe_window records have been applied since it, and applied
// again at the 8193rd. Positions are spread out on purpose, so a window
// measured in seqs or in entry counts instead of applied records disagrees.
// Guard: make the window unbounded, or measure it in position seq.
func TestDedupeWindowIsAPositionSpan(t *testing.T) {
	var w Window
	first := dedupeWrite(t, &w, dedupeCfg, DedupeWrite{Key: dedupeKey(1), Pos: pos(1, 1000)})
	if first.Ordinal != 1 {
		t.Fatalf("first write ordinal = %d, want 1", first.Ordinal)
	}
	for i := 2; i <= 8192; i++ {
		dedupeWrite(t, &w, dedupeCfg, DedupeWrite{Key: dedupeKey(i), Pos: pos(1, uint64(i)*1000)})
	}
	inside := w.Apply(LookupGiven{Entry: first}, dedupeCfg)
	if !inside.Live || inside.Recorded == nil || inside.Recorded.Pos != pos(1, 1000) {
		t.Fatalf("after 8192 writes the first is at age 8191 and must be live, got %+v", inside)
	}
	dedupeWrite(t, &w, dedupeCfg, DedupeWrite{Key: dedupeKey(8193), Pos: pos(1, 8193000)})
	outside := w.Apply(LookupGiven{Entry: first}, dedupeCfg)
	if outside.Live || outside.Recorded != nil || outside.Store {
		t.Fatalf("after 8193 writes the first is at age 8192 and must be dead, got %+v", outside)
	}
}

// TestAnEpochChangeDoesNotEndTheWindow: entries written under four different
// epochs are all live at the same ordinal, whatever epoch the owner is in now.
// The window never reads Pos.Epoch, which is what lets a promoted owner dedupe.
// Guard: call an entry dead when its epoch differs from the latest write's.
func TestAnEpochChangeDoesNotEndTheWindow(t *testing.T) {
	var w Window
	var written []*DedupeEntry
	for epoch := uint64(1); epoch <= 4; epoch++ {
		written = append(written, dedupeWrite(t, &w, dedupeCfg, DedupeWrite{Key: dedupeKey(int(epoch)), Pos: pos(epoch, 1)}))
	}
	for _, e := range written {
		d := w.Apply(LookupGiven{Entry: e}, dedupeCfg)
		if !d.Live || d.Recorded.Pos.Epoch != e.Pos.Epoch {
			t.Fatalf("entry written at epoch %d must be live after four epochs, got %+v", e.Pos.Epoch, d)
		}
	}
}

// TestAPromotedCopyDedupesARetriedPop: write path test 2 and 24 in miniature.
// The owner and a copy apply the same pop; the owner dies; the retry reaches
// the copy at a later epoch and gets the recorded element and position; a copy
// seeded from the owner's snapshot and cut answers the same.
// Guard: drop the result from the stored entry, or end the window at an epoch.
func TestAPromotedCopyDedupesARetriedPop(t *testing.T) {
	var owner, copyWin Window
	pop := DedupeWrite{Key: dedupeKey(7), Pos: pos(3, 7), Result: []byte("element-1")}
	ownerEntry := dedupeWrite(t, &owner, dedupeCfg, pop)
	copyEntry := dedupeWrite(t, &copyWin, dedupeCfg, pop)
	if !reflect.DeepEqual(ownerEntry, copyEntry) || owner.Ordinal != copyWin.Ordinal {
		t.Fatalf("owner and copy apply the same record: entries %+v vs %+v, ordinals %d vs %d",
			ownerEntry, copyEntry, owner.Ordinal, copyWin.Ordinal)
	}
	retry := copyWin.Apply(LookupGiven{Entry: copyEntry}, dedupeCfg)
	if !retry.Live || retry.Store || retry.Write != nil || retry.Refuse {
		t.Fatalf("a retried pop at the promoted copy must be answered and apply nothing, got %+v", retry)
	}
	if string(retry.Recorded.Result) != "element-1" || retry.Recorded.Pos != pos(3, 7) {
		t.Fatalf("recorded answer = %+v, want element-1 at 3:7", retry.Recorded)
	}

	var seeded Window
	cutAt := owner.Ordinal
	if d := seeded.Apply(OrdinalCut{Ordinal: cutAt}, dedupeCfg); !reflect.DeepEqual(d, WindowDecision{}) {
		t.Fatalf("a seed cut asks for nothing, got %+v", d)
	}
	snapshotEntry := *ownerEntry
	if d := seeded.Apply(LookupGiven{Entry: &snapshotEntry}, dedupeCfg); !d.Live || string(d.Recorded.Result) != "element-1" {
		t.Fatalf("a seeded copy must dedupe from the snapshot's entry, got %+v", d)
	}
}

// TestOversizeResultIsRefusedNotStored: a result over the cap is refused with
// nothing stored and nothing counted; one at the cap is stored whole.
// Guard: truncate the result to the cap and store it.
func TestOversizeResultIsRefusedNotStored(t *testing.T) {
	var w Window
	over := bytes.Repeat([]byte{'x'}, 1025)
	before := w
	d := w.Apply(DedupeWrite{Key: dedupeKey(1), Pos: pos(1, 1), Result: over}, dedupeCfg)
	if !d.Refuse || d.Store || d.Write != nil || d.Live {
		t.Fatalf("1025-byte result: %+v, want Refuse only", d)
	}
	if w != before {
		t.Fatalf("a refusal changed state: %+v -> %+v", before, w)
	}
	atCap := bytes.Repeat([]byte{'y'}, 1024)
	e := dedupeWrite(t, &w, dedupeCfg, DedupeWrite{Key: dedupeKey(2), Pos: pos(1, 1), Result: atCap})
	if !bytes.Equal(e.Result, atCap) || w.Ordinal != 1 {
		t.Fatalf("1024-byte result must be stored whole, got %d bytes at ordinal %d", len(e.Result), w.Ordinal)
	}
	if d := (&Window{}).Apply(DedupeWrite{Key: dedupeKey(3), Pos: pos(1, 1), Result: []byte{1}}, WindowConfig{Window: 8, MaxResult: 0}); !d.Refuse {
		t.Fatalf("a cap of 0 refuses any result, got %+v", d)
	}
}

// TestStoreIsRequestedForEveryAppliedWrite: a push has no result and still gets
// its entry, in the write's own batch, because a retried push must not append
// twice.
// Guard: store only when the write returns a value.
func TestStoreIsRequestedForEveryAppliedWrite(t *testing.T) {
	var w Window
	for i, result := range [][]byte{nil, {}, []byte("a"), nil} {
		want := DedupeWrite{Key: dedupeKey(i), Pos: pos(2, uint64(i)+1), Result: result}
		d := w.Apply(want, dedupeCfg)
		if !d.Store || d.Write == nil || d.Refuse || d.Live {
			t.Fatalf("write %d with result %q: %+v, want Store", i, result, d)
		}
		if d.Write.Key != want.Key || d.Write.Pos != want.Pos || d.Write.Ordinal != uint64(i)+1 || !bytes.Equal(d.Write.Result, result) {
			t.Fatalf("write %d stored %+v", i, d.Write)
		}
	}
}

// TestSweepDeletesOnlyOutsideTheWindow: dead entries go, at most Budget of them,
// in the order given; live and future entries stay; nothing changes state.
// Guard: delete a live entry, or ignore Budget and sweep the whole batch.
func TestSweepDeletesOnlyOutsideTheWindow(t *testing.T) {
	cfg := WindowConfig{Window: 10, MaxResult: 1024}
	var w Window
	w.Apply(OrdinalCut{Ordinal: 100}, cfg)
	candidates := []DedupeEntry{
		dedupeEntry(1, 85), dedupeEntry(2, 90), dedupeEntry(3, 91), dedupeEntry(4, 100),
		dedupeEntry(5, 50), dedupeEntry(6, 101), dedupeEntry(7, 40),
	}
	keys := func(ns ...int) []DedupeKey {
		var out []DedupeKey
		for _, n := range ns {
			out = append(out, dedupeKey(n))
		}
		return out
	}
	for _, tc := range []struct {
		budget int
		want   []DedupeKey
	}{
		{-1, nil}, {0, nil}, {1, keys(1)}, {2, keys(1, 2)}, {3, keys(1, 2, 5)}, {100, keys(1, 2, 5, 7)},
	} {
		got := w.Apply(SweepBatch{Candidates: candidates, Budget: tc.budget}, cfg)
		if !reflect.DeepEqual(got.Delete, tc.want) || got.Live || got.Store || got.Refuse {
			t.Fatalf("budget %d: %+v, want Delete %v only", tc.budget, got, tc.want)
		}
		if w.Ordinal != 100 {
			t.Fatalf("a sweep moved the ordinal to %d", w.Ordinal)
		}
	}
	dup := []DedupeEntry{dedupeEntry(1, 10), dedupeEntry(1, 10), dedupeEntry(2, 10), dedupeEntry(2, 100)}
	if got := w.Apply(SweepBatch{Candidates: dup, Budget: 9}, cfg); !reflect.DeepEqual(got.Delete, keys(1)) {
		t.Fatalf("a key listed twice is deleted once, and never when any listing is live, got %v", got.Delete)
	}
}

// TestBatchIsCappedAt255OpIndices: opIndex is one byte, so the 256th operation
// would share an entry with the first.
// Guard: let opIndex wrap at 256.
func TestBatchIsCappedAt255OpIndices(t *testing.T) {
	for _, tc := range []struct {
		n    int
		want uint8
		ok   bool
	}{{0, 0, true}, {1, 1, true}, {254, 254, true}, {255, 255, true}, {256, 0, false}, {1000, 0, false}, {-1, 0, false}} {
		got, ok := BatchOpCount(tc.n)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("BatchOpCount(%d) = %d, %v; want %d, %v", tc.n, got, ok, tc.want, tc.ok)
		}
	}
}

// TestAnUnknownWindowTreatsAnEntryAsLive: a window that was never fed has
// ordinal 0, which no stored entry can be at, so an existing entry is answered
// rather than applied again (SPEC-GAP reading: never apply a retry twice).
// Guard: apply the plain age formula to a future entry.
func TestAnUnknownWindowTreatsAnEntryAsLive(t *testing.T) {
	var w Window
	e := dedupeEntry(1, 500)
	if d := w.Apply(LookupGiven{Entry: &e}, dedupeCfg); !d.Live {
		t.Fatalf("an entry ahead of the window is live, got %+v", d)
	}
	if d := w.Apply(LookupGiven{}, dedupeCfg); d.Live || d.Recorded != nil || d.Store {
		t.Fatalf("no entry: the operation applies, got %+v", d)
	}
}

// TestAWindowOfZeroIsAWindowOfOne: a configured window of 0 would make every
// entry dead and a retried pop apply twice; the newest write stays live.
// Guard: use cfg.Window unchanged.
func TestAWindowOfZeroIsAWindowOfOne(t *testing.T) {
	cfg := WindowConfig{Window: 0, MaxResult: 8}
	var w Window
	older := dedupeWrite(t, &w, cfg, DedupeWrite{Key: dedupeKey(1), Pos: pos(1, 1)})
	newest := dedupeWrite(t, &w, cfg, DedupeWrite{Key: dedupeKey(2), Pos: pos(1, 2)})
	if d := w.Apply(LookupGiven{Entry: newest}, cfg); !d.Live {
		t.Fatalf("the newest write must stay live, got %+v", d)
	}
	if d := w.Apply(LookupGiven{Entry: older}, cfg); d.Live {
		t.Fatalf("the previous write is outside a window of one, got %+v", d)
	}
}

// TestSeedCutSetsTheOrdinal: a seed or a restart sets the ordinal exactly, in
// either direction, and a write after it continues from there.
// Guard: take the larger of the old and new ordinal.
func TestSeedCutSetsTheOrdinal(t *testing.T) {
	var w Window
	for _, ordinal := range []uint64{500, 12, 0, 77} {
		w.Apply(OrdinalCut{Ordinal: ordinal}, dedupeCfg)
		if w.Ordinal != ordinal {
			t.Fatalf("OrdinalCut %d left ordinal %d", ordinal, w.Ordinal)
		}
	}
	if e := dedupeWrite(t, &w, dedupeCfg, DedupeWrite{Key: dedupeKey(1), Pos: pos(9, 1)}); e.Ordinal != 78 {
		t.Fatalf("a write after a cut at 77 is ordinal 78, got %d", e.Ordinal)
	}
}

// dedupeMatchRows requires exactly one row for the event and returns its name.
func dedupeMatchRows(t *testing.T, w Window, ev WindowEvent, cfg WindowConfig) string {
	t.Helper()
	got := matchingWindowRows(&w, ev, cfg)
	if len(got) != 1 {
		t.Fatalf("window %+v cfg %+v event %T %+v: %d rows match %v, want exactly 1", w, cfg, ev, ev, len(got), got)
	}
	return got[0]
}

// dedupeLive is the plain-language liveness rule, written separately from the
// machine: dead only when the entry is at least `window` records behind.
func dedupeLive(windowOrdinal, entryOrdinal, window uint64) bool {
	if window == 0 {
		window = 1
	}
	if entryOrdinal > windowOrdinal {
		return true
	}
	return windowOrdinal-entryOrdinal < window
}

// TestEveryEventClassHasExactlyOneRow enumerates the cross product of the four
// event kinds with the window ordinal, the configured window and cap, the
// result length, the entry's age relation, the budget and the candidate mix,
// requires exactly one matching row for each input, and checks the decision
// and the state change against an independent oracle.
// Guard: let the live and dead lookup rows overlap (drop the `Entry != nil`
// guard of a lookup row) or leave a class uncovered; the count is not one.
func TestEveryEventClassHasExactlyOneRow(t *testing.T) {
	checked := 0
	ordinals := []uint64{0, 1, 2, 5, 100, 8192, 20000, math.MaxUint64 - 1}
	windows := []uint64{0, 1, 2, 8192}
	caps := []int{0, 1, 1024}
	deltas := []int64{5, 1, 0, -1, -2, -9, -10, -11, -1000, -8191, -8192, -8193}

	for _, wo := range ordinals {
		for _, wcfg := range windows {
			cfg := WindowConfig{Window: wcfg, MaxResult: 1024}
			w := Window{Ordinal: wo}

			// OrdinalCut: any ordinal.
			for _, cut := range []uint64{0, 1, wo, math.MaxUint64} {
				name := dedupeMatchRows(t, w, OrdinalCut{Ordinal: cut}, cfg)
				got := w
				d := got.Apply(OrdinalCut{Ordinal: cut}, cfg)
				if name != "seed/cut" || got.Ordinal != cut || !reflect.DeepEqual(d, WindowDecision{}) {
					t.Fatalf("seed cut %d from %d: row %s, state %+v, decision %+v", cut, wo, name, got, d)
				}
				checked++
			}

			// LookupGiven: nil, then every age relation.
			dedupeCheckLookup := func(entry *DedupeEntry) {
				name := dedupeMatchRows(t, w, LookupGiven{Entry: entry}, cfg)
				got := w
				d := got.Apply(LookupGiven{Entry: entry}, cfg)
				wantLive := entry != nil && dedupeLive(wo, entry.Ordinal, wcfg)
				if d.Live != wantLive || (d.Recorded != nil) != wantLive || d.Store || d.Write != nil || d.Refuse || d.Delete != nil || got != w {
					t.Fatalf("lookup %+v at ordinal %d window %d: row %s decision %+v", entry, wo, wcfg, name, d)
				}
				checked++
			}
			dedupeCheckLookup(nil)
			for _, delta := range deltas {
				ordinal := int64(wo) + delta
				if wo > math.MaxInt64 || ordinal < 0 {
					continue
				}
				e := dedupeEntry(1, uint64(ordinal))
				dedupeCheckLookup(&e)
			}

			// DedupeWrite: every result length against every cap.
			for _, maxResult := range caps {
				wcfg2 := WindowConfig{Window: wcfg, MaxResult: maxResult}
				for _, n := range []int{0, 1, 2, 1023, 1024, 1025} {
					ev := DedupeWrite{Key: dedupeKey(3), Pos: pos(2, 9), Result: bytes.Repeat([]byte{'r'}, n)}
					name := dedupeMatchRows(t, w, ev, wcfg2)
					got := w
					d := got.Apply(ev, wcfg2)
					if n > maxResult {
						if name != "write/oversize" || !d.Refuse || d.Store || d.Write != nil || got != w {
							t.Fatalf("oversize %d>%d: row %s decision %+v state %+v", n, maxResult, name, d, got)
						}
					} else if name != "write/store" || !d.Store || d.Write == nil || d.Refuse || got.Ordinal != wo+1 || d.Write.Ordinal != wo+1 {
						t.Fatalf("result %d<=%d at ordinal %d: row %s decision %+v state %+v", n, maxResult, wo, name, d, got)
					}
					checked++
				}
			}

			// SweepBatch: budgets against a candidate mix.
			var mix []DedupeEntry
			for i, delta := range deltas {
				ordinal := int64(wo) + delta
				if wo > math.MaxInt64 || ordinal < 0 {
					continue
				}
				mix = append(mix, dedupeEntry(100+i, uint64(ordinal)))
			}
			for _, budget := range []int{-1, 0, 1, 2, 5, 1000} {
				ev := SweepBatch{Candidates: mix, Budget: budget}
				name := dedupeMatchRows(t, w, ev, cfg)
				got := w
				d := got.Apply(ev, cfg)
				dead := 0
				for _, c := range mix {
					if !dedupeLive(wo, c.Ordinal, wcfg) {
						dead++
					}
				}
				want := min(max(budget, 0), dead)
				if len(d.Delete) != want || d.Live || d.Store || d.Refuse || got != w {
					t.Fatalf("sweep budget %d ordinal %d window %d: row %s deleted %d, want %d", budget, wo, wcfg, name, len(d.Delete), want)
				}
				for _, k := range d.Delete {
					for _, c := range mix {
						if c.Key == k && dedupeLive(wo, c.Ordinal, wcfg) {
							t.Fatalf("sweep deleted the live entry %+v", c)
						}
					}
				}
				checked++
			}
		}
	}
	if checked < 1000 {
		t.Fatalf("only %d combinations were checked", checked)
	}
	t.Logf("enumerated %d input classes, each matching exactly one row", checked)
}
