package main

import (
	"math"
	"slices"
	"testing"
)

// seededCount is how many elements of a store the owner was seeded with.
func seededCount(w *world, kind ReadKind, owner string) int {
	store := readStores[kind]
	return len(w.sets[layoutOf(kind)].elements(StoreRef{store[0], owner, store[1]}))
}

// TestPagesKeepTheSpecLimitForEligibleOwners: in a world of scale 20 a paged read keeps its spec
// limit, or the largest limit that a quarter of the owners were seeded with, and is only ever
// asked of owners seeded with at least that many elements. Never the fewest-seeded minimum, which
// made the feed and inbox a page of one.
func TestPagesKeepTheSpecLimitForEligibleOwners(t *testing.T) {
	w, err := newWorld(7, 20)
	if err != nil {
		t.Fatal(err)
	}
	for kind, store := range readStores {
		plan := w.pages[kind]
		if plan == nil {
			t.Fatalf("%s has no page plan", kind)
		}
		if plan.SpecLimit != readLimits[kind] || plan.Limit > plan.SpecLimit && plan.SpecLimit != 0 {
			t.Errorf("%s: spec %d, limit %d", kind, plan.SpecLimit, plan.Limit)
		}
		if plan.SpecLimit == 0 {
			if plan.Limit != 0 || len(plan.Eligible) != plan.Owners {
				t.Errorf("%s reads every element of every owner, got limit %d with %d of %d owners", kind, plan.Limit, len(plan.Eligible), plan.Owners)
			}
			continue
		}
		if share := float64(len(plan.Eligible)) / float64(plan.Owners); share < minEligibleShare {
			t.Errorf("%s: only %.0f%% of the owners are eligible", kind, 100*share)
		}
		for _, owner := range plan.Eligible {
			if n := seededCount(w, kind, owner); n < plan.Limit {
				t.Fatalf("%s: eligible owner %s was seeded with %d, below the page of %d", kind, owner, n, plan.Limit)
			}
		}
		// The limit is the largest one a quarter of the owners support: one more would lose them.
		if plan.Limit < plan.SpecLimit {
			if !plan.Fallback {
				t.Errorf("%s: the limit fell from %d to %d without saying so", kind, plan.SpecLimit, plan.Limit)
			}
			more := 0
			for _, id := range w.sets[layoutOf(kind)].ids(store[0]) {
				if seededCount(w, kind, id) >= plan.Limit+1 {
					more++
				}
			}
			if float64(more) >= minEligibleShare*float64(plan.Owners) {
				t.Errorf("%s: limit %d is not the largest, %d of %d owners hold %d", kind, plan.Limit, more, plan.Owners, plan.Limit+1)
			}
		} else if plan.Fallback {
			t.Errorf("%s: flagged as a fallback with the full spec limit", kind)
		}
	}
	// The headline of the change: feed and inbox are no longer a page of one.
	for _, kind := range []ReadKind{ReadSocialFeed, ReadSocialInbox} {
		if w.readLimit(kind) < 5 || len(w.pages[kind].Eligible) == 0 {
			t.Errorf("%s: limit %d with %d eligible owners", kind, w.readLimit(kind), len(w.pages[kind].Eligible))
		}
	}
	// Since the density change every owner holds the spec page, so no read falls back (the
	// density test pins the figures; here the headline is that the flag is never set).
	for kind, plan := range w.pages {
		if plan.Fallback {
			t.Errorf("%s: fell back from %d to %d", kind, plan.SpecLimit, plan.Limit)
		}
	}
	if w.memberLimit != 0 {
		t.Errorf("the room page reads every member, got a member page of %d", w.memberLimit)
	}
}

// TestNoPageOfAnOrderedStoreEverHoldsAWrittenElement: with the planner's own requests, with and
// without offsets, a page of a fifo or btree store ends inside the owner's seeded elements.
func TestNoPageOfAnOrderedStoreEverHoldsAWrittenElement(t *testing.T) {
	for _, depth := range []float64{0, 0.5} {
		w, err := newWorld(7, 20)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.configureAccess(accessConfig{Mode: accessZipf, ZipfS: 1, PageDepth: depth}); err != nil {
			t.Fatal(err)
		}
		mix, _ := newMix(benchLayouts, "")
		p := newPlanner(w, mix, 1, 0, 4)
		checked := 0
		for range 300000 {
			req := p.next()
			if req.Read == nil || !offsetEligible(req.Read.Kind) {
				continue
			}
			checked++
			if end := req.Read.Offset + req.Read.Limit; req.Read.Limit < 1 || end > seededCount(w, req.Read.Kind, req.Read.Entity) {
				t.Fatalf("%s of %s: page ends at %d, past the %d seeded elements", req.Read.Kind, req.Read.Entity, end, seededCount(w, req.Read.Kind, req.Read.Entity))
			}
		}
		if checked < 1000 {
			t.Fatalf("depth %v: only %d ordered pages planned", depth, checked)
		}
	}
}

func TestPlannerUsesTheSpecLimitsAndRecordShowsThem(t *testing.T) {
	w, err := newWorld(7, 3)
	if err != nil {
		t.Fatal(err)
	}
	mix, _ := newMix(benchLayouts, "chat.room_page")
	req := newPlanner(w, mix, 1, 0, 1).next()
	if req.Read == nil || req.Read.Limit != w.readLimit(ReadChatRoomPage) || req.Read.MemberLimit != w.memberLimit {
		t.Fatalf("room page op %+v", req.Read)
	}
	if !slices.Contains(w.pages[ReadChatRoomPage].Eligible, req.Read.Entity) {
		t.Errorf("room %s is not an eligible owner", req.Read.Entity)
	}
	per := w.elementsPerRead()
	if per["social.feed"] != w.readLimit(ReadSocialFeed) || per[membersKey] != w.memberLimit {
		t.Errorf("elements per read %v", per)
	}
	info := w.pageInfo()
	feed := info["social.feed"]
	if feed.Limit != w.readLimit(ReadSocialFeed) || feed.SpecLimit != 10 || feed.EligibleOwners != len(w.pages[ReadSocialFeed].Eligible) || feed.Owners == 0 ||
		math.Abs(feed.EligibleShare-float64(feed.EligibleOwners)/float64(feed.Owners)) > 1e-9 || feed.Fallback != (feed.Limit < feed.SpecLimit) {
		t.Errorf("page info of the feed %+v", feed)
	}
}

// Every read of the planner goes to an eligible owner, uniformly or by Zipf over the eligible ones,
// and the eligible set depends on the model only.
func TestReadsGoToEligibleOwnersAndTheSetIsTheModels(t *testing.T) {
	a, _ := newWorld(7, 20)
	b, _ := newWorld(7, 20)
	for kind := range readStores {
		if !slices.Equal(a.pages[kind].Eligible, b.pages[kind].Eligible) || a.pages[kind].Limit != b.pages[kind].Limit {
			t.Errorf("%s: the eligible set differs between two worlds of one seed", kind)
		}
	}
	for _, mode := range []string{accessUniform, accessZipf} {
		w, _ := newWorld(7, 20)
		if err := w.configureAccess(accessConfig{Mode: mode, ZipfS: 1}); err != nil {
			t.Fatal(err)
		}
		mix, _ := newMix(benchLayouts, "")
		p := newPlanner(w, mix, 1, 3, 8)
		for range 100000 {
			req := p.next()
			if req.Read == nil {
				continue
			}
			if plan := w.pages[req.Read.Kind]; plan != nil && !slices.Contains(plan.Eligible, req.Read.Entity) {
				t.Fatalf("%s mode: %s of %s, which is not eligible", mode, req.Read.Kind, req.Read.Entity)
			}
		}
	}
}

func TestRunRecordCarriesElementsPerRead(t *testing.T) {
	p, _ := fakeRun(t, nil)
	rec, err := runRepetition(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	if rec.ReadElements["social.feed"] != p.World.readLimit(ReadSocialFeed) {
		t.Errorf("record elements per read %v", rec.ReadElements)
	}
	if got := rec.ReadPages["social.feed"]; got.Limit != rec.ReadElements["social.feed"] || got.EligibleOwners == 0 || got.Owners == 0 {
		t.Errorf("record read_pages %+v", got)
	}
}

// The SQL and Mongo adapters read a page at an offset as the range seq >= Offset, which is only
// correct while the seeded Seq of a pushed store counts 0, 1, 2, ... in order.
func TestSeededSeqOfPushedStoresIsDense(t *testing.T) {
	w := testWorld(t)
	for layout, ds := range w.sets {
		for ref, elements := range ds.stores {
			kind, err := KindOf(layout, ref.Bucket, ref.Store)
			if err != nil || kind.Keyed() {
				continue
			}
			for i, e := range elements {
				if e.Seq != int64(i) {
					t.Fatalf("%s/%s/%s element %d has seq %d: the seq-range offset read would skip or repeat rows", ref.Bucket, ref.Owner, ref.Store, i, e.Seq)
				}
			}
		}
	}
}

// offsetWorld is a world whose planner takes an offset on every eligible read when depth is 1.
func offsetWorld(t *testing.T, depth float64) *world {
	t.Helper()
	w, err := newWorld(7, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.configureAccess(accessConfig{Mode: accessUniform, ZipfS: 1, PageDepth: depth}); err != nil {
		t.Fatal(err)
	}
	return w
}

// TestOffsetStaysInsideTheSeededElements: a read at an offset ends inside the owner's seeded
// elements, so it never sees a written element and every engine does the same work.
func TestOffsetStaysInsideTheSeededElements(t *testing.T) {
	w := offsetWorld(t, 1)
	mix, _ := newMix(benchLayouts, "")
	p := newPlanner(w, mix, 1, 0, 3)
	var deep, paged int
	for range 400000 {
		req := p.next()
		if req.Read == nil {
			continue
		}
		op := req.Read
		store, capped := readStores[op.Kind]
		if !capped {
			continue
		}
		kind, _ := KindOf(op.Layout, store[0], store[1])
		seeded := w.sets[op.Layout].elements(StoreRef{store[0], op.Entity, store[1]})
		if op.Offset == 0 {
			if op.AfterKey != "" {
				t.Fatalf("%s: AfterKey %q without an offset", op.Kind, op.AfterKey)
			}
			if !kind.Keyed() || kind == BTree {
				paged++
			}
			continue
		}
		deep++
		if kind == Hash {
			t.Fatalf("%s pages a hash store, which has no order to take an offset in", op.Kind)
		}
		if op.Limit < 1 || op.Offset+op.Limit > len(seeded) {
			t.Fatalf("%s of %s: offset %d + limit %d runs past the %d seeded elements", op.Kind, op.Entity, op.Offset, op.Limit, len(seeded))
		}
		if kind == BTree {
			if want := seeded[op.Offset-1].Key; op.AfterKey != want {
				t.Fatalf("%s of %s: AfterKey %q, want the key before the offset %q", op.Kind, op.Entity, op.AfterKey, want)
			}
		} else if op.AfterKey != "" {
			t.Fatalf("%s: a pushed store pages by seq, AfterKey %q must be empty", op.Kind, op.AfterKey)
		}
	}
	if deep == 0 {
		t.Fatal("no read took an offset")
	}
	t.Logf("%d reads at an offset, %d paged reads from the start", deep, paged)
}

// TestOffsetShareFollowsPageDepth: of the paged reads that have room for an offset, about
// -page-depth of them take one.
func TestOffsetShareFollowsPageDepth(t *testing.T) {
	for _, depth := range []float64{0, 0.3, 1} {
		w := offsetWorld(t, depth)
		mix, _ := newMix(benchLayouts, "")
		p := newPlanner(w, mix, 1, 1, 9)
		var room, taken float64
		for range 600000 {
			req := p.next()
			if req.Read == nil || !offsetEligible(req.Read.Kind) {
				continue
			}
			store := readStores[req.Read.Kind]
			n := len(w.sets[req.Read.Layout].elements(StoreRef{store[0], req.Read.Entity, store[1]}))
			if req.Read.Limit == 0 || n-req.Read.Limit < 1 {
				continue
			}
			room++
			if req.Read.Offset > 0 {
				taken++
			}
		}
		if room < 1000 {
			t.Fatalf("depth %v: only %v reads had room for an offset", depth, room)
		}
		if got := taken / room; math.Abs(got-depth) > 0.02 {
			t.Errorf("-page-depth %v: %.3f of the reads with room took an offset", depth, got)
		}
	}
}

// TestPageDepthDoesNotChangeTheRequestStream: the offset draws are made whether or not the
// offset is used, so the same seed gives the same operations and entities at any depth.
func TestPageDepthDoesNotChangeTheRequestStream(t *testing.T) {
	stream := func(depth float64) []string {
		w := offsetWorld(t, depth)
		mix, _ := newMix(benchLayouts, "")
		p := newPlanner(w, mix, 1, 2, 5)
		var out []string
		for range 20000 {
			req := p.next()
			entity := ""
			if req.Read != nil {
				entity = req.Read.Entity
			} else if req.Write != nil {
				entity = req.Write.ID
			}
			out = append(out, req.Kind+" "+entity)
		}
		return out
	}
	a, b := stream(0), stream(1)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("request %d is %q at depth 0 and %q at depth 1", i, a[i], b[i])
		}
	}
}
