package main

import (
	"runtime"
	"testing"
)

// minSeeded is the fewest elements every owner of the store a read pages through must be seeded
// with: twice the spec page, so a page offset of up to one page still fits. The user's rooms are a
// hash store, which has no order to take an offset in, so they need only the page itself.
func minSeeded(kind ReadKind) int {
	if kind == ReadChatUserRooms {
		return readLimits[kind]
	}
	return 2 * readLimits[kind]
}

// TestEveryPagedReadKeepsItsSpecLimitForEveryOwner fails on a model that seeds some owners with
// less than the read's page: at scales 1, 20 and 300 every owner of every paged store holds the
// minimum, so the eligible share is 100% and no read records a fallback.
func TestEveryPagedReadKeepsItsSpecLimitForEveryOwner(t *testing.T) {
	scales := []int{1, 20, 300}
	if testing.Short() {
		scales = scales[:2]
	}
	for _, scale := range scales {
		// One layout at a time: the model of all four at scale 300 does not fit twice in memory.
		for _, layout := range benchLayouts {
			ds, err := newDataset(layout, 1, scale)
			if err != nil {
				t.Fatal(err)
			}
			for kind, store := range readStores {
				if layoutOf(kind) != layout || readLimits[kind] == 0 {
					continue
				}
				plan, err := planPage(kind, ds)
				if err != nil {
					t.Fatalf("scale %d %s: %v", scale, kind, err)
				}
				if plan.Fallback || plan.Limit != readLimits[kind] || len(plan.Eligible) != plan.Owners {
					t.Errorf("scale %d %s: limit %d of spec %d, %d of %d owners eligible, fallback %v", scale, kind, plan.Limit, plan.SpecLimit, len(plan.Eligible), plan.Owners, plan.Fallback)
				}
				fewest := -1
				for _, id := range ds.ids(store[0]) {
					if n := len(ds.elements(StoreRef{store[0], id, store[1]})); fewest < 0 || n < fewest {
						fewest = n
					}
				}
				if fewest < minSeeded(kind) {
					t.Errorf("scale %d %s: the fewest seeded %s.%s elements of an owner is %d, want at least %d", scale, kind, store[0], store[1], fewest, minSeeded(kind))
				}
			}
			// Drop this layout before the next is built.
			runtime.GC()
		}
	}
}

// TestRecordedPlansShowNoFallbackAtScaleOne: the record's page figures say 100% eligible.
func TestRecordedPlansShowNoFallbackAtScaleOne(t *testing.T) {
	w, err := newWorld(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	for kind, info := range w.pageInfo() {
		if info.Fallback || info.Limit != info.SpecLimit || info.EligibleShare != 1 {
			t.Errorf("%s: %+v", kind, info)
		}
	}
}
