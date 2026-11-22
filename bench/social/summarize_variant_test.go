package main

import (
	"strings"
	"testing"
)

func withVariant(name string) func(*legRecord) {
	return func(r *legRecord) { r.EngineSettings["variant"] = name }
}

func withRateBasis(basis string) func(*legRecord) {
	return func(r *legRecord) { r.RateBasis = basis }
}

// TestGroupKeySeparatesVariantAndRateBasis: a cold, cache-raised or memory-limited leg is not a
// repeat of the primary leg, and an open leg at the common rate is not a repeat of one at 50% of
// the engine's own median; merging them corrupts the median of three and its basis.
func TestGroupKeySeparatesVariantAndRateBasis(t *testing.T) {
	groups := groupLegs([]*legRecord{
		testLeg(onRepeat(1, "VM1", 900)),
		testLeg(onRepeat(1, "VM1", 500), withVariant("cold")),
		testLeg(onRepeat(1, "VM1", 700), withVariant("cache-raised")),
	})
	if len(groups) != 3 {
		t.Errorf("legs differing only in variant made %d groups, want 3", len(groups))
	}
	groups = groupLegs([]*legRecord{
		testLeg(onRepeat(1, "VM1", 900), withRateBasis("common")),
		testLeg(onRepeat(1, "VM1", 450), withRateBasis("self-relative")),
	})
	if len(groups) != 2 {
		t.Errorf("legs differing only in rate basis made %d groups, want 2", len(groups))
	}
	groups = groupLegs([]*legRecord{
		testLeg(onRepeat(1, "VM1", 900), withVariant("cold"), withRateBasis("common")),
		testLeg(onRepeat(2, "VM1", 1100), withVariant("cold"), withRateBasis("common")),
		testLeg(onRepeat(3, "VM1", 1000), withVariant("cold"), withRateBasis("common")),
	})
	if len(groups) != 1 || groups[0].Median != 1000 {
		t.Errorf("three repeats equal in variant and rate basis made %d groups, want 1 with median 1000", len(groups))
	}
}

func TestSummaryLabelsVariant(t *testing.T) {
	table := renderSummary([]*legRecord{testLeg(withVariant("cache-raised"))})
	if !strings.Contains(table, "cache-raised") {
		t.Errorf("a variant leg's row does not name its variant:\n%s", table)
	}
}
