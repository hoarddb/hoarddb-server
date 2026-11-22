package cli

import (
	"fmt"
	"io"
	"slices"
	"strings"
)

// PlanThresholdsVersion is printed in a plan's Inputs so a reader can tell
// which constants produced it.
const PlanThresholdsVersion = "1"

// PlanThresholds is the only place the advisor holds numeric thresholds
// (spec section 6.1, Q4). Fractions are per-mille integers.
type PlanThresholds struct {
	GrowingListP95, GrowingListAny                      int   // S2: 8, 64
	OrderedPermille                                     int   // S2a: 980
	DistinctWithinPermille                              int   // S3: 1000
	LargeBytes, NotedBytes                              int64 // S4: 65536, 4096
	ResolvePermille                                     int   // S5, S6: 950
	SparsePermille                                      int   // S7: 50
	UniquePresentPermille                               int   // S1: 950
	OptionalFilterFail                                  int   // rule 1: a primary list with this many optional filters fails: 3
	AdhocOptionalFilters                                int   // NEW (build-doc finding, verdict rule): 15
	ContainersFits, ContainersMerge                     int   // spec 16.2: 36, 300
	BucketDeclBytes, StoreFieldBytes, RetiredFieldBytes int   // 436, 26, 10 (rule 11 numbers)
	TenantPermille, TenantMinLines                      int   // R-tenant-root: share of traced selectors, per mille; fewest lines: 900, 3
	LocalityDocuments, LocalityReadRatio                int   // spec 16.3: most documents of a small kind; reads per write: 100000, 10
}

// DefaultPlanThresholds returns the constants the advisor plans with.
func DefaultPlanThresholds() PlanThresholds {
	return PlanThresholds{
		GrowingListP95:         8,
		GrowingListAny:         64,
		OrderedPermille:        980,
		DistinctWithinPermille: 1000,
		LargeBytes:             65536,
		NotedBytes:             4096,
		ResolvePermille:        950,
		SparsePermille:         50,
		UniquePresentPermille:  950,
		OptionalFilterFail:     3,
		AdhocOptionalFilters:   15,
		ContainersFits:         36,
		ContainersMerge:        300,
		BucketDeclBytes:        436,
		StoreFieldBytes:        26,
		RetiredFieldBytes:      10,
		TenantPermille:         900,
		TenantMinLines:         3,
		LocalityDocuments:      100000,
		LocalityReadRatio:      10,
	}
}

// planThresholdLine is one printed constant of --thresholds.
type planThresholdLine struct {
	name    string
	amount  int64
	meaning string
}

// WriteThresholds prints every constant, one `name = value  # meaning` line,
// sorted by name, for --thresholds. A failed write is returned, not dropped.
func WriteThresholds(out io.Writer, th PlanThresholds) error {
	lines := []planThresholdLine{
		{"GrowingListAny", int64(th.GrowingListAny), "S2: children per parent at which a list grows without bound"},
		{"GrowingListP95", int64(th.GrowingListP95), "S2: children per parent at the 95th percentile that makes a list grow"},
		{"OrderedPermille", int64(th.OrderedPermille), "S2a: share of a list's sort key that is monotone, per mille"},
		{"DistinctWithinPermille", int64(th.DistinctWithinPermille), "S3: distinct share that makes a field a closed set, per mille"},
		{"LargeBytes", th.LargeBytes, "S4: bytes at the median that make a field large"},
		{"NotedBytes", th.NotedBytes, "S4: bytes at the median that make a field worth a note"},
		{"ResolvePermille", int64(th.ResolvePermille), "S5, S6: share of references that must resolve, per mille"},
		{"SparsePermille", int64(th.SparsePermille), "S7: presence below which a field is sparse, per mille"},
		{"UniquePresentPermille", int64(th.UniquePresentPermille), "S1: presence at which a field may be declared unique, per mille"},
		{"OptionalFilterFail", int64(th.OptionalFilterFail), "rule 1: optional filters on one primary list that fail it"},
		{"AdhocOptionalFilters", int64(th.AdhocOptionalFilters), "NEW, unratified: optional filters on one primary list that make it ad hoc (verdict rule); the corpus note names 15+ for one application and no other source"},
		{"ContainersFits", int64(th.ContainersFits), "spec 16.2: containers per principal that a few owned lists serve"},
		{"ContainersMerge", int64(th.ContainersMerge), "spec 16.2: containers per principal above which a merge serves the read; the spec says 'hundreds', 300 is the build note's reading and is unratified"},
		{"BucketDeclBytes", int64(th.BucketDeclBytes), "rule 11: declaration bytes per bucket"},
		{"StoreFieldBytes", int64(th.StoreFieldBytes), "rule 11: declaration bytes per store field"},
		{"RetiredFieldBytes", int64(th.RetiredFieldBytes), "rule 11: declaration bytes per retired field"},
		{"TenantPermille", int64(th.TenantPermille), "R-tenant-root: share of traced selectors that name one field, per mille"},
		{"LocalityDocuments", int64(th.LocalityDocuments), "spec 16.3: most documents a kind may hold and still be replicated for locality"},
		{"LocalityReadRatio", int64(th.LocalityReadRatio), "spec 16.3: traced read weight per unit of write weight that makes a kind read-heavy"},
		{"TenantMinLines", int64(th.TenantMinLines), "R-tenant-root: fewest traced selector lines before a tenant root is claimed"},
	}
	slices.SortFunc(lines, func(a, b planThresholdLine) int { return strings.Compare(a.name, b.name) })
	for _, line := range lines {
		if _, err := fmt.Fprintf(out, "%s = %d  # %s\n", line.name, line.amount, line.meaning); err != nil {
			return fmt.Errorf("write threshold %s: %w", line.name, err)
		}
	}
	return nil
}
