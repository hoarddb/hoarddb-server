package cli

import (
	"fmt"
	"strings"
	"testing"
)

func traceModel(lines ...TraceLine) *PlanModel {
	for index := range lines {
		lines[index].Ordinal = index + 1
	}
	return &PlanModel{Version: 1, Source: "hand", Trace: lines}
}

// TestDetectOptionalFiltersCounts: the count is in the note, and two filters are below the bar.
func TestDetectOptionalFiltersCounts(t *testing.T) {
	filters := func(n int) []TraceFilter {
		var out []TraceFilter
		for i := 0; i < n; i++ {
			out = append(out, TraceFilter{Field: fmt.Sprintf("f%d", i), Optional: true})
		}
		return out
	}
	th := DefaultPlanThresholds()
	got := detectOptionalFilters(traceModel(TraceLine{Op: "list", Coll: "issues", Filters: filters(10)}), th)
	if len(got) != 1 || !strings.Contains(got[0].Note, "10") {
		t.Fatalf("10 filters: %+v, want one signal naming 10", got)
	}
	if got := detectOptionalFilters(traceModel(TraceLine{Op: "list", Coll: "issues", Filters: filters(2)}), th); len(got) != 0 {
		t.Fatalf("2 filters: %+v, want nothing", got)
	}
}

// TestDetectPermissionByShape: each visibility word has its own reduction, and
// the fan decides seeks, a merge or a scan.
func TestDetectPermissionByShape(t *testing.T) {
	th := DefaultPlanThresholds()
	seen := map[string]bool{}
	for _, shape := range planVisibilityShapes {
		model := traceModel(TraceLine{Op: "list", Coll: "docs", Visibility: shape.Word})
		got := detectPermissionPredicate(model, th)
		if len(got) != 1 || !strings.Contains(got[0].Note, shape.Reduction) || seen[got[0].Note] {
			t.Fatalf("%s: %+v", shape.Word, got)
		}
		seen[got[0].Note] = true
	}
	for p95, want := range map[int]string{20: "seeks", 40: "a merge", 5000: "a scan"} {
		model := traceModel(TraceLine{Op: "list", Coll: "docs", Visibility: "container"})
		model.Fans = []ModelFan{{Parent: "users", Child: "docs", P50: 1, P95: p95, Max: p95}}
		got := detectPermissionPredicate(model, th)
		if len(got) != 1 || !strings.HasSuffix(got[0].Note, fmt.Sprintf("p95 = %d: %s", p95, want)) {
			t.Fatalf("p95 %d: %+v, want %s", p95, got, want)
		}
	}
}

// TestDetectLinkTableBothSides: one side alone is not a link table.
func TestDetectLinkTableBothSides(t *testing.T) {
	link := ModelKind{Name: "members", Fields: []ModelField{
		{Name: "_id", Type: "string", PresentPermille: 1000},
		{Name: "room_id", Type: "string", PresentPermille: 1000, Ref: &ModelRef{To: "rooms", ResolvePermille: 1000}},
		{Name: "user_id", Type: "string", PresentPermille: 1000, Ref: &ModelRef{To: "users", ResolvePermille: 1000}}}}
	th := DefaultPlanThresholds()
	one := traceModel(TraceLine{Op: "list", Coll: "members", By: []string{"room_id"}})
	one.Kinds = []ModelKind{link}
	if got := detectLinkTable(one, th); len(got) != 0 {
		t.Fatalf("one side: %+v, want nothing", got)
	}
	both := traceModel(TraceLine{Op: "list", Coll: "members", By: []string{"room_id"}}, TraceLine{Op: "list", Coll: "members", By: []string{"user_id"}})
	both.Kinds = []ModelKind{link}
	got := detectLinkTable(both, th)
	if len(got) != 1 || got[0].Other != "rooms,users" {
		t.Fatalf("both sides: %+v, want one signal naming rooms and users", got)
	}
}

// TestDetectTransactionsSpan: the span names the kinds the group touches.
func TestDetectTransactionsSpan(t *testing.T) {
	model := traceModel(
		TraceLine{Op: "insert", Coll: "orders", AtomicWith: "checkout"},
		TraceLine{Op: "update", Coll: "stock", AtomicWith: "checkout"})
	got := detectTransactions(model, DefaultPlanThresholds())
	if len(got) != 1 || !strings.Contains(got[0].Note, "spans 2 kinds: orders, stock") {
		t.Fatalf("got %+v", got)
	}
}
