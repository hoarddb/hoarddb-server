package cli

import (
	"strings"
	"testing"
)

func optionalFilters(n int) []TraceFilter {
	var out []TraceFilter
	for i := 0; i < n; i++ {
		out = append(out, TraceFilter{Field: string(rune('a' + i)), Optional: true})
	}
	return out
}

func scorecardFor(lines ...TraceLine) PlanScorecard {
	model := traceModel(lines...)
	th := DefaultPlanThresholds()
	return buildScorecard(model, detectAll(model, th), th)
}

// TestScorecardCellBoundaries guards rule 1 at the bar: 2 passes, 3 fails on a
// primary line and is partial on an admin line.
func TestScorecardCellBoundaries(t *testing.T) {
	cases := []struct {
		n       int
		surface string
		want    string
	}{{2, "", "P"}, {3, "", "F"}, {3, "admin", "p"}, {3, "primary", "F"}}
	for _, tc := range cases {
		card := scorecardFor(TraceLine{Op: "list", Coll: "issues", Surface: tc.surface, Filters: optionalFilters(tc.n)})
		if got := card.Rules[0].Cell; got != tc.want {
			t.Errorf("%d optional filters on %q surface: cell %s, want %s", tc.n, tc.surface, got, tc.want)
		}
	}
}

// TestVerdictNoRequiresAllThree guards the one verdict that says no: ad-hoc
// filters, a client sort, and a visibility predicate or a dynamic dimension.
// Without the visibility or dynamic leg it is a redesign, never a no.
func TestVerdictNoRequiresAllThree(t *testing.T) {
	line := TraceLine{Op: "list", Coll: "issues", SortFrom: "client", Filters: optionalFilters(20)}
	card := scorecardFor(line)
	if !strings.HasPrefix(card.Verdict, "only with redesign of dynamic filters, client sorting") {
		t.Fatalf("20 filters and client sort alone: verdict %q, want a redesign", card.Verdict)
	}
	withVisibility := line
	withVisibility.Visibility = "object_global"
	if card := scorecardFor(withVisibility); card.Verdict != "not a good candidate for hoardDB" {
		t.Fatalf("with a visibility predicate: verdict %q, want no", card.Verdict)
	}
	line.Dynamic = true
	if card := scorecardFor(line); card.Verdict != "not a good candidate for hoardDB" || !strings.Contains(card.Reason, "ad-hoc") {
		t.Fatalf("with a dynamic line: verdict %q reason %q", card.Verdict, card.Reason)
	}
}

// TestScorecardRulesThreeToFive covers joins, visibility and atomic groups.
func TestScorecardRulesThreeToFive(t *testing.T) {
	card := scorecardFor(
		TraceLine{Op: "join", Coll: "posts", Join: &TraceJoin{To: "users", On: "id", Kind: "pk"}},
		TraceLine{Op: "join", Coll: "posts", Join: &TraceJoin{To: "tags", On: "hash", Kind: "nonkey"}},
		TraceLine{Op: "list", Coll: "docs", Visibility: "object_global"},
		TraceLine{Op: "insert", Coll: "orders", AtomicWith: "checkout"},
		TraceLine{Op: "update", Coll: "stock", AtomicWith: "checkout"})
	if card.Rules[2].Cell != "F" || card.Rules[3].Cell != "F" || card.Rules[4].Cell != "F" {
		t.Fatalf("cells %s %s %s, want F F F for rules 3 to 5", card.Rules[2].Cell, card.Rules[3].Cell, card.Rules[4].Cell)
	}
	if card.Rules[0].Cell != "P" || card.Rules[1].Cell != "P" {
		t.Fatalf("rules 1 and 2 = %s %s, want P P", card.Rules[0].Cell, card.Rules[1].Cell)
	}
	if card.Verdict != "only with redesign of non-reducible joins, permissions in the query, atomic multi-entity writes" {
		t.Fatalf("verdict %q", card.Verdict)
	}
	reducible := scorecardFor(TraceLine{Op: "join", Coll: "posts", Join: &TraceJoin{To: "users", On: "id", Kind: "pk"}})
	if reducible.Verdict != "yes" || reducible.Reason != "" {
		t.Fatalf("a primary-key join alone: verdict %q reason %q, want yes", reducible.Verdict, reducible.Reason)
	}
}

// TestVisibilityJoinCountsForRuleFourOnly (M4): a permission predicate is one
// fail whether the extractor wrote it as a join or as an attribute of the read.
func TestVisibilityJoinCountsForRuleFourOnly(t *testing.T) {
	asJoin := scorecardFor(TraceLine{Op: "join", Coll: "docs", Join: &TraceJoin{To: "perms", On: "id", Kind: "visibility"}})
	asAttribute := scorecardFor(TraceLine{Op: "list", Coll: "docs", Visibility: "object_global"})
	for name, card := range map[string]PlanScorecard{"join": asJoin, "attribute": asAttribute} {
		if card.Rules[2].Cell != "P" || card.Rules[3].Cell != "F" {
			t.Errorf("%s: rule 3 %s, rule 4 %s, want P and F", name, card.Rules[2].Cell, card.Rules[3].Cell)
		}
	}
}

// TestSubstringFailsUnlessSearchIsDelegated: a substring match with no search
// statement for that kind is a fail of rule 1; with a search line for the kind
// it is the delegated pass, a partial. The old code covered every substring
// line, so the fail could never fire.
func TestSubstringFailsUnlessSearchIsDelegated(t *testing.T) {
	alone := scorecardFor(TraceLine{Op: "list", Coll: "users", Substring: true})
	if alone.Rules[0].Cell != "F" {
		t.Fatalf("a substring list with no delegated search: cell %s, want F", alone.Rules[0].Cell)
	}
	delegated := scorecardFor(TraceLine{Op: "list", Coll: "users", Substring: true}, TraceLine{Op: "search", Coll: "users"})
	if delegated.Rules[0].Cell != "P" {
		t.Fatalf("a substring list with a search line for the kind: cell %s, want P (delegated search is a pass)", delegated.Rules[0].Cell)
	}
	elsewhere := scorecardFor(TraceLine{Op: "list", Coll: "users", Substring: true}, TraceLine{Op: "search", Coll: "posts"})
	if elsewhere.Rules[0].Cell != "F" {
		t.Fatalf("search delegated for another kind covers nothing: cell %s, want F", elsewhere.Rules[0].Cell)
	}
	admin := scorecardFor(TraceLine{Op: "list", Coll: "users", Substring: true, Surface: "admin"})
	if admin.Rules[0].Cell != "p" {
		t.Fatalf("an admin substring list: cell %s, want p", admin.Rules[0].Cell)
	}
}

// TestConfidenceNamesEveryPartial: a partial cell never reads as "no rule found
// a trace line"; the setting sort names its line, and a delegated search is a
// pass that says so.
func TestConfidenceNamesEveryPartial(t *testing.T) {
	card := scorecardFor(
		TraceLine{Op: "list", Coll: "entry", SortFrom: "setting"},
		TraceLine{Op: "search", Coll: "entry"},
		TraceLine{Op: "list", Coll: "entry", Substring: true})
	if card.Rules[1].Cell != "p" || len(card.Rules[1].Evidence) != 1 || card.Rules[1].Evidence[0] != 1 {
		t.Fatalf("rule 2: %+v, want a partial citing trace line 1", card.Rules[1])
	}
	if card.Rules[0].Cell != "P" || !strings.Contains(card.Rules[0].Counts, "delegated") {
		t.Fatalf("rule 1: %+v, want a pass that says the search is delegated", card.Rules[0])
	}
	text := planDecidedBy(card)
	if strings.Contains(text, "No rule found") || !strings.Contains(text, "rule 2") {
		t.Fatalf("decided-by text %q", text)
	}
}

// TestSearchAloneAndBesideSubstringShowTheSameCell (m-II-1): delegated search
// is a pass by the spec, whether or not a substring line sits beside it.
func TestSearchAloneAndBesideSubstringShowTheSameCell(t *testing.T) {
	alone := scorecardFor(TraceLine{Op: "search", Coll: "entry"})
	beside := scorecardFor(TraceLine{Op: "search", Coll: "entry"}, TraceLine{Op: "list", Coll: "entry", Substring: true})
	if alone.Rules[0].Cell != "P" || beside.Rules[0].Cell != "P" {
		t.Fatalf("cells %s and %s, want P and P", alone.Rules[0].Cell, beside.Rules[0].Cell)
	}
}

// TestSearchCoversTheSameFieldWhenBothNameOne (m-II-2).
func TestSearchCoversTheSameFieldWhenBothNameOne(t *testing.T) {
	other := scorecardFor(
		TraceLine{Op: "search", Coll: "user", By: []string{"title"}},
		TraceLine{Op: "list", Coll: "user", By: []string{"email"}, Substring: true})
	if other.Rules[0].Cell != "F" {
		t.Fatalf("a search on title covered a substring on email: cell %s", other.Rules[0].Cell)
	}
	same := scorecardFor(
		TraceLine{Op: "search", Coll: "user", By: []string{"email"}},
		TraceLine{Op: "list", Coll: "user", By: []string{"email"}, Substring: true})
	if same.Rules[0].Cell != "P" {
		t.Fatalf("a search on the same field: cell %s, want P", same.Rules[0].Cell)
	}
}

// TestUnknownFanReachesTheScorecardAndTheLimits: a container predicate with no
// fans entry is an assumption, and the plan says so in rule 4 and in the limits.
func TestUnknownFanReachesTheScorecardAndTheLimits(t *testing.T) {
	model := traceModel(TraceLine{Op: "list", Coll: "cards", Visibility: "container"})
	model.Kinds = []ModelKind{{Name: "cards", Documents: 1, Sampled: 1, Fields: []ModelField{{Name: "_id", Type: "string", PresentPermille: 1000}}}}
	plan := planFor(t, model, nil)
	rule := plan.Scorecard.Rules[3]
	if rule.Cell != "P" || !strings.Contains(rule.Counts, "unknown") || !strings.Contains(rule.Counts, "fans") {
		t.Fatalf("rule 4: %+v, want a pass that says the fan is unknown", rule)
	}
	text := renderMarkdown(plan)
	if strings.Count(text, "p95 is unknown") < 2 {
		t.Fatalf("the unknown fan must appear in the scorecard and in the limits:\n%s", text)
	}
}

// TestSubstringFailNamesItsCause: a substring match with no delegated search
// fails for that reason, not for optional filters.
func TestSubstringFailNamesItsCause(t *testing.T) {
	card := scorecardFor(TraceLine{Op: "list", Coll: "user", Substring: true})
	counts := card.Rules[0].Counts
	if card.Rules[0].Cell != "F" || !strings.Contains(counts, "substring") || strings.Contains(counts, "optional filters") {
		t.Fatalf("rule 1: %+v", card.Rules[0])
	}
}

// TestPlanJSONCarriesTheLimits: --json has the decided-by line and the limits, as the Markdown does.
func TestPlanJSONCarriesTheLimits(t *testing.T) {
	data := mustJSON(t, planFor(t, blogWithTrace(t, true), nil))
	for _, want := range []string{`"decided_by"`, `"limits"`, "how often each read runs"} {
		if !strings.Contains(data, want) {
			t.Errorf("the JSON plan lacks %s", want)
		}
	}
}

// TestMixedAtomicGroupCitesOnlyItsPrimaryLines: an admin line inside a primary
// group does not inflate the primary evidence.
func TestMixedAtomicGroupCitesOnlyItsPrimaryLines(t *testing.T) {
	card := scorecardFor(
		TraceLine{Op: "insert", Coll: "orders", AtomicWith: "g"},
		TraceLine{Op: "update", Coll: "stock", AtomicWith: "g", Surface: "admin"})
	if card.Rules[4].Cell != "F" || len(card.Rules[4].Evidence) != 2 || card.Rules[4].Counts != "1 primary read or write: writes to more than one kind that must commit together" {
		t.Fatalf("rule 5: %+v", card.Rules[4])
	}
}
