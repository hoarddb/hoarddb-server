package cli

import (
	"fmt"
	"slices"
	"strings"
)

// planRuleNames are the five disqualifiers of the corpus note, in order.
var planRuleNames = [5]string{"dynamic filters", "client sorting", "non-reducible joins", "permissions in the query", "atomic multi-entity writes"}

// PlanRule is one of the five disqualifiers: a cell and the counts behind it.
type PlanRule struct {
	Number   int    `json:"number"`
	Name     string `json:"name"`
	Cell     string `json:"cell"` // "P" pass, "p" partial (admin surface only, or delegated), "F" fail
	Counts   string `json:"counts"`
	Evidence []int  `json:"evidence,omitempty"`
}

// PlanScorecard is part 2 of the plan.
type PlanScorecard struct {
	Rules   [5]PlanRule `json:"rules"`
	Verdict string      `json:"verdict"`
	Reason  string      `json:"reason,omitempty"`
	// DecidedBy names the trace lines behind every non-pass cell; Limits says
	// what the verdict cannot see, so both Markdown and JSON carry the same honesty.
	DecidedBy string   `json:"decided_by"`
	Limits    []string `json:"limits"`
}

const verdictNo = "not a good candidate for hoardDB"

func planIsPrimary(line TraceLine) bool { return line.Surface != "admin" }

// planVisibilityReducible says whether a visibility predicate is reduced by
// spec section 16.2: owner, membership and object-scoped always; a container
// predicate when the fan is not above the merge bar.
func planVisibilityReducible(model *PlanModel, line TraceLine, th PlanThresholds) bool {
	switch line.Visibility {
	case "owner", "membership", "object_scoped":
		return true
	case "container":
		for _, fan := range model.Fans {
			if fan.Child == line.Coll && fan.P95 > th.ContainersMerge {
				return false
			}
		}
		return true
	}
	return false
}

// planNonReducibleJoin reports whether a join line counts for rule 3 (spec section 15).
func planNonReducibleJoin(model *PlanModel, line TraceLine, th PlanThresholds) bool {
	if line.Join == nil {
		return false
	}
	switch line.Join.Kind {
	case "nonkey", "sort":
		return true
	case "aggregate":
		return line.Dynamic
	}
	return false
}

// ruleCell folds a rule's primary and admin hits into a cell and a sentence.
// ruleCellRequest is one rule's hits: its number, the primary and admin lines
// that count against it, the delegated-search or stored-setting sentence when
// there is one, and what the counted lines are.
type ruleCellRequest struct {
	Number         int
	Primary        []int
	Admin          []int
	Delegated      string
	DelegatedLines []int
	What           string
}

func ruleCell(req ruleCellRequest) PlanRule {
	number, primary, admin, delegated, delegatedLines, what := req.Number, req.Primary, req.Admin, req.Delegated, req.DelegatedLines, req.What
	rule := PlanRule{Number: number, Name: planRuleNames[number-1], Cell: "P", Counts: "none found"}
	switch {
	case len(primary) > 0:
		rule.Cell = "F"
		rule.Counts = fmt.Sprintf("%d primary read or write: %s", len(primary), what)
		rule.Evidence = append(rule.Evidence, primary...)
	case len(admin) > 0:
		rule.Cell = "p"
		rule.Counts = fmt.Sprintf("%d admin-surface read or write only: %s", len(admin), what)
	case delegated != "" && rule.Number == 1:
		// Delegated search is a pass (spec section 6); the cell says why.
		rule.Counts = delegated
		rule.Evidence = append(rule.Evidence, delegatedLines...)
	case delegated != "":
		rule.Cell = "p"
		rule.Counts = delegated
		rule.Evidence = append(rule.Evidence, delegatedLines...)
	}
	rule.Evidence = append(rule.Evidence, admin...)
	slices.Sort(rule.Evidence)
	return rule
}

// buildScorecard turns the signals and the trace into five cells and a verdict.
func buildScorecard(model *PlanModel, signals []PlanSignal, th PlanThresholds) PlanScorecard {
	var card PlanScorecard
	searchCovered := map[int]bool{}
	for _, signal := range signals {
		if signal.Detector == "R-external-search" {
			for _, line := range signal.Lines {
				searchCovered[line] = true
			}
		}
	}
	var filtersP, filtersA, substrP, substrA, sortP, sortA, joinP, joinA, visP, visA, atomicP, atomicA []int
	var unknownFans []int
	var sortSettingLines []int
	groups := map[string][]TraceLine{}
	for _, line := range model.Trace {
		primary := planIsPrimary(line)
		add := func(primaryList, adminList *[]int) {
			if primary {
				*primaryList = append(*primaryList, line.Ordinal)
			} else {
				*adminList = append(*adminList, line.Ordinal)
			}
		}
		optional := 0
		for _, filter := range line.Filters {
			if filter.Optional {
				optional++
			}
		}
		if line.Op == "list" && optional >= th.OptionalFilterFail {
			add(&filtersP, &filtersA)
		}
		// A substring match fails on any operation that carries it, unless a
		// search line delegates it (stated as intended in the build note).
		if line.Substring && !searchCovered[line.Ordinal] {
			add(&substrP, &substrA)
		}
		if (line.Visibility == "container" || line.Visibility == "object_global") && !planHasFan(model, line.Coll) {
			unknownFans = append(unknownFans, line.Ordinal)
		}
		if line.Op == "list" && line.SortFrom == "client" {
			add(&sortP, &sortA)
		}
		if line.Op == "list" && line.SortFrom == "setting" {
			sortSettingLines = append(sortSettingLines, line.Ordinal)
		}
		if planNonReducibleJoin(model, line, th) {
			add(&joinP, &joinA)
		}
		if line.Visibility == "object_global" || (line.Visibility == "container" && !planVisibilityReducible(model, line, th)) ||
			planNonReducibleVisibility(model, line, th) {
			add(&visP, &visA)
		}
		if line.AtomicWith != "" {
			groups[line.AtomicWith] = append(groups[line.AtomicWith], line)
		}
	}
	for _, id := range planSortedNames(groups) {
		kinds, guarded := map[string]bool{}, false
		for _, line := range groups[id] {
			kinds[line.Coll] = true
			guarded = guarded || len(line.Guard) > 0
		}
		if len(kinds) < 2 || guarded {
			continue
		}
		for _, line := range groups[id] {
			if planIsPrimary(line) {
				atomicP = append(atomicP, line.Ordinal)
			} else {
				atomicA = append(atomicA, line.Ordinal)
			}
		}

	}
	rule1 := "lists compose optional filters at run time; a store reads by the key it was built for"
	if len(substrP)+len(substrA) > 0 && len(filtersP)+len(filtersA) == 0 {
		rule1 = "a substring match with no delegated search: add a search line for the kind, or serve it with a search engine"
	} else if len(substrP)+len(substrA) > 0 {
		rule1 += "; and a substring match with no delegated search"
	}
	card.Rules[0] = ruleCell(ruleCellRequest{Number: 1, Primary: append(filtersP, substrP...), Admin: append(filtersA, substrA...), Delegated: planIf(len(searchCovered) > 0, "text search is delegated to a search engine (a pass)"), DelegatedLines: planSortedOrdinals(searchCovered), What: rule1})
	card.Rules[1] = ruleCell(ruleCellRequest{Number: 2, Primary: sortP, Admin: sortA, Delegated: planIf(len(sortSettingLines) > 0, "the sort field comes from a stored setting, so it is one order per owner"), DelegatedLines: sortSettingLines, What: "the client chooses the sort field; a store keeps one order"})
	card.Rules[2] = ruleCell(ruleCellRequest{Number: 3, Primary: joinP, Admin: joinA, What: "joins that are not a fetch by the far side's key"})
	card.Rules[3] = ruleCell(ruleCellRequest{Number: 4, Primary: visP, Admin: visA, What: "visibility predicates that a read starting at the principal cannot serve"})
	card.Rules[4] = ruleCell(ruleCellRequest{Number: 5, Primary: atomicP, Admin: atomicA, What: "writes to more than one kind that must commit together"})
	card.Verdict, card.Reason = planVerdict(model, signals, card, th)
	card.DecidedBy = planDecidedBy(card)
	card.Limits = planLimits(unknownFans)
	if len(unknownFans) > 0 {
		unknown := fmt.Sprintf("containers per principal p95 is unknown for %s (add a fans entry): the cell assumes it fits", planOrdinals(unknownFans))
		rule := &card.Rules[3]
		if rule.Cell == "P" && rule.Counts == "none found" {
			rule.Counts = unknown
		} else {
			rule.Counts += "; " + unknown
		}
		rule.Evidence = append(rule.Evidence, unknownFans...)
		slices.Sort(rule.Evidence)
	}
	return card
}

// planVerdict applies the build note's verdict rule (R1, labelled NEW):
// "not a good candidate" needs, on one primary list, AdhocOptionalFilters or
// more optional filters, a client-chosen sort, and either a wide visibility
// predicate or a dynamic line. Otherwise any F asks for a redesign of the
// failing rules, any p is a caveat, and no cell at all is a plain yes. The reason
// is the note of the first failing rule. This is a deliberate reading of the
// inputs, not a measurement: Confidence says what it cannot see.
func planVerdict(model *PlanModel, signals []PlanSignal, card PlanScorecard, th PlanThresholds) (string, string) {
	for _, line := range model.Trace {
		optional := 0
		for _, filter := range line.Filters {
			if filter.Optional {
				optional++
			}
		}
		wide := line.Visibility == "container" || line.Visibility == "object_global" || line.Dynamic
		if line.Op == "list" && planIsPrimary(line) && optional >= th.AdhocOptionalFilters && line.SortFrom == "client" && wide {
			return verdictNo, fmt.Sprintf("ad-hoc query surface on trace line %d: %d optional filters and a client-chosen sort, with a wide visibility predicate or a dynamic line", line.Ordinal, optional)
		}
	}
	var failing []string
	for _, rule := range card.Rules {
		if rule.Cell == "F" {
			failing = append(failing, rule.Name)
		}
	}
	if len(failing) > 0 {
		first := slices.IndexFunc(card.Rules[:], func(r PlanRule) bool { return r.Cell == "F" })
		return "only with redesign of " + strings.Join(failing, ", "), card.Rules[first].Counts
	}
	for _, rule := range card.Rules {
		if rule.Cell == "p" {
			return "yes with caveats", rule.Counts
		}
	}
	return "yes", ""
}

func planIf(condition bool, text string) string {
	if condition {
		return text
	}
	return ""
}

// planNonReducibleVisibility reports a join that applies a visibility
// predicate no rule of spec section 16.2 reduces. It counts for rule 4 only:
// one permission predicate is one fail, whether the extractor saw it as a join
// or as an attribute of the read.
func planNonReducibleVisibility(model *PlanModel, line TraceLine, th PlanThresholds) bool {
	return line.Join != nil && line.Join.Kind == "visibility" && !planVisibilityReducible(model, line, th)
}

func planSortedOrdinals(set map[int]bool) []int {
	var out []int
	for ordinal := range set {
		out = append(out, ordinal)
	}
	slices.Sort(out)
	return out
}

func planHasFan(model *PlanModel, child string) bool {
	return slices.ContainsFunc(model.Fans, func(fan ModelFan) bool { return fan.Child == child })
}

// planLimits is what every verdict cannot see, and any assumption it made.
func planLimits(unknownFans []int) []string {
	limits := []string{
		"This verdict is built on what the inputs claim: the sample, the model and the trace. It does not see how often each read runs (only the `weight` you gave), which lines are on the admin surface (only the `surface` you gave), the counts and fans you supplied, or any query you left out. A line left out can only make the verdict kinder.",
		"To change a verdict, change the design the trace describes and then update the trace. Editing a line to move a cell changes the claim, not the application.",
	}
	if len(unknownFans) > 0 {
		limits = append(limits, fmt.Sprintf("Containers per principal p95 is unknown for %s: rule 4 assumes it fits. Add a fans entry to the model to test that.", planOrdinals(unknownFans)))
	}
	return limits
}
