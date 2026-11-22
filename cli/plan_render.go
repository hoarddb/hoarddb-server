package cli

import (
	"encoding/json"
	"fmt"
	"strings"
)

// planMarker is the first line of every plan file, so --out can refuse to
// overwrite a file the advisor did not write.
const planMarker = "<!-- hoarddb-plan v1 -->"

// renderMarkdown writes the plan: marker, then the seven parts in spec section
// 14 order, then the two commands the developer runs next.
func renderMarkdown(plan Plan) string {
	var out strings.Builder
	line := func(format string, args ...any) { fmt.Fprintf(&out, format+"\n", args...) }
	line("%s", planMarker)
	line("# hoardDB plan\n")
	line("A suggestion, not a change: nothing here was created, and you pick every structure.\n")

	line("## 1. Inputs\n")
	for _, input := range plan.Inputs {
		line("- %s", input)
	}

	line("\n## 2. Scorecard\n")
	line("| # | Rule | Cell | Finding |")
	line("| --- | --- | --- | --- |")
	for _, rule := range plan.Scorecard.Rules {
		line("| %d | %s | %s | %s |", rule.Number, rule.Name, rule.Cell, rule.Counts)
	}
	line("\nP is a pass, p is partial (admin surface only, or a sort that comes from a setting), F is a fail.\n")
	line("### Verdict: %s", plan.Scorecard.Verdict)
	if plan.Scorecard.Reason != "" {
		line("\n%s", plan.Scorecard.Reason)
	}

	line("\n#### Confidence and limits\n")
	line("%s", plan.Scorecard.DecidedBy)
	for _, limit := range plan.Scorecard.Limits {
		line("%s", limit)
	}

	line("\n## 3. Query reductions\n")
	if len(plan.Reductions) == 0 {
		line("No traced read has a reduction yet. Add trace lines to see them.")
	}
	for _, row := range plan.Reductions {
		needs := ""
		if row.Needs != "" {
			needs = "  needs " + row.Needs
		}
		line("- %s (%s)\n  - source: `%s`\n  - hoardDB: `%s`\n  - cost: %s%s", row.Shape, planOrdinals(row.Lines), row.Source, row.Replacement, row.CostClass, needs)
	}

	line("\n## 4. Kinds and suggestions\n")
	for _, kind := range plan.Kinds {
		renderKind(&out, kind)
	}

	line("## 5. Will not port\n")
	if len(plan.WontPort) == 0 {
		line("Nothing in the inputs is outside what hoardDB serves.")
	}
	for _, text := range plan.WontPort {
		line("- %s", text)
	}

	line("\n## 6. Cost\n")
	line("Computed from the formats, not measured. A bucket costs no file, descriptor, mapping or goroutine.\n")
	line("| Design | Buckets | Store fields | Declaration bytes | Documents | Store elements | Keys per document |")
	line("| --- | --- | --- | --- | --- | --- | --- |")
	for _, cost := range plan.Cost {
		approx := ""
		if cost.Approx {
			approx = "≈ "
		}
		line("| %s | %d | %d | %d | %s%s | %s%s | %s |", cost.Design, cost.Buckets, cost.StoreFields, cost.DeclBytes, approx, planCount(cost.Documents), approx, planCount(cost.StoreElements), cost.KeysPerDoc)
	}
	for _, cost := range plan.Cost {
		line("\n%s: reads that scan today or cannot be served: %s", cost.Design, planList(cost.Scans))
	}
	for _, text := range plan.Locality {
		line("\nReplicate for locality: %s", text)
	}

	line("\n## 7. HQL\n")
	if names := planAlternativeBuckets(plan); len(names) > 0 {
		verb := "choices are"
		if plan.OpenChoices == 1 {
			verb = "choice is"
		}
		line("%d %s open; this section has more than one statement for `%s`. Pick one.\n", plan.OpenChoices, verb, strings.Join(names, "`, `"))
	}
	for _, statement := range plan.HQL {
		line("### %s\n", statement.Heading)
		line("```text\n%s", statement.Text)
		for _, mark := range statement.Marks {
			line("%s", mark)
		}
		line("```\n")
	}

	line("## Next\n")
	line("1. In a hoardDB shell, pick a database: `create database app`, then `use app`.")
	line("2. Run the statement you picked for each bucket.")
	line("3. Load each file into its bucket: `hoarddb ingest <file> --into app.<bucket>`.")
	return out.String()
}

// planDecidedBy names the trace lines behind every failing or partial cell.
func planDecidedBy(card PlanScorecard) string {
	var parts []string
	for _, rule := range card.Rules {
		if rule.Cell != "P" && len(rule.Evidence) > 0 {
			parts = append(parts, fmt.Sprintf("rule %d (%s, %s) from %s", rule.Number, rule.Name, rule.Cell, planOrdinals(rule.Evidence)))
		}
	}
	if len(parts) == 0 {
		return "No rule found a trace line to count against."
	}
	return "Decided by: " + strings.Join(parts, "; ") + "."
}

func planOrdinals(lines []int) string {
	if len(lines) == 0 {
		return "no trace line"
	}
	parts := make([]string, len(lines))
	for index, ordinal := range lines {
		parts[index] = fmt.Sprint(ordinal)
	}
	return "trace line " + strings.Join(parts, ", ")
}

func planList(items []string) string {
	if len(items) == 0 {
		return "none"
	}
	return strings.Join(items, "; ")
}

// planAlternativeBuckets names the buckets that have more than one statement.
func planAlternativeBuckets(plan Plan) []string {
	var names []string
	for _, kind := range plan.Kinds {
		for _, statement := range plan.HQL {
			if statement.Alternative && strings.HasPrefix(statement.Heading, kind.Name+" ") {
				names = append(names, kind.Name)
				break
			}
		}
	}
	for _, statement := range plan.HQL {
		if statement.Alternative && strings.Contains(statement.Heading, "as its own bucket") {
			names = append(names, strings.SplitN(statement.Heading, " ", 2)[0])
		}
	}
	return names
}

// renderKind writes one suggestion block: why, evidence, open choices.
func renderKind(out *strings.Builder, kind PlanKind) {
	fmt.Fprintf(out, "### %s (from %s; grade: %s)\n\n", kind.Name, strings.Join(kind.Sources, ", "), kind.Grade)
	for _, store := range kind.StoreFields {
		word := store.Store
		if word == "" {
			word = "order open: " + strings.Join(store.Alternatives, " or ")
		}
		fmt.Fprintf(out, "- %s is a store field (%s; grade: %s), from %s\n", store.Name, word, store.Grade, store.Source)
		for _, signal := range store.Evidence {
			fmt.Fprintf(out, "  - [%s] %s\n", signal.Detector, signal.Note)
		}
		if store.Grade == GradeConflict {
			fmt.Fprintf(out, "  - alternatives: %s\n", strings.Join(store.Placements, "; "))
		}
	}
	for _, signal := range kind.Evidence {
		if signal.Detector == "S1" || signal.Detector == "T1" || signal.Detector == "T6" {
			fmt.Fprintf(out, "- [%s] %s\n", signal.Detector, signal.Note)
		}
	}
	for _, choice := range kind.Open {
		fmt.Fprintf(out, "- open choice: %s\n", choice)
	}
	fmt.Fprintln(out)
}

// renderJSON writes the seven parts as one object, keys in the spec order.
func renderJSON(plan Plan) ([]byte, error) {
	doc := struct {
		Inputs     []string        `json:"inputs"`
		Scorecard  PlanScorecard   `json:"scorecard"`
		Reductions []PlanReduction `json:"reductions"`
		Kinds      []PlanKind      `json:"suggestions"`
		WontPort   []string        `json:"wont_port"`
		Cost       []PlanCost      `json:"cost"`
		Locality   []string        `json:"locality,omitempty"`
		HQL        []PlanStatement `json:"hql"`
	}{plan.Inputs, plan.Scorecard, plan.Reductions, plan.Kinds, plan.WontPort, plan.Cost, plan.Locality, plan.HQL}
	encoded, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("render plan as JSON: %w", err)
	}
	return append(encoded, '\n'), nil
}
