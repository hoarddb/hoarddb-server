package cli

import (
	"fmt"
	"slices"
	"strings"
)

// PlanReduction is one row of part 3: a source shape and the hoardDB read that replaces it.
type PlanReduction struct {
	Shape       string `json:"shape"`
	Source      string `json:"source"`
	Replacement string `json:"replacement"`
	CostClass   string `json:"cost_class"`
	Needs       string `json:"needs,omitempty"`
	Lines       []int  `json:"lines,omitempty"`
}

// PlanCost is part 6's table for one design.
type PlanCost struct {
	Design        string   `json:"design"`
	Buckets       int      `json:"buckets"`
	StoreFields   int      `json:"store_fields"`
	RetiredFields int      `json:"retired_fields"`
	DeclBytes     int64    `json:"declaration_bytes"`
	Documents     int64    `json:"documents"`
	Approx        bool     `json:"approximate"`
	StoreElements int64    `json:"store_elements"`
	KeysPerDoc    string   `json:"keys_per_document"`
	Scans         []string `json:"scans_today,omitempty"`
}

// PlanStatement is one complete create bucket statement under its own heading;
// Marks are the `-- ` lines printed after it.
type PlanStatement struct {
	Heading     string   `json:"heading"`
	Text        string   `json:"text"`
	Marks       []string `json:"marks,omitempty"`
	Alternative bool     `json:"alternative"`
}

// Plan is the whole result; parts are in the spec section 14 order.
type Plan struct {
	Inputs      []string        `json:"inputs"`
	Scorecard   PlanScorecard   `json:"scorecard"`
	Reductions  []PlanReduction `json:"reductions"`
	Kinds       []PlanKind      `json:"suggestions"`
	WontPort    []string        `json:"wont_port"`
	Cost        []PlanCost      `json:"cost"`
	Locality    []string        `json:"locality,omitempty"`
	HQL         []PlanStatement `json:"hql"`
	OpenChoices int             `json:"open_choices"`
}

// planReductionShapes are the shape names of spec section 15's join table, by
// the join kind an extractor writes.
var planReductionShapes = map[string]string{
	"pk":         "join to a row by its primary key",
	"nonkey":     "join on a non-key column",
	"visibility": "join to apply a visibility predicate",
	"aggregate":  "join to aggregate",
	"sort":       "join to sort by the far side",
}

// planCostClasses are the cost words a reduction row may carry.
var planCostClasses = []string{"seek", "keyed batch", "merge", "scatter", "scan"}

func planCount(n int64) string {
	text := fmt.Sprint(n)
	for i := len(text) - 3; i > 0; i -= 3 {
		text = text[:i] + "," + text[i:]
	}
	return text
}

func planLineText(line TraceLine) string {
	parts := []string{line.Op, line.Coll}
	if len(line.By) > 0 {
		parts = append(parts, "by "+strings.Join(line.By, ", "))
	}
	if len(line.Sort) > 0 {
		parts = append(parts, "sort "+strings.Join(line.Sort, ", "))
	}
	if line.Limit > 0 {
		parts = append(parts, fmt.Sprintf("limit %d", line.Limit))
	}
	return strings.Join(parts, " ")
}

// buildPlan assembles the seven parts. It is pure: the same inputs give the same Plan.
func buildPlan(model *PlanModel, signals []PlanSignal, kinds []PlanKind, card PlanScorecard, th PlanThresholds) Plan {
	plan := Plan{Scorecard: card, Kinds: kinds}
	plan.Inputs = planInputs(model)
	plan.Reductions, plan.WontPort = planReductions(model, signals, kinds, th)
	plan.Cost = planCosts(model, signals, kinds, th)
	plan.Locality = planLocality(model, kinds, th)
	plan.HQL = planStatements(model, signals, kinds)
	for _, kind := range kinds {
		plan.OpenChoices += len(kind.Open)
	}
	return plan
}

func planInputs(model *PlanModel) []string {
	inputs := []string{fmt.Sprintf("source: %s; thresholds version %s", model.Source, PlanThresholdsVersion)}
	for _, kind := range planKindsByName(model) {
		inputs = append(inputs, fmt.Sprintf("%s: %s documents (%s sampled)", kind.Name, planCount(kind.Documents), planCount(kind.Sampled)))
	}
	inputs = append(inputs, fmt.Sprintf("trace: %d lines", len(model.Trace)))
	return inputs
}

// planStoreFor finds the store field a kind became, and the bucket that holds it.
func planStoreFor(kinds []PlanKind, source string) (*PlanKind, *PlanStoreField) {
	for ki := range kinds {
		for si := range kinds[ki].StoreFields {
			if kinds[ki].StoreFields[si].Source == source {
				return &kinds[ki], &kinds[ki].StoreFields[si]
			}
		}
	}
	return nil, nil
}

func planBucketFor(kinds []PlanKind, source string) *PlanKind {
	for ki := range kinds {
		if slices.Contains(kinds[ki].Sources, source) {
			return &kinds[ki]
		}
	}
	return nil
}

// planReductions makes part 3 and part 5: one row per traced read that has a
// replacement, and a will-not-port line for every shape that has none.
func planReductions(model *PlanModel, signals []PlanSignal, kinds []PlanKind, th PlanThresholds) ([]PlanReduction, []string) {
	reductions := []PlanReduction{}
	wont := []string{}
	for _, line := range model.Trace {
		source := planLineText(line)
		lines := []int{line.Ordinal}
		if line.Join != nil {
			shape := planReductionShapes[line.Join.Kind]
			source = fmt.Sprintf("join %s to %s on %s", line.Coll, line.Join.To, line.Join.On)
			switch {
			case line.Join.Kind == "pk":
				reductions = append(reductions, PlanReduction{shape, source, `keep the id; db.<Bucket>["<id>"].<Field>.list({expand: true})`, "keyed batch", "F6 (not available yet)", lines})
			case planNonReducibleJoin(model, line, th), planNonReducibleVisibility(model, line, th):
				wont = append(wont, fmt.Sprintf("%s (%s, trace line %d): no reduction; serve it in the application or an external engine", source, shape, line.Ordinal))
			case line.Join.Kind == "aggregate":
				reductions = append(reductions, PlanReduction{shape, source, "a maintained count or sum on the owning document", "seek", "F2 (not available yet)", lines})
			default:
				reductions = append(reductions, PlanReduction{shape, source, "start at the principal: owned reads, then expand", "merge", "F6 (not available yet)", lines})
			}
			continue
		}
		if line.Op == "search" || line.Substring {
			wont = append(wont, fmt.Sprintf("%s (trace line %d): text search; use a search engine for it", source, line.Ordinal))
			continue
		}
		reductions = append(reductions, planLineReductions(line, source, signals, kinds)...)
	}
	for _, signal := range signals {
		if signal.Detector == "R-transactions" && strings.Contains(signal.Other, ",") {
			wont = append(wont, fmt.Sprintf("%s: writes to several kinds that must commit together; fold them into one document or keep the application's transaction", signal.Note))
		}
	}
	slices.SortStableFunc(reductions, func(a, b PlanReduction) int { return slices.Compare(a.Lines, b.Lines) })
	return reductions, wont
}

// planLineReductions turns one traced read into its reduction row, using the
// detector that classified it.
func planLineReductions(line TraceLine, source string, signals []PlanSignal, kinds []PlanKind) []PlanReduction {
	var rows []PlanReduction
	for _, signal := range signals {
		if !slices.Contains(signal.Lines, line.Ordinal) || signal.Kind != line.Coll {
			continue
		}
		lines := []int{line.Ordinal}
		switch signal.Detector {
		case "T1":
			if bucket := planBucketFor(kinds, line.Coll); bucket != nil {
				rows = append(rows, PlanReduction{"key read", source, fmt.Sprintf(`db.%s["<id>"].get()`, bucket.Name), "seek", "", lines})
			}
		case "T2", "T3", "T4", "T5":
			owner, store := planStoreFor(kinds, line.Coll)
			if owner == nil {
				continue
			}
			word := store.Store
			if word == "" {
				word = "<store>"
			}
			call := ".list(" + planLimitArg(line) + ")"
			switch signal.Detector {
			case "T3":
				call = `.get("<key>")`
			case "T5":
				call = ".pop()"
			}
			shape := "child rows by parent key, ordered"
			if signal.Detector == "T3" {
				shape = "child rows by parent key and one more key"
			}
			rows = append(rows, PlanReduction{shape, source, fmt.Sprintf(`db.%s["<id>"].%s%s  (%s)`, owner.Name, store.Name, call, word), "seek", "", lines})
		case "T6":
			bucket := planBucketFor(kinds, line.Coll)
			if bucket == nil {
				rows = append(rows, PlanReduction{"read across owners", source, "not served by the store field; choose the bucket design", "scan", "indexes (not available yet)", lines})
				continue
			}
			rows = append(rows, PlanReduction{"read across owners", source,
				fmt.Sprintf("db.%s.list({filter: {%s: ...}})", bucket.Name, planTitle(signal.Field)), "scan", "indexes (not available yet)", lines})
		}
	}
	return rows
}

func planLimitArg(line TraceLine) string {
	if line.Limit > 0 {
		return fmt.Sprintf("{limit: %d}", line.Limit)
	}
	return ""
}

// planCosts prices every design the plan prints (spec section 7.4): the one
// placed, and, when a choice is a conflict, the bucket design and the both design.
func planCosts(model *PlanModel, signals []PlanSignal, kinds []PlanKind, th PlanThresholds) []PlanCost {
	byName := map[string]ModelKind{}
	approx := false
	for _, kind := range model.Kinds {
		byName[kind.Name] = kind
		approx = approx || kind.Sampled < kind.Documents
	}
	conflicts := []string{}
	for _, kind := range kinds {
		for _, store := range kind.StoreFields {
			if store.Grade == GradeConflict {
				conflicts = append(conflicts, store.Source)
			}
		}
	}
	designs := []struct {
		name    string
		promote func(string) bool
		both    bool
	}{{"Design A (as suggested)", nil, false}}
	if len(conflicts) > 0 {
		designs = append(designs,
			struct {
				name    string
				promote func(string) bool
				both    bool
			}{"Design B (each conflict as its own bucket)", func(s string) bool { return slices.Contains(conflicts, s) }, false},
			struct {
				name    string
				promote func(string) bool
				both    bool
			}{"Design C (both: store field and bucket, written twice)", func(s string) bool { return slices.Contains(conflicts, s) }, true})
	}
	var costs []PlanCost
	for _, design := range designs {
		cost := PlanCost{Design: design.name, Approx: approx, Scans: []string{}}
		var keys []string
		for _, kind := range kinds {
			cost.Buckets++
			cost.Documents += planDocuments(kind, byName)
			scalars := int64(len(kind.Fields))
			var elements int64
			for _, store := range kind.StoreFields {
				if design.promote != nil && design.promote(store.Source) && !design.both {
					continue
				}
				cost.StoreFields++
				elements += planStoreElements(store, byName)
			}
			cost.StoreElements += elements
			perDoc := int64(0)
			if docs := planDocuments(kind, byName); docs > 0 {
				perDoc = elements / docs
			}
			keys = append(keys, fmt.Sprintf("%s 1+%d+%d", kind.Name, scalars, perDoc))
		}
		if design.promote != nil {
			for _, source := range conflicts {
				cost.Buckets++
				cost.Documents += byName[source].Documents
			}
		}
		cost.KeysPerDoc = strings.Join(keys, ", ")
		cost.DeclBytes = int64(cost.Buckets*th.BucketDeclBytes + cost.StoreFields*th.StoreFieldBytes + cost.RetiredFields*th.RetiredFieldBytes)
		cost.Scans = planScans(model, signals, kinds, conflicts, design.promote != nil)
		costs = append(costs, cost)
	}
	return costs
}

// planDocuments counts the documents a bucket holds in its own right.
func planDocuments(kind PlanKind, byName map[string]ModelKind) int64 {
	var total int64
	for _, source := range kind.Sources {
		total += byName[source].Documents
	}
	return total
}

// planStoreElements counts the elements of one store field: the documents of
// the owned kind, or the lists' typical length times the owner's documents.
func planStoreElements(store PlanStoreField, byName map[string]ModelKind) int64 {
	if kind, ok := byName[store.Source]; ok {
		return kind.Documents
	}
	owner, field, _ := strings.Cut(store.Source, ".")
	for _, candidate := range byName[owner].Fields {
		if candidate.Name == field && candidate.Array != nil {
			return byName[owner].Documents * int64(candidate.Array.LenP50)
		}
	}
	return 0
}

// planScans lists the traced reads that read every document of a bucket today.
func planScans(model *PlanModel, signals []PlanSignal, kinds []PlanKind, conflicts []string, promoted bool) []string {
	scans := []string{}
	for _, signal := range signals {
		if signal.Detector != "T6" {
			continue
		}
		for _, ordinal := range signal.Lines {
			line := model.Trace[slices.IndexFunc(model.Trace, func(l TraceLine) bool { return l.Ordinal == ordinal })]
			text := fmt.Sprintf("%s (weight %d)", planLineText(line), planTraceWeight(line))
			if slices.Contains(conflicts, signal.Kind) && !promoted {
				text = "not possible with the store field: " + text
			} else {
				text = "scan: " + text
			}
			scans = append(scans, text)
		}
	}
	if promoted {
		for _, signal := range signals {
			if signal.Detector == "T2" && slices.Contains(conflicts, signal.Kind) {
				for _, ordinal := range signal.Lines {
					line := model.Trace[slices.IndexFunc(model.Trace, func(l TraceLine) bool { return l.Ordinal == ordinal })]
					scans = append(scans, fmt.Sprintf("scan of every %s document: %s (weight %d)", signal.Kind, planLineText(line), planTraceWeight(line)))
				}
			}
		}
	}
	slices.Sort(scans)
	return scans
}

// planLocality names the small, read-heavy kinds that sit on the far side of a
// primary-key join (spec section 16.3).
func planLocality(model *PlanModel, kinds []PlanKind, th PlanThresholds) []string {
	reads, writes := map[string]int{}, map[string]int{}
	far := map[string]bool{}
	for _, line := range model.Trace {
		switch line.Op {
		case "insert", "update", "delete", "put":
			writes[line.Coll] += planTraceWeight(line)
		default:
			reads[line.Coll] += planTraceWeight(line)
		}
		if line.Join != nil && line.Join.Kind == "pk" {
			far[line.Join.To] = true
		}
	}
	lines := []string{}
	for _, kind := range planKindsByName(model) {
		bucket := planBucketFor(kinds, kind.Name)
		if bucket == nil || !far[kind.Name] || kind.Documents > int64(th.LocalityDocuments) || reads[kind.Name] < th.LocalityReadRatio*max(writes[kind.Name], 1) {
			continue
		}
		lines = append(lines, fmt.Sprintf("%s (%s documents, read weight %d, write weight %d): put it in a reference database at full replication with local reads; storage and write fan-out grow by the cluster size, and a local read may lag by the stale-read bound", bucket.Name, planCount(kind.Documents), reads[kind.Name], writes[kind.Name]))
	}
	return lines
}

// planSafeName keeps letters and digits and never starts with a digit.
func planSafeName(name string) string {
	var out strings.Builder
	for _, r := range name {
		if r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			out.WriteRune(r)
		}
	}
	text := out.String()
	if text == "" || (text[0] >= '0' && text[0] <= '9') {
		text = "F" + text
	}
	return text
}

// planStatements writes one complete create bucket statement per bucket, then
// one complete statement per alternative under its own heading.
func planStatements(model *PlanModel, signals []PlanSignal, kinds []PlanKind) []PlanStatement {
	statements := []PlanStatement{}
	byName := map[string]ModelKind{}
	for _, kind := range model.Kinds {
		byName[kind.Name] = kind
	}
	for _, kind := range kinds {
		statements = append(statements, planStatement(planStatementRequest{Heading: kind.Name, Bucket: kind.Name, Kind: kind, Signals: signals}))
		for index, store := range kind.StoreFields {
			if store.Orderless {
				for _, word := range store.Alternatives[min(1, len(store.Alternatives)):] {
					swap := map[int]string{index: word}
					statements = append(statements, planStatement(planStatementRequest{Heading: fmt.Sprintf("%s (%s as %s)", kind.Name, store.Name, word), Bucket: kind.Name, Kind: kind, Swap: swap, Signals: signals, Alternative: true}))
				}
			}
			if store.Grade == GradeConflict {
				source := byName[store.Source]
				alternative := PlanKind{Name: planBucketName(store.Source), Sources: []string{store.Source}}
				for _, field := range source.Fields {
					if field.Name != "_id" {
						alternative.Fields = append(alternative.Fields, PlanField{Name: planTitle(field.Name), Type: planColumnType(field.Type), Source: field.Name})
					}
				}
				statement := planStatement(planStatementRequest{Heading: fmt.Sprintf("%s (alternative: %s as its own bucket)", alternative.Name, store.Name), Bucket: alternative.Name, Kind: alternative, Signals: signals, Alternative: true})
				statement.Marks = append([]string{fmt.Sprintf("-- instead of the %s field above; with both, keep %s and add this bucket", store.Name, kind.Name)}, statement.Marks...)
				statements = append(statements, statement)
			}
		}
	}
	return statements
}

// planStatementRequest is what one create statement is built from. A struct
// rather than positional arguments, because the statement's callers differ only
// in which of these fields they set.
type planStatementRequest struct {
	Heading     string
	Bucket      string
	Kind        PlanKind
	Swap        map[int]string
	Signals     []PlanSignal
	Alternative bool
}

func planStatement(req planStatementRequest) PlanStatement {
	heading, bucket, kind, swap, signals, alternative := req.Heading, req.Bucket, req.Kind, req.Swap, req.Signals, req.Alternative
	var parts, marks []string
	for _, field := range kind.Fields {
		parts = append(parts, planSafeName(field.Name)+" "+field.Type)
		if field.Planned != "" {
			marks = append(marks, fmt.Sprintf("-- not available yet: %s %s (a unique field cannot be declared at create)", field.Planned, planSafeName(field.Name)))
		}
	}
	for index, store := range kind.StoreFields {
		word := store.Store
		if swapped, ok := swap[index]; ok {
			word = swapped
		} else if word == "" && len(store.Alternatives) > 0 {
			word = store.Alternatives[0]
		}
		parts = append(parts, planSafeName(store.Name)+" "+word)
		switch word {
		case "heapStore":
			marks = append(marks, "-- cannot be written on this build: heapStore has no push yet; alternative: "+planHeapAlternative)
		case "blobStore":
			marks = append(marks, "-- cannot be written on this build: blobStore has no put yet; alternative: "+planBlobAlternative)
		}
	}
	for _, signal := range signals {
		if signal.Detector == "T6" && slices.Contains(kind.Sources, signal.Kind) && signal.Field != "" {
			marks = append(marks, fmt.Sprintf("-- not available yet: index %s.%s (list filters scan)", planSafeName(bucket), planSafeName(planTitle(signal.Field))))
		}
	}
	slices.Sort(marks)
	marks = slices.Compact(marks)
	return PlanStatement{Heading: heading, Text: fmt.Sprintf("create bucket %s { %s }", planSafeName(bucket), strings.Join(parts, ", ")), Marks: marks, Alternative: alternative}
}

// planKindsByName returns the model's kinds sorted by name, so output never
// depends on the order a producer wrote them in.
func planKindsByName(model *PlanModel) []ModelKind {
	sorted := slices.Clone(model.Kinds)
	slices.SortFunc(sorted, func(a, b ModelKind) int { return strings.Compare(a.Name, b.Name) })
	return sorted
}
