package cli

import (
	"fmt"
	"slices"
	"strings"
)

// PlanSignal is one observation that supports a pattern (spec section 2,
// "signal"). Detector names are S1..S8 for shape and T1..T7 for the trace.
type PlanSignal struct {
	Detector string `json:"detector"`
	Kind     string `json:"kind"`
	Field    string `json:"field,omitempty"`
	Pattern  string `json:"pattern,omitempty"`
	Other    string `json:"other,omitempty"`
	Lines    []int  `json:"lines,omitempty"`
	Note     string `json:"note"`
}

// detectShape runs S1..S8 and returns the signals sorted by kind, field and
// detector, so two runs print the same plan.
func detectShape(model *PlanModel, th PlanThresholds) []PlanSignal {
	var signals []PlanSignal
	for _, kind := range model.Kinds {
		signals = append(signals, detectIdentity(kind, th)...)
		signals = append(signals, detectGrowingList(kind, th)...)
		signals = append(signals, detectLargeBytes(kind, th)...)
		signals = append(signals, detectSparseAndNested(kind, th)...)
	}
	signals = append(signals, detectReferences(model, th)...)
	sortPlanSignals(signals)
	return signals
}

// sortPlanSignals orders signals by kind, field, detector and note, stably.
func sortPlanSignals(signals []PlanSignal) {
	slices.SortStableFunc(signals, func(a, b PlanSignal) int {
		if c := strings.Compare(a.Kind, b.Kind); c != 0 {
			return c
		}
		if c := strings.Compare(a.Field, b.Field); c != 0 {
			return c
		}
		if c := strings.Compare(a.Detector, b.Detector); c != 0 {
			return c
		}
		return strings.Compare(a.Note, b.Note)
	})
}

// detectIdentity is S1: _id in every document is the key, and another scalar
// that is distinct everywhere is an alternate key candidate.
func detectIdentity(kind ModelKind, th PlanThresholds) []PlanSignal {
	var signals []PlanSignal
	for _, field := range kind.Fields {
		if field.Name == "_id" {
			if field.PresentPermille == 1000 {
				signals = append(signals, PlanSignal{Detector: "S1", Kind: kind.Name, Field: "_id", Pattern: "P1",
					Note: fmt.Sprintf("%s has _id in every sampled document: the key", kind.Name)})
			}
			continue
		}
		if !planIsKeyType(field.Type) || field.Ref != nil {
			continue
		}
		if field.DistinctPermille == 1000 && field.PresentPermille >= th.UniquePresentPermille {
			signals = append(signals, PlanSignal{Detector: "S1", Kind: kind.Name, Field: field.Name, Pattern: "P8",
				Note: fmt.Sprintf("%s.%s is distinct in every sampled document (present in %d per mille): unique", kind.Name, field.Name, field.PresentPermille)})
		}
	}
	return signals
}

// planIsKeyType reports whether a field of this type can act as an alternate key.
func planIsKeyType(typeWord string) bool {
	return slices.Contains([]string{"string", "int", "int32", "uuid"}, typeWord)
}

// detectGrowingList is S2, S2a and S3: an array of documents that grows is an
// owned collection; its order and key fields say which store it wants.
func detectGrowingList(kind ModelKind, th PlanThresholds) []PlanSignal {
	var signals []PlanSignal
	for _, field := range kind.Fields {
		array := field.Array
		if array == nil || !array.ElemIsDocument {
			continue
		}
		if array.LenP95 < th.GrowingListP95 && array.LenMax < th.GrowingListAny {
			continue
		}
		signals = append(signals, PlanSignal{Detector: "S2", Kind: kind.Name, Field: field.Name, Pattern: "P2/P3",
			Note: fmt.Sprintf("%s.%s is a list of documents: p95 length %d, longest %d", kind.Name, field.Name, array.LenP95, array.LenMax)})
		if array.OrderField != "" && array.MonotonePermille >= th.OrderedPermille {
			signals = append(signals, PlanSignal{Detector: "S2a", Kind: kind.Name, Field: field.Name, Pattern: "P2/P3", Other: array.OrderField,
				Note: fmt.Sprintf("%s.%s elements are time-ordered by %s (%d per mille of adjacent pairs rise); whether you read the oldest or the newest first is not in the data", kind.Name, field.Name, array.OrderField, array.MonotonePermille)})
		}
		if array.KeyField != "" && array.KeyDistinctPermille >= th.DistinctWithinPermille {
			pattern := "P4"
			if array.KeyIsTime {
				pattern = "P5"
			}
			signals = append(signals, PlanSignal{Detector: "S3", Kind: kind.Name, Field: field.Name, Pattern: pattern, Other: array.KeyField,
				Note: fmt.Sprintf("%s.%s elements carry their own key %s, distinct within every list", kind.Name, field.Name, array.KeyField)})
		}
	}
	return signals
}

// detectLargeBytes is S4: bytes an instance owns. Between the noted and the
// large boundary it is a note only, never a suggestion.
func detectLargeBytes(kind ModelKind, th PlanThresholds) []PlanSignal {
	var signals []PlanSignal
	for _, field := range kind.Fields {
		if field.Type != "binary" && field.Type != "string" {
			continue
		}
		switch {
		case field.BytesP50 >= th.LargeBytes:
			signals = append(signals, PlanSignal{Detector: "S4", Kind: kind.Name, Field: field.Name, Pattern: "P7",
				Note: fmt.Sprintf("%s.%s is %d bytes at the median, at or above the %d-byte blob boundary", kind.Name, field.Name, field.BytesP50, th.LargeBytes)})
		case field.BytesP50 >= th.NotedBytes:
			signals = append(signals, PlanSignal{Detector: "S4", Kind: kind.Name, Field: field.Name,
				Note: fmt.Sprintf("%s.%s is %d bytes at the median: noted, kept as a scalar", kind.Name, field.Name, field.BytesP50)})
		}
	}
	return signals
}

// detectSparseAndNested is S7 and S8.
func detectSparseAndNested(kind ModelKind, th PlanThresholds) []PlanSignal {
	var signals []PlanSignal
	for _, field := range kind.Fields {
		if field.PresentPermille < th.SparsePermille {
			signals = append(signals, PlanSignal{Detector: "S7", Kind: kind.Name, Field: field.Name,
				Note: fmt.Sprintf("%s.%s is optional: present in %d per mille of documents", kind.Name, field.Name, field.PresentPermille)})
		}
		if field.Type == "object" {
			signals = append(signals, PlanSignal{Detector: "S8", Kind: kind.Name, Field: field.Name, Pattern: "P11",
				Note: fmt.Sprintf("%s.%s is a nested object, read with its parent", kind.Name, field.Name)})
		}
	}
	return signals
}

// detectReferences is S5 and S6. A reference that resolves is S5 (P9, or P4 for
// an array of ids). S6 marks a kind as a candidate owned collection of the kind
// it points at, but only when nothing else refers to it and the target is not
// itself shared: a thing two kinds refer to has an identity of its own, so it
// cannot own another kind's rows.
func detectReferences(model *PlanModel, th PlanThresholds) []PlanSignal {
	var signals []PlanSignal
	referrers := map[string]map[string]bool{}
	for _, kind := range model.Kinds {
		for _, field := range kind.Fields {
			if field.Ref == nil || field.Ref.ResolvePermille < th.ResolvePermille {
				continue
			}
			if referrers[field.Ref.To] == nil {
				referrers[field.Ref.To] = map[string]bool{}
			}
			referrers[field.Ref.To][kind.Name] = true
			pattern := "P9"
			if field.Array != nil || field.Type == "array" {
				pattern = "P4"
			}
			signals = append(signals, PlanSignal{Detector: "S5", Kind: kind.Name, Field: field.Name, Pattern: pattern, Other: field.Ref.To,
				Note: fmt.Sprintf("%s.%s refers to %s (%d per mille resolve)", kind.Name, field.Name, field.Ref.To, field.Ref.ResolvePermille)})
		}
	}
	for _, kind := range model.Kinds {
		if len(referrers[kind.Name]) > 0 {
			continue
		}
		var candidates []ModelField
		for _, field := range kind.Fields {
			if field.Ref == nil || field.Ref.ResolvePermille < th.ResolvePermille || field.Ref.To == kind.Name {
				continue
			}
			if len(referrers[field.Ref.To]) < 2 {
				candidates = append(candidates, field)
			}
		}
		for _, field := range candidates {
			note := fmt.Sprintf("%s refers only to %s through %s (%d per mille resolve) and nothing refers to %s: a candidate owned collection of %s",
				kind.Name, field.Ref.To, field.Name, field.Ref.ResolvePermille, kind.Name, field.Ref.To)
			if len(candidates) > 1 {
				note += fmt.Sprintf("; %d owner candidates, so the owner is ambiguous", len(candidates))
			}
			signals = append(signals, PlanSignal{Detector: "S6", Kind: kind.Name, Field: field.Name, Pattern: "P2/P3/P4/P5", Other: field.Ref.To, Note: note})
		}
	}
	return signals
}

// planOwner says which reference field makes a kind an owned collection, and
// of which kind.
type planOwner struct{ Field, To string }

// planReferrers counts, per kind, the other kinds with a resolving reference to it.
func planReferrers(shape []PlanSignal) map[string]map[string]bool {
	referrers := map[string]map[string]bool{}
	for _, signal := range shape {
		if signal.Detector != "S5" {
			continue
		}
		if referrers[signal.Other] == nil {
			referrers[signal.Other] = map[string]bool{}
		}
		referrers[signal.Other][signal.Kind] = true
	}
	return referrers
}

func planTraceWeight(line TraceLine) int {
	if line.Weight <= 0 {
		return 1
	}
	return line.Weight
}

// planOwners decides, per kind, the one reference field that owns it. A kind
// that another kind refers to has an identity of its own and is never owned.
// The S6 field wins when it is unambiguous; otherwise the reference field with
// the most traced weight does, ties broken by field name.
func planOwners(model *PlanModel, shape []PlanSignal) map[string]planOwner {
	referrers := planReferrers(shape)
	owners := map[string]planOwner{}
	for _, kind := range model.Kinds {
		if len(referrers[kind.Name]) > 0 {
			continue
		}
		refTarget := map[string]string{}
		for _, signal := range shape {
			if signal.Detector == "S5" && signal.Kind == kind.Name && signal.Other != kind.Name {
				refTarget[signal.Field] = signal.Other
			}
		}
		var s6 []string
		for _, signal := range shape {
			if signal.Detector == "S6" && signal.Kind == kind.Name {
				s6 = append(s6, signal.Field)
			}
		}
		if len(s6) == 1 {
			owners[kind.Name] = planOwner{Field: s6[0], To: refTarget[s6[0]]}
			continue
		}
		weight := map[string]int{}
		for _, line := range model.Trace {
			if line.Coll != kind.Name || !slices.Contains([]string{"get", "list", "range", "take"}, line.Op) {
				continue
			}
			var scoped []string
			for _, name := range line.By {
				if _, isRef := refTarget[name]; isRef {
					scoped = append(scoped, name)
				}
			}
			if len(scoped) == 1 {
				weight[scoped[0]] += planTraceWeight(line)
			}
		}
		best, bestWeight := "", 0
		for _, name := range planSortedNames(weight) {
			if len(s6) > 0 && !slices.Contains(s6, name) {
				continue
			}
			if weight[name] > bestWeight {
				best, bestWeight = name, weight[name]
			}
		}
		if best != "" {
			owners[kind.Name] = planOwner{Field: best, To: refTarget[best]}
		}
	}
	return owners
}

// detectTrace runs T1..T7 over the model's trace. Lines that name a kind the
// model lacks are skipped: the trace reader already refused them for files.
func detectTrace(model *PlanModel, shape []PlanSignal, th PlanThresholds) []PlanSignal {
	known := map[string]bool{}
	for _, kind := range model.Kinds {
		known[kind.Name] = true
	}
	owners := planOwners(model, shape)
	var signals []PlanSignal
	for _, line := range model.Trace {
		if !known[line.Coll] {
			continue
		}
		signals = append(signals, detectTraceLine(line, owners[line.Coll], th)...)
	}
	sortPlanSignals(signals)
	return signals
}

func detectTraceLine(line TraceLine, owner planOwner, th PlanThresholds) []PlanSignal {
	at := []int{line.Ordinal}
	base := PlanSignal{Kind: line.Coll, Lines: at}
	switch line.Op {
	case "get", "list", "range":
		return detectTraceRead(line, owner, base)
	case "take":
		base.Detector = "T5"
		if len(line.By) > 0 {
			base.Field = line.By[0]
		}
		if owner.To != "" && base.Field == owner.Field {
			base.Other = owner.To
		}
		switch {
		case line.Order == "newest":
			base.Pattern = "P3"
		case strings.HasPrefix(line.Order, "priority:"):
			base.Pattern = "P6"
		default:
			base.Pattern = "P2"
		}
		base.Note = fmt.Sprintf("trace line %d takes the %s element by %s; claim and ack is the lossless form", line.Ordinal, line.Order, strings.Join(line.By, ", "))
		return []PlanSignal{base}
	case "put":
		if line.BytesP50 == 0 {
			return nil
		}
		base.Detector = "T7"
		base.Note = fmt.Sprintf("trace line %d stores %d bytes at the median", line.Ordinal, line.BytesP50)
		if line.BytesP50 >= th.LargeBytes {
			base.Pattern = "P7"
		} else {
			base.Note += ", under the blob boundary: a scalar binary field"
		}
		return []PlanSignal{base}
	}
	return nil
}

// detectTraceRead classifies one get, list or range line: T1 key read, T2 to T4
// owner-scoped reads, T6 everything else, which is a read across owners.
func detectTraceRead(line TraceLine, owner planOwner, base PlanSignal) []PlanSignal {
	across := func(why string) []PlanSignal {
		base.Detector, base.Pattern, base.Field = "T6", "P8", strings.Join(line.By, ",")
		base.Note = fmt.Sprintf("trace line %d reads %s %s: a read across owners, so a bucket", line.Ordinal, line.Coll, why)
		return []PlanSignal{base}
	}
	if len(line.By) == 0 {
		if line.Op == "get" {
			return nil
		}
		return across("with no selector")
	}
	if slices.Contains(line.By, "_id") {
		base.Detector, base.Pattern, base.Field = "T1", "P1", "_id"
		base.Note = fmt.Sprintf("trace line %d reads %s by _id", line.Ordinal, line.Coll)
		return []PlanSignal{base}
	}
	if owner.Field == "" || !slices.Contains(line.By, owner.Field) {
		return across("by " + strings.Join(line.By, ", "))
	}
	var others []string
	for _, name := range line.By {
		if name != owner.Field {
			others = append(others, name)
		}
	}
	base.Other, base.Field = owner.To, owner.Field
	switch {
	case len(others) == 0 && line.Op == "range" && line.Key != "":
		base.Detector, base.Pattern = "T4", "P5"
		base.Note = fmt.Sprintf("trace line %d reads a range of %s by %s within one document of %s", line.Ordinal, line.Key, owner.Field, owner.To)
	case len(others) == 0:
		base.Detector, base.Pattern = "T2", "P2/P3"
		base.Note = fmt.Sprintf("trace line %d lists %s within one document of %s, order not stated", line.Ordinal, line.Coll, owner.To)
		if len(line.Sort) > 0 {
			if strings.HasSuffix(line.Sort[0], ":desc") {
				base.Pattern = "P3"
			} else {
				base.Pattern = "P2"
			}
			base.Note = fmt.Sprintf("trace line %d lists %s within one document of %s sorted %s", line.Ordinal, line.Coll, owner.To, line.Sort[0])
		}
	case len(others) == 1 && len(line.Sort) == 0:
		base.Detector, base.Pattern, base.Other = "T3", "P4", owner.To
		base.Note = fmt.Sprintf("trace line %d reads %s by %s and %s with no sort: keyed within one document of %s", line.Ordinal, line.Coll, owner.Field, others[0], owner.To)
		base.Field = others[0]
	default:
		return across("by " + strings.Join(line.By, ", "))
	}
	return []PlanSignal{base}
}
