package cli

import (
	"fmt"
	"slices"
	"strings"
)

// planVisibilityShapes is the predicate table of spec section 16.2: the word a
// trace line carries, the reduction it earns, and whether the number of
// containers per principal decides the fit.
var planVisibilityShapes = []struct {
	Word, Reduction string
	DecidedByFan    bool
}{
	{"owner", "a field, filtered after read: always fits", false},
	{"membership", "two-sided store with expand: always fits", false},
	{"container", "grants at the container, two-sided, owned ordered lists per container and a merge for the first page", true},
	{"object_scoped", "a Permissions hashStore on the object, checked per child, inheritance by walking the parent pointer: always fits", false},
	{"object_global", "Principal.Readable as a two-sided store, or a scan", true},
}

// planFindKind returns the named kind of a model.
func planFindKind(model *PlanModel, name string) (ModelKind, bool) {
	for _, kind := range model.Kinds {
		if kind.Name == name {
			return kind, true
		}
	}
	return ModelKind{}, false
}

func ruleSignal(suffix, kind, field, note string, lines ...int) PlanSignal {
	return PlanSignal{Detector: "R-" + suffix, Kind: kind, Field: field, Lines: lines, Note: note}
}

// detectTenantRoot finds one field that nearly every traced selector names.
func detectTenantRoot(model *PlanModel, th PlanThresholds) []PlanSignal {
	counts := map[string]int{}
	lines := 0
	for _, line := range model.Trace {
		names := map[string]bool{}
		for _, name := range line.By {
			names[name] = true
		}
		for _, filter := range line.Filters {
			names[filter.Field] = true
		}
		delete(names, "_id")
		if len(names) == 0 {
			continue
		}
		lines++
		for name := range names {
			counts[name]++
		}
	}
	if lines < th.TenantMinLines {
		return nil
	}
	var signals []PlanSignal
	for _, name := range planSortedNames(counts) {
		if share := counts[name] * 1000 / lines; share >= th.TenantPermille {
			signals = append(signals, ruleSignal("tenant-root", "", name,
				fmt.Sprintf("%s appears in %d of %d traced selectors: a tenant root, so a bucket per tenant kind", name, counts[name], lines)))
		}
	}
	return signals
}

// detectLinkTable finds a link table that the trace reads from both sides.
func detectLinkTable(model *PlanModel, th PlanThresholds) []PlanSignal {
	type link struct{ kind, a, b string }
	var links []link
	for _, declared := range model.Links {
		links = append(links, link{declared.Kind, declared.A, declared.B})
	}
	for _, kind := range model.Kinds {
		var refs []string
		other := 0
		for _, field := range kind.Fields {
			switch {
			case field.Name == "_id":
			case field.Ref != nil:
				refs = append(refs, field.Ref.To)
			default:
				other++
			}
		}
		if len(refs) == 2 && other == 0 && !slices.ContainsFunc(links, func(l link) bool { return l.kind == kind.Name }) {
			links = append(links, link{kind.Name, refs[0], refs[1]})
		}
	}
	var signals []PlanSignal
	for _, candidate := range links {
		kind, ok := planFindKind(model, candidate.kind)
		if !ok {
			continue
		}
		targetOf := map[string]string{}
		for _, field := range kind.Fields {
			if field.Ref != nil {
				targetOf[field.Name] = field.Ref.To
			}
		}
		sides := map[string]bool{}
		var ordinals []int
		for _, line := range model.Trace {
			if line.Coll != candidate.kind || (line.Op != "list" && line.Op != "get") {
				continue
			}
			for _, name := range line.By {
				if name == "_id" {
					continue
				}
				if target, ok := targetOf[name]; ok && (target == candidate.a || target == candidate.b) {
					sides[target] = true
					ordinals = append(ordinals, line.Ordinal)
				} else if len(targetOf) == 0 {
					sides[name] = true
					ordinals = append(ordinals, line.Ordinal)
				}
			}
		}
		if len(sides) >= 2 {
			signals = append(signals, ruleSignal("link-table", candidate.kind, "",
				fmt.Sprintf("%s links %s and %s and is read from both sides: a link table", candidate.kind, candidate.a, candidate.b), ordinals...))
			signals[len(signals)-1].Other = candidate.a + "," + candidate.b
		}
	}
	return signals
}

// detectMembershipArray finds an array of references that the trace reads in reverse.
func detectMembershipArray(model *PlanModel, th PlanThresholds) []PlanSignal {
	var signals []PlanSignal
	for _, kind := range model.Kinds {
		for _, field := range kind.Fields {
			if field.Array == nil || field.Ref == nil {
				continue
			}
			var ordinals []int
			for _, line := range model.Trace {
				if line.Coll == kind.Name && (line.Op == "list" || line.Op == "get") && slices.Contains(line.By, field.Name) {
					ordinals = append(ordinals, line.Ordinal)
				}
			}
			if len(ordinals) > 0 {
				signals = append(signals, ruleSignal("membership-array", kind.Name, field.Name,
					fmt.Sprintf("%s.%s is a membership array of %s ids read in reverse (find the %s that contain one): a two-sided store", kind.Name, field.Name, field.Ref.To, kind.Name), ordinals...))
			}
		}
	}
	return signals
}

// detectPermissionPredicate maps a line's visibility shape to its reduction and,
// where the number of containers per principal decides the fit, says which.
func detectPermissionPredicate(model *PlanModel, th PlanThresholds) []PlanSignal {
	var signals []PlanSignal
	for _, line := range model.Trace {
		if line.Visibility == "" {
			continue
		}
		index := slices.IndexFunc(planVisibilityShapes, func(s struct {
			Word, Reduction string
			DecidedByFan    bool
		}) bool {
			return s.Word == line.Visibility
		})
		if index < 0 {
			continue
		}
		shape := planVisibilityShapes[index]
		note := fmt.Sprintf("trace line %d: %s predicate: %s", line.Ordinal, shape.Word, shape.Reduction)
		if shape.DecidedByFan {
			note += "; " + planFanVerdict(model, line.Coll, th)
		}
		signals = append(signals, ruleSignal("permission", line.Coll, line.Visibility, note, line.Ordinal))
	}
	return signals
}

// planFanVerdict reads the containers-per-principal p95 for a kind and says what it buys.
func planFanVerdict(model *PlanModel, child string, th PlanThresholds) string {
	best, found := 0, false
	for _, fan := range model.Fans {
		if fan.Child == child && (!found || fan.P95 > best) {
			best, found = fan.P95, true
		}
	}
	if !found {
		return "containers per principal p95 unknown: add a fans entry"
	}
	outcome := "a scan"
	switch {
	case best <= th.ContainersFits:
		outcome = "seeks"
	case best <= th.ContainersMerge:
		outcome = "a merge"
	}
	return fmt.Sprintf("containers per principal p95 = %d: %s", best, outcome)
}

// detectOptionalFilters finds lists that compose many filters at run time.
func detectOptionalFilters(model *PlanModel, th PlanThresholds) []PlanSignal {
	var signals []PlanSignal
	for _, line := range model.Trace {
		count := 0
		for _, filter := range line.Filters {
			if filter.Optional {
				count++
			}
		}
		if line.Op == "list" && count >= th.OptionalFilterFail {
			signals = append(signals, ruleSignal("optional-filters", line.Coll, "", fmt.Sprintf("trace line %d composes %d optional filters at run time", line.Ordinal, count), line.Ordinal))
		}
	}
	return signals
}

// detectClientSort finds lists whose sort field the client chooses.
func detectClientSort(model *PlanModel, th PlanThresholds) []PlanSignal {
	distinct := map[string]map[string]bool{}
	for _, line := range model.Trace {
		if line.SortFrom == "client" {
			if distinct[line.Coll] == nil {
				distinct[line.Coll] = map[string]bool{}
			}
			for _, sort := range line.Sort {
				distinct[line.Coll][sort] = true
			}
		}
	}
	var signals []PlanSignal
	for _, line := range model.Trace {
		if line.SortFrom == "client" {
			signals = append(signals, ruleSignal("client-sort", line.Coll, "", fmt.Sprintf("trace line %d sorts by a field the client chooses; %d distinct sort orders are traced for %s", line.Ordinal, len(distinct[line.Coll]), line.Coll), line.Ordinal))
		}
	}
	return signals
}

// detectHandCounters finds counters the application keeps by hand.
func detectHandCounters(model *PlanModel, th PlanThresholds) []PlanSignal {
	var signals []PlanSignal
	for _, kind := range model.Kinds {
		for _, field := range kind.Fields {
			if field.Role == "counter" {
				signals = append(signals, ruleSignal("hand-counters", kind.Name, field.Name, fmt.Sprintf("%s.%s is a counter kept by hand: a store's count is maintained for you", kind.Name, field.Name)))
			}
		}
	}
	for _, line := range model.Trace {
		if len(line.Inc) > 0 {
			signals = append(signals, ruleSignal("hand-counters", line.Coll, strings.Join(line.Inc, ","), fmt.Sprintf("trace line %d increments %s by hand", line.Ordinal, strings.Join(line.Inc, ", ")), line.Ordinal))
		}
	}
	return signals
}

// detectPeriodDocuments finds pre-aggregated period documents.
func detectPeriodDocuments(model *PlanModel, th PlanThresholds) []PlanSignal {
	var signals []PlanSignal
	for _, kind := range model.Kinds {
		for _, field := range kind.Fields {
			if field.Role == "period" {
				signals = append(signals, ruleSignal("period", kind.Name, field.Name, fmt.Sprintf("%s.%s names a period: a btreeStore keyed by period, with an increment", kind.Name, field.Name)))
			}
		}
	}
	return signals
}

// detectWholeTTL finds kinds whose whole collection expires.
func detectWholeTTL(model *PlanModel, th PlanThresholds) []PlanSignal {
	var signals []PlanSignal
	for _, kind := range model.Kinds {
		if kind.ExpireWhole {
			signals = append(signals, ruleSignal("whole-ttl", kind.Name, "", kind.Name+" expires as a whole: a retention setting, not a field"))
		}
	}
	return signals
}

// detectTransactions finds writes the source performs in one transaction.
func detectTransactions(model *PlanModel, th PlanThresholds) []PlanSignal {
	groups := map[string][]TraceLine{}
	for _, line := range model.Trace {
		if line.AtomicWith != "" {
			groups[line.AtomicWith] = append(groups[line.AtomicWith], line)
		}
	}
	var signals []PlanSignal
	for _, id := range planSortedNames(groups) {
		kinds := map[string]bool{}
		var ordinals []int
		for _, line := range groups[id] {
			kinds[line.Coll] = true
			ordinals = append(ordinals, line.Ordinal)
		}
		touched := planSortedNames(kinds)
		signals = append(signals, ruleSignal("transactions", touched[0], id,
			fmt.Sprintf("atomic group %s spans %d kinds: %s", id, len(touched), strings.Join(touched, ", ")), ordinals...))
		signals[len(signals)-1].Other = strings.Join(touched, ",")
	}
	return signals
}

// detectExternalSearch finds search that a search engine serves; it is a pass.
// Only an explicit search line says the search is delegated. A substring match
// is covered by a search line for the same kind; when both lines name fields
// (by or filters), they must share one, so a search on one field does not cover
// a substring match on another.
func detectExternalSearch(model *PlanModel, th PlanThresholds) []PlanSignal {
	var signals []PlanSignal
	for _, line := range model.Trace {
		switch {
		case line.Op == "search":
			signals = append(signals, ruleSignal("external-search", line.Coll, "", fmt.Sprintf("trace line %d searches text: use a search engine for it", line.Ordinal), line.Ordinal))
		case line.Substring && planSearchCovers(model, line):
			signals = append(signals, ruleSignal("external-search", line.Coll, "", fmt.Sprintf("trace line %d matches a substring and the trace delegates search for it: use a search engine", line.Ordinal), line.Ordinal))
		}
	}
	return signals
}

// planTraceFields lists the fields a line names, in by and filters.
func planTraceFields(line TraceLine) []string {
	fields := slices.Clone(line.By)
	for _, filter := range line.Filters {
		fields = append(fields, filter.Field)
	}
	return fields
}

// planSearchCovers reports whether a search line for the same kind covers a
// substring line: by kind when either names no field, by a shared field otherwise.
func planSearchCovers(model *PlanModel, substring TraceLine) bool {
	wanted := planTraceFields(substring)
	for _, line := range model.Trace {
		if line.Op != "search" || line.Coll != substring.Coll {
			continue
		}
		named := planTraceFields(line)
		if len(wanted) == 0 || len(named) == 0 {
			return true
		}
		for _, field := range wanted {
			if slices.Contains(named, field) {
				return true
			}
		}
	}
	return false
}

// detectAll runs the shape, trace and rev 2 detectors and sorts the result.
func detectAll(model *PlanModel, th PlanThresholds) []PlanSignal {
	shape := detectShape(model, th)
	signals := append([]PlanSignal(nil), shape...)
	signals = append(signals, detectTrace(model, shape, th)...)
	for _, detect := range []func(*PlanModel, PlanThresholds) []PlanSignal{
		detectTenantRoot, detectLinkTable, detectMembershipArray, detectPermissionPredicate, detectOptionalFilters,
		detectClientSort, detectHandCounters, detectPeriodDocuments, detectWholeTTL, detectTransactions, detectExternalSearch,
	} {
		signals = append(signals, detect(model, th)...)
	}
	sortPlanSignals(signals)
	return signals
}
