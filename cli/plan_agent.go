package cli

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
)

// planCriticalRule is the placement rule of the store types page, copied once.
const planCriticalRule = "If you will look a thing up, list it or index it on its own, across its owners, it is a bucket. " +
	"If it only ever exists as part of one document, it is a store field of that document."

// planPatterns is the pattern catalogue of spec section 5: the structure each
// pattern maps to. The guide prints it, so it cannot drift from what the
// detectors name.
var planPatterns = []struct{ ID, Structure string }{
	{"P1", "look an instance up by its own key: a bucket, _id_ is the key"},
	{"P2", "append to an owner's list, read oldest first or consume in order: fifoStore on the owner"},
	{"P3", "append to an owner's list, read newest first: lifoStore on the owner"},
	{"P4", "an owner's things looked up by a second key: hashStore on the owner"},
	{"P5", "an owner's things read in key order or by range: btreeStore on the owner"},
	{"P6", "an owner's things taken most important first: heapStore on the owner (no push yet)"},
	{"P7", "bytes an owner keeps: blobStore on the owner, or the pointer layout (no put yet)"},
	{"P8", "find instances by a field that is not the key, across owners: a bucket with the field declared and an index (a scan: indexes are not available yet)"},
	{"P9", "one kind refers to another: a declared scalar field holding the other kind's _id_"},
	{"P10", "a value read with its instance and never alone: a scalar field"},
	{"P11", "a nested object read whole with its instance: an object field"},
}

// renderGuide returns the rules as Markdown for a system prompt (spec section
// 18). It is built only from the tables the detectors and reports use.
func renderGuide() string {
	th := DefaultPlanThresholds()
	var out strings.Builder
	line := func(format string, args ...any) { fmt.Fprintf(&out, format+"\n", args...) }
	line("# hoarddb plan: rules for an agent\n")
	line("You read the application and write a model file. `hoarddb plan` judges it. The verdict changes only when the design does: there is no model inside the tool.\n")
	line("## The Critical Rule\n")
	line("%s\n", planCriticalRule)
	line("## Patterns and their structure\n")
	for _, pattern := range planPatterns {
		line("- %s: %s", pattern.ID, pattern.Structure)
	}
	line("\n## Query reductions\n")
	for _, kind := range planSortedNames(planReductionShapes) {
		line("- join kind `%s`: %s", kind, planReductionShapes[kind])
	}
	line("- an N+1 loop (a list, then a get per id) is a primary-key join: `expand`, a keyed batch")
	line("- child rows by parent key, ordered: an owned ordered store on the parent, one seek")
	line("- cost classes, cheapest first: %s", strings.Join(planCostClasses, ", "))
	line("\n## Visibility and locality\n")
	line("Start at the principal: what may this principal see, then owned reads, then `expand`. Set `visibility` on the trace line:\n")
	for _, shape := range planVisibilityShapes {
		line("- `%s`: %s", shape.Word, shape.Reduction)
	}
	line("\nFor `container` and `object_global` the number that decides the fit is containers or grants per principal at p95 (put it in `fans`): up to %d is seeks, up to %d is a merge, above that a scan.", th.ContainersFits, th.ContainersMerge)
	line("Small, read-everywhere kinds on the far side of primary-key joins belong in a reference database at full replication with local reads.")
	line("\n## The five disqualifiers\n")
	line("A rule fails (`F`) when the pattern is on a primary read or write, and is partial (`p`) on the admin surface only. Put `surface: admin` on secondary lines.\n")
	line("1. %s: a list with %d or more optional filters composed at run time. Text search is a pass only when a `search` line delegates it to a search engine; a `substring` match with no `search` line for the kind fails.", planRuleNames[0], th.OptionalFilterFail)
	line("2. %s: `sort_from: client` on a list. `setting` is partial.", planRuleNames[1])
	line("3. %s: a join on a non-key column, to sort by the far side, or to aggregate over a dimension chosen at run time.", planRuleNames[2])
	line("4. %s: `object_global`, a `container` predicate above the merge bar, or a visibility join no rule above reduces. A join of kind `visibility` with no `visibility` word on it always counts: put the word on the join line.", planRuleNames[3])
	line("5. %s: lines sharing `atomic_with` across two kinds with no `guard`.", planRuleNames[4])
	line("\nVerdicts: `yes`, `yes with caveats`, `only with redesign of <rules>`, `not a good candidate for hoardDB`. The last needs, on one primary list, %d or more optional filters AND a client-chosen sort AND either a `container` or `object_global` visibility predicate or a `dynamic` line. An `owner` predicate does not qualify. Say it as plainly as a yes.", th.AdhocOptionalFilters)
	line("\n## The loop\n")
	line("1. Run `hoarddb plan schema` and write a model file that validates against it.")
	line("2. Run `hoarddb plan model.json --json`.")
	line("3. Revise the DESIGN the model describes, or accept the fail. Do not edit trace lines, `surface`, counts or fans to move a cell: that changes the claim, not the application. A refusal carries a `fix` string you can act on.")
	line("4. Put the open choices to the human, with the cost lines of the plan beside each option.")
	line("\nYou do not pick the structure either: the developer does.")
	return out.String()
}

// schemaOf returns the JSON Schema (draft 2020-12 vocabulary) of a type. Each
// `doc` tag is the property's description; fields tagged omitempty are not
// required; objects refuse unknown keys, as the strict loader does.
func schemaOf(typ reflect.Type) map[string]any {
	switch typ.Kind() {
	case reflect.Pointer:
		return schemaOf(typ.Elem())
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return map[string]any{"type": "integer"}
	case reflect.Slice:
		return map[string]any{"type": "array", "items": schemaOf(typ.Elem())}
	case reflect.Map:
		return map[string]any{"type": "object", "additionalProperties": schemaOf(typ.Elem())}
	case reflect.Struct:
		properties := map[string]any{}
		var required []string
		for index := 0; index < typ.NumField(); index++ {
			field := typ.Field(index)
			name, options, _ := strings.Cut(field.Tag.Get("json"), ",")
			if name == "-" || name == "" {
				continue
			}
			property := schemaOf(field.Type)
			if doc := field.Tag.Get("doc"); doc != "" {
				property["description"] = doc
			}
			properties[name] = property
			if !strings.Contains(options, "omitempty") {
				required = append(required, name)
			}
		}
		schema := map[string]any{"type": "object", "properties": properties, "additionalProperties": false}
		if len(required) > 0 {
			schema["required"] = required
		}
		return schema
	}
	return map[string]any{}
}

// planSchemaJSON is the model file's schema, indented two spaces, keys sorted.
func planSchemaJSON() []byte {
	schema := schemaOf(reflect.TypeOf(PlanModel{}))
	schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
	schema["title"] = "hoarddb plan model file"
	encoded, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		// Invariant: the schema holds only strings, booleans and maps, which always encode.
		panic("plan schema is built from basic values and always encodes: " + err.Error())
	}
	return append(encoded, '\n')
}

// planAgentWords are the subcommands that print rules for an agent.
var planAgentWords = []string{"guide", "schema"}

func planIsAgentWord(word string) bool { return slices.Contains(planAgentWords, word) }
