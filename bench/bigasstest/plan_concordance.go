package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
	"github.com/hoarddb/hoarddb-server/cli"
)

// The advisor concordance (coverage item C15). `hoarddb plan` is the product's own answer to "which
// store type fits how I read this"; the benchmark seeds store types a developer picked. This file
// describes each layout to the advisor the way a relational source would look (one kind per
// document bucket, one child kind per store field that holds a foreign key to its owner, one trace
// line per way the mix reads it) and compares the store word the advisor proposes for each store
// field with the word the benchmark seeds. It connects to nothing and runs no request: the output is
// an agree, differ or open row per store, to be read and reported, never a throughput.

// The outcomes of one row.
const (
	outcomeAgree        = "agree"
	outcomeDiffer       = "differ"
	outcomeOpen         = "open"
	outcomeNotExercised = "not-exercised"
)

// concordanceRow is one store field of the benchmark's schema against the advisor's proposal.
type concordanceRow struct {
	Store     string // "Bucket.Store"
	Benchmark string // the store word the benchmark seeds
	Advisor   string // the store word the advisor proposes; empty when it leaves the choice open or the store has no kind
	Outcome   string
	Note      string
}

// planInputs are the two files `hoarddb plan <model.json> --trace <trace.jsonl>` reads.
type planInputs struct {
	Model []byte
	Trace []byte
}

// keyedStores are the hash stores a request reads or writes by key, which the advisor reads as a
// keyed collection (T3). Every other hash store is read as a whole list.
var keyedStores = map[string]bool{
	"Post.Likes": true, "User.Likes": true, "User.Settings": true, "User.Friends": true, "Run.Scratch": true,
}

// documentRefs are the scalar fields of a document bucket that hold the id of another bucket.
var documentRefs = map[string]string{
	"Post.Author": "User", "Group.Owner": "User", "Follow.From": "User", "Follow.To": "User",
	"Member.Room": "Room", "Member.User": "User", "Session.Owner": "User", "Run.Session": "Session",
}

// untouchedStores are the fifo, hash and btree stores the layouts declare and seed but no request
// of the mix reads or writes (checked against the mix and every adapter), so they are not exercised
// whatever their type.
var untouchedStores = map[string]bool{"User.Friends": true}

// exercisedStore reports whether some request of the mix reads or writes the store. The lifo, heap
// and blob stores are declared by the layouts and never touched, and so are the stores in
// untouchedStores; no source describes a read of them. The hash rows that agree rest on the hand
// tables keyedStores and documentRefs, not on the schema alone.
func exercisedStore(name, storeType string) bool {
	if untouchedStores[name] {
		return false
	}
	return storeType == "fifoStore" || storeType == "hashStore" || storeType == "btreeStore"
}

type planKindJSON struct {
	Name      string          `json:"name"`
	Documents int64           `json:"documents"`
	Sampled   int64           `json:"sampled"`
	Fields    []planFieldJSON `json:"fields"`
}

type planFieldJSON struct {
	Name            string       `json:"name"`
	Type            string       `json:"type"`
	PresentPermille int          `json:"present_permille"`
	Distinct        int          `json:"distinct_permille,omitempty"`
	Ref             *planRefJSON `json:"ref,omitempty"`
}

type planRefJSON struct {
	To      string `json:"to"`
	Resolve int    `json:"resolve_permille"`
}

type planTraceJSON struct {
	Op     string   `json:"op"`
	Coll   string   `json:"coll"`
	By     []string `json:"by,omitempty"`
	Sort   []string `json:"sort,omitempty"`
	Key    string   `json:"key,omitempty"`
	Limit  int      `json:"limit,omitempty"`
	Weight int      `json:"weight,omitempty"`
}

// planTypeOf maps a model value to the advisor's type word.
func planTypeOf(value any) string {
	switch value.(type) {
	case int, int64:
		return "int"
	case []byte:
		return "binary"
	}
	return "string"
}

func idField() planFieldJSON {
	return planFieldJSON{Name: "_id", Type: "string", PresentPermille: 1000, Distinct: 1000}
}

// firstElement returns the first element of a store, for the field names and types of its body.
func firstElement(gen *model.Generator, store string) (model.Element, bool) {
	for element := range gen.Elements(store) {
		return element, true
	}
	return model.Element{}, false
}

// firstDocument returns the first document of a bucket.
func firstDocument(gen *model.Generator, bucket string) (model.Document, bool) {
	for doc := range gen.Documents(bucket) {
		return doc, true
	}
	return model.Document{}, false
}

// buildPlanInputs describes one layout to the advisor: a kind per document bucket (read by id, as
// every bucket is), and for every exercised store field a child kind that refers to its owner with
// the trace line of the way the mix reads it (a page oldest first for a fifo or btree store, a
// keyed read for the keyed hash stores, a list for the others).
func buildPlanInputs(gen *model.Generator) (planInputs, error) {
	counts := gen.Counts()
	var kinds []planKindJSON
	var trace []planTraceJSON
	for _, bucket := range gen.Schema() {
		owner := strings.ToLower(bucket.Name)
		doc := planKindJSON{Name: owner, Documents: counts[bucket.Name], Sampled: counts[bucket.Name], Fields: []planFieldJSON{idField()}}
		sample, haveSample := firstDocument(gen, bucket.Name)
		for _, field := range bucket.Fields {
			if field.IsStore() {
				continue
			}
			column := planFieldJSON{Name: strings.ToLower(field.Name), Type: field.Type, PresentPermille: 1000}
			if haveSample {
				for _, value := range sample.Fields {
					if value.Name == field.Name {
						column.Type = planTypeOf(value.Value)
					}
				}
			}
			if target, ok := documentRefs[bucket.Name+"."+field.Name]; ok {
				column.Ref = &planRefJSON{To: strings.ToLower(target), Resolve: 1000}
			}
			doc.Fields = append(doc.Fields, column)
		}
		kinds = append(kinds, doc)
		trace = append(trace, planTraceJSON{Op: "get", Coll: owner, By: []string{"_id"}})
		for _, field := range bucket.Fields {
			if !field.IsStore() || !exercisedStore(bucket.Name+"."+field.Name, field.Type) {
				continue
			}
			name := bucket.Name + "." + field.Name
			child := planKindJSON{Name: owner + "_" + strings.ToLower(field.Name), Documents: counts[name], Sampled: counts[name], Fields: []planFieldJSON{idField(),
				{Name: owner + "_id", Type: "string", PresentPermille: 1000, Ref: &planRefJSON{To: owner, Resolve: 1000}}}}
			ownerField := owner + "_id"
			switch field.Type {
			case "fifoStore":
				child.Fields = append(child.Fields, planFieldJSON{Name: "ts", Type: "datetime", PresentPermille: 1000})
				trace = append(trace, planTraceJSON{Op: "list", Coll: child.Name, By: []string{ownerField}, Sort: []string{"ts:asc"}, Limit: 50})
			case "btreeStore":
				child.Fields = append(child.Fields, planFieldJSON{Name: "ts", Type: "datetime", PresentPermille: 1000})
				trace = append(trace, planTraceJSON{Op: "list", Coll: child.Name, By: []string{ownerField}, Sort: []string{"ts:asc"}, Limit: 50})
			default:
				child.Fields = append(child.Fields, planFieldJSON{Name: "key", Type: "string", PresentPermille: 1000})
				if keyedStores[name] {
					trace = append(trace, planTraceJSON{Op: "get", Coll: child.Name, By: []string{ownerField, "key"}})
				} else {
					trace = append(trace, planTraceJSON{Op: "list", Coll: child.Name, By: []string{ownerField}, Limit: 100})
				}
			}
			if element, ok := firstElement(gen, name); ok {
				for _, value := range element.Body {
					child.Fields = append(child.Fields, planFieldJSON{Name: strings.ToLower(value.Name), Type: planTypeOf(value.Value), PresentPermille: 1000})
				}
			}
			kinds = append(kinds, child)
		}
	}
	modelJSON, err := json.MarshalIndent(map[string]any{"version": 1, "source": "hand", "app": string(gen.Layout()), "kinds": kinds}, "", " ")
	if err != nil {
		return planInputs{}, fmt.Errorf("encoding the plan model of %s: %w", gen.Layout(), err)
	}
	var traceJSON bytes.Buffer
	for _, line := range trace {
		encoded, err := json.Marshal(line)
		if err != nil {
			return planInputs{}, fmt.Errorf("encoding a trace line of %s: %w", gen.Layout(), err)
		}
		traceJSON.Write(encoded)
		traceJSON.WriteByte('\n')
	}
	return planInputs{Model: modelJSON, Trace: traceJSON.Bytes()}, nil
}

// planOutputMu serialises runPlanJSON: cli.RunPlanCommand writes to the process's stdout.
var planOutputMu sync.Mutex

// runPlanJSON runs `hoarddb plan <model> --trace <trace> --json --no-input` in this process and
// returns the suggestions. The command connects to nothing; the model and trace are written to a
// scratch directory first.
func runPlanJSON(inputs planInputs) ([]cli.PlanKind, error) {
	dir, err := os.MkdirTemp("", "bigasstest-plan-*")
	if err != nil {
		return nil, fmt.Errorf("making a scratch directory for the plan inputs: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }() // a scratch directory of two small files
	modelPath, tracePath := filepath.Join(dir, "model.json"), filepath.Join(dir, "trace.jsonl")
	if err := os.WriteFile(modelPath, inputs.Model, 0o600); err != nil {
		return nil, fmt.Errorf("writing the plan model: %w", err)
	}
	if err := os.WriteFile(tracePath, inputs.Trace, 0o600); err != nil {
		return nil, fmt.Errorf("writing the plan trace: %w", err)
	}
	planOutputMu.Lock()
	defer planOutputMu.Unlock()
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("opening a pipe for the plan output: %w", err)
	}
	original := os.Stdout
	os.Stdout = writer
	collected := make(chan []byte, 1)
	go func() {
		out, _ := io.ReadAll(reader) // a pipe we own; its error ends the read and the empty output is refused below
		collected <- out
	}()
	exit := cli.RunPlanCommand([]string{modelPath, "--trace", tracePath, "--json", "--no-input"})
	os.Stdout = original
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("closing the plan output pipe: %w", err)
	}
	out := <-collected
	if err := reader.Close(); err != nil {
		return nil, fmt.Errorf("closing the plan output reader: %w", err)
	}
	if exit != cli.ExitOK {
		return nil, fmt.Errorf("hoarddb plan refused the benchmark's own model (exit %d): %s", exit, strings.TrimSpace(string(out)))
	}
	var plan struct {
		Kinds []cli.PlanKind `json:"suggestions"`
	}
	if err := json.Unmarshal(out, &plan); err != nil {
		return nil, fmt.Errorf("reading the plan JSON: %w", err)
	}
	return plan.Kinds, nil
}

// planConcordance runs the advisor on one layout and returns one row per store field of the
// layout's schema, sorted by store name.
func planConcordance(gen *model.Generator) ([]concordanceRow, error) {
	inputs, err := buildPlanInputs(gen)
	if err != nil {
		return nil, err
	}
	kinds, err := runPlanJSON(inputs)
	if err != nil {
		return nil, fmt.Errorf("layout %s: %w", gen.Layout(), err)
	}
	proposed := map[string]cli.PlanStoreField{} // by the source kind the field was folded from
	for _, kind := range kinds {
		for _, field := range kind.StoreFields {
			proposed[field.Source] = field
		}
	}
	var rows []concordanceRow
	for _, bucket := range gen.Schema() {
		for _, field := range bucket.Fields {
			if !field.IsStore() {
				continue
			}
			row := concordanceRow{Store: bucket.Name + "." + field.Name, Benchmark: field.Type}
			source := strings.ToLower(bucket.Name) + "_" + strings.ToLower(field.Name)
			advice, found := proposed[source]
			switch {
			case !exercisedStore(row.Store, field.Type):
				row.Outcome, row.Note = outcomeNotExercised, "no request of the mix reads or writes this store"
			case !found:
				row.Outcome, row.Note = outcomeDiffer, "the advisor made "+source+" a bucket of its own, not a store field"
			case advice.Store == "":
				row.Outcome, row.Note = outcomeOpen, "the advisor left the store word open: "+strings.Join(advice.Alternatives, ", ")
			case advice.Store == field.Type:
				row.Advisor, row.Outcome = advice.Store, outcomeAgree
			default:
				row.Advisor, row.Outcome, row.Note = advice.Store, outcomeDiffer, "the trace line decides it: "+traceWord(field.Type)
			}
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Store < rows[j].Store })
	return rows, nil
}

// traceWord says which trace line the benchmark's access was described with, for a differ note.
func traceWord(storeType string) string {
	switch storeType {
	case "btreeStore":
		return "a page in key order is a list sorted ascending to the advisor, which a fifoStore serves; only a range read asks for a btreeStore"
	case "hashStore":
		return "a whole-list read of a hashStore has no order to the advisor"
	}
	return "see the trace"
}

// formatConcordance prints the rows as the agree/differ list the coverage note asks for.
func formatConcordance(rows []concordanceRow) string {
	var out strings.Builder
	counts := map[string]int{}
	for _, row := range rows {
		counts[row.Outcome]++
		line := fmt.Sprintf("%-14s %-22s benchmark %s", row.Outcome, row.Store, row.Benchmark)
		if row.Advisor != "" {
			line += ", advisor " + row.Advisor
		}
		if row.Note != "" {
			line += " (" + row.Note + ")"
		}
		out.WriteString(line + "\n")
	}
	fmt.Fprintf(&out, "%d agree, %d differ, %d open, %d not exercised\n", counts[outcomeAgree], counts[outcomeDiffer], counts[outcomeOpen], counts[outcomeNotExercised])
	return out.String()
}

// planCheckMain prints the concordance of every benchmark layout.
func planCheckMain(out io.Writer, seed uint64) error {
	var all []concordanceRow
	for _, layout := range benchLayouts {
		gen, err := model.New(layout, seed, 1)
		if err != nil {
			return fmt.Errorf("plan-check: %w", err)
		}
		rows, err := planConcordance(gen)
		if err != nil {
			return fmt.Errorf("plan-check: %w", err)
		}
		if _, err := fmt.Fprintf(out, "layout %s\n%s\n", layout, formatConcordance(rows)); err != nil {
			return fmt.Errorf("plan-check: writing the report: %w", err)
		}
		all = append(all, rows...)
	}
	if len(all) == 0 {
		return errors.New("plan-check: no store field to compare")
	}
	_, err := fmt.Fprintf(out, "all layouts\n%s", formatConcordance(all))
	return err
}
