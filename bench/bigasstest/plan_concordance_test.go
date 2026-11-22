package main

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// The benchmark's own advisor check: describe each layout to `hoarddb plan` as the relational
// source it replaces, and compare the store type the advisor proposes for every benchmarked store
// field with the one the benchmark seeds.

func rowFor(rows []concordanceRow, store string) (concordanceRow, bool) {
	for _, row := range rows {
		if row.Store == store {
			return row, true
		}
	}
	return concordanceRow{}, false
}

func TestPlanInputsAreAValidModelAndTrace(t *testing.T) {
	for _, layout := range benchLayouts {
		gen, err := model.New(layout, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		inputs, err := buildPlanInputs(gen)
		if err != nil {
			t.Fatalf("%s: %v", layout, err)
		}
		var decoded struct {
			Version int `json:"version"`
			Kinds   []struct {
				Name string `json:"name"`
			} `json:"kinds"`
		}
		if err := json.Unmarshal(inputs.Model, &decoded); err != nil || decoded.Version != 1 {
			t.Fatalf("%s: model JSON %v, version %d", layout, err, decoded.Version)
		}
		for _, bucket := range gen.Schema() {
			if !slices.ContainsFunc(decoded.Kinds, func(k struct {
				Name string `json:"name"`
			}) bool {
				return k.Name == strings.ToLower(bucket.Name)
			}) {
				t.Errorf("%s: bucket %s has no kind in the model", layout, bucket.Name)
			}
		}
		if lines := strings.Count(strings.TrimSpace(string(inputs.Trace)), "\n") + 1; lines < len(gen.Schema()) {
			t.Errorf("%s: a trace of %d lines is too short to describe the reads", layout, lines)
		}
	}
}

// Every store the mix reads gets one row with the benchmark's word, whatever the advisor says;
// the stores no request touches are listed as not exercised, never silently left out.
func TestConcordanceCoversEveryDeclaredStore(t *testing.T) {
	for _, layout := range benchLayouts {
		gen, err := model.New(layout, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := planConcordance(gen)
		if err != nil {
			t.Fatalf("%s: %v", layout, err)
		}
		for _, bucket := range gen.Schema() {
			for _, field := range bucket.Fields {
				if !field.IsStore() {
					continue
				}
				row, ok := rowFor(rows, bucket.Name+"."+field.Name)
				if !ok {
					t.Errorf("%s: %s.%s has no concordance row", layout, bucket.Name, field.Name)
					continue
				}
				if row.Benchmark != field.Type {
					t.Errorf("%s: row says the benchmark seeds %s, the schema says %s", row.Store, row.Benchmark, field.Type)
				}
				if !slices.Contains([]string{outcomeAgree, outcomeDiffer, outcomeOpen, outcomeNotExercised}, row.Outcome) {
					t.Errorf("%s: outcome %q", row.Store, row.Outcome)
				}
			}
		}
	}
}

// The three stores the coverage note names, with what the advisor said when this was written: the
// feed is a fifo and the advisor agrees; the ledger's expenses are a btree and the advisor proposes
// a fifoStore, because the benchmark reads them as a page in key order and only a range read asks
// for a btreeStore; the memories are a hash listed whole and the advisor leaves the order open. The
// last two are findings to report. This test pins them, so a change in the advisor or in the way the
// benchmark reads a store shows as a failure to read, not as a silent drift.
func TestConcordanceTheStoresTheNoteNames(t *testing.T) {
	type expectation struct{ advisor, outcome string }
	want := map[model.Layout]map[string]expectation{
		model.Social: {"User.Feed": {"fifoStore", outcomeAgree}},
		model.Ledger: {"Account.Expenses": {"fifoStore", outcomeDiffer}},
		model.Agent:  {"User.Memories": {"", outcomeOpen}},
	}
	for layout, stores := range want {
		gen, err := model.New(layout, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := planConcordance(gen)
		if err != nil {
			t.Fatal(err)
		}
		for store, expected := range stores {
			row, ok := rowFor(rows, store)
			if !ok {
				t.Fatalf("%s: no row", store)
			}
			if row.Advisor != expected.advisor || row.Outcome != expected.outcome {
				t.Errorf("%s: advisor %q, outcome %q, want %q and %q (%s)", store, row.Advisor, row.Outcome, expected.advisor, expected.outcome, row.Note)
			}
		}
	}
}

// A disagreement is printed with both words so it can be read and reported, not hidden.
func TestConcordanceReportNamesEveryDisagreement(t *testing.T) {
	rows := []concordanceRow{
		{Store: "User.Feed", Benchmark: "fifoStore", Advisor: "fifoStore", Outcome: outcomeAgree},
		{Store: "Room.Members", Benchmark: "hashStore", Advisor: "btreeStore", Outcome: outcomeDiffer},
		{Store: "Post.Likes", Benchmark: "hashStore", Outcome: outcomeOpen, Note: "order open"},
	}
	text := formatConcordance(rows)
	for _, want := range []string{"agree", "User.Feed", "differ", "Room.Members", "benchmark hashStore", "advisor btreeStore", "open", "Post.Likes", "1 agree, 1 differ, 1 open"} {
		if !strings.Contains(text, want) {
			t.Errorf("the report lacks %q:\n%s", want, text)
		}
	}
}

// The two hand tables and the untouched-store set name real stores and fields, so a renamed bucket
// cannot leave a stale entry that silently stops deciding anything.
func TestConcordanceTablesNameRealStoresAndFields(t *testing.T) {
	stores, scalars := map[string]bool{}, map[string]bool{}
	for _, layout := range benchLayouts {
		gen, err := model.New(layout, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, bucket := range gen.Schema() {
			for _, field := range bucket.Fields {
				if field.IsStore() {
					stores[bucket.Name+"."+field.Name] = true
				} else {
					scalars[bucket.Name+"."+field.Name] = true
				}
			}
		}
	}
	for key := range keyedStores {
		if !stores[key] {
			t.Errorf("keyedStores names %s, which is not a store field of any layout", key)
		}
	}
	for key := range untouchedStores {
		if !stores[key] {
			t.Errorf("untouchedStores names %s, which is not a store field of any layout", key)
		}
	}
	for key, target := range documentRefs {
		if !scalars[key] {
			t.Errorf("documentRefs names %s, which is not a scalar field of any layout", key)
		}
		bucketFound := false
		for name := range stores {
			bucketFound = bucketFound || strings.HasPrefix(name, target+".")
		}
		for name := range scalars {
			bucketFound = bucketFound || strings.HasPrefix(name, target+".")
		}
		if !bucketFound {
			t.Errorf("documentRefs: %s refers to %s, which is not a bucket", key, target)
		}
	}
}

// User.Friends is seeded and no request of the mix touches it, so it is not exercised, whatever its
// store type; the totals are what the review settled.
func TestConcordanceCountsOnlyWhatTheMixTouches(t *testing.T) {
	var out strings.Builder
	if err := planCheckMain(&out, 1); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	if !strings.Contains(text, "not-exercised  User.Friends") {
		t.Errorf("User.Friends must be not-exercised:\n%s", text)
	}
	if !strings.Contains(text, "11 agree, 1 differ, 7 open, 4 not exercised") {
		t.Errorf("the totals are not 11 agree, 1 differ, 7 open, 4 not exercised:\n%s", text)
	}
}
