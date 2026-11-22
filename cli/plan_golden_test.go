package cli

import (
	"bytes"
	"encoding/json"
	"github.com/hoarddb/hoarddb-server/internal/repofile"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type corpusVerdict struct {
	Cells   string `json:"cells"`
	Verdict string `json:"verdict"`
}

func loadCorpusVerdicts(t *testing.T) map[string]corpusVerdict {
	t.Helper()
	data, err := os.ReadFile("testdata/plan/corpus/verdicts.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var verdicts map[string]corpusVerdict
	if err := json.Unmarshal(data, &verdicts); err != nil {
		t.Fatalf("verdicts golden: %v", err)
	}
	return verdicts
}

func loadCorpusModel(t *testing.T, app string) *PlanModel {
	t.Helper()
	file, err := os.Open(filepath.Join("testdata/plan/corpus", app+".model.json"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Errorf("close %s: %v", app, err)
		}
	})
	model, err := LoadPlanModel(file)
	if err != nil {
		t.Fatalf("%s: %v", app, err)
	}
	if err := ValidatePlanModel(model); err != nil {
		t.Fatalf("%s: %v", app, err)
	}
	return model
}

// planKindNames is the kind names of a model in file order: the signature that
// tells two models apart when they share no other field.
func planKindNames(model *PlanModel) string {
	var names []string
	for _, kind := range model.Kinds {
		names = append(names, kind.Name)
	}
	return strings.Join(names, ",")
}

// TestPlanCorpusHasSixteenModels pins the set, and keeps the blog model out of it.
func TestPlanCorpusHasSixteenModels(t *testing.T) {
	verdicts := loadCorpusVerdicts(t)
	if len(verdicts) != 16 {
		t.Fatalf("%d verdicts, want 16", len(verdicts))
	}
	files, err := filepath.Glob("testdata/plan/corpus/*.model.json")
	if err != nil || len(files) != 16 {
		t.Fatalf("%d model files (%v), want 16", len(files), err)
	}
	for app := range verdicts {
		if app == "blog" {
			t.Fatal("the blog model is not a corpus model")
		}
		if _, err := os.Stat("testdata/plan/corpus/" + app + ".model.json"); err != nil {
			t.Errorf("no model for %s", app)
		}
	}
}

// planCorpusMismatches is the list of applications whose plan differs from the
// corpus note's table. Each model is built only from the note's own section 7
// evidence (see <app>.evidence.md next to it), so a difference is a finding,
// not something to tune away: the spec (section 20) and the build note (R1, G1)
// say a model the rule cannot reproduce is a stop-and-report. The owner decides
// whether the rule, the corpus row or an unstated input is wrong. Each entry
// records what the plan gives today, so a rule change that moves it fails this
// test and the entry must be removed or re-recorded with the reason.
var planCorpusMismatches = map[string]struct{ cells, verdict, reason string }{
	"app-01": {"FFPPP", "only with redesign of dynamic filters, client sorting", "rule 3 only (corpus p, plan P): \"$group by view, country, version\" is a declared dimension set (spec reduces it). Rule 1 now fails as the corpus says, since the regex search line has no delegated search statement; the verdict now matches."},
	"app-02": {"PPPFP", "only with redesign of permissions in the query", "rules 1 and 5 (corpus p p, plan P P): the note counts 118 WHERE concatenations, not optional filters per list, and 10 transaction files without their span (unstated). Verdict matches."},
	"app-03": {"FFPFP", "not a good candidate for hoardDB", "rules 3 and 5 (corpus F F, plan P P): the 333 joins are not classified and the 192 transactions' spans are not stated (both unstated). Verdict matches."},
	"app-04": {"PPPPP", "yes", "verdict only (corpus \"yes with caveats\"): the note names two caveats (newest-first paged lists, reads five tables of its own) but R1 has no source of a caveat for an all-pass scorecard (rule incomplete)."},
	"app-05": {"FFPPP", "not a good candidate for hoardDB", "rules 3, 4, 5 (corpus F F F, plan P P P): joins unclassified, repos per user not stated (rule 4 needs the fan), transaction spans not stated (all unstated). Verdict matches, with the filter count at exactly the stated \"15+\"."},
	"app-06": {"PPPPP", "yes", "rules 1, 2, 3 (corpus F p p, plan P P P): search is in Elasticsearch, which the spec scores as a pass (spec 6 detector R-external-search), while the corpus scores rule 1 F; sorts are \"mostly fixed or a closed enum\" and the 139 aggregates are reports without a stated dynamic dimension. Corpus vs spec on delegated search."},
	"app-07": {"PPpPP", "yes with caveats", "rules 1, 2, 3, 4 (corpus F p F p p, plan P P p P P): the note puts the dynamic SQL and the GROUP BY in the \"offline\" archiver and says \"serving read is pre-computed archives\", so the evidenced surface is admin and the rules give a caveat. The corpus verdict \"no for the read model\" (and rule 3 F) treats the archiver as the product: corpus row vs rule scope, owner to decide."},
	"app-08": {"PFPFF", "only with redesign of client sorting, permissions in the query, atomic multi-entity writes", "rule 1 (F vs P): optional filters per list unstated (53 LIKE sites only); rule 2 (p vs F): corpus section 5 lists this application under client-chosen sort while its section 2 row says p, so the corpus contradicts itself; rule 3 (F vs P): 452 JOINs are not classified in the note (unstated). Rules 4 and 5 match."},
	"app-09": {"FpPPF", "only with redesign of dynamic filters, atomic multi-entity writes", "rule 2 (F vs p): the note says the sort column comes from a per-user setting and R1 scores setting as partial, while the corpus row and its reason say client sort (rule text vs corpus row); rule 3 (F vs P): the GROUP BY counts are a declared dimension, which spec 15 reduces; rule 5 (p vs F): surface of the transactions unstated, primary assumed."},
	"app-10": {"PPPPP", "yes", "rule 1 (corpus p, plan P): search is delegated to a plugin, which the spec scores as a pass (R-external-search), while the corpus scores a partial. Spec vs corpus."},
	"app-11": {"PPPPP", "yes", "rules 4 and 5 (corpus F p, plan P P): spec 16.2 says a membership shape \"always fits\" while the corpus scores the same role-array reverse lookup F (spec vs corpus); which kinds one withTransaction spans is unstated."},
	"app-12": {"FPPPP", "only with redesign of dynamic filters", "rules 3 and 4 (corpus p p, plan P P): section 7 says the $group sums are \"by currency, property\" (a declared dimension: F2 reduces it, spec 15) and the manager $in scoping is a membership shape (spec 16.2: always fits). Spec vs corpus: the corpus scores partial where the spec's own reductions say pass. Verdict matches."},
	"app-13": {"FFFFP", "not a good candidate for hoardDB", "rule 5 (corpus F, plan P): input unstated. The note counts \"transaction do 29\" but not which kinds one transaction spans, and rule 5 needs two kinds; nothing was invented."},
	"app-14": {"FPPPP", "only with redesign of dynamic filters", "rules 2, 3, 4 (corpus p p F, plan P P P): the sort and the dimension are not stated (unstated), and rooms per user is not stated, which rule 4 needs (unstated). Rule 1 now fails as the corpus says."},
	"app-15": {"FPPPP", "only with redesign of dynamic filters", "rules 2, 3, 4 (corpus p p F, plan P P P): rule 4 needs boards per user at p95 and the note states none (input unstated); the \"sort float field\" is a fixed sort and the hot aggregate is a fixed dimension (spec reduces both). Verdict differs because rule 4 is the unstated one."},
	"app-16": {"FPpFP", "only with redesign of dynamic filters, permissions in the query", "rules 2 and 3 (corpus F F, plan P p) and verdict: the note gives no sort source (input unstated), so no client-sort leg and the \"no\" conjunction cannot hold; the ~170 joins are not classified (unstated). The note's \"any column, any combination\" is prose the rule has no input for."},
}

// TestPlanReproducesTheCorpusVerdicts: every application not in the mismatch
// table must reproduce the corpus note's cells and verdict exactly; every
// application in it must still give the recorded (different) answer.
func TestPlanReproducesTheCorpusVerdicts(t *testing.T) {
	for app, want := range loadCorpusVerdicts(t) {
		model := loadCorpusModel(t, app)
		th := DefaultPlanThresholds()
		card := buildScorecard(model, detectAll(model, th), th)
		var cells strings.Builder
		for _, rule := range card.Rules {
			cells.WriteString(rule.Cell)
		}
		known, isKnown := planCorpusMismatches[app]
		if !isKnown {
			if cells.String() != want.Cells || card.Verdict != want.Verdict {
				t.Errorf("%s: plan gives %s / %q, corpus says %s / %q (add it to planCorpusMismatches with the reason, do not edit a rule)", app, cells.String(), card.Verdict, want.Cells, want.Verdict)
			}
			continue
		}
		if cells.String() == want.Cells && card.Verdict == want.Verdict {
			t.Errorf("%s now reproduces the corpus: remove it from planCorpusMismatches", app)
		}
		if cells.String() != known.cells || card.Verdict != known.verdict {
			t.Errorf("%s: plan gives %s / %q, the table records %s / %q (%s)", app, cells.String(), card.Verdict, known.cells, known.verdict, known.reason)
		}
	}
}

// TestPlanCorpusModelsAreShapesAndCountsOnly: no long strings, nothing outside
// the model schema (the strict loader already refuses unknown keys).
func TestPlanCorpusModelsAreShapesAndCountsOnly(t *testing.T) {
	files, _ := filepath.Glob("testdata/plan/corpus/*.model.json")
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var tree any
		if err := json.Unmarshal(data, &tree); err != nil {
			t.Fatal(err)
		}
		var walk func(any)
		walk = func(node any) {
			switch value := node.(type) {
			case string:
				if len(value) > 80 {
					t.Errorf("%s: a string of %d bytes", path, len(value))
				}
			case []any:
				for _, item := range value {
					walk(item)
				}
			case map[string]any:
				for _, item := range value {
					walk(item)
				}
			}
		}
		walk(tree)
	}
}

// TestLoadPlanModelNumbersTraceLines: a model's trace has no line numbers on
// disk, so the loader gives index+1, which the plan cites as evidence.
func TestLoadPlanModelNumbersTraceLines(t *testing.T) {
	model := loadCorpusModel(t, "app-04")
	for index, line := range model.Trace {
		if line.Ordinal != index+1 {
			t.Fatalf("line %d has ordinal %d", index, line.Ordinal)
		}
	}
}

// TestPlanBlogIsNotACorpusModel: the blog model is the docs example, not one of
// the sixteen corpus applications. It lives outside the corpus directory, has no
// verdict row, and its kinds are not the kinds of any corpus model.
func TestPlanBlogIsNotACorpusModel(t *testing.T) {
	data, err := os.ReadFile("testdata/plan/docs-blog.model.json")
	if err != nil {
		t.Fatal(err)
	}
	blog, err := LoadPlanModel(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("blog model: %v", err)
	}
	if _, inCorpus := loadCorpusVerdicts(t)["docs-blog"]; inCorpus {
		t.Fatal("the blog model has a corpus verdict row")
	}
	corpusFiles, err := filepath.Glob("testdata/plan/corpus/*.model.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range corpusFiles {
		if filepath.Base(path) == "docs-blog.model.json" {
			t.Fatal("the blog model is in the corpus directory")
		}
	}
	for app := range loadCorpusVerdicts(t) {
		if planKindNames(loadCorpusModel(t, app)) == planKindNames(blog) {
			t.Errorf("%s has the blog model's kinds (%s); the blog is not a corpus model", app, planKindNames(blog))
		}
	}
}

// TestPlanCorpusModelsCiteTheirEvidence: every model has an evidence file that
// cites one corpus sentence per trace line, and no two models are the same set
// of kinds (three applications were once one cloned file).
func TestPlanCorpusModelsCiteTheirEvidence(t *testing.T) {
	seen := map[string]string{}
	for app := range loadCorpusVerdicts(t) {
		model := loadCorpusModel(t, app)
		data := repofile.ReadPrivate(t, planCorpusEvidenceDir+"/"+app+".evidence.md")
		cited := strings.Count(string(data), "\n   - ")
		if cited != len(model.Trace) {
			t.Errorf("%s: %d trace lines but %d cited sentences", app, len(model.Trace), cited)
		}
		if !strings.Contains(string(data), "Unstated") {
			t.Errorf("%s: the evidence file does not say what the note leaves unstated", app)
		}
		key := planKindNames(model)
		if other, dup := seen[key]; dup {
			t.Errorf("%s and %s have the same kinds (%s)", app, other, key)
		}
		seen[key] = app
	}
}

// planCorpusCauses is the cause class of each recorded mismatch, in the words
// of the spec's "Known mismatches against the corpus" table. The spec names each
// application; planCorpusNames maps the label to that name.
// TestPlanSpecTableAgreesWithTheMismatchTable keeps the two in step.
var planCorpusCauses = map[string][]string{
	"app-04": {"rule gap"},
	"app-10": {"spec versus corpus"},
	"app-12": {"spec versus corpus"},
	"app-09": {"rule text versus corpus", "spec versus corpus", "input unstated"},
	"app-02": {"input unstated"},
	"app-15": {"input unstated", "spec versus corpus"},
	"app-11": {"spec versus corpus", "input unstated"},
	"app-14": {"input unstated"},
	"app-08": {"corpus internal", "input unstated"},
	"app-06": {"spec versus corpus", "input unstated"},
	"app-01": {"spec versus corpus"},
	"app-16": {"input unstated"},
	"app-13": {"input unstated"},
	"app-03": {"input unstated"},
	"app-05": {"input unstated"},
	"app-07": {"corpus row versus rule scope"},
}

// planCorpusEvidenceDir holds the evidence file of each corpus model and the
// README that maps each label to its application name. Those names are kept out of
// the public module, so the files live in the private docs tree.
const planCorpusEvidenceDir = "docs/internals/research/advisor-corpus-evidence"

// planCorpusNames reads the label-to-application table from the evidence README.
func planCorpusNames(t *testing.T) map[string]string {
	t.Helper()
	names := map[string]string{}
	for _, line := range strings.Split(string(repofile.ReadPrivate(t, planCorpusEvidenceDir+"/README.md")), "\n") {
		cells := strings.Split(line, "|")
		if len(cells) != 4 || !strings.HasPrefix(strings.TrimSpace(cells[1]), "app-") {
			continue
		}
		names[strings.TrimSpace(cells[1])] = strings.TrimSpace(cells[2])
	}
	if len(names) != 16 {
		t.Fatalf("the evidence README maps %d labels, want 16", len(names))
	}
	return names
}

// TestPlanSpecTableAgreesWithTheMismatchTable: every app in the mismatch map has
// a row in the spec's known-mismatches table that names each of its causes.
func TestPlanSpecTableAgreesWithTheMismatchTable(t *testing.T) {
	names := planCorpusNames(t)
	path, err := repofile.Find("docs/specs/access-pattern-advisor-spec.md")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	spec := string(data)
	at := strings.Index(spec, "## Known mismatches against the corpus")
	if at < 0 {
		t.Fatal("the spec has no known-mismatches section")
	}
	section := strings.ToLower(spec[at:])
	for app := range planCorpusMismatches {
		causes, ok := planCorpusCauses[app]
		label, labelled := names[app]
		if !ok || !labelled {
			t.Errorf("%s has no recorded cause class or spec label", app)
			continue
		}
		row := ""
		for _, line := range strings.Split(section, "\n") {
			if strings.HasPrefix(line, "| "+strings.ToLower(label)+" |") {
				row = line
			}
		}
		if row == "" {
			t.Errorf("the spec table has no row for %s", label)
			continue
		}
		for _, cause := range causes {
			if !strings.Contains(row, cause) {
				t.Errorf("the spec row for %s does not name the cause %q", label, cause)
			}
		}
	}
}
