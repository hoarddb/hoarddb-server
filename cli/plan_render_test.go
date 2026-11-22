package cli

import (
	"bytes"
	"io"
	"math/rand"
	"os"
	"slices"
	"strings"
	"testing"
)

// planFor runs the whole pure pipeline over one model with no answers.
func planFor(t *testing.T, model *PlanModel, answers planAnswers) Plan {
	t.Helper()
	th := DefaultPlanThresholds()
	signals := detectAll(model, th)
	kinds := placeKinds(model, signals, answers)
	return buildPlan(model, signals, kinds, buildScorecard(model, signals, th), th)
}

func checkGolden(t *testing.T, path string, got []byte) {
	t.Helper()
	if os.Getenv("PLAN_UPDATE_GOLDEN") != "" {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v (set PLAN_UPDATE_GOLDEN=1 to write it)", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("output differs from %s; rerun with PLAN_UPDATE_GOLDEN=1 and read the diff.\ngot:\n%s", path, got)
	}
}

// TestPlanBlogExampleMatchesGolden is spec T2: the blog plan, as the user page quotes it.
func TestPlanBlogExampleMatchesGolden(t *testing.T) {
	plan := planFor(t, blogWithTrace(t, true), nil)
	checkGolden(t, "testdata/plan/blog.plan.golden", []byte(renderMarkdown(plan)))
	data, err := renderJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	checkGolden(t, "testdata/plan/blog.plan.json.golden", data)
}

// TestPlanNextStepPrintsACommandRingIngestAccepts: a developer pastes the command the
// plan closes with, so it must pass the argument parser ring ingest itself uses.
func TestPlanNextStepPrintsACommandRingIngestAccepts(t *testing.T) {
	text := renderMarkdown(planFor(t, blogWithTrace(t, true), nil))
	_, next, found := strings.Cut(text, "## Next\n")
	if !found {
		t.Fatalf("the plan has no Next list:\n%s", text)
	}
	var step string
	for _, line := range strings.Split(next, "\n") {
		if strings.HasPrefix(line, "3. ") {
			step = line
		}
	}
	if step == "" {
		t.Fatalf("the Next list has no step 3:\n%s", next)
	}
	start, end := strings.Index(step, "`"), strings.LastIndex(step, "`")
	if start < 0 || end <= start {
		t.Fatalf("step 3 carries no command in backticks: %q", step)
	}
	command := strings.NewReplacer("<file>", "users.bson", "<bucket>", "Users").Replace(step[start+1 : end])
	arguments, ok := strings.CutPrefix(command, "hoarddb ingest ")
	if !ok {
		t.Fatalf("step 3 is %q, want an ingest command", command)
	}
	parsed, err := parseRingIngestArgs(splitCommand(arguments), ringIngestEnv{stdout: io.Discard, stderr: io.Discard})
	if err != nil {
		t.Fatalf("step 3, %q, is not a command ring ingest accepts: %v", command, err)
	}
	if parsed.into != "app.Users" {
		t.Fatalf("step 3, %q, parsed --into as %q, want app.Users", command, parsed.into)
	}
}

// TestPlanStatementsParse is spec T3: every statement parses with the ring parser
// and names only type words.
func TestPlanStatementsParse(t *testing.T) {
	allowed := map[string]bool{}
	for _, word := range ringTypeWords() {
		allowed[word] = true
	}
	for name, model := range map[string]*PlanModel{"blog": blogWithTrace(t, true), "blog-no-trace": loadBlogModel(t)} {
		for _, statement := range planFor(t, model, nil).HQL {
			parsed, err := Parse(statement.Text, "app")
			if err != nil {
				t.Fatalf("%s: %q did not parse: %v", name, statement.Text, err)
			}
			if parsed.Verb != "create bucket" {
				t.Fatalf("%s: %q parsed as %q", name, statement.Text, parsed.Verb)
			}
			fields, _ := parsed.Arg("fields")
			for _, field := range fields.Fields {
				if !allowed[field.Type] {
					t.Fatalf("%s: field %s has type %q outside the type words", name, field.Name, field.Type)
				}
			}
		}
	}
}

// TestPlanCostTableUsesRule11Constants is spec T11.
func TestPlanCostTableUsesRule11Constants(t *testing.T) {
	model := blogWithTrace(t, false)
	th := DefaultPlanThresholds()
	signals := detectAll(model, th)
	kinds := placeKinds(model, signals, nil)
	costs := planCosts(model, signals, kinds, th)
	want := int64(costs[0].Buckets*436 + costs[0].StoreFields*26)
	if costs[0].DeclBytes != want {
		t.Fatalf("declaration bytes %d, want buckets*436 + store fields*26 = %d", costs[0].DeclBytes, want)
	}
	th.BucketDeclBytes = 1000
	if changed := planCosts(model, signals, kinds, th); changed[0].DeclBytes != int64(costs[0].Buckets*1000+costs[0].StoreFields*26) {
		t.Fatalf("a changed constant did not change the table: %d", changed[0].DeclBytes)
	}
}

// TestPlanNoInputLeavesChoicesOpen is spec T8.
func TestPlanNoInputLeavesChoicesOpen(t *testing.T) {
	text := renderMarkdown(planFor(t, blogWithTrace(t, true), nil))
	if !strings.Contains(text, "open; this section has more than one statement for `Comment`") {
		t.Fatalf("the open-choices line is missing:\n%s", text)
	}
	if !strings.Contains(text, "create bucket Comment {") {
		t.Fatalf("the bucket alternative is not printed:\n%s", text)
	}
}

// TestPlanIsDeterministic is spec T1: shuffling the model's kinds changes no byte.
func TestPlanIsDeterministic(t *testing.T) {
	first := planFor(t, blogWithTrace(t, true), nil)
	wantMD, wantJSON := renderMarkdown(first), mustJSON(t, first)
	for seed := int64(1); seed <= 5; seed++ {
		model := blogWithTrace(t, true)
		rand.New(rand.NewSource(seed)).Shuffle(len(model.Kinds), func(i, j int) { model.Kinds[i], model.Kinds[j] = model.Kinds[j], model.Kinds[i] })
		plan := planFor(t, model, nil)
		if renderMarkdown(plan) != wantMD || mustJSON(t, plan) != wantJSON {
			t.Fatalf("seed %d: output changed with kind order", seed)
		}
	}
}

func mustJSON(t *testing.T, plan Plan) string {
	t.Helper()
	data, err := renderJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// TestScorecardPrintsNoAsPlainlyAsYes: one heading level and one format for every verdict.
func TestScorecardPrintsNoAsPlainlyAsYes(t *testing.T) {
	heading := func(verdict string) string {
		plan := Plan{Scorecard: PlanScorecard{Verdict: verdict}}
		for _, line := range strings.Split(renderMarkdown(plan), "\n") {
			if strings.HasPrefix(line, "### Verdict: ") {
				return strings.Replace(line, verdict, "%s", 1)
			}
		}
		return ""
	}
	yes, no := heading("yes"), heading(verdictNo)
	if yes == "" || yes != no {
		t.Fatalf("verdict lines differ: %q and %q", yes, no)
	}
	if !slices.Contains(planCostClasses, "seek") {
		t.Fatal("cost classes lost")
	}
}

// TestPlanVerdictStatesItsLimits (M5): every verdict is followed by what decided
// it and what the tool cannot see.
func TestPlanVerdictStatesItsLimits(t *testing.T) {
	text := renderMarkdown(planFor(t, blogWithTrace(t, true), nil))
	for _, want := range []string{"Confidence and limits", "how often each read runs", "change the design the trace describes"} {
		if !strings.Contains(text, want) {
			t.Errorf("the plan lacks %q", want)
		}
	}
	if !strings.Contains(renderGuide(), "Revise the DESIGN") || strings.Contains(renderGuide(), "until the scorecard has no") {
		t.Error("the guide must say revise the design, not the model")
	}
}
