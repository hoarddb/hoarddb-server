package cli

import (
	"os"
	"strings"
	"testing"
)

// TestLoadPlanModelRefusesUnknownField guards the strict decode: without
// DisallowUnknownFields a misspelt field reads as zero and the field vanishes
// from the plan as if it were sparse.
func TestLoadPlanModelRefusesUnknownField(t *testing.T) {
	src := `{"version":1,"source":"hand","kinds":[{"name":"posts","documents":2,"sampled":2,"fields":[` +
		`{"name":"title","type":"string","presnt_permille":1000}]}]}`
	_, err := LoadPlanModel(strings.NewReader(src))
	if err == nil {
		t.Fatal("a misspelt field was accepted")
	}
	for _, want := range []string{"presnt_permille", "present_permille"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err, want)
		}
	}
}

// TestValidatePlanModelRefusalsNameTheFix guards the repair message: a bare
// "invalid model" leaves the agent that wrote the file unable to repair it.
func TestValidatePlanModelRefusalsNameTheFix(t *testing.T) {
	okKind := ModelKind{Name: "posts", Documents: 2, Sampled: 2, Fields: []ModelField{{Name: "title", Type: "string", PresentPermille: 1000}}}
	withKinds := func(kinds ...ModelKind) *PlanModel {
		return &PlanModel{Version: PlanModelVersion, Source: "hand", Kinds: kinds}
	}
	cases := []struct {
		name  string
		model *PlanModel
		names []string
	}{
		{"version", &PlanModel{Version: 2, Source: "hand", Kinds: []ModelKind{okKind}}, []string{"version", "1"}},
		{"source", &PlanModel{Version: 1, Source: "guess", Kinds: []ModelKind{okKind}}, []string{"guess", "dump"}},
		{"empty kind name", withKinds(ModelKind{Name: "", Documents: 1, Sampled: 1}), []string{"kind"}},
		{"duplicate kind", withKinds(okKind, okKind), []string{"posts"}},
		{"permille range", withKinds(ModelKind{Name: "posts", Documents: 2, Sampled: 2, Fields: []ModelField{
			{Name: "title", Type: "string", PresentPermille: 1001}}}), []string{"title", "1001"}},
		{"ref to unknown kind", withKinds(ModelKind{Name: "posts", Documents: 2, Sampled: 2, Fields: []ModelField{
			{Name: "author", Type: "objectid", PresentPermille: 1000, Ref: &ModelRef{To: "users2", ResolvePermille: 1000}}}}), []string{"author", "users2"}},
		{"array length order", withKinds(ModelKind{Name: "posts", Documents: 2, Sampled: 2, Fields: []ModelField{
			{Name: "tags", Type: "array", PresentPermille: 1000, Array: &ModelArray{LenP50: 5, LenP95: 3, LenMax: 9}}}}), []string{"tags", "len_p50"}},
		{"unknown role", withKinds(ModelKind{Name: "posts", Documents: 2, Sampled: 2, Fields: []ModelField{
			{Name: "hits", Type: "int", PresentPermille: 1000, Role: "tally"}}}), []string{"hits", "tally"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidatePlanModel(tc.model)
			if err == nil {
				t.Fatal("the model was accepted")
			}
			msg := err.Error()
			for _, want := range tc.names {
				if !strings.Contains(msg, want) {
					t.Fatalf("error %q does not name %q", msg, want)
				}
			}
			if !strings.Contains(msg, "Fix") && !strings.Contains(msg, "fix") {
				t.Fatalf("error %q does not say how to fix it", msg)
			}
		})
	}
}

// TestBlogFixtureLoadsAndValidates keeps the committed blog model loadable by
// the strict loader and accepted by the validator, so later steps build on a
// fixture that is already a legal model.
func TestBlogFixtureLoadsAndValidates(t *testing.T) {
	file, err := os.Open("testdata/plan/blog.model.json")
	if err != nil {
		t.Fatalf("open blog model: %v", err)
	}
	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Errorf("close blog model: %v", err)
		}
	})
	model, err := LoadPlanModel(file)
	if err != nil {
		t.Fatalf("load blog model: %v", err)
	}
	if err := ValidatePlanModel(model); err != nil {
		t.Fatalf("validate blog model: %v", err)
	}
	if len(model.Kinds) != 3 {
		t.Fatalf("blog model has %d kinds, want users, posts and comments", len(model.Kinds))
	}
}
