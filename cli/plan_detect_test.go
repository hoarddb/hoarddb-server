package cli

import (
	"os"
	"testing"
)

// loadBlogModel loads the committed blog fixture for detector tests.
func loadBlogModel(t *testing.T) *PlanModel {
	t.Helper()
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
	return model
}

// kindWithArray builds a one-kind model whose only array is a list of documents.
func kindWithArray(lenP95, lenMax int) ModelKind {
	return ModelKind{Name: "posts", Documents: 10, Sampled: 10, Fields: []ModelField{
		{Name: "_id", Type: "string", PresentPermille: 1000, DistinctPermille: 1000},
		{Name: "comments", Type: "array", PresentPermille: 1000, Array: &ModelArray{
			LenP50: 1, LenP95: lenP95, LenMax: lenMax, ElemIsDocument: true}},
	}}
}

func planSignalsFor(signals []PlanSignal, detector, kind string) []PlanSignal {
	var found []PlanSignal
	for _, signal := range signals {
		if signal.Detector == detector && signal.Kind == kind {
			found = append(found, signal)
		}
	}
	return found
}

// TestDetectShapeBlog guards the entity-centric detector: comments belong to
// posts (S6) but not to users, because users is referred to by two kinds.
func TestDetectShapeBlog(t *testing.T) {
	signals := detectShape(loadBlogModel(t), DefaultPlanThresholds())
	owned := planSignalsFor(signals, "S6", "comments")
	if len(owned) != 1 || owned[0].Other != "posts" || owned[0].Field != "post_id" {
		t.Fatalf("S6 for comments = %+v, want one signal to posts via post_id", owned)
	}
	for _, kind := range []string{"comments", "posts", "users"} {
		if len(planSignalsFor(signals, "S1", kind)) == 0 {
			t.Errorf("no S1 identity signal for %s", kind)
		}
	}
	large := planSignalsFor(signals, "S4", "users")
	if len(large) != 1 || large[0].Field != "avatar" || large[0].Pattern != "P7" {
		t.Fatalf("S4 for users = %+v, want avatar as P7", large)
	}
}

// TestS2ThresholdBoundary guards the off-by-one in the growing-list test.
func TestS2ThresholdBoundary(t *testing.T) {
	cases := []struct {
		p95, max int
		want     int
	}{{7, 63, 0}, {8, 63, 1}, {7, 64, 1}, {7, 7, 0}}
	for _, tc := range cases {
		got := detectGrowingList(kindWithArray(tc.p95, tc.max), DefaultPlanThresholds())
		if len(got) != tc.want {
			t.Errorf("p95=%d max=%d: %d signals, want %d (%+v)", tc.p95, tc.max, len(got), tc.want, got)
		}
	}
}

// TestS2aOrderedAndS3Keyed guards the order and key signals inside a list.
func TestS2aOrderedAndS3Keyed(t *testing.T) {
	kind := kindWithArray(20, 20)
	kind.Fields[1].Array.OrderField = "ts"
	kind.Fields[1].Array.MonotonePermille = 979
	if got := detectGrowingList(kind, DefaultPlanThresholds()); len(got) != 1 {
		t.Fatalf("979 permille monotone: %d signals, want only S2", len(got))
	}
	kind.Fields[1].Array.MonotonePermille = 980
	kind.Fields[1].Array.KeyField = "name"
	kind.Fields[1].Array.KeyDistinctPermille = 1000
	got := detectGrowingList(kind, DefaultPlanThresholds())
	detectors := map[string]string{}
	for _, signal := range got {
		detectors[signal.Detector] = signal.Pattern
	}
	if detectors["S2a"] == "" || detectors["S3"] != "P4" {
		t.Fatalf("signals %+v, want S2a and S3 as P4", got)
	}
}
