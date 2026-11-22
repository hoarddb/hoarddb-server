package cli

import (
	"strings"
	"testing"
)

// blogWithTrace is the blog model plus the four traced reads of spec 4.2 that
// name its kinds. The author_id line is the cross-owner read.
func blogWithTrace(t *testing.T, withAuthorRead bool) *PlanModel {
	t.Helper()
	model := loadBlogModel(t)
	model.Trace = []TraceLine{
		{Ordinal: 1, Op: "get", Coll: "users", By: []string{"_id"}},
		{Ordinal: 2, Op: "list", Coll: "comments", By: []string{"post_id"}, Sort: []string{"ts:asc"}, Limit: 20, Weight: 10},
		{Ordinal: 3, Op: "insert", Coll: "comments"},
	}
	if withAuthorRead {
		model.Trace = append(model.Trace, TraceLine{Ordinal: 4, Op: "list", Coll: "comments", By: []string{"author_id"}, Sort: []string{"ts:desc"}, Limit: 10})
	}
	return model
}

func placeBlog(t *testing.T, model *PlanModel, answers planAnswers) []PlanKind {
	t.Helper()
	th := DefaultPlanThresholds()
	signals := append(detectShape(model, th), detectTrace(model, detectShape(model, th), th)...)
	sortPlanSignals(signals)
	return placeKinds(model, signals, answers)
}

func planKindNamed(kinds []PlanKind, name string) *PlanKind {
	for index := range kinds {
		if kinds[index].Name == name {
			return &kinds[index]
		}
	}
	return nil
}

func storeFieldOf(kind *PlanKind, name string) *PlanStoreField {
	for index := range kind.StoreFields {
		if kind.StoreFields[index].Name == name {
			return &kind.StoreFields[index]
		}
	}
	return nil
}

// TestPlanBlogPlacement guards the entity-centric result: two buckets, the
// comments folded into Post as a fifoStore, and the author read as a conflict.
func TestPlanBlogPlacement(t *testing.T) {
	kinds := placeBlog(t, blogWithTrace(t, true), nil)
	if len(kinds) != 2 || planKindNamed(kinds, "User") == nil || planKindNamed(kinds, "Post") == nil {
		t.Fatalf("kinds = %+v, want exactly User and Post", kinds)
	}
	comments := storeFieldOf(planKindNamed(kinds, "Post"), "Comments")
	if comments == nil || comments.Store != "fifoStore" || comments.Grade != GradeConflict {
		t.Fatalf("Post.Comments = %+v, want a fifoStore at grade conflict", comments)
	}
	if avatar := storeFieldOf(planKindNamed(kinds, "User"), "Avatar"); avatar == nil || avatar.Store != "blobStore" || len(avatar.Alternatives) == 0 {
		t.Fatalf("User.Avatar = %+v, want a blobStore with the pointer-layout alternative", avatar)
	}
	if open := planKindNamed(kinds, "Post").Open; len(open) != 1 || open[0] != "comments.placement" {
		t.Fatalf("Post open choices = %v, want comments.placement", open)
	}
}

// TestPlanCrossOwnerReadOverridesOwnership is spec T4: the author_id read
// turns a trace-grade store field into a conflict, and removing it restores trace.
func TestPlanCrossOwnerReadOverridesOwnership(t *testing.T) {
	without := storeFieldOf(planKindNamed(placeBlog(t, blogWithTrace(t, false), nil), "Post"), "Comments")
	if without == nil || without.Grade != GradeTrace {
		t.Fatalf("without the author read: %+v, want grade trace", without)
	}
	with := storeFieldOf(planKindNamed(placeBlog(t, blogWithTrace(t, true), nil), "Post"), "Comments")
	if with == nil || with.Grade != GradeConflict {
		t.Fatalf("with the author read: %+v, want grade conflict", with)
	}
}

// TestPlanNoTraceLeavesOrderOpen guards shape grade: the data cannot tell a
// queue from a stack, so the store word stays an open choice.
func TestPlanNoTraceLeavesOrderOpen(t *testing.T) {
	model := loadBlogModel(t)
	comments := storeFieldOf(planKindNamed(placeBlog(t, model, nil), "Post"), "Comments")
	if comments == nil || comments.Store != "" || !comments.Orderless || comments.Grade != GradeShape || len(comments.Alternatives) < 2 {
		t.Fatalf("Post.Comments = %+v, want an open order at grade shape", comments)
	}
}

// TestPlanAnswersOverrideEvidence is spec T7.
func TestPlanAnswersOverrideEvidence(t *testing.T) {
	kinds := placeBlog(t, blogWithTrace(t, true), planAnswers{"comments.placement": "store-field"})
	comments := storeFieldOf(planKindNamed(kinds, "Post"), "Comments")
	if comments == nil || comments.Grade != GradeAnswered {
		t.Fatalf("Post.Comments = %+v, want grade answered", comments)
	}
	overruled := false
	for _, signal := range comments.Evidence {
		if signal.Detector == "T6" && len(signal.Note) > 9 && signal.Note[:9] == "overruled" {
			overruled = true
		}
	}
	if !overruled {
		t.Fatalf("evidence %+v does not keep the T6 signal marked overruled", comments.Evidence)
	}
	if open := planKindNamed(kinds, "Post").Open; len(open) != 0 {
		t.Fatalf("open choices after an answer = %v", open)
	}
	both := placeBlog(t, blogWithTrace(t, true), planAnswers{"comments.placement": "bucket"})
	if comment := planKindNamed(both, "Comment"); comment == nil || comment.Grade != GradeAnswered {
		t.Fatalf("answer bucket gave Comment %+v, want grade answered", comment)
	}
	if planKindNamed(both, "Comment") == nil || storeFieldOf(planKindNamed(both, "Post"), "Comments") != nil {
		t.Fatalf("answer bucket gave %+v, want a Comment bucket and no store field", both)
	}
}

// TestPlanMarksStoresTheBuildCannotWrite is spec T6: a priority take lands on
// heapStore, which has no push in ring mode, so the alternative travels with it.
func TestPlanMarksStoresTheBuildCannotWrite(t *testing.T) {
	id := ModelField{Name: "_id", Type: "string", PresentPermille: 1000, DistinctPermille: 1000}
	model := &PlanModel{Version: 1, Source: "hand", Kinds: []ModelKind{
		{Name: "jobs", Documents: 50, Sampled: 50, Fields: []ModelField{id,
			{Name: "queue_id", Type: "string", PresentPermille: 1000, Ref: &ModelRef{To: "queues", ResolvePermille: 1000}},
			{Name: "rank", Type: "int", PresentPermille: 1000}}},
		{Name: "queues", Documents: 5, Sampled: 5, Fields: []ModelField{id}},
	}, Trace: []TraceLine{{Ordinal: 1, Op: "take", Coll: "jobs", By: []string{"queue_id"}, Order: "priority:rank"}}}
	kinds := placeBlog(t, model, nil)
	jobs := storeFieldOf(planKindNamed(kinds, "Queue"), "Jobs")
	if jobs == nil || jobs.Store != "heapStore" || len(jobs.Alternatives) != 1 {
		t.Fatalf("Queue.Jobs = %+v, want heapStore with one alternative", jobs)
	}
}

// TestPlanNeverProposesBucketPerInstance is spec T5: 300 files of one shape are
// one bucket named for the kind, not one bucket per file. The model comes from
// reading the directory, so it is the multi-instance input the rule is about.
func TestPlanNeverProposesBucketPerInstance(t *testing.T) {
	dir := t.TempDir()
	writeSameShapedFiles(t, dir, 300)
	model, _, err := planCollect([]string{dir}, 0, defaultMaxBuckets)
	if err != nil {
		t.Fatalf("planCollect: %v", err)
	}
	if len(model.Kinds) != 1 {
		t.Fatalf("300 files made %d kinds before placement, want 1", len(model.Kinds))
	}
	kinds := placeBlog(t, model, nil)
	if len(kinds) != 1 || kinds[0].Name != "Customer" {
		t.Fatalf("kinds = %+v, want one bucket named Customer", kinds)
	}
}

// TestPlanOrderOpenConflictStaysParseable: a store field that is both an open
// order and a conflict prints only statements the parser takes, and its order
// question offers only store words, each with a cost.
func TestPlanOrderOpenConflictStaysParseable(t *testing.T) {
	model := loadBlogModel(t)
	model.Trace = []TraceLine{{Ordinal: 1, Op: "list", Coll: "comments", By: []string{"author_id"}, Sort: []string{"ts:desc"}}}
	plan := planFor(t, model, nil)
	for _, statement := range plan.HQL {
		if _, err := Parse(statement.Text, "app"); err != nil {
			t.Fatalf("%q does not parse: %v", statement.Text, err)
		}
	}
	th := DefaultPlanThresholds()
	signals := detectAll(model, th)
	for _, question := range planQuestions(placeKinds(model, signals, nil), signals) {
		if question.Choice != "comments.order" {
			continue
		}
		for _, option := range question.Options {
			if !strings.HasSuffix(option.Answer, "Store") || option.Cost == "" {
				t.Fatalf("order option %+v is not a costed store word", option)
			}
		}
	}
}
