package main

import (
	"bytes"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/bench/social/ringload"
)

// wholeRequests is the request list of a step as the whole-document run sends it.
func wholeRequests(want []string) []string {
	whole := make([]string, len(want))
	for index, request := range want {
		whole[index] = strings.Replace(request, "get-fields ", "get ", 1)
	}
	return whole
}

// TestRingReadsProjectTheScalarsTheyUse: R2, R3, R4 and R7 ask get_fields, never get, and the
// request count of each operation is unchanged.
func TestRingReadsProjectTheScalarsTheyUse(t *testing.T) {
	for _, step := range ringSteps() {
		t.Run(step.name, func(t *testing.T) {
			model := ringFixture(t)
			if err := step.run(t.Context(), ringOpsOn(model, true)); err != nil {
				t.Fatal(err)
			}
			for _, request := range model.requestLog() {
				if strings.HasPrefix(request, "get ") {
					t.Errorf("a whole-document get %q in a projected run", request)
				}
			}
		})
	}
}

// TestRingNoProjectRestoresWholeDocumentReads: -ring-no-project sends the old get, with the
// same number of requests.
func TestRingNoProjectRestoresWholeDocumentReads(t *testing.T) {
	for _, step := range ringSteps() {
		t.Run(step.name, func(t *testing.T) {
			model := ringFixture(t)
			ops := ringOpsOn(model, true)
			ops.wholeDocuments = true
			if err := step.run(t.Context(), ops); err != nil {
				t.Fatal(err)
			}
			if got, want := model.requestLog(), wholeRequests(step.want); !slices.Equal(got, want) {
				t.Errorf("requests %q, want %q", got, want)
			}
		})
	}
}

// TestRingProjectedAnswersEqualWholeAnswers: a scalar an operation needs but the projection
// leaves out shows here as a differing answer.
func TestRingProjectedAnswersEqualWholeAnswers(t *testing.T) {
	for _, serial := range []bool{true, false} {
		model := ringFixture(t)
		projected, whole := ringOpsOn(model, serial), ringOpsOn(model, serial)
		whole.wholeDocuments = true
		ctx := t.Context()

		for _, user := range []int{0, 2} {
			a, errA := projected.profile(ctx, user)
			b, errB := whole.profile(ctx, user)
			if errA != nil || errB != nil || !reflect.DeepEqual(a, b) {
				t.Errorf("profile of user %d: projected %+v %v, whole %+v %v", user, a, errA, b, errB)
			}
		}
		timelineA, errA := projected.profileTimeline(ctx, 0, 20)
		timelineB, errB := whole.profileTimeline(ctx, 0, 20)
		if errA != nil || errB != nil || len(timelineA) != 3 || !reflect.DeepEqual(timelineA, timelineB) {
			t.Errorf("profile timeline: projected %+v %v, whole %+v %v", timelineA, errA, timelineB, errB)
		}
		pageA, errA := projected.postPage(ctx, 0, 1)
		pageB, errB := whole.postPage(ctx, 0, 1)
		if errA != nil || errB != nil || !pageA.Found || pageA.Post.Text == "" || !reflect.DeepEqual(pageA, pageB) {
			t.Errorf("post page: projected %+v %v, whole %+v %v", pageA, errA, pageB, errB)
		}
		missingA, errA := projected.postPage(ctx, 0, 99)
		missingB, errB := whole.postPage(ctx, 0, 99)
		if errA != nil || errB != nil || missingA.Found || missingB.Found {
			t.Errorf("an absent post: projected %+v %v, whole %+v %v", missingA, errA, missingB, errB)
		}
		for _, ops := range []*ringOps{projected, whole} {
			if err := ops.uploadAvatar(ctx, 2, []byte("new-avatar")); err != nil {
				t.Fatal(err)
			}
		}
		afterA, errA := projected.profile(ctx, 2)
		if errA != nil || afterA.Name != "user 2" || afterA.Bio != "bio 2" || !afterA.Joined.Equal(ringFixtureEpoch) {
			t.Errorf("an upload through a projected read lost a field: %+v %v", afterA, errA)
		}
		if !bytes.Contains(model.doc(ringload.UserBucket, "2").scalars, []byte("photo-bytes")) {
			t.Errorf("an upload through a projected read lost the Photo")
		}
		avatarA, errA := projected.avatar(ctx, 2)
		avatarB, errB := whole.avatar(ctx, 2)
		if errA != nil || errB != nil || string(avatarA) != "new-avatar" || string(avatarA) != string(avatarB) {
			t.Errorf("avatar: projected %q %v, whole %q %v", avatarA, errA, avatarB, errB)
		}
	}
}

// TestRingProjectionsNameDeclaredScalars: each list names only scalars the shape declares, and
// the post list covers every field the post document type decodes.
func TestRingProjectionsNameDeclaredScalars(t *testing.T) {
	for name, test := range map[string]struct {
		bucket string
		fields []string
	}{
		"post":    {ringload.PostBucket, ringPostScalars},
		"profile": {ringload.UserBucket, ringProfileScalars},
		"avatar":  {ringload.UserBucket, ringAvatarScalars},
		"upload":  {ringload.UserBucket, ringUserScalars},
	} {
		for _, field := range test.fields {
			if !slices.Contains(ringload.ScalarFields(test.bucket), field) {
				t.Errorf("%s projection names %q, which %s does not declare as a scalar", name, field, test.bucket)
			}
		}
	}
	var decoded []string
	docType := reflect.TypeOf(ringPostDoc{})
	for index := range docType.NumField() {
		decoded = append(decoded, docType.Field(index).Tag.Get("bson"))
	}
	slices.Sort(decoded)
	covered := slices.Clone(ringPostScalars)
	slices.Sort(covered)
	if !slices.Equal(decoded, covered) {
		t.Errorf("ringPostDoc decodes %v, the post projection asks for %v", decoded, covered)
	}
}

// TestRingModelRefusesAStoreFieldInAProjection mirrors the server: a store field is not a scalar.
func TestRingModelRefusesAStoreFieldInAProjection(t *testing.T) {
	model := ringFixture(t)
	if _, err := model.connect().getFields(t.Context(), ringload.UserBucket, "0", []string{"Name", ringload.UserFeed}); err == nil {
		t.Fatal("a projection naming the Feed store was accepted")
	}
	raw, err := model.connect().getFields(t.Context(), ringload.UserBucket, "0", []string{"Name"})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := bson.Unmarshal(raw, &got); err != nil || len(got) != 1 || got["Name"] != "user 0" {
		t.Errorf("projection of Name = %v %v", got, err)
	}
}
