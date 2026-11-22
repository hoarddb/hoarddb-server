package wire

import (
	"fmt"
	"testing"
)

// getManyAnswerText is a found answer's document bytes, or the error text, so two answers can be
// compared whole.
func getManyAnswerText(answer RingAnswer) string {
	switch {
	case answer.Error != nil:
		return "error " + answer.Error.Message
	case answer.Reply == nil || len(answer.Reply.Elements) == 0:
		return "no document"
	}
	return fmt.Sprintf("%x", answer.Reply.Elements[0])
}

// TestGetManyGetAndIDOnlyGetFieldsOfOneDocumentKeepTheirOwnAnswers: a plain get (all fields, a nil
// projection) and a get_fields naming only _id_ (an empty, non-nil projection) of the same
// document are two reads, in either order, and each answers what it answers alone.
func TestGetManyGetAndIDOnlyGetFieldsOfOneDocumentKeepTheirOwnAnswers(t *testing.T) {
	world := newGetManyWorld(t, u70AllowAll{})
	whole := getManyAnswerText(world.serve(getManyRead(t, world.near, "u0")).Answers[0])
	idOnly := getManyAnswerText(world.serve(getManyRead(t, world.near, "u0", "_id_")).Answers[0])
	if whole == idOnly {
		t.Fatalf("the world's document has nothing besides _id_, so the two shapes cannot be told apart")
	}
	for _, order := range []struct {
		name  string
		plain int
	}{{"get then get_fields", 0}, {"get_fields then get", 1}} {
		t.Run(order.name, func(t *testing.T) {
			reads := []RingRead{getManyRead(t, world.near, "u0"), getManyRead(t, world.near, "u0", "_id_")}
			if order.plain == 1 {
				reads[0], reads[1] = reads[1], reads[0]
			}
			reply := world.serve(reads...)
			want := []string{whole, idOnly}
			if order.plain == 1 {
				want[0], want[1] = want[1], want[0]
			}
			for i := range want {
				if got := getManyAnswerText(reply.Answers[i]); got != want[i] {
					t.Errorf("key %d answered %.60q, want what it answers alone: %.60q", i, got, want[i])
				}
			}
		})
	}
}

// TestGroupByOwnerKeepsAWholeReadAndAnIDOnlyProjectionApart: groupByOwner dedupes by the same key,
// so a nil projection and an empty one of one document are two refs.
func TestGroupByOwnerKeepsAWholeReadAndAnIDOnlyProjectionApart(t *testing.T) {
	world := newGetManyWorld(t, u70AllowAll{})
	ref := expandRef{ref: world.near, encID: u66EncID(t, "u0")}
	idOnly := ref
	idOnly.project = []uint16{}
	for _, refs := range [][]expandRef{{ref, idOnly}, {idOnly, ref}} {
		groups := groupByOwner(refs, func(bucketRef, uint16) string { return "a" })
		if len(groups) != 1 || len(groups[0].refs) != 2 {
			t.Fatalf("groupByOwner kept %d refs of 2 distinct reads", len(groups[0].refs))
		}
	}
}
