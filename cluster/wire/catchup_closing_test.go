package wire

import (
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// quietSource is a source whose pair was last written in an earlier epoch than the committed
// one (u32Epoch): it holds records up to the given position and has appended nothing since.
func quietSource(t *testing.T, applied cluster.Position) *u32Node {
	t.Helper()
	source := newU32Node(t, "source", nil)
	source.pairs.With(u32Slot, u32DB, func(pair *cluster.Pair) { pair.AppliedPos = applied })
	return source
}

// TestAClosingRoundFromAnEpochOpenerAnswersCaughtUp: a seed whose snapshot was cut in an
// earlier epoch asks its closing round from (committed epoch, 1), and a source that wrote
// nothing in the committed epoch has nothing to hand over: caught up at its own tail, for an
// empty pair and for one last written in an earlier epoch alike.
// [BREAK: drop the pinHolder case in serveCatchUp]
func TestAClosingRoundFromAnEpochOpenerAnswersCaughtUp(t *testing.T) {
	for _, tc := range []struct {
		name string
		tail cluster.Position
	}{
		{"an empty pair", cluster.Position{}},
		{"a pair quiet since an earlier epoch", u32Pos(u32Epoch-2, 9)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := quietSource(t, tc.tail)
			chunk, err := source.rep.ServePinnedCatchUp(context.Background(), CatchUpRequest{Slot: u32Slot, DB: u32DB, Node: "joiner", From: u32Pos(u32Epoch, 1)})
			if err != nil {
				t.Fatalf("the closing round of a pin holder: %v", err)
			}
			if chunk.Divergent || !current(chunk) || chunk.LastPosition != tc.tail || len(chunk.Records) != 0 {
				t.Fatalf("answer %+v, want an empty current chunk at the source's tail %s", chunk, tc.tail)
			}
		})
	}
}

// TestAClosingRoundOfAnOrdinaryCopyStillReadsTheBoundaryAsATail: a requester that is not a pin
// holder and asks from the same epoch opener keeps today's answer, divergent.
// [BREAK: ServeCatchUp passes pinHolder true]
func TestAClosingRoundOfAnOrdinaryCopyStillReadsTheBoundaryAsATail(t *testing.T) {
	source := quietSource(t, u32Pos(u32Epoch-2, 9))
	chunk, err := source.rep.ServeCatchUp(context.Background(), CatchUpRequest{Slot: u32Slot, DB: u32DB, Node: "copy", From: u32Pos(u32Epoch, 1)})
	if err != nil {
		t.Fatalf("ServeCatchUp: %v", err)
	}
	if !chunk.Divergent {
		t.Fatalf("answer %+v, want divergent for a requester that holds no pin", chunk)
	}
}

// TestAClosingRoundHoldingATailNobodyOwnsIsDivergent: a pin holder whose applied position is
// past the source's tail in the same epoch holds a tail nobody owns, pin or not.
// [BREAK: answer caught up for every pin holder with nothing to read]
func TestAClosingRoundHoldingATailNobodyOwnsIsDivergent(t *testing.T) {
	source := newU32Node(t, "source", nil)
	source.ownRun(u32Epoch, 1, 3)
	chunk, err := source.rep.ServePinnedCatchUp(context.Background(), CatchUpRequest{Slot: u32Slot, DB: u32DB, Node: "joiner", From: u32Pos(u32Epoch, 6)})
	if err != nil {
		t.Fatalf("ServePinnedCatchUp: %v", err)
	}
	if !chunk.Divergent {
		t.Fatalf("answer %+v, want divergent for a receiver at 7:5 against a source at 7:3", chunk)
	}
}

// TestANodeAnswersAPinHoldersCatchUpAsItsClosingRound: the node picks the pinned answer for
// the registered receiver of a live pin and the plain one for any other requester.
// [BREAK: answerCatchUp always serves the plain answer]
func TestANodeAnswersAPinHoldersCatchUpAsItsClosingRound(t *testing.T) {
	c := newCatchUpPinNode(t)
	c.pin("j", t2Slot, t2DB, 1)
	req := CatchUpRequest{Slot: t2Slot, DB: t2DB, Node: "j", From: u32Pos(u32Epoch, 1)}
	chunk, err := c.node.answerCatchUp(t.Context(), req)
	if err != nil || chunk.Divergent || !current(chunk) {
		t.Fatalf("the pin holder's closing round: %+v, %v, want current", chunk, err)
	}
	req.Node = "k"
	chunk, err = c.node.answerCatchUp(t.Context(), req)
	if err != nil || !chunk.Divergent {
		t.Fatalf("a requester with no pin: %+v, %v, want divergent", chunk, err)
	}
}
