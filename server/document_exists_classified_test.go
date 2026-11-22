package server

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

func errorPayload(t *testing.T, code transport.ErrorCode) []byte {
	t.Helper()
	payload, err := bson.Marshal(ErrorResponse{Code: int(code), Message: "refused"})
	if err != nil {
		t.Fatalf("encoding the error frame: %v", err)
	}
	return payload
}

// TestDocumentExistsIsAConstraintRefusal is T-05b (document-placement spec
// §6.5 item 1): an insert refused because its _id_ exists is a constraint's
// answer, the same on every copy, so the owner records it as refused by
// constraint, a replica and a replay skip it, and the disagreement report
// names it. A replica that refuses with DOCUMENT_EXISTS an entry its owner
// refused under a unique index agrees with its owner: the outcome is
// compared, never the code (build doc BD-4).
func TestDocumentExistsIsAConstraintRefusal(t *testing.T) {
	payload := errorPayload(t, transport.ErrDocumentExists)
	if got := constraintRefusedUnder(payload); got != "an existing document with this `_id_`" {
		t.Fatalf("constraintRefusedUnder(DOCUMENT_EXISTS) = %q; want \"an existing document with this `_id_`\"", got)
	}
	refused := ReplResponse{Opcode: int32(transport.OpError), Payload: payload}
	if got := applyOutcomeOf(refused, nil); got != ownerOutcomeRefusedByConstraint {
		t.Fatalf("the outcome of a DOCUMENT_EXISTS refusal is %d; want refused by constraint (%d)", got, ownerOutcomeRefusedByConstraint)
	}

	recorder := &recordedLog{}
	c := &Cluster{logger: slog.New(recorder)}
	const counter = "hoarddb_replica_constraint_disagreements_total"
	before := gatheredCounter(t, counter)
	entry := ReplEntry{Op: int32(transport.OpPut), Bucket: "Users", Seq: 9, OwnerOutcome: ownerOutcomeRefusedByConstraint}
	c.noteReplicaDisagreement(entry, refused, nil)
	if moved := gatheredCounter(t, counter) - before; moved != 0 {
		t.Fatalf("a replica refusing with DOCUMENT_EXISTS what its owner refused under a unique index moved the disagreement counter by %v", moved)
	}
	if loud := recorder.matching(slog.LevelError, "Users"); len(loud) != 0 {
		t.Fatalf("it was logged as a disagreement: %+v", loud)
	}

	// An owed entry the replica refused: the owner has not applied it yet, and
	// the report says so instead of "the owner applied".
	entry.OwnerOutcome = ownerOutcomeOwed
	c.noteReplicaDisagreement(entry, refused, nil)
	loud := recorder.matching(slog.LevelError, "Users")
	if len(loud) != 1 || !strings.Contains(loud[0].msg, "the owner has not applied the write at sequence 9 yet (owed)") ||
		strings.Contains(loud[0].msg, "the owner applied") {
		t.Fatalf("the report of an owed entry this replica refused is %+v; want one line saying the owner has not applied it yet", loud)
	}
}

// TestOwnerDecidedReachesApplyInfo is the plumbing of the owner's verdict
// (build doc step 4, BD-4's table): a replica's apply and a replay of a
// replica-origin entry carry the owner's outcome into store.ApplyInfo as
// OwnerDecided. Applied is applied, refused by constraint is refused, and an
// owed entry, an unknown outcome and one an earlier build did not log are no
// verdict: the replica decides. NoteHeldID is set only for an applied verdict,
// and reports into the apply's outcome.
func TestOwnerDecidedReachesApplyInfo(t *testing.T) {
	s := &Server{}
	cases := []struct {
		name    string
		outcome int32
		want    store.OwnerDecision
	}{
		{"applied", ownerOutcomeApplied, store.OwnerApplied},
		{"refused by constraint", ownerOutcomeRefusedByConstraint, store.OwnerRefused},
		{"owed", ownerOutcomeOwed, store.OwnerUndecided},
		{"unknown", ownerOutcomeUnknown, store.OwnerUndecided},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			live, outcome := withApplyOutcome(withOwnerDecided(context.Background(), ownerDecisionOf(tc.outcome)))
			replayed := withLoggedOwnerDecision(context.Background(),
				storage.WALOrigin{Kind: storage.WALOriginReplica, OwnerOutcome: byte(tc.outcome)})
			for leg, ctx := range map[string]context.Context{"a replica's apply": live, "a replay": replayed} {
				if got := applyInfoFrom(ctx, s).OwnerDecided; got != tc.want {
					t.Errorf("%s of an entry whose owner outcome is %s carries OwnerDecided %d; want %d", leg, tc.name, got, tc.want)
				}
			}
			info := applyInfoFrom(live, s)
			if (info.NoteHeldID != nil) != (tc.want == store.OwnerApplied) {
				t.Fatalf("NoteHeldID set = %v for owner outcome %s; want it set only for an applied verdict", info.NoteHeldID != nil, tc.name)
			}
			if info.NoteHeldID != nil {
				info.NoteHeldID()
				if !outcome.heldUnknownID {
					t.Fatal("NoteHeldID did not reach the apply's outcome")
				}
			}
		})
	}
	t.Run("absent", func(t *testing.T) {
		if got := applyInfoFrom(context.Background(), s).OwnerDecided; got != store.OwnerUndecided {
			t.Fatalf("an apply with no verdict carries OwnerDecided %d; want none", got)
		}
	})
	t.Run("not logged", func(t *testing.T) {
		ctx := withLoggedOwnerDecision(context.Background(), storage.WALOrigin{Kind: storage.WALOriginReplica,
			OwnerOutcome: byte(ownerOutcomeRefusedByConstraint), OutcomeNotLogged: true})
		if got := applyInfoFrom(ctx, s).OwnerDecided; got != store.OwnerUndecided {
			t.Fatalf("a replay of an entry an earlier build logged carries OwnerDecided %d; want none", got)
		}
	})
}
