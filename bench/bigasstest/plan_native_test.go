package main

import (
	"fmt"
	"hash/fnv"
	"testing"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// streamHash hashes four virtual users' first 25,000 requests at scale 2: the planner's whole
// output except WriteOp.Refs, which the native variant added.
func streamHash(t *testing.T, access accessConfig) string {
	t.Helper()
	w, err := newWorld(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.configureAccess(access); err != nil {
		t.Fatal(err)
	}
	mix, err := newMix(benchLayouts, "")
	if err != nil {
		t.Fatal(err)
	}
	digest := fnv.New64a()
	record := func(format string, args ...any) {
		if _, err := fmt.Fprintf(digest, format, args...); err != nil {
			t.Fatalf("hashing the planner stream: %v", err)
		}
	}
	for vuser := range 4 {
		p := newPlanner(w, mix, 1, vuser, 42)
		for range 25000 {
			req := p.next()
			record("%s|%s|%v|", req.Layout, req.Kind, req.PlanErr)
			if r := req.Read; r != nil {
				record("R%+v", *r)
			}
			if op := req.Write; op != nil {
				record("W%s|%d|%v|", op.ID, op.Ordinal, op.PreRead)
				for _, d := range op.Documents {
					record("D%s/%s/%s;", d.Bucket, d.ID, canonBody(d.Fields))
				}
				for _, e := range op.Elements {
					record("E%s/%d;", canonElement(e), e.Seq)
				}
				record("%v", op.Counters)
				if op.Claim != nil {
					record("C%s", op.Claim.Run)
				}
			}
			record("\n")
		}
	}
	return fmt.Sprintf("%016x", digest.Sum64())
}

// The golden values are the hash of the stream the planner produced before WriteOp.Refs existed
// (taken at the parent commit of the native build): attaching the member's User document to a
// join draws nothing, so the request stream every engine is offered is the same bytes.
func TestPlannerStreamIsIdenticalWithRefs(t *testing.T) {
	for name, tc := range map[string]struct {
		access accessConfig
		want   string
	}{
		"uniform": {accessConfig{Mode: accessUniform, ZipfS: 1, PageDepth: 0.3}, "f76086a66129e360"},
		"zipf":    {accessConfig{Mode: accessZipf, ZipfS: 1, PageDepth: 0.3}, "51fe35319e2800e1"},
	} {
		if got := streamHash(t, tc.access); got != tc.want {
			t.Errorf("%s: the planner's stream hashes to %s, it was %s before Refs", name, got, tc.want)
		}
	}
}

// A join carries the User document of the joining member in Refs, and nothing else does.
func TestJoinCarriesTheMemberUserAsRef(t *testing.T) {
	w, err := newWorld(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	mix, err := newMix(benchLayouts, "")
	if err != nil {
		t.Fatal(err)
	}
	p := newPlanner(w, mix, 1, 0, 9)
	joins := 0
	for range 20000 {
		req := p.next()
		if req.Write == nil {
			continue
		}
		op := req.Write
		if op.Kind != WriteChatJoin {
			if len(op.Refs) != 0 {
				t.Fatalf("%s carries %d refs, only a join should", op.Kind, len(op.Refs))
			}
			continue
		}
		joins++
		if len(op.Refs) != 1 {
			t.Fatalf("a join has %d refs, want the member's User document", len(op.Refs))
		}
		user := bodyString(op.Elements[0], "user")
		want, _ := w.sets[model.Chat].doc("User", user)
		if ref := op.Refs[0]; ref.Bucket != "User" || ref.ID != user || canonBody(ref.Fields) != canonBody(want.Fields) {
			t.Fatalf("join of %s carries ref %s/%s %s, want the seeded User %s", user, ref.Bucket, ref.ID, canonBody(ref.Fields), canonBody(want.Fields))
		}
	}
	if joins == 0 {
		t.Fatal("no join was planned, the check proved nothing")
	}
}
