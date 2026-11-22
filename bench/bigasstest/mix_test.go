package main

import (
	"math"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

func TestMixRatios(t *testing.T) {
	mix, err := newMix(benchLayouts, "")
	if err != nil {
		t.Fatal(err)
	}
	if mix.Profile().Name != "R" {
		t.Fatalf("the default profile is %q, want R", mix.Profile().Name)
	}
	checkMixRatios(t, mix)
	// Spot values: social reads are 90% of social requests and the feed is 10 of its 20 read weight.
	want := mix.shares()
	if got, w := want["social.feed"], 0.25*0.90*0.5; math.Abs(got-w) > 1e-9 {
		t.Errorf("social.feed share %v, want %v", got, w)
	}
	if got, w := want["chat.user_rooms"], 0.25*0.60*0.30; math.Abs(got-w) > 1e-9 {
		t.Errorf("chat.user_rooms share %v, want %v", got, w)
	}
	if got, w := want["ledger.invoices_page"], 0.25*0.70*0.30; math.Abs(got-w) > 1e-9 {
		t.Errorf("ledger.invoices_page share %v, want %v", got, w)
	}
	if got, w := want["agent.session_page"], 0.25*0.50*0.50; math.Abs(got-w) > 1e-9 {
		t.Errorf("agent.session_page share %v, want %v", got, w)
	}
}

// TestProfileSharesAreTheRuling pins the owner-ruled per-layout read shares of both profiles.
func TestProfileSharesAreTheRuling(t *testing.T) {
	r, err := profileByName("R")
	if err != nil {
		t.Fatal(err)
	}
	s, err := profileByName("S")
	if err != nil {
		t.Fatal(err)
	}
	wantR := map[model.Layout]float64{model.Social: 0.90, model.Chat: 0.60, model.Ledger: 0.70, model.Agent: 0.50}
	wantS := map[model.Layout]float64{model.Social: 0.20, model.Chat: 0.30, model.Ledger: 0.30, model.Agent: 0.10}
	for _, layout := range benchLayouts {
		if r.ReadShare[layout] != wantR[layout] || s.ReadShare[layout] != wantS[layout] {
			t.Errorf("%s: R %v S %v", layout, r.ReadShare[layout], s.ReadShare[layout])
		}
	}
	if got := r.pooledReads(benchLayouts); math.Abs(got-0.675) > 1e-9 {
		t.Errorf("profile R pools to %v reads, want 0.675", got)
	}
	if _, err := profileByName("Q"); err == nil || !strings.Contains(err.Error(), "R") || !strings.Contains(err.Error(), "S") {
		t.Errorf("an unknown profile must name the choices, got %v", err)
	}
}

func TestMixRatiosProfileS(t *testing.T) {
	prof, _ := profileByName("S")
	mix, err := newMixProfile(benchLayouts, "", prof)
	if err != nil {
		t.Fatal(err)
	}
	checkMixRatios(t, mix)
}

// checkMixRatios draws 10^6 requests and compares the layout, read/write and per-operation shares
// with what the mix says it offers.
func checkMixRatios(t *testing.T, mix *Mix) {
	t.Helper()
	rng := rand.New(rand.NewPCG(1, 2))
	const draws = 1_000_000
	byOp := map[string]float64{}
	byLayout := map[model.Layout]float64{}
	readsByLayout := map[model.Layout]float64{}
	var writes float64
	for range draws {
		entry := mix.draw(rng)
		byOp[entry.Kind]++
		byLayout[entry.Layout]++
		if entry.Write {
			writes++
		} else {
			readsByLayout[entry.Layout]++
		}
	}
	for _, layout := range benchLayouts {
		if got := byLayout[layout] / draws; math.Abs(got-0.25) > 0.005 {
			t.Errorf("layout %s share %.4f, want 0.25", layout, got)
		}
		// 0.5 percentage points on each layout's read share, measured within the layout.
		if got, want := readsByLayout[layout]/byLayout[layout], mix.Profile().ReadShare[layout]; math.Abs(got-want) > 0.005 {
			t.Errorf("layout %s read share %.4f, want %.2f", layout, got, want)
		}
	}
	if got, want := writes/draws, 1-mix.Profile().pooledReads(benchLayouts); math.Abs(got-want) > 0.005 {
		t.Errorf("write share %.4f, want %.4f", got, want)
	}
	// 0.05 percentage points per operation, widened to four standard deviations of the
	// binomial where that is larger (a share of 0.24 over 10^6 draws wobbles by 0.04 points).
	want := mix.shares()
	var total float64
	for kind, share := range want {
		total += share
		tolerance := math.Max(0.0005, 4*math.Sqrt(share*(1-share)/draws))
		if got := byOp[kind] / draws; math.Abs(got-share) > tolerance {
			t.Errorf("%s share %.5f, want %.5f", kind, got, share)
		}
	}
	if math.Abs(total-1) > 1e-9 {
		t.Errorf("shares add to %v", total)
	}
}

// TestMixDrawsExactlyThreeBeforeTheEntity pins the PCG discipline: layout, read or write, and
// operation are three draws, so the entity draw is the fourth on every engine and profile.
func TestMixDrawsExactlyThreeBeforeTheEntity(t *testing.T) {
	for _, name := range []string{"R", "S"} {
		prof, _ := profileByName(name)
		mix, _ := newMixProfile(benchLayouts, "", prof)
		a, b := rand.New(rand.NewPCG(9, 9)), rand.New(rand.NewPCG(9, 9))
		for i := range 2000 {
			mix.draw(a)
			b.IntN(4)
			b.Float64()
			b.Float64()
			if x, y := a.Uint64(), b.Uint64(); x != y {
				t.Fatalf("profile %s draw %d consumed a different number of values than three", name, i)
			}
		}
	}
}

func TestMixOnlyRestrictsToOneOperation(t *testing.T) {
	mix, err := newMix(benchLayouts, "social.follow_scan")
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(1, 2))
	for range 100 {
		if entry := mix.draw(rng); entry.Kind != "social.follow_scan" || entry.Write {
			t.Fatalf("drew %+v", entry)
		}
	}
	if _, err := newMix(benchLayouts, "nope"); err == nil || !strings.Contains(err.Error(), "social.feed") {
		t.Errorf("an unknown -op must list the choices, got %v", err)
	}
}

func testWorld(t *testing.T) *world {
	t.Helper()
	w, err := newWorld(7, 1)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestPlannerIsEngineIndependent(t *testing.T) {
	w := testWorld(t)
	mix, _ := newMix(benchLayouts, "")
	a, b, other := newPlanner(w, mix, 1, 3, 99), newPlanner(w, mix, 1, 3, 99), newPlanner(w, mix, 1, 4, 99)
	same := true
	for i := range 20000 {
		ra, rb, ro := a.next(), b.next(), other.next()
		if !reflect.DeepEqual(ra, rb) {
			t.Fatalf("request %d differs between two identical planners", i)
		}
		if !reflect.DeepEqual(ra, ro) {
			same = false
		}
	}
	if same {
		t.Error("two virtual users must draw different streams")
	}
}

func TestWritesAreWellFormed(t *testing.T) {
	w := testWorld(t)
	mix, _ := newMix(benchLayouts, "")
	p := newPlanner(w, mix, 2, 5, 1)
	seen := map[WriteKind]int{}
	for range 400000 {
		req := p.next()
		if req.Write == nil {
			continue
		}
		op := req.Write
		seen[op.Kind]++
		if !strings.HasPrefix(op.ID, "w2.5.") {
			t.Fatalf("write id %q", op.ID)
		}
		if len(op.Elements) == 0 && len(op.Documents) == 0 {
			t.Fatalf("%s writes nothing", op.Kind)
		}
		for _, e := range op.Elements {
			kind, err := KindOf(op.Layout, e.Bucket, e.Store)
			if err != nil {
				t.Fatalf("%s: %v", op.Kind, err)
			}
			if kind.Keyed() == (e.Key == "") {
				t.Fatalf("%s: element %s/%s key %q does not fit a %v store", op.Kind, e.Bucket, e.Store, e.Key, kind)
			}
			if e.Seq != SeqBase+op.Ordinal {
				t.Fatalf("%s: seq %d", op.Kind, e.Seq)
			}
			if _, ok := w.sets[op.Layout].doc(e.Bucket, e.Owner); !ok {
				t.Fatalf("%s writes into unseeded owner %s/%s", op.Kind, e.Bucket, e.Owner)
			}
		}
		for _, c := range op.Counters {
			if c.Element >= len(op.Elements) {
				t.Fatalf("%s counter points at element %d", op.Kind, c.Element)
			}
		}
		if op.Kind == WriteAgentMemory {
			if vec, _ := op.Elements[0].Body[1].Value.([]byte); len(vec) != 4*model.VectorLen {
				t.Fatalf("memory vector is %d bytes", len(vec))
			}
			// The seeded SQL schemas keep users.memory_n; without a delta it drifts from the
			// real count and a SQL engine would do less work than the entity layouts.
			var counted bool
			for _, c := range op.Counters {
				counted = counted || (c.Bucket == "User" && c.Field == "memory_n" && c.Delta == 1 && c.Element == 0)
			}
			if !counted {
				t.Fatalf("agent.memory carries no memory_n delta: %+v", op.Counters)
			}
		}
		if op.Kind == WriteLedgerExpense && op.Elements[0].Key[:10] <= "2000000000" {
			t.Fatalf("expense key %q must sort after every seeded key", op.Elements[0].Key)
		}
	}
	for _, kind := range []WriteKind{WriteSocialPost, WriteSocialComment, WriteSocialLike, WriteSocialFollow, WriteSocialGroupPost, WriteSocialSettings,
		WriteChatMessage, WriteChatJoin, WriteLedgerExpense, WriteLedgerInvoice, WriteAgentMessage, WriteAgentTask, WriteAgentScratch, WriteAgentMemory} {
		if seen[kind] == 0 {
			t.Errorf("write kind %s never drawn", kind)
		}
	}
}

func TestContentIsAFunctionOfSeedAndID(t *testing.T) {
	a, b, c := newContentGen(1), newContentGen(1), newContentGen(2)
	if a.sentence("w1.1.1", "text", 8) != b.sentence("w1.1.1", "text", 8) {
		t.Error("same seed and id must give the same text")
	}
	if a.sentence("w1.1.1", "text", 8) == c.sentence("w1.1.1", "text", 8) {
		t.Error("a different seed must change the text")
	}
	if a.sentence("w1.1.1", "text", 8) == a.sentence("w1.1.2", "text", 8) {
		t.Error("a different id must change the text")
	}
	if len(a.vector("w1.1.1")) != 256 {
		t.Error("a vector is 64 float32 values")
	}
}
