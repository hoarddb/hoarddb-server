package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// TestContractRing runs the shared contract against a real hoarddb-server. It skips unless
// HOARDDB_ADDR is set. The server is reached the way the scenarios binary reaches it:
// HOARDDB_ROOT_PASSWORD is the admin password, and HOARDDB_FINGERPRINT pins the certificate
// (or HOARDDB_INSECURE=1 skips the check for a server started on this machine).
func TestContractRing(t *testing.T) {
	addr := os.Getenv("HOARDDB_ADDR")
	if addr == "" {
		t.Skip("HOARDDB_ADDR is not set; start hoarddb-server and set HOARDDB_ADDR, HOARDDB_ROOT_PASSWORD and HOARDDB_FINGERPRINT (or HOARDDB_INSECURE=1) to run the live ring contract")
	}
	params := map[string]string{}
	seedArgs := []string{"-addr", addr}
	if fingerprint := os.Getenv("HOARDDB_FINGERPRINT"); fingerprint != "" {
		params["fingerprint"] = fingerprint
		seedArgs = append(seedArgs, "-fingerprint", fingerprint)
	} else if os.Getenv("HOARDDB_INSECURE") == "1" {
		params["insecure"] = "true"
		seedArgs = append(seedArgs, "-insecure")
	}
	// Both read forms must satisfy the contract: one Get per id, and get-many.
	for _, multiget := range []string{"off", "on"} {
		t.Run("multiget="+multiget, func(t *testing.T) {
			withMode := map[string]string{"multiget": multiget}
			for key, value := range params {
				withMode[key] = value
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			engine, err := openEngine(ctx, Config{Engine: "ring", URI: addr, Params: withMode, VUsers: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = engine.Close() }()
			// The ring has no drop database, so every run seeds databases of its own.
			database := fmt.Sprintf("bat%d%s", time.Now().UnixNano()%1_000_000_000, multiget)
			runContract(t, contractTarget{Engine: engine, Name: "ring", Database: database, SeedArgs: seedArgs})
		})
	}
}

// TestRingEmptyClaimAndReplacingWritesAreApplied settles two questions of the contract on the
// real server: a Claim on an empty queue, and a Put or Upsert over an existing key, both come
// back "applied" and so are not errors. It skips without HOARDDB_ADDR.
func TestRingEmptyClaimAndReplacingWritesAreApplied(t *testing.T) {
	addr := os.Getenv("HOARDDB_ADDR")
	if addr == "" {
		t.Skip("HOARDDB_ADDR is not set; see TestContractRing")
	}
	params := map[string]string{}
	seedArgs := []string{"-addr", addr}
	if fingerprint := os.Getenv("HOARDDB_FINGERPRINT"); fingerprint != "" {
		params["fingerprint"] = fingerprint
		seedArgs = append(seedArgs, "-fingerprint", fingerprint)
	} else if os.Getenv("HOARDDB_INSECURE") == "1" {
		params["insecure"] = "true"
		seedArgs = append(seedArgs, "-insecure")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	engine, err := openEngine(ctx, Config{Engine: "ring", URI: addr, Params: params, VUsers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = engine.Close() }()
	database := fmt.Sprintf("batq%d", time.Now().UnixNano()%1_000_000_000)
	target := contractTarget{Engine: engine, Name: "ring", Database: database, SeedArgs: seedArgs}
	if err := target.seed(ctx, model.Agent); err != nil {
		t.Fatal(err)
	}
	session, err := engine.Session(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	ops, err := session.Layout(model.Agent, layoutDatabase(database, model.Agent))
	if err != nil {
		t.Fatal(err)
	}
	world, err := newWorld(contractSeed, contractScale)
	if err != nil {
		t.Fatal(err)
	}
	run := world.sets[model.Agent].ids("Run")[0]
	claim := WriteOp{Layout: model.Agent, Kind: WriteAgentTask, ID: "x", Ordinal: 1, Claim: &ClaimRef{Run: run}}
	for range len(world.sets[model.Agent].elements(StoreRef{"Run", run, "Tasks"})) {
		if reply, err := ops.Write(ctx, claim); err != nil || len(reply.Elements) != 1 {
			t.Fatalf("claim of a seeded task: %v, %d elements", err, len(reply.Elements))
		}
	}
	reply, err := ops.Write(ctx, claim)
	if err != nil || len(reply.Elements) != 0 {
		t.Fatalf("claim on an empty queue: %v, %d elements; it must apply and return nothing", err, len(reply.Elements))
	}
	scratch := world.sets[model.Agent].elements(StoreRef{"Run", run, "Scratch"})
	if len(scratch) == 0 {
		t.Fatal("the seeded run has no scratch key to replace")
	}
	replace := WriteOp{Layout: model.Agent, Kind: WriteAgentScratch, ID: "y", Ordinal: 2, Elements: []model.Element{{
		Bucket: "Run", Owner: run, Store: "Scratch", Key: scratch[0].Key, Body: []model.Field{{Name: "value", Value: "replaced"}}}}}
	for attempt := range 2 {
		if _, err := ops.Write(ctx, replace); err != nil {
			t.Fatalf("put %d over an existing key: %v", attempt+1, err)
		}
	}
}

// TestRingBatchedWritesStoreTheSameData applies the same planned writes of every kind to two
// freshly seeded copies of the layouts, one through an unbatched session and one through a
// batched one, and requires the same stored data, through the contract's read-back, and fewer
// round trips on the batched side for every write that fans out. It skips without HOARDDB_ADDR.
func TestRingBatchedWritesStoreTheSameData(t *testing.T) {
	addr := os.Getenv("HOARDDB_ADDR")
	if addr == "" {
		t.Skip("HOARDDB_ADDR is not set; see TestContractRing")
	}
	params := map[string]string{}
	seedArgs := []string{"-addr", addr}
	if fingerprint := os.Getenv("HOARDDB_FINGERPRINT"); fingerprint != "" {
		params["fingerprint"] = fingerprint
		seedArgs = append(seedArgs, "-fingerprint", fingerprint)
	} else if os.Getenv("HOARDDB_INSECURE") == "1" {
		params["insecure"] = "true"
		seedArgs = append(seedArgs, "-insecure")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	stamp := time.Now().UnixNano() % 1_000_000_000
	type side struct {
		database string
		ops      map[model.Layout]LayoutOps
		trips    map[WriteKind]int
	}
	open := func(name string, batch string) *side {
		withBatch := map[string]string{"batch": batch}
		for key, value := range params {
			withBatch[key] = value
		}
		engine, err := openEngine(ctx, Config{Engine: "ring", URI: addr, Params: withBatch, VUsers: 1})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = engine.Close() })
		settings, err := engine.Settings(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if want := map[string]string{"0": "off", "1": "on"}[batch]; settings["batch"] != want {
			t.Fatalf("Settings batch = %q, want %q", settings["batch"], want)
		}
		target := contractTarget{Engine: engine, Name: "ring", Database: fmt.Sprintf("%s%d", name, stamp), SeedArgs: seedArgs}
		session, err := engine.Session(ctx, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = session.Close() })
		s := &side{database: target.Database, ops: map[model.Layout]LayoutOps{}, trips: map[WriteKind]int{}}
		for _, layout := range benchLayouts {
			if err := target.seed(ctx, layout); err != nil {
				t.Fatal(err)
			}
			ops, err := session.Layout(layout, layoutDatabase(target.Database, layout))
			if err != nil {
				t.Fatal(err)
			}
			s.ops[layout] = ops
		}
		return s
	}
	plain, batched := open("batoff", "0"), open("baton", "1")
	world, err := newWorld(contractSeed, contractScale)
	if err != nil {
		t.Fatal(err)
	}
	touched := map[model.Layout]map[StoreRef]bool{}
	documents := map[model.Layout]map[[2]string]bool{}
	fanned := map[WriteKind]bool{}
	for _, layout := range benchLayouts {
		touched[layout], documents[layout] = map[StoreRef]bool{}, map[[2]string]bool{}
		for _, kind := range contractWrites[layout] {
			mix, err := newMix(benchLayouts, string(kind))
			if err != nil {
				t.Fatal(err)
			}
			planner := newPlanner(world, mix, 1, 1, 42)
			for range contractWritesPerKind {
				req := planner.next()
				if req.PlanErr != nil || req.Write == nil {
					t.Fatalf("plan %s: %v", kind, req.PlanErr)
				}
				op := *req.Write
				var replies [2]Reply
				for i, s := range []*side{plain, batched} {
					reply, err := s.ops[layout].Write(ctx, op)
					if err != nil {
						t.Fatalf("%s write %s (batched=%v): %v", layout, kind, i == 1, err)
					}
					replies[i] = reply
					s.trips[op.Kind] += reply.RoundTrips
				}
				if replies[0].RoundTrips > replies[1].RoundTrips {
					fanned[op.Kind] = true
				}
				if len(op.Documents)+len(op.Elements) > 1 && replies[1].RoundTrips >= replies[0].RoundTrips {
					t.Errorf("%s fans out to %d writes but took %d round trips batched against %d unbatched", op.Kind, len(op.Documents)+len(op.Elements), replies[1].RoundTrips, replies[0].RoundTrips)
				}
				for _, element := range op.Elements {
					touched[layout][StoreRef{element.Bucket, element.Owner, element.Store}] = true
				}
				for _, doc := range op.Documents {
					documents[layout][[2]string{doc.Bucket, doc.ID}] = true
				}
			}
		}
	}
	if !fanned[WriteSocialPost] {
		t.Error("no social.post fanned out into a batch: the check proved nothing")
	}
	render := func(elements []model.Element) []string {
		lines := make([]string, len(elements))
		for i, element := range elements {
			lines[i] = fmt.Sprintf("%s|%v", element.Key, element.Body)
		}
		return lines
	}
	for layout, refs := range touched {
		for ref := range refs {
			kind, err := KindOf(layout, ref.Bucket, ref.Store)
			if err != nil {
				t.Fatal(err)
			}
			var got [2][]string
			for i, s := range []*side{plain, batched} {
				elements, err := s.ops[layout].(StoreReader).ReadStore(ctx, ref)
				if err != nil {
					t.Fatal(err)
				}
				got[i] = render(elements)
				if kind.Keyed() {
					sort.Strings(got[i])
				}
			}
			if !slices.Equal(got[0], got[1]) {
				t.Errorf("%s %s/%s/%s differs: unbatched %d elements, batched %d", layout, ref.Bucket, ref.Owner, ref.Store, len(got[0]), len(got[1]))
			}
		}
		for key := range documents[layout] {
			var got [2]model.Document
			for i, s := range []*side{plain, batched} {
				doc, err := s.ops[layout].(*ringOps).getDocument(ctx, key[0], key[1])
				if err != nil {
					t.Fatalf("reading %s %s: %v", key[0], key[1], err)
				}
				got[i] = doc
			}
			if fmt.Sprint(got[0]) != fmt.Sprint(got[1]) {
				t.Errorf("%s document %s/%s differs: %v against %v", layout, key[0], key[1], got[0], got[1])
			}
		}
	}
}
