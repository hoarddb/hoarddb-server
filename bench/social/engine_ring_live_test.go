package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/hoarddb/hoarddb-bson/bson"
	"io"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/bench/social/ringload"
	serverconfig "github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/server"
)

const ringLivePassword = "ring-engine-test-password"

// startRingServer runs a hoarddb-server in this process on a loopback port the kernel chose
// and returns the address its drivers listen on.
func startRingServer(t *testing.T) string {
	t.Helper()
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := reserved.Addr().String()
	if err := reserved.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := serverconfig.DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = addr
	service, err := server.NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.SeedRootUser("admin", ringLivePassword); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Stop() })
	return addr
}

func ringLiveConfig(addr, mix string) *config {
	cfg := mixTestConfig(mix, "social")
	cfg.Engine, cfg.Addr, cfg.Addrs, cfg.Password = engineRing, addr, []string{addr}, ringLivePassword
	cfg.Placement, cfg.Fanout = "h1", fanoutSingle
	cfg.SampleUsers = 30
	return cfg
}

// seededRingEngine seeds the tiny dataset into a fresh in-process node through the engine's own
// setup and seed, and returns the engine with the graph it was seeded from.
func seededRingEngine(t *testing.T) (*ringEngine, *socialGraph, string) {
	t.Helper()
	addr := startRingServer(t)
	cfg := ringLiveConfig(addr, "")
	engine := newRingEngine(cfg)
	engine.progressOut = io.Discard
	graph := newSocialGraph(tinySpec, datasetSeed)
	var out bytes.Buffer
	if err := seedOn(t.Context(), cfg, engine, graph, &out); err != nil {
		t.Fatalf("seeding the tiny dataset through the ring engine: %v", err)
	}
	return engine, graph, addr
}

// legacyWorldOnTheDatasetSeed is the legacy engine over its fake server, seeded from the graph
// the ring engine seeds from (the other fake worlds use a private seed).
func legacyWorldOnTheDatasetSeed(t *testing.T) *socialWorld {
	t.Helper()
	world := newFakeWorld(t)
	engine := world.engine
	engine.dial = nil
	fake := newFakeServer()
	engine.dial = func(string) (wireCaller, func(), error) { return fake.dial(nil, nil), func() {}, nil }
	world.graph = newSocialGraph(tinySpec, datasetSeed)
	if _, err := engine.setupSocial(t.Context(), tinySpec); err != nil {
		t.Fatal(err)
	}
	if err := engine.seedSocial(t.Context(), world.graph, 4); err != nil {
		t.Fatal(err)
	}
	return world
}

// TestRingLoadSeedIsTheHarnessSeed: the seeder the ring engine calls generates from the
// harness' spec and seed, so both sides describe one dataset.
func TestRingLoadSeedIsTheHarnessSeed(t *testing.T) {
	if ringload.DatasetSeed != datasetSeed {
		t.Fatalf("ringload seeds the graph with %d, the harness with %d", ringload.DatasetSeed, datasetSeed)
	}
	for _, spec := range datasetSpecs {
		theirs, err := ringload.LookupDataset(spec.Name)
		if err != nil || theirs != ringload.Spec(spec) {
			t.Errorf("dataset %s differs between the harness and ringload: %+v and %+v (%v)", spec.Name, spec, theirs, err)
		}
	}
}

// TestRingParityHashMatchesTheLegacyEngine is the dataset parity check (spec P4) across
// layouts: the same tiny dataset in the entity-centric layout over a real node and in the
// legacy layout over its fake server hash to the same canonical form, which is also the form
// the generator says it must have.
func TestRingParityHashMatchesTheLegacyEngine(t *testing.T) {
	ctx := t.Context()
	engine, graph, _ := seededRingEngine(t)
	ranks := sampleUsers(tinySpec.Users, 30, datasetSeed)

	ringDigest, err := readSample(ctx, engine, ranks, 4)
	if err != nil {
		t.Fatal(err)
	}
	legacy := legacyWorldOnTheDatasetSeed(t)
	legacyDigest, err := readSample(ctx, legacy.social, ranks, 4)
	if err != nil {
		t.Fatal(err)
	}
	if ringDigest.hash != legacyDigest.hash {
		t.Fatalf("the sample hash differs between layouts: ring %s, legacy %s; users that differ: %v", ringDigest.hash, legacyDigest.hash, differingUsers(ringDigest, legacyDigest, 10))
	}
	expected, err := canonicalHash(graphCanonical(graph, ranks))
	if err != nil || expected != ringDigest.hash {
		t.Fatalf("the ring dataset is not the generator's: hash %s, generator %s (%v)", ringDigest.hash, expected, err)
	}
	if counts := countSample(ringDigest.users); counts.Posts == 0 || counts.Following == 0 || counts.Feed == 0 || counts.Inbox == 0 || counts.Likes == 0 || counts.Comments == 0 {
		t.Errorf("the sample is too thin to prove anything: %+v", counts)
	}
	media := 0
	for _, user := range ringDigest.users {
		media += len(user.Media)
	}
	if media == 0 {
		t.Error("no sampled user holds an avatar or a photo; the media are not part of the proof")
	}

	var out bytes.Buffer
	if err := parityOn(ctx, engine.cfg, engine, graph, &out); err != nil {
		t.Fatal(err)
	}
	var result parityResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil || !result.MatchesGenerator || result.Engine != engineRing || result.SampleHash != ringDigest.hash {
		t.Fatalf("-mode parity-hash on the ring engine: %s %v", out.String(), err)
	}
}

// TestRingParityHashSeesADifferenceInTheData: the check is not vacuous; one lost follow edge
// changes the hash and names the user.
func TestRingParityHashSeesADifferenceInTheData(t *testing.T) {
	ctx := t.Context()
	engine, _, _ := seededRingEngine(t)
	ranks := sampleUsers(tinySpec.Users, 30, datasetSeed)
	before, err := readSample(ctx, engine, ranks, 4)
	if err != nil {
		t.Fatal(err)
	}
	ops, err := engine.newSocialOps()
	if err != nil {
		t.Fatal(err)
	}
	defer ops.close()
	victim := ranks[0]
	followed, err := ops.followingList(ctx, victim, 50)
	if err != nil || len(followed) == 0 {
		t.Fatalf("user %d follows nobody in the seed: %v %v", victim, followed, err)
	}
	other, _ := parseRank(followed[0])
	if err := ops.unfollow(ctx, victim, other); err != nil {
		t.Fatal(err)
	}
	after, err := readSample(ctx, engine, ranks, 4)
	if err != nil {
		t.Fatal(err)
	}
	if after.hash == before.hash {
		t.Fatal("removing a follow edge left the sample hash unchanged")
	}
	if differing := differingUsers(before, after, 5); !containsRank(differing, victim) {
		t.Errorf("the differing users are %v, want to name user %d", differing, victim)
	}
}

func containsRank(ranks []int, rank int) bool {
	for _, candidate := range ranks {
		if candidate == rank {
			return true
		}
	}
	return false
}

// ringRequestsPerOperation is what each operation of the mix costs on the ring layout, from
// HARNESS.md, for the operations whose cost does not depend on the data. N1 is 2 with
// notifications waiting and 1 without; Q1 is 3, or 2 on the rare round where another worker claimed its job.
var ringRequestsPerOperation = map[string]float64{"R1": 1, "R2": 3, "R4": 3, "R5": 1, "R6": 1, "R7": 1, "W2": 2, "W3": 3, "W5": 2, "W6": 2}

// TestRingEveryOperationOfTheMixRunsOnARealNode runs a short verified leg of each operation
// against one seeded node and reads the cost of the operation from the record.
func TestRingEveryOperationOfTheMixRunsOnARealNode(t *testing.T) {
	engine, _, addr := seededRingEngine(t)
	for _, operation := range operationIDs {
		t.Run(operation, func(t *testing.T) {
			cfg := ringLiveConfig(addr, "op:"+operation)
			cfg.Attach = true
			engine.cfg = cfg
			record, err := runMixLeg(t.Context(), cfg, tinySpec, engine)
			if err != nil {
				t.Fatalf("the %s leg: %v", operation, err)
			}
			if len(record.ErrorsByType) != 0 {
				t.Fatalf("the %s leg had errors: %v", operation, record.ErrorsByType)
			}
			figures := record.Ops[operation]
			if figures.Count == 0 {
				t.Fatalf("the %s leg ran no operation", operation)
			}
			if want, fixed := ringRequestsPerOperation[operation]; fixed && figures.ReqPerOp != want {
				t.Errorf("%s took %v requests per operation, want %v", operation, figures.ReqPerOp, want)
			}
			if operation == "N1" && (figures.ReqPerOp < 1 || figures.ReqPerOp > 2) {
				t.Errorf("N1 took %v requests per operation, want 1 or 2", figures.ReqPerOp)
			}
			if operation == "Q1" && (figures.ReqPerOp < 2 || figures.ReqPerOp > 3) {
				t.Errorf("Q1 took %v requests per operation, want 2 or 3", figures.ReqPerOp)
			}
			if mixVerifies(cfg.Mix) && record.Verified == 0 {
				t.Errorf("the %s leg verified no read", operation)
			}
			if record.Placement != "entity-documents" || record.EngineSettings["ring_serial"] != "false" {
				t.Errorf("record placement %q ring_serial %q", record.Placement, record.EngineSettings["ring_serial"])
			}
		})
	}
}

// TestRingLegOpensExactlyOneConnectionPerWorker: -depth is the pool. A leg at depth 3 dials 3
// connections for its workers and one to read the node's version for the record, and no more.
func TestRingLegOpensExactlyOneConnectionPerWorker(t *testing.T) {
	engine, _, addr := seededRingEngine(t)
	var dialled atomic.Int64
	engine.dial = func(ctx context.Context) (ringAPI, error) {
		dialled.Add(1)
		client, err := engine.dialClient(ctx)
		if err != nil {
			return nil, err
		}
		return &ringConn{client: client, database: engine.cfg.Database}, nil
	}
	cfg := ringLiveConfig(addr, "op:R1")
	cfg.Attach, cfg.Conc = true, 3
	engine.cfg = cfg
	if _, err := runMixLeg(t.Context(), cfg, tinySpec, engine); err != nil {
		t.Fatal(err)
	}
	if got := dialled.Load(); got != 4 {
		t.Fatalf("a leg at depth 3 dialled %d connections, want 3 workers and 1 for the node's version", got)
	}
}

func TestRingWitnessAndTLSCheckRunOnARealNode(t *testing.T) {
	engine, _, _ := seededRingEngine(t)
	durations, err := engine.witnessWrites(t.Context(), 5)
	if err != nil || len(durations) != 5 {
		t.Fatalf("witness writes: %d %v", len(durations), err)
	}
	findings, err := engine.checkEncryption(t.Context())
	if err != nil || len(findings) != 1 || !findings[0].Encrypted {
		t.Fatalf("tls check: %+v %v", findings, err)
	}
}

// TestRingDurabilityNamesTheNode: the record carries the node's version and the wire.
func TestRingDurabilityNamesTheNode(t *testing.T) {
	engine, _, _ := seededRingEngine(t)
	facts := engine.durability()
	if facts["wire"] != "entry-ring" || facts["version"] == "" || strings.HasPrefix(facts["version"], "unknown") {
		t.Fatalf("durability facts %v", facts)
	}
}

// TestRingPinnedFingerprintRefusesAnotherCertificate: -ring-fingerprint pins the node; a wrong
// value is refused, and the right one (the node's own) is not tested here because the in-process
// node generates its certificate at start.
func TestRingPinnedFingerprintRefusesAnotherCertificate(t *testing.T) {
	addr := startRingServer(t)
	cfg := ringLiveConfig(addr, "")
	cfg.RingFingerprint = strings.Repeat("00", 32)
	if _, err := newRingEngine(cfg).dialClient(t.Context()); err == nil {
		t.Fatal("a wrong fingerprint was accepted")
	}
}

// TestRingParallelFanoutOnARealNode: a publish's feed pushes in flight together on one
// connection are all applied exactly once, in both modes, and a push repeated under its request
// id (the retry of a lost reply) is answered by the owner's dedupe window and not applied twice.
// Whether the owner's log shares fsyncs between the in-flight pushes is measured on the node
// itself (the node's log counters are not exported); the timings logged here are a hint only.
func TestRingParallelFanoutOnARealNode(t *testing.T) {
	engine, _, _ := seededRingEngine(t)
	api, err := engine.open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer api.close()
	followers := make([]int, 24)
	for index := range followers {
		followers[index] = 30 + index
	}
	element := bson.D{{Key: "post", Value: "p"}}
	feedSizes := func() []int64 {
		sizes := make([]int64, len(followers))
		for index, follower := range followers {
			count, err := api.count(t.Context(), userStore(follower, ringload.UserFeed))
			if err != nil {
				t.Fatal(err)
			}
			sizes[index] = count
		}
		return sizes
	}
	for _, mode := range []struct {
		name  string
		width int
	}{{"serial", 0}, {"parallel", 8}} {
		before := feedSizes()
		started := time.Now()
		ops := &ringOps{api: api, fanoutWidth: mode.width}
		if err := ops.pushFeeds(t.Context(), followers, element, noLedger{}); err != nil {
			t.Fatalf("%s: %v", mode.name, err)
		}
		t.Logf("%s: %d pushes in %v", mode.name, len(followers), time.Since(started))
		for index, size := range feedSizes() {
			if size != before[index]+1 {
				t.Errorf("%s: user %d feed grew from %d to %d, want one copy", mode.name, followers[index], before[index], size)
			}
		}
	}
	store, id := userStore(followers[0], ringload.UserFeed), api.nextRequestID()
	before := feedSizes()
	for range 2 {
		if err := api.pushWithID(t.Context(), store, element, id); err != nil {
			t.Fatal(err)
		}
	}
	if after := feedSizes(); after[0] != before[0]+1 {
		t.Errorf("a push sent twice under one request id grew the feed from %d to %d, want one copy", before[0], after[0])
	}
}

// TestRingProjectedReadsEqualWholeReadsOnARealNode: against the real server's get_fields, the
// profile, profile timeline, post page and avatar of seeded users answer exactly as the
// whole-document reads do, and the projected run sends the same number of requests.
func TestRingProjectedReadsEqualWholeReadsOnARealNode(t *testing.T) {
	engine, graph, _ := seededRingEngine(t)
	ctx := t.Context()
	open := func(whole bool) *ringOps {
		api, err := engine.open(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(api.close)
		return &ringOps{api: api, wholeDocuments: whole}
	}
	projected, whole := open(false), open(true)
	checked := 0
	for user := range min(graph.spec.Users, 12) {
		a, errA := projected.profile(ctx, user)
		b, errB := whole.profile(ctx, user)
		if errA != nil || errB != nil || !reflect.DeepEqual(a, b) || a.Name == "" {
			t.Fatalf("profile of user %d: projected %+v %v, whole %+v %v", user, a, errA, b, errB)
		}
		timelineA, errA := projected.profileTimeline(ctx, user, 10)
		timelineB, errB := whole.profileTimeline(ctx, user, 10)
		if errA != nil || errB != nil || !reflect.DeepEqual(timelineA, timelineB) {
			t.Fatalf("timeline of user %d: projected %+v %v, whole %+v %v", user, timelineA, errA, timelineB, errB)
		}
		for _, post := range timelineA {
			seq := post.Seq
			pageA, errA := projected.postPage(ctx, user, seq)
			pageB, errB := whole.postPage(ctx, user, seq)
			if errA != nil || errB != nil || !pageA.Found || pageA.Post.Text == "" || !reflect.DeepEqual(pageA, pageB) {
				t.Fatalf("post %d:%d: projected %+v %v, whole %+v %v", user, seq, pageA, errA, pageB, errB)
			}
			checked++
		}
		avatarA, errA := projected.avatar(ctx, user)
		avatarB, errB := whole.avatar(ctx, user)
		if !errors.Is(errA, errNoAvatar) != !errors.Is(errB, errNoAvatar) || !bytes.Equal(avatarA, avatarB) {
			t.Fatalf("avatar of user %d: projected %d bytes %v, whole %d bytes %v", user, len(avatarA), errA, len(avatarB), errB)
		}
	}
	if checked == 0 {
		t.Fatal("no post was compared")
	}
	if projected.requests() != whole.requests() {
		t.Errorf("the projected run sent %d requests, the whole run %d", projected.requests(), whole.requests())
	}
}
