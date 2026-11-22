package main

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	hoardconfig "github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The -hoard-batch variant (batch-request spec §7.3, T-B1 to T-B3).

// TestRequestsPerOperationAreCountedBatched is T-B1's second table: under
// -hoard-batch a publish to every follower is 2 requests (the follower list and
// one batch), and like, follow, unfollow, comment and a post page are 1 each.
func TestRequestsPerOperationAreCountedBatched(t *testing.T) {
	world := newFakeWorld(t)
	world.engine.cfg.HoardBatch = true
	ctx := context.Background()
	ops := world.ops(t)
	author := world.busiestAuthor()
	if followers := len(world.followersOf(author)); followers == 0 || followers > batchItemLimit-1 {
		t.Fatalf("the busiest author has %d followers; the table needs 1 to 999", followers)
	}
	post := postRecord{Author: userID(author), Seq: 100, Text: "t", Created: time.Now().UTC()}
	cases := []struct {
		name string
		want int64
		run  func() error
	}{
		{"R4", 1, func() error { _, err := ops.postPage(ctx, 3, 1); return err }},
		{"W1", 2, func() error { _, err := ops.publishPost(ctx, post, noLedger{}); return err }},
		{"W2", 1, func() error { return ops.likePost(ctx, 4, 3, 1) }},
		{"W3", 1, func() error { return ops.follow(ctx, 4, 5) }},
		{"W4", 1, func() error { return ops.unfollow(ctx, 4, 5) }},
		{"W5", 1, func() error { return ops.commentOnPost(ctx, 4, 3, 1, "c") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := ops.requests()
			if err := tc.run(); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if got := ops.requests() - before; got != tc.want {
				t.Errorf("%s sent %d requests under -hoard-batch, want %d", tc.name, got, tc.want)
			}
		})
	}
}

// TestHoardBatchIsALabelledVariant: -hoard-batch records the leg as variant
// batch, and is refused beside another -variant and for another engine.
func TestHoardBatchIsALabelledVariant(t *testing.T) {
	cfg, err := parseFlags([]string{"-engine", "hoarddb", "-hoard-batch"})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.HoardBatch || cfg.Variant != variantBatch {
		t.Fatalf("-hoard-batch parsed to batch=%v variant=%q; the leg must be recorded as variant %q", cfg.HoardBatch, cfg.Variant, variantBatch)
	}
	if _, err := parseFlags([]string{"-engine", "hoarddb", "-hoard-batch", "-variant", "cache-raised"}); err == nil {
		t.Error("-hoard-batch beside another -variant was accepted; a batched run would be recorded under another name")
	}
	if _, err := parseFlags([]string{"-engine", "mysql", "-hoard-batch"}); err == nil {
		t.Error("-hoard-batch for MySQL was accepted")
	}
}

// TestWriteConcernSentOnTheBatchEnvelope is T-B2: under -hoard-batch the run's
// concern rides on the BATCH envelope, and no item carries one.
func TestWriteConcernSentOnTheBatchEnvelope(t *testing.T) {
	recorder := newConcernRecorder("majority")
	recorder.engine.cfg.HoardBatch = true
	ctx := context.Background()
	if _, err := recorder.engine.setupSocial(ctx, tinySpec); err != nil {
		t.Fatalf("setting up: %v", err)
	}
	if err := recorder.engine.seedSocial(ctx, newSocialGraph(tinySpec, 7), 2); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	ops, err := recorder.engine.newSocialOps()
	if err != nil {
		t.Fatal(err)
	}
	defer ops.close()
	post := postRecord{Author: userID(3), Seq: 100, Text: "t", Created: time.Now().UTC()}
	for name, step := range map[string]func() error{
		"W1": func() error { _, err := ops.publishPost(ctx, post, noLedger{}); return err },
		"W3": func() error { return ops.follow(ctx, 4, 5) },
	} {
		if err := step(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	batches := 0
	for _, sent := range recorder.sent() {
		if sent.op != transport.OpBatch {
			continue
		}
		batches++
		request := sent.request.(server.BatchRequest)
		if request.WriteConcern != "majority" {
			t.Errorf("a BATCH was sent with write concern %q; the run's concern rides on the envelope", request.WriteConcern)
		}
		for i, item := range request.Items {
			if _, err := item.Payload.LookupErr("writeconcern"); err == nil {
				t.Errorf("item %d of a BATCH names a write concern; the server refuses that", i+1)
			}
		}
	}
	if batches != 2 {
		t.Fatalf("%d BATCH requests were sent for a publish and a follow, want 2", batches)
	}
}

// startBenchServer starts a real hoardDB server on loopback and returns its
// driver address and root password.
func startBenchServer(t *testing.T) (string, string) {
	t.Helper()
	const password = "bench-batch-password"
	cfg := hoardconfig.DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = "127.0.0.1:0"
	capture := &listenerCapture{address: make(chan string, 1)}
	service, err := server.NewService(cfg, slog.New(capture))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := service.SeedRootUser("admin", password); err != nil {
		t.Fatalf("SeedRootUser: %v", err)
	}
	if err := service.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = service.Stop() })
	select {
	case addr := <-capture.address:
		return addr, password
	case <-time.After(10 * time.Second):
		t.Fatal("the server did not report its driver listener address")
	}
	return "", ""
}

// listenerCapture reads the driver listener's address from the server's log.
type listenerCapture struct{ address chan string }

func (h *listenerCapture) Enabled(context.Context, slog.Level) bool { return true }
func (h *listenerCapture) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *listenerCapture) WithGroup(string) slog.Handler            { return h }
func (h *listenerCapture) Handle(_ context.Context, record slog.Record) error {
	if record.Message != "TCP driver listener listening" {
		return nil
	}
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "address" {
			select {
			case h.address <- attr.Value.String():
			default:
			}
			return false
		}
		return true
	})
	return nil
}

// TestSocialBatchPublishLiveServer is T-B3: the batched publish against a live
// single-node server in the hu layout marks every follower delivered; with one
// follower's bucket dropped, every other follower is delivered, that one is
// not, and the publish returns that item's error.
func TestSocialBatchPublishLiveServer(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a server")
	}
	addr, password := startBenchServer(t)
	cfg := &config{Engine: engineHoardDB, Addr: addr, Addrs: []string{addr}, Password: password, Database: "social_batch",
		Users: tinySpec.Users, Conc: 4, OpTimeout: 30 * time.Second, Placement: "hu", HoardBatch: true}
	engine := newHoardEngine(cfg)
	world := &socialWorld{engine: engine, social: engine}
	world.seed(t)
	ctx := context.Background()
	ops := world.ops(t)
	author := world.busiestAuthor()
	followers := world.followersOf(author)
	if len(followers) < 2 {
		t.Fatalf("the busiest author has %d followers; the test needs two", len(followers))
	}

	ledger := &recordingLedger{}
	post := postRecord{Author: userID(author), Seq: 900, Text: "batched", Created: time.Now().UTC()}
	if _, err := ops.publishPost(ctx, post, ledger); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	for _, follower := range followers {
		if !slices.Contains(ledger.acknowledged, follower) {
			t.Errorf("follower %d was not marked delivered", follower)
		}
	}

	gone := followers[0]
	admin, err := dialWire(addr, password, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.close()
	var dropped server.DeleteBucketResponse
	if err := admin.call(transport.OpDeleteBucket, server.DeleteBucketRequest{Database: cfg.Database, Name: engine.placement().bucketOf(gone)}, &dropped); err != nil || !dropped.Success {
		t.Fatalf("dropping the bucket of follower %d: %+v, %v", gone, dropped, err)
	}
	ledger = &recordingLedger{}
	post.Seq = 901
	_, err = ops.publishPost(ctx, post, ledger)
	if err == nil || !strings.Contains(err.Error(), "the feed of user "+userID(gone)) {
		t.Fatalf("publishing with follower %d's bucket gone returned %v; want that item's error", gone, err)
	}
	for _, follower := range followers {
		switch {
		case follower == gone && slices.Contains(ledger.acknowledged, follower):
			t.Errorf("follower %d, whose bucket is gone, was marked delivered", follower)
		case follower != gone && !slices.Contains(ledger.acknowledged, follower):
			t.Errorf("follower %d was not marked delivered beside the one refused item", follower)
		}
	}
	var found server.FindResponse
	request := server.FindRequest{Database: cfg.Database, Bucket: engine.placement().bucketOf(author), Key: userID(author)}
	if err := admin.call(transport.OpGet, request, &found); err != nil || !found.Found {
		t.Fatalf("reading the author back: %+v, %v", found, err)
	}
	if _, err := bson.Raw(found.Data).LookupErr("_id_"); err != nil {
		t.Fatalf("the author's document has no _id_: %v", err)
	}
}
