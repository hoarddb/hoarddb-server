package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/bench/social/ringload"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

const batchSmokeDocs = 2000

// TestRingBatchSmoke prints the rate of 2000 inserts and of 2000 feed pushes sent one request each,
// in frames of 64 and in frames of 255, against one in-process node. It is a local sanity check
// that the frame path works end to end and is faster than the serial one, not a benchmark: one
// connection, one machine, no warm-up. The figures are internal and go in no document. It runs only
// when HOARDDB_RING_BATCH_SMOKE=1, and by name:
//
//	HOARDDB_RING_BATCH_SMOKE=1 go test ./bench/social -run TestRingBatchSmoke -count=1 -v
func TestRingBatchSmoke(t *testing.T) {
	if os.Getenv("HOARDDB_RING_BATCH_SMOKE") != "1" {
		t.Skip("set HOARDDB_RING_BATCH_SMOKE=1 to run the local smoke")
	}
	addr := startRingServer(t)
	cfg := ringLiveConfig(addr, "")
	engine := newRingEngine(cfg)
	ctx := t.Context()
	client, err := engine.dialClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := ringload.CreateSchema(ctx, client, cfg.Database, true); err != nil {
		t.Fatal(err)
	}
	users := client.Bucket(cfg.Database, ringload.UserBucket)

	for _, frame := range []int{0, 64, maxRingBatchSize} {
		took := insertUsers(ctx, t, client, users, "ins"+strconv.Itoa(frame)+"-", frame)
		t.Logf("insert  frame size %3d: %d docs in %v = %.0f ops/s, %v per op", frame, batchSmokeDocs, took.Round(time.Millisecond), float64(batchSmokeDocs)/took.Seconds(), (took / batchSmokeDocs).Round(time.Microsecond))
	}

	// The feed pushes go through the bench's own sender (pushFeeds), over the production connection.
	followers := seedFollowers(ctx, t, client, users)
	for _, frame := range []int{0, 64, maxRingBatchSize} {
		api := &ringConn{client: client, database: cfg.Database}
		ops := &ringOps{api: api}
		if frame > 0 {
			ops.batchSize, ops.batch = frame, newBatchTotals(frame)
		}
		started := time.Now()
		if err := ops.pushFeeds(ctx, followers, bson.D{{Key: "k", Value: "post"}}, noLedger{}); err != nil {
			t.Fatalf("feed pushes at frame size %d: %v", frame, err)
		}
		took := time.Since(started)
		t.Logf("feedpush frame size %3d: %d pushes in %v = %.0f ops/s, %v per op, %d requests", frame, len(followers), took.Round(time.Millisecond), float64(len(followers))/took.Seconds(), (took / time.Duration(len(followers))).Round(time.Microsecond), api.requests())
		if ops.batch != nil {
			fmt.Print(ops.batch.summary())
		}
	}
}

// insertUsers inserts batchSmokeDocs User documents named prefix+n, one request each when frame is
// 0 and otherwise in frames of that many, and returns how long it took.
func insertUsers(ctx context.Context, t *testing.T, client *ringclient.Client, users *ringclient.Bucket, prefix string, frame int) time.Duration {
	t.Helper()
	started := time.Now()
	if frame == 0 {
		for n := range batchSmokeDocs {
			if _, err := users.Insert(ctx, prefix+strconv.Itoa(n), ringclient.Doc{"Name": "u"}); err != nil {
				t.Fatalf("inserting %s%d: %v", prefix, n, err)
			}
		}
		return time.Since(started)
	}
	for start := 0; start < batchSmokeDocs; start += frame {
		batch := client.NewBatch()
		queued := users.In(batch)
		for n := start; n < min(start+frame, batchSmokeDocs); n++ {
			if _, err := queued.Insert(ctx, prefix+strconv.Itoa(n), ringclient.Doc{"Name": "u"}); err != nil {
				t.Fatalf("queuing %s%d: %v", prefix, n, err)
			}
		}
		sent, err := batch.Send(ctx)
		if err != nil {
			t.Fatalf("sending the frame at %d: %v", start, err)
		}
		for index, answer := range sent.Answers {
			if answer.Err != nil {
				t.Fatalf("insert %d of the frame at %d was refused: %v", index, start, answer.Err)
			}
		}
	}
	return time.Since(started)
}

// seedFollowers creates batchSmokeDocs users whose ids are the numbers pushFeeds addresses, and
// returns those numbers.
func seedFollowers(ctx context.Context, t *testing.T, client *ringclient.Client, users *ringclient.Bucket) []int {
	t.Helper()
	followers := make([]int, batchSmokeDocs)
	for start := 0; start < batchSmokeDocs; start += maxRingBatchSize {
		batch := client.NewBatch()
		queued := users.In(batch)
		for n := start; n < min(start+maxRingBatchSize, batchSmokeDocs); n++ {
			followers[n] = n
			if _, err := queued.Insert(ctx, ringload.UserID(n), ringclient.Doc{"Name": "f"}); err != nil {
				t.Fatalf("queuing follower %d: %v", n, err)
			}
		}
		if _, err := batch.Send(ctx); err != nil {
			t.Fatalf("seeding followers at %d: %v", start, err)
		}
	}
	return followers
}
