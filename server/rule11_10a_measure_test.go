package server

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The rule-11 measurements dispatch 10a's build doc asks for (§13): 10a.3's
// 1,000 pulls, 10a.5's drop records and sweep after a 10,000-bucket drop on
// four members (with and without an unreachable removed node), and 10a.8's
// state document after 10,000 refusals with retention held. They measure,
// they assert nothing a regression could miss, and they take minutes, so they
// run only with HOARDDB_MEASURE_10A=1 and -v; the build report records what
// they printed.

func skipUnlessMeasuring10a(t *testing.T) {
	t.Helper()
	if os.Getenv("HOARDDB_MEASURE_10A") != "1" {
		t.Skip("set HOARDDB_MEASURE_10A=1 to run the dispatch 10a rule-11 measurements")
	}
}

// TestMeasureAThousandPulls times 1,000 pulls of a ten-document bucket landed
// through Server.materializeRange on a real store: one store-wide Sync each
// (10a.3). The figure before the fix is this test with that Sync removed.
func TestMeasureAThousandPulls(t *testing.T) {
	skipUnlessMeasuring10a(t)
	const database, documents, pulls = "app", 10, 1000
	source := newTestServer(t)
	if resp, err := source.CreateBucket(CreateBucketRequest{Database: database, Name: "source", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("creating the source bucket: %+v, %v", resp, err)
	}
	for i := range documents {
		if resp, err := source.Insert(InsertRequest{Database: database, Bucket: "source", Key: fmt.Sprintf("doc%02d", i), Data: map[string]interface{}{"v": i}}); err != nil || !resp.Success {
			t.Fatalf("inserting: %+v, %v", resp, err)
		}
	}
	held := lookupBucket(source, database, "source")
	var stream bytes.Buffer
	if err := source.streamBucketRange(&stream, source.kv, held.slot, held.databaseID, held.id.Load()); err != nil {
		t.Fatalf("streaming the source's range: %v", err)
	}
	joiner := newTestServer(t)
	if resp, err := joiner.CreateDatabase(database); err != nil || !resp.Success {
		t.Fatalf("creating the joiner's database: %+v, %v", resp, err)
	}
	started := time.Now()
	for i := range pulls {
		pulled := migratedBucket{database: database, name: fmt.Sprintf("pulled%04d", i), storeType: BucketTypeHash}
		if err := joiner.materializeRange(pulled, stream.Bytes(), 0); err != nil {
			t.Fatalf("pull %d: %v", i, err)
		}
	}
	took := time.Since(started)
	t.Logf("MEASURE 10a.3: %d pulls of a %d-document bucket took %v, %v each", pulls, documents, took, took/pulls)
}

// massCreateAndDrop creates count hash buckets through node 0's client door,
// workers at a time, then drops them all at `one`.
func massCreateAndDrop(t *testing.T, tc *testCluster, members []int, count int) {
	t.Helper()
	const workers = 8
	var retried atomic.Int64
	run := func(op transport.Opcode, request func(name string) any) {
		names := make(chan string, count)
		for i := range count {
			names <- fmt.Sprintf("mass%05d", i)
		}
		close(names)
		var wg sync.WaitGroup
		failures := make(chan string, workers)
		for worker := range workers {
			client := tc.dial(members[worker%len(members)])
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { _ = client.Close() }()
				for name := range names {
					payload, err := bson.Marshal(request(name))
					if err != nil {
						failures <- err.Error()
						return
					}
					// A refusal with retry is retried, as a client would, and
					// counted: the build report says how many there were.
					// The "being pulled" refusal of a never-created name after a
					// settled removal is a defect the coordinator files; this
					// retry masks it here, so the count is the only trace.
					var frame *transport.Frame
					for attempt := 0; ; attempt++ {
						ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
						frame, err = client.SendRequest(ctx, op, payload)
						cancel()
						if err != nil || frame.Opcode != transport.OpError || attempt == 50 ||
							!bytes.Contains(frame.Payload, []byte("retry")) && !bytes.Contains(frame.Payload, []byte("being pulled")) {
							break
						}
						retried.Add(1)
						time.Sleep(100 * time.Millisecond)
					}
					if err != nil || frame.Opcode == transport.OpError {
						failures <- fmt.Sprintf("%s %s: %v %s", op, name, err, printable(string(frame.Payload)))
						return
					}
				}
			}()
		}
		wg.Wait()
		close(failures)
		for failure := range failures {
			t.Fatalf("mass %s: %s", op, failure)
		}
	}
	started := time.Now()
	run(transport.OpCreateBucket, func(name string) any {
		return bson.M{"database": testDatabase, "name": name, "type": string(BucketTypeHash), "write_concern": "one"}
	})
	t.Logf("MEASURE 10a.5: %d creates took %v (%d refusals with retry, retried)", count, time.Since(started), retried.Swap(0))
	started = time.Now()
	run(transport.OpDeleteBucket, func(name string) any {
		return bson.M{"database": testDatabase, "name": name, "write_concern": "one"}
	})
	t.Logf("MEASURE 10a.5: %d drops took %v (%d refusals with retry, retried)", count, time.Since(started), retried.Swap(0))
}

// holdTheTombstoneSweep keeps the background tombstone sweep from running
// for the test, so the records a mass drop writes are there to count and the
// sweep timed is the one the test calls.
func holdTheTombstoneSweep(t *testing.T) {
	t.Helper()
	interval := sweepRearmInterval.get()
	sweepRearmInterval.set(time.Hour)
	t.Cleanup(func() { sweepRearmInterval.set(interval) })
}

// reportDropRecords waits for every member to hold count drop records, prints
// each one's count, and runs and times each member's tombstone sweep.
func reportDropRecords(t *testing.T, tc *testCluster, members []int, count int) {
	t.Helper()
	recorded := func(index int) int {
		listed, err := tc.nodes[index].server.core.listDropRecords()
		if err != nil {
			t.Fatalf("listing node %d's drop records: %v", index+1, err)
		}
		return len(listed)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for _, index := range members {
		for recorded(index) < count && time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
		}
	}
	for _, index := range members {
		before := recorded(index)
		started := time.Now()
		deleted, err := clusterOf(tc, index).sweepTombstones(context.Background())
		took := time.Since(started)
		t.Logf("MEASURE 10a.5: node %d held %d drop records; its sweep took %v, deleted %d, left %d (%v)",
			index+1, before, took, deleted, recorded(index), err)
	}
}

// TestMeasureDropRecordsAfterAMassDrop: four members at factor 3; 10,000
// buckets created and dropped; each member's record count and sweep.
func TestMeasureDropRecordsAfterAMassDrop(t *testing.T) {
	skipUnlessMeasuring10a(t)
	const count = 10_000
	holdTheTombstoneSweep(t)
	tc := newTestCluster(t, 4, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2, 3)
	members := []int{0, 1, 2, 3}
	massCreateAndDrop(t, tc, members, count)
	reportDropRecords(t, tc, members, count)
}

// TestMeasureDropRecordsWithAnUnreachableRemovedNode: five members; the fifth
// is stopped and removed, so it stays in the history and cannot answer. The
// same 10,000 buckets are created and dropped on the four left.
func TestMeasureDropRecordsWithAnUnreachableRemovedNode(t *testing.T) {
	skipUnlessMeasuring10a(t)
	const count = 10_000
	holdTheTombstoneSweep(t)
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	tc.stopNodeKeepingData(4)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := clusterOf(tc, 0).proposeRemoval(ctx, tc.addrs[4]); err != nil {
		t.Fatalf("removing node 5: %v", err)
	}
	members := []int{0, 1, 2, 3}
	removedID := tc.addrs[4]
	if !eventually(func() bool {
		for _, index := range members {
			if nodeListed(clusterOf(tc, index).Members(), removedID) ||
				!slices.ContainsFunc(clusterOf(tc, index).removedInHistory(), func(n ring.Node) bool { return n.ID == removedID }) {
				return false
			}
		}
		return true
	}) {
		t.Fatal("setup: the four members never adopted the removal of node 5")
	}
	removal := clusterOf(tc, 0).Epoch()
	if !eventually(func() bool {
		for _, index := range members {
			if clusterOf(tc, index).settled.Load() < removal {
				return false
			}
		}
		return true
	}) {
		t.Fatal("setup: the four members never settled the removal of node 5")
	}
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2, 3)
	massCreateAndDrop(t, tc, members, count)
	reportDropRecords(t, tc, members, count)
}

// TestMeasureRefusalsOnAHotStateDocument: three members at factor 3; 10,000
// deletes of missing keys through the owner, with one member stopped so
// retention is held, and with every member up. The owner's state document
// size, and a delete's latency at the start and at the end (S-13).
func TestMeasureRefusalsOnAHotStateDocument(t *testing.T) {
	skipUnlessMeasuring10a(t)
	for _, held := range []bool{true, false} {
		t.Run(fmt.Sprintf("retention held %v", held), func(t *testing.T) { measureRefusals(t, held) })
	}
}

func measureRefusals(t *testing.T, retentionHeld bool) {
	const refusals, sample = 10_000, 200
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	full := ringWith(t, 3, tc.addrs...)
	bucket := bucketNamed(t, "hot", func(name string) bool { return ownerOn(full, name) == tc.addrs[0] })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	if retentionHeld {
		tc.stopNodeKeepingData(2)
	}
	owner := tc.nodes[0].server.core
	var first, last []time.Duration
	for i := range refusals {
		started := time.Now()
		tc.frame(0, transport.OpDelete, DeleteRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("missing%05d", i)})
		took := time.Since(started)
		switch {
		case i < sample:
			first = append(first, took)
		case i >= refusals-sample:
			last = append(last, took)
		}
	}
	st, err := owner.bucketStateNamed(testDatabase, bucket)
	if err != nil {
		t.Fatalf("reading the owner's state document: %v", err)
	}
	encoded, err := bson.Marshal(st)
	if err != nil {
		t.Fatalf("encoding the state document: %v", err)
	}
	slices.Sort(first)
	slices.Sort(last)
	t.Logf("MEASURE 10a.8: after %d refused deletes, retention held %v: %d refusals recorded, state document %d bytes; "+
		"a delete of a missing key took a median %v over the first %d, %v over the last %d",
		refusals, retentionHeld, len(st.Refusals), len(encoded), first[len(first)/2], sample, last[len(last)/2], sample)
}
