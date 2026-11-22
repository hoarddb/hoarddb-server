package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// frameSyncSource is the sender every push in this file names: a node no
// member runs, whose stream the receiver opens by hand (streamOf).
const frameSyncSource = "test-owner"

// replicaUnderTest is a one-member cluster over the fault store with the hash
// buckets app/copies and app/other to apply into and source's stream to it
// already open, so a test can send it a push and count what the push alone
// costs.
func replicaUnderTest(t *testing.T) *Cluster {
	t.Helper()
	dataDir := t.TempDir()
	kv := kvfault.New()
	s := newKVFaultServer(dataDir, kv)
	for _, name := range []string{"copies", "other"} {
		if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeHash}); err != nil || !resp.Success {
			t.Fatalf("creating bucket %q: %+v, %v", name, resp, err)
		}
	}
	if err := kv.Sync(); err != nil {
		t.Fatalf("syncing the catalog: %v", err)
	}
	c := retentionTestCluster(t, s, dataDir)
	t.Cleanup(func() {
		c.cancel()
		if err := s.wal.Close(); err != nil {
			t.Errorf("closing the WAL: %v", err)
		}
	})
	streamOf(t, c, frameSyncSource)
	return c
}

// framePuts is one stream frame of count puts into app/copies, at the source's
// sequences first .. first+count-1.
func framePuts(t *testing.T, first uint64, count int) []ReplEntry {
	t.Helper()
	entries := make([]ReplEntry, count)
	for i := range entries {
		key := fmt.Sprintf("k%04d", first+uint64(i))
		entries[i] = ReplEntry{Seq: first + uint64(i), Op: int32(transport.OpPut), Bucket: "copies", Key: []byte(key),
			Payload: putPayload(t, "app", "copies", key)}
	}
	return entries
}

// TestAStreamFrameIsLoggedWithOneSyncNotOnePerEntry is BUG-0521's named test.
// A replica applies a frame's entries one after another under the sender's
// stream lock, so no two of them ever wait for the log's fsync together: on
// the starting tree a frame of 64 entries cost 64 fsyncs, which capped one
// stream at one entry per fsync on a real disk. The frame is logged first and
// synced once; every entry it acknowledges is synced when the answer leaves.
func TestAStreamFrameIsLoggedWithOneSyncNotOnePerEntry(t *testing.T) {
	c := replicaUnderTest(t)
	var fsyncs atomic.Int64
	c.s.wal.SetFsyncHook(func() { fsyncs.Add(1) })
	defer c.s.wal.SetFsyncHook(nil)

	const frameEntries = maxReplBatch
	resp := pushToReplica(t, c, frameSyncSource, framePuts(t, 1, frameEntries))
	if !resp.Success || resp.AppliedThrough != frameEntries {
		t.Fatalf("the frame was answered success=%t applied_through=%d error=%q; want all %d entries acknowledged",
			resp.Success, resp.AppliedThrough, resp.Error, frameEntries)
	}
	if synced, last := c.s.wal.SyncedSequence(), c.s.wal.LastSequence(); synced < last {
		t.Fatalf("the frame was acknowledged with the log synced through %d of %d", synced, last)
	}
	if got := fsyncs.Load(); got != 1 {
		t.Fatalf("one frame of %d entries cost %d fsyncs of the replica's log, want 1", frameEntries, got)
	}
	for _, entry := range framePuts(t, 1, frameEntries) {
		if !holdsKey(t, c.s, "app", "copies", string(entry.Key)) {
			t.Fatalf("the frame was acknowledged and %s is not in the replica's store", entry.Key)
		}
	}
}

// TestAStreamFrameIsAppliedAndAcknowledgedOnlyAfterItsSync holds the frame's
// fsync and looks at the replica in the window: nothing of the frame is in the
// store and nothing is acknowledged. The store is synced on its own schedule,
// so an entry applied ahead of its log record could reach the disk without it,
// and a power loss would then leave the store ahead of the log the next
// sequences are given from.
func TestAStreamFrameIsAppliedAndAcknowledgedOnlyAfterItsSync(t *testing.T) {
	c := replicaUnderTest(t)
	reached := make(chan struct{})
	release := make(chan struct{})
	var first atomic.Bool
	c.s.wal.SetFsyncHook(func() {
		if first.CompareAndSwap(false, true) {
			close(reached)
			<-release
		}
	})
	defer c.s.wal.SetFsyncHook(nil)

	const frameEntries = 16
	answered := make(chan ReplPushResponse, 1)
	go func() { answered <- pushToReplica(t, c, frameSyncSource, framePuts(t, 1, frameEntries)) }()
	waitFor(t, reached, "the frame's first fsync")

	for _, entry := range framePuts(t, 1, frameEntries) {
		if holdsKey(t, c.s, "app", "copies", string(entry.Key)) {
			close(release)
			t.Fatalf("%s is in the replica's store while the fsync of its log record has not returned", entry.Key)
		}
	}
	select {
	case resp := <-answered:
		close(release)
		t.Fatalf("the frame was answered (applied_through=%d) while its fsync had not returned", resp.AppliedThrough)
	default:
	}
	close(release)

	select {
	case resp := <-answered:
		if !resp.Success || resp.AppliedThrough != frameEntries {
			t.Fatalf("after its fsync the frame was answered success=%t applied_through=%d error=%q",
				resp.Success, resp.AppliedThrough, resp.Error)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the frame's answer")
	}
	for _, entry := range framePuts(t, 1, frameEntries) {
		if !holdsKey(t, c.s, "app", "copies", string(entry.Key)) {
			t.Fatalf("the frame was acknowledged and %s is not in the replica's store", entry.Key)
		}
	}
}

// TestAFrameIsAnsweredAsItsEntriesAppliedOneAtATime: a frame that spans two
// buckets and holds an entry the replica refuses is answered as the
// entry-at-a-time apply answered it. The entries before the refusal are applied
// and acknowledged, the refused one is never logged, nothing after it is
// applied, and the run before it cost one fsync.
func TestAFrameIsAnsweredAsItsEntriesAppliedOneAtATime(t *testing.T) {
	c := replicaUnderTest(t)
	var fsyncs atomic.Int64
	c.s.wal.SetFsyncHook(func() { fsyncs.Add(1) })
	defer c.s.wal.SetFsyncHook(nil)

	put := func(seq uint64, bucket, key string) ReplEntry {
		return ReplEntry{Seq: seq, Op: int32(transport.OpPut), Bucket: bucket, Key: []byte(key), Payload: putPayload(t, "app", bucket, key)}
	}
	frame := []ReplEntry{
		put(1, "copies", "a"),
		put(2, "other", "b"),
		put(3, "copies", "c"),
		put(4, "missing", "d"),
		put(5, "copies", "e"),
	}
	loggedBefore := c.s.wal.LastSequence()
	resp := pushToReplica(t, c, frameSyncSource, frame)
	if resp.Success || resp.AppliedThrough != 3 {
		t.Fatalf("the frame was answered success=%t applied_through=%d error=%q; want refused at sequence 4 with 3 acknowledged",
			resp.Success, resp.AppliedThrough, resp.Error)
	}
	if got := appliedSnapshot(c)[frameSyncSource]; got != 3 {
		t.Fatalf("the replica's position for the sender is %d after the frame, want 3", got)
	}
	if logged := c.s.wal.LastSequence() - loggedBefore; logged != 3 {
		t.Fatalf("the frame put %d entries in the replica's log, want the 3 before the refusal", logged)
	}
	if got := fsyncs.Load(); got != 1 {
		t.Fatalf("the frame cost %d fsyncs of the replica's log, want 1 for the three entries before the refusal", got)
	}
	for _, held := range []struct{ bucket, key string }{{"copies", "a"}, {"other", "b"}, {"copies", "c"}} {
		if !holdsKey(t, c.s, "app", held.bucket, held.key) {
			t.Fatalf("%s/%s was acknowledged and is not in the replica's store", held.bucket, held.key)
		}
	}
	if holdsKey(t, c.s, "app", "copies", "e") {
		t.Fatal("copies/e, after the refused entry, was applied")
	}
}

// TestARunNeverWaitsForAFenceWhileItHoldsOne: a frame's run holds its
// entries' bucket fences until it is settled, so it must never wait for
// another one while it holds them: two runs, each behind a drop queued on the
// fence the other holds, would wait for ever. With app/other's fence held
// exclusively, as a drop holds it, the frame's entry of app/copies is settled
// and applied before the entry of app/other waits.
func TestARunNeverWaitsForAFenceWhileItHoldsOne(t *testing.T) {
	c := replicaUnderTest(t)
	drop := c.s.acquireFence(placementOf("app", "other"), fenceExclusive)
	released := false
	defer func() {
		if !released {
			drop.release()
		}
	}()

	frame := []ReplEntry{
		{Seq: 1, Op: int32(transport.OpPut), Bucket: "copies", Key: []byte("a"), Payload: putPayload(t, "app", "copies", "a")},
		{Seq: 2, Op: int32(transport.OpPut), Bucket: "other", Key: []byte("b"), Payload: putPayload(t, "app", "other", "b")},
	}
	answered := make(chan ReplPushResponse, 1)
	go func() { answered <- pushToReplica(t, c, frameSyncSource, frame) }()
	pollUntil(t, "copies/a to be applied while app/other's fence is held", func() bool {
		return holdsKey(t, c.s, "app", "copies", "a")
	})
	select {
	case resp := <-answered:
		t.Fatalf("the frame was answered (applied_through=%d) while app/other's fence was held", resp.AppliedThrough)
	default:
	}
	drop.release()
	released = true

	select {
	case resp := <-answered:
		if !resp.Success || resp.AppliedThrough != 2 {
			t.Fatalf("the frame was answered success=%t applied_through=%d error=%q; want both entries acknowledged",
				resp.Success, resp.AppliedThrough, resp.Error)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the frame's answer after the fence was released")
	}
	if !holdsKey(t, c.s, "app", "other", "b") {
		t.Fatal("other/b was acknowledged and is not in the replica's store")
	}
}

// TestAReturnedReplicaCatchesUpUnderWritersOnASlowLog is BUG-0521's
// reproduction, scaled down and made independent of the host's disk: the
// returned replica's log fsync is slowed to slowFsync in the test seam, which
// is what a real disk costs and tmpfs does not. A replica is stopped, a backlog
// is written, and it comes back while writers keep writing about 100 entries a
// second. On the starting tree it applied at most one entry per fsync, 40 a
// second, so it fell further behind; with a frame synced once it reaches the
// owner's tail.
func TestAReturnedReplicaCatchesUpUnderWritersOnASlowLog(t *testing.T) {
	const (
		slowFsync  = 25 * time.Millisecond
		backlog    = 600
		writers    = 4
		writerPace = 40 * time.Millisecond
		catchUpBy  = 12 * time.Second
	)
	var slowIndex atomic.Int64
	slowIndex.Store(-1)
	tc := newTestClusterTuned(t, 3, 3, nil, func(index int, c *Cluster) {
		if int64(index) == slowIndex.Load() {
			c.s.wal.SetFsyncHook(func() { time.Sleep(slowFsync) })
		}
	})
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	replicaIndex := (ownerIndex + 1) % len(tc.nodes)
	ownerID := tc.addrs[ownerIndex]
	ownerLog := tc.nodes[ownerIndex].server.core.wal

	start := stopNode(t, tc, replicaIndex)
	for i := 0; i < backlog; i++ {
		putDocumentAt(tc, ownerIndex, testDatabase, testBucket, fmt.Sprintf("backlog-%04d", i), "one")
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	clients := make([]*transport.DriverClient, 0, writers)
	// The writers stop before their clients close: a write on a closed client
	// is an error the test would report.
	defer func() {
		close(stop)
		wg.Wait()
		for _, client := range clients {
			_ = client.Close()
		}
	}()
	for w := 0; w < writers; w++ {
		client := tc.dial(ownerIndex)
		clients = append(clients, client)
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				case <-time.After(writerPace):
				}
				key := fmt.Sprintf("live-w%d-%06d", w, i)
				payload, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: testBucket, Key: key,
					Data: map[string]interface{}{"n": i}, WriteConcern: "one"})
				if err != nil {
					t.Errorf("marshal %s: %v", key, err)
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				frame, err := client.SendRequest(ctx, transport.OpPut, payload)
				cancel()
				if err != nil || frame.Opcode == transport.OpError {
					t.Errorf("write %s: %v", key, err)
					return
				}
			}
		}(w)
	}

	slowIndex.Store(int64(replicaIndex))
	restarted := start()
	returnedAt := time.Now()
	// The target is the owner's tail two seconds after the return: the backlog
	// and the writes made since, which a replica applying no faster than the
	// writers write never reaches.
	time.Sleep(2 * time.Second)
	target := ownerLog.LastSequence()
	for {
		position := appliedSnapshot(restarted.server.core.cluster)[ownerID]
		if position >= target {
			elapsed := time.Since(returnedAt)
			t.Logf("the returned replica reached sequence %d of the owner's log %v after it came back (at least %.0f entries/s)",
				target, elapsed, float64(position)/elapsed.Seconds())
			return
		}
		if time.Since(returnedAt) > catchUpBy {
			t.Fatalf("the returned replica is at sequence %d of the owner's log %v after it came back, short of %d "+
				"(tail now %d): with a %v fsync it applies no faster than the writers write",
				position, catchUpBy, target, ownerLog.LastSequence(), slowFsync)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
