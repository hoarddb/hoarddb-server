package server

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The owner's awaiting records (bucket-replication-rev8-spec §3.3, §5.2; build
// doc step 2.3).

// awaitingOnDisk is the members node's store holds an awaiting record for, for
// the bucket placed by key, sorted.
func awaitingOnDisk(t *testing.T, s *Server, key placementKey) []string {
	t.Helper()
	prefix := append(awaitingPrefix(), key.encoded...)
	snapshot := s.kv.NewSnapshot()
	defer snapshot.Discard()
	iterator := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyPrefixEnd(prefix)})
	defer iterator.Close()
	var members []string
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		_, member, err := parseAwaitingKey(iterator.Key()[len(awaitingPrefix()):])
		if err != nil {
			t.Fatalf("reading an awaiting record's key: %v", err)
		}
		members = append(members, member)
	}
	if err := iterator.Err(); err != nil {
		t.Fatalf("walking the awaiting records: %v", err)
	}
	slices.Sort(members)
	return members
}

// awaitingInMap is every member c's awaiting map holds for the bucket placed
// by key, whatever the record's term or generation, in no order.
func awaitingInMap(c *Cluster, key placementKey) []string {
	if c.awaitingCount.Load() == 0 {
		return nil
	}
	c.awaitingMu.Lock()
	defer c.awaitingMu.Unlock()
	members := make([]string, 0, len(c.awaiting[key]))
	for member := range c.awaiting[key] {
		members = append(members, member)
	}
	return members
}

// sortedAwaiting is the members the awaiting map holds for key, sorted.
func sortedAwaiting(c *Cluster, key placementKey) []string {
	members := awaitingInMap(c, key)
	slices.Sort(members)
	return members
}

// TestAnOwnerCrashBetweenAppendAndApplyOfAnIncreaseStillFiltersTheAwaitingMember
// is T-183 in its G2 form (build doc OQ-6). An owner whose append of an
// increase was synced and whose apply did not commit replays the alter at
// start by its own path, because the log says it originated it: the replay
// writes the awaiting records, and the map the write path reads holds the
// members once the node has started. The state is built directly: the alter
// is appended to the stopped owner's log and never applied. The routing half
// (G3, rule F5): a write of the bucket after the restart is sent to neither
// unseeded member, and the owner's streams to them do not stall.
func TestAnOwnerCrashBetweenAppendAndApplyOfAnIncreaseStillFiltersTheAwaitingMember(t *testing.T) {
	// Auto-rebalance off: the copy stays pending, which the pending-copy
	// worker would otherwise make at once.
	tc := newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	owner := tc.nodes[0].server.core
	key := placementOf(testDatabase, bucket)
	epoch := owner.cluster.Epoch()
	facts, _ := owner.peekSettled(testDatabase, bucket)
	want := mustReplicaIDsAt(t, owner.cluster, testDatabase, bucket, 3)[1:]
	slices.Sort(want)
	payload, err := bson.Marshal(AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3, PreviousFactor: 1,
		Epoch: epoch, Generation: facts.generation})
	if err != nil {
		t.Fatalf("encoding the alter: %v", err)
	}

	restarted := restartNodeAfterEditingItsWAL(t, tc, 0, func(wal *storage.WAL) {
		if _, err := wal.AppendFrom(bucket, uint8(transport.OpAlterBucket), nil, payload,
			storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: epoch}); err != nil {
			t.Fatalf("appending the alter to the owner's log: %v", err)
		}
	}).server.core

	if onDisk := awaitingOnDisk(t, restarted, key); !slices.Equal(onDisk, want) {
		t.Errorf("after the restart the owner's store holds awaiting records for %v, want %v", onDisk, want)
	}
	if inMemory := sortedAwaiting(restarted.cluster, key); !slices.Equal(inMemory, want) {
		t.Errorf("after the restart the owner's awaiting map holds %v, want %v", inMemory, want)
	}
	if facts, _ := restarted.peekSettled(testDatabase, bucket); facts.replicationFactor != 3 {
		t.Errorf("after the replay the owner's record declares %d, want 3", facts.replicationFactor)
	}

	putDocument(tc, 0, testDatabase, bucket, "after-the-restart")
	requireNothingOfTheBucketReachedTheAwaitingMembers(t, tc, bucket)
}

// TestAnOwnersReplayedAlterWhoseEpochLeftTheHistoryStillApplies is OQ-G3-2:
// an owner whose append of an increase was synced, whose apply did not commit,
// and whose membership history no longer holds the epoch the alter was logged
// under (more than 64 changes since, or a gap it learned past by gossip). The
// replay at start takes the owner's path, so the bucket's three stores take
// the new factor, the awaiting records name the members the current ring
// newly places, which is the set routing uses now, and one warning names the
// bucket and the missing epoch. The ring of epoch 0 is in no history. Without
// the fallback the replay fails in setAt before the store's gate and counts
// the entry skipped at Debug, and the catalog keeps the old factor while the
// log holds the alter.
func TestAnOwnersReplayedAlterWhoseEpochLeftTheHistoryStillApplies(t *testing.T) {
	// Auto-rebalance off: the copy stays pending, which the pending-copy
	// worker would otherwise make at once.
	tc := newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	owner := tc.nodes[0].server.core
	key := placementOf(testDatabase, bucket)
	const missingEpoch = InitialEpoch - 1
	if _, recorded := owner.cluster.ringAt(missingEpoch); recorded {
		t.Fatalf("the owner's membership history holds epoch %d; the test needs an epoch it lacks", missingEpoch)
	}
	facts, _ := owner.peekSettled(testDatabase, bucket)
	want := mustReplicaIDsAt(t, owner.cluster, testDatabase, bucket, 3)[1:]
	slices.Sort(want)
	payload, err := bson.Marshal(AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3, PreviousFactor: 1,
		Epoch: missingEpoch, Generation: facts.generation})
	if err != nil {
		t.Fatalf("encoding the alter: %v", err)
	}
	logged := &syncBuffer{}
	clusterTestLogger.Store(slog.New(slog.NewTextHandler(logged, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { clusterTestLogger.Store(nil) })

	restarted := restartNodeAfterEditingItsWAL(t, tc, 0, func(wal *storage.WAL) {
		if _, err := wal.AppendFrom(bucket, uint8(transport.OpAlterBucket), nil, payload,
			storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: missingEpoch}); err != nil {
			t.Fatalf("appending the alter to the owner's log: %v", err)
		}
	}).server.core

	requireHeldAt(t, restarted, bucket, 3, "after the owner replayed an alter whose epoch left its history")
	if onDisk := awaitingOnDisk(t, restarted, key); !slices.Equal(onDisk, want) {
		t.Errorf("after the restart the owner's store holds awaiting records for %v, want %v (the current ring's newly placed members)", onDisk, want)
	}
	if inMemory := sortedAwaiting(restarted.cluster, key); !slices.Equal(inMemory, want) {
		t.Errorf("after the restart the owner's awaiting map holds %v, want %v", inMemory, want)
	}
	var warnings []string
	for _, line := range strings.Split(logged.String(), "\n") {
		if strings.Contains(line, "level=WARN") && strings.Contains(line, "bucket="+bucket) &&
			strings.Contains(line, fmt.Sprintf("missing_epoch=%d", missingEpoch)) {
			warnings = append(warnings, line)
		}
	}
	if len(warnings) != 1 {
		t.Errorf("the owner's replay logged %d warnings naming bucket %q and missing epoch %d, want 1; the warnings it logged:\n%s",
			len(warnings), bucket, missingEpoch, logged.String())
	}
}

// TestAnOwnersReplayedAlterWhoseEpochLeftTheHistoryStillShipsToTheMembersItNewlyPlaces
// is OQ-G3-2 with a real epoch: the bucket is created at epoch E, a membership
// change moves the cluster to E+1 with the owner unchanged, and E is then
// forgotten by every survivor's history (what the 64-epoch trim does). The
// owner's log is given the increase as it would have logged it at E, and it
// restarts. The owner applies at the current ring; the members it newly places
// must receive the alter (it ships: its origin epoch is at or past the term)
// and write their needs-a-copy records. Without the fallback the replay fails
// before the store's gate; with one that keeps the logged epoch, the awaiting
// records are right but the alter is never sent.
func TestAnOwnersReplayedAlterWhoseEpochLeftTheHistoryStillShipsToTheMembersItNewlyPlaces(t *testing.T) {
	tc := newTestClusterWith(t, 4, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	owner := tc.nodes[0].server.core
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	putDocument(tc, 0, testDatabase, bucket, "before")
	logged := owner.cluster.Epoch()
	facts, _ := owner.peekSettled(testDatabase, bucket)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := owner.cluster.proposeRemoval(ctx, tc.addrs[3]); err != nil {
		t.Fatalf("removing node 4: %v", err)
	}
	if now := owner.cluster.Epoch(); now != logged+1 {
		t.Fatalf("epoch after the removal is %d, want %d", now, logged+1)
	}
	if o, err := owner.cluster.Owner(placementOf(testDatabase, bucket)); err != nil || o.ID != tc.addrs[0] {
		t.Skipf("the removal moved %q's owner to %v (%v); rerun", bucket, o.ID, err)
	}
	for i := 1; i <= 2; i++ {
		c := clusterOf(tc, i)
		pollUntil(t, "a survivor adopts the removal", func() bool { return c.Epoch() == logged+1 })
	}
	for i := 0; i <= 2; i++ {
		if err := clusterOf(tc, i).forgetMembership(logged); err != nil {
			t.Fatalf("forgetting epoch %d on node %d: %v", logged, i+1, err)
		}
	}
	want := mustReplicaIDsAt(t, owner.cluster, testDatabase, bucket, 3)[1:]
	slices.Sort(want)
	payload, err := bson.Marshal(AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3, PreviousFactor: 1,
		Epoch: logged, Generation: facts.generation})
	if err != nil {
		t.Fatalf("encoding the alter: %v", err)
	}
	restarted := restartNodeAfterEditingItsWAL(t, tc, 0, func(wal *storage.WAL) {
		if _, err := wal.AppendFrom(bucket, uint8(transport.OpAlterBucket), nil, payload,
			storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: logged}); err != nil {
			t.Fatalf("appending the alter: %v", err)
		}
	}).server.core
	if _, recorded := restarted.cluster.ringAt(logged); recorded {
		t.Fatalf("setup: the restarted owner still remembers epoch %d", logged)
	}
	requireHeldAt(t, restarted, bucket, 3, "after the owner replayed the alter")
	key := placementOf(testDatabase, bucket)
	if onDisk := awaitingOnDisk(t, restarted, key); !slices.Equal(onDisk, want) {
		t.Errorf("the owner's awaiting records name %v, want %v", onDisk, want)
	}
	for _, id := range want {
		index := nodeIndexByAddr(t, tc.addrs, id)
		member := tc.nodes[index].server.core
		if !eventually(func() bool { _, found := needsCopyOnDisk(t, member, bucket); return found }) {
			t.Errorf("node %d, which the owner's replayed alter newly places (awaiting record written), never wrote a needs-a-copy record: it was not sent the alter", index+1)
		}
	}
}

// TestAnOwnersAnswerToAnAlterWhoseEpochLeftTheHistoryNamesTheCurrentRingsMembers
// pins OQ-G3-2's other call site: alterAsOwner answers with the members the
// alter newly places at the epoch it placed them by, not the epoch it was
// logged under. Handed the logged alter, alterAnswerOf asks for a ring the
// history no longer holds and the replay's answer is an error after the alter
// has been applied.
func TestAnOwnersAnswerToAnAlterWhoseEpochLeftTheHistoryNamesTheCurrentRingsMembers(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	owner := tc.nodes[0].server.core
	const missingEpoch = InitialEpoch - 1
	if _, recorded := owner.cluster.ringAt(missingEpoch); recorded {
		t.Fatalf("the owner's membership history holds epoch %d; the test needs an epoch it lacks", missingEpoch)
	}
	facts, _ := owner.peekSettled(testDatabase, bucket)
	want := mustReplicaIDsAt(t, owner.cluster, testDatabase, bucket, 3)[1:]
	slices.Sort(want)

	answer, err := owner.alterAsOwner(withLineage(context.Background(), 1, true), AlterBucketRequest{Database: testDatabase, Name: bucket,
		ReplicationFactor: 3, PreviousFactor: 1, Epoch: missingEpoch, Generation: facts.generation})
	if err != nil {
		t.Fatalf("the owner's apply of an alter whose epoch left its history failed: %v", err)
	}
	pending := slices.Clone(answer.Pending)
	slices.Sort(pending)
	if !slices.Equal(pending, want) {
		t.Errorf("the owner's answer names %v as pending, want the current ring's newly placed members %v", pending, want)
	}
}

// TestAnOwnerRestartReloadsItsAwaitingCopies: the awaiting map lives in
// memory only, so an owner that restarts after an applied increase rebuilds it
// from its 'a' records at start (spec §3.3), and R8 still refuses an alter
// that would place a member whose copy is pending. Without the start-up load
// the records stay on disk, the map is empty, and the alter is let through.
func TestAnOwnerRestartReloadsItsAwaitingCopies(t *testing.T) {
	// Auto-rebalance off: the copy stays pending, which the pending-copy
	// worker would otherwise make at once.
	tc := newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	key := placementOf(testDatabase, bucket)
	want := mustReplicaIDsAt(t, clusterOf(tc, 0), testDatabase, bucket, 3)[1:]
	slices.Sort(want)

	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))
	if pending := sortedAwaiting(clusterOf(tc, 0), key); !slices.Equal(pending, want) {
		t.Fatalf("after the increase the owner's awaiting map holds %v, want %v", pending, want)
	}

	restarted := tc.restartNode(0).server.core
	if onDisk := awaitingOnDisk(t, restarted, key); !slices.Equal(onDisk, want) {
		t.Errorf("after the restart the owner's store holds awaiting records for %v, want %v", onDisk, want)
	}
	if inMemory := sortedAwaiting(restarted.cluster, key); !slices.Equal(inMemory, want) {
		t.Fatalf("after the restart the owner's awaiting map holds %v, want %v", inMemory, want)
	}
	waitForAlterFeature(t, tc)
	// An alter to 2, which the switch lets lower the factor, still places the
	// second member of the walk, whose copy is pending.
	requireUnavailableNotApplied(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2}),
		"is still copying to")
}

// TestAnAlterReplayIsIdempotentAndNotAppliedToARecreatedBucket is T-166. An
// alter logged against one incarnation of a bucket must not reach the bucket
// re-created under its name when the log is replayed, and replaying twice
// changes nothing. It is built with a declaration (p == n) so that no
// awaiting record is left behind: the drop's delete of them is G3's (OQ-10).
func TestAnAlterReplayIsIdempotentAndNotAppliedToARecreatedBucket(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 1}))
	if facts, _ := tc.nodes[0].server.core.peekSettled(testDatabase, bucket); facts.replicationFactor != 1 {
		t.Fatalf("the declaration did not apply: the record declares %d", facts.replicationFactor)
	}
	tc.ok(0, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: bucket})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})

	for restart := 1; restart <= 2; restart++ {
		core := tc.restartNode(0).server.core
		facts, _ := core.peekSettled(testDatabase, bucket)
		entry, _, err := core.readCatalogEntry(testDatabase, bucket)
		if err != nil {
			t.Fatalf("reading the re-created bucket's catalog entry: %v", err)
		}
		if facts.replicationFactor != 0 || entry.ReplicationFactor != 0 {
			t.Fatalf("after restart %d the re-created bucket declares %d (catalog %d): the old incarnation's alter was replayed onto it",
				restart, facts.replicationFactor, entry.ReplicationFactor)
		}
		if declared, registered := registeredFactor(core.cluster, testDatabase, bucket); registered {
			t.Fatalf("after restart %d the registry holds factor %d for the re-created bucket, which declares none", restart, declared)
		}
	}
}

// countingBatch counts what one alter's apply writes.
type countingBatch struct {
	storage.KVBatch
	writes, bytes int
	keys          [][]byte
}

func (b *countingBatch) Set(key, value []byte) error {
	b.writes++
	b.bytes += len(key) + len(value)
	b.keys = append(b.keys, append([]byte(nil), key...))
	return b.KVBatch.Set(key, value)
}

func (b *countingBatch) Delete(key []byte) error {
	b.writes++
	b.bytes += len(key)
	b.keys = append(b.keys, append([]byte(nil), key...))
	return b.KVBatch.Delete(key)
}

// TestAnIncreasesBatchDoesNotGrowWithTheOtherBuckets is the owner's half of
// rule 11's measurement (spec §13, T-43's shape; build doc §3 and step 2.3):
// one increase writes its catalog entry and one awaiting record per member it
// newly places, n − p of them, and what it writes is the same with 100,000
// other buckets on the node as with none. It also reports, measured, the bytes
// of one awaiting record and of the alter's log entry.
func TestAnIncreasesBatchDoesNotGrowWithTheOtherBuckets(t *testing.T) {
	if testing.Short() {
		t.Skip("creates 100,000 buckets")
	}
	// Nothing moves the other buckets: they are created on one node only, and
	// with no membership change and no rebalance they stay where they are.
	tc := newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	first := bucketOwnedBy(t, tc, 0, "sessions")
	second := bucketOwnedBy(t, tc, 0, "accounts")
	for _, bucket := range []string{first, second} {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	}
	owner := tc.nodes[0].server.core

	measure := func(bucket string) *countingBatch {
		t.Helper()
		var counted *countingBatch
		hook := func(b storage.KVBatch) storage.KVBatch {
			counted = &countingBatch{KVBatch: b}
			return counted
		}
		alterBatchHook.Store(&hook)
		defer alterBatchHook.Store(nil)
		alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))
		if counted == nil {
			t.Fatal("the alter's apply wrote through no batch")
		}
		return counted
	}
	alone := measure(first)

	const others = 100_000
	if created, err := owner.CreateDatabase("others"); err != nil || !created.Success {
		t.Fatalf("creating the other buckets' database: %+v, %v", created, err)
	}
	started := time.Now()
	for i := 0; i < others; i++ {
		if created, err := owner.CreateBucket(CreateBucketRequest{Database: "others", Name: fmt.Sprintf("other-%06d", i), Type: BucketTypeHash}); err != nil || !created.Success {
			t.Fatalf("creating other bucket %d: %+v, %v", i, created, err)
		}
	}
	t.Logf("created %d other buckets in %s", others, time.Since(started).Round(time.Millisecond))
	crowded := measure(second)

	// The catalog entry and n − p = 2 awaiting records.
	if alone.writes != 3 {
		t.Errorf("an increase from 1 to 3 wrote %d keys in its batch, want 3 (its catalog entry and 2 awaiting records)", alone.writes)
	}
	if crowded.writes != alone.writes || crowded.bytes != alone.bytes {
		t.Errorf("with %d other buckets the increase wrote %d keys and %d bytes; with none, %d keys and %d bytes",
			others, crowded.writes, crowded.bytes, alone.writes, alone.bytes)
	}
	record, err := bson.Marshal(awaitingRecord{Term: 1, AlterSeq: 1, Generation: 1, Since: time.Now().Unix()})
	if err != nil {
		t.Fatalf("encoding an awaiting record: %v", err)
	}
	logged := walPayloadSize(t, owner.wal, transport.OpAlterBucket, second)
	t.Logf("one increase from 1 to 3: %d keys, %d bytes of keys and values in its batch, alone and beside %d buckets; "+
		"one awaiting record's value is %d bytes, its key %d; the alter's log entry payload is %d bytes with a %d-byte database and a %d-byte bucket name",
		alone.writes, alone.bytes, others, len(record), len(awaitingKey(placementOf(testDatabase, second), owner.cluster.self.ID)),
		logged, len(testDatabase), len(second))
	if len(record) != 73 {
		t.Errorf("an awaiting record's value is %d bytes, spec §13 says 73", len(record))
	}
	if want := 111 + len(testDatabase) + len(second); logged != want {
		t.Errorf("the alter's logged payload is %d bytes, spec §13 says 111 plus the names, %d", logged, want)
	}
}

// walPayloadSize is the payload length of the last entry of op for bucket in
// the log.
func walPayloadSize(t *testing.T, wal *storage.WAL, op transport.Opcode, bucket string) int {
	t.Helper()
	entries, err := wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading the WAL: %v", err)
	}
	size := -1
	for _, entry := range entries {
		if transport.Opcode(entry.Op) == op && entry.Bucket == bucket {
			size = len(entry.Data)
		}
	}
	if size < 0 {
		t.Fatalf("no %s entry of bucket %q in the log", op, bucket)
	}
	return size
}
