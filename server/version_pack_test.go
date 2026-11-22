package server

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 11.1 (dispatch 11): a version is (term, sequence), every live apply
// and every replicating writer stamps it, and the limits refuse.

// TestPackVersionOrdersTermsBeforeSequences pins the packing: a later term
// outranks any sequence of an earlier one; within a term the sequence orders;
// term 0 packs as term 1, above every bare sequence written before versions
// were packed (which is how the case of a copy carried from before the hand-off
// is pinned: no supported path carries such a directory); the largest version
// it packs still encodes as a BSON integer inside a consistent get's answer;
// and the limits refuse, naming themselves. Guards: pack the bare term (the
// term-0 case); give the term 20 bits (the encoding case).
func TestPackVersionOrdersTermsBeforeSequences(t *testing.T) {
	mustPack := func(term, seq uint64) uint64 {
		t.Helper()
		version, err := packVersion(term, seq)
		if err != nil {
			t.Fatalf("packVersion(%d, %d): %v", term, seq, err)
		}
		return version
	}
	if later, earlier := mustPack(2, 1), mustPack(1, 1<<44-1); later <= earlier {
		t.Errorf("(2, 1) packs to %d, not above (1, 2^44-1) at %d", later, earlier)
	}
	if next, prev := mustPack(1, 41), mustPack(1, 40); next <= prev {
		t.Errorf("(1, 41) packs to %d, not above (1, 40) at %d", next, prev)
	}
	for _, seq := range []uint64{0, 1, 600, 1<<44 - 1} {
		if zero, one := mustPack(0, seq), mustPack(1, seq); zero != one {
			t.Errorf("term 0 packs (0, %d) to %d and term 1 to %d: a copy in term 0 must pack as term 1", seq, zero, one)
		}
		if packed := mustPack(0, seq); packed <= 1<<44-1 {
			t.Errorf("(0, %d) packs to %d, not above every bare sequence below 2^44", seq, packed)
		}
	}

	termLimit, seqLimit := versionLimits()
	largest := mustPack(termLimit-1, seqLimit-1)
	encoded, err := bson.Marshal(ConsistentGetResponse{Found: true, Version: largest})
	if err != nil {
		t.Fatalf("the largest version, %d, does not encode inside a consistent get's answer: %v", largest, err)
	}
	var decoded ConsistentGetResponse
	if err := bson.Unmarshal(encoded, &decoded); err != nil || decoded.Version != largest {
		t.Fatalf("the largest version decodes as %d (%v), want %d", decoded.Version, err, largest)
	}

	for _, refused := range []struct {
		term, seq uint64
		names     string
	}{
		{term: 1 << 19, seq: 1, names: "524,287 membership changes"},
		{term: 1, seq: 1 << 44, names: "2^44 entries"},
	} {
		_, err := packVersion(refused.term, refused.seq)
		if !errors.Is(err, ErrVersionLimit) || err == nil || !strings.Contains(err.Error(), refused.names) {
			t.Errorf("packVersion(%d, %d) = %v; want ErrVersionLimit naming %q", refused.term, refused.seq, err, refused.names)
		}
	}
}

// versionsOf reads key's version side-key on each node in indexes, failing
// the test when a node does not hold the key.
func versionsOf(t *testing.T, tc *testCluster, bucket, key string, indexes []int) []uint64 {
	t.Helper()
	versions := make([]uint64, 0, len(indexes))
	for _, index := range indexes {
		versions = append(versions, versionOn(t, tc.nodes[index].server.core, testDatabase, bucket, key))
	}
	return versions
}

// requireOneVersionAbove fails unless every copy's version is the same, at or
// above 2^44, and above floor.
func requireOneVersionAbove(t *testing.T, versions []uint64, floor uint64, what string) {
	t.Helper()
	for _, version := range versions {
		if version != versions[0] || version < 1<<44 || version <= floor {
			t.Errorf("%s: the copies carry versions %v; want one version, at or above 2^44 (%d) and above %d", what, versions, uint64(1)<<44, floor)
			return
		}
	}
}

// TestEveryCopyOfAWriteCarriesTheSameVersion: a put at all leaves one version
// on every copy of a hash and a btree bucket, at or above 2^44. A replica that
// was stopped across the put and receives it from the owner's log carries the
// owner's version. After the owner is removed and the bucket taken over, the
// new owner's writes, an update of a key the old owner wrote among them, are
// above every version the old owner wrote. Guard: stamp a write's version from
// its sequence alone.
func TestEveryCopyOfAWriteCarriesTheSameVersion(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	buckets := []string{"hashversions", "btreeversions"}
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: buckets[0], Type: BucketTypeHash})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: buckets[1], Type: BucketTypeBTree})
	every := []int{0, 1, 2}
	for _, bucket := range buckets {
		waitForServing(t, tc, testDatabase, bucket, 3)
		requireEveryCopy(t, tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k",
			Data: map[string]interface{}{"n": 1}, WriteConcern: "all"}), "the put of k in "+bucket)
		requireOneVersionAbove(t, versionsOf(t, tc, bucket, "k", every), 0, "a put at all in "+bucket)
	}

	// From the log: a replica of both buckets, owner of neither, stopped
	// across the put.
	owner, replicas := ownerAndReplicas(t, tc, testDatabase, buckets[0])
	btreeOwner, _ := ownerAndReplicas(t, tc, testDatabase, buckets[1])
	stopped := replicas[0]
	if stopped == btreeOwner {
		stopped = replicas[1]
	}
	start := stopNode(t, tc, stopped)
	for _, bucket := range buckets {
		tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "logged",
			Data: map[string]interface{}{"n": 2}, WriteConcern: "majority"})
	}
	start()
	for _, bucket := range buckets {
		pollUntil(t, "the restarted replica receives the put from the owner's log", func() bool {
			found, _, _, err := tc.nodes[stopped].server.core.versionedFind(testDatabase, bucket, "logged")
			return err == nil && found
		})
		requireOneVersionAbove(t, versionsOf(t, tc, bucket, "logged", every), 0, "a put a replica received from the log in "+bucket)
	}

	// After an owner change.
	var oldVersions uint64
	for _, bucket := range buckets {
		for _, key := range []string{"k", "logged"} {
			oldVersions = max(oldVersions, versionOn(t, tc.nodes[owner].server.core, testDatabase, bucket, key))
		}
	}
	removeCommitted(t, tc, stopped, owner)
	members := map[string]*Server{}
	var live []int
	for _, index := range every {
		if index != owner {
			members[tc.addrs[index]] = tc.nodes[index].server.core
			live = append(live, index)
		}
	}
	var unsettled string
	if !holdsWithin(takeOverWait, func() bool {
		unsettled = handOffUnsettled(members, testDatabase, buckets)
		return unsettled == ""
	}) {
		t.Fatalf("the hand-off did not settle: %s", unsettled)
	}
	for _, bucket := range buckets {
		newOwner, _ := ownerAndReplicas(t, tc, testDatabase, bucket)
		if newOwner == owner {
			newOwner = live[0]
		}
		// Two members remain of a factor-3 set, so the writes are at majority
		// and every remaining copy is waited for.
		tc.ok(newOwner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "after",
			Data: map[string]interface{}{"n": 3}, WriteConcern: "majority"})
		pollUntil(t, "every copy applies the new owner's put", func() bool {
			for _, index := range live {
				if found, _, _, err := tc.nodes[index].server.core.versionedFind(testDatabase, bucket, "after"); err != nil || !found {
					return false
				}
			}
			return true
		})
		requireOneVersionAbove(t, versionsOf(t, tc, bucket, "after", live), oldVersions, "the new owner's put in "+bucket)
		tc.ok(newOwner, transport.OpUpdate, bson.M{"database": testDatabase, "bucket": bucket, "key": "k",
			"set": bson.M{"n": 4}, "write_concern": "majority"})
		pollUntil(t, "every copy applies the new owner's update", func() bool {
			for _, index := range live {
				_, data, _, err := tc.nodes[index].server.core.versionedFind(testDatabase, bucket, "k")
				if err != nil || docField(t, data, "n") != "4" {
					return false
				}
			}
			return true
		})
		requireOneVersionAbove(t, versionsOf(t, tc, bucket, "k", live), oldVersions, "the new owner's update of k in "+bucket)
	}
}

// writerRow is one replicating writer of a hash or btree document: what it
// sends, and the key whose version it moves.
type writerRow struct {
	name string
	key  string
	op   transport.Opcode
	req  func(bucket string) any
}

// TestEveryReplicatedHashAndBTreeWriterStampsAVersion: every writer that
// replicates a hash or btree document — insert, batch insert, update,
// update-many and push — leaves on every copy one version, at or above 2^44,
// above the version the key had before the write. A writer that stamped 0
// would leave the previous write's version in place (the store writes the
// side-key only for a version above 0). The writers are found with a grep of
// server/ for hashStore.Put, hashStore.PutBatch, btreeStore.Put and
// btreeStore.PutBatch; a writer added later belongs in this table. Guard:
// restore the literal 0 in Server.UpdateCtx's hash arm (the update row's
// version is the one read before it).
func TestEveryReplicatedHashAndBTreeWriterStampsAVersion(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	every := []int{0, 1, 2}
	rows := []writerRow{
		{name: "insert", key: "w", op: transport.OpPut, req: func(bucket string) any {
			return InsertRequest{Database: testDatabase, Bucket: bucket, Key: "w", Data: map[string]interface{}{"n": 1, "group": "g"}, WriteConcern: "all"}
		}},
		{name: "batch insert", key: "b", op: transport.OpBatchPut, req: func(bucket string) any {
			return BatchInsertRequest{Database: testDatabase, Bucket: bucket, WriteConcern: "all",
				Data: []map[string]interface{}{{"_id_": "b", "n": 1, "group": "g"}}}
		}},
		{name: "update", key: "w", op: transport.OpUpdate, req: func(bucket string) any {
			return bson.M{"database": testDatabase, "bucket": bucket, "key": "w", "set": bson.M{"n": 2}, "write_concern": "all"}
		}},
		{name: "update-many", key: "b", op: transport.OpUpdate, req: func(bucket string) any {
			return bson.M{"database": testDatabase, "bucket": bucket, "multi": true, "filter": bson.M{"group": "g"},
				"set": bson.M{"n": 3}, "write_concern": "all"}
		}},
		{name: "push", key: "p", op: transport.OpFIFOPush, req: func(bucket string) any {
			return PushRequest{Database: testDatabase, Bucket: bucket, Data: map[string]interface{}{"_id_": "p", "n": 1}, WriteConcern: "all"}
		}},
	}
	for _, bucketType := range []BucketType{BucketTypeHash, BucketTypeBTree} {
		bucket := fmt.Sprintf("writers%s", bucketType)
		// Whole: the update-many row is refused on a spread bucket (build step 12c).
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: bucketType,
			Options: map[string]interface{}{"placement": "whole"}})
		waitForServing(t, tc, testDatabase, bucket, 3)
		for _, row := range rows {
			before := make([]uint64, len(every))
			for i, index := range every {
				_, _, version, err := tc.nodes[index].server.core.versionedFind(testDatabase, bucket, row.key)
				if err != nil {
					t.Fatalf("%s %s: reading the version before the write on node %d: %v", bucketType, row.name, index+1, err)
				}
				before[i] = version
			}
			frame := tc.frame(0, row.op, row.req(bucket))
			if frame.Opcode == transport.OpError {
				t.Fatalf("%s %s: refused: %s", bucketType, row.name, frame.Payload)
			}
			var floor uint64
			for _, version := range before {
				floor = max(floor, version)
			}
			var after []uint64
			pollUntil(t, fmt.Sprintf("every copy applies the %s %s", bucketType, row.name), func() bool {
				after = after[:0]
				for _, index := range every {
					_, _, version, err := tc.nodes[index].server.core.versionedFind(testDatabase, bucket, row.key)
					if err != nil {
						return false
					}
					after = append(after, version)
				}
				return true
			})
			requireOneVersionAbove(t, after, floor, fmt.Sprintf("%s %s of %s (versions before: %v)", bucketType, row.name, row.key, before))
		}
	}
}

// TestAWriteIsRefusedPastTheVersionLimit: a write whose version cannot be
// packed is refused before it is logged, UNAVAILABLE and not_applied, naming
// the limit and the dump that gets past it, and no node's log grows. A term
// that cannot be packed is not opened: after a removal takes the epoch to the
// term limit, the moved bucket's new owner refuses to open it, says why in one
// WARN however often its worker retries, and refuses writes with the same
// text. The seam lowers the limits so real writes and a real membership change
// reach them. Guards: no refusal in applyAsOwner (a log grows); none in
// openTerm (no WARN names the refused term).
func TestAWriteIsRefusedPastTheVersionLimit(t *testing.T) {
	t.Run("sequence", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, testBucket, 3)
		requireEveryCopy(t, tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "fits",
			Data: map[string]interface{}{"n": 1}, WriteConcern: "all"}), "the put that fits")
		owner, _ := ownerAndReplicas(t, tc, testDatabase, testBucket)
		tails := tc.walSequences()
		limit := clusterOf(tc, owner).walLastSequence() + 1
		limits := func() (uint64, uint64) { return 1 << versionTermBits, limit }
		versionLimitsHook.Store(&limits)
		defer versionLimitsHook.Store(nil)

		frame := tc.frame(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "past",
			Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
		var refusal ErrorResponse
		if frame.Opcode != transport.OpError || bson.Unmarshal(frame.Payload, &refusal) != nil {
			t.Fatalf("a put past the sequence limit was answered %s: %s", frame.Opcode, frame.Payload)
		}
		if transport.ErrorCode(refusal.Code) != transport.ErrUnavailable || refusal.WriteOutcome != outcomeNotApplied {
			t.Errorf("the refusal has code %d and outcome %q, want UNAVAILABLE and not_applied", refusal.Code, refusal.WriteOutcome)
		}
		for _, want := range []string{"2^44 entries", "hoarddb dump", testBucket} {
			if !strings.Contains(refusal.Message, want) {
				t.Errorf("the refusal %q does not say %q", refusal.Message, want)
			}
		}
		time.Sleep(200 * time.Millisecond)
		if after := tc.walSequences(); fmt.Sprint(after) != fmt.Sprint(tails) {
			t.Errorf("the refused put grew the logs from %v to %v", tails, after)
		}
	})

	t.Run("term", func(t *testing.T) {
		recorder := recordClusterLogs(t)
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, testBucket, 3)
		requireEveryCopy(t, tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "fits",
			Data: map[string]interface{}{"n": 1}, WriteConcern: "all"}), "the put that fits")
		owner, replicas := ownerAndReplicas(t, tc, testDatabase, testBucket)
		limit := clusterOf(tc, owner).Epoch() + 1
		limits := func() (uint64, uint64) { return limit, 1 << versionSeqBits }
		versionLimitsHook.Store(&limits)
		defer versionLimitsHook.Store(nil)

		removeCommitted(t, tc, replicas[0], owner)
		newOwner, _ := ownerAndReplicas(t, tc, testDatabase, testBucket)
		if newOwner == owner {
			t.Fatalf("setup: node 1 still places %s on the removed node %d", testBucket, owner+1)
		}
		refusedTerm := func() []recordedEntry {
			var named []recordedEntry
			for _, entry := range recorder.matching(slog.LevelWarn, "cannot open a term") {
				if entry.attrs["bucket"] == testBucket && strings.Contains(entry.msg, "524,287 membership changes") &&
					strings.Contains(entry.msg, "hoarddb dump") {
					named = append(named, entry)
				}
			}
			return named
		}
		pollUntil(t, "the new owner says it cannot open the bucket's term", func() bool { return len(refusedTerm()) > 0 })
		// The worker retries every round while the bucket stays unsettled;
		// rounds come at least every few seconds this early.
		time.Sleep(4 * time.Second)
		if warned := len(refusedTerm()); warned != 1 {
			t.Errorf("the refused term was named in %d WARNs, want 1 within strayWarnInterval", warned)
		}
		if _, opened, err := tc.nodes[newOwner].server.core.bucketLineage(testDatabase, testBucket); err != nil || opened {
			t.Errorf("the new owner opened the bucket at the term limit (opened=%v, %v)", opened, err)
		}
		frame := tc.frame(newOwner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "past",
			Data: map[string]interface{}{"n": 1}, WriteConcern: "one"})
		var refusal ErrorResponse
		if frame.Opcode != transport.OpError || bson.Unmarshal(frame.Payload, &refusal) != nil {
			t.Fatalf("a put to the bucket that cannot open was answered %s: %s", frame.Opcode, frame.Payload)
		}
		for _, want := range []string{"524,287 membership changes", "hoarddb dump"} {
			if !strings.Contains(refusal.Message, want) {
				t.Errorf("the refusal %q does not say %q", refusal.Message, want)
			}
		}
	})
}
