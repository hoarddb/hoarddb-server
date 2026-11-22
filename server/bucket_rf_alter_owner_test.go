package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The owner's path of the factor alter (bucket-replication-rev8-spec §5.2,
// build doc step 2.2). No write follows an increase in these tests: until G3
// filters a newly placed member out of the bucket's data entries, it refuses
// them and the owner's stream to it stalls (build doc §2 trap 10).

// alterFrame sends an alter to node index through its cluster's handler with
// no session, which is how a member's forwarded frame reaches it: the
// authorization gate is not run (TestAlterBucketRequiresDBAdmin runs it).
func alterFrame(t *testing.T, tc *testCluster, index int, req AlterBucketRequest) *transport.Frame {
	t.Helper()
	payload, err := bson.Marshal(req)
	if err != nil {
		t.Fatalf("encoding the alter: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	frame, err := tc.nodes[index].server.core.cluster.handleClientFrame(ctx, transport.NewFrame(transport.OpAlterBucket, payload))
	if err != nil {
		t.Fatalf("node %d's handler failed the alter: %v", index+1, err)
	}
	return frame
}

// alterAccepted is the answer to an alter the node accepted; a refusal fails
// the test.
func alterAccepted(t *testing.T, frame *transport.Frame) AlterBucketResponse {
	t.Helper()
	if frame.Opcode == transport.OpError {
		t.Fatalf("the alter was refused: %s", frame.Payload)
	}
	var answer AlterBucketResponse
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
		t.Fatalf("decoding the alter's answer: %v", err)
	}
	if !answer.Success {
		t.Fatalf("the alter was refused: %s", answer.Message)
	}
	return answer
}

// waitForWALCount waits until node index's log holds want entries of op for
// bucket.
func waitForWALCount(t *testing.T, tc *testCluster, index int, op transport.Opcode, bucket string, want int) {
	t.Helper()
	waitForWALCountWithin(t, tc, index, op, bucket, want, 20*time.Second)
}

// waitForWALCountWithin is waitForWALCount with a budget of its own, for a
// wait on a host the gates have oversubscribed. Each report, when given, adds
// what the other side was doing to the timeout's message, so a failure says
// whether the entry was never shipped or shipped late.
func waitForWALCountWithin(t *testing.T, tc *testCluster, index int, op transport.Opcode, bucket string, want int, budget time.Duration, report ...func() string) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for {
		got := walOpCount(t, tc.nodes[index].server.core.wal, op, bucket)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			message := fmt.Sprintf("node %d's log holds %d %s entries of bucket %q, want %d", index+1, got, op, bucket, want)
			for _, describe := range report {
				message += "; " + describe()
			}
			t.Fatalf("%s", message)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitForBarrier creates a database through node index and waits until every
// node in indexes has logged it. A database create is shipped on every
// member's stream in sequence order, so a node that logged it has been sent
// everything logged before it.
func waitForBarrier(t *testing.T, tc *testCluster, index int, name string, indexes ...int) {
	t.Helper()
	tc.ok(index, transport.OpCreateDB, CreateDatabaseRequest{Name: name})
	for _, member := range indexes {
		waitForWALCount(t, tc, member, transport.OpCreateDB, "", walOpCount(t, tc.nodes[index].server.core.wal, transport.OpCreateDB, ""))
	}
}

// loggedAlter is the alter of bucket as node index's log holds it.
func loggedAlter(t *testing.T, tc *testCluster, index int, bucket string) AlterBucketRequest {
	t.Helper()
	entries, err := tc.nodes[index].server.core.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading node %d's WAL: %v", index+1, err)
	}
	for _, entry := range entries {
		if transport.Opcode(entry.Op) == transport.OpAlterBucket && entry.Bucket == bucket {
			logged, err := decodeAlter(entry.Data)
			if err != nil {
				t.Fatalf("decoding the logged alter: %v", err)
			}
			return logged
		}
	}
	t.Fatalf("node %d logged no alter of bucket %q", index+1, bucket)
	return AlterBucketRequest{}
}

// TestAnAlterIsDeliveredToTheUnionOfTheSetsBeforeAndAfter is T-53's
// increase cases (spec §5.1): the alter is addressed to deliveryTargets at
// max(previous_factor, replication_factor), so a member the increase newly
// places, which holds nothing, receives it, and a member outside both sets
// does not. Each node's own log is read.
func TestAnAlterIsDeliveredToTheUnionOfTheSetsBeforeAndAfter(t *testing.T) {

	t.Run("1 to 3 on nodes at default 1", func(t *testing.T) {
		tc := newTestCluster(t, 3, 1)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		bucket := bucketOwnedBy(t, tc, 0, "sessions")
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})

		answer := alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))
		if len(answer.Pending) != 2 {
			t.Fatalf("an increase from 1 to 3 names %v as pending, want the two members it newly places", answer.Pending)
		}
		for index := range tc.nodes {
			waitForWALCount(t, tc, index, transport.OpAlterBucket, bucket, 1)
		}
	})

	t.Run("1 to 2 on nodes at default 3", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		bucket := bucketOwnedBy(t, tc, 0, "sessions")
		createBucketAt(tc, 0, testDatabase, bucket, 1)
		walk := mustReplicaIDsAt(t, tc.nodes[0].server.core.cluster, testDatabase, bucket, 3)
		second, third := nodeIndexByAddr(t, tc.addrs, walk[1]), nodeIndexByAddr(t, tc.addrs, walk[2])

		alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2}))
		waitForWALCount(t, tc, second, transport.OpAlterBucket, bucket, 1)
		waitForBarrier(t, tc, 0, "barrier", third)
		if logged := walOpCount(t, tc.nodes[third].server.core.wal, transport.OpAlterBucket, bucket); logged != 0 {
			t.Fatalf("node %d, in neither set, logged %d alters of bucket %q", third+1, logged, bucket)
		}
		if tc.nodes[third].server.core.bucketStoreOpen(testDatabase, bucket) {
			t.Fatalf("node %d, in neither set, holds bucket %q", third+1, bucket)
		}
	})

	// The decrease: the demoted members are in the set before, so they are
	// sent it.
	t.Run("3 to 1 on nodes at default 3", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		bucket := bucketOwnedBy(t, tc, 0, "sessions")
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, bucket, 3)

		answer := alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 1}))
		if len(answer.Releasing) != 2 {
			t.Fatalf("a decrease from 3 to 1 names %v as releasing, want the two members it demotes", answer.Releasing)
		}
		for index := range tc.nodes {
			waitForWALCount(t, tc, index, transport.OpAlterBucket, bucket, 1)
		}
	})
}

// mustReplicaIDsAt is the bucket's set at factor on c's current ring, owner
// first.
func mustReplicaIDsAt(t *testing.T, c *Cluster, database, bucket string, factor int) []string {
	t.Helper()
	set, err := c.replicasAtFactor(placementOf(database, bucket), factor)
	if err != nil {
		t.Fatalf("placing %s.%s at %d: %v", database, bucket, factor, err)
	}
	return nodeIDs(set)
}

// TestTheOwnersStampReplacesAClientsPreviousFactorAndEpoch is T-167: the
// three fields every member decides from are the owner's word, read under the
// alter's fence, whatever a client sent under their names (spec §3.2).
func TestTheOwnersStampReplacesAClientsPreviousFactorAndEpoch(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	owner := tc.nodes[0].server.core
	facts, _ := owner.peekSettled(testDatabase, bucket)

	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2,
		PreviousFactor: 9, Epoch: 999, Generation: 777}))

	logged := loggedAlter(t, tc, 0, bucket)
	if logged.PreviousFactor != 1 {
		t.Errorf("the logged alter carries previous_factor %d, want the bucket's factor 1", logged.PreviousFactor)
	}
	if logged.Epoch != owner.cluster.Epoch() {
		t.Errorf("the logged alter carries epoch %d, want the owner's %d", logged.Epoch, owner.cluster.Epoch())
	}
	if logged.Generation != facts.generation {
		t.Errorf("the logged alter carries generation %d, want the bucket's %d", logged.Generation, facts.generation)
	}
	if logged.ReplicationFactor != 2 {
		t.Errorf("the logged alter asks for %d copies, want 2", logged.ReplicationFactor)
	}
}

// TestAnAltersCatalogRewriteKeepsStoresIndexesAndFieldTypes is T-168 (spec
// §3.1, §5.2 step 4 and 5; RF-2; build doc OQ-9): the alter rewrites the catalog
// entry it reads, so stores, indexes and typed fields survive it; and on the
// owner and on a member that holds the bucket, the three places the factor is
// held agree after it, the snapshot a puller is sent ships the new factor, and
// the question a take-over asks is answered with the record's factor.
func TestAnAltersCatalogRewriteKeepsStoresIndexesAndFieldTypes(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketOwnedBy(t, tc, 0, "profiles")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash, ReplicationFactor: 2,
		Options: map[string]interface{}{
			"stores":  []interface{}{map[string]interface{}{"name": "Items", "engine": "fifoStore"}},
			"indices": []interface{}{"Email"},
			"fields": []interface{}{
				map[string]interface{}{"name": "Name", "type": "string", "required": true},
				map[string]interface{}{"name": "Age", "type": "int"},
			},
			"field_types": "warn",
		}})
	waitForServing(t, tc, testDatabase, bucket, 2)
	member := nodeIndexByAddr(t, tc.addrs, mustReplicaIDsAt(t, tc.nodes[0].server.core.cluster, testDatabase, bucket, 2)[1])
	holders := []int{0, member}
	before := make(map[int]catalogEntry, len(holders))
	for _, index := range holders {
		entry, _, err := tc.nodes[index].server.core.readCatalogEntry(testDatabase, bucket)
		if err != nil {
			t.Fatalf("reading node %d's catalog entry: %v", index+1, err)
		}
		before[index] = entry
	}

	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))
	waitForWALCount(t, tc, member, transport.OpAlterBucket, bucket, 1)

	key := placementOf(testDatabase, bucket)
	for _, index := range holders {
		core := tc.nodes[index].server.core
		deadline := time.Now().Add(10 * time.Second)
		for facts, _ := core.peekSettled(testDatabase, bucket); facts.replicationFactor != 3; facts, _ = core.peekSettled(testDatabase, bucket) {
			if time.Now().After(deadline) {
				t.Fatalf("node %d's record holds factor %d after the alter, want 3", index+1, facts.replicationFactor)
			}
			time.Sleep(20 * time.Millisecond)
		}
		entry, _, err := core.readCatalogEntry(testDatabase, bucket)
		if err != nil {
			t.Fatalf("reading node %d's catalog entry: %v", index+1, err)
		}
		was := before[index]
		switch {
		case entry.ReplicationFactor != 3:
			t.Errorf("node %d's catalog entry holds factor %d, want 3", index+1, entry.ReplicationFactor)
		case len(entry.Stores) != len(was.Stores) || len(entry.Stores) != 1:
			t.Errorf("node %d's catalog entry lost its store fields: %d, want %d", index+1, len(entry.Stores), len(was.Stores))
		case len(entry.Indices) != len(was.Indices) || len(entry.Indices) != 1:
			t.Errorf("node %d's catalog entry lost its indexes: %d, want %d", index+1, len(entry.Indices), len(was.Indices))
		case len(entry.Fields) != len(was.Fields) || len(entry.Fields) != 2 || entry.FieldTypes != "warn":
			t.Errorf("node %d's catalog entry lost its typed fields: %d fields in mode %q", index+1, len(entry.Fields), entry.FieldTypes)
		case entry.ID != was.ID || entry.Slot != was.Slot || entry.DatabaseGeneration != was.DatabaseGeneration || !entry.CreatedAt.Equal(was.CreatedAt):
			t.Errorf("node %d's catalog entry changed its identity: %+v, was %+v", index+1, entry, was)
		}
		if factor := core.cluster.effectiveRF(key); factor != 3 {
			t.Errorf("node %d's registry places the bucket at %d, want 3", index+1, factor)
		}
		snapshot, err := core.cluster.snapshotBucket(testDatabase, bucket, "")
		if err != nil {
			t.Fatalf("node %d's snapshot: %v", index+1, err)
		}
		if snapshot.replicationFactor != 3 {
			t.Errorf("node %d ships factor %d to a puller, want 3", index+1, snapshot.replicationFactor)
		}
		// The answer reads the record: made to differ from the registry, the
		// two must not be confused.
		core.cluster.recordBucketFactor(key, 1)
		answer, err := core.cluster.answerCopy(ReplBucketRef{Database: testDatabase, Bucket: bucket})
		core.cluster.recordBucketFactor(key, 3)
		if err != nil {
			t.Fatalf("node %d's answer for its copy: %v", index+1, err)
		}
		if answer.Factor != 3 {
			t.Errorf("node %d answers that its copy declares factor %d, want its record's 3", index+1, answer.Factor)
		}
	}
}

// TestAnAlterToTheDefaultOnAnInheritingBucketDeclaresIt is T-189 (owner
// R9-Q7): on a bucket that inherits 3, an alter to 3 is a change, inherit to
// declared. It is logged and answered as a declaration, the listing no longer
// says inherited, and a restart with another default keeps 3.
func TestAnAlterToTheDefaultOnAnInheritingBucketDeclaresIt(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)

	answer := alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))
	if !strings.Contains(answer.Message, "now declares 3 copies (it inherited 3") {
		t.Errorf("the alter to the inherited factor was not answered as a declaration: %q", answer.Message)
	}
	if logged := walOpCount(t, tc.nodes[0].server.core.wal, transport.OpAlterBucket, bucket); logged != 1 {
		t.Fatalf("the owner logged %d alters, want 1: a declaration is a change", logged)
	}
	var info BucketInfoResponse
	if err := bson.Unmarshal(tc.ok(0, transport.OpBucketInfo, BucketInfoRequest{Database: testDatabase, Bucket: bucket}).Payload, &info); err != nil {
		t.Fatalf("decoding the bucket's listing row: %v", err)
	}
	if info.Bucket.Inherited || info.Bucket.ReplicationFactor != 3 {
		t.Errorf("the listing row reads factor %d, inherited %v, want 3 declared", info.Bucket.ReplicationFactor, info.Bucket.Inherited)
	}

	tc.configure = func(index int, cfg *config.Config) {
		if index == 0 {
			cfg.Server.ReplicationFactor = 1
		}
	}
	restarted := tc.restartNode(0).server.core
	if facts, _ := restarted.peekSettled(testDatabase, bucket); facts.replicationFactor != 3 {
		t.Errorf("after a restart at default 1 the bucket's record declares %d, want 3", facts.replicationFactor)
	}
	if factor := restarted.cluster.effectiveRF(placementOf(testDatabase, bucket)); factor != 3 {
		t.Errorf("after a restart at default 1 the bucket is placed at %d, want 3", factor)
	}
}

// TestAFactorChangeKeepsTheBucketsIdSlotAndDocuments is T-14's up half: a
// freeze. The alter changes the factor and nothing of the bucket's identity or
// contents (spec §10.5); G5 adds the down half.
func TestAFactorChangeKeepsTheBucketsIdSlotAndDocuments(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	keys := []string{"a", "b", "c"}
	for _, key := range keys {
		putDocument(tc, 0, testDatabase, bucket, key)
	}
	owner := tc.nodes[0].server.core
	before, _ := owner.peekSettled(testDatabase, bucket)

	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))

	after, _ := owner.peekSettled(testDatabase, bucket)
	if after.id != before.id || after.slot != before.slot {
		t.Fatalf("the alter moved the bucket from id %d slot %d to id %d slot %d", before.id, before.slot, after.id, after.slot)
	}
	for _, key := range keys {
		var found FindResponse
		if err := bson.Unmarshal(tc.ok(0, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: key}).Payload, &found); err != nil {
			t.Fatalf("decoding the read of %q: %v", key, err)
		}
		if !found.Found {
			t.Errorf("document %q is gone after the alter", key)
		}
	}
}

// TestAlterBucketEmitsSchemaAlter is T-169's audit row: the owner's apply of
// an alter is a schema change, recorded with the old and the new factor.
func TestAlterBucketEmitsSchemaAlter(t *testing.T) {
	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	tc := newTestClusterWith(t, 3, 1, func(index int, cfg *config.Config) {
		if index == 0 {
			cfg.Audit.Enabled = true
			cfg.Audit.Output = auditPath
		}
	})
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})

	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))

	deadline := time.Now().Add(10 * time.Second)
	for {
		recorded, err := os.ReadFile(auditPath)
		if err != nil {
			t.Fatalf("reading the audit log: %v", err)
		}
		for _, line := range strings.Split(string(recorded), "\n") {
			if strings.Contains(line, `"event":"schema.alter"`) && strings.Contains(line, bucket) &&
				strings.Contains(line, "replication_factor 3 (was 1)") {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no schema.alter entry with the old and new factor for bucket %q; the log holds %q", bucket, recorded)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestAnAlterWaitsOutInFlightWritesAndNoWriteStraddlesIt is T-160 (spec §6.2
// rule F2, §9): the alter takes the bucket's fence exclusively, so a write
// already inside it is applied before the alter is logged, and the alter is
// not logged while that write is held. Every wait has a deadline, so a fence
// taken twice fails the test instead of hanging it (build doc §2 trap 6).
func TestAnAlterWaitsOutInFlightWritesAndNoWriteStraddlesIt(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	owner := tc.nodes[0].server.core.cluster

	held, release := make(chan struct{}), make(chan struct{})
	var taken atomic.Bool
	hook := func(nodeID string, repl ReplEntry) {
		if nodeID != owner.self.ID || transport.Opcode(repl.Op) != transport.OpPut || repl.Bucket != bucket || !taken.CompareAndSwap(false, true) {
			return
		}
		close(held)
		<-release
	}
	beforeLocalApplyHook.Store(&hook)
	t.Cleanup(func() { beforeLocalApplyHook.Store(nil) })

	write := sendInBackground(t, owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "w", Data: map[string]interface{}{"n": 1}})
	select {
	case <-held:
	case <-time.After(20 * time.Second):
		t.Fatal("the write never reached its local apply")
	}
	alter := sendInBackground(t, owner, transport.OpAlterBucket, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3})

	time.Sleep(500 * time.Millisecond)
	if logged := walOpCount(t, tc.nodes[0].server.core.wal, transport.OpAlterBucket, bucket); logged != 0 {
		close(release)
		t.Fatalf("the alter was logged while a write of the bucket was inside its fence: the write straddles it")
	}
	select {
	case answer := <-alter:
		close(release)
		t.Fatalf("the alter was answered while a write of the bucket was inside its fence: %+v", answer)
	default:
	}
	close(release)
	for _, answered := range []<-chan backgroundAnswer{write, alter} {
		select {
		case answer := <-answered:
			if answer.err != nil || answer.frame.Opcode == transport.OpError {
				t.Fatalf("a request failed: %v %s", answer.err, answer.payload())
			}
		case <-time.After(20 * time.Second):
			t.Fatal("a request held across the alter never finished: the fence was taken twice")
		}
	}
	entries, err := tc.nodes[0].server.core.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading the owner's WAL: %v", err)
	}
	var writeSeq, alterSeq uint64
	for _, entry := range entries {
		switch {
		case entry.Bucket == bucket && transport.Opcode(entry.Op) == transport.OpPut:
			writeSeq = entry.Seq
		case entry.Bucket == bucket && transport.Opcode(entry.Op) == transport.OpAlterBucket:
			alterSeq = entry.Seq
		}
	}
	if writeSeq == 0 || alterSeq == 0 || writeSeq > alterSeq {
		t.Fatalf("the owner logged the write at %d and the alter at %d; the write admitted first must come first", writeSeq, alterSeq)
	}
}

// backgroundAnswer is a request's answer, sent from the goroutine that made it.
type backgroundAnswer struct {
	frame *transport.Frame
	err   error
}

// payload is the answer's payload, for a failure message.
func (a backgroundAnswer) payload() string {
	if a.frame == nil {
		return ""
	}
	return string(a.frame.Payload)
}

// sendInBackground sends one request through c's handler with no session, on
// a goroutine, and returns the channel its answer arrives on.
func sendInBackground(t *testing.T, c *Cluster, op transport.Opcode, req any) <-chan backgroundAnswer {
	t.Helper()
	payload, err := bson.Marshal(req)
	if err != nil {
		t.Fatalf("encoding %s: %v", op, err)
	}
	answered := make(chan backgroundAnswer, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		frame, err := c.handleClientFrame(ctx, transport.NewFrame(op, payload))
		answered <- backgroundAnswer{frame: frame, err: err}
	}()
	return answered
}
