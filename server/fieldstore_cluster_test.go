package server

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Nested buckets step 8: a field write is an ordinary write to its parent
// bucket, so every test here drives the cluster's own paths — placement,
// replication, replay, commutation, the join fence — and asserts that a
// field store rides them with no mechanism of its own.

var postStores = []StoreDeclaration{
	{Name: "Comments", Engine: "fifoStore"},
	{Name: "Ranked", Engine: "heapStore"},
	{Name: "Tags", Engine: "hashStore"},
}

// createFieldBucket creates bucket in testDatabase declaring postStores and
// inserts document p1 into it, through node index.
func createFieldBucket(t *testing.T, tc *testCluster, index int, bucket string) {
	t.Helper()
	tc.ok(index, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash, Options: storesOption(postStores...)})
	tc.ok(index, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "p1", Data: map[string]interface{}{"title": "p1"}, WriteConcern: "all"})
}

// fieldCall sends one field verb through node index and decodes the answer.
func fieldCall(t *testing.T, tc *testCluster, index int, op transport.Opcode, req FieldRequest) FieldResponse {
	t.Helper()
	frame := tc.ok(index, op, req)
	var resp FieldResponse
	if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
		t.Fatalf("decoding the %s answer: %v", op, err)
	}
	return resp
}

// walOps counts, per opcode, the entries of bucket a node's WAL holds.
func walOps(t *testing.T, s *Server, bucket string) map[transport.Opcode]int {
	t.Helper()
	entries, err := s.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading the WAL: %v", err)
	}
	counts := make(map[transport.Opcode]int)
	for _, e := range entries {
		if e.Bucket == bucket {
			counts[transport.Opcode(e.Op)]++
		}
	}
	return counts
}

// ownerIndexOf is the index of the node that owns (database, bucket).
func ownerIndexOf(t *testing.T, tc *testCluster, bucket string) int {
	t.Helper()
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, bucket))
	if err != nil {
		t.Fatalf("owner of %q: %v", bucket, err)
	}
	return nodeIndexByAddr(t, tc.addrs, owner.ID)
}

// TestFieldStoreHasNoRingIdentity is spec T-08.
func TestFieldStoreHasNoRingIdentity(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})

	// fable F-02: if the field's name placed on the bucket's own owner the
	// test would prove nothing, so the names are chosen until they differ.
	bucket, field := "", ""
	for i := 0; i < 64 && field == ""; i++ {
		candidateBucket := fmt.Sprintf("Post%d", i)
		for _, candidateField := range []string{"Comments", "Tags", "Ranked"} {
			if ownerIndexOf(t, tc, candidateBucket) != ownerIndexOf(t, tc, candidateField) {
				bucket, field = candidateBucket, candidateField
				break
			}
		}
	}
	if field == "" {
		t.Fatalf("no bucket name among 64 placed apart from its field names; the ring has one owner")
	}
	t.Logf("bucket %q is owned by node %d, a bucket named %q would be owned by node %d",
		bucket, ownerIndexOf(t, tc, bucket)+1, field, ownerIndexOf(t, tc, field)+1)

	createFieldBucket(t, tc, 0, bucket)
	op, req := transport.OpFieldPut, FieldRequest{Database: testDatabase, Bucket: bucket, Key: "p1", Field: field, Element: "e1", Data: map[string]interface{}{"n": 1}}
	if field != "Tags" {
		op, req.Element = transport.OpFieldPush, ""
	}
	if resp := fieldCall(t, tc, 0, op, req); !resp.Success {
		t.Fatalf("the field write through node 1 failed: %s", resp.Message)
	}
	bucketOwner := ownerIndexOf(t, tc, bucket)
	for index, node := range tc.nodes {
		logged := walOps(t, node.server.core, bucket)[op] + walOps(t, node.server.core, field)[op]
		want := 0
		if index == bucketOwner {
			want = 1
		}
		if logged != want {
			t.Errorf("node %d logged %d %s entries, want %d: a field write is placed by its bucket, never its field", index+1, logged, op, want)
		}
	}

	frame := tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "Other", Type: BucketTypeHash, Options: map[string]interface{}{
		"stores": []interface{}{map[string]interface{}{"name": "Comments", "engine": "fifoStore", "replication_factor": 3}},
	}})
	var created CreateBucketResponse
	_ = bson.Unmarshal(frame.Payload, &created)
	if frame.Opcode != transport.OpError && created.Success {
		t.Fatalf("a store declaration carrying replication_factor was accepted")
	}
	if message := string(frame.Payload) + created.Message; !strings.Contains(message, "replication_factor") || !strings.Contains(message, `"Other"`) {
		t.Fatalf("the refusal %q does not name replication_factor and the bucket", message)
	}
}

// waitForField polls until check passes on every node or the deadline.
func waitForField(t *testing.T, tc *testCluster, what string, check func(s *Server) error) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for index, node := range tc.nodes {
		for {
			err := check(node.server.core)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d: %s: %v", index+1, what, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func TestFieldWriteReplicates(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createFieldBucket(t, tc, 0, testBucket)
	for n := range 3 {
		fieldCall(t, tc, 0, transport.OpFieldPush, FieldRequest{Database: testDatabase, Bucket: testBucket, Key: "p1", Field: "Comments", Data: map[string]interface{}{"n": n}, WriteConcern: "all"})
	}
	fieldCall(t, tc, 1, transport.OpFieldPut, FieldRequest{Database: testDatabase, Bucket: testBucket, Key: "p1", Field: "Tags", Element: "go", Data: map[string]interface{}{"w": 1}, WriteConcern: "all"})
	fieldCall(t, tc, 2, transport.OpFieldPop, FieldRequest{Database: testDatabase, Bucket: testBucket, Key: "p1", Field: "Comments", WriteConcern: "all"})

	// Elements are compared decoded, not as bytes: each applier marshals the
	// request's map itself, so member order differs by replica for field
	// elements exactly as it does for pushed and inserted documents
	// (Server.PushCtx, HashStore.Put).
	type listed struct {
		Element             bson.M
		ElementKey, EntryID string
		Position, Priority  *int64
	}
	listOf := func(s *Server, field string) ([]listed, error) {
		page, err := s.FieldList(FieldListRequest{FieldRequest: FieldRequest{Database: testDatabase, Bucket: testBucket, Key: "p1", Field: field}})
		if err != nil {
			return nil, err
		}
		var entries []listed
		for _, entry := range page.Entries {
			decoded := listed{ElementKey: entry.ElementKey, EntryID: entry.EntryID, Position: entry.Position, Priority: entry.Priority}
			if err := bson.Unmarshal(entry.Element, &decoded.Element); err != nil {
				return nil, err
			}
			entries = append(entries, decoded)
		}
		return entries, nil
	}
	reference := tc.nodes[ownerIndexOf(t, tc, testBucket)].server.core
	var wantComments, wantTags []listed
	waitForField(t, tc, "the owner holds the writes", func(s *Server) error {
		if s != reference {
			return nil
		}
		var err error
		if wantComments, err = listOf(s, "Comments"); err != nil || len(wantComments) != 2 {
			return fmt.Errorf("Comments holds %d elements (%v), want 2", len(wantComments), err)
		}
		wantTags, err = listOf(s, "Tags")
		return err
	})
	waitForField(t, tc, "every replica holds what the owner holds", func(s *Server) error {
		comments, err := listOf(s, "Comments")
		if err != nil {
			return err
		}
		tags, err := listOf(s, "Tags")
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(comments, wantComments) || !reflect.DeepEqual(tags, wantTags) {
			return fmt.Errorf("lists Comments %d, Tags %d; want the owner's %d and %d, element for element", len(comments), len(tags), len(wantComments), len(wantTags))
		}
		length, err := s.FieldLen(FieldRequest{Database: testDatabase, Bucket: testBucket, Key: "p1", Field: "Comments"})
		if err != nil || length.Len != 2 {
			return fmt.Errorf("Comments len %+v (%v), want 2", length, err)
		}
		return nil
	})
}

// TestFailedFieldWriteDoesNotOutliveItsDocument is spec T-07's cluster half
// (F-2). A field put fails on the owner and is recorded; the document is then
// deleted and created again. Replay reaches the recorded put beneath the
// mark: because it logged the parent's key, the delete and the re-insert name
// the same key, it does not commute, and it takes the loud path. Logging the
// element key instead would let it commute and land in the new document —
// last week's element in this week's post. (Re-creating the document is what
// makes the test able to fail: against a deleted parent the put is refused by
// the parent check inside its apply, whatever its key.)
func TestFailedFieldWriteDoesNotOutliveItsDocument(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	createFieldBucket(t, tc, ownerIndex, testBucket)
	owner := tc.nodes[ownerIndex].server.core

	failed := failNextApplyOn(t, owner.kv, anySeq)
	put := FieldRequest{Database: testDatabase, Bucket: testBucket, Key: "p1", Field: "Tags", Element: "stale", Data: map[string]interface{}{"n": 1}}
	if resp := fieldCall(t, tc, ownerIndex, transport.OpFieldPut, put); resp.Success {
		t.Fatalf("the field put whose apply was failed reported success")
	}
	if failed.Load() == 0 {
		t.Fatal("the field put never reached the store; the test failed nothing")
	}
	tc.ok(ownerIndex, transport.OpDelete, DeleteRequest{Database: testDatabase, Bucket: testBucket, Key: "p1"})
	tc.ok(ownerIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "p1", Data: map[string]interface{}{"title": "new p1"}})
	if st := bucketStateOf(t, owner, testDatabase, testBucket); !reflect.DeepEqual(st.FailedSeqs, []uint64{failed.Load()}) {
		t.Fatalf("before the restart failed_seqs = %v, want [%d]: the test did not build the recorded failure", st.FailedSeqs, failed.Load())
	}

	restarted := tc.restartNode(ownerIndex).server.core
	got, err := restarted.FieldGet(FieldRequest{Database: testDatabase, Bucket: testBucket, Key: "p1", Field: "Tags", Element: "stale"})
	if err != nil {
		t.Fatalf("reading Tags[stale] after replay: %v", err)
	}
	if got.Found {
		t.Fatalf("after replay the re-created p1 serves Tags[stale], an element written to the p1 that was deleted")
	}
	if st := bucketStateOf(t, restarted, testDatabase, testBucket); len(st.FailedSeqs) != 0 {
		t.Fatalf("after replay failed_seqs = %v, want none", st.FailedSeqs)
	}
}

func TestFieldPushIDsAgreeAcrossReplicas(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createFieldBucket(t, tc, 0, testBucket)
	fieldCall(t, tc, 0, transport.OpFieldPush, FieldRequest{Database: testDatabase, Bucket: testBucket, Key: "p1", Field: "Ranked", Priority: 7, Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	ids := make([]string, len(tc.nodes))
	waitForField(t, tc, "the heap element is on every node", func(s *Server) error {
		peeked, err := s.FieldPeek(FieldPeekRequest{FieldRequest: FieldRequest{Database: testDatabase, Bucket: testBucket, Key: "p1", Field: "Ranked"}})
		if err != nil || !peeked.Found || len(peeked.Items) != 1 {
			return fmt.Errorf("peek = %+v, %v", peeked, err)
		}
		// Three replicas that all answer an empty id would agree, so a missing
		// id is a failure, not a match.
		if peeked.Items[0].EntryID == "" {
			return fmt.Errorf("the peeked element carries no entry id: %+v", peeked.Items[0])
		}
		for index, node := range tc.nodes {
			if node.server.core == s {
				ids[index] = peeked.Items[0].EntryID
			}
		}
		return nil
	})
	for index := range ids {
		if ids[index] != ids[0] {
			t.Fatalf("the heap element's entry id differs across replicas: %q: every replica minted its own", ids)
		}
	}
}

// TestFieldWriteIsFencedDuringJoin: a joining owner that does not yet hold
// the bucket refuses a field write as it refuses an insert
// (TestClusterJoinFencesEmptyOwnedBucket's shape).
func TestFieldWriteIsFencedDuringJoin(t *testing.T) {
	fieldWriteIsFencedDuringJoin(t, false)
}

// TestFieldWriteIsFencedOnceTheTakeOverHasRecordedWhatItWaitsFor is the same
// refusal after the joiner's take-over worker has recorded the bucket's status.
// Which of the two refusals a push meets depends on whether the worker has
// recorded by then (fenceUnheldBucket wakes it and answers the hand-off only
// once it has), so a loaded host, which gives the worker time, reached the
// second in about 1 run in 200 (BUG-0487). Each is the fence; this test makes
// the second certain.
func TestFieldWriteIsFencedOnceTheTakeOverHasRecordedWhatItWaitsFor(t *testing.T) {
	fieldWriteIsFencedDuringJoin(t, true)
}

func fieldWriteIsFencedDuringJoin(t *testing.T, takeOverRecorded bool) {
	t.Helper()
	tc := newTestClusterWith(t, 3, 3, autoRebalanceOff)
	defer tc.stop()
	const buckets = 8
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for i := range buckets {
		createFieldBucket(t, tc, 0, fmt.Sprintf("posts%02d", i))
	}
	oc0 := tc.nodes[0].server.core.cluster
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := oc0.proposeJoin(ctx, newcomer); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	joiner := startJoinTarget(t, tc, ports, tc.token, func(c *config.Config) { c.Server.AutoRebalance = false })
	jc := joiner.server.core.cluster
	if err := jc.reconcileMembership(tc.addrs[0], oc0.memberNodes(), oc0.memberIDs(), oc0.Epoch()); err != nil {
		t.Fatalf("joining node resync: %v", err)
	}
	owned := ""
	for i := range buckets {
		name := fmt.Sprintf("posts%02d", i)
		if o, err := jc.Owner(placementOf(testDatabase, name)); err == nil && o.ID == newcomer.ID {
			owned = name
			break
		}
	}
	if owned == "" {
		t.Skip("joining node owns none of the created buckets; placement did not hand it one")
	}
	if takeOverRecorded {
		key := placementOf(testDatabase, owned)
		jc.wakeTakeOver(testDatabase, owned)
		pollUntil(t, "the joiner's take-over records what it waits for", func() bool {
			return jc.takeOverStatusOf(key).recorded()
		})
	}
	payload, _ := bson.Marshal(FieldRequest{Database: testDatabase, Bucket: owned, Key: "p1", Field: "Comments", Data: map[string]interface{}{"n": 1}})
	frame, err := joiner.client.SendRequest(ctx, transport.OpFieldPush, payload)
	if err != nil {
		t.Fatalf("field push to the joining node: %v", err)
	}
	refusal := string(frame.Payload)
	fenced := strings.Contains(refusal, "is moving to this node")
	if !takeOverRecorded {
		// The owner asks the members before it answers (BUG-0686): the members
		// hold the bucket, so the refusal names them and says retry.
		fenced = fenced || strings.Contains(refusal, "still hold(s) it")
	}
	if frame.Opcode != transport.OpError || !fenced {
		t.Fatalf("a field push to an owned-but-empty bucket was not fenced (response %s: %s)", frame.Opcode, string(frame.Payload))
	}
	// The refusal text alone cannot tell the join fence from the apply-time
	// check behind it (refuseUnservable answers the same error). The fence
	// refuses before the log: a write it lets through is appended first and
	// only then refused, a dead entry replay would have to skip.
	if logged := walOps(t, joiner.server.core, owned)[transport.OpFieldPush]; logged != 0 {
		t.Fatalf("the joining node logged %d FIELD_PUSH entries for %q before refusing them: the join fence did not stop the write", logged, owned)
	}
}

// TestFieldReadServedByAHolder: a field read is a read. A non-holder forwards
// it; nothing logs it.
func TestFieldReadServedByAHolder(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	createFieldBucket(t, tc, ownerIndex, testBucket)
	fieldCall(t, tc, ownerIndex, transport.OpFieldPut, FieldRequest{Database: testDatabase, Bucket: testBucket, Key: "p1", Field: "Tags", Element: "go", Data: map[string]interface{}{"w": 1}})
	get := FieldRequest{Database: testDatabase, Bucket: testBucket, Key: "p1", Field: "Tags", Element: "go"}
	for index := range tc.nodes {
		if resp := fieldCall(t, tc, index, transport.OpFieldGet, get); !resp.Found {
			t.Fatalf("FIELD_GET through node %d found nothing (owner is node %d)", index+1, ownerIndex+1)
		}
	}
	for index, node := range tc.nodes {
		if logged := walOps(t, node.server.core, testBucket)[transport.OpFieldGet]; logged != 0 {
			t.Fatalf("node %d logged %d FIELD_GET entries: a read was routed as a write", index+1, logged)
		}
	}
}
