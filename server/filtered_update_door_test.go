package server

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Build step 12c (document placement spec §6.7, T-38): a filtered update on a
// bucket recorded spread is refused at the client's door, with the fix in the
// text, and nothing is logged.

const spreadUpdateBucket = "Spread"
const wholeUpdateBucket = "Whole"

// filteredUpdateFixText are the pieces a refusal must carry: the thing named,
// then the runnable fix.
func filteredUpdateFixText(bucket, database string) []string {
	return []string{
		`Bucket "` + bucket + `" (database "` + database + `") is declared spread (placement: spread)`,
		"an update matching a filter is refused on a spread bucket",
		"db." + bucket + `.update("ada", {$set: {City: "London"}})`,
		"find them and update each id",
		"create bucket " + bucket + " in " + database + " { type: hash, placement: whole }",
	}
}

func requireFilteredUpdateRefusal(t *testing.T, reply *transport.Frame, bucket, database, what string) {
	t.Helper()
	refusal := decodeErrorFrame(t, reply, what)
	if refusal.Code != 4 {
		t.Errorf("%s: code %d (%q), want 4 INVALID_ARGUMENT", what, refusal.Code, refusal.Message)
	}
	for _, want := range filteredUpdateFixText(bucket, database) {
		if !strings.Contains(refusal.Message, want) {
			t.Errorf("%s: message %q does not contain %q", what, refusal.Message, want)
		}
	}
}

func decodeUpdateAnswer(t *testing.T, reply *transport.Frame, what string) UpdateResponse {
	t.Helper()
	if reply.Opcode == transport.OpError {
		t.Fatalf("%s: refused: %+v", what, decodeErrorFrame(t, reply, what))
	}
	var answer UpdateResponse
	if err := bson.Unmarshal(reply.Payload, &answer); err != nil {
		t.Fatalf("%s: decoding the answer: %v", what, err)
	}
	return answer
}

// TestFilteredUpdateRefusedAtDoorOnSpread is T-38, the standalone door
// (Server.fencedDispatch): a filtered or multi update on a spread bucket is
// refused with the fix and nothing changed (a standalone node keeps no log); the same on a whole
// bucket, and a keyed update on a spread one, still work.
func TestFilteredUpdateRefusedAtDoorOnSpread(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	createPlacementBucket(t, s, spreadUpdateBucket, BucketTypeHash, "spread")
	createPlacementBucket(t, s, wholeUpdateBucket, BucketTypeHash, "whole")
	for _, bucket := range []string{spreadUpdateBucket, wholeUpdateBucket} {
		routedInsert(t, s, "app", bucket, "ada", map[string]interface{}{"City": "Paris", "Kind": "x"})
	}
	send := func(req UpdateRequest) *transport.Frame {
		t.Helper()
		reply, err := s.fencedDispatch(context.Background(), requestFrame(t, transport.OpUpdate, req))
		if err != nil {
			t.Fatalf("fencedDispatch: %v", err)
		}
		return reply
	}
	set := map[string]interface{}{"City": "London"}

	for name, req := range map[string]UpdateRequest{
		"filter":       {Database: "app", Bucket: spreadUpdateBucket, Filter: map[string]interface{}{"Kind": "x"}, Set: set},
		"multi":        {Database: "app", Bucket: spreadUpdateBucket, Multi: true, Set: set},
		"multi filter": {Database: "app", Bucket: spreadUpdateBucket, Multi: true, Filter: map[string]interface{}{"Kind": "x"}, Set: set},
	} {
		requireFilteredUpdateRefusal(t, send(req), spreadUpdateBucket, "app", "a "+name+" update on a spread bucket")
	}
	if got := collectionDocumentOf(t, s, "app", spreadUpdateBucket, "ada")["City"]; got != "Paris" {
		t.Errorf("a refused update changed the document: City = %v", got)
	}

	if answer := decodeUpdateAnswer(t, send(UpdateRequest{Database: "app", Bucket: spreadUpdateBucket, Key: "ada", Set: set}), "keyed update on spread"); !answer.Success {
		t.Errorf("a keyed update on a spread bucket was refused: %+v", answer)
	}
	if got := collectionDocumentOf(t, s, "app", spreadUpdateBucket, "ada")["City"]; got != "London" {
		t.Errorf("the keyed update on spread did not apply: City = %v", got)
	}

	if answer := decodeUpdateAnswer(t, send(UpdateRequest{Database: "app", Bucket: wholeUpdateBucket, Filter: map[string]interface{}{"Kind": "x"}, Multi: true, Set: set}), "filtered update on whole"); !answer.Success {
		t.Errorf("a filtered update on a whole bucket was refused: %+v", answer)
	}
	if got := documentOf(t, s, "app", wholeUpdateBucket, "ada")["City"]; got != "London" {
		t.Errorf("the filtered update on a whole bucket did not apply: City = %v", got)
	}
}

// TestFilteredUpdateRefusedOnClusterSpread is T-38's cluster half, and the
// B5 row "filtered update via a node outside the template's replica set": the
// refusal is the same from the holder and from every node that holds no copy,
// where the door cannot see the bucket and the owner refuses instead. Nothing
// is logged on any node, and a whole bucket updates by filter through any node.
func TestFilteredUpdateRefusedOnClusterSpread(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: spreadUpdateBucket, Type: BucketTypeHash, Options: map[string]interface{}{"placement": "spread"}})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: wholeUpdateBucket, Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "whole"}})
	waitForServing(t, tc, testDatabase, spreadUpdateBucket, 1)
	waitForServing(t, tc, testDatabase, wholeUpdateBucket, 1)
	for _, bucket := range []string{spreadUpdateBucket, wholeUpdateBucket} {
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "ada",
			Data: map[string]interface{}{"City": "Paris", "Kind": "x"}})
	}
	holder := -1
	for index, node := range tc.nodes {
		if node.server.core.bucketStoreOpen(testDatabase, spreadUpdateBucket) {
			holder = index
		}
	}
	if holder < 0 {
		t.Fatal("no node holds the spread bucket")
	}
	set := map[string]interface{}{"City": "London"}
	filter := map[string]interface{}{"Kind": "x"}

	before := tc.walSequences()
	for index := range tc.nodes {
		role := "a node outside the bucket's replica set"
		if index == holder {
			role = "the holder"
		}
		for name, req := range map[string]UpdateRequest{
			"filtered": {Database: testDatabase, Bucket: spreadUpdateBucket, Filter: filter, Set: set},
			"multi":    {Database: testDatabase, Bucket: spreadUpdateBucket, Multi: true, Set: set},
		} {
			requireFilteredUpdateRefusal(t, tc.frame(index, transport.OpUpdate, req), spreadUpdateBucket, testDatabase,
				name+" update on a spread bucket through "+role)
		}
	}
	after := tc.walSequences()
	for index := range after {
		if after[index] != before[index] {
			t.Errorf("node %d logged a refused update: sequence %d -> %d", index+1, before[index], after[index])
		}
	}
	if got := tc.nodes[adaHolder(t, tc, spreadUpdateBucket)].server.core.mustGetCity(t, spreadUpdateBucket); got != "Paris" {
		t.Errorf("a refused update changed the document: City = %v", got)
	}

	for index := range tc.nodes {
		answer := decodeUpdateAnswer(t, tc.frame(index, transport.OpUpdate, UpdateRequest{Database: testDatabase, Bucket: spreadUpdateBucket, Key: "ada", Set: set}),
			"keyed update on spread through a node")
		if !answer.Success {
			t.Errorf("a keyed update on a spread bucket through node %d was refused: %+v", index+1, answer)
		}
		answer = decodeUpdateAnswer(t, tc.frame(index, transport.OpUpdate, UpdateRequest{Database: testDatabase, Bucket: wholeUpdateBucket, Filter: filter, Multi: true, Set: set}),
			"filtered update on whole through a node")
		if !answer.Success {
			t.Errorf("a filtered update on a whole bucket through node %d was refused: %+v", index+1, answer)
		}
	}
}

// TestKeyedUpdateWithFilterIsAllowedOnSpread is the other side of the step
// 12c line: a keyed update that carries a filter is not a scan, so it is not
// refused on a spread bucket, and it answers as it does on a whole bucket. The
// comparison is with the whole bucket for the same request, so the test does
// not depend on what a keyed filter does: today UpdateCtx applies the keyed
// update whether or not the filter matches. A refusal that ignored the key
// would turn it red.
func TestKeyedUpdateWithFilterIsAllowedOnSpread(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	createPlacementBucket(t, s, spreadUpdateBucket, BucketTypeHash, "spread")
	createPlacementBucket(t, s, wholeUpdateBucket, BucketTypeHash, "whole")
	for _, bucket := range []string{spreadUpdateBucket, wholeUpdateBucket} {
		routedInsert(t, s, "app", bucket, "ada", map[string]interface{}{"City": "Paris", "Kind": "x"})
	}
	keyedWithFilter := func(bucket string, filter map[string]interface{}, city string) UpdateResponse {
		t.Helper()
		req := UpdateRequest{Database: "app", Bucket: bucket, Key: "ada", Filter: filter, Set: map[string]interface{}{"City": city}}
		reply, err := s.fencedDispatch(context.Background(), requestFrame(t, transport.OpUpdate, req))
		if err != nil {
			t.Fatalf("fencedDispatch: %v", err)
		}
		return decodeUpdateAnswer(t, reply, "keyed update with a filter on "+bucket)
	}

	for _, step := range []struct {
		name   string
		filter map[string]interface{}
		city   string
	}{
		{"non-matching", map[string]interface{}{"Kind": "y"}, "London"},
		{"matching", map[string]interface{}{"Kind": "x"}, "Rome"},
	} {
		spreadAnswer := keyedWithFilter(spreadUpdateBucket, step.filter, step.city)
		wholeAnswer := keyedWithFilter(wholeUpdateBucket, step.filter, step.city)
		if !reflect.DeepEqual(spreadAnswer, wholeAnswer) {
			t.Errorf("a keyed update with a %s filter: spread answered %+v, whole answered %+v", step.name, spreadAnswer, wholeAnswer)
		}
		if step.name == "matching" && !spreadAnswer.Success {
			t.Errorf("a keyed update with a matching filter was not applied on spread: %+v", spreadAnswer)
		}
		spreadCity, wholeCity := collectionDocumentOf(t, s, "app", spreadUpdateBucket, "ada")["City"], documentOf(t, s, "app", wholeUpdateBucket, "ada")["City"]
		if spreadCity != wholeCity {
			t.Errorf("after the %s keyed update with a filter: spread City = %v, whole City = %v", step.name, spreadCity, wholeCity)
		}
	}
}

// TestKeyedUpdateWithFilterIsAllowedOnClusterSpread is the cluster half of
// TestKeyedUpdateWithFilterIsAllowedOnSpread: through the holder and through
// both nodes that hold no copy, a keyed update with a filter is not refused and
// answers as it does on a whole bucket. A node without a copy marks only a
// filtered scan for its owner (FilterDeferred), so a keyed update with a
// filter must reach the owner unmarked and apply there.
func TestKeyedUpdateWithFilterIsAllowedOnClusterSpread(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: spreadUpdateBucket, Type: BucketTypeHash, Options: map[string]interface{}{"placement": "spread"}})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: wholeUpdateBucket, Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "whole"}})
	waitForServing(t, tc, testDatabase, spreadUpdateBucket, 1)
	waitForServing(t, tc, testDatabase, wholeUpdateBucket, 1)
	for _, bucket := range []string{spreadUpdateBucket, wholeUpdateBucket} {
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "ada",
			Data: map[string]interface{}{"City": "Paris", "Kind": "x"}})
	}
	holder := -1
	for index, node := range tc.nodes {
		if node.server.core.bucketStoreOpen(testDatabase, spreadUpdateBucket) {
			holder = index
		}
	}
	if holder < 0 {
		t.Fatal("no node holds the spread bucket")
	}

	keyedWithFilter := func(index int, bucket string, filter map[string]interface{}, city string) UpdateResponse {
		t.Helper()
		req := UpdateRequest{Database: testDatabase, Bucket: bucket, Key: "ada", Filter: filter, Set: map[string]interface{}{"City": city}}
		return decodeUpdateAnswer(t, tc.frame(index, transport.OpUpdate, req),
			fmt.Sprintf("keyed update with a filter on %s through node %d", bucket, index+1))
	}
	for _, step := range []struct {
		name   string
		filter map[string]interface{}
		city   string
	}{
		{"non-matching", map[string]interface{}{"Kind": "y"}, "London"},
		{"matching", map[string]interface{}{"Kind": "x"}, "Rome"},
	} {
		for index := range tc.nodes {
			role := "a node outside the bucket's replica set"
			if index == holder {
				role = "the holder"
			}
			spreadAnswer := keyedWithFilter(index, spreadUpdateBucket, step.filter, step.city)
			wholeAnswer := keyedWithFilter(index, wholeUpdateBucket, step.filter, step.city)
			if !reflect.DeepEqual(spreadAnswer, wholeAnswer) {
				t.Errorf("a keyed update with a %s filter through %s: spread answered %+v, whole answered %+v", step.name, role, spreadAnswer, wholeAnswer)
			}
			if step.name == "matching" && !spreadAnswer.Success {
				t.Errorf("a keyed update with a matching filter through %s was not applied on spread: %+v", role, spreadAnswer)
			}
		}
		if got := tc.nodes[adaHolder(t, tc, spreadUpdateBucket)].server.core.mustGetCity(t, spreadUpdateBucket); got != step.city {
			t.Errorf("after the %s keyed update with a filter: spread City = %v, want %s", step.name, got, step.city)
		}
	}
}

// mustGetCity reads the City of "ada" in bucket from this node's own copy:
// of the bucket, or of the part a spread bucket keeps it in, whose owner
// the caller names.
func (s *Server) mustGetCity(t *testing.T, bucket string) interface{} {
	t.Helper()
	return collectionDocumentOf(t, s, testDatabase, bucket, "ada")["City"]
}

// adaHolder is the node whose own copy holds "ada" of bucket: the bucket's
// holder, or the owner of the part a spread bucket keeps it in.
func adaHolder(t *testing.T, tc *testCluster, bucket string) int {
	t.Helper()
	for index, node := range tc.nodes {
		if collectionDocumentOf(t, node.server.core, testDatabase, bucket, "ada") != nil {
			return index
		}
	}
	t.Fatalf("no node holds ada of %s", bucket)
	return -1
}

// TestLoggedTTLDeleteAppliesOnReplicaAndReplay is T-61, a guard for where the
// refusal lives: a TTL expiry on a spread bucket is a logged per-key OpDelete
// with expire_if, and it applies on the owner, on a replica and after a
// restart. A refusal moved from the door into Server.UpdateCtx or a handler
// would be asked of replicas and of replay, and this would go red there.
func TestLoggedTTLDeleteAppliesOnReplicaAndReplay(t *testing.T) {
	now := time.Now()
	tc, owner := ttlClusterPlaced(t, 3, 3, 2, now, placementSpread)
	defer tc.stop()
	replica := (owner + 1) % len(tc.nodes)
	if facts, held := tc.nodes[replica].server.core.peekBucket(testDatabase, "sessions"); !held || !facts.spread {
		t.Fatalf("the sessions bucket is not recorded spread on node %d (%+v, held %v): the guard would prove nothing", replica+1, facts, held)
	}

	// Each part is swept by its own owner, so every node sweeps.
	for _, node := range tc.nodes {
		node.server.core.sweepTTL(context.Background(), now)
	}
	waitForSessions(t, tc, []int{0, 1, 2}, "live")

	for _, index := range []int{replica, owner} {
		restarted := tc.restartNode(index)
		if got := strings.Join(sessionsOn(t, tc, index), ","); got != "live" {
			t.Errorf("node %d holds %v after a restart replayed the expiries, want [live]", index+1, got)
		}
		if state := bucketStateOf(t, restarted.server.core, testDatabase, "sessions"); len(state.FailedSeqs) != 0 {
			t.Errorf("node %d failed to replay an expiry: failed sequences %v", index+1, state.FailedSeqs)
		}
	}
}
