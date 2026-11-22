package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Tests for the born set's durability and the keyed reads that consult it
// (spread MVP note rev 2.1, R2 and R3): marks are confirmed by a majority of
// the template's declared factor, a router lost after its marks loses no
// acknowledged document, and a key in a part the born set does not record is
// never read from an orphan.

// spreadEventually polls check until it is true or the deadline passes.
func spreadEventually(t testing.TB, within time.Duration, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %s", what, within)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestMarkShortOfTheDeclaredSetRefusesTheWrite is tests 21 and 22 at factor
// 2 (R2): with the template's replica stalled, majority counts the copies in
// sync, which is the router alone, so the router itself requires a majority
// of the declared factor. The first write to a new part is refused retryable,
// not applied, and nothing is stored. Then the router is lost: the replica's
// born set holds every mark that guards an acknowledged document, so through
// the new router every acknowledged document is read, and inserting it again
// answers DOCUMENT_EXISTS.
func TestMarkShortOfTheDeclaredSetRefusesTheWrite(t *testing.T) {
	sc := newSpreadCluster(t, 3, 2, "User", nil)
	set := sc.templateSet("User")
	router, replica := set[0], set[1]
	third := 3 - router - replica
	acked := make([]string, 0, 30)
	used := map[int]bool{}
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("acked-%d", i)
		if reply := sc.put(third, key, map[string]interface{}{"N": i}); reply.Opcode == transport.OpError {
			t.Fatalf("insert %s: %s", key, refusalText(t, reply))
		}
		acked = append(acked, key)
		used[spreadPartOf(t, key)] = true
	}
	routerCluster := clusterOf(sc.testCluster, router)
	replicas, err := routerCluster.Replicas(placementOf("app", "User"))
	if err != nil {
		t.Fatal(err)
	}
	var replicaPeer *peer
	for _, node := range replicas {
		if node.ID == sc.addrs[replica] {
			replicaPeer = routerCluster.peerFor(node)
		}
	}
	if replicaPeer == nil {
		t.Fatal("the template's replica is not in its set on the router's view")
	}
	spreadEventually(t, 20*time.Second, "the replica's born set catches up", func() bool {
		born, err := sc.nodes[replica].server.core.readBornSet("app", "User")
		if err != nil {
			return false
		}
		for p := range used {
			if born.state(p) != partCreated {
				return false
			}
		}
		return true
	})

	// The replica stalls: stopped, and its stream out of the in-sync set.
	base := sc.stopNodeKeepingData(replica)
	replicaPeer.stall("test: the template's replica is stalled")
	if replicaPeer.inSync() {
		t.Fatal("the stalled replica still counts as in sync")
	}
	fresh := keyOutsideParts(t, "fresh", used)
	refused := sc.put(third, fresh, map[string]interface{}{"N": -1})
	if refused.Opcode != transport.OpError {
		t.Fatalf("a first write to a new part with the template's replica stalled answered %s %q", refused.Opcode, refused.Payload)
	}
	var refusal ErrorResponse
	if err := bson.Unmarshal(refused.Payload, &refusal); err != nil {
		t.Fatal(err)
	}
	if refusal.WriteOutcome != outcomeNotApplied || transport.ErrorCode(refusal.Code) != transport.ErrUnavailable ||
		!strings.Contains(refusal.Message, "could not record where this document goes") {
		t.Fatalf("the refusal is %+v, want UNAVAILABLE, not_applied, naming the short mark", refusal)
	}
	if _, found := sc.get(router, fresh); found.Found {
		t.Fatal("the refused document was stored")
	}

	// The router is lost: the replica comes back, catches up, and the router
	// is removed.
	sc.nodes[replica] = sc.startNode(replica, base, sc.token)
	spreadEventually(t, 30*time.Second, "the replica's copy of the template is back", func() bool {
		return sc.nodes[replica].server.core.bucketStoreOpen("app", "User")
	})
	sc.stopNodeKeepingData(router)
	removeNodeFrom(t, sc.testCluster, third, router)
	newRouter, err := clusterOf(sc.testCluster, third).Owner(placementOf("app", "User"))
	if err != nil {
		t.Fatal(err)
	}
	waitTakenOver(t, sc.nodes[sc.indexOf(newRouter.ID)].server.core, "app", "User")
	for _, index := range []int{third, replica} {
		for _, key := range acked {
			spreadEventually(t, 60*time.Second, fmt.Sprintf("%s through node %d after the router was lost", key, index+1), func() bool {
				_, found := sc.get(index, key)
				return found.Found
			})
		}
	}
	// count() covers every part (step 3): the documents acknowledged, no
	// fewer, and not the refused one.
	for _, index := range []int{third, replica} {
		spreadEventually(t, 60*time.Second, fmt.Sprintf("count() through node %d after the router was lost", index+1), func() bool {
			count, refused := sc.count(index)
			return refused == "" && count == int64(len(acked))
		})
	}
	for _, key := range acked {
		again := sc.put(third, key, map[string]interface{}{"N": 0})
		if again.Opcode != transport.OpError || errorCodeOf(t, again) != transport.ErrDocumentExists {
			t.Errorf("re-inserting the acknowledged %s answered %s %q, want DOCUMENT_EXISTS", key, again.Opcode, again.Payload)
		}
	}
	if _, found := sc.get(third, fresh); found.Found {
		t.Error("the refused document appeared after the router was lost")
	}
}

// TestKeyedReadOfAnUnbornPartIsNotFound is test 23 (R3) at factor 1: the
// template is lost as an RF 1 router loss loses it (dropped alone, past the
// router, so its parts stay on their holders as orphans), and the collection
// is created again. A keyed read of an old key answers NOT FOUND through every
// node and every keyed read verb, and a delete or keyed update of it creates
// no part and logs nothing under the template.
func TestKeyedReadOfAnUnbornPartIsNotFound(t *testing.T) {
	stores := map[string]interface{}{"stores": []interface{}{map[string]interface{}{"name": "Purchases", "engine": "fifoStore"}}}
	sc := newSpreadCluster(t, 3, 1, "User", stores)
	var old []string
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("old-%d", i)
		if reply := sc.put(i%3, key, map[string]interface{}{"N": i}); reply.Opcode == transport.OpError {
			t.Fatalf("insert %s: %s", key, refusalText(t, reply))
		}
		old = append(old, key)
	}
	router := sc.templateSet("User")[0]
	c := clusterOf(sc.testCluster, router)
	key := placementOf("app", "User")
	drop := transport.NewFrame(transport.OpDeleteBucket, spreadMarshal(t, DeleteBucketRequest{Database: "app", Name: "User"}))
	ctx := withRouterGateHeld(context.Background(), key)
	entry, err := c.prepareClientWrite(ctx, drop, "User", decodeFrameMeta(drop.Payload))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.write(ctx, entry, key, WcOne); err != nil {
		t.Fatalf("dropping the template alone: %v", err)
	}
	spreadEventually(t, 20*time.Second, "the template is gone everywhere", func() bool {
		for _, node := range sc.nodes {
			if node.server.core.bucketStoreOpen("app", "User") {
				return false
			}
		}
		return true
	})
	sc.createCollection(1, "User", stores)
	orphans := 0
	for _, node := range sc.nodes {
		if node.server.core.bucketStoreOpen("app", partName("User", spreadPartOf(t, old[0]))) {
			orphans++
		}
	}
	if orphans == 0 {
		t.Fatal("the old part is not held anywhere: the test has no orphan to read")
	}
	logged := func() (count int) {
		for _, node := range sc.nodes {
			count += walEntriesForBucket(t, node.server.core.wal, "User")
		}
		return count
	}
	before := logged()
	for index := range sc.nodes {
		for _, oldKey := range old[:10] {
			if _, found := sc.get(index, oldKey); found.Found {
				t.Fatalf("get of the old %s through node %d returned the orphan's document", oldKey, index+1)
			}
			exists := sc.frame(index, transport.OpExists, FindRequest{Database: "app", Bucket: "User", Key: oldKey})
			if value, err := bson.Raw(exists.Payload).LookupErr("exists"); exists.Opcode == transport.OpError || err != nil || value.Boolean() {
				t.Fatalf("exists of the old %s answered %s %q", oldKey, exists.Opcode, exists.Payload)
			}
			consistent := sc.frame(index, transport.OpConsistentGet, FindRequest{Database: "app", Bucket: "User", Key: oldKey})
			if strings.Contains(string(consistent.Payload), `"found": true`) || bytesHoldTrue(consistent.Payload, "found") {
				t.Fatalf("consistent get of the old %s answered %q", oldKey, consistent.Payload)
			}
			field := sc.frame(index, transport.OpFieldLen, FieldRequest{Database: "app", Bucket: "User", Key: oldKey, Field: "Purchases"})
			if field.Opcode != transport.OpError || errorCodeOf(t, field) != transport.ErrNotFound {
				t.Fatalf("a field read of the old %s answered %s %q", oldKey, field.Opcode, field.Payload)
			}
			fast, err := transport.MarshalGetFast(transport.GetFastRequest{Database: "app", Bucket: "User", Key: oldKey})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			reply, err := sc.nodes[index].client.SendRequest(ctx, transport.OpGetFast, fast)
			cancel()
			if err != nil || reply.Opcode != transport.OpGetFastResponse || reply.Payload[0] != byte(transport.GetFastNotFound) {
				t.Fatalf("get-fast of the old %s answered %v %v", oldKey, reply, err)
			}
		}
		deleted := sc.frame(index, transport.OpDelete, DeleteRequest{Database: "app", Bucket: "User", Key: old[20+index]})
		if replySucceeded(deleted.Opcode, deleted.Payload) {
			t.Fatalf("a delete of the old %s answered success", old[20+index])
		}
		updated := sc.frame(index, transport.OpUpdate, UpdateRequest{Database: "app", Bucket: "User", Key: old[25+index], Set: map[string]interface{}{"N": 0}})
		if replySucceeded(updated.Opcode, updated.Payload) {
			t.Fatalf("an update of the old %s answered success", old[25+index])
		}
	}
	if after := logged(); after != before {
		t.Fatalf("keyed reads, deletes and updates of unborn parts logged %d entries under the template", after-before)
	}

	// The first insert into the old part meets the orphan at its owner, which
	// answers its part_of; the router drops it and creates the part for this
	// incarnation (B4.2): the old document never comes back.
	p := spreadPartOf(t, old[0])
	fresh := keysInPart(t, "fresh", p, 1)[0]
	if reply := sc.put(2, fresh, map[string]interface{}{"N": -1}); reply.Opcode == transport.OpError {
		t.Fatalf("insert into the orphan's part: %s", refusalText(t, reply))
	}
	if _, found := sc.get(0, old[0]); found.Found {
		t.Fatal("the orphan's document came back after its part was created for this incarnation")
	}
	if _, found := sc.get(0, fresh); !found.Found {
		t.Fatal("the document written into the re-created part is not found")
	}
	template, err := sc.nodes[sc.templateSet("User")[0]].server.core.loadSpreadTemplate("app", "User")
	if err != nil {
		t.Fatal(err)
	}
	for index, node := range sc.nodes {
		if !node.server.core.bucketStoreOpen("app", partName("User", p)) {
			continue
		}
		if _, partOf, err := node.server.core.partFieldsOf("app", partName("User", p)); err != nil || partOf != template.incarnation {
			t.Errorf("node %d's part records part_of %d (%v), want this incarnation's %d", index+1, partOf, err, template.incarnation)
		}
	}
}

// bytesHoldTrue reports whether a BSON answer's top-level boolean field is
// true.
func bytesHoldTrue(payload []byte, field string) bool {
	value, err := bson.Raw(payload).LookupErr(field)
	if err != nil {
		return false
	}
	flag, ok := value.BooleanOK()
	return ok && flag
}

// TestMigrateOfASpreadBucketIsRefused is §2.4's operator MIGRATE row (N10):
// a spread collection moves part by part with the rebalance, not as one
// bucket; asked of a node that holds its template, the migrate is refused
// with that line.
func TestMigrateOfASpreadBucketIsRefused(t *testing.T) {
	sc := newSpreadCluster(t, 3, 2, "User", nil)
	holder := sc.templateSet("User")[1]
	reply := sc.frame(holder, transport.OpMigrate, MigrateRequest{Database: "app", Bucket: "User"})
	if reply.Opcode != transport.OpError || !strings.Contains(refusalText(t, reply), "moves part by part") {
		t.Fatalf("a migrate of a spread bucket answered %s %q", reply.Opcode, reply.Payload)
	}
}
