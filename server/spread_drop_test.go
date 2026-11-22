package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Tests for step 4 of the spread MVP: the drop of a spread collection, its
// resume, the orphans of a dropped incarnation and the clean refusals of the
// declaration changes (document placement spread MVP note, section 2.3, 2.4
// and 5, tests 10, 14).

// droppingText is the retry line of a drop in progress, character for
// character.
const droppingText = `Bucket "User" (database "app") is being dropped. Run 'drop bucket User in app' again to finish it.`

// partsHeld is how many part buckets of User node s holds.
func partsHeld(s *Server) int { return partsHeldOf(s, "User") }

// partsHeldOf is how many part buckets of collection node s holds.
func partsHeldOf(s *Server, collection string) int {
	held := 0
	for p := 0; p < defaultSpreadParts; p++ {
		if s.bucketStoreOpen("app", partName(collection, p)) {
			held++
		}
	}
	return held
}

// dropReply sends a client's drop of app.User.
func (n *spreadNode) dropReply() *transport.Frame {
	n.t.Helper()
	return n.send(transport.OpDeleteBucket, DeleteBucketRequest{Database: "app", Name: "User"})
}

// requireDropped fails unless reply is a successful drop.
func requireDropped(t testing.TB, reply *transport.Frame) {
	t.Helper()
	if reply.Opcode == transport.OpError {
		t.Fatalf("the drop was refused: %s", refusalTextOrEmptyTB(t, reply))
	}
	var answer DeleteBucketResponse
	if err := bson.Unmarshal(reply.Payload, &answer); err != nil || !answer.Success {
		t.Fatalf("the drop answered %s %q (%v)", reply.Opcode, reply.Payload, err)
	}
}

// requireRefusedWith fails unless reply is a refusal whose text is exactly want.
func requireRefusedWith(t testing.TB, what string, reply *transport.Frame, want string) {
	t.Helper()
	if got := refusalTextOrEmptyTB(t, reply); reply.Opcode != transport.OpError || got != want {
		t.Fatalf("%s answered %s %q, want the refusal %q", what, reply.Opcode, got, want)
	}
}

// recreate creates app.User spread again through the node's client door.
func (n *spreadNode) recreate() {
	n.t.Helper()
	reply := n.send(transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "User", Type: BucketTypeHash, Options: map[string]interface{}{"placement": "spread"}})
	if reply.Opcode == transport.OpError || !strings.Contains(string(reply.Payload), "spread") {
		n.t.Fatalf("creating app.User spread again answered %s %q", reply.Opcode, reply.Payload)
	}
}

// TestDropOfAnEmptySpreadCollection: a collection never written to has no born
// set and no part; the drop removes the template and the name is free.
func TestDropOfAnEmptySpreadCollection(t *testing.T) {
	n := newSpreadNode(t, nil)
	requireDropped(t, n.dropReply())
	if n.s.bucketStoreOpen("app", "User") {
		t.Fatal("the template is still held after the drop")
	}
	if reply := n.send(transport.OpGet, FindRequest{Database: "app", Bucket: "User", Key: "ada"}); reply.Opcode != transport.OpError || !strings.Contains(refusalTextOrEmptyTB(t, reply), "not found") {
		t.Fatalf("a get after the drop answered %s %q, want bucket not found", reply.Opcode, reply.Payload)
	}
	if reply := n.dropReply(); reply.Opcode != transport.OpError || !strings.Contains(refusalTextOrEmptyTB(t, reply), "not found") {
		t.Fatalf("a second drop answered %s %q, want bucket not found as a whole bucket's does", reply.Opcode, reply.Payload)
	}
	n.recreate()
}

// TestDropOfOnePartAndOfAll256Parts: the parts that exist are dropped with the
// template, from one part to all K, and a collection created again under the
// same name is empty; each old key can be inserted anew.
func TestDropOfOnePartAndOfAll256Parts(t *testing.T) {
	for _, wanted := range []int{1, defaultSpreadParts} {
		t.Run(fmt.Sprintf("%d parts", wanted), func(t *testing.T) {
			n := newSpreadNode(t, nil)
			var keys []string
			for p := 0; p < wanted; p++ {
				key := keysInPart(t, "old", p, 1)[0]
				keys = append(keys, key)
				if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: key, Data: map[string]interface{}{"Gen": "old"}}); reply.Opcode == transport.OpError {
					t.Fatalf("insert %s: %s", key, refusalTextOrEmptyTB(t, reply))
				}
			}
			if got := partsHeld(n.s); got != wanted {
				t.Fatalf("%d parts held before the drop, want %d", got, wanted)
			}
			requireDropped(t, n.dropReply())
			if got := partsHeld(n.s); got != 0 || n.s.bucketStoreOpen("app", "User") {
				t.Fatalf("after the drop %d parts are held, template held %v", got, n.s.bucketStoreOpen("app", "User"))
			}
			n.recreate()
			for _, key := range keys {
				if found := findAnswer(t, n.send(transport.OpGet, FindRequest{Database: "app", Bucket: "User", Key: key})); found.Found {
					t.Fatalf("%s came back after the drop", key)
				}
			}
			if count := fanOutCount(t, n.send(transport.OpCount, CountRequest{Database: "app", Bucket: "User"})); count != 0 {
				t.Fatalf("count() after drop and re-create is %d, want 0", count)
			}
			// A write to every old part: the new collection takes it as its first.
			for _, key := range keys {
				if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: key, Data: map[string]interface{}{"Gen": "new"}}); reply.Opcode == transport.OpError {
					t.Fatalf("insert %s after the re-create: %s", key, refusalTextOrEmptyTB(t, reply))
				}
			}
			if count := fanOutCount(t, n.send(transport.OpCount, CountRequest{Database: "app", Bucket: "User"})); count != int64(len(keys)) {
				t.Fatalf("count() is %d, want %d (only the new documents)", count, len(keys))
			}
		})
	}
}

// failingDropDoor is the one-member door with the drop of one part refused
// while armed: a part owner that is down, as the router meets it.
type failingDropDoor struct {
	localSpreadDoor
	part  string
	armed *atomic.Bool
}

func (d failingDropDoor) writePart(ctx context.Context, database string, entry ReplEntry, wc WriteConcernLevel) (ReplResponse, error) {
	if d.armed.Load() && entry.Bucket == d.part && transport.Opcode(entry.Op) == transport.OpDeleteBucket {
		return ReplResponse{}, errors.New("node n2 did not answer: connection refused")
	}
	return d.localSpreadDoor.writePart(ctx, database, entry, wc)
}

// dropWith runs the router's drop of app.User through door.
func dropWith(t *testing.T, n *spreadNode, door spreadDoor) (ReplResponse, error) {
	t.Helper()
	template, err := n.s.loadSpreadTemplate("app", "User")
	if err != nil {
		t.Fatal(err)
	}
	router := spreadRouter{s: n.s, door: door, t: template}
	payload := spreadMarshal(t, DeleteBucketRequest{Database: "app", Name: "User"})
	return router.routeWrite(context.Background(), ReplEntry{Op: int32(transport.OpDeleteBucket), Bucket: "User", Payload: payload}, 0)
}

// TestDropSpreadResumesAndNeverResurrects is test 10 (B4): a drop with a part
// owner down is refused with the retry line, and every operation then answers
// "being dropped", whole-collection reads and writes included. A second drop
// finishes. After a re-create and a write to every old part, count() is only
// the new writes.
func TestDropSpreadResumesAndNeverResurrects(t *testing.T) {
	n := newSpreadNode(t, nil)
	var keys []string
	for p := 0; p < 12; p++ {
		key := keysInPart(t, "old", p, 1)[0]
		keys = append(keys, key)
		if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: key, Data: map[string]interface{}{"Gen": "old"}}); reply.Opcode == transport.OpError {
			t.Fatal(refusalTextOrEmptyTB(t, reply))
		}
	}
	armed := &atomic.Bool{}
	armed.Store(true)
	door := failingDropDoor{localSpreadDoor: localSpreadDoor{s: n.s}, part: partName("User", 7), armed: armed}
	_, err := dropWith(t, n, door)
	if err == nil || err.Error() != droppingText || !errors.Is(err, ErrSpreadDropping) {
		t.Fatalf("a drop with a part owner down returned %v, want the retry line", err)
	}
	if !n.s.bucketStoreOpen("app", "User") {
		t.Fatal("the template was dropped before every part was")
	}
	// Unreadable and unwritable as a whole, never half visible.
	for label, reply := range map[string]*transport.Frame{
		"get":      n.send(transport.OpGet, FindRequest{Database: "app", Bucket: "User", Key: keys[0]}),
		"put":      n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: "fresh", Data: map[string]interface{}{"Gen": "new"}}),
		"count":    n.send(transport.OpCount, CountRequest{Database: "app", Bucket: "User"}),
		"find":     n.send(transport.OpScan, ScanRequest{Database: "app", Bucket: "User"}),
		"describe": n.send(transport.OpBucketStats, BucketStatsRequest{Database: "app", Bucket: "User"}),
		"batch":    n.send(transport.OpBatchPut, BatchInsertRequest{Database: "app", Bucket: "User", Data: []map[string]interface{}{{"_id_": "b1"}}}),
	} {
		requireRefusedWith(t, label+" of a collection being dropped", reply, droppingText)
	}
	dump, err := n.handler(rootGrantedContext(), transport.NewFrame(transport.OpDumpStart, spreadMarshal(t, DumpStartRequest{Database: "app", Bucket: "User", AcceptStores: true, AcceptDead: true})))
	if err != nil {
		t.Fatal(err)
	}
	requireRefusedWith(t, "dump of a collection being dropped", dump, droppingText)
	armed.Store(false)
	requireDropped(t, n.dropReply())
	if got := partsHeld(n.s); got != 0 || n.s.bucketStoreOpen("app", "User") {
		t.Fatalf("after the second drop %d parts are held, template held %v", got, n.s.bucketStoreOpen("app", "User"))
	}
	n.recreate()
	for _, key := range keys {
		if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: key, Data: map[string]interface{}{"Gen": "new"}}); reply.Opcode == transport.OpError {
			t.Fatalf("insert %s after the re-create: %s", key, refusalTextOrEmptyTB(t, reply))
		}
	}
	var page ScanResponse
	decodeAnswer(t, n.send(transport.OpScan, ScanRequest{Database: "app", Bucket: "User", Limit: 100}), &page)
	if len(page.Documents) != len(keys) {
		t.Fatalf("find() is %d documents, want %d", len(page.Documents), len(keys))
	}
	for _, document := range page.Documents {
		if gen, _ := document.Lookup("Gen").StringValueOK(); gen != "new" {
			t.Fatalf("a document of the dropped incarnation came back: %s", document)
		}
	}
}

// TestDropReadsTheBornSetAndNothingMoreThanItsParts: a drop never removes or
// rewrites the born set before the template's drop (S4-1): a refused drop
// leaves the born document readable with every part still recorded, only
// `dropping` added, so the resume knows what is left.
func TestDropKeepsTheBornSetUntilTheTemplateGoes(t *testing.T) {
	n := newSpreadNode(t, nil)
	for p := 0; p < 5; p++ {
		key := keysInPart(t, "k", p, 1)[0]
		if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: key, Data: map[string]interface{}{"N": 1}}); reply.Opcode == transport.OpError {
			t.Fatal(refusalTextOrEmptyTB(t, reply))
		}
	}
	before, err := n.s.readBornSet("app", "User")
	if err != nil {
		t.Fatal(err)
	}
	armed := &atomic.Bool{}
	armed.Store(true)
	// The last part fails, so every part before it was dropped already.
	last := 0
	for p := 0; p < defaultSpreadParts; p++ {
		if before.state(p) != 0 {
			last = p
		}
	}
	if _, err := dropWith(t, n, failingDropDoor{localSpreadDoor: localSpreadDoor{s: n.s}, part: partName("User", last), armed: armed}); err == nil {
		t.Fatal("the drop was not refused")
	}
	after, err := n.s.readBornSet("app", "User")
	if err != nil {
		t.Fatal(err)
	}
	if !after.dropping() {
		t.Fatal("the born set does not say dropping")
	}
	for p := 0; p < defaultSpreadParts; p++ {
		if before.state(p) != after.state(p) {
			t.Fatalf("part %d was %d and is %d: the drop rewrote the born set before the template went", p, before.state(p), after.state(p))
		}
	}
}

// TestStaleRouterOfADroppedCollectionRefusesAfterTheRecreate is the step 2
// blocker's class with the real drop: an operation loaded its template, the
// collection was dropped and created again while it waited for the gate, and
// it must not write into, or answer from, the new collection with the old
// incarnation.
func TestStaleRouterOfADroppedCollectionRefusesAfterTheRecreate(t *testing.T) {
	n := newSpreadNode(t, nil)
	if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: "seed", Data: map[string]interface{}{"N": 1}}); reply.Opcode == transport.OpError {
		t.Fatal(refusalTextOrEmptyTB(t, reply))
	}
	stale, err := n.s.loadSpreadTemplate("app", "User")
	if err != nil {
		t.Fatal(err)
	}
	requireDropped(t, n.dropReply())
	n.recreate()
	router := spreadRouter{s: n.s, door: localSpreadDoor{s: n.s}, t: stale}
	insert := spreadMarshal(t, InsertRequest{Database: "app", Bucket: "User", Key: "late", Data: map[string]interface{}{"N": 2}})
	_, err = router.routeWrite(context.Background(), ReplEntry{Op: int32(transport.OpPut), Bucket: "User", Payload: insert}, 0)
	if !errors.Is(err, ErrSpreadRecreated) {
		t.Fatalf("a write of the old incarnation returned %v, want the re-created refusal", err)
	}
	read := router.routeFanOut(context.Background(), transport.NewFrame(transport.OpCount, spreadMarshal(t, CountRequest{Database: "app", Bucket: "User"})))
	if hook := read.TakeWrittenHook(); hook != nil {
		hook()
	}
	if read.Opcode != transport.OpError || !strings.Contains(refusalTextOrEmptyTB(t, read), "dropped and created again") {
		t.Fatalf("a count of the old incarnation answered %s %q", read.Opcode, read.Payload)
	}
	got, err := router.routeRead(context.Background(), transport.NewFrame(transport.OpGet, spreadMarshal(t, FindRequest{Database: "app", Bucket: "User", Key: "late"})))
	if err != nil || got.Opcode != transport.OpError {
		t.Fatalf("a get of the old incarnation answered %v %v", got, err)
	}
	if count := fanOutCount(t, n.send(transport.OpCount, CountRequest{Database: "app", Bucket: "User"})); count != 0 {
		t.Fatalf("the new collection holds %d documents, want 0", count)
	}
	if got := partsHeld(n.s); got != 0 {
		t.Fatalf("%d parts of the old incarnation were made under the new collection", got)
	}
}

// TestRealDropNeverDeadlocksWithRoutedOperations is test 15 with the real
// router: 32 goroutines write, read, count, drop and re-create one collection
// on a node with no cluster, under -race, and all finish by the deadline. At
// the end no part is held after a last drop.
func TestRealDropNeverDeadlocksWithRoutedOperations(t *testing.T) {
	n := newSpreadNode(t, nil)
	const workers, rounds = 32, 60
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				key := fmt.Sprintf("w%d-%d", worker, i)
				switch k := (worker*7 + i*13) % 16; {
				case k == 0:
					n.dropReply()
				case k == 1:
					n.send(transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "User", Type: BucketTypeHash, Options: map[string]interface{}{"placement": "spread"}})
				case k < 4:
					n.send(transport.OpGet, FindRequest{Database: "app", Bucket: "User", Key: key})
				case k < 6:
					n.send(transport.OpCount, CountRequest{Database: "app", Bucket: "User"})
				case k == 6:
					n.send(transport.OpBatchPut, BatchInsertRequest{Database: "app", Bucket: "User", Data: []map[string]interface{}{{"_id_": key + "a"}, {"_id_": key + "b"}}})
				default:
					n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: key, Data: map[string]interface{}{"N": i}})
				}
			}
		}(worker)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(90 * time.Second):
		t.Fatal("writes, reads and drops of one collection did not finish: the gate and the fences deadlocked")
	}
	if left := n.s.routerGates.inFlight(); left != 0 {
		t.Fatalf("%d gate entries left", left)
	}
	// Whatever state the churn ended in, a last drop leaves nothing behind.
	if n.s.bucketStoreOpen("app", "User") {
		requireDropped(t, n.dropReply())
	}
	if got := partsHeld(n.s); got != 0 {
		t.Fatalf("%d parts are held after the last drop", got)
	}
	if held := n.s.queryInFlight(); held != 0 {
		t.Fatalf("%d execute slots held", held)
	}
}

// TestDropRecreateWriteAgreeOnWhatIsInTheCollection: documents written while
// the collection is dropped and created again are either in the new
// collection, wholly, or in none; a part never holds a document under another
// incarnation's name. After the churn the parts' part_of all equal the
// template's incarnation.
func TestDropRecreateKeepsEveryPartOfTheCurrentIncarnation(t *testing.T) {
	n := newSpreadNode(t, nil)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: fmt.Sprintf("c%d-%d", worker, i), Data: map[string]interface{}{"N": i}})
			}
		}(worker)
	}
	for round := 0; round < 8; round++ {
		n.dropReply()
		n.recreate()
		time.Sleep(20 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	template, err := n.s.loadSpreadTemplate("app", "User")
	if err != nil {
		t.Fatal(err)
	}
	for p := 0; p < defaultSpreadParts; p++ {
		part := partName("User", p)
		if !n.s.bucketStoreOpen("app", part) {
			continue
		}
		_, partOf, err := n.s.partFieldsOf("app", part)
		if err != nil || partOf != template.incarnation {
			t.Fatalf("part %d records part_of %d (%v), the template is incarnation %d", p, partOf, err, template.incarnation)
		}
	}
}
