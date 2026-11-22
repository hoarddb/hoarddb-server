package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Tests for step 2 of the spread MVP (document placement spread MVP note, rev
// 2.1, section 5 row 2): the router of keyed operations at every door, on a
// node that does not route. The cluster's doors are in
// spread_router_cluster_test.go.

// spreadNode is one node with no cluster (zero config), driven through its
// client door, with a spread collection app.User.
type spreadNode struct {
	t       *testing.T
	s       *Server
	handler transport.RequestHandler
}

// newSpreadNode starts a node with no cluster and creates app.User spread,
// with options added to the create's.
func newSpreadNode(t *testing.T, options map[string]interface{}) *spreadNode {
	t.Helper()
	s := newTestServer(t)
	if s.cluster != nil {
		t.Fatal("newTestServer started a cluster; this test is about a node with none")
	}
	n := &spreadNode{t: t, s: s, handler: NewHandler(s)}
	create := map[string]interface{}{"placement": "spread"}
	for key, value := range options {
		create[key] = value
	}
	reply := n.send(transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "User", Type: BucketTypeHash, Options: create})
	if reply.Opcode == transport.OpError || !strings.Contains(string(reply.Payload), "spread") {
		t.Fatalf("creating app.User spread answered %s %q", reply.Opcode, reply.Payload)
	}
	return n
}

// send sends one client frame through the node's client door.
func (n *spreadNode) send(op transport.Opcode, request any) *transport.Frame {
	n.t.Helper()
	payload, err := bson.Marshal(request)
	if err != nil {
		n.t.Fatalf("encoding %s: %v", op, err)
	}
	return n.sendRaw(op, payload)
}

func (n *spreadNode) sendRaw(op transport.Opcode, payload []byte) *transport.Frame {
	n.t.Helper()
	reply, err := n.handler(context.Background(), transport.NewFrame(op, payload))
	if err != nil {
		n.t.Fatalf("%s: the handler returned a Go error: %v", op, err)
	}
	// The answer is taken as written, as ServeFrame would: a query's
	// execute slot rides on it.
	if hook := reply.TakeWrittenHook(); hook != nil {
		hook()
	}
	return reply
}

// spreadPartOf is the part of User that holds key, at K = 256.
func spreadPartOf(t testing.TB, key string) int {
	t.Helper()
	encoded, err := store.EncodeComponent(key)
	if err != nil {
		t.Fatalf("encoding %q: %v", key, err)
	}
	p, err := store.PartOf(encoded, defaultSpreadParts)
	if err != nil {
		t.Fatalf("PartOf(%q): %v", key, err)
	}
	return p
}

// keysInPart returns count keys, prefix-<i>, whose part is target.
func keysInPart(t testing.TB, prefix string, target, count int) []string {
	t.Helper()
	var keys []string
	for i := 0; len(keys) < count; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		if spreadPartOf(t, key) == target {
			keys = append(keys, key)
		}
	}
	return keys
}

// keyOutsideParts returns a key, prefix-<i>, whose part is none of avoid.
func keyOutsideParts(t testing.TB, prefix string, avoid map[int]bool) string {
	t.Helper()
	for i := 0; ; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		if !avoid[spreadPartOf(t, key)] {
			return key
		}
	}
}

// templateCount is how many documents the template's own store holds, read
// past the router: the born set at most.
func templateCount(t testing.TB, s *Server, database, bucket string) int64 {
	t.Helper()
	reply, err := s.dispatch(context.Background(), transport.NewFrame(transport.OpCount, spreadMarshal(t, CountRequest{Database: database, Bucket: bucket})))
	if err != nil || reply.Opcode == transport.OpError {
		t.Fatalf("counting the template's own store: %v %q", err, reply.Payload)
	}
	// The count's execute slot is given back when its answer is written.
	if hook := reply.TakeWrittenHook(); hook != nil {
		hook()
	}
	value, err := bson.Raw(reply.Payload).LookupErr("count")
	if err != nil {
		t.Fatalf("count answer %q: %v", reply.Payload, err)
	}
	count, _ := value.AsInt64OK()
	return count
}

func spreadMarshal(t testing.TB, value any) []byte {
	t.Helper()
	payload, err := bson.Marshal(value)
	if err != nil {
		t.Fatalf("encoding %T: %v", value, err)
	}
	return payload
}

// findAnswer decodes a GET's answer.
func findAnswer(t testing.TB, reply *transport.Frame) FindResponse {
	t.Helper()
	if reply.Opcode == transport.OpError {
		t.Fatalf("a get was refused: %q", reply.Payload)
	}
	var found FindResponse
	if err := bson.Unmarshal(reply.Payload, &found); err != nil {
		t.Fatalf("decoding a get's answer: %v", err)
	}
	return found
}

// TestSpreadRoutesOnAZeroConfigNode is the zero-config door: a node with no
// cluster (s.cluster == nil) dispatches reads straight to its handlers and
// never reaches the cluster's one-member door, so the router is hooked there
// too. Every write lands in its part, every read is answered from it, and the
// template's store holds the born set and nothing else. On such a node a mark
// is confirmed when its local apply is committed (declared factor 1): each
// part a document went to reads 2 in the template's own store.
func TestSpreadRoutesOnAZeroConfigNode(t *testing.T) {
	n := newSpreadNode(t, nil)
	keys := make([]string, 0, 300)
	for i := 0; i < 300; i++ {
		key := fmt.Sprintf("user-%d", i)
		reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: key, Data: map[string]interface{}{"Name": key}})
		if reply.Opcode == transport.OpError {
			t.Fatalf("insert %s: %s", key, refusalText(t, reply))
		}
		keys = append(keys, key)
	}
	// An insert that names no id is minted one at the door, and routed by it.
	minted := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Data: map[string]interface{}{"Name": "minted"}})
	var mintedAnswer InsertResponse
	if err := bson.Unmarshal(minted.Payload, &mintedAnswer); err != nil || minted.Opcode == transport.OpError || mintedAnswer.ID == "" {
		t.Fatalf("an insert with no id answered %s %q", minted.Opcode, minted.Payload)
	}
	keys = append(keys, mintedAnswer.ID)

	born, err := n.s.readBornSet("app", "User")
	if err != nil {
		t.Fatal(err)
	}
	used := map[int]bool{}
	for _, key := range keys {
		p := spreadPartOf(t, key)
		used[p] = true
		if found := findAnswer(t, n.send(transport.OpGet, FindRequest{Database: "app", Bucket: "User", Key: key})); !found.Found {
			t.Fatalf("get %s through the template: not found", key)
		}
		inPart, err := n.s.Find(FindRequest{Database: "app", Bucket: partName("User", p), Key: key})
		if err != nil || !inPart.Found {
			t.Fatalf("document %s is not in its part %d: %+v, %v", key, p, inPart, err)
		}
		if state := born.state(p); state != partCreated {
			t.Fatalf("part %d holds %s but its committed mark is %d, want %d", p, key, state, partCreated)
		}
	}
	if len(used) < 100 {
		t.Fatalf("301 documents went to %d parts; the router is not spreading them", len(used))
	}
	if got := templateCount(t, n.s, "app", "User"); got != 1 {
		t.Fatalf("the template's own store holds %d documents, want 1 (the born set)", got)
	}
	exists := n.send(transport.OpExists, FindRequest{Database: "app", Bucket: "User", Key: keys[7]})
	if !strings.Contains(string(exists.Payload), "exists") || exists.Opcode == transport.OpError {
		t.Fatalf("exists answered %s %q", exists.Opcode, exists.Payload)
	}
	if value, err := bson.Raw(exists.Payload).LookupErr("exists"); err != nil || !value.Boolean() {
		t.Fatalf("exists of a stored key answered %q", exists.Payload)
	}
	update := n.send(transport.OpUpdate, UpdateRequest{Database: "app", Bucket: "User", Key: keys[3], Set: map[string]interface{}{"Age": 40}})
	if update.Opcode == transport.OpError || !replySucceeded(update.Opcode, update.Payload) {
		t.Fatalf("a keyed update answered %s %q", update.Opcode, update.Payload)
	}
	deleted := n.send(transport.OpDelete, DeleteRequest{Database: "app", Bucket: "User", Key: keys[4]})
	if deleted.Opcode == transport.OpError || !replySucceeded(deleted.Opcode, deleted.Payload) {
		t.Fatalf("a delete answered %s %q", deleted.Opcode, deleted.Payload)
	}
	if found := findAnswer(t, n.send(transport.OpGet, FindRequest{Database: "app", Bucket: "User", Key: keys[4]})); found.Found {
		t.Fatal("a deleted document is still found")
	}
	fast, err := transport.MarshalGetFast(transport.GetFastRequest{Database: "app", Bucket: "User", Key: keys[5]})
	if err != nil {
		t.Fatal(err)
	}
	if reply := n.sendRaw(transport.OpGetFast, fast); reply.Opcode != transport.OpGetFastResponse || len(reply.Payload) < 2 {
		t.Fatalf("get-fast answered %s %q", reply.Opcode, reply.Payload)
	}
	if gates := n.s.routerGates.inFlight(); gates != 0 {
		t.Fatalf("%d router gates are left held", gates)
	}
}

// TestKeyedOpsOfAnUnbornPartAreNotFound is R3 at the zero-config door: a key
// whose part is not created is answered not found by every keyed verb, from
// the user's name, asking no part, marking nothing and creating nothing. An
// orphan part of another incarnation that holds the key (an RF 1 router loss,
// then a re-create, in section 2.2's words) is never read by key, and the
// first insert into its part drops it and creates the part again (B4.2).
func TestKeyedOpsOfAnUnbornPartAreNotFound(t *testing.T) {
	n := newSpreadNode(t, map[string]interface{}{"stores": []interface{}{map[string]interface{}{"name": "Purchases", "engine": "fifoStore"}}})
	template, err := n.s.loadSpreadTemplate("app", "User")
	if err != nil {
		t.Fatal(err)
	}
	const key = "old-ada"
	p := spreadPartOf(t, key)
	orphan := createPartByHand(t, n.s, "User", p, template.incarnation+1)
	if resp, err := n.s.Insert(InsertRequest{Database: "app", Bucket: orphan, Key: key, Data: map[string]interface{}{"Name": "old"}}); err != nil || !resp.Success {
		t.Fatalf("seeding the orphan: %+v, %v", resp, err)
	}
	other := keyOutsideParts(t, "absent", map[int]bool{p: true})

	type row struct {
		op      transport.Opcode
		request any
		check   func(reply *transport.Frame) bool
	}
	notFound := func(reply *transport.Frame) bool {
		if reply.Opcode == transport.OpError {
			return strings.Contains(strings.ToLower(refusalText(t, reply)), "no document") && !strings.Contains(string(reply.Payload), unitSeparator)
		}
		var answer struct {
			Found   bool   `bson:"found"`
			Exists  bool   `bson:"exists"`
			Success bool   `bson:"success"`
			Message string `bson:"message"`
		}
		if err := bson.Unmarshal(reply.Payload, &answer); err != nil {
			return false
		}
		return !answer.Found && !answer.Exists && !answer.Success && !strings.Contains(answer.Message, unitSeparator)
	}
	for _, target := range []string{key, other} {
		rows := map[string]row{
			"get":            {transport.OpGet, FindRequest{Database: "app", Bucket: "User", Key: target}, notFound},
			"exists":         {transport.OpExists, FindRequest{Database: "app", Bucket: "User", Key: target}, notFound},
			"consistent get": {transport.OpConsistentGet, FindRequest{Database: "app", Bucket: "User", Key: target}, notFound},
			"delete":         {transport.OpDelete, DeleteRequest{Database: "app", Bucket: "User", Key: target}, notFound},
			"update":         {transport.OpUpdate, UpdateRequest{Database: "app", Bucket: "User", Key: target, Set: map[string]interface{}{"Age": 1}}, notFound},
			"field get":      {transport.OpFieldLen, FieldRequest{Database: "app", Bucket: "User", Key: target, Field: "Purchases"}, notFound},
			"field push":     {transport.OpFieldPush, FieldRequest{Database: "app", Bucket: "User", Key: target, Field: "Purchases", Data: map[string]interface{}{"sku": 1}}, notFound},
		}
		for label, row := range rows {
			if reply := n.send(row.op, row.request); !row.check(reply) {
				t.Errorf("%s of %s in an unborn part answered %s %q, want not found", label, target, reply.Opcode, reply.Payload)
			}
		}
		fast, err := transport.MarshalGetFast(transport.GetFastRequest{Database: "app", Bucket: "User", Key: target})
		if err != nil {
			t.Fatal(err)
		}
		if reply := n.sendRaw(transport.OpGetFast, fast); reply.Opcode != transport.OpGetFastResponse || reply.Payload[0] != byte(transport.GetFastNotFound) {
			t.Errorf("get-fast of %s in an unborn part answered %s %q", target, reply.Opcode, reply.Payload)
		}
	}
	if templateCount(t, n.s, "app", "User") != 0 {
		t.Error("a keyed operation of an unborn part marked the born set")
	}
	if !n.s.bucketStoreOpen("app", orphan) {
		t.Fatal("a keyed read or a delete touched the orphan")
	}
	if n.s.bucketStoreOpen("app", partName("User", spreadPartOf(t, other))) {
		t.Fatal("a delete, update or field write of an unborn key created its part")
	}

	// The first insert into the orphan's part drops it and creates the part
	// for this incarnation: the old document is gone, the new one is there.
	fresh := keysInPart(t, "fresh", p, 1)[0]
	if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: fresh, Data: map[string]interface{}{"Name": "new"}}); reply.Opcode == transport.OpError {
		t.Fatalf("insert into the orphan's part: %s", refusalText(t, reply))
	}
	if found := findAnswer(t, n.send(transport.OpGet, FindRequest{Database: "app", Bucket: "User", Key: key})); found.Found {
		t.Fatal("the orphan's document came back after its part was created for this incarnation")
	}
	if found := findAnswer(t, n.send(transport.OpGet, FindRequest{Database: "app", Bucket: "User", Key: fresh})); !found.Found {
		t.Fatal("the document written into the re-created part is not found")
	}
	if _, partOf, err := n.s.partFieldsOf("app", orphan); err != nil || partOf != template.incarnation {
		t.Fatalf("the part's part_of is %d (%v), want the template's incarnation %d", partOf, err, template.incarnation)
	}
}

// TestPartCreateCarriesTheTemplateDeclaration: a part is created with the
// template's declaration, so a document is judged in its part exactly as the
// collection declares it: typed fields, store fields, indexes and factor.
func TestPartCreateCarriesTheTemplateDeclaration(t *testing.T) {
	n := newSpreadNode(t, map[string]interface{}{
		"fields":      []interface{}{map[string]interface{}{"name": "Age", "type": "int"}, map[string]interface{}{"name": "Name", "type": "string", "required": true}},
		"field_types": "strict",
		"stores": []interface{}{
			map[string]interface{}{"name": "Purchases", "engine": "fifoStore"},
			map[string]interface{}{"name": "Friends", "engine": "hashStore"},
		},
		"indices": []interface{}{"Age"},
	})
	if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: "ada", Data: map[string]interface{}{"Name": "Ada", "Age": 36}}); reply.Opcode == transport.OpError {
		t.Fatalf("insert: %s", refusalText(t, reply))
	}
	template, _, err := n.s.readCatalogEntry("app", "User")
	if err != nil {
		t.Fatal(err)
	}
	part, found, err := n.s.readCatalogEntry("app", partName("User", spreadPartOf(t, "ada")))
	if err != nil || !found {
		t.Fatalf("the part's entry: %v, %v", found, err)
	}
	if part.Placement != placementWhole || part.Part != int32(spreadPartOf(t, "ada")) || part.PartOf == 0 || part.Parts != 0 {
		t.Fatalf("the part is recorded %+v", part)
	}
	compare := func(what string, a, b any) {
		t.Helper()
		if fmt.Sprintf("%v", a) != fmt.Sprintf("%v", b) {
			t.Errorf("the part's %s is %v, the template's %v", what, b, a)
		}
	}
	compare("typed fields", template.Fields, part.Fields)
	compare("field mode", template.FieldTypes, part.FieldTypes)
	compare("factor", template.ReplicationFactor, part.ReplicationFactor)
	storeNames := func(stores []catalogStore) []string {
		var names []string
		for _, declared := range stores {
			names = append(names, declared.Name+":"+declared.Engine)
		}
		return names
	}
	compare("store fields", storeNames(template.Stores), storeNames(part.Stores))
	indexNames := func(indices []catalogIndex) []string {
		var names []string
		for position, recorded := range indices {
			names = append(names, indexSpecOf(position, recorded).Name)
		}
		return names
	}
	compare("indexes", indexNames(template.Indices), indexNames(part.Indices))
	// Judged in its part as the collection declares it.
	if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: "bad", Data: map[string]interface{}{"Name": "B", "Age": "old"}}); reply.Opcode != transport.OpError {
		t.Fatalf("a mistyped document was stored: %s %q", reply.Opcode, reply.Payload)
	}
	if reply := n.send(transport.OpFieldPush, FieldRequest{Database: "app", Bucket: "User", Key: "ada", Field: "Purchases", Data: map[string]interface{}{"sku": 7}}); reply.Opcode == transport.OpError {
		t.Fatalf("a push into the part's store field: %s", refusalText(t, reply))
	}
}

// TestClientCannotSetAdmittedOnUpdate: the admitted flag of an UPDATE is the
// router's, for its own write of the born set, which the collection's typed
// fields do not judge; a client that sets it is refused at the door (E-12),
// as on a put.
func TestClientCannotSetAdmittedOnUpdate(t *testing.T) {
	n := newSpreadNode(t, map[string]interface{}{"fields": []interface{}{map[string]interface{}{"name": "Age", "type": "int"}}})
	if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: "ada", Data: map[string]interface{}{"Age": 36}}); reply.Opcode == transport.OpError {
		t.Fatalf("insert: %s", refusalText(t, reply))
	}
	reply := n.send(transport.OpUpdate, UpdateRequest{Database: "app", Bucket: "User", Key: "ada", Set: map[string]interface{}{"Age": "old"}, Admitted: true})
	if reply.Opcode != transport.OpError || !strings.Contains(refusalText(t, reply), "admitted") {
		t.Fatalf("a client update setting admitted answered %s %q", reply.Opcode, reply.Payload)
	}
	if found := findAnswer(t, n.send(transport.OpGet, FindRequest{Database: "app", Bucket: "User", Key: "ada"})); strings.Contains(found.Data.String(), "old") {
		t.Fatalf("the mistyped value was stored: %s", found.Data)
	}
}

// collectionDocumentOf reads key from this node's own store where the
// collection keeps it: in the part a spread template routes it to, or in the
// bucket itself. nil when this node's copy does not hold it.
func collectionDocumentOf(t *testing.T, s *Server, database, bucket, key string) map[string]interface{} {
	t.Helper()
	if part := partName(bucket, spreadPartOf(t, key)); s.bucketStoreOpen(database, part) {
		return documentOf(t, s, database, part, key)
	}
	if !s.bucketStoreOpen(database, bucket) || s.spreadTemplateHeld(database, bucket) {
		return nil
	}
	return documentOf(t, s, database, bucket, key)
}

// routedInsert inserts a document through the node's client door, where a
// spread bucket's router sends it to its part.
func routedInsert(t *testing.T, s *Server, database, bucket, key string, doc map[string]interface{}) {
	t.Helper()
	reply, err := NewHandler(s)(context.Background(), transport.NewFrame(transport.OpPut, spreadMarshal(t, InsertRequest{Database: database, Bucket: bucket, Key: key, Data: doc})))
	if err != nil || reply.Opcode == transport.OpError {
		t.Fatalf("inserting %s into %s/%s: %v %q", key, database, bucket, err, reply.Payload)
	}
}
