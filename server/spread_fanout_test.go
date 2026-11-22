package server

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Tests for step 3 of the spread MVP (document placement spread MVP note, rev
// 2.1, section 3 and section 5 row 3): the reads that cover every part of a
// spread collection, at the doors of a node that does not route. The
// cluster's doors are in spread_fanout_cluster_test.go.

// fanOutOptions is the declaration both twins are created with: an index to
// search by and a TTL index to sweep by.
func fanOutOptions() map[string]interface{} {
	return map[string]interface{}{"indices": []interface{}{"Age", ttlDeclaration("CreatedAt", 3600)}}
}

// fanOutTwins is a whole bucket and a spread collection of the same name,
// declaration and documents, each on its own node with no cluster, so every
// answer can be compared byte for byte with nothing renamed.
type fanOutTwins struct {
	t      *testing.T
	whole  *spreadNode
	spread *spreadNode
}

func newFanOutTwins(t *testing.T) *fanOutTwins {
	t.Helper()
	spread := newSpreadNode(t, fanOutOptions())
	s := newTestServer(t)
	whole := &spreadNode{t: t, s: s, handler: NewHandler(s)}
	options := fanOutOptions()
	options["placement"] = "whole"
	if reply := whole.send(transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "User", Type: BucketTypeHash, Options: options}); reply.Opcode == transport.OpError ||
		!strings.Contains(string(reply.Payload), "whole") {
		t.Fatalf("creating app.User whole answered %s %q", reply.Opcode, reply.Payload)
	}
	return &fanOutTwins{t: t, whole: whole, spread: spread}
}

// both sends one write to each twin and requires the same answer.
func (tw *fanOutTwins) both(op transport.Opcode, request any) {
	tw.t.Helper()
	wholeReply, spreadReply := tw.whole.send(op, request), tw.spread.send(op, request)
	if wholeReply.Opcode == transport.OpError || spreadReply.Opcode == transport.OpError {
		tw.t.Fatalf("%s: whole %s %q, spread %s %q", op, wholeReply.Opcode, wholeReply.Payload, spreadReply.Opcode, spreadReply.Payload)
	}
}

// fanOutDocument is document i: a distinct Name, an Age with ties, an
// Address on two of three, a Tags array, and a CreatedAt that the TTL sweep
// finds expired for one document in five.
func fanOutDocument(i int, now time.Time) map[string]interface{} {
	created := now
	if i%5 == 0 {
		created = now.Add(-2 * time.Hour)
	}
	document := map[string]interface{}{
		"Name":      fmt.Sprintf("name-%05d", (i*7919)%100000),
		"Age":       int32(20 + i%40),
		"Tags":      []interface{}{fmt.Sprintf("t%d", i%3), fmt.Sprintf("t%d", i%11)},
		"CreatedAt": primitive.NewDateTimeFromTime(created),
	}
	if i%3 != 0 {
		document["Address"] = map[string]interface{}{"City": fmt.Sprintf("city-%d", i%17)}
	}
	return document
}

// fanOutKey is document i's _id_.
func fanOutKey(i int) string { return fmt.Sprintf("u-%05d", i) }

// fanOutQuery is one read and how its two answers are compared: byte for
// byte after dropping the fields named in ignore, or, for an answer whose
// order across parts is not a whole bucket's (distinct, search), as sets.
type fanOutQuery struct {
	label   string
	op      transport.Opcode
	request any
	ignore  []string
	asSet   string
}

// fanOutQueries is every read of the fan-out class a test compares.
func fanOutQueries() []fanOutQuery {
	scan := func(label string, request ScanRequest) fanOutQuery {
		request.Database, request.Bucket = "app", "User"
		return fanOutQuery{label: label, op: transport.OpScan, request: request, ignore: []string{"bucket_id"}}
	}
	gt := func(field string, value int32) map[string]interface{} {
		return map[string]interface{}{field: map[string]interface{}{"$gt": value}}
	}
	return []fanOutQuery{
		{label: "count", op: transport.OpCount, request: CountRequest{Database: "app", Bucket: "User"}},
		{label: "count filtered", op: transport.OpCount, request: CountRequest{Database: "app", Bucket: "User", Filter: gt("Age", 40)}},
		scan("find", ScanRequest{}),
		scan("find all", ScanRequest{Limit: 5000}),
		scan("find skip", ScanRequest{Skip: 50, Limit: 300}),
		// Pages that end exactly at the last document of a phase: one part's
		// six, and all 256 parts' 2,000, where has_more must be false.
		scan("find ending at six", ScanRequest{Skip: 4, Limit: 2}),
		scan("find ending at 2000", ScanRequest{Skip: 1990, Limit: 10}),
		scan("sorted find ending at 2000", ScanRequest{Sort: map[string]int{"Name": 1}, Skip: 1990, Limit: 10}),
		scan("find filtered", ScanRequest{Filter: gt("Age", 30), Skip: 10, Limit: 50}),
		scan("find projected", ScanRequest{Projection: map[string]int{"Name": 1}, Limit: 200}),
		scan("find without the id", ScanRequest{Projection: map[string]int{"_id_": 0}, Skip: 5, Limit: 200}),
		scan("sorted by a distinct field", ScanRequest{Sort: map[string]int{"Name": 1}, Skip: 10, Limit: 20}),
		scan("sorted descending", ScanRequest{Sort: map[string]int{"Name": -1}, Limit: 150}),
		scan("sorted by a tied field", ScanRequest{Sort: map[string]int{"Age": 1}, Limit: 300}),
		scan("sorted by a tied field descending", ScanRequest{Sort: map[string]int{"Age": -1}, Skip: 7, Limit: 300}),
		scan("sorted and projected", ScanRequest{Sort: map[string]int{"Age": 1}, Projection: map[string]int{"Name": 1}, Limit: 250}),
		scan("sorted under an excluded ancestor", ScanRequest{Sort: map[string]int{"Address.City": 1}, Projection: map[string]int{"Address": 0}, Limit: 250}),
		scan("sorted by a position", ScanRequest{Sort: map[string]int{"Tags[1]": -1}, Projection: map[string]int{"Tags": 1}, Skip: 3, Limit: 250}),
		scan("sorted and filtered", ScanRequest{Filter: gt("Age", 50), Sort: map[string]int{"Name": 1}, Limit: 5000}),
		{label: "distinct", op: transport.OpDistinct, request: DistinctRequest{Database: "app", Bucket: "User", Field: "Age"}, asSet: "values"},
		{label: "distinct filtered", op: transport.OpDistinct, request: DistinctRequest{Database: "app", Bucket: "User", Field: "Address.City", Filter: gt("Age", 30)}, asSet: "values"},
		{label: "search by index", op: transport.OpSearch, request: SearchRequest{Database: "app", Bucket: "User", Filter: map[string]interface{}{"Age": int32(33)}}, asSet: "results"},
		{label: "search by scan", op: transport.OpSearch, request: SearchRequest{Database: "app", Bucket: "User", Filter: map[string]interface{}{"Name": "name-07919"}}, asSet: "results"},
		{label: "describe", op: transport.OpBucketStats, request: BucketStatsRequest{Database: "app", Bucket: "User"}, ignore: []string{"placement"}},
		{label: "info", op: transport.OpBucketInfo, request: BucketInfoRequest{Database: "app", Bucket: "User"}},
	}
}

// compare sends every query to both twins and requires the answers equal as
// the query says.
func (tw *fanOutTwins) compare(phase string) {
	tw.t.Helper()
	for _, query := range fanOutQueries() {
		wholeReply, spreadReply := tw.whole.send(query.op, query.request), tw.spread.send(query.op, query.request)
		if spreadReply.Opcode != wholeReply.Opcode {
			tw.t.Errorf("%s, %s: whole answered %s %q, spread %s %q", phase, query.label, wholeReply.Opcode, wholeReply.Payload, spreadReply.Opcode, spreadReply.Payload)
			continue
		}
		if bsonNamesAPart(spreadReply.Payload) {
			tw.t.Errorf("%s, %s: the spread answer names a part: %q", phase, query.label, spreadReply.Payload)
		}
		want, got := normalisedAnswer(tw.t, wholeReply.Payload, query), normalisedAnswer(tw.t, spreadReply.Payload, query)
		if !bytes.Equal(want, got) {
			tw.t.Errorf("%s, %s: the spread answer differs from the whole bucket's:\nwhole  %s\nspread %s", phase, query.label, bson.Raw(want), bson.Raw(got))
		}
	}
}

// normalisedAnswer is an answer with the ignored fields dropped and a set's
// array sorted by its elements' bytes.
func normalisedAnswer(t testing.TB, payload []byte, query fanOutQuery) []byte {
	t.Helper()
	var answer bson.D
	if err := bson.Unmarshal(payload, &answer); err != nil {
		t.Fatalf("%s: decoding %q: %v", query.label, payload, err)
	}
	kept := answer[:0]
	for _, element := range answer {
		ignored := false
		for _, name := range query.ignore {
			ignored = ignored || element.Key == name
		}
		if ignored {
			continue
		}
		if element.Key == query.asSet {
			element.Value = sortedSet(t, payload, query.asSet)
		}
		kept = append(kept, element)
	}
	encoded, err := bson.Marshal(kept)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// sortedSet is the array field of payload, each element re-encoded in a
// document of its own and sorted by those bytes.
func sortedSet(t testing.TB, payload []byte, field string) []string {
	t.Helper()
	value, err := bson.Raw(payload).LookupErr(field)
	if err != nil {
		return nil
	}
	array, ok := value.ArrayOK()
	if !ok {
		return nil
	}
	values, err := array.Values()
	if err != nil {
		t.Fatal(err)
	}
	set := make([]string, 0, len(values))
	for _, element := range values {
		set = append(set, fmt.Sprintf("%d:%x", element.Type, element.Value))
	}
	sort.Strings(set)
	return set
}

// createdParts is how many parts of User the born set records created.
func createdParts(t testing.TB, s *Server) int {
	t.Helper()
	born, err := s.readBornSet("app", "User")
	if err != nil {
		t.Fatal(err)
	}
	created := 0
	for p := 0; p < defaultSpreadParts; p++ {
		if born.state(p) == partCreated {
			created++
		}
	}
	return created
}

// TestSpreadFanOutMatchesAWholeBucket is the differential test of step 3: the
// same writes go to a whole bucket and to a spread collection, and every read
// of the fan-out class answers the same, byte for byte with the bucket's id
// and placement aside, and distinct and search compared as sets, whose order
// across parts is the parts' (section 3): on an empty collection, with one
// part, with all 256, after deletes, after updates, and after a TTL sweep.
func TestSpreadFanOutMatchesAWholeBucket(t *testing.T) {
	tw := newFanOutTwins(t)
	now := time.Now()
	tw.compare("empty")

	one := keysInPart(t, "solo", 7, 6)
	for i, key := range one {
		tw.both(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: key, Data: fanOutDocument(i, now)})
	}
	if got := createdParts(t, tw.spread.s); got != 1 {
		t.Fatalf("%d parts created for one part's keys", got)
	}
	tw.compare("one part")

	const documents = 2000
	for start := 0; start < documents; start += 200 {
		batch := make([]map[string]interface{}, 0, 200)
		for i := start; i < start+200; i++ {
			document := fanOutDocument(i, now)
			document["_id_"] = fanOutKey(i)
			batch = append(batch, document)
		}
		tw.both(transport.OpBatchPut, BatchInsertRequest{Database: "app", Bucket: "User", Data: batch})
	}
	if got := createdParts(t, tw.spread.s); got != defaultSpreadParts {
		t.Fatalf("%d parts created for %d documents, want all %d", got, documents, defaultSpreadParts)
	}
	tw.compare("all 256 parts")

	for i := 0; i < documents; i += 7 {
		tw.both(transport.OpDelete, DeleteRequest{Database: "app", Bucket: "User", Key: fanOutKey(i)})
	}
	tw.compare("after deletes")

	for i := 1; i < documents; i += 5 {
		tw.both(transport.OpUpdate, UpdateRequest{Database: "app", Bucket: "User", Key: fanOutKey(i),
			Set: map[string]interface{}{"Age": int32(99), "Name": fmt.Sprintf("renamed-%05d", i)}})
	}
	tw.compare("after updates")

	tw.whole.s.sweepTTL(context.Background(), time.Now())
	tw.spread.s.sweepTTL(context.Background(), time.Now())
	tw.compare("after a TTL sweep")
}

// TestSpreadFanOutSkipsAPartStillBeingCreated: a part the born set records at
// 1 (marked, its create perhaps in flight) holds no document of the
// collection's (section 2.2) and is not asked: a document stored in it past
// the router is not counted, found, or described.
func TestSpreadFanOutSkipsAPartStillBeingCreated(t *testing.T) {
	n := newSpreadNode(t, nil)
	created := keysInPart(t, "made", 3, 4)
	for _, key := range created {
		if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: key, Data: map[string]interface{}{"N": 1}}); reply.Opcode == transport.OpError {
			t.Fatal(refusalText(t, reply))
		}
	}
	template, err := n.s.loadSpreadTemplate("app", "User")
	if err != nil {
		t.Fatal(err)
	}
	const marked = 9
	part := createPartByHand(t, n.s, "User", marked, template.incarnation)
	if resp, err := n.s.Insert(InsertRequest{Database: "app", Bucket: part, Key: keysInPart(t, "stray", marked, 1)[0], Data: map[string]interface{}{"N": 2}}); err != nil || !resp.Success {
		t.Fatalf("seeding the marked part: %+v %v", resp, err)
	}
	if resp, err := n.s.Update(UpdateRequest{Database: "app", Bucket: "User", Key: bornDocumentID, Max: map[string]interface{}{bornField(marked): partMarked}, Admitted: true}); err != nil || !resp.Success {
		t.Fatalf("marking part %d at 1: %+v %v", marked, resp, err)
	}
	if count := fanOutCount(t, n.send(transport.OpCount, CountRequest{Database: "app", Bucket: "User"})); count != int64(len(created)) {
		t.Errorf("count() is %d, want %d: a part at 1 was counted", count, len(created))
	}
	var page ScanResponse
	decodeAnswer(t, n.send(transport.OpScan, ScanRequest{Database: "app", Bucket: "User", Limit: 100}), &page)
	if len(page.Documents) != len(created) || page.Total != int64(len(created)) {
		t.Errorf("find() returned %d of total %d, want %d", len(page.Documents), page.Total, len(created))
	}
	var described BucketStatsResponse
	decodeAnswer(t, n.send(transport.OpBucketStats, BucketStatsRequest{Database: "app", Bucket: "User"}), &described)
	if described.Entries != int64(len(created)) {
		t.Errorf("describe counts %d entries, want %d", described.Entries, len(created))
	}
}

// asRoot sends one client frame through the node's client door as the root
// user, whom dump requires.
func (n *spreadNode) asRoot(op transport.Opcode, request any) *transport.Frame {
	n.t.Helper()
	reply, err := n.handler(rootGrantedContext(), transport.NewFrame(op, spreadMarshal(n.t, request)))
	if err != nil {
		n.t.Fatalf("%s: the handler returned a Go error: %v", op, err)
	}
	if hook := reply.TakeWrittenHook(); hook != nil {
		hook()
	}
	return reply
}

// rootGrantedContext is the client context of the root user holding the root
// role on every database, which a dump through the client door requires.
func rootGrantedContext() context.Context {
	session := &transport.Session{}
	session.SetIdentity(transport.Identity{Username: "admin", Grants: []transport.ScopedGrant{{Role: roleRoot, DB: "*"}}})
	return transport.WithSession(context.Background(), session)
}

// bsonNamesAPart reports whether any key or text value of a BSON answer,
// at any depth, shows a part's name. The answer's binary framing is not
// text and is not read.
func bsonNamesAPart(payload []byte) bool {
	elements, err := bson.Raw(payload).Elements()
	if err != nil {
		return false
	}
	for _, element := range elements {
		if namesAPart(element.Key()) {
			return true
		}
		value := element.Value()
		if text, ok := value.StringValueOK(); ok && namesAPart(text) {
			return true
		}
		if nested, ok := value.DocumentOK(); ok && bsonNamesAPart(nested) {
			return true
		}
		if nested, ok := value.ArrayOK(); ok && bsonNamesAPart(nested) {
			return true
		}
	}
	return false
}

// fanOutCount is a count's answer, failing the test on a refusal.
func fanOutCount(t testing.TB, reply *transport.Frame) int64 {
	t.Helper()
	var counted CountResponse
	decodeAnswer(t, reply, &counted)
	return counted.Count
}

// decodeAnswer decodes a reply into into, failing the test on a refusal.
func decodeAnswer(t testing.TB, reply *transport.Frame, into any) {
	t.Helper()
	if reply.Opcode == transport.OpError {
		t.Fatalf("refused: %s", refusalTextOrEmptyTB(t, reply))
	}
	if err := bson.Unmarshal(reply.Payload, into); err != nil {
		t.Fatalf("decoding %s: %v", reply.Opcode, err)
	}
}

// refusalTextOrEmptyTB is refusalTextOrEmpty for a testing.TB.
func refusalTextOrEmptyTB(t testing.TB, reply *transport.Frame) string {
	t.Helper()
	var refused ErrorResponse
	if reply.Opcode != transport.OpError || bson.Unmarshal(reply.Payload, &refused) != nil {
		return ""
	}
	return refused.Message
}

// fanOutRequests is one request of every opcode of the fan-out class that a
// client sends on its own, naming app.User.
func fanOutRequests() map[string]struct {
	op      transport.Opcode
	request any
} {
	return map[string]struct {
		op      transport.Opcode
		request any
	}{
		"count":    {transport.OpCount, CountRequest{Database: "app", Bucket: "User"}},
		"find":     {transport.OpScan, ScanRequest{Database: "app", Bucket: "User"}},
		"distinct": {transport.OpDistinct, DistinctRequest{Database: "app", Bucket: "User", Field: "N"}},
		"search":   {transport.OpSearch, SearchRequest{Database: "app", Bucket: "User", Filter: map[string]interface{}{"N": int32(1)}}},
		"describe": {transport.OpBucketStats, BucketStatsRequest{Database: "app", Bucket: "User"}},
		"info":     {transport.OpBucketInfo, BucketInfoRequest{Database: "app", Bucket: "User"}},
		"dump":     {transport.OpDumpStart, DumpStartRequest{Database: "app", Bucket: "User", AcceptStores: true, AcceptDead: true}},
	}
}

// TestLostPartRefusesTheFanOut is B9 for the fan-out at the one-member door:
// a part the born set records created that its node does not hold refuses
// every read of the collection, UNAVAILABLE and naming no part; never a short
// count with no error.
func TestLostPartRefusesTheFanOut(t *testing.T) {
	n := newSpreadNode(t, nil)
	for i := 0; i < 40; i++ {
		if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: fmt.Sprintf("k-%d", i), Data: map[string]interface{}{"N": int32(1)}}); reply.Opcode == transport.OpError {
			t.Fatal(refusalText(t, reply))
		}
	}
	p := spreadPartOf(t, "k-0")
	drop := transport.NewFrame(transport.OpDeleteBucket, spreadMarshal(t, DeleteBucketRequest{Database: "app", Name: partName("User", p)}))
	if reply, err := n.s.fencedDispatch(context.Background(), drop); err != nil || reply.Opcode == transport.OpError {
		t.Fatalf("dropping the part past the router: %v %q", err, reply.Payload)
	}
	for label, read := range fanOutRequests() {
		reply := n.asRoot(read.op, read.request)
		if read.op == transport.OpDumpStart && reply.Opcode == transport.OpDumpStart {
			// A dump meets the lost part when it reaches it: a chunk is
			// refused, and the dump never ends cleanly.
			for reply.Opcode != transport.OpError {
				reply = n.asRoot(transport.OpDumpChunk, DumpChunkRequest{Database: "app", Bucket: "User"})
				if eof, _ := bson.Raw(reply.Payload).Lookup("eof").BooleanOK(); eof {
					t.Fatal("a dump with a lost part ended cleanly")
				}
			}
		}
		text := refusalTextOrEmpty(t, reply)
		if reply.Opcode != transport.OpError || errorCodeOf(t, reply) != transport.ErrUnavailable || !strings.Contains(text, "not held") ||
			bsonNamesAPart(reply.Payload) {
			t.Errorf("%s with a lost part answered %s %q, want the lost-part refusal", label, reply.Opcode, reply.Payload)
		}
	}
	if n.s.bucketStoreOpen("app", partName("User", p)) {
		t.Error("a read created the lost part again")
	}
}

// TestOneMemberFanOutReleasesEverySlot is test 19 (R1, the one-member door):
// with the query pool at 4 and a collection of 12 parts, 20 consecutive
// count() and find() calls each answer, and no execute slot is held after
// each: the router takes one slot per part read and gives it back as the part
// answers, and admits the fan-out itself from the forward pool.
func TestOneMemberFanOutReleasesEverySlot(t *testing.T) {
	pinPool := func(cfg *config.Config) { cfg.Query.Concurrency = 4 }
	run := func(t *testing.T, send func(transport.Opcode, any) *transport.Frame, s *Server) {
		for p := 0; p < 12; p++ {
			key := keysInPart(t, fmt.Sprintf("slot%d", p), p*20, 1)[0]
			if reply := send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: key, Data: map[string]interface{}{"N": int32(p)}}); reply.Opcode == transport.OpError {
				t.Fatal(refusalText(t, reply))
			}
		}
		for i := 0; i < 20; i++ {
			done := make(chan *transport.Frame, 1)
			op, request := transport.OpCount, any(CountRequest{Database: "app", Bucket: "User"})
			if i%2 == 1 {
				op, request = transport.OpScan, ScanRequest{Database: "app", Bucket: "User"}
			}
			go func() { done <- send(op, request) }()
			select {
			case reply := <-done:
				if reply.Opcode == transport.OpError {
					t.Fatalf("call %d (%s) refused: %s", i+1, op, refusalText(t, reply))
				}
				if hook := reply.TakeWrittenHook(); hook != nil {
					hook()
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("call %d (%s) did not answer in 5 s; execute slots in use: %d", i+1, op, s.queryInFlight())
			}
			// Over a connection the slots ride on the answer and are given
			// back once the server has written it, which can be a moment
			// after the client has read it.
			deadline := time.Now().Add(2 * time.Second)
			for s.queryInFlight() != 0 || s.forwardInFlight() != 0 {
				if time.Now().After(deadline) {
					t.Fatalf("after call %d (%s), %d execute and %d forward slots are still held", i+1, op, s.queryInFlight(), s.forwardInFlight())
				}
				time.Sleep(time.Millisecond)
			}
		}
	}
	t.Run("a node with no cluster", func(t *testing.T) {
		s := newTestServerWith(t, pinPool)
		n := &spreadNode{t: t, s: s, handler: NewHandler(s)}
		if reply := n.send(transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "User", Type: BucketTypeHash,
			Options: map[string]interface{}{"placement": "spread"}}); reply.Opcode == transport.OpError {
			t.Fatal(refusalText(t, reply))
		}
		run(t, n.send, s)
	})
	t.Run("a one-member cluster", func(t *testing.T) {
		tc := newTestClusterWith(t, 1, 1, func(_ int, cfg *config.Config) { pinPool(cfg) })
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "User", Type: BucketTypeHash, Options: map[string]interface{}{"placement": "spread"}})
		run(t, func(op transport.Opcode, request any) *transport.Frame { return tc.frame(0, op, request) }, tc.nodes[0].server.core)
	})
}

// dumpCollection dumps app.<bucket> through handler as the CLI does: start,
// chunks to EOF, done with the digests of what it received. It returns the
// documents, the start's manifest and the done's answer.
func dumpCollection(t *testing.T, send func(transport.Opcode, any) *transport.Frame, bucket string) ([]bson.Raw, DumpStartResponse, DumpDoneResponse) {
	t.Helper()
	var started DumpStartResponse
	decodeAnswer(t, send(transport.OpDumpStart, DumpStartRequest{Database: "app", Bucket: bucket, AcceptStores: true, AcceptDead: true}), &started)
	var documents []bson.Raw
	var docsFile, storesFile []byte
	for {
		var chunk DumpChunkResponse
		decodeAnswer(t, send(transport.OpDumpChunk, DumpChunkRequest{Database: "app", Bucket: bucket}), &chunk)
		switch chunk.Section {
		case "":
			docsFile = append(docsFile, chunk.Data...)
			for rest := chunk.Data; len(rest) > 0; {
				size := int(rest[0]) | int(rest[1])<<8 | int(rest[2])<<16 | int(rest[3])<<24
				documents = append(documents, bson.Raw(rest[:size]))
				rest = rest[size:]
			}
		case dumpSectionStores:
			storesFile = append(storesFile, chunk.Data...)
		}
		if chunk.EOF {
			break
		}
	}
	var done DumpDoneResponse
	decodeAnswer(t, send(transport.OpDumpDone, DumpDoneRequest{Database: "app", Bucket: bucket, Digest: digestOf(docsFile), StoresDigest: digestOf(storesFile)}), &done)
	return documents, started, done
}

// TestDumpOfASpreadCollectionFansIn is B3 at the one-member door: the dump of
// a spread collection is one stream of every document of every part, under
// the collection's name, whose digests the server checks and whose manifest
// says spread; and it holds the same documents a whole bucket's dump holds.
func TestDumpOfASpreadCollectionFansIn(t *testing.T) {
	tw := newFanOutTwins(t)
	now := time.Now()
	for i := 0; i < 600; i++ {
		tw.both(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: fanOutKey(i), Data: fanOutDocument(i, now)})
	}
	wholeDocs, _, wholeDone := dumpCollection(t, tw.whole.asRoot, "User")
	spreadDocs, started, done := dumpCollection(t, tw.spread.asRoot, "User")
	if started.Manifest == nil || started.Manifest.Bucket != "User" || started.Manifest.Placement != placementSpread {
		t.Fatalf("the spread dump's manifest is %+v, want bucket User, placement spread", started.Manifest)
	}
	if done.Documents != 600 || int64(len(spreadDocs)) != done.Documents || done.Documents != wholeDone.Documents {
		t.Fatalf("the spread dump streamed %d documents and counted %d; the whole dump %d", len(spreadDocs), done.Documents, wholeDone.Documents)
	}
	sortRaw := func(docs []bson.Raw) {
		sort.Slice(docs, func(i, j int) bool { return bytes.Compare(docs[i], docs[j]) < 0 })
	}
	sortRaw(wholeDocs)
	sortRaw(spreadDocs)
	for i := range wholeDocs {
		if !bytes.Equal(wholeDocs[i], spreadDocs[i]) {
			t.Fatalf("document %d differs:\nwhole  %s\nspread %s", i, wholeDocs[i], spreadDocs[i])
		}
	}
	if done.Bytes != wholeDone.Bytes {
		t.Errorf("the spread dump streamed %d bytes, the whole one %d", done.Bytes, wholeDone.Bytes)
	}
	// The router claimed no dump slot: a second dump starts at once after.
	if again, _, _ := dumpCollection(t, tw.spread.asRoot, "User"); len(again) != 600 {
		t.Errorf("a second dump streamed %d documents", len(again))
	}
	if left := tw.spread.s.spreadDumps.running(); left != 0 {
		t.Errorf("%d router dump sessions left after both dumps ended", left)
	}
}

// namesAPart reports whether text shows a part's name, raw or escaped as %q
// and a log's quoting print it.
func namesAPart(text string) bool {
	return strings.Contains(text, unitSeparator) || strings.Contains(text, `\x1f`) || strings.Contains(text, `\u001f`)
}

// TestPartNameNeverReachesAClient is test 9 for the fan-out and the dump: no
// answer, refusal, describe, dump manifest, dump chunk or WARN/ERROR log line
// shows a part's name. A part's own refusal (a result too large) comes back
// under the collection's name; a dump of a second spread collection that
// meets the first one's part on the node's one dump slot is refused naming
// the first collection, not its part; a lost part's refusal names none.
func TestPartNameNeverReachesAClient(t *testing.T) {
	var logged bytes.Buffer
	s := newTestServerWith(t, func(cfg *config.Config) { cfg.Query.MaxResultBytes = 64 << 10 })
	s.logger = slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn}))
	n := &spreadNode{t: t, s: s, handler: NewHandler(s)}
	for _, name := range []string{"User", "Other"} {
		if reply := n.send(transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeHash,
			Options: map[string]interface{}{"placement": "spread", "stores": []interface{}{map[string]interface{}{"name": "Purchases", "engine": "fifoStore"}}}}); reply.Opcode == transport.OpError {
			t.Fatal(refusalText(t, reply))
		}
		for i := 0; i < 400; i++ {
			key := fmt.Sprintf("%s-%d", name, i)
			if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: name, Key: key,
				Data: map[string]interface{}{"N": int32(i), "Pad": strings.Repeat("x", 300)}}); reply.Opcode == transport.OpError {
				t.Fatal(refusalText(t, reply))
			}
			if i%50 == 0 {
				if reply := n.send(transport.OpFieldPush, FieldRequest{Database: "app", Bucket: name, Key: key, Field: "Purchases",
					Data: map[string]interface{}{"sku": int32(i)}}); reply.Opcode == transport.OpError {
					t.Fatal(refusalText(t, reply))
				}
			}
		}
	}
	// Part 0 of User, the first a dump relays, holds more than one chunk, so
	// its owner's dump slot stays claimed while the first chunk is out.
	for _, key := range keysInPart(t, "big", 0, 12) {
		if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: key,
			Data: map[string]interface{}{"Pad": strings.Repeat("y", 512<<10)}}); reply.Opcode == transport.OpError {
			t.Fatal(refusalText(t, reply))
		}
	}
	check := func(label string, reply *transport.Frame) {
		t.Helper()
		if bsonNamesAPart(reply.Payload) {
			t.Errorf("%s shows a part's name: %s %q", label, reply.Opcode, reply.Payload)
		}
	}
	for label, read := range fanOutRequests() {
		if read.op != transport.OpDumpStart {
			check(label, n.asRoot(read.op, read.request))
		}
	}
	// A part refuses its share of a large find; the refusal is the
	// collection's.
	tooLarge := n.send(transport.OpScan, ScanRequest{Database: "app", Bucket: "User", Limit: 5000})
	if tooLarge.Opcode != transport.OpError || !strings.Contains(refusalTextOrEmpty(t, tooLarge), "app.User") {
		t.Errorf("a find past the result ceiling answered %s %q, want a refusal naming app.User", tooLarge.Opcode, tooLarge.Payload)
	}
	check("a part's refusal", tooLarge)

	// The dump: its start, chunks and done, and a second collection's dump
	// meeting the first's part on the one slot.
	check("dump start", n.asRoot(transport.OpDumpStart, DumpStartRequest{Database: "app", Bucket: "User", AcceptStores: true, AcceptDead: true}))
	check("dump chunk", n.asRoot(transport.OpDumpChunk, DumpChunkRequest{Database: "app", Bucket: "User"}))
	check("a second dump's start", n.asRoot(transport.OpDumpStart, DumpStartRequest{Database: "app", Bucket: "Other", AcceptStores: true, AcceptDead: true}))
	busy := n.asRoot(transport.OpDumpChunk, DumpChunkRequest{Database: "app", Bucket: "Other"})
	if busy.Opcode != transport.OpError || !strings.Contains(refusalTextOrEmpty(t, busy), "app/User") {
		t.Errorf("a second dump meeting the first's part answered %s %q, want a refusal naming app/User", busy.Opcode, busy.Payload)
	}
	check("a second dump refused", busy)
	check("the second dump's done", n.asRoot(transport.OpDumpDone, DumpDoneRequest{Database: "app", Bucket: "Other"}))
	for {
		chunk := n.asRoot(transport.OpDumpChunk, DumpChunkRequest{Database: "app", Bucket: "User"})
		check("dump chunk", chunk)
		var answer DumpChunkResponse
		decodeAnswer(t, chunk, &answer)
		if answer.EOF {
			break
		}
	}
	check("dump done", n.asRoot(transport.OpDumpDone, DumpDoneRequest{Database: "app", Bucket: "User"}))

	// A lost part's refusal.
	drop := transport.NewFrame(transport.OpDeleteBucket, spreadMarshal(t, DeleteBucketRequest{Database: "app", Name: partName("User", spreadPartOf(t, "User-0"))}))
	if reply, err := s.fencedDispatch(context.Background(), drop); err != nil || reply.Opcode == transport.OpError {
		t.Fatalf("dropping a part past the router: %v %q", err, reply.Payload)
	}
	for label, read := range fanOutRequests() {
		check(label+" with a lost part", n.asRoot(read.op, read.request))
	}
	for _, line := range strings.Split(logged.String(), "\n") {
		if namesAPart(line) {
			t.Errorf("a log line at WARN or above shows a part's name: %s", line)
		}
	}
}

// TestCreateOfAHeldSpreadNameAnswersAlreadyExists is the step 2 verifier's
// carried item 5 (its mutation V3 survived): a create of the name of a spread
// collection that exists answers success with "already exists", as a whole
// bucket's does, through the one-member door and through every node of a
// cluster, and makes nothing.
func TestCreateOfAHeldSpreadNameAnswersAlreadyExists(t *testing.T) {
	again := CreateBucketRequest{Database: "app", Name: "User", Type: BucketTypeHash, Options: map[string]interface{}{"placement": "spread"}}
	requireExists := func(label string, reply *transport.Frame) {
		t.Helper()
		var created CreateBucketResponse
		if reply.Opcode == transport.OpError || bson.Unmarshal(reply.Payload, &created) != nil || !created.Success ||
			!strings.Contains(created.Message, "already exists") {
			t.Errorf("%s: a create of the held spread name answered %s %q, want success saying already exists", label, reply.Opcode, reply.Payload)
		}
	}
	n := newSpreadNode(t, nil)
	requireExists("a node with no cluster", n.send(transport.OpCreateBucket, again))
	if got := templateCount(t, n.s, "app", "User"); got != 0 {
		t.Errorf("the template holds %d documents after the repeated create", got)
	}
	sc := newSpreadCluster(t, 3, 2, "User", nil)
	for index := range sc.nodes {
		requireExists(fmt.Sprintf("node %d", index+1), sc.frame(index, transport.OpCreateBucket, again))
	}
}

// TestFanOutTakesTheRouterGateOnce is the gate's ordering for a fan-out
// (spread_gate.go, spread_fanout.go): a fan-out takes the router gate shared,
// once, holding no fence. So it waits while a drop holds the gate
// exclusively, and answers once the drop lets go; and under -race, fan-outs,
// routed writes and exclusive holds that queue behind them all finish:
// a fan-out that took the gate twice would deadlock behind a queued
// exclusive hold (the reviewer's rw/main.go), and one that took none would
// answer while the drop held it.
func TestFanOutTakesTheRouterGateOnce(t *testing.T) {
	n := newSpreadNode(t, nil)
	for i := 0; i < 64; i++ {
		if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: fmt.Sprintf("g-%d", i), Data: map[string]interface{}{"N": int32(i)}}); reply.Opcode == transport.OpError {
			t.Fatal(refusalText(t, reply))
		}
	}
	key := placementOf("app", "User")
	release := n.s.holdRouterGate(key, true)
	answered := make(chan int64, 1)
	go func() {
		answered <- fanOutCount(t, n.send(transport.OpCount, CountRequest{Database: "app", Bucket: "User"}))
	}()
	select {
	case count := <-answered:
		release()
		t.Fatalf("count() answered %d while a drop held the router gate", count)
	case <-time.After(300 * time.Millisecond):
	}
	release()
	select {
	case count := <-answered:
		if count != 64 {
			t.Fatalf("count() is %d, want 64", count)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("count() did not answer once the drop let the gate go")
	}

	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				switch (worker + i) % 5 {
				case 0:
					hold := n.s.holdRouterGate(key, true)
					hold()
				case 1:
					n.send(transport.OpUpsert, InsertRequest{Database: "app", Bucket: "User", Key: fmt.Sprintf("w-%d-%d", worker, i), Data: map[string]interface{}{"N": int32(i)}})
				case 2:
					n.send(transport.OpScan, ScanRequest{Database: "app", Bucket: "User", Sort: map[string]int{"N": -1}, Limit: 10})
				case 3:
					n.send(transport.OpBucketStats, BucketStatsRequest{Database: "app", Bucket: "User"})
				default:
					n.send(transport.OpCount, CountRequest{Database: "app", Bucket: "User"})
				}
			}
		}(worker)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("fan-outs, routed writes and exclusive gate holds did not finish: the gate deadlocked")
	}
	if left := n.s.routerGates.inFlight(); left != 0 {
		t.Fatalf("%d gate entries left", left)
	}
	if held := n.s.queryInFlight(); held != 0 {
		t.Fatalf("%d execute slots held after every call answered", held)
	}
}

// TestIdleSpreadDumpIsTakenOver: a dump of a spread collection whose client
// sent nothing for the dump pin ceiling is ended by the collection's next
// dump start, which is not refused as one already running; within the
// ceiling it is.
func TestIdleSpreadDumpIsTakenOver(t *testing.T) {
	n := newSpreadNode(t, nil)
	for i := 0; i < 20; i++ {
		if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: fmt.Sprintf("d-%d", i), Data: map[string]interface{}{"N": int32(i)}}); reply.Opcode == transport.OpError {
			t.Fatal(refusalText(t, reply))
		}
	}
	start := DumpStartRequest{Database: "app", Bucket: "User", AcceptStores: true, AcceptDead: true}
	if reply := n.asRoot(transport.OpDumpStart, start); reply.Opcode == transport.OpError {
		t.Fatal(refusalText(t, reply))
	}
	if reply := n.asRoot(transport.OpDumpChunk, DumpChunkRequest{Database: "app", Bucket: "User"}); reply.Opcode == transport.OpError {
		t.Fatal(refusalText(t, reply))
	}
	busy := n.asRoot(transport.OpDumpStart, start)
	if busy.Opcode != transport.OpError || !strings.Contains(refusalTextOrEmpty(t, busy), "already running") {
		t.Fatalf("a second dump start within the ceiling answered %s %q, want already running", busy.Opcode, busy.Payload)
	}
	previous := dumpPinCeilingOverrideForTest
	dumpPinCeilingOverrideForTest = 50 * time.Millisecond
	t.Cleanup(func() { dumpPinCeilingOverrideForTest = previous })
	time.Sleep(100 * time.Millisecond)
	if documents, _, done := dumpCollection(t, n.asRoot, "User"); len(documents) != 20 || done.Documents != 20 {
		t.Fatalf("the dump after the idle one streamed %d documents and counted %d, want 20", len(documents), done.Documents)
	}
	if left := n.s.spreadDumps.running(); left != 0 {
		t.Errorf("%d router dump sessions left", left)
	}
	if n.s.dump != nil {
		t.Error("a part's dump slot is still claimed")
	}
}

// TestSpreadFanOutKeepsTheAuthRows: the fan-out class is authorized at the
// client's door, before any routing, exactly as on a whole bucket (section
// 2.4): a reader of the database reads every verb of it and is refused a
// dump, which needs the backup role and the root user; a user with no grant
// on the database is refused every one, and the router never runs for them.
func TestSpreadFanOutKeepsTheAuthRows(t *testing.T) {
	n := newSpreadNode(t, nil)
	if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: "a", Data: map[string]interface{}{"N": int32(1)}}); reply.Opcode == transport.OpError {
		t.Fatal(refusalText(t, reply))
	}
	as := func(grants ...transport.ScopedGrant) func(transport.Opcode, any) *transport.Frame {
		session := &transport.Session{}
		session.SetIdentity(transport.Identity{Username: "alice", Grants: grants})
		ctx := transport.WithSession(context.Background(), session)
		return func(op transport.Opcode, request any) *transport.Frame {
			reply, err := n.handler(ctx, transport.NewFrame(op, spreadMarshal(t, request)))
			if err != nil {
				t.Fatalf("%s: %v", op, err)
			}
			if hook := reply.TakeWrittenHook(); hook != nil {
				hook()
			}
			return reply
		}
	}
	reader, stranger := as(transport.ScopedGrant{Role: roleReader, DB: "app"}), as(transport.ScopedGrant{Role: roleReader, DB: "elsewhere"})
	routed := n.s.routerGates.inFlight
	for label, read := range fanOutRequests() {
		if refused := refusalTextOrEmpty(t, stranger(read.op, read.request)); !strings.Contains(refused, "permission denied") {
			t.Errorf("%s by a user with no grant on app answered %q, want permission denied", label, refused)
		}
		reply := reader(read.op, read.request)
		if read.op == transport.OpDumpStart {
			if refused := refusalTextOrEmpty(t, reply); !strings.Contains(refused, "permission denied") {
				t.Errorf("a dump by a reader answered %q, want permission denied", refused)
			}
			continue
		}
		if reply.Opcode == transport.OpError {
			t.Errorf("%s by a reader of app was refused: %s", label, refusalText(t, reply))
		}
	}
	if left := routed(); left != 0 {
		t.Errorf("%d gate entries left", left)
	}
}
