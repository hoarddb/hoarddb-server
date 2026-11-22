package server

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/transport"
)

// now() is fixed once, on the node a client's write
// enters, before the entry is logged, for every write shape; an applier (the
// owner's apply, a replica's, a replay) applies the logged instant and never
// reads a clock of its own.
//
// Every test here reads now() through nowClockHook set to a clock that moves
// one second on every reading. Two readings of one request are then two
// instants, whatever the timing, which is what a node with its own clock does
// to a write stamped on each copy.

// steppingNowClock makes every reading of now()'s clock one second later than
// the one before, and returns the count of readings.
func steppingNowClock(t *testing.T) *atomic.Int64 {
	t.Helper()
	base := time.Now().UTC().Truncate(time.Second)
	var readings atomic.Int64
	clock := func() time.Time { return base.Add(time.Duration(readings.Add(1)) * time.Second) }
	nowClockHook.Store(&clock)
	t.Cleanup(func() { nowClockHook.Store(nil) })
	return &readings
}

// nowArg is what a client sends for now().
func nowArg() primitive.DBPointer { return NowMarker() }

// stampedFields is a document with three now() calls: one at the top, one in
// an array and one in a document inside that array.
func stampedFields() map[string]interface{} {
	return map[string]interface{}{
		"at":  nowArg(),
		"arr": []interface{}{nowArg(), map[string]interface{}{"in": nowArg()}},
	}
}

// withID is fields plus an _id_.
func withID(id string, fields map[string]interface{}) map[string]interface{} {
	fields["_id_"] = id
	return fields
}

// datetimesIn lists every datetime in document, at any depth.
func datetimesIn(t *testing.T, document bson.Raw) []primitive.DateTime {
	t.Helper()
	var found []primitive.DateTime
	var walk func(value bson.RawValue)
	walk = func(value bson.RawValue) {
		switch value.Type {
		case bson.TypeDateTime:
			found = append(found, primitive.DateTime(value.DateTime()))
		case bson.TypeEmbeddedDocument, bson.TypeArray:
			elements, err := bson.Raw(value.Value).Elements()
			if err != nil {
				t.Fatalf("reading a stored document: %v", err)
			}
			for _, element := range elements {
				walk(element.Value())
			}
		}
	}
	walk(bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: document})
	return found
}

// oneInstant fails unless instants holds want datetimes, all equal, and
// returns that instant.
func oneInstant(t *testing.T, what string, instants []primitive.DateTime, want int) primitive.DateTime {
	t.Helper()
	if len(instants) != want {
		t.Fatalf("%s holds %d datetimes %v, want %d: a now() was left unsubstituted", what, len(instants), instants, want)
	}
	for _, instant := range instants[1:] {
		if instant != instants[0] {
			t.Fatalf("%s holds the datetimes %v: one request's now() calls must be one clock reading", what, instants)
		}
	}
	return instants[0]
}

// marshalRequest encodes request as a client frame's payload.
func marshalRequest(t *testing.T, request any) []byte {
	t.Helper()
	payload, err := bson.Marshal(request)
	if err != nil {
		t.Fatalf("encoding %T: %v", request, err)
	}
	return payload
}

// payloadAsLogged is what Cluster.handleClientFrame logs for payload: the
// resolved form, or the payload itself when nothing needed resolving.
func payloadAsLogged(t *testing.T, op transport.Opcode, payload []byte) []byte {
	t.Helper()
	resolved, err := resolveDocumentIDs(op, payload)
	if err != nil {
		t.Fatalf("resolveDocumentIDs(%s): %v", op, err)
	}
	if resolved == nil {
		return payload
	}
	return resolved
}

// namedWriteForm is one write that names its own key or _id_, with the
// number of now() calls it carries.
type namedWriteForm struct {
	name    string
	op      transport.Opcode
	request any
	nows    int
}

// namedWriteForms is every write shape that names its own key or _id_, in
// database: buckets events (hash), sorted (btree), queue (fifo), stack (lifo),
// ranked (heap), and Post declaring Comments (fifo) and Tags (hash) with a
// document p1.
func namedWriteForms(database string) []namedWriteForm {
	return []namedWriteForm{
		{"put with a key", transport.OpPut, InsertRequest{Database: database, Bucket: "events", Key: "k1", Data: stampedFields()}, 3},
		{"put with _id_", transport.OpPut, InsertRequest{Database: database, Bucket: "events", Data: withID("k2", stampedFields())}, 3},
		{"btree insert with a key", transport.OpBTreeInsert, InsertRequest{Database: database, Bucket: "sorted", Key: "k", Data: stampedFields()}, 3},
		{"batch whose every document names _id_", transport.OpBatchPut, BatchInsertRequest{Database: database, Bucket: "events", Data: []map[string]interface{}{
			withID("a", stampedFields()), withID("b", map[string]interface{}{"at": nowArg()}),
		}}, 4},
		{"fifo push with _id_", transport.OpFIFOPush, PushRequest{Database: database, Bucket: "queue", Data: withID("e", stampedFields())}, 3},
		{"lifo push with _id_", transport.OpLIFOPush, PushRequest{Database: database, Bucket: "stack", Data: withID("e", stampedFields())}, 3},
		{"heap push with _id_", transport.OpHeapPush, PushRequest{Database: database, Bucket: "ranked", Priority: 1, Data: withID("e", stampedFields())}, 3},
		{"field push with _id_", transport.OpFieldPush, FieldRequest{Database: database, Bucket: "Post", Key: "p1", Field: "Comments", Data: withID("c", stampedFields())}, 3},
		{"field put with _id_", transport.OpFieldPut, FieldRequest{Database: database, Bucket: "Post", Key: "p1", Field: "Tags", Element: "t", Data: withID("t", stampedFields())}, 3},
	}
}

// formBuckets are the plain buckets namedWriteForms writes to.
var formBuckets = []struct {
	name  string
	btype BucketType
}{{"events", BucketTypeHash}, {"sorted", BucketTypeBTree}, {"queue", BucketTypeFIFO}, {"stack", BucketTypeLIFO}, {"ranked", BucketTypeHeap}}

// TestResolveDocumentIDsSubstitutesNowWhenTheKeyIsNamed: a write
// that names its key returned nil from resolveDocumentIDs, so the sentinel was
// logged and every applier stamped its own clock.
func TestResolveDocumentIDsSubstitutesNowWhenTheKeyIsNamed(t *testing.T) {
	steppingNowClock(t)
	for _, form := range namedWriteForms("app") {
		t.Run(form.name, func(t *testing.T) {
			resolved, err := resolveDocumentIDs(form.op, marshalRequest(t, form.request))
			if err != nil {
				t.Fatalf("resolveDocumentIDs: %v", err)
			}
			if resolved == nil {
				t.Fatalf("resolveDocumentIDs returned nil, so the payload is logged with now() unsubstituted")
			}
			if bytes.Contains(resolved, nowSentinelKey) {
				t.Fatalf("the logged payload still holds the now() sentinel")
			}
			oneInstant(t, "the logged payload", datetimesIn(t, resolved), form.nows)
		})
	}
}

// updateWithEveryOperator is an update whose every value-bearing operator
// carries now(): six calls.
func updateWithEveryOperator(database, bucket, key string) UpdateRequest {
	return UpdateRequest{
		Database: database, Bucket: bucket, Key: key,
		Set:      map[string]interface{}{"When": nowArg(), "List": []interface{}{nowArg()}},
		Push:     map[string]interface{}{"Log": nowArg()},
		AddToSet: map[string]interface{}{"Seen": nowArg()},
		Min:      map[string]interface{}{"First": nowArg()},
		Max:      map[string]interface{}{"Last": nowArg()},
	}
}

// TestResolveDocumentIDsSubstitutesNowInAnUpdate: an update's
// operators were logged as sent. It asserts the payload, so a fix inside
// Server.UpdateCtx alone, which runs on every applier, still fails it.
func TestResolveDocumentIDsSubstitutesNowInAnUpdate(t *testing.T) {
	steppingNowClock(t)
	for _, form := range []struct {
		name    string
		request UpdateRequest
		nows    int
	}{
		{"every operator", updateWithEveryOperator("app", "events", "u"), 6},
		{"a multi update's filter and $set", UpdateRequest{Database: "app", Bucket: "events", Multi: true,
			Filter: map[string]interface{}{"Stamp": map[string]interface{}{"$lt": nowArg()}},
			Set:    map[string]interface{}{"When": nowArg()}}, 2},
	} {
		t.Run(form.name, func(t *testing.T) {
			resolved, err := resolveDocumentIDs(transport.OpUpdate, marshalRequest(t, form.request))
			if err != nil {
				t.Fatalf("resolveDocumentIDs: %v", err)
			}
			if resolved == nil {
				t.Fatalf("resolveDocumentIDs returned nil for an update with now(): the sentinel is logged as sent")
			}
			if bytes.Contains(resolved, nowSentinelKey) {
				t.Fatalf("the logged update still holds the now() sentinel")
			}
			oneInstant(t, "the logged update", datetimesIn(t, resolved), form.nows)
		})
	}
}

// TestResolveDocumentIDsLeavesAWriteWithoutNowUntouched: a named write or an
// update with no now() is not rewritten, and the
// clock is not read for it.
func TestResolveDocumentIDsLeavesAWriteWithoutNowUntouched(t *testing.T) {
	readings := steppingNowClock(t)
	for _, form := range []struct {
		op      transport.Opcode
		request any
	}{
		{transport.OpPut, InsertRequest{Database: "app", Bucket: "events", Key: "k", Data: map[string]interface{}{"n": 1}}},
		{transport.OpPut, InsertRequest{Database: "app", Bucket: "events", Data: map[string]interface{}{"_id_": "k", "n": 1}}},
		{transport.OpFIFOPush, PushRequest{Database: "app", Bucket: "queue", Data: map[string]interface{}{"_id_": "e"}}},
		{transport.OpUpdate, UpdateRequest{Database: "app", Bucket: "events", Key: "u", Set: map[string]interface{}{"n": 2}}},
	} {
		resolved, err := resolveDocumentIDs(form.op, marshalRequest(t, form.request))
		if err != nil || resolved != nil {
			t.Errorf("resolveDocumentIDs(%s) = %d bytes, %v; want nil, nil: nothing to rewrite", form.op, len(resolved), err)
		}
	}
	if got := readings.Load(); got != 0 {
		t.Errorf("the clock was read %d times for writes without now(); want 0", got)
	}
}

// TestResolveDocumentIDsTakesADocumentThatIsTheOldSentinelAsData: a put whose
// whole document is {"$$now": true} is a document with one field, never a
// date, and never a panic.
func TestResolveDocumentIDsTakesADocumentThatIsTheOldSentinelAsData(t *testing.T) {
	oldSentinel := map[string]interface{}{"$$now": true}
	for _, form := range []struct {
		op      transport.Opcode
		request any
	}{
		{transport.OpPut, InsertRequest{Database: "app", Bucket: "events", Key: "k", Data: oldSentinel}},
		{transport.OpBatchPut, BatchInsertRequest{Database: "app", Bucket: "events", Data: []map[string]interface{}{oldSentinel}}},
		{transport.OpFIFOPush, PushRequest{Database: "app", Bucket: "queue", Data: oldSentinel}},
		{transport.OpFieldPush, FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments", Data: oldSentinel}},
		{transport.OpUpdate, UpdateRequest{Database: "app", Bucket: "events", Key: "u", Set: oldSentinel}},
	} {
		func() {
			defer func() {
				if failure := recover(); failure != nil {
					t.Errorf("resolveDocumentIDs(%s) failed on a document with one field named $$now: %v", form.op, failure)
				}
			}()
			if _, err := resolveDocumentIDs(form.op, marshalRequest(t, form.request)); err != nil {
				t.Errorf("resolveDocumentIDs(%s): %v", form.op, err)
			}
		}()
	}
}

// TestTwoAppliersOfOneNamedKeyPutStoreTheSameDate is the two-applier
// form: one logged payload, applied by two servers at two clock readings, is
// one stored date.
func TestTwoAppliersOfOneNamedKeyPutStoreTheSameDate(t *testing.T) {
	steppingNowClock(t)
	payload := payloadAsLogged(t, transport.OpPut, marshalRequest(t, InsertRequest{
		Database: "app", Bucket: "events", Key: "k", Data: stampedFields(),
	}))
	var stored []primitive.DateTime
	for range 2 {
		s := newTestServer(t)
		createTestBucket(t, s, "app", "events", BucketTypeHash)
		answer, err := s.dispatch(context.Background(), &transport.Frame{Opcode: transport.OpPut, Payload: payload})
		if err != nil || answer.Opcode == transport.OpError {
			t.Fatalf("applying the logged put: %v %v", answer, err)
		}
		found, err := s.Find(FindRequest{Database: "app", Bucket: "events", Key: "k"})
		if err != nil || !found.Found {
			t.Fatalf("reading k back: %+v, %v", found, err)
		}
		stored = append(stored, oneInstant(t, "the stored document", datetimesIn(t, found.Data), 3))
	}
	if stored[0] != stored[1] {
		t.Fatalf("two appliers of one logged put stored %v and %v: each stamped its own clock", stored[0], stored[1])
	}
}

// TestAppliersReadNoClock: dispatch is the applier — the owner's apply, a
// replica's and replay all funnel through it — and none of them may read
// now()'s clock. A logged payload carries its instant already.
func TestAppliersReadNoClock(t *testing.T) {
	readings := steppingNowClock(t)
	s := newPostServer(t)
	for _, bucket := range formBuckets {
		createTestBucket(t, s, "app", bucket.name, bucket.btype)
	}
	mustInsert(t, s, "app", "events", "u", map[string]interface{}{"n": 1})
	forms := append(namedWriteForms("app"), namedWriteForm{"update", transport.OpUpdate, updateWithEveryOperator("app", "events", "u"), 6})
	for _, form := range forms {
		payload := payloadAsLogged(t, form.op, marshalRequest(t, form.request))
		readings.Store(0)
		answer, err := s.dispatch(context.Background(), &transport.Frame{Opcode: form.op, Payload: payload})
		if err != nil || answer.Opcode == transport.OpError {
			t.Fatalf("%s: applying the logged payload: %v %v", form.name, answer, err)
		}
		if got := readings.Load(); got != 0 {
			t.Errorf("%s: the applier read now()'s clock %d times; a replica or a replay must apply the logged instant", form.name, got)
		}
	}
}

// doorWrite sends one write through a standalone node's client door,
// Server.fencedDispatch, and fails the test on an error answer.
func doorWrite(t *testing.T, s *Server, op transport.Opcode, request any) *transport.Frame {
	t.Helper()
	answer, err := s.fencedDispatch(context.Background(), &transport.Frame{Opcode: op, Payload: marshalRequest(t, request)})
	if err != nil || answer.Opcode == transport.OpError {
		t.Fatalf("%s through the door: %v %v", op, answer, err)
	}
	return answer
}

// TestUpdateSetNowStoresADate runs on a standalone node: $set and the
// other value-bearing operators store a datetime, and a multi update stamps
// every matched document with one instant.
func TestUpdateSetNowStoresADate(t *testing.T) {
	steppingNowClock(t)
	s := newTestServer(t)
	// Whole: the multi-update below goes through the door, which a spread bucket refuses (build step 12c).
	createTestBucketWithOptions(t, s, "app", "events", BucketTypeHash, map[string]interface{}{"placement": "whole"})

	mustInsert(t, s, "app", "events", "u", map[string]interface{}{"n": 1})
	doorWrite(t, s, transport.OpUpdate, updateWithEveryOperator("app", "events", "u"))
	found, err := s.Find(FindRequest{Database: "app", Bucket: "events", Key: "u"})
	if err != nil || !found.Found {
		t.Fatalf("reading u back: %+v, %v", found, err)
	}
	oneInstant(t, "the updated document", datetimesIn(t, found.Data), 6)

	past := primitive.NewDateTimeFromTime(time.Now().Add(-time.Hour))
	for _, key := range []string{"m1", "m2"} {
		mustInsert(t, s, "app", "events", key, map[string]interface{}{"Stamp": past})
	}
	doorWrite(t, s, transport.OpUpdate, UpdateRequest{Database: "app", Bucket: "events", Multi: true,
		Filter: map[string]interface{}{"Stamp": map[string]interface{}{"$lt": nowArg()}},
		Set:    map[string]interface{}{"When": nowArg()}})
	var stamped []primitive.DateTime
	for _, key := range []string{"m1", "m2"} {
		document := documentOf(t, s, "app", "events", key)
		when, isDate := document["When"].(primitive.DateTime)
		if !isDate {
			t.Fatalf("%s.When = %T %v after a multi $set of now(); want a datetime", key, document["When"], document["When"])
		}
		stamped = append(stamped, when)
	}
	if stamped[0] != stamped[1] {
		t.Errorf("a multi update stamped %v and %v; one request is one instant", stamped[0], stamped[1])
	}
}

// TestStandaloneDoorSubstitutesNowForEveryShape: with no cluster there is no
// log, and the door is where now() is fixed, as resolveDocumentIDs fixes it on
// a cluster.
func TestStandaloneDoorSubstitutesNowForEveryShape(t *testing.T) {
	steppingNowClock(t)
	s := newPostServer(t)
	for _, bucket := range formBuckets {
		createTestBucket(t, s, "app", bucket.name, bucket.btype)
	}
	for _, form := range namedWriteForms("app") {
		doorWrite(t, s, form.op, form.request)
	}
	read := storedReaders(t, s, "app")
	for _, form := range namedWriteForms("app") {
		instants := read[form.name]()
		oneInstant(t, form.name, instants, form.nows)
	}
}

// storedReaders reads back, from one node's own store, what each of
// namedWriteForms wrote, as the datetimes it holds; nothing when the node does
// not hold it (yet).
func storedReaders(t *testing.T, s *Server, database string) map[string]func() []primitive.DateTime {
	find := func(bucket string, keys ...string) func() []primitive.DateTime {
		return func() []primitive.DateTime {
			var instants []primitive.DateTime
			for _, key := range keys {
				found, err := s.Find(FindRequest{Database: database, Bucket: bucket, Key: key})
				if err != nil || !found.Found {
					return nil
				}
				instants = append(instants, datetimesIn(t, found.Data)...)
			}
			return instants
		}
	}
	peek := func(bucket string) func() []primitive.DateTime {
		return func() []primitive.DateTime {
			peeked, err := s.Peek(PeekRequest{Database: database, Bucket: bucket})
			if err != nil || !peeked.Found {
				return nil
			}
			return datetimesIn(t, peeked.Items[0])
		}
	}
	field := func(read func(FieldRequest) (*FieldResponse, error), request FieldRequest) func() []primitive.DateTime {
		return func() []primitive.DateTime {
			element, err := read(request)
			if err != nil || !element.Found {
				return nil
			}
			return datetimesIn(t, element.Element)
		}
	}
	fieldPeeked := func(request FieldRequest) func() []primitive.DateTime {
		return func() []primitive.DateTime {
			peeked, err := s.FieldPeek(FieldPeekRequest{FieldRequest: request})
			if err != nil || !peeked.Found {
				return nil
			}
			return datetimesIn(t, peeked.Items[0].Element)
		}
	}
	return map[string]func() []primitive.DateTime{
		"put with a key":                        find("events", "k1"),
		"put with _id_":                         find("events", "k2"),
		"btree insert with a key":               find("sorted", "k"),
		"batch whose every document names _id_": find("events", "a", "b"),
		"fifo push with _id_":                   peek("queue"),
		"lifo push with _id_":                   peek("stack"),
		"heap push with _id_":                   peek("ranked"),
		"field push with _id_":                  fieldPeeked(FieldRequest{Database: database, Bucket: "Post", Key: "p1", Field: "Comments"}),
		"field put with _id_":                   field(s.FieldGet, FieldRequest{Database: database, Bucket: "Post", Key: "p1", Field: "Tags", Element: "t"}),
	}
}

// concerned is request with write concern all added, for a request type that
// carries no write concern field of its own (UpdateRequest): the coordinator
// reads it off the payload (frameMeta.writeConcern).
func concerned(t *testing.T, request any) bson.D {
	t.Helper()
	var fields bson.D
	if err := bson.Unmarshal(marshalRequest(t, request), &fields); err != nil {
		t.Fatalf("decoding %T: %v", request, err)
	}
	return append(fields, bson.E{Key: "writeconcern", Value: "all"})
}

// waitForInstants reads until a node's copy holds every now() of a write:
// write concern all answers once the copies in sync hold it, and a slower
// copy is not the defect under test.
func waitForInstants(read func() []primitive.DateTime, want int) []primitive.DateTime {
	deadline := time.Now().Add(10 * time.Second)
	for {
		instants := read()
		if len(instants) >= want || time.Now().After(deadline) {
			return instants
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestNowIsOneInstantOnEveryCopy runs on a three-node
// cluster at replication factor 3: every write shape that names its key, and
// an update, stores one instant on every copy, and no node's log carries the
// sentinel.
func TestNowIsOneInstantOnEveryCopy(t *testing.T) {
	steppingNowClock(t)
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, bucket := range formBuckets {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket.name, Type: bucket.btype})
	}
	createFieldBucket(t, tc, 0, "Post")
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: "events", Key: "u", Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})

	forms := namedWriteForms(testDatabase)
	for _, form := range forms {
		tc.ok(0, form.op, concerned(t, form.request))
	}
	tc.ok(0, transport.OpUpdate, concerned(t, updateWithEveryOperator(testDatabase, "events", "u")))

	type copyRead struct {
		name string
		nows int
		read func(s *Server) []primitive.DateTime
	}
	reads := []copyRead{{"update", 6, func(s *Server) []primitive.DateTime {
		found, err := s.Find(FindRequest{Database: testDatabase, Bucket: "events", Key: "u"})
		if err != nil || !found.Found {
			return nil
		}
		return datetimesIn(t, found.Data)
	}}}
	for _, form := range forms {
		reads = append(reads, copyRead{form.name, form.nows, func(s *Server) []primitive.DateTime {
			return storedReaders(t, s, testDatabase)[form.name]()
		}})
	}
	for _, shape := range reads {
		t.Run(shape.name, func(t *testing.T) {
			var first primitive.DateTime
			for index, node := range tc.nodes {
				read := func() []primitive.DateTime { return shape.read(node.server.core) }
				instant := oneInstant(t, fmt.Sprintf("node %d's copy", index+1), waitForInstants(read, shape.nows), shape.nows)
				if index == 0 {
					first = instant
					continue
				}
				if instant != first {
					t.Errorf("node %d stored %v, node 1 stored %v: each copy stamped its own clock", index+1, instant.Time(), first.Time())
				}
			}
		})
	}

	for index, node := range tc.nodes {
		entries, err := node.server.core.wal.ReadFrom(1)
		if err != nil {
			t.Fatalf("reading node %d's log: %v", index+1, err)
		}
		for _, entry := range entries {
			if bytes.Contains(entry.Data, nowSentinelKey) {
				t.Errorf("node %d logged a %s to %q with the now() sentinel in it: a replay stamps it again", index+1, transport.Opcode(entry.Op), entry.Bucket)
			}
		}
	}
}

// TestKeylessBTreeInsertIsKeyedBeforeTheLog: a btree insert is applied by
// Server.InsertCtx as a put is, and a keyless one was minted a key by each
// applier, because resolveDocumentIDs had no case for its opcode. Its key is
// now minted once, before the log, as a put's is.
func TestKeylessBTreeInsertIsKeyedBeforeTheLog(t *testing.T) {
	resolved, err := resolveDocumentIDs(transport.OpBTreeInsert, marshalRequest(t, InsertRequest{
		Database: "app", Bucket: "sorted", Data: map[string]interface{}{"n": 1},
	}))
	if err != nil {
		t.Fatalf("resolveDocumentIDs: %v", err)
	}
	var logged InsertRequest
	if resolved == nil || bson.Unmarshal(resolved, &logged) != nil || logged.Key == "" || logged.Data["_id_"] != logged.Key {
		t.Fatalf("a keyless btree insert is logged as %+v: it must name its key, or every applier mints its own", logged)
	}
}

// TestReplayAppliesTheLoggedInstant is the replay arm, built as
// TestLIFOPushIDStableAfterDelayedReplay is: the owner's apply of a keyed put
// fails, the replica applies it live, and the owner's restart replays it from
// its log. The replayed copy holds the replica's instant.
func TestReplayAppliesTheLoggedInstant(t *testing.T) {
	steppingNowClock(t)
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	replicaIndex := 1 - ownerIndex

	failed := failNextApplyOn(t, tc.nodes[ownerIndex].server.core.kv, anySeq)
	// The owner's apply is made to fail, so an error answer is expected.
	tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k", Data: stampedFields(), WriteConcern: "majority"})
	if failed.Load() == 0 {
		t.Fatal("the owner's apply never reached the store; the test built no replay")
	}

	readOn := func(index int) func() []primitive.DateTime {
		return func() []primitive.DateTime {
			found, err := tc.nodes[index].server.core.Find(FindRequest{Database: testDatabase, Bucket: testBucket, Key: "k"})
			if err != nil || !found.Found {
				return nil
			}
			return datetimesIn(t, found.Data)
		}
	}
	replicaInstant := oneInstant(t, "the replica's copy", waitForInstants(readOn(replicaIndex), 3), 3)
	tc.restartNode(ownerIndex)
	ownerInstant := oneInstant(t, "the owner's replayed copy", waitForInstants(readOn(ownerIndex), 3), 3)
	if ownerInstant != replicaInstant {
		t.Fatalf("the owner's replay stored %v, the replica holds %v: replay stamped now() again", ownerInstant.Time(), replicaInstant.Time())
	}
}
