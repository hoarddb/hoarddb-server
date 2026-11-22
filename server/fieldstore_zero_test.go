package server

import (
	"context"
	"strconv"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// zeroFieldAnswer sends one field verb through dispatch and returns the raw
// BSON of the answer: the wire is what BUG-0558 is about, and a decode into
// FieldResponse would hide a member the encoder dropped.
func zeroFieldAnswer(t *testing.T, s *Server, op transport.Opcode, request bson.M) bson.Raw {
	t.Helper()
	payload, err := bson.Marshal(request)
	if err != nil {
		t.Fatalf("encoding the %s request: %v", op, err)
	}
	answer, err := s.dispatch(context.Background(), transport.NewFrame(op, payload))
	if err != nil || answer.Opcode != op {
		t.Fatalf("%s answered %v, %v", op, answer, err)
	}
	return bson.Raw(answer.Payload)
}

// requireMember fails the test unless member is in raw (or, when want is
// false, is not) and, if present, is the integer wantValue.
func requireMember(t *testing.T, where string, raw bson.Raw, member string, wantPresent bool, wantValue int64) {
	t.Helper()
	got, err := raw.LookupErr(member)
	present := err == nil
	if present != wantPresent {
		t.Errorf("%s: member %q present = %v, want %v (raw %s)", where, member, present, wantPresent, raw)
		return
	}
	if !present {
		return
	}
	if number, ok := got.AsInt64OK(); !ok || number != wantValue {
		t.Errorf("%s: member %q = %v, want %d", where, member, got, wantValue)
	}
}

// TestFieldItemsCarryAZeroPositionAndPriority pins BUG-0558: the first element
// of a fifo or lifo field is at position 0 and a heap element pushed without a
// priority is at priority 0, and both must arrive as a real zero in a push, a
// peek, a list and a pop answer; an engine that has no position or priority
// answers neither.
func TestFieldItemsCarryAZeroPositionAndPriority(t *testing.T) {
	s := newTestServer(t)
	resp := createWithStores(t, s, "app", "Post", BucketTypeHash,
		StoreDeclaration{Name: "Comments", Engine: "fifoStore"},
		StoreDeclaration{Name: "Photos", Engine: "lifoStore"},
		StoreDeclaration{Name: "Ranked", Engine: "heapStore"},
		StoreDeclaration{Name: "Tags", Engine: "hashStore"},
		StoreDeclaration{Name: "Notes", Engine: "btreeStore"})
	if !resp.Success {
		t.Fatalf("creating Post: %s", resp.Message)
	}
	if inserted, err := s.Insert(InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}); err != nil || !inserted.Success {
		t.Fatalf("inserting p1: %+v, %v", inserted, err)
	}
	address := func(field string) bson.M {
		return bson.M{"database": "app", "bucket": "Post", "key": "p1", "field": field}
	}
	with := func(field string, extra bson.M) bson.M {
		request := address(field)
		for name, value := range extra {
			request[name] = value
		}
		return request
	}
	element := func(n int32) bson.M { return bson.M{"data": bson.M{"n": n}} }

	// The queue engines: position 0 is real, priority never exists.
	for _, field := range []string{"Comments", "Photos"} {
		pushed := zeroFieldAnswer(t, s, transport.OpFieldPush, with(field, element(1)))
		requireMember(t, field+" push", pushed, "position", true, 0)
		requireMember(t, field+" push", pushed, "priority", false, 0)

		peeked := zeroFieldAnswer(t, s, transport.OpFieldPeek, address(field))
		item := peeked.Lookup("items").Array().Index(0).Value().Document()
		requireMember(t, field+" peek item", item, "position", true, 0)
		requireMember(t, field+" peek item", item, "priority", false, 0)

		listed := zeroFieldAnswer(t, s, transport.OpFieldList, address(field))
		entry := listed.Lookup("entries").Array().Index(0).Value().Document()
		requireMember(t, field+" list entry", entry, "position", true, 0)
		requireMember(t, field+" list entry", entry, "priority", false, 0)

		popped := zeroFieldAnswer(t, s, transport.OpFieldPop, address(field))
		poppedItem := popped.Lookup("items").Array().Index(0).Value().Document()
		requireMember(t, field+" pop item", poppedItem, "position", true, 0)
		requireMember(t, field+" pop item", poppedItem, "priority", false, 0)
	}

	// The heap: priority 0 is the default and real, position never exists.
	pushed := zeroFieldAnswer(t, s, transport.OpFieldPush, with("Ranked", element(1)))
	requireMember(t, "heap push", pushed, "priority", true, 0)
	requireMember(t, "heap push", pushed, "position", false, 0)
	peeked := zeroFieldAnswer(t, s, transport.OpFieldPeek, address("Ranked"))
	item := peeked.Lookup("items").Array().Index(0).Value().Document()
	requireMember(t, "heap peek item", item, "priority", true, 0)
	requireMember(t, "heap peek item", item, "position", false, 0)
	listed := zeroFieldAnswer(t, s, transport.OpFieldList, address("Ranked"))
	entry := listed.Lookup("entries").Array().Index(0).Value().Document()
	requireMember(t, "heap list entry", entry, "priority", true, 0)
	requireMember(t, "heap list entry", entry, "position", false, 0)
	popped := zeroFieldAnswer(t, s, transport.OpFieldPop, address("Ranked"))
	poppedItem := popped.Lookup("items").Array().Index(0).Value().Document()
	requireMember(t, "heap pop item", poppedItem, "priority", true, 0)
	requireMember(t, "heap pop item", poppedItem, "position", false, 0)

	// The keyed engines carry neither.
	for _, field := range []string{"Tags", "Notes"} {
		zeroFieldAnswer(t, s, transport.OpFieldPut, with(field, bson.M{"element": "a", "data": bson.M{"n": int32(1)}}))
		listed := zeroFieldAnswer(t, s, transport.OpFieldList, address(field))
		entry := listed.Lookup("entries").Array().Index(0).Value().Document()
		requireMember(t, field+" list entry", entry, "position", false, 0)
		requireMember(t, field+" list entry", entry, "priority", false, 0)
		got := zeroFieldAnswer(t, s, transport.OpFieldGet, with(field, bson.M{"element": "a"}))
		requireMember(t, field+" get", got, "position", false, 0)
		requireMember(t, field+" get", got, "priority", false, 0)
	}
}

// wirePlacementText flattens a position and a priority to comparable text, with a
// nil member (an engine that has none) distinct from a zero.
func wirePlacementText(position, priority *int64) []string {
	text := func(member *int64) string {
		if member == nil {
			return "absent"
		}
		return strconv.FormatInt(*member, 10)
	}
	return []string{text(position), text(priority)}
}
