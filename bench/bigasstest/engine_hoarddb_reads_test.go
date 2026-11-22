package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// fakeRingReads is an in-memory ringReads that records every call, in order, as one line.
type fakeRingReads struct {
	calls []string
	// elements and keys are the store pages by "bucket/id/store".
	elements map[string][]bson.Raw
	keys     map[string][]string
	// docs are the documents by "bucket/id".
	docs map[string]bson.Raw
	// answerErr makes the GetMany answer of that "bucket/id" a per-key error.
	answerErr map[string]error
	// manyErr is returned by every GetMany call as the frame's error.
	manyErr error
}

func (f *fakeRingReads) StoreList(_ context.Context, bucket, id, store string, limit int, after []byte) (ringclient.ElementPage, error) {
	name := bucket + "/" + id + "/" + store
	f.calls = append(f.calls, fmt.Sprintf("StoreList %s limit=%d after=%d", name, limit, len(after)))
	return ringclient.ElementPage{Elements: f.elements[name], Keys: f.keys[name]}, nil
}

func (f *fakeRingReads) GetFields(_ context.Context, bucket, id string, fields []string) (bson.Raw, error) {
	f.calls = append(f.calls, fmt.Sprintf("GetFields %s/%s %v", bucket, id, fields))
	doc, ok := f.docs[bucket+"/"+id]
	if !ok {
		return nil, fmt.Errorf("get: %w", ringclient.ErrNotFound)
	}
	return doc, nil
}

func (f *fakeRingReads) GetMany(_ context.Context, bucket string, ids, fields []string) ([]ringAnswer, error) {
	f.calls = append(f.calls, fmt.Sprintf("GetMany %s n=%d first=%s %v", bucket, len(ids), strings.Join(ids[:min(1, len(ids))], ""), fields))
	if f.manyErr != nil {
		return nil, f.manyErr
	}
	answers := make([]ringAnswer, len(ids))
	for i, id := range ids {
		if err := f.answerErr[bucket+"/"+id]; err != nil {
			answers[i] = ringAnswer{Err: err}
			continue
		}
		doc, ok := f.docs[bucket+"/"+id]
		answers[i] = ringAnswer{Doc: doc, Found: ok}
	}
	return answers, nil
}

func (f *fakeRingReads) getManyCalls() int {
	n := 0
	for _, call := range f.calls {
		if strings.HasPrefix(call, "GetMany") {
			n++
		}
	}
	return n
}

func (f *fakeRingReads) getCalls() int {
	n := 0
	for _, call := range f.calls {
		if strings.HasPrefix(call, "GetFields") {
			n++
		}
	}
	return n
}

func ringTestDoc(t *testing.T, members bson.D) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(members)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// ringSocialFixture is user u1 whose feed names the posts ids; every post except those in
// missing exists.
func ringSocialFixture(t *testing.T, ids []string, missing ...string) *fakeRingReads {
	t.Helper()
	fake := &fakeRingReads{elements: map[string][]bson.Raw{}, keys: map[string][]string{}, docs: map[string]bson.Raw{}}
	for _, id := range ids {
		fake.elements["User/u1/Feed"] = append(fake.elements["User/u1/Feed"], ringTestDoc(t, bson.D{{Key: "post", Value: id}}))
		fake.docs["Post/"+id] = ringTestDoc(t, bson.D{{Key: "Text", Value: "text of " + id}})
	}
	for _, id := range missing {
		delete(fake.docs, "Post/"+id)
	}
	return fake
}

// ringChatFixture is room r1 with messages and members; every member user exists except those in missing.
func ringChatFixture(t *testing.T, members []string, missing ...string) *fakeRingReads {
	t.Helper()
	fake := &fakeRingReads{elements: map[string][]bson.Raw{}, keys: map[string][]string{}, docs: map[string]bson.Raw{}}
	fake.elements["Room/r1/Messages"] = []bson.Raw{ringTestDoc(t, bson.D{{Key: "text", Value: "hi"}})}
	for _, id := range members {
		fake.elements["Room/r1/Members"] = append(fake.elements["Room/r1/Members"], ringTestDoc(t, bson.D{{Key: "role", Value: "member"}}))
		fake.keys["Room/r1/Members"] = append(fake.keys["Room/r1/Members"], id)
		fake.docs["User/"+id] = ringTestDoc(t, bson.D{{Key: "Name", Value: "name of " + id}})
	}
	for _, id := range missing {
		delete(fake.docs, "User/"+id)
	}
	return fake
}

func ringReadOps(layout model.Layout, fake *fakeRingReads, multiget bool) *ringOps {
	return &ringOps{layout: layout, database: "db", reads: fake, multiget: multiget}
}

func postIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("p%d", i)
	}
	return ids
}

var (
	feedOp = ReadOp{Layout: model.Social, Kind: ReadSocialFeed, Entity: "u1", Limit: 10}
	roomOp = ReadOp{Layout: model.Chat, Kind: ReadChatRoomPage, Entity: "r1", Limit: 5}
)

// TestRingMultiGetOffKeepsTheCallSequenceOfEveryManyDocumentRead is the golden: with multiget
// off the adapter makes exactly the calls it made before the switch existed.
func TestRingMultiGetOffKeepsTheCallSequenceOfEveryManyDocumentRead(t *testing.T) {
	scalars := func(layout model.Layout, bucket string) string { return fmt.Sprint(ringScalars[layout][bucket]) }
	t.Run("social.feed", func(t *testing.T) {
		fake := ringSocialFixture(t, postIDs(3))
		reply, err := ringReadOps(model.Social, fake, false).Read(context.Background(), feedOp)
		if err != nil {
			t.Fatal(err)
		}
		want := "StoreList User/u1/Feed limit=10 after=0\n"
		for i := range 3 {
			want += fmt.Sprintf("GetFields Post/p%d %s\n", i, scalars(model.Social, "Post"))
		}
		if got := strings.Join(fake.calls, "\n") + "\n"; got != want {
			t.Fatalf("calls:\n%s\nwant:\n%s", got, want)
		}
		if reply.RoundTrips != 4 || len(reply.Documents) != 3 || reply.Documents[2].ID != "p2" {
			t.Fatalf("reply %d round trips, documents %+v", reply.RoundTrips, reply.Documents)
		}
	})
	t.Run("chat.room_page", func(t *testing.T) {
		fake := ringChatFixture(t, []string{"a", "b"})
		reply, err := ringReadOps(model.Chat, fake, false).Read(context.Background(), roomOp)
		if err != nil {
			t.Fatal(err)
		}
		want := "StoreList Room/r1/Messages limit=5 after=0\nStoreList Room/r1/Members limit=1000 after=0\n" +
			"GetFields User/a " + scalars(model.Chat, "User") + "\nGetFields User/b " + scalars(model.Chat, "User") + "\n"
		if got := strings.Join(fake.calls, "\n") + "\n"; got != want {
			t.Fatalf("calls:\n%s\nwant:\n%s", got, want)
		}
		if reply.RoundTrips != 4 || len(reply.Documents) != 2 {
			t.Fatalf("reply %d round trips, %d documents", reply.RoundTrips, len(reply.Documents))
		}
	})
}

// TestRingMissingDocumentFailsTheOperationInBothModes: a feed that names a deleted post fails
// the read naming the bucket and id, as the single Get does; the arms must do the same work.
func TestRingMissingDocumentFailsTheOperationInBothModes(t *testing.T) {
	for _, multiget := range []bool{false, true} {
		t.Run(ringMultiGetMode(multiget), func(t *testing.T) {
			fake := ringSocialFixture(t, postIDs(3), "p1")
			_, err := ringReadOps(model.Social, fake, multiget).Read(context.Background(), feedOp)
			if err == nil || !strings.Contains(err.Error(), "reading Post p1 in db") || !errors.Is(err, ringclient.ErrNotFound) {
				t.Fatalf("feed with a missing post: %v", err)
			}
			fake = ringChatFixture(t, []string{"a", "b"}, "b")
			_, err = ringReadOps(model.Chat, fake, multiget).Read(context.Background(), roomOp)
			if err == nil || !strings.Contains(err.Error(), "reading User b in db") || !errors.Is(err, ringclient.ErrNotFound) {
				t.Fatalf("room with a missing member: %v", err)
			}
		})
	}
}

// TestRingMultiGetOnIssuesOneGetManyPerPageAndNoGet counts the calls: a feed page is the page
// read and one GetMany, a room page the messages, the members and one GetMany.
func TestRingMultiGetOnIssuesOneGetManyPerPageAndNoGet(t *testing.T) {
	scalars := func(layout model.Layout, bucket string) string { return fmt.Sprint(ringScalars[layout][bucket]) }
	feed := ringSocialFixture(t, postIDs(10))
	reply, err := ringReadOps(model.Social, feed, true).Read(context.Background(), feedOp)
	if err != nil {
		t.Fatal(err)
	}
	if feed.getManyCalls() != 1 || feed.getCalls() != 0 || reply.RoundTrips != 2 {
		t.Fatalf("feed: %d GetMany, %d Get, %d round trips, want 1, 0, 2; calls %q", feed.getManyCalls(), feed.getCalls(), reply.RoundTrips, feed.calls)
	}
	if want := "GetMany Post n=10 first=p0 " + scalars(model.Social, "Post"); feed.calls[1] != want {
		t.Fatalf("feed GetMany call %q, want %q", feed.calls[1], want)
	}
	room := ringChatFixture(t, []string{"a", "b", "c", "d", "e", "f", "g"})
	reply, err = ringReadOps(model.Chat, room, true).Read(context.Background(), roomOp)
	if err != nil {
		t.Fatal(err)
	}
	if room.getManyCalls() != 1 || room.getCalls() != 0 || reply.RoundTrips != 3 {
		t.Fatalf("room: %d GetMany, %d Get, %d round trips, want 1, 0, 3; calls %q", room.getManyCalls(), room.getCalls(), reply.RoundTrips, room.calls)
	}
	if want := "GetMany User n=7 first=a " + scalars(model.Chat, "User"); room.calls[2] != want {
		t.Fatalf("room GetMany call %q, want %q", room.calls[2], want)
	}
}

// TestRingMultiGetReturnsTheSameDocumentsInTheSameOrder: the two arms must hand the runner
// identical replies, apart from the round trips.
func TestRingMultiGetReturnsTheSameDocumentsInTheSameOrder(t *testing.T) {
	read := func(layout model.Layout, fake *fakeRingReads, op ReadOp, multiget bool) Reply {
		reply, err := ringReadOps(layout, fake, multiget).Read(context.Background(), op)
		if err != nil {
			t.Fatal(err)
		}
		return reply
	}
	// the feed names p3 twice: the second answer is a document of its own, as with two Gets.
	ids := append(postIDs(5), "p3")
	feedOff, feedOn := read(model.Social, ringSocialFixture(t, ids), feedOp, false), read(model.Social, ringSocialFixture(t, ids), feedOp, true)
	roomOff, roomOn := read(model.Chat, ringChatFixture(t, []string{"z", "a", "m"}), roomOp, false), read(model.Chat, ringChatFixture(t, []string{"z", "a", "m"}), roomOp, true)
	for name, pair := range map[string][2]Reply{"feed": {feedOff, feedOn}, "room": {roomOff, roomOn}} {
		off, on := pair[0], pair[1]
		if len(off.Documents) == 0 || !reflect.DeepEqual(off.Documents, on.Documents) || !reflect.DeepEqual(off.Elements, on.Elements) {
			t.Errorf("%s: documents or elements differ between the arms:\noff %+v\non  %+v", name, off, on)
		}
		if on.RoundTrips >= off.RoundTrips {
			t.Errorf("%s: multiget used %d round trips, off %d", name, on.RoundTrips, off.RoundTrips)
		}
	}
	if feedOn.Documents[5].ID != "p3" || roomOn.Documents[0].ID != "z" {
		t.Errorf("order or duplicate lost: %+v %+v", feedOn.Documents, roomOn.Documents)
	}
}

// TestRingMultiGetLeavesChunkingToTheClient: 600 ids go to the client as one call; the adapter
// counts the frames the client will send, ceil(600/255) = 3, plus the page read.
func TestRingMultiGetLeavesChunkingToTheClient(t *testing.T) {
	fake := ringSocialFixture(t, postIDs(600))
	reply, err := ringReadOps(model.Social, fake, true).Read(context.Background(), feedOp)
	if err != nil {
		t.Fatal(err)
	}
	if fake.getManyCalls() != 1 || !strings.Contains(fake.calls[1], "n=600") {
		t.Fatalf("the adapter must pass all 600 ids in one call, calls %q", fake.calls)
	}
	if reply.RoundTrips != 1+3 || len(reply.Documents) != 600 {
		t.Fatalf("%d round trips for 600 posts, want 4; %d documents", reply.RoundTrips, len(reply.Documents))
	}
	for n, frames := range map[int]int{0: 0, 1: 1, 255: 1, 256: 2, 510: 2, 511: 3, 1000: 4} {
		if got := ringGetManyFrames(n); got != frames {
			t.Errorf("ringGetManyFrames(%d) = %d, want %d", n, got, frames)
		}
	}
}

// TestRingMultiGetOfNothingSendsNoFrame: an empty feed costs the page read alone, in both arms.
func TestRingMultiGetOfNothingSendsNoFrame(t *testing.T) {
	for _, multiget := range []bool{false, true} {
		fake := ringSocialFixture(t, nil)
		reply, err := ringReadOps(model.Social, fake, multiget).Read(context.Background(), feedOp)
		if err != nil || reply.RoundTrips != 1 || fake.getManyCalls() != 0 || len(reply.Documents) != 0 {
			t.Fatalf("multiget=%v: %d round trips, calls %q, err %v", multiget, reply.RoundTrips, fake.calls, err)
		}
	}
}

// TestRingMultiGetFailuresNameTheDocument: a frame error and a per-key error both fail the
// operation with the bucket and id (or the page) in the message, and the cause kept.
func TestRingMultiGetFailuresNameTheDocument(t *testing.T) {
	cause := errors.New("owner down")
	fake := ringSocialFixture(t, postIDs(3))
	fake.answerErr = map[string]error{"Post/p2": cause}
	_, err := ringReadOps(model.Social, fake, true).Read(context.Background(), feedOp)
	if err == nil || !errors.Is(err, cause) || !strings.Contains(err.Error(), "reading Post p2 in db") {
		t.Fatalf("per-key error: %v", err)
	}
	fake = ringSocialFixture(t, postIDs(3))
	fake.manyErr = cause
	_, err = ringReadOps(model.Social, fake, true).Read(context.Background(), feedOp)
	if err == nil || !errors.Is(err, cause) || !strings.Contains(err.Error(), "Post") || !strings.Contains(err.Error(), "db") {
		t.Fatalf("frame error: %v", err)
	}
}

func TestRingMultiGetParamParsesAndRecords(t *testing.T) {
	env := envOf(map[string]string{"HOARDDB_ROOT_PASSWORD": "secret"})
	for param, want := range map[string]bool{"on": true, "off": false} {
		got, err := parseRingConfig(Config{URI: "h:1", Params: map[string]string{"insecure": "true", "multiget": param}}, env)
		if err != nil || got.multiget != want {
			t.Errorf("multiget=%s: got %v, %v want %v", param, got.multiget, err, want)
		}
	}
	got, err := parseRingConfig(Config{URI: "h:1", Params: map[string]string{"insecure": "true"}}, env)
	if err != nil || got.multiget {
		t.Errorf("multiget is off by default, got %v, %v", got.multiget, err)
	}
	for _, bad := range []string{"1", "true", "yes", ""} {
		_, err := parseRingConfig(Config{URI: "h:1", Params: map[string]string{"insecure": "true", "multiget": bad}}, env)
		want := fmt.Sprintf("ring: -param multiget=%q is not on or off. Use on to read the documents of a page with one get-many frame, off to read them one at a time", bad)
		if err == nil || err.Error() != want {
			t.Errorf("multiget=%q: got %v, want %q", bad, err, want)
		}
	}
	if _, err := parseRingConfig(Config{URI: "h:1", Params: map[string]string{"insecure": "true", "colour": "x"}}, env); err == nil || !strings.Contains(err.Error(), "multiget") {
		t.Errorf("the unknown-key refusal must list multiget, got %v", err)
	}
	if ringMultiGetMode(true) != "on" || ringMultiGetMode(false) != "off" {
		t.Fatalf("modes %q %q", ringMultiGetMode(true), ringMultiGetMode(false))
	}
}

// TestRecordCarriesMultiGetAndKeepsLegsApart: the runner copies the engine's multiget setting
// into the record, a multiget record gets its own leg name, and an off record keeps the old one.
func TestRecordCarriesMultiGetAndKeepsLegsApart(t *testing.T) {
	p, _ := fakeRun(t, func(f *fakeEngine) { f.extraSettings = map[string]string{"multiget": "on"} })
	rec, err := runRepetition(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	if rec.MultiGet != "on" || rec.Settings["multiget"] != "on" {
		t.Fatalf("record multiget %q, settings %v", rec.MultiGet, rec.Settings)
	}
	off := completeRecord()
	on := completeRecord()
	off.MultiGet, on.MultiGet = "off", "on"
	if off.legName() != completeRecord().legName() {
		t.Error("a multiget=off record keeps the leg name it always had")
	}
	if off.legName() == on.legName() {
		t.Fatalf("multiget on and off share the leg %q", on.legName())
	}
	if dir := t.TempDir(); recordPath(dir, off) == recordPath(dir, on) {
		t.Fatal("multiget on and off records would overwrite each other")
	}
	encoded, err := json.Marshal(on)
	if err != nil || !strings.Contains(string(encoded), `"multiget":"on"`) {
		t.Fatalf("the record must carry multiget: %s %v", encoded, err)
	}
}
