package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

func TestParseRingConfigNative(t *testing.T) {
	env := envOf(map[string]string{"HOARDDB_ROOT_PASSWORD": "secret"})
	for param, want := range map[string]bool{"on": true, "off": false} {
		got, err := parseRingConfig(Config{URI: "h:1", Params: map[string]string{"insecure": "true", "native": param}}, env)
		if err != nil || got.native != want {
			t.Errorf("native=%s: got %v, %v want %v", param, got.native, err, want)
		}
	}
	got, err := parseRingConfig(Config{URI: "h:1", Params: map[string]string{"insecure": "true"}}, env)
	if err != nil || got.native {
		t.Errorf("native is off by default, got %v, %v", got.native, err)
	}
	for _, bad := range []string{"1", "true", "expand", ""} {
		_, err := parseRingConfig(Config{URI: "h:1", Params: map[string]string{"insecure": "true", "native": bad}}, env)
		want := fmt.Sprintf("ring: -param native=%q is not on or off. Use on to read the copies embedded in User.Feed and Room.Members elements (the data must be seeded with -layout-variant native), off for the common layout", bad)
		if err == nil || err.Error() != want {
			t.Errorf("native=%q: got %v, want %q", bad, err, want)
		}
	}
	if _, err := parseRingConfig(Config{URI: "h:1", Params: map[string]string{"insecure": "true", "colour": "x"}}, env); err == nil || !strings.Contains(err.Error(), "native") {
		t.Errorf("the unknown-key refusal must list native, got %v", err)
	}
	if ringNativeMode(true) != "on" || ringNativeMode(false) != "off" {
		t.Fatalf("modes %q %q", ringNativeMode(true), ringNativeMode(false))
	}
}

// native=on returns the documents a page names, so multiget=on beside it would be the same
// measurement under another label: the pair is refused when the config is read.
func TestNativeAndMultigetAreRefused(t *testing.T) {
	env := envOf(map[string]string{"HOARDDB_ROOT_PASSWORD": "secret"})
	_, err := parseRingConfig(Config{URI: "h:1", Params: map[string]string{"insecure": "true", "native": "on", "multiget": "on"}}, env)
	want := "ring: native=on already returns the documents a page names, so multiget=on would measure the same thing under another label. Use one of them"
	if err == nil || err.Error() != want {
		t.Fatalf("native=on with multiget=on: got %v, want %q", err, want)
	}
	for _, params := range []map[string]string{
		{"native": "on", "multiget": "off"}, {"native": "off", "multiget": "on"}, {"native": "off", "multiget": "off"},
	} {
		params["insecure"] = "true"
		if _, err := parseRingConfig(Config{URI: "h:1", Params: params}, env); err != nil {
			t.Errorf("%v must be accepted: %v", params, err)
		}
	}
	// batch is a write setting and native does not change what a write does.
	if _, err := parseRingConfig(Config{URI: "h:1", Params: map[string]string{"insecure": "true", "native": "on", "batch": "1"}}, env); err != nil {
		t.Errorf("native=on with batch=1 must be accepted: %v", err)
	}
}

func nativeBody(t *testing.T, fields bson.D) bson.Raw {
	t.Helper()
	return ringTestDoc(t, fields)
}

// ringNativeSocialFixture is user u1 whose feed holds native elements for posts ids.
func ringNativeSocialFixture(t *testing.T, ids []string) *fakeRingReads {
	t.Helper()
	fake := &fakeRingReads{elements: map[string][]bson.Raw{}, keys: map[string][]string{}, docs: map[string]bson.Raw{}}
	for i, id := range ids {
		fake.elements["User/u1/Feed"] = append(fake.elements["User/u1/Feed"], nativeBody(t, bson.D{
			{Key: "post", Value: id}, {Key: "at", Value: int64(100 + i)}, {Key: "author", Value: "u" + id}, {Key: "text", Value: "text of " + id}}))
	}
	return fake
}

// ringNativeChatFixture is room r1 with one message and native members.
func ringNativeChatFixture(t *testing.T, members []string) *fakeRingReads {
	t.Helper()
	fake := &fakeRingReads{elements: map[string][]bson.Raw{}, keys: map[string][]string{}, docs: map[string]bson.Raw{}}
	fake.elements["Room/r1/Messages"] = []bson.Raw{nativeBody(t, bson.D{{Key: "text", Value: "hi"}})}
	for i, id := range members {
		fake.elements["Room/r1/Members"] = append(fake.elements["Room/r1/Members"], nativeBody(t, bson.D{
			{Key: "user", Value: id}, {Key: "since", Value: int64(5 + i)}, {Key: "name", Value: "name of " + id}}))
		fake.keys["Room/r1/Members"] = append(fake.keys["Room/r1/Members"], id)
	}
	return fake
}

func ringNativeOps(layout model.Layout, fake *fakeRingReads) *ringOps {
	return &ringOps{layout: layout, database: "db", reads: fake, native: true}
}

// A native feed page is one StoreList and no Get: the post document is rebuilt from the copy,
// and the elements come back in the common shape the verifier checks.
func TestRingNativeFeedIsOneStoreList(t *testing.T) {
	fake := ringNativeSocialFixture(t, postIDs(3))
	reply, err := ringNativeOps(model.Social, fake).Read(context.Background(), feedOp)
	if err != nil {
		t.Fatal(err)
	}
	if want := "StoreList User/u1/Feed limit=10 after=0"; len(fake.calls) != 1 || fake.calls[0] != want {
		t.Fatalf("calls %q, want only %q", fake.calls, want)
	}
	if reply.RoundTrips != 1 {
		t.Fatalf("round trips %d, want 1", reply.RoundTrips)
	}
	if len(reply.Elements) != 3 || len(reply.Documents) != 3 {
		t.Fatalf("%d elements, %d documents", len(reply.Elements), len(reply.Documents))
	}
	for i, element := range reply.Elements {
		if got, want := canonBody(element.Body), fmt.Sprintf("post=s:%q;at=i:%d;", fmt.Sprintf("p%d", i), 100+i); got != want {
			t.Errorf("element %d body %s, want the common body %s", i, got, want)
		}
		doc := reply.Documents[i]
		if want := fmt.Sprintf("Author=s:%q;Text=s:%q;At=i:%d;", fmt.Sprintf("up%d", i), fmt.Sprintf("text of p%d", i), 100+i); doc.Bucket != "Post" || doc.ID != fmt.Sprintf("p%d", i) || canonBody(doc.Fields) != want {
			t.Errorf("document %d = %s/%s %s, want Post/p%d %s", i, doc.Bucket, doc.ID, canonBody(doc.Fields), i, want)
		}
	}
}

// A native room page is the messages and the members, two StoreLists whatever the member count.
func TestRingNativeRoomPageIsTwoStoreLists(t *testing.T) {
	for _, members := range []int{2, 40} {
		ids := make([]string, members)
		for i := range ids {
			ids[i] = fmt.Sprintf("m%d", i)
		}
		fake := ringNativeChatFixture(t, ids)
		reply, err := ringNativeOps(model.Chat, fake).Read(context.Background(), roomOp)
		if err != nil {
			t.Fatal(err)
		}
		want := "StoreList Room/r1/Messages limit=5 after=0\nStoreList Room/r1/Members limit=1000 after=0"
		if got := strings.Join(fake.calls, "\n"); got != want {
			t.Fatalf("%d members: calls\n%s\nwant\n%s", members, got, want)
		}
		if reply.RoundTrips != 2 {
			t.Fatalf("%d members: round trips %d, want 2", members, reply.RoundTrips)
		}
		if len(reply.Documents) != members || len(reply.Elements) != 1+members {
			t.Fatalf("%d members: %d documents, %d elements", members, len(reply.Documents), len(reply.Elements))
		}
		for i, doc := range reply.Documents {
			if want := fmt.Sprintf("Name=s:%q;", "name of "+ids[i]); doc.Bucket != "User" || doc.ID != ids[i] || canonBody(doc.Fields) != want {
				t.Errorf("document %d = %s/%s %s, want User/%s %s", i, doc.Bucket, doc.ID, canonBody(doc.Fields), ids[i], want)
			}
		}
		for _, member := range split(reply.Elements, "Members") {
			if got := canonBody(member.Body); strings.Contains(got, "name") {
				t.Errorf("member %s still carries its name: %s", member.Key, got)
			}
		}
	}
}

// A native adapter on a common seed must fail the read, not fall back to the two-step read: a
// silent fallback would put common numbers in a native leg.
func TestRingNativeRefusesACommonBody(t *testing.T) {
	fake := ringSocialFixture(t, postIDs(2))
	_, err := ringNativeOps(model.Social, fake).Read(context.Background(), feedOp)
	if err == nil || !strings.Contains(err.Error(), "-layout-variant native") {
		t.Fatalf("a common feed read in native mode: %v", err)
	}
	if fake.getCalls() != 0 {
		t.Fatalf("native mode fell back to %d Gets", fake.getCalls())
	}
	fake = ringChatFixture(t, []string{"a"})
	if _, err = ringNativeOps(model.Chat, fake).Read(context.Background(), roomOp); err == nil || !strings.Contains(err.Error(), "-layout-variant native") {
		t.Fatalf("a common room page read in native mode: %v", err)
	}
}

// Every read that is not the feed or the room page is the common code path in native mode.
func TestRingNativeLeavesOtherReadsAlone(t *testing.T) {
	fake := &fakeRingReads{elements: map[string][]bson.Raw{"User/u1/Inbox": {ringTestDoc(t, bson.D{{Key: "post", Value: "p1"}, {Key: "author", Value: "u2"}, {Key: "at", Value: int64(3)}})}},
		keys: map[string][]string{}, docs: map[string]bson.Raw{}}
	reply, err := ringNativeOps(model.Social, fake).Read(context.Background(), ReadOp{Layout: model.Social, Kind: ReadSocialInbox, Entity: "u1", Limit: 20})
	if err != nil || reply.RoundTrips != 1 || len(reply.Elements) != 1 || len(reply.Documents) != 0 {
		t.Fatalf("inbox in native mode: %+v %v", reply, err)
	}
}

func nativePostWrite() WriteOp {
	return WriteOp{Layout: model.Social, Kind: WriteSocialPost, ID: "w1", Ordinal: 1,
		Documents: []model.Document{{Bucket: "Post", ID: "pw1", Fields: []model.Field{{Name: "Author", Value: "u3"}, {Name: "Text", Value: "hello there"}, {Name: "At", Value: int64(77)}}}},
		Elements: []model.Element{
			{Bucket: "User", Owner: "u3", Store: "Feed", Seq: SeqBase + 1, Body: []model.Field{{Name: "post", Value: "pw1"}, {Name: "at", Value: int64(77)}}},
			{Bucket: "User", Owner: "u4", Store: "Inbox", Seq: SeqBase + 1, Body: []model.Field{{Name: "post", Value: "pw1"}, {Name: "author", Value: "u3"}, {Name: "at", Value: int64(77)}}}}}
}

func nativeJoinWrite() WriteOp {
	return WriteOp{Layout: model.Chat, Kind: WriteChatJoin, ID: "w2", Ordinal: 2,
		Documents: []model.Document{{Bucket: "Member", ID: "r1/u5", Fields: []model.Field{{Name: "Room", Value: "r1"}, {Name: "User", Value: "u5"}, {Name: "Since", Value: int64(9)}}}},
		Refs:      []model.Document{{Bucket: "User", ID: "u5", Fields: []model.Field{{Name: "Name", Value: "five name"}}}},
		Elements: []model.Element{
			{Bucket: "Room", Owner: "r1", Store: "Members", Key: "u5", Seq: SeqBase + 2, Body: []model.Field{{Name: "user", Value: "u5"}, {Name: "since", Value: int64(9)}}},
			{Bucket: "User", Owner: "u5", Store: "Rooms", Key: "r1", Seq: SeqBase + 2, Body: []model.Field{{Name: "room", Value: "r1"}, {Name: "since", Value: int64(9)}}}}}
}

// Native writes carry the copies in the elements that hold them, take them from the documents of
// the same write, and leave the op itself alone: the registry and the audit read its bodies.
func TestRingNativeWritesCarryTheCopiesAndLeaveTheOpAlone(t *testing.T) {
	post, join := nativePostWrite(), nativeJoinWrite()
	before := fmt.Sprint(post, join)
	posted, err := (&ringOps{layout: model.Social, native: true}).ringWrites(post)
	if err != nil {
		t.Fatal(err)
	}
	joined, err := (&ringOps{layout: model.Chat, native: true}).ringWrites(join)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := canonBody(posted[1].element.Body), `post=s:"pw1";at=i:77;author=s:"u3";text=s:"hello there";`; got != want {
		t.Errorf("feed element body %s, want %s", got, want)
	}
	if got, want := canonBody(posted[2].element.Body), `post=s:"pw1";author=s:"u3";at=i:77;`; got != want {
		t.Errorf("inbox element body %s, want it unchanged %s", got, want)
	}
	if got, want := canonBody(joined[1].element.Body), `user=s:"u5";since=i:9;name=s:"five name";`; got != want {
		t.Errorf("member element body %s, want %s", got, want)
	}
	if got, want := canonBody(joined[2].element.Body), `room=s:"r1";since=i:9;`; got != want {
		t.Errorf("rooms element body %s, want it unchanged %s", got, want)
	}
	if after := fmt.Sprint(post, join); after != before {
		t.Fatalf("ringWrites changed the op:\n%s\n%s", before, after)
	}
}

func TestRingCommonWritesAreUnchanged(t *testing.T) {
	post, join := nativePostWrite(), nativeJoinWrite()
	posted, err := (&ringOps{layout: model.Social}).ringWrites(post)
	if err != nil {
		t.Fatal(err)
	}
	joined, err := (&ringOps{layout: model.Chat}).ringWrites(join)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := canonBody(posted[1].element.Body), canonBody(post.Elements[0].Body); got != want {
		t.Errorf("common feed element %s, want %s", got, want)
	}
	if got, want := canonBody(joined[1].element.Body), canonBody(join.Elements[0].Body); got != want {
		t.Errorf("common member element %s, want %s", got, want)
	}
}

// A native write whose source document is not in the op is an error, never an empty copy.
func TestRingNativeWriteRefusesAMissingSource(t *testing.T) {
	post := nativePostWrite()
	post.Documents = nil
	if _, err := (&ringOps{layout: model.Social, native: true}).ringWrites(post); err == nil || !strings.Contains(err.Error(), "pw1") {
		t.Errorf("a post write without its Post document: %v", err)
	}
	join := nativeJoinWrite()
	join.Refs = nil
	if _, err := (&ringOps{layout: model.Chat, native: true}).ringWrites(join); err == nil || !strings.Contains(err.Error(), "u5") {
		t.Errorf("a join without the member's User document: %v", err)
	}
}

// ReadStore strips, so the audit's count and keyed checks see the common shape; ReadEmbeds
// returns the native bodies the embed audit compares.
func TestRingReadStoreStripsAndReadEmbedsDoesNot(t *testing.T) {
	fake := ringNativeChatFixture(t, []string{"a", "b"})
	ops := ringNativeOps(model.Chat, fake)
	stripped, err := ops.ReadStore(context.Background(), StoreRef{"Room", "r1", "Members"})
	if err != nil || len(stripped) != 2 {
		t.Fatalf("ReadStore: %v, %d elements", err, len(stripped))
	}
	for _, element := range stripped {
		if got := canonBody(element.Body); strings.Contains(got, "name") || element.Key == "" {
			t.Errorf("ReadStore returned %s key %q, want the common body and the key", got, element.Key)
		}
	}
	embedded, err := ops.ReadEmbeds(context.Background(), StoreRef{"Room", "r1", "Members"})
	if err != nil || len(embedded) != 2 {
		t.Fatalf("ReadEmbeds: %v, %d elements", err, len(embedded))
	}
	if got, want := canonBody(embedded[1].Body), `user=s:"b";since=i:6;name=s:"name of b";`; got != want || embedded[1].Key != "b" {
		t.Errorf("ReadEmbeds element %s key %q, want %s key b", got, embedded[1].Key, want)
	}
	if _, ok := LayoutOps(ops).(EmbedReader); !ok {
		t.Fatal("the ring adapter must implement EmbedReader")
	}
	// A store with no copy reads the same through both.
	plain, err := ops.ReadStore(context.Background(), StoreRef{"Room", "r1", "Messages"})
	if err != nil || len(plain) != 1 {
		t.Fatalf("ReadStore of messages: %v, %d", err, len(plain))
	}
}

// The open-time probe: the server's first feed element and first room member must carry the
// copies exactly when -param native=on, so a native run on a common seed (or the reverse) is
// refused before any request is made.
func TestNativeProbe(t *testing.T) {
	nativeSocial, commonSocial := ringNativeSocialFixture(t, postIDs(1)), ringSocialFixture(t, postIDs(1))
	nativeSocial.elements["User/u0/Feed"], commonSocial.elements["User/u0/Feed"] = nativeSocial.elements["User/u1/Feed"], commonSocial.elements["User/u1/Feed"]
	nativeChat, commonChat := ringNativeChatFixture(t, []string{"a"}), ringChatFixture(t, []string{"a"})
	nativeChat.elements["Room/r0/Members"], commonChat.elements["Room/r0/Members"] = nativeChat.elements["Room/r1/Members"], commonChat.elements["Room/r1/Members"]
	for name, tc := range map[string]struct {
		layout    model.Layout
		fake      *fakeRingReads
		native    bool
		wantError string
	}{
		"native seed, native on, social":  {model.Social, nativeSocial, true, ""},
		"common seed, native off, social": {model.Social, commonSocial, false, ""},
		"common seed, native on, social":  {model.Social, commonSocial, true, "this server holds the common layout but -param native=on was given. Reseed with -layout-variant native or change the flag"},
		"native seed, native off, social": {model.Social, nativeSocial, false, "this server holds the native layout but -param native=off was given. Reseed with -layout-variant common or change the flag"},
		"native seed, native on, chat":    {model.Chat, nativeChat, true, ""},
		"common seed, native off, chat":   {model.Chat, commonChat, false, ""},
		"common seed, native on, chat":    {model.Chat, commonChat, true, "this server holds the common layout but -param native=on was given"},
		"native seed, native off, chat":   {model.Chat, nativeChat, false, "this server holds the native layout but -param native=off was given"},
	} {
		err := probeVariant(context.Background(), tc.fake, tc.layout, tc.native)
		switch {
		case tc.wantError == "" && err != nil:
			t.Errorf("%s: %v", name, err)
		case tc.wantError != "" && (err == nil || !strings.Contains(err.Error(), tc.wantError)):
			t.Errorf("%s: got %v, want %q", name, err, tc.wantError)
		}
	}
	// A layout with no copy is not probed, and an unseeded database says nothing about the variant.
	empty := &fakeRingReads{elements: map[string][]bson.Raw{}, keys: map[string][]string{}, docs: map[string]bson.Raw{}}
	for _, layout := range []model.Layout{model.Social, model.Chat, model.Ledger, model.Agent} {
		if err := probeVariant(context.Background(), empty, layout, true); err != nil {
			t.Errorf("%s on an empty database: %v", layout, err)
		}
	}
	untouched := ringSocialFixture(t, postIDs(1))
	for _, layout := range []model.Layout{model.Ledger, model.Agent} {
		if err := probeVariant(context.Background(), untouched, layout, true); err != nil || len(untouched.calls) != 0 {
			t.Errorf("%s was probed: %v %v", layout, err, untouched.calls)
		}
	}
}

// The probe reads the first element of the first feed and the first room, one element each.
func TestNativeProbeReadsOneElement(t *testing.T) {
	fake := &fakeRingReads{elements: map[string][]bson.Raw{}, keys: map[string][]string{}, docs: map[string]bson.Raw{}}
	_ = probeVariant(context.Background(), fake, model.Social, false)
	_ = probeVariant(context.Background(), fake, model.Chat, false)
	want := []string{"StoreList User/u0/Feed limit=1 after=0", "StoreList Room/r0/Members limit=1 after=0"}
	if strings.Join(fake.calls, "|") != strings.Join(want, "|") {
		t.Fatalf("calls %q, want %q", fake.calls, want)
	}
}
