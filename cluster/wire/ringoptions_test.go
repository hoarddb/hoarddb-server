package wire

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// ringNineExpands is one expansion over the cap: 9 specs where 8 are taken.
func ringNineExpands() RingReadOptions {
	expand := make([]RingExpandSpec, 9)
	for i := range expand {
		expand[i] = RingExpandSpec{Field: "Author"}
	}
	return RingReadOptions{Expand: expand}
}

// TestReadOptionsRoundTrip: [GUARD the trailer carries local and expand and refuses a lying length].
func TestReadOptionsRoundTrip(t *testing.T) {
	want := RingReadOptions{Local: true, Expand: []RingExpandSpec{{Field: "Author", Fields: []string{"Name"}}}}
	trailer, err := EncodeRingReadOptions(want)
	if err != nil {
		t.Fatalf("encoding %+v: %v", want, err)
	}
	if len(trailer) < 3 {
		t.Fatalf("the trailer is %d bytes, want a 2-byte length and a document", len(trailer))
	}
	got, used, err := DecodeRingReadOptions(trailer)
	if err != nil {
		t.Fatalf("decoding %x: %v", trailer, err)
	}
	if used != len(trailer) {
		t.Fatalf("the decoder used %d of %d bytes", used, len(trailer))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip\n got %+v\nwant %+v", got, want)
	}
	lied := bytes.Clone(trailer)
	lied[0] ^= 0xff
	if _, _, err := DecodeRingReadOptions(lied); err == nil {
		t.Fatal("a trailer whose length lies was accepted")
	} else if !strings.Contains(err.Error(), "expand") {
		t.Fatalf("the refusal %q does not name expand", err)
	}
	empty, used, err := DecodeRingReadOptions(nil)
	if err != nil || used != 0 || !empty.Empty() {
		t.Fatalf("no trailer decoded as %+v and %d bytes, %v", empty, used, err)
	}
}

// TestExpandTooManyIsRefusedBeforeSending: [GUARD the cap of 8 lands before any bytes are
// produced, on the trailer and on both requests that carry it].
func TestExpandTooManyIsRefusedBeforeSending(t *testing.T) {
	want := cluster.CodeInvalidArgument.Text(cluster.CondExpandTooMany, map[string]string{"n": "9"})
	options := ringNineExpands()
	if raw, err := EncodeRingReadOptions(options); err == nil || len(raw) != 0 {
		t.Fatalf("the trailer encoded %d bytes, %v", len(raw), err)
	} else if err.Error() != want {
		t.Fatalf("the trailer refusal is %q, want %q", err, want)
	}
	read := RingRead{Class: RingClassGetFields, BucketID: 41, DeclVersion: 7, SchemaVersionSeen: 7,
		EncID: u65EncID, Fields: []string{"Name"}, Options: options}
	if raw, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &read}); err == nil || len(raw) != 0 {
		t.Fatalf("the get_fields encoded %d bytes, %v", len(raw), err)
	} else if err.Error() != want {
		t.Fatalf("the get_fields refusal is %q, want %q", err, want)
	}
	list := RingListRequest{Database: "app", Bucket: "User", Options: options}
	if raw, err := EncodeRingList(list); err == nil || len(raw) != 0 {
		t.Fatalf("the list encoded %d bytes, %v", len(raw), err)
	} else if err.Error() != want {
		t.Fatalf("the list refusal is %q, want %q", err, want)
	}
}

// TestOptionsOnAPeekAreRefused: [GUARD only get, store list and get_fields carry options].
func TestOptionsOnAPeekAreRefused(t *testing.T) {
	read := RingRead{Class: RingClassPeek, BucketID: 41, DeclVersion: 7, SchemaVersionSeen: 7,
		EncID: u65EncID, FieldID: 4, Limit: 5, Options: RingReadOptions{Local: true}}
	raw, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &read})
	if err == nil || len(raw) != 0 {
		t.Fatalf("the peek encoded %d bytes, %v", len(raw), err)
	}
	want := "read class 0x82 takes no options; drop local and expand (only get 0x81, store list 0x83 and get_fields 0x89 carry them)"
	if err.Error() != want {
		t.Fatalf("the refusal is %q, want %q", err, want)
	}
}

// TestListDocWithoutExpandStillReEncodes: [GUARD a list request with no options keeps its
// exact bytes, with no trailer members].
func TestListDocWithoutExpandStillReEncodes(t *testing.T) {
	req := RingListRequest{Database: "app", Bucket: "User", Limit: u65Int32(10)}
	raw, err := EncodeRingList(req)
	if err != nil {
		t.Fatalf("encoding %+v: %v", req, err)
	}
	got, err := DecodeRingList(raw)
	if err != nil {
		t.Fatalf("decoding %x: %v", raw, err)
	}
	if !reflect.DeepEqual(got, req) {
		t.Fatalf("round trip\n got %+v\nwant %+v", got, req)
	}
	for _, member := range []string{"expand", "local"} {
		if bytes.Contains(raw, []byte(member)) {
			t.Fatalf("a list request with no options names %q: %x", member, raw)
		}
	}
}

// TestReadOptionsRoundTripOnTheClassesThatCarryThem: [GUARD the trailer rides after the
// class form of a get_fields and a store list, and decodes back unchanged].
func TestReadOptionsRoundTripOnTheClassesThatCarryThem(t *testing.T) {
	options := RingReadOptions{Local: true, Expand: []RingExpandSpec{{Field: "Author", Fields: []string{"Name"}}}}
	base := RingRead{BucketID: 41, DeclVersion: 7, SchemaVersionSeen: 7, EncID: u65EncID, Options: options}
	getFields := base
	getFields.Class, getFields.Fields = RingClassGetFields, []string{"Name", "Bio"}
	storeList := base
	storeList.Class, storeList.FieldID, storeList.Limit, storeList.After = RingClassStoreList, 4, 10, []byte{9, 9}
	for _, read := range []RingRead{getFields, storeList} {
		raw, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &read})
		if err != nil {
			t.Fatalf("class 0x%02x: %v", read.Class, err)
		}
		decoded, err := DecodeRingKeyed(raw)
		if err != nil || decoded.Read == nil {
			t.Fatalf("class 0x%02x decodes as %+v, %v", read.Class, decoded, err)
		}
		if !reflect.DeepEqual(*decoded.Read, read) {
			t.Fatalf("class 0x%02x round trip\n got %+v\nwant %+v", read.Class, *decoded.Read, read)
		}
	}
}

// TestExpandedReplyRoundTrip: [GUARD the reply carries the expanded documents, the filtered
// count and which node served the read].
func TestExpandedReplyRoundTrip(t *testing.T) {
	far, err := bson.Marshal(bson.D{{Key: "Name", Value: "Ada"}})
	if err != nil {
		t.Fatal(err)
	}
	want := RingReply{Found: u65Bool(true), Expanded: [][]byte{far}, ExpandFiltered: u65Int32(2), Served: "local"}
	raw, err := EncodeRingReply(want)
	if err != nil {
		t.Fatalf("encoding %+v: %v", want, err)
	}
	got, err := DecodeRingReply(raw)
	if err != nil {
		t.Fatalf("decoding %x: %v", raw, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip\n got %+v\nwant %+v", got, want)
	}
	for name, reply := range map[string]RingReply{
		"expanded is not BSON": {Expanded: [][]byte{{1, 2, 3}}},
		"filtered below zero":  {ExpandFiltered: u65Int32(-1)},
		"served elsewhere":     {Served: "elsewhere"},
	} {
		if _, err := EncodeRingReply(reply); err == nil {
			t.Errorf("%s: the encoder accepted it", name)
		}
	}
}

// TestAnEmptyFieldsListIsRefusedNotSentAsEveryField: [GUARD fields: [] has no encoding, so it is
// refused instead of arriving as "every scalar"].
func TestAnEmptyFieldsListIsRefusedNotSentAsEveryField(t *testing.T) {
	options := RingReadOptions{Expand: []RingExpandSpec{{Field: "Author", Bucket: "User", Fields: []string{}}}}
	_, err := EncodeRingReadOptions(options)
	if err == nil || !strings.Contains(err.Error(), "expand[0].fields is empty") {
		t.Fatalf("an empty fields list was encoded: %v", err)
	}
	if _, err := EncodeRingReadOptions(RingReadOptions{Expand: []RingExpandSpec{{Field: "Author", Bucket: "User"}}}); err != nil {
		t.Fatalf("no fields list was refused: %v", err)
	}
}
