package server

import (
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// TestBtreeListingReturnsKeysAsWritten is BUG-0613: a string key put and read
// back by GET was listed by BTREE_RANGE, BTREE_FIRST and BTREE_LAST as
// "0x..." hex whenever it held a NUL byte or any other non-printable
// character. The owner ruled (2026-10-18) that a listing returns the key
// exactly as it was written.
func TestBtreeListingReturnsKeysAsWritten(t *testing.T) {
	s := newTestServer(t)
	createTestBucketWithOptions(t, s, "app", "ordered", BucketTypeBTree, nil)

	keys := []string{
		"a\x00",
		"bell\atab\tline\nreturn\r",
		"esc\x1b[0m",
		"del\x7f",
		"n\x00x",
		"zero\u200bwidth",
		"ünï",
		"0x6e0078",
	}
	for _, key := range keys {
		resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "ordered", Key: key, Data: map[string]interface{}{"a": int32(1)}})
		if err != nil || !resp.Success {
			t.Fatalf("Insert(%q) = %+v, %v", key, resp, err)
		}
	}

	listed, err := s.RangeQueryCtx(context.Background(), BTreeRangeRequest{Database: "app", Bucket: "ordered"})
	if err != nil {
		t.Fatalf("RangeQueryCtx: %v", err)
	}
	listedKeys := make(map[string]bool, len(listed.Entries))
	for _, entry := range listed.Entries {
		listedKeys[entry.Key] = true
	}
	for _, key := range keys {
		found, err := s.Find(FindRequest{Database: "app", Bucket: "ordered", Key: key})
		if err != nil || !found.Found {
			t.Fatalf("Find(%q) = %+v, %v: the point read must find what was put", key, found, err)
		}
		if !listedKeys[key] {
			t.Errorf("BTREE_RANGE did not list the key %q as written; it listed %d keys: %q", key, len(listed.Entries), listed.Entries)
		}
	}
	if len(listed.Entries) != len(keys) {
		t.Errorf("BTREE_RANGE listed %d entries, want %d", len(listed.Entries), len(keys))
	}

	for _, edge := range []struct {
		name string
		last bool
		want string
	}{{"BTREE_FIRST", false, "0x6e0078"}, {"BTREE_LAST", true, "ünï"}} {
		resp, err := s.Edge("app", "ordered", edge.last)
		if err != nil || len(resp.Entries) != 1 || resp.Entries[0].Key != edge.want {
			t.Errorf("%s = %+v, %v; want the key %q", edge.name, resp, err, edge.want)
		}
	}
}

// TestBtreeEntryKeyWithNulSurvivesBSON proves the wire half of BUG-0613: a
// BSON string carries a NUL byte, because it is length-prefixed.
func TestBtreeEntryKeyWithNulSurvivesBSON(t *testing.T) {
	encoded, err := bson.Marshal(BTreeEntry{Key: "n\x00x"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded BTreeEntry
	if err := bson.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Key != "n\x00x" {
		t.Errorf("decoded key = %q, want %q", decoded.Key, "n\x00x")
	}
}

// TestDisplayKeyKeepsHexForBytesThatAreNotUTF8 pins the one case that still
// prints as hex: a key that is not valid UTF-8 cannot travel as a BSON
// string, so it keeps its documented "0x..." form.
func TestDisplayKeyKeepsHexForBytesThatAreNotUTF8(t *testing.T) {
	cases := []struct {
		name string
		key  []byte
		want string
	}{
		{"invalid UTF-8", []byte{0xff, 0xfe, 0x00}, "0xfffe00"},
		{"a NUL byte", []byte("n\x00x"), "n\x00x"},
		{"a control byte", []byte("a\x1bb"), "a\x1bb"},
		{"plain text", []byte("ada"), "ada"},
		{"empty", []byte{}, ""},
	}
	for _, tc := range cases {
		if got := displayKey(tc.key); got != tc.want {
			t.Errorf("%s: displayKey(%q) = %q, want %q", tc.name, tc.key, got, tc.want)
		}
	}
}
