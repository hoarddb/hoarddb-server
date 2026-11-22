package storage

import (
	"bytes"
	"testing"
)

// TestNSExpiryIsInTheNodeBlockAndDistinct (expiry spec §3.1): the expiry queue is
// one node-wide record kind, so its byte lies in the node block and collides with
// no other node-record namespace.
func TestNSExpiryIsInTheNodeBlockAndDistinct(t *testing.T) {
	if NSExpiry != 0x11 {
		t.Fatalf("NSExpiry = %#02x, want 0x11", NSExpiry)
	}
	for _, other := range []byte{NSCatalog, NSReclaim, NSAllocator, NSMaterialize, NSBlobRegistry, NSReplication, NSSeedState} {
		if other == NSExpiry {
			t.Fatalf("NSExpiry collides with node-record namespace %#02x", other)
		}
	}
	key := NodeKey(NSExpiry, []byte{0x01})
	if len(key) != 12 || key[0] != 0xFF || key[1] != 0xFF || key[10] != NSExpiry {
		t.Fatalf("NodeKey(NSExpiry, …) = %x, want [FFFF][00×8][11][01]", key)
	}
	document := ExpiryKey(ExpiryEntry{AtMillis: 5, Slot: 1, DBID: 2, BucketID: 3, Kind: ExpiryKindDocument, EncID: []byte{0x01}})
	if !bytes.HasPrefix(document, ExpiryPrefix()) {
		t.Fatal("an expiry key does not start with ExpiryPrefix")
	}
}

// TestExpiryKeySortsByTimeThenIdentity: the tick seeks to the first key at or
// after now, so keys order by time first, with the sign flip putting negative
// millis before zero and positive millis, then by slot.
func TestExpiryKeySortsByTimeThenIdentity(t *testing.T) {
	entries := []ExpiryEntry{
		{AtMillis: 1700000000000, Slot: 1, DBID: 1, BucketID: 1, Kind: ExpiryKindDocument, EncID: []byte{0x01}},
		{AtMillis: 5, Slot: 9, DBID: 1, BucketID: 1, Kind: ExpiryKindDocument, EncID: []byte{0x01}},
		{AtMillis: 5, Slot: 2, DBID: 1, BucketID: 1, Kind: ExpiryKindDocument, EncID: []byte{0x01}},
		{AtMillis: 0, Slot: 1, DBID: 1, BucketID: 1, Kind: ExpiryKindDocument, EncID: []byte{0x01}},
		{AtMillis: -5, Slot: 1, DBID: 1, BucketID: 1, Kind: ExpiryKindDocument, EncID: []byte{0x01}},
	}
	want := []ExpiryEntry{entries[4], entries[3], entries[2], entries[1], entries[0]}
	for index := 1; index < len(want); index++ {
		before := ExpiryKey(want[index-1])
		after := ExpiryKey(want[index])
		if bytes.Compare(before, after) >= 0 {
			t.Fatalf("key %d (at %d, slot %d) does not sort before key %d (at %d, slot %d): %x vs %x",
				index-1, want[index-1].AtMillis, want[index-1].Slot, index, want[index].AtMillis, want[index].Slot, before, after)
		}
	}
}

// TestExpiryKeyRoundTripsBothKinds: the encID is whatever lies between the fixed
// head and the element tail, so a long id and a short id both parse back exactly.
func TestExpiryKeyRoundTripsBothKinds(t *testing.T) {
	long := bytes.Repeat([]byte{0xAB}, 1024)
	cases := []ExpiryEntry{
		{AtMillis: -42, Slot: 7, DBID: 3, BucketID: 11, Kind: ExpiryKindDocument, EncID: long},
		{AtMillis: 99, Slot: 8191, DBID: 1, BucketID: 2, Kind: ExpiryKindDocument, EncID: []byte{0x07}},
		{AtMillis: 1700000000000, Slot: 12, DBID: 5, BucketID: 6, Kind: ExpiryKindElement, EncID: []byte{0x01, 0x02}, FieldID: 9, Position: 1 << 40},
		{AtMillis: 1700000000000, Slot: 12, DBID: 5, BucketID: 6, Kind: ExpiryKindElement, EncID: long, FieldID: 4, Position: 3},
	}
	for _, want := range cases {
		got, err := ParseExpiryKey(ExpiryKey(want))
		if err != nil {
			t.Fatalf("ParseExpiryKey(kind %#02x, %d-byte id): %v", want.Kind, len(want.EncID), err)
		}
		if got.AtMillis != want.AtMillis || got.Slot != want.Slot || got.DBID != want.DBID ||
			got.BucketID != want.BucketID || got.Kind != want.Kind || !bytes.Equal(got.EncID, want.EncID) {
			t.Errorf("round trip changed the head: got %+v, want %+v", got, want)
		}
		if want.Kind == ExpiryKindElement && (got.FieldID != want.FieldID || got.Position != want.Position) {
			t.Errorf("round trip changed the element tail: got field %d position %d, want field %d position %d",
				got.FieldID, got.Position, want.FieldID, want.Position)
		}
	}
	if _, err := ParseExpiryKey([]byte{0xFF, 0xFF}); err == nil {
		t.Error("ParseExpiryKey accepted a key outside the expiry namespace")
	}
	bogus := ExpiryKey(ExpiryEntry{AtMillis: 1, Slot: 1, DBID: 1, BucketID: 1, Kind: 0x02, EncID: []byte{0x01}})
	if _, err := ParseExpiryKey(bogus); err == nil {
		t.Error("ParseExpiryKey accepted an unknown kind byte")
	}
	short := ExpiryKey(ExpiryEntry{AtMillis: 1, Slot: 1, DBID: 1, BucketID: 1, Kind: ExpiryKindElement, EncID: []byte{0x01}, FieldID: 2, Position: 3})
	if _, err := ParseExpiryKey(short[:len(short)-3]); err == nil {
		t.Error("ParseExpiryKey accepted an element key shorter than its tail")
	}
}
