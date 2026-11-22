package store

import (
	"strings"
	"testing"
)

// TestPayloadFileNameSlotWidth: a 13-bit slot prints as 3 hex digits below
// 4096 and 4 above it; both round-trip, and a 5-digit or non-hex slot is not
// a payload name.
func TestPayloadFileNameSlotWidth(t *testing.T) {
	const blob = "01a10195-469b-70c8-9259-ab06f3d74b94"
	for _, slot := range []uint16{0, 0xABC, 4095, 4096, 4971, 8191} {
		parsed, ok := ParsePayloadFileName(PayloadFileName(slot, 7, blob))
		if !ok || parsed.Slot != slot || parsed.BucketID != 7 || parsed.BlobID != blob {
			t.Errorf("slot %d: parsed %+v, %v", slot, parsed, ok)
		}
	}
	for _, name := range []string{
		"12345-" + "0000000000000007-" + blob,
		"zzz-" + "0000000000000007-" + blob,
		"12-" + "0000000000000007-" + blob,
		"2000-" + "0000000000000007-" + blob, // slot 8192 is not a slot
	} {
		if _, ok := ParsePayloadFileName(name); ok {
			t.Errorf("%q parsed, want refused", name)
		}
	}
	if _, ok := ParsePayloadFileName(strings.ToUpper(PayloadFileName(4971, 7, blob))); ok {
		t.Error("an upper-case name parsed")
	}
}
