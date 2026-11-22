package wire

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// The ring store's sync marker as these tests write and read it (storage/vouched_marker.go, format
// 2): two 4 KiB slots, each the magic, the format, a sequence, the version, the log floor and the
// CRC-32C of those 36 bytes, big-endian. Spelled out here so a change of the layout is a change to
// these tests too; reading goes through storage.ReadVouchedMarker, the open's own rule.
const markerSlotSize = 4096

// encodeMarkerSlot returns one whole slot.
func encodeMarkerSlot(format uint32, sequence, version, floor uint64) []byte {
	slot := make([]byte, markerSlotSize)
	copy(slot, "hdbvouch")
	binary.BigEndian.PutUint32(slot[8:], format)
	binary.BigEndian.PutUint64(slot[12:], sequence)
	binary.BigEndian.PutUint64(slot[20:], version)
	binary.BigEndian.PutUint64(slot[28:], floor)
	binary.BigEndian.PutUint32(slot[36:], crc32.Checksum(slot[:36], crc32.MakeTable(crc32.Castagnoli)))
	return slot
}

// writeVouchingMarker writes a ring store's sync marker that vouches version, with log floor 1.
func writeVouchingMarker(t *testing.T, store string, version uint64) {
	t.Helper()
	raw := append(encodeMarkerSlot(2, 2, version, 1), encodeMarkerSlot(2, 1, version, 1)...)
	mustWrite(t, filepath.Join(store, "VOUCHED"), string(raw))
}

// writeFormat1Marker replaces the store's sync marker with the 32-byte record an earlier build
// wrote (format 1: magic, format, version, floor, CRC-32C), as every seed made before the in-place
// marker carries.
func writeFormat1Marker(t *testing.T, store string) {
	t.Helper()
	raw := make([]byte, 32)
	copy(raw, "hdbvouch")
	binary.BigEndian.PutUint32(raw[8:], 1)
	binary.BigEndian.PutUint64(raw[12:], 1)
	binary.BigEndian.PutUint64(raw[20:], 1)
	binary.BigEndian.PutUint32(raw[28:], crc32.Checksum(raw[:28], crc32.MakeTable(crc32.Castagnoli)))
	mustWrite(t, filepath.Join(store, "VOUCHED"), string(raw))
}

// tearBothMarkerSlots flips a byte inside each slot's checksummed bytes: a state no advance leaves.
func tearBothMarkerSlots(t *testing.T, store string) {
	t.Helper()
	path := filepath.Join(store, "VOUCHED")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 2*markerSlotSize {
		t.Fatalf("the marker file is %d bytes, not two slots of %d", len(raw), markerSlotSize)
	}
	raw[20] ^= 0xff
	raw[markerSlotSize+20] ^= 0xff
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

// markerVersionOf reads the version of a marker file's bytes with the open's own rule; ok is false
// when raw is not a marker this build reads.
func markerVersionOf(t *testing.T, raw []byte) (version uint64, ok bool) {
	t.Helper()
	if raw == nil {
		return 0, false
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "VOUCHED"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	version, _, found, err := storage.ReadVouchedMarker(dir)
	return version, found && err == nil
}
