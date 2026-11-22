package storage

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cespare/xxhash/v2"
)

// frozenSlotRow is one row of testdata/slot-vectors.json, the conventions §3
// table written out. The rows come from the spec's byte layout, never from the
// function under test.
type frozenSlotRow struct {
	Database        string `json:"database"`
	Bucket          string `json:"bucket"`
	EncIDHex        string `json:"encid_hex"`
	PlacementKeyHex string `json:"placement_key_hex"`
	Slot            uint16 `json:"slot"`
}

func readFrozenSlotRows(t *testing.T) []frozenSlotRow {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "slot-vectors.json"))
	if err != nil {
		t.Fatalf("reading the slot vectors: %v", err)
	}
	var file struct {
		Vectors []frozenSlotRow `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("decoding the slot vectors: %v", err)
	}
	if len(file.Vectors) < 6 {
		t.Fatalf("the slot vector file holds %d rows, want the six conventions §3 rows", len(file.Vectors))
	}
	return file.Vectors
}

// TestKeyLayoutIsExact pins conventions §1 and placement §3 byte for byte: the
// database id comes before the bucket id, so drop database is one range per
// slot, and every document key is the 19-byte prefix plus its body.
func TestKeyLayoutIsExact(t *testing.T) {
	encID := []byte{0x50, 0x70, 0x31, 0x00, 0x00}

	// Placement §3's worked example: database 1, bucket 41, _id_ "p1", slot 0x1362.
	header := Key(0x1362, 1, 41, NSDocuments, encID)
	wantHeader := []byte{
		0x13, 0x62,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x29,
		0x01,
		0x50, 0x70, 0x31, 0x00, 0x00,
	}
	if !bytes.Equal(header, wantHeader) {
		t.Fatalf("Key(0x1362, 1, 41, NSDocuments, p1) = %x, want %x", header, wantHeader)
	}
	if len(Key(0x1362, 1, 41, NSDocuments)) != 19 {
		t.Fatalf("a data key with no body is %d bytes, want the 19-byte prefix", len(Key(0x1362, 1, 41, NSDocuments)))
	}

	field := Key(0x1362, 1, 41, NSDocuments, encID, []byte{0x00, 0x01})
	if len(field) != 26 || !bytes.Equal(field[19:], []byte{0x50, 0x70, 0x31, 0x00, 0x00, 0x00, 0x01}) {
		t.Fatalf("field 1 key = %x, want the header prefix, then encID, then fieldID 0x0001 (26 bytes)", field)
	}

	scope := Scope(0x1362, 1, 41)
	if !bytes.Equal(scope, wantHeader[:18]) {
		t.Fatalf("Scope(0x1362, 1, 41) = %x, want the first 18 bytes of the header key", scope)
	}
	if pair := PairPrefix(0x1362, 1); !bytes.Equal(pair, wantHeader[:10]) {
		t.Fatalf("PairPrefix(0x1362, 1) = %x, want [slot][database id], 10 bytes", pair)
	}

	node := NodeKey(NSCatalog, []byte("x"))
	wantNode := []byte{0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, NSCatalog, 'x'}
	if !bytes.Equal(node, wantNode) {
		t.Fatalf("NodeKey(NSCatalog, x) = %x, want %x: [0xFFFF][0 x 8][namespace] then the body", node, wantNode)
	}

	placement := PlacementKey("app", "Person", encID)
	wantPlacement, err := hex.DecodeString("0361707006506572736f6e5070310000")
	if err != nil {
		t.Fatalf("decoding the reference placement key: %v", err)
	}
	if !bytes.Equal(placement, wantPlacement) {
		t.Fatalf("PlacementKey(app, Person, p1) = %x, want %x", placement, wantPlacement)
	}
}

// TestSlotVectorsAreFrozen replays every row of testdata/slot-vectors.json:
// the placement key bytes must match, and SlotOf of those bytes must be the
// slot the spec computed. A changed row moves every document it covers.
func TestSlotVectorsAreFrozen(t *testing.T) {
	for _, row := range readFrozenSlotRows(t) {
		label := fmt.Sprintf("(%q, %q, %s)", row.Database, row.Bucket, row.EncIDHex)
		encID, err := hex.DecodeString(row.EncIDHex)
		if err != nil {
			t.Fatalf("%s: decoding encid_hex: %v", label, err)
		}
		key := PlacementKey(row.Database, row.Bucket, encID)
		if got := hex.EncodeToString(key); got != row.PlacementKeyHex {
			t.Errorf("%s: PlacementKey = %s, want %s", label, got, row.PlacementKeyHex)
		}
		if got := SlotOf(key); got != row.Slot {
			t.Errorf("%s: SlotOf = %d, want %d", label, got, row.Slot)
		}
	}
}

// TestSlotIsTopThirteenBits holds the slot to the top 13 bits of xxhash64 over
// a sweep of placement keys: the slot stays below SlotCount, its first byte
// stays below Badger's reserved 0x21, and the sweep reaches the upper half of
// the range, so the slot really is 13 bits and not 12.
func TestSlotIsTopThirteenBits(t *testing.T) {
	if SlotCount != 8192 {
		t.Fatalf("SlotCount = %d, want 8192 (13 bits)", SlotCount)
	}
	encID := []byte{0x10}
	sawUpperHalf := false
	for index := 0; index < 20000; index++ {
		key := PlacementKey("app", fmt.Sprintf("b%d", index), encID)
		got := SlotOf(key)
		want := uint16(xxhash.Sum64(key) >> 51)
		if got != want {
			t.Fatalf("SlotOf(%x) = %d, want the top 13 bits of xxhash64, %d", key, got, want)
		}
		if got >= SlotCount {
			t.Fatalf("SlotOf(%x) = %d, not below SlotCount %d", key, got, SlotCount)
		}
		if got>>8 >= 0x21 {
			t.Fatalf("SlotOf(%x) = %#x: the first byte reaches Badger's reserved 0x21", key, got)
		}
		if got >= 4096 {
			sawUpperHalf = true
		}
	}
	if !sawUpperHalf {
		t.Fatalf("no slot in 20000 keys reached 4096, so the slot is not using 13 bits")
	}
}

// TestNodeRecordSlotIsOutsideEveryRange keeps the node block out of every
// document range: NodeRecordSlot is 0xFFFF, no slot a placement key can hash
// to reaches it, and the seed-state namespace sits in that block.
func TestNodeRecordSlotIsOutsideEveryRange(t *testing.T) {
	if NodeRecordSlot != 0xFFFF {
		t.Fatalf("NodeRecordSlot = %#x, want 0xFFFF", NodeRecordSlot)
	}
	if NodeRecordSlot < SlotCount {
		t.Fatalf("NodeRecordSlot %#x is inside the slot range [0, %d): it would read as a real slot", NodeRecordSlot, SlotCount)
	}
	for index := 0; index < 20000; index++ {
		key := PlacementKey("app", fmt.Sprintf("n%d", index), []byte{0x10})
		if SlotOf(key) == NodeRecordSlot {
			t.Fatalf("placement key %x hashed to the node-record slot", key)
		}
	}
	if NSSeedState != 0x0F {
		t.Fatalf("NSSeedState = %#x, want 0x0F (conventions §1)", NSSeedState)
	}
	if node := NodeKey(NSSeedState, []byte{0x01}); !bytes.HasPrefix(node, []byte{0xFF, 0xFF}) {
		t.Fatalf("NodeKey(NSSeedState) = %x does not start in the node block", node)
	}
}
