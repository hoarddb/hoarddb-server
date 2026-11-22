package store

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// fieldVectorFile is store/testdata/fieldstore-vectors.json. Its hex was
// written by hand from the grammar, so the test compares the code with the
// spec rather than with itself.
type fieldVectorFile struct {
	Slot     uint16 `json:"slot"`
	BucketID uint64 `json:"bucket_id"`
	Owners   []struct {
		DocumentKey string `json:"document_key"`
		Hex         string `json:"hex"`
	} `json:"owners"`
	StorePrefixes []struct {
		DocumentKey string `json:"document_key"`
		FieldID     uint16 `json:"field_id"`
		Hex         string `json:"hex"`
	} `json:"store_prefixes"`
	FifoElements []struct {
		DocumentKey string `json:"document_key"`
		FieldID     uint16 `json:"field_id"`
		Position    uint64 `json:"position"`
		Hex         string `json:"hex"`
	} `json:"fifo_elements"`
	HeapElements []struct {
		DocumentKey string `json:"document_key"`
		FieldID     uint16 `json:"field_id"`
		Priority    string `json:"priority"`
		EntryID     string `json:"entry_id"`
		Hex         string `json:"hex"`
	} `json:"heap_elements"`
	KeyedElements []struct {
		DocumentKey string `json:"document_key"`
		FieldID     uint16 `json:"field_id"`
		ElementKey  string `json:"element_key"`
		Hex         string `json:"hex"`
	} `json:"keyed_elements"`
	StateKeys []struct {
		DocumentKey string `json:"document_key"`
		FieldID     uint16 `json:"field_id"`
		Hex         string `json:"hex"`
	} `json:"state_keys"`
	StateValues []struct {
		Head, Tail, Next, Count uint64
		Hex                     string `json:"hex"`
	} `json:"state_values"`
	L3Elements []struct {
		DocumentKey string `json:"document_key"`
		L2FieldID   uint16 `json:"l2_field_id"`
		L2Position  uint64 `json:"l2_position"`
		L3FieldID   uint16 `json:"l3_field_id"`
		Priority    string `json:"priority"`
		EntryID     string `json:"entry_id"`
		Hex         string `json:"hex"`
	} `json:"l3_elements"`
}

func loadFieldVectors(t *testing.T) fieldVectorFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "fieldstore-vectors.json"))
	if err != nil {
		t.Fatalf("reading the field-store vectors: %v", err)
	}
	var vectors fieldVectorFile
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatalf("decoding the field-store vectors: %v", err)
	}
	return vectors
}

func mustHex(t *testing.T, encoded string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		t.Fatalf("vector %q is not hex: %v", encoded, err)
	}
	return decoded
}

func mustPriority(t *testing.T, text string) int64 {
	t.Helper()
	priority, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		t.Fatalf("vector priority %q: %v", text, err)
	}
	return priority
}

func mustOwner(t *testing.T, slot uint16, id uint64, documentKey string) []byte {
	t.Helper()
	owner, err := FieldOwnerPrefix(slot, 1, id, documentKey)
	if err != nil {
		t.Fatalf("FieldOwnerPrefix(%q): %v", documentKey, err)
	}
	return owner
}

func assertVector(t *testing.T, name string, got, want []byte) {
	t.Helper()
	if !bytes.Equal(got, want) {
		t.Errorf("%s = %x, want %x", name, got, want)
	}
}

// TestFieldStoreKeyEncodingFrozen is spec T-01.
func TestFieldStoreKeyEncodingFrozen(t *testing.T) {
	vectors := loadFieldVectors(t)
	s, id := vectors.Slot, vectors.BucketID
	bucketFieldPrefix := storage.Key(s, 1, id, storage.NSFieldStore)

	for _, owner := range vectors.Owners {
		got := mustOwner(t, s, id, owner.DocumentKey)
		assertVector(t, "owner "+strconv.Quote(owner.DocumentKey), got, mustHex(t, owner.Hex))
		if !bytes.HasPrefix(got, bucketFieldPrefix) {
			t.Errorf("owner %q = %x does not begin with the bucket's field-store prefix %x", owner.DocumentKey, got, bucketFieldPrefix)
			continue
		}
		component := got[len(bucketFieldPrefix):]
		if len(component) < 3 || component[0] != 0x50 || !bytes.HasSuffix(component, []byte{0x00, 0x00}) {
			t.Errorf("owner %q = %x: want 0x50 ‖ key ‖ 0x00 0x00", owner.DocumentKey, component)
		}
		if bytes.IndexByte([]byte(owner.DocumentKey), 0x00) >= 0 && !bytes.Contains(component, []byte{0x00, 0xFF}) {
			t.Errorf("owner %q = %x: a 0x00 inside the key must be written 0x00 0xFF", owner.DocumentKey, component)
		}
	}
	for _, prefix := range vectors.StorePrefixes {
		got := FieldStorePrefix(mustOwner(t, s, id, prefix.DocumentKey), prefix.FieldID)
		assertVector(t, "store prefix of field "+strconv.Itoa(int(prefix.FieldID)), got, mustHex(t, prefix.Hex))
	}
	for _, element := range vectors.FifoElements {
		storePrefix := FieldStorePrefix(mustOwner(t, s, id, element.DocumentKey), element.FieldID)
		got := FieldElementKey(storePrefix, storage.NSQueue, PositionRemainder(element.Position))
		assertVector(t, "fifo element at "+strconv.FormatUint(element.Position, 10), got, mustHex(t, element.Hex))
	}
	var heapKeys [][]byte
	for _, element := range vectors.HeapElements {
		storePrefix := FieldStorePrefix(mustOwner(t, s, id, element.DocumentKey), element.FieldID)
		remainder, err := HeapRemainder(mustPriority(t, element.Priority), element.EntryID)
		if err != nil {
			t.Fatalf("HeapRemainder(%s): %v", element.Priority, err)
		}
		got := FieldElementKey(storePrefix, storage.NSQueue, remainder)
		assertVector(t, "heap element at priority "+element.Priority, got, mustHex(t, element.Hex))
		heapKeys = append(heapKeys, got)
	}
	// The vectors are listed lowest priority first; the key order must be
	// highest first, so a forward seek pops the highest.
	for i := 1; i < len(heapKeys); i++ {
		if bytes.Compare(heapKeys[i], heapKeys[i-1]) >= 0 {
			t.Errorf("heap key for priority %s does not sort before priority %s's", vectors.HeapElements[i].Priority, vectors.HeapElements[i-1].Priority)
		}
	}
	for _, element := range vectors.KeyedElements {
		storePrefix := FieldStorePrefix(mustOwner(t, s, id, element.DocumentKey), element.FieldID)
		remainder, err := KeyedRemainder(element.ElementKey)
		if err != nil {
			t.Fatalf("KeyedRemainder(%q): %v", element.ElementKey, err)
		}
		assertVector(t, "keyed element "+strconv.Quote(element.ElementKey), FieldElementKey(storePrefix, storage.NSDocuments, remainder), mustHex(t, element.Hex))
	}
	for _, state := range vectors.StateKeys {
		got := FieldStateKey(FieldStorePrefix(mustOwner(t, s, id, state.DocumentKey), state.FieldID))
		assertVector(t, "state key", got, mustHex(t, state.Hex))
	}
	for _, state := range vectors.StateValues {
		want := FieldInstanceState{Head: state.Head, Tail: state.Tail, Next: state.Next, Count: state.Count}
		assertVector(t, "state value", EncodeFieldInstanceState(want), mustHex(t, state.Hex))
	}
	for _, element := range vectors.L3Elements {
		l2Store := FieldStorePrefix(mustOwner(t, s, id, element.DocumentKey), element.L2FieldID)
		anchor := FieldElementAnchor(l2Store, PositionRemainder(element.L2Position))
		remainder, err := HeapRemainder(mustPriority(t, element.Priority), element.EntryID)
		if err != nil {
			t.Fatalf("HeapRemainder(%s): %v", element.Priority, err)
		}
		got := FieldElementKey(FieldStorePrefix(anchor, element.L3FieldID), storage.NSQueue, remainder)
		assertVector(t, "L3 element", got, mustHex(t, element.Hex))
	}
}

// randomFieldKey builds one field-store key of any shape the grammar allows.
func randomFieldKey(t *testing.T, rng *rand.Rand, slot uint16, id uint64) []byte {
	t.Helper()
	documentKey := make([]byte, rng.Intn(12))
	rng.Read(documentKey)
	owner := mustOwner(t, slot, id, string(documentKey))
	storePrefix := FieldStorePrefix(owner, uint16(rng.Intn(1<<16)))
	remainder := make([]byte, rng.Intn(20))
	rng.Read(remainder)
	switch rng.Intn(4) {
	case 0:
		return FieldStateKey(storePrefix)
	case 1:
		return FieldElementKey(storePrefix, storage.NSQueue, PositionRemainder(rng.Uint64()))
	case 2:
		return FieldElementKey(FieldStorePrefix(FieldElementAnchor(storePrefix, remainder), 1), storage.NSDocuments, remainder)
	default:
		return FieldElementKey(storePrefix, storage.NSDocuments, remainder)
	}
}

func TestFieldKeysStayInsideTheBucketRange(t *testing.T) {
	rng := rand.New(rand.NewSource(22))
	for _, bucket := range []struct {
		slot uint16
		id   uint64
	}{{0, 1}, {0x0ABC, 0x0102}, {storage.SlotCount - 1, storage.MaxBucketID - 1}} {
		bucketLow, bucketHigh := storage.Scope(bucket.slot, 1, bucket.id), storage.Scope(bucket.slot, 1, bucket.id+1)
		fieldLow, fieldHigh := storage.Key(bucket.slot, 1, bucket.id, storage.NSFieldStore), storage.Key(bucket.slot, 1, bucket.id, storage.NSFieldStore+1)
		for range 2000 {
			key := randomFieldKey(t, rng, bucket.slot, bucket.id)
			if bytes.Compare(key, bucketLow) < 0 || bytes.Compare(key, bucketHigh) >= 0 {
				t.Fatalf("key %x lies outside bucket (%d, %d)'s range [%x, %x)", key, bucket.slot, bucket.id, bucketLow, bucketHigh)
			}
			if bytes.Compare(key, fieldLow) < 0 || bytes.Compare(key, fieldHigh) >= 0 {
				t.Fatalf("key %x lies outside the bucket's field-store namespace [%x, %x)", key, fieldLow, fieldHigh)
			}
		}
	}
}

func TestFieldOwnersNeverPrefixEachOther(t *testing.T) {
	documentKeys := []string{"a", "a\x00", "ab", "a\x00b", "", "\x00"}
	owners := make([][]byte, len(documentKeys))
	for i, documentKey := range documentKeys {
		owners[i] = mustOwner(t, 3, 9, documentKey)
	}
	for i := range owners {
		for j := range owners {
			if i != j && bytes.HasPrefix(owners[j], owners[i]) {
				t.Errorf("the owner of %q (%x) is a prefix of the owner of %q (%x): a prefix walk of one document reaches another's stores",
					documentKeys[i], owners[i], documentKeys[j], owners[j])
			}
		}
	}
	// An owner's keys sort with its document: two owners' ranges are
	// disjoint, so ordering the owners orders their whole footprints.
	sorted := append([][]byte(nil), owners...)
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i], sorted[j]) < 0 })
	for i := 1; i < len(sorted); i++ {
		if bytes.Compare(prefixEnd(sorted[i-1]), sorted[i]) > 0 {
			t.Errorf("owner %x's range overlaps owner %x's", sorted[i-1], sorted[i])
		}
	}
}

func TestFieldInstanceStateRoundTrip(t *testing.T) {
	state := FieldInstanceState{Head: 0x0102030405060708, Tail: 2, Next: 3, Count: 1 << 40}
	encoded := EncodeFieldInstanceState(state)
	want := mustHex(t, "0102030405060708"+"0000000000000002"+"0000000000000003"+"0000010000000000")
	if !bytes.Equal(encoded, want) {
		t.Fatalf("EncodeFieldInstanceState = %x, want %x: Head, Tail, Next, Count, big-endian", encoded, want)
	}
	decoded, err := DecodeFieldInstanceState([]byte("k"), encoded)
	if err != nil || decoded != state {
		t.Fatalf("DecodeFieldInstanceState = %+v, %v, want %+v", decoded, err, state)
	}
	for _, size := range []int{31, 33, 0} {
		stateKey := []byte{0xAB, 0xCD}
		_, err := DecodeFieldInstanceState(stateKey, make([]byte, size))
		if err == nil {
			t.Errorf("a %d-byte state decoded without an error", size)
			continue
		}
		// The key is an internal encoding and belongs in the node log
		// (BUG-0607), never in the text a client could see.
		if bytes.Contains([]byte(err.Error()), []byte("abcd")) {
			t.Errorf("the %d-byte decode error %q names the key abcd", size, err)
		}
		var keyErr *storage.KeyError
		if !errors.As(err, &keyErr) || !bytes.Equal(keyErr.Key, stateKey) {
			t.Errorf("the %d-byte decode error %q does not carry the key for the node log", size, err)
		}
	}
}
