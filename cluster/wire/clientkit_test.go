package wire

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"
)

func TestU74ClientBucketCarriesTheHeaderValuesTheHandlerChecks(t *testing.T) {
	node, _ := u63Fresh(t)
	got, err := node.ClientBucket(context.Background(), u30iDB, "Box")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := node.bucket(context.Background(), u30iDB, "Box")
	if err != nil {
		t.Fatal(err)
	}
	if got.BucketID != ref.entry.Decl.BucketID || got.DeclVersion != ref.version || got.SchemaVersionSeen != ref.entry.Seen {
		t.Fatalf("header values %+v, want %d/%d/%d", got, ref.entry.Decl.BucketID, ref.version, ref.entry.Seen)
	}
	for _, name := range []string{"Name", "Queue", "Table"} {
		entry, _ := entryNamed(ref.table, name)
		if got.FieldIDs[name] != entry.ID || entry.ID == 0 {
			t.Fatalf("field %s id %d, want %d", name, got.FieldIDs[name], entry.ID)
		}
	}
	if _, err := node.ClientBucket(context.Background(), u30iDB, "Nope"); err == nil {
		t.Fatal("a bucket that does not exist resolved")
	}
}

func TestHeapPushArgEqualsTheInternalEncoder(t *testing.T) {
	element := u66Item(t, "heap-e1")
	for _, priority := range []int64{-7, 0, 5} {
		want, err := encodeHeapPushArg(priority, "e1", element)
		if err != nil {
			t.Fatal(err)
		}
		got, err := HeapPushArg(priority, "e1", element)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("HeapPushArg(%d) = %x, %v; the internal encoder gives %x", priority, got, err, want)
		}
	}
	if _, err := HeapPushArg(1, "", element); err == nil {
		t.Fatal("an empty entry id must be refused exactly as the internal encoder refuses it")
	}
}

// TestHeapPushArgFrozenBytes pins one arg byte for byte, independent of the encoder it wraps.
// Decoded by hand: length 39; int64 "p" = 5; string "e" = "e1"; binary "v" = the 5 bytes
// of the empty document 05 00 00 00 00; terminator.
func TestHeapPushArgFrozenBytes(t *testing.T) {
	const want = "270000001270000500000000000000026500030000006531000576000500000000050000000000"
	got, err := HeapPushArg(5, "e1", []byte{5, 0, 0, 0, 0})
	if err != nil || hex.EncodeToString(got) != want {
		t.Fatalf("HeapPushArg(5, e1, empty doc) = %x, %v; frozen bytes are %s", got, err, want)
	}
}
