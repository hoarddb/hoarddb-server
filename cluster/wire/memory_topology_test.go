package wire

import (
	"bytes"
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"
)

// TestTopologyReportsMemoryDatabases pins that the committed topology carries each
// database's storage byte: a memory database answers MemoryDatabase and DatabaseInfo.Memory,
// a disk database and an unknown id do not.
func TestTopologyReportsMemoryDatabases(t *testing.T) {
	node := u30iOpen(t, t.TempDir())
	ctx := context.Background()
	cacheID, err := node.CreateDatabaseWithStorage(ctx, "cache", 1, "memory")
	if err != nil {
		t.Fatalf("creating the memory database cache: %v", err)
	}
	plainID, err := node.CreateDatabaseRF(ctx, "plain", 1)
	if err != nil {
		t.Fatalf("creating the disk database plain: %v", err)
	}
	topo := node.topology
	if !topo.MemoryDatabase(cacheID) {
		t.Fatalf("MemoryDatabase(cache) = false, want true")
	}
	if topo.MemoryDatabase(plainID) {
		t.Fatalf("MemoryDatabase(plain) = true, want false")
	}
	if topo.MemoryDatabase(1 << 40) {
		t.Fatalf("MemoryDatabase(unknown id) = true, want false")
	}
	if !topo.AnyMemory() {
		t.Fatalf("AnyMemory() = false, want true")
	}
	cacheInfo, ok := topo.Database(cacheID)
	if !ok || !cacheInfo.Memory {
		t.Fatalf("Database(cache).Memory = %v (found %v), want true", cacheInfo.Memory, ok)
	}
	plainInfo, ok := topo.Database(plainID)
	if !ok || plainInfo.Memory {
		t.Fatalf("Database(plain).Memory = %v (found %v), want false", plainInfo.Memory, ok)
	}
}

// TestCreateDatabaseRFWritesNoStorageElement pins that a create with no storage word
// writes the same body it always did, and that a memory create writes the storage element.
func TestCreateDatabaseRFWritesNoStorageElement(t *testing.T) {
	disk := createDatabaseBody("app", 7, 1, "")
	want := bsoncore.BuildDocument(nil, joinElements(
		bsoncore.AppendStringElement(nil, "name", "app"),
		bsoncore.AppendInt64Element(nil, "id", 7),
		bsoncore.AppendInt32Element(nil, "rf", 1),
		bsoncore.AppendInt64Element(nil, "schema_version", 1),
	))
	if !bytes.Equal(disk, want) {
		t.Fatalf("createDatabaseBody with no storage = %x, want the pre-storage body %x", disk, want)
	}
	memory := createDatabaseBody("app", 7, 1, "memory")
	value, err := bsoncore.Document(memory).LookupErr("storage")
	if err != nil || value.StringValue() != "memory" {
		t.Fatalf("createDatabaseBody with storage memory: storage element = %q (err %v), want memory", value.StringValue(), err)
	}
}
