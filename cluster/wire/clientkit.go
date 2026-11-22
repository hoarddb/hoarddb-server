package wire

import (
	"context"
	"fmt"
)

// This file is the in-process side of the discovery a ring client needs before its first
// RING_KEYED frame. A remote client learns the same values from RING_ADMIN (create_bucket and
// describe_bucket reply with them; entry-ring-admin-wire.md), so nothing on the wire depends
// on this file. It is for a caller that already holds the Node.

// ClientBucket is what a client puts in a RING_KEYED header for one bucket.
type ClientBucket struct {
	// BucketID is the bucket's id.
	BucketID uint64
	// DeclVersion is the declaration version the client was built against.
	DeclVersion int64
	// SchemaVersionSeen is the schema_version_seen the header carries.
	SchemaVersionSeen int64
	// FieldIDs maps each declared field name to its id on the wire.
	FieldIDs map[string]uint16
}

// ClientBucket resolves database db's bucket name to the header values a client sends.
func (n *Node) ClientBucket(ctx context.Context, db, name string) (ClientBucket, error) {
	ref, err := n.bucket(ctx, db, name)
	if err != nil {
		return ClientBucket{}, fmt.Errorf("resolving bucket %q in database %q for a client: %w", name, db, err)
	}
	fields := make(map[string]uint16, len(ref.table.Entries))
	for _, entry := range ref.table.Entries {
		fields[entry.Name] = entry.ID
	}
	return ClientBucket{
		BucketID: ref.entry.Decl.BucketID, DeclVersion: ref.version, SchemaVersionSeen: ref.entry.Seen, FieldIDs: fields,
	}, nil
}

// ElementArg is the arg of a push to a fifoStore or lifoStore: element is one BSON document.
func ElementArg(element []byte) ([]byte, error) { return encodeElementArg(element) }

// PutArg is the arg of a put into a hashStore or btreeStore.
func PutArg(key string, element []byte) ([]byte, error) { return encodePutArg(key, element) }

// TakeKeyArg is the arg of a take from a hashStore or btreeStore.
func TakeKeyArg(key string) ([]byte, error) { return encodeTakeKeyArg(key) }

// HeapPushArg is the arg of a push to a heapStore: a priority (any int64 except the lowest),
// an entry id of 1 to 1024 bytes that names the element for later reads, and the element.
func HeapPushArg(priority int64, entryID string, element []byte) ([]byte, error) {
	return encodeHeapPushArg(priority, entryID, element)
}

// PopArg is the arg of a pop that takes up to n elements, n from 1 to 100. A pop with no
// arg takes one.
func PopArg(n int) ([]byte, error) { return encodePopArg(n) }
