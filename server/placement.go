package server

import (
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// placementKey is what the ring places a bucket by: the frozen encoding of
// (database, bucket) from storage.PlacementKey, so two same-named buckets in
// different databases land independently. It is a struct rather
// than a named string so that a bare bucket name — how placement was keyed
// before, and how the defect survived — cannot be passed where a key is
// wanted, not even as an untyped string constant.
type placementKey struct {
	encoded string
}

// placementOf is the placement key of a user bucket.
func placementOf(database, bucket string) placementKey {
	return placementKey{encoded: string(storage.PlacementKey(database, bucket, nil))}
}

// reservedPlacement is the placement key of a cluster-internal record that is
// not a user bucket, placed by its name as it always was (the auth master's
// authReservedKey), so the auth master did not move when placement became per-database. It
// cannot equal a user bucket's key: that key opens with uvarint(len(database)),
// so one opening with '_' (0x5F) declares a 95-byte database and is at least
// 97 bytes long — never the 12 bytes of "_system.auth".
func reservedPlacement(name string) placementKey {
	return placementKey{encoded: name}
}

// payloadPlacement is the placement key of a replicated entry or a WAL record,
// which name their bucket but carry the database only inside the request
// payload they replay — the per-database derivation. It decodes the payload, so a
// caller that already holds the database uses placementOf instead.
func payloadPlacement(op transport.Opcode, bucket string, payload []byte) placementKey {
	_, database := decodeFrameMeta(payload).scope(op)
	return placementOf(database, bucket)
}

// entryPlacement is payloadPlacement for a ReplEntry.
func entryPlacement(entry ReplEntry) placementKey {
	return payloadPlacement(transport.Opcode(entry.Op), entry.Bucket, entry.Payload)
}
