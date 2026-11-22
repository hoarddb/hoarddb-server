package storage

import "github.com/cespare/xxhash/v2"

// LegacyDatabaseID is the database id the old name-keyed server passes to Key,
// Scope and PairPrefix until it has real database ids: the xxhash64 of the
// database name, never 0. Bucket-level placement in the old server is
// SlotOf(PlacementKey(database, bucket, nil)): an empty encodedID means the
// bucket is placed as a whole, with no document in the key.
//
// DELETE with the units that introduce real database ids (U38/U39).
func LegacyDatabaseID(database string) uint64 {
	id := xxhash.Sum64String(database)
	if id == 0 {
		return 1
	}
	return id
}
