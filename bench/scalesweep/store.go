package main

import "context"

// store is one database under test. Every method is one operation a client
// issues; the leg times them, so an implementation does no extra work around
// the call.
type store interface {
	// name is the engine label recorded with every leg, taken from what the
	// implementation really is, never from a flag.
	name() string
	// durability states the acknowledgement semantics, recorded with the leg.
	durability() string
	// prepare creates the table or collection and its index when missing.
	prepare(ctx context.Context) error
	// nextKey is the highest stored key plus one (0 when empty): where the next
	// load starts, which stays right even if a failed insert left a gap.
	nextKey(ctx context.Context) (int64, error)
	// loadRange inserts the records with keys from (inclusive) to to (exclusive).
	loadRange(ctx context.Context, from, to int64, bodyBytes int) error
	// get reads one record by key; a missing key is an error.
	get(ctx context.Context, id int64) error
	// update rewrites the body and counter of an existing key.
	update(ctx context.Context, id, version int64, bodyBytes int) error
	// insert adds a record under a new key.
	insert(ctx context.Context, id int64, bodyBytes int) error
	// byOwner reads the records of one owner through the secondary index and
	// returns how many it found.
	byOwner(ctx context.Context, owner int64) (int, error)
	close()
}
