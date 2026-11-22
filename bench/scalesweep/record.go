package main

import (
	"encoding/hex"
	"math/rand/v2"
)

// ownerFanout is how many records share an owner: the secondary-index read
// returns up to this many rows, like "the items of one user".
const ownerFanout = 10

// record is the one entity every engine stores: a key, an indexed owner, a
// counter and a body of a stated size.
type record struct {
	ID    int64
	Owner int64
	Seq   int64
	Body  string
}

// ownerOf maps a record key to its owner, so each owner holds ownerFanout
// consecutive keys.
func ownerOf(id int64) int64 { return id / ownerFanout }

// bodyFor builds a body of exactly size bytes from the key and a version. The
// bytes come from a seeded generator, hex encoded, so they do not compress to
// nothing (an engine's compression must not be handed free wins) and are the
// same on every engine.
func bodyFor(id, version int64, size int) string {
	if size <= 0 {
		return ""
	}
	gen := rand.New(rand.NewPCG(uint64(id), uint64(version)+1)) //nolint:gosec // deterministic benchmark payload, not a secret
	raw := make([]byte, (size+1)/2)
	for i := range raw {
		raw[i] = byte(gen.Uint32())
	}
	return hex.EncodeToString(raw)[:size]
}

// newRecord is the record stored under id at version 0.
func newRecord(id int64, bodyBytes int) record {
	return record{ID: id, Owner: ownerOf(id), Seq: 0, Body: bodyFor(id, 0, bodyBytes)}
}
