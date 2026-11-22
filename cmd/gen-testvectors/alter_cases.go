package main

import (
	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// alterBucketCases are the six slots of the ALTER_BUCKET (0x8E) vectors that were captured on the
// retired three-node cluster (raise, lower, declare, no change, the in-band refusal, retry later).
// The vectors are gone and the slots stay, so no later case's number, database name or request
// id moves.
func alterBucketCases() []func(db string) exchangeCase { return retiredCases(6) }

// alterMissingCases are the slots after them: the alter of a missing bucket (retired with the
// cluster), the alter on a standalone node, which is still captured (the answer is an in-band
// refusal, not an ERROR: BUG-0685), and a put to a missing bucket on a cluster (retired).
func alterMissingCases() []func(db string) exchangeCase {
	const notes = "The request is database, name and replication_factor: a driver sends no other field. "
	return []func(db string) exchangeCase{
		retiredCase,
		func(db string) exchangeCase {
			return exchangeCase{
				name: "alter bucket: on a standalone node", group: "ddl", storeType: "hash",
				notes: notes + "A standalone node has no cluster to place a second copy on, so it refuses an alter that raises the factor in the answer's own " +
					"body: success false and the message that says to form a cluster, not an ERROR frame. The node's shape is judged before the " +
					"bucket is looked up, so the answer is the same for a bucket that does not exist.",
				setup:       bucketSetup(db, "users", server.BucketTypeHash),
				request:     alterBucket(db, "users", 2),
				wantOpcode:  transport.OpAlterBucket,
				wantField:   map[string]any{"success": false},
				wantMessage: []string{"cannot be raised to replication_factor 2", "running standalone"},
			}
		},
		retiredCase,
	}
}

// alterBucket is the request a driver sends: three fields and no other.
func alterBucket(db, bucket string, factor int32) call {
	return call{op: transport.OpAlterBucket, payload: bson.D{
		{Key: "database", Value: db}, {Key: "name", Value: bucket}, {Key: "replication_factor", Value: factor},
	}}
}
