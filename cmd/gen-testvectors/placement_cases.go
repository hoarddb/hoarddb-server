package main

import (
	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// placementCases are the create answers that carry a hash bucket's recorded
// placement. They sit last in the list so that adding them renumbers no
// existing case's database.
func placementCases() []func(db string) exchangeCase {
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create bucket records placement whole by default", group: "ddl", storeType: "hash",
				notes: "A hash bucket created with no placement is recorded whole for now (defaultHashPlacement; " +
					"spread again once the spread collection work completes); the answer names it. " +
					"Data is still stored whole until spreading ships.",
				setup:      []call{createDB(db)},
				request:    createBucket(db, "b", server.BucketTypeHash),
				wantOpcode: transport.OpCreateBucket,
				wantField:  map[string]any{"success": true, "placement": "whole"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create bucket with placement whole", group: "ddl", storeType: "hash",
				setup: []call{createDB(db)},
				request: call{op: transport.OpCreateBucket, payload: server.CreateBucketRequest{
					Database: db, Name: "b", Type: server.BucketTypeHash, Options: map[string]any{"placement": "whole"},
				}},
				wantOpcode: transport.OpCreateBucket,
				wantField:  map[string]any{"success": true, "placement": "whole"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create bucket with placement spread on a fifo bucket", group: "ddl", storeType: "fifo",
				notes: "Only hash buckets spread; the refusal names the type.",
				setup: []call{createDB(db)},
				request: call{op: transport.OpCreateBucket, payload: server.CreateBucketRequest{
					Database: db, Name: "b", Type: server.BucketTypeFIFO, Options: map[string]any{"placement": "spread"},
				}},
				wantOpcode:  transport.OpCreateBucket,
				wantField:   map[string]any{"success": false},
				wantMessage: []string{"fifo", "only hash buckets spread"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create bucket with a unique index on a spread bucket", group: "ddl", storeType: "hash",
				notes: "A unique field other than _id_ cannot be checked across the parts of a spread bucket; the refusal " +
					"names the placement: whole create that works.",
				setup: []call{createDB(db)},
				request: call{op: transport.OpCreateBucket, payload: server.CreateBucketRequest{
					Database: db, Name: "b", Type: server.BucketTypeHash,
					Options: map[string]any{"placement": "spread",
						"indices": []any{map[string]any{"fields": []any{"Email"}, "unique": true}}},
				}},
				wantOpcode:  transport.OpCreateBucket,
				wantField:   map[string]any{"success": false},
				wantMessage: []string{"Cannot declare Email unique", "placement: whole"},
			}
		},
	}
}

// placementDefaultCases are the create answers added with the default of whole
// (spread MVP step 1b). They are appended after every other case, so that
// adding them renumbers none of them.
func placementDefaultCases() []func(db string) exchangeCase {
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create bucket with placement spread", group: "ddl", storeType: "hash",
				notes: "Spread is opt-in while the default is whole.",
				setup: []call{createDB(db)},
				request: call{op: transport.OpCreateBucket, payload: server.CreateBucketRequest{
					Database: db, Name: "b", Type: server.BucketTypeHash, Options: map[string]any{"placement": "spread"},
				}},
				wantOpcode: transport.OpCreateBucket,
				wantField:  map[string]any{"success": true, "placement": "spread"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "create bucket with a unique index and no placement", group: "ddl", storeType: "hash",
				notes: "With no placement the bucket is whole, so a unique field other than _id_ is accepted.",
				setup: []call{createDB(db)},
				request: call{op: transport.OpCreateBucket, payload: server.CreateBucketRequest{
					Database: db, Name: "b", Type: server.BucketTypeHash,
					Options: map[string]any{"indices": []any{map[string]any{"fields": []any{"Email"}, "unique": true}}},
				}},
				wantOpcode: transport.OpCreateBucket,
				wantField:  map[string]any{"success": true, "placement": "whole"},
			}
		},
	}
}
