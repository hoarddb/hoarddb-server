package main

import (
	"strings"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// searchTruncationCases pin has_more on a SEARCH answer (BUG-0517): total is
// the number returned, and has_more says a further match exists beyond limit.
// They come last so every earlier exchange keeps its number, database name and
// request ID.
func searchTruncationCases() []func(db string) exchangeCase {
	computingPeople := func(db string, indexed bool) []call {
		options := map[string]any{}
		if indexed {
			options["indices"] = []any{"Field"}
		}
		setup := []call{createDB(db), {op: transport.OpCreateBucket, payload: server.CreateBucketRequest{
			Database: db, Name: "people", Type: server.BucketTypeHash, Options: options,
		}}}
		for _, person := range []map[string]any{
			{"Name": "Ada", "Born": int32(1815), "Field": "maths"},
			{"Name": "Grace", "Born": int32(1906), "Field": "computing"},
			{"Name": "Linus", "Born": int32(1969), "Field": "computing"},
		} {
			setup = append(setup, put(db, "people", strings.ToLower(person["Name"].(string)), person))
		}
		return setup
	}
	searchComputing := func(db string, limit int) call {
		return call{op: transport.OpSearch, payload: server.SearchRequest{
			Database: db, Bucket: "people", Filter: map[string]any{"Field": "computing"}, Limit: limit,
		}}
	}
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "search cut at its limit says has_more", group: "query", storeType: "hash",
				notes: "Two documents match and limit is 1: one result, total 1 (the number returned, not the number that match), " +
					"and has_more true. A search has no skip; ask again with a larger limit.",
				setup: computingPeople(db, true), request: searchComputing(db, 1),
				wantOpcode: transport.OpSearch, wantField: map[string]any{"total": 1, "has_more": true, "index_used": "Field_1"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "search with exactly limit matches is complete", group: "query", storeType: "hash",
				notes: "Two documents match and limit is 2: both returned, and has_more false, because no further match exists.",
				setup: computingPeople(db, true), request: searchComputing(db, 2),
				wantOpcode: transport.OpSearch, wantField: map[string]any{"total": 2, "has_more": false, "index_used": "Field_1"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "search by full scan cut at its limit says has_more", group: "query", storeType: "hash",
				notes: "The same on the path no index serves: the scan stops one match past the limit.",
				setup: computingPeople(db, false), request: searchComputing(db, 1),
				wantOpcode: transport.OpSearch, wantField: map[string]any{"total": 1, "has_more": true, "index_used": "none — full scan"},
			}
		},
	}
}
