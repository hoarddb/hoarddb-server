package server

import (
	"fmt"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// TestPrefixBloomLogFieldsNameEveryCounter: the line the ring node's close writes is the only
// way the prefix bloom counters leave a running server, and a measurement decides on them
// (the skipped share of the document rule, and Unmarked being 0), so each of the eleven must
// be there under its own name with its own value.
// Against: logging one rule's counters under the other's names, or dropping one.
func TestPrefixBloomLogFieldsNameEveryCounter(t *testing.T) {
	stats := storage.PrefixBloomStats{
		Document: storage.PrefixBloomRuleStats{Iterators: 1, Candidates: 2, Skipped: 3, Unmarked: 4, PrefixHashes: 5},
		Store:    storage.PrefixBloomRuleStats{Iterators: 6, Candidates: 7, Skipped: 8, Unmarked: 9, PrefixHashes: 10},
		Refused:  11,
	}
	want := "[document_iterators 1 document_candidates 2 document_skipped 3 document_unmarked 4 document_prefix_hashes 5 " +
		"store_iterators 6 store_candidates 7 store_skipped 8 store_unmarked 9 store_prefix_hashes 10 refused 11]"
	if got := fmt.Sprint(prefixBloomLogFields(stats)); got != want {
		t.Fatalf("the log fields are\n  %s\nwant\n  %s", got, want)
	}
}
