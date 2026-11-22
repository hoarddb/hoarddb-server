package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestListingRefusalMemberReadsWhatTheRefusalNames ties the CLI's reader
// (BUG-0687: `use` forgives this refusal) to the text listingIncomplete
// composes, for every listing opcode, and pins that no other text matches.
func TestListingRefusalMemberReadsWhatTheRefusalNames(t *testing.T) {
	for _, op := range []transport.Opcode{transport.OpListDBs, transport.OpListBuckets} {
		refusal := listingIncomplete(op, ring.Node{ID: "n3"}, "dial peer n3: connection refused")
		member, ok := ListingRefusalMember(refusal.Error())
		if !ok || member != "n3" {
			t.Errorf("%s: ListingRefusalMember(%q) = %q, %v; want n3, true", op, refusal.Error(), member, ok)
		}
	}
	for _, other := range []string{"", "server is shutting down", "database \"app\" not found"} {
		if member, ok := ListingRefusalMember(other); ok {
			t.Errorf("ListingRefusalMember(%q) = %q, true; want false", other, member)
		}
	}
}
