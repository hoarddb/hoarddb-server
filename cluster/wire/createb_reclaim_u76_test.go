package wire

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

// TestU76ALostCreateRaceReclaimsItsReverseDocument: [GUARD a create that loses the
// declaration race deletes its own reverse-index document, so repeated lost races leave
// _buckets holding only the winners' documents and show_buckets pays nothing for them].
func TestU76ALostCreateRaceReclaimsItsReverseDocument(t *testing.T) {
	for _, sameTable := range []bool{true, false} {
		real := newU39Real(t)
		const races = 6
		winners := map[string]uint64{}
		for i := range races {
			name := "Person" + string(rune('A'+i))
			winnerID, loserID := uint64(0xA00+i), uint64(0xB00+i)
			raced := false
			real.beforeWrite = func(_ int, req WriteRequest) {
				if family, _ := store.BucketsFamilyOf(req.Op.EncID); family != store.BucketsDeclaration || raced || req.Op.Class != cluster.OpInsert {
					return
				}
				raced = true
				if _, err := real.create(name, winnerID); err != nil {
					t.Errorf("the winning create of %s: %v", name, err)
				}
			}
			fields := u39Fields()
			if !sameTable {
				fields = append(fields, FieldEntry{Type: "string", Name: "City"})
			}
			if _, err := real.createWith(name, fields, u39Draws(loserID)); sameTable && err != nil {
				t.Fatalf("%s: the loser: %v", name, err)
			}
			winners[name] = winnerID
			if owner, found := real.reverse(loserID); found {
				t.Errorf("%s: the loser's reverse document %x survives, naming %q", name, loserID, owner)
			}
			if owner, found := real.reverse(winnerID); !found || owner != name {
				t.Errorf("%s: the winner's reverse document = %q, %v", name, owner, found)
			}
		}
		for name, id := range winners {
			if got := real.requireOneBucket(name); got != id {
				t.Errorf("%s: declaration id %x, want the winner's %x", name, got, id)
			}
		}
	}
}
