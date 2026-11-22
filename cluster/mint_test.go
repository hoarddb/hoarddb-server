package cluster

import (
	"sort"
	"testing"

	"github.com/google/uuid"
)

// TestU49MintIDIsUniqueAndMonotonic mints many ids in one process: each is a
// canonical 36-character UUIDv7, none repeats, and they sort in mint order, which is
// what keeps a list by encoded id roughly in insertion order (decisions §1).
func TestU49MintIDIsUniqueAndMonotonic(t *testing.T) {
	const count = 20000
	minted := make([]string, 0, count)
	seen := make(map[string]struct{}, count)
	for range count {
		id, err := MintID()
		if err != nil {
			t.Fatalf("MintID: %v", err)
		}
		if len(id) != 36 {
			t.Fatalf("id %q has %d characters, want 36", id, len(id))
		}
		parsed, err := uuid.Parse(id)
		if err != nil || parsed.Version() != 7 {
			t.Fatalf("id %q is not a UUIDv7 (err %v)", id, err)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("id %q minted twice", id)
		}
		seen[id] = struct{}{}
		minted = append(minted, id)
	}
	if !sort.StringsAreSorted(minted) {
		t.Fatal("ids minted in sequence do not sort in mint order")
	}
}
