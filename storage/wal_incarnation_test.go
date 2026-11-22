package storage

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
)

// incarnationID is a distinct, readable id for the chain table below.
func incarnationID(n byte) [16]byte {
	var id [16]byte
	for i := range id {
		id[i] = n
	}
	return id
}

// TestAPositionIsValidOnlyUpToTheBaseOfEveryLaterIncarnation is T-87's table:
// a position learned under link j is valid up to the lowest base of any later
// link, and unbounded under the last. The chain's bases are not in order
// (a restore makes a later link's base lower than an earlier one's), so a
// limit read from the last link alone is wrong for every row before it.
func TestAPositionIsValidOnlyUpToTheBaseOfEveryLaterIncarnation(t *testing.T) {
	restored := []WALIncarnation{
		{ID: incarnationID(1), Base: 0},
		{ID: incarnationID(2), Base: 100},
		{ID: incarnationID(3), Base: 40},
		{ID: incarnationID(4), Base: 90},
	}
	legacy := []WALIncarnation{
		{ID: incarnationID(5), Base: 70, Legacy: true},
		{ID: incarnationID(6), Base: 120},
		{ID: incarnationID(7), Base: 150},
	}
	upgradedThisStart := []WALIncarnation{{ID: incarnationID(8), Base: 70, Legacy: true}}
	id := func(n byte) []byte { full := incarnationID(n); return full[:] }

	cases := []struct {
		name      string
		chain     []WALIncarnation
		reported  []byte
		wantLimit uint64
		wantFound bool
	}{
		{"the first link, below a later restore", restored, id(1), 40, true},
		{"a link before the restore", restored, id(2), 40, true},
		{"the restored link", restored, id(3), 90, true},
		{"the current link", restored, id(4), math.MaxUint64, true},
		{"an id the chain does not hold", restored, id(9), 0, false},
		{"no id, and the first link is not legacy", restored, nil, 0, false},
		{"no id, and a legacy first link", legacy, nil, 120, true},
		{"no id, and a legacy first link that is the current one", upgradedThisStart, nil, math.MaxUint64, true},
		{"an id under a legacy chain", legacy, id(6), 150, true},
		{"an id not in a legacy chain", legacy, id(1), 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			limit, found := ValidThrough(tc.chain, tc.reported)
			if found != tc.wantFound || limit != tc.wantLimit {
				t.Fatalf("ValidThrough(%s) = (%d, %t), want (%d, %t)", tc.name, limit, found, tc.wantLimit, tc.wantFound)
			}
		})
	}
}

// appendEntries appends n entries to w and returns the last sequence.
func appendEntries(t *testing.T, w *WAL, n int) uint64 {
	t.Helper()
	var seq uint64
	for i := 0; i < n; i++ {
		var err error
		if seq, err = w.Append("b", OpInsert, []byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	return seq
}

// TestEveryOpenOfAnExistingLogMintsALink is A-4.1: every open of a log mints a
// link whose base is the last sequence found at that open, so a position a
// peer learned from an earlier process is never mistaken for one of this
// process, however much the log has grown since it opened. Nothing else mints.
func TestEveryOpenOfAnExistingLogMintsALink(t *testing.T) {
	dir := t.TempDir()
	reopen := func(w *WAL) *WAL {
		t.Helper()
		if w != nil {
			if err := w.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
		}
		opened, err := OpenWAL(dir, nil)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		return opened
	}

	w := reopen(nil)
	appendEntries(t, w, 3)
	w = reopen(w)
	appendEntries(t, w, 2)
	w = reopen(w)
	// Appends made under the current link mint nothing.
	appendEntries(t, w, 4)
	defer func() { _ = w.Close() }()

	chain := w.Incarnations()
	if len(chain) != 3 {
		t.Fatalf("three opens left %d links, want 3: %+v", len(chain), chain)
	}
	for i, want := range []uint64{0, 3, 5} {
		if chain[i].Base != want || chain[i].Legacy {
			t.Fatalf("link %d is base %d legacy %t, want base %d and not legacy: %+v", i, chain[i].Base, chain[i].Legacy, want, chain)
		}
	}
	if chain[0].ID == chain[1].ID || chain[1].ID == chain[2].ID || chain[0].ID == chain[2].ID {
		t.Fatalf("the links do not have distinct ids: %+v", chain)
	}
	if w.LogID() != chain[0].ID {
		t.Fatalf("the log's id is %x, want the first link's %x", w.LogID(), chain[0].ID)
	}

	// The chain is durable: a reader of the directory sees what the WAL holds.
	persisted, err := os.ReadFile(filepath.Join(dir, "incarnation.bson"))
	if err != nil || len(persisted) == 0 {
		t.Fatalf("incarnation.bson after three opens: %d bytes, %v", len(persisted), err)
	}
}

// TestALogWithoutAChainStartsWithALegacyLink is the first start on this build:
// a log that has entries and no chain gets a legacy link whose base is the last
// sequence found, and a log with neither gets an ordinary link at zero.
func TestALogWithoutAChainStartsWithALegacyLink(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if chain := w.Incarnations(); len(chain) != 1 || chain[0].Base != 0 || chain[0].Legacy {
		t.Fatalf("a new log's chain is %+v, want one ordinary link at base 0", chain)
	}
	appendEntries(t, w, 7)
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, "incarnation.bson")); err != nil {
		t.Fatalf("removing the chain to stand for a log written before it existed: %v", err)
	}
	w, err = OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = w.Close() }()
	if chain := w.Incarnations(); len(chain) != 1 || chain[0].Base != 7 || !chain[0].Legacy {
		t.Fatalf("a log with entries and no chain has %+v, want one legacy link at base 7", chain)
	}
}
