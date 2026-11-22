package server

import (
	"strings"
	"testing"
)

// fillArenaToTheLimit puts maxArenaChunks slabs in the table's arena, so the
// next storeName needs one more chunk than the table may make. The record's
// name stays in slab 0, which is made to look sparse: its bytes were mostly
// dropped and compacted away, which is the state compaction moves names out of.
func fillArenaToTheLimit(t *testing.T, table *catalogTable, rec *bucketRec) {
	t.Helper()
	if rec.nameRef>>arenaOffsetBits != 0 {
		t.Fatalf("the helper needs the name in slab 0, it is in slab %d", rec.nameRef>>arenaOffsetBits)
	}
	name := table.name(rec)
	holding := make([]byte, firstArenaChunk)
	copy(holding, name)
	table.arena = make([]*arenaChunk, maxArenaChunks)
	for slabNo := range table.arena {
		table.arena[slabNo] = &arenaChunk{buf: make([]byte, 1)}
	}
	table.arena[0] = &arenaChunk{buf: holding, used: len(name) + 128, live: len(name)}
	table.arenaCount = maxArenaChunks
	// Any slab but 0: isSparse is never true for the tail, whose names move in.
	table.arenaTail = 1
	table.compactWanted = true
}

// TestCatalogTableCompactionReportsAFullArena is BUG-0359 part one: moveName
// copies a name to the arena's tail, and when the arena is full the name stays
// where it is. The move cannot happen, so the pass that asked for it has to be
// told: silently skipping it leaves a sparse name behind with nothing said.
func TestCatalogTableCompactionReportsAFullArena(t *testing.T) {
	table := newCatalogTable()
	rec := claimNamed(t, table, "orders")
	nameRef := rec.nameRef
	fillArenaToTheLimit(t, table, rec)

	_, more, err := table.maintain()
	if err == nil {
		t.Fatal("compaction met a full arena and reported nothing; the name was left behind silently")
	}
	if !strings.Contains(err.Error(), "arena") {
		t.Fatalf("the error from a full arena does not name it: %v", err)
	}
	if more {
		t.Fatal("maintain reported more work after an error; the caller has to decide whether to retry")
	}
	// The failed move changes nothing: the record still finds its name and
	// still points at the bytes it had.
	if rec.nameRef != nameRef || table.name(rec) != "orders" {
		t.Fatalf("a refused move rewrote the record: nameRef %d -> %d, name %q", nameRef, rec.nameRef, table.name(rec))
	}
	if got := table.find("orders"); got != rec {
		t.Fatalf("after a refused move find(orders) returned %v, want %v", got, rec)
	}

	// Room for one more chunk: the pass that was stopped resumes at the chunk
	// it could not finish instead of skipping it for the rest of the pass.
	table.arena[2] = nil
	table.arenaHint = firstArenaChunk
	if _, _, err := table.maintain(); err != nil {
		t.Fatalf("retrying compaction after freeing an arena chunk: %v", err)
	}
	if table.name(rec) != "orders" || table.find("orders") != rec {
		t.Fatalf("after the retry the record holds name %q and find returns %v", table.name(rec), table.find("orders"))
	}
	if rec.nameRef>>arenaOffsetBits == 0 {
		t.Fatalf("the name did not move out of the sparse slab (nameRef %d)", rec.nameRef)
	}
}
