package wire

import (
	"context"
	"slices"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// TestAFlushSendsEveryPendingBitOfADatabaseInOneCommand is BUG-0808's cause: a node with
// thousands of ready pairs of one database sent them 64 at a time, about 40 Raft entries
// per node per database, each a full 1,024-byte bitmap, and the burst filled the Raft log
// fast enough to push a three-node cluster's quorum rounds past their bound. The body is
// the whole bitmap whatever it carries, so one flush sends one command per database and
// direction, carrying the union of the pending bits.
// [BREAK: drain at most 64 slots per command again]
func TestAFlushSendsEveryPendingBitOfADatabaseInOneCommand(t *testing.T) {
	const db, slots = 1, 4000
	cases := []struct {
		name  string
		kind  byte
		setUp func(t *testing.T, w *reportWorld, slot uint16)
	}{
		{"ready pairs report SEEDED", cluster.CommandSeeded, func(_ *testing.T, w *reportWorld, slot uint16) {
			w.setRecord(db, slot, machine.SeedReady)
		}},
		{"seeding pairs with a committed bit report RETIRE", cluster.CommandRetire, func(t *testing.T, w *reportWorld, slot uint16) {
			w.setRecord(db, slot, machine.SeedSeeding)
			w.setCommittedBit(t, db, slot)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newReportWorld(db)
			want := make([]uint16, 0, slots)
			for i := range uint16(slots) {
				slot := 2 * i // spread over the whole bitmap, not one byte run
				tc.setUp(t, w, slot)
				want = append(want, slot)
			}
			r := w.reporter()
			r.Reconcile()
			r.Flush(context.Background())
			sent := w.proposals(tc.kind)
			if len(sent) != 1 {
				t.Fatalf("one flush of %d pending bits sent %d commands of kind %d, want 1, not one per 64 slots",
					slots, len(sent), tc.kind)
			}
			if got := setSlots(t, sent[0].bits); !slices.Equal(got, want) {
				t.Fatalf("the command carries %d bits, want all %d pending ones", len(got), len(want))
			}
			r.Flush(context.Background())
			if n := len(w.allSends()); n != 1 {
				t.Fatalf("a second flush with nothing pending sent %d commands in all, want still 1", n)
			}
		})
	}
}

// TestBitsArrivingOverTwoFlushesSendOneCommandEach: bits noted between two flushes go in
// one command at the next flush, never one per 64, and what a flush sends is exactly what
// was pending at it. A few bits below the early threshold still wait for reportInterval,
// as before, and then go together.
// [BREAK: drain at most 64 slots per command again]
func TestBitsArrivingOverTwoFlushesSendOneCommandEach(t *testing.T) {
	const db = 1
	w := newReportWorld(db)
	r := w.reporter()
	note := func(from, to uint16) []uint16 {
		var noted []uint16
		for slot := from; slot < to; slot++ {
			w.setRecord(db, slot, machine.SeedReady)
			r.Note(db, slot, false)
			noted = append(noted, slot)
		}
		return noted
	}
	steps := []struct {
		name  string
		bits  func() []uint16
		wait  bool // advance reportInterval before the flush
		sends int  // SEEDED commands sent in all after the flush
	}{
		{"100 bits at the first flush", func() []uint16 { return note(0, 100) }, false, 1},
		{"200 more at the second flush", func() []uint16 { return note(100, 300) }, false, 2},
		{"10 more wait below the threshold", func() []uint16 { note(300, 310); return nil }, false, 2},
		{"the 10 go once they are reportInterval old", func() []uint16 { return []uint16{300, 301, 302, 303, 304, 305, 306, 307, 308, 309} }, true, 3},
	}
	for _, step := range steps {
		want := step.bits()
		if step.wait {
			w.advance(reportInterval)
		}
		r.Flush(context.Background())
		sent := w.proposals(cluster.CommandSeeded)
		if len(sent) != step.sends {
			t.Fatalf("%s: %d SEEDED commands in all, want %d", step.name, len(sent), step.sends)
		}
		if want == nil {
			continue
		}
		if got := setSlots(t, sent[len(sent)-1].bits); !slices.Equal(got, want) {
			t.Fatalf("%s: the last command carries %d bits (%v...), want the %d noted since the previous flush",
				step.name, len(got), got[:min(len(got), 4)], len(want))
		}
	}
}
