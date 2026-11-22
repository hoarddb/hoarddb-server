package wire

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// dialLink is a data channel that answers each catch-up from the named source's
// Replicator and records which source it was dialled with, so a test can see the source
// a call chose.
type dialLink struct {
	mu      sync.Mutex
	sources map[string]*Replicator
	dialled []string
}

func (l *dialLink) Write(context.Context, string, WriteFrame) error { return nil }

func (l *dialLink) Ack(context.Context, string, WriteAck) error { return nil }

func (l *dialLink) CatchUp(ctx context.Context, source string, req CatchUpRequest) (CatchUpChunk, error) {
	l.mu.Lock()
	l.dialled = append(l.dialled, source)
	rep := l.sources[source]
	l.mu.Unlock()
	return rep.ServeCatchUp(ctx, req)
}

// TestCatchUpIntoUsesThePinnedSource: the caller names the source, and CatchUpInto dials that
// source and no other; it never asks CatchUpSource to choose. [BREAK: call CatchUpSource]
func TestCatchUpIntoUsesThePinnedSource(t *testing.T) {
	first := newU32Node(t, "first", nil)
	first.ownRun(u32Epoch, 1, 5)
	second := newU32Node(t, "second", nil)
	second.ownRun(u32Epoch, 1, 5)
	link := &dialLink{sources: map[string]*Replicator{"first": first.rep, "second": second.rep}}
	chosen := 0
	copyNode := newU32Node(t, "copy", func(cfg *ReplicatorConfig) {
		cfg.Link = link
		cfg.CatchUpSource = func(uint16, uint64) (string, error) {
			chosen++
			return "first", nil
		}
	})

	var got []PairRecord
	_, err := copyNode.rep.CatchUpInto(context.Background(), CatchUpIntoRequest{
		Slot: u32Slot, DB: u32DB, From: u32Pos(u32Epoch, 1), Source: "second",
		Sink: func(rec PairRecord) error {
			got = append(got, rec)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("CatchUpInto: %v", err)
	}
	if len(link.dialled) != 1 || link.dialled[0] != "second" {
		t.Fatalf("dialled %v, want only the pinned source \"second\"", link.dialled)
	}
	if chosen != 0 {
		t.Fatalf("CatchUpSource was asked to choose %d times, want 0", chosen)
	}
	if len(got) != 5 {
		t.Fatalf("the sink received %d records from the pinned source, want 5", len(got))
	}
}

// TestCatchUpIntoHandsRecordsToTheSinkAndAppliesNothing: every record reaches the sink in
// position order, the returned position is the source's tail, and the copy's pair and
// documents are untouched. [BREAK: call applyChunk for each chunk]
func TestCatchUpIntoHandsRecordsToTheSinkAndAppliesNothing(t *testing.T) {
	source := newU32Node(t, "source", func(cfg *ReplicatorConfig) { cfg.ChunkRecords = 4 })
	source.ownRun(u32Epoch, 1, 10)
	copyNode := newU32Node(t, "copy", nil)
	copyNode.catchUpFrom(source, source)

	var got []PairRecord
	tail, err := copyNode.rep.CatchUpInto(context.Background(), CatchUpIntoRequest{
		Slot: u32Slot, DB: u32DB, From: u32Pos(u32Epoch, 1), Source: "source",
		Sink: func(rec PairRecord) error {
			got = append(got, rec)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("CatchUpInto: %v", err)
	}
	if tail != u32Pos(u32Epoch, 10) {
		t.Fatalf("returned %s, want the source's appended position 7:10", tail)
	}
	if len(got) != 10 {
		t.Fatalf("the sink received %d records, want 10", len(got))
	}
	for i, rec := range got {
		if want := u32Pos(u32Epoch, uint64(i+1)); rec.Pos != want {
			t.Fatalf("sink record %d is at %s, want %s: records must arrive in order", i, rec.Pos, want)
		}
	}
	if copyNode.applied() != (cluster.Position{}) {
		t.Fatalf("the copy's applied position moved to %s: CatchUpInto must apply nothing", copyNode.applied())
	}
	if copyNode.hasDocument(u32Pos(u32Epoch, 1)) {
		t.Fatal("a record the sink was handed was applied to the copy's documents")
	}
}

// TestCatchUpIntoReportsTheHorizonWithoutResetting: a source that no longer holds what the
// copy needs answers ErrResetAndSeed, and the caller, not CatchUpInto, decides to reset.
// [BREAK: call r.reset on the horizon answer]
func TestCatchUpIntoReportsTheHorizonWithoutResetting(t *testing.T) {
	source := newU32Node(t, "source", nil)
	source.ownRun(u32Epoch, 5, 9) // 7:1 to 7:4 are gone
	copyNode := newU32Node(t, "copy", nil)
	copyNode.catchUpFrom(source, source)

	_, err := copyNode.rep.CatchUpInto(context.Background(), CatchUpIntoRequest{
		Slot: u32Slot, DB: u32DB, From: u32Pos(u32Epoch, 1), Source: "source",
		Sink: func(PairRecord) error { return nil },
	})
	if !errors.Is(err, ErrResetAndSeed) {
		t.Fatalf("CatchUpInto answered %v, want ErrResetAndSeed", err)
	}
	if copyNode.seeds.count() != 0 {
		t.Fatalf("the Seeder was called %d times: CatchUpInto must not reset the pair", copyNode.seeds.count())
	}
	if copyNode.rep.IsCurrent(u32Slot, u32DB) || copyNode.applied() != (cluster.Position{}) {
		t.Fatalf("the copy's pair moved: applied %s", copyNode.applied())
	}
}

// TestCatchUpIntoEndsAtASinkError: a sink that refuses a record ends the call with that
// error, and no record after it is handed over. [BREAK: ignore the sink's error]
func TestCatchUpIntoEndsAtASinkError(t *testing.T) {
	source := newU32Node(t, "source", nil)
	source.ownRun(u32Epoch, 1, 3)
	copyNode := newU32Node(t, "copy", nil)
	copyNode.catchUpFrom(source, source)

	refused := errors.New("the sink refused the record")
	handed := 0
	_, err := copyNode.rep.CatchUpInto(context.Background(), CatchUpIntoRequest{
		Slot: u32Slot, DB: u32DB, From: u32Pos(u32Epoch, 1), Source: "source",
		Sink: func(PairRecord) error {
			handed++
			if handed == 2 {
				return refused
			}
			return nil
		},
	})
	if !errors.Is(err, refused) {
		t.Fatalf("CatchUpInto answered %v, want the sink's error", err)
	}
	if handed != 2 {
		t.Fatalf("the sink was handed %d records, want it to stop at the refusal (2)", handed)
	}
}
