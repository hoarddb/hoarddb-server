package machine

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// Every package-level identifier in this file and seed_model_test.go starts
// with "seed": all machine units share one test package.

func seedPos(epoch, seq uint64) cluster.Position { return cluster.Position{Epoch: epoch, Seq: seq} }

var (
	seedID1 = [16]byte{1}
	seedID2 = [16]byte{2}
	seedCut = seedPos(5, 100)
	seedCfg = SeedConfig{SyncBytes: 100, BufferBytes: 1000, WindowBytes: 8 << 20, Epoch: 5}
)

// seedClone is a deep copy, so a test can compare a record before and after.
func seedClone(r SeedRecord) SeedRecord {
	r.Cursor = slices.Clone(r.Cursor)
	r.Buffered = slices.Clone(r.Buffered)
	r.bufferedSize = slices.Clone(r.bufferedSize)
	return r
}

// seedSame compares two records field by field; a nil and an empty slice are equal.
func seedSame(a, b SeedRecord) bool {
	return a.State == b.State && a.Phase == b.Phase && a.Applied == b.Applied && a.Cut == b.Cut && a.RegEpoch == b.RegEpoch &&
		a.SeedID == b.SeedID && a.Source == b.Source && bytes.Equal(a.Cursor, b.Cursor) &&
		slices.Equal(a.Buffered, b.Buffered) && a.BufferedBytes == b.BufferedBytes && a.Dirty == b.Dirty &&
		slices.Equal(a.bufferedSize, b.bufferedSize)
}

func seedOutSame(a, b SeedOutcome) bool {
	return a.Phase == b.Phase && a.ApplyChunk == b.ApplyChunk && slices.Equal(a.ApplyBuffered, b.ApplyBuffered) &&
		a.Sync == b.Sync && a.MarkReady == b.MarkReady && a.RerunSweeps == b.RerunSweeps &&
		reflect.DeepEqual(a.AskCatchUp, b.AskCatchUp) && a.Restart == b.Restart && a.Refusal == b.Refusal
}

// seedRec builds a record. A seeding record that is past the reset is
// registered: its cut and applied position are seedCut.
func seedRec(state uint8, phase Phase, mods ...func(*SeedRecord)) SeedRecord {
	r := SeedRecord{State: state, Phase: phase, SeedID: seedID1, Source: "src"}
	if state == SeedSeeding && phase != PhaseReset {
		r.Cut, r.Applied, r.RegEpoch = seedCut, seedCut, seedCfg.Epoch
	}
	for _, m := range mods {
		m(&r)
	}
	return r
}

func seedWithCursor(k string) func(*SeedRecord) {
	return func(r *SeedRecord) { r.Cursor = []byte(k) }
}
func seedWithDirty(n int64) func(*SeedRecord) { return func(r *SeedRecord) { r.Dirty = n } }
func seedWithApplied(p cluster.Position) func(*SeedRecord) {
	return func(r *SeedRecord) { r.Applied = p }
}
func seedWithRegEpoch(e uint64) func(*SeedRecord)        { return func(r *SeedRecord) { r.RegEpoch = e } }
func seedWithCutAt(p cluster.Position) func(*SeedRecord) { return func(r *SeedRecord) { r.Cut = p } }

// seedWithBuffered holds the given seqs of epoch 5, each of size bytes.
func seedWithBuffered(size int64, seqs ...uint64) func(*SeedRecord) {
	return func(r *SeedRecord) {
		for _, q := range seqs {
			r.Buffered = append(r.Buffered, seedPos(5, q))
			r.bufferedSize = append(r.bufferedSize, size)
			r.BufferedBytes += size
		}
	}
}

// seedWant is rec with mods applied: the record an event is expected to leave.
func seedWant(rec SeedRecord, mods ...func(*SeedRecord)) SeedRecord {
	w := seedClone(rec)
	for _, m := range mods {
		m(&w)
	}
	return w
}

func seedAsk(p cluster.Position) *cluster.Position { return &p }

type seedRowCase struct {
	name string
	rec  SeedRecord
	ev   SeedEvent
	cfg  SeedConfig
	want SeedRecord
	out  SeedOutcome
}

// seedRowCases is one case per line of the build doc's §4.5 decision table,
// plus one per gap-filling row this unit added (each named "gap/").
func seedRowCases() []seedRowCase {
	var cases []seedRowCase
	add := func(name string, rec SeedRecord, ev SeedEvent, want SeedRecord, out SeedOutcome) {
		cases = append(cases, seedRowCase{name: name, rec: rec, ev: ev, cfg: seedCfg, want: want, out: out})
	}
	unchanged := func(name string, rec SeedRecord, ev SeedEvent, out SeedOutcome) {
		add(name, rec, ev, rec, out)
	}
	zero := func(r *SeedRecord) {
		r.Cut, r.Applied, r.Cursor, r.Buffered, r.bufferedSize, r.BufferedBytes, r.Dirty, r.RegEpoch = cluster.Position{}, cluster.Position{}, nil, nil, nil, 0, 0, 0
	}
	fresh := func(phase Phase) func(*SeedRecord) {
		return func(r *SeedRecord) {
			zero(r)
			r.State, r.Phase, r.SeedID, r.Source = SeedSeeding, phase, seedID2, "node-b"
		}
	}
	reset := SeedOutcome{Phase: PhaseReset}

	// AttemptWritten
	attempt := func(nonEmpty bool) AttemptWritten {
		return AttemptWritten{SeedID: seedID2, Source: "node-b", LocalNonEmpty: nonEmpty}
	}
	add("AttemptWritten/non-empty", SeedRecord{}, attempt(true), seedWant(SeedRecord{}, fresh(PhaseReset)), reset)
	add("AttemptWritten/empty", SeedRecord{}, attempt(false), seedWant(SeedRecord{}, fresh(PhaseSnapshot)), SeedOutcome{Phase: PhaseSnapshot})
	unchanged("AttemptWritten/retired-not-cancellable", seedRec(SeedRetired, PhaseNone), attempt(true), SeedOutcome{})
	unchanged("AttemptWritten/retiring-not-cancellable", seedRec(SeedRetiring, PhaseNone), attempt(false), SeedOutcome{})
	add("gap/AttemptWritten/ready-always-resets", seedRec(SeedReady, PhaseNone, seedWithApplied(seedPos(5, 150))), attempt(false),
		seedWant(SeedRecord{}, fresh(PhaseReset)), reset)
	add("gap/AttemptWritten/seeding-with-data-resets", seedRec(SeedSeeding, PhaseSnapshot, seedWithCursor("k5")), attempt(false),
		seedWant(SeedRecord{}, fresh(PhaseReset)), reset)
	add("gap/AttemptWritten/seeding-without-data-restarts-clean", seedRec(SeedSeeding, PhaseSnapshot), attempt(false),
		seedWant(SeedRecord{}, fresh(PhaseSnapshot)), SeedOutcome{Phase: PhaseSnapshot})

	// RegisterAccepted
	accepted := RegisterAccepted{Cut: seedCut, WindowBytes: 8 << 20}
	add("RegisterAccepted/from-reset", seedRec(SeedSeeding, PhaseReset), accepted,
		seedRec(SeedSeeding, PhaseSnapshot), SeedOutcome{Phase: PhaseSnapshot})
	unchanged("gap/RegisterAccepted/resume-same-cut", seedRec(SeedSeeding, PhaseSnapshot, seedWithCursor("k5")), accepted,
		SeedOutcome{Phase: PhaseSnapshot})
	add("gap/RegisterAccepted/same-cut-in-buffer", seedRec(SeedSeeding, PhaseBuffer), accepted,
		seedRec(SeedSeeding, PhaseSnapshot), SeedOutcome{Phase: PhaseSnapshot}) // the buffer was memory only
	unchanged("gap/RegisterAccepted/other-cut-over-chunks", seedRec(SeedSeeding, PhaseSnapshot, seedWithCursor("k5")),
		RegisterAccepted{Cut: seedPos(5, 120)}, SeedOutcome{Phase: PhaseSnapshot, Restart: 4})
	add("gap/RegisterAccepted/other-cut-before-any-chunk", seedRec(SeedSeeding, PhaseSnapshot), RegisterAccepted{Cut: seedPos(5, 120)},
		seedRec(SeedSeeding, PhaseSnapshot, seedWithCutAt(seedPos(5, 120)), seedWithApplied(seedPos(5, 120))), SeedOutcome{Phase: PhaseSnapshot})
	add("gap/RegisterAccepted/new-cut-drops-the-old-stream's-frames", seedRec(SeedSeeding, PhaseSnapshot, seedWithBuffered(30, 101)),
		RegisterAccepted{Cut: seedPos(5, 120)},
		seedRec(SeedSeeding, PhaseSnapshot, seedWithCutAt(seedPos(5, 120)), seedWithApplied(seedPos(5, 120))), SeedOutcome{Phase: PhaseSnapshot})
	unchanged("RegisterAccepted/epoch-differs", seedRec(SeedSeeding, PhaseReset), RegisterAccepted{Cut: seedPos(6, 3)},
		SeedOutcome{Phase: PhaseReset, Restart: 3})
	unchanged("gap/RegisterAccepted/not-seeding", seedRec(SeedReady, PhaseNone), accepted, SeedOutcome{})

	// RegisterRefused
	add("RegisterRefused/redirect", seedRec(SeedSeeding, PhaseReset), RegisterRefused{Reason: 1, Redirect: "node-c"},
		seedWant(seedRec(SeedSeeding, PhaseReset), func(r *SeedRecord) { r.Source = "node-c" }), SeedOutcome{Phase: PhaseReset})
	unchanged("RegisterRefused/not-a-holder", seedRec(SeedSeeding, PhaseReset), RegisterRefused{Reason: 3}, SeedOutcome{Phase: PhaseReset, Restart: 1})
	unchanged("RegisterRefused/not-current", seedRec(SeedSeeding, PhaseSnapshot), RegisterRefused{Reason: 4}, SeedOutcome{Phase: PhaseSnapshot, Restart: 1})
	unchanged("gap/RegisterRefused/disk", seedRec(SeedSeeding, PhaseReset), RegisterRefused{Reason: 1}, SeedOutcome{Phase: PhaseReset})
	unchanged("gap/RegisterRefused/database-dropped", seedRec(SeedSeeding, PhaseReset), RegisterRefused{Reason: 2}, SeedOutcome{Phase: PhaseReset})
	unchanged("gap/RegisterRefused/mask-conflict", seedRec(SeedSeeding, PhaseReset), RegisterRefused{Reason: 5}, SeedOutcome{Phase: PhaseReset})
	unchanged("gap/RegisterRefused/not-seeding", seedRec(SeedReady, PhaseNone), RegisterRefused{Reason: 3}, SeedOutcome{})

	// ChunkStored
	chunk := ChunkStored{Cursor: []byte("k5"), Bytes: 40, Entries: 3, DedupeEntries: 1}
	unchanged("ChunkStored/in-reset-is-not-applied", seedRec(SeedSeeding, PhaseReset), chunk, SeedOutcome{Phase: PhaseReset})
	add("ChunkStored/applied", seedRec(SeedSeeding, PhaseSnapshot, seedWithCursor("k2"), seedWithDirty(10)), chunk,
		seedRec(SeedSeeding, PhaseSnapshot, seedWithCursor("k5"), seedWithDirty(50)), SeedOutcome{Phase: PhaseSnapshot, ApplyChunk: true})
	add("ChunkStored/sync-at-threshold", seedRec(SeedSeeding, PhaseSnapshot, seedWithCursor("k2"), seedWithDirty(60)), chunk,
		seedRec(SeedSeeding, PhaseSnapshot, seedWithCursor("k5"), seedWithDirty(100)), SeedOutcome{Phase: PhaseSnapshot, ApplyChunk: true, Sync: true})
	finalChunk := chunk
	finalChunk.Final = true
	// The final chunk syncs on the same threshold as any other (seeding-speed-note.md): the
	// ready record's synced commit makes it durable.
	add("ChunkStored/final", seedRec(SeedSeeding, PhaseSnapshot, seedWithCursor("k2")), finalChunk,
		seedRec(SeedSeeding, PhaseBuffer, seedWithCursor("k5"), seedWithDirty(40)), SeedOutcome{Phase: PhaseBuffer, ApplyChunk: true})
	add("ChunkStored/final-sync-at-threshold", seedRec(SeedSeeding, PhaseSnapshot, seedWithCursor("k2"), seedWithDirty(60)), finalChunk,
		seedRec(SeedSeeding, PhaseBuffer, seedWithCursor("k5"), seedWithDirty(100)), SeedOutcome{Phase: PhaseBuffer, ApplyChunk: true, Sync: true})
	add("gap/ChunkStored/final-of-an-empty-range", seedRec(SeedSeeding, PhaseSnapshot), ChunkStored{Final: true},
		seedRec(SeedSeeding, PhaseBuffer), SeedOutcome{Phase: PhaseBuffer, ApplyChunk: true})
	unchanged("gap/ChunkStored/duplicate", seedRec(SeedSeeding, PhaseSnapshot, seedWithCursor("k5")), chunk, SeedOutcome{Phase: PhaseSnapshot})
	unchanged("gap/ChunkStored/older-than-the-cursor", seedRec(SeedSeeding, PhaseSnapshot, seedWithCursor("k9")), chunk, SeedOutcome{Phase: PhaseSnapshot})
	unchanged("gap/ChunkStored/late-in-buffer", seedRec(SeedSeeding, PhaseBuffer, seedWithCursor("k2")), chunk, SeedOutcome{Phase: PhaseBuffer})
	unchanged("gap/ChunkStored/late-after-drain", seedRec(SeedSeeding, PhaseNone, seedWithCursor("k2")), chunk, SeedOutcome{})
	unchanged("ChunkStored/epoch-moved-since-registration", seedRec(SeedSeeding, PhaseSnapshot, seedWithRegEpoch(4)), chunk,
		SeedOutcome{Phase: PhaseSnapshot, Restart: 3})
	unchanged("gap/ChunkStored/not-seeding", seedRec(SeedReady, PhaseNone), chunk, SeedOutcome{})

	// LiveWriteBuffered
	live := LiveWriteBuffered{Pos: seedPos(5, 101), Bytes: 30}
	add("LiveWriteBuffered/in-snapshot", seedRec(SeedSeeding, PhaseSnapshot), live,
		seedRec(SeedSeeding, PhaseSnapshot, seedWithBuffered(30, 101)), SeedOutcome{Phase: PhaseSnapshot})
	add("LiveWriteBuffered/in-buffer", seedRec(SeedSeeding, PhaseBuffer, seedWithBuffered(10, 101)), LiveWriteBuffered{Pos: seedPos(5, 103), Bytes: 30},
		seedRec(SeedSeeding, PhaseBuffer, seedWithBuffered(10, 101), seedWithBuffered(30, 103)), SeedOutcome{Phase: PhaseBuffer})
	unchanged("LiveWriteBuffered/over-the-buffer-bound", seedRec(SeedSeeding, PhaseSnapshot, seedWithBuffered(980, 101)),
		LiveWriteBuffered{Pos: seedPos(5, 102), Bytes: 30}, SeedOutcome{Phase: PhaseSnapshot, Restart: 4})
	add("gap/LiveWriteBuffered/exactly-at-the-bound", seedRec(SeedSeeding, PhaseSnapshot, seedWithBuffered(970, 101)),
		LiveWriteBuffered{Pos: seedPos(5, 102), Bytes: 30},
		seedRec(SeedSeeding, PhaseSnapshot, seedWithBuffered(970, 101), seedWithBuffered(30, 102)), SeedOutcome{Phase: PhaseSnapshot})
	unchanged("gap/LiveWriteBuffered/at-or-below-the-cut", seedRec(SeedSeeding, PhaseSnapshot), LiveWriteBuffered{Pos: seedCut, Bytes: 30}, SeedOutcome{Phase: PhaseSnapshot})
	unchanged("gap/LiveWriteBuffered/duplicate-of-a-held-position", seedRec(SeedSeeding, PhaseBuffer, seedWithBuffered(30, 101)), live, SeedOutcome{Phase: PhaseBuffer})
	unchanged("gap/LiveWriteBuffered/before-registration", seedRec(SeedSeeding, PhaseReset), live, SeedOutcome{Phase: PhaseReset})
	unchanged("gap/LiveWriteBuffered/after-drain", seedRec(SeedSeeding, PhaseNone), live, SeedOutcome{})
	unchanged("LiveWriteBuffered/epoch-differs", seedRec(SeedSeeding, PhaseSnapshot), LiveWriteBuffered{Pos: seedPos(6, 1), Bytes: 30},
		SeedOutcome{Phase: PhaseSnapshot, Restart: 3})
	unchanged("gap/LiveWriteBuffered/not-seeding", seedRec(SeedReady, PhaseNone), live, SeedOutcome{})

	// BufferApplyStep
	step := BufferApplyStep{Pos: seedPos(5, 101), Bytes: 30}
	for _, ph := range []Phase{PhaseReset, PhaseSnapshot, PhaseNone} {
		unchanged(fmt.Sprintf("BufferApplyStep/not-in-buffer-phase-%d", ph), seedRec(SeedSeeding, ph), step, SeedOutcome{Phase: ph, Restart: 4})
	}
	add("BufferApplyStep/next", seedRec(SeedSeeding, PhaseBuffer, seedWithBuffered(30, 101, 102), seedWithDirty(5)), step,
		seedRec(SeedSeeding, PhaseBuffer, seedWithApplied(seedPos(5, 101)), seedWithBuffered(30, 102), seedWithDirty(35)),
		SeedOutcome{Phase: PhaseBuffer, ApplyBuffered: []cluster.Position{seedPos(5, 101)}})
	add("gap/BufferApplyStep/sync-at-threshold", seedRec(SeedSeeding, PhaseBuffer, seedWithBuffered(30, 101), seedWithDirty(80)), step,
		seedRec(SeedSeeding, PhaseBuffer, seedWithApplied(seedPos(5, 101)), seedWithDirty(110)),
		SeedOutcome{Phase: PhaseBuffer, ApplyBuffered: []cluster.Position{seedPos(5, 101)}, Sync: true})
	add("gap/BufferApplyStep/next-from-catch-up-drops-the-held-copy", seedRec(SeedSeeding, PhaseBuffer, seedWithBuffered(30, 101, 102)),
		BufferApplyStep{Pos: seedPos(5, 101), Bytes: 77},
		seedRec(SeedSeeding, PhaseBuffer, seedWithApplied(seedPos(5, 101)), seedWithBuffered(30, 102), seedWithDirty(77)),
		SeedOutcome{Phase: PhaseBuffer, ApplyBuffered: []cluster.Position{seedPos(5, 101)}})
	unchanged("BufferApplyStep/gap", seedRec(SeedSeeding, PhaseBuffer, seedWithBuffered(30, 103)), BufferApplyStep{Pos: seedPos(5, 103), Bytes: 30},
		SeedOutcome{Phase: PhaseBuffer, AskCatchUp: seedAsk(seedPos(5, 101))})
	unchanged("gap/BufferApplyStep/already-applied", seedRec(SeedSeeding, PhaseBuffer, seedWithApplied(seedPos(5, 105))),
		BufferApplyStep{Pos: seedPos(5, 104), Bytes: 30}, SeedOutcome{Phase: PhaseBuffer})
	unchanged("gap/BufferApplyStep/the-applied-position-again", seedRec(SeedSeeding, PhaseBuffer, seedWithApplied(seedPos(5, 105))),
		BufferApplyStep{Pos: seedPos(5, 105), Bytes: 30}, SeedOutcome{Phase: PhaseBuffer})
	unchanged("BufferApplyStep/epoch-differs", seedRec(SeedSeeding, PhaseBuffer), BufferApplyStep{Pos: seedPos(6, 1)}, SeedOutcome{Phase: PhaseBuffer, Restart: 3})
	unchanged("gap/BufferApplyStep/not-seeding", seedRec(SeedReady, PhaseNone), step, SeedOutcome{})

	// BufferDrained
	drained := BufferDrained{Last: seedCut}
	add("BufferDrained/empty", seedRec(SeedSeeding, PhaseBuffer), drained, seedRec(SeedSeeding, PhaseNone),
		SeedOutcome{Phase: PhaseNone, Sync: true})
	add("BufferDrained/through-the-tail", seedRec(SeedSeeding, PhaseBuffer, seedWithApplied(seedPos(5, 110))), BufferDrained{Last: seedPos(5, 108)},
		seedRec(SeedSeeding, PhaseNone, seedWithApplied(seedPos(5, 110))), SeedOutcome{Phase: PhaseNone, Sync: true})
	unchanged("gap/BufferDrained/with-positions-still-held", seedRec(SeedSeeding, PhaseBuffer, seedWithBuffered(30, 101)), drained, SeedOutcome{Phase: PhaseBuffer})
	unchanged("gap/BufferDrained/behind-the-source-tail", seedRec(SeedSeeding, PhaseBuffer), BufferDrained{Last: seedPos(5, 140)},
		SeedOutcome{Phase: PhaseBuffer, AskCatchUp: seedAsk(seedPos(5, 101))})
	unchanged("gap/BufferDrained/snapshot-not-complete", seedRec(SeedSeeding, PhaseSnapshot), drained, SeedOutcome{Phase: PhaseSnapshot})
	unchanged("gap/BufferDrained/before-registration", seedRec(SeedSeeding, PhaseReset), drained, SeedOutcome{Phase: PhaseReset})
	unchanged("gap/BufferDrained/again-after-drain", seedRec(SeedSeeding, PhaseNone), drained, SeedOutcome{})
	unchanged("BufferDrained/epoch-differs", seedRec(SeedSeeding, PhaseBuffer), BufferDrained{Last: seedPos(6, 1)}, SeedOutcome{Phase: PhaseBuffer, Restart: 3})
	unchanged("gap/BufferDrained/not-seeding", seedRec(SeedReady, PhaseNone), drained, SeedOutcome{})

	// SyncDone
	unchanged("SyncDone/error", seedRec(SeedSeeding, PhaseNone, seedWithDirty(9)), SyncDone{Err: errors.New("disk")}, SeedOutcome{Restart: 4})
	add("SyncDone/ready", seedRec(SeedSeeding, PhaseNone, seedWithApplied(seedPos(5, 110)), seedWithCursor("k9"), seedWithDirty(9)), SyncDone{},
		seedRec(SeedReady, PhaseNone, seedWithApplied(seedPos(5, 110)), func(r *SeedRecord) { r.Cut = cluster.Position{} }),
		SeedOutcome{MarkReady: true, RerunSweeps: true})
	add("gap/SyncDone/intermediate-in-snapshot", seedRec(SeedSeeding, PhaseSnapshot, seedWithDirty(90)), SyncDone{},
		seedRec(SeedSeeding, PhaseSnapshot), SeedOutcome{Phase: PhaseSnapshot})
	add("gap/SyncDone/intermediate-in-buffer", seedRec(SeedSeeding, PhaseBuffer, seedWithDirty(90)), SyncDone{},
		seedRec(SeedSeeding, PhaseBuffer), SeedOutcome{Phase: PhaseBuffer})
	unchanged("gap/SyncDone/intermediate-in-reset", seedRec(SeedSeeding, PhaseReset), SyncDone{}, SeedOutcome{Phase: PhaseReset})
	unchanged("SyncDone/epoch-moved-since-registration", seedRec(SeedSeeding, PhaseNone, seedWithRegEpoch(4)), SyncDone{},
		SeedOutcome{Restart: 3})
	unchanged("gap/SyncDone/on-ready-is-not-ready-again", seedRec(SeedReady, PhaseNone), SyncDone{}, SeedOutcome{})
	unchanged("gap/SyncDone/on-retired-never-readies", seedRec(SeedRetired, PhaseNone), SyncDone{}, SeedOutcome{})
	unchanged("gap/SyncDone/on-retiring-never-readies", seedRec(SeedRetiring, PhaseNone), SyncDone{}, SeedOutcome{})
	unchanged("gap/SyncDone/error-on-ready-restarts-nothing", seedRec(SeedReady, PhaseNone), SyncDone{Err: errors.New("disk")}, SeedOutcome{})

	// RetireOrdered, DeleteStarted, RetireDeleted
	toRetired := func(r *SeedRecord) {
		r.State, r.Phase, r.Buffered, r.bufferedSize, r.BufferedBytes, r.Dirty, r.RegEpoch = SeedRetired, PhaseNone, nil, nil, 0, 0, 0
	}
	add("RetireOrdered/ready", seedRec(SeedReady, PhaseNone), RetireOrdered{Fence: seedPos(5, 150)}, seedWant(seedRec(SeedReady, PhaseNone), toRetired), SeedOutcome{})
	unchanged("RetireOrdered/retiring-not-cancellable", seedRec(SeedRetiring, PhaseNone), RetireOrdered{}, SeedOutcome{})
	add("RetireOrdered/already-retired", seedRec(SeedRetired, PhaseNone), RetireOrdered{}, seedRec(SeedRetired, PhaseNone), SeedOutcome{})
	add("RetireOrdered/seeding", seedRec(SeedSeeding, PhaseBuffer, seedWithBuffered(30, 101), seedWithDirty(4)), RetireOrdered{},
		seedWant(seedRec(SeedSeeding, PhaseBuffer), toRetired), SeedOutcome{})
	unchanged("gap/RetireOrdered/absent", SeedRecord{}, RetireOrdered{}, SeedOutcome{})
	add("DeleteStarted/retired", seedRec(SeedRetired, PhaseNone), DeleteStarted{}, seedRec(SeedRetiring, PhaseNone), SeedOutcome{})
	unchanged("gap/DeleteStarted/ready-is-not-deleted", seedRec(SeedReady, PhaseNone), DeleteStarted{}, SeedOutcome{})
	unchanged("gap/DeleteStarted/seeding", seedRec(SeedSeeding, PhaseSnapshot), DeleteStarted{}, SeedOutcome{Phase: PhaseSnapshot})
	unchanged("gap/DeleteStarted/already-retiring", seedRec(SeedRetiring, PhaseNone), DeleteStarted{}, SeedOutcome{})
	add("RetireDeleted/retiring", seedRec(SeedRetiring, PhaseNone), RetireDeleted{}, seedRec(SeedRetired, PhaseNone), SeedOutcome{})
	add("RetireDeleted/retired", seedRec(SeedRetired, PhaseNone), RetireDeleted{}, seedRec(SeedRetired, PhaseNone), SeedOutcome{})
	add("RetireDeleted/ready", seedRec(SeedReady, PhaseNone), RetireDeleted{}, seedRec(SeedRetired, PhaseNone), SeedOutcome{})
	add("RetireDeleted/seeding", seedRec(SeedSeeding, PhaseBuffer), RetireDeleted{}, seedWant(seedRec(SeedSeeding, PhaseBuffer), toRetired), SeedOutcome{})
	unchanged("gap/RetireDeleted/absent", SeedRecord{}, RetireDeleted{}, SeedOutcome{})

	// A cut in an earlier epoch than the committed one: a quiet pair's last
	// position (conv §10 rule 5). A later epoch is the source being ahead.
	early := []func(*SeedRecord){seedWithCutAt(seedPos(4, 5)), seedWithApplied(seedPos(4, 5))}
	add("RegisterAccepted/cut-in-an-earlier-epoch", seedRec(SeedSeeding, PhaseReset), RegisterAccepted{Cut: seedPos(4, 5)},
		seedRec(SeedSeeding, PhaseSnapshot, early...), SeedOutcome{Phase: PhaseSnapshot})
	unchanged("RegisterAccepted/cut-in-a-later-epoch", seedRec(SeedSeeding, PhaseReset), RegisterAccepted{Cut: seedPos(6, 0)}, SeedOutcome{Phase: PhaseReset, Restart: 3})
	add("RegisterAccepted/resume-registers-in-the-committed-epoch", seedRec(SeedSeeding, PhaseSnapshot, seedWithCursor("k5"), seedWithRegEpoch(0)), accepted,
		seedRec(SeedSeeding, PhaseSnapshot, seedWithCursor("k5")), SeedOutcome{Phase: PhaseSnapshot})
	add("LiveWriteBuffered/first-frame-of-the-committed-epoch-above-an-earlier-cut", seedRec(SeedSeeding, PhaseSnapshot, early...), LiveWriteBuffered{Pos: seedPos(5, 1), Bytes: 30},
		seedRec(SeedSeeding, PhaseSnapshot, append(slices.Clone(early), func(r *SeedRecord) {
			r.Buffered, r.bufferedSize, r.BufferedBytes = []cluster.Position{seedPos(5, 1)}, []int64{30}, 30
		})...), SeedOutcome{Phase: PhaseSnapshot})
	add("BufferApplyStep/opener-of-the-committed-epoch-after-an-earlier-cut", seedRec(SeedSeeding, PhaseBuffer, early...), BufferApplyStep{Pos: seedPos(5, 1), Bytes: 30},
		seedRec(SeedSeeding, PhaseBuffer, early...), SeedOutcome{Phase: PhaseBuffer, ApplyBuffered: []cluster.Position{seedPos(5, 1)}})
	cases[len(cases)-1].want.Applied, cases[len(cases)-1].want.Dirty = seedPos(5, 1), 30
	unchanged("BufferApplyStep/gap-after-an-earlier-cut", seedRec(SeedSeeding, PhaseBuffer, early...), BufferApplyStep{Pos: seedPos(5, 3), Bytes: 30},
		SeedOutcome{Phase: PhaseBuffer, AskCatchUp: seedAsk(seedPos(5, 1))})
	unchanged("BufferApplyStep/a-record-of-the-earlier-epoch-after-the-cut-is-a-gap", seedRec(SeedSeeding, PhaseBuffer, early...), BufferApplyStep{Pos: seedPos(4, 6), Bytes: 30},
		SeedOutcome{Phase: PhaseBuffer, AskCatchUp: seedAsk(seedPos(5, 1))})
	unchanged("BufferApplyStep/a-later-epoch-is-ahead", seedRec(SeedSeeding, PhaseBuffer, early...), BufferApplyStep{Pos: seedPos(6, 1), Bytes: 30},
		SeedOutcome{Phase: PhaseBuffer, Restart: 3})
	add("BufferDrained/a-quiet-pair-drains-at-its-earlier-cut", seedRec(SeedSeeding, PhaseBuffer, early...), BufferDrained{Last: seedPos(4, 5)},
		seedRec(SeedSeeding, PhaseNone, early...), SeedOutcome{Phase: PhaseNone, Sync: true})
	unchanged("BufferDrained/behind-the-source-after-an-earlier-cut", seedRec(SeedSeeding, PhaseBuffer, early...), BufferDrained{Last: seedPos(5, 3)},
		SeedOutcome{Phase: PhaseBuffer, AskCatchUp: seedAsk(seedPos(5, 1))})

	// A seeding record with a phase the machine does not know.
	for _, ev := range []SeedEvent{accepted, RegisterRefused{Reason: 3}, chunk, live, step, drained, SyncDone{}} {
		unchanged(fmt.Sprintf("gap/unknown-phase/%T", ev), SeedRecord{State: SeedSeeding, Phase: 9, Cut: seedCut, Applied: seedCut}, ev, SeedOutcome{Phase: 9, Restart: 4})
	}
	return cases
}

// TestSeedEveryRowOfTheDecisionTable runs one case per line of the build doc's
// §4.5 table, and per gap-filling row, checking the record and the outcome.
func TestSeedEveryRowOfTheDecisionTable(t *testing.T) {
	for _, c := range seedRowCases() {
		t.Run(c.name, func(t *testing.T) {
			rec := seedClone(c.rec)
			out := rec.Apply(c.ev, c.cfg)
			if !seedSame(rec, c.want) {
				t.Errorf("record after %T:\n got  %+v\n want %+v", c.ev, rec, c.want)
			}
			if !seedOutSame(out, c.out) {
				t.Errorf("outcome of %T:\n got  %+v\n want %+v", c.ev, out, c.out)
			}
			if rows := matchingSeedRows(&c.rec, c.ev, c.cfg); len(rows) != 1 {
				t.Errorf("rows matching the case = %v, want exactly one", rows)
			}
		})
	}
}

// seedClasses builds the cross product of record context x event class. A
// context is a (state, phase) pair with every relation the rows look at: a
// cursor held or not, positions buffered or not, and a cut in the committed
// epoch or not. Applied is always (cutEpoch, 100). An event varies its payload
// across every relation the rows look at.
func seedClasses() []seedRowCase {
	var out []seedRowCase
	states := []uint8{0, 1, 2, 3, 4, 5, 255}
	phases := []Phase{PhaseNone, PhaseReset, PhaseSnapshot, PhaseBuffer, 4}
	for _, state := range states {
		for _, phase := range phases {
			for _, hasCursor := range []bool{false, true} {
				for _, hasBuffered := range []bool{false, true} {
					for _, cutEpoch := range []uint64{4, 5, 6} {
						for _, regEpoch := range []uint64{5, 0} {
							rec := SeedRecord{State: state, Phase: phase, SeedID: seedID1, Source: "src",
								Cut: seedPos(cutEpoch, 90), Applied: seedPos(cutEpoch, 100), RegEpoch: regEpoch}
							if hasCursor {
								rec.Cursor = []byte("m")
							}
							if hasBuffered {
								seedWithBuffered(30, 120)(&rec)
							}
							for _, ev := range seedEventVariants(rec) {
								out = append(out, seedRowCase{rec: rec, ev: ev, cfg: seedCfg})
							}
						}
					}
				}
			}
		}
	}
	return out
}

func seedEventVariants(rec SeedRecord) []SeedEvent {
	var evs []SeedEvent
	positions := []cluster.Position{seedPos(5, 50), seedPos(5, 100), seedPos(5, 101), seedPos(5, 120), seedPos(5, 150), seedPos(4, 5), seedPos(4, 101), seedPos(6, 1), {}}
	for _, nonEmpty := range []bool{false, true} {
		evs = append(evs, AttemptWritten{SeedID: seedID2, Source: "n", LocalNonEmpty: nonEmpty})
	}
	for _, cut := range append([]cluster.Position{rec.Cut}, positions...) {
		evs = append(evs, RegisterAccepted{Cut: cut})
	}
	for reason := uint8(0); reason <= 6; reason++ {
		for _, redirect := range []string{"", "node-c"} {
			evs = append(evs, RegisterRefused{Reason: reason, Redirect: redirect})
		}
	}
	for _, cursor := range []string{"", "a", "m", "z"} {
		for _, entries := range []int{0, 2} {
			for _, final := range []bool{false, true} {
				evs = append(evs, ChunkStored{Cursor: []byte(cursor), Bytes: 10, Entries: entries, Final: final})
			}
		}
	}
	for _, p := range positions {
		for _, bytes := range []int64{10, 5000} {
			evs = append(evs, LiveWriteBuffered{Pos: p, Bytes: bytes}, BufferApplyStep{Pos: p, Bytes: bytes})
		}
		evs = append(evs, BufferDrained{Last: p})
	}
	evs = append(evs, SyncDone{}, SyncDone{Err: errors.New("disk")}, RetireOrdered{}, DeleteStarted{}, RetireDeleted{})
	return evs
}

// TestSeedEveryEventClassHasExactlyOneRow enumerates the cross product of event
// kind x record context x payload relation and fails if any class matches zero
// rows (a gap) or two (an ambiguity).
func TestSeedEveryEventClassHasExactlyOneRow(t *testing.T) {
	classes := seedClasses()
	kinds := map[string]bool{}
	for _, c := range classes {
		kinds[fmt.Sprintf("%T", c.ev)] = true
		rows := matchingSeedRows(&c.rec, c.ev, c.cfg)
		if len(rows) != 1 {
			t.Fatalf("%T on state %d phase %d cursor %q buffered %v cut %v: matched rows %v, want exactly one",
				c.ev, c.rec.State, c.rec.Phase, c.rec.Cursor, c.rec.Buffered, c.rec.Cut, rows)
		}
	}
	if len(kinds) != 11 {
		t.Errorf("the enumeration covered %d event kinds, want all 11", len(kinds))
	}
	t.Logf("enumerated %d event classes over %d event kinds, each with exactly one row", len(classes), len(kinds))
}

// TestSeedRefusalsAndRestartsNeverChangeState is the shared rule of build doc
// §4: a row that returns a restart or a refusal leaves every field as it was.
func TestSeedRefusalsAndRestartsNeverChangeState(t *testing.T) {
	checked := 0
	for _, c := range seedClasses() {
		rec := seedClone(c.rec)
		out := rec.Apply(c.ev, c.cfg)
		if out.Restart == 0 && out.Refusal == 0 {
			continue
		}
		checked++
		if !seedSame(rec, c.rec) {
			t.Fatalf("%T on %+v returned restart %d and changed the record to %+v", c.ev, c.rec, out.Restart, rec)
		}
	}
	if checked == 0 {
		t.Fatal("no class returned a restart; the check proves nothing")
	}
}

// TestSeedReadyIsOnlyEverASyncDoneWithNoError sweeps every class: the record
// becomes ready, and MarkReady is set, only for a seeding record in PhaseNone
// whose SyncDone carries no error.
func TestSeedReadyIsOnlyEverASyncDoneWithNoError(t *testing.T) {
	for _, c := range seedClasses() {
		rec := seedClone(c.rec)
		out := rec.Apply(c.ev, c.cfg)
		becameReady := rec.State == SeedReady && c.rec.State != SeedReady
		sd, isSync := c.ev.(SyncDone)
		legal := isSync && sd.Err == nil && c.rec.State == SeedSeeding && c.rec.Phase == PhaseNone && c.rec.RegEpoch == c.cfg.Epoch
		if becameReady != out.MarkReady || (becameReady && !legal) {
			t.Fatalf("%T on state %d phase %d: became ready %v, MarkReady %v", c.ev, c.rec.State, c.rec.Phase, becameReady, out.MarkReady)
		}
		if out.RerunSweeps && !out.MarkReady {
			t.Fatalf("%T asked to re-run the sweeps without MarkReady", c.ev)
		}
	}
}

// seedStreamTo drives a fresh record through attempt, register and the given
// chunks, returning the record.
func seedStreamTo(t *testing.T, nonEmpty bool, cursors ...string) SeedRecord {
	t.Helper()
	var r SeedRecord
	r.Apply(AttemptWritten{SeedID: seedID1, Source: "src", LocalNonEmpty: nonEmpty}, seedCfg)
	r.Apply(RegisterAccepted{Cut: seedCut}, seedCfg)
	for _, k := range cursors {
		r.Apply(ChunkStored{Cursor: []byte(k), Bytes: 10, Entries: 2}, seedCfg)
	}
	return r
}

// TestRecordResumesAtItsCut [persist the cursor with its chunk]: after a
// restart the record holds the last applied key and the cut, a re-registration
// on the same pinned snapshot changes nothing, and streaming goes on after the
// cursor without reapplying an earlier chunk.
func TestRecordResumesAtItsCut(t *testing.T) {
	r := seedStreamTo(t, false, "k1", "k2")
	persisted := seedClone(r)
	persisted.Buffered, persisted.bufferedSize, persisted.BufferedBytes, persisted.Dirty, persisted.RegEpoch = nil, nil, 0, 0, 0

	if string(persisted.Cursor) != "k2" || persisted.Cut != seedCut || persisted.Applied != seedCut || persisted.Phase != PhaseSnapshot {
		t.Fatalf("persisted record = %+v, want cursor k2, cut and applied %v, snapshot phase", persisted, seedCut)
	}
	before := seedClone(persisted)
	before.RegEpoch = seedCfg.Epoch // the resume registers again in the committed epoch
	if out := persisted.Apply(RegisterAccepted{Cut: seedCut}, seedCfg); out.Restart != 0 || !seedSame(persisted, before) {
		t.Fatalf("resuming on the same cut changed the record or restarted: %+v %+v", out, persisted)
	}
	if out := persisted.Apply(ChunkStored{Cursor: []byte("k1"), Bytes: 10, Entries: 2}, seedCfg); out.ApplyChunk {
		t.Fatalf("a chunk before the cursor was applied again: %+v", out)
	}
	if out := persisted.Apply(ChunkStored{Cursor: []byte("k3"), Bytes: 10, Entries: 2}, seedCfg); !out.ApplyChunk || string(persisted.Cursor) != "k3" {
		t.Fatalf("the chunk after the cursor was not applied: %+v cursor %q", out, persisted.Cursor)
	}
	if persisted.Applied != seedCut {
		t.Fatalf("applied = %v while seeding, want it kept at the cut %v", persisted.Applied, seedCut)
	}
}

// TestAppliedPositionIsNeverAPeersReport [adopt the source's position]: the
// applied position moves only by this node applying the next position itself.
func TestAppliedPositionIsNeverAPeersReport(t *testing.T) {
	r := seedStreamTo(t, false, "k1")
	if r.Applied != seedCut {
		t.Fatalf("applied = %v in the snapshot phase, want the cut %v", r.Applied, seedCut)
	}
	r.Apply(LiveWriteBuffered{Pos: seedPos(5, 101), Bytes: 10}, seedCfg)
	r.Apply(ChunkStored{Cursor: []byte("k2"), Bytes: 10, Entries: 1, Final: true}, seedCfg)
	if r.Applied != seedCut {
		t.Fatalf("applied = %v after a buffered frame and the final chunk, want the cut", r.Applied)
	}
	// The source says it has appended 5:900; that is a target, not a fact.
	r.Apply(BufferApplyStep{Pos: seedPos(5, 101), Bytes: 10}, seedCfg)
	out := r.Apply(BufferDrained{Last: seedPos(5, 900)}, seedCfg)
	if r.Applied != seedPos(5, 101) {
		t.Fatalf("applied = %v after a drain claim of 5:900, want 5:101: a peer's position was adopted", r.Applied)
	}
	if out.AskCatchUp == nil || *out.AskCatchUp != seedPos(5, 102) || out.Sync {
		t.Fatalf("outcome = %+v, want a catch-up from 5:102 and no sync", out)
	}
}

// TestReseedResetsTheRangeBeforeTheSnapshot [skip the reset]: a non-empty
// local range is reset before the first byte, no chunk is applied while the
// reset is owed, and a ready copy that reseeds resets whatever the caller says.
func TestReseedResetsTheRangeBeforeTheSnapshot(t *testing.T) {
	var r SeedRecord
	if out := r.Apply(AttemptWritten{SeedID: seedID1, Source: "src", LocalNonEmpty: true}, seedCfg); out.Phase != PhaseReset || r.Phase != PhaseReset {
		t.Fatalf("a non-empty range started in phase %d, want reset", out.Phase)
	}
	if out := r.Apply(ChunkStored{Cursor: []byte("k1"), Bytes: 10, Entries: 1}, seedCfg); out.ApplyChunk || len(r.Cursor) != 0 {
		t.Fatalf("a chunk was applied over the range before the reset: %+v", out)
	}
	if out := r.Apply(RegisterAccepted{Cut: seedCut}, seedCfg); out.Phase != PhaseSnapshot {
		t.Fatalf("register after the reset = %+v, want the snapshot phase", out)
	}

	ready := seedRec(SeedReady, PhaseNone, seedWithApplied(seedPos(5, 150)))
	if out := ready.Apply(AttemptWritten{SeedID: seedID2, Source: "src", LocalNonEmpty: false}, seedCfg); out.Phase != PhaseReset || !ready.Applied.Zero() {
		t.Fatalf("a ready copy reseeding: phase %d applied %v, want reset and a zeroed applied position", out.Phase, ready.Applied)
	}
}

// TestBufferGapAsksCatchUpNeverSmooths [apply the next buffered position]: a
// hole in the buffered positions is asked for, never skipped.
func TestBufferGapAsksCatchUpNeverSmooths(t *testing.T) {
	r := seedStreamTo(t, false)
	r.Apply(ChunkStored{Cursor: []byte("k1"), Bytes: 10, Entries: 1, Final: true}, seedCfg)
	r.Apply(LiveWriteBuffered{Pos: seedPos(5, 101), Bytes: 10}, seedCfg)
	r.Apply(LiveWriteBuffered{Pos: seedPos(5, 103), Bytes: 10}, seedCfg)
	if out := r.Apply(BufferApplyStep{Pos: seedPos(5, 101), Bytes: 10}, seedCfg); len(out.ApplyBuffered) != 1 {
		t.Fatalf("5:101 was not applied: %+v", out)
	}
	before := seedClone(r)
	out := r.Apply(BufferApplyStep{Pos: seedPos(5, 103), Bytes: 10}, seedCfg)
	if len(out.ApplyBuffered) != 0 || out.AskCatchUp == nil || *out.AskCatchUp != seedPos(5, 102) {
		t.Fatalf("outcome = %+v, want a catch-up from 5:102 and nothing applied", out)
	}
	if !seedSame(r, before) || r.Applied != seedPos(5, 101) {
		t.Fatalf("the gap changed the record: %+v", r)
	}
	// The catch-up supplies 5:102; then 5:103 is the next position.
	r.Apply(BufferApplyStep{Pos: seedPos(5, 102), Bytes: 10}, seedCfg)
	if out := r.Apply(BufferApplyStep{Pos: seedPos(5, 103), Bytes: 10}, seedCfg); len(out.ApplyBuffered) != 1 || r.Applied != seedPos(5, 103) {
		t.Fatalf("5:103 after the catch-up: %+v applied %v", out, r.Applied)
	}
	if len(r.Buffered) != 0 || r.BufferedBytes != 0 {
		t.Fatalf("buffered %v (%d bytes) after draining, want none", r.Buffered, r.BufferedBytes)
	}
}

// TestReadyIsWrittenWithSync [write ready unsynced]: the drain asks for a sync
// and is not ready; a failed sync restarts; only a clean SyncDone marks ready.
func TestReadyIsWrittenWithSync(t *testing.T) {
	r := seedStreamTo(t, false)
	r.Apply(ChunkStored{Cursor: []byte("k1"), Bytes: 10, Entries: 1, Final: true}, seedCfg)
	if r.State == SeedReady {
		t.Fatal("ready after the final chunk")
	}
	out := r.Apply(BufferDrained{Last: seedCut}, seedCfg)
	if !out.Sync || out.MarkReady || r.State != SeedSeeding || r.Phase != PhaseNone {
		t.Fatalf("drain = %+v state %d phase %d, want a sync and still seeding", out, r.State, r.Phase)
	}
	before := seedClone(r)
	if out := r.Apply(SyncDone{Err: errors.New("fsync failed")}, seedCfg); out.MarkReady || out.Restart != 4 || !seedSame(r, before) {
		t.Fatalf("failed sync = %+v record %+v, want a restart and no change", out, r)
	}
	out = r.Apply(SyncDone{}, seedCfg)
	if !out.MarkReady || !out.RerunSweeps || r.State != SeedReady || r.Dirty != 0 {
		t.Fatalf("clean sync = %+v state %d, want ready", out, r.State)
	}
	if !r.Cut.Zero() || len(r.Cursor) != 0 {
		t.Fatalf("ready record keeps cut %v cursor %q, want both cleared (seeding Formats 1)", r.Cut, r.Cursor)
	}
}

// TestSeedSnapshotCarriesDedupeEntries [skip 0x0E keys in the chunk /
// reintroduce a side channel]: a dedupe entry is one of Entries, applied in the
// chunk's own batch; DedupeEntries is a count and gates nothing.
func TestSeedSnapshotCarriesDedupeEntries(t *testing.T) {
	for _, c := range []ChunkStored{
		{Cursor: []byte("k2"), Bytes: 55, Entries: 3, DedupeEntries: 3},
		{Cursor: []byte("k2"), Bytes: 55, Entries: 3, DedupeEntries: 1},
		{Cursor: []byte("k2"), Bytes: 55, Entries: 3},
	} {
		r := seedStreamTo(t, false)
		out := r.Apply(c, seedCfg)
		if !out.ApplyChunk || string(r.Cursor) != "k2" || r.Dirty != 55 {
			t.Errorf("chunk with %d dedupe entries of %d: %+v cursor %q dirty %d, want it applied and counted",
				c.DedupeEntries, c.Entries, out, r.Cursor, r.Dirty)
		}
	}
}

// TestARetireIsNotCancellable [return to seeding from retiring]: a retired or
// retiring record takes no new attempt and no second retire order; it only goes
// forward to retiring and retired.
func TestARetireIsNotCancellable(t *testing.T) {
	for _, state := range []uint8{SeedRetired, SeedRetiring} {
		for _, nonEmpty := range []bool{false, true} {
			r := seedRec(state, PhaseNone)
			before := seedClone(r)
			out := r.Apply(AttemptWritten{SeedID: seedID2, Source: "src", LocalNonEmpty: nonEmpty}, seedCfg)
			if !seedSame(r, before) || out.Phase != PhaseNone {
				t.Errorf("state %d took a new attempt: %+v %+v", state, r, out)
			}
		}
	}
	r := seedRec(SeedRetiring, PhaseNone)
	r.Apply(RetireOrdered{}, seedCfg)
	if r.State != SeedRetiring {
		t.Errorf("a second retire order moved retiring to %d", r.State)
	}
	// The way out: delete finishes, the caller removes the record (seeding §8),
	// and the next attempt starts from the absent record.
	r.Apply(RetireDeleted{}, seedCfg)
	if r.State != SeedRetired {
		t.Errorf("RetireDeleted left state %d, want retired", r.State)
	}
	var absent SeedRecord
	if out := absent.Apply(AttemptWritten{SeedID: seedID2, Source: "src", LocalNonEmpty: false}, seedCfg); out.Phase != PhaseSnapshot {
		t.Errorf("a fresh attempt on the removed record = %+v", out)
	}
}

// TestReadyRerunsTheSweepsOnce [run the 'b'/'t' pass before ready, or never]:
// RerunSweeps rides on the one SyncDone that marks ready and on nothing else.
func TestReadyRerunsTheSweepsOnce(t *testing.T) {
	r := seedStreamTo(t, false)
	reruns := 0
	feed := func(ev SeedEvent) {
		if out := r.Apply(ev, seedCfg); out.RerunSweeps {
			reruns++
			if !out.MarkReady {
				t.Fatalf("sweeps re-run before ready by %T", ev)
			}
		}
	}
	feed(ChunkStored{Cursor: []byte("k1"), Bytes: 10, Entries: 1, Final: true})
	feed(SyncDone{})
	feed(BufferDrained{Last: seedCut})
	if reruns != 0 {
		t.Fatalf("sweeps re-run %d times before ready", reruns)
	}
	feed(SyncDone{})
	feed(SyncDone{})
	feed(BufferDrained{Last: seedCut})
	if reruns != 1 || r.State != SeedReady {
		t.Fatalf("sweeps re-run %d times, state %d; want exactly once and ready", reruns, r.State)
	}
}

// TestAQuietPairWithAnEarlierEpochCutSeeds: the pair's last record is (e-1,5)
// and the committed epoch is e, so the source's cut is the last position that
// exists, (e-1,5), not (e,0) (conv §10 rule 5). The seed must complete, with
// and without a first write of epoch e arriving above the cut.
func TestAQuietPairWithAnEarlierEpochCutSeeds(t *testing.T) {
	cfg := seedCfg
	cfg.Epoch = 7
	cut := seedPos(6, 5)
	start := func(t *testing.T) SeedRecord {
		t.Helper()
		var r SeedRecord
		r.Apply(AttemptWritten{SeedID: seedID1, Source: "src"}, cfg)
		if out := r.Apply(RegisterAccepted{Cut: cut}, cfg); out.Restart != 0 || r.Cut != cut || r.Applied != cut || r.Phase != PhaseSnapshot {
			t.Fatalf("a cut in epoch 6 under committed epoch 7: %+v record %+v, want it accepted", out, r)
		}
		if out := r.Apply(ChunkStored{Cursor: []byte("k1"), Bytes: 10, Entries: 1, Final: true}, cfg); out.Restart != 0 || r.Phase != PhaseBuffer {
			t.Fatalf("final chunk: %+v record %+v", out, r)
		}
		return r
	}
	t.Run("no write above the cut", func(t *testing.T) {
		r := start(t)
		if out := r.Apply(BufferDrained{Last: cut}, cfg); out.Restart != 0 || !out.Sync || r.Phase != PhaseNone {
			t.Fatalf("drain at the cut = %+v", out)
		}
		if out := r.Apply(SyncDone{}, cfg); !out.MarkReady || r.State != SeedReady || r.Applied != cut {
			t.Fatalf("sync = %+v record %+v, want ready at %v", out, r, cut)
		}
	})
	t.Run("first write of the committed epoch above the cut", func(t *testing.T) {
		r := start(t)
		first := seedPos(7, 1)
		if out := r.Apply(LiveWriteBuffered{Pos: first, Bytes: 10}, cfg); out.Restart != 0 || len(r.Buffered) != 1 {
			t.Fatalf("frame (7,1) = %+v record %+v", out, r)
		}
		if out := r.Apply(BufferApplyStep{Pos: first, Bytes: 10}, cfg); len(out.ApplyBuffered) != 1 || r.Applied != first {
			t.Fatalf("step (7,1) = %+v record %+v, want the opener applied after the earlier-epoch cut", out, r)
		}
		r.Apply(BufferDrained{Last: first}, cfg)
		if out := r.Apply(SyncDone{}, cfg); !out.MarkReady || r.Applied != first {
			t.Fatalf("sync = %+v record %+v", out, r)
		}
	})
}

// TestALaterEpochCutRestarts: a cut, a frame or a drain target from a later
// epoch than the committed one means the source is ahead of this node's view,
// and a seeding record that registered in an older committed epoch is stale.
func TestALaterEpochCutRestarts(t *testing.T) {
	cfg := seedCfg
	cfg.Epoch = 7
	var r SeedRecord
	r.Apply(AttemptWritten{SeedID: seedID1, Source: "src"}, cfg)
	before := seedClone(r)
	if out := r.Apply(RegisterAccepted{Cut: seedPos(8, 0)}, cfg); out.Restart != 3 || !seedSame(r, before) {
		t.Fatalf("a cut in epoch 8 under committed 7: %+v record %+v, want restart 3 and no change", out, r)
	}
	r.Apply(RegisterAccepted{Cut: seedPos(7, 0)}, cfg)
	for _, ev := range []SeedEvent{
		LiveWriteBuffered{Pos: seedPos(8, 1), Bytes: 10},
		BufferApplyStep{Pos: seedPos(8, 1), Bytes: 10},
		BufferDrained{Last: seedPos(8, 1)},
	} {
		before := seedClone(r)
		if out := r.Apply(ev, cfg); out.Restart != 3 || !seedSame(r, before) {
			t.Errorf("%T from epoch 8 under committed 7: %+v, want restart 3 and no change", ev, out)
		}
	}
	// The committed epoch moved after the registration: an epoch change mid-seed.
	cfg.Epoch = 8
	before = seedClone(r)
	if out := r.Apply(ChunkStored{Cursor: []byte("k1"), Bytes: 10, Entries: 1}, cfg); out.Restart != 3 || out.ApplyChunk || !seedSame(r, before) {
		t.Errorf("a chunk after the committed epoch moved: %+v, want restart 3", out)
	}
}
