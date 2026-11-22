package machine

import "testing"

// TestAnEmptyFinalChunkAfterAResumeEndsTheSnapshot: a resumed attempt applies nothing
// when it registered its cursor at the range's last key, so the source answers the next
// ack with {Entries: 0, Final: true}. That chunk ends the snapshot the same way a final
// chunk with entries does, and it moves no cursor. [BREAK: require entries in a final chunk]
// It asks for no sync with nothing dirty: the ready record's synced commit covers it.
func TestAnEmptyFinalChunkAfterAResumeEndsTheSnapshot(t *testing.T) {
	rec := seedRec(SeedSeeding, PhaseSnapshot, seedWithCursor("key-0100"))
	out := rec.Apply(ChunkStored{Final: true, Entries: 0}, seedCfg)
	if out.Restart != 0 {
		t.Fatalf("the empty final chunk restarted the attempt (reason %d); it is the end of the snapshot", out.Restart)
	}
	if out.Phase != PhaseBuffer {
		t.Fatalf("after the empty final chunk the phase is %v, want %v: the snapshot is complete", out.Phase, PhaseBuffer)
	}
	if !out.ApplyChunk {
		t.Fatalf("the empty final chunk asks ApplyChunk=false, want true: it ends the snapshot in a commit of its own")
	}
	if out.Sync {
		t.Fatalf("the empty final chunk asks for a sync with nothing dirty: a final chunk syncs on seed_sync_bytes like any " +
			"other, and the ready record's synced commit makes it durable (seeding-speed-note.md)")
	}
	if string(rec.Cursor) != "key-0100" {
		t.Fatalf("the empty chunk moved the cursor to %q, want key-0100: a chunk with no entries names no key", rec.Cursor)
	}
}

// TestARecordLoadedInBufferResumesAsASnapshot: a record reloaded in the buffer phase is a
// snapshot on the same cut, because the attempt buffer only ever lived in memory. The
// registration at that cut returns it to the snapshot phase with nothing buffered, so the
// source's empty final chunk (an ack past the cursor) can end it and the drain can run.
// [BREAK: leave the phase buffer]
func TestARecordLoadedInBufferResumesAsASnapshot(t *testing.T) {
	rec := seedRec(SeedSeeding, PhaseBuffer, seedWithBuffered(40, 101, 102), seedWithCursor("key-0100"))
	out := rec.Apply(RegisterAccepted{Cut: seedCut, WindowBytes: 1 << 20}, seedCfg)
	if out.Restart != 0 {
		t.Fatalf("the resume at the same cut restarted the attempt (reason %d)", out.Restart)
	}
	if rec.Phase != PhaseSnapshot {
		t.Fatalf("the reloaded record is in phase %v after registering again, want %v: a buffer-phase record is a snapshot on its cut", rec.Phase, PhaseSnapshot)
	}
	if len(rec.Buffered) != 0 || rec.BufferedBytes != 0 {
		t.Fatalf("the reloaded record still holds %d buffered positions (%d bytes); the buffer was memory only", len(rec.Buffered), rec.BufferedBytes)
	}
	if rec.Applied != seedCut || rec.Cut != seedCut || rec.RegEpoch != seedCfg.Epoch {
		t.Fatalf("after the resume the record is cut %v applied %v registered in epoch %d, want %v, %v and %d",
			rec.Cut, rec.Applied, rec.RegEpoch, seedCut, seedCut, seedCfg.Epoch)
	}
	// The ack that follows the empty resume answer ends the snapshot again, and the drain
	// starts from the cut: nothing was applied past it.
	final := rec.Apply(ChunkStored{Final: true, Entries: 0}, seedCfg)
	if final.Restart != 0 || final.Phase != PhaseBuffer || !final.ApplyChunk {
		t.Fatalf("the empty final chunk after the resume gave restart %d, phase %v, ApplyChunk %v; want none, %v and true",
			final.Restart, final.Phase, final.ApplyChunk, PhaseBuffer)
	}
	if rec.Applied != seedCut {
		t.Fatalf("the resume applied up to %v, want the cut %v: the snapshot moved nothing", rec.Applied, seedCut)
	}
}
