package machine

import (
	"bytes"
	"slices"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// Phase is where a seeding attempt is in its fixed order: reset the range,
// stream the snapshot, apply the buffered live writes, then sync (seeding §4).
type Phase uint8

// The phases of a seeding attempt. PhaseNone is both "no attempt" and "the
// buffer is drained and a sync is owed".
const (
	PhaseNone Phase = iota
	PhaseReset
	PhaseSnapshot
	PhaseBuffer
)

// The record states of conv §12. Zero is "absent": the key does not exist.
const (
	SeedSeeding  uint8 = 1
	SeedReady    uint8 = 2
	SeedRetired  uint8 = 3
	SeedRetiring uint8 = 4
)

// SeedConfig carries the replication.seed_* tuning and the committed epoch.
type SeedConfig struct {
	SyncBytes   int64  // replication.seed_sync_bytes
	BufferBytes int64  // replication.seed_buffer_bytes
	WindowBytes int64  // replication.seed_window_bytes
	Epoch       uint64 // the committed epoch (conv §7): a seed payload from another epoch restarts
}

// SeedRecord is the per-(slot, database) record of conv §12 plus the
// in-memory buffer accounting of the running attempt. Persist the record after
// every Apply; Buffered, BufferedBytes and Dirty are not persisted.
type SeedRecord struct {
	State         uint8 // 1 seeding, 2 ready, 3 retired, 4 retiring; 0 is absent
	Phase         Phase
	Applied       cluster.Position // stays at Cut through the snapshot; advances only by BufferApplyStep
	Cut           cluster.Position
	SeedID        [16]byte
	Source        string
	Cursor        []byte             // last applied key; may reach storage.MaxKeyBytes
	Buffered      []cluster.Position // live writes held above the cut, ascending
	BufferedBytes int64
	Dirty         int64  // bytes applied since the last Sync
	RegEpoch      uint64 // committed epoch when the cut was registered; not persisted, set again by a resume

	bufferedSize []int64 // parallel to Buffered
}

// SeedEvent is one input to SeedRecord.Apply.
type SeedEvent interface{ isSeedEvent() }

// AttemptWritten says the caller wrote a fresh attempt record.
type AttemptWritten struct {
	SeedID        [16]byte
	Source        string
	LocalNonEmpty bool
}

// RegisterAccepted says the source accepted the registration at Cut.
type RegisterAccepted struct {
	Cut         cluster.Position
	WindowBytes int64
}

// RegisterRefused says the source refused or redirected the registration.
type RegisterRefused struct {
	Reason   uint8
	Redirect string
}

// ChunkStored says one snapshot chunk arrived.
type ChunkStored struct {
	Cursor                 []byte
	Bytes                  int64
	Entries, DedupeEntries int
	Final                  bool
}

// LiveWriteBuffered says one live write above the cut arrived.
type LiveWriteBuffered struct {
	Pos   cluster.Position
	Bytes int64
}

// BufferApplyStep asks to apply one position.
type BufferApplyStep struct {
	Pos   cluster.Position
	Bytes int64
}

// BufferDrained says the caller believes the buffer is empty through Last.
type BufferDrained struct{ Last cluster.Position }

// SyncDone reports a finished Sync.
type SyncDone struct{ Err error }

// RetireOrdered says the pair left the copy set.
type RetireOrdered struct{ Fence cluster.Position }

// DeleteStarted says the first range-delete batch is being written.
type DeleteStarted struct{}

// RetireDeleted says the range delete finished.
type RetireDeleted struct{}

func (AttemptWritten) isSeedEvent()    {}
func (RegisterAccepted) isSeedEvent()  {}
func (RegisterRefused) isSeedEvent()   {}
func (ChunkStored) isSeedEvent()       {}
func (LiveWriteBuffered) isSeedEvent() {}
func (BufferApplyStep) isSeedEvent()   {}
func (BufferDrained) isSeedEvent()     {}
func (SyncDone) isSeedEvent()          {}
func (RetireOrdered) isSeedEvent()     {}
func (DeleteStarted) isSeedEvent()     {}
func (RetireDeleted) isSeedEvent()     {}

// SeedOutcome is what one event asks the caller to do.
type SeedOutcome struct {
	Phase         Phase
	ApplyChunk    bool
	ApplyBuffered []cluster.Position
	Sync          bool
	MarkReady     bool
	RerunSweeps   bool
	AskCatchUp    *cluster.Position
	Restart       uint8
	Refusal       cluster.Code
}

// seedRow is one line of the decision table. Rows are mutually exclusive and
// together cover every event against every record, so which row fires never
// depends on their order; a test enumerates the classes and requires exactly
// one match for each.
type seedRow struct {
	name  string
	match func(*SeedRecord, SeedEvent, SeedConfig) bool
	run   func(*SeedRecord, SeedEvent, SeedConfig) SeedOutcome
}

// seedOn builds a row for one event type.
func seedOn[E SeedEvent](name string, match func(*SeedRecord, E, SeedConfig) bool, run func(*SeedRecord, E, SeedConfig) SeedOutcome) seedRow {
	return seedRow{
		name: name,
		match: func(s *SeedRecord, ev SeedEvent, cfg SeedConfig) bool {
			e, ok := ev.(E)
			return ok && match(s, e, cfg)
		},
		run: func(s *SeedRecord, ev SeedEvent, cfg SeedConfig) SeedOutcome { return run(s, ev.(E), cfg) },
	}
}

// seedAttemptEvent is every event that only means something inside a running
// attempt; on a record that is not seeding it is stale and changes nothing.
func seedAttemptEvent(ev SeedEvent) bool {
	switch ev.(type) {
	case RegisterAccepted, RegisterRefused, ChunkStored, LiveWriteBuffered, BufferApplyStep, BufferDrained, SyncDone:
		return true
	}
	return false
}

// seedSeeding is a seeding record in a phase the machine knows.
func seedSeeding(s *SeedRecord) bool { return s.State == SeedSeeding && s.Phase <= PhaseBuffer }

// seedNoChunkYet is a seeding record that has applied no chunk: before the
// reset is confirmed, or registered with an empty cursor. A new cut is safe
// there, because the range holds nothing of the old one.
func seedNoChunkYet(s *SeedRecord) bool {
	return s.Phase == PhaseReset || (s.Phase == PhaseSnapshot && len(s.Cursor) == 0)
}

// seedStaleCut is a registered attempt that registered in an older committed
// epoch than the current one: the committed epoch moved mid-seed and the new
// owner's history is not comparable (conv §7, S7). The cut itself may be in an
// earlier epoch than the one it registered in: a quiet pair's cut is the last
// position that exists (conv §10 rule 5), so only RegEpoch says when it was
// taken. RegEpoch is runtime state; a record loaded from disk is stale until
// the caller's resume registers it again.
func seedStaleCut(s *SeedRecord, cfg SeedConfig) bool {
	return seedSeeding(s) && s.Phase != PhaseReset && s.RegEpoch != cfg.Epoch
}

// seedBadEpoch is an event whose payload position is from a later epoch than
// the committed one (the source is ahead of this node's view), or whose
// attempt is stale. A position from an equal or earlier epoch proceeds.
func seedBadEpoch(s *SeedRecord, cfg SeedConfig, payload cluster.Position) bool {
	return seedSeeding(s) && (payload.Epoch > cfg.Epoch || seedStaleCut(s, cfg))
}

// seedNext is the position a registered attempt applies next: the following
// seq of the applied epoch, or the opener (e,1) of the committed epoch when the
// applied position, usually the cut, is still in an earlier epoch.
func seedNext(s *SeedRecord, cfg SeedConfig) cluster.Position {
	if s.Applied.Epoch < cfg.Epoch {
		return cluster.Position{Epoch: cfg.Epoch, Seq: 1}
	}
	return cluster.Position{Epoch: s.Applied.Epoch, Seq: s.Applied.Seq + 1}
}

// seedFloor is the highest position the record already holds or buffers.
func seedFloor(s *SeedRecord) cluster.Position {
	if n := len(s.Buffered); n > 0 && s.Applied.Less(s.Buffered[n-1]) {
		return s.Buffered[n-1]
	}
	return s.Applied
}

// seedAdvances reports whether a chunk moves the snapshot forward: it ends an
// empty snapshot, or its last key is past the cursor.
func seedAdvances(s *SeedRecord, ev ChunkStored) bool {
	if ev.Final && ev.Entries == 0 {
		return true
	}
	return ev.Entries > 0 && len(ev.Cursor) > 0 && (len(s.Cursor) == 0 || bytes.Compare(ev.Cursor, s.Cursor) > 0)
}

// seedClearRun drops the in-memory and cursor state of an attempt.
func seedClearRun(s *SeedRecord) {
	s.Cursor, s.Buffered, s.bufferedSize, s.BufferedBytes, s.Dirty, s.RegEpoch = nil, nil, nil, 0, 0, 0
}

// seedBegin writes a fresh attempt: the cut, the applied position and the
// cursor are zeroed, because the range is about to be reset or is empty.
func seedBegin(s *SeedRecord, ev AttemptWritten, phase Phase) SeedOutcome {
	s.State, s.Phase, s.SeedID, s.Source = SeedSeeding, phase, ev.SeedID, ev.Source
	s.Cut, s.Applied = cluster.Position{}, cluster.Position{}
	seedClearRun(s)
	return SeedOutcome{}
}

// seedNeedsReset is true when the range may hold anything: the caller says so,
// the record is ready, or the record shows a previous attempt left data or owes
// a reset. The only seeding record that owes none is a registered one with an
// empty cursor.
func seedNeedsReset(s *SeedRecord, ev AttemptWritten) bool {
	return ev.LocalNonEmpty || s.State == SeedReady ||
		(s.State == SeedSeeding && (s.Phase != PhaseSnapshot || len(s.Cursor) > 0))
}

func seedNone(*SeedRecord, SeedEvent, SeedConfig) SeedOutcome { return SeedOutcome{} }

func seedRestart(reason uint8) func(*SeedRecord, SeedEvent, SeedConfig) SeedOutcome {
	return func(*SeedRecord, SeedEvent, SeedConfig) SeedOutcome { return SeedOutcome{Restart: reason} }
}

// seedSizeAt is the recorded byte size of the i-th buffered position.
func seedSizeAt(s *SeedRecord, i int) int64 {
	if i < len(s.bufferedSize) {
		return s.bufferedSize[i]
	}
	return 0
}

// seedDropApplied removes every buffered position at or below Applied.
func seedDropApplied(s *SeedRecord) {
	n := 0
	for n < len(s.Buffered) && !s.Applied.Less(s.Buffered[n]) {
		s.BufferedBytes -= seedSizeAt(s, n)
		n++
	}
	if n == 0 {
		return
	}
	rest, sizes := slices.Clone(s.Buffered[n:]), slices.Clone(s.bufferedSize[min(n, len(s.bufferedSize)):])
	s.Buffered, s.bufferedSize = nil, nil
	if len(rest) > 0 {
		s.Buffered, s.bufferedSize = rest, sizes
	}
}

func seedRetire(s *SeedRecord) {
	s.State, s.Phase = SeedRetired, PhaseNone
	seedClearRun(s)
}

// seedRows is the whole decision table (build doc §4.5). A row marked "gap"
// fills a case the brief's table is silent on, with the reading that never
// lets a record become ready for a range it does not completely hold.
var seedRows = []seedRow{
	// Events that only mean something inside an attempt.
	{"attempt-event/not-seeding", // gap: stale on a record with no attempt
		func(s *SeedRecord, ev SeedEvent, _ SeedConfig) bool {
			return seedAttemptEvent(ev) && s.State != SeedSeeding
		}, seedNone},
	{"attempt-event/unknown-phase", // gap: a record the machine cannot read starts over
		func(s *SeedRecord, ev SeedEvent, _ SeedConfig) bool {
			return seedAttemptEvent(ev) && s.State == SeedSeeding && s.Phase > PhaseBuffer
		}, seedRestart(4)},

	// AttemptWritten.
	seedOn("attempt/retire-is-not-cancellable",
		func(s *SeedRecord, _ AttemptWritten, _ SeedConfig) bool { return s.State > SeedReady },
		func(*SeedRecord, AttemptWritten, SeedConfig) SeedOutcome { return SeedOutcome{} }),
	seedOn("attempt/reset",
		func(s *SeedRecord, ev AttemptWritten, _ SeedConfig) bool {
			return s.State <= SeedReady && seedNeedsReset(s, ev)
		},
		func(s *SeedRecord, ev AttemptWritten, _ SeedConfig) SeedOutcome { return seedBegin(s, ev, PhaseReset) }),
	seedOn("attempt/snapshot",
		func(s *SeedRecord, ev AttemptWritten, _ SeedConfig) bool {
			return s.State <= SeedReady && !seedNeedsReset(s, ev)
		},
		func(s *SeedRecord, ev AttemptWritten, _ SeedConfig) SeedOutcome {
			return seedBegin(s, ev, PhaseSnapshot)
		}),

	// RegisterAccepted.
	seedOn("register/epoch-differs",
		func(s *SeedRecord, ev RegisterAccepted, cfg SeedConfig) bool {
			return seedSeeding(s) && ev.Cut.Epoch > cfg.Epoch // the source is ahead; an earlier epoch is a quiet pair's cut
		},
		func(*SeedRecord, RegisterAccepted, SeedConfig) SeedOutcome { return SeedOutcome{Restart: 3} }),
	seedOn("register/fresh", // no chunk applied yet: any cut is acceptable
		func(s *SeedRecord, ev RegisterAccepted, cfg SeedConfig) bool {
			return seedSeeding(s) && ev.Cut.Epoch <= cfg.Epoch && seedNoChunkYet(s)
		},
		func(s *SeedRecord, ev RegisterAccepted, cfg SeedConfig) SeedOutcome {
			s.RegEpoch = cfg.Epoch
			if ev.Cut != s.Cut {
				// Frames of an older stream are inside the new snapshot or lost
				// to it; a hole is caught by the apply, never smoothed.
				s.Buffered, s.bufferedSize, s.BufferedBytes = nil, nil, 0
			}
			s.Phase, s.Cut, s.Applied = PhaseSnapshot, ev.Cut, ev.Cut
			return SeedOutcome{}
		}),
	seedOn("register/resume-same-cut", // gap
		func(s *SeedRecord, ev RegisterAccepted, cfg SeedConfig) bool {
			return seedSeeding(s) && ev.Cut.Epoch <= cfg.Epoch && !seedNoChunkYet(s) && ev.Cut == s.Cut
		},
		func(s *SeedRecord, _ RegisterAccepted, cfg SeedConfig) SeedOutcome {
			s.RegEpoch = cfg.Epoch // a resume registers again after a restart
			if s.Phase == PhaseBuffer {
				// The buffer only ever lived in memory (contract 9), so a record reloaded in the
				// buffer phase is a snapshot on its cut: the source continues after the cursor,
				// and its empty final chunk turns the phase back to buffer for the drain.
				s.Phase = PhaseSnapshot
				s.Buffered, s.bufferedSize, s.BufferedBytes = nil, nil, 0
			}
			return SeedOutcome{}
		}),
	seedOn("register/cut-moved-over-applied-data", // gap: the range would mix two cuts
		func(s *SeedRecord, ev RegisterAccepted, cfg SeedConfig) bool {
			return seedSeeding(s) && ev.Cut.Epoch <= cfg.Epoch && !seedNoChunkYet(s) && ev.Cut != s.Cut
		},
		func(*SeedRecord, RegisterAccepted, SeedConfig) SeedOutcome { return SeedOutcome{Restart: 4} }),

	// RegisterRefused.
	seedOn("refused/redirect",
		func(s *SeedRecord, ev RegisterRefused, _ SeedConfig) bool {
			return seedSeeding(s) && ev.Reason == 1 && ev.Redirect != ""
		},
		func(s *SeedRecord, ev RegisterRefused, _ SeedConfig) SeedOutcome {
			s.Source = ev.Redirect
			return SeedOutcome{}
		}),
	seedOn("refused/not-a-holder-or-not-current",
		func(s *SeedRecord, ev RegisterRefused, _ SeedConfig) bool {
			return seedSeeding(s) && (ev.Reason == 3 || ev.Reason == 4)
		},
		func(*SeedRecord, RegisterRefused, SeedConfig) SeedOutcome { return SeedOutcome{Restart: 1} }),
	seedOn("refused/retry-later", // gap: disk, database dropped, mask conflict, unknown reason
		func(s *SeedRecord, ev RegisterRefused, _ SeedConfig) bool {
			return seedSeeding(s) && ev.Reason != 3 && ev.Reason != 4 && (ev.Reason != 1 || ev.Redirect == "")
		},
		func(*SeedRecord, RegisterRefused, SeedConfig) SeedOutcome { return SeedOutcome{} }),

	// ChunkStored.
	seedOn("chunk/epoch-differs",
		func(s *SeedRecord, _ ChunkStored, cfg SeedConfig) bool { return seedStaleCut(s, cfg) },
		func(*SeedRecord, ChunkStored, SeedConfig) SeedOutcome { return SeedOutcome{Restart: 3} }),
	seedOn("chunk/before-the-reset-is-done",
		func(s *SeedRecord, _ ChunkStored, _ SeedConfig) bool { return seedSeeding(s) && s.Phase == PhaseReset },
		func(*SeedRecord, ChunkStored, SeedConfig) SeedOutcome { return SeedOutcome{} }),
	seedOn("chunk/late", // gap: past the snapshot a chunk would overwrite newer data
		func(s *SeedRecord, _ ChunkStored, cfg SeedConfig) bool {
			return seedSeeding(s) && !seedStaleCut(s, cfg) && (s.Phase == PhaseBuffer || s.Phase == PhaseNone)
		},
		func(*SeedRecord, ChunkStored, SeedConfig) SeedOutcome { return SeedOutcome{} }),
	seedOn("chunk/duplicate", // gap
		func(s *SeedRecord, ev ChunkStored, cfg SeedConfig) bool {
			return seedSeeding(s) && !seedStaleCut(s, cfg) && s.Phase == PhaseSnapshot && !seedAdvances(s, ev)
		},
		func(*SeedRecord, ChunkStored, SeedConfig) SeedOutcome { return SeedOutcome{} }),
	seedOn("chunk/apply",
		func(s *SeedRecord, ev ChunkStored, cfg SeedConfig) bool {
			return seedSeeding(s) && !seedStaleCut(s, cfg) && s.Phase == PhaseSnapshot && seedAdvances(s, ev) && !ev.Final
		},
		func(s *SeedRecord, ev ChunkStored, cfg SeedConfig) SeedOutcome {
			// Every entry, a dedupe entry included, is applied in the chunk's
			// own batch with the cursor; DedupeEntries is a count for the metric.
			s.Cursor = slices.Clone(ev.Cursor)
			s.Dirty += ev.Bytes
			return SeedOutcome{ApplyChunk: true, Sync: s.Dirty >= cfg.SyncBytes}
		}),
	seedOn("chunk/final",
		func(s *SeedRecord, ev ChunkStored, cfg SeedConfig) bool {
			return seedSeeding(s) && !seedStaleCut(s, cfg) && s.Phase == PhaseSnapshot && seedAdvances(s, ev) && ev.Final
		},
		func(s *SeedRecord, ev ChunkStored, cfg SeedConfig) SeedOutcome {
			if len(ev.Cursor) > 0 {
				s.Cursor = slices.Clone(ev.Cursor)
			}
			s.Dirty += ev.Bytes
			s.Phase = PhaseBuffer // gap: the brief never says what ends the snapshot phase
			// Synced on the same rule as any chunk, not always: a record reloaded in the buffer
			// phase resumes as a snapshot on its cut (register/resume-same-cut), so a sync here
			// would only save re-fetching the chunk after a crash, and the ready record's synced
			// commit makes it durable anyway (seeding-speed-note.md).
			return SeedOutcome{ApplyChunk: true, Sync: s.Dirty >= cfg.SyncBytes}
		}),

	// LiveWriteBuffered.
	seedOn("live/epoch-differs",
		func(s *SeedRecord, ev LiveWriteBuffered, cfg SeedConfig) bool { return seedBadEpoch(s, cfg, ev.Pos) },
		func(*SeedRecord, LiveWriteBuffered, SeedConfig) SeedOutcome { return SeedOutcome{Restart: 3} }),
	seedOn("live/not-collecting", // gap: before the cut exists, or after the drain
		func(s *SeedRecord, ev LiveWriteBuffered, cfg SeedConfig) bool {
			return seedSeeding(s) && !seedBadEpoch(s, cfg, ev.Pos) && (s.Phase == PhaseReset || s.Phase == PhaseNone)
		},
		func(*SeedRecord, LiveWriteBuffered, SeedConfig) SeedOutcome { return SeedOutcome{} }),
	seedOn("live/duplicate", // gap: at or below the cut, applied, or already held
		func(s *SeedRecord, ev LiveWriteBuffered, cfg SeedConfig) bool {
			return seedSeeding(s) && !seedBadEpoch(s, cfg, ev.Pos) && (s.Phase == PhaseSnapshot || s.Phase == PhaseBuffer) && !seedFloor(s).Less(ev.Pos)
		},
		func(*SeedRecord, LiveWriteBuffered, SeedConfig) SeedOutcome { return SeedOutcome{} }),
	seedOn("live/over-the-buffer-bound",
		func(s *SeedRecord, ev LiveWriteBuffered, cfg SeedConfig) bool {
			return seedSeeding(s) && !seedBadEpoch(s, cfg, ev.Pos) && (s.Phase == PhaseSnapshot || s.Phase == PhaseBuffer) &&
				seedFloor(s).Less(ev.Pos) && s.BufferedBytes+ev.Bytes > cfg.BufferBytes
		},
		func(*SeedRecord, LiveWriteBuffered, SeedConfig) SeedOutcome { return SeedOutcome{Restart: 4} }),
	seedOn("live/buffer",
		func(s *SeedRecord, ev LiveWriteBuffered, cfg SeedConfig) bool {
			return seedSeeding(s) && !seedBadEpoch(s, cfg, ev.Pos) && (s.Phase == PhaseSnapshot || s.Phase == PhaseBuffer) &&
				seedFloor(s).Less(ev.Pos) && s.BufferedBytes+ev.Bytes <= cfg.BufferBytes
		},
		func(s *SeedRecord, ev LiveWriteBuffered, _ SeedConfig) SeedOutcome {
			s.Buffered = append(s.Buffered, ev.Pos)
			s.bufferedSize = append(s.bufferedSize, ev.Bytes)
			s.BufferedBytes += ev.Bytes
			return SeedOutcome{}
		}),

	// BufferApplyStep.
	seedOn("step/epoch-differs",
		func(s *SeedRecord, ev BufferApplyStep, cfg SeedConfig) bool { return seedBadEpoch(s, cfg, ev.Pos) },
		func(*SeedRecord, BufferApplyStep, SeedConfig) SeedOutcome { return SeedOutcome{Restart: 3} }),
	seedOn("step/not-in-the-buffer-phase",
		func(s *SeedRecord, ev BufferApplyStep, cfg SeedConfig) bool {
			return seedSeeding(s) && !seedBadEpoch(s, cfg, ev.Pos) && s.Phase != PhaseBuffer
		},
		func(*SeedRecord, BufferApplyStep, SeedConfig) SeedOutcome { return SeedOutcome{Restart: 4} }),
	seedOn("step/already-applied", // gap: nothing to do, and not a hole
		func(s *SeedRecord, ev BufferApplyStep, cfg SeedConfig) bool {
			return seedSeeding(s) && !seedBadEpoch(s, cfg, ev.Pos) && s.Phase == PhaseBuffer && !s.Applied.Less(ev.Pos)
		},
		func(*SeedRecord, BufferApplyStep, SeedConfig) SeedOutcome { return SeedOutcome{} }),
	seedOn("step/gap", // asked for, never smoothed
		func(s *SeedRecord, ev BufferApplyStep, cfg SeedConfig) bool {
			return seedSeeding(s) && !seedBadEpoch(s, cfg, ev.Pos) && s.Phase == PhaseBuffer && s.Applied.Less(ev.Pos) && ev.Pos != seedNext(s, cfg)
		},
		func(s *SeedRecord, _ BufferApplyStep, cfg SeedConfig) SeedOutcome {
			next := seedNext(s, cfg)
			return SeedOutcome{AskCatchUp: &next}
		}),
	seedOn("step/apply",
		func(s *SeedRecord, ev BufferApplyStep, cfg SeedConfig) bool {
			return seedSeeding(s) && !seedBadEpoch(s, cfg, ev.Pos) && s.Phase == PhaseBuffer && ev.Pos == seedNext(s, cfg)
		},
		func(s *SeedRecord, ev BufferApplyStep, cfg SeedConfig) SeedOutcome {
			s.Applied = ev.Pos
			s.Dirty += ev.Bytes
			seedDropApplied(s)
			return SeedOutcome{ApplyBuffered: []cluster.Position{ev.Pos}, Sync: s.Dirty >= cfg.SyncBytes}
		}),

	// BufferDrained.
	seedOn("drained/epoch-differs",
		func(s *SeedRecord, ev BufferDrained, cfg SeedConfig) bool { return seedBadEpoch(s, cfg, ev.Last) },
		func(*SeedRecord, BufferDrained, SeedConfig) SeedOutcome { return SeedOutcome{Restart: 3} }),
	seedOn("drained/not-buffering", // gap: the snapshot is not complete, or the drain already happened
		func(s *SeedRecord, ev BufferDrained, cfg SeedConfig) bool {
			return seedSeeding(s) && !seedBadEpoch(s, cfg, ev.Last) && s.Phase != PhaseBuffer
		},
		func(*SeedRecord, BufferDrained, SeedConfig) SeedOutcome { return SeedOutcome{} }),
	seedOn("drained/positions-still-held", // gap
		func(s *SeedRecord, ev BufferDrained, cfg SeedConfig) bool {
			return seedSeeding(s) && !seedBadEpoch(s, cfg, ev.Last) && s.Phase == PhaseBuffer && len(s.Buffered) > 0
		},
		func(*SeedRecord, BufferDrained, SeedConfig) SeedOutcome { return SeedOutcome{} }),
	seedOn("drained/behind-the-source-tail", // gap: the source appended past what this node applied
		func(s *SeedRecord, ev BufferDrained, cfg SeedConfig) bool {
			return seedSeeding(s) && !seedBadEpoch(s, cfg, ev.Last) && s.Phase == PhaseBuffer && len(s.Buffered) == 0 && s.Applied.Less(ev.Last)
		},
		func(s *SeedRecord, _ BufferDrained, cfg SeedConfig) SeedOutcome {
			next := seedNext(s, cfg)
			return SeedOutcome{AskCatchUp: &next}
		}),
	seedOn("drained/done",
		func(s *SeedRecord, ev BufferDrained, cfg SeedConfig) bool {
			return seedSeeding(s) && !seedBadEpoch(s, cfg, ev.Last) && s.Phase == PhaseBuffer && len(s.Buffered) == 0 && !s.Applied.Less(ev.Last)
		},
		func(s *SeedRecord, _ BufferDrained, _ SeedConfig) SeedOutcome {
			s.Phase = PhaseNone
			// Sync here is "make the attempt durable, then feed SyncDone" (contract 2), not a
			// request to sync this record's own commit: the ring's caller commits the drained
			// record without a sync and meets the request with the ready record's synced
			// commit. The model test drives SyncDone from this flag.
			return SeedOutcome{Sync: true}
		}),

	// SyncDone.
	seedOn("sync/epoch-differs",
		func(s *SeedRecord, _ SyncDone, cfg SeedConfig) bool { return seedStaleCut(s, cfg) },
		func(*SeedRecord, SyncDone, SeedConfig) SeedOutcome { return SeedOutcome{Restart: 3} }),
	seedOn("sync/failed",
		func(s *SeedRecord, ev SyncDone, cfg SeedConfig) bool {
			return seedSeeding(s) && !seedStaleCut(s, cfg) && ev.Err != nil
		},
		func(*SeedRecord, SyncDone, SeedConfig) SeedOutcome { return SeedOutcome{Restart: 4} }),
	seedOn("sync/ready", // the only row that ever sets MarkReady
		func(s *SeedRecord, ev SyncDone, cfg SeedConfig) bool {
			return seedSeeding(s) && !seedStaleCut(s, cfg) && ev.Err == nil && s.Phase == PhaseNone
		},
		func(s *SeedRecord, _ SyncDone, _ SeedConfig) SeedOutcome {
			s.State = SeedReady
			s.Cut = cluster.Position{} // a ready record keeps no cut and no cursor (seeding Formats 1)
			seedClearRun(s)
			return SeedOutcome{MarkReady: true, RerunSweeps: true}
		}),
	seedOn("sync/progress", // gap: a sync that is not the last one
		func(s *SeedRecord, ev SyncDone, cfg SeedConfig) bool {
			return seedSeeding(s) && !seedStaleCut(s, cfg) && ev.Err == nil && s.Phase != PhaseNone
		},
		func(s *SeedRecord, _ SyncDone, _ SeedConfig) SeedOutcome {
			s.Dirty = 0
			return SeedOutcome{}
		}),

	// RetireOrdered, DeleteStarted, RetireDeleted.
	seedOn("retire/ordered",
		func(s *SeedRecord, _ RetireOrdered, _ SeedConfig) bool {
			return s.State == SeedSeeding || s.State == SeedReady || s.State == SeedRetired
		},
		func(s *SeedRecord, _ RetireOrdered, _ SeedConfig) SeedOutcome {
			seedRetire(s)
			return SeedOutcome{}
		}),
	seedOn("retire/ordered-nothing-to-retire", // retiring is not cancellable; absent has nothing
		func(s *SeedRecord, _ RetireOrdered, _ SeedConfig) bool {
			return s.State != SeedSeeding && s.State != SeedReady && s.State != SeedRetired
		},
		func(*SeedRecord, RetireOrdered, SeedConfig) SeedOutcome { return SeedOutcome{} }),
	seedOn("retire/delete-started",
		func(s *SeedRecord, _ DeleteStarted, _ SeedConfig) bool { return s.State == SeedRetired },
		func(s *SeedRecord, _ DeleteStarted, _ SeedConfig) SeedOutcome {
			s.State = SeedRetiring
			return SeedOutcome{}
		}),
	seedOn("retire/delete-started-not-retired", // gap: a ready or seeding copy is never deleted
		func(s *SeedRecord, _ DeleteStarted, _ SeedConfig) bool { return s.State != SeedRetired },
		func(*SeedRecord, DeleteStarted, SeedConfig) SeedOutcome { return SeedOutcome{} }),
	seedOn("retire/deleted",
		func(s *SeedRecord, _ RetireDeleted, _ SeedConfig) bool {
			return s.State >= SeedSeeding && s.State <= SeedRetiring
		},
		func(s *SeedRecord, _ RetireDeleted, _ SeedConfig) SeedOutcome {
			seedRetire(s)
			return SeedOutcome{}
		}),
	seedOn("retire/deleted-nothing-held", // gap: an absent record is not made retired by a repeated event
		func(s *SeedRecord, _ RetireDeleted, _ SeedConfig) bool {
			return s.State < SeedSeeding || s.State > SeedRetiring
		},
		func(*SeedRecord, RetireDeleted, SeedConfig) SeedOutcome { return SeedOutcome{} }),
}

// matchingSeedRows names every row whose condition holds for ev.
func matchingSeedRows(s *SeedRecord, ev SeedEvent, cfg SeedConfig) []string {
	var names []string
	for _, row := range seedRows {
		if row.match(s, ev, cfg) {
			names = append(names, row.name)
		}
	}
	return names
}

// Apply feeds one event to the record and says what the caller must do. The
// caller persists the record after every Apply; the outcome's data work
// (a chunk, a buffered position) goes in the same batch as that write. A
// restart or a refusal leaves the record exactly as it was. SeedOutcome.Phase
// is the record's phase after the event.
//
// CONTRACT (the table relies on all of this; U44 and U45 must keep it):
//
//  1. The caller deletes the pair's range BEFORE it feeds RegisterAccepted,
//     when an outcome says PhaseReset. RegisterAccepted is therefore the
//     evidence that the reset happened, and no event reports it separately.
//     Feeding it first and deleting afterwards can resurrect keys after a
//     crash in between. The caller may ask the source first and delete once it
//     has the accept in hand (the puller does, BUG-0819: a pull does not delete
//     a range while nothing can be pulled); what matters is the order of the
//     delete and this event. An attempt that is never registered may still
//     lose its range without this event: when the pair leaves the node's copy
//     set the caller deletes the range and removes the record (the left seed's
//     drop), with no source asked.
//  2. SyncDone is fed synchronously, once the data the last Sync outcome
//     covers is written, with one sync outstanding at a time (it carries no
//     id). The sync that outcome asked for may be the commit that writes
//     SyncDone's own result: the ring's caller commits the ready record with a
//     sync, which makes every earlier write durable with it, and acts on the
//     ready record only after that commit returns. A record loaded from disk as
//     seeding in PhaseNone owes the same sync.
//  3. After a restart the caller resumes with RegisterAccepted on the same cut.
//     That sets RegEpoch again; until then every attempt event is stale and
//     returns Restart=3.
//  4. Restart=4 always means a fresh attempt with a reset. After RetireDeleted
//     the record stays retired; only the caller removing the record (seeding §8)
//     returns it to absent, and only then does AttemptWritten start a seed.
//  5. SeedConfig.Epoch is the committed epoch. A cut, frame or drain target
//     from a LATER epoch is Restart=3; an equal or earlier epoch proceeds,
//     because a quiet pair's cut is its last position, which may belong to an
//     earlier epoch. The first position applied above such a cut is (Epoch,1).
func (s *SeedRecord) Apply(ev SeedEvent, cfg SeedConfig) SeedOutcome {
	for _, row := range seedRows {
		if row.match(s, ev, cfg) {
			out := row.run(s, ev, cfg)
			out.Phase = s.Phase
			return out
		}
	}
	// Unreachable: TestSeedEveryEventClassHasExactlyOneRow proves every event has a row.
	return SeedOutcome{Phase: s.Phase}
}
