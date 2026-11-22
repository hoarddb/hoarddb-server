package machine

import "github.com/hoarddb/hoarddb-server/cluster"

// DedupeKey names one client operation: the request id the coordinator minted and the
// operation's index inside its batch (conventions §18). A batch carries at most
// 255 operations, so the index fits one byte.
type DedupeKey struct {
	RequestID [16]byte
	OpIndex   uint8
}

// DedupeEntry is one dedupe entry as it sits in the pair's range.
//
// Ordinal is the pair's cumulative count of applied records at the instant this
// write was applied. It is what the window is measured in, because a position
// cannot be: seq restarts at 1 in every epoch, so an entry written under an
// earlier epoch has no comparable seq at a promoted owner.
type DedupeEntry struct {
	Key     DedupeKey
	Pos     cluster.Position
	Ordinal uint64
	// Fingerprint is a hash of what the operation asked (class, bucket, document and
	// body), so a retry that reuses the request id for a different operation is
	// refused instead of answered with the first one's result.
	Fingerprint uint64
	Result      []byte
}

// WindowConfig carries replication.dedupe_window and the result cap.
type WindowConfig struct {
	Window    uint64
	MaxResult int
}

// Window is the dedupe state of one (slot, database) pair on one node.
type Window struct {
	// Ordinal counts the records this node has applied for the pair. An epoch
	// never resets it.
	Ordinal uint64
}

// WindowEvent is one fact the caller feeds the window.
type WindowEvent interface{ isWindowEvent() }

// DedupeWrite reports a newly applied record.
type DedupeWrite struct {
	Key         DedupeKey
	Pos         cluster.Position
	Fingerprint uint64
	Result      []byte
}

// LookupGiven carries the one key the caller read.
type LookupGiven struct{ Entry *DedupeEntry }

// SweepBatch carries a bounded scan of the pair's dedupe keys.
type SweepBatch struct {
	Candidates []DedupeEntry
	Budget     int
}

// OrdinalCut sets the ordinal from a seed cut or the persisted pair record.
type OrdinalCut struct{ Ordinal uint64 }

func (DedupeWrite) isWindowEvent() {}
func (LookupGiven) isWindowEvent() {}
func (SweepBatch) isWindowEvent()  {}
func (OrdinalCut) isWindowEvent()  {}

// WindowDecision is what one event asks the caller to do.
type WindowDecision struct {
	Live     bool
	Recorded *DedupeEntry
	Store    bool
	Write    *DedupeEntry
	Refuse   bool
	Delete   []DedupeKey
}

// windowRow is one row of the decision table. Rows are mutually exclusive, so
// which row fires never depends on their order; a test counts the matches for
// every input class and requires exactly one.
type windowRow struct {
	name  string
	match func(w *Window, ev WindowEvent, cfg WindowConfig) bool
	run   func(w *Window, ev WindowEvent, cfg WindowConfig) WindowDecision
}

// span is the live span in records. A configured 0 would make every entry dead
// and a retried pop apply twice, so the newest write is always live (config
// must refuse 0 as well).
func (c WindowConfig) span() uint64 { return max(c.Window, 1) }

// live reports whether e still answers its operation. An entry ahead of the
// window means the window was never fed or lags (a fresh node before its
// ordinal is loaded): it is answered rather than applied again, because the
// wrong answer in that direction is a duplicate pop.
func (w *Window) live(e DedupeEntry, cfg WindowConfig) bool {
	return e.Ordinal > w.Ordinal || w.Ordinal-e.Ordinal < cfg.span()
}

func windowIsOrdinalCut(_ *Window, ev WindowEvent, _ WindowConfig) bool {
	_, ok := ev.(OrdinalCut)
	return ok
}

func windowLookupOf(ev WindowEvent) (LookupGiven, bool) {
	l, ok := ev.(LookupGiven)
	return l, ok
}

func windowWriteOf(ev WindowEvent) (DedupeWrite, bool) {
	wr, ok := ev.(DedupeWrite)
	return wr, ok
}

func windowSweepOf(ev WindowEvent) (SweepBatch, bool) {
	sw, ok := ev.(SweepBatch)
	return sw, ok
}

// windowRows is the whole decision table (build doc §4.11 as ruled by the
// coordinator: a cumulative ordinal, no epoch in the liveness test).
var windowRows = []windowRow{
	{"write/oversize", // a result that cannot be replayed is refused, never truncated or lost
		func(_ *Window, ev WindowEvent, cfg WindowConfig) bool {
			wr, ok := windowWriteOf(ev)
			return ok && len(wr.Result) > cfg.MaxResult
		},
		func(*Window, WindowEvent, WindowConfig) WindowDecision { return WindowDecision{Refuse: true} }},
	{"write/store", // the entry goes in the same batch as the data, so no position exists without it
		func(_ *Window, ev WindowEvent, cfg WindowConfig) bool {
			wr, ok := windowWriteOf(ev)
			return ok && len(wr.Result) <= cfg.MaxResult
		},
		func(w *Window, ev WindowEvent, _ WindowConfig) WindowDecision {
			wr, _ := windowWriteOf(ev)
			w.Ordinal++
			return WindowDecision{Store: true, Write: &DedupeEntry{Key: wr.Key, Pos: wr.Pos, Ordinal: w.Ordinal, Fingerprint: wr.Fingerprint, Result: wr.Result}}
		}},
	{"lookup/none", // no key: the operation applies
		func(w *Window, ev WindowEvent, cfg WindowConfig) bool {
			l, ok := windowLookupOf(ev)
			return ok && l.Entry == nil
		},
		func(*Window, WindowEvent, WindowConfig) WindowDecision { return WindowDecision{} }},
	{"lookup/live", // answer from the record and apply nothing
		func(w *Window, ev WindowEvent, cfg WindowConfig) bool {
			l, ok := windowLookupOf(ev)
			return ok && l.Entry != nil && w.live(*l.Entry, cfg)
		},
		func(_ *Window, ev WindowEvent, _ WindowConfig) WindowDecision {
			l, _ := windowLookupOf(ev)
			return WindowDecision{Live: true, Recorded: l.Entry}
		}},
	{"lookup/dead", // outside the window: the operation applies again
		func(w *Window, ev WindowEvent, cfg WindowConfig) bool {
			l, ok := windowLookupOf(ev)
			return ok && l.Entry != nil && !w.live(*l.Entry, cfg)
		},
		func(*Window, WindowEvent, WindowConfig) WindowDecision { return WindowDecision{} }},
	{"seed/cut", // a seed cut or a reload of the pair record sets the ordinal, in either direction
		windowIsOrdinalCut,
		func(w *Window, ev WindowEvent, _ WindowConfig) WindowDecision {
			cut, _ := ev.(OrdinalCut)
			w.Ordinal = cut.Ordinal
			return WindowDecision{}
		}},
	{"sweep/no-budget",
		func(_ *Window, ev WindowEvent, _ WindowConfig) bool {
			sw, ok := windowSweepOf(ev)
			return ok && sw.Budget <= 0
		},
		func(*Window, WindowEvent, WindowConfig) WindowDecision { return WindowDecision{} }},
	{"sweep/select", // dead entries only, in the order given, at most Budget keys
		func(_ *Window, ev WindowEvent, _ WindowConfig) bool {
			sw, ok := windowSweepOf(ev)
			return ok && sw.Budget > 0
		},
		func(w *Window, ev WindowEvent, cfg WindowConfig) WindowDecision {
			sw, _ := windowSweepOf(ev)
			liveKeys := map[DedupeKey]struct{}{}
			for _, c := range sw.Candidates {
				if w.live(c, cfg) {
					liveKeys[c.Key] = struct{}{}
				}
			}
			var doomed []DedupeKey
			seen := map[DedupeKey]struct{}{}
			for _, c := range sw.Candidates {
				if len(doomed) >= sw.Budget {
					break
				}
				_, isLive := liveKeys[c.Key]
				_, dup := seen[c.Key]
				if isLive || dup {
					continue
				}
				seen[c.Key] = struct{}{}
				doomed = append(doomed, c.Key)
			}
			return WindowDecision{Delete: doomed}
		}},
}

// matchingWindowRows names every row whose condition holds for ev.
func matchingWindowRows(w *Window, ev WindowEvent, cfg WindowConfig) []string {
	var names []string
	for _, row := range windowRows {
		if row.match(w, ev, cfg) {
			names = append(names, row.name)
		}
	}
	return names
}

// Apply feeds one event to the window and returns what the caller must do. A
// refusal changes no state. The caller point-reads exactly the 36-byte key
// before a LookupGiven (the machine cannot see which key was asked), feeds
// OrdinalCut with the pair record's ordinal before the first lookup, and sends
// DedupeWrite only for a record newly applied (not for one the sequence
// machine reports as already applied).
func (w *Window) Apply(ev WindowEvent, cfg WindowConfig) WindowDecision {
	for _, row := range windowRows {
		if row.match(w, ev, cfg) {
			return row.run(w, ev, cfg)
		}
	}
	return WindowDecision{}
}

// BatchOpCount is the 255-operation cap: opIndex is one byte, so n > 255 is
// refused INVALID_ARGUMENT (conventions §18) before an index could wrap and
// two operations could share one entry. A negative n is refused as well.
func BatchOpCount(n int) (uint8, bool) {
	if n < 0 || n > 255 {
		return 0, false
	}
	return uint8(n), true
}
