package wire

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
)

// HandoffTargets is the cache of catch-up targets a Handoff keeps. *Handoff implements it.
type HandoffTargets interface {
	// CachedTarget is the last target computed for the pair, false when there is none.
	CachedTarget(slot uint16, db uint64) (cluster.Position, bool)
}

// targetTopology is a Topology whose Target is the Handoff's cached one.
type targetTopology struct {
	Topology
	targets HandoffTargets
}

// WithHandoffTarget returns topo with Target answered by the Handoff's cached target
// (conv §10 rule 2): the highest position a ready old copy reported, the zero Position
// when the pair never moved to this node.
func WithHandoffTarget(topo Topology, targets HandoffTargets) Topology {
	return targetTopology{Topology: topo, targets: targets}
}

// Target implements Topology.
func (t targetTopology) Target(slot uint16, db uint64) cluster.Position {
	pos, _ := t.targets.CachedTarget(slot, db)
	return pos
}

// PairOwnership raises the replayed and stopProved bits of a pair no barrier has touched
// (write path §2, item 95): a pair this node owns from the start has no predecessor to
// prove stopped and, once the log tail was scanned at boot, nothing left to replay.
// A pair with any flag set is not touched: an open barrier, or a replay a failed apply
// still owes, decides for itself when it may order again.
//
// A node that restarted (PairOwnershipConfig.Restarted) cannot tell a pair it always owned
// from one it owns by a move it never saw, so it claims nothing: a pair it owns is handed to
// the Handoff, which holds it until the stop bound from boot has passed and it caught up
// (BUG-0743).
type PairOwnership struct {
	pairs *cluster.PairSet
	cfg   PairOwnershipConfig
	db    sync.Map // database id -> *slotBits
	// deferred holds the pairs whose boot replay waits for a declaration (BUG-0760): Acquire
	// leaves them alone until Released, so the barrier and the replayed bit are decided once.
	deferred sync.Map // pairKey -> struct{}
}

// PairOwnershipConfig is what a restarted node adds to PairOwnership.
type PairOwnershipConfig struct {
	// Settled reports whether the Handoff has been fed the committed epoch. Nil means always.
	// Until it is, a pair's flags say nothing about the move the epoch made.
	Settled func() bool
	// Restarted is true when this process booted into cluster state a previous process
	// committed. A first boot leaves it false and keeps the ordinary rule.
	Restarted bool
	// Hold opens the lazy restart barrier for a pair this node owns at the committed
	// epoch. held=false means the pair is not this node's to order; decided=false means
	// the committed state is not applied yet and the caller asks again at the next touch.
	// It is called with no pair lock held.
	Hold func(slot uint16, db uint64) (held, decided bool)
}

// slotBits is one bit per slot: the pairs of one database this node already acquired.
type slotBits [storage.SlotCount / 64]atomic.Uint64

// NewPairOwnership returns an Ownership over pairs.
func NewPairOwnership(pairs *cluster.PairSet) *PairOwnership {
	return &PairOwnership{pairs: pairs}
}

// NewRestartedPairOwnership returns an Ownership over pairs whose first touch consults
// cfg.Hold when cfg.Restarted is set.
func NewRestartedPairOwnership(pairs *cluster.PairSet, cfg PairOwnershipConfig) *PairOwnership {
	return &PairOwnership{pairs: pairs, cfg: cfg}
}

// Acquire makes the pair orderable if it is unmarked. It costs one atomic load once the
// pair was acquired, and nothing per bucket.
func (o *PairOwnership) Acquire(slot uint16, db uint64) {
	loaded, ok := o.db.Load(db)
	if !ok {
		loaded, _ = o.db.LoadOrStore(db, new(slotBits))
	}
	bits := loaded.(*slotBits)
	word, bit := &bits[slot/64], uint64(1)<<(slot%64)
	if word.Load()&bit != 0 {
		return
	}
	if _, waiting := o.deferred.Load(pairKey{slot, db}); waiting {
		return // fail closed: the replay of its log tail is not finished; the next touch asks again
	}
	if o.cfg.Restarted {
		// Outside any pair lock: the Handoff takes the pair's lock itself.
		held, decided := o.cfg.Hold(slot, db)
		if !decided {
			return // fail closed: flags stay clear, so the gates hold; the next touch asks again
		}
		if held {
			word.Or(bit)
			return
		}
	}
	if o.cfg.Settled != nil && !o.cfg.Settled() {
		return // fail closed (BUG-0758): the epoch may have made this node owner; the next touch asks again
	}
	o.pairs.With(slot, db, func(pair *cluster.Pair) {
		if pair.Flags == 0 {
			pair.Flags = flagReplayed | flagStopProved
		}
	})
	word.Or(bit)
}

// Recovered marks a pair whose replay after a failed apply has completed: it may order
// again.
func (o *PairOwnership) Recovered(slot uint16, db uint64) {
	o.pairs.With(slot, db, func(pair *cluster.Pair) {
		pair.Flags &^= flagReplayNeeded | flagFirstApplyFailed
		pair.Flags |= flagReplayed
	})
}

// Defer marks a pair whose boot replay has not finished: Acquire does not decide it.
func (o *PairOwnership) Defer(slot uint16, db uint64) {
	o.deferred.Store(pairKey{slot, db}, struct{}{})
}

// isDeferred reports whether the pair's boot replay has not finished.
func (o *PairOwnership) isDeferred(slot uint16, db uint64) bool {
	_, waiting := o.deferred.Load(pairKey{slot, db})
	return waiting
}

// Release ends Defer after the pair's log tail was replayed: it may order once the ordinary
// Acquire decision (the restart barrier, the settled epoch) allows it. The fence goes first, so
// an Acquire that sees the pair released also sees flagReplayNeeded cleared.
func (o *PairOwnership) Release(slot uint16, db uint64) {
	o.pairs.With(slot, db, func(pair *cluster.Pair) {
		pair.Flags &^= flagReplayNeeded | flagBootReplay
	})
	o.deferred.Delete(pairKey{slot, db})
}

// TailScanner reads the retained log from a cursor and applies what the pair records have
// not counted. Scanning the tail is how appendedSeq is learned: Writer.Replay raises it for
// every record of the committed epoch, so a pair never reuses a position that is in the
// log (write path §3.1, spec-patch 71).
type TailScanner struct {
	replayer *cluster.Replayer
	writer   *Writer
}

// NewTailScanner returns a scanner over log that feeds writer. applied reads a pair's
// applied position, which is how the replayer skips what is already counted.
func NewTailScanner(log cluster.LogReader, writer *Writer, applied func(slot uint16, db uint64) cluster.Position) *TailScanner {
	return &TailScanner{replayer: cluster.NewReplayer(log, applied), writer: writer}
}

// ScanBoot is Scan for the boot replay: a pair whose next record needs a declaration the node
// cannot read yet stops and is listed in d instead of failing the open (BUG-0760).
// It stops between two records when ctx ends, which Scan does not: a close must not wait for a
// long tail, and a boot pass never leaves a pair half-applied (each record is one atomic advance).
func (s *TailScanner) ScanBoot(ctx context.Context, from uint64, d *replayDeferrals) error {
	replay := func(rec cluster.WALRecord) error {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("the boot replay was stopped: %w", err)
		}
		return s.writer.replay(rec, d)
	}
	if err := s.replayer.Replay(cluster.Cursor(from), cluster.Position{}, replay); err != nil {
		return fmt.Errorf("scanning the log tail from cursor %d: %w", from, err)
	}
	return nil
}

// Scan replays every record from cursor on that its pair has not applied.
func (s *TailScanner) Scan(_ context.Context, from uint64) error {
	if err := s.replayer.Replay(cluster.Cursor(from), cluster.Position{}, s.writer.Replay); err != nil {
		return fmt.Errorf("scanning the log tail from cursor %d: %w", from, err)
	}
	return nil
}
