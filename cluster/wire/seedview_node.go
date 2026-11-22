package wire

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// seedViewDeps is everything the view reads and drives from its node; tests fill it with fakes.
type seedViewDeps struct {
	State     func() cluster.ClusterState                                      // Node.State
	Records   func(visit func(slot uint16, db uint64, state uint8))            // Reconciler.Records
	Record    func(slot uint16, db uint64) (SeedRecord, bool)                  // Reconciler.Record
	Pull      func(slot uint16, db uint64) (PullStatus, bool)                  // Puller.Status
	Waiting   func(slot uint16, db uint64) []string                            // Reconciler.WaitingFor
	FeedRetry func(slot uint16, db uint64) (machine.SlotAction, error)         // Reconciler.Feed with machine.SeedRetryAsked{}
	Requeue   func(slot uint16, db uint64)                                     // dropQueue, then Puller.Queue, then Puller.rotate
	Forget    func(slot uint16, db uint64, anyway bool) (ForgetOutcome, error) // seedParts.forget: Reconciler.Forget, then a pass
	Report    func(error)
}

// nodeSeedingView is the node's SeedingView over its seeding engine. It keeps no state of
// its own: every call reads the engine, so it adds nothing per bucket and no goroutine.
type nodeSeedingView struct{ deps seedViewDeps }

// newSeedingView builds the view over deps.
func newSeedingView(deps seedViewDeps) *nodeSeedingView { return &nodeSeedingView{deps: deps} }

// Status returns one row for every held record that is not ready. It walks every held record
// once, under the Reconciler's lock for the walk only, because nothing indexes the pairs that
// are not ready (build note S4, Owner question 1).
func (v *nodeSeedingView) Status() []SeedRow {
	names := map[uint64]string{}
	for _, database := range v.deps.State().Databases {
		names[uint64(database.ID)] = database.Name
	}
	type pairKey struct {
		slot uint16
		db   uint64
	}
	var keys []pairKey
	// The visit only appends: it runs under the Reconciler's mutex and must not call back in.
	v.deps.Records(func(slot uint16, db uint64, state uint8) {
		if _, named := names[db]; named && state != machine.SeedReady {
			keys = append(keys, pairKey{slot, db})
		}
	})
	rows := make([]SeedRow, 0, len(keys))
	for _, key := range keys {
		record, held := v.deps.Record(key.slot, key.db)
		if !held {
			continue
		}
		pull, pulled := v.deps.Pull(key.slot, key.db)
		state, known := seedRowState(record, pull, pulled)
		if !known {
			v.deps.Report(fmt.Errorf("seeding status: slot %d of database %q holds a record in state %d, which no status word names", key.slot, names[key.db], record.State))
			continue
		}
		row := SeedRow{
			Database: names[key.db], Slot: int(key.slot), State: state,
			Source: record.Source, BytesDone: pull.Bytes, Entries: pull.Entries, Idle: pull.Idle,
		}
		if pull.Source != "" {
			row.Source = pull.Source
		}
		switch state {
		case SeedPlacing, SeedSeeding, SeedStalled:
			row.Refusal = pull.Detail
		case SeedRetired:
			row.Refusal = seedWaitText(v.deps.Waiting(key.slot, key.db))
		}
		rows = append(rows, row)
	}
	return rows
}

// seedRowState is the word for a held record and its pull. It returns false for a record state
// no word names, so a status row can never carry the machine's raw number.
func seedRowState(record SeedRecord, pull PullStatus, pulled bool) (SeedState, bool) {
	switch record.State {
	case machine.SeedReady:
		return SeedReady, true
	case machine.SeedRetired:
		return SeedRetired, true
	case machine.SeedRetiring:
		return SeedRetiring, true
	case machine.SeedSeeding:
		if !pulled {
			return SeedPlacing, true
		}
		switch pull.State {
		case PullStalled:
			return SeedStalled, true
		case PullPulling:
			return SeedSeeding, true
		case PullQueued:
			// A queued pair that has a source or applied entries began and waits out a backoff.
			if pull.Source != "" || pull.Entries > 0 {
				return SeedSeeding, true
			}
		}
		return SeedPlacing, true
	}
	return "", false
}

// seedWaitText says which members the deletion gate waits for, or "" when it waits for nobody.
func seedWaitText(members []string) string {
	switch len(members) {
	case 0:
		return ""
	case 1:
		return "waiting for " + members[0] + " to confirm it holds this slot"
	}
	return "waiting for " + strings.Join(members, ", ") + " to confirm they hold this slot"
}

// WaitingFor names the members the deletion gate waits for on one held pair, from the slot
// machine's last reconcile; nil when the pair is not held or the gate waits for nobody.
func (r *Reconciler) WaitingFor(slot uint16, db uint64) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	pair := r.pairs[reconcileKey{slot, db}]
	if pair == nil || !pair.present {
		return nil
	}
	return pair.slot.WaitingFor()
}

// Retry starts a seeding or stalled slot again from a fresh cut. The slot machine decides
// whether the slot may restart; a restart also puts the pair back on the puller's queue and
// moves it to the next legal source, which the machine's record write cannot do.
func (v *nodeSeedingView) Retry(ctx context.Context, database string, slot int) error {
	id, named := v.databaseID(database)
	if !named {
		return SeedNotHeld(database, slot, "absent")
	}
	action, err := v.deps.FeedRetry(uint16(slot), id)
	if err != nil {
		return fmt.Errorf("restarting slot %d of database %q: %w", slot, database, err)
	}
	if action.Refusal == cluster.CodeSeedNotHeld {
		word := "absent"
		if record, held := v.deps.Record(uint16(slot), id); held {
			pull, pulled := v.deps.Pull(uint16(slot), id)
			if state, known := seedRowState(record, pull, pulled); known {
				word = string(state)
			}
		}
		return SeedNotHeld(database, slot, word)
	}
	if action.Do == machine.SlotRestartSeed {
		v.deps.Requeue(uint16(slot), id)
	}
	return nil
}

// Forget is SeedingView.Forget. A retired record that no other member has confirmed a copy of
// may hold the only copy of the slot's data, so it is refused unless anyway is set. The engine
// decides both that and whether the record is retired, under the pair lock, on one reading of
// the record (the view reading them separately let a record move between the reads); a
// refusal is judged by the record as it is after that decision.
func (v *nodeSeedingView) Forget(ctx context.Context, database string, slot int, anyway bool) error {
	id, named := v.databaseID(database)
	if !named || slot < 0 || slot > math.MaxUint16 {
		return SeedNothingToForget(database, slot)
	}
	if _, held := v.deps.Record(uint16(slot), id); !held {
		return SeedNothingToForget(database, slot)
	}
	outcome, err := v.deps.Forget(uint16(slot), id, anyway)
	if err != nil {
		return fmt.Errorf("forgetting slot %d of database %q: %w", slot, database, err)
	}
	switch outcome {
	case ForgetRemoved:
		return nil
	case ForgetOnlyCopy:
		return SeedOnlyCopy(database, slot)
	}
	record, held := v.deps.Record(uint16(slot), id)
	if !held {
		return SeedNothingToForget(database, slot)
	}
	pull, pulled := v.deps.Pull(uint16(slot), id)
	state, known := seedRowState(record, pull, pulled)
	if !known {
		return fmt.Errorf("forgetting slot %d of database %q: it holds a record in state %d, which no status word names", slot, database, record.State)
	}
	return SeedHeld(database, slot, state)
}

// databaseID returns the id of the database named in the committed state.
func (v *nodeSeedingView) databaseID(name string) (uint64, bool) {
	for _, database := range v.deps.State().Databases {
		if database.Name == name {
			return uint64(database.ID), true
		}
	}
	return 0, false
}

// newNodeSeedingView is the view over n's seeding engine.
func newNodeSeedingView(n *Node) *nodeSeedingView {
	parts := n.cl.seeding.Load()
	return newSeedingView(seedViewDeps{
		State:   n.raft.State,
		Records: parts.recon.Records,
		Record:  parts.recon.Record,
		Pull:    parts.puller.Status,
		Waiting: parts.recon.WaitingFor,
		FeedRetry: func(slot uint16, db uint64) (machine.SlotAction, error) {
			return parts.recon.Feed(slot, db, machine.SeedRetryAsked{})
		},
		// Queue first: rotate does nothing for a pair that has no entry yet.
		Requeue: func(slot uint16, db uint64) {
			parts.dropQueue(slot, db)
			parts.puller.Queue(slot, db)
			parts.puller.rotate(reconcileKey{slot, db})
		},
		Forget: parts.forget,
		Report: n.cl.report,
	})
}
