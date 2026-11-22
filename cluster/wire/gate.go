package wire

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// gateAskFanout bounds how many POSITION_QUERYs one pair has in flight at once.
const gateAskFanout = 32

// gateInterval is the gate's period without a wake. A pass also runs on every reconcile
// that touches a retired pair; the period covers a member that answered nothing then.
const gateInterval = 30 * time.Second

// gateDeps is everything the gate driver needs from its node; tests fill it with fakes.
type gateDeps struct {
	Self      string
	Recon     *Reconciler
	Committed func() cluster.ClusterState
	// Epoch is the committed epoch. A pass reads it before its asks and again after
	// them, and drops the answers when it moved.
	Epoch func() uint64
	Ask   func(ctx context.Context, node string, q PositionQuery) (PositionReply, error)
	// Delete is the budgeted range delete (Puller.resetRange); it charges the reclaim budget.
	Delete func(ctx context.Context, key reconcileKey) error
	// Queue starts the seed of a pair the gate deleted because it came back into the
	// target set (Puller.Queue).
	Queue func(slot uint16, db uint64)
	// Report carries the RETIRE notes and says when one committed.
	Report  *seedReporter
	OnError func(error)
	// Deferred says a pair's boot replay has not finished, so its log holds records the fence
	// does not cover; the gate passes nothing for it (BUG-0760).
	Deferred func(slot uint16, db uint64) bool
	// Diverged says this node's cluster state is known to disagree with the cluster's: the gate
	// then decides and deletes nothing, because the state it judges a pair against is frozen
	// (BUG-0781). Nil means the state is trusted.
	Diverged func() bool
}

// gateDriver decides when a retired copy's range may be deleted (seeding §8).
type gateDriver struct {
	deps gateDeps
	wake chan struct{} // one pending wake; Kick never blocks
	// awaiting holds the pairs whose range is deleted and whose RETIRE was noted; the
	// value is true once the reporter said it committed and only the record is left to
	// remove. Pass is the only user, so it needs no lock.
	awaiting map[reconcileKey]bool
}

// newGateDriver returns a driver that has run no pass yet.
func newGateDriver(deps gateDeps) *gateDriver {
	return &gateDriver{deps: deps, wake: make(chan struct{}, 1), awaiting: make(map[reconcileKey]bool)}
}

// Kick asks the driver to run a pass soon. A wake already pending covers it.
func (g *gateDriver) Kick() {
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

// Run runs a pass on every wake and every gateInterval tick until ctx is done. A pass
// error goes to OnError, except the one a cancelled context causes: that is the shutdown.
func (g *gateDriver) Run(ctx context.Context) {
	ticker := time.NewTicker(gateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-g.wake:
		case <-ticker.C:
		}
		if err := g.Pass(ctx); err != nil && ctx.Err() == nil {
			g.deps.OnError(err)
		}
	}
}

// Pass runs one pass over the retired and retiring pairs. It first removes the records
// whose RETIRE committed. Then, for a retired pair, it asks the copies for their position
// and feeds every answer to the pair's machine as evidence; a failed ask holds the gate
// and is reported once per member per pass; when the machine opens the gate the pair is
// deleted and its RETIRE noted. A retiring pair finishes its delete with no evidence. A
// pair whose database is not in the committed state is skipped, so no query goes out for it.
func (g *gateDriver) Pass(ctx context.Context) error {
	if g.deps.Diverged != nil && g.deps.Diverged() {
		// The gate opens a range delete from the committed state and the copies' answers; a
		// frozen state is not the cluster's, and with replication factor 1 the range is the only copy.
		return nil
	}
	g.removeCommitted()
	state := g.deps.Committed()
	reported := make(map[string]bool)
	for _, key := range g.deps.Recon.PairsIn(machine.SeedRetired, machine.SeedRetiring) {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("seed gate pass: %w", err)
		}
		if _, ok := gateDBRF(state, key.db); !ok {
			continue
		}
		if _, waiting := g.awaiting[key]; waiting {
			continue // deleted, and the record stays until its RETIRE commits
		}
		if g.deps.Deferred != nil && g.deps.Deferred(key.slot, key.db) {
			continue // its log holds records the fence does not cover (BUG-0760)
		}
		rec, held := g.deps.Recon.Record(key.slot, key.db)
		if !held {
			continue
		}
		switch rec.State {
		case machine.SeedRetired:
			act, err := g.askPair(ctx, key, state, reported)
			if err != nil {
				return err
			}
			if act.Do == machine.SlotDeleteRange || act.Do == machine.SlotDeleteRangeThenSeed {
				g.deleteOpened(ctx, key)
			}
		case machine.SeedRetiring:
			g.finishDelete(ctx, key)
		}
	}
	return nil
}

// removeCommitted drops the record of every awaiting pair whose RETIRE committed. The
// reporter says so once, so the answer is kept until the record is gone.
func (g *gateDriver) removeCommitted() {
	for key, committed := range g.awaiting {
		if !committed {
			committed = g.deps.Report.RetireDone(key.db, key.slot)
			g.awaiting[key] = committed
		}
		if !committed {
			continue
		}
		if _, err := g.deps.Recon.Feed(key.slot, key.db, machine.RetireCommitted{}); err != nil {
			g.deps.OnError(fmt.Errorf("seed gate: remove the record of slot %d of database %d: %w", key.slot, key.db, err))
			continue
		}
		delete(g.awaiting, key)
	}
}

// deleteOpened starts the delete of a pair whose gate the machine opened. RetireStarted
// commits the retiring record, with the fence the disk holds now, before the first key
// goes, and the machine decides whether the delete may run at all.
func (g *gateDriver) deleteOpened(ctx context.Context, key reconcileKey) {
	snap := g.deps.Recon.cfg.Store.NewSnapshot()
	fence, _, err := reconcileReadFence(snap, key)
	snap.Discard()
	if err != nil {
		g.deps.OnError(fmt.Errorf("seed gate: slot %d of database %d: %w", key.slot, key.db, err))
		return
	}
	started, err := g.deps.Recon.Feed(key.slot, key.db, machine.RetireStarted{Fence: fence})
	if err != nil {
		g.deps.OnError(fmt.Errorf("seed gate: start the delete of slot %d of database %d: %w", key.slot, key.db, err))
		return
	}
	if started.Do != machine.SlotDeleteRange {
		return
	}
	g.finishDelete(ctx, key)
}

// finishDelete deletes the range of a retiring pair and tells its machine. Out of the
// target set the RETIRE is noted and the record waits for it; back in the target set
// the machine has already started a fresh seed, which the puller is asked to run, and no
// RETIRE is noted: the bit is not cleared for a copy that is wanted again.
func (g *gateDriver) finishDelete(ctx context.Context, key reconcileKey) {
	if err := g.deps.Delete(ctx, key); err != nil {
		g.deps.OnError(fmt.Errorf("seed gate: delete slot %d of database %d: %w", key.slot, key.db, err))
		return
	}
	act, err := g.deps.Recon.Feed(key.slot, key.db, machine.RetireDeleted{})
	if err != nil {
		g.deps.OnError(fmt.Errorf("seed gate: finish the delete of slot %d of database %d: %w", key.slot, key.db, err))
		return
	}
	switch {
	case act.ProposeRetire:
		g.deps.Report.Note(key.db, key.slot, true)
		g.awaiting[key] = false
	case act.Do == machine.SlotRestartSeed:
		g.deps.Queue(key.slot, key.db)
	}
}

// askPair asks the other copies of one retired pair and feeds the answers to its
// machine, in copy order, and returns the last action the machine gave. When the
// committed epoch moved during the asks, the answers name a view that is gone, so
// none is fed and the action is empty.
func (g *gateDriver) askPair(ctx context.Context, key reconcileKey, state cluster.ClusterState, reported map[string]bool) (machine.SlotAction, error) {
	var act machine.SlotAction
	epoch := g.deps.Epoch()
	members := gateCopies(state, g.deps.Self, key.slot, key.db)
	answers := g.askAll(ctx, key, members)
	if err := ctx.Err(); err != nil {
		return act, fmt.Errorf("seed gate pass: %w", err)
	}
	for i, id := range members {
		if answers[i].err != nil && !reported[id] {
			reported[id] = true
			g.deps.OnError(fmt.Errorf("seed gate: %w", answers[i].err))
		}
	}
	if g.deps.Epoch() != epoch {
		return act, nil
	}
	for i, id := range members {
		if answers[i].err != nil {
			continue
		}
		ev := machine.PositionReplied{
			Node:    id,
			Epoch:   epoch,
			State:   machine.SlotState(answers[i].reply.State),
			Applied: answers[i].reply.AppliedPos,
		}
		var err error
		if act, err = g.deps.Recon.Feed(key.slot, key.db, ev); err != nil {
			return act, fmt.Errorf("seed gate: feed the position of %s: %w", id, err)
		}
	}
	return act, nil
}

// gateAnswer is one member's answer to a POSITION_QUERY.
type gateAnswer struct {
	reply PositionReply
	err   error
}

// askAll asks every member at once, at most gateAskFanout in flight, and returns
// once all have answered. The goroutines are joined before it returns, so a pass
// leaves none behind.
func (g *gateDriver) askAll(ctx context.Context, key reconcileKey, members []string) []gateAnswer {
	answers := make([]gateAnswer, len(members))
	inFlight := make(chan struct{}, gateAskFanout)
	var wg sync.WaitGroup
	for i, id := range members {
		inFlight <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-inFlight }()
			answers[i].reply, answers[i].err = g.deps.Ask(ctx, id, PositionQuery{Slot: key.slot, DB: key.db})
		}()
	}
	wg.Wait()
	return answers
}

// PairsIn lists the held pairs whose disk record is in one of states, sorted by
// database and then slot. It is what a pass walks, so the order it returns is
// the order the pass acts in.
func (r *Reconciler) PairsIn(states ...uint8) []reconcileKey {
	r.mu.Lock()
	defer r.mu.Unlock()
	var keys []reconcileKey
	for key, p := range r.pairs {
		if p.present && slices.Contains(states, p.disk.State) {
			keys = append(keys, key)
		}
	}
	slices.SortFunc(keys, func(a, b reconcileKey) int {
		if a.db != b.db {
			return cmpUint(a.db, b.db)
		}
		return cmpUint(uint64(a.slot), uint64(b.slot))
	})
	return keys
}

// gateCopies is the committed copy set of a pair without self: what the gate asks.
// A database that is not in state has no copies.
func gateCopies(state cluster.ClusterState, self string, slot uint16, db uint64) []string {
	rf, ok := gateDBRF(state, db)
	if !ok {
		return nil
	}
	copies := reconcilePlacingTable(state, self).Copies(slot, rf)
	ids := make([]string, 0, len(copies))
	for _, m := range copies {
		if m.ID != self {
			ids = append(ids, m.ID)
		}
	}
	return ids
}

// gateDBRF returns the replication factor of the database with id db, and false
// when the committed state does not hold that database.
func gateDBRF(state cluster.ClusterState, db uint64) (int, bool) {
	for _, database := range state.Databases {
		if uint64(database.ID) == db {
			return int(database.RF), true
		}
	}
	return 0, false
}

// gainerGap names the first pair of a leaving member whose next copy set has a member that has not reported it ready.
type gainerGap struct {
	Database string
	Slot     uint16
	Member   string
}

// gainersReady is true when, for every pair the leaving member holds, every member of the pair's copy set without the leaver holds the committed SEEDED bit.
// The leaver's own bit does not count: once it is gone the pair has no copy of it. It reads only the committed rows: never a record, never a round trip.
func gainersReady(state cluster.ClusterState, leaving string) (gainerGap, bool) {
	next := state
	next.Members = slices.DeleteFunc(slices.Clone(state.Members), func(m cluster.Member) bool { return m.ID == leaving })
	now, after := cluster.BuildTable(state), cluster.BuildTable(next)
	rows := newSeedBitRows(state)
	for _, database := range state.Databases {
		for slot := range uint16(storage.SlotCount) {
			if !slices.ContainsFunc(now.Copies(slot, int(database.RF)), func(m cluster.Member) bool { return m.ID == leaving }) {
				continue
			}
			for _, member := range after.Copies(slot, int(database.RF)) {
				if !rows.Holds(member.ID, database.ID, slot) {
					return gainerGap{Database: database.Name, Slot: slot, Member: member.ID}, false
				}
			}
		}
	}
	return gainerGap{}, true
}
