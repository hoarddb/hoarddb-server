package wire

import (
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// RestartSeed writes a fresh attempt for a pair that is seeding: a new seed_id, phase reset,
// a zero cut and an empty cursor, in one committed record change (seeding section 7). The
// pull calls it when the source sent SEED_RESTART or a chunk sequence broke; the old
// attempt's later events find a new seed machine and change nothing. A pair that is not
// seeding is left alone. It takes the pair lock itself, so the caller must not hold it.
func (r *Reconciler) RestartSeed(slot uint16, dbID uint64) error {
	if err := r.needLoaded("RestartSeed"); err != nil {
		return err
	}
	key := reconcileKey{slot, dbID}
	var err error
	r.cfg.Pairs.With(slot, dbID, func(shared *cluster.Pair) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if err = r.checkNotBusy(key); err != nil {
			return
		}
		orig := r.pairs[key]
		if orig == nil || orig.slot.State != machine.SlotSeeding {
			return
		}
		sp := r.stageFrom(key, shared)
		r.freshAttempt(sp)
		w := r.newWriter()
		w.inline = shared
		defer w.abort()
		if err = r.persist(w, key, sp); err != nil {
			return
		}
		w.stage(key, sp)
		err = w.commit()
	})
	r.flushDrops()
	if err != nil {
		return fmt.Errorf("slot %d db %d: restart the seed: %w", slot, dbID, err)
	}
	return nil
}

// A left seed is a seeding pair whose node is no longer in its copy set: the last pass that
// judged the pair saw it outside (reconciled/seeding-not-in-target). Its partial range is
// deleted and its record forgotten, in an order that keeps a durable record that admits
// nothing over the range for as long as the range holds a key (BUG-0817): the record is
// voided, the range deleted, and the record removed last. The two record writes are here;
// the order is dropLeftSeed's.

// leftSeedLocked says p is a left seed. mu is held. A pair no pass has judged in this process
// is not one: after a restart the first pass decides, never a record alone.
func leftSeedLocked(p *reconcilePair) bool {
	return p != nil && p.present && p.slot.State == machine.SlotSeeding && p.hasSeen && !p.seen.inTarget
}

// leftSeed reports whether the pair is a left seed, for the pull, which then deletes the
// range instead of pulling into it.
func (r *Reconciler) leftSeed(slot uint16, dbID uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return leftSeedLocked(r.pairs[reconcileKey{slot, dbID}])
}

// voidLeftSeed makes a left seed's record an attempt in phase reset, durably, and returns
// that attempt's seed id; left is false, and nothing is written, for a pair that is not a
// left seed (it changed since the pass that ordered the drop, and the next pass judges it).
// An attempt in phase reset has no cut and no cursor and has not registered, so nothing can
// resume it and no chunk is applied under it, while the record still says seeding: the pair
// admits no write and no pushed record (recordSlots.Admits), and a boot that finds it starts
// from a range reset (in the copy set) or orders the drop again (out of it). A record that is
// already such an attempt is kept as it is, so the passes that repeat the order while the
// delete runs do not void the attempt the delete is running under. It takes the pair lock
// itself, so the caller must not hold it.
func (r *Reconciler) voidLeftSeed(slot uint16, dbID uint64) (attempt [16]byte, left bool, err error) {
	if err = r.needLoaded("voidLeftSeed"); err != nil {
		return attempt, false, err
	}
	key := reconcileKey{slot, dbID}
	r.cfg.Pairs.With(slot, dbID, func(shared *cluster.Pair) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if err = r.checkMutable(key); err != nil {
			return
		}
		orig := r.pairs[key]
		if !leftSeedLocked(orig) {
			return
		}
		if orig.slot.Phase == machine.PhaseReset {
			attempt, left = orig.slot.SeedID, true
			return
		}
		sp := r.stageFrom(key, shared)
		r.freshAttempt(sp)
		w := r.newWriter()
		w.inline = shared
		defer w.abort()
		if err = r.persist(w, key, sp); err != nil {
			return
		}
		w.stage(key, sp)
		if err = w.commit(); err != nil {
			return
		}
		attempt, left = sp.p.slot.SeedID, true
	})
	r.flushDrops()
	if err != nil {
		return attempt, false, fmt.Errorf("void the left seed's attempt: %w", err)
	}
	return attempt, left, nil
}

// removeLeftSeed removes a left seed's record, durably, after its range was deleted under the
// attempt voidLeftSeed returned, and reports whether it did. The record goes only if it is
// still that attempt and still unregistered: a pass that saw the pair come back wrote a new
// attempt (another seed id) and queued its pull, and that record stays. The check and the
// removal are one critical section under the pair lock, so the pair turns from "seeding" to
// "no record over an empty range" in one step, and a write admitted after it finds what an
// absent record promises. It takes the pair lock itself.
func (r *Reconciler) removeLeftSeed(slot uint16, dbID uint64, attempt [16]byte) (removed bool, err error) {
	if err = r.needLoaded("removeLeftSeed"); err != nil {
		return false, err
	}
	key := reconcileKey{slot, dbID}
	r.cfg.Pairs.With(slot, dbID, func(shared *cluster.Pair) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if err = r.checkMutable(key); err != nil {
			return
		}
		orig := r.pairs[key]
		if orig == nil || !orig.present || orig.slot.State != machine.SlotSeeding || orig.slot.Phase != machine.PhaseReset || orig.slot.SeedID != attempt {
			return
		}
		sp := r.stageFrom(key, shared)
		w := r.newWriter()
		w.inline = shared
		defer w.abort()
		sp.p.slot = machine.SlotRecord{}
		if err = r.persist(w, key, sp); err != nil {
			return
		}
		w.stage(key, sp)
		if err = w.commit(); err != nil {
			return
		}
		removed = true
	})
	r.flushDrops()
	if err != nil {
		return false, fmt.Errorf("remove the left seed's record: %w", err)
	}
	return removed, nil
}
