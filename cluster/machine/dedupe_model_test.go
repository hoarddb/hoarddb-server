package machine

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// The model test drives the window the way the wiring unit will: a node holds a
// Window and a key-value map standing in for the pair's range; a write looks
// the key up, applies the operation if the window says so, and stores the
// entry the decision hands back. A reference model written from the prose of
// conv §18 (a map keyed by request id and op index, one counter of applied
// records, a plain age comparison) is run beside it, and both must give the
// same answer to every operation.

// dedupeModelOutcome is the client-visible answer to one operation.
type dedupeModelOutcome struct {
	kind   string // applied, deduped or refused
	pos    cluster.Position
	result string
}

// dedupeModelWorld is the shared state an operation acts on: the owner's position
// counter and the queue head a pop consumes.
type dedupeModelWorld struct {
	epoch, seq, head uint64
}

// dedupeModelOp is one operation. A retry repeats the same dedupeModelOp.
type dedupeModelOp struct {
	key  DedupeKey
	push bool // a push returns nothing; a pop returns the head element
	big  bool // a pop whose element is over the result cap
}

// element is what a pop returns, or the oversize element of a big pop.
func (w *dedupeModelWorld) element(op dedupeModelOp, maxResult int) []byte {
	if op.push {
		return nil
	}
	if op.big {
		return bytes.Repeat([]byte{'b'}, maxResult+1)
	}
	return []byte(fmt.Sprintf("e%d", w.head))
}

// commit consumes the operation: a position and, for a pop, the head element.
func (w *dedupeModelWorld) commit(op dedupeModelOp) cluster.Position {
	w.seq++
	if !op.push {
		w.head++
	}
	return cluster.Position{Epoch: w.epoch, Seq: w.seq}
}

func (w *dedupeModelWorld) promote() { w.epoch++; w.seq = 0 }

type dedupeModelNode struct {
	win       Window
	kv        map[DedupeKey]DedupeEntry
	persisted uint64 // the pair record's ordinal, written in the entry's batch
}

func (n *dedupeModelNode) lookup(k DedupeKey) *DedupeEntry {
	if e, ok := n.kv[k]; ok {
		c := e
		return &c
	}
	return nil
}

// dedupeMachineSide is the owner and one copy, both applying every record.
type dedupeMachineSide struct {
	t            *testing.T
	cfg          WindowConfig
	world        dedupeModelWorld
	owner, copy_ *dedupeModelNode
}

func dedupeNewModelNode() *dedupeModelNode { return &dedupeModelNode{kv: map[DedupeKey]DedupeEntry{}} }

func (s *dedupeMachineSide) do(op dedupeModelOp) dedupeModelOutcome {
	d := s.owner.win.Apply(LookupGiven{Entry: s.owner.lookup(op.key)}, s.cfg)
	if d.Live {
		if d.Recorded == nil || d.Store || d.Write != nil {
			s.t.Fatalf("a live lookup answers from the record and stores nothing: %+v", d)
		}
		return dedupeModelOutcome{"deduped", d.Recorded.Pos, string(d.Recorded.Result)}
	}
	result := s.world.element(op, s.cfg.MaxResult)
	next := cluster.Position{Epoch: s.world.epoch, Seq: s.world.seq + 1}
	ev := DedupeWrite{Key: op.key, Pos: next, Result: result}
	d = s.owner.win.Apply(ev, s.cfg)
	if d.Refuse {
		if d.Store || d.Write != nil {
			s.t.Fatalf("a refusal stores nothing: %+v", d)
		}
		return dedupeModelOutcome{kind: "refused"}
	}
	if !d.Store || d.Write == nil {
		s.t.Fatalf("the machine said new without an entry to write: %+v", d)
	}
	s.world.commit(op)
	s.owner.kv[op.key] = *d.Write
	s.owner.persisted = d.Write.Ordinal
	dc := s.copy_.win.Apply(ev, s.cfg)
	if !dc.Store || dc.Write == nil || !reflect.DeepEqual(dc.Write, d.Write) {
		s.t.Fatalf("owner and copy applied one record and stored different entries: %+v vs %+v", d.Write, dc.Write)
	}
	s.copy_.kv[op.key] = *dc.Write
	s.copy_.persisted = dc.Write.Ordinal
	return dedupeModelOutcome{"applied", next, string(result)}
}

func (s *dedupeMachineSide) promote() {
	s.owner, s.copy_ = s.copy_, s.owner
	s.world.promote()
}

// restart drops the window and reloads the ordinal the pair record holds.
func (s *dedupeMachineSide) restart() {
	s.owner.win = Window{}
	s.owner.win.Apply(OrdinalCut{Ordinal: s.owner.persisted}, s.cfg)
}

// reseedCopy replaces the copy with a fresh node holding the owner's snapshot
// and a cut at the owner's ordinal.
func (s *dedupeMachineSide) reseedCopy() {
	fresh := dedupeNewModelNode()
	for k, e := range s.owner.kv {
		fresh.kv[k] = e
	}
	fresh.win.Apply(OrdinalCut{Ordinal: s.owner.win.Ordinal}, s.cfg)
	fresh.persisted = s.owner.win.Ordinal
	s.copy_ = fresh
}

// sweep scans a bounded run of one node's keys in key order and deletes what
// the decision names; it returns the decision's keys and the candidates.
func (s *dedupeMachineSide) sweep(n *dedupeModelNode, from, size, budget int) ([]DedupeKey, []DedupeEntry) {
	keys := make([]DedupeKey, 0, len(n.kv))
	for k := range n.kv {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if c := bytes.Compare(keys[i].RequestID[:], keys[j].RequestID[:]); c != 0 {
			return c < 0
		}
		return keys[i].OpIndex < keys[j].OpIndex
	})
	if len(keys) == 0 {
		return nil, nil
	}
	from %= len(keys)
	var cands []DedupeEntry
	for _, k := range keys[from:min(from+size, len(keys))] {
		cands = append(cands, n.kv[k])
	}
	d := n.win.Apply(SweepBatch{Candidates: cands, Budget: budget}, s.cfg)
	for _, k := range d.Delete {
		delete(n.kv, k)
	}
	return d.Delete, cands
}

// dedupeRefEntry and dedupeRefModel are the trivially correct reference.
type dedupeRefEntry struct {
	ordinal uint64
	pos     cluster.Position
	result  string
}

type dedupeRefModel struct {
	cfg     WindowConfig
	count   uint64
	entries map[DedupeKey]dedupeRefEntry
	world   dedupeModelWorld
}

func (r *dedupeRefModel) window() uint64 { return max(r.cfg.Window, 1) }

func (r *dedupeRefModel) isLive(e dedupeRefEntry) bool { return r.count-e.ordinal < r.window() }

func (r *dedupeRefModel) do(op dedupeModelOp) dedupeModelOutcome {
	if e, ok := r.entries[op.key]; ok && r.isLive(e) {
		return dedupeModelOutcome{"deduped", e.pos, e.result}
	}
	result := r.world.element(op, r.cfg.MaxResult)
	if len(result) > r.cfg.MaxResult {
		return dedupeModelOutcome{kind: "refused"}
	}
	pos := r.world.commit(op)
	r.count++
	r.entries[op.key] = dedupeRefEntry{ordinal: r.count, pos: pos, result: string(result)}
	return dedupeModelOutcome{"applied", pos, string(result)}
}

// TestDedupeModelAgainstAReferenceMap runs 20 fixed seeds of random sequences
// against the reference and requires identical outputs, plus the invariants:
// a retry inside the window is never applied twice, a retried pop returns the
// stored result, the machine never says new without an entry, and a sweep
// deletes only entries outside the window and never exceeds its budget.
// Guard: any of the guards of the unit tests, run through the same glue (end
// the window at an epoch change, drop the result, delete a live entry).
func TestDedupeModelAgainstAReferenceMap(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		dedupeRunModelSeed(t, seed)
	}
}

func dedupeRunModelSeed(t *testing.T, seed int64) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	cfg := WindowConfig{Window: uint64(3 + (seed%7)*5), MaxResult: 8}
	side := &dedupeMachineSide{t: t, cfg: cfg, world: dedupeModelWorld{epoch: 1}, owner: dedupeNewModelNode(), copy_: dedupeNewModelNode()}
	ref := &dedupeRefModel{cfg: cfg, entries: map[DedupeKey]dedupeRefEntry{}, world: dedupeModelWorld{epoch: 1}}

	var used []dedupeModelOp // every operation issued so far, for retries
	nextRequest := 0
	lastApplied := map[DedupeKey]uint64{} // the ordinal at which a key was last applied
	lastResult := map[DedupeKey]string{}

	for step := 0; step < 600; step++ {
		switch roll := rng.Intn(100); {
		case roll < 70:
			var op dedupeModelOp
			switch {
			case len(used) == 0 || rng.Intn(2) == 0:
				nextRequest++
				op = dedupeModelOp{key: dedupeKey(nextRequest), push: rng.Intn(3) == 0, big: rng.Intn(8) == 0}
			case rng.Intn(6) == 0:
				// The same request id at another op index is another operation.
				base := used[rng.Intn(len(used))]
				op = dedupeModelOp{key: DedupeKey{RequestID: base.key.RequestID, OpIndex: uint8(1 + rng.Intn(3))}, push: rng.Intn(3) == 0}
			default:
				op = used[rng.Intn(len(used))]
			}
			used = append(used, op)
			before := ref.count
			want := ref.do(op)
			got := side.do(op)
			if got != want {
				t.Fatalf("seed %d step %d op %+v: machine %+v, reference %+v", seed, step, op, got, want)
			}
			switch got.kind {
			case "applied":
				if prev, ok := lastApplied[op.key]; ok && before-prev < ref.window() {
					t.Fatalf("seed %d step %d: %+v applied twice inside the window (ordinal %d then %d, window %d)",
						seed, step, op.key, prev, before, ref.window())
				}
				lastApplied[op.key] = ref.count
				lastResult[op.key] = got.result
			case "deduped":
				if got.result != lastResult[op.key] {
					t.Fatalf("seed %d step %d: a deduped retry returned %q, the stored result was %q", seed, step, got.result, lastResult[op.key])
				}
			}
		case roll < 76:
			side.promote()
			ref.world.promote()
		case roll < 80:
			side.restart()
		case roll < 84:
			side.reseedCopy()
		default:
			node := side.owner
			if rng.Intn(3) == 0 {
				node = side.copy_
			}
			budget := rng.Intn(5)
			deleted, cands := side.sweep(node, rng.Intn(50), 1+rng.Intn(6), budget)
			if len(deleted) > budget {
				t.Fatalf("seed %d step %d: sweep deleted %d with budget %d", seed, step, len(deleted), budget)
			}
			dead := 0
			for _, c := range cands {
				if !ref.isLive(dedupeRefEntry{ordinal: c.Ordinal}) {
					dead++
				}
			}
			if len(deleted) != min(budget, dead) {
				t.Fatalf("seed %d step %d: sweep deleted %d of %d dead candidates with budget %d", seed, step, len(deleted), dead, budget)
			}
			for _, k := range deleted {
				if e, ok := ref.entries[k]; !ok || ref.isLive(e) {
					t.Fatalf("seed %d step %d: sweep deleted %+v, which is still inside the window", seed, step, k)
				}
			}
		}
		if side.owner.win.Ordinal != ref.count || side.copy_.win.Ordinal != ref.count {
			t.Fatalf("seed %d step %d: ordinals owner %d copy %d, reference %d", seed, step, side.owner.win.Ordinal, side.copy_.win.Ordinal, ref.count)
		}
	}
}
