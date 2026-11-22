package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// auditChunk is how many stores one Count call asks about.
const auditChunk = 500

// book keeps what the run was told was written: acknowledged writes per store, the keys
// they created, and the running ledger sums. After the run the audit compares it with what
// the engine says it holds, so a lost write fails the repetition.
type book struct {
	w        *world
	mu       sync.Mutex
	state    map[StoreRef]*refState
	sums     map[string]int64
	sumsOff  map[string]bool
	counters map[counterKey]*counterState
	claims   map[string]*claimState
	written  int64
	clock    int64
	// native is set when the engine holds the native layout variant: the audit then compares
	// the embedded copies of the stores the run wrote to with their sources.
	native bool
	// aggregates is set when the engine maintains the ledger's declared aggregates: the audit
	// then also compares the maintained expense sum of every account with the elements.
	aggregates bool
	// ledgerWrites says the mix writes to the ledger, so an aggregates run must have audited at least
	// one maintained sum; sumsAudited is how many it read, for the record.
	ledgerWrites bool
	sumsAudited  int
	// embedProblems are writes whose copy could not be worked out when they were acknowledged.
	embedProblems []string
	// timeout bounds every read of the audit, like -request-timeout bounds the load; 0 sets no bound.
	timeout time.Duration
}

// bounded gives one audit call its own deadline, so an engine that hangs fails the audit with a
// deadline error instead of stalling the repetition.
func (b *book) bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	if b.timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, b.timeout)
}

// deadlineHint says that a failed audit call ran past its deadline.
func (b *book) deadlineHint(call context.Context, err error) error {
	if err != nil && call.Err() != nil && errors.Is(call.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w (the audit's request deadline of %v passed: the engine did not answer)", err, b.timeout)
	}
	return err
}

type refState struct {
	layout    model.Layout
	added     int64
	keys      map[string]struct{}
	uncertain bool
	// vals and tried hold, per key of a keyed store, the content hashes of the writes the
	// engine acknowledged and of those that failed (and so may or may not have landed).
	vals  map[string][]ackedWrite
	tried map[string][]uint64
	// copies and copiesTried are, in native mode, the embedded fields of the elements of a store
	// that carries a copy, by copyID, as acknowledged writes and as failed writes (which may
	// have landed) gave them.
	copies      map[string]string
	copiesTried map[string][]string
}

// ackedWrite is an acknowledged put of one key: its content, and the logical times it was
// sent (start) and acknowledged (end). A put that finished before another began cannot be the
// last one the engine applied.
type ackedWrite struct {
	start, end int64
	hash       uint64
}

// counterKey names one denormalised counter of one document.
type counterKey struct {
	layout            model.Layout
	bucket, id, field string
}

// counterState is the sum of the deltas the engine was acknowledged to apply, or may have
// applied when uncertain.
type counterState struct {
	sum       int64
	uncertain bool
}

// claimState tracks which seeded tasks of a run a claim has returned, to check claim order.
type claimState struct {
	seeded    map[int]bool
	written   bool
	uncertain bool
}

// expectation is the count the engine must report: exactly count, or at least count when an
// unacknowledged write may also have landed.
type expectation struct {
	count     int64
	uncertain bool
}

func newBook(w *world) *book {
	return &book{w: w, state: map[StoreRef]*refState{}, sums: map[string]int64{}, sumsOff: map[string]bool{},
		counters: map[counterKey]*counterState{}, claims: map[string]*claimState{}}
}

func (b *book) ref(layout model.Layout, ref StoreRef) *refState {
	st := b.state[ref]
	if st == nil {
		st = &refState{layout: layout, keys: map[string]struct{}{}}
		b.state[ref] = st
	}
	return st
}

// ack records a write the engine acknowledged. An agent.task write whose claim found nothing
// pushed nothing, so it adds nothing.
func (b *book) ack(op *WriteOp, reply Reply) {
	if op.Claim != nil && len(reply.Elements) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.clock++
	end := b.clock
	ds := b.w.sets[op.Layout]
	created := make([]bool, len(op.Elements))
	for i, e := range op.Elements {
		ref := StoreRef{e.Bucket, e.Owner, e.Store}
		st := b.ref(op.Layout, ref)
		b.noteCopy(op, e, st, false)
		b.written++
		kind, err := KindOf(op.Layout, e.Bucket, e.Store)
		if err == nil && kind.Keyed() {
			if st.vals == nil {
				st.vals = map[string][]ackedWrite{}
			}
			st.vals[e.Key] = append(st.vals[e.Key], ackedWrite{start: op.started, end: end, hash: elementHash(e)})
			if _, seen := st.keys[e.Key]; seen || ds.seededKey(ref, e.Key) {
				continue
			}
			st.keys[e.Key] = struct{}{}
		}
		created[i] = true
		st.added++
		if e.Store == "Expenses" {
			b.sums[e.Owner] += fieldInt(e, "amount")
		}
	}
	for _, d := range op.Counters {
		if d.Element < 0 || (d.Element < len(created) && created[d.Element]) {
			b.counter(op.Layout, d).sum += d.Delta
		}
	}
	for _, doc := range op.Documents {
		st := b.ref(op.Layout, StoreRef{Bucket: doc.Bucket})
		b.written++
		if _, seeded := ds.doc(doc.Bucket, doc.ID); seeded {
			continue
		}
		if _, seen := st.keys[doc.ID]; seen {
			continue
		}
		st.keys[doc.ID] = struct{}{}
		st.added++
	}
	if op.Claim != nil {
		b.ref(op.Layout, StoreRef{"Run", op.Claim.Run, "Tasks"}).added--
	}
}

// snapshot returns, for the stores a read pages through, how many elements the engine had
// acknowledged as pushed when the read started.
func (b *book) snapshot(op ReadOp) func(StoreRef) int {
	refs := readRefs(op)
	b.mu.Lock()
	defer b.mu.Unlock()
	counts := make(map[StoreRef]int, len(refs))
	for _, ref := range refs {
		if st := b.state[ref]; st != nil {
			counts[ref] = int(st.added)
		}
	}
	return func(ref StoreRef) int { return counts[ref] }
}

// noteClaim records which task a claim returned from the run's queue.
func (b *book) noteClaim(run string, claimed model.Element) {
	b.mu.Lock()
	defer b.mu.Unlock()
	cs := b.claim(run)
	for i, seeded := range b.w.sets[model.Agent].elements(StoreRef{"Run", run, "Tasks"}) {
		if canonElement(seeded) == canonElement(claimed) {
			cs.seeded[i] = true
			return
		}
	}
	cs.written = true
}

func (b *book) claim(run string) *claimState {
	cs := b.claims[run]
	if cs == nil {
		cs = &claimState{seeded: map[int]bool{}}
		b.claims[run] = cs
	}
	return cs
}

// auditClaims checks claim order: while seeded tasks remain, a queue hands out the oldest, so
// the seeded tasks claimed are always the first k, and a written task is claimed only after
// every seeded one. The order among written tasks differs by engine and is not checked.
func (b *book) auditClaims() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var problems []string
	for run, cs := range b.claims {
		if cs.uncertain {
			continue
		}
		total := len(b.w.sets[model.Agent].elements(StoreRef{"Run", run, "Tasks"}))
		for i := range len(cs.seeded) {
			if !cs.seeded[i] {
				problems = append(problems, fmt.Sprintf("claim order of %s: %d seeded tasks were claimed but task %d was not among them", run, len(cs.seeded), i))
				break
			}
		}
		if cs.written && len(cs.seeded) < total {
			problems = append(problems, fmt.Sprintf("claim order of %s: a written task was claimed while %d seeded tasks were unclaimed", run, total-len(cs.seeded)))
		}
	}
	sort.Strings(problems)
	return problems
}

// begin stamps a write with the logical time it is sent.
func (b *book) begin(op *WriteOp) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.clock++
	op.started = b.clock
}

func (b *book) counter(layout model.Layout, d CounterDelta) *counterState {
	key := counterKey{layout, d.Bucket, d.ID, d.Field}
	st := b.counters[key]
	if st == nil {
		st = &counterState{}
		b.counters[key] = st
	}
	return st
}

// unknown records a write whose outcome is not known (it returned an error): everything it
// touches may or may not have landed.
func (b *book) unknown(op *WriteOp) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, e := range op.Elements {
		st := b.ref(op.Layout, StoreRef{e.Bucket, e.Owner, e.Store})
		st.uncertain = true
		b.noteCopy(op, e, st, true)
		if kind, err := KindOf(op.Layout, e.Bucket, e.Store); err == nil && kind.Keyed() {
			if st.tried == nil {
				st.tried = map[string][]uint64{}
			}
			st.tried[e.Key] = append(st.tried[e.Key], elementHash(e))
		}
		if e.Store == "Expenses" {
			b.sumsOff[e.Owner] = true
		}
	}
	for _, d := range op.Counters {
		b.counter(op.Layout, d).uncertain = true
	}
	for _, doc := range op.Documents {
		b.ref(op.Layout, StoreRef{Bucket: doc.Bucket}).uncertain = true
	}
	if op.Claim != nil {
		b.ref(op.Layout, StoreRef{"Run", op.Claim.Run, "Tasks"}).uncertain = true
		b.claim(op.Claim.Run).uncertain = true
	}
}

// ackedWrites is the number of elements and documents acknowledged so far.
func (b *book) ackedWrites() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.written
}

func (b *book) expected(ref StoreRef) expectation {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.expectedLocked(ref)
}

func (b *book) expectedLocked(ref StoreRef) expectation {
	st := b.state[ref]
	if st == nil {
		return expectation{}
	}
	ds := b.w.sets[st.layout]
	seeded := int64(len(ds.elements(ref)))
	if ref.Store == "" {
		seeded = ds.count(ref.Bucket)
	}
	return expectation{count: seeded + st.added, uncertain: st.uncertain}
}

func fieldInt(e model.Element, name string) int64 {
	for _, f := range e.Body {
		if f.Name == name {
			return toInt64(f.Value)
		}
	}
	return 0
}

// audit compares every store of layout the run wrote to with the engine's own count, and
// for the ledger the client's running sums with full reads.
func (b *book) audit(ctx context.Context, layout model.Layout, ops LayoutOps) error {
	b.mu.Lock()
	var refs []StoreRef
	for ref, st := range b.state {
		if st.layout == layout {
			refs = append(refs, ref)
		}
	}
	b.mu.Unlock()
	sort.Slice(refs, func(i, j int) bool { return fmt.Sprint(refs[i]) < fmt.Sprint(refs[j]) })
	var problems []string
	for start := 0; start < len(refs); start += auditChunk {
		chunk := refs[start:min(start+auditChunk, len(refs))]
		call, cancel := b.bounded(ctx)
		counts, err := ops.Count(call, chunk)
		err = b.deadlineHint(call, err)
		cancel()
		if err != nil {
			return fmt.Errorf("audit of %s: counting %d stores: %w", layout, len(chunk), err)
		}
		for _, ref := range chunk {
			want, got := b.expected(ref), counts[ref]
			if got == want.count || (want.uncertain && got > want.count) {
				continue
			}
			problems = append(problems, fmt.Sprintf("%s/%s/%s holds %d, expected %s%d", ref.Bucket, ref.Owner, ref.Store, got, map[bool]string{true: "at least ", false: ""}[want.uncertain], want.count))
		}
	}
	if layout == model.Ledger {
		sumProblems, err := b.auditSums(ctx, ops)
		if err != nil {
			return err
		}
		problems = append(problems, sumProblems...)
		if b.aggregates {
			maintained, err := b.auditMaintainedSums(ctx, ops)
			if err != nil {
				return err
			}
			problems = append(problems, maintained...)
		}
	}
	keyedProblems, err := b.auditKeyed(ctx, layout, ops)
	if err != nil {
		return err
	}
	problems = append(problems, keyedProblems...)
	if layout == model.Agent {
		problems = append(problems, b.auditClaims()...)
	}
	if b.native {
		embedProblems, err := b.auditEmbeds(ctx, layout, ops)
		if err != nil {
			return err
		}
		problems = append(problems, embedProblems...)
	}
	counterProblems, err := b.auditCounters(ctx, layout, ops)
	if err != nil {
		return err
	}
	problems = append(problems, counterProblems...)
	if len(problems) > 0 {
		shown := problems[:min(len(problems), 5)]
		return fmt.Errorf("audit of %s found %d lost or extra writes, the first: %s", layout, len(problems), strings.Join(shown, "; "))
	}
	return nil
}

func (b *book) auditSums(ctx context.Context, ops LayoutOps) ([]string, error) {
	b.mu.Lock()
	accounts := map[string]bool{}
	for account := range b.sums {
		accounts[account] = true
	}
	for account := range b.sumsOff {
		accounts[account] = true
	}
	b.mu.Unlock()
	ds := b.w.sets[model.Ledger]
	var problems []string
	for account := range accounts {
		call, cancel := b.bounded(ctx)
		reply, err := ops.Read(call, ReadOp{Layout: model.Ledger, Kind: ReadLedgerAll, Entity: account})
		err = b.deadlineHint(call, err)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("audit of ledger: reading every expense of %s: %w", account, err)
		}
		var got int64
		for _, e := range reply.Elements {
			got += fieldInt(e, "amount")
		}
		var want int64
		for _, e := range ds.elements(StoreRef{"Account", account, "Expenses"}) {
			want += fieldInt(e, "amount")
		}
		b.mu.Lock()
		want += b.sums[account]
		off := b.sumsOff[account]
		b.mu.Unlock()
		if got != want && (!off || got <= want) {
			problems = append(problems, fmt.Sprintf("the expense sum of %s is %d, the client's running sum is %d", account, got, want))
		}
	}
	sort.Strings(problems)
	return problems, nil
}

// auditMaintainedSums compares, for every Expenses and Invoices store of the ledger the run wrote
// to, the engine's maintained sum of amount with the sum of the elements read back from the same
// store. The run is over, so the two describe one store and must be equal whatever the client counted
// as acknowledged; the elements themselves are checked against seeded plus acknowledged writes by
// the count, key and running-sum audits. It counts what it read (sumsAudited), and an aggregates run
// whose mix writes expenses but read none proved nothing, so that fails.
func (b *book) auditMaintainedSums(ctx context.Context, ops LayoutOps) ([]string, error) {
	sums, ok := ops.(SumReader)
	if !ok {
		return nil, errors.New("audit of ledger: the adapter does not implement SumReader, so the maintained sums cannot be read back although aggregates are on")
	}
	reader, ok := ops.(StoreReader)
	if !ok {
		return nil, errors.New("audit of ledger: the adapter does not implement StoreReader, so the maintained sums cannot be compared with the elements")
	}
	b.mu.Lock()
	var refs []StoreRef
	for ref, st := range b.state {
		if st.layout == model.Ledger && ref.Bucket == "Account" && (ref.Store == "Expenses" || ref.Store == "Invoices") {
			refs = append(refs, ref)
		}
	}
	b.mu.Unlock()
	sort.Slice(refs, func(i, j int) bool { return fmt.Sprint(refs[i]) < fmt.Sprint(refs[j]) })
	var problems []string
	for _, ref := range refs {
		call, cancel := b.bounded(ctx)
		elements, err := reader.ReadStore(call, ref)
		err = b.deadlineHint(call, err)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("audit of ledger: reading back %s of %s: %w", ref.Store, ref.Owner, err)
		}
		var held int64
		for _, e := range elements {
			held += fieldInt(e, "amount")
		}
		call, cancel = b.bounded(ctx)
		maintained, err := sums.ReadSum(call, ref, "amount")
		err = b.deadlineHint(call, err)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("audit of ledger: reading the maintained sum of %s of %s: %w", ref.Store, ref.Owner, err)
		}
		b.sumsAudited++
		if maintained != held {
			problems = append(problems, fmt.Sprintf("the maintained sum of %s of %s is %d, its elements add up to %d", ref.Store, ref.Owner, maintained, held))
		}
	}
	if b.sumsAudited == 0 && b.ledgerWrites {
		problems = append(problems, "aggregates are on and the mix writes to the ledger, but the audit read no maintained sum, so it proved nothing")
	}
	return problems, nil
}

// auditKeyed reads back every keyed store the run wrote to and checks each key it wrote: the
// engine must hold content an acknowledged write gave that key. The count alone cannot see a
// put that replaced an existing key and was lost. Writes of different virtual users may race
// on one key, so any acknowledged content (or content of a failed write) is accepted.
func (b *book) auditKeyed(ctx context.Context, layout model.Layout, ops LayoutOps) ([]string, error) {
	b.mu.Lock()
	var refs []StoreRef
	for ref, st := range b.state {
		if st.layout == layout && (len(st.vals) > 0 || len(st.tried) > 0) {
			refs = append(refs, ref)
		}
	}
	b.mu.Unlock()
	if len(refs) == 0 {
		return nil, nil
	}
	reader, ok := ops.(StoreReader)
	if !ok {
		return []string{fmt.Sprintf("the adapter does not implement StoreReader, so the %d keyed stores written in %s cannot be read back", len(refs), layout)}, nil
	}
	sort.Slice(refs, func(i, j int) bool { return fmt.Sprint(refs[i]) < fmt.Sprint(refs[j]) })
	ds := b.w.sets[layout]
	var problems []string
	for _, ref := range refs {
		call, cancel := b.bounded(ctx)
		got, err := reader.ReadStore(call, ref)
		err = b.deadlineHint(call, err)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("audit of %s: reading back %s/%s/%s: %w", layout, ref.Bucket, ref.Owner, ref.Store, err)
		}
		held := map[string]uint64{}
		for _, e := range got {
			held[e.Key] = elementHash(e)
		}
		b.mu.Lock()
		st := b.state[ref]
		vals, tried := st.vals, st.tried
		b.mu.Unlock()
		seeded := map[string]uint64{}
		for _, e := range ds.elements(ref) {
			seeded[e.Key] = elementHash(e)
		}
		keys := map[string]bool{}
		for key := range vals {
			keys[key] = true
		}
		for key := range tried {
			keys[key] = true
		}
		for key := range keys {
			have, present := held[key]
			switch {
			case len(vals[key]) > 0:
				if !present || (!lastWriters(vals[key], have) && !slices.Contains(tried[key], have)) {
					problems = append(problems, fmt.Sprintf("%s/%s/%s key %q does not hold what an acknowledged write gave it", ref.Bucket, ref.Owner, ref.Store, key))
				}
			case present:
				if want, wasSeeded := seeded[key]; !slices.Contains(tried[key], have) && (!wasSeeded || have != want) {
					problems = append(problems, fmt.Sprintf("%s/%s/%s key %q holds content nobody wrote", ref.Bucket, ref.Owner, ref.Store, key))
				}
			default:
				if _, wasSeeded := seeded[key]; wasSeeded {
					problems = append(problems, fmt.Sprintf("%s/%s/%s seeded key %q vanished", ref.Bucket, ref.Owner, ref.Store, key))
				}
			}
		}
	}
	sort.Strings(problems)
	return problems, nil
}

// auditCounters reads the counters of every document a counter delta named and requires the
// seeded value plus the deltas the book saw applied (at least that, when a failed write may
// have landed). Engines that derive counters from their stores do not implement CounterReader.
func (b *book) auditCounters(ctx context.Context, layout model.Layout, ops LayoutOps) ([]string, error) {
	reader, ok := ops.(CounterReader)
	if !ok {
		return nil, nil
	}
	b.mu.Lock()
	byDoc := map[[2]string][]counterKey{}
	for key := range b.counters {
		if key.layout == layout {
			byDoc[[2]string{key.bucket, key.id}] = append(byDoc[[2]string{key.bucket, key.id}], key)
		}
	}
	b.mu.Unlock()
	docs := make([][2]string, 0, len(byDoc))
	for doc := range byDoc {
		docs = append(docs, doc)
	}
	sort.Slice(docs, func(i, j int) bool { return fmt.Sprint(docs[i]) < fmt.Sprint(docs[j]) })
	ds := b.w.sets[layout]
	var problems []string
	for _, docRef := range docs {
		call, cancel := b.bounded(ctx)
		got, err := reader.ReadCounters(call, docRef[0], docRef[1])
		err = b.deadlineHint(call, err)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("audit of %s: reading the counters of %s/%s: %w", layout, docRef[0], docRef[1], err)
		}
		seeded, _ := ds.doc(docRef[0], docRef[1])
		for _, key := range byDoc[docRef] {
			b.mu.Lock()
			st := *b.counters[key]
			b.mu.Unlock()
			want := st.sum
			for _, f := range seeded.Counters {
				if f.Name == key.field {
					want += toInt64(f.Value)
				}
			}
			have, present := got[key.field]
			if !present || have != want && (!st.uncertain || have <= want) {
				problems = append(problems, fmt.Sprintf("counter %s of %s/%s is %d, the applied writes imply %s%d", key.field, key.bucket, key.id, have, map[bool]string{true: "at least ", false: ""}[st.uncertain], want))
			}
		}
	}
	sort.Strings(problems)
	return problems, nil
}

// lastWriters reports whether hash is the content of an acknowledged put that could have been
// applied last: one that no other acknowledged put began after it finished.
func lastWriters(writes []ackedWrite, hash uint64) bool {
	var latestStart int64
	for _, w := range writes {
		latestStart = max(latestStart, w.start)
	}
	for _, w := range writes {
		if w.hash == hash && w.end > latestStart {
			return true
		}
	}
	return false
}
