package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// countStub answers Count from a map and ReadLedgerAll from a list.
type countStub struct {
	counts   map[StoreRef]int64
	expenses map[string][]model.Element
	// sums are the maintained sums ReadSum answers by account; sumErr makes it fail.
	sums   map[string]int64
	sumErr error
}

// ReadSum implements SumReader.
func (c *countStub) ReadSum(_ context.Context, ref StoreRef, _ string) (int64, error) {
	return c.sums[ref.Owner], c.sumErr
}

func (c *countStub) Read(_ context.Context, op ReadOp) (Reply, error) {
	return Reply{Elements: c.expenses[op.Entity]}, nil
}
func (c *countStub) ReadStore(_ context.Context, ref StoreRef) ([]model.Element, error) {
	return c.expenses[ref.Owner], nil
}
func (c *countStub) Write(context.Context, WriteOp) (Reply, error) { return Reply{}, errors.New("no") }
func (c *countStub) Count(_ context.Context, refs []StoreRef) (map[StoreRef]int64, error) {
	out := map[StoreRef]int64{}
	for _, ref := range refs {
		out[ref] = c.counts[ref]
	}
	return out, nil
}

func pushOp(layout model.Layout, bucket, owner, store, key string) *WriteOp {
	return &WriteOp{Layout: layout, Elements: []model.Element{{Bucket: bucket, Owner: owner, Store: store, Key: key}}}
}

func TestBookExpectedCounts(t *testing.T) {
	w := testWorld(t)
	b := newBook(w)
	ds := w.sets[model.Social]
	user := firstUserWith(t, ds, "Followers", 1)
	seededKey := ds.elements(StoreRef{"User", user, "Followers"})[0].Key
	followers := StoreRef{"User", user, "Followers"}
	feed := StoreRef{"User", user, "Feed"}
	b.ack(pushOp(model.Social, "User", user, "Followers", seededKey), Reply{})
	if got := b.expected(followers); got.count != int64(len(ds.elements(followers))) {
		t.Errorf("replacing a seeded key changed the count to %d", got.count)
	}
	b.ack(pushOp(model.Social, "User", user, "Followers", "new1"), Reply{})
	b.ack(pushOp(model.Social, "User", user, "Followers", "new1"), Reply{})
	if got := b.expected(followers); got.count != int64(len(ds.elements(followers)))+1 {
		t.Errorf("a new key written twice must count once, got %d", got.count)
	}
	b.ack(pushOp(model.Social, "User", user, "Feed", ""), Reply{})
	b.ack(pushOp(model.Social, "User", user, "Feed", ""), Reply{})
	if got := b.expected(feed); got.count != int64(len(ds.elements(feed)))+2 {
		t.Errorf("two pushes must count twice, got %d", got.count)
	}
}

func TestBookTaskQueueIsPushesMinusClaims(t *testing.T) {
	w := testWorld(t)
	b := newBook(w)
	run := w.sets[model.Agent].ids("Run")[0]
	tasks := StoreRef{"Run", run, "Tasks"}
	op := pushOp(model.Agent, "Run", run, "Tasks", "")
	op.Claim = &ClaimRef{Run: run}
	b.ack(op, Reply{Elements: []model.Element{{Bucket: "Run", Owner: run, Store: "Tasks"}}})
	b.ack(op, Reply{}) // the queue gave nothing: nothing was pushed
	if got := b.expected(tasks); got.count != 10 {
		t.Errorf("claim, ack and replacement is net zero, got %d, want the 10 seeded", got.count)
	}
}

func TestAuditCatchesALostWrite(t *testing.T) {
	w := testWorld(t)
	b := newBook(w)
	ds := w.sets[model.Chat]
	room := ds.ids("Room")[0]
	messages := StoreRef{"Room", room, "Messages"}
	for range 3 {
		b.ack(pushOp(model.Chat, "Room", room, "Messages", ""), Reply{})
	}
	seeded := int64(len(ds.elements(messages)))
	ops := &countStub{counts: map[StoreRef]int64{messages: seeded + 3}}
	if err := b.audit(context.Background(), model.Chat, ops); err != nil {
		t.Fatalf("a correct count failed the audit: %v", err)
	}
	ops.counts[messages] = seeded + 2
	err := b.audit(context.Background(), model.Chat, ops)
	if err == nil || !strings.Contains(err.Error(), "Room/"+room+"/Messages") {
		t.Fatalf("a lost write must fail the audit and name the store, got %v", err)
	}
}

func TestAuditToleratesUncertainWritesAboveTheFloor(t *testing.T) {
	w := testWorld(t)
	b := newBook(w)
	ds := w.sets[model.Chat]
	room := ds.ids("Room")[0]
	messages := StoreRef{"Room", room, "Messages"}
	seeded := int64(len(ds.elements(messages)))
	b.ack(pushOp(model.Chat, "Room", room, "Messages", ""), Reply{})
	b.unknown(pushOp(model.Chat, "Room", room, "Messages", ""))
	ops := &countStub{counts: map[StoreRef]int64{messages: seeded + 2}}
	if err := b.audit(context.Background(), model.Chat, ops); err != nil {
		t.Errorf("an unacknowledged write may have landed: %v", err)
	}
	ops.counts[messages] = seeded
	if err := b.audit(context.Background(), model.Chat, ops); err == nil {
		t.Error("below the acknowledged floor must fail")
	}
}

func TestAuditLedgerSum(t *testing.T) {
	w := testWorld(t)
	b := newBook(w)
	ds := w.sets[model.Ledger]
	account := ds.ids("Account")[0]
	expenses := StoreRef{"Account", account, "Expenses"}
	var all []model.Element
	all = append(all, ds.elements(expenses)...)
	op := &WriteOp{Layout: model.Ledger, Kind: WriteLedgerExpense, Elements: []model.Element{{Bucket: "Account", Owner: account, Store: "Expenses", Key: "4000000001-w",
		Body: []model.Field{{Name: "amount", Value: int64(500)}, {Name: "category", Value: "fees"}, {Name: "currency", Value: "EUR"}}}}}
	b.ack(op, Reply{})
	all = append(all, op.Elements[0])
	ops := &countStub{counts: map[StoreRef]int64{expenses: int64(len(all))}, expenses: map[string][]model.Element{account: all}}
	if err := b.audit(context.Background(), model.Ledger, ops); err != nil {
		t.Fatalf("matching sum failed: %v", err)
	}
	changed := append([]model.Element(nil), all...)
	changed[0].Body = []model.Field{{Name: "amount", Value: int64(1)}}
	ops.expenses[account] = changed
	if err := b.audit(context.Background(), model.Ledger, ops); err == nil || !strings.Contains(err.Error(), "sum") {
		t.Errorf("a different sum must fail, got %v", err)
	}
}

func TestBookCountersFollowCreationNotReplacement(t *testing.T) {
	w := testWorld(t)
	b := newBook(w)
	ds := w.sets[model.Social]
	post := ds.ids("Post")[0]
	user := ds.ids("User")[0]
	seededLike := ds.elements(StoreRef{"Post", post, "Likes"})
	like := func(key string) *WriteOp {
		return &WriteOp{Layout: model.Social, Kind: WriteSocialLike,
			Elements: []model.Element{{Bucket: "Post", Owner: post, Store: "Likes", Key: key, Body: []model.Field{{Name: "user", Value: key}}}},
			Counters: []CounterDelta{{Bucket: "Post", ID: post, Field: "likes_n", Delta: 1, Element: 0}}}
	}
	b.ack(like("new-user"), Reply{})
	b.ack(like("new-user"), Reply{})
	if len(seededLike) > 0 {
		b.ack(like(seededLike[0].Key), Reply{})
	}
	if got := b.counters[counterKey{model.Social, "Post", post, "likes_n"}].sum; got != 1 {
		t.Errorf("a new like counts once and a replaced like never: sum %d", got)
	}
	_ = user
}

// With aggregates on, the audit compares the engine's maintained sum with the sum of the elements
// it reads back, which are checked against seeded plus acknowledged amounts.
func TestAuditLedgerMaintainedSum(t *testing.T) {
	w := testWorld(t)
	ds := w.sets[model.Ledger]
	account := ds.ids("Account")[0]
	expenses := StoreRef{"Account", account, "Expenses"}
	all := append([]model.Element(nil), ds.elements(expenses)...)
	var total int64
	for _, e := range all {
		total += fieldInt(e, "amount")
	}
	b := newBook(w)
	b.aggregates = true
	op := &WriteOp{Layout: model.Ledger, Kind: WriteLedgerExpense, Elements: []model.Element{{Bucket: "Account", Owner: account, Store: "Expenses", Key: "4000000001-w",
		Body: []model.Field{{Name: "amount", Value: int64(500)}, {Name: "category", Value: "fees"}, {Name: "currency", Value: "EUR"}}}}}
	b.ack(op, Reply{})
	all = append(all, op.Elements[0])
	total += 500
	ops := &countStub{counts: map[StoreRef]int64{expenses: int64(len(all))}, expenses: map[string][]model.Element{account: all}, sums: map[string]int64{account: total}}
	if err := b.audit(context.Background(), model.Ledger, ops); err != nil {
		t.Fatalf("a maintained sum equal to the elements failed: %v", err)
	}
	ops.sums[account] = total - 500
	if err := b.audit(context.Background(), model.Ledger, ops); err == nil || !strings.Contains(err.Error(), "maintained sum") {
		t.Errorf("a maintained sum that lost the acknowledged write must fail naming it, got %v", err)
	}
	ops.sums[account] = total
	ops.sumErr = errors.New("aggregate not declared")
	if err := b.audit(context.Background(), model.Ledger, ops); err == nil || !strings.Contains(err.Error(), "aggregate not declared") {
		t.Errorf("a failed sum read must fail the audit with its error, got %v", err)
	}
	b.aggregates = false
	ops.sums[account] = 1
	ops.sumErr = nil
	if err := b.audit(context.Background(), model.Ledger, ops); err != nil {
		t.Errorf("with aggregates off the maintained sum is not read: %v", err)
	}
}

// An adapter that cannot read the maintained sum is not audited as if it passed.
func TestAuditAggregatesNeedASumReader(t *testing.T) {
	w := testWorld(t)
	ds := w.sets[model.Ledger]
	account := ds.ids("Account")[0]
	expenses := StoreRef{"Account", account, "Expenses"}
	b := newBook(w)
	b.aggregates = true
	b.ack(&WriteOp{Layout: model.Ledger, Kind: WriteLedgerExpense, Elements: []model.Element{{Bucket: "Account", Owner: account, Store: "Expenses", Key: "4000000001-w",
		Body: []model.Field{{Name: "amount", Value: int64(5)}}}}}, Reply{})
	elements := append(append([]model.Element(nil), ds.elements(expenses)...), model.Element{Bucket: "Account", Owner: account, Store: "Expenses", Key: "4000000001-w", Body: []model.Field{{Name: "amount", Value: int64(5)}}})
	ops := &noSumStub{countStub{counts: map[StoreRef]int64{expenses: int64(len(elements))}, expenses: map[string][]model.Element{account: elements}}}
	if err := b.audit(context.Background(), model.Ledger, ops); err == nil || !strings.Contains(err.Error(), "SumReader") {
		t.Errorf("an adapter without SumReader must fail the audit naming it, got %v", err)
	}
}

// noSumStub is a countStub whose method set has no ReadSum.
type noSumStub struct{ inner countStub }

func (n *noSumStub) Read(ctx context.Context, op ReadOp) (Reply, error) { return n.inner.Read(ctx, op) }
func (n *noSumStub) ReadStore(ctx context.Context, ref StoreRef) ([]model.Element, error) {
	return n.inner.ReadStore(ctx, ref)
}
func (n *noSumStub) Write(ctx context.Context, op WriteOp) (Reply, error) {
	return n.inner.Write(ctx, op)
}
func (n *noSumStub) Count(ctx context.Context, refs []StoreRef) (map[StoreRef]int64, error) {
	return n.inner.Count(ctx, refs)
}

// The audit also covers the Invoices sum, and records how many maintained sums it read.
func TestAuditMaintainedSumCoversInvoicesAndCountsWhatItRead(t *testing.T) {
	w := testWorld(t)
	ds := w.sets[model.Ledger]
	account := ds.ids("Account")[0]
	invoices := StoreRef{"Account", account, "Invoices"}
	elements := append([]model.Element(nil), ds.elements(invoices)...)
	var total int64
	for _, e := range elements {
		total += fieldInt(e, "amount")
	}
	b := newBook(w)
	b.aggregates = true
	b.ack(&WriteOp{Layout: model.Ledger, Kind: WriteLedgerInvoice, Elements: []model.Element{{Bucket: "Account", Owner: account, Store: "Invoices", Key: "INV-new",
		Body: []model.Field{{Name: "amount", Value: int64(70)}}}}}, Reply{})
	elements = append(elements, model.Element{Bucket: "Account", Owner: account, Store: "Invoices", Key: "INV-new", Body: []model.Field{{Name: "amount", Value: int64(70)}}})
	total += 70
	ops := &countStub{counts: map[StoreRef]int64{invoices: int64(len(elements))}, expenses: map[string][]model.Element{account: elements}, sums: map[string]int64{account: total}}
	if err := b.audit(context.Background(), model.Ledger, ops); err != nil {
		t.Fatalf("a matching invoice sum failed: %v", err)
	}
	if b.sumsAudited != 1 {
		t.Errorf("the audit read %d maintained sums, want 1", b.sumsAudited)
	}
	ops.sums[account] = total + 1
	if err := b.audit(context.Background(), model.Ledger, ops); err == nil || !strings.Contains(err.Error(), "Invoices") {
		t.Errorf("a wrong invoice sum must fail naming Invoices, got %v", err)
	}
}

// An aggregates run whose mix writes expenses but audited no maintained sum proved nothing.
func TestAuditFailsWhenAggregatesAreOnAndNothingWasAudited(t *testing.T) {
	w := testWorld(t)
	b := newBook(w)
	b.aggregates, b.ledgerWrites = true, true
	if err := b.audit(context.Background(), model.Ledger, &countStub{counts: map[StoreRef]int64{}}); err == nil || !strings.Contains(err.Error(), "read no maintained sum") {
		t.Errorf("zero audited sums with ledger writes in the mix must fail, got %v", err)
	}
	b.ledgerWrites = false
	if err := b.audit(context.Background(), model.Ledger, &countStub{counts: map[StoreRef]int64{}}); err != nil {
		t.Errorf("a mix with no ledger write has nothing to audit: %v", err)
	}
}
