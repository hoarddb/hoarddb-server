package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// fakeEngine is an in-memory engine loaded from the model. It is the reference
// implementation of the adapter rules and the engine the runner's unit tests use. The fault
// switches let a test prove the checks catch a bad engine.
type fakeEngine struct {
	base    string
	mu      sync.Mutex
	layouts map[model.Layout]*fakeLayout

	// emptyPages makes every read return nothing; dropWrites acknowledges a write without
	// applying it; reorder reverses every page; seedNothing leaves the layouts empty.
	emptyPages, dropWrites, reorder bool
	closed                          bool
	// retryWrites makes every write report one retry; settingsReads counts Settings calls.
	retryWrites   bool
	settingsReads int
	// ignoreReplace acknowledges a put over an existing key without storing it; skipCounters
	// acknowledges a write without applying its counter deltas.
	ignoreReplace, skipCounters bool
	// hangReads makes a read block until its context ends; ignoreOffset serves every page from
	// the first element, as an engine that drops the offset would.
	hangReads, ignoreOffset bool
	// claimNewest makes a claim hand out the newest task instead of the oldest; closeError is
	// what closing a session returns.
	claimNewest bool
	closeError  error
	// native makes the fake hold the hoardDB-native layout: seeded and written User.Feed and
	// Room.Members elements carry their copies, and the two native reads rebuild the documents
	// from them, as the ring adapter does. corruptCopy stores a wrong copy in every written
	// element; corruptSeeded stores a wrong copy in the first seeded element of every such store.
	native, corruptCopy, corruptSeeded bool
	// aggregates makes the fake report the ledger's declared aggregates (settings aggregates=on)
	// and answer ReadSum from the stored expenses; corruptSum answers one short, as an engine
	// whose maintained sum lost a write would.
	aggregates, corruptSum bool
	// corruptTotals answers ledger.totals_by_category one short in every first total.
	corruptTotals bool
	// extraSettings are added to what Settings reports, as an adapter with switches would.
	extraSettings map[string]string
}

type fakeLayout struct {
	mu     sync.Mutex
	layout model.Layout
	docs   map[string]model.Document
	stores map[StoreRef]*fakeStore
}

type fakeStore struct {
	kind StoreKind
	list []model.Element
}

func newFakeEngine(base string) *fakeEngine {
	return &fakeEngine{base: base, layouts: map[model.Layout]*fakeLayout{}}
}

// load fills a layout the way the scenarios binary does.
func (f *fakeEngine) load(layout model.Layout, seed uint64, scale int) error {
	return f.loadWith(layout, seed, scale, model.Options{})
}

// loadWith is load with generator options, for a deep ledger.
func (f *fakeEngine) loadWith(layout model.Layout, seed uint64, scale int, opts model.Options) error {
	gen, err := model.NewWith(layout, seed, scale, opts)
	if err != nil {
		return err
	}
	fl := &fakeLayout{layout: layout, docs: map[string]model.Document{}, stores: map[StoreRef]*fakeStore{}}
	native := gen.Native()
	for unit := range gen.Units() {
		fl.docs[unit.Doc.Bucket+"/"+unit.Doc.ID] = unit.Doc
		for _, e := range unit.Elements {
			kind, err := KindOf(layout, e.Bucket, e.Store)
			if err != nil {
				return err
			}
			ref := StoreRef{e.Bucket, e.Owner, e.Store}
			if fl.stores[ref] == nil {
				fl.stores[ref] = &fakeStore{kind: kind}
			}
			if f.native {
				extra, err := native.Embed(e)
				if err != nil {
					return err
				}
				if f.corruptSeeded && len(extra) > 0 && len(fl.stores[ref].list) == 0 {
					extra = append([]model.Field(nil), extra...)
					extra[len(extra)-1].Value = "corrupt"
				}
				e.Body = append(append([]model.Field(nil), e.Body...), extra...)
			}
			fl.stores[ref].list = append(fl.stores[ref].list, e)
		}
	}
	f.mu.Lock()
	f.layouts[layout] = fl
	f.mu.Unlock()
	return nil
}

func (f *fakeEngine) Settings(context.Context) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.settingsReads++
	settings := map[string]string{"engine_version": "fake-1", "tls": "off", "settings_reads": strconv.Itoa(f.settingsReads)}
	if f.native {
		settings["native"] = "on"
	}
	if f.aggregates {
		settings["aggregates"] = "on"
	}
	for key, value := range f.extraSettings {
		settings[key] = value
	}
	return settings, nil
}

func (f *fakeEngine) Session(context.Context, int) (Session, error) { return &fakeSession{f}, nil }

func (f *fakeEngine) Close() error { f.closed = true; return nil }

type fakeSession struct{ f *fakeEngine }

func (s *fakeSession) Layout(l model.Layout, database string) (LayoutOps, error) {
	if want := layoutDatabase(s.f.base, l); database != want {
		return nil, fmt.Errorf("fake: database %q, want %q", database, want)
	}
	s.f.mu.Lock()
	fl := s.f.layouts[l]
	s.f.mu.Unlock()
	if fl == nil {
		return nil, fmt.Errorf("fake: layout %s is not loaded", l)
	}
	return &fakeOps{f: s.f, fl: fl}, nil
}

func (s *fakeSession) Close() error { return s.f.closeError }

type fakeOps struct {
	f  *fakeEngine
	fl *fakeLayout
}

func (o *fakeOps) page(ref StoreRef, limit int) []model.Element { return o.pageAt(ref, 0, limit) }

// pageAt is page after skipping offset elements (none when the fake ignores offsets).
func (o *fakeOps) pageAt(ref StoreRef, offset, limit int) []model.Element {
	st := o.fl.stores[ref]
	if st == nil {
		return nil
	}
	list := append([]model.Element(nil), st.list...)
	if offset > 0 && !o.f.ignoreOffset {
		list = list[min(offset, len(list)):]
	}
	if limit > 0 && len(list) > limit {
		list = list[:limit]
	}
	return list
}

func (o *fakeOps) Read(ctx context.Context, op ReadOp) (Reply, error) {
	if o.f.hangReads {
		<-ctx.Done()
		return Reply{}, ctx.Err()
	}
	o.fl.mu.Lock()
	defer o.fl.mu.Unlock()
	reply := Reply{}
	e := op.Entity
	rt := 1
	switch op.Kind {
	case ReadSocialFeed:
		reply.Elements = o.pageAt(StoreRef{"User", e, "Feed"}, op.Offset, op.Limit)
		for _, el := range reply.Elements {
			reply.Documents = append(reply.Documents, o.fl.docs["Post/"+bodyString(el, "post")])
			rt++
		}
	case ReadSocialInbox:
		reply.Elements = o.pageAt(StoreRef{"User", e, "Inbox"}, op.Offset, op.Limit)
	case ReadSocialFollowers:
		reply.Elements = o.page(StoreRef{"User", e, "Followers"}, op.Limit)
	case ReadSocialFollowScan:
		for _, doc := range o.fl.docs {
			if doc.Bucket == "Follow" && doc.Fields[1].Value == e {
				reply.Documents = append(reply.Documents, doc)
			}
		}
		sort.Slice(reply.Documents, func(i, j int) bool { return reply.Documents[i].ID < reply.Documents[j].ID })
	case ReadChatRoomPage:
		reply.Elements = o.pageAt(StoreRef{"Room", e, "Messages"}, op.Offset, op.Limit)
		members := o.page(StoreRef{"Room", e, "Members"}, op.MemberLimit)
		reply.Elements = append(reply.Elements, members...)
		for _, m := range members {
			reply.Documents = append(reply.Documents, o.fl.docs["User/"+m.Key])
			rt++
		}
	case ReadChatUserRooms:
		reply.Elements = o.page(StoreRef{"User", e, "Rooms"}, op.Limit)
	case ReadLedgerPage, ReadLedgerAll:
		reply.Elements = o.pageAt(StoreRef{"Account", e, "Expenses"}, op.Offset, op.Limit)
	case ReadLedgerTotals:
		byCategory := map[string]*CategoryTotal{}
		for _, el := range o.page(StoreRef{"Account", e, "Expenses"}, 0) {
			entry := byCategory[bodyString(el, "category")]
			if entry == nil {
				entry = &CategoryTotal{Category: bodyString(el, "category")}
				byCategory[entry.Category] = entry
			}
			entry.Total += fieldInt(el, "amount")
			entry.Count++
		}
		for _, entry := range byCategory {
			reply.Totals = append(reply.Totals, *entry)
		}
		sort.Slice(reply.Totals, func(i, j int) bool { return reply.Totals[i].Category < reply.Totals[j].Category })
		if o.f.corruptTotals && len(reply.Totals) > 0 {
			reply.Totals[0].Total--
		}
		rt = 1
	case ReadLedgerInvoices:
		reply.Elements = o.page(StoreRef{"Account", e, "Invoices"}, op.Limit)
	case ReadAgentMemories:
		reply.Elements = o.page(StoreRef{"User", e, "Memories"}, op.Limit)
	case ReadAgentSession:
		reply.Elements = o.pageAt(StoreRef{"Session", e, "Messages"}, op.Offset, op.Limit)
	default:
		return Reply{}, fmt.Errorf("fake: unknown read %s", op.Kind)
	}
	reply.RoundTrips = rt
	if o.f.native && (op.Kind == ReadSocialFeed || op.Kind == ReadChatRoomPage) {
		elements, docs, err := nativeSplit(reply.Elements)
		if err != nil {
			return Reply{}, err
		}
		reply.Elements, reply.Documents = elements, docs
		reply.RoundTrips = map[ReadKind]int{ReadSocialFeed: 1, ReadChatRoomPage: 2}[op.Kind]
	}
	if o.f.emptyPages {
		reply.Elements, reply.Documents = nil, nil
	}
	if o.f.reorder {
		for i, j := 0, len(reply.Elements)-1; i < j; i, j = i+1, j-1 {
			reply.Elements[i], reply.Elements[j] = reply.Elements[j], reply.Elements[i]
		}
	}
	return reply, nil
}

// apply stores el. A push whose seq is already in the store is a duplicate-key error, as on
// every real engine (the seq is part of the primary key or the unique index).
func (o *fakeOps) apply(el model.Element) (bool, error) {
	ref := StoreRef{el.Bucket, el.Owner, el.Store}
	st := o.fl.stores[ref]
	if st == nil {
		kind, _ := KindOf(o.fl.layout, el.Bucket, el.Store)
		st = &fakeStore{kind: kind}
		o.fl.stores[ref] = st
	}
	if st.kind.Keyed() {
		for i, have := range st.list {
			if have.Key == el.Key {
				if !o.f.ignoreReplace {
					st.list[i] = el
				}
				return false, nil
			}
		}
		st.list = append(st.list, el)
		if st.kind == BTree {
			sort.SliceStable(st.list, func(i, j int) bool { return st.list[i].Key < st.list[j].Key })
		}
		return true, nil
	}
	if el.Seq != 0 {
		for _, have := range st.list {
			if have.Seq == el.Seq {
				return false, fmt.Errorf("fake: duplicate key (%s, %s, seq %d)", el.Bucket, el.Owner, el.Seq)
			}
		}
	}
	st.list = append(st.list, el)
	return true, nil
}

func (o *fakeOps) Write(_ context.Context, op WriteOp) (Reply, error) {
	o.fl.mu.Lock()
	defer o.fl.mu.Unlock()
	reply := Reply{RoundTrips: len(op.PreRead)}
	if o.f.retryWrites {
		reply.Retries = 1
	}
	if o.f.dropWrites {
		return reply, nil
	}
	if op.Claim != nil {
		ref := StoreRef{"Run", op.Claim.Run, "Tasks"}
		st := o.fl.stores[ref]
		reply.RoundTrips += 2
		if st == nil || len(st.list) == 0 {
			return reply, nil
		}
		if o.f.claimNewest {
			reply.Elements = []model.Element{st.list[len(st.list)-1]}
			st.list = st.list[:len(st.list)-1]
		} else {
			reply.Elements = []model.Element{st.list[0]}
			st.list = st.list[1:]
		}
	}
	for _, doc := range op.Documents {
		o.fl.docs[doc.Bucket+"/"+doc.ID] = doc
		reply.RoundTrips++
	}
	created := make([]bool, len(op.Elements))
	for i, el := range op.Elements {
		if o.f.native {
			copied, err := withCopy(op, &el)
			if err != nil {
				return reply, err
			}
			el = *copied
			if o.f.corruptCopy && model.IsNativeStore(el.Bucket, el.Store) {
				el.Body = append([]model.Field(nil), el.Body...)
				el.Body[len(el.Body)-1].Value = "corrupt"
			}
		}
		made, err := o.apply(el)
		if err != nil {
			return reply, err
		}
		created[i] = made
		reply.RoundTrips++
	}
	if !o.f.skipCounters {
		for _, d := range op.Counters {
			if d.Element >= 0 && !created[d.Element] {
				continue
			}
			o.addCounter(d)
		}
	}
	return reply, nil
}

func (o *fakeOps) Count(_ context.Context, refs []StoreRef) (map[StoreRef]int64, error) {
	o.fl.mu.Lock()
	defer o.fl.mu.Unlock()
	out := map[StoreRef]int64{}
	for _, ref := range refs {
		if ref.Store == "" {
			for _, doc := range o.fl.docs {
				if doc.Bucket == ref.Bucket {
					out[ref]++
				}
			}
			continue
		}
		if st := o.fl.stores[ref]; st != nil {
			out[ref] = int64(len(st.list))
		}
	}
	return out, nil
}

// addCounter adds a delta to a counter of a stored document.
func (o *fakeOps) addCounter(d CounterDelta) {
	key := d.Bucket + "/" + d.ID
	doc := o.fl.docs[key]
	counters := append([]model.Field(nil), doc.Counters...)
	for i, f := range counters {
		if f.Name == d.Field {
			counters[i].Value = toInt64(f.Value) + d.Delta
			doc.Counters = counters
			o.fl.docs[key] = doc
			return
		}
	}
	doc.Counters = append(counters, model.Field{Name: d.Field, Value: d.Delta})
	o.fl.docs[key] = doc
}

// ReadStore implements StoreReader.
func (o *fakeOps) ReadStore(_ context.Context, ref StoreRef) ([]model.Element, error) {
	o.fl.mu.Lock()
	defer o.fl.mu.Unlock()
	if o.f.native {
		common, _, err := nativeSplit(o.page(ref, 0))
		return common, err
	}
	return o.page(ref, 0), nil
}

// ReadSum implements SumReader: the sum of field over the stored elements, one short when
// corruptSum is set.
func (o *fakeOps) ReadSum(_ context.Context, ref StoreRef, field string) (int64, error) {
	o.fl.mu.Lock()
	defer o.fl.mu.Unlock()
	var sum int64
	for _, e := range o.page(ref, 0) {
		sum += fieldInt(e, field)
	}
	if o.f.corruptSum {
		sum--
	}
	return sum, nil
}

// ReadEmbeds implements EmbedReader: the stored bodies, copies included.
func (o *fakeOps) ReadEmbeds(_ context.Context, ref StoreRef) ([]model.Element, error) {
	o.fl.mu.Lock()
	defer o.fl.mu.Unlock()
	return o.page(ref, 0), nil
}

// ReadCounters implements CounterReader.
func (o *fakeOps) ReadCounters(_ context.Context, bucket, id string) (map[string]int64, error) {
	o.fl.mu.Lock()
	defer o.fl.mu.Unlock()
	doc, ok := o.fl.docs[bucket+"/"+id]
	if !ok {
		return nil, fmt.Errorf("fake: no document %s/%s", bucket, id)
	}
	out := map[string]int64{}
	for _, f := range doc.Counters {
		out[f.Name] = toInt64(f.Value)
	}
	return out, nil
}
