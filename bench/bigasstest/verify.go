package main

import (
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"sort"
	"sync"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// errShortPage marks a reply that holds fewer elements than the data requires: a silently
// empty or truncated page. It counts as a miss, not only as a failed check.
var errShortPage = errors.New("short page")

// registry remembers what the run has asked engines to write, by content, so a read can tell
// written content from invented content. Writes are registered before they are sent.
type registry struct {
	elements sync.Map // uint64 content hash -> struct{}
	docs     sync.Map // "Bucket/ID" -> canonical fields
}

func newRegistry() *registry { return &registry{} }

func elementHash(e model.Element) uint64 {
	digest := fnv.New64a()
	_, _ = digest.Write([]byte(canonElement(e)))
	return digest.Sum64()
}

// add registers every element and document of op.
func (r *registry) add(op *WriteOp) {
	for _, e := range op.Elements {
		r.elements.Store(elementHash(e), struct{}{})
	}
	for _, doc := range op.Documents {
		r.docs.Store(doc.Bucket+"/"+doc.ID, canonBody(doc.Fields))
	}
}

func (r *registry) hasElement(e model.Element) bool {
	_, ok := r.elements.Load(elementHash(e))
	return ok
}

// checkDoc accepts a document that is seeded or was written, comparing fields only: the
// counters of a SQL row change during a run.
func checkDoc(ds *Dataset, reg *registry, doc model.Document) error {
	if seeded, ok := ds.doc(doc.Bucket, doc.ID); ok {
		if canonBody(seeded.Fields) != canonBody(doc.Fields) {
			return fmt.Errorf("document %s/%s differs from the seeded one: got %s want %s", doc.Bucket, doc.ID, canonBody(doc.Fields), canonBody(seeded.Fields))
		}
		return nil
	}
	written, ok := reg.docs.Load(doc.Bucket + "/" + doc.ID)
	if !ok {
		return fmt.Errorf("document %s/%s was neither seeded nor written", doc.Bucket, doc.ID)
	}
	if written.(string) != canonBody(doc.Fields) {
		return fmt.Errorf("document %s/%s differs from what was written: got %s want %s", doc.Bucket, doc.ID, canonBody(doc.Fields), written)
	}
	return nil
}

// checkStore verifies the elements one read returned from one store. Ordered stores (fifo,
// lifo, btree) must start with the seeded elements in order. Every other element must be
// content the run wrote. A hash store is a set: seeded keys may be replaced by written
// content, every key appears once, and a listing that is not capped holds every seeded key.
func checkStoreAcked(ds *Dataset, reg *registry, ref StoreRef, kind StoreKind, limit, offset, acked int, got []model.Element) error {
	seeded := ds.elements(ref)
	if offset > 0 {
		// A page at an offset is the seeded elements from there on. The planner keeps
		// offset+limit inside them, so the page holds no written element and is exact.
		if kind == Hash {
			return fmt.Errorf("%s/%s/%s: a hash store has no order to take an offset of %d in", ref.Bucket, ref.Owner, ref.Store, offset)
		}
		if limit < 1 || offset+limit > len(seeded) {
			return fmt.Errorf("%s/%s/%s: a page of %d at offset %d runs past the %d seeded elements, so it cannot be checked exactly (a planner bug)", ref.Bucket, ref.Owner, ref.Store, limit, offset, len(seeded))
		}
		seeded = seeded[offset:]
	}
	if limit > 0 && len(got) > limit {
		return fmt.Errorf("%s/%s/%s returned %d elements for a limit of %d", ref.Bucket, ref.Owner, ref.Store, len(got), limit)
	}
	for _, e := range got {
		if e.Bucket != ref.Bucket || e.Owner != ref.Owner || e.Store != ref.Store {
			return fmt.Errorf("element of %s/%s/%s found in the page of %s/%s/%s", e.Bucket, e.Owner, e.Store, ref.Bucket, ref.Owner, ref.Store)
		}
	}
	// A push acknowledged before the read began must be visible to it.
	need := len(seeded) + acked
	if limit > 0 && limit < need {
		need = limit
	}
	if kind == Hash {
		return checkHash(ds, reg, ref, seeded, need, limit, got)
	}
	if len(got) < need {
		return fmt.Errorf("%w: %s/%s/%s returned %d elements, the seeded data holds %d", errShortPage, ref.Bucket, ref.Owner, ref.Store, len(got), need)
	}
	for i, e := range got {
		if i < len(seeded) {
			if canonElement(e) != canonElement(seeded[i]) {
				return fmt.Errorf("%s/%s/%s element %d differs from the seeded one: got %s want %s", ref.Bucket, ref.Owner, ref.Store, i, canonElement(e), canonElement(seeded[i]))
			}
			continue
		}
		if !reg.hasElement(e) {
			return fmt.Errorf("%s/%s/%s element %d (%s) was neither seeded nor written", ref.Bucket, ref.Owner, ref.Store, i, canonElement(e))
		}
	}
	return nil
}

func checkHash(ds *Dataset, reg *registry, ref StoreRef, seeded []model.Element, need, limit int, got []model.Element) error {
	if len(got) < need {
		return fmt.Errorf("%w: %s/%s/%s returned %d elements, the seeded data holds %d", errShortPage, ref.Bucket, ref.Owner, ref.Store, len(got), need)
	}
	byKey := map[string]model.Element{}
	for _, e := range got {
		if _, dup := byKey[e.Key]; dup {
			return fmt.Errorf("%s/%s/%s returned key %q twice", ref.Bucket, ref.Owner, ref.Store, e.Key)
		}
		byKey[e.Key] = e
		seededSame := false
		for _, s := range seeded {
			if s.Key == e.Key && canonElement(s) == canonElement(e) {
				seededSame = true
				break
			}
		}
		if !seededSame && !reg.hasElement(e) {
			return fmt.Errorf("%s/%s/%s key %q holds content that was neither seeded nor written: %s", ref.Bucket, ref.Owner, ref.Store, e.Key, canonElement(e))
		}
	}
	if limit == 0 || len(got) < limit {
		for _, s := range seeded {
			if _, ok := byKey[s.Key]; !ok {
				return fmt.Errorf("%w: %s/%s/%s lacks seeded key %q", errShortPage, ref.Bucket, ref.Owner, ref.Store, s.Key)
			}
		}
	}
	return nil
}

// split returns the elements of one store.
func split(elements []model.Element, store string) []model.Element {
	var out []model.Element
	for _, e := range elements {
		if e.Store == store {
			out = append(out, e)
		}
	}
	return out
}

// bodyString reads a string field of an element body.
func bodyString(e model.Element, name string) string {
	for _, f := range e.Body {
		if f.Name == name {
			s, _ := f.Value.(string)
			return s
		}
	}
	return ""
}

// verifyRead checks one reply against the model and the registry. It returns nil for a
// verified hit, an error wrapping errShortPage for a miss, and any other error for a
// failed check.
func verifyRead(w *world, reg *registry, op ReadOp, reply Reply) error {
	return verifyReadFloor(w, reg, op, reply, nil)
}

// readRefs are the stores a read pages through, whose acknowledged pushes it must show.
func readRefs(op ReadOp) []StoreRef {
	e := op.Entity
	switch op.Kind {
	case ReadSocialFeed:
		return []StoreRef{{"User", e, "Feed"}}
	case ReadSocialInbox:
		return []StoreRef{{"User", e, "Inbox"}}
	case ReadSocialFollowers:
		return []StoreRef{{"User", e, "Followers"}}
	case ReadChatRoomPage:
		return []StoreRef{{"Room", e, "Messages"}, {"Room", e, "Members"}}
	case ReadChatUserRooms:
		return []StoreRef{{"User", e, "Rooms"}}
	case ReadLedgerInvoices:
		return []StoreRef{{"Account", e, "Invoices"}}
	case ReadLedgerPage, ReadLedgerAll:
		return []StoreRef{{"Account", e, "Expenses"}}
	case ReadLedgerTotals:
		return nil // no store is paged; the totals are checked against the seeded data alone
	case ReadAgentMemories:
		return []StoreRef{{"User", e, "Memories"}}
	case ReadAgentSession:
		return []StoreRef{{"Session", e, "Messages"}}
	}
	return nil
}

// verifyReadFloor is verifyRead where acked says how many elements of a store were acknowledged
// as pushed before the read started (nil for none).
func verifyReadFloor(w *world, reg *registry, op ReadOp, reply Reply, acked func(StoreRef) int) error {
	ds := w.sets[op.Layout]
	e := op.Entity
	checkStore := func(ds *Dataset, reg *registry, ref StoreRef, kind StoreKind, limit, offset int, got []model.Element) error {
		floor := 0
		if acked != nil {
			floor = acked(ref)
		}
		return checkStoreAcked(ds, reg, ref, kind, limit, offset, floor, got)
	}
	switch op.Kind {
	case ReadSocialFeed:
		if err := checkStore(ds, reg, StoreRef{"User", e, "Feed"}, FIFO, op.Limit, op.Offset, reply.Elements); err != nil {
			return err
		}
		if len(reply.Documents) != len(reply.Elements) {
			return fmt.Errorf("feed of %s has %d elements and %d posts", e, len(reply.Elements), len(reply.Documents))
		}
		for i, doc := range reply.Documents {
			if want := bodyString(reply.Elements[i], "post"); doc.Bucket != "Post" || doc.ID != want {
				return fmt.Errorf("feed of %s: post %d is %s/%s, the element names %s", e, i, doc.Bucket, doc.ID, want)
			}
			if err := checkDoc(ds, reg, doc); err != nil {
				return err
			}
		}
		return nil
	case ReadSocialInbox:
		return checkStore(ds, reg, StoreRef{"User", e, "Inbox"}, FIFO, op.Limit, op.Offset, reply.Elements)
	case ReadSocialFollowers:
		return checkStore(ds, reg, StoreRef{"User", e, "Followers"}, Hash, op.Limit, op.Offset, reply.Elements)
	case ReadSocialFollowScan:
		return verifyFollowScan(ds, reg, e, reply.Documents)
	case ReadChatRoomPage:
		if err := checkStore(ds, reg, StoreRef{"Room", e, "Messages"}, FIFO, op.Limit, op.Offset, split(reply.Elements, "Messages")); err != nil {
			return err
		}
		members := split(reply.Elements, "Members")
		if err := checkStore(ds, reg, StoreRef{"Room", e, "Members"}, Hash, op.MemberLimit, 0, members); err != nil {
			return err
		}
		if len(split(reply.Elements, "Messages"))+len(members) != len(reply.Elements) {
			return fmt.Errorf("room page of %s holds elements of a store it did not ask for", e)
		}
		names := map[string]bool{}
		for _, doc := range reply.Documents {
			if doc.Bucket != "User" {
				return fmt.Errorf("room page of %s returned a %s document, want User", e, doc.Bucket)
			}
			if err := checkDoc(ds, reg, doc); err != nil {
				return err
			}
			names[doc.ID] = true
		}
		for _, member := range members {
			if !names[member.Key] {
				return fmt.Errorf("%w: room page of %s has no name for member %s", errShortPage, e, member.Key)
			}
		}
		return nil
	case ReadChatUserRooms:
		return checkStore(ds, reg, StoreRef{"User", e, "Rooms"}, Hash, op.Limit, op.Offset, reply.Elements)
	case ReadLedgerPage, ReadLedgerAll:
		return checkStore(ds, reg, StoreRef{"Account", e, "Expenses"}, BTree, op.Limit, op.Offset, reply.Elements)
	case ReadLedgerTotals:
		return verifyTotals(ds, e, reply)
	case ReadLedgerInvoices:
		return checkStore(ds, reg, StoreRef{"Account", e, "Invoices"}, Hash, op.Limit, op.Offset, reply.Elements)
	case ReadAgentMemories:
		return checkStore(ds, reg, StoreRef{"User", e, "Memories"}, Hash, op.Limit, op.Offset, reply.Elements)
	case ReadAgentSession:
		return checkStore(ds, reg, StoreRef{"Session", e, "Messages"}, FIFO, op.Limit, op.Offset, reply.Elements)
	}
	return fmt.Errorf("no verifier for read %s", op.Kind)
}

func verifyFollowScan(ds *Dataset, reg *registry, user string, docs []model.Document) error {
	seen := map[string]bool{}
	for _, doc := range docs {
		if doc.Bucket != "Follow" {
			return fmt.Errorf("follow scan returned a %s document", doc.Bucket)
		}
		if err := checkDoc(ds, reg, doc); err != nil {
			return err
		}
		for _, f := range doc.Fields {
			if f.Name == "To" && f.Value != user {
				return fmt.Errorf("follow scan for %s returned %s", user, doc.ID)
			}
		}
		if seen[doc.ID] {
			return fmt.Errorf("follow scan returned %s twice", doc.ID)
		}
		seen[doc.ID] = true
	}
	for _, follower := range ds.elements(StoreRef{"User", user, "Followers"}) {
		if !seen[follower.Key+">"+user] {
			return fmt.Errorf("%w: follow scan for %s lacks %s", errShortPage, user, follower.Key)
		}
	}
	return nil
}

// verifyClaim checks the task an agent.task write claimed: it must belong to the run's
// queue and be seeded or written content.
func verifyClaim(w *world, reg *registry, run string, claimed []model.Element) error {
	if len(claimed) != 1 {
		return fmt.Errorf("claim on %s returned %d tasks, want 1", run, len(claimed))
	}
	e := claimed[0]
	if e.Bucket != "Run" || e.Owner != run || e.Store != "Tasks" {
		return fmt.Errorf("claim on %s returned a task of %s/%s/%s", run, e.Bucket, e.Owner, e.Store)
	}
	for _, s := range w.sets[model.Agent].elements(StoreRef{"Run", run, "Tasks"}) {
		if canonElement(s) == canonElement(e) {
			return nil
		}
	}
	if !reg.hasElement(e) {
		return fmt.Errorf("claim on %s returned a task nobody seeded or wrote: %s", run, canonElement(e))
	}
	return nil
}

// expectedTotals is the sum of amount and the count of the seeded expenses of one account by
// category, sorted by category.
func expectedTotals(ds *Dataset, account string) []CategoryTotal {
	return totalsOf(ds.elements(StoreRef{"Account", account, "Expenses"}))
}

// totalsOf is the sum of amount and the count of expenses by category, sorted by category.
func totalsOf(expenses []model.Element) []CategoryTotal {
	byCategory := map[string]*CategoryTotal{}
	for _, e := range expenses {
		category := bodyString(e, "category")
		entry := byCategory[category]
		if entry == nil {
			entry = &CategoryTotal{Category: category}
			byCategory[category] = entry
		}
		entry.Total += fieldInt(e, "amount")
		entry.Count++
	}
	out := make([]CategoryTotal, 0, len(byCategory))
	for _, entry := range byCategory {
		out = append(out, *entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Category < out[j].Category })
	return out
}

// verifyTotals checks a totals reply against the seeded expenses. The read is record-only, so no
// expense is written while it runs and the seeded totals are the exact answer.
func verifyTotals(ds *Dataset, account string, reply Reply) error {
	if len(reply.Elements) != 0 || len(reply.Documents) != 0 {
		return fmt.Errorf("totals of %s: the reply holds %d elements and %d documents besides the totals", account, len(reply.Elements), len(reply.Documents))
	}
	want := expectedTotals(ds, account)
	for i := 1; i < len(reply.Totals); i++ {
		if reply.Totals[i-1].Category >= reply.Totals[i].Category {
			return fmt.Errorf("totals of %s are not in category order at %q (the adapter must return them sorted)", account, reply.Totals[i].Category)
		}
	}
	got := map[string]CategoryTotal{}
	for _, entry := range reply.Totals {
		got[entry.Category] = entry
	}
	for _, entry := range want {
		held, ok := got[entry.Category]
		switch {
		case !ok:
			return fmt.Errorf("%w: totals of %s lack category %s", errShortPage, account, entry.Category)
		case held != entry:
			return fmt.Errorf("totals of %s, category %s: got %d over %d expenses, want %d over %d", account, entry.Category, held.Total, held.Count, entry.Total, entry.Count)
		}
	}
	if len(reply.Totals) != len(want) {
		for _, entry := range reply.Totals {
			if !slices.ContainsFunc(want, func(w CategoryTotal) bool { return w.Category == entry.Category }) {
				return fmt.Errorf("totals of %s hold category %s, which no expense has", account, entry.Category)
			}
		}
	}
	return nil
}
