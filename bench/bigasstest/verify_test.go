package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

func seededReply(ds *Dataset, ref StoreRef, n int) []model.Element {
	list := ds.elements(ref)
	if n > len(list) {
		n = len(list)
	}
	return append([]model.Element(nil), list[:n]...)
}

func firstUserWith(t *testing.T, ds *Dataset, store string, atLeast int) string {
	t.Helper()
	for _, id := range ds.ids("User") {
		if len(ds.elements(StoreRef{"User", id, store})) >= atLeast {
			return id
		}
	}
	t.Fatalf("no user has %d %s", atLeast, store)
	return ""
}

func TestVerifyAcceptsSeededPageAndRejectsDamage(t *testing.T) {
	w := testWorld(t)
	reg := newRegistry()
	ds := w.sets[model.Social]
	user := firstUserWith(t, ds, "Inbox", 3)
	ref := StoreRef{"User", user, "Inbox"}
	op := ReadOp{Layout: model.Social, Kind: ReadSocialInbox, Entity: user, Limit: 20}
	good := seededReply(ds, ref, 20)
	if err := verifyRead(w, reg, op, Reply{Elements: good}); err != nil {
		t.Fatalf("seeded page rejected: %v", err)
	}
	if err := verifyRead(w, reg, op, Reply{}); !errors.Is(err, errShortPage) {
		t.Errorf("an empty page must be a short-page miss, got %v", err)
	}
	if err := verifyRead(w, reg, op, Reply{Elements: good[:len(good)-1]}); !errors.Is(err, errShortPage) {
		t.Errorf("a page one element short must be a miss, got %v", err)
	}
	swapped := append([]model.Element(nil), good...)
	swapped[0], swapped[1] = swapped[1], swapped[0]
	if err := verifyRead(w, reg, op, Reply{Elements: swapped}); err == nil || errors.Is(err, errShortPage) {
		t.Errorf("a reordered page must fail, got %v", err)
	}
	damaged := append([]model.Element(nil), good...)
	damaged[0].Body = []model.Field{{Name: "post", Value: "pX"}, {Name: "author", Value: "u0"}, {Name: "at", Value: int64(1)}}
	if err := verifyRead(w, reg, op, Reply{Elements: damaged}); err == nil {
		t.Error("a changed body must fail")
	}
	// Every owner is seeded with a full page, so a written element can only show in a page that
	// asks for one more than the owner was seeded with.
	all := seededReply(ds, ref, 1000)
	wide := op
	wide.Limit = len(all) + 1
	extra := model.Element{Bucket: "User", Owner: user, Store: "Inbox", Body: []model.Field{{Name: "post", Value: "pw1"}, {Name: "author", Value: "u1"}, {Name: "at", Value: int64(writeEpoch + 1)}}}
	withExtra := append(append([]model.Element(nil), all...), extra)
	if err := verifyRead(w, reg, wide, Reply{Elements: withExtra}); err == nil {
		t.Error("an element nobody wrote must fail")
	}
	reg.add(&WriteOp{Layout: model.Social, Elements: []model.Element{extra}})
	if err := verifyRead(w, reg, wide, Reply{Elements: withExtra}); err != nil {
		t.Errorf("a registered written element must pass: %v", err)
	}
	tight := wide
	tight.Limit = len(all)
	if err := verifyRead(w, reg, tight, Reply{Elements: withExtra}); err == nil {
		t.Error("more elements than the page limit must fail")
	}
}

func TestVerifyHashReadsAreSets(t *testing.T) {
	w := testWorld(t)
	reg := newRegistry()
	ds := w.sets[model.Social]
	user := firstUserWith(t, ds, "Followers", 2)
	op := ReadOp{Layout: model.Social, Kind: ReadSocialFollowers, Entity: user}
	all := seededReply(ds, StoreRef{"User", user, "Followers"}, 100)
	reversed := make([]model.Element, len(all))
	for i, e := range all {
		reversed[len(all)-1-i] = e
	}
	if err := verifyRead(w, reg, op, Reply{Elements: reversed}); err != nil {
		t.Errorf("order must not matter for a hash: %v", err)
	}
	if err := verifyRead(w, reg, op, Reply{Elements: all[1:]}); !errors.Is(err, errShortPage) {
		t.Errorf("a missing seeded key must be a miss, got %v", err)
	}
	dup := append(append([]model.Element(nil), all...), all[0])
	if err := verifyRead(w, reg, op, Reply{Elements: dup}); err == nil {
		t.Error("a repeated key must fail")
	}
}

func TestVerifyFeedChecksPostDocuments(t *testing.T) {
	w := testWorld(t)
	reg := newRegistry()
	ds := w.sets[model.Social]
	user := firstUserWith(t, ds, "Feed", 2)
	op := ReadOp{Layout: model.Social, Kind: ReadSocialFeed, Entity: user, Limit: 10}
	elements := seededReply(ds, StoreRef{"User", user, "Feed"}, 10)
	var docs []model.Document
	for _, e := range elements {
		doc, ok := ds.doc("Post", e.Body[0].Value.(string))
		if !ok {
			t.Fatal("seeded feed names a missing post")
		}
		docs = append(docs, doc)
	}
	if err := verifyRead(w, reg, op, Reply{Elements: elements, Documents: docs}); err != nil {
		t.Fatalf("good feed rejected: %v", err)
	}
	if err := verifyRead(w, reg, op, Reply{Elements: elements, Documents: docs[:len(docs)-1]}); err == nil {
		t.Error("a feed with a post missing must fail")
	}
	bad := append([]model.Document(nil), docs...)
	bad[0].Fields = []model.Field{{Name: "Author", Value: "u0"}, {Name: "Text", Value: "changed"}, {Name: "At", Value: int64(1)}}
	if err := verifyRead(w, reg, op, Reply{Elements: elements, Documents: bad}); err == nil {
		t.Error("a changed post must fail")
	}
}

func TestVerifyIntegerWidthDoesNotMatter(t *testing.T) {
	w := testWorld(t)
	ds := w.sets[model.Ledger]
	account := ds.ids("Account")[0]
	page := seededReply(ds, StoreRef{"Account", account, "Expenses"}, 50)
	for i := range page {
		body := append([]model.Field(nil), page[i].Body...)
		body[0].Value = int(body[0].Value.(int64))
		page[i].Body = body
	}
	op := ReadOp{Layout: model.Ledger, Kind: ReadLedgerPage, Entity: account, Limit: 50}
	if err := verifyRead(w, newRegistry(), op, Reply{Elements: page}); err != nil {
		t.Errorf("int and int64 must compare equal: %v", err)
	}
}

// The two read kinds added for profile R page a hash store, so the shared verifier must accept a
// seeded page and refuse an empty one or one that holds content nobody seeded.
func TestVerifyUserRoomsAndInvoicesPages(t *testing.T) {
	w := testWorld(t)
	cases := []struct {
		layout        model.Layout
		kind          ReadKind
		bucket, store string
	}{
		{model.Chat, ReadChatUserRooms, "User", "Rooms"},
		{model.Ledger, ReadLedgerInvoices, "Account", "Invoices"},
	}
	for _, tc := range cases {
		ds := w.sets[tc.layout]
		var owner string
		for _, id := range ds.ids(tc.bucket) {
			if len(ds.elements(StoreRef{tc.bucket, id, tc.store})) >= 1 {
				owner = id
				break
			}
		}
		if owner == "" {
			t.Fatalf("%s: no owner has any %s", tc.kind, tc.store)
		}
		limit := w.readLimit(tc.kind)
		if limit == 0 {
			t.Fatalf("%s: the page size must be capped, got 0", tc.kind)
		}
		op := ReadOp{Layout: tc.layout, Kind: tc.kind, Entity: owner, Limit: limit}
		good := seededReply(ds, StoreRef{tc.bucket, owner, tc.store}, limit)
		if err := verifyRead(w, newRegistry(), op, Reply{Elements: good}); err != nil {
			t.Errorf("%s: seeded page rejected: %v", tc.kind, err)
		}
		if err := verifyRead(w, newRegistry(), op, Reply{}); !errors.Is(err, errShortPage) {
			t.Errorf("%s: an empty page must be a short-page miss, got %v", tc.kind, err)
		}
		bogus := append([]model.Element(nil), good...)
		bogus[0].Key = "no-such-key"
		if err := verifyRead(w, newRegistry(), op, Reply{Elements: bogus}); err == nil {
			t.Errorf("%s: an element nobody seeded must fail", tc.kind)
		}
	}
}

// A page read at an offset is checked against the seeded elements from that offset, in order.
func TestVerifyPageAtOffset(t *testing.T) {
	w := testWorld(t)
	ds := w.sets[model.Social]
	user := firstUserWith(t, ds, "Inbox", 3)
	all := ds.elements(StoreRef{"User", user, "Inbox"})
	if len(all) < 3 {
		t.Skip("need 3 seeded inbox elements")
	}
	op := ReadOp{Layout: model.Social, Kind: ReadSocialInbox, Entity: user, Limit: 1, Offset: 1}
	if err := verifyRead(w, newRegistry(), op, Reply{Elements: all[1:2]}); err != nil {
		t.Errorf("the page at the offset was rejected: %v", err)
	}
	// The first page returned for a read at an offset means the engine ignored the offset.
	if err := verifyRead(w, newRegistry(), op, Reply{Elements: all[0:1]}); err == nil || errors.Is(err, errShortPage) {
		t.Errorf("a page from the start for a read at an offset must fail, got %v", err)
	}
	if err := verifyRead(w, newRegistry(), op, Reply{}); !errors.Is(err, errShortPage) {
		t.Errorf("an empty page at an offset must be a short-page miss, got %v", err)
	}
	past := ReadOp{Layout: model.Social, Kind: ReadSocialInbox, Entity: user, Limit: len(all), Offset: 1}
	if err := verifyRead(w, newRegistry(), past, Reply{Elements: all[1:]}); err == nil || !strings.Contains(err.Error(), "offset") {
		t.Errorf("an offset that leaves the seeded elements must be refused as a plan error, got %v", err)
	}
	hash := ReadOp{Layout: model.Social, Kind: ReadSocialFollowers, Entity: user, Limit: 1, Offset: 1}
	if err := verifyRead(w, newRegistry(), hash, Reply{}); err == nil || !strings.Contains(err.Error(), "offset") {
		t.Errorf("an offset on a hash store must be refused, got %v", err)
	}
}
