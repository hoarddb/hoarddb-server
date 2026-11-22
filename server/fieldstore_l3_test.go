package server

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// Nested buckets step 13 (S-13): L3 — stores declared on an L2 store's
// elements, addressed through the L2 element.

// newL3Server has Post { Comments fifoStore { Flags fifoStore },
// Albums hashStore { Pics blobStore } } and document p1.
func newL3Server(t *testing.T) *Server {
	t.Helper()
	s := newTestServer(t)
	resp := createWithStores(t, s, "app", "Post", BucketTypeHash,
		StoreDeclaration{Name: "Comments", Engine: "fifoStore", Fields: []StoreDeclaration{{Name: "Flags", Engine: "fifoStore"}}},
		StoreDeclaration{Name: "Albums", Engine: "hashStore", Fields: []StoreDeclaration{{Name: "Pics", Engine: "blobStore"}}})
	if !resp.Success {
		t.Fatalf("creating Post: %s", resp.Message)
	}
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}); err != nil || !resp.Success {
		t.Fatalf("inserting p1: %+v, %v", resp, err)
	}
	return s
}

func pushComment(t *testing.T, s *Server, n int) {
	t.Helper()
	fieldOK(t)(s.FieldPushCtx(context.Background(), FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments", Data: map[string]interface{}{"n": n}}))
}

func pushFlag(t *testing.T, s *Server, comment int64, n int) {
	t.Helper()
	fieldOK(t)(s.FieldPushCtx(context.Background(), FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Flags", Path: []interface{}{"Comments", comment}, Data: map[string]interface{}{"flag": n}}))
}

// flagAnchor is the L3 owner of comment position's stores.
func flagAnchor(t *testing.T, s *Server, position uint64) []byte {
	t.Helper()
	b := servedBucket(t, s, "app", "Post")
	owner, err := store.FieldOwnerPrefix(b.slot, b.databaseID, b.id.Load(), "p1")
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	comments, _ := b.storeFields().find("Comments", 0)
	return store.FieldElementAnchor(store.FieldStorePrefix(owner, uint16(comments.ID)), store.PositionRemainder(position))
}

func keysUnder(t *testing.T, s *Server, prefix []byte) int {
	t.Helper()
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	upper := append(append([]byte(nil), prefix[:len(prefix)-1]...), prefix[len(prefix)-1]+1)
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: upper})
	defer it.Close()
	n := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		n++
	}
	return n
}

// TestL3DeclarationRoundTrips is spec T-10.
func TestL3DeclarationRoundTrips(t *testing.T) {
	s := newL3Server(t)
	pushComment(t, s, 0)
	pushFlag(t, s, 0, 1)
	pushFlag(t, s, 0, 2)

	page, err := s.FieldList(FieldListRequest{FieldRequest: FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Flags", Path: []interface{}{"Comments", int64(0)}}})
	if err != nil || len(page.Entries) != 2 || page.Entries[1].Element.Lookup("flag").Int32() != 2 {
		t.Fatalf("listing comment 0's Flags = %+v, %v; want the two flags in order", page, err)
	}
	// The element key carries both ancestors: document, comment, flag — the
	// composition step 1's L3 vector freezes.
	b := servedBucket(t, s, "app", "Post")
	flags, _ := b.storeFields().find("Flags", mustFind(t, b, "Comments").ID)
	want := store.FieldElementKey(store.FieldStorePrefix(flagAnchor(t, s, 0), uint16(flags.ID)), storage.NSQueue, store.PositionRemainder(0))
	snap := s.kv.NewSnapshot()
	_, err = snap.Get(want)
	snap.Discard()
	if err != nil {
		t.Fatalf("the first flag is not stored at %x (document, comment 0, flag 0): %v", want, err)
	}

	for name, req := range map[string]FieldRequest{
		"no path":           {Database: "app", Bucket: "Post", Key: "p1", Field: "Flags"},
		"no L2 element":     {Database: "app", Bucket: "Post", Key: "p1", Field: "Flags", Path: []interface{}{"Comments"}},
		"no L2 field named": {Database: "app", Bucket: "Post", Key: "p1", Field: "Flags", Path: []interface{}{"", int64(0)}},
	} {
		if _, err := s.FieldPushCtx(context.Background(), req); !errors.Is(err, ErrFieldAddressIncomplete) {
			t.Errorf("an L3 push with %s = %v; want ErrFieldAddressIncomplete", name, err)
		}
	}
	missing := FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Flags", Path: []interface{}{"Comments", int64(5)}, Data: map[string]interface{}{"flag": 9}}
	if _, err := s.FieldPushCtx(context.Background(), missing); !errors.Is(err, ErrFieldParentNotFound) {
		t.Fatalf("an L3 push under a comment that does not exist = %v; want ErrFieldParentNotFound", err)
	}
	if _, err := s.FieldLen(missing); !errors.Is(err, ErrFieldParentNotFound) {
		t.Fatalf("an L3 len under a comment that does not exist = %v; want ErrFieldParentNotFound, never an empty store", err)
	}
}

func mustFind(t *testing.T, b *inMemoryBucket, name string) catalogStore {
	t.Helper()
	declared, found := b.storeFields().find(name, 0)
	if !found {
		t.Fatalf("Post declares no %s", name)
	}
	return declared
}

func TestL3KeysInvisibleToL2Scans(t *testing.T) {
	s := newL3Server(t)
	pushComment(t, s, 0)
	pushComment(t, s, 1)
	pushFlag(t, s, 0, 1)
	pushFlag(t, s, 0, 2)
	page, err := s.FieldList(FieldListRequest{FieldRequest: fieldReq("p1", "Comments")})
	if err != nil {
		t.Fatalf("listing Comments: %v: the walk of the comments read a key that is not a comment", err)
	}
	if len(page.Entries) != 2 {
		t.Fatalf("listing Comments = %d entries; want the 2 comments and no flag", len(page.Entries))
	}
	if got := fieldLen(t, s, "p1", "Comments"); got != 2 {
		t.Fatalf("Comments len = %d, want 2", got)
	}
	ctx := context.Background()
	for want := range 2 {
		popped := fieldOK(t)(s.FieldPopCtx(ctx, FieldPopRequest{FieldRequest: fieldReq("p1", "Comments")}))
		if !popped.Found || popped.Items[0].Element.Lookup("n").Int32() != int32(want) || popped.Items[0].Element.Lookup("flag").Type != 0 {
			t.Fatalf("pop %d on Comments returned %v; want comment %d, never a flag", want, popped.Items, want)
		}
	}
	if popped := fieldOK(t)(s.FieldPopCtx(ctx, FieldPopRequest{FieldRequest: fieldReq("p1", "Comments")})); popped.Found {
		t.Fatalf("a third pop found %v: the flags were read as comments", popped.Items)
	}
}

func TestPoppingL2ElementDestroysItsL3(t *testing.T) {
	s := newL3Server(t)
	pushComment(t, s, 0)
	pushComment(t, s, 1)
	pushFlag(t, s, 0, 1)
	pushFlag(t, s, 1, 2)
	pushFlag(t, s, 1, 3)
	// Two flag elements and the flag store's state under comment 1, one and
	// a state under comment 0.
	if under0, under1 := keysUnder(t, s, flagAnchor(t, s, 0)), keysUnder(t, s, flagAnchor(t, s, 1)); under0 != 2 || under1 != 3 {
		t.Fatalf("precondition: %d keys under comment 0 and %d under comment 1, want 2 and 3", under0, under1)
	}
	fieldOK(t)(s.FieldPopCtx(context.Background(), FieldPopRequest{FieldRequest: fieldReq("p1", "Comments")}))
	// Asserted on the keys: a fifo position is never reused, so no read could
	// reach a flag left behind under position 0.
	if got := keysUnder(t, s, flagAnchor(t, s, 0)); got != 0 {
		t.Fatalf("popping comment 0 left %d keys of its flags", got)
	}
	if got := keysUnder(t, s, flagAnchor(t, s, 1)); got != 3 {
		t.Fatalf("popping comment 0 left comment 1 with %d keys, want its 3", got)
	}
}

func TestDeletingL2ElementUnlinksItsL3Blobs(t *testing.T) {
	s := newL3Server(t)
	ctx := context.Background()
	fieldOK(t)(s.FieldPutCtx(ctx, FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Albums", Element: "a1", Data: map[string]interface{}{"name": "summer"}}))
	photo := bytes.Repeat([]byte("pic"), store.InlineThreshold)
	fieldOK(t)(s.FieldPutCtx(ctx, FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Pics", Path: []interface{}{"Albums", "a1"}, Element: "beach", Content: photo}))
	payload := onlyPayload(t, s)
	got, err := s.FieldGet(FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Pics", Path: []interface{}{"Albums", "a1"}, Element: "beach"})
	if err != nil || !bytes.Equal(got.Content, photo) {
		t.Fatalf("reading the L3 blob: %v", err)
	}
	fieldOK(t)(s.FieldDeleteCtx(ctx, FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Albums", Element: "a1"}))
	if fileExists(t, payload) {
		t.Fatalf("deleting album a1 left its Pics payload %s", payload)
	}
	b := servedBucket(t, s, "app", "Post")
	if n := keysInNamespace(t, s.kv, storage.BucketRef{Slot: b.slot, DatabaseID: b.databaseID, BucketID: b.id.Load()}, storage.NSBlobMeta); n != 0 {
		t.Fatalf("deleting album a1 left %d blob metadata keys", n)
	}
}
