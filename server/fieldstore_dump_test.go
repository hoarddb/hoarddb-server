package server

import (
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestOldClientRefusedForBucketWithStores: a dump that silently omitted every
// element would restore every post and none of its comments, under a
// matching digest (F-12). A client that does not write store fields is
// refused for a bucket that declares them, and served as before for every
// other bucket.
func TestOldClientRefusedForBucketWithStores(t *testing.T) {
	s := newPostServer(t)
	createTestBucket(t, s, "app", "Plain", BucketTypeHash)

	_, err := s.startDump(rootContext(), DumpStartRequest{Database: "app", Bucket: "Post"})
	if !errors.Is(err, ErrDumpNeedsNewerClient) || codeFor(err) != transport.ErrInvalidArgument {
		t.Fatalf("an old client's dump of Post = %v; want ErrDumpNeedsNewerClient", err)
	}
	for _, part := range []string{"app/Post", "Comments (fifo)", "Upgrade hoarddb"} {
		if !strings.Contains(err.Error(), part) {
			t.Fatalf("the refusal %q does not say %q", err, part)
		}
	}

	resp, err := s.startDump(rootContext(), DumpStartRequest{Database: "app", Bucket: "Plain"})
	// The layout is what this asserts — a bucket with no store fields dumps
	// with no stores section — and the version is the one every manifest has
	// (owner ruling 2026-09-30; it was 1 for such a bucket before).
	if err != nil || resp.Manifest.FormatVersion != 3 || len(resp.Manifest.Stores) != 0 {
		t.Fatalf("an old client's dump of Plain = %+v, %v; want format_version 3 and no stores (the refused dump must not hold the slot)", resp, err)
	}
	if _, err := s.dumpDone(rootContext(), DumpDoneRequest{Database: "app", Bucket: "Plain"}); err != nil {
		t.Fatalf("releasing Plain's dump: %v", err)
	}

	resp, err = s.startDump(rootContext(), DumpStartRequest{Database: "app", Bucket: "Post", AcceptStores: true})
	if err != nil || resp.Manifest.FormatVersion != 3 || len(resp.Manifest.Stores) != 3 {
		t.Fatalf("a new client's dump of Post = %+v, %v; want format_version 3 naming 3 stores", resp, err)
	}
	if _, err := s.dumpDone(rootContext(), DumpDoneRequest{Database: "app", Bucket: "Post"}); err != nil {
		t.Fatalf("releasing Post's dump: %v", err)
	}
}

// TestRestoreNeverCarriesStoreIds: a restore declares by name and routes
// every element by its field's name. Restored into a bucket whose ids differ
// from the source's, each element still lands under its own field.
func TestRestoreNeverCarriesStoreIds(t *testing.T) {
	s := newTestServer(t)
	// The target already declares Tags (id 1) and Ranked (id 2); the dump's
	// Comments had id 1 on its source and gets id 3 here.
	if resp := createWithStores(t, s, "app", "Post", BucketTypeHash,
		StoreDeclaration{Name: "Tags", Engine: "hashStore"}, StoreDeclaration{Name: "Ranked", Engine: "heapStore"}); !resp.Success {
		t.Fatalf("creating the target: %s", resp.Message)
	}
	started, err := s.startRestore(rootContext(), RestoreStartRequest{Database: "app", Bucket: "Post", FormatVersion: 2, StoreType: BucketTypeHash,
		Stores: []DumpStore{{Name: "Comments", Engine: "fifo"}}})
	if err != nil {
		t.Fatalf("starting the restore: %v", err)
	}
	document, _ := bson.Marshal(map[string]interface{}{"_id_": "p1", "title": "p1"})
	value, _ := bson.Marshal(map[string]interface{}{"n": 7})
	element, _ := bson.Marshal(dumpElementRecord{Field: "Comments", Owner: "p1", Key: int64(0), Value: value})
	for _, chunk := range []RestoreChunkRequest{
		{Database: "app", Bucket: "Post", SessionID: started.SessionID, Data: document},
		{Database: "app", Bucket: "Post", SessionID: started.SessionID, Data: element, Section: dumpSectionStores},
	} {
		if _, err := s.restoreChunk(rootContext(), chunk); err != nil {
			t.Fatalf("restoring a %q chunk: %v", chunk.Section, err)
		}
	}
	done, err := s.restoreDone(rootContext(), RestoreDoneRequest{Database: "app", Bucket: "Post", SessionID: started.SessionID, Documents: 1, Elements: 1})
	if err != nil || !done.Success || done.ElementsApplied != 1 {
		t.Fatalf("finishing the restore: %+v, %v", done, err)
	}
	comments, found := lookupBucket(s, "app", "Post").storeFields().find("Comments", 0)
	if !found || comments.ID != 3 {
		t.Fatalf("the restored Comments has id %d (found %v); want 3, the target's own", comments.ID, found)
	}
	if got := fieldLen(t, s, "p1", "Comments"); got != 1 {
		t.Fatalf("p1's Comments holds %d elements after restore, want the one restored", got)
	}
	for _, other := range []string{"Tags", "Ranked"} {
		if got := fieldLen(t, s, "p1", other); got != 0 {
			t.Fatalf("p1's %s holds %d elements: the element landed under another field", other, got)
		}
	}
}
