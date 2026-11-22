package server

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Nested buckets step 9: a blobStore field's metadata lives at the bucket's
// own [s][id][0x06][blob id], where migration, the reclaimer and the orphan
// pass look for it, and only the element → blob id mapping lives under 0x07.

// newBlobFieldServer is a fault-store server with no reclaim goroutine and
// bucket app.Post declaring Photos (blob), with document p1.
func newBlobFieldServer(t *testing.T) (*Server, *kvfault.Store) {
	t.Helper()
	kv := kvfault.New()
	s := newKVFaultServer(t.TempDir(), kv)
	if resp := createWithStores(t, s, "app", "Post", BucketTypeHash, StoreDeclaration{Name: "Photos", Engine: "blobStore"}); !resp.Success {
		t.Fatalf("creating Post: %s", resp.Message)
	}
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}); err != nil || !resp.Success {
		t.Fatalf("inserting p1: %+v, %v", resp, err)
	}
	return s, kv
}

// largePhoto is file-backed content: above the inline threshold.
var largePhoto = bytes.Repeat([]byte("photo "), store.InlineThreshold/5)

func putPhoto(t *testing.T, s *Server, element string, content []byte) *FieldResponse {
	t.Helper()
	return fieldOK(t)(s.FieldPutCtx(context.Background(), FieldRequest{
		Database: "app", Bucket: "Post", Key: "p1", Field: "Photos", Element: element, Content: content,
		Data: map[string]interface{}{"filename": element + ".jpg"},
	}))
}

func getPhoto(t *testing.T, s *Server, element string) *FieldResponse {
	t.Helper()
	resp, err := s.FieldGet(FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Photos", Element: element})
	if err != nil {
		t.Fatalf("getting Photos[%s]: %v", element, err)
	}
	return resp
}

// onlyPayload is the one payload file of bucket Post.
func onlyPayload(t *testing.T, s *Server) string {
	t.Helper()
	files := payloadFilesOf(t, s.blobRoot(), lookupBucket(s, "app", "Post").id.Load())
	if len(files) != 1 {
		t.Fatalf("Post has %d payload files, want 1: %v", len(files), files)
	}
	return files[0]
}

func TestBlobFieldPayloadSurvivesOrphanPass(t *testing.T) {
	useReclaimClock(t, newFakeReclaimClock())
	s, kv := newBlobFieldServer(t)
	putPhoto(t, s, "cover", largePhoto)
	payload := onlyPayload(t, s)
	ageFile(t, payload)

	setOrphanCursor(t, kv, leafOf(t, s, payload), "")
	orphanTickOrFail(t, s)
	if !fileExists(t, payload) {
		t.Fatalf("the orphan pass unlinked a live blob field's payload %s: its metadata is not where the pass looks", payload)
	}
	if got := getPhoto(t, s, "cover"); !got.Found || !bytes.Equal(got.Content, largePhoto) {
		t.Fatalf("after the orphan pass Photos[cover] = found %v, %d bytes; want the %d-byte content", got.Found, len(got.Content), len(largePhoto))
	}
}

func TestDropReclaimsBlobFieldPayloads(t *testing.T) {
	useReclaimClock(t, newFakeReclaimClock())
	s, _ := newBlobFieldServer(t)
	putPhoto(t, s, "cover", largePhoto)
	putPhoto(t, s, "back", largePhoto)
	id := lookupBucket(s, "app", "Post").id.Load()
	if files := payloadFilesOf(t, s.blobRoot(), id); len(files) != 2 {
		t.Fatalf("precondition: Post holds %d payload files, want 2", len(files))
	}
	dropOrFail(t, s, "app", "Post")
	lapUntilDrained(t, s)
	if files := payloadFilesOf(t, s.blobRoot(), id); len(files) != 0 {
		t.Fatalf("after the drop was reclaimed %d payload files of bucket %d remain: %v", len(files), id, files)
	}
}

func TestDeleteDocumentUnlinksBlobFieldPayloads(t *testing.T) {
	s, _ := newBlobFieldServer(t)
	putPhoto(t, s, "cover", largePhoto)
	payload := onlyPayload(t, s)

	// A delete whose commit fails leaves the document, the element, its
	// metadata and its file: the file is unlinked only after a commit.
	fail := func(storage.KV, uint16, uint64, uint64) error { return errors.New("injected commit failure") }
	store.ApplyFaultHook.Store(&fail)
	resp, err := s.DeleteCtx(context.Background(), DeleteRequest{Database: "app", Bucket: "Post", Key: "p1"})
	store.ApplyFaultHook.Store(nil)
	if err != nil || resp.Success {
		t.Fatalf("a delete whose commit fails = %+v, %v; want Success false", resp, err)
	}
	if got := getPhoto(t, s, "cover"); !got.Found || !bytes.Equal(got.Content, largePhoto) {
		t.Fatalf("after a failed delete Photos[cover] is found %v with %d bytes: the payload went before the commit", got.Found, len(got.Content))
	}

	if resp, err := s.DeleteCtx(context.Background(), DeleteRequest{Database: "app", Bucket: "Post", Key: "p1"}); err != nil || !resp.Success {
		t.Fatalf("deleting p1: %+v, %v", resp, err)
	}
	if fileExists(t, payload) {
		t.Fatalf("deleting p1 left its Photos payload %s", payload)
	}
	b := lookupBucket(s, "app", "Post")
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: storage.Key(b.slot, b.databaseID, b.id.Load(), storage.NSBlobMeta), Upper: storage.Key(b.slot, b.databaseID, b.id.Load(), storage.NSBlobMeta+1)})
	defer it.Close()
	if it.Seek(nil); it.Valid() {
		t.Fatalf("deleting p1 left blob metadata %x", it.Key())
	}
	if st := bucketStateOf(t, s, "app", "Post"); st.LogicalBytes != 0 {
		t.Fatalf("after its only document was deleted Post's logical size is %d, want 0", st.LogicalBytes)
	}
}

func TestBlobFieldOverwriteAndDelete(t *testing.T) {
	s, _ := newBlobFieldServer(t)
	putPhoto(t, s, "cover", largePhoto)
	first := onlyPayload(t, s)
	putPhoto(t, s, "cover", []byte("small, inline"))
	if fileExists(t, first) {
		t.Fatalf("overwriting Photos[cover] left the first payload %s", first)
	}
	if got := getPhoto(t, s, "cover"); string(got.Content) != "small, inline" {
		t.Fatalf("Photos[cover] reads %q after the overwrite", got.Content)
	}
	if got := fieldLen(t, s, "p1", "Photos"); got != 1 {
		t.Fatalf("Photos len after an overwrite = %d, want 1", got)
	}
	putPhoto(t, s, "back", largePhoto)
	back := onlyPayload(t, s)
	deleted := fieldOK(t)(s.FieldDeleteCtx(context.Background(), FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Photos", Element: "back"}))
	if !deleted.Found || fileExists(t, back) {
		t.Fatalf("deleting Photos[back]: found %v, payload still there %v", deleted.Found, fileExists(t, back))
	}
	page, err := s.FieldList(FieldListRequest{FieldRequest: FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Photos"}})
	if err != nil || len(page.Entries) != 1 || page.Entries[0].ElementKey != "cover" || page.Entries[0].BlobID == "" {
		t.Fatalf("listing Photos = %+v, %v; want cover with its blob id", page, err)
	}
}

// TestMigrateCarriesBlobFieldPayloads is spec T-13, inverted (C-13):
// migration carries a blob field's payloads, because their metadata is where
// it streams them from.
func TestMigrateCarriesBlobFieldPayloads(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	const bucket = "posts_blob"
	pair := migrationPairFor(t, tc, bucket)
	tc.ok(pair.source, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash,
		Options: storesOption(StoreDeclaration{Name: "Photos", Engine: "blobStore"})})
	tc.ok(pair.source, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "p1", Data: map[string]interface{}{"title": "p1"}})
	contents := map[string][]byte{"large": largePhoto, "inline": []byte("inline content")}
	for element, content := range contents {
		fieldCall(t, tc, pair.source, transport.OpFieldPut, FieldRequest{Database: testDatabase, Bucket: bucket, Key: "p1", Field: "Photos", Element: element, Content: content})
	}
	source := tc.nodes[pair.source].server.core
	sourceFiles := payloadFilesOf(t, source.blobRoot(), lookupBucket(source, testDatabase, bucket).id.Load())
	if len(sourceFiles) != 1 {
		t.Fatalf("precondition: the source holds %d payload files, want 1", len(sourceFiles))
	}
	pair.pull(t, tc, bucket)
	target := tc.nodes[pair.target].server.core
	targetBucket := lookupBucket(target, testDatabase, bucket)
	targetFiles := payloadFilesOf(t, target.blobRoot(), targetBucket.id.Load())
	if len(targetFiles) != 1 {
		t.Fatalf("the target holds %d payload files for the blob field, want 1: migration did not carry it", len(targetFiles))
	}
	for digest := range payloadDigests(t, sourceFiles) {
		if !payloadDigests(t, targetFiles)[digest] {
			t.Fatalf("the source's payload (%x) has no byte-identical copy on the target", digest)
		}
	}
	if n := keysInNamespace(t, target.kv, storage.BucketRef{Slot: targetBucket.slot, DatabaseID: targetBucket.databaseID, BucketID: targetBucket.id.Load()}, storage.NSBlobMeta); n != len(contents) {
		t.Fatalf("the target holds %d blob metadata keys at bucket level, want %d", n, len(contents))
	}
	// Serving the elements needs the declaration, which migration carries
	// from step 10.
	for element, content := range contents {
		got, err := target.FieldGet(FieldRequest{Database: testDatabase, Bucket: bucket, Key: "p1", Field: "Photos", Element: element})
		if err != nil || !got.Found || !bytes.Equal(got.Content, content) {
			t.Fatalf("the migrated Photos[%s] is not served byte-identical: %+v, %v", element, got, err)
		}
	}
}

// keysInNamespace counts one bucket's keys in one namespace.
func keysInNamespace(t *testing.T, kv storage.KV, ref storage.BucketRef, ns byte) int {
	slot, databaseID, id := ref.Slot, ref.DatabaseID, ref.BucketID
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: storage.Key(slot, databaseID, id, ns), Upper: storage.Key(slot, databaseID, id, ns+1)})
	defer it.Close()
	n := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		n++
	}
	return n
}
