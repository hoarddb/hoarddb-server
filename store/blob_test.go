package store

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ids"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// A slot and id with no zero nibble to hide behind: a name that dropped or
// misordered either would not parse back to these.
const (
	testBlobSlot uint16 = 0x2a7
	testBlobID   uint64 = 0x0123456789abcdef
)

func newTestBlobStore(t *testing.T) (*BlobStore, *kvfault.Store, string) {
	t.Helper()
	kv := kvfault.New()
	root := t.TempDir()
	return NewBlobStore(kv, storage.BucketRef{Slot: testBlobSlot, DatabaseID: 1, BucketID: testBlobID}, new(OwedApplies), BlobPayloads{Root: root}), kv, root
}

// payloadFiles lists every regular file under root, relative to it.
func payloadFiles(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			relative, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return relErr
			}
			files = append(files, relative)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	return files
}

func blobContent(size int, fill byte) []byte {
	return bytes.Repeat([]byte{fill}, size)
}

// TestInlineThreshold is T-08's first half and spec §11 Q5: content below 64
// KiB lives in the metadata value and costs no file; content at or above it is
// one file two fan-out levels down, whose name parses back to the bucket's
// recorded slot and id (Part A, spec §3.5).
func TestInlineThreshold(t *testing.T) {
	bs, _, root := newTestBlobStore(t)

	small := blobContent(InlineThreshold-1, 's')
	if _, err := bs.Put(ApplyInfo{}, "small", small, nil); err != nil {
		t.Fatalf("put 64 KiB - 1: %v", err)
	}
	if files := payloadFiles(t, root); len(files) != 0 {
		t.Fatalf("64 KiB - 1 bytes wrote payload files %v; below the threshold the content is inline", files)
	}

	large := blobContent(InlineThreshold+1, 'l')
	largeID, err := bs.Put(ApplyInfo{}, "large", large, nil)
	if err != nil {
		t.Fatalf("put 64 KiB + 1: %v", err)
	}
	files := payloadFiles(t, root)
	if len(files) != 1 {
		t.Fatalf("64 KiB + 1 bytes left payload files %v, want exactly one", files)
	}
	parts := strings.Split(filepath.ToSlash(files[0]), "/")
	if len(parts) != 3 || len(parts[0]) != 2 || len(parts[1]) != 2 {
		t.Fatalf("payload file %s is not under blobs/xx/yy/", files[0])
	}
	name, ok := ParsePayloadFileName(parts[2])
	if !ok {
		t.Fatalf("payload file name %q does not parse", parts[2])
	}
	if name.Slot != testBlobSlot || name.BucketID != testBlobID || name.BlobID != largeID || name.Temporary {
		t.Fatalf("payload file name %q parses to %+v, want slot %#x, bucket %#x, blob %s, final", parts[2], name, testBlobSlot, testBlobID, largeID)
	}

	for key, want := range map[string][]byte{"small": small, "large": large} {
		stored, err := bs.Get(key)
		if err != nil || stored == nil {
			t.Fatalf("get %s: %+v, %v", key, stored, err)
		}
		if !bytes.Equal(stored.Content, want) {
			t.Fatalf("get %s returned %d bytes, want the %d written", key, len(stored.Content), len(want))
		}
	}
}

// TestBlobFanOutIsUniform is T-08's rev 4 half (N-14): a million ids minted as
// fast as ids.NewDocumentID allows land no more than 4x the mean in any of the
// 65,536 leaves. A UUIDv7's leading hex digits are a 49.7-day time bucket, so a
// fan-out on the id's own prefix puts all of them in one leaf.
func TestBlobFanOutIsUniform(t *testing.T) {
	if testing.Short() {
		t.Skip("a million ids: long, run in §7")
	}
	const blobs = 1_000_000
	const leaves = 1 << 16
	perLeaf := make(map[string]int, leaves)
	for i := 0; i < blobs; i++ {
		blobID, err := ids.NewDocumentID()
		if err != nil {
			t.Fatalf("minting id %d: %v", i, err)
		}
		perLeaf[filepath.Dir(PayloadRelPath(testBlobSlot, testBlobID, blobID))]++
	}
	mean := float64(blobs) / leaves
	fullest, fullestLeaf := 0, ""
	for leaf, n := range perLeaf {
		if n > fullest {
			fullest, fullestLeaf = n, leaf
		}
	}
	if float64(fullest) > 4*mean {
		t.Fatalf("leaf %s holds %d of %d blobs, more than 4x the mean %.1f over %d leaves (%d leaves used)",
			fullestLeaf, fullest, blobs, mean, leaves, len(perLeaf))
	}
}

// TestDeleteNeverLeavesMetadataNamingAMissingFile is T-07's second clause and
// spec §3.5's mirror rule: the metadata goes first and the file second, so a
// delete stopped between the two leaves an orphan file and never metadata that
// names a missing one. The power-loss arm is why the metadata delete is synced
// before the unlink: an unsynced delete can be lost while the unlink, which
// the filesystem journals on its own, survives.
func TestDeleteNeverLeavesMetadataNamingAMissingFile(t *testing.T) {
	t.Run("stopped between the two steps", func(t *testing.T) {
		bs, _, _ := newTestBlobStore(t)
		if _, err := bs.Put(ApplyInfo{}, "k", blobContent(InlineThreshold*2, 'x'), nil); err != nil {
			t.Fatalf("put: %v", err)
		}
		crash := errors.New("simulated crash between the delete's two steps")
		hook := func(string) error { return crash }
		blobBetweenDeleteStepsHook.Store(&hook)
		_, err := bs.Delete(ApplyInfo{}, "k")
		blobBetweenDeleteStepsHook.Store(nil)
		if !errors.Is(err, crash) {
			t.Fatalf("delete returned %v, want the simulated crash", err)
		}
		stored, err := bs.Get("k")
		if err != nil {
			t.Fatalf("after a delete stopped between its steps, get failed: %v — metadata names a file that is gone", err)
		}
		if stored != nil {
			t.Fatalf("after a delete stopped between its steps, get still found blob %s", stored.BlobID)
		}
	})
	t.Run("power loss after the unlink", func(t *testing.T) {
		bs, kv, _ := newTestBlobStore(t)
		if _, err := bs.Put(ApplyInfo{}, "k", blobContent(InlineThreshold*2, 'x'), nil); err != nil {
			t.Fatalf("put: %v", err)
		}
		if err := kv.Sync(); err != nil {
			t.Fatalf("syncing the put: %v", err)
		}
		if deleted, err := bs.Delete(ApplyInfo{}, "k"); err != nil || !deleted {
			t.Fatalf("delete: %v, %v", deleted, err)
		}
		kv.PowerLoss()
		stored, err := bs.Get("k")
		if err != nil {
			t.Fatalf("after a power loss following the delete, get failed: %v — the unlink survived and the metadata delete did not", err)
		}
		if stored != nil {
			t.Fatalf("after a power loss following the delete, blob %s is back", stored.BlobID)
		}
	})
}

// TestBlobPutIsFsyncedAndRenamed is spec §3.5's put order: the content goes to
// a temporary name — the final name plus .tmp (SA-25) — and is renamed into
// place only once it is whole, so a put stopped before the rename leaves no
// file under the final name and no metadata.
func TestBlobPutIsFsyncedAndRenamed(t *testing.T) {
	bs, _, _ := newTestBlobStore(t)
	content := blobContent(InlineThreshold*3, 'r')
	crash := errors.New("simulated crash before the rename")
	var tempPath string
	var tempSize int64 = -1
	hook := func(path string) error {
		tempPath = path
		if info, err := os.Stat(path); err == nil {
			tempSize = info.Size()
		}
		return crash
	}
	blobBeforeRenameHook.Store(&hook)
	_, err := bs.Put(ApplyInfo{}, "k", content, nil)
	blobBeforeRenameHook.Store(nil)
	if !errors.Is(err, crash) {
		t.Fatalf("put returned %v, want the simulated crash", err)
	}
	if tempPath == "" {
		t.Fatal("the put never reached the point before its rename")
	}
	if tempSize != int64(len(content)) {
		t.Fatalf("before the rename the temporary held %d bytes, want the whole %d", tempSize, len(content))
	}
	name, ok := ParsePayloadFileName(filepath.Base(tempPath))
	if !ok || !name.Temporary || name.Slot != testBlobSlot || name.BucketID != testBlobID {
		t.Fatalf("the temporary %q parses to %+v, %v; want a temporary of this bucket's final name", filepath.Base(tempPath), name, ok)
	}
	finalPath := strings.TrimSuffix(tempPath, temporarySuffix)
	if _, err := os.Stat(finalPath); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a put stopped before its rename left %s (stat: %v); the final name must not exist", finalPath, err)
	}
	if stored, err := bs.Get("k"); err != nil || stored != nil {
		t.Fatalf("a put stopped before its rename is readable: %+v, %v", stored, err)
	}
}

// TestBlobOverwriteReplacesThePayload: a put over an existing key replaces the
// blob, its metadata and its file. The per-bucket store it replaces kept the
// old blob and its file forever.
func TestBlobOverwriteReplacesThePayload(t *testing.T) {
	bs, _, root := newTestBlobStore(t)
	for _, fill := range []byte{'a', 'b'} {
		if _, err := bs.Put(ApplyInfo{}, "k", blobContent(InlineThreshold+10, fill), nil); err != nil {
			t.Fatalf("put %c: %v", fill, err)
		}
	}
	if files := payloadFiles(t, root); len(files) != 1 {
		t.Fatalf("after overwriting one key the tree holds %v, want one file", files)
	}
	if n, err := bs.Len(); err != nil || n != 1 {
		t.Fatalf("Len = %d, %v; want 1", n, err)
	}
	if n, err := bs.DocCount(); err != nil || n != 1 {
		t.Fatalf("DocCount = %d, %v; want 1", n, err)
	}
	stored, err := bs.Get("k")
	if err != nil || stored == nil || stored.Content[0] != 'b' {
		t.Fatalf("get after the overwrite: %+v, %v; want the second content", stored, err)
	}
}

// TestParsePayloadFileName pins the one format the writer and Step 17's orphan
// pass share: it round-trips, it knows a temporary, and it refuses anything
// else, because the pass skips — never unlinks — a name it cannot identify.
func TestParsePayloadFileName(t *testing.T) {
	blobID, err := ids.NewDocumentID()
	if err != nil {
		t.Fatalf("minting an id: %v", err)
	}
	final := PayloadFileName(0x00f, 42, blobID)
	if want := "00f-000000000000002a-" + blobID; final != want {
		t.Fatalf("PayloadFileName = %q, want %q", final, want)
	}
	for _, tc := range []struct {
		name      string
		temporary bool
	}{{final, false}, {final + temporarySuffix, true}} {
		parsed, ok := ParsePayloadFileName(tc.name)
		if !ok || parsed != (PayloadName{Slot: 0x00f, BucketID: 42, BlobID: blobID, Temporary: tc.temporary}) {
			t.Fatalf("ParsePayloadFileName(%q) = %+v, %v", tc.name, parsed, ok)
		}
	}
	for _, foreign := range []string{
		"README",
		"_index.bson",
		blobID,
		"00F-000000000000002a-" + blobID,
		"00f-000000000000002A-" + blobID,
		"0f-000000000000002a-" + blobID,
		"00f-00000000000002a-" + blobID,
		"00f-000000000000002a-" + strings.ToUpper(blobID),
		"00f-000000000000002a-not-a-uuid",
		"00f-000000000000002a-" + blobID + ".bak",
		"00f_000000000000002a_" + blobID,
	} {
		if parsed, ok := ParsePayloadFileName(foreign); ok {
			t.Errorf("ParsePayloadFileName(%q) accepted a foreign name as %+v", foreign, parsed)
		}
	}
}

// TestBlobIterateDocumentsAgreesWithDocCount is ported from
// storage/enumerate_test.go: the walk and the count agree, and each emitted
// document carries what the put recorded.
func TestBlobIterateDocumentsAgreesWithDocCount(t *testing.T) {
	bs, _, _ := newTestBlobStore(t)
	type wantMeta struct {
		filename string
		size     int64
	}
	want := map[string]wantMeta{}
	for i := 0; i < 5; i++ {
		size := 10 + i
		if i%2 == 1 {
			size = InlineThreshold + i
		}
		filename := fmt.Sprintf("f%d.bin", i)
		blobID, err := bs.Put(ApplyInfo{}, fmt.Sprintf("k%d", i), blobContent(size, byte('0'+i)), map[string]interface{}{"filename": filename})
		if err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		want[blobID] = wantMeta{filename: filename, size: int64(size)}
	}

	count, err := bs.DocCount()
	if err != nil || count != 5 {
		t.Fatalf("DocCount = %d, %v; want 5", count, err)
	}
	type dumpedBlob struct {
		ID       string `bson:"_id_"`
		Filename string `bson:"filename"`
		Size     int64  `bson:"size"`
	}
	seen := map[string]dumpedBlob{}
	if err := bs.IterateDocuments(func(key, value []byte) error {
		var doc dumpedBlob
		if err := bson.Unmarshal(value, &doc); err != nil {
			return fmt.Errorf("decoding dumped blob metadata: %w", err)
		}
		if string(key) != doc.ID {
			t.Fatalf("key %q != emitted document _id_ %q", key, doc.ID)
		}
		seen[doc.ID] = doc
		return nil
	}); err != nil {
		t.Fatalf("IterateDocuments: %v", err)
	}
	if int64(len(seen)) != count {
		t.Fatalf("IterateDocuments emitted %d entries, DocCount says %d", len(seen), count)
	}
	for blobID, w := range want {
		got, ok := seen[blobID]
		if !ok {
			t.Fatalf("IterateDocuments never emitted blob %q", blobID)
		}
		if got.Filename != w.filename || got.Size != w.size {
			t.Fatalf("blob %q emitted %+v, want filename %q size %d", blobID, got, w.filename, w.size)
		}
	}
}

// TestBlobIterateDocumentsNeverTouchesPayload is ported from
// storage/enumerate_test.go: a dump emits metadata only, so it succeeds with
// the payload file gone from under it.
func TestBlobIterateDocumentsNeverTouchesPayload(t *testing.T) {
	bs, _, root := newTestBlobStore(t)
	if _, err := bs.Put(ApplyInfo{}, "k", blobContent(InlineThreshold*2, 'p'), map[string]interface{}{"filename": "f.bin"}); err != nil {
		t.Fatalf("put: %v", err)
	}
	for _, file := range payloadFiles(t, root) {
		if err := os.Remove(filepath.Join(root, file)); err != nil {
			t.Fatalf("removing the payload file: %v", err)
		}
	}
	emitted := 0
	if err := bs.IterateDocuments(func(_, _ []byte) error {
		emitted++
		return nil
	}); err != nil {
		t.Fatalf("IterateDocuments touched the payload path: %v", err)
	}
	if emitted != 1 {
		t.Fatalf("IterateDocuments emitted %d entries, want 1", emitted)
	}
}

// keysUnder lists the store keys in a range, so a test can assert that
// nothing names a blob without reading through the BlobStore's own accessors.
func keysUnder(t *testing.T, kv *kvfault.Store, bounds storage.IterOptions) []string {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	iter := snap.NewIterator(bounds)
	defer iter.Close()
	var keys []string
	for iter.Seek(bounds.Lower); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("iterating %x..%x: %v", bounds.Lower, bounds.Upper, err)
	}
	return keys
}

// TestFailedBlobPutLeavesNoFileAndNoKeyNamingTheBlob is BUG-0034's acceptance
// test. A put whose batch is refused after its payload file is already on disk
// must leave the disk and the store agreeing: the file is gone, and neither a
// metadata key nor a key-to-blob mapping names the blob. A replaced blob of the
// same key stays whole, because the failed put commits nothing.
func TestFailedBlobPutLeavesNoFileAndNoKeyNamingTheBlob(t *testing.T) {
	// Below the size of one mapping entry, so the batch refuses the first write
	// the put makes, after prepareBlob has written the payload.
	const refusingBatchBytes = 40
	content := blobContent(InlineThreshold*2, 'n')

	t.Run("a new key", func(t *testing.T) {
		bs, kv, root := newTestBlobStore(t)
		kv.SetMaxBatchBytes(refusingBatchBytes)
		_, err := bs.Put(ApplyInfo{}, "k", content, nil)
		if !errors.Is(err, storage.ErrBatchTooLarge) {
			t.Fatalf("put under a refusing batch returned %v, want ErrBatchTooLarge", err)
		}
		if files := payloadFiles(t, root); len(files) != 0 {
			t.Errorf("the failed put left payload files %v, want none", files)
		}
		if keys := keysUnder(t, kv, bs.metaRange()); len(keys) != 0 {
			t.Errorf("the failed put left metadata keys %q, want none", keys)
		}
		mapRange := storage.IterOptions{
			Lower: storage.Key(bs.slot, 1, bs.id, storage.NSDocuments),
			Upper: storage.Key(bs.slot, 1, bs.id, storage.NSDocuments+1),
		}
		if keys := keysUnder(t, kv, mapRange); len(keys) != 0 {
			t.Errorf("the failed put left mapping keys %q, want none", keys)
		}
		stored, err := bs.Get("k")
		if err != nil || stored != nil {
			t.Errorf("get after the failed put = %v, %v, want no blob and no error", stored, err)
		}
		if count, err := bs.DocCount(); err != nil || count != 0 {
			t.Errorf("DocCount after the failed put = %d, %v, want 0", count, err)
		}
	})

	t.Run("an overwrite", func(t *testing.T) {
		bs, kv, root := newTestBlobStore(t)
		firstID, err := bs.Put(ApplyInfo{}, "k", blobContent(InlineThreshold*2, 'o'), nil)
		if err != nil {
			t.Fatalf("first put: %v", err)
		}
		kv.SetMaxBatchBytes(refusingBatchBytes)
		if _, err := bs.Put(ApplyInfo{}, "k", content, nil); !errors.Is(err, storage.ErrBatchTooLarge) {
			t.Fatalf("overwrite under a refusing batch returned %v, want ErrBatchTooLarge", err)
		}
		if files := payloadFiles(t, root); len(files) != 1 {
			t.Errorf("after the failed overwrite the payload files are %v, want only the first generation's", files)
		}
		stored, err := bs.Get("k")
		if err != nil || stored == nil || stored.BlobID != firstID {
			t.Fatalf("get after the failed overwrite = %v, %v, want the first generation %s", stored, err, firstID)
		}
	})
}
