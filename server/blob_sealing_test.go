package server

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/sealed"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/store"
)

// D2b: a node whose store is sealed seals blob payload files too (at-rest
// encryption spec §4.2.1). These tests run on a fault-store server whose
// config says mode=on, whose node master is a fixed key, and whose blob key
// source is the one LoadOrCreateBlobKeySource hands NewService — so every path
// below the wiring is the production one — and, for the wiring itself, on
// nodes started through NewService.

// sealedBlobMarker is content a sealed node must never write to disk.
var sealedBlobMarker = []byte("D2B-SEALED-MARKER-7f3a9c")

// sealedTestContent is file-backed content (above store.InlineThreshold,
// several chunks) that ends in the marker.
func sealedTestContent() []byte {
	return append(bytes.Repeat([]byte{0x5A}, store.InlineThreshold+sealed.BlobChunkSize+321), sealedBlobMarker...)
}

// newSealedKVFaultServer is a fault-store server at mode=on whose node master
// is derived from seed, with no reclaim goroutine, holding the blob key source
// a started node would hold.
func newSealedKVFaultServer(t *testing.T, seed byte) (*Server, *kvfault.Store) {
	t.Helper()
	kv := kvfault.New()
	s := newKVFaultServer(t.TempDir(), kv)
	s.config.Encryption.Mode, s.config.Encryption.Keystore = "on", "keyfile"
	masterBytes := make([]byte, 32)
	for i := range masterBytes {
		masterBytes[i] = seed + byte(i)
	}
	s.nodeMaster = &NodeMaster{bytes: masterBytes, kid: sealed.Kid(masterBytes)}
	keys, err := LoadOrCreateBlobKeySource(s.config, s.nodeMaster, kv, discardLogger())
	if err != nil {
		t.Fatalf("loading the blob key source: %v", err)
	}
	s.blobKeys = keys
	return s, kv
}

func writeBlobOrFail(t *testing.T, s *Server, database, bucket, key string, content []byte) {
	t.Helper()
	resp, err := s.BlobWrite(BlobWriteRequest{Database: database, Bucket: bucket, Key: key, Data: content})
	if err != nil || !resp.Success {
		t.Fatalf("blob write %s/%s: %+v, %v", bucket, key, resp, err)
	}
}

func readBlobOrFail(t *testing.T, s *Server, database, bucket, key string) []byte {
	t.Helper()
	resp, err := s.BlobRead(BlobReadRequest{Database: database, Bucket: bucket, Key: key})
	if err != nil || !resp.Found {
		t.Fatalf("blob read %s/%s: %+v, %v", bucket, key, resp, err)
	}
	return resp.Data
}

// assertPayloadIsSealed fails unless the file begins with a purpose-0x06
// header and holds no byte of the marker.
func assertPayloadIsSealed(t *testing.T, path string) {
	t.Helper()
	onDisk, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	header, err := sealed.ParseFileHeader(onDisk)
	if err != nil || header.Purpose != sealed.PurposeBlobChunk {
		t.Fatalf("%s does not begin with a purpose-0x06 sealed header (%v)", path, err)
	}
	if bytes.Contains(onDisk, sealedBlobMarker) {
		t.Fatalf("%s holds the marker in the clear", path)
	}
}

// TestSealedBlobRoundTripsThroughABucketAndAFieldStore: a payload above the
// inline threshold is a sealed file whether it was put into a blob bucket or
// into a blobStore field, and both read back whole.
func TestSealedBlobRoundTripsThroughABucketAndAFieldStore(t *testing.T) {
	s, _ := newSealedKVFaultServer(t, 0x10)
	content := sealedTestContent()
	createBlobBucket(t, s, "uploads")
	writeBlobOrFail(t, s, "app", "uploads", "report", content)
	if got := readBlobOrFail(t, s, "app", "uploads", "report"); !bytes.Equal(got, content) {
		t.Fatalf("the blob bucket returned %d bytes that differ from the %d written", len(got), len(content))
	}
	bucketFiles := payloadFilesOf(t, s.blobRoot(), lookupBucket(s, "app", "uploads").id.Load())
	if len(bucketFiles) != 1 {
		t.Fatalf("the blob bucket has payload files %v, want one", bucketFiles)
	}
	assertPayloadIsSealed(t, bucketFiles[0])

	if resp := createWithStores(t, s, "app", "Post", BucketTypeHash, StoreDeclaration{Name: "Photos", Engine: "blobStore"}); !resp.Success {
		t.Fatalf("a sealed node refused a blobStore field: %s", resp.Message)
	}
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}); err != nil || !resp.Success {
		t.Fatalf("inserting p1: %+v, %v", resp, err)
	}
	if put := putPhoto(t, s, "cover", content); !put.Success {
		t.Fatalf("putting Photos[cover]: %+v", put)
	}
	if got := getPhoto(t, s, "cover"); !got.Found || !bytes.Equal(got.Content, content) {
		t.Fatalf("Photos[cover] = found %v, %d bytes; want the %d written", got.Found, len(got.Content), len(content))
	}
	assertPayloadIsSealed(t, onlyPayload(t, s))
}

// TestTamperedBlobChunkIsCorruptionNamingTheBlob: one changed byte in a sealed
// payload is outcome 2 of spec §4.2.1 h, and the error a client reads names
// the database, the bucket and the user key, says nothing was returned, and
// gives the fix.
func TestTamperedBlobChunkIsCorruptionNamingTheBlob(t *testing.T) {
	s, _ := newSealedKVFaultServer(t, 0x10)
	createBlobBucket(t, s, "uploads")
	writeBlobOrFail(t, s, "app", "uploads", "report", sealedTestContent())
	payload := payloadFilesOf(t, s.blobRoot(), lookupBucket(s, "app", "uploads").id.Load())[0]
	onDisk, err := os.ReadFile(payload)
	if err != nil {
		t.Fatalf("reading the payload: %v", err)
	}
	onDisk[sealed.FileHeaderLen+sealed.FileNonceLen+9] ^= 0x01
	if err := os.WriteFile(payload, onDisk, 0o600); err != nil {
		t.Fatalf("tampering with the payload: %v", err)
	}

	resp, err := s.BlobRead(BlobReadRequest{Database: "app", Bucket: "uploads", Key: "report"})
	if err == nil || (resp != nil && resp.Found) {
		t.Fatalf("a tampered blob was served: %+v, %v", resp, err)
	}
	var corrupt *sealed.BlobCorruptError
	if !errors.Is(err, sealed.ErrBlobCorrupt) || !errors.As(err, &corrupt) {
		t.Fatalf("want a *sealed.BlobCorruptError, got: %v", err)
	}
	for _, want := range []string{"app", "uploads", "report", "Nothing was returned", "Restore this blob from a replica or a backup"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not say %q: %v", want, err)
		}
	}
}

// TestSealedPayloadIsSweptByTheOrphanPassAndUnlinkedByDropBucket: the orphan
// pass and the drop reclaimer work on names, so a sealed payload is swept and
// unlinked exactly as a plaintext one, and the pass leaves a live one alone.
func TestSealedPayloadIsSweptByTheOrphanPassAndUnlinkedByDropBucket(t *testing.T) {
	useReclaimClock(t, newFakeReclaimClock())
	s, kv := newSealedKVFaultServer(t, 0x10)
	createBlobBucket(t, s, "uploads")
	writeBlobOrFail(t, s, "app", "uploads", "live", sealedTestContent())
	bucket := lookupBucket(s, "app", "uploads")
	livePayload := payloadFilesOf(t, s.blobRoot(), bucket.id.Load())[0]
	assertPayloadIsSealed(t, livePayload)

	orphanID := store.PayloadFileName(bucket.slot, bucket.id.Load(), "01a10195-469b-70c8-9259-ab06f3d74b94")
	parsed, ok := store.ParsePayloadFileName(orphanID)
	if !ok {
		t.Fatalf("the orphan's name %q does not parse", orphanID)
	}
	orphanRel, err := store.WritePayload(store.PayloadWrite{
		Root: s.blobRoot(), Slot: parsed.Slot, BucketID: parsed.BucketID, BlobID: parsed.BlobID, Keys: s.blobKeys, Content: sealedTestContent(),
	})
	if err != nil {
		t.Fatalf("writing the orphan: %v", err)
	}
	orphanPath := filepath.Join(s.blobRoot(), orphanRel)
	assertPayloadIsSealed(t, orphanPath)
	ageFile(t, orphanPath)
	ageFile(t, livePayload)

	setOrphanCursor(t, kv, leafOf(t, s, orphanPath), "")
	orphanTickOrFail(t, s)
	if fileExists(t, orphanPath) {
		t.Error("the orphan pass left a sealed orphan payload on disk")
	}
	setOrphanCursor(t, kv, leafOf(t, s, livePayload), "")
	orphanTickOrFail(t, s)
	if !fileExists(t, livePayload) {
		t.Fatal("the orphan pass unlinked a live sealed payload")
	}
	if got := readBlobOrFail(t, s, "app", "uploads", "live"); !bytes.Equal(got, sealedTestContent()) {
		t.Fatal("the live sealed blob no longer reads back after the orphan pass")
	}

	if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "uploads"}); err != nil || !resp.Success {
		t.Fatalf("drop bucket: %+v, %v", resp, err)
	}
	lapUntilDrained(t, s)
	if left := payloadFilesOf(t, s.blobRoot(), bucket.id.Load()); len(left) != 0 {
		t.Fatalf("dropping the bucket left sealed payload files %v", left)
	}
}

// TestSealedBlobMigratesBetweenSealedNodesWithDifferentMasters: the source
// opens the payload with its own epoch key before it ships, so the stream does
// not carry ciphertext the target cannot read; the target seals what it
// receives under its own registry and the payload's name on the target. No key
// is shared between the nodes.
func TestSealedBlobMigratesBetweenSealedNodesWithDifferentMasters(t *testing.T) {
	useReclaimClock(t, newFakeReclaimClock())
	source, _ := newSealedKVFaultServer(t, 0x10)
	target, _ := newSealedKVFaultServer(t, 0x70)
	content := sealedTestContent()
	createBlobBucket(t, source, "moved")
	writeBlobOrFail(t, source, "app", "moved", "big", content)
	writeBlobOrFail(t, source, "app", "moved", "small", []byte("inline"))
	moved := lookupBucket(source, "app", "moved")
	var stream bytes.Buffer
	if err := source.streamBucketRange(&stream, source.kv, moved.slot, moved.databaseID, moved.id.Load()); err != nil {
		t.Fatalf("streaming the sealed bucket: %v", err)
	}

	if resp, err := target.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("create database on the target: %+v, %v", resp, err)
	}
	live, err := target.liveGenerations("app")
	if err != nil || len(live) != 1 {
		t.Fatalf("live generations on the target: %v, %v", live, err)
	}
	if err := target.materializeRange(migratedBucket{database: "app", name: "moved", storeType: BucketTypeBlob, generation: live[0]}, stream.Bytes(), 0); err != nil {
		t.Fatalf("materializing the sealed bucket on a sealed target with another master: %v", err)
	}
	if got := readBlobOrFail(t, target, "app", "moved", "big"); !bytes.Equal(got, content) {
		t.Fatalf("the target returned %d bytes that differ from the %d the source held", len(got), len(content))
	}
	if got := readBlobOrFail(t, target, "app", "moved", "small"); string(got) != "inline" {
		t.Fatalf("the target returned %q for the inline blob", got)
	}
	targetPayloads := payloadFilesOf(t, target.blobRoot(), lookupBucket(target, "app", "moved").id.Load())
	if len(targetPayloads) != 1 {
		t.Fatalf("the target holds payload files %v, want one", targetPayloads)
	}
	assertPayloadIsSealed(t, targetPayloads[0])

	// The target's file is sealed under the target's key: the source's key
	// source does not open it.
	sourceKeyed := store.PayloadRead{
		Root: target.blobRoot(), Path: relativeToRoot(t, target, targetPayloads[0]), Slot: lookupBucket(target, "app", "moved").slot,
		BucketID: lookupBucket(target, "app", "moved").id.Load(), BlobID: blobIDOfPayload(t, targetPayloads[0]),
		Size: int64(len(content)), Keys: source.blobKeys,
	}
	if _, err := store.ReadPayload(sourceKeyed); !errors.Is(err, sealed.ErrBlobCorrupt) {
		t.Errorf("the source's keys opened the target's payload (or failed another way): %v", err)
	}
}

func relativeToRoot(t *testing.T, s *Server, path string) string {
	t.Helper()
	rel, err := filepath.Rel(s.blobRoot(), path)
	if err != nil {
		t.Fatalf("locating %s under the blob root: %v", path, err)
	}
	return rel
}

func blobIDOfPayload(t *testing.T, path string) string {
	t.Helper()
	parsed, ok := store.ParsePayloadFileName(filepath.Base(path))
	if !ok {
		t.Fatalf("%s is not a payload file name", path)
	}
	return parsed.BlobID
}

// rewriteStreamPayloads returns a migration stream with every payload record's
// data replaced by change(data).
func rewriteStreamPayloads(t *testing.T, stream []byte, change func([]byte) []byte) []byte {
	t.Helper()
	records, err := readMigrateStream(stream, migratedLayout{storeType: BucketTypeBlob})
	if err != nil {
		t.Fatalf("decoding the stream: %v", err)
	}
	var rewritten bytes.Buffer
	for _, record := range records {
		var encoded []byte
		if record.BlobID != "" {
			encoded, err = bson.Marshal(migratePayloadRecord{BlobID: record.BlobID, Data: change(record.Data)})
		} else {
			encoded, err = bson.Marshal(migrateRangeRecord{Key: record.Key, Value: record.Value})
		}
		if err != nil {
			t.Fatalf("encoding a stream record: %v", err)
		}
		rewritten.Write(encoded)
	}
	return rewritten.Bytes()
}

// TestMigratedPayloadOfTheWrongLengthIsRefusedBeforeSealing is spec §4.2.1 c's
// last sentence: a target checks a migrated payload against its metadata's size
// before sealing it, and a refusal leaves no payload file and no bucket.
func TestMigratedPayloadOfTheWrongLengthIsRefusedBeforeSealing(t *testing.T) {
	useReclaimClock(t, newFakeReclaimClock())
	source, _ := newSealedKVFaultServer(t, 0x10)
	target, _ := newSealedKVFaultServer(t, 0x70)
	createBlobBucket(t, source, "moved")
	writeBlobOrFail(t, source, "app", "moved", "big", sealedTestContent())
	moved := lookupBucket(source, "app", "moved")
	var stream bytes.Buffer
	if err := source.streamBucketRange(&stream, source.kv, moved.slot, moved.databaseID, moved.id.Load()); err != nil {
		t.Fatalf("streaming: %v", err)
	}
	truncated := rewriteStreamPayloads(t, stream.Bytes(), func(data []byte) []byte { return data[:len(data)-1] })

	if resp, err := target.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("create database on the target: %+v, %v", resp, err)
	}
	live, err := target.liveGenerations("app")
	if err != nil || len(live) != 1 {
		t.Fatalf("live generations on the target: %v, %v", live, err)
	}
	err = target.materializeRange(migratedBucket{database: "app", name: "moved", storeType: BucketTypeBlob, generation: live[0]}, truncated, 0)
	if err == nil {
		t.Fatal("the target accepted a payload one byte shorter than its metadata's size")
	}
	if !strings.Contains(err.Error(), "size") {
		t.Errorf("the refusal does not name the size mismatch: %v", err)
	}
	var written []string
	walkErr := filepath.WalkDir(target.blobRoot(), func(path string, entry fs.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			written = append(written, path)
		}
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	})
	if walkErr != nil || len(written) != 0 {
		t.Fatalf("the refused migration left files %v (%v)", written, walkErr)
	}
}

// TestSealedPayloadReadsAgainAfterARestartOfTheKeySource: the registry minted at
// the first start is the one the second start loads, so what the first start
// sealed the second reads.
func TestSealedPayloadReadsAgainAfterARestartOfTheKeySource(t *testing.T) {
	s, kv := newSealedKVFaultServer(t, 0x10)
	createBlobBucket(t, s, "uploads")
	writeBlobOrFail(t, s, "app", "uploads", "report", sealedTestContent())

	reloaded, err := LoadOrCreateBlobKeySource(s.config, s.nodeMaster, kv, discardLogger())
	if err != nil {
		t.Fatalf("reloading the blob key source: %v", err)
	}
	s.blobKeys = reloaded
	if got := readBlobOrFail(t, s, "app", "uploads", "report"); !bytes.Equal(got, sealedTestContent()) {
		t.Fatal("the reloaded key source does not read what the first one sealed")
	}
}

// TestTamperedBlobFindAnswersWithRestoreAdviceNotRetry: the find of a damaged
// blob carries its restore advice, and BUG-0591's "retry the command once"
// sentence for a plain read failure is wrong for damage a retry cannot repair.
func TestTamperedBlobFindAnswersWithRestoreAdviceNotRetry(t *testing.T) {
	s, _ := newSealedKVFaultServer(t, 0x10)
	createBlobBucket(t, s, "uploads")
	writeBlobOrFail(t, s, "app", "uploads", "report", sealedTestContent())
	payload := payloadFilesOf(t, s.blobRoot(), lookupBucket(s, "app", "uploads").id.Load())[0]
	onDisk, err := os.ReadFile(payload)
	if err != nil {
		t.Fatalf("reading the payload: %v", err)
	}
	onDisk[sealed.FileHeaderLen+sealed.FileNonceLen+9] ^= 0x01
	if err := os.WriteFile(payload, onDisk, 0o600); err != nil {
		t.Fatalf("tampering with the payload: %v", err)
	}

	resp, err := s.Find(FindRequest{Database: "app", Bucket: "uploads", Key: "report"})
	if err != nil || resp.Found {
		t.Fatalf("a tampered blob was served: %+v, %v", resp, err)
	}
	if !strings.Contains(resp.Message, "Restore this blob from a replica or a backup") {
		t.Errorf("the message lacks the restore advice: %s", resp.Message)
	}
	for _, banned := range []string{"retry the command once", "node log"} {
		if strings.Contains(resp.Message, banned) {
			t.Errorf("the damaged-blob message carries %q: %s", banned, resp.Message)
		}
	}
}
