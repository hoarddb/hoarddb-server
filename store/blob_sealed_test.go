package store

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/sealed"
)

// testBlobKeys is a sealed.BlobKeySource over fixed epoch keys, standing in for
// the server's registry: the store takes the interface and never the registry.
type testBlobKeys struct {
	current uint64
	epochs  map[uint64][]byte
	// failFileKey, when set, is what FileKey returns for every payload.
	failFileKey error
}

func newTestBlobKeys(epoch uint64, fill byte) *testBlobKeys {
	return &testBlobKeys{current: epoch, epochs: map[uint64][]byte{epoch: bytes.Repeat([]byte{fill}, 32)}}
}

func (keys *testBlobKeys) CurrentEpoch() uint64 { return keys.current }

func (keys *testBlobKeys) FileKey(epoch uint64, payloadName string) ([]byte, error) {
	if keys.failFileKey != nil {
		return nil, keys.failFileKey
	}
	epochKey, held := keys.epochs[epoch]
	if !held {
		return nil, fmt.Errorf("epoch %d: %w", epoch, sealed.ErrBlobKeyMissing)
	}
	return sealed.FileKey(epochKey, sealed.PurposeBlobChunk, payloadName)
}

func newSealedTestBlobStore(t *testing.T, keys sealed.BlobKeySource) (*BlobStore, string) {
	t.Helper()
	bs, _, root := newTestBlobStore(t)
	bs.keys = keys
	return bs, root
}

// onlyPayload returns the single payload file under root.
func onlyPayload(t *testing.T, root string) string {
	t.Helper()
	files := payloadFiles(t, root)
	if len(files) != 1 {
		t.Fatalf("payload files = %v, want exactly one", files)
	}
	return filepath.Join(root, files[0])
}

// TestSealedBlobPayloadIsASealedFileAndRoundTrips: with a key source, a payload
// above the inline threshold is a purpose-0x06 sealed file of exactly
// 24 + size + 28·n bytes that holds no byte of the content, and Get returns the
// content.
func TestSealedBlobPayloadIsASealedFileAndRoundTrips(t *testing.T) {
	bs, root := newSealedTestBlobStore(t, newTestBlobKeys(1, 0x11))
	marker := []byte("PLAINTEXT-MARKER-0123456789")
	content := append(blobContent(3*sealed.BlobChunkSize+17, 'm'), marker...)

	if _, err := bs.Put(ApplyInfo{}, "report", content, nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	onDisk, err := os.ReadFile(onlyPayload(t, root))
	if err != nil {
		t.Fatalf("reading the payload: %v", err)
	}
	if bytes.Contains(onDisk, marker) || bytes.Contains(onDisk, content[:64]) {
		t.Fatal("the payload file holds the content in the clear")
	}
	header, err := sealed.ParseFileHeader(onDisk)
	if err != nil {
		t.Fatalf("the payload file does not begin with a sealed header: %v", err)
	}
	if header.Purpose != sealed.PurposeBlobChunk || header.EpochID != 1 {
		t.Errorf("header purpose %#x epoch %d, want purpose 0x06 epoch 1", header.Purpose, header.EpochID)
	}
	wantLen, err := sealed.BlobFileLen(int64(len(content)))
	if err != nil || int64(len(onDisk)) != wantLen {
		t.Errorf("payload file is %d bytes, want 24 + size + 28·n = %d (%v)", len(onDisk), wantLen, err)
	}

	stored, err := bs.Get("report")
	if err != nil || stored == nil {
		t.Fatalf("get: %+v, %v", stored, err)
	}
	if !bytes.Equal(stored.Content, content) {
		t.Fatalf("get returned %d bytes that differ from the %d written", len(stored.Content), len(content))
	}
}

// TestTamperedBlobChunkIsCorruptionNamingTheBlob: one flipped ciphertext byte
// is outcome 2 of spec §4.2.1 h — ErrBlobCorrupt, not the checksum error — and
// nothing is returned.
func TestTamperedBlobChunkIsCorruptionNamingTheBlob(t *testing.T) {
	bs, root := newSealedTestBlobStore(t, newTestBlobKeys(1, 0x11))
	blobID, err := bs.Put(ApplyInfo{}, "report", blobContent(2*sealed.BlobChunkSize, 'c'), nil)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	payloadPath := onlyPayload(t, root)
	onDisk, err := os.ReadFile(payloadPath)
	if err != nil {
		t.Fatalf("reading the payload: %v", err)
	}
	onDisk[sealed.FileHeaderLen+sealed.FileNonceLen+5] ^= 0x01
	if err := os.WriteFile(payloadPath, onDisk, 0o600); err != nil {
		t.Fatalf("tampering with the payload: %v", err)
	}

	stored, err := bs.Get("report")
	if stored != nil {
		t.Fatalf("a tampered blob was returned (%d bytes)", len(stored.Content))
	}
	var corrupt *sealed.BlobCorruptError
	if !errors.As(err, &corrupt) || !errors.Is(err, sealed.ErrBlobCorrupt) {
		t.Fatalf("want a *sealed.BlobCorruptError, got: %v", err)
	}
	if errors.Is(err, ErrBlobChecksumMismatch) {
		t.Error("a chunk that failed authentication was reported as a checksum mismatch")
	}
	if corrupt.Reason != sealed.BlobCorruptChunkAuthentication || corrupt.Chunk != 0 {
		t.Errorf("corruption = %+v, want chunk 0 failing authentication", corrupt)
	}
	for _, want := range []string{blobID, "report"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not name %q: %v", want, err)
		}
	}
}

// TestSealedNodeNeverReadsAPayloadAsPlaintext: a plaintext file under a sealed
// blob's name is corruption, with no fallback (spec §4.2.1 e).
func TestSealedNodeNeverReadsAPayloadAsPlaintext(t *testing.T) {
	bs, root := newSealedTestBlobStore(t, newTestBlobKeys(1, 0x11))
	content := blobContent(InlineThreshold+1, 'p')
	if _, err := bs.Put(ApplyInfo{}, "report", content, nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := os.WriteFile(onlyPayload(t, root), content, 0o600); err != nil {
		t.Fatalf("substituting a plaintext file: %v", err)
	}

	stored, err := bs.Get("report")
	if stored != nil {
		t.Fatal("a plaintext file under a sealed blob's name was served")
	}
	if !errors.Is(err, sealed.ErrBlobCorrupt) {
		t.Fatalf("want ErrBlobCorrupt, got: %v", err)
	}
}

// TestBlobSealedUnderAMissingEpochIsAKeyProblemNotDamage is outcome 1 of spec
// §4.2.1 h.
func TestBlobSealedUnderAMissingEpochIsAKeyProblemNotDamage(t *testing.T) {
	bs, _ := newSealedTestBlobStore(t, newTestBlobKeys(1, 0x11))
	if _, err := bs.Put(ApplyInfo{}, "report", blobContent(InlineThreshold+1, 'k'), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	bs.keys = newTestBlobKeys(2, 0x22)

	_, err := bs.Get("report")
	if !errors.Is(err, sealed.ErrBlobKeyMissing) {
		t.Fatalf("want ErrBlobKeyMissing, got: %v", err)
	}
	if errors.Is(err, sealed.ErrBlobCorrupt) {
		t.Error("a missing epoch was reported as damage to the payload")
	}
}

// TestSealedPayloadIsBoundToItsOwnFileName: two payloads of one size swapped on
// disk both fail, because the file key is derived from the final name the
// reader recomputes and not from the metadata's path.
func TestSealedPayloadIsBoundToItsOwnFileName(t *testing.T) {
	bs, root := newSealedTestBlobStore(t, newTestBlobKeys(1, 0x11))
	for _, key := range []string{"first", "second"} {
		if _, err := bs.Put(ApplyInfo{}, key, blobContent(InlineThreshold+1, key[0]), nil); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	files := payloadFiles(t, root)
	if len(files) != 2 {
		t.Fatalf("payload files = %v, want two", files)
	}
	first, second := filepath.Join(root, files[0]), filepath.Join(root, files[1])
	firstBytes, errFirst := os.ReadFile(first)
	secondBytes, errSecond := os.ReadFile(second)
	if err := errors.Join(errFirst, errSecond); err != nil {
		t.Fatalf("reading the payloads: %v", err)
	}
	if err := errors.Join(os.WriteFile(first, secondBytes, 0o600), os.WriteFile(second, firstBytes, 0o600)); err != nil {
		t.Fatalf("swapping the payloads: %v", err)
	}

	for _, key := range []string{"first", "second"} {
		if stored, err := bs.Get(key); stored != nil || !errors.Is(err, sealed.ErrBlobCorrupt) {
			t.Errorf("get %s after the swap = %+v, %v; want ErrBlobCorrupt", key, stored, err)
		}
	}
}

// TestRefusedSealLeavesNoFileBehind: a payload that cannot be sealed fails the
// put and leaves neither the final file nor its temporary.
func TestRefusedSealLeavesNoFileBehind(t *testing.T) {
	keys := newTestBlobKeys(1, 0x11)
	keys.failFileKey = errors.New("the registry is unavailable")
	bs, root := newSealedTestBlobStore(t, keys)

	if _, err := bs.Put(ApplyInfo{}, "report", blobContent(InlineThreshold+1, 'r'), nil); err == nil {
		t.Fatal("a put whose payload could not be sealed succeeded")
	}
	if files := payloadFiles(t, root); len(files) != 0 {
		t.Fatalf("a refused seal left %v behind", files)
	}
	if stored, err := bs.Get("report"); err != nil || stored != nil {
		t.Fatalf("a refused put left a blob: %+v, %v", stored, err)
	}
}

// TestSealedBlobBelowTheThresholdStaysInline: content below the threshold is in
// the metadata value, sealed with the store, and writes no payload file.
func TestSealedBlobBelowTheThresholdStaysInline(t *testing.T) {
	bs, root := newSealedTestBlobStore(t, newTestBlobKeys(1, 0x11))
	content := blobContent(InlineThreshold-1, 'i')
	if _, err := bs.Put(ApplyInfo{}, "small", content, nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	if files := payloadFiles(t, root); len(files) != 0 {
		t.Fatalf("inline content wrote payload files %v", files)
	}
	stored, err := bs.Get("small")
	if err != nil || stored == nil || !bytes.Equal(stored.Content, content) {
		t.Fatalf("get small: %+v, %v", stored, err)
	}
}
