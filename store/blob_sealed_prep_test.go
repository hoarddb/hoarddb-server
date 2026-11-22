package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestBlobChecksumMismatchIsItsOwnErrorAndCarriesNoHash is outcome 3 of the
// at-rest encryption spec's §4.2.1 h on the path that exists today: a payload
// that reads whole but does not match its metadata is ErrBlobChecksumMismatch,
// and its text names neither the SHA-256 the put recorded nor the one the
// content hashes to — both are hashes of user content, and an error is a log
// line.
func TestBlobChecksumMismatchIsItsOwnErrorAndCarriesNoHash(t *testing.T) {
	bs, _, root := newTestBlobStore(t)
	content := blobContent(InlineThreshold+1, 'c')
	blobID, err := bs.Put(ApplyInfo{}, "report", content, nil)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	files := payloadFiles(t, root)
	if len(files) != 1 {
		t.Fatalf("the put left payload files %v, want exactly one", files)
	}
	payloadPath := filepath.Join(root, files[0])
	changed := append([]byte(nil), content...)
	changed[len(changed)/2] ^= 0x01
	if err := os.WriteFile(payloadPath, changed, 0o600); err != nil {
		t.Fatalf("changing the payload: %v", err)
	}
	recordedHash := sha256.Sum256(content)
	computedHash := sha256.Sum256(changed)

	stored, err := bs.Get("report")
	if stored != nil {
		t.Fatalf("a blob whose content does not match its checksum was returned (%d bytes)", len(stored.Content))
	}
	if err == nil {
		t.Fatal("want ErrBlobChecksumMismatch, got no error")
	}
	// The text is printed below only with both hashes replaced: a failure
	// message of this test is a log line too.
	recordedHex, computedHex := hex.EncodeToString(recordedHash[:]), hex.EncodeToString(computedHash[:])
	shown := strings.NewReplacer(recordedHex, "<the recorded SHA-256>", computedHex, "<the computed SHA-256>").Replace(err.Error())
	if !errors.Is(err, ErrBlobChecksumMismatch) {
		t.Errorf("want ErrBlobChecksumMismatch, got: %s", shown)
	}
	for what, hashHex := range map[string]string{"the recorded SHA-256": recordedHex, "the computed SHA-256": computedHex} {
		if strings.Contains(err.Error(), hashHex) {
			t.Errorf("the error text carries %s: %s", what, shown)
		}
	}
	for _, want := range []string{
		blobID,
		"report",
		"the metadata and the payload disagree",
		"Nothing was returned",
		"Re-put the blob, or restore its metadata and its payload from the same backup",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error text does not contain %q: %s", want, shown)
		}
	}
}

// TestMagicPrefixedBlobOnAPlaintextNodeIsReadVerbatim pins at-rest encryption
// spec §4.2.1 e on a plaintext node: a blob store with no key source never
// inspects a payload's first bytes. This content begins with what a sealed
// payload's header looks like — magic, version 0x02, purpose 0x06, flags and
// reserved 0 — and must be stored and returned byte for byte. It is green
// today because nothing sniffs; it is here so that the build that teaches the
// store to read sealed payloads cannot start deciding from the bytes.
func TestMagicPrefixedBlobOnAPlaintextNodeIsReadVerbatim(t *testing.T) {
	bs, _, root := newTestBlobStore(t)
	sealedLookingHeader := []byte{
		0x00, 'H', 'D', 'B', 0x02, 0x06, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	content := append(append([]byte(nil), sealedLookingHeader...), blobContent(InlineThreshold, 'u')...)

	if _, err := bs.Put(ApplyInfo{}, "lookalike", content, nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	files := payloadFiles(t, root)
	if len(files) != 1 {
		t.Fatalf("the put left payload files %v, want exactly one: the content must be file-backed for this test", files)
	}
	onDisk, err := os.ReadFile(filepath.Join(root, files[0]))
	if err != nil {
		t.Fatalf("reading the payload file: %v", err)
	}
	if !bytes.Equal(onDisk, content) {
		t.Fatal("the payload file is not the content, byte for byte")
	}
	stored, err := bs.Get("lookalike")
	if err != nil || stored == nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(stored.Content, content) {
		t.Fatalf("get returned %d bytes that differ from the %d written", len(stored.Content), len(content))
	}
}
