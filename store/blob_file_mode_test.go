package store

import (
	"os"
	"path/filepath"
	"testing"
)

// TestBlobPayloadFileAndLeafDirModes: blob payloads are the one
// remaining plaintext user-data format on disk, so WritePayload must create
// them at the F13 hygiene rule's 0600/0700 — the same house pattern already
// used for auth.token.key and users.json (authkey.go:LoadOrCreateAuthKey,
// users.go:usersConfig.Save) — instead of leaving the process umask default
// (0644 for the file, 0755 for the leaf directory) in place.
func TestBlobPayloadFileAndLeafDirModes(t *testing.T) {
	bs, _, root := newTestBlobStore(t)

	if _, err := bs.Put(ApplyInfo{}, "k", blobContent(InlineThreshold+1, 'p'), nil); err != nil {
		t.Fatalf("put: %v", err)
	}

	files := payloadFiles(t, root)
	if len(files) != 1 {
		t.Fatalf("payload files = %v, want exactly one", files)
	}
	payloadPath := filepath.Join(root, files[0])

	fileInfo, err := os.Stat(payloadPath)
	if err != nil {
		t.Fatalf("stat payload file: %v", err)
	}
	if perm := fileInfo.Mode().Perm(); perm != 0o600 {
		t.Fatalf("payload file %s permissions = %o, want 0600", payloadPath, perm)
	}

	leaf := filepath.Dir(payloadPath)
	leafInfo, err := os.Stat(leaf)
	if err != nil {
		t.Fatalf("stat leaf directory: %v", err)
	}
	if perm := leafInfo.Mode().Perm(); perm != 0o700 {
		t.Fatalf("leaf directory %s permissions = %o, want 0700", leaf, perm)
	}
}
