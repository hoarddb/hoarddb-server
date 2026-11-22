package transport

import (
	"os"
	"path/filepath"
	"testing"
)

// TestGenerateEd25519KeysWritesDurably guards BUG-0740: both files go through
// the shared durable write, so a power cut leaves no file or a whole file,
// never an empty one. Both files are 0600: the server runs under umask 077, so
// the certificate was owner-only in practice.
func TestGenerateEd25519KeysWritesDurably(t *testing.T) {
	var written []string
	original := writeKeyFileDurably
	writeKeyFileDurably = func(path string, contents []byte) error {
		written = append(written, filepath.Base(path))
		return original(path, contents)
	}
	t.Cleanup(func() { writeKeyFileDurably = original })

	dir := filepath.Join(t.TempDir(), "keys")
	paths, _, err := GenerateEd25519Keys(dir)
	if err != nil {
		t.Fatalf("GenerateEd25519Keys: %v", err)
	}
	if len(written) != 2 || written[0] != "server.key" || written[1] != "server.crt" {
		t.Fatalf("durable writes = %v, want [server.key server.crt]", written)
	}
	for path, want := range map[string]os.FileMode{paths.Key: 0o600, paths.Cert: 0o600} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if info.Mode().Perm() != want {
			t.Errorf("%s mode = %04o, want %04o", filepath.Base(path), info.Mode().Perm(), want)
		}
	}
	leftovers, _ := filepath.Glob(filepath.Join(dir, "*.tmp"))
	if len(leftovers) != 0 {
		t.Errorf("temporary files left behind: %v", leftovers)
	}
}
