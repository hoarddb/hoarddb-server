//go:build unix

package transport

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// TestKnownServersCreatesOwnerOnlyModesUnderUmaskZero (BUG-0532): a new
// directory is 0700 and a new file 0600 even when the umask would allow more.
func TestKnownServersCreatesOwnerOnlyModesUnderUmaskZero(t *testing.T) {
	previous := syscall.Umask(0)
	t.Cleanup(func() { syscall.Umask(previous) })
	dir := filepath.Join(ownerOnlyDir(t), "fresh")
	path := filepath.Join(dir, "known_servers")
	known := loadKnownServers(t, path)
	if err := known.Trust("db:7433", knownKeyA); err != nil {
		t.Fatalf("Trust: %v", err)
	}
	for target, want := range map[string]os.FileMode{dir: 0o700, path: 0o600} {
		info, err := os.Stat(target)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Errorf("%s created %04o, want %04o", target, got, want)
		}
	}
}
