package server

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-server/sealed"
)

// TestPrevDeletedOnlyWhenKidChanges asserts that two saves under the same kid
// leave .prev; a save under a new kid removes it.
func TestPrevDeletedOnlyWhenKidChanges(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)
	cfg := validUsersConfig()

	// Create the first node master with computed kid
	nm1Bytes := make([]byte, 32)
	for i := range nm1Bytes {
		nm1Bytes[i] = byte(i)
	}
	nm1 := &NodeMaster{
		bytes:   nm1Bytes,
		kid:     sealed.Kid(nm1Bytes), // Compute the kid from the bytes
		created: false,
	}

	// Save with first node master
	if err := cfg.Save(path, usersSealing{Keyring: nil, NodeMaster: nm1, Mode: "on"}); err != nil {
		t.Fatalf("first Save: %v", err)
	}

	// Save again with the same node master (should not delete .prev)
	cfg.Version = 13
	if err := cfg.Save(path, usersSealing{Keyring: nil, NodeMaster: nm1, Mode: "on"}); err != nil {
		t.Fatalf("second Save with same kid: %v", err)
	}

	// .prev should still exist
	_, err := os.Stat(path + ".prev")
	if os.IsNotExist(err) {
		t.Fatal(".prev was deleted on a save with the same kid; it should be kept")
	}

	// Create a different node master with a different kid
	nm2Bytes := make([]byte, 32)
	for i := range nm2Bytes {
		nm2Bytes[i] = byte(i + 100)
	}
	nm2 := &NodeMaster{
		bytes:   nm2Bytes,
		kid:     sealed.Kid(nm2Bytes), // Compute the kid from the bytes
		created: false,
	}

	// Save with the new node master (should delete .prev)
	cfg.Version = 14
	if err := cfg.Save(path, usersSealing{Keyring: nil, NodeMaster: nm2, Mode: "on"}); err != nil {
		t.Fatalf("third Save with different kid: %v", err)
	}

	// .prev should be deleted now
	_, err = os.Stat(path + ".prev")
	if err == nil {
		t.Fatal(".prev still exists after a save with a different kid; it should have been deleted")
	}
	if !os.IsNotExist(err) {
		t.Fatalf("Stat .prev error: %v", err)
	}
}

// TestPrevIsNeverPlaintextAfterEnabling asserts that the first sealed save
// over a plaintext users.json (no prior kid) does not leave users.json.prev
// holding the plaintext credential store (the delete guard at
// users.go:usersConfig.Save required oldKid != "", which is false on this
// no-key -> key transition, so the plaintext .prev survived the switch to
// sealed storage).
func TestPrevIsNeverPlaintextAfterEnabling(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)
	cfg := validUsersConfig()

	// Start with a plaintext file at mode=off, and capture the plaintext
	// bytes a stale .prev must never match once sealing is enabled.
	if err := cfg.Save(path, usersSealing{Keyring: nil, NodeMaster: nil, Mode: "off"}); err != nil {
		t.Fatalf("first Save plaintext: %v", err)
	}
	preSealPlaintext, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read pre-seal plaintext: %v", err)
	}

	// Enable encryption with a node master: oldKid == "" (no prior seal),
	// newKid != "" — the first-seal transition that guard missed.
	nm := &NodeMaster{
		bytes:   make([]byte, 32),
		kid:     sealed.Kid([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}),
		created: false,
	}
	copy(nm.bytes, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31})

	cfg.Version = 13
	if err := cfg.Save(path, usersSealing{Keyring: nil, NodeMaster: nm, Mode: "on"}); err != nil {
		t.Fatalf("Save with encryption: %v", err)
	}

	prevData, err := os.ReadFile(path + ".prev")
	if err != nil {
		if !os.IsNotExist(err) {
			t.Fatalf("Stat .prev: %v", err)
		}
		return // .prev absent: the first-seal transition left no stale backup.
	}
	if bytes.Equal(prevData, preSealPlaintext) {
		t.Fatalf("users.json.prev holds the plaintext pre-seal credential store after the first sealed save: %s", prevData)
	}
}

// TestSaveFsyncsDirectory asserts that the directory fsync happens after the
// rename and before any .prev delete (assert by call order through a seam,
// not by observing the filesystem).
func TestSaveFsyncsDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)
	cfg := validUsersConfig()

	// Create the first node master and save
	nm1Bytes := make([]byte, 32)
	for i := range nm1Bytes {
		nm1Bytes[i] = byte(i)
	}
	nm1 := &NodeMaster{
		bytes:   nm1Bytes,
		kid:     sealed.Kid(nm1Bytes),
		created: false,
	}

	if err := cfg.Save(path, usersSealing{Keyring: nil, NodeMaster: nm1, Mode: "on"}); err != nil {
		t.Fatalf("first Save: %v", err)
	}

	// Track the order of operations with a call log
	var callOrder []string
	oldDirFsyncer := dirFsyncer
	oldRenamer := usersFileRenamer
	oldDeleter := prevFileDeleter
	defer func() {
		dirFsyncer = oldDirFsyncer
		usersFileRenamer = oldRenamer
		prevFileDeleter = oldDeleter
	}()

	usersFileRenamer = func(oldpath, newpath string) error {
		callOrder = append(callOrder, "rename")
		return oldRenamer(oldpath, newpath)
	}

	dirFsyncer = func(dirPath string) error {
		callOrder = append(callOrder, "dirFsync")
		return oldDirFsyncer(dirPath)
	}

	prevFileDeleter = func(path string) error {
		callOrder = append(callOrder, "prevDelete")
		return oldDeleter(path)
	}

	// Create a different node master to trigger .prev deletion
	nm2Bytes := make([]byte, 32)
	for i := range nm2Bytes {
		nm2Bytes[i] = byte(i + 100)
	}
	nm2 := &NodeMaster{
		bytes:   nm2Bytes,
		kid:     sealed.Kid(nm2Bytes),
		created: false,
	}

	cfg.Version = 13
	if err := cfg.Save(path, usersSealing{Keyring: nil, NodeMaster: nm2, Mode: "on"}); err != nil {
		t.Fatalf("second Save: %v", err)
	}

	// Verify the call order: rename → dirFsync → prevDelete
	expectedOrder := []string{"rename", "dirFsync", "prevDelete"}
	if len(callOrder) != len(expectedOrder) {
		t.Fatalf("expected %d operations, got %d: %v", len(expectedOrder), len(callOrder), callOrder)
	}
	for i, expected := range expectedOrder {
		if i >= len(callOrder) || callOrder[i] != expected {
			t.Fatalf("operation %d: expected %q, got %q (full sequence: %v)", i, expected, callOrder[i], callOrder)
		}
	}
}

// TestTempFileNeverSurvivesAFailedSave asserts that .users-*.json is absent
// after a failed save.
func TestTempFileNeverSurvivesAFailedSave(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)
	cfg := validUsersConfig()

	// Create a node master that's nil to cause the save to fail at mode=on
	// (or we could make the config invalid)
	err := cfg.Save(path, usersSealing{Keyring: nil, NodeMaster: nil, Mode: "on"})
	if err == nil {
		t.Fatal("Save should have failed")
	}

	// Scan the directory for temp files
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if len(name) > len(".users-") && name[:7] == ".users-" && name[len(name)-5:] == ".json" {
			t.Fatalf("temp file %s was not cleaned up after failed save", name)
		}
	}
}

// testNodeMaster is a node master with fixed bytes, for sealing users.json.
func testNodeMaster(seed byte) *NodeMaster {
	masterBytes := make([]byte, 32)
	for i := range masterBytes {
		masterBytes[i] = seed + byte(i)
	}
	return &NodeMaster{bytes: masterBytes, kid: sealed.Kid(masterBytes)}
}

// TestStartupSweepsPlaintextPrevBesideSealedUsers: the first
// sealing save writes the plaintext store to users.json.prev and deletes it
// last, so a crash or a failed delete after the rename leaves the plaintext
// beside the sealed file. Start-up must remove it.
func TestStartupSweepsPlaintextPrevBesideSealedUsers(t *testing.T) {
	path := filepath.Join(t.TempDir(), UsersFileName)
	cfg := validUsersConfig()
	if err := cfg.Save(path, usersSealing{Mode: "off"}); err != nil {
		t.Fatalf("plaintext Save: %v", err)
	}

	original := prevFileDeleter
	prevFileDeleter = func(string) error { return errors.New("injected: the process stopped before the delete") }
	nm := testNodeMaster(1)
	cfg.Version = 13
	saveErr := cfg.Save(path, usersSealing{NodeMaster: nm, Mode: "on"})
	prevFileDeleter = original
	if saveErr == nil {
		t.Fatal("the first sealing Save succeeded with its .prev delete failing; the crash state was not built")
	}
	prev, err := os.ReadFile(path + ".prev")
	if err != nil || isEncryptedUsersFile(prev) {
		t.Fatalf("crash state not built: .prev read err %v, sealed %v", err, isEncryptedUsersFile(prev))
	}

	am := newTokenManager(t, nil, nil)
	if _, _, err := am.LoadUsers(path, nm, nil); err != nil {
		t.Fatalf("LoadUsers: %v", err)
	}
	if _, err := os.Stat(path + ".prev"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("users.json.prev survived start-up beside a sealed users.json (stat err %v)", err)
	}
}

// TestStartupKeepsPrevThatIsNotAPlaintextLeak pins what the sweep leaves
// alone: a sealed .prev (rotation's fallback), and any .prev beside a
// plaintext users.json (a node that never sealed).
func TestStartupKeepsPrevThatIsNotAPlaintextLeak(t *testing.T) {
	cases := []struct {
		name   string
		saveAs []usersSealing
		nm     *NodeMaster
	}{
		{"sealed prev beside sealed file", []usersSealing{
			{NodeMaster: testNodeMaster(1), Mode: "on"},
			{NodeMaster: testNodeMaster(1), Mode: "on"},
		}, testNodeMaster(1)},
		{"plaintext prev beside plaintext file", []usersSealing{
			{Mode: "off"},
			{Mode: "off"},
		}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), UsersFileName)
			cfg := validUsersConfig()
			for i, sealing := range tc.saveAs {
				cfg.Version = uint64(20 + i)
				if err := cfg.Save(path, sealing); err != nil {
					t.Fatalf("Save %d: %v", i+1, err)
				}
			}
			if _, err := os.Stat(path + ".prev"); err != nil {
				t.Fatalf("setup left no .prev: %v", err)
			}
			am := newTokenManager(t, nil, nil)
			if _, _, err := am.LoadUsers(path, tc.nm, nil); err != nil {
				t.Fatalf("LoadUsers: %v", err)
			}
			if _, err := os.Stat(path + ".prev"); err != nil {
				t.Fatalf("start-up removed a .prev it must keep: %v", err)
			}
		})
	}
}
