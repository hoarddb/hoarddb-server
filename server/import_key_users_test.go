package server

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestImportRefusesAMasterThatDoesNotOpenTheSealedUsersFile is BUG-0679:
// -import-key checked an imported node master against the credential files,
// the store key and the registries, but not against users.json, which is
// sealed under the node master too. A wrong master was accepted and left every
// user unreadable.
func TestImportRefusesAMasterThatDoesNotOpenTheSealedUsersFile(t *testing.T) {
	dataDir := t.TempDir()
	cfg := sealedTestConfigAt(t, dataDir, sealEdit(t, "require"))
	keystore, err := OpenKeystore(cfg)
	if err != nil {
		t.Fatalf("OpenKeystore: %v", err)
	}
	current, err := LoadOrCreateNodeMaster(dataDir, keystore)
	if err != nil {
		t.Fatalf("creating the node master: %v", err)
	}
	// Only users.json is sealed here, so no other check can be the one to refuse.
	sealedUsers, err := sealUsersWithNodeMaster([]byte(`{}`), current, 1)
	if err != nil {
		t.Fatalf("sealing users.json: %v", err)
	}
	usersPath := UsersFilePath(dataDir)
	if err := os.WriteFile(usersPath, sealedUsers, 0o600); err != nil {
		t.Fatalf("writing users.json: %v", err)
	}
	masterPath := filepath.Join(dataDir, "keys", NodeMasterFileName)
	masterBefore, err := os.ReadFile(masterPath)
	if err != nil {
		t.Fatalf("reading the wrapped node master: %v", err)
	}

	wrongFile := filepath.Join(t.TempDir(), "wrong.key")
	if err := os.WriteFile(wrongFile, bytes.Repeat([]byte{0x5A}, 32), 0o600); err != nil {
		t.Fatalf("writing the wrong master: %v", err)
	}
	err = ImportNodeMaster(cfg, wrongFile, true)
	if err == nil || !strings.Contains(err.Error(), "users.json") {
		t.Fatalf("importing a master that cannot open users.json answered %v, want a refusal naming users.json", err)
	}
	masterAfter, err := os.ReadFile(masterPath)
	if err != nil {
		t.Fatalf("reading the wrapped node master after the refusal: %v", err)
	}
	if !bytes.Equal(masterBefore, masterAfter) {
		t.Fatal("a refused import replaced the existing node master")
	}

	if err := verifyImportedMaster(dataDir, current.Bytes()); err != nil {
		t.Fatalf("the master that sealed users.json was refused: %v", err)
	}
}

// TestImportIgnoresAPlaintextUsersFile: a users.json that is not sealed under
// a node master holds nothing an imported master could fail to open.
func TestImportIgnoresAPlaintextUsersFile(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.WriteFile(UsersFilePath(dataDir), []byte(`{"users":[]}`), 0o600); err != nil {
		t.Fatalf("writing users.json: %v", err)
	}
	if err := verifyImportedMaster(dataDir, bytes.Repeat([]byte{0x5A}, 32)); err != nil {
		t.Fatalf("a plaintext users.json refused an imported master: %v", err)
	}
}
