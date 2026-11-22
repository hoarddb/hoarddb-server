package server

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestInitClusterTokenRefusesATokenItCannotRead: a cluster.token the node
// cannot read must stop the start, with the fix, and must be left exactly as
// it was. Minting a fresh token over it would put this node on a secret that
// no other member holds.
func TestInitClusterTokenRefusesATokenItCannotRead(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("an unreadable file is readable by root, so this test cannot make the file unreadable")
	}
	dataDir := t.TempDir()
	tokenPath := filepath.Join(dataDir, "cluster.token")
	original := ClusterToken{Token: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", CreatedAt: time.Now()}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal token: %v", err)
	}
	if err := os.WriteFile(tokenPath, data, 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	if err := os.Chmod(tokenPath, 0o000); err != nil {
		t.Fatalf("chmod token: %v", err)
	}

	_, _, err = InitClusterToken(dataDir)
	if err == nil {
		t.Fatal("InitClusterToken minted a token over a cluster.token it could not read")
	}
	for _, want := range []string{tokenPath, "move cluster.token aside", "nothing is lost without it", "chmod 600"} {
		if !bytes.Contains([]byte(err.Error()), []byte(want)) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}

	if err := os.Chmod(tokenPath, 0o600); err != nil {
		t.Fatalf("chmod token back: %v", err)
	}
	after, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatalf("read token after the refusal: %v", err)
	}
	if !bytes.Equal(after, data) {
		t.Errorf("cluster.token changed despite the refusal:\nbefore: %s\nafter:  %s", data, after)
	}
}

// TestInitClusterTokenRefusesACorruptToken: a cluster.token that does not
// parse is refused and kept for an operator to repair, never replaced.
func TestInitClusterTokenRefusesACorruptToken(t *testing.T) {
	dataDir := t.TempDir()
	tokenPath := filepath.Join(dataDir, "cluster.token")
	corrupt := []byte("{\"token\": \"truncated")
	if err := os.WriteFile(tokenPath, corrupt, 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	_, _, err := InitClusterToken(dataDir)
	if err == nil {
		t.Fatal("InitClusterToken minted a token over a cluster.token it could not parse")
	}
	for _, want := range []string{tokenPath, "move cluster.token aside", "nothing is lost without it"} {
		if !bytes.Contains([]byte(err.Error()), []byte(want)) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}

	after, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatalf("read token after the refusal: %v", err)
	}
	if !bytes.Equal(after, corrupt) {
		t.Errorf("cluster.token changed despite the refusal:\nbefore: %s\nafter:  %s", corrupt, after)
	}
}

// TestInitClusterTokenMintsWhenMissing: with no cluster.token on disk there is
// nothing to preserve, so a first start mints one and reports it as new.
func TestInitClusterTokenMintsWhenMissing(t *testing.T) {
	dataDir := t.TempDir()

	token, generated, err := InitClusterToken(dataDir)
	if err != nil {
		t.Fatalf("InitClusterToken: %v", err)
	}
	if !generated {
		t.Error("a missing cluster.token was not reported as newly generated")
	}
	if len(token) != 64 {
		t.Errorf("token is %d characters, want 64: %q", len(token), token)
	}
	stored, err := LoadClusterToken(dataDir)
	if err != nil {
		t.Fatalf("LoadClusterToken: %v", err)
	}
	if stored.Token != token {
		t.Errorf("the minted token is %q, the file holds %q", token, stored.Token)
	}

	again, generated, err := InitClusterToken(dataDir)
	if err != nil {
		t.Fatalf("InitClusterToken (second start): %v", err)
	}
	if generated {
		t.Error("a second start minted a new token over the existing one")
	}
	if again != token {
		t.Errorf("the second start returned %q, want the stored %q", again, token)
	}
}

// TestSaveClusterTokenReplacesTheFilePrivatelyAndLeavesNoTemporary: a save is
// a replacement of a credential, so the file ends at 0600 however it was
// before, reads back unchanged, and leaves nothing beside it.
func TestSaveClusterTokenReplacesTheFilePrivatelyAndLeavesNoTemporary(t *testing.T) {
	dataDir := t.TempDir()
	tokenPath := filepath.Join(dataDir, "cluster.token")
	if err := os.WriteFile(tokenPath, []byte("a file an earlier build left with a looser mode"), 0o644); err != nil {
		t.Fatalf("write the old token file: %v", err)
	}

	want := ClusterToken{Token: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CreatedAt: time.Now().Truncate(time.Second)}
	if err := SaveClusterToken(dataDir, want); err != nil {
		t.Fatalf("SaveClusterToken: %v", err)
	}

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		t.Fatalf("read data directory: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "cluster.token" {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Errorf("the data directory holds %v, want only cluster.token", names)
	}

	info, err := os.Stat(tokenPath)
	if err != nil {
		t.Fatalf("stat cluster.token: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("cluster.token mode is %04o, want 0600", perm)
	}

	got, err := LoadClusterToken(dataDir)
	if err != nil {
		t.Fatalf("LoadClusterToken: %v", err)
	}
	if got.Token != want.Token {
		t.Errorf("token read back is %q, want %q", got.Token, want.Token)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Errorf("created_at read back is %s, want %s", got.CreatedAt, want.CreatedAt)
	}
}
