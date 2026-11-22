package server

import (
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/argon2"
)

// pepperManager builds an AuthManager over a real keyring with an explicit
// deployment id, which is what the pepper's HKDF info depends on.
func pepperManager(t *testing.T, keyring *Keyring, deploymentID string) *AuthManager {
	t.Helper()
	if keyring == nil {
		var err error
		keyring, err = BuildKeyring(t.TempDir(), "", deploymentID, time.Hour, discardLogger())
		if err != nil {
			t.Fatalf("BuildKeyring: %v", err)
		}
	}
	return NewAuthManager(AuthTokenOptions{
		TTL:          15 * time.Minute,
		MaxLifetime:  12 * time.Hour,
		Skew:         time.Minute,
		Keyring:      keyring,
		DeploymentID: deploymentID,
	}, discardLogger())
}

// TestUserRecordRoundTripsPeppered asserts on the persisted bytes: the value
// in users.json is the peppered HMAC, not the client's Argon2 hash (spec 8.9).
func TestUserRecordRoundTripsPeppered(t *testing.T) {
	am := pepperManager(t, nil, "dep-1")
	const password = "correct-horse-battery"
	if err := am.SeedRootUser("alice", password); err != nil {
		t.Fatalf("SeedRootUser: %v", err)
	}

	record, err := am.recordFor("alice")
	if err != nil {
		t.Fatalf("recordFor: %v", err)
	}
	salt, err := base64.StdEncoding.DecodeString(record.Credentials.Salt)
	if err != nil {
		t.Fatalf("decode salt: %v", err)
	}
	params := record.Credentials.Params
	clientHash := argon2.IDKey([]byte(password), salt,
		params.Iterations, params.Memory, params.Parallelism, uint32(params.HashLen))
	clientB64 := base64.StdEncoding.EncodeToString(clientHash)
	if record.Credentials.Hash == clientB64 {
		t.Fatal("stored hash equals the client's Argon2 hash; the pepper was not applied")
	}

	// The exact bytes the client would send must not appear in the file.
	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)
	cfg := &usersConfig{
		FormatVersion: usersFormatVersion,
		Term:          1,
		Version:       1,
		Master:        "node-a",
		Users:         []UserRecord{*record},
	}
	if err := cfg.Save(path, usersSealing{Keyring: nil, NodeMaster: nil, Mode: "off"}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if strings.Contains(string(data), clientB64) {
		t.Fatal("users.json contains the unpeppered client hash")
	}
	if strings.Contains(string(data), password) {
		t.Fatal("users.json contains the plaintext password")
	}

	ok, err := am.VerifyUser("alice", password)
	if err != nil || !ok {
		t.Fatalf("VerifyUser = %v, %v, want true, nil", ok, err)
	}
}

// TestPepperVerifiesOnlyUnderItsOwnKid puts a record written under one pepper
// in front of a manager with a different pepper (same key, different
// deployment id) and requires the verification to fail.
func TestPepperVerifiesOnlyUnderItsOwnKid(t *testing.T) {
	keyring, err := BuildKeyring(t.TempDir(), "", "", time.Hour, discardLogger())
	if err != nil {
		t.Fatalf("BuildKeyring: %v", err)
	}
	writer := pepperManager(t, keyring, "deployment-a")
	if err := writer.SeedRootUser("alice", "correct-horse-battery"); err != nil {
		t.Fatalf("SeedRootUser: %v", err)
	}
	record, err := writer.recordFor("alice")
	if err != nil {
		t.Fatalf("recordFor: %v", err)
	}

	// Same keyring, different deployment id: same kid, different pepper.
	reader := pepperManager(t, keyring, "deployment-b")
	reader.mu.Lock()
	reader.users["alice"] = record
	reader.mu.Unlock()

	if ok, err := reader.VerifyUser("alice", "correct-horse-battery"); ok || !errors.Is(err, ErrBadPassword) {
		t.Fatalf("VerifyUser under a foreign pepper = %v, %v, want false, ErrBadPassword", ok, err)
	}
	if ok, err := writer.VerifyUser("alice", "correct-horse-battery"); err != nil || !ok {
		t.Fatalf("VerifyUser under its own pepper = %v, %v, want true, nil", ok, err)
	}
}

// TestPepperMissingKidNamesTheFix requires the error to say the key material
// changed rather than "invalid password" (build-doc trap 6).
func TestPepperMissingKidNamesTheFix(t *testing.T) {
	am := pepperManager(t, nil, "dep-1")
	if err := am.SeedRootUser("alice", "correct-horse-battery"); err != nil {
		t.Fatalf("SeedRootUser: %v", err)
	}
	am.mu.Lock()
	am.users["alice"].Credentials.PepperKID = "00000000"
	am.mu.Unlock()

	ok, err := am.VerifyUser("alice", "correct-horse-battery")
	if ok {
		t.Fatal("VerifyUser with a missing kid = true, want false")
	}
	if !errors.Is(err, ErrPepperKeyMissing) {
		t.Fatalf("error = %v, want ErrPepperKeyMissing", err)
	}
	for _, want := range []string{"key material changed", "restore", "reset-password"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not contain %q", err, want)
		}
	}
}

// TestKeyringPreviousKidWindowKeepsCredentials extends the keyring window test
// to a stored credential: after a rotation the record still verifies under the
// previous kid, and once the window closes the referenced key survives pruning.
func TestKeyringPreviousKidWindowKeepsCredentials(t *testing.T) {
	key1 := newSigningKey([]byte("first-key-material-32-bytes!!!!"))
	ring := &Keyring{
		entries:  []keyEntry{{Key: key1, AddedAt: time.Now()}},
		window:   time.Hour,
		keyInUse: func(string) int { return 0 },
	}
	writer := pepperManager(t, ring, "")
	if err := writer.SeedRootUser("alice", "correct-horse-battery"); err != nil {
		t.Fatalf("SeedRootUser: %v", err)
	}
	record, err := writer.recordFor("alice")
	if err != nil {
		t.Fatalf("recordFor: %v", err)
	}
	if record.Credentials.PepperKID != key1.ID {
		t.Fatalf("record kid = %s, want %s", record.Credentials.PepperKID, key1.ID)
	}

	// Rotate: a new current key, the old one still inside its window.
	key2 := newSigningKey([]byte("second-key-material-32-bytes!!!"))
	ring.add(key2, time.Now())
	if _, ok := ring.KeyByID(key1.ID); !ok {
		t.Fatal("previous kid must validate inside the window")
	}

	// Close the window. The record still names key1, so pruning must keep it.
	ring.mu.Lock()
	ring.window = 0
	ring.mu.Unlock()
	if ok, err := writer.VerifyUser("alice", "correct-horse-battery"); err != nil || !ok {
		t.Fatalf("VerifyUser after rotation = %v, %v, want true, nil", ok, err)
	}
	if _, ok := ring.KeyByID(key1.ID); !ok {
		t.Fatal("a kid a stored credential names must survive the window closing")
	}
}
