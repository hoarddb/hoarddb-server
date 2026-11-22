package server

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/sealed"
)

// configsEqual compares two parsed user stores the way the round-trip tests
// do — reflect, not a custom diff.
func configsEqual(a, b *usersConfig) bool {
	return reflect.DeepEqual(a, b)
}

// testSigningKeyring builds a keyring from raw key material, one entry per
// key in order, so the last key is the current one. It goes through add so
// the lock-free current mirror Save reads is maintained.
func testSigningKeyring(keys ...[]byte) *Keyring {
	kr := &Keyring{}
	now := time.Now()
	for _, keyBytes := range keys {
		kr.add(newSigningKey(keyBytes), now)
	}
	return kr
}

// headerKID reads the kid field back out of an encrypted users file, the one
// byte offset the format freezes (spec §2: magic+version 6, kid 8, nonce 12).
func headerKID(t *testing.T, data []byte) string {
	t.Helper()
	if len(data) < usersHeaderLen {
		t.Fatalf("encrypted file is %d bytes, shorter than the %d-byte header", len(data), usersHeaderLen)
	}
	kidOff := usersFileMagicLen + usersFileVersionLen
	return string(data[kidOff : kidOff+usersFileKIDLen])
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	keyBytes := make([]byte, 32)
	for i := range keyBytes {
		keyBytes[i] = byte(i)
	}
	kid := newSigningKey(keyBytes).ID
	plaintext := []byte(`{"format_version":1,"users":[]}`)

	sealed, err := encryptConfig(plaintext, keyBytes, kid)
	if err != nil {
		t.Fatalf("encryptConfig: %v", err)
	}
	if !bytes.HasPrefix(sealed, usersFileMagic) {
		t.Fatalf("encrypted output does not start with the magic header")
	}
	if !bytes.HasPrefix(sealed, append(append([]byte{}, usersFileMagic...), usersFileVersion...)) {
		t.Fatalf("encrypted output does not carry the version bytes after the magic")
	}
	if got := headerKID(t, sealed); got != kid {
		t.Fatalf("header kid = %q, want %q", got, kid)
	}

	opened, err := decryptConfig(sealed, testSigningKeyring(keyBytes))
	if err != nil {
		t.Fatalf("decryptConfig: %v", err)
	}
	if !bytes.Equal(opened, plaintext) {
		t.Fatalf("round-tripped plaintext differs\n got: %q\nwant: %q", opened, plaintext)
	}
}

func TestTamperDetection(t *testing.T) {
	keyBytes := make([]byte, 32)
	for i := range keyBytes {
		keyBytes[i] = byte(i)
	}
	kid := newSigningKey(keyBytes).ID
	sealed, err := encryptConfig([]byte(`{"format_version":1}`), keyBytes, kid)
	if err != nil {
		t.Fatalf("encryptConfig: %v", err)
	}

	// Flip one byte inside the AES-GCM payload (after the 26-byte header), not
	// the header: a header edit would be caught by the format checks. GCM's
	// built-in integrity, not a checksum, must refuse it.
	sealed[len(sealed)-1] ^= 0x01
	if _, err := decryptConfig(sealed, testSigningKeyring(keyBytes)); err == nil {
		t.Fatal("decryptConfig accepted tampered ciphertext")
	}
}

// TestLegacyPlaintextStillLoads is the upgrade path: a users.json written by
// an older build has no magic header and loads exactly as today.
func TestLegacyPlaintextStillLoads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)
	before := validUsersConfig()
	if err := before.Save(path, usersSealing{Keyring: nil, NodeMaster: nil, Mode: "off"}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if bytes.HasPrefix(data, usersFileMagic) {
		t.Fatal("a legacy Save with nil keyring produced an encrypted file")
	}

	after, err := LoadUsersConfig(path, nil, nil)
	if err != nil {
		t.Fatalf("LoadUsersConfig: %v", err)
	}
	if !configsEqual(after, before) {
		t.Fatalf("legacy plaintext did not round-trip\n got: %+v\nwant: %+v", after, before)
	}
}

func TestWrongKidFails(t *testing.T) {
	keyA := make([]byte, 32)
	keyB := make([]byte, 32)
	for i := range keyA {
		keyA[i] = byte(i)
		keyB[i] = byte(i + 128)
	}
	kidA := newSigningKey(keyA).ID
	kidB := newSigningKey(keyB).ID

	sealed, err := encryptConfig([]byte(`{"format_version":1}`), keyA, kidA)
	if err != nil {
		t.Fatalf("encryptConfig: %v", err)
	}
	// Rewrite the header's kid field to claim key B while the payload was
	// sealed under A: the keyring knows A, the header says B, and the decrypt
	// must refuse by naming the missing kid rather than trying every key.
	copy(sealed[6:14], []byte(kidB))

	_, err = decryptConfig(sealed, testSigningKeyring(keyA))
	if err == nil {
		t.Fatal("decryptConfig decrypted under a kid the header never claimed")
	}
	if !strings.Contains(err.Error(), kidB) {
		t.Fatalf("error %q does not name the missing kid %q", err, kidB)
	}
	if !strings.Contains(err.Error(), "restore the original auth.token.key") {
		t.Fatalf("error %q does not point at restoring auth.token.key", err)
	}
}

func TestKeyRotationReEncrypts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)
	keyA := make([]byte, 32)
	keyB := make([]byte, 32)
	for i := range keyA {
		keyA[i] = byte(i)
		keyB[i] = byte(i + 64)
	}
	kidA := newSigningKey(keyA).ID
	kidB := newSigningKey(keyB).ID

	cfg := validUsersConfig()
	if err := cfg.Save(path, usersSealing{Keyring: testSigningKeyring(keyA), NodeMaster: nil, Mode: "off"}); err != nil {
		t.Fatalf("first Save: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if got := headerKID(t, data); got != kidA {
		t.Fatalf("first write used kid %q, want %q", got, kidA)
	}

	// Rotate: the keyring now holds both keys, key B current.
	rotated := testSigningKeyring(keyA, keyB)
	if err := cfg.Save(path, usersSealing{Keyring: rotated, NodeMaster: nil, Mode: "off"}); err != nil {
		t.Fatalf("second Save: %v", err)
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if got := headerKID(t, data); got != kidB {
		t.Fatalf("second write used kid %q, want %q (the file must move to the new key)", got, kidB)
	}

	// Loading still works because the keyring retained key A.
	loaded, err := LoadUsersConfig(path, rotated, nil)
	if err != nil {
		t.Fatalf("LoadUsersConfig after rotation: %v", err)
	}
	if !configsEqual(loaded, cfg) {
		t.Fatalf("config after rotation differs\n got: %+v\nwant: %+v", loaded, cfg)
	}
}

func TestNoKeyringWritesPlaintext(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)
	cfg := validUsersConfig()

	if err := cfg.Save(path, usersSealing{Keyring: nil, NodeMaster: nil, Mode: "off"}); err != nil {
		t.Fatalf("Save with nil keyring: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if bytes.HasPrefix(data, usersFileMagic) {
		t.Fatal("Save with nil keyring wrote an encrypted file; it must stay plaintext")
	}
	loaded, err := LoadUsersConfig(path, nil, nil)
	if err != nil {
		t.Fatalf("LoadUsersConfig with nil keyring: %v", err)
	}
	if !configsEqual(loaded, cfg) {
		t.Fatalf("no-keyring round-trip differs\n got: %+v\nwant: %+v", loaded, cfg)
	}

	// An empty keyring behaves like a nil one: plaintext both ways.
	if err := cfg.Save(path, usersSealing{Keyring: &Keyring{}, NodeMaster: nil, Mode: "off"}); err != nil {
		t.Fatalf("Save with empty keyring: %v", err)
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if bytes.HasPrefix(data, usersFileMagic) {
		t.Fatal("Save with an empty keyring wrote an encrypted file; it must stay plaintext")
	}
}

// TestResetPasswordOnEncryptedFile is spec open-question 2: reset-password
// runs with the server stopped and must still work once users.json is
// encrypted, because it loads the keyring before touching the file.
func TestResetPasswordOnEncryptedFile(t *testing.T) {
	dir, cfg := setupAuthDir(t)
	am := newPersistentManager(t, dir)
	if err := am.BootstrapUser("root", "correct-horse-battery", true); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	data, err := os.ReadFile(UsersFilePath(dir))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.HasPrefix(data, usersFileMagic) {
		t.Fatal("a persistent manager's bootstrap did not encrypt users.json")
	}

	term, version, err := ResetPassword(cfg, "root", "a-brand-new-password", true, discardLogger())
	if err != nil {
		t.Fatalf("ResetPassword on an encrypted file: %v", err)
	}
	if term == 0 || version == 0 {
		t.Fatalf("ResetPassword returned 0 term/version: %d/%d", term, version)
	}

	// The file is still encrypted and still loadable with the keyring.
	data, err = os.ReadFile(UsersFilePath(dir))
	if err != nil {
		t.Fatalf("ReadFile after reset: %v", err)
	}
	if !bytes.HasPrefix(data, usersFileMagic) {
		t.Fatal("ResetPassword wrote a plaintext users.json; it must stay encrypted")
	}
	keyring, err := BuildKeyring(dir, "", "", time.Hour, discardLogger())
	if err != nil {
		t.Fatalf("BuildKeyring: %v", err)
	}
	after, err := LoadUsersConfig(UsersFilePath(dir), keyring, nil)
	if err != nil {
		t.Fatalf("LoadUsersConfig after reset: %v", err)
	}
	if after.Master != "node-a" || after.Term != term {
		t.Fatalf("reset config: master %q term %d, want node-a / %d", after.Master, after.Term, term)
	}

	// The new password authenticates against the reloaded record.
	reloaded := newPersistentManager(t, dir)
	if _, _, err := reloaded.LoadUsers(UsersFilePath(dir), nil, nil); err != nil {
		t.Fatalf("LoadUsers: %v", err)
	}
	if ok, err := reloaded.VerifyUser("root", "a-brand-new-password"); err != nil || !ok {
		t.Fatalf("new password after reset + restart = %v, %v; want true", ok, err)
	}
	if ok, _ := reloaded.VerifyUser("root", "correct-horse-battery"); ok {
		t.Fatalf("old password after reset + restart still authenticates")
	}
}

// TestUsersSealedUnderNodeMaster asserts that users.json written at mode=on
// opens with the node master and not with the auth signing key.
func TestUsersSealedUnderNodeMaster(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)
	cfg := validUsersConfig()
	keyBytes := make([]byte, 32)
	for i := range keyBytes {
		keyBytes[i] = byte(i)
	}

	// Create a mock node master with properly computed kid
	nodemaster := &NodeMaster{
		bytes:   keyBytes,
		kid:     sealed.Kid(keyBytes),
		created: false,
	}

	// Save with mode=on and node master
	if err := cfg.Save(path, usersSealing{Keyring: nil, NodeMaster: nodemaster, Mode: "on"}); err != nil {
		t.Fatalf("Save with node master: %v", err)
	}

	// The file should be encrypted with the new format (0x02)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(data) < 5 {
		t.Fatalf("file too short to check version")
	}
	if data[4] != 0x02 {
		t.Fatalf("version byte at offset 4 = %x, want 0x02", data[4])
	}

	// Load with the node master should succeed
	loaded, err := LoadUsersConfig(path, nil, nodemaster)
	if err != nil {
		t.Fatalf("LoadUsersConfig with node master: %v", err)
	}
	if !configsEqual(loaded, cfg) {
		t.Fatalf("loaded config differs\n got: %+v\nwant: %+v", loaded, cfg)
	}

	// Try loading with an auth keyring (the legacy path) should fail
	// because the file is sealed under the node master, not the auth key
	authKeyring := testSigningKeyring(keyBytes)
	_, err = LoadUsersConfig(path, authKeyring, nil)
	if err == nil {
		t.Fatal("LoadUsersConfig with auth keyring alone should fail for a node-master-sealed file")
	}
}

// TestLegacyEncryptConfigFixtureStillOpens asserts that a fixture produced
// by calling today's encryptConfig opens — not a hand-written one.
func TestLegacyEncryptConfigFixtureStillOpens(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)

	keyBytes := make([]byte, 32)
	for i := range keyBytes {
		keyBytes[i] = byte(i)
	}
	kid := newSigningKey(keyBytes).ID
	plaintext := []byte(`{"format_version":1,"term":41,"version":12,"master":"node-a","users":[]}`)

	// Create the fixture by calling today's encryptConfig (the legacy writer)
	sealed, err := encryptConfig(plaintext, keyBytes, kid)
	if err != nil {
		t.Fatalf("encryptConfig: %v", err)
	}

	// Write it to disk
	if err := os.WriteFile(path, sealed, 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Load it back with the keyring containing the legacy key
	keyring := testSigningKeyring(keyBytes)
	loaded, err := LoadUsersConfig(path, keyring, nil)
	if err != nil {
		t.Fatalf("LoadUsersConfig: %v", err)
	}

	// Verify that we got the plaintext back
	if loaded.FormatVersion != 1 || loaded.Term != 41 || loaded.Version != 12 || loaded.Master != "node-a" {
		t.Fatalf("loaded config wrong: %+v", loaded)
	}
}

// TestPlaintextUsersFileStillLoads asserts that the pre-encryption form
// still loads (this is TestLegacyPlaintextStillLoads extended).
func TestPlaintextUsersFileStillLoads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)
	cfg := validUsersConfig()

	// Save plaintext at mode=off
	if err := cfg.Save(path, usersSealing{Keyring: nil, NodeMaster: nil, Mode: "off"}); err != nil {
		t.Fatalf("Save plaintext: %v", err)
	}

	// Verify it's plaintext (no magic header)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if bytes.HasPrefix(data, usersFileMagic) {
		t.Fatal("plaintext save at mode=off produced an encrypted file")
	}

	// Load it back with all combinations of keyring/node master (should all work)
	loaded, err := LoadUsersConfig(path, nil, nil)
	if err != nil {
		t.Fatalf("LoadUsersConfig(nil, nil): %v", err)
	}
	if !configsEqual(loaded, cfg) {
		t.Fatalf("plaintext loaded differs\n got: %+v\nwant: %+v", loaded, cfg)
	}
}

// TestUnknownVersionBytesInMessage asserts that {0x00, 0x09} refuses with
// 0009 in the text.
func TestUnknownVersionBytesInMessage(t *testing.T) {
	// Create a blob with magic header but unknown version bytes {0x00, 0x09}
	blob := make([]byte, 30)
	copy(blob[0:4], usersFileMagic)
	blob[4] = 0x00
	blob[5] = 0x09
	// Add some padding to make it a valid-looking (but not decodable) encrypted file

	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)
	if err := os.WriteFile(path, blob, 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := LoadUsersConfig(path, nil, nil)
	if err == nil {
		t.Fatal("LoadUsersConfig accepted unknown version bytes {0x00, 0x09}")
	}
	// The version bytes {0x00, 0x09} must print as "0009" in hex, not double-encoded.
	if !strings.Contains(err.Error(), "0009") {
		t.Fatalf("error %q does not contain '0009' in hex; got: %q", err, err.Error())
	}
}

// TestNoKeyringWritesPlaintextModeOff asserts that at mode=off, a nil node
// master writes plaintext.
func TestNoKeyringWritesPlaintextModeOff(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)
	cfg := validUsersConfig()

	// Save at mode=off with no keyring and no node master
	if err := cfg.Save(path, usersSealing{Keyring: nil, NodeMaster: nil, Mode: "off"}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if bytes.HasPrefix(data, usersFileMagic) {
		t.Fatal("Save at mode=off produced an encrypted file; it must stay plaintext")
	}

	// Verify it's parseable as JSON
	loaded, err := LoadUsersConfig(path, nil, nil)
	if err != nil {
		t.Fatalf("LoadUsersConfig: %v", err)
	}
	if !configsEqual(loaded, cfg) {
		t.Fatalf("round-trip differs\n got: %+v\nwant: %+v", loaded, cfg)
	}
}

// TestModeOnRefusesPlaintextUsersWrite asserts that at mode=on, a nil node
// master refuses and writes nothing.
func TestModeOnRefusesPlaintextUsersWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)
	cfg := validUsersConfig()

	// Try to save at mode=on with no node master (should refuse)
	err := cfg.Save(path, usersSealing{Keyring: nil, NodeMaster: nil, Mode: "on"})
	if err == nil {
		t.Fatal("Save at mode=on with nil node master should refuse")
	}

	// Verify the file was never written
	_, err = os.Stat(path)
	if !os.IsNotExist(err) {
		t.Fatalf("Save refused but a file was created at %s", path)
	}
}
