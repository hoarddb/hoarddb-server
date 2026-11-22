package server

import (
	"bytes"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/clusterpepper"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/sealed"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The credential files of an encrypted node: the standalone signing key
// (auth.token.key), the cluster token (cluster.token), the TLS private key
// (keys/server.key) and the ring cluster's password key (auth.cluster.pepper).
// With encryption at rest on, each is sealed under the node master, whose key
// is not in the data directory, so a copy of the directory alone does not hold
// them in a usable form.

// Purpose bytes the credential envelopes carry (at-rest-encryption spec §2.3).
// They are spelled out here, not taken from the sealed package, so the test
// pins the on-disk value.
const (
	authKeyPurposeByte      = 0x09
	clusterTokenPurposeByte = 0x0A
	tlsKeyPurposeByte       = 0x0B
)

// credentialSecrets is what a node's credential files hold in the clear when
// they are not sealed.
type credentialSecrets struct {
	authKey      []byte
	clusterToken string
	tlsSeed      []byte
	// clusterPepper is auth.cluster.pepper's key when the directory holds one; nil when it does
	// not (a node outside a cluster never writes it).
	clusterPepper []byte
}

// liveCredentialSecrets reads the secrets out of a running service's memory,
// which is the only place a test can learn them once the files are sealed.
func liveCredentialSecrets(t *testing.T, qs *Service) credentialSecrets {
	t.Helper()
	token, err := qs.core.RequestClusterToken()
	if err != nil {
		t.Fatalf("reading the node's cluster token: %v", err)
	}
	privateKey, ok := qs.tlsCfg.Certificates[0].PrivateKey.(ed25519.PrivateKey)
	if !ok {
		t.Fatalf("the node's TLS key is %T, want ed25519", qs.tlsCfg.Certificates[0].PrivateKey)
	}
	return credentialSecrets{
		authKey:      append([]byte(nil), qs.core.auth.keyring.Current().Bytes...),
		clusterToken: token,
		tlsSeed:      privateKey.Seed(),
	}
}

// assertSealedCredential fails unless the file at path opens with the sealed
// envelope header for purpose and none of the secrets appears in it, raw, as
// hex or as base64.
func assertSealedCredential(t *testing.T, path string, purpose byte, secrets ...[]byte) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	for _, secret := range secrets {
		for _, form := range [][]byte{secret, []byte(hex.EncodeToString(secret)), []byte(base64.StdEncoding.EncodeToString(secret))} {
			if bytes.Contains(contents, form) {
				t.Errorf("%s holds a credential in the clear on an encrypted node", path)
			}
		}
	}
	if bytes.Contains(contents, []byte("PRIVATE KEY")) {
		t.Errorf("%s is a plaintext PEM private key on an encrypted node", path)
	}
	if len(contents) < sealed.EnvelopeHeaderLen || binary.BigEndian.Uint32(contents[0:4]) != sealed.Magic ||
		contents[4] != sealed.Version || contents[5] != purpose {
		t.Errorf("%s does not start with a sealed envelope of purpose %#02x (first bytes %x)", path, purpose, firstBytes(contents, 6))
	}
}

func firstBytes(contents []byte, count int) []byte {
	if len(contents) < count {
		return contents
	}
	return contents[:count]
}

// assertNoTemporaries fails if a credential write left a temporary file.
func assertNoTemporaries(t *testing.T, dataDir string) {
	t.Helper()
	for _, rel := range []string{"auth.token.key.tmp", "cluster.token.tmp", filepath.Join("keys", "server.key.tmp"), clusterpepper.FileName + ".tmp"} {
		if _, err := os.Stat(filepath.Join(dataDir, rel)); err == nil {
			t.Errorf("%s was left in the data directory", rel)
		}
	}
}

func assertCredentialFilesSealed(t *testing.T, dataDir string, secrets credentialSecrets) {
	t.Helper()
	assertSealedCredential(t, filepath.Join(dataDir, "auth.token.key"), authKeyPurposeByte, secrets.authKey)
	assertSealedCredential(t, filepath.Join(dataDir, "cluster.token"), clusterTokenPurposeByte, []byte(secrets.clusterToken))
	assertSealedCredential(t, filepath.Join(dataDir, "keys", "server.key"), tlsKeyPurposeByte, secrets.tlsSeed)
	if secrets.clusterPepper != nil {
		assertSealedCredential(t, filepath.Join(dataDir, clusterpepper.FileName), clusterPepperPurposeByte, secrets.clusterPepper)
	}
	assertNoTemporaries(t, dataDir)
}

func knownClusterToken(fill byte) string {
	return hex.EncodeToString(bytes.Repeat([]byte{fill}, 32))
}

// TestEncryptedNodeKeepsCredentialFilesSealed: a node started with encryption
// at rest writes its signing key and TLS key sealed, and seals the cluster
// token the first start found beside them.
func TestEncryptedNodeKeepsCredentialFilesSealed(t *testing.T) {
	dataDir := t.TempDir()
	token := knownClusterToken(0x3C)
	if err := SaveClusterToken(dataDir, ClusterToken{Token: token, CreatedAt: time.Now()}); err != nil {
		t.Fatalf("saving the cluster token: %v", err)
	}
	qs := newCapacityServiceAt(t, dataDir, sealEdit(t, "require"))
	secrets := liveCredentialSecrets(t, qs)
	if secrets.clusterToken != token {
		t.Fatalf("the node reads cluster token %q, want the one it was given", secrets.clusterToken)
	}
	if err := qs.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	assertCredentialFilesSealed(t, dataDir, secrets)
}

// plaintextCredentialDir writes the four credential files the way a node
// without sealed credentials left them, and returns what they hold.
func plaintextCredentialDir(t *testing.T, dataDir string) (credentialSecrets, string) {
	t.Helper()
	authKey := bytes.Repeat([]byte{0x5A}, 32)
	if err := os.WriteFile(filepath.Join(dataDir, "auth.token.key"), authKey, 0o600); err != nil {
		t.Fatalf("writing auth.token.key: %v", err)
	}
	token := knownClusterToken(0x6E)
	if err := SaveClusterToken(dataDir, ClusterToken{Token: token, CreatedAt: time.Now()}); err != nil {
		t.Fatalf("saving the cluster token: %v", err)
	}
	paths, fingerprint, err := transport.GenerateEd25519Keys(filepath.Join(dataDir, "keys"))
	if err != nil {
		t.Fatalf("generating TLS keys: %v", err)
	}
	keyPEM, err := os.ReadFile(paths.Key)
	if err != nil {
		t.Fatalf("reading the TLS key: %v", err)
	}
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		t.Fatalf("the generated TLS key is not PEM")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		t.Fatalf("parsing the generated TLS key: %v", err)
	}
	pepper, err := clusterpepper.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if err := newClusterPepperFile(CredentialKeys{}, dataDir).Store(pepper); err != nil {
		t.Fatalf("writing auth.cluster.pepper: %v", err)
	}
	return credentialSecrets{authKey: authKey, clusterToken: token, tlsSeed: parsed.(ed25519.PrivateKey).Seed(), clusterPepper: pepper.Bytes()}, fingerprint
}

// assertSameCredentials fails unless the running node uses the credentials the
// plaintext files held: sealing them must not invalidate an outstanding login
// token, change the cluster secret, or change the node's TLS identity.
func assertSameCredentials(t *testing.T, qs *Service, want credentialSecrets, wantFingerprint string) {
	t.Helper()
	if got, wantKid := qs.core.auth.keyring.Current().ID, newSigningKey(want.authKey).ID; got != wantKid {
		t.Errorf("the node signs with key %s, want %s from the original auth.token.key", got, wantKid)
	}
	if token, err := qs.core.RequestClusterToken(); err != nil || token != want.clusterToken {
		t.Errorf("the node reads cluster token %q (err %v), want the original", token, err)
	}
	if qs.core.fingerprint != wantFingerprint {
		t.Errorf("the node's TLS fingerprint is %s, want the original %s", qs.core.fingerprint, wantFingerprint)
	}
}

// TestEncryptedNodeSealsPlaintextCredentialFilesInPlace: an encrypted node
// whose credential files are plaintext (written before they were sealed) seals
// each in place on start, keeping the same key, token and identity, and reads
// the sealed form on the next start.
func TestEncryptedNodeSealsPlaintextCredentialFilesInPlace(t *testing.T) {
	dataDir := t.TempDir()
	original, fingerprint := plaintextCredentialDir(t, dataDir)
	seal := sealEdit(t, "require")

	first := newCapacityServiceAt(t, dataDir, seal)
	assertSameCredentials(t, first, original, fingerprint)
	if err := first.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	assertCredentialFilesSealed(t, dataDir, original)

	second := newCapacityServiceAt(t, dataDir, seal)
	assertSameCredentials(t, second, original, fingerprint)
	if err := second.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	assertCredentialFilesSealed(t, dataDir, original)
}

// TestEncryptedNodeRecoversFromAnInterruptedCredentialSeal: a start that
// stopped while it sealed a credential file leaves the plaintext file and a
// temporary beside it. The next start seals the file, removes the temporary,
// and keeps the credentials.
func TestEncryptedNodeRecoversFromAnInterruptedCredentialSeal(t *testing.T) {
	for _, tc := range []struct {
		name      string
		temporary func(plaintext []byte) []byte
	}{
		{"temporary cut short", func(plaintext []byte) []byte { return plaintext[:len(plaintext)/2] }},
		{"temporary complete", func(plaintext []byte) []byte { return plaintext }},
		{"temporary empty", func([]byte) []byte { return nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			original, fingerprint := plaintextCredentialDir(t, dataDir)
			for _, rel := range []string{"auth.token.key", "cluster.token", filepath.Join("keys", "server.key"), clusterpepper.FileName} {
				plaintext, err := os.ReadFile(filepath.Join(dataDir, rel))
				if err != nil {
					t.Fatalf("reading %s: %v", rel, err)
				}
				if err := os.WriteFile(filepath.Join(dataDir, rel+".tmp"), tc.temporary(plaintext), 0o600); err != nil {
					t.Fatalf("writing %s.tmp: %v", rel, err)
				}
			}
			qs := newCapacityServiceAt(t, dataDir, sealEdit(t, "require"))
			assertSameCredentials(t, qs, original, fingerprint)
			if err := qs.Stop(); err != nil {
				t.Fatalf("Stop: %v", err)
			}
			assertCredentialFilesSealed(t, dataDir, original)
		})
	}
}

// TestEncryptedNodeRemovesAPlaintextTemporaryBesideASealedCredential: a
// plaintext temporary left beside a credential file that is already sealed is
// removed on start, so no copy of the credential stays in the clear.
func TestEncryptedNodeRemovesAPlaintextTemporaryBesideASealedCredential(t *testing.T) {
	dataDir := t.TempDir()
	original, fingerprint := plaintextCredentialDir(t, dataDir)
	seal := sealEdit(t, "require")
	first := newCapacityServiceAt(t, dataDir, seal)
	if err := first.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "auth.token.key.tmp"), original.authKey, 0o600); err != nil {
		t.Fatalf("writing the stale temporary: %v", err)
	}
	second := newCapacityServiceAt(t, dataDir, seal)
	assertSameCredentials(t, second, original, fingerprint)
	if err := second.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	assertCredentialFilesSealed(t, dataDir, original)
}

// TestEncryptedNodeRefusesACredentialFileItsNodeMasterDoesNotOpen: a sealed
// credential file copied from another node does not open under this node's
// master. The start is refused, naming the file and the fix, and the file is
// left as it was: no new key is minted over it.
func TestEncryptedNodeRefusesACredentialFileItsNodeMasterDoesNotOpen(t *testing.T) {
	for _, rel := range []string{"auth.token.key", "cluster.token", filepath.Join("keys", "server.key"), clusterpepper.FileName} {
		t.Run(rel, func(t *testing.T) {
			donorDir, nodeDir := t.TempDir(), t.TempDir()
			if err := SaveClusterToken(donorDir, ClusterToken{Token: knownClusterToken(0x2E), CreatedAt: time.Now()}); err != nil {
				t.Fatalf("saving the donor's cluster token: %v", err)
			}
			// A node outside a cluster never writes the pepper, so the donor is given one to seal.
			donorPepper, err := clusterpepper.Generate()
			if err != nil {
				t.Fatal(err)
			}
			if err := newClusterPepperFile(CredentialKeys{}, donorDir).Store(donorPepper); err != nil {
				t.Fatalf("writing the donor's auth.cluster.pepper: %v", err)
			}
			donor := newCapacityServiceAt(t, donorDir, sealEdit(t, "require"))
			if err := donor.Stop(); err != nil {
				t.Fatalf("Stop: %v", err)
			}
			seal := sealEditWithKEK(t, "require", bytes.Repeat([]byte{0x2D}, 32))
			node := newCapacityServiceAt(t, nodeDir, seal)
			if err := node.Stop(); err != nil {
				t.Fatalf("Stop: %v", err)
			}
			copied, err := os.ReadFile(filepath.Join(donorDir, rel))
			if err != nil {
				t.Fatalf("reading the donor's %s: %v", rel, err)
			}
			if err := os.WriteFile(filepath.Join(nodeDir, rel), copied, 0o600); err != nil {
				t.Fatalf("copying %s: %v", rel, err)
			}

			cfg := config.DefaultConfig()
			cfg.Storage.DataDir = nodeDir
			cfg.Metrics.Enabled = false
			cfg.Audit.Enabled = false
			seal(cfg)
			refused, err := NewService(cfg, discardLogger())
			if err == nil {
				_ = refused.Stop()
				t.Fatalf("a node started with a %s sealed under another node master", rel)
			}
			for _, want := range []string{filepath.Join(nodeDir, rel), "node master"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal does not name %q: %v", want, err)
				}
			}
			after, readErr := os.ReadFile(filepath.Join(nodeDir, rel))
			if readErr != nil || !bytes.Equal(after, copied) {
				t.Errorf("the refused start changed %s (read error %v)", rel, readErr)
			}
		})
	}
}

// sealEditWithKEK is sealEdit with a key-encryption key of the test's choice,
// so two nodes of one test have different node masters.
func sealEditWithKEK(t *testing.T, mode string, kek []byte) func(*config.Config) {
	t.Helper()
	keyPath := filepath.Join(t.TempDir(), "kek")
	if err := os.WriteFile(keyPath, kek, 0o600); err != nil {
		t.Fatalf("writing the test key file: %v", err)
	}
	return func(cfg *config.Config) {
		cfg.Encryption.Keystore = "keyfile"
		cfg.Encryption.KeyFile = keyPath
		cfg.Encryption.Mode = mode
	}
}

// TestEncryptedClusterKeepsCredentialFilesSealed: two encrypted nodes form a
// cluster, replicate a write and serve it from either node; afterwards each
// node's cluster token and TLS key are sealed, and the nodes, restarted on
// those files, rejoin and serve a new write.
func TestEncryptedClusterKeepsCredentialFilesSealed(t *testing.T) {
	tc := newMixedCluster(t, 2, "require", "require")
	tc.waitReady()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k1", Data: map[string]interface{}{"v": "sealed-cluster"}})
	waitReadOnEveryNode(t, tc, "k1")

	seeds := make([][]byte, len(tc.nodes))
	for i, node := range tc.nodes {
		seeds[i] = node.server.tlsCfg.Certificates[0].PrivateKey.(ed25519.PrivateKey).Seed()
	}
	for i := range tc.nodes {
		tc.restartNode(i)
	}
	tc.waitReady()
	tc.ok(1, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k2", Data: map[string]interface{}{"v": "after-restart"}})
	waitReadOnEveryNode(t, tc, "k2")
	tc.stop()
	for i, node := range tc.nodes {
		dataDir := node.cfg.Storage.DataDir
		assertSealedCredential(t, filepath.Join(dataDir, "cluster.token"), clusterTokenPurposeByte, []byte(tc.token))
		assertSealedCredential(t, filepath.Join(dataDir, "keys", "server.key"), tlsKeyPurposeByte, seeds[i])
	}
}

// waitReadOnEveryNode waits until every node's own copy answers key.
func waitReadOnEveryNode(t *testing.T, tc *testCluster, key string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for i := range tc.nodes {
		for {
			found, _ := tc.readFromNode(i, key)
			if found {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d does not serve %s", i, key)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// TestAnEncryptedStartSealsThePepperInPlaceAndLeavesItsWrongCopiesAlone: [GUARD a plaintext
// auth.cluster.pepper (0600, HDBPEP1 magic) left by a node at encryption.mode off is sealed in
// place under purpose 0x0C by the first start with the mode on, keeps its pepper, opens with this
// node's master, and is refused with its restore text on a node whose master did not seal it; red
// when the pepper's row leaves credentialFiles]. A moved-aside auth.cluster.pepper.wrong-<kid> is
// not a credential file and is not sealed: it stays as it was written (documented in the
// encryption user page and the ring encryption spec's credential table).
func TestAnEncryptedStartSealsThePepperInPlaceAndLeavesItsWrongCopiesAlone(t *testing.T) {
	dataDir := t.TempDir()
	original, fingerprint := plaintextCredentialDir(t, dataDir)
	path := filepath.Join(dataDir, clusterpepper.FileName)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("the plaintext pepper file is mode %v, want 0600", info.Mode().Perm())
	}
	if contents, err := os.ReadFile(path); err != nil || !bytes.HasPrefix(contents, []byte(clusterpepper.FileMagic)) {
		t.Fatalf("the plaintext pepper file starts %q (%v), want the magic %q", firstBytes(contents, 8), err, clusterpepper.FileMagic)
	}
	wrongPepper, err := clusterpepper.Generate()
	if err != nil {
		t.Fatal(err)
	}
	wrongPath := path + ".wrong-" + wrongPepper.KID()
	wrongContents := clusterpepper.EncodeFile(wrongPepper)
	if err := os.WriteFile(wrongPath, wrongContents, 0o600); err != nil {
		t.Fatalf("writing %s: %v", wrongPath, err)
	}

	seal := sealEdit(t, "on")
	qs := newCapacityServiceAt(t, dataDir, seal)
	assertSameCredentials(t, qs, original, fingerprint)
	if err := qs.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	assertCredentialFilesSealed(t, dataDir, original)

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dataDir
	seal(cfg)
	keys, err := OpenCredentialKeys(cfg)
	if err != nil {
		t.Fatalf("OpenCredentialKeys: %v", err)
	}
	want, err := clusterpepper.FromBytes(original.clusterPepper)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := newClusterPepperFile(keys, dataDir).Load(); err != nil || !got.Equal(want) {
		t.Fatalf("the pepper sealed in place loads as %v, %v; want %v", got, err, want)
	}
	if after, err := os.ReadFile(wrongPath); err != nil || !bytes.Equal(after, wrongContents) {
		t.Fatalf("the start changed %s (read error %v): a wrong copy is not a credential file and is not sealed", wrongPath, err)
	}

	otherDir := t.TempDir()
	other := sealEditWithKEK(t, "on", bytes.Repeat([]byte{0x2D}, 32))
	node := newCapacityServiceAt(t, otherDir, other)
	if err := node.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	copied, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	copiedPath := filepath.Join(otherDir, clusterpepper.FileName)
	if err := os.WriteFile(copiedPath, copied, 0o600); err != nil {
		t.Fatalf("copying the sealed pepper: %v", err)
	}
	otherCfg := config.DefaultConfig()
	otherCfg.Storage.DataDir = otherDir
	otherCfg.Metrics.Enabled = false
	otherCfg.Audit.Enabled = false
	other(otherCfg)
	refused, err := NewService(otherCfg, discardLogger())
	if err == nil {
		_ = refused.Stop()
		t.Fatal("a node started with an auth.cluster.pepper another node master sealed")
	}
	for _, want := range []string{copiedPath, "sealed under node master kid", clusterPepperCredential.restore} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}
	if after, err := os.ReadFile(copiedPath); err != nil || !bytes.Equal(after, copied) {
		t.Errorf("the refused start changed %s (read error %v)", copiedPath, err)
	}
}
