package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/sealed"
)

// errInterruptedWrite stands in for a crash between a credential file's
// temporary and its rename.
var errInterruptedWrite = errors.New("interrupted before the rename")

// interruptCredentialWrite makes the credential write to the file named rel
// stop after writing its temporary, as a crash would, until the returned
// function (or the end of the test) restores the real writer.
func interruptCredentialWrite(t *testing.T, rel string) (restore func()) {
	t.Helper()
	original := writeCredentialFile
	writeCredentialFile = func(path string, contents []byte) error {
		if strings.HasSuffix(filepath.ToSlash(path), rel) {
			if err := os.WriteFile(path+".tmp", contents[:len(contents)/2], 0o600); err != nil {
				return err
			}
			return errInterruptedWrite
		}
		return original(path, contents)
	}
	restore = func() { writeCredentialFile = original }
	t.Cleanup(restore)
	return restore
}

// TestASealInterruptedAtEachFileIsFinishedByTheNextStart stops the in-place
// seal at each credential file in turn, after its temporary is written and
// before the rename. The start fails; the files already sealed stay sealed,
// the interrupted one is still whole, and the next start seals the rest with
// the same credentials.
func TestASealInterruptedAtEachFileIsFinishedByTheNextStart(t *testing.T) {
	for _, file := range credentialFiles {
		t.Run(file.relPath, func(t *testing.T) {
			dataDir := t.TempDir()
			original, fingerprint := plaintextCredentialDir(t, dataDir)
			seal := sealEdit(t, "require")
			restoreWriter := interruptCredentialWrite(t, file.relPath)

			cfg := sealedTestConfigAt(t, dataDir, seal)
			if refused, err := NewService(cfg, discardLogger()); err == nil {
				_ = refused.Stop()
				t.Fatalf("the start went on past an interrupted write of %s", file.relPath)
			} else if !errors.Is(err, errInterruptedWrite) {
				t.Fatalf("the start failed for another reason: %v", err)
			}
			if _, err := os.Stat(file.path(dataDir)); err != nil {
				t.Fatalf("%s is gone after the interrupted seal: %v", file.relPath, err)
			}
			restoreWriter()

			qs := newCapacityServiceAt(t, dataDir, seal)
			assertSameCredentials(t, qs, original, fingerprint)
			if err := qs.Stop(); err != nil {
				t.Fatalf("Stop: %v", err)
			}
			assertCredentialFilesSealed(t, dataDir, original)
		})
	}
}

func sealedTestConfigAt(t *testing.T, dataDir string, edit func(*config.Config)) *config.Config {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dataDir
	cfg.Metrics.Enabled = false
	cfg.Audit.Enabled = false
	edit(cfg)
	return cfg
}

// TestAMissingNodeMasterBesideSealedCredentialsIsRefused: a data directory
// whose only sealed files are credential files still proves a node master
// existed, so a missing one is refused rather than minted.
func TestAMissingNodeMasterBesideSealedCredentialsIsRefused(t *testing.T) {
	dataDir := t.TempDir()
	seal := sealEdit(t, "require")
	first := newCapacityServiceAt(t, dataDir, seal)
	if err := first.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	for _, rel := range []string{"store", "wal", "users.json", filepath.Join("keys", nodeStoreKeyFileName), filepath.Join("keys", NodeMasterFileName)} {
		if err := os.RemoveAll(filepath.Join(dataDir, rel)); err != nil {
			t.Fatalf("removing %s: %v", rel, err)
		}
	}
	ks, err := OpenKeystore(sealedTestConfigAt(t, dataDir, seal))
	if err != nil {
		t.Fatalf("OpenKeystore: %v", err)
	}
	_, err = LoadOrCreateNodeMaster(dataDir, ks)
	if err == nil || !strings.Contains(err.Error(), "auth.token.key (sealed under the node master)") {
		t.Fatalf("a missing node master beside sealed credential files answered %v, want a refusal naming them", err)
	}
	if _, statErr := os.Stat(filepath.Join(dataDir, "keys", NodeMasterFileName)); !os.IsNotExist(statErr) {
		t.Fatalf("a node master was minted beside sealed credential files")
	}
}

// TestImportRefusesAMasterThatDoesNotOpenTheCredentialFiles: -import-key
// checks an imported node master against the sealed credential files too.
func TestImportRefusesAMasterThatDoesNotOpenTheCredentialFiles(t *testing.T) {
	dataDir := t.TempDir()
	master := bytes.Repeat([]byte{0x31}, 32)
	keys := CredentialKeys{nodeMaster: &NodeMaster{bytes: master, kid: sealed.Kid(master)}, seal: true}
	if err := keys.SaveClusterToken(dataDir, ClusterToken{Token: knownClusterToken(0x44), CreatedAt: time.Now()}); err != nil {
		t.Fatalf("saving a sealed token: %v", err)
	}
	if err := verifyImportedMaster(dataDir, master); err != nil {
		t.Fatalf("the master that sealed the token was refused: %v", err)
	}
	other := bytes.Repeat([]byte{0x32}, 32)
	err := verifyImportedMaster(dataDir, other)
	if err == nil || !strings.Contains(err.Error(), "cluster.token") {
		t.Fatalf("a master that does not open cluster.token answered %v, want a refusal naming it", err)
	}
}

// TestASealedTokenIsResealedAtTheNextGeneration: each save of a sealed
// cluster token is sealed at the generation after the one it replaces (spec
// §2.3), and a node without a keystore refuses the sealed file, naming the
// settings that open it.
func TestASealedTokenIsResealedAtTheNextGeneration(t *testing.T) {
	dataDir := t.TempDir()
	master := bytes.Repeat([]byte{0x52}, 32)
	keys := CredentialKeys{nodeMaster: &NodeMaster{bytes: master, kid: sealed.Kid(master)}, seal: true}
	for wantGen := uint64(1); wantGen <= 3; wantGen++ {
		token, err := GenerateClusterToken()
		if err != nil {
			t.Fatalf("generating a token: %v", err)
		}
		if err := keys.SaveClusterToken(dataDir, ClusterToken{Token: token, CreatedAt: time.Now()}); err != nil {
			t.Fatalf("saving the token: %v", err)
		}
		contents, err := os.ReadFile(filepath.Join(dataDir, clusterTokenFileName))
		if err != nil {
			t.Fatalf("reading the token: %v", err)
		}
		if gen := binary.BigEndian.Uint64(contents[14:22]); gen != wantGen {
			t.Fatalf("save %d sealed at generation %d", wantGen, gen)
		}
	}
	_, err := LoadClusterToken(dataDir)
	if err == nil || !strings.Contains(err.Error(), "HOARDDB_ENCRYPTION_KEYSTORE") {
		t.Fatalf("a sealed token read without a keystore answered %v, want a refusal naming the settings", err)
	}
}

// TestACredentialFileIsReplacedByRenameNotRewritten pins the production
// writer: a credential write replaces the file's directory entry, so a hard
// link to the old file still holds the old content (an in-place rewrite
// would change it), leaves no temporary, and a write that fails leaves the
// old content in place.
func TestACredentialFileIsReplacedByRenameNotRewritten(t *testing.T) {
	dataDir := t.TempDir()
	path := filepath.Join(dataDir, clusterTokenFileName)
	first := ClusterToken{Token: knownClusterToken(0x61), CreatedAt: time.Now()}
	if err := SaveClusterToken(dataDir, first); err != nil {
		t.Fatalf("first save: %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the token: %v", err)
	}
	linkPath := filepath.Join(t.TempDir(), "old-token")
	if err := os.Link(path, linkPath); err != nil {
		t.Skipf("hard links are not available here: %v", err)
	}
	if err := SaveClusterToken(dataDir, ClusterToken{Token: knownClusterToken(0x62), CreatedAt: time.Now()}); err != nil {
		t.Fatalf("second save: %v", err)
	}
	linked, err := os.ReadFile(linkPath)
	if err != nil || !bytes.Equal(linked, before) {
		t.Fatalf("the second save rewrote the old file in place instead of replacing it (read error %v)", err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("the save left %s.tmp behind (stat: %v)", path, err)
	}

	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the token: %v", err)
	}
	// A directory where the temporary goes makes the write fail before the
	// rename; the file must still hold what it held.
	if err := os.Mkdir(path+".tmp", 0o700); err != nil {
		t.Fatalf("blocking the temporary: %v", err)
	}
	if err := SaveClusterToken(dataDir, first); err == nil {
		t.Fatalf("a save whose temporary cannot be written succeeded")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, current) {
		t.Fatalf("a failed save changed the token file (read error %v)", err)
	}
}

// TestASealedFileWithADamagedHeaderIsRefusedNotRewrapped: a sealed credential
// file whose magic or version bytes are damaged no longer looks sealed. It
// does not parse as the plaintext form either, so the start refuses it,
// naming the file, and leaves it as it was instead of sealing the damaged
// bytes again.
func TestASealedFileWithADamagedHeaderIsRefusedNotRewrapped(t *testing.T) {
	for _, file := range credentialFiles {
		t.Run(file.relPath, func(t *testing.T) {
			dataDir := t.TempDir()
			plaintextCredentialDir(t, dataDir)
			seal := sealEdit(t, "require")
			first := newCapacityServiceAt(t, dataDir, seal)
			if err := first.Stop(); err != nil {
				t.Fatalf("Stop: %v", err)
			}
			path := file.path(dataDir)
			damaged, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading %s: %v", path, err)
			}
			damaged[1] ^= 0xFF
			if err := os.WriteFile(path, damaged, 0o600); err != nil {
				t.Fatalf("damaging %s: %v", path, err)
			}
			refused, err := NewService(sealedTestConfigAt(t, dataDir, seal), discardLogger())
			if err == nil {
				_ = refused.Stop()
				t.Fatalf("a node started with a damaged %s", file.relPath)
			}
			if !strings.Contains(err.Error(), path) {
				t.Errorf("the refusal does not name %s: %v", path, err)
			}
			after, readErr := os.ReadFile(path)
			if readErr != nil || !bytes.Equal(after, damaged) {
				t.Errorf("the refused start rewrote the damaged %s (read error %v)", file.relPath, readErr)
			}
		})
	}
}

// TestACopiedSealedTokenRefusalGivesOnlyTheFixThatApplies: a cluster.token
// sealed under another node master is refused with the copy's fix, not with
// advice about file permissions.
func TestACopiedSealedTokenRefusalGivesOnlyTheFixThatApplies(t *testing.T) {
	dataDir := t.TempDir()
	donor := bytes.Repeat([]byte{0x71}, 32)
	donorKeys := CredentialKeys{nodeMaster: &NodeMaster{bytes: donor, kid: sealed.Kid(donor)}, seal: true}
	if err := donorKeys.SaveClusterToken(dataDir, ClusterToken{Token: knownClusterToken(0x72), CreatedAt: time.Now()}); err != nil {
		t.Fatalf("saving the donor's token: %v", err)
	}
	own := bytes.Repeat([]byte{0x73}, 32)
	keys := CredentialKeys{nodeMaster: &NodeMaster{bytes: own, kid: sealed.Kid(own)}, seal: true}
	_, _, err := keys.InitClusterToken(dataDir)
	if err == nil {
		t.Fatalf("a token sealed under another node master was accepted")
	}
	if strings.Contains(err.Error(), "chmod") {
		t.Errorf("the refusal gives permission advice that does not apply: %v", err)
	}
	if !strings.Contains(err.Error(), "move cluster.token aside") {
		t.Errorf("the refusal does not give the fix for a copied token: %v", err)
	}
}

// TestASealedCredentialWithoutAKeystoreNamesTheRoutesThatApply: a start with no
// keystore cannot open a sealed credential file. Its refusal always names the
// settings that open it, and for cluster.token also the move-aside route,
// which a start can take because nothing uses the token in the file. A sealed
// auth.token.key has no such alternative and must not be offered one.
func TestASealedCredentialWithoutAKeystoreNamesTheRoutesThatApply(t *testing.T) {
	dataDir := t.TempDir()
	master := bytes.Repeat([]byte{0x51}, 32)
	sealing := CredentialKeys{nodeMaster: &NodeMaster{bytes: master, kid: sealed.Kid(master)}, seal: true}
	if err := sealing.SaveClusterToken(dataDir, ClusterToken{Token: knownClusterToken(0x52), CreatedAt: time.Now()}); err != nil {
		t.Fatalf("sealing cluster.token: %v", err)
	}
	if err := sealing.writeCredential(dataDir, authKeyCredential, bytes.Repeat([]byte{0x53}, authKeyLen)); err != nil {
		t.Fatalf("sealing auth.token.key: %v", err)
	}

	for _, tc := range []struct {
		file      credentialFile
		moveAside bool
	}{
		{clusterTokenCredential, true},
		{authKeyCredential, false},
	} {
		t.Run(tc.file.relPath, func(t *testing.T) {
			_, _, err := (CredentialKeys{}).readCredential(dataDir, tc.file)
			if err == nil {
				t.Fatalf("a sealed %s was read with no keystore", tc.file.relPath)
			}
			if !strings.Contains(err.Error(), "HOARDDB_ENCRYPTION_KEYSTORE") {
				t.Errorf("the refusal does not name the settings that open it: %v", err)
			}
			if named := strings.Contains(err.Error(), "move cluster.token aside"); named != tc.moveAside {
				t.Errorf("move-aside advice named: %v, want %v: %v", named, tc.moveAside, err)
			}
			if tc.moveAside {
				for _, want := range []string{"move cluster.token aside", "nothing is lost without it"} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("the move-aside route does not name %q: %v", want, err)
					}
				}
			}
		})
	}
}
