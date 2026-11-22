package server

import (
	"bytes"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/clusterpepper"
	"github.com/hoarddb/hoarddb-server/sealed"
)

// clusterPepperPurposeByte is spelled out, not taken from the sealed package, so the test pins the
// on-disk value (ring user store spec 5.4).
const clusterPepperPurposeByte = 0x0C

func TestTheClusterPepperPurposeIsItsOwnByte(t *testing.T) {
	if sealed.PurposeClusterPepper != clusterPepperPurposeByte {
		t.Fatalf("sealed.PurposeClusterPepper = %#02x, want %#02x", sealed.PurposeClusterPepper, clusterPepperPurposeByte)
	}
	seen := map[byte]string{}
	for _, file := range credentialFiles {
		if other, taken := seen[file.purpose]; taken {
			t.Fatalf("%s and %s share purpose %#02x", file.relPath, other, file.purpose)
		}
		seen[file.purpose] = file.relPath
	}
	if seen[clusterPepperPurposeByte] != clusterpepper.FileName {
		t.Fatalf("no credential file row seals %s under purpose %#02x: %v", clusterpepper.FileName, clusterPepperPurposeByte, seen)
	}
}

// TestThePepperFileIsSealedAtModeOnAndSealedInPlaceWhenTheModeTurnsOn (spec 5.4): at mode off the
// pepper file is the 0600 plaintext form with its magic prefix; a sealing start seals it in place
// under purpose 0x0C, keeping the same pepper; a sealing node writes it sealed from the start, and
// no form of the pepper's bytes is on disk. Break: leave the pepper out of credentialFiles; red.
func TestThePepperFileIsSealedAtModeOnAndSealedInPlaceWhenTheModeTurnsOn(t *testing.T) {
	dataDir := t.TempDir()
	pepper, err := clusterpepper.Generate()
	if err != nil {
		t.Fatal(err)
	}
	plain := newClusterPepperFile(CredentialKeys{}, dataDir)
	if err := plain.Store(pepper); err != nil {
		t.Fatalf("storing at mode off: %v", err)
	}
	path := filepath.Join(dataDir, clusterpepper.FileName)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("the plaintext pepper file is mode %v, want 0600", info.Mode().Perm())
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(contents, []byte(clusterpepper.FileMagic)) {
		t.Fatalf("the plaintext pepper file starts %q, want the magic", firstBytes(contents, 8))
	}

	master := bytes.Repeat([]byte{0x6C}, 32)
	sealing := CredentialKeys{nodeMaster: &NodeMaster{bytes: master, kid: sealed.Kid(master)}, seal: true}
	if err := sealing.sealCredentialFiles(dataDir, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("sealing in place: %v", err)
	}
	assertSealedCredential(t, path, clusterPepperPurposeByte, pepper.Bytes())
	got, err := newClusterPepperFile(sealing, dataDir).Load()
	if err != nil || !got.Equal(pepper) {
		t.Fatalf("the pepper sealed in place loads as %v, %v; want %v", got, err, pepper)
	}
	if _, err := plain.Load(); err == nil || !strings.Contains(err.Error(), "sealed under a node master") {
		t.Fatalf("a sealed pepper read without the node master answered %v", err)
	}

	fresh := t.TempDir()
	if err := newClusterPepperFile(sealing, fresh).Store(pepper); err != nil {
		t.Fatalf("storing on a sealing node: %v", err)
	}
	assertSealedCredential(t, filepath.Join(fresh, clusterpepper.FileName), clusterPepperPurposeByte, pepper.Bytes())
	if _, err := os.Stat(filepath.Join(fresh, clusterpepper.FileName+".tmp")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a temporary was left beside the pepper file: %v", err)
	}
}

// TestAPepperFileIsMovedAsideNotDeleted (spec 5.4): the wrong-pepper heal keeps the file under
// auth.cluster.pepper.wrong-<kid>, and a missing file is fs.ErrNotExist (the node asks again).
func TestAPepperFileIsMovedAsideNotDeleted(t *testing.T) {
	dataDir := t.TempDir()
	file := newClusterPepperFile(CredentialKeys{}, dataDir)
	if _, err := file.Load(); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a missing pepper file answered %v, want fs.ErrNotExist", err)
	}
	pepper, err := clusterpepper.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Store(pepper); err != nil {
		t.Fatal(err)
	}
	aside, err := file.MoveAside(pepper.KID())
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(aside) != clusterpepper.FileName+".wrong-"+pepper.KID() {
		t.Fatalf("moved to %s", aside)
	}
	if _, err := os.Stat(aside); err != nil {
		t.Fatalf("the moved file is gone: %v", err)
	}
	if _, err := file.Load(); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("after the move the pepper file answered %v", err)
	}
}

// TestTheClusterPepperIsNotTheAuthTokenKey (spec 5.1; regression: clusterpepper.Generate takes
// no input): the cluster pepper and its key are neither the node's auth.token.key nor the per-node
// pepper derived from it, so tokens stay per node and a hash made with the cluster pepper does not
// verify under a node's own key.
func TestTheClusterPepperIsNotTheAuthTokenKey(t *testing.T) {
	dataDir := t.TempDir()
	keyBytes, err := CredentialKeys{}.loadOrCreateAuthKey(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	signing := newSigningKey(keyBytes)
	pepper, err := clusterpepper.Generate()
	if err != nil {
		t.Fatal(err)
	}
	var clusterID [16]byte
	clusterID[0] = 7
	clientHash := bytes.Repeat([]byte{0x41}, 32)
	nodePepper := PepperForKey(signing, "deployment")
	switch {
	case bytes.Equal(pepper.Bytes(), keyBytes):
		t.Fatal("the cluster pepper is the node's auth.token.key")
	case bytes.Equal(pepper.Bytes(), nodePepper):
		t.Fatal("the cluster pepper is the node's own pepper")
	case bytes.Equal(pepper.Key(clusterID).Hash(clientHash), PepperHash(nodePepper, clientHash)):
		t.Fatal("a hash made with the cluster pepper equals one made with the node's own pepper")
	}
}

// TestThePepperRestoreTextNamesTheBackupFix (review F2, rule 3): the refusal of an unreadable
// auth.cluster.pepper names both ways out: move it aside to fetch it again, or, when no member
// holds it, restore it from a backup of this node's own data directory, because a sealed copy opens
// only on the node that sealed it. Break: drop the backup sentence from the restore text; red.
func TestThePepperRestoreTextNamesTheBackupFix(t *testing.T) {
	dataDir := t.TempDir()
	master := bytes.Repeat([]byte{0x6D}, 32)
	sealing := CredentialKeys{nodeMaster: &NodeMaster{bytes: master, kid: sealed.Kid(master)}, seal: true}
	pepper, err := clusterpepper.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if err := newClusterPepperFile(sealing, dataDir).Store(pepper); err != nil {
		t.Fatal(err)
	}
	other := bytes.Repeat([]byte{0x6E}, 32)
	_, err = newClusterPepperFile(CredentialKeys{nodeMaster: &NodeMaster{bytes: other, kid: sealed.Kid(other)}, seal: true}, dataDir).Load()
	for _, want := range []string{"Move auth.cluster.pepper aside", "restore auth.cluster.pepper from a backup of this node's own data directory", "opens only on the node that sealed it"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal of a pepper sealed by another node does not say %q: %v", want, err)
		}
	}
}

// TestASealingStartNamesThePlaintextWrongPepperCopiesItLeaves: [GUARD a sealing start logs one line
// naming every auth.cluster.pepper.wrong-<kid> copy still in the clear, and not one a sealing node
// moved aside (that is the sealed file renamed); red when sealCredentialFiles does not look at the
// moved-aside copies]. A wrong copy is not a credential file and is not sealed in place (integration
// review F-4).
func TestASealingStartNamesThePlaintextWrongPepperCopiesItLeaves(t *testing.T) {
	dataDir := t.TempDir()
	master := bytes.Repeat([]byte{0x6C}, 32)
	sealing := CredentialKeys{nodeMaster: &NodeMaster{bytes: master, kid: sealed.Kid(master)}, seal: true}

	movedWhileSealing, err := clusterpepper.Generate()
	if err != nil {
		t.Fatal(err)
	}
	sealedFile := newClusterPepperFile(sealing, dataDir)
	if err := sealedFile.Store(movedWhileSealing); err != nil {
		t.Fatal(err)
	}
	sealedAside, err := sealedFile.MoveAside(movedWhileSealing.KID())
	if err != nil {
		t.Fatal(err)
	}
	movedWhileOff, err := clusterpepper.Generate()
	if err != nil {
		t.Fatal(err)
	}
	plainFile := newClusterPepperFile(CredentialKeys{}, dataDir)
	if err := plainFile.Store(movedWhileOff); err != nil {
		t.Fatal(err)
	}
	plainAside, err := plainFile.MoveAside(movedWhileOff.KID())
	if err != nil {
		t.Fatal(err)
	}

	var logged bytes.Buffer
	if err := sealing.sealCredentialFiles(dataDir, slog.New(slog.NewTextHandler(&logged, nil))); err != nil {
		t.Fatalf("sealCredentialFiles: %v", err)
	}
	lines := strings.Count(logged.String(), "kept in the clear")
	if lines != 1 || !strings.Contains(logged.String(), plainAside) {
		t.Fatalf("the sealing start logged %d lines naming plaintext wrong copies, want one naming %s:\n%s", lines, plainAside, logged.String())
	}
	if strings.Contains(logged.String(), sealedAside) {
		t.Fatalf("the sealing start named %s, a sealed copy, as in the clear:\n%s", sealedAside, logged.String())
	}
	if contents, err := os.ReadFile(plainAside); err != nil || !bytes.HasPrefix(contents, []byte(clusterpepper.FileMagic)) {
		t.Fatalf("the plaintext wrong copy was changed (read error %v)", err)
	}
}
