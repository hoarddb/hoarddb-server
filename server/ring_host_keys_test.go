package server

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/sealed"
)

// plaintextDataDirWithRing returns a copy of a data directory a node with encryption off and no
// keystore wrote (its earlier store, credentials and users in plaintext), with its ring
// directory removed, so the only sealed material in it is what a case adds.
func plaintextDataDirWithRing(t *testing.T, plain string) string {
	t.Helper()
	dataDir := t.TempDir()
	copyRingWhileRunning(t, plain, dataDir)
	if err := os.RemoveAll(filepath.Join(dataDir, ringDirName)); err != nil {
		t.Fatal(err)
	}
	if found, err := findSealedMaterial(dataDir); err != nil || len(found) > 0 {
		t.Fatalf("the plaintext fixture already holds sealed material %v (%v)", found, err)
	}
	return dataDir
}

// TestAMissingNodeMasterBesideASealedRingIsRefusedNotReplaced: R-7 [GUARD each of the four ring
// evidences, alone beside a plaintext earlier store, stops a new node master being minted over
// sealed ring data; red when that evidence is dropped from findSealedMaterial]. Ring encryption at
// rest spec §5.4 R-7, §9.1 step 4.
func TestAMissingNodeMasterBesideASealedRingIsRefusedNotReplaced(t *testing.T) {
	plain := t.TempDir()
	ringRunOnce(t, plain, nil)
	sealedDir := t.TempDir()
	ringRunOnce(t, sealedDir, ringKEKEdit(t, ringFirstKEK, "on"))
	sealedRing := filepath.Join(sealedDir, ringDirName)
	segments, err := filepath.Glob(filepath.Join(sealedRing, "wal", walSegmentGlob))
	if err != nil || len(segments) == 0 {
		t.Fatalf("the sealed run left no ring log segment (%v)", err)
	}

	for _, tc := range []struct {
		name     string
		add      func(t *testing.T, ringDir string)
		evidence string
	}{
		{"the ring store key file", func(t *testing.T, ringDir string) {
			copyRingWhileRunning(t, filepath.Join(sealedRing, ringKeysDirName, ringStoreKeyFileName), filepath.Join(ringDir, ringKeysDirName, ringStoreKeyFileName))
		}, "ring/keys/store.wrapped"},
		{"the ring log key file", func(t *testing.T, ringDir string) {
			copyRingWhileRunning(t, filepath.Join(sealedRing, ringKeysDirName, ringWALKeyFileName), filepath.Join(ringDir, ringKeysDirName, ringWALKeyFileName))
		}, "ring/keys/wal.wrapped"},
		{"a sealed ring store", func(t *testing.T, ringDir string) {
			copyRingWhileRunning(t, filepath.Join(sealedRing, "badger"), filepath.Join(ringDir, "badger"))
		}, "ring/badger/ (its key registry is sealed)"},
		{"a sealed ring log segment", func(t *testing.T, ringDir string) {
			copyRingWhileRunning(t, segments[0], filepath.Join(ringDir, "wal", filepath.Base(segments[0])))
		}, "ring/wal/" + filepath.Base(segments[0]) + " (sealed)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := plaintextDataDirWithRing(t, plain)
			ringDir := filepath.Join(dataDir, ringDirName)
			for _, dir := range []string{filepath.Join(ringDir, ringKeysDirName), filepath.Join(ringDir, "wal")} {
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			tc.add(t, ringDir)
			cfg := config.DefaultConfig()
			cfg.Storage.DataDir = dataDir
			ringKEKEdit(t, ringFirstKEK, "on")(cfg)
			ks, err := OpenKeystore(cfg)
			if err != nil {
				t.Fatal(err)
			}
			_, err = LoadOrCreateNodeMaster(dataDir, ks)
			if err == nil {
				t.Fatalf("a new node master was minted beside %s", tc.evidence)
			}
			if !strings.Contains(err.Error(), "is missing from data directory") || !strings.Contains(err.Error(), tc.evidence) {
				t.Fatalf("the refusal does not name %q as the sealed material:\n%v", tc.evidence, err)
			}
			if _, statErr := os.Stat(filepath.Join(dataDir, "keys", NodeMasterFileName)); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("a node master file exists after the refusal (stat: %v)", statErr)
			}
		})
	}
}

// TestImportKeyRefusesAMasterThatDoesNotOpenTheRingKeys: [GUARD -import-key's check opens both
// ring key files with the imported node master, beside a plaintext earlier store; red when
// verifyImportedMaster skips the ring files]. Spec §5.6, §9.1 step 4.
func TestImportKeyRefusesAMasterThatDoesNotOpenTheRingKeys(t *testing.T) {
	plain := t.TempDir()
	ringRunOnce(t, plain, nil)
	current, other := testRingNodeMaster(0x41), testRingNodeMaster(0x42)
	storeWrapped, err := sealed.Seal(sealed.PurposeStoreKey, current.Bytes(), ringStoreKeyIdentity, 1, bytes.Repeat([]byte{0x7E}, 32))
	if err != nil {
		t.Fatal(err)
	}
	_, registryWrapped, err := newSealedWALEpochRegistry(current.Bytes(), ringWALKeyIdentity)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		file    string
		wrapped []byte
	}{
		{ringStoreKeyFileName, storeWrapped},
		{ringWALKeyFileName, registryWrapped},
	} {
		t.Run(tc.file, func(t *testing.T) {
			dataDir := plaintextDataDirWithRing(t, plain)
			keysDir := filepath.Join(dataDir, ringDirName, ringKeysDirName)
			if err := os.MkdirAll(keysDir, 0o700); err != nil {
				t.Fatal(err)
			}
			keyPath := filepath.Join(keysDir, tc.file)
			if err := os.WriteFile(keyPath, tc.wrapped, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := verifyImportedMaster(dataDir, current.Bytes()); err != nil {
				t.Fatalf("the node master that sealed %s was refused: %v", keyPath, err)
			}
			err := verifyImportedMaster(dataDir, other.Bytes())
			if err == nil {
				t.Fatalf("a node master that does not open %s was accepted", keyPath)
			}
			if !strings.Contains(err.Error(), keyPath) {
				t.Fatalf("the refusal does not name %s:\n%v", keyPath, err)
			}
		})
	}
}
