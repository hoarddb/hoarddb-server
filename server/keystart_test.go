package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/sealed"
)

// dataDirSnapshot maps every path under root to a digest of its contents, so a
// test can prove a refused start wrote, changed and removed nothing.
func dataDirSnapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	snapshot := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			snapshot[relative] = "dir"
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(contents)
		snapshot[relative] = hex.EncodeToString(digest[:])
		return nil
	})
	if err != nil {
		t.Fatalf("snapshotting %s: %v", root, err)
	}
	return snapshot
}

func writeTestFile(t *testing.T, path string, contents []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("creating %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}

// sealedUsersEnvelope is the first 34 bytes of a v2 sealed users.json: the
// header alone, which is all a "sealed or not" decision may read.
func sealedUsersEnvelope() []byte {
	header := make([]byte, sealed.EnvelopeHeaderLen)
	copy(header, []byte{0x00, 'H', 'D', 'B', sealed.Version, sealed.PurposeUsers})
	return header
}

// legacyUsersEnvelope is the shipped {0x00, 0x01} users.json, encrypted under
// auth.token.key. It is not sealed under a node master and must not count.
func legacyUsersEnvelope() []byte {
	header := make([]byte, sealed.EnvelopeHeaderLen)
	copy(header, []byte{0x00, 'H', 'D', 'B', 0x00, 0x01})
	return header
}

// sealedWALSegment is a WAL segment holding only a sealed file header.
func sealedWALSegment() []byte {
	return sealed.MarshalFileHeader(&sealed.FileHeader{
		Magic:   sealed.Magic,
		Version: sealed.FileVersion,
		Purpose: sealed.PurposeWALEntry,
		EpochID: 1,
	})
}

// TestMissingNodeMasterOnASealedDirectoryIsRefusedNotMinted: A data
// directory that holds sealed material and has lost its node master must be
// refused, with nothing written: a fresh master beside data it cannot open
// destroys the evidence and misleads the operator.
func TestMissingNodeMasterOnASealedDirectoryIsRefusedNotMinted(t *testing.T) {
	sealedArtifacts := []struct {
		name  string
		plant func(t *testing.T, dataDir string)
		named string
	}{
		{"the wrapped store key", func(t *testing.T, dataDir string) {
			writeTestFile(t, filepath.Join(dataDir, "keys", nodeStoreKeyFileName), make([]byte, 82))
		}, "keys/store.wrapped"},
		{"a sealed users.json", func(t *testing.T, dataDir string) {
			writeTestFile(t, filepath.Join(dataDir, "users.json"), sealedUsersEnvelope())
		}, "users.json"},
		{"the wrapped WAL epoch registry", func(t *testing.T, dataDir string) {
			writeTestFile(t, filepath.Join(dataDir, "wal", WALEpochRegistryFileName), make([]byte, 82))
		}, "wal/keys.wrapped"},
		{"the wrapped blob epoch registry", func(t *testing.T, dataDir string) {
			writeTestFile(t, filepath.Join(dataDir, "keys", blobEpochRegistryFileName), make([]byte, 82))
		}, "keys/blobs.wrapped"},
		{"a sealed WAL segment", func(t *testing.T, dataDir string) {
			writeTestFile(t, filepath.Join(dataDir, "wal", "wal_00001.log"), sealedWALSegment())
		}, "wal_00001.log"},
		{"a sealed store registry with its key file gone", func(t *testing.T, dataDir string) {
			copyDataDir(t, sealedTestStore(t), dataDir)
			if err := os.Remove(filepath.Join(dataDir, "keys", nodeStoreKeyFileName)); err != nil {
				t.Fatalf("removing the store key from the copy: %v", err)
			}
		}, "store/"},
	}
	for _, tc := range sealedArtifacts {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			tc.plant(t, dataDir)
			keystore := testKeystore(t, 0x10)
			before := dataDirSnapshot(t, dataDir)

			masterAfterRefusal, err := LoadOrCreateNodeMaster(dataDir, keystore)
			if err == nil {
				t.Fatalf("LoadOrCreateNodeMaster minted a node master beside %s (created=%v)", tc.name, masterAfterRefusal.Created())
			}
			for _, want := range []string{"keys/" + NodeMasterFileName, dataDir, tc.named, "-import-key", "backup"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal does not mention %q: %v", want, err)
				}
			}
			if after := dataDirSnapshot(t, dataDir); !reflect.DeepEqual(before, after) {
				t.Errorf("the refused start changed the data directory:\nbefore %v\nafter  %v", before, after)
			}
		})
	}

	freshDirectories := []struct {
		name  string
		plant func(t *testing.T, dataDir string)
	}{
		{"an empty directory", func(*testing.T, string) {}},
		{"only the TLS material a start writes first", func(t *testing.T, dataDir string) {
			writeTestFile(t, filepath.Join(dataDir, "keys", "server.key"), []byte("tls"))
			writeTestFile(t, filepath.Join(dataDir, "keys", "server.crt"), []byte("tls"))
		}},
		{"a plaintext users.json", func(t *testing.T, dataDir string) {
			writeTestFile(t, filepath.Join(dataDir, "users.json"), []byte(`{"users":[]}`))
		}},
		{"a users.json encrypted under auth.token.key", func(t *testing.T, dataDir string) {
			writeTestFile(t, filepath.Join(dataDir, "users.json"), legacyUsersEnvelope())
		}},
		{"a plaintext WAL segment", func(t *testing.T, dataDir string) {
			writeTestFile(t, filepath.Join(dataDir, "wal", "wal_00001.log"), []byte("plaintext wal bytes"))
		}},
	}
	for _, tc := range freshDirectories {
		t.Run("still mints in "+tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			tc.plant(t, dataDir)
			masterMinted, err := LoadOrCreateNodeMaster(dataDir, testKeystore(t, 0x10))
			if err != nil {
				t.Fatalf("a directory with %s holds no sealed material and must mint on first start: %v", tc.name, err)
			}
			if !masterMinted.Created() {
				t.Error("Created should be true on first start")
			}
		})
	}
}

// goneMasterNode is a stopped sealed node whose node master file has been
// deleted, with the escrowed master it should be restored from.
type goneMasterNode struct {
	dataDir    string
	keyPath    string
	exportPath string
	kek        []byte
	master     []byte
}

func newGoneMasterNode(t *testing.T) goneMasterNode {
	t.Helper()
	original, keyPath := startSealedNode(t)
	dataDir := t.TempDir()
	copyDataDir(t, original, dataDir)

	exportPath := filepath.Join(t.TempDir(), "nm.key")
	if err := ExportNodeMaster(sealedServiceConfig(dataDir, keyPath, "require"), exportPath); err != nil {
		t.Fatalf("exporting the node master: %v", err)
	}
	master, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("reading the export: %v", err)
	}
	kek, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("reading the KEK: %v", err)
	}
	if err := os.Remove(filepath.Join(dataDir, "keys", NodeMasterFileName)); err != nil {
		t.Fatalf("deleting the node master: %v", err)
	}
	return goneMasterNode{dataDir: dataDir, keyPath: keyPath, exportPath: exportPath, kek: kek, master: master}
}

// TestNewServiceRefusesASealedDirectoryWhoseNodeMasterIsGone runs the refusal through
// the real start path, and proves the way out the refusal names actually works.
func TestNewServiceRefusesASealedDirectoryWhoseNodeMasterIsGone(t *testing.T) {
	node := newGoneMasterNode(t)
	cfg := sealedServiceConfig(node.dataDir, node.keyPath, "require")
	before := dataDirSnapshot(t, node.dataDir)

	srv, err := NewService(cfg, quietLogger())
	if err == nil {
		_ = srv.Stop()
		t.Fatal("NewService started a sealed node that has lost its node master")
	}
	for _, want := range []string{"keys/" + NodeMasterFileName, "-import-key", "backup", "keys/store.wrapped"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "wrong parent key") {
		t.Errorf("the refusal is still the misleading users.json failure: %v", err)
	}
	if after := dataDirSnapshot(t, node.dataDir); !reflect.DeepEqual(before, after) {
		t.Errorf("the refused start changed the data directory:\nbefore %v\nafter  %v", before, after)
	}

	// The fix the message names: import the escrowed master, without -force
	// because no decoy was written, then start.
	if err := ImportNodeMaster(cfg, node.exportPath, false); err != nil {
		t.Fatalf("-import-key of the escrowed master on the refused directory: %v", err)
	}
	restored, err := NewService(cfg, quietLogger())
	if err != nil {
		t.Fatalf("NewService after restoring the node master: %v", err)
	}
	if err := restored.Stop(); err != nil {
		t.Fatalf("stopping the restored node: %v", err)
	}
}

// TestMissingWALRegistryBesideSealedSegmentsIsRefusedNotMinted: the WAL epoch
// registry holds the only copy of the keys its segments are sealed under, so a
// registry minted beside sealed segments can never open them.
func TestMissingWALRegistryBesideSealedSegmentsIsRefusedNotMinted(t *testing.T) {
	master := encryptionTestNodeMaster()

	t.Run("sealed segments and no registry", func(t *testing.T) {
		dataDir := t.TempDir()
		writeTestFile(t, filepath.Join(dataDir, "wal", "wal_00001.log"), sealedWALSegment())
		before := dataDirSnapshot(t, dataDir)

		source, err := LoadOrCreateWALKeySource(dataDir, master)
		if err == nil {
			t.Fatalf("a WAL key source (%v) was minted beside a sealed segment", source)
		}
		for _, want := range []string{"wal/" + WALEpochRegistryFileName, "wal_00001.log", "backup"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the refusal does not mention %q: %v", want, err)
			}
		}
		if after := dataDirSnapshot(t, dataDir); !reflect.DeepEqual(before, after) {
			t.Errorf("the refused start changed the WAL directory:\nbefore %v\nafter  %v", before, after)
		}
	})

	for _, tc := range []struct {
		name     string
		contents []byte
	}{
		{"no segments", nil},
		{"only plaintext segments", []byte("plaintext wal bytes")},
	} {
		t.Run("still mints with "+tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			if tc.contents != nil {
				writeTestFile(t, filepath.Join(dataDir, "wal", "wal_00001.log"), tc.contents)
			}
			if _, err := LoadOrCreateWALKeySource(dataDir, master); err != nil {
				t.Fatalf("a WAL with no sealed segment must mint its registry: %v", err)
			}
			if _, err := os.Stat(filepath.Join(dataDir, "wal", WALEpochRegistryFileName)); err != nil {
				t.Errorf("no registry was written: %v", err)
			}
		})
	}
}

// TestImportKeyRefusesAMasterThatDoesNotOpenTheStoreKey: -import-key -force
// used to install any 32 bytes, leaving a node that cannot start and no trace
// of the file that was overwritten.
func TestImportKeyRefusesAMasterThatDoesNotOpenTheStoreKey(t *testing.T) {
	node := newGoneMasterNode(t)
	cfg := sealedServiceConfig(node.dataDir, node.keyPath, "require")
	wrongMaster := bytes.Repeat([]byte{0x99}, 32)
	wrongPath := filepath.Join(t.TempDir(), "wrong.key")
	writeTestFile(t, wrongPath, wrongMaster)
	masterPath := filepath.Join(node.dataDir, "keys", NodeMasterFileName)

	t.Run("no master on disk", func(t *testing.T) {
		before := dataDirSnapshot(t, node.dataDir)
		err := ImportNodeMaster(cfg, wrongPath, false)
		if err == nil {
			t.Fatal("ImportNodeMaster installed a master that does not open keys/store.wrapped")
		}
		if !strings.Contains(err.Error(), "keys/store.wrapped") {
			t.Errorf("the refusal does not name keys/store.wrapped: %v", err)
		}
		if after := dataDirSnapshot(t, node.dataDir); !reflect.DeepEqual(before, after) {
			t.Errorf("the refused import changed the data directory:\nbefore %v\nafter  %v", before, after)
		}
	})

	t.Run("a live master and -force", func(t *testing.T) {
		if err := ImportNodeMaster(cfg, node.exportPath, false); err != nil {
			t.Fatalf("restoring the right master: %v", err)
		}
		liveMaster, err := os.ReadFile(masterPath)
		if err != nil {
			t.Fatalf("reading the restored master: %v", err)
		}
		if err := ImportNodeMaster(cfg, wrongPath, true); err == nil {
			t.Fatal("-force overwrote a working node master with one that opens nothing")
		}
		stillLive, err := os.ReadFile(masterPath)
		if err != nil {
			t.Fatalf("reading the master after the refused import: %v", err)
		}
		if !bytes.Equal(liveMaster, stillLive) {
			t.Error("the refused -force import rewrote the wrapped node master")
		}
	})
}

// TestImportKeyRefusesAMasterThatDoesNotOpenTheBlobRegistry: keys/blobs.wrapped
// holds the only copy of the blob epoch keys, so a master that cannot open it
// leaves every sealed blob unreadable. It is the only key file in this
// directory, so the store key and WAL registry checks cannot be what refuses.
func TestImportKeyRefusesAMasterThatDoesNotOpenTheBlobRegistry(t *testing.T) {
	node := newBlobRegistryTestNode(t)
	node.mustLoad(t)
	dataDir := node.cfg.Storage.DataDir
	before := dataDirSnapshot(t, dataDir)

	wrongMaster := bytes.Repeat([]byte{0x99}, 32)
	err := verifyImportedMaster(dataDir, wrongMaster)
	if err == nil {
		t.Fatal("verifyImportedMaster accepted a master that does not open keys/blobs.wrapped")
	}
	for _, want := range []string{"keys/blobs.wrapped", "-export-key", "backup"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), hex.EncodeToString(wrongMaster)) {
		t.Error("the refusal contains the rejected master")
	}
	if after := dataDirSnapshot(t, dataDir); !reflect.DeepEqual(before, after) {
		t.Errorf("the check changed the data directory:\nbefore %v\nafter  %v", before, after)
	}

	if err := verifyImportedMaster(dataDir, node.nm.Bytes()); err != nil {
		t.Errorf("the master that sealed keys/blobs.wrapped was refused: %v", err)
	}
}

// TestKeyStartFailuresNeverPrintKeyMaterial: no refusal on the start path, and
// nothing logged on the way, may contain the KEK or the node master, raw or hex.
func TestKeyStartFailuresNeverPrintKeyMaterial(t *testing.T) {
	node := newGoneMasterNode(t)
	cfg := sealedServiceConfig(node.dataDir, node.keyPath, "require")
	wrongMaster := bytes.Repeat([]byte{0x99}, 32)
	wrongPath := filepath.Join(t.TempDir(), "wrong.key")
	writeTestFile(t, wrongPath, wrongMaster)

	var logged bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug}))

	var outputs []string
	if srv, err := NewService(cfg, logger); err != nil {
		outputs = append(outputs, err.Error())
	} else {
		_ = srv.Stop()
		t.Fatal("NewService started a sealed node that has lost its node master")
	}
	outputs = append(outputs, importFailureText(t, cfg, wrongPath))
	outputs = append(outputs, logged.String())

	for _, secret := range [][]byte{node.kek, node.master, wrongMaster} {
		for _, form := range []string{string(secret), hex.EncodeToString(secret)} {
			for _, output := range outputs {
				if strings.Contains(output, form) {
					t.Fatalf("key material (%d-byte secret) appears in start-up output", len(secret))
				}
			}
		}
	}
}

// importFailureText runs a -force import that must be refused and returns the
// refusal, failing the test if the import went through.
func importFailureText(t *testing.T, cfg *config.Config, keyPath string) string {
	t.Helper()
	err := ImportNodeMaster(cfg, keyPath, true)
	if err == nil {
		t.Fatal("ImportNodeMaster installed a master that does not open the sealed store key")
	}
	return err.Error()
}

// testKeystore is a keystore over a KEK of seed, seed+1, ... without a file.
func testKeystore(t *testing.T, seed byte) *Keystore {
	t.Helper()
	kek := make([]byte, 32)
	for i := range kek {
		kek[i] = seed + byte(i)
	}
	return &Keystore{kek: kek, kid: sealed.Kid(kek), mode: "on"}
}
