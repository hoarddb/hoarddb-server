package server

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/sealed"
)

// sealedTestStore fills a sealed node store the way
// TestEncryptedNodeSealsEveryBucketInOneStore does, closes it, and returns its
// data directory.
func sealedTestStore(t *testing.T) string {
	t.Helper()
	dataDir := t.TempDir()
	nodeMaster := encryptionTestNodeMaster()
	cfg := encryptionTestConfig(dataDir, "require", "keyfile")
	kv, _, err := openNodeStore(cfg, nodeMaster)
	if err != nil {
		t.Fatalf("opening the sealed node store: %v", err)
	}
	fillEncryptionTestNode(t, encryptionTestServer(cfg, nodeMaster, kv))
	if err := kv.Close(); err != nil {
		t.Fatalf("closing the sealed node store: %v", err)
	}
	return dataDir
}

// TestBadgerAssertionReadsLogFileKeyIDs holds rev 4 §7.2 rule 2's third half:
// every .vlog and .mem file opens with the id of the datakey that seals it, and
// a log written without the store key records 0 there. Zeroing one value
// log's key id must fail the assertion, naming the file; the MANIFEST and the
// key registry cannot see it.
func TestBadgerAssertionReadsLogFileKeyIDs(t *testing.T) {
	dataDir := sealedTestStore(t)
	valueLogs, err := filepath.Glob(filepath.Join(dataDir, nodeStoreDirName, "*.vlog"))
	if err != nil || len(valueLogs) == 0 {
		t.Fatalf("a closed sealed store holds no value log (%v); the test has nothing to break", err)
	}
	valueLog, err := os.OpenFile(valueLogs[0], os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("opening %s: %v", valueLogs[0], err)
	}
	if _, err := valueLog.WriteAt(make([]byte, 8), 0); err != nil {
		t.Fatalf("zeroing the key id of %s: %v", valueLogs[0], err)
	}
	if err := valueLog.Close(); err != nil {
		t.Fatalf("closing %s: %v", valueLogs[0], err)
	}

	recorder := &recordingT{}
	AssertBadgerStructureEncrypted(recorder, dataDir)
	if len(recorder.failures) == 0 {
		t.Fatalf("a value log with key id 0 passed the structural assertion: %s is unchecked", filepath.Base(valueLogs[0]))
	}
	if !strings.Contains(strings.Join(recorder.failures, "\n"), filepath.Base(valueLogs[0])) {
		t.Errorf("the assertion failed without naming %s: %v", filepath.Base(valueLogs[0]), recorder.failures)
	}
}

// sealedServiceConfig is a standalone node at the given mode, reading its KEK
// from keyPath.
func sealedServiceConfig(dataDir, keyPath, mode string) *config.Config {
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dataDir
	cfg.Metrics.Enabled = false
	cfg.Audit.Enabled = false
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.KeyFile = keyPath
	cfg.Encryption.Mode = mode
	return cfg
}

// startSealedNode runs NewService on a fresh data directory at mode=require,
// writes to it and stops it. It returns the directory and the key file.
func startSealedNode(t *testing.T) (dataDir, keyPath string) {
	t.Helper()
	dataDir = newClusteredTestDataDir(t)
	keyPath = makeKeyFile(t, t.TempDir(), 0x40)
	srv, err := NewService(sealedServiceConfig(dataDir, keyPath, "require"), quietLogger())
	if err != nil {
		t.Fatalf("NewService on a fresh data directory at mode=require: %v", err)
	}
	fillEncryptionTestNode(t, srv.core)
	if err := srv.Stop(); err != nil {
		t.Fatalf("stopping the sealed node: %v", err)
	}
	return dataDir, keyPath
}

// TestRequireSealsAFreshDataDirOnFirstStart is the acceptance claim's first
// line, through the real start path: a fresh data directory started at
// mode=require seals its store on first touch. The store key is wrapped under
// purpose 0x03 before anything is written, and what the node wrote passes the
// structural assertion.
func TestRequireSealsAFreshDataDirOnFirstStart(t *testing.T) {
	dataDir, _ := startSealedNode(t)
	wrappedStoreKey, err := os.ReadFile(filepath.Join(dataDir, "keys", nodeStoreKeyFileName))
	if err != nil {
		t.Fatalf("a fresh mode=require start wrote no store key: %v", err)
	}
	if len(wrappedStoreKey) < sealed.EnvelopeHeaderLen || wrappedStoreKey[5] != sealed.PurposeStoreKey {
		t.Fatalf("keys/%s is not a purpose-0x03 envelope (%d bytes)", nodeStoreKeyFileName, len(wrappedStoreKey))
	}
	AssertBadgerStructureEncrypted(t, dataDir)
}

// TestTamperedOrMissingStoreKeyNeverOpens: every way the store's key can be
// wrong refuses the start. None of them opens the store as plaintext or as
// empty, and none leaves it less sealed than it was. Which cause the refusal
// names is decided elsewhere; this test holds only that it is a refusal.
func TestTamperedOrMissingStoreKeyNeverOpens(t *testing.T) {
	sealedDir, keyPath := startSealedNode(t)
	otherNodeDir, _ := startSealedNode(t)

	flipLastByte := func(path string) func(t *testing.T, dataDir string) {
		return func(t *testing.T, dataDir string) {
			target := filepath.Join(dataDir, path)
			wrapped, err := os.ReadFile(target)
			if err != nil {
				t.Fatalf("reading %s: %v", target, err)
			}
			wrapped[len(wrapped)-1] ^= 0x01
			if err := os.WriteFile(target, wrapped, 0o600); err != nil {
				t.Fatalf("writing %s: %v", target, err)
			}
		}
	}
	cases := []struct {
		name     string
		breakKey func(t *testing.T, dataDir string)
		mode     string
	}{
		{"a flipped byte in the wrapped store key", flipLastByte(filepath.Join("keys", nodeStoreKeyFileName)), "require"},
		{"a flipped byte in the wrapped node master", flipLastByte(filepath.Join("keys", NodeMasterFileName)), "require"},
		{"the wrapped store key deleted", func(t *testing.T, dataDir string) {
			if err := os.Remove(filepath.Join(dataDir, "keys", nodeStoreKeyFileName)); err != nil {
				t.Fatalf("deleting the store key: %v", err)
			}
		}, "require"},
		{"another node's node master", func(t *testing.T, dataDir string) {
			foreign, err := os.ReadFile(filepath.Join(otherNodeDir, "keys", NodeMasterFileName))
			if err != nil {
				t.Fatalf("reading the other node's node master: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dataDir, "keys", NodeMasterFileName), foreign, 0o600); err != nil {
				t.Fatalf("installing the other node's node master: %v", err)
			}
		}, "require"},
		{"the sealed store started with mode=off", func(*testing.T, string) {}, "off"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			copyDataDir(t, sealedDir, dataDir)
			tc.breakKey(t, dataDir)
			cfg := sealedServiceConfig(dataDir, keyPath, tc.mode)
			if tc.mode == "off" {
				cfg.Encryption.Keystore = "off"
				cfg.Encryption.KeyFile = ""
			}
			srv, err := NewService(cfg, quietLogger())
			if err == nil {
				t.Errorf("NewService opened a node with %s", tc.name)
				if stopErr := srv.Stop(); stopErr != nil {
					t.Errorf("stopping the node that should not have opened: %v", stopErr)
				}
				return
			}
			AssertBadgerStructureEncrypted(t, dataDir)
		})
	}
}

// copyDataDir copies a stopped node's data directory, file modes included.
func copyDataDir(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.WalkDir(from, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		target := filepath.Join(to, relative)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, contents, info.Mode().Perm())
	})
	if err != nil {
		t.Fatalf("copying %s to %s: %v", from, to, err)
	}
}

// TestAcceptanceBadgerStructure is scripts/encryption-acceptance.sh's rule-2
// hook: the script stops a live sealed node and points this at its data
// directory. Without the variable there is nothing to check.
func TestAcceptanceBadgerStructure(t *testing.T) {
	dataDir := os.Getenv("HOARDDB_ENCRYPTION_ACCEPTANCE_DIR")
	if dataDir == "" {
		t.Skip("HOARDDB_ENCRYPTION_ACCEPTANCE_DIR is unset; scripts/encryption-acceptance.sh sets it")
	}
	AssertBadgerStructureEncrypted(t, dataDir)
}
