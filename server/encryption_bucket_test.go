package server

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// encryptionTestNodeMaster is a fixed node master, so a test can open what it
// sealed.
func encryptionTestNodeMaster() *NodeMaster {
	nm := &NodeMaster{bytes: make([]byte, 32), kid: "nm123456"}
	for i := range nm.bytes {
		nm.bytes[i] = byte(i + 1)
	}
	return nm
}

func encryptionTestConfig(dir, mode, keystore string) *config.Config {
	return &config.Config{
		Storage: config.StorageConfig{
			DataDir:            dir,
			BadgerBlockCacheMB: 64,
			BadgerIndexCacheMB: 64,
		},
		Encryption: config.EncryptionConfig{Mode: mode, Keystore: keystore},
	}
}

func encryptionTestServer(cfg *config.Config, nm *NodeMaster, kv storage.KV) *Server {
	return &Server{
		config:      cfg,
		nodeMaster:  nm,
		logger:      discardLogger(),
		dataStore:   &serverData{dbs: make(map[string]*inMemoryDB)},
		kv:          kv,
		authReqSeen: make(map[string]time.Time),
		knownNodes:  make(map[string]*NodeInfo),
	}
}

// recordingT collects what AssertBadgerStructureEncrypted reports, so a test
// can show the assertion fails where it must.
type recordingT struct{ failures []string }

func (r *recordingT) Fatalf(format string, args ...interface{}) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

func (r *recordingT) Errorf(format string, args ...interface{}) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

// fillEncryptionTestNode creates a hash bucket with a secondary index and a
// btree bucket on s and writes to both, so the store holds documents and index
// entries of more than one engine.
func fillEncryptionTestNode(t *testing.T, s *Server) {
	t.Helper()
	// Through the create verb, so anything a database create writes to disk is
	// on disk for the walk below to find.
	if resp, err := s.CreateDatabase("testdb"); err != nil || !resp.Success {
		t.Fatalf("creating testdb: %+v, %v", resp, err)
	}
	for _, req := range []CreateBucketRequest{
		{Database: "testdb", Name: "users", Type: BucketTypeHash, Options: map[string]interface{}{"indices": []string{"Email"}}},
		{Database: "testdb", Name: "events", Type: BucketTypeBTree},
	} {
		resp, err := s.createBucketLocked(context.Background(), req)
		if err != nil || !resp.Success {
			t.Fatalf("creating %s: %+v, %v", req.Name, resp, err)
		}
	}
	for i := 0; i < 50; i++ {
		for _, bucket := range []string{"users", "events"} {
			resp, err := s.Insert(InsertRequest{Database: "testdb", Bucket: bucket, Key: fmt.Sprintf("k%03d", i),
				Data: map[string]interface{}{"Email": fmt.Sprintf("user%d@example.com", i)}})
			if err != nil || !resp.Success {
				t.Fatalf("insert into %s: %+v, %v", bucket, resp, err)
			}
		}
	}
}

// TestEncryptedNodeSealsEveryBucketInOneStore is Step 13's rewrite of the
// per-bucket form. With every engine on the node's store there is one Badger
// directory to prove sealed, not one per bucket and one per index, and one
// structural assertion covers a hash bucket's documents, its index entries and
// a btree bucket together. No bucket writes a key file of its own. The
// plaintext arm shows the assertion can fail.
func TestEncryptedNodeSealsEveryBucketInOneStore(t *testing.T) {
	sealedDir := t.TempDir()
	nm := encryptionTestNodeMaster()
	cfg := encryptionTestConfig(sealedDir, "require", "keyfile")
	kv, _, err := openNodeStore(cfg, nm)
	if err != nil {
		t.Fatalf("opening the sealed node store: %v", err)
	}
	fillEncryptionTestNode(t, encryptionTestServer(cfg, nm, kv))
	if err := kv.Close(); err != nil {
		t.Fatalf("closing the sealed node store: %v", err)
	}
	AssertBadgerStructureEncrypted(t, sealedDir)

	// Everything the node wrote outside its store and its keys is suspect: a
	// per-bucket key file or directory would show up here, and so would a
	// directory named after a database, which lists its name in the clear.
	err = filepath.WalkDir(sealedDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path != sealedDir && entry.IsDir() {
			switch entry.Name() {
			case nodeStoreDirName, "keys", "wal":
				return filepath.SkipDir
			}
			t.Errorf("the node wrote a directory outside its store, keys and log, %s; a database or bucket needs none", path)
			return filepath.SkipDir
		}
		if !entry.IsDir() {
			t.Errorf("a bucket wrote a file of its own, %s; every bucket lives in the node's store under its one key", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the data directory: %v", err)
	}

	plaintextDir := t.TempDir()
	plaintextCfg := encryptionTestConfig(plaintextDir, "off", "off")
	plaintextKV, _, err := openNodeStore(plaintextCfg, nil)
	if err != nil {
		t.Fatalf("opening the plaintext node store: %v", err)
	}
	fillEncryptionTestNode(t, encryptionTestServer(plaintextCfg, nil, plaintextKV))
	if err := plaintextKV.Close(); err != nil {
		t.Fatalf("closing the plaintext node store: %v", err)
	}
	recorder := &recordingT{}
	AssertBadgerStructureEncrypted(recorder, plaintextDir)
	if len(recorder.failures) == 0 {
		t.Fatal("AssertBadgerStructureEncrypted passed a plaintext node store; it proves nothing")
	}
}

// TestRequireSealsABucketCreatedWithoutTheFlag: under mode=require the node's
// store is sealed, so a bucket on it is sealed whether or not the create asked.
// Per-bucket encryption policy has no subject once every bucket shares the
// node's key (spec §5.5, option 1: "the node is encrypted or it is not"), and
// the refusal that used to require the flag read a per-bucket key file that no
// longer exists.
func TestRequireSealsABucketCreatedWithoutTheFlag(t *testing.T) {
	cfg := encryptionTestConfig(t.TempDir(), "require", "keyfile")
	s := encryptionTestServer(cfg, encryptionTestNodeMaster(), kvfault.New())
	resp, err := s.createBucketLocked(context.Background(), CreateBucketRequest{
		Database: "testdb",
		Name:     "plain",
		Type:     BucketTypeHash,
	})
	if err != nil {
		t.Fatalf("createBucketLocked: %v", err)
	}
	if !resp.Success {
		t.Fatalf("mode=require refused a hash bucket created without the flag: %s", resp.Message)
	}
}

// TestEncryptedBucketRefusesWithoutNodeMaster verifies that a bucket asking for
// encryption on a node whose store is not sealed is refused, naming the
// keystore.
func TestEncryptedBucketRefusesWithoutNodeMaster(t *testing.T) {
	cfg := encryptionTestConfig(t.TempDir(), "off", "off")
	s := encryptionTestServer(cfg, nil, kvfault.New())

	resp, err := s.createBucketLocked(context.Background(), CreateBucketRequest{
		Database:  "testdb",
		Name:      "encrypted",
		Type:      BucketTypeHash,
		Encrypted: true,
	})
	if err != nil {
		t.Fatalf("createBucketLocked failed: %v", err)
	}
	if resp.Success {
		t.Errorf("encrypted request without node master should fail, got success=true")
	}
	if !strings.Contains(resp.Message, "keystore") {
		t.Errorf("message should mention keystore, got: %s", resp.Message)
	}
}

// TestPlaintextBucketUnchangedAtModeOff verifies that with the keystore off a
// bucket is created with no key file and no directory of its own.
func TestPlaintextBucketUnchangedAtModeOff(t *testing.T) {
	dir := t.TempDir()
	s := encryptionTestServer(encryptionTestConfig(dir, "off", "off"), nil, kvfault.New())

	resp, err := s.createBucketLocked(context.Background(), CreateBucketRequest{
		Database: "testdb",
		Name:     "plaintext",
		Type:     BucketTypeHash,
	})
	if err != nil {
		t.Fatalf("createBucketLocked failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("createBucketLocked returned success=false: %s", resp.Message)
	}
	matches, err := filepath.Glob(filepath.Join(dir, "testdb", "plaintext", "*"))
	if err != nil {
		t.Fatalf("globbing the bucket directory: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("a hash bucket wrote files of its own: %v; it lives in the node's store", matches)
	}
}
