package server

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-server/sealed"
	"github.com/hoarddb/hoarddb-server/store"
)

// TestSealedNodeWritesNoPlaintextBlobPayload is D2b's acceptance claim on a node
// started the way an operator starts one: at mode=require, a blob above the
// inline threshold leaves nothing readable under the data directory or the blob
// root, every payload file begins with a purpose-0x06 header, the registry
// that seals it is on disk beside the other keys, and a restart reads the blob
// back.
func TestSealedNodeWritesNoPlaintextBlobPayload(t *testing.T) {
	dataDir := newClusteredTestDataDir(t)
	keyPath := makeKeyFile(t, t.TempDir(), 0x40)
	content := sealedTestContent()
	cfg := sealedServiceConfig(dataDir, keyPath, "require")

	first, err := NewService(cfg, quietLogger())
	if err != nil {
		t.Fatalf("starting the sealed node: %v", err)
	}
	if resp, err := first.core.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("create database: %+v, %v", resp, err)
	}
	createBlobBucket(t, first.core, "uploads")
	writeBlobOrFail(t, first.core, "app", "uploads", "report", content)
	writeBlobOrFail(t, first.core, "app", "uploads", "note", append([]byte("inline "), sealedBlobMarker...))
	bucketID := lookupBucket(first.core, "app", "uploads").id.Load()
	if err := first.Stop(); err != nil {
		t.Fatalf("stopping the sealed node: %v", err)
	}

	blobRoot := filepath.Join(dataDir, blobRootDirName)
	payloads := payloadFilesOf(t, blobRoot, bucketID)
	if len(payloads) != 1 {
		t.Fatalf("the sealed node wrote payload files %v, want one", payloads)
	}
	assertPayloadIsSealed(t, payloads[0])
	if _, err := os.Stat(filepath.Join(dataDir, nodeKeysDirName, blobEpochRegistryFileName)); err != nil {
		t.Errorf("a sealed node that wrote a sealed blob has no keys/%s: %v", blobEpochRegistryFileName, err)
	}
	walkErr := filepath.WalkDir(dataDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		onDisk, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if bytes.Contains(onDisk, sealedBlobMarker) || bytes.Contains(onDisk, content[:128]) {
			t.Errorf("%s holds blob content in the clear", path)
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walking the data directory: %v", walkErr)
	}

	second, err := NewService(cfg, quietLogger())
	if err != nil {
		t.Fatalf("restarting the sealed node: %v", err)
	}
	t.Cleanup(func() {
		if err := second.Stop(); err != nil {
			t.Logf("stopping the restarted node: %v", err)
		}
	})
	if err := second.LoadCatalog(); err != nil {
		t.Fatalf("loading the catalog after the restart: %v", err)
	}
	if got := readBlobOrFail(t, second.core, "app", "uploads", "report"); !bytes.Equal(got, content) {
		t.Fatalf("after a restart the blob reads back %d bytes that differ from the %d written", len(got), len(content))
	}
}

// TestNewServiceHoldsABlobKeySourceOnASealedNodeOnly: the blob key source
// follows the store. A mode=off node has none even when a keystore is
// configured, and creates no registry; a sealed node has one and its registry.
func TestNewServiceHoldsABlobKeySourceOnASealedNodeOnly(t *testing.T) {
	keyPath := makeKeyFile(t, t.TempDir(), 0x40)
	cases := []struct {
		name       string
		mode       string
		keystore   string
		wantSource bool
	}{
		{"a sealed node", "require", "keyfile", true},
		{"a node with a keystore and encryption off", "off", "keyfile", false},
		{"a plain zero-config node", "off", "off", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := newClusteredTestDataDir(t)
			cfg := sealedServiceConfig(dataDir, keyPath, tc.mode)
			cfg.Encryption.Keystore = tc.keystore
			if tc.keystore == "off" {
				cfg.Encryption.KeyFile = ""
			}
			service, err := NewService(cfg, quietLogger())
			if err != nil {
				t.Fatalf("NewService: %v", err)
			}
			t.Cleanup(func() {
				if err := service.Stop(); err != nil {
					t.Logf("stopping: %v", err)
				}
			})
			if holds := service.core.blobKeys != nil; holds != tc.wantSource {
				t.Errorf("blobKeys held = %v, want %v", holds, tc.wantSource)
			}
			_, statErr := os.Stat(filepath.Join(dataDir, nodeKeysDirName, blobEpochRegistryFileName))
			if exists := statErr == nil; exists != tc.wantSource {
				t.Errorf("keys/%s exists = %v (%v), want %v", blobEpochRegistryFileName, exists, statErr, tc.wantSource)
			}
		})
	}
}

// TestSealedNodeDeclaresABlobStoreField replaces the Stage 2 refusal: under
// every mode a blobStore field is accepted, and it fails if the refusal branch
// returns.
func TestSealedNodeDeclaresABlobStoreField(t *testing.T) {
	for _, mode := range []string{"on", "require", "off"} {
		t.Run(mode, func(t *testing.T) {
			s := newRefusalTestServer(t, mode)
			blob := createWithStores(t, s, "app", "WithBlob", BucketTypeHash, StoreDeclaration{Name: "Photos", Engine: "blobStore"})
			if !blob.Success {
				t.Fatalf("under %s a blobStore field was refused: %s", mode, blob.Message)
			}
		})
	}
}

// TestSealedBlobPayloadFileLengthIsTheCodecs states the per-blob cost: a
// payload file is 24 + size + 28·n bytes, n = ⌈size / 64 KiB⌉, and nothing else
// is added per blob or per bucket.
func TestSealedBlobPayloadFileLengthIsTheCodecs(t *testing.T) {
	s, _ := newSealedKVFaultServer(t, 0x10)
	createBlobBucket(t, s, "uploads")
	for _, size := range []int{store.InlineThreshold, store.InlineThreshold + 1, 3*sealed.BlobChunkSize + 1} {
		content := bytes.Repeat([]byte{0x33}, size)
		writeBlobOrFail(t, s, "app", "uploads", "k", content)
		files := payloadFilesOf(t, s.blobRoot(), lookupBucket(s, "app", "uploads").id.Load())
		if len(files) != 1 {
			t.Fatalf("%d bytes: payload files %v, want one", size, files)
		}
		info, err := os.Stat(files[0])
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		chunks := (size + sealed.BlobChunkSize - 1) / sealed.BlobChunkSize
		if want := int64(24 + size + 28*chunks); info.Size() != want {
			t.Errorf("%d bytes: payload file is %d bytes, want %d", size, info.Size(), want)
		}
	}
}
