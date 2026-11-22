package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-server/store"
)

// The strings the probe writes. Each is unique to its role, so finding one
// under the data directory means that role's bytes are on disk unsealed.
const (
	probeHashKey      = "wolfhound-7d2f91ab"
	probeBTreeKey     = "marmalade-3c85e0d4"
	probeIndexedValue = "cellophane-b41a6f29"
	probeBlobMarker   = "tangerine-9e06c1f7"
)

// TestEncryptedNodeLeavesNoPlaintext is T-14: with encryption.mode on, a node
// that has written a hash key, a btree key and an indexed value is stopped, and
// every file under its data directory is searched for each. None may be found,
// whichever engine wrote it: they all live in the node's one sealed store.
//
// The fourth clause is the blob payload: a payload above the inline threshold
// is a file outside the store, so it is the one that would be in the clear if
// the sealed blob path (inventory D2b) regressed.
func TestEncryptedNodeLeavesNoPlaintext(t *testing.T) {
	dataDir := newClusteredTestDataDir(t)
	keyPath := makeKeyFile(t, t.TempDir(), 0x40)
	node, err := NewService(sealedServiceConfig(dataDir, keyPath, "on"), quietLogger())
	if err != nil {
		t.Fatalf("NewService at encryption mode on: %v", err)
	}
	writeEncryptionProbe(t, node.core)
	writeBlobProbe(t, node.core)
	if err := node.Stop(); err != nil {
		t.Fatalf("stopping the node: %v", err)
	}

	for _, probe := range []struct{ name, needle string }{
		{"hash_key", probeHashKey},
		{"btree_key", probeBTreeKey},
		{"indexed_value", probeIndexedValue},
		{"blob_payload", probeBlobMarker},
	} {
		t.Run(probe.name, func(t *testing.T) {
			where, err := firstFileContaining(dataDir, []byte(probe.needle))
			if err != nil {
				t.Fatalf("searching %s for %q: %v", dataDir, probe.needle, err)
			}
			if where != "" {
				t.Errorf("%q is on disk in the clear in %s, with encryption.mode on", probe.needle, where)
			}
		})
	}
}

// writeEncryptionProbe writes the probe strings through the server's own verbs:
// a hash key with an indexed field, and a btree key.
func writeEncryptionProbe(t *testing.T, s *Server) {
	t.Helper()
	if resp, err := s.CreateDatabase("probe"); err != nil || !resp.Success {
		t.Fatalf("creating the probe database: %+v, %v", resp, err)
	}
	for _, req := range []CreateBucketRequest{
		{Database: "probe", Name: "people", Type: BucketTypeHash, Options: map[string]interface{}{"indices": []string{"Email"}}},
		{Database: "probe", Name: "events", Type: BucketTypeBTree},
	} {
		if resp, err := s.CreateBucket(req); err != nil || !resp.Success {
			t.Fatalf("creating bucket %s: %+v, %v", req.Name, resp, err)
		}
	}
	if resp, err := s.Insert(InsertRequest{Database: "probe", Bucket: "people", Key: probeHashKey,
		Data: map[string]interface{}{"Email": probeIndexedValue}}); err != nil || !resp.Success {
		t.Fatalf("inserting the hash probe: %+v, %v", resp, err)
	}
	if resp, err := s.Insert(InsertRequest{Database: "probe", Bucket: "events", Key: probeBTreeKey,
		Data: map[string]interface{}{"n": int32(1)}}); err != nil || !resp.Success {
		t.Fatalf("inserting the btree probe: %+v, %v", resp, err)
	}
}

// writeBlobProbe writes a blob payload above the inline threshold, so it is a
// file outside the store, holding a unique marker.
func writeBlobProbe(t *testing.T, s *Server) {
	t.Helper()
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "probe", Name: "files", Type: BucketTypeBlob}); err != nil || !resp.Success {
		t.Fatalf("creating the blob probe bucket on a sealed node: %+v, %v", resp, err)
	}
	payload := bytes.Repeat([]byte(probeBlobMarker), (store.InlineThreshold/len(probeBlobMarker))+2)
	if resp, err := s.BlobWrite(BlobWriteRequest{Database: "probe", Bucket: "files", Key: "report", Data: payload}); err != nil || !resp.Success {
		t.Fatalf("writing the blob probe: %+v, %v", resp, err)
	}
}

// firstFileContaining returns the first file under root that holds needle, or
// "" when none does.
func firstFileContaining(root string, needle []byte) (string, error) {
	var found string
	errFound := errors.New("found")
	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !entry.Type().IsRegular() {
			return err
		}
		contains, err := fileContains(path, needle)
		if err != nil {
			return fmt.Errorf("reading %s: %w", path, err)
		}
		if contains {
			found = path
			return errFound
		}
		return nil
	})
	if walkErr != nil && !errors.Is(walkErr, errFound) {
		return "", walkErr
	}
	return found, nil
}

// fileContains reads path in chunks that overlap by one needle, because a
// store's value logs are preallocated and large, and a match can straddle a
// chunk boundary.
func fileContains(path string, needle []byte) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer func() { _ = file.Close() }()

	const chunkBytes = 1 << 20
	buffer := make([]byte, chunkBytes+len(needle))
	carried := 0
	for {
		read, err := file.Read(buffer[carried : carried+chunkBytes])
		window := buffer[:carried+read]
		if bytes.Contains(window, needle) {
			return true, nil
		}
		carried = min(len(needle)-1, len(window))
		copy(buffer, window[len(window)-carried:])
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
}
