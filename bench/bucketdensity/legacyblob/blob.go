// Package legacyblob is the retired per-bucket blob store, kept here as a
// bench fixture. It measures the retired layout: before the shared-storage
// build, every blob bucket held a directory of payload files and an
// _index.bson rewritten whole on every put, and the opencost mode's blob
// figures were produced by opening exactly this code. Only that open path is
// kept. It lives outside storage/, where nothing in the server can reach it.
// It is not the server's blob store: that is store/blob.go, over the node's
// shared store.
package legacyblob

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// BlobStore provides raw binary storage indexed by UUID v7 DbIDs.
// Files are stored on disk with no encoding overhead, and an index
// file maps DbIDs to metadata.
type BlobStore struct {
	dir   string
	index map[string]blobMeta
}

// blobMeta holds the metadata for a single blob entry.
type blobMeta struct {
	Filename    string                 `bson:"filename"`
	ContentType string                 `bson:"content_type"`
	Size        int64                  `bson:"size"`
	Checksum    string                 `bson:"checksum"`
	CreatedAt   time.Time              `bson:"created_at"`
	Extra       map[string]interface{} `bson:"extra,omitempty"`
}

// NewBlobStore creates a new blob store, creating the directory if needed.
func NewBlobStore(dir string) (*BlobStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating blob directory: %w", err)
	}

	bs := &BlobStore{
		dir:   dir,
		index: make(map[string]blobMeta),
	}

	if err := bs.loadIndex(); err != nil {
		return nil, fmt.Errorf("loading blob index: %w", err)
	}

	return bs, nil
}

// indexPath returns the path to the index file.
func (bs *BlobStore) indexPath() string {
	return filepath.Join(bs.dir, "_index.bson")
}

// loadIndex reads the _index.bson file into the in-memory index.
func (bs *BlobStore) loadIndex() error {
	data, err := os.ReadFile(bs.indexPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("reading index file: %w", err)
	}

	var index map[string]blobMeta
	if err := bson.Unmarshal(data, &index); err != nil {
		return fmt.Errorf("unmarshaling index: %w", err)
	}
	bs.index = index
	return nil
}
