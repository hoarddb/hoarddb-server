package server

import (
	"fmt"
	"github.com/hoarddb/hoarddb-server/namepolicy"
	"os"
	"path/filepath"
	"strings"

	"github.com/hoarddb/hoarddb-server/config"
)

// blobDirLockName is the file a node holds an exclusive lock on inside a
// configured blob directory. It is not in a two-level leaf and does not parse
// as a payload name, so the orphan pass never examines it.
const blobDirLockName = ".hoarddb-blob-dir.lock"

// nodeOwnedDirNames are the data directory's entries that other parts of the
// node own and manage: the store, the keys, the write-ahead log and the
// cluster state. A payload tree inside one of them sits in a directory
// something else creates, scans or removes files in.
var nodeOwnedDirNames = []string{nodeStoreDirName, "keys", "wal", "cluster"}

// storageNameRefusal is the refusal for a database name that is the name of
// one of this node's storage directories, or "" when the name is free. Both
// create database and create bucket (whose database is created implicitly)
// ask it, so the two cannot disagree.
//
// No database name reaches the filesystem — a database is keys
// in the node store — so this no longer protects a directory from a drop. It
// keeps a database from being named like the node's own storage, and it names
// the path the operator configured, so a refusal of "big-blobs" points at
// /mnt/big-blobs rather than at a "blobs" that is not there.
func (s *Server) storageNameRefusal(name string) string {
	for _, dir := range s.storageDirs() {
		if dir.Name == name {
			return dir.Refusal
		}
	}
	return ""
}

// storageDirs lists the database names this node's storage directories take, each with the
// refusal naming the directory the operator configured. It is the storage half of the
// shared name policy; the ring node is given the same list.
func (s *Server) storageDirs() []namepolicy.StorageDir {
	dirs := []namepolicy.StorageDir{{
		Name: nodeStoreDirName,
		Refusal: fmt.Sprintf("database name %q is reserved: it is the name of this node's store directory, %s; choose another name",
			nodeStoreDirName, filepath.Join(s.config.Storage.DataDir, nodeStoreDirName)),
	}}
	root := s.blobRoot()
	refusal := fmt.Sprintf("database name %q is reserved: it is the name of this node's blob directory, %s; choose another name", filepath.Base(root), root)
	if s.config.Storage.BlobDir != "" {
		refusal = fmt.Sprintf("database name %q is reserved: it is the name of this node's blob directory, %s (storage.blob_dir); choose another name", filepath.Base(root), root)
	}
	return append(dirs, namepolicy.StorageDir{Name: filepath.Base(root), Refusal: refusal})
}

// prepareBlobDir checks a configured storage.blob_dir, creates it and takes
// its lock, which the node holds until it stops. With blob_dir unset it
// returns a nil lock and does nothing: the default tree is inside the data
// directory, which the node store's own directory lock already holds
// exclusively, and zero-config start stays exactly what it was.
func prepareBlobDir(storage config.StorageConfig) (*blobDirLock, error) {
	if storage.BlobDir == "" {
		return nil, nil
	}
	blobDir, err := filepath.Abs(storage.BlobDir)
	if err != nil {
		return nil, fmt.Errorf("resolving storage.blob_dir %q: %w", storage.BlobDir, err)
	}
	dataDir, err := filepath.Abs(storage.DataDir)
	if err != nil {
		return nil, fmt.Errorf("resolving storage.data_dir %q: %w", storage.DataDir, err)
	}
	if blobDir == dataDir {
		return nil, fmt.Errorf("storage.blob_dir (HOARDDB_BLOB_DIR) %s is the data directory itself; point it at a directory of its own, such as the mount you want large payloads on, or unset it to keep them in %s",
			blobDir, filepath.Join(dataDir, blobRootDirName))
	}
	for _, owned := range nodeOwnedDirNames {
		ownedDir := filepath.Join(dataDir, owned)
		if pathWithin(ownedDir, blobDir) {
			return nil, fmt.Errorf("storage.blob_dir (HOARDDB_BLOB_DIR) %s is inside %s, which this node's %s owns; point it outside the data directory's %s directories, or unset it to keep payloads in %s",
				blobDir, ownedDir, owned, strings.Join(nodeOwnedDirNames, ", "), filepath.Join(dataDir, blobRootDirName))
		}
	}
	if err := os.MkdirAll(blobDir, 0o700); err != nil {
		return nil, fmt.Errorf("creating storage.blob_dir (HOARDDB_BLOB_DIR) %s: %w; create it and make it writable by the user running hoardDB, or point the setting elsewhere", blobDir, err)
	}
	return lockBlobDir(blobDir)
}

// pathWithin reports whether path is dir or lies below it.
func pathWithin(dir, path string) bool {
	rel, err := filepath.Rel(dir, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}
