package server

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestCreateDatabaseWritesNoDirectory: Every bucket is a key range
// in <data dir>/store, so a database needs nothing on disk; the directory the
// create still made listed every database name in plaintext, world-readable,
// on a sealed node too.
func TestCreateDatabaseWritesNoDirectory(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase: %+v, %v", resp, err)
	}
	if _, err := os.Stat(filepath.Join(s.config.Storage.DataDir, "app")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("creating database %q wrote <data dir>/app (stat: %v); nothing outside the store is needed", "app", err)
	}
}

// TestDropDatabaseDeletesNothingOfTheNode: A drop ran
// os.RemoveAll(<data dir>/<name>), and only "store" and "blobs" were refused
// as database names, so `create database keys; drop database keys` deleted the
// node's keys directory — its TLS key and, on a sealed node, the wrapped node
// master and store key — and reported success. No database name may reach a
// file of the node's own.
func TestDropDatabaseDeletesNothingOfTheNode(t *testing.T) {
	for _, name := range []string{"keys", "wal", "cluster"} {
		t.Run(name, func(t *testing.T) {
			s := newTestServer(t)
			sentinel := filepath.Join(s.config.Storage.DataDir, name, "node-owned")
			if err := os.MkdirAll(filepath.Dir(sentinel), 0o700); err != nil {
				t.Fatalf("making the node's %s directory: %v", name, err)
			}
			if err := os.WriteFile(sentinel, []byte("the node's own file"), 0o600); err != nil {
				t.Fatalf("writing %s: %v", sentinel, err)
			}
			created, err := s.CreateDatabase(name)
			if err != nil {
				t.Fatalf("CreateDatabase(%q): %v", name, err)
			}
			if created.Success {
				if _, err := s.DeleteDatabase(DeleteDatabaseRequest{Name: name}); err != nil {
					t.Fatalf("DeleteDatabase(%q): %v", name, err)
				}
			}
			if _, err := os.Stat(sentinel); err != nil {
				t.Errorf("dropping database %q deleted the node's own %s: %v", name, sentinel, err)
			}
		})
	}
}
