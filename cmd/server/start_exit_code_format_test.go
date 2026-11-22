package main

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/storage"
)

// TestFormat5DirectoryIsRefused is the server half of the prefix bloom spec's T13: the open of
// a ring directory written in an earlier format fails the start, and the start exits with the
// "wrong build, do not restart" status, like a cluster version this build cannot run. A
// restart cannot change what the directory holds, and from format 6 on every directory of an
// earlier build meets this refusal, so exit 1 would make a service manager loop on it.
// Against: leaving a ring format refusal at exit 1.
func TestFormat5DirectoryIsRefused(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ring")
	if err := os.MkdirAll(filepath.Join(dir, "badger"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "badger", "000001.sst"), []byte("a table without prefix hashes"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "FORMAT"), []byte("hoarddb ring format 5\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	node, err := wire.OpenNode(ctx, wire.NodeConfig{Dir: dir})
	if err == nil {
		_ = node.Close()
		t.Fatalf("OpenNode(%s) opened a format 5 directory", dir)
	}
	if got := startFailureExitCode(err); got != cluster.DivergedExitCode {
		t.Errorf("a start refused for the ring format (%v) exits %d, want %d", err, got, cluster.DivergedExitCode)
	}
}

// TestAMarkerOfAnotherFormatExitsAsAWrongBuild: a ring store whose sync marker another build wrote
// (a whole marker of another format) is the same "wrong build, do not restart" case as a ring
// directory of another format (store-vouched-cut review R-5), so the start exits 70 and a service
// manager does not loop on it. Against: leaving the marker's format refusal at exit 1.
func TestAMarkerOfAnotherFormatExitsAsAWrongBuild(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ring")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	node, err := wire.OpenNode(ctx, wire.NodeConfig{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	// The 32-byte record of an earlier build (format 1), which every seed made before the in-place
	// marker (format 2) carries.
	path := filepath.Join(dir, "badger", "VOUCHED")
	raw := make([]byte, 32)
	copy(raw, "hdbvouch")
	binary.BigEndian.PutUint32(raw[8:], 1)
	binary.BigEndian.PutUint64(raw[12:], 1)
	binary.BigEndian.PutUint64(raw[20:], 1)
	binary.BigEndian.PutUint32(raw[28:], crc32.Checksum(raw[:28], crc32.MakeTable(crc32.Castagnoli)))
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	node, err = wire.OpenNode(ctx, wire.NodeConfig{Dir: dir})
	if err == nil {
		_ = node.Close()
		t.Fatalf("OpenNode(%s) opened a store whose marker another build wrote", dir)
	}
	var otherFormat *storage.VouchedMarkerFormatError
	if !errors.As(err, &otherFormat) || otherFormat.Format != 1 {
		t.Fatalf("the refusal is not the marker's format refusal naming format 1: %v", err)
	}
	if got := startFailureExitCode(err); got != cluster.DivergedExitCode {
		t.Errorf("a start refused for the marker's format (%v) exits %d, want %d", err, got, cluster.DivergedExitCode)
	}
}
