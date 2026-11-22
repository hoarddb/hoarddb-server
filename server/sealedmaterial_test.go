package server

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// v2SegmentHeader is a format 2 WAL segment header (spec §3.1), built by hand
// so this package does not reach into storage: the v1 sealed signature, flags
// 0x01 for format 2 plus 0x02 when the entries are sealed, a reserved zero,
// the epoch and a non-zero generation.
func v2SegmentHeader(flags byte, epoch uint64) []byte {
	header := []byte{0x00, 0x48, 0x44, 0x42, 0x02, 0x04, flags, 0x00}
	header = binary.BigEndian.AppendUint64(header, epoch)
	return binary.BigEndian.AppendUint64(header, 0x5eed5eed5eed5eed)
}

// TestFindSealedMaterialSeesV2SealedSegmentsOnly: a v2 segment counts as sealed
// material when its flags say its entries are sealed (0x03), and never when they
// are plain (0x01), whose header is the same 24 bytes. Fails on the parent:
// findSealedWALSegments parses with sealed.ParseFileHeader, which refuses every
// v2 header, so a sealed v2 log reads as holding no sealed material (spec §5).
func TestFindSealedMaterialSeesV2SealedSegmentsOnly(t *testing.T) {
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, walDirName)
	if err := os.MkdirAll(walDir, 0o700); err != nil {
		t.Fatalf("creating the WAL directory: %v", err)
	}
	write := func(name string, header []byte) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(walDir, name), append(header, make([]byte, 64)...), 0o600); err != nil {
			t.Fatalf("writing %s: %v", name, err)
		}
	}
	write("wal_00001.log", v2SegmentHeader(0x01, 0))

	found, err := findSealedWALSegments(filepath.Join(dataDir, walDirName), walDirName)
	if err != nil {
		t.Fatalf("findSealedWALSegments over a plain v2 segment: %v", err)
	}
	if len(found) != 0 {
		t.Fatalf("a plain v2 segment was reported as sealed material: %v", found)
	}

	write("wal_00002.log", v2SegmentHeader(0x03, 7))
	found, err = findSealedWALSegments(filepath.Join(dataDir, walDirName), walDirName)
	if err != nil {
		t.Fatalf("findSealedWALSegments over a sealed v2 segment: %v", err)
	}
	if len(found) != 1 || found[0] != "wal/wal_00002.log (sealed)" {
		t.Fatalf("findSealedWALSegments = %v, want only the sealed v2 segment wal/wal_00002.log", found)
	}
}

// TestFindSealedMaterialSkipsTheSpareNames: a WAL segment is born as
// wal_spare.tmp, and a preallocated spare waits as wal_spare.ready, each with a
// full v2 header. Neither is a segment, so neither counts as sealed material,
// however sealed its header looks (spec §2.1). Break by: widening
// walSegmentGlob to every wal_ file.
func TestFindSealedMaterialSkipsTheSpareNames(t *testing.T) {
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, walDirName)
	if err := os.MkdirAll(walDir, 0o700); err != nil {
		t.Fatalf("creating the WAL directory: %v", err)
	}
	for _, name := range []string{"wal_spare.tmp", "wal_spare.ready"} {
		if err := os.WriteFile(filepath.Join(walDir, name), append(v2SegmentHeader(0x03, 7), make([]byte, 64)...), 0o600); err != nil {
			t.Fatalf("writing %s: %v", name, err)
		}
	}
	found, err := findSealedWALSegments(filepath.Join(dataDir, walDirName), walDirName)
	if err != nil {
		t.Fatalf("findSealedWALSegments beside the spares: %v", err)
	}
	if len(found) != 0 {
		t.Fatalf("findSealedWALSegments listed a spare as sealed material: %v", found)
	}
}
