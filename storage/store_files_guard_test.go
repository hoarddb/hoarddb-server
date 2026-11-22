package storage

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v4/fb"
	"github.com/dgraph-io/badger/v4/y"
)

// forceZeroIndexChecksum rewrites a plaintext table so its index checksum is a
// CRC32C of 0, which Badger's builder marshals as an empty message (a proto3
// zero value), giving a footer whose checksum length is 0. A real table does
// this about once in 4.3 billion, so the test forges one: the index is a
// flatbuffer, UncompressedSize is a 4-byte statistic Badger never reads back
// on open, and CRC32 is affine over GF(2), so the 32 bits of that field can be
// solved for the value that makes the CRC 0.
func forceZeroIndexChecksum(t *testing.T, tableBytes []byte) []byte {
	t.Helper()
	checksumLength := int(binary.BigEndian.Uint32(tableBytes[len(tableBytes)-4:]))
	indexLengthAt := len(tableBytes) - 4 - checksumLength - 4
	indexLength := int(binary.BigEndian.Uint32(tableBytes[indexLengthAt:]))
	indexStart := indexLengthAt - indexLength
	index := append([]byte(nil), tableBytes[indexStart:indexLengthAt]...)

	tableIndex := fb.GetRootAsTableIndex(index, 0)
	crcWith := func(sizeField uint32) uint32 {
		if !tableIndex.MutateUncompressedSize(sizeField) {
			t.Fatal("the table index has no UncompressedSize field to forge")
		}
		return crc32.Checksum(index, y.CastagnoliCrcTable)
	}
	base := crcWith(0)
	var columns [32]uint32
	for bit := range columns {
		columns[bit] = crcWith(1<<bit) ^ base
	}
	// Solve sum(columns[bit] for each set bit of v) == base, by elimination.
	var pivots [32]uint32 // pivots[b]: a reduced column whose highest set bit is b
	var pivotBits [32]uint32
	for bit, column := range columns {
		reduced, combination := column, uint32(1)<<bit
		for high := 31; high >= 0 && reduced != 0; high-- {
			if reduced&(1<<high) == 0 {
				continue
			}
			if pivots[high] == 0 {
				pivots[high], pivotBits[high] = reduced, combination
				break
			}
			reduced ^= pivots[high]
			combination ^= pivotBits[high]
		}
	}
	var solution uint32
	remaining := base
	for high := 31; high >= 0; high-- {
		if remaining&(1<<high) != 0 {
			if pivots[high] == 0 {
				t.Fatal("the CRC of this index cannot be forced to 0")
			}
			remaining ^= pivots[high]
			solution ^= pivotBits[high]
		}
	}
	if forgedCRC := crcWith(solution); forgedCRC != 0 {
		t.Fatalf("forging failed: CRC32C is %d", forgedCRC)
	}

	forged := append([]byte(nil), tableBytes[:indexStart]...)
	forged = append(forged, index...)
	forged = binary.BigEndian.AppendUint32(forged, uint32(indexLength))
	// No checksum message at all: pb.Checksum{CRC32C, 0} marshals to zero
	// bytes, and its length field follows.
	return binary.BigEndian.AppendUint32(forged, 0)
}

// TestATableWhoseIndexChecksumIsZeroStillOpens defends the zero-checksum case: a
// table written with a CRC of 0 has a zero-length checksum message in its
// footer, which Badger reads as a valid checksum of 0 and the guard refused.
func TestATableWhoseIndexChecksumIsZeroStillOpens(t *testing.T) {
	dir := writeClosedStore(t, nil)
	tablePath := tableFiles(t, dir)[0]
	tableBytes, err := os.ReadFile(tablePath)
	if err != nil {
		t.Fatalf("reading %s: %v", tablePath, err)
	}
	forged := forceZeroIndexChecksum(t, tableBytes)
	if footerChecksumLength := binary.BigEndian.Uint32(forged[len(forged)-4:]); footerChecksumLength != 0 {
		t.Fatalf("the forged footer has a checksum of %d bytes, want 0", footerChecksumLength)
	}
	if err := os.WriteFile(tablePath, forged, 0o600); err != nil {
		t.Fatalf("writing %s: %v", tablePath, err)
	}

	outcome := reopenInChild(t, dir, nil)
	if outcome.panicked || outcome.openError != "" {
		t.Fatalf("a table with a zero index checksum did not open: %s%s", outcome.openError, outcome.helperOutput)
	}
	if outcome.found != corruptDocumentCount {
		t.Fatalf("found %d of %d documents", outcome.found, corruptDocumentCount)
	}
}
