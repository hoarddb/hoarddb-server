package main

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func prefixHex(slot uint16, bucketID uint64, namespace byte, kind byte) string {
	prefix := make([]byte, 12)
	prefix[0], prefix[1] = byte(slot>>8), byte(slot)
	for position := 0; position < 8; position++ {
		prefix[2+position] = byte(bucketID >> (8 * (7 - position)))
	}
	prefix[10], prefix[11] = namespace, kind
	return hex.EncodeToString(prefix)
}

func TestClassifyKeyPrefix(t *testing.T) {
	cases := []struct {
		name       string
		prefix     string
		wantClass  string
		wantLimit  int
		wantRecord bool
	}{
		{"hash document", prefixHex(5, 9, 0x01, 'k'), "documents", 100, true},
		{"queue entry", prefixHex(5, 9, 0x04, 0), "queue", 100, true},
		{"bucket state", prefixHex(5, 9, 0x05, 0), "state", 100, true},
		{"bucket state, bare 11-byte key", prefixHex(5, 9, 0x05, 0)[:22], "state", 100, true},
		{"blob metadata", prefixHex(5, 9, 0x06, 0), "blobmeta", 100, true},
		{"field store element", prefixHex(5, 9, 0x07, 0), "fieldstore", 100, true},
		{"claimed delivery", prefixHex(5, 9, 0x0D, 'e'), "delivery-claimed", 100, true},
		{"dead delivery takes the largest dead limit", prefixHex(5, 9, 0x0D, 'x'), "delivery-dead", 102, true},
		{"catalog", prefixHex(0xFFFF, 0, 0x01, 0), "node-catalog", 100, true},
		{"replication position", prefixHex(0xFFFF, 0, 0x0C, 'p'), "node-replication-p", 100, true},
		{"key outside the shared layout", hex.EncodeToString([]byte("k-0001")), "", 0, false},
		{"slot out of range", prefixHex(4096, 1, 0x01, 0), "", 0, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			site, kept := classifyKeyPrefix(testCase.prefix)
			if kept != testCase.wantRecord {
				t.Fatalf("kept = %v, want %v", kept, testCase.wantRecord)
			}
			if site.class != testCase.wantClass || site.depthLimit != testCase.wantLimit {
				t.Errorf("site = %+v, want class %q limit %d", site, testCase.wantClass, testCase.wantLimit)
			}
		})
	}
}

func TestShareOutCapGivesSmallClassesAllTheyHave(t *testing.T) {
	shares := shareOutCap(map[string]int{"a": 10, "b": 100, "c": 1000}, 300)
	want := map[string]int{"a": 10, "b": 100, "c": 190}
	for class, wantShare := range want {
		if shares[class] != wantShare {
			t.Errorf("share of %s = %d, want %d (all shares: %v)", class, shares[class], wantShare, shares)
		}
	}
	allShares := shareOutCap(map[string]int{"a": 5, "b": 5}, 100)
	if allShares["a"] != 5 || allShares["b"] != 5 {
		t.Errorf("a cap above the total changed shares: %v", allShares)
	}
}

func bsonDocument(fieldValue string) []byte {
	// {"a": "<fieldValue>"} as BSON.
	stringLength := binary.LittleEndian.AppendUint32(nil, uint32(len(fieldValue)+1))
	body := append([]byte{0x02, 'a', 0}, stringLength...)
	body = append(body, fieldValue...)
	body = append(body, 0, 0)
	return append(binary.LittleEndian.AppendUint32(nil, uint32(len(body)+4)), body...)
}

func writeRawRecords(t *testing.T, directory string, records []rawRecord) {
	t.Helper()
	var lines bytes.Buffer
	for _, record := range records {
		line, err := json.Marshal(record)
		if err != nil {
			t.Fatalf("encoding a raw record: %v", err)
		}
		lines.Write(line)
		lines.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(directory, "records-1.jsonl"), lines.Bytes(), 0o644); err != nil {
		t.Fatalf("writing raw records: %v", err)
	}
}

func TestBuildDeduplicatesClassifiesAndIsDeterministic(t *testing.T) {
	rawDir := t.TempDir()
	documentsPrefix := prefixHex(3, 7, 0x01, 'k')
	queuePrefix := prefixHex(3, 8, 0x04, 0)
	records := []rawRecord{
		{KeyPrefix: documentsPrefix, Value: bsonDocument("one")},
		{KeyPrefix: documentsPrefix, Value: bsonDocument("one")}, // a repeat
		{KeyPrefix: queuePrefix, Value: bsonDocument("one")},     // same bytes, other class
		{KeyPrefix: documentsPrefix, Value: bsonDocument("two")},
		{KeyPrefix: hex.EncodeToString([]byte("k-0001")), Value: bsonDocument("foreign")},
		{KeyPrefix: documentsPrefix, Value: bsonDocument(strings.Repeat("x", 2000))}, // over the value cap below
	}
	writeRawRecords(t, rawDir, records)

	options := buildOptions{inputDirs: []string{rawDir}, maxRecords: 100, maxGzipBytes: 1 << 20, maxValueBytes: 1000}
	first, firstStats, err := build(options)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	second, _, err := build(options)
	if err != nil {
		t.Fatalf("build again: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Error("two builds of the same input differ")
	}
	if firstStats.Read != 6 || firstStats.Foreign != 1 || firstStats.Oversize != 1 || firstStats.Unique != 2 || firstStats.Kept != 2 {
		t.Errorf("stats = %+v, want read 6, foreign 1, oversize 1, unique 2, kept 2", firstStats)
	}

	reader, err := gzip.NewReader(bytes.NewReader(first))
	if err != nil {
		t.Fatalf("the output is not gzip: %v", err)
	}
	decoder := json.NewDecoder(reader)
	seen := map[string]bool{}
	for decoder.More() {
		var kept corpusRecord
		if err := decoder.Decode(&kept); err != nil {
			t.Fatalf("decoding a kept record: %v", err)
		}
		hash := sha256.Sum256(kept.Value)
		if seen[hex.EncodeToString(hash[:])] {
			t.Errorf("value %s appears twice in the corpus", base64.StdEncoding.EncodeToString(kept.Value))
		}
		seen[hex.EncodeToString(hash[:])] = true
		if kept.Class == "" || kept.DepthLimit == 0 {
			t.Errorf("record without a class or limit: %+v", kept)
		}
	}
	if len(seen) != 2 {
		t.Errorf("the corpus holds %d records, want 2", len(seen))
	}
}

func TestBuildStaysUnderTheRecordCap(t *testing.T) {
	rawDir := t.TempDir()
	var records []rawRecord
	for position := 0; position < 50; position++ {
		records = append(records, rawRecord{KeyPrefix: prefixHex(3, 7, 0x01, 'k'), Value: bsonDocument(fmt.Sprintf("document-%03d", position))})
	}
	writeRawRecords(t, rawDir, records)
	_, stats, err := build(buildOptions{inputDirs: []string{rawDir}, maxRecords: 20, maxGzipBytes: 1 << 20, maxValueBytes: 1000})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if stats.Unique != 50 || stats.Kept != 20 {
		t.Errorf("stats = %+v, want 50 unique and 20 kept", stats)
	}
}
