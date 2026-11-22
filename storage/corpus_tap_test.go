//go:build bsoncorpus

package storage_test

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// tapRecord is one line of $BSONCORPUS_OUT/records-<pid>.jsonl.
type tapRecord struct {
	KeyPrefix string `json:"keyPrefix"`
	Value     []byte `json:"value"`
}

func readTapRecords(t *testing.T, outputDir string) []tapRecord {
	t.Helper()
	recordFiles, err := filepath.Glob(filepath.Join(outputDir, fmt.Sprintf("records-%d.jsonl", os.Getpid())))
	if err != nil {
		t.Fatalf("listing the tap files: %v", err)
	}
	var records []tapRecord
	for _, recordFile := range recordFiles {
		opened, err := os.Open(recordFile)
		if err != nil {
			t.Fatalf("opening %s: %v", recordFile, err)
		}
		scanner := bufio.NewScanner(opened)
		scanner.Buffer(make([]byte, 0, 1<<20), 64<<20)
		for scanner.Scan() {
			var record tapRecord
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				t.Fatalf("parsing a tap line: %v", err)
			}
			records = append(records, record)
		}
		if err := scanner.Err(); err != nil {
			t.Fatalf("reading %s: %v", recordFile, err)
		}
		_ = opened.Close()
	}
	return records
}

// bsonDocumentWithInt32 is {"a": <number>} as BSON, 12 bytes.
func bsonDocumentWithInt32(number byte) []byte {
	return []byte{0x0c, 0, 0, 0, 0x10, 'a', 0, number, 0, 0, 0, 0}
}

func TestCorpusTapRecordsEveryDocumentValue(t *testing.T) {
	outputDir := t.TempDir()
	t.Setenv("BSONCORPUS_OUT", outputDir)

	kv, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the Badger store: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })

	documentKey := storage.Key(7, 1, 42, storage.NSDocuments, []byte("user-1"))
	arrayFramedKey := storage.Key(7, 1, 42, storage.NSQueue, []byte("counted"))
	plainKey := storage.Key(7, 1, 42, storage.NSIndex, []byte("plain"))
	document := bsonDocumentWithInt32(5)
	// A count-prefixed list is framed like an array of records, not like one
	// document: its leading int32 is the element count, not the byte length.
	arrayFramed := append([]byte{0x02, 0, 0, 0}, document...)

	batch := kv.NewBatch()
	for _, staged := range []struct{ key, value []byte }{
		{documentKey, document},
		{arrayFramedKey, arrayFramed},
		{plainKey, []byte("not bson at all")},
	} {
		if err := batch.Set(staged.key, staged.value); err != nil {
			t.Fatalf("staging %x: %v", staged.key, err)
		}
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("committing: %v", err)
	}

	records := readTapRecords(t, outputDir)
	if len(records) != 1 {
		t.Fatalf("the tap recorded %d values, want exactly the one document: %+v", len(records), records)
	}
	if !bytes.Equal(records[0].Value, document) {
		t.Errorf("recorded value = %x, want %x", records[0].Value, document)
	}
	wantPrefix := hex.EncodeToString(documentKey[:12])
	if records[0].KeyPrefix != wantPrefix {
		t.Errorf("recorded key prefix = %s, want the first 12 bytes of the key, %s", records[0].KeyPrefix, wantPrefix)
	}
}

func TestCorpusTapRecordsInstancePuts(t *testing.T) {
	outputDir := t.TempDir()
	t.Setenv("BSONCORPUS_OUT", outputDir)

	instance, err := storage.OpenBadger(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the Badger instance: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close() })

	document := bsonDocumentWithInt32(9)
	if err := instance.Put(storage.Key(1, 1, 2, storage.NSDocuments, []byte("k")), document); err != nil {
		t.Fatalf("Put of a document: %v", err)
	}
	if err := instance.Put([]byte("short"), []byte("xx")); err != nil {
		t.Fatalf("Put of a non-document: %v", err)
	}
	records := readTapRecords(t, outputDir)
	if len(records) != 1 || !bytes.Equal(records[0].Value, document) {
		t.Fatalf("the tap recorded %+v, want exactly the document %x", records, document)
	}
}

func TestCorpusTapFramingCheck(t *testing.T) {
	cases := []struct {
		name   string
		value  []byte
		record bool
	}{
		{"empty document", []byte{5, 0, 0, 0, 0}, true},
		{"one field", bsonDocumentWithInt32(1), true},
		{"four bytes", []byte{4, 0, 0, 0}, false},
		{"length prefix too large", []byte{0x0d, 0, 0, 0, 0x10, 'a', 0, 1, 0, 0, 0, 0}, false},
		{"length prefix too small", []byte{0x0b, 0, 0, 0, 0x10, 'a', 0, 1, 0, 0, 0, 0}, false},
		{"last byte not zero", []byte{0x0c, 0, 0, 0, 0x10, 'a', 0, 1, 0, 0, 0, 1}, false},
		{"empty value", nil, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			outputDir := t.TempDir()
			t.Setenv("BSONCORPUS_OUT", outputDir)
			instance, err := storage.OpenBadger(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
			if err != nil {
				t.Fatalf("opening the Badger instance: %v", err)
			}
			t.Cleanup(func() { _ = instance.Close() })
			if err := instance.Put([]byte("framing-key"), testCase.value); err != nil {
				t.Fatalf("Put: %v", err)
			}
			if recorded := len(readTapRecords(t, outputDir)) == 1; recorded != testCase.record {
				t.Errorf("recorded = %v, want %v for %x", recorded, testCase.record, testCase.value)
			}
		})
	}
}
