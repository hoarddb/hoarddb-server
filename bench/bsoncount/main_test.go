package main

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// bsonDocument encodes {_id: int32, v: string} by hand, so the test needs no BSON library.
func bsonDocument(id int32, text string) []byte {
	var body bytes.Buffer
	body.WriteByte(0x10)
	body.WriteString("_id\x00")
	_ = binary.Write(&body, binary.LittleEndian, id)
	body.WriteByte(0x02)
	body.WriteString("v\x00")
	_ = binary.Write(&body, binary.LittleEndian, int32(len(text)+1))
	body.WriteString(text + "\x00")
	body.WriteByte(0x00)
	var document bytes.Buffer
	_ = binary.Write(&document, binary.LittleEndian, int32(body.Len()+4))
	document.Write(body.Bytes())
	return document.Bytes()
}

func writeFixture(t *testing.T, path string, documents int) {
	t.Helper()
	var contents []byte
	for id := 0; id < documents; id++ {
		contents = append(contents, bsonDocument(int32(id), "text")...)
	}
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("writing fixture %s: %v", path, err)
	}
}

// TestCountsDocumentsPerFile counts the documents of each .bson file of a directory by name.
func TestCountsDocumentsPerFile(t *testing.T) {
	directory := t.TempDir()
	writeFixture(t, filepath.Join(directory, "users.bson"), 5)
	writeFixture(t, filepath.Join(directory, "orders.bson"), 11)
	writeFixture(t, filepath.Join(directory, "empty.bson"), 0)
	if err := os.WriteFile(filepath.Join(directory, "users.metadata.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run([]string{directory}, &output); err != nil {
		t.Fatalf("run: %v", err)
	}
	want := "empty\t0\norders\t11\nusers\t5\n"
	if output.String() != want {
		t.Errorf("output = %q, want %q", output.String(), want)
	}
}

// TestRefusesTruncatedFile names the file and the byte offset of a document that runs past the end.
func TestRefusesTruncatedFile(t *testing.T) {
	directory := t.TempDir()
	contents := bsonDocument(1, "text")
	if err := os.WriteFile(filepath.Join(directory, "cut.bson"), contents[:len(contents)-3], 0o600); err != nil {
		t.Fatal(err)
	}
	err := run([]string{directory}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "cut.bson") || !strings.Contains(err.Error(), "offset 0") {
		t.Fatalf("error = %v, want one naming cut.bson and offset 0", err)
	}
}

// TestRefusesZeroLengthPrefix: a length prefix below the smallest document would never advance the
// offset; it must be refused, naming the offset, and never loop.
func TestRefusesZeroLengthPrefix(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "zero.bson"), make([]byte, 16), 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- run([]string{directory}, &bytes.Buffer{}) }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "zero.bson") || !strings.Contains(err.Error(), "offset 0") {
			t.Fatalf("error = %v, want one naming zero.bson and offset 0", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return: a zero length prefix loops forever")
	}
}

// TestPrintsNoDocumentContent keeps the output to names and counts.
func TestPrintsNoDocumentContent(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "secret.bson"), bsonDocument(1, "SENTINEL-VALUE"), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run([]string{directory}, &output); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "SENTINEL") {
		t.Errorf("output holds document content: %q", output.String())
	}
}
