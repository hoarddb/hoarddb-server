package cli

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// dumpFileRecords splits a dump file's bytes into its records by the int32 length
// each BSON record starts with. It is test-local because the streaming reader of F3
// does not exist yet, and a test of the writer must not depend on it.
func dumpFileRecords(t *testing.T, fileBytes []byte) []bson.Raw {
	t.Helper()
	if !bytes.HasPrefix(fileBytes, []byte(dumpMagic)) {
		t.Fatalf("a dump file must start with the magic %q; it starts with %q", dumpMagic, fileBytes[:min(len(fileBytes), len(dumpMagic))])
	}
	rest := fileBytes[len(dumpMagic):]
	var records []bson.Raw
	for len(rest) > 0 {
		if len(rest) < 4 {
			t.Fatalf("a record at offset %d has %d bytes and a record's length prefix is 4", len(fileBytes)-len(rest), len(rest))
		}
		length := int(binary.LittleEndian.Uint32(rest[:4]))
		if length < 5 || length > len(rest) {
			t.Fatalf("a record at offset %d declares %d bytes and %d remain", len(fileBytes)-len(rest), length, len(rest))
		}
		records = append(records, bson.Raw(rest[:length]))
		rest = rest[length:]
	}
	return records
}

// dumpTestHeader is a valid header record: a decode of the summary and footer the
// writer appends is only meaningful in a file whose first record decodes.
func dumpTestHeader() *dumpHeader {
	return &dumpHeader{
		T:           dumpKindHeader,
		V:           dumpFormatVersion,
		Created:     time.Date(2026, 11, 20, 9, 30, 15, 0, time.UTC),
		Tool:        "hoarddb test",
		User:        "alice",
		Scope:       "all",
		Consistency: dumpConsistencyWord,
	}
}

// TestWriterRefusesAnExistingFinalFile pins rule 10: a final name is never
// overwritten, and the refusal names the file without its directory.
func TestWriterRefusesAnExistingFinalFile(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "app.hoarddump")
	existing := []byte("a dump taken earlier")
	if err := os.WriteFile(finalPath, existing, 0o600); err != nil {
		t.Fatalf("writing the existing file: %v", err)
	}

	writer, err := openDumpWriter(dir, "app.hoarddump")
	if writer != nil {
		if abortErr := writer.Abort(); abortErr != nil {
			t.Fatalf("aborting the unexpected writer: %v", abortErr)
		}
		t.Fatalf("openDumpWriter returned a writer for an existing final file")
	}
	want := "File app.hoarddump already exists. Pick another --out, or remove it"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("openDumpWriter over an existing file returned %v, want the text %q", err, want)
	}

	unchanged, readErr := os.ReadFile(finalPath)
	if readErr != nil {
		t.Fatalf("reading the existing file back: %v", readErr)
	}
	if !bytes.Equal(unchanged, existing) {
		t.Fatalf("the existing file changed from %q to %q", existing, unchanged)
	}
	if _, statErr := os.Stat(finalPath + ".partial"); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("a refused open left a partial file: %v", statErr)
	}
}

// TestAbortLeavesNoFile pins that a failed or interrupted dump leaves nothing an
// operator could restore from, and that Abort is safe to call twice.
func TestAbortLeavesNoFile(t *testing.T) {
	dir := t.TempDir()
	writer, err := openDumpWriter(dir, "app.hoarddump")
	if err != nil {
		t.Fatalf("opening a dump writer: %v", err)
	}
	if err := writer.WriteRecord(&dumpDB{T: dumpKindDB, Name: "app", RF: 1, Storage: "disk"}); err != nil {
		t.Fatalf("writing a record: %v", err)
	}
	if err := writer.Abort(); err != nil {
		t.Fatalf("aborting: %v", err)
	}
	if err := writer.Abort(); err != nil {
		t.Fatalf("a second Abort must be safe, it returned: %v", err)
	}
	for _, path := range []string{filepath.Join(dir, "app.hoarddump"), filepath.Join(dir, "app.hoarddump.partial")} {
		if _, statErr := os.Stat(path); !errors.Is(statErr, fs.ErrNotExist) {
			t.Fatalf("Abort left %s behind: %v", path, statErr)
		}
	}
}

// TestPartialNameIsUsedUntilClose pins that the final name appears only after the
// footer is written and the file is synced.
func TestPartialNameIsUsedUntilClose(t *testing.T) {
	dir := t.TempDir()
	writer, err := openDumpWriter(dir, "app.hoarddump")
	if err != nil {
		t.Fatalf("opening a dump writer: %v", err)
	}
	if err := writer.WriteRecord(dumpTestHeader()); err != nil {
		t.Fatalf("writing the header: %v", err)
	}

	finalPath := filepath.Join(dir, "app.hoarddump")
	partialPath := finalPath + ".partial"
	if _, statErr := os.Stat(partialPath); statErr != nil {
		t.Fatalf("while writing, the partial file must exist: %v", statErr)
	}
	if _, statErr := os.Stat(finalPath); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("while writing, the final file must not exist: %v", statErr)
	}

	if err := writer.Close(dumpCounters{Databases: 1, Buckets: 1, Entities: 1, Elements: 0}); err != nil {
		t.Fatalf("closing the dump writer: %v", err)
	}
	if _, statErr := os.Stat(finalPath); statErr != nil {
		t.Fatalf("after Close, the final file must exist: %v", statErr)
	}
	if _, statErr := os.Stat(partialPath); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("after Close, the partial file must be gone: %v", statErr)
	}
}

// TestFileModeIs0600 pins that a dump holds its documents for its owner only.
// createPrivateFile sets the mode explicitly, so the process umask cannot widen it;
// the test reads the mode of both names rather than changing a process-wide umask.
func TestFileModeIs0600(t *testing.T) {
	dir := t.TempDir()
	writer, err := openDumpWriter(dir, "app.hoarddump")
	if err != nil {
		t.Fatalf("opening a dump writer: %v", err)
	}
	partialPath := filepath.Join(dir, "app.hoarddump.partial")
	partialInfo, statErr := os.Stat(partialPath)
	if statErr != nil {
		t.Fatalf("statting the partial file: %v", statErr)
	}
	if got := partialInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("the partial file has mode %#o, want 0600", got)
	}

	if err := writer.Close(dumpCounters{}); err != nil {
		t.Fatalf("closing the dump writer: %v", err)
	}
	finalInfo, statErr := os.Stat(filepath.Join(dir, "app.hoarddump"))
	if statErr != nil {
		t.Fatalf("statting the final file: %v", statErr)
	}
	if got := finalInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("the final file has mode %#o, want 0600", got)
	}
}

// TestSummaryIsInsideTheChecksum pins the two-record tail of the file: the summary's
// counters are inside the bytes the footer's sha256 covers, the footer record starts
// exactly at the covered byte count, and the footer is not itself covered.
func TestSummaryIsInsideTheChecksum(t *testing.T) {
	dir := t.TempDir()
	writer, err := openDumpWriter(dir, "app.hoarddump")
	if err != nil {
		t.Fatalf("opening a dump writer: %v", err)
	}
	scalars, err := bson.Marshal(bson.D{{Key: "Name", Value: "Ada"}})
	if err != nil {
		t.Fatalf("marshalling a document: %v", err)
	}
	written := []any{
		dumpTestHeader(),
		&dumpDB{T: dumpKindDB, Name: "app", RF: 1, Storage: "disk"},
		&dumpDoc{T: dumpKindDoc, DB: "app", B: "User", ID: "ada", F: bson.Raw(scalars)},
	}
	for _, record := range written {
		if err := writer.WriteRecord(record); err != nil {
			t.Fatalf("writing %T: %v", record, err)
		}
	}
	counters := dumpCounters{Databases: 1, Buckets: 1, Entities: 1, Elements: 0}
	if err := writer.Close(counters); err != nil {
		t.Fatalf("closing the dump writer: %v", err)
	}

	filePath := filepath.Join(dir, "app.hoarddump")
	fileBytes, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("reading the dump back: %v", err)
	}
	records := dumpFileRecords(t, fileBytes)
	if len(records) != len(written)+2 {
		t.Fatalf("the file holds %d records and the writer wrote %d plus a summary and a footer", len(records), len(written))
	}

	footerRaw := records[len(records)-1]
	footerDecoded, err := decodeRecord(footerRaw)
	if err != nil {
		t.Fatalf("decoding the footer: %v", err)
	}
	footer, ok := footerDecoded.(*dumpFooter)
	if !ok {
		t.Fatalf("the last record decoded to %T, want a footer", footerDecoded)
	}

	summaryRaw := records[len(records)-2]
	summaryDecoded, err := decodeRecord(summaryRaw)
	if err != nil {
		t.Fatalf("decoding the summary: %v", err)
	}
	summary, ok := summaryDecoded.(*dumpSummaryRecord)
	if !ok {
		t.Fatalf("the second-to-last record decoded to %T, want a summary", summaryDecoded)
	}
	wantSummary := &dumpSummaryRecord{
		T:         dumpKindSummary,
		Records:   int64(len(written)),
		Databases: counters.Databases,
		Buckets:   counters.Buckets,
		Entities:  counters.Entities,
		Elements:  counters.Elements,
	}
	if !reflect.DeepEqual(summary, wantSummary) {
		t.Fatalf("the summary is %+v, want %+v", summary, wantSummary)
	}

	summaryOffset := int64(len(fileBytes) - len(footerRaw) - len(summaryRaw))
	footerOffset := int64(len(fileBytes) - len(footerRaw))
	if footer.Bytes != footerOffset {
		t.Fatalf("the footer says its checksum covers %d bytes and the footer record starts at %d", footer.Bytes, footerOffset)
	}
	if summaryOffset < int64(len(dumpMagic)) || summaryOffset+int64(len(summaryRaw)) > footer.Bytes {
		t.Fatalf("the summary at %d with %d bytes is not inside the covered bytes [%d, %d)", summaryOffset, len(summaryRaw), len(dumpMagic), footer.Bytes)
	}
	digest := sha256.Sum256(fileBytes[:footer.Bytes])
	if !bytes.Equal(footer.SHA256, digest[:]) {
		t.Fatalf("the footer's sha256 is %x and the covered bytes hash to %x", footer.SHA256, digest)
	}
}

// TestWriterRefusesARecordTheReaderWouldRefuse pins that a record the reader would
// refuse is never written: an oversize record returns the refusal and writes no bytes,
// and the writer still completes a whole file afterwards.
func TestWriterRefusesARecordTheReaderWouldRefuse(t *testing.T) {
	dir := t.TempDir()
	writer, err := openDumpWriter(dir, "app.hoarddump")
	if err != nil {
		t.Fatalf("opening a dump writer: %v", err)
	}
	oversize := &dumpDoc{T: dumpKindDoc, DB: "app", B: "User", ID: "ada", F: bsonDocumentOfSize(t, maxDumpRecordBytes+1)}
	if err := writer.WriteRecord(oversize); err == nil || !strings.Contains(err.Error(), "it was not written") {
		t.Fatalf("an oversize record gave %v, want the refusal", err)
	}
	info, err := os.Stat(filepath.Join(dir, "app.hoarddump.partial"))
	if err != nil {
		t.Fatalf("statting the partial: %v", err)
	}
	if info.Size() != int64(len(dumpMagic)) {
		t.Fatalf("the partial holds %d bytes after a refused record, want only the %d of the magic", info.Size(), len(dumpMagic))
	}
	if err := writer.Close(dumpCounters{}); err != nil {
		t.Fatalf("closing after a refused record: %v", err)
	}
}
