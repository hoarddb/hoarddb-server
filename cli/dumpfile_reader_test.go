package cli

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// dumpTestFile writes a small dump with the writer and returns its bytes: a header, a
// database, a bucket, one entity with a four-element fifo, a summary and a footer.
func dumpTestFile(t *testing.T) []byte {
	t.Helper()
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
		&dumpBucket{
			T: dumpKindBucket, DB: "app", Name: "User",
			Fields:      []dumpField{{Name: "Name", Type: "string"}, {Name: "Purchases", Type: "fifoStore"}},
			Aggregates:  []dumpAggregate{},
			Unsupported: []string{},
		},
		&dumpDoc{T: dumpKindDoc, DB: "app", B: "User", ID: "ada", F: bson.Raw(scalars)},
		&dumpElems{
			T: dumpKindElems, DB: "app", B: "User", ID: "ada", Field: "Purchases",
			Kind: "fifoStore", Seq: 0,
			Els: []bson.Raw{dumpTestDocument(t, "one"), dumpTestDocument(t, "two")},
		},
	}
	for _, record := range written {
		if err := writer.WriteRecord(record); err != nil {
			t.Fatalf("writing %T: %v", record, err)
		}
	}
	counters := dumpCounters{Databases: 1, Buckets: 1, Entities: 1, Elements: 2}
	if err := writer.Close(counters); err != nil {
		t.Fatalf("closing the dump writer: %v", err)
	}
	fileBytes, err := os.ReadFile(filepath.Join(dir, "app.hoarddump"))
	if err != nil {
		t.Fatalf("reading the dump back: %v", err)
	}
	return fileBytes
}

// readDumpUntilError reads a dump to its end and returns the error that ended it, after
// refusing any record the strict decode refuses.
func readDumpUntilError(t *testing.T, fileBytes []byte) error {
	t.Helper()
	reader := newDumpReader(bytes.NewReader(fileBytes))
	for {
		kind, raw, err := reader.Next()
		if err != nil {
			return err
		}
		record, decodeErr := decodeRecord(raw)
		if decodeErr != nil {
			t.Fatalf("record %q at offset %d does not decode: %v", kind, reader.Offset(), decodeErr)
		}
		if record.kindWord() != kind {
			t.Fatalf("Next returned kind %q and the record decodes to %q", kind, record.kindWord())
		}
	}
}

// TestReaderRefusesAHugeLengthWithoutAllocating pins the bound of section 3.1: a record
// length is refused before any buffer is allocated for it, so a damaged length prefix
// cannot make the reader reserve two gigabytes.
func TestReaderRefusesAHugeLengthWithoutAllocating(t *testing.T) {
	source := append([]byte(dumpMagic), 0xff, 0xff, 0xff, 0x7f, 0x00, 0x00, 0x00, 0x00)
	reader := newDumpReader(bytes.NewReader(source))

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	kind, raw, err := reader.Next()
	runtime.ReadMemStats(&after)

	if !errors.Is(err, errBadRecordLength) {
		t.Fatalf("a record declaring %d bytes gave %v, want errBadRecordLength", 0x7fffffff, err)
	}
	if kind != "" || raw != nil {
		t.Fatalf("a refused record returned kind %q and %d bytes", kind, len(raw))
	}
	if want := "offset " + strconv.Itoa(len(dumpMagic)); !strings.Contains(err.Error(), want) {
		t.Fatalf("the refusal %q does not carry the record's offset (%s)", err, want)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 1<<20 {
		t.Fatalf("refusing a %d-byte record allocated %d bytes; a record over the bound must be refused before its buffer is allocated", 0x7fffffff, allocated)
	}
}

// TestReaderRejectsABadMagic pins the first check of the reader: a stream that does not
// start with the magic is not a dump, and the refusal names the fix.
func TestReaderRejectsABadMagic(t *testing.T) {
	for _, test := range []struct {
		reason string
		source []byte
	}{
		{"a later layout generation", []byte("HDBDUMP2")},
		{"a different file", []byte("PK\x03\x04 and a zip archive follows")},
	} {
		t.Run(test.reason, func(t *testing.T) {
			reader := newDumpReader(bytes.NewReader(test.source))
			_, _, err := reader.Next()
			if !errors.Is(err, errBadDumpMagic) {
				t.Fatalf("a stream starting with %q gave %v, want errBadDumpMagic", test.source, err)
			}
			want := "This is not a hoarddb dump: it does not start with HDBDUMP1. Check the file name, or take a new dump with 'hoarddb dump'."
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("the refusal %q does not carry the text %q", err, want)
			}
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("a wrong magic of %d bytes was reported as cut off: %v", len(test.source), err)
			}
		})
	}
}

// TestReaderRefusesARecordThatDoesNotStartWithItsKind pins the kind read: the first
// member must be a string named t, so a record with a different first member, a non-string
// t or no member at all is refused before the strict decode runs. The summary of a newer
// version is still not a kind this reader knows, but it is not damage either; that case is
// decodeRecord's.
func TestReaderRefusesARecordThatDoesNotStartWithItsKind(t *testing.T) {
	for _, test := range []struct {
		reason string
		record bson.D
	}{
		{"a first member that is not t", bson.D{{Key: "x", Value: "header"}}},
		{"a t that is not a string", bson.D{{Key: dumpTypeMember, Value: int32(1)}}},
		{"no members at all", bson.D{}},
	} {
		t.Run(test.reason, func(t *testing.T) {
			record := marshalDumpTestRecord(t, test.record)
			fileBytes := append([]byte(dumpMagic), record...)
			reader := newDumpReader(bytes.NewReader(fileBytes))
			_, _, err := reader.Next()
			if !errors.Is(err, errRecordHasNoKind) {
				t.Fatalf("a record with %s gave %v, want errRecordHasNoKind", test.reason, err)
			}
		})
	}
}

// TestReaderReadsWhatTheWriterWrote pins the reader against the writer: every record
// comes back in file order with its kind word, each one strict-decodes, the offset ends
// at the file size, and the checksum the reader holds right after the summary is the
// footer's sha256.
func TestReaderReadsWhatTheWriterWrote(t *testing.T) {
	fileBytes := dumpTestFile(t)
	wantKinds := []string{
		dumpKindHeader, dumpKindDB, dumpKindBucket, dumpKindDoc, dumpKindElems,
		dumpKindSummary, dumpKindFooter,
	}

	reader := newDumpReader(bytes.NewReader(fileBytes))
	var kinds []string
	var footer *dumpFooter
	var summaryOffset int64
	var sumAfterSummary [32]byte
	for {
		kind, raw, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("after %d records: %v", len(kinds), err)
		}
		record, decodeErr := decodeRecord(raw)
		if decodeErr != nil {
			t.Fatalf("record %d (%s) does not decode: %v", len(kinds), kind, decodeErr)
		}
		if record.kindWord() != kind {
			t.Fatalf("Next returned kind %q for a record that decodes to %q", kind, record.kindWord())
		}
		if kind == dumpKindSummary {
			summaryOffset = reader.Offset()
			sumAfterSummary = reader.Sum()
		}
		if decodedFooter, ok := record.(*dumpFooter); ok {
			footer = decodedFooter
		}
		kinds = append(kinds, kind)
	}

	if strings.Join(kinds, ",") != strings.Join(wantKinds, ",") {
		t.Fatalf("the reader returned the kinds %v, want %v", kinds, wantKinds)
	}
	if got := reader.Offset(); got != int64(len(fileBytes)) {
		t.Fatalf("after the last record the reader is at offset %d and the file is %d bytes", got, len(fileBytes))
	}
	if footer == nil {
		t.Fatalf("the file holds no footer record")
	}
	if footer.Bytes != summaryOffset {
		t.Fatalf("the footer says its checksum covers %d bytes and the summary ends at %d", footer.Bytes, summaryOffset)
	}
	if !bytes.Equal(footer.SHA256, sumAfterSummary[:]) {
		t.Fatalf("the footer's sha256 is %x and the reader held %x after the summary", footer.SHA256, sumAfterSummary)
	}
}

// TestReaderTellsCutOffFromCleanEnd pins the two ends of the stream apart: a file that
// stops at a record boundary is a clean end, and a file that stops inside a length
// prefix or inside a record is a cut-off one, so the verifier can report each with its
// own text.
func TestReaderTellsCutOffFromCleanEnd(t *testing.T) {
	fileBytes := dumpTestFile(t)
	records := dumpFileRecords(t, fileBytes)
	footerBytes := len(records[len(records)-1])
	boundary := len(fileBytes) - footerBytes

	if err := readDumpUntilError(t, fileBytes[:boundary]); !errors.Is(err, io.EOF) {
		t.Fatalf("a file ending exactly at a record boundary gave %v, want io.EOF", err)
	}
	for _, test := range []struct {
		reason string
		cutAt  int
	}{
		{"inside a record's length prefix", boundary + 2},
		{"inside the footer record", boundary + footerBytes/2},
		{"inside the summary record", boundary - 10},
	} {
		t.Run(test.reason, func(t *testing.T) {
			err := readDumpUntilError(t, fileBytes[:test.cutAt])
			if errors.Is(err, io.EOF) {
				t.Fatalf("a file of %d bytes ending inside a record was reported as a clean end", test.cutAt)
			}
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("a file of %d bytes ending inside a record gave %v, want io.ErrUnexpectedEOF", test.cutAt, err)
			}
		})
	}
}

// TestDumpReaderAcceptsTheLargestRecords is the reader half of
// TestMaxDumpRecordBytesCoversTheLargestDocument: the two records of the largest payload
// the ring accepts go through Next whole and strict-decode. It fails when
// maxDumpRecordBytes is too small for a document at the ring's ceiling.
func TestDumpReaderAcceptsTheLargestRecords(t *testing.T) {
	for _, test := range largestDumpTestRecords(t) {
		t.Run(test.kind, func(t *testing.T) {
			fileBytes := make([]byte, 0, len(dumpMagic)+len(test.raw))
			fileBytes = append(fileBytes, dumpMagic...)
			fileBytes = append(fileBytes, test.raw...)

			reader := newDumpReader(bytes.NewReader(fileBytes))
			kind, raw, err := reader.Next()
			if err != nil {
				t.Fatalf("a %s record of %d bytes was refused: %v", test.kind, len(test.raw), err)
			}
			if kind != test.kind {
				t.Fatalf("Next returned kind %q, want %q", kind, test.kind)
			}
			if !bytes.Equal(raw, test.raw) {
				t.Fatalf("the reader returned %d bytes and the record is %d bytes", len(raw), len(test.raw))
			}
			if _, decodeErr := decodeRecord(raw); decodeErr != nil {
				t.Fatalf("the largest %s record does not strict-decode: %v", test.kind, decodeErr)
			}
			if got := reader.Offset(); got != int64(len(fileBytes)) {
				t.Fatalf("after the record the reader is at offset %d and the stream is %d bytes", got, len(fileBytes))
			}
			if _, _, eofErr := reader.Next(); !errors.Is(eofErr, io.EOF) {
				t.Fatalf("the end of the stream gave %v, want io.EOF", eofErr)
			}
		})
	}
}
