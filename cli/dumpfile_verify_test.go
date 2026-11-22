package cli

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// dumpVerifyTestCounts is the summary a writer must be given for the records of
// dumpVerifyTestRecords: one database, one bucket, two entities and three elements.
var dumpVerifyTestCounts = dumpCounters{Databases: 1, Buckets: 1, Entities: 2, Elements: 3}

// dumpVerifyTestRecords returns the records of the small dump the verification tests
// use: a header, one database, one bucket whose store field is storeWord, two entities
// and one store chunk of three elements. The chunk holds keys for a hashStore, so that
// the record is one a dump of that field would really hold.
func dumpVerifyTestRecords(t *testing.T, storeWord string) []any {
	t.Helper()
	scalars, err := bson.Marshal(bson.D{{Key: "Name", Value: "Ada"}})
	if err != nil {
		t.Fatalf("marshalling an entity's scalars: %v", err)
	}
	chunk := &dumpElems{
		T: dumpKindElems, DB: "app", B: "User", ID: "ada", Field: "Items",
		Kind: storeWord, Seq: 0,
		Els: []bson.Raw{
			dumpTestDocument(t, "one"),
			dumpTestDocument(t, "two"),
			dumpTestDocument(t, "three"),
		},
	}
	if storeWord == "hashStore" {
		chunk.Keys = []string{"a", "b", "c"}
	}
	return []any{
		dumpTestHeader(),
		&dumpDB{T: dumpKindDB, Name: "app", RF: 1, Storage: "disk"},
		&dumpBucket{
			T: dumpKindBucket, DB: "app", Name: "User",
			Fields:      []dumpField{{Name: "Name", Type: "string"}, {Name: "Items", Type: storeWord}},
			Aggregates:  []dumpAggregate{},
			Unsupported: []string{},
		},
		&dumpDoc{T: dumpKindDoc, DB: "app", B: "User", ID: "ada", F: scalars},
		&dumpDoc{T: dumpKindDoc, DB: "app", B: "User", ID: "grace", F: scalars},
		chunk,
	}
}

// writeVerifyTestDump writes records with the dump writer and returns the file's path.
func writeVerifyTestDump(t *testing.T, records []any, counters dumpCounters) string {
	t.Helper()
	dir := t.TempDir()
	writer, err := openDumpWriter(dir, "app.hoarddump")
	if err != nil {
		t.Fatalf("opening a dump writer: %v", err)
	}
	for _, record := range records {
		if err := writer.WriteRecord(record); err != nil {
			if abortErr := writer.Abort(); abortErr != nil {
				t.Fatalf("writing a %T: %v; aborting the writer also failed: %v", record, err, abortErr)
			}
			t.Fatalf("writing a %T: %v", record, err)
		}
	}
	if err := writer.Close(counters); err != nil {
		t.Fatalf("closing the dump writer: %v", err)
	}
	return filepath.Join(dir, "app.hoarddump")
}

// writeVerifyTestDumpWithSummary writes records and the given summary followed by a
// footer the verifier accepts. The writer's Close always writes the record count it
// observed, so a summary that disagrees with the records has to be built by hand: a
// wrong counter must be the file's only fault, or the checksum would report it as
// damage of the covered bytes and the count check of section 3.1 case 4 would never be
// reached.
func writeVerifyTestDumpWithSummary(t *testing.T, records []any, summary *dumpSummaryRecord) string {
	t.Helper()
	covered := []byte(dumpMagic)
	for _, record := range records {
		covered = append(covered, marshalVerifyTestRecord(t, record)...)
	}
	covered = append(covered, marshalVerifyTestRecord(t, summary)...)
	checksum := sha256.Sum256(covered)
	fileBytes := append(covered, marshalVerifyTestRecord(t, &dumpFooter{
		T: dumpKindFooter, Bytes: int64(len(covered)), SHA256: checksum[:],
	})...)
	path := filepath.Join(t.TempDir(), "hand.hoarddump")
	if err := os.WriteFile(path, fileBytes, 0o600); err != nil {
		t.Fatalf("writing a hand-built dump: %v", err)
	}
	return path
}

// marshalVerifyTestRecord encodes one record for a hand-built dump file.
func marshalVerifyTestRecord(t *testing.T, record any) []byte {
	t.Helper()
	encoded, err := bson.Marshal(record)
	if err != nil {
		t.Fatalf("marshalling a %T: %v", record, err)
	}
	return encoded
}

// readVerifyTestFile reads a test dump back from disk.
func readVerifyTestFile(t *testing.T, path string) []byte {
	t.Helper()
	fileBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return fileBytes
}

// isVerificationError reports whether err is one of the four errors a verification can
// end with: the three sentinels, or the reader's bad magic.
func isVerificationError(err error) bool {
	return errors.Is(err, errDumpIncomplete) || errors.Is(err, errDumpDamaged) ||
		errors.Is(err, errDumpNewerVersion) || errors.Is(err, errBadDumpMagic)
}

// assertEveryByteFlipIsDetected flips each of the eight bits of every byte of the file,
// one flip per case, and requires an error every time. It is a single test loop and not
// a subtest per case: a small dump already gives thousands of cases, and the failure
// message names the position and the bit.
func assertEveryByteFlipIsDetected(t *testing.T, fileBytes []byte) {
	t.Helper()
	flipPath := filepath.Join(t.TempDir(), "flipped.hoarddump")
	for position := 0; position < len(fileBytes); position++ {
		for bit := 0; bit < 8; bit++ {
			flipped := append([]byte(nil), fileBytes...)
			flipped[position] ^= 1 << bit
			if err := os.WriteFile(flipPath, flipped, 0o600); err != nil {
				t.Fatalf("writing the flip of bit %d of byte %d: %v", bit, position, err)
			}
			verification, err := verifyDump(flipPath)
			if err == nil {
				t.Fatalf("flipping bit %d of byte %d left the dump verifying as %+v; a changed file must be an error", bit, position, verification)
			}
			if !isVerificationError(err) {
				t.Fatalf("flipping bit %d of byte %d gave %v, which is none of the verifier's errors", bit, position, err)
			}
		}
	}
}

// dumpVerifyTestRecordsWithUnknownKind returns the small dump's records with a record of
// an unknown kind before the store chunk. A verifier that stopped at the unknown record
// would miss the chunk's three elements and fail the count check, so the newer-version
// text returned for this file proves that the whole file was read; so does the damaged
// text, which carries the footer's checksum.
func dumpVerifyTestRecordsWithUnknownKind(t *testing.T) []any {
	t.Helper()
	records := dumpVerifyTestRecords(t, "fifoStore")
	ordered := make([]any, 0, len(records)+1)
	ordered = append(ordered, records[:len(records)-1]...)
	ordered = append(ordered, bson.D{
		{Key: dumpTypeMember, Value: "missing"},
		{Key: "db", Value: "app"},
	})
	return append(ordered, records[len(records)-1])
}

// TestTruncatedDumpIsDetectedAtEveryByteOffset pins case 1 of section 3.1: a file cut at
// any offset, down to the empty file, ends without a footer and is reported incomplete.
// A cut inside the magic is incomplete too, not a wrong file: only a file that is long
// enough to hold a wrong magic is that.
func TestTruncatedDumpIsDetectedAtEveryByteOffset(t *testing.T) {
	path := writeVerifyTestDump(t, dumpVerifyTestRecords(t, "fifoStore"), dumpVerifyTestCounts)
	fileBytes := readVerifyTestFile(t, path)
	cutPath := filepath.Join(t.TempDir(), "cut.hoarddump")
	for offset := 0; offset < len(fileBytes); offset++ {
		if err := os.WriteFile(cutPath, fileBytes[:offset], 0o600); err != nil {
			t.Fatalf("writing a dump cut at offset %d: %v", offset, err)
		}
		verification, err := verifyDump(cutPath)
		if !errors.Is(err, errDumpIncomplete) {
			t.Fatalf("a dump of %d bytes cut at offset %d gave %v (verification %+v), want errDumpIncomplete", len(fileBytes), offset, err, verification)
		}
		if !strings.HasPrefix(err.Error(), "This dump is incomplete: it ends after ") {
			t.Fatalf("a dump cut at offset %d printed %q, which is not the incomplete text of section 3.1", offset, err)
		}
		if verification != (dumpVerification{}) {
			t.Fatalf("a dump cut at offset %d returned the summary %+v with its error", offset, verification)
		}
	}
}

// TestFlippedByteIsDetected flips every bit of every byte of a whole dump, summary and
// footer included, and requires an error every time. A checksum that covered only part
// of the file, or a footer trusted before the records it covers were read, would leave
// one of these cases verifying.
func TestFlippedByteIsDetected(t *testing.T) {
	path := writeVerifyTestDump(t, dumpVerifyTestRecords(t, "fifoStore"), dumpVerifyTestCounts)
	assertEveryByteFlipIsDetected(t, readVerifyTestFile(t, path))
}

// TestExtraBytesAfterTheFooterAreDamage pins case 4: a whole file followed by anything,
// even a byte too short to be a record, is damage and not an incomplete file.
func TestExtraBytesAfterTheFooterAreDamage(t *testing.T) {
	path := writeVerifyTestDump(t, dumpVerifyTestRecords(t, "fifoStore"), dumpVerifyTestCounts)
	fileBytes := readVerifyTestFile(t, path)
	extraPath := filepath.Join(t.TempDir(), "extra.hoarddump")
	for _, extra := range [][]byte{{0x00}, []byte("appended by something else")} {
		if err := os.WriteFile(extraPath, append(append([]byte(nil), fileBytes...), extra...), 0o600); err != nil {
			t.Fatalf("writing a dump with %d extra bytes: %v", len(extra), err)
		}
		verification, err := verifyDump(extraPath)
		if !errors.Is(err, errDumpDamaged) {
			t.Fatalf("a dump with %d extra bytes gave %v (verification %+v), want errDumpDamaged", len(extra), err, verification)
		}
		if !strings.HasPrefix(err.Error(), "This dump is damaged: ") {
			t.Fatalf("a dump with %d extra bytes printed %q, which is not the damaged text", len(extra), err)
		}
		if verification != (dumpVerification{}) {
			t.Fatalf("a dump with %d extra bytes returned the summary %+v with its error", len(extra), verification)
		}
	}
}

// dumpVerifyTestWidget is a record kind no version of hoarddb writes. The test of
// section 3.1 "What is frozen" adds it to the kind table to prove that bringing a kind
// back is a table edit, not a change to the reader.
type dumpVerifyTestWidget struct {
	T string `bson:"t"`
	N int64  `bson:"n"`
}

func (*dumpVerifyTestWidget) kindWord() string { return "widget" }

// TestARecordKindAddedToTheTableIsStrictDecodedCountedAndChecksummed registers a kind
// and a counter for it in the test, writes a dump holding one, and verifies it: the
// record must strict-decode, be counted in the summary's record count, and be covered
// by the checksum (the flip pass at the end).
func TestARecordKindAddedToTheTableIsStrictDecodedCountedAndChecksummed(t *testing.T) {
	const widgetKind = "widget"
	var widgetRecords int64
	tableLength := len(dumpRecordKinds)
	dumpRecordKinds = append(dumpRecordKinds, dumpRecordKind{
		word:      widgetKind,
		newRecord: func() dumpRecord { return &dumpVerifyTestWidget{} },
	})
	dumpRecordCounters[widgetKind] = func(*dumpTally, dumpRecord) { widgetRecords++ }
	t.Cleanup(func() {
		dumpRecordKinds = dumpRecordKinds[:tableLength]
		delete(dumpRecordCounters, widgetKind)
	})

	records := append(dumpVerifyTestRecords(t, "fifoStore"), &dumpVerifyTestWidget{T: widgetKind, N: 7})
	path := writeVerifyTestDump(t, records, dumpVerifyTestCounts)

	verification, err := verifyDump(path)
	if err != nil {
		t.Fatalf("a dump holding one %q record did not verify: %v", widgetKind, err)
	}
	if widgetRecords != 1 {
		t.Fatalf("the test's counter for %q records is %d, want 1: the verifier did not count the record", widgetKind, widgetRecords)
	}
	want := dumpVerification{
		Consistency: dumpConsistencyWord, Records: 7, Databases: 1, Buckets: 1,
		Entities: 2, Elements: 3, HasQueueField: true,
	}
	// The checksum covers the header's created time, so it differs per run; this test is about
	// the counts and flags, and the checksum has its own tests.
	verification.SHA256 = [32]byte{}
	if verification != want {
		t.Fatalf("the dump verified as %+v, want %+v", verification, want)
	}
	assertEveryByteFlipIsDetected(t, readVerifyTestFile(t, path))
}

// TestAnUnknownRecordKindInAFileWithAValidChecksumNamesANewerVersion pins the refusal of
// section 3.1 rev 7.1: a file whose checksum and counts agree but which holds a kind
// this version does not know was written by a newer hoarddb, and the refusal names the
// kind. The text itself is the evidence that the verifier read to the end: an early
// stop would report the file incomplete or damaged instead.
func TestAnUnknownRecordKindInAFileWithAValidChecksumNamesANewerVersion(t *testing.T) {
	path := writeVerifyTestDump(t, dumpVerifyTestRecordsWithUnknownKind(t), dumpVerifyTestCounts)

	verification, err := verifyDump(path)
	if !errors.Is(err, errDumpNewerVersion) {
		t.Fatalf("a dump holding an unknown record kind gave %v (verification %+v), want errDumpNewerVersion", err, verification)
	}
	want := `This dump holds a record of kind "missing", which this version of hoarddb does not know. A newer hoarddb wrote it; use that version to read it.`
	if err.Error() != want {
		t.Fatalf("the refusal printed %q, want %q", err, want)
	}
	if verification != (dumpVerification{}) {
		t.Fatalf("the refusal returned the summary %+v; a refusal carries no summary", verification)
	}
}

// TestAFlippedByteInARecordKindPrintsTheDamagedTextNotTheNewerVersionText pins R3-06: a
// kind word changed after the file was written is damage, never a newer version, because
// the checksum disagrees. The verifier read the whole file before deciding (the damaged
// text carries the footer's checksum), and it returned no summary.
func TestAFlippedByteInARecordKindPrintsTheDamagedTextNotTheNewerVersionText(t *testing.T) {
	path := writeVerifyTestDump(t, dumpVerifyTestRecordsWithUnknownKind(t), dumpVerifyTestCounts)
	fileBytes := readVerifyTestFile(t, path)
	kindAt := bytes.Index(fileBytes, []byte("missing"))
	if kindAt < 0 {
		t.Fatalf("the dump does not hold the kind word %q", "missing")
	}
	fileBytes[kindAt] ^= 0x01
	flippedPath := filepath.Join(t.TempDir(), "flipped.hoarddump")
	if err := os.WriteFile(flippedPath, fileBytes, 0o600); err != nil {
		t.Fatalf("writing the flipped dump: %v", err)
	}

	verification, err := verifyDump(flippedPath)
	if !errors.Is(err, errDumpDamaged) {
		t.Fatalf("a flipped byte in a record kind gave %v (verification %+v), want errDumpDamaged", err, verification)
	}
	if errors.Is(err, errDumpNewerVersion) {
		t.Fatalf("a flipped byte in a record kind was blamed on a newer version: %v", err)
	}
	if !strings.HasPrefix(err.Error(), "This dump is damaged: ") {
		t.Fatalf("the refusal printed %q, which is not the damaged text of case 2", err)
	}
	if strings.Contains(err.Error(), dumpNoReadableFooter) {
		t.Fatalf("the damaged text of %q found no readable footer: the verifier did not read to the end", err)
	}
	if verification != (dumpVerification{}) {
		t.Fatalf("the refusal returned the summary %+v; a refusal carries no summary", verification)
	}
}

// TestAnUnknownConsistencyWordInAFileWithAValidChecksumNamesANewerVersion pins the same
// path for the header's consistency word: the word is held until the checksum and the
// counts agree, then named in the newer-version refusal, with no summary returned.
func TestAnUnknownConsistencyWordInAFileWithAValidChecksumNamesANewerVersion(t *testing.T) {
	header := dumpTestHeader()
	header.Consistency = "entity-recheck-online"
	records := dumpVerifyTestRecords(t, "fifoStore")
	records[0] = header
	path := writeVerifyTestDump(t, records, dumpVerifyTestCounts)

	verification, err := verifyDump(path)
	if !errors.Is(err, errDumpNewerVersion) {
		t.Fatalf("a dump with an unknown consistency word gave %v (verification %+v), want errDumpNewerVersion", err, verification)
	}
	want := `This dump says its consistency is "entity-recheck-online", which this version of hoarddb does not know. A newer hoarddb wrote it; use that version to read it.`
	if err.Error() != want {
		t.Fatalf("the refusal printed %q, want %q", err, want)
	}
	if verification != (dumpVerification{}) {
		t.Fatalf("the refusal returned the summary %+v; a refusal carries no summary", verification)
	}
}

// TestCountsMustMatchTheSummary pins section 3.1 case 4 for each of the five counters:
// a summary that disagrees with the file is damage, and the text names the counter, what
// the summary said and what the file holds. The writer's Close owns the records counter,
// so that one case is a hand-built summary.
func TestCountsMustMatchTheSummary(t *testing.T) {
	for _, test := range []struct {
		name     string
		counters dumpCounters
		said     int64
		held     int64
	}{
		{"records", dumpVerifyTestCounts, 7, 6},
		{"databases", dumpCounters{Databases: 2, Buckets: 1, Entities: 2, Elements: 3}, 2, 1},
		{"buckets", dumpCounters{Databases: 1, Buckets: 3, Entities: 2, Elements: 3}, 3, 1},
		{"entities", dumpCounters{Databases: 1, Buckets: 1, Entities: 4, Elements: 3}, 4, 2},
		{"elements", dumpCounters{Databases: 1, Buckets: 1, Entities: 2, Elements: 5}, 5, 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			records := dumpVerifyTestRecords(t, "fifoStore")
			var path string
			if test.name == "records" {
				path = writeVerifyTestDumpWithSummary(t, records, &dumpSummaryRecord{
					T: dumpKindSummary, Records: test.said, Databases: 1, Buckets: 1,
					Entities: 2, Elements: 3,
				})
			} else {
				path = writeVerifyTestDump(t, records, test.counters)
			}

			verification, err := verifyDump(path)
			if !errors.Is(err, errDumpDamaged) {
				t.Fatalf("a summary that says %d %s gave %v (verification %+v), want errDumpDamaged", test.said, test.name, err, verification)
			}
			want := fmt.Sprintf("This dump is damaged: its summary says %d %s but the file holds %d. The file was changed after it was written. Restore from another copy, or take a new dump.", test.said, test.name, test.held)
			if err.Error() != want {
				t.Fatalf("the refusal printed %q, want %q", err, want)
			}
			if verification != (dumpVerification{}) {
				t.Fatalf("the refusal returned the summary %+v; a refusal carries no summary", verification)
			}
		})
	}
}

// TestVerifyReturnsConsistencyAndQueueFieldFlag pins what a verification returns on a
// whole file: the header's consistency word, the five counts and whether any bucket
// declares a fifoStore or lifoStore field, which is a fact about the file and never an
// error (the queue line of section 3.1).
func TestVerifyReturnsConsistencyAndQueueFieldFlag(t *testing.T) {
	for _, test := range []struct {
		storeWord string
		queue     bool
	}{
		{"fifoStore", true},
		{"lifoStore", true},
		{"hashStore", false},
	} {
		t.Run(test.storeWord, func(t *testing.T) {
			path := writeVerifyTestDump(t, dumpVerifyTestRecords(t, test.storeWord), dumpVerifyTestCounts)
			verification, err := verifyDump(path)
			if err != nil {
				t.Fatalf("a good dump did not verify: %v", err)
			}
			want := dumpVerification{
				Consistency: dumpConsistencyWord, Records: 6, Databases: 1, Buckets: 1,
				Entities: 2, Elements: 3, HasQueueField: test.queue,
			}
			// The checksum covers the header's created time, so it differs per run; this test is about
			// the counts and flags, and the checksum has its own tests.
			verification.SHA256 = [32]byte{}
			if verification != want {
				t.Fatalf("the dump verified as %+v, want %+v", verification, want)
			}
		})
	}
}

// TestAFlippedByteInTheFooterIsDamageNotIncomplete pins case 3 of section 3.1: the
// record after the summary is the footer even when a flipped byte makes it undecodable
// or gives it another kind word, so the file is damaged. Reading it as "ends with no
// footer" would tell the operator the dump was cut off when it was changed. The four
// length bytes are left out: a flipped length can declare a record that runs past the
// end of the file, which really is a cut-off record.
func TestAFlippedByteInTheFooterIsDamageNotIncomplete(t *testing.T) {
	path := writeVerifyTestDump(t, dumpVerifyTestRecords(t, "fifoStore"), dumpVerifyTestCounts)
	fileBytes := readVerifyTestFile(t, path)
	footerLength := len(marshalVerifyTestRecord(t, &dumpFooter{T: dumpKindFooter, SHA256: make([]byte, dumpRecordSHA256Bytes)}))
	footerStart := len(fileBytes) - footerLength
	flippedPath := filepath.Join(t.TempDir(), "flipped.hoarddump")
	for offset := footerStart + 4; offset < len(fileBytes); offset++ {
		flipped := append([]byte(nil), fileBytes...)
		flipped[offset] ^= 0x01
		if err := os.WriteFile(flippedPath, flipped, 0o600); err != nil {
			t.Fatalf("writing a dump with byte %d flipped: %v", offset, err)
		}
		_, err := verifyDump(flippedPath)
		if !errors.Is(err, errDumpDamaged) {
			t.Fatalf("flipping footer byte %d gave %v, want errDumpDamaged", offset-footerStart, err)
		}
	}
}

// TestAHeaderOfAnotherFormatVersionNamesBothVersions pins that the header's v member is
// checked: a file of another format version is refused with a text that names the file's
// version and the one this build reads, only after the checksum and counts agree, like an
// unknown kind. A flipped byte in v of a good file is damage, never a version claim.
func TestAHeaderOfAnotherFormatVersionNamesBothVersions(t *testing.T) {
	header := dumpTestHeader()
	header.V = 2
	records := dumpVerifyTestRecords(t, "fifoStore")
	records[0] = header
	path := writeVerifyTestDump(t, records, dumpVerifyTestCounts)

	verification, err := verifyDump(path)
	if !errors.Is(err, errDumpNewerVersion) {
		t.Fatalf("a version 2 dump gave %v (verification %+v), want errDumpNewerVersion", err, verification)
	}
	want := "This dump is format version 2 and this version of hoarddb reads version 1. Use the hoarddb that wrote it, or take a new dump with this version."
	if err.Error() != want {
		t.Fatalf("the refusal printed %q, want %q", err, want)
	}
	if verification != (dumpVerification{}) {
		t.Fatalf("the refusal returned the summary %+v", verification)
	}

	good := readVerifyTestFile(t, writeVerifyTestDump(t, dumpVerifyTestRecords(t, "fifoStore"), dumpVerifyTestCounts))
	marshaled := marshalVerifyTestRecord(t, dumpTestHeader())
	versionAt := bytes.Index(good, marshaled) + bytes.Index(marshaled, []byte("v\x00")) + 2
	good[versionAt] = 2
	flippedPath := filepath.Join(t.TempDir(), "flipped.hoarddump")
	if err := os.WriteFile(flippedPath, good, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyDump(flippedPath); !errors.Is(err, errDumpDamaged) {
		t.Fatalf("a flipped v gave %v, want errDumpDamaged", err)
	}
}
