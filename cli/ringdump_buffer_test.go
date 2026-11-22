package cli

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// dumpSpillFiles lists the spill files a buffer left in directory. Section 3.5 tells the
// operator to delete `.spill.*`, so the name a test looks for is the name the page uses.
func dumpSpillFiles(t *testing.T, directory string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(directory, "*.spill.*"))
	if err != nil {
		t.Fatalf("listing the spill files in %s: %v", directory, err)
	}
	return matches
}

// dumpSpillRecords builds count doc records, each carrying a scalar string of payload
// bytes, so a test can push a buffer past dumpSpillBytes without a server.
func dumpSpillRecords(t *testing.T, count, payload int) []*dumpDoc {
	t.Helper()
	records := make([]*dumpDoc, 0, count)
	for index := range count {
		body, err := bson.Marshal(bson.D{{Key: "Name", Value: strings.Repeat("x", payload)}})
		if err != nil {
			t.Fatalf("building the body of %d bytes: %v", payload, err)
		}
		records = append(records, &dumpDoc{
			T:  dumpKindDoc,
			DB: "app",
			B:  "User",
			ID: fmt.Sprintf("u%d", index),
			F:  bson.Raw(body),
		})
	}
	return records
}

// dumpFillBuffer writes size-byte entities into buffer until the records hold more than
// dumpSpillBytes, which is what makes a buffer spill.
func dumpFillBuffer(t *testing.T, buffer *dumpEntityBuffer, count, payload int) {
	t.Helper()
	for _, record := range dumpSpillRecords(t, count, payload) {
		if err := buffer.WriteRecord(record); err != nil {
			t.Fatalf("buffering a %d-byte entity: %v", payload, err)
		}
	}
}

// dumpNoSpillFilesLeft fails when directory still holds a spill file or the spiller's
// live count is not zero: a spill file left behind is a failure of section 3.5's "the
// operator deletes .spill.*" being true only on a kill -9.
func dumpNoSpillFilesLeft(t *testing.T, directory string, spiller *dumpSpiller) {
	t.Helper()
	if got := dumpSpillFiles(t, directory); len(got) != 0 {
		t.Fatalf("the spill files must be gone; %s still holds %v", directory, got)
	}
	if live := spiller.live.Load(); live != 0 {
		t.Fatalf("the live spill count must be 0 when no spill file exists; it is %d", live)
	}
}

// TestABufferOverTheThresholdSpillsAndCopiesBack pins section 3.3: an entity's records
// stay in memory up to dumpSpillBytes, then live in one spill file, and copying them out
// in order must produce exactly the bytes the stream would have written directly.
func TestABufferOverTheThresholdSpillsAndCopiesBack(t *testing.T) {
	const payload = 300 * 1024
	const entityRecords = 8

	t.Run("over the threshold", func(t *testing.T) {
		directory := t.TempDir()
		spiller := newDumpSpiller(directory, "app.hoarddump")
		buffer := newDumpEntityBuffer(spiller)
		dumpFillBuffer(t, buffer, entityRecords, payload)

		spillFiles := dumpSpillFiles(t, directory)
		if len(spillFiles) != 1 {
			t.Fatalf("%d records of %d bytes pass the %d-byte limit, so they must be in one spill file; %s holds %v",
				entityRecords, payload, dumpSpillBytes, directory, spillFiles)
		}
		if live := spiller.live.Load(); live != 1 {
			t.Fatalf("one spill file exists, so the live count must be 1; it is %d", live)
		}
		if peak := spiller.peak.Load(); peak != 1 {
			t.Fatalf("one spill file was created at most, so the peak must be 1; it is %d", peak)
		}

		writer, err := openDumpWriter(directory, "app.hoarddump")
		if err != nil {
			t.Fatalf("opening the dump writer: %v", err)
		}
		if err := buffer.copyTo(writer); err != nil {
			t.Fatalf("copying the buffered entity into the dump: %v", err)
		}
		if err := writer.Close(dumpCounters{}); err != nil {
			t.Fatalf("completing the dump: %v", err)
		}
		bufferedPath := filepath.Join(directory, "app.hoarddump")

		dumpNoSpillFilesLeft(t, directory, spiller)
		if peak := spiller.peak.Load(); peak != 1 {
			t.Fatalf("the peak must keep the one spill file the buffer used; it is %d", peak)
		}

		directDirectory := t.TempDir()
		directWriter, err := openDumpWriter(directDirectory, "app.hoarddump")
		if err != nil {
			t.Fatalf("opening the direct dump writer: %v", err)
		}
		for _, record := range dumpSpillRecords(t, entityRecords, payload) {
			if err := directWriter.WriteRecord(record); err != nil {
				t.Fatalf("writing an entity straight into the dump: %v", err)
			}
		}
		if err := directWriter.Close(dumpCounters{}); err != nil {
			t.Fatalf("completing the direct dump: %v", err)
		}

		bufferedBytes, err := os.ReadFile(bufferedPath)
		if err != nil {
			t.Fatalf("reading %s back: %v", bufferedPath, err)
		}
		directBytes, err := os.ReadFile(filepath.Join(directDirectory, "app.hoarddump"))
		if err != nil {
			t.Fatalf("reading the direct dump back: %v", err)
		}
		if !bytes.Equal(bufferedBytes, directBytes) {
			t.Fatalf("a dump whose entity was copied out of a spill file must be byte-identical to one written straight through; %d bytes against %d",
				len(bufferedBytes), len(directBytes))
		}

		found := dumpDocsOf(readDumpRecords(t, bufferedPath), "app", "User")
		if len(found) != entityRecords {
			t.Fatalf("the completed file must hold the %d entities the buffer held; it holds %d", entityRecords, len(found))
		}
	})

	t.Run("under the threshold", func(t *testing.T) {
		directory := t.TempDir()
		spiller := newDumpSpiller(directory, "app.hoarddump")
		buffer := newDumpEntityBuffer(spiller)
		dumpFillBuffer(t, buffer, 1, 16)

		if got := dumpSpillFiles(t, directory); len(got) != 0 {
			t.Fatalf("a buffer under dumpSpillBytes must not touch the disk; %s holds %v", directory, got)
		}
		if live := spiller.live.Load(); live != 0 {
			t.Fatalf("a buffer that never spilled must leave the live count at 0; it is %d", live)
		}

		writer, err := openDumpWriter(directory, "app.hoarddump")
		if err != nil {
			t.Fatalf("opening the dump writer: %v", err)
		}
		if err := buffer.copyTo(writer); err != nil {
			t.Fatalf("copying a buffer that stayed in memory: %v", err)
		}
		if err := writer.Close(dumpCounters{}); err != nil {
			t.Fatalf("completing the dump: %v", err)
		}
		if found := dumpDocsOf(readDumpRecords(t, filepath.Join(directory, "app.hoarddump")), "app", "User"); len(found) != 1 {
			t.Fatalf("the completed file must hold the one entity the buffer held; it holds %d", len(found))
		}
	})
}

// TestSpillFileIsDeletedOnSuccessErrorAndInterrupt pins section 3.3's promise that a
// spill file is deleted when the entity is copied or the run ends, including on error and
// interrupt: the three ways a run can leave a spill file behind, had the buffer leaked it.
func TestSpillFileIsDeletedOnSuccessErrorAndInterrupt(t *testing.T) {
	const payload = 300 * 1024

	t.Run("success", func(t *testing.T) {
		directory := t.TempDir()
		spiller := newDumpSpiller(directory, "app.hoarddump")
		buffer := newDumpEntityBuffer(spiller)
		dumpFillBuffer(t, buffer, 8, payload)
		if got := dumpSpillFiles(t, directory); len(got) != 1 {
			t.Fatalf("the buffer must have spilled before this subtest copies it; %s holds %v", directory, got)
		}

		writer, err := openDumpWriter(directory, "app.hoarddump")
		if err != nil {
			t.Fatalf("opening the dump writer: %v", err)
		}
		if err := buffer.copyTo(writer); err != nil {
			t.Fatalf("copying the buffered entity into the dump: %v", err)
		}
		if err := writer.Abort(); err != nil {
			t.Fatalf("aborting the dump writer: %v", err)
		}
		dumpNoSpillFilesLeft(t, directory, spiller)
	})

	t.Run("error", func(t *testing.T) {
		directory := t.TempDir()
		spiller := newDumpSpiller(directory, "app.hoarddump")
		buffer := newDumpEntityBuffer(spiller)
		dumpFillBuffer(t, buffer, 8, payload)
		if got := dumpSpillFiles(t, directory); len(got) != 1 {
			t.Fatalf("the buffer must have spilled before this subtest copies it; %s holds %v", directory, got)
		}

		writer, err := openDumpWriter(directory, "app.hoarddump")
		if err != nil {
			t.Fatalf("opening the dump writer: %v", err)
		}
		if err := writer.Abort(); err != nil {
			t.Fatalf("aborting the dump writer: %v", err)
		}
		copyErr := buffer.copyTo(writer)
		if copyErr == nil {
			t.Fatal("copying into an aborted writer must fail; it returned no error")
		}
		for _, want := range []string{"copying", "aborted"} {
			if !strings.Contains(copyErr.Error(), want) {
				t.Fatalf("the refusal must be wrapped and name the %q writer; it says: %v", want, copyErr)
			}
		}
		dumpNoSpillFilesLeft(t, directory, spiller)
	})

	t.Run("interrupt", func(t *testing.T) {
		directory := t.TempDir()
		spiller := newDumpSpiller(directory, "app.hoarddump")
		buffer := newDumpEntityBuffer(spiller)
		dumpFillBuffer(t, buffer, 8, payload)
		if got := dumpSpillFiles(t, directory); len(got) != 1 {
			t.Fatalf("the buffer must have spilled before this subtest closes it; %s holds %v", directory, got)
		}

		if err := buffer.close(); err != nil {
			t.Fatalf("closing an uncopied buffer: %v", err)
		}
		if err := buffer.close(); err != nil {
			t.Fatalf("closing a buffer twice must be fine; it returned: %v", err)
		}
		dumpNoSpillFilesLeft(t, directory, spiller)
	})
}
