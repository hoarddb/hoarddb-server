package server

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
)

// BUG-0674: a LIFO restore must not hold the bucket in memory, and must still
// pop in the original order.

// lifoRestoreResidentCap is the memory a LIFO restore may hold at once in these
// tests, whatever the size of the dump: well under the ~3 MB the bounded-memory
// test streams.
const lifoRestoreResidentCap = 256 << 10

// restoreChunksOf splits a section's records into chunks of about chunkBytes,
// never splitting a record, as the CLI's packRestoreChunks does.
func restoreChunksOf(t *testing.T, section []byte, chunkBytes int) [][]byte {
	t.Helper()
	records, err := parseBSONDocuments(section)
	if err != nil {
		t.Fatalf("splitting the dumped section: %v", err)
	}
	var chunks [][]byte
	var current []byte
	for _, record := range records {
		if len(current) > 0 && len(current)+len(record) > chunkBytes {
			chunks = append(chunks, current)
			current = nil
		}
		current = append(current, record...)
	}
	if len(current) > 0 {
		chunks = append(chunks, current)
	}
	return chunks
}

// startLIFORestore opens a restore of a LIFO dump into dest and returns the
// session the server holds for it.
func startLIFORestore(t *testing.T, ctx context.Context, dest *Server, dumped dumpedBucket) *restoreSession {
	t.Helper()
	started, err := dest.startRestore(ctx, RestoreStartRequest{
		Database: "app", Bucket: dumped.manifest.Bucket, FormatVersion: dumped.manifest.FormatVersion,
		StoreType: dumped.manifest.StoreType, Bytes: 1, Dead: dumped.done.Dead,
	})
	if err != nil {
		t.Fatalf("starting the restore of %s: %v", dumped.manifest.Bucket, err)
	}
	dest.restoreSessionMu.Lock()
	session := dest.restoreSessions[started.SessionID]
	dest.restoreSessionMu.Unlock()
	if session == nil {
		t.Fatalf("the restore of %s opened no session", dumped.manifest.Bucket)
	}
	return session
}

// TestLIFORestoreKeepsBoundedMemory: a LIFO dump far larger than the cap is
// restored in chunks, and the memory the session holds for the LIFO entries
// never passes the cap, while the chunks stream or while the finish pushes.
func TestLIFORestoreKeepsBoundedMemory(t *testing.T) {
	const entries = 1500
	source := newTestServer(t)
	createTestBucket(t, source, "app", "big", BucketTypeLIFO)
	pushPeekEntries(t, source, "big", entries, strings.Repeat("p", 2000))
	dumped := dumpSections(t, source, "big", DumpStartRequest{})
	if size := len(dumped.sections[""]); size < 10*lifoRestoreResidentCap {
		t.Fatalf("the dump is %d bytes; the test needs one far larger than the %d-byte cap", size, lifoRestoreResidentCap)
	}

	dest := newTestServer(t)
	session := startLIFORestore(t, rootContext(), dest, dumped)
	for i, chunk := range restoreChunksOf(t, dumped.sections[""], 64<<10) {
		if _, err := dest.restoreChunk(rootContext(), RestoreChunkRequest{
			Database: "app", Bucket: "big", SessionID: session.id, Data: chunk,
		}); err != nil {
			t.Fatalf("restoring chunk %d: %v", i, err)
		}
		if held := session.lifoPeakResidentBytes(); held > lifoRestoreResidentCap {
			t.Fatalf("after chunk %d the LIFO restore held %d bytes in memory, over the %d-byte cap: it is buffering the bucket",
				i, held, lifoRestoreResidentCap)
		}
	}
	done, err := dest.restoreDone(rootContext(), RestoreDoneRequest{
		Database: "app", Bucket: "big", SessionID: session.id, Documents: dumped.done.Documents,
	})
	if err != nil || !done.Success || done.Applied != entries {
		t.Fatalf("finishing the restore: %+v, %v; want success with %d applied", done, err, entries)
	}
	if held := session.lifoPeakResidentBytes(); held > lifoRestoreResidentCap {
		t.Fatalf("the LIFO restore held up to %d bytes in memory, over the %d-byte cap", held, lifoRestoreResidentCap)
	}
	again := dumpSections(t, dest, "big", DumpStartRequest{})
	if got, want := again.documentNumbers(t), dumped.documentNumbers(t); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("the restored bucket dumps in a different order than the source")
	}
}

// poppedEntry is one popped LIFO entry: its id and its number.
type poppedEntry struct {
	id string
	n  int64
}

func popAllEntries(t *testing.T, s *Server, bucket string) []poppedEntry {
	t.Helper()
	var popped []poppedEntry
	for {
		answer, err := s.Pop(PopRequest{Database: "app", Bucket: bucket})
		if err != nil {
			t.Fatalf("popping %s: %v", bucket, err)
		}
		if !answer.Found {
			return popped
		}
		var entry struct {
			ID string `bson:"_id_"`
			N  int64  `bson:"n"`
		}
		if err := bson.Unmarshal(answer.Items[0], &entry); err != nil {
			t.Fatalf("decoding a popped entry of %s: %v", bucket, err)
		}
		popped = append(popped, poppedEntry{id: entry.ID, n: entry.N})
	}
}

// TestLIFORestoreKeepsPopOrderIdsClaimedAndDead: the restored bucket pops in
// the source's order with the source's ids, its claimed entries back as
// ordinary entries in their place, and its dead entries dead.
func TestLIFORestoreKeepsPopOrderIdsClaimedAndDead(t *testing.T) {
	source := newTestServer(t)
	withLeaseClock(source)
	createTestBucket(t, source, "app", "stack", BucketTypeLIFO)
	for n := 1; n <= 8; n++ {
		if answer, err := source.Push(PushRequest{Database: "app", Bucket: "stack",
			Data: map[string]interface{}{"_id_": fmt.Sprintf("e%d", n), "n": int64(n)}}); err != nil || !answer.Success {
			t.Fatalf("pushing e%d: %+v, %v", n, answer, err)
		}
	}
	restoredDead(t, source, "stack", BucketTypeLIFO, 2, "")
	// The two newest are claimed: a restore brings them back as entries on top.
	claimOf(t, source, "stack", 2, 60_000)
	dumped := dumpSections(t, source, "stack", DumpStartRequest{AcceptDead: true})

	dest := newTestServer(t)
	answer := restoreSections(t, dest, dumped)
	if !answer.Success || answer.Applied != 8 || answer.DeadApplied != 2 {
		t.Fatalf("the restore answered %+v, want 8 entries and 2 dead", answer)
	}
	wantDead := deadListOf(t, source, "stack")
	gotDead := deadListOf(t, dest, "stack")
	if len(gotDead) != len(wantDead) {
		t.Fatalf("the restored bucket lists %d dead entries, want %d", len(gotDead), len(wantDead))
	}
	for i := range wantDead {
		if string(gotDead[i].Data) != string(wantDead[i].Data) || gotDead[i].Reason != wantDead[i].Reason {
			t.Errorf("dead entry %d came back as %v (%s), want %v (%s)", i, gotDead[i].Data, gotDead[i].Reason, wantDead[i].Data, wantDead[i].Reason)
		}
	}
	popped := popAllEntries(t, dest, "stack")
	if len(popped) != 8 {
		t.Fatalf("the restored bucket popped %d entries, want 8", len(popped))
	}
	for i, entry := range popped {
		want := poppedEntry{id: fmt.Sprintf("e%d", 8-i), n: int64(8 - i)}
		if entry != want {
			t.Fatalf("restored pop %d is %+v, want %+v: a LIFO restore must keep the pop order and the ids", i, entry, want)
		}
	}
}

// TestLIFORestoreStoppedBeforeTheFinishWritesNothing: a LIFO restore whose
// connection closes after its chunks and before RESTORE_DONE leaves the bucket
// with none of the dump's entries, rather than a part of the stack that would
// pop as if it were whole.
func TestLIFORestoreStoppedBeforeTheFinishWritesNothing(t *testing.T) {
	source := newTestServer(t)
	createTestBucket(t, source, "app", "stack", BucketTypeLIFO)
	pushPeekEntries(t, source, "stack", 20, "")
	dumped := dumpSections(t, source, "stack", DumpStartRequest{})

	dest := newTestServer(t)
	ctx, closeConnection := context.WithCancel(rootContext())
	session := startLIFORestore(t, ctx, dest, dumped)
	for i, chunk := range restoreChunksOf(t, dumped.sections[""], 256) {
		if _, err := dest.restoreChunk(rootContext(), RestoreChunkRequest{
			Database: "app", Bucket: "stack", SessionID: session.id, Data: chunk,
		}); err != nil {
			t.Fatalf("restoring chunk %d: %v", i, err)
		}
	}
	closeConnection()
	waitForRestoreRelease(t, dest, session)
	if popped := popAllEntries(t, dest, "stack"); len(popped) != 0 {
		t.Fatalf("a restore stopped before its finish left %d entries in the bucket, want none", len(popped))
	}
}

// waitForRestoreRelease waits until the connection watcher has dropped the
// session, as it does when the restore's connection closes.
func waitForRestoreRelease(t *testing.T, s *Server, session *restoreSession) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := s.activeRestoreFor(session.id, session.database, session.bucket); err != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the restore session is still open 5s after its connection closed")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// lifoSpillFiles lists the LIFO restore spill files in s's data directory.
func lifoSpillFiles(t *testing.T, s *Server) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(s.config.Storage.DataDir, lifoSpillPattern))
	if err != nil {
		t.Fatalf("listing spill files: %v", err)
	}
	return files
}

// spillStackDump is a LIFO bucket of entries carrying marker, dumped.
func spillStackDump(t *testing.T, marker string, pads ...int) dumpedBucket {
	t.Helper()
	source := newTestServer(t)
	createTestBucket(t, source, "app", "stack", BucketTypeLIFO)
	for n, pad := range pads {
		if answer, err := source.Push(PushRequest{Database: "app", Bucket: "stack",
			Data: map[string]interface{}{"n": int64(n + 1), "marker": marker, "pad": strings.Repeat("p", pad)}}); err != nil || !answer.Success {
			t.Fatalf("pushing entry %d: %+v, %v", n+1, answer, err)
		}
	}
	return dumpSections(t, source, "stack", DumpStartRequest{})
}

// sendLIFOChunks sends a dump's documents to session in small chunks and
// checks the spill file exists, so the removal tests cannot pass vacuously.
func sendLIFOChunks(t *testing.T, dest *Server, session *restoreSession, dumped dumpedBucket) {
	t.Helper()
	for i, chunk := range restoreChunksOf(t, dumped.sections[""], 512) {
		if _, err := dest.restoreChunk(rootContext(), RestoreChunkRequest{
			Database: "app", Bucket: "stack", SessionID: session.id, Data: chunk,
		}); err != nil {
			t.Fatalf("restoring chunk %d: %v", i, err)
		}
	}
	if files := lifoSpillFiles(t, dest); len(files) != 1 {
		t.Fatalf("a LIFO restore in progress has %d spill files, want 1", len(files))
	}
}

// waitForNoSpill waits for the session's release to remove the spill.
func waitForNoSpill(t *testing.T, s *Server) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for len(lifoSpillFiles(t, s)) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the LIFO restore's spill file is still there: %v", lifoSpillFiles(t, s))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestLIFORestoreRemovesItsSpillFile: the spill file is gone after a restore
// that finishes, one whose connection closes first, and one whose finish fails
// partway through its pushes.
func TestLIFORestoreRemovesItsSpillFile(t *testing.T) {
	t.Run("finished", func(t *testing.T) {
		dumped := spillStackDump(t, "m", 10, 10, 10, 10)
		dest := newTestServer(t)
		session := startLIFORestore(t, rootContext(), dest, dumped)
		sendLIFOChunks(t, dest, session, dumped)
		done, err := dest.restoreDone(rootContext(), RestoreDoneRequest{
			Database: "app", Bucket: "stack", SessionID: session.id, Documents: dumped.done.Documents,
		})
		if err != nil || !done.Success || done.Applied != 4 {
			t.Fatalf("finishing the restore: %+v, %v; want 4 applied", done, err)
		}
		if files := lifoSpillFiles(t, dest); len(files) != 0 {
			t.Fatalf("a finished LIFO restore left its spill file: %v", files)
		}
	})

	t.Run("connection_closed", func(t *testing.T) {
		dumped := spillStackDump(t, "m", 10, 10, 10, 10)
		dest := newTestServer(t)
		ctx, closeConnection := context.WithCancel(rootContext())
		session := startLIFORestore(t, ctx, dest, dumped)
		sendLIFOChunks(t, dest, session, dumped)
		closeConnection()
		waitForRestoreRelease(t, dest, session)
		waitForNoSpill(t, dest)
	})

	t.Run("finish_failed", func(t *testing.T) {
		// The third entry is over the destination's document limit, which a
		// chunk does not measure and the finish's push does.
		dumped := spillStackDump(t, "m", 10, 10, 8<<10, 10, 10)
		dest := newTestServerWith(t, func(cfg *config.Config) { cfg.Storage.MaxDocumentBytes = config.MinDocumentBytes })
		session := startLIFORestore(t, rootContext(), dest, dumped)
		sendLIFOChunks(t, dest, session, dumped)
		if _, err := dest.restoreDone(rootContext(), RestoreDoneRequest{
			Database: "app", Bucket: "stack", SessionID: session.id, Documents: dumped.done.Documents,
		}); err == nil {
			t.Fatalf("finishing a restore whose third push is refused succeeded, want the refusal")
		}
		if files := lifoSpillFiles(t, dest); len(files) != 0 {
			t.Fatalf("a failed LIFO finish left its spill file: %v", files)
		}
	})
}

// TestLIFORestoreSpillHoldsNoPlaintext: the spill file carries no document
// bytes in the clear, so a node with encryption at rest writes none.
func TestLIFORestoreSpillHoldsNoPlaintext(t *testing.T) {
	const marker = "spill-marker-7f3a9c"
	dumped := spillStackDump(t, marker, 10, 10, 10)
	dest := newTestServer(t)
	session := startLIFORestore(t, rootContext(), dest, dumped)
	sendLIFOChunks(t, dest, session, dumped)
	session.setupMu.Lock()
	flushErr := session.lifoSpill.writer.Flush()
	session.setupMu.Unlock()
	if flushErr != nil {
		t.Fatalf("flushing the spill: %v", flushErr)
	}
	spilled, err := os.ReadFile(lifoSpillFiles(t, dest)[0])
	if err != nil {
		t.Fatalf("reading the spill file: %v", err)
	}
	if len(spilled) == 0 || strings.Contains(string(spilled), marker) {
		t.Fatalf("the spill file (%d bytes) holds a document's text in the clear", len(spilled))
	}
}

// TestStartRemovesALeftoverLIFOSpill: a spill file a stopped node left behind
// is removed when the node starts.
func TestStartRemovesALeftoverLIFOSpill(t *testing.T) {
	var leftover string
	s := newTestServerWith(t, func(cfg *config.Config) {
		leftover = filepath.Join(cfg.Storage.DataDir, ".restore-lifo-123.spill")
		if err := os.WriteFile(leftover, []byte("sealed bytes nobody can open"), 0o600); err != nil {
			t.Fatalf("writing a leftover spill: %v", err)
		}
	})
	if _, err := os.Stat(leftover); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the leftover spill %s is still there after a start (stat: %v)", leftover, err)
	}
	if files := lifoSpillFiles(t, s); len(files) != 0 {
		t.Fatalf("spill files after a start: %v", files)
	}
}

// TestLIFORestoreHeadroomCountsItsSpill: free space that admits a FIFO
// restore of a dump refuses a LIFO restore of the same size, whose spill file
// needs the dump's size again, and the refusal says so.
func TestLIFORestoreHeadroomCountsItsSpill(t *testing.T) {
	s := newTestServer(t)
	orig := diskFreeBytes
	diskFreeBytes = func(string) (int64, error) { return 25_000, nil } // 2.0x of 10,000 fits; 3.0x does not
	t.Cleanup(func() { diskFreeBytes = orig })

	for _, storeType := range []BucketType{BucketTypeFIFO, BucketTypeLIFO} {
		_, err := s.startRestore(rootContext(), RestoreStartRequest{Database: "app", Bucket: string(storeType),
			FormatVersion: dumpManifestFormatVersion, StoreType: storeType, Bytes: 10_000, DryRun: true})
		switch {
		case storeType == BucketTypeFIFO && err != nil:
			t.Fatalf("a FIFO restore that fits was refused: %v", err)
		case storeType == BucketTypeLIFO && (!errors.Is(err, ErrRestoreInsufficientDiskSpace) || !strings.Contains(err.Error(), "spill file")):
			t.Fatalf("a LIFO restore without room for its spill answered %v, want the headroom refusal naming the spill file", err)
		}
	}
}
