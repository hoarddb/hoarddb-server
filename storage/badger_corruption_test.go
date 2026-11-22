package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The tests in this file damage a closed store's files on disk and reopen it
// in a child process (the test binary re-run with corruptHelperDirEnv set), so
// that a panic inside Badger, which may happen on a goroutine of its own where
// nothing can recover it, is a reported outcome rather than the end of the
// test binary.

const (
	corruptHelperDirEnv = "HOARDDB_CORRUPTION_HELPER_DIR"
	corruptHelperKeyEnv = "HOARDDB_CORRUPTION_HELPER_KEY"

	corruptDocumentCount = 3000
)

func corruptKey(index int) []byte {
	return []byte(fmt.Sprintf("doc-%05d", index))
}

// corruptValue is 256 bytes derived from the key, so a damaged block cannot
// compress away and a wrong value read back is told apart from the right one.
func corruptValue(index int) []byte {
	var valueBytes []byte
	seed := corruptKey(index)
	for len(valueBytes) < 256 {
		digest := sha256.Sum256(seed)
		valueBytes = append(valueBytes, digest[:]...)
		seed = digest[:]
	}
	return valueBytes[:256]
}

// TestCorruptionHelperProcess is not a test: it opens the store named by
// corruptHelperDirEnv, reads every document and prints one outcome line.
func TestCorruptionHelperProcess(t *testing.T) {
	dir := os.Getenv(corruptHelperDirEnv)
	if dir == "" {
		t.Skip("helper process only")
	}
	encryptionKey, err := hex.DecodeString(os.Getenv(corruptHelperKeyEnv))
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: bad key: %v\n", err)
		os.Exit(2)
	}
	if len(encryptionKey) == 0 {
		encryptionKey = nil
	}
	kv, err := OpenBadgerKV(dir, BadgerTuning{MemTableMB: 32}, encryptionKey)
	if err != nil {
		fmt.Printf("OUTCOME open-error\nERROR %v\n", err)
		os.Exit(0)
	}
	var found, missing, wrong, failed int
	var firstFailure error
	snapshot := kv.NewSnapshot()
	for index := 0; index < corruptDocumentCount; index++ {
		storedValue, getErr := snapshot.Get(corruptKey(index))
		switch {
		case errors.Is(getErr, ErrKeyNotFound):
			missing++
		case getErr != nil:
			failed++
			if firstFailure == nil {
				firstFailure = getErr
			}
		case !bytes.Equal(storedValue, corruptValue(index)):
			wrong++
		default:
			found++
		}
	}
	scanLines := []string{scanEveryDocument(snapshot, false), scanEveryDocument(snapshot, true)}
	snapshot.Discard()
	if err := kv.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "helper: close: %v\n", err)
	}
	fmt.Printf("OUTCOME opened found=%d missing=%d wrong=%d failed=%d\n", found, missing, wrong, failed)
	if firstFailure != nil {
		fmt.Printf("ERROR %v\n", firstFailure)
	}
	for _, scanLine := range scanLines {
		fmt.Println(scanLine)
	}
	os.Exit(0)
}

// scanEveryDocument walks the whole store in one direction and returns one
// line: how many documents the walk returned, how many of those had a wrong
// value or failed to read one, and the error the walk ended on.
func scanEveryDocument(snapshot KVSnapshot, reverse bool) string {
	direction := "forward"
	if reverse {
		direction = "reverse"
	}
	iterator := snapshot.NewIterator(IterOptions{Reverse: reverse})
	defer iterator.Close()
	var seen, wrong int
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		seen++
		var index int
		if _, err := fmt.Sscanf(string(iterator.Key()), "doc-%05d", &index); err != nil {
			wrong++
			continue
		}
		storedValue, err := iterator.Value()
		if err != nil || !bytes.Equal(storedValue, corruptValue(index)) {
			wrong++
		}
	}
	if walkErr := iterator.Err(); walkErr != nil {
		return fmt.Sprintf("SCAN %s seen=%d wrong=%d ended=error %s", direction, seen, wrong, strings.ReplaceAll(walkErr.Error(), "\n", " "))
	}
	return fmt.Sprintf("SCAN %s seen=%d wrong=%d ended=end", direction, seen, wrong)
}

type corruptOutcome struct {
	openError    string
	found        int
	missing      int
	wrong        int
	failed       int
	readError    string
	panicked     bool
	helperOutput string
	scans        []scanOutcome
}

// scanOutcome is one SCAN line of the helper: a walk over the whole store.
type scanOutcome struct {
	direction string
	seen      int
	wrong     int
	// endError is the error the walk ended on, empty when it reached the end.
	endError string
}

func reopenInChild(t *testing.T, dir string, encryptionKey []byte) corruptOutcome {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCorruptionHelperProcess$")
	cmd.Env = append(os.Environ(),
		corruptHelperDirEnv+"="+dir, corruptHelperKeyEnv+"="+hex.EncodeToString(encryptionKey))
	helperOutput, runErr := cmd.CombinedOutput()
	outcome := corruptOutcome{helperOutput: string(helperOutput)}
	if runErr != nil {
		outcome.panicked = strings.Contains(outcome.helperOutput, "panic")
		if !outcome.panicked {
			t.Fatalf("helper failed without a panic: %v\n%s", runErr, helperOutput)
		}
		return outcome
	}
	for _, line := range strings.Split(outcome.helperOutput, "\n") {
		switch {
		case line == "OUTCOME open-error":
			outcome.openError = "?"
		case strings.HasPrefix(line, "OUTCOME opened "):
			if _, err := fmt.Sscanf(line, "OUTCOME opened found=%d missing=%d wrong=%d failed=%d",
				&outcome.found, &outcome.missing, &outcome.wrong, &outcome.failed); err != nil {
				t.Fatalf("parsing helper line %q: %v", line, err)
			}
		case strings.HasPrefix(line, "SCAN "):
			outcome.scans = append(outcome.scans, parseScanLine(t, line))
		case strings.HasPrefix(line, "ERROR "):
			if outcome.openError != "" {
				outcome.openError = strings.TrimPrefix(line, "ERROR ")
			} else {
				outcome.readError = strings.TrimPrefix(line, "ERROR ")
			}
		}
	}
	return outcome
}

func parseScanLine(t *testing.T, line string) scanOutcome {
	t.Helper()
	var scan scanOutcome
	var ended string
	if _, err := fmt.Sscanf(line, "SCAN %s seen=%d wrong=%d ended=%s", &scan.direction, &scan.seen, &scan.wrong, &ended); err != nil {
		t.Fatalf("parsing helper line %q: %v", line, err)
	}
	if ended == "error" {
		_, scan.endError, _ = strings.Cut(line, "ended=error ")
	}
	return scan
}

// writeClosedStore writes corruptDocumentCount documents and closes the store,
// which flushes the memtable to a table and removes its log, so every document
// lives only in table files.
func writeClosedStore(t testing.TB, encryptionKey []byte) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")
	kv, err := OpenBadgerKV(dir, BadgerTuning{MemTableMB: 32}, encryptionKey)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	batch := kv.NewBatch()
	for index := 0; index < corruptDocumentCount; index++ {
		if err := batch.Set(corruptKey(index), corruptValue(index)); err != nil {
			t.Fatalf("setting document %d: %v", index, err)
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing: %v", err)
	}
	if err := kv.Close(); err != nil {
		t.Fatalf("closing the store: %v", err)
	}
	return dir
}

func tableFiles(t testing.TB, dir string) []string {
	t.Helper()
	tablePaths, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		t.Fatalf("listing tables: %v", err)
	}
	if len(tablePaths) == 0 {
		t.Fatalf("the closed store at %s holds no table file", dir)
	}
	sort.Strings(tablePaths)
	return tablePaths
}

var corruptionModes = []struct {
	name          string
	encryptionKey []byte
}{
	{name: "plaintext"},
	{name: "encrypted", encryptionKey: bytes.Repeat([]byte{0x42}, 32)},
}

// TestATruncatedTableIsAnOpenErrorNotAPanic defends the table case: a table
// cut short, or with its end zeroed the way an unsynced copy leaves it, must
// refuse the open with an error naming the file, never panic.
func TestATruncatedTableIsAnOpenErrorNotAPanic(t *testing.T) {
	damages := []struct {
		name   string
		damage func(tableBytes []byte) []byte
	}{
		{name: "cut in half", damage: func(tableBytes []byte) []byte { return tableBytes[:len(tableBytes)/2] }},
		{name: "cut by one byte", damage: func(tableBytes []byte) []byte { return tableBytes[:len(tableBytes)-1] }},
		{name: "cut by 4 KiB", damage: func(tableBytes []byte) []byte { return tableBytes[:len(tableBytes)-4096] }},
		{name: "cut to 3 bytes", damage: func(tableBytes []byte) []byte { return tableBytes[:3] }},
		{name: "last 4 KiB zeroed", damage: func(tableBytes []byte) []byte {
			clear(tableBytes[len(tableBytes)-4096:])
			return tableBytes
		}},
		{name: "index byte flipped", damage: func(tableBytes []byte) []byte {
			checksumLength := int(binary.BigEndian.Uint32(tableBytes[len(tableBytes)-4:]))
			indexEnd := len(tableBytes) - 4 - checksumLength - 4
			tableBytes[indexEnd-10] ^= 0xFF
			return tableBytes
		}},
	}
	for _, mode := range corruptionModes {
		for _, damageCase := range damages {
			t.Run(mode.name+"/"+damageCase.name, func(t *testing.T) {
				dir := writeClosedStore(t, mode.encryptionKey)
				tablePath := tableFiles(t, dir)[0]
				tableBytes, err := os.ReadFile(tablePath)
				if err != nil {
					t.Fatalf("reading %s: %v", tablePath, err)
				}
				if err := os.WriteFile(tablePath, damageCase.damage(tableBytes), 0o600); err != nil {
					t.Fatalf("writing %s: %v", tablePath, err)
				}

				outcome := reopenInChild(t, dir, mode.encryptionKey)
				if outcome.panicked {
					t.Fatalf("opening a store with a damaged table panicked:\n%s", outcome.helperOutput)
				}
				if outcome.openError == "" {
					t.Fatalf("opening a store with a damaged table succeeded (found=%d missing=%d)",
						outcome.found, outcome.missing)
				}
				if !strings.Contains(outcome.openError, filepath.Base(tablePath)) {
					t.Fatalf("the open error does not name the damaged table %s: %s",
						filepath.Base(tablePath), outcome.openError)
				}
				if !strings.Contains(outcome.openError, "backup") {
					t.Fatalf("the open error does not say how to repair the store: %s", outcome.openError)
				}
				t.Logf("refused: %s", outcome.openError)
			})
		}
	}
}

// TestATruncatedManifestIsAnOpenErrorNotAnEmptyStore defends the manifest case.
// Badger replays a manifest cut short as the history it holds, and removes
// every table file that history does not name, so the node would start empty
// and its tables would be gone.
func TestATruncatedManifestIsAnOpenErrorNotAnEmptyStore(t *testing.T) {
	cases := []struct {
		name   string
		damage func(manifestPath string) error
	}{
		{name: "cut to its header", damage: func(manifestPath string) error { return os.Truncate(manifestPath, 8) }},
		{name: "cut inside its first record", damage: func(manifestPath string) error { return os.Truncate(manifestPath, 12) }},
		{name: "deleted", damage: os.Remove},
	}
	for _, damageCase := range cases {
		t.Run(damageCase.name, func(t *testing.T) {
			dir := writeClosedStore(t, nil)
			tablesBefore := tableFiles(t, dir)
			if err := damageCase.damage(filepath.Join(dir, "MANIFEST")); err != nil {
				t.Fatalf("damaging the manifest: %v", err)
			}

			outcome := reopenInChild(t, dir, nil)
			if outcome.panicked {
				t.Fatalf("opening a store with a damaged manifest panicked:\n%s", outcome.helperOutput)
			}
			if outcome.openError == "" {
				t.Errorf("a store whose manifest was %s opened with found=%d missing=%d; it must refuse",
					damageCase.name, outcome.found, outcome.missing)
			}
			for _, tablePath := range tablesBefore {
				if _, err := os.Stat(tablePath); err != nil {
					t.Errorf("table %s did not survive the open: %v", tablePath, err)
				}
			}
			if outcome.openError != "" && !strings.Contains(outcome.openError, "backup") {
				t.Errorf("the refusal does not say how to repair the store: %s", outcome.openError)
			}
		})
	}
}

// TestALeftoverTableBesideAManifestThatNamesTablesStillOpens keeps the guard
// from refusing what a crash leaves: Badger writes a table before its manifest
// record, so a kill between the two leaves a table no record names, and
// Badger deletes it at the next open. The leftover is zeroed, as an unsynced
// write leaves it, so a footer check on it would refuse.
func TestALeftoverTableBesideAManifestThatNamesTablesStillOpens(t *testing.T) {
	dir := writeClosedStore(t, nil)
	leftoverPath := filepath.Join(dir, "000099.sst")
	if err := os.WriteFile(leftoverPath, make([]byte, 64<<10), 0o600); err != nil {
		t.Fatalf("writing the leftover table: %v", err)
	}

	outcome := reopenInChild(t, dir, nil)
	if outcome.panicked || outcome.openError != "" {
		t.Fatalf("a store with a crash's leftover table did not open: %s%s", outcome.openError, outcome.helperOutput)
	}
	if outcome.found != corruptDocumentCount {
		t.Fatalf("found %d of %d documents", outcome.found, corruptDocumentCount)
	}
	if _, err := os.Stat(leftoverPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Badger was expected to remove the leftover table, stat says: %v", err)
	}
}

// TestACorruptTableBlockIsAnErrorNotAMiss defends against a regression: one bit flipped
// inside a data block of a table must make a read of a key in that block fail,
// and a walk across the block end on an error. Before the fix the block's
// checksum failure was dropped by Badger's point lookup (levelHandler.get) and
// by its table iterators, so the documents read as never written and a walk
// stepped over them.
func TestACorruptTableBlockIsAnErrorNotAMiss(t *testing.T) {
	for _, mode := range corruptionModes {
		t.Run(mode.name, func(t *testing.T) {
			dir := writeClosedStore(t, mode.encryptionKey)
			tablePath := tableFiles(t, dir)[0]
			tableBytes, err := os.ReadFile(tablePath)
			if err != nil {
				t.Fatalf("reading %s: %v", tablePath, err)
			}
			// A third of the way in is inside the data blocks: the index and
			// its checksum sit at the end of the file.
			tableBytes[len(tableBytes)/3] ^= 0x10
			if err := os.WriteFile(tablePath, tableBytes, 0o600); err != nil {
				t.Fatalf("writing %s: %v", tablePath, err)
			}

			outcome := reopenInChild(t, dir, mode.encryptionKey)
			if outcome.panicked {
				t.Fatalf("reading a store with a damaged block panicked:\n%s", outcome.helperOutput)
			}
			if outcome.openError != "" {
				t.Fatalf("a damaged data block refused the open (%s); this test needs the damage found on read", outcome.openError)
			}
			t.Logf("point reads: found=%d missing=%d wrong=%d failed=%d; first error: %s",
				outcome.found, outcome.missing, outcome.wrong, outcome.failed, outcome.readError)
			if outcome.missing > 0 {
				t.Errorf("%d documents in the damaged block read as not found with no error", outcome.missing)
			}
			if outcome.wrong > 0 {
				t.Errorf("%d documents in the damaged block read back a wrong value with no error", outcome.wrong)
			}
			if outcome.failed == 0 {
				t.Errorf("no read failed, so the damage went unreported")
			}
			tableName := filepath.Base(tablePath)
			for _, wanted := range []string{"is damaged", tableName, "backup"} {
				if !strings.Contains(outcome.readError, wanted) {
					t.Errorf("the failed read's error does not say %q: %s", wanted, outcome.readError)
				}
			}
			if len(outcome.scans) != 2 {
				t.Fatalf("the helper reported %d walks, want 2:\n%s", len(outcome.scans), outcome.helperOutput)
			}
			for _, scan := range outcome.scans {
				t.Logf("%s walk: seen=%d wrong=%d ended on: %q", scan.direction, scan.seen, scan.wrong, scan.endError)
				if scan.wrong > 0 {
					t.Errorf("the %s walk returned %d documents with a wrong value", scan.direction, scan.wrong)
				}
				if scan.endError == "" {
					t.Errorf("the %s walk returned %d of %d documents and ended with no error: it stepped over the damaged block",
						scan.direction, scan.seen, corruptDocumentCount)
				} else if !strings.Contains(scan.endError, "is damaged") || !strings.Contains(scan.endError, tableName) {
					t.Errorf("the %s walk's error does not say the store file %s is damaged: %s", scan.direction, tableName, scan.endError)
				}
			}
		})
	}
}
