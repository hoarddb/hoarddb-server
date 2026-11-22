package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/dgraph-io/badger/v4"

	"github.com/hoarddb/hoarddb-server/internal/repofile"
)

const (
	helperModeEnv = "HOARDDB_MEMTABLE_HELPER"
	helperDirEnv  = "HOARDDB_MEMTABLE_HELPER_DIR"
	helperSizeEnv = "HOARDDB_MEMTABLE_HELPER_MB"
)

// TestMemtableHelperProcess is not a test: the tests below re-run the test
// binary with helperModeEnv set, to do what cannot be done in the test's own
// process (die by SIGKILL with a memtable unflushed, or let Badger kill the
// process the way it does).
func TestMemtableHelperProcess(t *testing.T) {
	mode := os.Getenv(helperModeEnv)
	if mode == "" {
		t.Skip("helper process only")
	}
	dir := os.Getenv(helperDirEnv)
	var sizeMB int
	if _, err := fmt.Sscan(os.Getenv(helperSizeEnv), &sizeMB); err != nil {
		fmt.Fprintf(os.Stderr, "helper: bad size: %v\n", err)
		os.Exit(2)
	}
	switch mode {
	case "open-and-die":
		// Open at sizeMB and write nothing, then SIGKILL. Badger creates a
		// memtable log on every open, so the crash leaves one although there
		// is nothing to replay.
		if _, err := OpenBadgerKV(dir, BadgerTuning{MemTableMB: sizeMB}, nil); err != nil {
			fmt.Fprintf(os.Stderr, "helper: open: %v\n", err)
			os.Exit(2)
		}
		if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
			fmt.Fprintf(os.Stderr, "helper: kill: %v\n", err)
			os.Exit(2)
		}
	case "write-and-die":
		kv, err := OpenBadgerKV(dir, BadgerTuning{MemTableMB: sizeMB}, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "helper: open: %v\n", err)
			os.Exit(2)
		}
		writeUnflushed(kv)
		// SIGKILL, not Close: the memtable stays in its write-ahead log.
		if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
			fmt.Fprintf(os.Stderr, "helper: kill: %v\n", err)
			os.Exit(2)
		}
	case "guarded-open":
		// OpenBadgerKV in a process of its own, because a store the guard lets
		// through to Badger with a log too large for the arena exits the process.
		kv, err := OpenBadgerKV(dir, BadgerTuning{MemTableMB: sizeMB}, nil)
		var shrink *MemtableShrinkError
		if errors.As(err, &shrink) {
			fmt.Fprintf(os.Stderr, "refused previous=%d requested=%d: %v\n", shrink.PreviousMB, shrink.RequestedMB, err)
			os.Exit(3)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "helper: open: %v\n", err)
			os.Exit(2)
		}
		if err := kv.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "helper: close: %v\n", err)
			os.Exit(2)
		}
		os.Exit(0)
	case "badger-direct-open":
		// What the server did before the guard: hand the directory to Badger.
		opts, err := badgerOptions(dir, BadgerTuning{MemTableMB: sizeMB}, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "helper: options: %v\n", err)
			os.Exit(2)
		}
		if _, err := badger.Open(opts); err != nil {
			fmt.Fprintf(os.Stderr, "helper: open: %v\n", err)
			os.Exit(2)
		}
		os.Exit(0)
	}
}

// writeUnflushed puts about 60 MB into the store: more than the 42 MB a
// 32 MiB memtable's arena can replay, less than the 128 MiB that flushes it.
func writeUnflushed(kv *BadgerKV) {
	payload := bytes.Repeat([]byte("x"), 256<<10)
	for batchIndex := 0; batchIndex < 6; batchIndex++ {
		batch := kv.NewBatch()
		for entryIndex := 0; entryIndex < 40; entryIndex++ {
			key := []byte(fmt.Sprintf("k-%02d-%02d", batchIndex, entryIndex))
			if err := batch.Set(key, payload); err != nil {
				fmt.Fprintf(os.Stderr, "helper: set: %v\n", err)
				os.Exit(2)
			}
		}
		if err := batch.Commit(false); err != nil {
			fmt.Fprintf(os.Stderr, "helper: commit: %v\n", err)
			os.Exit(2)
		}
	}
}

func runHelper(t *testing.T, mode, dir string, sizeMB int) (string, error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestMemtableHelperProcess$")
	cmd.Env = append(os.Environ(),
		helperModeEnv+"="+mode, helperDirEnv+"="+dir, fmt.Sprintf("%s=%d", helperSizeEnv, sizeMB))
	output, err := cmd.CombinedOutput()
	return string(output), err
}

// crashedAt128 leaves a store directory the way kill -9 does at a 128 MiB
// memtable: a write-ahead log of about 60 MB and nothing flushed.
func crashedAt128(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")
	output, err := runHelper(t, "write-and-die", dir, 128)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("helper was expected to die by SIGKILL, got err=%v output=%s", err, output)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("helper did not die by SIGKILL: %v\n%s", err, output)
	}
	memFiles, globErr := filepath.Glob(filepath.Join(dir, "*.mem"))
	if globErr != nil || len(memFiles) == 0 {
		t.Fatalf("a killed node left no .mem files in %s (glob error %v)", dir, globErr)
	}
	return dir
}

// idleCrashAt128 leaves a store directory the way kill -9 does when the node
// was opened at a 128 MiB memtable and nothing was written: the marker and the
// memtable log exist, and the log holds no entry.
func idleCrashAt128(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")
	output, err := runHelper(t, "open-and-die", dir, 128)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("helper was expected to die by SIGKILL, got err=%v output=%s", err, output)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("helper did not die by SIGKILL: %v\n%s", err, output)
	}
	memFiles, globErr := filepath.Glob(filepath.Join(dir, "*.mem"))
	if globErr != nil || len(memFiles) == 0 {
		t.Fatalf("a killed node left no .mem files in %s (glob error %v)", dir, globErr)
	}
	return dir
}

// TestACrashWithNothingUnflushedDoesNotRefuseASmallerMemtable (second case):
// Badger creates a memtable log on every open, so a node killed before it wrote
// anything leaves one behind although there is nothing to replay. The guard
// must not read that empty log as unflushed writes and refuse a smaller
// memtable. Before the fix the guard refused on the mere presence of a *.mem
// file.
func TestACrashWithNothingUnflushedDoesNotRefuseASmallerMemtable(t *testing.T) {
	dir := idleCrashAt128(t)
	kv, err := OpenBadgerKV(dir, BadgerTuning{MemTableMB: 32}, nil)
	var shrink *MemtableShrinkError
	if errors.As(err, &shrink) {
		t.Fatalf("opening at 32 MiB after a crash with nothing unflushed was refused (%v); there is nothing to replay", err)
	}
	if err != nil {
		t.Fatalf("opening at 32 MiB after a crash with nothing unflushed: %v", err)
	}
	if err := kv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestBadgerAloneKillsTheProcessOnAShrunkCrash is the premise of the guard,
// kept as a test so a Badger upgrade that changes it is noticed: without the
// guard, Badger exits with its own message and no hoardDB wording.
func TestBadgerAloneKillsTheProcessOnAShrunkCrash(t *testing.T) {
	dir := crashedAt128(t)
	output, err := runHelper(t, "badger-direct-open", dir, 32)
	if err == nil {
		t.Fatal("Badger opened a 60 MB write-ahead log into a 32 MiB memtable; the guard's premise no longer holds")
	}
	if !strings.Contains(output, "Arena too small") {
		t.Fatalf("Badger refused with something other than 'Arena too small':\n%s", output)
	}
}

func TestShrinkingAfterACrashIsRefusedBeforeBadgerSeesIt(t *testing.T) {
	dir := crashedAt128(t)

	_, err := OpenBadgerKV(dir, BadgerTuning{MemTableMB: 32}, nil)
	var shrink *MemtableShrinkError
	if !errors.As(err, &shrink) {
		t.Fatalf("OpenBadgerKV at 32 MiB over a crash left at 128 MiB = %v, want a *MemtableShrinkError", err)
	}
	if shrink.PreviousMB != 128 || shrink.RequestedMB != 32 {
		t.Errorf("refusal = %+v, want previous 128 and requested 32", shrink)
	}
	// Operators search for this text, so what it names is part of the contract:
	// the key and the variable to change, both sizes, and the directory.
	for _, want := range []string{
		"Memtable size mismatch", "storage.memtable_mb", "HOARDDB_MEMTABLE_MB",
		"this node is set to 32 MiB by storage.memtable_mb or HOARDDB_MEMTABLE_MB", "last running with 128 MiB", dir,
		"not stopped cleanly", "Nothing is lost", "set storage.memtable_mb (or HOARDDB_MEMTABLE_MB) to 128", "lower it to 32",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "Arena too small") {
		t.Errorf("refusal %q surfaces Badger's own message, which tells an operator nothing", err)
	}

	// The instruction the refusal gives works: start at the old size, stop
	// cleanly, then the smaller size opens.
	kv, err := OpenBadgerKV(dir, BadgerTuning{MemTableMB: 128}, nil)
	if err != nil {
		t.Fatalf("reopening at the previous size: %v", err)
	}
	if err := kv.Close(); err != nil {
		t.Fatalf("clean stop: %v", err)
	}
	kv, err = OpenBadgerKV(dir, BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening at 32 MiB after a clean stop: %v", err)
	}
	if err := kv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestMemtableSizeMayChangeWhenNothingIsUnflushed(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	for _, sizeMB := range []int{128, 32, 256, 64} {
		kv, err := OpenBadgerKV(dir, BadgerTuning{MemTableMB: sizeMB}, nil)
		if err != nil {
			t.Fatalf("opening at %d MiB after clean stops: %v", sizeMB, err)
		}
		if err := kv.Close(); err != nil {
			t.Fatalf("Close at %d MiB: %v", sizeMB, err)
		}
	}
}

func TestGrowingAfterACrashIsAllowed(t *testing.T) {
	dir := crashedAt128(t)
	kv, err := OpenBadgerKV(dir, BadgerTuning{MemTableMB: 256}, nil)
	if err != nil {
		t.Fatalf("opening a crash left at 128 MiB at 256 MiB: %v", err)
	}
	if err := kv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestAStoreWithNoRecordedSizeCountsAsTheOldDefault(t *testing.T) {
	dir := crashedAt128(t)
	if err := os.Remove(filepath.Join(dir, MemtableSizeFileName)); err != nil {
		t.Fatalf("removing the marker to look like an earlier build's directory: %v", err)
	}
	// With no marker the logs say 128 MiB, and opening at that size
	// is no shrink: the guard has nothing to refuse.
	kv, err := OpenBadgerKV(dir, BadgerTuning{MemTableMB: 128}, nil)
	if err != nil {
		t.Fatalf("opening an unrecorded directory: %v", err)
	}
	if err := kv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestTheDocumentedRefusalIsTheRealOne: an operator who searches the message
// must land on the page that explains it, so the page quotes it word for word
// (line breaks aside), in both the forms a server words it: a size the store
// chose, and one a setting asked for.
func TestTheDocumentedRefusalIsTheRealOne(t *testing.T) {
	page := repofile.Read(t, "docs/user/configuration.md")
	for _, refusal := range []*MemtableShrinkError{
		{Dir: "/var/lib/hoarddb/store", PreviousMB: 256, RequestedMB: 64, LogFiles: []string{"000001.mem"}, RequestedByChoice: true},
		{Dir: "/var/lib/hoarddb/store", PreviousMB: 256, RequestedMB: 128, LogFiles: []string{"000001.mem"}, RequestedBy: "HOARDDB_MEMTABLE_MB"},
	} {
		if !strings.Contains(strings.Join(strings.Fields(string(page)), " "), strings.Join(strings.Fields(refusal.Error()), " ")) {
			t.Errorf("docs/user/configuration.md does not quote the refusal as the server words it:\n%s", refusal.Error())
		}
	}
}

// TestRecordMemtableSizeSyncsTheFileAndTheDirectory: a power cut is
// not available to a test, so this is a substitute for the real property. It
// records the two syncs through the package's seams and checks their order:
// the temporary file is synced before it is renamed over the marker, and the
// directory is synced after the rename, which is what makes the rename itself
// durable.
func TestRecordMemtableSizeSyncsTheFileAndTheDirectory(t *testing.T) {
	dir := t.TempDir()
	markerPath := filepath.Join(dir, MemtableSizeFileName)
	var events []string
	markerInPlace := func() bool {
		text, err := os.ReadFile(markerPath)
		return err == nil && strings.TrimSpace(string(text)) == "67108864"
	}
	originalFileSync, originalDirectorySync := syncFile, syncDirectory
	t.Cleanup(func() { syncFile, syncDirectory = originalFileSync, originalDirectorySync })
	syncFile = func(file *os.File) error {
		events = append(events, fmt.Sprintf("file %s, marker in place %v", filepath.Base(file.Name()), markerInPlace()))
		return file.Sync()
	}
	syncDirectory = func(syncedDir string) error {
		events = append(events, fmt.Sprintf("directory %s, marker in place %v", syncedDir, markerInPlace()))
		return nil
	}

	if err := recordMemtableSize(dir, 64<<20); err != nil {
		t.Fatalf("recordMemtableSize: %v", err)
	}
	want := []string{
		"file " + MemtableSizeFileName + ".tmp, marker in place false",
		"directory " + dir + ", marker in place true",
	}
	if strings.Join(events, "\n") != strings.Join(want, "\n") {
		t.Errorf("syncs were\n%s\nwant\n%s", strings.Join(events, "\n"), strings.Join(want, "\n"))
	}
}

// TestAnEmptyMarkerIsNamedAsAPowerLoss: an empty marker is what a
// power loss during its write leaves, so the refusal says so and does not
// suggest deleting the file, which would let a shrunk crash past the guard.
func TestAnEmptyMarkerIsNamedAsAPowerLoss(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("creating the store directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, MemtableSizeFileName), nil, 0o600); err != nil {
		t.Fatalf("writing an empty marker: %v", err)
	}
	_, err := OpenBadgerKV(dir, BadgerTuning{MemTableMB: 32}, nil)
	if err == nil {
		t.Fatal("OpenBadgerKV opened a store whose memtable-size marker is empty")
	}
	for _, want := range []string{"is empty", "power loss", "134217728", "the memtable size this node last ran with"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "delete") {
		t.Errorf("refusal %q suggests deleting the marker after a power loss", err)
	}
}

// TestAStoreWithNoMarkerIsStillRefusedWhenItsLogsAreLarger (first case):
// a store that crashed at 128 MiB and lost its marker (deleted by hand, or a
// directory copied without it) is refused at 32 MiB in hoardDB's words. Before
// the fix the guard took the missing marker for the legacy 32 MiB, let the
// open through, and Badger exited the process with "Arena too small", so the
// open runs in a helper process.
func TestAStoreWithNoMarkerIsStillRefusedWhenItsLogsAreLarger(t *testing.T) {
	dir := crashedAt128(t)
	if err := os.Remove(filepath.Join(dir, MemtableSizeFileName)); err != nil {
		t.Fatalf("removing the marker: %v", err)
	}
	output, err := runHelper(t, "guarded-open", dir, 32)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 3 {
		t.Fatalf("opening at 32 MiB a store crashed at 128 MiB with no marker: err=%v, want the guard's refusal\n%s", err, output)
	}
	if !strings.Contains(output, "refused previous=128 requested=32") {
		t.Errorf("the refusal does not name the 128 MiB the logs were written at:\n%s", output)
	}
	if strings.Contains(output, "Arena too small") {
		t.Errorf("Badger's own fatal was reached:\n%s", output)
	}
}

// TestPreviousMemtableSizeReadsTheLogs: the size a store last ran at is the
// larger of the marker and half the largest write-ahead log, because Badger
// creates each memtable's log at twice the memtable size.
func TestPreviousMemtableSizeReadsTheLogs(t *testing.T) {
	crashed := crashedAt128(t)
	if err := os.Remove(filepath.Join(crashed, MemtableSizeFileName)); err != nil {
		t.Fatalf("removing the marker: %v", err)
	}
	empty := filepath.Join(t.TempDir(), "store")
	if err := os.MkdirAll(empty, 0o700); err != nil {
		t.Fatalf("creating a store directory: %v", err)
	}
	markerOnly := filepath.Join(t.TempDir(), "store")
	if err := os.MkdirAll(markerOnly, 0o700); err != nil {
		t.Fatalf("creating a store directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(markerOnly, MemtableSizeFileName), []byte("268435456\n"), 0o600); err != nil {
		t.Fatalf("writing a marker: %v", err)
	}
	for _, tt := range []struct {
		name   string
		dir    string
		wantMB int
	}{
		{"a crash at 128 MiB with no marker", crashed, 128},
		{"no marker and no logs: an earlier build's 32 MiB", empty, 32},
		{"a marker and no logs", markerOnly, 256},
	} {
		t.Run(tt.name, func(t *testing.T) {
			gotMB, err := PreviousMemtableMB(tt.dir)
			if err != nil {
				t.Fatalf("PreviousMemtableMB: %v", err)
			}
			if gotMB != tt.wantMB {
				t.Errorf("PreviousMemtableMB = %d, want %d", gotMB, tt.wantMB)
			}
		})
	}
}

// TestAnUnresolvedMemtableIsRefused: storage has no memtable default of its
// own, so a caller that forgets to resolve the size is refused rather than
// given one silently (memtable-host-scaled-design.md §10 item 2).
func TestAnUnresolvedMemtableIsRefused(t *testing.T) {
	_, err := OpenBadgerKV(filepath.Join(t.TempDir(), "store"), BadgerTuning{}, nil)
	if err == nil || !strings.Contains(err.Error(), "memtable size") {
		t.Fatalf("OpenBadgerKV with no memtable size = %v, want a refusal naming the memtable size", err)
	}
}
