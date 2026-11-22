package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
)

// The limits the tests use: a "4 GB" VM's MemTotal (the note's example), a
// "3 GB" VM's, and the 2.8 GiB of the note's WARN example.
const (
	limit4GBVM = int64(4080218112)
	limit3GBVM = 29 * gib / 10
	limit28GiB = 28 * gib / 10
)

var memtableTestNow = time.Date(2026, 10, 2, 9, 14, 3, 0, time.UTC)

func fixedMemoryLimit(limitBytes int64) func() (memoryLimit, error) {
	return func() (memoryLimit, error) {
		return memoryLimit{bytes: limitBytes, source: limitMemTotalNoLimit}, nil
	}
}

func memtableTestInputs(storeDir string, limitBytes int64) memtableInputs {
	return memtableInputs{
		storeDir:    storeDir,
		memoryLimit: fixedMemoryLimit(limitBytes),
		onLinux:     true,
		now:         func() time.Time { return memtableTestNow },
	}
}

// withMemoryLimit sizes every server this test opens from limitBytes, and puts
// the package's fixed test limit back afterwards.
func withMemoryLimit(t *testing.T, limitBytes int64) {
	t.Helper()
	previous := memtableMemoryLimit
	memtableMemoryLimit = fixedMemoryLimit(limitBytes)
	t.Cleanup(func() { memtableMemoryLimit = previous })
}

func readChoiceFile(t *testing.T, storeDir string) memtableChoice {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(storeDir, memtableChoiceFileName))
	if err != nil {
		t.Fatalf("reading the choice file: %v", err)
	}
	var choice memtableChoice
	if err := json.Unmarshal(raw, &choice); err != nil {
		t.Fatalf("the choice file %s is not the choice's JSON: %v", raw, err)
	}
	return choice
}

func memtableTestConfig(dataDir string) *config.Config {
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dataDir
	return cfg
}

// openStoreAt opens the node store as a start would, and returns the memtable
// it was opened at, read back from the store's batch bound.
func openStoreAt(t *testing.T, cfg *config.Config) (int64, memtableResolution) {
	t.Helper()
	kv, resolution, err := openNodeStore(cfg, nil)
	if err != nil {
		t.Fatalf("opening the node store: %v", err)
	}
	bound := kv.MaxBatchBytes()
	if err := kv.Close(); err != nil {
		t.Fatalf("closing the node store: %v", err)
	}
	return bound, resolution
}

const (
	resolveHelperModeEnv  = "HOARDDB_MEMTABLE_RESOLVE_HELPER"
	resolveHelperDirEnv   = "HOARDDB_MEMTABLE_RESOLVE_HELPER_DIR"
	resolveHelperLimitEnv = "HOARDDB_MEMTABLE_RESOLVE_HELPER_LIMIT"
	resolveHelperSizeEnv  = "HOARDDB_MEMTABLE_RESOLVE_HELPER_MB"
)

// TestMemtableResolveHelperProcess is not a test: the crash tests re-run the
// test binary with resolveHelperModeEnv set, to die by SIGKILL with a memtable
// unflushed, the way kill -9 leaves a node.
func TestMemtableResolveHelperProcess(t *testing.T) {
	mode := os.Getenv(resolveHelperModeEnv)
	if mode == "" {
		t.Skip("helper process only")
	}
	dir := os.Getenv(resolveHelperDirEnv)
	var kv storage.KV
	var err error
	switch mode {
	case "start-write-die":
		// A start of this build: the resolver decides the size.
		limitBytes, parseErr := strconv.ParseInt(os.Getenv(resolveHelperLimitEnv), 10, 64)
		if parseErr != nil {
			fmt.Fprintf(os.Stderr, "helper: bad limit: %v\n", parseErr)
			os.Exit(2)
		}
		memtableMemoryLimit = fixedMemoryLimit(limitBytes)
		cfg := memtableTestConfig(dir)
		if explicit := os.Getenv(resolveHelperSizeEnv); explicit != "" {
			cfg.Storage.MemtableMB, err = strconv.Atoi(explicit)
			if err != nil {
				fmt.Fprintf(os.Stderr, "helper: bad size: %v\n", err)
				os.Exit(2)
			}
		}
		kv, _, err = openNodeStore(cfg, nil)
	case "earlier-build-write-die":
		// A store written by a build before the resolver: no choice file.
		sizeMB, parseErr := strconv.Atoi(os.Getenv(resolveHelperSizeEnv))
		if parseErr != nil {
			fmt.Fprintf(os.Stderr, "helper: bad size: %v\n", parseErr)
			os.Exit(2)
		}
		kv, err = storage.OpenBadgerKV(filepath.Join(dir, nodeStoreDirName), storage.BadgerTuning{MemTableMB: sizeMB}, nil)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: open: %v\n", err)
		os.Exit(2)
	}
	// About 40 MiB in 4 MiB batches: under every memtable's batch bound, and
	// left unflushed in the write-ahead log.
	payload := bytes.Repeat([]byte("x"), 256<<10)
	for batchIndex := 0; batchIndex < 10; batchIndex++ {
		batch := kv.NewBatch()
		for entryIndex := 0; entryIndex < 16; entryIndex++ {
			if err := batch.Set([]byte(fmt.Sprintf("crash-%02d-%02d", batchIndex, entryIndex)), payload); err != nil {
				fmt.Fprintf(os.Stderr, "helper: set: %v\n", err)
				os.Exit(2)
			}
		}
		if err := batch.Commit(false); err != nil {
			fmt.Fprintf(os.Stderr, "helper: commit: %v\n", err)
			os.Exit(2)
		}
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
		fmt.Fprintf(os.Stderr, "helper: kill: %v\n", err)
		os.Exit(2)
	}
}

// crashNodeStore runs the helper in mode against dataDir and requires it to
// die by SIGKILL with write-ahead logs left behind.
func crashNodeStore(t *testing.T, mode, dataDir string, env ...string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestMemtableResolveHelperProcess$")
	cmd.Env = append(append(os.Environ(), resolveHelperModeEnv+"="+mode, resolveHelperDirEnv+"="+dataDir), env...)
	output, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("helper was expected to die by SIGKILL, got err=%v output=%s", err, output)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("helper did not die by SIGKILL: %v\n%s", err, output)
	}
	logs, globErr := filepath.Glob(filepath.Join(dataDir, nodeStoreDirName, "*.mem"))
	if globErr != nil || len(logs) == 0 {
		t.Fatalf("the killed helper left no write-ahead log (glob error %v)", globErr)
	}
}

func TestFreshStoreChoosesFromTheLimitAndPersists(t *testing.T) {
	storeDir := filepath.Join(t.TempDir(), nodeStoreDirName)
	resolution, err := resolveMemtable(memtableTestInputs(storeDir, limit4GBVM))
	if err != nil {
		t.Fatalf("resolveMemtable: %v", err)
	}
	if resolution.appliedMB != 64 || !resolution.chosenNow {
		t.Errorf("a fresh store on a 3.8 GiB host: applied %d MiB, chosen now %v; want 64, true", resolution.appliedMB, resolution.chosenNow)
	}
	choice := readChoiceFile(t, storeDir)
	if choice.MemtableMB != 64 || choice.Source != memtableSourceHost || choice.MemoryLimitBytes != limit4GBVM ||
		choice.MemoryLimitFrom != "the host's MemTotal; no cgroup memory limit is set" || !choice.ChosenAt.Equal(memtableTestNow) || choice.Build == "" {
		t.Errorf("choice file = %+v, want 64 MiB from the host's 3.8 GiB, chosen at %s, with the build", choice, memtableTestNow)
	}
}

// TestChoiceSurvivesACrashBeforeTheFirstOpen: the choice is written before the
// store's first open, so a start that dies in between leaves a choice and no
// store. The next start uses the choice and never sizes again.
func TestChoiceSurvivesACrashBeforeTheFirstOpen(t *testing.T) {
	storeDir := filepath.Join(t.TempDir(), nodeStoreDirName)
	if _, err := resolveMemtable(memtableTestInputs(storeDir, limit4GBVM)); err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	resolution, err := resolveMemtable(memtableTestInputs(storeDir, limit3GBVM))
	if err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	if resolution.appliedMB != 64 || resolution.chosenNow {
		t.Errorf("after a choice of 64 MiB on a host now at 2.9 GiB: applied %d, chosen now %v; want 64 from the file", resolution.appliedMB, resolution.chosenNow)
	}
}

// TestResizedDownHostRestartAfterCrashKeepsTheChosenSize: the hazard the
// "decided once" rule closes. A node chose 64 MiB on a 6 GiB host (a 7 GiB host now chooses 128), was killed
// with writes unflushed, and comes back on a host resized to 2.9 GiB, where a
// new store would choose 32. Re-deriving would ask for 32 and be refused by
// the shrink guard; the node must open at 64.
func TestResizedDownHostRestartAfterCrashKeepsTheChosenSize(t *testing.T) {
	dataDir := t.TempDir()
	crashNodeStore(t, "start-write-die", dataDir, fmt.Sprintf("%s=%d", resolveHelperLimitEnv, 6*gib))

	withMemoryLimit(t, limit3GBVM)
	kv, resolution, err := openNodeStore(memtableTestConfig(dataDir), nil)
	if err != nil {
		t.Fatalf("restarting a node killed at its chosen 64 MiB on a host now at 2.9 GiB was refused: %v", err)
	}
	defer func() {
		if err := kv.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	if resolution.appliedMB != 64 || kv.MaxBatchBytes() != config.MemtableBatchBytes(64) {
		t.Errorf("reopened at %d MiB (batch bound %d), want the chosen 64", resolution.appliedMB, kv.MaxBatchBytes())
	}
}

func TestExplicitEnvBeatsTheChoice(t *testing.T) {
	dataDir := t.TempDir()
	cfg := memtableTestConfig(dataDir)
	if bound, _ := openStoreAt(t, cfg); bound != config.MemtableBatchBytes(64) {
		t.Fatalf("a fresh store opened with batch bound %d, want 64 MiB's", bound)
	}
	choicePath := filepath.Join(dataDir, nodeStoreDirName, memtableChoiceFileName)
	choiceBefore, err := os.ReadFile(choicePath)
	if err != nil {
		t.Fatalf("reading the choice: %v", err)
	}

	t.Setenv("HOARDDB_MEMTABLE_MB", "256")
	cfg.Storage.MemtableMB = 256
	bound, resolution := openStoreAt(t, cfg)
	if bound != config.MemtableBatchBytes(256) {
		t.Errorf("with HOARDDB_MEMTABLE_MB=256 the store opened with batch bound %d, want 256 MiB's", bound)
	}
	if !strings.Contains(resolution.startLine(), "set by HOARDDB_MEMTABLE_MB") {
		t.Errorf("start line %q does not name the variable that set the size", resolution.startLine())
	}
	choiceAfter, err := os.ReadFile(choicePath)
	if err != nil {
		t.Fatalf("reading the choice: %v", err)
	}
	if !bytes.Equal(choiceBefore, choiceAfter) {
		t.Errorf("an explicit size rewrote the choice:\n%s\nwas\n%s", choiceAfter, choiceBefore)
	}

	t.Setenv("HOARDDB_MEMTABLE_MB", "")
	cfg.Storage.MemtableMB = 0
	if bound, _ := openStoreAt(t, cfg); bound != config.MemtableBatchBytes(64) {
		t.Errorf("after unsetting HOARDDB_MEMTABLE_MB the store opened with batch bound %d, want the chosen 64 MiB's", bound)
	}
}

func TestExplicitYAMLBeatsTheChoice(t *testing.T) {
	t.Setenv("HOARDDB_MEMTABLE_MB", "")
	dataDir := t.TempDir()
	if bound, _ := openStoreAt(t, memtableTestConfig(dataDir)); bound != config.MemtableBatchBytes(64) {
		t.Fatalf("a fresh store opened with batch bound %d, want 64 MiB's", bound)
	}
	configPath := filepath.Join(t.TempDir(), "hoardDB.yaml")
	if err := os.WriteFile(configPath, []byte("storage:\n  data_dir: "+dataDir+"\n  memtable_mb: 128\n"), 0o600); err != nil {
		t.Fatalf("writing the config file: %v", err)
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("loading the config file: %v", err)
	}
	bound, resolution := openStoreAt(t, cfg)
	if bound != config.MemtableBatchBytes(128) {
		t.Errorf("with storage.memtable_mb: 128 the store opened with batch bound %d, want 128 MiB's", bound)
	}
	if !strings.Contains(resolution.startLine(), "set by storage.memtable_mb in the config file") {
		t.Errorf("start line %q does not name the config file", resolution.startLine())
	}
	if choice := readChoiceFile(t, filepath.Join(dataDir, nodeStoreDirName)); choice.MemtableMB != 64 {
		t.Errorf("the explicit 128 changed the choice to %d", choice.MemtableMB)
	}
}

// TestAFreshStoreWithAnExplicitSizeStillRecordsItsOwnChoice: removing an
// explicit size goes back to the store's own choice, so a store first opened
// under one still records a choice of its own.
func TestAFreshStoreWithAnExplicitSizeStillRecordsItsOwnChoice(t *testing.T) {
	storeDir := filepath.Join(t.TempDir(), nodeStoreDirName)
	inputs := memtableTestInputs(storeDir, limit4GBVM)
	inputs.explicitMB = 256
	resolution, err := resolveMemtable(inputs)
	if err != nil {
		t.Fatalf("resolveMemtable: %v", err)
	}
	if resolution.appliedMB != 256 {
		t.Errorf("applied %d, want the explicit 256", resolution.appliedMB)
	}
	if choice := readChoiceFile(t, storeDir); choice.MemtableMB != 64 || choice.Source != memtableSourceHost {
		t.Errorf("choice = %+v, want this host's 64 MiB", choice)
	}
}

// The upgrade tests: a store written by a build before the choice existed
// keeps the size it last ran at, and never shrinks, even after a crash and on a
// host that would choose less.

func TestUpgradeMarker128KeepsIt(t *testing.T) {
	dataDir := t.TempDir()
	crashNodeStore(t, "earlier-build-write-die", dataDir, resolveHelperSizeEnv+"=128")
	withMemoryLimit(t, limit3GBVM)
	bound, resolution := openStoreAt(t, memtableTestConfig(dataDir))
	if bound != config.MemtableBatchBytes(128) {
		t.Errorf("an earlier build's store crashed at 128 MiB reopened with batch bound %d, want 128 MiB's", bound)
	}
	if choice := readChoiceFile(t, filepath.Join(dataDir, nodeStoreDirName)); choice.MemtableMB != 128 || choice.Source != memtableSourceUpgrade {
		t.Errorf("choice = %+v, want 128 MiB from the upgrade", choice)
	}
	if resolution.appliedMB != 128 {
		t.Errorf("applied %d, want 128", resolution.appliedMB)
	}
}

func TestUpgradeMarker32KeepsIt(t *testing.T) {
	dataDir := t.TempDir()
	crashNodeStore(t, "earlier-build-write-die", dataDir, resolveHelperSizeEnv+"=32")
	withMemoryLimit(t, 16*gib)
	bound, _ := openStoreAt(t, memtableTestConfig(dataDir))
	if bound != config.MemtableBatchBytes(32) {
		t.Errorf("an earlier build's store crashed at 32 MiB reopened with batch bound %d, want 32 MiB's (kept, not the host's 64)", bound)
	}
	if choice := readChoiceFile(t, filepath.Join(dataDir, nodeStoreDirName)); choice.MemtableMB != 32 || choice.Source != memtableSourceUpgrade {
		t.Errorf("choice = %+v, want 32 MiB from the upgrade", choice)
	}
}

// TestUpgradeNoMarkerUsesLogSize: a store that crashed at 128 MiB and has no
// marker (an older directory copied without it) is read from its logs. Taking
// it for an earlier build's 32 MiB would ask Badger to replay a 128 MiB log into
// a 32 MiB arena.
func TestUpgradeNoMarkerUsesLogSize(t *testing.T) {
	dataDir := t.TempDir()
	crashNodeStore(t, "earlier-build-write-die", dataDir, resolveHelperSizeEnv+"=128")
	if err := os.Remove(filepath.Join(dataDir, nodeStoreDirName, storage.MemtableSizeFileName)); err != nil {
		t.Fatalf("removing the marker: %v", err)
	}
	withMemoryLimit(t, limit3GBVM)
	bound, _ := openStoreAt(t, memtableTestConfig(dataDir))
	if bound != config.MemtableBatchBytes(128) {
		t.Errorf("a store crashed at 128 MiB with no marker reopened with batch bound %d, want 128 MiB's", bound)
	}
}

func TestUpgradeNeverShrinksOnASmallHost(t *testing.T) {
	dataDir := t.TempDir()
	earlier, err := storage.OpenBadgerKV(filepath.Join(dataDir, nodeStoreDirName), storage.BadgerTuning{MemTableMB: 128}, nil)
	if err != nil {
		t.Fatalf("opening a store as an earlier build did: %v", err)
	}
	if err := earlier.Close(); err != nil {
		t.Fatalf("closing it: %v", err)
	}
	withMemoryLimit(t, limit3GBVM)
	bound, resolution := openStoreAt(t, memtableTestConfig(dataDir))
	if bound != config.MemtableBatchBytes(128) {
		t.Errorf("an earlier build's 128 MiB store reopened with batch bound %d on a 2.9 GiB host, want 128 MiB's", bound)
	}
	warning, warns := resolution.tooLargeWarning()
	if !warns || !strings.Contains(warning, "128 MiB is large for this node's memory limit of 2.9 GiB") {
		t.Errorf("no WARN that 128 MiB is large for a 2.9 GiB host: %q", warning)
	}
}

func TestNoLimitReadFallsBack(t *testing.T) {
	probeErr := errors.New("reading /proc/self/cgroup: permission denied")
	unreadable := func() (memoryLimit, error) { return memoryLimit{}, probeErr }

	linux := memtableTestInputs(filepath.Join(t.TempDir(), nodeStoreDirName), 0)
	linux.memoryLimit = unreadable
	resolution, err := resolveMemtable(linux)
	if err != nil {
		t.Fatalf("resolveMemtable on Linux with a probe error: %v", err)
	}
	if resolution.appliedMB != 32 || resolution.choice.Source != memtableSourceNoLimitRead {
		t.Errorf("Linux, limit unreadable: %d MiB from %q, want the 32 MiB floor from %q", resolution.appliedMB, resolution.choice.Source, memtableSourceNoLimitRead)
	}
	warnings := resolution.warnings()
	if len(warnings) != 1 || !strings.Contains(warnings[0], probeErr.Error()) {
		t.Errorf("Linux, limit unreadable: warnings %q, want one naming %q", warnings, probeErr)
	}

	elsewhere := memtableTestInputs(filepath.Join(t.TempDir(), nodeStoreDirName), 0)
	elsewhere.memoryLimit = unreadable
	elsewhere.onLinux = false
	resolution, err = resolveMemtable(elsewhere)
	if err != nil {
		t.Fatalf("resolveMemtable off Linux: %v", err)
	}
	if resolution.appliedMB != config.AutoMemtableMaxMB || resolution.choice.Source != memtableSourceNoLimitRead {
		t.Errorf("off Linux: %d MiB from %q, want %d from %q", resolution.appliedMB, resolution.choice.Source, config.AutoMemtableMaxMB, memtableSourceNoLimitRead)
	}
	if warnings := resolution.warnings(); len(warnings) != 0 {
		t.Errorf("off Linux, where no limit is ever read, the start warned: %q", warnings)
	}
}

func TestAnUnreadableChoiceIsRefusedWithTheFix(t *testing.T) {
	storeDir := filepath.Join(t.TempDir(), nodeStoreDirName)
	if err := os.MkdirAll(storeDir, 0o700); err != nil {
		t.Fatalf("creating the store directory: %v", err)
	}
	choicePath := filepath.Join(storeDir, memtableChoiceFileName)
	if err := os.WriteFile(choicePath, []byte("{"), 0o600); err != nil {
		t.Fatalf("writing a broken choice: %v", err)
	}
	_, err := resolveMemtable(memtableTestInputs(storeDir, limit4GBVM))
	if err == nil {
		t.Fatal("a choice file that is not JSON was accepted")
	}
	for _, want := range []string{choicePath, "Delete it and start again", "keeps the size it last ran at"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not contain %q", err, want)
		}
	}
}

// The visibility strings, word for word (memtable-host-scaled-design.md §5.1,
// §5.2), under the owner's 64 MiB cap.

func TestStartLineFirstStart(t *testing.T) {
	storeDir := filepath.Join(t.TempDir(), nodeStoreDirName)
	resolution, err := resolveMemtable(memtableTestInputs(storeDir, limit4GBVM))
	if err != nil {
		t.Fatalf("resolveMemtable: %v", err)
	}
	want := "memtable: 64 MiB, chosen now for this new store from this node's memory limit of 3.8 GiB (the host's MemTotal; no cgroup memory limit is set). " +
		"Write bursts can peak near 0.72 GB of memory at this size; the largest atomic batch is 10 MB. " +
		"The choice is saved in " + filepath.Join(storeDir, memtableChoiceFileName) + " and kept on every restart, even if this host's memory changes. " +
		"To use another size, set storage.memtable_mb (32 to 1024) in the config file."
	if got := resolution.startLine(); got != want {
		t.Errorf("first start line:\n got %s\nwant %s", got, want)
	}
}

func TestStartLineLaterStart(t *testing.T) {
	storeDir := filepath.Join(t.TempDir(), nodeStoreDirName)
	if _, err := resolveMemtable(memtableTestInputs(storeDir, limit4GBVM)); err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	resolution, err := resolveMemtable(memtableTestInputs(storeDir, limit3GBVM))
	if err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	want := "memtable: 64 MiB, the size this store chose at its first start on 2026-10-02 (memory limit then 3.8 GiB, now 2.9 GiB). " +
		"Write bursts can peak near 0.72 GB; the largest atomic batch is 10 MB. " +
		"To use another size, set storage.memtable_mb (32 to 1024) in the config file."
	if got := resolution.startLine(); got != want {
		t.Errorf("later start line:\n got %s\nwant %s", got, want)
	}
}

func TestStartLineExplicit(t *testing.T) {
	storeDir := filepath.Join(t.TempDir(), nodeStoreDirName)
	inputs := memtableTestInputs(storeDir, limit4GBVM)
	inputs.explicitMB = 256
	inputs.explicitFromEnv = true
	resolution, err := resolveMemtable(inputs)
	if err != nil {
		t.Fatalf("resolveMemtable: %v", err)
	}
	want := "memtable: 256 MiB, set by HOARDDB_MEMTABLE_MB (this store's own choice, used when it is unset, is 64 MiB). " +
		"Write bursts can peak near 2.30 GB; the largest atomic batch is 40 MB."
	if got := resolution.startLine(); got != want {
		t.Errorf("explicit start line:\n got %s\nwant %s", got, want)
	}
}

func TestStartLineUpgrade(t *testing.T) {
	storeDir := filepath.Join(t.TempDir(), nodeStoreDirName)
	earlier, err := storage.OpenBadgerKV(storeDir, storage.BadgerTuning{MemTableMB: 128}, nil)
	if err != nil {
		t.Fatalf("opening a store as an earlier build did: %v", err)
	}
	if err := earlier.Close(); err != nil {
		t.Fatalf("closing it: %v", err)
	}
	resolution, err := resolveMemtable(memtableTestInputs(storeDir, limit3GBVM))
	if err != nil {
		t.Fatalf("resolveMemtable: %v", err)
	}
	want := "memtable: 128 MiB, the size this store last ran at, kept so it never shrinks, because the store had no memtable-choice.json. " +
		"That size is now this store's choice, even if a setting asked for it at the last run. A new store on this host would choose 32 MiB. " +
		"Write bursts can peak near 1.25 GB; the largest atomic batch is 20 MB. " +
		"The choice is saved in " + filepath.Join(storeDir, memtableChoiceFileName) + " and kept on every restart. " +
		"To use another size, set storage.memtable_mb (32 to 1024) in the config file."
	requireNoOtherBuild(t, resolution.startLine())
	if got := resolution.startLine(); got != want {
		t.Errorf("upgrade start line:\n got %s\nwant %s", got, want)
	}
}

// upgradeAt128 resolves an earlier build's 128 MiB store against limitBytes.
func upgradeAt128(t *testing.T, limitBytes int64, clustered bool) memtableResolution {
	t.Helper()
	storeDir := filepath.Join(t.TempDir(), nodeStoreDirName)
	if err := os.MkdirAll(storeDir, 0o700); err != nil {
		t.Fatalf("creating the store directory: %v", err)
	}
	for name, contents := range map[string]string{"MANIFEST": "", storage.MemtableSizeFileName: "134217728\n"} {
		if err := os.WriteFile(filepath.Join(storeDir, name), []byte(contents), 0o600); err != nil {
			t.Fatalf("writing %s: %v", name, err)
		}
	}
	inputs := memtableTestInputs(storeDir, limitBytes)
	inputs.clustered = clustered
	resolution, err := resolveMemtable(inputs)
	if err != nil {
		t.Fatalf("resolveMemtable: %v", err)
	}
	return resolution
}

func TestTooLargeWarn(t *testing.T) {
	warning, warns := upgradeAt128(t, limit28GiB, false).tooLargeWarning()
	want := "memtable: 128 MiB is large for this node's memory limit of 2.8 GiB: write bursts can peak near 1.25 GB, 42% of the limit, before caches and other work. " +
		"Give this node at least 3.5 GiB of memory, or stop it cleanly (SIGTERM) and set storage.memtable_mb: 32 in the config file (the size a new store on this host would choose)."
	if !warns || warning != want {
		t.Errorf("too-large WARN (fires %v):\n got %s\nwant %s", warns, warning, want)
	}

	warning, warns = upgradeAt128(t, limit28GiB, true).tooLargeWarning()
	want = "memtable: 128 MiB is large for this node's memory limit of 2.8 GiB: write bursts can peak near 1.25 GB, 42% of the limit, before caches and other work. " +
		"Give this node at least 3.5 GiB of memory, or lower it to 32 MiB on every member, one at a time, stopping each cleanly (SIGTERM); a member alone at another size is refused by its peers."
	if !warns || warning != want {
		t.Errorf("too-large WARN on a cluster member (fires %v):\n got %s\nwant %s", warns, warning, want)
	}
}

// TestTooLargeWarnSilentOnOwnChoice: a node never warns about the size it chose
// for itself, from about 1.3 GiB of memory up.
func TestTooLargeWarnSilentOnOwnChoice(t *testing.T) {
	for limitBytes := 13 * gib / 10; limitBytes <= 128*gib; limitBytes += gib / 20 {
		resolution, err := resolveMemtable(memtableTestInputs(filepath.Join(t.TempDir(), nodeStoreDirName), limitBytes))
		if err != nil {
			t.Fatalf("resolveMemtable at %d bytes: %v", limitBytes, err)
		}
		if warning, warns := resolution.tooLargeWarning(); warns {
			t.Errorf("a fresh store at a %.2f GiB limit warns about its own choice: %s", float64(limitBytes)/float64(gib), warning)
		}
	}
}

func TestFloorWarnsBelowAboutOnePointThreeGiB(t *testing.T) {
	resolution, err := resolveMemtable(memtableTestInputs(filepath.Join(t.TempDir(), nodeStoreDirName), gib))
	if err != nil {
		t.Fatalf("resolveMemtable: %v", err)
	}
	warning, warns := resolution.tooLargeWarning()
	want := "memtable: 32 MiB is large for this node's memory limit of 1.0 GiB: write bursts can peak near 0.46 GB, 43% of the limit, before caches and other work. " +
		"32 MiB is the smallest size, so give this node at least 1.3 GiB of memory."
	if !warns || warning != want {
		t.Errorf("floor WARN (fires %v):\n got %s\nwant %s", warns, warning, want)
	}
}

// TestShrinkRefusalNamesTheChoiceAsSource: the size a start asks for can now
// come from the store's choice, so the shrink refusal says which, and its fix
// is "set it back, start once, stop cleanly, then remove it" — the note's §4
// example: run at 256 by environment, crash, drop the environment.
func TestShrinkRefusalNamesTheChoiceAsSource(t *testing.T) {
	t.Setenv(memtableEnvVar, "")
	dataDir := t.TempDir()
	crashNodeStore(t, "start-write-die", dataDir,
		fmt.Sprintf("%s=%d", resolveHelperLimitEnv, testMemoryLimitBytes), resolveHelperSizeEnv+"=256")

	_, _, err := openNodeStore(memtableTestConfig(dataDir), nil)
	var shrink *storage.MemtableShrinkError
	if !errors.As(err, &shrink) {
		t.Fatalf("restarting at the chosen 64 MiB after a crash at an explicit 256 = %v, want a *MemtableShrinkError", err)
	}
	for _, want := range []string{
		"this node now runs at its chosen 64 MiB because storage.memtable_mb is not set and HOARDDB_MEMTABLE_MB is not set",
		"last running with 256 MiB",
		"set HOARDDB_MEMTABLE_MB=256 (or storage.memtable_mb: 256 in the config file)",
		"then stop it cleanly (SIGTERM or Ctrl-C, not kill -9) and remove the setting: the node goes back to its chosen 64 MiB",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "configured with") {
		t.Errorf("refusal %q says the node is configured with a size nothing configured", err)
	}
}

// TestShrinkRefusalNamesAnExplicitSource: an explicit size the store crashed
// above is named by the setting that asked for it.
func TestShrinkRefusalNamesAnExplicitSource(t *testing.T) {
	dataDir := t.TempDir()
	crashNodeStore(t, "start-write-die", dataDir,
		fmt.Sprintf("%s=%d", resolveHelperLimitEnv, testMemoryLimitBytes), resolveHelperSizeEnv+"=256")
	t.Setenv(memtableEnvVar, "128")
	cfg := memtableTestConfig(dataDir)
	cfg.Storage.MemtableMB = 128
	_, _, err := openNodeStore(cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "this node is set to 128 MiB by HOARDDB_MEMTABLE_MB") {
		t.Errorf("refusal %v does not name HOARDDB_MEMTABLE_MB as what asked for 128 MiB", err)
	}
}

// TestStartLineAfterTheChoiceIsKept: on later starts a kept size says when and
// why it was kept.
func TestStartLineAfterTheChoiceIsKept(t *testing.T) {
	resolution := upgradeAt128(t, limit3GBVM, false)
	later, err := resolveMemtable(memtableTestInputs(filepath.Dir(resolution.choicePath), limit3GBVM))
	if err != nil {
		t.Fatalf("resolveMemtable: %v", err)
	}
	want := "memtable: 128 MiB, the size this store kept on 2026-10-02 from its last run, when it had no memtable-choice.json, so that it never shrinks. A new store on this host would choose 32 MiB. " +
		"Write bursts can peak near 1.25 GB; the largest atomic batch is 20 MB. " +
		"To use another size, set storage.memtable_mb (32 to 1024) in the config file."
	if got := later.startLine(); got != want {
		t.Errorf("kept-size later start line:\n got %s\nwant %s", got, want)
	}
}

// TestStoreChosenAt64StaysAt64OnALargerHost: the automatic cap rose from 64 to
// 128 MiB (release burn-down §4d item 23), and a store keeps the size it chose.
// The choice file is read, never re-derived, so a store created at 64 MiB
// re-opens at 64 on a 16 GiB host that would now choose 128 for a new store.
// This guards behaviour that must not change, so it passes before and after
// the cap change.
func TestStoreChosenAt64StaysAt64OnALargerHost(t *testing.T) {
	const limit16GiB = int64(16) << 30
	storeDir := filepath.Join(t.TempDir(), nodeStoreDirName)
	if _, err := resolveMemtable(memtableTestInputs(storeDir, limit4GBVM)); err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	if choice := readChoiceFile(t, storeDir); choice.MemtableMB != 64 {
		t.Fatalf("setup: the 3.8 GiB host chose %d MiB, want 64", choice.MemtableMB)
	}
	resolution, err := resolveMemtable(memtableTestInputs(storeDir, limit16GiB))
	if err != nil {
		t.Fatalf("re-open on a 16 GiB host: %v", err)
	}
	if resolution.appliedMB != 64 || resolution.chosenNow {
		t.Errorf("re-open on a 16 GiB host: applied %d MiB, chosen now %v; want 64 from the choice file", resolution.appliedMB, resolution.chosenNow)
	}
	if choice := readChoiceFile(t, storeDir); choice.MemtableMB != 64 {
		t.Errorf("choice file rewritten to %d MiB on re-open, want 64 untouched", choice.MemtableMB)
	}
	freshStore := filepath.Join(t.TempDir(), nodeStoreDirName)
	fresh, err := resolveMemtable(memtableTestInputs(freshStore, limit16GiB))
	if err != nil {
		t.Fatalf("fresh store on a 16 GiB host: %v", err)
	}
	if fresh.appliedMB != 128 {
		t.Errorf("a fresh store on a 16 GiB host chose %d MiB, want 128", fresh.appliedMB)
	}
}
