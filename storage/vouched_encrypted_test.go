package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
)

// vouchedTestKey is the store key the encrypted vouched tests seal under.
var vouchedTestKey = bytes.Repeat([]byte{0x4E}, 32)

func vouchedTestKeySource() ([]byte, error) { return vouchedTestKey, nil }

// vouchedTestStoreKey, when not nil, is the key function every vouched test store opens with
// (openVouchedTestStore): withVouchedTestKey sets it for one test, so the encrypted variants and
// the regression runs with a key reuse the plaintext tests' builders instead of copies of them
// (ring encryption at rest spec §9.1 step 5). None of the tests that open through it calls
// t.Parallel, so no other test runs while it is set.
var vouchedTestStoreKey func() ([]byte, error)

// sealedVouchedTestStores counts the stores openVouchedTestStore found sealed on disk after an
// open with the key, and unsealedVouchedTestStores names the ones it found in plaintext. They are
// read from the store's files, not from the call: a key function that returns no key opens a
// plaintext store without an error, and a count of calls would pass it.
var (
	sealedVouchedTestStores   int
	unsealedVouchedTestStores []string
)

// openVouchedTestStore opens a vouched store as the node does: in plaintext, or under the test key
// while withVouchedTestKey holds. Under the key it then judges the directory, opened or refused:
// its key registry must refuse an empty key. A directory without a key registry (a refusal before
// Badger created a file) cannot be judged and is passed over.
func openVouchedTestStore(dir string, tuning BadgerTuning) (*BadgerKV, error) {
	kv, err := OpenVouchedBadgerKVChecked(dir, tuning, vouchedTestStoreKey, nil)
	if vouchedTestStoreKey == nil {
		return kv, err
	}
	if _, statErr := os.Stat(filepath.Join(dir, badger.KeyRegistryFileName)); statErr == nil {
		if _, registryErr := badger.OpenKeyRegistry(badger.KeyRegistryOptions{Dir: dir, ReadOnly: true}); errors.Is(registryErr, badger.ErrEncryptionKeyMismatch) {
			sealedVouchedTestStores++
		} else {
			unsealedVouchedTestStores = append(unsealedVouchedTestStores, dir)
		}
	}
	return kv, err
}

// vouchedTestKeyOrNil is the key a test opens a store with outside openVouchedTestStore: nil, or
// the test key while withVouchedTestKey holds.
func vouchedTestKeyOrNil(t *testing.T) []byte {
	t.Helper()
	if vouchedTestStoreKey == nil {
		return nil
	}
	key, err := vouchedTestStoreKey()
	noErr(t, err, "reading the test key")
	return key
}

// withVouchedTestKey makes every openVouchedTestStore of this test seal its store under
// vouchedTestKey, until the test ends, and fails the test when it ends with a store that
// openVouchedTestStore found in plaintext, or without one it found sealed.
func withVouchedTestKey(t *testing.T) {
	t.Helper()
	if vouchedTestStoreKey != nil {
		t.Fatal("withVouchedTestKey: the test key is already set; a keyed test ran inside another")
	}
	vouchedTestStoreKey = vouchedTestKeySource
	sealedBefore := sealedVouchedTestStores
	unsealedVouchedTestStores = nil
	t.Cleanup(func() {
		vouchedTestStoreKey = nil
		if len(unsealedVouchedTestStores) != 0 {
			t.Errorf("%s opened stores with the key whose key registry opens with an empty key, so they were not sealed: %v", t.Name(), unsealedVouchedTestStores)
		}
		if sealedVouchedTestStores == sealedBefore {
			t.Errorf("%s left no sealed store: it no longer opens through openVouchedTestStore, or the helper no longer seals", t.Name())
		}
	})
}

// isFirstFlushRefusal reports whether err is the guard's refusal of a table the MANIFEST does not
// name in a store whose MANIFEST names none: the state of a crash during the very first flush,
// which a plaintext store repairs when confirmFirstFlushCrash can read the table and its log, and
// a sealed store refuses (spec §7).
func isFirstFlushRefusal(err error) bool {
	var damaged *StoreDamagedError
	return errors.As(err, &damaged) && strings.Contains(damaged.Damage, badger.ManifestFilename+" names no table")
}

// requireSealedStore fails unless the store at dir has a key registry that refuses an empty key.
func requireSealedStore(t *testing.T, dir string) {
	t.Helper()
	if _, err := badger.OpenKeyRegistry(badger.KeyRegistryOptions{Dir: dir, ReadOnly: true}); !errors.Is(err, badger.ErrEncryptionKeyMismatch) {
		t.Fatalf("the key registry of %s opens with an empty key (%v): the store was not sealed under the test key", dir, err)
	}
}

// TestOpenVouchedBadgerKVWithAKeySealsTheStore: [GUARD the vouched open passes the key it is
// given to Badger, so the ring store is sealed; red when OpenVouchedBadgerKVChecked passes nil to
// badgerOptions: the key registry then opens with an empty key]. A reopen with the key reads the
// vouched commit back, and the marker stays plaintext beside the sealed files (ring encryption at
// rest spec §3.2, §9.1 step 1).
func TestOpenVouchedBadgerKVWithAKeySealsTheStore(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "badger")
	kv, err := OpenVouchedBadgerKVChecked(dir, vouchedTestTuning, vouchedTestKeySource, nil)
	noErr(t, err, "opening the store with a key")
	batch := kv.NewBatch()
	noErr(t, batch.Set([]byte("sealed"), []byte("value")), "setting a key")
	noErr(t, batch.Commit(true), "committing, synced")
	noErr(t, kv.Close(), "closing the store")

	if _, err := badger.OpenKeyRegistry(badger.KeyRegistryOptions{Dir: dir, ReadOnly: true}); !errors.Is(err, badger.ErrEncryptionKeyMismatch) {
		t.Fatalf("the store's key registry opens with an empty key (%v): the vouched open did not seal the store", err)
	}
	if _, err := os.Stat(filepath.Join(dir, vouchedFileName)); err != nil {
		t.Fatalf("no sync marker beside the sealed store: %v", err)
	}

	reopened, err := OpenVouchedBadgerKVChecked(dir, vouchedTestTuning, vouchedTestKeySource, nil)
	noErr(t, err, "reopening the sealed store with its key")
	defer func() { _ = reopened.Close() }() // a test's close; the read below is the result
	snapshot := reopened.NewSnapshot()
	defer snapshot.Discard()
	got, err := snapshot.Get([]byte("sealed"))
	noErr(t, err, "reading the vouched commit back")
	equal(t, "value", string(got), "the vouched commit's value")
}

// TestAVouchedStoreCutDuringItsFirstFlushIsRefusedWhenEncrypted: [GUARD the sealed form of the
// image TestAVouchedStoreCutDuringItsFirstFlushReopens repairs in plaintext (built by the same
// buildFirstFlushCutImage, with the key) is refused, not repaired: confirmFirstFlushCrash cannot
// read a sealed table or log, so the guard refuses the table the MANIFEST does not name; red when
// the key is not passed through the vouched open: the plaintext store is then repaired and
// reopens]. The refusal is the first-flush refusal, a vouched-open refusal the node adds its
// advice to; no .first-flush-crash file is left, and the table stays in place (ring encryption at
// rest spec §7 and §9.1 step 5, ruling 4: accepted). It replaces merge 1's
// TestAnEncryptedRingStoreKilledInItsFirstFlushIsRefused, which built the same image by a copy of
// the plaintext test's steps.
func TestAVouchedStoreCutDuringItsFirstFlushIsRefusedWhenEncrypted(t *testing.T) {
	withVouchedTestKey(t)
	image, leftover, tableBytes := buildFirstFlushCutImage(t)
	requireSealedStore(t, image)

	reopened, err := openVouchedTestStore(image, firstFlushCutTuning)
	if err == nil {
		_ = reopened.Close()
		t.Fatal("a sealed store cut during its first flush reopened: the guard repaired a table it cannot read")
	}
	if !IsVouchedOpenRefusal(err) || !isFirstFlushRefusal(err) {
		t.Fatalf("the refusal is not the first-flush refusal the node adds its advice to: %v", err)
	}
	if quarantined, _ := filepath.Glob(filepath.Join(image, "*"+firstFlushQuarantineSuffix+"*")); len(quarantined) != 0 {
		t.Fatalf("the refused open set files aside: %v", quarantined)
	}
	if after, readErr := os.ReadFile(leftover); readErr != nil || !bytes.Equal(after, tableBytes) {
		t.Fatalf("the refused open moved or changed the leftover table (read error %v)", readErr)
	}
}

// TestASealedTableBesideAPlaintextFirstLogIsNotConfirmedAFirstFlush: [GUARD the first-flush check
// never reads a sealed store's table as plaintext; red when tablesTheManifestNames passes
// encrypted=false for a keyed store: confirmFirstFlushCrash then finds a log beginning at version
// 1 and reads the sealed table's index (garbage to the flatbuffer reader), and the table is either
// set aside as a confirmed crash or the reader panics]. A sealed store's own logs fail the
// version-1 test by chance (their bytes are ciphertext), so the fixture puts a plaintext first log
// whose entries begin at version 1 beside the sealed table: only the encrypted flag keeps the
// sealed index from the parser (integration review F-3).
func TestASealedTableBesideAPlaintextFirstLogIsNotConfirmedAFirstFlush(t *testing.T) {
	tuning := BadgerTuning{MemTableMB: 32}
	running := filepath.Join(t.TempDir(), "running")
	kv, err := OpenVouchedBadgerKVChecked(running, tuning, vouchedTestKeySource, nil)
	noErr(t, err, "opening the sealed store")
	vouched := kv.NewBatch()
	noErr(t, vouched.Set([]byte("vouched"), []byte("kept")), "setting the vouched key")
	noErr(t, vouched.Commit(true), "committing the vouched key, synced")
	image := filepath.Join(t.TempDir(), "image")
	copyStoreFiles(t, running, image)
	noErr(t, kv.Close(), "closing the sealed store")
	tables := tableFiles(t, running)
	if len(tables) != 1 || filepath.Base(tables[0]) != "000001.sst" {
		t.Fatalf("SCENARIO NOT REACHED: the close left tables %v, want the first flush's 000001.sst", tables)
	}
	tableBytes, err := os.ReadFile(tables[0])
	noErr(t, err, "reading the sealed table")

	// A plaintext store's first log: one commit at version 1, read before any flush.
	plain := filepath.Join(t.TempDir(), "plain")
	plainKV, err := OpenVouchedBadgerKV(plain, tuning)
	noErr(t, err, "opening the plaintext store")
	plainBatch := plainKV.NewBatch()
	noErr(t, plainBatch.Set([]byte("vouched"), []byte("kept")), "setting the plaintext key")
	noErr(t, plainBatch.Commit(true), "committing the plaintext key, synced")
	plainLogs, err := filepath.Glob(filepath.Join(plain, "*.mem"))
	noErr(t, err, "listing the plaintext logs")
	if len(plainLogs) == 0 {
		t.Fatal("SCENARIO NOT REACHED: the plaintext store has no memtable log")
	}
	sealedLogs, err := filepath.Glob(filepath.Join(image, "*.mem"))
	noErr(t, err, "listing the sealed logs")
	for _, sealedLog := range sealedLogs {
		noErr(t, os.Remove(sealedLog), "removing a sealed log from the image")
	}
	for _, plainLog := range plainLogs {
		logBytes, err := os.ReadFile(plainLog)
		noErr(t, err, "reading a plaintext log")
		noErr(t, os.WriteFile(filepath.Join(image, filepath.Base(plainLog)), logBytes, 0o600), "writing the plaintext log")
	}
	noErr(t, plainKV.Close(), "closing the plaintext store")
	if logs, found := memtableLogVersions(image); !found || logs.lowest != firstCommitVersion {
		t.Fatalf("SCENARIO NOT REACHED: the image's logs do not begin at version 1 (%+v, found %v)", logs, found)
	}
	leftover := filepath.Join(image, "000001.sst")
	noErr(t, os.WriteFile(leftover, tableBytes, 0o600), "writing the sealed table")

	reopened, err := OpenVouchedBadgerKVChecked(image, tuning, vouchedTestKeySource, nil)
	if err == nil {
		_ = reopened.Close()
		t.Fatal("the sealed store opened over a table the MANIFEST does not name")
	}
	if !IsVouchedOpenRefusal(err) {
		t.Fatalf("the refusal is not the guard's: %v", err)
	}
	if quarantined, _ := filepath.Glob(filepath.Join(image, "*"+firstFlushQuarantineSuffix+"*")); len(quarantined) != 0 {
		t.Fatalf("the sealed table was confirmed a first-flush crash and set aside: %v", quarantined)
	}
	if after, readErr := os.ReadFile(leftover); readErr != nil || !bytes.Equal(after, tableBytes) {
		t.Fatalf("the refused open moved or changed the sealed table (read error %v)", readErr)
	}
}

// TestTheCutDiscardsAndTruncatesASealedStore: [GUARD the vouched cut works on a sealed store: it
// reads each commit's version from the decrypted memtable log, discards the commit above the marker
// and truncates the log there, and the vouched commit is read back; red when the open does not pass
// the key to Badger (the sealed log is then unreadable)]. Integration review F-5 (i); the plaintext
// cut's I-1..I-7 are unchanged.
func TestTheCutDiscardsAndTruncatesASealedStore(t *testing.T) {
	tuning := BadgerTuning{MemTableMB: 32}
	running := filepath.Join(t.TempDir(), "running")
	kv, err := OpenVouchedBadgerKVChecked(running, tuning, vouchedTestKeySource, nil)
	noErr(t, err, "opening the sealed store")
	vouched := kv.NewBatch()
	noErr(t, vouched.Set([]byte("vouched"), []byte("kept")), "setting the vouched key")
	noErr(t, vouched.Commit(true), "committing the vouched key, synced")
	unsynced := kv.NewBatch()
	noErr(t, unsynced.Set([]byte("unsynced"), []byte("above the marker")), "setting the unsynced key")
	noErr(t, unsynced.Commit(false), "committing the unsynced key")

	// The image is taken while the store runs: the marker vouches the first commit and the memtable
	// log holds both.
	image := filepath.Join(t.TempDir(), "image")
	copyStoreFiles(t, running, image)
	noErr(t, kv.Close(), "closing the running store")
	if _, err := badger.OpenKeyRegistry(badger.KeyRegistryOptions{Dir: image, ReadOnly: true}); !errors.Is(err, badger.ErrEncryptionKeyMismatch) {
		t.Fatalf("the image's key registry opens with an empty key (%v): the open did not seal the store under its key", err)
	}

	reopened, err := OpenVouchedBadgerKVChecked(image, tuning, vouchedTestKeySource, nil)
	noErr(t, err, "reopening the sealed image with its key")
	defer func() { _ = reopened.Close() }() // a test's close; the assertions below are the result
	report := reopened.VouchedOpenReport()
	want := VouchedOpenReport{Vouched: 1, Kept: 1, DiscardedCommits: 1, LogsTruncated: 1, LogsRemoved: 0}
	if report != want {
		t.Fatalf("the cut on the sealed image reported %+v, want %+v", report, want)
	}
	snapshot := reopened.NewSnapshot()
	defer snapshot.Discard()
	kept, err := snapshot.Get([]byte("vouched"))
	noErr(t, err, "the vouched commit is gone after the cut")
	equal(t, "kept", string(kept), "the vouched commit's value")
	if _, err := snapshot.Get([]byte("unsynced")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("the commit above the marker survived the cut of the sealed store (read error: %v)", err)
	}
}

// TestTheVouchedTestsPassWithAKey: [REGRESSION RUN, no guard: these tests pass with or without a
// key, so none can turn red because of encryption alone] the cut's vouched tests that the second
// review saw pass with a key (spec §9.1 step 5 said "the other twelve vouched tests with a key";
// the list below now holds twenty-seven), C1's TestADeleteInAFlushedTableStaysDeletedAfterAKill
// among them, and the Badger sync patches'
// storage tests), each run as written with every store it opens through openVouchedTestStore
// sealed under the test key. The three that a key changes are the encrypted variants above and
// beside their plaintext tests (the first-flush cut, the crash-point enumeration and T10).
// openVouchedTestStore reads each store's key registry after its open, and withVouchedTestKey
// fails a test that opened a plaintext store with the key or left no sealed one, so a test cannot
// pass here on plaintext stores.
func TestTheVouchedTestsPassWithAKey(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"TestADeleteInAFlushedTableStaysDeletedAfterAKill", TestADeleteInAFlushedTableStaysDeletedAfterAKill},
		{"TestADeleteInAFlushedTableStaysDeletedWhenItsStaleLogIsReplayed", TestADeleteInAFlushedTableStaysDeletedWhenItsStaleLogIsReplayed},
		{"TestADeleteStaysDeletedWhenACompactionLowersTheTablesHighestVersion", TestADeleteStaysDeletedWhenACompactionLowersTheTablesHighestVersion},
		{"TestACutThatReachesThreeLogsKeepsOnlyTheVouchedCommit", TestACutThatReachesThreeLogsKeepsOnlyTheVouchedCommit},
		{"TestAFirstFlushWithNothingVouchedOpensAfterACrash", TestAFirstFlushWithNothingVouchedOpensAfterACrash},
		{"TestAVouchedFirstFlushLeftoverBelowTheLogStaysRefused", TestAVouchedFirstFlushLeftoverBelowTheLogStaysRefused},
		{"TestAVouchedStoreCutAfterEarlierFlushesStaysRefused", TestAVouchedStoreCutAfterEarlierFlushesStaysRefused},
		{"TestTwoConcurrentSyncsThatSampledTheirFloorsOutOfOrderLeaveAMarkerTheNextOpenReads", TestTwoConcurrentSyncsThatSampledTheirFloorsOutOfOrderLeaveAMarkerTheNextOpenReads},
		{"TestTheMarkerMovesOnlyOnAReportedSync", TestTheMarkerMovesOnlyOnAReportedSync},
		{"TestTheRingStoreRunsWithSyncWritesOff", TestTheRingStoreRunsWithSyncWritesOff},
		{"TestVouchedMarkerFile", TestVouchedMarkerFile},
		{"TestEveryCallOfAnAdvanceCrashedOrFailedLeavesTheOldMarkerOrTheNew", TestEveryCallOfAnAdvanceCrashedOrFailedLeavesTheOldMarkerOrTheNew},
		{"TestAFallbackToVersionZeroOpens", TestAFallbackToVersionZeroOpens},
		{"TestTheNextAdvanceWritesTheSlotTheOpenDidNotChoose", TestTheNextAdvanceWritesTheSlotTheOpenDidNotChoose},
		{"TestEveryTearOfTheSlotBeingWrittenOpensWithTheOldMarker", TestEveryTearOfTheSlotBeingWrittenOpensWithTheOldMarker},
		// The marker's own tests (the two-slot advance, its tears, its poison and its refusals): the
		// marker stays plaintext beside a sealed store, so each passed with a key the first time it
		// ran with one.
		// Not here: TestAVouchedStoreWithADamagedLeftoverThatIsNotItsFirstTableStaysRefused. With a key the
		// open refuses every incomplete table before it looks at the table's id, so the run could not
		// fail with the id guard gone; the plain run is the guard's witness.
		{"TestALogCreatedAfterTheVersionIsReadIsCoveredByADirectoryFsync", TestALogCreatedAfterTheVersionIsReadIsCoveredByADirectoryFsync},
		{"TestTheFirstAdvanceAfterAnOpenSyncsTheDirectory", TestTheFirstAdvanceAfterAnOpenSyncsTheDirectory},
		{"TestALogCreatedDuringTheDirectoryFsyncIsFsyncedAtTheNextAdvance", TestALogCreatedDuringTheDirectoryFsyncIsFsyncedAtTheNextAdvance},
		{"TestAnAdvanceInAStoreDirectoryMovedAsideIsRefused", TestAnAdvanceInAStoreDirectoryMovedAsideIsRefused},
		{"TestASyncQueuedBehindAFailedMarkerAdvanceReturnsThePoison", TestASyncQueuedBehindAFailedMarkerAdvanceReturnsThePoison},
		{"TestASyncQueuedOnTheMarkerWhileAnotherBadgerSyncFailedReturnsThePoison", TestASyncQueuedOnTheMarkerWhileAnotherBadgerSyncFailedReturnsThePoison},
		{"TestTheFloorSampleIsReadInOrder", TestTheFloorSampleIsReadInOrder},
		{"TestTheMarkerAdvancesItsFloorAlone", TestTheMarkerAdvancesItsFloorAlone},
		{"TestAMarkerAdvanceRenamesNothingAndSyncsNoDirectory", TestAMarkerAdvanceRenamesNothingAndSyncsNoDirectory},
		{"TestAMarkerWithBothSlotsTornIsRefusedNamingTheFile", TestAMarkerWithBothSlotsTornIsRefusedNamingTheFile},
		{"TestTheMarkerFileIsCreatedWithItsDirectoryEntrySynced", TestTheMarkerFileIsCreatedWithItsDirectoryEntrySynced},
		{"TestAnOpenOnOneWholeSlotWarnsNamingBothSlots", TestAnOpenOnOneWholeSlotWarnsNamingBothSlots},
	} {
		t.Run(test.name, func(t *testing.T) {
			withVouchedTestKey(t)
			test.run(t)
		})
	}
}
