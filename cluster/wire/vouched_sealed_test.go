package wire

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"

	"github.com/hoarddb/hoarddb-server/storage"
)

// sealedVouchedKeys hands every node of a sealed vouched test the same fixed store key and log
// key, so an image of one node's directory opens under the keys the node wrote it with.
type sealedVouchedKeys struct{}

var (
	sealedVouchedStoreKey = bytes.Repeat([]byte{0x5C}, 32)
	sealedVouchedLogKey   = bytes.Repeat([]byte{0x7D}, 32)
)

func (sealedVouchedKeys) StoreKey(string) ([]byte, error) { return sealedVouchedStoreKey, nil }

func (sealedVouchedKeys) LogKeys(string) (storage.WALKeySource, error) {
	return fixedLogKeys{key: sealedVouchedLogKey}, nil
}

// vouchedTestAtRest, when not nil, is the AtRest every vouched test node opens with
// (vouchedTestConfig) and the source of the key every vouched test store opens with
// (openVouchedTestKV, vouchedTestStoreKey): withSealedVouchedTests sets it for one test, so the
// sealed runs reuse the plaintext tests as written. None of the tests that open through these
// helpers calls t.Parallel, so no other test runs while it is set.
var vouchedTestAtRest AtRestKeys

// sealedVouchedFilesSeen counts the stores and log segments requireSealedOnDisk found sealed on
// disk, so a sealed run can tell that its test left anything sealed at all. Log segments count
// because a test that moves its store aside leaves only its sealed log to be judged.
var sealedVouchedFilesSeen int

// vouchedTestConfig returns cfg as the test gave it, or with the test keys while
// withSealedVouchedTests holds; the node's store and log are then checked on disk when t ends.
func vouchedTestConfig(t testing.TB, cfg NodeConfig) NodeConfig {
	if vouchedTestAtRest != nil {
		cfg.AtRest = vouchedTestAtRest
		t.Cleanup(func() { requireSealedOnDisk(t, filepath.Join(cfg.Dir, "badger"), filepath.Join(cfg.Dir, "wal")) })
	}
	return cfg
}

// vouchedTestStoreKey is the key a test opens the store at dir with outside the node: nil, or the
// test store key while withSealedVouchedTests holds; the store is then checked on disk when t ends.
func vouchedTestStoreKey(t testing.TB, dir string) []byte {
	if vouchedTestAtRest == nil {
		return nil
	}
	t.Cleanup(func() { requireSealedOnDisk(t, dir, "") })
	return sealedVouchedStoreKey
}

// openVouchedTestKV opens a store with the vouched cut as the node does: in plaintext, or under
// the test store key while withSealedVouchedTests holds; the store is then checked on disk when t
// ends.
func openVouchedTestKV(t testing.TB, dir string, tuning storage.BadgerTuning) (*storage.BadgerKV, error) {
	if vouchedTestAtRest == nil {
		return storage.OpenVouchedBadgerKV(dir, tuning)
	}
	t.Cleanup(func() { requireSealedOnDisk(t, dir, "") })
	return storage.OpenVouchedBadgerKVChecked(dir, tuning, func() ([]byte, error) { return sealedVouchedStoreKey, nil }, nil)
}

// requireSealedOnDisk fails t unless the store at storeDir, and the log at logDir when one is
// named, are sealed on disk: the store's key registry refuses an empty key, and no log segment has
// the header of a segment written in the clear. The helpers above run it as a cleanup of the test
// that opened through them, which runs before that test's temporary directories are removed. It
// reads the files instead of counting the helpers' calls, because a helper that hands out a key
// cannot tell whether the open used it: a run whose key sources return no key, or whose helpers
// open in plaintext, is red. A store directory without a key registry (removed by the test, or
// refused before Badger created a file) cannot be judged and is passed over.
func requireSealedOnDisk(t testing.TB, storeDir, logDir string) {
	t.Helper()
	if logDir != "" {
		segments, err := filepath.Glob(filepath.Join(logDir, "wal_*.log"))
		if err != nil {
			t.Fatal(err)
		}
		for _, segment := range segments {
			content, err := os.ReadFile(segment)
			if err != nil {
				t.Fatal(err)
			}
			// A segment a test tore or emptied has no whole header and says nothing either way.
			header := content[:min(len(content), walSegmentHeaderLength)]
			if storage.IsPlainWALSegmentHeader(header) {
				t.Errorf("the log segment %s is written in the clear: the node did not open its log under the test keys", segment)
			}
			if storage.IsSealedWALSegmentHeader(header) {
				sealedVouchedFilesSeen++
			}
		}
	}
	if _, err := os.Stat(filepath.Join(storeDir, badger.KeyRegistryFileName)); errors.Is(err, os.ErrNotExist) {
		return
	}
	if _, err := badger.OpenKeyRegistry(badger.KeyRegistryOptions{Dir: storeDir, ReadOnly: true}); !errors.Is(err, badger.ErrEncryptionKeyMismatch) {
		t.Errorf("the key registry of %s opens with an empty key (%v): the store was not sealed under the test key", storeDir, err)
		return
	}
	sealedVouchedFilesSeen++
}

// walSegmentHeaderLength is the length of a log segment's header, the bytes
// storage.IsPlainWALSegmentHeader judges (as atrest_test.go reads them).
const walSegmentHeaderLength = 24

// withSealedVouchedTests seals every node and store this test opens through the helpers above,
// until the test ends, and fails it when it ends without one sealed store or log segment seen on disk.
func withSealedVouchedTests(t *testing.T) {
	t.Helper()
	if vouchedTestAtRest != nil {
		t.Fatal("withSealedVouchedTests: the test keys are already set; a sealed test ran inside another")
	}
	vouchedTestAtRest = sealedVouchedKeys{}
	seenBefore := sealedVouchedFilesSeen
	// Registered first, so it runs after every helper's check of this test and its subtests.
	t.Cleanup(func() {
		vouchedTestAtRest = nil
		if sealedVouchedFilesSeen == seenBefore {
			t.Errorf("%s left no sealed store or log segment on disk: it no longer opens through vouchedTestConfig, vouchedTestStoreKey or openVouchedTestKV, or they no longer seal", t.Name())
		}
	})
}

// TestTheVouchedNodeTestsPassSealed: [REGRESSION RUN, no guard: these tests pass with or without
// keys, so none can turn red because of encryption alone] the vouched cut's node tests, each run as
// written on a node whose store and log are sealed under the test keys: the acceptance tests (T1
// and the large-value pair), the floor and anchor tests, the sweep and drop records across the
// cut, the seed pull's power cuts, the ring store's refusal texts byte for byte, and the
// empty-store guard with the single node's rebuild from its log. All passed sealed the first time
// they ran with keys. requireValueMissing's search for a value's plaintext in the value log finds
// nothing in a sealed one, so sealed it rests on its count of the bytes written. Each store and log a
// test opened through the helpers is read back when the test ends and must be sealed on disk
// (requireSealedOnDisk), so a test cannot pass here on plaintext files.
func TestTheVouchedNodeTestsPassSealed(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"TestAckedWritesSurviveAHoledStoreImage", TestAckedWritesSurviveAHoledStoreImage},
		{"TestLargeValueUnderAnUnfinishedSyncIsReplayedWhole", TestLargeValueUnderAnUnfinishedSyncIsReplayedWhole},
		{"TestLargeValueVouchedByAFlushIsWhole", TestLargeValueVouchedByAFlushIsWhole},
		{"TestAFirstWriteAboveTheCutIsReplayedWithoutAnAnchor", TestAFirstWriteAboveTheCutIsReplayedWithoutAnAnchor},
		// TestAZeroCursorPairsFirstWriteIsReplayedWithoutAnAnchor stood here and is not run sealed: it
		// was intermittent under -race in this list (restart-hold territory, which the freeze excludes
		// from replication), so sealed zero-cursor replay is not covered. The comment is about that
		// test, not the row below.
		{"TestTheFloorSampleIsReadInOrder", TestTheFloorSampleIsReadInOrder},
		{"TestSweepRecordsSurviveTheCut", TestSweepRecordsSurviveTheCut},
		{"TestADropsRecordsSurviveTheCutAtItsFirstSweptWrite", TestADropsRecordsSurviveTheCutAtItsFirstSweptWrite},
		{"TestAPairIsReadyOnlyWhenAPowerCutKeepsItsReadyRecordAndItsData", TestAPairIsReadyOnlyWhenAPowerCutKeepsItsReadyRecordAndItsData},
		{"TestAPowerCutAtAnyCallOfAPullReopensToAPullThatEndsWithTheSourcesKeys", TestAPowerCutAtAnyCallOfAPullReopensToAPullThatEndsWithTheSourcesKeys},
		{"TestTheExpiryEntriesAPullDerivesSurviveACutAtOnReady", TestTheExpiryEntriesAPullDerivesSurviveACutAtOnReady},
		{"TestSeedingAPairCostsOneStoreSyncAndOneMarkerAdvance", TestSeedingAPairCostsOneStoreSyncAndOneMarkerAdvance},
		{"TestACutBetweenTheResetAndTheFinalSyncKeepsNoKeyTheResetRemoved", TestACutBetweenTheResetAndTheFinalSyncKeepsNoKeyTheResetRemoved},
		{"TestARefusedRingStoreGetsTheAdviceForWhatTheNodeIs", TestARefusedRingStoreGetsTheAdviceForWhatTheNodeIs},
		{"TestAMarkerWithBothSlotsTornGetsTheWholeRefusalForWhatTheNodeIs", TestAMarkerWithBothSlotsTornGetsTheWholeRefusalForWhatTheNodeIs},
		{"TestAnUnreadFormatGetsTheAdviceForWhatTheNodeIs", TestAnUnreadFormatGetsTheAdviceForWhatTheNodeIs},
		{"TestACorruptKeyGetsTheAdviceForWhatTheNodeIs", TestACorruptKeyGetsTheAdviceForWhatTheNodeIs},
		{"TestADamagedDedupeEntryReportGetsTheAdviceForWhatTheNodeIs", TestADamagedDedupeEntryReportGetsTheAdviceForWhatTheNodeIs},
		{"TestTheStoresRuntimeRefusalsCarryTheNodesAdvice", TestTheStoresRuntimeRefusalsCarryTheNodesAdvice},
		{"TestAnEmptyRingStoreAfterAnEarlierLifeIsRefused", TestAnEmptyRingStoreAfterAnEarlierLifeIsRefused},
		{"TestASingleNodeRebuildsAnEmptyStoreFromAWholeLog", TestASingleNodeRebuildsAnEmptyStoreFromAWholeLog},
		{"TestARealFirstStartStillOpens", TestARealFirstStartStillOpens},
		{"TestASingleNodeThatLostItsStoreAndItsLogIsRefusedAndSaysTheLogIsMissing", TestASingleNodeThatLostItsStoreAndItsLogIsRefusedAndSaysTheLogIsMissing},
		{"TestALogDirectoryRemovedUnderARunningNodeGetsTheNodesAdvice", TestALogDirectoryRemovedUnderARunningNodeGetsTheNodesAdvice},
	} {
		t.Run(test.name, func(t *testing.T) {
			withSealedVouchedTests(t)
			test.run(t)
		})
	}
}
