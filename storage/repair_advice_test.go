package storage

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
)

// TestTheRingStoresRuntimeRefusalsCarryItsNodesAdvice: [GUARD a damaged read, the write refusal of a
// store stalled behind a damaged table, and the close of such a store give the ring store's advice
// (none until its node sets it, then the node's), never the legacy store's 'remove-node ... empty
// data directory ... seeded'; red when they append storeRepairAdvice whatever the store] (review R2
// F-4). The legacy store keeps its advice.
func TestTheRingStoresRuntimeRefusalsCarryItsNodesAdvice(t *testing.T) {
	refusals := func(dir string) map[string]error {
		return map[string]error{
			"a damaged read":       &DamagedReadError{Dir: dir, Cause: errors.New("table block 3 failed its checksum")},
			"a stalled write":      &StoreRefusesWritesError{Dir: dir, Cause: badger.ErrL0StalledByDamagedTable},
			"a close of the stall": closeStalledError(dir, badger.ErrL0StalledByDamagedTable),
		}
	}
	kv, dir := openVouchedForTest(t)
	for name, err := range refusals(dir) {
		requireNoLegacyAdvice(t, name+", before the node sets advice", err.Error())
	}
	kv.SetRepairAdvice("THE NODE'S ADVICE")
	for name, err := range refusals(dir) {
		requireNoLegacyAdvice(t, name, err.Error())
		isTrue(t, strings.HasSuffix(err.Error(), ". THE NODE'S ADVICE"), "%s does not end in the node's advice: %v", name, err)
	}
	legacy := filepath.Join(t.TempDir(), "badger")
	legacyKV, err := OpenBadgerKV(legacy, vouchedTestTuning, nil)
	noErr(t, err)
	t.Cleanup(func() { _ = legacyKV.Close() })
	for name, err := range refusals(legacy) {
		isTrue(t, strings.Contains(err.Error(), "remove-node"), "the legacy store's %s lost its advice: %v", name, err)
	}
}

func requireNoLegacyAdvice(t *testing.T, name, text string) {
	t.Helper()
	for _, banned := range []string{"remove-node", "empty data directory", "seeded"} {
		isTrue(t, !strings.Contains(text, banned), "the ring store's %s says %q: %s", name, banned, text)
	}
}
