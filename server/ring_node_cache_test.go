package server

import (
	"errors"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// The ring node's Badger caches are the ones storage.badger_block_cache_mb and
// storage.badger_index_cache_mb name, as they are for the legacy node store.
func TestRingNodeConfigCarriesTheBadgerCacheSizes(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Storage.BadgerBlockCacheMB = 512
	cfg.Storage.BadgerIndexCacheMB = 256

	node := ringNodeConfig(cfg)
	if node.BlockCacheMB != 512 || node.IndexCacheMB != 256 {
		t.Fatalf("ring node caches are %d MB block and %d MB index, want 512 and 256", node.BlockCacheMB, node.IndexCacheMB)
	}
}

func TestRingCacheSizesAreSizedFromTheHostUnlessSet(t *testing.T) {
	const gib = int64(1) << 30
	host := func(bytes int64) func() (memoryLimit, error) {
		return func() (memoryLimit, error) { return memoryLimit{bytes: bytes}, nil }
	}
	unreadable := func() (memoryLimit, error) { return memoryLimit{}, errors.New("no limit") }
	cases := []struct {
		name         string
		block, index int
		limit        func() (memoryLimit, error)
		wantBlock    int
		wantIndex    int
	}{
		{"15 GiB host takes about a sixteenth", 0, 0, host(15 * gib), 960, 480},
		{"a large host stops at 1 GiB", 0, 0, host(256 * gib), 1024, 512},
		{"a 1 GiB device keeps the 128 MB floor", 0, 0, host(gib), 128, 64},
		{"an unreadable limit keeps Badger's defaults", 0, 0, unreadable, 0, 0},
		{"a set block cache is used as given", 300, 0, host(15 * gib), 300, 480},
		{"both set are used as given", 300, 100, unreadable, 300, 100},
	}
	for _, tc := range cases {
		cfg := config.DefaultConfig()
		cfg.Storage.BadgerBlockCacheMB, cfg.Storage.BadgerIndexCacheMB = tc.block, tc.index
		block, index := ringCacheSizesMB(cfg, tc.limit)
		if block != tc.wantBlock || index != tc.wantIndex {
			t.Errorf("%s: got %d MB block and %d MB index, want %d and %d", tc.name, block, index, tc.wantBlock, tc.wantIndex)
		}
	}
}
