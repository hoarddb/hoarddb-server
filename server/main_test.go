package server

import (
	"os"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// testMemoryLimitBytes is the memory limit every server built by this package's
// tests sizes its memtable from. Read from the test host instead, a test's
// memtable, and with it the batch bound it meets, would depend on the machine
// CI runs on (memtable-host-scaled-design.md §9, "Trap"). One byte under 7 GiB is the
// largest limit that still chooses 64 MiB, so the batch bound (10 MB) and the
// "not large" threshold these tests were written against hold now that the
// automatic cap is 128 MiB (burn-down §4d item 23).
const testMemoryLimitBytes = int64(7)<<30 - 1

func TestMain(m *testing.M) {
	memtableMemoryLimit = func() (memoryLimit, error) {
		return memoryLimit{bytes: testMemoryLimitBytes, source: limitMemTotalNoLimit}, nil
	}
	// The server tests' logs stay unpreallocated whatever a helper's config
	// says: about 80 tests build configs through several helpers, and a
	// preallocating log would write 128 MiB of zeros per node. The tests that
	// need another answer call resolveWALPreallocate or replace this variable
	// themselves.
	walPreallocateResolver = func(cfg *config.Config, walDir string) walModeResolution {
		return walModeResolution{Configured: cfg.Storage.WALPreallocate, Reason: "pinned off by the server tests"}
	}
	os.Exit(m.Run())
}
