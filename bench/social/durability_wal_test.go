package main

import (
	"strings"
	"testing"
)

const (
	walOnLine      = `time=2026-10-27T10:00:00.000Z level=INFO msg="write-ahead log segments are preallocated (storage.wal_preallocate=on)" component=storage`
	walAutoLine    = `time=2026-10-27T10:00:00.000Z level=INFO msg="write-ahead log segments are preallocated (storage.wal_preallocate=auto: ext2/3/4)" component=storage`
	walOffLine     = `time=2026-10-27T10:00:00.000Z level=INFO msg="write-ahead log segments are not preallocated (storage.wal_preallocate=off)" component=storage`
	walDefaultLine = `time=2026-10-27T10:00:00.000Z level=INFO msg="write-ahead log segments are not preallocated (storage.wal_preallocate=off)" component=storage`
	walNoWALLine   = `time=2026-10-27T10:00:00.000Z level=INFO msg="storage.wal_preallocate=on has no effect here: this node keeps no node-to-node write-ahead log (replication_factor 1, no seed nodes); the entry-ring node logs its own mode" component=storage`
	walRingOnLine  = `time=2026-10-27T10:00:00.001Z level=INFO msg="entry-ring: write-ahead log segments are preallocated (storage.wal_preallocate=on)" component=storage`
)

// TestHoardDurabilityRecordsTheWALPreallocationMode: the leg record carries the
// configured mode (from the launch environment) and the effective one (from the
// server's startup line), so a run cannot pass for a preallocated one by setting
// the key on a node that ignores it.
func TestHoardDurabilityRecordsTheWALPreallocationMode(t *testing.T) {
	cases := []struct {
		name, env, log, configured, effective string
	}{
		{"on", "HOARDDB_WAL_PREALLOCATE=on\n", quietStartLine + "\n" + walOnLine, "on", "on"},
		{"auto on ext4", "HOARDDB_WAL_PREALLOCATE=auto\n", walAutoLine, "auto", "on (ext2/3/4)"},
		{"off", "HOARDDB_WAL_PREALLOCATE='off'\n", walOffLine, "off", "off"},
		{"off by default", "HOARDDB_REPLICATION_FACTOR=2\n", walDefaultLine, "unset", "off"},
		{"no log, then the ring node's own line", "HOARDDB_WAL_PREALLOCATE=on\n", walNoWALLine + "\n" + walRingOnLine, "on", "none (no write-ahead log)"},
		{"no write-ahead log", "HOARDDB_WAL_PREALLOCATE=on\n", walNoWALLine, "on", "none (no write-ahead log)"},
		{"server without the key", "HOARDDB_WAL_PREALLOCATE=on\n", quietStartLine, "on", "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			facts := durableEngine(t, "", tc.env, tc.log).durability()
			if facts["wal_preallocate"] != tc.configured || facts["wal_preallocate_effective"] != tc.effective {
				t.Errorf("wal_preallocate=%q effective=%q; want %q and %q", facts["wal_preallocate"], facts["wal_preallocate_effective"], tc.configured, tc.effective)
			}
		})
	}
	facts := durableEngine(t, "", "", "").durability()
	if facts["wal_preallocate"] != "unknown" || facts["wal_preallocate_effective"] != "unknown" {
		t.Errorf("no launch files: %q and %q; want unknown for both", facts["wal_preallocate"], facts["wal_preallocate_effective"])
	}
}

// TestHoardDurabilityFailsOnAWALModeThatDiffers: a configured mode the server did
// not take, or a key set on a node with no log, is an integrity failure.
func TestHoardDurabilityFailsOnAWALModeThatDiffers(t *testing.T) {
	cases := []struct{ name, env, log, want string }{
		{"on configured, off effective", "HOARDDB_WAL_PREALLOCATE=on\n", walOffLine, "on"},
		{"off configured, on effective", "HOARDDB_WAL_PREALLOCATE=off\n", walOnLine, "off"},
		{"on configured, no log kept", "HOARDDB_WAL_PREALLOCATE=on\n", walNoWALLine, "no effect"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			failure := durableEngine(t, "", tc.env, tc.log).durability()[durabilityFailureKey]
			if !strings.Contains(strings.ToLower(failure), "wal_preallocate") || !strings.Contains(failure, tc.want) {
				t.Errorf("integrity failure %q; want one about wal_preallocate mentioning %q", failure, tc.want)
			}
		})
	}
	ok := durableEngine(t, "", "HOARDDB_WAL_PREALLOCATE=auto\n", walAutoLine).durability()[durabilityFailureKey]
	if ok != "" {
		t.Errorf("auto resolved to on is a match, but the leg failed: %q", ok)
	}
}
