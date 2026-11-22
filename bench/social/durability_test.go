package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// unsafeWarningLine is the line a node logs at start with the unsafe switch on,
// as slog's text handler writes it (server/ack_sync.go:unsafeAckWarning).
const unsafeWarningLine = `time=2026-10-24T10:00:00.000Z level=WARN msg="storage.unsafe_ack_before_sync is on: a write this node does not log to a write-ahead log (every write while it has no peers in its ring) is answered before it is synced to disk" component=storage`

const quietStartLine = `time=2026-10-24T10:00:00.000Z level=INFO msg="server started" component=server`

func writeTestFile(t *testing.T, name, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatalf("writing %s: %v", name, err)
	}
	return path
}

func durableEngine(t *testing.T, level, env, log string) *hoardEngine {
	t.Helper()
	cfg := &config{Engine: engineHoardDB, Database: "social", OpTimeout: time.Second, Level: level}
	if env != "" {
		cfg.HoardEnv = writeTestFile(t, "launch.env", env)
	}
	if log != "" {
		cfg.HoardLog = writeTestFile(t, "server.log", log)
	}
	return fakeHoardEngine(cfg)
}

// TestHoardDurabilityIsReported: durability() returns nil today. It must report the
// version, the replication factor, the write concern and whether the unsafe switch
// is set, from the launch environment and the log, because health carries none of them.
func TestHoardDurabilityIsReported(t *testing.T) {
	cases := []struct {
		name, level, env, log string
		want                  map[string]string
		failure               string
	}{
		{
			name: "L-A: switch unset, no warning", level: "L-A",
			env: "export HOARDDB_REPLICATION_FACTOR=1\n", log: quietStartLine,
			want: map[string]string{"version": "v1.0-fake", "replication_factor": "1", "write_concern": "node default", "unsafe_ack_before_sync": "false", "unsafe_ack_warning_in_log": "absent", "durability_level": "L-A"},
		},
		{
			name: "L-B: switch set, warning present", level: "L-B",
			env: "HOARDDB_UNSAFE_ACK_BEFORE_SYNC=true\n# a comment\nHOARDDB_WRITE_CONCERN=\"majority\"\n", log: quietStartLine + "\n" + unsafeWarningLine,
			want: map[string]string{"unsafe_ack_before_sync": "true", "unsafe_ack_warning_in_log": "present", "write_concern": "majority", "durability_level": "L-B"},
		},
		{
			name: "L-R with three copies", level: "L-R",
			env: "HOARDDB_REPLICATION_FACTOR=3\nHOARDDB_WRITE_CONCERN=majority\n", log: quietStartLine,
			want: map[string]string{"replication_factor": "3", "write_concern": "majority", "unsafe_ack_before_sync": "false"},
		},
		{
			name: "L-A claimed but the switch is set", level: "L-A",
			env: "HOARDDB_UNSAFE_ACK_BEFORE_SYNC=1\n", log: unsafeWarningLine,
			failure: "L-A",
		},
		{
			name: "L-B claimed but the switch is unset", level: "L-B",
			env: "HOARDDB_REPLICATION_FACTOR=1\n", log: quietStartLine,
			failure: "L-B",
		},
		{
			name: "environment says unsafe, the log has no warning", level: "L-B",
			env: "HOARDDB_UNSAFE_ACK_BEFORE_SYNC=true\n", log: quietStartLine,
			failure: "disagree",
		},
		{
			name: "log has the warning, the environment does not set the switch", level: "L-A",
			env: "HOARDDB_REPLICATION_FACTOR=1\n", log: unsafeWarningLine,
			failure: "disagree",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			facts := durableEngine(t, tc.level, tc.env, tc.log).durability()
			if facts == nil {
				t.Fatal("durability() returned nil")
			}
			for key, want := range tc.want {
				if facts[key] != want {
					t.Errorf("durability()[%q] = %q; want %q (all: %v)", key, facts[key], want, facts)
				}
			}
			failure := facts[durabilityFailureKey]
			if (tc.failure == "") != (failure == "") || !strings.Contains(failure, tc.failure) {
				t.Errorf("integrity failure %q; want one mentioning %q", failure, tc.failure)
			}
		})
	}
}

// TestHoardDurabilityNamesAnUnreadableLaunchFile: a file the harness named but the tool
// cannot read is recorded as a failure, never as a quiet L-A.
func TestHoardDurabilityNamesAnUnreadableLaunchFile(t *testing.T) {
	cfg := &config{Engine: engineHoardDB, Database: "social", OpTimeout: time.Second, Level: "L-A", HoardEnv: filepath.Join(t.TempDir(), "missing.env"), HoardLog: filepath.Join(t.TempDir(), "missing.log")}
	facts := fakeHoardEngine(cfg).durability()
	if !strings.Contains(facts[durabilityFailureKey], "missing.env") {
		t.Errorf("failure %q does not name the unreadable launch environment file", facts[durabilityFailureKey])
	}
}

// TestHoardDurabilityWithoutLaunchFiles: a local run with no harness files says what it
// does not know instead of claiming a level.
func TestHoardDurabilityWithoutLaunchFiles(t *testing.T) {
	facts := durableEngine(t, "", "", "").durability()
	if facts["unsafe_ack_before_sync"] != "unknown" || facts[durabilityFailureKey] != "" {
		t.Errorf("facts %v; want the switch unknown and no failure", facts)
	}
}
