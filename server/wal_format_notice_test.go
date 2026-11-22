package server

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
)

// walFormatNoticeCluster is the least appendWAL needs: a log, a data directory
// and a logger whose output the test can read.
func walFormatNoticeCluster(t *testing.T, dataDir string, logs *bytes.Buffer) *Cluster {
	t.Helper()
	wal, err := storage.OpenWAL(filepath.Join(dataDir, "wal"), nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	return &Cluster{
		s: &Server{
			config: &config.Config{Storage: config.StorageConfig{DataDir: dataDir}},
			wal:    wal,
		},
		logger: slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}
}

func countOriginFormatNotices(logs *bytes.Buffer) int {
	return strings.Count(logs.String(), "this node writes its log in the origin format in ")
}

// TestTheFirstOriginWriteTellsTheOperatorTheLogCannotGoBack covers the
// no-rollback point: once a node writes an origin-flagged entry, an earlier
// build reads it as a torn tail and discards it. The operator learns that from
// one INFO line, the first time, and never again on that data directory.
func TestTheFirstOriginWriteTellsTheOperatorTheLogCannotGoBack(t *testing.T) {
	dataDir := t.TempDir()
	var firstRunLogs bytes.Buffer
	first := walFormatNoticeCluster(t, dataDir, &firstRunLogs)
	entry := ReplEntry{Bucket: "app/q", Payload: []byte("x")}

	if _, err := first.appendWAL(entry, storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: 1}); err != nil {
		t.Fatalf("first appendWAL: %v", err)
	}
	if got := countOriginFormatNotices(&firstRunLogs); got != 1 {
		t.Fatalf("after the first origin write the node logged the notice %d times, want once:\n%s", got, firstRunLogs.String())
	}
	if !strings.Contains(firstRunLogs.String(), "level=INFO") || !strings.Contains(firstRunLogs.String(), dataDir) {
		t.Errorf("the notice is not an INFO line naming the data directory %s:\n%s", dataDir, firstRunLogs.String())
	}
	if _, err := first.appendWAL(entry, storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: 1}); err != nil {
		t.Fatalf("second appendWAL: %v", err)
	}
	if got := countOriginFormatNotices(&firstRunLogs); got != 1 {
		t.Errorf("a second write logged the notice again (%d lines total)", got)
	}

	// A restart of the same data directory has already said it.
	var restartLogs bytes.Buffer
	restarted := walFormatNoticeCluster(t, dataDir, &restartLogs)
	if _, err := restarted.appendWAL(entry, storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: 1}); err != nil {
		t.Fatalf("appendWAL after a restart: %v", err)
	}
	if got := countOriginFormatNotices(&restartLogs); got != 0 {
		t.Errorf("a restart logged the notice again (%d lines)", got)
	}
}

// TestWALFormatNoticeLoggedOnceAtTheFirstV2Open: the first start on a build
// that writes log segments in format 2 tells the operator, at the open and
// before any write, that an earlier build now refuses the directory, and
// records the empty segment-format-2 marker so a second start says nothing
// (WAL preallocated segments spec §5). Break by: logging it on every start, or
// only after a write. Fails on the parent: nothing is logged.
func TestWALFormatNoticeLoggedOnceAtTheFirstV2Open(t *testing.T) {
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, "wal")
	const notice = "this node writes its log in segment format 2 in "
	if _, _, err := InitClusterToken(dataDir); err != nil {
		t.Fatalf("InitClusterToken: %v", err)
	}
	var logged syncBuffer
	logger := slog.New(slog.NewTextHandler(&logged, nil))
	for start := 1; start <= 2; start++ {
		qs := newCapacityService(t, func(cfg *config.Config) {
			cfg.Storage.DataDir = dataDir
			cfg.Server.ReplicationFactor = 2
		}, logger)
		if start == 1 {
			requireNoOtherBuild(t, logged.String())
			if !strings.Contains(logged.String(), notice+walDir+`" `) {
				t.Errorf("the first open on this build did not log the segment format 2 notice for %s; the log:\n%s", walDir, logged.String())
			}
			marker, err := os.Stat(filepath.Join(walDir, "segment-format-2"))
			if err != nil || marker.Size() != 0 {
				t.Errorf("the segment-format-2 marker after the first start: %v, %v; want an empty file", marker, err)
			}
		}
		if err := qs.Stop(); err != nil {
			t.Fatalf("Stop after start %d: %v", start, err)
		}
	}
	if count := strings.Count(logged.String(), notice); count != 1 {
		t.Fatalf("two starts logged the segment format 2 notice %d times, want once", count)
	}
}
