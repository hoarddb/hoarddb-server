package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// -mode syncprobe measures the sync figures the durability witness needs on the
// disk under test. The reference is the cheapest data-only flush: a 4 KiB
// pwrite to a preallocated block followed by fdatasync, which commits the data
// and not the file's metadata, as a database's own log sync does. Two
// synchronised figures are printed beside it for the record: the in-place
// overwrite, and a write that extends a file, which also commits the size and
// block map (4.1 ms against 1.7 ms on the build host). They open with
// syncWriteFlag, which is O_DSYNC on Linux and O_SYNC where the platform has no
// O_DSYNC, so the append figure includes the metadata commit on those hosts and
// is not like-for-like across platforms. Only the data-only flush decides.

const (
	syncProbeBlock  = 4096
	syncProbeWrites = 300
	syncProbeWarmup = 20
)

// syncProbeResult is the one JSON line -mode syncprobe prints.
type syncProbeResult struct {
	Mode           string  `json:"mode"`
	Directory      string  `json:"directory"`
	Writes         int     `json:"writes"`
	DatasyncMsP50  float64 `json:"sync_datasync_ms_p50"`
	OverwriteMsP50 float64 `json:"sync_overwrite_ms_p50"`
	AppendMsP50    float64 `json:"sync_append_ms_p50"`
}

// timedSyncedWrites opens path with syncWriteFlag (O_DSYNC on Linux, O_SYNC on
// the platforms without it) and times count 4 KiB writes at the offsets
// offset(i) gives, discarding the first syncProbeWarmup.
func timedSyncedWrites(path string, count int, offset func(int) int64) ([]time.Duration, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|syncWriteFlag, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	block := make([]byte, syncProbeBlock)
	durations := make([]time.Duration, 0, count)
	for i := range syncProbeWarmup + count {
		started := time.Now()
		if _, err := file.WriteAt(block, offset(i)); err != nil {
			_ = file.Close() // the write error is the one to report
			return nil, fmt.Errorf("writing %s: %w", path, err)
		}
		if i >= syncProbeWarmup {
			durations = append(durations, time.Since(started))
		}
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("closing %s: %w", path, err)
	}
	return durations, nil
}

// timedDatasyncWrites times count 4 KiB overwrites of blocks that already
// exist, each followed by fdatasync, discarding the first syncProbeWarmup.
func timedDatasyncWrites(path string, count int) ([]time.Duration, error) {
	file, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	block := make([]byte, syncProbeBlock)
	durations := make([]time.Duration, 0, count)
	for i := range syncProbeWarmup + count {
		started := time.Now()
		if _, err := file.WriteAt(block, int64(i)*syncProbeBlock); err != nil {
			_ = file.Close() // the write error is the one to report
			return nil, fmt.Errorf("writing %s: %w", path, err)
		}
		if err := datasyncFile(file); err != nil {
			_ = file.Close() // the sync error is the one to report
			return nil, fmt.Errorf("fdatasync %s: %w", path, err)
		}
		if i >= syncProbeWarmup {
			durations = append(durations, time.Since(started))
		}
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("closing %s: %w", path, err)
	}
	return durations, nil
}

// measureSync returns the median data-only flush, synced overwrite and synced
// append in dir.
func measureSync(dir string) (figures syncFigures, err error) {
	name := func(kind string) string {
		return filepath.Join(dir, fmt.Sprintf(".syncprobe-%s-%d", kind, os.Getpid()))
	}
	datasyncPath, overwritePath, appendPath := name("datasync"), name("overwrite"), name("append")
	defer func() {
		for _, path := range []string{datasyncPath, overwritePath, appendPath} {
			if removeErr := os.Remove(path); removeErr != nil && !os.IsNotExist(removeErr) && err == nil {
				err = fmt.Errorf("removing %s: %w", path, removeErr)
			}
		}
	}()
	// Write each file once and flush it, so every block is allocated and the
	// timed writes only rewrite them.
	blocks := syncProbeWarmup + syncProbeWrites
	for _, path := range []string{datasyncPath, overwritePath} {
		if err := os.WriteFile(path, make([]byte, blocks*syncProbeBlock), 0o600); err != nil {
			return syncFigures{}, fmt.Errorf("preallocating %s: %w", path, err)
		}
		if err := syncFile(path); err != nil {
			return syncFigures{}, err
		}
	}
	datasyncs, err := timedDatasyncWrites(datasyncPath, syncProbeWrites)
	if err != nil {
		return syncFigures{}, err
	}
	overwrites, err := timedSyncedWrites(overwritePath, syncProbeWrites, func(i int) int64 { return int64(i) * syncProbeBlock })
	if err != nil {
		return syncFigures{}, err
	}
	appends, err := timedSyncedWrites(appendPath, syncProbeWrites, func(i int) int64 { return int64(i) * syncProbeBlock })
	if err != nil {
		return syncFigures{}, err
	}
	return syncFigures{
		DatasyncMs:  milliseconds(quantileOf(datasyncs, 0.5)),
		OverwriteMs: milliseconds(quantileOf(overwrites, 0.5)),
		AppendMs:    milliseconds(quantileOf(appends, 0.5)),
	}, nil
}

// syncFile flushes path so the preallocation is not still in flight when timing starts.
func syncFile(path string) error {
	file, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("opening %s to flush it: %w", path, err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close() // the sync error is the one to report
		return fmt.Errorf("flushing %s: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", path, err)
	}
	return nil
}

// runSyncProbe runs -mode syncprobe.
func runSyncProbe(cfg *config, out io.Writer) error {
	if cfg.SyncDir == "" {
		return fmt.Errorf("-mode syncprobe needs -sync-dir, a directory on the disk under test (the server's data directory)")
	}
	figures, err := measureSync(cfg.SyncDir)
	if err != nil {
		return fmt.Errorf("measuring sync latency in %s: %w", cfg.SyncDir, err)
	}
	return printJSON(out, "sync probe result", syncProbeResult{
		Mode: modeSyncProbe, Directory: cfg.SyncDir, Writes: syncProbeWrites,
		DatasyncMsP50: figures.DatasyncMs, OverwriteMsP50: figures.OverwriteMs, AppendMsP50: figures.AppendMs,
	})
}
