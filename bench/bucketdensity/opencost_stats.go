package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// statSummary describes a set of durations. Open cost is not usefully a mean:
// the number that decides whether a lazy-open design can serve a request is the
// tail, so the tail is reported alongside the middle.
type statSummary struct {
	Count      int     `json:"count"`
	MinMillis  float64 `json:"min_ms"`
	P50Millis  float64 `json:"p50_ms"`
	P95Millis  float64 `json:"p95_ms"`
	MaxMillis  float64 `json:"max_ms"`
	MeanMillis float64 `json:"mean_ms"`
}

// durationStats summarises observed durations using nearest-rank percentiles.
// It copies before sorting: the caller keeps the observations in the order they
// were taken, which is what makes a drift over the run visible.
func durationStats(observed []time.Duration) statSummary {
	if len(observed) == 0 {
		return statSummary{}
	}
	sorted := append([]time.Duration(nil), observed...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var total time.Duration
	for _, d := range sorted {
		total += d
	}
	return statSummary{
		Count:      len(sorted),
		MinMillis:  millis(sorted[0]),
		P50Millis:  millis(nearestRank(sorted, 0.50)),
		P95Millis:  millis(nearestRank(sorted, 0.95)),
		MaxMillis:  millis(sorted[len(sorted)-1]),
		MeanMillis: millis(total / time.Duration(len(sorted))),
	}
}

// nearestRank returns the element at ceil(q*n) in a sorted slice, which is the
// percentile definition that always names an observation actually taken rather
// than interpolating one that was not.
func nearestRank(sorted []time.Duration, quantile float64) time.Duration {
	rank := int(float64(len(sorted))*quantile + 0.9999999)
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

func millis(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / 1e6
}

// processIO is the byte counters the kernel keeps for a process, from
// /proc/<pid>/io. ReadBytes and WriteBytes are the ones that reached the block
// device, so a read served from the page cache does not move ReadBytes — which
// is how this harness proves its "cold" opens were actually cold, and how it
// prices a close as the flush it is.
type processIO struct {
	ReadBytes  int64 `json:"read_bytes"`
	WriteBytes int64 `json:"write_bytes"`
}

func readProcessIO(pid int) (processIO, error) {
	ioPath := fmt.Sprintf("/proc/%d/io", pid)
	raw, err := os.ReadFile(ioPath)
	if err != nil {
		return processIO{}, fmt.Errorf("reading %s: %w", ioPath, err)
	}
	var counters processIO
	seenRead, seenWrite := false, false
	for _, line := range strings.Split(string(raw), "\n") {
		name, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		parsed, parseErr := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if parseErr != nil {
			continue
		}
		switch name {
		case "read_bytes":
			counters.ReadBytes, seenRead = parsed, true
		case "write_bytes":
			counters.WriteBytes, seenWrite = parsed, true
		}
	}
	if !seenRead || !seenWrite {
		return processIO{}, fmt.Errorf("%s has no read_bytes/write_bytes pair", ioPath)
	}
	return counters, nil
}

// evictTreeFromPageCache evicts every regular file under root. A bucket is a
// directory of files — Badger's MANIFEST, KEYREGISTRY, vlog, memtable and SSTs —
// and an open touches most of them, so evicting one is not enough.
func evictTreeFromPageCache(root string) (int, error) {
	evicted := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("walking %q to evict it: %w", path, walkErr)
		}
		if entry.IsDir() {
			return nil
		}
		if err := evictFileFromPageCache(path); err != nil {
			return err
		}
		evicted++
		return nil
	})
	if err != nil {
		return evicted, err
	}
	return evicted, nil
}

// syncTree flushes every regular file under root to the device before it is
// evicted. POSIX_FADV_DONTNEED keeps dirty pages, so evicting a tree that was
// just written would leave most of it resident and quietly report a warm open
// as a cold one.
func syncTree(root string) error {
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("walking %q to sync it: %w", path, walkErr)
		}
		if entry.IsDir() {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("opening %q to sync it: %w", path, err)
		}
		syncErr := file.Sync()
		closeErr := file.Close()
		if syncErr != nil {
			return fmt.Errorf("syncing %q: %w", path, syncErr)
		}
		if closeErr != nil {
			return fmt.Errorf("closing %q after sync: %w", path, closeErr)
		}
		return nil
	})
}
