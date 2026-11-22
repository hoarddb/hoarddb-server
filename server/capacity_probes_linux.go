//go:build linux

package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// readOpenFDs counts this process's open descriptors, the same readdir of
// /proc/self/fd that client_golang's process_open_fds uses, and reads the
// RLIMIT_NOFILE soft limit they are refused at.
func readOpenFDs() (open, limit int64, err error) {
	open, err = countDirEntries("/proc/self/fd")
	if err != nil {
		return 0, 0, err
	}
	limit, err = readNoFileSoftLimit()
	if err != nil {
		return 0, 0, err
	}
	return open, limit, nil
}

// countDirEntries counts a directory's entries by name, without a stat each.
func countDirEntries(dirPath string) (int64, error) {
	dir, err := os.Open(dirPath)
	if err != nil {
		return 0, fmt.Errorf("opening %s: %w", dirPath, err)
	}
	names, readErr := dir.Readdirnames(-1)
	closeErr := dir.Close()
	if readErr != nil {
		return 0, fmt.Errorf("listing %s: %w", dirPath, readErr)
	}
	if closeErr != nil {
		return 0, fmt.Errorf("closing %s: %w", dirPath, closeErr)
	}
	return int64(len(names)), nil
}

// readNoFileSoftLimit reads RLIMIT_NOFILE's soft limit: open(2) fails with
// EMFILE there. Go raises the soft limit to the hard one at start, so this is
// what the process can actually use.
func readNoFileSoftLimit() (int64, error) {
	var rlimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit); err != nil {
		return 0, fmt.Errorf("reading RLIMIT_NOFILE: %w", err)
	}
	if uint64(rlimit.Cur) > math.MaxInt64 {
		return 0, errors.New("RLIMIT_NOFILE's soft limit is unlimited, so the descriptor ratio has no ceiling to divide by")
	}
	return int64(rlimit.Cur), nil
}

// readMappings counts this process's memory mappings and reads
// vm.max_map_count, where mmap starts failing with ENOMEM.
func readMappings() (count, limit int64, err error) {
	count, err = countMapsLines("/proc/self/maps")
	if err != nil {
		return 0, 0, err
	}
	limit, err = readIntFile("/proc/sys/vm/max_map_count")
	if err != nil {
		return 0, 0, err
	}
	return count, limit, nil
}

// countMapsLines counts the lines of a maps file, one per mapping. It streams
// and never parses: reading maps holds the process's mm read lock, so the
// read is kept as short as the file allows.
func countMapsLines(mapsPath string) (int64, error) {
	mapsFile, err := os.Open(mapsPath)
	if err != nil {
		return 0, fmt.Errorf("opening %s: %w", mapsPath, err)
	}
	lines, readErr := countNewlines(mapsFile)
	closeErr := mapsFile.Close()
	if readErr != nil {
		return 0, fmt.Errorf("reading %s: %w", mapsPath, readErr)
	}
	if closeErr != nil {
		return 0, fmt.Errorf("closing %s: %w", mapsPath, closeErr)
	}
	return lines, nil
}

// countNewlines counts '\n' bytes to EOF, in fixed-size chunks.
func countNewlines(r io.Reader) (int64, error) {
	chunk := make([]byte, 64*1024)
	var lines int64
	for {
		n, err := r.Read(chunk)
		lines += int64(bytes.Count(chunk[:n], []byte{'\n'}))
		if errors.Is(err, io.EOF) {
			return lines, nil
		}
		if err != nil {
			return 0, err
		}
	}
}

// readIntFile reads a file holding one decimal integer, such as a sysctl.
func readIntFile(intPath string) (int64, error) {
	raw, err := os.ReadFile(intPath)
	if err != nil {
		return 0, fmt.Errorf("reading %s: %w", intPath, err)
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s does not hold an integer: %w", intPath, err)
	}
	return parsed, nil
}

// readAnonMemory reads RssAnon + VmSwap from one read of a
// /proc/<pid>/status file, in bytes: the anonymous memory the process owns,
// resident or swapped. A swap-out moves a page from one term to the other,
// so the sum cannot fall while the node degrades (node-capacity-spec.md
// §3.1). Both fields are required; a missing one is an error, never a zero.
func readAnonMemory(statusPath string) (int64, error) {
	lines, err := readLines(statusPath)
	if err != nil {
		return 0, fmt.Errorf("reading %s: %w", statusPath, err)
	}
	fields := map[string]int64{}
	for _, line := range lines {
		for _, name := range []string{"RssAnon", "VmSwap"} {
			value, ok := strings.CutPrefix(line, name+":")
			if !ok {
				continue
			}
			parsed, err := parseKilobytes(statusPath, name, value)
			if err != nil {
				return 0, err
			}
			fields[name] = parsed
		}
	}
	for _, name := range []string{"RssAnon", "VmSwap"} {
		if _, ok := fields[name]; !ok {
			return 0, fmt.Errorf("%s has no %s line; the memory reading cannot be trusted", statusPath, name)
		}
	}
	return fields["RssAnon"] + fields["VmSwap"], nil
}

// hostMemoryLimit is the memory-limit member's production reading.
func hostMemoryLimit() (memoryLimit, error) {
	return readMemoryLimit(hostCgroupPaths)
}

// hostAnonMemory is the anonymous-memory member's production reading.
func hostAnonMemory() (int64, error) {
	return readAnonMemory("/proc/self/status")
}
