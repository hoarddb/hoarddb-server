package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// MemtableSizeFileName is the marker, kept beside the Badger files in the
// store directory, that records the memtable size the store was last opened
// with. Badger records it nowhere, and needs it: it replays a crashed
// memtable's write-ahead log into an arena sized for the memtable it is opened
// with, so a log written at a larger size does not fit a smaller one and
// Badger exits the process with "Arena too small". The file holds the size in
// bytes as decimal text and is plaintext in an encrypted store; it names no
// data.
const MemtableSizeFileName = "memtable-size"

// legacyMemtableBytes is what a directory with no marker was written with:
// builds before the setting existed ran a 32 MiB memtable and recorded nothing.
const legacyMemtableBytes = 32 << 20

// MemtableShrinkError is the refusal to open a store at a smaller memtable than
// the one it crashed at. Nothing in the directory is changed or lost.
type MemtableShrinkError struct {
	Dir         string
	PreviousMB  int
	RequestedMB int
	// LogFiles are the write-ahead logs of unflushed memtables that remain.
	LogFiles []string
	// RequestedBy names the setting that asked for RequestedMB, such as
	// "HOARDDB_MEMTABLE_MB". Empty means one of the two, not known which.
	RequestedBy string
	// RequestedByChoice says RequestedMB is the store's own choice, used
	// because no setting is set: the fix is then to set one for a single
	// start and remove it again, not to "lower" anything.
	RequestedByChoice bool
}

func (e *MemtableShrinkError) Error() string {
	crash := fmt.Sprintf("but its node store directory %s was last running with %d MiB and was not stopped cleanly: "+
		"%d write-ahead log file(s) of unflushed writes remain (%s), and they do not fit in a %d MiB memtable. Nothing is lost. ",
		e.Dir, e.PreviousMB, len(e.LogFiles), strings.Join(e.LogFiles, ", "), e.RequestedMB)
	if e.RequestedByChoice {
		return fmt.Sprintf("Memtable size mismatch: this node now runs at its chosen %d MiB because storage.memtable_mb is not set and HOARDDB_MEMTABLE_MB is not set, "+
			"%sTo fix it, set HOARDDB_MEMTABLE_MB=%d (or storage.memtable_mb: %d in the config file), start the node once and wait until it is serving, "+
			"then stop it cleanly (SIGTERM or Ctrl-C, not kill -9) and remove the setting: the node goes back to its chosen %d MiB.",
			e.RequestedMB, crash, e.PreviousMB, e.PreviousMB, e.RequestedMB)
	}
	requestedBy := e.RequestedBy
	if requestedBy == "" {
		requestedBy = "storage.memtable_mb or HOARDDB_MEMTABLE_MB"
	}
	return fmt.Sprintf("Memtable size mismatch: this node is set to %d MiB by %s, "+
		"%sTo fix it, set storage.memtable_mb (or HOARDDB_MEMTABLE_MB) to %d, start the node once and wait until it is serving, "+
		"then stop it cleanly (SIGTERM or Ctrl-C, not kill -9). After that you can lower it to %d.",
		e.RequestedMB, requestedBy, crash, e.PreviousMB, e.RequestedMB)
}

// guardMemtableShrink refuses to open dir at requestedBytes when the store was
// last opened at a larger memtable and a crash left write-ahead logs behind.
// It changes nothing.
func guardMemtableShrink(dir string, requestedBytes int64, tuning BadgerTuning) error {
	previousBytes, err := previousMemtableBytes(dir)
	if err != nil {
		return err
	}
	if requestedBytes >= previousBytes {
		return nil
	}
	logFiles, err := filepath.Glob(filepath.Join(dir, "*.mem"))
	if err != nil {
		return fmt.Errorf("listing the write-ahead logs in %s: %w", dir, err)
	}
	unflushed := make([]string, 0, len(logFiles))
	for _, logFile := range logFiles {
		hasEntries, err := memtableLogHasEntries(logFile)
		if err != nil {
			return err
		}
		if hasEntries {
			unflushed = append(unflushed, filepath.Base(logFile))
		}
	}
	if len(unflushed) == 0 {
		return nil
	}
	return &MemtableShrinkError{
		Dir:         dir,
		PreviousMB:  int(previousBytes >> 20),
		RequestedMB: int(requestedBytes >> 20),
		LogFiles:    unflushed,

		RequestedBy:       tuning.MemTableRequestedBy,
		RequestedByChoice: tuning.MemTableRequestedByChoice,
	}
}

// memtableLogHeaderSize is what Badger writes into a .mem write-ahead log
// before its first entry: an 8-byte key id and a 12-byte base IV
// (third_party/badger/value.go:vlogHeaderSize). memtableLogEntryHeaderMax is the
// longest an entry header can be (third_party/badger/structs.go:maxHeaderSize).
const (
	memtableLogHeaderSize     = 20
	memtableLogEntryHeaderMax = 22
)

// memtableLogHasEntries reports whether a .mem write-ahead log holds an entry
// Badger would replay. Badger opens a fresh memtable log on every open and
// leaves the bytes past its 20-byte header zeroed until something is written
// (third_party/badger/memtable.go:bootstrap and logFile.zeroNextEntry), and its
// replay stops at the first entry whose key is empty
// (third_party/badger/memtable.go:logFile.iterate). A first entry header that
// is all zero therefore holds nothing to replay, which is the log a node killed
// with nothing unflushed leaves. Any non-zero byte is treated as an entry, so an
// unreadable or malformed log still reaches the refusal rather than Badger's
// fatal.
func memtableLogHasEntries(path string) (bool, error) {
	logFile, err := os.Open(path)
	if err != nil {
		return false, fmt.Errorf("reading write-ahead log %s: %w", path, err)
	}
	defer func() { _ = logFile.Close() }() // read-only; nothing to flush
	header := make([]byte, memtableLogEntryHeaderMax)
	readBytes, err := logFile.ReadAt(header, memtableLogHeaderSize)
	if err != nil && !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("reading write-ahead log %s: %w", path, err)
	}
	if err != nil {
		// The log ends inside its first entry header: a log trimmed to the
		// 20-byte header (readBytes 0) has nothing to replay, and a partial
		// header is treated as an entry rather than guessed at.
		return readBytes > 0, nil
	}
	for _, b := range header {
		if b != 0 {
			return true, nil
		}
	}
	return false, nil
}

// PreviousMemtableMB is the memtable size, in MiB rounded up, that the store in
// dir last ran at: the larger of its marker and what its write-ahead logs show
// (previousMemtableBytes). A directory with neither is an earlier build's, which
// ran 32 MiB.
func PreviousMemtableMB(dir string) (int, error) {
	previousBytes, err := previousMemtableBytes(dir)
	if err != nil {
		return 0, err
	}
	return int((previousBytes + (1 << 20) - 1) >> 20), nil
}

// previousMemtableBytes is the size the store last ran at, read from two
// sources because either can be missing: the marker, which a hand
// deletion or a copy of the data files can lose, and the write-ahead logs a
// crash leaves, which Badger creates at twice the memtable size
// (third_party/badger/memtable.go:DB.openMemTable). A log that has been
// replayed once is truncated to its contents and then understates the size; the
// marker written by that replaying open covers it.
func previousMemtableBytes(dir string) (int64, error) {
	markerBytes, err := readMemtableSize(dir)
	if err != nil {
		return 0, err
	}
	logFiles, err := filepath.Glob(filepath.Join(dir, "*.mem"))
	if err != nil {
		return 0, fmt.Errorf("listing the write-ahead logs in %s: %w", dir, err)
	}
	previousBytes := markerBytes
	for _, logFile := range logFiles {
		logInfo, err := os.Stat(logFile)
		if err != nil {
			return 0, fmt.Errorf("reading the size of write-ahead log %s: %w", logFile, err)
		}
		previousBytes = max(previousBytes, logInfo.Size()/2)
	}
	return previousBytes, nil
}

// readMemtableSize returns the size the store was last opened with, or the
// pre-setting default when no marker exists (a new directory, or one written by
// an earlier build).
func readMemtableSize(dir string) (int64, error) {
	markerPath := filepath.Join(dir, MemtableSizeFileName)
	text, err := os.ReadFile(markerPath)
	if errors.Is(err, os.ErrNotExist) {
		return legacyMemtableBytes, nil
	}
	if err != nil {
		return 0, fmt.Errorf("reading %s to check the memtable size: %w", markerPath, err)
	}
	if len(strings.TrimSpace(string(text))) == 0 {
		return 0, fmt.Errorf("%s is empty: that is what a power loss while the node was starting leaves behind. "+
			"Leave the file in place, because the store may hold write-ahead logs written at a larger size. "+
			"Write the memtable size this node last ran with into it, in bytes as decimal text "+
			"(33554432 for 32 MiB, 67108864 for 64 MiB, 134217728 for 128 MiB), then start the node again",
			markerPath)
	}
	sizeBytes, err := strconv.ParseInt(strings.TrimSpace(string(text)), 10, 64)
	if err != nil || sizeBytes <= 0 {
		return 0, fmt.Errorf("%s does not hold a memtable size in bytes (%q). "+
			"If the node was stopped cleanly, delete the file and start again; otherwise restore it or start at the memtable size the node last ran with",
			markerPath, strings.TrimSpace(string(text)))
	}
	return sizeBytes, nil
}

// recordMemtableSize writes the marker after a successful open. It is written
// after, not before, so a second process refused by the directory lock cannot
// overwrite it; the window between Open returning and this write holds no
// writes of this run's. It is written durably: an empty marker after
// a power loss would refuse the next start.
func recordMemtableSize(dir string, sizeBytes int64) error {
	markerPath := filepath.Join(dir, MemtableSizeFileName)
	if err := WriteFileDurably(markerPath, []byte(strconv.FormatInt(sizeBytes, 10)+"\n")); err != nil {
		return fmt.Errorf("recording the memtable size: %w", err)
	}
	return nil
}
