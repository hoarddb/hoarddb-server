package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/fb"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
	"google.golang.org/protobuf/proto"
)

// Badger writes a table before its manifest record, so a kill during the very
// first flush leaves a manifest that names no table, one table, and the
// memtable log the table was built from. A manifest cut to its header looks the
// same on disk, but there the table holds data older than anything left in the
// logs, and Badger deletes it at the next open. The two are told apart by
// version, and the table is only ever read, never trusted.
//
// Why moving the table aside loses nothing. Badger flushes a memtable to an
// .sst, records the table in the manifest, and only then drops the memtable's
// log. A table the manifest does not name is therefore not part of the store
// yet, and the log it was built from is still on disk. When no earlier table
// can exist, the logs hold every write the store has acknowledged, so replaying
// them rebuilds the store and the leftover can only duplicate them. Every
// repair therefore needs the logs to begin at version 1: commit versions start
// at 1 and only grow, a table flushed before this one would hold the writes
// below the logs', and the logs of a store that has flushed once start above 1.
// On top of that:
//
//   - A table whose index reads (complete) must be the flush of the oldest
//     log: its highest version is at or above that log's. A table builds from
//     every entry of one memtable and nothing else, and the log holds a prefix
//     of those entries (a power cut loses the log's unsynced tail, never its
//     start), so the table built from the oldest log reaches at least the
//     log's highest version. A table below it is not that log's flush. What a
//     looser test than equality could admit is a later, compacted table beside
//     a first log whose removal failed, and Badger's order rules that out:
//     DB.flushToNamedTable syncs the manifest change, then removes the log and
//     syncs the directory, and only then does the table enter level 0, so a
//     log that begins at version 1 is still on disk only if no table ever
//     reached a level and no compaction ever ran. That order is guarded by
//     C1's tests (third_party/badger/flushed_log_retire_test.go); the flush
//     enumeration (TestNoStepOfAFlushLeavesAFirstLogBesideALaterTable) guards
//     the unlink before the naming, not the level-0 insert. The logs-begin-at-1
//     condition above is what keeps this safe: it is the only thing standing
//     between a cut manifest over earlier flushes and this relaxation.
//   - A table whose index does not read (half written, no footer) cannot be
//     compared. It must be 000001.sst, the first file id a fresh store
//     allocates; with the logs beginning at version 1, nothing was flushed
//     before it, so nothing older is hidden behind it.
//
// What stays refused: a table below the oldest log's highest version, a
// leftover that is not the only table, a log that is not on disk, and any
// state whose logs begin above version 1. Behind a manifest that names
// nothing these cannot be told apart from damage. Both tests need plaintext
// files; an encrypted store is not confirmed (the index and logs are sealed)
// and gets the general advice.
//
// confirmFirstFlushCrash returns the table's name when the state is confirmed
// a crash. It runs only on the refusal path, never on a healthy open.
func confirmFirstFlushCrash(dir string, tablesOnDisk []uint64, encrypted bool) (tableName string, confirmed bool) {
	if encrypted || len(tablesOnDisk) != 1 {
		return "", false
	}
	tableName = table.IDToFilename(tablesOnDisk[0])
	logs, logsFound := memtableLogVersions(dir)
	if !logsFound || logs.lowest != firstCommitVersion {
		return "", false
	}
	tableMaxVersion, err := tableIndexMaxVersion(filepath.Join(dir, tableName))
	if errors.Is(err, errIncompleteTable) {
		return tableName, tablesOnDisk[0] == firstTableID
	}
	if err != nil {
		return "", false
	}
	return tableName, tableMaxVersion >= logs.oldestHighest
}

const (
	// firstTableID is the id Badger gives the first table of a fresh store.
	firstTableID uint64 = 1
	// firstCommitVersion is the version of a fresh store's first commit.
	firstCommitVersion uint64 = 1
)

// errIncompleteTable marks a table file that does not end in a valid footer
// and index, as a kill during its write leaves it.
var errIncompleteTable = errors.New("the table is incomplete")

// firstFlushQuarantineSuffix is added to the leftover's name. The result does
// not end in .sst, so Badger ignores it, and it sorts beside the original.
const firstFlushQuarantineSuffix = ".first-flush-crash"

// errStoreInUse is returned when the directory lock is held by another process.
var errStoreInUse = errors.New("is in use by another process; stop it first")

// repairFirstFlushCrash moves the confirmed leftovers aside: a first-flush
// leftover, or every table a manifest naming none leaves in a store whose marker
// vouches nothing (nothingVouched). It runs after every refusing guard, so a
// refusal never follows a change, and under the directory lock Badger takes in
// its open, so it never touches a running node's store. The state is confirmed
// again under the lock. A table is moved, never deleted: a name already taken by
// an earlier crash gets a number.
func repairFirstFlushCrash(dir string, opts badger.Options, tableNames []string, nothingVouched bool) error {
	release, held, err := lockStoreDirectory(dir)
	if err != nil {
		return fmt.Errorf("the node store at %s was killed during its first flush: %w. "+
			"Stop this node, move %s out of %s by hand (keep it) and start again", dir, err, strings.Join(tableNames, " and "), dir)
	}
	if held {
		return fmt.Errorf("the node store at %s %w", dir, errStoreInUse)
	}
	defer release()
	again, err := guardStoreFiles(dir, opts, nothingVouched)
	if err != nil {
		return err
	}
	if !slices.Equal(again, tableNames) {
		return fmt.Errorf("the node store at %s changed while it was being checked; start again", dir)
	}
	for _, tableName := range tableNames {
		if err := setTableAside(dir, tableName, nothingVouched); err != nil {
			return err
		}
	}
	return nil
}

// setTableAside renames one leftover table out of Badger's sight, durably, and logs it.
func setTableAside(dir, tableName string, nothingVouched bool) error {
	keptName := tableName + firstFlushQuarantineSuffix
	for attempt := 2; ; attempt++ {
		_, err := os.Lstat(filepath.Join(dir, keptName))
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			return fmt.Errorf("checking whether %s exists: %w", keptName, err)
		}
		keptName = fmt.Sprintf("%s%s.%d", tableName, firstFlushQuarantineSuffix, attempt)
	}
	failure := func(cause error) error {
		return fmt.Errorf("the node store at %s was killed during its first flush and %s must be moved aside, "+
			"but that failed: %w. If %s is still in %s, move it out by hand (keep it) and start again; "+
			"otherwise just start again. Every write still in the memtable log is recovered either way",
			dir, tableName, cause, tableName, dir)
	}
	if err := os.Rename(filepath.Join(dir, tableName), filepath.Join(dir, keptName)); err != nil {
		return failure(err)
	}
	if err := syncDirectory(dir); err != nil {
		return failure(fmt.Errorf("making the move durable: %w", err))
	}
	if nothingVouched {
		slog.Warn("the node store was killed before its first table was in the manifest, and its sync marker vouches "+
			"no write, so the table was moved aside and the node replays its writes from its own log on this start",
			"dir", dir, "table", tableName, "kept_as", keptName)
		return nil
	}
	slog.Warn("the node store was killed during its first flush: its first table was not yet in the manifest, "+
		"so it was moved aside and every write still in the memtable log is replayed on this start",
		"dir", dir, "table", tableName, "kept_as", keptName)
	return nil
}

// tableIndexMaxVersion reads the highest version recorded in a plaintext
// table's index. The table here is one no manifest names, often half written
// by the kill that is being diagnosed, and checkTableFooter has not run on it,
// so every footer length is bounds-checked and the index checksum is verified
// the way Badger's initIndex does before the flatbuffer is read: the
// flatbuffers reader panics on garbage that is merely in bounds.
func tableIndexMaxVersion(tablePath string) (uint64, error) {
	tableBytes, err := os.ReadFile(tablePath)
	if err != nil {
		return 0, err
	}
	remaining := len(tableBytes) - tableFooterLengthBytes
	if remaining < 0 {
		return 0, fmt.Errorf("table %s is %d bytes, too short to hold a footer: %w", tablePath, len(tableBytes), errIncompleteTable)
	}
	checksumLength := int(binary.BigEndian.Uint32(tableBytes[remaining:]))
	checksumAt := remaining - checksumLength
	indexLengthAt := checksumAt - tableFooterLengthBytes
	if checksumLength > remaining || indexLengthAt < 0 {
		return 0, fmt.Errorf("table %s: its footer gives an index checksum of %d bytes: %w", tablePath, checksumLength, errIncompleteTable)
	}
	indexLength := int(binary.BigEndian.Uint32(tableBytes[indexLengthAt:]))
	if indexLength == 0 || indexLength > indexLengthAt {
		return 0, fmt.Errorf("table %s: its footer gives an index of %d bytes: %w", tablePath, indexLength, errIncompleteTable)
	}
	index := tableBytes[indexLengthAt-indexLength : indexLengthAt]
	var expectedChecksum pb.Checksum
	if err := proto.Unmarshal(tableBytes[checksumAt:remaining], &expectedChecksum); err != nil {
		return 0, fmt.Errorf("table %s: reading its index checksum: %w", tablePath, errors.Join(errIncompleteTable, err))
	}
	if err := verifyIndexChecksum(index, &expectedChecksum); err != nil {
		return 0, fmt.Errorf("table %s: %w", tablePath, errors.Join(errIncompleteTable, err))
	}
	return fb.GetRootAsTableIndex(index, 0).MaxVersion(), nil
}

// verifyIndexChecksum is y.VerifyChecksum, which panics on a checksum
// algorithm it does not know; a garbage footer can name one.
func verifyIndexChecksum(index []byte, expected *pb.Checksum) error {
	if expected.Algo != pb.Checksum_CRC32C && expected.Algo != pb.Checksum_XXHash64 {
		return fmt.Errorf("its index checksum names the unknown algorithm %d", expected.Algo)
	}
	return y.VerifyChecksum(index, expected)
}

// logVersions summarises the plaintext memtable logs of a store directory.
type logVersions struct {
	lowest        uint64 // the lowest version in any log
	oldestHighest uint64 // the highest version in the oldest log that holds an entry
}

// memtableLogVersions reads the logs in file-id order and returns their
// versions, and false when there is no log or none holds an entry. Badger's
// log entry is: meta, userMeta, uvarint key length, value length and expiry,
// key, value, CRC32 (Castagnoli, big-endian) over all of that. The version is
// the key's last 8 bytes. Replay stops at the first entry that does not
// verify, as Badger's does.
func memtableLogVersions(dir string) (logVersions, bool) {
	dirEntries, err := os.ReadDir(dir) // sorted by name; ids are zero-padded
	if err != nil {
		return logVersions{}, false
	}
	var versions logVersions
	var found, oldestSeen bool
	for _, dirEntry := range dirEntries {
		if !strings.HasSuffix(dirEntry.Name(), ".mem") {
			continue
		}
		logBytes, err := os.ReadFile(filepath.Join(dir, dirEntry.Name()))
		if err != nil {
			return logVersions{}, false
		}
		var logHighest uint64
		var logHasEntry bool
		for _, entryVersion := range logEntryVersions(logBytes) {
			if !found || entryVersion < versions.lowest {
				versions.lowest, found = entryVersion, true
			}
			logHighest = max(logHighest, entryVersion)
			logHasEntry = true
		}
		if logHasEntry && !oldestSeen {
			versions.oldestHighest, oldestSeen = logHighest, true
		}
	}
	return versions, found
}

// memtableLogHeaderBytes is the key id and base IV that start a log file.
const memtableLogHeaderBytes = 20

func logEntryVersions(logBytes []byte) []uint64 {
	var versions []uint64
	position := memtableLogHeaderBytes
	for position < len(logBytes) {
		entryKey, entryEnd, ok := readLogEntryKey(logBytes, position)
		if !ok {
			break
		}
		if len(entryKey) > 8 {
			versions = append(versions, y.ParseTs(entryKey))
		}
		position = entryEnd
	}
	return versions
}

// readLogEntryKey reads the entry at position and returns its key and the
// offset after it, or false when the entry is zeroed, cut short or fails its CRC.
func readLogEntryKey(logBytes []byte, position int) (entryKey []byte, entryEnd int, ok bool) {
	const metaBytes = 2 // meta and userMeta
	cursor := position + metaBytes
	var lengths [3]uint64 // key length, value length, expiry
	for index := range lengths {
		if cursor >= len(logBytes) {
			return nil, 0, false
		}
		decoded, width := binary.Uvarint(logBytes[cursor:])
		if width <= 0 {
			return nil, 0, false
		}
		lengths[index] = decoded
		cursor += width
	}
	keyLength, valueLength := lengths[0], lengths[1]
	if keyLength == 0 || keyLength > 1<<16 || valueLength > uint64(len(logBytes)) {
		return nil, 0, false
	}
	bodyEnd := cursor + int(keyLength) + int(valueLength)
	if bodyEnd+crc32.Size > len(logBytes) {
		return nil, 0, false
	}
	if binary.BigEndian.Uint32(logBytes[bodyEnd:]) != crc32.Checksum(logBytes[position:bodyEnd], y.CastagnoliCrcTable) {
		return nil, 0, false
	}
	return logBytes[cursor : cursor+int(keyLength)], bodyEnd + crc32.Size, true
}
