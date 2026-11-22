package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
)

// StoreDamagedError is the refusal to open a node store whose files are
// damaged. guardStoreFiles returns it for what Badger's open would mishandle:
// a panic on a goroutine of Badger's own, where no caller can recover it, or
// an empty start that deletes the table files holding the data.
// OpenBadgerKV returns it for a checksum Badger's open finds wrong.
type StoreDamagedError struct {
	Dir    string // the store directory
	Damage string // what is damaged, naming the file
	Risk   string // what opening it anyway would do; empty when Badger refused or the harm is not one named effect
	Cause  error  // Badger's own error, when Badger found the damage
	Repair string // replaces the generic restore-or-reseed advice when the state has a cheaper repair
	// AdviceByCaller leaves the advice out of the text, for the caller to add: the ring node's
	// store, where it depends on whether the node is a cluster member (withoutRepairAdvice).
	AdviceByCaller bool
}

func (e *StoreDamagedError) Error() string {
	refusal := "The store cannot be opened safely, so it was not opened and nothing in it was changed"
	switch {
	case e.Cause != nil:
		refusal = "The store was not opened"
	case e.Risk != "":
		refusal = fmt.Sprintf("Starting would %s, so the store was not opened and nothing in it was changed", e.Risk)
	}
	repair := e.Repair
	if repair == "" && e.AdviceByCaller {
		return fmt.Sprintf("the node store at %s is damaged: %s. %s", e.Dir, e.Damage, refusal)
	}
	if repair == "" {
		repair = storeRepairAdvice(e.Dir)
	}
	return fmt.Sprintf("the node store at %s is damaged: %s. %s. %s", e.Dir, e.Damage, refusal, repair)
}

func (e *StoreDamagedError) Unwrap() error { return e.Cause }

// badgerChecksumMismatch reports whether Badger's open failed on a checksum.
// Badger's y.Wrap flattens its errors with %+v, so errors.Is cannot see
// y.ErrChecksumMismatch; the text is matched instead, and the vendored copy
// pins it (TestATruncatedTableIsAnOpenErrorNotAPanic's flipped-index case).
func badgerChecksumMismatch(openErr error) bool {
	return strings.Contains(openErr.Error(), y.ErrChecksumMismatch.Error())
}

// guardStoreFiles checks what Badger's open does not check safely: that the
// manifest names the tables on disk, and that every table it names has a
// footer that fits inside the file. It changes nothing. It returns the names of
// the leftovers to set aside, which the caller moves with repairFirstFlushCrash
// once every other guard has passed: a confirmed first-flush leftover, or, when
// nothingVouched (a vouched store whose marker vouches no commit), every table
// a manifest naming none leaves. It costs one manifest replay, which Badger
// repeats, and two small reads per table: O(tables) at open, nothing per
// bucket and nothing after the open.
func guardStoreFiles(dir string, opts badger.Options, nothingVouched bool) (leftovers []string, err error) {
	tablesOnDisk, err := tableIDsOnDisk(dir)
	if err != nil {
		return nil, err
	}
	if len(tablesOnDisk) == 0 {
		// A new store, or one whose data is all still in memtable logs.
		return nil, nil
	}
	referenced, leftovers, err := tablesTheManifestNames(dir, opts, tablesOnDisk, nothingVouched)
	if err != nil {
		return nil, err
	}
	if len(leftovers) > 0 {
		return leftovers, nil
	}
	for _, tableID := range tablesOnDisk {
		if _, named := referenced[tableID]; !named {
			// Badger deletes a table its manifest does not name. That is how it
			// clears a table written just before a crash, whose manifest record
			// never landed, so only a manifest that names nothing is refused.
			continue
		}
		if err := checkTableFooter(dir, tableID); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func tableIDsOnDisk(dir string) ([]uint64, error) {
	dirEntries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("listing the node store at %s: %w", dir, err)
	}
	// os.ReadDir sorts by name, and table names are zero-padded ids, so the
	// first damaged table reported is the lowest id.
	var tableIDs []uint64
	for _, dirEntry := range dirEntries {
		if tableID, isTable := table.ParseFileID(dirEntry.Name()); isTable && !dirEntry.IsDir() {
			tableIDs = append(tableIDs, tableID)
		}
	}
	return tableIDs, nil
}

// tablesTheManifestNames replays the manifest read-only. A store with tables
// and a manifest that is missing or names none is refused: Badger would create
// or keep the empty history and delete every table. The exceptions are a
// confirmed first-flush crash, and a manifest naming none in a vouched store
// whose marker vouches nothing (nothingVouched): their names are returned as
// leftovers and nothing is changed here.
func tablesTheManifestNames(dir string, opts badger.Options, tablesOnDisk []uint64, nothingVouched bool) (tables map[uint64]badger.TableManifest, leftovers []string, err error) {
	tableCount := len(tablesOnDisk)
	manifestPath := filepath.Join(dir, badger.ManifestFilename)
	manifestFile, err := os.Open(manifestPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, &StoreDamagedError{Dir: dir,
			Damage: fmt.Sprintf("%s is missing, and the directory holds %d table files", badger.ManifestFilename, tableCount),
			Risk:   "start an empty store and delete those table files"}
	}
	if err != nil {
		return nil, nil, fmt.Errorf("opening the node store's manifest %s: %w", manifestPath, err)
	}
	defer func() { _ = manifestFile.Close() }() // read-only; nothing to flush

	replayed, _, err := badger.ReplayManifestFile(manifestFile, opts.ExternalMagicVersion, opts)
	if err != nil {
		// Badger refuses this manifest too. A damage refusal, so the ring node's open can leave
		// the advice to its caller as for every other damage.
		return nil, nil, &StoreDamagedError{Dir: dir,
			Damage: fmt.Sprintf("its manifest %s cannot be read (%v)", manifestPath, err)}
	}
	if len(replayed.Tables) == 0 {
		if nothingVouched {
			// The marker vouches no commit, so no table holds one the node needs from this
			// store: the cut discards every commit above the marker from the logs, and the node
			// replays them from its own log from the marker's floor. Setting the tables aside
			// loses nothing, whatever they hold and whatever the logs kept (review R-3: a power
			// cut during a first flush with nothing synced can leave the logs empty or gone).
			for _, tableID := range tablesOnDisk {
				leftovers = append(leftovers, table.IDToFilename(tableID))
			}
			return nil, leftovers, nil
		}
		if tableName, confirmed := confirmFirstFlushCrash(dir, tablesOnDisk, len(opts.EncryptionKey) > 0); confirmed {
			// The one state that is repaired rather than refused; the argument
			// is in confirmFirstFlushCrash.
			return nil, []string{tableName}, nil
		}
		return nil, nil, &StoreDamagedError{Dir: dir,
			Damage: fmt.Sprintf("%s names no table, but the directory holds %s; the manifest was cut short or overwritten, "+
				"and these files may be the only copy of the data written before it",
				badger.ManifestFilename, tableList(tablesOnDisk)),
			Risk: "start an empty store and delete those table files"}
	}
	return replayed.Tables, nil, nil
}

// Badger's table footer, read from the end of the file: the index, its
// length (4 bytes), the index checksum, the checksum's length (4 bytes). Both
// lengths are big-endian (y.BytesToU32). Badger's own open slices the file with
// them unchecked and panics when a cut or zeroed tail makes one point outside
// it; a footer that fits is then verified by Badger's index checksum, which
// fails as an error.
const tableFooterLengthBytes = 4

func checkTableFooter(dir string, tableID uint64) error {
	tableName := table.IDToFilename(tableID)
	tablePath := filepath.Join(dir, tableName)
	tableFile, err := os.Open(tablePath)
	if err != nil {
		return fmt.Errorf("opening table %s of the node store: %w", tablePath, err)
	}
	defer func() { _ = tableFile.Close() }() // read-only; nothing to flush

	fileInfo, err := tableFile.Stat()
	if err != nil {
		return fmt.Errorf("reading the size of table %s: %w", tablePath, err)
	}
	damaged := func(damage string) error {
		return &StoreDamagedError{Dir: dir,
			Damage: fmt.Sprintf("table %s is %d bytes and %s; the file was cut short or its end overwritten", tableName, fileInfo.Size(), damage)}
	}
	remaining := fileInfo.Size() - tableFooterLengthBytes
	if remaining < 0 {
		return damaged("too short to hold a table footer")
	}
	checksumLength, err := readFooterLength(tableFile, remaining)
	if err != nil {
		return fmt.Errorf("reading the footer of table %s: %w", tablePath, err)
	}
	remaining -= checksumLength + tableFooterLengthBytes
	// A checksum length of 0 is valid: a CRC32C of 0 marshals to an empty
	// message, and Badger opens such a table. A zeroed tail is
	// still caught by the index length below.
	if remaining < 0 {
		return damaged(fmt.Sprintf("its footer gives an index checksum of %d bytes", checksumLength))
	}
	indexLength, err := readFooterLength(tableFile, remaining)
	if err != nil {
		return fmt.Errorf("reading the footer of table %s: %w", tablePath, err)
	}
	if indexLength == 0 || indexLength > remaining {
		return damaged(fmt.Sprintf("its footer gives an index of %d bytes", indexLength))
	}
	return nil
}

func readFooterLength(tableFile *os.File, offset int64) (int64, error) {
	var lengthBytes [tableFooterLengthBytes]byte
	// ReadAt may return io.EOF with every byte read when the field ends the file.
	if bytesRead, err := tableFile.ReadAt(lengthBytes[:], offset); bytesRead < len(lengthBytes) {
		return 0, fmt.Errorf("at offset %d: %w", offset, err)
	}
	return int64(binary.BigEndian.Uint32(lengthBytes[:])), nil
}

// tableList names up to three table files for an error, then counts the rest.
func tableList(tableIDs []uint64) string {
	const shown = 3
	names := make([]string, 0, shown)
	for index, tableID := range tableIDs {
		if index == shown {
			names = append(names, fmt.Sprintf("and %d more", len(tableIDs)-shown))
			break
		}
		names = append(names, table.IDToFilename(tableID))
	}
	return strings.Join(names, ", ")
}
