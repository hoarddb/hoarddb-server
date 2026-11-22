package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// vouchedState is a ring store's sync marker as this process knows it, and what it counted.
// A store opened by OpenBadgerKV has on false and none of it is used. mu is taken only around
// the marker's comparison and write, never around a Badger call (store-vouched-cut-build.md §6,
// trap 2): the slot's choice, its sequence, its write and its fdatasync all run under it.
type vouchedState struct {
	on bool
	mu sync.Mutex
	// version and floor are the marker on disk, guarded by mu.
	version, floor uint64
	// file is the marker file, open for its in-place advances; slot is the slot holding the
	// marker on disk and sequence its sequence: the next advance writes the other slot at
	// sequence+1, whatever this process wrote before (review R-2 item 4). logFilesSynced is the
	// count of Badger's log files (DB.LogFilesCreated) the last directory fsync covered; 0 at
	// open, so the files Open created are fsynced at the first advance. All guarded by mu.
	file           *os.File
	slot           int
	sequence       uint64
	logFilesSynced uint64
	// floorAtOpen is the floor read at open (1 at first start).
	floorAtOpen uint64
	report      VouchedOpenReport
	// floorSource samples the log floor (§3.6); nil until the node sets it.
	floorSource atomic.Pointer[func() uint64]
	syncs       atomic.Uint64
	advances    atomic.Uint64
	markerWrite atomic.Int64 // nanoseconds spent writing the marker, in all
}

// VouchedOpenReport says what the open of a ring store found and did: a first start, or the
// cut, with what it discarded.
type VouchedOpenReport struct {
	FirstStart bool
	// Vouched is the marker's version at open; Kept is the highest version the store kept;
	// DiscardedCommits, LogsTruncated and LogsRemoved are Badger's VouchedCutReport.
	Vouched, Kept, DiscardedCommits uint64
	LogsTruncated, LogsRemoved      int
}

// beforeVouchedBadgerOpen, when set, runs in OpenVouchedBadgerKV immediately before badger.Open:
// the seam that shows the marker is on disk before Badger creates a file (T8).
var beforeVouchedBadgerOpen func(dir string)

// OpenVouchedBadgerKV opens a ring node's store at dir with the vouched cut
// (store-vouched-cut-spec.md §3.5): the marker is read and a damaged or foreign one refused
// before anything changes; today's three guards run in today's order; a first start creates
// the directory and the marker before Badger creates a file; a store with data files and no
// marker is refused (VouchedMarkerMissingError) before anything changes: this build writes the
// marker before Badger creates a file, so only an earlier build's store, or one whose marker was
// deleted, is in that state; the node adds the advice for what it is (ringDirAdvice). Every later successful sync advances
// the marker (syncAndVouch). The store opens without a key; OpenVouchedBadgerKVChecked takes one.
func OpenVouchedBadgerKV(dir string, tuning BadgerTuning) (*BadgerKV, error) {
	return OpenVouchedBadgerKVChecked(dir, tuning, nil, nil)
}

// OpenVouchedBadgerKVChecked is OpenVouchedBadgerKV that asks beforeFirstStart, when it is not nil,
// whether a store with no data file (with or without a marker) may start. The check runs before
// anything is written, so a refusal leaves the directory as it was and the next open asks again: a
// store the check refused never gets a marker that would let a later start through. The ring node
// refuses a first start when its Raft or its log shows an earlier life (review R-1).
//
// key, when it is not nil, returns Badger's master key, which seals the store (ring encryption at
// rest spec §8); a nil function or a nil key opens it in plaintext. It is called after the
// marker's refusals and beforeFirstStart, and before the marker or Badger writes a file, but before
// the file guards, which need the key to tell a sealed table from a plaintext one: a store those
// guards refuse has already been asked for its key. Such a store holds data files
// (StoreHasDataFiles), so a key function must never create a key for one; the server's ring load
// refuses a store with data files and no key file before the node opens. The marker itself stays plaintext:
// it holds the vouched version and the log floor, two counters, and no document data.
func OpenVouchedBadgerKVChecked(dir string, tuning BadgerTuning, key func() ([]byte, error), beforeFirstStart func() error) (*BadgerKV, error) {
	markerFile, found, err := readVouchedFile(dir)
	if err != nil {
		return nil, err
	}
	marker := markerFile.marker
	if found && markerFile.otherNotWhole != "" {
		warnOneWholeSlot(dir, markerFile)
	}
	hasData, err := hasStoreDataFiles(dir)
	if err != nil {
		return nil, err
	}
	if !found && hasData {
		return nil, &VouchedMarkerMissingError{Dir: dir}
	}
	// Asked whenever the store holds no data file, marker or not: a marker with no data file beside it
	// is not a state a store that opened once reaches (review R2 F-5).
	if !hasData && beforeFirstStart != nil {
		if err := beforeFirstStart(); err != nil {
			return nil, err
		}
	}
	var storeKey []byte
	if key != nil {
		if storeKey, err = key(); err != nil {
			return nil, err
		}
	}
	opts, err := badgerOptions(dir, tuning, storeKey)
	if err != nil {
		return nil, err
	}
	// A marker that vouches nothing: every commit is above it and the node replays it from its
	// log, so a table the MANIFEST does not name is set aside whatever it holds (review R-3).
	if err := guardStoreBeforeOpen(dir, opts, tuning, found && marker.version == 0); err != nil {
		return nil, withoutRepairAdvice(err)
	}
	var report VouchedOpenReport
	if found {
		opts = opts.WithVouchedCut(marker.version)
	} else {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("creating the node store directory %s: %w", dir, err)
		}
		marker = vouchedMarker{version: 0, floor: 1}
		if err := writeVouchedMarker(dir, marker); err != nil {
			return nil, err
		}
		markerFile = vouchedFile{marker: marker, sequence: 1, slot: 0}
		report.FirstStart = true
		opts = opts.WithVouchedCut(0)
	}
	if beforeVouchedBadgerOpen != nil {
		beforeVouchedBadgerOpen(dir)
	}
	db, err := openBadgerDB(dir, opts)
	if err != nil {
		return nil, withoutRepairAdvice(err)
	}
	kv, err := finishBadgerOpen(db, dir, opts)
	if err != nil {
		return nil, err
	}
	// The marker's own descriptor, for the in-place advances; the file's size never changes.
	advances, err := os.OpenFile(filepath.Join(dir, vouchedFileName), os.O_RDWR, 0)
	if err != nil {
		return nil, closeAfterOpenFailure(db, fmt.Errorf("opening the store's sync marker for its advances: %w", err))
	}
	kv.vouched.file, kv.vouched.slot, kv.vouched.sequence = advances, markerFile.slot, markerFile.sequence
	kv.vouched.on = true
	repairAdvice.Store(dir, "") // until its node sets its own (SetRepairAdvice)
	cut := db.VouchedCutReport()
	report.Vouched, report.Kept, report.DiscardedCommits = cut.Vouched, cut.Kept, cut.DiscardedTxns
	report.LogsTruncated, report.LogsRemoved = cut.LogsTruncated, cut.LogsRemoved
	kv.vouched.version, kv.vouched.floor, kv.vouched.floorAtOpen = marker.version, marker.floor, marker.floor
	kv.vouched.report = report
	return kv, nil
}

// warnOneWholeSlot says that the open uses the one whole slot of the marker and why that can
// lose data (review of S-3, F-4). A kill or power cut during an advance tears the slot being
// written and leaves the other, and the node replays from its log what the torn advance vouched;
// nothing is lost. Media damage to the newer slot reads exactly the same, and then the open cuts
// at the older slot's version and discards acknowledged commits above it that the log does not
// hold (seeded data has no log record). The open cannot tell the two apart, so it says so.
func warnOneWholeSlot(dir string, file vouchedFile) {
	slog.Warn(fmt.Sprintf("the store's sync marker %s has one whole slot: slot %d is whole (sequence %d, version %d, "+
		"log floor %d) and slot %d is not (%s), so the store opens at version %d. A process kill or a power cut during "+
		"a marker write leaves this, and the node then replays from its log what the unfinished write vouched. Damage "+
		"to the disk under the newer slot looks the same, and then commits above version %d that this node's log does "+
		"not hold, such as seeded data, are discarded. If you suspect the disk, restore the data directory from a "+
		"backup of it", filepath.Join(dir, vouchedFileName), file.slot, file.sequence, file.marker.version,
		file.marker.floor, 1-file.slot, file.otherNotWhole, file.marker.version, file.marker.version))
}

// hasStoreDataFiles reports whether dir holds a Badger data file: MANIFEST, a memtable log, a
// table or a value log. The marker, its temporary file and hoardDB's own small files are not
// data files, so a first start is exactly "none of these".
func hasStoreDataFiles(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("listing the node store directory %s: %w", dir, err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == badger.ManifestFilename || strings.HasSuffix(name, ".mem") || strings.HasSuffix(name, ".sst") ||
			strings.HasSuffix(name, ".vlog") {
			return true, nil
		}
	}
	return false, nil
}

// StoreHasDataFiles reports whether the store directory dir holds a Badger data file: MANIFEST, a
// memtable log, a table or a value log. A store whose MANIFEST is gone but whose tables remain
// holds data: the server's ring load reads it as a store, so it is judged by its key registry and
// never treated as a store to create a key for (ring encryption at rest spec §5.3).
func StoreHasDataFiles(dir string) (bool, error) {
	return hasStoreDataFiles(dir)
}

// VouchedOpenReport returns what the open found and did; the zero value for a store opened by
// OpenBadgerKV.
func (kv *BadgerKV) VouchedOpenReport() VouchedOpenReport {
	return kv.vouched.report
}

// VouchedFloor returns the log floor the marker held at open: 1 at a first start. The boot scan starts at or below it.
func (kv *BadgerKV) VouchedFloor() uint64 {
	return kv.vouched.floorAtOpen
}

// SetLogFloorSource sets the function every later sync samples the log floor from (§3.6).
// Until it is set, a sync keeps the marker's current floor. Node.open sets it once the boot
// hold is registered, never before (trap 5).
func (kv *BadgerKV) SetLogFloorSource(f func() uint64) {
	kv.vouched.floorSource.Store(&f)
}

// VouchedCounts returns the store syncs that succeeded, the marker advances, and the time the
// marker writes took in all, since the store opened.
func (kv *BadgerKV) VouchedCounts() (syncs, advances uint64, markerWrite time.Duration) {
	return kv.vouched.syncs.Load(), kv.vouched.advances.Load(), time.Duration(kv.vouched.markerWrite.Load())
}

// syncAndVouch is the one path to Badger's sync for BadgerKV.Sync and Commit(true), the six
// steps of §3.3 in order: sample the floor; sync, reading the version first (DB.SyncVouched);
// on an error poison; read the poison again; advance the marker when the version is above
// it, or equal with a higher floor; report. The callers wrap the error as they do today.
func (kv *BadgerKV) syncAndVouch() error {
	var floor uint64
	sampled := false
	if kv.vouched.on {
		if source := kv.vouched.floorSource.Load(); source != nil {
			floor, sampled = (*source)(), true
		}
	}
	version, err := kv.syncDB()
	if err != nil {
		return kv.poisonSync(err)
	}
	// Read after the version, never before (review R-2): Badger counts a log or value-log file
	// before it writes a commit to it, and publishes a commit's version after writing it, so every
	// file that holds a commit at or below version is in this count.
	logFiles := kv.db.LogFilesCreated()
	if poison := kv.syncPoison.Load(); poison != nil {
		return poison
	}
	if !kv.vouched.on {
		kv.vouched.syncs.Add(1)
		return nil
	}
	kv.vouched.mu.Lock()
	defer kv.vouched.mu.Unlock()
	// Read once more under the mutex (review of S-3, F-2): while this sync waited for it, another
	// one's marker advance or Badger sync may have failed and poisoned the store; a poison stored
	// before this read stops this advance. A failure not yet recorded is not seen, and this sync
	// still writes its own marker, in two windows (owner, 2026-11-20: documented, not closed): the
	// other sync's DB.SyncVouched has returned its error and it has not yet reached poisonSync; or
	// the kernel has not yet reported the failure to either sync (the case DB.SyncVouched's own
	// comment names). Outside them this is safe: the marker vouches only what this sync's own
	// Badger sync reported durable before it (I-1), it is one whole slot and its own fdatasync
	// (I-4, I-6), and the store is poisoned, so no sync that starts later writes one. Inside the
	// kernel window this sync's own success may be the failed one's unreported error, and the
	// marker may vouch writes that are not on disk; closing it needs every advance to wait for
	// the syncs in flight, which serialises them.
	if poison := kv.syncPoison.Load(); poison != nil {
		return poison
	}
	kv.vouched.syncs.Add(1)
	// Never below the marker's floor (review of S-3, F-1). A concurrent sync can sample a higher
	// floor after this one sampled and write it first with a lower version; written as sampled, the
	// newer slot would carry a lower floor than the older, which the open reads as damage. Raising
	// it is sound: a floor bounds the cursors of the first-cursor records whose commit is above its
	// version (§3.6), and every record above this version is above that lower one, so the marker's
	// floor bounds them too. A floor too high would skip owed records; this one is not.
	if !sampled || floor < kv.vouched.floor {
		floor = kv.vouched.floor
	}
	if version < kv.vouched.version || (version == kv.vouched.version && floor <= kv.vouched.floor) {
		return nil
	}
	if err := kv.advanceMarker(vouchedMarker{version: version, floor: floor}, logFiles); err != nil {
		return kv.poisonSync(err)
	}
	kv.vouched.version, kv.vouched.floor = version, floor
	return nil
}

// advanceMarker writes m into the marker in place, with kv.vouched.mu held, and counts the advance
// and its time. In order: the directory guard, without its interval (a write through the open
// descriptor would go on working in a directory removed or replaced under the node); the store
// directory's fsync, only when Badger created a log file since the last one, so every log that
// holds a vouched commit has a durable entry before the marker vouches it (I-1); then the slot that
// does not hold the marker on disk, written whole at its offset, and one fdatasync of the file,
// whose size never changes; only then is the marker taken as advanced (I-4). Any failure is the
// caller's to poison, and no sync that starts after the poison writes the marker again: a later
// fsync can succeed without the failed write having reached the disk (C1's failed-fsync
// reasoning). A sync already past the poison check may still write its own marker (syncAndVouch
// names the two windows). A torn or failed slot leaves the other
// slot, the marker before this advance, which is what the open then uses (I-6).
func (kv *BadgerKV) advanceMarker(m vouchedMarker, logFiles uint64) error {
	start := time.Now()
	err := kv.writeNextSlot(m, logFiles)
	kv.vouched.markerWrite.Add(int64(time.Since(start)))
	if err != nil {
		return err
	}
	kv.vouched.advances.Add(1)
	return nil
}

// writeNextSlot is advanceMarker's work, without the counting.
func (kv *BadgerKV) writeNextSlot(m vouchedMarker, logFiles uint64) error {
	if err := kv.dirGuard.checkNow(); err != nil {
		return err
	}
	if logFiles > kv.vouched.logFilesSynced {
		if err := syncDirectory(kv.dir); err != nil {
			return fmt.Errorf("making the store's new log files durable in %s before the sync marker vouches them: %w",
				kv.dir, err)
		}
		// The count read before the fsync, never one read after it: a file created during the
		// fsync may not be covered by it.
		kv.vouched.logFilesSynced = logFiles
	}
	next := vouchedSlot{vouchedMarker: m, sequence: kv.vouched.sequence + 1}
	slot := 1 - kv.vouched.slot
	if err := writeMarkerSlot(kv.vouched.file, next.encode(), int64(slot)*vouchedSlotSize); err != nil {
		return fmt.Errorf("writing slot %d of the store's sync marker (version %d, log floor %d): %w",
			slot, m.version, m.floor, err)
	}
	if err := syncMarkerFile(kv.vouched.file); err != nil {
		return fmt.Errorf("syncing slot %d of the store's sync marker (version %d, log floor %d): %w",
			slot, m.version, m.floor, err)
	}
	kv.vouched.slot, kv.vouched.sequence = slot, next.sequence
	return nil
}
