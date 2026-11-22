package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/internal/storeimage"
)

// The marker is two slots written in place (BUG-0860 option (c), slice S-3): an advance overwrites
// the slot that does not hold the marker chosen at open, then makes it durable with one fdatasync;
// no rename and no directory fsync. A torn or failed write of one slot leaves the other slot, the
// marker before the advance, so the open falls back to it (I-6), and the cut discards what only
// the torn advance vouched: the node replays it from its log.

// markerSlotBytes is the size of one slot as the tests read the file: one filesystem block, so a
// write of one never touches the other's block.
const markerSlotBytes = 4096

// tornRegion returns the bytes where an advance changed the marker file: [lo, hi).
func tornRegion(t *testing.T, before, after []byte) (lo, hi int) {
	t.Helper()
	equal(t, len(before), len(after), "an advance changed the marker file's size")
	lo, hi = -1, -1
	for i := range before {
		if before[i] != after[i] {
			if lo < 0 {
				lo = i
			}
			hi = i + 1
		}
	}
	isTrue(t, lo >= 0, "SCENARIO NOT REACHED: the advance did not change the marker file")
	return lo, hi
}

// markerFromBytes reads raw as the marker file of a scratch directory, with the production reader.
func markerFromBytes(t *testing.T, scratch string, raw []byte) (vouchedMarker, error) {
	t.Helper()
	noErr(t, os.WriteFile(filepath.Join(scratch, vouchedFileName), raw, 0o600))
	m, found, err := readVouchedMarker(scratch)
	if err == nil && !found {
		return m, errors.New("no marker found")
	}
	return m, err
}

// TestAMarkerAdvanceRenamesNothingAndSyncsNoDirectory: [GUARD an advance of the marker is one
// in-place write and one fdatasync of the marker file, with no rename, no fsync of a temporary
// file and no fsync of the store directory, when no memtable log or value-log file was created
// since the last directory fsync; red while the marker is replaced by rename (WriteFileDurably:
// 100 advances made 100 renames and 100 directory fsyncs)]. The diagnosis of 2026-11-19 measured
// the rename marker at 7.6 ms an advance on a quiet VM, about 3.9 advances per seeded pair.
func TestAMarkerAdvanceRenamesNothingAndSyncsNoDirectory(t *testing.T) {
	kv, dir := openVouchedForTest(t)
	// The first advance fsyncs the directory once: the files Open created are not yet covered.
	setDurably(t, kv, "first")
	path := filepath.Join(dir, vouchedFileName)
	before, err := os.Stat(path)
	noErr(t, err)
	logsBefore, err := memtableLogs(dir)
	noErr(t, err)

	var renames, directorySyncs, temporarySyncs, markerSyncs atomic.Int64
	restoreRename, restoreDir, restoreFile, restoreMarker := renameFile, syncDirectory, syncFile, syncMarkerFile
	t.Cleanup(func() {
		renameFile, syncDirectory, syncFile, syncMarkerFile = restoreRename, restoreDir, restoreFile, restoreMarker
	})
	renameFile = func(from, to string) error { renames.Add(1); return os.Rename(from, to) }
	syncDirectory = func(dirPath string) error { directorySyncs.Add(1); return syncDirectoryPath(dirPath) }
	syncFile = func(file *os.File) error { temporarySyncs.Add(1); return file.Sync() }
	syncMarkerFile = func(file *os.File) error { markerSyncs.Add(1); return fdatasyncFile(file) }

	_, advancesBefore, _ := kv.VouchedCounts()
	for i := range 100 {
		setDurably(t, kv, fmt.Sprintf("advance-%03d", i))
	}
	_, advances, _ := kv.VouchedCounts()
	equal(t, advancesBefore+100, advances, "SCENARIO NOT REACHED: 100 synced commits did not make 100 advances")
	logsAfter, err := memtableLogs(dir)
	noErr(t, err)
	equal(t, logsBefore, logsAfter, "SCENARIO NOT REACHED: a memtable log was created during the advances")

	type advanceCalls struct{ Renames, DirectoryFsyncs, TemporaryFileFsyncs, MarkerFdatasyncs int64 }
	equal(t, advanceCalls{MarkerFdatasyncs: 100},
		advanceCalls{renames.Load(), directorySyncs.Load(), temporarySyncs.Load(), markerSyncs.Load()},
		"the calls of 100 advances that created no file")
	after, err := os.Stat(path)
	noErr(t, err)
	isTrue(t, os.SameFile(before, after), "the marker file was replaced by another file")
	equal(t, before.Size(), after.Size(), "an advance changed the marker file's size")
}

// TestEveryTearOfTheSlotBeingWrittenOpensWithTheOldMarker: [GUARD a write of the marker torn at
// any byte, its first bytes reached and the rest not or the reverse, and any subset of its changed
// bytes reached and the rest not (sectors and cache lines reordered), reads as the marker before
// the advance, and the store opens with it: the cut discards the commit only the torn advance
// vouched (the node replays it from its log); the next advance then overwrites the torn slot and
// leaves the valid one; red while the marker is one record that a tear leaves damaged, and red
// when the checksum leaves out the floor or the version (review of S-3, F-3: the CRC over 28 or 20
// bytes)]. The advance moves both the version and the floor, so each of the slot's fields has bytes
// a tear can mix. The bytes are a real advance's, before and after; only the tear is built.
func TestEveryTearOfTheSlotBeingWrittenOpensWithTheOldMarker(t *testing.T) {
	running := filepath.Join(t.TempDir(), "badger")
	kv, err := openVouchedTestStore(running, vouchedTestTuning)
	noErr(t, err)
	var floor atomic.Uint64
	floor.Store(5)
	kv.SetLogFloorSource(func() uint64 { return floor.Load() })
	setDurably(t, kv, "old")
	old := markerOnDisk(t, running)
	oldBytes, err := os.ReadFile(filepath.Join(running, vouchedFileName))
	noErr(t, err)
	floor.Store(0x0a0b0c0d0e0f)
	setDurably(t, kv, "new")
	advanced := markerOnDisk(t, running)
	isTrue(t, advanced.version > old.version, "SCENARIO NOT REACHED: the second synced commit did not advance the marker")
	isTrue(t, advanced.floor > old.floor, "SCENARIO NOT REACHED: the second advance did not move the floor")
	// The kill image after the advance: the logs hold "new", the marker vouches it.
	image, err := storeimage.Take(running)
	noErr(t, err)
	noErr(t, kv.Close())
	newBytes := image[vouchedFileName]
	lo, hi := tornRegion(t, oldBytes, newBytes)

	scratch := t.TempDir()
	tears := 0
	for reached := 0; reached < hi-lo; reached++ {
		// The write's first bytes reached the disk and the rest did not.
		torn := bytes.Clone(oldBytes)
		copy(torn[lo:lo+reached], newBytes[lo:lo+reached])
		m, err := markerFromBytes(t, scratch, torn)
		noErr(t, err, "the marker torn after %d of the %d changed bytes", reached, hi-lo)
		equal(t, old, m, "the marker torn after %d of the %d changed bytes", reached, hi-lo)
		// The write's last bytes reached the disk and its first did not (sectors reordered).
		torn = bytes.Clone(newBytes)
		copy(torn[lo:lo+reached+1], oldBytes[lo:lo+reached+1])
		m, err = markerFromBytes(t, scratch, torn)
		noErr(t, err, "the marker missing its first %d of the %d changed bytes", reached+1, hi-lo)
		equal(t, old, m, "the marker missing its first %d of the %d changed bytes", reached+1, hi-lo)
		tears += 2
	}
	isTrue(t, tears > 0, "SCENARIO NOT REACHED: no tear was built")
	requireEverySubsetTearReadsTheOldMarker(t, oldBytes, newBytes, old)

	// Whole stores: the kill image with the marker torn halfway through the advance's bytes.
	half := lo + (hi-lo)/2
	tornHalf := bytes.Clone(oldBytes)
	copy(tornHalf[lo:half], newBytes[lo:half])
	imageDir := filepath.Join(t.TempDir(), "badger")
	noErr(t, storeimage.Write(imageDir, image))
	noErr(t, os.WriteFile(filepath.Join(imageDir, vouchedFileName), tornHalf, 0o600))
	reopened, err := openVouchedTestStore(imageDir, vouchedTestTuning)
	noErr(t, err, "the store whose last marker write was torn did not open")
	defer func() { noErr(t, reopened.Close()) }()
	equal(t, old.version, reopened.VouchedOpenReport().Vouched, "the open did not cut at the marker before the torn advance")
	isTrue(t, storeHas(t, reopened, "old"), "the commit the older slot vouched is gone")
	isTrue(t, !storeHas(t, reopened, "new"), "the commit only the torn advance vouched was kept: the cut must discard it, "+
		"and the node replays it from its log")

	// The next advance overwrites the torn slot and leaves the valid one byte for byte.
	slotStart := lo / markerSlotBytes * markerSlotBytes
	setDurably(t, reopened, "after the fallback")
	afterFallback, err := os.ReadFile(filepath.Join(imageDir, vouchedFileName))
	noErr(t, err)
	equal(t, len(tornHalf), len(afterFallback), "the advance after a fallback changed the file's size")
	for i := range tornHalf {
		inTornSlot := i >= slotStart && i < slotStart+markerSlotBytes
		if !inTornSlot && tornHalf[i] != afterFallback[i] {
			t.Fatalf("the advance after a fallback wrote byte %d, outside the torn slot [%d, %d): it overwrote the valid "+
				"slot", i, slotStart, slotStart+markerSlotBytes)
		}
	}
	next := markerOnDisk(t, imageDir)
	isTrue(t, next.version >= versionOfKey(t, reopened, "after the fallback"),
		"the advance after a fallback does not vouch its commit (marker %d)", next.version)
	// And a tear of that advance falls back to the same valid slot again.
	lo2, hi2 := tornRegion(t, tornHalf, afterFallback)
	tornAgain := bytes.Clone(tornHalf)
	copy(tornAgain[lo2:lo2+(hi2-lo2)/2], afterFallback[lo2:lo2+(hi2-lo2)/2])
	m, err := markerFromBytes(t, scratch, tornAgain)
	noErr(t, err, "a tear of the advance after a fallback")
	equal(t, old, m, "a tear of the advance after a fallback did not read as the valid slot")
}

// requireEverySubsetTearReadsTheOldMarker builds the file with every proper, non-empty subset of
// the bytes the advance changed taken from after it and the rest from before, and requires each to
// read as the marker before the advance. A slot's bytes are not written in order: a tear can keep
// any of them. Up to 16 changed bytes every subset is built; above that, 65,536 drawn ones.
func requireEverySubsetTearReadsTheOldMarker(t *testing.T, before, after []byte, old vouchedMarker) {
	t.Helper()
	var changed []int
	for i := range before {
		if before[i] != after[i] {
			changed = append(changed, i)
		}
	}
	isTrue(t, len(changed) >= 8, "SCENARIO NOT REACHED: the advance changed only %d bytes", len(changed))
	subsets := make([]uint64, 0, 1<<16)
	if len(changed) <= 16 {
		for mask := uint64(1); mask < 1<<len(changed)-1; mask++ {
			subsets = append(subsets, mask)
		}
	} else {
		draw := rand.New(rand.NewPCG(uint64(len(changed)), 0x5eed))
		for range 1 << 16 {
			subsets = append(subsets, draw.Uint64())
		}
	}
	torn := make([]byte, len(before))
	for _, mask := range subsets {
		copy(torn, before)
		reached := 0
		for bit, at := range changed {
			if mask>>(bit%64)&1 == 1 {
				torn[at] = after[at]
				reached++
			}
		}
		if reached == 0 || reached == len(changed) {
			continue
		}
		read, err := decodeVouchedFile(vouchedFileName, torn)
		noErr(t, err, "the marker with %d of its %d changed bytes reached (subset %b)", reached, len(changed), mask)
		equal(t, old, read.marker, "the marker with %d of its %d changed bytes reached (subset %b)", reached, len(changed), mask)
	}
}

// TestTheBytesOfOneMarkerSlotAreFrozen: [GUARD one slot's 40 bytes, checksum included, are these:
// the format on disk does not change by accident, and the checksum covers the magic, the format,
// the sequence, the version and the floor, 36 bytes; red when the checksum covers fewer bytes or
// another polynomial (review of S-3, F-3)]. The CRC-32C was computed outside Go as well.
func TestTheBytesOfOneMarkerSlotAreFrozen(t *testing.T) {
	slot := vouchedSlot{vouchedMarker{version: 0x0102030405060708, floor: 0x1112131415161718}, 0x2122232425262728}
	const want = "686462766f756368" + "00000002" + "2122232425262728" + "0102030405060708" + "1112131415161718" + "2985385d"
	encoded := slot.encode()
	equal(t, markerSlotBytes, len(encoded), "a slot's block size")
	equal(t, want, hex.EncodeToString(encoded[:40]), "a slot's 40 bytes")
	isTrue(t, bytes.Equal(encoded[40:], make([]byte, markerSlotBytes-40)), "a slot's block after its 40 bytes is not zeros")

	file := append(bytes.Clone(encoded), make([]byte, markerSlotBytes)...)
	read, err := decodeVouchedFile(vouchedFileName, file)
	noErr(t, err, "the frozen slot beside a zeroed one")
	equal(t, vouchedFile{marker: slot.vouchedMarker, sequence: slot.sequence, slot: 0, otherNotWhole: "it fails its checksum"},
		read, "the frozen slot as read")
}

// TestAMarkerWithBothSlotsTornIsRefusedNamingTheFile: [GUARD a marker file in which neither slot is
// whole is refused, naming the file and the damage, with nothing changed and no advice (the ring node
// adds it); red while the marker is one record]. One advance at a time writes one slot, so no crash
// of this build leaves both torn: the state is damage, never a fallback (owner ruling, review R-2
// item 3).
func TestAMarkerWithBothSlotsTornIsRefusedNamingTheFile(t *testing.T) {
	dir := vouchedStoreWithData(t)
	path := filepath.Join(dir, vouchedFileName)
	raw, err := os.ReadFile(path)
	noErr(t, err)
	isTrue(t, len(raw) == 2*markerSlotBytes, "the marker file is %d bytes, not two slots of %d", len(raw), markerSlotBytes)
	raw[20] ^= 0xff
	raw[markerSlotBytes+20] ^= 0xff
	noErr(t, os.WriteFile(path, raw, 0o600))
	before := dirState(t, dir)

	kv, err := openVouchedTestStore(dir, vouchedTestTuning)
	if err == nil {
		noErr(t, kv.Close())
	}
	var refusal *VouchedMarkerDamagedError
	isTrue(t, errors.As(err, &refusal), "want %T in the chain of %v", &refusal, err)
	equal(t, "the store's sync marker "+path+" is damaged (neither of its two slots is whole: each fails its "+
		"checksum), so this node cannot tell which of its last writes reached the disk. Nothing was changed", err.Error())
	requireNoRingAdvice(t, err.Error())
	equal(t, before, dirState(t, dir), "the refused open changed the directory")
}

// TestTheMarkerFileIsCreatedWithItsDirectoryEntrySynced is review R-2 item 2: [GUARD a first start
// creates the marker file whole (a temporary file, its fsync, the rename, the store directory's
// fsync) before Badger opens, so the file and its directory entry are durable before any data
// file exists; red when the file is created in place or without the directory's fsync]. Every
// later advance writes in place and needs no entry (TestAMarkerAdvanceRenamesNothingAndSyncsNoDirectory).
func TestTheMarkerFileIsCreatedWithItsDirectoryEntrySynced(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "badger")
	var mu sync.Mutex
	var events []string
	record := func(event string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, event)
	}
	restoreFile, restoreRename, restoreDir, restoreOpen := syncFile, renameFile, syncDirectory, beforeVouchedBadgerOpen
	t.Cleanup(func() {
		syncFile, renameFile, syncDirectory, beforeVouchedBadgerOpen = restoreFile, restoreRename, restoreDir, restoreOpen
	})
	syncFile = func(file *os.File) error {
		record("fsync of " + filepath.Base(file.Name()))
		return file.Sync()
	}
	renameFile = func(from, to string) error {
		record("rename " + filepath.Base(from) + " to " + filepath.Base(to))
		return os.Rename(from, to)
	}
	syncDirectory = func(dirPath string) error {
		if filepath.Clean(dirPath) == filepath.Clean(dir) {
			record("fsync of the store directory")
		}
		return syncDirectoryPath(dirPath)
	}
	beforeVouchedBadgerOpen = func(string) { record("badger.Open") }

	kv, err := openVouchedTestStore(dir, vouchedTestTuning)
	noErr(t, err)
	noErr(t, kv.Close())
	mu.Lock()
	defer mu.Unlock()
	isTrue(t, len(events) >= 4, "too few events: %v", events)
	equal(t, []string{"fsync of VOUCHED.tmp", "rename VOUCHED.tmp to VOUCHED", "fsync of the store directory", "badger.Open"},
		events[:4], "the first start's creation of the marker")
}

// TestAnOpenOnOneWholeSlotWarnsNamingBothSlots: [GUARD an open that uses the one whole slot of the
// marker while the other is not whole logs one WARN naming the marker, the slot it uses with its
// sequence, version and floor, the other slot and what is wrong with it, and the operator's advice;
// an open on two whole slots logs none; red while the fallback is silent (review of S-3, F-4)]. A
// torn advance and media damage to the newer slot read the same, and the second discards
// acknowledged commits the log does not hold: the operator must at least be told.
func TestAnOpenOnOneWholeSlotWarnsNamingBothSlots(t *testing.T) {
	for _, row := range []struct {
		name, state string
		damage      func(slot []byte)
	}{
		{"a slot failing its checksum", "it fails its checksum", func(slot []byte) { slot[27] ^= 0x01 }},
		{"a whole slot without the magic", "it does not begin with the marker's magic", func(slot []byte) {
			slot[0] = 'H'
			binary.BigEndian.PutUint32(slot[36:], crc32.Checksum(slot[:36], vouchedChecksumTable))
		}},
	} {
		t.Run(row.name, func(t *testing.T) {
			dir := vouchedStoreWithData(t)
			logged := captureLog(t)
			clean, err := openVouchedTestStore(dir, vouchedTestTuning)
			noErr(t, err)
			setDurably(t, clean, "older")
			setDurably(t, clean, "newer")
			noErr(t, clean.Close())
			equal(t, 0, len(logged.lines("one whole slot")), "an open on two whole slots warned")

			path := filepath.Join(dir, vouchedFileName)
			raw, err := os.ReadFile(path)
			noErr(t, err)
			newest, err := decodeVouchedFile(path, raw)
			noErr(t, err)
			// The older slot as read alone: the file with the newer slot zeroed.
			olderAlone := bytes.Clone(raw)
			clear(olderAlone[newest.slot*markerSlotBytes:][:markerSlotBytes])
			older, err := decodeVouchedFile(path, olderAlone)
			noErr(t, err)
			isTrue(t, older.slot != newest.slot && older.marker.version < newest.marker.version,
				"SCENARIO NOT REACHED: the slots are %+v and %+v", newest, older)
			row.damage(raw[newest.slot*markerSlotBytes:][:markerSlotBytes])
			noErr(t, os.WriteFile(path, raw, 0o600))

			kv, err := openVouchedTestStore(dir, vouchedTestTuning)
			noErr(t, err, "the store whose newer slot is damaged did not open on the older")
			defer func() { noErr(t, kv.Close()) }()
			equal(t, older.marker.version, kv.VouchedOpenReport().Vouched, "the open did not cut at the older slot")
			warnings := logged.lines("one whole slot")
			equal(t, 1, len(warnings), "WARN lines about the marker's slots: %v", warnings)
			for _, part := range []string{
				"level=WARN",
				path,
				fmt.Sprintf("slot %d is whole (sequence %d, version %d, log floor %d)", older.slot, older.sequence,
					older.marker.version, older.marker.floor),
				fmt.Sprintf("slot %d is not (%s)", newest.slot, row.state),
				fmt.Sprintf("the store opens at version %d", older.marker.version),
				"restore the data directory from a backup of it",
			} {
				isTrue(t, strings.Contains(warnings[0], part), "the WARN does not say %q: %s", part, warnings[0])
			}
		})
	}
}
