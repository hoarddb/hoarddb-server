package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"

	"github.com/hoarddb/hoarddb-server/internal/storeimage"
)

var vouchedTestTuning = BadgerTuning{MemTableMB: 8}

// noErr, isTrue and equal fail the test at once; the optional message is a format and its args.
func noErr(t *testing.T, err error, msgAndArgs ...any) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", messageOf("unexpected error", msgAndArgs), err)
	}
}

func isTrue(t *testing.T, cond bool, msgAndArgs ...any) {
	t.Helper()
	if !cond {
		t.Fatal(messageOf("condition not met", msgAndArgs))
	}
}

func equal(t *testing.T, want, got any, msgAndArgs ...any) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("%s:\nwant %#v\ngot  %#v", messageOf("not equal", msgAndArgs), want, got)
	}
}

func messageOf(fallback string, msgAndArgs []any) string {
	if len(msgAndArgs) == 0 {
		return fallback
	}
	return fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
}

// setDurably commits key with a synced commit.
func setDurably(t *testing.T, kv *BadgerKV, key string) {
	t.Helper()
	batch := kv.NewBatch()
	noErr(t, batch.Set([]byte(key), []byte("value of "+key)))
	noErr(t, batch.Commit(true))
}

// setUnsynced commits key without a sync.
func setUnsynced(t *testing.T, kv *BadgerKV, key string) {
	t.Helper()
	batch := kv.NewBatch()
	noErr(t, batch.Set([]byte(key), []byte("value of "+key)))
	noErr(t, batch.Commit(false))
}

func storeHas(t *testing.T, kv *BadgerKV, key string) bool {
	t.Helper()
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	_, err := snapshot.Get([]byte(key))
	if errors.Is(err, ErrKeyNotFound) {
		return false
	}
	noErr(t, err, "reading %s", key)
	return true
}

// markerOnDisk reads the marker file of dir with the production reader.
func markerOnDisk(t *testing.T, dir string) vouchedMarker {
	t.Helper()
	m, found, err := readVouchedMarker(dir)
	noErr(t, err)
	isTrue(t, found, "no sync marker in %s", dir)
	return m
}

// dirState is every file name of a directory with its bytes, for "nothing was changed".
func dirState(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	state := map[string][]byte{}
	entries, err := os.ReadDir(dir)
	noErr(t, err)
	for _, entry := range entries {
		content, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		noErr(t, err)
		state[entry.Name()] = content
	}
	return state
}

func fileNames(state map[string][]byte) []string {
	names := make([]string, 0, len(state))
	for name := range state {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// TestVouchedMarkerFile is T8: the marker's 32 bytes, the refusals (a damaged marker, another
// build's format, and data files with no marker) with nothing changed, and the first start (the
// marker is on disk before Badger creates a file).
func TestVouchedMarkerFile(t *testing.T) {
	t.Run("two whole slots of one block each, the file never a hole", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "badger")
		kv, err := openVouchedTestStore(dir, vouchedTestTuning)
		noErr(t, err)
		defer func() { noErr(t, kv.Close()) }()
		path := filepath.Join(dir, vouchedFileName)
		atCreation, err := os.ReadFile(path)
		noErr(t, err)
		equal(t, newVouchedFileBytes(vouchedMarker{version: 0, floor: 1}), atCreation, "the file a first start creates")
		setDurably(t, kv, "a")
		raw, err := os.ReadFile(path)
		noErr(t, err)
		equal(t, 8192, len(raw))
		sequences := map[uint64]bool{}
		for slot := range 2 {
			block := raw[slot*4096 : (slot+1)*4096]
			equal(t, "hdbvouch", string(block[:8]), "slot %d's magic", slot)
			equal(t, uint32(2), binary.BigEndian.Uint32(block[8:12]), "slot %d's format", slot)
			sequences[binary.BigEndian.Uint64(block[12:20])] = true
			isTrue(t, binary.BigEndian.Uint64(block[28:36]) != 0, "slot %d's floor is 0", slot)
			equal(t, crc32.Checksum(block[:36], crc32.MakeTable(crc32.Castagnoli)), binary.BigEndian.Uint32(block[36:40]),
				"slot %d's checksum", slot)
			equal(t, make([]byte, 4096-40), block[40:], "slot %d's block past its 40 bytes", slot)
		}
		equal(t, map[uint64]bool{1: true, 2: true}, sequences, "the sequences after one advance")
		isTrue(t, binary.BigEndian.Uint64(raw[4096+20:4096+28]) != 0, "slot 1 holds the advance's version")
		// Review R2 items 14 and 15: a slot never shares a block or a sector with the other, and both
		// are allocated at creation, so an advance allocates nothing.
		isTrue(t, vouchedSlotPayload <= 512 && vouchedSlotSize%4096 == 0, "a slot does not fit its sector or its block")
		info, err := os.Stat(path)
		noErr(t, err)
		allocated := info.Sys().(*syscall.Stat_t).Blocks * 512
		isTrue(t, allocated >= 8192, "the marker file has holes: %d bytes allocated of 8192", allocated)
	})

	// The read rule (review R-2 item 3, R2 items 15 and 17): any state no advance of this build
	// leaves is damage, refused naming the file and the damage, with nothing changed.
	slots := func(a, b vouchedSlot) []byte { return append(a.encode(), b.encode()...) }
	slot := func(sequence, version, floor uint64) vouchedSlot {
		return vouchedSlot{vouchedMarker: vouchedMarker{version: version, floor: floor}, sequence: sequence}
	}
	damaged := []struct {
		name, damage string
		raw          func(raw []byte) []byte
	}{
		{"both slots failing their checksum", "neither of its two slots is whole: each fails its checksum",
			func(raw []byte) []byte { raw[36] ^= 0xff; raw[4096+36] ^= 0xff; return raw }},
		{"a short file", "it is 8191 bytes, not the 8192 of its two slots", func(raw []byte) []byte { return raw[:8191] }},
		{"a long file", "it is 8193 bytes, not the 8192 of its two slots", func(raw []byte) []byte { return append(raw, 0) }},
		{"an empty file", "it is 0 bytes, not the 8192 of its two slots", func([]byte) []byte { return nil }},
		{"a wrong magic in both slots", "neither of its two slots is whole: a slot does not begin with the marker's magic",
			func([]byte) []byte {
				raw := slots(slot(2, 5, 1), slot(1, 4, 1))
				for _, at := range []int{0, 4096} {
					copy(raw[at:], "hdbvouc!")
					binary.BigEndian.PutUint32(raw[at+36:], crc32.Checksum(raw[at:at+36], crc32.MakeTable(crc32.Castagnoli)))
				}
				return raw
			}},
		{"two slots of the same sequence", "its two slots carry the same sequence 7",
			func([]byte) []byte { return slots(slot(7, 9, 1), slot(7, 8, 1)) }},
		{"a newer slot vouching a lower version", "its newer slot (sequence 8) vouches version 3 with log floor 1, below its " +
			"older slot's version 9 with log floor 1", func([]byte) []byte { return slots(slot(7, 9, 1), slot(8, 3, 1)) }},
		{"a newer slot with a lower floor", "its newer slot (sequence 4) vouches version 9 with log floor 2, below its " +
			"older slot's version 9 with log floor 5", func([]byte) []byte { return slots(slot(4, 9, 2), slot(3, 9, 5)) }},
		{"a floor of 0", "its slot 1 carries the log floor 0, which no build writes",
			func([]byte) []byte { return slots(slot(4, 9, 2), slot(3, 9, 0)) }},
	}
	for _, tc := range damaged {
		t.Run("refuses "+tc.name, func(t *testing.T) {
			dir := vouchedStoreWithData(t)
			path := filepath.Join(dir, vouchedFileName)
			raw, err := os.ReadFile(path)
			noErr(t, err)
			noErr(t, os.WriteFile(path, tc.raw(raw), 0o600))
			before := dirState(t, dir)

			kv, err := openVouchedTestStore(dir, vouchedTestTuning)
			if err == nil {
				noErr(t, kv.Close())
			}
			var refusal *VouchedMarkerDamagedError
			isTrue(t, errors.As(err, &refusal), "want %T in the chain of %v", &refusal, err)
			equal(t, "the store's sync marker "+path+" is damaged ("+tc.damage+"), so this node cannot tell which of its "+
				"last writes reached the disk. Nothing was changed", err.Error())
			requireNoRingAdvice(t, err.Error())
			after := dirState(t, dir)
			equal(t, fileNames(before), fileNames(after), "the refused open changed the directory's listing")
			equal(t, before, after, "the refused open changed a file")
		})
	}

	// One whole slot is the marker: the other was being written when the process or the power
	// stopped (I-6). Two whole slots: the higher sequence, whichever slot holds it.
	for _, tc := range []struct {
		name string
		raw  []byte
		want vouchedFile
	}{
		{"slot 0 alone whole", func() []byte { raw := slots(slot(5, 9, 2), slot(6, 12, 2)); raw[4096+36] ^= 1; return raw }(),
			vouchedFile{marker: vouchedMarker{version: 9, floor: 2}, sequence: 5, slot: 0, otherNotWhole: "it fails its checksum"}},
		{"slot 1 alone whole", func() []byte { raw := slots(slot(6, 12, 2), slot(5, 9, 2)); raw[36] ^= 1; return raw }(),
			vouchedFile{marker: vouchedMarker{version: 9, floor: 2}, sequence: 5, slot: 1, otherNotWhole: "it fails its checksum"}},
		{"the higher sequence in slot 1", slots(slot(5, 9, 2), slot(6, 12, 3)),
			vouchedFile{marker: vouchedMarker{version: 12, floor: 3}, sequence: 6, slot: 1}},
		{"the higher sequence in slot 0", slots(slot(6, 12, 3), slot(5, 9, 2)),
			vouchedFile{marker: vouchedMarker{version: 12, floor: 3}, sequence: 6, slot: 0}},
	} {
		t.Run("reads "+tc.name, func(t *testing.T) {
			got, err := decodeVouchedFile("VOUCHED", tc.raw)
			noErr(t, err)
			equal(t, tc.want, got)
		})
	}

	// Another build's marker is refused as another build's, naming its format, and the start exits
	// 70 (review R-5): an earlier build's 32-byte record (format 1, every rig seed made before this
	// slice carries one), or a whole slot of a later format.
	format1 := func(t *testing.T, dir string) {
		raw := make([]byte, 32)
		copy(raw, "hdbvouch")
		binary.BigEndian.PutUint32(raw[8:], 1)
		binary.BigEndian.PutUint64(raw[12:], 9)
		binary.BigEndian.PutUint64(raw[20:], 1)
		binary.BigEndian.PutUint32(raw[28:], crc32.Checksum(raw[:28], crc32.MakeTable(crc32.Castagnoli)))
		noErr(t, os.WriteFile(filepath.Join(dir, vouchedFileName), raw, 0o600))
	}
	format3 := func(t *testing.T, dir string) {
		raw, err := os.ReadFile(filepath.Join(dir, vouchedFileName))
		noErr(t, err)
		binary.BigEndian.PutUint32(raw[4096+8:], 3)
		binary.BigEndian.PutUint32(raw[4096+36:], crc32.Checksum(raw[4096:4096+36], crc32.MakeTable(crc32.Castagnoli)))
		noErr(t, os.WriteFile(filepath.Join(dir, vouchedFileName), raw, 0o600))
	}
	for _, tc := range []struct {
		name   string
		format uint32
		write  func(t *testing.T, dir string)
	}{{"an earlier build's format-1 marker", 1, format1}, {"a slot of a later format", 3, format3}} {
		t.Run("refuses "+tc.name, func(t *testing.T) {
			dir := vouchedStoreWithData(t)
			path := filepath.Join(dir, vouchedFileName)
			tc.write(t, dir)
			before := dirState(t, dir)

			kv, err := openVouchedTestStore(dir, vouchedTestTuning)
			if err == nil {
				noErr(t, kv.Close())
			}
			var refusal *VouchedMarkerFormatError
			isTrue(t, errors.As(err, &refusal), "want %T in the chain of %v", &refusal, err)
			requireNoOtherBuild(t, err.Error())
			equal(t, fmt.Sprintf("the store's sync marker %s has format %d, and this release reads format 2 only. "+
				"Nothing was changed", path, tc.format), err.Error())
			requireNoRingAdvice(t, err.Error())
			equal(t, before, dirState(t, dir), "the refused open changed the directory")
		})
	}

	t.Run("first start: the marker is on disk before Badger opens", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "badger")
		var atOpen vouchedMarker
		var foundAtOpen, dataAtOpen bool
		var readErr error
		restore := beforeVouchedBadgerOpen
		beforeVouchedBadgerOpen = func(openDir string) {
			atOpen, foundAtOpen, readErr = readVouchedMarker(openDir)
			dataAtOpen, _ = hasStoreDataFiles(openDir)
		}
		t.Cleanup(func() { beforeVouchedBadgerOpen = restore })

		kv, err := openVouchedTestStore(dir, vouchedTestTuning)
		noErr(t, err)
		defer func() { noErr(t, kv.Close()) }()
		noErr(t, readErr)
		isTrue(t, foundAtOpen, "the marker was not on disk when badger.Open ran")
		isTrue(t, !dataAtOpen, "a data file existed before badger.Open ran")
		equal(t, vouchedMarker{version: 0, floor: 1}, atOpen)
		equal(t, VouchedOpenReport{FirstStart: true}, kv.VouchedOpenReport())
		equal(t, uint64(1), kv.VouchedFloor())
	})

	// The first-start check (review R-1's guard): a store with no marker and no data file is a
	// first start only if the caller agrees. The ring node refuses one when its Raft or its log
	// shows an earlier life (ring/badger moved aside or deleted); the check runs before the marker
	// is written, so a refusal leaves nothing behind and the next start is refused again.
	t.Run("first start: a refusing check leaves nothing and is asked again", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "badger")
		refusal := errors.New("this node has run before")
		asked := 0
		for range 2 {
			kv, err := OpenVouchedBadgerKVChecked(dir, vouchedTestTuning, vouchedTestStoreKey, func() error { asked++; return refusal })
			if err == nil {
				noErr(t, kv.Close())
			}
			isTrue(t, errors.Is(err, refusal), "the open did not return the check's refusal: %v", err)
			_, statErr := os.Stat(dir)
			isTrue(t, os.IsNotExist(statErr), "the refused first start created the store directory: %v", statErr)
		}
		equal(t, 2, asked, "the check was not asked at each start")
	})
	t.Run("first start: an agreeing check opens a new store", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "badger")
		asked := 0
		kv, err := OpenVouchedBadgerKVChecked(dir, vouchedTestTuning, vouchedTestStoreKey, func() error { asked++; return nil })
		noErr(t, err)
		equal(t, VouchedOpenReport{FirstStart: true}, kv.VouchedOpenReport())
		noErr(t, kv.Close())
		equal(t, 1, asked)
	})
	t.Run("the check is asked when the store kept its marker and lost its data files", func(t *testing.T) {
		// Review R2 F-5: a marker with no data file beside it is not a state a store that has
		// opened once reaches; the check decides it as it decides a store with no marker.
		dir := vouchedStoreWithData(t)
		entries, err := os.ReadDir(dir)
		noErr(t, err)
		for _, entry := range entries {
			if entry.Name() != vouchedFileName {
				noErr(t, os.RemoveAll(filepath.Join(dir, entry.Name())))
			}
		}
		refusal := errors.New("this node has run before")
		asked := 0
		kv, err := OpenVouchedBadgerKVChecked(dir, vouchedTestTuning, vouchedTestStoreKey, func() error { asked++; return refusal })
		if err == nil {
			noErr(t, kv.Close())
		}
		isTrue(t, errors.Is(err, refusal), "the open did not ask the check: %v", err)
		equal(t, 1, asked)
		names, err := os.ReadDir(dir)
		noErr(t, err)
		equal(t, 1, len(names), "the refused open changed the directory")
	})
	t.Run("the check is not asked when the store has data files", func(t *testing.T) {
		dir := vouchedStoreWithData(t)
		kv, err := OpenVouchedBadgerKVChecked(dir, vouchedTestTuning, vouchedTestStoreKey, func() error {
			t.Error("the first-start check was asked for a store with a marker")
			return nil
		})
		noErr(t, err)
		noErr(t, kv.Close())
	})

	// No marker is a store no build of the cut wrote (an earlier build's, or one whose marker was
	// deleted): this build writes the marker before Badger creates a file, and its directory fsync
	// keeps the marker whenever any data file is there. It is refused, naming the state and no
	// advice (the ring node adds it), and nothing is changed; there is no flag that adopts it.
	missing := []struct {
		name  string
		build func(t *testing.T) string
	}{
		{"a store an earlier build wrote", func(t *testing.T) string {
			dir := filepath.Join(t.TempDir(), "badger")
			// Without the cut, and under the test key in a run with a key: the open below judges
			// the image's key registry, and an earlier build's sealed store is refused the same.
			legacy, err := OpenBadgerKV(dir, vouchedTestTuning, vouchedTestKeyOrNil(t))
			noErr(t, err)
			setDurably(t, legacy, "synced")
			setUnsynced(t, legacy, "unsynced")
			// The image of a killed process: the logs as the page cache holds them, no clean close.
			image, err := storeimage.Take(dir)
			noErr(t, err)
			noErr(t, legacy.Close())
			imageDir := filepath.Join(t.TempDir(), "badger")
			noErr(t, storeimage.Write(imageDir, image))
			return imageDir
		}},
		{"a store whose marker was deleted", func(t *testing.T) string {
			dir := vouchedStoreWithData(t)
			noErr(t, os.Remove(filepath.Join(dir, vouchedFileName)))
			return dir
		}},
	}
	for _, tc := range missing {
		t.Run("refuses "+tc.name, func(t *testing.T) {
			dir := tc.build(t)
			before := dirState(t, dir)

			kv, err := openVouchedTestStore(dir, vouchedTestTuning)
			if err == nil {
				noErr(t, kv.Close())
			}
			var refusal *VouchedMarkerMissingError
			isTrue(t, errors.As(err, &refusal), "want %T in the chain of %v", &refusal, err)
			requireNoOtherBuild(t, err.Error())
			equal(t, "the store "+dir+" holds data files and no sync marker "+filepath.Join(dir, vouchedFileName)+
				", so this node cannot tell which of its last writes reached the disk. Nothing was changed", err.Error())
			requireNoRingAdvice(t, err.Error())
			after := dirState(t, dir)
			equal(t, fileNames(before), fileNames(after), "the refused open changed the directory's listing")
			equal(t, before, after, "the refused open changed a file")
		})
	}
}

// requireNoRingAdvice fails unless a refusal of the ring store names the state and gives no advice:
// what is safe to do depends on whether the node is a cluster member, which storage does not know,
// so the ring node adds it (cluster/wire: Node.open). A reseed is never safe advice: a cluster
// member started on an empty directory loses its Raft state, and a single node has no peers
// (store-vouched-cut review R-1).
func requireNoRingAdvice(t *testing.T, refusal string) {
	t.Helper()
	for _, banned := range []string{"reseed", "HOARDDB_SEED_NODES", "seed", "aside", "backup", "empty data directory",
		"remove-node", "replace-node", "restore this node", "Deleting"} {
		isTrue(t, !strings.Contains(refusal, banned), "the refusal says %q, advice storage cannot give: %s", banned, refusal)
	}
}

// requireNoOtherBuild fails when a refusal sends the operator to a build other than this one. No
// other build was released (owner ruling 2026-11-21), so a refusal names what it found and the fix
// that works for this release.
func requireNoOtherBuild(t *testing.T, refusal string) {
	t.Helper()
	for _, banned := range []string{"earlier build", "earlier development build", "another build", "previous release", "the build that wrote",
		"earlier version", "older version", "previous version", "older build", "earlier hoardDB", "earlier engine"} {
		if strings.Contains(refusal, banned) {
			t.Errorf("the refusal says %q, and no other build was released: %s", banned, refusal)
		}
	}
}

// vouchedStoreWithData creates a vouched store with one synced commit and closes it.
func vouchedStoreWithData(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "badger")
	kv, err := openVouchedTestStore(dir, vouchedTestTuning)
	noErr(t, err)
	setDurably(t, kv, "a")
	noErr(t, kv.Close())
	return dir
}

// TestTheCutIsOptIn is T14's S2 half: OpenBadgerKV, the legacy store's and the benches' open,
// writes no marker; routing it through the marker would put markers in every bench seed.
func TestTheCutIsOptIn(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "badger")
	kv, err := OpenBadgerKV(dir, vouchedTestTuning, nil)
	noErr(t, err)
	setDurably(t, kv, "a")
	noErr(t, kv.Sync())
	noErr(t, kv.Close())
	_, err = os.Stat(filepath.Join(dir, vouchedFileName))
	isTrue(t, os.IsNotExist(err), "OpenBadgerKV wrote a sync marker: %v", err)
	_, err = os.Stat(filepath.Join(dir, vouchedFileName+".tmp"))
	isTrue(t, os.IsNotExist(err))
}
