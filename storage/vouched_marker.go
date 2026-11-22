package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

// vouchedFileName is the ring store's sync marker, in the store directory so it moves with
// the store's files (store-vouched-cut-spec.md §3.2). Badger ignores names it does not know.
const vouchedFileName = "VOUCHED"

// The marker file, format 2 (slice S-3, BUG-0860 option (c)): two slots, each one filesystem
// block, written in place. A slot's 40 bytes, big-endian: the magic, the format, the slot's
// sequence, the vouched version, the log floor, and the CRC-32C of those 36 bytes; the rest of
// the block is zeros. The slots are at offsets 0 and 4096, so they never share a 4 KiB block
// nor a 512-byte sector, and a slot's bytes fit in its first sector. The file is created whole,
// both slots holding real bytes, and its size never changes after that. Format 1 was one
// 32-byte record replaced by rename, which cost two fsyncs, a rename and a directory fsync per
// advance (7.6 ms quiet, 40 ms and more on a loaded VM; the diagnosis of 2026-11-19).
const (
	vouchedMagic       = "hdbvouch"
	vouchedFormat      = 2
	vouchedSlotSize    = 4096
	vouchedFileSize    = 2 * vouchedSlotSize
	vouchedSlotPayload = 40
	// vouchedFormat1Size is format 1's whole file: an earlier build's marker, refused as another
	// build's (exit 70), never read as damage.
	vouchedFormat1Size = 32
)

var vouchedChecksumTable = crc32.MakeTable(crc32.Castagnoli)

// writeMarkerSlot and syncMarkerFile are the two calls of an in-place advance: the slot's
// write at its offset and the file's fdatasync. Variables so a test can count them, tear the
// write and fail either.
var (
	writeMarkerSlot = func(file *os.File, slot []byte, offset int64) error {
		_, err := file.WriteAt(slot, offset)
		return err
	}
	syncMarkerFile = fdatasyncFile
)

// vouchedMarker is what the marker holds: the highest Badger commit version a reported sync
// made durable, and the log floor, a sequence of the node's own log at or below every record
// the boot replay may still owe (§3.6). The floor is never 0.
type vouchedMarker struct{ version, floor uint64 }

// vouchedSlot is one slot: a marker and the slot's sequence, which grows by one per advance.
type vouchedSlot struct {
	vouchedMarker
	sequence uint64
}

// encode returns the slot's whole block: its 40 bytes, then zeros.
func (s vouchedSlot) encode() []byte {
	encoded := make([]byte, vouchedSlotSize)
	copy(encoded, vouchedMagic)
	binary.BigEndian.PutUint32(encoded[8:], vouchedFormat)
	binary.BigEndian.PutUint64(encoded[12:], s.sequence)
	binary.BigEndian.PutUint64(encoded[20:], s.version)
	binary.BigEndian.PutUint64(encoded[28:], s.floor)
	binary.BigEndian.PutUint32(encoded[36:], crc32.Checksum(encoded[:36], vouchedChecksumTable))
	return encoded
}

// vouchedFile is the marker file as an open reads it: the marker the open uses, the sequence
// of its slot, and which slot holds it. The next advance writes the other slot. otherNotWhole is
// what is wrong with the other slot when it is not whole, empty when both are.
type vouchedFile struct {
	marker        vouchedMarker
	sequence      uint64
	slot          int
	otherNotWhole string
}

// newVouchedFileBytes is a new marker file holding m: slot 0 at sequence 1, slot 1 a copy at
// sequence 0, so both slots are whole from the start and the first advance writes slot 1.
func newVouchedFileBytes(m vouchedMarker) []byte {
	return append(vouchedSlot{m, 1}.encode(), vouchedSlot{m, 0}.encode()...)
}

// VouchedMarkerDamagedError refuses to open a ring store whose sync marker cannot be read as a
// marker: no slot is whole, the two slots contradict each other, or the file has another size.
// Nothing was changed. Its text names the damage only: what to do depends on whether the node is
// a cluster member, which the ring node adds (cluster/wire: Node.open).
type VouchedMarkerDamagedError struct {
	Path   string // the marker file
	Damage string // what is wrong with it
}

func (e *VouchedMarkerDamagedError) Error() string {
	return fmt.Sprintf("the store's sync marker %s is damaged (%s), so this node cannot tell which of its last writes "+
		"reached the disk. Nothing was changed", e.Path, e.Damage)
}

// VouchedMarkerFormatError refuses to open a ring store whose sync marker is whole and of a
// format this release does not read. Nothing was changed. No other build was released, so the
// text names the format it found and the one it reads and no build (owner ruling 2026-11-21);
// the start exits 70 as for a ring directory of another format (review R-5), and the advice
// for the node is the ring node's to add.
type VouchedMarkerFormatError struct {
	Path   string // the marker file
	Format uint32 // the format the marker carries
}

func (e *VouchedMarkerFormatError) Error() string {
	return fmt.Sprintf("the store's sync marker %s has format %d, and this release reads format %d only. "+
		"Nothing was changed", e.Path, e.Format, vouchedFormat)
}

// VouchedMarkerMissingError refuses to open a ring store that holds data files and no sync
// marker: the marker was deleted, or the files were put there by something else. This build writes
// the marker before Badger creates a file, so no crash of it leaves that state. Nothing was changed. Like the
// other marker refusals it carries no advice of its own.
type VouchedMarkerMissingError struct {
	Dir string // the store directory
}

func (e *VouchedMarkerMissingError) Error() string {
	return fmt.Sprintf("the store %s holds data files and no sync marker %s, so this node cannot tell which of its "+
		"last writes reached the disk. Nothing was changed", e.Dir, filepath.Join(e.Dir, vouchedFileName))
}

// ReadVouchedMarker reads the sync marker of the ring store in dir as the open reads it, and
// returns the version and log floor the open would cut at. found is false when there is no
// marker; err is one of the open's refusals (a damaged marker, one of another format) or the read's
// failure. The cluster harness checks a live copy with it.
func ReadVouchedMarker(dir string) (version, floor uint64, found bool, err error) {
	file, found, err := readVouchedFile(dir)
	return file.marker.version, file.marker.floor, found, err
}

// readVouchedMarker reads the marker the open of the store in dir would use (readVouchedFile).
func readVouchedMarker(dir string) (m vouchedMarker, found bool, err error) {
	file, found, err := readVouchedFile(dir)
	return file.marker, found, err
}

// readVouchedFile reads the marker file of the store in dir. found is false when there is none;
// err is one of the two refusals, or the read's own failure.
func readVouchedFile(dir string) (file vouchedFile, found bool, err error) {
	path := filepath.Join(dir, vouchedFileName)
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return vouchedFile{}, false, nil
	}
	if err != nil {
		return vouchedFile{}, false, fmt.Errorf("reading the store's sync marker %s: %w", path, err)
	}
	file, err = decodeVouchedFile(path, raw)
	if err != nil {
		return vouchedFile{}, false, err
	}
	return file, true, nil
}

// decodeVouchedFile applies the read rule (review R-2 items 3 and 15, R2 item 17). A whole
// format-1 record is another build's. Any other size than two slots is damage, never extended or
// truncated. A slot is whole when its magic and its checksum match, and a whole slot of another
// format is another build's. No whole slot is damage: one advance writes one slot, so no crash of
// this build tears both. With one whole slot the open uses it: the other was being written when
// the process or the power stopped, and vouches nothing (I-6), or the disk damaged it, which reads
// the same; the open warns (warnOneWholeSlot) and the result names what the other slot is. With two, the higher sequence wins;
// equal sequences, or a newer slot that vouches less than the older one, are damage, since no
// advance writes either. A whole slot whose floor is 0 is damage: no build writes one.
func decodeVouchedFile(path string, raw []byte) (vouchedFile, error) {
	damaged := func(damage string) (vouchedFile, error) {
		return vouchedFile{}, &VouchedMarkerDamagedError{Path: path, Damage: damage}
	}
	if len(raw) == vouchedFormat1Size && bytes.Equal(raw[:8], []byte(vouchedMagic)) &&
		crc32.Checksum(raw[:28], vouchedChecksumTable) == binary.BigEndian.Uint32(raw[28:]) {
		if format := binary.BigEndian.Uint32(raw[8:]); format != vouchedFormat {
			return vouchedFile{}, &VouchedMarkerFormatError{Path: path, Format: format}
		}
	}
	if len(raw) != vouchedFileSize {
		return damaged(fmt.Sprintf("it is %d bytes, not the %d of its two slots", len(raw), vouchedFileSize))
	}
	var whole []vouchedFile
	var notWhole [2]string
	checksumsFailed := 0
	for slot := range 2 {
		payload := raw[slot*vouchedSlotSize:][:vouchedSlotPayload]
		if crc32.Checksum(payload[:36], vouchedChecksumTable) != binary.BigEndian.Uint32(payload[36:]) {
			checksumsFailed++
			notWhole[slot] = "it fails its checksum"
			continue
		}
		if !bytes.Equal(payload[:8], []byte(vouchedMagic)) {
			notWhole[slot] = "it does not begin with the marker's magic"
			continue
		}
		if format := binary.BigEndian.Uint32(payload[8:]); format != vouchedFormat {
			return vouchedFile{}, &VouchedMarkerFormatError{Path: path, Format: format}
		}
		read := vouchedFile{
			marker: vouchedMarker{
				version: binary.BigEndian.Uint64(payload[20:]),
				floor:   binary.BigEndian.Uint64(payload[28:]),
			},
			sequence: binary.BigEndian.Uint64(payload[12:]),
			slot:     slot,
		}
		if read.marker.floor == 0 {
			return damaged(fmt.Sprintf("its slot %d carries the log floor 0, which no build writes", slot))
		}
		whole = append(whole, read)
	}
	switch {
	case len(whole) == 0 && checksumsFailed == 2:
		return damaged("neither of its two slots is whole: each fails its checksum")
	case len(whole) == 0:
		return damaged("neither of its two slots is whole: a slot does not begin with the marker's magic")
	case len(whole) == 1:
		only := whole[0]
		only.otherNotWhole = notWhole[1-only.slot]
		return only, nil
	}
	newer, older := whole[0], whole[1]
	if older.sequence > newer.sequence {
		newer, older = older, newer
	}
	if newer.sequence == older.sequence {
		return damaged(fmt.Sprintf("its two slots carry the same sequence %d", newer.sequence))
	}
	if newer.marker.version < older.marker.version || newer.marker.floor < older.marker.floor {
		return damaged(fmt.Sprintf("its newer slot (sequence %d) vouches version %d with log floor %d, below its older "+
			"slot's version %d with log floor %d", newer.sequence, newer.marker.version, newer.marker.floor,
			older.marker.version, older.marker.floor))
	}
	return newer, nil
}

// writeVouchedMarker creates the marker file of the store in dir holding m, whole, by
// WriteFileDurably (a temporary file, its fsync, the rename, the directory's fsync), so the file
// and its directory entry are durable before the caller goes on (review R-2 item 2). Only a first
// start creates it; every later advance writes a slot in place (BadgerKV.advanceMarker).
func writeVouchedMarker(dir string, m vouchedMarker) error {
	if err := WriteFileDurably(filepath.Join(dir, vouchedFileName), newVouchedFileBytes(m)); err != nil {
		return fmt.Errorf("creating the store's sync marker (version %d, log floor %d): %w", m.version, m.floor, err)
	}
	return nil
}
