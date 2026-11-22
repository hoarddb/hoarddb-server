// Package wire joins the pure state machines of cluster/machine to storage,
// the clock and the network. It sits above both cluster and cluster/machine
// because machine imports cluster, so a wiring file in cluster could not name
// a machine type (build doc, Layer 4 note).
package wire

import (
	"encoding/binary"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// seedRecordVersion is byte 0 of the record (conv §12).
const seedRecordVersion = 0x01

// seedRecordFixedBytes is the record up to and including the source length
// byte: 4 header bytes, five 8-byte words, the cut, the seed id and the length.
const seedRecordFixedBytes = 4 + 8*4 + 16 + 16 + 1

// SeedRecord is the per-(slot, database) record of conv §12 as it lies on
// disk. machine.SeedRecord holds the lifecycle fields; the applied WAL cursor
// and the applied ordinal belong to the write path, not to either machine, so
// they are added here. One value per (slot, database), never per bucket.
type SeedRecord struct {
	machine.SeedRecord
	// WALCursor is the node-local sequence of the log record that produced
	// Applied; a catch-up seeks to it instead of scanning the log.
	WALCursor uint64
	// Ordinal is the pair's cumulative count of applied records, the unit the
	// dedupe window is measured in. An epoch never resets it.
	Ordinal uint64
}

// SeedRecordKey is the 21-byte key of the record for one pair:
// [0xFFFF][0x00 x 8][0x0F][slot:2][db:8].
func SeedRecordKey(slot uint16, dbID uint64) []byte {
	var pair [10]byte
	binary.BigEndian.PutUint16(pair[0:2], slot)
	binary.BigEndian.PutUint64(pair[2:10], dbID)
	return storage.NodeKey(storage.NSSeedState, pair[:])
}

// seedRecordMaxSource and seedRecordMaxCursor are the widths of the source
// length byte and the cursor length word.
const (
	seedRecordMaxSource = 255
	seedRecordMaxCursor = 65535
)

// EncodeSeedRecord writes the value of conv §12: 71 bytes for a ready record
// with no source and no cursor (a pair written here and never seeded: the
// loader's rewrite, the first write's record and the interim of seeding S0),
// 77 with a 6-character source. It refuses a source over 255 bytes
// or a cursor over 65,535, which the length fields cannot hold: cutting them
// would name a member that does not exist.
func EncodeSeedRecord(rec SeedRecord) ([]byte, error) {
	if len(rec.Source) > seedRecordMaxSource {
		return nil, fmt.Errorf("seed record: source of %d bytes does not fit its 1-byte length. Node ids are limited to %d bytes; rename the node", len(rec.Source), seedRecordMaxSource)
	}
	if len(rec.Cursor) > seedRecordMaxCursor {
		return nil, fmt.Errorf("seed record: cursor of %d bytes does not fit its 2-byte length (limit %d)", len(rec.Cursor), seedRecordMaxCursor)
	}
	out := make([]byte, 0, seedRecordFixedBytes+len(rec.Source)+2+len(rec.Cursor))
	out = append(out, seedRecordVersion, rec.State, byte(rec.Phase), 0)
	out = cluster.AppendPosition(out, rec.Applied)
	out = binary.BigEndian.AppendUint64(out, rec.WALCursor)
	out = binary.BigEndian.AppendUint64(out, rec.Ordinal)
	out = cluster.AppendPosition(out, rec.Cut)
	out = append(out, rec.SeedID[:]...)
	out = append(out, byte(len(rec.Source)))
	out = append(out, rec.Source...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(rec.Cursor)))
	return append(out, rec.Cursor...), nil
}

// DecodeSeedRecord reads a value written by EncodeSeedRecord. It refuses an
// unknown version, a state or phase outside conv §12, non-zero flags, a short
// value and trailing bytes, so an accepted value re-encodes to itself.
func DecodeSeedRecord(raw []byte) (SeedRecord, error) {
	if len(raw) < seedRecordFixedBytes+2 {
		return SeedRecord{}, fmt.Errorf("seed record: %d bytes is shorter than the %d-byte minimum", len(raw), seedRecordFixedBytes+2)
	}
	if raw[0] != seedRecordVersion {
		return SeedRecord{}, fmt.Errorf("seed record: version %d is not the supported %d", raw[0], seedRecordVersion)
	}
	if raw[1] < machine.SeedSeeding || raw[1] > machine.SeedRetiring {
		return SeedRecord{}, fmt.Errorf("seed record: state %d is not 1 seeding, 2 ready, 3 retired or 4 retiring", raw[1])
	}
	if machine.Phase(raw[2]) > machine.PhaseBuffer {
		return SeedRecord{}, fmt.Errorf("seed record: phase %d is not 0 to 3", raw[2])
	}
	if raw[3] != 0 {
		return SeedRecord{}, fmt.Errorf("seed record: flags %d are not 0", raw[3])
	}
	var rec SeedRecord
	rec.State, rec.Phase = raw[1], machine.Phase(raw[2])
	rec.Applied = cluster.Position{Epoch: binary.BigEndian.Uint64(raw[4:12]), Seq: binary.BigEndian.Uint64(raw[12:20])}
	rec.WALCursor = binary.BigEndian.Uint64(raw[20:28])
	rec.Ordinal = binary.BigEndian.Uint64(raw[28:36])
	rec.Cut = cluster.Position{Epoch: binary.BigEndian.Uint64(raw[36:44]), Seq: binary.BigEndian.Uint64(raw[44:52])}
	copy(rec.SeedID[:], raw[52:68])
	sourceEnd := 69 + int(raw[68])
	if len(raw) < sourceEnd+2 {
		return SeedRecord{}, fmt.Errorf("seed record: source of %d bytes runs past the %d-byte value", raw[68], len(raw))
	}
	rec.Source = string(raw[69:sourceEnd])
	cursorLen := int(binary.BigEndian.Uint16(raw[sourceEnd : sourceEnd+2]))
	if len(raw) != sourceEnd+2+cursorLen {
		return SeedRecord{}, fmt.Errorf("seed record: cursor of %d bytes does not fill the %d-byte value", cursorLen, len(raw))
	}
	if cursorLen > 0 {
		rec.Cursor = append([]byte(nil), raw[sourceEnd+2:]...)
	}
	return rec, nil
}
