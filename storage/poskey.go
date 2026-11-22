package storage

import (
	"encoding/binary"
	"fmt"
)

// PositionKeyLen is the size of a retained-log key: the slot, the database id,
// the epoch and the seq (conv §9).
const PositionKeyLen = 2 + 8 + 8 + 8

// LogPosition is the epoch and seq a record was assigned in one (slot,
// database) pair's log. It mirrors cluster.Position field for field, because
// storage must not import cluster; a caller converts with
// storage.LogPosition(pos), which the compiler accepts only while the two
// structs stay identical.
type LogPosition struct {
	Epoch, Seq uint64
}

// PositionKey returns the 26-byte key a record is appended under:
// [slot:2][database id:8][epoch:8][seq:8], every field big-endian. The epoch is
// part of the key because seq restarts at 1 in each epoch, so the bare seq
// would name two different records (conv §9). Big-endian makes a bytewise
// compare of two keys of one pair agree with position order.
func PositionKey(slot uint16, db uint64, pos LogPosition) []byte {
	key := make([]byte, 0, PositionKeyLen)
	key = binary.BigEndian.AppendUint16(key, slot)
	key = binary.BigEndian.AppendUint64(key, db)
	key = binary.BigEndian.AppendUint64(key, pos.Epoch)
	return binary.BigEndian.AppendUint64(key, pos.Seq)
}

// DecodePositionKey reads the key written by PositionKey. Any other length is
// refused: a key of another length is not a retained-log key, and reading its
// first 26 bytes would name a different record.
func DecodePositionKey(key []byte) (slot uint16, db uint64, pos LogPosition, err error) {
	if len(key) != PositionKeyLen {
		return 0, 0, LogPosition{}, fmt.Errorf("retained-log key: want %d bytes (slot, database id, epoch, seq), got %d", PositionKeyLen, len(key))
	}
	return binary.BigEndian.Uint16(key[0:2]),
		binary.BigEndian.Uint64(key[2:10]),
		LogPosition{Epoch: binary.BigEndian.Uint64(key[10:18]), Seq: binary.BigEndian.Uint64(key[18:26])},
		nil
}
