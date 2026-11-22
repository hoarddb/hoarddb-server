package storage

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

// TestPositionKeyIsTwentySixBytes pins conv §9's key: slot, database id, epoch
// and seq, 26 bytes. The epoch is part of it because seq restarts at 1 in each
// epoch; a key without it names two records.
func TestPositionKeyIsTwentySixBytes(t *testing.T) {
	key := PositionKey(0x1fff, 41, LogPosition{Epoch: 42, Seq: 55401})
	if len(key) != 26 {
		t.Fatalf("a position key is %d bytes, want 26", len(key))
	}
	if got := binary.BigEndian.Uint16(key[0:2]); got != 0x1fff {
		t.Errorf("bytes 0-1 hold slot %#x, want 0x1fff", got)
	}
	if got := binary.BigEndian.Uint64(key[2:10]); got != 41 {
		t.Errorf("bytes 2-9 hold database id %d, want 41", got)
	}
	if got := binary.BigEndian.Uint64(key[10:18]); got != 42 {
		t.Errorf("bytes 10-17 hold epoch %d, want 42", got)
	}
	if got := binary.BigEndian.Uint64(key[18:26]); got != 55401 {
		t.Errorf("bytes 18-25 hold seq %d, want 55401", got)
	}

	sameSeq := PositionKey(0x1fff, 41, LogPosition{Epoch: 43, Seq: 55401})
	if bytes.Equal(key, sameSeq) {
		t.Error("the same seq in two epochs gave one key: the epoch is not in it")
	}
}

func TestPositionKeysOrderLikePositions(t *testing.T) {
	ordered := []LogPosition{{0, 1}, {0, 999}, {1, 1}, {1, 2}, {300, 1}}
	for i := 1; i < len(ordered); i++ {
		earlier := PositionKey(7, 3, ordered[i-1])
		later := PositionKey(7, 3, ordered[i])
		if bytes.Compare(earlier, later) >= 0 {
			t.Errorf("key of %v does not sort before key of %v", ordered[i-1], ordered[i])
		}
	}
}

func TestPositionKeyRoundTripsAndRefusesOtherLengths(t *testing.T) {
	want := LogPosition{Epoch: 9, Seq: 8}
	slot, db, pos, err := DecodePositionKey(PositionKey(5, 6, want))
	if err != nil || slot != 5 || db != 6 || pos != want {
		t.Fatalf("round trip gave slot %d db %d pos %v err %v", slot, db, pos, err)
	}
	for _, n := range []int{0, 16, 25, 27} {
		if _, _, _, err := DecodePositionKey(make([]byte, n)); err == nil {
			t.Errorf("a %d-byte key was accepted", n)
		}
	}
}

// TestPositionKeyOrderIsTupleOrder compares 10,000 random pairs of
// (slot, db, epoch, seq) tuples: the bytes sort as the tuples do. Fields are
// drawn from a small range half the time so that ties on the leading fields,
// which decide on the later ones, occur.
func TestPositionKeyOrderIsTupleOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(26))
	draw := func() (uint16, uint64, LogPosition) {
		if rng.Intn(2) == 0 {
			return uint16(rng.Intn(3)), uint64(rng.Intn(3)), LogPosition{Epoch: uint64(rng.Intn(3)), Seq: uint64(rng.Intn(3))}
		}
		return uint16(rng.Uint32()), rng.Uint64(), LogPosition{Epoch: rng.Uint64(), Seq: rng.Uint64()}
	}
	cmp := func(a, b uint64) int {
		switch {
		case a < b:
			return -1
		case a > b:
			return 1
		}
		return 0
	}
	for i := 0; i < 10000; i++ {
		slotA, dbA, posA := draw()
		slotB, dbB, posB := draw()
		want := cmp(uint64(slotA), uint64(slotB))
		for _, next := range []int{cmp(dbA, dbB), cmp(posA.Epoch, posB.Epoch), cmp(posA.Seq, posB.Seq)} {
			if want == 0 {
				want = next
			}
		}
		if got := bytes.Compare(PositionKey(slotA, dbA, posA), PositionKey(slotB, dbB, posB)); got != want {
			t.Fatalf("(%d,%d,%v) vs (%d,%d,%v): bytes compare %d, tuple compare %d", slotA, dbA, posA, slotB, dbB, posB, got, want)
		}
	}
}

// TestDecodePositionKeyMutations flips, drops and inserts bytes of valid keys
// 100,000 times: the decoder never panics, and an accepted key re-encodes to
// the same bytes.
func TestDecodePositionKeyMutations(t *testing.T) {
	rng := rand.New(rand.NewSource(27))
	for i := 0; i < 100000; i++ {
		key := PositionKey(uint16(rng.Uint32()), rng.Uint64(), LogPosition{Epoch: rng.Uint64(), Seq: rng.Uint64()})
		switch rng.Intn(3) {
		case 0:
			key[rng.Intn(len(key))] ^= byte(1 + rng.Intn(255))
		case 1:
			key = key[:rng.Intn(len(key)+1)]
		default:
			at := rng.Intn(len(key) + 1)
			key = append(key[:at], append([]byte{byte(rng.Intn(256))}, key[at:]...)...)
		}
		slot, db, pos, err := DecodePositionKey(key)
		if err != nil {
			continue
		}
		if again := PositionKey(slot, db, pos); !bytes.Equal(again, key) {
			t.Fatalf("accepted %x re-encodes to %x", key, again)
		}
	}
}

func TestPositionKeyOfWrongLengthIsRefusedNamingBothLengths(t *testing.T) {
	for _, n := range []int{25, 27} {
		_, _, _, err := DecodePositionKey(make([]byte, n))
		if err == nil || !strings.Contains(err.Error(), "26") || !strings.Contains(err.Error(), "got "+strconv.Itoa(n)) {
			t.Errorf("a %d-byte key gave %v, want an error naming 26 and %d", n, err, n)
		}
	}
}
