package cluster

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math/rand"
	"testing"
)

// snapshotWantHeader is the 28 bytes of membership Formats 3 for the minimal
// state at epoch 1, state_seq 1: magic, version 1, flags 1, two zeros, the two
// counters and the 537-byte body length (516 with joined_state_seq, plus 21 for cluster_version).
var snapshotWantHeader = []byte{
	0x48, 0x44, 0x43, 0x53, 0x01, 0x01, 0x00, 0x00,
	0, 0, 0, 0, 0, 0, 0, 1,
	0, 0, 0, 0, 0, 0, 0, 1,
	0x00, 0x00, 0x02, 0x61,
}

func TestSnapshotFileLayout(t *testing.T) {
	raw, err := EncodeSnapshot(stateMinimal())
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 641 {
		t.Fatalf("snapshot is %d bytes, membership Formats 3 says 548 with joined_state_seq, cluster_version adds 21 and the four empty user fields 72", len(raw))
	}
	if !bytes.Equal(raw[:28], snapshotWantHeader) {
		t.Fatalf("header\n got %x\nwant %x", raw[:28], snapshotWantHeader)
	}
	body := stateMustEncode(t, stateMinimal())
	if !bytes.Equal(raw[28:637], body) {
		t.Fatal("the body is not the ClusterState document of formats section 2, byte for byte")
	}
	castagnoli := crc32.MakeTable(crc32.Castagnoli)
	// The checksum covers [0, 28+n): the header and the body, not itself.
	// A checksum over the body alone would leave the counters unprotected.
	if got, want := binary.BigEndian.Uint32(raw[637:]), crc32.Checksum(raw[:637], castagnoli); got != want {
		t.Fatalf("CRC-32C is %08x, want %08x over bytes [0, 637)", got, want)
	}
	if binary.BigEndian.Uint32(raw[637:]) == crc32.Checksum(raw[28:637], castagnoli) {
		t.Fatal("the CRC covers the body only: the header counters are unprotected")
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	for _, st := range []ClusterState{stateMinimal(), stateFull()} {
		raw, err := EncodeSnapshot(st)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeSnapshot(raw)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		again, err := EncodeSnapshot(got)
		if err != nil || !bytes.Equal(again, raw) {
			t.Fatalf("round trip changed the bytes: %v", err)
		}
	}
}

func TestSnapshotRefusesWhatItCannotTrust(t *testing.T) {
	good, err := EncodeSnapshot(stateMinimal())
	if err != nil {
		t.Fatal(err)
	}
	refreshCRC := func(raw []byte) []byte {
		binary.BigEndian.PutUint32(raw[len(raw)-4:], crc32.Checksum(raw[:len(raw)-4], crc32.MakeTable(crc32.Castagnoli)))
		return raw
	}
	cases := map[string]func() []byte{
		"empty":             func() []byte { return nil },
		"truncated":         func() []byte { return good[:521] },
		"trailing byte":     func() []byte { return append(bytes.Clone(good), 0) },
		"wrong magic":       func() []byte { b := bytes.Clone(good); b[0] = 'X'; return refreshCRC(b) },
		"version 2":         func() []byte { b := bytes.Clone(good); b[4] = 2; return refreshCRC(b) },
		"flags 0":           func() []byte { b := bytes.Clone(good); b[5] = 0; return refreshCRC(b) },
		"reserved flag bit": func() []byte { b := bytes.Clone(good); b[5] = 3; return refreshCRC(b) },
		"reserved zeros":    func() []byte { b := bytes.Clone(good); b[7] = 1; return refreshCRC(b) },
		"epoch counter":     func() []byte { b := bytes.Clone(good); b[15] = 9; return refreshCRC(b) },
		"state_seq counter": func() []byte { b := bytes.Clone(good); b[23] = 9; return refreshCRC(b) },
		"body length":       func() []byte { b := bytes.Clone(good); b[27] = 0xEB; return refreshCRC(b) },
		"body flipped":      func() []byte { b := bytes.Clone(good); b[100] ^= 1; return b },
		"header flipped, crc stale": func() []byte {
			b := bytes.Clone(good)
			b[15] = 9
			return b
		},
		"crc flipped": func() []byte { b := bytes.Clone(good); b[521] ^= 1; return b },
	}
	for name, build := range cases {
		if _, err := DecodeSnapshot(build()); err == nil {
			t.Errorf("%s: a snapshot the node cannot trust was accepted", name)
		}
	}
}

// TestSnapshotDecoderMutations flips and splices 20,000 snapshots: the decoder
// never panics, and what it accepts re-encodes to the same bytes.
func TestSnapshotDecoderMutations(t *testing.T) {
	seeds := make([][]byte, 0, 2)
	for _, st := range []ClusterState{stateMinimal(), stateFull()} {
		raw, err := EncodeSnapshot(st)
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, raw)
	}
	rng := rand.New(rand.NewSource(21))
	for i := 0; i < 20000; i++ {
		raw := bytes.Clone(seeds[i%len(seeds)])
		for j := rng.Intn(4); j >= 0; j-- {
			switch rng.Intn(4) {
			case 0:
				raw[rng.Intn(len(raw))] ^= byte(1 << rng.Intn(8))
			case 1:
				raw = raw[:rng.Intn(len(raw)+1)]
			case 2:
				at := rng.Intn(len(raw) + 1)
				raw = append(raw[:at], append([]byte{byte(rng.Intn(256))}, raw[at:]...)...)
			case 3:
				if len(raw) > 0 {
					raw[rng.Intn(len(raw))] = byte(rng.Intn(256))
				}
			}
			if len(raw) == 0 {
				break
			}
		}
		st, err := DecodeSnapshot(raw)
		if err != nil {
			continue
		}
		again, err := EncodeSnapshot(st)
		if err != nil || !bytes.Equal(again, raw) {
			t.Fatalf("mutation %d was accepted but does not re-encode to itself: %v", i, err)
		}
	}
}
