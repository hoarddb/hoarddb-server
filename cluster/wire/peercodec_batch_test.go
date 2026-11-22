package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"runtime"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
)

func TestWriteAckBatchBodyRoundTrips(t *testing.T) {
	acks := []WriteAck{
		{Slot: 3, DB: 7, Pos: cluster.Position{Epoch: 2, Seq: 41}, Outcome: AckNotApplied},
		{Slot: 3, DB: 7, Pos: cluster.Position{Epoch: 2, Seq: 42}, Outcome: AckApplied},
		{Slot: 0xFFFF, DB: 1 << 63, Pos: cluster.Position{Epoch: 9, Seq: 1 << 40}, Outcome: AckRefused},
	}
	body, err := EncodeWriteAckBatchBody(acks)
	if err != nil {
		t.Fatalf("encoding three acks: %v", err)
	}
	if want := 2 + 3*ackBatchEntryBytes; len(body) != want {
		t.Fatalf("body is %d bytes, want %d", len(body), want)
	}
	back, err := DecodeWriteAckBatchBody(body)
	if err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if len(back) != len(acks) {
		t.Fatalf("decoded %d acks, want %d", len(back), len(acks))
	}
	for i := range acks {
		if back[i] != acks[i] {
			t.Fatalf("ack %d decoded as %+v, want %+v", i, back[i], acks[i])
		}
	}
	again, err := EncodeWriteAckBatchBody(back)
	if err != nil || !bytes.Equal(again, body) {
		t.Fatalf("re-encode = % x, %v; want the same bytes % x", again, err, body)
	}
}

func TestWriteAckBatchBodyRefusesWhatItCannotRead(t *testing.T) {
	one := []WriteAck{{Slot: 1, DB: 2, Pos: cluster.Position{Epoch: 1, Seq: 1}, Outcome: AckApplied}}
	if _, err := EncodeWriteAckBatchBody(nil); !errors.Is(err, errPeerBody) {
		t.Fatalf("an empty batch encoded: %v", err)
	}
	if _, err := EncodeWriteAckBatchBody(make([]WriteAck, maxBatchEntries+1)); !errors.Is(err, errPeerBody) {
		t.Fatalf("a batch of %d entries encoded: %v", maxBatchEntries+1, err)
	}

	good, err := EncodeWriteAckBatchBody(one)
	if err != nil {
		t.Fatalf("encoding one ack: %v", err)
	}
	entry := good[2:]
	overCap := make([]byte, 2+(maxBatchEntries+1)*ackBatchEntryBytes)
	binary.BigEndian.PutUint16(overCap, maxBatchEntries+1)
	badOutcome := bytes.Clone(good)
	badOutcome[len(badOutcome)-1] = byte(AckRefused) + 1
	cases := []struct {
		name string
		body []byte
	}{
		{"no count", []byte{0}},
		{"zero entries", []byte{0, 0}},
		{"257 entries with their bytes", overCap},
		{"256 entries with one entry's bytes", append([]byte{0x01, 0x00}, entry...)},
		{"two entries with one entry's bytes", append([]byte{0x00, 0x02}, entry...)},
		{"a trailing byte", append(bytes.Clone(good), 0)},
		{"a truncated entry", good[:len(good)-1]},
		{"outcome above refused", badOutcome},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := DecodeWriteAckBatchBody(c.body); !errors.Is(err, errPeerBody) {
				t.Fatalf("decoded: %v", err)
			}
		})
	}
}

func TestWriteBatchBodyRoundTrips(t *testing.T) {
	entries := []WriteEntry{
		{Slot: 3, DB: 7, PairRecord: PairRecord{Pos: cluster.Position{Epoch: 2, Seq: 41}, Cursor: 900,
			Record: storage.WALRecord{Class: 1, OpIndex: 2, RequestID: [16]byte{5}, Body: []byte("first")}}},
		{Slot: 3, DB: 7, PairRecord: PairRecord{Pos: cluster.Position{Epoch: 2, Seq: 42}, Cursor: 901,
			Record: storage.WALRecord{Class: 1, RequestID: [16]byte{6}}}},
		{Slot: 0xFFFF, DB: 1 << 63, PairRecord: PairRecord{Pos: cluster.Position{Epoch: 9, Seq: 1 << 40}, Cursor: 1 << 50,
			Record: storage.WALRecord{Class: 2, OpIndex: 1, RequestID: [16]byte{7}, Body: bytes.Repeat([]byte{0xAB}, 1024)}}},
	}
	body, err := EncodeWriteBatchBody(entries)
	if err != nil {
		t.Fatalf("encoding three entries of two pairs: %v", err)
	}
	back, err := DecodeWriteBatchBody(body)
	if err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if !reflect.DeepEqual(back, entries) {
		t.Fatalf("decoded %+v, want %+v", back, entries)
	}
	again, err := EncodeWriteBatchBody(back)
	if err != nil || !bytes.Equal(again, body) {
		t.Fatalf("re-encode = % x, %v; want the same bytes % x", again, err, body)
	}
}

func TestWriteBatchBodyRefusesWhatItCannotRead(t *testing.T) {
	one := []WriteEntry{{Slot: 1, DB: 2, PairRecord: PairRecord{Pos: cluster.Position{Epoch: 1, Seq: 1}, Cursor: 1,
		Record: storage.WALRecord{Class: 1, Body: []byte("abc")}}}}
	if _, err := EncodeWriteBatchBody(nil); !errors.Is(err, errPeerBody) {
		t.Fatalf("an empty WRITE_BATCH encoded: %v", err)
	}
	if _, err := EncodeWriteBatchBody(make([]WriteEntry, maxBatchEntries+1)); !errors.Is(err, errPeerBody) {
		t.Fatalf("a WRITE_BATCH of %d entries encoded: %v", maxBatchEntries+1, err)
	}

	good, err := EncodeWriteBatchBody(one)
	if err != nil {
		t.Fatalf("encoding one entry: %v", err)
	}
	entry := good[2:]
	overCap := make([]byte, 2+(maxBatchEntries+1)*writeBatchEntryFixed)
	binary.BigEndian.PutUint16(overCap, maxBatchEntries+1)
	// The record's body length sits after the entry's slot, database, position, cursor, operation and request id.
	lengthAt := 2 + 2 + 8 + 16 + 8 + 1 + 1 + 16
	longBody := bytes.Clone(good)
	binary.BigEndian.PutUint32(longBody[lengthAt:], 0xFFFF)
	// A count of 256 with no bytes behind it is refused before a 256-entry slice is sized from it (about 22 KiB).
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if _, err := DecodeWriteBatchBody([]byte{0x01, 0x00}); !errors.Is(err, errPeerBody) {
		t.Fatalf("256 entries with no bytes behind them decoded: %v", err)
	}
	runtime.ReadMemStats(&after)
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 8<<10 {
		t.Fatalf("refusing a 256-entry count allocated %d bytes; the count must be checked against the bytes before anything is sized from it", grew)
	}
	cases := []struct {
		name string
		body []byte
	}{
		{"no count", []byte{0}},
		{"zero entries", []byte{0, 0}},
		{"257 entries with their bytes", overCap},
		{"256 entries with one entry's bytes", append([]byte{0x01, 0x00}, entry...)},
		{"two entries with one entry's bytes", append([]byte{0x00, 0x02}, entry...)},
		{"a record body longer than the bytes left", longBody},
		{"a trailing byte", append(bytes.Clone(good), 0)},
		{"a truncated record", good[:len(good)-1]},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := DecodeWriteBatchBody(c.body); !errors.Is(err, errPeerBody) {
				t.Fatalf("decoded: %v", err)
			}
		})
	}
}
