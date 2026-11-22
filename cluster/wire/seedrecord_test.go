package wire

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

func formatsReadyRecord() SeedRecord {
	rec := SeedRecord{WALCursor: 0x00A1B2C3D4E5F607, Ordinal: 98213}
	rec.State, rec.Phase = machine.SeedReady, machine.PhaseNone
	rec.Applied = cluster.Position{Epoch: 41, Seq: 98213}
	rec.SeedID = [16]byte{0x3F, 0x0A, 0x8C, 0x1D, 0x22, 0xB9, 0x44, 0xE7, 0xA1, 0x5C, 0x0D, 0x6E, 0x88, 0xF3, 0x1B, 0x04}
	rec.Source = "node-b"
	return rec
}

// TestSeedRecordLayoutIsConvSection12 pins the key at 21 bytes and a ready
// record's value at 77 bytes with the applied ordinal at offset 28.
func TestSeedRecordLayoutIsConvSection12(t *testing.T) {
	key := SeedRecordKey(4962, 1)
	wantKey := []byte{0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0x0F, 0x13, 0x62, 0, 0, 0, 0, 0, 0, 0, 1}
	if !bytes.Equal(key, wantKey) {
		t.Fatalf("key = %x, want %x", key, wantKey)
	}
	raw := mustEncode(t, formatsReadyRecord())
	if len(raw) != 77 {
		t.Fatalf("a ready record with a 6-character source is %d bytes, want 77", len(raw))
	}
	want := []byte{0x01, 0x02, 0x00, 0x00}
	want = append(want, 0, 0, 0, 0, 0, 0, 0, 0x29, 0, 0, 0, 0, 0, 0x01, 0x7F, 0xA5)
	want = append(want, 0x00, 0xA1, 0xB2, 0xC3, 0xD4, 0xE5, 0xF6, 0x07)
	want = append(want, 0, 0, 0, 0, 0, 0x01, 0x7F, 0xA5) // applied ordinal, 8 bytes
	want = append(want, make([]byte, 16)...)
	seedID := formatsReadyRecord().SeedID
	want = append(want, seedID[:]...)
	want = append(want, 6, 'n', 'o', 'd', 'e', '-', 'b', 0, 0)
	if !bytes.Equal(raw, want) {
		t.Fatalf("value = %x\nwant    %x", raw, want)
	}
}

// TestTheOrdinalIsInTheRecordValue decodes the ordinal back: a record loaded
// without it would restart the dedupe window at 0.
func TestTheOrdinalIsInTheRecordValue(t *testing.T) {
	got, err := DecodeSeedRecord(mustEncode(t, formatsReadyRecord()))
	if err != nil {
		t.Fatal(err)
	}
	if got.Ordinal != 98213 || got.WALCursor != 0x00A1B2C3D4E5F607 {
		t.Fatalf("ordinal %d, cursor %x lost in the round trip", got.Ordinal, got.WALCursor)
	}
}

func TestSeedingRecordCarriesItsCursor(t *testing.T) {
	rec := formatsReadyRecord()
	rec.State, rec.Phase = machine.SeedSeeding, machine.PhaseSnapshot
	rec.Cut = cluster.Position{Epoch: 41, Seq: 90000}
	rec.Cursor = bytes.Repeat([]byte{0xAB}, 300)
	got, err := DecodeSeedRecord(mustEncode(t, rec))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Cursor, rec.Cursor) || got.Cut != rec.Cut || got.Phase != rec.Phase {
		t.Fatalf("seeding record changed in the round trip: %+v", got)
	}
}

func TestDecodeRefusesWhatItCannotRepresent(t *testing.T) {
	good := mustEncode(t, formatsReadyRecord())
	cases := map[string]func([]byte) []byte{
		"empty":         func([]byte) []byte { return nil },
		"short":         func(b []byte) []byte { return b[:60] },
		"version":       func(b []byte) []byte { b[0] = 2; return b },
		"state zero":    func(b []byte) []byte { b[1] = 0; return b },
		"state five":    func(b []byte) []byte { b[1] = 5; return b },
		"phase four":    func(b []byte) []byte { b[2] = 4; return b },
		"flags":         func(b []byte) []byte { b[3] = 1; return b },
		"trailing byte": func(b []byte) []byte { return append(b, 0) },
		"source runs over": func(b []byte) []byte {
			b[68] = 200
			return b
		},
	}
	for name, mutate := range cases {
		if _, err := DecodeSeedRecord(mutate(bytes.Clone(good))); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// TestDecodeSeedRecordSurvivesMutation flips, truncates and extends valid
// values 100,000 times: nothing panics, and a value that is accepted
// re-encodes to the same bytes.
func TestDecodeSeedRecordSurvivesMutation(t *testing.T) {
	seeding := formatsReadyRecord()
	seeding.State, seeding.Phase, seeding.Cursor = machine.SeedSeeding, machine.PhaseBuffer, []byte("cursor-key")
	seeds := [][]byte{mustEncode(t, formatsReadyRecord()), mustEncode(t, seeding)}
	rng := rand.New(rand.NewSource(44))
	accepted := 0
	for i := 0; i < 100_000; i++ {
		raw := bytes.Clone(seeds[rng.Intn(len(seeds))])
		for n := rng.Intn(4) + 1; n > 0 && len(raw) > 0; n-- {
			switch rng.Intn(4) {
			case 0:
				raw[rng.Intn(len(raw))] ^= byte(1 << rng.Intn(8))
			case 1:
				raw[rng.Intn(len(raw))] = byte(rng.Intn(256))
			case 2:
				raw = raw[:rng.Intn(len(raw)+1)]
			default:
				raw = append(raw, byte(rng.Intn(256)))
			}
		}
		rec, err := DecodeSeedRecord(raw)
		if err != nil {
			continue
		}
		accepted++
		if again := mustEncode(t, rec); !bytes.Equal(again, raw) {
			t.Fatalf("accepted %x but re-encoded %x", raw, again)
		}
	}
	if accepted == 0 {
		t.Fatal("no mutated value was accepted: the test exercises only the refusal path")
	}
}

func mustEncode(t *testing.T, rec SeedRecord) []byte {
	t.Helper()
	raw, err := EncodeSeedRecord(rec)
	if err != nil {
		t.Fatalf("EncodeSeedRecord: %v", err)
	}
	return raw
}

// TestEncodeRefusesWhatItWouldTruncate: a source over 255 bytes or a cursor
// over 65,535 would be cut silently, naming a member that does not exist.
func TestEncodeRefusesWhatItWouldTruncate(t *testing.T) {
	rec := formatsReadyRecord()
	rec.Source = string(bytes.Repeat([]byte("n"), 256))
	if _, err := EncodeSeedRecord(rec); err == nil {
		t.Fatal("a 256-byte source was encoded")
	}
	rec.Source = string(bytes.Repeat([]byte("n"), 255))
	if _, err := EncodeSeedRecord(rec); err != nil {
		t.Fatalf("a 255-byte source was refused: %v", err)
	}
	rec.Source = "n1"
	rec.Cursor = make([]byte, 65536)
	if _, err := EncodeSeedRecord(rec); err == nil {
		t.Fatal("a 65,536-byte cursor was encoded")
	}
}

// TestAMemberIdOverTheSourceBoundIsRefused: the reconciler refuses a state
// whose member id could not be a record's source.
func TestAMemberIdOverTheSourceBoundIsRefused(t *testing.T) {
	st := threeNodes(t)
	long := string(bytes.Repeat([]byte("m"), 256))
	register(t, &st, long, 1)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	g := newRig(t, kv, st, testSelf, true)
	if err := g.rec.OnBoot(t.Context()); err == nil {
		t.Fatal("a 256-byte member id was accepted")
	}
	if len(g.sink.actions) != 0 {
		t.Fatalf("actions were emitted for a refused state: %d", len(g.sink.actions))
	}
}
