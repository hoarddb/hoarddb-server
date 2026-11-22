package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// Storage-format slice A, unit F2: the dedupe entry's value is format 0x03 with canonical
// varints, the 36-byte key is unchanged and exact, the result bound survives.

func TestDedupeValueIsFormatThreeAndTheKeyIsExact(t *testing.T) {
	rig := newU31Rig(t, 8192)
	shared := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	var idA, idB [16]byte
	copy(idA[:], shared[:])
	copy(idB[:], shared[:])
	idA[15], idB[15] = 0xA, 0xB
	popA := func() WriteRequest {
		return WriteRequest{Slot: testSlot, DBID: testDB, Op: u31PopOp("e1"), RequestID: idA, Deadline: time.Second}
	}
	popB := func() WriteRequest {
		return WriteRequest{Slot: testSlot, DBID: testDB, Op: u31PopOp("e2"), RequestID: idB, Deadline: time.Second}
	}
	first, err := rig.w.Write(context.Background(), popA())
	if err != nil || first.Deduped {
		t.Fatalf("pop A = %+v, %v", first, err)
	}
	second, err := rig.w.Write(context.Background(), popB())
	if err != nil || second.Deduped {
		t.Fatalf("pop B = %+v, %v", second, err)
	}

	for _, id := range [][16]byte{idA, idB} {
		key := DedupeEntryKey(storage.BucketRef{Slot: testSlot, DatabaseID: testDB, BucketID: testBucket}, machine.DedupeKey{RequestID: id})
		if len(key) != 36 || !bytes.Equal(key[19:35], id[:]) {
			t.Fatalf("key %x: want 36 bytes with the whole request id %x at [19:35]", key, id)
		}
		snapshot := rig.kv.NewSnapshot()
		value, err := snapshot.Get(key)
		snapshot.Discard()
		if err != nil {
			t.Fatalf("the entry of %x is not stored under its exact key: %v", id, err)
		}
		if value[0] != 0x03 {
			t.Fatalf("the entry of %x is format %#02x, want 0x03", id, value[0])
		}
	}

	retryA, err := rig.w.Write(context.Background(), popA())
	if err != nil || !retryA.Deduped || !bytes.Equal(retryA.Result, first.Result) || retryA.Pos != first.Pos {
		t.Fatalf("retry A = %+v, %v; want A's own answer %+v", retryA, err, first)
	}
	retryB, err := rig.w.Write(context.Background(), popB())
	if err != nil || !retryB.Deduped || !bytes.Equal(retryB.Result, second.Result) || retryB.Pos != second.Pos {
		t.Fatalf("retry B = %+v, %v; want B's own answer %+v", retryB, err, second)
	}
	if got := rig.applied.Load(); got != 2 {
		t.Fatalf("the two pops were applied %d times, want 2: two ids sharing eight bytes keep two live entries", got)
	}
}

// dedupeGolden is the 19-byte value of ordinal 5,000,000, epoch 42, seq 3,000,000,
// fingerprint 0x0102030405060708 and no result (spec §2.1's typical early-life entry).
var dedupeGolden = []byte{
	0x03,
	0xC0, 0x96, 0xB1, 0x02, // ordinal 5,000,000
	0x2A,                   // epoch 42
	0xC0, 0x8D, 0xB7, 0x01, // seq 3,000,000
	0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
	0x00, // resultLen 0
}

func dedupeGoldenEntry() (storage.BucketRef, machine.DedupeEntry) {
	return storage.BucketRef{Slot: 5, DatabaseID: 3, BucketID: 41}, machine.DedupeEntry{
		Key: machine.DedupeKey{RequestID: [16]byte{1, 2, 3}, OpIndex: 9}, Pos: cluster.Position{Epoch: 42, Seq: 3_000_000},
		Ordinal: 5_000_000, Fingerprint: 0x0102030405060708,
	}
}

func TestDedupeValueIsCanonical(t *testing.T) {
	ref, entry := dedupeGoldenEntry()
	key, value, err := EncodeDedupeEntry(ref, entry)
	if err != nil {
		t.Fatalf("EncodeDedupeEntry: %v", err)
	}
	if !bytes.Equal(value, dedupeGolden) {
		t.Fatalf("value = %x (%d bytes), want the 19-byte golden %x", value, len(value), dedupeGolden)
	}
	nonMinimal := append([]byte{0x03, 0xC0, 0x96, 0xB1, 0x02, 0xAA, 0x00}, dedupeGolden[6:]...)
	overflow := append([]byte{0x03, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02}, dedupeGolden[5:]...)
	shortResult := append(append([]byte(nil), dedupeGolden[:18]...), 0x01)
	trailing := append(append([]byte(nil), dedupeGolden...), 'x')
	nonMinimalResultLen := append(append([]byte(nil), dedupeGolden[:18]...), 0x81, 0x00, 'x')
	for name, mutant := range map[string][]byte{
		"non-minimal epoch varint":     nonMinimal,
		"overflowing ordinal varint":   overflow,
		"resultLen past the bytes":     shortResult,
		"bytes past resultLen":         trailing,
		"non-minimal resultLen varint": nonMinimalResultLen,
		"truncated after fingerprint":  dedupeGolden[:18],
		"empty":                        nil,
	} {
		if _, _, err := DecodeDedupeEntry(key, mutant); err == nil {
			t.Errorf("%s: %x accepted", name, mutant)
		}
	}

	rng := rand.New(rand.NewSource(33))
	var seeds [][]byte
	for _, seed := range []machine.DedupeEntry{
		entry,
		{Key: entry.Key, Pos: cluster.Position{Epoch: 1, Seq: 1}, Ordinal: 1, Result: []byte("x")},
		{Key: entry.Key, Pos: cluster.Position{Epoch: 1 << 40, Seq: 1 << 50}, Ordinal: 1 << 60, Fingerprint: 1 << 63, Result: bytes.Repeat([]byte{0xAB}, 300)},
	} {
		_, value, err := EncodeDedupeEntry(ref, seed)
		if err != nil {
			t.Fatalf("EncodeDedupeEntry: %v", err)
		}
		seeds = append(seeds, value)
	}
	accepted := 0
	for i := 0; i < 100000; i++ {
		value := append([]byte(nil), seeds[rng.Intn(len(seeds))]...)
		for flips := rng.Intn(4) + 1; flips > 0; flips-- {
			switch rng.Intn(3) {
			case 0:
				if len(value) > 0 {
					value[rng.Intn(len(value))] ^= byte(1 << rng.Intn(8))
				}
			case 1:
				if len(value) > 0 {
					value = value[:rng.Intn(len(value))]
				}
			default:
				at := rng.Intn(len(value) + 1)
				value = append(value[:at], append([]byte{byte(rng.Intn(256))}, value[at:]...)...)
			}
		}
		gotRef, got, err := DecodeDedupeEntry(key, value)
		if err != nil {
			continue
		}
		accepted++
		if len(got.Result) > len(value) {
			t.Fatalf("decoded a %d-byte result out of a %d-byte value", len(got.Result), len(value))
		}
		_, again, err := EncodeDedupeEntry(gotRef, got)
		if err != nil || !bytes.Equal(again, value) {
			t.Fatalf("accepted %x re-encodes to %x (%v): a non-canonical value was read", value, again, err)
		}
	}
	if accepted == 0 {
		t.Fatal("the mutator never produced an acceptable value")
	}
}

func TestAResultOver65535BytesIsStillRefused(t *testing.T) {
	ref, entry := dedupeGoldenEntry()
	entry.Result = make([]byte, 65536)
	if _, _, err := EncodeDedupeEntry(ref, entry); !errors.Is(err, ErrDedupeResultTooLarge) {
		t.Fatalf("EncodeDedupeEntry of a 65536-byte result: err = %v, want ErrDedupeResultTooLarge", err)
	}
	entry.Result = make([]byte, 65535)
	if _, value, err := EncodeDedupeEntry(ref, entry); err != nil || len(value) != 1+4+1+4+8+3+65535 {
		t.Fatalf("a 65535-byte result: %d bytes, %v; want it stored", len(value), err)
	}

	badger, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("OpenBadgerKV: %v", err)
	}
	t.Cleanup(func() { _ = badger.Close() })
	batch := badger.NewBatch()
	defer batch.Discard()
	window := machine.Window{Ordinal: 4}
	cfg := machine.WindowConfig{Window: 8, MaxResult: 1 << 20}
	refused, err := RecordDedupe(batch, ref, window, machine.DedupeWrite{Key: entry.Key, Result: make([]byte, 65536)}, cfg)
	if !errors.Is(err, ErrDedupeResultTooLarge) {
		t.Fatalf("RecordDedupe of a 65536-byte result under a 1 MiB cap: err = %v, want ErrDedupeResultTooLarge", err)
	}
	if refused.Ordinal != 4 {
		t.Fatalf("a refused write moved the ordinal to %d", refused.Ordinal)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	snapshot := badger.NewSnapshot()
	defer snapshot.Discard()
	if _, err := snapshot.Get(DedupeEntryKey(ref, entry.Key)); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("a refused result left an entry behind (Get: %v)", err)
	}
}

// TestADamagedOrOldEntryIsNeverReadAsNoEntry is F2's data-safety proof: a lookup that hits
// bytes it cannot decode returns the error, never nil, because a silent miss would apply a
// retried pop twice.
func TestADamagedOrOldEntryIsNeverReadAsNoEntry(t *testing.T) {
	badger, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("OpenBadgerKV: %v", err)
	}
	t.Cleanup(func() { _ = badger.Close() })
	ref, entry := dedupeGoldenEntry()
	key := DedupeEntryKey(ref, entry.Key)
	formatTwo := []byte{2, 0, 0, 0, 0, 0, 0, 0, 13, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 11, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0, 0}
	for name, value := range map[string][]byte{"format 2": formatTwo, "garbage": []byte("zz"), "empty": {}} {
		batch := badger.NewBatch()
		if err := batch.Set(key, value); err != nil {
			t.Fatal(err)
		}
		if err := batch.Commit(false); err != nil {
			t.Fatal(err)
		}
		got, err := NewKVDedupeReader(badger).Lookup(ref.Slot, ref.DatabaseID, ref.BucketID, entry.Key)
		if err == nil || got != nil {
			t.Fatalf("%s: Lookup = %+v, %v; want the decode error and no entry", name, got, err)
		}
	}
}

// TestAFormatTwoDedupeEntryIsReportedDamagedAndLeftInPlace is R4 over the format-1
// fixture's Badger half (its entries are format 0x02): the sweeper names each entry with
// the spec's text and deletes nothing. The Badger half is opened directly because the
// fixture's log holds version-1 records, which F4's open refuses before any sweep.
func TestAFormatTwoDedupeEntryIsReportedDamagedAndLeftInPlace(t *testing.T) {
	dir := copyRingFixture(t)
	mustWrite(t, filepath.Join(dir, ringFormatFile), ringFormatLine)
	badger, err := storage.OpenBadgerKV(filepath.Join(dir, "badger"), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("OpenBadgerKV: %v", err)
	}
	t.Cleanup(func() { _ = badger.Close() })

	type entryKey struct {
		slot uint16
		db   uint64
		key  string
	}
	var entries []entryKey
	snapshot := badger.NewSnapshot()
	iter := snapshot.NewIterator(storage.IterOptions{})
	for iter.Seek(nil); iter.Valid(); iter.Next() {
		if key := iter.Key(); len(key) == dedupeKeyLen && key[18] == storage.NSDedupe {
			_, _, decodeErr := DecodeDedupeEntry(key, mustValue(t, iter))
			if decodeErr == nil || !strings.Contains(decodeErr.Error(), "format 0x02") {
				t.Fatalf("the fixture entry %x decodes (%v); it must be refused naming format 0x02", key, decodeErr)
			}
			entries = append(entries, entryKey{slot: binary.BigEndian.Uint16(key[0:2]), db: binary.BigEndian.Uint64(key[2:10]), key: string(key)})
		}
	}
	iter.Close()
	snapshot.Discard()
	if len(entries) < 3 {
		t.Fatalf("the fixture holds %d dedupe entries, want at least 3", len(entries))
	}

	pairs := &cluster.PairSet{}
	sweeper := NewDedupeSweeper(badger, pairs, machine.WindowConfig{Window: 1, MaxResult: 1024})
	seen := map[entryKey]bool{}
	for _, e := range entries {
		if seen[entryKey{slot: e.slot, db: e.db}] {
			continue
		}
		seen[entryKey{slot: e.slot, db: e.db}] = true
		pairs.With(e.slot, e.db, func(p *cluster.Pair) { p.Ordinal = 1 << 40 })
		_, _, err := sweeper.Pass(e.slot, e.db, 1000)
		if err == nil {
			t.Fatalf("Pass over slot %d database %d reported nothing; the format-2 entries must be reported damaged", e.slot, e.db)
		}
		for _, want := range []string{
			"the dedupe entry ", " of slot ", " database ", " does not decode (", "format 0x02",
			") and is left in place; a retry of its operation applies again once its window passes. Run 'status' to see this node; a store that reports many of these is damaged",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("Pass error %q\n  lacks %q", err.Error(), want)
			}
		}
		for _, banned := range []string{"seeded", "fresh data directory"} {
			if strings.Contains(err.Error(), banned) {
				t.Errorf("the sweeper's report says %q, advice that loses committed cluster state on a member: %v", banned, err)
			}
		}
	}
	snapshot = badger.NewSnapshot()
	defer snapshot.Discard()
	for _, e := range entries {
		if _, err := snapshot.Get([]byte(e.key)); err != nil {
			t.Errorf("the damaged entry %x was not left in place: %v", e.key, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, ringFormatFile)); err != nil {
		t.Fatal(err)
	}
}

func mustValue(t *testing.T, iter storage.KVIterator) []byte {
	t.Helper()
	value, err := iter.Value()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

// TestADedupeValueSayingMoreThan65535ResultBytesIsRefused: the decoder holds the same bound
// as the encoder, so a hand-built value whose length matches its 65,536 bytes is still damaged.
func TestADedupeValueSayingMoreThan65535ResultBytesIsRefused(t *testing.T) {
	ref, entry := dedupeGoldenEntry()
	key, _, err := EncodeDedupeEntry(ref, entry)
	if err != nil {
		t.Fatal(err)
	}
	oversize := append(append([]byte(nil), dedupeGolden[:18]...), 0x80, 0x80, 0x04) // uvarint 65536
	oversize = append(oversize, make([]byte, 65536)...)
	if _, _, err := DecodeDedupeEntry(key, oversize); err == nil {
		t.Fatal("a 65536-byte result with a matching length was accepted: the resultLen bound is gone")
	}
	exact := append(append([]byte(nil), dedupeGolden[:18]...), 0xFF, 0xFF, 0x03) // uvarint 65535
	exact = append(exact, make([]byte, 65535)...)
	if _, _, err := DecodeDedupeEntry(key, exact); err != nil {
		t.Fatalf("a 65535-byte result was refused: %v", err)
	}
}
