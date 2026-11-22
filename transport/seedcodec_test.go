package transport

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"math/rand/v2"
	"runtime"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

var seedTestID = [seedIDLen]byte{0x3F, 0x0A, 0x8C, 0x1D, 0x22, 0xB9, 0x44, 0xE7, 0xA1, 0x5C, 0x0D, 0x6E, 0x88, 0xF3, 0x1B, 0x04}

// seedTestHeader is the envelope of seeding Formats 2.
func seedTestHeader() Envelope {
	return Envelope{Epoch: 0x29, StateSeq: 0x29, Slot: 0x1362, DBID: 1}
}

// seedGoldenEntries are the two entries of seeding Formats 2.
func seedGoldenEntries() []SeedEntry {
	headerKey := mustHex("1362" + "0000000000000001" + "0000000000000029" + "01" + "5070310000")
	fieldKey := append(append([]byte{}, headerKey...), 0x00, 0x01)
	return []SeedEntry{
		{Key: headerKey, Value: mustHex("0203000000703100")},
		{Key: fieldKey, Value: mustHex("020400000041646100")},
	}
}

func mustHex(text string) []byte {
	raw, err := hex.DecodeString(text)
	if err != nil {
		panic(err)
	}
	return raw
}

func seedGoldenChunk() SeedChunk {
	return SeedChunk{Header: seedTestHeader(), SeedID: seedTestID, Index: 3, Entries: seedGoldenEntries()}
}

func TestSeedChunkMatchesTheSpecGolden(t *testing.T) {
	payload, err := EncodeSeedChunk(seedGoldenChunk())
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) != 133 {
		t.Fatalf("payload is %d bytes, seeding Formats 2 says 133", len(payload))
	}
	if got := payload[EnvelopeSize+28 : EnvelopeSize+32]; !bytes.Equal(got, []byte{0x7E, 0x0D, 0x88, 0xCE}) {
		t.Fatalf("crc32c is %x, seeding Formats 2 says 7e0d88ce", got)
	}
	decoded, err := DecodeSeedChunk(payload)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Index != 3 || len(decoded.Entries) != 2 || decoded.CRC != 0x7E0D88CE || decoded.Final {
		t.Fatalf("decoded %+v", decoded)
	}
}

// TestSeedChunkCRCIsOverEntriesOnly: a flipped entry byte is refused, while a
// flipped header byte (seed id, index) is not the CRC's business, so the chunk
// still decodes and the sequence check is what catches the index.
func TestSeedChunkCRCIsOverEntriesOnly(t *testing.T) {
	payload, err := EncodeSeedChunk(seedGoldenChunk())
	if err != nil {
		t.Fatal(err)
	}
	entriesAt := EnvelopeSize + seedIDLen + 8 + 4 + 4
	for at := entriesAt; at < len(payload); at++ {
		flipped := bytes.Clone(payload)
		flipped[at] ^= 0x01
		if _, err := DecodeSeedChunk(flipped); err == nil {
			t.Fatalf("flipping entry byte %d was accepted", at)
		}
	}
	for at := EnvelopeSize; at < EnvelopeSize+seedIDLen+8; at++ {
		flipped := bytes.Clone(payload)
		flipped[at] ^= 0x01
		if _, err := DecodeSeedChunk(flipped); err != nil {
			t.Fatalf("flipping seed id or chunk index byte %d was refused by the decoder (%v); the CRC covers entries only", at, err)
		}
	}
}

func TestChunkLargerThanTheNominalSizeIsNotSplit(t *testing.T) {
	const nominal = 1 << 20
	big := SeedEntry{Key: bytes.Repeat([]byte{0x01}, 24), Value: bytes.Repeat([]byte{0xAB}, 3*nominal)}
	chunk := SeedChunk{Header: seedTestHeader(), SeedID: seedTestID, Entries: []SeedEntry{big, seedGoldenEntries()[0]}}
	payload, err := EncodeSeedChunk(chunk)
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) <= nominal {
		t.Fatalf("payload is %d bytes, expected a chunk above the nominal %d", len(payload), nominal)
	}
	decoded, err := DecodeSeedChunk(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Entries) != 2 || !bytes.Equal(decoded.Entries[0].Value, big.Value) {
		t.Fatalf("the oversize entry did not survive whole: %d entries", len(decoded.Entries))
	}
	tooBig := SeedChunk{Header: seedTestHeader(), Entries: []SeedEntry{{Key: big.Key, Value: make([]byte, MaxFrameLength)}}}
	if _, err := EncodeSeedChunk(tooBig); !errors.Is(err, ErrSeedMalformed) || !strings.Contains(err.Error(), "frame ceiling") {
		t.Fatalf("an entry past the frame ceiling must be refused naming it, got %v", err)
	}
}

func TestCreditIsMonotonic(t *testing.T) {
	credit := NewCreditWindow()
	cut := SeedPosition{Epoch: 41, Seq: 100}
	if err := credit.Open(SeedAccept{SeedID: seedTestID, Cut: cut, WindowBytes: 8 << 20}); err != nil {
		t.Fatal(err)
	}
	ack := func(seq uint64, window uint32) SeedAck {
		return SeedAck{SeedID: seedTestID, Applied: SeedPosition{Epoch: 41, Seq: seq}, CreditBytes: window}
	}
	// The same absolute window restated twice is the same window: a delta
	// would have doubled it.
	for range 2 {
		decoded, err := DecodeSeedAck(EncodeSeedAck(ack(100, 4<<20)))
		if err != nil {
			t.Fatal(err)
		}
		if err := credit.Observe(decoded); err != nil {
			t.Fatal(err)
		}
		if window, _ := credit.Window(seedTestID); window != 4<<20 {
			t.Fatalf("window is %d after restating 4 MiB, want 4 MiB", window)
		}
	}
	// A smaller restatement replaces; it is not subtracted from the last.
	if err := credit.Observe(ack(120, 1<<20)); err != nil {
		t.Fatal(err)
	}
	if window, _ := credit.Window(seedTestID); window != 1<<20 {
		t.Fatalf("window is %d, want the restated 1 MiB", window)
	}
	// The acknowledged position never moves backwards.
	err := credit.Observe(ack(110, 8<<20))
	if !errors.Is(err, ErrSeedAckRegressed) {
		t.Fatalf("an ack below the applied position must be refused, got %v", err)
	}
	if window, _ := credit.Window(seedTestID); window != 1<<20 {
		t.Fatalf("a refused ack changed the window to %d", window)
	}
	// An ack for a seed that was never accepted is refused.
	stranger := ack(1, 1)
	stranger.SeedID[0] ^= 0xFF
	if err := credit.Observe(stranger); !errors.Is(err, ErrSeedUnknown) || errors.Is(err, ErrSeedAckRegressed) {
		t.Fatalf("an ack for an unknown seed must be refused as ErrSeedUnknown, got %v", err)
	}
}

func TestFinalChunkSetsTheLastFlag(t *testing.T) {
	for _, final := range []bool{false, true} {
		chunk := seedGoldenChunk()
		chunk.Final = final
		payload, err := EncodeSeedChunk(chunk)
		if err != nil {
			t.Fatal(err)
		}
		header, err := DecodeEnvelope(payload)
		if err != nil {
			t.Fatal(err)
		}
		if got := header.Flags&FlagLastOfChunk != 0; got != final {
			t.Fatalf("final=%v but envelope flag bit 1 is %v", final, got)
		}
		decoded, err := DecodeSeedChunk(payload)
		if err != nil || decoded.Final != final {
			t.Fatalf("final=%v decoded as %v (%v)", final, decoded.Final, err)
		}
	}
	payload, _ := EncodeSeedChunk(seedGoldenChunk())
	payload[19] |= 0x01 // flag bit 0, ack requested: not defined on a chunk
	if _, err := DecodeSeedChunk(payload); !errors.Is(err, ErrSeedMalformed) {
		t.Fatalf("bit 0 on a chunk must be refused, got %v", err)
	}
}

func TestSeedDedupeIsNotAMessage(t *testing.T) {
	if err := CheckKind(KindSeedDedupe, ChannelSeed); err == nil || !strings.Contains(err.Error(), "withdrawn") {
		t.Fatalf("kind 0x28 must be refused as withdrawn, got %v", err)
	}
	frame := EncodeEnvelope(Envelope{Version: EnvelopeVersion, Kind: KindSeedDedupe})
	decoders := map[string]func([]byte) error{
		"register": func(p []byte) error { _, err := DecodeSeedRegister(p); return err },
		"accept":   func(p []byte) error { _, err := DecodeSeedAccept(p); return err },
		"redirect": func(p []byte) error { _, err := DecodeSeedRedirect(p); return err },
		"refused":  func(p []byte) error { _, err := DecodeSeedRefused(p); return err },
		"busy":     func(p []byte) error { _, err := DecodeSeedBusy(p); return err },
		"chunk":    func(p []byte) error { _, err := DecodeSeedChunk(p); return err },
		"ack":      func(p []byte) error { _, err := DecodeSeedAck(p); return err },
		"restart":  func(p []byte) error { _, err := DecodeSeedRestart(p); return err },
	}
	for name, decode := range decoders {
		if err := decode(frame); err == nil {
			t.Fatalf("the %s decoder accepted kind 0x28", name)
		}
	}
	for kind := KindSeedRegister; kind <= KindSeedRestart; kind++ {
		if channel, ok := KindChannel(kind); !ok || channel != ChannelSeed {
			t.Fatalf("kind 0x%02x is not a seed kind", kind)
		}
	}
}

func chunkAt(index uint64, final bool) SeedChunk {
	return SeedChunk{SeedID: seedTestID, Index: index, Final: final}
}

func TestChunkIndexGapIsRefused(t *testing.T) {
	sequence := NewChunkSequence()
	for _, index := range []uint64{0, 1, 2} {
		if err := sequence.Accept(chunkAt(index, false)); err != nil {
			t.Fatalf("chunk %d: %v", index, err)
		}
	}
	err := sequence.Accept(chunkAt(4, true))
	if !errors.Is(err, ErrSeedChunkGap) {
		t.Fatalf("chunks 0,1,2,4 must be refused, got %v", err)
	}
	if text := err.Error(); !strings.Contains(text, hex.EncodeToString(seedTestID[:])) || !strings.Contains(text, "chunk 3 is missing") {
		t.Fatalf("the refusal must name the seed and the missing index, got %q", text)
	}
	if sequence.Finished(seedTestID) {
		t.Fatal("a refused final chunk finished the stream")
	}
}

func TestChunkSequenceStartsAtZeroAndEndsAtTheFinalChunk(t *testing.T) {
	sequence := NewChunkSequence()
	if err := sequence.Accept(chunkAt(1, false)); !errors.Is(err, ErrSeedChunkGap) {
		t.Fatalf("a stream starting at 1 must be refused, got %v", err)
	}
	if err := sequence.Accept(chunkAt(0, false)); err != nil {
		t.Fatal(err)
	}
	if err := sequence.Accept(chunkAt(0, false)); !errors.Is(err, ErrSeedChunkGap) {
		t.Fatalf("a repeated index must be refused, got %v", err)
	}
	if err := sequence.Accept(chunkAt(1, true)); err != nil || !sequence.Finished(seedTestID) {
		t.Fatalf("final chunk: %v", err)
	}
	if err := sequence.Accept(chunkAt(2, false)); !errors.Is(err, ErrSeedChunkGap) {
		t.Fatalf("a chunk after the final one must be refused, got %v", err)
	}
	sequence.Restart(seedTestID)
	if err := sequence.Accept(chunkAt(0, false)); err != nil {
		t.Fatalf("a restarted stream begins again at 0: %v", err)
	}
	other := chunkAt(0, false)
	other.SeedID[0] ^= 0xFF
	if err := sequence.Accept(other); err != nil {
		t.Fatalf("seed ids are tracked separately: %v", err)
	}
}

func TestSeedTrackersAreBounded(t *testing.T) {
	sequence := NewChunkSequence()
	credit := NewCreditWindow()
	var refusedChunks, refusedCredit error
	for n := 0; n < seedMaxTracked+1; n++ {
		id := [seedIDLen]byte{byte(n), byte(n >> 8)}
		refusedChunks = sequence.Accept(SeedChunk{SeedID: id})
		refusedCredit = credit.Open(SeedAccept{SeedID: id})
	}
	if refusedChunks == nil || refusedCredit == nil {
		t.Fatal("the trackers grew past their bound")
	}
}

func TestStorageKeyBoundMatches(t *testing.T) {
	if seedMaxKeyBytes != storage.MaxKeyBytes {
		t.Fatalf("seedMaxKeyBytes %d drifted from storage.MaxKeyBytes %d", seedMaxKeyBytes, storage.MaxKeyBytes)
	}
}

func TestSeedMessagesRoundTrip(t *testing.T) {
	header := seedTestHeader()
	register, err := EncodeSeedRegister(SeedRegister{Header: header, SeedID: seedTestID, MyPosition: SeedPosition{41, 98213}, Mask: 1, Cursor: []byte("cursor")})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeSeedRegister(register); err != nil || got.Mask != 1 || string(got.Cursor) != "cursor" || got.MyPosition.Seq != 98213 {
		t.Fatalf("register %+v %v", got, err)
	}
	if got, err := DecodeSeedAccept(EncodeSeedAccept(SeedAccept{Header: header, SeedID: seedTestID, Cut: SeedPosition{41, 100}, WindowBytes: 8 << 20, Resume: true})); err != nil || !got.Resume || got.WindowBytes != 8<<20 {
		t.Fatalf("accept %+v %v", got, err)
	}
	redirect, err := EncodeSeedRedirect(SeedRedirect{Header: header, SeedID: seedTestID, Source: "node-b"})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeSeedRedirect(redirect); err != nil || got.Source != "node-b" {
		t.Fatalf("redirect %+v %v", got, err)
	}
	refused, err := EncodeSeedRefused(SeedRefused{Header: header, SeedID: seedTestID, Reason: 5, Detail: "mask bit 3"})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeSeedRefused(refused); err != nil || got.Reason != 5 || got.Detail != "mask bit 3" {
		t.Fatalf("refused %+v %v", got, err)
	}
	if got, err := DecodeSeedBusy(EncodeSeedBusy(SeedBusy{Header: header, SeedID: seedTestID, RetryAfterMs: 250})); err != nil || got.RetryAfterMs != 250 {
		t.Fatalf("busy %+v %v", got, err)
	}
	restart, err := EncodeSeedRestart(SeedRestart{Header: header, SeedID: seedTestID, Reason: 3})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeSeedRestart(restart); err != nil || got.Reason != 3 {
		t.Fatalf("restart %+v %v", got, err)
	}
	if _, err := EncodeSeedRestart(SeedRestart{Reason: 5}); err == nil {
		t.Fatal("restart reason 5 does not exist")
	}
	if _, err := EncodeSeedRefused(SeedRefused{Reason: 6}); err == nil {
		t.Fatal("refused reason 6 does not exist")
	}
	// A kind that is not the decoder's own is refused.
	if _, err := DecodeSeedAck(restart); err == nil {
		t.Fatal("an ack decoder accepted a restart")
	}
}

// TestSeedDecodersRefuseHostileSizes: counts and lengths are checked against
// the bytes present before anything is allocated.
func TestSeedDecodersRefuseHostileSizes(t *testing.T) {
	payload, err := EncodeSeedChunk(SeedChunk{Header: seedTestHeader(), SeedID: seedTestID})
	if err != nil {
		t.Fatal(err)
	}
	countAt := EnvelopeSize + seedIDLen + 8
	payload[countAt], payload[countAt+1], payload[countAt+2], payload[countAt+3] = 0xFF, 0xFF, 0xFF, 0xFF
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if _, err := DecodeSeedChunk(payload); !errors.Is(err, ErrSeedMalformed) {
		t.Fatalf("4 billion entries in an empty chunk must be refused, got %v", err)
	}
	runtime.ReadMemStats(&after)
	if grown := after.TotalAlloc - before.TotalAlloc; grown > 1<<20 {
		t.Fatalf("a hostile entry count allocated %d bytes", grown)
	}
	huge := append(bytes.Clone(payload[:EnvelopeSize+seedIDLen+8]), 0, 0, 0, 1)
	huge = append(huge, 0, 0, 0, 0) // crc of nothing is 0
	huge = append(huge, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F, 1)
	if _, err := DecodeSeedChunk(huge); err == nil {
		t.Fatal("a key length past the frame was accepted")
	}
	if _, err := DecodeSeedChunk(make([]byte, MaxFrameLength+1)); !errors.Is(err, ErrSeedMalformed) {
		t.Fatalf("a payload over the frame ceiling must be refused, got %v", err)
	}
}

func TestSeedChunkRefusesANonCanonicalLength(t *testing.T) {
	// klen 1 written as the two-byte uvarint 81 00: same number, other bytes.
	body := []byte{0x81, 0x00, 0x01, 0x00}
	payload := seedStart(seedTestHeader(), KindSeedChunk, 0, 0)
	payload = append(payload, seedTestID[:]...)
	payload = append(payload, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1)
	crc := crc32.Checksum(body, castagnoli)
	payload = append(payload, byte(crc>>24), byte(crc>>16), byte(crc>>8), byte(crc))
	payload = append(payload, body...)
	if _, err := DecodeSeedChunk(payload); !errors.Is(err, ErrSeedMalformed) {
		t.Fatalf("an over-long uvarint must be refused, got %v", err)
	}
}

// seedMutationRuns is the build doc's bar for every decoder.
const seedMutationRuns = 20000

func TestSeedDecodersSurviveMutation(t *testing.T) {
	header := seedTestHeader()
	chunkPayload, err := EncodeSeedChunk(SeedChunk{Header: header, SeedID: seedTestID, Index: 3, Entries: seedGoldenEntries(), Final: true})
	if err != nil {
		t.Fatal(err)
	}
	registerPayload, _ := EncodeSeedRegister(SeedRegister{Header: header, SeedID: seedTestID, MyPosition: SeedPosition{41, 9}, Mask: 1, Cursor: []byte("abc")})
	redirectPayload, _ := EncodeSeedRedirect(SeedRedirect{Header: header, SeedID: seedTestID, Source: "node-b"})
	refusedPayload, _ := EncodeSeedRefused(SeedRefused{Header: header, SeedID: seedTestID, Reason: 4, Detail: "not current"})
	restartPayload, _ := EncodeSeedRestart(SeedRestart{Header: header, SeedID: seedTestID, Reason: 2})
	cases := []struct {
		name    string
		golden  []byte
		reencod func([]byte) ([]byte, error)
	}{
		{"register", registerPayload, func(p []byte) ([]byte, error) {
			msg, err := DecodeSeedRegister(p)
			if err != nil {
				return nil, err
			}
			return EncodeSeedRegister(msg)
		}},
		{"accept", EncodeSeedAccept(SeedAccept{Header: header, SeedID: seedTestID, Cut: SeedPosition{41, 100}, WindowBytes: 1 << 20, Resume: true}), func(p []byte) ([]byte, error) {
			msg, err := DecodeSeedAccept(p)
			return EncodeSeedAccept(msg), err
		}},
		{"redirect", redirectPayload, func(p []byte) ([]byte, error) {
			msg, err := DecodeSeedRedirect(p)
			if err != nil {
				return nil, err
			}
			return EncodeSeedRedirect(msg)
		}},
		{"refused", refusedPayload, func(p []byte) ([]byte, error) {
			msg, err := DecodeSeedRefused(p)
			if err != nil {
				return nil, err
			}
			return EncodeSeedRefused(msg)
		}},
		{"busy", EncodeSeedBusy(SeedBusy{Header: header, SeedID: seedTestID, RetryAfterMs: 5}), func(p []byte) ([]byte, error) {
			msg, err := DecodeSeedBusy(p)
			return EncodeSeedBusy(msg), err
		}},
		{"chunk", chunkPayload, func(p []byte) ([]byte, error) {
			msg, err := DecodeSeedChunk(p)
			if err != nil {
				return nil, err
			}
			return EncodeSeedChunk(msg)
		}},
		{"ack", EncodeSeedAck(SeedAck{Header: header, SeedID: seedTestID, Applied: SeedPosition{41, 100}, CreditBytes: 77}), func(p []byte) ([]byte, error) {
			msg, err := DecodeSeedAck(p)
			return EncodeSeedAck(msg), err
		}},
		{"restart", restartPayload, func(p []byte) ([]byte, error) {
			msg, err := DecodeSeedRestart(p)
			if err != nil {
				return nil, err
			}
			return EncodeSeedRestart(msg)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if again, err := tc.reencod(tc.golden); err != nil || !bytes.Equal(again, tc.golden) {
				t.Fatalf("the golden does not round-trip: %v", err)
			}
			rng := rand.New(rand.NewPCG(26, uint64(len(tc.name))))
			var before, after runtime.MemStats
			accepted := 0
			for run := 0; run < seedMutationRuns; run++ {
				mutated := mutateSeedPayload(rng, tc.golden)
				if tc.name == "chunk" && rng.IntN(2) == 0 {
					refreshChunkCRC(mutated)
				}
				runtime.ReadMemStats(&before)
				again, err := tc.reencod(mutated)
				runtime.ReadMemStats(&after)
				if grown := after.TotalAlloc - before.TotalAlloc; grown > 8<<20 {
					t.Fatalf("run %d allocated %d bytes for a %d byte input", run, grown, len(mutated))
				}
				if err != nil {
					continue
				}
				accepted++
				if !bytes.Equal(again, mutated) {
					t.Fatalf("run %d: accepted input re-encodes differently\n in  %x\n out %x", run, mutated, again)
				}
			}
			if accepted == 0 {
				t.Fatal("no mutation was ever accepted, the test proves nothing about re-encoding")
			}
		})
	}
}

// mutateSeedPayload flips, sets, inserts, deletes or truncates bytes of golden.
func mutateSeedPayload(rng *rand.Rand, golden []byte) []byte {
	mutated := bytes.Clone(golden)
	for edits := 1 + rng.IntN(3); edits > 0; edits-- {
		if len(mutated) == 0 {
			return mutated
		}
		at := rng.IntN(len(mutated))
		switch rng.IntN(6) {
		case 0:
			mutated[at] ^= byte(1 << rng.IntN(8))
		case 1:
			mutated[at] = byte(rng.IntN(256))
		case 2:
			mutated = append(mutated[:at], mutated[at+1:]...)
		case 3:
			mutated = append(mutated[:at], append([]byte{byte(rng.IntN(256))}, mutated[at:]...)...)
		case 4:
			mutated = mutated[:at]
		default:
			mutated = append(mutated, byte(rng.IntN(256)))
		}
	}
	return mutated
}

// refreshChunkCRC rewrites a mutated chunk's CRC so the mutation reaches the
// entry parser instead of stopping at the checksum.
func refreshChunkCRC(payload []byte) {
	crcAt := EnvelopeSize + seedIDLen + 8 + 4
	if len(payload) < crcAt+4 {
		return
	}
	binary.BigEndian.PutUint32(payload[crcAt:], crc32.Checksum(payload[crcAt+4:], castagnoli))
}

// seedChunkWithPayload returns a chunk whose SEED_CHUNK payload is exactly size
// bytes: one entry with a one-byte key and a value that fills the rest.
func seedChunkWithPayload(size int) SeedChunk {
	// Envelope, seed id, index, count, CRC, then the key length and key.
	fixed := EnvelopeSize + seedIDLen + 16 + 1 + 1
	width := 1
	for {
		valueLen := size - fixed - width
		w := uvarintLen(uint64(valueLen))
		if w == width {
			return SeedChunk{Header: seedTestHeader(), SeedID: seedTestID, Entries: []SeedEntry{{Key: []byte{0x01}, Value: make([]byte, valueLen)}}}
		}
		width = w
	}
}

// TestSeedChunkEncoderCeilingMatchesTheFrameWriter holds the SEED_CHUNK encoder
// to the ceiling the frame writer applies. The frame body is opcode, request ID
// and payload, so the largest payload that frames is MaxFrameLength minus the
// opcode and request ID. An encoder that accepts more hands back a chunk that
// cannot be sent.
func TestSeedChunkEncoderCeilingMatchesTheFrameWriter(t *testing.T) {
	// The window starts at the last size that frames, so the boundary is in it.
	largestFramed := -1
	for size := MaxFrameLength - 1 - RequestIDLen; size <= MaxFrameLength; size++ {
		_, encodeErr := EncodeSeedChunk(seedChunkWithPayload(size))

		frame := NewFrameWithID(OpAuth, [RequestIDLen]byte{}, make([]byte, size)).Encode()
		_, frameErr := DecodeFrame(bytes.NewReader(frame))

		switch {
		case encodeErr == nil && frameErr != nil:
			t.Fatalf("the encoder accepted a %d byte payload that the frame writer refuses: %v", size, frameErr)
		case encodeErr != nil && frameErr == nil:
			t.Fatalf("the encoder refused a %d byte payload that frames: %v", size, encodeErr)
		case encodeErr != nil && !errors.Is(encodeErr, ErrSeedMalformed):
			t.Fatalf("a %d byte payload was refused with %v, want ErrSeedMalformed", size, encodeErr)
		}
		if frameErr == nil {
			largestFramed = size
		}
	}
	if want := MaxFrameLength - 1 - RequestIDLen; largestFramed != want {
		t.Fatalf("the largest framed payload is %d bytes, want %d", largestFramed, want)
	}

	// The largest accepted chunk frames and decodes back to the same bytes.
	payload, err := EncodeSeedChunk(seedChunkWithPayload(largestFramed))
	if err != nil {
		t.Fatal(err)
	}
	frame, err := DecodeFrame(bytes.NewReader(NewFrameWithID(OpAuth, [RequestIDLen]byte{}, payload).Encode()))
	if err != nil {
		t.Fatalf("the largest accepted chunk does not frame: %v", err)
	}
	if !bytes.Equal(frame.Payload, payload) {
		t.Fatal("the largest accepted chunk did not survive framing")
	}
}

// TestSeedLimitIsALocalRefusalNotAPeerViolation fills both trackers to the
// 1024-seed cap, then sends one more seed. The cap is this node's memory bound,
// so the refusal must be ErrSeedLimit, never the peer-fault sentinels.
func TestSeedLimitIsALocalRefusalNotAPeerViolation(t *testing.T) {
	sequence := NewChunkSequence()
	credit := NewCreditWindow()
	for n := range seedMaxTracked {
		id := [seedIDLen]byte{byte(n), byte(n >> 8)}
		if err := sequence.Accept(SeedChunk{SeedID: id}); err != nil {
			t.Fatalf("seed %d below the cap: %v", n, err)
		}
		if err := credit.Open(SeedAccept{SeedID: id}); err != nil {
			t.Fatalf("seed %d below the cap: %v", n, err)
		}
	}

	overflow := [seedIDLen]byte{0xEE, 0xEE}
	err := sequence.Accept(SeedChunk{SeedID: overflow})
	if !errors.Is(err, ErrSeedLimit) || errors.Is(err, ErrSeedChunkGap) {
		t.Fatalf("a chunk past the seed cap must be ErrSeedLimit, not ErrSeedChunkGap, got %v", err)
	}
	err = credit.Open(SeedAccept{SeedID: overflow})
	if !errors.Is(err, ErrSeedLimit) || errors.Is(err, ErrSeedAckRegressed) {
		t.Fatalf("an accept past the seed cap must be ErrSeedLimit, not ErrSeedAckRegressed, got %v", err)
	}
	if _, known := credit.Window(overflow); known {
		t.Fatal("a refused accept opened the seed")
	}

	// An ack for a seed the window never opened is an unknown seed.
	err = credit.Observe(SeedAck{SeedID: overflow, Applied: SeedPosition{Epoch: 1, Seq: 1}, CreditBytes: 1})
	if !errors.Is(err, ErrSeedUnknown) || errors.Is(err, ErrSeedAckRegressed) {
		t.Fatalf("an ack for an unopened seed must be ErrSeedUnknown, not ErrSeedAckRegressed, got %v", err)
	}
	if ErrSeedLimit.Error() != "INVALID_ARGUMENT" || ErrSeedUnknown.Error() != "INVALID_ARGUMENT" {
		t.Fatal("the new seed sentinels must carry the INVALID_ARGUMENT text")
	}
}

// TestSeedChunkEntryCountIsBoundedBelowTheFrameLimit: a frame of minimal entries can hold
// millions of them, and the decoder's entry slice costs 32 bytes each, so the count has its
// own ceiling, refused before anything is allocated for it.
func TestSeedChunkEntryCountIsBoundedBelowTheFrameLimit(t *testing.T) {
	const count = seedMaxChunkEntries + 1
	entries := make([]SeedEntry, count)
	for i := range entries {
		entries[i] = SeedEntry{Key: []byte{1}}
	}
	// The encoder refuses it too, so a source with a huge seed_chunk_entries cannot build it.
	if _, err := EncodeSeedChunk(SeedChunk{Header: seedTestHeader(), SeedID: seedTestID, Entries: entries}); !errors.Is(err, ErrSeedMalformed) {
		t.Fatalf("the encoder accepted %d entries: %v", count, err)
	}
	// Built by hand: each entry is key length 1, key 1, value length 0 = 3 bytes.
	body := bytes.Repeat([]byte{1, 1, 0}, count)
	payload := append([]byte(nil), EncodeEnvelope(Envelope{Version: EnvelopeVersion, Kind: KindSeedChunk, Epoch: 1})...)
	payload = append(payload, seedTestID[:]...)
	payload = binary.BigEndian.AppendUint64(payload, 0)
	payload = binary.BigEndian.AppendUint32(payload, count)
	payload = binary.BigEndian.AppendUint32(payload, crc32.Checksum(body, castagnoli))
	payload = append(payload, body...)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := DecodeSeedChunk(payload)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrSeedMalformed) {
		t.Fatalf("a chunk of %d entries was accepted: %v", count, err)
	}
	if grown := after.TotalAlloc - before.TotalAlloc; grown > 1<<20 {
		t.Fatalf("refusing %d entries allocated %d bytes", count, grown)
	}
}
