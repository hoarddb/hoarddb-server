package wire

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"
	"runtime"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

func u33Bitmap(t *testing.T, slots ...uint16) []byte {
	t.Helper()
	bitmap := BitmapOfSlots(slots)
	if len(bitmap) == 0 {
		t.Fatalf("BitmapOfSlots(%v) built nothing", slots)
	}
	return bitmap
}

func TestHandoffCodecLayoutsAreTheRegistryBytes(t *testing.T) {
	bitmap := []byte{0x05, 0x80} // slots 0, 2, 15
	req, err := EncodeHandoffReq(HandoffReq{FromEpoch: 7, ToEpoch: 9, Bitmap: bitmap})
	if err != nil {
		t.Fatal(err)
	}
	want := binary.BigEndian.AppendUint64(nil, 7)
	want = binary.BigEndian.AppendUint64(want, 9)
	want = append(want, 0, 2, 0x05, 0x80)
	if !bytes.Equal(req, want) {
		t.Fatalf("HANDOFF_REQ = %x, want %x", req, want)
	}

	stopped, err := EncodeHandoffStopped(HandoffStopped{ToEpoch: 9, Bitmap: bitmap})
	if err != nil {
		t.Fatal(err)
	}
	want = append(binary.BigEndian.AppendUint64(nil, 9), 0, 2, 0x05, 0x80)
	if !bytes.Equal(stopped, want) {
		t.Fatalf("HANDOFF_STOPPED = %x, want %x", stopped, want)
	}

	ready, err := EncodeHandoffReady(HandoffReady{ToEpoch: 9, Bitmap: bitmap, AppliedPos: cluster.Position{Epoch: 8, Seq: 33}})
	if err != nil {
		t.Fatal(err)
	}
	want = append(binary.BigEndian.AppendUint64(nil, 9), 0, 2, 0x05, 0x80)
	want = cluster.AppendPosition(want, cluster.Position{Epoch: 8, Seq: 33})
	if !bytes.Equal(ready, want) {
		t.Fatalf("HANDOFF_READY = %x, want %x", ready, want)
	}

	query, err := EncodePositionQuery(PositionQuery{Slot: 300, DB: 4})
	if err != nil {
		t.Fatal(err)
	}
	want = binary.BigEndian.AppendUint16(nil, 300)
	want = binary.BigEndian.AppendUint64(want, 4)
	if !bytes.Equal(query, want) {
		t.Fatalf("POSITION_QUERY = %x, want %x", query, want)
	}

	reply, err := EncodePositionReply(PositionReply{Slot: 300, DB: 4, State: 2, AppliedPos: cluster.Position{Epoch: 8, Seq: 33}, AppliedCursor: 71})
	if err != nil {
		t.Fatal(err)
	}
	want = append(want, 2)
	want = cluster.AppendPosition(want, cluster.Position{Epoch: 8, Seq: 33})
	want = binary.BigEndian.AppendUint64(want, 71)
	if !bytes.Equal(reply, want) {
		t.Fatalf("POSITION_REPLY = %x, want %x", reply, want)
	}
}

func TestHandoffCodecRoundTrips(t *testing.T) {
	bitmap := u33Bitmap(t, 0, 2, 15, 8191)
	if got := SlotsOfBitmap(bitmap); len(got) != 4 || got[0] != 0 || got[1] != 2 || got[2] != 15 || got[3] != 8191 {
		t.Fatalf("SlotsOfBitmap = %v", got)
	}
	if len(bitmap) != 1024 {
		t.Fatalf("a bitmap naming slot 8191 is %d bytes, want 1024", len(bitmap))
	}
	if short := u33Bitmap(t, 3); len(short) != 1 || short[0] != 0x08 {
		t.Fatalf("a bitmap naming slot 3 = %x, want 08", short)
	}
	req := HandoffReq{FromEpoch: 1, ToEpoch: 2, Bitmap: bitmap}
	raw, err := EncodeHandoffReq(req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeHandoffReq(raw)
	if err != nil || got.FromEpoch != 1 || got.ToEpoch != 2 || !bytes.Equal(got.Bitmap, bitmap) {
		t.Fatalf("DecodeHandoffReq = %+v, %v", got, err)
	}
	readyRaw, err := EncodeHandoffReady(HandoffReady{ToEpoch: 2, Bitmap: bitmap, AppliedPos: cluster.Position{Epoch: 1, Seq: 5}})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := DecodeHandoffReady(readyRaw)
	if err != nil || ready.AppliedPos != (cluster.Position{Epoch: 1, Seq: 5}) {
		t.Fatalf("DecodeHandoffReady = %+v, %v", ready, err)
	}
	stoppedRaw, err := EncodeHandoffStopped(HandoffStopped{ToEpoch: 2, Bitmap: bitmap})
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := DecodeHandoffStopped(stoppedRaw)
	if err != nil || stopped.ToEpoch != 2 || !bytes.Equal(stopped.Bitmap, bitmap) {
		t.Fatalf("DecodeHandoffStopped = %+v, %v", stopped, err)
	}
}

func TestHandoffCodecRefusesWhatItCannotRead(t *testing.T) {
	good, err := EncodeHandoffReq(HandoffReq{FromEpoch: 1, ToEpoch: 2, Bitmap: []byte{1}})
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string][]byte{
		"empty":            nil,
		"short header":     good[:17],
		"missing bitmap":   good[:len(good)-1],
		"trailing byte":    append(append([]byte(nil), good...), 0),
		"zero length":      append(append([]byte(nil), good[:16]...), 0, 0),
		"length over 1024": append(append([]byte(nil), good[:16]...), 0x04, 0x01, 1),
	} {
		if _, err := DecodeHandoffReq(raw); err == nil {
			t.Errorf("%s: DecodeHandoffReq accepted %x", name, raw)
		}
	}
	if _, err := EncodeHandoffReq(HandoffReq{FromEpoch: 1, ToEpoch: 2}); err == nil {
		t.Error("EncodeHandoffReq accepted an empty bitmap")
	}
	if _, err := EncodeHandoffReq(HandoffReq{FromEpoch: 1, ToEpoch: 2, Bitmap: make([]byte, 1025)}); err == nil {
		t.Error("EncodeHandoffReq accepted a 1025-byte bitmap")
	}
	if _, err := DecodePositionQuery(make([]byte, 9)); err == nil {
		t.Error("DecodePositionQuery accepted 9 bytes")
	}
	if _, err := DecodePositionQuery(binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint16(nil, 8192), 1)); err == nil {
		t.Error("DecodePositionQuery accepted slot 8192")
	}
	for _, state := range []uint8{0, 5} {
		if _, err := EncodePositionReply(PositionReply{Slot: 1, DB: 1, State: state}); err == nil {
			t.Errorf("EncodePositionReply accepted state %d", state)
		}
	}
}

// u33Mutate flips, truncates or extends a valid body.
func u33Mutate(rng *rand.Rand, valid []byte) []byte {
	out := append([]byte(nil), valid...)
	switch rng.IntN(4) {
	case 0:
		out[rng.IntN(len(out))] ^= byte(1 << rng.IntN(8))
	case 1:
		out = out[:rng.IntN(len(out))]
	case 2:
		out = append(out, byte(rng.IntN(256)))
	default:
		for range 1 + rng.IntN(4) {
			out[rng.IntN(len(out))] = byte(rng.IntN(256))
		}
	}
	return out
}

func TestHandoffDecodersSurviveMutation(t *testing.T) {
	bitmap := u33Bitmap(t, 1, 9, 4000)
	pos := cluster.Position{Epoch: 4, Seq: 9}
	reqRaw, _ := EncodeHandoffReq(HandoffReq{FromEpoch: 3, ToEpoch: 4, Bitmap: bitmap})
	stoppedRaw, _ := EncodeHandoffStopped(HandoffStopped{ToEpoch: 4, Bitmap: bitmap})
	readyRaw, _ := EncodeHandoffReady(HandoffReady{ToEpoch: 4, Bitmap: bitmap, AppliedPos: pos})
	queryRaw, _ := EncodePositionQuery(PositionQuery{Slot: 12, DB: 5})
	replyRaw, _ := EncodePositionReply(PositionReply{Slot: 12, DB: 5, State: 2, AppliedPos: pos, AppliedCursor: 8})
	rng := rand.New(rand.NewPCG(33, 33))
	accepted := 0
	for range 100_000 {
		switch rng.IntN(5) {
		case 0:
			raw := u33Mutate(rng, reqRaw)
			if msg, err := DecodeHandoffReq(raw); err == nil {
				accepted++
				if len(msg.Bitmap) > len(raw) {
					t.Fatalf("decoded %d bitmap bytes from %d input bytes", len(msg.Bitmap), len(raw))
				}
				if again, err := EncodeHandoffReq(msg); err != nil || !bytes.Equal(again, raw) {
					t.Fatalf("HANDOFF_REQ %x re-encodes as %x, %v", raw, again, err)
				}
			}
		case 1:
			raw := u33Mutate(rng, stoppedRaw)
			if msg, err := DecodeHandoffStopped(raw); err == nil {
				accepted++
				if again, err := EncodeHandoffStopped(msg); err != nil || !bytes.Equal(again, raw) {
					t.Fatalf("HANDOFF_STOPPED %x re-encodes as %x, %v", raw, again, err)
				}
			}
		case 2:
			raw := u33Mutate(rng, readyRaw)
			if msg, err := DecodeHandoffReady(raw); err == nil {
				accepted++
				if again, err := EncodeHandoffReady(msg); err != nil || !bytes.Equal(again, raw) {
					t.Fatalf("HANDOFF_READY %x re-encodes as %x, %v", raw, again, err)
				}
			}
		case 3:
			raw := u33Mutate(rng, queryRaw)
			if msg, err := DecodePositionQuery(raw); err == nil {
				accepted++
				if again, err := EncodePositionQuery(msg); err != nil || !bytes.Equal(again, raw) {
					t.Fatalf("POSITION_QUERY %x re-encodes as %x, %v", raw, again, err)
				}
			}
		default:
			raw := u33Mutate(rng, replyRaw)
			if msg, err := DecodePositionReply(raw); err == nil {
				accepted++
				if again, err := EncodePositionReply(msg); err != nil || !bytes.Equal(again, raw) {
					t.Fatalf("POSITION_REPLY %x re-encodes as %x, %v", raw, again, err)
				}
			}
		}
	}
	if accepted == 0 {
		t.Fatal("no mutated body was accepted, so the re-encode check never ran")
	}
}

func TestHandoffDecodeAllocationIsBoundedByTheInput(t *testing.T) {
	// A header that claims 1,024 bitmap bytes over an 11-byte body must be refused
	// before anything of that size is allocated.
	liar := append(binary.BigEndian.AppendUint64(nil, 1), 0x04, 0x00, 1)
	const runs = 200
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range runs {
		if _, err := DecodeHandoffStopped(liar); err == nil {
			t.Fatal("accepted a bitmap longer than the body")
		}
	}
	runtime.ReadMemStats(&after)
	if perCall := (after.TotalAlloc - before.TotalAlloc) / runs; perCall >= 1024 {
		t.Fatalf("refusing a lying length allocated %d bytes per call, as much as the claimed bitmap", perCall)
	}
}
