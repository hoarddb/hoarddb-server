package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestGetManyDecoderSurvivesStructuredFrames: seeded frames built from real key bodies with the
// count, the lengths and the tail bent in the ways a hostile client would. The decoder never
// panics, refuses with INVALID_ARGUMENT the same way twice, and re-encodes what it accepts to the
// same bytes. A small deterministic version of a 300,000-mutant review run.
func TestGetManyDecoderSurvivesStructuredFrames(t *testing.T) {
	reads := getManyReads()
	bodies := make([][]byte, len(reads))
	for i, read := range reads {
		bodies[i] = keyBody(t, read)
	}
	rng := rand.New(rand.NewPCG(7, 11))
	accepted := 0
	for iteration := range 3000 {
		count := rng.IntN(258)
		sent := count
		switch rng.IntN(4) {
		case 0:
			sent = max(count+rng.IntN(3)-1, 0)
		case 1:
			sent = rng.IntN(256)
		}
		frame := binary.BigEndian.AppendUint64(nil, rng.Uint64())
		frame = binary.BigEndian.AppendUint64(frame, rng.Uint64())
		frame = binary.BigEndian.AppendUint32(frame, rng.Uint32())
		frame = append(frame, byte(count))
		for range sent {
			body := bodies[rng.IntN(len(bodies))]
			length := len(body)
			switch rng.IntN(12) {
			case 0:
				length = rng.IntN(65536)
			case 1:
				length = 0
			case 2:
				length = len(body) - 1
			case 3:
				length = len(body) + 1
			}
			frame = binary.BigEndian.AppendUint16(frame, uint16(length))
			frame = append(frame, body...)
		}
		if rng.IntN(5) == 0 {
			frame = append(frame, make([]byte, rng.IntN(4))...)
		}
		if rng.IntN(7) == 0 {
			frame = frame[:rng.IntN(len(frame))]
		}
		decoded, err := DecodeRingGetMany(frame)
		if err != nil {
			var coded *cluster.Error
			if !errors.As(err, &coded) || coded.Code != cluster.CodeInvalidArgument {
				t.Fatalf("iteration %d: refusal is not INVALID_ARGUMENT: %v", iteration, err)
			}
			if _, again := DecodeRingGetMany(frame); again == nil || again.Error() != err.Error() {
				t.Fatalf("iteration %d: the refusal is not deterministic", iteration)
			}
			continue
		}
		accepted++
		if again, err := EncodeRingGetMany(decoded); err != nil || !bytes.Equal(again, frame) {
			t.Fatalf("iteration %d: an accepted frame re-encodes differently: %v", iteration, err)
		}
	}
	if accepted == 0 {
		t.Fatal("no generated frame was accepted, so the round trip was never exercised")
	}
}

// TestGetManyAFrameOfMaximumBodiesStillRoundTrips: 255 get_fields keys, each naming 64 fields of
// 256 bytes, encode and decode.
func TestGetManyAFrameOfMaximumBodiesStillRoundTrips(t *testing.T) {
	base := getManyReads()[1]
	base.Fields = nil
	for i := range 64 {
		base.Fields = append(base.Fields, fmt.Sprintf("%03d%s", i, strings.Repeat("f", 252)))
	}
	reads := make([]RingRead, maxGetManyKeys)
	for i := range reads {
		reads[i] = base
	}
	raw, err := EncodeRingGetMany(RingGetManyRequest{Prologue: u65Prologue(), Reads: reads})
	if err != nil {
		t.Fatalf("the encoder refused 255 keys of the largest legal bodies: %v", err)
	}
	if _, err := DecodeRingGetMany(raw); err != nil {
		t.Fatalf("the decoder refused what the encoder wrote (%d bytes): %v", len(raw), err)
	}
}
