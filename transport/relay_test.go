package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func relaySamples() []ControlMessage {
	return []ControlMessage{
		RelayProposal{RequestID: [RelayRequestIDLen]byte{1, 2, 3}, Kind: 0x0E, Body: []byte{5, 0, 0, 0, 0}},
		RelayProposal{Kind: 0x05, Body: bytes.Repeat([]byte{7}, 300)},
		RelayReply{Status: RelayApplied, Epoch: 4, StateSeq: 90, EpochMoved: true},
		RelayReply{Status: RelayApplied, Epoch: 4, StateSeq: 91},
		RelayReply{Status: RelayNotLeader, Leader: "n2"},
		RelayReply{Status: RelayNotLeader},
		RelayReply{Status: RelayNoQuorum},
		RelayReply{Status: RelayRefused, Code: 0x1C, Message: "STATE_MOVED: retry"},
		RelayReply{Status: RelayRefused, Code: 0x04},
	}
}

func TestRelayMessagesRoundTripOnTheControlChannel(t *testing.T) {
	for _, msg := range relaySamples() {
		raw, err := EncodeControl(Envelope{Epoch: 3, StateSeq: 4}, msg)
		if err != nil {
			t.Fatalf("%T: encode: %v", msg, err)
		}
		env, got, err := DecodeControl(raw)
		if err != nil {
			t.Fatalf("%T: decode: %v", msg, err)
		}
		if !reflect.DeepEqual(got, msg) {
			t.Fatalf("decoded %+v, want %+v", got, msg)
		}
		if env.Kind != KindRelayProposal && env.Kind != KindRelayProposalReply {
			t.Fatalf("%T encodes kind 0x%02x", msg, env.Kind)
		}
		if home, ok := KindChannel(env.Kind); !ok || home != ChannelControl {
			t.Fatalf("kind 0x%02x is on %v, %v; want the control channel", env.Kind, home, ok)
		}
		if err := CheckKind(env.Kind, ChannelData); err == nil {
			t.Fatalf("kind 0x%02x accepted on the data channel", env.Kind)
		}
	}
}

func TestRelayProposalLayoutIsTheSpecs(t *testing.T) {
	raw, err := (RelayProposal{RequestID: [16]byte{9}, Kind: 0x0E, Body: []byte{1, 2}}).appendBody(nil)
	if err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte{9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0x0E, 0, 0, 0, 2), 1, 2)
	if !bytes.Equal(raw, want) {
		t.Fatalf("layout % x, want % x", raw, want)
	}
}

func TestRelayRefusesMalformedBodies(t *testing.T) {
	good, err := (RelayProposal{Kind: 1, Body: []byte{1, 2, 3}}).appendBody(nil)
	if err != nil {
		t.Fatal(err)
	}
	withLen := func(n uint32) []byte {
		raw := bytes.Clone(good)
		binary.BigEndian.PutUint32(raw[RelayRequestIDLen+1:], n)
		return raw
	}
	oversize := make([]byte, relayProposalFixed+MaxRelayBody+1)
	oversize[RelayRequestIDLen] = 1
	binary.BigEndian.PutUint32(oversize[RelayRequestIDLen+1:], MaxRelayBody+1)
	zeroKind := bytes.Clone(good)
	zeroKind[RelayRequestIDLen] = 0
	for name, body := range map[string][]byte{
		"empty": nil, "short header": good[:10], "zero kind": zeroKind, "zero length": withLen(0),
		"length over the bytes": withLen(4), "length under the bytes": withLen(2),
		"length over the cap": withLen(MaxRelayBody + 1), "body over the cap carried in full": oversize,
		"length of four gigabytes": withLen(0xFFFFFFFF), "trailing byte": append(bytes.Clone(good), 0),
	} {
		if _, err := decodeRelayProposal(body); !errors.Is(err, ErrControlBody) {
			t.Fatalf("%s: %v, want ErrControlBody", name, err)
		}
	}
	for name, body := range map[string][]byte{
		"empty": nil, "unknown status": {9},
		"applied short":        {0, 1, 2},
		"not leader no length": {1}, "not leader short hint": {1, 5, 'a'}, "not leader trailing": {1, 1, 'a', 'b'},
		"no quorum trailing": {2, 0}, "refused short": {3, 0, 1}, "refused length over": {3, 0, 1, 0, 0, 0, 9, 'a'},
		"refused length cap": {3, 0, 1, 0xFF, 0xFF, 0xFF, 0xFF},
	} {
		if _, err := decodeRelayReply(body); !errors.Is(err, ErrControlBody) {
			t.Fatalf("%s: %v, want ErrControlBody", name, err)
		}
	}
	badMoved := append([]byte{0}, make([]byte, 17)...)
	badMoved[17] = 2
	if _, err := decodeRelayReply(badMoved); !errors.Is(err, ErrControlBody) {
		t.Fatalf("epochMoved byte 2: %v", err)
	}
}

func TestRelayEncodeRefusesWhatDecodeRefuses(t *testing.T) {
	for name, msg := range map[string]ControlMessage{
		"no kind": RelayProposal{Body: []byte{1}}, "no body": RelayProposal{Kind: 1}, "body over the cap": RelayProposal{Kind: 1, Body: make([]byte, MaxRelayBody+1)},
		"unknown status": RelayReply{Status: 7}, "long hint": RelayReply{Status: RelayNotLeader, Leader: strings.Repeat("n", 256)},
		"long text": RelayReply{Status: RelayRefused, Message: strings.Repeat("x", MaxRelayMessage+1)},
	} {
		if _, err := msg.appendBody(nil); !errors.Is(err, ErrControlBody) {
			t.Fatalf("%s: %v, want ErrControlBody", name, err)
		}
	}
}

// TestRelayDecoderMutation flips, drops and appends bytes of valid messages 100,000 times:
// no panic, and whatever the decoder accepts encodes back to the same bytes.
func TestRelayDecoderMutation(t *testing.T) {
	rng := rand.New(rand.NewSource(57))
	var seeds [][]byte
	for _, msg := range relaySamples() {
		raw, err := EncodeControl(Envelope{Epoch: 1, StateSeq: 2}, msg)
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, raw)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	accepted := 0
	for i := 0; i < 100_000; i++ {
		raw := bytes.Clone(seeds[rng.Intn(len(seeds))])
		for flips := rng.Intn(4); flips >= 0; flips-- {
			switch rng.Intn(5) {
			case 0:
				raw[rng.Intn(len(raw))] ^= byte(1 << rng.Intn(8))
			case 1:
				raw[rng.Intn(len(raw))] = byte(rng.Intn(256))
			case 2:
				raw = raw[:rng.Intn(len(raw)+1)]
			case 3:
				raw = append(raw, byte(rng.Intn(256)))
			case 4:
				if len(raw) > EnvelopeSize+5 {
					binary.BigEndian.PutUint32(raw[len(raw)-4:], rng.Uint32())
				}
			}
			if len(raw) == 0 {
				raw = []byte{0}
			}
		}
		env, msg, err := DecodeControl(raw)
		if err != nil {
			continue
		}
		accepted++
		again, err := EncodeControl(env, msg)
		if err != nil || !bytes.Equal(again, raw) {
			t.Fatalf("accepted % x re-encodes to % x (%v)", raw, again, err)
		}
	}
	runtime.ReadMemStats(&after)
	// Every length is checked before a copy, so 100,000 inputs of at most a few hundred bytes
	// cannot allocate anywhere near a megabyte each.
	if grown := after.TotalAlloc - before.TotalAlloc; grown > 400<<20 {
		t.Fatalf("100,000 mutations allocated %d MiB", grown>>20)
	}
	if accepted == 0 {
		t.Fatal("no mutated input was accepted: the round-trip half of the test checked nothing")
	}
}
