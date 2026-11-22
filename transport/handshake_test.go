package transport

import (
	"bytes"
	"math/rand"
	"testing"
)

func handshakeSample() Handshake {
	return Handshake{
		Channel:     HandshakeChannelRaft,
		ClusterID:   [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Epoch:       7,
		StateSeq:    42,
		SentAtNs:    1_700_000_000_000_000_000,
		Incarnation: 3,
		NodeID:      "node-b",
		Min:         2, Max: 5, Writes: 4,
	}
}

func TestHandshakeLayout(t *testing.T) {
	raw, err := handshakeSample().Encode()
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != HandshakeBytes+6 {
		t.Fatalf("handshake is %d bytes, want 60 plus the 6-byte node id", len(raw))
	}
	want := map[string][]byte{
		"magic":       raw[0:4],
		"major":       raw[4:5],
		"channel":     raw[5:6],
		"min":         raw[6:7],
		"writes":      raw[7:8],
		"cluster_id":  raw[8:24],
		"epoch":       raw[24:32],
		"state_seq":   raw[32:40],
		"nodeIDLen":   raw[48:50],
		"incarnation": raw[50:58],
		"max":         raw[58:59],
		"reserved":    raw[59:60],
		"node id":     raw[60:],
	}
	exact := map[string][]byte{
		"magic":       []byte("HDPX"),
		"major":       {1},
		"channel":     {HandshakeChannelRaft},
		"min":         {2},
		"writes":      {4},
		"cluster_id":  {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		"epoch":       {0, 0, 0, 0, 0, 0, 0, 7},
		"state_seq":   {0, 0, 0, 0, 0, 0, 0, 42},
		"nodeIDLen":   {0, 6},
		"incarnation": {0, 0, 0, 0, 0, 0, 0, 3},
		"max":         {5},
		"reserved":    {0},
		"node id":     []byte("node-b"),
	}
	for field, got := range want {
		if !bytes.Equal(got, exact[field]) {
			t.Errorf("%s: got %x want %x", field, got, exact[field])
		}
	}
}

func TestHandshakeRoundTrip(t *testing.T) {
	raw, err := handshakeSample().Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeHandshake(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got != handshakeSample() {
		t.Fatalf("got %+v", got)
	}
}

func TestHandshakeRefusals(t *testing.T) {
	good, _ := handshakeSample().Encode()
	cases := map[string]func() []byte{
		"short":         func() []byte { return good[:59] },
		"truncated id":  func() []byte { return good[:len(good)-1] },
		"trailing":      func() []byte { return append(bytes.Clone(good), 'x') },
		"magic":         func() []byte { b := bytes.Clone(good); b[0] = 'Q'; return b },
		"major":         func() []byte { b := bytes.Clone(good); b[4] = 2; return b },
		"channel 0":     func() []byte { b := bytes.Clone(good); b[5] = 0; return b },
		"channel 9":     func() []byte { b := bytes.Clone(good); b[5] = 9; return b },
		"reserved":      func() []byte { b := bytes.Clone(good); b[59] = 1; return b },
		"empty node id": func() []byte { b := bytes.Clone(good[:60]); b[48], b[49] = 0, 0; return b },
	}
	for name, build := range cases {
		if _, err := DecodeHandshake(build()); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestHandshakeDecoderMutations(t *testing.T) {
	seed, _ := handshakeSample().Encode()
	rng := rand.New(rand.NewSource(21))
	for i := 0; i < 20000; i++ {
		raw := bytes.Clone(seed)
		for j := rng.Intn(4); j >= 0 && len(raw) > 0; j-- {
			switch rng.Intn(3) {
			case 0:
				raw[rng.Intn(len(raw))] ^= byte(1 << rng.Intn(8))
			case 1:
				raw = raw[:rng.Intn(len(raw)+1)]
			case 2:
				raw = append(raw, byte(rng.Intn(256)))
			}
		}
		h, err := DecodeHandshake(raw)
		if err != nil {
			continue
		}
		again, err := h.Encode()
		if err != nil || !bytes.Equal(again, raw) {
			t.Fatalf("mutation %d accepted but does not re-encode to itself: %v", i, err)
		}
	}
}

func TestStaleIncarnationIsOlderThanCommittedOnly(t *testing.T) {
	h := handshakeSample() // incarnation 3
	if err := h.CheckIncarnation(3); err != nil {
		t.Errorf("equal incarnation refused: %v", err)
	}
	if err := h.CheckIncarnation(2); err != nil {
		t.Errorf("newer incarnation refused: %v", err)
	}
	if err := h.CheckIncarnation(4); err == nil {
		t.Error("older incarnation accepted")
	}
}

func TestAMalformedRangeIsRefusedByTheEncoderAndTheDecoder(t *testing.T) {
	for name, r := range map[string][3]uint8{"min above max": {2, 1, 1}, "writes below min": {2, 3, 1}, "writes above max": {1, 2, 3}, "zero min with a max": {0, 1, 1}, "zero writes": {1, 1, 0}} {
		h := handshakeSample()
		h.Min, h.Max, h.Writes = r[0], r[1], r[2]
		if _, err := h.Encode(); err == nil {
			t.Errorf("%s: the encoder wrote range %v", name, r)
		}
		// The decoder gets the bytes a malformed sender would put on the wire: a valid
		// header with its range bytes overwritten (6 is min, 7 writes, 58 max).
		raw, err := handshakeSample().Encode()
		if err != nil {
			t.Fatal(err)
		}
		raw[6], raw[7], raw[58] = r[0], r[2], r[1]
		if _, err := DecodeHandshake(raw); err == nil {
			t.Errorf("%s: range %v accepted", name, r)
		}
	}
}

func TestTheHandshakeRoundTripsItsRange(t *testing.T) {
	for _, r := range [][3]uint8{{0, 0, 0}, {1, 1, 1}, {1, 2, 1}, {1, 255, 255}, {3, 9, 4}} {
		h := handshakeSample()
		h.Min, h.Max, h.Writes = r[0], r[1], r[2]
		raw, err := h.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) != HandshakeBytes+len(h.NodeID) {
			t.Fatalf("a range changed the handshake size to %d bytes", len(raw))
		}
		if raw[6] != r[0] || raw[58] != r[1] || raw[7] != r[2] || raw[59] != 0 {
			t.Fatalf("range %v landed in bytes 6=%d 7=%d 58=%d 59=%d", r, raw[6], raw[7], raw[58], raw[59])
		}
		got, err := DecodeHandshake(raw)
		if err != nil || got != h {
			t.Fatalf("range %v: got %+v, %v", r, got, err)
		}
	}
	good, _ := handshakeSample().Encode()
	good[59] = 1
	if _, err := DecodeHandshake(good); err == nil {
		t.Fatal("byte 59 is reserved and must be zero")
	}
}
