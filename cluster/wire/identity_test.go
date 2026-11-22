package wire

import (
	"bytes"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

func identitySample() IdentityReply {
	return IdentityReply{
		Nonce: 0x0102030405060708, Incarnation: 1_700_000_000, Witness: true, HoldsRaftState: true,
		ClusterID: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		DataAddr:  "127.0.0.1:4443", NodeID: "node-b",
	}
}

// TestIdentityCodecSurvivesMutation: [GUARD 100000 mutations of a request and a reply: no panic,
// and an accepted body re-encodes to the same bytes].
func TestIdentityCodecSurvivesMutation(t *testing.T) {
	withClient := identitySample()
	withClient.ClientAddr = "127.0.0.1:7434"
	reply, err := EncodeIdentityReply(identitySample())
	if err != nil {
		t.Fatal(err)
	}
	replyWithClient, err := EncodeIdentityReply(withClient)
	if err != nil {
		t.Fatal(err)
	}
	full := withClient
	full.Range, full.Build = cluster.StateRange{Min: 1, Max: 2, Writes: 2}, "1.2.0-rc1"
	replyFull, err := EncodeIdentityReply(full)
	if err != nil {
		t.Fatal(err)
	}
	requests := [][]byte{EncodeIdentityRequest(IdentityRequest{Nonce: 7}), EncodeIdentityRequest(IdentityRequest{Nonce: 7, Admit: true})}
	rng := rand.New(rand.NewPCG(5, 5))
	var acceptedRequests, acceptedReplies int
	for range 100_000 {
		mutated := u65Mutate(rng, requests[rng.IntN(len(requests))])
		if request, err := DecodeIdentityRequest(mutated); err == nil {
			acceptedRequests++
			if again := EncodeIdentityRequest(request); !bytes.Equal(again, mutated) {
				t.Fatalf("accepted request %x re-encodes as %x", mutated, again)
			}
		}
		mutated = u65Mutate(rng, reply)
		switch rng.IntN(3) {
		case 0:
			mutated = u65Mutate(rng, replyWithClient)
		case 1:
			mutated = u65Mutate(rng, replyFull)
		}
		if got, err := DecodeIdentityReply(mutated); err == nil {
			acceptedReplies++
			if again, err := EncodeIdentityReply(got); err != nil || !bytes.Equal(again, mutated) {
				t.Fatalf("accepted reply %x re-encodes as %x, %v", mutated, again, err)
			}
		}
	}
	if acceptedRequests == 0 || acceptedReplies == 0 {
		t.Fatalf("accepted %d requests and %d replies: the re-encode check never ran", acceptedRequests, acceptedReplies)
	}
}

// TestTheIdentityRequestCodecRefusesUnknownFlagBits: [GUARD BUG-0870 guard, the request is
// [nonce:8][flags:1] with bit 0 the admit bit: the bytes are pinned, bits 1 to 7 are refused, and the
// 8-byte form of the earlier build is refused, not read as "no admit" (no compatibility before the
// first release)]. Red while the codec writes and reads the nonce alone.
func TestTheIdentityRequestCodecRefusesUnknownFlagBits(t *testing.T) {
	nonce := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	for _, tc := range []struct {
		admit bool
		flags byte
	}{{false, 0x00}, {true, 0x01}} {
		want := append(bytes.Clone(nonce), tc.flags)
		got := EncodeIdentityRequest(IdentityRequest{Nonce: 0x0102030405060708, Admit: tc.admit})
		if !bytes.Equal(got, want) {
			t.Fatalf("admit %v encodes as %x, want %x", tc.admit, got, want)
		}
		decoded, err := DecodeIdentityRequest(want)
		if err != nil {
			t.Fatalf("admit %v: %x refused: %v", tc.admit, want, err)
		}
		if decoded != (IdentityRequest{Nonce: 0x0102030405060708, Admit: tc.admit}) {
			t.Fatalf("%x decodes as %+v", want, decoded)
		}
	}
	for bit := 1; bit < 8; bit++ {
		body := append(bytes.Clone(nonce), byte(1)<<bit)
		if _, err := DecodeIdentityRequest(body); err == nil {
			t.Errorf("flag bit %d accepted: %x", bit, body)
		} else if !strings.Contains(err.Error(), "flag bit") {
			t.Errorf("flag bit %d: the refusal does not name the flag bits: %v", bit, err)
		}
	}
	if _, err := DecodeIdentityRequest(nonce); err == nil {
		t.Error("the 8-byte request of the earlier build accepted")
	}
}

// TestIdentityReplyLayout pins the bytes of conventions section 6.1 for IDENTITY_REPLY.
func TestIdentityReplyLayout(t *testing.T) {
	raw, err := EncodeIdentityReply(identitySample())
	if err != nil {
		t.Fatal(err)
	}
	want := append([]byte{1, 2, 3, 4, 5, 6, 7, 8}, 0, 0, 0, 0, 0x65, 0x53, 0xF1, 0x00) // nonce, incarnation
	want = append(want, 1, 1)                                                          // witness, flags
	want = append(want, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
	want = append(want, byte(len("127.0.0.1:4443")))
	want = append(want, "127.0.0.1:4443"...)
	want = append(want, byte(len("node-b")))
	want = append(want, "node-b"...)
	if !bytes.Equal(raw, want) {
		t.Fatalf("reply is %x, want %x", raw, want)
	}
}

// TestAnIdentityReplyCarriesAnOptionalClientAddr pins the bytes of an IDENTITY_REPLY with flag
// bit 1: the client address follows the node id behind its own length byte, and a reply without
// one is byte-identical to the reply before the field existed.
// [BREAK: drop the client address from EncodeIdentityReply]
func TestAnIdentityReplyCarriesAnOptionalClientAddr(t *testing.T) {
	plain, err := EncodeIdentityReply(identitySample())
	if err != nil {
		t.Fatal(err)
	}
	reply := identitySample()
	reply.ClientAddr = "db1.example:7434"
	raw, err := EncodeIdentityReply(reply)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Clone(plain)
	want[8+8+1] |= 1 << 1
	want = append(want, byte(len("db1.example:7434")))
	want = append(want, "db1.example:7434"...)
	if !bytes.Equal(raw, want) {
		t.Fatalf("reply is %x, want %x", raw, want)
	}
	got, err := DecodeIdentityReply(raw)
	if err != nil || got.ClientAddr != "db1.example:7434" {
		t.Fatalf("decoded %+v, %v", got, err)
	}
	// Flag set with no text, text with no flag, and a client address of 256 bytes are all refused.
	flagOnly := bytes.Clone(plain)
	flagOnly[8+8+1] |= 1 << 1
	if _, err := DecodeIdentityReply(flagOnly); err == nil {
		t.Error("flag bit 1 with no client address accepted")
	}
	noFlag := bytes.Clone(raw)
	noFlag[8+8+1] &^= 1 << 1
	if _, err := DecodeIdentityReply(noFlag); err == nil {
		t.Error("a client address with flag bit 1 clear accepted")
	}
	reply.ClientAddr = string(bytes.Repeat([]byte("a"), 256))
	if _, err := EncodeIdentityReply(reply); err == nil {
		t.Error("a 256-byte client address encoded")
	}
	longest := IdentityReply{Nonce: 1, Incarnation: 1, NodeID: string(bytes.Repeat([]byte("n"), 255)), DataAddr: string(bytes.Repeat([]byte("d"), 255)), ClientAddr: string(bytes.Repeat([]byte("c"), 255)),
		Range: cluster.StateRange{Min: 1, Max: 2, Writes: 2}, Build: string(bytes.Repeat([]byte("b"), identityMaxBuild))}
	body, err := EncodeIdentityReply(longest)
	if err != nil || len(body) != identityMaxBytes {
		t.Fatalf("the longest reply is %d bytes (%v), the cap is %d", len(body), err, identityMaxBytes)
	}
}

// TestAnIdentityReplyWithAnUnknownFlagIsRefused: the flags byte has four defined bits; a node that
// sets another speaks a protocol this build does not know, and is refused rather than half read.
// [BREAK: drop the flags mask in DecodeIdentityReply].
func TestAnIdentityReplyWithAnUnknownFlagIsRefused(t *testing.T) {
	raw, err := EncodeIdentityReply(identitySample())
	if err != nil {
		t.Fatal(err)
	}
	const flagsAt = 8 + 8 + 1
	for bit := 4; bit < 8; bit++ {
		mutated := bytes.Clone(raw)
		mutated[flagsAt] |= 1 << bit
		if _, err := DecodeIdentityReply(mutated); err == nil {
			t.Errorf("flag bit %d accepted", bit)
		}
	}
	good := bytes.Clone(raw)
	good[flagsAt] = 0
	if got, err := DecodeIdentityReply(good); err != nil || got.HoldsRaftState {
		t.Fatalf("flags 0 decode as %+v, %v", got, err)
	}
}

func TestIdentityCodecRefusals(t *testing.T) {
	cases := map[string]func(*IdentityReply){
		"empty node id":      func(r *IdentityReply) { r.NodeID = "" },
		"256-byte node id":   func(r *IdentityReply) { r.NodeID = string(bytes.Repeat([]byte("a"), 256)) },
		"control in id":      func(r *IdentityReply) { r.NodeID = "n\n1" },
		"empty address":      func(r *IdentityReply) { r.DataAddr = "" },
		"invalid utf-8 addr": func(r *IdentityReply) { r.DataAddr = "h\xff:1" },
		"incarnation zero":   func(r *IdentityReply) { r.Incarnation = 0 },
		"incarnation <0":     func(r *IdentityReply) { r.Incarnation = -1 },
	}
	for name, mutate := range cases {
		reply := identitySample()
		mutate(&reply)
		if _, err := EncodeIdentityReply(reply); err == nil {
			t.Errorf("%s: encoded", name)
		}
	}
	raw, err := EncodeIdentityReply(identitySample())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeIdentityReply(append(bytes.Clone(raw), 0)); err == nil {
		t.Error("trailing byte accepted")
	}
	if _, err := DecodeIdentityReply(bytes.Repeat([]byte{0}, identityMaxBytes+1)); err == nil {
		t.Error("a body over the cap accepted")
	}
	witnessTwo := bytes.Clone(raw)
	witnessTwo[16] = 2
	if _, err := DecodeIdentityReply(witnessTwo); err == nil {
		t.Error("witness byte 2 accepted")
	}
	if _, err := DecodeIdentityRequest(make([]byte, 10)); err == nil {
		t.Error("a 10-byte request accepted")
	}
}

// TestAnIdentityReplyWithNoRangeIsLevelZero: a reply from a build that predates the range has no
// flag bit 2 and decodes with the zero range, which the join rule refuses by name.
func TestAnIdentityReplyWithNoRangeIsLevelZero(t *testing.T) {
	raw, err := EncodeIdentityReply(identitySample())
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeIdentityReply(raw)
	if err != nil || got.Range != (cluster.StateRange{}) || got.Build != "" {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestAnIdentityReplyRoundTripsItsRangeAndBuild(t *testing.T) {
	want := identitySample()
	want.ClientAddr = "127.0.0.1:7434"
	want.Range, want.Build = cluster.StateRange{Min: 3, Max: 4, Writes: 4}, "1.3.0"
	raw, err := EncodeIdentityReply(want)
	if err != nil {
		t.Fatal(err)
	}
	if raw[8+8+1]&(identityFlagRange|identityFlagBuild) != identityFlagRange|identityFlagBuild {
		t.Fatalf("flags %08b lack bits 2 and 3", raw[17])
	}
	got, err := DecodeIdentityReply(raw)
	if err != nil || got != want {
		t.Fatalf("got %+v, %v", got, err)
	}
	only := identitySample()
	only.Range = cluster.StateRange{Min: 1, Max: 1, Writes: 1}
	raw, _ = EncodeIdentityReply(only)
	if got, err := DecodeIdentityReply(raw); err != nil || got != only {
		t.Fatalf("range without a build: %+v, %v", got, err)
	}
	tooLong := identitySample()
	tooLong.Build = string(bytes.Repeat([]byte("x"), identityMaxBuild+1))
	if _, err := EncodeIdentityReply(tooLong); err == nil {
		t.Fatal("a 65-byte build was encoded")
	}
	// The range flag over three zero bytes is not canonical: the encoder would drop it.
	zero, _ := EncodeIdentityReply(identitySample())
	zero[17] |= identityFlagRange
	zero = append(zero, 0, 0, 0)
	if _, err := DecodeIdentityReply(zero); err == nil {
		t.Fatal("a range flag over zeros was accepted")
	}
}

func TestAMalformedIdentityRangeIsRefused(t *testing.T) {
	for name, r := range map[string]cluster.StateRange{"min above max": {Min: 2, Max: 1, Writes: 1}, "writes above max": {Min: 1, Max: 1, Writes: 2}, "zero min": {Min: 0, Max: 1, Writes: 1}} {
		reply := identitySample()
		reply.Range = r
		if _, err := EncodeIdentityReply(reply); err == nil {
			t.Errorf("%s: encoded", name)
		}
		good := identitySample()
		good.Range = cluster.StateRange{Min: 1, Max: 2, Writes: 2}
		raw, _ := EncodeIdentityReply(good)
		raw[len(raw)-3], raw[len(raw)-2], raw[len(raw)-1] = r.Min, r.Max, r.Writes
		if _, err := DecodeIdentityReply(raw); err == nil {
			t.Errorf("%s: decoded", name)
		}
	}
}
