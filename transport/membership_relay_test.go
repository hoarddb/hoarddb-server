package transport

import (
	"bytes"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func membershipRelaySamples() []ControlMessage {
	return []ControlMessage{
		MembershipRelay{RequestID: [RelayRequestIDLen]byte{1, 2, 3}, Cmd: MembershipJoin, NodeID: "node-b", DataAddr: "127.0.0.1:4443"},
		MembershipRelay{RequestID: [RelayRequestIDLen]byte{9}, Cmd: MembershipJoin, Witness: true, NodeID: "node-w", DataAddr: "h:1"},
		MembershipRelay{Cmd: MembershipPromote, NodeID: "node-c"},
		MembershipRelay{Cmd: MembershipActivate, NodeID: "node-c"},
		MembershipRelay{Cmd: MembershipReplaceNode, NodeID: "n2", DataAddr: "h:2"},
		MembershipRelay{Cmd: MembershipReplaceNode, AcceptLoss: true, NodeID: "n2", DataAddr: "h:2"},
		MembershipRelayReply{Status: MembershipDone, Epoch: 2, StateSeq: 7, Result: "Added node-b@h:2 as a member"},
		MembershipRelayReply{Status: MembershipNotLeader, Leader: "n1"},
		MembershipRelayReply{Status: MembershipNotLeader},
		MembershipRelayReply{Status: MembershipNoQuorum},
		MembershipRelayReply{Status: MembershipRefused, Code: 0x1D, Message: "node n2 is already a member at h:2"},
	}
}

func TestMembershipRelayMessagesRoundTripOnTheControlChannel(t *testing.T) {
	for _, msg := range membershipRelaySamples() {
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
		if env.Kind != KindMembershipRelay && env.Kind != KindMembershipRelayReply {
			t.Fatalf("%T encodes kind 0x%02x", msg, env.Kind)
		}
		if err := CheckKind(env.Kind, ChannelData); err == nil {
			t.Fatalf("kind 0x%02x accepted on the data channel", env.Kind)
		}
	}
}

// TestMembershipRelayLayoutIsThePlans pins the bytes of plan section 2.1.
func TestMembershipRelayLayoutIsThePlans(t *testing.T) {
	raw, err := (MembershipRelay{RequestID: [16]byte{9}, Cmd: MembershipJoin, Witness: true, NodeID: "ab", DataAddr: "h:1"}).appendBody(nil)
	if err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte{9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 1, 1, 2), "ab"...)
	want = append(append(want, 3), "h:1"...)
	if !bytes.Equal(raw, want) {
		t.Fatalf("request layout % x, want % x", raw, want)
	}
	// BUG-0870: the flags byte's bit 1 is replace_node's accept_loss.
	raw, err = (MembershipRelay{RequestID: [16]byte{9}, Cmd: MembershipReplaceNode, AcceptLoss: true, NodeID: "ab", DataAddr: "h:1"}).appendBody(nil)
	if err != nil {
		t.Fatal(err)
	}
	want = append(append([]byte{9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 5, 2, 2), "ab"...)
	want = append(append(want, 3), "h:1"...)
	if !bytes.Equal(raw, want) {
		t.Fatalf("replace request layout % x, want % x", raw, want)
	}
	raw, err = (MembershipRelayReply{Status: MembershipDone, Epoch: 2, StateSeq: 3, Result: "ok"}).appendBody(nil)
	if err != nil {
		t.Fatal(err)
	}
	want = []byte{0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 2, 'o', 'k'}
	if !bytes.Equal(raw, want) {
		t.Fatalf("DONE layout % x, want % x", raw, want)
	}
}

func TestMembershipRelayRefusesMalformedBodies(t *testing.T) {
	bad := map[string]MembershipRelay{
		"no command":               {NodeID: "n2"},
		"command 7":                {Cmd: 7, NodeID: "n2"},
		"empty node id":            {Cmd: MembershipPromote},
		"256-byte node id":         {Cmd: MembershipPromote, NodeID: strings.Repeat("a", 256)},
		"control in id":            {Cmd: MembershipPromote, NodeID: "n\n2"},
		"join without an address":  {Cmd: MembershipJoin, NodeID: "n2"},
		"promote with an address":  {Cmd: MembershipPromote, NodeID: "n2", DataAddr: "h:2"},
		"witness on a promote":     {Cmd: MembershipPromote, NodeID: "n2", Witness: true},
		"accept_loss on a join":    {Cmd: MembershipJoin, NodeID: "n2", DataAddr: "h:2", AcceptLoss: true},
		"witness on a replace":     {Cmd: MembershipReplaceNode, NodeID: "n2", DataAddr: "h:2", Witness: true},
		"activate with an address": {Cmd: MembershipActivate, NodeID: "n2", DataAddr: "h:2"},
	}
	for name, msg := range bad {
		if _, err := msg.appendBody(nil); err == nil {
			t.Errorf("%s: encoded", name)
		}
	}
	good, err := (MembershipRelay{Cmd: MembershipJoin, NodeID: "n2", DataAddr: "h:2"}).appendBody(nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string][]byte{
		"empty":                 nil,
		"trailing":              append(bytes.Clone(good), 0),
		"truncated":             good[:len(good)-1],
		"accept_loss on a join": append(append(bytes.Clone(good[:17]), 2), good[18:]...),
		"flags bit 2":           append(append(bytes.Clone(good[:17]), 4), good[18:]...),
	} {
		if _, err := decodeControlBody(KindMembershipRelay, body); err == nil {
			t.Errorf("request %s: accepted", name)
		}
	}
	for name, body := range map[string][]byte{
		"empty":             nil,
		"status 4":          {4},
		"DONE short":        {0, 1, 2},
		"DONE trailing":     append(mustBody(t, MembershipRelayReply{Status: MembershipDone, Result: "ok"}), 0),
		"DONE empty result": mustBody(t, MembershipRelayReply{Status: MembershipDone, Result: "x"})[:17+2],
		"NO_QUORUM extra":   {2, 0},
		"REFUSED short":     {3, 0, 1},
	} {
		if _, err := decodeControlBody(KindMembershipRelayReply, body); err == nil {
			t.Errorf("reply %s: accepted", name)
		}
	}
}

func mustBody(t *testing.T, msg ControlMessage) []byte {
	t.Helper()
	body, err := msg.appendBody(nil)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// TestMembershipRelayCodecSurvivesMutation: [GUARD 100000 mutations: no panic, and an accepted
// message re-encodes to the same bytes].
func TestMembershipRelayCodecSurvivesMutation(t *testing.T) {
	rng := rand.New(rand.NewSource(61))
	var seeds [][]byte
	for _, msg := range membershipRelaySamples() {
		raw, err := EncodeControl(Envelope{Epoch: 1, StateSeq: 2}, msg)
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, raw)
	}
	accepted := 0
	for range 100_000 {
		raw := bytes.Clone(seeds[rng.Intn(len(seeds))])
		for flips := rng.Intn(4); flips >= 0; flips-- {
			switch rng.Intn(4) {
			case 0:
				raw[rng.Intn(len(raw))] ^= byte(1 << rng.Intn(8))
			case 1:
				raw[rng.Intn(len(raw))] = byte(rng.Intn(256))
			case 2:
				raw = raw[:rng.Intn(len(raw)+1)]
			case 3:
				raw = append(raw, byte(rng.Intn(256)))
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
	if accepted == 0 {
		t.Fatal("no mutated input was accepted: the round-trip half of the test checked nothing")
	}
}
