package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"net"
	"testing"
	"time"
)

func controlSamples() []ControlMessage {
	return []ControlMessage{
		LeaseReq{Nonce: 7},
		Probe{Nonce: 8},
		ProbeReply{Nonce: 9, State: 2, AppliedStateSeq: 1234},
		LeaseGrant{Nonce: 10, GrantEpoch: 5, GrantTerm: 9, RTTMicros: 1500},
		FailureReport{Subject: "node-c", FirstFailureAtMillis: 1_700_000_000_000},
		ClusterNotice{Epoch: 6, StateSeq: 77},
	}
}

func TestControlMessagesRoundTrip(t *testing.T) {
	for _, msg := range controlSamples() {
		env := Envelope{Epoch: 3, StateSeq: 4}
		raw, err := EncodeControl(env, msg)
		if err != nil {
			t.Fatalf("%T: encode: %v", msg, err)
		}
		gotEnv, gotMsg, err := DecodeControl(raw)
		if err != nil {
			t.Fatalf("%T: decode: %v", msg, err)
		}
		if gotMsg != msg {
			t.Fatalf("%T: decoded %+v, want %+v", msg, gotMsg, msg)
		}
		if gotEnv.Epoch != 3 || gotEnv.StateSeq != 4 || gotEnv.Version != EnvelopeVersion {
			t.Fatalf("%T: envelope %+v lost its epoch or state_seq", msg, gotEnv)
		}
	}
}

func TestControlKindsAreOnTheControlChannel(t *testing.T) {
	wantKinds := map[byte]ControlMessage{
		0x08: LeaseReq{}, 0x09: Probe{}, 0x0A: ProbeReply{},
		0x0B: LeaseGrant{}, 0x0C: FailureReport{Subject: "a"}, 0x0D: ClusterNotice{},
	}
	for kind, msg := range wantKinds {
		raw, err := EncodeControl(Envelope{}, msg)
		if err != nil {
			t.Fatalf("%T: %v", msg, err)
		}
		if raw[1] != kind {
			t.Fatalf("%T encodes kind 0x%02x, want 0x%02x", msg, raw[1], kind)
		}
	}
}

func TestLeaseGrantLayoutIsTheSpecs(t *testing.T) {
	raw, err := EncodeControl(Envelope{}, LeaseGrant{Nonce: 1, GrantEpoch: 2, GrantTerm: 3, RTTMicros: 4})
	if err != nil {
		t.Fatal(err)
	}
	body := raw[EnvelopeSize:]
	if len(body) != 28 {
		t.Fatalf("LEASE_GRANT body is %d bytes, want 28 ([nonce:8][grantEpoch:8][grantTerm:8][rttMicros:4])", len(body))
	}
	if binary.BigEndian.Uint64(body[16:24]) != 3 {
		t.Fatalf("grantTerm is not at body offset 16: % x", body)
	}
	if binary.BigEndian.Uint32(body[24:28]) != 4 {
		t.Fatalf("rttMicros is not at body offset 24: % x", body)
	}
}

func TestNegativeRTTIsRejectedOnDecode(t *testing.T) {
	raw, err := EncodeControl(Envelope{}, LeaseGrant{Nonce: 1, GrantEpoch: 2, GrantTerm: 3, RTTMicros: 4})
	if err != nil {
		t.Fatal(err)
	}
	negative := bytes.Clone(raw)
	binary.BigEndian.PutUint32(negative[EnvelopeSize+24:], 0xFFFFFFFF) // -1 as int32
	if _, _, err := DecodeControl(negative); !errors.Is(err, ErrControlBody) {
		t.Fatalf("a grant with rttMicros -1 decoded: %v; a corrupt negative rtt would extend the lease", err)
	}
	if _, err := EncodeControl(Envelope{}, LeaseGrant{RTTMicros: -1}); err == nil {
		t.Fatal("encoding a negative rttMicros succeeded")
	}
}

func TestControlRefusesAKindFromAnotherChannel(t *testing.T) {
	for _, kind := range []byte{0x01, 0x0E, KindHandoffStopped, 0x25} {
		raw := append(EncodeEnvelope(Envelope{Version: EnvelopeVersion, Kind: kind}), make([]byte, 8)...)
		if _, _, err := DecodeControl(raw); !errors.Is(err, ErrInvalidKind) {
			t.Fatalf("kind 0x%02x decoded as a control message: %v", kind, err)
		}
	}
}

func TestControlRefusesWrongBodyLengthAndEmptySubject(t *testing.T) {
	raw, err := EncodeControl(Envelope{}, Probe{Nonce: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{raw[:len(raw)-1], append(bytes.Clone(raw), 0)} {
		if _, _, err := DecodeControl(bad); !errors.Is(err, ErrControlBody) {
			t.Fatalf("a PROBE body of the wrong length decoded: %v", err)
		}
	}
	if _, err := EncodeControl(Envelope{}, FailureReport{}); err == nil {
		t.Fatal("a FAILURE_REPORT with no subject encoded")
	}
	empty := append(EncodeEnvelope(Envelope{Version: EnvelopeVersion, Kind: 0x0C}), 0, 0, 0, 0, 0, 0, 0, 0, 0)
	if _, _, err := DecodeControl(empty); !errors.Is(err, ErrControlBody) {
		t.Fatalf("a FAILURE_REPORT with subjectLen 0 decoded: %v", err)
	}
}

// TestControlDecoderMutation flips, drops and appends bytes of valid
// messages 20,000 times: the decoder must not panic, and whatever it accepts
// must encode back to the same bytes.
func TestControlDecoderMutation(t *testing.T) {
	rng := rand.New(rand.NewSource(27))
	var seeds [][]byte
	for _, msg := range controlSamples() {
		raw, err := EncodeControl(Envelope{Epoch: 1, StateSeq: 2}, msg)
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, raw)
	}
	accepted := 0
	for i := 0; i < 20000; i++ {
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

func TestControlOpcodesAreTheProbePair(t *testing.T) {
	if OpClusterProbe != 0x9A || OpClusterProbeReply != 0x9B {
		t.Fatalf("control opcodes are 0x%02X/0x%02X, want 0x9A/0x9B", byte(OpClusterProbe), byte(OpClusterProbeReply))
	}
	for _, op := range []Opcode{OpClusterProbe, OpClusterProbeReply} {
		if !isNodeToNodeOpcode(op) {
			t.Fatalf("%s is not reserved for internode traffic: a client listener would serve it", op)
		}
	}
}

func startChannelServer(t *testing.T, ch Channel, handle ChannelHandler) *ChannelServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := ServeChannel(ln, ch, nil, handle)
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("closing the channel server: %v", err)
		}
	})
	return server
}

func testHello(id string) func() Handshake {
	return func() Handshake { return Handshake{NodeID: id, Epoch: 1} }
}

func TestChannelCallReturnsTheReplyAnyEpoch(t *testing.T) {
	var sawEpoch uint64
	server := startChannelServer(t, ChannelControl, func(_ context.Context, from Handshake, req Message) (*Message, error) {
		sawEpoch = req.Env.Epoch
		if from.NodeID != "caller" {
			t.Errorf("handler saw peer %q", from.NodeID)
		}
		reply := Message{Env: Envelope{Version: EnvelopeVersion, Kind: KindProbeReply, Epoch: 900}, Body: make([]byte, 17)}
		return &reply, nil
	})
	client := NewChannelClient(server.Addr().String(), ChannelControl, testHello("caller"))
	defer func() { _ = client.Close() }() // best-effort test cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reply, err := client.Call(ctx, Message{Env: Envelope{Version: EnvelopeVersion, Kind: KindProbe, Epoch: 2}, Body: make([]byte, 8)})
	if err != nil || reply == nil {
		t.Fatalf("call: %v, %v", reply, err)
	}
	if sawEpoch != 2 || reply.Env.Epoch != 900 || reply.Env.Kind != KindProbeReply {
		t.Fatalf("epochs 2 and 900 were fenced or lost: handler saw %d, reply %+v", sawEpoch, reply.Env)
	}
}

func TestChannelCallWithNoReplyIsAnAck(t *testing.T) {
	server := startChannelServer(t, ChannelControl, func(context.Context, Handshake, Message) (*Message, error) { return nil, nil })
	client := NewChannelClient(server.Addr().String(), ChannelControl, testHello("caller"))
	defer func() { _ = client.Close() }() // best-effort test cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ { // the second call proves the first left the stream aligned
		reply, err := client.Call(ctx, Message{Env: Envelope{Version: EnvelopeVersion, Kind: KindClusterNotice}, Body: make([]byte, 16)})
		if err != nil || reply != nil {
			t.Fatalf("call %d: reply %v, err %v; want an ack with no message", i, reply, err)
		}
	}
}

func TestChannelRefusesAKindOnTheWrongChannel(t *testing.T) {
	server := startChannelServer(t, ChannelControl, func(context.Context, Handshake, Message) (*Message, error) {
		t.Error("the handler ran for a data kind on the control channel")
		return nil, nil
	})
	client := NewChannelClient(server.Addr().String(), ChannelControl, testHello("caller"))
	defer func() { _ = client.Close() }() // best-effort test cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.Call(ctx, Message{Env: Envelope{Version: EnvelopeVersion, Kind: 0x01}})
	var wire *WireError
	if !errors.As(err, &wire) || wire.Code != ErrInvalidArgument {
		t.Fatalf("WRITE on the control channel: %v, want an INVALID_ARGUMENT wire error", err)
	}
}

func TestChannelRefusesAHandshakeForAnotherChannel(t *testing.T) {
	server := startChannelServer(t, ChannelControl, func(context.Context, Handshake, Message) (*Message, error) { return nil, nil })
	client := NewChannelClient(server.Addr().String(), ChannelData, testHello("caller"))
	defer func() { _ = client.Close() }() // best-effort test cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Call(ctx, Message{Env: Envelope{Version: EnvelopeVersion, Kind: 0x01}}); err == nil {
		t.Fatal("a data-channel handshake was served by the control listener")
	}
}

func TestChannelHandlerErrorReachesTheCaller(t *testing.T) {
	server := startChannelServer(t, ChannelControl, func(context.Context, Handshake, Message) (*Message, error) {
		return nil, errors.New("no lease to give")
	})
	client := NewChannelClient(server.Addr().String(), ChannelControl, testHello("caller"))
	defer func() { _ = client.Close() }() // best-effort test cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.Call(ctx, Message{Env: Envelope{Version: EnvelopeVersion, Kind: KindLeaseReq}, Body: make([]byte, 8)})
	var wire *WireError
	if !errors.As(err, &wire) || wire.Message != "no lease to give" {
		t.Fatalf("handler error arrived as %v", err)
	}
}

func TestChannelCallEndsWithItsContext(t *testing.T) {
	release := make(chan struct{})
	server := startChannelServer(t, ChannelControl, func(ctx context.Context, _ Handshake, _ Message) (*Message, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, nil
	})
	defer close(release)
	client := NewChannelClient(server.Addr().String(), ChannelControl, testHello("caller"))
	defer func() { _ = client.Close() }() // best-effort test cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := client.Call(ctx, Message{Env: Envelope{Version: EnvelopeVersion, Kind: KindProbe}, Body: make([]byte, 8)}); err == nil {
		t.Fatal("a call to a silent peer returned without an error")
	}
}

// TestChannelCloseEndsACallWithNoDeadline: a hung peer and a call whose
// context never ends must not make Close wait forever.
func TestChannelCloseEndsACallWithNoDeadline(t *testing.T) {
	entered := make(chan struct{}, 1)
	server := startChannelServer(t, ChannelControl, func(ctx context.Context, _ Handshake, _ Message) (*Message, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return nil, nil
	})
	client := NewChannelClient(server.Addr().String(), ChannelControl, testHello("caller"))
	called := make(chan error, 1)
	go func() {
		_, err := client.Call(context.Background(), Message{Env: Envelope{Version: EnvelopeVersion, Kind: KindProbe}, Body: make([]byte, 8)})
		called <- err
	}()
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- client.Close() }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close is still waiting behind a call that has no deadline")
	}
	select {
	case err := <-called:
		if err == nil {
			t.Fatal("the abandoned call returned success")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the in-flight call outlived Close")
	}
}
