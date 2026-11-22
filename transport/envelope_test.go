package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

// envelopeKinds is every kind of conventions 6.1, with its channel. It is
// written out here so a change to the registry table has to change a test.
var envelopeKinds = map[byte]Channel{
	0x01: ChannelData, 0x02: ChannelData, 0x03: ChannelData, 0x04: ChannelData,
	0x05: ChannelData, 0x06: ChannelData, 0x07: ChannelData,
	0x08: ChannelControl, 0x09: ChannelControl, 0x0A: ChannelControl,
	0x0B: ChannelControl, 0x0C: ChannelControl, 0x0D: ChannelControl,
	0x0E: ChannelData, 0x0F: ChannelData, 0x10: ChannelData, 0x11: ChannelData,
	0x12: ChannelData, 0x13: ChannelData, 0x14: ChannelData,
	0x15: ChannelData, 0x16: ChannelData, 0x17: ChannelData, 0x18: ChannelData,
	0x19: ChannelControl, 0x1A: ChannelControl,
	0x1B: ChannelIdentity, 0x1C: ChannelIdentity, 0x1D: ChannelControl, 0x1E: ChannelControl,
	0x20: ChannelSeed, 0x21: ChannelSeed, 0x22: ChannelSeed, 0x23: ChannelSeed,
	0x24: ChannelSeed, 0x25: ChannelSeed, 0x26: ChannelSeed, 0x27: ChannelSeed,
	0x29: ChannelControl, 0x2A: ChannelControl,
	0x2B: ChannelData, 0x2C: ChannelData,
}

func envelopeSample(kind byte) Envelope {
	return Envelope{
		Version: EnvelopeVersion, Kind: kind,
		Epoch: 0x0102030405060708, StateSeq: 0x1112131415161718,
		Flags: 0x0003, Slot: 0x2122, DBID: 0x3132333435363738,
	}
}

func TestEnvelopeIsOneLayout(t *testing.T) {
	for kind := range envelopeKinds {
		raw := EncodeEnvelope(envelopeSample(kind))
		if len(raw) != 30 {
			t.Fatalf("kind 0x%02x: envelope is %d bytes, want 30", kind, len(raw))
		}
		if raw[0] != EnvelopeVersion || raw[1] != kind {
			t.Fatalf("kind 0x%02x: version/kind bytes are %d/%d", kind, raw[0], raw[1])
		}
		if got := binary.BigEndian.Uint64(raw[2:10]); got != 0x0102030405060708 {
			t.Fatalf("kind 0x%02x: epoch at offset 2 is 0x%x", kind, got)
		}
		if got := binary.BigEndian.Uint64(raw[10:18]); got != 0x1112131415161718 {
			t.Fatalf("kind 0x%02x: state_seq at offset 10 is 0x%x", kind, got)
		}
		if got := binary.BigEndian.Uint16(raw[18:20]); got != 3 {
			t.Fatalf("kind 0x%02x: flags at offset 18 are 0x%x", kind, got)
		}
		if got := binary.BigEndian.Uint16(raw[20:22]); got != 0x2122 {
			t.Fatalf("kind 0x%02x: slot at offset 20 is 0x%x", kind, got)
		}
		if got := binary.BigEndian.Uint64(raw[22:30]); got != 0x3132333435363738 {
			t.Fatalf("kind 0x%02x: database id at offset 22 is 0x%x", kind, got)
		}
		back, err := DecodeEnvelope(append(raw, "body"...))
		if err != nil || back != envelopeSample(kind) {
			t.Fatalf("kind 0x%02x: decode = %+v, %v", kind, back, err)
		}
	}
}

func TestEnvelopeRefusesWhatItCannotRead(t *testing.T) {
	good := EncodeEnvelope(envelopeSample(0x01))
	if _, err := DecodeEnvelope(good[:29]); err == nil {
		t.Fatal("a 29-byte envelope decoded")
	}
	badVersion := bytes.Clone(good)
	badVersion[0] = EnvelopeVersion + 1
	if _, err := DecodeEnvelope(badVersion); err == nil {
		t.Fatal("the next version decoded")
	}
	badFlags := bytes.Clone(good)
	binary.BigEndian.PutUint16(badFlags[18:20], 0x0004)
	if _, err := DecodeEnvelope(badFlags); err == nil {
		t.Fatal("a reserved flag bit decoded")
	}
}

func TestFrameHeaderIsTheTransports(t *testing.T) {
	body := []byte("payload-after-the-envelope")
	payload := append(EncodeEnvelope(envelopeSample(0x01)), body...)
	frame := NewFrameWithID(OpReplPush, [RequestIDLen]byte{9}, payload).Encode()

	if FrameHeaderSize != 22 {
		t.Fatalf("FrameHeaderSize = %d, want 22", FrameHeaderSize)
	}
	if got, want := binary.LittleEndian.Uint32(frame[1:5]), uint32(1+16+30+len(body)); got != want {
		t.Fatalf("length word = %d, want %d: it counts opcode, request id and payload, not the 22-byte header", got, want)
	}
	if frame[6] != 9 {
		t.Fatal("request id is not where the transport puts it")
	}
	if !bytes.Equal(frame[FrameHeaderSize:FrameHeaderSize+30], payload[:30]) {
		t.Fatal("the envelope is not at payload offset 0")
	}
}

func TestKindOnTheWrongChannelIsRefused(t *testing.T) {
	for kind, home := range envelopeKinds {
		got, ok := KindChannel(kind)
		if !ok || got != home {
			t.Fatalf("kind 0x%02x: channel = %v, %v; want %v", kind, got, ok, home)
		}
		for _, other := range []Channel{ChannelData, ChannelControl, ChannelSeed, ChannelRaft, ChannelIdentity} {
			err := CheckKind(kind, other)
			if other == home {
				if err != nil {
					t.Fatalf("kind 0x%02x on its own channel %v: %v", kind, home, err)
				}
				continue
			}
			if err == nil {
				t.Fatalf("kind 0x%02x accepted on %v, belongs to %v", kind, other, home)
			}
			if msg := err.Error(); !strings.Contains(msg, "INVALID_ARGUMENT") || !strings.Contains(msg, home.String()) {
				t.Fatalf("kind 0x%02x: refusal %q names neither the code nor the home channel", kind, msg)
			}
		}
	}
	if err := CheckKind(0x0E, ChannelControl); err == nil {
		t.Fatal("SLOT_LIST accepted on the control channel")
	}
}

func TestUnknownKindNamesTheNumber(t *testing.T) {
	for _, kind := range []byte{0x00, 0x1F, 0x2D, 0xFF} {
		if _, ok := KindChannel(kind); ok {
			t.Fatalf("kind 0x%02x is registered", kind)
		}
		raw := EncodeEnvelope(envelopeSample(kind))
		_, err := DecodeEnvelope(raw)
		if err == nil {
			t.Fatalf("kind 0x%02x decoded", kind)
		}
		msg := err.Error()
		if !strings.Contains(msg, "INVALID_ARGUMENT") || !strings.Contains(msg, "unknown kind") ||
			!strings.Contains(msg, strconv.Itoa(int(kind))) {
			t.Fatalf("kind 0x%02x: refusal %q does not name the number", kind, msg)
		}
		if err := CheckKind(kind, ChannelData); err == nil || !strings.Contains(err.Error(), strconv.Itoa(int(kind))) {
			t.Fatalf("kind 0x%02x: CheckKind = %v", kind, err)
		}
	}
}

func TestHandoffStoppedIsADataKind(t *testing.T) {
	if KindHandoffStopped != 0x14 {
		t.Fatalf("KindHandoffStopped = 0x%02x", KindHandoffStopped)
	}
	if ch, ok := KindChannel(KindHandoffStopped); !ok || ch != ChannelData {
		t.Fatalf("0x14 routes to %v, %v; want data", ch, ok)
	}
	if err := CheckKind(KindHandoffStopped, ChannelControl); err == nil {
		t.Fatal("HANDOFF_STOPPED accepted on control")
	}
}

func TestSeedDedupeIsWithdrawn(t *testing.T) {
	if KindSeedDedupe != 0x28 {
		t.Fatalf("KindSeedDedupe = 0x%02x", KindSeedDedupe)
	}
	if ch, ok := KindChannel(KindSeedDedupe); ok {
		t.Fatalf("0x28 is registered on %v", ch)
	}
	_, err := DecodeEnvelope(EncodeEnvelope(envelopeSample(KindSeedDedupe)))
	if err == nil || !strings.Contains(err.Error(), "withdrawn") || !strings.Contains(err.Error(), "40") {
		t.Fatalf("0x28 decode = %v, want a withdrawn refusal naming 40", err)
	}
}

// TestTheAckBatchKindIsOnTheDataChannelOnly holds WRITE_ACK_BATCH to the data channel, where the WRITE_ACK it batches
// travels; on control it would be refused as a wrong-channel kind.
func TestTheAckBatchKindIsOnTheDataChannelOnly(t *testing.T) {
	if KindWriteAckBatch != 0x2B {
		t.Fatalf("KindWriteAckBatch = 0x%02x", KindWriteAckBatch)
	}
	if err := CheckKind(KindWriteAckBatch, ChannelData); err != nil {
		t.Fatalf("WRITE_ACK_BATCH refused on data: %v", err)
	}
	err := CheckKind(KindWriteAckBatch, ChannelControl)
	if err == nil || !strings.Contains(err.Error(), "data") {
		t.Fatalf("WRITE_ACK_BATCH on control = %v, want a refusal naming the data channel", err)
	}
}

// TestTheWriteBatchKindIsOnTheDataChannelOnly holds WRITE_BATCH to the data channel, where the WRITE it batches travels.
func TestTheWriteBatchKindIsOnTheDataChannelOnly(t *testing.T) {
	if KindWriteBatch != 0x2C {
		t.Fatalf("KindWriteBatch = 0x%02x", KindWriteBatch)
	}
	if err := CheckKind(KindWriteBatch, ChannelData); err != nil {
		t.Fatalf("WRITE_BATCH refused on data: %v", err)
	}
	err := CheckKind(KindWriteBatch, ChannelControl)
	if err == nil || !strings.Contains(err.Error(), "data") {
		t.Fatalf("WRITE_BATCH on control = %v, want a refusal naming the data channel", err)
	}
}

func TestClientPrologueIsTwentyBytes(t *testing.T) {
	in := Prologue{Epoch: 0x0102030405060708, StateSeq: 0x1112131415161718, DeadlineMs: 0x21222324}
	raw := EncodePrologue(in)
	if len(raw) != 20 {
		t.Fatalf("prologue is %d bytes, want 20", len(raw))
	}
	if binary.BigEndian.Uint64(raw[0:8]) != in.Epoch || binary.BigEndian.Uint64(raw[8:16]) != in.StateSeq ||
		binary.BigEndian.Uint32(raw[16:20]) != in.DeadlineMs {
		t.Fatalf("prologue layout wrong: % x", raw)
	}
	back, err := DecodePrologue(append(raw, "bson"...))
	if err != nil || back != in {
		t.Fatalf("decode = %+v, %v", back, err)
	}
	if zero, err := DecodePrologue(make([]byte, 20)); err != nil || zero != (Prologue{}) {
		t.Fatalf("all-zero prologue (route it, no deadline) = %+v, %v", zero, err)
	}
	if _, err := DecodePrologue(raw[:19]); err == nil {
		t.Fatal("a 19-byte prologue decoded")
	}
}

func TestEnvelopeAndPrologueMutation(t *testing.T) {
	rng := rand.New(rand.NewSource(23))
	seedEnv := EncodeEnvelope(envelopeSample(0x25))
	seedPro := EncodePrologue(Prologue{Epoch: 5, StateSeq: 6, DeadlineMs: 7})
	accepted := 0
	for i := 0; i < 20000; i++ {
		env := bytes.Clone(seedEnv)
		pro := bytes.Clone(seedPro)
		for flips := rng.Intn(4) + 1; flips > 0; flips-- {
			env[rng.Intn(len(env))] ^= byte(1 << rng.Intn(8))
			pro[rng.Intn(len(pro))] ^= byte(1 << rng.Intn(8))
		}
		if rng.Intn(5) == 0 {
			env = env[:rng.Intn(len(env)+1)]
			pro = pro[:rng.Intn(len(pro)+1)]
		}
		if got, err := DecodeEnvelope(env); err == nil {
			accepted++
			if !bytes.Equal(EncodeEnvelope(got), env[:EnvelopeSize]) {
				t.Fatalf("envelope % x re-encodes as % x", env, EncodeEnvelope(got))
			}
		}
		if got, err := DecodePrologue(pro); err == nil {
			if !bytes.Equal(EncodePrologue(got), pro[:PrologueSize]) {
				t.Fatalf("prologue % x re-encodes as % x", pro, EncodePrologue(got))
			}
		}
	}
	if accepted == 0 {
		t.Fatal("no mutated envelope was accepted, so the round trip was never checked")
	}
}

// TestAPeerOfTheOtherEnvelopeVersionIsRefused: this build speaks envelope version 2, the
// record format's version; a frame of version 1 (the build before) or 3 (a build after) is
// refused before any body is decoded, naming both versions and the fix.
func TestAPeerOfTheOtherEnvelopeVersionIsRefused(t *testing.T) {
	if EnvelopeVersion != 2 {
		t.Fatalf("EnvelopeVersion = %d, want 2: the record format moved and the wire must refuse the build before", EnvelopeVersion)
	}
	good := EncodeEnvelope(envelopeSample(0x01))
	for _, sender := range []byte{1, 3} {
		raw := bytes.Clone(good)
		raw[0] = sender
		_, err := DecodeEnvelope(append(raw, "body"...))
		if err == nil {
			t.Fatalf("a version-%d envelope decoded", sender)
		}
		want := fmt.Sprintf("this node speaks envelope version 2 and the sender speaks %d: nodes of different builds are never mixed; run the same build on every node, each on its own data directory: "+
			"an empty data directory loses that node's data and, on a cluster member, can lose committed cluster state", sender)
		if !strings.Contains(err.Error(), want) {
			t.Errorf("version %d: refusal %q\n  lacks %q", sender, err, want)
		}
		if strings.Contains(err.Error(), "fresh data directory") {
			t.Errorf("version %d: the refusal sends every node to a fresh data directory (store-vouched-cut review R-1): %v", sender, err)
		}
		if !errors.Is(err, ErrInvalidKind) {
			t.Errorf("version %d: the refusal is not ErrInvalidKind: %v", sender, err)
		}
	}
	if env, err := DecodeEnvelope(append(good, "body"...)); err != nil || env.Version != 2 {
		t.Fatalf("this build's own envelope: %+v, %v", env, err)
	}
}
