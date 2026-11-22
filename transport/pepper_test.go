package transport

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func pepperNonce() [PepperNonceLen]byte {
	var nonce [PepperNonceLen]byte
	for i := range nonce {
		nonce[i] = byte(0xA0 + i)
	}
	return nonce
}

func roundTripControl(t *testing.T, msg ControlMessage) ControlMessage {
	t.Helper()
	payload, err := EncodeControl(Envelope{Epoch: 3, StateSeq: 9}, msg)
	if err != nil {
		t.Fatalf("encode %T: %v", msg, err)
	}
	_, back, err := DecodeControl(payload)
	if err != nil {
		t.Fatalf("decode %T: %v", msg, err)
	}
	again, err := EncodeControl(Envelope{Epoch: 3, StateSeq: 9}, back)
	if err != nil || !bytes.Equal(again, payload) {
		t.Fatalf("%T does not re-encode to the same bytes: %v", msg, err)
	}
	return back
}

// TestPepperMessagesRoundTripStrictly (spec 5.3): PEPPER_REQUEST is a 16-byte nonce; PEPPER_REPLY
// echoes it with a status, and carries the kid and the 32 pepper bytes only on ok and a text only
// on refused. Every other length, status or kid spelling is refused at decode and at encode.
func TestPepperMessagesRoundTripStrictly(t *testing.T) {
	nonce := pepperNonce()
	if got := roundTripControl(t, PepperRequest{Nonce: nonce}); got != (PepperRequest{Nonce: nonce}) {
		t.Fatalf("request came back as %+v", got)
	}
	raw := make([]byte, PepperLen)
	raw[0], raw[31] = 0x11, 0x99
	pepper := NewPepperKey(raw)
	for _, reply := range []PepperReply{
		{Nonce: nonce, Status: PepperOK, KID: "0a1b2c3d", Pepper: pepper},
		{Nonce: nonce, Status: PepperNotHeld},
		{Nonce: nonce, Status: PepperRefused, Message: "w3 is a witness: it serves no client logins and is not given the cluster's password key."},
	} {
		got, ok := roundTripControl(t, reply).(PepperReply)
		if !ok || !samePepperReply(got, reply) {
			t.Fatalf("reply %d came back as %v", reply.Status, got)
		}
	}

	request, err := EncodeControl(Envelope{}, PepperRequest{Nonce: nonce})
	if err != nil {
		t.Fatal(err)
	}
	okReply, err := EncodeControl(Envelope{}, PepperReply{Nonce: nonce, Status: PepperOK, KID: "0a1b2c3d", Pepper: pepper})
	if err != nil {
		t.Fatal(err)
	}
	for name, payload := range map[string][]byte{
		"request one byte short": request[:len(request)-1],
		"request one byte long":  append(append([]byte(nil), request...), 0),
		"ok reply short":         okReply[:len(okReply)-1],
		"ok reply long":          append(append([]byte(nil), okReply...), 0),
	} {
		if _, _, err := DecodeControl(payload); !errors.Is(err, ErrControlBody) {
			t.Errorf("%s: %v, want ErrControlBody", name, err)
		}
	}
	for name, reply := range map[string]PepperReply{
		"unknown status":           {Nonce: nonce, Status: 3},
		"upper-case kid":           {Nonce: nonce, Status: PepperOK, KID: "0A1B2C3D", Pepper: pepper},
		"short kid":                {Nonce: nonce, Status: PepperOK, KID: "0a1b", Pepper: pepper},
		"no pepper on ok":          {Nonce: nonce, Status: PepperOK, KID: "0a1b2c3d"},
		"zero pepper on ok":        {Nonce: nonce, Status: PepperOK, KID: "0a1b2c3d", Pepper: NewPepperKey(make([]byte, PepperLen))},
		"kid on not held":          {Nonce: nonce, Status: PepperNotHeld, KID: "0a1b2c3d"},
		"pepper on refused":        {Nonce: nonce, Status: PepperRefused, Pepper: pepper, Message: "no"},
		"empty refusal":            {Nonce: nonce, Status: PepperRefused},
		"text on ok":               {Nonce: nonce, Status: PepperOK, KID: "0a1b2c3d", Pepper: pepper, Message: "x"},
		"refusal past its maximum": {Nonce: nonce, Status: PepperRefused, Message: strings.Repeat("x", MaxRelayMessage+1)},
	} {
		if _, err := EncodeControl(Envelope{}, reply); !errors.Is(err, ErrControlBody) {
			t.Errorf("encode %s: %v, want ErrControlBody", name, err)
		}
	}
	notHeld, err := EncodeControl(Envelope{}, PepperReply{Nonce: nonce, Status: PepperNotHeld})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := DecodeControl(append(append([]byte(nil), notHeld...), 'x')); !errors.Is(err, ErrControlBody) {
		t.Errorf("a not-held reply with a trailing byte: %v", err)
	}
	upper := append([]byte(nil), okReply...)
	upper[EnvelopeSize+PepperNonceLen+1] = 'A'
	if _, _, err := DecodeControl(upper); !errors.Is(err, ErrControlBody) {
		t.Errorf("an ok reply whose kid is not lowercase hex: %v", err)
	}
}

// TestPepperKindsAreControlChannelOnly (spec 5.3): the two kinds are 0x29 and 0x2A on the control
// channel, whose admission checks committed membership; the identity channel, which admits any
// pinned key, refuses them. Break: register the kinds on the identity channel; red.
func TestPepperKindsAreControlChannelOnly(t *testing.T) {
	if KindPepperRequest != 0x29 || KindPepperReply != 0x2A {
		t.Fatalf("pepper kinds are 0x%02x and 0x%02x, want 0x29 and 0x2A", KindPepperRequest, KindPepperReply)
	}
	for _, kind := range []byte{KindPepperRequest, KindPepperReply} {
		if channel, ok := KindChannel(kind); !ok || channel != ChannelControl {
			t.Fatalf("kind 0x%02x is on %v (registered %v), want control", kind, channel, ok)
		}
		for _, other := range []Channel{ChannelIdentity, ChannelData, ChannelSeed} {
			if err := CheckKind(kind, other); !errors.Is(err, ErrInvalidKind) {
				t.Fatalf("kind 0x%02x on the %v channel: %v, want refused", kind, other, err)
			}
		}
	}
	msg, err := NewControlMessage(Envelope{}, PepperRequest{Nonce: pepperNonce()})
	if err != nil {
		t.Fatal(err)
	}
	msg.Env.Kind = KindPepperRequest
	if _, err := (Message{Env: msg.Env, Body: msg.Body}).Control(); err != nil {
		t.Fatalf("a PEPPER_REQUEST read as control: %v", err)
	}
}

func samePepperReply(a, b PepperReply) bool {
	if !bytes.Equal(a.Pepper.Bytes(), b.Pepper.Bytes()) {
		return false
	}
	return a.Nonce == b.Nonce && a.Status == b.Status && a.KID == b.KID && a.Message == b.Message
}

// replyHolder keeps a reply in an unexported field, where fmt cannot call its methods.
type replyHolder struct {
	r       PepperReply
	Exposed PepperReply
}

// TestAPepperReplyNeverPrintsItsKey (review F4): a reply printed alone, inside another value
// (unexported or exported field, by value or pointer), or through slog's text and JSON handlers
// shows the kid and never the key's bytes. Break: hold the key in the reply as [PepperLen]byte or
// *[PepperLen]byte; red (the pointer leaks under %q inside an unexported field).
func TestAPepperReplyNeverPrintsItsKey(t *testing.T) {
	raw := make([]byte, PepperLen)
	for i := range raw {
		raw[i] = byte(0xC0 + i)
	}
	reply := PepperReply{Status: PepperOK, KID: "0a1b2c3d", Pepper: NewPepperKey(raw)}
	holder := replyHolder{r: reply, Exposed: reply}
	var out strings.Builder
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%d"} {
		fmt.Fprintf(&out, verb+"|"+verb+"|"+verb+"|"+verb+"\n", reply, &reply, holder, &holder)
	}
	var text, js bytes.Buffer
	slog.New(slog.NewTextHandler(&text, nil)).Info("x", "reply", reply, "holder", holder, "ptr", &holder)
	slog.New(slog.NewJSONHandler(&js, nil)).Info("x", "reply", reply, "holder", holder, "ptr", &holder)
	all := out.String() + text.String() + js.String()
	for _, form := range []string{string(raw), hex.EncodeToString(raw), strings.ToUpper(hex.EncodeToString(raw)), fmt.Sprintf("% x", raw),
		base64.StdEncoding.EncodeToString(raw), base64.RawURLEncoding.EncodeToString(raw), strings.Trim(fmt.Sprintf("%d", raw), "[]"),
		strings.Trim(fmt.Sprintf("%q", raw), "\"")} {
		if strings.Contains(all, form) {
			t.Fatalf("a printed PEPPER_REPLY holds %q:\n%s", form, all)
		}
	}
	if !strings.Contains(fmt.Sprint(reply), "0a1b2c3d") {
		t.Fatalf("a printed reply does not name its kid: %v", reply)
	}
}
