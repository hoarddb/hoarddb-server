package transport

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
)

// The cluster pepper hand-out (ring user store spec 5.3): a data member that holds no pepper asks a
// member that does. Both kinds ride the control channel, whose admission is the proof of
// membership (a pinned key for a committed member of this cluster); the identity channel, which
// admits any pinned key, never carries them. The pepper's bytes exist on the wire only inside that
// pinned TLS session.

const (
	// KindPepperRequest is PEPPER_REQUEST: [nonce:16]. The asker's identity is the handshake's.
	KindPepperRequest byte = 0x29
	// KindPepperReply is PEPPER_REPLY: [nonce:16][status:1] then, on ok, [kid:8][pepper:32], on
	// refused, [len:2][message]; not held carries nothing more.
	KindPepperReply byte = 0x2A
)

// PepperNonceLen is the length of a PEPPER_REQUEST nonce; PepperLen of a pepper; PepperKIDLen of
// its kid (lowercase hex).
const (
	PepperNonceLen = 16
	PepperLen      = 32
	PepperKIDLen   = 8
)

// PepperStatus is the status byte of a PEPPER_REPLY.
type PepperStatus byte

// The statuses of a PEPPER_REPLY.
const (
	// PepperOK carries the kid and the pepper.
	PepperOK PepperStatus = 0
	// PepperNotHeld: the holder has no pepper the cluster's state names; ask another member.
	PepperNotHeld PepperStatus = 1
	// PepperRefused: the asker may not have the pepper (a witness, or not a member in a state that
	// may hold it); Message says why.
	PepperRefused PepperStatus = 2
)

// PepperRequest is PEPPER_REQUEST.
type PepperRequest struct {
	Nonce [PepperNonceLen]byte
}

// PepperReply is PEPPER_REPLY. KID and Pepper are set only on PepperOK, Message only on
// PepperRefused. Printed by fmt, slog or encoding/json, a reply shows its kid, never the key.
type PepperReply struct {
	Nonce  [PepperNonceLen]byte
	Status PepperStatus
	KID    string
	// Pepper is the key on PepperOK, empty otherwise.
	Pepper  PepperKey
	Message string
}

// PepperKey is the 32 bytes of a PEPPER_REPLY. They are reachable only through a function, never
// stored in the struct: a reply held in an unexported field of another value is printed by fmt
// field by field without its Format, and an array (or a pointer to one, under %s or %q) would print
// whole there; a func prints as an address under every verb, and encoding/json skips it.
type PepperKey struct{ get func() *[PepperLen]byte }

// NewPepperKey copies raw, which must be PepperLen bytes; anything else gives the empty key.
func NewPepperKey(raw []byte) PepperKey {
	if len(raw) != PepperLen {
		return PepperKey{}
	}
	held := new([PepperLen]byte)
	copy(held[:], raw)
	return PepperKey{get: func() *[PepperLen]byte { return held }}
}

// Held reports whether the key carries bytes.
func (k PepperKey) Held() bool { return k.get != nil }

// Bytes returns a copy of the key's bytes, nil for the empty key.
func (k PepperKey) Bytes() []byte {
	if k.get == nil {
		return nil
	}
	return append([]byte(nil), k.get()[:]...)
}

func (k PepperKey) allZero() bool {
	if k.get == nil {
		return true
	}
	return *k.get() == [PepperLen]byte{}
}

// String prints that a key is there, never its bytes.
func (k PepperKey) String() string {
	if k.get == nil {
		return "no key"
	}
	return "key (redacted)"
}

// Format prints String for every verb.
func (k PepperKey) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(k.String())) } //nolint:errcheck // as PepperReply.Format

// MarshalJSON writes String.
func (k PepperKey) MarshalJSON() ([]byte, error) { return json.Marshal(k.String()) }

// String prints the reply without the key. String, Format, LogValue and MarshalJSON make a printed
// reply readable; they are cosmetic: the key cannot print because PepperKey holds it behind a
// function, whatever fmt does with the reply (the tests stay green with these methods removed).
func (m PepperReply) String() string {
	switch m.Status {
	case PepperOK:
		return fmt.Sprintf("PEPPER_REPLY ok (key %s)", m.KID)
	case PepperNotHeld:
		return "PEPPER_REPLY not held"
	case PepperRefused:
		return fmt.Sprintf("PEPPER_REPLY refused: %s", m.Message)
	}
	return fmt.Sprintf("PEPPER_REPLY status %d", m.Status)
}

// Format prints String for every verb, %x and %+v included.
func (m PepperReply) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(m.String())) } //nolint:errcheck // fmt.State's Write reports nothing a Formatter can act on

// LogValue prints String.
func (m PepperReply) LogValue() slog.Value { return slog.StringValue(m.String()) }

// MarshalJSON writes String, never the key.
func (m PepperReply) MarshalJSON() ([]byte, error) { return json.Marshal(m.String()) }

func (PepperRequest) controlKind() byte { return KindPepperRequest }
func (PepperReply) controlKind() byte   { return KindPepperReply }

func (m PepperRequest) appendBody(dst []byte) ([]byte, error) { return append(dst, m.Nonce[:]...), nil }

func validPepperKID(kid string) bool {
	if len(kid) != PepperKIDLen {
		return false
	}
	for i := 0; i < len(kid); i++ {
		if c := kid[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// checkPepperReply is the one rule set of the reply, applied at encode and, through the re-encode,
// at decode.
func checkPepperReply(m PepperReply) error {
	switch m.Status {
	case PepperOK:
		if !validPepperKID(m.KID) {
			return fmt.Errorf("%w: PEPPER_REPLY kid %q is not %d lowercase hex characters", ErrControlBody, m.KID, PepperKIDLen)
		}
		if m.Pepper.allZero() {
			return fmt.Errorf("%w: PEPPER_REPLY ok carries no key or an all-zero one", ErrControlBody)
		}
		if m.Message != "" {
			return fmt.Errorf("%w: PEPPER_REPLY ok carries a text", ErrControlBody)
		}
	case PepperNotHeld, PepperRefused:
		if m.KID != "" || m.Pepper.Held() {
			return fmt.Errorf("%w: PEPPER_REPLY status %d carries a key, which only ok carries", ErrControlBody, m.Status)
		}
		if m.Status == PepperNotHeld && m.Message != "" {
			return fmt.Errorf("%w: PEPPER_REPLY not held carries a text", ErrControlBody)
		}
		if m.Status == PepperRefused && (m.Message == "" || len(m.Message) > MaxRelayMessage) {
			return fmt.Errorf("%w: PEPPER_REPLY refusal text is %d bytes, it must be 1 to %d", ErrControlBody, len(m.Message), MaxRelayMessage)
		}
	default:
		return fmt.Errorf("%w: PEPPER_REPLY status %d is not one of 0 to 2", ErrControlBody, m.Status)
	}
	return nil
}

func (m PepperReply) appendBody(dst []byte) ([]byte, error) {
	if err := checkPepperReply(m); err != nil {
		return nil, err
	}
	dst = append(dst, m.Nonce[:]...)
	dst = append(dst, byte(m.Status))
	switch m.Status {
	case PepperOK:
		dst = append(dst, m.KID...)
		return append(dst, m.Pepper.Bytes()...), nil
	case PepperRefused:
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(m.Message)))
		return append(dst, m.Message...), nil
	}
	return dst, nil
}

func decodePepperRequest(body []byte) (PepperRequest, error) {
	if err := controlBodyLength(KindPepperRequest, body, PepperNonceLen); err != nil {
		return PepperRequest{}, err
	}
	var m PepperRequest
	copy(m.Nonce[:], body)
	return m, nil
}

func decodePepperReply(body []byte) (PepperReply, error) {
	const fixed = PepperNonceLen + 1
	if len(body) < fixed {
		return PepperReply{}, fmt.Errorf("%w: PEPPER_REPLY is %d bytes, shorter than its %d-byte fixed part", ErrControlBody, len(body), fixed)
	}
	m := PepperReply{Status: PepperStatus(body[PepperNonceLen])}
	copy(m.Nonce[:], body)
	rest := body[fixed:]
	switch m.Status {
	case PepperOK:
		if err := controlBodyLength(KindPepperReply, body, fixed+PepperKIDLen+PepperLen); err != nil {
			return PepperReply{}, err
		}
		m.KID = string(rest[:PepperKIDLen])
		m.Pepper = NewPepperKey(rest[PepperKIDLen:])
	case PepperNotHeld:
		if err := controlBodyLength(KindPepperReply, body, fixed); err != nil {
			return PepperReply{}, err
		}
	case PepperRefused:
		if len(rest) < 2 {
			return PepperReply{}, fmt.Errorf("%w: PEPPER_REPLY refused has no text length", ErrControlBody)
		}
		if err := controlBodyLength(KindPepperReply, body, fixed+2+int(binary.BigEndian.Uint16(rest))); err != nil {
			return PepperReply{}, err
		}
		m.Message = string(rest[2:])
	}
	// Whatever decodes must encode: this refuses the rules the layout alone does not carry.
	if err := checkPepperReply(m); err != nil {
		return PepperReply{}, err
	}
	return m, nil
}
