package transport

import (
	"fmt"
	"strings"
)

// ProtocolMajor is the protocol major version (tcp-protocol-spec.md
// section 3.1). It becomes ProtocolVersion's value in opcodes.go; the two
// names exist because they answer different questions: the version byte in
// every frame header (ProtocolVersion) and the major number inside the ALPN
// identifier (ProtocolMajor). Breaking changes raise both together, and an
// old major stays served for at least one release before being retired.
const ProtocolMajor uint8 = 1

// ALPNv1 is the ALPN identifier of the first protocol major. It is sent in
// the TLS handshake (NextProtos) of both transports and is what actually
// negotiates the version — the frame Ver byte is only a consistency guard
// against mixed-version streams.
const ALPNv1 = "hoarddb/1"

// ALPNFor returns the ALPN identifier for a protocol major number.
func ALPNFor(major uint8) string {
	return fmt.Sprintf("hoarddb/%d", major)
}

// MajorFromALPN parses a negotiated ALPN identifier back into its protocol
// major. The boolean is false when the string is not a hoarddb ALPN at all.
func MajorFromALPN(negotiated string) (uint8, bool) {
	if !strings.HasPrefix(negotiated, "hoarddb/") {
		return 0, false
	}
	var major uint8
	if _, err := fmt.Sscanf(negotiated, "hoarddb/%d", &major); err != nil {
		return 0, false
	}
	// Sscanf accepts "hoarddb/01" and "hoarddb/1x" the same as "hoarddb/1";
	// re-formatting the parsed number and comparing keeps the accept set
	// exactly the canonical identifiers.
	if ALPNFor(major) != negotiated {
		return 0, false
	}
	return major, true
}

// SupportedALPN lists every ALPN identifier this build serves. It feeds the
// refusal message for a connection whose negotiated ALPN is not one we
// serve, so a developer does not have to guess the version (AGENTS.md rule
// 3: errors explain the fix, not just the failure).
func SupportedALPN() []string {
	return []string{ALPNFor(ProtocolMajor)}
}

// VerifyNegotiatedALPN checks a negotiated ALPN identifier (from
// tls.ConnectionState.NegotiatedProtocol) against the identifiers this
// build serves and returns an error naming what it does serve when they
// differ. Both listeners run it after the handshake: Go's crypto/tls only
// fails the handshake itself when the client offered an ALPN list with
// zero overlap — a client that offered no ALPN at all negotiates an empty
// string, which would otherwise slide through to the frame gate as if it
// were version 1 (spec 3.1 rule 1).
func VerifyNegotiatedALPN(negotiated string) error {
	for _, supported := range SupportedALPN() {
		if negotiated == supported {
			return nil
		}
	}
	return &WireError{
		Code: ErrUnsupportedVersion,
		Message: fmt.Sprintf("negotiated protocol %q is not served by this server (supported: %s)",
			negotiated, strings.Join(SupportedALPN(), ", ")),
	}
}

// asWireError extracts a *WireError from err when present; callers that
// need the code (refusals) rather than just a message use it instead of a
// type switch at every site.
func asWireError(err error, out **WireError) bool {
	if we, ok := err.(*WireError); ok {
		*out = we
		return true
	}
	return false
}
