package transport

import (
	"strings"
	"testing"
)

// TestALPNForRoundTrip verifies the ALPN identifiers are exactly the
// canonical "hoarddb/<major>" forms and parse back to the same major.
func TestALPNForRoundTrip(t *testing.T) {
	if got := ALPNFor(ProtocolMajor); got != ALPNv1 {
		t.Fatalf("ALPNFor(ProtocolMajor) = %q, want %q (the well-known v1 identifier)", got, ALPNv1)
	}
	for major := uint8(1); major <= 5; major++ {
		id := ALPNFor(major)
		parsed, ok := MajorFromALPN(id)
		if !ok || parsed != major {
			t.Fatalf("MajorFromALPN(%q) = (%d, %v), want (%d, true)", id, parsed, ok, major)
		}
	}
	// Prefix-only or sloppy forms are not accepted, so "hoarddb/1" is
	// exactly "hoarddb/1" and nothing else.
	if _, ok := MajorFromALPN("hoarddb/01"); ok {
		t.Fatal("MajorFromALPN accepted a zero-padded identifier")
	}
	if _, ok := MajorFromALPN("hoarddb/1x"); ok {
		t.Fatal("MajorFromALPN accepted a trailing-suffix identifier")
	}
	if _, ok := MajorFromALPN("https"); ok {
		t.Fatal("MajorFromALPN accepted a non-hoarddb identifier")
	}
}

// TestVerifyNegotiatedALPNRefusedWithList verifies a client that negotiates
// an ALPN this build does not serve (or none at all) is refused with the
// supported-version list named in the message (spec 3.1 rule 1).
func TestVerifyNegotiatedALPNRefusedWithList(t *testing.T) {
	if err := VerifyNegotiatedALPN(ALPNv1); err != nil {
		t.Fatalf("expected the served ALPN to verify, got: %v", err)
	}
	for _, negotiated := range []string{"", "http/1.1", "hoarddb/9"} {
		err := VerifyNegotiatedALPN(negotiated)
		if err == nil {
			t.Fatalf("VerifyNegotiatedALPN(%q) succeeded; want refusal", negotiated)
		}
		var wireErr *WireError
		if !asWireError(err, &wireErr) {
			t.Fatalf("refusal for %q is not a WireError: %T", negotiated, err)
		}
		if wireErr.Code != ErrUnsupportedVersion {
			t.Fatalf("refusal code = %s, want UNSUPPORTED_VERSION", wireErr.Code)
		}
		if !strings.Contains(wireErr.Message, ALPNv1) {
			t.Fatalf("refusal for %q does not name the supported version: %q", negotiated, wireErr.Message)
		}
	}
}
