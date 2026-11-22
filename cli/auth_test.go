package cli

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestTokenFallbackClassification pins the client rule: only TOKEN_* earns a
// password retry. A nil-payload OpAuthFail decodes as AuthFailCredentials and
// must not; RATE_LIMITED must not (it means back off); PROTOCOL_MISMATCH and
// ALREADY_AUTHED must not.
func TestTokenFallbackClassification(t *testing.T) {
	cases := []struct {
		reason transport.AuthFailReason
		want   bool
	}{
		{transport.AuthFailCredentials, false},
		{transport.AuthFailTokenExpired, true},
		{transport.AuthFailTokenInvalid, true},
		{transport.AuthFailTokenUnsupported, true},
		{transport.AuthFailProtocolMismatch, false},
		{transport.AuthFailRateLimited, false},
		{transport.AuthFailAlreadyAuthed, false},
		{transport.AuthFailInternal, false},
	}
	for _, tc := range cases {
		if got := isTokenFallback(&AuthError{Reason: tc.reason}); got != tc.want {
			t.Errorf("isTokenFallback(%s) = %v, want %v", tc.reason, got, tc.want)
		}
	}
}

func TestAuthErrorMessagesNameTheFix(t *testing.T) {
	err := &AuthError{Reason: transport.AuthFailTokenExpired}
	if msg := err.Error(); msg == "" || msg == "authentication failed" {
		t.Fatalf("token-expired error message is not actionable: %q", msg)
	}
}

// TestRateLimitedAuthErrorShowsServerMessage is the CLI half of the rate-limit fix:
// RATE_LIMITED has two server causes (a busy derivation gate that checked no
// password, and the per-IP ledger) with different remedies, so the CLI must
// print the server's explanation instead of a fixed "too many attempts".
func TestRateLimitedAuthErrorShowsServerMessage(t *testing.T) {
	serverMessage := "password was not checked: the server waited 5s for a free password-hashing slot"
	msg := (&AuthError{Reason: transport.AuthFailRateLimited, Message: serverMessage}).Error()
	if !strings.Contains(msg, serverMessage) || !strings.Contains(msg, "RATE_LIMITED") {
		t.Fatalf("RATE_LIMITED error %q does not carry the reason and the server's message %q", msg, serverMessage)
	}
	if bare := (&AuthError{Reason: transport.AuthFailRateLimited}).Error(); !strings.Contains(bare, "not a credential failure") {
		t.Fatalf("RATE_LIMITED error without a server message lost its fallback text: %q", bare)
	}
}
