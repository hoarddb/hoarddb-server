package ringclient

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestPasswordMinLengthIsWhatTheServerReportedAtLogin: the server never sees a new password, so
// the minimum it reports on AUTH_OK is what a caller that takes one checks it against (BUG-0493).
func TestPasswordMinLengthIsWhatTheServerReportedAtLogin(t *testing.T) {
	body, err := bson.Marshal(bson.M{"success": true, "password_min_length": int32(14)})
	if err != nil {
		t.Fatal(err)
	}
	server := startFakeServerAuthOK(t, body, func(*fakeConn, *transport.Frame) {})
	if got := dialFake(t, server).PasswordMinLength(); got != 14 {
		t.Fatalf("PasswordMinLength = %d, want the 14 the server reported", got)
	}

	silent := startFakeServer(t, func(*fakeConn, *transport.Frame) {})
	if got := dialFake(t, silent).PasswordMinLength(); got != 0 {
		t.Fatalf("PasswordMinLength = %d after an AUTH_OK with no body, want 0 (none reported)", got)
	}
}

// TestALoginReplyThatDoesNotDecodeIsRefused: a minimum the client cannot read must not pass for none.
func TestALoginReplyThatDoesNotDecodeIsRefused(t *testing.T) {
	server := startFakeServerAuthOK(t, []byte{0x05, 0, 0, 0, 0x01}, func(*fakeConn, *transport.Frame) {})
	client, err := Dial(testContext(t), server.options())
	if err == nil {
		_ = client.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "login reply") {
		t.Fatalf("Dial with an undecodable AUTH_OK: %v, want a refusal naming the login reply", err)
	}
}
