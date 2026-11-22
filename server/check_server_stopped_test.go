package server

import (
	"net"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// An offline command that is not a password reset (a key export or import) is refused with
// the same text, so the text must not name a password reset or users.json.
func TestCheckServerStoppedRefusalFitsEveryOfflineCommand(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Close() })
	cfg := config.DefaultConfig()
	cfg.TCP.Listen = held.Addr().String()
	err = CheckServerStopped(cfg)
	if err == nil {
		t.Fatal("a bound driver port was not refused")
	}
	text := err.Error()
	if !strings.Contains(text, "stop it first") || strings.Contains(text, "password") || strings.Contains(text, "users.json") {
		t.Fatalf("refusal %q must tell the operator to stop the server without naming a password reset or users.json", text)
	}
}
