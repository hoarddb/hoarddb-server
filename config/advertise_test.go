package config

import (
	"strings"
	"testing"
)

// TestTCPAdvertiseIsAKey: [GUARD tcp.advertise is read from the file and from HOARDDB_TCP_ADVERTISE, and is empty by default].
func TestTCPAdvertiseIsAKey(t *testing.T) {
	if got := DefaultConfig().TCP.Advertise; got != "" {
		t.Fatalf("the default advertise address is %q, want none", got)
	}
	t.Setenv("HOARDDB_TCP_ADVERTISE", "db1.example:7433")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TCP.Advertise != "db1.example:7433" {
		t.Errorf("HOARDDB_TCP_ADVERTISE gave %q", cfg.TCP.Advertise)
	}
}

// TestTCPAdvertiseIsValidated: [GUARD an advertise address that is not host:port with a port 1-65535, or has a scheme, user
// info or whitespace, is refused naming the key and the fix].
func TestTCPAdvertiseIsValidated(t *testing.T) {
	for _, good := range []string{"", "db1.example:7433", "192.0.2.5:7433", "[2001:db8::5]:7433"} {
		cfg := DefaultConfig()
		cfg.TCP.Advertise = good
		if err := cfg.Validate(); err != nil {
			t.Errorf("advertise %q was refused: %v", good, err)
		}
	}
	for _, bad := range []string{"db1.example", "db1.example:0", "db1.example:70000", "https://db1.example:7433", "user@db1.example:7433", "db1 .example:7433", ":7433"} {
		cfg := DefaultConfig()
		cfg.TCP.Advertise = bad
		err := cfg.Validate()
		if err == nil {
			t.Errorf("advertise %q was accepted", bad)
			continue
		}
		if !strings.Contains(err.Error(), "tcp.advertise") || !strings.Contains(err.Error(), "HOARDDB_TCP_ADVERTISE") || !strings.Contains(err.Error(), "host:port") {
			t.Errorf("advertise %q: the message %q does not name the key and the form", bad, err)
		}
	}
}
