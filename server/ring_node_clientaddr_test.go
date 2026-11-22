package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// TestRingNodeConfigPublishesTheClientAddress: [GUARD tcp.advertise, or a listen address another machine can dial,
// reaches the ring node as its client address; a loopback or wildcard listen address publishes nothing].
func TestRingNodeConfigPublishesTheClientAddress(t *testing.T) {
	for _, c := range []struct{ advertise, listen, want string }{
		{"db1.example:7433", "0.0.0.0:7433", "db1.example:7433"},
		{"", "192.0.2.5:7433", "192.0.2.5:7433"},
		{"", "0.0.0.0:7433", ""},
		{"", "127.0.0.1:7433", ""},
	} {
		cfg := config.DefaultConfig()
		cfg.TCP.Advertise, cfg.TCP.Listen = c.advertise, c.listen
		if got := ringNodeConfig(cfg).ClientAddr; got != c.want {
			t.Errorf("advertise %q listen %q gave client address %q, want %q", c.advertise, c.listen, got, c.want)
		}
	}
}
