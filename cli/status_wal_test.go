package cli

import (
	"bytes"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
)

// TestStatusShowsWALRow: status prints one wal row for a node that keeps a
// log, saying whether segments are preallocated and why, whether the spare is
// ready, and a refusal on its own line; a node with no log, or an older
// server, prints none.
func TestStatusShowsWALRow(t *testing.T) {
	for _, tt := range []struct {
		name string
		wal  *server.HealthWAL
		want string
	}{
		{"auto on ext4", &server.HealthWAL{Preallocated: true, Configured: "auto", Reason: "ext2/3/4", SpareReady: true},
			"WAL:          preallocated (auto: ext2/3/4), spare ready\n"},
		{"on, spare still filling", &server.HealthWAL{Preallocated: true, Configured: "on"},
			"WAL:          preallocated (on), spare not ready yet\n"},
		{"off by default", &server.HealthWAL{Configured: "off"},
			"WAL:          not preallocated (off)\n"},
		{"auto declined", &server.HealthWAL{Configured: "auto", Reason: "XFS is not measured yet"},
			"WAL:          not preallocated (auto: XFS is not measured yet)\n"},
		{"refusing writes", &server.HealthWAL{Preallocated: true, Configured: "on", Refusal: "the write-ahead log refuses writes: no room"},
			"WAL:          preallocated (on), spare not ready yet\n              refusing writes: the write-ahead log refuses writes: no room\n"},
		{"no log", nil, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var terminal bytes.Buffer
			h := NewCommandHandler(&terminal, false)
			h.printWAL(tt.wal)
			if got := terminal.String(); got != tt.want {
				t.Errorf("status wal row:\n got %q\nwant %q", got, tt.want)
			}
		})
	}
}
