package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
)

// TestStatusShowsMemtableRow: one row after Healthy, in each form the design
// note drafts (memtable-host-scaled-design.md §5.3).
func TestStatusShowsMemtableRow(t *testing.T) {
	const gib = int64(1) << 30
	for _, tt := range []struct {
		name     string
		memtable *server.HealthMemtable
		want     string
	}{
		{"chosen from the host", &server.HealthMemtable{MB: 64, Source: "host", ChoiceMB: 64, ChosenFromLimitBytes: 4080218112, LimitBytes: 4080218112, BatchBytes: 10_066_329},
			"Memtable:     64 MiB  chosen at first start from 3.8 GiB memory; largest atomic batch 10 MB\n"},
		{"set in configuration", &server.HealthMemtable{MB: 256, Source: "config", ChoiceMB: 64, LimitBytes: 16 * gib, BatchBytes: 40_265_318},
			"Memtable:     256 MiB  set in configuration; this store's choice is 64 MiB\n"},
		{"set by the environment", &server.HealthMemtable{MB: 256, Source: "env", ChoiceMB: 64, LimitBytes: 16 * gib, BatchBytes: 40_265_318},
			"Memtable:     256 MiB  set by HOARDDB_MEMTABLE_MB; this store's choice is 64 MiB\n"},
		{"kept from the last run", &server.HealthMemtable{MB: 128, Source: "upgrade", ChoiceMB: 128, LimitBytes: 16 * gib, BatchBytes: 20_132_659},
			"Memtable:     128 MiB  kept from the store's last run; largest atomic batch 20 MB\n"},
		{"large for the node", &server.HealthMemtable{MB: 128, Source: "upgrade", ChoiceMB: 128, LimitBytes: 3006477107, BatchBytes: 20_132_659, Large: true},
			"Memtable:     128 MiB  kept from the store's last run; LARGE for this node's 2.8 GiB memory limit (see the log)\n"},
		{"differs from a member", &server.HealthMemtable{MB: 128, Source: "config", ChoiceMB: 64, LimitBytes: 16 * gib, BatchBytes: 20_132_659,
			Differs: []server.MemtablePeer{{NodeID: "n2", MB: 64}}},
			"Memtable:     128 MiB  DIFFERS from member n2 (64 MiB); members must match (see the log)\n"},
		{"no limit read", &server.HealthMemtable{MB: 32, Source: "no-limit-read", ChoiceMB: 32, BatchBytes: 5_033_164},
			"Memtable:     32 MiB  chosen at first start without a memory limit (it could not be read); largest atomic batch 5 MB\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var terminal bytes.Buffer
			h := NewCommandHandler(&terminal, false)
			h.printMemtable(tt.memtable)
			if got := terminal.String(); got != tt.want {
				t.Errorf("status memtable row:\n got %q\nwant %q", got, tt.want)
			}
		})
	}
}

func TestStatusOlderServerNoRow(t *testing.T) {
	var terminal bytes.Buffer
	h := NewCommandHandler(&terminal, false)
	h.printMemtable(nil)
	if strings.Contains(terminal.String(), "Memtable") {
		t.Errorf("an older server's status printed a memtable row: %q", terminal.String())
	}
}

// TestStatusPrintsTheMemtableFromALiveServer: the row comes through the wire.
func TestStatusPrintsTheMemtableFromALiveServer(t *testing.T) {
	c := newDumpTestServer(t)
	var terminal bytes.Buffer
	c.handler.terminal = &terminal
	if err := c.handler.handleStatus(); err != nil {
		t.Fatalf("handleStatus: %v", err)
	}
	if !strings.Contains(terminal.String(), "Healthy:      true\nMemtable:     ") {
		t.Errorf("status printed no memtable row after Healthy:\n%s", terminal.String())
	}
}
