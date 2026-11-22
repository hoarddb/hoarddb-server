package server

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// TestRequireModeRefusesToStartWhenTheStoreIsNotSealed: encryption.mode
// require means never serve unsealed (BUG-0612, owner ruling 2026-10-18). A
// start over a store whose value log has key id 0 fails, the error names the
// damaged file and the way out, and the store is released so the next start
// can take it.
func TestRequireModeRefusesToStartWhenTheStoreIsNotSealed(t *testing.T) {
	withDataDir, damagedLog, damagedPath := sealedDataDirWithDamagedMarker(t, "require")

	cfg := config.DefaultConfig()
	cfg.Metrics.Enabled = false
	cfg.Audit.Enabled = false
	withDataDir("require")(cfg)
	refused, err := NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		_ = refused.Stop()
		t.Fatal("NewService started a require node over a store whose value log key id is 0")
	}
	for _, want := range []string{damagedLog + " has key id 0", "encryption.mode", "require", "HOARDDB_ENCRYPTION_MODE=on"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not contain %q", err.Error(), want)
		}
	}

	// Nothing was written before the refusal: opening the store would have
	// rotated the damaged log away, and the marker is still the zeros it was.
	head, readErr := os.ReadFile(damagedPath)
	if readErr != nil || len(head) < 8 || !bytes.Equal(head[:8], make([]byte, 8)) {
		t.Errorf("after the refusal the head of %s is %v (%v), want eight zero bytes: the refusal came after the store was opened", damagedLog, head[:min(8, len(head))], readErr)
	}

	// The refusal released the data directory: an on node can take it.
	onNode := newCapacityService(t, withDataDir("on"), nil)
	stopService(t, onNode)
}
