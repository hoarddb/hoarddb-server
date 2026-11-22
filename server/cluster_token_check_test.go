package server

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
)

// warnCounter is a slog handler that counts records at Warn and above.
type warnCounter struct {
	mu    sync.Mutex
	warns []string
}

func (h *warnCounter) Enabled(context.Context, slog.Level) bool { return true }

func (h *warnCounter) Handle(_ context.Context, record slog.Record) error {
	if record.Level >= slog.LevelWarn {
		h.mu.Lock()
		h.warns = append(h.warns, record.Message)
		h.mu.Unlock()
	}
	return nil
}

func (h *warnCounter) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *warnCounter) WithGroup(string) slog.Handler      { return h }

func (h *warnCounter) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.warns)
}

// TestWrongClusterTokenChecksStayInMemory: refusing a wrong cluster token is
// a comparison against the token held in memory. The token file is read again
// only when it has changed on disk, which is what a rotation does, so a run of
// wrong tokens costs no file reads and no exclusive lock.
func TestWrongClusterTokenChecksStayInMemory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("an unreadable file is readable by root, so the test cannot tell a read from none")
	}
	dataDir := t.TempDir()
	tokenPath := filepath.Join(dataDir, "cluster.token")
	current := ClusterToken{Token: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", CreatedAt: time.Now()}
	if err := SaveClusterToken(dataDir, current); err != nil {
		t.Fatalf("SaveClusterToken: %v", err)
	}
	logs := &warnCounter{}
	c := &Cluster{
		s:      &Server{config: &config.Config{Storage: config.StorageConfig{DataDir: dataDir}}},
		logger: slog.New(logs),
		token:  current,
	}
	if !c.ValidateToken(current.Token) {
		t.Fatal("the current cluster token was refused")
	}
	// One wrong token while the file is readable: whatever a check learns
	// about the file, it learns here.
	if c.ValidateToken("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd") {
		t.Fatal("a wrong cluster token was accepted")
	}

	// The file is left in place and unchanged, but made unreadable: a check
	// that reads it now fails the read and says so.
	if err := os.Chmod(tokenPath, 0); err != nil {
		t.Fatalf("chmod the token file: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(tokenPath, 0600) })
	for range 20 {
		if c.ValidateToken("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb") {
			t.Fatal("a wrong cluster token was accepted")
		}
	}
	if got := logs.count(); got != 0 {
		t.Fatalf("20 wrong cluster tokens went to the token file %d times (%v); an unchanged file must not be read", got, logs.warns)
	}
	if !c.ValidateToken(current.Token) {
		t.Fatal("the current cluster token was refused after a run of wrong ones")
	}

	// A rotation changes the file, and the next check that does not match
	// the token in memory picks the new one up.
	if err := os.Chmod(tokenPath, 0600); err != nil {
		t.Fatalf("chmod the token file back: %v", err)
	}
	rotated := ClusterToken{Token: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", CreatedAt: time.Now()}
	if err := SaveClusterToken(dataDir, rotated); err != nil {
		t.Fatalf("SaveClusterToken (rotation): %v", err)
	}
	later := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(tokenPath, later, later); err != nil {
		t.Fatalf("chtimes the token file: %v", err)
	}
	if !c.ValidateToken(rotated.Token) {
		t.Fatal("the rotated cluster token was refused")
	}
}
