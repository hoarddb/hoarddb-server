package server

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestALoggingNodeAloneInItsRingSyncsBeforeItAcknowledges covers the
// second shape: a node that keeps a write-ahead log but is the only member
// of its ring — a seed before its peers join, seeds naming only itself, a
// cluster removed down to one — does not route (Cluster.Enabled is false),
// so its writes are dispatched locally and never logged. Having a log must
// not excuse them from the sync: what decides is whether this write was
// logged. The restart replays the log as a real one does.
func TestALoggingNodeAloneInItsRingSyncsBeforeItAcknowledges(t *testing.T) {
	writes := []struct {
		name  string
		write func(t *testing.T, s *Server)
	}{
		{"client PUT frame", func(t *testing.T, s *Server) {
			handlerSend(t, NewHandler(s), transport.OpPut, InsertRequest{Database: "app", Bucket: "users", Key: "ada", Data: map[string]interface{}{"name": "Ada"}})
		}},
		{"in-process insert", func(t *testing.T, s *Server) {
			mustInsert(t, s, "app", "users", "ada", map[string]interface{}{"name": "Ada"})
		}},
	}
	for _, tc := range writes {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			kv := kvfault.New()
			s := newKVFaultServer(dataDir, kv)
			createDurabilityBucket(t, s, "users", BucketTypeHash)
			if err := kv.Sync(); err != nil {
				t.Fatalf("sync after setup: %v", err)
			}
			c := retentionTestCluster(t, s, dataDir)
			defer c.cancel()
			if s.wal == nil || c.Enabled() {
				t.Fatalf("setup: want a logging node alone in its ring, have wal=%v enabled=%v", s.wal != nil, c.Enabled())
			}
			tc.write(t, s)

			kv.PowerLoss()
			if err := s.wal.Close(); err != nil {
				t.Fatalf("closing the WAL: %v", err)
			}
			restarted := newKVFaultServer(dataDir, kv)
			if err := restarted.LoadCatalog(); err != nil {
				t.Fatalf("LoadCatalog after the power loss: %v", err)
			}
			w, err := storage.OpenWAL(filepath.Join(dataDir, "wal"), nil)
			if err != nil {
				t.Fatalf("reopening the WAL: %v", err)
			}
			defer func() { _ = w.Close() }()
			restarted.wal = w
			if err := restarted.replayWAL(context.Background()); err != nil {
				t.Fatalf("replaying the WAL: %v", err)
			}
			foundDocument(t, restarted, "users", "ada")
		})
	}
}
