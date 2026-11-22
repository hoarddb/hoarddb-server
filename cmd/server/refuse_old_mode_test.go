package main

import (
	"log/slog"
	"os"
	"strings"
	"testing"
)

// TestTheOldReplicatedModeIsRefusedBeforeAnythingIsWritten: a replication factor above one, or a
// seed list, without cluster.enabled used to start the earlier hash ring. prepare refuses it now
// with the setting that selected it and the ring setting to use, and the data directory is as it
// was. Break: delete the refusal at the top of prepare.
func TestTheOldReplicatedModeIsRefusedBeforeAnythingIsWritten(t *testing.T) {
	cases := []struct {
		name     string
		wantName string
	}{
		{"replication factor 2", "HOARDDB_REPLICATION_FACTOR=2"},
		{"replication factor", "HOARDDB_REPLICATION_FACTOR=3"},
		{"seed nodes", "HOARDDB_SEED_NODES"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := loadStartConfig(t)
			switch tc.name {
			case "replication factor 2":
				cfg.Server.ReplicationFactor = 2
			case "replication factor":
				cfg.Server.ReplicationFactor = 3
			default:
				cfg.HashRing.SeedNodes = []string{"node1@127.0.0.1:4433"}
			}
			var banners, logs concurrentBuffer
			srv, err := prepare(cfg, slog.New(slog.NewJSONHandler(&logs, nil)), &banners, noTerminal())
			if err == nil {
				_ = srv.Stop()
				t.Fatal("prepare started the old replicated mode, want a refusal")
			}
			for _, want := range []string{tc.wantName, "old replicated mode", "HOARDDB_CLUSTER_ENABLED=true", "the Cluster mode section of the configuration page of the user docs"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal %q does not contain %q", err, want)
				}
			}
			entries, readErr := os.ReadDir(cfg.Storage.DataDir)
			if readErr == nil && len(entries) != 0 {
				t.Errorf("the refusal left %d entries in the data directory, want none", len(entries))
			}
		})
	}
}
