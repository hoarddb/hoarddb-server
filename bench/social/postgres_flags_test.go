package main

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"
)

// TestPostgresFlags: -engine postgres parses with the flags every competitor
// takes, defaults to TLS on a single node, and refuses what has
// no meaning for it.
func TestPostgresFlags(t *testing.T) {
	cfg, err := parseFlags([]string{"-engine", "postgres", "-mix", "rh"})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.PostgresTLS || !strings.Contains(cfg.PostgresDSN, "5432/social") {
		t.Errorf("defaults are tls=%v dsn=%q; want TLS on and the default PostgreSQL port", cfg.PostgresTLS, cfg.PostgresDSN)
	}
	if cfg.Fanout != fanoutSingle || cfg.nodeCount() != 1 {
		t.Errorf("fanout %q, %d nodes; want single and one node", cfg.Fanout, cfg.nodeCount())
	}
	for _, args := range [][]string{
		{"-engine", "postgres", "-mix", "rh", "-fanout", "bulk"},
		{"-engine", "postgres", "-mix", "rh", "-postgres-tls=false"},
		{"-engine", "postgres", "-mix", "rh", "-postgres-tls=false", "-postgres-dsn", "postgresql://bench@db.example:5432/social?sslmode=disable"},
	} {
		if _, err := parseFlags(args); err != nil {
			t.Errorf("%v: %v", args, err)
		}
	}
	if _, err := parseFlags([]string{"-engine", "postgres", "-write-concern", "all"}); err == nil || !strings.Contains(err.Error(), "-engine hoarddb") {
		t.Errorf("-write-concern on postgres gave %v; want the refusal that sends the reader to hoardDB", err)
	}
	if _, err := parseFlags([]string{"-engine", "postgres", "-placement", "h64"}); err == nil || !strings.Contains(err.Error(), "one form") {
		t.Errorf("-placement h64 on postgres gave %v; want a refusal that says it has one form", err)
	}
	if _, err := parseFlags([]string{"-engine", "postgres", "-hoard-batch"}); err == nil {
		t.Error("-hoard-batch was accepted on postgres")
	}
	if _, err := parseFlags([]string{"-engine", "oracle"}); err == nil || !strings.Contains(err.Error(), "postgres") {
		t.Errorf("an unknown engine gave %v; want the list to name postgres", err)
	}
}

// TestPostgresTLSDefaultIsRecordedOnTheLeg: the facts a leg carries say whether
// the connection was encrypted, because the record-only TLS-off leg exists.
func TestPostgresTLSDefaultIsRecordedOnTheLeg(t *testing.T) {
	engine := &postgresEngine{cfg: &config{Engine: enginePostgres}, facts: map[string]string{"tls": "false"}}
	if engine.durability()["tls"] != "false" {
		t.Errorf("durability facts %v do not record tls", engine.durability())
	}
}

// TestPostgresNodeConfigs: a single node is its own convergence reference.
func TestPostgresNodeConfigs(t *testing.T) {
	nodes, err := nodeConfigs(context.Background(), &config{Engine: enginePostgres, PostgresDSN: "postgresql://bench@db.example:5432/social"})
	if err != nil || len(nodes) != 1 {
		t.Fatalf("node configs = %d, %v; want one", len(nodes), err)
	}
}

// TestPostgresWitnessWritesCommittedRowsInAScratchTable: each timed write is
// its own autocommit INSERT, between a CREATE and a DROP.
func TestPostgresWitnessWritesCommittedRowsInAScratchTable(t *testing.T) {
	name := t.Name() + "/node"
	engine := &postgresEngine{cfg: &config{Engine: enginePostgres}, db: recordingPool(t, name)}
	durations, err := engine.witnessWrites(context.Background(), 12)
	if err != nil || len(durations) != 12 {
		t.Fatalf("witnessWrites = %d durations, %v; want 12", len(durations), err)
	}
	statements := statementsOn(name)
	if len(statements) != 12+witnessWarmup+2 || !strings.Contains(statements[0], "CREATE TABLE") || !strings.Contains(statements[len(statements)-1], "DROP TABLE") {
		t.Fatalf("%d statements, first %q; want a CREATE, %d INSERTs and a DROP", len(statements), statements[0], 12+witnessWarmup)
	}
	if !strings.Contains(statements[1], "$1") || strings.Contains(statements[1], "?") {
		t.Errorf("the witness INSERT is %q; want numbered placeholders", statements[1])
	}
}

// TestPostgresDurabilityFacts: the engine reads the server's own flush
// settings from pg_settings rather than assume them, and a server that runs
// with fsync off is failed, not recorded as a durable leg.
func TestPostgresDurabilityFacts(t *testing.T) {
	for _, test := range []struct {
		name, fsync, commit string
		failed              bool
	}{
		{"durable", "on", "on", false},
		{"relaxed commit is recorded, not failed", "on", "off", false},
		{"fsync off fails the leg", "off", "on", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine, state := newFakePostgresEngine(t)
			state.answer("SELECT version()", []string{"version"}, []driver.Value{"PostgreSQL 17.6 on x86_64-pc-linux-gnu"})
			state.answer("FROM pg_settings", []string{"name", "setting"},
				[]driver.Value{"fsync", test.fsync}, []driver.Value{"synchronous_commit", test.commit},
				[]driver.Value{"full_page_writes", "on"}, []driver.Value{"wal_sync_method", "fdatasync"},
				[]driver.Value{"shared_buffers", "524288"}, []driver.Value{"max_connections", "200"})
			if err := engine.readFacts(context.Background()); err != nil {
				t.Fatal(err)
			}
			facts := engine.durability()
			if facts["version"] == "" || facts["fsync"] != test.fsync || facts["synchronous_commit"] != test.commit || facts["wal_sync_method"] != "fdatasync" || facts["full_page_writes"] != "on" {
				t.Errorf("facts = %v; want the version and the settings read back from pg_settings", facts)
			}
			if got := facts[durabilityFailureKey] != ""; got != test.failed {
				t.Errorf("failure recorded = %v (%q); want %v", got, facts[durabilityFailureKey], test.failed)
			}
		})
	}
}

// TestPostgresCheckEncryptionAsksTheServer: tlscheck holds -conc connections
// and believes the server's pg_stat_ssl row for each, not the client's own flag.
func TestPostgresCheckEncryptionAsksTheServer(t *testing.T) {
	for _, test := range []struct {
		name      string
		ssl       bool
		encrypted bool
	}{{"tls session", true, true}, {"plaintext session", false, false}} {
		t.Run(test.name, func(t *testing.T) {
			engine, state := newFakePostgresEngine(t)
			engine.cfg.Conc = 2
			engine.cfg.PostgresDSN = "postgresql://bench@db.example:5432/social"
			state.answer("FROM pg_stat_ssl", []string{"ssl", "version", "cipher"}, []driver.Value{test.ssl, "TLSv1.3", "TLS_AES_256_GCM_SHA384"})
			findings, err := engine.checkEncryption(context.Background())
			if err != nil || len(findings) != 1 {
				t.Fatalf("checkEncryption = %v, %v; want one finding", findings, err)
			}
			if findings[0].Encrypted != test.encrypted {
				t.Errorf("finding %+v; want encrypted=%v", findings[0], test.encrypted)
			}
		})
	}
}
