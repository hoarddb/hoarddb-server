package main

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestCockroachFlags: -engine cockroach parses with the flags every competitor
// takes, defaults to the plaintext single insecure node, and refuses what has
// no meaning for it.
func TestCockroachFlags(t *testing.T) {
	cfg, err := parseFlags([]string{"-engine", "cockroach", "-mix", "rh"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CockroachTLS || !strings.Contains(cfg.CockroachDSN, "26257/social") {
		t.Errorf("defaults are tls=%v dsn=%q; want the plaintext single insecure node on 26257", cfg.CockroachTLS, cfg.CockroachDSN)
	}
	if cfg.Fanout != fanoutSingle || cfg.nodeCount() != 1 {
		t.Errorf("fanout %q, %d nodes; want single and one node", cfg.Fanout, cfg.nodeCount())
	}
	for _, args := range [][]string{
		{"-engine", "cockroach", "-mix", "rh", "-fanout", "bulk"},
		{"-engine", "cockroach", "-mix", "rh", "-cockroach-tls"},
		{"-engine", "cockroach", "-mix", "rh", "-cockroach-dsn", "postgresql://root@db.example:26257/social?sslmode=disable"},
	} {
		if _, err := parseFlags(args); err != nil {
			t.Errorf("%v: %v", args, err)
		}
	}
	if _, err := parseFlags([]string{"-engine", "cockroach", "-write-concern", "all"}); err == nil || !strings.Contains(err.Error(), "-engine hoarddb") {
		t.Errorf("-write-concern on cockroach gave %v; want the refusal that sends the reader to hoardDB", err)
	}
	if _, err := parseFlags([]string{"-engine", "cockroach", "-placement", "h64"}); err == nil || !strings.Contains(err.Error(), "one form") {
		t.Errorf("-placement h64 on cockroach gave %v; want a refusal that says it has one form", err)
	}
	if _, err := parseFlags([]string{"-engine", "cockroach", "-hoard-batch"}); err == nil {
		t.Error("-hoard-batch was accepted on cockroach")
	}
	if _, err := parseFlags([]string{"-engine", "oracle"}); err == nil || !strings.Contains(err.Error(), "cockroach") {
		t.Errorf("an unknown engine gave %v; want the list to name cockroach", err)
	}
}

// TestCockroachTLSDefaultIsRecordedOnTheLeg: the facts a leg carries say whether
// the connection was encrypted, because the recipe's node is insecure.
func TestCockroachTLSDefaultIsRecordedOnTheLeg(t *testing.T) {
	engine := &cockroachEngine{cfg: &config{Engine: engineCockroach}, facts: map[string]string{"tls": "false"}}
	if engine.durability()["tls"] != "false" {
		t.Errorf("durability facts %v do not record tls", engine.durability())
	}
}

// TestCockroachNodeConfigs: a single node is its own convergence reference.
func TestCockroachNodeConfigs(t *testing.T) {
	nodes, err := nodeConfigs(context.Background(), &config{Engine: engineCockroach, CockroachDSN: "postgresql://root@db.example:26257/social"})
	if err != nil || len(nodes) != 1 {
		t.Fatalf("node configs = %d, %v; want one", len(nodes), err)
	}
}

// TestErrorKindNamesTheSQLState: a server refusal is bucketed by its
// SQLSTATE, so a 40001 storm is visible as its own line in errors_by_type.
func TestErrorKindNamesTheSQLState(t *testing.T) {
	err := wrapForTest(&pgconn.PgError{Code: "40001", Message: "restart transaction"})
	if got := errorKind(err); got != "server_postgres_40001" {
		t.Errorf("errorKind = %q; want server_postgres_40001", got)
	}
}

// TestCockroachWitnessWritesCommittedRowsInAScratchTable: each timed write is
// its own autocommit INSERT, between a CREATE and a DROP.
func TestCockroachWitnessWritesCommittedRowsInAScratchTable(t *testing.T) {
	name := t.Name() + "/node"
	engine := &cockroachEngine{cfg: &config{Engine: engineCockroach}, db: recordingPool(t, name)}
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

// TestCockroachDurabilityFactsAreRead: the engine reads the server's own sync
// setting rather than assume it, and says when the server does not know it.
func TestCockroachDurabilityFacts(t *testing.T) {
	engine, state := newFakeCockroachEngine(t)
	state.answer("SELECT version()", []string{"version"}, []driver.Value{"CockroachDB CCL v25.2.0 (x86_64-pc-linux-gnu)"})
	state.answer("SHOW CLUSTER SETTING kv.raft_log.disable_synchronization_unsafe", []string{"value"}, []driver.Value{"false"})
	if err := engine.readFacts(context.Background()); err != nil {
		t.Fatal(err)
	}
	facts := engine.durability()
	if facts["version"] == "" || facts["raft_log_sync_disabled"] != "false" || facts["wal_sync_on_commit"] != "default (on)" {
		t.Errorf("facts = %v; want the version, the raft-log setting read back and the default sync", facts)
	}
}
