package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// TestPostgresDDL pins the translation: no MySQL type survives, every secondary key becomes a
// CREATE INDEX, and the memories table says where a pgvector column would go.
func TestPostgresDDL(t *testing.T) {
	for _, layout := range model.Layouts {
		var statements []string
		for _, line := range strings.Split(postgresDDL(layout, "scenarios_"+string(layout)), "\n") {
			if !strings.HasPrefix(line, "--") {
				statements = append(statements, line)
			}
		}
		text := strings.Join(statements, "\n")
		for _, mysqlOnly := range []string{"VARCHAR", "TINYINT", "BLOB", "ENGINE=", "utf8mb4", "  KEY ", "UNIQUE KEY"} {
			if strings.Contains(text, mysqlOnly) {
				t.Errorf("%s: PostgreSQL text still holds %q", layout, mysqlOnly)
			}
		}
		keys := 0
		for _, spec := range tables(layout) {
			keys += len(spec.Keys)
		}
		if got := countLines(text, "CREATE INDEX ", false) + countLines(text, "CREATE UNIQUE INDEX ", false); got != keys {
			t.Errorf("%s: %d CREATE INDEX lines, want %d", layout, got, keys)
		}
	}
	agent := postgresDDL(model.Agent, "scenarios_agent")
	if !strings.Contains(agent, "vec BYTEA NOT NULL") || !strings.Contains(agent, "vector(64)") {
		t.Errorf("memories must hold a plain BYTEA and mention vector(64):\n%s", agent)
	}
	if got := pgColumnType("TINYINT NOT NULL"); got != "SMALLINT NOT NULL" {
		t.Errorf("TINYINT became %q", got)
	}
}

func TestPostgresTLSRules(t *testing.T) {
	base := "postgres://bench:bench@localhost:5432/postgres"
	on := config{PostgresDSN: base, PostgresTLS: true}
	target, err := postgresConfig(on)
	if err != nil || target.TLSConfig == nil || !target.TLSConfig.InsecureSkipVerify || len(target.Fallbacks) != 0 {
		t.Fatalf("tls on must force TLS without verification: %+v %v", target, err)
	}
	verified := on
	verified.PostgresDSN = base + "?sslmode=verify-ca"
	if target, err := postgresConfig(verified); err != nil || target.TLSConfig == nil || (target.TLSConfig.VerifyConnection == nil && target.TLSConfig.VerifyPeerCertificate == nil) {
		t.Fatalf("a verifying sslmode of the DSN must be kept: %+v %v", target, err)
	}
	undo := on
	undo.PostgresDSN = base + "?sslmode=disable"
	if _, err := postgresConfig(undo); err == nil || !strings.Contains(err.Error(), "-postgres-tls") {
		t.Fatalf("sslmode=disable against -postgres-tls must be refused with the fix: %v", err)
	}
	off := on
	off.PostgresTLS = false
	if target, err := postgresConfig(off); err != nil || target.TLSConfig != nil || len(target.Fallbacks) != 0 {
		t.Fatalf("tls off must be plain text: %+v %v", target, err)
	}
	if _, err := postgresConfig(config{PostgresDSN: "postgres://bad host/"}); err == nil {
		t.Fatal("a bad DSN was accepted")
	}
}

// TestSeedPostgres seeds every layout into a real server and skips when POSTGRES_DSN is unset.
// The databases it creates carry a time-based name and are dropped afterwards.
func TestSeedPostgres(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN is not set; set it to postgres://user:password@host:5432/postgres?sslmode=disable to run against a PostgreSQL server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	base := fmt.Sprintf("scn_%d", time.Now().UnixNano()%1_000_000_000)
	cfg := config{Engine: "postgres", Layout: layoutAll, Scale: 1, Seed: 1, Database: base, Workers: 4, PostgresDSN: dsn, PostgresTLS: !strings.Contains(dsn, "sslmode=disable")}
	t.Cleanup(func() {
		target, err := postgresConfig(cfg)
		if err != nil {
			return
		}
		admin := openPostgres(target, 1)
		defer func() { _ = admin.Close() }()
		dropAll(admin, base)
	})
	var stdout, stderr bytes.Buffer
	if err := run(ctx, cfg, &stdout, &stderr); err != nil {
		t.Fatalf("%v\n%s", err, stderr.String())
	}
	if lines := strings.Count(stdout.String(), "\n"); lines != len(model.Layouts) {
		t.Fatalf("want one JSON line per layout, got %d", lines)
	}
	again := run(ctx, cfg, &bytes.Buffer{}, &bytes.Buffer{})
	if again == nil || !strings.Contains(again.Error(), "-database") || !strings.Contains(again.Error(), "already exists") {
		t.Fatalf("a second seed must be refused with the way out: %v", again)
	}
}

func dropAll(admin *sql.DB, base string) {
	for _, layout := range model.Layouts {
		_, _ = admin.Exec("DROP DATABASE IF EXISTS " + layoutDatabase(base, layout))
	}
}
