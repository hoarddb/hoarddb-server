package main

import (
	"os"
	"strings"
	"testing"
)

func TestPostgresBenchDSNNeedsTheEnvironment(t *testing.T) {
	t.Setenv("HOARDDB_BENCH_POSTGRES_DSN", "")
	if _, err := postgresBenchDSN(); err == nil || !strings.Contains(err.Error(), "HOARDDB_BENCH_POSTGRES_DSN") {
		t.Fatalf("err %v, want it to name the variable", err)
	}
	t.Setenv("HOARDDB_BENCH_POSTGRES_DSN", "postgresql://u@h/db")
	if dsn, err := postgresBenchDSN(); err != nil || dsn != "postgresql://u@h/db" {
		t.Fatalf("dsn %q err %v", dsn, err)
	}
}

func TestPostgresBatchInsertUsesNumberedPlaceholders(t *testing.T) {
	query, args := postgresBatchInsert(10, 13)
	if strings.Contains(query, "?") || !strings.HasSuffix(query, "($1, $2),($3, $4),($5, $6)") {
		t.Fatalf("query %q", query)
	}
	if len(args) != 6 || args[0] != "user_000010" || args[1] != "u10@test.com" || args[4] != "user_000012" {
		t.Fatalf("args %v", args)
	}
}

func TestPostgresBenchRefusesRelaxedDurability(t *testing.T) {
	durable := map[string]string{"synchronous_commit": "on", "fsync": "on", "full_page_writes": "on"}
	if err := checkPostgresDurable(durable); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"synchronous_commit", "fsync", "full_page_writes"} {
		relaxed := map[string]string{"synchronous_commit": "on", "fsync": "on", "full_page_writes": "on"}
		relaxed[name] = "off"
		if err := checkPostgresDurable(relaxed); err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("%s off: err %v, want a refusal naming it", name, err)
		}
	}
	if err := checkPostgresDurable(map[string]string{}); err == nil {
		t.Error("unread settings must be refused")
	}
}

// TestLivePostgresBench runs a tiny leg when HOARDDB_BENCH_POSTGRES_DSN is set.
func TestLivePostgresBench(t *testing.T) {
	if os.Getenv("HOARDDB_BENCH_POSTGRES_DSN") == "" {
		t.Skip("HOARDDB_BENCH_POSTGRES_DSN is not set: no PostgreSQL server to test against")
	}
	if err := runPostgresBench("batch", 200, 200, 4); err != nil {
		t.Fatal(err)
	}
	if err := runPostgresBench("single", 50, 50, 1); err != nil {
		t.Fatal(err)
	}
}
