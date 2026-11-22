package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The live tests drive a real server and are skipped without WIREMICRO_LIVE_<ENGINE>
// set to its host:port. Credentials come from WIREMICRO_PASSWORD and WIREMICRO_USER as
// in a real run; hoarddb also needs WIREMICRO_LIVE_HOARD_ENV (the server's launch
// environment file) and WIREMICRO_LIVE_HOARD_TABLE_SYNC.

func liveRun(t *testing.T, engineName, addrVar string, extra ...string) {
	t.Helper()
	addr := os.Getenv(addrVar)
	if addr == "" {
		t.Skipf("%s is not set: no %s server to test against", addrVar, engineName)
	}
	out := filepath.Join(t.TempDir(), "live.json")
	args := append([]string{"-engine", engineName, "-addr", addr, "-smoke", "-ops", "200", "-workers", "4",
		"-preload", "50", "-run-id", time.Now().UTC().Format("150405"), "-out", out}, extra...)
	var table, progress bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := run(ctx, args, os.Getenv, &table, &progress); err != nil {
		t.Fatalf("%v\n%s", err, progress.String())
	}
	if _, err := os.Stat(out); err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + table.String())
}

func TestLiveMySQL(t *testing.T)     { liveRun(t, engineMySQL, "WIREMICRO_LIVE_MYSQL") }
func TestLiveMongo(t *testing.T)     { liveRun(t, engineMongo, "WIREMICRO_LIVE_MONGO") }
func TestLivePostgres(t *testing.T)  { liveRun(t, enginePostgres, "WIREMICRO_LIVE_POSTGRES") }
func TestLiveCockroach(t *testing.T) { liveRun(t, engineCockroach, "WIREMICRO_LIVE_COCKROACH") }
func TestLiveHoarddb(t *testing.T) {
	env, sync := os.Getenv("WIREMICRO_LIVE_HOARD_ENV"), os.Getenv("WIREMICRO_LIVE_HOARD_TABLE_SYNC")
	if os.Getenv("WIREMICRO_LIVE_HOARDDB") != "" && (env == "" || sync == "") {
		t.Fatal("set WIREMICRO_LIVE_HOARD_ENV and WIREMICRO_LIVE_HOARD_TABLE_SYNC beside WIREMICRO_LIVE_HOARDDB")
	}
	liveRun(t, engineHoarddb, "WIREMICRO_LIVE_HOARDDB", "-hoard-env", env, "-hoard-table-sync", sync)
}
