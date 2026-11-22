package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type fakeReader struct {
	vars map[string]string
	err  error
}

func (f fakeReader) readVariables(context.Context) (map[string]string, error) { return f.vars, f.err }

func TestDurableSettingsPass(t *testing.T) {
	good := map[string]map[string]string{
		"mysql":     {"innodb_flush_log_at_trx_commit": "1", "sync_binlog": "1", "version": "8.4"},
		"mongo":     {"write_concern_w": "1", "write_concern_j": "true", "storage_persistent": "true"},
		"cockroach": {"raft_log_sync_disabled": "false", "version": "v25.2"},
		"postgres":  {"synchronous_commit": "on", "fsync": "on", "full_page_writes": "on", "version": "17.2"},
		"hoarddb":   {"unsafe_ack_before_sync": "unset", "table_sync_stated": "stepwise", "table_sync_launch_env": "unset"},
	}
	for engine, vars := range good {
		seen, err := checkDurability(context.Background(), engine, fakeReader{vars: vars})
		if err != nil {
			t.Errorf("%s: %v", engine, err)
		}
		if len(seen) != len(vars) {
			t.Errorf("%s: observed %v, want every variable echoed back", engine, seen)
		}
	}
}

func TestRelaxedSettingsAreRefused(t *testing.T) {
	cases := []struct {
		name   string
		engine string
		vars   map[string]string
		want   string
	}{
		{"mysql flush 2", "mysql", map[string]string{"innodb_flush_log_at_trx_commit": "2", "sync_binlog": "1"}, "innodb_flush_log_at_trx_commit"},
		{"mysql flush 0", "mysql", map[string]string{"innodb_flush_log_at_trx_commit": "0", "sync_binlog": "1"}, "innodb_flush_log_at_trx_commit"},
		{"mysql binlog 0", "mysql", map[string]string{"innodb_flush_log_at_trx_commit": "1", "sync_binlog": "0"}, "sync_binlog"},
		{"mysql missing", "mysql", map[string]string{"sync_binlog": "1"}, "innodb_flush_log_at_trx_commit"},
		{"mongo no journal", "mongo", map[string]string{"write_concern_w": "1", "write_concern_j": "false", "storage_persistent": "true"}, "write_concern_j"},
		{"mongo w0", "mongo", map[string]string{"write_concern_w": "0", "write_concern_j": "true", "storage_persistent": "true"}, "write_concern_w"},
		{"mongo in memory", "mongo", map[string]string{"write_concern_w": "1", "write_concern_j": "true", "storage_persistent": "false"}, "storage_persistent"},
		{"postgres async commit", "postgres", map[string]string{"synchronous_commit": "off", "fsync": "on", "full_page_writes": "on"}, "synchronous_commit"},
		{"postgres fsync off", "postgres", map[string]string{"synchronous_commit": "on", "fsync": "off", "full_page_writes": "on"}, "fsync"},
		{"postgres no full page writes", "postgres", map[string]string{"synchronous_commit": "on", "fsync": "on", "full_page_writes": "off"}, "full_page_writes"},
		{"postgres missing", "postgres", map[string]string{}, "synchronous_commit"},
		{"cockroach unsafe", "cockroach", map[string]string{"raft_log_sync_disabled": "true"}, "disable_synchronization_unsafe"},
		{"cockroach missing", "cockroach", map[string]string{}, "raft_log_sync_disabled"},
		{"hoard unsafe ack", "hoarddb", map[string]string{"unsafe_ack_before_sync": "true", "table_sync_stated": "stepwise", "table_sync_launch_env": "unset"}, "HOARDDB_UNSAFE_ACK_BEFORE_SYNC"},
		{"hoard table sync mismatch", "hoarddb", map[string]string{"unsafe_ack_before_sync": "unset", "table_sync_stated": "stepwise", "table_sync_launch_env": "msync"}, "HOARDDB_BADGER_TABLE_SYNC"},
		{"hoard table sync unstated", "hoarddb", map[string]string{"unsafe_ack_before_sync": "unset", "table_sync_stated": "", "table_sync_launch_env": "unset"}, "table_sync_stated"},
	}
	for _, c := range cases {
		_, err := checkDurability(context.Background(), c.engine, fakeReader{vars: c.vars})
		if !errors.Is(err, errRelaxedDurability) {
			t.Errorf("%s: err %v, want errRelaxedDurability", c.name, err)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err %q does not name %q", c.name, err, c.want)
		}
	}
}

func TestHoardUnsafeAckSpellingsAllCount(t *testing.T) {
	for _, spelling := range []string{"true", "TRUE", "1", "yes"} {
		vars := map[string]string{"unsafe_ack_before_sync": spelling, "table_sync_stated": "msync", "table_sync_launch_env": "unset"}
		if _, err := checkDurability(context.Background(), "hoarddb", fakeReader{vars: vars}); !errors.Is(err, errRelaxedDurability) {
			t.Errorf("%q was accepted", spelling)
		}
	}
}

func TestAReaderFailureIsNotRelaxedButStopsTheRun(t *testing.T) {
	_, err := checkDurability(context.Background(), "mysql", fakeReader{err: errors.New("connection refused")})
	if err == nil || errors.Is(err, errRelaxedDurability) || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("err %v", err)
	}
}

func TestUnknownEngineHasNoJudge(t *testing.T) {
	if _, err := checkDurability(context.Background(), "redis", fakeReader{}); err == nil {
		t.Fatal("an engine with no durability rule must not pass")
	}
}

func TestLaunchEnvironmentParsingMatchesAShell(t *testing.T) {
	got := parseLaunchEnv("# comment\n\nexport HOARDDB_UNSAFE_ACK_BEFORE_SYNC=\"true\"\nHOARDDB_BADGER_TABLE_SYNC='stepwise'\nbare\n")
	if got["HOARDDB_UNSAFE_ACK_BEFORE_SYNC"] != "true" || got["HOARDDB_BADGER_TABLE_SYNC"] != "stepwise" || len(got) != 2 {
		t.Fatalf("got %v", got)
	}
}

func TestHoardLaunchVariablesReadTheEnvFile(t *testing.T) {
	vars := hoardLaunchVariables("HOARDDB_UNSAFE_ACK_BEFORE_SYNC=true\nHOARDDB_BADGER_TABLE_SYNC=msync\n", "stepwise")
	if vars["unsafe_ack_before_sync"] != "true" || vars["table_sync_launch_env"] != "msync" || vars["table_sync_stated"] != "stepwise" {
		t.Fatalf("got %v", vars)
	}
	empty := hoardLaunchVariables("", "stepwise")
	if empty["unsafe_ack_before_sync"] != "unset" || empty["table_sync_launch_env"] != "unset" {
		t.Fatalf("got %v", empty)
	}
}
