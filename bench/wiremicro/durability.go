package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// errRelaxedDurability marks a refusal: a server setting the tool checks is
// weaker than the durable setting the comparison states.
var errRelaxedDurability = errors.New("durability setting is relaxed")

// variableReader reads the durability settings of the server under test, as
// the server (or, for hoardDB, the launch record) states them. The keys are
// the ones judgeDurability knows. A fake serves them in tests.
type variableReader interface {
	readVariables(ctx context.Context) (map[string]string, error)
}

// checkDurability reads the settings, returns them for printing, and refuses
// (errRelaxedDurability) when one is weaker than durable. A setting it cannot
// read counts as relaxed: an unknown setting proves nothing.
func checkDurability(ctx context.Context, engine string, reader variableReader) (map[string]string, error) {
	vars, err := reader.readVariables(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading the %s durability settings: %w", engine, err)
	}
	problems, err := judgeDurability(engine, vars)
	if err != nil {
		return nil, err
	}
	if len(problems) > 0 {
		return vars, fmt.Errorf("refusing to run, %s is not at durable settings: %s: %w", engine, strings.Join(problems, "; "), errRelaxedDurability)
	}
	return vars, nil
}

// judgeDurability lists what is relaxed; empty means durable.
func judgeDurability(engine string, vars map[string]string) ([]string, error) {
	switch engine {
	case engineMySQL:
		return judgeMySQL(vars), nil
	case enginePostgres:
		return judgePostgres(vars), nil
	case engineMongo:
		return judgeMongo(vars), nil
	case engineCockroach:
		return judgeCockroach(vars), nil
	case engineHoarddb:
		return judgeHoarddb(vars), nil
	default:
		return nil, fmt.Errorf("no durability rule for engine %q", engine)
	}
}

func judgeMySQL(vars map[string]string) []string {
	var problems []string
	if vars["innodb_flush_log_at_trx_commit"] != "1" {
		problems = append(problems, fmt.Sprintf("innodb_flush_log_at_trx_commit is %q, it must be 1 (run 'SET GLOBAL innodb_flush_log_at_trx_commit = 1')", vars["innodb_flush_log_at_trx_commit"]))
	}
	if vars["sync_binlog"] != "1" {
		problems = append(problems, fmt.Sprintf("sync_binlog is %q, it must be 1 (run 'SET GLOBAL sync_binlog = 1')", vars["sync_binlog"]))
	}
	return problems
}

func judgePostgres(vars map[string]string) []string {
	var problems []string
	for _, rule := range []struct{ name, fix string }{
		{"synchronous_commit", "a commit must wait for the WAL flush (run 'ALTER SYSTEM SET synchronous_commit = on' and reload)"},
		{"fsync", "the server must flush to disk (run 'ALTER SYSTEM SET fsync = on' and reload)"},
		{"full_page_writes", "torn pages must be guarded (run 'ALTER SYSTEM SET full_page_writes = on' and reload)"},
	} {
		if vars[rule.name] != "on" {
			problems = append(problems, fmt.Sprintf("%s is %q, it must be on: %s", rule.name, vars[rule.name], rule.fix))
		}
	}
	return problems
}

func judgeMongo(vars map[string]string) []string {
	var problems []string
	if vars["write_concern_j"] != "true" {
		problems = append(problems, fmt.Sprintf("write_concern_j is %q, it must be true: the write must wait for the journal (w:1, j:true)", vars["write_concern_j"]))
	}
	if w := vars["write_concern_w"]; w == "" || w == "0" {
		problems = append(problems, fmt.Sprintf("write_concern_w is %q, it must be 1 or more or majority", w))
	}
	if vars["storage_persistent"] != "true" {
		problems = append(problems, fmt.Sprintf("storage_persistent is %q, it must be true: an in-memory storage engine is not durable", vars["storage_persistent"]))
	}
	return problems
}

func judgeCockroach(vars map[string]string) []string {
	if vars["raft_log_sync_disabled"] != "false" {
		return []string{fmt.Sprintf("raft_log_sync_disabled is %q, it must be false: kv.raft_log.disable_synchronization_unsafe lets commits be acknowledged before the log is synced (run 'SET CLUSTER SETTING kv.raft_log.disable_synchronization_unsafe = false')", vars["raft_log_sync_disabled"])}
	}
	return nil
}

func judgeHoarddb(vars map[string]string) []string {
	var problems []string
	switch strings.ToLower(vars["unsafe_ack_before_sync"]) {
	case "unset", "", "false", "0", "no":
	default:
		problems = append(problems, fmt.Sprintf("HOARDDB_UNSAFE_ACK_BEFORE_SYNC is %q in the launch environment, it must be unset: the server would acknowledge before the sync", vars["unsafe_ack_before_sync"]))
	}
	stated := vars["table_sync_stated"]
	if stated != "stepwise" && stated != "msync" {
		problems = append(problems, fmt.Sprintf("table_sync_stated is %q, state stepwise or msync with -hoard-table-sync", stated))
	}
	if launched := vars["table_sync_launch_env"]; launched != "unset" && launched != "" && launched != stated {
		problems = append(problems, fmt.Sprintf("HOARDDB_BADGER_TABLE_SYNC is %q in the launch environment but -hoard-table-sync states %q: the run would measure a different mode than it reports", launched, stated))
	}
	return problems
}

// parseLaunchEnv reads KEY=VALUE lines as a shell would export them: blank
// lines and comments are skipped, "export " is dropped, and one pair of quotes
// around a value is removed.
func parseLaunchEnv(text string) map[string]string {
	env := map[string]string{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "export "))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, rest, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		env[strings.TrimSpace(name)] = strings.Trim(strings.TrimSpace(rest), `"'`)
	}
	return env
}

// hoardLaunchVariables turns the server's launch environment and the table-sync
// mode the operator states into the variables judgeHoarddb reads.
func hoardLaunchVariables(envText, tableSyncStated string) map[string]string {
	env := parseLaunchEnv(envText)
	return map[string]string{
		"unsafe_ack_before_sync": valueOrUnset(env["HOARDDB_UNSAFE_ACK_BEFORE_SYNC"]),
		"table_sync_launch_env":  valueOrUnset(env["HOARDDB_BADGER_TABLE_SYNC"]),
		"table_sync_stated":      tableSyncStated,
	}
}

func valueOrUnset(value string) string {
	if value == "" {
		return "unset"
	}
	return value
}
