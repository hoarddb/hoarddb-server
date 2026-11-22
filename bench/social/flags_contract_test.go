package main

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// contractFlags are the flags the shell harness calls (build doc S14, plus the
// early flags earlier dispatches added and left for this contract to absorb:
// -dataset, -repeat, -out, -verify-safety, -metrics-addr, -hist-prefix,
// -client-vms, -client-index, -rate, -mix, -attach). HARNESS.md must list every
// one with an example that parses.
var contractFlags = []string{
	"-mode", "-engine", "-dataset", "-placement", "-shards", "-addr", "-routing", "-mix", "-loop", "-depth", "-seconds",
	"-rate", "-client-vms", "-client-index", "-repeat", "-seed", "-out", "-variant", "-config", "-copies", "-level",
	"-write-concern", "-hoard-env", "-hoard-log", "-sync-ms", "-sync-overwrite-ms", "-sync-append-ms", "-witness-p50-ms", "-sync-dir", "-mongo-uri", "-mongo-wc", "-mongo-read-pref", "-mongo-tls",
	"-mysql-dsn", "-mysql-read-routing", "-mysql-tls", "-cockroach-dsn", "-cockroach-tls", "-postgres-dsn", "-postgres-tls", "-verify-safety", "-attach", "-hist-prefix", "-metrics-addr",
	"-sample", "-converge-timeout", "-warm-short", "-warm-converged", "-summarize", "-csv", "-login-concurrency", "-fanout",
	"-ledger-out", "-ledger", "-ledger-owner", "-wal-dir",
	"-hoard-batch", "-series-out", "-ring-fingerprint", "-ring-serial", "-ring-no-project",
}

var flagRow = regexp.MustCompile("^\\| `(-[a-z0-9-]+)` \\| .+ \\| `([^`]+)` \\|$")

// harnessFlagRows reads the flags table of HARNESS.md: flag name to the example argument list.
func harnessFlagRows(t *testing.T) map[string][]string {
	t.Helper()
	text, err := os.ReadFile("HARNESS.md")
	if err != nil {
		t.Fatalf("reading HARNESS.md: %v", err)
	}
	rows := map[string][]string{}
	for _, line := range strings.Split(string(text), "\n") {
		if match := flagRow.FindStringSubmatch(line); match != nil {
			rows[match[1]] = strings.Fields(match[2])
		}
	}
	return rows
}

// TestHelpListsHarnessFlags: -help names every flag HARNESS.md lists, so the
// shell harness can be written against either.
func TestHelpListsHarnessFlags(t *testing.T) {
	help := helpText()
	for name := range harnessFlagRows(t) {
		if !regexp.MustCompile("(?m)^  " + regexp.QuoteMeta(name) + "( |$)").MatchString(help) {
			t.Errorf("HARNESS.md lists %s, which -help does not show", name)
		}
	}
}

// TestHarnessFlagContract: the contract's flags are all listed, and every example
// in HARNESS.md is an argument list the tool accepts and whose flag it really sets.
func TestHarnessFlagContract(t *testing.T) {
	rows := harnessFlagRows(t)
	for _, name := range contractFlags {
		example, listed := rows[name]
		if !listed {
			t.Errorf("HARNESS.md does not list %s", name)
			continue
		}
		if !slices.ContainsFunc(example, func(argument string) bool { return argument == name || strings.HasPrefix(argument, name+"=") }) {
			t.Errorf("the example for %s, %q, does not use it", name, strings.Join(example, " "))
		}
		if _, err := parseFlags(example); err != nil {
			t.Errorf("the example for %s, %q, is refused: %v", name, strings.Join(example, " "), err)
		}
	}
	for name := range rows {
		if !slices.Contains(contractFlags, name) {
			t.Errorf("HARNESS.md lists %s but the contract test does not require it; add it to contractFlags", name)
		}
	}
}

// TestHarnessModesAreDocumented: every -mode the tool accepts has a row in the
// modes table, and every row names a mode the tool accepts.
func TestHarnessModesAreDocumented(t *testing.T) {
	text, err := os.ReadFile("HARNESS.md")
	if err != nil {
		t.Fatalf("reading HARNESS.md: %v", err)
	}
	modeRow := regexp.MustCompile("(?m)^\\| `([a-z][a-z-]*)` \\| ")
	documented := map[string]bool{}
	for _, match := range modeRow.FindAllStringSubmatch(string(text), -1) {
		documented[match[1]] = true
	}
	for _, mode := range knownModes() {
		if !documented[mode] {
			t.Errorf("-mode %s is not in HARNESS.md", mode)
		}
	}
	for mode := range documented {
		if !slices.Contains(knownModes(), mode) {
			t.Errorf("HARNESS.md documents -mode %s, which the tool does not know", mode)
		}
	}
}

// TestRenamedFlagsAreGone: the contract's names replaced the older ones outright
// (before v1.0 nothing keeps an alias), so an old script fails loudly.
func TestRenamedFlagsAreGone(t *testing.T) {
	for _, old := range []string{"-conc", "-duration", "-rand-seed"} {
		if _, err := parseFlags([]string{old, "4"}); err == nil {
			t.Errorf("%s still parses; the contract names are -depth, -seconds and -seed", old)
		}
	}
	cfg, err := parseFlags([]string{"-depth", "16", "-seconds", "2.5", "-seed", "9"})
	if err != nil || cfg.Conc != 16 || cfg.Duration.Seconds() != 2.5 || cfg.RandSeed != 9 {
		t.Errorf("-depth/-seconds/-seed parsed to %+v (%v)", cfg, err)
	}
	if _, err := parseFlags([]string{"-seconds", "0"}); err == nil {
		t.Error("-seconds 0 accepted")
	}
}

func TestPlacementLabels(t *testing.T) {
	cases := []struct {
		placement string
		shards    int
		users     int
		label     string
		count     int
		wantErr   bool
	}{
		{"h1", 0, 10_000, "single", 1, false},
		{"h64", 0, 10_000, "shard-64", 64, false},
		{"h64", 64, 10_000, "shard-64", 64, false},
		{"H256", 0, 10_000, "shard-256", 256, false},
		{"hu", 0, 10_000, "per-entity", 10_000, false},
		{"h64", 32, 10_000, "", 0, true},
		{"h2", 0, 10_000, "", 0, true},
	}
	for _, tc := range cases {
		label, count, err := placementLabel(tc.placement, tc.shards, tc.users)
		if (err != nil) != tc.wantErr || label != tc.label || count != tc.count {
			t.Errorf("placementLabel(%q, %d, %d) = %q, %d, %v; want %q, %d, error=%v", tc.placement, tc.shards, tc.users, label, count, err, tc.label, tc.count, tc.wantErr)
		}
	}
}

// TestRecordLabelsFollowThePlacementFlag: once an engine can lay a form out, the
// record names it: the label and the shard count come from -placement, not from a
// constant.
func TestRecordLabelsFollowThePlacementFlag(t *testing.T) {
	cases := []struct {
		placement, label string
		shards           int
	}{{"h1", "single", 1}, {"h64", "shard-64", 64}, {"h256", "shard-256", 256}, {"hu", "per-entity", tinySpec.Users}}
	for _, tc := range cases {
		record := newLegRecord()
		if err := labelRecord(record, &config{Engine: engineHoardDB, Placement: tc.placement}, tinySpec); err != nil {
			t.Fatalf("%s: %v", tc.placement, err)
		}
		if record.Placement != tc.label || record.Shards != tc.shards {
			t.Errorf("-placement %s recorded %q with %d shards; want %q with %d", tc.placement, record.Placement, record.Shards, tc.label, tc.shards)
		}
	}
}

// TestUnbuiltPlacementIsRefused: a record may not name a layout the data does
// not have, so the forms not built yet are refused, naming the phase they wait for.
func TestUnbuiltPlacementIsRefused(t *testing.T) {
	for _, placement := range []string{"h256"} {
		if _, err := parseFlags([]string{"-placement", placement}); err == nil || !strings.Contains(err.Error(), "phase 2") {
			t.Errorf("-placement %s error = %v; want a refusal that says phase 2", placement, err)
		}
	}
	for _, placement := range []string{"h1", "h64", "H64", "hu", "HU"} {
		if _, err := parseFlags([]string{"-placement", placement}); err != nil {
			t.Errorf("-placement %s: %v", placement, err)
		}
	}
}

// TestCompetitorsTakeTheirSingleFormOnly: MongoDB and MySQL have one layout, so
// a placement that names another is refused, and the single form is accepted.
func TestCompetitorsTakeTheirSingleFormOnly(t *testing.T) {
	for _, engineName := range []string{"mongo", "mysql", "cockroach", "postgres"} {
		_, err := parseFlags([]string{"-engine", engineName, "-placement", "h64"})
		if err == nil || !strings.Contains(err.Error(), "one form") {
			t.Errorf("-engine %s -placement h64: %v; want a refusal that says it has one form", engineName, err)
		}
		if _, err := parseFlags([]string{"-engine", engineName, "-placement", "h1"}); err != nil {
			t.Errorf("-engine %s -placement h1: %v", engineName, err)
		}
	}
}

func TestLoopFlagAgreesWithRate(t *testing.T) {
	good := [][]string{{"-loop", "closed"}, {"-loop", "open", "-rate", "100"}, {"-rate", "100"}, {}}
	for _, args := range good {
		if _, err := parseFlags(args); err != nil {
			t.Errorf("%v: %v", args, err)
		}
	}
	bad := [][]string{{"-loop", "open"}, {"-loop", "closed", "-rate", "100"}, {"-loop", "spiral"}}
	for _, args := range bad {
		if _, err := parseFlags(args); err == nil {
			t.Errorf("%v accepted", args)
		}
	}
}

// TestLegRecordNamesItsConfiguration: placement, shards, configuration, level,
// copies and the variant come from the flags into the record (carry-forward 4).
func TestLegRecordNamesItsConfiguration(t *testing.T) {
	cfg := mixTestConfig("rh", "social_labels")
	cfg.Addrs, cfg.Routing = []string{"a:1", "b:1", "c:1", "d:1", "e:1"}, routingB
	cfg.Config, cfg.Copies, cfg.Level, cfg.Variant, cfg.Placement = "C5b", 3, "L-R", "cache-raised", "h1"
	record := completedMixRecord(t, fakeHoardEngine(cfg), cfg)
	if record.Config != "C5b" || record.Nodes != 5 || record.Copies != 3 || record.Level != "L-R" || record.Placement != "single" || record.Shards != 1 {
		t.Errorf("record config=%q nodes=%d copies=%d level=%q placement=%q shards=%d", record.Config, record.Nodes, record.Copies, record.Level, record.Placement, record.Shards)
	}
	if record.EngineSettings["variant"] != "cache-raised" {
		t.Errorf("engine settings %v; want the variant recorded", record.EngineSettings)
	}
	cfg = mixTestConfig("rh", "social_labels2")
	cfg.Addrs = []string{"a:1", "b:1"}
	if record := completedMixRecord(t, fakeHoardEngine(cfg), cfg); record.Config != "C2" || record.Copies != 2 {
		t.Errorf("two nodes without -config recorded %q with %d copies; want C2 and 2", record.Config, record.Copies)
	}
	cfg = mixTestConfig("rh", "social_labels3")
	cfg.Addrs = []string{"a:1", "b:1", "c:1", "d:1", "e:1"}
	if _, err := runMixLeg(t.Context(), cfg, tinySpec, fakeHoardEngine(cfg)); err == nil || !strings.Contains(err.Error(), "-config") {
		t.Errorf("five nodes without -config: error = %v; want one that asks for -config", err)
	}
}
