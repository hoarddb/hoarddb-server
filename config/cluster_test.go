package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// existingEnvKeys are the product keys that predate the entry-ring scheme.
// They keep their names (conventions §16: "Existing product keys keep their
// names and are not part of this scheme"), so the derivation rule does not
// apply to them. Each value is the environment name the key already has; the
// test checks that the table still matches the struct tags, so it cannot rot
// silently.
var existingEnvKeys = map[string]string{
	"query.max_result_bytes":      "HOARDDB_QUERY_MAX_RESULT_BYTES",
	"query.concurrency":           "HOARDDB_QUERY_CONCURRENCY",
	"hash_ring.seed_nodes":        "HOARDDB_SEED_NODES",
	"hash_ring.seed_fingerprints": "HOARDDB_SEED_FINGERPRINTS",
}

// entryRingGroups are the three groups that conventions §16 names. Only the
// query group is shared with an existing struct; cluster and replication are
// new.
var entryRingGroups = []string{"cluster", "replication", "query"}

// topLevelConfigKeys is every top-level YAML key the configuration file
// accepted before the entry-ring scheme, plus the three groups it adds. Adding
// a key anywhere else at the top level is a bare name, and the scheme refuses
// it.
var topLevelConfigKeys = map[string]bool{
	"server": true, "tcp": true, "query": true, "tls": true, "auth": true,
	"logging": true, "metrics": true, "audit": true, "debug": true,
	"storage": true, "hash_ring": true, "encryption": true,
	"cluster": true, "replication": true,
}

// yamlFieldsOf returns the YAML name and struct field of every tagged field of
// the Config field whose YAML key is group.
func yamlFieldsOf(t *testing.T, group string) map[string]reflect.StructField {
	t.Helper()
	configType := reflect.TypeOf(Config{})
	for index := 0; index < configType.NumField(); index++ {
		outer := configType.Field(index)
		if outer.Tag.Get("yaml") != group {
			continue
		}
		fields := map[string]reflect.StructField{}
		for inner := 0; inner < outer.Type.NumField(); inner++ {
			field := outer.Type.Field(inner)
			name, _, _ := strings.Cut(field.Tag.Get("yaml"), ",")
			if name == "" || name == "-" {
				continue
			}
			fields[name] = field
		}
		return fields
	}
	t.Fatalf("Config has no field with yaml key %q", group)
	return nil
}

func TestTimingOrderingIsValidated(t *testing.T) {
	if err := DefaultCommittedTimings().Validate(); err != nil {
		t.Fatalf("the defaults must pass E11, got: %v", err)
	}

	// 2900 passes the old step_down < failover check (2900 < 5000) and is
	// refused by the real margin: 2900 + 100 > 2500.
	stepDown := DefaultCommittedTimings()
	stepDown.StepDownMS = 2900
	wantStepDown := "step_down_ms (2900) plus heartbeat_ms (100) must not exceed the failure-detection floor " +
		"(probe_failures - 1) x probe_interval_ms + probe_timeout_ms = 2500 ms: that is the earliest instant a majority can " +
		"commit a node down, so a node whose lease runs past it could still accept writes after it has been replaced. " +
		"Set 'step_down_ms' to 2000 (the default) or raise 'probe_interval_ms'. " +
		"The values leave 2500 - 2900 - 100 = -500 ms of margin."
	if err := stepDown.Validate(); err == nil || err.Error() != wantStepDown {
		t.Errorf("step_down_ms 2900 error = %v\nwant %q", err, wantStepDown)
	}

	// failover_ms below probe_failures x probe_interval_ms + probe_timeout_ms.
	failover := DefaultCommittedTimings()
	failover.FailoverMS = 3000
	wantFailover := "failover_ms (3000) must be at least probe_failures x probe_interval_ms + " +
		"probe_timeout_ms = 3500 ms, so the second observer's report reaches the leader while the window is open; " +
		"otherwise no member is ever declared down. Set 'failover_ms' to 3500 or more (the defaults give 3 x 1000 + 500 = 3500), " +
		"or lower probe_interval_ms."
	if err := failover.Validate(); err == nil || err.Error() != wantFailover {
		t.Errorf("failover_ms 3000 error = %v\nwant %q", err, wantFailover)
	}
}

// TestTimingMarginIsTheRealOne pins conventions §15's arithmetic. The lease
// margin is the real commit time of down minus the lease's reach:
// (probe_failures - 1) x probe_interval_ms + probe_timeout_ms - step_down_ms -
// heartbeat_ms. It is 400 ms at the defaults, and a set of timings whose margin
// is below 0 is refused. The sweep moves the probe interval too, so a check
// that hard-codes 2500 ms cannot pass every case.
func TestTimingMarginIsTheRealOne(t *testing.T) {
	defaults := DefaultCommittedTimings()
	if err := defaults.Validate(); err != nil {
		t.Fatalf("the defaults must pass E11, got: %v", err)
	}
	detection := (defaults.ProbeFailures-1)*defaults.ProbeIntervalMS + defaults.ProbeTimeoutMS
	if margin := detection - defaults.StepDownMS - defaults.HeartbeatMS; margin != 400 {
		t.Fatalf("the margin at the defaults = %d ms, want 400 (conventions §15)", margin)
	}

	cases := []struct {
		name        string
		intervalMS  int
		stepDownMS  int
		wantRefused bool
	}{
		{"defaults, margin 400", 1000, 2000, false},
		{"margin exactly 0", 1000, 2400, false},
		{"margin -1", 1000, 2401, true},
		{"2900 passes step_down < failover and is refused by the margin", 1000, 2900, true},
		{"probe_interval 2000, margin 0", 2000, 4400, false},
		{"probe_interval 2000, margin -1", 2000, 4401, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			timings := DefaultCommittedTimings()
			timings.ProbeIntervalMS = tc.intervalMS
			timings.StepDownMS = tc.stepDownMS
			// failover_ms must clear its own floor so only the margin can refuse.
			floor := timings.ProbeFailures*tc.intervalMS + timings.ProbeTimeoutMS
			timings.FailoverMS = max(defaultFailoverMS, floor)
			err := timings.Validate()
			if refused := err != nil; refused != tc.wantRefused {
				t.Errorf("step_down %d at probe_interval %d: refused = %v (err %v), want refused = %v",
					tc.stepDownMS, tc.intervalMS, refused, err, tc.wantRefused)
			}
		})
	}
}

// TestStepDownRefusalQuotesTheGivenValue pins the refusal text of membership §5: it
// quotes the given step_down_ms and heartbeat_ms, and the margin those values leave. The
// default is named only as the fix.
func TestStepDownRefusalQuotesTheGivenValue(t *testing.T) {
	heartbeat := DefaultCommittedTimings()
	heartbeat.StepDownMS = 2400
	heartbeat.HeartbeatMS = 150
	wantHeartbeat := "step_down_ms (2400) plus heartbeat_ms (150) must not exceed the failure-detection floor " +
		"(probe_failures - 1) x probe_interval_ms + probe_timeout_ms = 2500 ms: that is the earliest instant a majority can " +
		"commit a node down, so a node whose lease runs past it could still accept writes after it has been replaced. " +
		"Set 'step_down_ms' to 2000 (the default) or raise 'probe_interval_ms'. " +
		"The values leave 2500 - 2400 - 150 = -50 ms of margin."
	if err := heartbeat.Validate(); err == nil || err.Error() != wantHeartbeat {
		t.Errorf("step_down_ms 2400, heartbeat_ms 150 error = %v\nwant %q", err, wantHeartbeat)
	}
}

// TestDedupeWindowZeroIsRefused pins conventions §16: replication.dedupe_window
// is a positive count of applied records, and a zero or negative value is
// refused at start, through the file and through the environment, with the
// key, the default and the environment form in the text.
func TestDedupeWindowZeroIsRefused(t *testing.T) {
	const wantTail = "and it must be a positive number of applied records: a window of 0 or less leaves every dedupe entry dead, " +
		"so a retried request applies twice. Set 'replication.dedupe_window: 8192' (the default) in the configuration file, " +
		"or HOARDDB_REPLICATION_DEDUPE_WINDOW=8192, then restart this node."
	for _, value := range []string{"0", "-1"} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		content := []byte("replication:\n  dedupe_window: " + value + "\n")
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
		_, err := Load(path)
		want := "replication.dedupe_window is " + value + ", " + wantTail
		if err == nil || err.Error() != want {
			t.Errorf("dedupe_window %s from the file: Load error = %v\nwant %q", value, err, want)
		}
	}

	t.Setenv("HOARDDB_REPLICATION_DEDUPE_WINDOW", "0")
	_, err := Load("")
	if want := "replication.dedupe_window is 0, " + wantTail; err == nil || err.Error() != want {
		t.Errorf("dedupe_window 0 from the environment: Load error = %v\nwant %q", err, want)
	}
	t.Setenv("HOARDDB_REPLICATION_DEDUPE_WINDOW", "1")
	if _, err := Load(""); err != nil {
		t.Errorf("dedupe_window 1 must be accepted, got: %v", err)
	}
}

func TestVotersAreThreeOrFive(t *testing.T) {
	for _, voters := range []int{3, 5} {
		cfg := DefaultConfig()
		cfg.Cluster.Voters = voters
		if err := cfg.ValidateTimings(); err != nil {
			t.Errorf("voters %d must be accepted, got: %v", voters, err)
		}
	}
	for _, voters := range []int{2, 4, 7} {
		cfg := DefaultConfig()
		cfg.Cluster.Voters = voters
		if err := cfg.ValidateTimings(); err == nil {
			t.Errorf("voters %d must be refused at start", voters)
		}
	}
	cfg := DefaultConfig()
	cfg.Cluster.Voters = 4
	want := "cluster.voters is 4, and hoardDB runs 3 or 5 voters: with 2 neither node can commit while the other is down, " +
		"so writes stop instead of degrading, and with 4 a majority needs 3 nodes to agree. Set 'cluster.voters: 3' " +
		"(the default) or 'cluster.voters: 5' in the configuration file, or HOARDDB_CLUSTER_VOTERS=3, then restart this node."
	if err := cfg.ValidateTimings(); err == nil || err.Error() != want {
		t.Errorf("voters 4 error = %v\nwant %q", err, want)
	}
}

func TestEnvNamesAreDerived(t *testing.T) {
	if got, want := EnvName("replication.log_max_bytes"), "HOARDDB_REPLICATION_LOG_MAX_BYTES"; got != want {
		t.Errorf("EnvName(replication.log_max_bytes) = %q, want %q", got, want)
	}
	seen := map[string]bool{}
	for _, group := range entryRingGroups {
		for name, field := range yamlFieldsOf(t, group) {
			key := group + "." + name
			seen[key] = true
			if want, existing := existingEnvKeys[key]; existing {
				if got := field.Tag.Get("env"); got != want {
					t.Errorf("%s is an existing key: env tag %q, the exemption table says %q", key, got, want)
				}
				continue
			}
			if got, want := field.Tag.Get("env"), EnvName(key); got != want {
				t.Errorf("%s: env tag %q, the derived name is %q", key, got, want)
			}
		}
	}
	for _, key := range []string{"cluster.raft_heartbeat_ms", "replication.log_max_bytes", "query.page_limit_max"} {
		if !seen[key] {
			t.Errorf("the scheme has no field for %s", key)
		}
	}
}

func TestSeedKeysKeepTheirNames(t *testing.T) {
	hashRing := reflect.TypeOf(HashRingConfig{})
	for field, wantYAML := range map[string]string{"SeedNodes": "seed_nodes", "SeedFingerprints": "seed_fingerprints"} {
		structField, ok := hashRing.FieldByName(field)
		if !ok {
			t.Fatalf("HashRingConfig has no field %s", field)
		}
		if got, _, _ := strings.Cut(structField.Tag.Get("yaml"), ","); got != wantYAML {
			t.Errorf("%s yaml name %q, want %q", field, got, wantYAML)
		}
	}
	// The file form: a strict decode of the names the tree already ships
	// must accept them, so an existing configuration file needs no rewrite.
	cfg := DefaultConfig()
	content := []byte("hash_ring:\n  seed_nodes: [\"192.0.2.1:4433\"]\n  seed_fingerprints: [\"SHA256:abc\"]\n")
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := decodeConfigFile(path, content, cfg); err != nil {
		t.Fatalf("decode the shipped seed names: %v", err)
	}
	if len(cfg.HashRing.SeedNodes) != 1 || len(cfg.HashRing.SeedFingerprints) != 1 {
		t.Errorf("seed names did not decode: %+v", cfg.HashRing)
	}
}

func TestNoBareNamesOutsideTheTwoSeedKeys(t *testing.T) {
	configType := reflect.TypeOf(Config{})
	for index := 0; index < configType.NumField(); index++ {
		name, _, _ := strings.Cut(configType.Field(index).Tag.Get("yaml"), ",")
		if name != "" && name != "-" && !topLevelConfigKeys[name] {
			t.Errorf("top-level key %q is a bare name; the entry-ring scheme puts every new key in cluster., replication. or query.", name)
		}
	}
}

func TestLoadRunsTheStartupCheck(t *testing.T) {
	content := []byte("cluster:\n  voters: 4\n")
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "cluster.voters is 4") {
		t.Errorf("Load with voters 4 = %v; the start-up check must refuse it before the node joins", err)
	}
}

func TestEnvOverridesReachTheNewKeys(t *testing.T) {
	t.Setenv("HOARDDB_CLUSTER_VOTERS", "5")
	t.Setenv("HOARDDB_QUERY_PAGE_LIMIT_MAX", "20000")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Cluster.Voters != 5 || cfg.Query.PageLimitMax != 20000 {
		t.Errorf("environment did not reach the new keys: cluster %+v, query %+v", cfg.Cluster, cfg.Query)
	}
	t.Setenv("HOARDDB_CLUSTER_VOTERS", "five")
	if _, err := Load(""); err == nil || !strings.Contains(err.Error(), "HOARDDB_CLUSTER_VOTERS=\"five\" is not a whole number") {
		t.Errorf("a non-numeric voters value = %v; it must be refused naming the variable", err)
	}
}

// The compile-time assertion pins the width of replication.log_max_bytes. Its
// default is 4 GiB, which a 32-bit int cannot hold, so the field must be int64
// on every build. With the field as int this file does not compile on any
// platform, which is the failure the guard exists to make loud.
var _ int64 = ReplicationConfig{}.LogMaxBytes

// TestLogMaxBytesReadsBackAbove32Bits sets a value above 2^32 through the
// environment and reads it back, so the env path cannot narrow it.
func TestLogMaxBytesReadsBackAbove32Bits(t *testing.T) {
	t.Setenv("HOARDDB_REPLICATION_LOG_MAX_BYTES", "4294967296")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Replication.LogMaxBytes != 4294967296 {
		t.Errorf("replication.log_max_bytes from the environment = %d, want 4294967296", cfg.Replication.LogMaxBytes)
	}
}

// TestPeerLimitDefaultsAreTheDocumentedOnes pins the numbers conventions §16 gives for the
// internode bounds; cluster/wire keeps a test that its own defaults equal these.
func TestPeerLimitDefaultsAreTheDocumentedOnes(t *testing.T) {
	got := DefaultClusterConfig()
	want := map[string][2]int{
		"peer_max_pending": {got.PeerMaxPending, 128}, "peer_max_conns": {got.PeerMaxConns, 256},
		"peer_max_in_flight": {got.PeerMaxInFlight, 256}, "peer_idle_seconds": {got.PeerIdleSeconds, 120},
		"peer_frame_timeout_ms": {got.PeerFrameTimeoutMS, 10000}, "raft_max_conns": {got.RaftMaxConns, 64},
		"raft_idle_seconds": {got.RaftIdleSeconds, 120},
	}
	for key, pair := range want {
		if pair[0] != pair[1] {
			t.Errorf("cluster.%s defaults to %d, conventions §16 says %d", key, pair[0], pair[1])
		}
	}
	cfg := DefaultConfig()
	if err := cfg.ValidateTimings(); err != nil {
		t.Errorf("the defaults fail the start-up check: %v", err)
	}
}

// TestANegativePeerLimitIsRefusedByName: [GUARD: skip the negative check]
func TestANegativePeerLimitIsRefusedByName(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Cluster.PeerMaxConns = -1
	err := cfg.ValidateTimings()
	if err == nil || !strings.Contains(err.Error(), "cluster.peer_max_conns is -1") {
		t.Fatalf("a negative peer_max_conns: %v, want a refusal that names the key and the value", err)
	}
}

// TestARaftIdleBoundUnderTheRaftTimersIsRefused: a connection closed between a healthy
// leader's messages would make every RPC redial. [GUARD: drop the idle/timer comparison]
func TestARaftIdleBoundUnderTheRaftTimersIsRefused(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Cluster.RaftElectionMS = 30000
	cfg.Cluster.RaftIdleSeconds = 20
	err := cfg.ValidateTimings()
	if err == nil || !strings.Contains(err.Error(), "cluster.raft_idle_seconds (20)") || !strings.Contains(err.Error(), "at least 300") {
		t.Fatalf("raft_idle_seconds 20 under 30 s timers: %v, want the refusal naming the key and the minimum 300", err)
	}
	cfg.Cluster.RaftIdleSeconds = 0
	if err := cfg.ValidateTimings(); err != nil {
		t.Fatalf("zero takes the default and must pass: %v", err)
	}
}

func TestPeerLimitEnvOverridesReachTheKeys(t *testing.T) {
	t.Setenv("HOARDDB_CLUSTER_PEER_MAX_IN_FLIGHT", "17")
	t.Setenv("HOARDDB_CLUSTER_RAFT_IDLE_SECONDS", "301")
	cfg := DefaultConfig()
	if err := applyEntryRingEnv(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Cluster.PeerMaxInFlight != 17 || cfg.Cluster.RaftIdleSeconds != 301 {
		t.Fatalf("env overrides gave in_flight %d and raft idle %d, want 17 and 301", cfg.Cluster.PeerMaxInFlight, cfg.Cluster.RaftIdleSeconds)
	}
}

// TestRetentionCheckSecondsIsValidatedAndReadFromTheEnvironment: replication.retention_check_seconds
// (storage-format spec §3.7) defaults to 60, is refused outside 1 to 3600 naming the key and
// the range, and reaches the field from its derived environment name.
func TestRetentionCheckSecondsIsValidatedAndReadFromTheEnvironment(t *testing.T) {
	if got := DefaultReplicationConfig().RetentionCheckSeconds; got != 60 {
		t.Fatalf("replication.retention_check_seconds defaults to %d, want 60", got)
	}
	for _, bad := range []string{"0", "3601", "-5"} {
		t.Setenv("HOARDDB_REPLICATION_RETENTION_CHECK_SECONDS", bad)
		_, err := Load("")
		if err == nil || !strings.Contains(err.Error(), "replication.retention_check_seconds is "+bad) || !strings.Contains(err.Error(), "1 to 3600") {
			t.Errorf("HOARDDB_REPLICATION_RETENTION_CHECK_SECONDS=%s: Load = %v; want a refusal naming the key, the value and the range 1 to 3600", bad, err)
		}
	}
	t.Setenv("HOARDDB_REPLICATION_RETENTION_CHECK_SECONDS", "120")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Replication.RetentionCheckSeconds != 120 {
		t.Fatalf("replication.retention_check_seconds from the environment = %d, want 120", cfg.Replication.RetentionCheckSeconds)
	}
}

// TestNearestMaxElementsDefaultsEnvAndClamp pins query.nearest_max_elements: the
// default, the environment name, and the clamp to 1..10000000 on load.
func TestNearestMaxElementsDefaultsEnvAndClamp(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Query.NearestMaxElements != 100000 {
		t.Errorf("default NearestMaxElements = %d, want 100000", cfg.Query.NearestMaxElements)
	}
	for _, tc := range []struct {
		name string
		env  string
		want int
	}{
		{"set", "5", 5},
		{"zero clamps to one", "0", 1},
		{"large clamps to ten million", "99999999", 10000000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOARDDB_QUERY_NEAREST_MAX_ELEMENTS", tc.env)
			cfg, err := Load("")
			if err != nil {
				t.Fatalf("Load with %s: %v", tc.env, err)
			}
			if cfg.Query.NearestMaxElements != tc.want {
				t.Errorf("NearestMaxElements with env %s = %d, want %d", tc.env, cfg.Query.NearestMaxElements, tc.want)
			}
		})
	}
}
