package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func writeYAML(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hoarddb.yaml")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestZeroConfigDefaults(t *testing.T) {
	t.Setenv("HOARDDB_DATA_DIR", "")
	t.Setenv("HOARDDB_ROOT_USER", "")
	t.Setenv("HOARDDB_ROOT_PASSWORD", "")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Storage.DataDir != "./data" {
		t.Errorf("data_dir default = %q, want ./data (a no-argument start must not need /data)", cfg.Storage.DataDir)
	}
	if cfg.Server.RootUser != "admin" {
		t.Errorf("root user default = %q, want admin", cfg.Server.RootUser)
	}
}

func TestAuthDefaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Auth.TokenExpiry != 0 {
		t.Errorf("deprecated token_expiry_hours default = %d, want 0 (unset sentinel)", cfg.Auth.TokenExpiry)
	}
	if cfg.Auth.TokenTTLSeconds != 900 {
		t.Errorf("token_ttl_seconds = %d, want 900", cfg.Auth.TokenTTLSeconds)
	}
	if cfg.Auth.TokenMaxLifetimeHours != 12 {
		t.Errorf("token_max_lifetime_hours = %d, want 12", cfg.Auth.TokenMaxLifetimeHours)
	}
	if cfg.Auth.AuthSkewSeconds != 60 {
		t.Errorf("auth_skew_seconds = %d, want 60", cfg.Auth.AuthSkewSeconds)
	}
	if cfg.Auth.AuthMaxDerivations != runtime.GOMAXPROCS(0) {
		t.Errorf("auth_max_derivations = %d, want GOMAXPROCS=%d", cfg.Auth.AuthMaxDerivations, runtime.GOMAXPROCS(0))
	}
	if cfg.Auth.AuthKeyringWindowSeconds != 1800 {
		t.Errorf("auth_keyring_window_seconds = %d, want max(2*900s, 15m) = 1800", cfg.Auth.AuthKeyringWindowSeconds)
	}
}

func TestDeprecatedTokenExpiryHonouredWhenAlone(t *testing.T) {
	cfg, err := Load(writeYAML(t, "auth:\n  token_expiry_hours: 48\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Auth.TokenMaxLifetimeHours != 48 {
		t.Fatalf("token_max_lifetime_hours = %d, want the deprecated value 48", cfg.Auth.TokenMaxLifetimeHours)
	}
}

func TestNewMaxLifetimeBeatsDeprecated(t *testing.T) {
	cfg, err := Load(writeYAML(t, "auth:\n  token_expiry_hours: 48\n  token_max_lifetime_hours: 6\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Auth.TokenMaxLifetimeHours != 6 {
		t.Fatalf("token_max_lifetime_hours = %d, want the new key's 6", cfg.Auth.TokenMaxLifetimeHours)
	}
}

func TestEnvMaxLifetimeBeatsDeprecated(t *testing.T) {
	t.Setenv("HOARDDB_TOKEN_MAX_LIFETIME", "5")
	cfg, err := Load(writeYAML(t, "auth:\n  token_expiry_hours: 48\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Auth.TokenMaxLifetimeHours != 5 {
		t.Fatalf("token_max_lifetime_hours = %d, want env 5", cfg.Auth.TokenMaxLifetimeHours)
	}
}

func TestKeyringWindowTracksExplicitTTL(t *testing.T) {
	cfg, err := Load(writeYAML(t, "auth:\n  token_ttl_seconds: 60\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Auth.AuthKeyringWindowSeconds != 900 {
		t.Fatalf("keyring window = %d, want max(2*60s, 15m) = 900", cfg.Auth.AuthKeyringWindowSeconds)
	}
}

func TestKeyringWindowZeroDisablesOverlap(t *testing.T) {
	cfg, err := Load(writeYAML(t, "auth:\n  auth_keyring_window_seconds: 0\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Auth.AuthKeyringWindowSeconds != 0 {
		t.Fatalf("keyring window = %d, want the explicit 0 (overlap disabled)", cfg.Auth.AuthKeyringWindowSeconds)
	}
}

// TestPProfEnabledEnvOverride proves that HOARDDB_PPROF_ENABLED
// reaches cfg.Debug.PProfEnabled through applyEnvOverrides with the same
// setBool semantics as every other boolean env var — any non-empty value
// overrides the YAML ("true"/"1"/"yes" is true, anything else false), and an
// empty value leaves the YAML or default in place.
func TestPProfEnabledEnvOverride(t *testing.T) {
	cases := []struct {
		name string
		env  string
		yaml string
		want bool
	}{
		{name: "env true enables pprof with no debug section", env: "true", yaml: "", want: true},
		{name: "env 1 enables pprof with no debug section", env: "1", yaml: "", want: true},
		{name: "env 0 leaves the default off", env: "0", yaml: "", want: false},
		{name: "empty env leaves the default off", env: "", yaml: "", want: false},
		{name: "empty env keeps the yaml true", env: "", yaml: "debug:\n  pprof_enabled: true\n", want: true},
		{name: "env 0 overrides the yaml true", env: "0", yaml: "debug:\n  pprof_enabled: true\n", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOARDDB_PPROF_ENABLED", tc.env)
			configPath := ""
			if tc.yaml != "" {
				configPath = writeYAML(t, tc.yaml)
			}
			cfg, err := Load(configPath)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.Debug.PProfEnabled != tc.want {
				t.Fatalf("debug.pprof_enabled = %v with HOARDDB_PPROF_ENABLED=%q, want %v", cfg.Debug.PProfEnabled, tc.env, tc.want)
			}
		})
	}
}

// TestANonIntegerEnvironmentValueIsRefused: a value that does not
// parse used to leave the setting on its default with no message.
func TestANonIntegerEnvironmentValueIsRefused(t *testing.T) {
	cases := []struct {
		variable string
		value    string
		wantType string
	}{
		{"HOARDDB_MEMTABLE_MB", "abc", "whole number"},
		{"HOARDDB_MEMTABLE_MB", "64.5", "whole number"},
		{"HOARDDB_MEMTABLE_MB", "64MB", "whole number"},
		{"HOARDDB_REPLICATION_FACTOR", "3 ", "whole number"},
		{"HOARDDB_RESTORE_DISK_HEADROOM_MULTIPLIER", "lots", "number"},
		{"HOARDDB_AUTO_REBALANCE", "maybe", "true or false"},
		{"HOARDDB_METRICS_ENABLED", "enabled", "true or false"},
	}
	for _, tc := range cases {
		t.Run(tc.variable+"="+tc.value, func(t *testing.T) {
			t.Setenv(tc.variable, tc.value)
			_, err := Load("")
			if err == nil {
				t.Fatalf("Load accepted %s=%q, want an error", tc.variable, tc.value)
			}
			for _, want := range []string{tc.variable, `"` + tc.value + `"`, tc.wantType, "unset it"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not contain %q", err.Error(), want)
				}
			}
		})
	}
}

// TestEveryBadEnvironmentValueIsReportedAtOnce keeps one start from needing
// one restart per typo.
func TestEveryBadEnvironmentValueIsReportedAtOnce(t *testing.T) {
	t.Setenv("HOARDDB_MAX_CONNS", "many")
	t.Setenv("HOARDDB_TOKEN_TTL", "1h")
	_, err := Load("")
	if err == nil {
		t.Fatal("Load accepted two bad variables")
	}
	for _, want := range []string{"HOARDDB_MAX_CONNS", "HOARDDB_TOKEN_TTL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err.Error(), want)
		}
	}
}

func TestBooleanEnvironmentSpellings(t *testing.T) {
	cases := map[string]bool{"true": true, "1": true, "yes": true, "false": false, "0": false, "no": false}
	for spelling, want := range cases {
		t.Run(spelling, func(t *testing.T) {
			t.Setenv("HOARDDB_AUTO_REBALANCE", spelling)
			cfg, err := Load("")
			if err != nil {
				t.Fatalf("Load rejected %q: %v", spelling, err)
			}
			if cfg.Server.AutoRebalance != want {
				t.Errorf("AutoRebalance = %v for %q, want %v", cfg.Server.AutoRebalance, spelling, want)
			}
		})
	}
}

// TestCustomTLSModeIsRefusedOrServesTheConfiguredCertificate:
// Custom TLS is not built: a node serves the certificate it generated,
// whatever the tls settings say. A setting that is read and then ignored is a
// protection the operator believes in and does not have, so loading refuses
// it with a message that says so and what to use instead. The environment
// and the YAML file are both checked, because both reach the fields.
func TestCustomTLSModeIsRefusedOrServesTheConfiguredCertificate(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		yaml string
		// refusalNames is the variable the refusal must name; empty means
		// the configuration must load.
		refusalNames string
	}{
		{name: "default"},
		{name: "auto from the environment", env: map[string]string{"HOARDDB_TLS_MODE": "auto"}},
		{name: "custom from the environment", env: map[string]string{"HOARDDB_TLS_MODE": "custom"}, refusalNames: "HOARDDB_TLS_MODE"},
		{name: "custom from YAML", yaml: "tls:\n  mode: custom\n", refusalNames: "HOARDDB_TLS_MODE"},
		{name: "an unknown mode", env: map[string]string{"HOARDDB_TLS_MODE": "strict"}, refusalNames: "HOARDDB_TLS_MODE"},
		{name: "a certificate with auto", env: map[string]string{"HOARDDB_TLS_CERT": "/etc/hoarddb/server.crt"}, refusalNames: "HOARDDB_TLS_CERT"},
		{name: "a key with auto", env: map[string]string{"HOARDDB_TLS_KEY": "/etc/hoarddb/server.key"}, refusalNames: "HOARDDB_TLS_KEY"},
		{name: "a CA with auto", yaml: "tls:\n  ca: /etc/hoarddb/ca.crt\n", refusalNames: "HOARDDB_TLS_CA"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, variable := range []string{"HOARDDB_TLS_MODE", "HOARDDB_TLS_CERT", "HOARDDB_TLS_KEY", "HOARDDB_TLS_CA"} {
				t.Setenv(variable, tc.env[variable])
			}
			configPath := ""
			if tc.yaml != "" {
				configPath = writeYAML(t, tc.yaml)
			}
			_, err := Load(configPath)
			if tc.refusalNames == "" {
				if err != nil {
					t.Fatalf("Load refused a configuration that serves what it says: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Load accepted a tls setting that nothing reads; the node would serve its own certificate and warn nobody")
			}
			for _, want := range []string{tc.refusalNames, "not available in this version", "HOARDDB_SEED_FINGERPRINTS"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal %q does not contain %q", err.Error(), want)
				}
			}
		})
	}
}

// TestValidateRefusesOutOfRangeArgon2Settings keeps a mistyped auth.argon2_*
// key from starting the server and failing only when the first user record is
// seeded or saved (BUG-0228). The bounds are the credential validator's
// (server/users.go:validateCredentialMaterial, server/users.go:maxArgon2Parallelism):
// memory [65536, 1048576] KiB, iterations [3, 16], parallelism [1, 16]. One
// subtest per key checks just below and just above the range, and that the
// refusal names the key and the range so the message explains the fix
// (AGENTS.md rule 3).
func TestValidateRefusesOutOfRangeArgon2Settings(t *testing.T) {
	keys := []struct {
		name string
		// below and above are one step outside the permitted range.
		below, above func(*Config)
		wantRange    string
	}{
		{
			name:      "auth.argon2_memory",
			below:     func(c *Config) { c.Auth.Argon2Memory = 65535 },
			above:     func(c *Config) { c.Auth.Argon2Memory = 1048577 },
			wantRange: "[65536, 1048576]",
		},
		{
			name:      "auth.argon2_iterations",
			below:     func(c *Config) { c.Auth.Argon2Iterations = 2 },
			above:     func(c *Config) { c.Auth.Argon2Iterations = 17 },
			wantRange: "[3, 16]",
		},
		{
			name:      "auth.argon2_parallelism",
			below:     func(c *Config) { c.Auth.Argon2Parallelism = 0 },
			above:     func(c *Config) { c.Auth.Argon2Parallelism = 17 },
			wantRange: "[1, 16]",
		},
	}
	for _, key := range keys {
		t.Run(key.name, func(t *testing.T) {
			for _, side := range []struct {
				name   string
				mutate func(*Config)
			}{{"below", key.below}, {"above", key.above}} {
				cfg := DefaultConfig()
				side.mutate(cfg)
				err := cfg.Validate()
				if err == nil {
					t.Fatalf("%s %s the range: Validate accepted it; the server would start and fail at the first user record", key.name, side.name)
				}
				for _, want := range []string{key.name, key.wantRange} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("%s %s the range: refusal %q does not contain %q", key.name, side.name, err, want)
					}
				}
			}
		})
	}
}

// TestDefaultArgon2SettingsPassValidate pins that the built-in work factor is
// inside the range Validate enforces: the two must not drift apart, or the
// zero-config start (AGENTS.md rule 2) fails before it opens its store.
func TestDefaultArgon2SettingsPassValidate(t *testing.T) {
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatalf("DefaultConfig().Validate() = %v; the built-in work factor must pass the range check", err)
	}
}

// TestValidateRefusesAnArgon2HashLengthTheClampWouldRefuse keeps a configured
// auth.argon2_hash_length the credential validator would refuse from starting
// the node and then refusing every credential it makes (BUG-0739). The clamp
// accepts only 32 (server/users.go:validateCredentialClamp against
// server/auth.go:defaultArgon2HashLen), so the refusal must name the key and
// that value, and the default and 32 must still start.
func TestValidateRefusesAnArgon2HashLengthTheClampWouldRefuse(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Auth.Argon2HashLen = 48
	err := cfg.Validate()
	if err == nil {
		t.Fatal("auth.argon2_hash_length 48: Validate accepted it; the node would start and refuse every credential it makes")
	}
	for _, want := range []string{"auth.argon2_hash_length", "32"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not contain %q", err, want)
		}
	}

	for _, length := range []int{DefaultConfig().Auth.Argon2HashLen, 32} {
		cfg := DefaultConfig()
		cfg.Auth.Argon2HashLen = length
		if err := cfg.Validate(); err != nil {
			t.Errorf("auth.argon2_hash_length %d: Validate refused it: %v", length, err)
		}
	}
}

// TestPProfSamplingKnobs: the block and mutex sampling rates default to 1 ms and 1 in 100,
// read their variables as whole numbers, and refuse text the way the other numeric
// variables do.
func TestPProfSamplingKnobs(t *testing.T) {
	t.Setenv("HOARDDB_PPROF_BLOCK_RATE_NS", "")
	t.Setenv("HOARDDB_PPROF_MUTEX_FRACTION", "")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Debug.PProfBlockRateNS != 1000000 || cfg.Debug.PProfMutexFraction != 100 {
		t.Fatalf("defaults = %d ns and 1 in %d, want 1000000 and 100", cfg.Debug.PProfBlockRateNS, cfg.Debug.PProfMutexFraction)
	}
	t.Setenv("HOARDDB_PPROF_BLOCK_RATE_NS", "0")
	t.Setenv("HOARDDB_PPROF_MUTEX_FRACTION", "5")
	cfg, err = Load("")
	if err != nil || cfg.Debug.PProfBlockRateNS != 0 || cfg.Debug.PProfMutexFraction != 5 {
		t.Fatalf("env 0 and 5 gave %+v, %v", cfg.Debug, err)
	}
	for _, name := range []string{"HOARDDB_PPROF_BLOCK_RATE_NS", "HOARDDB_PPROF_MUTEX_FRACTION"} {
		t.Setenv("HOARDDB_PPROF_BLOCK_RATE_NS", "")
		t.Setenv("HOARDDB_PPROF_MUTEX_FRACTION", "")
		t.Setenv(name, "fast")
		if _, err := Load(""); err == nil || !strings.Contains(err.Error(), name) {
			t.Fatalf("%s=fast must be refused naming the variable, got %v", name, err)
		}
	}
}
