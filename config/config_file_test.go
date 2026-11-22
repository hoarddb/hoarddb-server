package config

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/hoarddb/hoarddb-server/internal/repofile"
)

func writeConfigFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing config file: %v", err)
	}
	return path
}

// TestLoadRefusesUnknownKeys guards that a key the Config struct does
// not define used to be dropped without a word, so a typo or a documented but
// unbuilt setting looked applied. Each case must fail, naming the key, its
// line and the way out.
func TestLoadRefusesUnknownKeys(t *testing.T) {
	cases := []struct {
		name     string
		yaml     string
		wantAll  []string
		wantNone []string
	}{
		{
			name:    "unbuilt top-level section",
			yaml:    "server:\n  node_id: a\n\nbucket_limits:\n  l2_hard_limit: 100000\n",
			wantAll: []string{`"bucket_limits"`, "line 4", "top level", "server"},
		},
		{
			name:    "misspelled key suggests the near one",
			yaml:    "storage:\n  data_dr: /tmp/x\n",
			wantAll: []string{`"data_dr"`, "line 2", "storage", `did you mean "data_dir"`},
		},
		{
			name:     "no near key lists the valid ones instead of guessing",
			yaml:     "logging:\n  max_size_mb: 100\n",
			wantAll:  []string{`"max_size_mb"`, "line 2", "logging", "level", "format", "output"},
			wantNone: []string{"did you mean"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfigFile(t, tc.yaml))
			if err == nil {
				t.Fatal("Load accepted a file with an unknown key")
			}
			for _, want := range tc.wantAll {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not contain %q", err, want)
				}
			}
			for _, unwanted := range tc.wantNone {
				if strings.Contains(err.Error(), unwanted) {
					t.Errorf("error %q should not contain %q", err, unwanted)
				}
			}
		})
	}
}

// TestLoadMissingFileNamesThePathAndTheFix covers the unreadable-file half of
// the config-file rule: the message must say which path and how to proceed.
func TestLoadMissingFileNamesThePathAndTheFix(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.yaml")
	_, err := Load(missing)
	if err == nil {
		t.Fatal("Load accepted a config path that does not exist")
	}
	for _, want := range []string{missing, "--config", "HOARDDB_CONFIG"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

// TestLoadFileLayersBelowEnvironment pins the precedence: defaults, then the
// file, then HOARDDB_* variables.
func TestLoadFileLayersBelowEnvironment(t *testing.T) {
	path := writeConfigFile(t, "storage:\n  data_dir: /from/file\n  memtable_mb: 64\nserver:\n  node_id: file-node\n")
	t.Setenv("HOARDDB_DATA_DIR", "/from/env")
	t.Setenv("HOARDDB_NODE_ID", "")
	t.Setenv("HOARDDB_MEMTABLE_MB", "")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Storage.DataDir != "/from/env" {
		t.Errorf("data_dir = %q, want the environment value /from/env", cfg.Storage.DataDir)
	}
	if cfg.Server.NodeID != "file-node" {
		t.Errorf("node_id = %q, want the file value", cfg.Server.NodeID)
	}
	if cfg.Server.MaxConns != DefaultConfig().Server.MaxConns {
		t.Errorf("max_conns = %d, want the built-in default %d", cfg.Server.MaxConns, DefaultConfig().Server.MaxConns)
	}
}

// TestLoadReadsKeyCommandFromTheFile: key_command has no environment form, so
// the file is the only way to set it.
func TestLoadReadsKeyCommandFromTheFile(t *testing.T) {
	path := writeConfigFile(t, "encryption:\n  keystore: command\n  mode: on\n  key_command: [\"/usr/bin/printf\", \"%s\", \"abc\"]\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := []string{"/usr/bin/printf", "%s", "abc"}
	if strings.Join(cfg.Encryption.KeyCommand, "|") != strings.Join(want, "|") {
		t.Errorf("key_command = %q, want %q", cfg.Encryption.KeyCommand, want)
	}
}

var yamlFence = regexp.MustCompile("(?s)```yaml\n(.*?)```")

// TestDocumentedYAMLParsesStrictly is the drift guard for the config file:
// every fenced yaml block on the configuration page that describes the server
// config (the prompt-theme example after it is a different schema) must decode
// into Config with unknown keys refused, exactly as the server reads it.
func TestDocumentedYAMLParsesStrictly(t *testing.T) {
	text := string(repofile.Read(t, "docs/user/configuration.md"))
	if cut := strings.Index(text, "## Prompt Themes"); cut >= 0 {
		text = text[:cut]
	}
	blocks := yamlFence.FindAllStringSubmatch(text, -1)
	if len(blocks) < 3 {
		t.Fatalf("expected the page's yaml examples (memtable, config file, encryption), found %d", len(blocks))
	}
	sawFullExample := false
	for index, block := range blocks {
		decoder := yaml.NewDecoder(bytes.NewReader([]byte(block[1])))
		decoder.KnownFields(true)
		if err := decoder.Decode(&Config{}); err != nil {
			t.Errorf("yaml block %d on docs/user/configuration.md does not match the Config struct: %v\n%s", index+1, err, block[1])
		}
		if strings.Contains(block[1], "/etc/hoarddb/config.yaml") {
			sawFullExample = true
		}
	}
	if !sawFullExample {
		t.Error("the page no longer carries the complete example file (# /etc/hoarddb/config.yaml)")
	}
}

// TestUserDocsDoNotDocumentTheUnbuiltBucketLimits is a docs
// substitute test: nothing implements l2_soft_limit/l2_hard_limit.
func TestUserDocsDoNotDocumentTheUnbuiltBucketLimits(t *testing.T) {
	// website/content/docs is generated from the user docs by the site build and is
	// not tracked, so a stale copy left on disk is not a documentation defect.
	userDocs, err := repofile.Find("docs/user")
	if err != nil {
		t.Fatalf("locating the user documentation: %v", err)
	}
	generated := filepath.Join("..", "..", "website", "content", "docs")
	for _, root := range []string{userDocs, filepath.Join("..", "..", "website", "content")} {
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() && path == generated {
				return filepath.SkipDir
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".md") {
				return nil
			}
			body, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			for _, name := range []string{"l2_soft_limit", "l2_hard_limit", "bucket_limits"} {
				if strings.Contains(string(body), name) {
					t.Errorf("%s documents %q, which nothing implements", path, name)
				}
			}
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			t.Fatalf("walking %s: %v", root, err)
		}
	}
}

// captureWarnings routes slog's default logger into a buffer for one test and
// restores it afterwards.
func captureWarnings(t *testing.T) *bytes.Buffer {
	t.Helper()
	var warnings bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&warnings, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &warnings
}

// TestEnvOverrideOfFileValueWarns guards BUG-0317: the image sets
// HOARDDB_METRICS_LISTEN and friends, which beat a mounted config file, and
// nothing said so. A file value that an environment variable replaces with a
// different value must be named on stderr, with the way out.
func TestEnvOverrideOfFileValueWarns(t *testing.T) {
	cases := []struct {
		name     string
		yaml     string
		env      map[string]string
		wantAll  []string
		wantNone bool
	}{
		{
			name:    "bind narrowed in the file is widened by the image",
			yaml:    "metrics:\n  listen: 127.0.0.1:9090\n",
			env:     map[string]string{"HOARDDB_METRICS_LISTEN": "0.0.0.0:9090"},
			wantAll: []string{"metrics.listen", "HOARDDB_METRICS_LISTEN", "127.0.0.1:9090", "0.0.0.0:9090", "Unset HOARDDB_METRICS_LISTEN"},
		},
		{
			name:    "data directory in the file loses to the image",
			yaml:    "storage:\n  data_dir: /mnt/volume/hoard\n",
			env:     map[string]string{"HOARDDB_DATA_DIR": "/data"},
			wantAll: []string{"storage.data_dir", "HOARDDB_DATA_DIR", "/mnt/volume/hoard", "/data"},
		},
		{
			name:     "same value in both places says nothing",
			yaml:     "metrics:\n  listen: 0.0.0.0:9090\n",
			env:      map[string]string{"HOARDDB_METRICS_LISTEN": "0.0.0.0:9090"},
			wantNone: true,
		},
		{
			name:     "no value in the file says nothing",
			yaml:     "server:\n  node_id: a\n",
			env:      map[string]string{"HOARDDB_METRICS_LISTEN": "0.0.0.0:9090"},
			wantNone: true,
		},
		{
			name:     "no variable says nothing",
			yaml:     "metrics:\n  listen: 127.0.0.1:9090\n",
			wantNone: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, variable := range []string{"HOARDDB_METRICS_LISTEN", "HOARDDB_DATA_DIR"} {
				t.Setenv(variable, "")
			}
			for variable, value := range tc.env {
				t.Setenv(variable, value)
			}
			warnings := captureWarnings(t)
			if _, err := Load(writeConfigFile(t, tc.yaml)); err != nil {
				t.Fatalf("Load: %v", err)
			}
			logged := warnings.String()
			if tc.wantNone {
				if strings.Contains(logged, "overrides") {
					t.Fatalf("expected no override warning, got:\n%s", logged)
				}
				return
			}
			for _, want := range tc.wantAll {
				if !strings.Contains(logged, want) {
					t.Errorf("warning is missing %q, got:\n%s", want, logged)
				}
			}
		})
	}
}

// TestLoadMarksOnlyTheHostnameDefaultAsDefaulted: the server matches a seed
// entry to itself by node id only when the id was configured, because cloned
// hosts share a hostname (BUG-0598). Load records which one it is.
func TestLoadMarksOnlyTheHostnameDefaultAsDefaulted(t *testing.T) {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		t.Skipf("no hostname to default the node id to: %v", err)
	}
	for _, tc := range []struct {
		name, file, env, wantID string
		wantDefaulted           bool
	}{
		{name: "unset", wantID: hostname, wantDefaulted: true},
		{name: "environment", env: hostname, wantID: hostname},
		{name: "file", file: "server:\n  node_id: " + hostname + "\n", wantID: hostname},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := ""
			if tc.file != "" {
				path = writeConfigFile(t, tc.file)
			}
			t.Setenv("HOARDDB_NODE_ID", tc.env)
			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.Server.NodeID != tc.wantID || cfg.Server.NodeIDDefaulted != tc.wantDefaulted {
				t.Errorf("node id %q, defaulted %v; want %q, defaulted %v",
					cfg.Server.NodeID, cfg.Server.NodeIDDefaulted, tc.wantID, tc.wantDefaulted)
			}
		})
	}
}
