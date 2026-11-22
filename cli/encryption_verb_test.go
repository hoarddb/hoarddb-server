package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/server"
)

// sealedEncryptionReport is what a sealed node sends.
func sealedEncryptionReport() *server.HealthEncryption {
	return &server.HealthEncryption{
		Mode: "require", Keystore: "keyfile", Sealed: true,
		KEKKid: "1a2b3c4d", NodeMasterKid: "5e6f7a8b",
		NotSealed: []string{"membership.json: peer addresses", "dumps: written in plaintext whatever the mode"},
	}
}

// TestEncryptionVerbPrintsEachState: sealed, plaintext, a node configured to
// seal whose files do not show it, and a server too old to say. Every form
// says what to do next (AGENTS.md rule 3) and none prints a key.
func TestEncryptionVerbPrintsEachState(t *testing.T) {
	for _, tc := range []struct {
		name   string
		report *server.HealthEncryption
		want   []string
		absent []string
	}{
		{"sealed", sealedEncryptionReport(),
			[]string{"ENCRYPTION AT REST", "Sealed:       yes", "Mode:         require", "Keystore:     keyfile",
				"KEK kid:      1a2b3c4d", "Node master:  5e6f7a8b", "membership.json: peer addresses", "dumps: written in plaintext"},
			[]string{"NOT SEALED"}},
		{"plaintext", &server.HealthEncryption{Mode: "off", Keystore: "off", NotSealed: []string{"dumps: written in plaintext whatever the mode"}},
			[]string{"Sealed:       no", "Mode:         off", "fresh data directory", "hoarddb dump"},
			[]string{"KEK kid", "NOT SEALED"}},
		{"configured but not confirmed", &server.HealthEncryption{Mode: "require", Keystore: "keyfile", Problem: "table 7 has KeyID == 0"},
			[]string{"Sealed:       NO", "configured to be sealed", "table 7 has KeyID == 0", "Do not rely on"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var terminal bytes.Buffer
			h := NewCommandHandler(&terminal, false)
			h.printEncryption(tc.report)
			for _, want := range tc.want {
				if !strings.Contains(terminal.String(), want) {
					t.Errorf("encryption output lacks %q:\n%s", want, terminal.String())
				}
			}
			for _, absent := range tc.absent {
				if strings.Contains(terminal.String(), absent) {
					t.Errorf("encryption output contains %q:\n%s", absent, terminal.String())
				}
			}
		})
	}
}

// TestPrintEncryptionProblemNamesAlteredBeforeStart: the Problem branch also
// covers a store whose files were altered before this start, so its sentence
// must name that case instead of asserting the store changed after it opened.
// BUG-0322.
func TestPrintEncryptionProblemNamesAlteredBeforeStart(t *testing.T) {
	var terminal bytes.Buffer
	h := NewCommandHandler(&terminal, false)
	h.printEncryption(&server.HealthEncryption{Mode: "require", Keystore: "keyfile", Problem: "table 7 has KeyID == 0"})
	output := terminal.String()
	if !strings.Contains(output, "its files were altered before this start") {
		t.Errorf("the encryption problem does not name a store altered before start:\n%s", output)
	}
	if strings.Contains(output, "changed after it opened or the check could not read it") {
		t.Errorf("the encryption problem still claims the store changed after it opened:\n%s", output)
	}
}

// TestEncryptionVerbOlderServer: a server that sends no report is not called
// plaintext; the verb says it cannot tell.
func TestEncryptionVerbOlderServer(t *testing.T) {
	var terminal bytes.Buffer
	h := NewCommandHandler(&terminal, false)
	h.printEncryption(nil)
	if !strings.Contains(terminal.String(), "does not report") || strings.Contains(terminal.String(), "Sealed:") {
		t.Errorf("an older server's encryption output = %q, want a statement that it does not report", terminal.String())
	}
}

// TestEncryptionVerbParses: the word is a global command.
func TestEncryptionVerbParses(t *testing.T) {
	cmd, err := ParseCommand("encryption")
	if err != nil || cmd.Type != "global" || cmd.Method != "encryption" {
		t.Errorf("ParseCommand(encryption) = %+v, %v, want a global encryption command", cmd, err)
	}
}

// sealedTestEdit seals the test server under a key file outside its data
// directory (the keystore refuses one inside it).
func sealedTestEdit(t *testing.T) func(*config.Config) {
	t.Helper()
	keyPath := filepath.Join(t.TempDir(), "kek")
	if err := os.WriteFile(keyPath, bytes.Repeat([]byte{0xA7}, 32), 0o600); err != nil {
		t.Fatalf("writing the key file: %v", err)
	}
	return func(cfg *config.Config) {
		cfg.Encryption.Keystore = "keyfile"
		cfg.Encryption.KeyFile = keyPath
		cfg.Encryption.Mode = "require"
	}
}

// TestEncryptionVerbAndBucketViewsOverTheWire: one sealed and one plaintext
// live server, through the real wire. The verb, describe bucket, stats() and
// show buckets all say what the store's structure says.
func TestEncryptionVerbAndBucketViewsOverTheWire(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*testing.T) func(*config.Config)
		// sealed is the encryption verb's own line; mode is the value the three
		// bucket views report, matched with whatever padding their block gives
		// the label.
		sealed string
		mode   string
	}{
		{"sealed", sealedTestEdit, "Sealed:       yes", "yes"},
		{"plaintext", func(*testing.T) func(*config.Config) { return nil }, "Sealed:       no", "no"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newDumpTestServerWith(t, tc.edit(t))
			var terminal bytes.Buffer
			c.handler.terminal = &terminal
			c.handler.currentDB = func() string { return "app" }
			for _, statement := range []string{
				"create database app", "create bucket Users { type: hash }",
				"encryption", "describe bucket Users", "show buckets", "db.Users.stats()",
			} {
				cmd, err := ParseCommand(statement)
				if err != nil {
					t.Fatalf("parsing %q: %v", statement, err)
				}
				if err := c.handler.Handle(cmd); err != nil {
					t.Fatalf("%s: %v", statement, err)
				}
			}
			output := terminal.String()
			if !strings.Contains(output, tc.sealed) {
				t.Errorf("the encryption verb lacks %q:\n%s", tc.sealed, output)
			}
			// The three bucket views report the mode: describe bucket, show
			// buckets and stats(). Each pads the label to its block's own
			// column, so match the label and the value, not the spacing.
			view := regexp.MustCompile(`Encrypted:\s+` + tc.mode + `\b`)
			if got := len(view.FindAllString(output, -1)); got != 3 {
				t.Errorf("the bucket views report %q %d times, want 3 (describe bucket, show buckets, stats):\n%s", tc.mode, got, output)
			}
		})
	}
}
