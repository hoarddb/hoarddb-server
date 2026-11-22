package cli

import (
	"strings"
	"testing"
)

// TestIngestShowsTheServersRefusalOfAControlCharacterName: an --into name that holds a control
// character is refused by the name rule the ring client and the server share
// (cluster/wire/ringadmin.go, checked while the request is encoded, so nothing is sent for
// that name). The refusal is shown with the character written out and nothing a terminal acts
// on reaches the screen. The test does not claim nothing was created: ingest may create the
// database before it meets a bad bucket name.
func TestIngestShowsTheServersRefusalOfAControlCharacterName(t *testing.T) {
	addr, _ := newRingTestHost(t)
	input := writeTextFile(t, "users.jsonl", "{\"_id\":\"a\"}\n")
	for _, tc := range []struct{ label, into string }{
		{"bucket", "app.us\x1b[2Jers"},
		{"database", "ap\x1b[2Jp.users"},
	} {
		t.Run(tc.label, func(t *testing.T) {
			code, stdout, stderr := ingestOnHost(t, addr, input, "--into", tc.into)
			shown := stdout + stderr
			if code == 0 {
				t.Fatalf("a name with a control character was accepted:\n%s", shown)
			}
			if !strings.Contains(shown, "control character") || !strings.Contains(shown, `\x1b`) {
				t.Errorf("the output does not show the refusal with the character written out:\n%s", shown)
			}
			if character, found := drivesTerminal(strings.ReplaceAll(shown, "\n", " ")); found {
				t.Errorf("the output holds %U, a character a terminal acts on:\n%q", character, shown)
			}
		})
	}
}
