package cli

import (
	"strings"
	"testing"
)

// TestPipedStdinUnchangedWithoutFlags (T21) pins what a script piped into the
// REPL gets today: the banner, each statement's output, the stop at the first
// failing statement (later lines never run), and the error Run returns, which
// cmd/cli prints and turns into exit 1. GAP-24 adds -c and -f beside this
// path; without those flags it must stay byte-identical.
func TestPipedStdinUnchangedWithoutFlags(t *testing.T) {
	cases := []struct {
		slug   string
		script string
		// wantErr is the error Run must return; empty means it returns nil.
		wantErr string
	}{
		{
			slug: "piped-stdin-stops-at-failure",
			script: strings.Join([]string{
				"use app", "db.Users.count()", `db.Users.find("u1")`, `db.Jobs.pop()`, "db.Nope.count()", "show databases",
			}, "\n") + "\n",
			wantErr: "count failed: server error (1)",
		},
		{
			slug: "piped-stdin-completes",
			script: strings.Join([]string{
				"use app", "db.Users.count()", `db.Users.find("zz")`, "", "  show buckets  ", "exit", "show databases",
			}, "\n") + "\n",
		},
		{
			slug:   "piped-stdin-ends-without-exit",
			script: "use app\ndb.Users.count()\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.slug, func(t *testing.T) {
			session := newFixtureSession(t, WithInput(strings.NewReader(tc.script)))
			session.out.Reset()
			runErr := session.cli.Run()

			transcript := session.out.String()
			if runErr != nil {
				transcript += "returned error: " + runErr.Error() + "\n"
			} else {
				transcript += "returned error: <nil>\n"
			}
			assertGolden(t, tc.slug, session.normaliser().apply(transcript))

			switch {
			case tc.wantErr == "" && runErr != nil:
				t.Errorf("Run returned %v, want nil", runErr)
			case tc.wantErr != "" && (runErr == nil || !strings.Contains(runErr.Error(), tc.wantErr)):
				t.Errorf("Run returned %v, want an error containing %q", runErr, tc.wantErr)
			}
			if tc.wantErr != "" && strings.Contains(session.out.String(), "DATABASES") {
				t.Errorf("a statement after the failing one ran; a piped script must stop at the first failure:\n%s", session.out.String())
			}
		})
	}
}
