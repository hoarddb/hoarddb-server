package cli

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestDumpAndRestoreJSONEmptyListsAreArrays: a nil slice marshals as null, so a
// successful dump printed "errors":null and a dump that wrote nothing printed
// "databases":null, which a script iterating either list fails on.
func TestDumpAndRestoreJSONEmptyListsAreArrays(t *testing.T) {
	session := newFixtureSession(t)
	out := session.outDir

	_, dumpStdout, _ := captureProcessStreams(t, func() int {
		return RunDumpCommand(append(verbConnectionFlags(session), "--json", "--out", out+"/ok", "app"))
	})
	_, nothingDumpedStdout, _ := captureProcessStreams(t, func() int {
		return RunDumpCommand(append(verbConnectionFlags(session), "--json", "--out", out+"/none", "app", "Nope"))
	})
	_, restoreStdout, _ := captureProcessStreams(t, func() int {
		return RunRestoreCommand(append(verbConnectionFlags(session), "--json", "--dry-run", "--dir", out+"/ok"))
	})
	emptyRestore, err := json.Marshal((&restoreSummary{}).result(ExitOK))
	if err != nil {
		t.Fatalf("marshalling an empty restore result: %v", err)
	}

	cases := []struct {
		name    string
		printed string
		want    string
	}{
		{"a successful dump has no errors", dumpStdout, `"errors":[]`},
		{"a dump that wrote nothing has no databases", nothingDumpedStdout, `"databases":[]`},
		{"a successful restore has no errors", restoreStdout, `"errors":[]`},
		{"a restore that applied nothing has no databases", string(emptyRestore), `"databases":[]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if strings.Contains(tc.printed, ":null") {
				t.Errorf("a list prints as null: %s", tc.printed)
			}
			if !strings.Contains(tc.printed, tc.want) {
				t.Errorf("output lacks %s: %s", tc.want, tc.printed)
			}
		})
	}
}
