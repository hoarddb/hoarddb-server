package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestCreateConfirmationNamesPlacement is build step 12a's CLI test: the
// create line of a hash bucket names the placement the server recorded, a
// bucket of another type keeps today's line, describe bucket shows the
// placement, and a placement the server refuses is printed as the refusal.
func TestCreateConfirmationNamesPlacement(t *testing.T) {
	c := newDumpTestServer(t)
	var terminal bytes.Buffer
	c.handler.terminal = &terminal
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}

	run := func(statement string) (string, error) {
		t.Helper()
		cmd, err := ParseCommand(statement)
		if err != nil {
			t.Fatalf("ParseCommand(%q): %v", statement, err)
		}
		terminal.Reset()
		err = c.handler.handleCreateBucket(cmd)
		return terminal.String(), err
	}

	for _, tc := range []struct {
		statement string
		want      string
	}{
		{"create bucket User in app", "OK  created bucket app.User (type hash, whole)\n"},
		{"create bucket Cache in app { type: hash, placement: whole }", "OK  created bucket app.Cache (type hash, whole)\n"},
		{"create bucket Wide in app { type: hash, placement: spread }", "OK  created bucket app.Wide (type hash, spread)\n"},
		{"create bucket Jobs in app { type: fifo }", "OK  created bucket app.Jobs (type fifo)\n"},
	} {
		printed, err := run(tc.statement)
		if err != nil {
			t.Fatalf("%s: %v", tc.statement, err)
		}
		if printed != tc.want {
			t.Errorf("%s printed %q, want %q", tc.statement, printed, tc.want)
		}
	}

	if _, err := run("create bucket Odd in app { placement: everywhere }"); err == nil || !strings.Contains(err.Error(), "spread") || !strings.Contains(err.Error(), "whole") {
		t.Errorf("placement everywhere gave %v, want a refusal naming spread and whole", err)
	}
	if _, err := run("create bucket Late in app { type: fifo, placement: spread }"); err == nil || !strings.Contains(err.Error(), "only hash buckets spread") {
		t.Errorf("placement spread on a fifo bucket gave %v, want the server's refusal", err)
	}

	for name, want := range map[string]string{"User": "whole", "Wide": "spread", "Cache": "whole"} {
		cmd, err := ParseCommand("describe bucket " + name + " in app")
		if err != nil {
			t.Fatalf("ParseCommand: %v", err)
		}
		terminal.Reset()
		if err := c.handler.handleDescribeBucket(cmd); err != nil {
			t.Fatalf("describe bucket %s: %v", name, err)
		}
		if !strings.Contains(terminal.String(), "Placement:") || !strings.Contains(terminal.String(), want) {
			t.Errorf("describe bucket %s printed %q, want a Placement row saying %s", name, terminal.String(), want)
		}
	}
	cmd, _ := ParseCommand("describe bucket Jobs in app")
	terminal.Reset()
	if err := c.handler.handleDescribeBucket(cmd); err != nil {
		t.Fatalf("describe bucket Jobs: %v", err)
	}
	if strings.Contains(terminal.String(), "Placement") {
		t.Errorf("describe bucket of a fifo bucket printed a Placement row: %q", terminal.String())
	}
}
