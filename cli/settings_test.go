package cli

import (
	"bytes"
	"strings"
	"testing"
)

// sessionWith runs statements through a session and returns what it printed.
func sessionWith(t *testing.T, statements ...string) (*CLI, string) {
	t.Helper()
	var out bytes.Buffer
	c := NewCLI(WithTerminal(&out))
	// A statement stream, not a terminal: set does not need a server, and this
	// keeps the test from paging anything.
	c.reader = strings.NewReader("")
	for _, statement := range statements {
		if err := c.handle(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	return c, out.String()
}

// TestSetChangesWhatIsPrinted is the feature: a setting can be changed while the
// session runs, and the change reaches the printer rather than only the CLI.
func TestSetChangesWhatIsPrinted(t *testing.T) {
	c, out := sessionWith(t, "set pretty on")
	if !c.pretty || !c.handler.pretty {
		t.Errorf("set pretty on left cli=%v handler=%v", c.pretty, c.handler.pretty)
	}
	if !strings.Contains(out, "pretty is now on for documents printed from now on.") {
		t.Errorf("set pretty on said %q", out)
	}

	c, out = sessionWith(t, "set pretty off")
	if c.pretty || c.handler.pretty {
		t.Errorf("set pretty off left cli=%v handler=%v", c.pretty, c.handler.pretty)
	}
	if !strings.Contains(out, "pretty is now off") {
		t.Errorf("set pretty off said %q", out)
	}

	c, _ = sessionWith(t, "set extjson on")
	if !c.extendedJSON || !c.handler.extendedJSON {
		t.Errorf("set extjson on left cli=%v handler=%v", c.extendedJSON, c.handler.extendedJSON)
	}

	c, _ = sessionWith(t, "set pager off")
	if !c.noPager {
		t.Error("set pager off left paging on")
	}
	if c, _ = sessionWith(t, "set pager on"); c.noPager {
		t.Error("set pager on left paging off")
	}
}

// TestSetAloneReportsTheSetting: asking is the same word as changing, and the
// answer names the command that would change it.
func TestSetAloneReportsTheSetting(t *testing.T) {
	_, out := sessionWith(t, "set pretty")
	if !strings.Contains(out, "pretty is off; 'set pretty on' changes it.") {
		t.Errorf("set pretty said %q", out)
	}
	_, out = sessionWith(t, "set pretty on", "set pretty")
	if !strings.Contains(out, "pretty is on; 'set pretty off' changes it.") {
		t.Errorf("set pretty after turning it on said %q", out)
	}
}

// TestSetRefusesWhatItDoesNotHave: a name that is not a setting, a value that is
// not a switch, and a third word all fail loudly, naming what exists.
func TestSetRefusesWhatItDoesNotHave(t *testing.T) {
	for _, tc := range []struct {
		statement string
		wants     []string
	}{
		{"set colour blue", []string{"unknown setting", "pretty", "extjson", "pager"}},
		{"set pretty maybe", []string{`"maybe" is not on or off`}},
		{"set pretty on extra", []string{"usage: set"}},
		{"set", []string{"usage: set <pretty|extjson|pager> [on|off]"}},
	} {
		c, out := sessionWith(t)
		err := c.handle(tc.statement)
		if err == nil {
			t.Errorf("%q was accepted", tc.statement)
			continue
		}
		for _, want := range tc.wants {
			if !strings.Contains(err.Error(), want) && !strings.Contains(out, want) {
				t.Errorf("%q failed with %q, which does not name %q", tc.statement, err, want)
			}
		}
	}
}

// TestPagerSettingSaysWhenItCannotApply: paging needs a terminal at both ends, and
// a session whose input is a pipe must be told that rather than left wondering why
// nothing changed.
func TestPagerSettingSaysWhenItCannotApply(t *testing.T) {
	c, out := sessionWith(t, "set pager on")
	if c.noPager {
		t.Fatal("set pager on turned paging off")
	}
	if !strings.Contains(out, "needs a terminal at both ends") {
		t.Errorf("a piped session that asked for paging was told %q", out)
	}
}

// TestSetIsCompletedAndInHelp: the setting is discoverable, both by Tab and by the
// help's topic list.
func TestSetIsCompletedAndInHelp(t *testing.T) {
	c := NewCLI(WithTerminal(&bytes.Buffer{}))

	for line, want := range map[string]string{
		"set pr":       "pretty",
		"set extj":     "extjson",
		"set pretty o": "on",
	} {
		_, got := c.completer()(line, len(line))
		found := false
		for _, candidate := range got {
			if candidate == want {
				found = true
			}
		}
		if !found {
			t.Errorf("completing %q offered %v, want %q among them", line, got, want)
		}
	}

	topics := strings.Join(helpTopicList(), ",")
	if !strings.Contains(topics, "set") {
		t.Errorf("the help topics are %q, without set", topics)
	}
}
