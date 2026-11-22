package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// TestIndentJSONKeepsEveryValue is the property that matters: the indented form is
// the compact document reflowed, not re-encoded. A string holding a colon and a
// comma, a nested object, a list and an empty object all come through untouched,
// and only the layout changes.
func TestIndentJSONKeepsEveryValue(t *testing.T) {
	compact := `{"a": {"b": [1, 2]}, "c": {}, "d": "x: y, z", "e": []}`
	want := "{\n" +
		`  "a": {` + "\n" +
		`    "b": [` + "\n" +
		"      1,\n" +
		"      2\n" +
		"    ]\n" +
		"  },\n" +
		`  "c": {},` + "\n" +
		`  "d": "x: y, z",` + "\n" +
		`  "e": []` + "\n" +
		"}"

	if got := indentJSON(compact); got != want {
		t.Errorf("indentJSON(%s) =\n%s\nwant:\n%s", compact, got, want)
	}
}

// TestIndentJSONSurvivesAMessageItDidNotBuild: a document the printer could not
// decode comes through as prose, and reflowing prose must not panic or mangle it.
func TestIndentJSONSurvivesAMessageItDidNotBuild(t *testing.T) {
	message := "(decode error: unknown element: 0x7b)"
	if got := indentJSON(message); got != message {
		t.Errorf("indentJSON(%q) = %q, want it unchanged", message, got)
	}
}

// TestPrettyIsOffUnlessAskedFor: the one-object-per-line form is what scripts and
// agents read, so it is the default and a flag changes it.
func TestPrettyIsOffUnlessAskedFor(t *testing.T) {
	var plain, pretty bytes.Buffer

	off := NewCommandHandler(&plain, false)
	off.printDocuments([]bson.Raw{bson.Raw([]byte{5, 0, 0, 0, 0})})
	if got := plain.String(); strings.Count(got, "\n") != 1 {
		t.Errorf("the default printer wrote %q, want one line", got)
	}

	on := NewCommandHandler(&pretty, false)
	on.SetPretty(true)
	on.printDocuments([]bson.Raw{docWithAField(t, 1), docWithAField(t, 2)})
	lines := strings.Split(strings.TrimRight(pretty.String(), "\n"), "\n")
	if len(lines) < 7 {
		t.Fatalf("two pretty documents printed %d lines, want them laid out:\n%s", len(lines), pretty.String())
	}
	if !strings.Contains(pretty.String(), "\n\n") {
		t.Errorf("pretty documents are not separated by a blank line:\n%q", pretty.String())
	}
}

// TestPrettyIsASessionOption: the flag has to reach the handler that prints, not
// just the CLI that parsed it.
func TestPrettyIsASessionOption(t *testing.T) {
	c := NewCLI(WithTerminal(&bytes.Buffer{}), WithPretty(true))
	if !c.pretty || !c.handler.pretty {
		t.Fatalf("--pretty did not reach the printer (cli=%v handler=%v)", c.pretty, c.handler.pretty)
	}
	if other := NewCLI(WithTerminal(&bytes.Buffer{})); other.handler.pretty {
		t.Fatal("a session that did not ask for pretty got it")
	}
}

// docWithAField builds a one-field BSON document for the printer tests.
func docWithAField(t *testing.T, value int32) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(bson.M{"a": value})
	if err != nil {
		t.Fatalf("building a test document: %v", err)
	}
	return raw
}
