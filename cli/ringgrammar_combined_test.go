package cli

import (
	"slices"
	"strings"
	"testing"
)

// TestAggregateAndIndexShareOneBody parses a store field with a with (...) clause and an index entry
// in the same create bucket, in either order, and in alter bucket, and prints each back to text
// that parses to the same statement. A writer that drops the clause or the entry fails.
func TestAggregateAndIndexShareOneBody(t *testing.T) {
	cases := []struct {
		line string
		want string
	}{
		{"create bucket P { Author string, Ts datetime, L fifoStore with (sum Total), index ByA (Author, Ts desc) }",
			"create bucket P { Author string, Ts datetime, L fifoStore with (sum Total), index ByA (Author, Ts desc) }"},
		{"create bucket P { index ByA (Author), L hashStore with (sum Total, count by Cat) }",
			"create bucket P { L hashStore with (sum Total, count by Cat), index ByA (Author) }"},
		{"create bucket P { L hashStore with (sum Total), index ByA (Author), A string }",
			"create bucket P { L hashStore with (sum Total), A string, index ByA (Author) }"},
		{"create bucket P { index ByA (Author) }", "create bucket P { index ByA (Author) }"},
		{"alter bucket P { add index ByA (Author, Ts desc), drop index Old, add X string, drop Y }",
			"alter bucket P { add X string, drop Y, add index ByA (Author, Ts desc), drop index Old }"},
		{"alter bucket P { drop index ByA }", "alter bucket P { drop index ByA }"},
	}
	for _, c := range cases {
		first, err := Parse(c.line, "app")
		if err != nil {
			t.Errorf("%s: %v", c.line, err)
			continue
		}
		if got := first.String(); got != c.want {
			t.Errorf("%s printed as %q, want %q", c.line, got, c.want)
			continue
		}
		second, err := Parse(first.String(), "app")
		if err != nil {
			t.Errorf("%q does not parse back: %v", first.String(), err)
			continue
		}
		if second.String() != first.String() {
			t.Errorf("%q reprints as %q", first.String(), second.String())
		}
	}
}

// TestAggregateIndexAfterFieldHandlesEveryEntryKind walks the comma-or-brace check after an index
// entry (which has no field to look at), then after a field and after a store field with a clause.
func TestAggregateIndexAfterFieldHandlesEveryEntryKind(t *testing.T) {
	for _, line := range []string{
		"create bucket P { index ByA (A), B string }",
		"create bucket P { index ByA (A) }",
		"create bucket P { index ByA (A), index ByB (B desc) }",
	} {
		if _, err := Parse(line, "app"); err != nil {
			t.Errorf("%s: %v", line, err)
		}
	}
	for line, want := range map[string]string{
		"create bucket P { index ByA (A) with (sum Total) }":                          "not after an index entry",
		"create bucket P { L fifoStore with (sum Total) index ByA (A) }":              `expected "," or "}"`,
		"create bucket P { index ByA (A) index ByB (B) }":                             `expected "," or "}"`,
		"alter bucket P { add index ByA (A) with (sum Total) }":                       `expected "," or "}"`,
		"create bucket P { A string with (sum Total), index ByA (A) }":                "a scalar (string)",
		"create bucket P { L fifoStore with (sum Total), index ByA (A, B, C, D, E) }": "at most 4",
	} {
		_, err := Parse(line, "app")
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: error %v, want text %q", line, err, want)
		}
	}
}

// TestCompletionAfterAggregateAndIndex checks the tokens offered at each point of a body that mixes
// the with clause and index entries, in create bucket and alter bucket.
func TestCompletionAfterAggregateAndIndex(t *testing.T) {
	cases := []struct {
		prefix string
		want   []string
	}{
		{"create bucket P { L fifoStore ", []string{"with", ","}},
		{"create bucket P { L fifoStore with ", []string{"("}},
		{"create bucket P { L fifoStore with (", []string{"sum", "count"}},
		{"create bucket P { L fifoStore with (sum Total), ", []string{"index"}},
		{"create bucket P { Author string ", []string{",", "}"}},
		{"create bucket P { index ByA (Author ", []string{")", "desc", ","}},
		{"create bucket P { index ByA (Author) ", []string{",", "}"}},
		{"create bucket P { index ByA (Author), ", []string{"index"}},
		{"alter bucket P { ", []string{"add", "drop"}},
		{"alter bucket P { add ", []string{"index"}},
		{"alter bucket P { drop ", []string{"index"}},
		{"alter bucket P { add index ByA (Author) ", []string{",", "}"}},
	}
	for _, c := range cases {
		offered := CompleteWith(nil, c.prefix, "app")
		for _, word := range c.want {
			if !slices.Contains(offered, word) {
				t.Errorf("after %q completion offers %v, want %q among them", c.prefix, offered, word)
			}
		}
	}
	if offered := CompleteWith(nil, "create bucket P { Author string ", "app"); slices.Contains(offered, "with") {
		t.Errorf("a scalar field is offered 'with': %v", offered)
	}
	if offered := CompleteWith(nil, "create bucket P { L fifoStore with (sum Total) ", "app"); slices.Contains(offered, "with") {
		t.Errorf("a second 'with' is offered: %v", offered)
	}
}
