package termio

import (
	"reflect"
	"testing"
)

// TestAddHistory covers the two rules that make Up useful: no empty entries, and
// no immediate repeat — pressing Up twice must reach the previous *command*.
func TestAddHistory(t *testing.T) {
	e := NewLineEditor(nil, nil)

	e.AddHistory("first")
	e.AddHistory("first")
	e.AddHistory("   ")
	e.AddHistory("second")

	want := []string{"first", "second"}
	if got := e.History(); !reflect.DeepEqual(got, want) {
		t.Fatalf("history = %v, want %v", got, want)
	}
}

// TestHistoryIsBounded asserts a long session cannot grow without limit.
func TestHistoryIsBounded(t *testing.T) {
	e := NewLineEditor(nil, nil)
	for i := 0; i < maxHistory+50; i++ {
		e.AddHistory(string(rune('a' + i%26)))
	}
	if got := len(e.History()); got > maxHistory {
		t.Fatalf("history length = %d, want at most %d", got, maxHistory)
	}
}

// TestApplyCompletion is the whole Tab decision, tested without a terminal.
func TestApplyCompletion(t *testing.T) {
	vocabulary := func(candidates ...string) Completer {
		return func(string, int) (int, []string) { return 0, candidates }
	}

	cases := []struct {
		name        string
		line        string
		pos         int
		completer   Completer
		wantLine    string
		wantPos     int
		wantDisplay []string
	}{
		{
			name: "a single candidate replaces the word",
			line: "fir", pos: 3, completer: vocabulary("first"),
			wantLine: "first", wantPos: 5,
		},
		{
			name: "several candidates extend to their common prefix",
			line: "f", pos: 1, completer: vocabulary("find", "first"),
			wantLine: "fi", wantPos: 2,
		},
		{
			name: "candidates that share nothing new are listed",
			line: "f", pos: 1, completer: vocabulary("find", "get"),
			wantLine: "f", wantPos: 1, wantDisplay: []string{"find", "get"},
		},
		{
			name: "no candidates leaves the line alone",
			line: "xyz", pos: 3, completer: vocabulary(),
			wantLine: "xyz", wantPos: 3,
		},
		{
			name: "a nil completer leaves the line alone",
			line: "abc", pos: 3, completer: nil,
			wantLine: "abc", wantPos: 3,
		},
		{
			name: "the word is replaced mid-line, not the whole line",
			line: "show buck", pos: 9,
			completer: func(string, int) (int, []string) { return 5, []string{"buckets"} },
			wantLine:  "show buckets", wantPos: 12,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			line, pos, display := applyCompletion([]rune(tc.line), tc.pos, tc.completer)
			if string(line) != tc.wantLine || pos != tc.wantPos {
				t.Fatalf("applyCompletion = %q pos %d, want %q pos %d", string(line), pos, tc.wantLine, tc.wantPos)
			}
			if !reflect.DeepEqual(display, tc.wantDisplay) {
				t.Fatalf("display = %v, want %v", display, tc.wantDisplay)
			}
		})
	}
}

// TestCommonPrefix covers the ambiguous case, including the UTF-8 guard: a
// partial rune must never be inserted into the line.
func TestCommonPrefix(t *testing.T) {
	cases := []struct {
		in   []string
		want string
	}{
		{[]string{"find", "first"}, "fi"},
		{[]string{"find"}, "find"},
		{[]string{"find", "get"}, ""},
		{nil, ""},
		{[]string{"héllo", "héxx"}, "hé"},
		{[]string{"é", "è"}, ""},
	}
	for _, tc := range cases {
		if got := commonPrefix(tc.in); got != tc.want {
			t.Fatalf("commonPrefix(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestTrimTrailingNewline asserts the plain path's normalisation, which is what
// keeps a piped line from carrying a stray carriage return into the parser.
func TestTrimTrailingNewline(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"status\n", "status"},
		{"status\r\n", "status"},
		{"status", "status"},
		{"", ""},
	}
	for _, tc := range cases {
		if got := trimTrailingNewline(tc.in); got != tc.want {
			t.Fatalf("trimTrailingNewline(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
