package cli

import (
	"errors"
	"strings"
	"testing"
)

// TestTwoStatementsOnOneLineAreRefusedWithTheFix: `use t; show
// buckets` was read as the database name "t;" and answered "database not
// found". The line is refused at parse time, so nothing runs, and the message
// names the rule and the second statement.
func TestTwoStatementsOnOneLineAreRefusedWithTheFix(t *testing.T) {
	tests := []struct {
		name       string
		line       string
		wantSecond string
	}{
		{"use then show", "use t; show buckets", "show buckets"},
		{"no space after semicolon", "use t;show buckets", "show buckets"},
		{"use then create", "use t; create bucket Orders { type: hash }", "create bucket Orders { type: hash }"},
		{"trailing semicolon on the second", "use t; show buckets;", "show buckets"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseCommand(tc.line)
			if err == nil {
				t.Fatalf("ParseCommand(%q) accepted two statements on one line", tc.line)
			}
			for _, fragment := range []string{"one statement per line", tc.wantSecond} {
				if !strings.Contains(err.Error(), fragment) {
					t.Errorf("error %q does not name %q", err, fragment)
				}
			}

			var out strings.Builder
			session := NewCLI(WithTerminal(&out))
			handleErr := session.handle(tc.line)
			var usage *UsageError
			if !errors.As(handleErr, &usage) || !usage.Parse {
				t.Errorf("handle(%q) = %v, want a parse UsageError so nothing runs", tc.line, handleErr)
			}
		})
	}
}

// TestSemicolonsThatAreNotASecondStatementStillParse pins the lines that must
// keep working: a closing semicolon, and a semicolon inside a quoted string or
// a document body.
func TestSemicolonsThatAreNotASecondStatementStillParse(t *testing.T) {
	for _, line := range []string{
		"use t;",
		"use t;  ",
		"show buckets;",
		`db.Orders.insert({note: "a; b"});`,
		`db.Orders.insert({note: 'a; b'})`,
	} {
		if _, err := ParseCommand(line); err != nil {
			t.Errorf("ParseCommand(%q) = %v, want it accepted", line, err)
		}
	}
}
