package cli

import (
	"reflect"
	"testing"
)

// TestStatementWithAParenthesisInAnArgumentParses: parseBucketMethod
// located the method call at the last '(' of the line, so any statement with a
// '(' inside an argument (ISODate("..."), now()) was cut in the wrong place and
// refused as an unknown bucket method.
func TestStatementWithAParenthesisInAnArgumentParses(t *testing.T) {
	cases := []struct {
		name       string
		line       string
		wantBucket string
		wantMethod string
		wantArgs   []string
	}{
		{
			name:       "ISODate literal",
			line:       `db.Ev.insert({ts: ISODate("2026-01-01")})`,
			wantBucket: "Ev", wantMethod: "insert",
			wantArgs: []string{`{ts: ISODate("2026-01-01")}`},
		},
		{
			name:       "now",
			line:       `db.Ev.insert({_id_: "b", at: now()})`,
			wantBucket: "Ev", wantMethod: "insert",
			wantArgs: []string{`{_id_: "b", at: now()}`},
		},
		{
			name:       "now with a method chain and a comma",
			line:       `db.Ev.insert({at: now().add(7, "d")})`,
			wantBucket: "Ev", wantMethod: "insert",
			wantArgs: []string{`{at: now().add(7, "d")}`},
		},
		{
			name:       "new Date",
			line:       `db.Ev.insert({ts: new Date("2026-01-01T00:00:00Z")})`,
			wantBucket: "Ev", wantMethod: "insert",
			wantArgs: []string{`{ts: new Date("2026-01-01T00:00:00Z")}`},
		},
		{
			name:       "a call at the top level of an argument list keeps its comma",
			line:       `db.Ev.put("k", now().add(7, "d"))`,
			wantBucket: "Ev", wantMethod: "put",
			wantArgs: []string{`"k"`, `now().add(7, "d")`},
		},
		{
			name:       "no parenthesis in the arguments, as before",
			line:       `db.Ev.get("k")`,
			wantBucket: "Ev", wantMethod: "get",
			wantArgs: []string{`"k"`},
		},
		{
			name:       "a dotted bucket name is still one flat bucket",
			line:       `db.Post.Comments.push({at: now()})`,
			wantBucket: "Post.Comments", wantMethod: "push",
			wantArgs: []string{`{at: now()}`},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd, err := ParseCommand(tc.line)
			if err != nil {
				t.Fatalf("ParseCommand(%q): %v", tc.line, err)
			}
			if cmd.Bucket != tc.wantBucket || cmd.Method != tc.wantMethod {
				t.Errorf("bucket %q method %q, want %q %q", cmd.Bucket, cmd.Method, tc.wantBucket, tc.wantMethod)
			}
			if !reflect.DeepEqual(cmd.Args, tc.wantArgs) {
				t.Errorf("args %q, want %q", cmd.Args, tc.wantArgs)
			}
		})
	}
}

// TestStoreFieldAddressWithAParenthesisInTheKey keeps the document key of a
// store field address out of the search for the method's '(': the
// key is a quoted string and may hold any character.
func TestStoreFieldAddressWithAParenthesisInTheKey(t *testing.T) {
	cmd, err := ParseCommand(`db.Post["a(b"].Comments.push({at: now()})`)
	if err != nil {
		t.Fatalf("ParseCommand: %v", err)
	}
	if cmd.Bucket != "Post" {
		t.Errorf("bucket %q, want Post", cmd.Bucket)
	}
	if want := []string{`{at: now()}`}; !reflect.DeepEqual(cmd.Args, want) {
		t.Errorf("args %q, want %q", cmd.Args, want)
	}
}
