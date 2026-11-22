package cli

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"unicode"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// hostileText holds a clipboard-write sequence, a screen clear, a C1 control
// and a DEL around ordinary letters. Any of them reaching a terminal as bytes
// is a failure.
const hostileText = "a\x1b]52;c;ZXZpbA==\x07b\x1b[2Jc\u009b31md\x7fe\rf"

// drivesTerminal reports the first character in text that a terminal would act
// on, other than the newline and tab a layout uses, and false when there is none.
func drivesTerminal(text string) (rune, bool) {
	for _, character := range text {
		if character == '\n' || character == '\t' {
			continue
		}
		if unicode.IsControl(character) {
			return character, true
		}
	}
	return 0, false
}

// TestServerSuppliedTextIsEscapedBeforePrinting runs statements against a
// server that answers with control characters in every name and message the
// statement prints, and checks nothing a terminal would act on is written.
func TestServerSuppliedTextIsEscapedBeforePrinting(t *testing.T) {
	// The colour-only text passes the printf funnel, which keeps the theme's own
	// colour sequences, so it fails unless the call site escapes the name itself.
	for label, text := range map[string]string{"control characters": hostileText, "colour only": colourOnlyText} {
		t.Run(label, func(t *testing.T) { serverSuppliedTextIsEscaped(t, text) })
	}
}

// colourOnlyText is a name that is nothing but a colour sequence.
const colourOnlyText = "a\x1b[31mred\x1b[8mhidden\x1b[0mb"

func serverSuppliedTextIsEscaped(t *testing.T, hostileText string) {
	t.Helper()
	cases := []struct {
		name      string
		statement string
		script    map[transport.Opcode]interface{}
	}{
		{"show databases", "show databases", map[transport.Opcode]interface{}{
			transport.OpListDBs: server.ListDatabasesResponse{Databases: []server.DatabaseInfo{{Name: hostileText, BucketCount: 1}}},
		}},
		{"show buckets", "show buckets in app", map[transport.Opcode]interface{}{
			transport.OpListBuckets: server.ListBucketsResponse{Found: true, Buckets: []server.BucketInfo{{Name: hostileText, Type: server.BucketTypeHash}}},
		}},
		{"describe database", "describe database app", map[transport.Opcode]interface{}{
			transport.OpListDBs:     server.ListDatabasesResponse{Databases: []server.DatabaseInfo{{Name: "app", BucketCount: 1}}},
			transport.OpListBuckets: server.ListBucketsResponse{Found: true, Buckets: []server.BucketInfo{{Name: hostileText, Type: server.BucketTypeHash}}},
		}},
		{"status", "status", map[transport.Opcode]interface{}{
			transport.OpHealthCheck: server.HealthResponse{NodeID: hostileText, Version: hostileText, Uptime: hostileText, Reasons: []string{hostileText}},
		}},
		{"nodes", "nodes", map[transport.Opcode]interface{}{
			transport.OpListNodes: server.NodeListResponse{Nodes: []*server.NodeInfo{{NodeID: hostileText, Addr: hostileText, Status: hostileText, Duplicate: hostileText}}},
		}},
		{"show users", "show users", map[transport.Opcode]interface{}{
			transport.OpUserList: server.UserListResponse{Master: hostileText, MasterAddr: hostileText, Users: []server.UserInfo{{Username: hostileText, Enabled: true}}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var terminal bytes.Buffer
			script := bannerScript()
			for opcode, response := range tc.script {
				script[opcode] = response
			}
			_, c := newStubServer(t, script, &terminal)
			terminal.Reset()
			if err := c.handle(tc.statement); err != nil {
				t.Fatalf("%s: %v", tc.statement, err)
			}
			if character, found := drivesTerminal(terminal.String()); found {
				t.Errorf("%s wrote %U, a character a terminal acts on:\n%q", tc.statement, character, terminal.String())
			}
			if !strings.Contains(terminal.String(), `\x1b`) {
				t.Errorf("%s output shows the escape character as text nowhere, want it visible but inert:\n%q", tc.statement, terminal.String())
			}
		})
	}
}

// TestErrorTextIsEscaped covers the messages that reach the REPL's "Error:"
// line and the one-shot error line: a server's refusal text and a failure that
// carries a name.
func TestErrorTextIsEscaped(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"server error", &ServerError{Code: transport.ErrNotFound, Message: hostileText}},
		{"refusal", &RefusedError{Message: hostileText}},
		{"not found", &NotFoundError{Kind: "bucket", Message: hostileText}},
		{"connect", &ConnectError{Phase: "tls", Err: errors.New(hostileText)}},
		// The print sites escape a second time, so only a direct test pins these.
		{"auth failure", &AuthError{Reason: transport.AuthFailCredentials, Message: hostileText}},
		{"auth rate limited", &AuthError{Reason: transport.AuthFailRateLimited, Message: hostileText}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if character, found := drivesTerminal(tc.err.Error()); found {
				t.Errorf("Error() holds %U, a character a terminal acts on: %q", character, tc.err.Error())
			}
		})
	}
}

// TestEscapingKeepsPlainTextAndLayout pins that ordinary text, accents, and a
// multi-line message come through unchanged.
func TestEscapingKeepsPlainTextAndLayout(t *testing.T) {
	for _, text := range []string{"users", "café-ünï", "first line\nsecond line\twith a tab", "bad \xff byte"} {
		if got := terminalSafeText(text); got != text {
			t.Errorf("terminalSafeText(%q) = %q, want it unchanged", text, got)
		}
	}
	if got := terminalSafe("users"); got != "users" {
		t.Errorf("terminalSafe(users) = %q, want it unchanged", got)
	}
}

// TestNeutralizeControlsKeepsOnlyTheThemesColour pins the printf funnel: the
// theme's colour sequences pass, and any other escape does not.
func TestNeutralizeControlsKeepsOnlyTheThemesColour(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"colour", "\x1b[38;5;196mred\x1b[0m", "\x1b[38;5;196mred\x1b[0m"},
		{"clipboard write", "x\x1b]52;c;QQ==\x07y", `x\x1b]52;c;QQ==\x07y`},
		{"screen clear", "\x1b[2J", `\x1b[2J`},
		{"unterminated colour", "\x1b[38;5", `\x1b[38;5`},
		{"cursor move named like colour", "\x1b[1;1H", `\x1b[1;1H`},
		{"carriage return", "a\rb", `a\x0db`},
		{"bidi override", "a\u202eb", `a\u202eb`},
	}
	for _, tc := range cases {
		if got := neutralizeControls(tc.in); got != tc.want {
			t.Errorf("%s: neutralizeControls(%q) = %q, want %q", tc.name, tc.in, got, tc.want)
		}
	}
}

// TestInertWriterReportsTheCallersLength keeps io.Writer's contract: a short
// count would make fmt.Fprintf report an error for every escaped line.
func TestInertWriterReportsTheCallersLength(t *testing.T) {
	var sink bytes.Buffer
	text := "name \x1b[2J\n"
	written, err := inertWriter{&sink}.Write([]byte(text))
	if err != nil || written != len(text) {
		t.Fatalf("Write = %d, %v, want %d, nil", written, err, len(text))
	}
	if got := sink.String(); got != "name \\x1b[2J\n" {
		t.Errorf("output = %q", got)
	}
}

// TestDocumentRenderingEscapesKeysAndValues covers the document path, which
// shows stored keys and strings written by anyone with write access.
func TestDocumentRenderingEscapesKeysAndValues(t *testing.T) {
	handler := &CommandHandler{}
	rendered := handler.doc(map[string]interface{}{hostileText: hostileText})
	if character, found := drivesTerminal(rendered); found {
		t.Errorf("document rendering holds %U: %q", character, rendered)
	}
}

// TestDumpProgressEscapesNames covers the dump and restore progress and
// summary lines, which name databases and buckets taken from a listing or a
// dump directory.
func TestDumpProgressEscapesNames(t *testing.T) {
	var out bytes.Buffer
	summary := newDumpSummary()
	summary.incomplete = true
	summary.failures = append(summary.failures, dumpFailure{Database: hostileText, Bucket: hostileText, Error: hostileText})
	printDumpSummary(&out, summary)
	if character, found := drivesTerminal(out.String()); found {
		t.Errorf("dump summary holds %U: %q", character, out.String())
	}
}

// TestPrintfAndNotefNeutraliseControls pins the funnel itself: the four
// methods every statement's output goes through escape what they are given,
// so a call site that forgot to escape a name is still caught.
func TestPrintfAndNotefNeutraliseControls(t *testing.T) {
	var terminal, chatter bytes.Buffer
	handler := &CommandHandler{terminal: &terminal, chatter: &chatter}
	handler.printf("out %s\n", "x\x1b]52;c;QQ==\x07y")
	handler.notef("note %s\n", "x\x1b[2Jy")
	session := &CLI{out: &sessionOutput{terminal: &terminal}, chatter: &chatter}
	session.printf("out %s\n", "x\x1b[2Jy")
	session.notef("note %s\n", "x\x1b[2Jy")
	for _, written := range []string{terminal.String(), chatter.String()} {
		if character, found := drivesTerminal(written); found {
			t.Errorf("output holds %U: %q", character, written)
		}
	}
	// The theme's colour still goes through.
	terminal.Reset()
	handler.printf("%s\n", "\x1b[38;5;196mred\x1b[0m")
	if terminal.String() != "\x1b[38;5;196mred\x1b[0m\n" {
		t.Errorf("colour was altered: %q", terminal.String())
	}
}
