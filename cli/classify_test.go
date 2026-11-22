package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// wireCodeExpectations is written from the gap24-one-shot-spec record §6.1
// and §6.2, not derived from the code under test: the name and class of each of
// the eighteen non-zero wire codes (FIELD_TYPE from typed-fields spec §6.1, BATCH_TOO_LARGE from BUG-0467 G11, DOCUMENT_EXISTS from the document-placement build doc BD-1).
var wireCodeExpectations = []struct {
	code  transport.ErrorCode
	name  string
	class string
	exit  int
}{
	{0x01, "NOT_FOUND", "refused", ExitRefused},
	{0x02, "ALREADY_EXISTS", "refused", ExitRefused},
	{0x03, "PERMISSION_DENIED", "refused", ExitRefused},
	{0x04, "INVALID_ARGUMENT", "refused", ExitRefused},
	{0x05, "INTERNAL", "refused", ExitRefused},
	{0x06, "UNAVAILABLE", "unavailable", ExitUnavailable},
	{0x07, "TIMEOUT", "unavailable", ExitUnavailable},
	{0x08, "CONFLICT", "refused", ExitRefused},
	{0x09, "QUOTA_EXCEEDED", "refused", ExitRefused},
	{0x0A, "UNSUPPORTED_VERSION", "refused", ExitRefused},
	{0x0B, "EARLY_DATA_REJECTED", "unavailable", ExitUnavailable},
	{0x0C, "NOT_AUTHENTICATED", "refused", ExitRefused},
	{0x0D, "OVERLOADED", "unavailable", ExitUnavailable},
	{0x0E, "RESULT_TOO_LARGE", "refused", ExitRefused},
	{0x0F, "DUPLICATE_KEY", "refused", ExitRefused},
	{0x10, "FIELD_TYPE", "refused", ExitRefused},
	{0x11, "BATCH_TOO_LARGE", "refused", ExitRefused},
	{0x12, "DOCUMENT_EXISTS", "refused", ExitRefused},
}

// TestErrorCodeMappingIsTotal (T8): every wire code and auth reason the
// transport defines has a CLI name and class, and anything past them renders
// in the CLI's own format. The codes are built from integers, not imported as
// a list, so a code added to transport/errors.go without a CLI row fails here.
func TestErrorCodeMappingIsTotal(t *testing.T) {
	seenNames := map[string]transport.ErrorCode{}
	for _, want := range wireCodeExpectations {
		if got := codeName(want.code); got != want.name {
			t.Errorf("codeName(%#x) = %q, want %q", uint8(want.code), got, want.name)
		}
		if other, dup := seenNames[want.name]; dup {
			t.Errorf("codes %#x and %#x share the name %q", uint8(other), uint8(want.code), want.name)
		}
		seenNames[want.name] = want.code

		info := classify(&ServerError{Code: want.code, Message: "m"})
		if info.Class != want.class || info.Exit != want.exit || info.Code != want.name {
			t.Errorf("classify(%s) = class %q code %q exit %d, want %q %q %d", want.name, info.Class, info.Code, info.Exit, want.class, want.name, want.exit)
		}
		if info.WireCode == nil || *info.WireCode != int(want.code) {
			t.Errorf("classify(%s).WireCode = %v, want %d", want.name, info.WireCode, want.code)
		}
	}

	// The transport's own String() is not the CLI's format for an unknown
	// value; pin both so the day one changes, the CLI's format is still right.
	for value := 0x13; value <= 0x40; value++ {
		code := transport.ErrorCode(value)
		if !strings.HasPrefix(code.String(), "UNKNOWN(") {
			t.Fatalf("transport now names error code %#x (%s): add it to wireCodeTable and to wireCodeExpectations", value, code)
		}
		if got, want := codeName(code), fmt.Sprintf("UNKNOWN_%d", value); got != want {
			t.Errorf("codeName(%#x) = %q, want %q (no parentheses)", value, got, want)
		}
	}
	if got := transport.ErrorCode(0x13).String(); got != "UNKNOWN(19)" {
		t.Errorf("transport.ErrorCode(0x13).String() = %q; the CLI assumes the parenthesised form", got)
	}
	if got := codeName(transport.ErrOK); got != "UNKNOWN_0" {
		t.Errorf("codeName(0) = %q, want UNKNOWN_0: an error frame never carries code 0 on purpose", got)
	}

	wantAuth := map[transport.AuthFailReason]string{
		1: "AUTH_CREDENTIALS", 2: "AUTH_TOKEN_EXPIRED", 3: "AUTH_TOKEN_INVALID", 4: "AUTH_TOKEN_UNSUPPORTED",
		5: "AUTH_PROTOCOL_MISMATCH", 6: "AUTH_RATE_LIMITED", 7: "AUTH_ALREADY_AUTHED", 8: "AUTH_INTERNAL",
	}
	for reason := transport.AuthFailReason(1); reason <= 8; reason++ {
		if got := authCodeName(reason); got != wantAuth[reason] {
			t.Errorf("authCodeName(%d) = %q, want %q", reason, got, wantAuth[reason])
		}
		info := classify(&AuthError{Reason: reason})
		if info.Class != "auth" || info.Exit != ExitAuth || info.Code != wantAuth[reason] {
			t.Errorf("classify(auth %d) = %q %q %d, want auth %q %d", reason, info.Class, info.Code, info.Exit, wantAuth[reason], ExitAuth)
		}
	}
	for value := 9; value <= 0x20; value++ {
		reason := transport.AuthFailReason(value)
		if !strings.HasPrefix(reason.String(), "UNKNOWN(") {
			t.Fatalf("transport now names auth reason %d (%s): add it to authCodeName and to wantAuth", value, reason)
		}
		if got, want := authCodeName(reason), fmt.Sprintf("AUTH_UNKNOWN_%d", value); got != want {
			t.Errorf("authCodeName(%d) = %q, want %q", value, got, want)
		}
	}
	if got := authCodeName(0); got != "AUTH_UNKNOWN_0" {
		t.Errorf("authCodeName(0) = %q, want AUTH_UNKNOWN_0", got)
	}

	tooLarge := classify(&ServerError{Code: transport.ErrResultTooLarge, Message: "m", MaxResultBytes: 1000, RetainedBytes: 2500})
	if tooLarge.MaxResultBytes == nil || *tooLarge.MaxResultBytes != 1000 || tooLarge.RetainedBytes == nil || *tooLarge.RetainedBytes != 2500 {
		t.Errorf("RESULT_TOO_LARGE lost its byte counts: %+v", tooLarge)
	}
	plain := classify(&ServerError{Code: transport.ErrInternal, Message: "m"})
	if plain.MaxResultBytes != nil || plain.RetainedBytes != nil {
		t.Errorf("a code that carries no byte counts reported some: %+v", plain)
	}
}

// TestClassifyTable pins the class, code and exit of every kind of failure
// that is not a plain wire code (spec §6.1 and §6.2).
func TestClassifyTable(t *testing.T) {
	overloaded := &ServerError{Code: transport.ErrOverloaded, Message: "server at its connection limit"}
	rows := []struct {
		name  string
		err   error
		class string
		code  string
		exit  int
	}{
		{"usage", &UsageError{Message: "usage: use <database>"}, "usage", "USAGE", ExitUsage},
		{"json unsupported", &JSONUnsupportedError{Verb: "dump"}, "usage", "JSON_UNSUPPORTED", ExitUsage},
		{"container absent", &NotFoundError{Kind: "bucket", Message: "bucket not found"}, "refused", "NOT_FOUND", ExitRefused},
		{"in-band refusal", &RefusedError{Message: "not applied"}, "refused", "REFUSED", ExitRefused},
		{"dial failure", &ConnectError{Phase: "dial", Err: errors.New("connection refused")}, "connect", "CONNECT_FAILED", ExitConnect},
		{"tls configuration", &ConnectError{Phase: "tls", Err: errors.New("bad ca file")}, "connect", "CONNECT_FAILED", ExitConnect},
		{"untrusted certificate", &ConnectError{Phase: "trust", Err: errors.New("not trusted")}, "connect", "TLS_UNTRUSTED", ExitConnect},
		{"pin mismatch", &ConnectError{Phase: "pin", Err: errors.New("fingerprint differs")}, "connect", "TLS_UNTRUSTED", ExitConnect},
		{"overload during AUTH is connect, not auth", &ConnectError{Phase: "auth-handshake", Err: overloaded}, "connect", "OVERLOADED", ExitConnect},
		{"overload mid-session is unavailable", overloaded, "unavailable", "OVERLOADED", ExitUnavailable},
		{"auth handshake with nothing to name", &ConnectError{Phase: "auth-handshake", Err: errors.New("reset")}, "connect", "CONNECT_FAILED", ExitConnect},
		{"not connected", ErrNotConnected, "connect", "CONNECT_FAILED", ExitConnect},
		{"interrupted", &InterruptedError{After: 1, Total: 3}, "interrupted", "INTERRUPTED", ExitInterrupted},
		{"reply lost, EOF", io.EOF, "unavailable", "CONNECTION_LOST", ExitUnavailable},
		{"reply lost, unexpected EOF", io.ErrUnexpectedEOF, "unavailable", "CONNECTION_LOST", ExitUnavailable},
		{"reply lost, network error", &net.OpError{Op: "read", Err: errors.New("connection reset by peer")}, "unavailable", "CONNECTION_LOST", ExitUnavailable},
		{"an error nobody typed", errors.New("something else"), "refused", "UNCLASSIFIED", ExitRefused},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			for _, err := range []error{row.err, fmt.Errorf("find failed: %w", row.err)} {
				info := classify(err)
				if info.Class != row.class || info.Code != row.code || info.Exit != row.exit {
					t.Errorf("classify(%v) = class %q code %q exit %d, want %q %q %d", err, info.Class, info.Code, info.Exit, row.class, row.code, row.exit)
				}
			}
		})
	}
}

// TestClassifyNeverReadsTheMessage: classification is by type and wire code
// only. The same message under two codes classifies as the two codes, which is
// also how the missing-database, permission and duplicate rows get more
// specific without a CLI change: the day
// the server answers NOT_FOUND or PERMISSION_DENIED for them, they classify as
// those codes and still exit 2.
func TestClassifyNeverReadsTheMessage(t *testing.T) {
	const missingDatabase = `database "app" not found on this node. Run 'show databases' to list what exists`
	const notAllowed = "permission denied: this operation needs the admin role. Grant it with 'grant role admin@* to bob'"
	rows := []struct {
		code    transport.ErrorCode
		message string
		want    string
	}{
		{transport.ErrInternal, missingDatabase, "INTERNAL"},
		{transport.ErrNotFound, missingDatabase, "NOT_FOUND"},
		{transport.ErrInternal, notAllowed, "INTERNAL"},
		{transport.ErrPermissionDenied, notAllowed, "PERMISSION_DENIED"},
		{transport.ErrAlreadyExists, `database "app" already exists`, "ALREADY_EXISTS"},
	}
	for _, row := range rows {
		info := classify(&ServerError{Code: row.code, Message: row.message})
		if info.Code != row.want || info.Class != "refused" || info.Exit != ExitRefused {
			t.Errorf("classify(%s, %.30q) = %q %q %d, want refused %s 2", row.code, row.message, info.Class, info.Code, info.Exit, row.want)
		}
		if info.Message != row.message {
			t.Errorf("Message = %q, want the server's message verbatim", info.Message)
		}
	}
}

// TestUnknownWireCodeIsRefusedNotSuccess: a code a newer server sends and this
// CLI has never heard of is an error with the documented fallback, never a
// success, and never one of the retryable classes.
func TestUnknownWireCodeIsRefusedNotSuccess(t *testing.T) {
	for _, value := range []transport.ErrorCode{0x13, 0x7F, 0xFF} {
		info := classify(&ServerError{Code: value, Message: "ownership epoch is stale"})
		want := fmt.Sprintf("UNKNOWN_%d", uint8(value))
		if info.Class != "refused" || info.Code != want || info.Exit != ExitRefused {
			t.Errorf("classify(unknown %#x) = %q %q %d, want refused %s 2", uint8(value), info.Class, info.Code, info.Exit, want)
		}
		if info.WireCode == nil || *info.WireCode != int(value) {
			t.Errorf("WireCode = %v, want %d so a script can still branch on the number", info.WireCode, value)
		}
		if info.Exit == ExitOK {
			t.Errorf("unknown code %#x classified as success", uint8(value))
		}
	}

	t.Run("end to end, through the wire", func(t *testing.T) {
		payload := mustMarshal(t, map[string]interface{}{"code": 200, "message": "written by a newer server"})
		var out bytes.Buffer
		_, c := newStubServer(t, map[transport.Opcode]interface{}{
			transport.OpCount: stubRawReply{Opcode: transport.OpError, Payload: payload},
		}, &out)
		c.currentDB = "app"
		info := classify(c.handle("db.Users.count()"))
		if info.Code != "UNKNOWN_200" || info.Class != "refused" || info.Exit != ExitRefused {
			t.Errorf("classify = %q %q %d, want refused UNKNOWN_200 2", info.Class, info.Code, info.Exit)
		}
	})

	t.Run("a frame that carried no code at all", func(t *testing.T) {
		info := classify(&ServerError{Raw: true})
		if info.Code != "UNKNOWN_0" || info.Class != "refused" || info.WireCode != nil {
			t.Errorf("classify(raw) = %q %q wire %v, want refused UNKNOWN_0 with no wire code", info.Class, info.Code, info.WireCode)
		}
	})
}

// Two of the three server refusals that arrive with the wrong or no code
// from a single node, against a real server; the third, an in-band refusal, needs the
// Part 4 sweep and is TestClassifyCreateExistingDatabase there. Each classifies
// around the server: exit 2, with the code the server actually sent.
func TestClassifyMissingDatabaseRead(t *testing.T) {
	session := missingDatabaseSession(t)
	info := classify(session.cli.handle("db.Users.count()"))
	if info.Class != "refused" || info.Code != "NOT_FOUND" || info.Exit != ExitRefused {
		t.Errorf("classify = %q %q %d, want refused NOT_FOUND 2", info.Class, info.Code, info.Exit)
	}
	if !strings.Contains(info.Message, `database "app" not found on this node`) {
		t.Errorf("Message = %q, want the server's text", info.Message)
	}
}

func TestClassifyAuthorizationRefusal(t *testing.T) {
	session := newGoldenSession(t)
	for _, statement := range []string{"create database app", "create user bob roles [reader@app]"} {
		if err := session.cli.handle(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	bob := NewCLI(WithAddress(session.addr), WithInsecureTLS(true), WithCredentials("bob", goldenUserPassword), WithTerminal(io.Discard))
	if err := bob.Connect(); err != nil {
		t.Fatalf("connecting as bob: %v", err)
	}
	defer bob.Close()

	info := classify(bob.handle("remove-node n2"))
	if info.Class != "refused" || info.Exit != ExitRefused {
		t.Fatalf("classify = %q %q %d, want a refusal with exit 2", info.Class, info.Code, info.Exit)
	}
	if info.Code != "PERMISSION_DENIED" {
		t.Errorf("Code = %q, want PERMISSION_DENIED", info.Code)
	}
	if !strings.Contains(info.Message, "permission denied") {
		t.Errorf("Message = %q, want the server's permission text", info.Message)
	}
}

// The third case: create database on an existing name answers
// ALREADY_EXISTS as an error frame.
func TestClassifyCreateExistingDatabase(t *testing.T) {
	session := newGoldenSession(t)
	if err := session.cli.handle("create database app"); err != nil {
		t.Fatalf("first create: %v", err)
	}
	info := classify(session.cli.handle("create database app"))
	if info.Class != "refused" || info.Code != "ALREADY_EXISTS" || info.Exit != ExitRefused {
		t.Errorf("classify = %q %q %d, want refused ALREADY_EXISTS 2", info.Class, info.Code, info.Exit)
	}
	if !strings.Contains(info.Message, "already exists") {
		t.Errorf("Message = %q, want the server's message", info.Message)
	}
}
