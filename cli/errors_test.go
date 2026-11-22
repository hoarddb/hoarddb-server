package cli

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// dropAppFromAnotherConnection makes the session's current database vanish
// under it: a second connection drops it, and the first still believes it is
// there, so a read reaches the server's "database not found" refusal.
func dropAppFromAnotherConnection(t *testing.T, session *goldenSession) {
	t.Helper()
	other := NewCLI(WithAddress(session.addr), WithInsecureTLS(true), WithCredentials("admin", goldenPassword), WithTerminal(io.Discard))
	if err := other.Connect(); err != nil {
		t.Fatalf("second connection: %v", err)
	}
	defer other.Close()
	if err := other.handle("drop database app"); err != nil {
		t.Fatalf("dropping the database from the second connection: %v", err)
	}
}

// missingDatabaseSession returns a session whose current database "app" no
// longer exists on the server.
func missingDatabaseSession(t *testing.T) *goldenSession {
	t.Helper()
	session := newGoldenSession(t)
	if err := session.cli.handle("create database app"); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := session.cli.handle("use app"); err != nil {
		t.Fatalf("use app: %v", err)
	}
	dropAppFromAnotherConnection(t, session)
	return session
}

// TestSendReturnsTypedServerError: an OpError frame reaches the caller as a
// *ServerError that errors.As recovers, through the wrapping handlers add.
// Before Part 3 the code was flattened into a string.
func TestSendReturnsTypedServerError(t *testing.T) {
	t.Run("a real server refusal keeps its wire code and message", func(t *testing.T) {
		session := missingDatabaseSession(t)
		err := session.cli.handle("db.Users.count()")
		var serverErr *ServerError
		if !errors.As(err, &serverErr) {
			t.Fatalf("err = %v (%T), want a *ServerError reachable with errors.As", err, err)
		}
		if serverErr.Code != transport.ErrNotFound {
			t.Errorf("Code = %v, want NOT_FOUND for a missing database", serverErr.Code)
		}
		if !strings.Contains(serverErr.Message, `database "app" not found on this node`) {
			t.Errorf("Message = %q, want the server's message verbatim", serverErr.Message)
		}
		if serverErr.Raw {
			t.Error("Raw is set for a payload that decoded")
		}
	})

	t.Run("RESULT_TOO_LARGE carries both byte counts", func(t *testing.T) {
		payload, err := bson.Marshal(server.ErrorResponse{
			Code: int(transport.ErrResultTooLarge), Message: "result too large", MaxResultBytes: 1000, RetainedBytes: 2500,
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var out bytes.Buffer
		_, c := newStubServer(t, map[transport.Opcode]interface{}{
			transport.OpCount: stubRawReply{Opcode: transport.OpError, Payload: payload},
		}, &out)
		c.currentDB = "app"
		var serverErr *ServerError
		if err := c.handle("db.Users.count()"); !errors.As(err, &serverErr) {
			t.Fatalf("err = %v (%T), want a *ServerError", err, err)
		}
		if serverErr.Code != transport.ErrResultTooLarge || serverErr.MaxResultBytes != 1000 || serverErr.RetainedBytes != 2500 {
			t.Errorf("got code %v max %d retained %d, want RESULT_TOO_LARGE 1000 2500", serverErr.Code, serverErr.MaxResultBytes, serverErr.RetainedBytes)
		}
	})

	t.Run("an undecodable payload is Raw and claims no code", func(t *testing.T) {
		var out bytes.Buffer
		_, c := newStubServer(t, map[transport.Opcode]interface{}{
			transport.OpCount: stubRawReply{Opcode: transport.OpError, Payload: undecodableErrorPayload(t)},
		}, &out)
		c.currentDB = "app"
		var serverErr *ServerError
		if err := c.handle("db.Users.count()"); !errors.As(err, &serverErr) {
			t.Fatalf("err = %v (%T), want a *ServerError", err, err)
		}
		if !serverErr.Raw || serverErr.Code != 0 {
			t.Errorf("got Raw=%v Code=%v, want Raw with code 0 (stop-and-ask S3)", serverErr.Raw, serverErr.Code)
		}
	})
}

// TestServerErrorTextUnchanged guards the REPL's text: the typed error renders
// what the flat string rendered, in each of the three shapes send produced.
func TestServerErrorTextUnchanged(t *testing.T) {
	t.Run("a coded refusal", func(t *testing.T) {
		session := missingDatabaseSession(t)
		err := session.cli.handle("db.Users.count()")
		want := `count failed: server error (1): database "app" not found on this node. Run 'show databases' to list what exists, or 'create database app' to create it`
		if err == nil || err.Error() != want {
			t.Errorf("err = %v, want %q", err, want)
		}
	})

	replies := []struct {
		name    string
		payload []byte
	}{
		{"an undecodable payload", undecodableErrorPayload(t)},
		{"a decoded payload with no message", mustMarshal(t, server.ErrorResponse{Code: int(transport.ErrInternal)})},
		{"an empty payload", nil},
	}
	for _, reply := range replies {
		t.Run(reply.name+" is the bare text", func(t *testing.T) {
			var out bytes.Buffer
			_, c := newStubServer(t, map[transport.Opcode]interface{}{
				transport.OpCount: stubRawReply{Opcode: transport.OpError, Payload: reply.payload},
			}, &out)
			c.currentDB = "app"
			err := c.handle("db.Users.count()")
			if err == nil || err.Error() != "count failed: server error" {
				t.Errorf("err = %v, want %q", err, "count failed: server error")
			}
		})
	}
}

// undecodableErrorPayload is a well-formed BSON document that is not an
// ErrorResponse (the code is a string). It has to be well formed: the driver
// decodes every reply into the caller's response type before send sees the
// opcode, so bytes that are not BSON at all fail there as "unmarshal response"
// and never reach the OpError arm.
func undecodableErrorPayload(t *testing.T) []byte {
	t.Helper()
	return mustMarshal(t, bson.M{"code": "five", "message": "not a number"})
}

func mustMarshal(t *testing.T, document interface{}) []byte {
	t.Helper()
	payload, err := bson.Marshal(document)
	if err != nil {
		t.Fatalf("marshal %T: %v", document, err)
	}
	return payload
}
