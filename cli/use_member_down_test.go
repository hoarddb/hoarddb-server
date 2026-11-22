package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// listingRefusalOf is the OpError reply a node sends for LIST_DATABASES while
// a member is silent (BUG-0468's all-members rule): code UNAVAILABLE and the
// text server.listingIncomplete composes, verbatim.
func listingRefusalOf(t *testing.T, member string) stubRawReply {
	t.Helper()
	payload, err := bson.Marshal(server.ErrorResponse{
		Code: int(transport.ErrUnavailable),
		Message: "LIST_DATABASES asks every member of the cluster, and member " + member + " did not answer " +
			"(dial peer " + member + " at 127.0.0.1:4522: connect: connection refused), so the listing would be incomplete and was not sent. " +
			"Retry in a moment; if the member is down, run 'nodes' to see every member and its state",
	})
	if err != nil {
		t.Fatalf("encoding the refusal: %v", err)
	}
	return stubRawReply{Opcode: transport.OpError, Payload: payload}
}

// TestUseSelectsTheDatabaseWhileAMemberIsDown is BUG-0687. `use` needs one
// fact (the database exists), not the complete listing LIST_DATABASES refuses
// to give while a member is silent, so it selects the database, says which
// member could not confirm it, and the next statement answers. `show
// databases` keeps BUG-0468's refusal. The stub stands in for a surviving node
// of a three-node cluster whose third member is stopped.
func TestUseSelectsTheDatabaseWhileAMemberIsDown(t *testing.T) {
	var results, chatter bytes.Buffer
	_, c := newStubServer(t, map[transport.Opcode]interface{}{
		transport.OpListDBs: listingRefusalOf(t, "n3"),
		transport.OpCount:   server.CountResponse{Count: 7},
	}, &results)
	c.handler.SetChatter(&chatter)

	if err := c.handle("use app"); err != nil {
		t.Fatalf("use app with a member down: %v", err)
	}
	if c.currentDB != "app" {
		t.Errorf("the session is on database %q, want %q", c.currentDB, "app")
	}
	for _, want := range []string{"app", "n3", "Could not confirm"} {
		if !strings.Contains(chatter.String(), want) {
			t.Errorf("the note %q does not contain %q", chatter.String(), want)
		}
	}
	results.Reset()
	if err := c.handle("db.H.count()"); err != nil {
		t.Fatalf("count after use: %v", err)
	}
	if !strings.Contains(results.String(), "7") {
		t.Errorf("count printed %q, want the stub's 7", results.String())
	}

	err := c.handle("show databases")
	if err == nil || !strings.Contains(err.Error(), "member n3 did not answer") {
		t.Errorf("show databases with a member down returned %v, want BUG-0468's refusal naming n3", err)
	}
}

// TestUseOfAnotherServerErrorStillFails pins that only the listing refusal is
// forgiven: any other failure of the existence check (a permission refusal,
// an unrelated unavailable) still leaves the session where it was.
func TestUseOfAnotherServerErrorStillFails(t *testing.T) {
	payload, err := bson.Marshal(server.ErrorResponse{Code: int(transport.ErrUnavailable), Message: "server is shutting down"})
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	var results bytes.Buffer
	_, c := newStubServer(t, map[transport.Opcode]interface{}{
		transport.OpListDBs: stubRawReply{Opcode: transport.OpError, Payload: payload},
	}, &results)
	if err := c.handle("use app"); err == nil {
		t.Fatal("use app succeeded against an unrelated server error")
	}
	if c.currentDB != "" {
		t.Errorf("the session moved to %q", c.currentDB)
	}
}

// TestUseOfATypoWhileAMemberIsDownFailsOnTheFirstStatement is BUG-0687's
// second acceptance line. The CLI cannot tell a typo from a database the
// silent member alone holds, so `use` selects and says it could not confirm;
// the first statement that names the database then ends in the server's own
// not-found, which names the fix, instead of a silent success.
func TestUseOfATypoWhileAMemberIsDownFailsOnTheFirstStatement(t *testing.T) {
	notFound, err := bson.Marshal(server.ErrorResponse{
		Code:    int(transport.ErrNotFound),
		Message: `Database "typo" not found. Run 'show databases' to list what exists, or 'create database typo' to create it`,
	})
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	var results, chatter bytes.Buffer
	_, c := newStubServer(t, map[transport.Opcode]interface{}{
		transport.OpListDBs: listingRefusalOf(t, "n3"),
		transport.OpCount:   stubRawReply{Opcode: transport.OpError, Payload: notFound},
	}, &results)
	c.handler.SetChatter(&chatter)

	if err := c.handle("use typo"); err != nil {
		t.Fatalf("use typo with a member down: %v", err)
	}
	if !strings.Contains(chatter.String(), "Could not confirm") {
		t.Errorf("the note %q does not say the database was not confirmed", chatter.String())
	}
	err = c.handle("db.H.count()")
	if err == nil || !strings.Contains(err.Error(), "create database typo") {
		t.Errorf("the first statement returned %v, want the not-found that names the fix", err)
	}
}

// TestUseKeepsFailingOnEveryOtherUnavailableAnswer pins that the BUG-0687
// forgiveness fires on the listing refusal alone. A node that has not been
// added yet, one holding its start hold, one without quorum, and a membership
// change in flight all answer UNAVAILABLE too; selecting a database on any of
// them would hide an error that must fail.
func TestUseKeepsFailingOnEveryOtherUnavailableAnswer(t *testing.T) {
	for _, cause := range []error{server.ErrNotYetAdded, server.ErrStartHold, server.ErrNoQuorum, server.ErrEpochInProgress} {
		payload, err := bson.Marshal(server.ErrorResponse{Code: int(transport.ErrUnavailable), Message: cause.Error()})
		if err != nil {
			t.Fatalf("encoding %v: %v", cause, err)
		}
		var results bytes.Buffer
		_, c := newStubServer(t, map[transport.Opcode]interface{}{
			transport.OpListDBs: stubRawReply{Opcode: transport.OpError, Payload: payload},
		}, &results)
		if err := c.handle("use app"); err == nil {
			t.Errorf("use app succeeded against %q", cause)
		}
		if c.currentDB != "" {
			t.Errorf("the session moved to %q on %q", c.currentDB, cause)
		}
	}
}
