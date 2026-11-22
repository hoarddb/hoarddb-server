package cli

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestUpsertVerbSendsUpsert: db.User.upsert({...}) sends UPSERT with that
// document, and an upsert with no document is a usage line with an example
// rather than a write of an empty document.
func TestUpsertVerbSendsUpsert(t *testing.T) {
	var results bytes.Buffer
	stub, c := newStubServer(t, map[transport.Opcode]interface{}{
		transport.OpUpsert: server.InsertResponse{ID: "a", Success: true},
	}, &results)
	c.currentDB = "app"
	results.Reset()

	if err := c.handle(`db.User.upsert({_id_: "a", v: 1})`); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	payloads := stub.payloadsOf(transport.OpUpsert)
	if len(payloads) != 1 {
		t.Fatalf("the server received %d UPSERT frames, want 1 (sequence %v)", len(payloads), stub.sequence())
	}
	var sent server.InsertRequest
	if err := bson.Unmarshal(payloads[0], &sent); err != nil {
		t.Fatalf("decoding the UPSERT payload: %v", err)
	}
	if sent.Database != "app" || sent.Bucket != "User" || sent.Data["_id_"] != "a" {
		t.Errorf("UPSERT payload = %+v, want database app, bucket User, document _id_ a", sent)
	}
	if got := results.String(); got != "OK  id=a\n" {
		t.Errorf("the results stream carries %q, want the OK line", got)
	}

	err := c.handle(`db.User.upsert()`)
	var usage *UsageError
	if !errors.As(err, &usage) {
		t.Fatalf("upsert() with no document = %v, want a *UsageError", err)
	}
	if !strings.Contains(usage.Message, `db.User.upsert({_id_: "`) {
		t.Errorf("the usage line %q has no runnable example", usage.Message)
	}
	if got := stub.sent(transport.OpUpsert); got != 1 {
		t.Errorf("upsert() with no document sent a frame: %d UPSERT frames in all, want 1", got)
	}
}
