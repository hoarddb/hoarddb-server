package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

func u40AlterRequest(database, bucket string) wire.RingAdminRequest {
	return wire.RingAdminRequest{Cmd: wire.AdminAlterBucket, Database: database, Bucket: bucket,
		Add: []wire.RingAdminField{{Name: "Email", Type: "string"}}, Drop: []string{}}
}

// TestU40AlterBucketIsRuledAsTheLegacyAlter: alter_bucket needs the db-admin action on the
// database, as the legacy alter bucket does. A session with no role and a reader are denied
// with the one admin denial text and change nothing; a db-manager on the database alters it,
// and on another database is denied.
func TestU40AlterBucketIsRuledAsTheLegacyAlter(t *testing.T) {
	qs, _, root := ringComposeServer(t)
	alter := u40AlterRequest(ringComposeDB, "User")

	nobody := ringAdminUser(t, qs, "nobody40")
	denied := ringAdminRefusal(t, ringAdminFrame(t, nobody, alter), cluster.CodePermissionDenied)
	reader := ringAdminUser(t, qs, "reader40", Grant{Role: roleReader, DB: ringComposeDB})
	if again := ringAdminRefusal(t, ringAdminFrame(t, reader, alter), cluster.CodePermissionDenied); again.Message != denied.Message {
		t.Errorf("denial texts differ: %q and %q", denied.Message, again.Message)
	}
	described := ringAdmin(t, root, wire.RingAdminRequest{Cmd: wire.AdminDescribeBucket, Database: ringComposeDB, Bucket: "User"})
	for _, field := range described.Fields {
		if field.Name == "Email" {
			t.Fatal("a denied alter changed the bucket")
		}
	}

	elsewhere := ringAdminUser(t, qs, "elsewhere40", Grant{Role: roleDBManager, DB: "other"})
	ringAdminRefusal(t, ringAdminFrame(t, elsewhere, alter), cluster.CodePermissionDenied)

	manager := ringAdminUser(t, qs, "manager40", Grant{Role: roleDBManager, DB: ringComposeDB})
	reply := ringAdmin(t, manager, alter)
	if reply.Cmd != wire.AdminAlterBucket || reply.DeclVersion != described.DeclVersion+1 {
		t.Fatalf("reply = %+v, want the declaration one version on", reply)
	}
	var found bool
	for _, field := range reply.Fields {
		found = found || field.Name == "Email"
	}
	if !found {
		t.Fatalf("the reply does not list the new field: %+v", reply.Fields)
	}
}

// TestU40AStaleWriteIsRefusedSchemaNotCurrentAfterAnAlter: a write whose declaration version is
// the one before an alter is refused SCHEMA_NOT_CURRENT over the wire and writes nothing; the
// same write under the header the alter replied with is accepted.
func TestU40AStaleWriteIsRefusedSchemaNotCurrentAfterAnAlter(t *testing.T) {
	_, bucket, client := ringComposeServer(t)
	altered := ringAdmin(t, client, u40AlterRequest(ringComposeDB, "User"))

	insert := func(header wire.ClientBucket, id string) *transport.Frame {
		op := ringHeader(header, cluster.OpInsert, id)
		op.IDValue = ringTyped(id)
		op.Fields = []cluster.FieldOp{{FieldID: bucket.FieldIDs["Name"], Value: ringTyped("Ada")}}
		payload, err := wire.EncodeRingKeyed(wire.RingKeyedRequest{Op: op})
		if err != nil {
			t.Fatal(err)
		}
		return client.ringSend(transport.OpRingKeyed, payload)
	}
	refusal := ringAdminRefusal(t, insert(bucket, "stale"), cluster.CodeSchemaNotCurrent)
	if !strings.Contains(refusal.Message, "app.User changed after this client read it") || strings.Contains(refusal.Message, "retrying will not help") {
		t.Errorf("SCHEMA_NOT_CURRENT text: %s", refusal.Message)
	}
	current := wire.ClientBucket{BucketID: altered.BucketID, DeclVersion: altered.DeclVersion, SchemaVersionSeen: altered.SchemaVersionSeen, FieldIDs: bucket.FieldIDs}
	if frame := insert(current, "fresh"); frame.Opcode != transport.OpRingReply {
		t.Fatalf("a write under the current header was answered %s %q", frame.Opcode, frame.Payload)
	}
	got := ringKeyed(t, client, wire.RingKeyedRequest{Read: &wire.RingRead{Class: wire.RingClassGet, BucketID: altered.BucketID,
		DeclVersion: altered.DeclVersion, SchemaVersionSeen: altered.SchemaVersionSeen, EncID: ringHeader(current, wire.RingClassGet, "stale").EncID}})
	if got.Found == nil || *got.Found {
		t.Fatal("the refused write left a document behind")
	}
}
