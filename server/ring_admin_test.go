package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// ringAdminUser creates a user with the given grants and returns a connection logged in as it.
func ringAdminUser(t *testing.T, qs *Service, name string, grants ...Grant) *tcpServerClient {
	t.Helper()
	credential, err := NewCredential("pw-for-"+name, DefaultArgon2Params())
	if err != nil {
		t.Fatal(err)
	}
	if err := qs.core.auth.CreateUser(name, credential, nil, true); err != nil {
		t.Fatal(err)
	}
	for _, grant := range grants {
		if _, err := qs.core.auth.GrantRole(name, grant); err != nil {
			t.Fatal(err)
		}
	}
	c := dialTCPServer(t, qs.tcpListener.Addr().String())
	c.loginAs(name, "pw-for-"+name)
	return c
}

func ringAdminRefusal(t *testing.T, frame *transport.Frame, code cluster.Code) wire.RingError {
	t.Helper()
	if frame.Opcode != transport.OpError {
		t.Fatalf("answered %s %q, want an error", frame.Opcode, frame.Payload)
	}
	refusal, err := wire.DecodeRingError(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if refusal.Code != code {
		t.Fatalf("code %s (%q), want %s", refusal.Code, refusal.Message, code)
	}
	return refusal
}

// TestU75TheSmokeTestCreatesItsBucketThroughTheWire: ringComposeServer makes the database
// and the bucket with RING_ADMIN and reads the ids from the reply; the first-five-minutes
// path then runs on those ids (TestRingOpcodesWorkThroughTheRunningServer), and the same
// ids come back from describe_bucket and show_buckets.
func TestU75TheSmokeTestCreatesItsBucketThroughTheWire(t *testing.T) {
	_, bucket, c := ringComposeServer(t)
	described := ringAdmin(t, c, wire.RingAdminRequest{Cmd: wire.AdminDescribeBucket, Database: ringComposeDB, Bucket: "User"})
	if described.BucketID != bucket.BucketID || described.DeclVersion != bucket.DeclVersion || described.SchemaVersionSeen != bucket.SchemaVersionSeen {
		t.Fatalf("describe_bucket %+v differs from the ids the create told", described)
	}
	listed := ringAdmin(t, c, wire.RingAdminRequest{Cmd: wire.AdminShowBuckets, Database: ringComposeDB})
	if len(listed.Buckets) != 1 || listed.Buckets[0].Name != "User" || listed.Buckets[0].BucketID != bucket.BucketID {
		t.Fatalf("show_buckets %+v", listed.Buckets)
	}
	refusal := ringAdminRefusal(t, ringAdminFrame(t, c, wire.RingAdminRequest{Cmd: wire.AdminCreateBucket, Database: ringComposeDB, Name: "User",
		Fields: []wire.RingAdminField{{Name: "Other", Type: "string"}}}), cluster.CodeBucketExists)
	if !strings.Contains(refusal.Message, "describe bucket User in app") {
		t.Errorf("BUCKET_EXISTS text: %s", refusal.Message)
	}
}

// TestU75ANoGrantsUserIsDeniedEveryAdminCommand: a user with no role gets PERMISSION_DENIED
// for each of the four commands and creates nothing; a db-manager on one database creates
// there and nowhere else; a reader reads the catalog and creates nothing.
func TestU75ANoGrantsUserIsDeniedEveryAdminCommand(t *testing.T) {
	qs, _, _ := ringComposeServer(t)
	commands := []wire.RingAdminRequest{
		{Cmd: wire.AdminCreateDatabase, Name: "fresh"},
		{Cmd: wire.AdminCreateBucket, Database: ringComposeDB, Name: "Intruder", Fields: []wire.RingAdminField{}},
		{Cmd: wire.AdminDescribeBucket, Database: ringComposeDB, Bucket: "User"},
		{Cmd: wire.AdminShowBuckets, Database: ringComposeDB},
	}
	nobody := ringAdminUser(t, qs, "nobody")
	var texts []string
	for _, req := range commands {
		refusal := ringAdminRefusal(t, ringAdminFrame(t, nobody, req), cluster.CodePermissionDenied)
		texts = append(texts, refusal.Message)
		for _, name := range []string{ringComposeDB, "User", "fresh", "Intruder"} {
			if strings.Contains(refusal.Message, name) {
				t.Errorf("%s: the denial names %q: %s", req.Cmd, name, refusal.Message)
			}
		}
	}
	for _, text := range texts[1:] {
		if text != texts[0] {
			t.Errorf("denial texts differ: %q and %q", texts[0], text)
		}
	}
	if listed := ringAdmin(t, admin2(t, qs), wire.RingAdminRequest{Cmd: wire.AdminShowBuckets, Database: ringComposeDB}); len(listed.Buckets) != 1 {
		t.Fatalf("a denied create left %+v", listed.Buckets)
	}

	manager := ringAdminUser(t, qs, "manager", Grant{Role: roleDBManager, DB: ringComposeDB})
	ringAdmin(t, manager, wire.RingAdminRequest{Cmd: wire.AdminCreateBucket, Database: ringComposeDB, Name: "Order", Fields: []wire.RingAdminField{}})
	ringAdminRefusal(t, ringAdminFrame(t, manager, commands[0]), cluster.CodePermissionDenied)

	reader := ringAdminUser(t, qs, "reader", Grant{Role: roleReader, DB: ringComposeDB})
	if listed := ringAdmin(t, reader, commands[3]); len(listed.Buckets) != 2 {
		t.Fatalf("a reader's show_buckets: %+v", listed.Buckets)
	}
	ringAdmin(t, reader, commands[2])
	ringAdminRefusal(t, ringAdminFrame(t, reader, commands[1]), cluster.CodePermissionDenied)
}

// admin2 logs a second bootstrap-root connection in.
func admin2(t *testing.T, qs *Service) *tcpServerClient {
	t.Helper()
	c := dialTCPServer(t, qs.tcpListener.Addr().String())
	c.login()
	return c
}
