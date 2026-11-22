package server

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestU42DropBucketIsRuledAsTheLegacyDrop: drop_bucket needs the db-admin action on the
// database, as the legacy drop bucket does. A session with no role and a reader are denied
// with the one admin denial text and the bucket is still there; a db-manager of another
// database is denied; a db-manager of this one drops it, and a second drop is NOT_FOUND.
func TestU42DropBucketIsRuledAsTheLegacyDrop(t *testing.T) {
	qs, _, root := ringComposeServer(t)
	drop := wire.RingAdminRequest{Cmd: wire.AdminDropBucket, Database: ringComposeDB, Bucket: "User"}

	nobody := ringAdminUser(t, qs, "nobody42")
	denied := ringAdminRefusal(t, ringAdminFrame(t, nobody, drop), cluster.CodePermissionDenied)
	reader := ringAdminUser(t, qs, "reader42", Grant{Role: roleReader, DB: ringComposeDB})
	if again := ringAdminRefusal(t, ringAdminFrame(t, reader, drop), cluster.CodePermissionDenied); again.Message != denied.Message {
		t.Errorf("denial texts differ: %q and %q", denied.Message, again.Message)
	}
	elsewhere := ringAdminUser(t, qs, "elsewhere42", Grant{Role: roleDBManager, DB: "other"})
	ringAdminRefusal(t, ringAdminFrame(t, elsewhere, drop), cluster.CodePermissionDenied)
	describe := wire.RingAdminRequest{Cmd: wire.AdminDescribeBucket, Database: ringComposeDB, Bucket: "User"}
	if described := ringAdmin(t, root, describe); described.State != "active" {
		t.Fatalf("a denied drop changed the bucket: %+v", described)
	}

	manager := ringAdminUser(t, qs, "manager42", Grant{Role: roleDBManager, DB: ringComposeDB})
	if reply := ringAdmin(t, manager, drop); reply.Cmd != wire.AdminDropBucket || reply.State != "dropping" || reply.BucketID == 0 {
		t.Fatalf("reply = %+v", reply)
	}
	// The documents go in the background; once they have, the bucket is gone and a drop is NOT_FOUND.
	for range 400 {
		if frame := ringAdminFrame(t, manager, drop); frame.Opcode == transport.OpError {
			ringAdminRefusal(t, frame, cluster.CodeNotFound)
			ringAdminRefusal(t, ringAdminFrame(t, root, describe), cluster.CodeNotFound)
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("the dropped bucket never finished dropping")
}
