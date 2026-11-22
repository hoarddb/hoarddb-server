package server

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The factor alter's tests (bucket-replication-rev8-spec §14.1, §14.2; build
// doc bucket-replication-g1g2-build.md §5). Most send the alter through the
// cluster's handler with no session, as a member's forwarded frame reaches it;
// TestAlterBucketRequiresDBAdmin sends it as a client's session does.

// TestAlterBucketRequestWireNames pins every key of the factor alter's request
// (spec §3.2): each field set, so a missing tag shows as a lowercased Go name,
// and no `type` key, because a store-type conversion is an opcode of its own.
func TestAlterBucketRequestWireNames(t *testing.T) {
	got := encodedKeys(t, AlterBucketRequest{Database: "app", Name: "Sessions", ReplicationFactor: 5, PreviousFactor: 3, Epoch: 12, Generation: 4})
	want := []string{"database", "name", "replication_factor", "previous_factor", "epoch", "generation"}
	if !slices.Equal(got, want) {
		t.Fatalf("AlterBucketRequest encodes as %v, want %v", got, want)
	}
	encoded, err := bson.Marshal(AlterBucketRequest{Database: "app", Name: "Sessions", ReplicationFactor: 5, PreviousFactor: 3})
	if err != nil {
		t.Fatalf("marshalling AlterBucketRequest: %v", err)
	}
	if factor, ok := bson.Raw(encoded).Lookup("replication_factor").Int32OK(); !ok || factor != 5 {
		t.Fatalf("replication_factor on the wire = %v, want int32 5", bson.Raw(encoded).Lookup("replication_factor"))
	}
}

// TestAlterBucketResponseWireNames pins the answer's keys (spec §4.2): the
// pending and releasing lists are omitted when empty.
func TestAlterBucketResponseWireNames(t *testing.T) {
	full := encodedKeys(t, AlterBucketResponse{Success: true, Message: "m", ReplicationFactor: 5, PreviousFactor: 3,
		Pending: []string{"node-d"}, Releasing: []string{"node-c"}})
	if want := []string{"success", "message", "replication_factor", "previous_factor", "pending", "releasing"}; !slices.Equal(full, want) {
		t.Fatalf("AlterBucketResponse encodes as %v, want %v", full, want)
	}
	bare := encodedKeys(t, AlterBucketResponse{Success: true, Message: "m", ReplicationFactor: 3, PreviousFactor: 3})
	if want := []string{"success", "message", "replication_factor", "previous_factor"}; !slices.Equal(bare, want) {
		t.Fatalf("AlterBucketResponse with nothing pending or releasing encodes as %v, want %v", bare, want)
	}
}

// TestAlterBucketIsPlacedByItsBucketName is T-169's placement row: the alter
// names its bucket under "name", as the other bucket DDL does, and a frame
// read under "bucket" would place it nowhere — routed as database DDL, or run
// on whichever node the client reached.
func TestAlterBucketIsPlacedByItsBucketName(t *testing.T) {
	payload, err := bson.Marshal(AlterBucketRequest{Database: "app", Name: "Sessions", ReplicationFactor: 3})
	if err != nil {
		t.Fatalf("marshalling the alter: %v", err)
	}
	bucket, database := decodeFrameMeta(payload).scope(transport.OpAlterBucket)
	if bucket != "Sessions" || database != "app" {
		t.Fatalf("the alter is scoped to bucket %q in database %q, want Sessions in app", bucket, database)
	}
}

// walOpCount counts the entries of op for bucket in a node's write-ahead log.
func walOpCount(t *testing.T, wal *storage.WAL, op transport.Opcode, bucket string) int {
	t.Helper()
	entries, err := wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading the WAL: %v", err)
	}
	count := 0
	for _, entry := range entries {
		if transport.Opcode(entry.Op) == op && entry.Bucket == bucket {
			count++
		}
	}
	return count
}

// alterAsSession sends an alter to node index through the handler a client's
// connection reaches, with session on the context: the authorization gate runs
// first, as it does for a frame read off the wire.
func alterAsSession(t *testing.T, tc *testCluster, index int, session *transport.Session, req AlterBucketRequest) *transport.Frame {
	t.Helper()
	payload, err := bson.Marshal(req)
	if err != nil {
		t.Fatalf("encoding the alter: %v", err)
	}
	ctx, cancel := context.WithTimeout(transport.WithSession(context.Background(), session), 30*time.Second)
	defer cancel()
	frame, err := tc.nodes[index].server.handler(ctx, transport.NewFrame(transport.OpAlterBucket, payload))
	if err != nil {
		t.Fatalf("node %d's handler failed the alter: %v", index+1, err)
	}
	return frame
}

// clientSession is a client connection authenticated as username with grants.
func clientSession(username string, grants ...transport.ScopedGrant) *transport.Session {
	identity := ident(grants...)
	identity.Username = username
	session := &transport.Session{Authenticated: true}
	session.SetIdentity(identity)
	return session
}

// TestAlterBucketRequiresDBAdmin is T-169's G3 row (spec §4.4, owner R9-Q5):
// OpAlterBucket is ruled {ActionDBAdmin, ScopeDatabase}, as a create and a
// declare are. A client session with dbAdmin on the database has its alter
// applied; one with read or write only, or dbAdmin on another database, is
// refused by the gate and nothing is logged on any node; a member session sending
// the alter raw is refused too, since a follower forwards inside OpReplPush.
func TestAlterBucketRequiresDBAdmin(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)

	member := &transport.Session{Authenticated: true, Peer: true}
	member.SetIdentity(transport.Identity{Username: NodeUser})
	for _, session := range []struct {
		name    string
		session *transport.Session
		applied bool
	}{
		{"a reader of the database", clientSession("reader", transport.ScopedGrant{Role: "reader", DB: testDatabase}), false},
		{"a writer of the database", clientSession("writer", transport.ScopedGrant{Role: "writer", DB: testDatabase}), false},
		{"a db-manager of another database", clientSession("elsewhere", transport.ScopedGrant{Role: "db-manager", DB: "other"}), false},
		{"a db-manager of the database", clientSession("manager", transport.ScopedGrant{Role: "db-manager", DB: testDatabase}), true},
		{"a cluster member sending the alter raw", member, false},
	} {
		t.Run(session.name, func(t *testing.T) {
			bucket := bucketOwnedBy(t, tc, 0, strings.ReplaceAll(session.name, " ", ""))
			createBucketAt(tc, 0, testDatabase, bucket, 1)
			frame := alterAsSession(t, tc, 0, session.session, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2})
			if session.applied {
				if answer := alterAccepted(t, frame); answer.ReplicationFactor != 2 || answer.PreviousFactor != 1 {
					t.Errorf("%s's alter of %q answered factor %d from %d, want 2 from 1", session.name, bucket, answer.ReplicationFactor, answer.PreviousFactor)
				}
				if logged := walOpCount(t, tc.nodes[0].server.core.wal, transport.OpAlterBucket, bucket); logged != 1 {
					t.Errorf("the owner logged %d alters of %q that %s sent, want 1", logged, bucket, session.name)
				}
				return
			}
			if frame.Opcode != transport.OpError {
				t.Fatalf("%s's alter of %q was answered %s, not a refusal: %s", session.name, bucket, frame.Opcode, frame.Payload)
			}
			if code := decodeErrorCode(t, frame.Payload); code != int(transport.ErrPermissionDenied) {
				t.Fatalf("%s's alter of %q was refused with code %d, %q; want PERMISSION_DENIED from the gate",
					session.name, bucket, code, decodeErrorMessage(t, frame.Payload))
			}
			if session.session != member {
				if message := decodeErrorMessage(t, frame.Payload); !strings.Contains(message, `lacks "db-manager"`) {
					t.Errorf("%s's refusal does not name the db-manager grant it lacks: %q", session.name, message)
				}
			}
			for index, node := range tc.nodes {
				if logged := walOpCount(t, node.server.core.wal, transport.OpAlterBucket, bucket); logged != 0 {
					t.Errorf("node %d logged %d alters of %q that %s sent, which the gate refused", index+1, logged, bucket, session.name)
				}
			}
		})
	}
}

// TestAMemberAdvertisesBucketRFAlter: a build advertises bucket_rf_alter, and
// a three-node cluster's gossip carries it from every member to every other
// (spec §4.4, §11.2: advertised from G3, in the commit that adds the alter's
// authorization row).
func TestAMemberAdvertisesBucketRFAlter(t *testing.T) {
	if !containsString(clusterFeatures(), featureBucketRFAlter) {
		t.Fatalf("clusterFeatures() = %v, which lacks %q", clusterFeatures(), featureBucketRFAlter)
	}
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	waitForAlterFeature(t, tc)
}
