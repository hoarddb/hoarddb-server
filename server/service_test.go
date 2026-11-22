package server

import (
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// createBucketOverTheWire sends a create bucket frame to the handler and returns
// the answer, as a client's request reaches the server.
func createBucketOverTheWire(t *testing.T, handler transport.RequestHandler, bucketType BucketType) *transport.Frame {
	t.Helper()
	request := CreateBucketRequest{Database: "app", Name: "b", Type: bucketType}
	answer, err := handler(context.Background(), transport.NewFrame(transport.OpCreateBucket, mustMarshal(t, request)))
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return answer
}

// TestCreateBucketOfAnotherTypeIsRefused: a create that names an
// existing bucket with a different store type is not the redelivery of the
// first create, so it is refused with ALREADY_EXISTS, with a message that names
// the type the bucket has and the way out. Before, it answered success and only
// the prose said otherwise.
func TestCreateBucketOfAnotherTypeIsRefused(t *testing.T) {
	s := newTestServer(t)
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	handler := NewHandler(s)
	if first := createBucketOverTheWire(t, handler, BucketTypeHash); first.Opcode == transport.OpError {
		t.Fatalf("first create refused: %s", first.Payload)
	}

	answer := createBucketOverTheWire(t, handler, BucketTypeFIFO)
	if answer.Opcode != transport.OpError {
		t.Fatalf("create of the same name as another type answered %s, want ERROR: the caller asked for a fifo and holds a hash", answer.Opcode)
	}
	if got, want := decodeErrorCode(t, answer.Payload), int(transport.ErrAlreadyExists); got != want {
		t.Errorf("error code = %d, want %d (ALREADY_EXISTS)", got, want)
	}
	message := decodeErrorMessage(t, answer.Payload)
	for _, want := range []string{`"b"`, "type hash", "fifo", "drop bucket b"} {
		if !strings.Contains(message, want) {
			t.Errorf("message %q does not contain %q", message, want)
		}
	}
	stats, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: "b"})
	if err != nil || stats.Type != BucketTypeHash {
		t.Errorf("the bucket is %+v, %v after the refused create, want it unchanged as a hash", stats, err)
	}
}

// TestCreateBucketSameTypeStaysIdempotent is the half of the type-conflict rule that must not
// change: the same create delivered twice is a success that says the bucket
// exists, which a replay and a redelivery depend on.
func TestCreateBucketSameTypeStaysIdempotent(t *testing.T) {
	s := newTestServer(t)
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		created, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "b", Type: BucketTypeHash})
		if err != nil || !created.Success {
			t.Fatalf("create attempt %d = %+v, %v, want success", attempt, created, err)
		}
		if attempt == 2 && !strings.Contains(created.Message, "already exists (type: hash)") {
			t.Errorf("second create message %q does not say the bucket already exists", created.Message)
		}
	}
}

// TestCreateBucketOfAnotherTypeIsRefusedOnACluster checks the type-conflict refusal through a
// cluster: the owner's refusal reaches the client with its code from whichever
// node the client is connected to, and a create of the same type still succeeds.
func TestCreateBucketOfAnotherTypeIsRefusedOnACluster(t *testing.T) {
	tc := newTestCluster(t, 3, 2)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})

	for node := range 3 {
		answer := tc.frame(node, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeFIFO})
		if answer.Opcode != transport.OpError {
			t.Fatalf("node %d answered %s to a create of another type, want ERROR", node+1, answer.Opcode)
		}
		if got, want := decodeErrorCode(t, answer.Payload), int(transport.ErrAlreadyExists); got != want {
			t.Errorf("node %d: error code = %d, want %d (ALREADY_EXISTS)", node+1, got, want)
		}
	}
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
}

// TestCreateBucketInMissingDatabaseSaysWhatItDid: `create bucket b in nodb`
// keeps its convenience — a missing database is created with the bucket — but
// the reply must say so, or a typo in the database name reads as if the data
// went somewhere it did not (owner ruling 2026-10-18, BUG-0264). A database
// that already exists keeps the plain reply: nothing but the bucket was made.
func TestCreateBucketInMissingDatabaseSaysWhatItDid(t *testing.T) {
	s := newTestServer(t)

	created, err := s.CreateBucket(CreateBucketRequest{Database: "nodb", Name: "b", Type: BucketTypeHash})
	if err != nil || !created.Success {
		t.Fatalf("create bucket in a missing database = %+v, %v, want success", created, err)
	}
	if !strings.Contains(created.Message, `created database "nodb"`) || !strings.Contains(created.Message, `bucket "b"`) {
		t.Errorf("the reply %q does not say it created the database as well as the bucket; a typo in the database name would go unnoticed", created.Message)
	}

	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	existing, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "b", Type: BucketTypeHash})
	if err != nil || !existing.Success {
		t.Fatalf("create bucket in an existing database = %+v, %v, want success", existing, err)
	}
	if strings.Contains(existing.Message, "created database") {
		t.Errorf("the reply %q claims it created the database, but \"app\" already existed", existing.Message)
	}
}
