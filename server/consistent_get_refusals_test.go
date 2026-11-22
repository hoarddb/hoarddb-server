package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestConsistentGetOnAMissingBucketMatchesGet is BUG-0469 item 4: a GET of a
// bucket (or database) that does not exist is an ERROR frame, NOT_FOUND, with
// the "Run 'show buckets ...'" advice; a consistent get answered it in band as
// found:false, which a driver that tests `found` reads as a missing key.
func TestConsistentGetOnAMissingBucketMatchesGet(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)
	cases := []struct{ name, database, bucket string }{
		{"missing bucket", "app", "nobody"},
		{"missing database", "nodb", "users"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.dispatch(context.Background(), requestFrame(t, transport.OpGet,
				FindRequest{Database: tc.database, Bucket: tc.bucket, Key: "k"}))
			if err != nil {
				t.Fatalf("dispatching GET: %v", err)
			}
			want := decodeErrorFrame(t, got, "GET of a "+tc.name)

			reply, err := s.dispatch(context.Background(), requestFrame(t, transport.OpConsistentGet,
				ConsistentGetRequest{Database: tc.database, Bucket: tc.bucket, Key: "k"}))
			if err != nil {
				t.Fatalf("dispatching CONSISTENT_GET: %v", err)
			}
			if reply.Opcode != transport.OpError {
				t.Fatalf("consistent get of a %s answered in band as %s, want the ERROR frame GET answers", tc.name, reply.Opcode)
			}
			refusal := decodeErrorFrame(t, reply, "consistent get of a "+tc.name)
			if refusal.Code != want.Code || refusal.Message != want.Message {
				t.Errorf("consistent get refused with code %d %q, GET with code %d %q", refusal.Code, refusal.Message, want.Code, want.Message)
			}
		})
	}
}

// TestConsistentGetOfAWrongTypeBucketFromANodeThatDoesNotHoldItIsRefusedLikeTheHolder
// is BUG-0469's cluster path: the coordinator reads every holder, and a holder
// that refused the verb as the wrong store type reached the comparison as an
// unanswered holder, so a node that does not hold the bucket answered with a
// "did not answer" refusal instead of the wrong-type sentence the holder gives.
func TestConsistentGetOfAWrongTypeBucketFromANodeThatDoesNotHoldItIsRefusedLikeTheHolder(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeFIFO})
	waitForServing(t, tc, testDatabase, testBucket, 1)
	holder, _ := ownerAndReplicas(t, tc, testDatabase, testBucket)

	// The cluster's read path forwards a read to the bucket's owner, so a node
	// outside the replica set coordinates only when handed the frame below the
	// forward (Server.dispatch): the coordinator that reads every holder over
	// the wire, where the holder's refusal reaches it as a peer's answer.
	want := wrongTypeSentence("consistent get is not supported for bucket type fifo.", BucketTypeFIFO).Error()
	for index := range tc.nodes {
		role := "a node that does not hold the bucket"
		if index == holder {
			role = "the holder"
		}
		reply := consistentGetFrame(t, tc.nodes[index], testDatabase, testBucket, "k")
		what := "consistent get of a fifo bucket at " + role
		if reply.Opcode != transport.OpError {
			t.Fatalf("%s: answered %s, want an ERROR frame", what, reply.Opcode)
		}
		refusal := decodeErrorFrame(t, reply, what)
		if refusal.Code != 4 {
			t.Errorf("%s: code %d (%q), want 4 INVALID_ARGUMENT", what, refusal.Code, refusal.Message)
		}
		if !strings.HasSuffix(refusal.Message, want) {
			t.Errorf("%s: message %q does not end with the wrong-type sentence %q", what, refusal.Message, want)
		}
	}

	outsider := (holder + 1) % len(tc.nodes)
	reply, err := tc.nodes[outsider].server.core.dispatch(context.Background(), requestFrame(t, transport.OpConsistentGet,
		ConsistentGetRequest{Database: testDatabase, Bucket: testBucket, Key: "k"}))
	if err != nil {
		t.Fatalf("dispatching CONSISTENT_GET at node %d: %v", outsider+1, err)
	}
	what := "consistent get coordinated by a node outside the bucket's replica set"
	if reply.Opcode != transport.OpError {
		t.Fatalf("%s: answered %s, want an ERROR frame", what, reply.Opcode)
	}
	refusal := decodeErrorFrame(t, reply, what)
	if refusal.Code != 4 || !strings.HasSuffix(refusal.Message, want) {
		t.Errorf("%s: code %d %q, want 4 ending with %q", what, refusal.Code, refusal.Message, want)
	}
}

// TestConsistentGetOfAMissingBucketInAClusterIsNotFoundAtEveryNode is the last
// open case of BUG-0469: every holder answered that it has no such bucket, and
// the coordinator reported them as holders that "did not answer", with advice to
// retry. It is the NOT_FOUND refusal GET and a standalone consistent get give.
func TestConsistentGetOfAMissingBucketInAClusterIsNotFoundAtEveryNode(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 1)

	cases := []struct {
		name, database, bucket string
		wantText               string
	}{
		{"missing bucket", testDatabase, "nobody", "show buckets in " + testDatabase},
		{"missing database", "nodb", testBucket, "show databases"},
	}
	for _, tcase := range cases {
		check := func(what string, reply *transport.Frame) {
			t.Helper()
			if reply.Opcode != transport.OpError {
				t.Fatalf("%s: answered %s, want an ERROR frame", what, reply.Opcode)
			}
			refusal := decodeErrorFrame(t, reply, what)
			if refusal.Code != 1 {
				t.Errorf("%s: code %d (%q), want 1 NOT_FOUND", what, refusal.Code, refusal.Message)
			}
			if !strings.Contains(refusal.Message, tcase.wantText) || strings.Contains(refusal.Message, "retry") {
				t.Errorf("%s: message %q should give the standard not-found advice (%q) and no retry advice", what, refusal.Message, tcase.wantText)
			}
		}
		for index := range tc.nodes {
			check(fmt.Sprintf("consistent get of a %s at node %d", tcase.name, index+1),
				consistentGetFrame(t, tc.nodes[index], tcase.database, tcase.bucket, "k"))
		}
		for index := range tc.nodes {
			reply, err := tc.nodes[index].server.core.dispatch(context.Background(), requestFrame(t, transport.OpConsistentGet,
				ConsistentGetRequest{Database: tcase.database, Bucket: tcase.bucket, Key: "k"}))
			if err != nil {
				t.Fatalf("dispatching CONSISTENT_GET at node %d: %v", index+1, err)
			}
			check(fmt.Sprintf("consistent get of a %s coordinated through dispatch at node %d", tcase.name, index+1), reply)
		}
	}
}

// TestNotFoundRefusalNeedsEveryHolderToSayNotFound pins the rule for holders
// that disagree: only a read in which every holder answered "not found" is
// refused as not found. One that holds the bucket, one that did not answer, or
// a holder from before the flag (a plain error) is a divergence or an unknown
// the comparison still has to judge.
func TestNotFoundRefusalNeedsEveryHolderToSayNotFound(t *testing.T) {
	missing := &sentencedError{message: "bucket not found", cause: ErrBucketNotFound}
	cases := []struct {
		name  string
		reads map[string]holderRead
		want  bool
	}{
		{"all not found", map[string]holderRead{"a": {err: missing}, "b": {err: missing}}, true},
		{"one holds the bucket", map[string]holderRead{"a": {err: missing}, "b": {}}, false},
		{"one did not answer", map[string]holderRead{"a": {err: missing}, "b": {err: errors.New("dial: refused")}}, false},
		{"holder from before the flag", map[string]holderRead{"a": {err: errors.New("bucket not found")}}, false},
		{"no holders", map[string]holderRead{}, false},
	}
	for _, tcase := range cases {
		t.Run(tcase.name, func(t *testing.T) {
			refusal := consistentRead{reads: tcase.reads}.notFoundRefusal()
			if (refusal != nil) != tcase.want {
				t.Fatalf("notFoundRefusal() = %v, want a refusal: %v", refusal, tcase.want)
			}
		})
	}
}
