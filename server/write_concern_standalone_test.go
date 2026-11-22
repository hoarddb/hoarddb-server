package server

import (
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestStandaloneNodeRefusesAnUnknownWriteConcern: a node that does not route
// answers a write concern word it does not know as a cluster does
// (INVALID_ARGUMENT, the accepted words, write_outcome not_applied) and
// applies nothing. Before the fix a standalone node acknowledged the write.
// The frame goes through NewHandler, the client door; Server.dispatch is the
// funnel replay shares and never sees a client frame.
func TestStandaloneNodeRefusesAnUnknownWriteConcern(t *testing.T) {
	const database, bucket = "app", "users"
	for _, field := range []string{"writeconcern", "write_concern"} {
		for _, mistyped := range []string{"quorum", "majorty", "2"} {
			t.Run(field+"="+mistyped, func(t *testing.T) {
				s := newTestServer(t)
				createTestBucket(t, s, database, bucket, BucketTypeHash)
				payload, err := bson.Marshal(bson.D{
					{Key: "database", Value: database}, {Key: "bucket", Value: bucket}, {Key: "key", Value: "k"},
					{Key: "data", Value: bson.M{"n": 1}}, {Key: field, Value: mistyped},
				})
				if err != nil {
					t.Fatalf("marshal put: %v", err)
				}
				resp, err := NewHandler(s)(context.Background(), transport.NewFrame(transport.OpPut, payload))
				if err != nil {
					t.Fatalf("dispatch: unexpected transport error: %v", err)
				}
				if resp.Opcode != transport.OpError {
					t.Fatalf("write concern %q was accepted (response %s); it must be refused", mistyped, resp.Opcode)
				}
				var refusal ErrorResponse
				if err := bson.Unmarshal(resp.Payload, &refusal); err != nil {
					t.Fatalf("decode refusal: %v", err)
				}
				if refusal.Code != int(transport.ErrInvalidArgument) {
					t.Errorf("refused with code %d, want INVALID_ARGUMENT (%d)", refusal.Code, transport.ErrInvalidArgument)
				}
				if refusal.WriteOutcome != outcomeNotApplied {
					t.Errorf("write_outcome = %q, want %q", refusal.WriteOutcome, outcomeNotApplied)
				}
				for _, want := range []string{mistyped, "majority", "all", "one"} {
					if !strings.Contains(refusal.Message, want) {
						t.Errorf("refusal does not mention %q: %s", want, refusal.Message)
					}
				}
				if got, err := s.Find(FindRequest{Database: database, Bucket: bucket, Key: "k"}); err == nil && got.Found {
					t.Errorf("the refused write was applied")
				}
			})
		}
	}
}

// TestStandaloneNodeStillAcceptsKnownWriteConcerns guards the fix against
// over-refusing: every accepted word is a no-op on one node, not an error.
func TestStandaloneNodeStillAcceptsKnownWriteConcerns(t *testing.T) {
	const database, bucket = "app", "users"
	s := newTestServer(t)
	createTestBucket(t, s, database, bucket, BucketTypeHash)
	for _, word := range []string{"1", "one", "majority", "all", "MAJORITY"} {
		payload, err := bson.Marshal(bson.D{
			{Key: "database", Value: database}, {Key: "bucket", Value: bucket}, {Key: "key", Value: "k-" + word},
			{Key: "data", Value: bson.M{"n": 1}}, {Key: "writeconcern", Value: word},
		})
		if err != nil {
			t.Fatalf("marshal put: %v", err)
		}
		resp, err := NewHandler(s)(context.Background(), transport.NewFrame(transport.OpPut, payload))
		if err != nil || resp.Opcode == transport.OpError {
			t.Errorf("write concern %q was refused on one node: %v %v", word, err, resp)
		}
	}
}
