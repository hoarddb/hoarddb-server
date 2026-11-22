package server

import (
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestExistsOnKeylessTypeIsRefused is BUG-0516 row 5: EXISTS on a fifo, lifo or
// heap answered exists:false, which is an answer to a question the server never
// asked; the refusal sentence Find carried was dropped by the handler. An empty
// key was reported the same way.
func TestExistsOnKeylessTypeIsRefused(t *testing.T) {
	s := newTestServer(t)
	for _, bucketType := range []BucketType{BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap} {
		t.Run(string(bucketType), func(t *testing.T) {
			bucket := "q_" + string(bucketType)
			createTestBucket(t, s, "app", bucket, bucketType)
			seedMatrixBucket(t, s, bucket, bucketType)

			reply, err := s.dispatch(context.Background(), requestFrame(t, transport.OpExists, FindRequest{Database: "app", Bucket: bucket, Key: "k1"}))
			if err != nil {
				t.Fatalf("dispatching EXISTS: %v", err)
			}
			if reply.Opcode != transport.OpError {
				t.Fatalf("EXISTS on a %s answered %s %s, want an error frame", bucketType, reply.Opcode, reply.Payload)
			}
			refused := decodeErrorFrame(t, reply, "EXISTS on a "+string(bucketType))
			if refused.Code != int(transport.ErrInvalidArgument) {
				t.Errorf("code %d, want INVALID_ARGUMENT (%d): %s", refused.Code, transport.ErrInvalidArgument, refused.Message)
			}
			if !strings.Contains(refused.Message, string(bucketType)) {
				t.Errorf("the refusal %q does not name the bucket type %s", refused.Message, bucketType)
			}
		})
	}

	t.Run("an empty key is not a miss", func(t *testing.T) {
		createTestBucket(t, s, "app", "h", BucketTypeHash)
		reply, err := s.dispatch(context.Background(), requestFrame(t, transport.OpExists, FindRequest{Database: "app", Bucket: "h"}))
		if err != nil {
			t.Fatalf("dispatching EXISTS: %v", err)
		}
		refused := decodeErrorFrame(t, reply, "EXISTS with no key")
		if refused.Code != int(transport.ErrInvalidArgument) {
			t.Errorf("code %d, want INVALID_ARGUMENT: %s", refused.Code, refused.Message)
		}
	})

	t.Run("a hash still answers both ways", func(t *testing.T) {
		createTestBucket(t, s, "app", "h2", BucketTypeHash)
		seedMatrixBucket(t, s, "h2", BucketTypeHash)
		for key, want := range map[string]bool{"k1": true, "absent": false} {
			reply, err := s.dispatch(context.Background(), requestFrame(t, transport.OpExists, FindRequest{Database: "app", Bucket: "h2", Key: key}))
			if err != nil {
				t.Fatalf("dispatching EXISTS: %v", err)
			}
			var answer struct {
				Exists bool `bson:"exists"`
			}
			if reply.Opcode != transport.OpExistsResponse || bson.Unmarshal(reply.Payload, &answer) != nil || answer.Exists != want {
				t.Errorf("EXISTS %q on a hash: %s %s, want exists=%v", key, reply.Opcode, reply.Payload, want)
			}
		}
	})
}
