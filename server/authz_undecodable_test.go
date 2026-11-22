package server

import (
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestUndecodableRoutingFieldDoesNotWidenAGrant: The gate reads a
// frame's database from frameMeta, and when that decode failed it authorized
// the frame against an empty resource — which databaseMatches treats as
// covered by any grant. frameMeta and the handlers decode different structs:
// frameMeta holds "write_concern" as a string, and no request struct has that
// key at all. So a payload whose write_concern is a number fails only the
// gate's decode, is authorized as unscoped, and the handler writes to the
// database it names. A user scoped to one database must never reach another.
func TestUndecodableRoutingFieldDoesNotWidenAGrant(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "secret", "vault", BucketTypeHash)

	sess := &transport.Session{}
	sess.SetIdentity(transport.Identity{
		Username: "alice", Roles: []string{"writer"}, Databases: []string{"app"},
		Grants: []transport.ScopedGrant{{Role: "writer", DB: "app"}},
	})
	ctx := transport.WithSession(context.Background(), sess)

	for _, tt := range []struct {
		name string
		op   transport.Opcode
	}{
		{"write", transport.OpPut},
		{"read", transport.OpGet},
	} {
		t.Run(tt.name, func(t *testing.T) {
			payload := mustMarshalBSON(t, bson.M{
				"database": "secret", "bucket": "vault", "key": "k",
				"data":          bson.M{"v": 1},
				"write_concern": int32(5),
			})
			// The handler's own decode reads the database the frame names, so
			// a gate that lets this through has let alice into "secret".
			var handlerView InsertRequest
			if err := bson.Unmarshal(payload, &handlerView); err != nil || handlerView.Database != "secret" {
				t.Fatalf("test setup: the handler's decode gave %+v, %v; want database \"secret\"", handlerView, err)
			}

			if err := s.authorize(ctx, transport.NewFrame(tt.op, payload)); err == nil {
				t.Errorf("alice (writer@app only) was authorized for a %s on database \"secret\": a payload the gate could not decode was checked as unscoped", tt.op)
			}
		})
	}
}
