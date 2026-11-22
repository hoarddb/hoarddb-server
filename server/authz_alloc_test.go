package server

import (
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0098, part 2: every authorized request paid for a deep copy of the
// session identity (Roles, Databases and Grants) inside the gate. The gate only
// reads them, so the copy bought nothing on this path.

// authorizeAllocFrame is a PUT-shaped request whose scope the gate reads, the
// shape a client sends for an ordinary write.
func authorizeAllocFrame() *transport.Frame {
	return transport.NewFrame(transport.OpPut, wireRequest(
		wireString("database", "app"), wireString("bucket", "docs"),
		wireString("key", "k1"),
		wireDocument("data", wireRequest(wireString("n", "1"))),
	))
}

// authorizeAllocContext is a session allowed to write app, attached to a
// context the way the transport attaches it.
func authorizeAllocContext() context.Context {
	sess := &transport.Session{}
	sess.SetIdentity(transport.Identity{
		Username:  "alice",
		Roles:     []string{roleRoot},
		Databases: []string{"*"},
		Grants:    []transport.ScopedGrant{{Role: roleRoot, DB: "*"}},
	})
	return transport.WithSession(context.Background(), sess)
}

// TestAuthorizedRequestAllocatesOnlyItsPayloadDecode is the failing test: the
// gate reads the payload once to find its scope, and must add no per-call work
// of its own. The bound is that one decode and no more: 13 objects for this
// request, all of them inside bson.Unmarshal. Before the fix an authorized
// request made 16, the extra three being the identity's three copied slices.
//
// The decode itself is BUG-0098 part 1b: removing it needs the payload's parsed
// value threaded from the gate to every handler, which the bug file defers as a
// multi-call-site change. The count here is therefore the decode's, and the
// assertion is that nothing else is added to it.
func TestAuthorizedRequestAllocatesOnlyItsPayloadDecode(t *testing.T) {
	s := &Server{}
	ctx := authorizeAllocContext()
	frame := authorizeAllocFrame()

	allocs := testing.AllocsPerRun(1000, func() {
		if err := s.authorize(ctx, frame); err != nil {
			t.Fatalf("an allowed request was refused: %v", err)
		}
	})
	const decodeAllocations = 13
	if allocs > decodeAllocations {
		t.Errorf("an authorized scoped request made %.0f allocations per call, want at most %d (its one payload decode); the gate is adding per-call work of its own, such as the identity's three copied slices",
			allocs, decodeAllocations)
	}
}

// BenchmarkAuthorizeAllowedScopedRequest records the gate's cost per authorized
// request and the allocation count TestAuthorizedRequestAllocatesOnlyItsPayloadDecode
// bounds.
func BenchmarkAuthorizeAllowedScopedRequest(b *testing.B) {
	s := &Server{}
	ctx := authorizeAllocContext()
	frame := authorizeAllocFrame()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := s.authorize(ctx, frame); err != nil {
			b.Fatalf("an allowed request was refused: %v", err)
		}
	}
}
