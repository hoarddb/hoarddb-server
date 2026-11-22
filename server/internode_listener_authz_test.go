package server

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The internode listener serves cluster members. A session that arrived on it
// is exempt from the user authorization gate because the coordinator already
// authorized the original client, so the exemption must belong to the node
// identity, not to the port: anything else lets whoever can reach the port
// skip the gate with an ordinary account.

// authFailMessage decodes the message of an AUTH_FAIL payload.
func authFailMessage(t *testing.T, frame *transport.Frame) string {
	t.Helper()
	var failure struct {
		Message string `bson:"message"`
	}
	if err := bson.Unmarshal(frame.Payload, &failure); err != nil {
		t.Fatalf("decoding the AUTH_FAIL payload: %v", err)
	}
	return failure.Message
}

// TestUserCredentialsRefusedOnInternodeListener is the mirror of
// TestNodeCredentialsRefusedOnDriverPort: a user's password is not a way onto
// the member-only port.
func TestUserCredentialsRefusedOnInternodeListener(t *testing.T) {
	authMgr := newTestAuthManager(t, discardLogger())
	seedUser(t, authMgr, "alice")
	s := userTestServer(t)

	peerSession := &transport.Session{Peer: true}
	ctx := transport.WithSession(context.Background(), peerSession)
	payload := mustMarshalBSON(t, &AuthRequest{Username: "alice", Password: "correct-horse-battery"})

	resp, err := handleAuth(ctx, transport.NewFrame(transport.OpAuth, payload), authMgr, s, discardLogger())
	if err != nil {
		t.Fatalf("handleAuth: %v", err)
	}
	if resp.Opcode != transport.OpAuthFail {
		t.Fatalf("a user's password on the internode listener answered %s, want AUTH_FAIL", resp.Opcode)
	}
	if identity, ok := peerSession.Identity(); ok {
		t.Fatalf("the internode session was given the identity %q; it must stay unauthenticated", identity.Username)
	}
	if message := authFailMessage(t, resp); !strings.Contains(message, "driver") {
		t.Fatalf("the refusal %q does not tell the client to use the driver port", message)
	}
}

// TestUserTokenRefusedOnInternodeListener covers the other way in: a
// resumption token a user earned on the driver port.
func TestUserTokenRefusedOnInternodeListener(t *testing.T) {
	authMgr := newTestAuthManager(t, discardLogger())
	seedUser(t, authMgr, "alice")
	s := userTestServer(t)
	token, _ := mintToken(t, authMgr, "alice")

	peerSession := &transport.Session{Peer: true}
	ctx := transport.WithSession(context.Background(), peerSession)
	payload := mustMarshalBSON(t, &AuthRequest{Token: token})

	resp, err := handleAuth(ctx, transport.NewFrame(transport.OpAuth, payload), authMgr, s, discardLogger())
	if err != nil {
		t.Fatalf("handleAuth: %v", err)
	}
	if resp.Opcode != transport.OpAuthFail {
		t.Fatalf("a user's token on the internode listener answered %s, want AUTH_FAIL", resp.Opcode)
	}
	if identity, ok := peerSession.Identity(); ok {
		t.Fatalf("the internode session was given the identity %q; it must stay unauthenticated", identity.Username)
	}
}

// TestPeerExemptionBelongsToTheNodeIdentity pins who the gate waves through
// on the internode listener: the node identity, and nobody else.
func TestPeerExemptionBelongsToTheNodeIdentity(t *testing.T) {
	s := userTestServer(t)
	handler := NewHandler(s)

	createDatabase := func(sess *transport.Session, name string) *transport.Frame {
		t.Helper()
		ctx := transport.WithSession(context.Background(), sess)
		resp, err := handler(ctx, transport.NewFrame(transport.OpCreateDB, mustMarshalBSON(t, &CreateDatabaseRequest{Name: name})))
		if err != nil {
			t.Fatalf("handler: %v", err)
		}
		return resp
	}

	reader := &transport.Session{Peer: true}
	reader.SetIdentity(transport.Identity{
		Username: "reader", Roles: []string{"reader"}, Databases: []string{"app"},
		Grants: []transport.ScopedGrant{{Role: "reader", DB: "app"}},
	})
	if resp := createDatabase(reader, "byreader"); resp.Opcode != transport.OpError {
		t.Fatalf("a read-only user on an internode session created a database (answer %s); the exemption is the node's, not the port's", resp.Opcode)
	}

	unauthenticated := &transport.Session{Peer: true}
	if resp := createDatabase(unauthenticated, "bynobody"); resp.Opcode != transport.OpError {
		t.Fatalf("an internode session with no identity created a database (answer %s)", resp.Opcode)
	}

	node := &transport.Session{Peer: true}
	node.SetIdentity(transport.Identity{Username: NodeUser})
	migrate := transport.NewFrame(transport.OpMigrate, mustMarshalBSON(t, &MigrateRequest{Database: "app", Bucket: "b"}))
	if err := s.authorize(transport.WithSession(context.Background(), node), migrate); err != nil {
		t.Fatalf("a cluster member's MIGRATE was refused: %v", err)
	}
}

// TestMemberSessionIsRefusedOpcodesMembersDoNotSend pins that the member
// exemption admits the opcodes members send as themselves and no other gated
// one. Table over every opcode in opRules.
func TestMemberSessionIsRefusedOpcodesMembersDoNotSend(t *testing.T) {
	s := userTestServer(t)
	member := &transport.Session{Peer: true}
	member.SetIdentity(transport.Identity{Username: NodeUser})
	ctx := transport.WithSession(context.Background(), member)

	for opcode, rule := range opRules {
		t.Run(opcode.String(), func(t *testing.T) {
			err := s.authorize(ctx, transport.NewFrame(opcode, nil))
			_, sentByMembers := memberGatedOpcodes[opcode]
			switch {
			case sentByMembers:
				if err != nil {
					t.Fatalf("a member's %s was refused: %v", opcode, err)
				}
			case rule.Action == ActionNone:
				if err != nil {
					t.Fatalf("a member's ungated %s was refused: %v", opcode, err)
				}
			default:
				if !errors.Is(err, ErrPermissionDenied) {
					t.Fatalf("a member's %s was admitted or refused wrongly (err %v), want a permission denial", opcode, err)
				}
			}
		})
	}
	if _, ok := memberGatedOpcodes[transport.OpMigrate]; !ok || len(memberGatedOpcodes) != 1 {
		t.Fatalf("memberGatedOpcodes = %v, want exactly OpMigrate", memberGatedOpcodes)
	}
}

// TestDumpRootGateExemptsOnlyTheNodeIdentity is the same rule on the dump and
// restore path, which has its own gate.
func TestDumpRootGateExemptsOnlyTheNodeIdentity(t *testing.T) {
	s := userTestServer(t)

	reader := &transport.Session{Peer: true}
	reader.SetIdentity(transport.Identity{
		Username: "reader", Roles: []string{"reader"}, Databases: []string{"app"},
		Grants: []transport.ScopedGrant{{Role: "reader", DB: "app"}},
	})
	if err := requireRootIdentity(transport.WithSession(context.Background(), reader), s); err == nil {
		t.Fatal("a read-only user on an internode session passed the dump gate")
	}

	node := &transport.Session{Peer: true}
	node.SetIdentity(transport.Identity{Username: NodeUser})
	if err := requireRootIdentity(transport.WithSession(context.Background(), node), s); err != nil {
		t.Fatalf("a cluster member's forwarded dump was refused: %v", err)
	}
}
