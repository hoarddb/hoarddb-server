package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// These tests cover one defect. The authorization gate found a frame's database by
// decoding its payload as BSON, and GET_FAST's payload is a compact binary
// layout, so every GET_FAST was refused once an undecodable
// payload became a refusal. They run over the driver listener because the defect
// lived between the listener and the handler: a direct handleGetFast call
// never meets the gate.

const getFastTestPassword = "get-fast-password-123"

// getFastUsers are the scoped users the listener tests log in as. Each holds
// one grant, so a test names the role it is about.
var getFastUsers = []UserRole{
	{Role: "reader", DB: "app"},
	{Role: "writer", DB: "app"},
	{Role: "db-manager", DB: "app"},
	{Role: "backup-operator", DB: "app"},
	{Role: "reader", DB: "*"},
}

// getFastUsername names the user holding exactly one grant.
func getFastUsername(grant UserRole) string {
	if grant.DB == "*" {
		return grant.Role + "-everywhere"
	}
	return grant.Role + "-" + grant.DB
}

// newGetFastAuthzServer starts a server with the driver listener, two
// databases holding one readable document each, and one user per grant in
// getFastUsers. It returns the listener's address.
func newGetFastAuthzServer(t *testing.T) string {
	t.Helper()
	qs, addr := newTCPEnabledServer(t, nil)
	seedSmallQueryBucket(t, qs.core, "events")
	createTestBucket(t, qs.core, "secret", "vault", BucketTypeHash)
	insertScanDoc(t, qs.core, "secret", "vault", "k01", map[string]interface{}{"n": int32(7)})

	root := dialTCPServer(t, addr)
	root.login()
	for _, grant := range getFastUsers {
		// One credential per user: the store refuses a salt it has seen.
		credential, err := NewCredential(getFastTestPassword, DefaultArgon2Params())
		if err != nil {
			t.Fatalf("NewCredential: %v", err)
		}
		var created UserMutationResponse
		root.call(transport.OpUserCreate, &UserCreateRequest{
			RequestID:  fmt.Sprintf("create-%s-%d", getFastUsername(grant), time.Now().UnixNano()),
			Username:   getFastUsername(grant),
			Credential: UserCredential{Algo: credential.Algo, Salt: credential.Salt, Hash: credential.Hash, Params: credential.Params},
			Roles:      []UserRole{grant},
		}, &created)
	}
	return addr
}

// mustGetFastPayload encodes a GET_FAST request with the production encoder.
func mustGetFastPayload(t *testing.T, database, bucket, key string) []byte {
	t.Helper()
	payload, err := transport.MarshalGetFast(transport.GetFastRequest{Database: database, Bucket: bucket, Key: key})
	if err != nil {
		t.Fatalf("encoding GET_FAST for %s.%s/%s: %v", database, bucket, key, err)
	}
	return payload
}

// sendGetFast sends one GET_FAST payload and returns the answer, or the error
// that stood in for one. A closed stream is an error here, not a test abort:
// on the unfixed tree a payload the gate mis-scoped reaches the handler, and
// the handler's failure resets the connection.
func sendGetFast(conn *queryConn, payload []byte) (*transport.Frame, error) {
	if err := conn.send(transport.NewFrame(transport.OpGetFast, payload)); err != nil {
		return nil, err
	}
	if err := conn.conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return nil, fmt.Errorf("setting the read deadline: %w", err)
	}
	return conn.read()
}

// answerText renders an answer for a failure message: an error frame as its
// message, anything else as its opcode.
func answerText(answer *transport.Frame) string {
	if answer.Opcode != transport.OpError {
		return answer.Opcode.String()
	}
	var refusal ErrorResponse
	if err := bson.Unmarshal(answer.Payload, &refusal); err != nil {
		return fmt.Sprintf("ERROR with an unreadable payload (%v)", err)
	}
	return fmt.Sprintf("ERROR code %d: %s", refusal.Code, refusal.Message)
}

// refusalMessage returns the message of an error frame, failing the test when
// the answer is anything else.
func refusalMessage(t *testing.T, answer *transport.Frame) string {
	t.Helper()
	if answer.Opcode != transport.OpError {
		t.Fatalf("answered %s; want %s", answer.Opcode, transport.OpError)
	}
	var refusal ErrorResponse
	if err := bson.Unmarshal(answer.Payload, &refusal); err != nil {
		t.Fatalf("decoding the refusal: %v", err)
	}
	return refusal.Message
}

// TestGateAndRouterAgreeOnTheScopeOfEveryScopedOpcode is the guard
// that asks for one reader. The gate (frameScope) and the cluster's router (handleClientFrame)
// each read the bucket and database a frame names, and they read GET_FAST
// differently until the router was given the gate's reader. Every opcode the
// authorization table scopes is read both ways here, from a payload in the
// encoding its handler decodes, and the two must agree on a scope that names
// something. An opcode with a layout of its own needs a sample in
// wireLayoutSamples and a case in hasWireLayoutPayload; adding one without the
// other fails here.
func TestGateAndRouterAgreeOnTheScopeOfEveryScopedOpcode(t *testing.T) {
	wireLayoutSamples := map[transport.Opcode][]byte{
		transport.OpGetFast: mustGetFastPayload(t, "app", "events", "k01"),
	}
	bsonSample := mustMarshalBSON(t, map[string]interface{}{"database": "app", "bucket": "events", "name": "events", "key": "k01"})

	for op, rule := range opRules {
		if rule.Scope == ScopeNone {
			continue
		}
		sample, hasLayout := wireLayoutSamples[op]
		if hasLayout != hasWireLayoutPayload(op) {
			t.Errorf("%s: wireLayoutSamples has a sample = %v but hasWireLayoutPayload = %v; they must name the same opcodes", op, hasLayout, hasWireLayoutPayload(op))
			continue
		}
		if !hasLayout {
			sample = bsonSample
		}
		frame := transport.NewFrame(op, sample)

		gateBucket, gateDatabase, err := frameScope(frame)
		if err != nil {
			t.Errorf("%s: the gate cannot read a payload in the encoding its handler decodes: %v", op, err)
			continue
		}
		routerBucket, routerDatabase, err := scopeOfFrame(frame, decodeFrameMeta(frame.Payload))
		if err != nil {
			t.Errorf("%s: the router cannot read it: %v", op, err)
			continue
		}
		if gateBucket != routerBucket || gateDatabase != routerDatabase {
			t.Errorf("%s: the gate reads bucket %q in database %q, the router reads bucket %q in database %q", op, gateBucket, gateDatabase, routerBucket, routerDatabase)
		}
		if gateDatabase == "" {
			t.Errorf("%s: the scope the gate and router agree on names no database", op)
		}
	}
}

// TestGetFastOverTheListenerAnswersEveryRoleThatMayRead is the half of
// the GET_FAST defect a user feels: root, and every role that satisfies a read on the
// database, gets the document from GET_FAST, and a miss is a status, not an
// error.
func TestGetFastOverTheListenerAnswersEveryRoleThatMayRead(t *testing.T) {
	addr := newGetFastAuthzServer(t)

	logins := []string{"admin"}
	for _, grant := range getFastUsers {
		logins = append(logins, getFastUsername(grant))
	}
	for _, username := range logins {
		t.Run(username, func(t *testing.T) {
			password := getFastTestPassword
			if username == "admin" {
				password = "test123"
			}
			conn := mustDialQueryConn(t, addr, username, password)

			found, err := sendGetFast(conn, mustGetFastPayload(t, "app", "events", "k01"))
			if err != nil {
				t.Fatalf("GET_FAST app.events/k01: %v", err)
			}
			if found.Opcode != transport.OpGetFastResponse {
				t.Fatalf("GET_FAST app.events/k01 answered %s; want %s", answerText(found), transport.OpGetFastResponse)
			}
			status, documentBytes, err := transport.UnmarshalGetFastResponse(found.Payload)
			if err != nil {
				t.Fatalf("decoding the GET_FAST response: %v", err)
			}
			if status != transport.GetFastFound {
				t.Fatalf("GET_FAST app.events/k01 status = 0x%02X; want found (0x%02X)", status, transport.GetFastFound)
			}
			var document struct {
				N int32 `bson:"n"`
			}
			if err := bson.Unmarshal(documentBytes, &document); err != nil {
				t.Fatalf("decoding the document GET_FAST returned: %v", err)
			}
			if document.N != 1 {
				t.Fatalf("GET_FAST app.events/k01 returned n=%d; want 1", document.N)
			}

			missed, err := sendGetFast(conn, mustGetFastPayload(t, "app", "events", "no-such-key"))
			if err != nil {
				t.Fatalf("GET_FAST for a missing key: %v", err)
			}
			if missed.Opcode != transport.OpGetFastResponse {
				t.Fatalf("GET_FAST for a missing key answered %s; want %s", answerText(missed), transport.OpGetFastResponse)
			}
			missStatus, _, err := transport.UnmarshalGetFastResponse(missed.Payload)
			if err != nil {
				t.Fatalf("decoding the GET_FAST miss: %v", err)
			}
			if missStatus != transport.GetFastNotFound {
				t.Fatalf("GET_FAST for a missing key status = 0x%02X; want not-found (0x%02X)", missStatus, transport.GetFastNotFound)
			}
		})
	}
}

// TestGetFastOverTheListenerRefusesADatabaseOutsideTheGrant is what the
// undecodable-payload refusal closed, held for GET_FAST: a user granted one database is refused another,
// as a permission denial that names the grant to ask for, and the document
// does not come back. The session survives the refusal.
func TestGetFastOverTheListenerRefusesADatabaseOutsideTheGrant(t *testing.T) {
	addr := newGetFastAuthzServer(t)

	for _, username := range []string{"reader-app", "writer-app", "db-manager-app"} {
		t.Run(username, func(t *testing.T) {
			conn := mustDialQueryConn(t, addr, username, getFastTestPassword)

			refused, err := sendGetFast(conn, mustGetFastPayload(t, "secret", "vault", "k01"))
			if err != nil {
				t.Fatalf("GET_FAST secret.vault/k01: %v", err)
			}
			message := refusalMessage(t, refused)
			for _, want := range []string{"permission denied", `lacks "reader" on "secret"`, "grant role reader@secret to " + username} {
				if !strings.Contains(message, want) {
					t.Errorf("the refusal %q does not contain %q: a user scoped to \"app\" must be refused \"secret\" as unauthorized, with the grant to ask for", message, want)
				}
			}

			allowed, err := sendGetFast(conn, mustGetFastPayload(t, "app", "events", "k01"))
			if err != nil {
				t.Fatalf("GET_FAST app.events/k01 after the refusal: %v", err)
			}
			if allowed.Opcode != transport.OpGetFastResponse {
				t.Fatalf("after the refusal, GET_FAST on the granted database answered %s; want %s", answerText(allowed), transport.OpGetFastResponse)
			}
		})
	}
}

// TestMalformedGetFastIsRefusedAtTheGate sends payloads the gate cannot scope
// as GET_FAST. Each must be answered by the gate's refusal, on a stream that
// stays open, with a message that says how GET_FAST is laid out.
//
// The BSON-shaped case is the one that matters for isolation. It names a
// database the user is granted, in the encoding every other opcode uses, so a
// gate reading GET_FAST as BSON authorizes it and hands the handler a payload
// the handler reads by a different layout. Gate and handler must read the
// same bytes the same way (the root cause of the refusal).
func TestMalformedGetFastIsRefusedAtTheGate(t *testing.T) {
	addr := newGetFastAuthzServer(t)

	bsonShaped := mustMarshalBSON(t, FindRequest{Database: "app", Bucket: "events", Key: "k01"})
	const layoutAdvice = "1-byte database length"
	for _, tt := range []struct {
		name    string
		payload []byte
		want    string
	}{
		{"truncated database name", []byte{200, 'a', 'p', 'p'}, layoutAdvice},
		{"truncated key", mustGetFastPayload(t, "app", "events", "k01")[:13], layoutAdvice},
		{"empty payload", nil, layoutAdvice},
		{"BSON naming a granted database", bsonShaped, layoutAdvice},
		{"no database", mustGetFastPayload(t, "", "events", "k01"), "does not name a database"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn := mustDialQueryConn(t, addr, "reader-app", getFastTestPassword)

			refused, err := sendGetFast(conn, tt.payload)
			if err != nil {
				t.Fatalf("the malformed GET_FAST got no answer (%v): the gate must refuse it with an error frame, not pass it to a handler that resets the stream", err)
			}
			message := refusalMessage(t, refused)
			if !strings.Contains(message, "refusing") || !strings.Contains(message, "GET_FAST") {
				t.Errorf("the refusal %q is not the gate's: it must name GET_FAST and say it is refusing", message)
			}
			if !strings.Contains(message, tt.want) {
				t.Errorf("the refusal %q does not contain %q", message, tt.want)
			}
			if strings.Contains(message, "write_concern") {
				t.Errorf("the refusal %q tells a GET_FAST client to send BSON string fields, which GET_FAST does not have", message)
			}

			allowed, err := sendGetFast(conn, mustGetFastPayload(t, "app", "events", "k01"))
			if err != nil {
				t.Fatalf("GET_FAST after the refusal: %v", err)
			}
			if allowed.Opcode != transport.OpGetFastResponse {
				t.Fatalf("after the refusal, a well-formed GET_FAST answered %s; want %s", answerText(allowed), transport.OpGetFastResponse)
			}
		})
	}
}

// TestGateScopesGetFastByItsWireLayout pins the gate's own decision, without a
// listener: the database a GET_FAST frame is checked against is the one its
// compact layout names, and a payload that layout cannot read is refused.
func TestGateScopesGetFastByItsWireLayout(t *testing.T) {
	s := newTestServer(t)

	sess := &transport.Session{}
	sess.SetIdentity(transport.Identity{
		Username: "alice", Roles: []string{"reader"}, Databases: []string{"app"},
		Grants: []transport.ScopedGrant{{Role: "reader", DB: "app"}},
	})
	ctx := transport.WithSession(context.Background(), sess)

	for _, tt := range []struct {
		name       string
		payload    []byte
		authorized bool
	}{
		{"the granted database", mustGetFastPayload(t, "app", "events", "k01"), true},
		{"another database", mustGetFastPayload(t, "secret", "vault", "k01"), false},
		{"BSON naming the granted database", mustMarshalBSON(t, FindRequest{Database: "app", Bucket: "events", Key: "k01"}), false},
		{"truncated", []byte{200, 'a', 'p', 'p'}, false},
		{"empty payload", nil, false},
		{"no database", mustGetFastPayload(t, "", "events", "k01"), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := s.authorize(ctx, transport.NewFrame(transport.OpGetFast, tt.payload))
			if tt.authorized && err != nil {
				t.Errorf("alice (reader@app) was refused a GET_FAST on her own database: %v", err)
			}
			if !tt.authorized && err == nil {
				t.Error("alice (reader@app) was authorized for a GET_FAST the gate could not scope to \"app\"")
			}
		})
	}
}
