package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// send writes one frame and returns the response as it came, an OpError
// included, so a test can assert a refusal.
func (c *tcpServerClient) send(op transport.Opcode, req interface{}) *transport.Frame {
	c.t.Helper()
	payload, err := bson.Marshal(req)
	if err != nil {
		c.t.Fatalf("marshal %s request: %v", op, err)
	}
	_ = c.conn.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := c.conn.Write(transport.NewFrame(op, payload).Encode()); err != nil {
		c.t.Fatalf("write %s: %v", op, err)
	}
	frame, err := transport.DecodeFrame(c.br)
	if err != nil {
		c.t.Fatalf("read response to %s: %v", op, err)
	}
	return frame
}

// userConfigVersion reads the user store's version, which an alter carries.
func (c *tcpServerClient) userConfigVersion() uint64 {
	c.t.Helper()
	var listing UserListResponse
	c.call(transport.OpUserList, &UserListRequest{}, &listing)
	return listing.Version
}

// TestUserManagerCannotExceedItsOwnRoles: an account administrator who is not
// root gives only roles it holds itself, on a database it holds them on, and
// changes only accounts whose roles it holds. Without that ceiling the role
// is root by another name (spec 8.3: user-manager cannot add nodes).
func TestUserManagerCannotExceedItsOwnRoles(t *testing.T) {
	_, addr := newTCPEnabledServer(t, nil)
	root := dialTCPServer(t, addr)
	root.login()
	root.call(transport.OpCreateDB, &CreateDatabaseRequest{Name: "app"}, nil)
	root.call(transport.OpCreateDB, &CreateDatabaseRequest{Name: "billing"}, nil)
	const password = "ceiling-password-123"
	for _, account := range []struct {
		name  string
		roles []UserRole
	}{
		{"um", []UserRole{{Role: "user-manager", DB: "*"}, {Role: "writer", DB: "app"}}},
		{"second-root", []UserRole{{Role: "root", DB: "*"}}},
		{"app-reader", []UserRole{{Role: "reader", DB: "app"}}},
	} {
		var created UserMutationResponse
		root.call(transport.OpUserCreate, userCreateFor(t, account.name, password, account.roles), &created)
	}

	um := dialTCPServer(t, addr)
	um.loginAs("um", password)
	newCredential := func() *UserCredential {
		material, err := NewCredential("a-replacement-password-123", DefaultArgon2Params())
		if err != nil {
			t.Fatalf("NewCredential: %v", err)
		}
		return &UserCredential{Algo: material.Algo, Salt: material.Salt, Hash: material.Hash, Params: material.Params}
	}
	disabled := false
	requestID := func(what string) string { return fmt.Sprintf("%s-%d", what, time.Now().UnixNano()) }

	refused := []struct {
		name string
		op   transport.Opcode
		req  func() interface{}
	}{
		{"grant root to itself", transport.OpUserGrant, func() interface{} {
			return &UserGrantRequest{RequestID: requestID("g1"), Username: "um", Role: UserRole{Role: "root", DB: "*"}}
		}},
		{"grant cluster-manager to itself", transport.OpUserGrant, func() interface{} {
			return &UserGrantRequest{RequestID: requestID("g2"), Username: "um", Role: UserRole{Role: "cluster-manager", DB: "*"}}
		}},
		{"grant a role on a database it holds none on", transport.OpUserGrant, func() interface{} {
			return &UserGrantRequest{RequestID: requestID("g3"), Username: "app-reader", Role: UserRole{Role: "reader", DB: "billing"}}
		}},
		{"grant a role above its own on its database", transport.OpUserGrant, func() interface{} {
			return &UserGrantRequest{RequestID: requestID("g4"), Username: "app-reader", Role: UserRole{Role: "db-manager", DB: "app"}}
		}},
		{"create a root account", transport.OpUserCreate, func() interface{} {
			return userCreateFor(t, "minted-root", password, []UserRole{{Role: "root", DB: "*"}})
		}},
		{"create a backup-operator", transport.OpUserCreate, func() interface{} {
			return userCreateFor(t, "minted-backup", password, []UserRole{{Role: "backup-operator", DB: "*"}})
		}},
		{"replace its own roles with root", transport.OpUserAlter, func() interface{} {
			return &UserAlterRequest{RequestID: requestID("a1"), Username: "um", Roles: &[]UserRole{{Role: "root", DB: "*"}}, Version: um.userConfigVersion()}
		}},
		{"set a root account's password", transport.OpUserAlter, func() interface{} {
			return &UserAlterRequest{RequestID: requestID("a2"), Username: "second-root", Credentials: newCredential(), Version: um.userConfigVersion()}
		}},
		{"disable a root account", transport.OpUserAlter, func() interface{} {
			return &UserAlterRequest{RequestID: requestID("a3"), Username: "second-root", Enabled: &disabled, Version: um.userConfigVersion()}
		}},
		{"revoke from a root account", transport.OpUserRevoke, func() interface{} {
			return &UserRevokeRequest{RequestID: requestID("r1"), Username: "second-root", Role: UserRole{Role: "root", DB: "*"}}
		}},
		{"drop a root account", transport.OpUserDrop, func() interface{} {
			return &UserDropRequest{RequestID: requestID("d1"), Username: "second-root"}
		}},
	}
	for _, tc := range refused {
		frame := um.send(tc.op, tc.req())
		if frame.Opcode != transport.OpError {
			t.Errorf("user-manager could %s: answered %s", tc.name, frame.Opcode)
			continue
		}
		var refusal ErrorResponse
		_ = bson.Unmarshal(frame.Payload, &refusal)
		if refusal.Code != int(transport.ErrPermissionDenied) {
			t.Errorf("%s: refused with code %d (%q), want PERMISSION_DENIED", tc.name, refusal.Code, refusal.Message)
		}
	}

	allowed := []struct {
		name string
		op   transport.Opcode
		req  func() interface{}
	}{
		{"create an account with a role it holds", transport.OpUserCreate, func() interface{} {
			return userCreateFor(t, "helper", password, []UserRole{{Role: "reader", DB: "app"}})
		}},
		{"grant a role it holds", transport.OpUserGrant, func() interface{} {
			return &UserGrantRequest{RequestID: requestID("g5"), Username: "app-reader", Role: UserRole{Role: "writer", DB: "app"}}
		}},
		{"set the password of an account within its roles", transport.OpUserAlter, func() interface{} {
			return &UserAlterRequest{RequestID: requestID("a4"), Username: "app-reader", Credentials: newCredential(), Version: um.userConfigVersion()}
		}},
		{"revoke a role it holds", transport.OpUserRevoke, func() interface{} {
			return &UserRevokeRequest{RequestID: requestID("r2"), Username: "app-reader", Role: UserRole{Role: "writer", DB: "app"}}
		}},
		{"drop an account within its roles", transport.OpUserDrop, func() interface{} {
			return &UserDropRequest{RequestID: requestID("d2"), Username: "helper"}
		}},
	}
	for _, tc := range allowed {
		if frame := um.send(tc.op, tc.req()); frame.Opcode == transport.OpError {
			var refusal ErrorResponse
			_ = bson.Unmarshal(frame.Payload, &refusal)
			t.Errorf("user-manager could not %s: %q", tc.name, refusal.Message)
		}
	}

	// root itself has no ceiling.
	var granted UserMutationResponse
	root.call(transport.OpUserGrant, &UserGrantRequest{RequestID: requestID("g6"), Username: "app-reader", Role: UserRole{Role: "cluster-manager", DB: "*"}}, &granted)
}
