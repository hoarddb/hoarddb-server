package main

import (
	"context"
	"fmt"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// userCases are the live vectors of what a user change does to a token minted
// before it (handbook auth.md section 8). The user commands themselves are
// RING_ADMIN's and are vectored in the ring group (ring_users_cases.go); the
// changes here are made through the server's own API.
func userCases() []func(db string) exchangeCase {
	// The legacy user opcodes' seventeen exchanges are retired slots (retiredCase), so the cases
	// after them keep their numbers: drivers manage users through RING_ADMIN (ring_users_cases.go).
	cases := retiredCases(17)
	return append(cases,
		tokenAfterChange("auth with a token minted before a password change", "pw-changed",
			"The setup authenticates as the account and keeps the token; the account's password is then changed (an alter with credentials), and the token is presented. "+
				"The counter the change bumped makes the token invalid: TOKEN_INVALID, never CREDENTIALS, so a driver falls back to the password once.",
			[]string{"reader"}, changePassword, transport.OpAuthFail),
		tokenAfterChange("auth with a token minted before a grant", "role-granted",
			"The account gets one more grant after the token was minted (USER_GRANT). Each grant or revoke that changes something bumps the counter, so the token is TOKEN_INVALID.",
			[]string{"reader"}, grantWriter, transport.OpAuthFail),
		tokenAfterChange("auth with a token after a roles-only alter", "role-replaced",
			"An alter that replaces only roles does not bump the counter, so the token is still accepted. The refreshed token carries only the roles both the record and the presented token hold.",
			[]string{"reader", "writer"}, replaceWithReader, transport.OpAuthOK),
		func(db string) exchangeCase {
			return exchangeCase{
				name: "write after a roles-only alter narrowed the refresh", group: "users", storeType: "hash", noReplay: true, connection: connFresh,
				notes: "The account holds reader and writer on this vector's database. Its setup authenticates, then a roles-only alter leaves it reader only, " +
					"then the setup refreshes with its token: the refresh narrows to the roles both hold, so the write is refused PERMISSION_DENIED. The account, " +
					"the database and the bucket are made by root before the setup and are not part of it.",
				prepare: prepareAccount("role-narrowed", []string{"reader", "writer"}, true),
				setup: []call{
					{op: transport.OpAuth, payload: userAuth("role-narrowed", userPassword)},
					{hook: replaceWithReader("role-narrowed")},
					{op: transport.OpAuth, payload: tokenAuth{username: "role-narrowed"}},
				},
				request:    put(db, "users", "ada", ada()),
				wantOpcode: transport.OpError, wantCode: transport.ErrPermissionDenied, wantField: notApplied(),
				wantMessage: []string{`permission denied: user "role-narrowed"`},
			}
		},
	)
}

// userAuth is a password AUTH for an account made by the case.
func userAuth(username, password string) server.AuthRequest {
	return server.AuthRequest{Username: username, Password: password, ProtocolVersion: 1, Capabilities: []string{}, Client: "hoarddb-vectors/1"}
}

// accountChange is a step on the account through the server's own API.
type accountChange func(username string) func(ctx context.Context, g *generation, db string) error

func changePassword(username string) func(ctx context.Context, g *generation, db string) error {
	return func(ctx context.Context, g *generation, db string) error {
		service, err := g.service("single")
		if err != nil {
			return err
		}
		material, err := server.NewCredential("a-different-vectors-password", server.Argon2Params{})
		if err != nil {
			return fmt.Errorf("deriving the new credential: %w", err)
		}
		_, err = service.Auth().AlterUser(server.AlterSpec{Username: username, Version: service.Auth().Version(), Credential: &material})
		if err != nil {
			return fmt.Errorf("changing the password of %q: %w", username, err)
		}
		return nil
	}
}

func grantWriter(username string) func(ctx context.Context, g *generation, db string) error {
	return func(ctx context.Context, g *generation, db string) error {
		service, err := g.service("single")
		if err != nil {
			return err
		}
		if _, err := service.Auth().GrantRole(username, server.Grant{Role: "writer", DB: db}); err != nil {
			return fmt.Errorf("granting writer to %q: %w", username, err)
		}
		return nil
	}
}

func replaceWithReader(username string) func(ctx context.Context, g *generation, db string) error {
	return func(ctx context.Context, g *generation, db string) error {
		service, err := g.service("single")
		if err != nil {
			return err
		}
		grants := []server.Grant{{Role: "reader", DB: db}}
		if _, err := service.Auth().AlterUser(server.AlterSpec{Username: username, Version: service.Auth().Version(), Grants: &grants}); err != nil {
			return fmt.Errorf("replacing the roles of %q: %w", username, err)
		}
		return nil
	}
}

// prepareAccount creates the account (and, when withBucket, the database and
// the hash bucket users) through root before the connection is made. roles are
// granted on the vector's own database.
func prepareAccount(username string, roles []string, withBucket bool) func(ctx context.Context, g *generation, db string) error {
	return func(ctx context.Context, g *generation, db string) error {
		if withBucket {
			if err := g.requestsOn(ctx, "single", bucketSetup(db, "users", server.BucketTypeHash)); err != nil {
				return err
			}
		}
		service, err := g.service("single")
		if err != nil {
			return err
		}
		material, err := server.NewCredential(userPassword, server.Argon2Params{})
		if err != nil {
			return fmt.Errorf("deriving the credential of %q: %w", username, err)
		}
		grants := make([]server.Grant, 0, len(roles))
		for _, role := range roles {
			grants = append(grants, server.Grant{Role: role, DB: db})
		}
		if err := service.Auth().CreateUser(username, material, grants, true); err != nil {
			return fmt.Errorf("creating %q: %w", username, err)
		}
		return nil
	}
}

// tokenAfterChange is a vector whose request presents a token the setup minted
// before the account was changed. The account and the change are made through
// the server's own API, so the vector is not replayed: it is a decode vector
// for the answer, and leg 8 of the harness exercises the behaviour live.
func tokenAfterChange(name, username, notes string, roles []string, change accountChange, answer transport.Opcode) func(db string) exchangeCase {
	return func(db string) exchangeCase {
		c := exchangeCase{
			name: name, group: "users", noReplay: true, connection: connFresh,
			notes: notes + " The account is made by root before the setup and the change is made between the setup and the request; " +
				"neither is part of the setup, and the token is recorded normalised.",
			prepare: prepareAccount(username, roles, false),
			setup:   []call{{op: transport.OpAuth, payload: userAuth(username, userPassword)}, {hook: change(username)}},
			request: call{op: transport.OpAuth, payload: tokenAuth{username: username}}, useSetupToken: true,
			wantOpcode: answer,
		}
		if answer == transport.OpAuthFail {
			c.wantReason = transport.AuthFailTokenInvalid
			c.wantMessage = []string{"authenticate again with a password"}
		} else {
			c.vary = []string{"capabilities"}
		}
		return c
	}
}

// deadAfterCases are the vectors of the optional after of DEAD_REQUEUE and
// DEAD_DISCARD (BUG-0637).
func deadAfterCases() []func(db string) exchangeCase {
	// twoDead pushes two entries and claims and releases them five times, the
	// fifth return of each moves it to the dead list.
	twoDead := func(db string) []call {
		steps := queueSetup(db, "q", server.BucketTypeFIFO, transport.OpFIFOPush, 2)
		for range 5 {
			steps = append(steps, claimCall(db, "q", 2, 0), leaseCall(transport.OpQueueRelease, db, "q", setupLeaseMarker))
		}
		return steps
	}
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "requeue all after a dead id", group: "write", storeType: "fifo",
				notes: "after is the dead_id a request starts past (exclusive, as dead() pages): an all request that started at the head and was refused " +
					"steps past the refused entries with it. The bucket holds two dead entries; after names the first, so the second moves and the first is left.",
				setup:      twoDead(db),
				request:    call{op: transport.OpDeadRequeue, payload: server.DeadMoveRequest{Database: db, Bucket: "q", All: true, After: firstDeadID}},
				wantOpcode: transport.OpDeadRequeue, wantField: map[string]any{"success": true, "done": int32(1), "left": int64(1)},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "discard all after a dead id", group: "write", storeType: "fifo",
				notes:      "Discard shares the cursor: the second dead entry is dropped and the first is left.",
				setup:      twoDead(db),
				request:    call{op: transport.OpDeadDiscard, payload: server.DeadMoveRequest{Database: db, Bucket: "q", All: true, After: firstDeadID}},
				wantOpcode: transport.OpDeadDiscard, wantField: map[string]any{"success": true, "done": int32(1), "left": int64(1)},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "requeue of one dead id with after", group: "errors", storeType: "fifo",
				notes:      "after pages an all request. Sent with a dead_id it is refused and nothing is moved.",
				setup:      bucketSetup(db, "q", server.BucketTypeFIFO),
				request:    call{op: transport.OpDeadRequeue, payload: server.DeadMoveRequest{Database: db, Bucket: "q", DeadID: firstDeadID, After: firstDeadID}},
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantField: notApplied(),
				wantMessage: []string{"requeue's after pages an all request"},
			}
		},
	}
}

// firstDeadID is the dead_id of the first entry that died in a fresh fifo.
const firstDeadID = "0000000000000000"
