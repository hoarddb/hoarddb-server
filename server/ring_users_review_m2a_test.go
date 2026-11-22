package server

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// Review fixes of M2a (2026-11-20): F1, a token of a dropped account must not resume on an account
// re-created under the same name; F2, the binding's fences and grantor and the login's kid check
// each held by a test.

// TestATokenOfADroppedAccountDoesNotResumeOnItsRecreation (review F1, spec 9 F17, B17): a token
// minted for ada at created_seq 4 is refused once ada was dropped and created again (created_seq
// 6, epoch 0 again), so a token-resumed session never names the new account to the cluster.
// Break: compare only subject and epoch; red.
func TestATokenOfADroppedAccountDoesNotResumeOnItsRecreation(t *testing.T) {
	am, table, follower, key, kid, _ := followedManager(t)
	table.set(cluster.UserView{Users: []cluster.UserRow{tableRow("ada", "ada-password-1", key, kid, 4, 4)}, PepperKID: kid, StateSeq: 4})
	awaitInstalled(t, follower, 4)
	claims, err := am.ClaimsFor("ada")
	if err != nil {
		t.Fatal(err)
	}
	token, err := am.GenerateToken(claims)
	if err != nil {
		t.Fatal(err)
	}
	if presented, err := am.ValidateToken(token); err != nil || am.VerifyClaims(presented) != nil {
		t.Fatalf("the token does not resume on its own account: %v", err)
	}
	table.set(cluster.UserView{Users: []cluster.UserRow{tableRow("ada", "ada-password-1", key, kid, 6, 6)}, PepperKID: kid, DropSeq: 5, StateSeq: 6})
	awaitInstalled(t, follower, 6)
	presented, err := am.ValidateToken(token)
	if err != nil {
		t.Fatal(err)
	}
	err = am.VerifyClaims(presented)
	if !errors.Is(err, ErrTokenInvalid) || !strings.Contains(err.Error(), "dropped and created again") {
		t.Fatalf("the old account's token on the re-created account answered %v, want refused naming the re-creation", err)
	}
}

// TestAZeroConfigTokenCarriesNoIncarnation (review F1): a node without a cluster mints the claims
// it minted before (no incarnation member), and its tokens resume as before.
func TestAZeroConfigTokenCarriesNoIncarnation(t *testing.T) {
	am := pepperManager(t, nil, "")
	if err := am.SeedRootUser("root", "root-password-1"); err != nil {
		t.Fatal(err)
	}
	claims, err := am.ClaimsFor("root")
	if err != nil {
		t.Fatal(err)
	}
	token, err := am.GenerateToken(claims)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(mustB64URL(t, strings.Split(token, ".")[0])), `"inc"`) {
		t.Fatalf("a zero-config token carries an incarnation: %s", token)
	}
	presented, err := am.ValidateToken(token)
	if err != nil || am.VerifyClaims(presented) != nil {
		t.Fatalf("a zero-config token does not resume: %v", err)
	}
}

// recordingNode is a scripted node whose proposals are kept, answered applied.
func recordingBinding(t *testing.T) (*ringUsersReplicated, *fakeUserNode, context.Context, *[][]byte) {
	t.Helper()
	var bodies [][]byte
	binding, node, ctx := scriptedBinding(t, func(_ context.Context, _ byte, body []byte) (cluster.Result, error) {
		bodies = append(bodies, body)
		return cluster.Result{StateSeq: 9}, nil
	})
	return binding, node, ctx, &bodies
}

func bodyInt64(t *testing.T, body []byte, key string) int64 {
	t.Helper()
	v, ok := bsoncore.Document(body).Lookup(key).Int64OK()
	if !ok {
		t.Fatalf("the body has no int64 %q", key)
	}
	return v
}

// TestTheGrantorIsTheSessionsAccountNotTheTables (review F2 C, B17): by_seq is the incarnation
// the session logged in as, even when the table now holds another account of that name, so the
// applier refuses a session of a dropped account. Break: take by_seq from the table's row; red.
func TestTheGrantorIsTheSessionsAccountNotTheTables(t *testing.T) {
	binding, node, ctx, bodies := recordingBinding(t)
	for i := range node.view.Users {
		if node.view.Users[i].Name == "root" {
			node.view.Users[i].CreatedSeq, node.view.Users[i].ChangedSeq = 7, 7
		}
	}
	if _, err := binding.DropUser(ctx, [16]byte{8}, dropAda()); err != nil {
		t.Fatal(err)
	}
	if got := bodyInt64(t, (*bodies)[0], "by_seq"); got != 2 {
		t.Fatalf("by_seq is %d, want the session's incarnation 2 (the table's root is 7)", got)
	}
}

// TestEveryRowChangeCarriesTheClientsExpectSeq (review F2 B): a client's expect_seq, not the
// table's changed_seq, is the fence alter, drop, grant and revoke carry. Break: always use the
// table's row; red.
func TestEveryRowChangeCarriesTheClientsExpectSeq(t *testing.T) {
	binding, _, ctx, bodies := recordingBinding(t)
	stale := ptr(int64(1)) // ada's row is at changed_seq 3
	calls := map[string]func() error{
		"alter": func() error {
			_, err := binding.AlterUser(ctx, [16]byte{9}, wire.RingUserRequest{Name: "ada", ExpectSeq: stale, Enabled: ptr(false)})
			return err
		},
		"drop": func() error {
			_, err := binding.DropUser(ctx, [16]byte{10}, wire.RingUserRequest{Name: "ada", ExpectSeq: stale})
			return err
		},
		"grant": func() error {
			_, err := binding.GrantRole(ctx, [16]byte{11}, wire.RingUserRequest{Name: "ada", ExpectSeq: stale, Grant: wire.RingUserGrant{Role: "writer", DB: "app"}})
			return err
		},
		"revoke": func() error {
			_, err := binding.RevokeRole(ctx, [16]byte{12}, wire.RingUserRequest{Name: "ada", ExpectSeq: stale, Grant: wire.RingUserGrant{Role: "reader", DB: "*"}})
			return err
		},
	}
	for what, call := range calls {
		*bodies = nil
		if err := call(); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		if got := bodyInt64(t, (*bodies)[0], "expect_seq"); got != 1 {
			t.Errorf("%s carried expect_seq %d, want the client's 1", what, got)
		}
	}
}

// TestAStaleClientRevisionIsRefusedForEveryRowChange (review F2 B, end to end): each of alter,
// drop, grant and revoke sent with a revision older than the row's is refused CONFLICT with the
// re-read text, and the row is unchanged. Break: always use the table's row; red.
func TestAStaleClientRevisionIsRefusedForEveryRowChange(t *testing.T) {
	qs, root := clusterUsersServer(t)
	created := u1Do(t, root, u1Create(t, "ada", "ada-password-1", u1DropSeq(t, root)))
	moved := u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "ada", Grant: wire.RingUserGrant{Role: "reader", DB: "app"}}})
	stale := ptr(created.Users.Revision)
	for _, req := range []wire.RingAdminRequest{
		{Cmd: wire.AdminAlterUser, User: &wire.RingUserRequest{Name: "ada", ExpectSeq: stale, Enabled: ptr(false)}},
		{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: "ada", ExpectSeq: stale}},
		{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "ada", ExpectSeq: stale, Grant: wire.RingUserGrant{Role: "writer", DB: "app"}}},
		{Cmd: wire.AdminRevokeRole, User: &wire.RingUserRequest{Name: "ada", ExpectSeq: stale, Grant: wire.RingUserGrant{Role: "reader", DB: "app"}}},
	} {
		text := clusterRefusal(t, root, req, cluster.CodeConflict)
		if !strings.Contains(text, "the user changed since this node read it") || !strings.Contains(text, "'user describe ada'") {
			t.Errorf("%s at a stale revision answered %q", req.Cmd, text)
		}
		if row, ok := qs.ringNode.Users().User("ada"); !ok || row.ChangedSeq != moved.Users.Revision {
			t.Fatalf("%s at a stale revision changed the row: %+v", req.Cmd, row)
		}
	}
}

// TestARowPepperedUnderAnotherKeyIsRefusedNotVerified (review F2 D, spec 5.5): a cluster row whose
// pepper_kid is not the key this node holds is refused, even when its hash would verify under this
// node's key. Break: drop the kid comparison; red (the login succeeds).
func TestARowPepperedUnderAnotherKeyIsRefusedNotVerified(t *testing.T) {
	am := clusterMemberManager(t)
	pepper, key := testClusterPepper(t)
	am.SetClusterPasswordKey(fakeClusterKey{key: key, kid: pepper.KID()})
	am.mu.Lock()
	am.users["ada"] = clusterRecord("ada", "ada-password-1", key, "0bad0bad", 3)
	am.mu.Unlock()
	ok, err := am.VerifyUser("ada", "ada-password-1")
	if ok || !errors.Is(err, ErrPepperKeyMissing) {
		t.Fatalf("a row peppered under another kid answered %v, %v; want refused", ok, err)
	}
}

func mustB64URL(t *testing.T, text string) []byte {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(text)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
