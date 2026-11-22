package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"golang.org/x/crypto/argon2"

	"github.com/hoarddb/hoarddb-server/authrules"
	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/clusterpepper"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The replicated binding of the ring user commands (ring user store spec 6.1, 6.2, 4.3; steps
// M2a.3 and M2a.5): on a cluster member each change becomes a cluster command carrying the
// client's fences, a retry under the same request id is answered from the cluster's request list,
// and the texts say whether anything changed.

// clusterUsersServer is a one-node cluster member with a root account created through the cluster
// (the root task, armed as cmd/server arms it), and a session logged in as that root.
func clusterUsersServer(t *testing.T) (*Service, *tcpServerClient) {
	t.Helper()
	listen := ringClusterPorts(t)
	qs, addr := newTCPEnabledServer(t, func(cfg *config.Config) {
		cfg.Server.NodeID = "node-a"
		cfg.Server.Listen = listen
		cfg.Cluster.Enabled = true
		// The root task measures the root password as a first start does; the host's is short.
		cfg.Auth.PasswordMinLength = 0
	})
	root := dialTCPServer(t, addr)
	root.login()
	return qs, root
}

// clusterRefusal sends req and returns the refusal, failing unless it has code.
func clusterRefusal(t *testing.T, c *tcpServerClient, req wire.RingAdminRequest, code cluster.Code) string {
	t.Helper()
	return ringAdminRefusal(t, u1Frame(t, c, req), code).Message
}

func ptr[T any](v T) *T { return &v }

// TestRingUserCommandsChangeTheClusterTable: create, grant, revoke, alter and drop over the ring
// on a cluster member become cluster commands; a login on the node sees each change as soon as
// the command answers; the replies carry the row's changed_seq as its revision.
func TestRingUserCommandsChangeTheClusterTable(t *testing.T) {
	qs, root := clusterUsersServer(t)
	created := u1Do(t, root, u1Create(t, "ada", "ada-password-1", u1DropSeq(t, root)))
	row, ok := qs.ringNode.Users().User("ada")
	if !ok {
		t.Fatal("the create did not reach the cluster's table")
	}
	if created.Users.Revision != row.ChangedSeq || row.CreatedSeq != row.ChangedSeq {
		t.Fatalf("create answered revision %d, the row's changed_seq is %d", created.Users.Revision, row.ChangedSeq)
	}
	if !bytes.Equal(row.Hash, mustClusterHash(t, qs, row, "ada-password-1")) {
		t.Fatal("the stored hash is not the client hash peppered once with the cluster key")
	}
	u1Login(t, qs, "ada", "ada-password-1")

	granted := u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "ada", Grant: wire.RingUserGrant{Role: "writer", DB: "app"}}})
	described := u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminDescribeUser, User: &wire.RingUserRequest{Name: "ada"}})
	if described.Users.User == nil || described.Users.User.Revision != granted.Users.Revision || len(described.Users.User.Roles) != 1 {
		t.Fatalf("after the grant describe_user answered %+v (grant revision %d)", described.Users.User, granted.Users.Revision)
	}
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminRevokeRole, User: &wire.RingUserRequest{Name: "ada", ExpectSeq: ptr(granted.Users.Revision), Grant: wire.RingUserGrant{Role: "writer", DB: "app"}}})

	credential, err := NewRingCredential("ada-password-2")
	if err != nil {
		t.Fatal(err)
	}
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminAlterUser, User: &wire.RingUserRequest{Name: "ada", Credential: credential}})
	stale := dialTCPServer(t, qs.tcpListener.Addr().String())
	if frame, _, _ := stale.authCall(&AuthRequest{Username: "ada", Password: "ada-password-1"}); frame.Opcode == transport.OpAuthOK {
		t.Fatal("the old password still logs in after the alter answered")
	}
	u1Login(t, qs, "ada", "ada-password-2")

	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: "ada"}})
	if _, ok := qs.core.auth.UserInfo("ada"); ok {
		t.Fatal("the dropped account is still in this node's store when the drop answered")
	}
	clusterRefusal(t, root, wire.RingAdminRequest{Cmd: wire.AdminDescribeUser, User: &wire.RingUserRequest{Name: "ada"}}, cluster.CodeNotFound)
	if listed := u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminShowUsers}); listed.Users.DropSeq != qs.ringNode.Users().DropSeq || listed.Users.DropSeq == 0 {
		t.Fatalf("show_users answered drop fence %d, the cluster's user_drop_seq is %d", listed.Users.DropSeq, qs.ringNode.Users().DropSeq)
	}
}

// mustClusterHash is password's client hash under row's salt and work factor, peppered once with
// the node's cluster key.
func mustClusterHash(t *testing.T, qs *Service, row cluster.UserRow, password string) []byte {
	t.Helper()
	key, _, err := qs.ringNode.PasswordKey()
	if err != nil {
		t.Fatal(err)
	}
	clientHash := argon2.IDKey([]byte(password), row.Salt, uint32(row.Iterations), uint32(row.Memory), uint8(row.Parallelism), uint32(len(row.Hash)))
	return key.Hash(clientHash)
}

// TestAClientRetryWithTheSameRequestIDIsAnsweredAsSuccess (F18, B12): a change retried under its
// request id while the cluster remembers it is answered success and applied once, even with a new
// salt in the body. Break: drop the list check; red (the create is refused "exists").
func TestAClientRetryWithTheSameRequestIDIsAnsweredAsSuccess(t *testing.T) {
	qs, root := clusterUsersServer(t)
	fence := u1DropSeq(t, root)
	id := func(n uint64) [16]byte {
		var id [16]byte
		binary.BigEndian.PutUint64(id[:8], 0xF18)
		binary.BigEndian.PutUint64(id[8:], n)
		return id
	}
	steps := []wire.RingAdminRequest{
		u1Create(t, "ada", "ada-password-1", fence),
		{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "ada", Grant: wire.RingUserGrant{Role: "writer", DB: "app"}}},
		{Cmd: wire.AdminRevokeRole, User: &wire.RingUserRequest{Name: "ada", Grant: wire.RingUserGrant{Role: "writer", DB: "app"}}},
		{Cmd: wire.AdminAlterUser, User: &wire.RingUserRequest{Name: "ada", Enabled: ptr(false)}},
	}
	for i, req := range steps {
		first := u1SendWithID(t, root, id(uint64(i)), req)
		if first.Opcode != transport.OpRingReply {
			t.Fatalf("%s answered %s %q", req.Cmd, first.Opcode, first.Payload)
		}
		seq := qs.ringNode.Users().StateSeq
		retry := req
		if req.Cmd == wire.AdminCreateUser {
			retry = u1Create(t, "ada", "ada-password-1", fence) // a new salt under the old id
		}
		again := u1SendWithID(t, root, id(uint64(i)), retry)
		if again.Opcode != transport.OpRingReply {
			t.Fatalf("the retry of %s answered %s %q, want success", req.Cmd, again.Opcode, again.Payload)
		}
		if now := qs.ringNode.Users().StateSeq; now != seq {
			t.Fatalf("the retry of %s moved state_seq %d -> %d: it was applied again", req.Cmd, seq, now)
		}
	}
}

// TestADropOfAnAbsentNameAnswersNotFound (B9): a typo is never silent success.
func TestADropOfAnAbsentNameAnswersNotFound(t *testing.T) {
	_, root := clusterUsersServer(t)
	text := clusterRefusal(t, root, wire.RingAdminRequest{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: "adaa"}}, cluster.CodeNotFound)
	if !strings.Contains(text, `there is no user "adaa"`) || !strings.Contains(text, "'show users'") {
		t.Fatalf("drop of an absent name answered %q", text)
	}
}

// TestAClientRetryOfACreateCarriesItsFirstDropFence (R3): a create applied with its reply lost,
// the account dropped, 300 other user commands, then the client's retry under the same id with
// its first fence: refused, never applied. Break: let the binding fill expect_drop_seq from its
// table; red (ada is back).
func TestAClientRetryOfACreateCarriesItsFirstDropFence(t *testing.T) {
	qs, root := clusterUsersServer(t)
	fence := u1DropSeq(t, root)
	var r1 [16]byte
	copy(r1[:], "create-ada-r1---")
	create := u1Create(t, "ada", "ada-password-1", fence)
	if frame := u1SendWithID(t, root, r1, create); frame.Opcode != transport.OpRingReply {
		t.Fatalf("the first create answered %s %q", frame.Opcode, frame.Payload)
	}
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: "ada"}})
	u1Do(t, root, u1Create(t, "bob", "bob-password-1", u1DropSeq(t, root)))
	for i := range 150 {
		db := fmt.Sprintf("db%d", i)
		u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "bob", Grant: wire.RingUserGrant{Role: "reader", DB: db}}})
		u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminRevokeRole, User: &wire.RingUserRequest{Name: "bob", Grant: wire.RingUserGrant{Role: "reader", DB: db}}})
	}
	if _, remembered := qs.ringNode.Users().RequestApplied(r1); remembered {
		t.Fatal("the first create is still in the request list; the test needs it evicted")
	}
	text := ringAdminRefusal(t, u1SendWithID(t, root, r1, create), cluster.CodeConflict).Message
	if !strings.Contains(text, "dropped since") {
		t.Errorf("the retry was refused with %q, want the drop fence's text", text)
	}
	if _, back := qs.ringNode.Users().User("ada"); back {
		t.Fatal("a retried create brought the dropped account back")
	}
}

// TestACreateUserFrameWithoutTheFenceIsRefusedByTheBinding (R3): a create with no expect_drop_seq
// is refused naming the read that gives it, and nothing is proposed. Break: default the field to
// the table's value; red.
func TestACreateUserFrameWithoutTheFenceIsRefusedByTheBinding(t *testing.T) {
	qs, root := clusterUsersServer(t)
	before := qs.ringNode.Users().StateSeq
	credential, err := NewRingCredential("ada-password-1")
	if err != nil {
		t.Fatal(err)
	}
	cred := bson.D{{Key: "algo", Value: credential.Algo}, {Key: "salt", Value: credential.Salt}, {Key: "hash", Value: credential.Hash},
		{Key: "memory", Value: credential.Memory}, {Key: "iterations", Value: credential.Iterations}, {Key: "parallelism", Value: credential.Parallelism}}
	body, err := bson.Marshal(bson.D{{Key: "cmd", Value: wire.AdminCreateUser}, {Key: "name", Value: "ada"}, {Key: "roles", Value: bson.A{}}, {Key: "credential", Value: cred}})
	if err != nil {
		t.Fatal(err)
	}
	frame := root.ringSend(transport.OpRingAdmin, body)
	refusal := ringAdminRefusal(t, frame, cluster.CodeInvalidArgument)
	if !strings.Contains(refusal.Message, "expect_drop_seq") || !strings.Contains(refusal.Message, "show_users") {
		t.Errorf("the refusal does not name the fence and the read: %q", refusal.Message)
	}
	if now := qs.ringNode.Users().StateSeq; now != before {
		t.Fatalf("state_seq moved %d -> %d: a create without its fence was proposed", before, now)
	}
}

// TestAFenceRefusalAfterAnUndecidedCreateReportsItDone (R3-F2): a create whose request id the
// cluster remembers is answered done from the request list whatever the drop sequence says: the
// list is read before any fence, so a drop that committed after the create (moving user_drop_seq
// past the retry's fence) never turns the retry into a refusal or a second apply. Break: judge the
// fence before the list; red.
func TestAFenceRefusalAfterAnUndecidedCreateReportsItDone(t *testing.T) {
	qs, root := clusterUsersServer(t)
	fence := u1DropSeq(t, root)
	var r1 [16]byte
	copy(r1[:], "create-ada-r3f2-")
	create := u1Create(t, "ada", "ada-password-1", fence)
	if frame := u1SendWithID(t, root, r1, create); frame.Opcode != transport.OpRingReply {
		t.Fatalf("the first create answered %s %q", frame.Opcode, frame.Payload)
	}
	u1Do(t, root, u1Create(t, "bob", "bob-password-1", u1DropSeq(t, root)))
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: "bob"}})
	if qs.ringNode.Users().DropSeq == fence {
		t.Fatal("the drop did not move user_drop_seq")
	}
	again := u1SendWithID(t, root, r1, create)
	if again.Opcode != transport.OpRingReply {
		t.Fatalf("the retry of a remembered create answered %s %q, want done", again.Opcode, again.Payload)
	}
}

// TestShowUsersAndDescribeUserNeverReturnASaltHashOrPepperKid (spec 4.3): no reply of the
// replicated binding carries credential material.
func TestShowUsersAndDescribeUserNeverReturnASaltHashOrPepperKid(t *testing.T) {
	qs, root := clusterUsersServer(t)
	u1Do(t, root, u1Create(t, "ada", "ada-password-1", u1DropSeq(t, root)))
	row, _ := qs.ringNode.Users().User("ada")
	for _, req := range []wire.RingAdminRequest{{Cmd: wire.AdminShowUsers}, {Cmd: wire.AdminDescribeUser, User: &wire.RingUserRequest{Name: "ada"}}} {
		frame := u1Frame(t, root, req)
		for what, secret := range map[string][]byte{"salt": row.Salt, "hash": row.Hash, "pepper_kid": []byte(row.PepperKID)} {
			if bytes.Contains(frame.Payload, secret) {
				t.Errorf("the %s reply carries the account's %s", req.Cmd, what)
			}
		}
	}
}

// TestARoleGrantBeyondTheCapIsRefusedOnACluster (U1 carried item a): the cluster table never holds
// a row show_users could not carry: the 65th grant is refused with the zero-config text, and a
// grant already held is still a no-op success. Break: skip the count; red.
func TestARoleGrantBeyondTheCapIsRefusedOnACluster(t *testing.T) {
	qs, root := clusterUsersServer(t)
	u1Do(t, root, u1Create(t, "ada", "ada-password-1", u1DropSeq(t, root)))
	for i := range wire.MaxUserGrants {
		u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "ada", Grant: wire.RingUserGrant{Role: "reader", DB: fmt.Sprintf("db%d", i)}}})
	}
	text := clusterRefusal(t, root, wire.RingAdminRequest{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "ada", Grant: wire.RingUserGrant{Role: "reader", DB: "one-more"}}}, cluster.CodeInvalidArgument)
	if !strings.Contains(text, fmt.Sprintf("at most %d grants", wire.MaxUserGrants)) {
		t.Errorf("the 65th grant was refused with %q", text)
	}
	if row, _ := qs.ringNode.Users().User("ada"); len(row.Roles) != wire.MaxUserGrants {
		t.Fatalf("the row holds %d grants", len(row.Roles))
	}
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "ada", Grant: wire.RingUserGrant{Role: "reader", DB: "db0"}}})
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminShowUsers})
}

// fakeUserNode is the cluster as the binding sees it, scripted: its table, its key and what a
// proposal answers.
type fakeUserNode struct {
	mu        sync.Mutex
	view      cluster.UserView
	keyErr    error
	key       clusterpepper.Key
	kid       string
	propose   func(ctx context.Context, kind byte, body []byte) (cluster.Result, error)
	proposals int
	deadline  time.Time
}

func (f *fakeUserNode) Users() cluster.UserView {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.view
}

func (f *fakeUserNode) PasswordKey() (clusterpepper.Key, string, error) {
	return f.key, f.kid, f.keyErr
}

func (f *fakeUserNode) ProposeUserCommand(ctx context.Context, kind byte, body []byte) (cluster.Result, error) {
	f.mu.Lock()
	f.proposals++
	f.deadline, _ = ctx.Deadline()
	f.mu.Unlock()
	return f.propose(ctx, kind, body)
}

// noWait is an install waiter for a binding whose tests never succeed.
type noWait struct{}

func (noWait) awaitInstalled(context.Context, int64) error { return nil }

// scriptedBinding is a replicated binding over a fake node whose table holds root and ada, called
// as root.
func scriptedBinding(t *testing.T, propose func(context.Context, byte, []byte) (cluster.Result, error)) (*ringUsersReplicated, *fakeUserNode, context.Context) {
	t.Helper()
	pepper, key := testClusterPepper(t)
	rootRow := tableRow("root", "root-password-1", key, pepper.KID(), 2, 2)
	rootRow.Roles[0].Role = "root"
	node := &fakeUserNode{key: key, kid: pepper.KID(), propose: propose,
		view: cluster.UserView{Users: []cluster.UserRow{tableRow("ada", "ada-password-1", key, pepper.KID(), 3, 3), rootRow}, PepperKID: pepper.KID(), StateSeq: 3}}
	server := &Server{auth: clusterMemberManager(t), logger: discardLogger()}
	binding := newRingUsersReplicated(server, node, noWait{}, userProposalDeadline)
	sess := &transport.Session{}
	sess.SetIdentity(transport.Identity{Username: "root", Grants: []transport.ScopedGrant{{Role: "root", DB: "*"}}, Incarnation: 2})
	return binding, node, transport.WithSession(context.Background(), sess)
}

func dropAda() wire.RingUserRequest { return wire.RingUserRequest{Name: "ada"} }

// TestAUserChangeWithNoLeaderKnownAnswersNothingChanged (B3): a decided ending (no leader known)
// answers "was not applied ... nothing changed". Break: map every no-quorum ending to the
// undecided text; red.
func TestAUserChangeWithNoLeaderKnownAnswersNothingChanged(t *testing.T) {
	binding, _, ctx := scriptedBinding(t, func(context.Context, byte, []byte) (cluster.Result, error) {
		return cluster.Result{}, &cluster.Error{Code: cluster.CodeUnavailable, Message: "no Raft leader answered in time, so nothing was changed (no leader is elected yet)"}
	})
	_, err := binding.DropUser(ctx, [16]byte{1}, dropAda())
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeUnavailable {
		t.Fatalf("answered %v, want UNAVAILABLE", err)
	}
	want := `drop user "ada" was not applied: no leader of the cluster answered within 20 seconds, so nothing changed. Run 'nodes' to see which members are down, and run the statement again when a majority is up.`
	if refusal.Message != want {
		t.Fatalf("answered %q\nwant     %q", refusal.Message, want)
	}
}

// TestAUserChangeCutAfterTheSendAnswersMayHaveBeenApplied (B3): an ending after an attempt whose
// answer was lost answers "may or may not have been applied". Break: map undecided to the decided
// text; red.
func TestAUserChangeCutAfterTheSendAnswersMayHaveBeenApplied(t *testing.T) {
	binding, _, ctx := scriptedBinding(t, func(context.Context, byte, []byte) (cluster.Result, error) {
		return cluster.Result{}, &wire.UndecidedError{Err: errors.New("relaying the command to leader node-b: connection reset")}
	})
	_, err := binding.DropUser(ctx, [16]byte{2}, dropAda())
	want := `drop user "ada" may or may not have been applied: this node sent it, but no answer came back within 20 seconds. Run 'nodes' to see which members are down; when a majority is up, run 'user describe ada' to see whether the change was made before running the statement again.`
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeUnavailable || refusal.Message != want {
		t.Fatalf("answered %v\nwant UNAVAILABLE %q", err, want)
	}
}

// TestAFenceRefusalAfterAnUndecidedAttemptAnswersMayHaveBeenApplied (B5): a fence refusal that
// follows an undecided attempt is not "was not applied": the lost attempt may be what moved the
// row. Break: answer the fence's own text; red.
func TestAFenceRefusalAfterAnUndecidedAttemptAnswersMayHaveBeenApplied(t *testing.T) {
	binding, _, ctx := scriptedBinding(t, func(context.Context, byte, []byte) (cluster.Result, error) {
		return cluster.Result{}, &wire.UndecidedError{Err: &cluster.Error{Code: cluster.CodeConflict,
			Message: `drop user "ada" was not applied: the user changed since this node read it (revision 3, now 9)`}}
	})
	_, err := binding.DropUser(ctx, [16]byte{3}, dropAda())
	if err == nil || !strings.Contains(err.Error(), "may or may not have been applied") || strings.Contains(err.Error(), "was not applied") {
		t.Fatalf("a fence refusal after an undecided attempt answered %v", err)
	}
}

// TestTheUserProposalDeadlineIsBelowTheClientDefault (B4): the binding gives its proposal 20
// seconds, under the client's default request timeout, so a default client reads the server's
// text rather than its own deadline. Break: set the deadline to 30 s; red.
func TestTheUserProposalDeadlineIsBelowTheClientDefault(t *testing.T) {
	if userProposalDeadline >= ringclient.DefaultRequestTimeout {
		t.Fatalf("the user proposal deadline %v is not below the client's default %v", userProposalDeadline, ringclient.DefaultRequestTimeout)
	}
	binding, node, ctx := scriptedBinding(t, func(ctx context.Context, _ byte, _ []byte) (cluster.Result, error) {
		return cluster.Result{}, &cluster.Error{Code: cluster.CodeUnavailable, Message: "no Raft leader answered in time"}
	})
	began := time.Now()
	if _, err := binding.DropUser(ctx, [16]byte{4}, dropAda()); err == nil {
		t.Fatal("the scripted refusal was lost")
	}
	if node.deadline.IsZero() || node.deadline.After(began.Add(userProposalDeadline+time.Second)) {
		t.Fatalf("the proposal ran under deadline %v, want at most %v from the call", node.deadline, userProposalDeadline)
	}
}

// TestAUserChangeOnANodeWithoutThePasswordKeyIsRefusedBeforeProposing (spec 6.1 step 2, 6.2): a
// node that holds no cluster key answers the key's text and proposes nothing. Break: propose
// first; red.
func TestAUserChangeOnANodeWithoutThePasswordKeyIsRefusedBeforeProposing(t *testing.T) {
	binding, node, ctx := scriptedBinding(t, func(context.Context, byte, []byte) (cluster.Result, error) {
		return cluster.Result{StateSeq: 9}, nil
	})
	node.keyErr = errors.New("password logins are not available on this node yet: the fetching text")
	_, err := binding.DropUser(ctx, [16]byte{5}, dropAda())
	if err == nil || !strings.Contains(err.Error(), "the fetching text") {
		t.Fatalf("answered %v, want the key's text", err)
	}
	if node.proposals != 0 {
		t.Fatalf("%d proposals were sent by a node without the key", node.proposals)
	}
}

// gatedWait is an install waiter that holds every wait until released.
type gatedWait struct{ release chan struct{} }

func (g gatedWait) awaitInstalled(ctx context.Context, _ int64) error {
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TestAChangeAnswersOnlyOnceThisNodeInstalledIt (spec 6.1 step 5, F5): the binding answers after
// its own follower installed the commit, so a login on this node right after the answer sees the
// change. Break: answer when the proposal returns; red (the answer comes before the install).
func TestAChangeAnswersOnlyOnceThisNodeInstalledIt(t *testing.T) {
	binding, _, ctx := scriptedBinding(t, func(context.Context, byte, []byte) (cluster.Result, error) {
		return cluster.Result{StateSeq: 9}, nil
	})
	gate := gatedWait{release: make(chan struct{})}
	binding.installed = gate
	answered := make(chan error, 1)
	go func() {
		_, err := binding.DropUser(ctx, [16]byte{6}, dropAda())
		answered <- err
	}()
	select {
	case err := <-answered:
		t.Fatalf("the drop answered (%v) before this node installed its commit", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(gate.release)
	if err := <-answered; err != nil {
		t.Fatalf("after the install the drop answered %v", err)
	}
}

// TestALoginOnASmallNodeNeverRunsAboveTheWindow (F6, spec 4.2): no credential above the window
// every member accepts reaches the cluster's table, so a login on any member, a small one
// included, never derives above the window's top; and a node configured above it derives at the
// top. Break: skip the window check; red (the proposal is sent).
func TestALoginOnASmallNodeNeverRunsAboveTheWindow(t *testing.T) {
	binding, node, ctx := scriptedBinding(t, func(context.Context, byte, []byte) (cluster.Result, error) {
		return cluster.Result{StateSeq: 9}, nil
	})
	binding.server.auth.argon2.Memory = 4 * authrulesMaxMemory()
	credential, err := NewRingCredential("ada-password-1")
	if err != nil {
		t.Fatal(err)
	}
	credential.Memory = 2 * int64(authrulesMaxMemory())
	roles := []wire.RingUserGrant{}
	_, err = binding.CreateUser(ctx, [16]byte{7}, wire.RingUserRequest{Name: "bea", ExpectDropSeq: 0, Roles: &roles, Credential: credential})
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument || !strings.Contains(refusal.Message, "outside what every member of a cluster accepts") {
		t.Fatalf("a credential above the window answered %v", err)
	}
	if node.proposals != 0 {
		t.Fatal("a credential above the window was proposed")
	}
	if got := clusterWindowParams(binding.server.auth.argon2).Memory; got != authrulesMaxMemory() {
		t.Fatalf("a node configured above the window derives at %d KiB, want the window's top %d", got, authrulesMaxMemory())
	}
}

func authrulesMaxMemory() uint32 { return authrules.MaxArgon2MemoryKiB }

// TestEveryReplicatedUserRefusalExitsAsItsRetryRuleSays mirrors the zero-config table test on a
// cluster member: a refusal a retry never clears (a name that exists, the last administrator)
// exits 2; a stale fence, which a rerun reads again, is CONFLICT (exit 8). The user limit is the
// applier's (TestTheTableRefusesUser10001).
func TestEveryReplicatedUserRefusalExitsAsItsRetryRuleSays(t *testing.T) {
	qs, root := clusterUsersServer(t)
	fence := u1DropSeq(t, root)
	u1Do(t, root, u1Create(t, "ada", "ada-password-1", fence))
	read := u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminDescribeUser, User: &wire.RingUserRequest{Name: "ada"}}).Users.User.Revision
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "ada", Grant: wire.RingUserGrant{Role: "writer", DB: "app"}}})
	rootName := qs.core.config.Server.RootUser
	cases := []struct {
		name string
		req  wire.RingAdminRequest
		code cluster.Code
		says string
	}{
		{"exists", u1Create(t, "ada", "ada-password-2", u1DropSeq(t, root)), cluster.CodeUserExists,
			cluster.CodeUserExists.Text(cluster.CondUserExists, map[string]string{"user": "ada"})},
		{"last administrator", wire.RingAdminRequest{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: rootName}}, cluster.CodeInvalidArgument, "last enabled account"},
		{"stale revision", wire.RingAdminRequest{Cmd: wire.AdminAlterUser, User: &wire.RingUserRequest{Name: "ada", ExpectSeq: &read, Enabled: ptr(false)}}, cluster.CodeConflict, "changed"},
	}
	retryClears := map[cluster.Code]bool{cluster.CodeConflict: true}
	for _, c := range cases {
		message := clusterRefusal(t, root, c.req, c.code)
		if !strings.Contains(message, c.says) {
			t.Errorf("%s: %q does not say %q", c.name, message, c.says)
		}
		if want := map[bool]int{true: 8, false: 2}[retryClears[c.code]]; c.code.Exit() != want {
			t.Errorf("%s: %s exits %d, want %d", c.name, c.code, c.code.Exit(), want)
		}
	}
	// The drop fence: a user dropped since the read.
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: "ada"}})
	clusterRefusal(t, root, u1Create(t, "bob", "bob-password-1", fence), cluster.CodeConflict)
}
