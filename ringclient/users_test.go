package ringclient

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	"golang.org/x/crypto/argon2"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// usersFake answers every user command with a fixed reply and records the frames it was sent.
type usersFake struct {
	mu     sync.Mutex
	frames []*transport.Frame
	refuse *wire.RingError
}

func (f *usersFake) handle(conn *fakeConn, frame *transport.Frame) {
	f.mu.Lock()
	f.frames = append(f.frames, frame)
	refuse := f.refuse
	f.mu.Unlock()
	if refuse != nil {
		conn.refuse(frame, refuse.Code, refuse.Message)
		return
	}
	request, err := wire.DecodeRingAdmin(frame.Payload)
	if err != nil {
		conn.refuse(frame, cluster.CodeInvalidArgument, err.Error())
		return
	}
	row := wire.RingUserRow{Name: "ada", Roles: []wire.RingUserGrant{{Role: "reader", DB: "app"}}, Enabled: true, Epoch: 1, CreatedMs: 1_700_000_000_000, PasswordChangedMs: 1_700_000_000_000, Revision: 8}
	users := &wire.RingAdminUsers{Revision: 8, Result: "done"}
	switch request.Cmd {
	case wire.AdminShowUsers:
		users = &wire.RingAdminUsers{Revision: 8, DropSeq: 3, Users: []wire.RingUserRow{row}}
	case wire.AdminDescribeUser:
		users = &wire.RingAdminUsers{DropSeq: 3, User: &row}
	default:
		users.Name = request.User.Name
	}
	payload, err := wire.EncodeRingAdminReply(wire.RingAdminReply{Cmd: request.Cmd, Users: users})
	if err != nil {
		conn.refuse(frame, cluster.CodeInvalidArgument, err.Error())
		return
	}
	conn.reply(frame, payload)
}

func (f *usersFake) last(t *testing.T) (*transport.Frame, wire.RingAdminRequest) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	frame := f.frames[len(f.frames)-1]
	request, err := wire.DecodeRingAdmin(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	return frame, request
}

// TestCreateUserSendsADerivedCredentialNotThePassword: the frame a create sends holds an Argon2id
// credential that verifies against the password, the drop fence the caller gave and the request
// id it chose, and nowhere the password's bytes.
func TestCreateUserSendsADerivedCredentialNotThePassword(t *testing.T) {
	fake := &usersFake{}
	client := dialFake(t, startFakeServer(t, fake.handle))
	const password = "correct-horse-battery-staple"
	id := [16]byte{4, 2}
	result, err := client.CreateUser(testContext(t), "ada", password, []UserGrant{{Role: "reader", DB: "app"}}, 17, WithRequestID(id))
	if err != nil {
		t.Fatal(err)
	}
	if result.Revision != 8 || result.Result != "done" {
		t.Errorf("result %+v", result)
	}
	frame, request := fake.last(t)
	if frame.RequestID != id {
		t.Errorf("sent under id %x, want %x", frame.RequestID, id)
	}
	if bytes.Contains(frame.Payload, []byte(password)) {
		t.Fatal("the create frame carries the password")
	}
	user := request.User
	if user.ExpectDropSeq != 17 || len(*user.Roles) != 1 {
		t.Errorf("request %+v", user)
	}
	c := user.Credential
	if got := argon2.IDKey([]byte(password), c.Salt, uint32(c.Iterations), uint32(c.Memory), uint8(c.Parallelism), uint32(len(c.Hash))); !bytes.Equal(got, c.Hash) {
		t.Error("the credential does not verify against the password")
	}
	defaults := server.DefaultArgon2Params()
	if c.Memory != int64(defaults.Memory) || c.Iterations != int64(defaults.Iterations) || c.Parallelism != int32(defaults.Parallelism) || len(c.Salt) != defaults.SaltLen || len(c.Hash) != defaults.HashLen {
		t.Errorf("the client derives with %+v, the server's defaults are %+v", c, defaults)
	}
	if _, err := client.CreateUser(testContext(t), "bob", password, nil, 17); err != nil {
		t.Fatal(err)
	}
	_, again := fake.last(t)
	if bytes.Equal(again.User.Credential.Salt, c.Salt) {
		t.Error("two creates drew the same salt")
	}
	if _, err := client.CreateUser(testContext(t), "eve", "", nil, 0); err == nil {
		t.Error("an empty password was sent")
	}
}

// TestUserCallsCarryTheirFences: alter, drop, grant and revoke carry expect_seq when the caller
// gives one and leave it out when not; the reads return the drop fence and the revisions.
func TestUserCallsCarryTheirFences(t *testing.T) {
	fake := &usersFake{}
	client := dialFake(t, startFakeServer(t, fake.handle))
	ctx := testContext(t)
	seven := int64(7)
	off := false
	grant := UserGrant{Role: "writer", DB: "app"}
	newPassword := "a-new-password-1"
	steps := []struct {
		name string
		call func() error
		want *int64
	}{
		{"alter", func() error {
			_, err := client.AlterUser(ctx, "ada", UserChange{ExpectSeq: &seven, Enabled: &off, Password: &newPassword})
			return err
		}, &seven},
		{"drop", func() error { _, err := client.DropUser(ctx, "ada", &seven); return err }, &seven},
		{"grant", func() error { _, err := client.GrantRole(ctx, "ada", grant, nil); return err }, nil},
		{"revoke", func() error { _, err := client.RevokeRole(ctx, "ada", grant, &seven); return err }, &seven},
	}
	for _, step := range steps {
		if err := step.call(); err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
		frame, request := fake.last(t)
		got := request.User.ExpectSeq
		if (got == nil) != (step.want == nil) || got != nil && *got != *step.want {
			t.Errorf("%s: expect_seq %v, want %v", step.name, got, step.want)
		}
		if step.name == "alter" && (request.User.Credential == nil || *request.User.Enabled || bytes.Contains(frame.Payload, []byte(newPassword))) {
			t.Errorf("alter sent %+v", request.User)
		}
		if step.name != "alter" && request.User.Grant.Role == "" && step.name != "drop" {
			t.Errorf("%s sent no grant", step.name)
		}
	}
	list, err := client.ShowUsers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if list.DropSeq != 3 || list.Revision != 8 || len(list.Users) != 1 || list.Users[0].Revision != 8 || list.Users[0].Created.UnixMilli() != 1_700_000_000_000 {
		t.Errorf("show users %+v", list)
	}
	one, err := client.DescribeUser(ctx, "ada")
	if err != nil {
		t.Fatal(err)
	}
	if one.DropSeq != 3 || one.User.Name != "ada" || len(one.User.Roles) != 1 {
		t.Errorf("describe user %+v", one)
	}
}

// TestUserCallsReturnTheServersRefusalText: a refusal reaches the caller as an *Error with the
// server's code and text unchanged.
func TestUserCallsReturnTheServersRefusalText(t *testing.T) {
	text := `create user "ada" was not applied: a user was dropped since the users were read (drop at 9, read at 3). Run 'user describe ada' to see whether it exists, then read the users again and run the statement again.`
	fake := &usersFake{refuse: &wire.RingError{Code: cluster.CodeConflict, Message: text}}
	client := dialFake(t, startFakeServer(t, fake.handle))
	_, err := client.CreateUser(testContext(t), "ada", "ada-password-1", nil, 3)
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeConflict || refusal.Message != text {
		t.Fatalf("CreateUser returned %v", err)
	}
	if _, err := client.DescribeUser(testContext(t), "ada"); !errors.As(err, &refusal) || refusal.Message != text {
		t.Fatalf("DescribeUser returned %v", err)
	}
}
