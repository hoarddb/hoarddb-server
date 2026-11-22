package wire

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

func u1Credential() *RingUserCredential {
	return &RingUserCredential{Algo: cluster.UserAlgo, Salt: bytes.Repeat([]byte{7}, 16), Hash: bytes.Repeat([]byte{9}, 32),
		Memory: 65536, Iterations: 3, Parallelism: 4}
}

func u1Seq(n int64) *int64 { return &n }

func u1Requests() []RingAdminRequest {
	no := false
	roles := []RingUserGrant{{Role: "reader", DB: "app"}, {Role: "writer", DB: "*"}}
	empty := []RingUserGrant{}
	return []RingAdminRequest{
		{Cmd: AdminCreateUser, User: &RingUserRequest{Name: "ada", ExpectDropSeq: 0, Roles: &roles, Credential: u1Credential()}},
		{Cmd: AdminCreateUser, User: &RingUserRequest{Name: "ada", ExpectDropSeq: 41, Roles: &empty, Credential: u1Credential()}},
		{Cmd: AdminAlterUser, User: &RingUserRequest{Name: "ada", Credential: u1Credential()}},
		{Cmd: AdminAlterUser, User: &RingUserRequest{Name: "ada", ExpectSeq: u1Seq(44), Enabled: &no}},
		{Cmd: AdminAlterUser, User: &RingUserRequest{Name: "ada", ExpectSeq: u1Seq(0), Credential: u1Credential(), Enabled: &no, Roles: &roles}},
		{Cmd: AdminAlterUser, User: &RingUserRequest{Name: "ada", Roles: &empty}},
		{Cmd: AdminDropUser, User: &RingUserRequest{Name: "ada"}},
		{Cmd: AdminDropUser, User: &RingUserRequest{Name: "ada", ExpectSeq: u1Seq(7)}},
		{Cmd: AdminGrantRole, User: &RingUserRequest{Name: "ada", Grant: RingUserGrant{Role: "reader", DB: "app"}}},
		{Cmd: AdminRevokeRole, User: &RingUserRequest{Name: "ada", ExpectSeq: u1Seq(9), Grant: RingUserGrant{Role: "user-manager", DB: "*"}}},
		{Cmd: AdminShowUsers},
		{Cmd: AdminDescribeUser, User: &RingUserRequest{Name: "ada"}},
	}
}

func u1Row() RingUserRow {
	return RingUserRow{Name: "ada", Roles: []RingUserGrant{{Role: "reader", DB: "app"}}, Enabled: true, Epoch: 2,
		CreatedMs: 1_700_000_000_000, PasswordChangedMs: 1_700_000_000_000, Revision: 12}
}

func u1Replies() []RingAdminReply {
	var replies []RingAdminReply
	for _, cmd := range []string{AdminCreateUser, AdminAlterUser, AdminDropUser, AdminGrantRole, AdminRevokeRole} {
		replies = append(replies, RingAdminReply{Cmd: cmd, Users: &RingAdminUsers{Name: "ada", Revision: 12, Result: `user "ada" created`}})
	}
	row := u1Row()
	return append(replies,
		RingAdminReply{Cmd: AdminShowUsers, Users: &RingAdminUsers{Revision: 12, DropSeq: 3, Users: []RingUserRow{row, {Name: "zed", Roles: []RingUserGrant{}}}}},
		RingAdminReply{Cmd: AdminShowUsers, Users: &RingAdminUsers{Users: []RingUserRow{}}},
		RingAdminReply{Cmd: AdminDescribeUser, Users: &RingAdminUsers{DropSeq: 3, User: &row}},
	)
}

// TestRingAdminUserCommandsRoundTripStrictly: [GUARD each of the seven user commands, and each
// reply, encodes and decodes to the same value; an extra, missing, reordered or mistyped member
// is refused and named].
func TestRingAdminUserCommandsRoundTripStrictly(t *testing.T) {
	for _, req := range u1Requests() {
		raw, err := EncodeRingAdmin(req)
		if err != nil {
			t.Fatalf("%s %+v: %v", req.Cmd, req.User, err)
		}
		got, err := DecodeRingAdmin(raw)
		if err != nil || !reflect.DeepEqual(got, req) {
			t.Fatalf("round trip of %s %+v gave %+v, %v", req.Cmd, req.User, got.User, err)
		}
	}
	for _, reply := range u1Replies() {
		raw, err := EncodeRingAdminReply(reply)
		if err != nil {
			t.Fatalf("%s: %v", reply.Cmd, err)
		}
		got, err := DecodeRingAdminReply(raw)
		if err != nil || !reflect.DeepEqual(got, reply) {
			t.Fatalf("round trip of the %s reply gave %+v, %v", reply.Cmd, got.Users, err)
		}
	}
	cred := bson.D{{Key: "algo", Value: "argon2id"}, {Key: "salt", Value: bytes.Repeat([]byte{7}, 16)}, {Key: "hash", Value: bytes.Repeat([]byte{9}, 32)},
		{Key: "memory", Value: int64(65536)}, {Key: "iterations", Value: int64(3)}, {Key: "parallelism", Value: int32(4)}}
	roles := bson.A{bson.D{{Key: "role", Value: "reader"}, {Key: "db", Value: "app"}}}
	cases := []struct {
		name string
		doc  bson.D
		want []string
	}{
		{"create without the drop fence", bson.D{{Key: "cmd", Value: AdminCreateUser}, {Key: "name", Value: "ada"}, {Key: "roles", Value: roles}, {Key: "credential", Value: cred}},
			[]string{"expect_drop_seq", "show_users"}},
		{"create without a credential", bson.D{{Key: "cmd", Value: AdminCreateUser}, {Key: "name", Value: "ada"}, {Key: "expect_drop_seq", Value: int64(0)}, {Key: "roles", Value: roles}},
			[]string{"create_user", "credential"}},
		{"create with a password", bson.D{{Key: "cmd", Value: AdminCreateUser}, {Key: "name", Value: "ada"}, {Key: "expect_drop_seq", Value: int64(0)}, {Key: "roles", Value: roles}, {Key: "credential", Value: cred}, {Key: "password", Value: "secret"}},
			[]string{"password", "Remove password"}},
		{"create reordered", bson.D{{Key: "cmd", Value: AdminCreateUser}, {Key: "expect_drop_seq", Value: int64(0)}, {Key: "name", Value: "ada"}, {Key: "roles", Value: roles}, {Key: "credential", Value: cred}},
			[]string{"layout"}},
		{"create fence as int32", bson.D{{Key: "cmd", Value: AdminCreateUser}, {Key: "name", Value: "ada"}, {Key: "expect_drop_seq", Value: int32(0)}, {Key: "roles", Value: roles}, {Key: "credential", Value: cred}},
			[]string{"layout"}},
		{"alter that changes nothing", bson.D{{Key: "cmd", Value: AdminAlterUser}, {Key: "name", Value: "ada"}},
			[]string{"alter_user", "credential, enabled or roles"}},
		{"alter fence as int32", bson.D{{Key: "cmd", Value: AdminAlterUser}, {Key: "name", Value: "ada"}, {Key: "expect_seq", Value: int32(4)}, {Key: "enabled", Value: true}},
			[]string{"layout"}},
		{"drop with a role", bson.D{{Key: "cmd", Value: AdminDropUser}, {Key: "name", Value: "ada"}, {Key: "role", Value: "reader"}},
			[]string{"drop_user", "role"}},
		{"grant without db", bson.D{{Key: "cmd", Value: AdminGrantRole}, {Key: "name", Value: "ada"}, {Key: "role", Value: "reader"}},
			[]string{"grant_role", "db"}},
		{"show_users with a name", bson.D{{Key: "cmd", Value: AdminShowUsers}, {Key: "name", Value: "ada"}},
			[]string{"show_users", "name"}},
		{"describe without a name", bson.D{{Key: "cmd", Value: AdminDescribeUser}},
			[]string{"describe_user", "name"}},
	}
	for _, c := range cases {
		raw, err := bson.Marshal(c.doc)
		if err != nil {
			t.Fatal(err)
		}
		_, err = DecodeRingAdmin(raw)
		if err == nil {
			t.Errorf("%s: accepted", c.name)
			continue
		}
		for _, want := range c.want {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s: refusal %q does not name %q", c.name, err, want)
			}
		}
	}
}

// TestRingUserRequestRefusals: [GUARD every member is bounded before anything is sent].
func TestRingUserRequestRefusals(t *testing.T) {
	bad := func(edit func(*RingUserCredential)) *RingUserCredential { c := u1Credential(); edit(c); return c }
	roles := []RingUserGrant{{Role: "reader", DB: "app"}}
	cases := []struct {
		name string
		req  RingAdminRequest
		want []string
	}{
		{"empty name", RingAdminRequest{Cmd: AdminDropUser, User: &RingUserRequest{}}, []string{"drop_user", "name"}},
		{"control in name", RingAdminRequest{Cmd: AdminDescribeUser, User: &RingUserRequest{Name: "a\x00b"}}, []string{"name", "control character"}},
		{"no user members", RingAdminRequest{Cmd: AdminDropUser}, []string{"drop_user", "name"}},
		{"another algorithm", RingAdminRequest{Cmd: AdminCreateUser, User: &RingUserRequest{Name: "ada", Roles: &roles, Credential: bad(func(c *RingUserCredential) { c.Algo = "plain" })}}, []string{"algo", "argon2id"}},
		{"short salt", RingAdminRequest{Cmd: AdminCreateUser, User: &RingUserRequest{Name: "ada", Roles: &roles, Credential: bad(func(c *RingUserCredential) { c.Salt = c.Salt[:15] })}}, []string{"salt", "16 to 64"}},
		{"short hash", RingAdminRequest{Cmd: AdminCreateUser, User: &RingUserRequest{Name: "ada", Roles: &roles, Credential: bad(func(c *RingUserCredential) { c.Hash = c.Hash[:31] })}}, []string{"hash", "32"}},
		{"zero iterations", RingAdminRequest{Cmd: AdminAlterUser, User: &RingUserRequest{Name: "ada", Credential: bad(func(c *RingUserCredential) { c.Iterations = 0 })}}, []string{"iterations"}},
		{"negative fence", RingAdminRequest{Cmd: AdminDropUser, User: &RingUserRequest{Name: "ada", ExpectSeq: u1Seq(-1)}}, []string{"expect_seq", "0 or more"}},
		{"negative drop fence", RingAdminRequest{Cmd: AdminCreateUser, User: &RingUserRequest{Name: "ada", ExpectDropSeq: -1, Roles: &roles, Credential: u1Credential()}}, []string{"expect_drop_seq", "0 or more"}},
		{"create without roles", RingAdminRequest{Cmd: AdminCreateUser, User: &RingUserRequest{Name: "ada", Credential: u1Credential()}}, []string{"create_user", "roles"}},
		{"empty role", RingAdminRequest{Cmd: AdminGrantRole, User: &RingUserRequest{Name: "ada", Grant: RingUserGrant{DB: "app"}}}, []string{"grant_role", "role"}},
		{"drop with a fence for create", RingAdminRequest{Cmd: AdminDropUser, User: &RingUserRequest{Name: "ada", ExpectDropSeq: 3}}, []string{"drop_user", "expect_drop_seq"}},
		{"grant with a credential", RingAdminRequest{Cmd: AdminGrantRole, User: &RingUserRequest{Name: "ada", Grant: RingUserGrant{Role: "reader", DB: "app"}, Credential: u1Credential()}}, []string{"grant_role", "credential"}},
		{"user members on a bucket command", RingAdminRequest{Cmd: AdminDescribeBucket, Database: "app", Bucket: "B", User: &RingUserRequest{Name: "ada"}}, []string{"describe_bucket", "user"}},
		{"bucket members on a user command", RingAdminRequest{Cmd: AdminDropUser, Database: "app", User: &RingUserRequest{Name: "ada"}}, []string{"drop_user", "database"}},
	}
	for _, c := range cases {
		_, err := EncodeRingAdmin(c.req)
		if err == nil {
			t.Errorf("%s: accepted", c.name)
			continue
		}
		var coded *cluster.Error
		if !errors.As(err, &coded) || coded.Code != cluster.CodeInvalidArgument {
			t.Errorf("%s: %v is not INVALID_ARGUMENT", c.name, err)
		}
		for _, want := range c.want {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s: refusal %q does not name %q", c.name, err, want)
			}
		}
	}
	replyCases := []struct {
		name  string
		reply RingAdminReply
	}{
		{"mutation without a result", RingAdminReply{Cmd: AdminDropUser, Users: &RingAdminUsers{Name: "ada"}}},
		{"negative revision", RingAdminReply{Cmd: AdminDropUser, Users: &RingAdminUsers{Name: "ada", Revision: -1, Result: "x"}}},
		{"list without users", RingAdminReply{Cmd: AdminShowUsers, Users: &RingAdminUsers{}}},
		{"describe without a user", RingAdminReply{Cmd: AdminDescribeUser, Users: &RingAdminUsers{}}},
		{"user reply with a bucket", RingAdminReply{Cmd: AdminDescribeUser, BucketID: 3, Users: &RingAdminUsers{User: &RingUserRow{Name: "ada", Roles: []RingUserGrant{}}}}},
		{"bucket reply with users", RingAdminReply{Cmd: AdminShowBuckets, Buckets: []RingAdminListed{}, Users: &RingAdminUsers{Users: []RingUserRow{}}}},
		{"mutation with a list", RingAdminReply{Cmd: AdminGrantRole, Users: &RingAdminUsers{Name: "ada", Result: "x", Users: []RingUserRow{}}}},
	}
	for _, c := range replyCases {
		if _, err := EncodeRingAdminReply(c.reply); err == nil {
			t.Errorf("reply %s: accepted", c.name)
		}
	}
}

// TestARingUserRequestCarriesNoPasswordMember: [GUARD the request types hold no member that can
// carry a password: the only text members are the account name, the algorithm word (which must be
// argon2id), and role and database names; the only byte members are the salt and the hash, each
// bounded to its length].
func TestARingUserRequestCarriesNoPasswordMember(t *testing.T) {
	var texts, blobs []string
	var walk func(prefix string, typ reflect.Type)
	walk = func(prefix string, typ reflect.Type) {
		for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice && typ.Elem().Kind() != reflect.Uint8 {
			typ = typ.Elem()
		}
		switch {
		case typ.Kind() == reflect.String:
			texts = append(texts, prefix)
		case typ.Kind() == reflect.Slice:
			blobs = append(blobs, prefix)
		case typ.Kind() == reflect.Struct:
			for i := range typ.NumField() {
				walk(strings.TrimPrefix(prefix+"."+typ.Field(i).Name, "."), typ.Field(i).Type)
			}
		}
	}
	walk("", reflect.TypeOf(RingUserRequest{}))
	if got := strings.Join(texts, " "); got != "Name Credential.Algo Roles.Role Roles.DB Grant.Role Grant.DB" {
		t.Errorf("text members of a user request are %s", got)
	}
	if got := strings.Join(blobs, " "); got != "Credential.Salt Credential.Hash" {
		t.Errorf("byte members of a user request are %s", got)
	}
	roles := []RingUserGrant{}
	cred := u1Credential()
	cred.Hash = []byte("correct horse battery staple, a password of 32 bytes"[:32])
	cred.Algo = "password"
	if _, err := EncodeRingAdmin(RingAdminRequest{Cmd: AdminCreateUser, User: &RingUserRequest{Name: "ada", Roles: &roles, Credential: cred}}); err == nil {
		t.Error("a credential whose algorithm is not argon2id was encoded")
	}
}

// TestUserClassBytesNeverReachTheWire: [GUARD the seven user classes are 0xF5 to 0xFB, above the
// membership classes and every transport opcode, distinct, and refused as a RING_KEYED class byte].
func TestUserClassBytesNeverReachTheWire(t *testing.T) {
	classes := []RingClass{RingClassCreateUser, RingClassAlterUser, RingClassDropUser, RingClassGrantRole, RingClassRevokeRole, RingClassShowUsers, RingClassDescribeUser}
	for i, class := range classes {
		if class != RingClass(0xF5+i) {
			t.Errorf("class %d is 0x%02x, want 0x%02x", i, class, 0xF5+i)
		}
		if transport.Opcode(class) <= transport.OpRingReply || class <= RingClassReplaceNode {
			t.Errorf("class 0x%02x is not above the opcodes and the membership classes", class)
		}
		body := append(make([]byte, RingPrologueLen), class)
		body = append(body, make([]byte, 64)...)
		if _, err := DecodeRingKeyed(body); err == nil {
			t.Errorf("class 0x%02x was accepted as the class byte of a RING_KEYED body", class)
		}
	}
	for _, cmd := range []string{AdminCreateUser, AdminAlterUser, AdminDropUser, AdminGrantRole, AdminRevokeRole, AdminShowUsers, AdminDescribeUser} {
		if !strings.Contains(adminCommands, cmd) {
			t.Errorf("the command list does not name %s", cmd)
		}
	}
}

// TestUserRefusalsClipWhatTheCallerSent: [GUARD a refusal that repeats a member key or an
// algorithm word the caller chose repeats at most 64 bytes of it, so its text does not grow with
// the body].
func TestUserRefusalsClipWhatTheCallerSent(t *testing.T) {
	long := strings.Repeat("k", 4000)
	raw, err := bson.Marshal(bson.D{{Key: "cmd", Value: AdminDropUser}, {Key: "name", Value: "ada"}, {Key: long, Value: "x"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = DecodeRingAdmin(raw)
	if err == nil || len(err.Error()) > 400 {
		t.Errorf("a long stray key gave a refusal of %d bytes", len(fmtLen(err)))
	}
	cred := u1Credential()
	cred.Algo = strings.Repeat("a", 4000)
	roles := []RingUserGrant{}
	_, err = EncodeRingAdmin(RingAdminRequest{Cmd: AdminCreateUser, User: &RingUserRequest{Name: "ada", Roles: &roles, Credential: cred}})
	if err == nil || len(err.Error()) > 400 {
		t.Errorf("a long algorithm word gave a refusal of %d bytes", len(fmtLen(err)))
	}
}

func fmtLen(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
