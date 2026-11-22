package main

import (
	"bytes"
	"sync"

	"github.com/hoarddb/hoarddb-bson/bson"
	"golang.org/x/crypto/argon2"

	hc "github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The user-administration vectors are the seven user commands of RING_ADMIN
// (entry-ring-admin-wire.md section 10). A credential is derived here the way a driver must
// derive it: Argon2id, version 0x13, 64 MiB, three passes, four lanes, a 16-byte salt and a
// 32-byte hash, sent as raw binary. A salt is a fixed byte pattern so two generations are
// identical. A vector never carries the password.

// userPassword is the password every derived credential of the vectors hides.
const userPassword = "vectors-user-password"

var (
	ringCredentialMutex sync.Mutex
	ringCredentialCache = make(map[byte]*wire.RingUserCredential)
)

// ringCredential is the credential for userPassword with the salt of sixteen copies of seed,
// derived once per run.
func ringCredential(seed byte) *wire.RingUserCredential {
	ringCredentialMutex.Lock()
	defer ringCredentialMutex.Unlock()
	if cached, ok := ringCredentialCache[seed]; ok {
		return cached
	}
	salt := bytes.Repeat([]byte{seed}, 16)
	credential := &wire.RingUserCredential{Algo: hc.UserAlgo, Salt: salt, Hash: argon2.IDKey([]byte(userPassword), salt, 3, 65536, 4, 32),
		Memory: 65536, Iterations: 3, Parallelism: 4}
	ringCredentialCache[seed] = credential
	return credential
}

// ringUserReplyFields are the members of the user replies (admin-wire section 10.2).
var ringUserReplyFields = []string{"cmd", "name", "revision", "result", "user_drop_seq", "users[].name", "users[].roles", "users[].enabled", "users[].epoch",
	"users[].created_ms", "users[].password_changed_ms", "users[].revision", "user"}

// userVectors are the seven user commands: a request and its reply each, the refusals a driver
// must tell apart, and the requests and replies a strict decoder refuses.
func (b *ringBuilder) userVectors() {
	fields, replyFields := ringFieldTable["admin"], ringUserReplyFields
	change := func(cmd, name, line string) []byte {
		return b.must(wire.EncodeRingAdminReply(wire.RingAdminReply{Cmd: cmd, Users: &wire.RingAdminUsers{Name: name, Revision: 4, Result: line}}))
	}
	seven := int64(7)
	off := false
	reader := []wire.RingUserGrant{{Role: "reader", DB: "app"}}
	create := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminCreateUser,
		User: &wire.RingUserRequest{Name: "ada", ExpectDropSeq: 3, Roles: &reader, Credential: ringCredential(1)}}))
	b.exchange("ring admin: create_user", "create_user takes name, expect_drop_seq, roles and credential, in that order, all required. The credential is Argon2id derived on the client (algo, salt and hash as binary, memory, iterations, parallelism); no member carries a password. "+
		"expect_drop_seq is user_drop_seq from the last show_users or describe_user reply: the server refuses the create CONFLICT when a user was dropped since, so a retry, which resends the first attempt's request id and the same expect_drop_seq, never brings back a dropped account. The reply is name, revision (what a later change sends as expect_seq) and one result line.",
		transport.OpRingAdmin, fields, create, replyFields, change(wire.AdminCreateUser, "ada", `user "ada" created`))
	alter := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminAlterUser, User: &wire.RingUserRequest{Name: "ada", Credential: ringCredential(2)}}))
	b.exchange("ring admin: alter_user of the password", "alter_user takes name, then optionally expect_seq, credential, enabled and roles, in that order; at least one of the last three. A new password is a new credential with a fresh salt. Without expect_seq the server takes the revision it holds.",
		transport.OpRingAdmin, fields, alter, replyFields, change(wire.AdminAlterUser, "ada", `user "ada" altered (credentials)`))
	disable := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminAlterUser, User: &wire.RingUserRequest{Name: "ada", ExpectSeq: &seven, Enabled: &off}}))
	b.exchange("ring admin: alter_user disabling at a revision", "enabled false disables the account and ends its sessions; expect_seq is the revision the client read, and the server refuses the change CONFLICT when the account has changed since.",
		transport.OpRingAdmin, fields, disable, replyFields, change(wire.AdminAlterUser, "ada", `user "ada" altered (enabled)`))
	none := []wire.RingUserGrant{}
	roles := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminAlterUser, User: &wire.RingUserRequest{Name: "ada", Roles: &none}}))
	b.exchange("ring admin: alter_user replacing the roles with none", "roles replaces the whole role list; an empty array removes every grant, an absent roles leaves them alone.",
		transport.OpRingAdmin, fields, roles, replyFields, change(wire.AdminAlterUser, "ada", `user "ada" altered (roles)`))
	drop := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: "ada", ExpectSeq: &seven}}))
	b.exchange("ring admin: drop_user", "drop_user takes name and optionally expect_seq. It ends the account's sessions and moves the store's user_drop_seq.",
		transport.OpRingAdmin, fields, drop, replyFields, change(wire.AdminDropUser, "ada", `user "ada" dropped`))
	grant := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "ada", Grant: wire.RingUserGrant{Role: "writer", DB: "app"}}}))
	b.exchange("ring admin: grant_role", "grant_role takes name, optionally expect_seq, then role and db (\"*\" for every database). A grant already held succeeds and changes nothing.",
		transport.OpRingAdmin, fields, grant, replyFields, change(wire.AdminGrantRole, "ada", `granted writer@app to "ada"`))
	revoke := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminRevokeRole, User: &wire.RingUserRequest{Name: "ada", Grant: wire.RingUserGrant{Role: "writer", DB: "app"}}}))
	b.exchange("ring admin: revoke_role", "revoke_role has grant_role's members. Revoking a grant that is not held succeeds and changes nothing.",
		transport.OpRingAdmin, fields, revoke, replyFields, change(wire.AdminRevokeRole, "ada", `revoked writer@app from "ada"`))
	row := wire.RingUserRow{Name: "ada", Roles: reader, Enabled: true, Epoch: 1, CreatedMs: 1_767_225_600_000, PasswordChangedMs: 1_767_225_600_000, Revision: 4}
	admin := wire.RingUserRow{Name: "admin", Roles: []wire.RingUserGrant{{Role: "root", DB: "*"}}, Enabled: true, Epoch: 1, CreatedMs: 1_767_225_000_000, PasswordChangedMs: 1_767_225_000_000, Revision: 4}
	show := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminShowUsers}))
	b.exchange("ring admin: show_users", "show_users takes no member. The reply is revision (the store's), user_drop_seq (what the next create_user sends as expect_drop_seq) and every account ordered by name, each with roles, enabled, epoch, created_ms, password_changed_ms (Unix milliseconds) and revision. No salt, hash or key id is in any reply.",
		transport.OpRingAdmin, fields, show, replyFields, b.must(wire.EncodeRingAdminReply(wire.RingAdminReply{Cmd: wire.AdminShowUsers, Users: &wire.RingAdminUsers{Revision: 4, DropSeq: 3, Users: []wire.RingUserRow{admin, row}}})))
	describe := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminDescribeUser, User: &wire.RingUserRequest{Name: "ada"}}))
	b.exchange("ring admin: describe_user", "describe_user takes name. The reply is user_drop_seq and user, one account in show_users's row shape. An absent name is NOT_FOUND.",
		transport.OpRingAdmin, fields, describe, replyFields, b.must(wire.EncodeRingAdminReply(wire.RingAdminReply{Cmd: wire.AdminDescribeUser, Users: &wire.RingAdminUsers{DropSeq: 3, User: &row}})))

	b.refused("ring admin: create_user after a drop since the read", "The create's expect_drop_seq is older than the store's user_drop_seq: a user was dropped since the client read the users. CONFLICT; nothing was created. Read the users again and send the create with the new user_drop_seq.",
		transport.OpRingAdmin, fields, create, wire.RingError{Code: hc.CodeConflict, Message: `create user "ada" was not applied: a user was dropped since the users were read (drop at 5, read at 3). Run 'user describe ada' to see whether it exists, then read the users again and run the statement again.`},
		"a user was dropped since the users were read")
	b.refused("ring admin: a change at a stale revision", "expect_seq is not the account's revision: CONFLICT; nothing was changed. Describe the user and repeat the change against what it shows.",
		transport.OpRingAdmin, fields, disable, wire.RingError{Code: hc.CodeConflict, Message: `alter user "ada" was not applied: the user changed since it was read (revision 7, now 9). Run 'user describe ada' and repeat the change against what it shows.`},
		"the user changed since it was read")
	b.refused("ring admin: drop_user of an absent name", "A drop of a name that does not exist is NOT_FOUND, never success, so a typo is not silent.",
		transport.OpRingAdmin, fields, b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: "adaa"}})),
		wire.RingError{Code: hc.CodeNotFound, Message: `drop user "adaa" was not applied: there is no user "adaa". Run 'show users' to list the users.`}, "there is no user")
	b.refused("ring admin: drop_user of the last administrator", "The store keeps at least one enabled account holding root or user-manager on every database; dropping or disabling the last one is refused INVALID_ARGUMENT (exit 2: a retry is refused the same way) with the grant to give first.",
		transport.OpRingAdmin, fields, b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: "admin"}})),
		wire.RingError{Code: hc.CodeInvalidArgument, Message: `drop user "admin" was refused: it is the last enabled account that can manage users (root, or user-manager on every database). Give another enabled account that authority first, for example 'grant role user-manager@* to ops', then run the statement again`},
		"last enabled account that can manage users")
	b.refused("ring admin: user commands without the user-manager role", "The seven commands need user-manager@* or root@*; a grant on one database is denied. PERMISSION_DENIED is one text for every command.",
		transport.OpRingAdmin, fields, show, wire.RingError{Code: hc.CodePermissionDenied, Message: wire.ErrUsersDenied.Error()}, "user-manager@*")
	b.refused("ring admin: a user change on a cluster member with no leader", "On a cluster member a user change is a cluster command: when no leader answers within 20 seconds it is UNAVAILABLE and says nothing changed. After an attempt whose answer was lost the text says it may or may not have been applied instead.",
		transport.OpRingAdmin, fields, b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: "ada"}})),
		wire.RingError{Code: hc.CodeUnavailable, Message: `drop user "ada" was not applied: no leader of the cluster answered within 20 seconds, so nothing changed. Run 'nodes' to see which members are down, and run the statement again when a majority is up.`},
		"nothing changed")

	cred := ringDoc("algo", hc.UserAlgo, "salt", ringCredential(1).Salt, "hash", ringCredential(1).Hash, "memory", int64(65536), "iterations", int64(3), "parallelism", int32(4))
	grants := []any{ringDocValue("role", "reader", "db", "app")}
	b.refusal("ring admin refused: create_user without the drop fence", "create_user requires expect_drop_seq; the refusal names it and where to read it.", transport.OpRingAdmin, "request", fields,
		ringDoc("cmd", wire.AdminCreateUser, "name", "ada", "roles", grants, "credential", ringRawDoc(cred)))
	b.refusal("ring admin refused: create_user carrying a password", "No user command takes a password member; the refusal names it.", transport.OpRingAdmin, "request", fields,
		ringDoc("cmd", wire.AdminCreateUser, "name", "ada", "expect_drop_seq", int64(0), "roles", grants, "credential", ringRawDoc(cred), "password", userPassword))
	b.refusal("ring admin refused: expect_drop_seq as int32", "expect_drop_seq and expect_seq are int64.", transport.OpRingAdmin, "request", fields,
		ringDoc("cmd", wire.AdminCreateUser, "name", "ada", "expect_drop_seq", int32(0), "roles", grants, "credential", ringRawDoc(cred)))
	b.refusal("ring admin refused: alter_user that changes nothing", "alter_user needs at least one of credential, enabled and roles.", transport.OpRingAdmin, "request", fields,
		ringDoc("cmd", wire.AdminAlterUser, "name", "ada"))
	b.refusal("ring admin refused: grant_role without db", "grant_role and revoke_role need role and db.", transport.OpRingAdmin, "request", fields,
		ringDoc("cmd", wire.AdminGrantRole, "name", "ada", "role", "reader"))
	b.refusal("ring admin refused: show_users with a name", "show_users takes no member besides cmd.", transport.OpRingAdmin, "request", fields,
		ringDoc("cmd", wire.AdminShowUsers, "name", "ada"))
	b.refusal("ring admin reply refused: show_users without user_drop_seq", "A show_users reply is revision, user_drop_seq and users.", transport.OpRingAdmin, "reply", replyFields,
		ringDoc("cmd", wire.AdminShowUsers, "revision", int64(4), "users", []any{}))
	b.refusal("ring admin reply refused: a user change reply with a list", "A change reply is name, revision and result, and nothing else.", transport.OpRingAdmin, "reply", replyFields,
		ringDoc("cmd", wire.AdminDropUser, "name", "ada", "revision", int64(4), "result", "x", "users", []any{}))
}

// ringDocValue is a nested document for ringDoc.
func ringDocValue(pairs ...any) any { return ringRawDoc(ringDoc(pairs...)) }

// ringRawDoc embeds an encoded document as a member value.
func ringRawDoc(raw []byte) bson.Raw { return bson.Raw(raw) }
