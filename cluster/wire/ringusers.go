package wire

import (
	"bytes"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/authrules"
	"github.com/hoarddb/hoarddb-server/cluster"
)

// This file is the codec of the seven user commands of RING_ADMIN (entry-ring-admin-wire.md
// section 10, ring-user-store.md 4.3). Like the rest of the admin wire a body is accepted only
// in its one canonical layout; the documents are written and read member by member here, not
// through a struct, because whether a member is present carries meaning (an alter's credential,
// enabled and roles; a fence) and a struct cannot tell absent from zero. A request never carries
// a password: the client derives an Argon2id credential and sends the salt and hash.

// The cmd values of the user commands.
const (
	// AdminCreateUser creates an account with a client-derived credential.
	AdminCreateUser = "create_user"
	// AdminAlterUser changes an account's credential, enabled flag or whole role list.
	AdminAlterUser = "alter_user"
	// AdminDropUser removes an account.
	AdminDropUser = "drop_user"
	// AdminGrantRole adds one grant to an account.
	AdminGrantRole = "grant_role"
	// AdminRevokeRole removes one grant from an account.
	AdminRevokeRole = "revoke_role"
	// AdminShowUsers lists every account, never with credential material.
	AdminShowUsers = "show_users"
	// AdminDescribeUser returns one account, never with credential material.
	AdminDescribeUser = "describe_user"
)

// The classes of the user commands. Like every admin class they are never on the wire; they
// sit at 0xF5 to 0xFB, above the membership classes (0xF0 to 0xF4) and every transport opcode.
const (
	// RingClassCreateUser is a create_user command.
	RingClassCreateUser RingClass = 0xF5
	// RingClassAlterUser is an alter_user command.
	RingClassAlterUser RingClass = 0xF6
	// RingClassDropUser is a drop_user command.
	RingClassDropUser RingClass = 0xF7
	// RingClassGrantRole is a grant_role command.
	RingClassGrantRole RingClass = 0xF8
	// RingClassRevokeRole is a revoke_role command.
	RingClassRevokeRole RingClass = 0xF9
	// RingClassShowUsers is a show_users command.
	RingClassShowUsers RingClass = 0xFA
	// RingClassDescribeUser is a describe_user command.
	RingClassDescribeUser RingClass = 0xFB
)

const (
	// MaxUserGrants bounds the grants one request or one row carries, and so the grants one
	// account may hold: a store must refuse the grant past it, or no reply could list the account.
	MaxUserGrants = 64
	// ringMaxUserResult bounds the result line of a user change reply, in bytes.
	ringMaxUserResult = 512
	// ringMaxArgon2Factor bounds memory and iterations: Argon2id takes them as uint32.
	ringMaxArgon2Factor = 1<<32 - 1
	// ringMaxArgon2Threads bounds parallelism: Argon2id takes it as uint8.
	ringMaxArgon2Threads = 255
)

// userClasses lists the seven classes, for RingClasses.
func userClasses() []RingClass {
	return []RingClass{RingClassCreateUser, RingClassAlterUser, RingClassDropUser, RingClassGrantRole,
		RingClassRevokeRole, RingClassShowUsers, RingClassDescribeUser}
}

func isUserCmd(cmd string) bool {
	switch cmd {
	case AdminCreateUser, AdminAlterUser, AdminDropUser, AdminGrantRole, AdminRevokeRole, AdminShowUsers, AdminDescribeUser:
		return true
	}
	return false
}

// UserClassOf is the class a user cmd is authorized as; ok is false for any other cmd.
func UserClassOf(cmd string) (RingClass, bool) {
	switch cmd {
	case AdminCreateUser:
		return RingClassCreateUser, true
	case AdminAlterUser:
		return RingClassAlterUser, true
	case AdminDropUser:
		return RingClassDropUser, true
	case AdminGrantRole:
		return RingClassGrantRole, true
	case AdminRevokeRole:
		return RingClassRevokeRole, true
	case AdminShowUsers:
		return RingClassShowUsers, true
	case AdminDescribeUser:
		return RingClassDescribeUser, true
	}
	return 0, false
}

// RingUserGrant is one role on one database; DB "*" is every database.
type RingUserGrant struct {
	Role, DB string
}

// RingUserCredential is a credential the client derived: Argon2id over the password with Salt,
// giving Hash. The server peppers Hash before it keeps it; the password never leaves the client.
type RingUserCredential struct {
	// Algo is always "argon2id".
	Algo string
	// Salt is 16 to 64 random bytes, never shared between accounts; Hash is 32 bytes.
	Salt, Hash []byte
	// Memory (KiB) and Iterations are the Argon2id work factors; Parallelism its lanes.
	Memory, Iterations int64
	Parallelism        int32
}

// RingUserRequest is the member set of a user command. Which members a command takes, and which
// it requires, is section 10.1's table; a member a command does not take must be zero.
type RingUserRequest struct {
	// Name is the account, 1 to 1021 bytes of printable UTF-8. Every command but show_users takes it.
	Name string
	// ExpectSeq is the account's revision the client last read: alter, drop, grant and revoke take
	// it, optionally. Absent, the server fills it from its own table.
	ExpectSeq *int64
	// ExpectDropSeq is the user_drop_seq of the last show_users or describe_user reply the client
	// read. create_user requires it on the wire; a retry resends the first attempt's value.
	ExpectDropSeq int64
	// Credential is required by create_user and optional on alter_user.
	Credential *RingUserCredential
	// Enabled is alter_user's, optional.
	Enabled *bool
	// Roles is the whole role list: required (possibly empty) by create_user, optional on alter_user.
	Roles *[]RingUserGrant
	// Grant is the one grant of grant_role and revoke_role.
	Grant RingUserGrant
}

// RingUserRow is one account as a reply carries it: never a salt, hash or key id.
type RingUserRow struct {
	Name    string
	Roles   []RingUserGrant
	Enabled bool
	// Epoch is the account's login epoch: a new password or a disable moves it.
	Epoch int64
	// CreatedMs and PasswordChangedMs are Unix milliseconds.
	CreatedMs, PasswordChangedMs int64
	// Revision is what a later change of this account sends as expect_seq.
	Revision int64
}

// RingAdminUsers is the reply of a user command. A change (create, alter, drop, grant, revoke)
// sets Name, Revision and Result; show_users sets Revision, DropSeq and Users (non-nil);
// describe_user sets DropSeq and User.
type RingAdminUsers struct {
	Name string
	// Revision is the account's revision after a change, or the user store's for show_users.
	Revision int64
	// Result is one printable line saying what was done.
	Result string
	// DropSeq is the store's user_drop_seq: what the next create_user sends as expect_drop_seq.
	DropSeq int64
	Users   []RingUserRow
	User    *RingUserRow
}

// userMembers is the canonical member order of each user command, after cmd; the ones marked
// required must be present.
var userMembers = map[string][]string{
	AdminCreateUser:   {"name", "expect_drop_seq", "roles", "credential"},
	AdminAlterUser:    {"name", "expect_seq", "credential", "enabled", "roles"},
	AdminDropUser:     {"name", "expect_seq"},
	AdminGrantRole:    {"name", "expect_seq", "role", "db"},
	AdminRevokeRole:   {"name", "expect_seq", "role", "db"},
	AdminShowUsers:    {},
	AdminDescribeUser: {"name"},
}

var userRequired = map[string][]string{
	AdminCreateUser:   {"name", "expect_drop_seq", "roles", "credential"},
	AdminAlterUser:    {"name"},
	AdminDropUser:     {"name"},
	AdminGrantRole:    {"name", "role", "db"},
	AdminRevokeRole:   {"name", "role", "db"},
	AdminDescribeUser: {"name"},
}

// userMissingFix is the fix a refusal names for a required member that is absent.
func userMissingFix(cmd, member string) string {
	switch member {
	case "expect_drop_seq":
		return "a create carries the drop fence so a retry can never bring back an account dropped in between: send user_drop_seq from the last show_users or describe_user reply, and resend the same value on a retry"
	case "credential":
		return "send the Argon2id credential derived from the password on the client (algo, salt, hash, memory, iterations, parallelism), never the password"
	case "roles":
		return "send the account's roles as an array of {role, db}, empty for none"
	}
	return "send " + member
}

// checkUserText bounds a name, role or database: 1 to 1021 bytes of UTF-8 with no control character.
func checkUserText(cmd, member, text string) error {
	if len(text) == 0 || len(text) > ringMaxName {
		return ringRefusal("%s: %s is %d bytes and it is 1 to %d. Send the %s as 'show users' lists it", cmd, member, len(text), ringMaxName, member)
	}
	if !utf8.ValidString(text) {
		return ringRefusal("%s: %s is not valid UTF-8. Send it as UTF-8 text", cmd, member)
	}
	if at := strings.IndexFunc(text, unicode.IsControl); at >= 0 {
		return ringRefusal("%s: %s has a control character at byte %d, which none may hold. Send printable characters", cmd, member, at)
	}
	return nil
}

func checkUserGrants(cmd, member string, grants []RingUserGrant) error {
	if len(grants) > MaxUserGrants {
		return ringRefusal("%s: %s holds %d grants and at most %d are taken. Send fewer grants", cmd, member, len(grants), MaxUserGrants)
	}
	for _, grant := range grants {
		if err := checkUserText(cmd, "role", grant.Role); err != nil {
			return err
		}
		if err := checkUserText(cmd, "db", grant.DB); err != nil {
			return err
		}
	}
	return nil
}

func checkUserCredential(cmd string, c *RingUserCredential) error {
	switch {
	case c.Algo != cluster.UserAlgo:
		return ringRefusal("%s: credential algo is %s and only %q is taken. Derive the credential with Argon2id on the client and send the salt and hash, never the password", cmd, echoClipped(c.Algo), cluster.UserAlgo)
	case len(c.Salt) < authrules.MinSaltLen || len(c.Salt) > authrules.MaxSaltLen:
		return ringRefusal("%s: credential salt is %d bytes and it is %d to %d. Draw a fresh random salt for every credential", cmd, len(c.Salt), authrules.MinSaltLen, authrules.MaxSaltLen)
	case len(c.Hash) != authrules.Argon2HashLen:
		return ringRefusal("%s: credential hash is %d bytes and Argon2id's output is %d. Send the raw Argon2id output", cmd, len(c.Hash), authrules.Argon2HashLen)
	case c.Memory < 1 || c.Memory > ringMaxArgon2Factor:
		return ringRefusal("%s: credential memory is %d KiB and it is 1 to %d", cmd, c.Memory, int64(ringMaxArgon2Factor))
	case c.Iterations < 1 || c.Iterations > ringMaxArgon2Factor:
		return ringRefusal("%s: credential iterations is %d and it is 1 to %d", cmd, c.Iterations, int64(ringMaxArgon2Factor))
	case c.Parallelism < 1 || c.Parallelism > ringMaxArgon2Threads:
		return ringRefusal("%s: credential parallelism is %d and it is 1 to %d", cmd, c.Parallelism, ringMaxArgon2Threads)
	}
	return nil
}

// checkUserRequest checks a user command: no member of another command, the members it takes,
// each bounded.
func checkUserRequest(req RingAdminRequest) error {
	if err := adminStray(req, ""); err != nil {
		return err
	}
	if req.Cmd == AdminShowUsers {
		if req.User != nil {
			return ringRefusal("show_users takes no member besides cmd, and this request also sets user members. Remove them")
		}
		return nil
	}
	user := req.User
	if user == nil {
		return ringRefusal("%s needs name, the account it acts on. Send name", req.Cmd)
	}
	set := map[string]bool{"name": user.Name != "", "expect_seq": user.ExpectSeq != nil, "expect_drop_seq": user.ExpectDropSeq != 0,
		"credential": user.Credential != nil, "enabled": user.Enabled != nil, "roles": user.Roles != nil, "role": user.Grant.Role != "", "db": user.Grant.DB != ""}
	for _, member := range []string{"expect_seq", "expect_drop_seq", "credential", "enabled", "roles", "role", "db"} {
		if set[member] && !slices.Contains(userMembers[req.Cmd], member) {
			return ringRefusal("%s takes only %s and cmd, and this request also sets %s. Remove %s", req.Cmd, strings.Join(userMembers[req.Cmd], ", "), member, member)
		}
	}
	if err := checkUserText(req.Cmd, "name", user.Name); err != nil {
		return err
	}
	if user.ExpectSeq != nil && *user.ExpectSeq < 0 {
		return ringRefusal("%s: expect_seq is %d and a revision is 0 or more. Send the revision the last read of the user returned, or leave expect_seq out", req.Cmd, *user.ExpectSeq)
	}
	if user.ExpectDropSeq < 0 {
		return ringRefusal("%s: expect_drop_seq is %d and it is 0 or more. Send user_drop_seq from the last show_users or describe_user reply", req.Cmd, user.ExpectDropSeq)
	}
	switch req.Cmd {
	case AdminCreateUser:
		if user.Roles == nil {
			return ringRefusal("create_user needs roles: %s", userMissingFix(req.Cmd, "roles"))
		}
		if user.Credential == nil {
			return ringRefusal("create_user needs credential: %s", userMissingFix(req.Cmd, "credential"))
		}
	case AdminAlterUser:
		if user.Credential == nil && user.Enabled == nil && user.Roles == nil {
			return ringRefusal("alter_user changes nothing: send at least one of credential, enabled or roles")
		}
	case AdminGrantRole, AdminRevokeRole:
		if err := checkUserGrants(req.Cmd, "the grant", []RingUserGrant{user.Grant}); err != nil {
			return err
		}
	}
	if user.Credential != nil {
		if err := checkUserCredential(req.Cmd, user.Credential); err != nil {
			return err
		}
	}
	if user.Roles != nil {
		return checkUserGrants(req.Cmd, "roles", *user.Roles)
	}
	return nil
}

func appendUserGrants(doc []byte, key string, grants []RingUserGrant) []byte {
	elems := make([][]byte, len(grants))
	for i, grant := range grants {
		var g []byte
		g = bsoncore.AppendStringElement(g, "role", grant.Role)
		g = bsoncore.AppendStringElement(g, "db", grant.DB)
		elems[i] = bsoncore.AppendDocumentElement(nil, strconv.Itoa(i), bsoncore.BuildDocument(nil, g))
	}
	return bsoncore.AppendArrayElement(doc, key, bsoncore.BuildDocument(nil, bytes.Join(elems, nil)))
}

func appendUserCredential(doc []byte, c *RingUserCredential) []byte {
	var cred []byte
	cred = bsoncore.AppendStringElement(cred, "algo", c.Algo)
	cred = bsoncore.AppendBinaryElement(cred, "salt", bsontype.BinaryGeneric, c.Salt)
	cred = bsoncore.AppendBinaryElement(cred, "hash", bsontype.BinaryGeneric, c.Hash)
	cred = bsoncore.AppendInt64Element(cred, "memory", c.Memory)
	cred = bsoncore.AppendInt64Element(cred, "iterations", c.Iterations)
	cred = bsoncore.AppendInt32Element(cred, "parallelism", c.Parallelism)
	return bsoncore.AppendDocumentElement(doc, "credential", bsoncore.BuildDocument(nil, cred))
}

// userRequestBytes writes a checked user command in its canonical layout.
func userRequestBytes(req RingAdminRequest) []byte {
	doc := bsoncore.AppendStringElement(nil, "cmd", req.Cmd)
	if user := req.User; user != nil {
		doc = bsoncore.AppendStringElement(doc, "name", user.Name)
		if user.ExpectSeq != nil {
			doc = bsoncore.AppendInt64Element(doc, "expect_seq", *user.ExpectSeq)
		}
		switch req.Cmd {
		case AdminCreateUser:
			doc = bsoncore.AppendInt64Element(doc, "expect_drop_seq", user.ExpectDropSeq)
			doc = appendUserGrants(doc, "roles", *user.Roles)
			doc = appendUserCredential(doc, user.Credential)
		case AdminAlterUser:
			if user.Credential != nil {
				doc = appendUserCredential(doc, user.Credential)
			}
			if user.Enabled != nil {
				doc = bsoncore.AppendBooleanElement(doc, "enabled", *user.Enabled)
			}
			if user.Roles != nil {
				doc = appendUserGrants(doc, "roles", *user.Roles)
			}
		case AdminGrantRole, AdminRevokeRole:
			doc = bsoncore.AppendStringElement(doc, "role", user.Grant.Role)
			doc = bsoncore.AppendStringElement(doc, "db", user.Grant.DB)
		}
	}
	return bsoncore.BuildDocument(nil, doc)
}

// errUserLayout is the refusal of a member with the wrong type; the re-encode compare names the
// rest of the layout faults (order, duplicates) with the same text.
func errUserLayout(cmd string) error {
	return ringRefusal("a %s request does not have the layout of entry-ring-admin-wire.md section 10 (members in order, none duplicated or extra, the stated types, absent rather than written as a default)", cmd)
}

func readUserGrants(value bsoncore.Value) ([]RingUserGrant, bool) {
	array, ok := value.ArrayOK()
	if !ok {
		return nil, false
	}
	values, err := array.Values()
	if err != nil || len(values) > MaxUserGrants {
		return nil, false
	}
	grants := make([]RingUserGrant, 0, len(values))
	for _, item := range values {
		doc, ok := item.DocumentOK()
		if !ok {
			return nil, false
		}
		role, okRole := doc.Lookup("role").StringValueOK()
		db, okDB := doc.Lookup("db").StringValueOK()
		if !okRole || !okDB {
			return nil, false
		}
		grants = append(grants, RingUserGrant{Role: role, DB: db})
	}
	return grants, true
}

func readUserCredential(value bsoncore.Value) (*RingUserCredential, bool) {
	doc, ok := value.DocumentOK()
	if !ok {
		return nil, false
	}
	var c RingUserCredential
	var okAlgo, okMemory, okIterations, okParallelism bool
	var saltType, hashType byte
	var okSalt, okHash bool
	c.Algo, okAlgo = doc.Lookup("algo").StringValueOK()
	saltType, c.Salt, okSalt = doc.Lookup("salt").BinaryOK()
	hashType, c.Hash, okHash = doc.Lookup("hash").BinaryOK()
	c.Memory, okMemory = doc.Lookup("memory").Int64OK()
	c.Iterations, okIterations = doc.Lookup("iterations").Int64OK()
	c.Parallelism, okParallelism = doc.Lookup("parallelism").Int32OK()
	if !okAlgo || !okSalt || !okHash || !okMemory || !okIterations || !okParallelism || saltType != bsontype.BinaryGeneric || hashType != bsontype.BinaryGeneric {
		return nil, false
	}
	c.Salt, c.Hash = bytes.Clone(c.Salt), bytes.Clone(c.Hash)
	return &c, true
}

// unpackUserRequest reads a user command member by member. It names a member the command does not
// take and a required member that is missing; a member of the wrong type is a layout refusal, and
// DecodeRingAdmin's re-encode compare refuses every other difference from the canonical layout.
func unpackUserRequest(cmd string, raw []byte) (RingAdminRequest, error) {
	elements, err := bsoncore.Document(raw).Elements()
	if err != nil {
		return RingAdminRequest{}, ringRefusal("a %s request is not a BSON document: %v", cmd, err)
	}
	present := map[string]bsoncore.Value{}
	for _, element := range elements {
		key := element.Key()
		if key == "cmd" {
			continue
		}
		if !slices.Contains(userMembers[cmd], key) {
			if cmd == AdminShowUsers {
				key = clipBare(key)
				return RingAdminRequest{}, ringRefusal("show_users takes no member besides cmd, and this request also sets %s. Remove %s", key, key)
			}
			key = clipBare(key)
			return RingAdminRequest{}, ringRefusal("%s takes only %s and cmd, and this request also sets %s. Remove %s", cmd, strings.Join(userMembers[cmd], ", "), key, key)
		}
		present[key] = element.Value()
	}
	for _, member := range userRequired[cmd] {
		if _, ok := present[member]; !ok {
			return RingAdminRequest{}, ringRefusal("%s needs %s: %s", cmd, member, userMissingFix(cmd, member))
		}
	}
	req := RingAdminRequest{Cmd: cmd}
	if cmd == AdminShowUsers {
		return req, nil
	}
	user := &RingUserRequest{}
	var ok bool
	if user.Name, ok = present["name"].StringValueOK(); !ok {
		return RingAdminRequest{}, errUserLayout(cmd)
	}
	if value, set := present["expect_seq"]; set {
		seq, ok := value.Int64OK()
		if !ok {
			return RingAdminRequest{}, errUserLayout(cmd)
		}
		user.ExpectSeq = &seq
	}
	if value, set := present["expect_drop_seq"]; set {
		if user.ExpectDropSeq, ok = value.Int64OK(); !ok {
			return RingAdminRequest{}, errUserLayout(cmd)
		}
	}
	if value, set := present["credential"]; set {
		if user.Credential, ok = readUserCredential(value); !ok {
			return RingAdminRequest{}, errUserLayout(cmd)
		}
	}
	if value, set := present["enabled"]; set {
		enabled, ok := value.BooleanOK()
		if !ok {
			return RingAdminRequest{}, errUserLayout(cmd)
		}
		user.Enabled = &enabled
	}
	if value, set := present["roles"]; set {
		grants, ok := readUserGrants(value)
		if !ok {
			return RingAdminRequest{}, errUserLayout(cmd)
		}
		user.Roles = &grants
	}
	if value, set := present["role"]; set {
		if user.Grant.Role, ok = value.StringValueOK(); !ok {
			return RingAdminRequest{}, errUserLayout(cmd)
		}
	}
	if value, set := present["db"]; set {
		if user.Grant.DB, ok = value.StringValueOK(); !ok {
			return RingAdminRequest{}, errUserLayout(cmd)
		}
	}
	req.User = user
	return req, nil
}

// checkUserRow bounds one account of a reply.
func checkUserRow(row RingUserRow) error {
	if err := checkUserText("a user reply", "name", row.Name); err != nil {
		return err
	}
	if row.Roles == nil {
		return ringRefusal("user %q of a reply has no roles member; an account with none carries an empty array", row.Name)
	}
	if err := checkUserGrants("a user reply", "roles", row.Roles); err != nil {
		return err
	}
	if row.Epoch < 0 || row.CreatedMs < 0 || row.PasswordChangedMs < 0 || row.Revision < 0 {
		return ringRefusal("user %q of a reply has epoch %d, created_ms %d, password_changed_ms %d and revision %d, each 0 or more",
			row.Name, row.Epoch, row.CreatedMs, row.PasswordChangedMs, row.Revision)
	}
	return nil
}

// checkUserReply checks the reply of a user command; zeroRest says no member of another command is set.
func checkUserReply(reply RingAdminReply, zeroRest bool) error {
	users := reply.Users
	if users == nil || !zeroRest {
		return ringRefusal("a %s reply holds only the members of entry-ring-admin-wire.md section 10.2", reply.Cmd)
	}
	if users.Revision < 0 || users.DropSeq < 0 {
		return ringRefusal("a %s reply has revision %d and user_drop_seq %d, each 0 or more", reply.Cmd, users.Revision, users.DropSeq)
	}
	switch reply.Cmd {
	case AdminShowUsers:
		if users.Users == nil || users.Name != "" || users.Result != "" || users.User != nil {
			return ringRefusal("a show_users reply is revision, user_drop_seq and users, and nothing else")
		}
		if len(users.Users) > authrules.MaxUsers {
			return ringRefusal("a show_users reply holds %d users and a store holds at most %d", len(users.Users), authrules.MaxUsers)
		}
		for _, row := range users.Users {
			if err := checkUserRow(row); err != nil {
				return err
			}
		}
		return nil
	case AdminDescribeUser:
		if users.User == nil || users.Users != nil || users.Name != "" || users.Result != "" || users.Revision != 0 {
			return ringRefusal("a describe_user reply is user_drop_seq and user, and nothing else")
		}
		return checkUserRow(*users.User)
	}
	if users.Users != nil || users.User != nil || users.DropSeq != 0 {
		return ringRefusal("a %s reply is name, revision and result, and nothing else", reply.Cmd)
	}
	if err := checkUserText(reply.Cmd, "name", users.Name); err != nil {
		return err
	}
	if len(users.Result) == 0 || len(users.Result) > ringMaxUserResult || !utf8.ValidString(users.Result) || strings.IndexFunc(users.Result, unicode.IsControl) >= 0 {
		return ringRefusal("a %s reply's result is one line of 1 to %d bytes of printable UTF-8", reply.Cmd, ringMaxUserResult)
	}
	return nil
}

func appendUserRow(doc []byte, key string, row RingUserRow) []byte {
	var r []byte
	r = bsoncore.AppendStringElement(r, "name", row.Name)
	r = appendUserGrants(r, "roles", row.Roles)
	r = bsoncore.AppendBooleanElement(r, "enabled", row.Enabled)
	r = bsoncore.AppendInt64Element(r, "epoch", row.Epoch)
	r = bsoncore.AppendInt64Element(r, "created_ms", row.CreatedMs)
	r = bsoncore.AppendInt64Element(r, "password_changed_ms", row.PasswordChangedMs)
	r = bsoncore.AppendInt64Element(r, "revision", row.Revision)
	return bsoncore.AppendDocumentElement(doc, key, bsoncore.BuildDocument(nil, r))
}

// userReplyBytes writes a checked user reply in its canonical layout.
func userReplyBytes(reply RingAdminReply) []byte {
	users := reply.Users
	doc := bsoncore.AppendStringElement(nil, "cmd", reply.Cmd)
	switch reply.Cmd {
	case AdminShowUsers:
		doc = bsoncore.AppendInt64Element(doc, "revision", users.Revision)
		doc = bsoncore.AppendInt64Element(doc, "user_drop_seq", users.DropSeq)
		rows := make([][]byte, len(users.Users))
		for i, row := range users.Users {
			rows[i] = appendUserRow(nil, strconv.Itoa(i), row)
		}
		doc = bsoncore.AppendArrayElement(doc, "users", bsoncore.BuildDocument(nil, bytes.Join(rows, nil)))
	case AdminDescribeUser:
		doc = bsoncore.AppendInt64Element(doc, "user_drop_seq", users.DropSeq)
		doc = appendUserRow(doc, "user", *users.User)
	default:
		doc = bsoncore.AppendStringElement(doc, "name", users.Name)
		doc = bsoncore.AppendInt64Element(doc, "revision", users.Revision)
		doc = bsoncore.AppendStringElement(doc, "result", users.Result)
	}
	return bsoncore.BuildDocument(nil, doc)
}

func readUserRow(value bsoncore.Value) (RingUserRow, bool) {
	doc, ok := value.DocumentOK()
	if !ok {
		return RingUserRow{}, false
	}
	var row RingUserRow
	var okName, okEnabled, okEpoch, okCreated, okChanged, okRevision, okRoles bool
	row.Name, okName = doc.Lookup("name").StringValueOK()
	row.Roles, okRoles = readUserGrants(doc.Lookup("roles"))
	row.Enabled, okEnabled = doc.Lookup("enabled").BooleanOK()
	row.Epoch, okEpoch = doc.Lookup("epoch").Int64OK()
	row.CreatedMs, okCreated = doc.Lookup("created_ms").Int64OK()
	row.PasswordChangedMs, okChanged = doc.Lookup("password_changed_ms").Int64OK()
	row.Revision, okRevision = doc.Lookup("revision").Int64OK()
	return row, okName && okRoles && okEnabled && okEpoch && okCreated && okChanged && okRevision
}

// unpackUserReply reads a user reply; DecodeRingAdminReply's re-encode compare refuses any
// difference from the canonical layout.
func unpackUserReply(cmd string, raw []byte) (RingAdminReply, error) {
	bad := func() (RingAdminReply, error) {
		return RingAdminReply{}, ringRefusal("a %s reply is not the document of entry-ring-admin-wire.md section 10.2", cmd)
	}
	doc := bsoncore.Document(raw)
	users := &RingAdminUsers{}
	var ok bool
	switch cmd {
	case AdminShowUsers:
		if users.Revision, ok = doc.Lookup("revision").Int64OK(); !ok {
			return bad()
		}
		if users.DropSeq, ok = doc.Lookup("user_drop_seq").Int64OK(); !ok {
			return bad()
		}
		array, ok := doc.Lookup("users").ArrayOK()
		if !ok {
			return bad()
		}
		values, err := array.Values()
		if err != nil || len(values) > authrules.MaxUsers {
			return bad()
		}
		users.Users = make([]RingUserRow, 0, len(values))
		for _, value := range values {
			row, ok := readUserRow(value)
			if !ok {
				return bad()
			}
			users.Users = append(users.Users, row)
		}
	case AdminDescribeUser:
		if users.DropSeq, ok = doc.Lookup("user_drop_seq").Int64OK(); !ok {
			return bad()
		}
		row, ok := readUserRow(doc.Lookup("user"))
		if !ok {
			return bad()
		}
		users.User = &row
	default:
		if users.Name, ok = doc.Lookup("name").StringValueOK(); !ok {
			return bad()
		}
		if users.Revision, ok = doc.Lookup("revision").Int64OK(); !ok {
			return bad()
		}
		if users.Result, ok = doc.Lookup("result").StringValueOK(); !ok {
			return bad()
		}
	}
	return RingAdminReply{Cmd: cmd, Users: users}, nil
}

// echoClipped quotes at most 64 bytes of a value a refusal repeats, as the cluster's appliers do,
// so the text does not grow with what the caller sent.
func echoClipped(text string) string {
	if len(text) <= 64 {
		return strconv.Quote(text)
	}
	return strconv.Quote(text[:64]) + "…"
}

// clipBare is at most 64 bytes of a member name a refusal repeats, unquoted.
func clipBare(text string) string {
	if len(text) <= 64 {
		return text
	}
	return text[:64] + "…"
}
