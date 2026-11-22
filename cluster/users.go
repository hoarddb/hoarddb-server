package cluster

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"math"
	"slices"
	"strconv"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/authrules"
)

// The user table of a ring cluster (ring user store spec 4.1): users, roles and grants are rows of
// the replicated state, changed only by the six user commands and read by each node's login path.
// The table is per cluster: nothing in it is per bucket, store or document.

// UserRequestIDLen is the length of a user command's request id: the RING frame's own request id.
const UserRequestIDLen = 16

// UserAlgo is the one credential algorithm a row holds.
const UserAlgo = "argon2id"

// PepperKIDLen is the length of a pepper key id: hex characters naming the cluster pepper.
const PepperKIDLen = 8

// UserRow is one account. Its slices are never written once the row is in a committed state: an
// applier that changes a row builds a new row and a new table (copy-on-write, spec 4.1).
type UserRow struct {
	Name string
	// Roles is sorted by (role, db) and holds each grant once.
	Roles []authrules.Grant
	Algo  string
	// Salt is the client's salt, 16 to 64 raw bytes.
	Salt []byte
	// Hash is HMAC-SHA256 of the client's Argon2id output under the cluster pepper key: 32 bytes,
	// peppered once by the node that proposed the change.
	Hash []byte
	// PepperKID names the pepper the hash was made with; always the state's PepperKID.
	PepperKID   string
	Memory      int64
	Iterations  int64
	Parallelism int32
	Enabled     bool
	// Epoch is the token revocation counter, LoginGeneration moves whenever a login must end; both
	// stay in the uint32 range the server's tokens carry.
	Epoch           int64
	LoginGeneration int64
	// CreatedMs and PasswordChangedMs are unix milliseconds the proposer stamped into the command;
	// shown to people, never used to tell accounts apart.
	CreatedMs         int64
	PasswordChangedMs int64
	// CreatedSeq is the state_seq of the commit that created the row: the account's incarnation.
	CreatedSeq int64
	// ChangedSeq is the state_seq of the commit that last changed the row: the fence (expect_seq)
	// every row-changing command presents.
	ChangedSeq int64
}

// UserRequest is one applied user command: its request id and the state_seq that applied it.
type UserRequest struct {
	ID       [UserRequestIDLen]byte
	StateSeq int64
}

// UserView is the user table as one node has applied it, read without copying the rest of the state
// (spec 6.3, B16). Its slices are shared with the committed state: a reader must never write into
// them.
type UserView struct {
	Users     []UserRow
	Requests  []UserRequest
	PepperKID string
	// DropSeq is the state's user_drop_seq: what a proposer puts in USER_CREATE's expect_drop_seq.
	DropSeq  int64
	StateSeq int64
}

// RequestApplied reports whether a user command with request id was applied while it is still
// among the remembered ones, and the state_seq that applied it.
func (v UserView) RequestApplied(id [UserRequestIDLen]byte) (int64, bool) {
	return requestApplied(v.Requests, id)
}

// User returns the row named name.
func (v UserView) User(name string) (UserRow, bool) {
	i, ok := userIndex(v.Users, name)
	if !ok {
		return UserRow{}, false
	}
	return v.Users[i], true
}

func requestApplied(requests []UserRequest, id [UserRequestIDLen]byte) (int64, bool) {
	for _, applied := range requests {
		if applied.ID == id {
			return applied.StateSeq, true
		}
	}
	return 0, false
}

func userIndex(users []UserRow, name string) (int, bool) {
	return slices.BinarySearchFunc(users, name, func(row UserRow, name string) int { return cmp.Compare(row.Name, name) })
}

// validPepperKID reports whether kid is a pepper key id: PepperKIDLen lowercase hex characters.
func validPepperKID(kid string) bool {
	if len(kid) != PepperKIDLen {
		return false
	}
	for i := 0; i < len(kid); i++ {
		if c := kid[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// checkUserRow refuses a row the state could not keep in one spelling: a field outside its range.
// Encode and decode both call it.
func checkUserRow(row UserRow) error {
	switch {
	case row.Name == "":
		return stateRefusal("a user row has an empty name")
	case row.Algo != UserAlgo:
		return differs(stateRefusal("user %s has credential algorithm %q: this build reads %q", echoClipped(row.Name), row.Algo, UserAlgo),
			"holds the value %q for a credential algorithm, which this build does not accept", row.Algo)
	case len(row.Salt) < authrules.MinSaltLen || len(row.Salt) > authrules.MaxSaltLen:
		return stateRefusal("user %s has a salt of %d bytes: a salt is %d to %d bytes", echoClipped(row.Name), len(row.Salt), authrules.MinSaltLen, authrules.MaxSaltLen)
	case len(row.Hash) != authrules.Argon2HashLen:
		return stateRefusal("user %s has a hash of %d bytes: a stored hash is %d bytes", echoClipped(row.Name), len(row.Hash), authrules.Argon2HashLen)
	case !validPepperKID(row.PepperKID):
		return stateRefusal("user %s has pepper_kid %q: it is %d lowercase hex characters", echoClipped(row.Name), row.PepperKID, PepperKIDLen)
	case row.Epoch < 0 || row.Epoch > math.MaxUint32 || row.LoginGeneration < 0 || row.LoginGeneration > math.MaxUint32:
		return stateRefusal("user %s has epoch %d and login_generation %d: both are 0 to %d", echoClipped(row.Name), row.Epoch, row.LoginGeneration, uint32(math.MaxUint32))
	case row.CreatedSeq < 1 || row.ChangedSeq < row.CreatedSeq:
		return stateRefusal("user %s has created_seq %d and changed_seq %d: a row is created by a commit (1 or more) and changed at or after it", echoClipped(row.Name), row.CreatedSeq, row.ChangedSeq)
	}
	for i, grant := range row.Roles {
		if !authrules.KnownRole(grant.Role) {
			return differs(stateRefusal("user %s holds the role %q, which is not a role: the roles are %v", echoClipped(row.Name), grant.Role, authrules.RoleNames()),
				"holds the value %q for a role, which this build does not accept", grant.Role)
		}
		if grant.DB == "" {
			return stateRefusal("user %s holds %s on an empty database name", echoClipped(row.Name), grant.Role)
		}
		if i > 0 && authrules.CompareGrants(row.Roles[i-1], grant) >= 0 {
			return stateRefusal("user %s roles are not strictly sorted by (role, db) at %s@%s: write them with EncodeClusterState", echoClipped(row.Name), grant.Role, grant.DB)
		}
	}
	return nil
}

// canonicalUsers returns the table and the request list as the state document writes them, or a
// refusal: rows sorted by name, each name once, each row's roles normalised, the requests at most
// authrules.RequestListSize and ascending by state_seq.
func canonicalUsers(st ClusterState) ([]UserRow, error) {
	if err := checkUserTable(st.Users, st.PepperKID); err != nil {
		return nil, err
	}
	users := slices.Clone(st.Users)
	slices.SortFunc(users, func(a, b UserRow) int { return cmp.Compare(a.Name, b.Name) })
	for i := range users {
		users[i].Roles = authrules.NormalizeGrants(users[i].Roles)
		if i > 0 && users[i].Name == users[i-1].Name {
			return nil, stateRefusal("user %s appears twice in the cluster state: a user name is unique", echoClipped(users[i].Name))
		}
		if err := checkUserRow(users[i]); err != nil {
			return nil, err
		}
	}
	if st.PepperKID != "" && !validPepperKID(st.PepperKID) {
		return nil, stateRefusal("cluster state pepper_kid %q is not empty and not %d lowercase hex characters", st.PepperKID, PepperKIDLen)
	}
	if err := checkUserRequests(st.UserRequests); err != nil {
		return nil, err
	}
	return users, nil
}

// checkUserTable refuses a table over the user ceiling and a row whose pepper key id is not the
// state's: every credential names the state's kid (spec 4.2), so rows and an empty kid, or two
// kids, cannot both be committed.
func checkUserTable(users []UserRow, stateKID string) error {
	if len(users) > authrules.MaxUsers {
		return stateRefusal("cluster state holds %d users and at most %d are kept", len(users), authrules.MaxUsers)
	}
	for _, row := range users {
		if row.PepperKID != stateKID {
			return stateRefusal("user %s has pepper_kid %q and the cluster state %q: every credential names the state's password key", echoClipped(row.Name), row.PepperKID, stateKID)
		}
	}
	return nil
}

func checkUserRequests(requests []UserRequest) error {
	if len(requests) > authrules.RequestListSize {
		return stateRefusal("cluster state remembers %d user requests and at most %d are kept", len(requests), authrules.RequestListSize)
	}
	for i := 1; i < len(requests); i++ {
		if requests[i].StateSeq <= requests[i-1].StateSeq {
			return stateRefusal("cluster state user_requests are not strictly ascending by state_seq at %d: the oldest is first", requests[i].StateSeq)
		}
	}
	return nil
}

// appendUsers writes the three user fields of the state document, after settings.
func appendUsers(body []byte, users []UserRow, pepperKID string, dropSeq int64, requests []UserRequest) []byte {
	rows := make([][]byte, len(users))
	for i, u := range users {
		roles := make([][]byte, len(u.Roles))
		for j, grant := range u.Roles {
			var g []byte
			g = bsoncore.AppendStringElement(g, "role", grant.Role)
			g = bsoncore.AppendStringElement(g, "db", grant.DB)
			roles[j] = bsoncore.AppendDocumentElement(nil, strconv.Itoa(j), bsoncore.BuildDocument(nil, g))
		}
		var row []byte
		row = bsoncore.AppendStringElement(row, "name", u.Name)
		row = bsoncore.AppendArrayElement(row, "roles", bsoncore.BuildDocument(nil, bytes.Join(roles, nil)))
		row = bsoncore.AppendStringElement(row, "algo", u.Algo)
		row = bsoncore.AppendBinaryElement(row, "salt", bsontype.BinaryGeneric, u.Salt)
		row = bsoncore.AppendBinaryElement(row, "hash", bsontype.BinaryGeneric, u.Hash)
		row = bsoncore.AppendStringElement(row, "pepper_kid", u.PepperKID)
		row = bsoncore.AppendInt64Element(row, "memory", u.Memory)
		row = bsoncore.AppendInt64Element(row, "iterations", u.Iterations)
		row = bsoncore.AppendInt32Element(row, "parallelism", u.Parallelism)
		row = bsoncore.AppendBooleanElement(row, "enabled", u.Enabled)
		row = bsoncore.AppendInt64Element(row, "epoch", u.Epoch)
		row = bsoncore.AppendInt64Element(row, "login_generation", u.LoginGeneration)
		row = bsoncore.AppendInt64Element(row, "created_ms", u.CreatedMs)
		row = bsoncore.AppendInt64Element(row, "password_changed_ms", u.PasswordChangedMs)
		row = bsoncore.AppendInt64Element(row, "created_seq", u.CreatedSeq)
		row = bsoncore.AppendInt64Element(row, "changed_seq", u.ChangedSeq)
		rows[i] = bsoncore.AppendDocumentElement(nil, strconv.Itoa(i), bsoncore.BuildDocument(nil, row))
	}
	body = bsoncore.AppendArrayElement(body, "users", bsoncore.BuildDocument(nil, bytes.Join(rows, nil)))
	body = bsoncore.AppendStringElement(body, "pepper_kid", pepperKID)
	body = bsoncore.AppendInt64Element(body, "user_drop_seq", dropSeq)
	applied := make([][]byte, len(requests))
	for i, r := range requests {
		var row []byte
		row = bsoncore.AppendBinaryElement(row, "request", bsontype.BinaryGeneric, r.ID[:])
		row = bsoncore.AppendInt64Element(row, "state_seq", r.StateSeq)
		applied[i] = bsoncore.AppendDocumentElement(nil, strconv.Itoa(i), bsoncore.BuildDocument(nil, row))
	}
	return bsoncore.AppendArrayElement(body, "user_requests", bsoncore.BuildDocument(nil, bytes.Join(applied, nil)))
}

// decodeUsers reads the three user fields of the state document into st.
func decodeUsers(top *stateFields, st *ClusterState) error {
	err := top.rows("users", bson.TypeEmbeddedDocument, func(i int, raw []byte) error {
		row, err := decodeUserRow(raw)
		if err != nil {
			return err
		}
		if i > 0 && row.Name <= st.Users[i-1].Name {
			return stateRefusal("cluster state users are not strictly sorted by name, each name once, at %s: write them with EncodeClusterState", echoClipped(row.Name))
		}
		st.Users = append(st.Users, row)
		return nil
	})
	if err != nil {
		return err
	}
	if st.PepperKID, err = top.str("pepper_kid"); err != nil {
		return err
	}
	if st.PepperKID != "" && !validPepperKID(st.PepperKID) {
		return stateRefusal("cluster state pepper_kid %q is not empty and not %d lowercase hex characters", st.PepperKID, PepperKIDLen)
	}
	if err := checkUserTable(st.Users, st.PepperKID); err != nil {
		return err
	}
	if st.UserDropSeq, err = top.int64("user_drop_seq"); err != nil {
		return err
	}
	if st.UserDropSeq < 0 || st.UserDropSeq > st.StateSeq {
		return stateRefusal("cluster state user_drop_seq %d is outside 0 to the state's state_seq %d", st.UserDropSeq, st.StateSeq)
	}
	err = top.rows("user_requests", bson.TypeEmbeddedDocument, func(i int, raw []byte) error {
		if i >= authrules.RequestListSize {
			return stateRefusal("cluster state remembers more than %d user requests", authrules.RequestListSize)
		}
		f, err := newStateFields("user request", bsoncore.Document(raw))
		if err != nil {
			return err
		}
		id, err := f.binary("request", UserRequestIDLen)
		if err != nil {
			return err
		}
		var applied UserRequest
		copy(applied.ID[:], id)
		if applied.StateSeq, err = f.int64("state_seq"); err != nil {
			return err
		}
		if err := f.finish(); err != nil {
			return err
		}
		st.UserRequests = append(st.UserRequests, applied)
		return nil
	})
	if err != nil {
		return err
	}
	return checkUserRequests(st.UserRequests)
}

func decodeUserRow(raw []byte) (UserRow, error) {
	var row UserRow
	f, err := newStateFields("user", bsoncore.Document(raw))
	if err != nil {
		return row, err
	}
	if row.Name, err = f.str("name"); err != nil {
		return row, err
	}
	err = f.rows("roles", bson.TypeEmbeddedDocument, func(_ int, raw []byte) error {
		g, err := newStateFields("user role", bsoncore.Document(raw))
		if err != nil {
			return err
		}
		var grant authrules.Grant
		if grant.Role, err = g.str("role"); err != nil {
			return err
		}
		if grant.DB, err = g.str("db"); err != nil {
			return err
		}
		row.Roles = append(row.Roles, grant)
		return g.finish()
	})
	if err != nil {
		return row, err
	}
	if row.Algo, err = f.str("algo"); err != nil {
		return row, err
	}
	if row.Salt, err = f.binaryBetween("salt", authrules.MinSaltLen, authrules.MaxSaltLen); err != nil {
		return row, err
	}
	if row.Hash, err = f.binary("hash", authrules.Argon2HashLen); err != nil {
		return row, err
	}
	if row.PepperKID, err = f.str("pepper_kid"); err != nil {
		return row, err
	}
	if row.Memory, err = f.int64("memory"); err != nil {
		return row, err
	}
	if row.Iterations, err = f.int64("iterations"); err != nil {
		return row, err
	}
	if row.Parallelism, err = f.int32("parallelism"); err != nil {
		return row, err
	}
	if row.Enabled, err = f.boolean("enabled"); err != nil {
		return row, err
	}
	for _, field := range []struct {
		key string
		dst *int64
	}{
		{"epoch", &row.Epoch}, {"login_generation", &row.LoginGeneration}, {"created_ms", &row.CreatedMs},
		{"password_changed_ms", &row.PasswordChangedMs}, {"created_seq", &row.CreatedSeq}, {"changed_seq", &row.ChangedSeq},
	} {
		if *field.dst, err = f.int64(field.key); err != nil {
			return row, err
		}
	}
	if err := f.finish(); err != nil {
		return row, err
	}
	return row, checkUserRow(row)
}

// binaryBetween returns a generic-subtype binary of min to max bytes, copied.
func (f *stateFields) binaryBetween(key string, minLen, maxLen int) ([]byte, error) {
	v, err := f.take(key, bson.TypeBinary)
	if err != nil {
		return nil, err
	}
	if len(v.Data) < 5 || int(binary.LittleEndian.Uint32(v.Data)) != len(v.Data)-5 {
		return nil, stateRefusal("%s field %q has a binary length prefix that disagrees with its bytes", f.where, key)
	}
	if subtype := v.Data[4]; subtype != 0 {
		return nil, stateRefusal("%s field %q has binary subtype 0x%02x, want 0x00", f.where, key, subtype)
	}
	payload := v.Data[5:]
	if len(payload) < minLen || len(payload) > maxLen {
		return nil, stateRefusal("%s field %q is %d bytes, want %d to %d", f.where, key, len(payload), minLen, maxLen)
	}
	return slices.Clone(payload), nil
}
