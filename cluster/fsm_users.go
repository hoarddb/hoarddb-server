package cluster

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strconv"
	"unicode"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/authrules"
	"github.com/hoarddb/hoarddb-server/namepolicy"
)

// The six user commands (ring user store spec 4.2). Every refusal below is meant for the leader's
// dry run: a committed entry refused for anything but STATE_MOVED stops the node (clusterFSM.Apply),
// so an applier is a pure function of the state, the command and the authrules constants, the same
// on the leader's copy and on every follower. Appliers never write into the table they were given:
// clone shares it, so each builds a new slice and a new row (copy-on-write, spec 4.1).
const (
	CommandUserBootstrap byte = 0x11
	CommandUserCreate    byte = 0x12
	CommandUserAlter     byte = 0x13
	CommandUserDrop      byte = 0x14
	CommandUserGrant     byte = 0x15
	CommandUserRevoke    byte = 0x16
)

// UserCommandKind reports whether kind is one of the six user commands.
func UserCommandKind(kind byte) bool {
	return kind >= CommandUserBootstrap && kind <= CommandUserRevoke
}

// UserCredential is a credential as a user command carries it: the client's salt, the hash the
// proposing node peppered once, the Argon2id work factor and the pepper's key id.
type UserCredential struct {
	Salt        []byte
	Hash        []byte
	Memory      int64
	Iterations  int64
	Parallelism int32
	PepperKID   string
}

// UserBootstrapBody is USER_BOOTSTRAP: the first administrator, root on every database, made only
// on an empty table. Its credential's kid becomes the state's.
type UserBootstrapBody struct {
	Request    [UserRequestIDLen]byte
	Name       string
	Credential UserCredential
	CreatedMs  int64
}

// UserCreateBody is USER_CREATE. By and BySeq name the session's account: its name and the
// created_seq of the row it logged in as. ExpectDropSeq is the user_drop_seq of the table the
// proposer read (UserView.DropSeq).
type UserCreateBody struct {
	Request       [UserRequestIDLen]byte
	By            string
	BySeq         int64
	Name          string
	ExpectDropSeq int64
	Roles         []authrules.Grant
	Credential    UserCredential
	Enabled       bool
	CreatedMs     int64
}

// UserAlterBody is USER_ALTER: a new credential, an enabled flag, a whole role list, or any of
// them. PasswordChangedMs is carried only with a credential.
type UserAlterBody struct {
	Request           [UserRequestIDLen]byte
	By                string
	BySeq             int64
	Name              string
	ExpectSeq         int64
	Credential        *UserCredential
	PasswordChangedMs int64
	Enabled           *bool
	Roles             *[]authrules.Grant
}

// UserDropBody is USER_DROP.
type UserDropBody struct {
	Request   [UserRequestIDLen]byte
	By        string
	BySeq     int64
	Name      string
	ExpectSeq int64
}

// UserRoleBody is USER_GRANT or USER_REVOKE: one grant.
type UserRoleBody struct {
	Request   [UserRequestIDLen]byte
	By        string
	BySeq     int64
	Name      string
	ExpectSeq int64
	Grant     authrules.Grant
}

func appendCredential(doc []byte, c UserCredential) []byte {
	var cred []byte
	cred = bsoncore.AppendStringElement(cred, "algo", UserAlgo)
	cred = bsoncore.AppendBinaryElement(cred, "salt", bsontype.BinaryGeneric, c.Salt)
	cred = bsoncore.AppendBinaryElement(cred, "hash", bsontype.BinaryGeneric, c.Hash)
	cred = bsoncore.AppendInt64Element(cred, "memory", c.Memory)
	cred = bsoncore.AppendInt64Element(cred, "iterations", c.Iterations)
	cred = bsoncore.AppendInt32Element(cred, "parallelism", c.Parallelism)
	cred = bsoncore.AppendStringElement(cred, "pepper_kid", c.PepperKID)
	return bsoncore.AppendDocumentElement(doc, "credential", bsoncore.BuildDocument(nil, cred))
}

func appendRoles(doc []byte, roles []authrules.Grant) []byte {
	elems := make([][]byte, len(roles))
	for i, grant := range roles {
		var g []byte
		g = bsoncore.AppendStringElement(g, "role", grant.Role)
		g = bsoncore.AppendStringElement(g, "db", grant.DB)
		elems[i] = bsoncore.AppendDocumentElement(nil, strconv.Itoa(i), bsoncore.BuildDocument(nil, g))
	}
	return bsoncore.AppendArrayElement(doc, "roles", bsoncore.BuildDocument(nil, bytes.Join(elems, nil)))
}

func appendActor(doc []byte, request [UserRequestIDLen]byte, by string, bySeq int64, name string) []byte {
	doc = bsoncore.AppendBinaryElement(doc, "request", bsontype.BinaryGeneric, request[:])
	doc = bsoncore.AppendStringElement(doc, "by", by)
	doc = bsoncore.AppendInt64Element(doc, "by_seq", bySeq)
	return bsoncore.AppendStringElement(doc, "name", name)
}

// Encode is the BSON body of the command.
func (b UserBootstrapBody) Encode() []byte {
	var doc []byte
	doc = bsoncore.AppendBinaryElement(doc, "request", bsontype.BinaryGeneric, b.Request[:])
	doc = bsoncore.AppendStringElement(doc, "name", b.Name)
	doc = appendCredential(doc, b.Credential)
	doc = bsoncore.AppendInt64Element(doc, "created_ms", b.CreatedMs)
	return bsoncore.BuildDocument(nil, doc)
}

// Encode is the BSON body of the command.
func (b UserCreateBody) Encode() []byte {
	doc := appendActor(nil, b.Request, b.By, b.BySeq, b.Name)
	doc = bsoncore.AppendInt64Element(doc, "expect_drop_seq", b.ExpectDropSeq)
	doc = appendRoles(doc, b.Roles)
	doc = appendCredential(doc, b.Credential)
	doc = bsoncore.AppendBooleanElement(doc, "enabled", b.Enabled)
	doc = bsoncore.AppendInt64Element(doc, "created_ms", b.CreatedMs)
	return bsoncore.BuildDocument(nil, doc)
}

// Encode is the BSON body of the command.
func (b UserAlterBody) Encode() []byte {
	doc := appendActor(nil, b.Request, b.By, b.BySeq, b.Name)
	doc = bsoncore.AppendInt64Element(doc, "expect_seq", b.ExpectSeq)
	if b.Credential != nil {
		doc = appendCredential(doc, *b.Credential)
		doc = bsoncore.AppendInt64Element(doc, "password_changed_ms", b.PasswordChangedMs)
	}
	if b.Enabled != nil {
		doc = bsoncore.AppendBooleanElement(doc, "enabled", *b.Enabled)
	}
	if b.Roles != nil {
		doc = appendRoles(doc, *b.Roles)
	}
	return bsoncore.BuildDocument(nil, doc)
}

// Encode is the BSON body of the command.
func (b UserDropBody) Encode() []byte {
	doc := appendActor(nil, b.Request, b.By, b.BySeq, b.Name)
	doc = bsoncore.AppendInt64Element(doc, "expect_seq", b.ExpectSeq)
	return bsoncore.BuildDocument(nil, doc)
}

// Encode is the BSON body of the command.
func (b UserRoleBody) Encode() []byte {
	doc := appendActor(nil, b.Request, b.By, b.BySeq, b.Name)
	doc = bsoncore.AppendInt64Element(doc, "expect_seq", b.ExpectSeq)
	doc = bsoncore.AppendStringElement(doc, "role", b.Grant.Role)
	doc = bsoncore.AppendStringElement(doc, "db", b.Grant.DB)
	return bsoncore.BuildDocument(nil, doc)
}

// UserCommandRequest reads the request id of a user command body, for the proposer's
// committed-effect check; ok is false for a body that carries none.
func UserCommandRequest(body []byte) (id [UserRequestIDLen]byte, ok bool) {
	subtype, data, found := bsoncore.Document(body).Lookup("request").BinaryOK()
	if !found || subtype != bsontype.BinaryGeneric || len(data) != UserRequestIDLen {
		return id, false
	}
	copy(id[:], data)
	return id, true
}

// userCommand is what every user kind but USER_BOOTSTRAP reads besides its own fields.
type userCommand struct {
	request [UserRequestIDLen]byte
	by      string
	bySeq   int64
	name    string
}

// statement is how a refusal names the command, as a person typed it.
type statement string

func (f *commandFields) requestID() ([UserRequestIDLen]byte, error) {
	var id [UserRequestIDLen]byte
	v, ok, err := f.find("request", bson.TypeBinary)
	if err != nil {
		return id, err
	}
	if !ok {
		return id, f.missing("request")
	}
	if len(v.Data) < 5 || v.Data[4] != 0 || len(v.Data)-5 != UserRequestIDLen {
		return id, commandRefusal(CodeInvalidArgument, "%s field \"request\" must be %d bytes of generic binary: the RING frame's request id", f.where, UserRequestIDLen)
	}
	copy(id[:], v.Data[5:])
	return id, nil
}

// binaryBetween reads a generic binary of minLen to maxLen bytes, copied.
func (f *commandFields) binaryBetween(key string, minLen, maxLen int) ([]byte, error) {
	v, ok, err := f.find(key, bson.TypeBinary)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, f.missing(key)
	}
	if len(v.Data) < 5 || v.Data[4] != 0 || len(v.Data)-5 < minLen || len(v.Data)-5 > maxLen {
		return nil, commandRefusal(CodeInvalidArgument, "%s field %q must be %d to %d bytes of generic binary", f.where, key, minLen, maxLen)
	}
	return slices.Clone(v.Data[5:]), nil
}

func (f *commandFields) actor() (userCommand, error) {
	var c userCommand
	var err error
	if c.request, err = f.requestID(); err != nil {
		return c, err
	}
	if c.by, err = f.str("by"); err != nil {
		return c, err
	}
	if c.bySeq, err = f.int64("by_seq"); err != nil {
		return c, err
	}
	c.name, err = f.str("name")
	return c, err
}

// credential reads the credential document: its shape only (types, lengths, the kid's form). The
// window and the kid's value are judged later, in the order of spec 4.2.
func (f *commandFields) credential() (UserCredential, error) {
	var c UserCredential
	doc, err := f.document("credential")
	if err != nil {
		return c, err
	}
	algo, err := doc.str("algo")
	if err != nil {
		return c, err
	}
	if c.Salt, err = doc.binaryBetween("salt", authrules.MinSaltLen, authrules.MaxSaltLen); err != nil {
		return c, err
	}
	if c.Hash, err = doc.binaryBetween("hash", authrules.Argon2HashLen, authrules.Argon2HashLen); err != nil {
		return c, err
	}
	if c.Memory, err = doc.int64("memory"); err != nil {
		return c, err
	}
	if c.Iterations, err = doc.int64("iterations"); err != nil {
		return c, err
	}
	if c.Parallelism, err = doc.int32("parallelism"); err != nil {
		return c, err
	}
	if c.PepperKID, err = doc.str("pepper_kid"); err != nil {
		return c, err
	}
	if err := doc.finish(); err != nil {
		return c, err
	}
	if algo != UserAlgo {
		return c, commandRefusal(CodeInvalidArgument, "%s credential algorithm is %q: a cluster keeps %q only", f.where, algo, UserAlgo)
	}
	if !validPepperKID(c.PepperKID) {
		return c, commandRefusal(CodeInvalidArgument, "%s credential pepper_kid %q is not %d lowercase hex characters", f.where, c.PepperKID, PepperKIDLen)
	}
	return c, nil
}

func (f *commandFields) roles() ([]authrules.Grant, error) {
	rows, err := f.documents("roles")
	if err != nil {
		return nil, err
	}
	roles := make([]authrules.Grant, 0, len(rows))
	for _, row := range rows {
		var grant authrules.Grant
		if grant.Role, err = row.str("role"); err != nil {
			return nil, err
		}
		if grant.DB, err = row.str("db"); err != nil {
			return nil, err
		}
		if err := row.finish(); err != nil {
			return nil, err
		}
		roles = append(roles, grant)
	}
	return roles, nil
}

// refuseRepeat is the first check after the shape: a request the cluster remembers was applied, so
// this is a retry, and applying it again could undo a later change (spec 4.2, B1).
func (s *ClusterState) refuseRepeat(request [UserRequestIDLen]byte) error {
	if seq, ok := requestApplied(s.UserRequests, request); ok {
		return conflict("user command request %s was already applied at state_seq %d, so this repeat was not applied again", hex.EncodeToString(request[:]), seq)
	}
	return nil
}

// usernamePolicy is the name rule of a new account: 1 to namepolicy.MaxBytes bytes, no control
// character (a terminal acts on one when it lists users), and not the node identity's name.
func usernamePolicy(what statement, name string) error {
	const fix = "Use a printable name of at most 255 bytes"
	switch {
	case name == "":
		return commandRefusal(CodeInvalidArgument, "%s was refused: a user needs a name. %s", what, fix)
	case name == authrules.NodeUser:
		return commandRefusal(CodeReservedName, "%s was refused: %q is the name a node presents to its peers, so no account may take it. Choose another name", what, authrules.NodeUser)
	case len(name) > namepolicy.MaxBytes:
		return commandRefusal(CodeInvalidArgument, "%s was refused: the name is %d bytes. %s", what, len(name), fix)
	}
	for offset, character := range name {
		if unicode.IsControl(character) {
			return commandRefusal(CodeInvalidArgument, "%s was refused: the name holds a control character (U+%04X at byte %d). %s", what, character, offset, fix)
		}
	}
	return nil
}

// grantPolicy refuses a role that is not a role and a database that is neither every database nor
// a valid database name.
func grantPolicy(what statement, grants []authrules.Grant) error {
	for _, grant := range grants {
		if !authrules.KnownRole(grant.Role) {
			hint := ""
			if current, renamed := authrules.RenamedRole(grant.Role); renamed {
				hint = fmt.Sprintf(" (the role is called %q)", current)
			}
			return commandRefusal(CodeInvalidArgument, "%s was refused: %s is not a role%s. The roles are %v", what, echoClipped(grant.Role), hint, authrules.RoleNames())
		}
		if grant.DB == authrules.AllDatabases {
			continue
		}
		if violation := namepolicy.CheckDatabase(grant.DB, nil); violation != nil {
			reason := map[namepolicy.Rule]string{
				namepolicy.RuleEmpty: "the name is empty", namepolicy.RuleReserved: "the name is reserved for internal use",
				namepolicy.RuleTooLong: fmt.Sprintf("the name is over %d bytes", namepolicy.MaxBytes),
			}[violation.Rule]
			if reason == "" {
				reason = "a database name has no '/', '\\' or NUL and is not '.' or '..'"
			}
			return commandRefusal(CodeInvalidArgument, "%s was refused: %s@%s names no valid database: %s. Name a database, or * for every one", what, grant.Role, echoClipped(grant.DB), reason)
		}
	}
	return nil
}

// credentialPolicy judges a credential against the window every member accepts and the cluster's
// pepper key id. stateKID is "" only for USER_BOOTSTRAP, which sets it.
func credentialPolicy(what statement, c UserCredential, stateKID string) error {
	if outside := authrules.CredentialWindowViolation(c.Memory, c.Iterations, int64(c.Parallelism), len(c.Salt), len(c.Hash)); outside != "" {
		return commandRefusal(CodeInvalidArgument, "%s was refused: the credential is outside what every member accepts: %s. Derive it again with the hoarddb CLI or a current driver", what, outside)
	}
	if stateKID != "" && c.PepperKID != stateKID {
		return conflict("%s was refused: the credential was peppered with key %s and the cluster's password key is %s, so no member could check it. The node that sent it holds the wrong key; send the change through another node", what, c.PepperKID, stateKID)
	}
	return nil
}

// grantorRow is the first half of "by may not" (spec 4.2): the session's account must still exist
// as the same account (its created_seq, B17), be enabled and hold root or user-manager on every
// database, judged against the committed row. It returns that row for the ceiling check.
func (s *ClusterState) grantorRow(what statement, c userCommand) (UserRow, error) {
	i, ok := userIndex(s.Users, c.by)
	if ok && s.Users[i].CreatedSeq == c.bySeq && authrules.CanAdministerUsers(s.Users[i].Enabled, s.Users[i].Roles) {
		return s.Users[i], nil
	}
	return UserRow{}, commandRefusal(CodePermissionDenied, "%s was refused: the account that asked (%s) no longer holds user-manager on every database, or was dropped and created again. Ask an account that holds it, or run 'grant role user-manager@* to %s' from one", what, echoClipped(c.by), c.by)
}

// checkCeiling is the second half: the change stays under the committed grantor's ceiling, every
// grant it gives and every grant the target holds being one the grantor holds.
func checkCeiling(what statement, grantor UserRow, target, given []authrules.Grant) error {
	if breach, ok := authrules.CheckCeiling(grantor.Roles, target, given); !ok {
		if breach.Given {
			return commandRefusal(CodePermissionDenied, "%s was refused: %s cannot give %s@%s, because a user administrator who is not root gives only roles it holds itself. Ask a root user", what, echoClipped(grantor.Name), breach.Grant.Role, breach.Grant.DB)
		}
		return commandRefusal(CodePermissionDenied, "%s was refused: %s cannot change an account that holds %s@%s, because a user administrator who is not root changes only accounts whose roles it holds itself. Ask a root user", what, echoClipped(grantor.Name), breach.Grant.Role, breach.Grant.DB)
	}
	return nil
}

// targetRow finds the row a command changes and checks its fence: absent is NOT_FOUND, and a row
// changed since the proposer read it (expect_seq != changed_seq) is CONFLICT, so a retry can never
// undo a later change, across a drop and re-create too, since a state_seq is never reused.
func (s *ClusterState) targetRow(what statement, name string, expectSeq int64) (int, error) {
	i, ok := userIndex(s.Users, name)
	if !ok {
		return 0, commandRefusal(CodeNotFound, "%s was not applied: there is no user %s. Run 'show users' to list the users", what, echoClipped(name))
	}
	if row := s.Users[i]; row.ChangedSeq != expectSeq {
		return 0, conflict("%s was not applied: the user changed since this node read it (revision %d, now %d). Run 'user describe %s' and repeat the change against what it shows", what, expectSeq, row.ChangedSeq, name)
	}
	return i, nil
}

// adminsExcept counts the accounts other than name that pass the last-administrator predicate.
func adminsExcept(users []UserRow, name string) int {
	count := 0
	for _, row := range users {
		if row.Name != name && authrules.CanAdministerUsers(row.Enabled, row.Roles) {
			count++
		}
	}
	return count
}

// guardLastAdmin refuses a change that leaves no enabled account able to manage users.
func (s *ClusterState) guardLastAdmin(what statement, before UserRow, afterEnabled bool, afterRoles []authrules.Grant, dropped bool) error {
	if !authrules.CanAdministerUsers(before.Enabled, before.Roles) {
		return nil
	}
	if !dropped && authrules.CanAdministerUsers(afterEnabled, afterRoles) {
		return nil
	}
	if adminsExcept(s.Users, before.Name) > 0 {
		return nil
	}
	// INVALID_ARGUMENT, not CONFLICT: a retry is refused the same way (ring-user-store.md 4.2).
	return commandRefusal(CodeInvalidArgument, "%s was refused: it is the last enabled account that can manage users (root, or user-manager on every database). Give another enabled account that authority first, for example 'grant role user-manager@* to ops', then run the statement again", what)
}

// refuseSaltInUse refuses a salt any row holds already, the changed row's own included: one leaked
// salt must not become a table.
func (s *ClusterState) refuseSaltInUse(what statement, salt []byte) error {
	if slices.ContainsFunc(s.Users, func(row UserRow) bool { return bytes.Equal(row.Salt, salt) }) {
		return conflict("%s was refused: the credential's salt is already in use by another credential. Derive the credential again with a fresh random salt", what)
	}
	return nil
}

// refuseNegativeTime refuses a time stamp before 1970: the proposer stamps unix milliseconds from
// its clock, so a negative one is a malformed body.
func refuseNegativeTime(where, key string, ms int64) error {
	if ms < 0 {
		return commandRefusal(CodeInvalidArgument, "%s field %q is %d: it is unix milliseconds from the proposer's clock, never negative", where, key, ms)
	}
	return nil
}

func bumped(counter int64, what statement) (int64, error) {
	if counter >= math.MaxUint32 {
		return 0, conflict("%s was refused: the account's revocation counter is at its limit; drop the account and create it again", what)
	}
	return counter + 1, nil
}

// commitUserChange replaces the table with users and remembers request, in new slices: the ones
// the state held are shared with readers and must never be written.
func (s *ClusterState) commitUserChange(users []UserRow, request [UserRequestIDLen]byte) {
	s.Users = users
	from := max(0, len(s.UserRequests)+1-authrules.RequestListSize)
	requests := make([]UserRequest, 0, len(s.UserRequests)-from+1)
	requests = append(requests, s.UserRequests[from:]...)
	s.UserRequests = append(requests, UserRequest{ID: request, StateSeq: s.StateSeq + 1})
}

// replaceRow returns a copy of the table with row at i.
func replaceRow(users []UserRow, i int, row UserRow) []UserRow {
	next := slices.Clone(users)
	next[i] = row
	return next
}

// copyRow returns row with its own slices, so a change to the copy never reaches the committed row.
func copyRow(row UserRow) UserRow {
	row.Roles = slices.Clone(row.Roles)
	row.Salt = slices.Clone(row.Salt)
	row.Hash = slices.Clone(row.Hash)
	return row
}

func (s *ClusterState) applyUserBootstrap(f *commandFields) error {
	request, err := f.requestID()
	if err != nil {
		return err
	}
	name, err := f.str("name")
	if err != nil {
		return err
	}
	cred, err := f.credential()
	if err != nil {
		return err
	}
	createdMs, err := f.int64("created_ms")
	if err != nil {
		return err
	}
	if err := refuseNegativeTime(f.where, "created_ms", createdMs); err != nil {
		return err
	}
	if err := f.finish(); err != nil {
		return err
	}
	what := statement(fmt.Sprintf("create the first administrator %s", echoClipped(name)))
	if err := s.refuseRepeat(request); err != nil {
		return err
	}
	if err := usernamePolicy(what, name); err != nil {
		return err
	}
	if err := credentialPolicy(what, cred, ""); err != nil {
		return err
	}
	if len(s.Members) == 0 {
		return commandRefusal(CodeInvalidArgument, "%s was refused: the cluster is not bootstrapped yet, so it has no state to hold users", what)
	}
	if len(s.Users) != 0 || s.PepperKID != "" {
		return conflict("%s was refused: this cluster already has users. Log in as an existing administrator", what)
	}
	row := UserRow{
		Name: name, Roles: []authrules.Grant{{Role: authrules.RoleRoot, DB: authrules.AllDatabases}}, Algo: UserAlgo,
		Salt: cred.Salt, Hash: cred.Hash, PepperKID: cred.PepperKID, Memory: cred.Memory, Iterations: cred.Iterations,
		Parallelism: cred.Parallelism, Enabled: true, CreatedMs: createdMs, PasswordChangedMs: createdMs,
		CreatedSeq: s.StateSeq + 1, ChangedSeq: s.StateSeq + 1,
	}
	s.PepperKID = cred.PepperKID
	s.commitUserChange([]UserRow{row}, request)
	return nil
}

func (s *ClusterState) applyUserCreate(f *commandFields) error {
	c, err := f.actor()
	if err != nil {
		return err
	}
	expectDropSeq, err := f.int64("expect_drop_seq")
	if err != nil {
		return err
	}
	roles, err := f.roles()
	if err != nil {
		return err
	}
	cred, err := f.credential()
	if err != nil {
		return err
	}
	enabled, err := f.boolean("enabled")
	if err != nil {
		return err
	}
	createdMs, err := f.int64("created_ms")
	if err != nil {
		return err
	}
	if err := refuseNegativeTime(f.where, "created_ms", createdMs); err != nil {
		return err
	}
	if err := f.finish(); err != nil {
		return err
	}
	what := statement(fmt.Sprintf("create user %s", echoClipped(c.name)))
	roles = authrules.NormalizeGrants(roles)
	if err := s.refuseRepeat(c.request); err != nil {
		return err
	}
	if err := usernamePolicy(what, c.name); err != nil {
		return err
	}
	if err := grantPolicy(what, roles); err != nil {
		return err
	}
	if err := credentialPolicy(what, cred, s.PepperKID); err != nil {
		return err
	}
	grantor, err := s.grantorRow(what, c)
	if err != nil {
		return err
	}
	at, exists := userIndex(s.Users, c.name)
	if exists {
		return &Error{Code: CodeUserExists, Message: CodeUserExists.Text(CondUserExists, map[string]string{"user": clipName(c.name)})}
	}
	// The create-side fence (owner ruling 2026-11-20, review R3): a create built before a later
	// drop is refused at any distance, so a retry of a create whose account was dropped since
	// never brings it back once its request id has left the list. A create built before an
	// unrelated drop is refused too; the proposer reads again.
	if expectDropSeq != s.UserDropSeq {
		return conflict("%s was not applied: a user was dropped since this node read the users (drop at state_seq %d, read at %d). Run 'user describe %s' to see whether it exists, then run the statement again", what, s.UserDropSeq, expectDropSeq, c.name)
	}
	if err := checkCeiling(what, grantor, nil, roles); err != nil {
		return err
	}
	if err := s.refuseSaltInUse(what, cred.Salt); err != nil {
		return err
	}
	if len(s.Users) >= authrules.MaxUsers {
		return commandRefusal(CodeInvalidArgument, "%s was refused: user limit reached: the cluster holds %d users, the most it keeps. Drop an account first", what, authrules.MaxUsers)
	}
	row := UserRow{
		Name: c.name, Roles: roles, Algo: UserAlgo, Salt: cred.Salt, Hash: cred.Hash, PepperKID: cred.PepperKID,
		Memory: cred.Memory, Iterations: cred.Iterations, Parallelism: cred.Parallelism, Enabled: enabled,
		CreatedMs: createdMs, PasswordChangedMs: createdMs, CreatedSeq: s.StateSeq + 1, ChangedSeq: s.StateSeq + 1,
	}
	users := make([]UserRow, 0, len(s.Users)+1)
	users = append(users, s.Users[:at]...)
	users = append(users, row)
	users = append(users, s.Users[at:]...)
	s.commitUserChange(users, c.request)
	return nil
}

func (s *ClusterState) applyUserAlter(f *commandFields) error {
	c, err := f.actor()
	if err != nil {
		return err
	}
	expectSeq, err := f.int64("expect_seq")
	if err != nil {
		return err
	}
	var cred *UserCredential
	var changedMs int64
	if _, has := f.vals["credential"]; has {
		read, err := f.credential()
		if err != nil {
			return err
		}
		cred = &read
		if changedMs, err = f.int64("password_changed_ms"); err != nil {
			return err
		}
		if err := refuseNegativeTime(f.where, "password_changed_ms", changedMs); err != nil {
			return err
		}
	}
	var enabled *bool
	if _, has := f.vals["enabled"]; has {
		flag, err := f.boolean("enabled")
		if err != nil {
			return err
		}
		enabled = &flag
	}
	var roles []authrules.Grant
	replaceRoles := false
	if _, has := f.vals["roles"]; has {
		if roles, err = f.roles(); err != nil {
			return err
		}
		roles, replaceRoles = authrules.NormalizeGrants(roles), true
	}
	if err := f.finish(); err != nil {
		return err
	}
	what := statement(fmt.Sprintf("alter user %s", echoClipped(c.name)))
	if cred == nil && enabled == nil && !replaceRoles {
		return commandRefusal(CodeInvalidArgument, "%s carries no credential, enabled flag or roles, so there is nothing to change", what)
	}
	if err := s.refuseRepeat(c.request); err != nil {
		return err
	}
	if replaceRoles {
		if err := grantPolicy(what, roles); err != nil {
			return err
		}
	}
	if cred != nil {
		if err := credentialPolicy(what, *cred, s.PepperKID); err != nil {
			return err
		}
	}
	grantor, err := s.grantorRow(what, c)
	if err != nil {
		return err
	}
	i, err := s.targetRow(what, c.name, expectSeq)
	if err != nil {
		return err
	}
	before := s.Users[i]
	if err := checkCeiling(what, grantor, before.Roles, roles); err != nil {
		return err
	}
	row := copyRow(before)
	if replaceRoles {
		row.Roles = roles
	}
	if enabled != nil {
		row.Enabled = *enabled
	}
	if err := s.guardLastAdmin(what, before, row.Enabled, row.Roles, false); err != nil {
		return err
	}
	if cred != nil {
		if err := s.refuseSaltInUse(what, cred.Salt); err != nil {
			return err
		}
		row.Salt, row.Hash, row.PepperKID = cred.Salt, cred.Hash, cred.PepperKID
		row.Memory, row.Iterations, row.Parallelism = cred.Memory, cred.Iterations, cred.Parallelism
		row.PasswordChangedMs = changedMs
	}
	// Today's rules (server AlterUser, spec 4.2 F11): a new credential or a disable ends every
	// login, a whole role replacement ends none.
	if cred != nil || (before.Enabled && !row.Enabled) {
		if row.Epoch, err = bumped(row.Epoch, what); err != nil {
			return err
		}
		if row.LoginGeneration, err = bumped(row.LoginGeneration, what); err != nil {
			return err
		}
	}
	row.ChangedSeq = s.StateSeq + 1
	s.commitUserChange(replaceRow(s.Users, i, row), c.request)
	return nil
}

func (s *ClusterState) applyUserDrop(f *commandFields) error {
	c, err := f.actor()
	if err != nil {
		return err
	}
	expectSeq, err := f.int64("expect_seq")
	if err != nil {
		return err
	}
	if err := f.finish(); err != nil {
		return err
	}
	what := statement(fmt.Sprintf("drop user %s", echoClipped(c.name)))
	if err := s.refuseRepeat(c.request); err != nil {
		return err
	}
	grantor, err := s.grantorRow(what, c)
	if err != nil {
		return err
	}
	i, err := s.targetRow(what, c.name, expectSeq)
	if err != nil {
		return err
	}
	before := s.Users[i]
	if err := checkCeiling(what, grantor, before.Roles, nil); err != nil {
		return err
	}
	if err := s.guardLastAdmin(what, before, false, nil, true); err != nil {
		return err
	}
	users := make([]UserRow, 0, len(s.Users)-1)
	users = append(users, s.Users[:i]...)
	users = append(users, s.Users[i+1:]...)
	s.UserDropSeq = s.StateSeq + 1
	s.commitUserChange(users, c.request)
	return nil
}

// applyUserRole is USER_GRANT (grant true) and USER_REVOKE. Granting a held grant and revoking one
// that is not held succeed and change nothing on the row (no epoch, no changed_seq), today's
// GrantRoleAs and RevokeRoleAs; the command still takes a state_seq and enters the request list
// (spec 4.2, B12 and B15).
func (s *ClusterState) applyUserRole(f *commandFields, grant bool) error {
	c, err := f.actor()
	if err != nil {
		return err
	}
	expectSeq, err := f.int64("expect_seq")
	if err != nil {
		return err
	}
	var change authrules.Grant
	if change.Role, err = f.str("role"); err != nil {
		return err
	}
	if change.DB, err = f.str("db"); err != nil {
		return err
	}
	if err := f.finish(); err != nil {
		return err
	}
	what := statement(fmt.Sprintf("revoke role %s@%s from %s", change.Role, change.DB, echoClipped(c.name)))
	if grant {
		what = statement(fmt.Sprintf("grant role %s@%s to %s", change.Role, change.DB, echoClipped(c.name)))
	}
	if err := s.refuseRepeat(c.request); err != nil {
		return err
	}
	if err := grantPolicy(what, []authrules.Grant{change}); err != nil {
		return err
	}
	grantor, err := s.grantorRow(what, c)
	if err != nil {
		return err
	}
	i, err := s.targetRow(what, c.name, expectSeq)
	if err != nil {
		return err
	}
	before := s.Users[i]
	var given []authrules.Grant
	if grant {
		given = []authrules.Grant{change}
	}
	if err := checkCeiling(what, grantor, before.Roles, given); err != nil {
		return err
	}
	held := slices.Contains(before.Roles, change)
	if held == grant {
		s.commitUserChange(s.Users, c.request)
		return nil
	}
	row := copyRow(before)
	if grant {
		row.Roles = authrules.NormalizeGrants(append(row.Roles, change))
	} else {
		row.Roles = slices.DeleteFunc(row.Roles, func(g authrules.Grant) bool { return g == change })
		if err := s.guardLastAdmin(what, before, row.Enabled, row.Roles, false); err != nil {
			return err
		}
	}
	if row.Epoch, err = bumped(row.Epoch, what); err != nil {
		return err
	}
	row.ChangedSeq = s.StateSeq + 1
	s.commitUserChange(replaceRow(s.Users, i, row), c.request)
	return nil
}

// clipName is a user name cut to 64 bytes for a refusal text that puts its own quotes around it.
func clipName(name string) string {
	if len(name) <= 64 {
		return name
	}
	return name[:64] + "…"
}
