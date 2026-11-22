package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/authrules"
)

var (
	rootAll    = authrules.Grant{Role: authrules.RoleRoot, DB: authrules.AllDatabases}
	managerAll = authrules.Grant{Role: authrules.RoleUserManager, DB: authrules.AllDatabases}
	clusterAll = authrules.Grant{Role: authrules.RoleClusterManager, DB: authrules.AllDatabases}
	writerApp  = authrules.Grant{Role: authrules.RoleWriter, DB: "app"}
	readerApp  = authrules.Grant{Role: authrules.RoleReader, DB: "app"}
)

func userReq(n byte) [UserRequestIDLen]byte {
	var id [UserRequestIDLen]byte
	id[0], id[15] = 0xEE, n
	return id
}

// userCred is a credential inside the window whose salt and hash are unique to seed.
func userCred(seed int) UserCredential {
	salt := bytes.Repeat([]byte{0x5A}, authrules.MinSaltLen)
	salt[0], salt[1] = byte(seed), byte(seed>>8)
	hash := bytes.Repeat([]byte{0x3C}, authrules.Argon2HashLen)
	hash[0], hash[1] = byte(seed), byte(seed>>8)
	return UserCredential{Salt: salt, Hash: hash, Memory: authrules.MinArgon2MemoryKiB, Iterations: authrules.MinArgon2Iterations, Parallelism: 4, PepperKID: userTestKID}
}

// userFixture is a cluster state with a user table, and the helpers that build commands against it
// the way a proposing node would: by the session's account, with the target's current revision.
type userFixture struct {
	t    testing.TB
	st   ClusterState
	next int
}

// newUserFixture bootstraps fsmCluster and its first administrator, root.
func newUserFixture(t testing.TB) *userFixture {
	t.Helper()
	fx := &userFixture{t: t, st: fsmCluster(t), next: 1}
	fx.apply(CommandUserBootstrap, UserBootstrapBody{Request: fx.req(), Name: "root", Credential: fx.cred(), CreatedMs: 1}.Encode())
	return fx
}

func (fx *userFixture) req() [UserRequestIDLen]byte {
	fx.next++
	var id [UserRequestIDLen]byte
	id[0], id[1], id[2] = 0xAB, byte(fx.next), byte(fx.next>>8)
	return id
}

func (fx *userFixture) cred() UserCredential {
	fx.next++
	return userCred(fx.next)
}

func (fx *userFixture) row(name string) UserRow {
	fx.t.Helper()
	row, ok := UserView{Users: fx.st.Users}.User(name)
	if !ok {
		fx.t.Fatalf("no user %q", name)
	}
	return row
}

func (fx *userFixture) has(name string) bool {
	_, ok := UserView{Users: fx.st.Users}.User(name)
	return ok
}

func (fx *userFixture) seqOf(name string) int64 { return fx.row(name).CreatedSeq }

func (fx *userFixture) apply(kind byte, body []byte) Result {
	fx.t.Helper()
	return fsmApply(fx.t, &fx.st, kind, body)
}

// refuse demands a refusal and that the state is byte for byte what it was.
func (fx *userFixture) refuse(kind byte, body []byte) error {
	fx.t.Helper()
	before := fsmEncodedTB(fx.t, fx.st)
	users := fx.st.Users
	_, err := fx.st.ApplyCommand(Command{Version: CommandVersion, Kind: kind, BaseSeq: uint64(fx.st.StateSeq), Body: body})
	if err == nil {
		fx.t.Fatalf("kind 0x%02x was accepted", kind)
	}
	if !bytes.Equal(before, fsmEncodedTB(fx.t, fx.st)) || (len(users) > 0 && &fx.st.Users[0] != &users[0]) {
		fx.t.Fatalf("kind 0x%02x was refused (%v) but the state changed", kind, err)
	}
	return err
}

func fsmEncodedTB(t testing.TB, st ClusterState) []byte {
	t.Helper()
	raw, err := EncodeClusterState(st)
	if err != nil {
		t.Fatalf("state does not encode: %v", err)
	}
	return raw
}

func (fx *userFixture) createBody(by, name string, roles ...authrules.Grant) UserCreateBody {
	return UserCreateBody{Request: fx.req(), By: by, BySeq: fx.seqOf(by), Name: name, ExpectDropSeq: fx.st.UserDropSeq, Roles: roles, Credential: fx.cred(), Enabled: true, CreatedMs: 2}
}

func (fx *userFixture) create(by, name string, roles ...authrules.Grant) {
	fx.t.Helper()
	fx.apply(CommandUserCreate, fx.createBody(by, name, roles...).Encode())
}

func (fx *userFixture) dropBody(by, name string) UserDropBody {
	return UserDropBody{Request: fx.req(), By: by, BySeq: fx.seqOf(by), Name: name, ExpectSeq: fx.row(name).ChangedSeq}
}

func (fx *userFixture) roleBody(by, name string, grant authrules.Grant) UserRoleBody {
	return UserRoleBody{Request: fx.req(), By: by, BySeq: fx.seqOf(by), Name: name, ExpectSeq: fx.row(name).ChangedSeq, Grant: grant}
}

func (fx *userFixture) alterBody(by, name string) UserAlterBody {
	return UserAlterBody{Request: fx.req(), By: by, BySeq: fx.seqOf(by), Name: name, ExpectSeq: fx.row(name).ChangedSeq}
}

// filler applies n user commands that change no row: grants root already holds.
func (fx *userFixture) filler(n int) {
	fx.t.Helper()
	for range n {
		fx.apply(CommandUserGrant, fx.roleBody("root", "root", rootAll).Encode())
	}
}

func wantMention(t testing.TB, err error, code Code, words ...string) {
	t.Helper()
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Code != code {
		t.Fatalf("got %v, want a %s refusal", err, code)
	}
	for _, w := range words {
		if !strings.Contains(refusal.Message, w) {
			t.Fatalf("refusal %q does not say %q", refusal.Message, w)
		}
	}
}

func TestUserBootstrapAppliesOnlyToAnEmptyTable(t *testing.T) {
	fx := newUserFixture(t)
	err := fx.refuse(CommandUserBootstrap, UserBootstrapBody{Request: fx.req(), Name: "admin", Credential: fx.cred(), CreatedMs: 3}.Encode())
	wantMention(t, err, CodeConflict, "already has users")
	if fx.has("admin") || len(fx.st.Users) != 1 {
		t.Fatal("a second bootstrap added a user")
	}
}

func TestUserBootstrapFixesTheStatesPepperKid(t *testing.T) {
	st := fsmCluster(t)
	if st.PepperKID != "" {
		t.Fatalf("a fresh cluster has pepper_kid %q", st.PepperKID)
	}
	cred := userCred(7)
	cred.PepperKID = "00ff00ff"
	res := fsmApply(t, &st, CommandUserBootstrap, UserBootstrapBody{Request: userReq(1), Name: "root", Credential: cred, CreatedMs: 9}.Encode())
	if st.PepperKID != "00ff00ff" {
		t.Fatalf("pepper_kid is %q after USER_BOOTSTRAP, want the row's", st.PepperKID)
	}
	row := st.Users[0]
	if row.Name != "root" || !slices.Equal(row.Roles, []authrules.Grant{rootAll}) || !row.Enabled || row.CreatedSeq != res.StateSeq || row.ChangedSeq != res.StateSeq || row.PasswordChangedMs != 9 {
		t.Fatalf("bootstrap row %+v", row)
	}
	if res.EpochMoved {
		t.Fatal("a user command moved the ring epoch")
	}
}

func TestACredentialWithAnotherPepperKidIsRefused(t *testing.T) {
	fx := newUserFixture(t)
	body := fx.createBody("root", "ada")
	body.Credential.PepperKID = "ffffffff"
	wantMention(t, fx.refuse(CommandUserCreate, body.Encode()), CodeConflict, "ffffffff", userTestKID)
	fx.create("root", "ada")
	alter := fx.alterBody("root", "ada")
	cred := fx.cred()
	cred.PepperKID = "12345678"
	alter.Credential = &cred
	wantMention(t, fx.refuse(CommandUserAlter, alter.Encode()), CodeConflict, "12345678")
}

// TestACreateRetriedAfterADropDoesNotBringTheAccountBack (B1): create ada (R1) commits and the
// reply is lost; another administrator drops ada; R1 arrives again through a new leader. It is
// refused, the list says R1 was applied (what committedEffect reads), and ada stays gone. Break:
// remove the user_requests check; red (ada is back with the old password).
func TestACreateRetriedAfterADropDoesNotBringTheAccountBack(t *testing.T) {
	fx := newUserFixture(t)
	create := fx.createBody("root", "ada", readerApp)
	fx.apply(CommandUserCreate, create.Encode())
	fx.apply(CommandUserDrop, fx.dropBody("root", "ada").Encode())
	err := fx.refuse(CommandUserCreate, create.Encode())
	wantMention(t, err, CodeConflict, "already applied")
	if fx.has("ada") {
		t.Fatal("the retried create brought ada back")
	}
	if _, done := (UserView{Requests: fx.st.UserRequests}).RequestApplied(create.Request); !done {
		t.Fatal("the request list does not remember R1, so a proposer cannot answer the retry as done")
	}
}

// TestARetriedDropDoesNotDropARecreatedAccount (B10): drop R1 commits and the reply is lost, ada is
// created again, and 257 later user commands push R1 out of the list; R1 arrives again and is
// refused by its expect_seq. Break: drop the expect_seq check from the drop applier; red.
func TestARetriedDropDoesNotDropARecreatedAccount(t *testing.T) {
	fx := newUserFixture(t)
	fx.create("root", "ada")
	drop := fx.dropBody("root", "ada")
	fx.apply(CommandUserDrop, drop.Encode())
	fx.create("root", "ada")
	fx.filler(authrules.RequestListSize + 1)
	if _, remembered := (UserView{Requests: fx.st.UserRequests}).RequestApplied(drop.Request); remembered {
		t.Fatal("the list still remembers R1; the test needs it evicted")
	}
	wantMention(t, fx.refuse(CommandUserDrop, drop.Encode()), CodeConflict, "changed since this node read it")
	if !fx.has("ada") {
		t.Fatal("the retried drop dropped the re-created ada")
	}
}

// TestARetriedRevokeDoesNotRemoveARegrantedRole (B10): the same shape for a revoke and a re-grant.
// Break: drop the revoke applier's expect_seq check; red.
func TestARetriedRevokeDoesNotRemoveARegrantedRole(t *testing.T) {
	fx := newUserFixture(t)
	fx.create("root", "ada", writerApp)
	revoke := fx.roleBody("root", "ada", writerApp)
	fx.apply(CommandUserRevoke, revoke.Encode())
	fx.apply(CommandUserGrant, fx.roleBody("root", "ada", writerApp).Encode())
	fx.filler(authrules.RequestListSize + 1)
	wantMention(t, fx.refuse(CommandUserRevoke, revoke.Encode()), CodeConflict, "changed since this node read it")
	if !slices.Contains(fx.row("ada").Roles, writerApp) {
		t.Fatal("the retried revoke removed the re-granted role")
	}
}

// TestARetriedPasswordChangeDoesNotUndoALaterOne (F2, B5): alter P1 commits and the reply is
// lost, alter P2 commits, P1 arrives again through a new leader: refused, the row keeps P2, and
// the list reports P1 done (not the row, which carries P2). Break: remove the list check and the
// fence; red (P1 restored).
func TestARetriedPasswordChangeDoesNotUndoALaterOne(t *testing.T) {
	fx := newUserFixture(t)
	fx.create("root", "ada")
	p1 := fx.alterBody("root", "ada")
	c1 := fx.cred()
	p1.Credential, p1.PasswordChangedMs = &c1, 10
	fx.apply(CommandUserAlter, p1.Encode())
	p2 := fx.alterBody("root", "ada")
	c2 := fx.cred()
	p2.Credential, p2.PasswordChangedMs = &c2, 20
	fx.apply(CommandUserAlter, p2.Encode())
	wantMention(t, fx.refuse(CommandUserAlter, p1.Encode()), CodeConflict, "already applied")
	if !bytes.Equal(fx.row("ada").Salt, c2.Salt) {
		t.Fatal("the retried password change undid the later one")
	}
	if _, done := (UserView{Requests: fx.st.UserRequests}).RequestApplied(p1.Request); !done {
		t.Fatal("the list does not report P1 done")
	}
}

// TestARepeatOfAnAppliedCommandIsRefusedWhileItsRequestIsRemembered: every kind, sent again with
// the same request id while the list holds it, is refused by the list, not by whatever row check
// would come next. Break: remove the list check; red for create (name exists instead) and for a
// re-sent grant after a revoke (the fence instead).
func TestARepeatOfAnAppliedCommandIsRefusedWhileItsRequestIsRemembered(t *testing.T) {
	fx := newUserFixture(t)
	bootstrap := UserBootstrapBody{Request: fx.st.UserRequests[0].ID, Name: "root", Credential: fx.cred(), CreatedMs: 1}
	create := fx.createBody("root", "ada")
	fx.apply(CommandUserCreate, create.Encode())
	grant := fx.roleBody("root", "ada", writerApp)
	fx.apply(CommandUserGrant, grant.Encode())
	fx.apply(CommandUserRevoke, fx.roleBody("root", "ada", writerApp).Encode())
	alter := fx.alterBody("root", "ada")
	off := false
	alter.Enabled = &off
	fx.apply(CommandUserAlter, alter.Encode())
	revoke := fx.roleBody("root", "ada", readerApp)
	fx.apply(CommandUserRevoke, revoke.Encode())
	drop := fx.dropBody("root", "ada")
	fx.apply(CommandUserDrop, drop.Encode())
	for name, repeat := range map[string]struct {
		kind byte
		body []byte
	}{
		"bootstrap": {CommandUserBootstrap, bootstrap.Encode()},
		"create":    {CommandUserCreate, create.Encode()},
		"grant":     {CommandUserGrant, grant.Encode()},
		"alter":     {CommandUserAlter, alter.Encode()},
		"revoke":    {CommandUserRevoke, revoke.Encode()},
		"drop":      {CommandUserDrop, drop.Encode()},
	} {
		err := fx.refuse(repeat.kind, repeat.body)
		var refusal *Error
		if !errors.As(err, &refusal) || refusal.Code != CodeConflict || !strings.Contains(refusal.Message, "already applied") {
			t.Errorf("%s repeated: %v, want the list's CONFLICT", name, err)
		}
	}
}

// TestARetriedCreateNeverBringsADroppedAccountBack (review R3, owner ruling 2026-11-20): create
// ada, drop ada, then no-op commands; a retry of the create under its first body is refused at
// every distance, inside the request list and long after it is evicted (300 and 10,000 later
// commands), and ada stays gone. Break: drop the expect_drop_seq check; red at 255.
func TestARetriedCreateNeverBringsADroppedAccountBack(t *testing.T) {
	fx := newUserFixture(t)
	create := fx.createBody("root", "ada", writerApp)
	fx.apply(CommandUserCreate, create.Encode())
	fx.apply(CommandUserDrop, fx.dropBody("root", "ada").Encode())
	retry := func(later int) {
		t.Helper()
		st := fx.st
		_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: CommandUserCreate, BaseSeq: uint64(st.StateSeq), Body: create.Encode()})
		if err == nil {
			t.Fatalf("after %d later commands the retried create applied and brought ada back", later)
		}
		wantMention(t, err, CodeConflict)
		if _, back := (UserView{Users: st.Users}).User("ada"); back {
			t.Fatalf("after %d later commands ada is back", later)
		}
	}
	later := 0
	for _, distance := range []int{0, 1, 254, 255, 256, 300, 10000} {
		fx.filler(distance - later)
		later = distance
		retry(later)
	}
}

func TestTheRequestListKeepsTheLast256(t *testing.T) {
	fx := newUserFixture(t)
	first := fx.st.UserRequests[0]
	fx.filler(authrules.RequestListSize + 44)
	list := fx.st.UserRequests
	if len(list) != authrules.RequestListSize {
		t.Fatalf("the list holds %d requests, want %d", len(list), authrules.RequestListSize)
	}
	if _, kept := (UserView{Requests: list}).RequestApplied(first.ID); kept {
		t.Fatal("the oldest request was not evicted first")
	}
	if list[len(list)-1].StateSeq != fx.st.StateSeq {
		t.Fatalf("the newest entry is at state_seq %d, want the last commit's %d", list[len(list)-1].StateSeq, fx.st.StateSeq)
	}
	for i := 1; i < len(list); i++ {
		if list[i].StateSeq != list[i-1].StateSeq+1 {
			t.Fatalf("entry %d is at %d after %d: the list is the last commits, oldest first", i, list[i].StateSeq, list[i-1].StateSeq)
		}
	}
}

// TestDroppingTheLastUserManagerIsRefusedWhileAClusterManagerRemains is the cluster's half of the
// one predicate (F3, B8): with root gone, ops (user-manager@*) is the last account that can manage
// users although infra holds cluster-manager@*. Break: count cluster-manager; red.
func TestDroppingTheLastUserManagerIsRefusedWhileAClusterManagerRemains(t *testing.T) {
	fx := newUserFixture(t)
	fx.create("root", "ops", managerAll, readerApp)
	fx.create("root", "infra", clusterAll)
	fx.apply(CommandUserDrop, fx.dropBody("root", "root").Encode())
	off := false
	disable := fx.alterBody("ops", "ops")
	disable.Enabled = &off
	strip := fx.alterBody("ops", "ops")
	strip.Roles = &[]authrules.Grant{readerApp}
	for name, c := range map[string]struct {
		kind byte
		body []byte
	}{
		"drop":    {CommandUserDrop, fx.dropBody("ops", "ops").Encode()},
		"disable": {CommandUserAlter, disable.Encode()},
		"strip":   {CommandUserAlter, strip.Encode()},
		"revoke":  {CommandUserRevoke, fx.roleBody("ops", "ops", managerAll).Encode()},
	} {
		err := fx.refuse(c.kind, c.body)
		var refusal *Error
		if !errors.As(err, &refusal) || refusal.Code != CodeInvalidArgument || !strings.Contains(refusal.Message, "last enabled account that can manage users") {
			t.Errorf("%s of the last user-manager: %v, want the last-administrator refusal", name, err)
		}
	}
}

func TestANoOpGrantAndRevokeChangeNothingOnTheRow(t *testing.T) {
	fx := newUserFixture(t)
	fx.create("root", "ada", readerApp)
	before := fx.row("ada")
	for _, c := range []struct {
		kind  byte
		grant authrules.Grant
	}{{CommandUserGrant, readerApp}, {CommandUserRevoke, writerApp}} {
		body := fx.roleBody("root", "ada", c.grant)
		seq := fx.st.StateSeq
		res := fx.apply(c.kind, body.Encode())
		after := fx.row("ada")
		if after.Epoch != before.Epoch || after.ChangedSeq != before.ChangedSeq || !reflect.DeepEqual(after.Roles, before.Roles) {
			t.Fatalf("kind 0x%02x of a no-op changed the row: %+v -> %+v", c.kind, before, after)
		}
		if res.StateSeq != seq+1 {
			t.Fatalf("a no-op did not take a state_seq")
		}
		if got, ok := (UserView{Requests: fx.st.UserRequests}).RequestApplied(body.Request); !ok || got != res.StateSeq {
			t.Fatalf("a no-op did not enter the request list")
		}
	}
}

func TestARoleGrantAndRevokeBumpTheEpochAndARoleReplacementDoesNot(t *testing.T) {
	fx := newUserFixture(t)
	fx.create("root", "ada", readerApp)
	step := func(what string, kind byte, body []byte, epoch, generation int64) {
		t.Helper()
		before := fx.row("ada")
		fx.apply(kind, body)
		after := fx.row("ada")
		if after.Epoch-before.Epoch != epoch || after.LoginGeneration-before.LoginGeneration != generation {
			t.Fatalf("%s moved epoch by %d and login_generation by %d, want %d and %d", what, after.Epoch-before.Epoch, after.LoginGeneration-before.LoginGeneration, epoch, generation)
		}
		if after.ChangedSeq != fx.st.StateSeq {
			t.Fatalf("%s did not move changed_seq to the commit's state_seq", what)
		}
	}
	step("grant", CommandUserGrant, fx.roleBody("root", "ada", writerApp).Encode(), 1, 0)
	step("revoke", CommandUserRevoke, fx.roleBody("root", "ada", writerApp).Encode(), 1, 0)
	replace := fx.alterBody("root", "ada")
	replace.Roles = &[]authrules.Grant{writerApp}
	step("role replacement", CommandUserAlter, replace.Encode(), 0, 0)
	password := fx.alterBody("root", "ada")
	cred := fx.cred()
	password.Credential, password.PasswordChangedMs = &cred, 5
	step("new password", CommandUserAlter, password.Encode(), 1, 1)
	off, on := false, true
	disable := fx.alterBody("root", "ada")
	disable.Enabled = &off
	step("disable", CommandUserAlter, disable.Encode(), 1, 1)
	enable := fx.alterBody("root", "ada")
	enable.Enabled = &on
	step("enable", CommandUserAlter, enable.Encode(), 0, 0)
	if fx.row("ada").PasswordChangedMs != 5 {
		t.Fatal("the new password did not stamp password_changed_ms from the command")
	}
}

func TestACredentialOutsideTheWindowIsRefused(t *testing.T) {
	fx := newUserFixture(t)
	for name, edit := range map[string]func(*UserCredential){
		"memory below": func(c *UserCredential) { c.Memory = authrules.MinArgon2MemoryKiB - 1 },
		"memory above": func(c *UserCredential) { c.Memory = authrules.MaxArgon2MemoryKiB + 1 },
		"iterations":   func(c *UserCredential) { c.Iterations = authrules.MaxArgon2Iterations + 1 },
		"parallelism":  func(c *UserCredential) { c.Parallelism = 0 },
	} {
		body := fx.createBody("root", "ada")
		edit(&body.Credential)
		err := fx.refuse(CommandUserCreate, body.Encode())
		var refusal *Error
		if !errors.As(err, &refusal) || refusal.Code != CodeInvalidArgument {
			t.Errorf("%s: %v, want INVALID_ARGUMENT", name, err)
		}
	}
}

// TestACommandFromADroppedAndRecreatedGrantorIsRefused (B17): a session of bob, made before bob was
// dropped and created again, carries the old created_seq and may change nothing. Break: compare
// the grantor by name only; red.
func TestACommandFromADroppedAndRecreatedGrantorIsRefused(t *testing.T) {
	fx := newUserFixture(t)
	fx.create("root", "bob", managerAll)
	oldSeq := fx.seqOf("bob")
	fx.apply(CommandUserDrop, fx.dropBody("root", "bob").Encode())
	fx.create("root", "bob", managerAll)
	body := fx.createBody("bob", "ada")
	body.BySeq = oldSeq
	wantMention(t, fx.refuse(CommandUserCreate, body.Encode()), CodePermissionDenied, "bob")
}

// TestANonRootManagerCannotDropDemoteOrPromoteRoot (spec 4.2, "by may not ... exceeds its
// ceiling"): a user-manager on every database that is not root may not drop an account holding
// root, revoke root from it, give root through an alter, or give itself root. Each is refused
// PERMISSION_DENIED by the ceiling, judged against the committed rows. Break: skip the ceiling on
// drop, on revoke, or pass no given grants from alter; red.
func TestANonRootManagerCannotDropDemoteOrPromoteRoot(t *testing.T) {
	fx := newUserFixture(t)
	fx.create("root", "mgr", managerAll)
	fx.create("root", "boss", rootAll)
	fx.create("root", "ada", readerApp)
	giveRoot := fx.alterBody("mgr", "ada")
	giveRoot.Roles = &[]authrules.Grant{rootAll}
	selfEscalate := fx.alterBody("mgr", "mgr")
	selfEscalate.Roles = &[]authrules.Grant{managerAll, rootAll}
	for name, c := range map[string]struct {
		kind byte
		body []byte
	}{
		"drop boss":       {CommandUserDrop, fx.dropBody("mgr", "boss").Encode()},
		"revoke root":     {CommandUserRevoke, fx.roleBody("mgr", "boss", rootAll).Encode()},
		"alter gives":     {CommandUserAlter, giveRoot.Encode()},
		"alter self root": {CommandUserAlter, selfEscalate.Encode()},
	} {
		var refusal *Error
		if err := fx.refuse(c.kind, c.body); !errors.As(err, &refusal) || refusal.Code != CodePermissionDenied {
			t.Errorf("%s: %v, want PERMISSION_DENIED from the ceiling", name, err)
		}
	}
}

func TestCreateUserRefusesAnExistingName(t *testing.T) {
	fx := newUserFixture(t)
	fx.create("root", "ada")
	err := fx.refuse(CommandUserCreate, fx.createBody("root", "ada").Encode())
	wantMention(t, err, CodeUserExists, CodeUserExists.Text(CondUserExists, map[string]string{"user": "ada"}))
}

func TestCreateUserRefusesAReusedSalt(t *testing.T) {
	fx := newUserFixture(t)
	body := fx.createBody("root", "ada")
	body.Credential.Salt = slices.Clone(fx.row("root").Salt)
	wantMention(t, fx.refuse(CommandUserCreate, body.Encode()), CodeConflict, "salt")
}

func TestTheTableRefusesUser10001(t *testing.T) {
	fx := newUserFixture(t)
	users := slices.Clone(fx.st.Users)
	for i := len(users); i < authrules.MaxUsers; i++ {
		users = append(users, userTestRow(fmt.Sprintf("u%05d", i), 2))
	}
	slices.SortFunc(users, func(a, b UserRow) int { return strings.Compare(a.Name, b.Name) })
	fx.st.Users = users
	wantMention(t, fx.refuse(CommandUserCreate, fx.createBody("root", "zz").Encode()), CodeInvalidArgument, "10000")
}

func TestCreateRefusesTheReservedNodeName(t *testing.T) {
	fx := newUserFixture(t)
	wantMention(t, fx.refuse(CommandUserCreate, fx.createBody("root", authrules.NodeUser).Encode()), CodeReservedName, authrules.NodeUser)
}

func TestAGrantNamingABadDatabaseIsRefused(t *testing.T) {
	fx := newUserFixture(t)
	bad := authrules.Grant{Role: authrules.RoleReader, DB: "a/b"}
	wantMention(t, fx.refuse(CommandUserCreate, fx.createBody("root", "ada", bad).Encode()), CodeInvalidArgument, "a/b")
	fx.create("root", "ada")
	wantMention(t, fx.refuse(CommandUserGrant, fx.roleBody("root", "ada", bad).Encode()), CodeInvalidArgument, "a/b")
	wantMention(t, fx.refuse(CommandUserGrant, fx.roleBody("root", "ada", authrules.Grant{Role: "readWrite", DB: "app"}).Encode()), CodeInvalidArgument, "writer")
}

// TestTwoDropsOfTheLastTwoAdministratorsLeaveOne: both drops are built against one state; the
// first commits, the second meets STATE_MOVED, and stamped again it is refused by the guard.
func TestTwoDropsOfTheLastTwoAdministratorsLeaveOne(t *testing.T) {
	fx := newUserFixture(t)
	fx.create("root", "ops", managerAll)
	base := fx.st.StateSeq
	first := fx.dropBody("root", "root").Encode()
	second := fx.dropBody("ops", "ops").Encode()
	fx.apply(CommandUserDrop, first)
	_, err := fx.st.ApplyCommand(Command{Version: CommandVersion, Kind: CommandUserDrop, BaseSeq: uint64(base), Body: second})
	fsmWantCode(t, err, CodeStateMoved)
	wantMention(t, fx.refuse(CommandUserDrop, second), CodeInvalidArgument, "last enabled account")
	if len(fx.st.Users) != 1 || !fx.has("ops") {
		t.Fatalf("users %v, want ops alone", fx.st.Users)
	}
}

// TestGrantIsCheckedAgainstTheGrantorsCommittedRoles: bob's session still holds writer@app, but
// the committed row lost it, so bob may no longer give it.
func TestGrantIsCheckedAgainstTheGrantorsCommittedRoles(t *testing.T) {
	fx := newUserFixture(t)
	fx.create("root", "bob", managerAll, writerApp)
	fx.create("bob", "carol", writerApp)
	fx.apply(CommandUserRevoke, fx.roleBody("root", "bob", writerApp).Encode())
	wantMention(t, fx.refuse(CommandUserCreate, fx.createBody("bob", "dave", writerApp).Encode()), CodePermissionDenied, "writer@app")
}

// TestThereIsNoUserResetKind is a regression: rev 1's USER_RESET (0x17) was withdrawn by owner
// ruling 2. Break: add a 0x17 case.
func TestThereIsNoUserResetKind(t *testing.T) {
	fx := newUserFixture(t)
	_, err := fx.st.ApplyCommand(Command{Version: CommandVersion, Kind: 0x17, BaseSeq: uint64(fx.st.StateSeq), Body: fsmDoc(fsmStr("name", "root"))})
	fsmWantCode(t, err, CodeInvalidArgument)
	if knownCommandKind(0x17) {
		t.Fatal("0x17 is a command kind")
	}
}

// TestAnApplierNormalisesRoles (B11): roles in another order, and repeated, are stored sorted and
// once, the spelling the snapshot decoder demands, and the state round-trips.
func TestAnApplierNormalisesRoles(t *testing.T) {
	fx := newUserFixture(t)
	fx.create("root", "ada", writerApp, readerApp, writerApp)
	if got := fx.row("ada").Roles; !reflect.DeepEqual(got, []authrules.Grant{readerApp, writerApp}) {
		t.Fatalf("create stored roles %v, want them sorted and once", got)
	}
	if decoded, err := DecodeClusterState(fsmEncodedTB(t, fx.st)); err != nil || !reflect.DeepEqual(decoded.Users, fx.st.Users) {
		t.Fatalf("after the create the decoded table differs from the applied one (%v)", err)
	}
	alter := fx.alterBody("root", "ada")
	alter.Roles = &[]authrules.Grant{writerApp, managerAll, writerApp, readerApp}
	fx.apply(CommandUserAlter, alter.Encode())
	want := []authrules.Grant{readerApp, managerAll, writerApp}
	if got := fx.row("ada").Roles; !reflect.DeepEqual(got, want) {
		t.Fatalf("stored roles %v, want %v", got, want)
	}
	decoded, err := DecodeClusterState(fsmEncodedTB(t, fx.st))
	if err != nil {
		t.Fatalf("the state does not round-trip: %v", err)
	}
	if !reflect.DeepEqual(decoded.Users, fx.st.Users) {
		t.Fatal("the decoded table differs from the applied one")
	}
}

// withExtraField is body with one more field, which no build reads.
func withExtraField(body []byte) []byte {
	elems, err := bsoncore.Document(body).Elements()
	if err != nil {
		panic(err) // invariant: body was built by an encoder in this file
	}
	var out []byte
	for _, e := range elems {
		out = append(out, e...)
	}
	return bsoncore.BuildDocument(nil, append(out, fsmStr("zz_extra", "x")...))
}

// refusalSteps builds the state userRefusalCases is written against: root disabled, mgr
// (user-manager on every database and reader of app) the last account that can manage users, and
// ada a reader of app. apply commits one command.
func refusalSteps(fx *userFixture, apply func(kind byte, body []byte)) {
	apply(CommandUserBootstrap, UserBootstrapBody{Request: fx.req(), Name: "root", Credential: fx.cred(), CreatedMs: 1}.Encode())
	apply(CommandUserCreate, fx.createBody("root", "mgr", managerAll, readerApp).Encode())
	apply(CommandUserCreate, fx.createBody("root", "ada", readerApp).Encode())
	off := false
	disable := fx.alterBody("root", "root")
	disable.Enabled = &off
	apply(CommandUserAlter, disable.Encode())
}

// refusalFixture is a state built by refusalSteps.
func refusalFixture(t testing.TB) *userFixture {
	t.Helper()
	fx := &userFixture{t: t, st: fsmCluster(t), next: 1}
	refusalSteps(fx, func(kind byte, body []byte) { fx.apply(kind, body) })
	return fx
}

// userRefusalCases is one command per refusal of spec 4.2, against a state refusalSteps built.
func userRefusalCases(fx *userFixture) map[string]struct {
	kind byte
	body []byte
} {
	type c = struct {
		kind byte
		body []byte
	}
	stale := fx.dropBody("mgr", "ada")
	stale.ExpectSeq--
	badKid := fx.createBody("mgr", "kid")
	badKid.Credential.PepperKID = "ffffffff"
	window := fx.createBody("mgr", "win")
	window.Credential.Iterations = 99
	salt := fx.createBody("mgr", "salty")
	salt.Credential.Salt = slices.Clone(fx.row("ada").Salt)
	ghost := fx.createBody("mgr", "x")
	ghost.By = "ghost"
	disabledGrantor := fx.createBody("root", "x2")
	negativeCreated := fx.createBody("mgr", "past")
	negativeCreated.CreatedMs = -1
	negativeChanged := fx.alterBody("mgr", "ada")
	negativeCred := fx.cred()
	negativeChanged.Credential, negativeChanged.PasswordChangedMs = &negativeCred, -5
	empty := fx.alterBody("mgr", "ada")
	giveRoot := fx.alterBody("mgr", "ada")
	giveRoot.Roles = &[]authrules.Grant{rootAll}
	selfEscalate := fx.alterBody("mgr", "mgr")
	selfEscalate.Roles = &[]authrules.Grant{managerAll, readerApp, rootAll}
	return map[string]c{
		"bootstrap again":  {CommandUserBootstrap, UserBootstrapBody{Request: fx.req(), Name: "b", Credential: fx.cred()}.Encode()},
		"repeat":           {CommandUserCreate, UserCreateBody{Request: fx.st.UserRequests[len(fx.st.UserRequests)-1].ID, By: "mgr", BySeq: fx.seqOf("mgr"), Name: "y", Credential: fx.cred(), Enabled: true}.Encode()},
		"reserved name":    {CommandUserCreate, fx.createBody("mgr", authrules.NodeUser).Encode()},
		"control name":     {CommandUserCreate, fx.createBody("mgr", "a\x07b").Encode()},
		"unknown role":     {CommandUserCreate, fx.createBody("mgr", "r", authrules.Grant{Role: "god", DB: "*"}).Encode()},
		"bad database":     {CommandUserGrant, fx.roleBody("mgr", "ada", authrules.Grant{Role: "reader", DB: ".."}).Encode()},
		"window":           {CommandUserCreate, window.Encode()},
		"kid":              {CommandUserCreate, badKid.Encode()},
		"grantor gone":     {CommandUserCreate, ghost.Encode()},
		"grantor disabled": {CommandUserCreate, disabledGrantor.Encode()},
		"grantor ceiling":  {CommandUserCreate, fx.createBody("mgr", "z", rootAll).Encode()},
		// root is disabled here, so it is not an administrator and the guard would let these
		// through: only the ceiling refuses a manager that is not root changing an account that
		// holds root (spec 4.2, "by may not").
		"ceiling drop root":     {CommandUserDrop, fx.dropBody("mgr", "root").Encode()},
		"ceiling revoke root":   {CommandUserRevoke, fx.roleBody("mgr", "root", rootAll).Encode()},
		"ceiling alter gives":   {CommandUserAlter, giveRoot.Encode()},
		"ceiling self-escalate": {CommandUserAlter, selfEscalate.Encode()},
		"name exists":           {CommandUserCreate, fx.createBody("mgr", "ada").Encode()},
		"absent":                {CommandUserDrop, UserDropBody{Request: fx.req(), By: "mgr", BySeq: fx.seqOf("mgr"), Name: "nobody"}.Encode()},
		"stale fence":           {CommandUserDrop, stale.Encode()},
		"last admin":            {CommandUserRevoke, fx.roleBody("mgr", "mgr", managerAll).Encode()},
		"salt in use":           {CommandUserCreate, salt.Encode()},
		"nothing to change":     {CommandUserAlter, empty.Encode()},
		"negative created":      {CommandUserCreate, negativeCreated.Encode()},
		"negative changed":      {CommandUserAlter, negativeChanged.Encode()},
		"missing fields":        {CommandUserDrop, fsmDoc(fsmStr("name", "ada"))},
		"unread field":          {CommandUserDrop, withExtraField(fx.dropBody("mgr", "ada").Encode())},
	}
}

func TestARefusedUserCommandChangesNothing(t *testing.T) {
	fx := refusalFixture(t)
	for name, c := range userRefusalCases(fx) {
		t.Run(name, func(t *testing.T) {
			sub := &userFixture{t: t, st: fx.st, next: 1000}
			if err := sub.refuse(c.kind, c.body); !errors.As(err, new(*Error)) {
				t.Fatalf("refused with %v, not a cluster refusal", err)
			}
		})
	}
}

// TestTheDryRunAndApplyAgreeOnEveryUserRefusal (B11): every refusal of spec 4.2 comes back the
// same from the leader's DryRun (what ProposeContext and relayAuthority call) and from an apply on
// a copy of the same state, and none reaches the log. Break: give the applier a check the dry run
// path skips; red.
func TestTheDryRunAndApplyAgreeOnEveryUserRefusal(t *testing.T) {
	r := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	fx := &userFixture{t: t, next: 1, st: r.State()}
	refusalSteps(fx, func(kind byte, body []byte) {
		t.Helper()
		if _, err := r.Propose(Command{Version: CommandVersion, Kind: kind, BaseSeq: uint64(r.State().StateSeq), Body: body}); err != nil {
			t.Fatalf("kind 0x%02x: %v", kind, err)
		}
		fx.st = r.State()
	})
	cases := userRefusalCases(fx)
	if len(cases) < 15 {
		t.Fatalf("only %d refusal cases", len(cases))
	}
	before := lastLogIndex(t, r)
	for name, c := range cases {
		cmd := Command{Version: CommandVersion, Kind: c.kind, BaseSeq: uint64(r.State().StateSeq), Body: c.body}
		dry := r.DryRun(context.Background(), cmd)
		copyOfState := r.State()
		_, applied := copyOfState.applyCommandWith(cmd, nil, BuildStateRange())
		_, proposed := r.Propose(cmd)
		if dry == nil || applied == nil || proposed == nil {
			t.Errorf("%s: dry run %v, apply %v, propose %v: every path must refuse", name, dry, applied, proposed)
			continue
		}
		if dry.Error() != applied.Error() || dry.Error() != proposed.Error() {
			t.Errorf("%s: the paths disagree:\n dry run %v\n apply   %v\n propose %v", name, dry, applied, proposed)
		}
	}
	if got := lastLogIndex(t, r); got != before || r.IsDiverged() {
		t.Fatalf("a refusal reached the log (%d -> %d, diverged %v)", before, got, r.IsDiverged())
	}
}

// userRandomCommand is a random user or non-user command that the state accepts or refuses; the
// property only needs every replica to answer it the same.
func userRandomCommand(r *rand.Rand, fx *userFixture) (byte, []byte) {
	// root stays: it is the grantor of every command, and a refusal by a missing grantor would
	// make every later command the same refusal.
	names := []string{"ada", "bob", "cy"}
	name := names[r.Intn(len(names))]
	grants := []authrules.Grant{readerApp, writerApp, managerAll, clusterAll, {Role: authrules.RoleDBManager, DB: "z"}}
	pick := func() []authrules.Grant {
		var out []authrules.Grant
		for range r.Intn(4) {
			out = append(out, grants[r.Intn(len(grants))])
		}
		return out
	}
	expect := int64(0)
	if fx.has(name) {
		expect = fx.row(name).ChangedSeq
	}
	switch r.Intn(7) {
	case 0:
		return CommandUserCreate, UserCreateBody{Request: fx.req(), By: "root", BySeq: fx.seqOf("root"), Name: name, ExpectDropSeq: fx.st.UserDropSeq, Roles: pick(), Credential: fx.cred(), Enabled: r.Intn(2) == 0}.Encode()
	case 1:
		roles := pick()
		on := r.Intn(2) == 0
		return CommandUserAlter, UserAlterBody{Request: fx.req(), By: "root", BySeq: fx.seqOf("root"), Name: name, ExpectSeq: expect, Roles: &roles, Enabled: &on}.Encode()
	case 2:
		return CommandUserDrop, UserDropBody{Request: fx.req(), By: "root", BySeq: fx.seqOf("root"), Name: name, ExpectSeq: expect}.Encode()
	case 3:
		return CommandUserGrant, UserRoleBody{Request: fx.req(), By: "root", BySeq: fx.seqOf("root"), Name: name, ExpectSeq: expect, Grant: grants[r.Intn(len(grants))]}.Encode()
	case 4:
		return CommandUserRevoke, UserRoleBody{Request: fx.req(), By: "root", BySeq: fx.seqOf("root"), Name: name, ExpectSeq: expect, Grant: grants[r.Intn(len(grants))]}.Encode()
	case 5:
		return CommandSetSetting, fsmDoc(fsmStr("key", "heartbeat_ms"), fsmI64("value", int64(100+r.Intn(50))))
	}
	return CommandSeeded, fsmNodeBitsBody("n2", 1, fsmBitsAt(r.Intn(8192)))
}

// TestUserCommandsApplyIdenticallyOnEveryReplica: a random sequence of user and non-user commands,
// applied to three replicas, leaves byte-equal encodings and equal tables in memory. Break: make
// an applier build roles from a map without sorting; red.
func TestUserCommandsApplyIdenticallyOnEveryReplica(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		r := rand.New(rand.NewSource(seed))
		fx := newUserFixture(t)
		replicas := []ClusterState{fx.st, fx.st, fx.st}
		for range 150 {
			kind, body := userRandomCommand(r, fx)
			var answers []string
			for i := range replicas {
				_, err := replicas[i].ApplyCommand(Command{Version: CommandVersion, Kind: kind, BaseSeq: uint64(replicas[i].StateSeq), Body: body})
				answers = append(answers, fmt.Sprint(err))
			}
			if answers[0] != answers[1] || answers[0] != answers[2] {
				t.Fatalf("seed %d: replicas answered %q", seed, answers)
			}
			fx.st = replicas[0]
		}
		for i := 1; i < len(replicas); i++ {
			if !bytes.Equal(fsmEncodedTB(t, replicas[0]), fsmEncodedTB(t, replicas[i])) || !reflect.DeepEqual(replicas[0].Users, replicas[i].Users) {
				t.Fatalf("seed %d: replica %d differs from replica 0", seed, i)
			}
		}
	}
}

// deepUsers copies a table with every row's slices at their full capacity, so a write past a
// slice's length into a shared backing array is seen too.
func deepUsers(users []UserRow) []UserRow {
	out := make([]UserRow, len(users))
	for i, row := range users {
		out[i] = row
		out[i].Roles = slices.Clone(row.Roles[:cap(row.Roles)])
		out[i].Salt = slices.Clone(row.Salt[:cap(row.Salt)])
		out[i].Hash = slices.Clone(row.Hash[:cap(row.Hash)])
	}
	return out
}

func sameDeepUsers(a, b []UserRow) bool {
	return reflect.DeepEqual(deepUsers(a), b)
}

// TestAUserApplierNeverWritesIntoTheStateItWasGiven (F9): every user kind, accepted or refused,
// applied to a shallow copy of a state (what DryRun and relayAuthority hand ApplyCommand) leaves
// the original table as it was, down to the bytes past each slice's length; and a reader of the
// narrow accessor races no applier (run with -race). Break: append into a row's Roles in place;
// red.
func TestAUserApplierNeverWritesIntoTheStateItWasGiven(t *testing.T) {
	accepted := newUserFixture(t)
	accepted.create("root", "ada", readerApp)
	off := false
	disable := accepted.alterBody("root", "ada")
	disable.Enabled = &off
	password := accepted.alterBody("root", "ada")
	cred := accepted.cred()
	password.Credential = &cred
	roles := accepted.alterBody("root", "ada")
	roles.Roles = &[]authrules.Grant{writerApp, readerApp}
	type command = struct {
		kind byte
		body []byte
	}
	acceptedCommands := []command{
		{CommandUserCreate, accepted.createBody("root", "new", writerApp).Encode()},
		{CommandUserGrant, accepted.roleBody("root", "ada", writerApp).Encode()},
		{CommandUserRevoke, accepted.roleBody("root", "ada", readerApp).Encode()},
		{CommandUserAlter, disable.Encode()},
		{CommandUserAlter, password.Encode()},
		{CommandUserAlter, roles.Encode()},
		{CommandUserDrop, accepted.dropBody("root", "ada").Encode()},
	}
	refused := refusalFixture(t)
	var refusedCommands []command
	for _, c := range userRefusalCases(refused) {
		refusedCommands = append(refusedCommands, c)
	}
	for _, run := range []struct {
		name     string
		st       ClusterState
		commands []command
		mustPass bool
	}{{"accepted", accepted.st, acceptedCommands, true}, {"refused", refused.st, refusedCommands, false}} {
		original := run.st
		// Give every row's roles spare capacity, so an in-place append would land in shared memory.
		users := slices.Clone(original.Users)
		for i := range users {
			users[i].Roles = slices.Grow(slices.Clone(users[i].Roles), 4)
		}
		original.Users = users
		snapshot := deepUsers(original.Users)
		f := &clusterFSM{state: original}
		var wg sync.WaitGroup
		stop := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, row := range f.userView().Users {
					_ = len(row.Salt) + int(row.Hash[0])
					for _, g := range row.Roles {
						_ = g.Role + g.DB
					}
				}
			}
		}()
		for _, c := range run.commands {
			copyOfState := original
			_, err := copyOfState.applyCommandWith(Command{Version: CommandVersion, Kind: c.kind, BaseSeq: uint64(original.StateSeq), Body: c.body}, nil, BuildStateRange())
			if run.mustPass && err != nil {
				t.Errorf("%s: kind 0x%02x refused (%v): the test needs it applied", run.name, c.kind, err)
			}
			if !sameDeepUsers(original.Users, snapshot) {
				t.Errorf("%s: kind 0x%02x wrote into the table it was given", run.name, c.kind)
				break
			}
		}
		close(stop)
		wg.Wait()
	}
}
