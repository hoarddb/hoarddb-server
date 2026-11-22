package server

import (
	"encoding/base64"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/argon2"

	"github.com/hoarddb/hoarddb-server/clusterpepper"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The cluster-mode user store of a ring node (ring user store spec 6.3, 5.5, 7.3; step M2a.1):
// no users.json, the old file refused at start, and a login checked against the cluster's
// password key, chosen by the record's origin, with no Argon2 run while the node holds no key.

// fakeClusterKey is a cluster password key source: the key and kid it hands out, or err, and the
// cluster id it names.
type fakeClusterKey struct {
	key       clusterpepper.Key
	kid       string
	err       error
	clusterID [16]byte
}

func (f fakeClusterKey) PasswordKey() (clusterpepper.Key, string, error) { return f.key, f.kid, f.err }

func (f fakeClusterKey) ClusterID() [16]byte { return f.clusterID }

// testClusterPepper is a cluster pepper and its key for a fixed cluster id.
func testClusterPepper(t *testing.T) (clusterpepper.Pepper, clusterpepper.Key) {
	t.Helper()
	pepper, err := clusterpepper.Generate()
	if err != nil {
		t.Fatal(err)
	}
	return pepper, pepper.Key([16]byte{0xC1})
}

// clusterMemberManager is an AuthManager as a cluster member builds it: no users file, users
// from the cluster's table.
func clusterMemberManager(t *testing.T) *AuthManager {
	t.Helper()
	keyring, err := BuildKeyring(t.TempDir(), "", "", time.Hour, discardLogger())
	if err != nil {
		t.Fatalf("BuildKeyring: %v", err)
	}
	return NewAuthManager(AuthTokenOptions{TTL: 15 * time.Minute, MaxLifetime: 12 * time.Hour, Keyring: keyring, ClusterMember: true}, discardLogger())
}

// clusterRecord is a record as the cluster's table holds it: the client hash peppered with key.
func clusterRecord(name, password string, key clusterpepper.Key, kid string, createdSeq int64) *UserRecord {
	params := DefaultArgon2Params()
	salt := []byte(strings.Repeat(name[:1], params.SaltLen))
	clientHash := argon2.IDKey([]byte(password), salt, params.Iterations, params.Memory, params.Parallelism, uint32(params.HashLen))
	return &UserRecord{Username: name, Roles: []Grant{{Role: "reader", DB: "*"}}, Enabled: true,
		Credentials: CredentialMaterial{Algo: "argon2id", Salt: base64.StdEncoding.EncodeToString(salt), Hash: base64.StdEncoding.EncodeToString(key.Hash(clientHash)), Params: params, PepperKID: kid},
		createdSeq:  createdSeq, changedSeq: createdSeq}
}

// TestAZeroConfigRecordNeverTakesTheClusterPepperBranch (spec 5.5): the pepper a login checks
// with is chosen by where the record came from, not by the node's mode. A users.json record is
// checked under the node's own keyring even on a manager that holds a cluster key, and a record
// of the cluster's table under the cluster key. Break: choose by the manager's mode; red.
func TestAZeroConfigRecordNeverTakesTheClusterPepperBranch(t *testing.T) {
	am := clusterMemberManager(t)
	pepper, key := testClusterPepper(t)
	am.SetClusterPasswordKey(fakeClusterKey{key: key, kid: pepper.KID()})
	am.clusterMember = false
	if err := am.SeedRootUser("local", "local-password-1"); err != nil {
		t.Fatal(err)
	}
	am.clusterMember = true
	am.mu.Lock()
	am.users["ada"] = clusterRecord("ada", "ada-password-1", key, pepper.KID(), 7)
	am.mu.Unlock()
	if ok, err := am.VerifyUser("local", "local-password-1"); !ok || err != nil {
		t.Fatalf("a users.json record on a cluster member: %v, %v; want it checked under the node's keyring", ok, err)
	}
	if ok, err := am.VerifyUser("ada", "ada-password-1"); !ok || err != nil {
		t.Fatalf("a cluster record: %v, %v; want it checked under the cluster key", ok, err)
	}
	if ok, _ := am.VerifyUser("ada", "wrong-password"); ok {
		t.Fatal("a wrong password was accepted for a cluster record")
	}
}

// TestNoArgon2RunsOnAClusterNodeWithoutThePasswordKey (spec 5.5, 6.2): a cluster member that
// cannot check a password (no users yet, the key not fetched, a witness, or still starting)
// answers with the case's text before any derivation, for a known account and an unknown one.
// Break: derive before asking for the key; red (a derivation is counted).
func TestNoArgon2RunsOnAClusterNodeWithoutThePasswordKey(t *testing.T) {
	am := clusterMemberManager(t)
	pepper, key := testClusterPepper(t)
	am.mu.Lock()
	am.users["ada"] = clusterRecord("ada", "ada-password-1", key, pepper.KID(), 3)
	am.mu.Unlock()
	var derivations atomic.Int64
	verifyUserHook = func() { derivations.Add(1) }
	defer func() { verifyUserHook = nil }()

	_, err := am.verifyLogin("ada", "ada-password-1")
	if err == nil || !strings.Contains(err.Error(), "not opened its cluster state") {
		t.Fatalf("a login before the ring node opened answered %v", err)
	}
	noKey := errors.New("password logins are not available on this node yet: the case text")
	am.SetClusterPasswordKey(fakeClusterKey{err: noKey})
	for _, name := range []string{"ada", "nobody"} {
		if _, err := am.verifyLogin(name, "ada-password-1"); !errors.Is(err, noKey) {
			t.Errorf("login of %q without the key answered %v, want the key source's text", name, err)
		}
	}
	if reason := am.passwordLoginRefusal(); reason != noKey.Error() {
		t.Errorf("the AUTH refusal is %q, want the key source's text", reason)
	}
	if got := derivations.Load(); got != 0 {
		t.Fatalf("%d Argon2 derivations ran on a node without the password key", got)
	}
	am.SetClusterPasswordKey(fakeClusterKey{key: key, kid: pepper.KID()})
	if reason := am.passwordLoginRefusal(); reason != "" {
		t.Errorf("a node holding the key refuses logins: %q", reason)
	}
	if ok, err := am.VerifyUser("ada", "ada-password-1"); !ok || err != nil {
		t.Fatalf("with the key held: %v, %v", ok, err)
	}
}

// TestALegacyUserChangeOnAClusterMemberIsRefused (spec 6.3): a cluster member's users are the
// cluster's table, so no path writes the in-memory store directly; the refusal names the ring
// user commands. Break: let CreateUser through; red.
func TestALegacyUserChangeOnAClusterMemberIsRefused(t *testing.T) {
	am := clusterMemberManager(t)
	credential, err := NewCredential("bob-password-1", DefaultArgon2Params())
	if err != nil {
		t.Fatal(err)
	}
	changes := map[string]error{
		"create":   am.CreateUser("bob", credential, nil, true),
		"drop":     am.DropUser("bob"),
		"seed":     am.SeedRootUser("root", "root-password-1"),
		"password": am.SetPassword("bob", "bob-password-2"),
	}
	_, changes["alter"] = am.AlterUser(AlterSpec{Username: "bob"})
	_, changes["grant"] = am.GrantRole("bob", Grant{Role: "reader", DB: "*"})
	_, changes["revoke"] = am.RevokeRole("bob", Grant{Role: "reader", DB: "*"})
	for what, err := range changes {
		if !errors.Is(err, ErrUsersInCluster) {
			t.Errorf("%s on a cluster member answered %v, want ErrUsersInCluster", what, err)
		}
	}
	if !strings.Contains(ErrUsersInCluster.Error(), "create_user") {
		t.Errorf("the refusal does not name the ring user commands: %q", ErrUsersInCluster)
	}
	if len(am.UserInfos()) != 0 {
		t.Fatalf("the store changed: %+v", am.UserInfos())
	}
}

// TestAClusterMemberRefusesToStartWithAUsersFile (spec 7.3, 6.2): a cluster member never reads
// users.json, so finding one stops the start with the move-aside fix rather than ignoring
// accounts the operator may think exist. Break: skip the check; red (the service starts).
func TestAClusterMemberRefusesToStartWithAUsersFile(t *testing.T) {
	dir := t.TempDir()
	usersPath := UsersFilePath(dir)
	if err := os.WriteFile(usersPath, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dir
	cfg.Cluster.Enabled = true
	cfg.Metrics.Enabled = false
	_, err := NewService(cfg, discardLogger())
	if err == nil {
		t.Fatal("a cluster member started with a users.json in its data directory")
	}
	for _, want := range []string{"this node runs in cluster mode, where users live in the cluster's replicated state, so " + usersPath + " is not read. " +
		"Most likely it is from a start without cluster.enabled. Move it aside (mv " + usersPath + " " + usersPath + ".not-clustered) and start the node again. " +
		"If the cluster has no administrator yet, set HOARDDB_ROOT_PASSWORD on one data node before starting it"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal lacks %q: %v", want, err)
		}
	}
	requireNoOtherBuild(t, err.Error())
}

// TestAClusterMemberWritesNoUsersFile (spec 6.3): a cluster member is built with no users path and
// leaves none behind. Break: keep UsersPath in cluster mode; red.
func TestAClusterMemberWritesNoUsersFile(t *testing.T) {
	listen := ringClusterPorts(t)
	qs := ringClusterService(t, listen, nil, nil)
	if qs.core.auth.usersPath != "" || !qs.core.auth.clusterMember {
		t.Fatalf("a cluster member's AuthManager has usersPath %q, clusterMember %v", qs.core.auth.usersPath, qs.core.auth.clusterMember)
	}
	ringClusterStart(t, qs)
	if err := qs.Stop(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(UsersFilePath(qs.config.Storage.DataDir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a cluster member left a users.json: %v", err)
	}
}

// TestTheNoKeyTextIsChosenByCase (B7, spec 6.2): a password AUTH on a cluster member that cannot
// check passwords is answered with the case's text, before the connection's attempt is spent:
// here a cluster with no users yet. (The witness case runs across real members in
// cli.TestAUserCreatedThroughOneMemberLogsInOnAnother; the texts per case are the ring node's,
// pinned by the pepper tests.) Break: drop the AUTH pre-check; red (the answer is "invalid
// username or password").
func TestTheNoKeyTextIsChosenByCase(t *testing.T) {
	listen := ringClusterPorts(t)
	qs := ringClusterService(t, listen, nil, nil)
	ringClusterStart(t, qs)
	client := dialTCPServer(t, qs.tcpListener.Addr().String())
	for attempt := range 2 {
		frame, _, fail := client.authCall(&AuthRequest{Username: "root", Password: "any-password-1"})
		if frame.Opcode != transport.OpAuthFail {
			t.Fatalf("attempt %d: a login on a cluster with no users answered %s", attempt, frame.Opcode)
		}
		if !strings.HasPrefix(fail.Message, "this cluster has no users yet, so no login can succeed") {
			t.Fatalf("attempt %d: answered %q, want the no-users-yet text", attempt, fail.Message)
		}
	}
}
