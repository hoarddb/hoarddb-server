package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"slices"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/hoarddb/hoarddb-server/authrules"
	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/clusterpepper"
)

// The user-table follower (ring user store spec 6.3, step M2a.2): a cluster member's AuthManager
// follows the replicated table, installs it by created_seq, ends the logins a change revokes, and
// keeps a state_seq watermark the binding waits on.

// fakeUserTable is a user table source a test moves by hand.
type fakeUserTable struct {
	mu   sync.Mutex
	view cluster.UserView
	wake chan struct{}
}

func newFakeUserTable() *fakeUserTable { return &fakeUserTable{wake: make(chan struct{}, 1)} }

func (f *fakeUserTable) Users() cluster.UserView {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.view
}

func (f *fakeUserTable) UserTableChanged() <-chan struct{} { return f.wake }

// set installs view as the applied state and wakes the follower.
func (f *fakeUserTable) set(view cluster.UserView) {
	f.mu.Lock()
	f.view = view
	f.mu.Unlock()
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

// tableRow is a row of the cluster's table whose hash is password peppered under key.
func tableRow(name, password string, key clusterpepper.Key, kid string, createdSeq, changedSeq int64) cluster.UserRow {
	record := clusterRecord(name, password, key, kid, createdSeq)
	salt := bytes.Repeat([]byte(name[:1]), DefaultArgon2Params().SaltLen)
	hash := mustB64(record.Credentials.Hash)
	params := DefaultArgon2Params()
	return cluster.UserRow{Name: name, Roles: []authrules.Grant{{Role: "reader", DB: "*"}}, Algo: cluster.UserAlgo, Salt: salt, Hash: hash,
		PepperKID: kid, Memory: int64(params.Memory), Iterations: int64(params.Iterations), Parallelism: int32(params.Parallelism),
		Enabled: true, CreatedMs: 1_700_000_000_000, PasswordChangedMs: 1_700_000_000_000, CreatedSeq: createdSeq, ChangedSeq: changedSeq}
}

// followedManager is a cluster member's AuthManager holding the cluster key, with a follower over
// a fake table; closed records every account whose sessions the follower ended.
func followedManager(t *testing.T) (*AuthManager, *fakeUserTable, *userFollower, clusterpepper.Key, string, func() []string) {
	t.Helper()
	am := clusterMemberManager(t)
	pepper, key := testClusterPepper(t)
	am.SetClusterPasswordKey(fakeClusterKey{key: key, kid: pepper.KID()})
	table := newFakeUserTable()
	var mu sync.Mutex
	var closedNames []string
	closed := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(closedNames)
	}
	follower := newUserFollower(am, table, func(name string) int {
		mu.Lock()
		defer mu.Unlock()
		closedNames = append(closedNames, name)
		return 1
	}, discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	follower.start(ctx)
	t.Cleanup(func() {
		cancel()
		follower.wait()
	})
	return am, table, follower, key, pepper.KID(), closed
}

func awaitInstalled(t *testing.T, follower *userFollower, seq int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := follower.awaitInstalled(ctx, seq); err != nil {
		t.Fatalf("the follower did not install state_seq %d: %v", seq, err)
	}
}

// TestTheFollowerInstallsTheTableAndLoginsUseIt: a row of the table logs in on this node once the
// follower installed the state that holds it, and a login waits for nothing else.
func TestTheFollowerInstallsTheTableAndLoginsUseIt(t *testing.T) {
	am, table, follower, key, kid, _ := followedManager(t)
	table.set(cluster.UserView{Users: []cluster.UserRow{tableRow("ada", "ada-password-1", key, kid, 4, 4)}, PepperKID: kid, StateSeq: 4})
	awaitInstalled(t, follower, 4)
	if ok, err := am.VerifyUser("ada", "ada-password-1"); !ok || err != nil {
		t.Fatalf("login of an installed row: %v, %v", ok, err)
	}
	if info, ok := am.UserInfo("ada"); !ok || !info.Enabled || len(info.Roles) != 1 {
		t.Fatalf("installed record: %+v, %v", info, ok)
	}
}

// TestADropOnAFollowerAnswersSuccessPromptly (F5): the watermark is the state_seq the follower
// last installed, so a wait for a drop's commit ends at once although no row carries that seq.
// Break: wait on the highest changed_seq of the table; red (the wait times out).
func TestADropOnAFollowerAnswersSuccessPromptly(t *testing.T) {
	_, table, follower, key, kid, closed := followedManager(t)
	ada := tableRow("ada", "ada-password-1", key, kid, 4, 4)
	bob := tableRow("bob", "bob-password-1", key, kid, 5, 5)
	table.set(cluster.UserView{Users: []cluster.UserRow{ada, bob}, PepperKID: kid, StateSeq: 5})
	awaitInstalled(t, follower, 5)
	table.set(cluster.UserView{Users: []cluster.UserRow{ada}, PepperKID: kid, DropSeq: 9, StateSeq: 9})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := follower.awaitInstalled(ctx, 9); err != nil {
		t.Fatalf("waiting for the drop's commit: %v", err)
	}
	if !slices.Contains(closed(), "bob") {
		t.Errorf("the dropped account's sessions were not ended: %v", closed())
	}
}

// TestADropAndRecreateCoalescedIntoOneInstallEndsTheOldSessions (F17): a drop and a re-create of
// one name seen in one install (the proposer's clock fixed, so created_ms is the same) end the
// old account's logins, because accounts are told apart by created_seq. Break: compare
// CreatedAt; red.
func TestADropAndRecreateCoalescedIntoOneInstallEndsTheOldSessions(t *testing.T) {
	am, table, follower, key, kid, closed := followedManager(t)
	table.set(cluster.UserView{Users: []cluster.UserRow{tableRow("ada", "ada-password-1", key, kid, 4, 4)}, PepperKID: kid, StateSeq: 4})
	awaitInstalled(t, follower, 4)
	standing, err := am.verifyLogin("ada", "ada-password-1")
	if err != nil {
		t.Fatal(err)
	}
	// Dropped at 5 and created again at 6, read by the follower only at 6: same name, same
	// created_ms (the fixed clock), same login generation.
	table.set(cluster.UserView{Users: []cluster.UserRow{tableRow("ada", "ada-password-1", key, kid, 6, 6)}, PepperKID: kid, DropSeq: 5, StateSeq: 6})
	awaitInstalled(t, follower, 6)
	if !standing.Revoked() {
		t.Fatal("a login to the dropped account is still valid on the re-created one")
	}
	if !slices.Contains(closed(), "ada") {
		t.Errorf("the dropped account's sessions were not closed: %v", closed())
	}
}

// TestANewPasswordOrADisableEndsTheLoginsAndAGrantDoesNot (spec 6.3, F11): the follower ends the
// logins of a row whose login generation moved or that was disabled, and keeps those of a row
// that only gained a grant.
func TestANewPasswordOrADisableEndsTheLoginsAndAGrantDoesNot(t *testing.T) {
	am, table, follower, key, kid, _ := followedManager(t)
	rows := []cluster.UserRow{tableRow("ada", "pw-ada-1234", key, kid, 2, 2), tableRow("bob", "pw-bob-1234", key, kid, 3, 3), tableRow("cy", "pw-cy-12345", key, kid, 4, 4)}
	table.set(cluster.UserView{Users: rows, PepperKID: kid, StateSeq: 4})
	awaitInstalled(t, follower, 4)
	standings := map[string]interface{ Revoked() bool }{}
	for name, password := range map[string]string{"ada": "pw-ada-1234", "bob": "pw-bob-1234", "cy": "pw-cy-12345"} {
		standing, err := am.verifyLogin(name, password)
		if err != nil {
			t.Fatal(err)
		}
		standings[name] = standing
	}
	next := slices.Clone(rows)
	next[0].LoginGeneration, next[0].Epoch, next[0].ChangedSeq = 1, 1, 5
	next[1].Enabled, next[1].ChangedSeq = false, 6
	next[2].Roles, next[2].Epoch, next[2].ChangedSeq = append(slices.Clone(next[2].Roles), authrules.Grant{Role: "writer", DB: "app"}), 1, 7
	table.set(cluster.UserView{Users: next, PepperKID: kid, StateSeq: 7})
	awaitInstalled(t, follower, 7)
	if !standings["ada"].Revoked() || !standings["bob"].Revoked() {
		t.Error("a new password or a disable left the login valid")
	}
	if standings["cy"].Revoked() {
		t.Error("a grant ended the login")
	}
}

// TestAuthConfigVersionReportsTheLastInstalledStateSeq (spec 6.3, rule 11): in cluster mode the
// gauge hoarddb_auth_config_version is the state_seq the follower last installed, no new series.
func TestAuthConfigVersionReportsTheLastInstalledStateSeq(t *testing.T) {
	am, table, follower, key, kid, _ := followedManager(t)
	table.set(cluster.UserView{Users: []cluster.UserRow{tableRow("ada", "ada-password-1", key, kid, 4, 4)}, PepperKID: kid, StateSeq: 42})
	awaitInstalled(t, follower, 42)
	if got := authConfigGauge(t); got != 42 {
		t.Errorf("hoarddb_auth_config_version = %v, want 42", got)
	}
	if got := am.Version(); got != 42 {
		t.Errorf("Version() = %d, want 42", got)
	}
	// A commit that does not touch the table still moves the watermark and the gauge.
	view := table.Users()
	view.StateSeq = 43
	table.set(view)
	awaitInstalled(t, follower, 43)
	if got := authConfigGauge(t); got != 43 {
		t.Errorf("after a non-user commit the gauge is %v, want 43", got)
	}
}

// TestTheFollowerInstallsOnlyWhenTheTableChanged (spec 6.3, rule 11): copy-on-write makes a new
// table a new slice, so a commit that leaves the table alone costs an identity check, not an
// O(users) install. Break: install on every wake; red (the installs counter moves).
func TestTheFollowerInstallsOnlyWhenTheTableChanged(t *testing.T) {
	_, table, follower, key, kid, _ := followedManager(t)
	users := []cluster.UserRow{tableRow("ada", "ada-password-1", key, kid, 4, 4)}
	table.set(cluster.UserView{Users: users, PepperKID: kid, StateSeq: 4})
	awaitInstalled(t, follower, 4)
	before := follower.installs.Load()
	for seq := int64(5); seq < 15; seq++ {
		table.set(cluster.UserView{Users: users, PepperKID: kid, StateSeq: seq})
		awaitInstalled(t, follower, seq)
	}
	if got := follower.installs.Load() - before; got != 0 {
		t.Fatalf("%d installs for ten commits that left the table alone", got)
	}
}

func mustB64(text string) []byte {
	raw, err := base64.StdEncoding.DecodeString(text)
	if err != nil {
		panic(err) // a test helper over a value this file encoded itself
	}
	return raw
}

// authConfigGauge reads hoarddb_auth_config_version.
func authConfigGauge(t *testing.T) float64 {
	t.Helper()
	var metric dto.Metric
	if err := authConfigVersion.Write(&metric); err != nil {
		t.Fatal(err)
	}
	return metric.GetGauge().GetValue()
}
