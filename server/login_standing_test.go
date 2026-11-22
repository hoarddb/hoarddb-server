package server

import (
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestPushedConfigRevokesOnlyLoginsItChanges: a member applying the master's
// config ends the logins of an account the config drops, disables or gives new
// credentials, and leaves every other login alone, including one opened
// against this node's own first-start account, which the first push replaces
// at an epoch no higher.
func TestPushedConfigRevokesOnlyLoginsItChanges(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "admin")
	for _, name := range []string{"kept", "regranted", "disabled", "repassworded", "dropped"} {
		seedUser(t, am, name)
	}
	standings := map[string]*transport.LoginStanding{}
	for _, name := range []string{"admin", "kept", "regranted", "disabled", "repassworded", "dropped"} {
		standing, err := am.verifyLogin(name, "correct-horse-battery")
		if err != nil {
			t.Fatalf("login as %s: %v", name, err)
		}
		standings[name] = standing
	}

	current := am.SnapshotConfig()
	byName := map[string]UserRecord{}
	for _, record := range current.Users {
		byName[record.Username] = record
	}
	// admin: the cluster's own account of that name, with other credentials
	// at the same epoch, as the first push to a node that seeded its own.
	admin := rootRecord("admin")
	admin.Credentials.Salt = testSalt("cluster-admin")
	admin.FirstStart = true
	regranted := byName["regranted"]
	regranted.Roles = append(regranted.Roles, Grant{Role: "reader", DB: "app"})
	regranted.Epoch++
	disabled := byName["disabled"]
	disabled.Enabled = false
	disabled.Epoch++
	disabled.LoginGeneration++
	repassworded := byName["repassworded"]
	repassworded.Credentials.Salt = testSalt("new-salt")
	repassworded.Epoch++
	repassworded.LoginGeneration++

	pushed := &usersConfig{
		FormatVersion: usersFormatVersion,
		Term:          current.Term + 1,
		Version:       current.Version + 1,
		Master:        "node-a",
		Users:         []UserRecord{admin, byName["kept"], regranted, disabled, repassworded},
	}
	revoked, err := am.applyConfigRevoking(pushed)
	if err != nil {
		t.Fatalf("apply the pushed config: %v", err)
	}
	wantRevoked := map[string]bool{"disabled": true, "repassworded": true, "dropped": true}
	if len(revoked) != len(wantRevoked) {
		t.Errorf("revoked %v, want exactly %v", revoked, wantRevoked)
	}
	for name, standing := range standings {
		if got := standing.Revoked(); got != wantRevoked[name] {
			t.Errorf("%s's login revoked = %v, want %v", name, got, wantRevoked[name])
		}
	}
}

// followerPair is a master's user store and a follower's, sharing the key
// material a cluster shares, with the follower holding the master's config.
type followerPair struct {
	master   *AuthManager
	follower *AuthManager
}

func newFollowerPair(t *testing.T) followerPair {
	t.Helper()
	keyring, err := BuildKeyring(t.TempDir(), strings.Repeat("c", 64), "", time.Hour, discardLogger())
	if err != nil {
		t.Fatalf("BuildKeyring: %v", err)
	}
	pair := followerPair{master: newTokenManager(t, keyring, nil), follower: newTokenManager(t, keyring, nil)}
	seedUser(t, pair.master, "admin")
	return pair
}

// push hands the follower the master's current config, as the master's push
// does; only the final state arrives, as when pushes coalesce or the follower
// missed one.
func (p followerPair) push(t *testing.T) []string {
	t.Helper()
	cfg := p.master.SnapshotConfig()
	_, followerVersion := p.follower.ConfigTermVersion()
	cfg.Term = 1
	if cfg.Version <= followerVersion {
		cfg.Version = followerVersion + 1
	}
	revoked, err := p.follower.applyConfigRevoking(cfg)
	if err != nil {
		t.Fatalf("apply the master's config on the follower: %v", err)
	}
	return revoked
}

// createAlice creates alice on the master with password.
func (p followerPair) createAlice(t *testing.T, password string) {
	t.Helper()
	credential, err := NewCredential(password, DefaultArgon2Params())
	if err != nil {
		t.Fatalf("NewCredential: %v", err)
	}
	if err := p.master.CreateUser("alice", credential, []Grant{{Role: "reader", DB: "app"}}, true); err != nil {
		t.Fatalf("CreateUser alice: %v", err)
	}
}

// TestFollowerEndsLoginsOfAnAccountChangedBetweenPushes: a follower that sees
// only the final config still ends a login whose account was dropped and
// created again under the same name, or disabled and enabled again, in
// between: neither is the account the session logged in to.
func TestFollowerEndsLoginsOfAnAccountChangedBetweenPushes(t *testing.T) {
	const password = "alice-first-password-123"
	for _, tc := range []struct {
		name   string
		change func(t *testing.T, p followerPair)
	}{
		{"dropped and created again", func(t *testing.T, p followerPair) {
			if err := p.master.DropUser("alice"); err != nil {
				t.Fatalf("DropUser: %v", err)
			}
			p.createAlice(t, "alice-second-password-123")
		}},
		{"disabled and enabled again", func(t *testing.T, p followerPair) {
			disabled, enabled := false, true
			if _, err := p.master.AlterUser(AlterSpec{Username: "alice", Enabled: &disabled, Version: p.master.Version()}); err != nil {
				t.Fatalf("disable: %v", err)
			}
			if _, err := p.master.AlterUser(AlterSpec{Username: "alice", Enabled: &enabled, Version: p.master.Version()}); err != nil {
				t.Fatalf("enable: %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pair := newFollowerPair(t)
			pair.createAlice(t, password)
			pair.push(t)
			standing, err := pair.follower.verifyLogin("alice", password)
			if err != nil {
				t.Fatalf("alice's login on the follower: %v", err)
			}

			tc.change(t, pair)
			pair.push(t)
			if !standing.Revoked() {
				t.Fatalf("alice's login on the follower survived her account being %s", tc.name)
			}
		})
	}
}

// TestFirstPushKeepsLoginsToTheNodesOwnFirstStartAccount: a node that created
// its own root account at its first start, and served logins with it, keeps
// those logins when the master's config replaces that account with the
// cluster's root account of the same name.
func TestFirstPushKeepsLoginsToTheNodesOwnFirstStartAccount(t *testing.T) {
	pair := newFollowerPair(t)
	seedUser(t, pair.follower, "admin")
	standing, err := pair.follower.verifyLogin("admin", "correct-horse-battery")
	if err != nil {
		t.Fatalf("admin's login on the new node: %v", err)
	}
	pair.push(t)
	if standing.Revoked() {
		t.Fatal("the first push ended a login to the node's own first-start root account")
	}
}

// recordForTest returns a copy of username's record.
func (am *AuthManager) recordForTest(username string) (UserRecord, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	record, ok := am.users[username]
	if !ok {
		return UserRecord{}, false
	}
	return *record, true
}
