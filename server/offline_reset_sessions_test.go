package server

import "testing"

// TestOfflineResetEndsTheUsersLoginsOnAFollower: a follower that receives the
// config of a master whose users file was reset offline ends a login opened
// before the reset.
func TestOfflineResetEndsTheUsersLoginsOnAFollower(t *testing.T) {
	const oldPassword = "alice-first-password-123"
	dir, cfg := setupAuthDir(t)
	master := newPersistentManager(t, dir)
	if err := master.BootstrapUser("root", "correct-horse-battery", true); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	credential, err := NewCredential(oldPassword, DefaultArgon2Params())
	if err != nil {
		t.Fatalf("NewCredential: %v", err)
	}
	if err := master.CreateUser("alice", credential, []Grant{{Role: "reader", DB: "app"}}, true); err != nil {
		t.Fatalf("CreateUser alice: %v", err)
	}

	follower := newTokenManager(t, master.keyring, nil)
	firstConfig := master.SnapshotConfig()
	if _, err := follower.applyConfigRevoking(firstConfig); err != nil {
		t.Fatalf("apply the first config: %v", err)
	}
	standing, err := follower.verifyLogin("alice", oldPassword)
	if err != nil {
		t.Fatalf("alice's login on the follower: %v", err)
	}

	if _, _, err := ResetPassword(cfg, "alice", "alice-reset-password-456", true, discardLogger()); err != nil {
		t.Fatalf("ResetPassword: %v", err)
	}
	restarted := newPersistentManager(t, dir)
	if _, _, err := restarted.LoadUsers(UsersFilePath(dir), nil, nil); err != nil {
		t.Fatalf("reload after reset: %v", err)
	}
	if _, err := follower.applyConfigRevoking(restarted.SnapshotConfig()); err != nil {
		t.Fatalf("apply the reset config on the follower: %v", err)
	}
	if !standing.Revoked() {
		t.Fatal("alice's login on the follower survived her password being reset offline")
	}
}
