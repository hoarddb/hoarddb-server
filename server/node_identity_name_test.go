package server

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// BUG-0172: NodeUser is the name a node presents to its peers. An account
// with the same name blurs audit attribution, so no path may create one.
func TestCreateUserRefusesTheNodeIdentityName(t *testing.T) {
	s := userTestServer(t)
	material := CredentialMaterial{Algo: "argon2id", Salt: testSalt("salt"), Hash: testHash("pw"), Params: defaultArgon2Params()}
	err := s.auth.CreateUser(NodeUser, material, nil, true)
	if err == nil {
		t.Fatalf("CreateUser(%q) succeeded, want a refusal", NodeUser)
	}
	for _, want := range []string{NodeUser, "reserved", "another"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not contain %q", err, want)
		}
	}
	if _, ok := s.auth.UserInfo(NodeUser); ok {
		t.Fatalf("a refused CreateUser still stored %q", NodeUser)
	}
}

func TestSeedRootUserRefusesTheNodeIdentityName(t *testing.T) {
	am := newTestAuthManager(t, discardLogger())
	err := am.SeedRootUser(NodeUser, "correct-horse-battery")
	if err == nil {
		t.Fatalf("SeedRootUser(%q) succeeded, want a refusal", NodeUser)
	}
	if !strings.Contains(err.Error(), "reserved") {
		t.Errorf("refusal %q does not say the name is reserved", err)
	}
	if _, ok := am.UserInfo(NodeUser); ok {
		t.Fatalf("a refused SeedRootUser still stored %q", NodeUser)
	}
}

// A store written before the reservation may already hold the account. It
// must still load (refusing to start would turn a naming defect into an
// outage) and must be named in a warning.
func TestLoadUsersWarnsAboutAnExistingNodeIdentityAccount(t *testing.T) {
	dir, _ := setupAuthDir(t)
	writer := newPersistentManager(t, dir)
	if err := writer.BootstrapUser("root", "correct-horse-battery", true); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	writer.mu.Lock()
	writer.users[NodeUser] = &UserRecord{Username: NodeUser, Enabled: true, Credentials: writer.users["root"].Credentials}
	err := writer.persistLocked()
	writer.mu.Unlock()
	if err != nil {
		t.Fatalf("persist planted account: %v", err)
	}

	var logOutput bytes.Buffer
	reader := newPersistentManager(t, dir)
	reader.logger = slog.New(slog.NewTextHandler(&logOutput, nil))
	if _, _, err := reader.LoadUsers(UsersFilePath(dir), nil, nil); err != nil {
		t.Fatalf("LoadUsers refused a store holding %q: %v", NodeUser, err)
	}
	if !strings.Contains(logOutput.String(), NodeUser) || !strings.Contains(logOutput.String(), "level=WARN") {
		t.Fatalf("no warning naming %q; log was:\n%s", NodeUser, logOutput.String())
	}
}
