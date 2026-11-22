package server

import (
	"errors"
	"strings"
	"testing"
)

// A username is printed by user listings, logs and audit rows, and a terminal
// acts on a control character instead of showing it. A create refuses one,
// with the same rule database, bucket and field names follow; an account made
// before the rule stays usable.
func TestCreateUserRefusesControlCharactersInTheName(t *testing.T) {
	material := CredentialMaterial{Algo: "argon2id", Salt: testSalt("salt"), Hash: testHash("pw"), Params: defaultArgon2Params()}
	tests := []struct {
		name     string
		username string
		wantCode string
	}{
		{"escape sequence", "alice\x1b[2J", "U+001B"},
		{"newline", "bob\nadmin", "U+000A"},
		{"delete", "carol\x7f", "U+007F"},
		{"C1 control", "dave\u0085", "U+0085"},
	}
	for _, tc := range tests {
		t.Run("seed root user, "+tc.name, func(t *testing.T) {
			am := newTestAuthManager(t, discardLogger())
			if err := am.SeedRootUser(tc.username, "correct-horse-battery"); !errors.Is(err, ErrUsernameInvalid) {
				t.Errorf("SeedRootUser(%q) = %v, want ErrUsernameInvalid", tc.username, err)
			}
		})
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := userTestServer(t)
			err := s.auth.CreateUser(tc.username, material, nil, true)
			if err == nil {
				t.Fatalf("CreateUser(%q) succeeded, want a refusal", tc.username)
			}
			if !errors.Is(err, ErrUsernameInvalid) {
				t.Errorf("CreateUser(%q) error %v does not wrap ErrUsernameInvalid", tc.username, err)
			}
			for _, want := range []string{"control character", tc.wantCode, "create user <name>"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal %q does not contain %q", err, want)
				}
			}
			if strings.ContainsAny(err.Error(), "\x1b\x7f\n\u0085") {
				t.Errorf("refusal %q echoes the control character raw", err)
			}
			if _, ok := s.auth.UserInfo(tc.username); ok {
				t.Fatalf("a refused CreateUser still stored %q", tc.username)
			}
		})
	}
}

func TestCreateUserRefusesANameOverTheLimit(t *testing.T) {
	s := userTestServer(t)
	material := CredentialMaterial{Algo: "argon2id", Salt: testSalt("salt"), Hash: testHash("pw"), Params: defaultArgon2Params()}
	err := s.auth.CreateUser(strings.Repeat("a", maxNameBytes+1), material, nil, true)
	if !errors.Is(err, ErrUsernameInvalid) || !strings.Contains(err.Error(), "too long") || len(err.Error()) > 600 {
		t.Fatalf("CreateUser over the limit = %v, want a short 'too long' refusal wrapping ErrUsernameInvalid", err)
	}
	if err := s.auth.CreateUser(strings.Repeat("a", maxNameBytes), material, nil, true); err != nil {
		t.Errorf("CreateUser at the limit: %v", err)
	}
}

func TestCreateUserKeepsAcceptingPrintableNames(t *testing.T) {
	s := userTestServer(t)
	material := CredentialMaterial{Algo: "argon2id", Salt: testSalt("salt"), Hash: testHash("pw"), Params: defaultArgon2Params()}
	for _, username := range []string{"alice", "bob.smith@example.com", "Zoë", "日本語"} {
		material.Salt = testSalt("salt-" + username)
		if err := s.auth.CreateUser(username, material, nil, true); err != nil {
			t.Errorf("CreateUser(%q): %v", username, err)
		}
	}
}

// An account created before the rule must still be altered and dropped, or it
// could never be removed.
func TestExistingControlCharacterNameStaysManageable(t *testing.T) {
	s := userTestServer(t)
	legacy := "legacy\x07"
	s.auth.mu.Lock()
	s.auth.users[legacy] = &UserRecord{Username: legacy, Enabled: true, Roles: []Grant{{Role: "root", DB: "*"}}}
	s.auth.users["other"] = &UserRecord{Username: "other", Enabled: true, Roles: []Grant{{Role: "root", DB: "*"}}}
	s.auth.mu.Unlock()

	disabled := false
	if _, err := s.auth.AlterUser(AlterSpec{Username: legacy, Enabled: &disabled}); err != nil {
		t.Fatalf("AlterUser on an existing name with a control character: %v", err)
	}
	if err := s.auth.DropUser(legacy); err != nil {
		t.Fatalf("DropUser on an existing name with a control character: %v", err)
	}
}
