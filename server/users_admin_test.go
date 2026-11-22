package server

import (
	"encoding/base64"
	"encoding/json"
	"github.com/hoarddb/hoarddb-server/transport"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// workFactorCase is one credential work factor and whether the clamp must
// accept it against the manager's configured default.
type workFactorCase struct {
	name     string
	params   Argon2Params
	accepted bool
}

func workFactorCases(configured Argon2Params) []workFactorCase {
	withIterations := func(iterations uint32) Argon2Params {
		params := configured
		params.Iterations = iterations
		return params
	}
	return []workFactorCase{
		{"configured default", configured, true},
		{"floor", withIterations(defaultArgon2Iterations), true},
		{"below the floor", withIterations(defaultArgon2Iterations - 1), false},
		{"at the ceiling", withIterations(2 * configured.Iterations), true},
		{"over the ceiling", withIterations(2*configured.Iterations + 1), false},
		{"far over the ceiling", withIterations(1 << 30), false},
	}
}

// TestCredentialWorkFactorIsBoundedOnBothSides runs every work-factor field's
// window through each path that reaches the clamp: user create, user alter,
// a pushed config and a file load. A refusal names the field and its range.
func TestCredentialWorkFactorIsBoundedOnBothSides(t *testing.T) {
	configured := defaultArgon2Params()
	configured.Iterations = 5

	newManager := func(t *testing.T) *AuthManager {
		t.Helper()
		keyring, err := BuildKeyring(t.TempDir(), "", "", time.Hour, discardLogger())
		if err != nil {
			t.Fatalf("BuildKeyring: %v", err)
		}
		return NewAuthManager(AuthTokenOptions{
			TTL: 15 * time.Minute, MaxLifetime: 12 * time.Hour, Skew: 60 * time.Second,
			Keyring: keyring, Argon2: configured,
		}, discardLogger())
	}
	material := func(name string, params Argon2Params) CredentialMaterial {
		return CredentialMaterial{Algo: "argon2id", Salt: testSalt(name), Hash: testHash(name), Params: params}
	}
	pushedConfig := func(params Argon2Params) *usersConfig {
		record := rootRecord("root")
		record.Credentials.Params = params
		return &usersConfig{FormatVersion: usersFormatVersion, Term: 3, Version: 9, Master: "m", Users: []UserRecord{record}}
	}

	paths := []struct {
		name string
		run  func(t *testing.T, params Argon2Params) error
	}{
		{"validator", func(_ *testing.T, params Argon2Params) error {
			return validateCredentialMaterial(material("v", params), configured)
		}},
		{"create", func(t *testing.T, params Argon2Params) error {
			return newManager(t).CreateUser("alice", material("alice", params), []Grant{{Role: "reader", DB: "app"}}, true)
		}},
		{"alter", func(t *testing.T, params Argon2Params) error {
			am := newManager(t)
			if err := am.CreateUser("alice", material("alice", configured), []Grant{{Role: "reader", DB: "app"}}, true); err != nil {
				t.Fatalf("create alice at the configured default: %v", err)
			}
			replacement := material("alice-2", params)
			_, err := am.AlterUser(AlterSpec{Username: "alice", Credential: &replacement, Version: am.Version()})
			return err
		}},
		{"pushed config", func(t *testing.T, params Argon2Params) error {
			return newManager(t).ApplyConfig(pushedConfig(params))
		}},
		{"file load", func(t *testing.T, params Argon2Params) error {
			usersPath := filepath.Join(t.TempDir(), UsersFileName)
			encoded, err := json.Marshal(pushedConfig(params))
			if err != nil {
				t.Fatalf("marshal users file: %v", err)
			}
			if err := os.WriteFile(usersPath, encoded, 0o600); err != nil {
				t.Fatalf("write users file: %v", err)
			}
			_, _, err = newManager(t).LoadUsers(usersPath, nil, nil)
			return err
		}},
	}

	for _, path := range paths {
		for _, tc := range workFactorCases(configured) {
			t.Run(path.name+"/"+tc.name, func(t *testing.T) {
				err := path.run(t, tc.params)
				switch {
				case tc.accepted && err != nil:
					t.Fatalf("refused, want accepted: %v", err)
				case !tc.accepted && err == nil:
					t.Fatalf("accepted iterations %d, want a refusal", tc.params.Iterations)
				case !tc.accepted && !strings.Contains(err.Error(), "iterations"):
					t.Fatalf("refusal %q does not name the field", err)
				case !tc.accepted && tc.params.Iterations > configured.Iterations &&
					!strings.Contains(err.Error(), "[3, 10]"):
					t.Fatalf("refusal %q does not name the permitted range [3, 10]", err)
				}
			})
		}
	}
}

// TestConfiguredWorkFactorAboveDefaultSurvivesSave pins that a server tuned
// above the package default can store and reload credentials derived at its
// own configured work factor: the save and the reload judge a record against
// the running configuration, not the package literal.
func TestConfiguredWorkFactorAboveDefaultSurvivesSave(t *testing.T) {
	configured := defaultArgon2Params()
	configured.Iterations = 4 * defaultArgon2Iterations
	configured.Memory = 4 * defaultArgon2Memory

	dir := t.TempDir()
	usersPath := filepath.Join(dir, UsersFileName)
	keyring, err := BuildKeyring(t.TempDir(), "", "", time.Hour, discardLogger())
	if err != nil {
		t.Fatalf("BuildKeyring: %v", err)
	}
	newManager := func() *AuthManager {
		return NewAuthManager(AuthTokenOptions{
			TTL: 15 * time.Minute, MaxLifetime: 12 * time.Hour, Skew: 60 * time.Second,
			Keyring: keyring, UsersPath: usersPath, Argon2: configured,
		}, discardLogger())
	}

	record := rootRecord("root")
	record.Credentials.Params = configured
	pushed := &usersConfig{FormatVersion: usersFormatVersion, Term: 2, Version: 5, Master: "m", Users: []UserRecord{record}}
	if err := newManager().ApplyConfig(pushed); err != nil {
		t.Fatalf("applying a config at the configured work factor: %v", err)
	}
	if _, _, err := newManager().LoadUsers(usersPath, nil, nil); err != nil {
		t.Fatalf("reloading the saved config: %v", err)
	}
}

// TestUserCredentialSaltMustDecodeAndMatchSaltLen pins BUG-0629: a salt the
// server cannot decode, or whose decoded length is not params.salt_len (and at
// least the 16-byte floor), is refused at create and at alter. Before the fix
// the account was stored and then could never log in.
func TestUserCredentialSaltMustDecodeAndMatchSaltLen(t *testing.T) {
	raw := func(length int) string { return base64.StdEncoding.EncodeToString(make([]byte, length)) }
	withSaltLen := func(saltLen int) func(*CredentialMaterial) {
		return func(material *CredentialMaterial) {
			material.Salt = raw(saltLen)
			material.Params.SaltLen = saltLen
		}
	}
	cases := []struct {
		name    string
		mutate  func(*CredentialMaterial)
		wantErr string // empty: accepted
	}{
		{"good", func(*CredentialMaterial) {}, ""},
		{"undecodable", func(m *CredentialMaterial) { m.Salt = "not*base64!" }, "salt must be standard base64"},
		{"decoded shorter than salt_len", func(m *CredentialMaterial) { m.Salt = raw(4) }, "decodes to 4 bytes"},
		{"decoded longer than salt_len", func(m *CredentialMaterial) { m.Salt = raw(64) }, "decodes to 64 bytes"},
		{"decoded below the floor, salt_len agrees", withSaltLen(8), "salt_len"},
	}
	newManager := func(t *testing.T) *AuthManager {
		t.Helper()
		keyring, err := BuildKeyring(t.TempDir(), "", "", time.Hour, discardLogger())
		if err != nil {
			t.Fatalf("BuildKeyring: %v", err)
		}
		return NewAuthManager(AuthTokenOptions{
			TTL: 15 * time.Minute, MaxLifetime: 12 * time.Hour, Skew: 60 * time.Second,
			Keyring: keyring, Argon2: defaultArgon2Params(),
		}, discardLogger())
	}
	derive := func(t *testing.T, password string) CredentialMaterial {
		t.Helper()
		credential := testCredential(t, password)
		return CredentialMaterial{Algo: credential.Algo, Salt: credential.Salt, Hash: credential.Hash, Params: credential.Params}
	}
	reader := []Grant{{Role: "reader", DB: "app"}}

	for _, tc := range cases {
		t.Run("create/"+tc.name, func(t *testing.T) {
			material := derive(t, "pw")
			tc.mutate(&material)
			am := newManager(t)
			err := am.CreateUser("alice", material, reader, true)
			assertSaltRefusal(t, err, tc.wantErr)
			if _, stored := am.users["alice"]; stored != (tc.wantErr == "") {
				t.Fatalf("account stored = %v after create with wantErr %q", stored, tc.wantErr)
			}
		})
		t.Run("alter/"+tc.name, func(t *testing.T) {
			am := newManager(t)
			if err := am.CreateUser("alice", derive(t, "old-pw"), reader, true); err != nil {
				t.Fatalf("create alice: %v", err)
			}
			replacement := derive(t, "new-pw")
			tc.mutate(&replacement)
			_, err := am.AlterUser(AlterSpec{Username: "alice", Credential: &replacement, Version: am.Version()})
			assertSaltRefusal(t, err, tc.wantErr)
			if tc.wantErr == "" {
				return
			}
			ok, verifyErr := am.VerifyUser("alice", "old-pw")
			if verifyErr != nil || !ok {
				t.Fatalf("refused alter locked the account: ok=%v err=%v", ok, verifyErr)
			}
		})
	}
}

func assertSaltRefusal(t *testing.T, err error, wantErr string) {
	t.Helper()
	if wantErr == "" {
		if err != nil {
			t.Fatalf("refused, want accepted: %v", err)
		}
		return
	}
	if err == nil {
		t.Fatalf("accepted, want a refusal containing %q", wantErr)
	}
	if got := codeFor(err); got != transport.ErrInvalidArgument {
		t.Fatalf("code = %v, want INVALID_ARGUMENT: %v", got, err)
	}
	if !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("refusal %q does not contain %q", err, wantErr)
	}
}

// TestAccountWithUnusableSaltDoesNotStopTheServer pins the BUG-0629 decision
// that only a write refuses a bad salt: a users file or pushed config that
// already holds one still loads (the account just cannot log in), because
// refusing would turn one stale record into a boot, save or replication outage.
func TestAccountWithUnusableSaltDoesNotStopTheServer(t *testing.T) {
	keyring, err := BuildKeyring(t.TempDir(), "", "", time.Hour, discardLogger())
	if err != nil {
		t.Fatalf("BuildKeyring: %v", err)
	}
	newManager := func() *AuthManager {
		return NewAuthManager(AuthTokenOptions{
			TTL: 15 * time.Minute, MaxLifetime: 12 * time.Hour, Skew: 60 * time.Second,
			Keyring: keyring, Argon2: defaultArgon2Params(),
		}, discardLogger())
	}
	record := rootRecord("root")
	record.Credentials.Salt = "not*base64!"
	cfg := &usersConfig{FormatVersion: usersFormatVersion, Term: 3, Version: 9, Master: "m", Users: []UserRecord{record}}

	usersPath := filepath.Join(t.TempDir(), UsersFileName)
	encoded, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(usersPath, encoded, 0o600); err != nil {
		t.Fatalf("write users file: %v", err)
	}
	if _, _, err := newManager().LoadUsers(usersPath, nil, nil); err != nil {
		t.Fatalf("a users file with an unusable salt refused to load: %v", err)
	}
	if err := newManager().ApplyConfig(cfg); err != nil {
		t.Fatalf("a pushed config with an unusable salt was refused: %v", err)
	}
}
