package server

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/sealed"
)

// rootRecord is a valid minimal record for round-trip and validation tests.
func rootRecord(username string) UserRecord {
	return UserRecord{
		Username: username,
		Roles:    []Grant{{Role: "root", DB: "*"}},
		Credentials: CredentialMaterial{
			Algo:      "argon2id",
			Salt:      testSalt("salt"),
			Hash:      "aGFzaA==",
			Params:    defaultArgon2Params(),
			PepperKID: "deadbeef",
		},
		Enabled:           true,
		CreatedAt:         time.Unix(1_700_000_000, 0).UTC(),
		PasswordChangedAt: time.Unix(1_700_000_000, 0).UTC(),
	}
}

func validUsersConfig() *usersConfig {
	return &usersConfig{
		FormatVersion: usersFormatVersion,
		Term:          41,
		Version:       12,
		Master:        "node-a",
		Users:         []UserRecord{rootRecord("root")},
	}
}

func TestUsersConfigRoundTrips(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)
	want := validUsersConfig()
	if err := want.Save(path, usersSealing{Keyring: nil, NodeMaster: nil, Mode: "off"}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if mode := info.Mode().Perm(); mode != 0600 {
		t.Fatalf("users.json mode = %o, want 0600", mode)
	}

	got, err := LoadUsersConfig(path, nil, nil)
	if err != nil {
		t.Fatalf("LoadUsersConfig: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round-tripped config differs\n got: %+v\nwant: %+v", got, want)
	}
}

func TestUsersConfigSaveKeepsPrev(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)
	first := validUsersConfig()
	if err := first.Save(path, usersSealing{Keyring: nil, NodeMaster: nil, Mode: "off"}); err != nil {
		t.Fatalf("first Save: %v", err)
	}
	second := validUsersConfig()
	second.Version = 13
	second.Users = append(second.Users, UserRecord{
		Username:          "alice",
		Roles:             []Grant{{Role: "writer", DB: "app"}},
		Credentials:       rootRecord("alice").Credentials,
		Enabled:           true,
		CreatedAt:         first.Users[0].CreatedAt,
		PasswordChangedAt: first.Users[0].PasswordChangedAt,
	})
	if err := second.Save(path, usersSealing{Keyring: nil, NodeMaster: nil, Mode: "off"}); err != nil {
		t.Fatalf("second Save: %v", err)
	}

	prev, err := LoadUsersConfig(path+".prev", nil, nil)
	if err != nil {
		t.Fatalf("LoadUsersConfig(prev): %v", err)
	}
	if !reflect.DeepEqual(prev, first) {
		t.Fatalf("users.json.prev is not the previous config")
	}
}

func TestCorruptUsersFileFailsTheStart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)
	if err := os.WriteFile(path, []byte("{not json"), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := LoadUsersConfig(path, nil, nil)
	if err == nil {
		t.Fatal("LoadUsersConfig corrupt file = nil error, want failure")
	}
	if !strings.Contains(err.Error(), path) {
		t.Fatalf("error %q does not name the path %s", err, path)
	}
}

func TestLoadUsersConfigMissingIsFirstStart(t *testing.T) {
	_, err := LoadUsersConfig(filepath.Join(t.TempDir(), UsersFileName), nil, nil)
	if !errors.Is(err, ErrNoUsersFile) {
		t.Fatalf("missing file error = %v, want ErrNoUsersFile", err)
	}
}

func TestLogicallyWrongConfigIsRefused(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*usersConfig)
		want   string
	}{
		{"empty user list", func(c *usersConfig) { c.Users = nil }, "no users"},
		{"no administrator", func(c *usersConfig) {
			c.Users = []UserRecord{{
				Username:    "reader",
				Roles:       []Grant{{Role: "reader", DB: "app"}},
				Credentials: rootRecord("reader").Credentials,
				Enabled:     true,
			}}
		}, "lock everyone out"},
		{
			"disabled administrator only", func(c *usersConfig) {
				c.Users = []UserRecord{rootRecord("root")}
				c.Users[0].Enabled = false
			}, "lock everyone out",
		},
		{"cluster-manager the only administrator", func(c *usersConfig) {
			c.Users = []UserRecord{rootRecord("infra")}
			c.Users[0].Roles = []Grant{{Role: "cluster-manager", DB: "*"}}
		}, "every other account in the old file is lost"},
		{"cluster-manager only, the hand-edit route named", func(c *usersConfig) {
			c.Users = []UserRecord{rootRecord("infra")}
			c.Users[0].Roles = []Grant{{Role: "cluster-manager", DB: "*"}}
		}, "If users.json is plaintext JSON (a file from before encryption), give an enabled account root@* or user-manager@* in its roles"},
		{"unknown format version", func(c *usersConfig) { c.FormatVersion = 99 }, "unsupported format_version"},
		{"duplicate user", func(c *usersConfig) {
			c.Users = append(c.Users, c.Users[0])
		}, "more than once"},
		{"memory below floor", func(c *usersConfig) {
			c.Users[0].Credentials.Params.Memory = 8
		}, "memory 8"},
		{"memory above ceiling", func(c *usersConfig) {
			c.Users[0].Credentials.Params.Memory = 1 << 30
		}, "memory 1073741824"},
		{"wrong algo", func(c *usersConfig) {
			c.Users[0].Credentials.Algo = "bcrypt"
		}, "argon2id"},
		{"hash length wrong", func(c *usersConfig) {
			c.Users[0].Credentials.Params.HashLen = 16
		}, "hash_len 16"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validUsersConfig()
			tc.mutate(cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("Validate = nil, want refusal")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

func TestUsersConfigSaveRefusesInvalid(t *testing.T) {
	path := filepath.Join(t.TempDir(), UsersFileName)
	cfg := validUsersConfig()
	cfg.Users = nil
	if err := cfg.Save(path, usersSealing{Keyring: nil, NodeMaster: nil, Mode: "off"}); err == nil {
		t.Fatal("Save invalid config = nil error, want refusal")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Save wrote a file for an invalid config: %v", err)
	}
}

func TestUsersConfigNewerThan(t *testing.T) {
	cfg := validUsersConfig() // term 41, version 12
	cases := []struct {
		term, version uint64
		want          bool
	}{
		{41, 11, true},
		{41, 12, false},
		{41, 13, false},
		{40, 999, true},
		{42, 0, false},
	}
	for _, tc := range cases {
		if got := cfg.NewerThan(tc.term, tc.version); got != tc.want {
			t.Fatalf("NewerThan(%d,%d) = %v, want %v", tc.term, tc.version, got, tc.want)
		}
	}
}

func TestUsersConfigJSONLayoutIsFrozen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)
	if err := validUsersConfig().Save(path, usersSealing{Keyring: nil, NodeMaster: nil, Mode: "off"}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	// The field names operators and scripts will point at (spec 3.2, 5.1).
	for _, want := range []string{
		`"format_version": 1`,
		`"term": 41`,
		`"version": 12`,
		`"master": "node-a"`,
		`"user": "root"`,
		`"role": "root"`,
		`"db": "*"`,
		`"pepper_kid": "deadbeef"`,
		`"salt_len": 16`,
		`"hash_len": 32`,
	} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("users.json is missing %s\n%s", want, data)
		}
	}
	var generic map[string]any
	if err := json.Unmarshal(data, &generic); err != nil {
		t.Fatalf("users.json is not valid JSON: %v", err)
	}
}

// TestUsersSaveSurfacesPrevDeletionError asserts that when the sealing
// kid changes, Save attempts to delete the stale .prev backup as its last
// step. If that deletion fails, the failure must not be swallowed — Save
// must return an error naming the backup path, not nil.
func TestUsersSaveSurfacesPrevDeletionError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)
	cfg := validUsersConfig()

	nm1Bytes := make([]byte, 32)
	for i := range nm1Bytes {
		nm1Bytes[i] = byte(i)
	}
	nm1 := &NodeMaster{bytes: nm1Bytes, kid: sealed.Kid(nm1Bytes), created: false}

	if err := cfg.Save(path, usersSealing{Keyring: nil, NodeMaster: nm1, Mode: "on"}); err != nil {
		t.Fatalf("first Save: %v", err)
	}

	nm2Bytes := make([]byte, 32)
	for i := range nm2Bytes {
		nm2Bytes[i] = byte(i + 100)
	}
	nm2 := &NodeMaster{bytes: nm2Bytes, kid: sealed.Kid(nm2Bytes), created: false}

	oldDeleter := prevFileDeleter
	deletionErr := errors.New("permission denied")
	prevFileDeleter = func(string) error { return deletionErr }
	defer func() { prevFileDeleter = oldDeleter }()

	cfg.Version = 13
	err := cfg.Save(path, usersSealing{Keyring: nil, NodeMaster: nm2, Mode: "on"})
	if err == nil {
		t.Fatal("Save returned nil despite a failing .prev deletion; the error must be surfaced")
	}
	if !errors.Is(err, deletionErr) {
		t.Fatalf("Save error does not wrap the deletion error: %v", err)
	}
	wantPath := path + ".prev"
	if !strings.Contains(err.Error(), "removing previous credentials backup") || !strings.Contains(err.Error(), wantPath) {
		t.Fatalf("Save error = %q, want it to name %q and read \"removing previous credentials backup\"", err.Error(), wantPath)
	}
}

// TestUsersSaveNormalPathUnchanged asserts the fix does not disturb the
// ordinary case: a real .prev deletion still succeeds and Save still
// returns nil.
func TestUsersSaveNormalPathUnchanged(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, UsersFileName)
	cfg := validUsersConfig()

	nm1Bytes := make([]byte, 32)
	for i := range nm1Bytes {
		nm1Bytes[i] = byte(i)
	}
	nm1 := &NodeMaster{bytes: nm1Bytes, kid: sealed.Kid(nm1Bytes), created: false}

	if err := cfg.Save(path, usersSealing{Keyring: nil, NodeMaster: nm1, Mode: "on"}); err != nil {
		t.Fatalf("first Save: %v", err)
	}

	nm2Bytes := make([]byte, 32)
	for i := range nm2Bytes {
		nm2Bytes[i] = byte(i + 100)
	}
	nm2 := &NodeMaster{bytes: nm2Bytes, kid: sealed.Kid(nm2Bytes), created: false}

	cfg.Version = 13
	if err := cfg.Save(path, usersSealing{Keyring: nil, NodeMaster: nm2, Mode: "on"}); err != nil {
		t.Fatalf("second Save with real prevFileDeleter: %v", err)
	}

	if _, err := os.Stat(path + ".prev"); !os.IsNotExist(err) {
		t.Fatalf("expected .prev to be deleted, Stat err = %v", err)
	}
}

// TestUsersFileAtParallelismFourLoadsOnOneCPU covers the restart refusal: a
// users.json written at the default parallelism of 4 on a larger host was
// refused at load once Go saw one CPU, so a node did not come back after its
// CPU allowance shrank.
func TestUsersFileAtParallelismFourLoadsOnOneCPU(t *testing.T) {
	path := filepath.Join(t.TempDir(), UsersFileName)
	pinGOMAXPROCS(t, 4)
	written := validUsersConfig()
	if err := written.Save(path, usersSealing{Mode: "off"}); err != nil {
		t.Fatalf("Save at GOMAXPROCS=4: %v", err)
	}

	pinGOMAXPROCS(t, 1)
	am := newTokenManager(t, nil, nil)
	if _, _, err := am.LoadUsers(path, nil, nil); err != nil {
		t.Fatalf("LoadUsers of a parallelism-4 file at GOMAXPROCS=1: %v", err)
	}
	loaded, err := LoadUsersConfig(path, nil, nil)
	if err != nil {
		t.Fatalf("LoadUsersConfig: %v", err)
	}
	if err := loaded.validateWith(am.argon2); err != nil {
		t.Fatalf("validateWith at GOMAXPROCS=1: %v", err)
	}
}

// TestPushedParallelismFourConfigAppliesOnOneCPU covers the pushed-config
// refusal: a config authored on a 4+ CPU master was refused by a smaller
// member, which then stayed on its old config.
func TestPushedParallelismFourConfigAppliesOnOneCPU(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "root")

	pinGOMAXPROCS(t, 1)
	pushed := &usersConfig{FormatVersion: usersFormatVersion, Term: 7, Version: 3, Master: "m",
		Users: []UserRecord{rootRecord("root"), rootRecord("operator")}}
	if err := am.ApplyConfig(pushed); err != nil {
		t.Fatalf("ApplyConfig of a parallelism-4 config at GOMAXPROCS=1: %v", err)
	}
	if got := len(am.UserInfos()); got != 2 {
		t.Fatalf("after the push the member holds %d users, want 2", got)
	}
}

// TestCredentialClampEdgesDoNotFollowTheHost keeps the §6.2 clamp's edges
// now that the parallelism bound is a constant: each case names the
// guard in validateCredentialMaterial it exercises, and the answer must be the
// same on a 1-CPU host and a 64-CPU one.
func TestCredentialClampEdgesDoNotFollowTheHost(t *testing.T) {
	const parallelismCeiling = 16
	cases := []struct {
		name   string
		mutate func(*Argon2Params)
		want   string // "" means accepted
	}{
		{"parallelism lower guard: 0", func(p *Argon2Params) { p.Parallelism = 0 }, "parallelism 0 is outside the permitted range [1, 16]"},
		{"parallelism at the floor: 1", func(p *Argon2Params) { p.Parallelism = 1 }, ""},
		{"parallelism at the ceiling: 16", func(p *Argon2Params) { p.Parallelism = parallelismCeiling }, ""},
		{"parallelism upper guard: 17", func(p *Argon2Params) { p.Parallelism = parallelismCeiling + 1 }, "parallelism 17 is outside the permitted range [1, 16]"},
		{"parallelism upper guard: 255", func(p *Argon2Params) { p.Parallelism = 255 }, "parallelism 255 is outside the permitted range [1, 16]"},
		{"memory lower guard", func(p *Argon2Params) { p.Memory = defaultArgon2Memory - 1 }, "memory 65535 is outside the permitted range"},
		{"memory upper guard", func(p *Argon2Params) { p.Memory = 2*defaultArgon2Memory + 1 }, "memory 131073 is outside the permitted range"},
		{"iterations lower guard", func(p *Argon2Params) { p.Iterations = defaultArgon2Iterations - 1 }, "iterations 2 is below the minimum of 3"},
	}
	for _, procs := range []int{1, 64} {
		pinGOMAXPROCS(t, procs)
		for _, tc := range cases {
			material := rootRecord("root").Credentials
			tc.mutate(&material.Params)
			err := validateCredentialMaterial(material, defaultArgon2Params())
			switch {
			case tc.want == "" && err != nil:
				t.Errorf("GOMAXPROCS=%d %s: refused, want accepted: %v", procs, tc.name, err)
			case tc.want != "" && err == nil:
				t.Errorf("GOMAXPROCS=%d %s: accepted, want a refusal containing %q", procs, tc.name, tc.want)
			case tc.want != "" && !strings.Contains(err.Error(), tc.want):
				t.Errorf("GOMAXPROCS=%d %s: error %q does not contain %q", procs, tc.name, err, tc.want)
			}
		}
	}
}

// TestCredentialValidatorUsesTheConfigRanges pins that the §6.2 clamp and the
// config loader share one set of work-factor bounds. Config.Validate refuses
// a configured default outside them at load (BUG-0228) and the validator
// refuses a client value outside them on receipt, so a value one accepts and
// the other refuses cannot drift apart again (BUG-0226's [1, 16] parallelism
// bound). The validator is exercised at each config edge and one step past it.
func TestCredentialValidatorUsesTheConfigRanges(t *testing.T) {
	configured := defaultArgon2Params()
	cases := []struct {
		name   string
		set    func(*Argon2Params)
		refuse bool
	}{
		{"memory at config.MinArgon2Memory", func(p *Argon2Params) { p.Memory = config.MinArgon2Memory }, false},
		{"memory below config.MinArgon2Memory", func(p *Argon2Params) { p.Memory = config.MinArgon2Memory - 1 }, true},
		{"iterations at config.MinArgon2Iterations", func(p *Argon2Params) { p.Iterations = config.MinArgon2Iterations }, false},
		{"iterations below config.MinArgon2Iterations", func(p *Argon2Params) { p.Iterations = config.MinArgon2Iterations - 1 }, true},
		{"parallelism at config.MinArgon2Parallelism", func(p *Argon2Params) { p.Parallelism = config.MinArgon2Parallelism }, false},
		{"parallelism at config.MaxArgon2Parallelism", func(p *Argon2Params) { p.Parallelism = config.MaxArgon2Parallelism }, false},
		{"parallelism below config.MinArgon2Parallelism", func(p *Argon2Params) { p.Parallelism = config.MinArgon2Parallelism - 1 }, true},
		{"parallelism above config.MaxArgon2Parallelism", func(p *Argon2Params) { p.Parallelism = config.MaxArgon2Parallelism + 1 }, true},
	}
	for _, tc := range cases {
		params := configured
		tc.set(&params)
		material := rootRecord("root").Credentials
		material.Params = params
		err := validateCredentialMaterial(material, configured)
		switch {
		case tc.refuse && err == nil:
			t.Errorf("%s: accepted, want a refusal", tc.name)
		case !tc.refuse && err != nil:
			t.Errorf("%s: refused, want accepted: %v", tc.name, err)
		}
	}
}
