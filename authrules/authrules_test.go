package authrules

import (
	"slices"
	"testing"
)

func TestRoleIncludesFollowsThePrivilegeTable(t *testing.T) {
	for _, tc := range []struct {
		held, granted string
		want          bool
	}{
		{RoleRoot, RoleRestoreOperator, true},
		{RoleDBManager, RoleWriter, true},
		{RoleDBManager, RoleReader, true},
		{RoleWriter, RoleReader, true},
		{RoleBackupOperator, RoleReader, true},
		{RoleUserManager, RoleUserManager, true},
		{RoleUserManager, RoleReader, false},
		{RoleClusterManager, RoleUserManager, false},
		{RoleReader, RoleWriter, false},
	} {
		if got := RoleIncludes(tc.held, tc.granted); got != tc.want {
			t.Errorf("RoleIncludes(%q, %q) = %v, want %v", tc.held, tc.granted, got, tc.want)
		}
	}
}

func TestTheCeilingChecksGivenGrantsBeforeTheTargets(t *testing.T) {
	manager := []Grant{{RoleUserManager, AllDatabases}, {RoleWriter, "app"}}
	if _, ok := CheckCeiling(manager, []Grant{{RoleReader, "app"}}, []Grant{{RoleReader, "app"}}); !ok {
		t.Fatal("a writer of app was refused giving reader on app")
	}
	breach, ok := CheckCeiling(manager, []Grant{{RoleRoot, AllDatabases}}, []Grant{{RoleReader, "vault"}})
	if ok || breach.Grant != (Grant{RoleReader, "vault"}) || !breach.Given {
		t.Fatalf("breach = %+v, %v; want the given reader@vault first", breach, ok)
	}
	breach, ok = CheckCeiling(manager, []Grant{{RoleRoot, AllDatabases}}, nil)
	if ok || breach.Grant != (Grant{RoleRoot, AllDatabases}) || breach.Given {
		t.Fatalf("breach = %+v, %v; want the target's root@*", breach, ok)
	}
	if _, ok := CheckCeiling([]Grant{{RoleRoot, AllDatabases}}, []Grant{{RoleRoot, AllDatabases}}, []Grant{{RoleRoot, "x"}}); !ok {
		t.Fatal("root on every database has a ceiling")
	}
	if _, ok := CheckCeiling([]Grant{{RoleRoot, "app"}}, nil, []Grant{{RoleReader, "vault"}}); ok {
		t.Fatal("root on one database gave a role on another")
	}
}

func TestCanAdministerUsersCountsOnlyRootAndUserManagerOnEveryDatabase(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled bool
		grants  []Grant
		want    bool
	}{
		{"root on every database", true, []Grant{{RoleRoot, AllDatabases}}, true},
		{"user-manager on every database", true, []Grant{{RoleUserManager, AllDatabases}}, true},
		{"cluster-manager", true, []Grant{{RoleClusterManager, AllDatabases}}, false},
		{"user-manager on one database", true, []Grant{{RoleUserManager, "app"}}, false},
		{"root on one database", true, []Grant{{RoleRoot, "app"}}, false},
		{"disabled root", false, []Grant{{RoleRoot, AllDatabases}}, false},
	} {
		if got := CanAdministerUsers(tc.enabled, tc.grants); got != tc.want {
			t.Errorf("%s: CanAdministerUsers = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestNormalizeGrantsSortsDedupesAndCopies(t *testing.T) {
	in := []Grant{{RoleWriter, "b"}, {RoleReader, "a"}, {RoleWriter, "a"}, {RoleReader, "a"}}
	got := NormalizeGrants(in)
	want := []Grant{{RoleReader, "a"}, {RoleWriter, "a"}, {RoleWriter, "b"}}
	if !slices.Equal(got, want) {
		t.Fatalf("NormalizeGrants = %v, want %v", got, want)
	}
	if in[0] != (Grant{RoleWriter, "b"}) {
		t.Fatal("NormalizeGrants wrote into the slice it was given")
	}
}

func TestTheCredentialWindowIsInclusive(t *testing.T) {
	inside := func(m, i, p int64, s, h int) bool { return CredentialWindowViolation(m, i, p, s, h) == "" }
	if !inside(MinArgon2MemoryKiB, MinArgon2Iterations, MinArgon2Threads, MinSaltLen, Argon2HashLen) ||
		!inside(MaxArgon2MemoryKiB, MaxArgon2Iterations, MaxArgon2Threads, MaxSaltLen, Argon2HashLen) {
		t.Fatal("a bound of the window is outside it")
	}
	for name, outside := range map[string]bool{
		"memory below":  inside(MinArgon2MemoryKiB-1, 3, 4, 16, 32),
		"memory above":  inside(MaxArgon2MemoryKiB+1, 3, 4, 16, 32),
		"too few iters": inside(MinArgon2MemoryKiB, MinArgon2Iterations-1, 4, 16, 32),
		"too many":      inside(MinArgon2MemoryKiB, MaxArgon2Iterations+1, 4, 16, 32),
		"no threads":    inside(MinArgon2MemoryKiB, 3, 0, 16, 32),
		"many threads":  inside(MinArgon2MemoryKiB, 3, MaxArgon2Threads+1, 16, 32),
		"short salt":    inside(MinArgon2MemoryKiB, 3, 4, MinSaltLen-1, 32),
		"long salt":     inside(MinArgon2MemoryKiB, 3, 4, MaxSaltLen+1, 32),
		"short hash":    inside(MinArgon2MemoryKiB, 3, 4, 16, 31),
	} {
		if outside {
			t.Errorf("%s: accepted", name)
		}
	}
}
