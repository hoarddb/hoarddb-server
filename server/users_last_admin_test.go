package server

import (
	"errors"
	"strings"
	"testing"
)

// TestDroppingTheLastUserManagerIsRefusedWhileAClusterManagerRemains is the zero-config half of
// the one last-administrator predicate (ring user store spec 4.2, F3 and B8): a cluster-manager
// cannot create, alter or grant users, so it does not count, and the last account holding
// user-manager on every database is refused a drop, a disable, a stripping alter and a revoke. The
// refusal names the grant to give first. The cluster's half is in package cluster under the same
// name. Break: count cluster-manager in authrules.CanAdministerUsers; red.
func TestDroppingTheLastUserManagerIsRefusedWhileAClusterManagerRemains(t *testing.T) {
	s := userTestServer(t)
	for name, grant := range map[string]Grant{"ops": {Role: "user-manager", DB: "*"}, "infra": {Role: "cluster-manager", DB: "*"}} {
		if err := s.auth.CreateUser(name, CredentialMaterial{
			Algo: "argon2id", Salt: testSalt(name), Hash: testHash(name), Params: defaultArgon2Params(),
		}, []Grant{grant}, true); err != nil {
			t.Fatalf("CreateUser(%s): %v", name, err)
		}
	}
	disabled := false
	for change, err := range map[string]error{
		"drop":    s.auth.DropUser("ops"),
		"disable": second(s.auth.AlterUser(AlterSpec{Username: "ops", Enabled: &disabled, Version: s.auth.Version()})),
		"strip":   second(s.auth.AlterUser(AlterSpec{Username: "ops", Grants: &[]Grant{{Role: "reader", DB: "app"}}, Version: s.auth.Version()})),
		"revoke":  second(s.auth.RevokeRole("ops", Grant{Role: "user-manager", DB: "*"})),
	} {
		if !errors.Is(err, ErrLastAdmin) {
			t.Errorf("%s of the last user-manager while a cluster-manager remains = %v, want ErrLastAdmin", change, err)
			continue
		}
		for _, fact := range []string{"the last enabled account that can manage users", "root, or user-manager on every database", "'grant role user-manager@* to "} {
			if !strings.Contains(err.Error(), fact) {
				t.Errorf("%s refusal %q does not say %q", change, err, fact)
			}
		}
		if strings.Contains(err.Error(), "cluster-manager") {
			t.Errorf("%s refusal %q still advises cluster-manager, which the guard does not count", change, err)
		}
	}
	if got := len(s.auth.UserInfos()); got != 2 {
		t.Fatalf("users = %d after the refusals, want 2", got)
	}
}

func second[T any](_ T, err error) error { return err }
