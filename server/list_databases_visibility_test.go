package server

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// userCreateFor builds a user-create request for name with password and
// roles, with a credential derived as the CLI derives one.
func userCreateFor(t *testing.T, name, password string, roles []UserRole) *UserCreateRequest {
	t.Helper()
	credential, err := NewCredential(password, DefaultArgon2Params())
	if err != nil {
		t.Fatalf("NewCredential: %v", err)
	}
	return &UserCreateRequest{
		RequestID:  fmt.Sprintf("create-%s-%d", name, time.Now().UnixNano()),
		Username:   name,
		Credential: UserCredential{Algo: credential.Algo, Salt: credential.Salt, Hash: credential.Hash, Params: credential.Params},
		Roles:      roles,
	}
}

// loginAs authenticates c as name, failing the test on a refusal.
func (c *tcpServerClient) loginAs(name, password string) {
	c.t.Helper()
	var authResp AuthResponse
	if resp := c.call(transport.OpAuth, &LoginRequest{Username: name, Password: password}, &authResp); resp.Opcode != transport.OpAuthOK {
		c.t.Fatalf("login as %s: opcode %s, want AUTH_OK", name, resp.Opcode)
	}
}

// listedDatabaseNames lists databases on c and returns the names.
func (c *tcpServerClient) listedDatabaseNames() []string {
	c.t.Helper()
	var listing ListDatabasesResponse
	c.call(transport.OpListDBs, &ListDatabasesRequest{}, &listing)
	names := make([]string, 0, len(listing.Databases))
	for _, db := range listing.Databases {
		names = append(names, db.Name)
	}
	return names
}

// visibilityCase is one account and the databases a listing shows it.
type visibilityCase struct {
	user  string
	roles []UserRole
	want  []string
}

// visibilityCases are the accounts both deployments check, against the
// databases app, billing and tenant-x.
var visibilityCases = []visibilityCase{
	{user: "reader-app", roles: []UserRole{{Role: "reader", DB: "app"}}, want: []string{"app"}},
	{user: "two-dbs", roles: []UserRole{{Role: "writer", DB: "app"}, {Role: "db-manager", DB: "billing"}}, want: []string{"app", "billing"}},
	{user: "restore-only", roles: []UserRole{{Role: "restore-operator", DB: "tenant-x"}}, want: []string{"tenant-x"}},
	{user: "no-roles", roles: nil, want: []string{}},
	{user: "reader-all", roles: []UserRole{{Role: "reader", DB: "*"}}, want: []string{"app", "billing", "tenant-x"}},
}

var visibilityDatabases = []string{"app", "billing", "tenant-x"}

const visibilityPassword = "visibility-password-123"

// TestListDatabasesShowsOnlyDatabasesTheUserHoldsARoleOn: a database listing
// names the databases the caller holds a role on, and every database for a
// role on "*". It holds on a single node and on a cluster, whose
// listing is gathered from every member.
func TestListDatabasesShowsOnlyDatabasesTheUserHoldsARoleOn(t *testing.T) {
	t.Run("single node", func(t *testing.T) {
		_, addr := newTCPEnabledServer(t, nil)
		root := dialTCPServer(t, addr)
		root.login()
		for _, name := range visibilityDatabases {
			root.call(transport.OpCreateDB, &CreateDatabaseRequest{Name: name}, nil)
		}
		for _, tc := range visibilityCases {
			var created UserMutationResponse
			root.call(transport.OpUserCreate, userCreateFor(t, tc.user, visibilityPassword, tc.roles), &created)
		}
		if got := root.listedDatabaseNames(); !slices.Equal(got, visibilityDatabases) {
			t.Fatalf("root lists %v, want every database %v", got, visibilityDatabases)
		}
		for _, tc := range visibilityCases {
			client := dialTCPServer(t, addr)
			client.loginAs(tc.user, visibilityPassword)
			if got := client.listedDatabaseNames(); !slices.Equal(got, tc.want) {
				t.Errorf("%s (%v) lists %v, want %v", tc.user, tc.roles, got, tc.want)
			}
		}
	})

	t.Run("cluster", func(t *testing.T) {
		cluster := newTestCluster(t, 2, 2)
		defer cluster.stop()
		master := clusterMasterIndex(t, cluster)
		for _, name := range visibilityDatabases {
			cluster.ok(0, transport.OpCreateDB, &CreateDatabaseRequest{Name: name})
		}
		for _, tc := range visibilityCases {
			cluster.ok(master, transport.OpUserCreate, userCreateFor(t, tc.user, visibilityPassword, tc.roles))
		}
		for _, addr := range cluster.driverAddrs {
			waitForLogin(t, addr, visibilityCases[len(visibilityCases)-1].user, visibilityPassword)
			for _, tc := range visibilityCases {
				client := dialTCPServer(t, addr)
				client.loginAs(tc.user, visibilityPassword)
				if got := client.listedDatabaseNames(); !slices.Equal(got, tc.want) {
					t.Errorf("%s (%v) lists %v through %s, want %v", tc.user, tc.roles, got, addr, tc.want)
				}
			}
		}
	})
}

// waitForLogin polls until user can log in on addr: a user created on the
// master reaches another member with the next auth-config push.
func waitForLogin(t *testing.T, addr, user, password string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := clusterAuthAs(t, addr, user, password)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s cannot log in on %s within 30s: %v", user, addr, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
