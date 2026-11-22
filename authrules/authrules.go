// Package authrules holds the account rules that the server and the replicated cluster state
// must apply identically: the role names, which role includes which, the ceiling a grantor works
// under, the one predicate for "can administer users", the reserved node-identity name, the size of
// the applied-request list and the credential window (ring user store spec 4.2, F10).
//
// It is a leaf: it imports only standard packages that read no clock, no file, no randomness and no
// configuration, because the cluster's appliers call it and an applier is a pure function of the
// state and the command. A change here can change what a committed command does, so it needs a
// state level bump. The cluster's goldens catch what they record: the vocabulary golden records the
// role names, what each role includes, the administrator predicate, the credential window, the
// request-list size, the user ceiling and the node name; the state-after golden replays one corpus
// of user commands and one ceiling refusal. A change they do not reach (another ceiling case, for
// one) is caught only by the tests of that rule.
package authrules

import (
	"cmp"
	"fmt"
	"slices"
)

// The built-in roles. Their names are the privilege names the server reports, so an error that
// says what is missing is also what to grant.
const (
	RoleRoot            = "root"
	RoleReader          = "reader"
	RoleWriter          = "writer"
	RoleDBManager       = "db-manager"
	RoleUserManager     = "user-manager"
	RoleClusterManager  = "cluster-manager"
	RoleBackupOperator  = "backup-operator"
	RoleRestoreOperator = "restore-operator"
)

// AllDatabases is the database of a grant that covers every database.
const AllDatabases = "*"

// NodeUser is the username a node presents when it authenticates to a peer. No account may take
// it: audit rows would no longer tell a node from a person.
const NodeUser = "__node__"

// RequestListSize is how many applied user commands the cluster remembers by request id. A retry
// under the same id is recognised while fewer than this many later user commands have committed.
const RequestListSize = 256

// MaxUsers is the most accounts one user store holds, on a single node or in a cluster.
const MaxUsers = 10000

// The credential window: the work factors and lengths any member accepts in a committed
// credential. They are literal numbers on purpose (spec 4.2, B11): derived from a configuration
// constant, an edit there would change what apply accepts without touching the state level.
const (
	MinArgon2MemoryKiB  = 65536
	MaxArgon2MemoryKiB  = 131072
	MinArgon2Iterations = 3
	MaxArgon2Iterations = 6
	MinArgon2Threads    = 1
	MaxArgon2Threads    = 16
	Argon2HashLen       = 32
	MinSaltLen          = 16
	MaxSaltLen          = 64
)

// roleNames is every valid role, in the order an error lists them.
var roleNames = []string{
	RoleRoot, RoleReader, RoleWriter, RoleDBManager, RoleUserManager,
	RoleClusterManager, RoleBackupOperator, RoleRestoreOperator,
}

// renamedRoles maps the names earlier development builds used to the current ones. It feeds
// error text only: a retired name is an unknown role and grants nothing.
var renamedRoles = map[string]string{
	"read":         RoleReader,
	"readWrite":    RoleWriter,
	"dbAdmin":      RoleDBManager,
	"userAdmin":    RoleUserManager,
	"clusterAdmin": RoleClusterManager,
	"backup":       RoleBackupOperator,
	"restore":      RoleRestoreOperator,
}

// Grant is one role on one database, or on AllDatabases.
type Grant struct {
	Role string
	DB   string
}

// RoleNames returns every valid role, in the order an error lists them.
func RoleNames() []string { return slices.Clone(roleNames) }

// KnownRole reports whether role is one of the current role names.
func KnownRole(role string) bool { return slices.Contains(roleNames, role) }

// RenamedRole returns the current name of a role an earlier development build used.
func RenamedRole(old string) (current string, renamed bool) {
	current, renamed = renamedRoles[old]
	return current, renamed
}

// RoleIncludes reports whether holding role held confers everything role granted does.
func RoleIncludes(held, granted string) bool {
	if held == granted || held == RoleRoot {
		return true
	}
	switch held {
	case RoleDBManager:
		return granted == RoleWriter || granted == RoleReader
	case RoleWriter, RoleBackupOperator:
		return granted == RoleReader
	default:
		return false
	}
}

// Unrestricted reports whether grants hold root on every database, which has no ceiling.
func Unrestricted(grants []Grant) bool {
	return slices.Contains(grants, Grant{Role: RoleRoot, DB: AllDatabases})
}

// Holds reports whether grants hold want: a role that includes it, on the same database or on
// every one.
func Holds(grants []Grant, want Grant) bool {
	return slices.ContainsFunc(grants, func(held Grant) bool {
		return (held.DB == AllDatabases || held.DB == want.DB) && RoleIncludes(held.Role, want.Role)
	})
}

// CeilingBreach is the first grant a grantor reached past: one it gave (Given true) or one the
// account it changes already holds (Given false).
type CeilingBreach struct {
	Grant Grant
	Given bool
}

// CheckCeiling applies the ceiling a grantor who is not root works under: every grant it gives,
// and every grant the account it changes already holds, must be one the grantor holds. ok is false
// with the first breach, the given grants checked before the target's.
func CheckCeiling(grantor, target, given []Grant) (CeilingBreach, bool) {
	if Unrestricted(grantor) {
		return CeilingBreach{}, true
	}
	for _, grant := range given {
		if !Holds(grantor, grant) {
			return CeilingBreach{Grant: grant, Given: true}, false
		}
	}
	for _, grant := range target {
		if !Holds(grantor, grant) {
			return CeilingBreach{Grant: grant}, false
		}
	}
	return CeilingBreach{}, true
}

// CanAdministerUsers is the one last-administrator predicate (spec 4.2, F3 and B8): an enabled
// account holding root or user-manager on every database, the grants that allow user
// administration without a database. cluster-manager does not count: it cannot create, alter or
// grant users.
func CanAdministerUsers(enabled bool, grants []Grant) bool {
	if !enabled {
		return false
	}
	return slices.ContainsFunc(grants, func(held Grant) bool {
		return held.DB == AllDatabases && (held.Role == RoleRoot || held.Role == RoleUserManager)
	})
}

// NormalizeGrants returns grants sorted by role, then database, with repeats dropped, in a new
// slice. The replicated state keeps one spelling of a role list.
func NormalizeGrants(grants []Grant) []Grant {
	sorted := slices.Clone(grants)
	slices.SortFunc(sorted, CompareGrants)
	return slices.Compact(sorted)
}

// CompareGrants orders grants by role, then database.
func CompareGrants(a, b Grant) int {
	if c := cmp.Compare(a.Role, b.Role); c != 0 {
		return c
	}
	return cmp.Compare(a.DB, b.DB)
}

// CredentialWindowViolation says what in a credential lies outside the window, or "" when all of
// it lies inside. memoryKiB, iterations and threads are the Argon2id work factor; saltLen and
// hashLen are byte lengths.
func CredentialWindowViolation(memoryKiB, iterations, threads int64, saltLen, hashLen int) string {
	switch {
	case memoryKiB < MinArgon2MemoryKiB || memoryKiB > MaxArgon2MemoryKiB:
		return fmt.Sprintf("its memory cost is %d KiB and a cluster accepts %d to %d KiB", memoryKiB, MinArgon2MemoryKiB, MaxArgon2MemoryKiB)
	case iterations < MinArgon2Iterations || iterations > MaxArgon2Iterations:
		return fmt.Sprintf("it takes %d iterations and a cluster accepts %d to %d", iterations, MinArgon2Iterations, MaxArgon2Iterations)
	case threads < MinArgon2Threads || threads > MaxArgon2Threads:
		return fmt.Sprintf("its parallelism is %d and a cluster accepts %d to %d", threads, MinArgon2Threads, MaxArgon2Threads)
	case saltLen < MinSaltLen || saltLen > MaxSaltLen:
		return fmt.Sprintf("its salt is %d bytes and a cluster accepts %d to %d", saltLen, MinSaltLen, MaxSaltLen)
	case hashLen != Argon2HashLen:
		return fmt.Sprintf("its hash is %d bytes and a cluster accepts exactly %d", hashLen, Argon2HashLen)
	}
	return ""
}
