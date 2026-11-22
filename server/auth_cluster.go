package server

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"

	"github.com/hoarddb/hoarddb-server/clusterpepper"
)

// A cluster member's user store (ring user store spec 5.5, 6.3, 7.3): the rows of the cluster's
// replicated table, installed by the table follower, checked under the cluster's password key.
// Nothing is read from or written to users.json, and no path but the follower writes the
// in-memory store. Rule 11: one atomic pointer per node; nothing per bucket, store or user.

// clusterPasswordKey is where a cluster member's AuthManager takes the cluster's password key:
// the ring node (wire.Node.PasswordKey). It answers an error with the case's rule 3 text when
// this node cannot check a password now (no users yet, the key not fetched, a witness).
type clusterPasswordKey interface {
	PasswordKey() (clusterpepper.Key, string, error)
	// ClusterID is the id of the cluster whose state this node applies; a token of a cluster row
	// names it (M2b review F-2).
	ClusterID() [16]byte
}

// clusterKeySource holds the key source in an atomic pointer: set once the ring node opens.
type clusterKeySource struct{ source clusterPasswordKey }

// ErrUsersInCluster refuses a user change that would write a cluster member's store directly
// (the legacy user statements, the offline paths): its users are the cluster's table.
var ErrUsersInCluster = errors.New("this node is a cluster member: its users live in the cluster's replicated state and change only through the ring user commands " +
	"(create_user, alter_user, drop_user, grant_role, revoke_role, as ringclient and current drivers send them). " +
	"The hoarddb CLI's user statements do not reach them in this build. Nothing was changed")

// ErrResetPasswordInCluster is the refusal of the offline reset on a cluster member (ring user
// store spec 6.7): its users live in the cluster's replicated state, which a stopped node cannot
// change, and it keeps no users.json.
var ErrResetPasswordInCluster = errors.New("--reset-password does not work on a cluster node: users live in the cluster's replicated state, which a stopped node cannot change. " +
	"Start the cluster, log in on any node as another administrator (an account with user-manager or root), and run 'alter user <name> password'. " +
	"If no other administrator can log in, the cluster's users cannot be recovered; see \"Keep two administrators\" in the user-management page")

// errClusterKeyNotOpen is the refusal of a login before the ring node opened: the node cannot
// know the cluster's key yet.
var errClusterKeyNotOpen = errors.New("password logins are not available on this node yet: it is still starting and has not opened its cluster state. Retry in a moment")

// SetClusterPasswordKey points a cluster member's logins at the ring node's password key.
func (am *AuthManager) SetClusterPasswordKey(source clusterPasswordKey) {
	am.clusterKey.Store(&clusterKeySource{source: source})
}

// passwordKey is the cluster's password key and its kid, or the refusal of a login now.
func (am *AuthManager) passwordKey() (clusterpepper.Key, string, error) {
	held := am.clusterKey.Load()
	if held == nil {
		return clusterpepper.Key{}, "", errClusterKeyNotOpen
	}
	return held.source.PasswordKey()
}

// tokenClusterID is the cluster id a token of an account with incarnation createdSeq names: the
// hex id of this node's cluster for a row of the cluster's table, "" for a users.json record.
func (am *AuthManager) tokenClusterID(createdSeq int64) string {
	if createdSeq == 0 {
		return ""
	}
	held := am.clusterKey.Load()
	if held == nil {
		return ""
	}
	id := held.source.ClusterID()
	return hex.EncodeToString(id[:])
}

// passwordLoginRefusal is the text a password AUTH is refused with before anything is checked, or
// "": a cluster member without the cluster's key refuses with the case's text (spec 6.2), and
// never runs Argon2 for it.
func (am *AuthManager) passwordLoginRefusal() string {
	if !am.clusterMember {
		return ""
	}
	if _, _, err := am.passwordKey(); err != nil {
		return err.Error()
	}
	return ""
}

// refuseInCluster is the guard of every path that would write the store directly.
func (am *AuthManager) refuseInCluster(what string) error {
	if am.clusterMember {
		return fmt.Errorf("%s: %w", what, ErrUsersInCluster)
	}
	return nil
}

// refuseUsersFileInCluster stops a cluster member's start when its data directory holds a
// users.json: that file is never read in cluster mode, and serving without it while it is there
// would hide accounts the operator thinks exist (spec 7.3, 6.2).
func refuseUsersFileInCluster(usersPath string) error {
	_, err := os.Stat(usersPath)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("checking for an old users file at %s: %w", usersPath, err)
	}
	return fmt.Errorf("this node runs in cluster mode, where users live in the cluster's replicated state, so %s is not read. "+
		"Most likely it is from a start without cluster.enabled. Move it aside (mv %s %s.not-clustered) and start the node again. "+
		"If the cluster has no administrator yet, set HOARDDB_ROOT_PASSWORD on one data node before starting it",
		usersPath, usersPath, usersPath)
}
