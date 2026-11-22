package server

import (
	"fmt"

	"github.com/hoarddb/hoarddb-server/config"
)

// ringSelfID returns the identity the cluster knows this node by, resolved the
// way NewCluster resolves it, without starting a cluster. The users file names
// its master by this id (BUG-0596), and an offline command has no running
// Cluster to ask. A node outside any cluster is known by its configured id.
func ringSelfID(cfg *config.Config) (string, error) {
	if !legacyRingOn(cfg) {
		return cfg.Server.NodeID, nil
	}
	configured, err := selfNode(cfg)
	if err != nil {
		return "", err
	}
	membershipPath := membershipPathFor(cfg.Storage.DataDir)
	persisted, found, err := readMembershipRecord(membershipPath)
	if err != nil {
		return "", err
	}
	if found {
		resolved, err := locateSelf(persisted.Members, configured, cfg.Server.Listen, membershipPath)
		if err != nil {
			return "", fmt.Errorf("resolve this node's cluster id: %w", err)
		}
		return resolved.ID, nil
	}
	_, resolved, _, err := memberList(cfg, configured)
	if err != nil {
		return "", fmt.Errorf("resolve this node's cluster id: %w", err)
	}
	return resolved.ID, nil
}
