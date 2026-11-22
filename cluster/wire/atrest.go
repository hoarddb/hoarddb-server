package wire

import (
	"fmt"

	"github.com/hoarddb/hoarddb-server/storage"
)

// AtRestKeys hands a node the keys its store and its log are sealed under (ring encryption at
// rest spec §8). The node asks for both after the ring directory passed its format and cluster
// version checks, and asks for the store key after the store's marker refusals and first-start
// check, before Badger or the log creates a file. It asks before the store's file guards (they
// need the key), so an implementation may create a key only for a store directory with no data
// file (storage.StoreHasDataFiles): one with data files was created with its key or in plaintext,
// and the server's load refuses it before the node opens when no key file is beside it. The log
// key has one more exception, which ringLogKeys names. A nil
// key or key source means that part is plaintext. A node whose NodeConfig.AtRest is nil asks
// nothing and runs in plaintext, as every earlier build did.
type AtRestKeys interface {
	// StoreKey returns Badger's master key for the store in storeDir (<Dir>/badger).
	StoreKey(storeDir string) ([]byte, error)
	// LogKeys returns the key source the log in logDir (<Dir>/wal) seals its entries under.
	LogKeys(logDir string) (storage.WALKeySource, error)
}

// ringStoreKey asks the node's AtRest for the store key, or returns nil when the node runs in
// plaintext. The vouched open calls it after its own refusals and before it writes the marker or
// Badger creates a file (spec §5.3).
func (n *Node) ringStoreKey(storeDir string) ([]byte, error) {
	if n.cfg.AtRest == nil {
		return nil, nil
	}
	storeKey, err := n.cfg.AtRest.StoreKey(storeDir)
	if err != nil {
		return nil, fmt.Errorf("getting the key the ring store in %s is sealed under: %w", storeDir, err)
	}
	return storeKey, nil
}

// ringLogKeys asks the node's AtRest for the log keys, or returns nil when the node runs in
// plaintext. The log is opened once per start (openLog), so it is asked once. Normally after the
// store key (spec §5.3); the store's first-start check opens an existing log before the store key
// is asked for when the store is empty (refuseEmptyStoreAfterEarlierLife), and then a log registry
// absent beside a log directory with no sealed segment is created first, and is left in place if
// that check refuses: the next start loads it.
func (n *Node) ringLogKeys(logDir string) (storage.WALKeySource, error) {
	if n.cfg.AtRest == nil {
		return nil, nil
	}
	logKeys, err := n.cfg.AtRest.LogKeys(logDir)
	if err != nil {
		return nil, fmt.Errorf("getting the keys the ring log in %s is sealed under: %w", logDir, err)
	}
	return logKeys, nil
}
