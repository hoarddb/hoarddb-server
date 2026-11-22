package storage

// WALKeySource provides access to WAL segment encryption keys and epoch information.
// It is accepted by OpenWAL so that storage never learns the registry format,
// the node master key, or their relationship.
//
// WALKeySource implementations must be safe for concurrent use.
type WALKeySource interface {
	// CurrentEpoch returns the epoch id that new WAL segments are sealed under.
	CurrentEpoch() uint64

	// FileKey returns the 32-byte key for the given segment index under the given epoch.
	// It returns an error if the epoch is not found or the key cannot be derived.
	FileKey(epoch uint64, segIdx int) ([]byte, error)
}
