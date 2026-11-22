// write_concern.go implements write concern levels for hoardDB.
// Write concern controls how many nodes must acknowledge a write before
// the client receives a success response. This provides tunable
// durability guarantees per write operation.
package server

import (
	"errors"
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
)

// WriteConcernLevel represents the durability level for a write operation.
type WriteConcernLevel int

const (
	// WcOne acknowledges after a single node (leader only). Default.
	// Lowest latency, but writes can be lost on leader failure.
	WcOne WriteConcernLevel = 1

	// WcMajority waits for floor(RF/2)+1 total acknowledgements before
	// returning success. Survives up to floor(RF/2) simultaneous node
	// failures.
	WcMajority WriteConcernLevel = 2

	// WcAll waits for every node in the cluster to acknowledge the write.
	// Highest durability, but any single node failure blocks all writes.
	WcAll WriteConcernLevel = 3
)

// DefaultWriteConcernTimeout is the maximum time to wait for quorum
// acknowledgement before returning a timeout error to the client.
const DefaultWriteConcernTimeout = 5 * time.Second

// String returns a human-readable name for the write concern level.
func (wcl WriteConcernLevel) String() string {
	switch wcl {
	case WcOne:
		return "1"
	case WcMajority:
		return "majority"
	case WcAll:
		return "all"
	default:
		return fmt.Sprintf("unknown(%d)", int(wcl))
	}
}

// ErrUnknownWriteConcern is under every refusal of a write concern the server
// does not recognise. It reaches the wire as INVALID_ARGUMENT: serving it at
// some default would acknowledge a write more weakly than the caller asked.
var ErrUnknownWriteConcern = errors.New("unknown write concern")

// ParseWriteConcern parses a string into a WriteConcernLevel.
// Accepted inputs: "1" or "one", "majority", "all" (case-insensitive).
// Anything else is an error that lists them.
func ParseWriteConcern(s string) (WriteConcernLevel, error) {
	canonical, ok := config.CanonicalWriteConcern(s)
	if !ok {
		return 0, fmt.Errorf("%w: %q is not one of: %s", ErrUnknownWriteConcern, s, config.WriteConcernValues)
	}
	switch canonical {
	case "majority":
		return WcMajority, nil
	case "all":
		return WcAll, nil
	}
	return WcOne, nil
}

// QuorumSize returns the number of acknowledgements needed for a
// majority quorum given the replication factor. floor(rf/2)+1.
func QuorumSize(rf int) int {
	if rf <= 0 {
		return 1
	}
	return rf/2 + 1
}

// WriteConcernConfig holds the write concern parameters for a single
// write operation.
type WriteConcernConfig struct {
	// Level controls the required acknowledgement count.
	Level WriteConcernLevel

	// Timeout is the maximum duration to wait for quorum acknowledgement.
	// If zero, DefaultWriteConcernTimeout is used.
	Timeout time.Duration
}
