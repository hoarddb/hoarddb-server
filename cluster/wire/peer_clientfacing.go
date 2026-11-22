package wire

import (
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// clientFacing is the error a forwarder shows its caller for a refusal the owner sent.
// EPOCH_FENCED is node-to-node only (membership 7): the caller sees STALE_EPOCH, which
// tells it to refresh its table and retry, never a code its error table has no fix for.
func clientFacing(err error, owner string) error {
	var coded *cluster.Error
	if errors.As(err, &coded) && coded.Code == cluster.CodeEpochFenced {
		return &cluster.Error{Code: cluster.CodeStaleEpoch, Message: fmt.Sprintf(
			"the request was forwarded to %s, which is at another epoch than this node (%s), so the two sides disagree and nothing was applied. Refresh the topology and retry; a retry with the same request id is safe", owner, coded.Message)}
	}
	return err
}
