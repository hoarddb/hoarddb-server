package wire

import (
	"errors"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// IncStats are node-scoped counts of inc calls by outcome, never per bucket; the exposition the spec
// names as hoarddb_store_inc_total{outcome} reads these. Applied is an inc that wrote, Deduped a retry
// answered from its dedupe entry, Refused an inc the owner refused (the developer's to fix).
type IncStats struct {
	Applied, Deduped, Refused atomic.Uint64
}

// count files one finished inc under its outcome. An error that is not a refusal (a lost leader, a
// timeout) is the caller's to retry and is not an outcome of the inc itself.
func (s *IncStats) count(answer WriteAnswer, err error) {
	var refused *cluster.Error
	switch {
	case errors.As(err, &refused) && (refused.Code == cluster.CodeInvalidArgument || refused.Code == cluster.CodeResultTooLarge):
		s.Refused.Add(1)
	case err != nil:
	case answer.Deduped:
		s.Deduped.Add(1)
	default:
		s.Applied.Add(1)
	}
}
