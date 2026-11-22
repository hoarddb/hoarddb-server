package server

import (
	"context"
	"errors"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// expireLocked applies a TTL sweep's conditional delete to a hash bucket
// whose mutex the caller holds. It is an ordinary delete through the ordinary
// write path — the same WAL entry, apply turn and shipping — whose condition
// every node evaluates against its own stored document (build doc S-7).
//
// It is the same delete as a user's (owner ruling 2026-09-30): the document's
// field stores go in its batch and its blob payloads are unlinked after it,
// through the code Server.deleteDocument runs. Because the cascade is inside
// the apply, a replica that applies the entry cascades too. A cascade one
// batch cannot hold deletes nothing — the same answer on every node — and the
// response says the document was skipped.
func (s *Server) expireLocked(ctx context.Context, bucket *inMemoryBucket, req DeleteRequest) (*DeleteResponse, error) {
	condition := store.ExpiryCondition{
		Field:         req.ExpireIf.Field,
		DateBefore:    int64(req.ExpireIf.DateBefore),
		SecondsBefore: req.ExpireIf.SecondsBefore,
	}
	spared, payloads, err := bucket.hashStore.DeleteIfExpired(applyInfoFrom(ctx, s), req.Key, condition, bucket.fieldLayout())
	s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "EXPIRE"}, err)
	if errors.Is(err, storage.ErrBatchTooLarge) {
		return &DeleteResponse{Success: false, Skipped: true, Message: expirySkippedMessage(bucket, req)}, nil
	}
	if err == nil {
		err = s.unlinkCascadedPayloads(req, payloads)
	}
	if err != nil {
		return &DeleteResponse{Success: false, Message: failureMessage("expiring the document failed", err)}, nil
	}
	if spared {
		return &DeleteResponse{Success: true, Spared: true, Message: "document no longer expired; left in place"}, nil
	}
	return &DeleteResponse{Success: true, Message: "document expired"}, nil
}
