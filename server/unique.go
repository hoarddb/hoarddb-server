package server

import (
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// uniqueRefusal returns the unique index refusal inside err, labelled with the
// bucket it was refused on, or nil when err is not one. The mutating handlers
// answer an ordinary failure as a Success:false response, which reaches the
// client with no error code; a duplicate is returned as the error instead, so
// the wire carries ErrDuplicateKey and a driver can branch on it (build doc §4
// F-2(a)). The refusal is returned bare, not in the store's wrap chain, so it
// reads the same from every path that produces it (§4 F-2(b)).
func uniqueRefusal(bucket string, err error) error {
	var duplicate *store.DuplicateKeyError
	if !errors.As(err, &duplicate) {
		return nil
	}
	duplicate.Bucket = bucket
	indexUniqueRefusals.Inc()
	return duplicate
}

// uniquePrecheck is the owner's hint (GAP-14 build doc Step 7, spec §4.4.1):
// before a write to a bucket declaring a unique index is appended to the WAL,
// each unique key it would take is looked up in a fresh snapshot, with no
// bucket lock. A key another document holds refuses the write with the same
// refusal the apply would give — so nothing is appended, applied or shipped
// for it. It is a hint, not the arbiter: a write it lets through is still
// checked inside the apply, which is what enforces the constraint. An update
// is not pre-checked: its new value exists only once its operators run.
//
// A bucket with no unique index pays one read of its published descriptors.
func (c *Cluster) uniquePrecheck(entry ReplEntry) (ReplResponse, bool) {
	op := transport.Opcode(entry.Op)
	switch op {
	case transport.OpPut, transport.OpUpsert, transport.OpBatchPut, transport.OpBatchUpsert, transport.OpFIFOPush, transport.OpLIFOPush, transport.OpHeapPush:
	default:
		return ReplResponse{}, false
	}
	database := decodeFrameMeta(entry.Payload).Database
	facts, held := c.s.peekBucket(database, entry.Bucket)
	if !held || facts.bucketType != BucketTypeHash || !declaresUnique(facts.indexes()) {
		return ReplResponse{}, false
	}
	documents, err := precheckedDocuments(op, entry.Payload)
	if err != nil {
		return ReplResponse{}, false
	}
	view, snap, err := c.s.resolveForRead(database, entry.Bucket)
	if err != nil || view.idxMgr == nil {
		return ReplResponse{}, false
	}
	defer snap.Discard()
	written := make(map[string]bool, len(documents))
	for _, document := range documents {
		written[document.key] = true
	}
	for _, document := range documents {
		refusal := view.idxMgr.CheckUnique(snap, nil, []byte(document.key), nil, document.fields)
		var duplicate *store.DuplicateKeyError
		// A holder that is itself a document of the batch may be moving off the
		// value within it; only the apply, which sees the batch in order, can
		// tell.
		if !errors.As(refusal, &duplicate) || written[duplicate.Holder] {
			continue
		}
		indexUniqueRefusals.Inc()
		duplicate.Bucket = entry.Bucket
		duplicate.Position = document.position
		frame := errorFrame(&transport.Frame{Opcode: op}, duplicate)
		return ReplResponse{Opcode: int32(frame.Opcode), Payload: frame.Payload}, true
	}
	return ReplResponse{}, false
}

// declaresUnique reports whether any of specs is a unique index this server
// enforces: an index on a path is unusable and enforces nothing.
func declaresUnique(specs []store.IndexSpec) bool {
	for _, spec := range specs {
		if spec.Unique && !indexesAPath(spec) {
			return true
		}
	}
	return false
}

// precheckedDocument is one document a write would store, decoded as the
// store will decode it.
type precheckedDocument struct {
	key    string
	fields map[string]interface{}
	// position is the document's 1-based place in a batch, 0 for one document.
	position int
}

// precheckedDocuments decodes the documents a write would store under the
// keys its payload resolves them to (resolveDocumentIDs has already minted any
// missing id).
func precheckedDocuments(op transport.Opcode, payload []byte) ([]precheckedDocument, error) {
	switch op {
	case transport.OpPut, transport.OpUpsert:
		var req InsertRequest
		if err := bson.Unmarshal(payload, &req); err != nil {
			return nil, fmt.Errorf("decoding the insert: %w", err)
		}
		key := req.Key
		if key == "" {
			key, _ = req.Data["_id_"].(string)
		}
		document, err := storedForm(req.Data, key)
		return []precheckedDocument{{key: key, fields: document}}, err
	case transport.OpBatchPut, transport.OpBatchUpsert:
		var req BatchInsertRequest
		if err := bson.Unmarshal(payload, &req); err != nil {
			return nil, fmt.Errorf("decoding the batch insert: %w", err)
		}
		documents := make([]precheckedDocument, 0, len(req.Data))
		for position, data := range req.Data {
			key, _ := data["_id_"].(string)
			document, err := storedForm(data, key)
			if err != nil {
				return nil, err
			}
			documents = append(documents, precheckedDocument{key: key, fields: document, position: position + 1})
		}
		return documents, nil
	default:
		var req PushRequest
		if err := bson.Unmarshal(payload, &req); err != nil {
			return nil, fmt.Errorf("decoding the push: %w", err)
		}
		key, _ := req.Data["_id_"].(string)
		document, err := storedForm(req.Data, key)
		return []precheckedDocument{{key: key, fields: document}}, err
	}
}

// storedForm is data as the store will decode it: the key set as "_id_", and
// one BSON round trip. The payload's now() was fixed before it got here.
func storedForm(data map[string]interface{}, key string) (map[string]interface{}, error) {
	if data == nil {
		data = make(map[string]interface{})
	}
	data["_id_"] = key
	encoded, err := bson.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("encoding document %q: %w", key, err)
	}
	var document map[string]interface{}
	if err := bson.Unmarshal(encoded, &document); err != nil {
		return nil, fmt.Errorf("decoding document %q: %w", key, err)
	}
	return document, nil
}

// The owner's outcome for an entry, carried to its replicas (ReplEntry.
// OwnerOutcome, build doc S-5). Unknown is the zero value on purpose: an owner
// from before the field never sets it, and a replica then reports nothing.
const (
	ownerOutcomeUnknown             int32 = 0
	ownerOutcomeApplied             int32 = 1
	ownerOutcomeRefusedByConstraint int32 = 2
	// ownerOutcomeOwed is an entry the owner's store failed to apply and has
	// not applied yet: it is owed to the owner's replay, so no replica may
	// apply or acknowledge it (BUG-0247).
	ownerOutcomeOwed int32 = 3
)

// applyOutcomeOf classifies one local apply of an entry: refused by a unique
// index, applied, or unknown — a failure of another kind, which is not a
// constraint's answer and is reported elsewhere.
func applyOutcomeOf(resp ReplResponse, err error) int32 {
	if err != nil {
		// A store failure is owed to replay, not unknown: the owner's apply did
		// not commit it, and it will be retried (BUG-0247).
		if errors.Is(err, store.ErrApplyFailed) {
			return ownerOutcomeOwed
		}
		return ownerOutcomeUnknown
	}
	if transport.Opcode(resp.Opcode) == transport.OpError {
		if refusedByConstraint(resp.Payload) {
			return ownerOutcomeRefusedByConstraint
		}
		return ownerOutcomeUnknown
	}
	var answer struct {
		Success *bool `bson:"success"`
	}
	if bson.Unmarshal(resp.Payload, &answer) == nil && answer.Success != nil && !*answer.Success {
		return ownerOutcomeUnknown
	}
	return ownerOutcomeApplied
}

// refusedByConstraint reports whether an error frame's payload is a
// document-level refusal every copy gives alike: a unique index's, a field's
// (FIELD_TYPE), an insert's of an existing _id_ (DOCUMENT_EXISTS), or one
// marked constraint_refusal (a path refusal, typed-fields spec §6.1). It reads the encoded frame because replay and the replica's
// outcome see only that.
func refusedByConstraint(payload []byte) bool {
	return constraintRefusedUnder(payload) != ""
}

// constraintRefusedUnder names the constraint an error frame's payload was
// refused under, as the disagreement report words it, or "" when the frame
// is not a constraint's refusal.
func constraintRefusedUnder(payload []byte) string {
	var answer ErrorResponse
	if bson.Unmarshal(payload, &answer) != nil {
		return ""
	}
	switch transport.ErrorCode(answer.Code) {
	case transport.ErrDuplicateKey:
		return "a unique index"
	case transport.ErrFieldType:
		return "the bucket's declared field types"
	case transport.ErrDocumentExists:
		return "an existing document with this `_id_`"
	}
	if answer.ConstraintRefusal {
		return "a path rule"
	}
	return ""
}

// noteReplicaDisagreement is the replica's side of S-5: an entry its owner
// applied and it refused under a constraint, or the reverse, means its copy
// of the bucket differs from the owner's at that position — the ownership
// change window of spec §4.5, a seeded replica, a bug. The push is still
// acknowledged: a retry cannot change a deterministic outcome, and the repair
// is a verify walk, not this path. It logs one ERROR, which names the bucket,
// the sequence and the constraint, never the value (RD-10), and counts it.
// The constraint is read from this replica's own refusal; when only the
// owner refused, the entry says that it did and not under what, so the line
// names the constraints an owner refuses under.
func (c *Cluster) noteReplicaDisagreement(entry ReplEntry, resp ReplResponse, err error) {
	if entry.OwnerOutcome == ownerOutcomeUnknown {
		return
	}
	local := applyOutcomeOf(resp, err)
	if local == ownerOutcomeUnknown || local == entry.OwnerOutcome {
		return
	}
	// An owed entry is one the owner has not applied yet: its replay applies it,
	// so a replica that applied it agrees with where the owner is going.
	if entry.OwnerOutcome == ownerOutcomeOwed && local == ownerOutcomeApplied {
		return
	}
	replicaConstraintDisagreements.Inc()
	database := decodeFrameMeta(entry.Payload).Database
	answers := fmt.Sprintf("the owner refused the write at sequence %d (under a unique index, the bucket's declared field types or a path rule, "+
		"or because its `_id_` existed) and this replica applied it", entry.Seq)
	switch {
	case local == ownerOutcomeRefusedByConstraint && entry.OwnerOutcome == ownerOutcomeOwed:
		answers = fmt.Sprintf("the owner has not applied the write at sequence %d yet (owed) and this replica refused it under %s",
			entry.Seq, constraintRefusedUnder(resp.Payload))
	case local == ownerOutcomeRefusedByConstraint:
		answers = fmt.Sprintf("the owner applied the write at sequence %d and this replica refused it under %s",
			entry.Seq, constraintRefusedUnder(resp.Payload))
	}
	c.logger.Error(fmt.Sprintf(
		"Bucket %q in database %q: %s, so this node's copy of the bucket differs from the owner's. Nothing was retried or repaired: "+
			"dump the bucket on its owner with 'hoarddb dump' and restore it here with 'restore'.",
		entry.Bucket, database, answers),
		"database", database, "bucket", entry.Bucket, "seq", entry.Seq,
		"owner_outcome", describeOutcome(entry.OwnerOutcome), "replica_outcome", describeOutcome(local))
}

func describeOutcome(outcome int32) string {
	switch outcome {
	case ownerOutcomeRefusedByConstraint:
		return "refused"
	case ownerOutcomeOwed:
		return "owed"
	}
	return "applied"
}

// bucketDeclaresUnique reports whether the bucket a WAL entry names declares
// a unique index on this node.
func (s *Server) bucketDeclaresUnique(entry storage.WALEntry) bool {
	facts, held := s.peekBucket(decodeFrameMeta(entry.Data).Database, entry.Bucket)
	return held && declaresUnique(facts.indexes())
}
