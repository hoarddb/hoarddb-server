package server

import (
	"errors"
	"fmt"
	"github.com/hoarddb/hoarddb-server/sealed"
	"strconv"
	"strings"
	"syscall"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Error responses, and why they are shaped this way.
//
// Two rules from AGENTS.md meet here. Rule 3: an error tells the developer what
// to do about it, not just that something failed. And the transport's own rule:
// a handler that returns a Go error has its stream reset, so the client sees a
// torn connection instead of a reason. An operation that failed for an ordinary
// reason — the disk filled up, a peer was unreachable, a request was malformed —
// must therefore come back as an error *frame* with a message in it, and a nil
// error, so the session survives and the caller can act.

// ErrBucketNotFound is the sentinel under every "no such bucket" error. The
// message it is wrapped with names the database, the bucket and the commands
// that list or create it (noBucketErr).
var ErrBucketNotFound = errors.New("bucket not found")

// ErrDatabaseNotFound is the sentinel under every "no such database" error. It
// reaches the wire as NOT_FOUND: the message names the commands that
// list or create the database, the code lets a script branch without reading it.
var ErrDatabaseNotFound = errors.New("database not found")

// ErrDatabaseAlreadyExists is under the refusal of a create database for a name
// that is live. It reaches the wire as ALREADY_EXISTS: a create of a
// bucket that exists is a harmless replay, but a database create that quietly
// succeeded would hide a typo that happens to name another database.
var ErrDatabaseAlreadyExists = errors.New("database already exists")

// ErrBucketAlreadyExists is under the refusal of a create bucket that names a
// live bucket with a different store type. It reaches the wire as
// ALREADY_EXISTS. The same create with the same type is not an error: a replay
// and a redelivery take that path.
var ErrBucketAlreadyExists = errors.New("bucket already exists with another store type")

// ErrPermissionDenied is under every refusal by the authorization gate and by
// the internode listener. It reaches the wire as PERMISSION_DENIED, so a client
// that retries on INTERNAL does not retry a refusal for ever.
var ErrPermissionDenied = errors.New("permission denied")

// ErrNotHolder is under the read refusal of a node that is not a holder of the
// bucket's key range: the bucket may well exist, on another node. It
// reaches the wire as UNAVAILABLE, as the same node's write-path refusal does.
var ErrNotHolder = errors.New("this node does not hold the bucket")

// ErrNotYetAdded is under the refusal of a node whose member list comes from
// its seed settings and has not been confirmed by a member: a node started for
// joining, before 'join' adds it (join-freeze spec §5.4). It
// reaches the wire as UNAVAILABLE: retry once the node has been added.
var ErrNotYetAdded = errors.New("this node has not been added to the cluster yet")

// ErrDuplicateNodeID is under the refusal of a user change while two live
// processes run under this node's id, one of them on a copy of its data
// directory (BUG-0600). It reaches the wire as UNAVAILABLE: retry once only one
// process runs.
var ErrDuplicateNodeID = errors.New("two processes are running under this node id")

// ErrSchemaChangePaused is under the refusal of a schema change while a node
// that joined the cluster is still being copied in, and of one whose
// membership epoch changed while it was in flight (join-freeze spec §7.1). It
// reaches the wire as UNAVAILABLE: nothing was applied, retry.
var ErrSchemaChangePaused = errors.New("schema changes are paused while a node joins the cluster")

// ErrFieldType is under every refusal of a document for one of its fields
// that the bucket's rules refuse: a top-level name no path can reach (E-18)
// and, with declared types, a value of the wrong type. It reaches the wire as
// FIELD_TYPE, its own code because an application branches on it, and it is a
// constraint's answer: every copy and every replay refuses alike.
var ErrFieldType = errors.New("a field of the document is refused")

// ErrDeclarationUnusable is under the refusal of a write to a bucket whose
// recorded typed-field declaration this binary cannot enforce: written by a
// newer hoardDB, or refused by this version's checks (typed-fields spec §8.4).
// Reads are served. It reaches the wire as UNSUPPORTED_VERSION
// (build doc SA-16); replay counts it skipped, because an operator must act.
var ErrDeclarationUnusable = errors.New("the bucket's typed-field declaration cannot be enforced")

// ErrAdmittedSetByClient is E-12: a client request carrying the internal
// admitted flag, which only read repair's own put may set.
var ErrAdmittedSetByClient = errors.New("admitted set by a client")

// ErrRepairBoundSetByClient is a client delete carrying the internal
// repair_bound, which only a consistent get's repair sets.
var ErrRepairBoundSetByClient = errors.New("repair_bound set by a client")

// ErrStoreDeclarationInvalid is under every refusal of a store-field
// declaration (nested buckets, GAP-22): a bad name, engine, parent type or
// depth. It is the request's fault, so it reaches the wire as
// ErrInvalidArgument.
var ErrStoreDeclarationInvalid = errors.New("invalid store-field declaration")

// ErrFieldDeclarationInvalid is under every refusal of a typed-field
// declaration (typed-fields spec E-9): a bad name, type word, shape, limit or
// unique. It is the request's fault, so it reaches the wire as
// ErrInvalidArgument.
var ErrFieldDeclarationInvalid = errors.New("invalid typed-field declaration")

// ErrFilteredUpdateOnSpread is under the refusal of an update matching a
// filter on a spread bucket (document placement spec §6.7). It is the
// request's fault, so it reaches the wire as ErrInvalidArgument.
var ErrFilteredUpdateOnSpread = errors.New("filtered update on a spread bucket")

// ErrPeekLimit is under every refusal of a peek's limit: negative, or above
// the cap. It is the request's fault, so it reaches the wire as
// ErrInvalidArgument.
var ErrPeekLimit = errors.New("invalid peek limit")

// ErrPopLimit is ErrPeekLimit for a pop: negative, or above maxPopLimit.
var ErrPopLimit = errors.New("invalid pop limit")

// ErrWrongBucketType is under every refusal of a verb the bucket's store type
// does not answer: a range on a hash, a blob read on a fifo, an update on a
// heap. It reaches the wire as ErrInvalidArgument, because the caller sent it
// and the message already names the bucket, its type and the type the verb
// needs; INTERNAL would tell the developer to report a server fault.
var ErrWrongBucketType = errors.New("verb not answered by this store type")

// ErrExistsRefused is under every refusal of an EXISTS that has no yes-or-no
// answer: an empty key, or a bucket type with no keys to test. It reaches the
// wire as ErrInvalidArgument. An answer of exists:false would claim the server
// looked and found nothing, which it did not.
var ErrExistsRefused = errors.New("exists refused")

// ErrEmptyDocumentID is under every refusal of a document whose _id_ is
// present as an empty string: a key nothing can ever address. Omitting _id_
// asks for a generated key; sending "" names nothing, so it is refused rather
// than silently replaced (BUG-0153). It is the request's fault, so it reaches
// the wire as ErrInvalidArgument. An absent or non-string _id_ keeps the
// documented substitution.
var ErrEmptyDocumentID = errors.New("a document's _id_ must be a non-empty string")

// The field-store refusals (nested buckets, spec §10.3). Each is wrapped by a
// message that lists what the bucket does declare.
var (
	// ErrFieldNotDeclared is a field the bucket does not declare.
	ErrFieldNotDeclared = errors.New("store field not declared")
	// ErrFieldParentNotFound is a field operation on a document that does not
	// exist: never answered as an empty store.
	ErrFieldParentNotFound = errors.New("parent document not found")
	// ErrFieldWrongVerb is a verb the field's engine does not have.
	ErrFieldWrongVerb = errors.New("operation not supported by the field's engine")
	// ErrFieldAddressIncomplete is a field operation missing the bucket, the
	// parent document's key, the field or an element key.
	ErrFieldAddressIncomplete = errors.New("incomplete field-store address")
)

// bucketNotFoundErr is the error for a bucket this node does not have.
func bucketNotFoundErr(database, bucket string) error {
	return fmt.Errorf("%w: %s", ErrBucketNotFound, noBucketErr(database, bucket))
}

// bucketDroppedErr is the refusal for a mutation whose bucket this node was
// the ring owner of and no longer holds a local copy of, because it was
// dropped. It wraps ErrBucketNotFound so it gets the same
// definitive "not found" wire classification (codeFor) instead of the join
// fence's ErrJoinSyncIncomplete, whose "rebalancing has not moved the data
// here yet" text is wrong for this cause: there is no rebalance in progress
// and no previous owner holds the data.
func bucketDroppedErr(database, bucket string) error {
	bucket = displayBucket(bucket)
	return fmt.Errorf("%w: bucket %q was dropped from database %q — create it again to accept writes",
		ErrBucketNotFound, bucket, database)
}

// databaseNotFoundErr is the error for a database this node does not have: the
// recovery message of noDatabaseErr under ErrDatabaseNotFound.
func databaseNotFoundErr(database string) error {
	return &sentencedError{message: noDatabaseErr(database), cause: ErrDatabaseNotFound}
}

// databaseNotFoundInClusterErr is databaseNotFoundErr for a cluster-wide
// listing: every member was asked, so "on this node" would undersell it.
func databaseNotFoundInClusterErr(database string) error {
	return &sentencedError{cause: ErrDatabaseNotFound, message: fmt.Sprintf(
		"database %q not found on any member of the cluster. Run 'show databases' to list what exists, or 'create database %s' to create it",
		database, database)}
}

// bucketTypeMismatchErr is the refusal for a create of an existing bucket with
// another store type. A bucket's type cannot be changed in place, so the way out
// is another name, or a drop (which deletes the data) and a create.
func bucketTypeMismatchErr(req CreateBucketRequest, existing BucketType) error {
	return &sentencedError{
		message: fmt.Sprintf("bucket %q already exists in database %q with type %s, so this create (type: %s) was not applied. "+
			"A bucket's type cannot be changed in place: use another name, or run 'drop bucket %s' (this deletes its data) "+
			"and create it again with the type you want",
			req.Name, req.Database, existing, req.Type, req.Name),
		cause: ErrBucketAlreadyExists,
	}
}

// databaseExistsMessage is the sentence for a create of a database that is
// live, in the response of a replayed create and in the refusal of a client's.
func databaseExistsMessage(database string) string {
	return fmt.Sprintf("database %q already exists", database)
}

// sentencedError is an error with a sentence of its own, matched by the
// sentinel under it. A wrap with %w would append the sentinel's text, which
// can say the wrong thing where it is reported: an owner's "this node" read at
// the coordinator, or a timeout's "write concern not satisfied" before the
// sentence that says the write is stored.
type sentencedError struct {
	message string
	cause   error
}

func (e *sentencedError) Error() string { return e.message }

func (e *sentencedError) Unwrap() error { return e.cause }

// The outcomes an error for a write carries (spec §19.3.11, §20.12.3). A
// client reads them to know whether a retry is safe.
const (
	// outcomeNotApplied: nothing was stored anywhere. Retrying is safe.
	outcomeNotApplied = "not_applied"
	// outcomeApplied: the write is stored on the owner and reaches the copies
	// that are reachable; copies_confirmed says how many hold it now.
	outcomeApplied = "applied"
	// outcomeUnknown: the write may be stored.
	outcomeUnknown = "unknown"
)

// writeOutcomeError is an error answered to a request that went through the
// replicated write path, with whether the write was stored and, when it was,
// how many copies hold it. errorFrame puts all three on the wire.
type writeOutcomeError struct {
	outcome           string
	confirmed, factor int
	err               error
}

func (e *writeOutcomeError) Error() string { return e.err.Error() }

func (e *writeOutcomeError) Unwrap() error { return e.err }

// withOutcome gives err an outcome, unless it is nil or already carries one:
// the first place that knows what happened to the write says so, and a caller
// further out only fills in what nobody said.
func withOutcome(outcome string, err error) error {
	if err == nil {
		return nil
	}
	var known *writeOutcomeError
	if errors.As(err, &known) {
		return err
	}
	return &writeOutcomeError{outcome: outcome, err: err}
}

// storedWith is the error for a write that is stored on the owner and held by
// confirmed of factor copies: the write-concern timeout, or a replica refused
// for being plaintext.
func storedWith(confirmed, factor int, err error) error {
	return &writeOutcomeError{outcome: outcomeApplied, confirmed: confirmed, factor: factor, err: err}
}

// refusalWithOutcome gives a write's error frame — answered as a response, by a
// pre-check or by the handler that applied the entry, rather than as a Go
// error — the outcome its producer knows, unless it already carries one.
func refusalWithOutcome(payload []byte, outcome string) []byte {
	var refusal ErrorResponse
	if err := bson.Unmarshal(payload, &refusal); err != nil || refusal.WriteOutcome != "" {
		return payload
	}
	refusal.WriteOutcome = outcome
	marked, err := bson.Marshal(refusal)
	if err != nil {
		return payload
	}
	return marked
}

// isDataWrite reports whether op writes a bucket's data: the verbs whose
// refusal says whether the write happened (clients/handbook/errors.md section
// 4). Bucket and database DDL, user and cluster management, and restore are
// not, and neither is a read.
func isDataWrite(op transport.Opcode) bool {
	switch op {
	case transport.OpPut, transport.OpUpsert, transport.OpDelete, transport.OpBatchPut, transport.OpBatchUpsert,
		transport.OpFIFOPush, transport.OpFIFOPop, transport.OpLIFOPush, transport.OpLIFOPop,
		transport.OpBTreeInsert, transport.OpHeapPush, transport.OpHeapPop,
		transport.OpBlobWrite, transport.OpUpdate,
		transport.OpFieldPush, transport.OpFieldPop, transport.OpFieldPut, transport.OpFieldDelete,
		transport.OpQueueClaim, transport.OpQueueAck, transport.OpQueueRelease, transport.OpQueueExtend,
		transport.OpDeadRequeue, transport.OpDeadDiscard:
		return true
	}
	return false
}

// refusedBeforeApply gives a data write's error frame write_outcome
// not_applied (BUG-0514), unless it carries an outcome already or the request
// reached the store. applied is the sink the request's applies reported to, nil
// where none was attempted. An apply that committed makes a later error
// something other than a refusal, and a store failure is not known to have
// stored nothing (a commit can fail after its batch reached the store's
// memory): both keep what they say, which on a node that does not route is
// nothing, unknown to a client.
func refusedBeforeApply(op transport.Opcode, reply *transport.Frame, applied *applyOutcome) *transport.Frame {
	if reply.Opcode != transport.OpError || !isDataWrite(op) {
		return reply
	}
	if applied != nil && (applied.committed || applied.storeFailure != nil) {
		return reply
	}
	reply.Payload = refusalWithOutcome(reply.Payload, outcomeNotApplied)
	return reply
}

// errorFrame builds an error response carrying the reason.
//
// Code is always a real, non-OK transport.ErrorCode: left at its Go zero
// value, ErrorResponse.Code would serialize as 0, which is transport.ErrOK —
// a driver that trusts the code field (reasonably; that is what it is for)
// would read a failure as a success. Every handler error reaches the wire
// through this one function, so classifying it here once is enough; the
// classification is coarse (most callers just wrap an arbitrary Go error,
// with no sentinel to distinguish "bad request" from "internal failure"),
// but coarse-and-non-zero is what fixes the actual reported hazard. Finer
// per-case codes are a worthwhile follow-up, not required to close this gap.
func errorFrame(req *transport.Frame, err error) *transport.Frame {
	errResp := ErrorResponse{
		Code:    int(codeFor(err)),
		Message: explainError(err),
	}
	// A refused result's message is the typed error's own text: explainError
	// renders the whole wrap chain, and a btree walk's error arrives wrapped in
	// IterateDocuments' "iterating key %q:", which W-2's message must not carry.
	var tooLarge *resultTooLargeError
	if errors.As(err, &tooLarge) {
		errResp.Message = tooLarge.Error()
		errResp.MaxResultBytes = tooLarge.ceiling
		errResp.RetainedBytes = tooLarge.retained
	}
	// A duplicate's message is its own sentence (spec §4.6), not the store's
	// wrap chain around it: the same refusal reads the same from every path.
	var duplicate *store.DuplicateKeyError
	if errors.As(err, &duplicate) {
		errResp.Message = duplicate.Error()
	}
	// A path refusal shares INVALID_ARGUMENT with a malformed request, but it
	// is the document's answer on every copy and every replay: the frame says
	// so, because replay reads the frame (refusedByConstraint).
	errResp.ConstraintRefusal = errors.Is(err, ErrPathRefused)
	var repeated *repeatedIDError
	if errors.As(err, &repeated) {
		errResp.Message = repeated.Error()
		errResp.Results = repeated.results
	}
	var exists *documentExistsError
	if errors.As(err, &exists) {
		errResp.Message = exists.Error()
		errResp.Results = exists.results
	}
	var outcome *writeOutcomeError
	if errors.As(err, &outcome) {
		errResp.WriteOutcome = outcome.outcome
		errResp.CopiesConfirmed = outcome.confirmed
		errResp.ReplicationFactor = outcome.factor
	}
	body, merr := bson.Marshal(errResp)
	if merr != nil {
		body = nil
	}
	return transport.NewFrameWithID(transport.OpError, req.RequestID, body)
}

// codeFor classifies err for the wire. The handful of sentinels this
// codebase already defines get their natural code; everything else — the
// common case, since most handler errors are ad hoc fmt.Errorf wraps with no
// sentinel — falls back to ErrInternal, which is still correct: it is never
// ErrOK, so it can never be mistaken for success.
func codeFor(err error) transport.ErrorCode {
	var refusal *frameRefusal
	var nameTooLong *nameTooLongError
	var batch *batchRefusal
	var repeated *repeatedIDError
	var exists *documentExistsError
	switch {
	case errors.As(err, &repeated), errors.As(err, &exists):
		return transport.ErrDocumentExists
	case errors.As(err, &refusal):
		return refusal.code
	case errors.As(err, &batch):
		return batch.code
	case errors.As(err, &nameTooLong), errors.Is(err, errCreateInputRefused), errors.Is(err, ErrRestoreReservedName),
		errors.Is(err, ErrRestoreFormatVersion), errors.Is(err, ErrRestorePathEscapesRoot), errors.Is(err, ErrRestoreStoreTypeMismatch),
		errors.Is(err, ErrRestoreDropBlobDataLoss), errors.Is(err, ErrRestoreNoActiveSession):
		return transport.ErrInvalidArgument
	case errors.Is(err, ErrBucketNotFound), errors.Is(err, ErrDatabaseNotFound):
		return transport.ErrNotFound
	case errors.Is(err, ErrPermissionDenied):
		return transport.ErrPermissionDenied
	case errors.Is(err, ErrDatabaseAlreadyExists), errors.Is(err, ErrBucketAlreadyExists), errors.Is(err, ErrAlreadyMember),
		errors.Is(err, ErrNodeIDTaken):
		return transport.ErrAlreadyExists
	case errors.Is(err, ErrNotMember), errors.Is(err, ErrUserNotFound):
		return transport.ErrNotFound
	case errors.Is(err, ErrUserExists):
		return transport.ErrAlreadyExists
	case errors.Is(err, ErrUserVersion):
		return transport.ErrConflict
	case errors.Is(err, ErrUnknownRole), errors.Is(err, ErrTooManyGrants), errors.Is(err, ErrCredentialInvalid), errors.Is(err, ErrUsernameRequired), errors.Is(err, ErrReservedUsername), errors.Is(err, ErrUsernameInvalid),
		errors.Is(err, ErrSaltReused), errors.Is(err, ErrTooManyUsers), errors.Is(err, ErrLastAdmin):
		return transport.ErrInvalidArgument
	case errors.Is(err, ErrEpochInProgress):
		return transport.ErrUnavailable
	case errors.Is(err, ErrMemberMismatch):
		return transport.ErrConflict
	case errors.Is(err, ErrWriteConcernTimeout):
		return transport.ErrTimeout
	case errors.Is(err, ErrJoinSyncIncomplete), errors.Is(err, ErrNotOwner), errors.Is(err, ErrEpochMismatch), errors.Is(err, ErrRemovedNode),
		errors.Is(err, ErrOwnershipMoved), errors.Is(err, ErrOwnerMembershipMismatch), errors.Is(err, ErrForwardLost), errors.Is(err, errMemberLacksFeature), errors.Is(err, ErrNotHolder),
		errors.Is(err, errMembersUnheard), errors.Is(err, ErrNotYetAdded), errors.Is(err, ErrDuplicateNodeID), errors.Is(err, ErrSchemaChangePaused), errors.Is(err, ErrHandoffInProgress),
		errors.Is(err, ErrVersionLimit), errors.Is(err, ErrConsistentGetRefused),
		errors.Is(err, errAlterAwaitsPlacementPass), errors.Is(err, errAlterCopyPending), errors.Is(err, errDecreaseNotCovered),
		errors.Is(err, ErrMemberDiskFull), errors.Is(err, ErrStartHold), errors.Is(err, ErrListingIncomplete),
		errors.Is(err, ErrNotMaster), errors.Is(err, ErrNoQuorum), errors.Is(err, ErrRestoreInsufficientDiskSpace),
		errors.Is(err, ErrRestoreBucketBeingDumped), errors.Is(err, ErrRestoreBucketConflict),
		errors.Is(err, errBatchChainStopped), errors.Is(err, errBatchShuttingDown), errors.Is(err, errReplayingLog),
		errors.Is(err, ErrSpreadMarkShort), errors.Is(err, ErrSpreadPartLost), errors.Is(err, ErrSpreadDropping), errors.Is(err, ErrSpreadRecreated):
		return transport.ErrUnavailable
	case errors.Is(err, ErrDumpRequiresRoot), errors.Is(err, ErrDeadRestoreOnly):
		return transport.ErrPermissionDenied
	case errors.Is(err, ErrDumpAlreadyRunning):
		return transport.ErrConflict
	case errors.Is(err, ErrDumpNoActiveSession), errors.Is(err, ErrDumpDocumentTooLarge), errors.Is(err, ErrDumpNeedsNewerClient), errors.Is(err, ErrBlobTooLarge), errors.Is(err, ErrBlobEmpty), errors.Is(err, ErrDocumentTooLarge), errors.Is(err, ErrDocumentTooDeep), errors.Is(err, ErrMalformedPayload), errors.Is(err, ErrUpdateOperand), errors.Is(err, ErrPeekLimit), errors.Is(err, ErrFilteredUpdateOnSpread), errors.Is(err, ErrPopLimit), errors.Is(err, ErrClaimLimit), errors.Is(err, ErrLeaseOption), errors.Is(err, ErrClaimEntryTooLarge), errors.Is(err, ErrDeadOption), errors.Is(err, ErrExistsRefused), errors.Is(err, ErrWrongBucketType), errors.Is(err, ErrUnknownWriteConcern), errors.Is(err, ErrPathRefused), errors.Is(err, ErrAdmittedSetByClient), errors.Is(err, ErrRepairBoundSetByClient), errors.Is(err, ErrEmptyDocumentID),
		errors.Is(err, ErrTooLargeToReplicate), errors.Is(err, ErrFilterRefused), errors.Is(err, ErrNotSupportedOnSpread):
		return transport.ErrInvalidArgument
	case errors.Is(err, ErrDumpPinCeilingExceeded):
		return transport.ErrUnavailable
	case errors.Is(err, ErrDumpDigestMismatch):
		return transport.ErrConflict
	case errors.Is(err, ErrResultTooLarge):
		return transport.ErrResultTooLarge
	case errors.Is(err, errBatchOverBound):
		return transport.ErrBatchTooLarge
	case errors.Is(err, ErrFieldType):
		return transport.ErrFieldType
	case errors.Is(err, store.ErrDuplicateKey):
		return transport.ErrDuplicateKey
	case errors.Is(err, ErrStoreDeclarationInvalid), errors.Is(err, ErrFieldDeclarationInvalid), errors.Is(err, ErrFieldWrongVerb), errors.Is(err, ErrFieldAddressIncomplete):
		return transport.ErrInvalidArgument
	case errors.Is(err, ErrFieldNotDeclared), errors.Is(err, ErrFieldParentNotFound):
		return transport.ErrNotFound
	case errors.Is(err, ErrDeclarationUnusable):
		return transport.ErrUnsupportedVersion
	default:
		return transport.ErrInternal
	}
}

// errorResponse is errorFrame plus the nil error that keeps the client's stream
// alive. Handlers return exactly this: a defined response the caller can decode,
// not a reset.
func errorResponse(frame *transport.Frame, err error) (*transport.Frame, error) {
	return errorFrame(frame, err), nil
}

// failureMessage builds a client-facing message from the operation that failed
// and its cause.
//
// The mutating handlers report an ordinary failure as a Success:false response
// rather than an error frame, so explainError has to be applied here too —
// otherwise a full disk reaches the client as a correct but unhelpful sentence.
func failureMessage(context string, err error) string {
	return fmt.Sprintf("%s: %s", context, explainError(err))
}

// readFailureMessage is failureMessage for a read of one bucket, and it logs
// the internal key a failed read stopped at before the message drops it. A read
// that reached a damaged file of the node store is answered with the bucket's
// name first: the store is shared by every bucket on the node, so the storage
// error, which names the file and the repair, cannot say whose read it was.
func (s *Server) readFailureMessage(database, bucket, context string, err error) string {
	s.logKeyReadFailure(err)
	var damaged *storage.DamagedReadError
	if errors.As(err, &damaged) {
		return fmt.Sprintf("bucket %q in database %q could not be read: %v", bucket, database, damaged)
	}
	message := failureMessage(context, err)
	// The sentence is true only of a failed storage read, the one failure that
	// leaves a key in the log. A deliberate refusal (an index that is not ready)
	// carries its own advice, and so does a damaged blob, which a retry cannot
	// repair.
	var keyErr *storage.KeyError
	if !errors.As(err, &keyErr) || errors.Is(err, sealed.ErrBlobCorrupt) {
		return message
	}
	return fmt.Sprintf("%s. Bucket %q in database %q could not be read: retry the command once, and if it fails again check the node log, which records the failing key",
		message, bucket, database)
}

// explainError adds the recovery step to failures whose cause does not imply it.
//
// A full disk is the case that prompted this: it surfaced as an empty
// Success:false with a bare error opcode, so a client could not tell "the node
// ran out of space, free some and retry" from "this request was wrong". The
// underlying error text is kept — it names the filesystem's own reason — and the
// hint is appended rather than replacing it.
func explainError(err error) string {
	if err == nil {
		return ""
	}
	var overBound *batchTooLargeRefusal
	var tooLarge *storage.BatchTooLargeError
	var entryTooLarge *store.IndexEntryTooLargeError
	switch {
	case errors.As(err, &overBound):
		return overBound.Error()
	case errors.As(err, &tooLarge):
		return batchTooLargeMessage(tooLarge, "")
	case errors.As(err, &entryTooLarge):
		return indexEntryTooLargeMessage(entryTooLarge)
	case errors.Is(err, syscall.ENOSPC):
		return fmt.Sprintf("%v. The node has run out of disk space: free space on it (check the data directory) and retry, or write to a bucket on another node. A node that fills its disk while serving can be killed by the kernel on the next write, so treat this as urgent", err)
	case errors.Is(err, syscall.EDQUOT):
		return fmt.Sprintf("%v. The node has exceeded its disk quota: raise the quota or free space, then retry", err)
	case errors.Is(err, syscall.EROFS):
		return fmt.Sprintf("%v. The node's data directory is on a read-only filesystem: remount it read-write and restart the node", err)
	case errors.Is(err, syscall.EACCES):
		return fmt.Sprintf("%v. The node cannot write to its data directory: check ownership and permissions on it (the server must own the directory it was given)", err)
	}
	return err.Error()
}

// logKeyReadFailure records the internal storage key of a failed operation at
// one node-store key, which the client's message leaves out because the key is
// an encoding it never sent; an operator diagnosing the failure needs it. It is
// logBatchTooLarge's counterpart for the read and write paths, and it logs
// through the server's configured logger rather than the process default.
func (s *Server) logKeyReadFailure(err error) {
	var keyErr *storage.KeyError
	if !errors.As(err, &keyErr) {
		return
	}
	// A missing key is an ordinary answer on the read paths, not a failure: only
	// the key of an operation that broke is worth an operator's log line.
	if errors.Is(keyErr.Cause, storage.ErrKeyNotFound) {
		return
	}
	s.logger.Warn("a node-store operation failed at a key",
		"key", fmt.Sprintf("%x", keyErr.Key), "error", err)
}

// indexEntryTooLargeMessage tells a client which indexed field made an index
// entry too long for the store, that nothing was written, and what to do. The
// limit comes from the error because the room a value has depends on the
// entry's layout (index prefix, type tag, document key). The key itself holds
// the caller's data and is logged by logKeyTooLarge, not repeated here.
func indexEntryTooLargeMessage(refused *store.IndexEntryTooLargeError) string {
	quoted := make([]string, len(refused.Fields))
	for i, field := range refused.Fields {
		quoted[i] = strconv.Quote(field)
	}
	noun := "field " + quoted[0]
	if len(quoted) > 1 {
		noun = "fields " + strings.Join(quoted, ", ")
	}
	return fmt.Sprintf("the indexed %s of document %q is longer than index %q can hold: the index entry would be %s bytes, and the store's key limit is %s bytes including the value, so nothing was written. "+
		"Index a shorter field, store the long text in a field that is not indexed, or index a hash or a prefix of it",
		noun, refused.Document, refused.Index, groupThousands(refused.Cause.Size), groupThousands(refused.Cause.Limit))
}

// logKeyTooLarge records the storage key an oversized index entry stopped at,
// which the client's message leaves out.
func (s *Server) logKeyTooLarge(database, bucket string, err error) {
	var tooLarge *storage.KeyTooLargeError
	if !errors.As(err, &tooLarge) {
		return
	}
	s.logger.Warn("refused a document whose index entry is longer than the store's key limit",
		"database", database, "bucket", bucket, "key_bytes", tooLarge.Size, "limit_bytes", tooLarge.Limit,
		"key_prefix", fmt.Sprintf("%x", tooLarge.Key[:min(len(tooLarge.Key), 64)]))
}

// batchTooLargeMessage tells a client what an over-budget batch needs. It does
// not print the error's own text, which ends in the storage key the batch stopped
// at: an encoding the caller never sent and cannot act on. The key is logged by
// logBatchTooLarge instead. The sentence that opens it is storage.ErrBatchTooLarge's
// text, because the ingest command matches that text to halve a batch; cli's
// TestBatchTooLargeSentinelTextMatchesStorage pins the match.
//
// sizeSentence, when not empty, is inserted after the opening sentence: what the
// caller sent, so it can see how far over the bound it was.
func batchTooLargeMessage(tooLarge *storage.BatchTooLargeError, sizeSentence string) string {
	limits := fmt.Sprintf("%s bytes", groupThousands(int(tooLarge.Bytes)))
	if tooLarge.Entries > 0 {
		limits = fmt.Sprintf("%s bytes and %s entries", groupThousands(int(tooLarge.Bytes)), groupThousands(int(tooLarge.Entries)))
	}
	if tooLarge.EntryCountReached {
		return fmt.Sprintf("This %s, so none of it was written.%s It ran out of entries before it ran out of bytes (the entry-count bound): the store commits at most %s in one batch, and every document adds one entry plus one for each index entry it produces, so small documents reach the entry count first. Send fewer documents per batch, or load the file with the hoarddb ingest command, which sizes batches for you. See the limitations page of the user docs.",
			storage.ErrBatchTooLarge.Error(), sizeSentence, limits)
	}
	return fmt.Sprintf("This %s, so none of it was written.%s The store commits at most %s in one batch, counting every index entry a document adds (a value of 1 MiB or more counts as almost nothing). Send fewer or smaller documents per batch - for example 38 documents of 100 KiB, or 600 of 8 KiB, fit - or load the file with the hoarddb ingest command, which sizes batches for you. See the limitations page of the user docs.",
		storage.ErrBatchTooLarge.Error(), sizeSentence, limits)
}

// errBatchOverBound is under every batch put refused for its size; codeFor
// answers it BATCH_TOO_LARGE. It is not storage.ErrBatchTooLarge, which a pop,
// a claim or a field store's delete can also meet, and whose code stays what it
// was.
var errBatchOverBound = errors.New("batch over the store's atomic bound")

// batchTooLargeRefusal is a batch put the store refused whole. It carries what
// the client sent so the message can say how far over the bound it was.
type batchTooLargeRefusal struct {
	cause        *storage.BatchTooLargeError
	documents    int
	encodedBytes int
}

func (r *batchTooLargeRefusal) Error() string {
	sent := fmt.Sprintf(" This batch is %s documents", groupThousands(r.documents))
	if r.encodedBytes > 0 {
		sent += fmt.Sprintf(", %s encoded", formatBytesIEC(int64(r.encodedBytes)))
	}
	return batchTooLargeMessage(r.cause, sent+".")
}

// Is makes errors.Is match errBatchOverBound and, through the cause, the
// storage sentinel the apply outcome already keys on.
func (r *batchTooLargeRefusal) Is(target error) bool { return target == errBatchOverBound }

func (r *batchTooLargeRefusal) Unwrap() error { return r.cause }

// OverBoundBatchMessage is the wire text of a batch of documents over the store's
// bound, for a store that commits bytesBound bytes and entriesBound entries
// (entryCountReached says which one the batch ran out of). cmd/gen-testvectors
// builds the driver vector from it, so the vector and the server cannot drift
// into two hand-written sentences.
func OverBoundBatchMessage(bytesBound, entriesBound int64, entryCountReached bool, documents, encodedBytes int) string {
	cause := &storage.BatchTooLargeError{Bytes: bytesBound, Entries: entriesBound, EntryCountReached: entryCountReached}
	return (&batchTooLargeRefusal{cause: cause, documents: documents, encodedBytes: encodedBytes}).Error()
}

// overBoundAnswer turns the in-band BATCH_PUT_OK refusal of a batch over the
// store's bound into the ERROR frame a client is answered with (BUG-0467 G11):
// the handler's in-band shape is for a replica's apply and a replay, which read
// it. frame is the client's request and reply the handler's answer to it; any
// other reply is returned as it is.
func overBoundAnswer(frame, reply *transport.Frame, applied *applyOutcome) *transport.Frame {
	if !isOverBoundReply(transport.Opcode(reply.Opcode), applied) {
		return reply
	}
	return errorFrame(frame, applied.overBound)
}

// isOverBoundReply reports whether a handler answered with the in-band refusal
// of a batch over the store's bound, which applied recorded.
func isOverBoundReply(replyOpcode transport.Opcode, applied *applyOutcome) bool {
	return applied != nil && applied.overBound != nil &&
		(replyOpcode == transport.OpBatchPutOK || replyOpcode == transport.OpBatchUpsertOK)
}
