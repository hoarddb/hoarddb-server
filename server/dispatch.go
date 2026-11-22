package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// NewHandler creates a transport.RequestHandler that dispatches frames to
// the core Server methods.
func NewHandler(s *Server) transport.RequestHandler {
	return func(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
		// The raw payload is validated first, before anything below reads it:
		// the node-to-node handlers decode their request to find the cluster
		// token, the authorization gate decodes it to find the bucket and
		// database it names, and cluster routing, the write path and every
		// handler decode it again (server/nesting_depth.go). Bytes Validate
		// refuses are answered without being decoded by any of them. The
		// handlers below take the frame, so they read the validated bytes as
		// frame.Payload, not as the Document.
		if _, err := validateRequest(frame.Opcode, frame.Payload); err != nil {
			if frame.Opcode == transport.OpBatch {
				return s.refuseBatch(frame, batchDepthRefusal(frame.Payload, err)), nil
			}
			return refusedBeforeApply(frame.Opcode, errorFrame(frame, err), nil), nil
		}

		// Node-to-node traffic is handled next: these frames authenticate
		// with the cluster token and are never treated as client requests.
		// Nothing in this switch meets opRules, so an opcode belongs here
		// only when the driver listener refuses it by name and its handler
		// checks the cluster token; an opcode an operator sends from a user
		// session goes below the gate.
		// TestNoOpcodeIsDispatchedBeforeAuthorizeWithoutARecordedGate holds
		// both conditions for every case.
		switch frame.Opcode {
		case transport.OpGossip:
			return handleGossip(ctx, frame, s)
		case transport.OpReplPush:
			return handleReplPush(ctx, frame, s)
		case transport.OpClusterPropose:
			return handleClusterPropose(ctx, frame, s)
		case transport.OpMembershipChange:
			return handleMembershipChange(ctx, frame, s)
		case transport.OpMigrateStart:
			return handleMigrateStart(ctx, frame, s)
		case transport.OpMigrateData:
			return handleMigrateData(ctx, frame, s)
		case transport.OpMigrateDone:
			return handleMigrateDone(ctx, frame, s)
		case transport.OpVersionedGet:
			return handleVersionedGet(ctx, frame, s)
		case transport.OpAuthConfigPush:
			return handleAuthConfigPush(ctx, frame, s)
		}

		// DEAD_RESTORE is a restore's own write, routed by Server.routeWrite,
		// which never passes this handler. A client that sends it, whatever
		// its role, is refused before routing (claim-ack-spec 7.3).
		if frame.Opcode == transport.OpDeadRestore {
			return errorResponse(frame, withOutcome(outcomeNotApplied, ErrDeadRestoreOnly))
		}

		// Authorization gate: one check, before dispatch. It sits above
		// cluster routing and the user handlers, and below the node-to-node
		// switch (a peer session is exempt by session, not by opcode). It
		// must NOT live in s.dispatch: WAL replay and replicated entries
		// funnel through there by design, so a gate there would refuse
		// replay after a restart and the node would never come back
		// (TestGateDoesNotRunOnWALReplay).
		// A data write it refuses was never routed or applied: not_applied,
		// on a cluster node as on a standalone one (BUG-0514).
		if err := s.authorize(ctx, frame); err != nil {
			return refusedBeforeApply(frame.Opcode, errorFrame(frame, err), nil), nil
		}

		// No client frame may name a part: a part name holds the separator no
		// client can create (B7). It is answered as NOT FOUND of the displayed
		// name, before routing, whatever the frame carries. Router-built
		// frames never pass here, and a node-to-node frame was answered above.
		if err := refuseClientPartName(frame); err != nil {
			return refusedBeforeApply(frame.Opcode, errorFrame(frame, err), nil), nil
		}

		// The blob cap and the document limit are client-write rules: they sit
		// here, ahead of cluster routing and the WAL, so a replica or a replay
		// never meets them (server/blob_limit.go, server/document_limit.go).
		// A put whose bucket this node cannot resolve is marked so the owner
		// applies the bucket's real limit (BUG-0582).
		deferred, err := s.refuseOversizedWrite(frame)
		if err != nil {
			return refusedBeforeApply(frame.Opcode, errorFrame(frame, err), nil), nil
		}
		if deferred {
			ctx = withTypeLimitDeferred(ctx)
		}
		// The admitted flag is read repair's own (typed-fields spec §5.6): a
		// client that sets it is refused here, at the client door, and never
		// inside dispatch, which replay, replication and read repair all
		// funnel through and which must keep it.
		if err := refuseClientAdmitted(frame); err != nil {
			return errorResponse(frame, withOutcome(outcomeNotApplied, err))
		}
		if err := refuseClientRepairBound(frame); err != nil {
			return errorResponse(frame, withOutcome(outcomeNotApplied, err))
		}

		// A batch is answered by its own handler, below every gate above: the
		// envelope has passed them under its unscoped row, and the handler runs
		// each of them again on every item before any item runs
		// (batch-request spec §4.2). It is never a localHandlers entry.
		if frame.Opcode == transport.OpBatch {
			return s.handleBatch(ctx, frame)
		}

		// The operator's membership and rebalancing verbs and user management
		// are handled before cluster bucket routing: these frames are not
		// bucket-scoped and must not be forwarded to a bucket owner. The
		// cluster verbs act on the node that receives them; user mutations
		// enforce master-only themselves (spec 5.4), and list and get are
		// served by any node.
		switch frame.Opcode {
		case transport.OpClusterAdd:
			return handleClusterAdd(ctx, frame, s)
		case transport.OpClusterRemove:
			return handleClusterRemove(ctx, frame, s)
		case transport.OpMigrate:
			return handleMigrate(ctx, frame, s)
		case transport.OpUserCreate:
			return handleUserCreate(ctx, frame, s)
		case transport.OpUserAlter:
			return handleUserAlter(ctx, frame, s)
		case transport.OpUserDrop:
			return handleUserDrop(ctx, frame, s)
		case transport.OpUserList:
			return handleUserList(ctx, frame, s)
		case transport.OpUserGet:
			return handleUserGet(ctx, frame, s)
		case transport.OpUserGrant:
			return handleUserGrant(ctx, frame, s)
		case transport.OpUserRevoke:
			return handleUserRevoke(ctx, frame, s)
		}

		// On a cluster node every frame is routed: bucket-scoped mutations go
		// to the bucket's owner and are acknowledged only once the write
		// concern is met, and reads are answered locally when this node holds
		// a copy. A standalone node dispatches straight through.
		//
		// A removed node comes in by the routed door whatever its ring holds:
		// removed from a two-node cluster its ring has one member and Enabled
		// is false, and its reads were answered from copies that stopped
		// changing at the removal (BUG-0628). The routed door forwards them to
		// a member, as it does for a node removed from a larger cluster. Two
		// atomic loads per frame, nothing per bucket.
		if s.cluster != nil && (s.cluster.Enabled() || s.cluster.removed()) {
			return s.cluster.handleClientFrame(ctx, frame)
		}
		// A read commits nothing, and is kept out of the writes a sync
		// waits to gather (ackSyncGroup).
		if frame.Opcode.IsReadOnly() {
			return s.fencedDispatch(ctx, frame)
		}
		// acknowledgeDurably drops the answer of a write whose sync fails; a
		// pop's is kept here, because it is the only copy of the entry.
		var dispatched *transport.Frame
		reply, err := acknowledgeDurably(ctx, s, func(ctx context.Context) (*transport.Frame, error) {
			answer, dispatchErr := s.fencedDispatch(ctx, frame)
			dispatched = answer
			return answer, dispatchErr
		})
		var notDurable *writeNotDurableError
		if !errors.As(err, &notDurable) {
			return reply, err
		}
		// Whether the answer holds taken entries is answersWithTakenEntries';
		// whether the words are pop's or claim's is isPopOpcode's
		// (claim-ack-spec 5.4). Both decide here.
		if answersWithTakenEntries(frame.Opcode) {
			target := popTargetOf(frame)
			unanswered, answerTaken := notDurable.forPop(target), notDurable.answerPop
			warning := "a pop is answered with its entry although its removal could not be synced; the entry may be served again after a restart"
			if !isPopOpcode(frame.Opcode) {
				unanswered, answerTaken = notDurable.forClaim(target), notDurable.answerClaim
				warning = "a claim is answered with its lease although it could not be synced; after a restart the lease may be unknown and its entries delivered again"
			}
			err = withOutcome(outcomeUnknown, unanswered)
			if dispatched != nil && answerHoldsPoppedEntry(frame.Opcode, dispatched.Opcode, dispatched.Payload) {
				taken, stampErr := answerTaken(dispatched, target)
				if stampErr == nil {
					s.logger.Warn(warning, "op", frame.Opcode, "target", target, "error", notDurable.syncErr)
					return taken, nil
				}
				err = withOutcome(outcomeUnknown, errors.Join(err, stampErr))
			}
		}
		return errorResponse(frame, err)
	}
}

// localHandler executes one frame against local storage.
type localHandler func(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error)

// localHandlers is every opcode this build executes against local storage,
// and the one place that says so. Server.dispatch answers from it, and
// implementsOpcode reads it, so an owner refuses a forwarded write it cannot
// run before the write reaches its log (BUG-0561): the two cannot disagree
// about an opcode, because there is no second list to add it to. It is filled
// in init because the handlers reach Server.dispatch, which reads it.
var localHandlers map[transport.Opcode]localHandler

func init() {
	localHandlers = map[transport.Opcode]localHandler{
		transport.OpAuth: func(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
			return handleAuth(ctx, frame, s.auth, s, s.logger)
		},
		transport.OpGet:           handleGet,
		transport.OpGetFast:       handleGetFast,
		transport.OpConsistentGet: handleConsistentGet,
		transport.OpPut:           handlePut,
		transport.OpUpsert:        handleUpsert,
		transport.OpBatchUpsert:   handleBatchUpsert,
		transport.OpDelete:        handleDelete,
		transport.OpExists:        handleExists,
		transport.OpBatchPut:      handleBatchPut,
		transport.OpFIFOPush:      handleFIFOPush,
		transport.OpFIFOPop:       handleFIFOPop,
		transport.OpFIFOPeek:      handleFIFOPeek,
		transport.OpQueueClaim:    handleQueueClaim,
		transport.OpQueueAck:      handleQueueAck,
		transport.OpQueueRelease:  handleQueueRelease,
		transport.OpQueueExtend:   handleQueueExtend,
		transport.OpDeadList:      handleDeadList,
		transport.OpDeadRequeue:   handleDeadRequeue,
		transport.OpDeadDiscard:   handleDeadDiscard,
		transport.OpDeadRestore:   handleDeadRestore,
		transport.OpLIFOPush:      handleLIFOPush,
		transport.OpLIFOPop:       handleLIFOPop,
		transport.OpLIFOPeek:      handleLIFOPeek,
		transport.OpBTreeInsert:   handleBTreeInsert,
		transport.OpBTreeLookup:   handleBTreeLookup,
		transport.OpHeapPush:      handleHeapPush,
		transport.OpHeapPop:       handleHeapPop,
		transport.OpCreateDB:      handleCreateDB,
		transport.OpCreateBucket:  handleCreateBucket,
		transport.OpDeleteDB:      handleDeleteDB,
		transport.OpDeleteBucket:  handleDeleteBucket,
		transport.OpListBuckets:   handleListBuckets,
		transport.OpListDBs:       handleListDatabases,
		transport.OpBucketStats:   handleBucketStats,
		transport.OpBucketInfo:    handleBucketInfo,
		transport.OpUpdate:        handleUpdate,
		transport.OpBTreeFirst: func(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
			return handleEdge(ctx, frame, s, false)
		},
		transport.OpBTreeLast: func(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
			return handleEdge(ctx, frame, s, true)
		},
		transport.OpBlobWrite: handleBlobWrite,
		transport.OpBlobRead:  handleBlobRead,
		transport.OpPing: func(_ context.Context, frame *transport.Frame, _ *Server) (*transport.Frame, error) {
			return respond(frame, transport.OpPong, &PingResponse{OK: true})
		},
		transport.OpHealthCheck:        handleHealth,
		transport.OpListNodes:          handleListNodes,
		transport.OpAlterBucket:        handleAlterBucket,
		transport.OpDeclareStoreFields: handleDeclareStoreFields,
		transport.OpFieldPush:          handleFieldPush,
		transport.OpFieldPop:           handleFieldPop,
		transport.OpFieldPeek:          withoutContext(handleFieldPeek),
		transport.OpFieldPut:           handleFieldPut,
		transport.OpFieldGet:           withoutContext(handleFieldGet),
		transport.OpFieldDelete:        handleFieldDelete,
		transport.OpFieldLen:           withoutContext(handleFieldLen),
		transport.OpFieldList:          withoutContext(handleFieldList),
		transport.OpDumpStart:          handleDumpStart,
		transport.OpDumpChunk:          handleDumpChunk,
		transport.OpDumpDone:           handleDumpDone,
		transport.OpRestoreStart:       handleRestoreStart,
		transport.OpRestoreChunk:       handleRestoreChunk,
		transport.OpRestoreDone:        handleRestoreDone,
		transport.OpScan:               handleScan,
		transport.OpSearch:             handleSearch,
		transport.OpBTreeRange:         handleBTreeRange,
		transport.OpCount:              handleCount,
		transport.OpDistinct:           handleDistinct,
	}
}

// withoutContext adapts a handler that reads nothing from the context.
func withoutContext(handle func(frame *transport.Frame, s *Server) (*transport.Frame, error)) localHandler {
	return func(_ context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
		return handle(frame, s)
	}
}

// implementsOpcode reports whether this build executes op (localHandlers).
func implementsOpcode(op transport.Opcode) bool {
	if withheld := withheldOpcodeForTesting.Load(); withheld != nil && *withheld == op {
		return false
	}
	_, ok := localHandlers[op]
	return ok
}

// withheldOpcodeForTesting makes implementsOpcode answer false for one opcode,
// so a test can stand in for an owner whose build lacks it. It is an atomic
// and not a delete from localHandlers because every running server goroutine
// reads that map, and a write from the test goroutine races them.
var withheldOpcodeForTesting atomic.Pointer[transport.Opcode]

// dispatch executes one frame against local storage. Client requests,
// replicated entries and WAL replay all funnel through here, so every
// mutation has exactly one implementation rather than a second one that can
// drift out of agreement with the first.
//
// A query runs under an execution slot (admitQuery). Admission is taken here
// and not in NewHandler: a coordinator that only forwards must not be charged
// a slot for work it does not execute, and the owner's execution of a
// forwarded query arrives here through handleReplPush, never through
// NewHandler. WAL replay also enters here, but never with a query: no read is
// ever appended to the WAL.
func (s *Server) dispatch(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
	handle, ok := localHandlers[frame.Opcode]
	if !ok {
		s.logger.Warn("unsupported opcode", "opcode", frame.Opcode)
		return errorResponse(frame, fmt.Errorf("unsupported opcode 0x%02X (request id %s): this server does not implement it",
			uint8(frame.Opcode), frame.RequestIDString()))
	}
	if isQueryOpcode(frame.Opcode) {
		return s.admitQuery(ctx, frame, func(ctx context.Context) (*transport.Frame, error) {
			return handle(ctx, frame, s)
		})
	}
	return handle(ctx, frame, s)
}

// handleAuth processes an AUTH request. The transport layer owns session
// state; this validates credentials or a resumption token, sets the session
// identity through the context, and returns a token plus a reason code on
// every failure path.
func handleAuth(ctx context.Context, frame *transport.Frame, authMgr *AuthManager, s *Server, logger *slog.Logger) (*transport.Frame, error) {
	var req AuthRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return authFailFrame(frame, transport.AuthFailInternal,
			fmt.Sprintf("could not decode AUTH request: %v", err)), nil
	}
	// Until a credential has checked out, the username is only what the
	// client sent: logs and audit entries repeat a bounded part of it.
	loggedUser := boundedIdentifier(req.Username)

	// Protocol version assertion (spec 3.1): a driver that believes it
	// speaks a different major gets a named failure at AUTH instead of a
	// decode failure later, and the log records which fleet version is
	// actually connecting. Unknown capability strings are ignored, never an
	// error.
	if req.ProtocolVersion != 0 && uint8(req.ProtocolVersion) != transport.ProtocolMajor {
		logger.Warn("client protocol version mismatch",
			"username", loggedUser,
			"client_protocol_version", req.ProtocolVersion,
			"server_protocol_version", transport.ProtocolMajor,
		)
		return authFailFrame(frame, transport.AuthFailProtocolMismatch,
			fmt.Sprintf("server speaks protocol %d, client sent %d", transport.ProtocolMajor, req.ProtocolVersion)), nil
	}
	if req.Client != "" || len(req.Capabilities) > 0 {
		logger.Info("client connect", "username", loggedUser, "client", boundedIdentifier(req.Client),
			"capabilities", boundedCapabilities(req.Capabilities))
	}

	// The session travels in the context; it is where identity is recorded so
	// transport never imports server (spec 3.7).
	sess, _ := transport.SessionFromContext(ctx)

	// The internode listener is for cluster members, which present the node
	// identity and the cluster token. A user's password or resumption token
	// is refused here before any derivation, the mirror of refusing cluster
	// credentials on the driver listener: a session on this port is exempt
	// from the user gate, so a user account must never become one.
	if sess != nil && sess.Peer && (req.Token != "" || req.Username != NodeUser) {
		logger.Warn("user credentials on internode listener", "addr", remoteAddr(sess))
		s.audit.Log(AuditEntry{Event: EventAuthFailure, User: loggedUser, Result: "user credentials on internode port", AuthMethod: authMethodOf(req)})
		return authFailFrame(frame, transport.AuthFailCredentials,
			"user accounts are not accepted on the internode listener; connect to the driver port (HOARDDB_TCP_LISTEN, 7433 by default)"), nil
	}

	// A node still waiting for its first config, or a follower past the
	// staleness bound, refuses logins and token refreshes. Node credentials on
	// the internode port are exempt: a joining node authenticates with the
	// cluster token before it has any user config (spec 5.3.4, 5.3.5).
	if req.Username != NodeUser && (sess == nil || !sess.Peer) {
		if reason := s.authConfigUnavailableReason(authMgr); reason != "" {
			logger.Warn("AUTH refused: auth config unavailable", "username", loggedUser, "reason", reason)
			return uncheckedAuthFail(frame, transport.AuthFailInternal, reason), nil
		}
	}

	// A cluster member that holds no cluster password key cannot check any
	// password: it says why, before the connection's one password attempt is
	// spent and before any Argon2 runs (ring user store spec 5.5, 6.2).
	if req.Token == "" && req.Username != NodeUser {
		if reason := authMgr.passwordLoginRefusal(); reason != "" {
			logger.Warn("password AUTH refused: no cluster password key on this node", "username", loggedUser, "reason", reason)
			return uncheckedAuthFail(frame, transport.AuthFailInternal, reason), nil
		}
	}

	// A non-empty Token means token semantics and the Password in the same
	// frame is ignored entirely. There is no in-frame fallback: that is the
	// driver's job on the next frame, and an in-frame fallback would make
	// every reason code meaningless and let one frame cost a validation plus
	// a 33 ms derivation (spec 3.2 rule 3).
	if req.Token != "" {
		return handleTokenAuth(frame, req, authMgr, s, sess, logger)
	}

	// Password AUTH is accepted once per connection. The claim is taken before
	// the derivation runs so concurrent pipelined AUTH frames cannot each pay
	// for one, and an already-authenticated connection is refused instead of
	// re-deriving (spec 3.8). Token AUTH is exempt: a refresh is allowed.
	if sess != nil && !sess.BeginPasswordAuth() {
		logger.Warn("password AUTH on an already-claimed connection", "addr", sess.RemoteAddr)
		s.audit.Log(AuditEntry{Event: EventAuthFailure, User: loggedUser, Result: "already authenticated", AuthMethod: "password"})
		return authFailFrame(frame, transport.AuthFailAlreadyAuthed,
			"connection is already authenticated; reuse the session instead of sending a password again"), nil
	}

	// Password semantics. standing is the account's login standing, taken
	// with the credentials checked; the node identity has none.
	var username string
	var standing *transport.LoginStanding
	if req.Username == NodeUser {
		// Cluster credentials authenticate on the internode listener and
		// nowhere else (spec 10.2).
		if sess == nil || !sess.Peer {
			logger.Warn("node credentials on driver listener", "addr", remoteAddr(sess))
			s.audit.Log(AuditEntry{Event: EventAuthFailure, User: req.Username, Result: "cluster credentials on driver port", AuthMethod: "password"})
			return authFailFrame(frame, transport.AuthFailCredentials,
				"cluster credentials are only accepted on the internode listener"), nil
		}
		if s.cluster == nil || !s.cluster.ValidateToken(req.Password) {
			logger.Warn("node auth failed", "reason", "invalid cluster token")
			s.audit.Log(AuditEntry{Event: EventAuthFailure, User: req.Username, Result: "invalid cluster token", AuthMethod: "password"})
			return authFailFrame(frame, transport.AuthFailCredentials, "invalid cluster token"), nil
		}
		username = req.Username
	} else {
		// One derivation at a time process-wide, so MaxConnsPerIP cannot
		// multiply 64 MiB by the connection count (spec 3.8). The slot is
		// released as soon as VerifyUser returns; the Argon2 call itself is
		// the only thing that runs under it.
		if !authMgr.acquireDerivation() {
			// No password was checked, so the refusal must not spend the
			// connection's one attempt. The transport sees the
			// RATE_LIMITED reason and keeps it out of the per-IP ledger.
			if sess != nil {
				sess.AbandonPasswordAuth()
			}
			logger.Warn("AUTH derivation rate limited", "username", loggedUser)
			s.audit.Log(AuditEntry{Event: EventAuthFailure, User: loggedUser, Result: "derivation rate limited", AuthMethod: "password"})
			return authFailFrame(frame, transport.AuthFailRateLimited, authMgr.busyRefusalMessage()), nil
		}
		var err error
		standing, err = authMgr.verifyLogin(req.Username, req.Password)
		authMgr.releaseDerivation()
		if err != nil {
			logger.Warn("auth failed", "username", loggedUser, "error", err)
			s.audit.Log(AuditEntry{Event: EventAuthFailure, User: loggedUser, Result: err.Error(), AuthMethod: "password"})
			return authFailFrame(frame, transport.AuthFailCredentials, "invalid username or password"), nil
		}
		username = req.Username
	}

	var minted TokenClaims
	var record *UserRecord
	if username == NodeUser {
		// The node identity is not a user record; mint its claims from a
		// synthetic record so the node path does not go through ClaimsFor.
		record = &UserRecord{
			Username: NodeUser,
			Roles:    []Grant{{Role: "admin", DB: "*"}},
		}
		minted = authMgr.MintClaims(record, nil, time.Now())
	} else {
		var err error
		record, err = authMgr.recordFor(username)
		if err != nil {
			return authFailFrame(frame, transport.AuthFailInternal, "could not mint claims: "+err.Error()), nil
		}
		record.standing = standing
		minted = authMgr.MintClaims(record, nil, time.Now())
	}
	return finishAuth(frame, authMgr, s, sess, record, minted, false)
}

// handleTokenAuth validates a resumption token, re-derives claims from the
// record intersected with the presented token, and refreshes the session.
func handleTokenAuth(frame *transport.Frame, req AuthRequest, authMgr *AuthManager,
	s *Server, sess *transport.Session, logger *slog.Logger) (*transport.Frame, error) {

	// The staleness bound applies to refreshes as much as to fresh logins
	// (spec 5.3.5): otherwise a dropped user's 900 s token on a node that has
	// not heard the epoch bump is renewable indefinitely. This is also the
	// refresh half of the config-less bootstrap block.
	if reason := s.authConfigUnavailableReason(authMgr); reason != "" {
		logger.Warn("token refresh refused: auth config unavailable", "reason", reason)
		return uncheckedAuthFail(frame, transport.AuthFailInternal, reason), nil
	}

	claims, err := authMgr.ValidateToken(req.Token)
	if err != nil {
		reason := transport.AuthFailTokenInvalid
		if errors.Is(err, ErrTokenExpired) {
			reason = transport.AuthFailTokenExpired
		}
		logger.Warn("token auth failed", "reason", err)
		s.audit.Log(AuditEntry{Event: EventAuthFailure, User: boundedIdentifier(claims.Subject), Result: err.Error(), AuthMethod: "token"})
		return authFailFrame(frame, reason, tokenFailureMessage(reason)), nil
	}

	// A non-empty Username that disagrees with the token subject is a driver
	// bug and is refused cheaply, before any derivation (spec 3.2 rule 3).
	if req.Username != "" && req.Username != claims.Subject {
		return authFailFrame(frame, transport.AuthFailTokenInvalid,
			"username does not match the token subject"), nil
	}
	standing, err := authMgr.verifyClaimsStanding(claims)
	if err != nil {
		logger.Warn("token auth failed", "username", claims.Subject, "error", err)
		return authFailFrame(frame, transport.AuthFailTokenInvalid,
			"token is no longer valid; authenticate again with a password"), nil
	}

	if tokenRefreshGapHook != nil {
		tokenRefreshGapHook()
	}
	record, err := authMgr.recordFor(claims.Subject)
	if err != nil {
		return authFailFrame(frame, transport.AuthFailTokenInvalid,
			"token subject is not a known user"), nil
	}
	// The account may have been dropped and created again, revoked or disabled between the check
	// above and this read: the token is minted from this record, so it must still stand for it
	// (M2b review F-1).
	if err := authMgr.claimsHoldFor(claims, record); err != nil {
		logger.Warn("token auth failed", "username", claims.Subject, "error", err)
		return authFailFrame(frame, transport.AuthFailTokenInvalid,
			"token is no longer valid; authenticate again with a password"), nil
	}
	record.standing = standing
	minted := authMgr.MintClaims(record, &claims, time.Now())
	return finishAuth(frame, authMgr, s, sess, record, minted, true)
}

// finishAuth mints the response token, records the session identity and
// returns OpAuthOK with both expiry numbers and the auth_token capability.
// The scoped grant set is rebuilt from the record and frozen on the session
// here, because the token claims are flat and cannot carry the pairing.
func finishAuth(frame *transport.Frame, authMgr *AuthManager, s *Server,
	sess *transport.Session, record *UserRecord, claims TokenClaims, resumed bool) (*transport.Frame, error) {

	token, err := authMgr.GenerateToken(claims)
	if err != nil {
		return authFailFrame(frame, transport.AuthFailInternal, "could not mint token: "+err.Error()), nil
	}
	if sess != nil {
		sess.SetIdentity(transport.Identity{
			Username:       claims.Subject,
			Roles:          claims.Roles,
			Databases:      claims.Databases,
			Grants:         sessionGrants(record, claims.Roles, claims.Databases),
			TokenExpiresAt: time.Unix(claims.ExpiresAt, 0),
			Resumed:        resumed,
			Standing:       record.standing,
			Incarnation:    claims.Incarnation,
		})
	}

	resp := AuthResponse{
		Success:         true,
		Token:           token,
		TokenExpiresAt:  claims.ExpiresAt,
		TokenDeadline:   claims.Deadline,
		ProtocolVersion: uint32(transport.ProtocolMajor),
		ServerVersion:   Version,
		Capabilities:    serverCapabilities(),
		// The client that derives a new credential applies the minimum,
		// because this server never sees the plaintext (BUG-0493).
		PasswordMinLength: authMgr.passwordMinLength,
	}
	payload, err := bson.Marshal(resp)
	if err != nil {
		return authFailFrame(frame, transport.AuthFailInternal,
			fmt.Sprintf("could not encode the AUTH_OK response: %v", err)), nil
	}
	method := "password"
	if resumed {
		method = "token"
	}
	s.audit.Log(AuditEntry{Event: EventAuthLogin, User: claims.Subject, Result: "ok", AuthMethod: method})
	return transport.NewFrameWithID(transport.OpAuthOK, frame.RequestID, payload), nil
}

// remoteAddr names a session's peer for logs, tolerating a nil session (the
// direct handler calls in tests).
func remoteAddr(sess *transport.Session) string {
	if sess == nil {
		return ""
	}
	return sess.RemoteAddr
}

// authFailFrame builds an OpAuthFail with an explicit non-zero reason. Every
// failure path uses it so `Reason` is never left at the zero value.
func authFailFrame(frame *transport.Frame, reason transport.AuthFailReason, message string) *transport.Frame {
	payload, err := bson.Marshal(&AuthFailResponse{Reason: uint8(reason), Message: message})
	if err != nil {
		return errorFrame(frame, fmt.Errorf("encoding the AUTH_FAIL response: %w", err))
	}
	return transport.NewFrameWithID(transport.OpAuthFail, frame.RequestID, payload)
}

// uncheckedAuthFail builds an AUTH_FAIL for a refusal made before any
// credential was looked at, and marks it so the per-IP ledger does not count
// it as a failed login. The mark is not on the wire.
func uncheckedAuthFail(frame *transport.Frame, reason transport.AuthFailReason, message string) *transport.Frame {
	refusal := authFailFrame(frame, reason, message)
	refusal.MarkCredentialUnchecked()
	return refusal
}

// tokenFailureMessage returns an actionable server message for a token
// failure (AGENTS.md rule 3).
func tokenFailureMessage(reason transport.AuthFailReason) string {
	switch reason {
	case transport.AuthFailTokenExpired:
		return "AUTH token expired; authenticate again with a password"
	default:
		return "AUTH token is invalid; authenticate again with a password"
	}
}

// handleScan processes a bounded bucket scan request. It is a single
// request/response pair like handlePut, so the response carries OpScan itself.
func handleScan(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req ScanRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal scan request: %w", err))
	}
	resp, err := s.ScanCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpScan, resp)
}

// handleGet processes a hash store GET request.
func handleGet(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req FindRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal get request: %w", err))
	}
	resp, err := s.Find(req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpGetResponse, resp)
}

// handlePut processes a hash store PUT request.
func handlePut(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req InsertRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal put request: %w", err))
	}
	req.verb = verbInsert
	resp, err := s.InsertCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpPut, resp)
}

// handleUpsert processes a hash or btree UPSERT: the document replaces the
// scalar fields of the one stored under its _id_ and the document's stores are
// kept. The answer is UPSERT_OK.
func handleUpsert(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req InsertRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal upsert request: %w", err))
	}
	req.verb = verbUpsert
	resp, err := s.InsertCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpUpsertOK, resp)
}

// handleDelete processes a hash store DELETE request.
func handleDelete(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req DeleteRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal delete request: %w", err))
	}
	resp, err := s.DeleteCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpDelete, resp)
}

// handleExists processes a hash store EXISTS request.
func handleExists(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req FindRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal exists request: %w", err))
	}
	exists, err := s.Exists(req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpExistsResponse, map[string]bool{"exists": exists})
}

// handleBatchPut processes a batch insert request.
func handleBatchPut(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req BatchInsertRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal batch put request: %w", err))
	}
	req.verb = verbInsert
	resp, err := s.BatchInsertCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpBatchPutOK, resp)
}

// handleBatchUpsert processes a BATCH_UPSERT: each document is an UPSERT of its
// own _id_. The answer is BATCH_UPSERT_OK.
func handleBatchUpsert(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req BatchInsertRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal batch upsert request: %w", err))
	}
	req.verb = verbUpsert
	resp, err := s.BatchInsertCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpBatchUpsertOK, resp)
}

// handleFIFOPush processes a FIFO push request.
func handleFIFOPush(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req PushRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal FIFO push request: %w", err))
	}
	req.verb = verbInsert
	resp, err := s.PushCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpFIFOPush, resp)
}

// handleFIFOPop processes a FIFO pop request.
func handleFIFOPop(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req PopRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal FIFO pop request: %w", err))
	}
	resp, err := s.PopCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpFIFOPopResponse, resp)
}

// handleFIFOPeek processes a FIFO peek request.
func handleFIFOPeek(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req PeekRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal FIFO peek request: %w", err))
	}
	resp, err := s.Peek(req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpFIFOPeek, resp)
}

// handleQueueClaim processes a claim of a fifo, lifo or heap bucket.
func handleQueueClaim(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req ClaimRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal queue claim request: %w", err))
	}
	resp, err := s.ClaimCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpQueueClaim, resp)
}

// handleQueueAck processes an ack of a lease.
func handleQueueAck(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req LeaseRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal queue ack request: %w", err))
	}
	resp, err := s.AckCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpQueueAck, resp)
}

// handleQueueRelease processes a release of a lease.
func handleQueueRelease(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req LeaseRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal queue release request: %w", err))
	}
	resp, err := s.ReleaseCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpQueueRelease, resp)
}

// handleQueueExtend processes an extend of a lease.
func handleQueueExtend(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req LeaseRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal queue extend request: %w", err))
	}
	resp, err := s.ExtendCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpQueueExtend, resp)
}

// handleDeadList processes a list of a bucket's dead entries.
func handleDeadList(_ context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req DeadListRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal dead list request: %w", err))
	}
	resp, err := s.DeadList(req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpDeadList, resp)
}

// handleDeadRequeue processes a requeue of dead entries.
func handleDeadRequeue(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req DeadMoveRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal dead requeue request: %w", err))
	}
	resp, err := s.DeadRequeueCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpDeadRequeue, resp)
}

// handleDeadDiscard processes a discard of dead entries.
func handleDeadDiscard(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req DeadMoveRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal dead discard request: %w", err))
	}
	resp, err := s.DeadDiscardCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpDeadDiscard, resp)
}

// handleDeadRestore processes the write of one dead entry from a dump. A client
// frame never gets here: NewHandler refuses it.
func handleDeadRestore(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req DeadRestoreRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal dead restore request: %w", err))
	}
	resp, err := s.DeadRestoreCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpDeadRestore, resp)
}

// handleLIFOPush processes a LIFO push request.
func handleLIFOPush(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req PushRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal LIFO push request: %w", err))
	}
	req.verb = verbInsert
	resp, err := s.PushCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpLIFOPush, resp)
}

// handleLIFOPop processes a LIFO pop request.
func handleLIFOPop(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req PopRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal LIFO pop request: %w", err))
	}
	resp, err := s.PopCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpLIFOPopResponse, resp)
}

// handleLIFOPeek processes a LIFO peek request.
func handleLIFOPeek(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req PeekRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal LIFO peek request: %w", err))
	}
	resp, err := s.Peek(req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpLIFOPeek, resp)
}

// handleBTreeInsert processes a B+tree insert request.
func handleBTreeInsert(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req InsertRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal btree insert request: %w", err))
	}
	req.verb = verbInsert
	resp, err := s.InsertCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpBTreeInsert, resp)
}

// handleBTreeLookup processes a B+tree lookup request.
func handleBTreeLookup(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req FindRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal btree lookup request: %w", err))
	}
	resp, err := s.Find(req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpBTreeResponse, resp)
}

// handleBTreeRange processes a B+tree range scan request.
func handleBTreeRange(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req BTreeRangeRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal btree range request: %w", err))
	}
	resp, err := s.RangeQueryCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpBTreeResponse, resp)
}

// handleSearch processes a secondary-index query.
func handleSearch(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req SearchRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal search request: %w", err))
	}
	resp, err := s.SearchCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpSearch, resp)
}

// handleHeapPush processes a heap push request.
func handleHeapPush(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req PushRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal heap push request: %w", err))
	}
	req.verb = verbInsert
	resp, err := s.PushCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpHeapPush, resp)
}

// handleHeapPop processes a heap pop request.
func handleHeapPop(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req PopRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal heap pop request: %w", err))
	}
	resp, err := s.PopCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpHeapPopResponse, resp)
}

// handleCreateDB processes a create database request.
func handleCreateDB(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req CreateDatabaseRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal create db request: %w", err))
	}
	resp, err := s.createDatabaseCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	s.audit.Log(AuditEntry{Event: EventSchemaCreate, Target: "database:" + req.Name, Result: "ok"})
	return respond(frame, transport.OpCreateDB, resp)
}

// handleCreateBucket processes a create bucket request.
func handleCreateBucket(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req CreateBucketRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal create bucket request: %w", err))
	}
	resp, err := s.createBucketLocked(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	s.audit.Log(AuditEntry{Event: EventSchemaCreate, Target: "bucket:" + req.Database + "." + req.Name, Result: "ok"})
	return respond(frame, transport.OpCreateBucket, resp)
}

// handleHealth processes a health check request. It exposes the same
// health payload the gRPC HealthCheck RPC returned.
func handleHealth(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	resp, err := s.HealthCheck()
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpGetResponse, resp)
}

// handleListNodes processes a cluster node listing request. Nodes are
// ordered by node ID so the response is deterministic. An empty payload asks
// for the nodes alone: a BSON decode of nothing fails, and the clients that
// send nothing are the ones that never asked for more.
func handleListNodes(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req NodeListRequest
	if len(frame.Payload) > 0 {
		if err := bson.Unmarshal(frame.Payload, &req); err != nil {
			return errorResponse(frame, fmt.Errorf("decode list nodes request: %w", err))
		}
	}
	known := s.KnownNodes()
	nodes := make([]*NodeInfo, 0, len(known))
	for _, n := range known {
		nodes = append(nodes, &n)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })
	if s.cluster.Enabled() {
		now := time.Now()
		for _, n := range nodes {
			if s.cluster.instances.liveDuplicate(n.NodeID, now) {
				n.Duplicate = duplicateNodeMark
			}
		}
	}

	resp := &NodeListResponse{Nodes: nodes}
	// The count is the epoch record's, not len(nodes): the table is a cache of
	// who has gossiped, and nothing keeps it equal to the member set.
	if s.cluster == nil {
		resp.Standalone = true
	} else {
		// One read, so the count and the epoch come from the same view.
		epoch, members := s.cluster.epochMembers()
		resp.MemberCount, resp.Epoch = len(members), epoch
	}
	if s.cluster.Enabled() {
		resp.SchemaChanges = s.cluster.schemaChangesReport()
	}
	if req.History {
		resp.HistoryReported = true
		if s.cluster != nil {
			resp.MembershipHistory = s.cluster.historyAbove(0)
		}
	}
	return respond(frame, transport.OpGetResponse, resp)
}

// authMethodOf names the credential an AUTH frame carries, for the audit log.
func authMethodOf(req AuthRequest) string {
	if req.Token != "" {
		return "token"
	}
	return "password"
}

// refuseClientAdmitted is E-12: a client request carrying the admitted flag
// (spec §5.6). It covers every client opcode whose payload decodes into
// InsertRequest — OpPut (handlePut), OpUpsert (handleUpsert) and
// OpBTreeInsert (handleBTreeInsert) —
// because each reaches InsertCtx, which skips the name scan when the flag is
// set, and OpUpdate, whose admitted flag is a spread router's own write of its
// born set and skips the typed-field check (UpdateRequest.Admitted). The flag is read with the handler's own struct codec, never by an
// exact key: the codec falls back to a case-insensitive match, so "Admitted"
// sets the field as well as "admitted". The decode skips data's value by its
// length. A payload that does not decode is left to the handler to refuse.
func refuseClientAdmitted(frame *transport.Frame) error {
	if frame.Opcode != transport.OpPut && frame.Opcode != transport.OpUpsert && frame.Opcode != transport.OpBTreeInsert && frame.Opcode != transport.OpUpdate {
		return nil
	}
	var flag struct {
		Admitted bool `bson:"admitted"`
	}
	if err := bson.Unmarshal(frame.Payload, &flag); err != nil || !flag.Admitted {
		return nil
	}
	return &sentencedError{
		message: `The field "admitted" is internal to hoardDB's own repair path and cannot be set by a client. Remove it from the request.`,
		cause:   ErrAdmittedSetByClient,
	}
}

// refuseClientRepairBound refuses a client delete, put or btree insert that
// carries repair_bound, the bound only a consistent get's own repairs set: a
// client that set it could make a delete skip a document, or a write be
// dropped while reported a success. It is read with the handler's struct
// codec, as refuseClientAdmitted reads admitted. A node's repair is not a
// client frame: it reaches the owner through the cluster's write path.
func refuseClientRepairBound(frame *transport.Frame) error {
	if frame.Opcode != transport.OpDelete && frame.Opcode != transport.OpPut && frame.Opcode != transport.OpUpsert && frame.Opcode != transport.OpBTreeInsert {
		return nil
	}
	var bound struct {
		RepairBound uint64 `bson:"repair_bound"`
	}
	if err := bson.Unmarshal(frame.Payload, &bound); err != nil || bound.RepairBound == 0 {
		return nil
	}
	return &sentencedError{
		message: `The field "repair_bound" is internal to hoardDB's own repair path and cannot be set by a client. Remove it from the request.`,
		cause:   ErrRepairBoundSetByClient,
	}
}
