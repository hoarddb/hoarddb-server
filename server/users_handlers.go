package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/hoarddb/hoarddb-server/namepolicy"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// authReservedKey is the ring key whose owner is the cluster master for user
// changes (spec 5.2). It is deliberately not a legal database.bucket pair:
// create database "_system" is refused so a user cannot make the key point at
// their own bucket. It is placed by its bare name (reservedPlacement), not by
// a user bucket's (database, bucket) key, so the move to placement by database left the master
// where it was.
const authReservedKey = "_system.auth"

// authReservedDatabase is the database name that must never be created.
const authReservedDatabase = namepolicy.ReservedDatabase

// authRequestDedupWindow bounds how long a client mutation request_id is
// remembered, so a retry after a timeout is a no-op but the table cannot grow
// without limit (spec 6.1).
const authRequestDedupWindow = 10 * time.Minute

// ErrNotMaster is returned when a user mutation arrives on a node that does
// not own the reserved auth key (spec 5.4).
var ErrNotMaster = errors.New("user changes are served by the cluster master")

// ErrNoQuorum is returned when the master cannot confirm a strict majority of
// the current term's members are reachable. It is retryable, and it fails
// closed: accepting a config a healed partition would discard is worse than
// refusing the command (spec 5.2).
var ErrNoQuorum = errors.New("no quorum")

// IsReservedDatabaseName reports whether name is reserved for internal use.
// The check lives here so dump/restore can call the same function rather than
// growing a second definition of a legal name (user-store-build.md 15, item 5).
func IsReservedDatabaseName(name string) bool {
	return namepolicy.IsReservedDatabase(name)
}

// selfNode is this node's ring identity, used when the cluster is disabled
// and this node is trivially its own master.
func (s *Server) selfNode() ring.Node {
	return ring.Node{ID: s.config.Server.NodeID, Addr: s.config.Server.Listen}
}

// authSelfID is this node's identity as the auth master check must see it. On
// a cluster that is the ring's resolved self, which is the id (or address) the
// seed list names this node under; the configured node id defaults to the
// host's name and would never equal a ring id (BUG-0592).
func (s *Server) authSelfID() string {
	if s.cluster == nil || !s.cluster.Enabled() {
		return s.config.Server.NodeID
	}
	return s.cluster.self.ID
}

// recordedMasterID is the id the users file names this node by when it is the
// master: the cluster's resolved self whenever a cluster exists, even a ring of
// one member. authSelfID cannot serve, because it must agree with authMaster,
// which answers with the configured id while the ring has one member. Cluster
// self is fixed when the cluster is built, so this does not change when the
// ring grows; ringSelfID resolves the same value offline for reset-password.
func (s *Server) recordedMasterID() string {
	if s.cluster == nil {
		return s.config.Server.NodeID
	}
	return s.cluster.self.ID
}

// authMaster returns the node that owns the reserved auth key: itself on a
// standalone node, the ring owner otherwise.
func (s *Server) authMaster() (ring.Node, error) {
	if s.cluster == nil || !s.cluster.Enabled() {
		return s.selfNode(), nil
	}
	return s.cluster.Owner(reservedPlacement(authReservedKey))
}

// requireMaster refuses a mutation on a node that is not the auth master,
// naming the master and the fix (spec 5.4), and then confirms a quorum on the
// master before accepting (spec 5.2).
func (s *Server) requireMaster(ctx context.Context) error {
	master, err := s.authMaster()
	if err != nil {
		return fmt.Errorf("determine the auth master: %w", err)
	}
	if selfID := s.authSelfID(); master.ID != selfID {
		duplicated := ""
		if s.cluster != nil && s.cluster.instances.liveDuplicate(master.ID, time.Now()) {
			duplicated = fmt.Sprintf("\n       two processes are running as %s, and neither takes user changes until only one runs; 'status' says which", master.ID)
		}
		// The address is where a client connects (the master's tcp.listen),
		// not the master's ring address, which no client can reach (BUG-0709).
		address, note := s.masterClientAddress(master)
		return fmt.Errorf("%w\n       this node: %s (not the master)\n       %s%s",
			ErrNotMaster, selfID, notMasterDirections(master, address, note), duplicated)
	}
	if s.cluster != nil && s.cluster.Enabled() {
		if err := s.cluster.duplicateSelfRefusal(); err != nil {
			return err
		}
	}
	if s.cluster != nil && s.cluster.Enabled() && s.cluster.bootstrapped.Load() {
		return s.cluster.unconfirmedMasterRefusal()
	}
	return s.requireQuorum(ctx)
}

// unconfirmedMasterRefusal refuses a user change on a node that places the
// auth master on itself by a member list no member has confirmed. A node
// started with another node's list places the master by a ring the cluster
// does not share, and would accept changes next to the real master
// (BUG-0598); the same holds for a founder until its fellow founders answer.
func (c *Cluster) unconfirmedMasterRefusal() error {
	message := "user changes are refused on this node until a cluster member confirms its member list: it comes from its seed settings " +
		"and no member has confirmed it yet. Run 'join " + c.joinAddress() + "' on a member, then retry. Nothing was changed."
	if cause := c.membershipRefusal(); cause != nil {
		message = fmt.Sprintf("user changes are refused on this node, because it cannot be confirmed as a cluster member: %v. Nothing was changed.", cause)
	} else if silent, founder := c.foundersNotAnswered(); founder {
		message = fmt.Sprintf("user changes are refused on this node until every founder in its seed list has answered it; %s has not answered yet. "+
			"Start it, or, if it is not coming back, run 'remove-node <node-id>' for it on a running founder. Nothing was changed.",
			strings.Join(silent, ", "))
	}
	return &sentencedError{message: message, cause: ErrNotYetAdded}
}

// requireQuorum confirms a strict majority of the current term's members are
// reachable before the master accepts a mutation. A standalone node is
// trivially its own majority (⌊1/2⌋+1 = 1). If the check cannot answer — no
// membership view, an unreachable member — it refuses, because a majority
// gate that fails open is worse than none (spec 5.2).
func (s *Server) requireQuorum(ctx context.Context) error {
	if s.cluster == nil || !s.cluster.Enabled() {
		return nil
	}
	reachable, needed, total := s.cluster.QuorumReachable(ctx)
	if reachable < needed {
		return fmt.Errorf("%w: user changes need a strict majority of the %d current cluster members (need %d), but only %d could be reached; this is retryable — a peer may be down or the membership may be changing, so retry once the members are reachable",
			ErrNoQuorum, total, needed, reachable)
	}
	return nil
}

// sessionUser returns the authenticated username behind a frame, or "" when
// there is none (the transport has already refused anonymous frames).
func sessionUser(ctx context.Context) string {
	if sess, ok := transport.SessionFromContext(ctx); ok {
		if identity, ok := sess.Identity(); ok {
			return identity.Username
		}
	}
	return ""
}

// authRequestAlreadyApplied reports whether this request_id was successfully
// applied inside the dedup window.
func (s *Server) authRequestAlreadyApplied(id string) bool {
	if id == "" {
		return false
	}
	s.authReqMu.Lock()
	defer s.authReqMu.Unlock()
	now := time.Now()
	for key, at := range s.authReqSeen {
		if now.Sub(at) > authRequestDedupWindow {
			delete(s.authReqSeen, key)
		}
	}
	_, ok := s.authReqSeen[id]
	return ok
}

// markAuthRequest records a successfully applied request_id.
func (s *Server) markAuthRequest(id string) {
	if id == "" {
		return
	}
	s.authReqMu.Lock()
	defer s.authReqMu.Unlock()
	s.authReqSeen[id] = time.Now()
}

// requestGrantor is the account behind a user-change frame, with its
// session's grants, for the ceiling the change is checked against. A call
// with no session is the server's own and has none.
func requestGrantor(ctx context.Context) *Grantor {
	sess, ok := transport.SessionFromContext(ctx)
	if !ok || sess == nil {
		return nil
	}
	identity, ok := sess.Identity()
	if !ok {
		return &Grantor{}
	}
	return &Grantor{Username: identity.Username, Grants: identity.Grants}
}

// grantsFromWire converts wire roles to the stored grant type.
func grantsFromWire(roles []UserRole) []Grant {
	grants := make([]Grant, 0, len(roles))
	for _, role := range roles {
		grants = append(grants, Grant(role))
	}
	return grants
}

// userMutationFrame marshals a mutation response.
func userMutationFrame(frame *transport.Frame, opcode transport.Opcode, resp UserMutationResponse) *transport.Frame {
	return respondFrame(frame, opcode, resp)
}

// malformedUserRequest is the refusal of a user-administration payload that does
// not decode. It carries ErrMalformedPayload so it answers INVALID_ARGUMENT: the
// client sent the bytes, and INTERNAL would tell it to report a server fault.
func malformedUserRequest(request string, cause error) error {
	return fmt.Errorf("%s request: %w (%v); nothing was changed. Build the request with the 'hoarddb' CLI or a current driver, which send the documented fields",
		request, ErrMalformedPayload, cause)
}

// handleUserCreate processes OpUserCreate. The frame carries a
// client-derived credential, never a plaintext password.
func handleUserCreate(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req UserCreateRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, malformedUserRequest("user create", err))
	}
	if err := s.requireMaster(ctx); err != nil {
		return errorResponse(frame, err)
	}
	if s.authRequestAlreadyApplied(req.RequestID) {
		return userMutationFrame(frame, transport.OpUserCreate, UserMutationResponse{
			Success: true, Message: "duplicate request ignored (already applied)", Version: s.auth.Version(),
		}), nil
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	material := CredentialMaterial{
		Algo:   req.Credential.Algo,
		Salt:   req.Credential.Salt,
		Hash:   req.Credential.Hash,
		Params: req.Credential.Params,
	}
	if err := s.auth.CreateUserAs(requestGrantor(ctx), req.Username, material, grantsFromWire(req.Roles), enabled); err != nil {
		return errorResponse(frame, err)
	}
	s.markAuthRequest(req.RequestID)
	s.scheduleAuthConfigPush()
	s.audit.Log(AuditEntry{Event: EventUserCreate, User: sessionUser(ctx), Target: "user:" + req.Username, Result: "ok"})
	return userMutationFrame(frame, transport.OpUserCreate, UserMutationResponse{
		Success: true, Message: fmt.Sprintf("user %q created", req.Username), Version: s.auth.Version(),
	}), nil
}

// handleUserAlter processes OpUserAlter.
func handleUserAlter(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req UserAlterRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, malformedUserRequest("user alter", err))
	}
	if err := s.requireMaster(ctx); err != nil {
		return errorResponse(frame, err)
	}
	if s.authRequestAlreadyApplied(req.RequestID) {
		return userMutationFrame(frame, transport.OpUserAlter, UserMutationResponse{
			Success: true, Message: "duplicate request ignored (already applied)", Version: s.auth.Version(),
		}), nil
	}

	// Every field is optional-with-presence (spec 12.4): absent means
	// unchanged, so a credentials-only alter must not touch the role set. The
	// changed list is computed once and serves both the audit Result and the
	// response echo the client checks (spec 6.1 item 3).
	changed := make([]string, 0, 3)
	spec := AlterSpec{Grantor: requestGrantor(ctx), Username: req.Username, Version: req.Version, Enabled: req.Enabled}
	if req.Roles != nil {
		r := grantsFromWire(*req.Roles)
		spec.Grants = &r
		changed = append(changed, "roles")
	}
	if req.Enabled != nil {
		changed = append(changed, "enabled")
	}
	if req.Credentials != nil {
		spec.Credential = &CredentialMaterial{
			Algo:   req.Credentials.Algo,
			Salt:   req.Credentials.Salt,
			Hash:   req.Credentials.Hash,
			Params: req.Credentials.Params,
		}
		changed = append(changed, "credentials")
	}
	version, err := s.auth.AlterUser(spec)
	if err != nil {
		return errorResponse(frame, err)
	}
	s.markAuthRequest(req.RequestID)
	s.scheduleAuthConfigPush()
	// A disable or a new password closes that user's live sessions here, and
	// every other member closes its own when the change reaches it. A role
	// change does not: it takes effect at the next AUTH/refresh (spec 8.8).
	if (req.Enabled != nil && !*req.Enabled) || req.Credentials != nil {
		if closed := s.closeUserSessions(req.Username); closed > 0 {
			s.logger.Info("closed sessions for a user whose access was revoked", "username", req.Username, "closed", closed)
		}
	}
	// The audit line names the changed fields, so a password reset is
	// distinguishable from a role edit (spec 12.4).
	result := "no changes"
	if len(changed) > 0 {
		result = "changed: " + strings.Join(changed, ",")
	}
	s.audit.Log(AuditEntry{Event: EventUserAlter, User: sessionUser(ctx), Target: "user:" + req.Username, Result: result})
	return userMutationFrame(frame, transport.OpUserAlter, UserMutationResponse{
		Success: true, Message: fmt.Sprintf("user %q altered", req.Username), Version: version, Changed: changed,
	}), nil
}

// handleUserDrop processes OpUserDrop.
func handleUserDrop(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req UserDropRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, malformedUserRequest("user drop", err))
	}
	if err := s.requireMaster(ctx); err != nil {
		return errorResponse(frame, err)
	}
	if s.authRequestAlreadyApplied(req.RequestID) {
		return userMutationFrame(frame, transport.OpUserDrop, UserMutationResponse{
			Success: true, Message: "duplicate request ignored (already applied)", Version: s.auth.Version(),
		}), nil
	}
	if err := s.auth.DropUserAs(requestGrantor(ctx), req.Username); err != nil {
		return errorResponse(frame, err)
	}
	s.markAuthRequest(req.RequestID)
	s.scheduleAuthConfigPush()
	// A drop closes that user's live sessions (spec 8.8); the close happens
	// after DropUser released the user-store lock.
	if closed := s.closeUserSessions(req.Username); closed > 0 {
		s.logger.Info("closed sessions for dropped user", "username", req.Username, "closed", closed)
	}
	s.audit.Log(AuditEntry{Event: EventUserDrop, User: sessionUser(ctx), Target: "user:" + req.Username, Result: "ok"})
	return userMutationFrame(frame, transport.OpUserDrop, UserMutationResponse{
		Success: true, Message: fmt.Sprintf("user %q dropped", req.Username), Version: s.auth.Version(),
	}), nil
}

// handleUserGrant processes OpUserGrant.
func handleUserGrant(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req UserGrantRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, malformedUserRequest("user grant", err))
	}
	if err := s.requireMaster(ctx); err != nil {
		return errorResponse(frame, err)
	}
	if s.authRequestAlreadyApplied(req.RequestID) {
		return userMutationFrame(frame, transport.OpUserGrant, UserMutationResponse{
			Success: true, Message: "duplicate request ignored (already applied)", Version: s.auth.Version(),
		}), nil
	}
	version, err := s.auth.GrantRoleAs(requestGrantor(ctx), req.Username, Grant{Role: req.Role.Role, DB: req.Role.DB})
	if err != nil {
		return errorResponse(frame, err)
	}
	s.markAuthRequest(req.RequestID)
	s.scheduleAuthConfigPush()
	s.audit.Log(AuditEntry{Event: EventUserAlter, User: sessionUser(ctx), Target: "user:" + req.Username, Result: "grant " + req.Role.Role + "@" + req.Role.DB})
	return userMutationFrame(frame, transport.OpUserGrant, UserMutationResponse{
		Success: true, Message: fmt.Sprintf("granted %s@%s to %q", req.Role.Role, req.Role.DB, req.Username), Version: version,
	}), nil
}

// handleUserRevoke processes OpUserRevoke.
func handleUserRevoke(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req UserRevokeRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, malformedUserRequest("user revoke", err))
	}
	if err := s.requireMaster(ctx); err != nil {
		return errorResponse(frame, err)
	}
	if s.authRequestAlreadyApplied(req.RequestID) {
		return userMutationFrame(frame, transport.OpUserRevoke, UserMutationResponse{
			Success: true, Message: "duplicate request ignored (already applied)", Version: s.auth.Version(),
		}), nil
	}
	version, err := s.auth.RevokeRoleAs(requestGrantor(ctx), req.Username, Grant{Role: req.Role.Role, DB: req.Role.DB})
	if err != nil {
		return errorResponse(frame, err)
	}
	s.markAuthRequest(req.RequestID)
	s.scheduleAuthConfigPush()
	s.audit.Log(AuditEntry{Event: EventUserAlter, User: sessionUser(ctx), Target: "user:" + req.Username, Result: "revoke " + req.Role.Role + "@" + req.Role.DB})
	return userMutationFrame(frame, transport.OpUserRevoke, UserMutationResponse{
		Success: true, Message: fmt.Sprintf("revoked %s@%s from %q", req.Role.Role, req.Role.DB, req.Username), Version: version,
	}), nil
}

// handleUserList processes OpUserList. Any authenticated node serves it.
func handleUserList(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	master, _ := s.authMaster()
	address, note := s.masterClientAddress(master)
	return respond(frame, transport.OpUserList, UserListResponse{
		Users:          s.auth.UserInfos(),
		Version:        s.auth.Version(),
		Master:         master.ID,
		MasterAddr:     address,
		MasterAddrNote: note,
	})
}

// handleUserGet processes OpUserGet. Any authenticated node serves it.
func handleUserGet(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req UserGetRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, malformedUserRequest("user get", err))
	}
	info, ok := s.auth.UserInfo(req.Username)
	if !ok {
		// An unknown user is a successful answer, as a miss is on every read
		// (errors.md section 2); the CLI reads found for its own sentence.
		return respond(frame, transport.OpUserGet, UserGetResponse{
			Found:   false,
			Version: s.auth.Version(),
			Message: fmt.Sprintf("user %q not found; %s", req.Username, userListHint),
		})
	}
	return respond(frame, transport.OpUserGet, UserGetResponse{Found: true, User: info, Version: s.auth.Version()})
}
