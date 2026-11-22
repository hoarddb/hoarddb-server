package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// This file implements spec 5.3: the master writes users.json locally (the
// commit point), then pushes the config to every member and collects acks.
// Every node must be able to authenticate a login locally, so the config goes
// to all members rather than to rf replicas.

// AuthConfigPushRequest is one auth-config push. A request with no Config is
// a version probe: the member either acks (it is already at the pushed
// version, the common case) or asks for the document. That gate is what keeps
// a converged cluster's push a version comparison and an ack instead of a
// whole-file transfer and fsync on every node (spec 5.3.2).
type AuthConfigPushRequest struct {
	Token   string `bson:"token"`
	Term    uint64 `bson:"term"`
	Version uint64 `bson:"version"`
	Master  string `bson:"master"`
	// Config is the usersConfig document as JSON, present only when the
	// member asked for it.
	Config []byte `bson:"config,omitempty"`
}

// AuthConfigPushResponse is the member's answer, carried on the frozen
// OpAuthConfigAck opcode. Ack means the member holds the pushed version (it
// applied it or was already there). NeedConfig asks the master for the
// document.
type AuthConfigPushResponse struct {
	Ack        bool   `bson:"ack"`
	NeedConfig bool   `bson:"need_config,omitempty"`
	Term       uint64 `bson:"term"`
	Version    uint64 `bson:"version"`
	Error      string `bson:"error,omitempty"`
}

// authConfigAck builds the member's answer on the OpAuthConfigAck opcode.
func authConfigAck(frame *transport.Frame, resp AuthConfigPushResponse) (*transport.Frame, error) {
	return respond(frame, transport.OpAuthConfigAck, resp)
}

// handleAuthConfigPush applies a config pushed by the master. It validates
// the document before touching anything, refuses a term older than this
// node's membership view, and applies only a newer (term, version). A
// logically-wrong config is refused with the current config kept and the node
// still serving, which is what stops one bad config from bricking every
// node's startup (spec 5.1, 5.3.2).
func handleAuthConfigPush(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	c := s.cluster
	if c == nil {
		return authConfigAck(frame, AuthConfigPushResponse{Error: "replication is not enabled on this node"})
	}
	var req AuthConfigPushRequest
	noteRequestDecode("handleAuthConfigPush", frame.Opcode, frame.Payload)
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return authConfigAck(frame, AuthConfigPushResponse{Error: "decode auth config push: " + err.Error()})
	}
	if !c.ValidateToken(req.Token) {
		c.logger.Warn("auth config push rejected: invalid cluster token", "master", req.Master)
		return authConfigAck(frame, AuthConfigPushResponse{Error: "invalid cluster token"})
	}
	if epoch := c.Epoch(); req.Term < epoch {
		return authConfigAck(frame, AuthConfigPushResponse{Error: fmt.Sprintf(
			"auth config term %d is older than this node's membership epoch %d; refusing", req.Term, epoch)})
	}

	// Version gate: already at or ahead of the pushed version. The pushed
	// config is what has to be newer than ours, not the other way round.
	localTerm, localVersion := s.auth.ConfigTermVersion()
	pushed := &usersConfig{Term: req.Term, Version: req.Version}
	if !pushed.NewerThan(localTerm, localVersion) {
		s.auth.TouchConfigSync()
		return authConfigAck(frame, AuthConfigPushResponse{Ack: true, Term: req.Term, Version: req.Version})
	}

	if len(req.Config) == 0 {
		term, version := s.auth.ConfigTermVersion()
		return authConfigAck(frame, AuthConfigPushResponse{NeedConfig: true, Term: term, Version: version})
	}

	var incoming usersConfig
	if err := json.Unmarshal(req.Config, &incoming); err != nil {
		return authConfigAck(frame, AuthConfigPushResponse{Error: "decode pushed auth config: " + err.Error()})
	}
	revoked, err := s.auth.applyConfigRevoking(&incoming)
	if err != nil {
		term, version := s.auth.ConfigTermVersion()
		c.logger.Warn("refusing pushed auth config; keeping the current one", "error", err)
		return authConfigAck(frame, AuthConfigPushResponse{Error: err.Error(), Term: term, Version: version})
	}
	// The master ends its own sessions of these accounts; every other member
	// ends its own here, as it applies the change.
	for _, username := range revoked {
		if closed := s.closeUserSessions(username); closed > 0 {
			s.logger.Info("closed sessions for a user whose access was revoked", "username", username, "closed", closed)
		}
	}
	return authConfigAck(frame, AuthConfigPushResponse{Ack: true, Term: incoming.Term, Version: incoming.Version})
}

// pushAuthConfigTo sends a version probe to one member and, if it is behind,
// the full document. It returns the member's final answer.
func (c *Cluster) pushAuthConfigTo(ctx context.Context, member ring.Node, req AuthConfigPushRequest) (AuthConfigPushResponse, error) {
	p := c.peerFor(member)
	cli, err := p.dial(ctx)
	if err != nil {
		return AuthConfigPushResponse{}, err
	}
	probe := req
	probe.Config = nil
	resp, err := c.sendAuthConfigPush(ctx, cli, probe)
	if err != nil {
		p.dropClient()
		return AuthConfigPushResponse{}, err
	}
	if !resp.NeedConfig {
		return resp, nil
	}
	resp, err = c.sendAuthConfigPush(ctx, cli, req)
	if err != nil {
		p.dropClient()
		return AuthConfigPushResponse{}, err
	}
	return resp, nil
}

// sendAuthConfigPush sends one push frame and decodes the ack.
func (c *Cluster) sendAuthConfigPush(ctx context.Context, cli peerConn, req AuthConfigPushRequest) (AuthConfigPushResponse, error) {
	payload, err := bson.Marshal(req)
	if err != nil {
		return AuthConfigPushResponse{}, fmt.Errorf("marshal auth config push: %w", err)
	}
	frame, err := cli.SendRequest(ctx, transport.LaneControl, transport.OpAuthConfigPush, payload)
	if err != nil {
		return AuthConfigPushResponse{}, err
	}
	var resp AuthConfigPushResponse
	if len(frame.Payload) > 0 {
		if err := decodePeerAnswer(frame.Payload, &resp); err != nil {
			return AuthConfigPushResponse{}, fmt.Errorf("decode auth config ack: %w", err)
		}
	}
	if resp.Error != "" {
		return resp, fmt.Errorf("member rejected auth config: %s", resp.Error)
	}
	return resp, nil
}

// replicateAuthConfig pushes the current config to every other member and
// updates the lag metrics. It never blocks a caller on a slow member for more
// than the probe timeout, and a failed member is retried by the loop.
func (s *Server) replicateAuthConfig(ctx context.Context) {
	c := s.cluster
	if c == nil || !c.Enabled() {
		return
	}
	cfg := s.auth.SnapshotConfig()
	if cfg == nil {
		return
	}
	// The term is the membership epoch that elected this master, not whatever
	// the record was last written with: a follower refuses a config whose term
	// is older than its own membership view.
	cfg.Term = c.Epoch()
	body, err := json.Marshal(cfg)
	if err != nil {
		s.logger.Warn("marshal auth config for push", "error", err)
		return
	}
	req := AuthConfigPushRequest{
		Token:   c.localToken(),
		Term:    cfg.Term,
		Version: cfg.Version,
		Master:  c.self.ID,
		Config:  body,
	}

	diverged := 0
	maxLag := uint64(0)
	oldestUnacked := time.Time{}
	for _, member := range c.Members() {
		if member.ID == c.self.ID {
			continue
		}
		started := time.Now()
		resp, err := c.pushAuthConfigTo(ctx, member, req)
		if err != nil {
			diverged++
			if oldestUnacked.IsZero() || started.Before(oldestUnacked) {
				oldestUnacked = started
			}
			s.logger.Debug("auth config push failed", "peer", member.ID, "error", err)
			continue
		}
		if !resp.Ack {
			diverged++
			if oldestUnacked.IsZero() || started.Before(oldestUnacked) {
				oldestUnacked = started
			}
		}
		if resp.Version < cfg.Version {
			if lag := cfg.Version - resp.Version; lag > maxLag {
				maxLag = lag
			}
		}
	}

	authConfigDivergedMembers.Set(float64(diverged))
	authConfigLag.Set(float64(maxLag))
	if oldestUnacked.IsZero() {
		authConfigOldestUnackedSeconds.Set(0)
	} else {
		authConfigOldestUnackedSeconds.Set(time.Since(oldestUnacked).Seconds())
	}
}

// ScheduleAuthConfigPush wakes the replication loop after a local mutation so
// the change propagates immediately rather than waiting for the next tick. It
// never blocks: a full channel means a push is already pending.
func (c *Cluster) ScheduleAuthConfigPush() {
	if c == nil || c.authPushCh == nil {
		return
	}
	select {
	case c.authPushCh <- struct{}{}:
	default:
	}
}

// authConfigLoop periodically pushes the config while this node is the
// master, and retries unacked members on the retry interval (spec 5.3.3). It
// runs on every node so a mastership change is picked up without a restart.
func (c *Cluster) authConfigLoop() {
	defer c.storeWG.Done()
	retry := time.Duration(c.s.config.Auth.AuthMasterRetrySeconds) * time.Second
	if retry <= 0 {
		retry = 5 * time.Second
	}
	ticker := time.NewTicker(retry)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
		case <-c.authPushCh:
		}
		if !c.s.isAuthMaster() {
			c.s.auth.SetFollower(true)
			continue
		}
		c.s.auth.SetFollower(false)
		c.s.replicateAuthConfig(c.ctx)
	}
}

// authConfigUnavailableReason returns a retryable message when this node must
// not serve a user login right now: it has no config at all (a clustered node
// waiting for its first push), or it is a follower that has not synced within
// the staleness bound and may be missing a revocation. It is empty when the
// node may serve. It takes the manager explicitly because handleAuth receives
// it separately from the Server.
func (s *Server) authConfigUnavailableReason(authMgr *AuthManager) string {
	if authMgr.ConfigBlocked() {
		return "this node has no auth config yet; it is waiting for the cluster master to send it — retry shortly"
	}
	if s.authConfigStale(authMgr) {
		return "this node is more than auth_config_max_staleness_seconds behind the cluster master and is refusing password logins and token refreshes; retry once it resyncs"
	}
	return ""
}

// scheduleAuthConfigPush wakes the replication loop after a successful local
// mutation. It is a no-op on a standalone node.
func (s *Server) scheduleAuthConfigPush() {
	if s.cluster != nil {
		s.cluster.ScheduleAuthConfigPush()
	}
}

// isAuthMaster reports whether this node owns the reserved auth key.
func (s *Server) isAuthMaster() bool {
	master, err := s.authMaster()
	if err != nil {
		return false
	}
	return master.ID == s.authSelfID()
}

// authConfigStale reports whether a follower has gone longer than the
// configured bound without syncing. The master is authoritative for its own
// config and is never stale; a standalone node is never a follower.
func (s *Server) authConfigStale(authMgr *AuthManager) bool {
	if !authMgr.IsFollower() {
		return false
	}
	maxAge := time.Duration(s.config.Auth.AuthConfigMaxStalenessSeconds) * time.Second
	if maxAge <= 0 {
		return false
	}
	return authMgr.ConfigStale(maxAge)
}

// SnapshotConfig returns a copy of the current config for a push.
func (am *AuthManager) SnapshotConfig() *usersConfig {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.configLocked()
}

// NewerThan reports whether the current config is newer than (term, version).
func (am *AuthManager) NewerThan(term, version uint64) bool {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return (&usersConfig{Term: am.term, Version: am.version}).NewerThan(term, version)
}

// ConfigTermVersion returns the current fence.
func (am *AuthManager) ConfigTermVersion() (uint64, uint64) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.term, am.version
}

// TouchConfigSync marks the config as freshly synced without changing it. The
// master's periodic version probe keeps a healthy follower's clock fresh; a
// follower that stops hearing from the master goes stale and refuses logins.
func (am *AuthManager) TouchConfigSync() {
	am.mu.Lock()
	am.lastSync = time.Now()
	am.mu.Unlock()
}

// ApplyConfig installs a config pushed by the master. It validates first and
// refuses a logically-wrong document, keeping the current config. Caller must
// have already checked the term against the membership epoch. Applying writes
// the file (temp + rename) before swapping the in-memory snapshot.
func (am *AuthManager) ApplyConfig(cfg *usersConfig) error {
	_, err := am.applyConfigRevoking(cfg)
	return err
}

// applyConfigRevoking is ApplyConfig returning the accounts the config
// dropped, disabled or gave new credentials, whose sessions on this node the
// caller closes. Their logins are already revoked when it returns.
func (am *AuthManager) applyConfigRevoking(cfg *usersConfig) ([]string, error) {
	if err := am.refuseInCluster("apply a pushed users config"); err != nil {
		return nil, err
	}
	if err := cfg.errRetiredRoles(); err != nil {
		return nil, errRetiredRolesInPush(err)
	}
	if err := cfg.validateWith(am.argon2); err != nil {
		return nil, fmt.Errorf("refusing pushed auth config: %w", err)
	}
	cfg.warnUnusableSalts(am.logger, "pushed config")
	am.mu.Lock()
	defer am.mu.Unlock()
	if !cfg.NewerThan(am.term, am.version) {
		am.lastSync = time.Now()
		am.blocked = false
		return nil, nil
	}
	if am.usersPath != "" {
		if err := cfg.Save(am.usersPath, usersSealing{Keyring: am.keyring, NodeMaster: am.nodeMaster, Mode: am.mode, Argon2: am.argon2}); err != nil {
			return nil, fmt.Errorf("persist pushed auth config: %w", err)
		}
	}
	_, revoked := am.installLocked(cfg, nil)
	am.loaded = true
	am.lastSync = time.Now()
	am.blocked = false
	am.updateMetricsLocked()
	return revoked, nil
}

// ConfigBlocked reports whether this clustered node is still waiting for its
// first config. Such a node refuses AUTH rather than serving logins from an
// empty user list (spec 5.3.4).
func (am *AuthManager) ConfigBlocked() bool {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.blocked
}

// SetConfigBlocked marks a config-less clustered node as blocking AUTH until
// a push arrives.
func (am *AuthManager) SetConfigBlocked(blocked bool) {
	am.mu.Lock()
	am.blocked = blocked
	am.mu.Unlock()
}

// IsFollower reports whether this node trails a master for auth config.
func (am *AuthManager) IsFollower() bool {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.follower
}

// SetFollower records whether this node trails another for auth config.
func (am *AuthManager) SetFollower(follower bool) {
	am.mu.Lock()
	am.follower = follower
	am.mu.Unlock()
}

// ConfigStale reports whether the last config sync is older than maxAge.
func (am *AuthManager) ConfigStale(maxAge time.Duration) bool {
	am.mu.RLock()
	defer am.mu.RUnlock()
	if am.lastSync.IsZero() {
		return true
	}
	return time.Since(am.lastSync) > maxAge
}

// updateMetricsLocked publishes the config version gauge. Caller holds am.mu.
func (am *AuthManager) updateMetricsLocked() {
	authConfigVersion.Set(float64(am.version))
}
