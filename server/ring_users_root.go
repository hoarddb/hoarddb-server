package server

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-server/authrules"
	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/clusterpepper"
)

// The root task (ring user store spec 6.5, step M2b.1): a cluster member started with a root
// password (HOARDDB_ROOT_PASSWORD, its file, or the first start's prompt) creates the cluster's
// first administrator through the cluster, once, whatever the order of starts:
//
//   - at start the password is turned into the credential a client derives (a fresh salt, inside
//     the window every member accepts) and forgotten; only the name and the derived bytes are kept;
//   - once the ring node is open, one goroutine waits until this node knows a leader and is a
//     committed data member, then reads the cluster's user table: when it is empty and names no
//     key, the task takes this node's bootstrap pepper (made and written durably first, spec 5.2),
//     peppers the client hash once and proposes USER_BOOTSTRAP under one request id, retrying with
//     backoff inside a proposal budget;
//   - a cluster that has users (or a key) ends the task with the warning of spec 6.2, and a table
//     that holds the task's own request (a lost reply, B14) ends it as its success;
//   - it never runs on a witness, which warns at start instead (F16), and it ends when the node
//     closes.
//
// Two nodes started with a root password both reach the proposal only when both are committed
// data members of a cluster with no users; the first USER_BOOTSTRAP to commit wins, the other is
// refused by the leader's dry run and its task, reading the table again, warns; its pepper is
// replaced by the cluster's (spec 5.4).
//
// Rule 11: per node at most one goroutine, which exits once the administrator exists or the
// cluster has users, and about 100 bytes of derived credential while it runs; nothing per bucket,
// store or user; no metric. Each round reads the user table without copying it and the member list
// (O(members)); a membership change costs nothing here.

const (
	// rootTaskFirstWait and rootTaskMaxWait bound the task's wait between rounds.
	rootTaskFirstWait = 200 * time.Millisecond
	rootTaskMaxWait   = 5 * time.Second
)

// clusterRoot is the first administrator a cluster member was given at start: its name, the
// setting the password came from, the credential derived from it, and the request id every
// attempt carries, so the cluster remembers the bootstrap as this node's. The password itself is
// not kept.
type clusterRoot struct {
	username   string
	setting    string
	credential *wire.RingUserCredential
	request    [cluster.UserRequestIDLen]byte
}

// newClusterRoot derives the credential of password as a client would, inside the window every
// member accepts (a node configured above it derives at its top, spec 4.2 B20).
func newClusterRoot(username, password, setting string, params Argon2Params) (*clusterRoot, error) {
	credential, err := ringCredential(password, clusterWindowParams(params))
	if err != nil {
		return nil, fmt.Errorf("derive the cluster's first administrator %q: %w", username, err)
	}
	root := &clusterRoot{username: username, setting: setting, credential: credential}
	if _, err := rand.Read(root.request[:]); err != nil {
		return nil, fmt.Errorf("derive the cluster's first administrator %q: draw a request id: %w", username, err)
	}
	return root, nil
}

// rootTaskNode is the cluster as the root task sees it: the ring node.
type rootTaskNode interface {
	Users() cluster.UserView
	FirstAdministratorReady() ([16]byte, bool)
	BootstrapPepper() (clusterpepper.Pepper, error)
	ProposeUserCommand(ctx context.Context, kind byte, body []byte) (cluster.Result, error)
}

// rootTask is one node's root task.
type rootTask struct {
	root      *clusterRoot
	node      rootTaskNode
	installed installWaiter
	budget    time.Duration
	logger    *slog.Logger
	operator  io.Writer
	// audit records the administrator's creation; nil writes nothing.
	audit func(AuditEntry)
	// waiting is the last reason a round could not finish, logged once per change.
	waiting string
}

func newRootTask(root *clusterRoot, node rootTaskNode, installed installWaiter, budget time.Duration, logger *slog.Logger, operator io.Writer) *rootTask {
	return &rootTask{root: root, node: node, installed: installed, budget: budget, logger: logger, operator: operator}
}

// run is the task: rounds until the administrator exists, the cluster has users, a decided
// refusal ends it, or ctx ends.
func (t *rootTask) run(ctx context.Context) {
	wait := rootTaskFirstWait
	for {
		if t.round(ctx) {
			return
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		wait = min(2*wait, rootTaskMaxWait)
	}
}

// round reads the cluster and, when it has no users, proposes the administrator once. It reports
// whether the task is over.
func (t *rootTask) round(ctx context.Context) bool {
	view := t.node.Users()
	if seq, ok := view.RequestApplied(t.root.request); ok {
		t.created(ctx, seq)
		return true
	}
	if len(view.Users) > 0 || view.PepperKID != "" {
		t.warn(t.root.setting + " was not used: this cluster already has users. Log in as an existing administrator, or unset it.")
		return true
	}
	clusterID, ready := t.node.FirstAdministratorReady()
	if !ready {
		t.note("waiting for a leader and for this node to be a committed member before creating the cluster's first administrator")
		return false
	}
	pepper, err := t.node.BootstrapPepper()
	if err != nil {
		// The state named a key between the read and here: the next round reads the users.
		t.note(err.Error())
		return false
	}
	body := cluster.UserBootstrapBody{Request: t.root.request, Name: t.root.username, CreatedMs: time.Now().UnixMilli(), Credential: cluster.UserCredential{
		Salt: t.root.credential.Salt, Hash: pepper.Key(clusterID).Hash(t.root.credential.Hash), Memory: t.root.credential.Memory,
		Iterations: t.root.credential.Iterations, Parallelism: t.root.credential.Parallelism, PepperKID: pepper.KID()}}.Encode()
	proposal, cancel := context.WithTimeout(ctx, t.budget)
	defer cancel()
	result, err := t.node.ProposeUserCommand(proposal, cluster.CommandUserBootstrap, body)
	if err == nil {
		t.created(ctx, result.StateSeq)
		return true
	}
	var refused *cluster.Error
	var undecided *wire.UndecidedError
	switch {
	case errors.As(err, &undecided):
		// It may have committed: the next round finds the request, or the table, or neither.
		t.note("the cluster's first administrator was sent, with no answer yet: " + err.Error())
		return false
	case errors.As(err, &refused) && refused.Code == cluster.CodeConflict:
		// Another bootstrap committed first; the next round reads it once this node applied it.
		t.note("another node created the cluster's first administrator first: " + refused.Message)
		return false
	case errors.As(err, &refused) && refused.Code != cluster.CodeUnavailable:
		t.logger.Error("the cluster's first administrator was refused and is not created; fix the cause and restart this node", "user", t.root.username, "error", err)
		return true
	default:
		t.note("no leader took the cluster's first administrator yet: " + err.Error())
		return false
	}
}

// created reports the administrator this task's bootstrap made, once this node installed it, so a
// login here works when the line is read.
func (t *rootTask) created(ctx context.Context, seq int64) {
	// Audited when the commit is known, not when this node shows it (M2a review F7).
	if t.audit != nil {
		t.audit(AuditEntry{Event: EventUserCreate, User: "(" + t.root.setting + ")", Target: "user:" + t.root.username,
			Result: fmt.Sprintf("ok: the cluster's first administrator, state_seq %d", seq)})
	}
	wait, cancel := context.WithTimeout(ctx, t.budget)
	defer cancel()
	if err := t.installed.awaitInstalled(wait, seq); err != nil {
		t.logger.Warn("the cluster's first administrator is created but this node has not installed it yet", "user", t.root.username, "state_seq", seq, "error", err)
	}
	t.logger.Info("created the cluster's first administrator", "user", t.root.username, "state_seq", seq)
}

// warn tells the log and the operator.
func (t *rootTask) warn(text string) {
	t.logger.Warn(text)
	writeOperatorLine(t.operator, t.logger, text)
}

// note logs a reason the task waits, once per reason.
func (t *rootTask) note(reason string) {
	if reason == t.waiting {
		return
	}
	t.waiting = reason
	t.logger.Info("the cluster's first administrator is not created yet", "user", t.root.username, "reason", reason)
}

// writeOperatorLine writes text to the operator's output (cmd/server's stderr), when there is one.
func writeOperatorLine(operator io.Writer, logger *slog.Logger, text string) {
	if operator == nil {
		return
	}
	if _, err := fmt.Fprintln(operator, text); err != nil {
		logger.Warn("could not write to the operator's output", "error", err)
	}
}

// SetOperatorOutput is where this node tells its operator what it did with a root password given
// at start (spec 6.2: a warning goes to stderr as well as the log). cmd/server passes stderr.
func (qs *Service) SetOperatorOutput(w io.Writer) {
	qs.operator = w
}

// armClusterRoot is BootstrapUsersFrom on a cluster member: it keeps the credential of password
// for the root task, which starts when the ring node opens (spec 6.5).
func (qs *Service) armClusterRoot(username, password string, allowSeed bool, setting string) error {
	if password == "" || username == "" {
		return nil
	}
	switch {
	case qs.config.Cluster.Witness:
		text := setting + " was not used: this node is a witness, which holds no password key and serves no logins. Set it on a data node instead."
		qs.logger.Warn(text)
		writeOperatorLine(qs.operator, qs.logger, text)
		return nil
	case !allowSeed:
		qs.logger.Warn(setting + " was not used: auth_bootstrap_root is false, so this node does not create the cluster's first administrator")
		return nil
	}
	if minLength := int(qs.core.auth.passwordMinLength); minLength > 0 {
		if length := utf8.RuneCountInString(password); length < minLength {
			return fmt.Errorf("the root password in %s is %d characters, below the %d-character minimum set by HOARDDB_PASSWORD_MIN_LENGTH; choose a longer one, or unset %s and start hoarddb-server from a terminal to type one",
				setting, length, minLength, setting)
		}
	}
	if err := checkUsernameNotReserved(username); err != nil {
		return fmt.Errorf("the cluster's first administrator: %w", err)
	}
	if err := checkUsername(username); err != nil {
		return fmt.Errorf("the cluster's first administrator: %w", err)
	}
	root, err := newClusterRoot(username, password, setting, qs.core.auth.argon2)
	if err != nil {
		return err
	}
	if configured, used := qs.core.auth.argon2.withDefaults(), clusterWindowParams(qs.core.auth.argon2); configured.Memory != used.Memory ||
		configured.Iterations != used.Iterations || configured.Parallelism != used.Parallelism {
		// B20: every member must be able to check it, so it is derived inside the cluster's window.
		qs.logger.Info("the cluster's first administrator's password hash is derived inside the work factor every cluster member accepts, not at auth.argon2_*",
			"memory_kib", used.Memory, "iterations", used.Iterations, "parallelism", used.Parallelism)
	}
	qs.clusterRoot = root
	if qs.ringNode != nil {
		qs.startRootTask()
	}
	return nil
}

// startRootTask runs the root task until it ends or the ring node closes.
func (qs *Service) startRootTask() {
	if qs.clusterRoot == nil || qs.stopRoot != nil {
		return
	}
	ctx, stop := context.WithCancel(qs.core.ctx)
	done := make(chan struct{})
	qs.stopRoot, qs.rootDone = stop, done
	task := newRootTask(qs.clusterRoot, qs.ringNode, qs.users, qs.userDeadline(), qs.logger, qs.operator)
	task.audit = func(entry AuditEntry) { qs.core.audit.Log(entry) }
	go func() {
		defer close(done)
		task.run(ctx)
	}()
}

// stopRootTask ends the root task and waits for it: it reads the ring node, so it ends first.
func (qs *Service) stopRootTask() {
	if qs.stopRoot == nil {
		return
	}
	qs.stopRoot()
	<-qs.rootDone
	qs.stopRoot, qs.rootDone = nil, nil
}

// clusterWindowParams is params moved into the credential window every cluster member accepts
// (ring user store spec 4.2, F6): a node configured above it derives at the window's top.
func clusterWindowParams(params Argon2Params) Argon2Params {
	params = params.withDefaults()
	params.Memory = min(max(params.Memory, authrules.MinArgon2MemoryKiB), authrules.MaxArgon2MemoryKiB)
	params.Iterations = min(max(params.Iterations, authrules.MinArgon2Iterations), authrules.MaxArgon2Iterations)
	params.Parallelism = min(max(params.Parallelism, authrules.MinArgon2Threads), authrules.MaxArgon2Threads)
	params.SaltLen, params.HashLen = authrules.MinSaltLen, authrules.Argon2HashLen
	return params
}
