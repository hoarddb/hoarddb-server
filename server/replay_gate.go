package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// errReplayingLog is the refusal every client frame gets until the ring node's log has been
// applied. It is UNAVAILABLE: the node is healthy and will serve, a retry is right.
var errReplayingLog = errors.New("the node is replaying its log")

// replayGate holds the driver listener's handler shut until the ring node's log is applied.
//
// Raft applies a restarted node's log asynchronously (the library does not restore committed
// logs at open), so without this a node whose log holds an entry it refuses would serve reads
// and writes from a half-built or frozen state for the length of the replay, on every start of
// a restart loop. The mixed-build spec (6.3, round-5 M1) makes serving wait for
// LogApplied() || Diverged(); a diverged node never serves, because it is about to exit.
type replayGate struct {
	// ready reports that the log is applied and the node is not diverged.
	ready func() bool
	next  transport.RequestHandler
	// open latches true the first time ready holds, so a served frame costs one atomic load.
	open atomic.Bool
}

func (g *replayGate) handle(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
	if !g.open.Load() {
		if !g.ready() {
			if replayExempt(frame) {
				return g.next(ctx, frame)
			}
			refusal := &sentencedError{cause: errReplayingLog, message: "replaying its log: this node restarted and is applying the cluster's log before it serves. Nothing was changed; retry in a few seconds."}
			answer := errorFrame(frame, withOutcome(outcomeNotApplied, refusal))
			if frame.Opcode == transport.OpAuth {
				// No credential was looked at, so the per-IP ledger must not count this as a
				// failed login: a few AUTHs during one replay would lock the operator out.
				answer.MarkCredentialUnchecked()
			}
			return answer, nil
		}
		g.open.Store(true)
	}
	return g.next(ctx, frame)
}

// replayExempt reports whether a frame may pass while the gate is shut: the read-only liveness
// and node-status requests, so a waiting node can be inspected with the CLI (status shows why it
// is not serving). They are OpPing, OpHealthCheck and OpRingAdmin carrying describe_node; every
// data frame, every other admin command and the catalog reads stay gated, because the state they
// read is still being rebuilt.
func replayExempt(frame *transport.Frame) bool {
	switch frame.Opcode {
	case transport.OpPing, transport.OpHealthCheck:
		return true
	case transport.OpRingAdmin:
		cmd, err := bson.Raw(frame.Payload).LookupErr("cmd")
		if err != nil {
			return false
		}
		name, isString := cmd.StringValueOK()
		return isString && name == wire.AdminDescribeNode
	}
	return false
}

// gateDriverHandler puts the replay gate in front of the driver listener's handler when a ring
// node is open, and returns the handler unchanged when none is.
func (qs *Service) gateDriverHandler(next transport.RequestHandler) transport.RequestHandler {
	if qs.ringNode == nil {
		return next
	}
	node := qs.ringNode
	gate := &replayGate{ready: node.StateLoaded, next: next}
	return gate.handle
}

// replayPoll is how often awaitReplay asks the node whether its state is loaded: a cheap check
// (a latch read once loaded; before that a few log reads), so a short tick is fine.
const replayPoll = 10 * time.Millisecond

// awaitReplay waits, up to the node's boot timeout, until the log is applied or the node has
// diverged, so a start that returns has either a node that serves or a Diverged channel that is
// closed. A node that cannot finish in time (a member restarted with its peers down never learns
// the commit index) keeps answering UNAVAILABLE and starts serving the moment it can; it is not
// failed, because the log arrives on the listeners this start has already opened.
func (qs *Service) awaitReplay() {
	node := qs.ringNode
	if node == nil {
		return
	}
	deadline := time.NewTimer(wire.BootTimeout)
	defer deadline.Stop()
	poll := time.NewTicker(replayPoll)
	defer poll.Stop()
	for !node.StateLoaded() && !node.IsDiverged() {
		if qs.bootWaitAborted.Load() {
			return
		}
		select {
		case <-qs.core.ctx.Done():
			return
		case <-deadline.C:
			qs.logger.Warn("this node is still replaying its log; client requests are answered UNAVAILABLE until it has applied it",
				"waited", wire.BootTimeout.String())
			return
		case <-poll.C:
		}
	}
}

// AbortBootWait ends the wait inside Start for the node's log, so a signal received during a long
// boot wait lets Start return and the caller run Stop. The node is not failed: it keeps
// answering UNAVAILABLE until it can serve, and Stop closes it.
func (qs *Service) AbortBootWait() { qs.bootWaitAborted.Store(true) }

// divergedStopDeadline bounds the stop of a diverged node: Close does no data-moving work, but
// what stops under it (replication, the legacy store) is not under this package's control, and
// a node that has decided to leave must leave.
const divergedStopDeadline = 10 * time.Second

// StopWithin stops the server like Stop, but gives up waiting after deadline. The caller is
// about to exit the process, which finishes what a stuck Stop would not.
func (qs *Service) StopWithin(deadline time.Duration) error {
	return stopWithin(qs.Stop, deadline)
}

// stopWithin runs stop and returns its result, or an error once deadline passes. The goroutine
// of a stop that never returns is left behind on purpose: the process exits next.
func stopWithin(stop func() error, deadline time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- stop() }()
	select {
	case err := <-done:
		return err
	case <-time.After(deadline):
		return fmt.Errorf("the server did not stop within %s", deadline)
	}
}

// WaitForExit blocks until a signal arrives or the ring node diverges, stops the server and
// returns the process exit status: 0 after a signal, cluster.DivergedExitCode after a
// divergence, 1 when the stop failed. It looks at Diverged first, so a divergence recorded
// while the log was replaying (before this was called) is not left to a random choice between
// two ready channels.
func (qs *Service) WaitForExit(signals <-chan os.Signal) int {
	select {
	case <-qs.Diverged():
		return qs.stopDiverged()
	default:
	}
	select {
	case <-qs.Diverged():
		return qs.stopDiverged()
	case sig := <-signals:
		qs.logger.Info("received signal, shutting down", "signal", sig)
		if err := qs.Stop(); err != nil {
			qs.logger.Error("server stopped with error", "error", err)
			return 1
		}
		return 0
	}
}

func (qs *Service) stopDiverged() int {
	record, _ := qs.ringNode.Divergence()
	qs.logger.Error("this node disagrees with the cluster about its state and is stopping; the line above names the entry and the fix",
		"exit_code", cluster.DivergedExitCode, "state_seq", record.StateSeq)
	if err := qs.StopWithin(divergedStopDeadline); err != nil {
		qs.logger.Error("the stop of a diverged node did not finish; exiting anyway", "error", err)
	}
	return cluster.DivergedExitCode
}
