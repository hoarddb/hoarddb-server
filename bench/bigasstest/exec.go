package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// env is what every request of a repetition shares.
type env struct {
	w    *world
	reg  *registry
	book *book
	// timeout bounds one request; 0 sets no bound.
	timeout time.Duration
}

// errRequestTimeout marks a request that ran past the per-request timeout.
var errRequestTimeout = errors.New("request timed out")

// callContext bounds one request so that a hung server cannot stall a level.
func (e *env) callContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if e.timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, e.timeout)
}

// finish turns the end of a bounded call into the result's error: a call that ran past its own
// deadline, while the run itself was still going, is a timeout.
func (e *env) finish(ctx, call context.Context, res *result, err error) {
	if err != nil && call.Err() != nil && ctx.Err() == nil {
		err = fmt.Errorf("%w after %v", errRequestTimeout, e.timeout)
		res.TimedOut = true
	}
	res.OpErr = err
}

// result is what one request did. Latency stops at the last reply byte; the verification
// that fills VerifyErr runs after the clock has stopped.
type result struct {
	Layout model.Layout
	Kind   string
	Write  bool
	// Offset is the offset of a read that started inside its store, 0 for any other request.
	Offset    int
	Latency   time.Duration
	Reply     Reply
	OpErr     error
	VerifyErr error
	// TimedOut marks an OpErr that is the per-request timeout. Its Latency is the elapsed time.
	TimedOut bool
}

// runRequest performs one request through ops, then verifies a read or accounts for a write.
// The write is registered before it is sent, so a concurrent read of it is recognised.
func (e *env) runRequest(ctx context.Context, ops LayoutOps, req request) result {
	res := result{Layout: req.Layout, Kind: req.Kind, Write: req.Write != nil}
	if req.PlanErr != nil {
		res.OpErr = req.PlanErr
		return res
	}
	if req.Read != nil {
		res.Offset = req.Read.Offset
		floor := e.book.snapshot(*req.Read)
		call, cancel := e.callContext(ctx)
		started := time.Now()
		reply, err := ops.Read(call, *req.Read)
		res.Latency = time.Since(started)
		res.Reply = reply
		e.finish(ctx, call, &res, err)
		cancel()
		if err == nil {
			res.VerifyErr = verifyReadFloor(e.w, e.reg, *req.Read, reply, floor)
		}
		return res
	}
	e.reg.add(req.Write)
	e.book.begin(req.Write)
	call, cancel := e.callContext(ctx)
	started := time.Now()
	reply, err := ops.Write(call, *req.Write)
	res.Latency = time.Since(started)
	res.Reply = reply
	e.finish(ctx, call, &res, err)
	cancel()
	if err != nil {
		e.book.unknown(req.Write)
		return res
	}
	e.book.ack(req.Write, reply)
	if req.Write.Claim != nil && len(reply.Elements) > 0 {
		res.VerifyErr = verifyClaim(e.w, e.reg, req.Write.Claim.Run, reply.Elements)
		if res.VerifyErr == nil {
			e.book.noteClaim(req.Write.Claim.Run, reply.Elements[0])
		}
	}
	return res
}
