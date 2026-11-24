package cli

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringDumpPageTries is how many times a dump asks for one data page in all when the ring
// answers STATE_MOVED or OWNER_DOWN: the first call and the retries (section 3.5, "A
// membership change during a dump").
const ringDumpPageTries = 5

// ringDumpPageRetryWait is how long a dump waits between two attempts at a refused page.
// It is a variable only so a test can shorten it; a shipped run waits one second.
var ringDumpPageRetryWait = time.Second

// A page answered NO_QUORUM or LEASE_LAPSED is asked for again after a random wait below
// ringDumpQuorumBackoffBase that doubles to ringDumpQuorumBackoffMax, the rule the Go driver
// applies to the same refusals, for at most ringDumpQuorumBound from the first refusal. The
// driver's own bound is its caller's deadline; a dump has no per-page deadline, so the bound
// is a figure: a member that has not seen a majority for a minute is not a stall.
// They are variables only so a test can shorten them.
var (
	ringDumpQuorumBackoffBase = 100 * time.Millisecond
	ringDumpQuorumBackoffMax  = 5 * time.Second
	ringDumpQuorumBound       = time.Minute
)

// retryMovedPage runs one data-page call and retries it while the ring refuses it because
// a slot moved or its owner is down. what names the page in the failure text; call closes
// over the request, so every attempt asks for the same cursor.
//
// This is the only retry a dump makes, and it is a retry of a call that returned nothing:
// an entity already written to the file is never read again (section 3.2 item 7). Any
// other error is returned at once and untouched - a permission refusal, a lost document or
// a broken connection is not a membership change, and asking again would only repeat it.
func (r *ringDumpRun) retryMovedPage(what string, call func() error) error {
	call = func(inner func() error) func() error {
		return func() error {
			_, err := retryQuorum(r, func() (struct{}, error) { return struct{}{}, inner() })
			return err
		}
	}(call)
	for attempt := 1; ; attempt++ {
		err := call()
		if err == nil {
			return nil
		}
		refusalCode, isRefusal := ringDumpPageRefusal(err)
		if !isRefusal {
			return err
		}
		if attempt >= ringDumpPageTries {
			// The text is a *RingError rather than the ringclient.Error itself, because
			// ringFailure prints a refusal's own message and would hide the fix and the
			// count. Its Err keeps the refusal in the chain, so errors.As still finds it and
			// the exit stays the code's own class (8).
			reason := fmt.Sprintf("%s still answered %s after %d tries, %s apart. The ring is changing its members or an owner is down: run 'nodes' to see the members, then run the same dump again.",
				what, refusalCode, ringDumpPageTries, ringDumpPageWaitText())
			return &RingError{
				Code:    refusalCode.String(),
				Message: reason,
				Exit:    refusalCode.Exit(),
				Err:     fmt.Errorf("%s was refused %d times in a row: %w", what, ringDumpPageTries, err),
			}
		}
		if err := r.waitForPageRetry(); err != nil {
			return err
		}
	}
}

// retryQuorum runs one ring call of a dump and asks again while the member answers NO_QUORUM
// or LEASE_LAPSED, for at most ringDumpQuorumBound from the first refusal. Every call a dump
// makes goes through it, so a stall in the listing of buckets is waited out like one in a
// page. The refusal is returned unchanged after the bound, so its own text reaches the user.
func retryQuorum[answer any](r *ringDumpRun, call func() (answer, error)) (answer, error) {
	return retryQuorumCtx(r.ctx, call)
}

// retryQuorumCtx is retryQuorum for a caller that holds only a context: a restore's calls.
// Asking again is safe for every call it wraps, though not always because nothing was applied.
// A read changes nothing. A create database that a repeat finds made is answered CONFLICT, and
// createDatabase reads the row back to tell its own earlier create from another database. A
// create bucket is two writes, the reverse-index insert and then the declaration, so the refusal
// can come after the first has applied; the create is idempotent (CreateBucket.Run), and the
// repeat completes what was left and returns the same id.
func retryQuorumCtx[answer any](ctx context.Context, call func() (answer, error)) (answer, error) {
	var refusals int
	var first time.Time
	for {
		got, err := call()
		if err == nil || !ringDumpQuorumRefusal(err) {
			return got, err
		}
		if refusals == 0 {
			first = time.Now()
		}
		refusals++
		if time.Since(first) >= ringDumpQuorumBound {
			return got, err
		}
		if waitErr := waitForQuorumRetry(ctx, refusals); waitErr != nil {
			return got, waitErr
		}
	}
}

// ringDumpQuorumRefusal reports whether a page read was refused because the member could not
// see a voter majority or renew its lease: it answers that for a stall of a moment (a pause,
// a busy disk, a node that has just joined) and serves again. The refusal is returned
// unchanged when the bound passes, so its own text, which names the fix, reaches the user.
func ringDumpQuorumRefusal(err error) bool {
	var refusal *ringclient.Error
	return errors.As(err, &refusal) && (refusal.Code == cluster.CodeNoQuorum || refusal.Code == cluster.CodeLeaseLapsed)
}

// waitForQuorumRetry waits a random time below the base doubled once per earlier refusal,
// up to the maximum (full jitter, as the Go driver does), or until ctx is cancelled.
func waitForQuorumRetry(ctx context.Context, refusals int) error {
	limit := ringDumpQuorumBackoffBase
	for step := 1; step < refusals && limit < ringDumpQuorumBackoffMax; step++ {
		limit *= 2
	}
	timer := time.NewTimer(rand.N(min(limit, ringDumpQuorumBackoffMax)))
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting to retry a call the ring refused: %w", ctx.Err())
	}
}

// ringDumpPageRefusal reports whether a page read was refused because the ring is moving
// the slot that holds it, and with which code. It reads the code, never the text: a
// retried page is one the ring answered STATE_MOVED or OWNER_DOWN.
func ringDumpPageRefusal(err error) (cluster.Code, bool) {
	var refusal *ringclient.Error
	if !errors.As(err, &refusal) {
		return 0, false
	}
	if refusal.Code != cluster.CodeStateMoved && refusal.Code != cluster.CodeOwnerDown {
		return 0, false
	}
	return refusal.Code, true
}

// waitForPageRetry waits one retry interval. The select on the run's context is what makes
// Ctrl-C end the wait at once instead of at the end of the interval: the context error it
// returns sends the run down the interrupt path of r.fail.
func (r *ringDumpRun) waitForPageRetry() error {
	timer := time.NewTimer(ringDumpPageRetryWait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-r.ctx.Done():
		return fmt.Errorf("waiting to retry a page the ring refused: %w", r.ctx.Err())
	}
}

// ringDumpPageWaitText is the wait as the refusal text says it: "1 second" for the shipped
// interval, and the duration's own text otherwise, so a test that shortens the wait is not
// told a figure the run never used.
func ringDumpPageWaitText() string {
	if ringDumpPageRetryWait == time.Second {
		return "1 second"
	}
	return ringDumpPageRetryWait.String()
}
