package wire

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

// A release records one byte for a retry (the state it left the lease in) and an extend
// that byte and the new deadline, so a deduped retry answers what the first attempt did.
const (
	releaseResultBytes = 1
	extendResultBytes  = 1 + 8
)

// leaseStates numbers the states a release or an extend can record, in the order of
// store.LeaseState's constants. Zero is never recorded.
var leaseStates = []store.LeaseState{store.LeaseAcked, store.LeaseReleased, store.LeaseExtended, store.LeaseExpired, store.LeaseUnknown}

// leaseResult is the dedupe result of a release or an extend, nil for any other class.
func leaseResult(class byte, outcome LeaseOutcome) []byte {
	if class != cluster.OpRelease && class != cluster.OpExtend {
		return nil
	}
	recorded := make([]byte, 0, extendResultBytes)
	for i, state := range leaseStates {
		if state == outcome.State {
			recorded = append(recorded, byte(i+1))
		}
	}
	if class == cluster.OpExtend {
		recorded = binary.BigEndian.AppendUint64(recorded, uint64(outcome.Until))
	}
	return recorded
}

// leaseChangeOf reads a recorded release or extend result.
func leaseChangeOf(class byte, recorded []byte) (LeaseChange, error) {
	want := releaseResultBytes
	if class == cluster.OpExtend {
		want = extendResultBytes
	}
	if len(recorded) != want || recorded[0] < 1 || int(recorded[0]) > len(leaseStates) {
		return LeaseChange{}, fmt.Errorf("the recorded result of a lease change is %d bytes %x, want %d bytes that start with a state", len(recorded), recorded, want)
	}
	change := LeaseChange{State: leaseStates[recorded[0]-1]}
	if class == cluster.OpExtend {
		change.Until = int64(binary.BigEndian.Uint64(recorded[1:]))
	}
	return change, nil
}

// LeaseChange is what a release or an extend returns.
type LeaseChange struct {
	// State is released or extended when the lease was live, expired when its deadline had
	// passed and unknown when the owner holds no such lease. The last two are answers, not
	// errors: the messages come back by the return step on their own.
	State store.LeaseState
	// Until is the lease's new deadline in Unix milliseconds; zero unless State is extended.
	Until int64
	// Pos is the log position the change holds.
	Pos cluster.Position
	// Deduped says the answer was recomposed from an earlier attempt of the same request.
	Deduped bool
}

// extendLengthFits reports whether a lease of length ms is one an extend may ask for.
func extendLengthFits(length int64) bool {
	return length >= store.MinLeaseMs && length <= store.MaxLeaseMs
}

// ReleaseRequest gives a lease back before it runs out.
type ReleaseRequest struct {
	StoreTarget
	// Lease is the id Claim returned.
	Lease [16]byte
}

// Release gives a lease back: its messages return to the field by the return rule and
// count as a delivery. A lease that is expired or unknown is an answer in State, not an
// error. It is a logged, fenced owner write that records the state, so a retry with the
// same request id returns it again.
func (n *Node) Release(ctx context.Context, req ReleaseRequest, options ...WriteOption) (LeaseChange, error) {
	opts, err := newWriteOptions(options)
	if err != nil {
		return LeaseChange{}, err
	}
	call, err := n.fieldCall(ctx, req.StoreTarget, "release")
	if err != nil {
		return LeaseChange{}, err
	}
	op := call.ref.template()
	op.Class, op.LeaseID, op.Fields = cluster.OpRelease, req.Lease, []cluster.FieldOp{{FieldID: call.fieldID}}
	change, _, err := n.leaseChange(ctx, call, op, opts)
	return change, err
}

// ExtendRequest moves a lease's deadline.
type ExtendRequest struct {
	StoreTarget
	// Lease is the id Claim returned.
	Lease [16]byte
	// LeaseMs is the lease's new length counted from the moment the owner orders the
	// extend, 1000 to 3600000.
	LeaseMs int64
}

// Extend moves a live lease's deadline to the owner's clock plus req.LeaseMs. A lease
// that is expired or unknown is an answer in State, not an error. It is a logged, fenced
// owner write that records the state and the deadline, so a retry with the same request id
// returns them again.
func (n *Node) Extend(ctx context.Context, req ExtendRequest, options ...WriteOption) (LeaseChange, error) {
	opts, err := newWriteOptions(options)
	if err != nil {
		return LeaseChange{}, err
	}
	call, err := n.fieldCall(ctx, req.StoreTarget, "extend")
	if err != nil {
		return LeaseChange{}, err
	}
	if !extendLengthFits(req.LeaseMs) {
		return LeaseChange{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"an extend of %s.%s.%s asks for a lease of %d ms and a lease is %d to %d ms. Nothing was changed. Send a length within that range",
			req.Database, req.Bucket, req.Field, req.LeaseMs, store.MinLeaseMs, store.MaxLeaseMs)}
	}
	op := call.ref.template()
	op.Class, op.LeaseID, op.Until, op.Fields = cluster.OpExtend, req.Lease, req.LeaseMs, []cluster.FieldOp{{FieldID: call.fieldID}}
	change, _, err := n.leaseChange(ctx, call, op, opts)
	return change, err
}

// leaseChange sends a release or an extend and reads the state it recorded; the write's
// own answer comes back too, for the ring reply's outcome and copy count.
func (n *Node) leaseChange(ctx context.Context, call fieldCall, op cluster.Op, opts writeOptions) (LeaseChange, WriteAnswer, error) {
	answer, err := n.send(ctx, call.ref, call.encID, op, opts)
	if err != nil {
		return LeaseChange{}, WriteAnswer{}, err
	}
	change, err := leaseChangeOf(op.Class, answer.Result)
	if err != nil {
		return LeaseChange{}, WriteAnswer{}, fmt.Errorf("reading the answer of the lease change at %s: %w", answer.Pos, err)
	}
	change.Pos, change.Deduped = answer.Pos, answer.Deduped
	return change, answer, nil
}
