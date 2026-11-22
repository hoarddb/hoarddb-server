package wire

import (
	"context"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// ResumeJoining is OnBoot for a joining node that restarted mid-seed: it reconciles like
// OnBoot (the records are loaded already, by LoadRecords), except that a record already seeding keeps its attempt, its
// cursor and its seed_id, so the pull resumes with a re-registration (conv §12 contract 4).
// A ready record on a joining node is still rewritten to seeding from reset, because a
// ready record cannot arrive with data older than the log (conv §14). It exists because
// OnBoot rewrites a seeding record too, which would throw away every chunk already applied
// (SPEC-GAP T3-2).
func (r *Reconciler) ResumeJoining(ctx context.Context) error {
	return r.pass1(func() ([]Action, error) {
		if err := r.needLoaded("ResumeJoining"); err != nil {
			return nil, err
		}
		state := r.cfg.State.State()
		if member, ok := reconcileMember(state, r.cfg.NodeID); !ok || member.State != cluster.MemberJoining {
			return nil, fmt.Errorf("resume on boot: node %q is not joining in state_seq %d. Use OnBoot for a node that is not", r.cfg.NodeID, state.StateSeq)
		}
		r.mu.Lock()
		r.keepSeeding = true
		r.mu.Unlock()
		defer func() {
			r.mu.Lock()
			r.keepSeeding = false
			r.mu.Unlock()
		}()
		return r.reconcile(ctx, state, true)
	})
}

// LoadRecords reads the seed records from disk without reconciling anything. Node.open calls
// it before the tail scan on every cluster node; the driver's first pass (OnStateSeq, or
// ResumeJoining and OnJoiningCommit on a joiner) reconciles once the committed state is
// applied, which at boot it is not yet.
func (r *Reconciler) LoadRecords() error {
	return r.pass1(func() ([]Action, error) { return nil, r.load() })
}

// HasRecords reports whether the node holds any seed record, ready or not.
func (r *Reconciler) HasRecords() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pairs) > 0
}
