package server

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The question (replication spec §20.8.5, build doc step 9.6): what is your
// copy of these buckets? Dispatch 10's hand-off asks it of every node that may
// hold a copy before a new owner takes a bucket over; dispatch 9 builds the
// question and the answer.
//
// It rides REPL_PUSH (no new opcode): a push with Handoff set carries no
// entries, changes nothing, and is refused across epochs like a push that
// changes state, because an answer is only comparable between two nodes on
// one ring. Each bucket is answered under its fence taken exclusively, so no
// write is between its log append and its apply while its position is read.

// handoffAskLimit is the most buckets one question names: a request that grew
// with the number of buckets would stop fitting a frame at the scale this
// database is for (build doc §1 trap 5).
const handoffAskLimit = 4096

// ReplHandoff is the question a push carries.
type ReplHandoff struct {
	Ask []ReplBucketRef `bson:"ask"`
	// NamedAllAt, from a removed node, says it has named every bucket it holds
	// to the bucket's owner (removed_handoff.go), and is the membership epoch
	// whose owners it named them to, or namedAllEnded once nothing is left to
	// hand over. A member waits for one at or above its own epoch, or for the
	// node to be confirmed gone, before it reports the change settled (owner
	// ruling R7; BUG-0701). Internode only.
	NamedAllAt uint64 `bson:"named_all_at,omitempty"`
	// HeardNamedAllAt, on a member's probe of a removed node (a frame with
	// no Ask and no NamedAllAt), is the highest NamedAllAt the member holds
	// from it: 0 after a restart. The removed node answers the probe with its
	// notice when that is safe, or names the member's buckets to it again
	// first (BUG-0702). Internode only.
	HeardNamedAllAt uint64 `bson:"heard_named_all_at,omitempty"`
}

// namedAllEnded is NamedAllAt once a removed node's hand-off has ended: every
// bucket it holds is served by its owner, was dropped, or is not handed over
// (auto-rebalance off, another incarnation). It covers every epoch, since
// nothing is left that a later membership could move before its copy lands.
// It is the highest value BSON carries as an integer: a uint64 above it does
// not encode, and the frame carrying it was never sent.
const namedAllEnded uint64 = math.MaxInt64

// ReplBucketRef names one bucket.
type ReplBucketRef struct {
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
}

// ReplCopy is a node's answer for one bucket: whether it holds a copy, the
// copy's lineage (spec §20.8.1), its two placement marks for the current epoch
// (§20.8.2), and whether it owes an apply — its state document records a
// failed apply replay has not retried (A-9.2, review F-5).
type ReplCopy struct {
	Holds       bool   `bson:"holds,omitempty"`
	Term        uint64 `bson:"term,omitempty"`
	Through     uint64 `bson:"through,omitempty"`
	BaseTerm    uint64 `bson:"base_term,omitempty"`
	BaseThrough uint64 `bson:"base_through,omitempty"`
	Opened      bool   `bson:"opened,omitempty"`
	Moved       uint64 `bson:"moved,omitempty"`
	Outside     uint64 `bson:"outside,omitempty"`
	Owes        bool   `bson:"owes,omitempty"`
	// Incarnation is the held copy's (R8-A): copies are compared by their
	// lineage only within one incarnation.
	Incarnation uint64 `bson:"incarnation,omitempty"`
	// Dropped are the incarnations of the bucket this node recorded dropped
	// (R8-C), usually none: a take-over never copies one of them.
	Dropped []uint64 `bson:"dropped,omitempty"`
	// Factor is the factor the held copy's record declares, 0 when it
	// inherits the node default: the record and not the registry, because the
	// record is what a pull of the copy ships, so a take-over and a member
	// that missed an alter learn the same number
	// (bucket-replication-rev8-spec §11.3).
	Factor int32 `bson:"factor,omitempty"`
	// CreateInFlight says a create of the name is in flight at the answering
	// node (registerCreate's orphan), which answers that it holds no copy
	// yet: a take-over that finds no copy anywhere does not record "nothing"
	// for it (on-demand pull spec §4.2, D-10). Internode only.
	CreateInFlight bool `bson:"create_in_flight,omitempty"`
}

// answerQuestion is handleReplPush's answer to a push that carries a question.
func (c *Cluster) answerQuestion(req ReplPushRequest) ReplPushResponse {
	if len(req.Entries) > 0 || req.Stream != nil || req.ReturnResponse {
		return ReplPushResponse{Success: false, Error: fmt.Sprintf(
			"a question from %s carries entries or a stream; a question changes nothing — ask it alone", req.FromNode)}
	}
	copies, err := c.answerHandoff(req.Handoff)
	if err != nil {
		return ReplPushResponse{Success: false, Error: err.Error()}
	}
	// A follower asks again once it has taken this node's term: its stream
	// from this node, stalled on entries of that term, may deliver now.
	c.wakeSenderOf(req.FromNode)
	return ReplPushResponse{Success: true, Copies: copies}
}

// answerHandoff answers a question, bucket by bucket, in the order asked.
func (c *Cluster) answerHandoff(question *ReplHandoff) ([]ReplCopy, error) {
	if len(question.Ask) > handoffAskLimit {
		return nil, fmt.Errorf("the question names %d buckets; at most %d are answered at once — ask in parts", len(question.Ask), handoffAskLimit)
	}
	copies := make([]ReplCopy, 0, len(question.Ask))
	for _, ref := range question.Ask {
		answer, err := c.answerCopy(ref)
		if err != nil {
			return nil, err
		}
		copies = append(copies, answer)
	}
	return copies, nil
}

// answerCopy reads one bucket's copy under its fence, taken exclusively.
func (c *Cluster) answerCopy(ref ReplBucketRef) (ReplCopy, error) {
	fence := c.s.acquireFence(placementOf(ref.Database, ref.Bucket), fenceExclusive)
	defer fence.release()
	return c.readCopy(ref)
}

// readCopy is answerCopy for a caller that holds the bucket's fence
// exclusively already.
func (c *Cluster) readCopy(ref ReplBucketRef) (ReplCopy, error) {
	key := placementOf(ref.Database, ref.Bucket)
	marks := c.placementMarksOf(key)
	dropped, err := c.s.droppedIncarnations(ref.Database, ref.Bucket)
	if err != nil {
		return ReplCopy{}, fmt.Errorf("answering for bucket %q in database %q: %w", ref.Bucket, ref.Database, err)
	}
	if !c.s.bucketStoreOpen(ref.Database, ref.Bucket) {
		return ReplCopy{Moved: marks.Moved, Outside: marks.Outside, Dropped: dropped,
			CreateInFlight: c.createInFlight(ref.Database, ref.Bucket)}, nil
	}
	st, err := c.s.bucketStateNamed(ref.Database, ref.Bucket)
	if err != nil {
		return ReplCopy{}, fmt.Errorf("answering for bucket %q in database %q: %w", ref.Bucket, ref.Database, err)
	}
	facts, _ := c.s.peekSettled(ref.Database, ref.Bucket)
	return ReplCopy{
		Holds: true, Term: st.Term, Through: st.Through, BaseTerm: st.BaseTerm, BaseThrough: st.BaseThrough,
		Opened: c.copyIsOpened(key, st, marks), Moved: marks.Moved, Outside: marks.Outside,
		Owes: len(st.FailedSeqs) > 0, Incarnation: st.Incarnation, Dropped: dropped, Factor: facts.replicationFactor,
	}, nil
}

// createInFlight reports whether a create of the name is in flight here:
// registered as an orphan until it is applied, or until it is handed off when
// a membership change overtook it (registerCreate). An O(1) lookup under
// sweepLeftMu, a leaf: answerCopy holds the bucket's exclusive fence, so the
// order is fence → sweepLeftMu, and registerCreate and dropOrphan take
// sweepLeftMu alone.
func (c *Cluster) createInFlight(database, bucket string) bool {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	_, inFlight := c.orphans[heldBucket{database: database, bucket: bucket}]
	return inFlight
}

// copyIsOpened reports whether this node's copy is the one its term's origin
// holds (SA-9.4): as its state document says, or — for a copy with no lineage,
// a bucket from before this build until its first write here — when this node
// is the ring's owner and the copy is not marked moved (spec §20.8.1). marks
// are the bucket's for the current epoch (placementMarksOf), which the pass
// may not have written yet.
func (c *Cluster) copyIsOpened(key placementKey, st store.BucketState, marks placementMarks) bool {
	if st.Opened {
		return true
	}
	if st.Term != 0 || st.Through != 0 {
		return false
	}
	owner, err := c.Owner(key)
	return err == nil && owner.ID == c.self.ID && !c.removed() && marks.Moved == 0
}

// cacheIsOpened is copyIsOpened from the lineage cache alone: the term, the
// opened bit and the bit that says the position is zero (a term-0 copy's
// through, which the cache does not otherwise keep). The caller holds no
// lock.
func (c *Cluster) cacheIsOpened(key placementKey, term uint64, flags uint32, marks placementMarks) bool {
	if flags&lineageOpened != 0 {
		return true
	}
	if term != 0 || flags&lineagePositionZero == 0 {
		return false
	}
	owner, err := c.Owner(key)
	return err == nil && owner.ID == c.self.ID && !c.removed() && marks.Moved == 0
}

// askCopies asks node what its copies of refs are, in parts of at most
// handoffAskLimit buckets, and returns the answers in the order asked.
func (c *Cluster) askCopies(ctx context.Context, node ring.Node, refs []ReplBucketRef) ([]ReplCopy, error) {
	copies := make([]ReplCopy, 0, len(refs))
	for start := 0; start < len(refs); start += handoffAskLimit {
		part := refs[start:min(start+handoffAskLimit, len(refs))]
		answered, err := c.askCopiesOnce(ctx, node, part)
		if err != nil {
			return nil, err
		}
		copies = append(copies, answered...)
	}
	return copies, nil
}

// askCopiesOnceHook, when set, runs on the asking node just before
// askCopiesOnce sends its one frame — after the dial and the encoding, so only
// a question that is really sent is seen — with the asking node's id and how
// many buckets the frame names. Tests count questions with it. Nil in
// production.
var askCopiesOnceHook atomic.Pointer[func(nodeID string, asked int)]

// askCopiesOnce sends one question of at most handoffAskLimit buckets.
func (c *Cluster) askCopiesOnce(ctx context.Context, node ring.Node, refs []ReplBucketRef) ([]ReplCopy, error) {
	cli, err := c.peerFor(node).dial(ctx)
	if err != nil {
		return nil, fmt.Errorf("asking %s what its copies of %d buckets are: %w", node.ID, len(refs), err)
	}
	payload, err := bson.Marshal(ReplPushRequest{Token: c.localToken(), FromNode: c.self.ID, Epoch: c.Epoch(),
		Handoff: &ReplHandoff{Ask: refs}})
	if err != nil {
		return nil, fmt.Errorf("encoding the question to %s: %w", node.ID, err)
	}
	if hook := askCopiesOnceHook.Load(); hook != nil {
		(*hook)(c.self.ID, len(refs))
	}
	frame, err := cli.SendRequest(ctx, transport.LaneRequest, transport.OpReplPush, payload)
	if err != nil {
		return nil, fmt.Errorf("asking %s what its copies of %d buckets are: %w", node.ID, len(refs), err)
	}
	var resp ReplPushResponse
	if err := decodePeerAnswer(frame.Payload, &resp); err != nil {
		return nil, fmt.Errorf("decoding %s's answer: %w", node.ID, err)
	}
	if !resp.Success && resp.Refusal == refusalEpoch {
		return nil, fmt.Errorf("%s did not answer what its copies are: %s: %w", node.ID, resp.Error, errQuestionAcrossEpochs)
	}
	if !resp.Success {
		return nil, fmt.Errorf("%s did not answer what its copies are: %s", node.ID, resp.Error)
	}
	if len(resp.Copies) != len(refs) {
		return nil, fmt.Errorf("%s answered %d copies for the %d buckets asked", node.ID, len(resp.Copies), len(refs))
	}
	return resp.Copies, nil
}
