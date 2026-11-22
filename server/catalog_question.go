package server

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The catalog question: what does a member know of a database, and
// does it hold one of its buckets? A node's own view can be short of the
// cluster's. A node that joined learns a database only with its first copied
// bucket, and holds none of the buckets it owns until each is pulled. What it
// creates from that view stands beside what the members hold: a database
// create starts a second incarnation, and a bucket create its owner logs makes
// every holder drop its copy for the owner's empty one
// (decideGenerationOfCreate, G-6 (d)). A node therefore asks the members before
// it creates from a view that may be short.
//
// It rides REPL_PUSH (no new opcode), as dispatch 9's copy question does: a
// push that carries it carries nothing else and changes nothing.

// ownerQuestionPassOverHook, when set, has an owner create a bucket it does
// not hold without asking the members, or the nodes removed within the history
// (awaitCopyARemovedNodeMayHold), whether one does: a test makes a stray
// incarnation with it (10a.6). False in production.
var ownerQuestionPassOverHook atomic.Bool

// catalogQuestionWait bounds how long a create waits for the members' answers.
// It is shorter than the write-concern timeout because a create waits for it
// before it waits for anything else.
const catalogQuestionWait = 2 * time.Second

// ReplCatalogAsk is the question a push carries: database, and optionally one
// of its buckets.
type ReplCatalogAsk struct {
	Database string `bson:"database"`
	Bucket   string `bson:"bucket,omitempty"`
}

// ReplCatalogAnswer is a member's answer: the incarnations of the database it
// holds live and knows dropped, and the incarnation its copy of the bucket
// asked about was created in, or 0 when it holds none. HeldIncarnation is that
// copy's own incarnation, the bucket's (R8-A), read from its state document.
type ReplCatalogAnswer struct {
	Live            []int64 `bson:"live,omitempty"`
	Dropped         []int64 `bson:"dropped,omitempty"`
	HeldGeneration  int64   `bson:"held_generation,omitempty"`
	HeldIncarnation uint64  `bson:"held_incarnation,omitempty"`
	// DroppedIncarnations are the incarnations of the bucket asked about that
	// this node recorded dropped (R8-C): a holder of one of them holds a
	// dropped bucket, not the name's.
	DroppedIncarnations []uint64 `bson:"dropped_incarnations,omitempty"`
}

// answerCatalog is handleReplPush's answer to a push that carries a catalog
// question.
func (c *Cluster) answerCatalog(req ReplPushRequest) ReplPushResponse {
	if len(req.Entries) > 0 || req.Stream != nil || req.ReturnResponse || req.Handoff != nil {
		return ReplPushResponse{Success: false, Error: fmt.Sprintf(
			"a catalog question from %s carries entries, a stream or another question; a question changes nothing — ask it alone", req.FromNode)}
	}
	ask := req.Catalog
	if hook := catalogAnswerHook.Load(); hook != nil {
		(*hook)(c.self.ID)
	}
	live, err := c.s.liveGenerations(ask.Database)
	if err != nil {
		return ReplPushResponse{Success: false, Error: fmt.Sprintf("reading this node's incarnations of database %q: %v", ask.Database, err)}
	}
	dropped, err := c.s.droppedGenerations(ask.Database)
	if err != nil {
		return ReplPushResponse{Success: false, Error: fmt.Sprintf("reading this node's dropped incarnations of database %q: %v", ask.Database, err)}
	}
	answer := &ReplCatalogAnswer{Live: live, Dropped: dropped}
	if ask.Bucket != "" {
		answer.HeldGeneration, answer.HeldIncarnation, err = c.s.openBucketIdentity(ask.Database, ask.Bucket)
		if err != nil {
			return ReplPushResponse{Success: false, Error: err.Error()}
		}
		if answer.DroppedIncarnations, err = c.s.droppedIncarnations(ask.Database, ask.Bucket); err != nil {
			return ReplPushResponse{Success: false, Error: err.Error()}
		}
	}
	return ReplPushResponse{Success: true, Catalog: answer}
}

// openBucketIdentity is the incarnation of its database this node's served
// copy of database/bucket was created in, and the copy's own incarnation from
// its state document; 0 and 0 when it serves none. It looks the bucket up
// once: a drop may remove it at any moment, so a second lookup after
// bucketStoreOpen can find nothing (verifier finding D-1).
func (s *Server) openBucketIdentity(database, bucket string) (int64, uint64, error) {
	facts, held := s.peekSettled(database, bucket)
	if !held || facts.id == 0 {
		return 0, 0, nil
	}
	snapshot := s.kv.NewSnapshot()
	defer snapshot.Discard()
	st, err := store.LoadBucketState(snapshot, facts.slot, facts.databaseID, facts.id)
	if err != nil {
		return 0, 0, fmt.Errorf("reading the incarnation of bucket %q in database %q: %w", bucket, database, err)
	}
	return facts.generation, st.Incarnation, nil
}

// catalogHolder is a member that holds the bucket a question named, and the
// incarnation its copy was created in.
type catalogHolder struct {
	member      string
	generation  int64
	incarnation uint64
}

// catalogSurvey is what the other members answered to one catalog question.
type catalogSurvey struct {
	// live is each answering member's live incarnations of the database, in
	// membership order.
	live [][]int64
	// dropped is every incarnation of the database any member knows dropped.
	dropped []int64
	// holders are the members that hold the bucket asked about.
	holders []catalogHolder
	// droppedIncarnations are the bucket's incarnations any member recorded
	// dropped.
	droppedIncarnations []uint64
	// unheard names each member that did not answer, and unanswered says why.
	unheard    []string
	unanswered []error
}

// surveyCatalog asks every other current member ask, in parallel, and waits at
// most catalogQuestionWait for them. With no other member it asks nobody.
func (c *Cluster) surveyCatalog(ctx context.Context, ask ReplCatalogAsk) catalogSurvey {
	_, members := c.epochMembers()
	others := slices.DeleteFunc(members, func(member ring.Node) bool { return member.ID == c.self.ID })
	if len(others) == 0 {
		return catalogSurvey{}
	}
	askCtx, cancel := context.WithTimeout(ctx, catalogQuestionWait)
	defer cancel()
	answers := make([]ReplCatalogAnswer, len(others))
	failures := make([]error, len(others))
	var asked sync.WaitGroup
	for i, member := range others {
		asked.Add(1)
		go func() {
			defer asked.Done()
			answers[i], failures[i] = c.askCatalog(askCtx, member, ask)
		}()
	}
	asked.Wait()
	var survey catalogSurvey
	for i, member := range others {
		if failures[i] != nil {
			survey.unheard = append(survey.unheard, member.ID)
			survey.unanswered = append(survey.unanswered, failures[i])
			continue
		}
		survey.live = append(survey.live, answers[i].Live)
		survey.dropped = mergeGenerations(survey.dropped, answers[i].Dropped)
		for _, incarnation := range answers[i].DroppedIncarnations {
			if !slices.Contains(survey.droppedIncarnations, incarnation) {
				survey.droppedIncarnations = append(survey.droppedIncarnations, incarnation)
			}
		}
		if answers[i].HeldGeneration != 0 {
			survey.holders = append(survey.holders, catalogHolder{
				member: member.ID, generation: answers[i].HeldGeneration, incarnation: answers[i].HeldIncarnation})
		}
	}
	return survey
}

// askCatalog asks one member a catalog question.
func (c *Cluster) askCatalog(ctx context.Context, member ring.Node, ask ReplCatalogAsk) (ReplCatalogAnswer, error) {
	cli, err := c.peerFor(member).dial(ctx)
	if err != nil {
		return ReplCatalogAnswer{}, fmt.Errorf("asking %s what it knows of database %q: %w", member.ID, ask.Database, err)
	}
	payload, err := bson.Marshal(ReplPushRequest{Token: c.localToken(), FromNode: c.self.ID, Epoch: c.Epoch(), Catalog: &ask})
	if err != nil {
		return ReplCatalogAnswer{}, fmt.Errorf("encoding the catalog question to %s: %w", member.ID, err)
	}
	frame, err := cli.SendRequest(ctx, transport.LaneRequest, transport.OpReplPush, payload)
	if err != nil {
		return ReplCatalogAnswer{}, fmt.Errorf("asking %s what it knows of database %q: %w", member.ID, ask.Database, err)
	}
	var resp ReplPushResponse
	if err := decodePeerAnswer(frame.Payload, &resp); err != nil {
		return ReplCatalogAnswer{}, fmt.Errorf("decoding %s's answer about database %q: %w", member.ID, ask.Database, err)
	}
	if !resp.Success {
		return ReplCatalogAnswer{}, fmt.Errorf("%s did not say what it knows of database %q: %s", member.ID, ask.Database, resp.Error)
	}
	if resp.Catalog == nil {
		return ReplCatalogAnswer{}, fmt.Errorf("%s answered the question about database %q without an answer; it runs a build that does not know the question", member.ID, ask.Database)
	}
	return *resp.Catalog, nil
}

// createAwaitingCopy is the refusal of a create at an owner that is pulling the
// bucket now: the bucket exists, and its copy is landing here.
func createAwaitingCopy(database, bucket string) error {
	return &sentencedError{cause: ErrJoinSyncIncomplete, message: fmt.Sprintf(
		"this node owns bucket %q in database %q since a membership change and is copying it from the bucket's other holders now, "+
			"so nothing was created. Retry in a moment: once the copy lands, the create answers that the bucket already exists",
		bucket, database)}
}

// refuseCreateBesideCopy is the join fence for a bucket create. This
// node owns database/bucket. When it holds no copy, it asks the other members
// whether one does before it creates the bucket. A holder means the bucket
// exists and this node has not copied it yet. The create is refused with retry,
// as a write is: the copy it would create is empty, and each holder would drop
// its own copy for it.
//
// It asks on every create of a bucket this node does not hold, not only after
// a membership change. A node cannot tell from its own state whether a change
// has settled: a joiner's record of the epochs before it joined is its own boot
// view, in which it owned what it now owns. Ruling 39's settling window
// (the on-demand-pull-spec record §3) is what will let it ask only inside
// the window. Until then a create waits one parallel round trip to the members.
//
// A member that does not answer is passed over only when the name's replica
// set has not changed since the epoch every member settled at (R8-D, review
// M-5): a member settled at an epoch has confirmed every hand-off up to it, so
// a set unchanged since then is held, if at all, by its members — this node
// among them. When the set changed, or the floor has left the history, the
// create is refused with retry, naming the unheard members.
//
// A copy of an incarnation this node or any member knows dropped is not the
// bucket: its holder missed the drop, and the create is what replaces that
// copy (G-6 (d), review F-1). Nor is a copy of a bucket this node dropped and
// remembers dropping. Past that memory (recentDrops and the reclaim record), such
// a create is refused with retry until the holder applies the drop.
func (c *Cluster) refuseCreateBesideCopy(ctx context.Context, database, bucket string, factor int) (uint64, error) {
	if c.s.bucketStoreOpen(database, bucket) {
		return 0, nil
	}
	if c.pullPending(database, bucket) {
		return 0, createAwaitingCopy(database, bucket)
	}
	epoch := c.Epoch()
	if ownerQuestionPassOverHook.Load() {
		return epoch, nil
	}
	survey := c.surveyCatalog(ctx, ReplCatalogAsk{Database: database, Bucket: bucket})
	if err := c.refuseOverUnheard(database, bucket, factor, survey); err != nil {
		return 0, err
	}
	if len(survey.holders) == 0 {
		return epoch, nil
	}
	holders, err := c.liveHolders(database, bucket, survey)
	if err != nil {
		return 0, err
	}
	if len(holders) == 0 {
		return epoch, nil
	}
	// This node dropped the bucket, and a holder has not applied the drop yet:
	// its copy is the dropped one, and this node's stream delivers the drop
	// before the create. Read only once a holder answered, so a create of a new
	// name never pays for it.
	if c.s.recentDrops.contains(database, bucket) {
		return epoch, nil
	}
	droppedHere, err := c.s.recentlyDroppedBucket(database, bucket)
	if err != nil {
		return 0, fmt.Errorf("deciding whether bucket %q in database %q was dropped on this node: %w", bucket, database, err)
	}
	if droppedHere {
		return epoch, nil
	}
	return 0, &sentencedError{cause: ErrJoinSyncIncomplete, message: fmt.Sprintf(
		"this node owns bucket %q in database %q since a membership change and has not copied it yet; %s still hold(s) it. "+
			"Creating it here would replace their copy with an empty one, so nothing was created. "+
			"Retry in a moment: the copy lands on its own ('nodes' shows the members)",
		bucket, database, strings.Join(holders, ", "))}
}

// liveHolders is the members of survey that hold a live copy of
// database/bucket: not of a database incarnation this node or any member knows
// dropped (G-6 (d), review F-1), and not of a bucket incarnation this node or
// any member recorded dropped (R8-C) — such a holder missed the drop.
func (c *Cluster) liveHolders(database, bucket string, survey catalogSurvey) ([]string, error) {
	dropped, err := c.s.droppedGenerations(database)
	if err != nil {
		return nil, fmt.Errorf("deciding whether bucket %q in database %q exists on another member: %w", bucket, database, err)
	}
	dropped = mergeGenerations(dropped, survey.dropped)
	var holders []string
	for _, holder := range survey.holders {
		if slices.Contains(dropped, holder.generation) || slices.Contains(survey.droppedIncarnations, holder.incarnation) {
			continue
		}
		droppedHere, err := c.s.dropRecorded(database, bucket, holder.incarnation)
		if err != nil {
			return nil, fmt.Errorf("deciding whether bucket %q in database %q exists on another member: %w", bucket, database, err)
		}
		if !droppedHere {
			holders = append(holders, holder.member)
		}
	}
	return holders, nil
}

// catalogAnswerHook, when set, runs as this node answers a catalog question,
// and may block. A test holds the members' answers with it. Nil in production.
var catalogAnswerHook atomic.Pointer[func(nodeID string)]

type surveyEpochCtxKey struct{}

// withSurveyEpoch carries the epoch a create's or a drop's catalog question
// was asked at, from the door (fenceUnheldBucket) to applyAsOwner, which
// re-checks it inside the fence (R8-D, review M-4).
func withSurveyEpoch(ctx context.Context, epoch uint64) context.Context {
	return context.WithValue(ctx, surveyEpochCtxKey{}, epoch)
}

// surveyEpochFrom returns what withSurveyEpoch attached.
func surveyEpochFrom(ctx context.Context) (uint64, bool) {
	epoch, ok := ctx.Value(surveyEpochCtxKey{}).(uint64)
	return epoch, ok
}

// setMovedSinceSettled reports whether the replica set of key at factor on the
// ring of the settled floor differs from its set today, or that ring is no
// longer in the history: two ring lookups.
func (c *Cluster) setMovedSinceSettled(key placementKey, factor int) bool {
	settledRing, recorded := c.ringAt(c.settledFloorNow())
	if !recorded {
		return true
	}
	then, err := settledRing.Replicas(key.encoded, factor)
	if err != nil {
		return true
	}
	now, err := c.replicasAtFactor(key, factor)
	if err != nil {
		return true
	}
	return !slices.EqualFunc(then, now, func(a, b ring.Node) bool { return a.ID == b.ID })
}

// refuseOverUnheard is the create's and the drop's rule for members that did
// not answer: passed over while the name's set is unchanged since the settled
// floor, refused with retry otherwise.
func (c *Cluster) refuseOverUnheard(database, bucket string, factor int, survey catalogSurvey) error {
	if len(survey.unheard) == 0 {
		return nil
	}
	if !c.setMovedSinceSettled(placementOf(database, bucket), factor) {
		c.logger.Warn("deciding a bucket this node owns without hearing from every member whether one holds it; "+
			"its replica set has not changed since every member settled",
			"database", database, "bucket", bucket, "unanswered", survey.unanswered)
		return nil
	}
	return unheardMembersRefusal(database, bucket, survey.unheard, "Nothing was changed")
}

// unheardMembersRefusal is the retry for a bucket this node owns since a
// membership change while members it asked whether one holds the bucket have
// not answered (O-4): it names them, and the remove-node that ends the wait
// for one that is gone for good. done says what the refused request did.
func unheardMembersRefusal(database, bucket string, unheard []string, done string) error {
	verb := "has"
	if len(unheard) > 1 {
		verb = "have"
	}
	return &sentencedError{cause: ErrJoinSyncIncomplete, message: fmt.Sprintf(
		"this node owns bucket %q in database %q since a membership change and has not heard from every member whether one holds it: "+
			"%s %s not answered. %s. Retry once they answer; a member that is not coming back is removed with "+
			"'remove-node <node>' ('nodes' shows the members)",
		bucket, database, strings.Join(unheard, ", "), verb, done)}
}

// refuseDropBesideCopy is the drop's question at an owner that holds no copy
// (R8-D (c)): a member that holds a live copy means the bucket
// exists and this node has not copied it, and the drop is refused with retry;
// so is one an unheard member may hold. When every member answered that it
// holds none, the bucket is not found and nothing is logged. It returns the
// epoch it asked at.
func (c *Cluster) refuseDropBesideCopy(ctx context.Context, database, bucket string) (uint64, error) {
	return 0, c.refuseUnheldBesideCopy(ctx, database, bucket, unheldVerb{done: "dropped", noun: "drop"})
}

// refuseUnheldMutation is the question every mutation but a create and a drop
// meets at an owner that holds no copy (BUG-0685 for the alter, BUG-0686 for
// the rest), the drop's in the mutation's words: a name no member holds is
// NOT_FOUND, not the retryable "no local copy" a joiner that has yet to copy
// the bucket is told, which a driver retries until its deadline. Cost (rule 11):
// one survey round per miss, none on a held bucket, nothing per bucket.
func (c *Cluster) refuseUnheldMutation(ctx context.Context, op transport.Opcode, database, bucket string) error {
	verb := unheldVerb{done: "changed", noun: "change"}
	if op == transport.OpAlterBucket {
		verb.noun = "alter"
	}
	return c.refuseUnheldBesideCopy(ctx, database, bucket, verb)
}

// unheldVerb is how a refusal of a mutation at an owner that holds no copy
// names what was not done ("dropped") and the mutation ("drop").
type unheldVerb struct{ done, noun string }

func (c *Cluster) refuseUnheldBesideCopy(ctx context.Context, database, bucket string, verb unheldVerb) error {
	if c.pullPending(database, bucket) {
		return &sentencedError{cause: ErrJoinSyncIncomplete, message: fmt.Sprintf(
			"this node owns bucket %q in database %q and is copying it from its other holders now, so nothing was %s. "+
				"Retry in a moment, once the copy lands", bucket, database, verb.done)}
	}
	survey := c.surveyCatalog(ctx, ReplCatalogAsk{Database: database, Bucket: bucket})
	holders, err := c.liveHolders(database, bucket, survey)
	if err != nil {
		return err
	}
	if len(holders) > 0 {
		return &sentencedError{cause: ErrJoinSyncIncomplete, message: fmt.Sprintf(
			"this node owns bucket %q in database %q since a membership change and has not copied it yet; %s still hold(s) it. "+
				"Nothing was %s: a %s logged here would not reach the bucket this node is about to copy. "+
				"Retry in a moment: the copy lands on its own ('nodes' shows the members)",
			bucket, database, strings.Join(holders, ", "), verb.done, verb.noun)}
	}
	if err := c.refuseOverUnheard(database, bucket, c.effectiveRF(placementOf(database, bucket)), survey); err != nil {
		return err
	}
	return bucketNotFoundErr(database, bucket)
}

// recheckUnheldInFence re-checks, inside the bucket's fence and before
// anything is opened or logged, the local facts a create's or a drop's
// question was answered on (R8-D (a), review M-4): no pull of the name is
// pending, and the membership epoch is the one the question was asked at. No
// question is asked here.
func (c *Cluster) recheckUnheldInFence(ctx context.Context, op transport.Opcode, key placementKey, database, bucket string) error {
	if c.pullPending(database, bucket) {
		return createAwaitingCopy(database, bucket)
	}
	asked, ok := surveyEpochFrom(ctx)
	_, epoch, err := c.ownerView(key)
	if err != nil {
		return err
	}
	if !ok || asked != epoch {
		// A join adopted since the question is the join freeze's to answer:
		// its refusal names the joiner and says when schema changes resume.
		if paused := c.refuseSchemaChange(op, epoch); paused != nil {
			return paused
		}
		return &sentencedError{cause: ErrJoinSyncIncomplete, message: fmt.Sprintf(
			"bucket %q in database %q: this node's membership changed while it asked the members about the bucket, "+
				"so nothing was changed. Retry the request", bucket, database)}
	}
	return nil
}
