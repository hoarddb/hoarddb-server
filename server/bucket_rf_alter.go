package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The factor alter: alter bucket Sessions { replication_factor: 5 }
// (bucket-replication-rev8-spec rev 9.1, §3 to §6; build doc
// bucket-replication-g1g2-build.md). The alter is an entry of the owner's term,
// logged and shipped on the owner's stream to the union of the bucket's sets
// before and after the change (§5.1).
//
// The factor is held in three places and the alter writes all three, in this
// order and inside the bucket's exclusive fence: the catalog entry, in the
// apply's batch; then, once the batch has committed, the bucket's record
// (bucketRec.replicationFactor), which a snapshot ships and the listings read;
// then the registry (Cluster.bucketFactors), which places and routes. An apply
// that left the record at the old factor would compile and route correctly and
// ship the old factor to every puller (§3.1, RF-2). On the owner the awaiting
// map is published before all three (§5.2 step 5).
//
// The handler decides the owner's path or a member's by the entry's origin, as
// its log records it, never by the ring: an `own` alter replayed at start takes
// the owner's path even after a membership change (§5.2).

// decodeAlter reads an alter's payload.
func decodeAlter(payload []byte) (AlterBucketRequest, error) {
	var req AlterBucketRequest
	if err := bson.Unmarshal(payload, &req); err != nil {
		return AlterBucketRequest{}, fmt.Errorf("decoding the alter of a bucket's replication factor: %w", err)
	}
	return req, nil
}

// stampAlter is the owner's stamp of an alter, made inside the bucket's
// exclusive fence once fenceAsSettledOwner has passed, and before anything of
// the alter is addressed or logged (spec §5.2 step 3). previous_factor is the
// effective factor of the bucket's record read under this fence, epoch the
// one fenceAsSettledOwner returned, and generation the record's database
// generation; a client's values for the three are replaced. An alter that
// changes nothing — the bucket declared the factor asked for — is answered
// here and logged nowhere (R9). The factor asked for is stamped too, so every
// reader of the logged payload reads one encoding of it.
func (c *Cluster) stampAlter(entry ReplEntry, epoch uint64) (ReplEntry, *ReplResponse, error) {
	req, err := decodeAlter(entry.Payload)
	if err != nil {
		return entry, nil, err
	}
	facts, held := c.s.peekSettled(req.Database, req.Name)
	if !held || facts.id == 0 {
		return entry, nil, c.missingBucketErr(req.Database, req.Name)
	}
	if facts.replicationFactor != 0 && facts.replicationFactor == req.ReplicationFactor {
		answer, err := alterAnswer(AlterBucketResponse{Success: true, ReplicationFactor: req.ReplicationFactor, PreviousFactor: req.ReplicationFactor,
			Message: fmt.Sprintf("bucket %q already asks for %s; nothing changed.", req.Name, copiesOf(req.ReplicationFactor))})
		return entry, answer, err
	}
	previous := facts.replicationFactor
	if previous == 0 {
		previous = int32(c.rf)
	}
	// R7 and R8 again, under this fence: an alter or a membership change
	// since the prechecks can have changed what they read.
	if err := c.refuseUnfinishedPass(req.Name, epoch); err != nil {
		return entry, nil, err
	}
	if err := c.refusePendingCopy(req, previous); err != nil {
		return entry, nil, err
	}
	stamped, err := stampPayload(entry.Payload, bson.D{
		{Key: "replication_factor", Value: req.ReplicationFactor},
		{Key: "previous_factor", Value: previous},
		{Key: "epoch", Value: int64(epoch)},
		{Key: "generation", Value: facts.generation},
	})
	if err != nil {
		return entry, nil, fmt.Errorf("stamping the alter of bucket %q with its factor, epoch and generation: %w", req.Name, err)
	}
	entry.Payload = stamped
	return entry, nil, nil
}

// The alter's refusals before it is logged (spec §4.3). R2 and R3 are the
// shape predicate the create shares, in the alter's own words, answered as a
// success: false body, which the same request meets again until an operator
// acts; R4 is UNAVAILABLE, retried with back-off. A refused alter is in no log
// and can never be replayed.

// errAlterAwaitsPlacementPass is under R7 (spec §4.3): the bucket's owner has
// not finished recording which buckets the last membership change moved, and
// an alter waits for that. It reaches the wire as UNAVAILABLE: retry.
var errAlterAwaitsPlacementPass = errors.New("the owner's placement pass for its membership epoch has not finished")

// errAlterCopyPending is under R8 (spec §4.3, owner R9-Q1): the alter would
// place a member whose copy from an earlier increase is still pending. It
// reaches the wire as UNAVAILABLE: retry once the copy has landed.
var errAlterCopyPending = errors.New("a member the alter places is still copying the bucket from an earlier alter")

// refuseUnfinishedPass is R7: an alter is refused while this node's placement
// pass for epoch has not finished (the pass half of mayBeUnsettled). One
// atomic load. No time is promised: none has been measured (A F-11).
func (c *Cluster) refuseUnfinishedPass(bucket string, epoch uint64) error {
	if epoch <= InitialEpoch || c.passFinishedAt.Load() >= epoch {
		return nil
	}
	return &sentencedError{cause: errAlterAwaitsPlacementPass, message: fmt.Sprintf(
		"bucket %q cannot change its replication_factor yet: %s is still recording which buckets the last membership change moved (epoch %d). "+
			"Run the alter again.", bucket, c.self.ID, epoch)}
}

// refusePendingCopy is R8 as narrowed (owner R9-Q1): an alter is refused when
// the set it asks for places a member this node holds an awaiting record for,
// whose copy from an earlier increase is still pending. current is the
// bucket's factor now, the one that earlier alter set. A record matches as it
// does for rule F5, by the copy's term and generation (G3 build doc
// OQ-G3-1): one of another incarnation or term is inert, and refuses
// nothing. One atomic load when nothing is pending anywhere, one map lookup
// otherwise.
func (c *Cluster) refusePendingCopy(req AlterBucketRequest, current int32) error {
	key := placementOf(req.Database, req.Name)
	match, err := c.awaitingMatchOfCopy(req.Database, req.Name)
	if err != nil {
		return err
	}
	awaiting := c.awaitingMatching(key, match)
	if len(awaiting) == 0 {
		return nil
	}
	walk, err := c.replicasAtFactor(key, int(max(current, req.ReplicationFactor)))
	if err != nil {
		return fmt.Errorf("placing bucket %q to judge the members it awaits: %w", req.Name, err)
	}
	for position, member := range walk {
		if !slices.Contains(awaiting, member.ID) || position >= int(req.ReplicationFactor) {
			continue
		}
		return &sentencedError{cause: errAlterCopyPending, message: fmt.Sprintf(
			"bucket %q is still copying to %s from its last replication_factor change (to %d), and replication_factor %d keeps %s in its set. "+
				"Run 'show buckets' on %s and retry when the row reads %d/%d; run 'describe bucket %s' on %s to see why its copy has not landed; "+
				"or lower replication_factor to %d to stop waiting for %s.",
			req.Name, member.ID, current, req.ReplicationFactor, member.ID, c.self.ID, current, current, req.Name, member.ID, position, member.ID)}
	}
	return nil
}

// alterShapeRefusal is R2 or R3 for an alter, in its own words, or "". A
// factor below one is no number of copies (spec §4.1: an integer of 1 or
// more). It reads only the factor asked for: a decrease has no shape refusal
// of its own, and R11 judges it on the owner (G5 removed R10).
func (s *Server) alterShapeRefusal(req AlterBucketRequest) string {
	if req.ReplicationFactor < 1 {
		return fmt.Sprintf("replication_factor of bucket %q must be 1 or more: it is the number of copies the cluster keeps, and %d asks for none. "+
			"Run the alter again with replication_factor 1 or more.", req.Name, req.ReplicationFactor)
	}
	shape := s.factorShapeOf(int(req.ReplicationFactor), "")
	switch {
	case shape.standalone:
		return fmt.Sprintf("bucket %q cannot be raised to replication_factor %d: this server is running standalone, so there is no cluster "+
			"to place a second copy on. Form a cluster as the replication page of the user docs describes, then run the alter once 'nodes' shows the members.",
			req.Name, req.ReplicationFactor)
	case shape.members > 0:
		return fmt.Sprintf("replication_factor %d is larger than the cluster's %d members: bucket %q would have no node for its %s. "+
			"Add members first ('join' on each new node, then 'nodes' to confirm), or use replication_factor %d.",
			req.ReplicationFactor, shape.members, req.Name, missingCopies(shape.members+1, int(req.ReplicationFactor)), shape.members)
	}
	return ""
}

// missingCopies names the copies from first to last as a sentence does:
// "4th copy", "4th and 5th copies".
func missingCopies(first, last int) string {
	ordinals := make([]string, 0, last-first+1)
	for copyNumber := first; copyNumber <= last; copyNumber++ {
		ordinals = append(ordinals, ordinal(copyNumber))
	}
	if len(ordinals) == 1 {
		return ordinals[0] + " copy"
	}
	return joinNames(ordinals) + " copies"
}

// ordinal is n written as an English ordinal: 1st, 2nd, 3rd, 4th, 11th, 21st.
func ordinal(n int) string {
	suffix := "th"
	if n%100 < 11 || n%100 > 13 {
		switch n % 10 {
		case 1:
			suffix = "st"
		case 2:
			suffix = "nd"
		case 3:
			suffix = "rd"
		}
	}
	return fmt.Sprintf("%d%s", n, suffix)
}

// refuseAlterWithoutFeature is R4: an alter is refused while a member that
// has gossiped does not advertise bucket_rf_alter, at the node the request
// reaches and at the owner (spec §4.3, §11.2). The owner's half protects
// against an older member only; against an older owner only the coordinator's
// half does, and only while the coordinator has heard that owner gossip
// (BUG-0561). A member never heard from is not refused (owner R9-Q4).
func (c *Cluster) refuseAlterWithoutFeature(entry ReplEntry) error {
	if transport.Opcode(entry.Op) != transport.OpAlterBucket {
		return nil
	}
	member := c.memberWithout(featureBucketRFAlter)
	if member == "" {
		return nil
	}
	return &sentencedError{cause: errMemberLacksFeature, message: fmt.Sprintf(
		"replication_factor of bucket %q cannot be changed while member %s runs a version that does not change factors: "+
			"it would keep placing the bucket at %d and the two nodes would disagree about which nodes hold it. "+
			"Upgrade %s, wait for 'nodes' to show it active, then run the alter again.",
		entry.Bucket, member, c.effectiveRF(entryPlacement(entry)), member)}
}

// alterTargets is the bucket's set at the factor an alter asks for, without
// this node.
func (c *Cluster) alterTargets(entry ReplEntry, key placementKey) ([]ring.Node, error) {
	req, err := decodeAlter(entry.Payload)
	if err != nil {
		return nil, err
	}
	set, err := c.replicasAtFactor(key, int(req.ReplicationFactor))
	if err != nil {
		return nil, fmt.Errorf("placing bucket %q at the factor its alter asks for: %w", entry.Bucket, err)
	}
	return nodesNotIn(set, []ring.Node{c.self}), nil
}

// alterPrecheck is the owner's ask before an alter is logged, beside the
// create's (bucketRFPrecheck): a body for R2 and R3, an error for R4, R7, R8
// and R11. It runs before the bucket's fence, so R11's exchange holds no
// write of the bucket (spec §8.2); R7 and R8 are judged again inside the
// fence (stampAlter).
func (c *Cluster) alterPrecheck(ctx context.Context, entry ReplEntry) (ReplResponse, bool, error) {
	if transport.Opcode(entry.Op) != transport.OpAlterBucket {
		return ReplResponse{}, false, nil
	}
	req, err := decodeAlter(entry.Payload)
	if err != nil {
		return ReplResponse{}, false, err
	}
	if reason := c.s.alterShapeRefusal(req); reason != "" {
		answer, err := alterAnswer(AlterBucketResponse{Success: false, ReplicationFactor: req.ReplicationFactor, Message: reason})
		if err != nil {
			return ReplResponse{}, false, err
		}
		return *answer, true, nil
	}
	if err := c.refuseAlterWithoutFeature(entry); err != nil {
		return ReplResponse{}, false, err
	}
	if err := c.refuseUnfinishedPass(req.Name, c.Epoch()); err != nil {
		return ReplResponse{}, false, err
	}
	current := int32(c.effectiveRF(entryPlacement(entry)))
	if err := c.refusePendingCopy(req, current); err != nil {
		return ReplResponse{}, false, err
	}
	return ReplResponse{}, false, c.refuseUncoveredDecrease(ctx, req, current)
}

// errDecreaseNotCovered is R11 (spec §4.3, §8.2): a survivor of a decrease
// has not shown that it holds everything a copy the decrease releases holds.
// It reaches the wire as UNAVAILABLE: retry once the survivor has caught up.
var errDecreaseNotCovered = errors.New("a survivor of the decrease has not shown that it holds what the copies it releases hold")

// refuseUncoveredDecrease is R11: before a decrease from current is logged,
// the owner asks every other member of the bucket's set at current what its
// copy is (askCopies, one frame each, bounded by wcTimeout as the take-over's
// question is) and reads its own copy under the bucket's fence, which it
// takes and releases here. The decrease is refused unless every survivor —
// the owner among them — answered, holds the bucket in the owner's term, is
// not marked outside, owes no apply, and covers every demoted copy that
// answered holding one (coversForRelease, the release's own rule). A demoted
// member that does not answer refuses nothing: its copy is released on its
// own node, later, by the same rule, which is the guarantee; this exchange is
// the operator's early answer. An unanswered survivor is behind (rev 3 F-12).
func (c *Cluster) refuseUncoveredDecrease(ctx context.Context, req AlterBucketRequest, current int32) error {
	if req.ReplicationFactor >= current {
		return nil
	}
	key := placementOf(req.Database, req.Name)
	before, err := c.replicasAtFactor(key, int(current))
	if err != nil {
		return fmt.Errorf("placing bucket %q to ask its members before lowering it: %w", req.Name, err)
	}
	// The ring's prefix property: the set at the lower factor is the first
	// members of the walk, and the decrease demotes the rest (spec §8.1).
	kept := min(int(req.ReplicationFactor), len(before))
	survivors, demoted := before[:kept], before[kept:]
	ref := ReplBucketRef{Database: req.Database, Bucket: req.Name}
	own, err := c.answerCopy(ref)
	if err != nil {
		return fmt.Errorf("reading this node's copy of bucket %q before lowering it: %w", req.Name, err)
	}
	copies := map[string]ReplCopy{c.self.ID: own}
	for _, member := range before {
		if member.ID == c.self.ID {
			continue
		}
		answer, err := c.askOne(ctx, member, heldBucket{database: req.Database, bucket: req.Name}, c.wcTimeout)
		if err != nil {
			c.logger.Debug("a member did not answer before a decrease", "database", req.Database, "bucket", req.Name, "member", member.ID, "error", err)
			continue
		}
		copies[member.ID] = answer
	}
	for _, survivor := range survivors {
		answer, answered := copies[survivor.ID]
		if answered && survivorCovers(answer, own, demoted, copies) {
			continue
		}
		return c.decreaseNotCovered(req, current, survivor.ID, nodeIDs(demoted))
	}
	return nil
}

// survivorCovers reports whether a survivor's copy, answered, may keep the
// bucket once the demoted copies go: held in the owner's term, not outside,
// owing nothing, and covering every demoted copy that answered holding one.
func survivorCovers(survivor, owner ReplCopy, demoted []ring.Node, copies map[string]ReplCopy) bool {
	if !survivor.Holds || survivor.Term != owner.Term || survivor.Outside != 0 || survivor.Owes {
		return false
	}
	for _, member := range demoted {
		if released, answered := copies[member.ID]; answered && released.Holds && !coversForRelease(survivor, released) {
			return false
		}
	}
	return true
}

// decreaseNotCovered is R11's refusal, naming the survivor and the copies the
// decrease would release.
func (c *Cluster) decreaseNotCovered(req AlterBucketRequest, current int32, survivor string, demoted []string) error {
	verb := "holds"
	if len(demoted) > 1 {
		verb = "hold"
	}
	message := fmt.Sprintf("bucket %q cannot be lowered from replication_factor %d to %d: %s has not shown that it holds everything %s %s "+
		"(its copy is behind, or it did not answer). Run 'show buckets' on %s and retry when the row reads %d/%d.",
		req.Name, current, req.ReplicationFactor, survivor, joinNames(demoted), verb, c.self.ID, current, current)
	if survivor != c.self.ID {
		message += fmt.Sprintf(" If %s is not coming back, run 'remove-node %s' first.", survivor, survivor)
	}
	return &sentencedError{cause: errDecreaseNotCovered, message: message}
}

// alterConcernSet is the members whose copy an alter's write concern counts:
// the bucket's set at the smaller of its two factors, without this node
// (spec §9; review A F-4). They are the copies that keep the bucket across the
// change and move their lineage past the alter. A member the increase newly
// places writes no copy, and one a decrease demotes does not move its lineage,
// so neither may count toward the alter's majority, though both are sent it:
// at the union, an increase from 2 to 5 could be answered on the owner and
// two members that hold nothing. The result is never nil, so an empty set
// counts no member.
func (c *Cluster) alterConcernSet(entry ReplEntry, key placementKey) ([]ring.Node, error) {
	set, err := c.replicasAtFactor(key, int(min(previousFactorOf(entry.Payload), alteredFactorOf(entry.Payload))))
	if err != nil {
		return nil, fmt.Errorf("placing the copies the alter of bucket %q is confirmed by: %w", entry.Bucket, err)
	}
	keepers := make([]ring.Node, 0, len(set))
	for _, node := range set {
		if node.ID != c.self.ID {
			keepers = append(keepers, node)
		}
	}
	return keepers, nil
}

// alterAnswer is an alter's answer as the owner's write path returns it.
func alterAnswer(resp AlterBucketResponse) (*ReplResponse, error) {
	payload, err := bson.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("encoding the answer to an alter of bucket replication: %w", err)
	}
	return &ReplResponse{Opcode: int32(transport.OpAlterBucket), Payload: payload}, nil
}

// handleAlterBucket applies an alter: the owner's path for an entry this node
// originated and for one nothing stamped, a member's for one another node's
// stream carried here. Nothing stamps an alter on a node that does not route:
// one with no cluster, or a cluster node alone on its ring, which logs nothing
// and reaches here through fencedDispatch (BUG-0623). Either is the bucket's
// owner at every factor, and the member's path would read the missing stamp's
// epoch 0 as a ring it has lost and mark the only copy outside its set.
func handleAlterBucket(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	req, err := decodeAlter(frame.Payload)
	if err != nil {
		return errorResponse(frame, err)
	}
	var resp *AlterBucketResponse
	if s.cluster == nil || !generationsTrusted(ctx) || lineageFrom(ctx).originated {
		resp, err = s.alterAsOwner(ctx, req)
	} else {
		resp, err = s.alterAsMember(ctx, req)
	}
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpAlterBucket, resp)
}

// alterAsOwner is the owner's apply of an alter it stamped (spec §5.2 steps 4
// and 5): in the apply's batch, the catalog entry rewritten and, for an
// increase, one awaiting record per member it newly places; once the batch
// has committed, the awaiting map, then the record, then the registry, all
// inside the fence the caller holds exclusively, so nothing here takes it
// again (build doc §2 traps 2 and 6).
func (s *Server) alterAsOwner(ctx context.Context, req AlterBucketRequest) (*AlterBucketResponse, error) {
	bucket, unlock, err := s.lockBucket(req.Database, req.Name)
	if err != nil {
		return nil, err
	}
	defer unlock()
	declared := bucket.rec.replicationFactor.Load()
	// On a cluster that routes, the factor before is the owner's stamp, read
	// from this record under the same fence and logged with the entry, so a
	// replay reads the number the live apply did (spec §5.2 step 3). A node
	// that does not route logs nothing, so it stamps here, under the fence
	// fencedDispatch holds, what stampAlter would: the factor from its
	// record, its current epoch and the record's generation.
	previous := req.PreviousFactor
	if s.cluster == nil || !generationsTrusted(ctx) {
		previous = int32(s.effectiveFactorOf(int(declared)))
		req.PreviousFactor, req.Generation = previous, bucket.rec.generation.Load()
		if s.cluster != nil {
			req.Epoch = s.cluster.Epoch()
		}
	}
	if declared != 0 && declared == req.ReplicationFactor {
		// Reached only where nothing stamped the alter first: a node that
		// does not route.
		return &AlterBucketResponse{Success: true, ReplicationFactor: req.ReplicationFactor, PreviousFactor: previous,
			Message: fmt.Sprintf("bucket %q already asks for %s; nothing changed.", req.Name, copiesOf(req.ReplicationFactor))}, nil
	}
	rewrite := factorRewrite{req: req, info: applyInfoFrom(ctx, s), catalog: true}
	key := placementOf(req.Database, req.Name)
	// placing is the alter as this apply places it: at its own epoch, or at
	// the current one when that epoch's ring has left the history (OQ-G3-2).
	placing, ringMissing := req, false
	if s.cluster != nil {
		placing.Epoch, ringMissing = s.cluster.ownerPlacingEpoch(req.Epoch)
		// The supersede (owner R9-Q1): the records of members the new factor
		// does not place are deleted in this apply's batch. They are found in
		// the apply's own snapshot, not the map, which a replay at start runs
		// before.
		placedAt, err := s.cluster.setAt(placing.Epoch, key, int(req.ReplicationFactor))
		if err != nil {
			return nil, err
		}
		rewrite.keeps = placedAt
	}
	if s.cluster != nil && req.ReplicationFactor > previous {
		placed, err := s.cluster.newlyPlaced(placing, previous)
		if err != nil {
			return nil, err
		}
		rewrite.placed = placed
		rewrite.awaiting = awaitingRecord{AlterSeq: int64(lineageFrom(ctx).originSeq), Generation: req.Generation, Since: time.Now().Unix()}
	}
	applied, failed, err := s.rewriteFactor(ctx, bucket, &rewrite)
	if failed != nil || err != nil {
		return failed, err
	}
	if !applied {
		// A replay of an alter the bucket holds already: the store passed it
		// over (store.Gate), and nothing of it is applied again.
		return &AlterBucketResponse{Success: true, ReplicationFactor: req.ReplicationFactor, PreviousFactor: previous,
			Message: fmt.Sprintf("bucket %q holds this change of its replication_factor already; nothing was applied again.", req.Name)}, nil
	}
	if ringMissing {
		s.logger.Warn("an alter of a bucket's replication factor replayed at start names a membership epoch whose ring this node no longer remembers; "+
			"it is applied, and the copies it newly places are taken from the current ring",
			"database", req.Database, "bucket", req.Name, "missing_epoch", req.Epoch, "current_epoch", placing.Epoch,
			"replication_factor", req.ReplicationFactor)
	}
	if len(rewrite.superseded) > 0 {
		s.cluster.dropAwaiting(key, rewrite.superseded)
	}
	if len(rewrite.placed) > 0 {
		s.cluster.publishAwaiting(key, rewrite.placed, rewrite.awaiting)
		s.cluster.listAlterPendingCopies(req.Database, req.Name)
	}
	bucket.rec.replicationFactor.Store(req.ReplicationFactor)
	s.registerBucketFactor(req.Database, req.Name, req.ReplicationFactor)
	s.audit.Log(AuditEntry{Event: EventSchemaAlter, Target: "bucket:" + req.Database + "." + req.Name,
		Result: fmt.Sprintf("ok: replication_factor %d (was %d)", req.ReplicationFactor, previous)})
	return s.alterAnswerOf(placing, previous)
}

// ownerPlacingEpoch is the epoch whose ring places the owner's apply of an
// alter logged under logged: that epoch, or the current one, with ringMissing
// true, when logged's ring has left the membership history (OQ-G3-2). Only a
// replay at start meets that: the live owner stamps the epoch it is on. The
// current ring is the set routing uses now, which rule F5 filters and an empty
// member places itself by (Server.alterOfNoCopy), so the members it newly
// places are the ones the stream sends nothing of the bucket but the alter.
// Failing instead would leave the catalog at the old factor while the log
// holds the alter, for ever.
func (c *Cluster) ownerPlacingEpoch(logged uint64) (epoch uint64, ringMissing bool) {
	if _, recorded := c.ringAt(logged); recorded || logged == c.Epoch() {
		return logged, false
	}
	return c.Epoch(), true
}

// alterAsMember is a member's apply of an alter its owner shipped (spec §5.3).
// It decides from its own copy and the entry alone, never from its registry,
// which a member that missed an earlier alter has wrong. A held copy rewrites
// its factor in its three stores whatever its row (§3.1): row 1, a copy the
// alter keeps, moves its lineage as any applied entry does; every other held
// copy is marked outside in the same batch and keeps its lineage where it was
// (rule F4), then is woken to the take-over, which copies it afresh if the new
// factor places it. A member that holds nothing writes or deletes its
// needs-a-copy record (alterOfNoCopy).
//
// The apply does not compare the payload's generation with the copy's
// (OQ-G3-3): the lineage already keeps an alter off a copy of another
// incarnation. A recreated bucket's create is above every entry of the bucket
// it replaced in one owner's log, so an old alter is at or below the copy's
// position in its term or of an older term, and is passed over (spec §5.1).
// The generation is read by rule F5's match and by the drops' prefix deletes.
func (s *Server) alterAsMember(ctx context.Context, req AlterBucketRequest) (*AlterBucketResponse, error) {
	answer := &AlterBucketResponse{Success: true, ReplicationFactor: req.ReplicationFactor, PreviousFactor: req.PreviousFactor}
	if !s.bucketStoreOpen(req.Database, req.Name) {
		if err := s.alterOfNoCopy(req); err != nil {
			return nil, err
		}
		return answer, nil
	}
	key := placementOf(req.Database, req.Name)
	row, missing := s.cluster.alterRowOf(req, key)
	if row == alterRingMissing {
		s.logger.Warn("an alter of a bucket's replication factor names a membership epoch whose ring this node no longer remembers; "+
			"its copy is marked outside its set and copied afresh from the owner if the new factor places it",
			"database", req.Database, "bucket", req.Name, "missing_epoch", missing, "replication_factor", req.ReplicationFactor)
	}
	bucket, unlock, err := s.lockBucket(req.Database, req.Name)
	if err != nil {
		return nil, err
	}
	defer unlock()
	rewrite := factorRewrite{req: req, info: applyInfoFrom(ctx, s), catalog: true}
	if row != alterKeeps {
		// The lineage stays where it was (F4). The receiver's rule was judged
		// under this fence (refuseByLineage), and the outside copy's exception
		// admits an alter of a term above the copy's, which the store's own
		// check would stop: the placement effect moves no position for it to
		// judge.
		rewrite.info.OriginSeq, rewrite.info.CheckLineage = 0, false
		s.cluster.marksWriteMu.Lock()
		defer s.cluster.marksWriteMu.Unlock()
		marks := s.cluster.recordedMarks(key).merged(placementMarks{Outside: s.cluster.Epoch()})
		rewrite.marks = &marks
	}
	applied, failed, err := s.rewriteFactor(ctx, bucket, &rewrite)
	if failed != nil || err != nil {
		return failed, err
	}
	if !applied {
		return answer, nil
	}
	if rewrite.marks != nil {
		s.cluster.publishPlacementMarks(key, *rewrite.marks)
	}
	bucket.rec.replicationFactor.Store(req.ReplicationFactor)
	s.registerBucketFactor(req.Database, req.Name, req.ReplicationFactor)
	// The demoted set (spec §8.3), once the batch has committed: a copy a
	// decrease takes out of its set is released by the rearm once its
	// survivors cover it (row 2); one an alter keeps or places again leaves
	// the set (rows 1 and 3). A copy that was outside the set before the
	// alter is a membership change's, and stays the sweep's.
	held := heldBucket{database: req.Database, bucket: req.Name}
	switch {
	case s.cluster.stillReplica(key):
		s.cluster.forgetDemoted(held)
	case s.cluster.demotedBy(req, key):
		s.cluster.noteDemoted(held)
	}
	if rewrite.marks != nil {
		s.cluster.wakeTakeOver(req.Database, req.Name)
	}
	return answer, nil
}

// demotedBy reports whether req is a decrease whose old set, at its epoch,
// placed this node: the copy is one the decrease demotes (spec §8.1). A ring
// that left the history says no, and the copy is left to the sweep.
func (c *Cluster) demotedBy(req AlterBucketRequest, key placementKey) bool {
	if req.ReplicationFactor >= req.PreviousFactor {
		return false
	}
	before, err := c.setAt(req.Epoch, key, int(req.PreviousFactor))
	return err == nil && nodeListed(before, c.self.ID)
}

// learnFactor is the backstop rule (spec §11.3, rules 1 and 2): this node's
// copy, whose record declares asked, follows an owner whose record declares
// learned, an alter of the factor this node never applied. Under the bucket's
// exclusive fence, in one batch, its catalog entry is rewritten to learned
// and, when this node's membership of the set on the current ring differs
// between the two factors or cannot be read, the copy is marked outside; once
// the batch has committed, the record, then the registry. Its lineage does not
// move: it holds what it held. The record is read again under the fence, and a
// factor that changed since the question is left to the next round. It
// reports whether it marked the copy. Both factors 0 mean inherit.
func (s *Server) learnFactor(held heldBucket, asked, learned int32) (bool, error) {
	key := placementOf(held.database, held.bucket)
	fence := s.acquireFence(key, fenceExclusive)
	defer fence.release()
	bucket, unlock, err := s.lockBucket(held.database, held.bucket)
	if err != nil {
		return false, err
	}
	defer unlock()
	if bucket.rec.replicationFactor.Load() != asked {
		return false, nil
	}
	c := s.cluster
	placedBefore, knownBefore := c.placedAt(key, s.effectiveFactorOf(int(asked)))
	placedAfter, knownAfter := c.placedAt(key, s.effectiveFactorOf(int(learned)))
	var marks *placementMarks
	if !knownBefore || !knownAfter || placedBefore != placedAfter {
		c.marksWriteMu.Lock()
		defer c.marksWriteMu.Unlock()
		merged := c.recordedMarks(key).merged(placementMarks{Outside: c.Epoch()})
		marks = &merged
	}
	batch := s.kv.NewBatch()
	defer batch.Discard()
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	if err := rewriteCatalogFactor(snap, batch, AlterBucketRequest{Database: held.database, Name: held.bucket, ReplicationFactor: learned}); err != nil {
		return false, err
	}
	if marks != nil {
		if err := putPlacementMarks(batch, key, *marks); err != nil {
			return false, err
		}
	}
	if err := batch.Commit(true); err != nil {
		return false, fmt.Errorf("writing the replication factor of bucket %q in database %q learned from its owner: %w", held.bucket, held.database, err)
	}
	if marks != nil {
		c.publishPlacementMarks(key, *marks)
	}
	bucket.rec.replicationFactor.Store(learned)
	s.registerBucketFactor(held.database, held.bucket, learned)
	s.logger.Info("learned a bucket's replication factor from its owner, an alter this node never applied",
		"database", held.database, "bucket", held.bucket, "replication_factor", learned, "was", asked, "marked_outside", marks != nil)
	return marks != nil, nil
}

// placedAt reports whether this node is in the set of the bucket placed by key
// at factor on the current ring, and whether the ring could say.
func (c *Cluster) placedAt(key placementKey, factor int) (placed, known bool) {
	set, err := c.replicasAtFactor(key, factor)
	if err != nil {
		return false, false
	}
	return nodeListed(set, c.self.ID), true
}

// alterRow is the row of spec §5.3's table a held copy takes for an alter.
type alterRow int

const (
	// alterKeeps is row 1: the copy is in the set at both factors at the
	// alter's epoch and at every epoch since, and is not marked outside.
	alterKeeps alterRow = iota
	// alterMarks is rows 2 and 3 and the term table's outside row: the copy is
	// marked outside and keeps its lineage.
	alterMarks
	// alterRingMissing is row 6: a ring of the alter's epochs is missing from
	// the membership history, so the copy is treated as row 3.
	alterRingMissing
)

// alterRowOf is rule F4's marking function (spec §6.2) for this node's held
// copy of the bucket placed by key: row 1 only when keepsCopyAcross finds the
// copy in set(El, p) and in set(e, n) for every e in [El, Ecur] and the copy
// is not marked outside; row 6, with the epoch whose ring is missing, when a
// ring of those epochs cannot be read; otherwise a mark. It reads the entry's
// factors and epoch and the copy's marks, never the registry.
func (c *Cluster) alterRowOf(req AlterBucketRequest, key placementKey) (alterRow, uint64) {
	keeps, missing, known := c.keepsCopyAcross(req)
	switch {
	case !known:
		return alterRingMissing, missing
	case keeps && c.placementMarksOf(key).Outside == 0:
		return alterKeeps, 0
	}
	return alterMarks, 0
}

// factorRewrite is one apply of an alter: whether it rewrites the catalog
// entry, the awaiting records it writes beside it, one per member in placed,
// each with awaiting's fields and the copy's term, and, on the owner, the
// set at the new factor: the apply deletes the awaiting record of every member
// not in it, and lists them in superseded. On a member, marks, when set, are
// the copy's placement marks the batch writes (rule F4).
type factorRewrite struct {
	req        AlterBucketRequest
	info       store.ApplyInfo
	catalog    bool
	placed     []string
	awaiting   awaitingRecord
	keeps      []ring.Node
	superseded []string
	marks      *placementMarks
}

// alterBatchHook, when set, is handed the batch of every alter's apply and
// returns the batch the apply writes to: a test counts what one alter writes.
// Nil in production.
var alterBatchHook atomic.Pointer[func(storage.KVBatch) storage.KVBatch]

// rewriteFactor commits one apply of an alter, with the bucket's applied
// sequence and lineage as the rewrite's info says. The catalog entry is the
// one the apply's snapshot holds, changed in one field and re-encoded with
// encodeCatalogEntry, so no field another feature wrote is lost (BUG-0128).
// The first result says whether the batch committed: a replay of an entry at
// or below the bucket's applied mark commits nothing and is no failure. A
// store failure is the second result, as the answer.
func (s *Server) rewriteFactor(ctx context.Context, bucket *inMemoryBucket, rewrite *factorRewrite) (bool, *AlterBucketResponse, error) {
	req := rewrite.req
	applied, applyErr := store.Apply(store.ApplyRequest{
		KV: bucket.kv, Slot: bucket.slot, DatabaseID: bucket.databaseID, ID: bucket.id.Load(), Keyed: false, Info: rewrite.info, Owed: bucket.owedApplies(),
		Op: func(snap storage.KVSnapshot, st *store.BucketState, b storage.KVBatch) error {
			if hook := alterBatchHook.Load(); hook != nil {
				b = (*hook)(b)
			}
			if rewrite.catalog {
				if err := rewriteCatalogFactor(snap, b, req); err != nil {
					return err
				}
			}
			key := placementOf(req.Database, req.Name)
			if rewrite.marks != nil {
				if err := putPlacementMarks(b, key, *rewrite.marks); err != nil {
					return err
				}
			}
			rewrite.awaiting.Term = int64(st.Term)
			if rewrite.keeps != nil {
				superseded, err := supersedeAwaiting(snap, b, key, rewrite.keeps)
				if err != nil {
					return err
				}
				rewrite.superseded = superseded
			}
			return writeAwaiting(b, key, rewrite.placed, rewrite.awaiting)
		},
	})
	s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Name, b: bucket, op: "ALTER BUCKET"}, applyErr)
	if applyErr != nil {
		failed, err := alterApplyFailure(req, applyErr)
		return false, failed, err
	}
	return applied, nil, nil
}

// rewriteCatalogFactor puts the bucket's catalog entry, as snap holds it, into
// b with the alter's factor.
func rewriteCatalogFactor(snap storage.KVSnapshot, b storage.KVBatch, req AlterBucketRequest) error {
	encoded, err := snap.Get(catalogKey(req.Database, req.Name))
	if err != nil {
		return fmt.Errorf("reading the catalog entry of bucket %q in database %q: %w", req.Name, req.Database, err)
	}
	var entry catalogEntry
	if err := bson.Unmarshal(encoded, &entry); err != nil {
		return fmt.Errorf("decoding the catalog entry of bucket %q in database %q: %w", req.Name, req.Database, err)
	}
	entry.database, entry.bucket = req.Database, req.Name
	entry.ReplicationFactor = req.ReplicationFactor
	if encoded, err = encodeCatalogEntry(entry); err != nil {
		return err
	}
	return b.Set(catalogKey(req.Database, req.Name), encoded)
}

// alterApplyFailure answers an alter whose apply failed: a store failure in
// the answer, as the declare answers one, and any other error as itself.
func alterApplyFailure(req AlterBucketRequest, applyErr error) (*AlterBucketResponse, error) {
	if errors.Is(applyErr, store.ErrApplyFailed) {
		return &AlterBucketResponse{Success: false, ReplicationFactor: req.ReplicationFactor, PreviousFactor: req.PreviousFactor,
			Message: failureMessage(fmt.Sprintf("changing the replication_factor of bucket %q failed", req.Name), applyErr)}, nil
	}
	return nil, applyErr
}

// newlyPlaced is the members an increase from previous places at the alter's
// epoch that previous does not: by the ring's prefix property, the nodes
// after the first previous of the bucket's walk (spec §7.1).
func (c *Cluster) newlyPlaced(req AlterBucketRequest, previous int32) ([]string, error) {
	key := placementOf(req.Database, req.Name)
	before, err := c.setAt(req.Epoch, key, int(previous))
	if err != nil {
		return nil, err
	}
	after, err := c.setAt(req.Epoch, key, int(req.ReplicationFactor))
	if err != nil {
		return nil, err
	}
	return nodeIDs(nodesNotIn(after, before)), nil
}

// alterAnswerOf is the owner's answer to an alter it applied (spec §4.2): the
// members an increase newly places, or a decrease un-places, at req's epoch
// (the one the alter was logged under, or ownerPlacingEpoch's current one),
// and the sentence for the case.
func (s *Server) alterAnswerOf(req AlterBucketRequest, previous int32) (*AlterBucketResponse, error) {
	answer := &AlterBucketResponse{Success: true, ReplicationFactor: req.ReplicationFactor, PreviousFactor: previous}
	if previous == req.ReplicationFactor {
		// An inheriting bucket asked for the factor it inherits: inherit
		// becomes declared (owner R9-Q7).
		answer.Message = fmt.Sprintf("bucket %q now declares %s (it inherited %d from this node's default); "+
			"a later change of the default no longer changes it.", req.Name, copiesOf(req.ReplicationFactor), previous)
		return answer, nil
	}
	if s.cluster == nil {
		answer.Message = fmt.Sprintf("bucket %q now asks for %s (was %d).", req.Name, copiesOf(req.ReplicationFactor), previous)
		return answer, nil
	}
	key := placementOf(req.Database, req.Name)
	before, err := s.cluster.setAt(req.Epoch, key, int(previous))
	if err != nil {
		return nil, err
	}
	after, err := s.cluster.setAt(req.Epoch, key, int(req.ReplicationFactor))
	if err != nil {
		return nil, err
	}
	owner := s.cluster.self.ID
	if len(nodesNotIn(after, before)) == 0 && len(nodesNotIn(before, after)) == 0 {
		// The cluster has fewer members than either factor asks for — one
		// alone on its ring, or a decrease to the member count — so every
		// member holds a copy at both, and none is added or released.
		holders := nodeIDs(after)
		answer.Message = fmt.Sprintf("bucket %q now asks for %s (was %d). No copy is added or deleted: %s every member the cluster has, "+
			"and %s a copy at both factors. Run 'show buckets' on %s.",
			req.Name, copiesOf(req.ReplicationFactor), previous, isOrAre(holders), holdOrHolds(holders), owner)
		return answer, nil
	}
	if req.ReplicationFactor > previous {
		answer.Pending = nodeIDs(nodesNotIn(after, before))
		answer.Message = fmt.Sprintf("bucket %q now asks for %s (was %d). %s will copy it; writes keep going to the %s. "+
			"Run 'show buckets' on %s (its owner) to watch the count reach %d/%d, and 'describe bucket %s' on %s to see why a copy has not started.",
			req.Name, copiesOf(req.ReplicationFactor), previous, joinNames(answer.Pending), holdingCopies(previous), owner,
			req.ReplicationFactor, req.ReplicationFactor, req.Name, answer.Pending[0])
		return answer, nil
	}
	answer.Releasing = nodeIDs(nodesNotIn(before, after))
	survivors := nodeIDs(after)
	answer.Message = fmt.Sprintf("bucket %q now asks for %s (was %d). %s once %s shown to hold everything %s; "+
		"its space is reclaimed in the background after that. Run 'show buckets' on %s.",
		req.Name, copiesOf(req.ReplicationFactor), previous, releasedCopies(answer.Releasing), isOrAre(survivors), releasedHold(answer.Releasing), owner)
	return answer, nil
}

// copiesOf is a count of copies as a sentence says it: "1 copy", "3 copies".
func copiesOf(count int32) string {
	if count == 1 {
		return "1 copy"
	}
	return fmt.Sprintf("%d copies", count)
}

// holdingCopies names the copies that hold a bucket now: "the 3 copies that
// hold it", "the 1 copy that holds it".
func holdingCopies(count int32) string {
	if count == 1 {
		return "1 copy that holds it"
	}
	return fmt.Sprintf("%d copies that hold it", count)
}

// releasedCopies names the copies a decrease deletes, as the sentence's
// subject.
func releasedCopies(releasing []string) string {
	if len(releasing) == 1 {
		return releasing[0] + "'s copy will be deleted"
	}
	return "the copies on " + joinNames(releasing) + " will be deleted"
}

// isOrAre names the survivors with the verb that agrees with them.
func isOrAre(survivors []string) string {
	if len(survivors) == 1 {
		return survivors[0] + " is"
	}
	return joinNames(survivors) + " are"
}

// holdOrHolds is the pronoun and verb for nodes that each hold a copy.
func holdOrHolds(nodes []string) string {
	if len(nodes) == 1 {
		return "it holds"
	}
	return "each holds"
}

// releasedHold is the pronoun for what the released copies hold.
func releasedHold(releasing []string) string {
	if len(releasing) == 1 {
		return "it holds"
	}
	return "they hold"
}

// joinNames joins names as a sentence lists them: "a", "a and b", "a, b and c".
func joinNames(names []string) string {
	switch len(names) {
	case 0:
		return ""
	case 1:
		return names[0]
	}
	return strings.Join(names[:len(names)-1], ", ") + " and " + names[len(names)-1]
}

// nodesNotIn is the nodes of set that are not in other, in set's order.
func nodesNotIn(set, other []ring.Node) []ring.Node {
	var outside []ring.Node
	for _, node := range set {
		if !nodeListed(other, node.ID) {
			outside = append(outside, node)
		}
	}
	return outside
}

// setAt is the bucket's set at factor in the ring of epoch, owner first: the
// membership history's ring, or the current ring for the current epoch.
func (c *Cluster) setAt(epoch uint64, key placementKey, factor int) ([]ring.Node, error) {
	if then, recorded := c.ringAt(epoch); recorded {
		return then.Replicas(key.encoded, factor)
	}
	if epoch == c.Epoch() {
		return c.replicasAtFactor(key, factor)
	}
	return nil, fmt.Errorf("the ring of epoch %d is not in this node's membership history", epoch)
}

// keepsCopyAcross is row 1's ring test (spec §5.3): whether this node is in
// the bucket's set at the old factor at the alter's epoch, and in its set at
// the new factor at that epoch and at every epoch since. known is false, with
// the epoch, when a ring of those epochs is not in the membership history
// (row 6).
func (c *Cluster) keepsCopyAcross(req AlterBucketRequest) (keeps bool, missing uint64, known bool) {
	key := placementOf(req.Database, req.Name)
	before, err := c.setAt(req.Epoch, key, int(req.PreviousFactor))
	if err != nil {
		return false, req.Epoch, false
	}
	keeps = nodeListed(before, c.self.ID)
	for epoch := req.Epoch; epoch <= c.Epoch(); epoch++ {
		after, err := c.setAt(epoch, key, int(req.ReplicationFactor))
		if err != nil {
			return false, epoch, false
		}
		keeps = keeps && nodeListed(after, c.self.ID)
	}
	return keeps, 0, true
}

// alterRefusalTags pair each alter refusal a retry clears (and, since
// BUG-0685, the one that is not found and never clears) with the tag its
// push response carries and the sentinel the coordinator rebuilds from it
// (BUG-0643).
var alterRefusalTags = []struct {
	tag      string
	sentinel error
}{
	{refusalAlterMemberLacksFeature, errMemberLacksFeature},
	{refusalAlterAwaitsPass, errAlterAwaitsPlacementPass},
	{refusalAlterCopyPending, errAlterCopyPending},
	{refusalAlterNotCovered, errDecreaseNotCovered},
	{refusalAlterBucketNotFound, ErrBucketNotFound},
}

// alterRefusalTag is the tag of the alter refusal err wraps, or "" for any
// other error.
func alterRefusalTag(err error) string {
	for _, refusal := range alterRefusalTags {
		if errors.Is(err, refusal.sentinel) {
			return refusal.tag
		}
	}
	return ""
}

// alterRefusalSentinel is the sentinel a coordinator gives the alter refusal
// tagged tag, or nil when tag is not one.
func alterRefusalSentinel(tag string) error {
	for _, refusal := range alterRefusalTags {
		if refusal.tag == tag {
			return refusal.sentinel
		}
	}
	return nil
}
