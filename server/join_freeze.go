package server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// No schema change while a node is joining (join-freeze spec, owner ruling 42).
// This file holds what the nodes tell each other about settling a membership
// change on gossip: each node's own Settled epoch, the best it has heard for
// every other member, and how far its own hand-off has got (spec §3.4, §8.2,
// §8.5).

// MemberSettled is one member's Settled epoch as a node relays it on gossip.
type MemberSettled struct {
	ID      string `bson:"id"`
	Settled uint64 `bson:"settled"`
}

// PlacementToCopy is a copy a node lacks after a membership change, which the
// operator makes with 'migrate <database> <bucket>' run on Node.
type PlacementToCopy struct {
	Database string `bson:"database" json:"database"`
	Bucket   string `bson:"bucket" json:"bucket"`
	Node     string `bson:"node" json:"node"`
}

// HandOffProgress is how far a node's hand-off of one membership change has
// got. Total is the buckets it held and owned before the change whose replica
// set the change altered — each moved bucket counted once, by its previous
// owner — and Done those every member of the new set has confirmed. GaveUp is
// set when its sweep gave up for now; WaitingOn names the members its last
// pass's hand-offs were refused by; Unconfirmed is, with auto-rebalance off,
// the first copies still to be made by hand, of UnconfirmedCount.
type HandOffProgress struct {
	Epoch            uint64            `bson:"epoch"`
	Total            uint64            `bson:"total"`
	Done             uint64            `bson:"done"`
	GaveUp           bool              `bson:"gave_up,omitempty"`
	WaitingOn        []string          `bson:"waiting_on,omitempty"`
	Unconfirmed      []PlacementToCopy `bson:"unconfirmed,omitempty"`
	UnconfirmedCount uint64            `bson:"unconfirmed_count,omitempty"`
}

// settledGossip is what this node says on gossip about settling: its own
// Settled epoch, the best it has for every other member, and its hand-off.
func (c *Cluster) settledGossip() (uint64, []MemberSettled, *HandOffProgress) {
	c.settleMu.Lock()
	relayed := make([]MemberSettled, 0, len(c.settledOf))
	for id, settled := range c.settledOf {
		relayed = append(relayed, MemberSettled{ID: id, Settled: settled})
	}
	c.settleMu.Unlock()
	slices.SortFunc(relayed, func(a, b MemberSettled) int { return strings.Compare(a.ID, b.ID) })
	return c.settled.Load(), relayed, c.ownHandOff()
}

// mergeSettled takes what member from said about settling: its own Settled
// epoch, the best it has for the others, and its hand-off. Every value only
// rises; a non-member's word, and anyone's word about this node, is ignored —
// this node's own value is its own (spec §8.2). A hand-off is replaced unless
// it is of an older change than the one held. A rise is persisted.
func (c *Cluster) mergeSettled(from string, own uint64, relayed []MemberSettled, handOff *HandOffProgress) {
	members := c.memberIDSet()
	if _, member := members[from]; !member || from == c.self.ID || c.removed() {
		return
	}
	raised := false
	c.settleMu.Lock()
	raise := func(id string, settled uint64) {
		if _, member := members[id]; !member || id == c.self.ID || settled <= c.settledOf[id] {
			return
		}
		if c.settledOf == nil {
			c.settledOf = make(map[string]uint64)
		}
		c.settledOf[id] = settled
		raised = true
	}
	raise(from, own)
	for _, heard := range relayed {
		raise(heard.ID, heard.Settled)
	}
	if handOff != nil {
		if held, known := c.handOffOf[from]; !known || held.Epoch <= handOff.Epoch {
			if c.handOffOf == nil {
				c.handOffOf = make(map[string]HandOffProgress)
			}
			if !known || !sameHandOff(held, *handOff) {
				c.handOffHeardStale.Store(true)
			}
			c.handOffOf[from] = *handOff
		}
	}
	c.settleMu.Unlock()
	if !raised {
		// A changed hand-off changes the refusal's progress and, with
		// auto-rebalance off, its 'migrate' commands: without this a node
		// that holds nothing of the change advised waiting for ever.
		if c.handOffHeardStale.Load() {
			c.recomputeJoinFreezeForCounts()
		}
		return
	}
	if err := c.writeMembershipRecord(); err != nil {
		c.logger.Warn("could not record the members' settled epochs; after a restart this node hears them again on gossip", "error", err)
	}
	c.recomputeJoinFreeze()
}

// sameHandOff reports whether two hand-offs say the same.
func sameHandOff(a, b HandOffProgress) bool {
	return a.Epoch == b.Epoch && a.Total == b.Total && a.Done == b.Done && a.GaveUp == b.GaveUp &&
		a.UnconfirmedCount == b.UnconfirmedCount && slices.Equal(a.WaitingOn, b.WaitingOn) &&
		slices.Equal(a.Unconfirmed, b.Unconfirmed)
}

// pruneSettled forgets what this node heard about nodes that are no longer
// members.
func (c *Cluster) pruneSettled(members []ring.Node) {
	c.settleMu.Lock()
	defer c.settleMu.Unlock()
	for id := range c.settledOf {
		if !nodeListed(members, id) {
			delete(c.settledOf, id)
		}
	}
	for id := range c.handOffOf {
		if !nodeListed(members, id) {
			delete(c.handOffOf, id)
		}
	}
}

// settledOfMembers is a copy of settledOf, which it trims, restricted to
// members: the record holds no value for a node that is not a member, even
// when it is written between an adoption and the pruning of the live map.
func settledOfMembers(settledOf map[string]uint64, members []ring.Node) map[string]uint64 {
	for id := range settledOf {
		if !nodeListed(members, id) {
			delete(settledOf, id)
		}
	}
	return copyEpochMap(settledOf)
}

// joinedAtSnapshot is the members' join epochs as last derived. The map is
// never changed in place.
func (c *Cluster) joinedAtSnapshot() map[string]uint64 {
	if joined := c.joinedAt.Load(); joined != nil {
		return *joined
	}
	return nil
}

// joinedAtChange is what an adoption derives the join epochs from.
type joinedAtChange struct {
	held          map[string]uint64
	before, after []ring.Node
	epoch         uint64
	// everyone is set when this node itself joins with the adoption: it was
	// bootstrapped, or the catch-up took it through an epoch that omitted it.
	everyone bool
	// floor is the lowest Settled epoch among the members after the change.
	floor uint64
}

// deriveJoinedAt is the join epochs after an adoption (join-freeze spec
// §8.3): a member new to this node's view joined at the adopted epoch — every
// member does, when this node is the one joining, since it needs every holder
// to hand off to it; the others keep theirs. A member that left is forgotten
// once every remaining member has settled past its join; until then it is
// kept, and keeps the freeze: undoing a join with 'remove-node' lifts the
// freeze only when the remaining members have handed back what they moved
// (spec §6.3, way out 3).
func deriveJoinedAt(change joinedAtChange) map[string]uint64 {
	derived := make(map[string]uint64, len(change.after))
	for _, member := range change.after {
		switch {
		case change.everyone || !nodeListed(change.before, member.ID):
			derived[member.ID] = change.epoch
		case change.held[member.ID] > 0:
			derived[member.ID] = change.held[member.ID]
		}
	}
	for id, joinedAt := range change.held {
		if !nodeListed(change.after, id) && joinedAt > change.floor {
			derived[id] = joinedAt
		}
	}
	return derived
}

// settleAtFounding records this node Settled at the epoch it founded the
// cluster at (owner ruling 52). A founder holds every copy that epoch placed,
// and no membership change has moved one, so it has nothing to hand off there.
// Without the record settledFloor reads 0 on a cluster that never changed
// membership, the first epoch's ring is never the one a create's or a drop's
// question compares (R8-D (b)), and a floor of 0 lies outside the history.
// A joiner never gets here: its first view is disputed by the members, and it
// is confirmed only by adopting its join.
func (c *Cluster) settleAtFounding(epoch uint64) {
	if !c.raiseSettled(epoch) {
		return
	}
	c.logger.Info("this node founded the cluster at this epoch and holds every copy the epoch placed: it is settled there", "epoch", epoch)
	c.recomputeJoinFreeze()
	c.wakeGossip()
}

// settledFloor is the lowest Settled epoch among members: this node's own for
// itself, the best heard for the others, 0 for one never heard.
func settledFloor(self string, own uint64, settledOf map[string]uint64, members []ring.Node) uint64 {
	floor := own
	for _, member := range members {
		if member.ID != self {
			floor = min(floor, settledOf[member.ID])
		}
	}
	return floor
}

// settledFloorNow is settledFloor over this node's state as it stands (review
// N-3): its own Settled, a copy of the best heard for every other member taken
// under settleMu, and the members read after releasing it — settleMu is a leaf
// and is never taken under viewMu. One map copy of O(members).
func (c *Cluster) settledFloorNow() uint64 {
	own := c.settled.Load()
	c.settleMu.Lock()
	heard := maps.Clone(c.settledOf)
	c.settleMu.Unlock()
	_, members := c.epochMembers()
	return settledFloor(c.self.ID, own, heard, members)
}

// waitedOn is a member the paused schema changes wait on, and why; Down is
// set when this node cannot reach it.
type waitedOn struct {
	Node  string
	State string
	Down  bool
}

// joinFreezeReport is whether schema changes are paused on this node's view of
// one membership epoch, and the facts the refusal and 'nodes' print (spec
// §8.4). Joiners are the members whose join has not settled; Stalled the
// members whose hand-off waits on another; WaitingOn the members they wait on
// and every member that has not settled for a reason of its own.
type joinFreezeReport struct {
	Epoch     uint64
	Paused    bool
	JoinEpoch uint64
	// SettledAt is the lowest Settled epoch among the members.
	SettledAt uint64
	Joiners   []string
	// Joining is the joiners still members; Departed those removed before
	// their join settled, whose change the members are still settling.
	Joining   []string
	Departed  []string
	WaitingOn []waitedOn
	Stalled   []string
	// Awaited is the members the stalled holders' hand-offs wait on.
	Awaited        []string
	Copied, Total  int
	CountsComplete bool
	// Migrate is the first copies still to make by hand, with auto-rebalance
	// off, and MigrateLeft how many there are in all, as the members said.
	Migrate     []PlacementToCopy
	MigrateLeft int
}

// joinFreezeInput is everything the judge reads, gathered by
// recomputeJoinFreeze.
type joinFreezeInput struct {
	epoch      uint64
	self       string
	members    []string
	joinedAt   map[string]uint64
	ownSettled uint64
	// settledOf is the best heard for each other member; a value for this
	// node is ignored, its own is ownSettled.
	settledOf map[string]uint64
	// handOff is each member's hand-off progress, this node's own included.
	handOff map[string]HandOffProgress
	// downFor is how long each member this node cannot reach has been down.
	downFor map[string]time.Duration
}

// settledOfMember is input's value for member id.
func (in joinFreezeInput) settledOfMember(id string) uint64 {
	if id == in.self {
		return in.ownSettled
	}
	return in.settledOf[id]
}

// judgeJoinFreeze decides whether schema changes are paused (spec §3.2): a
// member whose join epoch is above the lowest Settled epoch of the members has
// not settled, and while one has not, the cluster is paused. Every missing
// input reads as unsettled, so none lifts the pause early.
func judgeJoinFreeze(in joinFreezeInput) joinFreezeReport {
	report := joinFreezeReport{Epoch: in.epoch}
	floor := uint64(0)
	for index, member := range in.members {
		if settled := in.settledOfMember(member); index == 0 || settled < floor {
			floor = settled
		}
	}
	report.SettledAt = floor
	for id, joinedAt := range in.joinedAt {
		if joinedAt > floor {
			report.Joiners = append(report.Joiners, id)
			report.JoinEpoch = max(report.JoinEpoch, joinedAt)
		}
	}
	if len(report.Joiners) == 0 {
		return report
	}
	report.Paused = true
	// A node that is itself joining takes every member as joined (§8.3): the
	// joiner it names is itself.
	if len(report.Joiners) == len(in.members) && slices.Contains(report.Joiners, in.self) {
		report.Joiners = []string{in.self}
	}
	slices.Sort(report.Joiners)
	for _, joiner := range report.Joiners {
		if slices.Contains(in.members, joiner) {
			report.Joining = append(report.Joining, joiner)
		} else {
			report.Departed = append(report.Departed, joiner)
		}
	}

	var unsettled []string
	for _, member := range in.members {
		if in.settledOfMember(member) < report.JoinEpoch {
			unsettled = append(unsettled, member)
		}
	}
	waited := make(map[string]bool)
	awaited := make(map[string]bool)
	for _, member := range unsettled {
		progress, reported := in.handOff[member]
		_, down := in.downFor[member]
		if reported && progress.Epoch >= report.JoinEpoch && len(progress.WaitingOn) > 0 && !down {
			report.Stalled = append(report.Stalled, member)
			for _, awaitedMember := range progress.WaitingOn {
				waited[awaitedMember] = true
				awaited[awaitedMember] = true
			}
			continue
		}
		waited[member] = true
	}
	report.Awaited = sortedMemberIDs(awaited)
	for _, member := range sortedMemberIDs(waited) {
		_, down := in.downFor[member]
		report.WaitingOn = append(report.WaitingOn, waitedOn{Node: member, State: in.stateOf(member, report), Down: down})
	}

	report.CountsComplete = true
	for _, member := range in.members {
		progress, reported := in.handOff[member]
		if !reported || progress.Epoch != report.JoinEpoch {
			report.CountsComplete = false
			continue
		}
		report.Copied += int(progress.Done)
		report.Total += int(progress.Total)
	}
	// Each holder lists the copies its own buckets lack, so two holders of a
	// bucket list the same copy; it is printed once.
	for _, member := range in.members {
		progress, reported := in.handOff[member]
		if !reported || progress.Epoch != report.JoinEpoch {
			continue
		}
		for _, copyToMake := range progress.Unconfirmed {
			if !slices.Contains(report.Migrate, copyToMake) {
				report.Migrate = append(report.Migrate, copyToMake)
			}
		}
		report.MigrateLeft = max(report.MigrateLeft, int(progress.UnconfirmedCount))
	}
	report.MigrateLeft = max(report.MigrateLeft, len(report.Migrate))
	return report
}

// stateOf says, in a few words, why the paused schema changes wait on member.
func (in joinFreezeInput) stateOf(member string, report joinFreezeReport) string {
	if downFor, down := in.downFor[member]; down {
		return "unreachable for " + downFor.Round(time.Second).String()
	}
	progress, reported := in.handOff[member]
	settled := in.settledOfMember(member) >= report.JoinEpoch
	switch {
	case settled:
		return "has not taken every copy handed to it yet"
	case reported && progress.Epoch >= report.JoinEpoch && progress.GaveUp:
		return fmt.Sprintf("gave up handing off %d buckets for now; retrying while its members answer", progress.Total-progress.Done)
	case reported && progress.Epoch >= report.JoinEpoch:
		return fmt.Sprintf("still handing off (%d left)", progress.Total-progress.Done)
	}
	return "has not reported since " + strings.Join(report.Joiners, " and ") + " joined"
}

// sortedMemberIDs is the member ids of set, sorted.
func sortedMemberIDs(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// recomputeJoinFreeze gathers the judge's input and publishes its report. The
// inputs are read one lock at a time, none under another (spec §12), and
// recomputeMu keeps an older report from replacing a newer one. It logs once
// when this node's view pauses schema changes and once when it allows them
// again.
func (c *Cluster) recomputeJoinFreeze() {
	c.recomputeMu.Lock()
	defer c.recomputeMu.Unlock()
	c.joinFreezeRecomputedAt.Store(time.Now().UnixNano())
	// Cleared before the hand-offs are read, so one heard after the read sets
	// it again.
	c.handOffHeardStale.Store(false)

	epoch, members := c.epochMembers()
	in := joinFreezeInput{
		epoch:      epoch,
		self:       c.self.ID,
		members:    memberIDsOf(members),
		joinedAt:   c.joinedAtSnapshot(),
		ownSettled: c.settled.Load(),
		handOff:    make(map[string]HandOffProgress),
		downFor:    make(map[string]time.Duration),
	}
	now := time.Now()
	c.peersMu.RLock()
	for _, member := range members {
		if p := c.peers[member.ID]; p != nil && member.ID != c.self.ID {
			if since, down := p.downSince(); down {
				in.downFor[member.ID] = now.Sub(since)
			}
		}
	}
	c.peersMu.RUnlock()
	if own := c.ownHandOff(); own != nil {
		in.handOff[c.self.ID] = *own
	}
	c.settleMu.Lock()
	in.settledOf = copyEpochMap(c.settledOf)
	for id, progress := range c.handOffOf {
		in.handOff[id] = progress
	}
	c.settleMu.Unlock()

	report := judgeJoinFreeze(in)
	previous := c.joinFreeze.Swap(&report)
	// The settling window closes (on-demand pull spec §3.1, §4.2): what reads
	// in it recorded is forgotten, under takeOver.mu after the closed report
	// is published, so a read that re-checks the window under the same lock
	// records nothing after this. recomputeMu → takeOver.mu.
	if readWindowClosedBy(previous, &report) {
		c.clearReadOutcomes()
	}
	switch {
	case report.Paused && (previous == nil || !previous.Paused):
		c.logger.Info("schema changes are paused while a node joins the cluster, until every member has handed it its buckets; 'nodes' shows the progress",
			"joining", strings.Join(report.Joiners, ","), "epoch", report.Epoch)
	case !report.Paused && previous != nil && previous.Paused:
		c.logger.Info("schema changes are allowed again: every member has settled the join", "epoch", report.Epoch)
	}
}

// joinFreezeCountInterval is the least time between two recomputes that a
// changed hand-off count asks for.
const joinFreezeCountInterval = time.Second

// readWindowClosedBy reports whether publishing report over previous closes
// the settling window: previous read open (another epoch, or not every member
// settled it) and report reads closed.
func readWindowClosedBy(previous, report *joinFreezeReport) bool {
	wasOpen := previous == nil || previous.Epoch != report.Epoch || previous.SettledAt < previous.Epoch
	return wasOpen && report.SettledAt >= report.Epoch
}

// recomputeJoinFreezeForCounts recomputes for a changed progress count, or a
// peer found down or back, at most once per joinFreezeCountInterval: counts
// change once per bucket, and a peer's state at every gossip round.
func (c *Cluster) recomputeJoinFreezeForCounts() {
	if time.Since(time.Unix(0, c.joinFreezeRecomputedAt.Load())) < joinFreezeCountInterval {
		return
	}
	c.recomputeJoinFreeze()
}

// joinFreezeOffHook switches the join freeze off when set: the tests of the
// lines of defence behind it send schema changes during a
// join on purpose, and would otherwise only show the freeze refusing them
// first. False in production.
var joinFreezeOffHook atomic.Bool

// joinFreezeLiftedHook, when set, says whether a node's view has lifted its
// pause, whatever its report says: a test makes one coordinator lift before
// the owner it forwards to, which every join does for up to a gossip round.
// Nil in production.
var joinFreezeLiftedHook atomic.Pointer[func(nodeID string) bool]

func (c *Cluster) joinFreezeLifted() bool {
	hook := joinFreezeLiftedHook.Load()
	return hook != nil && (*hook)(c.self.ID)
}

// isSchemaChange reports whether a client opcode changes the schema: what the
// join freeze refuses (spec §1). A restore's start creates or drops its bucket;
// a factor alter changes the set its bucket is placed on
// (bucket-replication-rev8-spec §4.3 R5).
func isSchemaChange(op transport.Opcode) bool {
	switch op {
	case transport.OpCreateDB, transport.OpDeleteDB, transport.OpCreateBucket, transport.OpDeleteBucket,
		transport.OpDeclareStoreFields, transport.OpAlterBucket, transport.OpRestoreStart:
		return true
	}
	return false
}

// refuseSchemaChange is the join freeze's one rule (spec §5, §8.4): a schema
// change decided at epoch is refused when the published report is for another
// epoch — the membership changed while the request was in flight — or pauses
// schema changes. One atomic load when nothing is joining. It is called on
// client-originated paths only: never where a logged entry is applied, which
// would stall a replica's stream behind it for ever (build doc, trap 1).
func (c *Cluster) refuseSchemaChange(op transport.Opcode, epoch uint64) error {
	if !isSchemaChange(op) || joinFreezeOffHook.Load() {
		return nil
	}
	report := c.joinFreeze.Load()
	switch {
	case report.Epoch != epoch:
		return &sentencedError{cause: ErrSchemaChangePaused, message: fmt.Sprintf(
			"The cluster's membership changed while this request was in flight (epoch %d to %d); nothing was applied. Retry it.",
			epoch, report.Epoch)}
	case report.Paused && !c.joinFreezeLifted():
		return &sentencedError{cause: ErrSchemaChangePaused, message: report.refusal()}
	}
	return nil
}

// refusal is the paused report's refusal text (spec §7.1): who is joining and
// how far the copying has got; the members it waits on, each with its state,
// and the holders stalled behind them; and the ways out in the order of spec
// §6.3, with what each costs. It never advises a create: the data exists.
func (report *joinFreezeReport) refusal() string {
	var text strings.Builder
	var clauses []string
	if len(report.Joining) > 0 {
		clauses = append(clauses, fmt.Sprintf("%s %s the cluster", andList(report.Joining), pluralVerb(report.Joining, "joins", "join")))
	}
	if len(report.Departed) > 0 {
		clauses = append(clauses, fmt.Sprintf("the members settle the removal of %s, which left before its join settled", andList(report.Departed)))
	}
	fmt.Fprintf(&text, "Schema changes are paused while %s", strings.Join(clauses, ", and "))
	// The count is a floor, never a total: a member that has not reported
	// counts nothing, and a holder's total is known only once its pass has
	// walked its catalog.
	if report.Copied > 0 {
		fmt.Fprintf(&text, ": at least %d %s of this membership change %s been copied to their new places",
			report.Copied, pluralNoun(report.Copied, "bucket", "buckets"), pluralNoun(report.Copied, "has", "have"))
	}
	text.WriteString(". Nothing was created, dropped, altered or restored; reads and writes to existing buckets carry on. " +
		"Run this again once 'nodes' shows the change settled - it settles on its own.")
	if len(report.WaitingOn) > 0 {
		states := make([]string, 0, len(report.WaitingOn))
		for _, member := range report.WaitingOn {
			states = append(states, member.Node+", "+member.State)
		}
		fmt.Fprintf(&text, "\nWaiting on: %s.", strings.Join(states, "; "))
	}
	if len(report.Stalled) > 0 && len(report.Awaited) > 0 {
		fmt.Fprintf(&text, " %s cannot finish handing off until %s %s.", andList(report.Stalled), andList(report.Awaited),
			pluralVerb(report.Awaited, "answers", "answer"))
	}
	text.WriteString("\nWays out: ")
	var ways []string
	var down []string
	for _, member := range report.WaitingOn {
		if member.Down {
			down = append(down, member.Node)
		}
	}
	for _, member := range down {
		ways = append(ways, "start "+member+" again")
	}
	for _, joiner := range report.Joining {
		ways = append(ways, fmt.Sprintf("undo the join with 'remove-node %s' run on another member", joiner))
	}
	for _, member := range down {
		ways = append(ways, fmt.Sprintf("if %s is not coming back, 'remove-node %s' - buckets whose only copy is on %s then become unreachable, "+
			"and a bucket of the same name created afterwards is a new, empty bucket", member, member, member))
	}
	if len(report.Migrate) > 0 {
		// Auto-rebalance is off: nothing is copied until the operator does it.
		commands := make([]string, 0, len(report.Migrate))
		for _, copyToMake := range report.Migrate[:min(len(report.Migrate), unconfirmedListed)] {
			commands = append(commands, fmt.Sprintf("'migrate %s %s' on %s", copyToMake.Database, copyToMake.Bucket, copyToMake.Node))
		}
		more := ""
		if extra := report.MigrateLeft - len(commands); extra > 0 {
			more = fmt.Sprintf(", and %d more ('nodes' lists them as they are copied)", extra)
		}
		ways = append([]string{"auto-rebalance is off, so copy each moved bucket by hand: run " + strings.Join(commands, "; ") + more}, ways...)
	} else if len(down) == 0 {
		ways = append([]string{"wait: every member is handing off"}, ways...)
	}
	text.WriteString(strings.Join(ways, "; or ") + ".")
	return text.String()
}

// schemaChangesReport is this node's published report as 'nodes' carries it.
func (c *Cluster) schemaChangesReport() *SchemaChangesReport {
	report := c.joinFreeze.Load()
	carried := &SchemaChangesReport{
		Paused:    report.Paused,
		Epoch:     report.Epoch,
		SettledAt: report.SettledAt,
		Joiners:   report.Joiners,
		Stalled:   report.Stalled,
		Copied:    report.Copied,
		Total:     report.Total,
		Migrate:   report.Migrate,
	}
	for _, member := range report.WaitingOn {
		carried.WaitingOn = append(carried.WaitingOn, WaitedOnMember{Node: member.Node, State: member.State})
	}
	if report.Paused {
		carried.Message = report.refusal()
	}
	return carried
}

// andList joins names as "a", "a and b", "a, b and c".
func andList(names []string) string {
	if len(names) <= 1 {
		return strings.Join(names, "")
	}
	return strings.Join(names[:len(names)-1], ", ") + " and " + names[len(names)-1]
}

// pluralNoun picks the form of a word for a count.
func pluralNoun(count int, one, many string) string {
	if count == 1 {
		return one
	}
	return many
}

// pluralVerb picks the verb form for a list of names.
func pluralVerb(names []string, one, many string) string {
	if len(names) == 1 {
		return one
	}
	return many
}

// schemaRefusalText is the text of a freeze refusal, without the wrapping a
// forward or an outcome adds, for an owner to send back to a coordinator.
func schemaRefusalText(err error) string {
	var sentenced *sentencedError
	if errors.As(err, &sentenced) && errors.Is(sentenced, ErrSchemaChangePaused) {
		return sentenced.message
	}
	return err.Error()
}

// startRestoreUnlessPaused is the door of the join freeze for a restore's
// start (spec §5.1), which is routed like a read: a real start is refused
// while schema changes are paused; a dry run is answered, with a line saying
// a real restore would be refused now (Q9).
func (c *Cluster) startRestoreUnlessPaused(ctx context.Context, frame *transport.Frame, bucket, database string) (*transport.Frame, error) {
	paused := c.refuseSchemaChange(frame.Opcode, c.Epoch())
	if paused == nil {
		return c.serveRead(ctx, frame, bucket, database)
	}
	var start RestoreStartRequest
	if err := bson.Unmarshal(frame.Payload, &start); err != nil || !start.DryRun {
		return errorFrame(frame, withOutcome(outcomeNotApplied, paused)), nil
	}
	answer, err := c.serveRead(ctx, frame, bucket, database)
	if err != nil || answer.Opcode == transport.OpError {
		return answer, err
	}
	var dryRun RestoreStartResponse
	if err := bson.Unmarshal(answer.Payload, &dryRun); err != nil {
		return nil, fmt.Errorf("reading the dry run's answer to add that a real restore would be refused: %w", err)
	}
	dryRun.Message = strings.TrimSpace(dryRun.Message + " A real restore would be refused now: " + paused.Error())
	payload, err := bson.Marshal(dryRun)
	if err != nil {
		return nil, fmt.Errorf("encoding the dry run's answer: %w", err)
	}
	return transport.NewFrameWithID(answer.Opcode, answer.RequestID, payload), nil
}
