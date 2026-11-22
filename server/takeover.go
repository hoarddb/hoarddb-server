package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The take-over (replication spec §20.8.4 to §20.8.6; build doc steps 10b.3,
// 10b.5 and 10b.6).
//
// After a membership change moves a bucket, its new owner does not serve it
// until it holds every entry a remaining node holds: it asks the bucket's
// previous owners, or every member when they are gone, and opens a term of its
// own only once its copy is the best one. Until then every write and every
// drop of the bucket is refused inside its fence, and a create waits for the
// take-over and is answered from the copy it lands. This file holds what the
// write path and the worker share; the worker is takeOverRound.

// takeOverStatus is what this node knows of one take-over in progress, so a
// refused client is told what the bucket waits for (spec §20.12.2).
type takeOverStatus struct {
	// source is the previous owner whose answer was final on the ordinary
	// path: a read of the bucket is forwarded to it until the open.
	source      ring.Node
	sourceKnown bool
	// gone is a previous owner that did not answer, waitingOn the members the
	// fallback still waits for, and removable whether a majority of the
	// current members answered without them, so that a removal could commit.
	gone      string
	waitingOn []string
	removable bool
	// removedHolds says a node removed within the history answered the
	// fallback that it holds a live copy, which the members' catalog survey
	// cannot see (awaitCopyARemovedNodeMayHold).
	removedHolds bool
	// needsCopy says the bucket waits for a whole copy, which with
	// auto_rebalance off is the operator's to start with 'migrate'.
	needsCopy bool
	// copyRefused is why the last attempt to copy the bucket in failed: the
	// holders' own refusal, which a client is told as it is.
	copyRefused error
	// incarnationHolders are members that hold live copies of two
	// incarnations of the name, which no take-over merges (I-H1).
	incarnationHolders []string
	// owesApplies are the sequences this node's own copy, the most complete
	// one, owes (A-10.3), and owedAdvice what a restart does about them.
	owesApplies []uint64
	owedAdvice  string
	// versionLimit is why the bucket can open no term here: the epoch does
	// not fit a version (packVersion). It never clears; the remedy is a dump.
	versionLimit error
	// createInFlightAt are the nodes, this one included, with a create of the
	// name in flight: the take-over found no copy and waits for the create to
	// land (on-demand pull spec §6.8).
	createInFlightAt []string
	// handedOffBy is the removed node that handed the bucket to this node and
	// did not answer the take-over's question about its copy (BUG-0688): the
	// take-over found no copy, but the node said it holds one.
	handedOffBy string
}

// blocked reports whether the take-over waits on something a refusal names
// and waiting does not end: a member that does not answer, two incarnations,
// the holders' refusal of the copy, or a removed node that handed the bucket
// here and does not answer about its copy.
func (s takeOverStatus) blocked() bool {
	return len(s.waitingOn) > 0 || len(s.incarnationHolders) > 0 || s.copyRefused != nil || s.handedOffBy != ""
}

// waitsOnMembersOnly reports whether the take-over is blocked only by current
// members that did not answer, with no removed node holding a live copy.
func (s takeOverStatus) waitsOnMembersOnly() bool {
	return len(s.waitingOn) > 0 && !s.removedHolds && len(s.incarnationHolders) == 0 && s.copyRefused == nil
}

// recorded reports whether a take-over recorded anything about what it waits
// for.
func (s takeOverStatus) recorded() bool {
	return s.sourceKnown || s.gone != "" || len(s.waitingOn) > 0 || s.needsCopy || s.copyRefused != nil || len(s.incarnationHolders) > 0 ||
		len(s.owesApplies) > 0 || s.versionLimit != nil || len(s.createInFlightAt) > 0 || s.handedOffBy != ""
}

// copyRefusedError is the refusal of a request to a bucket whose take-over
// could not copy it in: it matches ErrHandoffInProgress and the holders'
// refusal both.
type copyRefusedError struct {
	message string
	refusal error
}

func (e *copyRefusedError) Error() string   { return e.message }
func (e *copyRefusedError) Unwrap() []error { return []error{ErrHandoffInProgress, e.refusal} }

// takeOverState is the worker's bookkeeping, guarded by its own mutex, a
// leaf. Every map holds only buckets whose take-over is in progress: at rest
// they are empty.
type takeOverState struct {
	mu sync.Mutex
	// wake is buffered by one: a wake while the worker runs a round is kept
	// for the next.
	wake chan struct{}
	// named are buckets a refusal, a holder's ask or an operator's migrate
	// named since the worker last took them, by placement key.
	named map[placementKey]heldBucket
	// leave are the buckets an operator's migrate gave leave to copy whole
	// while auto_rebalance is off (A-10.6).
	leave map[placementKey]bool
	// status is each take-over in progress, for its refusal.
	status map[placementKey]takeOverStatus
	// waiting is each wait for an origin's stream to bring a copy to the end
	// of the origin's term, while it lasts (originWaitOver, followerWaitOver).
	waiting map[placementKey]originWait
	// strayWarnedAt is when two live incarnations of a name were last named
	// in a WARN.
	strayWarnedAt map[placementKey]time.Time
	// limitWarnedAt is when a bucket's refused term was last named in a
	// WARN. It is a map of its own, so that the version limit's WARN never
	// holds back a WARN about two incarnations of the same bucket.
	limitWarnedAt map[placementKey]time.Time
	// foundNothing is what a take-over last found of a name nobody here
	// holds (on-demand pull spec §4.2): no live copy anywhere, for the
	// callers waiting on it to answer "not found", or members that did not
	// answer, for a read to name. Readers do not take an entry (every waiter
	// hears it, BUG-0639); at most onDemandOutcomeCap are kept
	// (recordOutcome), the adoption and the window's close clear every entry
	// but a migrate's (clearReadOutcomesLocked), and a drop forgets one.
	foundNothing map[placementKey]foundOutcome
	// bounds is each bucket's shared on-demand read bound (spec §6.1): the
	// instant every read of the take-over stops waiting, created by its
	// first waiting read (readBound). It is not part of status, which the
	// worker republishes every round.
	bounds map[placementKey]waitBound
	// handedOff is each bucket a removed node's hand-off asked this node, its
	// owner, to take, by the removed node the name was placed on, until the
	// take-over ends (BUG-0688, removed_settle.go).
	handedOff map[placementKey]string
}

// foundOutcome is one take-over's finding about a name nobody here holds:
// when, at which epoch, and the current members that did not answer — none
// for "no live copy anywhere". leave marks a "nothing" an operator's migrate
// gave leave for, which only it is answered from (awaitOwnCopy) and which the
// whole-map clears keep.
type foundOutcome struct {
	at      time.Time
	epoch   uint64
	unheard []string
	leave   bool
}

// onDemandOutcomeCap bounds foundNothing (spec §4.2): a read storm over
// missing names cannot grow it past this.
const onDemandOutcomeCap = 4096

// onDemandUnheardTTL is how long a read answers from a finding that members
// did not answer, before the next read asks again (catalogQuestionWait's
// length): a member that is down is not asked on every read.
const onDemandUnheardTTL = 2 * time.Second

// waitBound is the instant the reads of one take-over stop waiting, and the
// epoch of the read that set it: a read of a later epoch takes it as absent,
// so a later take-over of the name never inherits a spent bound (F-2, R-4).
type waitBound struct {
	until time.Time
	epoch uint64
}

func newTakeOverState() takeOverState {
	return takeOverState{
		wake:          make(chan struct{}, 1),
		named:         make(map[placementKey]heldBucket),
		leave:         make(map[placementKey]bool),
		status:        make(map[placementKey]takeOverStatus),
		waiting:       make(map[placementKey]originWait),
		strayWarnedAt: make(map[placementKey]time.Time),
		limitWarnedAt: make(map[placementKey]time.Time),
		foundNothing:  make(map[placementKey]foundOutcome),
		bounds:        make(map[placementKey]waitBound),
		handedOff:     make(map[placementKey]string),
	}
}

// wakeTakeOver names database/bucket to the worker and wakes it: the bucket
// is taken before any nobody has asked for (A-10.7).
func (c *Cluster) wakeTakeOver(database, bucket string) {
	c.takeOver.mu.Lock()
	c.takeOver.named[placementOf(database, bucket)] = heldBucket{database: database, bucket: bucket}
	c.takeOver.mu.Unlock()
	c.signalTakeOver()
}

// onDemandNamedCap is how many names takeOver.named may hold for a read to
// add one (spec §4.3): a read storm over missing names cannot deepen the queue
// past it. It is measured on the whole map, whatever named the entries; the
// other callers of wakeTakeOver and takeOverRound's re-insert stay uncapped,
// so after a change that moved many buckets the map is O(unsettled) as before.
const onDemandNamedCap = 4096

// takeOverBeforeNamedInsertHook, when set, runs in wakeTakeOverCapped between
// its length check and its insert, with takeOver.mu held, and may block. A
// test parks one read there. Nil in production.
var takeOverBeforeNamedInsertHook atomic.Pointer[func(nodeID, database, bucket string)]

// wakeTakeOverCapped is wakeTakeOver for a read (spec §4.3, R-1): the length
// check and the insert are one takeOver.mu section, so concurrent reads cannot
// each see room and each insert. A name already queued is always accepted; it
// cannot deepen the map. It reports false, naming nothing, once the map holds
// onDemandNamedCap names.
func (c *Cluster) wakeTakeOverCapped(database, bucket string) bool {
	key := placementOf(database, bucket)
	c.takeOver.mu.Lock()
	if _, queued := c.takeOver.named[key]; !queued && len(c.takeOver.named) >= onDemandNamedCap {
		c.takeOver.mu.Unlock()
		return false
	}
	if hook := takeOverBeforeNamedInsertHook.Load(); hook != nil {
		(*hook)(c.self.ID, database, bucket)
	}
	c.takeOver.named[key] = heldBucket{database: database, bucket: bucket}
	c.takeOver.mu.Unlock()
	c.signalTakeOver()
	return true
}

// signalTakeOver wakes the worker without naming a bucket.
func (c *Cluster) signalTakeOver() {
	select {
	case c.takeOver.wake <- struct{}{}:
	default:
	}
}

// takeOverStatusOf is the status of key's take-over, or the zero status.
func (c *Cluster) takeOverStatusOf(key placementKey) takeOverStatus {
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	return c.takeOver.status[key]
}

// handoffInProgress is the refusal of a request to a bucket this node owns
// and has not taken over, in the words of spec §20.12.2.
func (c *Cluster) handoffInProgress(database, bucket string) error {
	return c.handoffInProgressAs(database, bucket, c.takeOverStatusOf(placementOf(database, bucket)))
}

// handoffInProgressAs is handoffInProgress for status, which a read may have
// completed with what it knows before the take-over records it (needsCopy,
// spec §6.6).
func (c *Cluster) handoffInProgressAs(database, bucket string, status takeOverStatus) error {
	name := database + "." + bucket
	var message string
	switch {
	case status.versionLimit != nil:
		return &sentencedError{cause: errors.Join(ErrHandoffInProgress, status.versionLimit), message: fmt.Sprintf(
			"ownership of %s is moving to this node, which cannot open it: %v", name, status.versionLimit)}
	case len(status.incarnationHolders) > 0:
		message = fmt.Sprintf("ownership of %s is moving to this node, and %s hold live copies of two different incarnations of it: "+
			"two buckets of one name, created apart, which this node does not merge. Nothing was stored, and writes are refused "+
			"until one of the copies goes: remove the node that holds the copy that is not wanted ('remove-node <node>'), or empty "+
			"that node's data directory and add it again", name, strings.Join(status.incarnationHolders, " and "))
	case len(status.owesApplies) > 0:
		message = fmt.Sprintf("ownership of %s is moving to this node, %s, whose own copy is the most complete one and owes the applies of "+
			"sequences %v; nothing was stored. %s", name, c.self.ID, status.owesApplies, status.owedAdvice)
	case status.copyRefused != nil:
		return &copyRefusedError{refusal: status.copyRefused, message: fmt.Sprintf(
			"ownership of %s is moving to this node, which could not copy the bucket in from the holders of its most complete copy; "+
				"nothing was stored: %v", name, status.copyRefused)}
	case status.needsCopy:
		message = fmt.Sprintf("ownership of %s is moving to this node, which does not hold the bucket's most complete copy; nothing was stored. "+
			"Run 'migrate %s %s' on %s to copy it", name, database, bucket, c.self.ID)
	case len(status.waitingOn) > 0:
		message = fmt.Sprintf("ownership of %s is moving to this node. Its previous owner %s is gone, and %s, which may hold a copy, "+
			"cannot be reached, so this node cannot tell which copy is complete; nothing was stored. Bring %s back",
			name, status.gone, strings.Join(status.waitingOn, ", "), strings.Join(status.waitingOn, ", "))
		if status.removable {
			message += fmt.Sprintf(". Or remove it with 'remove-node %s': writes that only it holds are then lost", status.waitingOn[0])
		}
	case status.handedOffBy != "":
		return removedOwnerAwaited(database, bucket, status.handedOffBy, "nothing was read")
	case len(status.createInFlightAt) > 0:
		message = fmt.Sprintf("ownership of %s is moving to this node, and a create of it is in flight at %s; nothing was stored. "+
			"Retry in a moment: the create lands on its own ('nodes' shows the members)", name, strings.Join(status.createInFlightAt, " and "))
	case status.sourceKnown:
		message = fmt.Sprintf("ownership of %s is moving to this node and it is collecting the bucket's last writes from %s; "+
			"nothing was stored. Retry in a moment", name, status.source.ID)
	default:
		message = fmt.Sprintf("ownership of %s is moving to this node, which is finding the bucket's most complete copy; "+
			"nothing was stored. Retry in a moment", name)
	}
	return &sentencedError{cause: ErrHandoffInProgress, message: message}
}

// ownerAwaitsTakeOver reports whether this node, the ring's owner of the
// bucket placed by key, has not taken it over: its copy is not opened or is
// marked moved, or it holds none and a take-over of it is running. It costs
// two atomic reads and the copy's in-memory term while nothing is unsettled;
// the marks are read only when something is (rev 7, review F-14 item 6).
func (c *Cluster) ownerAwaitsTakeOver(key placementKey, epoch uint64, database, bucket string) (bool, error) {
	if !c.s.bucketStoreOpen(database, bucket) {
		return c.pullPending(database, bucket), nil
	}
	if c.mayBeUnsettled(epoch) && c.placementMarksOf(key).Moved != 0 {
		return true, nil
	}
	term, opened, err := c.s.bucketLineage(database, bucket)
	if err != nil {
		return false, fmt.Errorf("reading whether this node has opened bucket %q in database %q: %w", bucket, database, err)
	}
	if opened {
		return false, nil
	}
	if term != 0 {
		return true, nil
	}
	// A copy with no lineage is from before lineages, until its first write
	// here: opened on its unmoved owner (SA-9.4).
	st, err := c.s.bucketStateNamed(database, bucket)
	if err != nil {
		return false, fmt.Errorf("reading whether this node has opened bucket %q in database %q: %w", bucket, database, err)
	}
	return !c.copyIsOpened(key, st, c.placementMarksOf(key)), nil
}

// readsOpenedCopyAtRest reports the steady state of ownerReadAwaitsTakeOver
// answering false at epoch, read without the bucket's placement key: this node holds
// the bucket, its lineage cache says the copy is opened, and no placement mark
// can be held (mayBeUnsettled). Every other case is left to
// ownerReadAwaitsTakeOver. The one-member door asks it first, so a node a
// removal left alone reads a bucket it has taken over with no allocation more
// than a node that was always alone (BUG-0630).
func (c *Cluster) readsOpenedCopyAtRest(epoch uint64, database, bucket string) bool {
	if c.mayBeUnsettled(epoch) {
		return false
	}
	facts, held := c.s.peekBucket(database, bucket)
	return held && facts.id != 0 && facts.lineageFlags&lineageKnown != 0 && facts.lineageFlags&lineageOpened != 0
}

// readGateLineageLoadHook, when set, runs in ownerReadAwaitsTakeOverState each
// time the read gate loads the state document on a lineage-cache miss, before
// the load, so a test can count the document's reads. Nil in production.
var readGateLineageLoadHook atomic.Pointer[func(database, bucket string)]

// ownerReadAwaitsTakeOver is ownerAwaitsTakeOver for a read, which takes no
// fence and must not wait on the bucket's mutex (S-2). The marks alone are not
// enough: the placement pass marks only the copies held when it runs, so a
// copy that lands after it — a create the old owner's stream delivers late, a
// pull that lands across the adoption — is moved and unmarked (BUG-0513). The
// copy's lineage decides, as on the write path; it is
// ownerReadAwaitsTakeOverState with no document in hand, which loads one on a
// cache miss.
func (c *Cluster) ownerReadAwaitsTakeOver(key placementKey, epoch uint64, database, bucket string) (bool, error) {
	return c.ownerReadAwaitsTakeOverState(key, epoch, database, bucket, nil)
}

// ownerReadAwaitsTakeOverState is the read gate for a caller that has already
// read the copy's state document from its own read's snapshot (versionedRead):
// the gate uses loaded rather than reading the document a second time. On a
// cache miss it fills the lineage cache when no writer holds the bucket's lock
// (tryFillLineage, which reads the document again under that lock: the
// document this caller holds may be older than a writer's commit), so a bucket
// costs one miss per start, not one per read. It never waits on the mutex, so
// S-2 holds.
func (c *Cluster) ownerReadAwaitsTakeOverState(key placementKey, epoch uint64, database, bucket string, loaded *store.BucketState) (bool, error) {
	facts, held := c.s.peekBucket(database, bucket)
	if !held || facts.id == 0 {
		return c.pullPending(database, bucket), nil
	}
	var marks placementMarks
	if c.mayBeUnsettled(epoch) {
		marks = c.placementMarksOf(key)
	}
	if marks.Moved != 0 {
		return true, nil
	}
	if facts.lineageFlags&lineageKnown != 0 {
		return !c.cacheIsOpened(key, facts.term, facts.lineageFlags, marks), nil
	}
	var st store.BucketState
	if loaded != nil {
		st = *loaded
	} else {
		if hook := readGateLineageLoadHook.Load(); hook != nil {
			(*hook)(database, bucket)
		}
		snapshot := c.s.kv.NewSnapshot()
		defer snapshot.Discard()
		var err error
		st, err = store.LoadBucketState(snapshot, facts.slot, facts.databaseID, facts.id)
		if err != nil {
			return false, fmt.Errorf("reading whether this node has opened bucket %q in database %q: %w", bucket, database, err)
		}
	}
	filled, ok, err := c.s.tryFillLineage(database, bucket, facts.id)
	if err != nil {
		return false, fmt.Errorf("filling the lineage of bucket %q in database %q: %w", bucket, database, err)
	}
	if ok {
		st = filled
	}
	return !c.copyIsOpened(key, st, marks), nil
}

// takeOverWaitStep bounds one wait for a take-over that has not registered
// its marker yet.
const takeOverWaitStep = 50 * time.Millisecond

// awaitTakeOver waits, holding no fence, for this node's take-over of the
// bucket to finish, up to the write-concern timeout (step 10b.3): a create of
// a bucket that is moving here is answered from the copy the take-over lands,
// or created when the take-over found none. It wakes the worker for the
// bucket and waits on its marker (migrateActive), and answers
// ErrHandoffInProgress when the wait runs out.
func (c *Cluster) awaitTakeOver(ctx context.Context, key placementKey, database, bucket string) error {
	deadline := time.Now().Add(c.wcTimeout)
	for {
		_, epoch, err := c.ownerView(key)
		if err != nil {
			return fmt.Errorf("reading the owner of bucket %q in database %q while waiting for its take-over: %w", bucket, database, err)
		}
		pending, err := c.ownerAwaitsTakeOver(key, epoch, database, bucket)
		if err != nil {
			return fmt.Errorf("reading whether bucket %q in database %q is taken over: %w", bucket, database, err)
		}
		// The copy opens in the batch that lands it, a moment before the
		// worker removes the marker; a create let in between would meet the
		// marker inside the fence and be refused (refuseUnservable).
		if !pending && !c.pullPending(database, bucket) {
			return nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return c.handoffInProgress(database, bucket)
		}
		c.wakeTakeOver(database, bucket)
		c.migrateMu.Lock()
		done := c.migrateActive[migrateKey(database, bucket)]
		c.migrateMu.Unlock()
		step := time.NewTimer(min(remaining, takeOverWaitStep))
		select {
		case <-done:
		case <-step.C:
		case <-ctx.Done():
			step.Stop()
			return fmt.Errorf("waiting for this node to take over bucket %q in database %q: %w", bucket, database, ctx.Err())
		}
		step.Stop()
	}
}

// awaitOwnCopy waits for the worker to take over a bucket this node owns and
// holds nothing of: a holder's hand-off and an operator's migrate wait on it,
// as they waited on the pull the owner used to run itself. It answers nil once
// this node serves an opened copy, ErrMigrateBucketNotFound when the take-over
// found no live copy anywhere, and the hand-off's refusal once the
// write-concern timeout has passed with no copy of the bucket being pulled in
// — the worker waits on something the refusal names — or ctx ends.
func (c *Cluster) awaitOwnCopy(ctx context.Context, key placementKey, database, bucket string) error {
	// A holder hands the name off, or a migrate or a create asks for it: a
	// read's "nothing" or "unheard" for it is out of date (spec §4.2).
	c.forgetReadOutcomeOf(key)
	began := time.Now()
	deadline := began.Add(c.wcTimeout)
	woken := false
	for {
		if c.s.bucketStoreOpen(database, bucket) {
			_, opened, err := c.s.bucketLineage(database, bucket)
			if err != nil {
				return fmt.Errorf("reading whether this node's copy of bucket %q in database %q is opened: %w", bucket, database, err)
			}
			if opened {
				return nil
			}
		}
		if c.takeFoundNothing(key, began) {
			return fmt.Errorf("%w: no member, and no node removed within the history, holds a live copy of bucket %q in database %q",
				ErrMigrateBucketNotFound, bucket, database)
		}
		c.migrateMu.Lock()
		done := c.migrateActive[migrateKey(database, bucket)]
		c.migrateMu.Unlock()
		// A take-over waiting on a member that does not answer, blocked on two
		// incarnations, or whose copy the holders refused, is answered at once:
		// a holder's sweep asks every bucket it hands off in turn, and an
		// operator is told the holders' own refusal. Past the write-concern
		// timeout it waits on only while the take-over is copying the bucket
		// in, as a caller waited on the owner's own pull before.
		if c.takeOverStatusOf(key).blocked() || (time.Now().After(deadline) && !c.pullRunning(database, bucket)) {
			return c.handoffInProgress(database, bucket)
		}
		if done == nil && !woken {
			c.wakeTakeOver(database, bucket)
			woken = true
		}
		step := time.NewTimer(takeOverWaitStep)
		select {
		case <-done:
		case <-step.C:
		case <-ctx.Done():
			step.Stop()
			return fmt.Errorf("waiting for this node to take over bucket %q in database %q: %w", bucket, database, ctx.Err())
		}
		step.Stop()
	}
}

// takeFoundNothing reports whether a take-over that a migrate gave leave to
// found no live copy of key anywhere after since. It does not forget the
// answer: a create, an operator's migrate and a holder's hand-off sweep wait
// on one take-over of one name, and the first reader taking the answer left
// the others waiting out the write-concern timeout (BUG-0639). A waiter that
// began after the answer does not hear it (since). Only a "nothing" written
// with leave answers it (R-2): a read's finding that members did not answer
// is not "no member holds it", and an ordinary "nothing" was not decided for
// this waiter.
func (c *Cluster) takeFoundNothing(key placementKey, since time.Time) bool {
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	found, recorded := c.takeOver.foundNothing[key]
	return recorded && found.leave && found.at.After(since)
}

// recordOutcome records a take-over's finding about key. Called with
// takeOver.mu held, by the worker only. At onDemandOutcomeCap the entries no
// waiter can still be waiting for — older than a create's createTakeOverWait
// write-concern timeouts, twice over — go, and then the oldest: a linear scan
// that runs only when the map is full, never on the read path. A finding
// never hides a migrate's: a later "nothing" keeps its leave, and a finding
// that members did not answer is not recorded over it.
func (c *Cluster) recordOutcome(key placementKey, outcome foundOutcome) {
	existing, present := c.takeOver.foundNothing[key]
	if present && existing.leave && !outcome.leave {
		if len(outcome.unheard) > 0 {
			return
		}
		outcome.leave = true
	}
	if !present && len(c.takeOver.foundNothing) >= onDemandOutcomeCap {
		c.evictOutcomesLocked(outcome.at)
	}
	c.takeOver.foundNothing[key] = outcome
}

// evictOutcomesLocked makes room in a full foundNothing: every entry older
// than any waiter can have started before, or else the oldest one.
func (c *Cluster) evictOutcomesLocked(now time.Time) {
	var oldestKey placementKey
	var oldest time.Time
	for name, found := range c.takeOver.foundNothing {
		if now.Sub(found.at) > 2*createTakeOverWait*c.wcTimeout {
			delete(c.takeOver.foundNothing, name)
			continue
		}
		if oldest.IsZero() || found.at.Before(oldest) {
			oldestKey, oldest = name, found.at
		}
	}
	if len(c.takeOver.foundNothing) >= onDemandOutcomeCap {
		delete(c.takeOver.foundNothing, oldestKey)
	}
}

// forgetReadOutcomeOf forgets a read's finding about key: a holder hands the
// name off, or a create of it is applied here. A migrate's "nothing" is kept
// for the migrate waiting on it.
func (c *Cluster) forgetReadOutcomeOf(key placementKey) {
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	if found, recorded := c.takeOver.foundNothing[key]; recorded && !found.leave {
		delete(c.takeOver.foundNothing, key)
	}
}

// takeOverClaimed reports whether a take-over or a pull of the bucket holds
// its marker.
func (c *Cluster) takeOverClaimed(database, bucket string) bool {
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	_, claimed := c.migrateActive[migrateKey(database, bucket)]
	return claimed
}

// awaitCopyARemovedNodeMayHold is the question a create at the name's owner
// asks before it creates (BUG-0625, BUG-0627): when this node holds no copy
// and a node removed within the membership history owned the name, that node
// may hold its only copy, which no member knows of — the take-over has not
// been named the bucket until the removed node's ask arrives, and the
// catalog survey reaches members only. So the create does what an operator's
// migrate on the owner does (Cluster.ensureMigrated): it gives the take-over
// leave to copy, which also makes it say when it found the name nowhere, and
// waits for it. A copy that lands answers the create ("already exists", the
// writes kept), even with auto_rebalance off; a take-over that found no live
// copy lets the create go on. A removed node that does not answer within
// removedNodeAnswerWait is not waited for (the fallback's rule, spec
// §20.8.5), so a removed node that is gone delays such a create by one
// take-over round and does not refuse it.
//
// One take-over round can outlast awaitOwnCopy's write-concern timeout (a
// previous owner's ask, then the fallback's): while one runs and blocks on
// nothing a refusal names, the create waits on, up to createTakeOverWait in
// all, then is refused with the hand-off's retry.
//
// Cost (rule 11), per create of a name this node does not hold: a walk of the
// membership history it keeps (removedInHistory) and, after a removal, at
// most gossipHistoryLimit owner lookups in cached rings, with no exchange.
// The round is paid only by a create of a name a removed node owned, for as
// long as the history keeps an epoch it owned the name at. No per-bucket
// state: the leave and the found-nothing mark are the take-over's own
// entries, which its end deletes.
func (c *Cluster) awaitCopyARemovedNodeMayHold(ctx context.Context, database, bucket string) error {
	key := placementOf(database, bucket)
	if ownerQuestionPassOverHook.Load() || c.s.bucketStoreOpen(database, bucket) || !c.ownedByARemovedNode(key) {
		return nil
	}
	c.giveCopyLeave(database, bucket)
	began := time.Now()
	deadline := began.Add(createTakeOverWait * c.wcTimeout)
	for {
		err := c.awaitOwnCopy(ctx, key, database, bucket)
		switch {
		case err == nil, errors.Is(err, ErrMigrateBucketNotFound):
			return nil
		case errors.Is(err, ErrHandoffInProgress) && c.takeOverStatusOf(key).waitsOnMembersOnly():
			// Every removed node that answered holds no live copy, and the
			// round waits on a current member: whether that member's silence
			// refuses the create is the members' survey's question
			// (refuseCreateBesideCopy, owner ruling 52), asked next.
			return nil
		case !errors.Is(err, ErrHandoffInProgress) || c.takeOverStatusOf(key).blocked():
			return err
		case !c.takeOverClaimed(database, bucket):
			// The round may have ended between awaitOwnCopy's last look and
			// this one.
			if c.takeFoundNothing(key, began) {
				return nil
			}
			return err
		case time.Now().After(deadline):
			return err
		}
	}
}

// createTakeOverWait is how many write-concern timeouts a create waits in all
// for a take-over round still running (awaitCopyARemovedNodeMayHold): a
// previous owner's ask and the fallback's each take at most one.
const createTakeOverWait = 3

// The on-demand read (on-demand pull spec §3 to §6, build doc step 1;
// BUG-0340).
//
// After a membership change this node can own a bucket it does not hold yet.
// In the settling window a read of it names the bucket to the worker, which
// stays the only goroutine that opens or lands a copy here, waits a short
// bound, and is answered from the copy the worker lands, which is kept.
// Outside the window "not found" means not found, at the cost of
// readWindowOpen.

// onDemandWait is how long a read in the settling window waits for the
// take-over of its bucket (spec §6.1, owner ruling O-6): a constant, not
// configuration.
const onDemandWait = time.Second

// onDemandOutcome is how a read that took the on-demand path was answered:
// the outcome label of hoarddb_ondemand_reads_total.
type onDemandOutcome string

const (
	onDemandLanded    onDemandOutcome = "landed"
	onDemandForwarded onDemandOutcome = "forwarded"
	onDemandRetry     onDemandOutcome = "retry"
	onDemandNotFound  onDemandOutcome = "not_found"
)

// onDemandMetrics are the on-demand read's two aggregate series (spec §10): a
// counter by outcome and a histogram of the waits. Neither carries a bucket or
// a database label: a series per bucket is unusable at 10^6 buckets
// (rule 11).
type onDemandMetrics struct {
	reads *prometheus.CounterVec
	waits prometheus.Histogram
}

func newOnDemandMetrics() onDemandMetrics {
	reads := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "hoarddb_ondemand_reads_total",
		Help: "Reads at this node of a bucket it owns and had not taken over yet, while a membership change settles, by how they were answered: " +
			"landed (from the copy this node took over during the read), forwarded (to the bucket's previous owner), retry, or not_found. " +
			"Steady retry means a member the take-over waits on does not answer ('nodes' names it).",
	}, []string{"outcome"})
	for _, outcome := range []onDemandOutcome{onDemandLanded, onDemandForwarded, onDemandRetry, onDemandNotFound} {
		reads.WithLabelValues(string(outcome))
	}
	return onDemandMetrics{reads: reads, waits: prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "hoarddb_ondemand_wait_seconds",
		Help:    "How long a read waited for this node to take its bucket over, while a membership change settles. At most 1 second each.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	})}
}

// noteOnDemand records one on-demand read's answer and, when it waited, how
// long.
func (c *Cluster) noteOnDemand(outcome onDemandOutcome, waited time.Duration) {
	c.onDemand.reads.WithLabelValues(string(outcome)).Inc()
	if waited > 0 {
		c.onDemand.waits.Observe(waited.Seconds())
	}
}

// readWindowOpen reports whether a read decided at epoch is in the settling
// window (spec §3.1): the join freeze's published report is of another epoch,
// or not every member has settled it. The report covers joins and removals
// alike. One atomic pointer load: the caller holds the epoch its ownership was
// decided at (ownerView), so no viewMu lock is taken here, and
// settledFloorNow, which takes settleMu and clones a map, is not read.
func (c *Cluster) readWindowOpen(epoch uint64) bool {
	report := c.joinFreeze.Load()
	return report.Epoch != epoch || report.SettledAt < report.Epoch
}

// movedSinceReadFloor is spec §3.3's moved test: the owner of key on the ring
// of the published report's settled floor is not this node, or that ring has
// left the history (the safe side). It is setMovedSinceSettled at factor 1 —
// only an owner's change makes this node the reader's answerer — with the
// report's floor in place of settledFloorNow. ringAt takes historyMu
// exclusively even on a cache hit, so in-window reads of unheld names
// serialise on that leaf lock for one map read.
func (c *Cluster) movedSinceReadFloor(key placementKey) bool {
	floorRing, recorded := c.ringAt(c.joinFreeze.Load().SettledAt)
	if !recorded {
		return true
	}
	owner, err := floorRing.Owner(key.encoded)
	return err != nil || owner.ID != c.self.ID
}

// readsOnDemand reports whether a read at this node, the bucket's owner, is
// answered by onDemandRead: its copy here is not taken over (awaits, §3.2), or
// it holds nothing of the name, pulls nothing (awaits is false), is in the
// settling window and the name's owner moved since the window's floor (§3.3).
// An unmoved name holds nothing because it did not exist here when the window
// opened, and is left to the handler's "not found". A describe keeps the
// GAP-23 G4-G7 answer (taking-over, or the join fence) and is never sent here.
// A restore takes the wait too (step 11, owner ruling R5): it runs against the
// copy that lands; where the wait would answer the synthesised needsCopy
// refusal it asks ensureDatabase's question to the members instead
// (answerOnDemand), which refuses it while they are silent (BUG-0362).
// Outside the window a read of an unheld name pays the lockless bucketServed
// lookup and readWindowOpen; in it, one ringAt.
func (c *Cluster) readsOnDemand(op transport.Opcode, awaits bool, key placementKey, epoch uint64, database, bucket string) bool {
	if op == transport.OpBucketStats {
		return false
	}
	if awaits {
		return true
	}
	if c.s.bucketServed(database, bucket) || c.s.bucketStoreOpen(database, bucket) {
		return false
	}
	if !c.readWindowOpen(epoch) {
		// A name a removed node owned is a miss that says so (owner ruling R8).
		// A restore goes on as a create does after that bound
		// (awaitCopyARemovedNodeMayHold).
		return !isRestoreOpcode(op) && c.removedFormerOwnerOf(key) != ""
	}
	return c.movedSinceReadFloor(key)
}

// takeOverBeforeBoundStoreHook, when set, runs in readBound between its read
// of takeOver.bounds and its store of a new bound, with takeOver.mu held, and
// may block. A test parks the first waiter there. Nil in production.
var takeOverBeforeBoundStoreHook atomic.Pointer[func(nodeID, database, bucket string)]

// readBound is the instant a read decided at epoch stops waiting for key's
// take-over (spec §6.1, R5): the first waiting read stores now + onDemandWait
// with its epoch, and every later read of the same take-over uses it. The read
// of the map and the store are one takeOver.mu section, so two first waiters
// cannot each store their own (R2-1a). A stored bound of an older epoch is
// replaced; a newer-or-equal one wins (R-4). The window is re-checked in the
// same section, against the clears that run under takeOver.mu after the closed
// report is published: a read whose window has closed stores nothing and waits
// its own bound.
func (c *Cluster) readBound(key placementKey, epoch uint64) time.Time {
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	if !c.readWindowOpen(epoch) {
		return time.Now().Add(onDemandWait)
	}
	if stored, found := c.takeOver.bounds[key]; found && stored.epoch >= epoch {
		return stored.until
	}
	if hook := takeOverBeforeBoundStoreHook.Load(); hook != nil {
		database, bucket, _ := key.names()
		(*hook)(c.self.ID, database, bucket)
	}
	bound := waitBound{until: time.Now().Add(onDemandWait), epoch: epoch}
	c.takeOver.bounds[key] = bound
	return bound.until
}

// clearReadOutcomes forgets every read's bound and every finding but a
// migrate's (spec §4.2, §6.1): at an adoption, and when the settling window
// closes. Called under membershipFileMu or recomputeMu, so the order is that
// lock → takeOver.mu, which no path inverts.
func (c *Cluster) clearReadOutcomes() {
	c.takeOver.mu.Lock()
	c.clearReadOutcomesLocked()
	c.takeOver.mu.Unlock()
}

// clearReadOutcomesLocked is clearReadOutcomes with takeOver.mu held. A
// "nothing" a migrate gave leave for is kept: the migrate may not have polled
// it yet, and is answered ErrMigrateBucketNotFound rather than its hand-off
// refusal at its deadline (F-6).
func (c *Cluster) clearReadOutcomesLocked() {
	clear(c.takeOver.bounds)
	maps.DeleteFunc(c.takeOver.foundNothing, func(_ placementKey, found foundOutcome) bool { return !found.leave })
}

// recordWindowOutcomeLocked records a read-named take-over's finding while the
// settling window is open for the epoch the round decided at (takingOver's):
// the window is evaluated under takeOver.mu, held by the caller, so no write
// lands after the close's clear (F-6).
func (c *Cluster) recordWindowOutcomeLocked(bucketTakeOver *takingOver, unheard []string) {
	if !c.readWindowOpen(bucketTakeOver.epoch) {
		return
	}
	c.recordOutcome(bucketTakeOver.key, foundOutcome{at: time.Now(), epoch: bucketTakeOver.epoch, unheard: unheard})
}

// cachedOutcome is the finding a read decided at epoch may answer from: one
// of that epoch, and, for members that did not answer, younger than
// onDemandUnheardTTL.
func (c *Cluster) cachedOutcome(key placementKey, epoch uint64) (foundOutcome, bool) {
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	found, recorded := c.takeOver.foundNothing[key]
	if !recorded || found.epoch != epoch || (len(found.unheard) > 0 && time.Since(found.at) >= onDemandUnheardTTL) {
		return foundOutcome{}, false
	}
	return found, true
}

// answerFromOutcome answers a read from the take-over's finding (spec §6.8): no
// live copy anywhere is the handler's ordinary "not found" with its create
// advice; members that did not answer are retry naming them, the way
// refuseOverUnheard names them (O-4), when unheardToo. A held copy is never
// answered "not found" from a finding.
func (c *Cluster) answerFromOutcome(op transport.Opcode, key placementKey, epoch uint64, database, bucket string, unheardToo bool, began time.Time) (onDemandAnswer, bool) {
	found, recorded := c.cachedOutcome(key, epoch)
	switch {
	case !recorded || c.s.bucketServed(database, bucket):
		return onDemandAnswer{}, false
	case len(found.unheard) == 0:
		// In the window a name a removed node owned waits for that node's
		// hand-off: it is a retry naming the node until its notice arrives or
		// it is confirmed gone, then a miss that names it (owner ruling R8).
		if node := c.awaitedRemovedOwnerOf(key); node != "" {
			return onDemandAnswer{refusal: removedOwnerAwaited(database, bucket, node, "nothing was read"), outcome: onDemandRetry, waited: waitedSince(began)}, true
		}
		// A restore of a name found nowhere goes on to its handler, as a
		// create does.
		if node := c.removedFormerOwnerOf(key); node != "" && !isRestoreOpcode(op) {
			return onDemandAnswer{refusal: c.missNamingRemovedOwner(database, bucket, node), outcome: onDemandNotFound,
				waited: waitedSince(began)}, true
		}
		return onDemandAnswer{serve: true, outcome: onDemandNotFound, waited: waitedSince(began)}, true
	case unheardToo:
		return onDemandAnswer{refusal: unheardMembersRefusal(database, bucket, found.unheard, "Nothing was read"),
			outcome: onDemandRetry, waited: waitedSince(began)}, true
	}
	return onDemandAnswer{}, false
}

// waitedSince is how long a read has waited since began, or nothing when it
// has not begun to wait.
func waitedSince(began time.Time) time.Duration {
	if began.IsZero() {
		return 0
	}
	return time.Since(began)
}

// onDemandAnswer is how onDemandRead answers a read: from this node's copy
// (serve), sent on to source (forward: the final previous owner, with the
// serve-or-refuse mark; reroute: the new owner, with waitLeft), or refused.
// An empty outcome records nothing: a rerouted read is answered, and counted,
// where it is rerouted to.
type onDemandAnswer struct {
	serve    bool
	forward  bool
	reroute  bool
	source   ring.Node
	waitLeft time.Duration
	refusal  error
	outcome  onDemandOutcome
	waited   time.Duration
}

// onDemandRequest is one read onDemandRead answers. wait is the most it may
// wait here: the whole bound at the client's door, what a forwarded read's
// ReadWaitMs left it, or nothing past the internode waiters' cap (spec §8).
// rerouted says it was rerouted here once already, so it is not again.
type onDemandRequest struct {
	op       transport.Opcode
	key      placementKey
	epoch    uint64
	database string
	bucket   string
	wait     time.Duration
	rerouted bool
}

// onDemandRead answers a read at the client's door of a bucket this node owns
// and has not taken over (readsOnDemand): it may wait the whole bound.
func (c *Cluster) onDemandRead(ctx context.Context, op transport.Opcode, key placementKey, epoch uint64, database, bucket string) onDemandAnswer {
	return c.answerOnDemand(ctx, onDemandRequest{op: op, key: key, epoch: epoch, database: database, bucket: bucket, wait: onDemandWait})
}

// answerOnDemand answers an on-demand read in spec §6.8's order. Before the
// placement pass of the epoch has finished the worker takes nothing over and
// nothing is known of a source, so the read names its bucket and answers retry
// at once (N-1). A read whose take-over has heard from the final previous
// owner is sent there at once when this node must copy the bucket whole (it
// holds nothing, its copy is shipping, or auto_rebalance is off: §6.1, §6.6);
// with auto_rebalance off and nothing to open here it is refused at once with
// the 'migrate' advice, since no copy lands without the operator (§6.6,
// stop-and-ask 14). Otherwise it names the bucket to the worker and waits, as
// awaitTakeOver does, on the bucket's marker or a takeOverWaitStep, naming it
// again each step, holding no fence, up to its bound: answered from the copy
// once it is served and opened here, rerouted once to the new owner if an
// adoption moved the bucket away (§8), and past the bound sent to the final
// previous owner if the take-over has heard from it, else refused with the
// hand-off's retry. A restore is never sent on: only the owner restores
// (holderServes). The take-over is never cancelled when a read gives up; the
// landed copy is kept.
func (c *Cluster) answerOnDemand(ctx context.Context, read onDemandRequest) onDemandAnswer {
	database, bucket := read.database, read.bucket
	// After the window a name a removed node owned is an ordinary miss that
	// names the node (owner ruling R8); a restore goes on as a create does.
	if !c.readWindowOpen(read.epoch) && !isRestoreOpcode(read.op) {
		if node := c.removedFormerOwnerOf(read.key); node != "" {
			// Unless that node handed the bucket here and it has not landed:
			// the node said it holds it (BUG-0688).
			if handing := c.handedOffBy(read.key); handing != "" {
				return onDemandAnswer{refusal: removedOwnerAwaited(database, bucket, handing, "nothing was read"), outcome: onDemandRetry}
			}
			return onDemandAnswer{refusal: c.missNamingRemovedOwner(database, bucket, node), outcome: onDemandNotFound}
		}
	}
	if answer, cached := c.answerFromOutcome(read.op, read.key, read.epoch, database, bucket, true, time.Time{}); cached {
		return answer
	}
	if !c.placementPassFinished() {
		c.wakeTakeOverCapped(database, bucket)
		return onDemandAnswer{refusal: c.handoffInProgress(database, bucket), outcome: onDemandRetry}
	}
	if answer, sent := c.forwardAtOnce(read, 0); sent {
		return answer
	}
	if !c.s.bucketServed(database, bucket) && !c.mayCopyWhole(read.key) {
		// A restore is not told to migrate: it goes on to ensureDatabase's
		// question to the members, which refuses it while they are silent and
		// otherwise lets it decide exists, merge or create (owner ruling R5).
		if isRestoreOpcode(read.op) {
			return onDemandAnswer{serve: true}
		}
		status := c.takeOverStatusOf(read.key)
		status.needsCopy = true
		return onDemandAnswer{refusal: c.handoffInProgressAs(database, bucket, status), outcome: onDemandRetry}
	}
	// Past the internode waiters' cap, or the queue is full: the read is
	// answered past the bound at once.
	if read.wait <= 0 || !c.wakeTakeOverCapped(database, bucket) {
		return c.pastTheBound(read, 0)
	}
	began := time.Now()
	deadline := c.readBound(read.key, read.epoch)
	if own := began.Add(read.wait); own.Before(deadline) {
		deadline = own
	}
	for {
		if owner, _, err := c.ownerView(read.key); err == nil && owner.ID != c.self.ID {
			return c.rerouteOnDemand(read, owner, deadline, began)
		}
		awaits, err := c.ownerReadAwaitsTakeOver(read.key, read.epoch, database, bucket)
		if err != nil {
			return onDemandAnswer{refusal: err, outcome: onDemandRetry, waited: time.Since(began)}
		}
		if !awaits && c.s.bucketServed(database, bucket) {
			return onDemandAnswer{serve: true, outcome: onDemandLanded, waited: time.Since(began)}
		}
		// A take-over that found no live copy anywhere while this read waited
		// answers it now. One that found members silent is still undecided,
		// and the read waits its bound (spec §6.1).
		if answer, cached := c.answerFromOutcome(read.op, read.key, read.epoch, database, bucket, false, began); cached {
			return answer
		}
		if answer, sent := c.forwardAtOnce(read, time.Since(began)); sent {
			return answer
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		// The read names its bucket again each step, as awaitTakeOver does: a
		// round that left it unsettled may have dropped its name at the cap
		// (requeueUnsettled), and the read keeps no marker of its own (rule
		// 11), so a drop costs it one step.
		c.wakeTakeOverCapped(database, bucket)
		c.migrateMu.Lock()
		done := c.migrateActive[migrateKey(database, bucket)]
		c.migrateMu.Unlock()
		step := time.NewTimer(min(remaining, takeOverWaitStep))
		select {
		case <-done:
		case <-step.C:
		case <-ctx.Done():
			step.Stop()
			return onDemandAnswer{refusal: fmt.Errorf("waiting for this node to take over bucket %q in database %q: %w", bucket, database, ctx.Err()),
				outcome: onDemandRetry, waited: time.Since(began)}
		}
		step.Stop()
	}
	return c.pastTheBound(read, time.Since(began))
}

// forwardAtOnce is the read's answer, before or while it waits, when the
// take-over has heard the final previous owner and this node has no copy to
// open in the bound (spec §6.1, O-6 (b)): it holds nothing, so the plan is a
// whole copy; its copy is shipping in (pullRunning, not the take-over's own
// marker pullPending reads); or auto_rebalance is off, so no copy
// lands without the operator (§6.6). A restore is never sent on. Whether a
// copy is held is read lock-free (bucketServed), as the read gate reads it: a
// read never waits on the bucket's lock (query-pipelining spec §3.5, I-1), and
// a copy mid-materialise reads as none, which forwards a read the previous
// owner can answer.
func (c *Cluster) forwardAtOnce(read onDemandRequest, waited time.Duration) (onDemandAnswer, bool) {
	if isRestoreOpcode(read.op) {
		return onDemandAnswer{}, false
	}
	status := c.takeOverStatusOf(read.key)
	if !status.sourceKnown {
		return onDemandAnswer{}, false
	}
	if c.s.bucketServed(read.database, read.bucket) && !c.pullRunning(read.database, read.bucket) && c.mayCopyWhole(read.key) {
		return onDemandAnswer{}, false
	}
	return onDemandAnswer{forward: true, source: status.source, outcome: onDemandForwarded, waited: waited}, true
}

// pastTheBound is the read's answer once its bound is spent (spec §6.8): sent
// to the final previous owner when the take-over has heard from it, else the
// hand-off's retry. A restore is never sent on.
func (c *Cluster) pastTheBound(read onDemandRequest, waited time.Duration) onDemandAnswer {
	if status := c.takeOverStatusOf(read.key); status.sourceKnown && !isRestoreOpcode(read.op) {
		return onDemandAnswer{forward: true, source: status.source, outcome: onDemandForwarded, waited: waited}
	}
	// A name this node holds nothing of waits for the removed node it was
	// placed on (owner ruling R8); a copy held here is the take-over's, and
	// keeps the hand-off's retry.
	if !c.s.bucketServed(read.database, read.bucket) {
		if node := c.awaitedRemovedOwnerOf(read.key); node != "" {
			return onDemandAnswer{refusal: removedOwnerAwaited(read.database, read.bucket, node, "nothing was read"), outcome: onDemandRetry, waited: waited}
		}
	}
	return onDemandAnswer{refusal: c.handoffInProgress(read.database, read.bucket), outcome: onDemandRetry, waited: waited}
}

// rerouteOnDemand is the read's answer when an adoption moved its bucket to
// owner while it waited (spec §8): rerouted once, with what is left of its
// bound, keyed on the request. A read rerouted here already, and a restore,
// are answered retry instead.
func (c *Cluster) rerouteOnDemand(read onDemandRequest, owner ring.Node, deadline, began time.Time) onDemandAnswer {
	waited := time.Since(began)
	if read.rerouted || isRestoreOpcode(read.op) {
		return onDemandAnswer{refusal: readMovedOnRetry(read.database, read.bucket), outcome: onDemandRetry, waited: waited}
	}
	return onDemandAnswer{reroute: true, source: owner, waitLeft: max(time.Until(deadline), time.Millisecond), waited: waited}
}

// readMovedOnRetry refuses a read that was rerouted once already and finds its
// bucket owned elsewhere again: the chain of forwards stops here (spec §8).
func readMovedOnRetry(database, bucket string) error {
	return &sentencedError{cause: ErrHandoffInProgress, message: fmt.Sprintf(
		"bucket %q in database %q moved to another node while this read waited for it, and the read was already sent on once; "+
			"nothing was read. Retry in a moment: it goes to the bucket's owner ('nodes' shows the members)", bucket, database)}
}

// onDemandInternodeWaiters bounds how many forwarded reads may wait here at
// once (spec §8): past it, a forwarded read is answered past the bound at
// once, so a storm of forwards cannot hold every request handler.
const onDemandInternodeWaiters = 64

// forwardedReadOnDemand is answerOnDemand for a read a coordinator or an
// owner forwarded here (handleReplPush): it waits at most what its ReadWaitMs
// left it.
func (c *Cluster) forwardedReadOnDemand(ctx context.Context, read onDemandRequest) onDemandAnswer {
	if c.onDemandInternodeWaiting.Add(1) > onDemandInternodeWaiters {
		read.wait = 0
	}
	defer c.onDemandInternodeWaiting.Add(-1)
	return c.answerOnDemand(ctx, read)
}

// sendOnDemandAnswer carries out an on-demand answer that is not served from
// this node's copy: the forward to the final previous owner with the
// serve-or-refuse mark, the reroute to the new owner, or the refusal. served
// reports that the read is answered from this node's copy, by the caller.
func (c *Cluster) sendOnDemandAnswer(ctx context.Context, frame *transport.Frame, bucket string, answer onDemandAnswer) (reply *transport.Frame, served bool, err error) {
	if answer.outcome != "" {
		c.noteOnDemand(answer.outcome, answer.waited)
	}
	switch {
	case answer.serve:
		return nil, true, nil
	case answer.forward:
		reply, err = c.forwardRead(ctx, frame, answer.source, bucket, readForward{serveFromCopy: true})
	case answer.reroute:
		reply, err = c.forwardRead(ctx, frame, answer.source, bucket, readForward{waitMs: answer.waitLeft.Milliseconds(), rerouted: true})
	default:
		reply = errorFrame(frame, answer.refusal)
	}
	return reply, false, err
}

// The worker (step 10b.5).

const (
	// takeOverRetryFloor and takeOverRetryCeiling bound the worker's back-off
	// while something is unsettled; a wake resets it to the floor.
	takeOverRetryFloor   = 200 * time.Millisecond
	takeOverRetryCeiling = 30 * time.Second
	// removedNodeAnswerWait is how long the fallback waits for a node that
	// left the membership within the history: it is asked without being
	// waited for (spec §20.8.5).
	removedNodeAnswerWait = 3 * time.Second
	// strayWarnInterval spaces the WARN that names two live incarnations of a
	// name the fallback found.
	strayWarnInterval = time.Hour
)

// takeOverOffHook stops every node's worker when set: a test of the placement
// pass reads the marks a change records before the worker clears them as it
// settles the copies. False in production.
var takeOverOffHook atomic.Bool

// takeOverPausedHook, when set, stops the worker of each node it answers true
// for: a test keeps some nodes' copies as a membership change left them while
// others take their buckets over. Nil in production.
var takeOverPausedHook atomic.Pointer[func(nodeID string) bool]

// takeOverPaused reports whether takeOverPausedHook stops this node's worker.
func (c *Cluster) takeOverPaused() bool {
	hook := takeOverPausedHook.Load()
	return hook != nil && (*hook)(c.self.ID)
}

// takeOverBeforeOpenHook, when set, runs on a node's worker once it has
// decided how to take a bucket over and before it opens it or copies it in,
// with the bucket's marker registered, and may block. Tests hold a take-over
// with it. Nil in production.
var takeOverBeforeOpenHook atomic.Pointer[func(nodeID, database, bucket string)]

// takeOverBeforeReadHook, when set, runs on a node's worker just before it
// reads one bucket's copy from the node store in a round (the owned buckets of
// takeOverOwned, the followed ones of followOwners), and may block. Tests hold
// a round mid-read with it. Nil in production.
var takeOverBeforeReadHook atomic.Pointer[func(nodeID, database, bucket string)]

// takeOverAfterReleaseHook, when set, runs on a node's worker once it has
// deleted its own copy of a bucket to copy a more complete one over it, before
// the pull, and may block. Tests stop the node there. Nil in production.
var takeOverAfterReleaseHook atomic.Pointer[func(nodeID, database, bucket string)]

// names decodes a user bucket's placement key (storage.PlacementKey).
func (k placementKey) names() (database, bucket string, ok bool) {
	raw := []byte(k.encoded)
	length, read := binary.Uvarint(raw)
	if read <= 0 || uint64(len(raw)-read) < length {
		return "", "", false
	}
	database, raw = string(raw[read:read+int(length)]), raw[read+int(length):]
	length, read = binary.Uvarint(raw)
	if read <= 0 || uint64(len(raw)-read) != length {
		return "", "", false
	}
	return database, string(raw[read:]), true
}

// runTakeOver is the take-over worker: one goroutine per node, owned by the
// cluster's WaitGroup, woken by an adoption, by a refusal that names a bucket,
// by a holder's or an operator's migrate, and by a back-off timer while
// anything is unsettled. It holds no fence and no lock across an exchange
// with another node (spec §20.8.4).
func (c *Cluster) runTakeOver() {
	defer c.storeWG.Done()
	wait := takeOverRetryFloor
	for {
		timer := time.NewTimer(wait)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return
		case <-c.takeOver.wake:
			wait = takeOverRetryFloor
		case <-timer.C:
		}
		timer.Stop()
		left, soon := c.takeOverRound(c.ctx)
		switch {
		case left == 0:
			wait = takeOverRetryCeiling
		case soon:
			wait = takeOverRetryFloor
		default:
			wait = min(2*wait, takeOverRetryCeiling)
		}
	}
}

// takeOverRound settles what it can of this node's unsettled copies, the
// buckets named to it first (A-10.7), and returns how many it left and
// whether one of them waits on a stream that is delivering now. A removed node
// takes nothing over; a node whose placement pass has not finished waits for
// its marks. Each bucket does what this node's role in its set asks: its owner
// takes it over (step 10b.5); another member follows the owner (step 10b.6);
// a node outside the set has nothing to do here — the sweep releases its
// copy. Before each completion a bucket named meanwhile is taken: first if it
// is the round's, at once in a nested pass if not (spec §5). The questions are
// asked in frames, one per node asked, not one per bucket (rule 11).
func (c *Cluster) takeOverRound(ctx context.Context) (int, bool) {
	if c.removed() || takeOverOffHook.Load() || c.takeOverPaused() {
		return 0, false
	}
	if !c.placementPassFinished() {
		return 1, true
	}
	work := c.takeOverWork()
	outcomes := make(map[heldBucket]copyOutcome, len(work))
	nesting := &nestedPasses{inRound: make(map[placementKey]bool, len(work)), ran: make(map[placementKey]heldBucket)}
	for _, held := range work {
		nesting.inRound[placementOf(held.database, held.bucket)] = true
	}
	c.runRoundItems(ctx, work, outcomes, nesting)
	left, soon := 0, false
	var unsettled []heldBucket
	// A nested bucket is not in work: put back with it, or a nested bucket the
	// pass left pending would wait for a read to name it again (ODP-5).
	for _, held := range append(work, slices.SortedFunc(maps.Values(nesting.ran), compareHeldBuckets)...) {
		outcome := outcomes[held]
		if outcome == copySettled {
			continue
		}
		left++
		soon = soon || outcome == copyAwaitsStream
		unsettled = append(unsettled, held)
	}
	c.requeueUnsettled(unsettled)
	// The settle mark a sweep could not raise while a name was unfound
	// (takeOverUnfound) is raised once the take-over has found them all.
	if !c.takeOverUnfound() {
		c.raiseSettleIfClear()
	}
	return left, soon
}

// runRoundItems takes over or follows each of items by this node's role in its
// set, the round's and a nested pass's alike: its owner takes it over (step
// 10b.5); another member with an open copy follows the owner (step 10b.6); a
// node outside the set has nothing to do here — the sweep releases its copy.
// nesting is nil in a nested pass, which does not nest again.
func (c *Cluster) runRoundItems(ctx context.Context, items []heldBucket, outcomes map[heldBucket]copyOutcome, nesting *nestedPasses) {
	var owned []roundItem
	followers := make(map[string][]roundItem)
	for _, held := range items {
		key := placementOf(held.database, held.bucket)
		replicas, err := c.Replicas(key)
		switch {
		case err != nil:
			outcomes[held] = copyPending
		case replicas[0].ID == c.self.ID:
			owned = append(owned, roundItem{held: held, key: key, owner: replicas[0]})
		case nodeListed(replicas, c.self.ID) && c.s.bucketStoreOpen(held.database, held.bucket):
			followers[replicas[0].ID] = append(followers[replicas[0].ID], roundItem{held: held, key: key, owner: replicas[0]})
		default:
			// Neither the owner nor a follower with an open copy: no
			// endTakeOver reaches this bucket here, so its status and a read's
			// bound end with this arm (on-demand pull spec §6.1, F-2).
			c.takeOver.mu.Lock()
			delete(c.takeOver.status, key)
			delete(c.takeOver.bounds, key)
			delete(c.takeOver.handedOff, key)
			c.takeOver.mu.Unlock()
			outcomes[held] = copySettled
		}
	}
	c.takeOverOwned(ctx, owned, outcomes, nesting)
	for _, ownerID := range slices.Sorted(maps.Keys(followers)) {
		c.followOwners(ctx, followers[ownerID], outcomes)
	}
}

// nestedPasses is what a round's boundaries need to run nested passes (spec
// §5): which buckets are the round's, so that a name of one of them is the
// round's to complete, and every bucket a nested pass ran, so that the round
// puts back the ones it left pending.
type nestedPasses struct {
	inRound map[placementKey]bool
	ran     map[placementKey]heldBucket
}

// takeOverUnfound reports whether this node's take-over has a name it has not
// found a copy for yet: one named and not taken, or one whose take-over is in
// progress (owner ruling R6, 2026-10-21). While it does, the node does not report
// itself settled, so the settling window stays open and a read or a restore of
// such a bucket takes the on-demand wait instead of answering from an empty
// store: after a removal at factor 1, the removed owner's hand-off names the
// bucket to its successor, which holds nothing of it, and its own sweep would
// otherwise settle at once. A take-over that cannot decide (a member silent)
// keeps the node unsettled exactly as a sweep's residue does (the sweep gives
// up retrying after sweepRetryGiveUp and stays unsettled until its members
// answer, the next change, or the next start): the take-over retries until the
// member answers or is removed. A bucket a removed node handed here counts
// until its take-over ends (BUG-0688): a round clears named when it takes the
// name, and a copy in progress records no status, so without it the node
// settled mid-copy, or after a round that did not hear the removed node, once
// that node's notice had arrived. O(1): three map lengths under takeOver.mu.
func (c *Cluster) takeOverUnfound() bool {
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	return len(c.takeOver.named) > 0 || len(c.takeOver.status) > 0 || len(c.takeOver.handedOff) > 0
}

// requeueUnsettled puts the buckets a round did not settle back in
// takeOver.named, so a bucket a client or a holder named stays first in line.
// A bucket still being moved (marked in c.unsettled), given a migrate's leave,
// or waited on for its origin's stream always goes back. Any other bucket — a
// name a read, a refusal or a holder's ask named — goes back only while the
// queue holds fewer than onDemandNamedCap names (owner ruling R3, 2026-10-20):
// a read storm over missing names with a member silent never settles them, and
// re-inserting them all let the queue grow by up to the cap every round. A
// bucket left out loses its status and its read bound, as the round's
// default arm does; its next read names it again, and its unheard finding
// answers reads in between. The marks are read under placementMu first, so
// takeOver.mu stays a leaf; the cap check and the inserts are one takeOver.mu
// section, as in wakeTakeOverCapped.
func (c *Cluster) requeueUnsettled(unsettled []heldBucket) {
	if len(unsettled) == 0 {
		return
	}
	keys := make([]placementKey, len(unsettled))
	marked := make([]bool, len(unsettled))
	c.placementMu.Lock()
	for index, held := range unsettled {
		keys[index] = placementOf(held.database, held.bucket)
		_, marked[index] = c.unsettled[keys[index]]
	}
	c.placementMu.Unlock()
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	capped := make([]int, 0, len(unsettled))
	for index, held := range unsettled {
		key := keys[index]
		_, waited := c.takeOver.waiting[key]
		if marked[index] || waited || c.takeOver.leave[key] {
			c.takeOver.named[key] = held
			continue
		}
		capped = append(capped, index)
	}
	for _, index := range capped {
		key := keys[index]
		if _, queued := c.takeOver.named[key]; queued || len(c.takeOver.named) < onDemandNamedCap {
			c.takeOver.named[key] = unsettled[index]
			continue
		}
		delete(c.takeOver.status, key)
		delete(c.takeOver.bounds, key)
	}
	if hook := requeueUnsettledDoneHook.Load(); hook != nil {
		(*hook)(len(c.takeOver.named))
	}
}

// requeueUnsettledDoneHook, when set, runs at the end of requeueUnsettled with
// takeOver.mu held and the length of the queue after the round's re-inserts.
// The queue is over its cap only inside that one lock section, where a sampler
// cannot see it, so a test records the largest length here. Nil in production.
var requeueUnsettledDoneHook atomic.Pointer[func(namedAfter int)]

// leavePending marks buckets a round did not reach as waiting for the next
// one: a bucket with no outcome would read as settled and drop out of the work.
func leavePending(outcomes map[heldBucket]copyOutcome, items []roundItem) {
	for _, item := range items {
		outcomes[item.held] = copyPending
	}
}

// roundItem is one bucket of a round, with its ring owner now.
type roundItem struct {
	held  heldBucket
	key   placementKey
	owner ring.Node
}

// noteOutcome records what a round did with a bucket, and logs a reason it
// did not finish.
func (c *Cluster) noteOutcome(outcomes map[heldBucket]copyOutcome, held heldBucket, outcome copyOutcome, err error) {
	outcomes[held] = outcome
	if err != nil {
		c.logger.Warn("the take-over of a bucket could not finish this round; it is tried again",
			"database", held.database, "bucket", held.bucket, "error", err)
	}
}

// takeOverWork is the buckets named to the worker since the last round, then
// every bucket whose copy here carries a mark: O(named + unsettled).
func (c *Cluster) takeOverWork() []heldBucket {
	c.takeOver.mu.Lock()
	work := make([]heldBucket, 0, len(c.takeOver.named))
	for _, named := range c.takeOver.named {
		work = append(work, named)
	}
	clear(c.takeOver.named)
	c.takeOver.mu.Unlock()
	slices.SortFunc(work, compareHeldBuckets)
	listed := len(work)
	c.placementMu.Lock()
	marked := make([]placementKey, 0, len(c.unsettled))
	for key := range c.unsettled {
		marked = append(marked, key)
	}
	c.placementMu.Unlock()
	for _, key := range marked {
		database, bucket, ok := key.names()
		held := heldBucket{database: database, bucket: bucket}
		if ok && !slices.Contains(work[:listed], held) {
			work = append(work, held)
		}
	}
	slices.SortFunc(work[listed:], compareHeldBuckets)
	return work
}

func compareHeldBuckets(a, b heldBucket) int {
	return strings.Compare(a.database+"\x00"+a.bucket, b.database+"\x00"+b.bucket)
}

// copyOutcome is what one round did with one bucket.
type copyOutcome int

const (
	// copySettled: nothing is left to do for the bucket here.
	copySettled copyOutcome = iota
	// copyPending: it waits for something the next round asks again.
	copyPending
	// copyAwaitsStream: it waits for entries a stream is delivering now.
	copyAwaitsStream
)

// askHolderHook, when set, runs before this node asks node about the copies
// it holds; an error it returns is taken as node's silence. Tests make a
// removed node unheard with it. Nil in production.
var askHolderHook atomic.Pointer[func(askerID, nodeID string) error]

// askFrames asks node about refs in frames of at most handoffAskLimit, within
// wait in all.
func (c *Cluster) askFrames(ctx context.Context, node ring.Node, refs []ReplBucketRef, wait time.Duration) ([]ReplCopy, error) {
	if hook := askHolderHook.Load(); hook != nil {
		if err := (*hook)(c.self.ID, node.ID); err != nil {
			return nil, err
		}
	}
	askCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	return c.askCopies(askCtx, node, refs)
}

// refsOf is the bucket references of a round's buckets, in order.
func refsOf(roundBuckets []roundItem) []ReplBucketRef {
	refs := make([]ReplBucketRef, 0, len(roundBuckets))
	for _, roundBucket := range roundBuckets {
		refs = append(refs, ReplBucketRef{Database: roundBucket.held.database, Bucket: roundBucket.held.bucket})
	}
	return refs
}

// claimTakeOver registers the bucket's marker (migrateActive), which a create,
// a replica's pull, a migrate and a read wait on, for the take-over's whole
// duration (review requirement 3). It reports false while another pull of
// the bucket holds it.
func (c *Cluster) claimTakeOver(held heldBucket) (func(), bool) {
	key := migrateKey(held.database, held.bucket)
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	if _, busy := c.migrateActive[key]; busy {
		return nil, false
	}
	done := make(chan struct{})
	c.migrateActive[key] = done
	return func() {
		c.migrateMu.Lock()
		delete(c.migrateActive, key)
		close(done)
		c.migrateMu.Unlock()
	}, true
}

// setTakeOverStatus records what a take-over waits for, for its refusal.
func (c *Cluster) setTakeOverStatus(key placementKey, status takeOverStatus) {
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	c.takeOver.status[key] = status
}

// endTakeOver forgets a take-over that finished, the leave to copy it, and
// any wait for its origin's stream, which lives only while a copy waits
// (rule 11).
func (c *Cluster) endTakeOver(key placementKey) {
	c.takeOver.mu.Lock()
	delete(c.takeOver.status, key)
	delete(c.takeOver.leave, key)
	delete(c.takeOver.waiting, key)
	delete(c.takeOver.limitWarnedAt, key)
	delete(c.takeOver.bounds, key)
	delete(c.takeOver.handedOff, key)
	c.takeOver.mu.Unlock()
}

// giveCopyLeave lets the worker copy the bucket whole with auto_rebalance off,
// as an operator's migrate does (A-10.6).
func (c *Cluster) giveCopyLeave(database, bucket string) {
	c.takeOver.mu.Lock()
	c.takeOver.leave[placementOf(database, bucket)] = true
	c.takeOver.mu.Unlock()
}

// mayCopyWhole reports whether the worker may copy the bucket whole on its
// own: auto_rebalance is on, or an operator's migrate gave leave.
func (c *Cluster) mayCopyWhole(key placementKey) bool {
	if c.s.config.Server.AutoRebalance {
		return true
	}
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	return c.takeOver.leave[key]
}

// forgetTakeOverOf takes a dropped name off the worker's queue (R8-C).
func (c *Cluster) forgetTakeOverOf(key placementKey) {
	c.takeOver.mu.Lock()
	delete(c.takeOver.named, key)
	delete(c.takeOver.leave, key)
	delete(c.takeOver.status, key)
	delete(c.takeOver.waiting, key)
	delete(c.takeOver.strayWarnedAt, key)
	delete(c.takeOver.limitWarnedAt, key)
	delete(c.takeOver.foundNothing, key)
	delete(c.takeOver.bounds, key)
	delete(c.takeOver.handedOff, key)
	c.takeOver.mu.Unlock()
}

// copyLineage is the content a copy holds, as a pair: its own term and
// position, or, for a copy at position 0 in a term above its first, the
// lineage it was opened or accepted from — a term with no entry in it is
// transparent (spec §20.8.1; A-10.2).
func copyLineage(term, through, baseTerm, baseThrough uint64) (uint64, uint64) {
	if through == 0 && (baseTerm != 0 || baseThrough != 0) {
		return baseTerm, baseThrough
	}
	return term, through
}

// holdsAtLeast reports whether copy c holds everything copy m holds: both of
// one incarnation (I-H1), and c's content at or past m's in m's term, or c in
// a later term whose base is at or past m (R8-B).
func holdsAtLeast(c, m ReplCopy) bool {
	if !c.Holds || !m.Holds || c.Incarnation != m.Incarnation {
		return false
	}
	mTerm, mThrough := copyLineage(m.Term, m.Through, m.BaseTerm, m.BaseThrough)
	cTerm, cThrough := copyLineage(c.Term, c.Through, c.BaseTerm, c.BaseThrough)
	switch {
	case cTerm == mTerm:
		return cThrough >= mThrough
	case c.Term > m.Term:
		return c.BaseTerm > mTerm || (c.BaseTerm == mTerm && c.BaseThrough >= mThrough)
	default:
		return false
	}
}

// heldCopy is one node's answer about one bucket, with who gave it.
type heldCopy struct {
	node   ring.Node
	answer ReplCopy
	// self is this node's own copy, current a member of the membership.
	self, current bool
}

// takeOverPlan is how a take-over completes.
type takeOverPlan struct {
	// best is the copy the bucket is taken from, held by best.node, and
	// alternatives the other copies that hold everything it does, which a
	// copy is pulled from when best's holder refuses it.
	best         heldCopy
	alternatives []heldCopy
	// ordinary says the newest previous owner that opened answered: its copy
	// is the end of its term, and its stream still delivers.
	ordinary bool
	// resume says the best copy is this node's own opened one, and every
	// previous owner answered that it had not opened since (A-10.1).
	resume bool
	// nothing says no node holds a live copy: there is nothing to take over.
	nothing bool
}

// errNotYetDecided is a take-over that cannot decide this round: a member did
// not answer, a node is on another epoch, or two incarnations are live.
var errNotYetDecided = errors.New("the take-over cannot decide which copy is the bucket's yet")

// takeOverCandidate is a previous owner of a bucket, and the epoch at which
// it ceded it.
type takeOverCandidate struct {
	node  ring.Node
	ceded uint64
}

// takeOverCandidates is every node, other than this one, that was the
// bucket's ring owner at an epoch from own's moved mark − 1 (the current
// epoch − 1 for a copy with no mark, or none) to the current epoch − 1, newest
// first, at its address in the history (A-10.1) — and, while none is found,
// further back: a change that did not move the bucket (an unrelated join
// after the one that did) leaves this node the owner at epoch − 1, and a node
// holding no copy has no mark that says when the bucket moved (T-140).
// complete is false when one of those epochs has left the history.
func (c *Cluster) takeOverCandidates(key placementKey, own ReplCopy, epoch uint64) (candidates []takeOverCandidate, complete bool) {
	if epoch <= InitialEpoch {
		return nil, true
	}
	oldest := epoch - 1
	if own.Holds && own.Moved > InitialEpoch && own.Moved-1 < oldest {
		oldest = own.Moved - 1
	}
	complete = true
	var seen []string
	for at := epoch - 1; (at >= oldest || len(candidates) == 0) && at >= InitialEpoch; at-- {
		placed, recorded := c.ringAt(at)
		if !recorded {
			complete = false
			continue
		}
		owner, err := placed.Owner(key.encoded)
		if err != nil || owner.ID == c.self.ID || slices.Contains(seen, owner.ID) {
			continue
		}
		seen = append(seen, owner.ID)
		candidates = append(candidates, takeOverCandidate{node: owner, ceded: at + 1})
	}
	return candidates, complete
}

// errQuestionAcrossEpochs is a question refused because the asked node is on
// another membership epoch: not an absence, a "not yet".
var errQuestionAcrossEpochs = errors.New("the asked node is on another membership epoch")

// askOne asks node about one bucket, within wait.
func (c *Cluster) askOne(ctx context.Context, node ring.Node, held heldBucket, wait time.Duration) (ReplCopy, error) {
	answers, err := c.askFrames(ctx, node, []ReplBucketRef{{Database: held.database, Bucket: held.bucket}}, wait)
	if err != nil {
		return ReplCopy{}, fmt.Errorf("asking %s about bucket %q in database %q: %w", node.ID, held.bucket, held.database, err)
	}
	return answers[0], nil
}

// takingOver is one bucket a round takes over, as the decision is made.
type takingOver struct {
	roundItem
	own        heldCopy
	candidates []takeOverCandidate
	// next is the candidate the ordinary path asks next.
	next int
	// everyAnswered is false once a previous owner did not answer, or an
	// epoch of the candidates has left the history; dropped is every
	// incarnation of the name an answering node recorded dropped.
	everyAnswered bool
	dropped       []uint64
	// sawCreateInFlight says an answer, or this node's own orphans, had a
	// create of the name in flight: a "nothing" decided then is not recorded,
	// since the create is about to land (on-demand pull spec §4.2, R2-6), and
	// createInFlightAt names where, for a read's retry (§6.8).
	sawCreateInFlight bool
	createInFlightAt  []string
	// heard is every node that answered the fallback's question.
	heard []string
	// epoch is the epoch the round read this bucket's owner at (ownerView):
	// the worker's writes of a read's outcome re-check the settling window
	// with it (on-demand pull spec §12.5, R-3).
	epoch    uint64
	plan     takeOverPlan
	decided  bool
	fallback bool
	err      error
	// release removes the bucket's marker (claimTakeOver).
	release func()
}

// takeOverOwned is this node's take-over of the buckets it owns and has not
// opened (spec §20.8.5). Each bucket's marker is registered first, so that
// everything that waits on the bucket waits on it; no fence is held across an
// exchange. The ordinary path asks the previous owners, newest first, one
// frame per node per pass: the newest that answers that it opened is final,
// provided every newer one answered that it had not. The rest go to the
// fallback, which asks every current member, and every node removed within
// the history, one frame each, and decides only once every current member has
// answered.
func (c *Cluster) takeOverOwned(ctx context.Context, owned []roundItem, outcomes map[heldBucket]copyOutcome, nesting *nestedPasses) {
	var taking []*takingOver
	for index, ownedBucket := range owned {
		// A stopping node ends its round at the next bucket instead of the last:
		// Stop waits for the worker, and every read below holds the node store.
		if ctx.Err() != nil {
			leavePending(outcomes, owned[index:])
			break
		}
		if hook := takeOverBeforeReadHook.Load(); hook != nil {
			(*hook)(c.self.ID, ownedBucket.held.database, ownedBucket.held.bucket)
		}
		// A copy this node has taken over already is settled without the
		// marker: a create of the name that meets the marker at its apply is
		// refused (refuseUnservable), and every refusal that names a bucket
		// wakes a round.
		//
		// Only this worker opens or lands a copy of a bucket this node owns,
		// and only inside completeTakeOver while holding that bucket's marker.
		// A nested pass is run by the same goroutine between two completions,
		// and it reads each bucket's own before claiming it. So for every
		// bucket, outer or nested, no copy is opened or landed between its
		// read of own and its claim.
		own, err := c.answerCopy(ReplBucketRef{Database: ownedBucket.held.database, Bucket: ownedBucket.held.bucket})
		if err != nil {
			c.noteOutcome(outcomes, ownedBucket.held, copyPending, err)
			continue
		}
		if own.Holds && own.Opened && own.Moved == 0 {
			c.endTakeOver(ownedBucket.key)
			outcomes[ownedBucket.held] = copySettled
			continue
		}
		if hook := takeOverBeforeClaimHook.Load(); hook != nil {
			(*hook)(c.self.ID, ownedBucket.held.database, ownedBucket.held.bucket)
		}
		release, claimed := c.claimTakeOver(ownedBucket.held)
		if !claimed {
			outcomes[ownedBucket.held] = copyPending
			continue
		}
		_, epoch, err := c.ownerView(ownedBucket.key)
		if err != nil {
			release()
			c.noteOutcome(outcomes, ownedBucket.held, copyPending, err)
			continue
		}
		candidates, complete := c.takeOverCandidates(ownedBucket.key, own, epoch)
		taking = append(taking, &takingOver{roundItem: ownedBucket, own: heldCopy{node: c.self, answer: own, self: true, current: true},
			candidates: candidates, everyAnswered: complete, dropped: slices.Clone(own.Dropped), epoch: epoch, release: release})
	}
	if ctx.Err() != nil {
		for _, bucketTakeOver := range taking {
			bucketTakeOver.release()
			outcomes[bucketTakeOver.held] = copyPending
		}
		return
	}
	c.askPreviousOwners(ctx, taking)
	var fallbacks []*takingOver
	for _, bucketTakeOver := range taking {
		if bucketTakeOver.fallback && bucketTakeOver.err == nil {
			fallbacks = append(fallbacks, bucketTakeOver)
		}
	}
	c.askEveryHolder(ctx, fallbacks)
	// Each bucket's marker is removed as soon as its own take-over ends — after
	// its open or its copy's commit — not when the round does: a create waiting
	// on the marker, and a replicated create refused while it stands
	// (refuseUnservable), must not wait out the round's other buckets.
	completions := newCompletionQueue(taking)
	for _, bucketTakeOver := range taking {
		if bucketTakeOver.err != nil || !bucketTakeOver.decided {
			bucketTakeOver.release()
			c.noteOutcome(outcomes, bucketTakeOver.held, copyPending, bucketTakeOver.err)
		}
	}
	for {
		if nesting != nil && ctx.Err() == nil {
			c.takeNamedAtBoundary(ctx, completions, outcomes, nesting)
		}
		bucketTakeOver, more := completions.next()
		if !more {
			return
		}
		if ctx.Err() != nil {
			bucketTakeOver.release()
			outcomes[bucketTakeOver.held] = copyPending
			continue
		}
		outcome, err := c.completeTakeOver(ctx, bucketTakeOver)
		// Counted where the take-over completes, not where the fallback is
		// decided: a fallback that waits for an unheard member is decided
		// again every round. A take-over that found nothing to take took
		// nothing from anyone.
		if outcome == copySettled && err == nil && bucketTakeOver.fallback && !bucketTakeOver.plan.nothing {
			c.takeOverFallbacks.Add(1)
		}
		bucketTakeOver.release()
		c.noteOutcome(outcomes, bucketTakeOver.held, outcome, err)
	}
}

// takeOverBeforeClaimHook, when set, runs in takeOverOwned between a bucket's
// read of its own copy and its claim: the interval the one-opener invariant
// says no copy is opened or landed in. A test holds the worker there. Nil in
// production.
var takeOverBeforeClaimHook atomic.Pointer[func(nodeID, database, bucket string)]

// completionQueue is the order a round completes its decided take-overs in
// (spec §5): opens, resumes and nothings first, whole copies after, so an open
// never waits behind another bucket's copy; and a bucket a read names moves to
// the front at the next boundary. Every step is O(1) amortised: a round of
// many buckets with a steady stream of names is not O(round) per completion.
type completionQueue struct {
	order []*takingOver
	front []*takingOver
	// remaining is every decided take-over not completed yet, by key.
	remaining map[placementKey]*takingOver
}

func newCompletionQueue(taking []*takingOver) *completionQueue {
	queue := &completionQueue{remaining: make(map[placementKey]*takingOver, len(taking))}
	for _, bucketTakeOver := range taking {
		if bucketTakeOver.err == nil && bucketTakeOver.decided {
			queue.order = append(queue.order, bucketTakeOver)
			queue.remaining[bucketTakeOver.key] = bucketTakeOver
		}
	}
	slices.SortStableFunc(queue.order, func(a, b *takingOver) int {
		return compareBool(a.copiesWhole(), b.copiesWhole())
	})
	return queue
}

func compareBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case a:
		return 1
	}
	return -1
}

// next is the take-over to complete now: a named one first.
func (q *completionQueue) next() (*takingOver, bool) {
	for _, line := range []*[]*takingOver{&q.front, &q.order} {
		for len(*line) > 0 {
			bucketTakeOver := (*line)[0]
			*line = (*line)[1:]
			if _, waiting := q.remaining[bucketTakeOver.key]; waiting {
				delete(q.remaining, bucketTakeOver.key)
				return bucketTakeOver, true
			}
		}
	}
	return nil, false
}

// takeNamedAtBoundary is spec §5's preemption point, run before each
// completion of a round: it takes every name off the queue. A name of one of
// the round's take-overs still to complete moves to the front; a name of
// another of the round's buckets is left to the round, which puts back the
// ones it did not settle; any other name runs at once in a nested pass, on this
// goroutine, as a round of its own that does not nest again — each bucket
// reads its own copy, claims, asks and completes for itself.
func (c *Cluster) takeNamedAtBoundary(ctx context.Context, completions *completionQueue, outcomes map[heldBucket]copyOutcome, nesting *nestedPasses) {
	var front []*takingOver
	var nested []heldBucket
	c.takeOver.mu.Lock()
	for key, held := range c.takeOver.named {
		if bucketTakeOver, waiting := completions.remaining[key]; waiting {
			front = append(front, bucketTakeOver)
		} else if !nesting.inRound[key] {
			nested = append(nested, held)
		}
	}
	// Every name is taken: a name the round holds needs no second place in
	// line, and a nested one the pass leaves pending is put back after the
	// round, with the round's own.
	clear(c.takeOver.named)
	c.takeOver.mu.Unlock()
	if len(front) > 0 {
		slices.SortStableFunc(front, func(a, b *takingOver) int {
			if order := compareBool(a.copiesWhole(), b.copiesWhole()); order != 0 {
				return order
			}
			return compareHeldBuckets(a.held, b.held)
		})
		completions.front = append(front, completions.front...)
	}
	if len(nested) == 0 {
		return
	}
	slices.SortFunc(nested, compareHeldBuckets)
	for _, held := range nested {
		nesting.ran[placementOf(held.database, held.bucket)] = held
	}
	c.runRoundItems(ctx, nested, outcomes, nil)
}

// copiesWhole reports whether a decided take-over's plan ends in a whole copy
// (copyForTakeOver), by completeTakeOver's switch: nothing, a resume, an open
// of this node's own copy and a wait for the origin's stream do not. It only
// orders a round's completions, so a stream wait that turns into a copy when
// its origin's wait is over counts as no copy.
func (t *takingOver) copiesWhole() bool {
	own, plan := t.own.answer, t.plan
	switch {
	case plan.nothing, plan.resume, plan.best.self:
		return false
	case own.Holds && own.Incarnation != plan.best.answer.Incarnation:
		return false
	case own.Holds && own.Outside == 0 && !own.Owes && holdsAtLeast(own, plan.best.answer):
		return false
	case own.Holds && plan.ordinary && own.Term == plan.best.answer.Term && own.Outside == 0:
		return false
	}
	return true
}

// askPreviousOwners is the ordinary path for every bucket of taking, one frame
// per previous owner per pass.
func (c *Cluster) askPreviousOwners(ctx context.Context, taking []*takingOver) {
	for {
		asking := make(map[string][]*takingOver)
		for _, bucketTakeOver := range taking {
			if bucketTakeOver.decided || bucketTakeOver.fallback || bucketTakeOver.err != nil {
				continue
			}
			if bucketTakeOver.next == len(bucketTakeOver.candidates) {
				bucketTakeOver.fallback = true
				continue
			}
			candidate := bucketTakeOver.candidates[bucketTakeOver.next].node.ID
			asking[candidate] = append(asking[candidate], bucketTakeOver)
		}
		if len(asking) == 0 {
			return
		}
		for _, candidateID := range slices.Sorted(maps.Keys(asking)) {
			askedOfCandidate := asking[candidateID]
			node := askedOfCandidate[0].candidates[askedOfCandidate[0].next].node
			refs := make([]ReplBucketRef, 0, len(askedOfCandidate))
			for _, bucketTakeOver := range askedOfCandidate {
				refs = append(refs, ReplBucketRef{Database: bucketTakeOver.held.database, Bucket: bucketTakeOver.held.bucket})
			}
			answers, err := c.askFrames(ctx, node, refs, c.wcTimeout)
			for index, bucketTakeOver := range askedOfCandidate {
				c.judgeCandidate(bucketTakeOver, answers, index, err)
			}
		}
	}
}

// judgeCandidate takes one previous owner's answer about one bucket.
func (c *Cluster) judgeCandidate(bucketTakeOver *takingOver, answers []ReplCopy, index int, err error) {
	candidate := bucketTakeOver.candidates[bucketTakeOver.next]
	switch {
	case errors.Is(err, errQuestionAcrossEpochs):
		c.setTakeOverStatus(bucketTakeOver.key, takeOverStatus{})
		bucketTakeOver.err = fmt.Errorf("%w: %s, a previous owner, is on another membership epoch: %w", errNotYetDecided, candidate.node.ID, err)
		return
	case err != nil:
		bucketTakeOver.everyAnswered = false
		bucketTakeOver.fallback = true
		return
	}
	answer := answers[index]
	bucketTakeOver.dropped = append(bucketTakeOver.dropped, answer.Dropped...)
	// A previous owner whose moved mark predates the epoch it ceded at
	// regained the bucket and never settled: not opened (SA-10.8, Q48).
	regained := answer.Moved != 0 && answer.Moved < candidate.ceded
	if !answer.Holds || !answer.Opened || regained || slices.Contains(bucketTakeOver.dropped, answer.Incarnation) {
		bucketTakeOver.next++
		return
	}
	if answer.Owes {
		// Its copy lacks an entry its replicas applied (A-10.3): the holders
		// are asked, and one in its term at or past its position that owes
		// nothing is the source.
		bucketTakeOver.fallback = true
		return
	}
	c.setTakeOverStatus(bucketTakeOver.key, takeOverStatus{source: candidate.node, sourceKnown: true})
	bucketTakeOver.plan = takeOverPlan{best: heldCopy{node: candidate.node, answer: answer}, ordinary: true}
	bucketTakeOver.decided = true
}

// holderAnswers is one node's answers to a fallback's question.
type holderAnswers struct {
	node    ring.Node
	current bool
	answers []ReplCopy
	err     error
}

// askHolders asks every current member and every node removed within the
// history about refs, once each, in parallel; a removed node is given
// removedNodeAnswerWait and is not waited for beyond it. A current member this
// node records as down (peer.downSince) is asked too, with catalogQuestionWait
// rather than the write concern's timeout (owner ruling R2, 2026-10-20): the
// record is learned on the gossip lane and can be stale while the member
// answers on the request lane, so a live one still answers and the outcome is
// the one a member not recorded down gets; one that is really gone costs the
// short wait, as a catalog question to a silent member does.
func (c *Cluster) askHolders(ctx context.Context, refs []ReplBucketRef) []*holderAnswers {
	_, members := c.epochMembers()
	var nodes []*holderAnswers
	for _, member := range members {
		if member.ID != c.self.ID {
			nodes = append(nodes, &holderAnswers{node: member, current: true})
		}
	}
	for _, removed := range c.removedInHistory() {
		nodes = append(nodes, &holderAnswers{node: removed})
	}
	var wg sync.WaitGroup
	for _, node := range nodes {
		wait, recordedDown := c.wcTimeout, false
		switch {
		case !node.current:
			wait = removedNodeAnswerWait
		case c.peerKnownDown(node.node.ID):
			wait, recordedDown = catalogQuestionWait, true
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			node.answers, node.err = c.askFrames(ctx, node.node, refs, wait)
			if node.err != nil && recordedDown {
				node.err = fmt.Errorf("%w: %s: %w", errPeerDown, node.node.ID, node.err)
			}
		}()
	}
	wg.Wait()
	return nodes
}

// errPeerDown marks a current member this node records as down that did not
// answer the fallback within catalogQuestionWait: it counts as a member that
// did not answer, as any other silent member does.
var errPeerDown = errors.New("the member is recorded as down and did not answer")

// peerKnownDown reports whether this node's peer for nodeID reports it down.
func (c *Cluster) peerKnownDown(nodeID string) bool {
	c.peersMu.RLock()
	p := c.peers[nodeID]
	c.peersMu.RUnlock()
	if p == nil {
		return false
	}
	_, down := p.downSince()
	return down
}

// askEveryHolder is the fallback for every bucket of taking, one frame per
// node asked.
func (c *Cluster) askEveryHolder(ctx context.Context, taking []*takingOver) {
	if len(taking) == 0 {
		return
	}
	refs := make([]ReplBucketRef, 0, len(taking))
	for _, bucketTakeOver := range taking {
		refs = append(refs, ReplBucketRef{Database: bucketTakeOver.held.database, Bucket: bucketTakeOver.held.bucket})
	}
	nodes := c.askHolders(ctx, refs)
	for index, bucketTakeOver := range taking {
		var copies []heldCopy
		var unheard []string
		for _, node := range nodes {
			switch {
			case node.err == nil:
				copies = append(copies, heldCopy{node: node.node, answer: node.answers[index], current: node.current})
			case node.current:
				unheard = append(unheard, node.node.ID)
			}
		}
		c.decideFallback(bucketTakeOver, copies, unheard)
	}
}

// decideFallback is spec §20.8.5's fallback for one bucket, under I-H1:
// copies are compared only within the incarnation the current members hold
// live.
func (c *Cluster) decideFallback(bucketTakeOver *takingOver, copies []heldCopy, unheard []string) {
	bucketTakeOver.createInFlightAt = nil
	if c.createInFlight(bucketTakeOver.held.database, bucketTakeOver.held.bucket) {
		bucketTakeOver.createInFlightAt = append(bucketTakeOver.createInFlightAt, c.self.ID)
	}
	for _, held := range copies {
		if held.answer.CreateInFlight && !slices.Contains(bucketTakeOver.createInFlightAt, held.node.ID) {
			bucketTakeOver.createInFlightAt = append(bucketTakeOver.createInFlightAt, held.node.ID)
		}
	}
	bucketTakeOver.sawCreateInFlight = len(bucketTakeOver.createInFlightAt) > 0
	bucketTakeOver.heard = bucketTakeOver.heard[:0]
	for _, answered := range copies {
		bucketTakeOver.dropped = append(bucketTakeOver.dropped, answered.answer.Dropped...)
		bucketTakeOver.heard = append(bucketTakeOver.heard, answered.node.ID)
	}
	if bucketTakeOver.own.answer.Holds {
		copies = append(copies, bucketTakeOver.own)
	}
	var gone string
	if len(bucketTakeOver.candidates) > 0 {
		gone = bucketTakeOver.candidates[0].node.ID
	}
	_, members := c.epochMembers()
	if len(unheard) > 0 {
		heard := len(members) - len(unheard)
		c.setTakeOverStatus(bucketTakeOver.key, takeOverStatus{gone: gone, waitingOn: unheard, removable: heard >= len(members)/2+1,
			removedHolds: slices.ContainsFunc(copies, func(held heldCopy) bool {
				return !held.current && held.answer.Holds && !slices.Contains(bucketTakeOver.dropped, held.answer.Incarnation)
			})})
		c.takeOver.mu.Lock()
		c.recordWindowOutcomeLocked(bucketTakeOver, unheard)
		c.takeOver.mu.Unlock()
		bucketTakeOver.err = fmt.Errorf("%w: %s did not answer, and may hold a copy", errNotYetDecided, strings.Join(unheard, ", "))
		return
	}
	var live []uint64
	var liveHolders []string
	for _, held := range copies {
		if held.current && held.answer.Holds && !slices.Contains(bucketTakeOver.dropped, held.answer.Incarnation) && !slices.Contains(live, held.answer.Incarnation) {
			live = append(live, held.answer.Incarnation)
			liveHolders = append(liveHolders, held.node.ID)
		}
	}
	if len(live) > 1 {
		c.warnTwoIncarnations(bucketTakeOver.key, bucketTakeOver.held, liveHolders)
		c.setTakeOverStatus(bucketTakeOver.key, takeOverStatus{incarnationHolders: liveHolders})
		bucketTakeOver.err = fmt.Errorf("%w: %s hold two live incarnations of the bucket", errNotYetDecided, strings.Join(liveHolders, " and "))
		return
	}
	var eligible []heldCopy
	for _, held := range copies {
		if !held.answer.Holds || slices.Contains(bucketTakeOver.dropped, held.answer.Incarnation) {
			continue
		}
		if len(live) == 1 && held.answer.Incarnation != live[0] {
			continue
		}
		eligible = append(eligible, held)
	}
	best, found := bestOf(eligible)
	bucketTakeOver.decided = true
	if !found {
		bucketTakeOver.plan = takeOverPlan{nothing: true}
		return
	}
	bucketTakeOver.plan = takeOverPlan{best: best, alternatives: coveringCopies(eligible, best),
		resume: best.self && best.answer.Opened && bucketTakeOver.everyAnswered}
}

// coveringCopies is the copies of others, best excluded, that hold everything
// best holds and owe nothing, current members first.
func coveringCopies(copies []heldCopy, best heldCopy) []heldCopy {
	var covering []heldCopy
	for _, candidate := range copies {
		if candidate.self || candidate.node.ID == best.node.ID || candidate.answer.Owes || !holdsAtLeast(candidate.answer, best.answer) {
			continue
		}
		covering = append(covering, candidate)
	}
	slices.SortStableFunc(covering, func(a, b heldCopy) int {
		switch {
		case a.current == b.current:
			return 0
		case a.current:
			return -1
		default:
			return 1
		}
	})
	return covering
}

// bestOf is the copy with the highest (term, through), among copies of one
// incarnation, this node's own on a tie — it is opened or resumed where it
// lies, where another's is copied whole; a copy that owes an apply is not the
// best while a copy in its term at or past its position answers without owing
// (A-10.3).
func bestOf(copies []heldCopy) (heldCopy, bool) {
	var best heldCopy
	found := false
	for _, candidate := range copies {
		if !found || lineageAbove(candidate.answer, best.answer) || (candidate.self && !lineageAbove(best.answer, candidate.answer)) {
			best, found = candidate, true
		}
	}
	if !found || !best.answer.Owes {
		return best, found
	}
	for _, candidate := range copies {
		if !candidate.answer.Owes && candidate.answer.Term == best.answer.Term && candidate.answer.Through >= best.answer.Through {
			return candidate, true
		}
	}
	return best, true
}

// lineageAbove reports whether a's (term, through) is above b's.
func lineageAbove(a, b ReplCopy) bool {
	return a.Term > b.Term || (a.Term == b.Term && a.Through > b.Through)
}

// warnTwoIncarnations says, at most once per strayWarnInterval per bucket,
// that two members hold live copies of two incarnations of one name, which
// no take-over merges (I-H1).
func (c *Cluster) warnTwoIncarnations(key placementKey, held heldBucket, holders []string) {
	c.takeOver.mu.Lock()
	last, warned := c.takeOver.strayWarnedAt[key]
	due := !warned || time.Since(last) >= strayWarnInterval
	if due {
		c.takeOver.strayWarnedAt[key] = time.Now()
	}
	c.takeOver.mu.Unlock()
	c.divergencesSeen.Add(1)
	if !due {
		return
	}
	c.logger.Warn(fmt.Sprintf("bucket %q in database %q has two live copies of different incarnations, on %s: two buckets of one name, "+
		"created apart. Writes to it are refused until one goes: remove the node that holds the one that is not wanted "+
		"('remove-node <node>'), or empty that node's data directory and add it again", held.bucket, held.database, strings.Join(holders, " and ")),
		"database", held.database, "bucket", held.bucket, "holders", holders)
}

// completeTakeOver completes one decided take-over: nothing to take, a
// resume, an open of this node's own copy, a wait for the origin's stream, or
// a whole copy.
func (c *Cluster) completeTakeOver(ctx context.Context, bucketTakeOver *takingOver) (copyOutcome, error) {
	held, key, own, plan := bucketTakeOver.held, bucketTakeOver.key, bucketTakeOver.own.answer, bucketTakeOver.plan
	if plan.nothing && bucketTakeOver.sawCreateInFlight {
		// A create of the name is about to land: no "nothing" is recorded for
		// it, for a migrate or a read, and the take-over waits for it, so the
		// next round finds the copy. A read meanwhile is told to retry, naming
		// where the create is in flight (on-demand pull spec §6.8).
		c.setTakeOverStatus(key, takeOverStatus{createInFlightAt: bucketTakeOver.createInFlightAt})
		return copyPending, nil
	}
	if plan.nothing {
		// No node holds a live copy of the name: there is nothing to take
		// over, and a create of it is a new bucket. A migrate waiting on the
		// take-over (it gave leave) is told so. A removed node that handed the
		// bucket here said it holds a copy: unless it answered the question,
		// "nothing" is not decided, and it is waited for until it answers or is
		// confirmed gone (owner rulings R7, R8; BUG-0688). The check and the
		// record are one takeOver.mu section, so an ask recorded meanwhile is
		// either seen here or deleted by endTakeOver below.
		c.takeOver.mu.Lock()
		if handing := c.takeOver.handedOff[key]; handing != "" && !slices.Contains(bucketTakeOver.heard, handing) {
			c.takeOver.status[key] = takeOverStatus{handedOffBy: handing}
			c.takeOver.mu.Unlock()
			return copyPending, fmt.Errorf("%w: %s, removed, handed bucket %q in database %q to this node and did not answer about its copy",
				errNotYetDecided, handing, held.bucket, held.database)
		}
		switch {
		case c.takeOver.leave[key]:
			c.recordOutcome(key, foundOutcome{at: time.Now(), epoch: bucketTakeOver.epoch, leave: true})
		default:
			// A read in the window answers "not found" from this, without
			// asking every member again (spec §4.2).
			c.recordWindowOutcomeLocked(bucketTakeOver, nil)
		}
		c.takeOver.mu.Unlock()
		c.endTakeOver(key)
		return copySettled, nil
	}
	if own.Holds && own.Incarnation != plan.best.answer.Incarnation {
		// A copy of another incarnation is never copied over (I-H1): it is a
		// divergence an operator is told about, not a take-over.
		holders := []string{c.self.ID, plan.best.node.ID}
		c.warnTwoIncarnations(key, held, holders)
		c.setTakeOverStatus(key, takeOverStatus{incarnationHolders: holders})
		return copyPending, fmt.Errorf("%w: this node holds incarnation %d and %s holds incarnation %d", errNotYetDecided,
			own.Incarnation, plan.best.node.ID, plan.best.answer.Incarnation)
	}
	// No term is opened or resumed, and nothing copied in, before the start
	// hold opens (on-demand pull spec §9.2): until a majority of the peers has
	// answered, this node's log may be behind what they hold of it, and a term
	// opened now would stamp the copy with an applied sequence below the tail
	// the hold moves the log past. The hold's opening wakes the worker.
	if c.startHold.shut() {
		return copyPending, nil
	}
	if hook := takeOverBeforeOpenHook.Load(); hook != nil {
		(*hook)(c.self.ID, held.database, held.bucket)
	}
	switch {
	case plan.resume:
		return c.finishTakeOver(key, c.resumeTerm(held, key))
	case plan.best.self && own.Owes:
		// The most complete copy is this node's own and owes an apply: no copy
		// in its term at or past its position owes nothing, or bestOf would
		// have taken it (A-10.3). A copy that owes is never opened as it
		// stands, and it is the best there is, so it is kept, never deleted:
		// the bucket stays refused until the owed apply is made (a restart's
		// replay makes it) or an operator decides.
		return copyPending, c.keepOwingBestCopy(held, key)
	case plan.best.self:
		// The most complete copy is this node's own: it is opened where it
		// lies, whatever marks the change that moved it left — an outside mark
		// says where the copy was placed, not what it holds — and the open
		// clears them. Copying it in would delete it first and then ask this
		// node for it (copyForTakeOver).
		return c.finishTakeOver(key, c.openTerm(held, key))
	case own.Holds && own.Outside == 0 && !own.Owes && holdsAtLeast(own, plan.best.answer):
		return c.finishTakeOver(key, c.openTerm(held, key))
	case own.Holds && plan.ordinary && own.Term == plan.best.answer.Term && own.Outside == 0 && !c.originWaitOver(key):
		// Behind in the term its origin is still delivering: the entries in
		// flight when the membership changed are on their way.
		return copyAwaitsStream, nil
	}
	if !c.mayCopyWhole(key) {
		status := c.takeOverStatusOf(key)
		status.needsCopy = true
		c.setTakeOverStatus(key, status)
		return copyPending, nil
	}
	err := c.copyForTakeOver(ctx, held, key, own, plan)
	if err != nil {
		status := c.takeOverStatusOf(key)
		status.copyRefused = err
		c.setTakeOverStatus(key, status)
	}
	return c.finishTakeOver(key, err)
}

// keepOwingBestCopy records why a take-over whose best copy is this node's own
// and owes an apply does not complete, and says so once per strayWarnInterval:
// the sequences it owes, and the remedies.
func (c *Cluster) keepOwingBestCopy(held heldBucket, key placementKey) error {
	st, err := c.s.bucketStateNamed(held.database, held.bucket)
	if err != nil {
		return fmt.Errorf("reading what this node's copy of bucket %q in database %q owes: %w", held.bucket, held.database, err)
	}
	status := c.takeOverStatusOf(key)
	status.owesApplies = st.FailedSeqs
	status.owedAdvice = c.owedApplyAdvice(held)
	c.setTakeOverStatus(key, status)
	c.takeOver.mu.Lock()
	last, warned := c.takeOver.strayWarnedAt[key]
	due := !warned || time.Since(last) >= strayWarnInterval
	if due {
		c.takeOver.strayWarnedAt[key] = time.Now()
	}
	c.takeOver.mu.Unlock()
	if due {
		c.logger.Warn(fmt.Sprintf("bucket %q in database %q: this node, %s, holds its most complete copy, which owes the applies of sequences %v; "+
			"a copy that owes an apply is not opened as it stands, so writes to the bucket are refused. %s",
			held.bucket, held.database, c.self.ID, st.FailedSeqs, status.owedAdvice),
			"database", held.database, "bucket", held.bucket, "owes", st.FailedSeqs)
	}
	return fmt.Errorf("%w: this node's own copy of bucket %q in database %q is the most complete and owes the applies of sequences %v",
		errNotYetDecided, held.bucket, held.database, st.FailedSeqs)
}

// owedApplyAdvice says what restarting this node does about the writes its
// copy owes, and the way out when it cannot make them (rule 3). The start
// replays an owed write beneath the copy's mark only when it commutes with the
// writes applied after it (repl_recover.go:commutesWithLater): a queue or heap
// write has no key and never does, nor does any write to a bucket with a
// unique index; a keyed write does unless a later write to its key overtook
// it. A write that is not replayed is dropped from the debt and logged, and the
// copy opens without it. A replica that applied it holds it only until this
// node opens a new term and the replica follows it, so the dump comes first.
func (c *Cluster) owedApplyAdvice(held heldBucket) string {
	facts, _ := c.s.peekBucket(held.database, held.bucket)
	queue := facts.bucketType == BucketTypeFIFO || facts.bucketType == BucketTypeLIFO || facts.bucketType == BucketTypeHeap
	wayOut := fmt.Sprintf("To keep it, first dump the bucket from a replica that holds the write "+
		"('hoarddb dump -address <replica's client address> --out ./owed %s %s'), then restart hoarddb-server on %s, "+
		"then restore the dump over the copy ('hoarddb restore -address <%s's client address> --dir ./owed --drop')",
		held.database, held.bucket, c.self.ID, c.self.ID)
	switch {
	case queue:
		return fmt.Sprintf("A %s bucket's write never commutes with later writes, so a restart never replays it: restart hoarddb-server on %s "+
			"and its start logs that the write was not replayed and opens the copy without it. %s", facts.bucketType, c.self.ID, wayOut)
	case declaresUnique(facts.indexes()):
		return fmt.Sprintf("The bucket declares a unique index, so a restart never replays a write beneath the copy's mark: restart hoarddb-server on %s "+
			"and its start logs that the write was not replayed and opens the copy without it. %s", c.self.ID, wayOut)
	default:
		return fmt.Sprintf("To make the applies, restart hoarddb-server on %s: its start replays each owed write from its log, except one that "+
			"a later write to the same key has overtaken, which it logs as not replayed and leaves out of the copy. %s", c.self.ID, wayOut)
	}
}

// originWait is one wait for an origin's stream: when it began, and the
// position it last saw and when that position last advanced.
type originWait struct {
	began, advanced time.Time
	position        uint64
}

// originWaitOver reports whether this node has waited the write-concern
// timeout for its copy to reach the end of its origin's term, starting the
// wait on the first call.
func (c *Cluster) originWaitOver(key placementKey) bool {
	now := time.Now()
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	wait, waiting := c.takeOver.waiting[key]
	if !waiting {
		c.takeOver.waiting[key] = originWait{began: now, advanced: now}
		return false
	}
	return now.Sub(wait.began) >= c.wcTimeout
}

// followerWaitCap bounds a follower's wait for the old owner's stream, in
// write-concern timeouts from its first round, however steadily the stream
// delivers: a stream that only trickles must not hold a replica behind the
// base for ever (owner ruling, 2026-10-09). It is per process: a restart
// begins the wait again.
const followerWaitCap = 10

// followerWaitOver reports whether a follower's wait for the old owner's
// stream to bring its copy up to the owner's base is over, starting the wait
// on the first call. through is the copy's position in the old owner's term.
// The wait ends one write-concern timeout after through last rose — a stream
// that is delivering is not given up on — or followerWaitCap timeouts after
// it began (spec §20.8.6).
func (c *Cluster) followerWaitOver(key placementKey, through uint64) bool {
	now := time.Now()
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	wait, waiting := c.takeOver.waiting[key]
	if !waiting {
		c.takeOver.waiting[key] = originWait{began: now, advanced: now, position: through}
		return false
	}
	if through > wait.position {
		wait.position, wait.advanced = through, now
		c.takeOver.waiting[key] = wait
	}
	return now.Sub(wait.advanced) >= c.wcTimeout || now.Sub(wait.began) >= followerWaitCap*c.wcTimeout
}

// finishTakeOver ends a take-over whose open, resume or copy returned err.
func (c *Cluster) finishTakeOver(key placementKey, err error) (copyOutcome, error) {
	if err != nil {
		return copyPending, err
	}
	c.endTakeOver(key)
	// Settled (R8-F): the sweep's mark waits for nothing else of this bucket.
	c.markIfResidueClear()
	return copySettled, nil
}

// openTerm opens a term on this node's own copy (spec §20.8.5 step 4): under
// the bucket's exclusive fence, one synced batch writes term = this epoch,
// through 0, the base = the lineage the copy reached (a term with no entry is
// transparent), opened, applied_seq = the log's last sequence and no failed
// sequence (trap 8: what the log holds below belongs to the copy this one no
// longer is), and deletes the bucket's placement marks.
func (c *Cluster) openTerm(held heldBucket, key placementKey) error {
	fence := c.s.acquireFence(key, fenceExclusive)
	defer fence.release()
	owner, epoch, err := c.ownerView(key)
	if err != nil {
		return fmt.Errorf("reading the owner of bucket %q in database %q before opening a term on it: %w", held.bucket, held.database, err)
	}
	if owner.ID != c.self.ID {
		return fmt.Errorf("%w: ownership of bucket %q in database %q moved to %s before this node opened it", errNotYetDecided, held.bucket, held.database, owner.ID)
	}
	// A node that disagrees with a member about the set at its own epoch
	// opens no term: its ring may not be the committed one. Defence in depth
	// only; one set per epoch is what the membership change itself keeps.
	if c.mismatch.Load() {
		return fmt.Errorf("%w: this node's membership disagrees with a peer's at epoch %d, so it opens no term on bucket %q in database %q",
			errNotYetDecided, epoch, held.bucket, held.database)
	}
	if err := c.refuseTermPastLimit(held, key, epoch); err != nil {
		return err
	}
	last := c.walLastSequence()
	return c.s.rewriteLineageIf(held.database, held.bucket, func(st *store.BucketState) bool {
		st.BaseTerm, st.BaseThrough = copyLineage(st.Term, st.Through, st.BaseTerm, st.BaseThrough)
		st.Term, st.Through, st.Opened = epoch, 0, true
		st.AppliedSeq, st.FailedSeqs = max(st.AppliedSeq, last), nil
		return true
	})
}

// refuseTermPastLimit refuses to open a term at an epoch no version can
// hold (packVersion): every write in it would be refused. The refusal is
// permanent, so it is not errNotYetDecided; the worker retries it every round,
// and its WARN is said once per strayWarnInterval per bucket. The take-over's
// status carries it, so every request to the bucket is told the limit and the
// dump that gets past it instead of "retry".
func (c *Cluster) refuseTermPastLimit(held heldBucket, key placementKey, epoch uint64) error {
	_, packErr := packVersion(epoch, 0)
	if packErr == nil {
		return nil
	}
	refusal := versionLimitRefusal(held.database, held.bucket, packErr)
	status := c.takeOverStatusOf(key)
	status.versionLimit = refusal
	c.setTakeOverStatus(key, status)
	c.takeOver.mu.Lock()
	last, warned := c.takeOver.limitWarnedAt[key]
	due := !warned || time.Since(last) >= strayWarnInterval
	if due {
		c.takeOver.limitWarnedAt[key] = time.Now()
	}
	c.takeOver.mu.Unlock()
	if due {
		c.logger.Warn(fmt.Sprintf("this node cannot open a term at epoch %d: %v", epoch, refusal),
			"database", held.database, "bucket", held.bucket, "epoch", epoch)
	}
	return fmt.Errorf("opening a term at epoch %d: %w", epoch, refusal)
}

// resumeTerm takes back a term this node opened and nobody has opened since
// (A-10.1): it clears the copy's marks and changes nothing else — not
// applied_seq, not failed_seqs, which may record a debt the copy still owes.
func (c *Cluster) resumeTerm(held heldBucket, key placementKey) error {
	fence := c.s.acquireFence(key, fenceExclusive)
	defer fence.release()
	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(placementMarkKey(key)); err != nil {
		return fmt.Errorf("clearing the marks of bucket %q in database %q to resume its term: %w", held.bucket, held.database, err)
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("resuming the term of bucket %q in database %q: %w", held.bucket, held.database, err)
	}
	c.forgetPlacementMarks(key)
	return nil
}

// copyForTakeOver replaces this node's copy, if it holds one, with the best
// copy, pulled from its holder as a hand-off — any holder serves that, a
// removed node included — and opened in the catalog batch that lands it, so
// there is no instant at which this node holds a complete copy it has not
// opened. It is never called when the best copy is this node's own
// (completeTakeOver opens that one, or keeps it while it owes): a copy held
// here is then another node's to cover, of the best copy's incarnation and at
// or below it, so its deletion — a release: no drop entry, no drop record —
// gives up at most what the take-over chose not to take, and noteConcession
// says what that is (BUG-0475): it is the only trace of those entries.
func (c *Cluster) copyForTakeOver(ctx context.Context, held heldBucket, key placementKey, own ReplCopy, plan takeOverPlan) error {
	// Refused before the release, which would delete the very copy the pull
	// was to read.
	if plan.best.node.ID == c.self.ID {
		return fmt.Errorf("%w: bucket %q in database %q", errPullFromThisNode, held.bucket, held.database)
	}
	// This node's copy is released by the pull that replaces it, once that
	// pull has fetched and checked the whole of the copy it lands and it fits
	// beside this one (BUG-0664): a pull refused before then, whatever the
	// reason — the disk (BUG-0551), the holder, the network, the copy's
	// content — leaves this node's copy as it was.
	var replaced *copyReplacement
	if own.Holds {
		replaced = &copyReplacement{why: "to copy the bucket's most complete copy over it", afterRelease: func() {
			c.noteConcession(held.database, held.bucket, own, plan.best.answer)
			if hook := takeOverAfterReleaseHook.Load(); hook != nil {
				(*hook)(c.self.ID, held.database, held.bucket)
			}
		}}
	}
	err := c.pullFromHolder(ctx, held, plan.best, replaced)
	if err == nil || !holderRefusedCopy(err) {
		return err
	}
	// The holder of the best copy refused it — it does not hand data to this
	// node, or no longer holds it. Another holder of everything it holds is as
	// good a source; on the ordinary path they are found by asking everyone.
	alternatives := plan.alternatives
	if plan.ordinary {
		var answered []heldCopy
		for _, node := range c.askHolders(ctx, []ReplBucketRef{{Database: held.database, Bucket: held.bucket}}) {
			if node.err == nil {
				answered = append(answered, heldCopy{node: node.node, answer: node.answers[0], current: node.current})
			}
		}
		alternatives = coveringCopies(answered, plan.best)
	}
	refusals := []error{err}
	for _, alternative := range alternatives {
		if err = c.pullFromHolder(ctx, held, alternative, replaced); err == nil || !holderRefusedCopy(err) {
			return err
		}
		refusals = append(refusals, err)
	}
	return errors.Join(refusals...)
}

// errPullFromThisNode refuses a take-over's pull whose source is the node
// taking the bucket over: its copy would be released first, and the pull
// would then find nothing to copy.
var errPullFromThisNode = errors.New("a take-over never copies a bucket in from the node taking it over")

// holderRefusedCopy reports whether a take-over's pull failed because its
// source would not or could not hand the bucket over, so another holder of an
// equally complete copy is tried.
//
// A copy this node refuses for what it holds (errPulledCopyRefused: a value
// nested past the bound, a damaged stream, a payload its metadata does not
// match) is the holder's, not the bucket's: another holder's copy may be clean
// (BUG-0664).
func holderRefusedCopy(err error) bool {
	return errors.Is(err, errPlaintextPeerRefused) || errors.Is(err, ErrMigrateBucketNotFound) || errors.Is(err, errPullOfDroppedIncarnation) ||
		errors.Is(err, errPulledCopyRefused)
}

// pullFromHolder copies the bucket in from one holder as a take-over's pull,
// opened in the batch that lands it.
// replaced is this node's own copy the pull replaces, or nil.
func (c *Cluster) pullFromHolder(ctx context.Context, held heldBucket, source heldCopy, replaced *copyReplacement) error {
	if source.node.ID == c.self.ID {
		return fmt.Errorf("%w: bucket %q in database %q", errPullFromThisNode, held.bucket, held.database)
	}
	plan := pullPlan{database: held.database, bucket: held.bucket, source: source.node, mustBePlaced: true, takeOver: true, replaces: replaced}
	if err := c.pullBucketWith(ctx, plan); err != nil {
		return fmt.Errorf("copying bucket %q in database %q from %s to take it over: %w", held.bucket, held.database, source.node.ID, err)
	}
	c.takeOverCopies.Add(1)
	c.logger.Info("took a bucket over by copying it from the holder of its most complete copy",
		"database", held.database, "bucket", held.bucket, "source", source.node.ID, "term", source.answer.Term, "through", source.answer.Through)
	return nil
}

// releaseLocalCopy deletes this node's copy of a bucket under its exclusive
// fence, as a release does: no drop entry, no drop record.
func (c *Cluster) releaseLocalCopy(database, bucket, why string) error {
	fence := c.s.acquireFence(placementOf(database, bucket), fenceExclusive)
	defer fence.release()
	return c.deleteLocalCopyLocked(database, bucket, why)
}

// deleteLocalCopyLocked is releaseLocalCopy for a caller that holds the
// bucket's fence exclusively.
func (c *Cluster) deleteLocalCopyLocked(database, bucket, why string) error {
	resp, err := c.s.DeleteBucket(DeleteBucketRequest{Database: database, Name: bucket})
	if err != nil {
		return fmt.Errorf("deleting this node's copy of bucket %q in database %q %s: %w", bucket, database, why, err)
	}
	if !resp.Success {
		return fmt.Errorf("deleting this node's copy of bucket %q in database %q %s: %s", bucket, database, why, resp.Message)
	}
	return nil
}

// rewriteLineageIf rewrites the state document of database/bucket by change,
// when change reports it changed it, in one synced batch that also deletes the
// bucket's placement marks, and records the result in the bucket's in-memory
// lineage. The caller holds the bucket's fence exclusively, so no apply is
// between its log append and its apply.
//
// A change of the copy's term also deletes the bucket's awaiting records in
// the batch: this node no longer logs in the term they name (spec §7.3), when
// it accepts a new owner's term or opens one of its own on taking the bucket
// back.
func (s *Server) rewriteLineageIf(database, bucket string, change func(*store.BucketState) bool) error {
	if s.serverDataAccessor().getDB(database) == nil {
		return databaseNotFoundErr(database)
	}
	b := s.lockServedRecord(database, bucket)
	if b == nil {
		return bucketNotFoundErr(database, bucket)
	}
	defer b.mu.Unlock()
	snapshot := s.kv.NewSnapshot()
	defer snapshot.Discard()
	st, err := store.LoadBucketState(snapshot, b.slot, storage.LegacyDatabaseID(database), b.id.Load())
	if err != nil {
		return fmt.Errorf("reading the lineage of bucket %q in database %q: %w", bucket, database, err)
	}
	term := st.Term
	if !change(&st) {
		return nil
	}
	key := placementOf(database, bucket)
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if err := st.Put(batch, b.slot, storage.LegacyDatabaseID(database), b.id.Load()); err != nil {
		return fmt.Errorf("staging the lineage of bucket %q in database %q: %w", bucket, database, err)
	}
	if err := batch.Delete(placementMarkKey(key)); err != nil {
		return fmt.Errorf("clearing the marks of bucket %q in database %q: %w", bucket, database, err)
	}
	leftTerm := st.Term != term
	if leftTerm {
		if err := deleteNodeRecords(snapshot, batch, storage.AwaitingRecordKind, bucketRecordScope(key), nil); err != nil {
			return fmt.Errorf("deleting the awaiting records of bucket %q in database %q: %w", bucket, database, err)
		}
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("writing the lineage of bucket %q in database %q: %w", bucket, database, err)
	}
	b.setLineageStateLocked(st)
	if s.cluster != nil {
		s.cluster.forgetPlacementMarks(key)
		if leftTerm {
			s.cluster.forgetAwaitingBucket(key)
		}
	}
	return nil
}
