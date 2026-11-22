package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"
)

// isReadOperation says which side of the reads/writes split an operation is
// on. N1 consumes, so it counts on the write side (spec 6.5); Q1 is a queue
// write; M1 is a read.
func isReadOperation(operation string) bool {
	return strings.HasPrefix(operation, "R") || operation == "M1"
}

// opAccumulator is one operation's measurements in one worker.
type opAccumulator struct {
	latencies *histogram
	requests  int64
	// fanout is the followers every counted publish copied to (W1 only).
	fanout int64
	// resume is the requests spent resuming the authors' post numbering (W1 only).
	resume int64
}

// mixResult is one mix worker's measurements, merged after the run.
type mixResult struct {
	perOperation  map[string]*opAccumulator
	reads, writes *histogram
	errors        map[string]int64
	// recordFailure is a latency the histogram could not hold.
	recordFailure error
	// verified counts the sampled reads that were checked and right; misses the
	// ones that found nothing where something must be; failure is the first
	// wrong answer, which stops the run.
	verified, misses int64
	failure          error
	// series is the per-second counts of -series-out; nil without the flag.
	series *opSeries
}

func newMixResult() *mixResult {
	return &mixResult{perOperation: map[string]*opAccumulator{}, reads: newHistogram(), writes: newHistogram(), errors: map[string]int64{}}
}

// mixWorker runs operations drawn from a mix, one at a time, on a connection of its own.
type mixWorker struct {
	cfg         *config
	eng         socialEngine
	graph       *socialGraph
	book        *socialBook
	scheduler   *mixScheduler
	rng         *rand.Rand
	zipf        func() int
	text        *postGenerator
	ops         socialOps
	measureFrom time.Time
	arrivals    *arrivalSchedule
	result      *mixResult
	ledger      deliveryLedger
	// lastFanout is the followers the last publish copied to, read by the loop
	// once that publish is counted.
	lastFanout int64
	// lastResume is the requests the last publish spent in beginPost, resuming
	// the author's numbering from the newest stored post (0 once resumed).
	lastResume int64
}

func (w *mixWorker) accumulator(operation string) *opAccumulator {
	if w.result.perOperation[operation] == nil {
		w.result.perOperation[operation] = &opAccumulator{latencies: newHistogram()}
	}
	return w.result.perOperation[operation]
}

// loop runs operations until stopAt or until ctx is cancelled.
func (w *mixWorker) loop(ctx context.Context, stopAt time.Time) error {
	defer func() {
		if w.ops != nil {
			w.ops.close()
		}
	}()
	for ctx.Err() == nil && time.Now().Before(stopAt) {
		scheduled, ok := w.awaitArrival(ctx, stopAt)
		if !ok {
			return nil
		}
		if w.ops == nil {
			ops, err := w.eng.newSocialOps()
			if err != nil {
				w.result.errors["dial"]++
				time.Sleep(100 * time.Millisecond)
				continue
			}
			w.ops = ops
		}
		operation := w.scheduler.next()
		started := time.Now()
		windowStart, queueDelay := started, time.Duration(0)
		if w.arrivals != nil {
			windowStart, queueDelay = scheduled, max(started.Sub(scheduled), 0)
		}
		requestsBefore := w.ops.requests()
		verify, err := w.run(ctx, operation)
		completed := time.Now()
		elapsed := time.Since(started) + queueDelay
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.result.errors[errorKind(err)]++
			if isDesynced(err) {
				w.ops.close()
				w.ops = nil
			}
			continue
		}
		if windowStart.Before(w.measureFrom) {
			if w.result.series != nil {
				w.result.series.add(isReadOperation(operation), false, completed, w.measureFrom)
			}
			if err := w.check(verify); err != nil {
				return err
			}
			continue
		}
		accumulator := w.accumulator(operation)
		accumulator.requests += w.ops.requests() - requestsBefore
		if operation == "W1" {
			accumulator.fanout += w.lastFanout
			accumulator.resume += w.lastResume
		}
		sides := []*histogram{accumulator.latencies, w.result.writes}
		if isReadOperation(operation) {
			sides[1] = w.result.reads
		}
		for _, side := range sides {
			if err := side.record(elapsed); err != nil {
				w.result.recordFailure = err
				return err
			}
		}
		if w.result.series != nil {
			w.result.series.add(isReadOperation(operation), true, completed, w.measureFrom)
		}
		if err := w.check(verify); err != nil {
			return err
		}
	}
	return nil
}

// check runs a sampled read's verification, after its clock has stopped, and
// counts the result. A wrong answer is returned and stops the run.
func (w *mixWorker) check(verify func() error) error {
	if verify == nil {
		return nil
	}
	err := verify()
	if err == nil {
		w.result.verified++
		return nil
	}
	var miss *readMiss
	if errors.As(err, &miss) {
		w.result.misses++
	}
	w.result.failure = err
	return err
}

// awaitArrival is the open-loop wait: a closed loop has no schedule.
func (w *mixWorker) awaitArrival(ctx context.Context, stopAt time.Time) (time.Time, bool) {
	if w.arrivals == nil {
		return time.Time{}, true
	}
	scheduled := w.arrivals.next()
	if !scheduled.Before(stopAt) || waitUntil(ctx, scheduled) != nil {
		return time.Time{}, false
	}
	return scheduled, true
}

// otherUser is a uniform user other than not.
func (w *mixWorker) otherUser(not int) int {
	user := w.rng.IntN(w.graph.spec.Users - 1)
	if user >= not {
		user++
	}
	return user
}

// sampled says whether this read is one of the checked ones.
func (w *mixWorker) sampled() bool { return w.rng.Float64()*100 < w.cfg.VerifyPct }

// run performs one operation. The actor is uniform over users; the target of
// a read of someone else is Zipf (spec 6.1). A sampled read returns the check
// to run once its clock has stopped.
func (w *mixWorker) run(ctx context.Context, operation string) (func() error, error) {
	actor, target := w.rng.IntN(w.graph.spec.Users), w.zipf()
	postSeq := int64(w.rng.IntN(w.graph.postsPerUser)) // a seeded post: always there
	var verify func() error
	var err error
	switch operation {
	case "R1":
		verify, err = w.homeTimeline(ctx, actor)
	case "R2":
		_, err = w.ops.profile(ctx, target)
	case "R3":
		verify, err = w.profileTimeline(ctx, target)
	case "R4":
		verify, err = w.postPage(ctx, target, postSeq)
	case "R5":
		verify, err = w.followCheck(ctx, actor, w.followCandidate(actor, target))
	case "R6":
		_, err = w.ops.followingList(ctx, actor, followingPageSize)
	case "R7":
		_, err = w.ops.avatar(ctx, w.book.avatarOwners[w.rng.IntN(len(w.book.avatarOwners))])
	case "N1":
		_, err = w.ops.readNotifications(ctx, actor, inboxPopSize)
	case "W1":
		err = w.publish(ctx, actor)
	case "W2":
		w.book.counters(target, postSeq).likesIssued.Add(1)
		err = w.ops.likePost(ctx, actor, target, postSeq)
	case "W3":
		followee := w.otherUserOr(target, actor)
		w.book.touch(actor, followee)
		err = w.ops.follow(ctx, actor, followee)
	case "W4":
		followee := w.formerFollowee(actor)
		w.book.touch(actor, followee)
		err = w.ops.unfollow(ctx, actor, followee)
	case "W5":
		err = w.comment(ctx, actor, target, postSeq)
	case "W6":
		err = w.ops.uploadAvatar(ctx, actor, w.avatarBytes())
	case "M1":
		_, err = w.ops.mergedTimeline(ctx, w.followees(actor))
	case "Q1":
		_, err = w.ops.queueRound(ctx, w.rng.IntN(outboxBuckets))
	default:
		return nil, fmt.Errorf("operation %q is not one of %s", operation, strings.Join(operationIDs, ", "))
	}
	if err != nil {
		return nil, fmt.Errorf("operation %s: %w", operation, err)
	}
	return verify, nil
}

func (w *mixWorker) homeTimeline(ctx context.Context, actor int) (func() error, error) {
	sampled := w.sampled()
	ackedBefore := w.book.users[actor].feedAcked.Load()
	feed, err := w.ops.homeTimeline(ctx, actor, feedPageSize)
	if err != nil || !sampled {
		return nil, err
	}
	return func() error {
		return verifyFeed(w.book.feedExpectation(actor, ackedBefore, feedPageSize, w.cfg.VerifySafety), feed)
	}, nil
}

func (w *mixWorker) profileTimeline(ctx context.Context, target int) (func() error, error) {
	sampled := w.sampled()
	ackedBefore := w.book.users[target].postsAcked.Load()
	posts, err := w.ops.profileTimeline(ctx, target, profilePageSize)
	if err != nil || !sampled {
		return nil, err
	}
	return func() error {
		return verifyProfileTimeline(w.book.postsExpectation(target, ackedBefore, profilePageSize, w.cfg.VerifySafety), posts)
	}, nil
}

func (w *mixWorker) postPage(ctx context.Context, author int, seq int64) (func() error, error) {
	sampled := w.sampled()
	commentsAcked := w.book.counters(author, seq).commentsAcked.Load()
	page, err := w.ops.postPage(ctx, author, seq)
	if err != nil || !sampled {
		return nil, err
	}
	return func() error {
		return verifyPostPage(w.book.postPageExpectation(author, seq, commentsAcked, w.cfg.VerifySafety), page)
	}, nil
}

func (w *mixWorker) followCheck(ctx context.Context, actor, target int) (func() error, error) {
	sampled := w.sampled()
	follows, err := w.ops.followCheck(ctx, actor, target)
	if err != nil || !sampled {
		return nil, err
	}
	return func() error { return verifyFollowCheck(w.book.followExpectation(actor, target), follows) }, nil
}

// comment is W5, counted before it is sent and again once acknowledged.
func (w *mixWorker) comment(ctx context.Context, commenter, author int, seq int64) error {
	counters := w.book.counters(author, seq)
	counters.commentsIssued.Add(1)
	if err := w.ops.commentOnPost(ctx, commenter, author, seq, w.text.words(80)); err != nil {
		return err
	}
	counters.commentsAcked.Add(1)
	return nil
}

// followCandidate is half the time someone actor follows and otherwise a
// Zipf-chosen user, so R5 sees both answers.
func (w *mixWorker) followCandidate(actor, target int) int {
	if followed := w.graph.following(actor); len(followed) > 0 && w.rng.IntN(2) == 0 {
		return int(followed[w.rng.IntN(len(followed))])
	}
	if target == actor {
		return w.otherUser(actor)
	}
	return target
}

func (w *mixWorker) otherUserOr(target, not int) int {
	if target == not {
		return w.otherUser(not)
	}
	return target
}

// formerFollowee is someone actor followed when the dataset was seeded, which
// W4 unfollows (deleting an absent edge is harmless).
func (w *mixWorker) formerFollowee(actor int) int {
	followed := w.graph.following(actor)
	return int(followed[w.rng.IntN(len(followed))])
}

func (w *mixWorker) followees(actor int) []int {
	followed := w.graph.following(actor)
	followees := make([]int, 0, mergedPostsTotal)
	for _, followee := range followed[:min(len(followed), mergedPostsTotal)] {
		followees = append(followees, int(followee))
	}
	return followees
}

func (w *mixWorker) avatarBytes() []byte {
	content := make([]byte, avatarBytes)
	for i := range content {
		content[i] = byte(w.rng.Uint32())
	}
	return content
}

// publish is W1: the author's next post, numbered under the author's lock.
func (w *mixWorker) publish(ctx context.Context, author int) error {
	requestsBefore := w.ops.requests()
	seq, finish, err := w.book.beginPost(ctx, w.ops, author)
	w.lastResume = w.ops.requests() - requestsBefore
	if err != nil {
		return err
	}
	post := postRecord{
		Author: userID(author), Seq: seq,
		Text:    w.text.words(postTextMin + w.rng.IntN(postTextMax-postTextMin+1)),
		Created: time.Now().UTC().Truncate(time.Millisecond),
		Tags:    []string{tagPool[w.rng.IntN(len(tagPool))]},
	}
	fanout, err := w.ops.publishPost(ctx, post, w.ledger)
	w.lastFanout = int64(fanout)
	finish(err == nil)
	return err
}

// runMixWorkers runs cfg.Conc workers until stopAt and returns their results.
func runMixWorkers(ctx context.Context, cfg *config, eng socialEngine, graph *socialGraph, book *socialBook, measureFrom, stopAt time.Time) ([]*mixResult, error) {
	results := make([]*mixResult, cfg.Conc)
	failures := make([]error, cfg.Conc)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var arrivals *arrivalSchedule
	if rate := cfg.offeredRate(); rate > 0 {
		arrivals = newArrivalSchedule(time.Now(), rate, cfg.RandSeed)
	}
	// Every worker's series has the same shape so they merge by index.
	warmup := max(time.Until(measureFrom), 0)
	var group sync.WaitGroup
	for index := range results {
		results[index] = newMixResult()
		if cfg.SeriesOut != "" {
			results[index].series = newOpSeries(warmup, stopAt.Sub(measureFrom))
		}
		scheduler, err := newMixScheduler(cfg.Mix, cfg.RandSeed+uint64(index))
		if err != nil {
			return nil, fmt.Errorf("-mix: %w", err)
		}
		rng := rand.New(rand.NewPCG(cfg.RandSeed, uint64(index)+1))
		worker := &mixWorker{
			cfg: cfg, eng: eng, graph: graph, book: book, scheduler: scheduler, rng: rng,
			zipf: newUserPicker(rng, graph.spec.Users, cfg.ZipfS), text: &postGenerator{rng: rng},
			measureFrom: measureFrom, arrivals: arrivals, result: results[index], ledger: book.ledger(),
		}
		group.Add(1)
		go func() {
			defer group.Done()
			if err := worker.loop(ctx, stopAt); err != nil {
				failures[index] = err
				cancel()
			}
		}()
	}
	group.Wait()
	return results, errors.Join(failures...)
}

// seedRetryCounter is implemented by an engine whose seeding retried statements
// the server aborted, so a run that needed many shows it in its notes.
type seedRetryCounter interface{ seedRetries() int64 }

// runMixLeg sets the dataset up (or attaches to it), warms up, measures one
// leg of the configured mix and returns its perf-record/2 record.
func runMixLeg(ctx context.Context, cfg *config, spec datasetSpec, eng socialEngine) (*legRecord, error) {
	graph := newSocialGraph(spec, datasetSeed)
	record := newLegRecord()
	record.Engine, record.Dataset, record.Mix = cfg.Engine, spec.Name, mixLabel(cfg.Mix)
	for fact, value := range eng.durability() {
		record.EngineSettings[fact] = value
	}
	record.WriteConcern = record.EngineSettings["write_concern"]
	if err := labelRecord(record, cfg, spec); err != nil {
		return nil, err
	}
	record.Seed, record.Repeat = cfg.RandSeed, cfg.Repeat
	record.Witness.SingleSyncMsP50, record.Witness.SyncOverwriteMsP50, record.Witness.SingleSyncAppendMsP50 = cfg.SyncMS, cfg.SyncOverwriteMS, cfg.SyncAppendMS
	record.Witness.Depth1WriteP50Ms = cfg.WitnessP50MS
	if cfg.SyncMS > 0 && cfg.WitnessP50MS > 0 {
		record.Witness.P50OverSingleSync = cfg.WitnessP50MS / cfg.SyncMS
	}
	record.Date = time.Now().UTC().Format(time.DateOnly)
	if cfg.Attach {
		record.Notes = append(record.Notes, "attached to an existing dataset")
	} else {
		setupStart := time.Now()
		if _, err := eng.setupSocial(ctx, spec); err != nil {
			return nil, fmt.Errorf("setting up the %s schema: %w", spec.Name, err)
		}
		seedStart := time.Now()
		if err := eng.seedSocial(ctx, graph, cfg.Conc); err != nil {
			return nil, fmt.Errorf("seeding %s: %w", spec.Name, err)
		}
		record.Notes = append(record.Notes, fmt.Sprintf("setup_s=%.1f seed_s=%.1f", seedStart.Sub(setupStart).Seconds(), time.Since(seedStart).Seconds()))
		if counter, ok := eng.(seedRetryCounter); ok && counter.seedRetries() > 0 {
			record.Notes = append(record.Notes, fmt.Sprintf("seed_deadlock_retries=%d", counter.seedRetries()))
		}
	}
	if mapper, ok := eng.(placementMapper); ok {
		mapped, err := mapper.placementMap(ctx)
		if err != nil {
			return nil, fmt.Errorf("reading who owns the shard buckets: %w", err)
		}
		if len(mapped.Owners) > 0 {
			record.OwnersPerNode, record.OwnerNodeIDs, record.HoldersOfPrimaryBucket = mapped.Owners, mapped.NodeIDs, mapped.Holders
		}
	}
	book := newSocialBook(graph, cfg.Attach)
	measureFrom := time.Now().Add(cfg.Warmup)
	stopAt := measureFrom.Add(cfg.Duration)
	results, runErr := runMixWorkers(ctx, cfg, eng, graph, book, measureFrom, stopAt)
	measured := min(max(time.Since(measureFrom), 0), cfg.Duration)
	// The series goes out before every early return below: a killed leg ends on
	// one of them, and the kill cycle's rate comes from this file (BD-2).
	if cfg.SeriesOut != "" {
		if err := writeSeries(cfg.SeriesOut, results, measured); err != nil {
			return record, err
		}
	}
	if err := writeAckLedger(ctx, cfg, eng, book); err != nil {
		return record, err
	}
	if err := fillMixRecord(record, cfg, results, measured); err != nil {
		return record, err
	}
	if failure := firstFailure(results); failure != nil {
		record.Notes = append(record.Notes, "verification failed: "+failure.Error())
		return record, fmt.Errorf("verification failed: %w", failure)
	}
	if runErr != nil {
		return record, runErr
	}
	if failure := record.EngineSettings[durabilityFailureKey]; failure != "" {
		record.Integrity = append(record.Integrity, failure)
		return record, fmt.Errorf("P6 integrity failure, the comparison is invalid: %s", failure)
	}
	if mixVerifies(cfg.Mix) && record.Verified == 0 {
		return record, errors.New("no read was verified: the run was too short or the mix has too few reads to prove anything; raise -seconds or -verify-pct")
	}
	return record, nil
}

// firstFailure is the first wrong answer any worker saw.
func firstFailure(results []*mixResult) error {
	for _, result := range results {
		if result.failure != nil {
			return result.failure
		}
	}
	return nil
}

// verifiedOperations are the reads whose answers are checked against the
// bookkeeping (spec 6.6).
var verifiedOperations = []string{"R1", "R3", "R4", "R5"}

// mixVerifies says whether a mix contains a read the run checks; a run of
// op:R2 or op:W1 has nothing to verify and is not an error for having none.
func mixVerifies(mix string) bool {
	if operation, pinned := strings.CutPrefix(mix, "op:"); pinned {
		return slices.Contains(verifiedOperations, operation)
	}
	return true
}

// mixLabel is the mix as the record names it: RH, WH, SK or op:<ID>.
func mixLabel(mix string) string {
	if strings.HasPrefix(mix, "op:") {
		return mix
	}
	return strings.ToUpper(mix)
}

// fillMixRecord merges the workers' results into the leg record and writes
// the histogram files.
func fillMixRecord(record *legRecord, cfg *config, results []*mixResult, measured time.Duration) error {
	reads, writes := newHistogram(), newHistogram()
	perOperation := map[string]*opAccumulator{}
	for _, result := range results {
		reads.merge(result.reads)
		writes.merge(result.writes)
		record.Verified += result.verified
		record.Misses += result.misses
		for kind, count := range result.errors {
			record.ErrorsByType[kind] += count
		}
		for operation, accumulator := range result.perOperation {
			if perOperation[operation] == nil {
				perOperation[operation] = &opAccumulator{latencies: newHistogram()}
			}
			perOperation[operation].latencies.merge(accumulator.latencies)
			perOperation[operation].requests += accumulator.requests
			perOperation[operation].fanout += accumulator.fanout
			perOperation[operation].resume += accumulator.resume
		}
		if result.recordFailure != nil {
			return histogramRangeError(result.recordFailure)
		}
	}
	all := newHistogram()
	all.merge(reads)
	all.merge(writes)
	record.Loop, record.Depth, record.Warm = "closed", cfg.Conc, cfg.WarmConverged
	if cfg.ClientVMs > 1 {
		record.Depth = cfg.TotalConc
	}
	if cfg.Rate > 0 {
		record.Loop = "open"
		rate := cfg.Rate
		record.OfferedRate = &rate
	}
	record.WarmupS, record.MeasuredS = cfg.Warmup.Seconds(), measured.Seconds()
	if measured > 0 {
		record.ReadsPerSec = float64(reads.count()) / measured.Seconds()
		record.WritesPerSec = float64(writes.count()) / measured.Seconds()
		record.OpsPerSec = record.ReadsPerSec + record.WritesPerSec
	}
	record.Latency = legLatency{Read: newLatencyStats(reads), Write: newLatencyStats(writes), All: newLatencyStats(all)}
	for operation, accumulator := range perOperation {
		count := accumulator.latencies.count()
		figures := opRecord{Count: count, ReqPerOp: float64(accumulator.requests) / float64(count), Latency: newLatencyStats(accumulator.latencies)}
		// Only hoardDB's publish is two requests plus one per follower, so only
		// there can a gate subtract the fan-out from req_per_op. A competitor
		// with -fanout bulk sends about 3 requests whatever the follower count,
		// and the difference would go negative.
		if operation == "W1" && count > 0 && (cfg.Engine == engineHoardDB || cfg.Engine == engineRing) {
			mean := float64(accumulator.fanout) / float64(count)
			figures.FanoutMean = &mean
			resume := float64(accumulator.resume) / float64(count)
			figures.ResumeRequestsMean = &resume
		}
		record.Ops[operation] = figures
	}
	if cfg.HistPrefix != "" {
		record.Histogram = histFileName(cfg.HistPrefix)
		if err := writeHistFiles(cfg.HistPrefix, reads, writes); err != nil {
			return err
		}
	}
	return nil
}

// executeMix runs a -mix leg against the configured engine and writes its
// record, one JSON line, to out.
func executeMix(ctx context.Context, cfg *config, out io.Writer) error {
	spec, err := lookupDataset(cfg.Dataset)
	if err != nil {
		return err
	}
	eng, err := newEngine(ctx, cfg)
	if err != nil {
		return err
	}
	defer eng.close()
	social, ok := eng.(socialEngine)
	if !ok {
		return fmt.Errorf("-engine %s does not implement the social operations", cfg.Engine)
	}
	record, runErr := runMixLeg(ctx, cfg, spec, social)
	if record != nil {
		if err := writeLegRecord(out, record); err != nil {
			return err
		}
	}
	return runErr
}
