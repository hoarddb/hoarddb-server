package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The TTL sweeper (GAP-14 build doc Step 17, spec §6.1–§6.2). One goroutine
// per node, on Server rather than Cluster, because a standalone node — most
// users — has no Cluster (§4 F-8). A pass visits the buckets that declare a
// TTL index, never every bucket (S-9): at 10^6 buckets it costs what the
// TTL buckets cost. On a cluster a pass skips every bucket this node does not
// own; the owner's expiry is an ordinary delete, appended, applied in the
// owner's order and shipped, so replicas never sweep. Each delete carries its
// expiry condition (Step 16) and the lowest write concern, and holds no lock
// of the sweeper's: enumerate under a snapshot, release it, then delete, each
// delete taking the bucket's locks itself (spec §8).

// ttlSweepBatch bounds the expired keys one round of a bucket's walk
// collects before deleting them. A variable so a test can make a backlog
// span several rounds.
var ttlSweepBatch = 1000

// errTTLRoundFull stops a round's walk once it holds ttlSweepBatch keys.
var errTTLRoundFull = errors.New("TTL round full")

// ttlBucket names a bucket that declares a TTL index.
type ttlBucket struct {
	database, bucket string
}

// ttlRegistry is the set of buckets declaring a TTL index, maintained where a
// declaration is recorded and where a bucket is dropped. Its mutex is a leaf.
// The zero value is ready.
type ttlRegistry struct {
	mu      sync.Mutex
	buckets map[placementKey]ttlBucket
}

// note records whether database/bucket declares a TTL index.
func (r *ttlRegistry) note(database, bucket string, specs []store.IndexSpec) {
	declares := false
	for _, spec := range specs {
		if spec.TTL != nil && !indexesAPath(spec) {
			declares = true
		}
	}
	key := placementOf(database, bucket)
	r.mu.Lock()
	defer r.mu.Unlock()
	if !declares {
		delete(r.buckets, key)
		return
	}
	if r.buckets == nil {
		r.buckets = make(map[placementKey]ttlBucket)
	}
	r.buckets[key] = ttlBucket{database: database, bucket: bucket}
}

// forget removes a dropped bucket.
func (r *ttlRegistry) forget(database, bucket string) {
	r.mu.Lock()
	delete(r.buckets, placementOf(database, bucket))
	r.mu.Unlock()
}

// forgetDatabase removes every bucket of a dropped database.
func (r *ttlRegistry) forgetDatabase(database string) {
	r.mu.Lock()
	for key, named := range r.buckets {
		if named.database == database {
			delete(r.buckets, key)
		}
	}
	r.mu.Unlock()
}

// list returns the registered buckets in name order.
func (r *ttlRegistry) list() []ttlBucket {
	r.mu.Lock()
	listed := make([]ttlBucket, 0, len(r.buckets))
	for _, named := range r.buckets {
		listed = append(listed, named)
	}
	r.mu.Unlock()
	sort.Slice(listed, func(i, j int) bool {
		if listed[i].database != listed[j].database {
			return listed[i].database < listed[j].database
		}
		return listed[i].bucket < listed[j].bucket
	})
	return listed
}

// ttlSkipLogInterval is the least time between two of the sweeper's lines
// about a skipped document, over every bucket of the node: a document that
// cannot expire is skipped again each pass, and at 10^6 buckets one line
// per bucket per pass would be the log. A variable so a test can shorten it.
var ttlSkipLogInterval = time.Minute

// ttlSkipLog paces those lines. Its mutex is a leaf. The zero value is ready.
type ttlSkipLog struct {
	mu       sync.Mutex
	last     time.Time
	unlogged int
}

// due reports whether a skip at now is logged, and how many skips since the
// last logged one were not.
func (l *ttlSkipLog) due(now time.Time) (bool, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.last.IsZero() && now.Sub(l.last) < ttlSkipLogInterval {
		l.unlogged++
		return false, 0
	}
	unlogged := l.unlogged
	l.last, l.unlogged = now, 0
	return true, unlogged
}

// ttlPass is what one pass did.
type ttlPass struct {
	visited, deleted, spared, failed int
	// skipped: expired documents left whole because their store fields are
	// too large to delete in one batch. They are not a backlog the budget
	// would clear, so they do not make the pass exhausted.
	skipped int
	// exhausted: the pass ended on its budget with expired documents left.
	exhausted bool
	// backlog is the expired documents the pass collected and did not
	// reach: a lower bound on what is left.
	backlog int
}

// startTTLSweeper starts the node's one sweeper goroutine, unless the
// interval is zero. Stop cancels s.ctx and waits on ttlWG.
func (s *Server) startTTLSweeper() {
	interval := s.config.Server.TTLSweepSeconds
	if interval <= 0 {
		s.logger.Info("TTL sweeper disabled: ttl_sweep_seconds is 0, so no document expires")
		return
	}
	for _, named := range s.ttl.list() {
		s.logTTLIndexes(named, interval)
	}
	s.ttlWG.Add(1)
	go func() {
		defer s.ttlWG.Done()
		ticker := time.NewTicker(time.Duration(interval) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
			}
			for s.ctx.Err() == nil {
				pass := s.sweepTTL(s.ctx, time.Now())
				if !pass.exhausted {
					break
				}
				s.logger.Info("TTL sweep ran out of its budget with expired documents left; starting the next pass now",
					"deleted", pass.deleted, "spared", pass.spared, "failed", pass.failed)
			}
		}
	}()
}

// logTTLIndexes is the start's one Info line per TTL index.
func (s *Server) logTTLIndexes(named ttlBucket, interval int) {
	facts, held := s.peekBucket(named.database, named.bucket)
	if !held {
		return
	}
	for _, spec := range facts.indexes() {
		if spec.TTL != nil && !indexesAPath(spec) {
			s.logger.Info("TTL index: documents expire this long after the date in its field, checked every sweep",
				"database", named.database, "bucket", named.bucket, "index", spec.Name, "field", spec.Fields[0].Field,
				"expire_after_seconds", spec.TTL.ExpireAfterSeconds, "sweep_seconds", interval)
		}
	}
}

// sweepTTL runs one pass at now: every registered bucket this node owns,
// until each is drained or the budget runs out.
func (s *Server) sweepTTL(ctx context.Context, now time.Time) ttlPass {
	budget := time.Duration(s.config.Server.TTLSweepBudgetSeconds) * time.Second
	if budget <= 0 {
		budget = 5 * time.Second
	}
	started := time.Now()
	deadline := started.Add(budget)
	var pass ttlPass
	defer func() {
		ttlSweepDuration.Observe(time.Since(started).Seconds())
		ttlBacklogEstimate.Set(float64(pass.backlog))
		if pass.exhausted {
			ttlSweepBudgetExhausted.Inc()
		}
	}()
	for _, named := range s.ttl.list() {
		if ctx.Err() != nil {
			return pass
		}
		if time.Now().After(deadline) {
			pass.exhausted = true
			return pass
		}
		if s.cluster != nil && s.cluster.Enabled() && !s.cluster.ownsBucket(placementOf(named.database, named.bucket)) {
			continue
		}
		pass.visited++
		if !s.sweepBucket(ctx, named, now, deadline, &pass) {
			pass.exhausted = true
			return pass
		}
	}
	return pass
}

// sweepBucket expires one bucket's documents, round after round, and reports
// whether it drained before the deadline. An index that is building or
// conflicted is skipped until it is ready: its entries are not the truth yet.
func (s *Server) sweepBucket(ctx context.Context, named ttlBucket, now, deadline time.Time, pass *ttlPass) bool {
	facts, held := s.peekBucket(named.database, named.bucket)
	if !held {
		return true
	}
	failureLogged := false
	for _, spec := range facts.indexes() {
		if spec.TTL == nil || spec.State != store.IndexReady || indexesAPath(spec) {
			continue
		}
		cutoff := now.Add(-time.Duration(spec.TTL.ExpireAfterSeconds) * time.Second)
		attempted := make(map[string]bool)
		for {
			keys, err := s.expiredKeys(named, spec, now, attempted)
			if err != nil {
				s.logger.Warn("TTL sweep could not read an index; the bucket is skipped this pass",
					"database", named.database, "bucket", named.bucket, "index", spec.Name, "error", err)
				break
			}
			if len(keys) == 0 {
				break
			}
			for position, key := range keys {
				attempted[key] = true
				s.expireOne(ctx, named, spec.Fields[0].Field, key, cutoff, pass, &failureLogged)
				if time.Now().After(deadline) || ctx.Err() != nil {
					pass.backlog += len(keys) - position - 1
					return false
				}
			}
		}
	}
	return true
}

// expiredKeys collects up to ttlSweepBatch keys the two TTL brackets return
// that this pass has not tried yet, under one snapshot it releases before any
// delete.
func (s *Server) expiredKeys(named ttlBucket, spec store.IndexSpec, now time.Time, attempted map[string]bool) ([]string, error) {
	view, snap, err := s.resolveForRead(named.database, named.bucket)
	if err != nil {
		return nil, fmt.Errorf("resolving the bucket: %w", err)
	}
	defer snap.Discard()
	index := store.NewSecondaryIndex(snapshotKV{snap: snap}, view.slot, view.databaseID, view.id.Load(), spec.ID)
	var keys []string
	for _, bracket := range store.TTLBounds(now, time.Duration(spec.TTL.ExpireAfterSeconds)*time.Second) {
		err := index.ScanBracket(bracket.Tag, bracket.Upper, func(docKey []byte) error {
			if !attempted[string(docKey)] {
				keys = append(keys, string(docKey))
			}
			if len(keys) >= ttlSweepBatch {
				return errTTLRoundFull
			}
			return nil
		})
		if errors.Is(err, errTTLRoundFull) {
			return keys, nil
		}
		if err != nil {
			return nil, err
		}
	}
	return keys, nil
}

// expireOne issues one conditional delete through the ordinary write path,
// at the lowest write concern: a sweep must not wait out the write concern
// timeout per document for a replica that is down (spec §6.2.2).
func (s *Server) expireOne(ctx context.Context, named ttlBucket, field, key string, cutoff time.Time, pass *ttlPass, failureLogged *bool) {
	payload, err := bson.Marshal(bson.M{
		"database": named.database, "bucket": named.bucket, "key": key, "write_concern": "1",
		"expire_if": bson.M{"field": field, "date_before": primitive.NewDateTimeFromTime(cutoff), "seconds_before": cutoff.Unix()},
	})
	if err == nil {
		var answer *transport.Frame
		answer, err = s.routeWrite(ctx, transport.OpDelete, payload)
		if err == nil {
			err = frameError(answer)
		}
		if err == nil {
			var resp DeleteResponse
			if decodeErr := bson.Unmarshal(answer.Payload, &resp); decodeErr != nil {
				err = fmt.Errorf("decoding the answer to the expiry: %w", decodeErr)
			} else {
				switch {
				case resp.Spared:
					pass.spared++
					ttlSweepDocuments.WithLabelValues("spared").Inc()
					return
				case resp.Skipped:
					pass.skipped++
					ttlSweepDocuments.WithLabelValues("skipped").Inc()
					s.logTTLSkip(named, key, resp.Message)
					return
				case resp.Success:
					pass.deleted++
					ttlSweepDocuments.WithLabelValues("deleted").Inc()
					return
				}
				// A failed apply is answered in-band as Success false, not as an
				// error frame; the message is the reason.
				err = errors.New(resp.Message)
			}
		}
	}
	pass.failed++
	ttlSweepDocuments.WithLabelValues("failed").Inc()
	if !*failureLogged {
		*failureLogged = true
		s.logger.Warn(fmt.Sprintf("TTL sweep could not expire a document of bucket %q in database %q; the pass moves on and the next retries it. "+
			"Further failures in this bucket are not logged this pass.", named.bucket, named.database),
			"database", named.database, "bucket", named.bucket, "document", key, "error", err)
	}
}

// logTTLSkip writes the Warn line for a document the sweep skipped, at most
// once per ttlSkipLogInterval. reason is the expiry's own answer: the
// document, its store fields with their counts, and how to empty them.
func (s *Server) logTTLSkip(named ttlBucket, key, reason string) {
	logged, unlogged := s.ttlSkips.due(time.Now())
	if !logged {
		return
	}
	s.logger.Warn(fmt.Sprintf("TTL sweep skipped a document. %s This line is written at most once every %s; "+
		"%d other skips since the last one were not logged, and hoarddb_ttl_sweep_documents_total{outcome=\"skipped\"} counts them all.",
		reason, ttlSkipLogInterval, unlogged),
		"database", named.database, "bucket", named.bucket, "document", key)
}
