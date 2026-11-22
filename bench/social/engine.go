package main

import (
	"context"
	"fmt"
	"time"
)

// Engine names accepted by -engine.
const (
	engineHoardDB = "hoarddb"
	engineMongo   = "mongo"
	engineMySQL   = "mysql"
	// enginePostgres is PostgreSQL over its own wire, through pgx.
	enginePostgres = "postgres"
	// engineCockroach is a single insecure CockroachDB node over the Postgres wire.
	engineCockroach = "cockroach"
	// engineRing is hoardDB over the entry-ring wire, in the entity-centric layout: documents
	// with store fields (bench/social/ringload), not one bucket per user.
	engineRing = "ring"
)

// opClock lets an engine narrow the interval charged to an operation. The
// worker sets start before the call; an engine moves start forward past any
// unmeasured prelude and sets stop before verification-only work, so the clock
// covers the same thing on every engine: the reads and writes a real client
// issues, never the checking.
type opClock struct {
	start, stop time.Time
}

// elapsed is the charged duration; stop is zero when the engine did no
// verification-only work after the operation.
func (c *opClock) elapsed() time.Duration {
	if c.stop.IsZero() {
		return time.Since(c.start)
	}
	return c.stop.Sub(c.start)
}

// readRequest is one timeline read: the newest Limit posts of Author plus the
// author's profile.
type readRequest struct {
	Author string
	Limit  int
	// Sampled reads are verified; the engine also returns the data the
	// verifier needs, after stopping the clock.
	Sampled bool
	Clock   *opClock
}

// timelinePage is what a sampled read returns. The verifier needs the author's
// whole post count. hoardDB's scan returns it from the same snapshot as the
// page, so TotalLow == TotalHigh. A SQL or document engine has no such
// snapshot without a transaction, so it counts before and after the page and
// the page must be consistent with one of the two (see resolveTotal).
type timelinePage struct {
	Posts               []postView
	TotalLow, TotalHigh int64
	ProfileOwner        string // the user id the profile document claims
}

// engineSession is one worker's handle on the database. hoardDB's is a
// dedicated TLS socket; the others share a pool of exactly -conc connections.
type engineSession interface {
	readTimeline(ctx context.Context, request readRequest) (timelinePage, error)
	writePost(ctx context.Context, author string, post map[string]interface{}) error
	putProfile(ctx context.Context, userID string, profile map[string]interface{}) error
	// attachState reports an existing user's post count and the next free
	// sequence number, for -attach.
	attachState(ctx context.Context, userID string) (total, nextSeq int64, err error)
	close()
}

// schemaInfo describes what setup created.
type schemaInfo struct {
	Layout  string   // "per-user-buckets", "single-collection" or "single-table"
	Detail  []string // the DDL or index definitions in force
	Buckets int
	// PerBucketCreation is true when setup creates one object per user, so
	// buckets_created_per_sec means something.
	PerBucketCreation bool
}

// engine is one database under test.
type engine interface {
	// setup creates the schema; the caller times it.
	setup(ctx context.Context) (schemaInfo, error)
	// attach describes an existing seeded dataset instead of creating one.
	attach(ctx context.Context) (schemaInfo, error)
	newSession() (engineSession, error)
	// desynced reports whether a session can no longer be trusted after err.
	desynced(err error) bool
	// durability is what the run records about acknowledgement semantics.
	durability() map[string]string
	// target is the endpoint with credentials removed.
	target() string
	close()
}

func newEngine(ctx context.Context, cfg *config) (engine, error) {
	switch cfg.Engine {
	case engineHoardDB:
		return newHoardEngine(cfg), nil
	case engineMongo:
		return newMongoEngine(ctx, cfg)
	case engineMySQL:
		return newMySQLEngine(ctx, cfg)
	case enginePostgres:
		return newPostgresEngine(ctx, cfg)
	case engineCockroach:
		return newCockroachEngine(ctx, cfg)
	case engineRing:
		return newRingEngine(cfg), nil
	default:
		return nil, fmt.Errorf("unknown -engine %q: use hoarddb, mongo, mysql, postgres, cockroach or ring", cfg.Engine)
	}
}

// resolveTotal picks the post count a page of pageLen posts was read at, given
// the counts taken immediately before (low) and after (high) it. A page is
// consistent with a count c when it holds min(limit, c) posts; if neither
// bracket count explains the page, it is wrong, not merely racy.
func resolveTotal(limit int, low, high int64, pageLen int) (int64, error) {
	if low == high {
		return low, nil
	}
	if int64(pageLen) == min(int64(limit), high) {
		return high, nil
	}
	if int64(pageLen) == min(int64(limit), low) {
		return low, nil
	}
	return 0, fmt.Errorf("a page of %d posts is consistent with neither bracketing count (%d before, %d after, limit %d)", pageLen, low, high, limit)
}
