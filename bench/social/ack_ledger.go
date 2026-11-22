package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The acknowledged-write ledger (WAL power-loss tooling, spec 7.2, owner D8).
//
// A power-loss cycle needs to know which posts the server said it had stored
// before the reset, so that after the reset it can check each one is still
// there. The ledger is the harness's own record of that, per author:
//
//   - Acked is the highest post sequence the author had acknowledged. One
//     author's posts are serialised by bookUser.authoring, so every post below
//     it was issued and answered before it, and the highest is the one a lost
//     log tail would take first.
//   - InFlight is the newest post sent whose answer never came back (the reset
//     landed on it) or came back as an error. The server may or may not hold it;
//     the check reports which and never fails on it.
//
// It is not the deliveryLedger of ops.go, which counts fan-out copies into
// followers' feeds and is something else.

// ackLedgerSchema names the file format, so a later change is refused loudly.
const ackLedgerSchema = "ack-ledger/1"

// ackLedger is the live record, fed by socialBook.beginPost.
type ackLedger struct {
	mutex  sync.Mutex
	issued map[int]int64 // highest sequence sent, per author
	acked  map[int]int64 // highest sequence acknowledged, per author
}

func newAckLedger() *ackLedger {
	return &ackLedger{issued: map[int]int64{}, acked: map[int]int64{}}
}

// sending records that the author's post seq is about to be sent.
func (l *ackLedger) sending(author int, seq int64) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if current, known := l.issued[author]; !known || seq > current {
		l.issued[author] = seq
	}
}

// acknowledged records that the server answered the author's post seq.
func (l *ackLedger) acknowledged(author int, seq int64) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if current, known := l.acked[author]; !known || seq > current {
		l.acked[author] = seq
	}
}

// ackEntry is one author in the ledger file.
type ackEntry struct {
	// User is the author's rank and Author its id as the server knows it.
	User   int    `json:"user"`
	Author string `json:"author"`
	// Bucket is where the author's record lives under the run's placement.
	Bucket string `json:"bucket"`
	// Owner is the node that owns Bucket, read from the cluster when the ledger
	// was written; empty when it could not be read. -ledger-owner scopes a check
	// to the authors one node owns.
	Owner    string `json:"owner,omitempty"`
	Acked    *int64 `json:"acked,omitempty"`
	InFlight *int64 `json:"in_flight,omitempty"`
}

// ackLedgerFile is the JSON file -ledger-out writes.
type ackLedgerFile struct {
	Schema  string     `json:"schema"`
	Engine  string     `json:"engine,omitempty"`
	Dataset string     `json:"dataset,omitempty"`
	Authors []ackEntry `json:"authors"`
	// OwnerErrors counts buckets whose owner could not be read.
	OwnerErrors int `json:"owner_errors,omitempty"`
}

// snapshot is the ledger as a file. owners maps a bucket to the node that owns
// it; a bucket missing from it has no recorded owner.
func (l *ackLedger) snapshot(place placement, owners map[string]string) ackLedgerFile {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	file := ackLedgerFile{Schema: ackLedgerSchema, Authors: []ackEntry{}}
	for author, issued := range l.issued {
		entry := ackEntry{User: author, Author: userID(author), Bucket: place.bucketOf(author)}
		entry.Owner = owners[entry.Bucket]
		acked, hasAcked := l.acked[author]
		if hasAcked {
			entry.Acked = &acked
		}
		if !hasAcked || issued > acked {
			inFlight := issued
			entry.InFlight = &inFlight
		}
		file.Authors = append(file.Authors, entry)
	}
	slices.SortFunc(file.Authors, func(a, b ackEntry) int { return a.User - b.User })
	return file
}

// bucketOwnerReader is an engine that can say which node owns a bucket.
type bucketOwnerReader interface {
	bucketOwner(ctx context.Context, bucket string) (string, error)
}

// bucketOwner is the first node of the bucket's replica set, or standalone.
func (h *hoardEngine) bucketOwner(_ context.Context, bucket string) (string, error) {
	caller, release, err := h.openCaller()
	if err != nil {
		return "", fmt.Errorf("opening a connection to describe bucket %s: %w", bucket, err)
	}
	defer release()
	var described server.BucketStatsResponse
	if err := caller.call(transport.OpBucketStats, server.BucketStatsRequest{Database: h.cfg.Database, Bucket: bucket}, &described); err != nil {
		return "", fmt.Errorf("describing bucket %s: %w", bucket, err)
	}
	if !described.Found {
		return "", fmt.Errorf("describing bucket %s: %s", bucket, described.Message)
	}
	if len(described.Nodes) == 0 {
		return standaloneNode, nil
	}
	return described.Nodes[0], nil
}

// ownerLookupDeadline is the most time the owner lookups of one ledger may take
// in all. After a hard reset a dial to a node that drops packets waits out its
// own timeout, and one lookup per bucket added up to about an hour; the owners
// only scope a check to one node (scope "all" needs none), so the ledger is
// written without them once the time is up.
const ownerLookupDeadline = 20 * time.Second

// ownerLookup is what lookupOwners found: the owner per bucket (empty for a
// bucket whose lookup failed) and how many buckets failed.
type ownerLookup struct {
	owners map[string]string
	failed int
}

// distinctBuckets is the set of buckets the entries live in.
func distinctBuckets(entries []ackEntry) map[string]struct{} {
	buckets := map[string]struct{}{}
	for _, entry := range entries {
		buckets[entry.Bucket] = struct{}{}
	}
	return buckets
}

// lookupOwners asks for the owner of each distinct bucket, one at a time, and
// stops asking once ctx is done. It never writes to entries.
func lookupOwners(ctx context.Context, reader bucketOwnerReader, entries []ackEntry) ownerLookup {
	lookup := ownerLookup{owners: map[string]string{}}
	for bucket := range distinctBuckets(entries) {
		if ctx.Err() != nil {
			lookup.failed++
			continue
		}
		owner, err := reader.bucketOwner(ctx, bucket)
		if err != nil {
			lookup.failed++
		}
		lookup.owners[bucket] = owner
	}
	return lookup
}

// writeAckLedger writes the leg's ledger to cfg.LedgerOut. It runs after the
// workers have stopped, whatever they ended with, because a leg that lost its
// connection to a reset node is exactly the one whose ledger is wanted. Owners
// are read best effort: a node that is down answers no describe, and the
// ledger then names no owner for that bucket and counts the miss.
func writeAckLedger(ctx context.Context, cfg *config, eng engine, book *socialBook) error {
	if cfg.LedgerOut == "" {
		return nil
	}
	place := placementOfConfig(cfg)
	file := book.acks.snapshot(place, nil)
	file.Engine, file.Dataset = cfg.Engine, cfg.Dataset
	if reader, ok := eng.(bucketOwnerReader); ok {
		lookupCtx, cancel := context.WithTimeout(ctx, ownerLookupDeadline)
		defer cancel()
		found := make(chan ownerLookup, 1)
		go func() { found <- lookupOwners(lookupCtx, reader, file.Authors) }()
		select {
		case lookup := <-found:
			for index := range file.Authors {
				file.Authors[index].Owner = lookup.owners[file.Authors[index].Bucket]
			}
			file.OwnerErrors = lookup.failed
		case <-lookupCtx.Done():
			// The lookup goroutine is left to finish its dial on its own; it only
			// reads file.Authors and sends its answer to a buffered channel.
			file.OwnerErrors = len(distinctBuckets(file.Authors))
		}
	}
	encoded, err := json.MarshalIndent(file, "", " ")
	if err != nil {
		return fmt.Errorf("encoding the ledger: %w", err)
	}
	// Written beside its final name and renamed, so a reader never sees half a file.
	temporary := cfg.LedgerOut + ".tmp"
	if err := os.WriteFile(temporary, append(encoded, '\n'), 0o644); err != nil {
		return fmt.Errorf("writing -ledger-out %s: %w", cfg.LedgerOut, err)
	}
	if err := os.Rename(temporary, cfg.LedgerOut); err != nil {
		return fmt.Errorf("moving the ledger to %s: %w", filepath.Clean(cfg.LedgerOut), err)
	}
	return nil
}

// ledgerPageSize is how many of an author's newest posts the check reads.
const ledgerPageSize = 64

// missingPost is an acknowledged post the check did not find.
type missingPost struct {
	Author string `json:"author"`
	Seq    int64  `json:"seq"`
	Owner  string `json:"owner,omitempty"`
}

// ledgerCheckResult is the one JSON line -mode ledgercheck prints.
type ledgerCheckResult struct {
	Mode            string        `json:"mode"`
	Authors         int           `json:"authors"`
	Checked         int           `json:"checked"`
	Skipped         int           `json:"skipped"`
	Found           int           `json:"found"`
	InFlightPresent int           `json:"in_flight_present"`
	InFlightAbsent  int           `json:"in_flight_absent"`
	Missing         []missingPost `json:"missing"`
	Pass            bool          `json:"pass"`
}

// postHeld says whether the author's timeline holds the post seq. It reads the
// newest page and, only when every post on it is newer than seq, asks for the
// post itself.
func postHeld(ctx context.Context, ops socialOps, user int, seq int64) (bool, error) {
	page, err := ops.profileTimeline(ctx, user, ledgerPageSize)
	if err != nil {
		return false, fmt.Errorf("reading the timeline of user %d: %w", user, err)
	}
	oldest := int64(-1)
	for _, post := range page {
		if post.Seq == seq {
			return true, nil
		}
		if oldest < 0 || post.Seq < oldest {
			oldest = post.Seq
		}
	}
	if len(page) < ledgerPageSize || oldest < seq {
		return false, nil
	}
	found, err := ops.postPage(ctx, user, seq)
	if err != nil {
		return false, fmt.Errorf("reading post %d of user %d: %w", seq, user, err)
	}
	return found.Found, nil
}

// checkAckLedger reads every author the ledger names and compares.
func checkAckLedger(ctx context.Context, ops socialOps, ledger ackLedgerFile, owner string) (ledgerCheckResult, error) {
	result := ledgerCheckResult{Mode: modeLedgerCheck, Authors: len(ledger.Authors), Missing: []missingPost{}}
	for _, entry := range ledger.Authors {
		if owner != "" && entry.Owner != owner {
			result.Skipped++
			continue
		}
		result.Checked++
		if entry.Acked != nil {
			held, err := postHeld(ctx, ops, entry.User, *entry.Acked)
			if err != nil {
				return result, err
			}
			if held {
				result.Found++
			} else {
				result.Missing = append(result.Missing, missingPost{Author: entry.Author, Seq: *entry.Acked, Owner: entry.Owner})
			}
		}
		if entry.InFlight != nil {
			held, err := postHeld(ctx, ops, entry.User, *entry.InFlight)
			if err != nil {
				return result, err
			}
			if held {
				result.InFlightPresent++
			} else {
				result.InFlightAbsent++
			}
		}
	}
	result.Pass = len(result.Missing) == 0
	return result, nil
}

// runLedgerCheckOn checks ledger against an open engine, prints the result and
// fails when an acknowledged post is missing.
func runLedgerCheckOn(ctx context.Context, cfg *config, eng socialEngine, ledger ackLedgerFile, out io.Writer) error {
	if ledger.Schema != ackLedgerSchema {
		return fmt.Errorf("-ledger has schema %q, want %q", ledger.Schema, ackLedgerSchema)
	}
	ops, err := eng.newSocialOps()
	if err != nil {
		return fmt.Errorf("opening a connection for the ledger check: %w", err)
	}
	defer ops.close()
	result, err := checkAckLedger(ctx, ops, ledger, cfg.LedgerOwner)
	if err != nil {
		return err
	}
	line, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encoding the ledger check: %w", err)
	}
	if _, err := fmt.Fprintln(out, string(line)); err != nil {
		return fmt.Errorf("writing the ledger check: %w", err)
	}
	if !result.Pass {
		var lost []string
		for _, post := range result.Missing {
			lost = append(lost, fmt.Sprintf("author %s seq %d", post.Author, post.Seq))
		}
		return fmt.Errorf("ledger check failed: %d acknowledged posts are missing: %v", len(lost), lost)
	}
	return nil
}

// runLedgerCheck is -mode ledgercheck: it reads -ledger and checks it against
// the engine the flags name.
func runLedgerCheck(ctx context.Context, cfg *config, out io.Writer) error {
	encoded, err := os.ReadFile(cfg.LedgerFile)
	if err != nil {
		return fmt.Errorf("reading -ledger: %w", err)
	}
	var ledger ackLedgerFile
	if err := json.Unmarshal(encoded, &ledger); err != nil {
		return fmt.Errorf("-ledger %s is not a ledger file: %w", cfg.LedgerFile, err)
	}
	eng, err := newEngine(ctx, cfg)
	if err != nil {
		return err
	}
	defer eng.close()
	social, ok := eng.(socialEngine)
	if !ok {
		return errors.New("-mode ledgercheck needs an engine that implements the social operations")
	}
	return runLedgerCheckOn(ctx, cfg, social, ledger, out)
}
